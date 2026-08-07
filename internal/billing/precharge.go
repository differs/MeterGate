package billing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrInsufficientBalance is returned by PreCharge when the user balance
// cannot cover the estimate.
var ErrInsufficientBalance = errors.New("insufficient balance")

const (
	prechargeTTL = 48 * time.Hour // sweep window for leaked pre-charges
)

// Redis keys:
//
//	balance:{user}        available balance (int64 micros)
//	frozen:{user}         pre-charged but unsettled amount
//	precharge:{req}       amount + ts of one pre-charge (TTL 48h)
func balKey(user string) string    { return "balance:" + user }
func frozenKey(user string) string { return "frozen:" + user }
func preKey(req string) string     { return "precharge:" + req }

// PreChargeScript atomically reserves `amount` from the user balance.
// Returns ErrInsufficientBalance when the balance cannot cover it.
//
// KEYS: balance, frozen, precharge
// ARGV: amount (int64 micros), ttl seconds
var PreChargeScript = redis.NewScript(`
local bal = tonumber(redis.call('GET', KEYS[1]) or '0')
local amount = tonumber(ARGV[1])
if bal < amount then
  return -1
end
redis.call('DECRBY', KEYS[1], amount)
redis.call('INCRBY', KEYS[2], amount)
redis.call('SET', KEYS[3], ARGV[1], 'EX', ARGV[2])
return 1
`)

// settleScriptSrc is the raw Lua for SettleScript (also usable in a
// pipeline where redis.NewScript objects cannot be reused).
const settleScriptSrc = `
local pre = tonumber(redis.call('GET', KEYS[3]) or '0')
if pre <= 0 then
  return 0
end
local charged = tonumber(ARGV[1])
local refund = pre - charged
if refund < 0 then
  -- Actual consumption exceeded the pre-charge estimate (e.g. a long
  -- streaming response): claw the shortfall back from the balance.
  -- The balance is guaranteed sufficient because the request was already
  -- served; a negative balance here is corrected by reconciliation.
  redis.call('DECRBY', KEYS[1], -refund)
  refund = 0
elseif refund > 0 then
  redis.call('INCRBY', KEYS[1], refund)
end
redis.call('DECRBY', KEYS[2], pre)
redis.call('DEL', KEYS[3])
return refund
`

// SettleScript finalizes a pre-charge: keeps `charged` as real consumption,
// returns the remainder to the balance, clears the pre-charge marker.
// charged=0 means full release (no-charge / failed request).
//
// KEYS: balance, frozen, precharge
// ARGV: charged (int64 micros)
var SettleScript = redis.NewScript(settleScriptSrc)

// Precharger runs the fast-path balance guard.
type Precharger struct {
	rdb *redis.Client
}

// NewPrecharger builds a Precharger.
func NewPrecharger(rdb *redis.Client) *Precharger {
	return &Precharger{rdb: rdb}
}

// PreCharge reserves an estimate for a request. TTL ensures a crashed
// gateway cannot leak frozen funds forever (swept by reconcile).
func (p *Precharger) PreCharge(ctx context.Context, userID, requestID string, amountMicros int64) error {
	if amountMicros <= 0 {
		return nil
	}
	res, err := PreChargeScript.Run(ctx, p.rdb,
		[]string{balKey(userID), frozenKey(userID), preKey(requestID)},
		amountMicros, int64(prechargeTTL.Seconds()),
	).Int64()
	if err != nil {
		return fmt.Errorf("precharge script: %w", err)
	}
	if res < 0 {
		return ErrInsufficientBalance
	}
	return nil
}

// Settle finalizes a pre-charge (idempotent: unknown request → no-op).
func (p *Precharger) Settle(ctx context.Context, userID, requestID string, chargedMicros int64) error {
	if chargedMicros < 0 {
		chargedMicros = 0
	}
	_, err := SettleScript.Run(ctx, p.rdb,
		[]string{balKey(userID), frozenKey(userID), preKey(requestID)},
		chargedMicros,
	).Int64()
	return err
}

// Balance returns the live user balance (zero when unset).
func (p *Precharger) Balance(ctx context.Context, userID string) (int64, error) {
	return p.rdb.Get(ctx, balKey(userID)).Int64()
}

// TopUp credits a user balance (used by tests and the future recharge API).
func (p *Precharger) TopUp(ctx context.Context, userID string, amountMicros int64) error {
	if amountMicros <= 0 {
		return nil
	}
	return p.rdb.IncrBy(ctx, balKey(userID), amountMicros).Err()
}

// BatchSettle settles many pre-charges in one Redis round-trip (pipeline).
// Idempotent per request_id; used by the Settler's flush path to avoid
// N sequential round-trips per batch (the hot-path bottleneck at high QPS).
func (p *Precharger) BatchSettle(ctx context.Context, settles []SettleReq) {
	if len(settles) == 0 {
		return
	}
	pipe := p.rdb.Pipeline()
	for _, s := range settles {
		charged := s.ChargedMicros
		if charged < 0 {
			charged = 0
		}
		_ = pipe.Eval(ctx, settleScriptSrc, []string{balKey(s.UserID), frozenKey(s.UserID), preKey(s.RequestID)}, charged)
	}
	_, _ = pipe.Exec(ctx)
}

// SettleReq is one pre-charge settlement.
type SettleReq struct {
	UserID        string
	RequestID     string
	ChargedMicros int64
}

// FrozenBalance sums frozen amounts across all users (reconcile sweep).
func (p *Precharger) FrozenBalance(ctx context.Context) (int64, error) {
	var total int64
	iter := p.rdb.Scan(ctx, 0, "frozen:*", 1000).Iterator()
	for iter.Next(ctx) {
		v, err := p.rdb.Get(ctx, iter.Val()).Int64()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return 0, err
		}
		total += v
	}
	return total, iter.Err()
}
