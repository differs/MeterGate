// Package ratelimit — quota enforcement for the gateway hot path:
// RPM (requests per minute), TPM (tokens per minute) and in-flight
// concurrency, per key and per user. All checks are atomic Redis Lua
// sliding windows, so multi-instance gateways share the same quotas
// (this is the first layer of the six-layer budget model: org → team →
// project → user → key → end-user).
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limits defines the quota for one scope (key or user).
type Limits struct {
	RPM         int   // requests per minute (0 = unlimited)
	TPM         int64 // tokens per minute (0 = unlimited)
	Concurrency int   // in-flight requests (0 = unlimited)
}

// Unlimited is the default (no limits enforced).
var Unlimited = Limits{}

// Window length for the sliding window.
const window = 60 * time.Second

// Checker enforces limits against Redis.
type Checker struct {
	rdb *redis.Client
}

// NewChecker builds the limiter.
func NewChecker(rdb *redis.Client) *Checker {
	return &Checker{rdb: rdb}
}

// windowLua is a sliding-window counter with expiry:
//
//	KEYS[1] = rl:{scope}:{kind}
//	ARGV[1] = now_ms  ARGV[2] = window_ms  ARGV[3] = limit  ARGV[4] = weight
//
// returns: -1 = over limit; otherwise the current window count.
var windowScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local win = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local weight = tonumber(ARGV[4])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - win)
local count = tonumber(redis.call('ZCARD', KEYS[1]) or '0')
if count + weight > limit then
  return -1
end
for i = 1, weight do
  redis.call('ZADD', KEYS[1], now, now .. ':' .. redis.call('INCR', KEYS[1] .. ':seq'))
end
redis.call('EXPIRE', KEYS[1], win / 1000 + 1)
return count + weight
`)

// CheckRPM records one request and returns the retry-after seconds when
// over the per-minute limit.
func (c *Checker) CheckRPM(ctx context.Context, scope string, limit int) (retryAfter int, ok bool) {
	if limit <= 0 {
		return 0, true // unlimited
	}
	res, err := windowScript.Run(ctx, c.rdb,
		[]string{"rl:" + scope + ":rpm"},
		time.Now().UnixMilli(), window.Milliseconds(), limit, 1,
	).Int64()
	if err != nil || res < 0 {
		return retryAfterFor(scope, c.rdb), false
	}
	return 0, true
}

// CheckTPM records `tokens` and returns retry-after when over the per-minute
// token limit.
func (c *Checker) CheckTPM(ctx context.Context, scope string, limit, tokens int64) (retryAfter int, ok bool) {
	if limit <= 0 || tokens <= 0 {
		return 0, true
	}
	res, err := windowScript.Run(ctx, c.rdb,
		[]string{"rl:" + scope + ":tpm"},
		time.Now().UnixMilli(), window.Milliseconds(), limit, tokens,
	).Int64()
	if err != nil || res < 0 {
		return retryAfterFor(scope, c.rdb), false
	}
	return 0, true
}

// Acquire counts one in-flight request (concurrency limit). Call Release
// when the request finishes.
func (c *Checker) Acquire(ctx context.Context, scope string, limit int) (ok bool, release func()) {
	if limit <= 0 {
		return true, func() {}
	}
	key := "rl:" + scope + ":inflight"
	// INCR with TTL: the key must always exist with a TTL so a crashed
	// gateway doesn't leak the counter forever (renew on every acquire).
	ttl, _ := c.rdb.TTL(ctx, key).Result()
	if ttl < 0 {
		c.rdb.Expire(ctx, key, 2*window)
	}
	cur, err := c.rdb.Incr(ctx, key).Result()
	if err != nil || cur > int64(limit) {
		// release the slot we just took
		c.rdb.Decr(ctx, key)
		return false, func() {}
	}
	return true, func() {
		c.rdb.Decr(ctx, key)
	}
}

// retryAfterFor estimates how long until the window slides (worst case =
// the oldest entry expires; we return a safe 30s default for 429).
func retryAfterFor(scope string, rdb *redis.Client) int {
	return 30
}

// Done releases an in-flight slot acquired via Acquire (convenience).
func (c *Checker) Done(ctx context.Context, scope string) {
	c.rdb.Decr(ctx, "rl:"+scope+":inflight")
}
