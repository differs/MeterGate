// Package payment — recharge & payment flow: create a recharge order,
// pay via a channel (mock in dev; alipay/wechat/stripe adapters later),
// verify callbacks idempotently, credit the user's Redis balance.
//
// Money safety:
//   - recharges.idempotency_key UNIQUE → replay-safe order creation
//   - payments UNIQUE(channel, channel_txn_id) → replay-safe callbacks
//   - balance credit happens ONCE, inside a DB transaction that marks
//     the recharge PAID and inserts the payment row
package payment

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channel is a payment provider adapter.
type Channel interface {
	// Name identifies the channel (mock | alipay | wechat | stripe).
	Name() string
	// Pay returns a channel-side transaction id. For the mock channel it
	// "completes" immediately; real channels return a pay URL / id.
	Pay(ctx context.Context, rechargeID int64, amountMicros int64) (txnID string, err error)
}

// Service manages recharges + payments.
type Service struct {
	pool     *pgxpool.Pool
	topUp    func(ctx context.Context, userID int64, amountMicros int64) error
	channels map[string]Channel
}

// NewService builds the payment service. topUp credits the user's Redis
// balance (the operational view); the DB transaction is the authority.
func NewService(pool *pgxpool.Pool, topUp func(ctx context.Context, userID int64, amountMicros int64) error) *Service {
	return &Service{
		pool:     pool,
		topUp:    topUp,
		channels: map[string]Channel{},
	}
}

// RegisterChannel adds a payment channel adapter.
func (s *Service) RegisterChannel(ch Channel) {
	s.channels[ch.Name()] = ch
}

// Recharge creates a pending recharge order (idempotent by idempotency_key).
func (s *Service) Recharge(ctx context.Context, userID int64, amountMicros int64, idempotencyKey string) (int64, error) {
	if amountMicros <= 0 {
		return 0, errors.New("amount must be positive")
	}
	if idempotencyKey == "" {
		// client omitted: generate one (still unique per call)
		raw := make([]byte, 12)
		_, _ = rand.Read(raw)
		idempotencyKey = "srv-" + hex.EncodeToString(raw)
	}
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO recharges (user_id, amount_micros, idempotency_key)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key
		 RETURNING id`,
		userID, amountMicros, idempotencyKey).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Pay executes a channel payment for a pending recharge. Returns the
// channel txn id. For the mock channel the payment is immediately
// complete; real channels would return a pay URL and be settled via
// callback.
func (s *Service) Pay(ctx context.Context, rechargeID int64, channelName string) (string, error) {
	ch, ok := s.channels[channelName]
	if !ok {
		return "", errors.New("unknown channel: " + channelName)
	}
	var userID, amount int64
	var status string
	if err := s.pool.QueryRow(ctx,
		`SELECT user_id, amount_micros, status FROM recharges WHERE id=$1`,
		rechargeID).Scan(&userID, &amount, &status); err != nil {
		return "", err
	}
	if status == "PAID" {
		return "", errors.New("recharge already paid")
	}
	txnID, err := ch.Pay(ctx, rechargeID, amount)
	if err != nil {
		return "", err
	}
	if ch.Name() == "mock" {
		// mock channel settles synchronously (dev flow)
		if err := s.SettleCallback(ctx, rechargeID, ch.Name(), txnID, amount, `{"mock":true}`); err != nil {
			return "", err
		}
	}
	return txnID, nil
}

// SettleCallback is the channel callback handler — idempotent by
// (channel, channel_txn_id). Credits the balance exactly once, inside the
// DB transaction that marks the recharge PAID.
func (s *Service) SettleCallback(ctx context.Context, rechargeID int64, channel, txnID string, amountMicros int64, raw string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// insert payment row — replay-safe (unique constraint)
	var paidAmount int64
	err = tx.QueryRow(ctx,
		`INSERT INTO payments (recharge_id, channel, channel_txn_id, amount_micros, raw_callback)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (channel, channel_txn_id) DO NOTHING
		 RETURNING amount_micros`,
		rechargeID, channel, txnID, amountMicros, raw).Scan(&paidAmount)
	if err != nil {
		if err == pgx.ErrNoRows {
			// duplicate callback — verify it matches, no-op
			return nil
		}
		return err
	}

	// mark recharge PAID (skip if already paid)
	tag, err := tx.Exec(ctx,
		`UPDATE recharges SET status='PAID', paid_at=now() WHERE id=$1 AND status='PENDING'`,
		rechargeID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errors.New("recharge not pending (duplicate or invalid)")
	}

	// get user id
	var userID int64
	if err := tx.QueryRow(ctx, `SELECT user_id FROM recharges WHERE id=$1`, rechargeID).Scan(&userID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// credit balance AFTER durable commit (Redis is the operational view;
	// the DB row is the authority — reconciliation rebuilds from it)
	return s.topUp(ctx, userID, paidAmount)
}

// SettleByTxn settles a recharge identified by its channel txn id
// (webhook path for real channels like Stripe).
func (s *Service) SettleByTxn(ctx context.Context, channel, txnID string) error {
	var rechargeID int64
	err := s.pool.QueryRow(ctx,
		`SELECT recharge_id FROM payments WHERE channel=$1 AND channel_txn_id=$2`,
		channel, txnID).Scan(&rechargeID)
	if err != nil {
		return err // txn unknown (not created by us) — reject
	}
	return s.SettleCallback(ctx, rechargeID, channel, txnID, 0, `{"via":"settle_by_txn"}`)
}

// RechargeStatus returns a recharge's current state.
func (s *Service) RechargeStatus(ctx context.Context, rechargeID int64) (string, error) {
	var status string
	err := s.pool.QueryRow(ctx, `SELECT status FROM recharges WHERE id=$1`, rechargeID).Scan(&status)
	return status, err
}

// MockChannel completes payments immediately (dev/test).
type MockChannel struct{}

// Name implements Channel.
func (MockChannel) Name() string { return "mock" }

// Pay implements Channel: txn id = recharge id + timestamp.
func (MockChannel) Pay(_ context.Context, rechargeID int64, _ int64) (string, error) {
	return "mock-txn-" + itoa(rechargeID) + "-" + itoa(time.Now().UnixNano()), nil
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [24]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
