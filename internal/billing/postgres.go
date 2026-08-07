package billing

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresOrderStore implements OrderStore on PostgreSQL.
// The request_id PRIMARY KEY is the idempotency guard: INSERT ... ON
// CONFLICT DO NOTHING makes the settle path replay-safe.
type PostgresOrderStore struct {
	pool *pgxpool.Pool
}

// NewPostgresOrderStore builds the store and applies the schema
// (idempotent DDL via CREATE TABLE IF NOT EXISTS).
func NewPostgresOrderStore(ctx context.Context, dsn string) (*PostgresOrderStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := applySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresOrderStore{pool: pool}, nil
}

func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	schema, err := SchemaSQL()
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, schema)
	return err
}

// InsertOrder implements OrderStore (idempotent).
func (s *PostgresOrderStore) InsertOrder(ctx context.Context, o Order) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO orders
			(request_id, user_id, model, provider, status,
			 prompt_tokens, completion_tokens, total_tokens,
			 amount_micros, duration_ms, ttft_ms, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (request_id) DO NOTHING`,
		o.RequestID, o.UserID, o.Model, o.Provider, o.Status,
		o.PromptTokens, o.CompletionTokens, o.TotalTokens,
		o.AmountMicros, o.DurationMs, o.TTFTMs, time.Now())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Summary implements OrderStore: per-status aggregates for one day
// (YYYY-MM-DD, interpreted in UTC).
func (s *PostgresOrderStore) Summary(ctx context.Context, day string) (map[string]DaySummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status,
		       COUNT(*), COALESCE(SUM(total_tokens),0), COALESCE(SUM(amount_micros),0)
		FROM orders
		WHERE created_at >= $1::date AND created_at < $1::date + INTERVAL '1 day'
		GROUP BY status`, day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]DaySummary{}
	for rows.Next() {
		var st string
		var d DaySummary
		if err := rows.Scan(&st, &d.Count, &d.TotalTokens, &d.AmountMicros); err != nil {
			return nil, err
		}
		out[st] = d
	}
	return out, rows.Err()
}

// InsertRefund implements RefundStore (idempotent by idempotency_key).
// created=false means the row already existed — the caller must not
// re-execute the refund (prevents double credit on reconcile re-runs).
func (s *PostgresOrderStore) InsertRefund(ctx context.Context, r Refund) (int64, bool, error) {
	var id int64
	var created bool
	err := s.pool.QueryRow(ctx, `
		INSERT INTO refunds
			(request_id, user_id, order_id, reason_code, amount_micros,
			 direction, status, approval_level, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id, (xmax = 0) AS created`,
		r.RequestID, r.UserID, r.OrderID, r.ReasonCode,
		r.AmountMicros, r.Direction, r.Status, r.ApprovalLevel, r.IdempotencyKey).Scan(&id, &created)
	if err != nil {
		return 0, false, err
	}
	return id, created, nil
}

// ListRefunds implements RefundStore.
func (s *PostgresOrderStore) ListRefunds(ctx context.Context, limit int) ([]Refund, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, request_id, user_id, order_id, reason_code, amount_micros,
		       direction, status, approval_level, idempotency_key, created_at
		FROM refunds ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Refund
	for rows.Next() {
		var r Refund
		if err := rows.Scan(&r.ID, &r.RequestID, &r.UserID, &r.OrderID, &r.ReasonCode,
			&r.AmountMicros, &r.Direction, &r.Status, &r.ApprovalLevel,
			&r.IdempotencyKey, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkExecuted implements RefundStore.
func (s *PostgresOrderStore) MarkExecuted(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE refunds SET status = 'EXECUTED' WHERE id = $1 AND status = 'PENDING'`, id)
	return err
}

// Anomalies returns human-readable anomaly descriptions for a day:
// negative amounts, empty request IDs, non-terminal statuses.
func (s *PostgresOrderStore) Anomalies(ctx context.Context, day string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT request_id, status, amount_micros, prompt_tokens, completion_tokens
		FROM orders
		WHERE created_at >= $1::date AND created_at < $1::date + INTERVAL '1 day'
		  AND (amount_micros < 0 OR request_id = '' OR status NOT IN ('SETTLED','NO_CHARGE'))
		LIMIT 100`, day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var rid, st string
		var amt, pt, ct int64
		if err := rows.Scan(&rid, &st, &amt, &pt, &ct); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("request=%s status=%s amount=%d prompt=%d completion=%d",
			rid, st, amt, pt, ct))
	}
	return out, rows.Err()
}

// NegativeAmountOrders implements OrderStore: orders charged below zero
// (settle bugs) — every one of them is refundable.
func (s *PostgresOrderStore) NegativeAmountOrders(ctx context.Context, day string) ([]Order, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT request_id, user_id, model, provider, status,
		       prompt_tokens, completion_tokens, total_tokens,
		       amount_micros, duration_ms, ttft_ms, created_at
		FROM orders
		WHERE created_at >= $1::date AND created_at < $1::date + INTERVAL '1 day'
		  AND amount_micros < 0
		LIMIT 1000`, day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.RequestID, &o.UserID, &o.Model, &o.Provider, &o.Status,
			&o.PromptTokens, &o.CompletionTokens, &o.TotalTokens,
			&o.AmountMicros, &o.DurationMs, &o.TTFTMs, &o.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// Close releases the pool.
func (s *PostgresOrderStore) Close() { s.pool.Close() }

// SchemaSQL returns the embedded schema (single statement batch).
func SchemaSQL() (string, error) {
	if schemaSQL == "" {
		return "", errors.New("schema.sql empty")
	}
	return schemaSQL, nil
}

//go:embed schema.sql
var schemaSQL string

var _ OrderStore = (*PostgresOrderStore)(nil)
var _ = pgx.ErrNoRows // keep pgx import for future use
