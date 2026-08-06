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
