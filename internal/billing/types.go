// Package billing implements MeterGate's dual-track billing engine:
//
//   - Fast path (pre-charge): atomic Redis Lua deduction before the request
//     hits the upstream — prevents overselling a user's balance.
//   - Slow path (settle): consumes request-level metering events and writes
//     terminal-state order rows into PostgreSQL — exact, idempotent,
//     append-only. Reconciliation later cross-checks the two tracks.
//
// Money is represented as int64 micro-units ("micros", 1e-6 of the base
// currency unit) everywhere to avoid float drift.
package billing

import (
	"context"
	"time"
)

// Terminal order statuses. Orders are inserted in their terminal state —
// there is no PENDING → SETTLED transition (the pre-charge already guarded
// the balance; settle is the single source of truth).
const (
	StatusSettled  = "SETTLED"
	StatusNoCharge = "NO_CHARGE" // upstream error / zero-completion insurance
)

// Order is the terminal bill row for one request.
type Order struct {
	RequestID        string
	UserID           string
	Model            string
	Provider         string
	Status           string
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	AmountMicros     int64 // user-side charge (micro-units)
	DurationMs       int64
	TTFTMs           int64
	CreatedAt        time.Time
}

// OrderStore persists terminal orders (PostgreSQL in production).
type OrderStore interface {
	// InsertOrder is idempotent: a duplicate request_id is a no-op.
	InsertOrder(ctx context.Context, o Order) (inserted bool, err error)
	// InsertOrders batch-inserts terminal orders (M5: multi-row INSERT,
	// one commit per batch). Duplicates are skipped at the statement level.
	InsertOrders(ctx context.Context, orders []Order) error
	// Summary returns per-status aggregates for the given day (reconcile).
	Summary(ctx context.Context, day string) (map[string]DaySummary, error)
	// Anomalies returns human-readable anomaly descriptions for a day.
	Anomalies(ctx context.Context, day string) ([]string, error)
	// NegativeAmountOrders returns orders with amount < 0 (refundable).
	NegativeAmountOrders(ctx context.Context, day string) ([]Order, error)
}

// DaySummary aggregates one order status for one day.
type DaySummary struct {
	Count        int64
	TotalTokens  int64
	AmountMicros int64
}
