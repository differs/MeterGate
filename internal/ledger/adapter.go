// Package ledger defines the LedgerAdapter — the seam where the billing
// stack talks to the money store. PostgreSQL is the default implementation;
// a TigerBeetle adapter can be swapped in at this boundary without touching
// callers (L4 of the architecture: ledger externality).
//
// The interface is deliberately the union of what the platform needs:
// order persistence (OrderStore), adjustment entries (RefundStore), live
// balance reads and time-window replay (reconciliation / balance rebuild).
package ledger

import (
	"context"
	"time"

	"github.com/differs/MeterGate/internal/billing"
)

// Adapter is the money-store boundary.
type Adapter interface {
	billing.OrderStore
	billing.RefundStore

	// Balance returns the net balance for a user: sum(orders) + sum(refunds)
	// for the user. (The realtime Redis balance remains the operational
	// view; this is the authoritative, replayable figure.)
	Balance(ctx context.Context, userID string) (int64, error)

	// Replay streams all entries (orders + refunds) in a time window,
	// oldest first. Used by reconciliation and balance rebuilds.
	Replay(ctx context.Context, from, to time.Time, fn func(billing.Entry) error) error
}
