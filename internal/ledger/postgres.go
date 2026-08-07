package ledger

import (
	"context"

	"github.com/differs/MeterGate/internal/billing"
)

// PostgresAdapter is the default LedgerAdapter backed by PostgreSQL.
// All interface methods are promoted from the embedded store (orders,
// refunds, Balance, Replay live in the billing package where the pool
// is accessible); this type exists purely as the adapter boundary for
// future backends (TigerBeetle).
type PostgresAdapter struct {
	*billing.PostgresOrderStore
}

// NewPostgresAdapter connects, applies the schema and wraps the store.
func NewPostgresAdapter(ctx context.Context, dsn string) (*PostgresAdapter, error) {
	store, err := billing.NewPostgresOrderStore(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresAdapter{PostgresOrderStore: store}, nil
}

var _ Adapter = (*PostgresAdapter)(nil)
