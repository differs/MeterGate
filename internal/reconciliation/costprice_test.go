package reconciliation

import (
	"context"
	"testing"

	"github.com/differs/MeterGate/internal/billing"
)

// mockModelCostSource returns scripted per-model supplier costs.
type mockModelCostSource struct {
	costs []ProviderModelCost
}

func (m *mockModelCostSource) FetchModelCosts(_ context.Context, _ string, _ string) ([]ProviderModelCost, error) {
	return m.costs, nil
}

// TestCostPriceSyncer: supplier bill ($10 for 1M tokens) → cost price
// table updated so margin analysis uses real supplier pricing.
func TestCostPriceSyncer(t *testing.T) {
	old := billing.CostPriceFor("gpt-4o")
	defer billing.UpdateCostPrice("gpt-4o", old)

	src := &mockModelCostSource{costs: []ProviderModelCost{
		{Provider: "openai", Model: "gpt-4o", Tokens: 2_000_000, CostMicros: 20_000_000},
	}}
	s := NewCostPriceSyncer(src, testLogger())
	n, err := s.SyncDay(context.Background(), "openai", "2026-08-07")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("updated = %d, want 1", n)
	}
	// 20_000_000 micros ($20) / 2M tokens = $10 per 1M tokens
	// = 10_000_000 micros per 1M
	if got := billing.CostPriceFor("gpt-4o").OutputPer1M; got != 10_000_000 {
		t.Fatalf("cost per 1M = %d, want 10000000", got)
	}
}

// TestCostPriceSyncerSkipsZero: zero tokens/cost rows must not poison the table.
func TestCostPriceSyncerSkipsZero(t *testing.T) {
	old := billing.CostPriceFor("gpt-4o")
	defer billing.UpdateCostPrice("gpt-4o", old)

	src := &mockModelCostSource{costs: []ProviderModelCost{
		{Provider: "openai", Model: "gpt-4o", Tokens: 0, CostMicros: 100},
		{Provider: "openai", Model: "gpt-4o", Tokens: 100, CostMicros: 0},
	}}
	s := NewCostPriceSyncer(src, testLogger())
	n, err := s.SyncDay(context.Background(), "openai", "2026-08-07")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("updated = %d, want 0 (zero rows skipped)", n)
	}
}
