package billing

import (
	"context"
	"testing"

	"github.com/differs/MeterGate/internal/metering"
)

// TestSettleUsesRequestStartSnapshot: a mid-request price change must NOT
// reprice an in-flight request — settlement uses the frozen snapshot.
func TestSettleUsesRequestStartSnapshot(t *testing.T) {
	store := newMemStore()
	settler := NewSettler(store, nil, testLogger(), 10)
	defer settler.Close()
	ctx := context.Background()

	// Request starts at $10/1M output...
	ev := testEvent("req-snap-1")
	ev.Pricing = &metering.PricingSnapshot{
		InputPer1M:  2_000_000,  // $2
		OutputPer1M: 10_000_000, // $10
	}
	ev.PromptTokens = 1000
	ev.CompletionTokens = 1000

	// ...then the price table changes to $8/1M BEFORE settlement.
	old := PriceFor("gpt-4o")
	UpdatePrice("gpt-4o", ModelPrice{InputPer1M: 2_000_000, OutputPer1M: 8_000_000})
	defer UpdatePrice("gpt-4o", old)

	if err := settler.Handle(ctx, ev); err != nil {
		t.Fatal(err)
	}
	settler.FlushSync(ctx)

	// Charged at the REQUEST-START price: 1000*2 + 1000*10 = 12,000,000 µ
	want := int64(12_000) // 1000*2 + 1000*10 = 12,000 µ
	if store.orders[0].AmountMicros != want {
		t.Fatalf("amount = %d, want %d (request-start price must win)", store.orders[0].AmountMicros, want)
	}
}

// TestSettleFallsBackToCurrentPrice: legacy events without a snapshot use
// the current table (backwards compatible).
func TestSettleFallsBackToCurrentPrice(t *testing.T) {
	store := newMemStore()
	settler := NewSettler(store, nil, testLogger(), 10)
	defer settler.Close()
	ctx := context.Background()

	ev := testEvent("req-legacy-1") // no Pricing snapshot
	ev.PromptTokens = 1000
	ev.CompletionTokens = 1000
	if err := settler.Handle(ctx, ev); err != nil {
		t.Fatal(err)
	}
	settler.FlushSync(ctx)

	p := PriceFor("gpt-4o")
	want := CalculateAmount(1000, 1000, p)
	if store.orders[0].AmountMicros != want {
		t.Fatalf("amount = %d, want %d", store.orders[0].AmountMicros, want)
	}
}
