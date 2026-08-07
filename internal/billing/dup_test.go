package billing

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestDuplicateEventsNoDoubleCharge: at-least-once Kafka semantics mean
// the same event can be consumed twice (consumer rebalance, commit
// failure, replay). The chain — Settler buffer → PG ON CONFLICT →
// Redis settle idempotency — must never double-charge.
func TestDuplicateEventsNoDoubleCharge(t *testing.T) {
	mr, rdb := newTestRedis(t)
	defer mr.Close()
	pre := NewPrecharger(rdb)
	ctx := context.Background()
	_ = pre.TopUp(ctx, "u1", 100_000_000_000)

	store := newMemStore()
	settler := NewSettler(store, pre, testLogger(), 100)
	defer settler.Close()

	ev := testEvent("req-dup-1")
	ev.PromptTokens = 1000
	ev.CompletionTokens = 1000

	// Real chain: gateway pre-charges first, then the event settles it.
	if err := pre.PreCharge(ctx, "u1", ev.RequestID, 20_000); err != nil {
		t.Fatal(err)
	}

	// Simulate a rebalance: the same event is delivered 5 times across
	// two "consumer instances" (concurrent Handle calls).
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = settler.Handle(ctx, ev)
		}()
	}
	wg.Wait()
	settler.FlushSync(ctx)

	if len(store.orders) != 1 {
		t.Fatalf("orders = %d, want 1 (duplicates must dedupe)", len(store.orders))
	}
	bal, _ := pre.Balance(ctx, "u1")
	want := 100_000_000_000 - store.orders[0].AmountMicros
	if bal != want {
		t.Fatalf("balance = %d, want %d (no double charge)", bal, want)
	}
	if fr, _ := pre.FrozenBalance(ctx); fr != 0 {
		t.Fatalf("frozen = %d, want 0", fr)
	}
}

// TestRebalanceAcrossSettlers: two Settler instances (two consumer
// members) process the same event after a partition reassignment — the
// shared PG + Redis must still produce exactly one charge.
func TestRebalanceAcrossSettlers(t *testing.T) {
	mr, rdb := newTestRedis(t)
	defer mr.Close()
	pre := NewPrecharger(rdb)
	ctx := context.Background()
	_ = pre.TopUp(ctx, "u1", 100_000_000_000)

	store := newMemStore()
	// NOTE: two Settlers share ONE store in production via PG; here we
	// emulate the shared store with a mutex-guarded memStore… but the real
	// dedupe is PG's ON CONFLICT. Simulate by sharing the same memStore.
	s1 := NewSettler(store, pre, testLogger(), 50)
	defer s1.Close()
	s2 := NewSettler(store, pre, testLogger(), 50)
	defer s2.Close()

	// Same event delivered to both "instances" (rebalance overlap window).
	for i := 0; i < 100; i++ {
		ev := testEvent(fmt.Sprintf("req-rb-%d", i))
		ev.PromptTokens = 100
		ev.CompletionTokens = 100
		_ = s1.Handle(ctx, ev)
		_ = s2.Handle(ctx, ev) // duplicate delivery to the other member
	}
	s1.FlushSync(ctx)
	s2.FlushSync(ctx)

	if len(store.orders) != 100 {
		t.Fatalf("orders = %d, want 100 (one per request_id)", len(store.orders))
	}
}
