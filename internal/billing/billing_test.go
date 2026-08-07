package billing

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return mr, rdb
}

func TestCalculateAmount(t *testing.T) {
	p := ModelPrice{InputPer1M: 2_000_000, OutputPer1M: 8_000_000}
	// 1000 prompt + 500 completion @ $2/$8 per 1M
	// = 1000*2 + 500*8 = 6000 micros ($0.006)
	if got := CalculateAmount(1000, 500, p); got != 6000 {
		t.Fatalf("amount = %d, want 6000", got)
	}
}

func TestEstimatePreChargeCapsMaxTokens(t *testing.T) {
	p := ModelPrice{InputPer1M: 2_000_000, OutputPer1M: 8_000_000}
	huge := int64(1_000_000)
	got := EstimatePreCharge(1000, &huge, p)
	// capped at 16000 completion: (1000*2 + 16000*8)/1M * 1.1 = (0.002+0.128) * 1.1
	if got > 150_000 {
		t.Fatalf("estimate = %d, want <= 150000 (cap must apply)", got)
	}
}

func TestPreChargeSettleRoundTrip(t *testing.T) {
	mr, rdb := newTestRedis(t)
	defer mr.Close()
	pre := NewPrecharger(rdb)
	ctx := context.Background()

	if err := pre.TopUp(ctx, "u1", 1_000_000); err != nil {
		t.Fatal(err)
	}
	// 1) pre-charge 200_000
	if err := pre.PreCharge(ctx, "u1", "req-1", 200_000); err != nil {
		t.Fatal(err)
	}
	if bal, _ := pre.Balance(ctx, "u1"); bal != 800_000 {
		t.Fatalf("balance = %d, want 800000", bal)
	}
	if fr, _ := pre.FrozenBalance(ctx); fr != 200_000 {
		t.Fatalf("frozen = %d, want 200000", fr)
	}

	// 2) settle with actual charge 80_000 → refund 120_000
	if err := pre.Settle(ctx, "u1", "req-1", 80_000); err != nil {
		t.Fatal(err)
	}
	if bal, _ := pre.Balance(ctx, "u1"); bal != 920_000 {
		t.Fatalf("balance = %d, want 920000", bal)
	}
	if fr, _ := pre.FrozenBalance(ctx); fr != 0 {
		t.Fatalf("frozen = %d, want 0", fr)
	}
}

func TestPreChargeInsufficient(t *testing.T) {
	mr, rdb := newTestRedis(t)
	defer mr.Close()
	pre := NewPrecharger(rdb)
	ctx := context.Background()

	if err := pre.TopUp(ctx, "u1", 100); err != nil {
		t.Fatal(err)
	}
	err := pre.PreCharge(ctx, "u1", "req-1", 1_000_000)
	if err != ErrInsufficientBalance {
		t.Fatalf("err = %v, want ErrInsufficientBalance", err)
	}
}

// TestPreChargeClawback: when the actual charge EXCEEDS the pre-charge
// estimate (long streaming responses), the shortfall is deducted from the
// balance — the user must never pay less than the metered amount.
func TestPreChargeClawback(t *testing.T) {
	mr, rdb := newTestRedis(t)
	defer mr.Close()
	pre := NewPrecharger(rdb)
	ctx := context.Background()

	if err := pre.TopUp(ctx, "u1", 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := pre.PreCharge(ctx, "u1", "req-1", 200); err != nil {
		t.Fatal(err)
	}
	// charge 5000 >> pre-charge 200
	if err := pre.Settle(ctx, "u1", "req-1", 5000); err != nil {
		t.Fatal(err)
	}
	if bal, _ := pre.Balance(ctx, "u1"); bal != 1_000_000-5000 {
		t.Fatalf("balance = %d, want %d (shortfall must be clawed back)", bal, 1_000_000-5000)
	}
	if fr, _ := pre.FrozenBalance(ctx); fr != 0 {
		t.Fatalf("frozen = %d, want 0", fr)
	}
}

func TestPreChargeNoChargeFullRelease(t *testing.T) {
	mr, rdb := newTestRedis(t)
	defer mr.Close()
	pre := NewPrecharger(rdb)
	ctx := context.Background()

	if err := pre.TopUp(ctx, "u1", 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := pre.PreCharge(ctx, "u1", "req-1", 500_000); err != nil {
		t.Fatal(err)
	}
	// zero-completion insurance: charged = 0 → full release
	if err := pre.Settle(ctx, "u1", "req-1", 0); err != nil {
		t.Fatal(err)
	}
	if bal, _ := pre.Balance(ctx, "u1"); bal != 1_000_000 {
		t.Fatalf("balance = %d, want 1000000 (full refund)", bal)
	}
}

func TestSettleIdempotent(t *testing.T) {
	store := &memStore{}
	settler := NewSettler(store, nil, testLogger(), 10)
	ctx := context.Background()

	ev := testEvent("req-1")
	if err := settler.Handle(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if err := settler.Handle(ctx, ev); err != nil {
		t.Fatal(err)
	}
	settler.Close() // flushes the batch synchronously
	if len(store.orders) != 1 {
		t.Fatalf("orders = %d, want 1 (idempotent)", len(store.orders))
	}
	o := store.orders[0]
	if o.Status != StatusSettled {
		t.Fatalf("status = %s", o.Status)
	}
	if o.AmountMicros != CalculateAmount(10, 20, PriceFor("gpt-4o")) {
		t.Fatalf("amount = %d", o.AmountMicros)
	}
}

func TestSettleBatch(t *testing.T) {
	store := &memStore{}
	settler := NewSettler(store, nil, testLogger(), 10) // batch size 10
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		if err := settler.Handle(ctx, testEvent(fmt.Sprintf("req-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	settler.Close()
	if len(store.orders) != 25 {
		t.Fatalf("orders = %d, want 25 (batched commits)", len(store.orders))
	}
}
