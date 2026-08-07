package reconciliation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/billing"
)

// fakeStore implements OrderStore + RefundStore in memory for tests.
type fakeStore struct {
	billing.OrderStore
	orders  []billing.Order
	refunds []billing.Refund
}

func (f *fakeStore) InsertOrder(_ context.Context, o billing.Order) (bool, error) {
	for _, ex := range f.orders {
		if ex.RequestID == o.RequestID {
			return false, nil
		}
	}
	f.orders = append(f.orders, o)
	return true, nil
}

func (f *fakeStore) Summary(_ context.Context, _ string) (map[string]billing.DaySummary, error) {
	out := map[string]billing.DaySummary{}
	for _, o := range f.orders {
		s := out[o.Status]
		s.Count++
		s.TotalTokens += o.TotalTokens
		s.AmountMicros += o.AmountMicros
		out[o.Status] = s
	}
	return out, nil
}

func (f *fakeStore) Anomalies(_ context.Context, _ string) ([]string, error) {
	var out []string
	for _, o := range f.orders {
		if o.AmountMicros < 0 {
			out = append(out, "negative:"+o.RequestID)
		}
	}
	return out, nil
}

func (f *fakeStore) NegativeAmountOrders(_ context.Context, _ string) ([]billing.Order, error) {
	var out []billing.Order
	for _, o := range f.orders {
		if o.AmountMicros < 0 {
			out = append(out, o)
		}
	}
	return out, nil
}

func (f *fakeStore) InsertRefund(_ context.Context, r billing.Refund) (int64, bool, error) {
	for _, ex := range f.refunds {
		if ex.IdempotencyKey == r.IdempotencyKey {
			return ex.ID, false, nil
		}
	}
	r.ID = int64(len(f.refunds) + 1)
	f.refunds = append(f.refunds, r)
	return r.ID, true, nil
}

func (f *fakeStore) ListRefunds(_ context.Context, _ int) ([]billing.Refund, error) {
	return f.refunds, nil
}

func (f *fakeStore) MarkExecuted(_ context.Context, id int64) error {
	for i := range f.refunds {
		if f.refunds[i].ID == id {
			f.refunds[i].Status = billing.RefundExecuted
		}
	}
	return nil
}

func TestAutoRefundNegativeAmounts(t *testing.T) {
	mr := miniredis.RunT(t)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	pre := billing.NewPrecharger(rdb)
	ctx := context.Background()
	_ = pre.TopUp(ctx, "u1", 1_000_000)

	store := &fakeStore{}
	// negative order: user was charged -500 micros (a settle bug)
	_, _ = store.InsertOrder(ctx, billing.Order{
		RequestID: "req-bad", UserID: "u1", Model: "m", Status: billing.StatusSettled,
		PromptTokens: 10, CompletionTokens: 10, TotalTokens: 20, AmountMicros: -500,
	})

	recon := New(store, store, pre, slog.New(slog.DiscardHandler))
	recon.AutoThresholdMicros = 1_000 // anything above → manual

	rep, err := recon.RunDay(ctx, "2026-08-06", true)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RefundsAuto != 1 {
		t.Fatalf("auto refunds = %d, want 1", rep.RefundsAuto)
	}
	if rep.Anomalies != 1 {
		t.Fatalf("anomalies = %d, want 1", rep.Anomalies)
	}
	// user balance credited by 500
	bal, _ := pre.Balance(ctx, "u1")
	if bal != 1_000_500 {
		t.Fatalf("balance = %d, want 1000500", bal)
	}
	// refund row executed
	if store.refunds[0].Status != billing.RefundExecuted {
		t.Fatalf("refund status = %s, want EXECUTED", store.refunds[0].Status)
	}

	// Idempotency: run again → no duplicate refund, no double credit.
	rep2, err := recon.RunDay(ctx, "2026-08-06", true)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.RefundsAuto != 0 {
		t.Fatalf("second run auto refunds = %d, want 0 (idempotent)", rep2.RefundsAuto)
	}
	bal, _ = pre.Balance(ctx, "u1")
	if bal != 1_000_500 {
		t.Fatalf("balance after rerun = %d, want 1000500 (no double credit)", bal)
	}
}

func TestLargeRefundRequiresManual(t *testing.T) {
	store := &fakeStore{}
	ctx := context.Background()
	_, _ = store.InsertOrder(ctx, billing.Order{
		RequestID: "req-huge", UserID: "u1", Model: "m", Status: billing.StatusSettled,
		AmountMicros: -500_000_000, // 500 units — above threshold
	})
	recon := New(store, store, nil, slog.New(slog.DiscardHandler))
	recon.AutoThresholdMicros = 1_000

	rep, err := recon.RunDay(ctx, "2026-08-06", true)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RefundsAuto != 0 || rep.RefundsManual != 1 {
		t.Fatalf("auto=%d manual=%d, want 0/1", rep.RefundsAuto, rep.RefundsManual)
	}
	if store.refunds[0].Status != billing.RefundPending {
		t.Fatalf("large refund must pend for manual approval, got %s", store.refunds[0].Status)
	}
}

func TestValidateDay(t *testing.T) {
	if _, err := ValidateDay("2026-08-06"); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateDay("08/06/2026"); err == nil {
		t.Fatal("bad format must fail")
	}
}
