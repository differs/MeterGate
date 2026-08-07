package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/reconciliation"
)

// fakeStore implements OrderStore + RefundStore in memory.
type fakeStore struct {
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

func (f *fakeStore) Anomalies(_ context.Context, _ string) ([]string, error) { return nil, nil }

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

func newTestAPI(t *testing.T) (*httptest.Server, *billing.Precharger) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	pre := billing.NewPrecharger(rdb)
	_ = pre.TopUp(context.Background(), "u1", 42_000)

	store := &fakeStore{}
	recon := reconciliation.New(store, store, pre, slog.New(slog.DiscardHandler))
	adm := New(store, store, pre, recon, "admin-secret")
	ts := httptest.NewServer(adm.Handler())
	t.Cleanup(ts.Close)
	return ts, pre
}

func TestAdminAuth(t *testing.T) {
	ts, _ := newTestAPI(t)
	resp, err := http.Get(ts.URL + "/admin/balance?user=u1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestAdminBalance(t *testing.T) {
	ts, _ := newTestAPI(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/admin/balance?user=u1", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["balance_micros"] != float64(42_000) {
		t.Fatalf("balance = %v, want 42000", body["balance_micros"])
	}
}

func TestAdminReconcile(t *testing.T) {
	ts, _ := newTestAPI(t)
	payload := []byte(`{"day":"2026-08-06","auto_refund":true}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/reconcile", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer admin-secret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var rep reconciliation.Report
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		t.Fatal(err)
	}
	if rep.Day != "2026-08-06" {
		t.Fatalf("day = %s", rep.Day)
	}
}
