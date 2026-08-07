package billing

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/differs/MeterGate/internal/metering"
)

// memStore is an in-memory OrderStore for tests (map-backed: O(1) lookups,
// so it does not distort Settler throughput benchmarks).
type memStore struct {
	mu      sync.Mutex
	orders  []Order
	refunds []Refund
	byID    map[string]struct{}
}

func newMemStore() *memStore {
	return &memStore{byID: map[string]struct{}{}}
}

func (m *memStore) InsertOrders(_ context.Context, orders []Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range orders {
		if _, ok := m.byID[o.RequestID]; ok {
			continue
		}
		o.CreatedAt = time.Now()
		m.orders = append(m.orders, o)
		m.byID[o.RequestID] = struct{}{}
	}
	return nil
}

func (m *memStore) InsertOrder(_ context.Context, o Order) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byID[o.RequestID]; ok {
		return false, nil
	}
	o.CreatedAt = time.Now()
	m.orders = append(m.orders, o)
	m.byID[o.RequestID] = struct{}{}
	return true, nil
}

func (m *memStore) Anomalies(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (m *memStore) NegativeAmountOrders(_ context.Context, _ string) ([]Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Order
	for _, o := range m.orders {
		if o.AmountMicros < 0 {
			out = append(out, o)
		}
	}
	return out, nil
}

func (m *memStore) Summary(_ context.Context, _ string) (map[string]DaySummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]DaySummary{}
	for _, o := range m.orders {
		s := out[o.Status]
		s.Count++
		s.TotalTokens += o.TotalTokens
		s.AmountMicros += o.AmountMicros
		out[o.Status] = s
	}
	return out, nil
}

func (m *memStore) SumProviderUsage(_ context.Context, _ string) (map[string]MyUsageRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]MyUsageRow{}
	for _, o := range m.orders {
		u := out[o.Provider]
		u.PromptTok += o.PromptTokens
		u.ComplTok += o.CompletionTokens
		u.TotalTok += o.TotalTokens
		out[o.Provider] = u
	}
	return out, nil
}

func (m *memStore) Balance(_ context.Context, userID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var net int64
	for _, o := range m.orders {
		if o.UserID == userID {
			net += o.AmountMicros
		}
	}
	return net, nil
}

func (m *memStore) Replay(_ context.Context, _ time.Time, _ time.Time, fn func(Entry) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range m.orders {
		if err := fn(Entry{RequestID: o.RequestID, UserID: o.UserID, Model: o.Model,
			Status: o.Status, AmountMicros: o.AmountMicros, Direction: "ORDER"}); err != nil {
			return err
		}
	}
	return nil
}

func (m *memStore) Close() {}

func (m *memStore) InsertRefund(_ context.Context, r Refund) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ex := range m.refunds {
		if ex.IdempotencyKey == r.IdempotencyKey {
			return ex.ID, false, nil
		}
	}
	r.ID = int64(len(m.refunds) + 1)
	m.refunds = append(m.refunds, r)
	return r.ID, true, nil
}

func (m *memStore) ListRefunds(_ context.Context, _ int) ([]Refund, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refunds, nil
}

func (m *memStore) MarkExecuted(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.refunds {
		if m.refunds[i].ID == id {
			m.refunds[i].Status = RefundExecuted
		}
	}
	return nil
}

func (m *memStore) countForTest() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.orders)
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func testEvent(rid string) metering.Event {
	return metering.Event{
		RequestID:        rid,
		UserID:           "u1",
		Model:            "gpt-4o",
		Provider:         "mock",
		Status:           metering.StatusCompleted,
		PromptTokens:     10,
		CompletionTokens: 20,
		Timestamp:        time.Now(),
	}
}
