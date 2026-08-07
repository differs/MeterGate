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
	mu     sync.Mutex
	orders []Order
	byID   map[string]struct{}
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
