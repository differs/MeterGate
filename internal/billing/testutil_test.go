package billing

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/differs/MeterGate/internal/metering"
)

// memStore is an in-memory OrderStore for tests.
type memStore struct {
	mu     sync.Mutex
	orders []Order
}

func (m *memStore) InsertOrder(_ context.Context, o Order) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ex := range m.orders {
		if ex.RequestID == o.RequestID {
			return false, nil
		}
	}
	o.CreatedAt = time.Now()
	m.orders = append(m.orders, o)
	return true, nil
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
