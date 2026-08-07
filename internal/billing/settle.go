package billing

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/differs/MeterGate/internal/metering"
)

// Settler consumes request-level metering events and persists terminal
// orders in BATCHES (M5): events accumulate in a buffer and flush every
// `batch` rows or `maxWait`, whichever comes first — one commit per batch
// instead of one per request (fsync 15K/s → ~30/s).
//
//	event ──▶ buffer ──▶ INSERT multi-row (idempotent) ──▶ pre-charge settle
//
// Idempotency: orders.request_id is UNIQUE; batch replay is safe. The
// pre-charge settle step is per-row and itself idempotent (SettleScript
// no-ops for unknown/expired pre-charges).
type Settler struct {
	store   OrderStore
	pre     *Precharger // nil disables the Redis settle step
	log     *slog.Logger
	batch   int
	maxWait time.Duration

	mu      sync.Mutex
	buf     []Order
	pending []metering.Event // events whose orders are in buf (for settle)
	flushAt time.Time
	flushed chan struct{} // closed by the flusher when idle; used for Close
	done    chan struct{}
	wg      sync.WaitGroup
}

// NewSettler builds a Settler with batch-commit semantics.
// batch: rows per commit (default 500). maxWait: flush deadline (default 50ms).
func NewSettler(store OrderStore, pre *Precharger, log *slog.Logger, batch int) *Settler {
	if batch <= 0 {
		batch = 500
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Settler{
		store:   store,
		pre:     pre,
		log:     log,
		batch:   batch,
		maxWait: 50 * time.Millisecond,
		done:    make(chan struct{}),
	}
	s.wg.Add(1)
	go s.flushLoop()
	return s
}

// Handle buffers one metering event. Non-blocking; drops the event when
// the buffer is full (the gateway must never back up on billing — lost
// events are recoverable from audit logs in reconciliation).
func (s *Settler) Handle(ctx context.Context, ev metering.Event) error {
	if ev.RequestID == "" {
		return errors.New("metering event missing request_id")
	}
	o := orderFromEvent(ev)
	if o == nil {
		return nil // failed request → nothing to bill (zero-completion insurance)
	}

	s.mu.Lock()
	s.buf = append(s.buf, *o)
	s.pending = append(s.pending, ev)
	flush := len(s.buf) >= s.batch
	if !flush && s.flushAt.IsZero() {
		s.flushAt = time.Now().Add(s.maxWait)
	}
	s.mu.Unlock()

	if flush {
		s.flush()
	}
	return nil
}

// flushLoop periodically flushes partial batches on the maxWait deadline.
func (s *Settler) flushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.maxWait)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.mu.Lock()
			due := !s.flushAt.IsZero() && time.Now().After(s.flushAt)
			s.mu.Unlock()
			if due {
				s.flush()
			}
		}
	}
}

// flush writes the buffered orders in one batch and settles pre-charges.
func (s *Settler) flush() {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return
	}
	orders := s.buf
	events := s.pending
	s.buf = nil
	s.pending = nil
	s.flushAt = time.Time{}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.store.InsertOrders(ctx, orders); err != nil {
		s.log.Error("batch order insert failed (orders will be lost unless replayed)",
			"count", len(orders), "err", err)
		return
	}
	for i, ev := range events {
		if s.pre != nil {
			charged := orders[i].AmountMicros
			if orders[i].Status == StatusNoCharge {
				charged = 0
			}
			if err := s.pre.Settle(ctx, ev.UserID, ev.RequestID, charged); err != nil {
				s.log.Warn("precharge settle failed (swept by reconcile)",
					"request_id", ev.RequestID, "err", err)
			}
		}
	}
	s.log.Debug("batch settled", "count", len(orders))
}

// Close flushes remaining rows and stops the flusher.
func (s *Settler) Close() {
	close(s.done)
	s.flush()
	s.wg.Wait()
}

// orderFromEvent converts a metering event to a terminal order.
// Failed requests (zero-completion insurance) become NO_CHARGE orders.
func orderFromEvent(ev metering.Event) *Order {
	status := StatusSettled
	completion := int64(ev.CompletionTokens)
	if ev.Status == metering.StatusFailed {
		status = StatusNoCharge
		completion = 0
	}
	p := PriceFor(ev.Model)
	amount := CalculateAmount(int64(ev.PromptTokens), completion, p)
	return &Order{
		RequestID:        ev.RequestID,
		UserID:           ev.UserID,
		Model:            ev.Model,
		Provider:         ev.Provider,
		Status:           status,
		PromptTokens:     int64(ev.PromptTokens),
		CompletionTokens: completion,
		TotalTokens:      int64(ev.PromptTokens) + completion,
		AmountMicros:     amount,
		DurationMs:       ev.DurationMs,
		TTFTMs:           ev.TTFTMs,
	}
}
