package billing

import (
	"context"
	"errors"
	"log/slog"

	"github.com/differs/MeterGate/internal/metering"
)

// Settler consumes request-level metering events and persists terminal
// orders. It is the slow-path "exact accumulation" track:
//
//	event ──▶ price (snapshot) ──▶ terminal order (idempotent INSERT)
//	       ──▶ pre-charge settle (Redis, refund remainder)
//
// Idempotency: orders.request_id is UNIQUE; the settle path is safe to
// retry/replay without double charging.
type Settler struct {
	store   OrderStore
	pre     *Precharger // nil disables the Redis settle step
	log     *slog.Logger
	batch   int
	onEvent func(metering.Event) // test hook / observability
}

// NewSettler builds a Settler. batch controls how many orders are grouped
// per commit (batch-commit pipeline; M5 lifts this into Kafka workers).
func NewSettler(store OrderStore, pre *Precharger, log *slog.Logger, batch int) *Settler {
	if batch <= 0 {
		batch = 100
	}
	return &Settler{store: store, pre: pre, log: log, batch: batch}
}

// Handle processes one metering event. Called from the event sink.
func (s *Settler) Handle(ctx context.Context, ev metering.Event) error {
	if ev.RequestID == "" {
		return errors.New("metering event missing request_id")
	}

	status := StatusSettled
	completion := int64(ev.CompletionTokens)
	if ev.Status == metering.StatusFailed {
		// Zero-completion insurance: no charge for requests that never
		// produced output. Aborted streams still bill partial tokens.
		status = StatusNoCharge
		completion = 0
	}

	p := PriceFor(ev.Model)
	amount := CalculateAmount(int64(ev.PromptTokens), completion, p)

	inserted, err := s.store.InsertOrder(ctx, Order{
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
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil // duplicate event, already settled
	}

	if s.pre != nil {
		charged := amount
		if status == StatusNoCharge {
			charged = 0
		}
		if err := s.pre.Settle(ctx, ev.UserID, ev.RequestID, charged); err != nil {
			s.log.Warn("precharge settle failed (will be swept by reconcile)",
				"request_id", ev.RequestID, "err", err)
		}
	}
	if s.onEvent != nil {
		s.onEvent(ev)
	}
	s.log.Debug("order settled", "request_id", ev.RequestID, "amount_micros", amount)
	return nil
}
