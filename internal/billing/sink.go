package billing

import (
	"context"
	"log/slog"

	"github.com/differs/MeterGate/internal/metering"
	"github.com/differs/MeterGate/internal/obs"
)

// Sink is a metering.Sink backed by an in-process channel feeding the
// Settler. It decouples the gateway hot path from PostgreSQL: the gateway
// never blocks on bill writes (batch-commit lands in M5 with Kafka).
//
// The channel is buffered; if the buffer fills (settler slower than the
// gateway), events are DROPPED and counted — the gateway must never back
// up on billing. Lost events are recoverable from the audit log / upstream
// usage in the L2 reconciliation pipeline.
type Sink struct {
	ch      chan metering.Event
	settler *Settler
	log     *slog.Logger
	metrics *obs.Metrics // optional
	dropped chan int64   // receives drop counters for tests/observability
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewSink starts a background consumer goroutine.
// buffer: event queue depth (default 100_000; each event ~300B).
func NewSink(ctx context.Context, settler *Settler, log *slog.Logger, buffer int) *Sink {
	if buffer <= 0 {
		buffer = 500_000
	}
	sctx, cancel := context.WithCancel(ctx)
	s := &Sink{
		ch:      make(chan metering.Event, buffer),
		settler: settler,
		log:     log,
		dropped: make(chan int64, 1),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go s.loop(sctx)
	return s
}

func (s *Sink) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-s.ch:
			s.handle(ev)
		}
	}
}

// Close stops the consumers and drains remaining events (best effort).
func (s *Sink) Close() {
	s.cancel()
	// drain leftovers
	for {
		select {
		case ev := <-s.ch:
			s.handle(ev)
		default:
			close(s.done)
			return
		}
	}
}

func (s *Sink) handle(ev metering.Event) {
	if err := s.settler.Handle(context.Background(), ev); err != nil {
		s.log.Error("settle failed", "request_id", ev.RequestID, "err", err)
	}
}

// WithMetrics attaches Prometheus instrumentation.
func (s *Sink) WithMetrics(m *obs.Metrics) *Sink {
	s.metrics = m
	return s
}

// Emit implements metering.Sink. Non-blocking; drops on full buffer.
func (s *Sink) Emit(ev metering.Event) error {
	select {
	case s.ch <- ev:
		return nil
	default:
		if s.metrics != nil {
			s.metrics.SinkDroppedTotal.Inc()
		}
		s.log.Warn("metering sink buffer full, event dropped", "request_id", ev.RequestID)
		return nil
	}
}

var _ metering.Sink = (*Sink)(nil)
