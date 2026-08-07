package billing

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/differs/MeterGate/internal/metering"
)

// Settler consumes request-level metering events and persists terminal
// orders in BATCHES: events accumulate in per-shard buffers and flush
// every `batch` rows or `maxWait`, whichever comes first — one commit per
// batch instead of one per request (fsync 15K/s → ~30/s).
//
// Concurrency: the buffer is SHARDED (default: NumCPU shards, each with
// its own mutex + flusher goroutine). Events are routed by request_id
// hash, so parallel event producers never contend on a single lock — the
// load-test bottleneck that capped throughput at ~5.5K req/s.
//
//	event ──▶ shard(hash) ──▶ batch INSERT (idempotent) ──▶ pre-charge settle
//
// Idempotency: orders.request_id is UNIQUE; batch replay is safe.
type Settler struct {
	store   OrderStore
	pre     *Precharger // nil disables the Redis settle step
	log     *slog.Logger
	batch   int
	maxWait time.Duration
	shards  []*settleShard
	wg      sync.WaitGroup
}

type settleShard struct {
	mu      sync.Mutex
	buf     []Order
	pending []metering.Event
	flushAt time.Time
	trigger chan struct{}
	done    chan struct{}
}

// NewSettler builds a Settler with batched, sharded writes.
// batch: rows per commit (default 500). maxWait: flush deadline (default 50ms).
func NewSettler(store OrderStore, pre *Precharger, log *slog.Logger, batch int) *Settler {
	if batch <= 0 {
		batch = 500
	}
	if log == nil {
		log = slog.Default()
	}
	n := runtime.NumCPU()
	if n < 4 {
		n = 4
	}
	if n > 32 {
		n = 32
	}
	s := &Settler{
		store:   store,
		pre:     pre,
		log:     log,
		batch:   batch,
		maxWait: 50 * time.Millisecond,
		shards:  make([]*settleShard, n),
	}
	for i := range s.shards {
		sh := &settleShard{
			trigger: make(chan struct{}, 1),
			done:    make(chan struct{}),
		}
		s.shards[i] = sh
		s.wg.Add(1)
		go s.flushLoop(sh)
	}
	return s
}

// Handle buffers one metering event. Non-blocking; drops the event when
// the shard buffer is full (the gateway must never back up on billing —
// lost events are recoverable from audit logs in reconciliation).
func (s *Settler) Handle(ctx context.Context, ev metering.Event) error {
	if ev.RequestID == "" {
		return errors.New("metering event missing request_id")
	}
	o := orderFromEvent(ev)
	if o == nil {
		return nil // failed request → nothing to bill
	}
	sh := s.shards[shardIndex(ev.RequestID, len(s.shards))]
	sh.mu.Lock()
	sh.buf = append(sh.buf, *o)
	sh.pending = append(sh.pending, ev)
	full := len(sh.buf) >= s.batch
	if !full && sh.flushAt.IsZero() {
		sh.flushAt = time.Now().Add(s.maxWait)
	}
	sh.mu.Unlock()
	if full {
		s.signalFlush(sh)
	}
	return nil
}

func (s *Settler) signalFlush(sh *settleShard) {
	select {
	case sh.trigger <- struct{}{}:
	default:
	}
}

// flushLoop is one shard's flusher: triggered by batch-full signals and
// the maxWait deadline. All PG/Redis I/O happens here, off the event
// consumption path.
func (s *Settler) flushLoop(sh *settleShard) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.maxWait)
	defer ticker.Stop()
	for {
		select {
		case <-sh.done:
			return
		case <-sh.trigger:
			s.flush(sh)
		case <-ticker.C:
			sh.mu.Lock()
			due := !sh.flushAt.IsZero() && time.Now().After(sh.flushAt)
			sh.mu.Unlock()
			if due {
				s.flush(sh)
			}
		}
	}
}

// flush writes the shard's buffered orders in one batch and settles
// pre-charges (one Redis pipeline round-trip).
func (s *Settler) flush(sh *settleShard) {
	sh.mu.Lock()
	if len(sh.buf) == 0 {
		sh.mu.Unlock()
		return
	}
	orders := sh.buf
	events := sh.pending
	sh.buf = nil
	sh.pending = nil
	sh.flushAt = time.Time{}
	sh.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.store.InsertOrders(ctx, orders); err != nil {
		s.log.Error("batch order insert failed (orders lost unless replayed)",
			"count", len(orders), "err", err)
		return
	}
	if s.pre != nil {
		settles := make([]SettleReq, len(events))
		for i, ev := range events {
			charged := orders[i].AmountMicros
			if orders[i].Status == StatusNoCharge {
				charged = 0
			}
			settles[i] = SettleReq{UserID: ev.UserID, RequestID: ev.RequestID, ChargedMicros: charged}
		}
		s.pre.BatchSettle(ctx, settles)
	}
	s.log.Debug("batch settled", "shard", shardOf(sh, s.shards), "count", len(orders))
}

// FlushSync forces all shards to flush NOW and blocks until every batch
// is persisted. Used by the Kafka consumer BEFORE committing offsets —
// closes the "commit before durable write" loss window (at-least-once
// becomes truly at-least-once).
func (s *Settler) FlushSync(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, sh := range s.shards {
		wg.Add(1)
		go func(sh *settleShard) {
			defer wg.Done()
			s.flush(sh)
		}(sh)
	}
	wg.Wait()
	return ctx.Err()
}

// Close flushes remaining rows and stops all shard flushers.
func (s *Settler) Close() {
	for _, sh := range s.shards {
		close(sh.done)
	}
	for _, sh := range s.shards {
		s.flush(sh)
	}
	s.wg.Wait()
}

func shardIndex(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

func shardOf(sh *settleShard, shards []*settleShard) int {
	for i, s := range shards {
		if s == sh {
			return i
		}
	}
	return -1
}

// priceFromEvent returns the request-start price snapshot, falling back
// to the current table for legacy events without a snapshot.
func priceFromEvent(ev metering.Event) ModelPrice {
	if ev.Pricing != nil {
		return ModelPrice{InputPer1M: ev.Pricing.InputPer1M, OutputPer1M: ev.Pricing.OutputPer1M}
	}
	return PriceFor(ev.Model)
}

// orderFromEvent converts a metering event to a terminal order.
// Failed requests (zero-completion insurance) become NO_CHARGE orders
// with amount 0 — the user pays nothing for requests that never produced
// output, not even the prompt tokens.
func orderFromEvent(ev metering.Event) *Order {
	status := StatusSettled
	completion := int64(ev.CompletionTokens)
	// Price from the REQUEST-START snapshot (never the current table —
	// in-flight requests must not be repriced by mid-request changes).
	p := priceFromEvent(ev)
	amount := CalculateAmount(int64(ev.PromptTokens), completion, p)
	if ev.Status == metering.StatusFailed {
		status = StatusNoCharge
		completion = 0
		amount = 0 // zero-completion insurance: failed requests are free
	}
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
