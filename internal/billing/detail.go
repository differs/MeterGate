package billing

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/differs/MeterGate/internal/metering"
)

// DetailRow is one request-level billing detail row (ClickHouse).
// This is the "request-level detail" track of the L2 detail/summary split:
// audit-grade, replayable, TTL-managed — never the source of balance truth
// (PostgreSQL account_ledger is), but the source for reconciliation Layer A.
type DetailRow struct {
	RequestID        string
	UserID           string
	Model            string
	Provider         string
	Status           string // completed | aborted | failed
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	AmountMicros     int64
	DurationMs       int64
	TTFTMs           int64
	Ts               time.Time
}

// DetailSink persists billing details in batches (M5: ClickHouse).
// It is a metering.Sink — the gateway emits events; the detail writer
// converts and batches them into the detail table.
type DetailSink struct {
	conn    driver.Conn
	log     *slog.Logger
	batchN  int
	maxWait time.Duration

	mu      sync.Mutex
	buf     []DetailRow
	flushAt time.Time
	done    chan struct{}
	wg      sync.WaitGroup
}

// NewDetailSink connects to ClickHouse, applies the detail schema and
// starts the batch writer.
// batchN: rows per insert (default 1000); maxWait: flush deadline (default 1s).
func NewDetailSink(ctx context.Context, dsn string, log *slog.Logger, batchN int) (*DetailSink, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{dsn}})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	if err := conn.Exec(ctx, detailSchema); err != nil {
		conn.Close()
		return nil, err
	}
	if batchN <= 0 {
		batchN = 1000
	}
	if log == nil {
		log = slog.Default()
	}
	s := &DetailSink{
		conn:    conn,
		log:     log,
		batchN:  batchN,
		maxWait: time.Second,
		done:    make(chan struct{}),
	}
	s.wg.Add(1)
	go s.flushLoop()
	return s, nil
}

const detailSchema = `
CREATE TABLE IF NOT EXISTS billing_detail (
    request_id       String,
    user_id          String,
    model            String,
    provider         String,
    status           LowCardinality(String),
    prompt_tokens    UInt64,
    completion_tokens UInt64,
    total_tokens     UInt64,
    amount_micros    Int64,
    duration_ms      UInt64,
    ttft_ms          UInt64,
    ts               DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (user_id, ts, request_id)
TTL ts + INTERVAL 180 DAY
SETTINGS index_granularity = 8192`

// Emit implements metering.Sink (non-blocking, drops on full buffer).
// The amount is computed with the same pricing logic as the Settler so the
// detail tier and the order tier always agree (Layer A reconciliation).
func (s *DetailSink) Emit(ev metering.Event) error {
	p := priceFromEvent(ev)
	completion := int64(ev.CompletionTokens)
	amount := CalculateAmount(int64(ev.PromptTokens), completion, p)
	if ev.Status == metering.StatusFailed {
		completion = 0
		amount = 0 // zero-completion insurance: failed requests are free
	}
	row := DetailRow{
		RequestID:        ev.RequestID,
		UserID:           ev.UserID,
		Model:            ev.Model,
		Provider:         ev.Provider,
		Status:           string(ev.Status),
		PromptTokens:     int64(ev.PromptTokens),
		CompletionTokens: completion,
		TotalTokens:      int64(ev.PromptTokens) + completion,
		AmountMicros:     amount,
		DurationMs:       ev.DurationMs,
		TTFTMs:           ev.TTFTMs,
		Ts:               ev.Timestamp,
	}
	if row.Ts.IsZero() {
		row.Ts = time.Now()
	}
	s.mu.Lock()
	s.buf = append(s.buf, row)
	flush := len(s.buf) >= s.batchN
	if !flush && s.flushAt.IsZero() {
		s.flushAt = time.Now().Add(s.maxWait)
	}
	s.mu.Unlock()
	if flush {
		s.flush()
	}
	return nil
}

func (s *DetailSink) flushLoop() {
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

func (s *DetailSink) flush() {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return
	}
	rows := s.buf
	s.buf = nil
	s.flushAt = time.Time{}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO billing_detail
			(request_id, user_id, model, provider, status,
			 prompt_tokens, completion_tokens, total_tokens,
			 amount_micros, duration_ms, ttft_ms, ts)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		s.log.Error("clickhouse prepare batch failed", "err", err)
		return
	}
	for _, r := range rows {
		_ = batch.Append(r.RequestID, r.UserID, r.Model, r.Provider, r.Status,
			r.PromptTokens, r.CompletionTokens, r.TotalTokens,
			r.AmountMicros, r.DurationMs, r.TTFTMs, r.Ts.UTC())
	}
	if err := batch.Send(); err != nil {
		s.log.Error("clickhouse batch send failed (details lost)", "count", len(rows), "err", err)
		return
	}
	s.log.Debug("detail batch written", "count", len(rows))
}

// Lookup returns the detail row for one request_id (traceability).
// Returns nil when not found (may still be buffered — check orders table).
func (s *DetailSink) Lookup(ctx context.Context, requestID string) (*DetailRow, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT request_id, user_id, model, provider, status,
		       prompt_tokens, completion_tokens, total_tokens,
		       amount_micros, duration_ms, ttft_ms, ts
		FROM billing_detail WHERE request_id = ? LIMIT 1`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var r DetailRow
	if err := rows.Scan(&r.RequestID, &r.UserID, &r.Model, &r.Provider, &r.Status,
		&r.PromptTokens, &r.CompletionTokens, &r.TotalTokens,
		&r.AmountMicros, &r.DurationMs, &r.TTFTMs, &r.Ts); err != nil {
		return nil, err
	}
	return &r, rows.Err()
}

// Close flushes remaining rows and closes the connection.
func (s *DetailSink) Close() {
	close(s.done)
	s.flush()
	s.wg.Wait()
	s.conn.Close()
}

var _ metering.Sink = (*DetailSink)(nil)
