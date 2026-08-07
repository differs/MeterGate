package billing

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/differs/MeterGate/internal/metering"
)

// KafkaConsumer consumes metering events from Kafka and feeds them to a
// Settler. It is the production event pipeline for multi-instance
// gateways: every gateway writes to the topic, one or more settle workers
// consume and batch-commit into PostgreSQL.
//
// At-least-once semantics with BATCHED commits: messages accumulate in a
// window (500 msgs or 200ms), are handed to the Settler, then ONE
// CommitMessages call advances the group offset. Per-message commits are
// a known kafka-go anti-pattern (~1ms RPC each) that caps throughput at
// ~500-1000 events/s — the load-test bottleneck we measured and fixed.
//
// Replays are safe because order inserts are idempotent (request_id).
type KafkaConsumer struct {
	reader  *kafka.Reader
	settler *Settler
	log     *slog.Logger
	batchN  int
	maxWait time.Duration
	done    chan struct{}
}

// NewKafkaConsumer builds a consumer group reader.
// groupID: consumer group (default "metergate-settle").
func NewKafkaConsumer(brokers []string, topic, groupID string, settler *Settler, log *slog.Logger, workers int) *KafkaConsumer {
	if topic == "" {
		topic = "metering.events"
	}
	if groupID == "" {
		groupID = "metergate-settle"
	}
	if workers <= 0 {
		workers = 4
	}
	if log == nil {
		log = slog.Default()
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       brokers,
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1 << 20, // 1MB: batch pulls instead of per-message round-trips
		MaxBytes:      64 << 20,
		MaxWait:       500 * time.Millisecond,
		QueueCapacity: 4096, // internal prefetch buffer
		StartOffset:   kafka.LastOffset,
	})
	return &KafkaConsumer{
		reader:  reader,
		settler: settler,
		log:     log,
		batchN:  500,
		maxWait: 200 * time.Millisecond,
		done:    make(chan struct{}),
	}
}

// Lag returns the total consumer lag (observability; 0 when idle/drained).
func (c *KafkaConsumer) Lag() int64 {
	return c.reader.Stats().Lag
}

// Run blocks until ctx is cancelled, consuming events continuously.
func (c *KafkaConsumer) Run(ctx context.Context) error {
	defer close(c.done)
	var (
		batch   []kafka.Message
		firstAt time.Time
		flush   = func() {
			if len(batch) == 0 {
				return
			}
			for _, m := range batch {
				var ev metering.Event
				if err := json.Unmarshal(m.Value, &ev); err != nil {
					c.log.Error("bad metering event, skipping", "err", err)
					continue
				}
				if err := c.settler.Handle(context.Background(), ev); err != nil {
					c.log.Error("settle failed", "request_id", ev.RequestID, "err", err)
				}
			}
			// Durable-before-commit: force the Settler to persist NOW,
			// then commit. A crash between write and commit re-consumes
			// the batch — idempotent, no loss, no double charge.
			if err := c.settler.FlushSync(context.Background()); err != nil {
				c.log.Error("flush sync failed; not committing", "count", len(batch), "err", err)
				return // stop consuming; operator intervenes
			}
			// ONE commit for the whole batch (group semantics: highest
			// offset commits everything before it).
			if err := c.reader.CommitMessages(context.Background(), batch...); err != nil {
				c.log.Error("kafka commit failed (will re-consume, idempotent)", "count", len(batch), "err", err)
			}
			c.log.Debug("kafka batch consumed", "count", len(batch))
			batch = batch[:0]
			firstAt = time.Time{}
		}
	)

	for {
		select {
		case <-ctx.Done():
			flush()
			c.log.Info("kafka consumer stopping")
			return c.reader.Close()
		default:
		}

		// Fetch with a bounded wait: when no new messages arrive, the
		// deadline fires so the residual batch (any un-flushed messages)
		// is still persisted. Without this, a quiet topic leaves the
		// last partial batch stuck forever — the consumer-hang bug found
		// in load testing.
		fetchCtx, cancel := context.WithTimeout(ctx, c.maxWait)
		m, err := c.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				flush() // drain residual batch on quiet periods
				continue
			}
			if ctx.Err() != nil {
				flush()
				return c.reader.Close()
			}
			c.log.Error("kafka fetch failed", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if len(batch) == 0 {
			firstAt = time.Now()
		}
		batch = append(batch, m)
		if len(batch) >= c.batchN || time.Since(firstAt) >= c.maxWait {
			flush()
		}
	}
}
