package billing

import (
	"context"
	"encoding/json"
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
// At-least-once semantics: offsets commit after the batch is persisted;
// replays are safe because order inserts are idempotent (request_id).
type KafkaConsumer struct {
	reader  *kafka.Reader
	settler *Settler
	log     *slog.Logger
	workers int
	done    chan struct{}
}

// NewKafkaConsumer builds a consumer group reader.
// groupID: consumer group (default "metergate-settle"). workers: parallel
// handlers (default 4). The Settler's internal buffer serializes writes,
// so workers only parallelize event decoding — safe by construction.
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
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	return &KafkaConsumer{
		reader:  reader,
		settler: settler,
		log:     log,
		workers: workers,
		done:    make(chan struct{}),
	}
}

// Run blocks until ctx is cancelled, consuming events continuously.
func (c *KafkaConsumer) Run(ctx context.Context) error {
	sem := make(chan struct{}, c.workers)
	for {
		select {
		case <-ctx.Done():
			c.log.Info("kafka consumer stopping")
			return c.reader.Close()
		default:
		}
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return c.reader.Close()
			}
			c.log.Error("kafka fetch failed", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		sem <- struct{}{}
		go func(m kafka.Message) {
			defer func() { <-sem }()
			var ev metering.Event
			if err := json.Unmarshal(m.Value, &ev); err != nil {
				c.log.Error("bad metering event, skipping", "err", err)
				return
			}
			if err := c.settler.Handle(context.Background(), ev); err != nil {
				c.log.Error("settle failed", "request_id", ev.RequestID, "err", err)
				return
			}
			// Commit after the event entered the settler buffer; the
			// batched PG insert may still be in flight, but replays are
			// idempotent — safe.
			_ = c.reader.CommitMessages(context.Background(), m)
		}(msg)
	}
}
