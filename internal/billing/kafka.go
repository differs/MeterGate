package billing

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/differs/MeterGate/internal/metering"
)

// KafkaSink is the production metering.Sink: it publishes request-level
// metering events to the Kafka topic. Multi-instance gateways share the
// event stream; the settle worker consumes it with exactly-once-ish
// semantics (idempotent order inserts make replays safe).
type KafkaSink struct {
	writer *kafka.Writer
	log    *slog.Logger
}

// NewKafkaSink builds a Kafka producer for metering events.
// brokers: comma-separated "host:port". topic: default "metering.events".
// Async mode never blocks the gateway hot path; delivery errors surface
// through the writer's Errors channel and are logged (they are acceptable —
// the audit log is the last-resort recovery source).
func NewKafkaSink(brokers []string, topic string, log *slog.Logger) *KafkaSink {
	if topic == "" {
		topic = "metering.events"
	}
	if log == nil {
		log = slog.Default()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // same request_id → same partition (ordering)
		BatchSize:    500,
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        true, // never block the gateway hot path
		// Async delivery errors surface here (kafka-go v0.4.x API).
		Completion: func(_ []kafka.Message, err error) {
			if err != nil {
				log.Error("kafka write failed (event lost; audit log is recovery source)", "err", err)
			}
		},
	}
	return &KafkaSink{writer: w, log: log}
}

// Emit implements metering.Sink (async; failures are logged and dropped —
// the audit log remains the last-resort recovery source).
func (s *KafkaSink) Emit(ev metering.Event) error {
	ev.Timestamp = time.Now().UTC()
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return s.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(ev.RequestID),
		Value: b,
	})
}

// Close flushes pending messages.
func (s *KafkaSink) Close() error {
	return s.writer.Close()
}

var _ metering.Sink = (*KafkaSink)(nil)
