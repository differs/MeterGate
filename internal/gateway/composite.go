package gateway

import (
	"github.com/differs/MeterGate/internal/metering"
)

// CompositeSink fans one metering event out to multiple sinks (in-process
// settle, Kafka bus, ClickHouse detail tier). The gateway emits once; each
// sink decides its own batching and delivery semantics.
type CompositeSink struct {
	Sinks []metering.Sink
}

// Emit implements metering.Sink: non-blocking, best-effort fan-out.
func (c *CompositeSink) Emit(ev metering.Event) error {
	for _, s := range c.Sinks {
		_ = s.Emit(ev)
	}
	return nil
}

var _ metering.Sink = (*CompositeSink)(nil)
