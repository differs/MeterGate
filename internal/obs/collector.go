// Package obs — periodic collectors that keep the "defined" gauges
// actually alive: frozen balance, Kafka consumer lag, channel health.
// Reconciliation metrics are wired directly at run time.
package obs

import (
	"context"
	"log/slog"
	"time"
)

// ChannelState is one routing channel's health snapshot.
type ChannelState struct {
	ID           string
	Healthy      bool
	BreakerState string // closed | open | half_open | unknown
}

// Collector periodically refreshes gauges from component providers.
type Collector struct {
	m        *Metrics
	frozen   func() (int64, error)
	kafkaLag func() (int64, error)
	channels func() []ChannelState
	interval time.Duration
	log      *slog.Logger
}

// NewCollector builds the periodic gauge refresher. Any provider may be
// nil (its gauge stays at the last value).
func NewCollector(m *Metrics, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	return &Collector{m: m, log: log, interval: 30 * time.Second}
}

// WithFrozen attaches the frozen-balance provider (Redis scan).
func (c *Collector) WithFrozen(fn func() (int64, error)) *Collector {
	c.frozen = fn
	return c
}

// WithKafkaLag attaches the consumer-lag provider.
func (c *Collector) WithKafkaLag(fn func() (int64, error)) *Collector {
	c.kafkaLag = fn
	return c
}

// WithChannels attaches the routing health provider.
func (c *Collector) WithChannels(fn func() []ChannelState) *Collector {
	c.channels = fn
	return c
}

// Run refreshes gauges until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	c.refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh()
		}
	}
}

func (c *Collector) refresh() {
	if c.frozen != nil {
		if v, err := c.frozen(); err == nil {
			c.m.FrozenBalanceMicros.Set(float64(v))
		} else {
			c.log.Warn("frozen gauge refresh failed", "err", err)
		}
	}
	if c.kafkaLag != nil {
		if v, err := c.kafkaLag(); err == nil {
			c.m.KafkaConsumerLag.Set(float64(v))
		} else {
			c.log.Warn("kafka lag gauge refresh failed", "err", err)
		}
	}
	if c.channels != nil {
		for _, ch := range c.channels() {
			healthy := 0.0
			if ch.Healthy {
				healthy = 1
			}
			c.m.ChannelHealth.WithLabelValues(ch.ID).Set(healthy)
			c.m.ChannelBreakerState.WithLabelValues(ch.ID).Set(breakerValue(ch.BreakerState))
		}
	}
}

func breakerValue(state string) float64 {
	switch state {
	case "open":
		return 1
	case "half_open":
		return 2
	default:
		return 0
	}
}
