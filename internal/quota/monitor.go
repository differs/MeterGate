// Package quota monitors budget consumption: it periodically samples
// the sliding-window counters of every configured quota scope (org →
// team → project → user) and publishes the usage ratio as a Prometheus
// gauge, so alerting rules can fire before a scope is exhausted.
//
// The key and end-user layers are per-request scopes (their raw keys are
// never stored) and are monitored via rate_limited_total instead.
package quota

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/obs"
)

// Scope is one configured quota read from the database.
type Scope struct {
	Layer string // org | team | project | user
	ID    int64
	RPM   int64
	TPM   int64
}

// ScopeProvider returns every configured quota (RPM or TPM > 0).
type ScopeProvider func(ctx context.Context) ([]Scope, error)

// Monitor samples quota usage on an interval.
type Monitor struct {
	rdb      *redis.Client
	metrics  *obs.Metrics
	interval time.Duration
	query    ScopeProvider
	log      *slog.Logger
}

// NewMonitor builds the sampler (interval defaults to 30s).
func NewMonitor(rdb *redis.Client, m *obs.Metrics, log *slog.Logger) *Monitor {
	if log == nil {
		log = slog.Default()
	}
	return &Monitor{rdb: rdb, metrics: m, interval: 30 * time.Second, log: log}
}

// WithInterval overrides the sampling period (tests).
func (m *Monitor) WithInterval(d time.Duration) *Monitor {
	m.interval = d
	return m
}

// Configure registers the scope provider (injected by main: the DB pool
// lives in auth; this package stays storage-agnostic).
func (m *Monitor) Configure(query ScopeProvider) *Monitor {
	m.query = query
	return m
}

// Run samples until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.sample(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sample(ctx)
		}
	}
}

// SampleNow runs one sampling pass (exposed for tests/manual refresh).
func (m *Monitor) SampleNow(ctx context.Context) {
	m.sample(ctx)
}

func (m *Monitor) sample(ctx context.Context) {
	if m.query == nil {
		return
	}
	scopes, err := m.query(ctx)
	if err != nil {
		m.log.Warn("quota monitor: scope scan failed", "err", err)
		return
	}
	for _, s := range scopes {
		scopeID := scopeName(s.Layer, s.ID)
		if s.RPM > 0 {
			count, err := m.rdb.ZCard(ctx, "rl:"+scopeID+":rpm").Result()
			if err == nil {
				m.metrics.QuotaUsageRatio.WithLabelValues(s.Layer, scopeID).Set(ratio(count, s.RPM))
			}
		}
		if s.TPM > 0 {
			count, err := m.rdb.ZCard(ctx, "rl:"+scopeID+":tpm").Result()
			if err == nil {
				m.metrics.QuotaUsageRatio.WithLabelValues(s.Layer, scopeID).Set(ratio(count, s.TPM))
			}
		}
	}
}

// scopeName mirrors the gateway's scope convention (budgetLimiter).
func scopeName(layer string, id int64) string {
	switch layer {
	case "org", "team", "project", "user":
		return layer + "-" + itoa(id)
	default:
		return ""
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func ratio(count int64, limit int64) float64 {
	if limit <= 0 {
		return 0
	}
	r := float64(count) / float64(limit)
	if r > 1 {
		return 1
	}
	return r
}
