// Package obs centralizes MeterGate's Prometheus metrics — the
// observability surface that keeps the platform alive in production:
// request SLOs, billing accuracy signals, event-bus health, routing
// health and money-integrity indicators.
//
// Every metric is registered once via Metrics() and can be wired from
// any package without import cycles.
package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics bundles all MeterGate metrics.
type Metrics struct {
	// --- HTTP / SLO ---
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	// --- Billing fast path (pre-charge) ---
	PrechargeTotal    *prometheus.CounterVec // result: ok|insufficient|error
	PrechargeDuration prometheus.Histogram

	// --- Billing slow path (settle) ---
	OrdersSettledTotal *prometheus.CounterVec // status: settled|no_charge
	SettleBatchSize    prometheus.Histogram
	SettleDuration     prometheus.Histogram
	RefundIssuedTotal  *prometheus.CounterVec // level: auto|manual

	// --- Event bus integrity ---
	SinkDroppedTotal prometheus.Counter
	KafkaWriteErrors prometheus.Counter
	KafkaConsumerLag prometheus.Gauge // set by consumers when observable

	// --- Money integrity ---
	FrozenBalanceMicros  prometheus.Gauge       // unsettled pre-charges
	ReconDiffTotal       *prometheus.CounterVec // layer: internal|supplier
	ReconWithinTolerance prometheus.Gauge       // 1 = last run clean

	// --- Rate limiting ---
	RateLimitedTotal *prometheus.CounterVec // layer: key|user

	// --- Routing health ---
	ChannelHealth       *prometheus.GaugeVec // 1 = healthy
	ChannelBreakerState *prometheus.GaugeVec // 0 closed 1 open 2 half-open
}

// New registers and returns all metrics.
func New() *Metrics {
	return &Metrics{
		HTTPRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "metergate_http_requests_total",
			Help: "HTTP requests by path, method and status class.",
		}, []string{"path", "method", "code"}),
		HTTPRequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "metergate_http_request_duration_seconds",
			Help:    "HTTP request latency.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"path"}),

		PrechargeTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "metergate_precharge_total",
			Help: "Pre-charge attempts by result.",
		}, []string{"result"}),
		PrechargeDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "metergate_precharge_duration_seconds",
			Help:    "Pre-charge (Redis Lua) latency.",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.05},
		}),

		OrdersSettledTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "metergate_orders_settled_total",
			Help: "Settled orders by status.",
		}, []string{"status"}),
		SettleBatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "metergate_settle_batch_size",
			Help:    "Orders per committed batch.",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000},
		}),
		SettleDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "metergate_settle_duration_seconds",
			Help:    "Batch settle (PG insert + Redis settle) latency.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
		RefundIssuedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "metergate_refunds_total",
			Help: "Refunds issued by approval level.",
		}, []string{"level"}),

		SinkDroppedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "metergate_sink_dropped_total",
			Help: "Metering events dropped (buffer overflow) — money-integrity risk.",
		}),
		KafkaWriteErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "metergate_kafka_write_errors_total",
			Help: "Kafka producer write errors.",
		}),
		KafkaConsumerLag: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "metergate_kafka_consumer_lag",
			Help: "Total consumer group lag (0 = fully drained).",
		}),

		FrozenBalanceMicros: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "metergate_frozen_balance_micros",
			Help: "Unsettled pre-charge balance — must trend to ~0 after drain.",
		}),
		ReconDiffTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "metergate_recon_diffs_total",
			Help: "Reconciliation differences by layer.",
		}, []string{"layer"}),
		ReconWithinTolerance: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "metergate_recon_within_tolerance",
			Help: "1 if the last reconciliation run was within tolerance.",
		}),

		RateLimitedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "metergate_rate_limited_total",
			Help: "Requests rejected by the quota layer (six-layer budget model).",
		}, []string{"layer"}),

		ChannelHealth: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "metergate_channel_health",
			Help: "1 = channel healthy (30s failure window).",
		}, []string{"channel"}),
		ChannelBreakerState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "metergate_channel_breaker_state",
			Help: "0 closed, 1 open, 2 half-open.",
		}, []string{"channel"}),
	}
}
