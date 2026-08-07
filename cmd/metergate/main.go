// Command metergate runs the MeterGate data plane: an OpenAI-compatible
// LLM gateway with streaming-accurate token metering and dual-track billing.
//
// Configuration (environment variables):
//
//	METERGATE_PORT          listen port (default 3000)
//	METERGATE_UPSTREAM      upstream chat completions endpoint URL
//	METERGATE_UPSTREAM_KEY  upstream API key
//	METERGATE_API_KEYS      comma-separated accepted client API keys
//
// Billing (optional — gateway runs without them; billing then relies on
// the audit log only):
//
//	METERGATE_REDIS_ADDR    Redis address (pre-charge fast path)
//	METERGATE_PG_DSN        PostgreSQL DSN (terminal order storage)
//
// Routing (optional — single upstream without it):
//
//	METERGATE_CONFIG        routing config YAML (multi-channel, price-weighted,
//	                        30s failure window, circuit breaker)
//
// Admin API (optional):
//
//	METERGATE_ADMIN_PORT   admin HTTP port (disabled when empty)
//	METERGATE_ADMIN_KEY    admin API key (required with the port)
//
// Event bus + detail tier (M5, optional — falls back to in-process sinks):
//
//	METERGATE_KAFKA        comma-separated Kafka brokers (event bus)
//	METERGATE_CLICKHOUSE   ClickHouse address, e.g. 127.0.0.1:9000 (detail tier)
//
// Example (full stack):
//
//	METERGATE_UPSTREAM=http://127.0.0.1:9901/v1/chat/completions \
//	METERGATE_API_KEYS=sk-bench-1 \
//	METERGATE_REDIS_ADDR=127.0.0.1:6379 \
//	METERGATE_PG_DSN=postgres://postgres:pw@127.0.0.1:5432/metergate \
//	metergate
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/admin"
	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/gateway"
	"github.com/differs/MeterGate/internal/metering"
	"github.com/differs/MeterGate/internal/reconciliation"
	"github.com/differs/MeterGate/internal/router"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	port := envOr("METERGATE_PORT", "3000")
	upstreamURL := envOr("METERGATE_UPSTREAM", "")
	upstreamKey := envOr("METERGATE_UPSTREAM_KEY", "")
	configPath := envOr("METERGATE_CONFIG", "")
	redisAddr := envOr("METERGATE_REDIS_ADDR", "")
	pgDSN := envOr("METERGATE_PG_DSN", "")
	adminPort := envOr("METERGATE_ADMIN_PORT", "")
	adminKey := envOr("METERGATE_ADMIN_KEY", "")
	kafkaBrokers := splitKeys(envOr("METERGATE_KAFKA", ""))
	chAddr := envOr("METERGATE_CLICKHOUSE", "")

	apiKeys := splitKeys(os.Getenv("METERGATE_API_KEYS"))
	if upstreamURL == "" && configPath == "" {
		logger.Error("METERGATE_UPSTREAM or METERGATE_CONFIG is required")
		os.Exit(1)
	}
	if len(apiKeys) == 0 {
		logger.Error("at least one METERGATE_API_KEYS entry is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- billing stack (optional) ---
	var (
		precharger *billing.Precharger
		store      *billing.PostgresOrderStore
		sink       *billing.Sink
	)

	if redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis unavailable, pre-charge disabled", "err", err)
		} else {
			precharger = billing.NewPrecharger(rdb)
			logger.Info("pre-charge enabled", "redis", redisAddr)
		}
	}

	if pgDSN != "" {
		var err error
		store, err = billing.NewPostgresOrderStore(ctx, pgDSN)
		if err != nil {
			logger.Error("postgres unavailable, billing disabled", "err", err)
			store = nil
		} else {
			defer store.Close()
			logger.Info("order storage enabled")
		}
	}

	if precharger != nil && store != nil {
		settler := billing.NewSettler(store, precharger, logger, 500)
		sink = billing.NewSink(ctx, settler, logger, 10_000)
		defer sink.Close()
		logger.Info("dual-track billing enabled (pre-charge + batched settle)")
	} else if store != nil {
		settler := billing.NewSettler(store, nil, logger, 500)
		sink = billing.NewSink(ctx, settler, logger, 10_000)
		defer sink.Close()
		logger.Info("billing enabled (batched settle only, no pre-charge)")
	}

	// --- M5: Kafka event bus + ClickHouse detail tier (optional) ---
	var kafkaSink *billing.KafkaSink
	if len(kafkaBrokers) > 0 {
		kafkaSink = billing.NewKafkaSink(kafkaBrokers, "metering.events", logger)
		defer kafkaSink.Close()
		logger.Info("kafka event bus enabled", "brokers", len(kafkaBrokers))
	}
	var detailSink *billing.DetailSink
	if chAddr != "" {
		var err error
		detailSink, err = billing.NewDetailSink(ctx, chAddr, logger, 1000)
		if err != nil {
			logger.Error("clickhouse unavailable, detail tier disabled", "err", err)
			detailSink = nil
		} else {
			defer detailSink.Close()
			logger.Info("clickhouse detail tier enabled", "addr", chAddr)
		}
	}

	// --- admin API (optional) ---
	if adminPort != "" && store != nil {
		if adminKey == "" {
			logger.Error("METERGATE_ADMIN_KEY required with METERGATE_ADMIN_PORT")
			os.Exit(1)
		}
		recon := reconciliation.New(store, store, precharger, logger)
		adm := admin.New(store, store, precharger, recon, adminKey)
		adminSrv := &http.Server{
			Addr:    ":" + adminPort,
			Handler: adm.Handler(),
		}
		go func() {
			logger.Info("admin API listening", "addr", adminSrv.Addr)
			if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("admin server failed", "err", err)
			}
		}()
		defer adminSrv.Shutdown(context.Background())
	}

	// --- gateway ---
	opts := []gateway.Option{
		gateway.WithKeys(apiKeys),
		gateway.WithLogger(logger),
	}
	if precharger != nil {
		opts = append(opts, gateway.WithPreCharge(func(ctx context.Context, userID, requestID, model string, promptTokens int64, maxTokens *int64) error {
			p := billing.PriceFor(model)
			return precharger.PreCharge(ctx, userID, requestID, billing.EstimatePreCharge(promptTokens, maxTokens, p))
		}))
	}
	// Sink chain: in-process settle sink + optional Kafka bus + optional
	// ClickHouse detail tier. The gateway emits once; all sinks receive
	// the same event (log sink is always on inside the gateway).
	combo := &gateway.CompositeSink{Sinks: []metering.Sink{}}
	if sink != nil {
		combo.Sinks = append(combo.Sinks, sink)
	}
	if kafkaSink != nil {
		combo.Sinks = append(combo.Sinks, kafkaSink)
	}
	if detailSink != nil {
		combo.Sinks = append(combo.Sinks, detailSink)
	}
	if len(combo.Sinks) > 0 {
		opts = append(opts, gateway.WithMeteringSink(combo))
	}

	// --- upstream: routing engine (M3) or single upstream ---
	var up gateway.Upstream
	if configPath != "" {
		cfg, err := router.LoadConfig(configPath)
		if err != nil {
			logger.Error("routing config invalid", "err", err)
			os.Exit(1)
		}
		snap, err := router.BuildSnapshot(cfg)
		if err != nil {
			logger.Error("routing config invalid", "err", err)
			os.Exit(1)
		}
		r := router.NewRouter(snap)
		up = router.NewRoutingUpstream(r)
		logger.Info("routing engine enabled",
			"channels", len(cfg.Channels),
			"models", len(cfg.Models),
			"config", configPath)
	} else {
		up = gateway.NewHTTPUpstream(upstreamURL, upstreamKey)
	}
	srv := gateway.NewServer(up, opts...)

	logger.Info("metergate starting",
		"port", port,
		"upstream", upstreamURL,
		"config", configPath,
		"api_keys", len(apiKeys),
		"precharge", precharger != nil,
		"order_store", store != nil,
	)
	if err := srv.ListenAndServe(ctx, ":"+port); err != nil {
		logger.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
	logger.Info("metergate stopped")
}

func splitKeys(raw string) []string {
	out := make([]string, 0, 4)
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return out
}
