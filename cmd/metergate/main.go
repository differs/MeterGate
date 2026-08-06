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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/gateway"
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
	redisAddr := envOr("METERGATE_REDIS_ADDR", "")
	pgDSN := envOr("METERGATE_PG_DSN", "")

	apiKeys := splitKeys(os.Getenv("METERGATE_API_KEYS"))
	if upstreamURL == "" {
		logger.Error("METERGATE_UPSTREAM is required")
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
		settler := billing.NewSettler(store, precharger, logger, 100)
		sink = billing.NewSink(ctx, settler, logger, 10_000)
		defer sink.Close()
		logger.Info("dual-track billing enabled (pre-charge + async settle)")
	} else if store != nil {
		settler := billing.NewSettler(store, nil, logger, 100)
		sink = billing.NewSink(ctx, settler, logger, 10_000)
		defer sink.Close()
		logger.Info("billing enabled (settle only, no pre-charge)")
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
	if sink != nil {
		opts = append(opts, gateway.WithMeteringSink(sink))
	}

	up := gateway.NewHTTPUpstream(upstreamURL, upstreamKey)
	srv := gateway.NewServer(up, opts...)

	logger.Info("metergate starting",
		"port", port,
		"upstream", upstreamURL,
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
