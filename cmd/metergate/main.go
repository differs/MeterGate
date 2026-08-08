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
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/admin"
	"github.com/differs/MeterGate/internal/auth"
	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/gateway"
	"github.com/differs/MeterGate/internal/ledger"
	"github.com/differs/MeterGate/internal/metering"
	"github.com/differs/MeterGate/internal/obs"
	"github.com/differs/MeterGate/internal/payment"
	"github.com/differs/MeterGate/internal/portal"
	"github.com/differs/MeterGate/internal/reconciliation"
	"github.com/differs/MeterGate/internal/router"
	"github.com/differs/MeterGate/pkg/openai"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	level := slog.LevelInfo
	switch envOr("METERGATE_LOG_LEVEL", "info") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	port := envOr("METERGATE_PORT", "3000")
	upstreamURL := envOr("METERGATE_UPSTREAM", "")
	upstreamKey := envOr("METERGATE_UPSTREAM_KEY", "")
	configPath := envOr("METERGATE_CONFIG", "")
	redisAddr := envOr("METERGATE_REDIS_ADDR", "")
	pgDSN := envOr("METERGATE_PG_DSN", "")
	pgShards := envOr("METERGATE_PG_SHARDS", "")
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

	// --- observability ---
	metrics := obs.New()

	// store is the billing store: orders + refunds (PostgresOrderStore or
	// ShardedStore both satisfy it).
	type billingStore interface {
		billing.OrderStore
		billing.RefundStore
	}
	var (
		precharger *billing.Precharger
		store      billingStore
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
		// LedgerAdapter boundary: PostgreSQL today, TigerBeetle later.
		adapter, err := ledger.NewPostgresAdapter(ctx, pgDSN)
		if err != nil {
			logger.Error("postgres unavailable, billing disabled", "err", err)
			store = nil
		} else {
			store = adapter.PostgresOrderStore
			defer adapter.Close()
			logger.Info("order storage enabled (ledger: postgres)")
		}
	} else if pgShards != "" {
		// Sharded mode: METERGATE_PG_SHARDS=4 + METERGATE_PG_DSN_0..3
		n, err := strconv.Atoi(pgShards)
		if err != nil || n < 2 {
			logger.Error("METERGATE_PG_SHARDS must be an integer >= 2", "value", pgShards)
			os.Exit(1)
		}
		shards := make([]billing.ShardStore, 0, n)
		for i := 0; i < n; i++ {
			dsn := os.Getenv(fmt.Sprintf("METERGATE_PG_DSN_%d", i))
			if dsn == "" {
				logger.Error("METERGATE_PG_DSN_" + strconv.Itoa(i) + " required in sharded mode")
				os.Exit(1)
			}
			adapter, err := ledger.NewPostgresAdapter(ctx, dsn)
			if err != nil {
				logger.Error("shard connect failed", "shard", i, "err", err)
				os.Exit(1)
			}
			shards = append(shards, adapter.PostgresOrderStore)
		}
		sharded := billing.NewShardedStore(shards)
		store = sharded
		logger.Info("sharded order storage enabled", "shards", n)
	}

	if precharger != nil {
		precharger.WithMetrics(metrics)
	}
	if precharger != nil && store != nil {
		settler := billing.NewSettler(store, precharger, logger, 500).WithMetrics(metrics)
		sink = billing.NewSink(ctx, settler, logger, 10_000).WithMetrics(metrics)
		defer sink.Close()
		logger.Info("dual-track billing enabled (pre-charge + batched settle)")
	} else if store != nil {
		settler := billing.NewSettler(store, nil, logger, 500).WithMetrics(metrics)
		sink = billing.NewSink(ctx, settler, logger, 10_000).WithMetrics(metrics)
		defer sink.Close()
		logger.Info("billing enabled (batched settle only, no pre-charge)")
	}

	// --- M5: Kafka event bus + ClickHouse detail tier (optional) ---
	var kafkaSink *billing.KafkaSink
	var kafkaConsumer *billing.KafkaConsumer
	if len(kafkaBrokers) > 0 {
		kafkaSink = billing.NewKafkaSink(kafkaBrokers, "metering.events", logger)
		defer kafkaSink.Close()
		logger.Info("kafka event bus enabled", "brokers", len(kafkaBrokers))
		// Kafka consumption mode: the Settler is driven by the consumer
		// group instead of the in-process sink — durable, multi-instance
		// safe. The in-process sink below is then not wired.
		if store != nil {
			settlerK := billing.NewSettler(store, precharger, logger, 500)
			kafkaConsumer = billing.NewKafkaConsumer(kafkaBrokers, "metering.events", "metergate-settle", settlerK, logger, 4)
			go func() {
				if err := kafkaConsumer.Run(ctx); err != nil {
					logger.Error("kafka consumer stopped", "err", err)
				}
			}()
			logger.Info("kafka consumer mode enabled (settle via event bus)")
		}
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

	// --- P0 commercial: auth + payment + portal (requires PG) ---
	var keyStore *auth.CachedKeyStore
	if pgDSN != "" {
		authSvc, err := auth.NewService(ctx, pgDSN)
		if err != nil {
			logger.Error("auth service failed, portal disabled", "err", err)
		} else {
			keyStore = auth.NewCachedKeyStore(authSvc.NewKeyStore(), 60*time.Second)

			paySvc := payment.NewService(authSvc.Pool(), func(ctx context.Context, userID int64, amount int64) error {
				if precharger == nil {
					return fmt.Errorf("precharger disabled (no redis)")
				}
				return precharger.TopUp(ctx, "user-"+fmt.Sprint(userID), amount)
			})
			paySvc.RegisterChannel(payment.MockChannel{})

			portalAPI := portal.New(authSvc, paySvc, adminKey)
			if sk := envOr("METERGATE_STRIPE_SECRET_KEY", ""); sk != "" {
				stripeCh := payment.NewStripeChannel(sk, envOr("METERGATE_STRIPE_WEBHOOK_SECRET", ""))
				paySvc.RegisterChannel(stripeCh)
				portalAPI.WithStripe(stripeCh)
				logger.Info("stripe channel enabled (test or live mode)")
			}
			if precharger != nil {
				portalAPI.WithBalance(func(ctx context.Context, userID int64) (int64, error) {
					return precharger.Balance(ctx, "user-"+fmt.Sprint(userID))
				})
			}
			if usageStore, ok := store.(interface {
				UsageByDay(ctx context.Context, userID string, days int) ([]billing.UsageDay, error)
			}); ok {
				portalAPI.WithUsage(usageStore.UsageByDay)
			}
			if modelStore, ok := store.(interface {
				UsageByModel(ctx context.Context, userID string, days int) ([]billing.UsageByModel, error)
			}); ok {
				portalAPI.WithUsageByModel(modelStore.UsageByModel)
			}
			if webDir := envOr("METERGATE_WEB_DIR", ""); webDir != "" {
				portalAPI.WithWeb(webDir)
				logger.Info("merchant portal web enabled", "dir", webDir)
			}

			// JWT sessions (HS256; secret from env, >= 32 bytes)
			if jwtSecret := envOr("METERGATE_JWT_SECRET", ""); jwtSecret != "" {
				portalAPI.WithJWT(auth.NewJWTManager(jwtSecret))
				logger.Info("session JWT enabled")
			}

			// OIDC (authorization-code flow; auto-register on first login)
			if oidcURL := envOr("METERGATE_OIDC_PROVIDER_URL", ""); oidcURL != "" {
				oidcCfg := auth.OIDCConfig{
					ProviderURL:  oidcURL,
					ClientID:     envOr("METERGATE_OIDC_CLIENT_ID", ""),
					ClientSecret: envOr("METERGATE_OIDC_CLIENT_SECRET", ""),
					RedirectURL:  envOr("METERGATE_OIDC_REDIRECT_URL", "http://localhost:3002/api/oidc/callback"),
					AutoRegister: true,
				}
				oidcSvc, err := auth.NewOIDC(oidcCfg, authSvc, auth.NewJWTManager(envOr("METERGATE_JWT_SECRET", "metergate-dev-secret-change-me-please-32")), logger)
				if err != nil {
					logger.Error("oidc init failed", "err", err)
				} else {
					portalAPI.WithOIDC(oidcSvc)
					logger.Info("oidc enabled", "provider", oidcURL)
				}
			}

			go func() {
				portalSrv := &http.Server{Addr: ":" + envOr("METERGATE_PORTAL_PORT", "3002"), Handler: portalAPI.Handler()}
				logger.Info("portal API listening", "addr", portalSrv.Addr)
				if err := portalSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					logger.Error("portal server failed", "err", err)
				}
			}()
			logger.Info("commercial stack enabled (users/keys/recharge/pay)")
		}
	}

	// --- admin API (optional) ---
	if adminPort != "" && store != nil {
		if adminKey == "" {
			logger.Error("METERGATE_ADMIN_KEY required with METERGATE_ADMIN_PORT")
			os.Exit(1)
		}
		recon := reconciliation.New(store, store, precharger, logger).WithMetrics(metrics)
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
		// Freeze request-start prices into metering events (accuracy: a
		// mid-request price change must never reprice in-flight requests).
		gateway.WithPriceSnapshot(func(model string) (int64, int64, bool) {
			p := billing.PriceFor(model)
			return p.InputPer1M, p.OutputPer1M, true
		}),
		gateway.WithMetrics(metrics),
	}
	if keyStore != nil {
		opts = append(opts, gateway.WithKeyResolver(func(raw string) (string, bool) {
			uid, err := keyStore.Resolve(context.Background(), raw)
			if err != nil {
				return "", false
			}
			return "user-" + fmt.Sprint(uid), true
		}))
		logger.Info("gateway auth: static keys + stored keys")

		if precharger != nil {
			// KeyLimiter: enforces per-key RPM/TPM/concurrency limits from
			// the stored key config (user-level limits layer next).
			rdb := precharger.Redis()
			if rdb != nil {
				limiter := newKeyLimiter(rdb, keyStore)
				opts = append(opts, gateway.WithRateLimiter(limiter))
				logger.Info("rate limiting enabled (RPM/TPM/concurrency per key)")
			}
		}
	}
	if precharger != nil {
		opts = append(opts, gateway.WithPreCharge(func(ctx context.Context, userID, requestID, model string, promptTokens int64, maxTokens *int64) error {
			p := billing.PriceFor(model)
			return precharger.PreCharge(ctx, userID, requestID, billing.EstimatePreCharge(promptTokens, maxTokens, p))
		}))
	}
	// Sink chain: in-process settle sink (only without Kafka consumer mode)
	// + Kafka bus + ClickHouse detail tier. The gateway emits once; all
	// sinks receive the same event (log sink is always on inside the gateway).
	combo := &gateway.CompositeSink{Sinks: []metering.Sink{}}
	if sink != nil && kafkaConsumer == nil {
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
		autoRouter, err := router.BuildAutoRouter(cfg, snap)
		if err != nil {
			logger.Error("routing config invalid (auto_router)", "err", err)
			os.Exit(1)
		}
		r := router.NewRouter(snap)
		if autoRouter != nil {
			r = router.NewRouterWithAuto(snap, autoRouter)
			logger.Info("auto router enabled", "models", len(cfg.AutoRouter.Models), "cost_quality", cfg.AutoRouter.CostQuality)
		}
		up = router.NewRoutingUpstream(r)
		if autoRouter != nil {
			opts = append(opts, gateway.WithModelResolver(func(model string, req *openai.ChatCompletionRequest) (string, error) {
				if model == "auto" || model == "openrouter/auto" {
					return autoRouter.Pick(req)
				}
				return model, nil
			}))
		}
		logger.Info("routing engine enabled",
			"channels", len(cfg.Channels),
			"models", len(cfg.Models),
			"config", configPath)
	} else {
		up = gateway.NewHTTPUpstream(upstreamURL, upstreamKey)
	}
	// --- periodic gauge collectors (observability) ---
	collector := obs.NewCollector(metrics, logger)
	if precharger != nil {
		collector.WithFrozen(func() (int64, error) {
			return precharger.FrozenBalance(ctx)
		})
	}
	if kafkaConsumer != nil {
		collector.WithKafkaLag(func() (int64, error) {
			return kafkaConsumer.Lag(), nil
		})
	}
	if r, ok := up.(*router.RoutingUpstream); ok {
		collector.WithChannels(func() []obs.ChannelState {
			rr := r.Router()
			out := make([]obs.ChannelState, 0, 8)
			for _, id := range rr.Channels() {
				healthy, state := rr.ChannelHealth(id)
				out = append(out, obs.ChannelState{ID: id, Healthy: healthy, BreakerState: state})
			}
			return out
		})
	}
	go collector.Run(ctx)

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
