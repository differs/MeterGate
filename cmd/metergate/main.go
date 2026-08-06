// Command metergate runs the MeterGate data plane: an OpenAI-compatible
// LLM gateway with streaming-accurate token metering.
//
// M1 configuration (environment variables):
//
//	METERGATE_PORT        listen port (default 3000)
//	METERGATE_UPSTREAM    upstream chat completions endpoint URL
//	METERGATE_UPSTREAM_KEY upstream API key
//	METERGATE_API_KEYS    comma-separated accepted client API keys
//
// Example:
//
//	METERGATE_UPSTREAM=http://127.0.0.1:9901/v1/chat/completions \
//	METERGATE_API_KEYS=sk-bench-1 \
//	metergate
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	apiKeys := strings.Split(envOr("METERGATE_API_KEYS", ""), ",")

	if upstreamURL == "" {
		logger.Error("METERGATE_UPSTREAM is required")
		os.Exit(1)
	}
	keys := make([]string, 0, len(apiKeys))
	for _, k := range apiKeys {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		logger.Error("at least one METERGATE_API_KEYS entry is required")
		os.Exit(1)
	}

	up := gateway.NewHTTPUpstream(upstreamURL, upstreamKey)
	srv := gateway.NewServer(up, gateway.WithKeys(keys), gateway.WithLogger(logger))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("metergate starting",
		"port", port,
		"upstream", upstreamURL,
		"api_keys", len(keys),
	)
	if err := srv.ListenAndServe(ctx, ":"+port); err != nil {
		logger.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
	logger.Info("metergate stopped")
}
