// Package gateway implements MeterGate's stateless data plane: an
// OpenAI-compatible HTTP endpoint that forwards requests to upstream model
// providers while metering tokens locally on the streaming hot path.
//
// The data plane is deliberately dependency-free and sidecar-friendly:
// routing snapshots, pre-charge and metering sinks are injected interfaces,
// so the same code runs standalone (M1) and inside the full platform (M3+).
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Upstream is a model provider the gateway forwards to.
type Upstream interface {
	// RoundTrip performs a chat completion call. For streaming requests the
	// caller passes a chunk callback; the upstream implementation must call
	// it for every SSE data payload (excluding the [DONE] sentinel).
	Chat(ctx context.Context, req *ChatCall) (*ChatResult, error)
}

// ChatCall is a normalized chat completion invocation.
type ChatCall struct {
	RequestID string
	UserID    string
	Model     string
	Stream    bool
	// Body is the original request JSON (transparent forwarding).
	Body []byte
	// OnChunk receives decoded SSE data payloads (streaming only).
	OnChunk func(data []byte) error
	// UpstreamURL / UpstreamKey are resolved by the caller-provided router.
	UpstreamURL string
	UpstreamKey string
}

// ChatResult is the outcome of a non-streaming call.
type ChatResult struct {
	StatusCode int
	Body       []byte
	// UpstreamUsageRaw is the raw JSON of the upstream usage object, kept
	// for cross-validation with local metering.
	UpstreamUsageRaw string
}

// Server hosts the OpenAI-compatible HTTP API.
type Server struct {
	upstream  Upstream
	keys      map[string]bool // accepted API keys (M1: static; later: store)
	log       *slog.Logger
	httpSrv   *http.Server
	now       func() time.Time
	requestID func() string
}

// Option configures a Server.
type Option func(*Server)

// WithKeys sets the accepted API keys (static allowlist for M1).
func WithKeys(keys []string) Option {
	return func(s *Server) {
		s.keys = make(map[string]bool, len(keys))
		for _, k := range keys {
			s.keys[k] = true
		}
	}
}

// WithLogger overrides the default logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) { s.log = l }
}

// WithRequestID overrides the request ID generator (tests).
func WithRequestID(fn func() string) Option {
	return func(s *Server) { s.requestID = fn }
}

// NewServer builds a gateway server around an upstream.
func NewServer(upstream Upstream, opts ...Option) *Server {
	s := &Server{
		upstream: upstream,
		log:      slog.Default(),
		now:      time.Now,
		requestID: func() string {
			return newRequestID()
		},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Handler returns the HTTP handler (routes registered).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return s.authMiddleware(mux)
}

// ListenAndServe starts the gateway until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	s.httpSrv = &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("metergate listening", "addr", addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
