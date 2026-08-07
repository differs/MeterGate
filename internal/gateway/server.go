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
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/differs/MeterGate/internal/metering"
	"github.com/differs/MeterGate/internal/obs"
	"github.com/differs/MeterGate/pkg/openai"
)

// ctxKeyUserID is defined in middleware.go.

// ModelResolver maps the requested model to a concrete model before
// routing (e.g. "auto" → a picked model from the auto router). Returning
// an error rejects the request.
type ModelResolver func(model string, req *openai.ChatCompletionRequest) (string, error)

// PriceSnapshotFunc returns the prices effective at request start, frozen
// into the metering event so settlement never reprices in-flight requests.
type PriceSnapshotFunc func(model string) (inputPer1M, outputPer1M int64, ok bool)

// PreChargeFunc reserves funds for a request before upstream forwarding.
// Returning an error rejects the request (e.g. insufficient balance).
// This is the billing fast path; the async settle track releases the
// remainder after the stream terminates.
type PreChargeFunc func(ctx context.Context, userID, requestID, model string, promptTokens int64, maxTokens *int64) error

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
	// Provider is the winning channel ID (set by the routing upstream),
	// recorded in the metering event.
	Provider string
	// Pricing is the request-start price freeze (settlement must use it).
	Pricing *metering.PricingSnapshot
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
	upstream   Upstream
	keys       map[string]bool                 // accepted API keys (M1: static; later: store)
	sink       metering.Sink                   // optional billing event sink (M2+)
	preCharge  PreChargeFunc                   // optional billing fast path (M2+)
	resolver   ModelResolver                   // optional model resolution ("auto")
	priceSnap  PriceSnapshotFunc               // optional request-start price freeze
	metrics    *obs.Metrics                    // optional Prometheus metrics
	keyResolve func(raw string) (string, bool) // optional DB-backed auth
	log        *slog.Logger
	httpSrv    *http.Server
	now        func() time.Time
	requestID  func() string
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

// WithKeyResolver attaches DB-backed key auth: the static allowlist is
// checked first; unresolved keys fall through to the resolver (cached).
func WithKeyResolver(fn func(raw string) (string, bool)) Option {
	return func(s *Server) { s.keyResolve = fn }
}

// WithMetrics attaches Prometheus metrics instrumentation.
func WithMetrics(m *obs.Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// WithPriceSnapshot attaches the request-start price freeze.
func WithPriceSnapshot(fn PriceSnapshotFunc) Option {
	return func(s *Server) { s.priceSnap = fn }
}

// WithModelResolver attaches model resolution ("auto" support).
func WithModelResolver(fn ModelResolver) Option {
	return func(s *Server) { s.resolver = fn }
}

// WithPreCharge attaches the billing fast path (atomic fund reservation).
func WithPreCharge(fn PreChargeFunc) Option {
	return func(s *Server) { s.preCharge = fn }
}

// WithMeteringSink attaches a billing event sink (settle pipeline).
func WithMeteringSink(sink metering.Sink) Option {
	return func(s *Server) { s.sink = sink }
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

// instrument wraps the mux with request metrics (SLO: error rate, p99).
func (s *Server) instrument(next http.Handler) http.Handler {
	if s.metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		code := rec.status
		class := fmt.Sprintf("%dxx", code/100)
		s.metrics.HTTPRequestsTotal.WithLabelValues(r.URL.Path, r.Method, class).Inc()
		s.metrics.HTTPRequestDuration.WithLabelValues(r.URL.Path).Observe(time.Since(start).Seconds())
	})
}

// statusRecorder captures the response status for metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Handler returns the HTTP handler (routes registered).
// Public paths (/healthz, /metrics) bypass auth; the API surface is
// auth-wrapped and instrumented.
func (s *Server) Handler() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("POST /v1/chat/completions", s.handleChat)

	public := http.NewServeMux()
	public.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if s.metrics != nil {
		public.Handle("/metrics", promhttp.Handler())
	}
	public.Handle("/", s.instrument(s.authMiddleware(api)))
	return public
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
