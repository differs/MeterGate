package metering

import (
	"time"
)

// EventStatus describes how a metered request terminated.
type EventStatus string

const (
	StatusCompleted EventStatus = "completed"
	StatusAborted   EventStatus = "aborted" // client disconnect
	StatusFailed    EventStatus = "failed"  // upstream error, no completion
)

// PricingSnapshot freezes the prices effective at REQUEST START.
// Settlement must use this snapshot, never the current price table —
// otherwise a price change mid-request makes billing unreproducible
// and supplier reconciliation impossible (accuracy gap #1).
type PricingSnapshot struct {
	InputPer1M  int64     `json:"input_per_1m"`
	OutputPer1M int64     `json:"output_per_1m"`
	EffectiveAt time.Time `json:"effective_at"`
}

// Event is the request-level metering record emitted at stream termination.
// Exactly one Event is emitted per request; downstream consumers (billing
// worker, reconciliation) rely on request_id for idempotency.
type Event struct {
	RequestID        string           `json:"request_id"`
	UserID           string           `json:"user_id"`
	Model            string           `json:"model"`
	Provider         string           `json:"provider"`
	Status           EventStatus      `json:"status"`
	PromptTokens     int              `json:"prompt_tokens"`
	CompletionTokens int              `json:"completion_tokens"`
	CacheTokens      int              `json:"cache_tokens"`
	TTFTMs           int64            `json:"ttft_ms"`
	DurationMs       int64            `json:"duration_ms"`
	UpstreamUsageRaw string           `json:"upstream_usage_raw,omitempty"` // raw usage JSON for cross-check
	Pricing          *PricingSnapshot `json:"pricing,omitempty"`            // request-start prices
	Timestamp        time.Time        `json:"timestamp"`
}

// Sink consumes metering events. Implementations: log sink (M1),
// Kafka sink (M3+), test sink.
type Sink interface {
	Emit(Event) error
}
