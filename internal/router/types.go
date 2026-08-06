// Package router implements MeterGate's routing engine — the layer that
// decides which upstream channel serves each request, modeled on OpenRouter's
// production routing:
//
//   - Two-layer model: model selection (which model) and channel selection
//     (which provider serves it). M3 implements channel selection; the
//     auto model router is a later milestone.
//   - Default strategy: 30s failure window + inverse-square price weighting
//     (cheapest *reliable* provider gets ~9x the traffic of a 3x-priced one).
//   - Per-channel circuit breaker (Closed → Open → HalfOpen) on top of the
//     failure window.
//   - Fallback chain: primary → cheaper-stable alternatives → unhealthy
//     channels last; 4xx client errors never retry.
//
// The engine is pure and dependency-free: the hot path (Select) touches no
// locks, no IO — only an atomically-swapped immutable snapshot.
package router

import "time"

// Channel is one upstream provider serving a set of models.
type Channel struct {
	ID      string
	Name    string
	BaseURL string
	Key     string
	// Prices in micros per 1M tokens (cost side). Used for inverse-square
	// weighting — the cheapest reliable channel wins most traffic.
	InputPer1M  int64
	OutputPer1M int64
	// Weight is an optional extra bias multiplier (default 1).
	Weight int64
}

// ChannelSpec is a channel in a model's candidate set.
type ChannelSpec struct {
	Channel *Channel
	// Healthy reflects the 30s failure window (false → deprioritized,
	// not removed).
	Healthy bool
	// BreakerOpen means the circuit breaker is tripped (fast-fail).
	BreakerOpen bool
}

// ModelRoute maps a model slug to its candidate channels.
type ModelRoute struct {
	Model    string
	Channels []ChannelSpec
}

// Decision is the outcome of a routing selection.
type Decision struct {
	// Primary is the channel to try first.
	Primary *Channel
	// Fallbacks are tried in order when the primary fails.
	Fallbacks []*Channel
	// Model is the resolved model slug.
	Model string
}

// ErrModelNotFound is returned when no route exists for a model.
var ErrModelNotFound = errModelNotFound

type errModelNotFoundType struct{}

func (errModelNotFoundType) Error() string { return "model not found in routing table" }

var errModelNotFound errModelNotFoundType

// Clock abstracts time for testability.
type Clock interface {
	Now() time.Time
}

// RealClock is the production clock.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }
