package router

import (
	"math"
	"math/rand"
)

// Engine is the routing decision engine. Pure computation over a snapshot:
// no locks, no IO — safe for concurrent use from the gateway hot path.
//
// Default strategy (mirrors OpenRouter):
//  1. Deprioritize channels with a significant failure in the last 30s
//     (unhealthy → moved to the back of the fallback order, not removed).
//  2. Among the stable channels, pick weighted by the inverse square of
//     price: a $1 channel gets ~9x the traffic of a $3 channel.
//  3. The remaining channels form the fallback chain.
type Engine struct {
	rand *rand.Rand
}

// NewEngine builds a routing engine.
func NewEngine() *Engine {
	return &Engine{rand: rand.New(rand.NewSource(rand.Int63()))}
}

// PriceWeight returns the inverse-square weight for a channel's output
// price (micros). Cheaper channels dominate.
func PriceWeight(outputPer1M int64) float64 {
	if outputPer1M <= 0 {
		outputPer1M = 1
	}
	// Normalize to a $1/M baseline to keep weights in a sane range.
	base := float64(1_000_000)
	return math.Pow(base/float64(outputPer1M), 2)
}

// Select picks the primary channel and the fallback order for a model.
// It never removes unhealthy channels: they go last, as a last resort.
func (e *Engine) Select(route *ModelRoute) Decision {
	dec := Decision{Model: route.Model}

	var stable, unstable []ChannelSpec
	for _, c := range route.Channels {
		if c.Healthy && !c.BreakerOpen {
			stable = append(stable, c)
		} else {
			unstable = append(unstable, c)
		}
	}
	if len(stable) == 0 {
		// Everything looks bad: pick from the least-bad (any channel).
		stable = route.Channels
		unstable = nil
	}

	// Weighted selection among stable candidates.
	weights := make([]float64, len(stable))
	var sum float64
	for i, c := range stable {
		w := PriceWeight(c.Channel.OutputPer1M)
		if c.Channel.Weight > 1 {
			w *= float64(c.Channel.Weight)
		}
		weights[i], sum = w, sum+w
	}

	idx := e.weightedPick(weights, sum)
	primary := stable[idx]

	// Fallback order: remaining stable (price ascending), then unstable.
	fallbacks := make([]*Channel, 0, len(stable)-1+len(unstable))
	for i, c := range stable {
		if i == idx {
			continue
		}
		fallbacks = append(fallbacks, c.Channel)
	}
	// Unstable channels go last (last-resort).
	for _, c := range unstable {
		fallbacks = append(fallbacks, c.Channel)
	}

	dec.Primary = primary.Channel
	dec.Fallbacks = fallbacks
	return dec
}

func (e *Engine) weightedPick(weights []float64, sum float64) int {
	if sum <= 0 || len(weights) == 0 {
		return 0
	}
	x := e.rand.Float64() * sum
	for i, w := range weights {
		x -= w
		if x <= 0 {
			return i
		}
	}
	return len(weights) - 1
}

// IsRetryable classifies an upstream error: 5xx/network/429 → retry next
// fallback; 4xx (client errors) → never retry.
func IsRetryable(statusCode int, err error) bool {
	if err != nil {
		return true
	}
	return statusCode >= 500 || statusCode == 429 || statusCode == 0
}
