package router

import (
	"strings"

	"github.com/differs/MeterGate/pkg/openai"
)

// AutoRouter picks a model per request when the caller sends the
// "auto" model (OpenRouter's openrouter/auto equivalent). MeterGate's
// light-weight version is pure local computation: a complexity score
// derived from the prompt, gated by a cost/quality tradeoff dial
// (0 = always the most capable model, 10 = always the cheapest).
//
// Zero external calls, zero per-request latency beyond a few string
// scans — the heavy "LLM classifier" approach is deliberately avoided.
type AutoRouter interface {
	Pick(req *openai.ChatCompletionRequest) (string, error)
}

// AutoCandidate is one model in the auto pool.
type AutoCandidate struct {
	Model       string
	InputPer1M  int64
	OutputPer1M int64
}

// LocalAutoRouter is the default implementation.
type LocalAutoRouter struct {
	pool        []AutoCandidate
	costQuality int // 0-10, default 7
}

// NewLocalAutoRouter builds the router from a candidate pool.
func NewLocalAutoRouter(pool []AutoCandidate, costQuality int) *LocalAutoRouter {
	if costQuality < 0 {
		costQuality = 0
	}
	if costQuality > 10 {
		costQuality = 10
	}
	if len(pool) == 0 {
		pool = []AutoCandidate{{Model: "gpt-4o-mini", InputPer1M: 150_000, OutputPer1M: 600_000}}
	}
	return &LocalAutoRouter{pool: pool, costQuality: costQuality}
}

// reasoningHints are strong signals that the prompt needs a capable model.
var reasoningHints = []string{
	"reason", "explain", "analysis", "analyze", "debug", "optimize",
	"refactor", "compare", "synthesize", "evaluate", "prove", "derive",
	"design", "architecture", "strategy", "math", "calculus", "code",
	"algorithm", "complex",
}

// Pick implements AutoRouter.
func (r *LocalAutoRouter) Pick(req *openai.ChatCompletionRequest) (string, error) {
	if len(r.pool) == 1 {
		return r.pool[0].Model, nil
	}
	// Hard rules at the dial extremes (OpenRouter semantics):
	//   0 → always the most capable model
	//   10 → always the cheapest
	if r.costQuality == 0 {
		return r.mostCapable(), nil
	}
	if r.costQuality == 10 {
		return r.cheapest(), nil
	}
	complexity := r.complexity(req)
	// In between: threshold scales with the dial — higher dial = more
	// requests go to the capable model (quality bias).
	threshold := float64(10-r.costQuality) / 10 * 0.9
	if complexity >= threshold {
		return r.mostCapable(), nil
	}
	return r.cheapest(), nil
}

// complexity scores 0..1 from prompt length + reasoning hints + tools.
func (r *LocalAutoRouter) complexity(req *openai.ChatCompletionRequest) float64 {
	var chars int
	hasTool := false
	hintHits := 0
	for _, m := range req.Messages {
		switch c := m.Content.(type) {
		case string:
			chars += len(c)
			lower := strings.ToLower(c)
			for _, h := range reasoningHints {
				if strings.Contains(lower, h) {
					hintHits++
				}
			}
		}
	}
	// tool calls (function calling) → complex
	for _, m := range req.Messages {
		if m.Role == "tool" || m.Role == "function" {
			hasTool = true
		}
	}
	// max_tokens > 2000 → long generation → slightly more capable
	longOutput := req.MaxTokens != nil && *req.MaxTokens > 2000

	score := 0.0
	// length: 0 chars → 0, 10K chars → 1
	if chars > 0 {
		score += 0.4 * min(float64(chars)/10000, 1)
	}
	score += 0.3 * min(float64(hintHits)/3, 1)
	if hasTool {
		// tool-calling requests are agentic workloads — strong signal
		score += 0.5
	}
	if longOutput {
		score += 0.1
	}
	if score > 1 {
		return 1
	}
	return score
}

func (r *LocalAutoRouter) cheapest() string {
	best := r.pool[0]
	for _, c := range r.pool[1:] {
		if c.OutputPer1M < best.OutputPer1M {
			best = c
		}
	}
	return best.Model
}

func (r *LocalAutoRouter) mostCapable() string {
	best := r.pool[0]
	for _, c := range r.pool[1:] {
		if c.OutputPer1M > best.OutputPer1M {
			best = c
		}
	}
	return best.Model
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
