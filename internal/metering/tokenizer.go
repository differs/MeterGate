// Package metering implements the gateway-side token metering primitives.
//
// Design notes (see docs/ in the repository root for the full architecture):
//   - The gateway meters tokens locally while streaming, independent of the
//     upstream `usage` field. The two numbers are cross-checked at billing
//     time; a >1% variance triggers an audit event.
//   - M1 uses an approximate estimator (no external tokenizer dependency).
//     The TokenEstimator interface is the seam where tiktoken-cl100k (or a
//     Rust core via FFI) plugs in later without touching callers.
package metering

import (
	"sync"

	"github.com/differs/MeterGate/pkg/openai"
)

// TokenEstimator counts tokens for a text fragment.
type TokenEstimator interface {
	CountText(s string) int
}

// ApproxEstimator is a dependency-free approximation:
//   - ASCII text: ~4 chars per token (matches cl100k English behavior)
//   - wide text (CJK etc.): ~1.5 chars per token
//
// Accuracy is sufficient for pre-charge estimation and cross-checking;
// exact billing always prefers the upstream `usage` object.
type ApproxEstimator struct{}

// CountText implements TokenEstimator.
func (ApproxEstimator) CountText(s string) int {
	var ascii, wide int
	for _, r := range s {
		if r < 0x80 {
			ascii++
		} else {
			wide++
		}
	}
	if ascii == 0 && wide == 0 {
		return 0
	}
	return ascii/4 + wide*2/3 + 1
}

// Usage is a point-in-time metering snapshot.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Accumulator incrementally meters a streaming response. Safe for
// concurrent use (the SSE reader goroutine and the request controller both
// touch it).
type Accumulator struct {
	est        TokenEstimator
	mu         sync.Mutex
	prompt     int
	completion int
	aborted    bool
}

// NewAccumulator creates an accumulator with the given estimator.
func NewAccumulator(est TokenEstimator, promptTokens int) *Accumulator {
	return &Accumulator{est: est, prompt: promptTokens}
}

// AddDelta meters one streamed content fragment.
func (a *Accumulator) AddDelta(content string) {
	if content == "" {
		return
	}
	n := a.est.CountText(content)
	a.mu.Lock()
	a.completion += n
	a.mu.Unlock()
}

// MarkAborted records that the stream ended before completion (client
// disconnect or upstream error). Billing treats this per the channel policy.
func (a *Accumulator) MarkAborted() {
	a.mu.Lock()
	a.aborted = true
	a.mu.Unlock()
}

// Aborted reports whether the stream was marked aborted.
func (a *Accumulator) Aborted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.aborted
}

// Snapshot returns the current metering state.
func (a *Accumulator) Snapshot() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Usage{
		PromptTokens:     a.prompt,
		CompletionTokens: a.completion,
		TotalTokens:      a.prompt + a.completion,
	}
}

// EstimatePromptTokens estimates prompt size from request messages.
// Content may be a plain string or a multimodal part array; only text
// parts meter as tokens (vision cost calculation lands in M4+).
func EstimatePromptTokens(est TokenEstimator, messages []openai.ChatMessage) int {
	total := 0
	for _, m := range messages {
		total += 4 // role overhead per message
		switch c := m.Content.(type) {
		case string:
			total += est.CountText(c)
		case []any:
			for _, part := range c {
				if pm, ok := part.(map[string]any); ok {
					if text, ok := pm["text"].(string); ok {
						total += est.CountText(text)
					}
				}
			}
		}
	}
	return total
}
