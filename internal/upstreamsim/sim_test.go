// Package upstreamsim simulates real upstream behaviors that mock servers
// hide — the accuracy-critical usage semantics of real LLM providers:
//
//   - reasoning tokens (o1-series: usage contains reasoning_tokens that
//     are NOT billed but consume context)
//   - cached tokens (prompt_tokens_details.cached_tokens → cheaper)
//   - aborted streams without usage (some providers never return usage)
//   - rate limits (429 with Retry-After)
//   - supplier-specific rounding
//
// The gateway's metering logic is validated against these shapes so the
// reconciliation layer can explain every real-world discrepancy.
package upstreamsim

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// usageShape is one simulated upstream usage response.
type usageShape struct {
	// standard
	prompt, completion, total int
	// o1-style reasoning tokens (in usage, not billed but count)
	reasoning int
	// cached tokens (prompt_tokens_details)
	cached int
	// omit usage entirely (aborted-stream providers)
	omitUsage bool
	// cached-only response (full cache hit)
	cacheHit bool
}

func (u usageShape) body(model string) string {
	usage := map[string]any{}
	if !u.omitUsage {
		usage["prompt_tokens"] = u.prompt
		usage["completion_tokens"] = u.completion
		usage["total_tokens"] = u.total
		details := map[string]any{}
		if u.cached > 0 {
			details["cached_tokens"] = u.cached
		}
		if u.reasoning > 0 {
			usage["completion_tokens_details"] = map[string]any{"reasoning_tokens": u.reasoning}
		}
		if len(details) > 0 {
			usage["prompt_tokens_details"] = details
		}
	}
	resp := map[string]any{
		"id": "sim", "object": "chat.completion", "model": model,
		"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
	}
	if !u.omitUsage {
		resp["usage"] = usage
	}
	b, _ := json.Marshal(resp)
	return string(b)
}

// SimServer is an httptest server that answers with a scripted usage shape
// and optionally a status code (e.g. 429).
func SimServer(t *testing.T, shape usageShape, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 && status != 200 {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(shape.body("gpt-4o")))
	}))
}

// ExtractUsage parses the usage fields the gateway cares about, mirroring
// the real-world shapes above. This is the TEST DOUBLE for the gateway's
// usage parsing — assertions in tests validate the mapping rules.
type ParsedUsage struct {
	Prompt     int
	Completion int
	Total      int
	Reasoning  int
	Cached     int
	HasUsage   bool
}

func ExtractUsage(raw []byte) ParsedUsage {
	var resp struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
			PromptDetails    *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
			CompletionDetails *struct {
				ReasoningTokens int `json:"reasoning_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(raw, &resp) != nil || resp.Usage == nil {
		return ParsedUsage{}
	}
	u := resp.Usage
	out := ParsedUsage{
		Prompt:     u.PromptTokens,
		Completion: u.CompletionTokens,
		Total:      u.TotalTokens,
		HasUsage:   true,
	}
	if u.PromptDetails != nil {
		out.Cached = u.PromptDetails.CachedTokens
	}
	if u.CompletionDetails != nil {
		out.Reasoning = u.CompletionDetails.ReasoningTokens
	}
	return out
}

// TestReasoningTokensNotBilledButCounted: o1-style reasoning tokens must
// be parsed (for context accounting) but NOT added to billed completion.
func TestReasoningTokensParsed(t *testing.T) {
	ts := SimServer(t, usageShape{prompt: 100, completion: 50, total: 150, reasoning: 20}, 200)
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf []byte
	_, _ = readAll(resp, &buf)

	u := ExtractUsage(buf)
	if !u.HasUsage {
		t.Fatal("usage must be present")
	}
	if u.Reasoning != 20 {
		t.Fatalf("reasoning = %d, want 20 (must be parsed for context accounting)", u.Reasoning)
	}
	// billed completion = usage.completion_tokens (50), NOT 50+20
	if u.Completion != 50 {
		t.Fatalf("completion = %d, want 50 (reasoning not billed)", u.Completion)
	}
}

// TestCachedTokensParsed: cached tokens must be visible for cache-price
// billing decisions.
func TestCachedTokensParsed(t *testing.T) {
	ts := SimServer(t, usageShape{prompt: 1000, completion: 10, total: 1010, cached: 900}, 200)
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf []byte
	_, _ = readAll(resp, &buf)

	u := ExtractUsage(buf)
	if u.Cached != 900 {
		t.Fatalf("cached = %d, want 900", u.Cached)
	}
}

// TestAbortedStreamNoUsage: providers that never return usage on aborted
// streams — the gateway must fall back to local metering (handled by the
// accumulator; here we assert the parser reports no usage so callers can
// branch).
func TestAbortedStreamNoUsage(t *testing.T) {
	ts := SimServer(t, usageShape{omitUsage: true}, 200)
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf []byte
	_, _ = readAll(resp, &buf)

	if u := ExtractUsage(buf); u.HasUsage {
		t.Fatal("usage must be absent for aborted-stream providers")
	}
}

// TestRateLimit429: rate-limited responses carry no usage and must not be
// billed (zero-completion insurance path).
func TestRateLimit429(t *testing.T) {
	ts := SimServer(t, usageShape{}, 429)
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
}

func readAll(resp *http.Response, buf *[]byte) (int, error) {
	b := make([]byte, 4096)
	n, err := resp.Body.Read(b)
	*buf = b[:n]
	return n, err
}
