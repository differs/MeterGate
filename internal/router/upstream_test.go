package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/differs/MeterGate/internal/gateway"
)

// TestRoutingUpstreamFailover: primary channel down (5xx) → request
// succeeds via fallback; provider is stamped.
func TestRoutingUpstreamFailover(t *testing.T) {
	var sickHits, okHits int
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sickHits++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer sick.Close()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","object":"chat.completion","model":"m","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":5,"total_tokens":10}}`))
	}))
	defer ok.Close()

	chA := &Channel{ID: "a", BaseURL: sick.URL, Key: "k", OutputPer1M: 1_000_000}
	chB := &Channel{ID: "b", BaseURL: ok.URL, Key: "k", OutputPer1M: 2_000_000}
	snap := &Snapshot{Version: 1, Models: map[string]*ModelRoute{
		"m": mkRoute("m", chA, chB),
	}}
	r := NewRouter(snap)
	up := NewRoutingUpstream(r)

	call := &gateway.ChatCall{Model: "m", Body: []byte(`{"model":"m"}`)}
	res, err := up.Chat(context.Background(), call)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if okHits != 1 || sickHits != 1 {
		t.Fatalf("hits: sick=%d ok=%d, want 1/1 (failover must happen)", sickHits, okHits)
	}
	if call.Provider != "b" {
		t.Fatalf("provider = %q, want b (winning channel)", call.Provider)
	}
	// After the failure, channel a is unhealthy; b is primary.
	if h, st := r.ChannelHealth("a"); h || st != stateClosed {
		t.Logf("channel a health: healthy=%v breaker=%s", h, st)
	}
}

// TestRoutingUpstreamClientErrorNoRetry: 400 must not hit fallback.
func TestRoutingUpstreamClientErrorNoRetry(t *testing.T) {
	var badHits, okHits int
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer bad.Close()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okHits++
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	chA := &Channel{ID: "a", BaseURL: bad.URL, Key: "k", OutputPer1M: 1_000_000}
	chB := &Channel{ID: "b", BaseURL: ok.URL, Key: "k", OutputPer1M: 2_000_000}
	snap := &Snapshot{Version: 1, Models: map[string]*ModelRoute{"m": mkRoute("m", chA, chB)}}
	r := NewRouter(snap)
	up := NewRoutingUpstream(r)

	call := &gateway.ChatCall{Model: "m", Body: []byte(`{"model":"m"}`)}
	_, err := up.Chat(context.Background(), call)
	if err == nil {
		t.Fatal("expected error for 400")
	}
	if okHits != 0 {
		t.Fatalf("fallback must NOT be tried on 4xx (okHits=%d)", okHits)
	}
}
