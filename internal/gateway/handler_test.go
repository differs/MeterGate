package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeUpstream implements Upstream with scripted behavior.
type fakeUpstream struct {
	status int
	body   string
	chunks []string
	last   *ChatCall
}

func (f *fakeUpstream) Chat(_ context.Context, call *ChatCall) (*ChatResult, error) {
	f.last = call
	if call.Stream {
		for _, c := range f.chunks {
			if err := call.OnChunk([]byte(c)); err != nil {
				return nil, err
			}
		}
		return &ChatResult{StatusCode: http.StatusOK}, nil
	}
	return &ChatResult{StatusCode: f.status, Body: []byte(f.body)}, nil
}

func newTestServer(f *fakeUpstream) *httptest.Server {
	srv := NewServer(f, WithKeys([]string{"sk-test"}))
	ts := httptest.NewServer(srv.Handler())
	return ts
}

func TestChatNonStream(t *testing.T) {
	f := &fakeUpstream{
		status: http.StatusOK,
		body:   `{"id":"1","object":"chat.completion","model":"gpt-4o","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`,
	}
	ts := newTestServer(f)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// Request ID must be present on the upstream call.
	if f.last == nil || f.last.RequestID == "" {
		t.Fatal("upstream call missing request ID")
	}
}

func TestChatStream(t *testing.T) {
	f := &fakeUpstream{
		chunks: []string{
			`{"id":"1","object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"hello "},"finish_reason":null}]}`,
			`{"id":"1","object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"world"},"finish_reason":null}]}`,
			`{"id":"1","object":"chat.completion.chunk","model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		},
	}
	ts := newTestServer(f)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want SSE", ct)
	}
}

func TestAuthRejected(t *testing.T) {
	f := &fakeUpstream{}
	ts := newTestServer(f)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-4o","messages":[]}`))
	req.Header.Set("Authorization", "Bearer sk-wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if f.last != nil {
		t.Fatal("upstream must not be called on auth failure")
	}
}
