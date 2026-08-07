package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPUpstream is the M1 upstream: a single OpenAI-compatible endpoint
// resolved from configuration. The routing engine (price-weighted channel
// selection + 30s failure window + circuit breaker) replaces this in M3;
// the Upstream interface stays unchanged.
type HTTPUpstream struct {
	// Endpoint is the full chat completions URL, e.g.
	// "http://127.0.0.1:9901/v1/chat/completions".
	Endpoint string
	Key      string
	Client   *http.Client
}

// NewHTTPUpstream builds an upstream with sane timeouts.
func NewHTTPUpstream(endpoint, key string) *HTTPUpstream {
	return &HTTPUpstream{
		Endpoint: endpoint,
		Key:      key,
		Client: &http.Client{
			Timeout: 10 * time.Minute, // LLM streams can be long
			Transport: &http.Transport{
				MaxIdleConns:        1024,
				MaxIdleConnsPerHost: 512,
				MaxConnsPerHost:     0, // unlimited (gateway fan-out)
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Chat implements Upstream. Streaming responses are decoded chunk-by-chunk
// and handed to call.OnChunk (which forwards to the client and meters).
func (u *HTTPUpstream) Chat(ctx context.Context, call *ChatCall) (*ChatResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.Endpoint, bytes.NewReader(call.Body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+u.Key)

	resp, err := u.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if call.Stream {
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			return &ChatResult{StatusCode: resp.StatusCode, Body: body}, nil
		}
		// Feed decoded SSE payloads to the handler's OnChunk.
		if err := scanSSELines(resp.Body, func(data []byte) error {
			if call.OnChunk != nil {
				return call.OnChunk(data)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return &ChatResult{StatusCode: http.StatusOK}, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	result := &ChatResult{StatusCode: resp.StatusCode, Body: body}
	// Extract upstream usage for cross-validation with local metering.
	var parsed struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && json.Unmarshal(body, &parsed) == nil && parsed.Usage != nil {
		raw, _ := json.Marshal(parsed.Usage)
		result.UpstreamUsageRaw = string(raw)
	}
	call.UpstreamURL = u.Endpoint
	return result, nil
}

var _ Upstream = (*HTTPUpstream)(nil)

// ensure fmt stays referenced if file evolves (kept for error wrapping).
var _ = fmt.Sprintf
