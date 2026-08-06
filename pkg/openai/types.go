// Package openai defines the OpenAI-compatible wire protocol types used by
// MeterGate's data plane. Unknown request fields are preserved via Raw so the
// gateway can act as a transparent proxy while still reading what it needs
// (model, stream flag, message content) for routing and metering.
package openai

import (
	"encoding/json"
	"time"
)

// ChatMessage is a single message in the conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"` // string or []ContentPart (multimodal)
}

// ChatCompletionRequest is the OpenAI-compatible chat completion request.
// Raw keeps the exact body so the gateway forwards byte-identical payloads.
type ChatCompletionRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
	// MaxTokens is only read for pre-charge estimation.
	MaxTokens *int `json:"max_tokens,omitempty"`
	// Raw is the untouched request body for transparent forwarding.
	Raw json.RawMessage `json:"-"`
}

// Usage mirrors the upstream usage object. It is the authoritative billing
// input when present; the gateway's local metering is cross-checked against it.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatCompletionResponse is the non-streaming response.
type ChatCompletionResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int         `json:"index"`
		Message      ChatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage `json:"usage,omitempty"`
}

// ChatCompletionChunk is one SSE data payload of a streaming response.
type ChatCompletionChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

// ChunkChoice carries the incremental delta of a stream chunk.
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// Delta is the incremental content piece of a stream chunk.
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// StreamEvent is a parsed SSE line: either a data payload or the [DONE] sentinel.
type StreamEvent struct {
	Data []byte
	Done bool
}

// ParseChunk decodes a raw SSE data payload into a chunk.
func ParseChunk(data []byte) (*ChatCompletionChunk, error) {
	var c ChatCompletionChunk
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// NewChunk builds a minimal chunk (used when upstream chunks must be
// re-encoded or for tests).
func NewChunk(id, model string) *ChatCompletionChunk {
	return &ChatCompletionChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
	}
}
