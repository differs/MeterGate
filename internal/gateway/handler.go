package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/differs/MeterGate/internal/metering"
	"github.com/differs/MeterGate/pkg/openai"
)

// handleChat serves POST /v1/chat/completions.
// Flow: decode request → estimate prompt → forward → meter streaming →
// emit one metering event at termination.
func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := ctx.Value(ctxKeyUserID).(string)

	// Read the full body once: needed for transparent forwarding AND
	// prompt estimation. (Bodies are small; upstreams cap input sizes.)
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req openai.ChatCompletionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	req.Raw = body
	if req.Model == "" {
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}

	call := &ChatCall{
		RequestID: s.requestID(),
		UserID:    userID,
		Model:     req.Model,
		Stream:    req.Stream,
		Body:      body,
		// M1: upstream resolved from env config by the concrete upstream
		// implementation; the routing engine replaces this in M3.
	}

	// Prompt metering (approximate; exact prompt tokens come from upstream).
	est := metering.ApproxEstimator{}
	promptTokens := metering.EstimatePromptTokens(est, req.Messages)

	// Billing fast path: reserve funds before touching the upstream.
	if s.preCharge != nil {
		if err := s.preCharge(ctx, userID, call.RequestID, req.Model, int64(promptTokens), maxTokensPtr(req.MaxTokens)); err != nil {
			writeError(w, http.StatusPaymentRequired, "insufficient balance or pre-charge rejected")
			return
		}
	}

	if req.Stream {
		s.handleStream(ctx, w, call, promptTokens, est)
		return
	}
	s.handleNonStream(ctx, w, call, promptTokens)
}

func (s *Server) handleNonStream(ctx context.Context, w http.ResponseWriter, call *ChatCall, promptTokens int) {
	start := s.now()
	result, err := s.upstream.Chat(ctx, call)
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream error: "+err.Error())
		return
	}
	status := metering.StatusCompleted
	completionTokens := 0
	var usage *openai.Usage
	if result.StatusCode >= 200 && result.StatusCode < 300 {
		// Best-effort parse of upstream usage for cross-checking.
		var resp openai.ChatCompletionResponse
		if json.Unmarshal(result.Body, &resp) == nil && resp.Usage != nil {
			usage = resp.Usage
			completionTokens = usage.CompletionTokens
		}
	} else {
		status = metering.StatusFailed
	}
	w.WriteHeader(result.StatusCode)
	_, _ = w.Write(result.Body)

	usageRaw := ""
	if usage != nil {
		usageRaw = string(mustJSON(usage))
	}
	s.emit(metering.Event{
		RequestID:        call.RequestID,
		UserID:           call.UserID,
		Model:            call.Model,
		Provider:         providerOf(call),
		Status:           status,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		DurationMs:       s.now().Sub(start).Milliseconds(),
		UpstreamUsageRaw: usageRaw,
	})
}

// handleStream transparently forwards SSE while accumulating local token
// metering on every chunk. It is the hot path: zero allocations beyond the
// per-chunk parse, and it always drains the upstream before returning.
func (s *Server) handleStream(ctx context.Context, w http.ResponseWriter, call *ChatCall, promptTokens int, est metering.TokenEstimator) {
	acc := metering.NewAccumulator(est, promptTokens)
	start := s.now()

	// Stream body through: response headers must reach the client before
	// the first chunk, so set them here and have the upstream write to the
	// response directly.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	call.OnChunk = func(data []byte) error {
		chunk, err := openai.ParseChunk(data)
		if err != nil {
			return err
		}
		for _, c := range chunk.Choices {
			if c.Delta.Content != "" {
				acc.AddDelta(c.Delta.Content)
			}
		}
		// Forward verbatim.
		_, werr := w.Write(append(append([]byte("data: "), data...), '\n', '\n'))
		flusher.Flush()
		return werr
	}

	_, err := s.upstream.Chat(ctx, call)
	status := metering.StatusCompleted
	if err != nil {
		status = metering.StatusFailed
		acc.MarkAborted()
		// Upstream died before [DONE]; terminate the client stream.
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}

	u := acc.Snapshot()
	s.emit(metering.Event{
		RequestID:        call.RequestID,
		UserID:           call.UserID,
		Model:            call.Model,
		Provider:         providerOf(call),
		Status:           status,
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		TTFTMs:           s.now().Sub(start).Milliseconds(),
		DurationMs:       s.now().Sub(start).Milliseconds(),
	})
}

func (s *Server) emit(ev metering.Event) {
	ev.Timestamp = s.now()
	// Always log (audit trail, zero-dependency default sink).
	b, _ := json.Marshal(ev)
	s.log.Info("metering", "event", string(b))
	// Forward to the billing pipeline when wired (M2+).
	if s.sink != nil {
		_ = s.sink.Emit(ev)
	}
}

// --- helpers ---------------------------------------------------------------

func providerOf(call *ChatCall) string {
	if call.Provider != "" {
		return call.Provider
	}
	return call.UpstreamURL
}

func maxTokensPtr(v *int) *int64 {
	if v == nil {
		return nil
	}
	vv := int64(*v)
	return &vv
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": msg, "type": "metergate_error"},
	})
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// scanSSELines is a helper for upstream implementations that read SSE
// payloads line by line (see upstreams.go).
func scanSSELines(r io.Reader, onData func([]byte) error) error {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		trimmed := bytes.TrimSpace(line)
		if !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(trimmed, []byte("data:")))
		if len(payload) == 0 {
			continue
		}
		if string(payload) == "[DONE]" {
			return nil
		}
		if err := onData(payload); err != nil {
			return err
		}
	}
}

// requestSSE performs an SSE POST and feeds decoded payloads to onData.
// Used by the concrete upstream in upstreams.go.
func requestSSE(url, key string, body []byte, onData func([]byte) error) (int, error) {
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, fmt.Errorf("upstream status %d", resp.StatusCode)
	}
	return resp.StatusCode, scanSSELines(resp.Body, onData)
}
