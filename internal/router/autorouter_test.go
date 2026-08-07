package router

import (
	"testing"

	"github.com/differs/MeterGate/pkg/openai"
)

func mkAutoReq(content string) *openai.ChatCompletionRequest {
	return &openai.ChatCompletionRequest{
		Messages: []openai.ChatMessage{{Role: "user", Content: content}},
	}
}

// TestAutoRouterCostQuality0: dial 0 → always most capable.
func TestAutoRouterCostQuality0(t *testing.T) {
	r := NewLocalAutoRouter([]AutoCandidate{
		{Model: "cheap", OutputPer1M: 100_000},
		{Model: "capable", OutputPer1M: 10_000_000},
	}, 0)
	// even trivial prompts go to the capable model
	if m, _ := r.Pick(mkAutoReq("hi")); m != "capable" {
		t.Fatalf("dial 0 picked %s, want capable", m)
	}
}

// TestAutoRouterCostQuality10: dial 10 → always cheapest.
func TestAutoRouterCostQuality10(t *testing.T) {
	r := NewLocalAutoRouter([]AutoCandidate{
		{Model: "cheap", OutputPer1M: 100_000},
		{Model: "capable", OutputPer1M: 10_000_000},
	}, 10)
	if m, _ := r.Pick(mkAutoReq("analyze and optimize this complex algorithm with proof")); m != "cheap" {
		t.Fatalf("dial 10 picked %s, want cheap", m)
	}
}

// TestAutoRouterDefault: default dial 7 → simple prompts cheap, complex
// prompts capable.
func TestAutoRouterDefault(t *testing.T) {
	r := NewLocalAutoRouter([]AutoCandidate{
		{Model: "cheap", OutputPer1M: 100_000},
		{Model: "capable", OutputPer1M: 10_000_000},
	}, 7)

	if m, _ := r.Pick(mkAutoReq("hello")); m != "cheap" {
		t.Fatalf("simple prompt picked %s, want cheap", m)
	}
	complexPrompt := "Please design the architecture of a distributed system, explain the reasoning behind each decision, analyze the tradeoffs, evaluate the failure modes, and prove the correctness of the algorithm with a formal derivation"
	if m, _ := r.Pick(mkAutoReq(complexPrompt)); m != "capable" {
		t.Fatalf("complex prompt picked %s, want capable", m)
	}
}

// TestAutoRouterSingleCandidate: pool of one → always that model.
func TestAutoRouterSingleCandidate(t *testing.T) {
	r := NewLocalAutoRouter([]AutoCandidate{{Model: "only", OutputPer1M: 1}}, 7)
	if m, _ := r.Pick(mkAutoReq("anything")); m != "only" {
		t.Fatalf("picked %s, want only", m)
	}
}

// TestAutoRouterTools: tool-calling requests score complex.
func TestAutoRouterTools(t *testing.T) {
	r := NewLocalAutoRouter([]AutoCandidate{
		{Model: "cheap", OutputPer1M: 100_000},
		{Model: "capable", OutputPer1M: 10_000_000},
	}, 7)
	req := &openai.ChatCompletionRequest{
		Messages: []openai.ChatMessage{
			{Role: "user", Content: "call the function"},
			{Role: "tool", Content: `{"result": 42}`},
		},
	}
	if m, _ := r.Pick(req); m != "capable" {
		t.Fatalf("tool request picked %s, want capable", m)
	}
}
