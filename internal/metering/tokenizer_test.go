package metering

import (
	"sync"
	"testing"

	"github.com/differs/MeterGate/pkg/openai"
)

func TestApproxEstimator(t *testing.T) {
	est := ApproxEstimator{}
	cases := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"a", 1},     // 1 ascii -> 1/4=0 + 1 = 1
		{"hello", 2}, // 5 ascii -> 5/4=1 + 1 = 2
		{"你好世界", 3},  // 4 wide -> 4*2/3=2 + 1 = 3
		{"hello world", 3},
	}
	for _, c := range cases {
		if got := est.CountText(c.in); got != c.want {
			t.Errorf("CountText(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestEstimatePromptTokens(t *testing.T) {
	est := ApproxEstimator{}
	got := EstimatePromptTokens(est, []openai.ChatMessage{
		{Role: "system", Content: "you are a helpful assistant"},
		{Role: "user", Content: "hello world"},
	})
	// 4 + 7 ("you are a helpful assistant" 27 chars) + 4 + 3 ("hello world" 11 chars) = 18
	if got != 18 {
		t.Errorf("EstimatePromptTokens = %d, want 18", got)
	}
}

func TestAccumulatorConcurrent(t *testing.T) {
	acc := NewAccumulator(ApproxEstimator{}, 10)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acc.AddDelta("hello world token stream ")
			_ = acc.Snapshot()
		}()
	}
	wg.Wait()
	u := acc.Snapshot()
	if u.PromptTokens != 10 {
		t.Fatalf("prompt = %d, want 10", u.PromptTokens)
	}
	if u.CompletionTokens <= 0 {
		t.Fatalf("completion = %d, want > 0", u.CompletionTokens)
	}
	if u.TotalTokens != u.PromptTokens+u.CompletionTokens {
		t.Fatalf("total mismatch: %d != %d", u.TotalTokens, u.PromptTokens+u.CompletionTokens)
	}
}

func TestAccumulatorAbort(t *testing.T) {
	acc := NewAccumulator(ApproxEstimator{}, 0)
	if acc.Aborted() {
		t.Fatal("fresh accumulator must not be aborted")
	}
	acc.MarkAborted()
	if !acc.Aborted() {
		t.Fatal("expected aborted")
	}
}
