package reconciliation

import (
	"context"
	"log/slog"
	"testing"

	"github.com/differs/MeterGate/internal/billing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// memUsageAggregator simulates our side's aggregated usage.
type memUsageAggregator struct {
	usage map[string]billing.MyUsageRow
}

func (m *memUsageAggregator) SumProviderUsage(_ context.Context, _ string) (map[string]billing.MyUsageRow, error) {
	return m.usage, nil
}

// mockBillSource simulates a supplier bill; allow injecting a mismatch.
type mockBillSource struct {
	bills map[string]*ProviderBill
}

func (m *mockBillSource) FetchDay(_ context.Context, provider string, _ string) (*ProviderBill, error) {
	return m.bills[provider], nil
}

func TestProviderReconWithinTolerance(t *testing.T) {
	agg := &memUsageAggregator{usage: map[string]billing.MyUsageRow{
		"openai": {PromptTok: 1_000_000, ComplTok: 500_000, TotalTok: 1_500_000},
	}}
	// bill matches within 万分之五 (0.05%)
	src := &mockBillSource{bills: map[string]*ProviderBill{
		"openai": {Provider: "openai", PromptTok: 1_000_400, ComplTok: 500_200},
	}}
	rc := NewProviderReconciler(agg, src, 0.0005, testLogger())
	diffs, err := rc.Run(context.Background(), "2026-08-07")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("diffs = %d, want 1", len(diffs))
	}
	if !diffs[0].WithinTolerance {
		t.Fatalf("within tolerance: prompt %.4f%% compl %.4f%%",
			diffs[0].PromptDiffPct*100, diffs[0].ComplDiffPct*100)
	}
}

func TestProviderReconMismatchFlagged(t *testing.T) {
	agg := &memUsageAggregator{usage: map[string]billing.MyUsageRow{
		"deepseek": {PromptTok: 10_000_000, ComplTok: 5_000_000, TotalTok: 15_000_000},
	}}
	// bill says we used 20% more than we recorded — supplier billed us for
	// something we don't see (retries/cached tokens misattribution)
	src := &mockBillSource{bills: map[string]*ProviderBill{
		"deepseek": {Provider: "deepseek", PromptTok: 12_000_000, ComplTok: 6_000_000},
	}}
	rc := NewProviderReconciler(agg, src, 0.0005, testLogger())
	diffs, err := rc.Run(context.Background(), "2026-08-07")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 || diffs[0].WithinTolerance {
		t.Fatalf("mismatch must be flagged: %+v", diffs)
	}
	// 20% diff on both classes
	if diffs[0].PromptDiffPct < 0.15 || diffs[0].PromptDiffPct > 0.18 {
		t.Fatalf("prompt diff = %.2f, want ~0.167", diffs[0].PromptDiffPct)
	}
}

func TestProviderReconMissingBill(t *testing.T) {
	agg := &memUsageAggregator{usage: map[string]billing.MyUsageRow{
		"openai": {PromptTok: 1, ComplTok: 1},
	}}
	src := &mockBillSource{bills: map[string]*ProviderBill{}} // no bill
	rc := NewProviderReconciler(agg, src, 0.0005, testLogger())
	diffs, _ := rc.Run(context.Background(), "2026-08-07")
	if len(diffs) != 0 {
		t.Fatalf("missing bill must be skipped, got %d diffs", len(diffs))
	}
}
