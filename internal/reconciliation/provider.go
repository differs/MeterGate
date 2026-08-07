package reconciliation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/differs/MeterGate/internal/billing"
)

// ProviderBill is one day's supplier-side usage/cost for one provider.
type ProviderBill struct {
	Provider   string
	Day        string // YYYY-MM-DD
	PromptTok  int64
	ComplTok   int64
	CacheTok   int64
	CostMicros int64 // supplier-side cost in micro-units
}

// ProviderBillSource fetches supplier bills (API / CSV / portal).
// Implementations: real provider APIs, or a mock for testing.
type ProviderBillSource interface {
	FetchDay(ctx context.Context, provider string, day string) (*ProviderBill, error)
}

// ProviderDiff is one provider-day comparison result.
type ProviderDiff struct {
	Provider string
	Day      string
	Mine     billing.MyUsageRow
	Theirs   ProviderBill
	// TokenDiffPct: |mine - theirs| / theirs per token class.
	PromptDiffPct   float64
	ComplDiffPct    float64
	CostDiffPct     float64
	WithinTolerance bool
}

// ProviderReconciler compares our aggregated usage against supplier bills.
// This is reconciliation Layer 2: it answers "is our gross margin real?".
type ProviderReconciler struct {
	store     UsageAggregator
	source    ProviderBillSource
	tolerance float64 // e.g. 0.0005 (万分之五)
	log       *slog.Logger
}

// UsageAggregator sums our side's usage per provider per day.
type UsageAggregator interface {
	SumProviderUsage(ctx context.Context, day string) (map[string]billing.MyUsageRow, error)
}

// NewProviderReconciler builds Layer-2 reconciliation.
func NewProviderReconciler(store UsageAggregator, source ProviderBillSource, tolerance float64, log *slog.Logger) *ProviderReconciler {
	if tolerance <= 0 {
		tolerance = 0.0005
	}
	if log == nil {
		log = slog.Default()
	}
	return &ProviderReconciler{store: store, source: source, tolerance: tolerance, log: log}
}

// Run compares every provider's daily usage with the supplier bill.
func (r *ProviderReconciler) Run(ctx context.Context, day string) ([]ProviderDiff, error) {
	mine, err := r.store.SumProviderUsage(ctx, day)
	if err != nil {
		return nil, err
	}
	var diffs []ProviderDiff
	for provider, my := range mine {
		bill, err := r.source.FetchDay(ctx, provider, day)
		if err != nil {
			r.log.Error("supplier bill fetch failed", "provider", provider, "day", day, "err", err)
			continue
		}
		if bill == nil {
			r.log.Warn("no supplier bill", "provider", provider, "day", day)
			continue
		}
		d := ProviderDiff{
			Provider:      provider,
			Day:           day,
			Mine:          my,
			Theirs:        *bill,
			PromptDiffPct: pctDiff(my.PromptTok, bill.PromptTok),
			ComplDiffPct:  pctDiff(my.ComplTok, bill.ComplTok),
			CostDiffPct:   pctDiff(my.CostMicros, bill.CostMicros),
		}
		d.WithinTolerance = d.PromptDiffPct <= r.tolerance &&
			d.ComplDiffPct <= r.tolerance &&
			d.CostDiffPct <= r.tolerance
		if !d.WithinTolerance {
			r.log.Warn("provider usage mismatch",
				"provider", provider, "day", day,
				"prompt_pct", fmt.Sprintf("%.4f%%", d.PromptDiffPct*100),
				"compl_pct", fmt.Sprintf("%.4f%%", d.ComplDiffPct*100),
				"cost_pct", fmt.Sprintf("%.4f%%", d.CostDiffPct*100))
		}
		diffs = append(diffs, d)
	}
	return diffs, nil
}

func pctDiff(mine, theirs int64) float64 {
	if theirs == 0 {
		if mine == 0 {
			return 0
		}
		return 1 // 100%: we have usage, bill says zero
	}
	diff := float64(mine - theirs)
	if diff < 0 {
		diff = -diff
	}
	return diff / float64(theirs)
}

// Ensure time import is used (day parsing helpers may be added later).
var _ = time.Now
