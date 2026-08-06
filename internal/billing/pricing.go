package billing

// Pricing table for M2. Prices are per 1M tokens, in micro-units of the
// base currency (so "$2.00/1M" = 2_000_000 micros/1M).
//
// M3 replaces this static table with the versioned model catalog from the
// control plane (price snapshots ride along with each metering event so
// late settlement always uses the price effective at request time).
type ModelPrice struct {
	InputPer1M  int64 // micros per 1M prompt tokens
	OutputPer1M int64 // micros per 1M completion tokens
}

var defaultPrices = map[string]ModelPrice{
	"gpt-4o":        {InputPer1M: 2_500_000, OutputPer1M: 10_000_000},
	"gpt-4o-mini":   {InputPer1M: 150_000, OutputPer1M: 600_000},
	"deepseek-chat": {InputPer1M: 270_000, OutputPer1M: 1_100_000},
}

// PriceFor returns the price for a model, falling back to a safe default
// so settlement never blocks on an unknown model.
func PriceFor(model string) ModelPrice {
	if p, ok := defaultPrices[model]; ok {
		return p
	}
	return ModelPrice{InputPer1M: 1_000_000, OutputPer1M: 2_000_000}
}

// CalculateAmount computes the charge in micro-units:
//
//	amount = prompt * inputPer1M / 1e6 + completion * outputPer1M / 1e6
//
// Integer math only; rounding is downward and bounded (≤ 1 micro per
// component), which is negligible at real token volumes and deterministic
// for reconciliation.
func CalculateAmount(prompt, completion int64, p ModelPrice) int64 {
	return prompt*p.InputPer1M/1_000_000 + completion*p.OutputPer1M/1_000_000
}

// EstimatePreCharge estimates a pre-charge amount for the fast path.
// The completion side is capped (min(max_tokens, cap)) so a malicious
// max_tokens cannot freeze an unbounded amount.
func EstimatePreCharge(promptTokens int64, maxTokens *int64, p ModelPrice) int64 {
	cap := int64(16_000)
	completionCap := cap
	if maxTokens != nil && *maxTokens < cap {
		completionCap = *maxTokens
	}
	est := CalculateAmount(promptTokens, completionCap, p)
	// 10% headroom to avoid rejecting legitimate requests.
	return est + est/10 + 1
}
