// Package reconciliation implements MeterGate's reconciliation engine —
// the money-integrity layer of the platform:
//
//   - Layer A (internal): order-row integrity (anomalies, day summary)
//   - Layer B (ledger vs runtime): PostgreSQL orders vs Redis frozen
//     balance — leaked pre-charges surface here
//   - Layer C (day close): full-day close with auto-refunds for detected
//     differences (small amounts auto-executed, large amounts manual)
//
// Auto-refund rules (M4):
//
//	negative-amount orders   → CREDIT |amount| (AUTO, idempotent)
//	duplicate charges        → CREDIT duplicate (detected via replay)
//	leaked frozen balance    → reported (MANUAL — attribution needed)
//
// Every refund is an independent ledger entry keyed by idempotency_key;
// executing it credits the user's Redis balance and marks the refund
// EXECUTED. Nothing ever mutates the original order row.
package reconciliation

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/differs/MeterGate/internal/billing"
)

// Report summarizes one reconciliation run.
type Report struct {
	Day           string
	OrderCount    int64
	SettledCount  int64
	NoChargeCnt   int64
	TotalAmount   int64
	Anomalies     int
	RefundsAuto   int
	RefundsManual int
	FrozenLeaked  int64
}

// Reconciler runs the reconciliation layers.
type Reconciler struct {
	orders  billing.OrderStore
	refunds billing.RefundStore
	pre     *billing.Precharger // may be nil (no Redis)
	log     *slog.Logger
	// AutoThresholdMicros: refunds at or below this amount execute
	// automatically; above → MANUAL approval.
	AutoThresholdMicros int64
}

// New builds a Reconciler.
func New(orders billing.OrderStore, refunds billing.RefundStore, pre *billing.Precharger, log *slog.Logger) *Reconciler {
	return &Reconciler{
		orders:              orders,
		refunds:             refunds,
		pre:                 pre,
		log:                 log,
		AutoThresholdMicros: 100_000_000, // 100 units of base currency
	}
}

// RunDay executes Layer A + B + C for one day and returns the report.
// When autoRefund is true, eligible differences produce refund entries
// (AUTO level executed immediately; MANUAL left pending).
func (r *Reconciler) RunDay(ctx context.Context, day string, autoRefund bool) (*Report, error) {
	rep := &Report{Day: day}

	// --- Layer A: order summary + anomalies ---
	summary, err := r.orders.Summary(ctx, day)
	if err != nil {
		return nil, err
	}
	for _, st := range []string{billing.StatusSettled, billing.StatusNoCharge} {
		s := summary[st]
		rep.OrderCount += s.Count
		rep.TotalAmount += s.AmountMicros
		if st == billing.StatusSettled {
			rep.SettledCount = s.Count
		} else {
			rep.NoChargeCnt = s.Count
		}
	}

	anomalies, err := r.orders.Anomalies(ctx, day)
	if err != nil {
		return nil, err
	}
	rep.Anomalies = len(anomalies)
	for _, a := range anomalies {
		r.log.Warn("order anomaly", "day", day, "detail", a)
	}

	// --- Layer B: frozen balance leak check (needs Redis) ---
	if r.pre != nil {
		frozen, err := r.pre.FrozenBalance(ctx)
		if err != nil {
			return nil, err
		}
		rep.FrozenLeaked = frozen
		if frozen > 0 {
			r.log.Warn("frozen balance leak (needs manual attribution)",
				"day", day, "frozen_micros", frozen)
		}
	}

	// --- Auto-refunds (only when enabled and differences exist) ---
	if autoRefund {
		if err := r.autoRefundNegativeAmounts(ctx, day, rep); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

// autoRefundNegativeAmounts issues CREDIT refunds for negative-amount
// orders (a settle bug would produce them; the user must never pay for
// our bug). Small amounts execute immediately, large amounts pend for
// manual approval.
func (r *Reconciler) autoRefundNegativeAmounts(ctx context.Context, day string, rep *Report) error {
	neg, err := r.orders.NegativeAmountOrders(ctx, day)
	if err != nil {
		return err
	}
	for _, o := range neg {
		amount := -o.AmountMicros
		level := billing.ApprovalAuto
		status := billing.RefundPending
		if amount > r.AutoThresholdMicros {
			level = billing.ApprovalManual
		}
		id, created, err := r.refunds.InsertRefund(ctx, billing.Refund{
			RequestID:      o.RequestID,
			UserID:         o.UserID,
			OrderID:        o.RequestID,
			ReasonCode:     billing.ReasonNegativeAmount,
			AmountMicros:   amount,
			Direction:      billing.DirectionCredit,
			Status:         status,
			ApprovalLevel:  level,
			IdempotencyKey: "neg:" + o.RequestID,
		})
		if err != nil {
			return err
		}
		if !created {
			continue // already refunded in a previous run
		}

		if level == billing.ApprovalAuto && r.pre != nil {
			// Execute: credit the Redis balance, mark EXECUTED.
			if err := r.pre.TopUp(ctx, o.UserID, amount); err != nil {
				r.log.Error("auto refund topup failed", "refund_id", id, "err", err)
				continue
			}
			if err := r.refunds.MarkExecuted(ctx, id); err != nil {
				r.log.Error("mark refund executed failed", "refund_id", id, "err", err)
			}
			rep.RefundsAuto++
		} else {
			rep.RefundsManual++
		}
		r.log.Info("refund issued", "refund_id", id, "user", o.UserID,
			"amount", amount, "level", level)
	}
	return nil
}

// ErrDayFormat is returned for invalid day strings.
var ErrDayFormat = errors.New("day must be YYYY-MM-DD")

// ValidateDay parses and normalizes a day string.
func ValidateDay(day string) (string, error) {
	t, err := time.Parse("2006-01-02", day)
	if err != nil {
		return "", ErrDayFormat
	}
	return t.Format("2006-01-02"), nil
}
