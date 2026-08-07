package billing

import (
	"context"
	"time"
)

// Refund directions and statuses.
const (
	DirectionCredit = "CREDIT" // money back to the user
	DirectionDebit  = "DEBIT"  // clawback from the user

	RefundPending  = "PENDING"
	RefundApproved = "APPROVED"
	RefundExecuted = "EXECUTED"
	RefundRejected = "REJECTED"

	ApprovalAuto   = "AUTO"
	ApprovalManual = "MANUAL"
)

// Reason codes for refunds.
const (
	ReasonNegativeAmount   = "NEGATIVE_AMOUNT"
	ReasonReconDiff        = "RECON_DIFF"
	ReasonDuplicateCharge  = "DUPLICATE_CHARGE"
	ReasonZeroCompletion   = "ZERO_COMPLETION"
	ReasonModerationReject = "MODERATION_REJECTED"
)

// Refund is an adjustment order against a user's balance. Refunds NEVER
// mutate the original order row (append-only ledger discipline) — they are
// independent entries that reconciliation sums together.
type Refund struct {
	ID             int64
	RequestID      string // originating request (may be empty for manual)
	UserID         string
	OrderID        string // originating order request_id (may be empty)
	ReasonCode     string
	AmountMicros   int64
	Direction      string
	Status         string
	ApprovalLevel  string
	IdempotencyKey string // unique: prevents double-execution
	CreatedAt      time.Time
}

// RefundStore persists refund orders.
type RefundStore interface {
	// InsertRefund creates a refund. A duplicate idempotency_key returns
	// the existing ID with created=false (callers must not re-execute).
	InsertRefund(ctx context.Context, r Refund) (id int64, created bool, err error)
	// ListRefunds returns the most recent refunds.
	ListRefunds(ctx context.Context, limit int) ([]Refund, error)
	// MarkExecuted transitions PENDING → EXECUTED (idempotent).
	MarkExecuted(ctx context.Context, id int64) error
}

// Entry is one immutable money movement (order or refund) on replay,
// used by LedgerAdapter.Replay.
type Entry struct {
	RequestID    string
	UserID       string
	Model        string
	Status       string // SETTLED | NO_CHARGE
	AmountMicros int64
	Direction    string // ORDER | CREDIT | DEBIT
	CreatedAt    time.Time
}
