// Command reconcile is MeterGate's daily reconciliation CLI.
//
// M2 scope (Layer A, internal):
//   - per-day summary by order status (settled / no-charge)
//   - anomaly scan: negative amounts, empty request IDs, orphan pre-charges
//   - frozen balance sweep report (leaked pre-charges from crashed gateways)
//
// Usage:
//
//	reconcile --pg-dsn postgres://... --day 2026-08-06
//	reconcile --pg-dsn postgres://... --redis 127.0.0.1:6379 --sweep
//
// Exit code 0 = clean, 1 = differences found (CI-friendly).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/reconciliation"
)

func main() {
	var (
		pgDSN      = flag.String("pg-dsn", "", "PostgreSQL DSN (required)")
		redisAddr  = flag.String("redis", "", "Redis address (for frozen sweep)")
		day        = flag.String("day", time.Now().AddDate(0, 0, -1).Format("2006-01-02"), "day to reconcile (YYYY-MM-DD, default yesterday)")
		autoRefund = flag.Bool("auto-refund", false, "issue refunds for detected differences (small amounts auto-executed)")
		verbose    = flag.Bool("v", false, "verbose output")
	)
	flag.Parse()
	ctx := context.Background()

	if *pgDSN == "" {
		fmt.Fprintln(os.Stderr, "error: --pg-dsn is required")
		flag.Usage()
		os.Exit(2)
	}

	store, err := billing.NewPostgresOrderStore(ctx, *pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect store: %v\n", err)
		os.Exit(2)
	}
	defer store.Close()

	var pre *billing.Precharger
	if *redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: redis unreachable: %v\n", err)
		} else {
			pre = billing.NewPrecharger(rdb)
		}
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	recon := reconciliation.New(store, store, pre, logger)
	rep, err := recon.RunDay(ctx, *day, *autoRefund)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: reconcile: %v\n", err)
		os.Exit(2)
	}
	dirty := false
	fmt.Printf("=== MeterGate reconcile %s ===\n", rep.Day)
	fmt.Printf("  SETTLED    count=%-10d\n", rep.SettledCount)
	fmt.Printf("  NO_CHARGE  count=%-10d\n", rep.NoChargeCnt)
	fmt.Printf("  total amount_micros=%d\n", rep.TotalAmount)
	if rep.Anomalies > 0 {
		dirty = true
		fmt.Printf("  ✗ %d anomaly rows (see log)\n", rep.Anomalies)
	} else {
		fmt.Println("  ✓ no anomalies")
	}
	if rep.FrozenLeaked > 0 {
		dirty = true
		fmt.Printf("  ✗ frozen (unsettled pre-charge) balance: %d micros\n", rep.FrozenLeaked)
	} else {
		fmt.Println("  ✓ no frozen balance")
	}
	if *autoRefund {
		fmt.Printf("  refunds: %d auto-executed, %d manual-pending\n", rep.RefundsAuto, rep.RefundsManual)
		if rep.RefundsAuto > 0 {
			dirty = true
		}
	}

	if *verbose {
		fmt.Printf("  store: %s\n", *pgDSN)
	}
	if dirty {
		fmt.Println("result: DIFFERENCES FOUND")
		os.Exit(1)
	}
	fmt.Println("result: CLEAN")
}
