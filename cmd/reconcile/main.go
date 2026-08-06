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
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/billing"
)

func main() {
	var (
		pgDSN     = flag.String("pg-dsn", "", "PostgreSQL DSN (required)")
		redisAddr = flag.String("redis", "", "Redis address (for sweep)")
		day       = flag.String("day", time.Now().AddDate(0, 0, -1).Format("2006-01-02"), "day to reconcile (YYYY-MM-DD, default yesterday)")
		sweep     = flag.Bool("sweep", false, "report frozen (pre-charged) balance sweep")
		verbose   = flag.Bool("v", false, "verbose output")
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

	dirty := false

	// 1) Day summary by status.
	summary, err := store.Summary(ctx, *day)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: summary: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("=== MeterGate reconcile %s ===\n", *day)
	if len(summary) == 0 {
		fmt.Println("  (no orders recorded for this day)")
	}
	for _, st := range []string{billing.StatusSettled, billing.StatusNoCharge} {
		if s, ok := summary[st]; ok {
			fmt.Printf("  %-10s count=%-10d tokens=%-15d amount_micros=%d\n",
				st, s.Count, s.TotalTokens, s.AmountMicros)
		}
	}

	// 2) Anomaly scan (cheap guards; full cross-checks land in M4).
	anomalies, err := store.Anomalies(ctx, *day)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: anomaly scan: %v\n", err)
		os.Exit(2)
	}
	if len(anomalies) > 0 {
		dirty = true
		fmt.Printf("  ✗ %d anomaly rows:\n", len(anomalies))
		for _, a := range anomalies {
			fmt.Printf("    - %s\n", a)
		}
	} else {
		fmt.Println("  ✓ no anomalies")
	}

	// 3) Frozen balance sweep (optional, needs Redis).
	if *sweep && *redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: redis unreachable, skip sweep: %v\n", err)
		} else {
			pre := billing.NewPrecharger(rdb)
			frozen, err := pre.FrozenBalance(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: sweep: %v\n", err)
				os.Exit(2)
			}
			if frozen > 0 {
				dirty = true
				fmt.Printf("  ✗ frozen (unsettled pre-charge) balance: %d micros — check for leaked pre-charges\n", frozen)
			} else {
				fmt.Println("  ✓ no frozen balance")
			}
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
