package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/auth"
	"github.com/differs/MeterGate/internal/obs"
	"github.com/differs/MeterGate/internal/ratelimit"
)

// userKeyLimiter enforces the two lowest layers of the six-layer budget
// model:
//
//	layer 1 (key):  per-key RPM/TPM/concurrency from the stored key config
//	layer 2 (user): user-level aggregate RPM/TPM shared by ALL keys of a
//	                 user (checked after the key layer passes)
//
// Both layers are atomic Redis sliding windows, so multi-instance
// gateways share the same budgets.
type userKeyLimiter struct {
	check   *ratelimit.Checker
	keys    *auth.CachedKeyStore
	metrics *obs.Metrics
	log     *slog.Logger
}

func newUserKeyLimiter(rdb *redis.Client, keys *auth.CachedKeyStore, m *obs.Metrics) *userKeyLimiter {
	return &userKeyLimiter{
		check:   ratelimit.NewChecker(rdb),
		keys:    keys,
		metrics: m,
		log:     slog.Default(),
	}
}

// Allow implements gateway.RateLimiter: key layer first (RPM → TPM →
// concurrency), then the user aggregate layer (RPM → TPM).
func (k *userKeyLimiter) Allow(ctx context.Context, rawKey string, promptTokens int64) (int, bool) {
	// --- Layer 1: per-key limits (cached) ---
	limits, err := k.keys.Limits(ctx, rawKey)
	if err != nil {
		return 0, true // unknown key: auth already rejected it upstream
	}
	if limits.RPM > 0 {
		if retry, ok := k.check.CheckRPM(ctx, rawKey, limits.RPM); !ok {
			return k.reject("key", retry)
		}
	}
	if limits.TPM > 0 {
		// estimate total tokens: prompt + capped completion estimate
		estimated := promptTokens + 1000
		if retry, ok := k.check.CheckTPM(ctx, rawKey, limits.TPM, estimated); !ok {
			return k.reject("key", retry)
		}
	}
	if limits.Concurrency > 0 {
		ok, _ := k.check.Acquire(ctx, rawKey, limits.Concurrency)
		if !ok {
			return k.reject("key", 0)
		}
		// the gateway calls Done(rawKey) at request end, which DECRs
	}

	// --- Layer 2: user-level aggregate budget (shared across keys) ---
	uid, err := k.keys.Resolve(ctx, rawKey)
	if err == nil {
		ul, err2 := k.keys.UserLimits(ctx, uid)
		if err2 == nil && (ul.RPM > 0 || ul.TPM > 0) {
			scope := fmt.Sprintf("user-%d", uid)
			if ul.RPM > 0 {
				if retry, ok := k.check.CheckRPM(ctx, scope, ul.RPM); !ok {
					return k.reject("user", retry)
				}
			}
			if ul.TPM > 0 {
				estimated := promptTokens + 1000
				if retry, ok := k.check.CheckTPM(ctx, scope, ul.TPM, estimated); !ok {
					return k.reject("user", retry)
				}
			}
		}
	}
	return 0, true
}

// Done implements gateway.RateLimiter (releases the key-layer in-flight slot).
func (k *userKeyLimiter) Done(ctx context.Context, rawKey string) {
	k.check.Done(ctx, rawKey)
}

func (k *userKeyLimiter) reject(layer string, retryAfter int) (int, bool) {
	k.metrics.RateLimitedTotal.WithLabelValues(layer).Inc()
	k.log.Warn("rate limit exceeded", "layer", layer)
	return retryAfter, false
}
