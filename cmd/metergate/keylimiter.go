package main

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/auth"
	"github.com/differs/MeterGate/internal/ratelimit"
)

// keyLimiter enforces per-key RPM/TPM/concurrency limits from the stored
// key config. The user-level layer (aggregating all keys of a user) is
// the next layer of the six-layer budget model.
type keyLimiter struct {
	check *ratelimit.Checker
	keys  *auth.CachedKeyStore
	log   *slog.Logger
}

func newKeyLimiter(rdb *redis.Client, keys *auth.CachedKeyStore) *keyLimiter {
	return &keyLimiter{
		check: ratelimit.NewChecker(rdb),
		keys:  keys,
		log:   slog.Default(),
	}
}

// Allow implements gateway.RateLimiter: checks RPM + TPM + concurrency.
func (k *keyLimiter) Allow(ctx context.Context, key string, promptTokens int64) (int, bool) {
	// Resolve the key's limits (cached).
	limits, err := k.keys.Limits(ctx, key)
	if err != nil || (limits.RPM == 0 && limits.TPM == 0 && limits.Concurrency == 0) {
		return 0, true // unknown key or unlimited
	}

	if limits.RPM > 0 {
		if retry, ok := k.check.CheckRPM(ctx, key, limits.RPM); !ok {
			return retry, false
		}
	}
	if limits.TPM > 0 {
		// estimate total tokens: prompt + capped completion estimate
		estimated := promptTokens + 1000
		if retry, ok := k.check.CheckTPM(ctx, key, limits.TPM, estimated); !ok {
			return retry, false
		}
	}
	if limits.Concurrency > 0 {
		ok, release := k.check.Acquire(ctx, key, limits.Concurrency)
		if !ok {
			return 0, false
		}
		// release is deferred by the gateway via Done; keep a map-free
		// design: the gateway calls Done(key) at request end, which DECRs.
		_ = release
	}
	return 0, true
}

// Done implements gateway.RateLimiter.
func (k *keyLimiter) Done(ctx context.Context, key string) {
	k.check.Done(ctx, key)
}
