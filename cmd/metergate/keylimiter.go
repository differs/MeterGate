package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/differs/MeterGate/internal/auth"
	"github.com/differs/MeterGate/internal/gateway"
	"github.com/differs/MeterGate/internal/obs"
	"github.com/differs/MeterGate/internal/ratelimit"
)

// budgetLimiter enforces all six layers of the budget model, from the
// finest (end-user) to the coarsest (org):
//
//	layer 6 (end-user): per end-user RPM/TPM of a key (scope eu:{key}:{id});
//	                    only when the request carries X-End-User
//	layer 1 (key):      per-key RPM/TPM/concurrency from the stored config
//	layer 2 (user):     user-level aggregate RPM/TPM across ALL keys
//	layer 3 (project):  project-level aggregate RPM/TPM across users
//	layer 4 (team):     team-level aggregate RPM/TPM across projects
//	layer 5 (org):      org-level aggregate RPM/TPM across teams
//
// All layers are atomic Redis sliding windows, so multi-instance
// gateways share the same budgets.
type budgetLimiter struct {
	check   *ratelimit.Checker
	keys    *auth.CachedKeyStore
	metrics *obs.Metrics
	log     *slog.Logger
}

func newBudgetLimiter(rdb *redis.Client, keys *auth.CachedKeyStore, m *obs.Metrics) *budgetLimiter {
	return &budgetLimiter{
		check:   ratelimit.NewChecker(rdb),
		keys:    keys,
		metrics: m,
		log:     slog.Default(),
	}
}

// Allow implements gateway.RateLimiter: key layer (RPM → TPM →
// concurrency), then user layer (RPM → TPM), then project layer (RPM → TPM).
func (k *budgetLimiter) Allow(ctx context.Context, rawKey string, promptTokens int64) (int, bool) {
	// --- Layer 6: per end-user limits of this key (only when the
	// request carries X-End-User and the key has end-user quotas) ---
	limits, err := k.keys.Limits(ctx, rawKey)
	if err != nil {
		return 0, true // unknown key: auth already rejected it upstream
	}
	if eu, ok := ctx.Value(gateway.CtxKeyEndUser).(string); ok && eu != "" {
		if limits.EndUserRPM > 0 || limits.EndUserTPM > 0 {
			euScope := "eu:" + rawKey + ":" + eu
			if limits.EndUserRPM > 0 {
				if retry, ok := k.check.CheckRPM(ctx, euScope, limits.EndUserRPM); !ok {
					return k.reject("end_user", retry)
				}
			}
			if limits.EndUserTPM > 0 {
				estimated := promptTokens + 1000
				if retry, ok := k.check.CheckTPM(ctx, euScope, limits.EndUserTPM, estimated); !ok {
					return k.reject("end_user", retry)
				}
			}
		}
	}

	// --- Layer 1: per-key limits (cached) ---
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

	// --- Layers 2-5: user → project → team → org (aggregate budgets) ---
	// One cached chain snapshot replaces five per-layer lookups.
	if uid, err := k.keys.Resolve(ctx, rawKey); err == nil {
		ci, err := k.keys.ChainOfUser(ctx, uid)
		if err == nil {
			if !k.checkAggregateLimits(ctx, "user", uid, ci.User, promptTokens) {
				return k.reject("user", 0)
			}
			if ci.ProjectID > 0 {
				if !k.checkAggregateLimits(ctx, "project", ci.ProjectID, ci.Project, promptTokens) {
					return k.reject("project", 0)
				}
				if ci.TeamID > 0 {
					if !k.checkAggregateLimits(ctx, "team", ci.TeamID, ci.Team, promptTokens) {
						return k.reject("team", 0)
					}
					if ci.OrgID > 0 {
						if !k.checkAggregateLimits(ctx, "org", ci.OrgID, ci.Org, promptTokens) {
							return k.reject("org", 0)
						}
					}
				}
			}
		}
	}
	return 0, true
}

// checkAggregateLimits enforces one RPM/TPM budget layer against a
// preloaded quota. scopeID is the layer's id (user/project/team/org).
func (k *budgetLimiter) checkAggregateLimits(ctx context.Context, layer string, scopeID int64, limits auth.Limits, promptTokens int64) bool {
	scope := fmt.Sprintf("%s-%d", layer, scopeID)
	if limits.RPM > 0 {
		if _, ok := k.check.CheckRPM(ctx, scope, limits.RPM); !ok {
			return false
		}
	}
	if limits.TPM > 0 {
		estimated := promptTokens + 1000
		if _, ok := k.check.CheckTPM(ctx, scope, limits.TPM, estimated); !ok {
			return false
		}
	}
	return true
}

// Done implements gateway.RateLimiter (releases the key-layer in-flight slot).
func (k *budgetLimiter) Done(ctx context.Context, rawKey string) {
	k.check.Done(ctx, rawKey)
}

func (k *budgetLimiter) reject(layer string, retryAfter int) (int, bool) {
	k.metrics.RateLimitedTotal.WithLabelValues(layer).Inc()
	k.log.Warn("rate limit exceeded", "layer", layer)
	return retryAfter, false
}
