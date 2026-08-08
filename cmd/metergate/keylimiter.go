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
	uid, err := k.keys.Resolve(ctx, rawKey)
	if err == nil {
		if !k.checkAggregate(ctx, uid, promptTokens, "user") {
			return k.reject("user", 0)
		}
		pid, err := k.keys.ProjectOfUser(ctx, uid)
		if err == nil && pid > 0 {
			if !k.checkAggregate(ctx, pid, promptTokens, "project") {
				return k.reject("project", 0)
			}
			// --- Layer 4: team (project belongs to a team) ---
			tid, err := k.keys.TeamOfProject(ctx, pid)
			if err == nil && tid > 0 {
				if !k.checkAggregate(ctx, tid, promptTokens, "team") {
					return k.reject("team", 0)
				}
				// --- Layer 5: org (team belongs to an org) ---
				oid, err := k.keys.OrgOfTeam(ctx, tid)
				if err == nil && oid > 0 {
					if !k.checkAggregate(ctx, oid, promptTokens, "org") {
						return k.reject("org", 0)
					}
				}
			}
		}
	}
	return 0, true
}

// checkAggregate enforces one RPM/TPM budget layer (user or project)
// against a scope id. The scope is the raw id (the layer prefix is
// applied by the caller via Checker scope names).
func (k *budgetLimiter) checkAggregate(ctx context.Context, id int64, promptTokens int64, layer string) bool {
	// layer=user → scope user-{id}; layer=project → project-{id}
	scope := fmt.Sprintf("%s-%d", layer, id)
	var limits auth.Limits
	var err error
	switch layer {
	case "user":
		limits, err = k.keys.UserLimits(ctx, id)
	case "project":
		limits, err = k.keys.ProjectLimits(ctx, id)
	case "team":
		limits, err = k.keys.TeamLimits(ctx, id)
	case "org":
		limits, err = k.keys.OrgLimits(ctx, id)
	}
	if err != nil {
		return true // unknown scope: pass
	}
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
