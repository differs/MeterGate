// Package portal — commercial user-facing API: register, login, API key
// management, recharge & pay. Serves the P0 "can take money" loop.
package portal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/differs/MeterGate/internal/auth"
	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/payment"
)

type ctxKey int

const ctxKeyUserID ctxKey = iota

// API is the portal HTTP surface.
type API struct {
	auth         *auth.Service
	payments     *payment.Service
	adminKey     string
	jwt          *auth.JWTManager                                                                   // may be nil (JWT disabled)
	oidc         *auth.OIDC                                                                         // may be nil (OIDC disabled)
	balance      func(ctx context.Context, userID int64) (int64, error)                             // may be nil
	usage        func(ctx context.Context, userID string, days int) ([]billing.UsageDay, error)     // may be nil
	usageByModel func(ctx context.Context, userID string, days int) ([]billing.UsageByModel, error) // may be nil
	stripe       *payment.StripeChannel                                                             // may be nil
	webDir       string                                                                             // static merchant portal directory (optional)
}

// New builds the portal API.
func New(a *auth.Service, p *payment.Service, adminKey string) *API {
	return &API{auth: a, payments: p, adminKey: adminKey}
}

// WithJWT enables session JWT issuance on login.
func (api *API) WithJWT(m *auth.JWTManager) *API {
	api.jwt = m
	return api
}

// WithOIDC mounts the OIDC login/callback endpoints.
func (api *API) WithOIDC(o *auth.OIDC) *API {
	api.oidc = o
	return api
}

// WithBalance attaches the balance reader (Redis-backed).
func (api *API) WithBalance(fn func(ctx context.Context, userID int64) (int64, error)) *API {
	api.balance = fn
	return api
}

// WithUsage attaches the per-user usage aggregator (PG-backed).
func (api *API) WithUsage(fn func(ctx context.Context, userID string, days int) ([]billing.UsageDay, error)) *API {
	api.usage = fn
	return api
}

// WithStripe attaches the Stripe channel (webhook + pay).
func (api *API) WithStripe(sc *payment.StripeChannel) *API {
	api.stripe = sc
	return api
}

// WithUsageByModel attaches the per-model usage aggregator (cost analysis).
func (api *API) WithUsageByModel(fn func(ctx context.Context, userID string, days int) ([]billing.UsageByModel, error)) *API {
	api.usageByModel = fn
	return api
}

// WithWeb serves the merchant portal static files at /.
func (api *API) WithWeb(dir string) *API {
	api.webDir = dir
	return api
}

// Handler returns the portal router (admin-key protected in dev;
// session/JWT auth replaces this in production).
func (api *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/register", api.register)
	mux.HandleFunc("POST /api/login", api.login)
	mux.HandleFunc("POST /api/keys", api.createKey)
	mux.HandleFunc("GET /api/keys", api.listKeys)
	mux.HandleFunc("POST /api/recharge", api.recharge)
	mux.HandleFunc("POST /api/recharge/pay", api.payHandler)
	mux.HandleFunc("GET /api/recharge/status", api.rechargeStatus)
	if api.oidc != nil {
		mux.Handle("/api/oidc/login", api.oidc)
		mux.Handle("/api/oidc/callback", api.oidc)
	}
	mux.HandleFunc("GET /api/balance", api.balanceHandler)
	mux.HandleFunc("GET /api/usage", api.usageHandler)
	mux.HandleFunc("GET /api/usage/models", api.usageModelsHandler)
	mux.HandleFunc("POST /api/payment/webhook", api.webhookHandler)
	if api.webDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(api.webDir)))
	}
	return api.authMiddleware(mux)
}

func (api *API) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// OIDC endpoints are public: the IdP redirects back WITHOUT our
		// auth header (browser flow), so /api/oidc/* must bypass.
		if strings.HasPrefix(r.URL.Path, "/api/oidc/") {
			next.ServeHTTP(w, r)
			return
		}
		// Static merchant portal assets are public (all data flows
		// through the authenticated /api/* endpoints).
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		// 1) admin key (dev/ops) OR 2) session JWT
		if api.adminKey != "" && r.Header.Get("Authorization") == "Bearer "+api.adminKey {
			next.ServeHTTP(w, r)
			return
		}
		if api.jwt != nil {
			raw := r.Header.Get("Authorization")
			raw = strings.TrimPrefix(raw, "Bearer ")
			raw = strings.TrimSpace(raw)
			if raw != "" {
				if claims, err := api.jwt.Verify(raw); err == nil {
					ctx := context.WithValue(r.Context(), ctxKeyUserID, claims.UserID)
					r = r.WithContext(ctx)
					next.ServeHTTP(w, r)
					return
				}
			}
		}
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// balanceHandler returns the authenticated user's balance (JWT context).
func (api *API) balanceHandler(w http.ResponseWriter, r *http.Request) {
	uid, err := userFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "authenticated user required"})
		return
	}
	if api.balance == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "balance reader not configured"})
		return
	}
	bal, err := api.balance(r.Context(), uid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_id": uid, "balance_micros": bal})
}

// usageHandler returns the authenticated user's per-day usage.
func (api *API) usageHandler(w http.ResponseWriter, r *http.Request) {
	uid, err := userFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "authenticated user required"})
		return
	}
	if api.usage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "usage not configured"})
		return
	}
	days := 7
	if raw := r.URL.Query().Get("days"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			days = v
		}
	}
	rows, err := api.usage(r.Context(), fmt.Sprintf("user-%d", uid), days)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_id": uid, "days": days, "usage": rows})
}

// usageModelsHandler returns per-model usage with cost/margin analysis.
func (api *API) usageModelsHandler(w http.ResponseWriter, r *http.Request) {
	uid, err := userFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "authenticated user required"})
		return
	}
	if api.usageByModel == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "usage/models not configured"})
		return
	}
	days := 7
	if raw := r.URL.Query().Get("days"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			days = v
		}
	}
	rows, err := api.usageByModel(r.Context(), fmt.Sprintf("user-%d", uid), days)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var totalRevenue, totalCost int64
	for _, m := range rows {
		totalRevenue += m.AmountMicros
		totalCost += m.CostMicros
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user_id": uid, "days": days,
		"models":               rows,
		"total_revenue_micros": totalRevenue,
		"total_cost_micros":    totalCost,
		"total_margin_micros":  totalRevenue - totalCost,
	})
}

// webhookHandler receives channel callbacks (Stripe webhook). The OIDC
// bypass above covers it: webhook carries NO auth header.
func (api *API) webhookHandler(w http.ResponseWriter, r *http.Request) {
	payload, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	channel := r.URL.Query().Get("channel")
	if channel == "" {
		channel = "stripe"
	}
	ch := api.channel(channel)
	if ch == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown channel"})
		return
	}
	// channel-specific signature verification
	switch c := ch.(type) {
	case *payment.StripeChannel:
		body, err := c.VerifyWebhook(payload, r.Header.Get("Stripe-Signature"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid signature"})
			return
		}
		evType, txnID, err := payment.ParseWebhookEvent(body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad event"})
			return
		}
		if evType != "payment_intent.succeeded" {
			writeJSON(w, http.StatusOK, map[string]any{"received": true, "ignored": evType})
			return
		}
		// find the recharge by the metadata we stored at Pay()
		if err := api.payments.SettleByTxn(r.Context(), channel, txnID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"received": true})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]any{"error": "channel not supported"})
}

// channel returns the registered channel by name (hack: expose via payments).
func (api *API) channel(name string) any {
	if name == "stripe" && api.stripe != nil {
		return api.stripe
	}
	return nil
}

// userFromRequest extracts user_id: JWT context first, then query/header.
func userFromRequest(r *http.Request) (int64, error) {
	if v, ok := r.Context().Value(ctxKeyUserID).(int64); ok {
		return v, nil
	}
	raw := r.URL.Query().Get("user_id")
	if raw == "" {
		raw = r.Header.Get("X-User-Id")
	}
	if raw == "" {
		// fall back to a username lookup via header (dev)
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(raw, 10, 64)
}

func (api *API) register(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid body"})
		return
	}
	u, err := api.auth.Register(r.Context(), strings.TrimSpace(req.Username), req.Password)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": u})
}

func (api *API) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid body"})
		return
	}
	u, err := api.auth.Login(r.Context(), strings.TrimSpace(req.Username), req.Password)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
		return
	}
	resp := map[string]any{"user_id": u.ID}
	if api.jwt != nil {
		token, err := api.jwt.Sign(u.ID, u.Username)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "jwt sign failed"})
			return
		}
		resp["token"] = token
		resp["token_type"] = "Bearer"
	} else {
		token, _ := auth.SessionToken(u.ID)
		resp["token"] = token
	}
	writeJSON(w, http.StatusOK, resp)
}

func (api *API) createKey(w http.ResponseWriter, r *http.Request) {
	uid, err := userFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "user_id required"})
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	key, err := api.auth.CreateKey(r.Context(), uid, req.Name)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "note": "shown once"})
}

func (api *API) listKeys(w http.ResponseWriter, r *http.Request) {
	uid, err := userFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "user_id required"})
		return
	}
	keys, err := api.auth.ListKeys(r.Context(), uid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (api *API) recharge(w http.ResponseWriter, r *http.Request) {
	uid, err := userFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "user_id required"})
		return
	}
	var req struct {
		AmountMicros   int64  `json:"amount_micros"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AmountMicros <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "amount_micros > 0 required"})
		return
	}
	id, err := api.payments.Recharge(r.Context(), uid, req.AmountMicros, req.IdempotencyKey)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recharge_id": id, "status": "PENDING"})
}

func (api *API) payHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RechargeID int64  `json:"recharge_id"`
		Channel    string `json:"channel"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RechargeID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "recharge_id required"})
		return
	}
	if req.Channel == "" {
		req.Channel = "mock"
	}
	txn, err := api.payments.Pay(r.Context(), req.RechargeID, req.Channel)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"txn_id": txn, "status": "PAID"})
}

func (api *API) rechargeStatus(w http.ResponseWriter, r *http.Request) {
	rid, err := strconv.ParseInt(r.URL.Query().Get("recharge_id"), 10, 64)
	if err != nil || rid <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "recharge_id required"})
		return
	}
	status, err := api.payments.RechargeStatus(r.Context(), rid)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recharge_id": rid, "status": status})
}

var _ = time.Now
