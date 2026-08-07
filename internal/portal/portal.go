// Package portal — commercial user-facing API: register, login, API key
// management, recharge & pay. Serves the P0 "can take money" loop.
package portal

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/differs/MeterGate/internal/auth"
	"github.com/differs/MeterGate/internal/payment"
)

// API is the portal HTTP surface.
type API struct {
	auth     *auth.Service
	payments *payment.Service
	adminKey string
}

// New builds the portal API.
func New(a *auth.Service, p *payment.Service, adminKey string) *API {
	return &API{auth: a, payments: p, adminKey: adminKey}
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
	return api.adminAuth(mux)
}

func (api *API) adminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if api.adminKey == "" || r.Header.Get("Authorization") != "Bearer "+api.adminKey {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// userFromRequest extracts user_id from query/header (dev: admin-driven).
func userFromRequest(r *http.Request) (int64, error) {
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
	token, _ := auth.SessionToken(u.ID)
	writeJSON(w, http.StatusOK, map[string]any{"user_id": u.ID, "token": token})
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
