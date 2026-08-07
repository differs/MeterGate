// Package admin implements MeterGate's admin API — the operational
// surface for balance queries, order lookups, reconciliation triggers
// and refund management. It is deliberately small and read/action
// oriented; the merchant portal (M6) builds on top of it.
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/differs/MeterGate/internal/billing"
	"github.com/differs/MeterGate/internal/reconciliation"
)

// API wires the admin endpoints to the billing stack.
type API struct {
	orders   billing.OrderStore
	refunds  billing.RefundStore
	pre      *billing.Precharger // may be nil
	recon    *reconciliation.Reconciler
	adminKey string
}

// New builds the admin API.
func New(orders billing.OrderStore, refunds billing.RefundStore,
	pre *billing.Precharger, recon *reconciliation.Reconciler, adminKey string) *API {
	return &API{orders: orders, refunds: refunds, pre: pre, recon: recon, adminKey: adminKey}
}

// Handler returns the admin HTTP handler with auth.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/balance", a.getBalance)
	mux.HandleFunc("GET /admin/orders", a.listOrders)
	mux.HandleFunc("GET /admin/refunds", a.listRefunds)
	mux.HandleFunc("POST /admin/reconcile", a.runReconcile)
	return a.auth(mux)
}

func (a *API) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.adminKey == "" || r.Header.Get("Authorization") != "Bearer "+a.adminKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
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

func (a *API) getBalance(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "user is required"})
		return
	}
	if a.pre == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "redis not enabled"})
		return
	}
	bal, err := a.pre.Balance(r.Context(), user)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": user, "balance_micros": bal})
}

func (a *API) listOrders(w http.ResponseWriter, r *http.Request) {
	// M4: bounded recent orders for a user via the store's summary path;
	// a full paginated query endpoint lands with the merchant portal.
	user := r.URL.Query().Get("user")
	day := r.URL.Query().Get("day")
	if user == "" || day == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "user and day are required"})
		return
	}
	summary, err := a.orders.Summary(r.Context(), day)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user": user, "day": day, "summary": summary,
		"note": "per-order detail endpoint ships with the merchant portal (M6)",
	})
}

func (a *API) listRefunds(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	refunds, err := a.refunds.ListRefunds(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"refunds": refunds})
}

type reconcileReq struct {
	Day        string `json:"day"`
	AutoRefund bool   `json:"auto_refund"`
}

func (a *API) runReconcile(w http.ResponseWriter, r *http.Request) {
	var req reconcileReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid body"})
		return
	}
	if req.Day == "" {
		req.Day = time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	}
	day, err := reconciliation.ValidateDay(req.Day)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if a.recon == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "reconciler not wired"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	rep, err := a.recon.RunDay(ctx, day, req.AutoRefund)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, rep)
}
