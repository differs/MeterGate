package gateway

import (
	"context"
	"net/http"
	"strings"
)

type ctxKey int

const (
	ctxKeyUserID ctxKey = iota
)

// authMiddleware validates the Bearer API key.
// M1: static allowlist injected via WithKeys; the store-backed key
// service replaces this in M2 without touching the handler.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		key := strings.TrimPrefix(auth, "Bearer ")
		key = strings.TrimSpace(key)
		if key == "" || (s.keys != nil && !s.keys[key]) {
			writeError(w, http.StatusUnauthorized, "invalid or missing API key")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyUserID, key)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
