package gateway

import (
	"context"
	"net/http"
	"strings"
)

type ctxKey int

const (
	ctxKeyUserID ctxKey = iota
	ctxKeyRawKey
	ctxKeyEndUser
)

// CtxKeyEndUser is the exported context key for the end-user identity
// (X-End-User header). The quota layer reads it to enforce layer 6.
var CtxKeyEndUser = ctxKeyEndUser

// authMiddleware validates the Bearer API key.
// M1: static allowlist injected via WithKeys; the store-backed key
// service replaces this in M2 without touching the handler.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		key := strings.TrimPrefix(auth, "Bearer ")
		key = strings.TrimSpace(key)
		userID := key
		ok := key != ""
		if ok && s.keys != nil && !s.keys[key] {
			// static allowlist miss → try the DB-backed resolver
			if s.keyResolve != nil {
				if uid, resolved := s.keyResolve(key); resolved {
					userID = uid
				} else {
					ok = false
				}
			} else {
				ok = false
			}
		}
		if !ok {
			writeError(w, http.StatusUnauthorized, "invalid or missing API key")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyUserID, userID)
		ctx = context.WithValue(ctx, ctxKeyRawKey, key)
		// Layer 6 (end-user) scope: the final consumer identity, taken
		// from the standard X-End-User header (may be empty = no quota).
		if eu := r.Header.Get("X-End-User"); eu != "" {
			ctx = context.WithValue(ctx, ctxKeyEndUser, eu)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
