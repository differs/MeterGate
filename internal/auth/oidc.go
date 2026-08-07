package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDCConfig configures the OpenID Connect authorization-code flow.
type OIDCConfig struct {
	ProviderURL  string // e.g. https://accounts.google.com
	ClientID     string
	ClientSecret string
	RedirectURL  string // our /api/oidc/callback
	// AutoRegister: when true, unknown subjects get a local account
	// (username = provider email/sub).
	AutoRegister bool
}

// OIDC is the identity-provider client: discovery, login redirect,
// callback exchange, id_token verification (JWKS), auto-register.
type OIDC struct {
	cfg   OIDCConfig
	auth  *Service
	jwt   *JWTManager
	oauth *oauth2.Config
	verif *oidc.IDTokenVerifier
	log   *slog.Logger
}

// NewOIDC performs provider discovery and builds the verifier.
func NewOIDC(cfg OIDCConfig, auth *Service, jwt *JWTManager, log *slog.Logger) (*OIDC, error) {
	if log == nil {
		log = slog.Default()
	}
	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, cfg.ProviderURL)
	if err != nil {
		return nil, err
	}
	oauth := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  cfg.RedirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
	}
	return &OIDC{
		cfg:   cfg,
		auth:  auth,
		jwt:   jwt,
		oauth: oauth,
		verif: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
		log:   log,
	}, nil
}

// LoginURL builds the authorization URL with a fresh CSRF state.
func (o *OIDC) LoginURL(state string) string {
	return o.oauth.AuthCodeURL(state, oauth2.AccessTypeOnline)
}

// NewState generates a CSRF token (stored server-side or in a cookie).
func NewState() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Callback exchanges the authorization code, verifies the id_token
// (issuer/audience/signature via JWKS), resolves or auto-registers the
// user, and returns a MeterGate session JWT.
func (o *OIDC) Callback(ctx context.Context, code, state string) (string, error) {
	if code == "" {
		return "", errors.New("missing code")
	}
	tok, err := o.oauth.Exchange(ctx, code)
	if err != nil {
		return "", err
	}
	rawIDToken, ok := tok.Extra("id_token").(string)
	if !ok {
		return "", errors.New("no id_token in exchange response")
	}
	idToken, err := o.verif.Verify(ctx, rawIDToken)
	if err != nil {
		return "", err
	}

	// identity claims
	var claims struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return "", err
	}
	username := claims.Email
	if username == "" {
		username = idToken.Subject
	}

	// resolve or auto-register the local account (idempotent by subject)
	userID, err := o.resolveUser(ctx, idToken.Subject, username)
	if err != nil {
		return "", err
	}
	return o.jwt.Sign(userID, username)
}

// resolveUser finds an account by OIDC subject or auto-registers one.
func (o *OIDC) resolveUser(ctx context.Context, subject, username string) (int64, error) {
	// subject-indexed lookup: external_identities table
	if uid, err := o.subjectUserID(ctx, subject); err == nil && uid > 0 {
		return uid, nil
	}
	if !o.cfg.AutoRegister {
		return 0, errors.New("unknown user (auto-register disabled)")
	}
	// create local account (idempotent by username)
	u, err := o.auth.Register(ctx, username, randomPassword())
	if err != nil && !errors.Is(err, ErrUserExists) {
		return 0, err
	}
	uid := u.ID
	if errors.Is(err, ErrUserExists) {
		uid, err = o.userIDByUsername(ctx, username)
		if err != nil {
			return 0, err
		}
	}
	// link subject → local user
	if err := o.linkSubject(ctx, subject, uid); err != nil {
		return 0, err
	}
	return uid, nil
}

// randomPassword: OIDC users don't use password auth; store an
// unguessable hash so the password field is never empty.
func randomPassword() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "oidc-" + hex.EncodeToString(b)
}

// subjectUserID looks up a local user by OIDC subject.
func (o *OIDC) subjectUserID(ctx context.Context, subject string) (int64, error) {
	var uid int64
	err := o.auth.pool.QueryRow(ctx,
		`SELECT user_id FROM external_identities WHERE provider='oidc' AND subject=$1`,
		subject).Scan(&uid)
	return uid, err
}

// userIDByUsername finds a local user id by username.
func (o *OIDC) userIDByUsername(ctx context.Context, username string) (int64, error) {
	var uid int64
	err := o.auth.pool.QueryRow(ctx,
		`SELECT id FROM users WHERE username=$1`, username).Scan(&uid)
	return uid, err
}

// linkSubject binds an OIDC subject to a local user (idempotent).
func (o *OIDC) linkSubject(ctx context.Context, subject string, userID int64) error {
	_, err := o.auth.pool.Exec(ctx,
		`INSERT INTO external_identities (provider, subject, user_id)
		 VALUES ('oidc', $1, $2)
		 ON CONFLICT (provider, subject) DO NOTHING`, subject, userID)
	return err
}

// ServeHTTP wires the OIDC endpoints onto the portal router:
//
//	GET /api/oidc/login    → 302 to the IdP
//	GET /api/oidc/callback → exchange + set session JWT
func (o *OIDC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/oidc/login":
		state, _ := NewState()
		http.SetCookie(w, &http.Cookie{
			Name: "oidc_state", Value: state,
			Path: "/", HttpOnly: true, MaxAge: 300,
		})
		http.Redirect(w, r, o.LoginURL(state), http.StatusFound)
	case "/api/oidc/callback":
		o.handleCallback(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (o *OIDC) handleCallback(w http.ResponseWriter, r *http.Request) {
	// CSRF: state must match the cookie
	ck, err := r.Cookie("oidc_state")
	if err != nil || ck.Value == "" || ck.Value != r.URL.Query().Get("state") {
		http.Error(w, `{"error":"state mismatch"}`, http.StatusBadRequest)
		return
	}
	jwtToken, err := o.Callback(r.Context(), r.URL.Query().Get("code"), ck.Value)
	if err != nil {
		o.log.Warn("oidc callback failed", "err", err)
		http.Error(w, `{"error":"oidc login failed"}`, http.StatusUnauthorized)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: "metergate_session", Value: jwtToken,
		Path: "/", HttpOnly: true, MaxAge: int((24 * time.Hour).Seconds()),
	})
	writeJSON(w, http.StatusOK, map[string]any{"token": jwtToken, "expires_in": 86400})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
