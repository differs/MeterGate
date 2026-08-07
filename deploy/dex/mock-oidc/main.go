// mock-oidc: minimal standard OIDC provider for end-to-end verification
// of MeterGate's OIDC client (discovery → authorize → code exchange →
// id_token verification via JWKS → auto-register).
//
// Implements exactly the OIDC Authorization Code Flow endpoints:
//
//	/.well-known/openid-configuration
//	/authorize   (auto-approves; redirects with ?code=)
//	/token       (exchanges code for id_token + access_token)
//	/keys        (JWKS, HS256 oct key)
package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"
)

const (
	issuer      = "http://127.0.0.1:5557"
	clientID    = "metergate"
	clientSec   = "mock-secret"
	redirectURI = "http://localhost:3002/api/oidc/callback"
	keyID       = "mock-rsa-1"
)

// RS256 signing key (standard IdP path — go-oidc verifies against JWKS).
var rsaKey *rsa.PrivateKey

func init() {
	var err error
	rsaKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
}

var codes = map[string]string{} // code -> subject

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// signJWT builds a compact JWT (RS256).
func signJWT(claims map[string]any) string {
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": keyID}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	unsigned := b64(hb) + "." + b64(cb)
	digest := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, digest[:])
	if err != nil {
		panic(err)
	}
	return unsigned + "." + b64(sig)
}

func discovery(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"issuer":                                issuer,
		"authorization_endpoint":                issuer + "/authorize",
		"token_endpoint":                        issuer + "/token",
		"jwks_uri":                              issuer + "/keys",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"scopes_supported":                      []string{"openid", "profile", "email"},
	})
}

// authorize: validates params, issues a code, redirects back (auto-approve).
func authorize(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	if q.Get("client_id") != clientID {
		http.Error(w, "bad client_id", http.StatusBadRequest)
		return
	}
	redirect := q.Get("redirect_uri")
	if redirect != "http://localhost:3002/api/oidc/callback" &&
		redirect != "http://localhost:3202/api/oidc/callback" {
		http.Error(w, "bad redirect_uri", http.StatusBadRequest)
		return
	}
	subject := "oidc-user-alice"
	code := fmt.Sprintf("code-%d", time.Now().UnixNano())
	codes[code] = subject
	loc := redirect + "?code=" + code + "&state=" + q.Get("state")
	http.Redirect(w, r, loc, http.StatusFound)
}

// token: exchanges the code for id_token + access_token.
func token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	// client auth: Basic OR body params (oauth2 defaults to body params
	// when the discovery doc does not declare a token auth method)
	u, p, ok := r.BasicAuth()
	if !ok {
		u = r.Form.Get("client_id")
		p = r.Form.Get("client_secret")
	}
	if u != clientID || p != clientSec {
		http.Error(w, "bad client auth", http.StatusUnauthorized)
		return
	}
	code := r.Form.Get("code")
	log.Printf("token exchange: code=%q codes=%v", code, codes)
	subject, ok := codes[code]
	if !ok {
		http.Error(w, "bad code", http.StatusBadRequest)
		return
	}
	// code reuse allowed in this mock (production IdPs reject reuse)
	now := time.Now().Unix()
	idToken := signJWT(map[string]any{
		"iss": issuer, "sub": subject, "aud": clientID,
		"exp": now + 3600, "iat": now,
		"email": "alice@metergate.dev", "email_verified": true,
		"name": "Alice",
		// openidconnect requires a nonce claim matching the authorize nonce
		"nonce": "",
	})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"access_token": "mock-access",
		"token_type":   "Bearer",
		"expires_in":   3600,
		"id_token":     idToken,
	})
}

// keys: JWKS with the RSA public key (go-oidc verifies against it).
func keys(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	n := rsaKey.PublicKey.N
	e := big.NewInt(int64(rsaKey.PublicKey.E))
	json.NewEncoder(w).Encode(map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"kid": keyID,
			"n":   b64(n.Bytes()),
			"e":   b64(e.Bytes()),
		}},
	})
}

var _ = x509.MarshalPKIXPublicKey
var _ = pem.EncodeToMemory

func main() {
	http.HandleFunc("/.well-known/openid-configuration", discovery)
	http.HandleFunc("/authorize", authorize)
	http.HandleFunc("/token", token)
	http.HandleFunc("/keys", keys)
	log.Println("mock-oidc listening on :5557")
	log.Fatal(http.ListenAndServe(":5557", nil))
}
