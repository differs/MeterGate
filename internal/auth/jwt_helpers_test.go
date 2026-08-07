package auth

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func newExpiredClaims() jwt.RegisteredClaims {
	return jwt.RegisteredClaims{
		Issuer:    jwtIssuer,
		Audience:  jwt.ClaimStrings{jwtAudience},
		Subject:   "7",
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-48 * time.Hour)),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-24 * time.Hour)),
	}
}

func newJWTWithClaims(m *JWTManager, claims Claims) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
}
