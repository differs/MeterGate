package auth

import (
	"testing"
)

func TestJWTSignVerify(t *testing.T) {
	m := NewJWTManager("test-secret-that-is-long-enough-32bytes!")
	token, err := m.Sign(42, "alice")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := m.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != 42 || claims.Username != "alice" {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestJWTRejectsWrongSecret(t *testing.T) {
	m1 := NewJWTManager("secret-one-1234567890abcdef")
	m2 := NewJWTManager("secret-two-1234567890abcdef")
	token, _ := m1.Sign(1, "bob")
	if _, err := m2.Verify(token); err == nil {
		t.Fatal("wrong secret must fail verification")
	}
}

func TestJWTRejectsExpired(t *testing.T) {
	m := NewJWTManager("test-secret-that-is-long-enough-32bytes!")
	// craft an already-expired token via the low-level API
	claims := Claims{
		UserID: 7, Username: "eve",
		RegisteredClaims: newExpiredClaims(),
	}
	token, err := newJWTWithClaims(m, claims)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Verify(token); err == nil {
		t.Fatal("expired token must fail verification")
	}
}

func TestJWTRejectsTampered(t *testing.T) {
	m := NewJWTManager("test-secret-that-is-long-enough-32bytes!")
	token, _ := m.Sign(1, "alice")
	// flip a character in the payload section
	if len(token) > 20 {
		bad := token[:10] + "X" + token[11:]
		if _, err := m.Verify(bad); err == nil {
			t.Fatal("tampered token must fail")
		}
	}
}
