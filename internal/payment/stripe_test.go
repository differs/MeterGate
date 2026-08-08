package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func sign(payload, secret string) string {
	t := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(t + "." + payload))
	return fmt.Sprintf("t=%s,v1=%s", t, hex.EncodeToString(mac.Sum(nil)))
}

func TestStripeWebhookValidSignature(t *testing.T) {
	sc := NewStripeChannel("sk_test_x", "whsec_test_123")
	payload := []byte(`{"type":"payment_intent.succeeded","data":{"object":{"id":"pi_123"}}}`)
	body, err := sc.VerifyWebhook(payload, sign(string(payload), "whsec_test_123"))
	if err != nil {
		t.Fatal(err)
	}
	evType, txnID, err := ParseWebhookEvent(body)
	if err != nil {
		t.Fatal(err)
	}
	if evType != "payment_intent.succeeded" || txnID != "pi_123" {
		t.Fatalf("event=%s txn=%s", evType, txnID)
	}
}

func TestStripeWebhookRejectsForged(t *testing.T) {
	sc := NewStripeChannel("sk_test_x", "whsec_test_123")
	payload := []byte(`{"type":"payment_intent.succeeded","data":{"object":{"id":"pi_evil"}}}`)
	// wrong secret signs the payload
	if _, err := sc.VerifyWebhook(payload, sign(string(payload), "wrong-secret")); err == nil {
		t.Fatal("forged signature must fail")
	}
	// tampered payload with the right secret
	if _, err := sc.VerifyWebhook([]byte(`{"type":"payment_intent.succeeded","data":{"object":{"id":"pi_evil2"}}}`),
		sign(`{"type":"payment_intent.succeeded","data":{"object":{"id":"pi_orig"}}}`, "whsec_test_123")); err == nil {
		t.Fatal("tampered payload must fail")
	}
}

func TestStripeWebhookDisabledWithoutSecret(t *testing.T) {
	sc := NewStripeChannel("sk_test_x", "")
	payload := []byte(`{"type":"x"}`)
	if _, err := sc.VerifyWebhook(payload, ""); err != nil {
		t.Fatalf("verification disabled must pass: %v", err)
	}
}
