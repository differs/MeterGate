package payment

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// StripeChannel is a real payment channel via the Stripe REST API
// (PaymentIntents + webhook). Works in TEST MODE with sk_test_ keys —
// no production merchant account required for integration testing.
type StripeChannel struct {
	secretKey     string
	webhookSecret string
	baseURL       string // https://api.stripe.com (override in tests)
	client        *http.Client
}

// NewStripeChannel builds the adapter. webhookSecret enables signature
// verification of /api/payment/webhook callbacks.
func NewStripeChannel(secretKey, webhookSecret string) *StripeChannel {
	return &StripeChannel{
		secretKey:     secretKey,
		webhookSecret: webhookSecret,
		baseURL:       "https://api.stripe.com",
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

// Name implements Channel.
func (s *StripeChannel) Name() string { return "stripe" }

// Pay implements Channel: creates a PaymentIntent and returns its id.
// The returned id is stored as channel_txn_id; the actual settlement
// happens via the webhook (payment_intent.succeeded).
func (s *StripeChannel) Pay(_ context.Context, rechargeID int64, amountMicros int64) (string, error) {
	// Stripe amounts are in the base unit (cents); micros → cents rounding.
	cents := (amountMicros + 5_000) / 10_000 // 1e4 micros = 1 cent
	form := url.Values{}
	form.Set("amount", fmt.Sprint(cents))
	form.Set("currency", "cny")
	form.Set("payment_method_types[]", "card")
	form.Set("metadata[recharge_id]", fmt.Sprint(rechargeID))
	form.Set("metadata[source]", "metergate")
	form.Set("automatic_payment_methods[enabled]", "false")

	req, err := http.NewRequest(http.MethodPost, s.baseURL+"/v1/payment_intents", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(s.secretKey, "")
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("stripe: %s: %s", resp.Status, string(body))
	}
	var pi struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &pi); err != nil {
		return "", err
	}
	return pi.ID, nil
}

// VerifyWebhook validates the Stripe signature header and returns the
// event payload. Returns ErrInvalidSignature on mismatch.
func (s *StripeChannel) VerifyWebhook(payload []byte, sigHeader string) ([]byte, error) {
	if s.webhookSecret == "" {
		return payload, nil // verification disabled
	}
	// t=...,v1=...
	parts := map[string]string{}
	for _, kv := range strings.Split(sigHeader, ",") {
		if i := strings.IndexByte(kv, '='); i > 0 {
			parts[kv[:i]] = kv[i+1:]
		}
	}
	signedPayload := parts["t"] + "." + string(payload)
	mac := hmac.New(sha256.New, []byte(s.webhookSecret))
	mac.Write([]byte(signedPayload))
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts["v1"])) {
		return nil, ErrInvalidSignature
	}
	return payload, nil
}

// ErrInvalidSignature is returned for forged webhook callbacks.
var ErrInvalidSignature = errors.New("invalid stripe signature")

// ParseWebhookEvent extracts the event type + PaymentIntent id.
func ParseWebhookEvent(payload []byte) (eventType, txnID string, err error) {
	var ev struct {
		Type string `json:"type"`
		Data struct {
			Object struct {
				ID string `json:"id"`
			} `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", "", err
	}
	return ev.Type, ev.Data.Object.ID, nil
}

// httpPost is a helper kept for symmetry (unused placeholder removed).
var _ = bytes.NewReader
