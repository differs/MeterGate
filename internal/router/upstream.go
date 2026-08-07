package router

import (
	"context"
	"errors"
	"net/http"

	"github.com/differs/MeterGate/internal/gateway"
)

// RoutingUpstream implements gateway.Upstream over multiple channels:
//
//	Route(model) → Decision{primary, fallbacks}
//	  → dispatch primary (breaker gate → forward → record outcome)
//	  → on retryable failure: next fallback, repeat
//	  → 4xx client errors never retry
//
// Every attempted channel is recorded into the health tracker + breaker;
// the winning channel is stamped into the metering event (provider).
type RoutingUpstream struct {
	router *Router
	// channelID → client
	clients map[string]*gateway.HTTPUpstream
}

// Router exposes the underlying router (observability collectors).
func (u *RoutingUpstream) Router() *Router {
	return u.router
}

// NewRoutingUpstream builds the multi-channel upstream.
// HTTPUpstream instances are shared per channel (connection reuse).
func NewRoutingUpstream(r *Router) *RoutingUpstream {
	clients := map[string]*gateway.HTTPUpstream{}
	for _, route := range r.snap.Load().Models {
		for _, c := range route.Channels {
			id := c.Channel.ID
			if _, ok := clients[id]; ok {
				continue
			}
			clients[id] = gateway.NewHTTPUpstream(c.Channel.BaseURL, c.Channel.Key)
		}
	}
	return &RoutingUpstream{router: r, clients: clients}
}

// ErrAllChannelsFailed is returned when every channel in the chain failed.
var ErrAllChannelsFailed = errors.New("all channels failed")

// Chat implements gateway.Upstream with failover across channels.
func (u *RoutingUpstream) Chat(ctx context.Context, call *gateway.ChatCall) (*gateway.ChatResult, error) {
	dec, err := u.router.Route(call.Model)
	if err != nil {
		return nil, err
	}

	chain := make([]*Channel, 0, 1+len(dec.Fallbacks))
	chain = append(chain, dec.Primary)
	chain = append(chain, dec.Fallbacks...)

	var lastErr error
	for _, ch := range chain {
		// Breaker gate: consumes half-open probes; open → skip channel.
		b, ok := u.router.breakers[ch.ID]
		if ok && !b.Allow() {
			u.router.RecordOutcome(ch.ID, false)
			lastErr = ErrAllChannelsFailed
			continue
		}

		client := u.clients[ch.ID]
		call.UpstreamURL = ch.BaseURL
		result, cerr := client.Chat(ctx, call)

		if cerr == nil && result != nil && result.StatusCode >= 200 && result.StatusCode < 300 {
			u.router.RecordOutcome(ch.ID, true)
			call.Provider = ch.ID
			return result, nil
		}

		// Failure: record and decide whether to retry.
		u.router.RecordOutcome(ch.ID, false)
		status := 0
		if result != nil {
			status = result.StatusCode
		}
		lastErr = cerr
		if cerr == nil {
			lastErr = errors.New("upstream status " + itoaStatus(status))
		}
		if !IsRetryable(status, cerr) {
			// 4xx client error — the request itself is bad, never retry.
			return result, lastErr
		}
	}
	if lastErr == nil {
		lastErr = ErrAllChannelsFailed
	}
	return nil, lastErr
}

func itoaStatus(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [3]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

var _ gateway.Upstream = (*RoutingUpstream)(nil)
var _ = http.StatusOK
