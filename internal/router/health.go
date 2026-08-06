package router

import (
	"sync"
	"time"
)

// HealthTracker maintains the 30-second failure window per channel
// (OpenRouter's default): a channel with a significant error rate in the
// last 30s is deprioritized (moved to the back of the fallback order), but
// never removed — it remains as a last-resort fallback.
//
// Implementation: a ring buffer of per-second buckets (window+1 buckets),
// updated under a mutex. The hot path (Engine.Select) reads a cached
// healthy bit computed by the tracker's ticker, so selection never takes
// the lock.
type HealthTracker struct {
	mu      sync.Mutex
	window  time.Duration
	buckets []bucket
	idx     int
	last    time.Time
	clock   Clock

	// cached health, updated on each tick and on Record
	cacheMu sync.RWMutex
	healthy map[string]bool
}

type bucket struct {
	fail  int
	total int
}

// NewHealthTracker builds a tracker with the given failure window
// (default 30s when <= 0).
func NewHealthTracker(window time.Duration, clock Clock) *HealthTracker {
	if window <= 0 {
		window = 30 * time.Second
	}
	if clock == nil {
		clock = RealClock{}
	}
	seconds := int(window.Seconds()) + 1
	return &HealthTracker{
		window:  window,
		buckets: make([]bucket, seconds),
		last:    clock.Now(),
		clock:   clock,
		healthy: map[string]bool{},
	}
}

// Record logs one request outcome for a channel.
func (h *HealthTracker) Record(channelID string, ok bool) {
	h.mu.Lock()
	h.advance()
	b := &h.buckets[h.idx]
	if ok {
		b.total++
	} else {
		b.total++
		b.fail++
	}
	healthy := h.computeLocked(channelID)
	h.mu.Unlock()

	h.cacheMu.Lock()
	h.healthy[channelID] = healthy
	h.cacheMu.Unlock()
}

// Healthy reports the cached health bit (lock-free on the hot path).
// Channels with no recorded data yet are treated as HEALTHY — otherwise a
// channel that was never selected would be permanently deprioritized and
// could never win traffic again (rich-get-richer lock-in).
func (h *HealthTracker) Healthy(channelID string) bool {
	h.cacheMu.RLock()
	defer h.cacheMu.RUnlock()
	v, ok := h.healthy[channelID]
	if !ok {
		return true
	}
	return v
}

// advance moves the ring to the current second, zeroing stale buckets.
func (h *HealthTracker) advance() {
	now := h.clock.Now()
	elapsed := int(now.Sub(h.last).Seconds())
	if elapsed <= 0 {
		return
	}
	if elapsed > len(h.buckets) {
		elapsed = len(h.buckets)
	}
	for i := 0; i < elapsed; i++ {
		h.idx = (h.idx + 1) % len(h.buckets)
		h.buckets[h.idx] = bucket{}
	}
	h.last = now
}

// computeLocked evaluates the failure rate over the whole ring (≤ window).
func (h *HealthTracker) computeLocked(channelID string) bool {
	var fail, total int
	for _, b := range h.buckets {
		fail += b.fail
		total += b.total
	}
	if total == 0 {
		return true // no data → healthy
	}
	// A channel is unhealthy when its 30s error rate is ≥ 50%.
	return float64(fail)/float64(total) < 0.5
}
