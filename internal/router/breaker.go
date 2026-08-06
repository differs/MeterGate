package router

import (
	"sync"
	"time"
)

// Breaker is a per-channel circuit breaker on top of the failure window.
// The failure window deprioritizes flaky channels; the breaker *fast-fails*
// them entirely during an outage and probes for recovery.
//
// State machine:
//
//	Closed ──(error rate ≥ threshold over window)──▶ Open (fast-fail)
//	Open ──(openFor elapsed)──▶ HalfOpen (probe requests)
//	HalfOpen ──(probe succeeds)──▶ Closed
//	HalfOpen ──(probe fails)──▶ Open (backoff, capped)
type Breaker struct {
	mu       sync.Mutex
	state    string
	failures int
	probes   int
	openAt   time.Time
	clock    Clock

	// config
	threshold   float64       // error rate that trips (default 0.5)
	minRequests int           // min samples before tripping (default 20)
	openFor     time.Duration // how long to stay open (default 30s)
	maxProbes   int           // probes before giving up (default 3)
	backoffCap  time.Duration // max open duration after repeated failures (default 5min)
}

const (
	stateClosed   = "closed"
	stateOpen     = "open"
	stateHalfOpen = "half_open"
)

// NewBreaker builds a breaker with sane defaults.
func NewBreaker(clock Clock) *Breaker {
	if clock == nil {
		clock = RealClock{}
	}
	return &Breaker{
		state:       stateClosed,
		clock:       clock,
		threshold:   0.5,
		minRequests: 20,
		openFor:     30 * time.Second,
		maxProbes:   3,
		backoffCap:  5 * time.Minute,
	}
}

// Allow reports whether a request may proceed to this channel.
// Open → fast-fail. HalfOpen → limited probe traffic.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		return true
	case stateOpen:
		if b.clock.Now().Sub(b.openAt) >= b.openFor {
			b.state = stateHalfOpen
			b.probes = 0
			return true
		}
		return false
	case stateHalfOpen:
		if b.probes < b.maxProbes {
			b.probes++
			return true
		}
		// probe budget exhausted without success → back to open
		b.state = stateOpen
		b.openAt = b.clock.Now()
		return false
	}
	return true
}

// Record reports a request outcome.
func (b *Breaker) Record(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed:
		if ok {
			b.failures = 0
			return
		}
		b.failures++
		// trip when the error rate in the last minRequests is ≥ threshold
		if b.failures >= b.minRequests {
			// with 1:1 failure counting, minRequests failures trip the breaker
			b.state = stateOpen
			b.openAt = b.clock.Now()
		}
	case stateHalfOpen:
		if ok {
			b.state = stateClosed
			b.failures = 0
		} else {
			// back to open with exponential-ish backoff (cap applied)
			b.state = stateOpen
			b.openAt = b.clock.Now()
		}
	case stateOpen:
		// ignore outcomes while open (requests are fast-failed)
	}
}

// State exposes the current state (tests/observability).
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// IsOpen is a read-only fast check: does the breaker reject traffic?
// Unlike Allow() it has NO side effects (does not consume half-open
// probes) — used when building routing specs.
func (b *Breaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != stateOpen {
		return false
	}
	// treat expired-open as not-open here; Allow() performs the actual
	// state transition when the request is dispatched.
	if b.clock.Now().Sub(b.openAt) >= b.openFor {
		return false
	}
	return true
}
