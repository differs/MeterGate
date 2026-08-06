package router

import (
	"testing"
	"time"
)

func mkChannel(id string, pricePer1M int64) *Channel {
	return &Channel{ID: id, Name: id, BaseURL: "http://mock/" + id, Key: "k",
		InputPer1M: pricePer1M, OutputPer1M: pricePer1M}
}

func mkRoute(model string, channels ...*Channel) *ModelRoute {
	r := &ModelRoute{Model: model}
	for _, c := range channels {
		r.Channels = append(r.Channels, ChannelSpec{Channel: c, Healthy: true})
	}
	return r
}

// TestInverseSquareWeighting: a $1 channel must dominate a $3 channel
// ~9:1 (1/1² vs 1/3²).
func TestInverseSquareWeighting(t *testing.T) {
	e := NewEngine()
	cheap := mkChannel("cheap", 1_000_000)   // $1/M
	pricey := mkChannel("pricey", 3_000_000) // $3/M
	route := mkRoute("m1", cheap, pricey)

	const n = 20_000
	cheapWins := 0
	for i := 0; i < n; i++ {
		d := e.Select(route)
		if d.Primary.ID == "cheap" {
			cheapWins++
		}
	}
	ratio := float64(cheapWins) / float64(n-cheapWins)
	if ratio < 5 || ratio > 15 {
		t.Fatalf("cheap/pricey ratio = %.1f, want ~9", ratio)
	}
}

// TestUnhealthyDeprioritized: an unhealthy channel must never be primary
// while a healthy one exists, but remains in the fallback chain.
func TestUnhealthyDeprioritized(t *testing.T) {
	e := NewEngine()
	healthy := mkChannel("healthy", 2_000_000)
	sick := mkChannel("sick", 1_000_000) // cheaper but unhealthy
	route := &ModelRoute{Model: "m1", Channels: []ChannelSpec{
		{Channel: healthy, Healthy: true},
		{Channel: sick, Healthy: false},
	}}

	for i := 0; i < 100; i++ {
		d := e.Select(route)
		if d.Primary.ID != "healthy" {
			t.Fatalf("primary = %s, want healthy (unhealthy must not win)", d.Primary.ID)
		}
		if len(d.Fallbacks) != 1 || d.Fallbacks[0].ID != "sick" {
			t.Fatalf("fallbacks = %+v, want [sick]", d.Fallbacks)
		}
	}
}

// TestBreakerTripsAndRecovers: 20 failures trip the breaker; after openFor
// it half-opens and a probe success closes it again.
func TestBreakerTripsAndRecovers(t *testing.T) {
	fake := &fakeClock{now: time.Now()}
	b := NewBreaker(fake)

	for i := 0; i < 19; i++ {
		b.Record(false)
	}
	if b.State() != stateClosed {
		t.Fatalf("state = %s, want closed (below threshold)", b.State())
	}
	b.Record(false) // 20th failure trips
	if b.State() != stateOpen {
		t.Fatalf("state = %s, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker must reject traffic")
	}

	fake.now = fake.now.Add(31 * time.Second)
	if !b.Allow() {
		t.Fatal("expired open breaker must half-open and allow probes")
	}
	b.Record(true) // probe succeeds → closed
	if b.State() != stateClosed {
		t.Fatalf("state = %s, want closed after probe success", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker must allow traffic")
	}
}

// TestHealthWindow: >50% failures in the 30s window mark unhealthy.
func TestHealthWindow(t *testing.T) {
	fake := &fakeClock{now: time.Now()}
	h := NewHealthTracker(30*time.Second, fake)

	h.Record("c1", true)
	h.Record("c1", true)
	h.Record("c1", false)
	if !h.Healthy("c1") {
		t.Fatal("33% failure must stay healthy")
	}
	h.Record("c1", false)
	if h.Healthy("c1") {
		t.Fatal("50% failure must be unhealthy (>=50% trips the window)")
	}

	// Window slides: after 31s the old failures age out.
	fake.now = fake.now.Add(31 * time.Second)
	h.Record("c1", true)
	if !h.Healthy("c1") {
		t.Fatal("healthy after window slide")
	}
}

// TestIsRetryable: 4xx is terminal, 5xx/429/network retry.
func TestIsRetryable(t *testing.T) {
	cases := []struct {
		code int
		err  bool
		want bool
	}{
		{200, false, false}, // success is not an error path here
		{400, false, false},
		{404, false, false},
		{429, false, true},
		{500, false, true},
		{502, false, true},
		{0, true, true},
	}
	for _, c := range cases {
		got := IsRetryable(c.code, boolErr(c.err))
		if c.code == 200 && c.err == false {
			continue // skip success
		}
		if got != c.want {
			t.Errorf("IsRetryable(%d, err=%v) = %v, want %v", c.code, c.err, got, c.want)
		}
	}
}

func boolErr(b bool) error {
	if b {
		return errBoom
	}
	return nil
}

var errBoom = &boomError{}

type boomError struct{}

func (*boomError) Error() string { return "boom" }

// fakeClock is a controllable Clock for deterministic tests.
type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }
