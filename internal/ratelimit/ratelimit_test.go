package ratelimit

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestChecker(t *testing.T) *Checker {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return NewChecker(rdb)
}

func TestRPMEnforced(t *testing.T) {
	c := newTestChecker(t)
	ctx := context.Background()
	// limit 3 per minute
	for i := 0; i < 3; i++ {
		if _, ok := c.CheckRPM(ctx, "key-1", 3); !ok {
			t.Fatalf("request %d should pass", i+1)
		}
	}
	if _, ok := c.CheckRPM(ctx, "key-1", 3); ok {
		t.Fatal("4th request must be rejected (RPM=3)")
	}
	// different key unaffected
	if _, ok := c.CheckRPM(ctx, "key-2", 3); !ok {
		t.Fatal("other key must be independent")
	}
}

func TestTPMEnforced(t *testing.T) {
	c := newTestChecker(t)
	ctx := context.Background()
	// limit 100 tokens/min; two requests of 60 each → second over
	if _, ok := c.CheckTPM(ctx, "key-t", 100, 60); !ok {
		t.Fatal("first should pass")
	}
	if _, ok := c.CheckTPM(ctx, "key-t", 100, 60); ok {
		t.Fatal("second must be rejected (60+60 > 100)")
	}
}

func TestConcurrencyEnforced(t *testing.T) {
	c := newTestChecker(t)
	ctx := context.Background()
	ok1, rel1 := c.Acquire(ctx, "key-c", 2)
	ok2, rel2 := c.Acquire(ctx, "key-c", 2)
	if !ok1 || !ok2 {
		t.Fatal("two concurrent slots should be granted")
	}
	ok3, _ := c.Acquire(ctx, "key-c", 2)
	if ok3 {
		t.Fatal("third concurrent request must be rejected")
	}
	rel1()
	rel2()
	ok4, rel4 := c.Acquire(ctx, "key-c", 2)
	if !ok4 {
		t.Fatal("slot must free after release")
	}
	rel4()
}

func TestUnlimited(t *testing.T) {
	c := newTestChecker(t)
	ctx := context.Background()
	// limit 0 = unlimited
	for i := 0; i < 10; i++ {
		if _, ok := c.CheckRPM(ctx, "key-u", 0); !ok {
			t.Fatal("unlimited must always pass")
		}
	}
	if ok, _ := c.Acquire(ctx, "key-u", 0); !ok {
		t.Fatal("unlimited concurrency must pass")
	}
}
