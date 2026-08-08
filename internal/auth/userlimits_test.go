package auth

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// User-level quota tests need a live PostgreSQL. Set PG_TEST_DSN
// (e.g. postgres://postgres:mg@127.0.0.1:5446/metergate); tests skip
// otherwise.
func testService(t *testing.T) *Service {
	t.Helper()
	dsn := testingEnv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping DB test")
	}
	ctx := context.Background()
	svc, err := NewService(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { svc.Pool().Close() })
	return svc
}

func testingEnv(k string) string {
	return os.Getenv(k)
}

func TestUserLimitsRoundtrip(t *testing.T) {
	svc := testService(t)
	ctx := context.Background()

	u, err := svc.Register(ctx, fmt.Sprintf("limits-test-%d", time.Now().UnixNano()), "password123")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// default: unlimited
	l, err := svc.ResolveUserLimits(ctx, u.ID)
	if err != nil {
		t.Fatalf("resolve default: %v", err)
	}
	if l.RPM != 0 || l.TPM != 0 {
		t.Fatalf("new user must be unlimited, got %+v", l)
	}

	// set aggregate budget
	if err := svc.SetUserLimits(ctx, u.ID, 5, 1000); err != nil {
		t.Fatalf("set: %v", err)
	}
	l, err = svc.ResolveUserLimits(ctx, u.ID)
	if err != nil {
		t.Fatalf("resolve set: %v", err)
	}
	if l.RPM != 5 || l.TPM != 1000 {
		t.Fatalf("want rpm=5 tpm=1000, got %+v", l)
	}
}

func TestProjectLimitsRoundtrip(t *testing.T) {
	svc := testService(t)
	ctx := context.Background()

	p, err := svc.CreateProject(ctx, "test-project", 0, 0)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if p.ID == 0 {
		t.Fatal("project id must be assigned")
	}

	// default unlimited + no assignment
	l, err := svc.ResolveProjectLimits(ctx, p.ID)
	if err != nil {
		t.Fatalf("resolve default: %v", err)
	}
	if l.RPM != 0 || l.TPM != 0 {
		t.Fatalf("new project must be unlimited, got %+v", l)
	}

	// set aggregate budget
	if err := svc.SetProjectLimits(ctx, p.ID, 10, 5000); err != nil {
		t.Fatalf("set: %v", err)
	}
	l, err = svc.ResolveProjectLimits(ctx, p.ID)
	if err != nil {
		t.Fatalf("resolve set: %v", err)
	}
	if l.RPM != 10 || l.TPM != 5000 {
		t.Fatalf("want rpm=10 tpm=5000, got %+v", l)
	}

	// assign a user, read back membership
	u, err := svc.Register(ctx, fmt.Sprintf("project-test-%d", time.Now().UnixNano()), "password123")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := svc.SetUserProject(ctx, u.ID, p.ID); err != nil {
		t.Fatalf("assign: %v", err)
	}
	pid, err := svc.ProjectOfUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("membership: %v", err)
	}
	if pid != p.ID {
		t.Fatalf("want project %d, got %d", p.ID, pid)
	}
}
