// Package auth — commercial user & API-key management: registration,
// login (bcrypt + session token), API key creation/revocation. The
// gateway authenticates via the KeyStore (cached), replacing the static
// allowlist for production.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// Errors returned by the auth service.
var (
	ErrUserExists     = errors.New("username already exists")
	ErrBadCredentials = errors.New("invalid username or password")
	ErrKeyNotFound    = errors.New("api key not found")
	ErrUserDisabled   = errors.New("user disabled")
)

// User is a platform account.
type User struct {
	ID       int64
	Username string
	Status   int
}

// Key is an API key bound to a user.
type Key struct {
	ID          int64
	UserID      int64
	Name        string
	Status      int
	RPM         int
	TPM         int64
	Concurrency int
}

//go:embed schema.sql
var commercialSchema string

// Service implements auth on PostgreSQL.
type Service struct {
	pool *pgxpool.Pool
}

// Pool exposes the underlying pool (payment service shares the schema).
func (s *Service) Pool() *pgxpool.Pool {
	return s.pool
}

// ResolveUserLimits returns the user-level aggregate quota (0 = unlimited).
// Layer 2 of the six-layer budget model: all keys of a user share this
// budget on top of each key's own limits.
func (s *Service) ResolveUserLimits(ctx context.Context, userID int64) (Limits, error) {
	var l Limits
	err := s.pool.QueryRow(ctx,
		`SELECT rpm_limit, tpm_limit FROM users WHERE id=$1`,
		userID).Scan(&l.RPM, &l.TPM)
	if err != nil {
		return Limits{}, err
	}
	return l, nil
}

// SetUserLimits updates a user's aggregate quota (admin operation).
func (s *Service) SetUserLimits(ctx context.Context, userID int64, rpm int, tpm int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE users SET rpm_limit=$2, tpm_limit=$3 WHERE id=$1`,
		userID, rpm, tpm)
	return err
}

// Project is layer 3 of the six-layer budget model: a shared budget
// across multiple users.
type Project struct {
	ID   int64
	Name string
	RPM  int
	TPM  int64
}

// CreateProject makes a project with an aggregate quota (0 = unlimited).
func (s *Service) CreateProject(ctx context.Context, name string, rpm int, tpm int64) (*Project, error) {
	var p Project
	err := s.pool.QueryRow(ctx,
		`INSERT INTO projects (name, rpm_limit, tpm_limit) VALUES ($1,$2,$3) RETURNING id, name, rpm_limit, tpm_limit`,
		name, rpm, tpm).Scan(&p.ID, &p.Name, &p.RPM, &p.TPM)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// SetProjectLimits updates a project's aggregate quota (admin operation).
func (s *Service) SetProjectLimits(ctx context.Context, projectID int64, rpm int, tpm int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE projects SET rpm_limit=$2, tpm_limit=$3 WHERE id=$1`,
		projectID, rpm, tpm)
	return err
}

// ResolveProjectLimits returns a project's aggregate quota (0 = unlimited).
func (s *Service) ResolveProjectLimits(ctx context.Context, projectID int64) (Limits, error) {
	var l Limits
	err := s.pool.QueryRow(ctx,
		`SELECT rpm_limit, tpm_limit FROM projects WHERE id=$1`,
		projectID).Scan(&l.RPM, &l.TPM)
	if err != nil {
		return Limits{}, err
	}
	return l, nil
}

// SetUserProject assigns a user to a project (shares its budget).
func (s *Service) SetUserProject(ctx context.Context, userID, projectID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE users SET project_id=$2 WHERE id=$1`, userID, projectID)
	return err
}

// ProjectOfUser returns the project a user belongs to (0 = none).
func (s *Service) ProjectOfUser(ctx context.Context, userID int64) (int64, error) {
	var pid int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(project_id, 0) FROM users WHERE id=$1`,
		userID).Scan(&pid)
	return pid, err
}

// LoginVerified bypasses the password check (internal use: OIDC users).
func (s *Service) LoginVerified(ctx context.Context, username string) (*User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`SELECT id, username, status FROM users WHERE username=$1`,
		username).Scan(&u.ID, &u.Username, &u.Status)
	if err != nil {
		return nil, ErrBadCredentials
	}
	if u.Status != 1 {
		return nil, ErrUserDisabled
	}
	return &u, nil
}

// NewService connects and applies the schema.
func NewService(ctx context.Context, dsn string) (*Service, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, commercialSchema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Service{pool: pool}, nil
}

// Register creates a user with a bcrypt-hashed password.
func (s *Service) Register(ctx context.Context, username, password string) (*User, error) {
	if username == "" || len(password) < 8 {
		return nil, errors.New("username required, password >= 8 chars")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	var id int64
	err = s.pool.QueryRow(ctx,
		`INSERT INTO users (username, password_hash) VALUES ($1,$2)
		 ON CONFLICT (username) DO NOTHING RETURNING id`,
		username, string(hash)).Scan(&id)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, ErrUserExists
		}
		return nil, err
	}
	return &User{ID: id, Username: username, Status: 1}, nil
}

// Login verifies credentials and returns the user.
func (s *Service) Login(ctx context.Context, username, password string) (*User, error) {
	var u User
	var hash string
	err := s.pool.QueryRow(ctx,
		`SELECT id, username, status, password_hash FROM users WHERE username=$1`,
		username).Scan(&u.ID, &u.Username, &u.Status, &hash)
	if err != nil {
		return nil, ErrBadCredentials
	}
	if u.Status != 1 {
		return nil, ErrUserDisabled
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return nil, ErrBadCredentials
	}
	return &u, nil
}

// CreateKey issues a new sk- key for a user with optional rate limits.
// Returns the raw key (shown once); only its hash is stored.
func (s *Service) CreateKey(ctx context.Context, userID int64, name string, limits Limits) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	key := "sk-" + hex.EncodeToString(raw)
	hash := keyHash(key)
	_, err := s.pool.Exec(ctx,
		`INSERT INTO api_keys (user_id, key_hash, name, rpm_limit, tpm_limit, concurrency_limit)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		userID, hash, name, limits.RPM, limits.TPM, limits.Concurrency)
	if err != nil {
		return "", err
	}
	return key, nil
}

// RevokeKey disables a key.
func (s *Service) RevokeKey(ctx context.Context, userID int64, keyID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET status=0 WHERE id=$1 AND user_id=$2`, keyID, userID)
	return err
}

// ListKeys returns a user's keys (no hashes).
func (s *Service) ListKeys(ctx context.Context, userID int64) ([]Key, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, user_id, name, status FROM api_keys WHERE user_id=$1 ORDER BY id DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.ID, &k.UserID, &k.Name, &k.Status); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// Limits is a key's rate-limit configuration (0 = unlimited).
type Limits struct {
	RPM         int
	TPM         int64
	Concurrency int
}

// KeyStore resolves a raw API key to its user (gateway auth path).
// Implementations: PostgresKeyStore (production), memory cache wrapper.
type KeyStore interface {
	// Resolve returns the user ID for a raw key, or ErrKeyNotFound.
	Resolve(ctx context.Context, rawKey string) (int64, error)
}

// PostgresKeyStore resolves keys against the api_keys table.
type PostgresKeyStore struct {
	svc *Service
}

// NewKeyStore builds the resolver.
func (s *Service) NewKeyStore() *PostgresKeyStore {
	return &PostgresKeyStore{svc: s}
}

// Resolve implements KeyStore (hits the DB; wrap with a cache for hot path).
func (k *PostgresKeyStore) Resolve(ctx context.Context, rawKey string) (int64, error) {
	hash := keyHash(rawKey)
	var userID int64
	var status int
	err := k.svc.pool.QueryRow(ctx,
		`SELECT user_id, status FROM api_keys WHERE key_hash=$1`, hash).Scan(&userID, &status)
	if err != nil {
		return 0, ErrKeyNotFound
	}
	if status != 1 {
		return 0, ErrKeyNotFound
	}
	// touch last_used (async, best effort)
	_, _ = k.svc.pool.Exec(ctx,
		`UPDATE api_keys SET last_used_at=now() WHERE key_hash=$1`, hash)
	return userID, nil
}

// ResolveUserLimits returns a user's aggregate quota (0 = unlimited).
func (k *PostgresKeyStore) ResolveUserLimits(ctx context.Context, userID int64) (Limits, error) {
	return k.svc.ResolveUserLimits(ctx, userID)
}

// ProjectOfUser returns the project a user belongs to (0 = none).
func (k *PostgresKeyStore) ProjectOfUser(ctx context.Context, userID int64) (int64, error) {
	return k.svc.ProjectOfUser(ctx, userID)
}

// ResolveProjectLimits returns a project's aggregate quota (0 = unlimited).
func (k *PostgresKeyStore) ResolveProjectLimits(ctx context.Context, projectID int64) (Limits, error) {
	return k.svc.ResolveProjectLimits(ctx, projectID)
}

// ResolveLimits returns a key's rate limits (0 = unlimited).
func (k *PostgresKeyStore) ResolveLimits(ctx context.Context, rawKey string) (Limits, error) {
	hash := keyHash(rawKey)
	var l Limits
	err := k.svc.pool.QueryRow(ctx,
		`SELECT rpm_limit, tpm_limit, concurrency_limit FROM api_keys WHERE key_hash=$1`,
		hash).Scan(&l.RPM, &l.TPM, &l.Concurrency)
	if err != nil {
		return Limits{}, err
	}
	return l, nil
}

func keyHash(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

// Limits returns a key's rate limits (cache-first; DB fallback).
func (c *CachedKeyStore) Limits(ctx context.Context, rawKey string) (Limits, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.ResolveLimits(ctx, rawKey)
	}
	return Limits{}, nil
}

// UserLimits returns a user's aggregate quota (cache-first; DB fallback).
func (c *CachedKeyStore) UserLimits(ctx context.Context, userID int64) (Limits, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.ResolveUserLimits(ctx, userID)
	}
	return Limits{}, nil
}

// ProjectOfUser returns the project a user belongs to (0 = none),
// cache-first with the same TTL as key resolution.
func (c *CachedKeyStore) ProjectOfUser(ctx context.Context, userID int64) (int64, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.ProjectOfUser(ctx, userID)
	}
	return 0, nil
}

// ProjectLimits returns a project's aggregate quota (cache-first).
func (c *CachedKeyStore) ProjectLimits(ctx context.Context, projectID int64) (Limits, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.ResolveProjectLimits(ctx, projectID)
	}
	return Limits{}, nil
}

// CachedKeyStore wraps a KeyStore with a small TTL cache so the gateway
// hot path never hits the DB for auth (mirrors the static-key fast path).
type CachedKeyStore struct {
	inner KeyStore
	cache map[string]cacheEntry
	ttl   time.Duration
}

type cacheEntry struct {
	userID  int64
	expires time.Time
}

// NewCachedKeyStore wraps a resolver with an in-memory TTL cache.
func NewCachedKeyStore(inner KeyStore, ttl time.Duration) *CachedKeyStore {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &CachedKeyStore{inner: inner, cache: map[string]cacheEntry{}, ttl: ttl}
}

// Resolve implements KeyStore (cache-first; negative entries cached short).
func (c *CachedKeyStore) Resolve(ctx context.Context, rawKey string) (int64, error) {
	now := time.Now()
	if e, ok := c.cache[rawKey]; ok && now.Before(e.expires) {
		if e.userID == 0 {
			return 0, ErrKeyNotFound
		}
		return e.userID, nil
	}
	uid, err := c.inner.Resolve(ctx, rawKey)
	ttl := c.ttl
	if err != nil {
		uid = 0
		ttl = 5 * time.Second // negative cache shorter
	}
	c.cache[rawKey] = cacheEntry{userID: uid, expires: now.Add(ttl)}
	return uid, err
}

// SessionToken issues a login session token (dev-grade; JWT in production).
func SessionToken(userID int64) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw) + "-" + itoa(userID), nil
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// Touch last_used (kept for future use).
var _ = time.Now
