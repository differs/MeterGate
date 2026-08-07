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
	ID     int64
	UserID int64
	Name   string
	Status int
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

// CreateKey issues a new sk- key for a user. Returns the raw key (shown
// once); only its hash is stored.
func (s *Service) CreateKey(ctx context.Context, userID int64, name string) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	key := "sk-" + hex.EncodeToString(raw)
	hash := keyHash(key)
	_, err := s.pool.Exec(ctx,
		`INSERT INTO api_keys (user_id, key_hash, name) VALUES ($1,$2,$3)`,
		userID, hash, name)
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

func keyHash(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
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
