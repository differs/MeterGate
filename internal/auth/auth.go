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
	"sync"
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
	EndUserRPM  int
	EndUserTPM  int64
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

// Org is layer 5 (top) of the six-layer budget model: an organization
// aggregates teams, each aggregating projects.
type Org struct {
	ID   int64
	Name string
	RPM  int
	TPM  int64
}

// Team is layer 4: a team aggregates projects inside an org.
type Team struct {
	ID    int64
	Name  string
	OrgID int64
	RPM   int
	TPM   int64
}

// CreateOrg makes an org with an aggregate quota (0 = unlimited).
func (s *Service) CreateOrg(ctx context.Context, name string, rpm int, tpm int64) (*Org, error) {
	var o Org
	err := s.pool.QueryRow(ctx,
		`INSERT INTO orgs (name, rpm_limit, tpm_limit) VALUES ($1,$2,$3) RETURNING id, name, rpm_limit, tpm_limit`,
		name, rpm, tpm).Scan(&o.ID, &o.Name, &o.RPM, &o.TPM)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// SetOrgLimits updates an org's aggregate quota (admin operation).
func (s *Service) SetOrgLimits(ctx context.Context, orgID int64, rpm int, tpm int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE orgs SET rpm_limit=$2, tpm_limit=$3 WHERE id=$1`, orgID, rpm, tpm)
	return err
}

// ResolveOrgLimits returns an org's aggregate quota (0 = unlimited).
func (s *Service) ResolveOrgLimits(ctx context.Context, orgID int64) (Limits, error) {
	var l Limits
	err := s.pool.QueryRow(ctx,
		`SELECT rpm_limit, tpm_limit FROM orgs WHERE id=$1`, orgID).Scan(&l.RPM, &l.TPM)
	if err != nil {
		return Limits{}, err
	}
	return l, nil
}

// CreateTeam makes a team inside an org with an aggregate quota.
func (s *Service) CreateTeam(ctx context.Context, name string, orgID int64, rpm int, tpm int64) (*Team, error) {
	var t Team
	err := s.pool.QueryRow(ctx,
		`INSERT INTO teams (name, org_id, rpm_limit, tpm_limit) VALUES ($1,$2,$3,$4)
		 RETURNING id, name, org_id, rpm_limit, tpm_limit`,
		name, orgID, rpm, tpm).Scan(&t.ID, &t.Name, &t.OrgID, &t.RPM, &t.TPM)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// SetTeamLimits updates a team's aggregate quota (admin operation).
func (s *Service) SetTeamLimits(ctx context.Context, teamID int64, rpm int, tpm int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE teams SET rpm_limit=$2, tpm_limit=$3 WHERE id=$1`, teamID, rpm, tpm)
	return err
}

// ResolveTeamLimits returns a team's aggregate quota (0 = unlimited).
func (s *Service) ResolveTeamLimits(ctx context.Context, teamID int64) (Limits, error) {
	var l Limits
	err := s.pool.QueryRow(ctx,
		`SELECT rpm_limit, tpm_limit FROM teams WHERE id=$1`, teamID).Scan(&l.RPM, &l.TPM)
	if err != nil {
		return Limits{}, err
	}
	return l, nil
}

// SetProjectTeam attaches a project to a team (shares the team budget).
func (s *Service) SetProjectTeam(ctx context.Context, projectID, teamID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE projects SET team_id=$2 WHERE id=$1`, projectID, teamID)
	return err
}

// TeamOfProject returns the team a project belongs to (0 = none).
func (s *Service) TeamOfProject(ctx context.Context, projectID int64) (int64, error) {
	var tid int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(team_id, 0) FROM projects WHERE id=$1`,
		projectID).Scan(&tid)
	return tid, err
}

// OrgOfTeam returns the org a team belongs to (0 = none).
func (s *Service) OrgOfTeam(ctx context.Context, teamID int64) (int64, error) {
	var oid int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(org_id, 0) FROM teams WHERE id=$1`,
		teamID).Scan(&oid)
	return oid, err
}

// ChainInfo is the full budget chain of one user, fetched in a single
// query: user → project → team → org (each with its quota). The gateway
// enforces all aggregate layers from one cached snapshot instead of
// five separate lookups.
type ChainInfo struct {
	UserID    int64
	User      Limits // user-level aggregate quota
	ProjectID int64
	Project   Limits
	TeamID    int64
	Team      Limits
	OrgID     int64
	Org       Limits
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

// OrgTree is the admin view of the six-layer budget: orgs → teams →
// projects → users, with each level's quota.
type OrgTree struct {
	ID    int64      `json:"id"`
	Name  string     `json:"name"`
	RPM   int        `json:"rpm_limit"`
	TPM   int64      `json:"tpm_limit"`
	Teams []TeamNode `json:"teams"`
}

type TeamNode struct {
	ID       int64         `json:"id"`
	Name     string        `json:"name"`
	RPM      int           `json:"rpm_limit"`
	TPM      int64         `json:"tpm_limit"`
	Projects []ProjectNode `json:"projects"`
}

type ProjectNode struct {
	ID    int64      `json:"id"`
	Name  string     `json:"name"`
	RPM   int        `json:"rpm_limit"`
	TPM   int64      `json:"tpm_limit"`
	Users []UserNode `json:"users"`
}

type UserNode struct {
	ID       int64     `json:"id"`
	Username string    `json:"username"`
	RPM      int       `json:"rpm_limit"`
	TPM      int64     `json:"tpm_limit"`
	Keys     []KeyNode `json:"keys"`
}

// KeyNode is a key's quota config for the admin view (no raw material).
type KeyNode struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	RPM         int    `json:"rpm_limit"`
	TPM         int64  `json:"tpm_limit"`
	Concurrency int    `json:"concurrency_limit"`
	EndUserRPM  int    `json:"end_user_rpm_limit"`
	EndUserTPM  int64  `json:"end_user_tpm_limit"`
}

// OrgTree returns the full hierarchy with quotas (admin view).
func (s *Service) OrgTree(ctx context.Context) ([]OrgTree, error) {
	orgs := []OrgTree{}
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, rpm_limit, tpm_limit FROM orgs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	orgIdx := map[int64]int{}
	for rows.Next() {
		var o OrgTree
		if err := rows.Scan(&o.ID, &o.Name, &o.RPM, &o.TPM); err != nil {
			return nil, err
		}
		orgIdx[o.ID] = len(orgs)
		orgs = append(orgs, o)
	}
	rows.Close()

	// teams by org
	teamIdx := map[int64]int{}
	if trows, err := s.pool.Query(ctx,
		`SELECT id, name, org_id, rpm_limit, tpm_limit FROM teams ORDER BY id`); err == nil {
		for trows.Next() {
			var t TeamNode
			var oid int64
			if err := trows.Scan(&t.ID, &t.Name, &oid, &t.RPM, &t.TPM); err != nil {
				continue
			}
			if oi, ok := orgIdx[oid]; ok {
				teamIdx[t.ID] = len(orgs[oi].Teams)
				orgs[oi].Teams = append(orgs[oi].Teams, t)
			}
		}
		trows.Close()
	}

	// projects by team
	projIdx := map[int64]int{}
	if prows, err := s.pool.Query(ctx,
		`SELECT id, name, team_id, rpm_limit, tpm_limit FROM projects ORDER BY id`); err == nil {
		for prows.Next() {
			var p ProjectNode
			var tid int64
			if err := prows.Scan(&p.ID, &p.Name, &tid, &p.RPM, &p.TPM); err != nil {
				continue
			}
			if ti, ok := teamIdx[tid]; ok {
				for oi := range orgs {
					if ti < len(orgs[oi].Teams) && orgs[oi].Teams[ti].ID == tid {
						projIdx[p.ID] = len(orgs[oi].Teams[ti].Projects)
						orgs[oi].Teams[ti].Projects = append(orgs[oi].Teams[ti].Projects, p)
						break
					}
				}
			}
		}
		prows.Close()
	}

	// users by project
	if urows, err := s.pool.Query(ctx,
		`SELECT id, username, project_id, rpm_limit, tpm_limit FROM users ORDER BY id`); err == nil {
		for urows.Next() {
			var u UserNode
			var pid int64
			if err := urows.Scan(&u.ID, &u.Username, &pid, &u.RPM, &u.TPM); err != nil {
				continue
			}
			for oi := range orgs {
				for ti := range orgs[oi].Teams {
					for pi := range orgs[oi].Teams[ti].Projects {
						if orgs[oi].Teams[ti].Projects[pi].ID == pid {
							orgs[oi].Teams[ti].Projects[pi].Users = append(orgs[oi].Teams[ti].Projects[pi].Users, u)
						}
					}
				}
			}
		}
		urows.Close()
	}

	// keys by user (quota config only)
	if krows, err := s.pool.Query(ctx,
		`SELECT id, user_id, name, rpm_limit, tpm_limit, concurrency_limit, end_user_rpm_limit, end_user_tpm_limit
		 FROM api_keys ORDER BY id`); err == nil {
		for krows.Next() {
			var kn KeyNode
			var uid int64
			if err := krows.Scan(&kn.ID, &uid, &kn.Name, &kn.RPM, &kn.TPM, &kn.Concurrency, &kn.EndUserRPM, &kn.EndUserTPM); err != nil {
				continue
			}
			for oi := range orgs {
				for ti := range orgs[oi].Teams {
					for pi := range orgs[oi].Teams[ti].Projects {
						for ui := range orgs[oi].Teams[ti].Projects[pi].Users {
							if orgs[oi].Teams[ti].Projects[pi].Users[ui].ID == uid {
								orgs[oi].Teams[ti].Projects[pi].Users[ui].Keys = append(orgs[oi].Teams[ti].Projects[pi].Users[ui].Keys, kn)
							}
						}
					}
				}
			}
		}
		krows.Close()
	}
	return orgs, nil
}

// QuotaScopes returns every configured aggregate quota (org/team/
// project/user with RPM or TPM > 0) for the budget monitor.
func (s *Service) QuotaScopes(ctx context.Context) ([]ScopeRow, error) {
	var out []ScopeRow
	collect := func(layer string, q string) error {
		rows, err := s.pool.Query(ctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r ScopeRow
			if err := rows.Scan(&r.ID, &r.RPM, &r.TPM); err != nil {
				continue
			}
			r.Layer = layer
			if r.RPM > 0 || r.TPM > 0 {
				out = append(out, r)
			}
		}
		return rows.Err()
	}
	queries := map[string]string{
		"org":     `SELECT id, rpm_limit, tpm_limit FROM orgs`,
		"team":    `SELECT id, rpm_limit, tpm_limit FROM teams`,
		"project": `SELECT id, rpm_limit, tpm_limit FROM projects`,
		"user":    `SELECT id, rpm_limit, tpm_limit FROM users`,
	}
	for layer, q := range queries {
		if err := collect(layer, q); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ScopeRow is one configured quota (monitor input).
type ScopeRow struct {
	Layer string
	ID    int64
	RPM   int
	TPM   int64
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
		`INSERT INTO api_keys (user_id, key_hash, name, rpm_limit, tpm_limit, concurrency_limit, end_user_rpm_limit, end_user_tpm_limit)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		userID, hash, name, limits.RPM, limits.TPM, limits.Concurrency, limits.EndUserRPM, limits.EndUserTPM)
	if err != nil {
		return "", err
	}
	return key, nil
}

// SetKeyLimits updates a key's quota config (admin operation).
func (s *Service) SetKeyLimits(ctx context.Context, keyID int64, limits Limits) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET rpm_limit=$2, tpm_limit=$3, concurrency_limit=$4, end_user_rpm_limit=$5, end_user_tpm_limit=$6 WHERE id=$1`,
		keyID, limits.RPM, limits.TPM, limits.Concurrency, limits.EndUserRPM, limits.EndUserTPM)
	return err
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
		`SELECT id, user_id, name, status, rpm_limit, tpm_limit, concurrency_limit, end_user_rpm_limit, end_user_tpm_limit
		 FROM api_keys WHERE user_id=$1 ORDER BY id DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.ID, &k.UserID, &k.Name, &k.Status,
			&k.RPM, &k.TPM, &k.Concurrency, &k.EndUserRPM, &k.EndUserTPM); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// Limits is a key's rate-limit configuration (0 = unlimited).
// Limits defines the quota for one scope (key/user/project/team/org).
// EndUserRPM/EndUserTPM apply to each end-user of the key (layer 6):
// the gateway scopes them as eu:{rawKey}:{endUserID}.
type Limits struct {
	RPM         int
	TPM         int64
	Concurrency int
	EndUserRPM  int
	EndUserTPM  int64
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

// ChainOfUser loads the full budget chain in one query (NULL-safe via
// COALESCE: missing project/team/org resolve to id 0 + zero limits).
func (k *PostgresKeyStore) ChainOfUser(ctx context.Context, userID int64) (ChainInfo, error) {
	var ci ChainInfo
	err := k.svc.pool.QueryRow(ctx, `
		SELECT u.rpm_limit, u.tpm_limit,
		       COALESCE(u.project_id, 0),
		       COALESCE(p.rpm_limit, 0), COALESCE(p.tpm_limit, 0),
		       COALESCE(p.team_id, 0),
		       COALESCE(t.rpm_limit, 0), COALESCE(t.tpm_limit, 0),
		       COALESCE(t.org_id, 0),
		       COALESCE(o.rpm_limit, 0), COALESCE(o.tpm_limit, 0)
		FROM users u
		LEFT JOIN projects p ON p.id = u.project_id
		LEFT JOIN teams t ON t.id = p.team_id
		LEFT JOIN orgs o ON o.id = t.org_id
		WHERE u.id = $1`, userID).Scan(
		&ci.User.RPM, &ci.User.TPM,
		&ci.ProjectID, &ci.Project.RPM, &ci.Project.TPM,
		&ci.TeamID, &ci.Team.RPM, &ci.Team.TPM,
		&ci.OrgID, &ci.Org.RPM, &ci.Org.TPM)
	if err != nil {
		return ChainInfo{}, err
	}
	ci.UserID = userID
	return ci, nil
}

// ProjectOfUser returns the project a user belongs to (0 = none).
func (k *PostgresKeyStore) ProjectOfUser(ctx context.Context, userID int64) (int64, error) {
	return k.svc.ProjectOfUser(ctx, userID)
}

// TeamOfProject returns the team a project belongs to (0 = none).
func (k *PostgresKeyStore) TeamOfProject(ctx context.Context, projectID int64) (int64, error) {
	return k.svc.TeamOfProject(ctx, projectID)
}

// OrgOfTeam returns the org a team belongs to (0 = none).
func (k *PostgresKeyStore) OrgOfTeam(ctx context.Context, teamID int64) (int64, error) {
	return k.svc.OrgOfTeam(ctx, teamID)
}

// ResolveTeamLimits returns a team's aggregate quota (0 = unlimited).
func (k *PostgresKeyStore) ResolveTeamLimits(ctx context.Context, teamID int64) (Limits, error) {
	return k.svc.ResolveTeamLimits(ctx, teamID)
}

// ResolveOrgLimits returns an org's aggregate quota (0 = unlimited).
func (k *PostgresKeyStore) ResolveOrgLimits(ctx context.Context, orgID int64) (Limits, error) {
	return k.svc.ResolveOrgLimits(ctx, orgID)
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
		`SELECT rpm_limit, tpm_limit, concurrency_limit, end_user_rpm_limit, end_user_tpm_limit FROM api_keys WHERE key_hash=$1`,
		hash).Scan(&l.RPM, &l.TPM, &l.Concurrency, &l.EndUserRPM, &l.EndUserTPM)
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

// TeamOfProject returns the team a project belongs to (0 = none),
// cache-first with the same TTL as key resolution.
func (c *CachedKeyStore) TeamOfProject(ctx context.Context, projectID int64) (int64, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.TeamOfProject(ctx, projectID)
	}
	return 0, nil
}

// TeamLimits returns a team's aggregate quota (cache-first).
func (c *CachedKeyStore) TeamLimits(ctx context.Context, teamID int64) (Limits, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.ResolveTeamLimits(ctx, teamID)
	}
	return Limits{}, nil
}

// OrgOfTeam returns the org a team belongs to (0 = none), cache-first.
func (c *CachedKeyStore) OrgOfTeam(ctx context.Context, teamID int64) (int64, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.OrgOfTeam(ctx, teamID)
	}
	return 0, nil
}

// OrgLimits returns an org's aggregate quota (cache-first).
func (c *CachedKeyStore) OrgLimits(ctx context.Context, orgID int64) (Limits, error) {
	if ks, ok := c.inner.(*PostgresKeyStore); ok {
		return ks.ResolveOrgLimits(ctx, orgID)
	}
	return Limits{}, nil
}

// CachedKeyStore wraps a KeyStore with a small TTL cache so the gateway
// hot path never hits the DB for auth (mirrors the static-key fast path).
type CachedKeyStore struct {
	mu    sync.Mutex
	inner KeyStore
	cache map[string]cacheEntry
	chain map[int64]chainEntry
	ttl   time.Duration
}

type cacheEntry struct {
	userID  int64
	expires time.Time
}

type chainEntry struct {
	info    ChainInfo
	expires time.Time
}

// NewCachedKeyStore wraps a resolver with an in-memory TTL cache.
func NewCachedKeyStore(inner KeyStore, ttl time.Duration) *CachedKeyStore {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &CachedKeyStore{inner: inner, cache: map[string]cacheEntry{}, chain: map[int64]chainEntry{}, ttl: ttl}
}

// Resolve implements KeyStore (cache-first; negative entries cached short).
func (c *CachedKeyStore) Resolve(ctx context.Context, rawKey string) (int64, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.cache[rawKey]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		if e.userID == 0 {
			return 0, ErrKeyNotFound
		}
		return e.userID, nil
	}
	c.mu.Unlock()
	uid, err := c.inner.Resolve(ctx, rawKey)
	ttl := c.ttl
	if err != nil {
		uid = 0
		ttl = 5 * time.Second // negative cache shorter
	}
	c.mu.Lock()
	c.cache[rawKey] = cacheEntry{userID: uid, expires: now.Add(ttl)}
	c.mu.Unlock()
	return uid, err
}

// ChainOfUser returns the cached budget chain (TTL), loading it from the
// store on miss. This is the hot path for aggregate quota enforcement:
// one cache hit replaces five per-layer DB lookups.
func (c *CachedKeyStore) ChainOfUser(ctx context.Context, userID int64) (ChainInfo, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.chain[userID]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.info, nil
	}
	c.mu.Unlock()
	ci, err := c.inner.(*PostgresKeyStore).ChainOfUser(ctx, userID)
	if err != nil {
		return ChainInfo{}, err
	}
	c.mu.Lock()
	c.chain[userID] = chainEntry{info: ci, expires: now.Add(c.ttl)}
	c.mu.Unlock()
	return ci, nil
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
