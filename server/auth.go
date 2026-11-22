package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// Version is the server version reported on the wire and in the startup log.
// It is the version package's value (devel on a plain build, the tag under
// ldflags), so the flag, the log, the audit header and
// AuthResponse.ServerVersion all read one source.
var Version = version.Version

// common errors returned by the auth layer.
var (
	ErrUserNotFound = errors.New("user not found")
	ErrUserDisabled = errors.New("user is disabled")
	ErrBadPassword  = errors.New("invalid password")
	ErrUserExists   = errors.New("user already exists")
	// ErrUserDroppedSince refuses a create whose drop fence predates a drop: the account it
	// names may be one that was dropped, so creating it again could bring it back.
	ErrUserDroppedSince = errors.New("a user was dropped since the users were read")
	// ErrTooManyGrants refuses a change that would give one account more grants than a reply can
	// carry (wire.MaxUserGrants).
	ErrTooManyGrants = errors.New("too many grants on one account")

	// ErrUsernameInvalid refuses an account name that holds a control character.
	ErrUsernameInvalid = errors.New("username is not allowed")

	// ErrReservedUsername refuses an account named for the node identity.
	ErrReservedUsername = errors.New("username is reserved")

	// ErrPepperKeyMissing means a record names a signing key the keyring no
	// longer holds, so its peppered password cannot be verified. The cause is
	// almost always changed key material (cluster token or deployment id),
	// not a wrong password, and the error says so (spec 6.2.1).
	ErrPepperKeyMissing = errors.New("password signing key missing")

	// ErrLastAdmin refuses a mutation that would leave no enabled user with
	// user-manager, cluster-manager or root authority (spec 4.7).
	ErrLastAdmin = errors.New("refusing to remove the last administrator")
	// ErrUserVersion refuses an alter whose version does not match committed
	// state, so a concurrent grant is not silently dropped (spec 6.1).
	ErrUserVersion = errors.New("user config version mismatch")
	// ErrSaltReused refuses a creation whose salt already exists, because one
	// leaked salt must not become a table (spec 6.2).
	ErrSaltReused = errors.New("credential salt is already in use")
	// ErrTooManyUsers refuses creation past the supported ceiling (spec 9).
	ErrTooManyUsers = errors.New("user limit reached")

	// ErrUnknownRole is under every refusal of a role name that is not one of
	// the current roles. The caller named it, so it reaches the wire as
	// INVALID_ARGUMENT (BUG-0622).
	ErrUnknownRole = errors.New("unknown role")
	// ErrCredentialInvalid is under every refusal of credential material the
	// client derived: an unsupported algorithm, an empty or undecodable salt or
	// hash, a work factor outside the clamp. The client built it, so it reaches
	// the wire as INVALID_ARGUMENT (BUG-0622).
	ErrCredentialInvalid = errors.New("the credential is not acceptable")
	// ErrUsernameRequired is under the refusal of a create with an empty name.
	ErrUsernameRequired = errors.New("a username is required")

	// ErrTokenExpired is a signature-valid token that is past its expiry or
	// absolute deadline. ErrTokenInvalid is everything else: bad signature,
	// malformed parse, unknown key, wrong epoch. handleAuth maps the two to
	// distinct wire reason codes.
	ErrTokenExpired = errors.New("auth token expired")
	ErrTokenInvalid = errors.New("auth token invalid")
)

// maxTokenBytes caps AuthRequest.Token before any decode: 4 KiB is generous
// for a claims blob and bounds the parse work an unauthenticated caller can
// ask for (spec 3.4 requirement 4).
const maxTokenBytes = 4096

// tokenDecodeHook, when non-nil, runs immediately before the claims JSON
// decode. Tests use it to prove the size cap and segment checks reject before
// the decoder is reached. Nil in production.
var tokenDecodeHook func()

// tokenRefreshGapHook, when non-nil, runs in a token refresh between the check of the claims and
// the read of the record the new token is minted from. Tests use it to place a revocation there.
// Nil in production.
var tokenRefreshGapHook func()

// verifyUserHook, when non-nil, runs immediately before VerifyUser's Argon2
// derivation. Tests use it as an instrumented counter and a way to hold a
// derivation slot open. Nil in production.
var verifyUserHook func()

// tokenNowHook, when non-nil, is the clock ValidateToken reads instead of
// time.Now. Tests use it to place a validation at a chosen instant, which is
// the only way to exercise an expiry boundary without waiting for it. Nil in
// production.
var tokenNowHook func() time.Time

// tokenNow is one reading of the clock token expiry is checked against.
func tokenNow() time.Time {
	if tokenNowHook != nil {
		return tokenNowHook()
	}
	return time.Now()
}

// TokenClaims is the signed token payload. Every field is signed, so adding
// one after tokens are in the field invalidates outstanding tokens — which is
// why the signature covers the exact received claims bytes rather than a
// re-marshal (spec 3.4).
type TokenClaims struct {
	// Subject is the username; the only identity source.
	Subject string `json:"sub"`
	// Roles and Databases are always the record ∩ presented intersection.
	Roles     []string `json:"roles,omitempty"`
	Databases []string `json:"dbs,omitempty"`
	// IssuedAt (iat) and ExpiresAt (exp) are unix seconds.
	IssuedAt  int64 `json:"iat"`
	ExpiresAt int64 `json:"exp"`
	// Deadline (abs) is the absolute end of the refresh chain; inherited on
	// refresh, never extended.
	Deadline int64 `json:"abs"`
	// Epoch is the per-user revocation counter at mint time.
	Epoch uint32 `json:"epoch"`
	// KeyID (kid) selects the signing key generation.
	KeyID string `json:"kid"`
	// Incarnation (inc) is the account's created_seq in a cluster's user
	// table: a token resumes only on the account it was minted for, never on
	// one created again under the same name (ring user store spec 9, F17).
	// Absent on a node without a cluster, whose tokens are unchanged.
	Incarnation int64 `json:"inc,omitempty"`
	// ClusterID (cid) is the hex id of the cluster whose table holds the account, for a cluster
	// row: a cluster rebuilt with the same token key and the same first administrator reaches the
	// same created_seq and epoch, and must still refuse the old cluster's tokens (M2b review F-2).
	// Absent on a node without a cluster.
	ClusterID string `json:"cid,omitempty"`
	// Node is informational: the issuing node.
	Node string `json:"node,omitempty"`
}

// Grant is one role bound to one database. DB is "*" for every database
// (spec 3.2): the same shape whether the grant is scoped or global.
type Grant struct {
	Role string `json:"role"`
	DB   string `json:"db"`
}

// CredentialMaterial is the persisted password material for one user. Hash
// is the *peppered* value (HMAC-SHA256 over the client's Argon2id hash), never
// the client hash itself (spec 3.2, 6.2.1). PepperKID names the key material
// the pepper was derived from so rotation is a window, not a fleet re-auth.
type CredentialMaterial struct {
	Algo      string       `json:"algo"` // "argon2id", the only accepted value
	Salt      string       `json:"salt"` // base64
	Hash      string       `json:"hash"` // base64, peppered
	Params    Argon2Params `json:"params"`
	PepperKID string       `json:"pepper_kid"`
}

// UserRecord is the one user shape, both persisted and in memory (spec 3.2).
// There is deliberately no second type with a mapping layer: two shapes to
// keep in sync is how a role edit goes missing.
type UserRecord struct {
	Username          string             `json:"user"`
	Roles             []Grant            `json:"roles"`
	Credentials       CredentialMaterial `json:"credentials"`
	Enabled           bool               `json:"enabled"`
	Epoch             uint32             `json:"epoch"`
	CreatedAt         time.Time          `json:"created_at"`
	PasswordChangedAt time.Time          `json:"password_changed_at"`
	// LoginGeneration moves whenever a login to this account must end: new
	// credentials or a disable. A member compares it, with CreatedAt, against
	// the record its sessions logged in to, so a change it saw only as a final
	// state (a disable undone, pushes coalesced) still ends them. Absent in
	// files written before it existed, which reads as 0.
	LoginGeneration uint32 `json:"login_generation,omitempty"`
	// FirstStart marks the root account a node creates for itself at its
	// first start. Two such accounts of one name are the same principal
	// meeting, not a re-created account: the first push to a new member
	// replaces its own with the cluster's and keeps the logins it served.
	FirstStart bool `json:"first_start,omitempty"`

	// standing is set only on the copy handed to the AUTH path, never on a
	// stored record, and is not persisted: it is the login standing taken
	// with the credentials that AUTH checked.
	standing *transport.LoginStanding

	// createdSeq and changedSeq are set only on a record installed from a
	// cluster's replicated table (ring user store spec 4.1, 6.3): the
	// state_seq that created the account (its incarnation) and the one that
	// last changed it. A non-zero createdSeq is the record's origin: its hash
	// is peppered with the cluster's password key, not this node's keyring.
	// Never persisted.
	createdSeq int64
	changedSeq int64
}

// RoleNames returns the distinct role names in first-seen order. It is the
// only bridge from the typed record to the flat token claims, so there is
// exactly one stored shape (spec 3.2).
func (r *UserRecord) RoleNames() []string {
	if len(r.Roles) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(r.Roles))
	names := make([]string, 0, len(r.Roles))
	for _, grant := range r.Roles {
		if _, ok := seen[grant.Role]; ok {
			continue
		}
		seen[grant.Role] = struct{}{}
		names = append(names, grant.Role)
	}
	return names
}

// DatabaseNames returns the distinct database names in first-seen order.
func (r *UserRecord) DatabaseNames() []string {
	if len(r.Roles) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(r.Roles))
	names := make([]string, 0, len(r.Roles))
	for _, grant := range r.Roles {
		if _, ok := seen[grant.DB]; ok {
			continue
		}
		seen[grant.DB] = struct{}{}
		names = append(names, grant.DB)
	}
	return names
}

// hasAdminGrant reports whether the record holds the authority the
// last-administrator guard protects: root or user-manager on every database
// (authrules.CanAdministerUsers, ring user store spec 4.2 B8).
func (r *UserRecord) hasAdminGrant() bool {
	return grantsHaveAdmin(r.Roles)
}

// Argon2Params carries the password-hashing cost. JSON matches the frozen
// users.json params object (spec 3.2); BSON carries the same names on the
// client-user-creation wire so a driver and the server agree field for field.
type Argon2Params struct {
	Memory      uint32 `json:"memory" bson:"memory"`
	Iterations  uint32 `json:"iterations" bson:"iterations"`
	Parallelism uint8  `json:"parallelism" bson:"parallelism"`
	SaltLen     int    `json:"salt_len" bson:"salt_len"`
	HashLen     int    `json:"hash_len" bson:"hash_len"`
}

// Default Argon2id parameters (server/auth.go's historical literals).
const (
	defaultArgon2Memory      uint32 = 65536
	defaultArgon2Iterations  uint32 = 3
	defaultArgon2Parallelism uint8  = 4
	defaultArgon2SaltLen     int    = 16
	defaultArgon2HashLen     int    = 32
)

func (p Argon2Params) withDefaults() Argon2Params {
	if p.Memory == 0 {
		p.Memory = defaultArgon2Memory
	}
	if p.Iterations == 0 {
		p.Iterations = defaultArgon2Iterations
	}
	if p.Parallelism == 0 {
		p.Parallelism = defaultArgon2Parallelism
	}
	if p.SaltLen <= 0 {
		p.SaltLen = defaultArgon2SaltLen
	}
	if p.HashLen <= 0 {
		p.HashLen = defaultArgon2HashLen
	}
	return p
}

// AuthTokenOptions configures an AuthManager. It is a struct because the
// manager needs TTL, absolute lifetime, clock skew, a signing keyring and an
// epoch store — more parameters than a readable call site (AGENTS.md rule 7).
type AuthTokenOptions struct {
	// TTL is the per-token lifetime; a token is refreshed by use.
	TTL time.Duration
	// MaxLifetime is the absolute deadline carried unchanged across
	// refreshes; the chain ends there and a password is required again.
	MaxLifetime time.Duration
	// Skew is the clock leeway applied to exp and iat, both directions.
	Skew time.Duration
	// Keyring supplies the signing keys. Required: a manager with no keyring
	// cannot mint a token.
	Keyring *Keyring
	// Epochs persists per-user revocation counters. May be nil until the
	// epoch wiring lands; VerifyClaims nil-checks it.
	Epochs *EpochStore
	// NodeID is stamped into claims as informational provenance. Empty is
	// valid (omitempty).
	NodeID string
	// DeploymentID is the domain-separation suffix the pepper is derived
	// under (spec 6.2.1). It must match the value BuildKeyring used, or stored
	// passwords stop verifying. Empty is valid.
	DeploymentID string
	// UsersPath is where users.json is persisted. Empty disables persistence,
	// which is only correct for tests that exercise in-memory auth only.
	UsersPath string
	// Master is the cluster master recorded in the users.json header. A
	// standalone node records itself.
	Master string
	// Argon2 is the password-hashing cost. Zero fields use the defaults.
	Argon2 Argon2Params
	// MaxDerivations bounds concurrent Argon2 derivations process-wide. <=0
	// uses GOMAXPROCS, which is the documented default (spec 14.7).
	MaxDerivations int
	// DerivationTimeout is how long a password AUTH waits for a derivation
	// slot before being answered AuthFailRateLimited. <=0 uses 5s.
	DerivationTimeout time.Duration
	// PasswordMinLength is the configured password_min_length, reported on
	// every AUTH_OK so the client that derives a new credential can apply it
	// (BUG-0493). It is passed through as configured, with no default here,
	// so the number a client enforces is the one the operator set.
	PasswordMinLength int32
	// ClusterMember is true on a node with cluster.enabled: its users are the
	// cluster's replicated table (ring user store spec 6.3), installed by the
	// table follower, so UsersPath is empty and every direct change refuses.
	ClusterMember bool
}

// AuthManager manages user authentication and token generation.
type AuthManager struct {
	mu           sync.RWMutex
	users        map[string]*UserRecord
	keyring      *Keyring
	selfID       string      // the id the cluster knows this node by (ring id); recorded as master in files this node writes
	nodeMaster   *NodeMaster // the 32-byte root key for node-level encryption; nil when keystore=off
	mode         string      // encryption mode: off, on, or require
	tokenExp     time.Duration
	maxLifetime  time.Duration
	skew         time.Duration
	epochs       *EpochStore
	nodeID       string
	deploymentID string
	usersPath    string
	master       string
	term         uint64
	version      uint64
	// dropSeq is the user_drop_seq of this store (ring-user-store.md 4.3, R3): it rises on every
	// drop, whichever path made it, and on every install, to at least the store version. It is not
	// persisted, so after a restart it starts at the loaded version and a fence read before the
	// restart is refused once rather than trusted.
	dropSeq int64
	loaded  bool
	// lastSync is when the config was last loaded, applied from a push, or
	// locally changed. A follower that has not synced within the staleness
	// bound refuses password AUTHs and refreshes (spec 5.3.5).
	lastSync time.Time
	// blocked is true for a clustered node that started with no users.json and
	// has not yet received one. Such a node refuses AUTH rather than serving
	// logins from an empty user list (spec 5.3.4).
	blocked bool
	// follower is true when this node is not the auth master. Only a follower
	// can be stale: the master is authoritative for its own config.
	follower bool
	argon2   Argon2Params
	// gate bounds concurrent Argon2 derivations; derivationTimeout is how
	// long an AUTH waits for a slot before being rate-limited (spec 3.8).
	gate              *derivationGate
	derivationTimeout time.Duration
	// passwordMinLength is reported on AUTH_OK; it is set once here and never
	// changes, so it is read without mu.
	passwordMinLength int32
	logger            *slog.Logger

	// clusterMember is set once at construction (AuthTokenOptions); clusterKey
	// is the ring node's password key source, set when the node opens.
	clusterMember bool
	clusterKey    atomic.Pointer[clusterKeySource]

	// standingMu guards standings, the login standing of each account that
	// has logged in on this node since its credentials last changed. An entry
	// is taken while am.mu is held for reading and revoked while it is held
	// for writing, so a login and a revocation cannot interleave. One small
	// entry per account, nothing per bucket.
	standingMu sync.Mutex
	standings  map[string]*transport.LoginStanding
}

// standingLocked returns username's current login standing, making one if
// there is none. Caller holds am.mu, for reading or writing.
func (am *AuthManager) standingLocked(username string) *transport.LoginStanding {
	am.standingMu.Lock()
	defer am.standingMu.Unlock()
	if am.standings == nil {
		am.standings = make(map[string]*transport.LoginStanding)
	}
	standing, ok := am.standings[username]
	if !ok {
		standing = &transport.LoginStanding{}
		am.standings[username] = standing
	}
	return standing
}

// revokeLoginsLocked ends every session username opened under its current
// credentials: their standing is revoked, and the next login takes a new one.
// Caller holds am.mu for writing.
func (am *AuthManager) revokeLoginsLocked(username string) {
	am.standingMu.Lock()
	defer am.standingMu.Unlock()
	if standing, ok := am.standings[username]; ok {
		standing.Revoke()
		delete(am.standings, username)
	}
}

// derivationGate bounds concurrent Argon2 derivations process-wide. It is a
// semaphore, not a queue: a caller that cannot acquire a slot within the
// timeout is refused rather than left waiting forever, because a queued AUTH
// that never returns is its own failure mode (spec 3.8).
type derivationGate struct {
	slots chan struct{}
}

func newDerivationGate(size int) *derivationGate {
	if size < 1 {
		size = 1
	}
	return &derivationGate{slots: make(chan struct{}, size)}
}

func (g *derivationGate) acquire(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case g.slots <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

func (g *derivationGate) release() {
	<-g.slots
}

// acquireDerivation reserves a slot in the process-wide Argon2 semaphore.
func (am *AuthManager) acquireDerivation() bool {
	return am.gate.acquire(am.derivationTimeout)
}

// releaseDerivation returns a slot acquired by acquireDerivation.
func (am *AuthManager) releaseDerivation() {
	am.gate.release()
}

// busyRefusalMessage is the RATE_LIMITED text for an AUTH that found every
// derivation slot taken. A client in a login storm reads it at 3am, so it says
// the password was never looked at and lists what actually ends the storm
// (root AGENTS.md rule 3).
func (am *AuthManager) busyRefusalMessage() string {
	slots := cap(am.gate.slots)
	slotNoun := "slots"
	if slots == 1 {
		slotNoun = "slot"
	}
	return fmt.Sprintf("password was not checked: the server waited %s for a free password-hashing slot (it has %d password-hashing %s) "+
		"and every one stayed busy with other logins. Retry after a jittered backoff (this connection may retry "+
		"the same AUTH); to stop paying for a password hash per connection, reuse pooled connections or resume "+
		"with the session token from an earlier login; if this host has spare CPU and memory, raise "+
		"HOARDDB_AUTH_MAX_DERIVATIONS (now %d)",
		am.derivationTimeout, slots, slotNoun, slots)
}

// NewAuthManager creates an AuthManager over the supplied options. The
// signing key comes from the keyring — a manager never generates one, so no
// code path can sign a token without a persisted or derived key.
func NewAuthManager(opts AuthTokenOptions, logger *slog.Logger) *AuthManager {
	keyring := opts.Keyring
	if keyring == nil {
		keyring = &Keyring{}
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	maxLifetime := opts.MaxLifetime
	if maxLifetime <= 0 {
		maxLifetime = 12 * time.Hour
	}
	skew := opts.Skew
	if skew < 0 {
		skew = 0
	}
	maxDerivations := opts.MaxDerivations
	if maxDerivations <= 0 {
		maxDerivations = runtime.GOMAXPROCS(0)
	}
	derivationTimeout := opts.DerivationTimeout
	if derivationTimeout <= 0 {
		derivationTimeout = 5 * time.Second
	}
	am := &AuthManager{
		users:             make(map[string]*UserRecord),
		keyring:           keyring,
		tokenExp:          ttl,
		maxLifetime:       maxLifetime,
		skew:              skew,
		epochs:            opts.Epochs,
		nodeID:            opts.NodeID,
		deploymentID:      opts.DeploymentID,
		usersPath:         opts.UsersPath,
		master:            opts.Master,
		argon2:            opts.Argon2.withDefaults(),
		gate:              newDerivationGate(maxDerivations),
		derivationTimeout: derivationTimeout,
		passwordMinLength: opts.PasswordMinLength,
		logger:            logger,
		clusterMember:     opts.ClusterMember,
	}
	// The keyring must know which kids a stored credential still names before
	// it prunes one. It is installed here, after am exists, and reads am.mu
	// under the keyring's lock; no AuthManager path may hold am.mu while
	// calling a keyring method, or the two locks deadlock.
	keyring.SetKeyInUse(am.CountPepperKID)
	return am
}

// CountPepperKID reports how many stored records name a signing key. It is
// the keyring's prune guard: a key still named by a credential is never
// dropped, however old (spec 6.2.1). It takes am.mu.RLock and is called from
// under the keyring lock, so no AuthManager method may hold am.mu while
// calling a keyring method.
func (am *AuthManager) CountPepperKID(kid string) int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	count := 0
	for _, record := range am.users {
		if record.Credentials.PepperKID == kid {
			count++
		}
	}
	return count
}

// SeedRootUser creates the root admin user with Argon2id-hashed password.
func (am *AuthManager) SeedRootUser(username, password string) error {
	if err := am.refuseInCluster("seed root user"); err != nil {
		return err
	}
	if err := checkUsernameNotReserved(username); err != nil {
		return fmt.Errorf("seed root user: %w", err)
	}
	if err := checkUsername(username); err != nil {
		return fmt.Errorf("seed root user: %w", err)
	}
	// Derive the key material before taking am.mu. The keyring's prune
	// callback reads am.mu, so taking am.mu first would be an ABBA deadlock
	// against the keyring's own lock.
	key := am.keyring.Current()
	if len(key.Bytes) == 0 {
		return errors.New("seed root user: no signing key loaded")
	}
	pepper := PepperForKey(key, am.deploymentID)

	salt := make([]byte, am.argon2.SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}

	clientHash := argon2.IDKey([]byte(password), salt,
		am.argon2.Iterations, am.argon2.Memory, am.argon2.Parallelism,
		uint32(am.argon2.HashLen))
	storedHash := PepperHash(pepper, clientHash)

	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.users[username]; exists {
		return fmt.Errorf("seed root user: %w", ErrUserExists)
	}

	now := time.Now().UTC()
	record := &UserRecord{
		Username: username,
		Credentials: CredentialMaterial{
			Algo:      "argon2id",
			Salt:      base64.StdEncoding.EncodeToString(salt),
			Hash:      base64.StdEncoding.EncodeToString(storedHash),
			Params:    am.argon2,
			PepperKID: key.ID,
		},
		Roles:             []Grant{{Role: authrules.RoleRoot, DB: authrules.AllDatabases}},
		Enabled:           true,
		CreatedAt:         now,
		PasswordChangedAt: now,
		FirstStart:        true,
	}
	if am.epochs != nil {
		record.Epoch = am.epochs.Get(username)
	}

	am.users[username] = record
	am.logger.Info("root user seeded", "username", username, "roles", record.RoleNames())
	if err := am.persistLocked(); err != nil {
		delete(am.users, username)
		return fmt.Errorf("persist seeded root user %q: %w", username, err)
	}
	return nil
}

// VerifyUser checks if the given password matches the stored hash.
//
// The record is copied under RLock and the lock is released before the
// Argon2id derivation runs. Holding a write lock across ~33 ms of hashing
// would serialize every AUTH in the process and defeat the derivation
// semaphore (spec 3.8).
func (am *AuthManager) VerifyUser(username, password string) (bool, error) {
	standing, err := am.verifyLogin(username, password)
	return standing != nil, err
}

// verifyLogin is VerifyUser returning the login standing taken together with
// the credentials it checked, so a change committed after that read revokes
// the session the login opens. It is nil exactly when the login failed.
func (am *AuthManager) verifyLogin(username, password string) (*transport.LoginStanding, error) {
	// A cluster member checks passwords under the cluster's password key and
	// asks for it first: without it no password can be checked, so nothing
	// is derived, for a known account or an unknown one (spec 5.5).
	var clusterKey clusterpepper.Key
	var clusterKID string
	if am.clusterMember {
		key, kid, err := am.passwordKey()
		if err != nil {
			return nil, err
		}
		clusterKey, clusterKID = key, kid
	}
	am.mu.RLock()
	record, exists := am.users[username]
	if !exists {
		am.mu.RUnlock()
		am.deriveWithoutAccount(password, am.argon2)
		return nil, ErrUserNotFound
	}
	enabled := record.Enabled
	material := record.Credentials
	fromCluster := record.createdSeq > 0
	var standing *transport.LoginStanding
	if enabled {
		standing = am.standingLocked(username)
	}
	am.mu.RUnlock()

	if !enabled {
		am.deriveWithoutAccount(password, material.Params)
		return nil, ErrUserDisabled
	}

	// The pepper is chosen by where the record came from (spec 5.5): a row
	// of the cluster's table is checked under the cluster's key, a users.json
	// record under the pepper for the kid it names, which is what makes a
	// signing-key rotation a window rather than a fleet-wide re-auth. A miss
	// means the key material changed and the operator has to restore it or
	// reset the password: say so, because "invalid password" would send them
	// looking in the wrong place.
	var verify func(clientHash, expected []byte) bool
	if fromCluster {
		if !am.clusterMember || material.PepperKID != clusterKID {
			return nil, fmt.Errorf("%w: user %q: its password was peppered with the cluster key %q and this node holds %q",
				ErrPepperKeyMissing, username, material.PepperKID, clusterKID)
		}
		verify = func(clientHash, expected []byte) bool { return clusterKey.Verify(clientHash, expected) }
	} else {
		key, ok := am.keyring.KeyByID(material.PepperKID)
		if !ok {
			return nil, fmt.Errorf("%w: user %q: the key material changed — the signing key %q this password was written under is not in the keyring; restore the key material (cluster token or HOARDDB_AUTH_DEPLOYMENT_ID) or reset the password with --reset-password",
				ErrPepperKeyMissing, username, material.PepperKID)
		}
		pepper := PepperForKey(key, am.deploymentID)
		verify = func(clientHash, expected []byte) bool { return hmac.Equal(expected, PepperHash(pepper, clientHash)) }
	}

	salt, err := base64.StdEncoding.DecodeString(material.Salt)
	if err != nil {
		return nil, fmt.Errorf("decode salt: %w", err)
	}

	expectedHash, err := base64.StdEncoding.DecodeString(material.Hash)
	if err != nil {
		return nil, fmt.Errorf("decode hash: %w", err)
	}
	params := material.Params.withDefaults()

	if verifyUserHook != nil {
		verifyUserHook()
	}
	clientHash := argon2.IDKey([]byte(password), salt,
		params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
	if !verify(clientHash, expectedHash) {
		return nil, ErrBadPassword
	}

	return standing, nil
}

// deriveWithoutAccount does the work of one password check for a login with no
// usable credential (no such user, or a disabled one) and discards the result,
// so the answer takes as long as a wrong password for an enabled account and
// says nothing about which accounts exist. It runs under the caller's
// derivation slot, as the real check does.
func (am *AuthManager) deriveWithoutAccount(password string, params Argon2Params) {
	params = params.withDefaults()
	salt := make([]byte, params.SaltLen)
	if verifyUserHook != nil {
		verifyUserHook()
	}
	argon2.IDKey([]byte(password), salt, params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
}

// GenerateToken signs claims into the wire format <claims_b64>.<sig_b64>,
// base64url without padding. The signature covers the exact claims segment
// bytes, so a verifier never re-marshals claims (spec 3.4 requirement 1). The
// current key's kid is stamped into the claims so a validator selects the key
// by id instead of trying every key.
func (am *AuthManager) GenerateToken(claims TokenClaims) (string, error) {
	key := am.keyring.Current()
	if len(key.Bytes) == 0 {
		return "", errors.New("sign token: no signing key loaded")
	}
	claims.KeyID = key.ID

	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("sign token: marshal claims: %w", err)
	}
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	mac := hmac.New(sha256.New, key.Bytes)
	mac.Write([]byte(claimsB64))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return claimsB64 + "." + sigB64, nil
}

// ClaimsFor mints a fresh password-AUTH claim set from the user record,
// starting a new refresh chain (and therefore a freshly jittered absolute
// deadline).
func (am *AuthManager) ClaimsFor(username string) (TokenClaims, error) {
	record, err := am.recordFor(username)
	if err != nil {
		return TokenClaims{}, err
	}
	return am.MintClaims(record, nil, time.Now()), nil
}

// mintExpiry is the exp claim for a token minted at now: the ceiling of
// now+ttl in whole seconds. Truncating would give the token up to a second
// less life than its TTL, which a short TTL makes visible (BUG-0633). The
// claim stays whole seconds, so the wire format is unchanged.
func mintExpiry(now time.Time, ttl time.Duration) int64 {
	expiresAt := now.Add(ttl)
	whole := expiresAt.Unix()
	if expiresAt.After(time.Unix(whole, 0)) {
		return whole + 1
	}
	return whole
}

// MintClaims derives the next token's claims. It is never "from the record
// alone" or "from the token alone": roles and databases are the intersection
// of the record and the presented token, so a narrowed token is never widened
// and a narrowed record takes effect on the next refresh (spec 3.8). The
// absolute deadline is inherited unchanged from presented on a refresh and
// jittered ±10% only when a chain starts, so it cannot creep.
func (am *AuthManager) MintClaims(record *UserRecord, presented *TokenClaims, now time.Time) TokenClaims {
	roles := record.RoleNames()
	databases := record.DatabaseNames()
	deadline := am.jitterDeadline(now.Add(am.maxLifetime))
	if presented != nil {
		roles = intersectClaims(record.RoleNames(), presented.Roles)
		databases = intersectClaims(record.DatabaseNames(), presented.Databases)
		deadline = time.Unix(presented.Deadline, 0)
	}
	return TokenClaims{
		Subject:     record.Username,
		Roles:       roles,
		Databases:   databases,
		IssuedAt:    now.Unix(),
		ExpiresAt:   mintExpiry(now, am.tokenExp),
		Deadline:    deadline.Unix(),
		Epoch:       record.Epoch,
		KeyID:       am.keyring.Current().ID,
		Incarnation: record.createdSeq,
		ClusterID:   am.tokenClusterID(record.createdSeq),
		Node:        am.nodeID,
	}
}

// ValidateToken is pure token validation: size cap, exactly two segments,
// strict base64url, kid lookup, HMAC over the received claims segment, then
// time checks. It performs no record lookup and takes no AuthManager lock
// beyond the keyring's own.
//
// The kid lives inside the claims, so the claims segment is decoded before
// the HMAC runs — there is no way to select a key by kid otherwise, and
// trying every key is forbidden (spec 3.4 requirement 6). The size cap at the
// top bounds that decode, and no claim content is ever included in an error.
func (am *AuthManager) ValidateToken(presented string) (TokenClaims, error) {
	var zero TokenClaims
	if len(presented) == 0 || len(presented) > maxTokenBytes {
		return zero, fmt.Errorf("%w: token length %d", ErrTokenInvalid, len(presented))
	}

	claimsB64, sigB64, found := strings.Cut(presented, ".")
	if !found || claimsB64 == "" || sigB64 == "" || strings.Contains(sigB64, ".") {
		return zero, fmt.Errorf("%w: malformed token segments", ErrTokenInvalid)
	}

	claimsJSON, err := base64.RawURLEncoding.DecodeString(claimsB64)
	if err != nil {
		return zero, fmt.Errorf("%w: invalid claims encoding", ErrTokenInvalid)
	}
	if tokenDecodeHook != nil {
		tokenDecodeHook()
	}
	var claims TokenClaims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return zero, fmt.Errorf("%w: malformed claims", ErrTokenInvalid)
	}

	key, ok := am.keyring.KeyByID(claims.KeyID)
	if !ok {
		return zero, fmt.Errorf("%w: unknown signing key", ErrTokenInvalid)
	}

	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return zero, fmt.Errorf("%w: invalid signature encoding", ErrTokenInvalid)
	}
	mac := hmac.New(sha256.New, key.Bytes)
	mac.Write([]byte(claimsB64))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return zero, fmt.Errorf("%w: signature mismatch", ErrTokenInvalid)
	}

	now := tokenNow()
	if claims.ExpiresAt != 0 && now.After(time.Unix(claims.ExpiresAt, 0).Add(am.skew)) {
		return zero, fmt.Errorf("%w: token expired", ErrTokenExpired)
	}
	if claims.IssuedAt > now.Add(am.skew).Unix() {
		return zero, fmt.Errorf("%w: issued in the future", ErrTokenInvalid)
	}
	if claims.Deadline != 0 && !now.Before(time.Unix(claims.Deadline, 0)) {
		return zero, fmt.Errorf("%w: refresh chain deadline passed", ErrTokenExpired)
	}
	return claims, nil
}

// VerifyClaims performs the state checks a pure token validation cannot:
// the user still exists and the claim epoch is the record's current epoch. It
// runs under RLock so a concurrent InvalidateUserTokens cannot interleave a
// read-then-compare into a spurious accept.
func (am *AuthManager) VerifyClaims(claims TokenClaims) error {
	_, err := am.verifyClaimsStanding(claims)
	return err
}

// verifyClaimsStanding is VerifyClaims returning the login standing taken
// together with the epoch it compared, so a revocation committed after that
// read ends the session the refresh opens.
func (am *AuthManager) verifyClaimsStanding(claims TokenClaims) (*transport.LoginStanding, error) {
	am.mu.RLock()
	record, ok := am.users[claims.Subject]
	if !ok {
		am.mu.RUnlock()
		return nil, fmt.Errorf("%w: unknown user", ErrTokenInvalid)
	}
	held := *record
	var standing *transport.LoginStanding
	if record.Enabled {
		standing = am.standingLocked(claims.Subject)
	}
	am.mu.RUnlock()

	if err := am.claimsHoldFor(claims, &held); err != nil {
		return nil, err
	}
	return standing, nil
}

// claimsHoldFor reports why claims no longer stand for record: another cluster, another
// incarnation of the account, another epoch, or a disabled account. A token refresh asks it of the
// record it verified and again of the record it mints from (M2b review F-1), so a change between
// the two reads never mints a token.
func (am *AuthManager) claimsHoldFor(claims TokenClaims, record *UserRecord) error {
	if claims.Incarnation != record.createdSeq {
		return fmt.Errorf("%w: the account %q was dropped and created again since this token was minted; log in again with a password", ErrTokenInvalid, claims.Subject)
	}
	if claims.ClusterID != am.tokenClusterID(record.createdSeq) {
		return fmt.Errorf("%w: this token was minted in another cluster (%q); log in again with a password", ErrTokenInvalid, claims.ClusterID)
	}
	if claims.Epoch != record.Epoch {
		return fmt.Errorf("%w: epoch mismatch", ErrTokenInvalid)
	}
	if !record.Enabled {
		return fmt.Errorf("%w: the account is disabled", ErrTokenInvalid)
	}
	return nil
}

// InvalidateUserTokens bumps a user's epoch, revoking every token issued
// with the previous counter, and persists the record. The record's epoch is
// the only authority now; the retired side file is read once at load and
// never written (spec 6.4). It is the hook every record mutation must call.
func (am *AuthManager) InvalidateUserTokens(username string) error {
	if err := am.refuseInCluster("invalidate tokens"); err != nil {
		return err
	}
	am.mu.Lock()
	defer am.mu.Unlock()

	record, ok := am.users[username]
	if !ok {
		return nil
	}
	record.Epoch++
	if err := am.persistLocked(); err != nil {
		record.Epoch--
		return fmt.Errorf("persist epoch bump for %q: %w", username, err)
	}
	return nil
}

// LoadUsers reads the persisted store into memory, folding the retired
// per-user epoch counters into each record. A missing file returns
// (false, 0, nil): it is a first start, not a failure. folded counts the
// records the side file raised; the caller logs it once per start.
func (am *AuthManager) LoadUsers(path string, nodeMaster *NodeMaster, epochs *EpochStore) (present bool, folded int, err error) {
	if err := am.refuseInCluster("load users"); err != nil {
		return false, 0, err
	}
	cfg, err := LoadUsersConfig(path, am.keyring, nodeMaster)
	if errors.Is(err, ErrNoUsersFile) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	removed, err := sweepPlaintextPrev(path)
	if err != nil {
		return false, 0, err
	}
	if removed {
		am.logger.Warn("removed an unencrypted users.json.prev left beside the encrypted user store by an interrupted save",
			"path", path+".prev")
	}
	if err := cfg.errRetiredRoles(); err != nil {
		return false, 0, errRetiredRolesInFile(path, err)
	}
	if err := cfg.validateWith(am.argon2); err != nil {
		return false, 0, fmt.Errorf("invalid users file %s: %w", path, err)
	}
	cfg.warnUnusableSalts(am.logger, "users file")

	am.mu.Lock()
	defer am.mu.Unlock()
	am.usersPath = path
	folded, _ = am.installLocked(cfg, epochs)
	if _, planted := am.users[NodeUser]; planted {
		// Not fatal: the account is inert (handleAuth sends NodeUser to the
		// cluster-token branch), and refusing to start would turn a naming
		// defect into an outage.
		am.logger.Warn("users file holds an account named for the node identity; it cannot log in and blurs audit attribution. Drop it with 'drop user \""+NodeUser+"\"'",
			"user", NodeUser, "path", path)
	}
	am.loaded = true
	am.lastSync = time.Now()
	am.updateMetricsLocked()
	return true, folded, nil
}

// SetSelfID sets the id recorded as the master in the users file this node
// writes. It is the ring id, known only after the cluster is built.
func (am *AuthManager) SetSelfID(id string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.selfID = id
}

// installLocked replaces the in-memory records from a config, folding the
// retired epoch counters, and returns how many records the side file raised
// and which accounts lost their logins: dropped, disabled, or given new
// credentials by the config. Their sessions on this node are ended, as the
// master ends its own. Caller holds am.mu for writing.
func (am *AuthManager) installLocked(cfg *usersConfig, epochs *EpochStore) (int, []string) {
	folded := 0
	var revoked []string
	incoming := make(map[string]*UserRecord, len(cfg.Users))
	for i := range cfg.Users {
		incoming[cfg.Users[i].Username] = &cfg.Users[i]
	}
	for name, current := range am.users {
		next, kept := incoming[name]
		if !kept || !next.Enabled || !sameLogin(current, next) {
			am.revokeLoginsLocked(name)
			revoked = append(revoked, name)
		}
	}
	am.term = cfg.Term
	am.version = cfg.Version
	am.dropSeq = max(am.dropSeq+1, int64(cfg.Version))
	am.master = cfg.Master
	am.users = make(map[string]*UserRecord, len(cfg.Users))
	for i := range cfg.Users {
		record := cfg.Users[i]
		if epochs != nil {
			if side := epochs.Get(record.Username); side > record.Epoch {
				record.Epoch = side
				folded++
			}
		}
		am.users[record.Username] = &record
	}
	return folded, revoked
}

// sameLogin reports whether a session that logged in to current may go on
// once the record is next. It may when next is the same account (one
// CreatedAt) whose logins have not been ended since (one LoginGeneration),
// or when both are first-start root accounts: a new member's own meeting the
// cluster's. A re-created account has a new CreatedAt and is not a first-start
// account, so it never passes; nor does a disable undone before this member
// saw it, which moved the generation.
func sameLogin(current, next *UserRecord) bool {
	// A row of a cluster's table names its account by the commit that created
	// it (created_seq, ring user store spec 4.1 F17): a drop and a re-create
	// seen in one install keep no session even with the proposer's clock
	// giving both the same created time.
	if current.createdSeq != 0 || next.createdSeq != 0 {
		return current.createdSeq == next.createdSeq && current.LoginGeneration == next.LoginGeneration
	}
	if !current.CreatedAt.Equal(next.CreatedAt) {
		return current.FirstStart && next.FirstStart
	}
	return current.LoginGeneration == next.LoginGeneration
}

// BootstrapUser applies the spec 7.1 precedence: seed only when no file
// exists, and never overwrite a stored record with a stale environment
// credential. A mismatch is a warning, not an error: the server must still
// start, from the stored record.
func (am *AuthManager) BootstrapUser(username, password string, allowSeed bool) error {
	return am.BootstrapUserFrom(username, password, allowSeed, "HOARDDB_ROOT_PASSWORD")
}

// BootstrapUserFrom is BootstrapUser for a password that came from setting
// (HOARDDB_ROOT_PASSWORD, or HOARDDB_ROOT_PASSWORD_FILE), which the refusal
// names so the operator changes the setting they actually set (BUG-0504).
func (am *AuthManager) BootstrapUserFrom(username, password string, allowSeed bool, setting string) error {
	// A cluster's first administrator is a cluster command, never a record in this node's
	// memory: Service.BootstrapUsersFrom arms the root task instead (ring user store spec 6.5).
	if err := am.refuseInCluster("bootstrap the root user"); err != nil {
		return err
	}
	am.mu.RLock()
	loaded := am.loaded
	_, exists := am.users[username]
	am.mu.RUnlock()

	if loaded {
		if username == "" || password == "" {
			return nil
		}
		if !exists {
			am.logger.Warn("configured root user is absent from the stored users; keeping the stored users (a stale environment variable must not add an administrator)",
				"configured_user", username)
			return nil
		}
		if ok, verifyErr := am.VerifyUser(username, password); verifyErr != nil || !ok {
			reason := "password does not match"
			if verifyErr != nil {
				reason = verifyErr.Error()
			}
			am.logger.Warn("configured root credential does not match the stored record; keeping the stored record",
				"user", username, "reason", reason)
		}
		return nil
	}

	if !allowSeed {
		am.logger.Warn("auth_bootstrap_root is false and no users file exists; not seeding a root user")
		return nil
	}
	if username == "" || password == "" {
		return nil
	}
	// BUG-0504: the seed is the only place the server holds this plaintext, so
	// it is measured here as create user and reset-password measure theirs.
	// Counted in characters, as the CLI counts (cli/user_commands.go).
	if am.passwordMinLength > 0 {
		if length := utf8.RuneCountInString(password); length < int(am.passwordMinLength) {
			return fmt.Errorf("the root password in %s is %d characters, below the %d-character minimum set by HOARDDB_PASSWORD_MIN_LENGTH; choose a longer one, or unset %s and start hoarddb-server from a terminal to type one",
				setting, length, am.passwordMinLength, setting)
		}
	}
	if err := am.SeedRootUser(username, password); err != nil {
		return err
	}
	am.logger.Info("root user bootstrapped into users.json", "username", username, "path", am.usersPath)
	return nil
}

// SetPassword replaces a user's password material under the writer lock,
// bumps the user's epoch so outstanding tokens die, and persists. It is the
// `--reset-password` path (spec 7.1 rule 3).
func (am *AuthManager) SetPassword(username, password string) error {
	if err := am.refuseInCluster("set password"); err != nil {
		return err
	}
	key := am.keyring.Current()
	if len(key.Bytes) == 0 {
		return errors.New("set password: no signing key loaded")
	}
	pepper := PepperForKey(key, am.deploymentID)

	salt := make([]byte, am.argon2.SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}
	clientHash := argon2.IDKey([]byte(password), salt,
		am.argon2.Iterations, am.argon2.Memory, am.argon2.Parallelism, uint32(am.argon2.HashLen))
	storedHash := PepperHash(pepper, clientHash)

	am.mu.Lock()
	defer am.mu.Unlock()
	record, ok := am.users[username]
	if !ok {
		return fmt.Errorf("set password for %q: %w", username, ErrUserNotFound)
	}
	previousCredentials := record.Credentials
	previousEpoch := record.Epoch
	previousChanged := record.PasswordChangedAt

	record.Credentials = CredentialMaterial{
		Algo:      "argon2id",
		Salt:      base64.StdEncoding.EncodeToString(salt),
		Hash:      base64.StdEncoding.EncodeToString(storedHash),
		Params:    am.argon2,
		PepperKID: key.ID,
	}
	record.PasswordChangedAt = time.Now().UTC()
	record.Epoch++
	record.LoginGeneration++
	if err := am.persistLocked(); err != nil {
		record.Credentials = previousCredentials
		record.Epoch = previousEpoch
		record.LoginGeneration--
		record.PasswordChangedAt = previousChanged
		return fmt.Errorf("persist password change for %q: %w", username, err)
	}
	am.revokeLoginsLocked(username)
	return nil
}

// DefaultArgon2Params returns the server's default work factor. Clients use
// it to derive a credential the server accepts without ever seeing the
// plaintext (spec 6.2).
func DefaultArgon2Params() Argon2Params {
	return defaultArgon2Params()
}

// NewCredential derives an Argon2id credential from a plaintext password
// with a fresh random salt. It is the client-side user-creation path: the
// caller sends only the returned material, never the password.
func NewCredential(password string, params Argon2Params) (CredentialMaterial, error) {
	if password == "" {
		return CredentialMaterial{}, errors.New("derive credential: the password is empty")
	}
	params = params.withDefaults()
	salt := make([]byte, params.SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return CredentialMaterial{}, fmt.Errorf("derive credential: generate salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt,
		params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
	return CredentialMaterial{
		Algo:   "argon2id",
		Salt:   base64.StdEncoding.EncodeToString(salt),
		Hash:   base64.StdEncoding.EncodeToString(hash),
		Params: params,
	}, nil
}

// persistLocked writes the current records atomically and advances the
// version. Caller holds am.mu. An empty usersPath is an in-memory-only
// manager (tests) and is a no-op.
func (am *AuthManager) persistLocked() error {
	if am.usersPath == "" {
		return nil
	}
	// Only the auth master persists a mutation, so the file it writes names
	// this node as the master, by the id the cluster knows it by (BUG-0596).
	if am.selfID != "" {
		am.master = am.selfID
	}
	am.version++
	cfg := am.configLocked()
	if err := cfg.Save(am.usersPath, usersSealing{Keyring: am.keyring, NodeMaster: am.nodeMaster, Mode: am.mode, Argon2: am.argon2}); err != nil {
		am.version--
		return err
	}
	// A local write is the commit point: this node's config is current, so a
	// bootstrap or mutation clears the config-less block and refreshes the
	// staleness clock.
	am.lastSync = time.Now()
	am.blocked = false
	am.updateMetricsLocked()
	return nil
}

// configLocked snapshots the in-memory records, sorted by username so the
// file is deterministic. Caller holds am.mu.
func (am *AuthManager) configLocked() *usersConfig {
	users := make([]UserRecord, 0, len(am.users))
	for _, record := range am.users {
		users = append(users, *record)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })
	return &usersConfig{
		FormatVersion: usersFormatVersion,
		Term:          am.term,
		Version:       am.version,
		Master:        am.master,
		Users:         users,
	}
}

// recordFor returns a copy of a user record so callers work outside the
// manager lock.
func (am *AuthManager) recordFor(username string) (*UserRecord, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	record, ok := am.users[username]
	if !ok {
		return nil, fmt.Errorf("claims for %q: %w", username, ErrUserNotFound)
	}
	return &UserRecord{
		Username:          record.Username,
		Roles:             append([]Grant(nil), record.Roles...),
		Credentials:       record.Credentials,
		Enabled:           record.Enabled,
		Epoch:             record.Epoch,
		CreatedAt:         record.CreatedAt,
		PasswordChangedAt: record.PasswordChangedAt,
		LoginGeneration:   record.LoginGeneration,
		FirstStart:        record.FirstStart,
		createdSeq:        record.createdSeq,
		changedSeq:        record.changedSeq,
	}, nil
}

// intersectClaims returns the entries of record that are also present in
// presented, preserving record order. It is the subset rule: a refresh can
// only narrow, never widen.
func intersectClaims(record, presented []string) []string {
	allowed := make(map[string]struct{}, len(presented))
	for _, value := range presented {
		allowed[value] = struct{}{}
	}
	var out []string
	for _, value := range record {
		if _, ok := allowed[value]; ok {
			out = append(out, value)
		}
	}
	return out
}

// jitterDeadline spreads absolute deadlines ±10% per chain so a pool
// established at deploy time does not fall back to the password within
// milliseconds of itself (spec 3.2 rule 6).
func (am *AuthManager) jitterDeadline(deadline time.Time) time.Time {
	delta := int64(am.maxLifetime) / 10
	if delta <= 0 {
		return deadline
	}
	return deadline.Add(time.Duration(mathrand.Int64N(2*delta+1) - delta))
}
