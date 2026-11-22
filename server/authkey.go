package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/hkdf"
)

// File names for the two pieces of auth state that live beside the cluster
// token. Both are 0600 and both are read at startup; a missing file is a
// first start, a corrupt one is a startup error (spec 11).
const (
	authKeyFile = "auth.token.key"
	epochsFile  = "auth.token.epochs"

	// rootPasswordFile is where older versions kept the
	// generated root password in plain text. It is never written now: a start
	// that finds one converts it (the hash goes to users.json) and deletes it.
	rootPasswordFile = "root.password"
)

// HKDF parameters. The salt and info are frozen: changing them after tokens
// are in the field invalidates every outstanding token (spec 3.5).
const (
	authKeySalt     = "hoarddb"
	authKeyInfoBase = "auth-token-v1:"
	authKeyLen      = 32
)

// Pepper HKDF parameters. These two strings are frozen once a users.json has
// shipped: changing either re-derives every pepper and stops every stored
// password verifying (spec 6.2.1).
const (
	pepperHKDFSalt = "hoarddb-auth-pepper-v1"
	pepperInfoBase = "auth-pepper-v1:"
	pepperLen      = 32
)

// SigningKey is one HMAC-SHA256 signing key generation. ID is the `kid`
// carried in token claims so a validator selects a key by ID rather than
// trying every key (spec 3.4 requirement 6).
type SigningKey struct {
	ID    string
	Bytes []byte
}

// newSigningKey derives the `kid` (8 hex chars of sha256(key)) and wraps the
// key material. The bytes are never logged.
func newSigningKey(keyBytes []byte) SigningKey {
	sum := sha256.Sum256(keyBytes)
	return SigningKey{
		ID:    hex.EncodeToString(sum[:])[:8],
		Bytes: keyBytes,
	}
}

// keyEntry is one key generation and when it was added to the ring. AddedAt
// is the rotation time for a previous key, so the window measures how long a
// token signed just before a rotation keeps validating.
type keyEntry struct {
	Key     SigningKey
	AddedAt time.Time
}

// Keyring holds at most two signing keys — the current one and the
// immediately previous one — while every stored password is written under a
// key no record references. Validation selects by `kid`; there is no
// try-every-key fallback (spec 3.4 requirement 6).
//
// Pruning is lazy — Current and KeyByID compare time.Now() against
// AddedAt+window on read — so there is no ticker and no goroutine. The
// mutex is required because pruning mutates the ring while concurrent
// validations read it.
//
// A key named by a stored credential survives pruning however old it is:
// dropping it is not a failed token, it is a user who can never authenticate
// again (spec 6.2.1). keyInUse reports how many credentials name a kid; a nil
// callback is treated as "unknown", which keeps every key rather than
// guessing a key is unused.
type Keyring struct {
	mu       sync.Mutex
	entries  []keyEntry
	window   time.Duration
	keyInUse func(kid string) int
	logger   *slog.Logger

	// current mirrors entries[last] as a lock-free snapshot for the write
	// path. usersConfig.Save must pick the encryption key without taking
	// keyring.mu: it runs under the AuthManager's lock, and taking keyring.mu
	// there inverts the established lock order (a concurrent VerifyUser holds
	// keyring.mu while pruning and blocks on am.mu for the keyInUse callback).
	// The write path never prunes, so it reads the mirrored current key only.
	current atomic.Value // SigningKey
}

// currentKeyLocked mirrors the newest key for Save. Caller holds k.mu.
func (k *Keyring) currentKeyLocked() {
	if len(k.entries) == 0 {
		return
	}
	k.current.Store(k.entries[len(k.entries)-1].Key)
}

// SetKeyInUse installs the callback pruneLocked consults before dropping a
// key. It must not call back into the keyring (no keyring methods), or the
// lock re-enters.
func (k *Keyring) SetKeyInUse(fn func(kid string) int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.keyInUse = fn
}

// Current returns the key new tokens are signed with, after dropping any
// previous key whose overlap window has expired. The last entry is never
// pruned: it is the key the node is currently signing with.
func (k *Keyring) Current() SigningKey {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pruneLocked()
	if len(k.entries) == 0 {
		return SigningKey{}
	}
	return k.entries[len(k.entries)-1].Key
}

// currentNoPrune returns the key Save encrypts a users file under: the same
// key Current returns — the last entry is never pruned — read without the
// keyring mutex. Save runs under am.mu on the persistence and push-accept
// paths, and taking keyring.mu there would invert the lock order against a
// concurrent prune (which calls back into am.mu via the keyInUse callback).
// The mirrored snapshot is maintained by add and BuildKeyring.
func (k *Keyring) currentNoPrune() SigningKey {
	if v := k.current.Load(); v != nil {
		return v.(SigningKey)
	}
	return SigningKey{}
}

// KeyByID returns the key with the given `kid`. An unknown kid is a miss;
// it never falls back to trying every key.
func (k *Keyring) KeyByID(id string) (SigningKey, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pruneLocked()
	for i := len(k.entries) - 1; i >= 0; i-- {
		if k.entries[i].Key.ID == id {
			return k.entries[i].Key, true
		}
	}
	return SigningKey{}, false
}

// add installs a new current key, then prunes. The ring keeps the current key
// and the previous one while their window is open, and additionally keeps any
// older key a stored credential still names, so a rotation can never lock a
// user out (spec 6.2.1).
func (k *Keyring) add(key SigningKey, at time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.entries = append(k.entries, keyEntry{Key: key, AddedAt: at})
	k.currentKeyLocked()
	retained := k.pruneLocked()
	if retained > 0 && k.logger != nil {
		k.logger.Warn("auth signing keys kept because stored passwords still reference them",
			"referenced_keys", retained,
			"fix", "have those users change their passwords, or keep the previous key material")
	}
}

// pruneLocked drops previous keys that are both past their overlap window and
// referenced by no stored credential. It returns the number of keys retained
// only because a credential references them. Caller holds k.mu.
//
// A nil keyInUse is "unknown": every key is retained, because a pruned
// pepper is a user who can never authenticate again, and guessing the other
// way is irreversible.
func (k *Keyring) pruneLocked() int {
	if len(k.entries) <= 1 {
		return 0
	}
	now := time.Now()
	current := k.entries[len(k.entries)-1]
	previous := k.entries[:len(k.entries)-1]
	survivors := make([]keyEntry, 0, len(k.entries))
	referenced := 0
	for i, entry := range previous {
		count := -1
		if k.keyInUse != nil {
			count = k.keyInUse(entry.Key.ID)
		}
		inUse := count != 0 // -1 (unknown) or >0 both mean "keep"
		windowOpen := k.window > 0 && now.Before(entry.AddedAt.Add(k.window))
		lastPrevious := i == len(previous)-1
		if inUse || (lastPrevious && windowOpen) {
			survivors = append(survivors, entry)
			if inUse && count > 0 && !windowOpen {
				referenced++
			}
		}
	}
	survivors = append(survivors, current)
	k.entries = survivors
	return referenced
}

// LoadOrCreateAuthKey returns the standalone node's signing key from
// {dataDir}/auth.token.key, on a node that does not seal its credential files.
func LoadOrCreateAuthKey(dataDir string) ([]byte, error) {
	return CredentialKeys{}.loadOrCreateAuthKey(dataDir)
}

// loadOrCreateAuthKey returns the standalone node's signing key from
// {dataDir}/auth.token.key, opening it under the node master when it is
// sealed. A missing file is a first start and is created with 32 random
// bytes at 0600, sealed when k seals the node's credentials. An existing file
// that does not hold exactly 32 bytes is corrupt, and one that does not open
// is refused: silently regenerating either would invalidate every
// outstanding token while looking like a normal boot.
func (k CredentialKeys) loadOrCreateAuthKey(dataDir string) ([]byte, error) {
	path := authKeyCredential.path(dataDir)
	key, _, err := k.readCredential(dataDir, authKeyCredential)
	if err == nil {
		if len(key) != authKeyLen {
			return nil, fmt.Errorf("auth signing key %s is corrupt: got %d bytes, want %d; restore it from backup, or delete it to generate a new key (this invalidates all outstanding tokens)", path, len(key), authKeyLen)
		}
		return key, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read auth signing key: %w", err)
	}

	key = make([]byte, authKeyLen)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate auth signing key: %w", err)
	}
	if err := k.writeCredential(dataDir, authKeyCredential, key); err != nil {
		return nil, fmt.Errorf("write auth signing key: %w", err)
	}
	return key, nil
}

// RootPasswordFilePath is the plaintext root password file an older version
// left in dataDir. Only the upgrade conversion and -reset-password look at it.
func RootPasswordFilePath(dataDir string) string {
	return filepath.Join(dataDir, rootPasswordFile)
}

// ReadLegacyRootPassword returns the root password an older version saved to
// {dataDir}/root.password, so an upgrade can seed or keep it as a hash before
// the file is deleted. found is false when there is no file. An empty file is
// an error rather than a silent new credential: it only exists when the old
// write was interrupted, and it holds nothing to convert.
func ReadLegacyRootPassword(dataDir string) (password string, found bool, err error) {
	path := RootPasswordFilePath(dataDir)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read root password file %s: %w", path, err)
	}
	password = strings.TrimSpace(string(data))
	if password == "" {
		return "", false, fmt.Errorf("root password file %s is empty, which happens only when its write was interrupted; it holds no usable credential. Set HOARDDB_ROOT_PASSWORD and restart to provide the root password directly, bypassing this file", path)
	}
	return password, true, nil
}

// RetireRootPasswordFile removes the root.password an older version left in
// dataDir once users.json holds the root credential: the plaintext must not
// outlive the conversion to a hash. A missing file is not an error: most
// nodes never had one, or have none left to retire.
//
// It runs at startup before the listeners open, or from -reset-password
// after CheckServerStopped, so nothing reads the file concurrently.
func RetireRootPasswordFile(dataDir string) error {
	path := RootPasswordFilePath(dataDir)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove root password file %s: %w", path, err)
	}
	return nil
}

// PepperForKey derives the per-kid password pepper from an existing signing
// key's bytes (spec 6.2.1). The key never leaves the process and the derived
// pepper is never persisted: it exists so a stolen users.json is inert
// without the key material. The parameters are frozen once records exist.
func PepperForKey(key SigningKey, deploymentID string) []byte {
	reader := hkdf.New(sha256.New, key.Bytes,
		[]byte(pepperHKDFSalt), []byte(pepperInfoBase+deploymentID))
	pepper := make([]byte, pepperLen)
	// hkdf.Reader over SHA-256 cannot fail for 32 bytes; there is no
	// recoverable error to propagate.
	_, _ = io.ReadFull(reader, pepper)
	return pepper
}

// PepperHash applies the pepper to a client-derived Argon2 hash. HMAC, not a
// second Argon2: the work factor was already paid by the client, and a pepper
// is a secret rather than a stretch parameter (spec 6.2.1).
func PepperHash(pepper, clientHash []byte) []byte {
	mac := hmac.New(sha256.New, pepper)
	mac.Write(clientHash)
	return mac.Sum(nil)
}

// DeriveAuthKey derives a cluster-wide signing key from the cluster token
// with HKDF-SHA256. The info string includes the deployment identifier so two
// deployments that share a cluster token do not trust each other's tokens.
// The parameters are frozen once tokens are in the field.
func DeriveAuthKey(clusterToken, deploymentID string) []byte {
	reader := hkdf.New(sha256.New, []byte(clusterToken),
		[]byte(authKeySalt), []byte(authKeyInfoBase+deploymentID))
	key := make([]byte, authKeyLen)
	// hkdf.Reader over SHA-256 cannot fail for 32 bytes; there is no
	// recoverable error to propagate.
	_, _ = io.ReadFull(reader, key)
	return key
}

// BuildKeyring builds the signing keyring. Precedence is the whole point: if
// a cluster token is present the derived key is used and auth.token.key is
// not loaded at all — not current, not previous. The ignored file is named in
// one log line so an operator who joined a standalone node to a cluster is
// not left guessing why tokens stopped validating. The stale file is left on
// disk (deleting a key file during a transition turns a recoverable state
// into an unrecoverable one).
func BuildKeyring(dataDir, clusterToken, deploymentID string, window time.Duration, logger *slog.Logger) (*Keyring, error) {
	return buildKeyring(keyringRequest{
		dataDir: dataDir, clusterToken: clusterToken, deploymentID: deploymentID, window: window, logger: logger,
	})
}

// keyringRequest is what buildKeyring builds the signing keyring from.
type keyringRequest struct {
	dataDir      string
	clusterToken string
	deploymentID string
	window       time.Duration
	// credentials reads and creates auth.token.key, sealed when the node
	// seals its credential files.
	credentials CredentialKeys
	logger      *slog.Logger
}

// buildKeyring is BuildKeyring for a node whose credential files may be
// sealed.
func buildKeyring(req keyringRequest) (*Keyring, error) {
	keyPath := filepath.Join(req.dataDir, authKeyFile)
	logger := req.logger

	var keyBytes []byte
	var source string
	if req.clusterToken != "" {
		keyBytes = DeriveAuthKey(req.clusterToken, req.deploymentID)
		source = "derived from cluster token"
		if _, err := os.Stat(keyPath); err == nil {
			logger.Warn("auth signing key ignored: cluster token present, using derived signing key",
				"path", keyPath)
		}
	} else {
		var err error
		keyBytes, err = req.credentials.loadOrCreateAuthKey(req.dataDir)
		if err != nil {
			return nil, err
		}
		source = "loaded from " + keyPath
	}

	if len(keyBytes) == 0 {
		return nil, fmt.Errorf("auth signing key is empty (source: %s)", source)
	}

	key := newSigningKey(keyBytes)
	logger.Info("auth signing key ready", "kid", key.ID, "source", source)
	kr := &Keyring{
		entries: []keyEntry{{Key: key, AddedAt: time.Now()}},
		window:  req.window,
		logger:  logger,
	}
	kr.currentKeyLocked()
	return kr, nil
}

// EpochStore persists the per-user revocation counters. A restart must not
// roll revocation back: with a persisted signing key, an in-memory counter
// would return to zero and re-validate exactly the tokens it was bumped to
// revoke (spec 3.6).
//
// Every write is atomic (temp file + rename). A torn write that loses the
// last bump would revive the revoked tokens on the next start, silently.
type EpochStore struct {
	path   string
	mu     sync.Mutex
	epochs map[string]uint32
}

// LoadEpochStore reads {dataDir}/auth.token.epochs. A missing file is a
// first start; a corrupt one is an error, because starting with zeroed
// counters would silently un-revoke tokens.
func LoadEpochStore(dataDir string) (*EpochStore, error) {
	path := filepath.Join(dataDir, epochsFile)
	store := &EpochStore{path: path, epochs: make(map[string]uint32)}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read auth epoch store %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &store.epochs); err != nil {
		return nil, fmt.Errorf("parse auth epoch store %s: %w", path, err)
	}
	return store, nil
}

// Get returns the persisted epoch for a user; absent users are epoch 0.
func (e *EpochStore) Get(username string) uint32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.epochs[username]
}

// The write side of EpochStore is retired: the record's epoch is the only
// authority now, and this file is folded into users.json on load and never
// written again (spec 6.4). Deleting the file would turn a rollback into a
// silent security regression, so it stays on disk.
