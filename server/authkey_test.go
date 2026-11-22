package server

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadOrCreateAuthKeyCreates0600AndReuses(t *testing.T) {
	dir := t.TempDir()

	first, err := LoadOrCreateAuthKey(dir)
	if err != nil {
		t.Fatalf("first LoadOrCreateAuthKey: %v", err)
	}
	if len(first) != authKeyLen {
		t.Fatalf("generated key length = %d, want %d", len(first), authKeyLen)
	}

	path := filepath.Join(dir, authKeyFile)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("key file permissions = %o, want 600", perm)
	}

	second, err := LoadOrCreateAuthKey(dir)
	if err != nil {
		t.Fatalf("second LoadOrCreateAuthKey: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("second load returned a different key; the file must be reused, not regenerated")
	}
}

func TestLoadOrCreateAuthKeyTruncatedIsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, authKeyFile)
	if err := os.WriteFile(path, []byte("too short"), 0600); err != nil {
		t.Fatalf("write truncated key: %v", err)
	}

	_, err := LoadOrCreateAuthKey(dir)
	if err == nil {
		t.Fatal("truncated key file must fail the start, not regenerate")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error %q does not name the corrupt path %q", err, path)
	}
}

func TestDeriveAuthKeyIsDeterministicAndDeploymentSeparated(t *testing.T) {
	a := DeriveAuthKey("cluster-secret", "prod")
	b := DeriveAuthKey("cluster-secret", "prod")
	if !bytes.Equal(a, b) {
		t.Fatal("DeriveAuthKey is not deterministic")
	}
	if len(a) != authKeyLen {
		t.Fatalf("derived key length = %d, want %d", len(a), authKeyLen)
	}
	c := DeriveAuthKey("cluster-secret", "staging")
	if bytes.Equal(a, c) {
		t.Fatal("deployment id must separate derived keys")
	}
	if bytes.Equal(a, DeriveAuthKey("other-cluster", "prod")) {
		t.Fatal("different cluster tokens must derive different keys")
	}
}

func TestBuildKeyringUsesDerivedKeyAndIgnoresKeyFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadOrCreateAuthKey(dir); err != nil {
		t.Fatalf("create standalone key: %v", err)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	ring, err := BuildKeyring(dir, "cluster-secret", "prod", time.Minute, logger)
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	want := DeriveAuthKey("cluster-secret", "prod")
	if !bytes.Equal(ring.Current().Bytes, want) {
		t.Fatal("cluster token present: keyring must use the derived key, not auth.token.key")
	}
	if !strings.Contains(logBuf.String(), "ignored") {
		t.Fatalf("ignored key file was not named in the log; got %q", logBuf.String())
	}

	// The ignored file is left on disk, not deleted.
	if _, err := os.Stat(filepath.Join(dir, authKeyFile)); err != nil {
		t.Fatalf("auth.token.key was removed during the switch: %v", err)
	}
}

func TestKeyringPreviousKidWindow(t *testing.T) {
	previous := newSigningKey([]byte("previous-key-material-32-bytes!"))
	current := newSigningKey([]byte("current-key-material-32-bytes!!"))
	ring := &Keyring{
		window: time.Minute,
		entries: []keyEntry{
			{Key: previous, AddedAt: time.Now()},
			{Key: current, AddedAt: time.Now()},
		},
		keyInUse: func(string) int { return 0 },
	}

	if _, ok := ring.KeyByID(previous.ID); !ok {
		t.Fatal("previous kid inside the overlap window must validate")
	}
	if got, ok := ring.KeyByID(current.ID); !ok || got.ID != current.ID {
		t.Fatal("current kid must validate")
	}
	if _, ok := ring.KeyByID("deadbeef"); ok {
		t.Fatal("unknown kid must miss; there is no try-every-key fallback")
	}
	if got := ring.Current(); got.ID != current.ID {
		t.Fatalf("Current() = %s, want the newest key %s", got.ID, current.ID)
	}

	// window 0 means no overlap: a superseded key is retired immediately.
	ring.window = 0
	if _, ok := ring.KeyByID(previous.ID); ok {
		t.Fatal("previous kid must be refused once the overlap window is disabled")
	}
	if _, ok := ring.KeyByID(current.ID); !ok {
		t.Fatal("current kid must survive pruning")
	}
}

func TestKeyringHoldsAtMostTwoKeys(t *testing.T) {
	ring := &Keyring{window: time.Hour, keyInUse: func(string) int { return 0 }}
	for i := 0; i < 4; i++ {
		ring.add(newSigningKey([]byte{byte(i) + 1, 2, 3, 4}), time.Now())
	}
	if got := len(ring.entries); got != 2 {
		t.Fatalf("keyring holds %d keys, want at most 2", got)
	}
}

// TestKeyringPruneRefusesAReferencedKid is the invariant that, if wrong,
// locks users out permanently: a stored credential names a kid, the key's
// window has closed, and the prune must keep it anyway (spec 6.2.1).
func TestKeyringPruneRefusesAReferencedKid(t *testing.T) {
	referenced := newSigningKey([]byte("referenced-key-material-32-bytes"))
	unreferenced := newSigningKey([]byte("unreferenced-key-material-32-by"))
	current := newSigningKey([]byte("current-key-material-32-bytes!!!"))
	ring := &Keyring{
		window:   0, // no overlap: prune would drop both previous keys
		entries:  []keyEntry{{Key: referenced, AddedAt: time.Now()}, {Key: unreferenced, AddedAt: time.Now()}, {Key: current, AddedAt: time.Now()}},
		keyInUse: func(kid string) int { return 0 },
	}
	ring.keyInUse = func(kid string) int {
		if kid == referenced.ID {
			return 3
		}
		return 0
	}

	// A read path prunes lazily; the referenced key must survive and the
	// unreferenced one must go.
	if _, ok := ring.KeyByID(current.ID); !ok {
		t.Fatal("current kid must validate")
	}
	if _, ok := ring.KeyByID(referenced.ID); !ok {
		t.Fatal("a kid a stored credential names must never be pruned")
	}
	if _, ok := ring.KeyByID(unreferenced.ID); ok {
		t.Fatal("an unreferenced kid past its window must be pruned")
	}
}

// TestKeyringPruneRefusesWithNilStore proves a missing user store fails safe:
// the keyring cannot prove a kid is unreferenced, so it keeps it rather than
// guessing (spec 6.2.1, build-doc trap 4).
func TestKeyringPruneRefusesWithNilStore(t *testing.T) {
	previous := newSigningKey([]byte("previous-key-material-32-bytes!"))
	current := newSigningKey([]byte("current-key-material-32-bytes!!"))
	ring := &Keyring{
		window:  0,
		entries: []keyEntry{{Key: previous, AddedAt: time.Now()}, {Key: current, AddedAt: time.Now()}},
	}
	if _, ok := ring.KeyByID(previous.ID); !ok {
		t.Fatal("with no user store, pruning must not drop a possibly-referenced key")
	}
}

// TestReadLegacyRootPassword: the upgrade reads an older version's file,
// trimmed, and a missing file is "not found", not an error.
func TestReadLegacyRootPassword(t *testing.T) {
	dir := t.TempDir()
	if _, found, err := ReadLegacyRootPassword(dir); found || err != nil {
		t.Fatalf("no file: found=%v err=%v, want not found and no error", found, err)
	}
	if err := os.WriteFile(RootPasswordFilePath(dir), []byte("older-secret\n"), 0600); err != nil {
		t.Fatalf("write root.password: %v", err)
	}
	password, found, err := ReadLegacyRootPassword(dir)
	if err != nil || !found || password != "older-secret" {
		t.Fatalf("ReadLegacyRootPassword = %q, %v, %v; want the trimmed contents", password, found, err)
	}
}

// TestReadLegacyRootPasswordEmptyErrorNamesTheFix: an empty file is a corrupt
// first-run artifact (an older version's write was interrupted), not a
// credential to convert; the error says so and points at a real product
// action rather than a bare "delete it".
func TestReadLegacyRootPasswordEmptyErrorNamesTheFix(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(RootPasswordFilePath(dir), []byte("\n"), 0600); err != nil {
		t.Fatalf("write empty root password: %v", err)
	}
	_, _, err := ReadLegacyRootPassword(dir)
	if err == nil {
		t.Fatal("an empty root password file must fail, not silently start without a root credential")
	}
	if strings.Contains(err.Error(), "delete") {
		t.Fatalf("error %q tells the operator to delete the file", err)
	}
	if !strings.Contains(err.Error(), "HOARDDB_ROOT_PASSWORD") {
		t.Fatalf("error %q does not name a real product action for the corrupt file", err)
	}
}

// TestEpochStoreLoadsAndStaysReadOnly proves the retired side file is never
// written again: the file is made read-only and every read still works
// (spec 6.4).
func TestEpochStoreLoadsAndStaysReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, epochsFile)
	if err := os.WriteFile(path, []byte(`{"alice":7}`), 0600); err != nil {
		t.Fatalf("write epochs: %v", err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatalf("chmod epochs: %v", err)
	}

	store, err := LoadEpochStore(dir)
	if err != nil {
		t.Fatalf("LoadEpochStore: %v", err)
	}
	if got := store.Get("alice"); got != 7 {
		t.Fatalf("epoch = %d, want 7", got)
	}
	if got := store.Get("bob"); got != 0 {
		t.Fatalf("absent user epoch = %d, want 0", got)
	}

	// A write would fail on the read-only file, so a successful re-read of
	// the source proves nothing attempted one.
	reloaded, err := LoadEpochStore(dir)
	if err != nil {
		t.Fatalf("reload read-only epoch store: %v", err)
	}
	if got := reloaded.Get("alice"); got != 7 {
		t.Fatalf("epoch after reload = %d, want 7", got)
	}
}

func TestLoadEpochStoreCorruptIsError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, epochsFile), []byte("{not json"), 0600); err != nil {
		t.Fatalf("write corrupt epochs: %v", err)
	}
	if _, err := LoadEpochStore(dir); err == nil {
		t.Fatal("corrupt epoch store must fail the start, not silently zero the counters")
	}
}
