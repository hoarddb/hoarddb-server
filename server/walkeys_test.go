package server

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// TestWALKeySourceResolveSegmentIndex tests that a segment index is resolved to a stable 32-byte key.
func TestWALKeySourceResolveSegmentIndex(t *testing.T) {
	dataDir := t.TempDir()

	// Create a keystore with a test KEK
	kek := make([]byte, 32)
	for i := 0; i < 32; i++ {
		kek[i] = byte(i)
	}
	ks := &Keystore{
		kek:  kek,
		kid:  sealed.Kid(kek),
		mode: "keyfile",
	}

	nm, err := LoadOrCreateNodeMaster(dataDir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	// Load or create the WAL key source
	keySource, err := LoadOrCreateWALKeySource(dataDir, nm)
	if err != nil {
		t.Fatalf("LoadOrCreateWALKeySource: %v", err)
	}

	// Get a key for a specific segment
	epoch := keySource.CurrentEpoch()
	key1, err := keySource.FileKey(epoch, 5)
	if err != nil {
		t.Fatalf("FileKey(epoch=%d, segIdx=5): %v", epoch, err)
	}

	if len(key1) != 32 {
		t.Errorf("key length: got %d, want 32", len(key1))
	}

	// Same segment should give the same key
	key2, err := keySource.FileKey(epoch, 5)
	if err != nil {
		t.Fatalf("FileKey(epoch=%d, segIdx=5) second time: %v", epoch, err)
	}

	if len(key2) != 32 {
		t.Errorf("key2 length: got %d, want 32", len(key2))
	}

	// Keys must be identical
	for i := 0; i < 32; i++ {
		if key1[i] != key2[i] {
			t.Errorf("key mismatch at byte %d: first=%02x, second=%02x", i, key1[i], key2[i])
		}
	}

	// Different segment should give a different key
	key3, err := keySource.FileKey(epoch, 7)
	if err != nil {
		t.Fatalf("FileKey(epoch=%d, segIdx=7): %v", epoch, err)
	}

	if len(key3) != 32 {
		t.Errorf("key3 length: got %d, want 32", len(key3))
	}

	// key3 must be different from key1
	same := true
	for i := 0; i < 32; i++ {
		if key1[i] != key3[i] {
			same = false
			break
		}
	}
	if same {
		t.Errorf("key for segment 7 must be different from key for segment 5, but they are identical")
	}
}

// TestWALKeySourceCurrentEpoch tests that CurrentEpoch returns the epoch ID.
func TestWALKeySourceCurrentEpoch(t *testing.T) {
	dataDir := t.TempDir()

	// Create a keystore with a test KEK
	kek := make([]byte, 32)
	for i := 0; i < 32; i++ {
		kek[i] = byte(i)
	}
	ks := &Keystore{
		kek:  kek,
		kid:  sealed.Kid(kek),
		mode: "keyfile",
	}

	nm, err := LoadOrCreateNodeMaster(dataDir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	// Load or create the WAL key source
	keySource, err := LoadOrCreateWALKeySource(dataDir, nm)
	if err != nil {
		t.Fatalf("LoadOrCreateWALKeySource: %v", err)
	}

	epoch := keySource.CurrentEpoch()
	if epoch != 1 {
		t.Errorf("CurrentEpoch: got %d, want 1", epoch)
	}
}

// TestWALKeySourceMissingEpochRefusal tests that a missing epoch is refused clearly.
func TestWALKeySourceMissingEpochRefusal(t *testing.T) {
	dataDir := t.TempDir()

	// Create a keystore with a test KEK
	kek := make([]byte, 32)
	for i := 0; i < 32; i++ {
		kek[i] = byte(i)
	}
	ks := &Keystore{
		kek:  kek,
		kid:  sealed.Kid(kek),
		mode: "keyfile",
	}

	nm, err := LoadOrCreateNodeMaster(dataDir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	// Load or create the WAL key source
	keySource, err := LoadOrCreateWALKeySource(dataDir, nm)
	if err != nil {
		t.Fatalf("LoadOrCreateWALKeySource: %v", err)
	}

	// Try to get a key for a non-existent epoch
	_, err = keySource.FileKey(999, 0)
	if err == nil {
		t.Errorf("FileKey for missing epoch: expected error, got nil")
	}

	// Error message must name the epoch
	errStr := err.Error()
	if !contains(errStr, "999") {
		t.Errorf("error must name the missing epoch (999), got: %s", errStr)
	}
}

// TestWALKeySourceLoadsExistingRegistry tests that an existing registry can be loaded.
func TestWALKeySourceLoadsExistingRegistry(t *testing.T) {
	dataDir := t.TempDir()

	// Create a keystore with a test KEK
	kek := make([]byte, 32)
	for i := 0; i < 32; i++ {
		kek[i] = byte(i)
	}
	ks := &Keystore{
		kek:  kek,
		kid:  sealed.Kid(kek),
		mode: "keyfile",
	}

	nm, err := LoadOrCreateNodeMaster(dataDir, ks)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster: %v", err)
	}

	// Load or create the WAL key source (creates registry)
	keySource1, err := LoadOrCreateWALKeySource(dataDir, nm)
	if err != nil {
		t.Fatalf("LoadOrCreateWALKeySource: %v", err)
	}

	key1, err := keySource1.FileKey(keySource1.CurrentEpoch(), 42)
	if err != nil {
		t.Fatalf("FileKey: %v", err)
	}

	// Load the WAL key source again (should load existing registry)
	keySource2, err := LoadOrCreateWALKeySource(dataDir, nm)
	if err != nil {
		t.Fatalf("LoadOrCreateWALKeySource second time: %v", err)
	}

	key2, err := keySource2.FileKey(keySource2.CurrentEpoch(), 42)
	if err != nil {
		t.Fatalf("FileKey second time: %v", err)
	}

	// Keys must be identical (same registry, same epoch, same segment)
	if len(key1) != len(key2) {
		t.Errorf("key length mismatch: %d vs %d", len(key1), len(key2))
	}
	for i := 0; i < len(key1); i++ {
		if key1[i] != key2[i] {
			t.Errorf("key mismatch at byte %d", i)
		}
	}
}

// TestWALKeySourceNilNodeMasterReturnsNil tests that a nil node master returns nil key source.
func TestWALKeySourceNilNodeMasterReturnsNil(t *testing.T) {
	dataDir := t.TempDir()

	keySource, err := LoadOrCreateWALKeySource(dataDir, nil)
	if err != nil {
		t.Fatalf("LoadOrCreateWALKeySource with nil node master: expected nil error, got %v", err)
	}

	if keySource != nil {
		t.Errorf("LoadOrCreateWALKeySource with nil node master: expected nil, got %T", keySource)
	}
}

// TestWALKeySourceImplementsInterface tests that walKeySource implements storage.WALKeySource.
func TestWALKeySourceImplementsInterface(t *testing.T) {
	var _ storage.WALKeySource = (*walKeySource)(nil)
}

// walWiringMarker is the body both WAL wiring tests append: the sealed test
// asserts it is absent from the segment, the plaintext test that it is present,
// so the absence cannot be vacuous (recheck).
const walWiringMarker = "wiringmarker_4c7e91af"

// TestNewServiceSealsTheWAL is the difference between being able to seal the
// WAL and sealing it on a running server. Every test above builds a key source
// by hand; this one asserts NewService installs one, by appending through the
// WAL it opened and finding no plaintext in the file.
func TestNewServiceSealsTheWAL(t *testing.T) {
	const marker = walWiringMarker

	dataDir := newClusteredTestDataDir(t)
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(i)
	}
	// The keystore refuses a key file inside the data directory, so it lives
	// in a directory of its own.
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, kek, 0o600); err != nil {
		t.Fatalf("writing the test KEK: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	// The WAL exists only where replication can happen (trap 4): a standalone
	// node would open none and this test would assert nothing.
	cfg.Server.ReplicationFactor = 2
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keyPath
	cfg.Encryption.Mode = "on"

	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if srv.core.wal == nil {
		t.Fatal("NewService opened no WAL, so this test proves nothing")
	}

	if _, err := srv.core.wal.Append("wiring", storage.OpInsert, []byte(marker), []byte(marker)); err != nil {
		t.Fatalf("Append through the server's WAL: %v", err)
	}
	if err := srv.core.wal.Close(); err != nil {
		t.Fatalf("closing the server's WAL: %v", err)
	}

	// The epoch registry was created under the node master.
	registryPath := filepath.Join(dataDir, "wal", WALEpochRegistryFileName)
	if _, err := os.Stat(registryPath); err != nil {
		t.Fatalf("no WAL epoch registry at %s: %v", registryPath, err)
	}

	segmentBytes, err := os.ReadFile(filepath.Join(dataDir, "wal", "wal_00001.log"))
	if err != nil {
		t.Fatalf("reading the server's WAL segment: %v", err)
	}
	// In segment format 2 a plain segment has a header with the same first six
	// bytes, so only the header's sealed flag says the key source was used.
	if !storage.IsSealedWALSegmentHeader(segmentBytes) {
		t.Error("the server's WAL segment has no sealed header: the key source was not installed")
	}
	if bytes.Contains(segmentBytes, []byte(marker)) {
		t.Error("the marker appears in the server's WAL segment: entries are reaching disk in the clear")
	}
}

// TestNewServiceLeavesTheWALPlaintextWithoutAKeystore is the other half: with
// keystore=off there is no node master, so the WAL's entries stay in
// plaintext: a format 2 segment whose header says plain.
func TestNewServiceLeavesTheWALPlaintextWithoutAKeystore(t *testing.T) {
	dataDir := newClusteredTestDataDir(t)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Server.ReplicationFactor = 2

	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if srv.core.wal == nil {
		t.Fatal("NewService opened no WAL, so this test proves nothing")
	}
	if _, err := srv.core.wal.Append("plain", storage.OpInsert, []byte(walWiringMarker), []byte(walWiringMarker)); err != nil {
		t.Fatalf("Append through the server's WAL: %v", err)
	}
	if err := srv.core.wal.Close(); err != nil {
		t.Fatalf("closing the server's WAL: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dataDir, "wal", WALEpochRegistryFileName)); !os.IsNotExist(err) {
		t.Errorf("an epoch registry was created with keystore=off: %v", err)
	}
	segmentBytes, err := os.ReadFile(filepath.Join(dataDir, "wal", "wal_00001.log"))
	if err != nil {
		t.Fatalf("reading the server's WAL segment: %v", err)
	}
	// A plain segment in format 2 has a header too; its flags say plain.
	if storage.IsSealedWALSegmentHeader(segmentBytes) {
		t.Error("the WAL was sealed with no keystore configured")
	}
	// The positive control for TestNewServiceSealsTheWAL: a plaintext segment
	// shows the marker to a byte scan. WAL segments are not compressed
	// (storage/wal*.go has no compressor), so an interior window is enough and
	// is what the Badger-side scan uses too.
	if !bytes.Contains(segmentBytes, []byte(walWiringMarker[2:len(walWiringMarker)-2])) {
		t.Error("the marker is not in a plaintext WAL segment: the sealed test's absence check would prove nothing")
	}
}

// newClusteredTestDataDir is a data directory a clustered NewService will
// accept: the WAL only exists where replication can happen, and a replicating
// node needs its cluster token before NewCluster will build.
func newClusteredTestDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := SaveClusterToken(dir, ClusterToken{
		Token:     "wal-wiring-test-cluster-token",
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	return dir
}
