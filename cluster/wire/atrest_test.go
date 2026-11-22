package wire

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/hoarddb/hoarddb-server/storage"
)

// recordingAtRest is an AtRestKeys that records when the node asks and what the store and log
// directories held at that moment.
type recordingAtRest struct {
	t         *testing.T
	storeKey  []byte
	logKeys   storage.WALKeySource
	storeAsks []string
	logAsks   []string
}

func (r *recordingAtRest) StoreKey(storeDir string) ([]byte, error) {
	if entries, err := os.ReadDir(storeDir); err == nil && len(entries) > 0 {
		r.t.Errorf("the store key was asked for after %s held %d entries: Badger created a file before it had the key", storeDir, len(entries))
	}
	r.storeAsks = append(r.storeAsks, storeDir)
	return r.storeKey, nil
}

func (r *recordingAtRest) LogKeys(logDir string) (storage.WALKeySource, error) {
	if entries, err := os.ReadDir(logDir); err == nil && len(entries) > 0 {
		r.t.Errorf("the log keys were asked for after %s held %d entries: the log created a file before it had its keys", logDir, len(entries))
	}
	r.logAsks = append(r.logAsks, logDir)
	return r.logKeys, nil
}

// fixedLogKeys is a one-epoch key source with a fixed key for every segment.
type fixedLogKeys struct{ key []byte }

func (f fixedLogKeys) CurrentEpoch() uint64 { return 1 }

func (f fixedLogKeys) FileKey(epoch uint64, _ int) ([]byte, error) {
	if epoch != 1 {
		return nil, errors.New("no such epoch")
	}
	return f.key, nil
}

// TestNodeOpenAsksForTheStoreKeyBeforeBadgerCreatesAFile: [GUARD the node asks AtRest for the
// store key before Badger creates a file and opens the store with it; red when the call is
// removed (the store's key registry then opens with an empty key) or moved after the store
// open (the recorder then sees Badger's files)]. Ring encryption at rest spec §9.1 step 1.
func TestNodeOpenAsksForTheStoreKeyBeforeBadgerCreatesAFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	keys := &recordingAtRest{t: t, storeKey: bytes.Repeat([]byte{0x5A}, 32), logKeys: fixedLogKeys{key: bytes.Repeat([]byte{0x6B}, 32)}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: dir, AtRest: keys})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("closing the node: %v", err)
	}
	if len(keys.storeAsks) != 1 || keys.storeAsks[0] != filepath.Join(dir, "badger") {
		t.Fatalf("the node asked for the store key %v, want once for %s", keys.storeAsks, filepath.Join(dir, "badger"))
	}
	if len(keys.logAsks) != 1 || keys.logAsks[0] != filepath.Join(dir, "wal") {
		t.Fatalf("the node asked for the log keys %v, want once for %s", keys.logAsks, filepath.Join(dir, "wal"))
	}
	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: filepath.Join(dir, "badger"), ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Fatalf("the store's key registry opens with an empty key (%v): the store was not sealed under the key AtRest returned", err)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "wal", "wal_*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("no log segment under %s (%v)", filepath.Join(dir, "wal"), err)
	}
	header := make([]byte, 24)
	segment, err := os.Open(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = segment.Close() }()
	if _, err := segment.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	if !storage.IsSealedWALSegmentHeader(header) {
		t.Fatalf("%s is not sealed: the log did not open with the key source AtRest returned", segments[0])
	}
}

// countingAtRest hands fixed keys and counts the asks.
type countingAtRest struct {
	storeKey           []byte
	logKeys            storage.WALKeySource
	storeAsks, logAsks int
}

func (c *countingAtRest) StoreKey(string) ([]byte, error) {
	c.storeAsks++
	return c.storeKey, nil
}

func (c *countingAtRest) LogKeys(string) (storage.WALKeySource, error) {
	c.logAsks++
	return c.logKeys, nil
}

// TestASingleNodeRebuildsItsSealedStoreFromItsSealedLog: [GUARD a single node whose sealed store
// was moved aside rebuilds it from its sealed log: the first-start check opens the log with the
// node's log keys and the boot replay reads the document back, asking AtRest once for each key;
// red when Node.openLog opens the log without its keys (the sealed log does not open as plaintext)].
// Integration review F-5 (ii).
func TestASingleNodeRebuildsItsSealedStoreFromItsSealedLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ring")
	storeKey, logKeys := bytes.Repeat([]byte{0x5A}, 32), fixedLogKeys{key: bytes.Repeat([]byte{0x6B}, 32)}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first, err := OpenNode(ctx, NodeConfig{Dir: dir, AtRest: &countingAtRest{storeKey: storeKey, logKeys: logKeys}})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	u30iSchema(t, first)
	if _, err := first.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("closing the node: %v", err)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "wal", "wal_*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("no log segment under %s (%v)", filepath.Join(dir, "wal"), err)
	}
	header := make([]byte, 24)
	segment, err := os.Open(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := segment.ReadAt(header, 0)
	if closeErr := segment.Close(); readErr != nil || closeErr != nil {
		t.Fatalf("reading %s: %v %v", segments[0], readErr, closeErr)
	}
	if !storage.IsSealedWALSegmentHeader(header) {
		t.Fatalf("SCENARIO NOT REACHED: %s is not sealed", segments[0])
	}
	moveAside(t, dir, "badger")

	keys := &countingAtRest{storeKey: storeKey, logKeys: logKeys}
	rebuilt, err := OpenNode(ctx, NodeConfig{Dir: dir, AtRest: keys})
	if err != nil {
		t.Fatalf("the node did not rebuild its sealed store from its sealed log: %v", err)
	}
	t.Cleanup(func() {
		if err := rebuilt.Close(); err != nil {
			t.Errorf("closing the rebuilt node: %v", err)
		}
	})
	if !rebuilt.kv.VouchedOpenReport().FirstStart {
		t.Fatal("SCENARIO NOT REACHED: the store's open was not a first start")
	}
	if _, err := rebuilt.Get(ctx, u30iDB, "User", "u1"); err != nil {
		t.Fatalf("the document is not back after the rebuild from the sealed log: %v", err)
	}
	if keys.storeAsks != 1 || keys.logAsks != 1 {
		t.Fatalf("the rebuilding start asked for the store key %d times and the log keys %d times, want once each", keys.storeAsks, keys.logAsks)
	}
	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: filepath.Join(dir, "badger"), ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Fatalf("the rebuilt store's key registry opens with an empty key (%v): it was rebuilt in plaintext", err)
	}
}
