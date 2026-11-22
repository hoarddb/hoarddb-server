package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// ReadOnlyKV is a stopped node store opened for reading only. It offers snapshots and nothing
// that writes, and the store underneath is opened with Badger's read-only flag, which takes a
// shared directory lock and creates, repairs and truncates nothing. It exists for offline tools
// (faultctl digest) that must never change a node's directory; the node itself opens its store
// through OpenBadgerKV and nothing about that open changes.
type ReadOnlyKV struct {
	kv *BadgerKV
}

// ErrStoreSealed is under the refusal to open an encrypted store read-only: the tool has no key.
var ErrStoreSealed = errors.New("the store is encrypted at rest")

// OpenReadOnly opens the store in dir read-only. It refuses, naming the fix, a directory that is
// no store, an encrypted store (ErrStoreSealed), a store another process holds open, and a store
// whose logs need a repair that only a read-write open performs. It never retries read-write and
// never runs OpenBadgerKV's guards, one of which can repair a crashed first flush by writing.
func OpenReadOnly(dir string) (*ReadOnlyKV, error) {
	if _, err := os.Stat(filepath.Join(dir, badger.ManifestFilename)); err != nil {
		return nil, fmt.Errorf("%s is not a node store: no %s file (%v)", dir, badger.ManifestFilename, err)
	}
	if registry, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: dir, ReadOnly: true}); err != nil {
		if errors.Is(err, badger.ErrEncryptionKeyMismatch) {
			return nil, fmt.Errorf("opening %s read-only: %w, and no key is available to a read-only open", dir, ErrStoreSealed)
		}
	} else {
		_ = registry.Close() // read-only; nothing to flush
	}
	// Badger replays an unflushed log into an arena of the memtable size it is opened with and
	// exits the process when the log does not fit, so the size is the node's own rule for it.
	memtableBytes, err := previousMemtableBytes(dir)
	if err != nil {
		return nil, err
	}
	db, err := badger.Open(badger.DefaultOptions(dir).
		WithReadOnly(true).
		WithLogger(nil).
		WithValueLogFileSize(128 << 20).
		WithMemTableSize(memtableBytes).
		WithBlockCacheSize(64 << 20).
		WithIndexCacheSize(64 << 20).
		WithChecksumVerificationMode(options.OnBlockRead))
	if err != nil {
		return nil, explainReadOnlyFailure(dir, err)
	}
	kv, err := newBadgerKV(db, dir)
	if err != nil {
		return nil, closeAfterOpenFailure(db, err)
	}
	return &ReadOnlyKV{kv: kv}, nil
}

func explainReadOnlyFailure(dir string, err error) error {
	switch {
	case badgerChecksumMismatch(err):
		return &StoreDamagedError{Dir: dir, Damage: fmt.Sprintf("a file failed its checksum (%v)", err), Cause: err}
	case strings.Contains(err.Error(), "Cannot acquire directory lock"):
		return fmt.Errorf("the store at %s is in use: a node is probably still running on it; "+
			"stop the node cleanly (SIGTERM), then run this command again: %w", dir, err)
	case errors.Is(err, badger.ErrTruncateNeeded) || strings.Contains(err.Error(), "not properly closed"):
		return fmt.Errorf("the store at %s was not stopped cleanly, and Badger will not open it read-only (its logs need a repair that only a read-write open does). "+
			"Copy the directory first if it is evidence; then start the node on it once, wait until it is serving, "+
			"stop it with SIGTERM (not kill -9), and run this command again: %w", dir, err)
	default:
		return fmt.Errorf("opening the store at %s read-only: %w", dir, err)
	}
}

// NewSnapshot opens a consistent read view of the store.
func (readOnly *ReadOnlyKV) NewSnapshot() KVSnapshot { return readOnly.kv.NewSnapshot() }

// Close releases the store. Every snapshot and iterator must be closed first.
func (readOnly *ReadOnlyKV) Close() error { return readOnly.kv.Close() }
