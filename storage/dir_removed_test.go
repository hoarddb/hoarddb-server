package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// pastDirCheckInterval is longer than the interval at which the store and the
// WAL re-check that their directory is still the one they opened, so the
// first write after it is checked.
const pastDirCheckInterval = 400 * time.Millisecond

// TestAStoreWhoseDirectoryIsGoneRefusesTheNextWrite defends BUG-0390: a write
// to the open descriptors of a removed (or replaced) store directory succeeds
// and is lost at the next restart, because nothing on disk has its name any
// more. The store must refuse the write, say why in WriteRefusal, and keep
// refusing.
func TestAStoreWhoseDirectoryIsGoneRefusesTheNextWrite(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		breakDir func(t *testing.T, storeDir string)
	}{
		{"removed", func(t *testing.T, storeDir string) {
			if err := os.RemoveAll(storeDir); err != nil {
				t.Fatalf("removing the store directory: %v", err)
			}
		}},
		{"replaced", func(t *testing.T, storeDir string) {
			if err := os.Rename(storeDir, storeDir+".moved"); err != nil {
				t.Fatalf("moving the store directory away: %v", err)
			}
			if err := os.Mkdir(storeDir, 0o700); err != nil {
				t.Fatalf("creating an empty directory in its place: %v", err)
			}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			storeDir := filepath.Join(t.TempDir(), "store")
			kv, err := OpenBadgerKV(storeDir, BadgerTuning{MemTableMB: 32}, nil)
			if err != nil {
				t.Fatalf("OpenBadgerKV: %v", err)
			}
			t.Cleanup(func() { logClose(t, "the store", kv.Close()) })
			if err := commitOneKey(kv, "before"); err != nil {
				t.Fatalf("a write before the directory changed: %v", err)
			}

			scenario.breakDir(t, storeDir)
			time.Sleep(pastDirCheckInterval)

			writeErr := commitOneKey(kv, "after")
			if writeErr == nil {
				t.Fatalf("a write after the store directory was %s was acknowledged; it is lost at the next restart", scenario.name)
			}
			var refused *StoreRefusesWritesError
			if !errors.As(writeErr, &refused) {
				t.Fatalf("the write was refused with %T, not a StoreRefusesWritesError: %v", writeErr, writeErr)
			}
			for _, wanted := range []string{storeDir, "removed or replaced", "restart"} {
				if !strings.Contains(writeErr.Error(), wanted) {
					t.Errorf("the refusal does not name %q: %v", wanted, writeErr)
				}
			}
			if refusal := kv.WriteRefusal(); refusal == nil || !strings.Contains(refusal.Error(), storeDir) {
				t.Errorf("WriteRefusal = %v, want the refusal naming %s", refusal, storeDir)
			}
			if err := commitOneKey(kv, "again"); err == nil {
				t.Errorf("a second write after the directory was %s was acknowledged", scenario.name)
			}
		})
	}
}

// TestAStoreReportsAGoneDirectoryWithoutAWrite: a node that takes no writes
// still says, when asked (HEALTH_CHECK, /ready), that its directory is gone.
func TestAStoreReportsAGoneDirectoryWithoutAWrite(t *testing.T) {
	storeDir := filepath.Join(t.TempDir(), "store")
	kv, err := OpenBadgerKV(storeDir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	t.Cleanup(func() { logClose(t, "the store", kv.Close()) })
	if refusal := kv.WriteRefusal(); refusal != nil {
		t.Fatalf("a fresh store refuses writes: %v", refusal)
	}
	if err := os.RemoveAll(storeDir); err != nil {
		t.Fatalf("removing the store directory: %v", err)
	}
	time.Sleep(pastDirCheckInterval)
	if refusal := kv.WriteRefusal(); refusal == nil {
		t.Fatalf("WriteRefusal is nil for a store whose directory was removed")
	}
}

// TestAWALWhoseDirectoryIsGoneRefusesTheNextAppend is BUG-0390 for the
// replication log: an append to a removed log directory was fsynced to an
// unlinked segment and acknowledged. AppendUnsyncedFrom is refused the same
// way: its entry is acknowledged once SyncThrough returns, and SyncThrough
// would flush the unlinked segment (spec §4.3: it checks WriteRefusal as
// Append does).
func TestAWALWhoseDirectoryIsGoneRefusesTheNextAppend(t *testing.T) {
	appends := map[string]func(w *WAL) error{
		"Append": func(w *WAL) error {
			_, err := w.Append("events", 1, []byte("after"), []byte("x"))
			return err
		},
		"AppendUnsyncedFrom": func(w *WAL) error {
			_, err := w.AppendUnsyncedFrom("events", 1, []byte("after"), []byte("x"), WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: 1})
			return err
		},
	}
	for name, appendAfter := range appends {
		t.Run(name, func(t *testing.T) {
			walDir := filepath.Join(t.TempDir(), "wal")
			w, err := OpenWAL(walDir, nil)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			t.Cleanup(func() { logClose(t, "the WAL", w.Close()) })
			if _, err := w.Append("events", 1, []byte("before"), []byte("x")); err != nil {
				t.Fatalf("an append before the directory was removed: %v", err)
			}
			if err := os.RemoveAll(walDir); err != nil {
				t.Fatalf("removing the WAL directory: %v", err)
			}
			time.Sleep(pastDirCheckInterval)
			appendErr := appendAfter(w)
			if appendErr == nil {
				t.Fatalf("an append after the WAL directory was removed was accepted; it is lost at the next restart")
			}
			for _, wanted := range []string{walDir, "removed or replaced", "restart"} {
				if !strings.Contains(appendErr.Error(), wanted) {
					t.Errorf("the refusal does not name %q: %v", wanted, appendErr)
				}
			}
		})
	}
}

func commitOneKey(kv *BadgerKV, key string) error {
	batch := kv.NewBatch()
	if err := batch.Set([]byte(key), []byte("v")); err != nil {
		batch.Discard()
		return err
	}
	return batch.Commit(true)
}

// logClose logs a close error: closing over a removed directory may fail,
// and that failure is not what these tests assert.
func logClose(t *testing.T, what string, err error) {
	if err != nil {
		t.Logf("closing %s: %v", what, err)
	}
}
