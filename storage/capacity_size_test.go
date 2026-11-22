package storage

import (
	"os"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// TestBadgerKVSizeRefusesWhenMetricsDisabled (node-capacity build doc BT-6)
// proves Size refuses the two cases where Badger's DB.Size returns 0, 0 with
// no error, and that the node store's own options take neither.
func TestBadgerKVSizeRefusesWhenMetricsDisabled(t *testing.T) {
	t.Run("metrics disabled", func(t *testing.T) {
		db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithMetricsEnabled(false).WithLogger(nil))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		kv := &BadgerKV{db: db}
		t.Cleanup(func() { _ = kv.Close() })
		if lsm, vlog, err := kv.Size(); err == nil {
			t.Fatalf("Size with metrics disabled = %d, %d, nil; Badger reports 0, 0 here and it must be refused", lsm, vlog)
		}
	})
	t.Run("in memory", func(t *testing.T) {
		db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		kv := &BadgerKV{db: db}
		t.Cleanup(func() { _ = kv.Close() })
		if lsm, vlog, err := kv.Size(); err == nil {
			t.Fatalf("Size of an in-memory store = %d, %d, nil; Badger reports 0, 0 here and it must be refused", lsm, vlog)
		}
	})
	t.Run("the node store's own options", func(t *testing.T) {
		dir := t.TempDir()
		kv, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		batch := kv.NewBatch()
		if err := batch.Set([]byte("capacity-size"), []byte("one value")); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := batch.Commit(false); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := kv.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		// Badger measures the directory inside Open, before it creates the value
		// log, and then once a minute (badger v4.9.6 db.go:Open calls
		// calculateSize ahead of vlog.open). A store opened on an empty
		// directory therefore reports 0, 0 until its first tick, which is a
		// true, stale reading, not a refusal. Reopening makes Open measure the
		// files the write left, so the total must be positive.
		if _, _, err := kv.Size(); err != nil {
			t.Fatalf("Size of the node store before reopening = %v; production options must not take the error branch", err)
		}
		if err := kv.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		kv, err = OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		t.Cleanup(func() { _ = kv.Close() })
		lsm, vlog, err := kv.Size()
		if err != nil || lsm+vlog <= 0 {
			t.Fatalf("Size of the node store = %d, %d, %v; want no error and a positive total", lsm, vlog, err)
		}
	})
}

// TestWALSegmentBytesReportsReadDirFailure (node-capacity build doc BT-5)
// proves SegmentBytes sums segment sizes and reports an unreadable directory
// as an error, where listSegments would have returned nothing and read as 0.
func TestWALSegmentBytesReportsReadDirFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	first := []byte("first segment bytes")
	second := []byte("second segment, a little longer")
	for name, content := range map[string][]byte{segmentFileName(90): first, segmentFileName(91): second} {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	active, err := os.Stat(segmentFilePath(dir, w.activeIndex))
	if err != nil {
		t.Fatalf("stat the active segment: %v", err)
	}
	want := active.Size() + int64(len(first)+len(second))
	if got, err := w.SegmentBytes(); err != nil || got != want {
		t.Fatalf("SegmentBytes = %d, %v; want %d, nil", got, err, want)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove the WAL directory: %v", err)
	}
	if got, err := w.SegmentBytes(); err == nil {
		t.Fatalf("SegmentBytes of a removed directory = %d, nil; want an error, not a zero", got)
	}
}
