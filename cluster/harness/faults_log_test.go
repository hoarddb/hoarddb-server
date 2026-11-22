package harness

import (
	"errors"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestFaultyLogCoversTheUnsyncedPath: the disk failure reaches the unsynced append too, and a
// split append-then-SyncThrough pays the injected latency once, as an append does.
func TestFaultyLogCoversTheUnsyncedPath(t *testing.T) {
	wal, err := storage.OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	})
	origin := storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: 1}
	log := faultyLog{Log: wal, fault: &diskFault{}}

	seq, err := log.AppendUnsyncedFrom("bucket", storage.OpInsert, []byte("k1"), []byte("v1"), origin)
	if err != nil {
		t.Fatalf("AppendUnsyncedFrom with no fault: %v", err)
	}
	if err := log.SyncThrough(seq); err != nil {
		t.Fatalf("SyncThrough(%d) with no fault: %v", seq, err)
	}

	log.fault.failing.Store(true)
	if _, err := log.AppendUnsyncedFrom("bucket", storage.OpInsert, []byte("k2"), []byte("v2"), origin); !errors.Is(err, errDiskFull) {
		t.Fatalf("AppendUnsyncedFrom with the disk fault returned %v, want errDiskFull", err)
	}
	log.fault.failing.Store(false)

	// The latency is charged once per write, at the flush: the append runs under the pair lock, and a
	// delay there would serialise writers that the split exists to let share one flush (review min-1).
	const slow = 50 * time.Millisecond
	log.fault.slowNanos.Store(int64(slow))
	start := time.Now()
	seq, err = log.AppendUnsyncedFrom("bucket", storage.OpInsert, []byte("k3"), []byte("v3"), origin)
	if err != nil {
		t.Fatalf("AppendUnsyncedFrom on a slow disk: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= slow/2 {
		t.Fatalf("AppendUnsyncedFrom on a slow disk took %v, want it to leave the delay to the flush", elapsed)
	}
	if err := log.SyncThrough(seq); err != nil {
		t.Fatalf("SyncThrough(%d) on a slow disk: %v", seq, err)
	}
	if elapsed := time.Since(start); elapsed < slow || elapsed >= 2*slow {
		t.Fatalf("append then flush on a slow disk took %v, want the delay once: at least %v and under %v", elapsed, slow, 2*slow)
	}
}
