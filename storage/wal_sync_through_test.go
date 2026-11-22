package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestAppendUnsyncedThenSyncThroughIsOneFsync: a run of unsynced appends is not
// durable until SyncThrough, which makes all of it durable with one fsync, even
// when the run crossed a rotation (the rotation synced the file it closed).
func TestAppendUnsyncedThenSyncThroughIsOneFsync(t *testing.T) {
	for _, mode := range walFlushModes {
		t.Run(mode.name, func(t *testing.T) {
			// Small enough that the run below rotates at least once.
			w := mode.open(t, t.TempDir(), 2048)
			defer func() {
				if err := w.Close(); err != nil {
					t.Errorf("closing the WAL: %v", err)
				}
			}()
			var fsyncs atomic.Int64
			w.SetFsyncHook(func() { fsyncs.Add(1) })

			const run = 64
			var last uint64
			for i := 0; i < run; i++ {
				seq, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte(fmt.Sprintf("k%02d", i)), []byte("payload"),
					WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: uint64(i + 1)})
				if err != nil {
					t.Fatalf("unsynced append %d: %v", i, err)
				}
				last = seq
			}
			if synced := w.SyncedSequence(); synced >= last {
				t.Fatalf("the log reports itself synced through %d after %d unsynced appends ending at %d", synced, run, last)
			}
			if got := fsyncs.Load(); got != 0 {
				t.Fatalf("%d unsynced appends cost %d group-commit fsyncs, want 0", run, got)
			}
			if err := w.SyncThrough(last); err != nil {
				t.Fatalf("SyncThrough(%d): %v", last, err)
			}
			if synced := w.SyncedSequence(); synced < last {
				t.Fatalf("SyncThrough(%d) returned with the log synced through %d", last, synced)
			}
			if got := fsyncs.Load(); got != 1 {
				t.Fatalf("SyncThrough after %d unsynced appends cost %d fsyncs, want 1", run, got)
			}
			entries, err := w.ReadFrom(1)
			if err != nil {
				t.Fatalf("reading the log back: %v", err)
			}
			if len(entries) != run {
				t.Fatalf("read back %d entries, want %d", len(entries), run)
			}
		})
	}
}

// TestSyncThroughRacesAppendsAndRotation runs SyncThrough beside appends that
// rotate the log (run with -race): the file it syncs must never be closed under
// it, which would poison the log with a spurious failure.
func TestSyncThroughRacesAppendsAndRotation(t *testing.T) {
	for _, mode := range walFlushModes {
		t.Run(mode.name, func(t *testing.T) {
			w := mode.open(t, t.TempDir(), 1024)
			defer func() {
				if err := w.Close(); err != nil {
					t.Errorf("closing the WAL: %v", err)
				}
			}()

			const writers, perWriter = 8, 40
			var wg sync.WaitGroup
			failures := make(chan error, writers*perWriter)
			for g := 0; g < writers; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						key := []byte(fmt.Sprintf("g%d-%d", g, i))
						if g%2 == 0 {
							if _, err := w.Append("bucket", OpInsert, key, []byte("payload")); err != nil {
								failures <- fmt.Errorf("append %s: %w", key, err)
							}
							continue
						}
						seq, err := w.AppendUnsyncedFrom("bucket", OpInsert, key, []byte("payload"),
							WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: uint64(g*perWriter + i + 1)})
						if err != nil {
							failures <- fmt.Errorf("unsynced append %s: %w", key, err)
							continue
						}
						if err := w.SyncThrough(seq); err != nil {
							failures <- fmt.Errorf("sync through %d: %w", seq, err)
						}
					}
				}(g)
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
			if synced, lastSeq := w.SyncedSequence(), w.LastSequence(); synced != lastSeq {
				t.Fatalf("every append returned or was synced through, and the log is synced through %d of %d", synced, lastSeq)
			}
		})
	}
}

// TestSyncThroughAClosedLogIsAnError: the log closed under a caller is an
// error it can answer, not a sync of a closed file that poisons the log.
func TestSyncThroughAClosedLogIsAnError(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	seq, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte("k"), []byte("v"),
		WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: 1})
	if err != nil {
		t.Fatalf("unsynced append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing the WAL: %v", err)
	}
	if err := w.SyncThrough(seq); err == nil {
		t.Fatal("SyncThrough on a closed log returned nil")
	}
}
