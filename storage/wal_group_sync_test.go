package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// groupSyncDiskLatency stands in for a real disk's fsync in these tests: on a
// tmpfs an fsync returns at once and no writer ever waits behind another, so
// the group commit would have nothing to batch whether or not it works.
const groupSyncDiskLatency = 200 * time.Microsecond

// TestGroupSyncCoversEveryEntryWrittenBeforeIt: one group-commit fsync makes
// durable every entry written to the file before it began, so concurrent
// writers share fsyncs instead of taking turns at them. Before the fix the
// leader marked only its own sequence synced, and each waiter behind it with a
// higher sequence led a further fsync: about one fsync per append.
func TestGroupSyncCoversEveryEntryWrittenBeforeIt(t *testing.T) {
	cases := []struct {
		name         string
		segmentBytes int64
	}{
		// 4 MiB holds the whole run, and a preallocated log fills its spares
		// at that size rather than 64 MiB each.
		{name: "one segment", segmentBytes: 4 << 20},
		// Small enough that the run rotates many times: every rotation syncs
		// and closes the file under the group commit's waiters.
		{name: "rotating segments", segmentBytes: 64 << 10},
	}
	for _, mode := range walFlushModes {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				groupSyncCoversEveryEntryWrittenBeforeIt(t, func(dir string) *WAL { return mode.open(t, dir, tc.segmentBytes) })
			})
		}
	}
}

// groupSyncCoversEveryEntryWrittenBeforeIt is one run of
// TestGroupSyncCoversEveryEntryWrittenBeforeIt on the log open opens.
func groupSyncCoversEveryEntryWrittenBeforeIt(t *testing.T, open func(dir string) *WAL) {
	dir := t.TempDir()
	w := open(dir)
	var fsyncs atomic.Int64
	injectFlush(w, func() {
		fsyncs.Add(1)
		time.Sleep(groupSyncDiskLatency)
	})

	const writers, perWriter = 64, 200
	appends := writers * perWriter
	var wg sync.WaitGroup
	failures := make(chan error, appends)
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				seq, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("w%02d-%03d", writer, i)), []byte("payload"))
				if err != nil {
					failures <- fmt.Errorf("writer %d append %d: %w", writer, i, err)
					return
				}
				// The durability point: Append returned, so the log
				// must already count this entry as synced.
				if synced := w.SyncedSequence(); synced < seq {
					failures <- fmt.Errorf("append returned sequence %d with the log synced only through %d", seq, synced)
					return
				}
			}
		}(writer)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}

	got := fsyncs.Load()
	t.Logf("%d concurrent appends cost %d group-commit fsyncs", appends, got)
	if limit := int64(appends / 8); got > limit {
		t.Fatalf("%d appends from %d writers cost %d group-commit fsyncs, want at most %d: "+
			"an fsync is not covering the entries written before it", appends, writers, got, limit)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := open(dir)
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("closing the reopened WAL: %v", err)
		}
	}()
	entries, err := reopened.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the log back: %v", err)
	}
	if len(entries) != appends {
		t.Fatalf("read back %d entries after reopen, want %d", len(entries), appends)
	}
}

// TestGroupSyncLeaderCoversQueuedEntries pins the group commit down to its
// exact fsync count. In the first round a leader with the lowest sequence holds
// its fsync open while the other callers queue behind it; in the second, one
// fsync must cover every queued caller.
//
//   - sync through: entries 1..N are written unsynced before any fsync, so the
//     first fsync covers them all: 1 fsync.
//   - append: entries 2..N are written while the first fsync is in flight, so
//     it may not cover them and the next one must: 2 fsyncs.
//
// Before the fix each round marked only its leader's sequence synced, so a
// queued caller with a higher sequence led a further fsync for an entry the
// previous one had already made durable.
func TestGroupSyncLeaderCoversQueuedEntries(t *testing.T) {
	const callers = 32
	cases := []struct {
		name       string
		wantFsyncs int64
		// prepare runs before any fsync; wait is what every caller runs, and
		// caller 0's wait must be the first to reach the fsync.
		prepare func(t *testing.T, w *WAL)
		wait    func(w *WAL, caller int) (uint64, error)
	}{
		{
			name:       "sync through",
			wantFsyncs: 1,
			prepare: func(t *testing.T, w *WAL) {
				for i := 0; i < callers; i++ {
					if _, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte(fmt.Sprintf("k%02d", i)), []byte("payload"),
						WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: uint64(i + 1)}); err != nil {
						t.Fatalf("unsynced append %d: %v", i, err)
					}
				}
			},
			wait: func(w *WAL, caller int) (uint64, error) {
				seq := uint64(caller + 1)
				return seq, w.SyncThrough(seq)
			},
		},
		{
			name:       "append",
			wantFsyncs: 2,
			prepare:    func(*testing.T, *WAL) {},
			wait: func(w *WAL, caller int) (uint64, error) {
				return w.Append("bucket", OpInsert, []byte(fmt.Sprintf("k%02d", caller)), []byte("payload"))
			},
		},
	}
	for _, mode := range walFlushModes {
		for _, tc := range cases {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				w := mode.open(t, t.TempDir(), 4<<20)
				defer func() {
					if err := w.Close(); err != nil {
						t.Errorf("closing the WAL: %v", err)
					}
				}()
				tc.prepare(t, w)

				var arrived atomic.Int64
				w.SetBeforeSyncHook(func(uint64) { arrived.Add(1) })
				var fsyncs atomic.Int64
				injectFlush(w, func() {
					if fsyncs.Add(1) > 1 {
						return
					}
					// Hold the first fsync until every caller is queued behind it.
					deadline := time.Now().Add(60 * time.Second)
					for arrived.Load() < callers && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					time.Sleep(20 * time.Millisecond)
				})

				type outcome struct {
					seq uint64
					err error
				}
				outcomes := make(chan outcome, callers)
				var wg sync.WaitGroup
				run := func(caller int) {
					defer wg.Done()
					seq, err := tc.wait(w, caller)
					if err == nil && w.SyncedSequence() < seq {
						err = fmt.Errorf("returned with the log synced only through %d", w.SyncedSequence())
					}
					outcomes <- outcome{seq: seq, err: err}
				}
				wg.Add(1)
				go run(0)
				for fsyncs.Load() == 0 {
					time.Sleep(100 * time.Microsecond)
				}
				for caller := 1; caller < callers; caller++ {
					wg.Add(1)
					go run(caller)
				}
				wg.Wait()
				close(outcomes)
				for o := range outcomes {
					if o.err != nil {
						t.Fatalf("caller waiting on sequence %d: %v", o.seq, o.err)
					}
				}
				if got := fsyncs.Load(); got != tc.wantFsyncs {
					t.Fatalf("%d callers queued behind one fsync cost %d group-commit fsyncs, want %d", callers, got, tc.wantFsyncs)
				}
			})
		}
	}
}

// TestGroupSyncSharesFsyncAcrossSyncThroughCallers: the replica path, where each
// caller writes a batch unsynced and then waits in SyncThrough, shares fsyncs
// across callers the way Append does.
func TestGroupSyncSharesFsyncAcrossSyncThroughCallers(t *testing.T) {
	for _, mode := range walFlushModes {
		t.Run(mode.name, func(t *testing.T) {
			w := mode.open(t, t.TempDir(), 4<<20)
			defer func() {
				if err := w.Close(); err != nil {
					t.Errorf("closing the WAL: %v", err)
				}
			}()
			var fsyncs atomic.Int64
			injectFlush(w, func() {
				fsyncs.Add(1)
				time.Sleep(groupSyncDiskLatency)
			})

			const callers, rounds, batch = 64, 50, 4
			var wg sync.WaitGroup
			failures := make(chan error, callers)
			for caller := 0; caller < callers; caller++ {
				wg.Add(1)
				go func(caller int) {
					defer wg.Done()
					for round := 0; round < rounds; round++ {
						var last uint64
						for i := 0; i < batch; i++ {
							seq, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte(fmt.Sprintf("c%02d-%03d-%d", caller, round, i)), []byte("payload"),
								WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: uint64(round*batch + i + 1)})
							if err != nil {
								failures <- fmt.Errorf("caller %d unsynced append: %w", caller, err)
								return
							}
							last = seq
						}
						if err := w.SyncThrough(last); err != nil {
							failures <- fmt.Errorf("caller %d SyncThrough(%d): %w", caller, last, err)
							return
						}
						if synced := w.SyncedSequence(); synced < last {
							failures <- fmt.Errorf("SyncThrough(%d) returned with the log synced only through %d", last, synced)
							return
						}
					}
				}(caller)
			}
			wg.Wait()
			close(failures)
			for failure := range failures {
				t.Fatal(failure)
			}

			syncCalls := callers * rounds
			got := fsyncs.Load()
			t.Logf("%d concurrent SyncThrough calls cost %d group-commit fsyncs", syncCalls, got)
			// A quarter, not the eighth Append is held to: a SyncThrough caller does
			// four unsynced appends between fsyncs, so under the race detector fewer
			// callers are queued at each one. One fsync per call is what this catches.
			if limit := int64(syncCalls / 4); got > limit {
				t.Fatalf("%d SyncThrough calls from %d callers cost %d fsyncs, want at most %d", syncCalls, callers, got, limit)
			}
		})
	}
}

// TestGroupSyncDoesNotClaimEntriesWrittenDuringFsync: an entry written after the
// group-commit fsync began may not be in what that fsync made durable, so the
// fsync must not count it as synced.
func TestGroupSyncDoesNotClaimEntriesWrittenDuringFsync(t *testing.T) {
	for _, mode := range walFlushModes {
		t.Run(mode.name, func(t *testing.T) {
			w := mode.open(t, t.TempDir(), 4<<20)
			defer func() {
				if err := w.Close(); err != nil {
					t.Errorf("closing the WAL: %v", err)
				}
			}()

			var lateSeq uint64
			var lateErr error
			var once sync.Once
			// The hook runs in the leader after it has decided what its fsync covers
			// and before the fsync: an entry written here is one the fsync may miss.
			injectFlush(w, func() {
				once.Do(func() {
					lateSeq, lateErr = w.AppendUnsyncedFrom("bucket", OpInsert, []byte("late"), []byte("payload"),
						WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: 1})
				})
			})

			seq, err := w.Append("bucket", OpInsert, []byte("first"), []byte("payload"))
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			if lateErr != nil {
				t.Fatalf("appending during the fsync: %v", lateErr)
			}
			if lateSeq <= seq {
				t.Fatalf("the entry written during the fsync got sequence %d, not after the leader's %d", lateSeq, seq)
			}
			if synced := w.SyncedSequence(); synced != seq {
				t.Fatalf("after one fsync the log reports itself synced through %d, want %d: sequence %d was written after that fsync began",
					synced, seq, lateSeq)
			}
			if err := w.SyncThrough(lateSeq); err != nil {
				t.Fatalf("SyncThrough(%d): %v", lateSeq, err)
			}
			if synced := w.SyncedSequence(); synced < lateSeq {
				t.Fatalf("SyncThrough(%d) returned with the log synced only through %d", lateSeq, synced)
			}
		})
	}
}

// TestGroupSyncFailureFailsEveryWaiter: a failed group-commit fsync fails every
// append waiting on it, advances nothing, and is never retried. A second fsync
// after a failed one can report success for pages the kernel already dropped,
// so a waiter that retried it would acknowledge a write that is not on disk.
func TestGroupSyncFailureFailsEveryWaiter(t *testing.T) {
	for _, mode := range walFlushModes {
		t.Run(mode.name, func(t *testing.T) {
			w := mode.open(t, t.TempDir(), 4<<20)
			defer func() {
				// The log is poisoned; Close's own fsync may succeed or not, and
				// either is fine here.
				_ = w.Close()
			}()

			const writers = 16
			var arrived atomic.Int64
			w.SetBeforeSyncHook(func(uint64) { arrived.Add(1) })

			simulated := errors.New("simulated EIO from fsync")
			var syncCalls atomic.Int64
			failFlushes(w, func(*os.File) error {
				if syncCalls.Add(1) > 1 {
					// What a retried fsync can do after a failed one: report
					// success though the failed pages never reached the disk.
					return nil
				}
				// Hold the failing fsync until every writer has written its entry,
				// and give them time to queue behind it.
				deadline := time.Now().Add(60 * time.Second)
				for arrived.Load() < writers && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				time.Sleep(50 * time.Millisecond)
				return simulated
			})
			syncedBefore := w.SyncedSequence()

			var wg sync.WaitGroup
			appendErrs := make(chan error, writers)
			for writer := 0; writer < writers; writer++ {
				wg.Add(1)
				go func(writer int) {
					defer wg.Done()
					_, err := w.Append("bucket", OpInsert, []byte(fmt.Sprintf("w%02d", writer)), []byte("payload"))
					appendErrs <- err
				}(writer)
			}
			wg.Wait()
			close(appendErrs)

			succeeded := 0
			for err := range appendErrs {
				if err == nil {
					succeeded++
					continue
				}
				if !errors.Is(err, simulated) {
					t.Errorf("a waiting append failed with %v, want it to wrap %v", err, simulated)
				}
			}
			if succeeded > 0 {
				t.Errorf("%d of %d appends waiting on a failed fsync reported success", succeeded, writers)
			}
			if calls := syncCalls.Load(); calls != 1 {
				t.Errorf("the group commit ran %d fsyncs, want 1: an fsync after a failed one was retried", calls)
			}
			if synced := w.SyncedSequence(); synced != syncedBefore {
				t.Errorf("a failed fsync moved the synced sequence from %d to %d", syncedBefore, synced)
			}
			if err := w.SyncThrough(w.LastSequence()); err == nil {
				t.Error("SyncThrough on a log poisoned by a failed fsync succeeded")
			}
		})
	}
}
