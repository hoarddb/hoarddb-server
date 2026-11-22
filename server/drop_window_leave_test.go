package server

import (
	"context"
	"github.com/hoarddb/hoarddb-server/storage"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// Every way out of a drop leaves BUG-0527's unlink window: an entry left
// behind holds the reclaimer off that bucket id, or every record of that
// database, until the node restarts, and nothing reports it. Each test below
// takes one exit and checks the window is empty and the range is still
// reclaimed on the same node.

// requireWindowEmpty fails unless s's unlink window holds no drop.
func requireWindowEmpty(t *testing.T, s *Server, after string) {
	t.Helper()
	s.unlinkWindow.mu.Lock()
	ids, databases := len(s.unlinkWindow.ids), len(s.unlinkWindow.databases)
	s.unlinkWindow.mu.Unlock()
	if ids != 0 || databases != 0 {
		t.Fatalf("after %s the unlink window still holds %d bucket drop(s) and %d database drop(s)", after, ids, databases)
	}
}

// failingCommits makes every batch commit on kv fail until the returned
// function is called: no batch fits a bound of one byte.
func failingCommits(kv *kvfault.Store) (restore func()) {
	kv.SetMaxBatchBytes(1)
	return func() { kv.SetMaxBatchBytes(kvfault.DefaultMaxBatchBytes) }
}

// requireRangeReclaimed drains s's reclaimer and fails if any key of the
// bucket is left.
func requireRangeReclaimed(t *testing.T, s *Server, kv *kvfault.Store, ref storage.BucketRef) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	t.Helper()
	lapUntilDrained(t, s)
	if live := liveKeysIn(t, kv, slot, databaseID, id); live != 0 {
		t.Fatalf("after the drain %d key(s) of dropped bucket %d are still live", live, id)
	}
}

func TestABucketDropWhoseCommitFailsLeavesTheUnlinkWindow(t *testing.T) {
	s, kv, _ := crashWindowServer(t)
	dropped := lookupBucket(s, "app", "gone")
	id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID

	restore := failingCommits(kv)
	resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "gone"})
	restore()
	if err != nil || resp.Success {
		t.Fatalf("the drop with a failing commit answered %+v, %v; want a refusal", resp, err)
	}
	requireWindowEmpty(t, s, "a bucket drop whose commit failed")

	dropOrFail(t, s, "app", "gone")
	requireWindowEmpty(t, s, "the bucket drop that followed it")
	requireRangeReclaimed(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
}

func TestALegacyDatabaseDropLeavesTheUnlinkWindowWhetherItsCommitFailsOrNot(t *testing.T) {
	s, kv, _ := crashWindowServer(t)
	dropped := lookupBucket(s, "app", "gone")
	id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID

	restore := failingCommits(kv)
	resp, err := s.deleteLegacyDatabase(context.Background(), "app")
	restore()
	if err != nil || resp.Success {
		t.Fatalf("the legacy drop with a failing commit answered %+v, %v; want a refusal", resp, err)
	}
	requireWindowEmpty(t, s, "a legacy database drop whose commit failed")

	if resp, err := s.deleteLegacyDatabase(context.Background(), "app"); err != nil || !resp.Success {
		t.Fatalf("the legacy drop: %+v, %v", resp, err)
	}
	requireWindowEmpty(t, s, "a legacy database drop")
	requireRangeReclaimed(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
}

// windowPanic is what the panic tests' hooks panic with.
const windowPanic = "simulated panic inside a drop"

// recoverDrop runs drop and returns what it panicked with, nil if it did not.
func recoverDrop(drop func()) (panicked any) {
	defer func() { panicked = recover() }()
	drop()
	return nil
}

// requireRangeWhole fails unless bucket id still holds whole keys after a
// lap: a drop that panicked between its commit and its unlink leaves the name
// linked, so the reclaimer must keep off the range (the reader rule, spec
// §5.2.2), and a restart reclaims it (TestADropCrashedAtEachPointRecovers).
func requireRangeWhole(t *testing.T, s *Server, kv *kvfault.Store, ref storage.BucketRef, whole int) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	t.Helper()
	if _, err := newReclaimer(s).lap(context.Background()); err != nil {
		t.Fatalf("a lap after the panic: %v", err)
	}
	if live := liveKeysIn(t, kv, slot, databaseID, id); live != whole {
		t.Fatalf("a lap after a panic between the commit and the unlink left %d of the bucket's %d keys: it reclaimed a linked name", live, whole)
	}
}

// TestAPanicInsideABucketDropLeavesTheWindowOnlyWhereThatIsSafe panics a
// bucket drop at each point of its window and recovers. Before the commit
// and after the unlink the window must be left; between them the name is
// still linked, and leaving would hand the reclaimer a bucket a reader can
// still resolve, so the entry stays until a restart.
func TestAPanicInsideABucketDropLeavesTheWindowOnlyWhereThatIsSafe(t *testing.T) {
	t.Cleanup(func() {
		dropPhaseHook.Store(nil)
		afterDropHiddenHook.Store(nil)
	})
	panicAt := func(phase dropPhase) func(dropPhase, string, string) {
		return func(at dropPhase, _, _ string) {
			if at == phase {
				panic(windowPanic)
			}
		}
	}
	dropGone := func(s *Server) func() {
		return func() { _, _ = s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "gone"}) }
	}

	t.Run("before the commit", func(t *testing.T) {
		s, kv, _ := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		hook := panicAt(dropBeforeCommit)
		dropPhaseHook.Store(&hook)
		panicked := recoverDrop(dropGone(s))
		dropPhaseHook.Store(nil)
		if panicked != windowPanic {
			t.Fatalf("the drop panicked with %v, want the hook's panic", panicked)
		}
		requireWindowEmpty(t, s, "a bucket drop that panicked before its commit")
		dropOrFail(t, s, "app", "gone")
		requireRangeReclaimed(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
	})

	t.Run("between the commit and the unlink", func(t *testing.T) {
		s, kv, _ := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		whole := liveKeysIn(t, kv, slot, databaseID, id)
		hook := panicAt(dropCommitted)
		dropPhaseHook.Store(&hook)
		panicked := recoverDrop(dropGone(s))
		dropPhaseHook.Store(nil)
		if panicked != windowPanic {
			t.Fatalf("the drop panicked with %v, want the hook's panic", panicked)
		}
		requireRangeWhole(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, whole)
	})

	t.Run("after the unlink", func(t *testing.T) {
		s, kv, _ := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		hook := func(_, _ string, _ uint64, _ uint16) { panic(windowPanic) }
		afterDropHiddenHook.Store(&hook)
		panicked := recoverDrop(dropGone(s))
		afterDropHiddenHook.Store(nil)
		if panicked != windowPanic {
			t.Fatalf("the drop panicked with %v, want the hook's panic", panicked)
		}
		requireWindowEmpty(t, s, "a bucket drop that panicked after its unlink")
		requireRangeReclaimed(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
	})
}

// TestAPanicInsideADatabaseDropLeavesTheWindowOnlyWhereThatIsSafe is the same
// for a database drop: before its commit the window is left, between the
// commit and the detach the entry stays.
func TestAPanicInsideADatabaseDropLeavesTheWindowOnlyWhereThatIsSafe(t *testing.T) {
	t.Cleanup(func() {
		generationDropCommitHook.Store(nil)
		afterDatabaseDropCommittedHook.Store(nil)
	})
	dropApp := func(s *Server) func() {
		return func() { _, _ = s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"}) }
	}

	t.Run("before the commit", func(t *testing.T) {
		s, kv, _ := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		hook := func(string) error { panic(windowPanic) }
		generationDropCommitHook.Store(&hook)
		panicked := recoverDrop(dropApp(s))
		generationDropCommitHook.Store(nil)
		if panicked != windowPanic {
			t.Fatalf("the drop panicked with %v, want the hook's panic", panicked)
		}
		requireWindowEmpty(t, s, "a database drop that panicked before its commit")
		if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"}); err != nil || !resp.Success {
			t.Fatalf("the drop after the panic: %+v, %v", resp, err)
		}
		requireRangeReclaimed(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id})
	})

	t.Run("between the commit and the detach", func(t *testing.T) {
		s, kv, _ := crashWindowServer(t)
		dropped := lookupBucket(s, "app", "gone")
		id, slot, databaseID := dropped.id.Load(), dropped.slot, dropped.databaseID
		whole := liveKeysIn(t, kv, slot, databaseID, id)
		hook := func(string) { panic(windowPanic) }
		afterDatabaseDropCommittedHook.Store(&hook)
		panicked := recoverDrop(dropApp(s))
		afterDatabaseDropCommittedHook.Store(nil)
		if panicked != windowPanic {
			t.Fatalf("the drop panicked with %v, want the hook's panic", panicked)
		}
		requireRangeWhole(t, s, kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, whole)
	})
}

func TestADatabaseDropOfADatabaseNotOnThisNodeLeavesTheUnlinkWindow(t *testing.T) {
	s, _, _ := crashWindowServer(t)
	resp, err := s.applyGenerationDrop(context.Background(), "elsewhere", []int64{1})
	if err != nil || !resp.Success {
		t.Fatalf("the drop of a database this node does not hold: %+v, %v", resp, err)
	}
	requireWindowEmpty(t, s, "a database drop that found the database not on this node")
}
