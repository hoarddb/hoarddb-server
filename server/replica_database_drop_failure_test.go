package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A replica's apply of a database drop reports a store failure inside its
// answer (Success false) with no Go error. These tests drive a replica on the
// fault store by hand with a drop of app pushed from frameSyncSource whose
// commit fails there (BUG-0572): the push must be refused, its copy voided,
// and the owner's re-send applied, before and after a retention pass and a
// restart.

// appDropEntry is the owner's drop of every live generation of app on s, at
// the sender's sequence seq.
func appDropEntry(t *testing.T, s *Server, seq uint64) ReplEntry {
	t.Helper()
	live, err := s.liveGenerations("app")
	if err != nil || len(live) == 0 {
		t.Fatalf("setup: app's live generations are %v (%v)", live, err)
	}
	return ReplEntry{Seq: seq, Op: int32(transport.OpDeleteDB),
		Payload: requestBody(t, DeleteDatabaseRequest{Name: "app", Generations: live})}
}

// failAppDropCommits fails every commit of a drop of app until the returned
// function is called, and counts the commits it failed.
func failAppDropCommits(t *testing.T) (*atomic.Int32, func()) {
	t.Helper()
	var failed atomic.Int32
	hook := func(database string) error {
		if database != "app" {
			return nil
		}
		failed.Add(1)
		return errors.New("injected: the drop's synced commit failed")
	}
	generationDropCommitHook.Store(&hook)
	t.Cleanup(func() { generationDropCommitHook.Store(nil) })
	return &failed, func() { generationDropCommitHook.Store(nil) }
}

// loggedCopyOf is the copy of the sender's entry seq in c's log.
func loggedCopyOf(t *testing.T, c *Cluster, seq uint64) storage.WALEntry {
	t.Helper()
	logged, err := c.s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the replica's log: %v", err)
	}
	for _, entry := range logged {
		if entry.Origin.Kind == storage.WALOriginReplica && entry.Origin.Source == frameSyncSource && entry.Origin.Seq == seq {
			return entry
		}
	}
	t.Fatalf("the replica's log holds no copy of %s's entry %d", frameSyncSource, seq)
	return storage.WALEntry{}
}

func TestReplicaDatabaseDropApplyFailureIsNotAcknowledged(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	drop := appDropEntry(t, s, 1)
	streamOf(t, c, frameSyncSource)

	failed, heal := failAppDropCommits(t)
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{drop})
	if failed.Load() == 0 {
		t.Fatal("setup: the drop's commit was never reached on the replica")
	}
	if resp.Success || resp.AppliedThrough >= drop.Seq {
		t.Fatalf("a drop whose apply failed was answered success=%t applied_through=%d; want refused below %d",
			resp.Success, resp.AppliedThrough, drop.Seq)
	}
	if !c.shouldApply(frameSyncSource, drop.Seq) {
		t.Fatal("the replica's position moved past a drop it did not apply: the owner's re-send would be skipped")
	}
	if !s.bucketStoreOpen("app", "held") {
		t.Fatal("setup: app/held is gone although the drop's commit failed")
	}
	c.appliedMu.Lock()
	void, voided := c.orphanVoids[frameSyncSource]
	c.appliedMu.Unlock()
	if failedCopy := loggedCopyOf(t, c, drop.Seq); !voided || !void.covers(failedCopy) {
		t.Fatalf("the copy of the failed drop (local seq %d) is not voided (void %+v, present %t): a replay would apply it beside the re-send",
			failedCopy.Seq, void, voided)
	}

	heal()
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{drop}); !resp.Success || resp.AppliedThrough != drop.Seq {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if s.bucketStoreOpen("app", "held") {
		t.Fatal("after the re-send applied, app/held is still served")
	}
}

func TestReplicaDatabaseDropFailureSurvivesRetention(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	// The replica's own relogged drop (G-12) would be a second copy of the
	// drop in its log, which a replay applies: off, the owner's entry is the
	// only one, and only the owner's re-send can apply it.
	generationSeamsHook.Store(&generationSeams{noRelog: true})
	t.Cleanup(func() { generationSeamsHook.Store(nil) })
	s.wal.SetMaxSegmentBytes(1024)
	drop := appDropEntry(t, s, 1)
	streamOf(t, c, frameSyncSource)

	failed, heal := failAppDropCommits(t)
	pushToReplica(t, c, frameSyncSource, []ReplEntry{drop})
	if failed.Load() == 0 {
		t.Fatal("setup: the drop's commit was never reached on the replica")
	}
	heal()
	failedCopy := loggedCopyOf(t, c, drop.Seq)

	// Writes to another database's bucket would be dropped with app; app's
	// sealer is written as its owner, which seals the drop's segment.
	filler := strings.Repeat("x", 700)
	for _, key := range []string{"f1", "f2"} {
		if err := putThrough(c, asOwner, "sealer", key, filler); err != nil {
			t.Fatalf("the write that seals the drop's segment: %v", err)
		}
	}
	if err := c.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	if first, err := s.wal.FirstSequence(); err != nil || first <= failedCopy.Seq {
		t.Fatalf("setup: the log still holds the failed drop's copy at %d (first %d, %v): retention did not pass it",
			failedCopy.Seq, first, err)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if !restarted.shouldApply(frameSyncSource, drop.Seq) {
		t.Fatal("after retention and a restart the replica's position is past a drop it never applied: the owner's re-send is skipped and app stays live")
	}
	if resp := pushToReplica(t, restarted, frameSyncSource, []ReplEntry{drop}); !resp.Success || resp.AppliedThrough != drop.Seq {
		t.Fatalf("the re-send after the restart was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if restarted.s.bucketStoreOpen("app", "held") {
		t.Fatal("after the restart and the re-send, app/held is still served")
	}
}

// TestReplicaDatabaseDropRefusalIsAcknowledged guards the other side: a drop
// every copy refuses alike (an error frame) or finds nothing to do (a database
// not on this node) is settled and acknowledged, never left for the owner to
// re-send for ever.
func TestReplicaDatabaseDropRefusalIsAcknowledged(t *testing.T) {
	_, _, c, _ := newReplicaOnFaultStore(t)
	streamOf(t, c, frameSyncSource)
	for _, drop := range []struct {
		name       string
		request    DeleteDatabaseRequest
		errorFrame bool
	}{
		{name: "an invalid name, refused with an error frame", request: DeleteDatabaseRequest{Name: "../app", Generations: []int64{1}}, errorFrame: true},
		{name: "a database this node does not hold", request: DeleteDatabaseRequest{Name: "elsewhere", Generations: []int64{1}}},
	} {
		t.Run(drop.name, func(t *testing.T) {
			seq := c.recordedPosition(frameSyncSource).AppliedThrough + 1
			entry := ReplEntry{Seq: seq, Op: int32(transport.OpDeleteDB), Payload: requestBody(t, drop.request)}
			if drop.errorFrame {
				answer, err := c.applyLocal(context.Background(), entry)
				if err != nil || transport.Opcode(answer.Opcode) != transport.OpError {
					t.Fatalf("setup: the drop was answered %s (%v), want an error frame", transport.Opcode(answer.Opcode), err)
				}
			}
			if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{entry}); !resp.Success || resp.AppliedThrough != seq {
				t.Fatalf("the drop was answered success=%t applied_through=%d error=%q; want it acknowledged at %d",
					resp.Success, resp.AppliedThrough, resp.Error, seq)
			}
			if c.shouldApply(frameSyncSource, seq) {
				t.Fatalf("the replica's position is below %d after acknowledging it", seq)
			}
		})
	}
}
