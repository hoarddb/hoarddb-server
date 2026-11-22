package server

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A replica's apply of a database create reports a store failure inside its
// answer (Success false) with no Go error, and so does a name it refuses. These
// tests drive a replica on the fault store by hand with a create pushed from
// frameSyncSource (BUG-0573): a create its store failed must be refused, its
// copy voided, and the owner's re-send applied; a create every copy refuses
// alike must still be acknowledged, or the owner re-sends it for ever.

const (
	createdDatabase   = "fresh"
	createdGeneration = int64(777)
	carriedGeneration = int64(555)
)

// databaseCreateEntry is the owner's create of createdDatabase at the sender's
// sequence seq, carrying the dropped generations carried.
func databaseCreateEntry(t *testing.T, seq uint64, carried []int64) ReplEntry {
	t.Helper()
	return ReplEntry{Seq: seq, Op: int32(transport.OpCreateDB), Payload: requestBody(t,
		CreateDatabaseRequest{Name: createdDatabase, Generation: createdGeneration, DroppedGenerations: carried})}
}

// failCommitsOf stores in hook a function that fails every commit for
// createdDatabase until the returned heal is called, and counts the commits
// it failed.
func failCommitsOf(t *testing.T, hook *atomic.Pointer[func(database string) error]) (*atomic.Int32, func()) {
	t.Helper()
	var failed atomic.Int32
	fail := func(database string) error {
		if database != createdDatabase {
			return nil
		}
		failed.Add(1)
		return errors.New("injected: the store's commit failed")
	}
	hook.Store(&fail)
	t.Cleanup(func() { hook.Store(nil) })
	return &failed, func() { hook.Store(nil) }
}

// assertCreateRefusedAndVoided checks that a create whose apply failed was not
// acknowledged, left the position below it, created nothing, and had its
// logged copy voided.
func assertCreateRefusedAndVoided(t *testing.T, s *Server, c *Cluster, create ReplEntry, resp ReplPushResponse) {
	t.Helper()
	if resp.Success || resp.AppliedThrough >= create.Seq {
		t.Fatalf("a create whose apply failed was answered success=%t applied_through=%d; want refused below %d",
			resp.Success, resp.AppliedThrough, create.Seq)
	}
	if !c.shouldApply(frameSyncSource, create.Seq) {
		t.Fatal("the replica's position moved past a create it did not apply: the owner's re-send would be skipped")
	}
	if live, err := s.liveGenerations(createdDatabase); err != nil || len(live) != 0 {
		t.Fatalf("setup: the replica holds generations %v of %q (%v) although the create failed", live, createdDatabase, err)
	}
	c.appliedMu.Lock()
	void, voided := c.orphanVoids[frameSyncSource]
	c.appliedMu.Unlock()
	if failedCopy := loggedCopyOf(t, c, create.Seq); !voided || !void.covers(failedCopy) {
		t.Fatalf("the copy of the failed create (local seq %d) is not voided (void %+v, present %t): a replay would apply it beside the re-send",
			failedCopy.Seq, void, voided)
	}
}

// assertReSendApplies pushes create again and checks the replica now holds its
// incarnation.
func assertReSendApplies(t *testing.T, s *Server, c *Cluster, create ReplEntry) {
	t.Helper()
	if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{create}); !resp.Success || resp.AppliedThrough != create.Seq {
		t.Fatalf("the re-send was answered success=%t applied_through=%d error=%q", resp.Success, resp.AppliedThrough, resp.Error)
	}
	if live, err := s.liveGenerations(createdDatabase); err != nil || !slices.Contains(live, createdGeneration) {
		t.Fatalf("after the re-send applied, the replica's generations of %q are %v (%v); want %d among them",
			createdDatabase, live, err, createdGeneration)
	}
}

func TestReplicaDatabaseCreateApplyFailureIsNotAcknowledged(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	streamOf(t, c, frameSyncSource)
	create := databaseCreateEntry(t, 1, []int64{carriedGeneration})

	failed, heal := failCommitsOf(t, &generationDropCommitHook)
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{create})
	if failed.Load() == 0 {
		t.Fatal("setup: the commit of the drop the create carries was never reached on the replica")
	}
	assertCreateRefusedAndVoided(t, s, c, create, resp)

	heal()
	assertReSendApplies(t, s, c, create)
	if dropped, err := s.generationDropped(createdDatabase, carriedGeneration); err != nil || !dropped {
		t.Fatalf("after the re-send applied, the carried generation %d is not recorded dropped (%v)", carriedGeneration, err)
	}
}

func TestReplicaDatabaseCreateCatalogCommitFailureIsNotAcknowledged(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	streamOf(t, c, frameSyncSource)
	create := databaseCreateEntry(t, 1, nil)

	failed, heal := failCommitsOf(t, &generationAddCommitHook)
	resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{create})
	if failed.Load() == 0 {
		t.Fatal("setup: the commit of the create's catalog entry was never reached on the replica")
	}
	assertCreateRefusedAndVoided(t, s, c, create, resp)

	heal()
	assertReSendApplies(t, s, c, create)
}

// TestReplicaDatabaseCreateRefusalIsStillAcknowledged guards the other side: a
// create every copy refuses alike (a name no database may have), a create of a
// generation this node holds dropped (late), and a legacy create of a database
// already live are settled and acknowledged, never left for the owner to
// re-send for ever.
func TestReplicaDatabaseCreateRefusalIsStillAcknowledged(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	streamOf(t, c, frameSyncSource)
	if resp, err := s.applyGenerationDrop(context.Background(), "gone", []int64{createdGeneration}); err != nil || !resp.Success {
		t.Fatalf("setup: dropping generation %d of gone: %+v, %v", createdGeneration, resp, err)
	}
	for _, create := range []struct {
		name       string
		request    CreateDatabaseRequest
		refusedNow bool
	}{
		{name: "a reserved name", request: CreateDatabaseRequest{Name: authReservedDatabase, Generation: createdGeneration}, refusedNow: true},
		{name: "an invalid name", request: CreateDatabaseRequest{Name: "../fresh", Generation: createdGeneration}, refusedNow: true},
		{name: "a generation dropped here", request: CreateDatabaseRequest{Name: "gone", Generation: createdGeneration}},
		{name: "a legacy create of a live database", request: CreateDatabaseRequest{Name: "app"}},
	} {
		t.Run(create.name, func(t *testing.T) {
			seq := c.recordedPosition(frameSyncSource).AppliedThrough + 1
			entry := ReplEntry{Seq: seq, Op: int32(transport.OpCreateDB), Payload: requestBody(t, create.request)}
			if create.refusedNow {
				answer, err := c.applyLocal(withClusterApply(context.Background()), entry)
				var created CreateDatabaseResponse
				if err != nil || transport.Opcode(answer.Opcode) == transport.OpError ||
					bson.Unmarshal(answer.Payload, &created) != nil || created.Success {
					t.Fatalf("setup: the create was answered %s %+v (%v), want Success false", transport.Opcode(answer.Opcode), created, err)
				}
			}
			if resp := pushToReplica(t, c, frameSyncSource, []ReplEntry{entry}); !resp.Success || resp.AppliedThrough != seq {
				t.Fatalf("the create was answered success=%t applied_through=%d error=%q; want it acknowledged at %d",
					resp.Success, resp.AppliedThrough, resp.Error, seq)
			}
			if c.shouldApply(frameSyncSource, seq) {
				t.Fatalf("the replica's position is below %d after acknowledging it", seq)
			}
		})
	}
}
