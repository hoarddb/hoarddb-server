package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// bucketStateOf reads a served bucket's state document through its recorded
// slot and id.
func bucketStateOf(t *testing.T, s *Server, database, bucket string) store.BucketState {
	t.Helper()
	b := lookupBucket(s, database, bucket)
	if b == nil {
		t.Fatalf("bucket %s/%s is not served", database, bucket)
	}
	b.rec.mu.RLock()
	slot, databaseID, id := b.slot, b.databaseID, b.id.Load()
	b.rec.mu.RUnlock()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	st, err := store.LoadBucketState(snap, slot, databaseID, id)
	if err != nil {
		t.Fatalf("LoadBucketState(%s/%s): %v", database, bucket, err)
	}
	return st
}

// loggedSequence is the sequence this node's WAL gave the one entry of op on
// bucket. The log's last sequence will not do: a broadcast entry can land on
// this node after the entry of interest.
func loggedSequence(t *testing.T, s *Server, op transport.Opcode, bucket string) uint64 {
	t.Helper()
	entries, err := s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the WAL: %v", err)
	}
	var seq uint64
	for _, e := range entries {
		if transport.Opcode(e.Op) == op && e.Bucket == bucket {
			if seq != 0 {
				t.Fatalf("the WAL holds two %s entries for %q", op, bucket)
			}
			seq = e.Seq
		}
	}
	if seq == 0 {
		t.Fatalf("the WAL holds no %s entry for %q", op, bucket)
	}
	return seq
}

// TestCreateWritesTheCreateFloor is spec §5.2.2's create floor: a created
// bucket's applied_seq starts at the sequence of its own create, so replayed
// entries of an earlier bucket of the same name are at or below it and are
// skipped. On a standalone node there is no sequence and the floor is 0.
func TestCreateWritesTheCreateFloor(t *testing.T) {
	t.Run("cluster owner", func(t *testing.T) {
		tc := newTestCluster(t, 2, 1)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
		if err != nil {
			t.Fatalf("owner: %v", err)
		}
		ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
		core := tc.nodes[ownerIndex].server.core
		tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		createSeq := loggedSequence(t, core, transport.OpCreateBucket, testBucket)
		if floor := bucketStateOf(t, core, testDatabase, testBucket).AppliedSeq; floor != createSeq {
			t.Fatalf("the created bucket's applied_seq is %d, want its create's sequence %d", floor, createSeq)
		}
	})

	t.Run("standalone", func(t *testing.T) {
		s := newTestServer(t)
		mustCreateBucket(t, s, "app", "users")
		if floor := bucketStateOf(t, s, "app", "users").AppliedSeq; floor != 0 {
			t.Fatalf("a standalone node's create floor is %d, want 0: it has no sequence", floor)
		}
	})
}

// TestApplySeqAndWriteVersionAreSeparate is SA-17's separation: on a replica
// the node's own WAL sequence (the apply sequence) and the owner's (the write
// version) differ, so neither may be read from the other's key, and replay is
// marked on its own.
func TestApplySeqAndWriteVersionAreSeparate(t *testing.T) {
	ctx := withWriteVersion(withApplySeq(t.Context(), 7), 42)
	if got := applySeqFrom(ctx); got != 7 {
		t.Errorf("apply sequence = %d, want this node's 7", got)
	}
	if got := writeVersionFrom(ctx); got != 42 {
		t.Errorf("write version = %d, want the owner's 42", got)
	}
	if isReplay(ctx) {
		t.Error("a live dispatch reads as replay")
	}
	replayed := withReplay(withApplySeq(t.Context(), 9))
	if !isReplay(replayed) || applySeqFrom(replayed) != 9 || writeVersionFrom(replayed) != 0 {
		t.Errorf("a replayed dispatch = (replay %v, apply seq %d, version %d), want (true, 9, 0)",
			isReplay(replayed), applySeqFrom(replayed), writeVersionFrom(replayed))
	}
}
