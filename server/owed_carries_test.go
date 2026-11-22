package server

import (
	"context"
	"github.com/hoarddb/hoarddb-server/storage"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The failed-apply carry outlives the engine that recorded it (catalog-gc
// spec §5.1 item 2, build doc CG-8). An engine is going to be built per
// operation, so a carry that lives only in the engine that failed is a
// forgotten owed apply: replication state lost without an error.

// recordingCtx is the owner's live apply of the entry at seq, whose store
// failure is recorded (spec §4.4 rule 5).
func recordingCtx(seq uint64) context.Context {
	return withRecordFailures(withApplySeq(context.Background(), seq))
}

// failOneRecordedPut makes the store fail the commit of a recorded put at seq
// and returns the bucket it failed on, which then owes seq.
func failOneRecordedPut(t *testing.T, s *Server, database, bucket string, seq uint64) *inMemoryBucket {
	t.Helper()
	failed := failNextApplyOn(t, s.kv, anySeq)
	resp, err := s.InsertCtx(recordingCtx(seq), InsertRequest{Database: database, Bucket: bucket, Key: "lost", Data: map[string]interface{}{"v": 1}})
	if err != nil {
		t.Fatalf("the failing put answered an error instead of a failed response: %v", err)
	}
	if resp.Success || failed.Load() != seq {
		t.Fatalf("the put at %d was not failed by the store (success %v, failed seq %d); the test failed nothing", seq, resp.Success, failed.Load())
	}
	resident := lookupBucket(s, database, bucket)
	if !s.bucketOwesApplies(database, bucket) {
		t.Fatalf("after a recorded store failure at %d the bucket does not owe it", seq)
	}
	return resident
}

// newHandleOf builds what CG-13's per-operation handle is: a fresh struct for
// the bucket the resident one names, with its own engines over the node store.
func newHandleOf(t *testing.T, s *Server, resident *inMemoryBucket) *inMemoryBucket {
	t.Helper()
	id := resident.id.Load()
	// rec and epoch are what noteApplyOutcome reads to record the lineage; a
	// handle without them dereferences a nil record while the test holds the
	// record's lock, and the cleanup's closeAllRecords then waits on it forever.
	handle := &inMemoryBucket{rec: resident.rec, epoch: resident.epoch, bucketType: resident.bucketType, slot: resident.slot, databaseID: storage.LegacyDatabaseID("app")}
	handle.id.Store(id)
	if err := s.buildEngines(handle, s.kv, handle.slot, id); err != nil {
		t.Fatalf("building a second engine for bucket %d: %v", id, err)
	}
	return handle
}

// TestOwedCarrySurvivesHandles: a put fails on the owner and is carried; the
// next write goes through a new engine for the same bucket, and its batch
// records the failure in the bucket's failed_seqs all the same. A new engine
// with a carry of its own drops the owed sequence, and replay never retries it.
func TestOwedCarrySurvivesHandles(t *testing.T) {
	const database, bucket = "app", "carried"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	const failedSeq, nextSeq = 10, 11
	resident := failOneRecordedPut(t, s, database, bucket, failedSeq)

	handle := newHandleOf(t, s, resident)
	ctx := recordingCtx(nextSeq)
	putErr := func() error {
		resident.rec.mu.Lock()
		defer resident.rec.mu.Unlock()
		putErr := handle.hashStore.Put(applyInfoFrom(ctx, s), "next", map[string]interface{}{"v": 2}, 0)
		s.noteApplyOutcome(ctx, appliedWrite{database: database, bucket: bucket, b: handle, op: "PUT"}, putErr)
		return putErr
	}()
	if putErr != nil {
		t.Fatalf("the put at %d through the new engine: %v", nextSeq, putErr)
	}

	if recorded := handle.owedApplies().Recorded(); recorded != 1 {
		t.Errorf("the new engine's carry records %d failures after its batch, want 1: it did not inherit the owed sequence %d", recorded, failedSeq)
	}
	if st := bucketStateOf(t, s, database, bucket); !slices.Equal(st.FailedSeqs, []uint64{failedSeq}) {
		t.Errorf("failed_seqs after the next batch = %v, want [%d]: the carry did not survive the new engine, so replay would never retry it", st.FailedSeqs, failedSeq)
	}
	if !s.bucketOwesApplies(database, bucket) {
		t.Error("the bucket no longer owes an apply after its failure was recorded")
	}
	if n := s.owed.count(); n != 1 {
		t.Errorf("the owed registry holds %d sequences, want 1 (%d)", n, failedSeq)
	}
}

// TestOwedCarryDroppedWithBucket: a drop removes the bucket's entry from the
// carry table, as it removes its sequences from the owed registry. The bucket's
// state document goes with its range, so nothing would ever settle the entry,
// and it would keep every engine build on the node taking the table's lock.
func TestOwedCarryDroppedWithBucket(t *testing.T) {
	const database, bucket = "app", "dropped"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	resident := failOneRecordedPut(t, s, database, bucket, 10)
	id := resident.id.Load()
	if !s.owed.carries.owes(id) {
		t.Fatalf("bucket %d owes an apply and the carry table has no entry for it", id)
	}

	resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket})
	if err != nil || !resp.Success {
		t.Fatalf("dropping %s/%s: %+v, %v", database, bucket, resp, err)
	}
	if s.owed.carries.owes(id) {
		t.Errorf("the carry table still holds dropped bucket %d", id)
	}
	if n := s.owed.carries.owing.Load(); n != 0 {
		t.Errorf("the carry table counts %d entries after the only owing bucket was dropped, want 0", n)
	}
	if n := s.owed.count(); n != 0 {
		t.Errorf("the owed registry holds %d sequences of the dropped bucket, want 0", n)
	}
}

// TestOwedCarryKeepsRecordedUntilRestart (spec §5.1 item 2): once a failure is
// recorded in failed_seqs, the carry has nothing pending but still owes, so its
// entry stays — replay is the only retry. After the restart replay applies the
// write, and nothing is owed any more.
func TestOwedCarryKeepsRecordedUntilRestart(t *testing.T) {
	tc, ownerIndex := applyOrderCluster(t)
	defer tc.stop()
	ownerCore := tc.nodes[ownerIndex].server.core

	failed := failNextApplyOn(t, ownerCore.kv, anySeq)
	putValue(t, tc, ownerIndex, "a", "a1", "majority")
	seqA := failed.Load()
	if seqA == 0 {
		t.Fatal("A's apply never reached the store; the test failed nothing")
	}
	if frame := putValue(t, tc, ownerIndex, "b", "b1", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write B: %s", string(frame.Payload))
	}
	resident := lookupBucket(ownerCore, testDatabase, testBucket)
	id := resident.id.Load()
	resident.rec.mu.RLock()
	owes, recorded := resident.owedApplies().Owes(), resident.owedApplies().Recorded()
	resident.rec.mu.RUnlock()
	if !owes || recorded != 1 {
		t.Fatalf("after B the carry owes=%v recorded=%d, want it to owe with 1 recorded (A at %d)", owes, recorded, seqA)
	}
	if !ownerCore.owed.carries.owes(id) {
		t.Fatalf("A (%d) is recorded in failed_seqs with nothing pending, and the carry table dropped bucket %d's entry: it stays until the restart that retries it", seqA, id)
	}
	if !ownerCore.bucketOwesApplies(testDatabase, testBucket) {
		t.Fatal("the bucket does not report that it owes A")
	}

	restarted := tc.restartNode(ownerIndex).server.core
	if _, found := valueOnNode(t, tc, ownerIndex, "a"); !found {
		t.Fatalf("after the restart A (seq %d) is missing: replay did not retry it", seqA)
	}
	if restarted.bucketOwesApplies(testDatabase, testBucket) {
		t.Error("after the restart that retried A the bucket still owes an apply")
	}
	if st := bucketStateOf(t, restarted, testDatabase, testBucket); len(st.FailedSeqs) != 0 {
		t.Errorf("after the restart failed_seqs = %v, want none", st.FailedSeqs)
	}
}
