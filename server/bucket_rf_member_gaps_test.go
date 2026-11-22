package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Three behaviours of the member's side that G3-b's verifier found no test
// for: the outside check of row 1, the needs-a-copy delete of a drop of no
// copy, and why the mark rows turn off the store's lineage check.

// markedCopyInBothSets is three nodes at default 3, a bucket at 2 owned by
// node 1, and the second holder's copy marked outside by hand, as a pass marks
// a copy that left its set and is placed in it again: an increase to 3 keeps
// that copy in both sets, so only its mark keeps it from row 1.
func markedCopyInBothSets(t *testing.T) (tc *testCluster, bucket string, marked int) {
	t.Helper()
	takeOverOff(t)
	tc = newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket = bucketOwnedBy(t, tc, 0, "sessions")
	createBucketAt(tc, 0, testDatabase, bucket, 2)
	waitForServing(t, tc, testDatabase, bucket, 2)
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	marked = nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 3)[1])
	if err := tc.nodes[marked].server.core.cluster.markOutside(placementOf(testDatabase, bucket)); err != nil {
		t.Fatalf("marking node %d's copy outside: %v", marked+1, err)
	}
	return tc, bucket, marked
}

// TestAnAlterKeepsTheLineageOfAnOutsideCopyBothSetsPlace is build doc §2 trap
// 3: an increase that keeps a copy in both sets is row 1 by the rings, but a
// copy marked outside lacks entries, so the alter is applied for its placement
// effect only and its lineage stays where it was. Moved to the alter's
// sequence, the copy would claim entries it lacks, and a take-over fallback
// could choose it.
func TestAnAlterKeepsTheLineageOfAnOutsideCopyBothSetsPlace(t *testing.T) {
	tc, bucket, marked := markedCopyInBothSets(t)
	defer tc.stop()
	core := tc.nodes[marked].server.core
	ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
	before, err := core.cluster.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading node %d's copy: %v", marked+1, err)
	}
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
	waitForBarrier(t, tc, 0, "after-the-increase", marked)
	after, err := core.cluster.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading node %d's copy: %v", marked+1, err)
	}
	alterSeq := ownerSeqOfAlter(t, tc, 0, bucket, 3)
	if after.Through != before.Through || after.Through >= alterSeq {
		t.Errorf("the increase moved node %d's outside copy's lineage from through %d to %d (the alter at %d): row 1 was taken on a copy marked outside",
			marked+1, before.Through, after.Through, alterSeq)
	}
	if after.Outside == 0 {
		t.Errorf("the increase cleared node %d's outside mark", marked+1)
	}
}

// TestADropOfABucketAMemberHoldsNothingOfDeletesItsNeedsACopyRecord: a
// member an increase placed holds only its needs-a-copy record when the
// bucket is dropped; the drop, addressed to every member, is applied there as
// a drop of no copy, which deletes the record in its batch (spec §3.3).
func TestADropOfABucketAMemberHoldsNothingOfDeletesItsNeedsACopyRecord(t *testing.T) {
	tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
	defer tc.stop()
	for member := 1; member <= 2; member++ {
		if _, found := needsCopyOnDisk(t, tc.nodes[member].server.core, bucket); !found {
			t.Fatalf("setup: node %d holds no needs-a-copy record of %q", member+1, bucket)
		}
	}
	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	waitForBarrier(t, tc, 0, "after-the-drop", 1, 2)
	for member := 1; member <= 2; member++ {
		requireNoNeedsCopy(t, tc.nodes[member].server.core, bucket, "after the bucket's drop")
	}
}

// TestAnAlterOfANewerTermIsAppliedToAnOutsideCopy is G3-b's deviation 2: the
// term table's exception admits an alter whose term is above an outside
// copy's, and the store's lineage check, judging the same entry, would refuse
// it as of a term the copy has not taken (store.ErrTermAhead): refused, the
// alter stalls the owner's stream to the member for good, since the copy
// takes no newer term before it is copied afresh. The apply is made here as
// the replica path makes it, with the entry's term on the context, a term
// above the copy's.
func TestAnAlterOfANewerTermIsAppliedToAnOutsideCopy(t *testing.T) {
	tc, bucket, marked := markedCopyInBothSets(t)
	defer tc.stop()
	core := tc.nodes[marked].server.core
	key := placementOf(testDatabase, bucket)
	term, _, err := core.bucketLineage(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading node %d's copy's term: %v", marked+1, err)
	}
	st, err := core.bucketStateNamed(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading node %d's copy's state: %v", marked+1, err)
	}
	facts, _ := core.peekSettled(testDatabase, bucket)
	ctx := withEntryTerm(withLineage(context.Background(), st.Through+100, false), term+1, true)
	fence := core.acquireFence(key, fenceExclusive)
	answer, err := core.alterAsMember(ctx, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3, PreviousFactor: 2,
		Epoch: core.cluster.Epoch(), Generation: facts.generation})
	fence.release()
	if err != nil || answer == nil || !answer.Success {
		t.Fatalf("node %d refused an alter of term %d on its outside copy of term %d: %+v, %v", marked+1, term+1, term, answer, err)
	}
	requireHeldAt(t, core, bucket, 3, "after an alter of a newer term on an outside copy")
}
