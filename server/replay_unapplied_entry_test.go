package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestReplayRestoresAnUnappliedEntryBelowALaterAppliedOne pins what replay
// does with the state once argued about: an entry of a bucket that was
// appended to the WAL and never applied, below a later entry of the same
// bucket that was.
//
// It is kept, with its assertions, and its hole is now built the way the live
// system can build it (build doc SA-29): a write through Cluster.applyAsOwner
// whose store apply fails at the commit (store.ApplyFaultHook). Its apply
// ticket closes on the failure, so the later write advances the bucket's
// applied_seq past it, and the hole is represented by the bucket's failed_seqs
// (spec §4.4 rule 5): replay applies a recorded entry beneath the mark. The
// earlier form appended the entry with a bare Cluster.appendWAL — no ticket, no
// apply, no failure — which Step 7 made unreachable: every append for a bucket
// opens an apply ticket first, so only a crash leaves an entry appended and
// unapplied, and a crash stops the later writer too. Its negative proof is the
// break "Gate ignores FailedSeqs".
func TestReplayRestoresAnUnappliedEntryBelowALaterAppliedOne(t *testing.T) {
	// Two nodes because a one-node ring runs in single-node mode, which has no
	// owner write path; RF=1 so the owner alone decides what is readable.
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner of %q: %v", testBucket, err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)

	// The slow writer's half: appended, its apply failed at the commit.
	failed := failNextApplyOn(t, tc.nodes[ownerIdx].server.core.kv, anySeq)
	tc.writeConcerned(ownerIdx, "unapplied", "majority")
	unappliedSeq := failed.Load()
	if unappliedSeq == 0 {
		t.Fatal("the unapplied write's apply never reached the store; the test built no hole")
	}
	if frame := tc.writeConcerned(ownerIdx, "applied", "majority"); frame.Opcode == transport.OpError {
		t.Fatalf("write the later entry: %s", string(frame.Payload))
	}
	laterSeq := tc.nodes[ownerIdx].server.core.wal.LastSequence()
	if laterSeq <= unappliedSeq {
		t.Fatalf("the later write took seq %d, not above the unapplied entry's %d", laterSeq, unappliedSeq)
	}
	if found, _ := tc.readFromNode(ownerIdx, "unapplied"); found {
		t.Fatal("the unapplied entry is readable before any replay; the test did not build the hole it means to")
	}

	tc.restartNode(ownerIdx)

	if found, _ := tc.readFromNode(ownerIdx, "applied"); !found {
		t.Fatalf("after replay the later write (seq %d) is missing", laterSeq)
	}
	if found, msg := tc.readFromNode(ownerIdx, "unapplied"); !found {
		t.Fatalf("after replay the entry at seq %d, below the applied seq %d, is missing (%s): replay skipped a WAL entry that never applied",
			unappliedSeq, laterSeq, msg)
	}
}
