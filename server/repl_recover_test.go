package server

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// dropDatabaseTestName names the database TestReplayedDropDatabaseTakesTheNewGeneration
// drops and re-creates. dropDatabaseOriginalBucket is the bucket the first
// generation holds, so its pending database-drop record has something to
// expand; dropDatabaseTestBucket is the second generation's bucket, the one
// the test's assertions follow. All three are distinct from
// testDatabase/testBucket so the test reads on its own.
const (
	dropDatabaseTestName       = "regen"
	dropDatabaseOriginalBucket = "original"
	dropDatabaseTestBucket     = "survivor"
)

// errReclaimHeldForTest is what blockExpansionOf makes the reclaimer's
// expander fail with, standing in for a lap that has not run yet.
var errReclaimHeldForTest = errors.New("reclaim expansion held for a replayed-drop test")

// blockExpansionOf keeps database's pending drop record from ever being
// expanded (and so deleted): the reclaimer errors on the one bucket entry it
// would convert, every lap, and retries — through the same ctx-aware sleep a
// production shutdown relies on, so Server.Stop still returns promptly. A
// held reclaim lap (blocking in reclaimLapHook, as holdReclaimer does) is
// deliberately not used here: Service.Stop cancels the server's context and
// then waits on the reclaimer's WaitGroup, and a lap blocked unconditionally
// in the hook never observes that cancellation, so Stop hangs forever on the
// node this test restarts.
func blockExpansionOf(t *testing.T, database string) {
	t.Helper()
	hook := func(db, _ string) error {
		if db == database {
			return errReclaimHeldForTest
		}
		return nil
	}
	expandBucketHook.Store(&hook)
	t.Cleanup(func() { expandBucketHook.Store(nil) })
}

// TestReplayedDropDatabaseTakesTheNewGeneration is the acceptance
// test for replayed drops, the reproduction verbatim: dropDatabaseTestName is created, given
// a bucket, and dropped through the cluster (so node 1's WAL holds the
// create and the drop); it is then re-created and given a document directly
// on node 1's core, bypassing dispatch and so the WAL entirely
// (server/dispatch.go's s.dispatch is where every WAL-backed mutation
// funnels through) — standing in for a materialised copy, a migration
// result, or any other direct write the bug file names. Node 1 restarts with
// its WAL still holding the stale OpDeleteDB. Before the fix, replay
// dispatches it unconditionally: DeleteDatabaseCtx recomputes the drop's
// max_id against the *current* allocator mark, which now covers the
// re-created database's bucket too, and the re-created database is dropped a
// second time — its content and its name both gone. After the fix
// (databaseDropAtOrBelowMark, server/repl_recover.go), LoadCatalog (which
// every restart runs before replay — cmd/server/main.go, and now
// server/cluster_replication_test.go:startNode, which had been skipping it)
// attaches the re-created bucket, above the pending drop's max_id, before
// replay starts; the snapshot databaseDropGenerations takes from that state
// tells the gate the database was already re-created, so replay skips the
// stale entry and the re-created database survives the restart with its
// document intact.
func TestReplayedDropDatabaseTakesTheNewGeneration(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	// The pending database-drop record this test's gate depends on must
	// still be on disk, unexpanded, when node 1 restarts.
	blockExpansionOf(t, dropDatabaseTestName)

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: dropDatabaseTestName})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: dropDatabaseTestName, Name: dropDatabaseOriginalBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: dropDatabaseTestName})

	// Re-create the database and give it a document directly on node 1's
	// core, entirely outside the log.
	target := tc.nodes[0].server.core
	if resp, err := target.CreateDatabase(dropDatabaseTestName); err != nil || !resp.Success {
		t.Fatalf("re-creating database %q directly: %+v, %v", dropDatabaseTestName, resp, err)
	}
	if resp, err := target.CreateBucket(CreateBucketRequest{Database: dropDatabaseTestName, Name: dropDatabaseTestBucket, Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("re-creating bucket %q directly: %+v, %v", dropDatabaseTestBucket, resp, err)
	}
	if resp, err := target.Insert(InsertRequest{Database: dropDatabaseTestName, Bucket: dropDatabaseTestBucket, Key: "seeded", Data: map[string]interface{}{"v": "seeded"}}); err != nil || !resp.Success {
		t.Fatalf("writing the seeded document directly: %+v, %v", resp, err)
	}
	if found, err := target.Find(FindRequest{Database: dropDatabaseTestName, Bucket: dropDatabaseTestBucket, Key: "seeded"}); err != nil || !found.Found {
		t.Fatalf("before the restart the re-created database does not hold the seeded key: %+v, %v", found, err)
	}

	restarted := tc.restartNode(0).server.core

	if lookupBucket(restarted, dropDatabaseTestName, dropDatabaseTestBucket) == nil {
		t.Fatalf("after the restart database %q is not served: the replayed drop took the re-created generation with it", dropDatabaseTestName)
	}
	found, err := restarted.Find(FindRequest{Database: dropDatabaseTestName, Bucket: dropDatabaseTestBucket, Key: "seeded"})
	if err != nil {
		t.Fatalf("after the restart, reading the seeded document failed: %v", err)
	}
	if !found.Found {
		t.Fatalf("after the restart the re-created database lost the seeded document: %s", found.Message)
	}
}

// TestOrdinaryDropStaysDroppedAfterReplay guards the replayed-drop fix against the
// regression its first draft had: a database created, given a bucket,
// and dropped — with nothing re-created after it — must stay dropped once
// replay has finished with its whole history, including its own drop.
// Without databaseDropGenerations' snapshot (taken once, before replay
// dispatches anything), a live re-check at the OpDeleteDB entry sees the
// dropped generation's own bucket resurrected by that bucket's own,
// still-undispatched CreateBucket entry replaying later in the same pass
// (service.go:catalogedCreate's "create over a pending expansion" path), and
// wrongly gates a drop that must take effect — leaving a dropped database
// served again after every restart that has not yet reclaimed it.
func TestOrdinaryDropStaysDroppedAfterReplay(t *testing.T) {
	const database, bucket = "ordinarydrop", "bucket1"
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	blockExpansionOf(t, database)

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})

	restarted := tc.restartNode(0).server.core
	if lookupBucket(restarted, database, bucket) != nil {
		t.Fatalf("after the restart, database %q dropped with nothing re-created after it is still served: replay's gate over-skipped its own, legitimate drop", database)
	}
}

// TestMaterialisedCopySurvivesReplayedDrop is the acceptance test for a
// materialised copy, the reproduction verbatim: X is created, written, dropped and re-created
// through the cluster; node 2's X gets a document outside the log; node 1
// drops X locally and pulls it from node 2; node 1 restarts. The replayed
// OpDeleteBucket and OpCreateBucket are at or below the materialised copy's
// applied_seq — its floor is node 1's log position when it was materialised
// (build doc SA-18) — so replay skips them (spec §5.2.2's DDL gate), and the
// document that reached the copy outside the log is still there.
func TestMaterialisedCopySurvivesReplayedDrop(t *testing.T) {
	tc, _ := recreatedAndMaterialised(t, "materialised")
	defer tc.stop()

	restarted := tc.restartNode(0).server.core
	found, err := restarted.Find(FindRequest{Database: testDatabase, Bucket: "materialised", Key: "seeded"})
	if err != nil {
		t.Fatalf("after the restart, reading the seeded document failed: %v", err)
	}
	if !found.Found {
		t.Fatalf("after the restart the materialised copy lost the seeded document: %s", found.Message)
	}
}

// TestARefusedBatchIsNotAppliedByReplay checks, on the one node that
// holds a bucket, that a batch the store refused for its entry count, with the
// client told none of it was written, is in the node's log, and the restart's
// replay must not apply it. Replay writes no read-repair version key, so the
// logged entry that was too many entries live fits when replayed. Two nodes
// at replication factor 1, not one node: a lone node serves its writes
// without the cluster's write path, stamps no version live either, and so
// never refuses what its replay would accept.
func TestARefusedBatchIsNotAppliedByReplay(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	bucket := bucketNameOwnedBy(t, tc.nodes[0].server.core.cluster, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	logged := tc.nodes[0].server.core.wal.LastSequence()
	entryBoundIn(t, tc.frame(0, transport.OpBatchPut, smallDocumentBatch("app", bucket, 60000)))
	waitForWALSequence(t, tc.nodes[0], logged+1)
	if held := tc.bucketCountOn(0, "app", bucket); held != 0 {
		t.Fatalf("after the refusal the owner holds %d documents, want 0", held)
	}

	tc.restartNode(0)
	if held := tc.bucketCountOn(0, "app", bucket); held != 0 {
		t.Fatalf("after the owner restarted it holds %d documents of a batch it refused, want 0: replay applied the refused entry", held)
	}
}
