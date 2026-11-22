package server

import (
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Restores in the window (on-demand pull build doc step 11; spec §7; BUG-0340
// criterion 2): a restore at the bucket's new owner, while the owner it
// replaced was removed and its copy has not landed, is refused with retry or
// waits for the copy; it never creates a bucket beside the one the cluster
// holds, and the original data remain.

// restoreAtTheSuccessor removes the owner of a bucket holding "held" on a
// three-node cluster at factor rf, sends a merge RESTORE_START for it to the
// node that now owns it, and returns that answer and the successor's index.
func restoreAtTheSuccessor(t *testing.T, rf int, prefix string) (*testCluster, *transport.Frame, int, string) {
	t.Helper()
	return restoreAtTheSuccessorWith(t, rf, prefix, func(int, *config.Config) {})
}

// restoreAtTheSuccessorWith is restoreAtTheSuccessor on a cluster whose
// configuration the caller adjusts.
func restoreAtTheSuccessorWith(t *testing.T, rf int, prefix string, adjust func(int, *config.Config)) (*testCluster, *transport.Frame, int, string) {
	t.Helper()
	tc, successor, bucket := removedOwnersSuccessor(t, rf, prefix, adjust)
	frame := tc.frame(successor, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	return tc, frame, successor, bucket
}

// removedOwnersSuccessor builds a three-node cluster whose node 0 owned a hash
// bucket holding "held", then is removed; the successor has been told, and the
// third node follows. It returns the cluster, the successor's index and the
// bucket's name, with nothing yet restored.
func removedOwnersSuccessor(t *testing.T, rf int, prefix string, adjust func(int, *config.Config)) (*testCluster, int, string) {
	t.Helper()
	tc := newTestClusterWith(t, 3, rf, adjust)
	t.Cleanup(tc.stop)
	before := ringWith(t, rf, tc.addrs...)
	after := ringWith(t, rf, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, prefix, func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, rf)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	successor := nodeIndexByAddr(t, tc.addrs, ownerOn(after, bucket))
	other := 3 - successor
	removeAndTell(t, tc, successor, 0)
	via := clusterOf(tc, successor)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	return tc, successor, bucket
}

// TestARestoreInARemovalWindowNeverLandsBesideTheBucket: at factor 1 the
// removed owner holds the bucket's only copy, and the successor holds
// nothing. Guard: the members' and removed nodes' question before a restore
// creates a bucket.
func TestARestoreInARemovalWindowNeverLandsBesideTheBucket(t *testing.T) {
	tc, frame, successor, bucket := restoreAtTheSuccessor(t, 1, "restleave")
	neverLandedBeside(t, tc, frame, successor, bucket)
}

// TestARestoreInARemovalWindowAtAReplicaSuccessor: at factor 2 the successor
// holds a replica it has not taken over. Guard: the holder check.
func TestARestoreInARemovalWindowAtAReplicaSuccessor(t *testing.T) {
	tc, frame, successor, bucket := restoreAtTheSuccessor(t, 2, "restrepl")
	neverLandedBeside(t, tc, frame, successor, bucket)
}

// restoreIntoTheRemovedOwnersCopy runs a whole restore of one document, "restored",
// at the removed owner's successor with auto_rebalance off, and returns the
// start answer. The dump declares an index and a store field the bucket the
// removed owner held does not have.
func restoreIntoTheRemovedOwnersCopy(t *testing.T, tc *testCluster, successor int, bucket string, drop bool) RestoreStartResponse {
	t.Helper()
	startFrame := restoreStartPastTheHandoffRetry(t, tc, successor, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
		Indexes: []string{"age"}, Stores: []DumpStore{{Name: "Purchases", Engine: "fifo"}}, Drop: drop,
	})
	if startFrame.Opcode == transport.OpError {
		t.Fatalf("the restore start was refused: %s", errorFrameText(startFrame))
	}
	var started RestoreStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &started); err != nil {
		t.Fatalf("decoding the restore start answer: %v", err)
	}
	restoredDocument, err := bson.Marshal(map[string]interface{}{"_id_": "restored", "age": 7})
	if err != nil {
		t.Fatalf("encoding the restored document: %v", err)
	}
	tc.ok(successor, transport.OpRestoreChunk, RestoreChunkRequest{
		Database: testDatabase, Bucket: bucket, SessionID: started.SessionID, Data: restoredDocument,
	})
	tc.ok(successor, transport.OpRestoreDone, RestoreDoneRequest{
		Database: testDatabase, Bucket: bucket, SessionID: started.SessionID, Documents: 1,
	})
	return started
}

// restoreStartPastTheHandoffRetry sends req until the successor stops answering
// the hand-off's "Retry in a moment", which the door gives while the
// placement pass after the removal is still running. The retry is the
// documented answer and these tests are about what follows it.
func restoreStartPastTheHandoffRetry(t *testing.T, tc *testCluster, successor int, req RestoreStartRequest) *transport.Frame {
	t.Helper()
	var frame *transport.Frame
	holdsWithin(takeOverWait, func() bool {
		frame = tc.frame(successor, transport.OpRestoreStart, req)
		return frame.Opcode != transport.OpError || !strings.Contains(errorFrameText(frame), "Retry in a moment")
	})
	return frame
}

// declaresPurchasesFifo says whether the bucket at node index declares the
// dump's store field.
func declaresPurchasesFifo(t *testing.T, tc *testCluster, index int, bucket string) bool {
	t.Helper()
	var info BucketInfoResponse
	frame := tc.ok(index, transport.OpBucketInfo, BucketInfoRequest{Database: testDatabase, Bucket: bucket})
	if err := bson.Unmarshal(frame.Payload, &info); err != nil {
		t.Fatalf("decoding the bucket info: %v", err)
	}
	return slices.Contains(info.StoreFields, StoreFieldInfo{Name: "Purchases", Engine: "fifo"})
}

// TestARestoreIntoACopyATakeOverLandsMergesAsIntoAnExistingBucket is owner
// ruling R10 (revised). With auto_rebalance off and factor 1, a restore at the
// successor of a removed owner finds the bucket not held. The create it sends
// gives the take-over leave to copy the removed owner's bucket here and is
// answered "already exists". The restore must then take the bucket-exists
// branch as if it had held the bucket from the start: Created false, the dump's
// store field declared, its index reported missing, and the original data kept
// beside the restored.
// Guard: the settle before the catalog lookup in startRestore; without it the
// restore reports Created and skips the declarations and the missing indexes.
func TestARestoreIntoACopyATakeOverLandsMergesAsIntoAnExistingBucket(t *testing.T) {
	tc, successor, bucket := removedOwnersSuccessor(t, 1, "restmerge",
		func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	started := restoreIntoTheRemovedOwnersCopy(t, tc, successor, bucket, false)
	if started.Created {
		t.Errorf("the restore reports it created %s, which the removed owner's copy already was", bucket)
	}
	if !slices.Equal(started.MissingIndexes, []string{"age"}) {
		t.Errorf("missing indexes = %v, want [age]: the dump's index was not compared with the copy's", started.MissingIndexes)
	}
	if !declaresPurchasesFifo(t, tc, successor, bucket) {
		t.Errorf("the dump's store field Purchases (fifo) was not declared on the copy")
	}
	for _, key := range []string{"held", "restored"} {
		if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, key) }) {
			t.Errorf("%q does not read back through the successor after the restore", key)
		}
	}
}

// TestARestoreWithDropIntoACopyATakeOverLandsDropsAndRecreatesIt is R10
// (revised) with --drop: the copy the create's take-over lands is dropped and
// recreated as the exists branch does, so the data only the removed owner held
// is gone and the restored data is present, with the dump's declarations.
// Guard: the same settle in startRestore; without it --drop drops nothing.
func TestARestoreWithDropIntoACopyATakeOverLandsDropsAndRecreatesIt(t *testing.T) {
	tc, successor, bucket := removedOwnersSuccessor(t, 1, "restdrop",
		func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	started := restoreIntoTheRemovedOwnersCopy(t, tc, successor, bucket, true)
	if !started.Dropped {
		t.Errorf("--drop did not drop the removed owner's copy")
	}
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "restored") }) {
		t.Errorf("\"restored\" does not read back after the drop and recreate")
	}
	if foundThrough(tc, successor, testDatabase, bucket, "held") {
		t.Errorf("\"held\" survived --drop: the copy was not dropped")
	}
	if !declaresPurchasesFifo(t, tc, successor, bucket) {
		t.Errorf("the recreated bucket does not declare the dump's store field Purchases")
	}
}

// TestARestoreOfAnotherTypeIntoACopyATakeOverLandsIsRefused is R10 (revised)'s
// guard on the type: the copy is a hash bucket, so a btree dump is refused
// with the exists branch's mismatch, and the copy is left as it was.
// Guard: the same settle in startRestore, which lets the type check run.
func TestARestoreOfAnotherTypeIntoACopyATakeOverLandsIsRefused(t *testing.T) {
	tc, successor, bucket := removedOwnersSuccessor(t, 1, "restkind",
		func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	frame := restoreStartPastTheHandoffRetry(t, tc, successor, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeBTree, Bytes: 1,
	})
	if frame.Opcode != transport.OpError || !strings.Contains(errorFrameText(frame), "the dump is type btree") {
		t.Fatalf("a btree dump into a hash copy was not refused for its type: opcode %v, %q", frame.Opcode, errorFrameText(frame))
	}
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, successor, testDatabase, bucket, "held") }) {
		t.Errorf("\"held\" does not read back after the refused restore")
	}
}
