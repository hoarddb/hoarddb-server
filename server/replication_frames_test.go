package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// replicationNamespaceDump reads every record of the replication namespace and
// the number of stored versions the scan touched. A write of a key, even with
// the value it already had, adds a version, so the pair is a count of writes
// as well as a picture of the contents.
func replicationNamespaceDump(t *testing.T, kv storage.KV) (contents string, versions int64) {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	lower, upper := replicationRange()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: upper, IncludeDeleted: true})
	defer iterator.Close()
	var lines []string
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		stored, err := iterator.Value()
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("reading a replication record: %v", err)
		}
		lines = append(lines, fmt.Sprintf("%x=%x", iterator.Key(), stored))
	}
	return strings.Join(lines, "\n"), iterator.Visits()
}

// dataDirListing lists every file under dir except the log and the node store,
// with size and modification time.
func dataDirListing(t *testing.T, dir string) string {
	t.Helper()
	var lines []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if entry.IsDir() && (relative == "wal" || relative == nodeStoreDirName) {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		lines = append(lines, fmt.Sprintf("%s %d %d", relative, info.Size(), info.ModTime().UnixNano()))
		return nil
	})
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// fifoCluster is a cluster with one FIFO bucket written to, and the nodes that
// matter: the owner, and a replica that is not the owner.
type fifoCluster struct {
	tc           *testCluster
	ownerIndex   int
	replicaIndex int
	ownerID      string
}

func newFIFOCluster(t *testing.T, pushes int) *fifoCluster {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	for i := 0; i < pushes; i++ {
		if frame := tc.ok(0, transport.OpFIFOPush, PushRequest{
			Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("push %d failed: %s", i, string(frame.Payload))
		}
	}
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	found := &fifoCluster{tc: tc, ownerIndex: nodeIndexByAddr(t, tc.addrs, owner.ID), replicaIndex: -1, ownerID: owner.ID}
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			found.replicaIndex = nodeIndexByAddr(t, tc.addrs, id)
			break
		}
	}
	if found.replicaIndex == -1 {
		t.Fatal("the bucket has no replica other than its owner")
	}
	waitForWALSequence(t, tc.nodes[found.replicaIndex], uint64(pushes))
	return found
}

func (f *fifoCluster) replica() *Cluster { return f.tc.nodes[f.replicaIndex].server.core.cluster }

// TestZeroSequenceEntryDoesNotRewriteTheReplicationStateFile covers this
// scenario. A frame that moves no position used to rewrite the state file; now
// there is no such file, and the store takes no write under the namespace.
func TestZeroSequenceEntryDoesNotRewriteTheReplicationStateFile(t *testing.T) {
	cluster := newFIFOCluster(t, 3)
	replica := cluster.replica()
	contentsBefore, versionsBefore := replicationNamespaceDump(t, replica.s.kv)

	resp := pushToReplica(t, replica, cluster.ownerID, []ReplEntry{
		{Seq: 0, Op: int32(transport.OpFIFOPush), Bucket: testBucket, Payload: putPayload(t, testDatabase, testBucket, "zero")},
	})
	if !resp.Success {
		t.Fatalf("the replica refused a zero-sequence entry: %s", resp.Error)
	}
	if _, err := os.Stat(legacyStatePathOf(replica)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the replication state file exists after a zero-sequence frame (stat: %v)", err)
	}
	contentsAfter, versionsAfter := replicationNamespaceDump(t, replica.s.kv)
	if contentsAfter != contentsBefore || versionsAfter != versionsBefore {
		t.Errorf("a zero-sequence frame wrote to the replication namespace (versions %d -> %d)", versionsBefore, versionsAfter)
	}
}

// TestAReplicatedFrameWritesNoPositionRecord is the acceptance test for position cost: with
// 1,000 and then 10,000 snapshot positions recorded, one applied frame creates
// or rewrites no file outside the log and the store, and writes nothing to the
// replication namespace — the cost does not scale with the number of positions.
func TestAReplicatedFrameWritesNoPositionRecord(t *testing.T) {
	cluster := newFIFOCluster(t, 3)
	replica := cluster.replica()
	dataDir := replica.s.config.Storage.DataDir
	template := ownEntriesOf(t, cluster.tc.nodes[cluster.ownerIndex].server.core.cluster, testBucket)[0]

	recorded := 0
	nextSeq := template.Seq + 1000
	for _, positions := range []int{1_000, 10_000} {
		for ; recorded < positions; recorded++ {
			if err := replica.recordSeeded("ghost-source", "ghost-db", fmt.Sprintf("bucket-%05d", recorded), uint64(recorded+1)); err != nil {
				t.Fatalf("recording position %d: %v", recorded, err)
			}
		}
		listingBefore := dataDirListing(t, dataDir)
		contentsBefore, versionsBefore := replicationNamespaceDump(t, replica.s.kv)
		time.Sleep(20 * time.Millisecond) // a rewrite within the clock's granularity still changes the size or name

		frame := template
		frame.Seq = nextSeq
		nextSeq++
		if resp := pushToReplica(t, replica, cluster.ownerID, []ReplEntry{frame}); !resp.Success || resp.AppliedThrough != frame.Seq {
			t.Fatalf("with %d positions the frame was not applied: %+v", positions, resp)
		}
		if listingAfter := dataDirListing(t, dataDir); listingAfter != listingBefore {
			t.Errorf("with %d positions one applied frame changed the files of the data directory:\nbefore:\n%s\nafter:\n%s", positions, listingBefore, listingAfter)
		}
		contentsAfter, versionsAfter := replicationNamespaceDump(t, replica.s.kv)
		if contentsAfter != contentsBefore || versionsAfter != versionsBefore {
			t.Errorf("with %d positions one applied frame wrote to the replication namespace (versions %d -> %d)", positions, versionsBefore, versionsAfter)
		}
	}
}

// killedAfterPowerLoss restarts the node over the same store and log, as a power
// cut leaves them: what was committed without a sync is gone.
func killedAfterPowerLoss(t *testing.T, dataDir string, kv *kvfault.Store, killed *Server) *Cluster {
	t.Helper()
	kv.PowerLoss()
	if err := killed.wal.Close(); err != nil {
		t.Fatalf("closing the log: %v", err)
	}
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	cluster := retentionTestCluster(t, restarted, dataDir)
	t.Cleanup(cluster.cancel)
	return cluster
}

func newReplicaOnFaultStore(t *testing.T) (*Server, *kvfault.Store, *Cluster, string) {
	t.Helper()
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	// "sealer" is written as its owner, the others as a replica: a node that
	// applies a bucket as a replica has not taken it over, and refuses to
	// write it as an owner (step 10b.3).
	for name, bucketType := range map[string]BucketType{"held": BucketTypeHash, "queue": BucketTypeFIFO, "sealer": BucketTypeHash} {
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: bucketType}); err != nil || !resp.Success {
			t.Fatalf("creating bucket %q: %+v, %v", name, resp, err)
		}
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	t.Cleanup(c.cancel)
	return s, kv, c, dataDir
}

// TestACheckpointIsDurableBeforeTheLogIsTruncated: a replica applies entries, a
// retention pass prunes its own log past them, the power goes, and the owner
// re-sends. The position must be in the store before the log entries that
// carried it are gone, or the re-send is applied a second time.
func TestACheckpointIsDurableBeforeTheLogIsTruncated(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	s.wal.SetMaxSegmentBytes(1024)
	const applied = 5
	document := strings.Repeat("x", 700)
	for seq := uint64(1); seq <= applied; seq++ {
		payload := putPayload(t, "app", "held", fmt.Sprintf("k%d", seq))
		if _, err := c.applyAsReplica(context.Background(), ReplEntry{Seq: seq, Op: int32(transport.OpPut), Bucket: "held", Key: []byte("k"), Payload: payload}, true, "test-owner", false); err != nil {
			t.Fatalf("applying %d: %v", seq, err)
		}
		c.markApplied("test-owner", seq)
	}
	if err := putThrough(c, asOwner, "sealer", "filler", document); err != nil {
		t.Fatalf("the write that seals the segments: %v", err)
	}
	if err := c.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	if first, err := s.wal.FirstSequence(); err != nil || first <= applied {
		t.Fatalf("test setup: the log still holds sequence %d (first %d, %v): nothing was truncated past the applied entries", applied, first, err)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if restarted.shouldApply("test-owner", applied) {
		t.Fatalf("after the power loss the position of test-owner is below %d: the checkpoint was not durable before the log entries were truncated", applied)
	}
	if !restarted.shouldApply("test-owner", applied+1) {
		t.Fatalf("after the power loss the position of test-owner is above %d", applied)
	}
}

// TestASkipUnderASnapshotIsCheckpointedBeforeTheAcknowledgement: a frame whose
// every entry is covered by a snapshot position moves the stream position with
// no log entry. The node is killed right after it answers; it must not report a
// lower position than the one it acknowledged.
func TestASkipUnderASnapshotIsCheckpointedBeforeTheAcknowledgement(t *testing.T) {
	_, kv, c, dataDir := newReplicaOnFaultStore(t)
	const floor = 40
	if err := c.recordSeeded("test-owner", "app", "held", floor); err != nil {
		t.Fatalf("recording the snapshot position: %v", err)
	}
	resp := pushToReplica(t, c, "test-owner", []ReplEntry{
		{Seq: floor - 5, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered")},
		{Seq: floor - 1, Op: int32(transport.OpPut), Bucket: "held", Payload: putPayload(t, "app", "held", "covered2")},
	})
	if !resp.Success || resp.AppliedThrough != floor-1 {
		t.Fatalf("the covered entries were not acknowledged through %d: %+v", floor-1, resp)
	}
	restarted := killedAfterPowerLoss(t, dataDir, kv, c.s)
	if restarted.shouldApply("test-owner", floor-1) {
		t.Fatalf("after a kill right behind the acknowledgement the position of test-owner is below %d", floor-1)
	}
}

// TestAnOwnerLandedPushSurvivesAKillWithoutADuplicate is BUG-0208's closing
// test. A stream push that lands on the bucket's owner is logged before the
// acknowledgement, and the node is killed after that append and before any
// checkpoint. If the owner re-originated the entry, its log carries this node's
// own origin and the start rebuild cannot give the sender's position back, so
// the re-send is applied a second time and the FIFO gains an element. A push
// applied as a replica carries the sender's sequence in its origin, the rebuild
// recovers the position from the log, and the re-send is skipped.
//
// The kill is simulated where the existing kill tests put it: the store's
// position record is rewound to what a kill before the checkpoint leaves
// (rewindStoredPositions, as TestAPositionSurvivesAKillWithNoCheckpoint does),
// then the power goes and the node starts over the surviving log.
func TestAnOwnerLandedPushSurvivesAKillWithoutADuplicate(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	const (
		oldOwner = "old-owner"
		pushes   = 4
	)
	if !c.ownsBucket(placementOf("app", "queue")) {
		t.Fatal("test setup: the node does not own app/queue, so the push would not land on an owner")
	}
	stream := make([]ReplEntry, 0, pushes)
	for seq := uint64(1); seq <= pushes; seq++ {
		payload, err := bson.Marshal(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"n": seq}})
		if err != nil {
			t.Fatalf("encoding push %d: %v", seq, err)
		}
		stream = append(stream, ReplEntry{Seq: seq, Op: int32(transport.OpFIFOPush), Bucket: "queue", Payload: payload})
	}
	if resp := pushToReplica(t, c, oldOwner, stream); !resp.Success || resp.AppliedThrough != pushes {
		t.Fatalf("the old owner's pushes were not acknowledged through %d: %+v", pushes, resp)
	}
	// Killed in the window between the log append and the checkpoint the owner
	// owed for the re-originated entry: no durable position survives it.
	rewindStoredPositions(t, c)

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log after the kill: %v", err)
	}
	if resp := pushToReplica(t, restarted, oldOwner, stream); !resp.Success {
		t.Fatalf("the re-send was refused: %s", resp.Error)
	}
	counted, err := restarted.s.Count(CountRequest{Database: "app", Bucket: "queue"})
	if err != nil {
		t.Fatalf("counting the queue: %v", err)
	}
	if counted.Count != pushes {
		t.Fatalf("the queue holds %d elements after a kill before the checkpoint and a re-send of the %d acknowledged pushes; want %d, a duplicate element",
			counted.Count, pushes, pushes)
	}
}
