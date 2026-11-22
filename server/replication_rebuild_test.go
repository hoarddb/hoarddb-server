package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// rewindStoredPositions sets every "p" record of a node's store back to
// position zero, as the record the stream's handshake wrote stands when no
// checkpoint after it reached the disk. The incarnation it records is kept:
// the handshake writes it, synced, before the first entry is acknowledged, so
// no kill loses it (stream_handshake.go:Cluster.rebaseStream).
func rewindStoredPositions(t *testing.T, cluster *Cluster) {
	t.Helper()
	snapshot := cluster.s.kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSReplication, []byte{positionRecordKind})
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyPrefixEnd(lower)})
	defer iterator.Close()
	batch := cluster.s.kv.NewBatch()
	defer batch.Discard()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		stored, err := iterator.Value()
		if err != nil {
			t.Fatalf("reading a position record: %v", err)
		}
		var record positionRecord
		if err := bson.Unmarshal(stored, &record); err != nil {
			t.Fatalf("decoding a position record: %v", err)
		}
		record.AppliedThrough = 0
		rewound, err := bson.Marshal(record)
		if err != nil {
			t.Fatalf("encoding a position record: %v", err)
		}
		if err := batch.Set(bytes.Clone(iterator.Key()), rewound); err != nil {
			t.Fatalf("rewinding a position record: %v", err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the rewound position records: %v", err)
	}
}

// ownEntriesOf returns the entries of bucket that cluster logged as the owner,
// as the owner would ship them.
func ownEntriesOf(t *testing.T, owner *Cluster, bucket string) []ReplEntry {
	t.Helper()
	logged, err := owner.s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the owner's log: %v", err)
	}
	var shipped []ReplEntry
	for _, entry := range logged {
		if entry.Origin.Kind == storage.WALOriginOwn && entry.Bucket == bucket && transport.Opcode(entry.Op) == transport.OpFIFOPush {
			shipped = append(shipped, ReplEntry{Seq: entry.Seq, Op: int32(entry.Op), Bucket: entry.Bucket, Key: entry.Key, Payload: entry.Data})
		}
	}
	return shipped
}

// pushToReplica sends entries to replica as owner's stream would.
func pushToReplica(t *testing.T, replica *Cluster, ownerID string, entries []ReplEntry) ReplPushResponse {
	t.Helper()
	// An owner ships each entry in its bucket's term (step 9.2), which the
	// replica judges it by (step 10b.2): an entry made here is stamped with
	// the term the replica's copy follows, as its owner's would be.
	stamped := make([]ReplEntry, len(entries))
	for index, entry := range entries {
		if entry.Term == 0 && judgedByLineage(transport.Opcode(entry.Op), entry.Bucket) {
			_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
			if term, _, err := replica.s.bucketLineage(database, entry.Bucket); err == nil {
				entry.Term = term
			}
		}
		stamped[index] = entry
	}
	entries = stamped
	push, err := bson.Marshal(ReplPushRequest{Token: replica.localToken(), FromNode: ownerID, Entries: entries,
		Epoch: replica.Epoch(), Stream: streamOf(t, replica, ownerID)})
	if err != nil {
		t.Fatalf("marshal replication push: %v", err)
	}
	respFrame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), replica.s)
	if err != nil {
		t.Fatalf("replication push: %v", err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(respFrame.Payload, &resp); err != nil {
		t.Fatalf("decode replication ack: %v", err)
	}
	return resp
}

// TestAPositionSurvivesAKillWithNoCheckpoint: a replica killed with no
// checkpoint and no state file rebuilds its position from its own log, so the
// owner's re-send of entries it holds is skipped. On the previous build the
// same shape, with the state file deleted, counted 20.
func TestAPositionSurvivesAKillWithNoCheckpoint(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	const pushes = 10
	for i := 0; i < pushes; i++ {
		if frame := tc.ok(0, transport.OpFIFOPush, PushRequest{
			Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"n": i}, WriteConcern: "all",
		}); frame.Opcode == transport.OpError {
			t.Fatalf("push %d failed: %s", i, string(frame.Payload))
		}
	}
	ownerNode, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex, replicaIndex := nodeIndexByAddr(t, tc.addrs, ownerNode.ID), -1
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != ownerNode.ID {
			replicaIndex = nodeIndexByAddr(t, tc.addrs, id)
			break
		}
	}
	if replicaIndex == -1 {
		t.Fatal("the bucket has no replica other than its owner")
	}
	resend := ownEntriesOf(t, tc.nodes[ownerIndex].server.core.cluster, testBucket)
	if len(resend) != pushes {
		t.Fatalf("the owner logged %d pushes of the bucket, want %d", len(resend), pushes)
	}
	waitForWALSequence(t, tc.nodes[replicaIndex], pushes)

	// Killed: no clean-stop checkpoint, the position in the store back where the
	// handshake left it, no state file.
	replica := tc.nodes[replicaIndex].server.core.cluster
	replica.skipStopCheckpoint = true
	rewindStoredPositions(t, replica)
	dataDir := tc.nodes[replicaIndex].cfg.Storage.DataDir
	if tc.nodes[replicaIndex].client != nil {
		_ = tc.nodes[replicaIndex].client.Close()
		tc.nodes[replicaIndex].client = nil
	}
	if err := tc.nodes[replicaIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", replicaIndex+1, err)
	}
	for _, name := range []string{"replication_state.bson"} {
		if err := os.Remove(filepath.Join(dataDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removing %s: %v", name, err)
		}
	}
	tc.nodes[replicaIndex].server = nil
	tc.nodes[replicaIndex] = tc.startNode(replicaIndex, filepath.Dir(dataDir), tc.token)

	restarted := tc.nodes[replicaIndex].server.core.cluster
	resp := pushToReplica(t, restarted, ownerNode.ID, resend)
	if !resp.Success {
		t.Fatalf("the replica refused the re-send: %s", resp.Error)
	}
	if got := tc.localCount(replicaIndex); got != pushes {
		t.Fatalf("the replica holds %d elements after the owner re-sent %d it already held; want %d", got, pushes, pushes)
	}
}

// commitRefusingKV is a store whose commits fail while refuse is set: the store
// failing, as opposed to an operation the store refuses.
type commitRefusingKV struct {
	storage.KV
	refuse atomic.Bool
}

func (kv *commitRefusingKV) NewBatch() storage.KVBatch {
	return &commitRefusingBatch{KVBatch: kv.KV.NewBatch(), owner: kv}
}

type commitRefusingBatch struct {
	storage.KVBatch
	owner *commitRefusingKV
}

func (batch *commitRefusingBatch) Commit(sync bool) error {
	if batch.owner.refuse.Load() {
		return errors.New("the store refused the commit")
	}
	return batch.KVBatch.Commit(sync)
}

// TestAnEntryWhoseApplyFailedIsCountedAndReplayed is SA-2.1: a replica logs an
// entry before it applies it, so one whose apply fails is in the log and above
// the bucket's applied sequence. The rebuilt position counts it, and the node's
// own replay applies it at the next start, so the owner's re-send, now skipped,
// costs the node nothing.
func TestAnEntryWhoseApplyFailedIsCountedAndReplayed(t *testing.T) {
	dataDir := t.TempDir()
	kv := &commitRefusingKV{KV: kvfault.New()}
	s := newKVFaultServer(dataDir, kv)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "held", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the bucket: %+v, %v", resp, err)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the catalog: %v", err)
	}
	c := retentionTestCluster(t, s, dataDir)
	defer c.cancel()

	// The store fails the commit: the log takes the entry and the apply fails.
	kv.refuse.Store(true)
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "held", Key: "k", Data: map[string]interface{}{"v": "value"}})
	if err != nil {
		t.Fatalf("encoding the put: %v", err)
	}
	const ownerSeq = 77
	if _, err := c.applyAsReplica(context.Background(), ReplEntry{Seq: ownerSeq, Op: int32(transport.OpPut), Bucket: "held", Key: []byte("k"), Payload: payload}, true, "test-owner", false); err == nil {
		t.Fatal("test setup: the apply was expected to fail and did not")
	}
	if s.wal.LastSequence() != 1 {
		t.Fatalf("test setup: the log holds %d entries, want the failed one", s.wal.LastSequence())
	}
	kv.refuse.Store(false)
	if err := s.wal.Close(); err != nil {
		t.Fatalf("closing the log: %v", err)
	}

	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	restartedCluster := retentionTestCluster(t, restarted, dataDir)
	defer restartedCluster.cancel()
	if restartedCluster.shouldApply("test-owner", ownerSeq) {
		t.Fatalf("the rebuilt position of test-owner is below %d: the entry whose apply failed was not counted", ownerSeq)
	}
	if err := restarted.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	found, err := restarted.Find(FindRequest{Database: "app", Bucket: "held", Key: "k"})
	if err != nil {
		t.Fatalf("reading the document: %v", err)
	}
	if !found.Found {
		t.Fatal("the replay did not apply the entry whose apply had failed, yet its re-send is skipped: the node would never hold it (SA-2.1)")
	}
}
