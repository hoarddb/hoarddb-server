package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.8, the replicated-create question: a bucket a replica
// creates from its owner's stream commits its catalog batch unsynced
// (nodestore.go:Server.commitBucketCreate, batch.Commit(false)), and the
// replica then acknowledges the create. An earlier defect was a pulled copy confirmed
// before it was durable; this asks whether a replicated create is the same.
// It is not, if the replica's log holds the create durably before the
// acknowledgement and a replay re-applies a create whose catalog batch a power
// loss took.

// TestAReplicatedCreateSurvivesAPowerLossOnceAcknowledged: a replica — a fault
// store that loses every batch committed without a sync — applies a create and
// a put from its owner's stream, along applyAsReplica, and acknowledges both.
// The power goes, and takes the create's catalog batch. Restarted on the same
// store and log, its replay brings the bucket back, in the owner's
// incarnation, with the put. So the unsynced commit is safe, and the test pins
// why: applyAsReplica appends the entry to the log, and AppendFrom returns only
// after its fsync, before the create is applied or acknowledged; a pull
// has no log entry behind it, which is why it is synced. The fault
// store cannot lose the log's fsync, which storage's WAL tests guard. Guard:
// replay passes over a create the replica logged — the bucket is gone.
func TestAReplicatedCreateSurvivesAPowerLossOnceAcknowledged(t *testing.T) {
	const database, bucket, incarnation = "app", "replicated", uint64(0x1234_5678_9abc)
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	if _, err := s.CreateDatabase(database); err != nil {
		t.Fatalf("creating the database: %v", err)
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the database's catalog: %v", err)
	}
	live, err := s.liveGenerations(database)
	if err != nil || len(live) != 1 {
		t.Fatalf("reading the database's generation: %v %v", live, err)
	}
	c := retentionTestCluster(t, s, dataDir)
	defer c.cancel()

	create, err := bson.Marshal(bson.D{
		{Key: "database", Value: database}, {Key: "name", Value: bucket}, {Key: "type", Value: string(BucketTypeHash)},
		{Key: "db_gen", Value: live[0]}, {Key: "dropped_generations", Value: []int64{}},
		{Key: "term", Value: int64(InitialEpoch)}, {Key: "incarnation", Value: int64(incarnation)},
	})
	if err != nil {
		t.Fatal(err)
	}
	put, err := bson.Marshal(InsertRequest{Database: database, Bucket: bucket, Key: "k", Data: map[string]interface{}{"v": 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []ReplEntry{
		{Seq: 1, Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: create, OwnerOutcome: ownerOutcomeApplied},
		{Seq: 2, Op: int32(transport.OpPut), Bucket: bucket, Key: []byte("k"), Payload: put, OwnerOutcome: ownerOutcomeApplied, Term: InitialEpoch},
	} {
		resp, err := c.applyAsReplica(context.Background(), entry, true, "test-owner", true)
		if err != nil || transport.Opcode(resp.Opcode) == transport.OpError {
			t.Fatalf("the replica's apply of %s: %s %v", transport.Opcode(entry.Op), resp.Payload, err)
		}
	}
	if !s.bucketStoreOpen(database, bucket) {
		t.Fatal("setup: the replica does not serve the bucket it created")
	}

	// The replica has acknowledged the create. The power goes.
	kv.PowerLoss()
	if err := s.wal.Close(); err != nil {
		t.Fatalf("closing the WAL: %v", err)
	}
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	lostTheBatch := !restarted.bucketStoreOpen(database, bucket)
	w, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("reopening the WAL: %v", err)
	}
	defer func() { _ = w.Close() }()
	restarted.wal = w
	if err := restarted.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the WAL: %v", err)
	}
	if !lostTheBatch {
		t.Fatal("setup: the create's catalog batch survived the power loss, so the replay is not what restores it")
	}
	if !restarted.bucketStoreOpen(database, bucket) {
		t.Fatalf("after a power loss the replica no longer serves bucket %q, whose create it had acknowledged", bucket)
	}
	if got := lineageOf(t, restarted, database, bucket).Incarnation; got != incarnation {
		t.Errorf("the replayed create is incarnation %d, want the owner's %d", got, incarnation)
	}
	if resp, err := restarted.Find(FindRequest{Database: database, Bucket: bucket, Key: "k"}); err != nil || !resp.Found {
		t.Errorf("after a power loss the acknowledged put into %q is missing: %v", bucket, err)
	}
}
