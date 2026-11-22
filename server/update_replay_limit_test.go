package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// An update logged before the document limit existed carries no stamped limit,
// and its owner acknowledged it at whatever size it produced. Replay and the
// replica apply path never measure a client's write (server/document_limit.go),
// so they must apply it as the owner did: refusing it would drop an
// acknowledged update, and on a replica leave the two copies different.
//
// The constants below are a stored document of 20 MiB plus an update that adds 12:
// 32 MiB, over the 30 MiB ceiling every node shares.
const (
	storedBeforeUpdate = 20 << 20
	updateAddedBytes   = 12 << 20
)

func unstampedGrowingUpdate(t *testing.T, database, bucket, key string) []byte {
	t.Helper()
	return mustMarshalBSON(t, UpdateRequest{Database: database, Bucket: bucket, Key: key,
		Set: map[string]interface{}{"more": strings.Repeat("y", updateAddedBytes)}})
}

func hasMoreField(t *testing.T, found *FindResponse) bool {
	t.Helper()
	_, present := bson.Raw(found.Data).Lookup("more").StringValueOK()
	return present
}

// TestReplayAppliesAnAcknowledgedUpdateLoggedBeforeTheDocumentLimit: a node
// restarts over a log whose last update was acknowledged at 32 MiB. The replay
// must neither fail to start the node nor drop the update.
func TestReplayAppliesAnAcknowledgedUpdateLoggedBeforeTheDocumentLimit(t *testing.T) {
	s := newTestServer(t)
	w, err := storage.OpenWAL(filepath.Join(t.TempDir(), "wal"), nil)
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	defer func() { _ = w.Close() }()
	attachWALToStartedServer(s, w)

	appendEntry := func(op transport.Opcode, bucket, key string, request any) {
		t.Helper()
		payload, isRaw := request.([]byte)
		if !isRaw {
			payload = mustMarshalBSON(t, request)
		}
		if _, err := w.Append(bucket, byte(op), []byte(key), payload); err != nil {
			t.Fatalf("logging %s: %v", op, err)
		}
	}
	appendEntry(transport.OpCreateDB, "", "", CreateDatabaseRequest{Name: "app"})
	appendEntry(transport.OpCreateBucket, "docs", "", CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})
	appendEntry(transport.OpPut, "docs", "grown", InsertRequest{Database: "app", Bucket: "docs", Key: "grown", Data: docOfEncodedSize(storedBeforeUpdate)})
	appendEntry(transport.OpUpdate, "docs", "grown", unstampedGrowingUpdate(t, "app", "docs", "grown"))

	if err := s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log failed: %v", err)
	}
	found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "grown"})
	if err != nil || !found.Found {
		t.Fatalf("Find after replay: %+v, %v", found, err)
	}
	if !hasMoreField(t, found) {
		t.Error("replay dropped an update its owner had acknowledged: the document is back to its size before the update")
	}
}

// TestReplicaAppliesAnUnstampedUpdateWhateverItsResultingSize is the same
// property on the replication apply path, where an owner that predates the
// limit ships an update with no stamp.
func TestReplicaAppliesAnUnstampedUpdateWhateverItsResultingSize(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	createTestHashBucket(t, tc)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "grown", Data: docOfEncodedSize(storedBeforeUpdate)})
	waitForWALSequence(t, tc.nodes[1], 3)

	replica := tc.nodes[1].server.core
	entry := ReplEntry{
		Op: int32(transport.OpUpdate), Bucket: testBucket, Key: []byte("grown"),
		Payload: unstampedGrowingUpdate(t, testDatabase, testBucket, "grown"),
	}
	resp, err := asReplica(replica.cluster, entry)
	if err != nil {
		t.Fatalf("the replica refused the owner's acknowledged update: %v", err)
	}
	if transport.Opcode(resp.Opcode) == transport.OpError {
		t.Fatalf("the replica answered the owner's acknowledged update with an error: %s", resp.Payload)
	}
	found, err := replica.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "grown"})
	if err != nil || !found.Found || !hasMoreField(t, found) {
		t.Errorf("the replica did not apply the update: %+v, %v", found, err)
	}
}
