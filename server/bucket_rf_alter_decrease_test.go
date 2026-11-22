package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The decrease's delivery (bucket-replication-rev8-spec §5.1, §8): the alter
// is addressed to the union of the sets before and after, so the members a
// decrease demotes are sent it. T-164, which pinned R10's refusal of every
// lower factor, was deleted with R10 (G4 to G7 build doc step 5).

// TestADecreaseReachesADemotedNodeAfterAnOwnerRestart is T-33 (build doc
// OQ-7): a decrease from 3 to 2, with the node it demotes down, is delivered
// to it from the owner's log after the owner restarts, because the replay
// addresses the alter by its own row of entryFactor, the union of the sets
// before and after. Addressed at the new factor only, the demoted node would
// never receive it.
func TestADecreaseReachesADemotedNodeAfterAnOwnerRestart(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	demoted := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, tc.nodes[0].server.core.cluster, testDatabase, bucket, 3)[2])
	startDemoted := stopNode(t, tc, demoted)

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
	tc.restartNode(0)
	startDemoted()

	waitForWALCount(t, tc, demoted, transport.OpAlterBucket, bucket, 1)
}

// The decrease through a client (bucket-replication-rev8-spec §4.3, §8; build
// doc step 5). G5 removed R10, the refusal that kept every lower factor off
// users until the release of the demoted copies was built, so a decrease is
// judged by R11's coverage and nothing else of its own.

// TestALowerThroughAClientSessionIsAccepted is step 5's own test: with no test
// switch, a decrease from 3 to 2 sent through a client session is accepted. It
// is sent to the node it demotes, so the coordinator's path and the owner's
// both judge it; it is logged once, on the owner, and the demoted copy is
// released by the sweep's own path. Red while R10 refuses it ("cannot be
// lowered yet").
func TestALowerThroughAClientSessionIsAccepted(t *testing.T) {
	shortenSweepRearm(t, 200*time.Millisecond)
	ds := newDecreaseSet(t, 3, nil)
	defer ds.tc.stop()
	coordinator := ds.walk[2]
	answer := lowerThroughClientUntilAnswered(t, ds, coordinator, 2)
	if !answer.Success || answer.ReplicationFactor != 2 || answer.PreviousFactor != 3 {
		t.Fatalf("the decrease from 3 to 2 through node %d's client session was answered %+v, want success at 2 from 3",
			coordinator+1, answer)
	}
	if logged := walOpCount(t, ds.tc.nodes[0].server.core.wal, transport.OpAlterBucket, ds.bucket); logged != 1 {
		t.Errorf("the owner logged %d alters of %q, want 1", logged, ds.bucket)
	}
	if factor := clusterOf(ds.tc, 0).effectiveRF(placementOf(testDatabase, ds.bucket)); factor != 2 {
		t.Errorf("the owner places %q at %d after the decrease, want 2", ds.bucket, factor)
	}
	requireReleased(t, ds)

}

// lowerThroughClientUntilAnswered sends the decrease to factor through node
// index's client session, as a driver does, until it is answered with a body:
// an UNAVAILABLE refusal (R11 while a survivor catches up after the create, or
// R7) is retried for up to thirty seconds. A success: false body is returned,
// so R10's refusal fails the caller at once.
func lowerThroughClientUntilAnswered(t *testing.T, ds decreaseSet, index int, factor int32) AlterBucketResponse {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		frame := ds.tc.frame(index, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: ds.bucket, ReplicationFactor: factor})
		if frame.Opcode != transport.OpError {
			var answer AlterBucketResponse
			if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
				t.Fatalf("decoding the alter's answer: %v", err)
			}
			if !answer.Success {
				t.Fatalf("the decrease of %q to %d through node %d's client session was refused: %q", ds.bucket, factor, index+1, answer.Message)
			}
			return answer
		}
		if time.Now().After(deadline) {
			t.Fatalf("the decrease of %q through node %d's client session was never answered: %s", ds.bucket, index+1, refusalOf(t, frame).Message)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
