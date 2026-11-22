package server

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The alter's write concern (bucket-replication-rev8-spec §9; build doc step
// 2.4). The alter is routed to the union of its two sets, and its concern
// counts only the copies that keep the bucket: the set at the smaller factor,
// without the owner. The write-concern timeout is shortened so a ship gives up
// in two seconds (replShipTimeout is half of it).

// alterConcernCluster is five nodes at default 1 and a bucket at 2 owned by
// the first, with the survivor: the other member of its set at 2, the one copy
// the alter's concern counts.
func alterConcernCluster(t *testing.T) (tc *testCluster, bucket string, survivor int) {
	t.Helper()
	tc = newTestClusterTuned(t, 5, 1, nil, func(_ int, c *Cluster) { c.wcTimeout = 4 * time.Second })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket = bucketOwnedBy(t, tc, 0, "sessions")
	createBucketAt(tc, 0, testDatabase, bucket, 2)
	waitForServing(t, tc, testDatabase, bucket, 2)
	survivor = nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, tc.nodes[0].server.core.cluster, testDatabase, bucket, 5)[1])
	return tc, bucket, survivor
}

// TestAnAltersConcernCountsOnlyCopiesThatKeepIt is T-182a (A F-4, RF-6): an
// increase from 2 to 5 with the survivor's stream held for less than the ship
// timeout, so it stays in sync and has not applied the alter. Counted over the
// union, the alter's majority is met by the owner and two members that hold
// nothing, and a take-over by the survivor would revert it. Counted over the
// copies that keep it, the alter waits for the survivor. Then the owner is
// removed, and the survivor, which takes the bucket over from its own copy,
// stores and lists the factor the client was answered.
func TestAnAltersConcernCountsOnlyCopiesThatKeepIt(t *testing.T) {
	tc, bucket, survivor := alterConcernCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	hold := owner.replShipTimeout() / 2

	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[survivor])))
	alter := sendInBackground(t, owner, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 5})
	select {
	case answer := <-alter:
		unfreeze()
		t.Fatalf("the alter was answered while the one copy that keeps the bucket had not applied it: %s", answer.payload())
	case <-time.After(hold):
	}
	if facts, _ := tc.nodes[survivor].server.core.peekSettled(testDatabase, bucket); facts.replicationFactor != 2 {
		unfreeze()
		t.Fatalf("the survivor applied the alter while its stream was held: its record declares %d", facts.replicationFactor)
	}
	unfreeze()
	var answered AlterBucketResponse
	select {
	case answer := <-alter:
		if answer.err != nil || answer.frame.Opcode == transport.OpError {
			t.Fatalf("the alter failed: %v %s", answer.err, answer.payload())
		}
		if err := bson.Unmarshal(answer.frame.Payload, &answered); err != nil || !answered.Success {
			t.Fatalf("the alter's answer: %+v, %v", answered, err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the alter was never answered once the survivor's stream was released")
	}
	survivorCore := tc.nodes[survivor].server.core
	if facts, _ := survivorCore.peekSettled(testDatabase, bucket); facts.replicationFactor != answered.ReplicationFactor {
		t.Fatalf("the alter was answered at %d while the survivor's record declares %d", answered.ReplicationFactor, facts.replicationFactor)
	}

	// The owner is stopped, then removed: the survivor's copy is the only
	// one left that holds the bucket's writes, so the take-over lands from it,
	// on whichever member the ring now names, with the factor its record holds.
	ownerID := owner.self.ID
	stopNode(t, tc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := survivorCore.cluster.proposeRemoval(ctx, ownerID); err != nil {
		t.Fatalf("removing the owner: %v", err)
	}
	newOwner, err := survivorCore.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("resolving the new owner: %v", err)
	}
	newOwnerIndex := nodeIndexByAddr(t, tc.addrs, newOwner.ID)
	newOwnerCore := tc.nodes[newOwnerIndex].server.core
	waitTakenOver(t, newOwnerCore, testDatabase, bucket)
	if factor := newOwnerCore.cluster.effectiveRF(placementOf(testDatabase, bucket)); factor != int(answered.ReplicationFactor) {
		t.Errorf("after the take-over the new owner places the bucket at %d; the client was answered %d", factor, answered.ReplicationFactor)
	}
	var info BucketInfoResponse
	if err := bson.Unmarshal(tc.ok(newOwnerIndex, transport.OpBucketInfo, BucketInfoRequest{Database: testDatabase, Bucket: bucket}).Payload, &info); err != nil {
		t.Fatalf("decoding the bucket's listing row: %v", err)
	}
	if info.Bucket.ReplicationFactor != int(answered.ReplicationFactor) || info.Bucket.Inherited {
		t.Errorf("after the take-over 'show buckets' lists factor %d (inherited %v); the client was answered %d",
			info.Bucket.ReplicationFactor, info.Bucket.Inherited, answered.ReplicationFactor)
	}
}

// TestAnAlterWithEverySurvivorStalledIsAnsweredOnTheOwnerAlone is T-182b, a
// freeze of §9's stated residual (RF-6): with the survivor's stream held
// longer than the ship timeout, the survivor is stalled and out of the in-sync
// set, so D-2 resolves the alter's majority over the owner alone and answers
// it once the ship gives up. Its answer carries no copies_confirmed: it is DDL
// (spec §4.2). It fails if a later change counts stalled survivors or adds a
// count to the answer without updating §4.2.
func TestAnAlterWithEverySurvivorStalledIsAnsweredOnTheOwnerAlone(t *testing.T) {
	tc, bucket, survivor := alterConcernCluster(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster

	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[survivor])))
	defer unfreeze()
	started := time.Now()
	alter := sendInBackground(t, owner, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 5})
	var answer backgroundAnswer
	select {
	case answer = <-alter:
	case <-time.After(20 * time.Second):
		t.Fatal("the alter was never answered with its only survivor stalled")
	}
	took := time.Since(started)
	if answer.err != nil || answer.frame.Opcode == transport.OpError {
		t.Fatalf("the alter failed: %v %s", answer.err, answer.payload())
	}
	if took < owner.replShipTimeout() {
		t.Errorf("the alter was answered after %s, before the survivor's ship could give up (%s)", took, owner.replShipTimeout())
	}
	if facts, _ := tc.nodes[survivor].server.core.peekSettled(testDatabase, bucket); facts.replicationFactor != 2 {
		t.Errorf("the stalled survivor's record declares %d; it was held and cannot have applied the alter", facts.replicationFactor)
	}
	if _, err := bson.Raw(answer.frame.Payload).LookupErr("copies_confirmed"); err == nil {
		t.Errorf("the alter's answer carries copies_confirmed: %s", answer.frame.Payload)
	}
}
