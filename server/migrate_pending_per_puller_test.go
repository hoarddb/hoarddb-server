package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ownerWithTwoAwaitingMembers is a bucket on node 0 raised from one copy to
// three, with auto-rebalance off so the test is the only puller (BUG-0676).
type ownerWithTwoAwaitingMembers struct {
	t      *testing.T
	tc     *testCluster
	owner  *Server
	bucket string
	key    string
}

func newOwnerWithTwoAwaitingMembers(t *testing.T) *ownerWithTwoAwaitingMembers {
	t.Helper()
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	putDocument(tc, 0, testDatabase, bucket, "doc")
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	return &ownerWithTwoAwaitingMembers{t: t, tc: tc, owner: tc.nodes[0].server.core, bucket: bucket, key: migrateKey(testDatabase, bucket)}
}

func (o *ownerWithTwoAwaitingMembers) start(member int) MigrateStartResponse {
	o.t.Helper()
	incarnation := o.owner.wal.Incarnation()
	payload, err := bson.Marshal(MigrateStartRequest{Token: o.owner.cluster.localToken(), Database: testDatabase, Bucket: o.bucket,
		FromNode: o.tc.addrs[member], Epoch: o.owner.cluster.Epoch(), Store: o.tc.nodes[member].server.core.storeClaim(),
		Incarnation: incarnation[:]})
	if err != nil {
		o.t.Fatalf("encoding node %d's MIGRATE_START: %v", member+1, err)
	}
	frame, err := handleMigrateStart(context.Background(), transport.NewFrame(transport.OpMigrateStart, payload), o.owner)
	if err != nil {
		o.t.Fatalf("the owner's handler failed node %d's MIGRATE_START: %v", member+1, err)
	}
	var started MigrateStartResponse
	if err := bson.Unmarshal(frame.Payload, &started); err != nil || !started.Ok {
		o.t.Fatalf("the owner refused node %d's MIGRATE_START: %+v, %v", member+1, started, err)
	}
	return started
}

// pull reads the snapshot's one chunk and returns the digest a puller sends.
func (o *ownerWithTwoAwaitingMembers) chunk(member int, started MigrateStartResponse) (MigrateDataResponse, string) {
	o.t.Helper()
	payload, err := bson.Marshal(MigrateDataRequest{Token: o.owner.cluster.localToken(), Database: testDatabase, Bucket: o.bucket,
		Index: 0, Transfer: started.Transfer})
	if err != nil {
		o.t.Fatalf("encoding node %d's MIGRATE_DATA: %v", member+1, err)
	}
	frame, err := handleMigrateData(context.Background(), transport.NewFrame(transport.OpMigrateData, payload), o.owner)
	if err != nil {
		o.t.Fatalf("the owner's handler failed node %d's MIGRATE_DATA: %v", member+1, err)
	}
	var served MigrateDataResponse
	if err := bson.Unmarshal(frame.Payload, &served); err != nil {
		o.t.Fatalf("decoding node %d's MIGRATE_DATA answer: %v", member+1, err)
	}
	digest := sha256.Sum256(served.Data)
	return served, hex.EncodeToString(digest[:])
}

func (o *ownerWithTwoAwaitingMembers) done(member int, started MigrateStartResponse, digest string) MigrateDoneResponse {
	o.t.Helper()
	payload, err := bson.Marshal(MigrateDoneRequest{Token: o.owner.cluster.localToken(), Database: testDatabase, Bucket: o.bucket,
		Digest: digest, Transfer: started.Transfer})
	if err != nil {
		o.t.Fatalf("encoding node %d's MIGRATE_DONE: %v", member+1, err)
	}
	frame, err := handleMigrateDone(context.Background(), transport.NewFrame(transport.OpMigrateDone, payload), o.owner)
	if err != nil {
		o.t.Fatalf("the owner's handler failed node %d's MIGRATE_DONE: %v", member+1, err)
	}
	var finished MigrateDoneResponse
	if err := bson.Unmarshal(frame.Payload, &finished); err != nil {
		o.t.Fatalf("decoding node %d's MIGRATE_DONE answer: %v", member+1, err)
	}
	return finished
}

// TestTheFirstPullersDoneDoesNotDropTheSecondPullersSnapshot guards the other
// half of BUG-0676: MIGRATE_DONE cleared the bucket's one snapshot, so
// whichever pull finished first dropped the other's.
func TestTheFirstPullersDoneDoesNotDropTheSecondPullersSnapshot(t *testing.T) {
	o := newOwnerWithTwoAwaitingMembers(t)
	first, second := o.start(1), o.start(2)
	_, firstDigest := o.chunk(1, first)
	if finished := o.done(1, first, firstDigest); !finished.Ok {
		t.Fatalf("node 2's MIGRATE_DONE was refused: %s", finished.Error)
	}
	if held := o.owner.cluster.pendingCount(o.key); held != 1 {
		t.Errorf("after node 2's pull finished the owner holds %d snapshots of the bucket, want node 3's one", held)
	}
	served, secondDigest := o.chunk(2, second)
	if !served.Ok {
		t.Fatalf("node 2 finishing dropped node 3's snapshot; its chunk was refused: %s", served.Error)
	}
	if finished := o.done(2, second, secondDigest); !finished.Ok {
		t.Errorf("node 3's MIGRATE_DONE was refused: %s", finished.Error)
	}
	if held := o.owner.cluster.pendingCount(o.key); held != 0 {
		t.Errorf("both pulls finished and the owner still holds %d snapshots of the bucket", held)
	}
}

// TestAPullerThatPullsAgainReplacesItsOwnSnapshotOnly keeps the entries of a
// bucket bounded by its pullers: a restarted pull leaves no snapshot behind.
func TestAPullerThatPullsAgainReplacesItsOwnSnapshotOnly(t *testing.T) {
	o := newOwnerWithTwoAwaitingMembers(t)
	o.start(1)
	other := o.start(2)
	o.start(1)
	o.start(1)
	if held := o.owner.cluster.pendingCount(o.key); held != 2 {
		t.Errorf("two pullers, one of them pulling three times: the owner holds %d snapshots, want 2", held)
	}
	if served, _ := o.chunk(2, other); !served.Ok {
		t.Errorf("node 3's pull was replaced by node 2 pulling again: %s", served.Error)
	}
}

// TestAnAbandonedPullersSnapshotIsFreedWhenItsIdleTimeRunsOut covers the pull
// whose puller went away without MIGRATE_DONE: nothing else frees the snapshot.
func TestAnAbandonedPullersSnapshotIsFreedWhenItsIdleTimeRunsOut(t *testing.T) {
	previous := pendingSnapshotIdle
	pendingSnapshotIdle = 200 * time.Millisecond
	t.Cleanup(func() { pendingSnapshotIdle = previous })
	o := newOwnerWithTwoAwaitingMembers(t)
	o.start(1)
	active := o.start(2)

	// Node 3 keeps asking, so only node 2's snapshot goes.
	deadline := time.Now().Add(5 * time.Second)
	for o.owner.cluster.pendingCount(o.key) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the abandoned pull's snapshot was not freed: %d held", o.owner.cluster.pendingCount(o.key))
		}
		if served, _ := o.chunk(2, active); !served.Ok {
			t.Fatalf("the active pull's chunk was refused: %s", served.Error)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if served, _ := o.chunk(2, active); !served.Ok {
		t.Errorf("the active pull lost its snapshot with the abandoned one: %s", served.Error)
	}
	time.Sleep(3 * pendingSnapshotIdle)
	if held := o.owner.cluster.pendingCount(o.key); held != 0 {
		t.Errorf("a pull nobody asked about for 3 idle times is still held: %d snapshots", held)
	}
}

// TestAnExpiredTimerOfAReplacedSnapshotLeavesTheNewerSnapshotHeld covers a
// timer that fired just as its snapshot was replaced: its callback is blocked
// on migrateMu, runs after the replacement, and must not free the newer one.
func TestAnExpiredTimerOfAReplacedSnapshotLeavesTheNewerSnapshotHeld(t *testing.T) {
	o := newOwnerWithTwoAwaitingMembers(t)
	cluster := o.owner.cluster
	puller := o.tc.addrs[1]
	o.start(1)
	cluster.migrateMu.Lock()
	replaced := cluster.migratePending[o.key][puller]
	cluster.migrateMu.Unlock()
	newer := o.start(1)

	cluster.expirePending(o.key, replaced)

	if held := cluster.pendingCount(o.key); held != 1 {
		t.Fatalf("the replaced snapshot's expiry freed the newer snapshot: %d held, want 1", held)
	}
	if served, _ := o.chunk(1, newer); !served.Ok {
		t.Errorf("the newer snapshot's chunk was refused after the replaced one expired: %s", served.Error)
	}
}
