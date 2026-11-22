package server

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The snapshot's clear (bucket-replication-rev8-spec §7.3; G3 build doc step
// 2): the awaiting record of the member a snapshot seeds is deleted inside the
// snapshot's exclusive fence, in a synced batch, and dropped from the map
// before the fence is released. An entry logged before the snapshot is in it;
// one logged after reads the map after the delete and is routed to the
// member, so the member, once it has landed its copy, holds every write
// exactly once.

// pendingOnOneMember is a three-node cluster at default 1 with auto-rebalance
// off everywhere, and a bucket owned by node 1 created at 2 and raised to 3:
// its copy is pending on member alone, and every write keeps its majority on
// the two copies that hold it.
func pendingOnOneMember(t *testing.T) (tc *testCluster, bucket string, member int) {
	t.Helper()
	tc = newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	names, member := pendingOn(t, tc, 1, "sessions")
	bucket = names[0]
	createBucketAt(tc, 0, testDatabase, bucket, 2)
	waitForServing(t, tc, testDatabase, bucket, 2)
	answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if len(answer.Pending) != 1 || answer.Pending[0] != tc.addrs[member] {
		t.Fatalf("the increase names %v as pending, want only node %d", answer.Pending, member+1)
	}
	return tc, bucket, member
}

// migrateInBackground runs the member's pull of bucket, as 'migrate' does, on
// a goroutine, and returns the channel its result arrives on.
func migrateInBackground(c *Cluster, bucket string) <-chan error {
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pulled <- c.ensureMigrated(ctx, testDatabase, bucket)
	}()
	return pulled
}

// awaitMigrated fails the test unless the pull returns, and returns without
// an error, in time.
func awaitMigrated(t *testing.T, pulled <-chan error, what string) {
	t.Helper()
	select {
	case err := <-pulled:
		if err != nil {
			t.Fatalf("%s failed: %v", what, err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("%s never returned", what)
	}
}

// requireHeldLocally fails unless node s's own copy of bucket holds every key
// in keys, and holds no other document.
func requireHeldLocally(t *testing.T, s *Server, bucket string, keys []string) {
	t.Helper()
	var missing []string
	for _, key := range keys {
		found, err := s.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
		if err != nil {
			t.Fatalf("reading %q from %s's own copy of %q: %v", key, s.cluster.self.ID, bucket, err)
		}
		if !found.Found {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%s's own copy of %q lacks %d of %d acknowledged writes: %v", s.cluster.self.ID, bucket, len(missing), len(keys), missing)
	}
	if held := localCountOf(t, s, bucket); held != int64(len(keys)) {
		t.Fatalf("%s's own copy of %q holds %d documents, want the %d acknowledged writes", s.cluster.self.ID, bucket, held, len(keys))
	}
}

// awaitExclusiveFencePending waits until an exclusive hold of the bucket
// placed by key is pending or held on node s: a shared hold that does not wait
// is then refused, so every write sent after it queues behind the hold.
func awaitExclusiveFencePending(t *testing.T, s *Server, key placementKey, what string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		probe := s.tryAcquireFenceShared(key)
		if probe == nil {
			return
		}
		probe.release()
		if time.Now().After(deadline) {
			t.Fatalf("%s was never pending on %s's fence of %q", what, s.cluster.self.ID, key.encoded)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestTheAwaitingRecordIsClearedInsideTheSnapshotFence is T-172. A write W is
// held at beforeLocalApplyHook, inside its fence, while the member's pull asks
// for its snapshot, so the snapshot's exclusive fence waits for W. Once that
// exclusive hold is pending, eight more writes are sent: they queue behind it
// and enter their fence when the snapshot releases it. The snapshot is then
// held at afterSnapshotFenceReleasedHook until every queued write is
// acknowledged, so a clear placed anywhere after the fence runs after they
// have read the awaiting map, every run (BUG-0594). Nothing else is written
// until the copy lands. The member's own storage then holds every
// acknowledged write: W is in the snapshot, and the queued writes read the map
// after the clear, so they are routed to the member, which refuses them until
// its copy lands and is then sent them again. A record cleared after the
// snapshot's fence is released routes the queued writes without the member,
// above the snapshot's high-water, and no stall replays them to it.
func TestTheAwaitingRecordIsClearedInsideTheSnapshotFence(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	owner := clusterOf(tc, 0)
	keys := []string{"before"}
	putDocument(tc, 0, testDatabase, bucket, "before")

	held, release := holdAt(t, &beforeLocalApplyHook, owner.self.ID, transport.OpPut, bucket)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	write := sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held", Data: map[string]interface{}{"n": 1}})
	awaitHeld(t, held, "the write")
	keys = append(keys, "held")
	snapshotted, releaseSnapshot := holdBucketAt(t, &afterSnapshotFenceReleasedHook, bucket)
	pulled := migrateInBackground(clusterOf(tc, member), bucket)
	awaitExclusiveFencePending(t, tc.nodes[0].server.core, placementOf(testDatabase, bucket), "the snapshot's exclusive hold")
	var queued []<-chan backgroundAnswer
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("queued-%d", i)
		queued = append(queued, sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: map[string]interface{}{"n": i}}))
		keys = append(keys, key)
	}
	close(release)
	awaitHeld(t, snapshotted, "the snapshot, its fence released")
	awaitAnswer(t, write, "the held write")
	for i, answered := range queued {
		awaitAnswer(t, answered, fmt.Sprintf("queued write %d", i))
	}
	releaseSnapshot()
	awaitMigrated(t, pulled, fmt.Sprintf("node %d's migrate of %q", member+1, bucket))
	putDocument(tc, 0, testDatabase, bucket, "after")
	keys = append(keys, "after")

	waitForBarrier(t, tc, 0, "barrier", member)
	requireHeldLocally(t, tc.nodes[member].server.core, bucket, keys)
	if pending := awaitingInMap(owner, placementOf(testDatabase, bucket)); len(pending) != 0 {
		t.Errorf("after node %d's copy landed the owner's awaiting map still holds %v", member+1, pending)
	}
	if onDisk := awaitingOnDisk(t, tc.nodes[0].server.core, placementOf(testDatabase, bucket)); len(onDisk) != 0 {
		t.Errorf("after node %d's copy landed the owner's store still holds awaiting records for %v", member+1, onDisk)
	}
}

// TestAWriteHeldBeforeTheFenceAcrossASeedingSnapshotReachesTheSeededMember is
// T-181 (A F-1). A write W is held at beforeOwnerFenceHook, before its fence,
// while the member's seeding snapshot runs, clears the member's record, and
// the copy lands. Released, W computes its addressees inside its fence, after
// the clear, so it is routed to the member, which holds it in its own storage
// with the writes that follow.
func TestAWriteHeldBeforeTheFenceAcrossASeedingSnapshotReachesTheSeededMember(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	owner := clusterOf(tc, 0)
	putDocument(tc, 0, testDatabase, bucket, "before")

	held, release := holdAt(t, &beforeOwnerFenceHook, owner.self.ID, transport.OpPut, bucket)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	write := sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "w", Data: map[string]interface{}{"n": 1}})
	awaitHeld(t, held, "the write")
	awaitMigrated(t, migrateInBackground(clusterOf(tc, member), bucket), fmt.Sprintf("node %d's migrate of %q", member+1, bucket))
	close(release)
	awaitAnswer(t, write, "the held write")
	for i := 0; i < 3; i++ {
		putDocument(tc, 0, testDatabase, bucket, fmt.Sprintf("later-%d", i))
	}

	waitForBarrier(t, tc, 0, "barrier", member)
	requireHeldLocally(t, tc.nodes[member].server.core, bucket, []string{"before", "w", "later-0", "later-1", "later-2"})
}

// TestASnapshotWithoutAPullerIdClearsNothing: a snapshot that names no puller
// seeds no member this node can name, so it clears no awaiting record (spec
// §7.3, "a pull without FromNode clears nothing"): the record and the map
// entry stay. Clearing every member's record in its place would route the
// bucket's entries to members that hold nothing. It is asked for directly, as
// every caller but the handler asks, and through the handler by a
// MIGRATE_START without from_node that names the source's own log
// incarnation, which refuseAPullerNotYetStreamedTo lets through.
func TestASnapshotWithoutAPullerIdClearsNothing(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)
	want := []string{tc.addrs[member]}
	requireKept := func(how string) {
		t.Helper()
		if onDisk := awaitingOnDisk(t, owner, key); !slices.Equal(onDisk, want) {
			t.Errorf("after %s the owner's store holds awaiting records for %v, want %v", how, onDisk, want)
		}
		if inMemory := sortedAwaiting(owner.cluster, key); !slices.Equal(inMemory, want) {
			t.Errorf("after %s the owner's awaiting map holds %v, want %v", how, inMemory, want)
		}
	}
	if _, err := owner.cluster.snapshotBucket(testDatabase, bucket, ""); err != nil {
		t.Fatalf("the owner's snapshot of %q that names no puller failed: %v", bucket, err)
	}
	requireKept("a snapshot that names no puller")

	incarnation := owner.wal.Incarnation()
	payload, err := bson.Marshal(MigrateStartRequest{Token: owner.cluster.localToken(), Database: testDatabase, Bucket: bucket,
		Epoch: owner.cluster.Epoch(), Store: owner.storeClaim(), Incarnation: incarnation[:]})
	if err != nil {
		t.Fatalf("encoding the MIGRATE_START: %v", err)
	}
	frame, err := handleMigrateStart(context.Background(), transport.NewFrame(transport.OpMigrateStart, payload), owner)
	if err != nil {
		t.Fatalf("the owner's handler failed a MIGRATE_START without from_node: %v", err)
	}
	var started MigrateStartResponse
	if err := bson.Unmarshal(frame.Payload, &started); err != nil || !started.Ok {
		t.Fatalf("the owner refused a MIGRATE_START without from_node that names its own incarnation: %+v, %v", started, err)
	}
	clearAllPending(owner.cluster, migrateKey(testDatabase, bucket))
	requireKept("a MIGRATE_START without from_node")
}
