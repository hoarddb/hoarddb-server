package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// servedBucketOnThreeNodes is a bucket every node of a three-node cluster
// already serves, from the stream, so any pull of it by a replica stops at
// "already served".
func servedBucketOnThreeNodes(t *testing.T) (tc *testCluster, ownerIndex int, owner *Cluster, key string) {
	t.Helper()
	tc = newTestCluster(t, 3, 3)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex = ownerIndexOf(t, tc, testBucket)
	return tc, ownerIndex, clusterOf(tc, ownerIndex), migrateKey(testDatabase, testBucket)
}

func pullWithin(t *testing.T, replica, owner *Cluster) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := replica.pullBucket(ctx, testDatabase, testBucket, owner.self); err != nil {
		t.Fatalf("the pull of a bucket this node already serves failed, want a clean stop: %v", err)
	}
}

// TestAPullThatStopsAsAlreadyServedFreesTheOwnersSnapshot is BUG-0730: the
// stop returned before MIGRATE_DONE, so the owner kept the snapshot it built
// for the pull until pendingSnapshotIdle (an hour) freed it.
func TestAPullThatStopsAsAlreadyServedFreesTheOwnersSnapshot(t *testing.T) {
	tc, ownerIndex, owner, key := servedBucketOnThreeNodes(t)
	replica := clusterOf(tc, (ownerIndex+1)%len(tc.nodes))

	pullWithin(t, replica, owner)

	if held := owner.pendingCount(key); held != 0 {
		t.Fatalf("the pull stopped because the bucket is already served and the owner still holds %d pending snapshots of it; "+
			"they stay until pendingSnapshotIdle (%s)", held, pendingSnapshotIdle)
	}
}

// TestEarlyReleaseOfAServedPullDoesNotDoubleReleaseOrTouchOthers: two replicas
// stop at the same time and a third snapshot, held for a puller still
// copying, must survive; a second MIGRATE_DONE for a released transfer is
// refused, not a panic or a second release.
func TestEarlyReleaseOfAServedPullDoesNotDoubleReleaseOrTouchOthers(t *testing.T) {
	tc, ownerIndex, owner, key := servedBucketOnThreeNodes(t)
	first := clusterOf(tc, (ownerIndex+1)%len(tc.nodes))
	second := clusterOf(tc, (ownerIndex+2)%len(tc.nodes))

	// A snapshot held for a puller that is mid-copy: a made-up puller id, so
	// no real pull replaces it.
	const midCopyPuller = "mid-copy-puller"
	owner.setPending(key, &pendingSnapshot{target: midCopyPuller, transfer: "mid-copy"})

	var wg sync.WaitGroup
	for _, replica := range []*Cluster{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pullWithin(t, replica, owner)
		}()
	}
	wg.Wait()

	owner.migrateMu.Lock()
	remaining := len(owner.migratePending[key])
	_, midCopyHeld := owner.migratePending[key][midCopyPuller]
	owner.migrateMu.Unlock()
	if remaining != 1 || !midCopyHeld {
		t.Fatalf("after two served-stop pulls the owner holds %d snapshots (mid-copy one held: %v), want only the mid-copy one", remaining, midCopyHeld)
	}

	// The same pull again, after its snapshot is gone, is a fresh start that
	// frees its own snapshot again: the count never goes below the others'.
	pullWithin(t, first, owner)
	if held := owner.pendingCount(key); held != 1 {
		t.Fatalf("a repeated served-stop pull left %d snapshots, want the mid-copy one only", held)
	}
	owner.clearPending(key, midCopyPuller)
	owner.clearPending(key, midCopyPuller)
	if held := owner.pendingCount(key); held != 0 {
		t.Fatalf("clearing the last snapshot twice left %d held", held)
	}
}
