package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAPullServedOnAnEpochThisNodeHasLeftIsNotLanded: a replica's re-seed is
// served by its owner, and before it lands the membership changes. The copy
// was the source's placement on the old epoch: landed, it would carry no mark
// of the change and follow no owner's term until an entry of a later term
// reached it (the proof test met it as a hand-off that never settled). It is
// discarded, nothing is landed, and the pull answers that it crossed epochs,
// which the sweep asks again. Guard: land it whatever the epoch.
func TestAPullServedOnAnEpochThisNodeHasLeftIsNotLanded(t *testing.T) {
	// No sweep: once the join commits, the owner, which adopts first, would
	// serve the replica's sweep a second, legitimate pull of the bucket while
	// the test's own pull is held, and that pull would meet the hook too.
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	replica := (owner + 1) % len(tc.nodes)
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	puller := clusterOf(tc, replica)
	if err := puller.releaseLocalCopy(testDatabase, testBucket, "for the test"); err != nil {
		t.Fatalf("setup: releasing the replica's copy: %v", err)
	}

	held, release := make(chan struct{}), make(chan struct{})
	var heldOnce sync.Once
	hold := func(database, bucket string) {
		if database == testDatabase && bucket == testBucket {
			heldOnce.Do(func() { close(held) })
			<-release
		}
	}
	beforeMaterializeHook.Store(&hold)
	defer beforeMaterializeHook.Store(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pulled := make(chan error, 1)
	go func() {
		pulled <- puller.pullBucketPlaced(ctx, testDatabase, testBucket, mustNode(t, puller, tc.addrs[owner]), true)
	}()
	select {
	case <-held:
	case <-time.After(30 * time.Second):
		t.Fatal("setup: the replica's pull never reached its materialise")
	}
	before := puller.Epoch()
	ports := reserveNodePorts(t)
	if err := clusterOf(tc, owner).proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	if !holdsWithin(10*time.Second, func() bool { return puller.Epoch() > before }) {
		t.Fatal("setup: the replica never adopted the join")
	}
	close(release)
	err := <-pulled
	if puller.s.bucketStoreOpen(testDatabase, testBucket) {
		t.Fatalf("the replica landed a copy served on epoch %d after it moved to epoch %d (pull answered %v)", before, puller.Epoch(), err)
	}
	if !errors.Is(err, errPullAcrossEpochs) {
		t.Errorf("the pull answered %v; want the refusal of a pull across epochs, which the sweep asks again", err)
	}
}
