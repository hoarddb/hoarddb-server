package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A pull that fails to land forgets the bucket's snapshot position,
// and two callers that waited on that failed pull then pulled at once.

// noSweepNoRetention keeps a node from rebalancing and from pruning on its
// own, so the test's calls are the only pulls and nothing moves a position.
func noSweepNoRetention(cfg *config.Config) {
	cfg.Server.AutoRebalance = false
	cfg.Server.WALRetentionSeconds = 0
}

// TestCallersThatWaitedOnAFailedPullDoNotLoseTheSnapshotPosition:
// Three callers ask a joiner to migrate one bucket: the first pulls and is held
// before it materialises; the other two find it in flight and wait. The first
// pull fails to land (a test seam ends its materialise), which forgets its
// snapshot position. On the starting tree the two waiters then pulled at once
// without either registering: both recorded the snapshot position, one landed,
// and the other, failing on the copy that had just landed, forgot the position
// the first had recorded — under the same key — so the owner's replay applies
// the bucket's history over the copy (the double apply). Every step is
// ordered by the test's hooks; nothing waits on timing. Guard: a waiter whose
// pull failed pulls without registering.
func TestCallersThatWaitedOnAFailedPullDoNotLoseTheSnapshotPosition(t *testing.T) {
	tc := newTestClusterWith(t, 2, 3, func(_ int, cfg *config.Config) { noSweepNoRetention(cfg) })
	defer tc.stop()
	ports := reserveNodePorts(t)
	members := append([]ring.Node{}, tc.nodes[0].server.core.cluster.Members()...)
	after := ringOf(t, append(members, ports.ringNode()), 3)
	bucket := ""
	for i := 0; i < 1000 && bucket == ""; i++ {
		name := fmt.Sprintf("waited%03d", i)
		if owner, err := after.Owner(placementOf(testDatabase, name).encoded); err == nil && owner.ID != ports.ringAddr {
			bucket = name
		}
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	for n := 0; n < 3; n++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", n),
			Data: map[string]interface{}{"n": n}, WriteConcern: "majority"})
	}
	ownerID := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, bucket)[0]
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerID)
	owner := tc.nodes[ownerIndex].server.core.cluster

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := owner.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	// The owner's stream may open and must not carry entries: its replay would
	// create the bucket on the joiner, and an entry at or below the snapshot
	// position would move the joiner's position past it, which drops the
	// position legitimately.
	release := holdStreamEntries(owner.peerFor(ports.ringNode()))
	defer release()
	joinerNode := startJoinTarget(t, tc, ports, tc.token, noSweepNoRetention)
	defer func() { _ = joinerNode.client.Close() }()
	joiner := joinerNode.server.core.cluster
	adoptFrom(t, joiner, owner)

	var arrived atomic.Int32
	firstHeld, releaseFirst := make(chan struct{}), make(chan struct{})
	beforeMaterialize := func(string, string) {
		if arrived.Add(1) == 1 {
			close(firstHeld)
			<-releaseFirst
		}
	}
	// A retry is held as it starts until the other has started too, and each
	// notes how many pulls of the bucket are running when it starts. A tree
	// that runs one retry at a time has no second to wait for; the hook gives
	// up after a few seconds and lets the one retry go.
	var started, mostAtOnce atomic.Int32
	bothStarted := make(chan struct{})
	pullStarted := func(database, name string) {
		joiner.migrateMu.Lock()
		running := int32(len(joiner.pulls[migrateKey(database, name)]))
		joiner.migrateMu.Unlock()
		for {
			most := mostAtOnce.Load()
			if running <= most || mostAtOnce.CompareAndSwap(most, running) {
				break
			}
		}
		switch started.Add(1) {
		case 1:
		case 3:
			close(bothStarted)
		default:
			select {
			case <-bothStarted:
			case <-time.After(3 * time.Second):
			}
		}
	}
	pullStartedHook.Store(&pullStarted)
	defer pullStartedHook.Store(nil)
	beforeMaterializeHook.Store(&beforeMaterialize)
	defer beforeMaterializeHook.Store(nil)
	var failedOnce atomic.Bool
	failFirst := func(uint64) error {
		if failedOnce.CompareAndSwap(false, true) {
			return errors.New("test seam: the first pull does not land")
		}
		return nil
	}
	materializeCatalogHook.Store(&failFirst)
	defer materializeCatalogHook.Store(nil)
	var waiting sync.WaitGroup
	waiting.Add(2)
	// A caller can wait more than once (on the first pull, then on a retry);
	// the test waits for the first two waits.
	var waits atomic.Int32
	waited := func(string, string) {
		if waits.Add(1) <= 2 {
			waiting.Done()
		}
	}
	waitForInFlightPullHook.Store(&waited)
	defer waitForInFlightPullHook.Store(nil)

	results := make(chan error, 3)
	go func() { results <- joiner.ensureMigrated(ctx, testDatabase, bucket) }()
	select {
	case <-firstHeld:
	case err := <-results:
		t.Fatalf("the first pull ended before its materialise: %v", err)
	case <-ctx.Done():
		t.Fatalf("the first pull never reached its materialise")
	}
	for i := 0; i < 2; i++ {
		go func() { results <- joiner.ensureMigrated(ctx, testDatabase, bucket) }()
	}
	waiting.Wait()
	close(releaseFirst)

	var outcomes []error
	for i := 0; i < 3; i++ {
		outcomes = append(outcomes, <-results)
	}
	if !joiner.s.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("the joiner does not hold %s after three migrate calls (%v)", bucket, outcomes)
	}
	if got := joiner.seededThroughFor(ownerID, testDatabase, bucket); got == 0 {
		t.Errorf("the joiner holds %s with no snapshot position from its owner (%d pulls reached their materialise; outcomes %v): "+
			"the owner's replay will apply the bucket's history over the copy", bucket, arrived.Load(), outcomes)
	}
	if got := mostAtOnce.Load(); got > 1 {
		t.Errorf("%d pulls of %s ran on the joiner at once after the first failed (%d started in all): the two callers that "+
			"waited on it each pulled, and a retry that fails forgets the snapshot position the other recorded", got, bucket, started.Load())
	}
}
