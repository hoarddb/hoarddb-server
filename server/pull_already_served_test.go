package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAPullDoesNotRecordItsPositionUnderACreateInFlight is the other order of
// the same loss: the stream's create of the bucket has passed the replica's
// "is a pull of it pending" check (refuseUnservable) and has not applied yet
// when a pull starts. The pull, reading "not served", would record its
// snapshot position; the create then applies, the writes behind it pass as
// held by a snapshot whose materialise is refused as already served, and the
// position is forgotten. The bucket's fence, which the create holds from that
// check to its apply, is what keeps the pull's check and its record on one
// side of the create. The create is held in that window by a hook; the pull is
// started; three writes are made; the create is let go. Guard: the served
// check without the fence.
func TestAPullDoesNotRecordItsPositionUnderACreateInFlight(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, replicaIndex)

	admitted := make(chan struct{})
	releaseCreate := make(chan struct{})
	var once sync.Once
	hold := func(c *Cluster, entry ReplEntry) {
		if c == replica && transport.Opcode(entry.Op) == transport.OpCreateBucket && entry.Bucket == testBucket {
			once.Do(func() {
				close(admitted)
				<-releaseCreate
			})
		}
	}
	afterReplicaAdmittedHook.Store(&hold)
	defer afterReplicaAdmittedHook.Store(nil)
	releaseCreateOnce := sync.OnceFunc(func() { close(releaseCreate) })
	defer releaseCreateOnce()

	created := make(chan *transport.Frame, 1)
	go func() {
		created <- tc.frame(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket,
			Type: BucketTypeHash})
	}()
	select {
	case <-admitted:
	case <-time.After(20 * time.Second):
		t.Fatal("the replica's create never passed its pending-pull check")
	}
	if frame := <-created; frame.Opcode == transport.OpError {
		t.Fatalf("creating the bucket: %s", frame.Payload)
	}
	// Written before the pull, so its snapshot holds them and its position
	// covers them; their pushes to the replica wait behind the held create.
	var held []string
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("held%d", i)
		putValue(t, tc, ownerIndex, key, "v", "one")
		held = append(held, key)
	}

	reachedMaterialize := make(chan struct{})
	releasePull := make(chan struct{})
	holdPull := func(database, bucket string) {
		if database == testDatabase && bucket == testBucket {
			close(reachedMaterialize)
			<-releasePull
		}
	}
	beforeMaterializeHook.Store(&holdPull)
	defer beforeMaterializeHook.Store(nil)
	releasePullOnce := sync.OnceFunc(func() { close(releasePull) })
	defer releasePullOnce()
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pulled <- replica.pullBucket(ctx, testDatabase, testBucket, owner.self)
	}()
	// The pull either waits on the fence the create holds, or — without it —
	// records its position and reaches its materialise.
	select {
	case <-reachedMaterialize:
	case <-time.After(2 * time.Second):
	}
	releaseCreateOnce()
	caughtUpWithin(t, owner.peerFor(mustNode(t, owner, tc.addrs[replicaIndex])), 20*time.Second)
	releasePullOnce()
	select {
	case err := <-pulled:
		t.Logf("the pull ended: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the pull never ended")
	}

	for _, key := range held {
		if !holdsKeyLocally(tc.nodes[replicaIndex], testBucket, key) {
			t.Errorf("the replica does not hold %q, written while a pull of the bucket started under the stream's create of it: "+
				"the pull recorded a snapshot position the create's bucket made it pass the write over for", key)
		}
	}
}

// TestAPullOfABucketTheStreamAlreadyServesSkipsNothing: a pull that starts
// once the stream has already given this node the bucket — its create arrived
// first — must not record the snapshot's position. Recorded, every entry of
// the stream at or below it is passed over as held by the snapshot, while the
// snapshot never lands (the bucket is already served here) and its position
// is then forgotten: the entries passed over are acknowledged and never
// applied. T-100 met this 2 times in 121 on the merged tree of dispatch 9 —
// the owner's replay of the create reached the entering node between its
// "do I hold it" check and its pull. Here the order is built with hooks: the
// owner's pushes to a replica are held while three writes are made, the
// replica pulls the bucket it already holds and is held before its
// materialise, the pushes are let through, then the materialise.
func TestAPullOfABucketTheStreamAlreadyServesSkipsNothing(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, replicaIndex)
	toReplica := owner.peerFor(mustNode(t, owner, tc.addrs[replicaIndex]))
	for i := 0; i < 2; i++ {
		putValue(t, tc, ownerIndex, fmt.Sprintf("before%d", i), "v", "all")
	}

	unfreeze := freezePeer(toReplica)
	defer unfreeze()
	var held []string
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("held%d", i)
		putValue(t, tc, ownerIndex, key, "v", "one")
		held = append(held, key)
	}

	reached := make(chan struct{})
	releasePull := make(chan struct{})
	hold := func(database, bucket string) {
		if database == testDatabase && bucket == testBucket {
			close(reached)
			<-releasePull
		}
	}
	beforeMaterializeHook.Store(&hold)
	defer beforeMaterializeHook.Store(nil)
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pulled <- replica.pullBucket(ctx, testDatabase, testBucket, owner.self)
	}()
	ended := false
	select {
	case <-reached:
	case err := <-pulled:
		// A pull that sees the bucket served and stops is the fix; the
		// stream then delivers what it holds back.
		t.Logf("the pull ended before its materialise: %v", err)
		ended = true
	case <-time.After(20 * time.Second):
		t.Fatal("the pull never reached its materialise")
	}
	unfreeze()
	caughtUpWithin(t, toReplica, 20*time.Second)
	close(releasePull)
	if !ended {
		select {
		case err := <-pulled:
			t.Logf("the pull of a bucket this node serves ended: %v", err)
		case <-time.After(20 * time.Second):
			t.Fatal("the pull never ended")
		}
	}

	for _, key := range held {
		if !holdsKeyLocally(tc.nodes[replicaIndex], testBucket, key) {
			t.Errorf("the replica does not hold %q, which its owner's stream delivered while a pull of the bucket it already served was in flight: "+
				"the entry was passed over as held by a snapshot that never landed", key)
		}
	}
}
