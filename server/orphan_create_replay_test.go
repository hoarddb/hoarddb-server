package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAnOrphanCreateSettlesWhenACededWriteFollowsIt is the verifier's F2
// scene for BUG-0528's fix: a create of one bucket and a write to another
// both read their ownership on the owner, a join moves both buckets, the
// create is answered (an orphan), and then the write is logged in the owner's
// term and finds its bucket moved. Delivering that write to the members that
// stay in its bucket's set must not deliver the orphaned create to anyone:
// the hand-off settles (auto-rebalance on, as by default). Without the
// replay of the orphaned create the verifier saw 12 of 12 settle.
func TestAnOrphanCreateSettlesWhenACededWriteFollowsIt(t *testing.T) {
	lowerSweepBounds(t, time.Minute)
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	joiner := reserveJoinerPlaced(t, func(joiner string) bool {
		return joinerFrom(t, "orphan", tc.addrs, joiner, 0) && joinerFrom(t, "cededd", tc.addrs, joiner, 0)
	})
	s := &autoOffOrphan{settlingCluster: &settlingCluster{tc: tc, ports: joiner}, owner: clusterOf(tc, 0)}
	defer s.tc.stop()
	joined := s.joinedRing(t)
	movedFromOwner := func(name string) bool {
		return s.owner.ownsBucket(placementOf(testDatabase, name)) && ownerOn(joined, name) == s.ports.ringAddr
	}
	s.orphan = bucketNamed(t, "orphan", movedFromOwner)
	data := bucketNamed(t, "cededd", movedFromOwner)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: data, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, data, 3)

	createGate, putGate := make(chan struct{}), make(chan struct{})
	var releaseCreate, releasePut sync.Once
	defer releaseCreate.Do(func() { close(createGate) })
	defer releasePut.Do(func() { close(putGate) })
	var reached sync.WaitGroup
	reached.Add(2)
	var createOnce, putOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != s.owner.self.ID {
			return
		}
		switch {
		case op == transport.OpCreateBucket && bucket == s.orphan:
			createOnce.Do(reached.Done)
			<-createGate
		case op == transport.OpPut && bucket == data:
			putOnce.Do(reached.Done)
			<-putGate
		}
	}
	afterOwnerViewHook.Store(&hold)
	defer afterOwnerViewHook.Store(nil)

	send := func(op transport.Opcode, request any) <-chan struct{} {
		payload, err := bson.Marshal(request)
		if err != nil {
			t.Fatalf("encoding %s: %v", op, err)
		}
		answered := make(chan struct{})
		client := tc.dial(0)
		go func() {
			defer close(answered)
			defer func() { _ = client.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			// Neither answer is asserted: both are overtaken by the join.
			_, _ = client.SendRequest(ctx, op, payload)
		}()
		return answered
	}
	createAnswered := send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.orphan, Type: BucketTypeHash})
	putAnswered := send(transport.OpPut, InsertRequest{Database: testDatabase, Bucket: data, Key: "ceded",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	held := make(chan struct{})
	go func() { reached.Wait(); close(held) }()
	select {
	case <-held:
	case <-time.After(30 * time.Second):
		t.Fatal("setup: the create and the write never both reached their owner's view")
	}

	s.joiner = startJoinTarget(t, tc, s.ports, tc.token)
	s.join(t, 1)
	releaseCreate.Do(func() { close(createGate) })
	select {
	case <-createAnswered:
	case <-time.After(60 * time.Second):
		t.Fatal("the held create was never answered")
	}
	releasePut.Do(func() { close(putGate) })
	select {
	case <-putAnswered:
	case <-time.After(60 * time.Second):
		t.Fatal("the held write was never answered")
	}
	waitLifted(t, s.members(), 60*time.Second, "after an orphaned create and a ceded write")
}
