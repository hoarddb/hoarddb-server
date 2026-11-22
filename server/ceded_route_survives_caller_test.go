package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestACededOwnerQueuesAWriteWhoseCallerHasGone is BUG-0556: the third site
// step 9 of query pipelining moved to routeContext, Cluster.queueCededEntry,
// driven with a caller that has gone. A write is logged in its owner's term
// and finds, after its local apply, that a join moved its bucket (BUG-0528's
// scene); a ceded owner still queues it for the members the sender's rule
// addresses it to. Its routing turn is held by an earlier write to a bucket
// the owner keeps, and its caller goes away while it waits. The entry is in
// the log already, so the turn must outlast the caller: given up, it stalls
// the owner's stream to the replica, which then leaves the in-sync set until
// the log replay brings it back.
func TestACededOwnerQueuesAWriteWhoseCallerHasGone(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	sc := findCededWriteScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range []string{sc.moved, sc.kept} {
		tc.ok(sc.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		tc.ok(sc.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "before",
			Data: map[string]interface{}{"n": 0}, WriteConcern: "all"})
	}

	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	replicaCluster := tc.nodes[sc.replica].server.core.cluster
	replicaID := tc.addrs[sc.replica]
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, replicaID))

	// The write to the moved bucket is held inside its fence, after it read
	// its ownership and before it is logged, while the join is adopted.
	cededReached := make(chan struct{})
	cededRelease := make(chan struct{})
	var cededHoldOnce, cededReleaseOnce sync.Once
	holdCeded := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != ownerCluster.self.ID || op != transport.OpPut || bucket != sc.moved {
			return
		}
		held := false
		cededHoldOnce.Do(func() { held = true })
		if held {
			close(cededReached)
			<-cededRelease
		}
	}
	afterOwnerViewHook.Store(&holdCeded)
	defer afterOwnerViewHook.Store(nil)
	defer cededReleaseOnce.Do(func() { close(cededRelease) })

	callerCtx, callerGone := context.WithCancel(context.Background())
	defer callerGone()
	cededDone := make(chan struct{})
	go func() {
		defer close(cededDone)
		// The caller is gone, and the owner ceded the bucket: the answer is
		// not asserted.
		_, _ = ownerCluster.write(callerCtx, putEntry(t, sc.moved, "in-flight"), placementOf(testDatabase, sc.moved), WcOne)
	}()
	select {
	case <-cededReached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the write to the moved bucket never reached its owner's fence")
	}

	generationBeforeJoin := toReplica.streamGeneration()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer joinCancel()
	if err := ownerCluster.proposeJoin(joinCtx, sc.newcomer); err != nil {
		t.Fatalf("join %s: %v", sc.newcomer.ID, err)
	}
	pollUntil(t, "the replica adopts the join", func() bool { return replicaCluster.Epoch() == ownerCluster.Epoch() })
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by the newcomer", sc.moved, owner, err)
	}
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.kept)); err != nil || owner.ID != ownerCluster.self.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by its owner", sc.kept, owner, err)
	}
	pollUntil(t, "the owner's stream to the replica is streaming again after the join", func() bool {
		return toReplica.inGenerationAfter(generationBeforeJoin) && toReplica.caughtUp()
	})

	// The earlier write, to the kept bucket, stops after its local apply and
	// before its ticket names its peers: until it is released, no later
	// write may be routed to any peer.
	heldReached := make(chan uint64, 1)
	heldRelease := make(chan struct{})
	var heldHoldOnce, heldReleaseOnce sync.Once
	holdEarlier := func(entry ReplEntry) {
		if entry.Bucket != sc.kept || transport.Opcode(entry.Op) != transport.OpPut {
			return
		}
		held := false
		heldHoldOnce.Do(func() { held = true })
		if held {
			heldReached <- entry.Seq
			<-heldRelease
		}
	}
	replicaEnqueueHook.Store(&holdEarlier)
	defer replicaEnqueueHook.Store(nil)
	defer heldReleaseOnce.Do(func() { close(heldRelease) })

	heldDone := make(chan struct{})
	go func() {
		defer close(heldDone)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Its caller stays: the write is acknowledged once a majority holds it.
		_, _ = ownerCluster.write(ctx, putEntry(t, sc.kept, "held"), placementOf(testDatabase, sc.kept), WcMajority)
	}()
	var heldSeq uint64
	select {
	case heldSeq = <-heldReached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the earlier write never reached its routing")
	}

	generationBefore := toReplica.streamGeneration()
	stopWatch := watchStreamLeaving(toReplica)

	// Released, the ceded write is logged above the held one, applied, finds
	// its bucket moved and names its targets in queueCededEntry, then waits
	// for its turn behind the held ticket. Its caller goes away there.
	cededReleaseOnce.Do(func() { close(cededRelease) })
	awaitCededTurn(t, ownerCluster.shipOrder, heldSeq, replicaID, cededDone)
	callerGone()
	time.Sleep(routeTurnSettle)
	leftStreaming := stopWatch()
	heldReleaseOnce.Do(func() { close(heldRelease) })
	for _, done := range []<-chan struct{}{heldDone, cededDone} {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("a write was never answered")
		}
	}

	if generationAfter := toReplica.streamGeneration(); generationAfter != generationBefore {
		t.Fatalf("the ceded owner's stream to the replica changed state %d time(s) while a write it logged in its term, whose "+
			"caller had gone, waited for its routing turn in queueCededEntry; while the write ahead was held it was seen %s. "+
			"A write already in the log must be routed whether or not its caller is there, or the replica leaves the in-sync set",
			generationAfter-generationBefore, leftStreaming)
	}
	if !eventually(func() bool { return holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, "in-flight") }) {
		t.Fatalf("the replica %s never received the write its ceded owner %s queued for it in %s",
			replicaID, tc.addrs[sc.owner], sc.moved)
	}
	if !eventually(func() bool { return holdsKeyLocallyIn(tc.nodes[sc.replica], sc.kept, "held") }) {
		t.Fatalf("the replica never received the held write to %s", sc.kept)
	}
}

// awaitCededTurn waits until a write above floor has named its peers, the
// replica among them, which queueCededEntry does just before it waits for its
// routing turn, or has returned. A write that returned without naming the
// replica fails the setup: the case would not test the turn at all.
func awaitCededTurn(t *testing.T, order *shipOrder, floor uint64, replicaID string, done <-chan struct{}) {
	t.Helper()
	returned := false
	pollUntil(t, fmt.Sprintf("the ceded write above seq %d names the replica and reaches its routing turn", floor), func() bool {
		select {
		case <-done:
			returned = true
			return true
		default:
		}
		order.mu.Lock()
		defer order.mu.Unlock()
		for ticket := range order.pending {
			if _, named := ticket.peers[replicaID]; ticket.floor > floor && named {
				return true
			}
		}
		return false
	})
	if returned {
		t.Fatal("setup: the ceded write returned before it waited for its routing turn behind the held write")
	}
}

// watchStreamLeaving samples p's stream state every millisecond until the
// returned stop is called, and stop describes the first state other than
// streaming it saw. A stalled stream replays and is streaming again within
// moments, and afterwards only its generation remembers the stall, so the
// description is what names the reason.
func watchStreamLeaving(p *peer) (stop func() string) {
	stopWatch := make(chan struct{})
	watched := make(chan string, 1)
	go func() {
		leftStreaming := "streaming at every sample (the stall and its replay fell between samples)"
		seen := false
		for {
			if state, reason := p.streamStatus(); state != streamStreaming && !seen {
				leftStreaming, seen = fmt.Sprintf("%s (%q)", state, reason), true
			}
			select {
			case <-stopWatch:
				watched <- leftStreaming
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return func() string {
		close(stopWatch)
		return <-watched
	}
}
