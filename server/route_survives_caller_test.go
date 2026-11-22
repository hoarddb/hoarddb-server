package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// routeTurnSettle is how long a write whose routing turn is blocked is left
// blocked before the write ahead of it is released. A cancelled caller's
// write that gives up its turn stalls the stream within microseconds; the
// margin only absorbs scheduling under -race.
const routeTurnSettle = 300 * time.Millisecond

// TestOwnerQueuesAWriteWhoseCallerHasGone is query pipelining's step 9 (OC-8,
// option B). A write already in the owner's log must be routed to its
// replicas whether or not its caller is still there. QP-3 cancels a
// connection's context when its client goes away, and a write whose routing
// turn is held by an earlier write then gave up the turn and stalled the
// replica's stream: the replica left the in-sync set, and every other
// client's write was counted against the copies left (at RF 2, the owner
// alone) until the log replay brought the stream back.
//
// Each case holds one write between its local apply and its routing, so its
// ship ticket holds every peer's turn, and runs a second write behind it
// whose caller is gone. The stream to the replica must not change state.
func TestOwnerQueuesAWriteWhoseCallerHasGone(t *testing.T) {
	t.Run("a bucket write", func(t *testing.T) {
		runRouteSurvivesCaller(t, routeBucketWriteOfAGoneCaller)
	})
	t.Run("a database broadcast", func(t *testing.T) {
		runRouteSurvivesCaller(t, routeBroadcastOfAGoneCaller)
	})
}

// routeScene is the cluster a case runs in: an owner, its replica, and two
// buckets the owner owns.
type routeScene struct {
	tc                    *testCluster
	owner                 *Cluster
	ownerIndex, replica   int
	heldBucket, behindBkt string
	// heldSeq is the sequence of the write held before its routing.
	heldSeq uint64
}

// goneCallerWrite runs a write behind the held one whose caller goes away,
// returning once it has reached its routing turn, and a channel closed when
// the write returns.
type goneCallerWrite func(t *testing.T, sc *routeScene) (done <-chan struct{})

func runRouteSurvivesCaller(t *testing.T, behind goneCallerWrite) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	placement := tc.nodes[0].server.core.cluster
	heldBucket, behindBucket := bucketsOnOneOwner(t, placement, false)
	ownerNode, err := placement.Owner(placementOf(testDatabase, heldBucket))
	if err != nil {
		t.Fatalf("resolving the owner of %q: %v", heldBucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerNode.ID)
	sc := &routeScene{tc: tc, owner: tc.nodes[ownerIndex].server.core.cluster, ownerIndex: ownerIndex,
		replica: 1 - ownerIndex, heldBucket: heldBucket, behindBkt: behindBucket}

	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range []string{heldBucket, behindBucket} {
		tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 2)
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "before",
			Data: map[string]interface{}{"n": 0}, WriteConcern: "all"})
	}
	toReplica := sc.owner.peerFor(mustNode(t, sc.owner, tc.addrs[sc.replica]))
	pollUntil(t, "the owner's stream to the replica is streaming and caught up", toReplica.caughtUp)
	generationBefore := toReplica.streamGeneration()

	// The held write stops after its local apply, before its ticket names its
	// peers: until it is released, no later write may be routed to any peer.
	reached := make(chan uint64, 1)
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	hold := func(entry ReplEntry) {
		if entry.Bucket != heldBucket || transport.Opcode(entry.Op) != transport.OpPut {
			return
		}
		held := false
		holdOnce.Do(func() { held = true })
		if held {
			reached <- entry.Seq
			<-release
		}
	}
	replicaEnqueueHook.Store(&hold)
	defer replicaEnqueueHook.Store(nil)
	defer releaseOnce.Do(func() { close(release) })

	heldDone := make(chan struct{})
	go func() {
		defer close(heldDone)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Its caller stays: the write is acknowledged once both copies hold it.
		_, _ = sc.owner.write(ctx, putEntry(t, heldBucket, "held"), placementOf(testDatabase, heldBucket), WcMajority)
	}()
	select {
	case sc.heldSeq = <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the held write never reached its routing")
	}

	// Watched while the write ahead is held: a stalled stream replays and is
	// streaming again within moments, and afterwards only its generation
	// remembers the stall. The watch names the reason when it sees one.
	stopWatch := make(chan struct{})
	watched := make(chan string, 1)
	go func() {
		leftStreaming := "streaming at every sample (the stall and its replay fell between samples)"
		seen := false
		for {
			if state, reason := toReplica.streamStatus(); state != streamStreaming && !seen {
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
	behindDone := behind(t, sc)
	time.Sleep(routeTurnSettle)
	close(stopWatch)
	leftStreaming := <-watched
	releaseOnce.Do(func() { close(release) })
	for _, done := range []<-chan struct{}{heldDone, behindDone} {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("a write was never answered")
		}
	}

	if generationAfter := toReplica.streamGeneration(); generationAfter != generationBefore {
		t.Fatalf("the owner's stream to the replica changed state %d time(s) while a write whose caller had gone waited for "+
			"its routing turn; while the write ahead was held it was seen %s. A write already in the log must be routed "+
			"whether or not its caller is there, or the replica leaves the in-sync set and other writes are acknowledged without it",
			generationAfter-generationBefore, leftStreaming)
	}
	if !eventually(func() bool { return holdsKeyLocallyIn(tc.nodes[sc.replica], heldBucket, "held") }) {
		t.Fatalf("the replica never received the held write to %s", heldBucket)
	}
}

// routeBucketWriteOfAGoneCaller writes the second bucket through
// Cluster.write with a context cancelled after the write's WAL append, before
// its local apply.
func routeBucketWriteOfAGoneCaller(t *testing.T, sc *routeScene) <-chan struct{} {
	ctx, cancel := context.WithCancel(context.Background())
	appliedSeq := make(chan uint64, 1)
	var cancelOnce sync.Once
	gone := func(nodeID string, repl ReplEntry) {
		if nodeID != sc.owner.self.ID || repl.Bucket != sc.behindBkt || transport.Opcode(repl.Op) != transport.OpPut {
			return
		}
		cancelOnce.Do(func() {
			cancel()
			appliedSeq <- repl.Seq
		})
	}
	beforeLocalApplyHook.Store(&gone)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		// The caller is gone, so its answer is not asserted.
		_, _ = sc.owner.write(ctx, putEntry(t, sc.behindBkt, "gone"), placementOf(testDatabase, sc.behindBkt), WcMajority)
	}()
	var behindSeq uint64
	select {
	case behindSeq = <-appliedSeq:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the write behind the held one never reached its local apply")
	}
	if behindSeq <= sc.heldSeq {
		t.Fatalf("setup: the write behind has seq %d, not above the held write's %d", behindSeq, sc.heldSeq)
	}
	awaitRoutingTurn(t, sc.owner.shipOrder, sc.heldSeq, done)
	if !eventually(func() bool { return holdsKeyLocallyIn(sc.tc.nodes[sc.ownerIndex], sc.behindBkt, "gone") }) {
		t.Fatalf("setup: the owner never applied the write behind the held one")
	}
	return done
}

// routeBroadcastOfAGoneCaller creates a database, which is broadcast to every
// member, and cancels its caller's context once the broadcast has named its
// peers and is waiting for its routing turn. Nothing before the routing
// observes the context, so where the cancel falls between the append and the
// turn makes no difference.
func routeBroadcastOfAGoneCaller(t *testing.T, sc *routeScene) <-chan struct{} {
	payload, err := bson.Marshal(CreateDatabaseRequest{Name: "routed_db"})
	if err != nil {
		t.Fatalf("encoding the database create: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		_, _ = sc.owner.handleClientFrame(ctx, transport.NewFrame(transport.OpCreateDB, payload))
	}()
	awaitRoutingTurn(t, sc.owner.shipOrder, sc.heldSeq, done)
	cancel()
	return done
}

// awaitRoutingTurn waits until a write above floor has named its peers, which
// it does just before it waits for its routing turn, or has returned.
func awaitRoutingTurn(t *testing.T, order *shipOrder, floor uint64, done <-chan struct{}) {
	t.Helper()
	pollUntil(t, fmt.Sprintf("a write above seq %d reaches its routing turn", floor), func() bool {
		select {
		case <-done:
			return true
		default:
		}
		order.mu.Lock()
		defer order.mu.Unlock()
		for ticket := range order.pending {
			if ticket.floor > floor && ticket.peers != nil {
				return true
			}
		}
		return false
	})
}

// putEntry is the replication entry of a PUT of key to bucket.
func putEntry(t *testing.T, bucket, key string) ReplEntry {
	t.Helper()
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
		Data: map[string]interface{}{"key": key}})
	if err != nil {
		t.Fatalf("encoding a put of %q to %q: %v", key, bucket, err)
	}
	return ReplEntry{Op: int32(transport.OpPut), Bucket: bucket, Key: []byte(key), Payload: payload}
}

// streamGeneration reports the stream's generation without touching its
// wake.
func (p *peer) streamGeneration() uint64 {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.generation
}
