package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The forward path (on-demand pull build doc step 8; spec §6.2, §6.4, §6.8,
// §8): past the bound a read is forwarded to the bucket's final previous owner
// with the serve-or-refuse marker; a read whose node stops being the owner
// while it waits is rerouted once to the new owner; a restore never takes
// either.

// leaveCase is a removal's survivor that owns a bucket it holds a replica of,
// with its take-over held before its open: three nodes at factor 3, the
// bucket's owner (node 1) removed while it still runs. The survivor's take-over
// has heard the removed node's final answer, so the removed node is the
// bucket's final previous owner (sourceKnown).
type leaveCase struct {
	tc                        *testCluster
	bucket                    string
	previous, newOwner, other int
	held                      *workerGate
}

func setUpLeaveCase(t *testing.T, prefix string, storeType BucketType, fill func(tc *testCluster, bucket string), opts ...func(cfg *config.Config)) leaveCase {
	t.Helper()
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) {
		for _, opt := range opts {
			opt(cfg)
		}
	})
	t.Cleanup(tc.stop)
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, tc.addrs[1], tc.addrs[2])
	bucket := bucketNamed(t, prefix, func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: storeType})
	waitForServing(t, tc, testDatabase, bucket, 3)
	fill(tc, bucket)
	newOwnerID := ownerOn(after, bucket)
	newOwner := nodeIndexByAddr(t, tc.addrs, newOwnerID)
	other := 3 - newOwner

	held := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeOpenHook, newOwnerID, func(name string) {
		if name == bucket {
			held.hold()
		}
	})
	removeAndTell(t, tc, newOwner, 0)
	via := clusterOf(tc, newOwner)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	held.await(t, "the open of the new owner's take-over", takeOverWait)
	key := placementOf(testDatabase, bucket)
	if status := via.takeOverStatusOf(key); !status.sourceKnown || status.source.ID != tc.addrs[0] {
		t.Fatalf("setup: the new owner's take-over does not name the removed node as the final previous owner: %+v", status)
	}
	return leaveCase{tc: tc, bucket: bucket, previous: 0, newOwner: newOwner, other: other, held: held}
}

// putHeld writes the document "held" at every copy.
func putHeld(tc *testCluster, bucket string) {
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
}

// forwardsOf records every forwarded read of bucket that leaves any node, as
// the request it went in, from now until the test ends.
type forwardsOf struct {
	mu   sync.Mutex
	sent []forwardedRead
}

type forwardedRead struct {
	from          string
	waitMs        int64
	rerouted      bool
	serveFromCopy bool
}

func recordForwards(t *testing.T, bucket string) *forwardsOf {
	t.Helper()
	record := &forwardsOf{}
	hook := func(req *ReplPushRequest) {
		if !req.ReturnResponse || len(req.Entries) != 1 || req.Entries[0].Bucket != bucket ||
			!transport.Opcode(req.Entries[0].Op).IsReadOnly() && !isRestoreOpcode(transport.Opcode(req.Entries[0].Op)) {
			return
		}
		record.mu.Lock()
		record.sent = append(record.sent, forwardedRead{from: req.FromNode, waitMs: req.ReadWaitMs, rerouted: req.Rerouted, serveFromCopy: req.ServeFromCopy})
		record.mu.Unlock()
	}
	beforePeerCallHook.Store(&hook)
	t.Cleanup(func() { beforePeerCallHook.Store(nil) })
	return record
}

func (f *forwardsOf) list() []forwardedRead {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forwardedRead(nil), f.sent...)
}

// timedFrame sends op at node index and returns the answer and how long it took.
func timedFrame(tc *testCluster, index int, op transport.Opcode, req any) (*transport.Frame, time.Duration) {
	began := time.Now()
	frame := tc.frame(index, op, req)
	return frame, time.Since(began)
}

// TestAFindPastTheBoundIsForwardedToTheFinalPreviousOwner is BUG-0340's first
// criterion in the leave case: the new owner holds a replica and its
// take-over is held before the open, so a get there waits its bound and is
// then forwarded to the removed node, the final previous owner, which answers
// it from its copy. Released, the copy lands. Guard: the forward past the
// bound.
func TestAFindPastTheBoundIsForwardedToTheFinalPreviousOwner(t *testing.T) {
	s := setUpLeaveCase(t, "pastbound", BucketTypeHash, putHeld)
	forwards := recordForwards(t, s.bucket)
	get := FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}
	frame, took := timedFrame(s.tc, s.newOwner, transport.OpGet, get)
	foundHeld(t, "a get at the new owner past the bound", frame)
	if took < onDemandWait*9/10 {
		t.Errorf("the get was answered after %s; a copy the new owner holds is waited for up to the bound (%s) first", took, onDemandWait)
	}
	sent := forwards.list()
	if len(sent) != 1 || !sent[0].serveFromCopy || sent[0].from != s.tc.addrs[s.newOwner] {
		t.Errorf("the get was forwarded %+v; want once, from the new owner, with the serve-or-refuse marker", sent)
	}
	s.held.release()
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(s.tc, s.newOwner, testDatabase, s.bucket, "held") }) {
		t.Error("the copy did not land at the new owner once its take-over was released")
	}
}

// TestAPeekForwardedToTheFinalPreviousOwnerAnswersTheNextPopsHead is the §6.4
// leave-case probe: a peek at the new owner, past the bound, is forwarded to
// the removed node and answers the element the new owner's first pop takes
// once its take-over is released. Guard: the forward past the bound (a peek
// is answered retry without it).
func TestAPeekForwardedToTheFinalPreviousOwnerAnswersTheNextPopsHead(t *testing.T) {
	s := setUpLeaveCase(t, "peeked", BucketTypeFIFO, func(tc *testCluster, bucket string) {
		for n := 1; n <= 3; n++ {
			tc.ok(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
		}
	})
	frame := s.tc.frame(s.newOwner, transport.OpFIFOPeek, PeekRequest{Database: testDatabase, Bucket: s.bucket})
	answered(t, "a peek at the new owner past the bound", frame)
	var peeked PeekResponse
	if err := bson.Unmarshal(frame.Payload, &peeked); err != nil || !peeked.Found || len(peeked.Items) == 0 {
		t.Fatalf("the forwarded peek found no element (err %v): %s", err, frame.Payload)
	}
	s.held.release()
	var popped PopResponse
	if !holdsWithin(takeOverWait, func() bool {
		frame := s.tc.frame(s.newOwner, transport.OpFIFOPop, PopRequest{Database: testDatabase, Bucket: s.bucket})
		return frame.Opcode != transport.OpError && bson.Unmarshal(frame.Payload, &popped) == nil && popped.Found
	}) {
		t.Fatal("no pop at the new owner succeeded once its take-over was released")
	}
	if len(popped.Items) == 0 || elementID(popped.Items[0]) != elementID(peeked.Items[0]) {
		t.Errorf("the forwarded peek answered %s, and the new owner's first pop took %v", peeked.Items[0], popped.Items)
	}
}

// elementID is a queue element's _id_, which names it whatever order its
// fields are encoded in.
func elementID(element bson.Raw) string {
	id, _ := element.Lookup("_id_").StringValueOK()
	return id
}

// TestAForwardedReadNeverAnswersNotFound is the serve-or-refuse marker: the
// removed node releases its copy after it answered the take-over's question,
// and before the read is forwarded to it. The forwarded read is answered
// retry, never "not found". Guard: the marker check at the receiver.
func TestAForwardedReadNeverAnswersNotFound(t *testing.T) {
	s := setUpLeaveCase(t, "released", BucketTypeHash, putHeld)
	if err := clusterOf(s.tc, s.previous).releaseLocalCopy(testDatabase, s.bucket, "for the test"); err != nil {
		t.Fatalf("releasing the previous owner's copy: %v", err)
	}
	frame := s.tc.frame(s.newOwner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"})
	refusedAwaitingCopyOrRetry(t, "a get forwarded to a previous owner that released its copy", frame)
	// The new owner's hand-off retry: the copy lands there. Without the marker
	// the previous owner answers as a node that does not hold the bucket,
	// which sends the client elsewhere and says it may never have existed.
	if message := errorFrameText(frame); !strings.Contains(message, "is moving to this node") {
		t.Errorf("the forwarded get was answered %q; want the new owner's hand-off retry", message)
	}
}

// refusedAwaitingCopyOrRetry fails the test unless frame is an UNAVAILABLE
// retry that does not say the data does not exist.
func refusedAwaitingCopyOrRetry(t *testing.T, what string, frame *transport.Frame) {
	t.Helper()
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Errorf("%s was answered %s, want UNAVAILABLE retry: %s", what, frame.Opcode, frame.Payload)
		return
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable || strings.Contains(refusal.Message, "not found") {
		t.Errorf("%s was answered code %d, want UNAVAILABLE retry and never not found: %s", what, refusal.Code, refusal.Message)
	}
}

// TestARestoreInAWindowIsNeverForwarded is fail-silently 1: in the leave case,
// with the final previous owner known, a RESTORE_START at the new owner is
// answered retry and is neither forwarded to the previous owner nor rerouted.
// Guard: the restore exclusion.
func TestARestoreInAWindowIsNeverForwarded(t *testing.T) {
	s := setUpLeaveCase(t, "restored", BucketTypeHash, putHeld)
	forwards := recordForwards(t, s.bucket)
	frame := s.tc.frame(s.newOwner, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: s.bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	refusedAwaitingCopyOrRetry(t, "a restore start at the new owner mid take-over", frame)
	if sent := forwards.list(); len(sent) != 0 {
		t.Errorf("the restore start was forwarded %+v; a restore is only ever the owner's", sent)
	}
}

// TestAReadWithAutoRebalanceOffForwardsAndNeverPulls is O-5: with
// auto_rebalance off, a read in the leave case is forwarded to the final
// previous owner at once, and a read at a joiner that holds nothing, with no
// previous owner known, is answered at once with the needsCopy refusal naming
// 'migrate', not the default "finding the bucket's most complete copy". No
// read starts a pull. Guard: the read gives no leave and synthesises the
// needsCopy answer (stop-and-ask 14, confirmed).
func TestAReadWithAutoRebalanceOffForwardsAndNeverPulls(t *testing.T) {
	var pulls atomic.Int64
	countPulls := func(_, _ string) { pulls.Add(1) }
	pullStartedHook.Store(&countPulls)
	t.Cleanup(func() { pullStartedHook.Store(nil) })
	rebalanceOff := func(cfg *config.Config) { cfg.Server.AutoRebalance = false }

	t.Run("the leave case forwards at once", func(t *testing.T) {
		s := setUpLeaveCase(t, "offleave", BucketTypeHash, putHeld, rebalanceOff)
		frame, took := timedFrame(s.tc, s.newOwner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"})
		foundHeld(t, "a get at the new owner with auto_rebalance off", frame)
		if took >= onDemandWait/2 {
			t.Errorf("the get took %s; with auto_rebalance off a known previous owner is forwarded to at once", took)
		}
	})
	t.Run("the join case answers migrate at once", func(t *testing.T) {
		s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: "offjoin", count: 1, joinerOptions: []func(*config.Config){rebalanceOff}})
		began := time.Now()
		frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.buckets[0], Key: "held"})
		took := time.Since(began)
		message := errorFrameText(frame)
		if want := fmt.Sprintf("migrate %s %s", testDatabase, s.buckets[0]); !strings.Contains(message, want) {
			t.Errorf("a get at a joiner with auto_rebalance off was answered %q; want the needsCopy refusal naming '%s'", message, want)
		}
		if took >= onDemandWait/2 {
			t.Errorf("the get took %s; with auto_rebalance off and no source it is answered at once", took)
		}
		if s.joiner.mayCopyWhole(placementOf(testDatabase, s.buckets[0])) {
			t.Errorf("the read gave the take-over leave to copy")
		}
	})
	if n := pulls.Load(); n != 0 {
		t.Errorf("%d pulls started; with auto_rebalance off a read never pulls", n)
	}
}

// TestAReadReroutesOnceAndMakesAtMostThreeHops is §8's hop bound, at each node
// the chain can reach. (1) A read that arrives with Rerouted at a node that is
// not the owner is answered retry, not sent on and not answered from its
// replica. (2) A read that arrives with ServeFromCopy at the owner mid
// take-over, whose own §6.8 would forward it, is answered from its own copy
// and sent nowhere. With the reroute keyed on the request (the second-join
// test reroutes once), the chain is at most coordinator → old owner → new
// owner → its final previous owner. Guards: the Rerouted check and the
// ServeFromCopy check.
func TestAReadReroutesOnceAndMakesAtMostThreeHops(t *testing.T) {
	s := setUpLeaveCase(t, "hops", BucketTypeHash, putHeld)
	forwards := recordForwards(t, s.bucket)
	get := FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}
	push := func(at int, mark func(*ReplPushRequest)) ReplPushResponse {
		t.Helper()
		target := clusterOf(s.tc, at)
		req := ReplPushRequest{Token: target.localToken(), FromNode: s.tc.addrs[s.other], ReturnResponse: true, Epoch: target.Epoch(),
			Entries: []ReplEntry{{Op: int32(transport.OpGet), Bucket: s.bucket, Payload: mustMarshalBSON(t, get)}}}
		mark(&req)
		ack, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, mustMarshalBSON(t, req)), target.s)
		if err != nil {
			t.Fatalf("pushing the read to node %d: %v", at+1, err)
		}
		var resp ReplPushResponse
		if err := bson.Unmarshal(ack.Payload, &resp); err != nil {
			t.Fatalf("decoding the answer: %v", err)
		}
		return resp
	}

	rerouted := push(s.other, func(req *ReplPushRequest) { req.Rerouted = true; req.ReadWaitMs = 500 })
	if rerouted.Success || rerouted.Refusal == "" {
		t.Errorf("a rerouted read at a node that is not the owner was answered success=%t refusal=%q; want a retry refusal",
			rerouted.Success, rerouted.Refusal)
	}
	served := push(s.newOwner, func(req *ReplPushRequest) { req.ServeFromCopy = true })
	if !served.Success || len(served.Responses) != 1 || transport.Opcode(served.Responses[0].Opcode) == transport.OpError {
		t.Errorf("a serve-from-copy read at a node holding an open copy was not answered from it: success=%t error=%q", served.Success, served.Error)
	}
	if sent := forwards.list(); len(sent) != 0 {
		t.Errorf("a rerouted or serve-from-copy read was sent on %+v; neither is ever forwarded again", sent)
	}
}

// secondJoin is a joiner J1 that owns a bucket it holds nothing of, with its
// take-over held, and a second joiner J2 that the cluster has already joined
// and that owns the bucket on the new membership, which J1 has not adopted.
type secondJoin struct {
	joinerOwningUnpulled
	proposer   *Cluster
	second     *testNode
	secondPort *testNodePorts
	bucket     string
}

func setUpSecondJoin(t *testing.T, prefix string) secondJoin {
	t.Helper()
	secondPorts := reserveNodePorts(t)
	s := setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: prefix, count: 1, also: func(joined []ring.Node, name string) bool {
		return ownerOn(ringOf(t, append(append([]ring.Node{}, joined...), secondPorts.ringNode()), 3), name) == secondPorts.ringAddr
	}, reseed: func() { secondPorts = reserveNodePorts(t) }})
	bucket := s.buckets[0]
	firstID := s.joiner.self.ID
	// J1 is a member: J2 pins it and it pins J2, as any member does.
	s.tc.nodes = append(s.tc.nodes, s.joinerNode)
	s.tc.addrs = append(s.tc.addrs, firstID)
	s.tc.fingerprints = append(s.tc.fingerprints, preGeneratedNodeFingerprints(t, []string{s.joinerNode.cfg.Storage.DataDir})...)

	// J1 does not hear the second join until the test brings it there.
	unreachable := func(_ context.Context, _, toNode, _ string) error {
		if toNode == firstID {
			return fmt.Errorf("test: %s does not hear the second join yet", firstID)
		}
		return nil
	}
	membershipSendHook.Store(&unreachable)
	t.Cleanup(func() { membershipSendHook.Store(nil) })
	skipFirst := func(nodeID, _, _ string) bool { return nodeID == firstID }
	gossipReconcileHook.Store(&skipFirst)
	t.Cleanup(func() { gossipReconcileHook.Store(nil) })

	held := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeClaimHook, firstID, func(name string) {
		if name == bucket {
			held.hold()
		}
	})
	proposer := s.tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, secondPorts.ringNode()); err != nil {
		t.Fatalf("proposing the second join: %v", err)
	}
	for _, node := range s.tc.nodes[:2] {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(secondPorts.ringNode())))
	}
	second := startJoinTarget(t, s.tc, secondPorts, s.tc.token)
	t.Cleanup(func() { _ = second.client.Close() })
	adoptFrom(t, second.server.core.cluster, proposer)
	if !s.joiner.actsAsOwner(placementOf(testDatabase, bucket)) {
		t.Fatalf("setup: J1 adopted the second join before the test brought it there")
	}
	if !second.server.core.cluster.actsAsOwner(placementOf(testDatabase, bucket)) {
		t.Fatalf("setup: J2 does not own %s", bucket)
	}
	return secondJoin{joinerOwningUnpulled: s, proposer: proposer, second: second, secondPort: secondPorts, bucket: bucket}
}

// readThroughTheSecondJoin starts a get at J1, brings J1 to the second join
// while the get waits, and returns the get's answer.
func (s secondJoin) readThroughTheSecondJoin(t *testing.T) *transport.Frame {
	t.Helper()
	answer := make(chan *transport.Frame, 1)
	go func() {
		answer <- s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"})
	}()
	time.Sleep(onDemandWait / 4)
	adoptFrom(t, s.joiner, s.proposer)
	return <-answer
}

// TestASecondJoinDuringTheWaitNeverAnswersNotFound is F-9 (a): J2 joins while
// a read waits at J1. The read is rerouted once to J2 with what is left of its
// bound, and J2, the owner holding nothing, runs §6.8 on it at its replica
// branch: the answer is the bucket's document or retry, never "not found".
// Guard: the reroute and §6.8 at handleReplPush.
func TestASecondJoinDuringTheWaitNeverAnswersNotFound(t *testing.T) {
	s := setUpSecondJoin(t, "secondjoin")
	forwards := recordForwards(t, s.bucket)
	frame := s.readThroughTheSecondJoin(t)
	if frame.Opcode == transport.OpError {
		refusedAwaitingCopyOrRetry(t, "a get at J1 rerouted to J2", frame)
	} else {
		foundHeld(t, "a get at J1 rerouted to J2", frame)
	}
	sent := forwards.list()
	if len(sent) == 0 || sent[0].from != s.joiner.self.ID || !sent[0].rerouted || sent[0].waitMs <= 0 || sent[0].waitMs >= onDemandWait.Milliseconds() {
		t.Errorf("the read was forwarded %+v; want J1 to reroute it once to J2 with what is left of its bound", sent)
	}
}

// TestAReadRerouteCarriesTheMarkersAlongTheChain is §8's chain end to end,
// through the second join: J1 reroutes the read once (Rerouted, a wait below
// the bound), and anything J2 sends on carries ServeFromCopy. At most three
// forwards in all, and none after a ServeFromCopy one. Guard: the reroute is
// keyed on the request.
func TestAReadRerouteCarriesTheMarkersAlongTheChain(t *testing.T) {
	s := setUpSecondJoin(t, "chain")
	forwards := recordForwards(t, s.bucket)
	s.readThroughTheSecondJoin(t)
	sent := forwards.list()
	if len(sent) == 0 || len(sent) > 3 {
		t.Fatalf("the read made %d forwards %+v; want one to three", len(sent), sent)
	}
	reroutes := 0
	for index, forward := range sent {
		if forward.rerouted {
			reroutes++
		}
		if forward.from == s.second.server.core.cluster.self.ID && !forward.serveFromCopy {
			t.Errorf("J2 sent the rerouted read on without the serve-or-refuse marker: %+v", forward)
		}
		if index > 0 && sent[index-1].serveFromCopy {
			t.Errorf("a serve-from-copy read was sent on again: %+v", sent)
		}
	}
	if reroutes != 1 {
		t.Errorf("the read was rerouted %d times, want once: %+v", reroutes, sent)
	}
}
