package wire

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The deferred-pair gate (the deferred pair gate spec): a pair whose boot replay has
// not finished (PairOwnership.isDeferred) holds acknowledged records above its applied
// position, so every answer to a peer that carries or rests on that position is an error.

// gateNode is a restarted cluster node with one pair deferred, made with the BUG-0760 seam:
// the pair's store and record are put back behind its log, the node boots with its Raft apply
// held so the replay cannot read the bucket's declaration, and the background passes that
// would finish the replay are held (bootReplayPass), so the pair stays deferred until release.
type gateNode struct {
	t    *testing.T
	node *Node
	slot uint16
	db   uint64
	// low is the applied position the restart left; tail is the last position in the pair's log.
	low, tail cluster.Position
	// calmSlot and calmDB name a pair of the same node whose replay had nothing to wait for.
	calmSlot uint16
	calmDB   uint64
	unhold   func()
}

func newGateNode(t *testing.T) *gateNode {
	t.Helper()
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	dbID := bug0760Box(t, node, u30iDB)
	calmID := bug0760Box(t, node, "calm")
	slot := bug0760Slot(t, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	before := savePair(t, dir, slot, dbID)
	node = s0OpenClusterNode(t, dir, nil)
	bug0760Push(t, node, u30iDB, "x1", "x2")
	tail := node.pairs.Snapshot(slot, dbID).AppliedPos
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	rewindPair(t, dir, before)

	pass := make(chan struct{})
	var once sync.Once
	unhold := func() { once.Do(func() { close(pass) }) }
	bootReplayPass = func() {
		select {
		case <-pass:
		case <-time.After(2 * time.Minute): // a test that forgot to release must not hang Close for good
		}
	}
	// Registered before openHeld, so it runs after the node's Close has ended the replay goroutine.
	t.Cleanup(func() { bootReplayPass = nil })
	reopened, _, release := openHeld(t, dir)
	// Registered after openHeld, so it runs before the node's Close, which waits for the pass.
	t.Cleanup(unhold)
	release()
	g := &gateNode{t: t, node: reopened, slot: slot, db: dbID, tail: tail, calmSlot: bug0760Slot(t, "calm"), calmDB: calmID, unhold: unhold}
	g.low = reopened.pairs.Snapshot(slot, dbID).AppliedPos
	if !reopened.own.isDeferred(slot, dbID) || !g.low.Less(tail) {
		t.Fatalf("the fixture's pair is not deferred behind its log: deferred %v, applied %s, log tail %s", reopened.own.isDeferred(slot, dbID), g.low, tail)
	}
	if reopened.own.isDeferred(g.calmSlot, calmID) {
		t.Fatal("the fixture's second pair is deferred too; it must be one that replayed at the boot")
	}
	deadline := time.Now().Add(30 * time.Second)
	for !reopened.cl.serving.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the restarted node did not begin to serve its peers within 30 s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return g
}

// release lets the boot replay's passes run and waits for one to release the pair.
func (g *gateNode) release() {
	g.t.Helper()
	g.unhold()
	deadline := time.Now().Add(30 * time.Second)
	for g.node.own.isDeferred(g.slot, g.db) {
		if time.Now().After(deadline) {
			g.t.Fatal("the pair was still deferred 30 s after its replay was let go")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (g *gateNode) envelope(kind byte, slot uint16, db uint64) transport.Envelope {
	return transport.Envelope{Version: transport.EnvelopeVersion, Kind: kind, Epoch: g.node.topology.Committed(), Slot: slot, DBID: db}
}

// askPosition is a POSITION_QUERY from a peer, through the node's data channel handler.
func (g *gateNode) askPosition(slot uint16, db uint64) (*transport.Message, error) {
	g.t.Helper()
	body, err := EncodePositionQuery(PositionQuery{Slot: slot, DB: db})
	if err != nil {
		g.t.Fatal(err)
	}
	return g.node.serveData(context.Background(), transport.Handshake{NodeID: "peer"}, transport.Message{Env: g.envelope(kindPositionQuery, slot, db), Body: body})
}

// position is askPosition for a pair that must answer.
func (g *gateNode) position(slot uint16, db uint64) PositionReply {
	g.t.Helper()
	reply, err := g.askPosition(slot, db)
	if err != nil || reply == nil {
		g.t.Fatalf("the position query for slot %d of database %d was not answered: %v", slot, db, err)
	}
	answered, err := DecodePositionReply(reply.Body)
	if err != nil {
		g.t.Fatal(err)
	}
	return answered
}

// offers gathers the pair's POSITION_REPLY the way Node.catchUpSource does for each copy: a
// copy whose query failed is skipped and makes no offer.
func (g *gateNode) offers() []catchUpOffer {
	g.t.Helper()
	reply, err := g.askPosition(g.slot, g.db)
	if err != nil {
		return nil
	}
	answered, err := DecodePositionReply(reply.Body)
	if err != nil {
		g.t.Fatal(err)
	}
	return []catchUpOffer{{ID: g.node.cfg.NodeID, Reply: answered}}
}

// registerSeed is a SEED_REGISTER for the pair from a peer, through the node's seed source.
func (g *gateNode) registerSeed(slot uint16, db uint64, id byte) (*transport.Message, error) {
	g.t.Helper()
	msg := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: g.node.topology.Committed(), Slot: slot, DBID: db}, SeedID: t2ID(id)}))
	return g.node.cl.seed.Serve("peer", msg)
}

func positionAfter(pos cluster.Position) cluster.Position {
	return cluster.Position{Epoch: pos.Epoch, Seq: pos.Seq + 1}
}

func replayRefusalText(node string, slot uint16, db uint64) string {
	return cluster.CodeUnavailable.Text(cluster.CondPairReplaying, map[string]string{
		"slot": strconv.Itoa(int(slot)), "db": strconv.FormatUint(db, 10), "node": node,
	})
}

// wantReplayRefusal fails unless err is the gate's refusal for the pair on node: the coded
// UNAVAILABLE with the exact text of the spec's section 2.
func wantReplayRefusal(t *testing.T, what string, err error, node string, slot uint16, db uint64) {
	t.Helper()
	var refusal *cluster.Error
	want := replayRefusalText(node, slot, db)
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable || refusal.Message != want {
		t.Fatalf("%s: got error %v, want the refusal %q", what, err, want)
	}
	// A peer reads the text and not the code: neither marker a catch-up requester acts on may be in it.
	if overWire := errors.New(err.Error()); isBelowHorizon(overWire) || catchUpFenced(overWire) {
		t.Fatalf("%s: the refusal's text %q reads as a below-horizon or epoch-fenced answer on the peer channel, and a requester would reset or spin on it", what, err)
	}
}

// TestADeferredPairGivesNoPosition is G1 (row 1): a position query for a deferred pair is
// answered with the error and no position, so no asker can take the low position for an
// answer, and a node choosing a catch-up source gets no offer from it.
// [BREAK: drop the check in servePosition; answer state "seeding" with the position]
func TestADeferredPairGivesNoPosition(t *testing.T) {
	g := newGateNode(t)
	reply, err := g.askPosition(g.slot, g.db)
	if reply != nil {
		answered, decodeErr := DecodePositionReply(reply.Body)
		t.Fatalf("the deferred pair answered a position query with state %d and position %s (decode: %v); its log holds records up to %s, so any position it gives is below what it acknowledged",
			answered.State, answered.AppliedPos, decodeErr, g.tail)
	}
	wantReplayRefusal(t, "the position query for the deferred pair", err, g.node.cfg.NodeID, g.slot, g.db)
	if source, chosen := chooseCatchUpSource(cluster.Position{}, g.offers()); chosen {
		t.Fatalf("a node that holds nothing chose %s, whose pair is deferred, as its catch-up source", source)
	}
	// The pair beside it is answered as ever (G-4).
	if calm := g.position(g.calmSlot, g.calmDB); calm.State != PositionStateReady || calm.AppliedPos.Zero() {
		t.Fatalf("the pair that is not deferred answered state %d position %s, want ready and its position", calm.State, calm.AppliedPos)
	}
}

// gateLink is a new owner's data channel on which one old copy is a real node with a deferred
// pair: its answer is whatever that node's data channel handler gives, carried the way the peer
// channel carries it (a refusal's text, without its code). Every other copy answers from the
// u33Link's table.
type gateLink struct {
	*u33Link
	oldID string
	old   *gateNode
}

func (l gateLink) QueryPosition(ctx context.Context, peer string, query PositionQuery) (PositionReply, error) {
	if peer != l.oldID {
		return l.u33Link.QueryPosition(ctx, peer, query)
	}
	reply, err := l.old.askPosition(l.old.slot, l.old.db)
	if err != nil {
		return PositionReply{}, errors.New(err.Error())
	}
	answered, err := DecodePositionReply(reply.Body)
	if err != nil {
		return PositionReply{}, err
	}
	answered.Slot, answered.DB = query.Slot, query.DB
	return answered, nil
}

// newGateOwner is new owner id of a move whose old copy oldID is the deferred node old.
func newGateOwner(id, oldID string, old *gateNode) *u33Node {
	node := &u33Node{id: id, pairs: &cluster.PairSet{}, link: newU33Link(), catch: &u33Catch{}, drain: &u33Drain{}, clock: &u33Clock{}}
	node.h = NewHandoff(HandoffConfig{
		Self: id, Pairs: node.pairs, Link: gateLink{u33Link: node.link, oldID: oldID, old: old}, CatchUp: node.catch, Drain: node.drain, Clock: node.clock, Retry: u33Retry,
	})
	return node
}

// wantShortfall fails unless Target held: the shortfall error, naming the replaying copy, and
// nothing published.
func wantShortfall(t *testing.T, what string, owner *u33Node, slot uint16, fromEpoch uint64) {
	t.Helper()
	target, err := owner.h.Target(context.Background(), slot, u33DB, fromEpoch)
	if err == nil {
		t.Fatalf("%s: Target published %s over a copy whose pair is deferred; that copy holds acknowledged records above any position it could give", what, target)
	}
	if !strings.Contains(err.Error(), "needs ready answers from") {
		t.Fatalf("%s: Target failed with %v, want the shortfall error", what, err)
	}
	if cached, known := owner.h.CachedTarget(slot, u33DB); known {
		t.Fatalf("%s: a target (%s) is published although Target reported a shortfall", what, cached)
	}
	if owner.h.Degraded(slot, u33DB) {
		t.Fatalf("%s: the pair is marked degraded although no target was published", what)
	}
}

// TestHandoffTargetHoldsOverADeferredCopy is G2 (rows 1 and 2, invariant G-2): Target never
// publishes while a deferred copy is the only live copy not counted, in the branch with
// enough copies alive and in the degraded branch, whether the copy is a peer or this node.
// [BREAK: answer "not ready" instead of an error: (b) publishes; return errCopyNotReady for
// this node's own deferred pair: (c) publishes]
func TestHandoffTargetHoldsOverADeferredCopy(t *testing.T) {
	g := newGateNode(t)
	first := u33State(1, 3, u33Active("A", "C", "D"))
	ready := func(pos cluster.Position) PositionReply {
		return PositionReply{State: PositionStateReady, AppliedPos: pos}
	}

	// (a) Three old copies alive, two answers needed: A is deferred, C answers, D is silent.
	enough := u33State(2, 3, u33Active("A", "B", "C", "D"))
	slotA := u33Moved(t, first, enough, 3, "A", "B")[0]
	ownerA := newGateOwner("B", "A", g)
	ownerA.h.OnEpoch(first)
	ownerA.h.OnEpoch(enough)
	u33Stopped(t, ownerA, "A", 2, slotA)
	ownerA.link.replies["C"] = ready(g.low)
	ownerA.link.silent["D"] = true
	wantShortfall(t, "(a) enough copies alive, one deferred and one silent", ownerA, slotA, 1)

	// (b) The degraded branch: C and D are committed down, so A is the only old copy alive and
	// fewer copies exist than the two answers needed.
	lost := u33State(2, 3, []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", cluster.MemberActive),
		u33Member("C", cluster.MemberDown), u33Member("D", cluster.MemberDown)})
	slotB := u33Moved(t, first, lost, 3, "A", "B")[0]
	ownerB := newGateOwner("B", "A", g)
	ownerB.h.OnEpoch(first)
	ownerB.h.OnEpoch(lost)
	u33Stopped(t, ownerB, "A", 2, slotB)
	wantShortfall(t, "(b) the deferred copy is the only old copy alive", ownerB, slotB, 1)

	// (d) After the release both publish the copy's real position.
	g.release()
	for _, tc := range []struct {
		what     string
		owner    *u33Node
		slot     uint16
		degraded bool
	}{
		{"(d) after the release, enough copies alive", ownerA, slotA, false},
		{"(d) after the release, the degraded branch", ownerB, slotB, true},
	} {
		target, err := tc.owner.h.Target(context.Background(), tc.slot, u33DB, 1)
		if err != nil || target != g.tail {
			t.Fatalf("%s: Target = %s, %v; want the released copy's real position %s", tc.what, target, err, g.tail)
		}
		if cached, known := tc.owner.h.CachedTarget(tc.slot, u33DB); !known || cached != g.tail {
			t.Fatalf("%s: the published target is %s (known %v), want %s", tc.what, cached, known, g.tail)
		}
		if got := tc.owner.h.Degraded(tc.slot, u33DB); got != tc.degraded {
			t.Fatalf("%s: degraded = %v, want %v", tc.what, got, tc.degraded)
		}
	}
}

// TestHandoffTargetHoldsOverItsOwnDeferredPair is G2 (c) (row 2): the only old copy alive is
// this node, and its own pair is deferred. Its own low position is not an answer, and the
// refusal is a plain error: "not ready" would account for the copy and open the pair degraded.
// [BREAK: drop the check in positionOf; return errCopyNotReady for it; check Ready first]
func TestHandoffTargetHoldsOverItsOwnDeferredPair(t *testing.T) {
	applied := cluster.Position{Epoch: 1, Seq: 7}
	node, slot := u5717Survivor(t, applied, false)
	var deferred atomic.Bool
	deferred.Store(true)
	node.h.cfg.Deferred = func(s uint16, db uint64) bool { return deferred.Load() && s == slot && db == u33DB }
	node.clock.set(u33Bound)

	if err := node.h.Tick(context.Background()); err == nil {
		t.Error("Tick reported nothing while this node's own deferred pair was the only copy to ask")
	}
	if target, known := node.h.CachedTarget(slot, u33DB); known {
		t.Fatalf("a target (%s) was published from this node's own deferred pair: its log holds acknowledged records above it", target)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("the pair opened on this node's own deferred copy: flags %05b", flags)
	}
	node.catch.mu.Lock()
	for _, call := range node.catch.calls {
		if call == (pairKey{slot, u33DB}) {
			t.Error("a catch-up of the pair ran before any copy had answered")
		}
	}
	node.catch.mu.Unlock()
	for _, readyToo := range []bool{true, false} {
		node.h.cfg.Ready = func(uint16, uint64) bool { return readyToo }
		_, _, err := node.h.positionOf(context.Background(), "C", slot, u33DB)
		wantReplayRefusal(t, "this node's own position of its deferred pair", err, "C", slot, u33DB)
		if errors.Is(err, errCopyNotReady) {
			t.Fatalf("this node's own deferred pair (record ready: %v) answered errCopyNotReady: Target accounts for such a copy and leaves it out, which opens the pair without it", readyToo)
		}
	}
	node.h.cfg.Ready = nil

	// Released. A Tick advances a bounded number of awaiting pairs, in rotation, and this node
	// took over every slot of the lost owners, so the pair's turn comes after a few Ticks.
	deferred.Store(false)
	for tick := 0; tick < 200 && u33Flags(node.pairs, slot)&flagAwaiting != 0; tick++ {
		if err := node.h.Tick(context.Background()); err != nil {
			t.Fatalf("Tick after the release: %.300s", err)
		}
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("after the release the pair's flags are %05b, want the awaiting hold cleared and the stop proof set", flags)
	}
	if target, known := node.h.CachedTarget(slot, u33DB); !known || target != applied || !node.h.Degraded(slot, u33DB) {
		t.Fatalf("after the release the published target is %s (known %v, degraded %v), want this node's own position %s, degraded", target, known, node.h.Degraded(slot, u33DB), applied)
	}
}

// deferredSource is a catch-up source whose pair a restart left deferred: its log holds
// 7:1 to 7:5, its store and applied position stop at 7:3, and the cluster committed epoch 8
// meanwhile, so the appended position it would report is the low one.
func deferredSource(t *testing.T, deferred *atomic.Bool) *u32Node {
	t.Helper()
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) {
		cfg.Deferred = func(slot uint16, db uint64) bool { return deferred.Load() && slot == u32Slot && db == u32DB }
	})
	source.ownRun(u32Epoch, 1, 5)
	source.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) {
		p.AppliedPos = u32Pos(u32Epoch, 3)
		p.Flags |= flagReplayNeeded | flagBootReplay
	})
	source.committed.Store(u32Epoch + 1)
	return source
}

// TestADeferredSourceNeverAnswersDivergent is G3 (row 4): the deferred pair's unapplied tail is
// in an older epoch than the committed one, and the requester holds more than the source has
// applied. The catch-up is answered with the error before anything is read: never Divergent,
// which makes the requester reset a good copy, and never CaughtUp.
// [BREAK: gate only servePosition; drop the check in serveCatchUp]
func TestADeferredSourceNeverAnswersDivergent(t *testing.T) {
	var deferred atomic.Bool
	deferred.Store(true)
	source := deferredSource(t, &deferred)
	held := u32Pos(u32Epoch, 5)
	request := CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "requester", Epoch: u32Epoch + 1, From: positionAfter(held)}
	for _, serve := range []struct {
		name string
		call func(context.Context, CatchUpRequest) (CatchUpChunk, error)
	}{
		{"ServeCatchUp", source.rep.ServeCatchUp},
		{"ServePinnedCatchUp", source.rep.ServePinnedCatchUp},
	} {
		chunk, err := serve.call(context.Background(), request)
		if err == nil {
			t.Fatalf("%s answered a requester at %s from a deferred pair applied at 7:3: %+v (divergent %v, caught up %v); a divergent answer makes the requester reset a copy that holds more than the source has applied",
				serve.name, held, chunk, chunk.Divergent, chunk.CaughtUp)
		}
		wantReplayRefusal(t, serve.name+" for the deferred pair", err, "source", u32Slot, u32DB)
		if chunk.Divergent || chunk.CaughtUp || len(chunk.Records) != 0 || !chunk.SourceAppended.Zero() {
			t.Fatalf("%s returned %+v beside its refusal; no position may leave a deferred pair", serve.name, chunk)
		}
	}
	if pins := source.rep.PinnedFloors(); len(pins) != 0 {
		t.Fatalf("the refused catch-ups left %d log floors pinned: the refusal must come before anything is read or pinned", len(pins))
	}

	// The requester's side: its copy holds 7:1 to 7:5 and is not reset by the refusal.
	requester := newU32Node(t, "requester", nil)
	for seq := uint64(1); seq <= 5; seq++ {
		if reply := requester.write(u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{})); reply.Ack.Outcome != AckApplied {
			t.Fatalf("the requester did not apply 7:%d: %+v", seq, reply.Ack)
		}
	}
	requester.committed.Store(u32Epoch + 1)
	requester.catchUpFrom(source, source)
	err := requester.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB)
	if resets := requester.seeds.count(); resets != 0 || errors.Is(err, ErrResetAndSeed) {
		t.Fatalf("a catch-up from a deferred source reset the requester's copy %d times (%v); it holds %s and the source's log holds the same", resets, err, held)
	}
	if err == nil {
		t.Fatal("a catch-up from a deferred source reported success; the source gave no position, so nothing says the copy is current")
	}
	if got := requester.applied(); got != held {
		t.Fatalf("the requester's applied position is %s after the refused catch-up, want %s untouched", got, held)
	}
	if requester.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("the refused catch-up marked the requester's pair current")
	}

	// Released: the source's replay has applied its tail, and the same request is answered.
	source.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) {
		p.AppliedPos = held
		p.Flags &^= flagReplayNeeded | flagBootReplay
	})
	deferred.Store(false)
	chunk, err := source.rep.ServeCatchUp(context.Background(), request)
	if err != nil || chunk.Divergent || !current(chunk) {
		t.Fatalf("after the release the catch-up answered %+v, %v; want caught up at %s", chunk, err, held)
	}
}

// TestADeferredNodeRefusesACatchUp is G3's wiring half: the node hands its deferred set to the
// replicator, so the data channel refuses a catch-up of the deferred pair and serves the pair
// beside it. [BREAK: leave ReplicatorConfig.Deferred unset in Node.peers]
func TestADeferredNodeRefusesACatchUp(t *testing.T) {
	g := newGateNode(t)
	request := CatchUpRequest{Slot: g.slot, DB: g.db, Node: "peer", Epoch: g.node.topology.Committed(), From: positionAfter(g.low)}
	chunk, err := g.node.answerCatchUp(context.Background(), request)
	if err == nil {
		t.Fatalf("the node served a catch-up of its deferred pair from its applied position %s: %+v", g.low, chunk)
	}
	wantReplayRefusal(t, "the catch-up of the deferred pair", err, g.node.cfg.NodeID, g.slot, g.db)
	calm := g.position(g.calmSlot, g.calmDB).AppliedPos
	if chunk, err := g.node.answerCatchUp(context.Background(), CatchUpRequest{Slot: g.calmSlot, DB: g.calmDB, Node: "peer", Epoch: g.node.topology.Committed(), From: positionAfter(calm)}); err != nil || !current(chunk) {
		t.Fatalf("the catch-up of the pair that is not deferred answered %+v, %v; want caught up at %s", chunk, err, calm)
	}
}

// TestADeferredPairAcceptsNoSeedRegistration is G4 (row 5): a new registration and a resume
// are refused with the error and leave no pin and no open snapshot. The puller reads an error
// as a call that failed: it keeps its attempt and asks again (Puller.callFailed), where a
// SEED_RESTART would make it reset its range.
// [BREAK: drop the check in register; check after pinFor]
func TestADeferredPairAcceptsNoSeedRegistration(t *testing.T) {
	rig := newT2Rig(t, SeedOptions{})
	rig.write([]byte("a"))
	cut := rig.write([]byte("b"))
	var deferred atomic.Bool
	deferred.Store(true)
	rig.src.cfg.Deferred = func(slot uint16, db uint64) bool { return deferred.Load() && slot == t2Slot && db == t2DB }

	register := func(id byte, mask byte, cursor []byte) (*transport.Message, error) {
		msg := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: rig.epoch.Load(), Slot: t2Slot, DBID: t2DB}, SeedID: t2ID(id), Mask: mask, Cursor: cursor}))
		return rig.src.Serve("n2", msg)
	}
	for _, attempt := range []struct {
		name   string
		mask   byte
		cursor []byte
	}{
		{"a new registration", 0, nil},
		{"a resume", seedMaskResume, t2Key(1)},
	} {
		reply, err := register(1, attempt.mask, attempt.cursor)
		if reply != nil || err == nil {
			t.Fatalf("%s for a deferred pair was answered with kind 0x%02x (%v), want the refusal as an error: an accept pins the low position %s, and a SEED_RESTART makes the puller reset its range", attempt.name, t2Kind(reply), err, cut)
		}
		wantReplayRefusal(t, attempt.name+" for the deferred pair", err, "n1", t2Slot, t2DB)
		if pins, open := rig.src.Pins(), rig.kv.open.Load(); pins != 0 || open != 0 {
			t.Fatalf("%s for a deferred pair left %d pins and %d open snapshots, want none", attempt.name, pins, open)
		}
	}

	deferred.Store(false)
	reply, err := register(1, 0, nil)
	if err != nil {
		t.Fatalf("a registration after the release: %v", err)
	}
	if accept := rig.mustAccept(reply); accept.Cut != seedPosition(cut) {
		t.Fatalf("the registration after the release was cut at %+v, want the pair's position %s", accept.Cut, cut)
	}
}

// TestADeferredNodeAcceptsNoSeedRegistration is G4's wiring half: the node hands its deferred
// set to its seed source. [BREAK: leave SeedSourceConfig.Deferred unset in newSeedSource]
func TestADeferredNodeAcceptsNoSeedRegistration(t *testing.T) {
	g := newGateNode(t)
	reply, err := g.registerSeed(g.slot, g.db, 1)
	if reply != nil || err == nil {
		t.Fatalf("the node answered a seed registration for its deferred pair with kind 0x%02x (%v), want the refusal as an error", t2Kind(reply), err)
	}
	wantReplayRefusal(t, "the seed registration for the deferred pair", err, g.node.cfg.NodeID, g.slot, g.db)
	if pins := g.node.cl.seed.Pins(); pins != 0 {
		t.Fatalf("the refused registration left %d pins", pins)
	}
}

// TestNoHandoffReadyForADeferredPair is G5 (row 3): the awaiting side sends no HANDOFF_READY
// naming a pair that is deferred on it, because the message carries the pair's applied
// position and the old owner latches its drain there. The READY goes out, with the real
// position, on the old owner's next request after the release.
// [BREAK: drop the check in Handoff.queue]
func TestNoHandoffReadyForADeferredPair(t *testing.T) {
	low := cluster.Position{Epoch: 1, Seq: 10}
	node, slots := u33Awaiting(t, low)
	held, free := slots[0], slots[1]
	var deferred atomic.Bool
	deferred.Store(true)
	node.h.cfg.Deferred = func(s uint16, db uint64) bool { return deferred.Load() && s == held && db == u33DB }
	names := func(slot uint16) (count int, applied cluster.Position) {
		node.link.mu.Lock()
		defer node.link.mu.Unlock()
		for _, ready := range node.link.readies {
			for _, named := range SlotsOfBitmap(ready.msg.Bitmap) {
				if named == slot {
					count++
					applied = ready.msg.AppliedPos
				}
			}
		}
		return count, applied
	}

	for _, slot := range []uint16{held, free} {
		u33SetApplied(node.pairs, slot, u33DB, low)
		u33Stopped(t, node, "A", 2, slot)
		if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if err := node.h.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if count, applied := names(held); count != 0 {
		t.Fatalf("%d HANDOFF_READY messages name the deferred pair, carrying %s: the old owner latches its drain at a position below what this node acknowledged", count, applied)
	}
	if count, applied := names(free); count != 1 || applied != low {
		t.Fatalf("the pair that is not deferred was named in %d HANDOFF_READY messages (position %s), want one at %s", count, applied, low)
	}

	// The replay finishes: the pair's applied position is its real one, and the old owner, which
	// never got a READY, is still asking.
	replayed := cluster.Position{Epoch: 1, Seq: 14}
	u33SetApplied(node.pairs, held, u33DB, replayed)
	deferred.Store(false)
	request := HandoffReq{FromEpoch: 1, ToEpoch: 2, Bitmap: BitmapOfSlots([]uint16{held})}
	if err := node.h.OnHandoffReq("A", u33Env(kindHandoffReq, 2, u33DB), request); err != nil {
		t.Fatal(err)
	}
	if count, applied := names(held); count != 1 || applied != replayed {
		t.Fatalf("after the release the old owner's next request was answered with %d HANDOFF_READY messages naming the pair (position %s), want one at %s", count, applied, replayed)
	}
}

// TestTheNodeHandsItsDeferredSetToTheHandoff is the wiring half of G2 (c) and G5 (rows 2 and
// 3): the Handoff of a node asks the node's own deferred set, while the pair is deferred and
// after its release. [BREAK: leave HandoffConfig.Deferred unset in Node.wire]
func TestTheNodeHandsItsDeferredSetToTheHandoff(t *testing.T) {
	g := newGateNode(t)
	hook := g.node.handoff.cfg.Deferred
	if hook == nil {
		t.Fatal("the node's Handoff has no Deferred hook: its own deferred pair would count as an answer and be named in a HANDOFF_READY")
	}
	if !hook(g.slot, g.db) || hook(g.calmSlot, g.calmDB) {
		t.Fatalf("the Handoff sees deferred = %v for the deferred pair and %v for the pair beside it, want true and false", hook(g.slot, g.db), hook(g.calmSlot, g.calmDB))
	}
	_, _, err := g.node.handoff.positionOf(context.Background(), g.node.cfg.NodeID, g.slot, g.db)
	wantReplayRefusal(t, "the node's own position of its deferred pair", err, g.node.cfg.NodeID, g.slot, g.db)
	g.release()
	if hook(g.slot, g.db) {
		t.Fatal("the Handoff still sees the pair deferred after its release")
	}
	if own, _, err := g.node.handoff.positionOf(context.Background(), g.node.cfg.NodeID, g.slot, g.db); err != nil || own != g.tail {
		t.Fatalf("after the release the node's own position is %s, %v; want %s", own, err, g.tail)
	}
}

// TestTheGateEndsWithTheDeferral is G6 (invariants G-3 and G-4): after the release rows 1 to 5
// answer as before the change; a pair that is fenced without being deferred was never gated;
// and the rows the gate leaves alone still behave as they did for the deferred pair.
// [BREAK: never clear the gate; key the gate on flagReplayNeeded; gate the stored read]
func TestTheGateEndsWithTheDeferral(t *testing.T) {
	g := newGateNode(t)
	ctx := context.Background()
	target := StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}

	// A pair fenced at run time by a failed apply (flagReplayNeeded without a deferral) is not
	// the gate's: its low position is honest about what it acknowledged (spec section 8).
	g.node.pairs.With(g.calmSlot, g.calmDB, func(p *cluster.Pair) { p.Flags |= flagReplayNeeded })
	fenced := g.position(g.calmSlot, g.calmDB)
	g.node.pairs.With(g.calmSlot, g.calmDB, func(p *cluster.Pair) { p.Flags &^= flagReplayNeeded })
	if fenced.State != PositionStateReady || fenced.AppliedPos.Zero() {
		t.Fatalf("a pair fenced by flagReplayNeeded and not deferred answered state %d position %s, want its answer of before the change", fenced.State, fenced.AppliedPos)
	}

	// Rows 9 and 10: a client's read and write of the deferred pair are refused, naming the replay.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, readErr := g.node.Peek(ctx, PeekRequest{StoreTarget: target, Count: 100})
		writeErr := g.node.Push(ctx, PushRequest{StoreTarget: target, Element: bson.D{{Key: "item", Value: "early"}}})
		if readErr == nil || writeErr == nil {
			t.Fatalf("a client was served by a pair that has not replayed its log tail: read %v, write %v", readErr, writeErr)
		}
		if strings.Contains(readErr.Error(), "replaying this slot") && strings.Contains(writeErr.Error(), "replaying this slot") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the refusals of the deferred pair never named the replay: read %v, write %v", readErr, writeErr)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Row 11: a stored read forwarded by a peer is answered whatever the pair's fence says. It
	// is how another node's deferred pair reads a declaration, so it must stay ungated.
	encoded, err := store.EncodeComponent("Box")
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeForwardReadBody(ForwardReadBody{Stored: true, Req: ReadRequest{BucketID: store.BucketsBucketID, EncID: encoded}})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := g.node.serveData(ctx, transport.Handshake{NodeID: "peer"}, transport.Message{Env: g.envelope(transport.KindForwardRead, g.slot, g.db), Body: body})
	if err != nil || stored == nil || stored.Env.Kind != transport.KindForwardReadReply {
		t.Fatalf("a stored read of the deferred pair's slot was not answered: %v", err)
	}
	if _, _, err := DecodeForwardReadStored(stored.Body); err != nil {
		t.Fatalf("the stored read of the deferred pair's slot answered something that is not a stored document: %v", err)
	}

	// The release: every gated row answers as before the change.
	g.release()
	if answered := g.position(g.slot, g.db); answered.State != PositionStateReady || answered.AppliedPos != g.tail {
		t.Fatalf("after the release the position query answered state %d position %s, want ready at %s", answered.State, answered.AppliedPos, g.tail)
	}
	chunk, err := g.node.answerCatchUp(ctx, CatchUpRequest{Slot: g.slot, DB: g.db, Node: "peer", Epoch: g.node.topology.Committed(), From: positionAfter(g.tail)})
	if err != nil || chunk.Divergent || !current(chunk) {
		t.Fatalf("after the release a catch-up from %s answered %+v, %v; want caught up", g.tail, chunk, err)
	}
	if reply, err := g.registerSeed(g.slot, g.db, 2); err != nil || reply == nil {
		t.Fatalf("after the release a seed registration was still refused with an error: %v", err)
	}
	if got := bug0760Queue(t, g.node, u30iDB); len(got) != 2 || got[0] != "x1" || got[1] != "x2" {
		t.Fatalf("Queue after the release = %v, want [x1 x2]", got)
	}
}

// TestTheGateLeavesAcksAndTailShippingAlone is G6's part for rows 6 and 7, which read the
// applied position of a deferred pair and are deliberately unchanged: a refused frame's ack
// carries the low position with "not applied" (conservative), and the tail is shipped from the
// low position (a copy refuses a duplicate by position).
func TestTheGateLeavesAcksAndTailShippingAlone(t *testing.T) {
	low := u32Pos(u32Epoch, 3)

	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Deferred = func(uint16, uint64) bool { return true } })
	for seq := uint64(1); seq <= 3; seq++ {
		copyNode.write(u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{}))
	}
	copyNode.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) { p.Flags |= flagReplayNeeded | flagBootReplay })
	frame := u32Frame(t, u32Epoch, u32Pos(u32Epoch, 4), cluster.Position{})
	op, err := cluster.DecodeOp(frame.Record.Body)
	if err != nil {
		t.Fatal(err)
	}
	ack, applied := copyNode.rep.applyQueued(pairKey{u32Slot, u32DB}, queuedWrite{frame: frame, op: op})
	if applied || ack.Outcome != AckNotApplied || ack.Pos != low {
		t.Fatalf("row 6: a frame for a deferred pair was acknowledged %+v (applied %v), want not applied at %s as before the change", ack, applied, low)
	}

	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) { cfg.Deferred = func(uint16, uint64) bool { return true } })
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)
	owner.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) { p.Flags |= flagReplayNeeded | flagBootReplay })
	owner.rep.ShipTail(u32Slot, u32DB)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if frames := owner.link.sent("copy"); len(frames) != 1 || frames[0].Pos != low {
		t.Fatalf("row 7: ShipTail of a deferred pair sent %v, want its one tail frame at %s as before the change", frames, low)
	}
}
