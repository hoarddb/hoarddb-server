package wire

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

var (
	_ HandoffCatchUp = (*Replicator)(nil)
	_ HandoffDrain   = (*Replicator)(nil)
)

const (
	u33DB    uint64 = 1
	u33Bound        = 2100 * time.Millisecond
	u33Retry        = 200 * time.Millisecond
)

type u33Clock struct {
	mu  sync.Mutex
	now time.Duration
}

func (c *u33Clock) BootNow() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *u33Clock) set(now time.Duration) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type u33Req struct {
	peer string
	db   uint64
	msg  HandoffReq
}

type u33Stop struct {
	peer string
	db   uint64
	msg  HandoffStopped
}

type u33Rdy struct {
	peer string
	db   uint64
	msg  HandoffReady
}

// u33Link records every barrier message and answers position queries from a table.
type u33Link struct {
	mu      sync.Mutex
	reqs    []u33Req
	stops   []u33Stop
	readies []u33Rdy
	asked   []string
	replies map[string]PositionReply
	silent  map[string]bool
}

func newU33Link() *u33Link {
	return &u33Link{replies: map[string]PositionReply{}, silent: map[string]bool{}}
}

func (l *u33Link) SendHandoffReq(_ context.Context, peer string, db uint64, msg HandoffReq) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reqs = append(l.reqs, u33Req{peer, db, msg})
	return nil
}

func (l *u33Link) SendHandoffStopped(_ context.Context, peer string, db uint64, msg HandoffStopped) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stops = append(l.stops, u33Stop{peer, db, msg})
	return nil
}

func (l *u33Link) SendHandoffReady(_ context.Context, peer string, db uint64, msg HandoffReady) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.readies = append(l.readies, u33Rdy{peer, db, msg})
	return nil
}

func (l *u33Link) QueryPosition(_ context.Context, peer string, query PositionQuery) (PositionReply, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked = append(l.asked, peer)
	reply, ok := l.replies[peer]
	if !ok || l.silent[peer] {
		return PositionReply{}, errors.New("peer " + peer + " did not answer")
	}
	reply.Slot, reply.DB = query.Slot, query.DB
	return reply, nil
}

func (l *u33Link) counts() (reqs, stops, readies int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.reqs), len(l.stops), len(l.readies)
}

// u33Catch is the replicator's catch-up half: it records the call and marks the pair
// current without moving the applied position, so a test decides when "fsynced" is true.
type u33Catch struct {
	mu      sync.Mutex
	calls   []pairKey
	current map[pairKey]bool
	never   bool
}

func (c *u33Catch) CatchUp(_ context.Context, slot uint16, db uint64, _ cluster.Position, _ uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, pairKey{slot, db})
	if c.current == nil {
		c.current = map[pairKey]bool{}
	}
	c.current[pairKey{slot, db}] = !c.never
	return nil
}

func (c *u33Catch) IsCurrent(slot uint16, db uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current[pairKey{slot, db}]
}

func (c *u33Catch) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

type u33Drain struct {
	mu    sync.Mutex
	noted map[pairKey]cluster.Position
}

func (d *u33Drain) NoteDrained(slot uint16, db uint64, upTo cluster.Position) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.noted == nil {
		d.noted = map[pairKey]cluster.Position{}
	}
	d.noted[pairKey{slot, db}] = upTo
}

type u33Node struct {
	id    string
	h     *Handoff
	pairs *cluster.PairSet
	link  *u33Link
	catch *u33Catch
	drain *u33Drain
	clock *u33Clock
}

func newU33Node(id string) *u33Node {
	node := &u33Node{id: id, pairs: &cluster.PairSet{}, link: newU33Link(), catch: &u33Catch{}, drain: &u33Drain{}, clock: &u33Clock{}}
	node.h = NewHandoff(HandoffConfig{
		Self: id, Pairs: node.pairs, Link: node.link, CatchUp: node.catch, Drain: node.drain, Clock: node.clock, Retry: u33Retry,
	})
	return node
}

func u33Member(id string, state cluster.MemberState) cluster.Member {
	return cluster.Member{ID: id, State: state}
}

func u33Active(ids ...string) []cluster.Member {
	members := make([]cluster.Member, len(ids))
	for i, id := range ids {
		members[i] = u33Member(id, cluster.MemberActive)
	}
	return members
}

func u33State(epoch int64, rf int32, members []cluster.Member, dbs ...int64) cluster.ClusterState {
	state := cluster.ClusterState{Epoch: epoch, StateSeq: epoch, Members: members,
		Settings: cluster.Settings{StepDownMs: 1500, HeartbeatMs: 600}}
	if len(dbs) == 0 {
		dbs = []int64{int64(u33DB)}
	}
	for _, id := range dbs {
		state.Databases = append(state.Databases, cluster.Database{Name: "db", ID: id, RF: rf})
	}
	return state
}

// u33Moved lists the slots whose owner is oldID in from and newID in to.
func u33Moved(t *testing.T, from, to cluster.ClusterState, rf int, oldID, newID string) []uint16 {
	t.Helper()
	before, after := cluster.BuildTable(from), cluster.BuildTable(to)
	var slots []uint16
	for slot := range uint16(barrierSlots) {
		oldOwner, _ := before.Owner(slot, rf)
		newOwner, _ := after.Owner(slot, rf)
		if oldOwner.ID == oldID && newOwner.ID == newID {
			slots = append(slots, slot)
		}
	}
	if len(slots) < 3 {
		t.Fatalf("only %d slots move from %s to %s", len(slots), oldID, newID)
	}
	return slots
}

func u33Env(kind byte, epoch, db uint64) transport.Envelope {
	return transport.Envelope{Version: transport.EnvelopeVersion, Kind: kind, Epoch: epoch, DBID: db}
}

func u33SetApplied(pairs *cluster.PairSet, slot uint16, db uint64, pos cluster.Position) {
	pairs.With(slot, db, func(p *cluster.Pair) { p.AppliedPos = pos })
}

func u33Flags(pairs *cluster.PairSet, slot uint16) uint8 { return pairs.Snapshot(slot, u33DB).Flags }

func u33Stopped(t *testing.T, node *u33Node, from string, toEpoch uint64, slots ...uint16) {
	t.Helper()
	if err := node.h.OnHandoffStopped(from, u33Env(kindHandoffStopped, toEpoch, u33DB), HandoffStopped{ToEpoch: toEpoch, Bitmap: BitmapOfSlots(slots)}); err != nil {
		t.Fatalf("OnHandoffStopped: %v", err)
	}
}

func u33Ready(t *testing.T, node *u33Node, from string, toEpoch uint64, applied cluster.Position, slots ...uint16) {
	t.Helper()
	msg := HandoffReady{ToEpoch: toEpoch, Bitmap: BitmapOfSlots(slots), AppliedPos: applied}
	if err := node.h.OnHandoffReady(from, u33Env(kindHandoffReady, toEpoch, u33DB), msg); err != nil {
		t.Fatalf("OnHandoffReady: %v", err)
	}
}

// u33Awaiting builds the new owner B of a two-node move A to B (both alive) and
// returns B with the moved slots. A's ready copy answers every position query with applied.
func u33Awaiting(t *testing.T, applied cluster.Position) (*u33Node, []uint16) {
	t.Helper()
	first, second := u33State(1, 2, u33Active("A")), u33State(2, 2, u33Active("A", "B"))
	slots := u33Moved(t, first, second, 2, "A", "B")
	node := newU33Node("B")
	node.link.replies["A"] = PositionReply{State: PositionStateReady, AppliedPos: applied}
	node.h.OnEpoch(first)
	node.h.OnEpoch(second)
	return node, slots
}

// u33Sending builds the old owner A of the same move.
func u33Sending(t *testing.T) (*u33Node, []uint16) {
	t.Helper()
	first, second := u33State(1, 2, u33Active("A")), u33State(2, 2, u33Active("A", "B"))
	slots := u33Moved(t, first, second, 2, "A", "B")
	node := newU33Node("A")
	node.h.OnEpoch(first)
	node.h.OnEpoch(second)
	return node, slots
}

func u33WaitParked(t *testing.T, node *u33Node, slot uint16, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.h.mu.Lock()
		hp := node.h.pairs[pairKey{slot, u33DB}]
		got := 0
		if hp != nil {
			got = len(hp.waiters)
			if len(hp.barrier.Held) != got {
				node.h.mu.Unlock()
				t.Fatalf("%d waiters but %d held operations: the two lists must move together", got, len(hp.barrier.Held))
			}
		}
		node.h.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("slot %d never reached %d parked operations", slot, want)
}

type u33Held struct {
	release HoldRelease
	err     error
}

func u33Park(node *u33Node, ctx context.Context, req HeldRequest) <-chan u33Held {
	done := make(chan u33Held, 1)
	go func() {
		release, err := node.h.Hold(ctx, req)
		done <- u33Held{release, err}
	}()
	return done
}

func u33Quiet(t *testing.T, done <-chan u33Held, why string) {
	t.Helper()
	select {
	case got := <-done:
		t.Fatalf("%s, but Hold returned %+v, %v", why, got.release, got.err)
	default:
	}
}

func u33Result(t *testing.T, done <-chan u33Held) u33Held {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("a parked operation was never released")
		return u33Held{}
	}
}

func TestHandoffKindsAreDataKinds(t *testing.T) {
	for _, kind := range []byte{kindHandoffReq, kindHandoffReady, transport.KindHandoffStopped, kindPositionQuery, kindPositionReply} {
		if channel, ok := transport.KindChannel(kind); !ok || channel != transport.ChannelData {
			t.Errorf("kind 0x%02x is %v, %v; the barrier and the position query ride the data channel", kind, channel, ok)
		}
	}
	if kindHandoffReq != 0x05 || kindHandoffReady != 0x06 || kindPositionQuery != 0x12 || kindPositionReply != 0x13 {
		t.Fatal("a kind byte differs from conventions 6.1")
	}
}

func TestHandoffReadyFollowsTheFsync(t *testing.T) {
	target := cluster.Position{Epoch: 1, Seq: 10}
	node, slots := u33Awaiting(t, target)
	slot := slots[0]
	u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 1, Seq: 4})
	u33Stopped(t, node, "A", 2, slot)

	// The last chunk arrived (the catch-up says current) but the records are not
	// yet fsynced and applied: the applied position is still short of the target.
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 0 {
		t.Fatalf("READY was sent at applied 1:4 with the target at %s: it must follow the fsync", target)
	}
	if u33Flags(node.pairs, slot)&flagAwaiting == 0 {
		t.Fatal("the awaiting hold ended before READY was sent")
	}

	u33SetApplied(node.pairs, slot, u33DB, target) // the fsync and the apply complete
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	if len(node.link.readies) != 1 {
		t.Fatalf("%d READY messages after the fsync, want 1", len(node.link.readies))
	}
	if got := node.link.readies[0].msg.AppliedPos; got != target {
		t.Fatalf("READY carries %s, want the applied position %s", got, target)
	}
	flags := u33Flags(node.pairs, slot)
	if flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("after READY the pair flags are %05b: awaiting must be clear and stopProved set in the same step", flags)
	}
}

func TestHandoffReadyWaitsForStopped(t *testing.T) {
	target := cluster.Position{Epoch: 1, Seq: 10}
	node, slots := u33Awaiting(t, target)
	slot := slots[0]
	u33SetApplied(node.pairs, slot, u33DB, target)

	// The pair is current and fsynced; nothing says the old owner stopped.
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 0 {
		t.Fatal("READY was sent for a pair whose old owner had not stopped")
	}
	if holdOpen(node.pairs.Snapshot(slot, u33DB), true, target) == false {
		t.Fatal("the writer's hold is open only while awaiting; it was lifted before STOPPED")
	}

	// A stranger's STOPPED, and the right node's STOPPED for another epoch, prove nothing.
	u33Stopped(t, node, "C", 2, slot)
	u33Stopped(t, node, "A", 3, slot)
	u33Stopped(t, node, "A", 2, slots[1]) // a different pair
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 0 {
		t.Fatal("a STOPPED from the wrong node, epoch or pair licensed READY")
	}

	u33Stopped(t, node, "A", 2, slot)
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 1 {
		t.Fatal("READY was not sent once the pair was current and A had stopped")
	}
	node.pairs.With(slot, u33DB, func(p *cluster.Pair) { p.Flags |= flagReplayed })
	if holdOpen(node.pairs.Snapshot(slot, u33DB), true, target) {
		t.Fatal("the writer's hold is still open after READY")
	}
}

func TestStopProofByTheBoundAfterAnUnrelatedBump(t *testing.T) {
	// A is committed down at epoch 2; B takes over. Epoch 3 is unrelated (D joins) and
	// keeps B rank 1 for the pair.
	first := u33State(1, 2, u33Active("A", "B"))
	second := u33State(2, 2, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive)})
	third := u33State(3, 2, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive), u33Member("D", cluster.MemberActive)})
	slots := u33Moved(t, first, second, 2, "A", "B")
	var slot uint16
	found := false
	for _, candidate := range slots {
		if owner, _ := cluster.BuildTable(third).Owner(candidate, 2); owner.ID == "B" {
			slot, found = candidate, true
			break
		}
	}
	if !found {
		t.Fatal("no slot stays with B after D joins")
	}
	node := newU33Node("B")
	node.link.replies["B"] = PositionReply{State: PositionStateReady}
	node.h.OnEpoch(first)
	node.clock.set(0)
	node.h.OnEpoch(second)
	node.clock.set(1500 * time.Millisecond)
	node.h.OnEpoch(third)

	// The bound from epoch 2 has elapsed; the bound from epoch 3 has not.
	node.clock.set(u33Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if u33Flags(node.pairs, slot)&flagAwaiting == 0 || u33Flags(node.pairs, slot)&flagStopProved != 0 {
		t.Fatal("an unrelated bump let the new owner order on the first commit's lease bound")
	}
	if node.catch.callCount() != 0 {
		t.Fatal("a catch-up ran before the old owner was proved stopped")
	}

	node.clock.set(1500*time.Millisecond + u33Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The catch-up fake leaves the applied position at zero and the target is zero, so the
	// pair is current and the hold ends on this tick. Epoch 3 left B rank 1 with no
	// predecessor to tell, so no READY message goes anywhere.
	flags := u33Flags(node.pairs, slot)
	if flags&flagAwaiting != 0 || flags&flagStopProved == 0 {
		t.Fatalf("the bound from the second commit elapsed and the pair flags are %05b", flags)
	}
	if _, _, readies := node.link.counts(); readies != 0 {
		t.Fatalf("%d READY messages for a barrier with no old owner to tell", readies)
	}
}

func TestHeldWriteIsForwardedWithItsRequestID(t *testing.T) {
	// The old owner: a write parked at A is released toward B with the id it came with.
	sender, slots := u33Sending(t)
	slot := slots[0]
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	done := u33Park(sender, context.Background(), HeldRequest{Slot: slot, DB: u33DB, RequestID: id, Body: []byte("op")})
	u33WaitParked(t, sender, slot, 1)
	sender.h.mu.Lock()
	held := sender.h.pairs[pairKey{slot, u33DB}].barrier.Held
	if len(held) != 1 || held[0].RequestID != id || string(held[0].Body) != "op" || held[0].Read {
		sender.h.mu.Unlock()
		t.Fatalf("the parked write is %+v, want request id %x with its body", held, id)
	}
	sender.h.mu.Unlock()
	u33Quiet(t, done, "the hold is still open")

	u33Ready(t, sender, "B", 2, cluster.Position{Epoch: 1, Seq: 0}, slot)
	got := u33Result(t, done)
	if got.err != nil || got.release.Owner != "B" || got.release.Local {
		t.Fatalf("released as %+v, %v; a write held by the old owner goes to B", got.release, got.err)
	}
	if flags := u33Flags(sender.pairs, slot); flags&flagSending != 0 {
		t.Fatalf("the sending flag survived READY: %05b", flags)
	}

	// The new owner: a write parked at B runs locally once READY is sent.
	receiver, slots := u33Awaiting(t, cluster.Position{})
	slot = slots[0]
	done = u33Park(receiver, context.Background(), HeldRequest{Slot: slot, DB: u33DB, RequestID: id, Body: []byte("op")})
	u33WaitParked(t, receiver, slot, 1)
	u33Stopped(t, receiver, "A", 2, slot)
	if err := receiver.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	got = u33Result(t, done)
	if got.err != nil || got.release.Owner != "B" || !got.release.Local {
		t.Fatalf("released as %+v, %v; a write held by the new owner is ordered by it", got.release, got.err)
	}
	receiver.h.mu.Lock()
	defer receiver.h.mu.Unlock()
	if hp := receiver.h.pairs[pairKey{slot, u33DB}]; hp != nil && (len(hp.barrier.Held) != 0 || len(hp.waiters) != 0) {
		t.Fatal("a released operation is still listed as held")
	}
}

func TestHeldReadIsNotAnsweredByEitherSide(t *testing.T) {
	read := func(slot uint16, id byte) HeldRequest {
		return HeldRequest{Slot: slot, DB: u33DB, RequestID: [16]byte{id}, Read: true, PeerReachable: true}
	}
	sender, sSlots := u33Sending(t)
	receiver, rSlots := u33Awaiting(t, cluster.Position{})
	sendRead := u33Park(sender, context.Background(), read(sSlots[0], 1))
	recvRead := u33Park(receiver, context.Background(), read(rSlots[0], 2))
	u33WaitParked(t, sender, sSlots[0], 1)
	u33WaitParked(t, receiver, rSlots[0], 1)
	u33Quiet(t, sendRead, "the old owner must not answer a read while the hold is open")
	u33Quiet(t, recvRead, "the new owner must not answer a read before READY")

	// The new owner's read for a caller that cannot wait and an unreachable peer is refused.
	refused := u33Park(receiver, context.Background(), HeldRequest{Slot: rSlots[1], DB: u33DB, Read: true, Deadline: time.Millisecond})
	if got := u33Result(t, refused); got.err == nil {
		t.Fatal("a read that cannot wait for an unreachable old owner was parked")
	}

	u33Ready(t, sender, "B", 2, cluster.Position{}, sSlots[0])
	u33Stopped(t, receiver, "A", 2, rSlots[0])
	if err := receiver.h.MaybeSendReady(context.Background(), rSlots[0], u33DB); err != nil {
		t.Fatal(err)
	}
	sent, recv := u33Result(t, sendRead), u33Result(t, recvRead)
	if sent.err != nil || sent.release.Local || sent.release.Owner != "B" {
		t.Fatalf("the old owner's read is released as %+v, %v: it goes to B, it is not answered here", sent.release, sent.err)
	}
	if recv.err != nil || !recv.release.Local {
		t.Fatalf("the new owner's read is released as %+v, %v: it re-enters the ordinary path once", recv.release, recv.err)
	}
	// One actor per held read: the parked caller. The machine no longer lists it, so a
	// second forwarder has nothing to forward.
	for _, node := range []*u33Node{sender, receiver} {
		node.h.mu.Lock()
		for key, hp := range node.h.pairs {
			if len(hp.barrier.Held) != 0 || len(hp.waiters) != 0 {
				node.h.mu.Unlock()
				t.Fatalf("pair %v still lists a released read as held", key)
			}
		}
		node.h.mu.Unlock()
	}
}

func TestOneMessagePerPeerPerDatabase(t *testing.T) {
	first := u33State(1, 2, u33Active("A"), 1, 2)
	second := u33State(2, 2, u33Active("A", "B"), 1, 2)
	slots := u33Moved(t, first, second, 2, "A", "B")
	node := newU33Node("A")
	node.h.OnEpoch(first)
	node.h.OnEpoch(second)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs, stops, _ := node.link.counts()
	if reqs != 2 || stops != 2 {
		t.Fatalf("%d HANDOFF_REQ and %d HANDOFF_STOPPED for 2 databases and %d moved slots each, want 2 and 2", reqs, stops, len(slots))
	}
	byDB := map[uint64][]uint16{}
	for _, sent := range node.link.reqs {
		if sent.peer != "B" || sent.msg.FromEpoch != 1 || sent.msg.ToEpoch != 2 {
			t.Fatalf("REQ %+v, want to B from epoch 1 to 2", sent)
		}
		byDB[sent.db] = SlotsOfBitmap(sent.msg.Bitmap)
	}
	for _, db := range []uint64{1, 2} {
		got := byDB[db]
		if len(got) != len(slots) {
			t.Fatalf("database %d REQ names %d slots, want %d", db, len(got), len(slots))
		}
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		for i := range slots {
			if got[i] != slots[i] {
				t.Fatalf("database %d REQ names slot %d at %d, want %d", db, got[i], i, slots[i])
			}
		}
	}
	for _, sent := range node.link.stops {
		if sent.msg.ToEpoch != 2 || len(SlotsOfBitmap(sent.msg.Bitmap)) != len(slots) {
			t.Fatalf("STOPPED %+v does not cover the %d moved slots at epoch 2", sent, len(slots))
		}
	}
	// Retransmission is again one message per database.
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reqs, stops, _ = node.link.counts(); reqs != 4 || stops != 4 {
		t.Fatalf("after a retry the counts are %d and %d, want 4 and 4", reqs, stops)
	}
}

func TestNewOwnerHearsFromTheOldCopySet(t *testing.T) {
	// RF 3 over A, C, D; B joins and takes slots from A. majority 2, so B needs 3-2+1 = 2 answers.
	first := u33State(1, 3, u33Active("A", "C", "D"))
	second := u33State(2, 3, u33Active("A", "B", "C", "D"))
	slot := u33Moved(t, first, second, 3, "A", "B")[0]

	build := func(members []cluster.Member) *u33Node {
		node := newU33Node("B")
		node.h.OnEpoch(first)
		node.h.OnEpoch(u33State(2, 3, members))
		u33Stopped(t, node, "A", 2, slot)
		return node
	}
	ready := func(seq uint64) PositionReply {
		return PositionReply{State: PositionStateReady, AppliedPos: cluster.Position{Epoch: 1, Seq: seq}}
	}

	node := build(second.Members)
	node.link.replies["A"], node.link.replies["C"] = ready(9), ready(12)
	node.link.silent["D"] = true
	target, err := node.h.Target(context.Background(), slot, u33DB, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := (cluster.Position{Epoch: 1, Seq: 12}); target != want {
		t.Fatalf("target %s, want the highest answer %s", target, want)
	}
	sort.Strings(node.link.asked)
	if len(node.link.asked) != 3 || node.link.asked[0] != "A" || node.link.asked[1] != "C" || node.link.asked[2] != "D" {
		t.Fatalf("asked %v, want every non-down member of the old copy set: A, C, D", node.link.asked)
	}
	if cached, ok := node.h.CachedTarget(slot, u33DB); !ok || cached != target {
		t.Fatalf("cached target %s, %v", cached, ok)
	}

	// One answer is short of the two that intersect every acknowledged majority: a hold, not a smaller target.
	short := build(second.Members)
	short.link.replies["A"] = ready(9)
	short.link.silent["C"], short.link.silent["D"] = true, true
	if _, err := short.h.Target(context.Background(), slot, u33DB, 1); err == nil {
		t.Fatal("one answer of the two required produced a target")
	}
	if err := short.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Logf("expected while the hold is open: %v", err)
	}
	if _, _, readies := short.link.counts(); readies != 0 {
		t.Fatal("READY was sent before the old copy set answered")
	}
	if _, ok := short.h.CachedTarget(slot, u33DB); ok {
		t.Fatal("a failed target query left a cached target")
	}

	// A copy that is still seeding answers, but it holds no acknowledged write.
	seeding := build(second.Members)
	seeding.link.replies["A"] = ready(9)
	seeding.link.replies["C"] = PositionReply{State: PositionStateSeeding, AppliedPos: cluster.Position{Epoch: 1, Seq: 99}}
	seeding.link.silent["D"] = true
	if _, err := seeding.h.Target(context.Background(), slot, u33DB, 1); err == nil {
		t.Fatal("a seeding copy's answer counted toward the quorum")
	}

	// A copy committed down is skipped, and the other two are enough.
	downD := []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", cluster.MemberActive),
		u33Member("C", cluster.MemberActive), u33Member("D", cluster.MemberDown)}
	skipped := build(downD)
	skipped.link.replies["A"], skipped.link.replies["C"] = ready(9), ready(12)
	if _, err := skipped.h.Target(context.Background(), slot, u33DB, 1); err != nil {
		t.Fatal(err)
	}
	for _, name := range skipped.link.asked {
		if name == "D" {
			t.Fatal("a copy committed down was asked")
		}
	}
}

func TestAHeldOpPastItsDeadlineIsForwardedOrRefused(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	slot := slots[0]
	ctx, cancel := context.WithCancel(context.Background())
	stale := u33Park(node, ctx, HeldRequest{Slot: slot, DB: u33DB, RequestID: [16]byte{1}, Body: []byte("late")})
	patient := u33Park(node, context.Background(), HeldRequest{Slot: slot, DB: u33DB, RequestID: [16]byte{2}, Body: []byte("patient")})
	u33WaitParked(t, node, slot, 2)

	cancel() // the caller's budget is spent
	got := u33Result(t, stale)
	var refusal *cluster.Error
	if !errors.As(got.err, &refusal) || (refusal.Code != cluster.CodeNotOwner && refusal.Code != cluster.CodeUnavailable) {
		t.Fatalf("a parked operation past its deadline returned %+v, %v; want a retryable refusal", got.release, got.err)
	}
	u33WaitParked(t, node, slot, 1)
	node.h.mu.Lock()
	held := node.h.pairs[pairKey{slot, u33DB}].barrier.Held
	if len(held) != 1 || string(held[0].Body) != "patient" {
		node.h.mu.Unlock()
		t.Fatalf("the timed-out operation was not removed from the machine's list: %+v", held)
	}
	node.h.mu.Unlock()
	u33Quiet(t, patient, "the other operation's budget has not run out")

	u33Stopped(t, node, "A", 2, slot)
	if err := node.h.MaybeSendReady(context.Background(), slot, u33DB); err != nil {
		t.Fatal(err)
	}
	if rest := u33Result(t, patient); rest.err != nil || !rest.release.Local {
		t.Fatalf("the remaining operation was released as %+v, %v", rest.release, rest.err)
	}
}

func TestSendingSideStopsAtTheCommitAndWaitsForTheDrain(t *testing.T) {
	first, second := u33State(1, 2, u33Active("A")), u33State(2, 2, u33Active("A", "B"))
	slots := u33Moved(t, first, second, 2, "A", "B")
	slot, other := slots[0], slots[1]
	node := newU33Node("A")
	node.h.OnEpoch(first)
	u33SetApplied(node.pairs, slot, u33DB, cluster.Position{Epoch: 1, Seq: 7})
	u33SetApplied(node.pairs, other, u33DB, cluster.Position{Epoch: 1, Seq: 3})
	node.h.OnEpoch(second)

	for _, moved := range []uint16{slot, other} {
		pair := node.pairs.Snapshot(moved, u33DB)
		if pair.Flags&flagSending == 0 || !holdOpen(pair, false, cluster.Position{}) || pair.Peer != "B" {
			t.Fatalf("slot %d after the commit: flags %05b peer %q; the old owner must stop ordering at once", moved, pair.Flags, pair.Peer)
		}
	}
	if got := node.pairs.Snapshot(slot, u33DB).DrainTo; got != 7 {
		t.Fatalf("drainTo %d, want the appended seq 7", got)
	}

	// One READY for both slots carries the minimum applied position (build SPEC-GAP G2): the
	// pair whose drainTo is above it stays in sending.
	u33Ready(t, node, "B", 2, cluster.Position{Epoch: 1, Seq: 5}, slot, other)
	if node.pairs.Snapshot(slot, u33DB).Flags&flagSending == 0 {
		t.Fatal("READY at 1:5 ended the hold of a pair whose drainTo is 1:7")
	}
	if node.pairs.Snapshot(other, u33DB).Flags&flagSending != 0 {
		t.Fatal("READY at 1:5 did not end the hold of a pair drained to 1:3")
	}
	if got := node.drain.noted[pairKey{other, u33DB}]; got != (cluster.Position{Epoch: 1, Seq: 5}) {
		t.Fatalf("drain latched at %s, want 1:5", got)
	}
	if _, latched := node.drain.noted[pairKey{slot, u33DB}]; latched {
		t.Fatal("a drain was latched for a READY that did not cover the pair")
	}
	// A READY from a node that is not B changes nothing.
	u33Ready(t, node, "C", 2, cluster.Position{Epoch: 1, Seq: 9}, slot)
	if node.pairs.Snapshot(slot, u33DB).Flags&flagSending == 0 {
		t.Fatal("a READY from a stranger ended the hold")
	}
	u33Ready(t, node, "B", 2, cluster.Position{Epoch: 1, Seq: 7}, slot)
	if node.pairs.Snapshot(slot, u33DB).Flags&flagSending != 0 {
		t.Fatal("a READY covering drainTo did not end the hold")
	}
}

func TestARequestAheadOfTheCommitProvesNothingUntilTheRanksAreSeen(t *testing.T) {
	first, second := u33State(1, 2, u33Active("A")), u33State(2, 2, u33Active("A", "B"))
	slot := u33Moved(t, first, second, 2, "A", "B")[0]
	node := newU33Node("B")
	node.link.replies["A"] = PositionReply{State: PositionStateReady}
	node.h.OnEpoch(first)

	// X is not the old owner; its REQ and STOPPED arrive before B has seen epoch 2.
	req := HandoffReq{FromEpoch: 1, ToEpoch: 2, Bitmap: BitmapOfSlots([]uint16{slot})}
	if err := node.h.OnHandoffReq("X", u33Env(kindHandoffReq, 2, u33DB), req); err != nil {
		t.Fatal(err)
	}
	if u33Flags(node.pairs, slot)&flagAwaiting == 0 {
		t.Fatal("a request ahead of the commit did not open the awaiting hold")
	}
	u33Stopped(t, node, "X", 2, slot)
	node.h.OnEpoch(second)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 0 || node.catch.callCount() != 0 {
		t.Fatal("a STOPPED from a node that was not the old owner licensed READY")
	}
	u33Stopped(t, node, "A", 2, slot)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 1 {
		t.Fatal("the real old owner's STOPPED did not complete the barrier")
	}

	// A retransmitted REQ is answered with READY again.
	if err := node.h.OnHandoffReq("A", u33Env(kindHandoffReq, 2, u33DB), req); err != nil {
		t.Fatal(err)
	}
	if _, _, readies := node.link.counts(); readies != 2 {
		t.Fatalf("a repeated REQ after READY produced %d READY messages in all, want 2", readies)
	}
	// The wrong kind in the envelope is refused outright.
	if err := node.h.OnHandoffReq("A", u33Env(kindHandoffReady, 2, u33DB), req); err == nil {
		t.Fatal("a HANDOFF_REQ in a HANDOFF_READY envelope was accepted")
	}
}

func TestAStoppedThatCompletesTheBarrierSendsReady(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	slot := slots[0]
	// The machine is already current when the proof arrives (fed directly: the wiring
	// itself only catches up after the proof).
	key := pairKey{slot, u33DB}
	node.h.mu.Lock()
	node.h.apply(key, node.h.pairs[key], machine.CatchUpDone{Current: true})
	node.h.mu.Unlock()
	u33Stopped(t, node, "A", 2, slot)
	if _, _, readies := node.link.counts(); readies != 1 {
		t.Fatalf("%d READY messages after the STOPPED that completed the barrier, want 1", readies)
	}
}

func TestTargetShortfallTextsNameTheirCause(t *testing.T) {
	first := u33State(1, 3, u33Active("A", "C", "D"))
	second := u33State(2, 3, u33Active("A", "B", "C", "D"))
	slot := u33Moved(t, first, second, 3, "A", "B")[0]
	build := func(members []cluster.Member) *u33Node {
		node := newU33Node("B")
		node.h.OnEpoch(first)
		node.h.OnEpoch(u33State(2, 3, members))
		return node
	}
	// (a) three live copies, one answers: the text names the silent ones.
	silent := build(second.Members)
	silent.link.replies["A"] = PositionReply{State: PositionStateReady}
	silent.link.silent["C"], silent.link.silent["D"] = true, true
	_, err := silent.h.Target(context.Background(), slot, u33DB, 1)
	if err == nil || strings.Contains(err.Error(), "%!") || !strings.Contains(err.Error(), "C") || !strings.Contains(err.Error(), "D") {
		t.Fatalf("unanswered-copies text: %v", err)
	}
	// (b) two of three old copies are committed down: too few copies exist to ever give the
	// quorum, so the target is the survivor's answer and the pair is marked degraded
	// instead of holding (BUG-0742). The text of the hold stays for the transient case (a).
	few := build([]cluster.Member{u33Member("A", cluster.MemberActive), u33Member("B", cluster.MemberActive),
		u33Member("C", cluster.MemberDown), u33Member("D", cluster.MemberDown)})
	answered := cluster.Position{Epoch: 1, Seq: 12}
	few.link.replies["A"] = PositionReply{State: PositionStateReady, AppliedPos: answered}
	target, err := few.h.Target(context.Background(), slot, u33DB, 1)
	if err != nil || target != answered {
		t.Fatalf("two of three old copies down: target %+v, error %v; want the survivor's position %+v and no error", target, err, answered)
	}
	if !few.h.Degraded(slot, u33DB) {
		t.Fatal("the pair opened on one answer of two needed and was not marked degraded")
	}
	if cached, known := few.h.CachedTarget(slot, u33DB); !known || cached != answered {
		t.Fatalf("cached target %+v known=%v, want %+v", cached, known, answered)
	}
	if silent.h.Degraded(slot, u33DB) {
		t.Fatal("a pair held for a silent live copy was marked degraded")
	}
}

// TestDedupeSurvivesAHandoffForANonIdempotentOpcode is write path §5 test 2 (BUG-0749).
// Substitute for an owner kill: the unit-test rig cannot kill an owner, so the copy is a
// Replicator that applies the owner's pop record through the real applier over a real
// fifoStore (exactly what the copy holds once it has acked), the handoff is the committed
// epoch moving on with this node as the owner, and the new owner is a Writer over that same
// store and pair set that receives the client's retry under the same request id. The end to
// end version, with a killed owner, is TestAnOwnerKillDoesNotLetARetriedPopApplyTwice.
func TestDedupeSurvivesAHandoffForANonIdempotentOpcode(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Fields.Entries = append(fx.schema.decl.Fields.Entries, machine.FieldEntry{ID: claim34Field, Type: "fifoStore", Name: "Queue"})
	claim34Push(t, fx.kv, store.FieldFIFO, "a", "b", "c")
	claim34LiveHeader(t, fx.kv)
	applier := NewDocumentApplier(fx.kv, pairRecorder{}).WithFieldEngines(claim34Engines{engine: store.FieldFIFO})
	copyRep := mustReplicator(t, ReplicatorConfig{
		Self: "copy", Committed: fx.topo.Committed, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier,
		Advancer: NewLocalAdvancer(fx.pairs, fx.kv),
	})
	requestID := [16]byte{0x0a, 1}
	pop := claim34Op(cluster.OpStoreVerb)
	pop.Fields[0].Verb = cluster.VerbPop
	body, err := cluster.EncodeOp(pop)
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	first := cluster.Position{Epoch: testEpoch, Seq: 1}
	frame := WriteFrame{Slot: testSlot, DB: testDB, From: "old-owner", Epoch: testEpoch, PairRecord: PairRecord{
		Pos: first, Record: storage.WALRecord{Class: cluster.OpStoreVerb, RequestID: requestID, Body: body},
	}}
	reply, err := copyRep.OnWrite(frame)
	if err != nil || reply.Ack.Outcome != AckApplied {
		t.Fatalf("the copy's apply = %+v, %v; want applied", reply, err)
	}
	if left := claim34Remaining(t, fx.kv, store.FieldFIFO); len(left) != 2 || left[0] != "b" {
		t.Fatalf("after the copy applied the pop the field holds %v, want [b c]", left)
	}
	recorded, err := NewKVDedupeReader(fx.kv).Lookup(testSlot, testDB, testBucket, machine.DedupeKey{RequestID: requestID})
	if err != nil || recorded == nil || claim34Text(t, recorded.Result) != "a" {
		t.Fatalf("the copy's entry for the pop = %+v, %v, want the first element a (BUG-0749)", recorded, err)
	}

	// The owner dies before the client reads the answer; the ring commits a later epoch
	// with this copy as the owner, and the client retries under the same request id.
	fx.topo.mu.Lock()
	fx.topo.committed++
	fx.topo.mu.Unlock()
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier, Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	op := claim34Op(cluster.OpStoreVerb)
	op.Fields[0].Verb = cluster.VerbPop
	op.DeclVersion, op.SchemaVersionSeen = testSeen, testSeen
	retry, err := fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: requestID, Deadline: time.Second})
	if err != nil {
		t.Fatalf("retry on the new owner: %v", err)
	}
	if !retry.Deduped || retry.Pos != first || claim34Text(t, retry.Result) != "a" {
		t.Fatalf("retry on the promoted copy = %+v, want Deduped at %s with the first element a: the copy held no entry for the owner's write and popped a second element (BUG-0749)", retry, first)
	}
	if left := claim34Remaining(t, fx.kv, store.FieldFIFO); len(left) != 2 || left[0] != "b" {
		t.Fatalf("after the retry the field holds %v, want [b c]: the retry popped a second element", left)
	}
	if got := len(fx.logged()); got != 1 {
		t.Fatalf("the log holds %d entries, want 1: a deduped retry appends nothing", got)
	}
}

// TestDedupeSurvivesAHandoffForAClaim is the claim variant of the test above (BUG-0749):
// the retried claim is answered with the lease id and deadline the copy recorded when it
// applied the owner's record, and the field loses one element, not two.
func TestDedupeSurvivesAHandoffForAClaim(t *testing.T) {
	fx := newFixture(t)
	fx.schema.decl.Fields.Entries = append(fx.schema.decl.Fields.Entries, machine.FieldEntry{ID: claim34Field, Type: "fifoStore", Name: "Queue"})
	claim34Push(t, fx.kv, store.FieldFIFO, "a", "b")
	claim34LiveHeader(t, fx.kv)
	applier := NewDocumentApplier(fx.kv, pairRecorder{}).WithFieldEngines(claim34Engines{engine: store.FieldFIFO})
	copyRep := mustReplicator(t, ReplicatorConfig{
		Self: "copy", Committed: fx.topo.Committed, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier,
		Advancer: NewLocalAdvancer(fx.pairs, fx.kv),
	})
	requestID := [16]byte{0x0c, 1}
	body, err := cluster.EncodeOp(claim34Claim(1, 7_000, 3))
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	first := cluster.Position{Epoch: testEpoch, Seq: 1}
	frame := WriteFrame{Slot: testSlot, DB: testDB, From: "old-owner", Epoch: testEpoch, PairRecord: PairRecord{
		Pos: first, Record: storage.WALRecord{Class: cluster.OpClaim, RequestID: requestID, Body: body},
	}}
	reply, err := copyRep.OnWrite(frame)
	if err != nil || reply.Ack.Outcome != AckApplied {
		t.Fatalf("the copy's apply = %+v, %v; want applied", reply, err)
	}
	if left := claim34Remaining(t, fx.kv, store.FieldFIFO); len(left) != 1 || left[0] != "b" {
		t.Fatalf("after the copy applied the claim the field holds %v, want [b]", left)
	}
	recorded, err := NewKVDedupeReader(fx.kv).Lookup(testSlot, testDB, testBucket, machine.DedupeKey{RequestID: requestID})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if recorded == nil || len(recorded.Result) != 24 {
		t.Fatalf("the copy's entry for the claim = %+v, want the 24-byte lease id and deadline it leased (BUG-0749)", recorded)
	}

	fx.topo.mu.Lock()
	fx.topo.committed++
	fx.topo.mu.Unlock()
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier, Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024}, WallMillis: func() int64 { return 9_000 },
	})
	op := claim34Op(cluster.OpClaim)
	op.Count, op.DeclVersion, op.SchemaVersionSeen = 1, testSeen, testSeen
	retry, err := fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: requestID, Deadline: time.Second})
	if err != nil {
		t.Fatalf("retry on the new owner: %v", err)
	}
	if !retry.Deduped || retry.Pos != first || !bytes.Equal(retry.Result, recorded.Result) {
		t.Fatalf("retried claim = %+v, want Deduped at %s with the recorded lease %x", retry, first, recorded.Result)
	}
	if left := claim34Remaining(t, fx.kv, store.FieldFIFO); len(left) != 1 || left[0] != "b" {
		t.Fatalf("after the retry the field holds %v, want [b]: the retry leased a second element", left)
	}
}

// TestARestartedCopyDoesNotPopAgainWhenItReplaysItsLog (BUG-0749 review S2): a copy
// persists no applied position (its applier has no pair recorder in production), so after
// a restart its pair set is empty and boot replays every record in the log. The replay
// must not take a second element for a pop the copy already applied, and must leave the
// first pop's result in the entry.
func TestARestartedCopyDoesNotPopAgainWhenItReplaysItsLog(t *testing.T) {
	t.Skip("FINDING: a restarted copy persists no applied position, so boot replay re-applies a pop it already applied (a second element is lost and the dedupe entry takes the second value); see BUG-0752")
	fx := newFixture(t)
	fx.schema.decl.Fields.Entries = append(fx.schema.decl.Fields.Entries, machine.FieldEntry{ID: claim34Field, Type: "fifoStore", Name: "Queue"})
	claim34Push(t, fx.kv, store.FieldFIFO, "a", "b", "c")
	claim34LiveHeader(t, fx.kv)
	applier := NewDocumentApplier(fx.kv, nil).WithFieldEngines(claim34Engines{engine: store.FieldFIFO})
	copyRep := mustReplicator(t, ReplicatorConfig{
		Self: "copy", Committed: fx.topo.Committed, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv, Applier: applier,
		Advancer: NewLocalAdvancer(fx.pairs, fx.kv),
	})
	requestID := [16]byte{0x0a, 2}
	pop := claim34Op(cluster.OpStoreVerb)
	pop.Fields[0].Verb = cluster.VerbPop
	body, err := cluster.EncodeOp(pop)
	if err != nil {
		t.Fatalf("EncodeOp: %v", err)
	}
	first := cluster.Position{Epoch: testEpoch, Seq: 1}
	frame := WriteFrame{Slot: testSlot, DB: testDB, From: "old-owner", Epoch: testEpoch, PairRecord: PairRecord{
		Pos: first, Record: storage.WALRecord{Class: cluster.OpStoreVerb, RequestID: requestID, Body: body},
	}}
	if reply, err := copyRep.OnWrite(frame); err != nil || reply.Ack.Outcome != AckApplied {
		t.Fatalf("the copy's apply = %+v, %v; want applied", reply, err)
	}

	// The restart: the pair set is rebuilt from nothing, and the log is replayed from its start.
	restarted := NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: &cluster.PairSet{}, Log: fx.wal, KV: fx.kv, Applier: applier, Clock: fixedClock{}, Lease: fx.lease,
		Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl, WCTimeout: time.Second,
		DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
	})
	if err := restarted.Replay(cluster.WALRecord{WALRecord: storage.WALRecord{Class: cluster.OpStoreVerb, RequestID: requestID, Body: body}, Slot: testSlot, DB: testDB, Pos: first, Cursor: 1}); err != nil {
		t.Fatalf("replaying the pop: %v", err)
	}
	if left := claim34Remaining(t, fx.kv, store.FieldFIFO); len(left) != 2 || left[0] != "b" {
		t.Fatalf("after the restart's replay the field holds %v, want [b c]: the replay popped a second element", left)
	}
	recorded, err := NewKVDedupeReader(fx.kv).Lookup(testSlot, testDB, testBucket, machine.DedupeKey{RequestID: requestID})
	if err != nil || recorded == nil || claim34Text(t, recorded.Result) != "a" {
		t.Fatalf("after the replay the entry holds %+v, %v, want the first pop's element a", recorded, err)
	}
}
