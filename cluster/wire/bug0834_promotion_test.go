package wire

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// BUG-0834, the promotion rule. A member that left a pair's copy set keeps its committed
// SEEDED bit until its RETIRE commits, and until then it may hold acknowledged writes that no
// member of the copy set has (an activation adds a copy before that copy holds the data). A
// new owner's target, and the source a catch-up reads from, must take that holder's answer.

// bug0834Seeded is the committed SEEDED rows that give each named member the bit for slot.
func bug0834Seeded(t *testing.T, slot uint16, holders ...string) []cluster.SeedBitmap {
	t.Helper()
	rows := make([]cluster.SeedBitmap, 0, len(holders))
	for _, id := range holders {
		bits := make([]byte, barrierSlots/8)
		if err := machine.Set(bits, slot); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, cluster.SeedBitmap{Node: id, DBID: int64(u33DB), Bits: bits})
	}
	return rows
}

// bug0834RankedSlot returns a slot whose ranking at state starts with order.
func bug0834RankedSlot(t *testing.T, state cluster.ClusterState, order ...string) uint16 {
	t.Helper()
	table := cluster.BuildTable(state)
	for slot := range uint16(barrierSlots) {
		ranking := table.Ranking(slot)
		if len(ranking) < len(order) {
			continue
		}
		match := true
		for i, id := range order {
			match = match && ranking[i].ID == id
		}
		if match {
			return slot
		}
	}
	t.Fatalf("no slot ranks %v first", order)
	return 0
}

// bug0834Case is the RF 2 window: at epoch 1 the pair's copies are A (owner) and J, the
// joiner the activation added at 0:0, and R, the copy the activation retired, still holds its
// bit. At epoch 2 A is committed down and J is the new owner. holders names the members whose
// bit is set at both epochs; rState is R's member state at epoch 2.
type bug0834Case struct {
	holders []string
	rState  cluster.MemberState
	extra   []cluster.Member // members added to both states (unranked ones do not move the table)
	// rowOnly names members that have a SEEDED row for the database with another slot's bit
	// set and this slot's clear: a holder of other pairs, not of this one.
	rowOnly []string
}

func bug0834States(t *testing.T, tc bug0834Case) (uint16, cluster.ClusterState, cluster.ClusterState) {
	t.Helper()
	first := u33State(1, 2, append(u33Active("A", "J", "R"), tc.extra...))
	slot := bug0834RankedSlot(t, first, "A", "J", "R")
	second := u33State(2, 2, append([]cluster.Member{u33Member("A", cluster.MemberDown), u33Member("J", cluster.MemberActive), u33Member("R", tc.rState)}, tc.extra...))
	for _, state := range []*cluster.ClusterState{&first, &second} {
		state.SeedBitmaps = append(bug0834Seeded(t, slot, tc.holders...), bug0834Seeded(t, (slot+1)%barrierSlots, tc.rowOnly...)...)
		state.Settings.DeadMs = 600000
	}
	return slot, first, second
}

// bug0834NewOwner is J fed both states: awaiting its first position on the slot at 0:0.
func bug0834NewOwner(t *testing.T, tc bug0834Case) (*u33Node, uint16) {
	t.Helper()
	slot, first, second := bug0834States(t, tc)
	node := newU33Node("J")
	node.h.OnEpoch(first)
	node.clock.set(0)
	node.h.OnEpoch(second)
	return node, slot
}

// TestANewOwnerTakesTheRetiredHoldersPosition: J opens only at or above what R, the copy the
// activation retired, still holds. Before the fix Target asked the old copy set alone ({J}
// once A is down), took J's ready 0:0 and counted it as not degraded: the loss was silent.
// [GUARD: mutation 1, ask only copy-set members; mutation 3, ask everyone ever]
func TestANewOwnerTakesTheRetiredHoldersPosition(t *testing.T) {
	held := cluster.Position{Epoch: 1, Seq: 7}
	allHolders := []string{"A", "J", "R"}
	cases := []struct {
		name     string
		holders  []string
		rowOnly  []string
		reply    *PositionReply
		silent   bool
		wantOpen bool
		want     cluster.Position
		asksR    bool
	}{
		{name: "a retired holder ahead sets the target", holders: allHolders,
			reply: &PositionReply{State: PositionStateRetired, AppliedPos: held}, wantOpen: true, want: held, asksR: true},
		{name: "a ready holder outside the copy set sets the target", holders: allHolders,
			reply: &PositionReply{State: PositionStateReady, AppliedPos: held}, wantOpen: true, want: held, asksR: true},
		{name: "a silent holder holds the pair", holders: allHolders, silent: true, asksR: true},
		{name: "a holder whose delete started is not evidence", holders: allHolders,
			reply: &PositionReply{State: PositionStateRetiring, AppliedPos: held}, wantOpen: true, asksR: true},
		{name: "a holder answering seeding is not evidence", holders: allHolders,
			reply: &PositionReply{State: PositionStateSeeding, AppliedPos: held}, wantOpen: true, asksR: true},
		{name: "a member whose bit is clear is not asked", holders: []string{"A", "J"}, rowOnly: []string{"R"}, silent: true, wantOpen: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, slot := bug0834NewOwner(t, bug0834Case{holders: tc.holders, rowOnly: tc.rowOnly, rState: cluster.MemberActive})
			if tc.reply != nil {
				node.link.replies["R"] = *tc.reply
			}
			node.link.silent["R"] = tc.silent
			target, err := node.h.Target(context.Background(), slot, u33DB, 1)
			asked := slices.Contains(node.link.asked, "R")
			if asked != tc.asksR {
				t.Fatalf("R asked: %v, want %v (asked %v)", asked, tc.asksR, node.link.asked)
			}
			if !tc.wantOpen {
				if err == nil {
					t.Fatalf("an unanswered holder of the pair was skipped: target %+v", target)
				}
				if !strings.Contains(err.Error(), "R") {
					t.Fatalf("the hold does not name the holder it waits for: %v", err)
				}
				if _, known := node.h.CachedTarget(slot, u33DB); known {
					t.Fatal("a target was recorded while a holder of the pair was unaccounted for")
				}
				return
			}
			if err != nil || target != tc.want {
				t.Fatalf("target %+v, error %v; want %+v", target, err, tc.want)
			}
		})
	}
}

// TestAHolderAnsweringAnErrorMakesThePromotionWait: a holder whose pair is deferred behind its
// log replay answers POSITION_QUERY with an error, not a position (the deferred-pair gate,
// build/deferred-pair-gate, spec section 3 rows 1 to 5). That is no evidence that it holds
// less: the promotion waits, and proceeds once it answers.
// [GUARD: mutation 2, accept a retired answer but proceed past an error answer]
func TestAHolderAnsweringAnErrorMakesThePromotionWait(t *testing.T) {
	held := cluster.Position{Epoch: 1, Seq: 7}
	node, slot := bug0834NewOwner(t, bug0834Case{holders: []string{"A", "J", "R"}, rState: cluster.MemberActive})
	gated := &bug0834GatedLink{u33Link: node.link, gated: map[string]bool{"R": true}}
	node.h.cfg.Link = gated
	node.link.replies["R"] = PositionReply{State: PositionStateRetired, AppliedPos: held}
	if target, err := node.h.Target(context.Background(), slot, u33DB, 1); err == nil {
		t.Fatalf("a holder that answered an error was passed over: target %+v", target)
	}
	if _, known := node.h.CachedTarget(slot, u33DB); known {
		t.Fatal("a target was recorded while the holder answered an error")
	}
	gated.open("R")
	target, err := node.h.Target(context.Background(), slot, u33DB, 1)
	if err != nil || target != held {
		t.Fatalf("after the holder answered: target %+v, error %v; want %+v", target, err, held)
	}
}

// bug0834GatedLink answers a position query from a gated peer with the error a deferred pair
// answers peers with.
type bug0834GatedLink struct {
	*u33Link
	gated map[string]bool
}

func (l *bug0834GatedLink) open(peer string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.gated, peer)
}

func (l *bug0834GatedLink) QueryPosition(ctx context.Context, peer string, query PositionQuery) (PositionReply, error) {
	l.mu.Lock()
	gated := l.gated[peer]
	l.mu.Unlock()
	if gated {
		return PositionReply{}, errors.New("slot is deferred: applying it waits for its log replay")
	}
	return l.u33Link.QueryPosition(ctx, peer, query)
}

// TestANewOwnerWaitsForADownHolderUntilItIsDead: R holds its bit and is committed down, so
// what only it holds may come back with it. The pair holds, saying who it waits for and what
// to do, until R is declared dead (the committed dead_ms after it went down); it then opens on what
// the live members hold and is marked degraded. Before the fix it opened at once, on 0:0, not
// degraded.
func TestANewOwnerWaitsForADownHolderUntilItIsDead(t *testing.T) {
	node, slot := bug0834NewOwner(t, bug0834Case{holders: []string{"A", "J", "R"}, rState: cluster.MemberDown})
	_, err := node.h.Target(context.Background(), slot, u33DB, 1)
	if err == nil {
		t.Fatal("the pair opened while a down holder may hold acknowledged writes no live member has")
	}
	for _, words := range []string{"R", "down", "dead", "'nodes'"} {
		if !strings.Contains(err.Error(), words) {
			t.Fatalf("the hold %q does not say %q", err, words)
		}
	}
	if slices.Contains(node.link.asked, "R") {
		t.Fatal("a member committed down was asked for its position")
	}
	if _, known := node.h.CachedTarget(slot, u33DB); known {
		t.Fatal("a target was recorded while a down holder was waited for")
	}

	third := u33State(3, 2, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("J", cluster.MemberActive), u33Member("R", cluster.MemberDead)})
	third.SeedBitmaps = bug0834Seeded(t, slot, "A", "J", "R")
	third.Settings.DeadMs = 600000
	node.h.OnEpoch(third)
	target, err := node.h.Target(context.Background(), slot, u33DB, 2)
	if err != nil || !target.Zero() {
		t.Fatalf("once R is dead: target %+v, error %v; want J's own 0:0", target, err)
	}
	if !node.h.Degraded(slot, u33DB) {
		t.Fatal("the pair opened without a holder that was declared dead and was not marked degraded")
	}
}

// TestAJoiningHolderIsNotWaitedFor: a joiner's pulled pairs carry its bit, but a joiner never
// counted for a write concern, so it holds no acknowledged write that only it has. A silent
// joiner must not hold every promotion of the pairs it pulled.
func TestAJoiningHolderIsNotWaitedFor(t *testing.T) {
	node, slot := bug0834NewOwner(t, bug0834Case{holders: []string{"A", "J", "Q"}, rState: cluster.MemberActive,
		extra: []cluster.Member{u33Member("Q", cluster.MemberJoining)}})
	node.link.silent["Q"] = true
	target, err := node.h.Target(context.Background(), slot, u33DB, 1)
	if err != nil || !target.Zero() {
		t.Fatalf("target %+v, error %v; a joining holder held the promotion", target, err)
	}
}

// TestTheNewOwnerDoesNotOpenBelowTheRetiredHolder: driven by Tick, as the supervisor does. The
// catch-up here does not move the applied position, so a pair whose target is the holder's
// 1:7 must stay awaiting; before the fix it opened at 0:0 and served "is not in".
func TestTheNewOwnerDoesNotOpenBelowTheRetiredHolder(t *testing.T) {
	node, slot := bug0834NewOwner(t, bug0834Case{holders: []string{"A", "J", "R"}, rState: cluster.MemberActive})
	node.link.replies["R"] = PositionReply{State: PositionStateRetired, AppliedPos: cluster.Position{Epoch: 1, Seq: 7}}
	node.clock.set(u33Bound)
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if flags := u33Flags(node.pairs, slot); flags&flagAwaiting == 0 {
		t.Fatalf("the new owner opened at 0:0 below the retired holder's 1:7: flags %05b", flags)
	}
	if target, known := node.h.CachedTarget(slot, u33DB); !known || target != (cluster.Position{Epoch: 1, Seq: 7}) {
		t.Fatalf("cached target %+v known=%v, want the holder's 1:7", target, known)
	}
}

// TestAnRF3PromotionTakesTheRetiredHolder: copies A, B, J at RF 3, where J is a joiner at 0:0
// and R the copy the activation retired. A write acknowledged by A and R (the majority of the
// copies before the activation) is on neither B nor J. With A down, B and J make the quorum
// of two, so the old rule opened on B's position; the target must be R's.
func TestAnRF3PromotionTakesTheRetiredHolder(t *testing.T) {
	first := u33State(1, 3, u33Active("A", "B", "J", "R"))
	slot := bug0834RankedSlot(t, first, "A", "B", "J", "R")
	second := u33State(2, 3, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberActive), u33Member("J", cluster.MemberActive), u33Member("R", cluster.MemberActive)})
	for _, state := range []*cluster.ClusterState{&first, &second} {
		state.SeedBitmaps = bug0834Seeded(t, slot, "A", "B", "J", "R")
		state.Settings.DeadMs = 600000
	}
	own, held := cluster.Position{Epoch: 1, Seq: 5}, cluster.Position{Epoch: 1, Seq: 9}
	for _, silent := range []bool{false, true} {
		node := newU33Node("B")
		u33SetApplied(node.pairs, slot, u33DB, own)
		node.h.OnEpoch(first)
		node.clock.set(0)
		node.h.OnEpoch(second)
		node.link.replies["J"] = PositionReply{State: PositionStateReady}
		node.link.replies["R"] = PositionReply{State: PositionStateRetired, AppliedPos: held}
		node.link.silent["R"] = silent
		target, err := node.h.Target(context.Background(), slot, u33DB, 1)
		if silent {
			if err == nil {
				t.Fatalf("with R silent the quorum of B and J opened the pair at %+v", target)
			}
			continue
		}
		if err != nil || target != held {
			t.Fatalf("target %+v, error %v; want R's %+v", target, err, held)
		}
		if node.h.Degraded(slot, u33DB) {
			t.Fatal("a quorum of copies answered, yet the target was marked degraded")
		}
	}
}

// TestACatchUpReadsFromARetiredHolder: the new owner's catch-up must be able to read from the
// holder that set its target. A retiring holder (its delete has started) is not a source.
func TestACatchUpReadsFromARetiredHolder(t *testing.T) {
	held := cluster.Position{Epoch: 1, Seq: 7}
	cases := []struct {
		state uint8
		want  string
	}{
		{PositionStateRetired, "R"},
		{PositionStateReady, "R"},
		{PositionStateRetiring, ""},
		{PositionStateSeeding, ""},
	}
	for _, tc := range cases {
		source, found := chooseCatchUpSource(cluster.Position{}, []catchUpOffer{{ID: "R", Reply: PositionReply{State: tc.state, AppliedPos: held}}})
		if source != tc.want || found != (tc.want != "") {
			t.Fatalf("state %d: source %q found %v, want %q", tc.state, source, found, tc.want)
		}
	}
	// On a tie the copy listed first, the owner, stays the source.
	source, _ := chooseCatchUpSource(cluster.Position{}, []catchUpOffer{
		{ID: "O", Reply: PositionReply{State: PositionStateReady, AppliedPos: held}},
		{ID: "R", Reply: PositionReply{State: PositionStateRetired, AppliedPos: held}},
	})
	if source != "O" {
		t.Fatalf("source %q on a tie, want the owner listed first", source)
	}
}

// TestHoldersAreTheLiveMembersWithTheBit: the catch-up source asks Handoff.Holders besides the
// copy set. It lists the members other than this node whose bit is set and that can answer:
// not a joiner, not a member committed down or dead, not one whose bit is clear.
func TestHoldersAreTheLiveMembersWithTheBit(t *testing.T) {
	members := []cluster.Member{u33Member("A", cluster.MemberActive), u33Member("J", cluster.MemberActive),
		u33Member("R", cluster.MemberActive), u33Member("L", cluster.MemberLeaving), u33Member("D", cluster.MemberDown),
		u33Member("X", cluster.MemberDead), u33Member("Q", cluster.MemberJoining), u33Member("C", cluster.MemberActive)}
	state := u33State(1, 2, members)
	slot := uint16(77)
	state.SeedBitmaps = bug0834Seeded(t, slot, "A", "J", "R", "L", "D", "X", "Q")
	node := newU33Node("J")
	node.h.OnEpoch(state)
	got := node.h.Holders(slot, u33DB)
	slices.Sort(got)
	if want := []string{"A", "L", "R"}; !slices.Equal(got, want) {
		t.Fatalf("holders %v, want %v", got, want)
	}
	// A RETIRE clears the bit at a state_seq that does not move the epoch: it must be seen.
	retired := state
	retired.StateSeq++
	retired.SeedBitmaps = bug0834Seeded(t, slot, "A", "J", "L", "D", "X", "Q")
	node.h.OnEpoch(retired)
	if got := node.h.Holders(slot, u33DB); slices.Contains(got, "R") {
		t.Fatalf("holders %v after R's bit was cleared at the same epoch", got)
	}
}

// bug0834CatchUpNode is J as a Node at state: its topology, its Handoff fed state, and its pairs,
// enough for Node.catchUpSourceVia. The u33 link answers the position queries.
func bug0834CatchUpNode(state cluster.ClusterState) (*Node, *u33Link) {
	fake := newU33Node("J")
	fake.h.OnEpoch(state)
	node := &Node{cfg: NodeConfig{NodeID: "J"}, topology: NewClusterTopology("J", c5Source{state: state}),
		handoff: fake.h, pairs: fake.pairs, cl: &clusterParts{ctx: context.Background()}}
	return node, fake.link
}

// TestACatchUpAsksTheCopiesThenTheHolders drives Node.catchUpSource's choice: the copy set is
// asked first and wins a tie (the owner stays the source), a holder outside the copy set that
// is strictly ahead is the source, and once the owner is down the holder is the only source.
// Before BUG-0834's fix the holder was never asked: with the owner down the catch-up found no
// source, and the new owner had no way to reach the target its holder set.
// [GUARD: mutation 1b, catchUpSource asks the copy set only]
func TestACatchUpAsksTheCopiesThenTheHolders(t *testing.T) {
	low, held := cluster.Position{Epoch: 1, Seq: 5}, cluster.Position{Epoch: 1, Seq: 7}
	slot, live, ownerDown := bug0834States(t, bug0834Case{holders: []string{"A", "J", "R"}, rState: cluster.MemberActive})
	clearSlot, _, bitClear := bug0834States(t, bug0834Case{holders: []string{"A", "J"}, rowOnly: []string{"R"}, rState: cluster.MemberActive})
	if clearSlot != slot {
		t.Fatalf("the two constructions rank different slots first: %d and %d", slot, clearSlot)
	}
	cases := []struct {
		name      string
		state     cluster.ClusterState
		replies   map[string]PositionReply
		want      string
		wantAsked []string
	}{
		{name: "on a tie the owner stays the source", state: live,
			replies: map[string]PositionReply{"A": {State: PositionStateReady, AppliedPos: held}, "R": {State: PositionStateRetired, AppliedPos: held}},
			want:    "A", wantAsked: []string{"A", "R"}},
		{name: "a holder strictly ahead of the copies is the source", state: live,
			replies: map[string]PositionReply{"A": {State: PositionStateReady, AppliedPos: low}, "R": {State: PositionStateRetired, AppliedPos: held}},
			want:    "R", wantAsked: []string{"A", "R"}},
		{name: "with the owner down the holder is the only source", state: ownerDown,
			replies: map[string]PositionReply{"R": {State: PositionStateRetired, AppliedPos: held}},
			want:    "R", wantAsked: []string{"R"}},
		{name: "a holder whose delete started is not a source", state: ownerDown,
			replies:   map[string]PositionReply{"R": {State: PositionStateRetiring, AppliedPos: held}},
			wantAsked: []string{"R"}},
		{name: "a member whose bit is clear is not asked", state: bitClear,
			replies:   map[string]PositionReply{"R": {State: PositionStateRetired, AppliedPos: held}},
			wantAsked: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, link := bug0834CatchUpNode(tc.state)
			maps.Copy(link.replies, tc.replies)
			source, err := node.catchUpSourceVia(link, slot, u33DB)
			if !slices.Equal(link.asked, tc.wantAsked) {
				t.Fatalf("asked %v, want %v in that order", link.asked, tc.wantAsked)
			}
			if tc.want == "" {
				if !errors.Is(err, ErrNoCatchUpSource) {
					t.Fatalf("source %q, error %v; want ErrNoCatchUpSource", source, err)
				}
				return
			}
			if err != nil || source != tc.want {
				t.Fatalf("source %q, error %v; want %q", source, err, tc.want)
			}
		})
	}
}
