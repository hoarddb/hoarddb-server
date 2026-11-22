package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// noiseAppend puts count records of other pairs in the node's log, so a read that starts at the
// log's first record walks them and one that starts at the pair's tail does not.
func noiseAppend(n *u32Node, count int) {
	for slot := uint16(1000); slot < 1000+uint16(count); slot++ {
		n.ownAppend(slot, u32Pos(u32Epoch, 1), cluster.Position{})
	}
}

func countingNode(t *testing.T, id string) (*u32Node, *scanCountingLog) {
	t.Helper()
	var counting *scanCountingLog
	node := newU32Node(t, id, func(cfg *ReplicatorConfig) {
		counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = counting
	})
	return node, counting
}

// TestShipTailSendsOnlyThePairsTailRecord (BUG-0746): an owner whose stream state is gone queues
// the pair's tail once, from the cursor the pair holds, so one frame is sent and the log's other
// entries are not read. [GUARD: ShipTail without the cursor reads from the log's first record]
func TestShipTailSendsOnlyThePairsTailRecord(t *testing.T) {
	owner, counting := countingNode(t, "owner")
	owner.copies = []string{"copy"}
	noiseAppend(owner, 300)
	owner.ownRun(u32Epoch, 1, 3)
	owner.rep.ShipTail(u32Slot, u32DB)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	frames := owner.link.sent("copy")
	if len(frames) != 1 || frames[0].Pos != u32Pos(u32Epoch, 3) {
		t.Fatalf("%d frames sent, first %v; want exactly the tail record %s", len(frames), frames, u32Pos(u32Epoch, 3))
	}
	if read := counting.entries.Load(); read > 5 {
		t.Fatalf("shipping one tail read %d log entries, want about 1 (a read from the log's first record walks 300 more)", read)
	}
}

// TestAPromotedCopyShipsItsTailFromTheCursorItAppliedAt (BUG-0746): a copy holds no pair record
// of its own, so the cursor of its applied tail must be remembered when it applies the frame, or
// the node promoted by a failover has nothing to ship from. [GUARD: do not set TailCursor in
// applyDurably]
func TestAPromotedCopyShipsItsTailFromTheCursorItAppliedAt(t *testing.T) {
	promoted, counting := countingNode(t, "promoted")
	promoted.copies = []string{"other"}
	noiseAppend(promoted, 300)
	for seq := uint64(1); seq <= 3; seq++ {
		if reply := promoted.write(u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{})); reply.Ack.Outcome != AckApplied {
			t.Fatalf("the copy refused %d: %+v", seq, reply)
		}
	}
	if cursor := promoted.pairs.Snapshot(u32Slot, u32DB).TailCursor; cursor == 0 {
		t.Fatal("the copy applied three records and remembers no cursor for its tail")
	}
	counting.entries.Store(0)
	promoted.rep.ShipTail(u32Slot, u32DB)
	if err := promoted.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	frames := promoted.link.sent("other")
	if len(frames) != 1 || frames[0].Pos != u32Pos(u32Epoch, 3) {
		t.Fatalf("the promoted copy sent %d frames, want the tail record %s", len(frames), u32Pos(u32Epoch, 3))
	}
	if read := counting.entries.Load(); read > 5 {
		t.Fatalf("shipping the tail read %d log entries, want about 1", read)
	}
}

// TestARefusedTailIsShippedAgainFromTheTailNotTheLogsStart (BUG-0746): a copy that is behind
// refuses the tail as a gap; the stream rewinds, and with no ack yet it must resume at the
// record it started from, or the restarted owner re-reads the pair's whole history.
// [GUARD: rewindLocked without the anchor goes to FloorCursor]
func TestARefusedTailIsShippedAgainFromTheTailNotTheLogsStart(t *testing.T) {
	owner, counting := countingNode(t, "owner")
	owner.copies = []string{"copy"}
	noiseAppend(owner, 300)
	owner.ownRun(u32Epoch, 1, 3)
	owner.rep.ShipTail(u32Slot, u32DB)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	counting.entries.Store(0)
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 1), Outcome: AckRefused})
	// Its catch-up applied 1 and 2, which acknowledges nothing in flight.
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 2), Outcome: AckApplied})
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(owner.link.sent("copy")); got != 2 {
		t.Fatalf("%d frames sent after the rewind, want the tail again (2 in all)", got)
	}
	if read := counting.entries.Load(); read > 5 {
		t.Fatalf("the rewind read %d log entries, want about 1: it restarted at the log's first record", read)
	}
	if owner.rep.Acked(u32Slot, u32DB, u32Pos(u32Epoch, 3)) != 0 {
		t.Fatal("the restart anchor made an unacknowledged copy count as having the tail")
	}
}

// TestShipTailOfAnEmptyPairSendsNothing: a pair this node holds but has applied nothing in.
func TestShipTailOfAnEmptyPairSendsNothing(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.rep.ShipTail(u32Slot, u32DB)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := owner.link.total(); got != 0 {
		t.Fatalf("an empty pair shipped %d frames", got)
	}
}

// TestAPairOpenedByAMoveCallsOpenedOnceWhenItsHoldEnds (BUG-0746): the new owner learns its
// pair is open from the barrier's READY, not while the hold is open, and only once. It proves
// the callback; that the callback ships is TestAPromotedCopyShipsItsTailFromTheCursorItAppliedAt.
func TestAPairOpenedByAMoveCallsOpenedOnceWhenItsHoldEnds(t *testing.T) {
	first, second := u33State(1, 2, u33Active("A")), u33State(2, 2, u33Active("A", "B"))
	slots := u33Moved(t, first, second, 2, "A", "B")
	node := newU33Node("B")
	var opened []uint16
	node.h.cfg.Opened = func(slot uint16, _ uint64) { opened = append(opened, slot) }
	node.link.replies["A"] = PositionReply{State: PositionStateReady}
	node.h.OnEpoch(first)
	node.h.OnEpoch(second)
	slot := slots[0]
	key := pairKey{slot, u33DB}
	node.h.mu.Lock()
	node.h.apply(key, node.h.pairs[key], machine.CatchUpDone{Current: true})
	node.h.mu.Unlock()
	if len(opened) != 0 {
		t.Fatalf("Opened fired for %v while the hold was open", opened)
	}
	u33Stopped(t, node, "A", 2, slot)
	node.h.mu.Lock()
	node.h.apply(key, node.h.pairs[key], machine.CatchUpDone{Current: true})
	node.h.mu.Unlock()
	if len(opened) != 1 || opened[0] != slot {
		t.Fatalf("Opened fired for %v, want exactly slot %d once the hold ended", opened, slot)
	}
}

// TestARestartHoldDoesNotCallOpened: the restarted node ships its own tails; the lazy hold that
// ends on a pair's first touch is not a move. [GUARD: drop the fromEpoch test]
func TestARestartHoldDoesNotCallOpened(t *testing.T) {
	st := u33State(2, 2, u33Active("A", "B"))
	node := newU33Node("A")
	var opened []uint16
	node.h.cfg.Opened = func(slot uint16, _ uint64) { opened = append(opened, slot) }
	node.h.OnEpoch(st)
	var slot uint16
	for s := uint16(0); s < 64; s++ {
		if owner, ok := node.h.table.Owner(s, 2); ok && owner.ID == "A" {
			slot = s
			break
		}
	}
	if held, decided := node.h.RestartHold(slot, u33DB, 2, 2); !decided || !held {
		t.Fatalf("restart hold: held %v decided %v", held, decided)
	}
	key := pairKey{slot, u33DB}
	node.clock.set(time.Hour) // past the stop bound
	node.h.mu.Lock()
	node.h.apply(key, node.h.pairs[key], machine.CatchUpDone{Current: true})
	node.h.mu.Unlock()
	if err := node.h.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !node.h.pairs[key].barrier.ReadySent && node.h.pairs[key].barrier.Hold != machine.HoldNone {
		t.Fatalf("the restart hold did not end: %+v", node.h.pairs[key].barrier)
	}
	if len(opened) != 0 {
		t.Fatalf("a restart hold called Opened for %v", opened)
	}
}

// TestShipTailOfASeededNeverWrittenPairReadsNothing (BUG-0746): a pair seeded and not written
// since has an applied position and no cursor, because its cut has no record in this node's log.
// Sending its "tail" would mean reading the log from its first record for every such pair, so it
// sends nothing and reads nothing. [GUARD: ship with an unknown cursor]
func TestShipTailOfASeededNeverWrittenPairReadsNothing(t *testing.T) {
	owner, counting := countingNode(t, "owner")
	owner.copies = []string{"copy"}
	noiseAppend(owner, 300)
	owner.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) {
		p.AppliedPos, p.TailCursor = u32Pos(u32Epoch, 9), 0
	})
	counting.entries.Store(0)
	owner.rep.ShipTail(u32Slot, u32DB)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := owner.link.total(); got != 0 {
		t.Fatalf("a seeded, never written pair shipped %d frames, want none", got)
	}
	if read := counting.entries.Load(); read != 0 {
		t.Fatalf("a seeded, never written pair read %d log entries, want 0 (a read from the log's first record walks 300)", read)
	}
}

// TestAMemberThatWasDownAndIsActiveIsNoticed (BUG-0746): a member committed down that is active
// at the same incarnation is not seeded, so the owner must send it the tails again.
// [GUARD: wentActive always false]
func TestAMemberThatWasDownAndIsActiveIsNoticed(t *testing.T) {
	state := func(member cluster.MemberState) cluster.ClusterState {
		return cluster.ClusterState{Members: []cluster.Member{{ID: "n1", State: cluster.MemberActive}, {ID: "n2", State: member}}}
	}
	down := map[string]bool{"n2": true}
	if !wentActive(down, state(cluster.MemberActive)) {
		t.Fatal("a member that was down and is active went unnoticed")
	}
	if wentActive(down, state(cluster.MemberDown)) {
		t.Fatal("a member that is still down was taken for a return")
	}
	if wentActive(map[string]bool{}, state(cluster.MemberActive)) {
		t.Fatal("a member that was never down was taken for a return")
	}
}
