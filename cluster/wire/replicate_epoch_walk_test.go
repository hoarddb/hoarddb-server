package wire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// BUG-0872: after an epoch move (a restart, or a membership change in which this node stays the
// pair's owner) a pair's first write is the opener of the new epoch, seq 1, which carries the pair's
// previous end. The stream moved a copy's read forward only for the next seq of the same epoch, so
// the opener's read started where the copy's last read of the pair stopped, just after the pair's
// old tail, and walked every entry the other pairs wrote since: about half the log, once per pair
// and once per copy. An owner that loses the pair and regains it without a restart is not covered
// here: its copies' send state is stale and ShipTail queues no previous end.

// epochGap is the number of other pairs' entries between a pair's old tail and its opener in the
// tests that follow one pair: well past maxChainLead, so neither the tail cache nor a chained cursor
// reaches across it and only a read that walks the gap decodes it.
const epochGap = 4 * maxChainLead

// opener is the first position of the epoch after u32Epoch.
var opener = u32Pos(u32Epoch+1, 1)

// enqueueOwned queues a record the owner just appended at the pair's tail, as the write path does
// under the pair lock.
func enqueueOwned(n *u32Node, slot uint16, pos, prevEnd cluster.Position) {
	n.t.Helper()
	n.rep.Enqueue(slot, u32DB, pos, prevEnd, n.pairs.Snapshot(slot, u32DB).TailCursor)
}

// ackSentSince acknowledges as applied every frame sent to the copy from index from on, as a copy
// that holds them does, and returns how many frames have been sent.
func ackSentSince(n *u32Node, from int) int {
	frames := n.link.sent("copy")
	for _, frame := range frames[from:] {
		n.rep.OnWriteAck(WriteAck{From: "copy", Slot: frame.Slot, DB: frame.DB, Pos: frame.Pos, Outcome: AckApplied})
	}
	return len(frames)
}

// settleAcked flushes and acknowledges until nothing new is sent, and returns how many frames have
// been sent.
func settleAcked(t *testing.T, n *u32Node) int {
	t.Helper()
	sent := 0
	for pass := 0; pass < 4; pass++ {
		if err := n.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		sent = ackSentSince(n, sent)
	}
	return sent
}

// requirePositions fails unless the frames sent from index from on are exactly want, in order.
func requirePositions(t *testing.T, n *u32Node, from int, want ...cluster.Position) {
	t.Helper()
	frames := n.link.sent("copy")[from:]
	got := make([]cluster.Position, len(frames))
	for index, frame := range frames {
		got[index] = frame.Pos
	}
	if len(got) != len(want) {
		t.Fatalf("the copy was sent %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("the copy was sent %v, want %v", got, want)
		}
	}
}

// manualClock is a clock a test moves by hand, for the re-arm and ack deadlines.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(by time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(by)
	c.mu.Unlock()
}

// epochOwner is walkOwnerWith with a hand-moved clock.
func epochOwner(t *testing.T, batched bool) (*u32Node, *scanCountingLog, *manualClock) {
	t.Helper()
	clock := &manualClock{now: time.Unix(1_800_000_000, 0)}
	var counting *scanCountingLog
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = counting
		cfg.Now = clock.Now
		if batched {
			cfg.Link = &writeBatchLink{u32Link: cfg.Link.(*u32Link)}
		}
	})
	owner.copies = []string{"copy"}
	return owner, counting, clock
}

// TestTheFirstWriteOfANewEpochReadsNearItsRecord (BUG-0872): an owner restarts into a new epoch,
// ships every pair's old tail, and each pair then writes its opener. The opener's read starts near
// the opener, so the pass decodes about the entries written since the openers began, where the
// unfixed lane walks from each pair's old tail to its opener: about half the log per pair.
// [GUARD: drop the opener from Enqueue's "follows"]
func TestTheFirstWriteOfANewEpochReadsNearItsRecord(t *testing.T) {
	const pairs, between = 24, 50
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting := walkOwner(t, batched, pairs, walkGap)
		sent := settleAcked(t, owner)
		requireOneTailFrameEach(t, owner, pairs)
		logEntries := owner.wal.LastSequence()
		counting.entries.Store(0)
		counting.seeks.Store(0)
		for _, slot := range walkSlots(pairs) {
			owner.ownAppend(slot, opener, u32Pos(u32Epoch, 1))
			enqueueOwned(owner, slot, opener, u32Pos(u32Epoch, 1))
			noiseUnsynced(owner, between)
		}
		appended := int64(owner.wal.LastSequence() - logEntries)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		frames := owner.link.sent("copy")[sent:]
		seen := make(map[uint16]bool)
		for _, frame := range frames {
			if frame.Pos != opener || seen[frame.Slot] {
				t.Fatalf("frame %+v is not a pair's only opener", frame)
			}
			seen[frame.Slot] = true
		}
		if len(seen) != pairs {
			t.Fatalf("%d openers reached the copy, want %d", len(seen), pairs)
		}
		if read := counting.entries.Load(); read > 2*appended {
			t.Fatalf("the openers of %d pairs decoded %d log entries, want at most %d (twice the %d entries written since the restart pass): a read from each pair's old tail decodes about %d",
				pairs, read, 2*appended, appended, int64(pairs)*int64(logEntries)/2)
		}
		if seeks := counting.seeks.Load(); seeks > 4 {
			t.Fatalf("the openers of %d pairs seeked the log %d times, want at most 4: they lie close together and one chained cursor reaches them all", pairs, seeks)
		}
	})
}

// TestAnOpenerShippedAgainFromThePairsFirstReadSkipsTheGapBeforeIt (BUG-0872): a copy that refused
// the restarted owner's tail is re-armed at the record its first read started at, the old tail.
// That read ships the old tail and the opener, and does not walk the other pairs' entries between
// them: the pair has no record there. [GUARD: drop the jump over the pair's empty gap in the read]
func TestAnOpenerShippedAgainFromThePairsFirstReadSkipsTheGapBeforeIt(t *testing.T) {
	const between = 50
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting, clock := epochOwner(t, batched)
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		noiseUnsynced(owner, epochGap)
		owner.rep.ShipTail(u32Slot, u32DB)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, 0, u32Pos(u32Epoch, 1))
		// The copy is behind the old tail: it refuses it and waits for a catch-up.
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Outcome: AckRefused})
		owner.ownAppend(u32Slot, opener, u32Pos(u32Epoch, 1))
		enqueueOwned(owner, u32Slot, opener, u32Pos(u32Epoch, 1))
		noiseUnsynced(owner, between)
		clock.advance(time.Hour)
		owner.rep.Retry()
		counting.entries.Store(0)
		counting.seeks.Store(0)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, 1, u32Pos(u32Epoch, 1), opener)
		if read := counting.entries.Load(); read > 2*between {
			t.Fatalf("shipping the old tail and the opener again decoded %d log entries, want at most %d: the %d entries between them are other pairs'", read, 2*between, epochGap)
		}
		if seeks := counting.seeks.Load(); seeks > 2 {
			t.Fatalf("shipping the old tail and the opener again seeked the log %d times, want at most 2", seeks)
		}
	})
}

// TestACopyCaughtUpAcrossAnEpochMoveIsSentTheOpenerWithoutAWalk (BUG-0872): a copy that refused the
// old tail is caught up and acknowledges it after the opener was queued. The acknowledgement
// rewinds the copy to just after the old tail, which forgets any hint, and the next read still
// starts at the opener. [GUARD: drop the jump over the pair's empty gap at the read's start]
func TestACopyCaughtUpAcrossAnEpochMoveIsSentTheOpenerWithoutAWalk(t *testing.T) {
	const between = 50
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting, _ := epochOwner(t, batched)
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		noiseUnsynced(owner, epochGap)
		owner.rep.ShipTail(u32Slot, u32DB)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Outcome: AckRefused})
		owner.ownAppend(u32Slot, opener, u32Pos(u32Epoch, 1))
		enqueueOwned(owner, u32Slot, opener, u32Pos(u32Epoch, 1))
		noiseUnsynced(owner, between)
		// The catch-up's cumulative acknowledgement of the old tail.
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 1), Outcome: AckApplied})
		counting.entries.Store(0)
		counting.seeks.Store(0)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, 1, opener)
		if read := counting.entries.Load(); read > 2*between {
			t.Fatalf("sending the opener to a copy caught up to the old tail decoded %d log entries, want at most %d: the %d entries before it are other pairs'", read, 2*between, epochGap)
		}
	})
}

// TestACopyThatWasAwayAcrossAnEpochMoveGetsEveryRecordInOrder (BUG-0872, no record skipped): the
// copy acknowledged position 1, missed 2 and 3 (sent and never acknowledged), and the cluster moved
// to a new epoch while it was away. Once the ack deadline rewinds it, it is sent 2, 3 and the
// opener, in order, and the read skips only the other pairs' entries after 3.
// [GUARD: start the read at the opener for a copy that has not read the pair's tail]
func TestACopyThatWasAwayAcrossAnEpochMoveGetsEveryRecordInOrder(t *testing.T) {
	const between = 50
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting, clock := epochOwner(t, batched)
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		enqueueOwned(owner, u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		sent := settleAcked(t, owner)
		for seq := uint64(2); seq <= 3; seq++ {
			noiseUnsynced(owner, between)
			owner.ownAppend(u32Slot, u32Pos(u32Epoch, seq), cluster.Position{})
			enqueueOwned(owner, u32Slot, u32Pos(u32Epoch, seq), cluster.Position{})
		}
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, sent, u32Pos(u32Epoch, 2), u32Pos(u32Epoch, 3))
		noiseUnsynced(owner, epochGap)
		owner.ownAppend(u32Slot, opener, u32Pos(u32Epoch, 3))
		enqueueOwned(owner, u32Slot, opener, u32Pos(u32Epoch, 3))
		noiseUnsynced(owner, between)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		lost := len(owner.link.sent("copy"))
		clock.advance(time.Hour)
		owner.rep.Retry()
		counting.entries.Store(0)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, lost, u32Pos(u32Epoch, 2), u32Pos(u32Epoch, 3), opener)
		if read := counting.entries.Load(); read > 4*between {
			t.Fatalf("sending 2, 3 and the opener again decoded %d log entries, want at most %d: the %d entries after 3 are other pairs'", read, 4*between, epochGap)
		}
	})
}

// TestAnOpenerAfterARecordNeverQueuedIsNotJumpedTo (BUG-0872, the safety of the fix): position 2 of
// the pair reached the log without being queued, as a record applied while this node was a copy
// does, so the opener whose previous end is 2 does not follow the last queued record (1). The
// copy must be sent 2 before the opener; starting its read at the opener would skip it.
// [GUARD: treat every opener as following, whatever its previous end]
func TestAnOpenerAfterARecordNeverQueuedIsNotJumpedTo(t *testing.T) {
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, _, _ := epochOwner(t, batched)
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		enqueueOwned(owner, u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		sent := settleAcked(t, owner)
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 2), cluster.Position{})
		noiseUnsynced(owner, epochGap)
		owner.ownAppend(u32Slot, opener, u32Pos(u32Epoch, 2))
		enqueueOwned(owner, u32Slot, opener, u32Pos(u32Epoch, 2))
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, sent, u32Pos(u32Epoch, 2), opener)
	})
}

// TestACopyRewoundIntoTheNewestGapSkipsIt (BUG-0872, review F3): the pair keeps its newest large gap,
// not its first. The copy acknowledged the opener, which sits one large gap after the old tail; the
// pair's next record sits a second large gap later and is never acknowledged, so the re-arm rewinds
// the copy to just after the opener: inside the second gap. Its read starts at that record and walks
// neither gap. [GUARD: keep the oldest gap instead of the newest]
func TestACopyRewoundIntoTheNewestGapSkipsIt(t *testing.T) {
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting, clock := epochOwner(t, batched)
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		owner.rep.ShipTail(u32Slot, u32DB)
		settleAcked(t, owner)
		noiseUnsynced(owner, epochGap)
		owner.ownAppend(u32Slot, opener, u32Pos(u32Epoch, 1))
		enqueueOwned(owner, u32Slot, opener, u32Pos(u32Epoch, 1))
		sent := settleAcked(t, owner)
		noiseUnsynced(owner, epochGap)
		second := u32Pos(u32Epoch+1, 2)
		owner.ownAppend(u32Slot, second, cluster.Position{})
		enqueueOwned(owner, u32Slot, second, cluster.Position{})
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, sent, second)
		lost := len(owner.link.sent("copy"))
		// The copy could not apply it yet: the re-arm rewinds it to just after the opener it acknowledged.
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Outcome: AckNotApplied})
		clock.advance(time.Hour)
		owner.rep.Retry()
		counting.entries.Store(0)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requirePositions(t, owner, lost, second)
		if read := counting.entries.Load(); read > 100 {
			t.Fatalf("sending the record after the opener again decoded %d log entries, want at most 100: the %d entries between the opener and it are other pairs'", read, epochGap)
		}
	})
}
