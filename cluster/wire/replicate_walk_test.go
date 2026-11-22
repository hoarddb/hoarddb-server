package wire

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// BUG-0865: after a restart every owned pair is queued at its old tail, and a read used to run
// from there to the end of the shared log to find the pair's later records. The tests below put
// the other pairs' entries AFTER each pair's record (the shape of a node whose pairs went cold
// at different times), which TestShipTailSendsOnlyThePairsTailRecord does not: it appends its
// noise first, so the tail is at the log's end and the walk is free.

// walkGap is the number of other pairs' entries after each pair's record. It is above
// maxTailLead, so the shared tail cache cannot bridge two pairs, and below maxChainLead, so a
// lane's chained cursor can.
const walkGap = maxTailLead + 400

// noiseUnsynced appends count entries of other pairs without a fsync each and syncs once, so a
// test can build a log of 10^5 entries in well under a second.
func noiseUnsynced(n *u32Node, count int) {
	n.t.Helper()
	rec := u32Record(n.t, u32Pos(u32Epoch, 1), cluster.Position{})
	record := storage.EncodeWALRecord(0, rec.Record.RequestID, rec.Record.Body)
	var last uint64
	for index := 0; index < count; index++ {
		slot := uint16(2000 + index%500)
		cursor, err := n.wal.AppendUnsyncedFrom("3.41", rec.Record.Class, storage.PositionKey(slot, u32DB, storage.LogPosition(rec.Pos)), record, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: rec.Pos.Epoch})
		if err != nil {
			n.t.Fatalf("AppendUnsyncedFrom: %v", err)
		}
		last = cursor
	}
	if err := n.wal.SyncThrough(last); err != nil {
		n.t.Fatalf("SyncThrough: %v", err)
	}
}

// walkSlots are the slots of the pairs a walk test restarts.
func walkSlots(count int) []uint16 {
	slots := make([]uint16, count)
	for index := range slots {
		slots[index] = uint16(10 + index)
	}
	return slots
}

// walkOwner is an owner that restarted: one record per pair, each followed by gap entries of other
// pairs, and every pair queued with ShipTail, as the supervisor does after a boot. The link
// batches when batched, so both lane paths are covered. The returned log counts the entries and
// the seeks the lane asks of the log.
func walkOwner(t *testing.T, batched bool, pairs, gap int) (*u32Node, *scanCountingLog) {
	t.Helper()
	owner, counting := walkOwnerWith(t, batched, nil)
	for _, slot := range walkSlots(pairs) {
		owner.ownAppend(slot, u32Pos(u32Epoch, 1), cluster.Position{})
		noiseUnsynced(owner, gap)
	}
	for _, slot := range walkSlots(pairs) {
		owner.rep.ShipTail(slot, u32DB)
	}
	return owner, counting
}

// walkOwnerWith builds the node of walkOwner without a log, over a counting log that wrap may
// wrap again.
func walkOwnerWith(t *testing.T, batched bool, wrap func(Log) Log) (*u32Node, *scanCountingLog) {
	t.Helper()
	var counting *scanCountingLog
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = counting
		if wrap != nil {
			cfg.Log = wrap(counting)
		}
		if batched {
			cfg.Link = &writeBatchLink{u32Link: cfg.Link.(*u32Link)}
		}
	})
	owner.copies = []string{"copy"}
	return owner, counting
}

func eachLaneKind(t *testing.T, run func(t *testing.T, batched bool)) {
	t.Helper()
	for _, tc := range []struct {
		name    string
		batched bool
	}{{"one record per call", false}, {"batched", true}} {
		t.Run(tc.name, func(t *testing.T) { run(t, tc.batched) })
	}
}

// requireOneTailFrameEach fails unless every pair's tail, and nothing else, reached the copy.
func requireOneTailFrameEach(t *testing.T, owner *u32Node, pairs int) {
	t.Helper()
	frames := owner.link.sent("copy")
	if len(frames) != pairs {
		t.Fatalf("%d frames sent, want one tail record for each of %d pairs", len(frames), pairs)
	}
	seen := make(map[uint16]bool)
	for _, frame := range frames {
		if frame.Pos != u32Pos(u32Epoch, 1) || seen[frame.Slot] {
			t.Fatalf("frame %+v is not a pair's only tail record (seen %v)", frame, seen)
		}
		seen[frame.Slot] = true
	}
}

// TestARestartedOwnerLaneReadsAreBoundedByTheLog (BUG-0865): the lane reads each pair's tail and
// stops at the pair's last known record; it does not walk to the end of the log for every pair.
// The entries decoded stay within a small multiple of the log, where the unbounded read decodes
// the log once per pair (about pairs/2 times). [GUARD: drop pairRead.through]
func TestARestartedOwnerLaneReadsAreBoundedByTheLog(t *testing.T) {
	const pairs = 24
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting := walkOwner(t, batched, pairs, walkGap)
		logEntries := int64(owner.wal.LastSequence())
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireOneTailFrameEach(t, owner, pairs)
		if read := counting.entries.Load(); read > 2*logEntries {
			t.Fatalf("restarting %d pairs decoded %d log entries, want at most %d (twice the log of %d entries): a read that does not stop at the pair's last record decodes about %d",
				pairs, read, 2*logEntries, logEntries, int64(pairs)*logEntries/2)
		}
	})
}

// TestALanePassSeeksTheLogAFewTimesNotOncePerPair (BUG-0865, R2): the pairs of one pass are read
// in log order on one chained cursor, so the pass seeks the log once however many pairs it holds.
// [GUARD: drop the sort / one cursor per pair]
func TestALanePassSeeksTheLogAFewTimesNotOncePerPair(t *testing.T) {
	const pairs = 24
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting := walkOwner(t, batched, pairs, walkGap)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireOneTailFrameEach(t, owner, pairs)
		if seeks := counting.seeks.Load(); seeks > 4 {
			t.Fatalf("one lane pass over %d pairs seeked the log %d times, want at most 4: each pair that starts within reach of the cursor the last read left must continue on it", pairs, seeks)
		}
	})
}

// cancelOnSeekLog cancels a context at its first seek: the moment a shutdown arrives while a
// lane pass is reading.
type cancelOnSeekLog struct {
	Log
	cancel context.CancelFunc
	// cancelAt is the seek that cancels, 1 when zero.
	cancelAt int
	seeks    int
	mu       sync.Mutex
}

func (l *cancelOnSeekLog) OpenCursor(from uint64) (storage.WALCursor, error) {
	l.mu.Lock()
	l.seeks++
	cancelNow := l.seeks == max(l.cancelAt, 1)
	l.mu.Unlock()
	if cancelNow {
		l.cancel()
	}
	return l.Log.OpenCursor(from)
}

func (l *cancelOnSeekLog) seekCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seeks
}

// TestALanePassStopsBetweenPairsOnCancel (BUG-0865, R3): a lane whose context ends stops at the
// next pair, and a read checks the context before it seeks the log, because a seek costs tens of
// milliseconds on a large segment and a pass holds thousands of pairs (the stop that needed
// SIGKILL). The pairs are spread past what one chained cursor reaches, so every pair would seek.
// [GUARD: drop the context check between pairs and before OpenCursor]
func TestALanePassStopsBetweenPairsOnCancel(t *testing.T) {
	const pairs = 5
	eachLaneKind(t, func(t *testing.T, batched bool) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var seeking *cancelOnSeekLog
		owner, _ := walkOwnerWith(t, batched, func(log Log) Log {
			seeking = &cancelOnSeekLog{Log: log, cancel: cancel}
			return seeking
		})
		for _, slot := range walkSlots(pairs) {
			owner.ownAppend(slot, u32Pos(u32Epoch, 1), cluster.Position{})
			noiseUnsynced(owner, 5*maxTailLead)
		}
		for _, slot := range walkSlots(pairs) {
			owner.rep.ShipTail(slot, u32DB)
		}
		if err := owner.flush(ctx); err != nil {
			t.Fatal(err)
		}
		if seeks := seeking.seekCount(); seeks != 1 {
			t.Fatalf("the lane seeked the log %d times after its context ended on the first seek, want exactly 1: a cancelled pass must not read the pairs after it", seeks)
		}
	})
}

// appendOnFirstReadLog runs hook once, at the first read of the log: after the lane has decided
// where to stop and before it has read anything.
type appendOnFirstReadLog struct {
	Log
	once sync.Once
	hook func()
}

func (l *appendOnFirstReadLog) ReadBatch(cur storage.WALCursor, upTo uint64, maxBytes int) ([]storage.WALEntry, storage.WALCursor, error) {
	l.once.Do(l.hook)
	return l.Log.ReadBatch(cur, upTo, maxBytes)
}

// TestABoundedReadStillShipsARecordAppendedDuringThePass (BUG-0865, the safety of R1): a record
// the owner appends and queues after the lane took its stop is above the stop, and must reach the
// copy on the pass its queueing causes, in order after the tail. The bound is safe only because
// every append of a pair is followed by an Enqueue that raises what the stop is read from.
// [GUARD: bound at TailCursor alone]
func TestABoundedReadStillShipsARecordAppendedDuringThePass(t *testing.T) {
	eachLaneKind(t, func(t *testing.T, batched bool) {
		var owner *u32Node
		var inserting *appendOnFirstReadLog
		owner, _ = walkOwnerWith(t, batched, func(log Log) Log {
			inserting = &appendOnFirstReadLog{Log: log, hook: func() {
				oldTail := owner.pairs.Snapshot(u32Slot, u32DB).TailCursor
				owner.ownAppend(u32Slot, u32Pos(u32Epoch, 2), cluster.Position{})
				// The write path queues a record before its apply moves TailCursor, so for the lane
				// the new record is covered by lastCursor alone: the bound must take the larger of the two.
				owner.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) { p.TailCursor = oldTail })
				owner.rep.Enqueue(u32Slot, u32DB, u32Pos(u32Epoch, 2), cluster.Position{}, owner.wal.LastSequence())
			}}
			return inserting
		})
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		noiseUnsynced(owner, 50)
		owner.rep.ShipTail(u32Slot, u32DB)
		for pass := 0; pass < 3; pass++ {
			if err := owner.flush(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		frames := owner.link.sent("copy")
		if len(frames) != 2 || frames[0].Pos != u32Pos(u32Epoch, 1) || frames[1].Pos != u32Pos(u32Epoch, 2) {
			t.Fatalf("the copy was sent %v, want positions 1 and 2 in order: the record appended during the pass was lost", frames)
		}
	})
}

// TestADeferredPairIsReadToTheEndOfTheLog (BUG-0865): a pair whose boot replay has not finished
// has a TailCursor that lags its log by design (BUG-0760), so the stop at the tail would hide the
// records it has not applied yet. Its read stays unbounded, as before. [GUARD: bound a deferred pair]
func TestADeferredPairIsReadToTheEndOfTheLog(t *testing.T) {
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, _ := walkOwnerWith(t, batched, nil)
		owner.rep.cfg.Deferred = func(slot uint16, db uint64) bool { return slot == u32Slot }
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 1), cluster.Position{})
		tailCursor := owner.wal.LastSequence()
		owner.ownAppend(u32Slot, u32Pos(u32Epoch, 2), cluster.Position{})
		// The pair record on disk is behind the log: the boot replay has not applied position 2.
		owner.pairs.With(u32Slot, u32DB, func(p *cluster.Pair) {
			p.AppliedPos, p.TailCursor = u32Pos(u32Epoch, 1), tailCursor
		})
		owner.rep.ShipTail(u32Slot, u32DB)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		frames := owner.link.sent("copy")
		if len(frames) != 2 || frames[1].Pos != u32Pos(u32Epoch, 2) {
			t.Fatalf("a deferred pair was sent %v, want its tail and the record after it (positions 1 and 2)", frames)
		}
	})
}

// TestAChainedCursorIsNotWalkedPastItsLead (BUG-0865, R2): a chained cursor is continued only when
// the next pair starts within maxChainLead entries of it; further away a seek is cheaper than
// walking, so the pairs here (spaced past the lead) each seek and read one entry.
// [GUARD: drop the lead check on the chain]
func TestAChainedCursorIsNotWalkedPastItsLead(t *testing.T) {
	const pairs = 5
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, counting := walkOwner(t, batched, pairs, maxChainLead+1000)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireOneTailFrameEach(t, owner, pairs)
		if read := counting.entries.Load(); read > 10*pairs {
			t.Fatalf("%d pairs spaced %d entries apart decoded %d log entries, want about %d: the chained cursor was walked across the gaps", pairs, maxChainLead+1000, read, pairs)
		}
	})
}

// TestAServedCatchUpOnAnEndedContextDoesNotSeekTheLog (BUG-0865, R3): the check before the seek is
// the only stop a served catch-up has before it reads, and a seek scans a segment.
// [GUARD: drop the context check before OpenCursor]
func TestAServedCatchUpOnAnEndedContextDoesNotSeekTheLog(t *testing.T) {
	owner, counting := walkOwnerWith(t, false, nil)
	owner.ownRun(u32Epoch, 1, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := owner.rep.ServeCatchUp(ctx, CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "copy", Epoch: u32Epoch, From: u32Pos(u32Epoch, 1)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a catch-up served on an ended context answered %v, want the context's error", err)
	}
	if seeks := counting.seeks.Load(); seeks != 0 {
		t.Fatalf("a catch-up served on an ended context seeked the log %d times, want 0", seeks)
	}
}

// TestACancelledBatchedPassSendsNothingItHadRead (BUG-0865, F6): a pass whose context ends while
// it reads sends none of the shipments it had read, instead of handing a stopped link a frame
// that only rewinds the pairs.
func TestACancelledBatchedPassSendsNothingItHadRead(t *testing.T) {
	const pairs = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var seeking *cancelOnSeekLog
	owner, _ := walkOwnerWith(t, true, func(log Log) Log {
		seeking = &cancelOnSeekLog{Log: log, cancel: cancel, cancelAt: 2}
		return seeking
	})
	for _, slot := range walkSlots(pairs) {
		owner.ownAppend(slot, u32Pos(u32Epoch, 1), cluster.Position{})
		noiseUnsynced(owner, 5*maxTailLead)
	}
	for _, slot := range walkSlots(pairs) {
		owner.rep.ShipTail(slot, u32DB)
	}
	if err := owner.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := seeking.seekCount(); got != 2 {
		t.Fatalf("the lane seeked %d times, want 2 (the first pair read, the second cancelled)", got)
	}
	if frames := owner.link.sent("copy"); len(frames) != 0 {
		t.Fatalf("a cancelled pass sent %d frames, want none", len(frames))
	}
}

// TestALaneReadDoesNotWaitForThePairLock (BUG-0865, F1): the owner's write holds the pair's lock
// through its apply and queues the record inside it, so a lane that took the lock to read the
// tail cursor would ship only after every apply. The read uses the pair's published view.
// [GUARD: read TailCursor with PairSet.Snapshot]
func TestALaneReadDoesNotWaitForThePairLock(t *testing.T) {
	eachLaneKind(t, func(t *testing.T, batched bool) {
		owner, _ := walkOwnerWith(t, batched, nil)
		owner.ownRun(u32Epoch, 1, 2)
		owner.rep.ShipTail(u32Slot, u32DB)
		held, release := make(chan struct{}), make(chan struct{})
		go owner.pairs.With(u32Slot, u32DB, func(*cluster.Pair) {
			close(held)
			<-release
		})
		<-held
		flushed := make(chan error, 1)
		go func() { flushed <- owner.flush(context.Background()) }()
		select {
		case err := <-flushed:
			close(release)
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			close(release)
			<-flushed
			t.Fatal("the lane did not ship while a writer held the pair's lock: its read waits for the lock")
		}
		if frames := owner.link.sent("copy"); len(frames) == 0 {
			t.Fatal("nothing was shipped")
		}
	})
}
