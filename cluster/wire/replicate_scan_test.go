package wire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// scanCountingLog counts the log entries and the cursor seeks a replicator asks of the log.
type scanCountingLog struct {
	Log
	entries atomic.Int64
	seeks   atomic.Int64
}

func (l *scanCountingLog) OpenCursor(from uint64) (storage.WALCursor, error) {
	l.seeks.Add(1)
	return l.Log.OpenCursor(from)
}

func (l *scanCountingLog) ReadBatch(cur storage.WALCursor, upTo uint64, maxBytes int) ([]storage.WALEntry, storage.WALCursor, error) {
	batch, next, err := l.Log.ReadBatch(cur, upTo, maxBytes)
	l.entries.Add(int64(len(batch)))
	return batch, next, err
}

// TestShippingNewPairsReadsTheLogOncePerRecord (BUG-0745): a pair the stream has not shipped yet
// starts reading at the record the write path just appended, not at the first retained record, and
// the next read of a pair continues on the cursor the last one left. Shipping one record for each
// of many new pairs reads about one log entry each; reading from the log's start for every pair
// is quadratic in the pairs. [GUARD: ignore the append hint / seek the log on every read]
func TestShippingNewPairsReadsTheLogOncePerRecord(t *testing.T) {
	var counting *scanCountingLog
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = counting
	})
	owner.copies = []string{"copy"}
	const pairs = 200
	for slot := uint16(0); slot < pairs; slot++ {
		pos := u32Pos(u32Epoch, 1)
		owner.ownAppend(slot, pos, cluster.Position{})
		owner.rep.Enqueue(slot, u32DB, pos, cluster.Position{}, owner.wal.LastSequence())
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(owner.link.sent("copy")); got != pairs {
		t.Fatalf("%d frames were shipped, want one for each of %d pairs", got, pairs)
	}
	if seeks := counting.seeks.Load(); seeks > 3 {
		t.Fatalf("shipping %d new pairs seeked the log %d times, want the first only: a pair with no cursor of its own starts from the tail cursor", pairs, seeks)
	}
	if read := counting.entries.Load(); read > 3*pairs {
		t.Fatalf("shipping one record for each of %d new pairs read %d log entries, want about %d (reading from the log's start for each pair reads about %d)", pairs, read, pairs, pairs*pairs/2)
	}
}

// TestASecondShipOfAPairContinuesOnTheCursor: after a read that reached the log's tail the next
// read of the pair does not seek the log again. [GUARD: drop the stored cursor]
func TestASecondShipOfAPairContinuesOnTheCursor(t *testing.T) {
	var counting *scanCountingLog
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = counting
	})
	owner.copies = []string{"copy"}
	for seq := uint64(1); seq <= 20; seq++ {
		pos := u32Pos(u32Epoch, seq)
		owner.ownAppend(u32Slot, pos, cluster.Position{})
		owner.rep.Enqueue(u32Slot, u32DB, pos, cluster.Position{}, owner.wal.LastSequence())
		// Forget the shared tails, so only the pair's own stored cursor can save the seek.
		owner.rep.mu.Lock()
		owner.rep.tails = nil
		owner.rep.mu.Unlock()
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: pos, Outcome: AckApplied})
	}
	if seeks := counting.seeks.Load(); seeks > 2 {
		t.Fatalf("20 records of one pair seeked the log %d times, want the first read only", seeks)
	}
}

// TestAPairShippedAfterALaterPairStillSkipsTheSeek: pairs written together are shipped in any
// order, and a pair whose record is before the newest tail starts from an older remembered one.
// [GUARD: remember one tail cursor only]
func TestAPairShippedAfterALaterPairStillSkipsTheSeek(t *testing.T) {
	var counting *scanCountingLog
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		counting = &scanCountingLog{Log: cfg.Log}
		cfg.Log = counting
	})
	owner.copies = []string{"copy"}
	ship := func(slot uint16) {
		pos := u32Pos(u32Epoch, 1)
		owner.rep.Enqueue(slot, u32DB, pos, cluster.Position{}, owner.wal.LastSequence()+0)
		if err := owner.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	pos := u32Pos(u32Epoch, 1)
	owner.ownAppend(0, pos, cluster.Position{})
	ship(0)
	owner.ownAppend(1, pos, cluster.Position{})
	cursor1 := owner.wal.LastSequence()
	owner.ownAppend(2, pos, cluster.Position{})
	ship(2)
	owner.rep.Enqueue(1, u32DB, pos, cluster.Position{}, cursor1)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(owner.link.sent("copy")); got != 3 {
		t.Fatalf("%d frames shipped, want 3", got)
	}
	if seeks := counting.seeks.Load(); seeks != 1 {
		t.Fatalf("three pairs shipped out of order seeked the log %d times, want the first only", seeks)
	}
}

// TestAnAppliedAckDoesNotReadTheLogsFirstSequence: on a real node FloorCursor takes the log's lock
// and reads its directory, and an ack arrives for every applied record, so the hot path asks for
// it only when a rewind needs it. [GUARD: compute the floor before looking at the ack]
func TestAnAppliedAckDoesNotReadTheLogsFirstSequence(t *testing.T) {
	var floors atomic.Int64
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		inner := cfg.FloorCursor
		cfg.FloorCursor = func(slot uint16, db uint64) uint64 { floors.Add(1); return inner(slot, db) }
	})
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 5)
	owner.rep.Enqueue(u32Slot, u32DB, u32Pos(u32Epoch, 5), cluster.Position{}, 1)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := floors.Load()
	for seq := uint64(1); seq <= 5; seq++ {
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, seq), Outcome: AckApplied})
	}
	if got := floors.Load() - before; got != 0 {
		t.Fatalf("five applied acks asked for the log's first sequence %d times, want 0", got)
	}
}

// scanFlakyLog reads one entry per batch and fails its second ReadBatch.
type scanFlakyLog struct {
	Log
	calls int
}

var errScanBoom = errors.New("injected log read failure")

func (l *scanFlakyLog) ReadBatch(cur storage.WALCursor, upTo uint64, _ int) ([]storage.WALEntry, storage.WALCursor, error) {
	l.calls++
	if l.calls == 2 {
		return nil, cur, errScanBoom
	}
	return l.Log.ReadBatch(cur, upTo, 1)
}

// TestALogErrorAfterTheFirstBatchOfAReusedCursorIsReportedAsItIs: a cursor kept across calls is
// sought again only if its first read fails; a failure on a later batch is the log's, and
// reopening at the start of the read would re-read records and report them out of order.
// [GUARD: leave reused set after the first batch]
func TestALogErrorAfterTheFirstBatchOfAReusedCursorIsReportedAsItIs(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.ownRun(u32Epoch, 1, 3)
	flaky := &scanFlakyLog{Log: owner.wal}
	cursor, err := owner.wal.OpenCursor(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readPairRecords(context.Background(), flaky, pairKey{u32Slot, u32DB}, pairRead{from: 1, records: 10, bytes: 1 << 20, cursor: &cursor})
	if !errors.Is(err, errScanBoom) || strings.Contains(err.Error(), "out of order") {
		t.Fatalf("a failure on the second batch was reported as %v, want the log's own error", err)
	}
}
