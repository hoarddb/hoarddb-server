package wire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// BUG-0809: a pair with no record replays its first logged record as a no-op, so a first write
// whose apply failed (row 9) or whose unsynced Badger commit was lost to a power loss is skipped by
// recovery, the fence is lifted, and the next write takes the position after it.

// bug0809FailFirstInsert fails the apply of the first insert after its append, as a disk error would.
type bug0809FailFirstInsert struct {
	*DocumentApplier
	failed atomic.Bool
}

func (a *bug0809FailFirstInsert) FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if rec.Op.Class == cluster.OpInsert && a.failed.CompareAndSwap(false, true) {
		return nil, errors.New("disk on fire")
	}
	return a.DocumentApplier.FillWithResult(batch, rec)
}

// bug0809Readable polls Get until the document is readable or the timeout passes: a pair that is
// still fenced or held answers an error for a while, a skipped record answers one for good.
func bug0809Readable(node *Node, id string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := node.Get(context.Background(), u30iDB, "User", id)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// bug0809LockedOnly is the node's advancer without the pipeline's two sections, so an RF 1 write takes
// the locked path (Writer.order's single critical section). It is not a real RF 2 write: cluster/wire has
// no two-node Node harness, and the locked path is the one an RF 2 owner write takes.
type bug0809LockedOnly struct{ inner seedAwareAdvancer }

func (a bug0809LockedOnly) Advance(slot uint16, db uint64, step AdvanceStep) error {
	return a.inner.Advance(slot, db, step)
}

func (a bug0809LockedOnly) Recordless(slot uint16, db uint64) bool {
	return a.inner.Recordless(slot, db)
}

// TestAFirstRecordOfARecordlessPairIsReplayedNotSkipped (arm 1, row 9, RF 1 pipelined path): the first
// insert to a pair with no record is durable in the log and its apply fails; recoverPair must apply it
// before the fence lifts, and the next write must take the position after it without moving past it.
// Passes are held so that no interim record exists before the first write, which is the window
// between a database's creation and its first reconcile pass. [BREAK: skip a record-less pair on
// every replay]
func TestAFirstRecordOfARecordlessPairIsReplayedNotSkipped(t *testing.T) {
	bug0809FirstRecordReplayed(t, false)
}

// TestAFirstRecordOfARecordlessPairIsReplayedOnTheLockedPath is the same on the locked path (see
// bug0809LockedOnly: an RF 1 node forced onto it, not a real RF 2 write).
func TestAFirstRecordOfARecordlessPairIsReplayedOnTheLockedPath(t *testing.T) {
	bug0809FirstRecordReplayed(t, true)
}

func bug0809FirstRecordReplayed(t *testing.T, locked bool) {
	t.Helper()
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	defer release()
	dbID := s0Schema(t, node)
	ids, slot := s0IDsInOneSlot(t, 2)
	recon := node.cl.seeding.Load().recon
	if _, found := recon.Record(slot, dbID); found {
		t.Fatal("the pair has a record before its first write; the test needs a record-less pair")
	}
	documents, ok := node.writer.cfg.Applier.(*DocumentApplier)
	if !ok {
		t.Fatalf("the node's applier is %T, want *DocumentApplier", node.writer.cfg.Applier)
	}
	node.writer.cfg.Applier = &bug0809FailFirstInsert{DocumentApplier: documents}
	if locked {
		advancer, ok := node.writer.cfg.Advancer.(seedAwareAdvancer)
		if !ok {
			t.Fatalf("the node's advancer is %T, want seedAwareAdvancer", node.writer.cfg.Advancer)
		}
		node.writer.cfg.Advancer = bug0809LockedOnly{inner: advancer}
	}

	requestID := [16]byte{0x08, 0x09, 1}
	doc := bson.D{{Key: "_id_", Value: ids[0]}, {Key: "Name", Value: ids[0]}}
	first, err := node.Insert(context.Background(), u30iDB, "User", doc, WithRequestID(requestID))
	t.Logf("the first insert, whose apply failed, answered %+v, err %v", first, err)
	pair := node.pairs.Snapshot(slot, dbID)
	firstPos := cluster.Position{Epoch: pair.AppendedEpoch, Seq: pair.AppendedSeq}
	if firstPos.Seq == 0 {
		t.Fatal("the first insert took no position; the test needs it appended")
	}

	if err := bug0809Readable(node, ids[0], 3*time.Second); err != nil {
		t.Errorf("before any further write, the first insert at %s is not readable after recoverPair: %v; the pair's record is %v, its applied position %s",
			firstPos, err, node.SlotRecordState(slot, dbID), node.pairs.Snapshot(slot, dbID).AppliedPos)
	}
	record, found := recon.Record(slot, dbID)
	if !found || record.State != machine.SeedReady || record.Applied != firstPos || record.Ordinal != 1 || record.WALCursor == 0 {
		t.Errorf("after recoverPair the pair's record is %+v (found %v), want ready at %s, ordinal 1, with the write's cursor", record, found, firstPos)
	}
	if after := node.pairs.Snapshot(slot, dbID); after.AppliedPos != firstPos || after.Ordinal != 1 || after.Flags&flagReplayNeeded != 0 {
		t.Errorf("after recoverPair the pair is applied %s, ordinal %d, flags %#x; want %s, 1, not fenced", after.AppliedPos, after.Ordinal, after.Flags, firstPos)
	}

	retry, err := node.Insert(context.Background(), u30iDB, "User", doc, WithRequestID(requestID))
	if err != nil || !retry.Deduped || retry.Pos != firstPos {
		t.Errorf("a retry with the same request id answered %+v, err %v; want deduped at %s: the dedupe entry rides the replayed batch", retry, err, firstPos)
	}

	second, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: ids[1]}, {Key: "Name", Value: ids[1]}})
	if err != nil {
		t.Fatalf("the second insert: %v", err)
	}
	if second.Pos != (cluster.Position{Epoch: firstPos.Epoch, Seq: firstPos.Seq + 1}) {
		t.Errorf("the second insert took %s, want %d:%d, the position after the first", second.Pos, firstPos.Epoch, firstPos.Seq+1)
	}
	if err := bug0809Readable(node, ids[0], time.Second); err != nil {
		t.Errorf("after the second write at %s the first insert at %s is still not readable: it was skipped for good: %v", second.Pos, firstPos, err)
	}
	if record, _ := recon.Record(slot, dbID); record.Ordinal != 2 || record.Applied != second.Pos {
		t.Errorf("after the second write the record is %+v, want ordinal 2 at %s", record, second.Pos)
	}
}

// TestTheBatchThatCreatesAPairsRecordSurvivesAPowerLoss (arm 2, at the advancer): every owner and
// copy apply commits its batch unsynced (Commit(false)) and is answered after the log's fsync, so the
// one batch that must not stay in the page cache is the one that creates the pair's record: without
// it the boot replay finds a record-less pair and skips every logged record of it. A power loss
// (kvfault.PowerLoss discards every unsynced batch) must leave the created record on disk.
func TestTheBatchThatCreatesAPairsRecordSurvivesAPowerLoss(t *testing.T) {
	disk := kvfault.New()
	g, st, slots := s0ReconcilerRig(t, disk)
	e := uint64(st.Epoch)
	first := AdvanceRequest{
		Stage: func(batch storage.KVBatch, _ *cluster.Pair) (Advance, error) {
			if err := batch.Set(storage.Key(slots.owner, reconDB, 5, storage.NSDocuments, []byte("d1")), []byte("v")); err != nil {
				return Advance{}, err
			}
			return Advance{Applied: cluster.Position{Epoch: e, Seq: 1}, WALCursor: 7, Ordinal: 1}, nil
		},
		// As the Writer's own Commit: unsynced unless the advancer asks for a sync.
		Commit: func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, first); err != nil {
		t.Fatalf("the first write to a record-less in-target pair: %v", err)
	}
	disk.PowerLoss()
	snapshot := disk.NewSnapshot()
	defer snapshot.Discard()
	if _, err := snapshot.Get(SeedRecordKey(slots.owner, reconDB)); err != nil {
		t.Fatalf("after a power loss the record the first write created is gone (%v): the boot replay would skip the acknowledged write at %d:1 and every later record of the pair", err, e)
	}
}

// TestARecoveryReplayCreatesARecordOnlyWhereARowNineLeftItOwed (BUG-0809 part B, at the advancer): a
// replay of a record-less pair creates its record only at the pair's last appended position and only
// after that record's own row 9 (flagFirstApplyFailed); any other record of the pair is skipped, and a
// pair outside the copy set stays refused with nothing staged, so its fence stays. [BREAK: create on
// every flagged replay whatever its position; BREAK: create outside the copy set]
func TestARecoveryReplayCreatesARecordOnlyWhereARowNineLeftItOwed(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	e := uint64(st.Epoch)
	owedAt := cluster.Position{Epoch: e, Seq: 5}
	flag := func(slot uint16) {
		g.pairs.With(slot, reconDB, func(p *cluster.Pair) {
			p.Flags |= flagReplayNeeded | flagFirstApplyFailed
			p.AppendedEpoch, p.AppendedSeq = owedAt.Epoch, owedAt.Seq
		})
	}
	replay := func(slot uint16, pos cluster.Position) (staged bool, err error) {
		err = s0Advancer(g).Advance(slot, reconDB, AdvanceStep{
			Replay: true, Pos: pos,
			Stage: func(batch storage.KVBatch, _ *cluster.Pair) (AdvanceTo, error) {
				staged = true
				if err := batch.Set(storage.Key(slot, reconDB, 5, storage.NSDocuments, []byte("d1")), []byte("v")); err != nil {
					return AdvanceTo{}, err
				}
				return AdvanceTo{Applied: pos, WALCursor: 41, Ordinal: 1}, nil
			},
			Commit: func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
		})
		return staged, err
	}

	flag(slots.owner)
	if staged, err := replay(slots.owner, cluster.Position{Epoch: e, Seq: 4}); !errors.Is(err, errNothingStaged) || staged {
		t.Fatalf("the replay of an older record of a flagged record-less pair: err %v, staged %v; want errNothingStaged with nothing staged", err, staged)
	}
	if staged, err := replay(slots.owner, owedAt); err != nil || !staged {
		t.Fatalf("the replay of the owed record: err %v, staged %v; want the record created", err, staged)
	}
	if record, found := diskRecord(t, g.kv, slots.owner); !found || record.State != machine.SeedReady || record.Applied != owedAt || record.Ordinal != 1 || record.WALCursor != 41 {
		t.Fatalf("the created record is %+v (found %v), want ready at %s, ordinal 1, cursor 41", record, found, owedAt)
	}
	if g.pairs.Snapshot(slots.owner, reconDB).Flags&flagFirstApplyFailed != 0 {
		t.Fatal("the flag outlived the record it asked for")
	}

	flag(slots.outside)
	staged, err := replay(slots.outside, owedAt)
	if !errors.Is(err, ErrPairNotReady) || staged || !strings.Contains(err.Error(), "saved in this node's log") || !strings.Contains(err.Error(), "stays fenced") {
		t.Fatalf("the owed replay of a pair outside the copy set: err %v, staged %v; want ErrPairNotReady saying the write is saved and the pair stays fenced, nothing staged", err, staged)
	}
	if _, found := diskRecord(t, g.kv, slots.outside); found {
		t.Fatal("the refused replay left a record behind")
	}
}

// TestARecoveryScanThatMissesTheOwedRecordLeavesThePairFenced (review F1): recoverPair lifted the fence
// whenever the scan returned no error, though the owed record may not be in the scanned range (the log
// is truncated past it, or the pair's last appended position names no record). The pair must stay
// fenced, with the owed position in the error. [BREAK: call Recovered without reading the flag]
func TestARecoveryScanThatMissesTheOwedRecordLeavesThePairFenced(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	defer release()
	dbID := s0Schema(t, node)
	_, slot := s0IDsInOneSlot(t, 1)
	owed := cluster.Position{Epoch: 1, Seq: 9}
	node.pairs.With(slot, dbID, func(p *cluster.Pair) {
		p.Flags |= flagReplayNeeded | flagFirstApplyFailed
		p.AppendedEpoch, p.AppendedSeq = owed.Epoch, owed.Seq
	})
	err := node.recoverPair(context.Background(), slot, dbID)
	if err == nil || !strings.Contains(err.Error(), owed.String()) {
		t.Fatalf("recoverPair answered %v after a scan that found no owed record; want an error naming %s", err, owed)
	}
	flags := node.pairs.Snapshot(slot, dbID).Flags
	if flags&flagReplayNeeded == 0 || flags&flagFirstApplyFailed == 0 {
		t.Fatalf("the pair's flags are %#x; want it still fenced with the owed flag set", flags)
	}
}

// TestAnOwedRecordOfAnotherPairDoesNotFailAScan (review F2): the owed replay of a pair outside the copy
// set is refused, and the refusal ended the whole tail scan, so recovering any pair whose scan starts at
// or below that record failed. Writer.replay skips the refusal; the owed pair stays fenced.
// [BREAK: return the refusal from Writer.replay]
func TestAnOwedRecordOfAnotherPairDoesNotFailAScan(t *testing.T) {
	advancer := &failingAdvancer{err: errors.Join(ErrPairNotReady, errOwedNotHeld)}
	writer := &Writer{cfg: WriterConfig{Pairs: &cluster.PairSet{}, Advancer: advancer}}
	if err := writer.replay(bug0760Record(t, 3, 9, 1), nil); err != nil {
		t.Fatalf("the owed refusal of another pair failed the scan: %v", err)
	}
	if advancer.calls != 1 {
		t.Fatalf("the advancer was asked %d times, want 1", advancer.calls)
	}
}

// TestTheOwedRefusalOutsideTheCopySetIsDistinct: the advancer marks that refusal so Writer.replay can
// tell it from a pair that is not ready.
func TestTheOwedRefusalOutsideTheCopySetIsDistinct(t *testing.T) {
	g, st, slots := s0ReconcilerRig(t, nil)
	owedAt := cluster.Position{Epoch: uint64(st.Epoch), Seq: 5}
	g.pairs.With(slots.outside, reconDB, func(p *cluster.Pair) {
		p.Flags |= flagReplayNeeded | flagFirstApplyFailed
		p.AppendedEpoch, p.AppendedSeq = owedAt.Epoch, owedAt.Seq
	})
	err := s0Advancer(g).Advance(slots.outside, reconDB, AdvanceStep{
		Replay: true, Pos: owedAt,
		Stage:  func(storage.KVBatch, *cluster.Pair) (AdvanceTo, error) { return AdvanceTo{}, nil },
		Commit: func(batch storage.KVBatch, sync bool) error { return batch.Commit(sync) },
	})
	if !errors.Is(err, ErrPairNotReady) || !errors.Is(err, errOwedNotHeld) {
		t.Fatalf("the owed refusal is %v; want it to wrap both ErrPairNotReady and errOwedNotHeld", err)
	}
}
