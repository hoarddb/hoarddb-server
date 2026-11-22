package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The RF 1 owner write splits its critical section around the WAL fsync (pair-lock-across-fsync spec 3.2):
//
//	first section (pair lock): checks, assignment, unsynced append, push a pending entry
//	wait (no lock):            SyncThrough, which shares the group commit with every other waiter
//	second section (pair lock): apply the durable entries at the head of the pending list, in position order
//
// Every other user of the pair lock waits for the pending list to be empty (cluster.PairSet), so it sees
// the states it saw before the split. A write whose first section could read what a pending entry will write
// waits for the list to drain first (pendingConflicts): the only reads of the first section are keyed by the
// operation's own document or dedupe key, apart from the declaration, which only catalog writes and bumps
// change, and both of those drain.

// pipelineCap is the most owner writes of one pair that may be appended and not applied: the copy
// window, so a later RF 2 pipeline cannot outrun what one copy accepts in flight.
const pipelineCap = defaultWindow

// pipelinedAdvancer is the advancer a write to this database may split its section with: only an RF 1
// database does (a write to a larger RF keeps the locked path until the copy side can take it), and
// only an advancer that can run the two sections.
func (w *Writer) pipelinedAdvancer(dbRow DatabaseInfo) (PipelineAdvancer, bool) {
	if dbRow.RF != 1 {
		return nil, false
	}
	advancer, ok := w.cfg.Advancer.(PipelineAdvancer)
	return advancer, ok
}

// pendingApply is the apply state of one pending write. The pair lock guards every field except proved.
type pendingApply struct {
	rec AppliedRecord
	// proved is set by the write's own goroutine, outside the lock, once its SyncThrough returned nil: the
	// entry is durable whatever the log's synced sequence says (a lower bound).
	proved atomic.Bool

	done         bool
	result       []byte
	applyFailure error
	// syncErr is why the entry was neither applied nor known durable: its flush failed.
	syncErr error
	// notSynced is the answer of an entry whose batch committed and whose sync failed: applied once,
	// not known to survive a restart (BUG-0809).
	notSynced error
}

// pendingKeys are the hashes a pending entry is compared by: the document the operation changes (zero when
// it is not one document, which conflicts with everything) and its dedupe key.
func pendingKeys(req WriteRequest) (doc, dedupe uint64) {
	if len(req.Op.EncID) > 0 {
		key := binary.BigEndian.AppendUint64(nil, req.Op.BucketID)
		doc = xxhash.Sum64(append(key, req.Op.EncID...))
		if doc == 0 {
			doc = 1
		}
	}
	key := binary.BigEndian.AppendUint64(nil, req.Op.BucketID)
	key = append(key, req.RequestID[:]...)
	return doc, xxhash.Sum64(append(key, req.OpIndex))
}

// firstSection is what the first section hands to the rest of the write.
type firstSection struct {
	// out is the outcome of a write that was held, forwarded, deduped, re-admitted or whose append was
	// refused; err is a refusal.
	out ordered
	err error
	// busy is set when a reconcile pass holds the pair: the caller waits outside the lock and starts again.
	busy       error
	prepareErr error
	// work, cursor, pos and committed describe the entry the section pushed; work is nil when it pushed none.
	work      *pendingApply
	cursor    uint64
	pos       cluster.Position
	committed uint64
}

// orderPipelined is order for an RF 1 database.
func (w *Writer) orderPipelined(ctx context.Context, advancer PipelineAdvancer, req WriteRequest, dbRow DatabaseInfo, readmitted bool) (ordered, error) {
	doc, dedupe := pendingKeys(req)
	deadline := time.Now().Add(advanceBusyWait)
	for {
		var first firstSection
		ref := w.cfg.Pairs.WithPipeline(req.Slot, req.DBID, func(p *cluster.Pair, pending *cluster.Pipeline) bool {
			first = firstSection{}
			return w.firstSection(p, pending, advancer, req, dbRow, readmitted, doc, dedupe, &first)
		})
		switch {
		case first.busy != nil:
			// The reconcile pass commits in milliseconds; wait for it with no lock held, as Advance does.
			if time.Now().After(deadline) {
				return ordered{}, fmt.Errorf("advancing slot %d database %d for request %x: %w", req.Slot, req.DBID, req.RequestID, first.busy)
			}
			select {
			case <-ctx.Done():
				return ordered{}, fmt.Errorf("advancing slot %d database %d for request %x: waiting for the reconcile pass that holds the pair: %w", req.Slot, req.DBID, req.RequestID, ctx.Err())
			case <-time.After(advanceBusyPoll):
			}
			continue
		case first.prepareErr != nil:
			return ordered{}, fmt.Errorf("advancing slot %d database %d for request %x: %w", req.Slot, req.DBID, req.RequestID, first.prepareErr)
		case first.err != nil:
			return ordered{}, first.err
		case first.work == nil:
			return first.out, nil
		}
		return w.secondSection(ref, advancer, req, &first)
	}
}

// firstSection runs under the pair lock. It returns false only when the pair entry was dropped while it
// waited, before it had decided anything, and the caller runs it again on a fresh entry.
func (w *Writer) firstSection(p *cluster.Pair, pending *cluster.Pipeline, advancer PipelineAdvancer, req WriteRequest, dbRow DatabaseInfo, readmitted bool, doc, dedupe uint64, first *firstSection) bool {
	var plan stagePlan
	var committed uint64
	var recordless bool
	// heldHere is set once a PrepareLocked of this section added the pair's log-floor hold. Every exit
	// that appends nothing gives it back: the advancer's Stage, which reads Appended, never runs on those
	// exits, and a hold left there pins the floor for the life of the process (store-vouched-cut-build.md
	// §2, review edit F3). The epoch recheck's continue calls PrepareLocked again, which then adds nothing.
	heldHere := false
	defer func() {
		if heldHere && first.work == nil {
			advancer.AbandonLocked(req.Slot, req.DBID)
		}
	}()
	for {
		if !pending.WaitTurn(pipelineCap) {
			return false
		}
		// The one read of the committed epoch that this section assigns with. The overlap decision below
		// and the assignment must use the same value: an epoch that moved between them would give the opener
		// of the new epoch a PrevEnd below the pending positions, and every copy would reset the pair.
		committed = w.cfg.Topology.Committed()
		if pending.Len() > 0 && (committed != pending.Epoch() || req.Rewrite != nil || pending.Conflicts(doc, dedupe)) {
			// The write could read what a pending entry will write, or opens a new epoch, or is a catalog
			// write: it waits for the list to empty, then decides again from the start.
			start := time.Now()
			if !pending.Drain() {
				return false
			}
			if w.cfg.DrainObserved != nil {
				w.cfg.DrainObserved(time.Since(start))
			}
			continue
		}
		var err error
		var held bool
		recordless, held, err = advancer.PrepareLocked(p, req.Slot, req.DBID)
		heldHere = heldHere || held
		if err != nil {
			if errors.Is(err, ErrPairBusy) {
				first.busy = err
			} else {
				first.prepareErr = err
			}
			return true
		}
		var out ordered
		var proceed bool
		plan, out, proceed, err = w.planStage(p, req, dbRow, readmitted, uint64(pending.Len()), func() uint64 { return committed })
		if !proceed {
			first.out, first.err = out, err
			return true
		}
		if w.cfg.Topology.Committed() != committed {
			// The epoch moved between the decision and the append: decide again on the new one. This is a
			// backstop of the one-read rule above (planStage assigns with the epoch this section read), not
			// the guard: it cannot close the window it names, because the epoch can still move after it, as
			// after any single read (F8). TestTheEpochRecheckDecidesAgainOnAMovedEpoch is the test that
			// needs it.
			continue
		}
		break
	}

	cursor, err := w.cfg.Log.AppendUnsyncedFrom("", plan.op.Class, plan.key, plan.record, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: committed})
	if err != nil {
		// Row 8: fence the pair. No further position is taken until a replay, and appendedSeq is unchanged, so
		// this position is reused. The pipeline is not marked: the pending entries are written and still apply.
		p.Flags |= flagReplayNeeded
		first.out = ordered{appendErr: err, pos: plan.pos, committed: committed}
		return true
	}
	p.AppendedSeq, p.AppendedEpoch = plan.pos.Seq, committed
	work := &pendingApply{rec: AppliedRecord{
		Slot: req.Slot, DB: req.DBID, Op: plan.op, Pos: plan.pos, Cursor: cursor,
		RequestID: req.RequestID, OpIndex: req.OpIndex, ClaimSink: req.ClaimSink,
	}}
	pushDoc := doc
	if recordless {
		// The write that creates the pair's record: nothing may be appended behind it (a zero key conflicts
		// with every later write) until it has applied. A replay of a record-less pair redoes only the
		// record at its last appended position, and only after that record's own row 9 (flagFirstApplyFailed,
		// BUG-0809), so a second record appended behind this one could never be replayed.
		pushDoc = 0
	}
	pending.Push(cluster.PendingWrite{Pos: plan.pos, Cursor: cursor, DocKey: pushDoc, DedupeKey: dedupe, Work: work})
	first.work, first.cursor, first.pos, first.committed = work, cursor, plan.pos, committed
	return true
}

// secondSection waits for the write's flush with no lock held, then applies every durable entry at the head
// of the pending list, in position order, and reads this write's own outcome.
func (w *Writer) secondSection(ref *cluster.PipelineRef, advancer PipelineAdvancer, req WriteRequest, first *firstSection) (ordered, error) {
	syncErr := w.cfg.Log.SyncThrough(first.cursor)
	if syncErr == nil {
		first.work.proved.Store(true)
	}
	var mine pendingApply
	finished := false
	ref.With(func(p *cluster.Pair, pending *cluster.Pipeline) {
		// A flush that failed proves nothing about this entry; the log's synced sequence is a lower bound of
		// what is durable, so the entries it covers still apply, and no entry above it does.
		durable := first.cursor
		if syncErr != nil {
			durable = w.cfg.Log.SyncedSequence()
		}
		w.applyDurablePrefix(advancer, p, pending, req.Slot, req.DBID, durable)
		if syncErr != nil {
			w.failUnsynced(p, pending, first.cursor, syncErr)
		}
		if first.work.done {
			finished = true
			mine.result, mine.applyFailure, mine.syncErr, mine.notSynced = first.work.result, first.work.applyFailure, first.work.syncErr, first.work.notSynced
		}
	})
	switch {
	case !finished:
		return ordered{}, fmt.Errorf("position %s of request %x is still pending after its flush: the pair's pending list lost it, so whether it applied is not known; retry with the same request id", first.pos, req.RequestID)
	case mine.syncErr != nil && errors.Is(mine.syncErr, storage.ErrLogClosed):
		// Close fsyncs the active file, so the record is durable unless that failed, and it is not applied
		// either way: answer as a crash after the fsync does, with no refusal code, so a retry with the same
		// request id is deduped after the restart's replay.
		return ordered{}, fmt.Errorf("the log closed while position %s of request %x waited for its fsync, so whether it was written is not known; retry with the same request id: %w", first.pos, req.RequestID, mine.syncErr)
	case mine.notSynced != nil:
		return ordered{}, mine.notSynced
	case mine.syncErr != nil:
		// A failed flush poisons the log, and the record may or may not be on disk: the same unknown outcome
		// as a closed log, not a refusal, so no client is told a write was never made. A retry with the same
		// request id is deduped once the restart's replay has applied it, if it was written.
		return ordered{}, fmt.Errorf("the flush of position %s of request %x failed, so whether it was written is not known; retry with the same request id: %w", first.pos, req.RequestID, mine.syncErr)
	}
	return ordered{durable: true, pos: first.pos, committed: first.committed, result: mine.result, applyFailure: mine.applyFailure}, nil
}

// applyDurablePrefix applies, in position order, the head entries that are durable: at or below durable,
// or proved by their own flush. Every durable entry is enqueued for the copies, applied or not, because the
// stream ships only the pairs that were enqueued. Once an entry failed to apply the rest are enqueued and
// answered durable with an apply failure, and the pair's replay applies them all.
func (w *Writer) applyDurablePrefix(advancer PipelineAdvancer, p *cluster.Pair, pending *cluster.Pipeline, slot uint16, db uint64, durable uint64) {
	for head := pending.Head(); head != nil; head = pending.Head() {
		// Invariant: only firstSection pushes entries, and it pushes this type. A foreign entry has no place
		// to record an apply failure, so a branch that popped it would acknowledge a write nothing applied;
		// the panic is the only honest answer to a second pusher that does not carry the state (flash F2).
		work := head.Work.(*pendingApply)
		if head.Cursor > durable && !work.proved.Load() {
			return
		}
		w.cfg.Replication.Enqueue(slot, db, head.Pos, work.rec.Op.PrevEnd, head.Cursor)
		if pending.Failed() {
			work.applyFailure = fmt.Errorf("position %s is saved in the log and is not lost, but it is not applied yet: an earlier write to the same pair could not be applied, so the pair applies its saved writes again, in order, which makes this one visible", head.Pos)
		} else {
			w.applyPending(advancer, p, pending, slot, db, work)
		}
		work.done = true
		pending.PopHead()
	}
}

// applyPending is the second half of today's critical section for one entry: fill, the pair record and the
// Badger commit (unsynced unless it creates the pair's record, BUG-0809), through the advancer. A failure is
// row 9: the position stays taken, the pair stops serving until its replay re-applies the record, and the
// pipeline's own mark stops the entries behind it.
func (w *Writer) applyPending(advancer PipelineAdvancer, p *cluster.Pair, pending *cluster.Pipeline, slot uint16, db uint64, work *pendingApply) {
	pos := work.rec.Pos
	step := AdvanceStep{
		// The first section admitted this write under the pair's lock and appended it; the second applies it
		// whatever the committed cluster state says now.
		Admitted: true,
		Appended: func() bool { return true },
		Stage: func(batch storage.KVBatch, pair *cluster.Pair) (AdvanceTo, error) {
			rec := work.rec
			rec.Ordinal = pair.Ordinal + 1
			filled, err := w.fill(batch, rec)
			if err != nil {
				work.applyFailure = err
				return AdvanceTo{}, err
			}
			work.result = filled
			return AdvanceTo{Applied: pos, WALCursor: rec.Cursor, Ordinal: rec.Ordinal}, nil
		},
		Commit: func(batch storage.KVBatch, sync bool) error {
			if err := batch.Commit(sync); err != nil {
				if errors.Is(err, storage.ErrCommittedNotSynced) {
					// Not row 9: the batch is in the store and the advancer installs it (BUG-0809).
					work.notSynced, work.result = notSyncedAnswer(pos, work.rec.RequestID, err), nil
					return err
				}
				work.applyFailure, work.result = fmt.Errorf("committing the batch of position %s: %w", pos, err), nil
				return work.applyFailure
			}
			return nil
		},
	}
	if err := advancer.AdvanceLocked(p, slot, db, step); err != nil && work.notSynced == nil {
		if work.applyFailure == nil {
			// The advancer refused before the stage, which nothing should produce: the busy mark and every
			// change of a record are made under the pair's lock after the list drained, so neither can appear
			// between the first section and now (ErrPairBusy is unreachable here), and the step is Admitted,
			// so the copy set, which the cluster state moves without this lock, is not judged again. Fail
			// closed rather than apply.
			work.applyFailure, work.result = fmt.Errorf("applying position %s: %w", pos, err), nil
		}
		p.Flags |= flagReplayNeeded
		p.Flags &^= flagReplayed
		w.markFirstApplyFailed(p, slot, db)
		pending.Fail()
	}
}

// failUnsynced ends, with the flush's error, every head entry at or below cursor that the flush neither
// proved nor the log's synced sequence covers, marks the pipeline failed and fences the pair as a refused
// append does. Entries above cursor are left to their own writers, whose flush reports the same failure.
func (w *Writer) failUnsynced(p *cluster.Pair, pending *cluster.Pipeline, cursor uint64, syncErr error) {
	// Marked before the pops, and only for a list that has entries: the mark ends when the list empties, so
	// one set after the last pop, or on a list another writer's section already emptied, would outlive it and
	// fail the first write after the pair's replay (flash F1).
	if pending.Len() > 0 {
		pending.Fail()
	}
	p.Flags |= flagReplayNeeded
	for head := pending.Head(); head != nil && head.Cursor <= cursor; head = pending.Head() {
		work := head.Work.(*pendingApply) // invariant: only firstSection pushes entries, and it pushes this type
		work.syncErr = syncErr
		work.done = true
		pending.PopHead()
	}
}
