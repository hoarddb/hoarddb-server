package wire

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The reasons a record is rejected before the sequence rules see it
// (machine.RecordRejected.Because).
const (
	rejectedByFence   uint8 = 1
	rejectedByDecoder uint8 = 2
)

// ownerOutcomeApplied is the WALOrigin.OwnerOutcome of a record the owner shipped
// because it applied it; a refused write consumes no position, so nothing else is sent.
const ownerOutcomeApplied byte = 1

type applyVerdict uint8

const (
	// verdictApplied: the record was the next one and is now durable and applied.
	verdictApplied applyVerdict = iota + 1
	// verdictDuplicate: the copy already holds the record.
	verdictDuplicate
	// verdictGap: the copy is behind the record and must catch up; nothing was applied.
	verdictGap
	// verdictReset: the copy holds a tail nobody owns.
	verdictReset
	// verdictNotApplied: the machine could not judge the record yet (its epoch is older).
	verdictNotApplied
)

type applyOutcome struct {
	verdict applyVerdict
	// askFrom is the first position the copy still needs, for verdictGap.
	askFrom cluster.Position
	// applied is the copy's applied position after the call.
	applied cluster.Position
}

// WriteResult is a copy's answer to one WRITE: the ack to send back, and the catch-up it
// wants, if any.
type WriteResult struct {
	Ack WriteAck
	// CatchUpFrom, when set, is where the copy wants CatchUp to start.
	CatchUpFrom *cluster.Position
}

// WriteAdmission is the synchronous half of a WRITE on a copy: either the ack that answers it
// at once (not applied), or the decoded record, ready to be sequenced and applied.
type WriteAdmission struct {
	frame WriteFrame
	op    cluster.Op
	// ack is the answer when the frame was not admitted.
	ack      WriteAck
	admitted bool
}

// Admitted reports whether the frame was admitted. If not, the admission carries the ack that
// answers the WRITE at once.
func (a WriteAdmission) Admitted() bool { return a.admitted }

// AdmitWrite is everything a copy decides about a pushed record without touching the pair's
// sequence: a seeding or behind copy answers "not applied", a frame from another epoch is
// fenced (and a frame from a newer one tells the node it is behind), and a record that does
// not decode, is of an unbuilt class or names a field this copy cannot resolve yet is
// refused. It must run on the connection goroutine: the fence has to take effect before
// the node serves again (conventions section 7), and a copy that cannot apply has to say so
// in the reply, so the owner stops its batch at the first such frame. It takes no pair lock,
// because a copy apply (applyDurably) holds that lock across its AppendFrom fsync: a not-applied
// ack carries no position (the owner ignores it) and a refusal is fed to the machine without one.
func (r *Replicator) AdmitWrite(frame WriteFrame) (WriteAdmission, error) {
	key := pairKey{frame.Slot, frame.DB}
	ack := WriteAck{From: r.cfg.Self, Slot: frame.Slot, DB: frame.DB}
	notApplied := WriteAdmission{frame: frame}
	if r.cfg.Ready != nil && !r.cfg.Ready(frame.Slot, frame.DB) {
		if r.cfg.Buffer != nil {
			r.cfg.Buffer.Offer(frame.Slot, frame.DB, frame)
		}
		ack.Outcome = AckNotApplied
		notApplied.ack = ack
		return notApplied, nil
	}
	committed := r.cfg.Committed()
	if frame.Epoch > committed && r.cfg.MarkBehind != nil {
		r.cfg.MarkBehind(frame.Epoch)
	}
	if frame.Epoch == committed && r.cfg.Behind != nil && r.cfg.Behind(committed) {
		// The cluster moved past this node's epoch; a record of the old epoch is not applied
		// by a node that has stopped serving. The owner holds it in flight and ships it again
		// once this copy answers applied.
		ack.Outcome = AckNotApplied
		notApplied.ack = ack
		return notApplied, nil
	}
	if frame.Epoch != committed {
		if err := r.reject(key, frame.Pos, frame.From, rejectedByFence); err != nil {
			return WriteAdmission{}, err
		}
		return WriteAdmission{}, &cluster.Error{Code: cluster.CodeEpochFenced, Message: cluster.CodeEpochFenced.Text(cluster.CondEpochFenced, map[string]string{
			"kind": "WRITE", "node": frame.From, "e": strconv.FormatUint(frame.Epoch, 10), "e_seen": strconv.FormatUint(committed, 10),
		})}
	}
	op, err := r.decodeRecord(frame.From, frame.PairRecord, key)
	if err != nil {
		return WriteAdmission{}, err
	}
	return WriteAdmission{frame: frame, op: op, admitted: true}, nil
}

// OnWrite applies one pushed record on a copy at once (write path section 4): AdmitWrite,
// then the sequence rules, which apply it only when it is the next one. A gap is refused
// SEQ_GAP and asks for a catch-up, never applied out of order, and a divergent tail asks
// the Seeder to reset the pair, which is the only place anything is discarded. The live
// stream does not use it: a copy's node admits a frame with AdmitWrite and queues it with
// EnqueueWrite, so the connection does not wait for the fsync.
func (r *Replicator) OnWrite(frame WriteFrame) (WriteResult, error) {
	key := pairKey{frame.Slot, frame.DB}
	adm, err := r.AdmitWrite(frame)
	if err != nil {
		return WriteResult{}, err
	}
	if !adm.admitted {
		return WriteResult{Ack: adm.ack}, nil
	}
	out, err := r.applyDecoded(frame.From, r.cfg.Committed(), frame.PairRecord, adm.op, key)
	if err != nil {
		return WriteResult{}, err
	}
	ack, _, err := r.ackFor(key, out)
	if err != nil {
		return WriteResult{}, err
	}
	reply := WriteResult{Ack: ack}
	if out.verdict == verdictGap {
		reply.CatchUpFrom = &out.askFrom
	}
	return reply, nil
}

// ackFor is the one place an apply outcome becomes the ack a copy sends, for the live queue and
// for OnWrite alike. It also does what the outcome calls for on the copy: a gap clears the pair's
// current flag, and a divergent tail asks the Seeder to reset the pair. applied reports that the
// record is now held, new or a duplicate.
func (r *Replicator) ackFor(key pairKey, out applyOutcome) (ack WriteAck, applied bool, err error) {
	ack = WriteAck{From: r.cfg.Self, Slot: key.slot, DB: key.db, Pos: out.applied}
	switch out.verdict {
	case verdictApplied, verdictDuplicate:
		ack.Outcome = AckApplied
		return ack, true, nil
	case verdictGap:
		r.clearCurrent(key)
		ack.Outcome = AckRefused
	case verdictReset:
		ack.Outcome = AckRefused
		// A pushed record is a copy's business. If this node owns the pair by now, the epoch
		// moved after the record was judged and the verdict is about an ownership that has
		// ended: the handoff's catch-up decides what the new owner's pair needs (BUG-0816).
		// The reset holds the pair's catch-up flight, so the handoff waits for it.
		if !r.owns(key) {
			err = r.resetAsCopy(key, ReseedDivergent)
		}
	default:
		ack.Outcome = AckNotApplied
	}
	return ack, false, err
}

func (r *Replicator) clearCurrent(key pairKey) {
	r.mu.Lock()
	delete(r.current, key)
	r.mu.Unlock()
}

// reset asks the Seeder to reset and seed the pair.
func (r *Replicator) reset(key pairKey, cause ReseedCause) error {
	r.clearCurrent(key)
	if err := r.cfg.Seeds.ResetAndSeed(key.slot, key.db, cause); err != nil {
		return fmt.Errorf("resetting and seeding slot %d of database %d: %w", key.slot, key.db, err)
	}
	return nil
}

// report hands an error that has no caller to the configured sink.
func (r *Replicator) report(err error) {
	if r.cfg.OnError != nil {
		r.cfg.OnError(err)
	}
}

// reject feeds a record the fence or the decoder refused to the machine, which is inert
// for it. A machine that changed would be a bug, so it is an error.
func (r *Replicator) reject(key pairKey, pos cluster.Position, from string, because uint8) error {
	// No pair lock and no applied position: a refusal is inert whatever the pair holds, and this
	// runs on the connection, which must not wait for a worker's fsync.
	seq := machine.PairSeq{Epoch: r.cfg.Committed()}
	if _, changed := seq.Apply(machine.RecordRejected{Got: pos, From: from, Because: because}); changed {
		return fmt.Errorf("a refused record at %s of slot %d changed the sequence state: refusal must be inert", pos, key.slot)
	}
	return nil
}

// decodeRecord decodes a record's operation and checks it is one this copy can append: the
// record's class matches its body, the class is built, and the copy has published the
// declaration version the record was written under (a copy that cannot resolve the
// record's fields yet refuses it, so nothing is appended and then failed).
func (r *Replicator) decodeRecord(source string, rec PairRecord, key pairKey) (cluster.Op, error) {
	op, err := cluster.DecodeOp(rec.Record.Body)
	if err == nil && op.Class != rec.Record.Class {
		err = fmt.Errorf("the record says class %#02x and its body says %#02x", rec.Record.Class, op.Class)
	}
	if err == nil && !r.cfg.Applier.Handles(op.Class) {
		err = fmt.Errorf("operation class %#02x is not built yet", op.Class)
	}
	if err != nil {
		if rejectErr := r.reject(key, rec.Pos, source, rejectedByDecoder); rejectErr != nil {
			return cluster.Op{}, rejectErr
		}
		return cluster.Op{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("refused position %s of slot %d from %s: %v. Nothing was applied", rec.Pos, key.slot, source, err)}
	}
	if r.cfg.SchemaReady != nil {
		// Before the pair lock and before any append.
		if err := r.cfg.SchemaReady(key.db, op); err != nil {
			return cluster.Op{}, err
		}
	}
	return op, nil
}

// applyRecord offers one record to the pair's sequence rules and, when it is the next
// one, makes it durable and applies it. The pair lock covers the whole of it, as it
// does on the owner.
func (r *Replicator) applyRecord(source string, committed uint64, rec PairRecord, key pairKey) (applyOutcome, error) {
	op, err := r.decodeRecord(source, rec, key)
	if err != nil {
		return applyOutcome{}, err
	}
	return r.applyDecoded(source, committed, rec, op, key)
}

// applyDecoded is applyRecord for a record decodeRecord has already accepted.
func (r *Replicator) applyDecoded(source string, committed uint64, rec PairRecord, op cluster.Op, key pairKey) (applyOutcome, error) {
	var out applyOutcome
	var failure error
	r.cfg.Pairs.With(key.slot, key.db, func(p *cluster.Pair) {
		out, failure = r.applyLocked(p, applyInput{source: source, committed: committed, rec: rec, op: op, key: key})
	})
	return out, failure
}

type applyInput struct {
	source    string
	committed uint64
	rec       PairRecord
	op        cluster.Op
	key       pairKey
}

// applyLocked offers one record to the pair. The record state is read here, under the pair
// lock, for every path that applies (the live queue, OnWrite, the catch-up): a copy that was
// reset while a catch-up held a chunk, or while a frame waited in the queue, must apply nothing
// into the range the reset is deleting. Checking once before a loop would leave the records
// after the flip to the advancer's own refusal, which cannot tell a gap from a reset. The lock
// order is the pair lock, then the reconciler's mutex, as everywhere.
func (r *Replicator) applyLocked(p *cluster.Pair, in applyInput) (applyOutcome, error) {
	if r.cfg.Ready != nil && !r.cfg.Ready(in.key.slot, in.key.db) {
		return applyOutcome{verdict: verdictNotApplied, applied: p.AppliedPos}, nil
	}
	if p.Flags&flagReplayNeeded != 0 {
		return applyOutcome{verdict: verdictNotApplied, applied: p.AppliedPos}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf("slot %d of database %d waits for its log replay before it applies more records. Nothing was applied; the replay runs on its own", in.key.slot, in.key.db)}
	}
	seq := machine.PairSeq{Epoch: in.committed, AppliedPos: p.AppliedPos, PrevEnd: p.AppliedPos}
	decision, _ := seq.Apply(machine.RecordApplied{Pos: in.rec.Pos, RequestID: in.rec.Record.RequestID, PrevEnd: in.op.PrevEnd})
	switch {
	case decision.ResetAndSeed:
		return applyOutcome{verdict: verdictReset, applied: p.AppliedPos}, nil
	case decision.AskCatchUp != nil:
		return applyOutcome{verdict: verdictGap, askFrom: *decision.AskCatchUp, applied: p.AppliedPos}, nil
	case seq.AppliedPos == in.rec.Pos && p.AppliedPos != in.rec.Pos:
		return r.applyDurably(p, in)
	case !p.AppliedPos.Less(in.rec.Pos):
		return applyOutcome{verdict: verdictDuplicate, applied: p.AppliedPos}, nil
	}
	return applyOutcome{verdict: verdictNotApplied, applied: p.AppliedPos}, nil
}

// applyDurably appends the record to this node's log with the owner as its source,
// which fsyncs it, then applies it and the pair record in one Badger batch (write
// path §4: an ack means fsynced and applied). The batch is the owner's: the advancer
// adds the pair record to the data and installs the position after the commit, so a
// restarted copy loads the position it acked instead of replaying from its seed cut
// (BUG-0752). The advancer runs under the pair lock this call already holds, through
// LockedAdvancer; it refuses before Stage when the pair is not ready, and then nothing
// was appended.
func (r *Replicator) applyDurably(p *cluster.Pair, in applyInput) (applyOutcome, error) {
	rec := in.rec
	appended := false
	step := AdvanceStep{
		Appended: func() bool { return appended },
		Stage: func(batch storage.KVBatch, pair *cluster.Pair) (AdvanceTo, error) {
			// Before the append: a record whose declaration cannot be read is refused with nothing
			// durable, so no apply can fail after it and fence a pair no copy replays (BUG-0771).
			if resolver, ok := r.cfg.Applier.(EngineResolver); ok {
				if err := resolver.ResolveEngines(in.key.db, in.op); err != nil {
					return AdvanceTo{}, err
				}
			}
			record := storage.EncodeWALRecord(rec.Record.OpIndex, rec.Record.RequestID, rec.Record.Body)
			origin := storage.WALOrigin{Kind: storage.WALOriginReplica, Source: in.source, Seq: rec.Cursor, OwnerOutcome: ownerOutcomeApplied, Term: rec.Pos.Epoch}
			cursor, err := r.cfg.Log.AppendFrom("", rec.Record.Class, storage.PositionKey(in.key.slot, in.key.db, storage.LogPosition(rec.Pos)), record, origin)
			if err != nil {
				return AdvanceTo{}, fmt.Errorf("appending position %s of slot %d from %s: %w", rec.Pos, in.key.slot, in.source, err)
			}
			appended = true
			applied := AppliedRecord{
				Slot: in.key.slot, DB: in.key.db, Op: in.op, Pos: rec.Pos, Cursor: cursor, Ordinal: pair.Ordinal + 1,
				RequestID: rec.Record.RequestID, OpIndex: rec.Record.OpIndex,
			}
			// The result fillApplied returns has no caller on a copy: the dedupe entry carries it.
			if _, err := fillApplied(batch, r.cfg.Applier, applied, r.cfg.DedupeWindow); err != nil {
				return AdvanceTo{}, err
			}
			return AdvanceTo{Applied: rec.Pos, WALCursor: cursor, Ordinal: applied.Ordinal}, nil
		},
		// Unsynced unless the batch creates the pair's record (BUG-0809): the record is durable in the log
		// and a replay re-applies it once the pair has a record.
		Commit: func(batch storage.KVBatch, sync bool) error {
			if err := batch.Commit(sync); err != nil {
				return fmt.Errorf("committing the batch of position %s: %w", rec.Pos, err)
			}
			return nil
		},
	}
	err := r.cfg.Advancer.AdvanceLocked(p, in.key.slot, in.key.db, step)
	switch {
	case err == nil:
		return applyOutcome{verdict: verdictApplied, applied: rec.Pos}, nil
	case !appended:
		if errors.Is(err, ErrPairNotReady) || errors.Is(err, ErrPairBusy) || errors.Is(err, errNothingStaged) {
			// The pair takes no record now (not ready here, or a reconcile pass holds it):
			// the owner holds the frame and ships it again.
			return applyOutcome{verdict: verdictNotApplied, applied: p.AppliedPos}, nil
		}
		return applyOutcome{verdict: verdictNotApplied, applied: p.AppliedPos}, err
	}
	// The record is durable and stays so: the pair stops applying until its replay
	// re-applies it, exactly as on the owner.
	p.Flags |= flagReplayNeeded
	p.Flags &^= flagReplayed
	return applyOutcome{verdict: verdictNotApplied, applied: p.AppliedPos}, err
}

// applyChunk applies a chunk's records in order and stops at the first one that is not
// applied or already held. It reports whether every record was. A copy's catch-up (asCopy)
// stops, with nothing more applied and no reset, once this node owns the pair (BUG-0816).
func (r *Replicator) applyChunk(ctx context.Context, source string, req CatchUpRequest, chunk CatchUpChunk, asCopy bool) (bool, error) {
	key := pairKey{req.Slot, req.DB}
	committed := r.cfg.Committed()
	for _, rec := range chunk.Records {
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("catch-up of slot %d stopped before position %s: %w", req.Slot, rec.Pos, err)
		}
		if asCopy && r.owns(key) {
			return false, nil
		}
		out, err := r.applyRecord(source, committed, rec, key)
		if err != nil {
			return false, fmt.Errorf("applying position %s of a catch-up of slot %d: %w", rec.Pos, req.Slot, err)
		}
		switch out.verdict {
		case verdictApplied, verdictDuplicate:
		case verdictReset:
			if asCopy && r.owns(key) {
				return false, nil
			}
			if err := r.reset(key, ReseedDivergent); err != nil {
				return false, err
			}
			return false, ErrResetAndSeed
		default:
			return false, nil
		}
	}
	return true, nil
}
