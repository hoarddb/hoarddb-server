package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// readApplyFailure marks a handler's pre-write store read failure as the store
// failing, not the request being refused, without changing the message a
// client sees. The replica's push branch reads the mark from the apply outcome
// and leaves the entry unapplied for the owner to re-send, instead of
// recording a divergence (BUG-0583).
type readApplyFailure struct{ err error }

func (f readApplyFailure) Error() string { return f.err.Error() }
func (f readApplyFailure) Unwrap() error { return f.err }

// Is makes errors.Is(marked, store.ErrApplyFailed) true and lets every other
// target fall through to the wrapped error.
func (f readApplyFailure) Is(target error) bool { return target == store.ErrApplyFailed }

// markReadApplyFailure marks a store read a handler failed as an apply failure,
// as the write path's store marks its own. A nil error stays nil.
func markReadApplyFailure(err error) error {
	if err == nil {
		return nil
	}
	return readApplyFailure{err: err}
}

// recordReadApplyFailure records a handler's store read failure where a failed
// write is recorded. The read failed before store.Apply ran, so Apply never
// carried the sequence: the entry is logged and shipped, a later write moves
// the bucket's applied_seq past it, and replay would skip it for good, leaving
// the owner's copy without a write its replicas hold (BUG-0584). The sequence
// is carried on the bucket's engine, so the next apply writes it into
// failed_seqs and replay retries it. noteApplyOutcome then does the rest as for
// a failed Put: the outcome's storeFailure (a replica leaves the entry for its
// owner to re-send, BUG-0583), the owed registry that holds WAL retention
// until the retry (backed by the carry, so not a phantom), the carry table and
// the store-key log line. Only the owner's live apply and replay carry
// (ApplyInfo.Record).
func (s *Server) recordReadApplyFailure(ctx context.Context, write appliedWrite, readErr error) {
	write.b.owedApplies().CarryFailure(applyInfoFrom(ctx, s))
	s.noteApplyOutcome(ctx, write, readErr)
}

// settleReplayedNoEffect consumes the sequence of a replayed update that found
// nothing to write, so a failed read recorded in failed_seqs does not stay
// there for good and end in a false "lost write" ERROR (BUG-0584). Outside
// replay, and for a sequence that is no recorded failure, it does nothing.
//
// byFilter is a multi-document update: it matched on its replicas, and the
// writes after it may be why it matches nothing here, so beneath the mark it is
// refused as a non-commuting replay is (SA-27) and the loud path reports that
// this copy differs. An update of one document that is gone stays quiet: an
// update never changes whether a document exists, so owner and replica agree.
func (s *Server) settleReplayedNoEffect(ctx context.Context, write appliedWrite, byFilter bool) error {
	if !isReplay(ctx) {
		return nil
	}
	request := store.ApplyRequest{KV: write.b.kv, Slot: write.b.slot, DatabaseID: write.b.databaseID, ID: write.b.id.Load(),
		Info: applyInfoFrom(ctx, s), Owed: write.b.owedApplies()}
	settle := store.SettleNoEffect
	if byFilter {
		settle = store.RefuseNoEffectBeneathMark
	}
	settleErr := settle(request)
	s.noteApplyOutcome(ctx, write, settleErr)
	if settleErr != nil {
		return fmt.Errorf("settling the replayed update of bucket %q in database %q: %w", write.bucket, write.database, settleErr)
	}
	return nil
}

// settleReplayedRefusal consumes the sequence of a recorded failed write whose
// replay a handler refused before it reached the store, and reports it as
// SA-27 reports a recorded failure refused beneath the mark (BUG-0586). The
// handler's refusals (a type check, a guard, an operator on a field a later
// write changed) return before store.Apply, the only call that consumes a
// recorded sequence, so one would stay in failed_seqs until the log's floor
// passed it and a "can never be replayed" ERROR followed. It is done here once
// rather than in each handler, and it is never quiet: the owner cannot tell a
// write a later one superseded from one its replicas applied. Nothing happens
// for an entry whose handler reached the store (noteApplyOutcome ran: Apply
// settles or deliberately leaves its own refusals, and a store failure stays
// owed for the next retry), for a bucket that is gone, or for a sequence that
// is no recorded failure.
func (s *Server) settleReplayedRefusal(ctx context.Context, entry storage.WALEntry, outcome *applyOutcome) {
	if outcome.noted {
		return
	}
	database := decodeFrameMeta(entry.Data).Database
	b, unlock, err := s.lockBucket(database, entry.Bucket)
	if err != nil {
		// The bucket is gone or was never created here: no state document to
		// hold a recorded failure.
		return
	}
	defer unlock()
	write := appliedWrite{database: database, bucket: entry.Bucket, b: b, op: strings.ToUpper(transport.Opcode(entry.Op).String())}
	request := store.ApplyRequest{KV: b.kv, Slot: b.slot, DatabaseID: b.databaseID, ID: b.id.Load(),
		Info: applyInfoFrom(ctx, s), Owed: b.owedApplies()}
	// A store failure of the state write is carried as Apply carries one, and
	// the entry stays owed for the next restart; it does not stop replay.
	if settleErr := store.RefuseRecordedFailure(request); settleErr != nil {
		s.noteApplyOutcome(ctx, write, settleErr)
	}
}
