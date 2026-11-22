package store

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The engine-independent apply (shared-storage spec §4.4, build doc Step 8
// item 10). Every mutation of a bucket on the shared store goes through Apply:
// it loads the state document, gates the entry, runs the engine's operation
// into one batch together with the state document, and commits. It is also
// where rule 5's failed-apply record lives, so no engine can forget it.

// ErrApplyFailed marks an apply the store itself did not carry out — a read of
// the state or the range failed, or a batch operation or its commit did. It is
// carried and recorded (spec §4.4 rule 5). A refusal by the operation's own
// logic never carries it.
var ErrApplyFailed = errors.New("the store failed to apply the write")

// ErrNotReplayedOutOfOrder is SA-27's loud path: a recorded failure that
// replay found beneath the mark and could not apply without putting an older
// effect over a newer one. It was removed from the record; the caller logs the
// ERROR and counts it.
var ErrNotReplayedOutOfOrder = errors.New("a recorded failed write was not replayed: applying it now would not match its replicas")

// The receiver's rule in the store (replication spec §20.8.3, R-7; build doc
// step 10b.2). A replica's live apply of an entry of its owner's stream is
// judged against the copy's lineage in the batch that would carry its effect:
// an entry is applied only when it is of the term the copy follows and its
// origin sequence is above the copy's position. Each is meaningful only within
// one incarnation of the bucket: an entry for a copy of another incarnation is
// refused before it reaches the store (dispatch 10a's divergence).
var (
	// ErrTermAhead is an entry of a term above the copy's: the copy must reach
	// the new term's base first. Its local sequence is consumed, with the
	// caller's record of the stop (ApplyInfo.RecordStop), and nothing else is
	// written.
	ErrTermAhead = errors.New("the entry is of a term above the one this copy follows")
	// ErrTermBehind is an entry of a term below the copy's: a former owner's
	// entry that arrived after the copy moved on. It is consumed.
	ErrTermBehind = errors.New("the entry is of a term below the one this copy follows")
	// ErrAlreadyHeld is an entry of the copy's term at or below its position:
	// by the lineage the copy holds it. It is consumed.
	ErrAlreadyHeld = errors.New("the copy already holds this entry of its term")
)

// ApplyFaultHook, when set, runs immediately before an apply's batch commits,
// and a non-nil return fails the apply as the store failing. It is the one
// place a test can fail an apply inside the store (build doc §1 trap 4): a
// failure injected above the engine skips the carry, which is the code under
// test. kv lets an in-process multi-node test target one node. Nil in
// production. It is exported because the server's tests set it.
var ApplyFaultHook atomic.Pointer[func(kv storage.KV, slot uint16, id uint64, seq uint64) error]

// ApplyInfo is what the write path knows about the entry being applied.
type ApplyInfo struct {
	// Seq is this node's own WAL sequence for the entry, 0 without a WAL.
	Seq uint64
	// Replay is WAL replay: an entry at or below the mark is skipped unless
	// it is a recorded failure.
	Replay bool
	// Record is set where a store failure must be recorded: the owner's live
	// apply and replay, never a replica's live apply (§1 trap 4 (d)).
	Record bool
	// LogFloor returns the first sequence the WAL still holds; a recorded
	// failure below it can never be replayed. Nil without a WAL.
	LogFloor func() (uint64, error)
	// Commutes reports whether applying this entry beneath appliedSeq gives
	// the result its replicas have (SA-27). Replay sets it; nil is false.
	Commutes func(appliedSeq uint64) bool
	// NoteCommitAbove, when above zero, records in the bucket's LargeCommits
	// an apply whose batch writes more than this many bytes of keys and
	// values. Set where the owner applies its own entries (Record).
	NoteCommitAbove int64
	// NoteCommitAboveEntries, when above zero, records in LargeCommits an
	// apply whose batch writes more than this many keys, the state document
	// included. A store bounds a batch by its keys as well as its bytes, so a
	// batch of many small documents can be too large for a smaller member at
	// a size NoteCommitAbove passes. Set alongside it.
	NoteCommitAboveEntries int64
	// OriginSeq is the sequence the entry's origin gave it — this node's own
	// on the owner, the owner's on a replica — which moves the bucket's
	// lineage position (BucketState.Through). 0 leaves it where it is: an
	// entry logged before origins were recorded, or no log at all.
	OriginSeq uint64
	// Origin is set when this node originated the entry, as the bucket's
	// owner: an apply then marks the copy opened.
	Origin bool
	// Term is the term the entry ships in (ReplEntry.Term), read only with
	// CheckLineage.
	Term uint64
	// CheckLineage judges the entry against the copy's lineage before its
	// effect (the receiver's rule): set by a replica's live apply of an entry
	// of its owner's stream, and by replay of such an entry from the term its
	// log record carries (BUG-0588), never by the owner's apply. Only an
	// entry above the mark is judged. CheckPosition also judges OriginSeq
	// against the copy's position; it is off for a source whose log went
	// backwards, whose sequences repeat ones the position counts.
	CheckLineage  bool
	CheckPosition bool
	// OwnerCommitted is a replica's live apply of an entry its owner reports
	// it committed. A batch too large for this node's store is then this
	// member's smaller memtable, not the operation's answer, and is
	// not consumed: the entry stays above the mark for the resized member's
	// replay.
	OwnerCommitted bool
	// OwnerDecided is the owner's verdict on the entry, for a replica's live
	// apply and a replay of an entry applied from another node's stream: an
	// insert follows it instead of deciding against this copy (spec §6.5).
	// OwnerUndecided everywhere else.
	OwnerDecided OwnerDecision
	// NoteHeldID, when set, is called by an insert the owner applied over a
	// key this copy already held: the copies agree once it is put, and the
	// caller logs and counts that this copy had an _id_ its owner did not.
	NoteHeldID func()
	// RecordStop, when set, is asked about a refusal a replica's live apply
	// consumes: an entry of a later term, or a refusal of the operation. When
	// the refusal stops the entry's stream, it adds the caller's record of the
	// stop to the state-only batch that consumes the entry and reports true,
	// and the batch is then synced. The consumption is then never durable
	// without the record: a crash between the two left the entry counted as
	// applied, and its source's later entries replayed past it (BUG-0571).
	RecordStop func(b storage.KVBatch, st BucketState, refusal error) (bool, error)
}

// OwedApplies is a bucket's in-memory carry: the sequences whose store apply
// failed and which the bucket's next mark-advancing batch records. The caller
// owns it and hands it to each engine it builds for the bucket, so it outlives
// any one engine; it is guarded by the bucket mutex every mutation already
// holds.
type OwedApplies struct {
	pending []uint64
	// unreplayable holds the recorded sequences the last batch dropped
	// because the log no longer holds them, for the caller's ERROR line.
	unreplayable []uint64
	// recorded is the size of the bucket's FailedSeqs as last written.
	recorded int
	// largeCommits is the size of the bucket's LargeCommits as last written,
	// for the server's once-per-start warning when the list grows (BUG-0291).
	largeCommits int
}

// Unreplayable returns the recorded failures the last apply dropped because
// they are below the log's first sequence (rule 5 prune (d)).
func (o *OwedApplies) Unreplayable() []uint64 { return o.unreplayable }

// Recorded is the number of failures the bucket's state document holds, as
// its last write left it.
func (o *OwedApplies) Recorded() int { return o.recorded }

// LargeCommits is the number of large commits the bucket's state document
// holds, as its last write left it. The list is rewritten by every write of
// the bucket, so a long one is a cost every write pays while a peer holds the
// log back (BUG-0291).
func (o *OwedApplies) LargeCommits() int { return o.largeCommits }

// Owes reports whether the bucket owes any apply: one carried in memory for
// its next batch, or one its state document records. O(1).
func (o *OwedApplies) Owes() bool { return len(o.pending) > 0 || o.recorded > 0 }

// ApplyRequest is one mutation of one bucket. It is a struct because it has
// eight fields.
type ApplyRequest struct {
	KV         storage.KV
	Slot       uint16
	DatabaseID uint64
	ID         uint64
	// Keyed is true for a key-addressed engine (hash, btree, blob), where a
	// recorded failure can be applied beneath the mark when it commutes.
	// Queue and heap operations are never applied there (spec §4.4, SA-27).
	Keyed bool
	Info  ApplyInfo
	Owed  *OwedApplies
	// Op adds the operation's effect to the batch and updates st (entries,
	// cursors, logical size). Its own errors are refusals.
	Op func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error
}

// storeFailure marks an error as the store failing rather than the operation
// refusing.
type storeFailure struct{ err error }

func (f *storeFailure) Error() string { return f.err.Error() }
func (f *storeFailure) Unwrap() error { return f.err }

// asStoreFailure marks err as a store failure, except the seam's batch-too-big
// error, which is the operation's answer: every node applying the entries in
// order gives the same one.
func asStoreFailure(err error) error {
	if err == nil || errors.Is(err, storage.ErrBatchTooLarge) {
		return err
	}
	return &storeFailure{err: err}
}

func isStoreFailure(err error) bool {
	var failure *storeFailure
	return errors.As(err, &failure)
}

// Apply runs one mutation. It returns (false, nil) for a replayed entry the
// gate skips, (true, nil) once the effect and the state document have
// committed together, store.ErrApplyOutOfOrder unchanged, ErrApplyFailed for a
// store failure (carried when Info.Record), ErrNotReplayedOutOfOrder for a
// recorded failure that does not commute, and the operation's refusal as it
// is.
func Apply(req ApplyRequest) (bool, error) {
	applyInfo := req.Info
	req.Owed.unreplayable = nil
	snap := req.KV.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, req.Slot, req.DatabaseID, req.ID)
	if err != nil {
		return false, req.carry(fmt.Errorf("loading the state of bucket %d: %w", req.ID, err))
	}
	admitted, err := Gate(st, applyInfo.Seq, applyInfo.Replay)
	if err != nil || !admitted {
		return false, err
	}
	beneath := applyInfo.Seq != 0 && applyInfo.Seq <= st.AppliedSeq
	// Beneath the mark Gate admits only replay's recorded failure, which never
	// applied: later entries may have moved the position past its origin
	// sequence, and judged by it the failed write would be passed over as
	// held and lost. Its first apply judged it and let it through.
	if applyInfo.CheckLineage && !beneath {
		if err := req.judgeLineage(st); err != nil {
			return false, err
		}
	}
	if beneath && (!req.Keyed || applyInfo.Commutes == nil || !applyInfo.Commutes(st.AppliedSeq)) {
		if err := req.commitState(st, applyInfo.Seq, false); err != nil {
			return false, req.carry(err)
		}
		return false, fmt.Errorf("%w: bucket %d, sequence %d", ErrNotReplayedOutOfOrder, req.ID, applyInfo.Seq)
	}

	// A refusal may leave st half-updated by the operation; the state-only
	// batch it can need is written from the state as loaded. The slices are
	// copied because settleCarry filters st's in place before the commit, and
	// a refusal at the commit writes loaded after it.
	loaded := st
	loaded.FailedSeqs = append([]uint64(nil), st.FailedSeqs...)
	loaded.LargeCommits = append([]uint64(nil), st.LargeCommits...)
	batch := req.KV.NewBatch()
	defer batch.Discard()
	var written batchWritten
	if err := req.Op(failureSnapshot{snap}, &st, failureBatch{KVBatch: batch, written: &written}); err != nil {
		if isStoreFailure(err) {
			return false, req.carry(err)
		}
		return false, req.consume(loaded, beneath, err)
	}
	if err := req.settleCarry(&st); err != nil {
		return false, req.carry(err)
	}
	if applyInfo.Seq != 0 && written.exceeds(applyInfo) {
		st.LargeCommits = insertSorted(st.LargeCommits, applyInfo.Seq)
	}
	st.Advance(applyInfo.Seq)
	st.Follow(applyInfo.OriginSeq, applyInfo.Origin)
	if err := req.commit(st, batch, false); err != nil {
		if errors.Is(err, storage.ErrBatchTooLarge) {
			// The state document is what crossed the bound: still the
			// operation's answer.
			return false, req.consume(loaded, beneath, err)
		}
		return false, req.carry(err)
	}
	return true, nil
}

// SettleNoEffect consumes the sequence of a replayed entry that, run again,
// has nothing to write and could not have changed whether a document exists:
// an update of one document that is gone. Such a handler returns before Apply,
// so a recorded failure of the entry would stay in failed_seqs for good, ending
// in a false "lost write" ERROR. It acts only on a replayed entry that is a
// recorded failure, with the state-only batch a refusal beneath the mark uses
// (commitState): the mark moves and the sequence is removed. req needs KV,
// Slot, ID, Info and Owed; Op is unused. A store failure is carried as Apply
// carries one.
func SettleNoEffect(req ApplyRequest) error {
	return req.settleNoEffect(settleQuietly)
}

// RefuseNoEffectBeneathMark is SettleNoEffect for a replayed entry that
// matches by a filter, a multi-document update: it matched on the replicas
// that applied it, and the writes after it may be why it matches nothing
// here. Beneath the mark it is refused as Apply refuses a non-commuting
// replay (SA-27): settled, and ErrNotReplayedOutOfOrder returned for the
// caller's loud path. At or above the mark nothing later has applied, so it
// settles quietly.
func RefuseNoEffectBeneathMark(req ApplyRequest) error {
	return req.settleNoEffect(refuseBeneathMark)
}

// RefuseRecordedFailure is RefuseNoEffectBeneathMark for a replayed recorded
// failure that a handler refused before the store: it refuses at or above the
// mark too. The original apply passed the handler (or never judged the request
// at all, for a failed pre-write read), so the refusal at replay is judged
// against the current state, and the owner cannot tell a write a later one
// superseded from one its replicas applied (BUG-0586). Like Apply's refusal
// beneath the mark it settles the record and returns ErrNotReplayedOutOfOrder
// for the caller's loud path.
func RefuseRecordedFailure(req ApplyRequest) error {
	return req.settleNoEffect(refuseAlways)
}

// noEffectMode is what settleNoEffect answers once it has consumed a sequence.
type noEffectMode int

const (
	// settleQuietly returns nil.
	settleQuietly noEffectMode = iota
	// refuseBeneathMark returns ErrNotReplayedOutOfOrder only beneath the mark.
	refuseBeneathMark
	// refuseAlways returns ErrNotReplayedOutOfOrder wherever the mark is.
	refuseAlways
)

func (req ApplyRequest) settleNoEffect(mode noEffectMode) error {
	// As Apply clears it: the caller logs what an apply left unreplayable on
	// every call that follows, so a stale list would be logged again.
	req.Owed.unreplayable = nil
	if !req.Info.Replay || req.Info.Seq == 0 {
		return nil
	}
	snap := req.KV.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, req.Slot, req.DatabaseID, req.ID)
	if err != nil {
		return req.carry(fmt.Errorf("loading the state of bucket %d: %w", req.ID, err))
	}
	if !st.failed(req.Info.Seq) {
		return nil
	}
	beneath := req.Info.Seq <= st.AppliedSeq
	if err := req.commitState(st, req.Info.Seq, false); err != nil {
		return req.carry(err)
	}
	if mode == refuseAlways || (mode == refuseBeneathMark && beneath) {
		return fmt.Errorf("%w: bucket %d, sequence %d", ErrNotReplayedOutOfOrder, req.ID, req.Info.Seq)
	}
	return nil
}

// judgeLineage is the receiver's rule against the copy as loaded. An entry it
// does not apply has no effect on the copy, but its local sequence is
// consumed in a state-only batch, so a replay of this node's log passes over
// it. That batch is not synced but for a stop it records: a power cut can take
// it, and replay then meets the entry above the mark and judges it again from
// the term its log record carries, against the same lineage, since a term
// change is synced and moves the mark past every entry logged before it
// (BUG-0588). For an entry of a later term that is a refusal the sender
// re-sends, and the re-send is logged again once the copy has taken that term;
// for one the copy holds, or one of an earlier term, it is the entry's answer.
func (req ApplyRequest) judgeLineage(st BucketState) error {
	applyInfo := req.Info
	var skipped error
	switch {
	case applyInfo.Term > st.Term:
		skipped = fmt.Errorf("%w: bucket %d follows term %d, the entry is of term %d", ErrTermAhead, req.ID, st.Term, applyInfo.Term)
	case applyInfo.Term < st.Term:
		skipped = fmt.Errorf("%w: bucket %d follows term %d, the entry is of term %d", ErrTermBehind, req.ID, st.Term, applyInfo.Term)
	case applyInfo.CheckPosition && applyInfo.OriginSeq != 0 && applyInfo.OriginSeq <= st.Through:
		skipped = fmt.Errorf("%w: bucket %d is at %d of term %d, the entry at %d", ErrAlreadyHeld, req.ID, st.Through, st.Term, applyInfo.OriginSeq)
	default:
		return nil
	}
	if applyInfo.Seq != 0 {
		var stopped error
		if errors.Is(skipped, ErrTermAhead) {
			stopped = skipped
		}
		if err := req.commitStopped(st, applyInfo.Seq, false, stopped); err != nil {
			return req.carry(err)
		}
	}
	return skipped
}

// consume makes a refusal the entry's answer for good (spec §4.4 rule 5): a
// state-only batch moves the mark to its sequence, so replay skips the entry
// rather than answer it again against a later state or along a path that
// writes a different batch (replay stamps no read-repair version,
// so a batch the live apply found too many entries fits at replay). Rule 5
// prune (b) is the same batch for a recorded entry replay finds refused. A
// refusal that replay meets above the mark is left there, as is a replica's
// batch-too-large for an entry its owner committed (OwnerCommitted): neither
// is known to be the answer, and a resized member applies the
// latter from its own log. It returns the refusal, or the store failure that
// kept the state from committing.
func (req ApplyRequest) consume(loaded BucketState, beneath bool, refusal error) error {
	info := req.Info
	tooLarge := errors.Is(refusal, storage.ErrBatchTooLarge)
	live := !info.Replay && info.Seq != 0 && (!tooLarge || !info.OwnerCommitted)
	if !beneath && !live {
		return refusal
	}
	// settleCarry runs again on loaded; what a first run at the commit
	// found unreplayable would otherwise be reported twice.
	req.Owed.unreplayable = nil
	// Only a batch too large answers differently at replay, so only its
	// mark must outlive a power loss that keeps the log entry.
	var stopped error
	if live {
		stopped = refusal
	}
	if err := req.commitStopped(loaded, info.Seq, tooLarge, stopped); err != nil {
		return req.carry(err)
	}
	return refusal
}

// commitState writes the state alone — the carry folded in, the mark moved to
// seq if it is above it, and seq removed from the record — for an entry that
// was consumed without an effect.
func (req ApplyRequest) commitState(st BucketState, seq uint64, sync bool) error {
	return req.commitStopped(st, seq, sync, nil)
}

// commitStopped is commitState for a replica's live apply that consumes the
// entry for refusal: Info.RecordStop may add its record of the stop to the
// batch, which is then synced.
func (req ApplyRequest) commitStopped(st BucketState, seq uint64, sync bool, refusal error) error {
	if err := req.settleCarry(&st); err != nil {
		return err
	}
	st.Advance(seq)
	batch := req.KV.NewBatch()
	defer batch.Discard()
	if refusal != nil && req.Info.RecordStop != nil {
		recorded, err := req.Info.RecordStop(batch, st, refusal)
		if err != nil {
			return fmt.Errorf("recording the stop at sequence %d of bucket %d: %w", seq, req.ID, err)
		}
		sync = sync || recorded
	}
	return req.commit(st, batch, sync)
}

// settleCarry folds the carry into st and drops recorded sequences the log no
// longer holds. It runs in every batch that writes the state document.
func (req ApplyRequest) settleCarry(st *BucketState) error {
	st.Owe(req.Owed.pending)
	if (len(st.FailedSeqs) == 0 && len(st.LargeCommits) == 0) || req.Info.LogFloor == nil {
		return nil
	}
	floor, err := req.Info.LogFloor()
	if err != nil {
		return fmt.Errorf("reading the log's first sequence: %w", err)
	}
	// An entry below the log's first sequence is never re-sent from it, so
	// nothing asks whether it committed.
	keptLarge := st.LargeCommits[:0]
	for _, seq := range st.LargeCommits {
		if seq >= floor {
			keptLarge = append(keptLarge, seq)
		}
	}
	st.LargeCommits = keptLarge
	if len(keptLarge) == 0 {
		st.LargeCommits = nil
	}
	if len(st.FailedSeqs) == 0 {
		return nil
	}
	kept := st.FailedSeqs[:0]
	for _, seq := range st.FailedSeqs {
		if seq < floor {
			req.Owed.unreplayable = append(req.Owed.unreplayable, seq)
			continue
		}
		kept = append(kept, seq)
	}
	st.FailedSeqs = kept
	if len(kept) == 0 {
		st.FailedSeqs = nil
	}
	return nil
}

// commit writes st into batch and commits it, synced when sync is set; on
// success the carry is recorded and cleared.
func (req ApplyRequest) commit(st BucketState, batch storage.KVBatch, sync bool) error {
	if err := st.Put(batch, req.Slot, req.DatabaseID, req.ID); err != nil {
		return asStoreFailure(err)
	}
	if hook := ApplyFaultHook.Load(); hook != nil {
		if err := (*hook)(req.KV, req.Slot, req.ID, req.Info.Seq); err != nil {
			return asStoreFailure(fmt.Errorf("committing the apply of sequence %d to bucket %d: %w", req.Info.Seq, req.ID, err))
		}
	}
	if err := batch.Commit(sync); err != nil {
		return asStoreFailure(fmt.Errorf("committing the apply of sequence %d to bucket %d: %w", req.Info.Seq, req.ID, err))
	}
	req.Owed.pending = nil
	req.Owed.recorded = len(st.FailedSeqs)
	req.Owed.largeCommits = len(st.LargeCommits)
	return nil
}

// carry records a store failure in the carry when recording is on, and marks
// the error ErrApplyFailed. A refusal passes through unmarked.
func (req ApplyRequest) carry(err error) error {
	if !isStoreFailure(err) {
		if errors.Is(err, storage.ErrBatchTooLarge) {
			return err
		}
		err = &storeFailure{err: err}
	}
	req.Owed.queueFailure(req.Info)
	return fmt.Errorf("%w: %w", ErrApplyFailed, err)
}

// CarryFailure queues the entry's sequence for the bucket's next batch, as
// Apply does for a store failure it meets itself. A handler that read the
// store before its write and failed never reaches Apply, and the entry is
// already logged and shipped, so without this the sequence is never recorded
// in failed_seqs and replay skips it once a later write has moved the mark.
// It queues only where failures are recorded (ApplyInfo.Record). It clears the
// list the last apply left unreplayable, as Apply does at its start: the
// caller logs that list on every call that follows.
func (o *OwedApplies) CarryFailure(info ApplyInfo) {
	o.unreplayable = nil
	o.queueFailure(info)
}

func (o *OwedApplies) queueFailure(info ApplyInfo) {
	if info.Record && info.Seq != 0 {
		o.pending = append(o.pending, info.Seq)
	}
}

// failureSnapshot marks every read error but a missing key as a store failure.
type failureSnapshot struct{ storage.KVSnapshot }

func (s failureSnapshot) Get(key []byte) ([]byte, error) {
	value, err := s.KVSnapshot.Get(key)
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return nil, asStoreFailure(err)
	}
	return value, err
}

func (s failureSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	return failureIterator{s.KVSnapshot.NewIterator(opts)}
}

// failureIterator marks a value read's error as a store failure.
type failureIterator struct{ storage.KVIterator }

func (it failureIterator) Value() ([]byte, error) {
	value, err := it.KVIterator.Value()
	return value, asStoreFailure(err)
}

// Err marks the error a walk ended on as a store failure.
func (it failureIterator) Err() error {
	return asStoreFailure(it.KVIterator.Err())
}

// batchWritten is what an operation wrote into its batch, for
// ApplyInfo.NoteCommitAbove and NoteCommitAboveEntries.
type batchWritten struct {
	bytes int64
	keys  int64
}

// exceeds reports whether the batch, with the state document Apply adds to
// it, is over either threshold info notes a large commit above.
func (w batchWritten) exceeds(info ApplyInfo) bool {
	overBytes := info.NoteCommitAbove > 0 && w.bytes > info.NoteCommitAbove
	overKeys := info.NoteCommitAboveEntries > 0 && w.keys+1 > info.NoteCommitAboveEntries
	return overBytes || overKeys
}

// failureBatch marks every batch operation's error as a store failure, except
// the batch being too big, which is a refusal.
// It also counts the keys, and the bytes of keys and values, the operation
// writes.
type failureBatch struct {
	storage.KVBatch
	written *batchWritten
}

func (b failureBatch) Set(key, value []byte) error {
	b.written.bytes += int64(len(key) + len(value))
	b.written.keys++
	return asStoreFailure(b.KVBatch.Set(key, value))
}

func (b failureBatch) Delete(key []byte) error {
	b.written.bytes += int64(len(key))
	b.written.keys++
	return asStoreFailure(b.KVBatch.Delete(key))
}

// ReclaimVerbHook, when set, runs at the entry of every verb-layer entry
// point that reads or deletes an engine's keys by its own cursor: heap push,
// pop, peek, DeleteAtPosition and DeleteAtID; the queue verbs; hash and btree
// DeleteAtPosition. The reclaimer deletes a dropped bucket's keys with the
// store's plain Delete and must reach none of these (spec §5.2.5); its test
// counts the calls (build doc Step 15, T-26(d)). It lives here, beside the
// engines, because the server package that the reclaimer is in cannot be
// reached from them. A verb added to any engine later must call it in the same
// change. Nil in production.
var ReclaimVerbHook atomic.Pointer[func(verb string)]

func callReclaimVerbHook(verb string) {
	if hook := ReclaimVerbHook.Load(); hook != nil {
		(*hook)(verb)
	}
}

// DocumentField is one declared field of an inserted document.
type DocumentField struct {
	// FieldID is the declaration's two-byte id; 0 and 1 are never field keys.
	FieldID uint16
	// Store marks a store field. An insert carries no elements, so it writes
	// no key for one: a store that was never written costs nothing.
	Store bool
	// Stored is a scalar's bsonType:1 ‖ BSON value bytes, as written.
	Stored []byte
}

// DocumentInsert is one new document: its bucket, its encoded _id_, its header
// value (bsonType:1 ‖ bytes) and its present fields.
type DocumentInsert struct {
	Ref    storage.BucketRef
	EncID  []byte
	Header []byte
	Fields []DocumentField
}

// firstScalarFieldID is the lowest id a scalar key carries: 0 marks the header
// in a write payload and 1 is _id_, whose only key is the header
// (placement §3).
const firstScalarFieldID = 2

// WriteInsert sets a new document's header and one key per present scalar into
// batch, and nothing for a store field. It never commits: the caller commits
// the one batch, so a failure part way leaves no key of the document.
func WriteInsert(batch storage.KVBatch, req DocumentInsert) error {
	if len(req.EncID) == 0 {
		return errors.New("inserting a document with no encoded _id_")
	}
	doc := DocumentPrefix(req.Ref.Slot, req.Ref.DatabaseID, req.Ref.BucketID, req.EncID)
	for _, field := range req.Fields {
		if field.FieldID < firstScalarFieldID {
			return fmt.Errorf("field id %d cannot be a key: 0 marks the header and 1 is _id_, which has only the header key", field.FieldID)
		}
	}
	if err := batch.Set(doc, req.Header); err != nil {
		return fmt.Errorf("writing the header of document %x: %w", req.EncID, err)
	}
	for _, field := range req.Fields {
		if field.Store {
			continue
		}
		if err := batch.Set(FieldKey(doc, field.FieldID), field.Stored); err != nil {
			return fmt.Errorf("writing field %d of document %x: %w", field.FieldID, req.EncID, err)
		}
	}
	return nil
}
