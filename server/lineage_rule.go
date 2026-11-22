package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The receiver's rule (replication spec §20.8.3, R-7; build doc step 10b.2).
// A bucket entry that arrives on a stream, other than a create or a drop, is
// judged against the copy it would change:
//
//	the copy is marked outside       refused, awaiting_handoff: it is never
//	                                 applied to again, but for an alter's
//	                                 placement effect
//	                                 (judgeAlterOnOutsideCopy)
//	the entry's term is above        refused, awaiting_handoff: the copy must
//	                                 reach the new term's base first
//	the entry's term is below        passed over and acknowledged, and counted
//	same term, at or below through   passed over and acknowledged (the store)
//	same term, above through         applied
//
// The first three are decided here, before the entry is logged, from the
// placement marks and the copy's term held in memory beside its id. The
// fourth needs the copy's position, which only its state document holds, so
// the store decides it in the batch that would carry the effect
// (store.ApplyInfo.CheckLineage). It is sound because the take-over opens a
// term at every change of owner (step 10b.5): a new owner never writes in its
// copy's old term from a sequence of its own log, which a position of the old
// owner's log would hide. Every comparison is within one incarnation:
// an entry for a copy of another incarnation never reaches this rule
// (dispatch 10a's divergence). Cost: one read of the marks and one atomic
// read of the term per entry; nothing per bucket.

// refusalAwaitingHandoff is a stream push refused because the copy it names
// has not taken the entry's term, or has left the bucket's set. Nothing at or
// past the entry is acknowledged; the sender retries.
const refusalAwaitingHandoff = "awaiting_handoff"

// refusalHandoffInProgress is a forwarded request refused by an owner that
// has not taken the bucket over (ErrHandoffInProgress); the coordinator
// answers its client with the owner's text. Nothing was stored.
const refusalHandoffInProgress = "handoff_in_progress"

// errAwaitingHandoff is a replica's refusal of an entry its copy cannot take
// yet: the copy is outside the bucket's set, or follows an earlier term.
var errAwaitingHandoff = errors.New("this node's copy of the bucket has not taken the entry's term")

// errEntryOfOlderTerm is an entry of a term below the copy's: a former owner's
// entry that arrived after the copy moved on. It is not applied and not
// logged, and the stream moves past it.
var errEntryOfOlderTerm = errors.New("the entry is of a term below the one this node's copy follows")

// awaitingHandoffError names the bucket an awaiting_handoff refusal is about.
type awaitingHandoffError struct {
	database, bucket string
	reason           string
}

func (e *awaitingHandoffError) Error() string {
	return fmt.Sprintf("bucket %q in database %q: %s; nothing at or past this entry is acknowledged until it has", e.bucket, e.database, e.reason)
}

func (e *awaitingHandoffError) Unwrap() error { return errAwaitingHandoff }

type entryTermCtxKey struct{}

// judgedEntry is what the store judges a replica's stream entry by: the term
// it ships in, and whether its origin sequence is comparable with the copy's
// position.
type judgedEntry struct {
	term          uint64
	checkPosition bool
}

// withEntryTerm marks a replica's live apply of a stream entry shipped in
// term, so the store judges it against the copy's lineage — its position too,
// unless checkPosition is false: a source whose log went backwards writes
// sequences the position already counts (recordedStream.clamped), and such a
// source's entries are judged by term and by the per-source position only, as
// before the lineage existed.
func withEntryTerm(ctx context.Context, term uint64, checkPosition bool) context.Context {
	return context.WithValue(ctx, entryTermCtxKey{}, judgedEntry{term: term, checkPosition: checkPosition})
}

// entryTermFrom is what withEntryTerm attached, and whether it did.
func entryTermFrom(ctx context.Context) (judgedEntry, bool) {
	judged, ok := ctx.Value(entryTermCtxKey{}).(judgedEntry)
	return judged, ok
}

// judgedByLineage reports whether the receiver's rule applies to op: every
// bucket entry but a create and a drop, which are not judged (spec §20.8.3).
func judgedByLineage(op transport.Opcode, bucket string) bool {
	return bucket != "" && op != transport.OpCreateBucket && op != transport.OpDeleteBucket
}

// refuseByLineage is the half of the receiver's rule decided before the
// entry is logged. The caller holds the bucket's fence and has found the
// bucket open (refuseUnservable).
func (c *Cluster) refuseByLineage(entry ReplEntry) error {
	op := transport.Opcode(entry.Op)
	if !judgedByLineage(op, entry.Bucket) {
		return nil
	}
	_, database := decodeFrameMeta(entry.Payload).scope(op)
	// An alter reaches a member that holds no copy, which has no lineage to
	// judge it by: a member the increase newly places, or one that never held
	// the bucket (bucket-replication-rev8-spec §5.3, the rows for a member that
	// holds nothing). Judged as a copy of term 0, it would be refused for ever.
	if op == transport.OpAlterBucket {
		if _, held := c.s.peekBucket(database, entry.Bucket); !held {
			return nil
		}
	}
	key := placementOf(database, entry.Bucket)
	if marks := c.placementMarksOf(key); marks.Outside != 0 {
		if op == transport.OpAlterBucket {
			return c.judgeAlterOnOutsideCopy(entry, database)
		}
		c.wakeTakeOver(database, entry.Bucket)
		return &awaitingHandoffError{database: database, bucket: entry.Bucket,
			reason: fmt.Sprintf("this node's copy left the bucket's replica set at epoch %d and is copied again before it takes any entry", marks.Outside)}
	}
	term, _, err := c.s.bucketLineage(database, entry.Bucket)
	if err != nil {
		return fmt.Errorf("reading the term of bucket %q in database %q to judge entry %d: %w", entry.Bucket, database, entry.Seq, err)
	}
	switch {
	case entry.Term > term:
		// The refusal names the bucket to this node's worker, which asks the
		// owner for the term (step 10b.6).
		c.wakeTakeOver(database, entry.Bucket)
		return &awaitingHandoffError{database: database, bucket: entry.Bucket,
			reason: fmt.Sprintf("this node's copy follows term %d and the entry is of term %d, which it has not taken yet", term, entry.Term)}
	case entry.Term < term:
		return fmt.Errorf("%w: bucket %q in database %q follows term %d, the entry (sequence %d) is of term %d",
			errEntryOfOlderTerm, entry.Bucket, database, term, entry.Seq, entry.Term)
	}
	return nil
}

// judgeAlterOnOutsideCopy is the term table's one exception (spec §5.3): an
// alter reaching a copy marked outside is applied for its placement effect,
// which keeps the mark and moves no position, when its term is not below the
// copy's. Refused, it would stall the owner's stream to this member, and the
// placement change it carries is one the member must learn. An alter of an
// older term is passed over, as every entry of an older term is: the copy
// follows a newer owner, whose factor it learns at its next follow (§11.3).
func (c *Cluster) judgeAlterOnOutsideCopy(entry ReplEntry, database string) error {
	term, _, err := c.s.bucketLineage(database, entry.Bucket)
	if err != nil {
		return fmt.Errorf("reading the term of bucket %q in database %q to judge its alter %d: %w", entry.Bucket, database, entry.Seq, err)
	}
	if entry.Term < term {
		return fmt.Errorf("%w: bucket %q in database %q follows term %d, its alter (sequence %d) is of term %d",
			errEntryOfOlderTerm, entry.Bucket, database, term, entry.Seq, entry.Term)
	}
	return nil
}

// lineageSkipped reports whether the store passed over a replica's entry by
// the receiver's rule: the copy holds it, or it is of an older term. The
// entry was consumed, so the stream moves past it.
func lineageSkipped(err error) bool {
	return errors.Is(err, store.ErrAlreadyHeld) || errors.Is(err, store.ErrTermBehind)
}

// passedOverByLineage answers a replica's entry the store judged by the
// receiver's rule. One it passed over moves the stream past it as an applied
// entry does: it is in this node's log, consumed, and the position is rebuilt
// from there. One of a later term is refused: the in-memory check before the
// log should have caught it, and the store consumed its local sequence, so the
// sender's re-send is logged again once the copy has taken the term.
func (c *Cluster) passedOverByLineage(sender string, entry ReplEntry, judged error, recordPosition bool) (ReplResponse, error) {
	if errors.Is(judged, store.ErrTermAhead) {
		_, database := decodeFrameMeta(entry.Payload).scope(transport.Opcode(entry.Op))
		return ReplResponse{}, &awaitingHandoffError{database: database, bucket: entry.Bucket, reason: judged.Error()}
	}
	if errors.Is(judged, store.ErrTermBehind) {
		c.entriesOfOlderTerm.Add(1)
	}
	if recordPosition {
		c.markApplied(sender, entry.Seq)
	}
	if _, err := c.settleSizeRefusal(sender, entry.Seq); err != nil {
		return ReplResponse{}, fmt.Errorf("settling %s's recorded size refusal past entry %d, passed over by the lineage rule: %w", sender, entry.Seq, err)
	}
	if err := c.settleDivergence(sender, entry.Seq); err != nil {
		return ReplResponse{}, fmt.Errorf("settling %s's recorded divergence past entry %d, passed over by the lineage rule: %w", sender, entry.Seq, err)
	}
	return ReplResponse{}, nil
}
