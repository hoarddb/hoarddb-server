package store

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// ErrApplyOutOfOrder is a live apply at or below its bucket's applied_seq. It
// means applies left sequence order, and it is an error rather than a skip:
// a silent skip on the live path would turn every future ordering bug into
// silent loss (build doc §1, trap 4).
var ErrApplyOutOfOrder = errors.New("apply out of sequence order")

// BucketState is a bucket's one state document, at [s][id][0x05]
// (shared-storage spec §3.4, §4.4): the queue cursors, the persisted entry
// count, the highest WAL sequence whose effects are in the bucket's range,
// and the bucket's logical size (§11 Q22). LogicalBytes is the bytes of the
// bucket's data: len(key)+len(value) of every 0x04, 'e' and 'x' key, and not
// of the bookkeeping kinds 'h', 'd' and 'c' (claim-ack-spec.md §3.3).
type BucketState struct {
	Head         uint64 `bson:"head"`
	Tail         uint64 `bson:"tail"`
	Next         uint64 `bson:"next"`
	Entries      int64  `bson:"entries"`
	AppliedSeq   uint64 `bson:"applied_seq"`
	LogicalBytes int64  `bson:"logical_bytes"`
	// FailedSeqs is spec §4.4 rule 5's record: the sequences at or below
	// AppliedSeq whose store apply failed and which replay still owes. It is
	// sorted, holds no duplicate, and is absent from the document when empty,
	// so a bucket with no failure pays nothing for it.
	FailedSeqs []uint64 `bson:"failed_seqs,omitempty"`
	// LargeCommits records the sequences of the applies that committed a
	// batch larger than ApplyInfo.NoteCommitAbove bytes or
	// NoteCommitAboveEntries keys, which the smallest memtable a member may
	// run might not commit whole. It is written in the
	// batch that commits the effect, so a crash never separates the two, and
	// an owner re-sending such an entry from its log reads here that it
	// applied it. Sorted, no duplicates, pruned below the log's first sequence, and
	// absent when empty: a bucket without a large batch pays nothing for it.
	LargeCommits []uint64 `bson:"large_commits,omitempty"`
	// The lineage (replication spec §20.8.1): Term is the membership epoch at
	// which the owner this copy follows opened the bucket — a bucket's first
	// term is the epoch its create was logged under — and Through is the
	// origin's sequence of the last entry of that term applied here. BaseTerm
	// and BaseThrough are what the term was opened from. Opened says this node
	// is the term's origin. Unlike AppliedSeq, which is this node's own log
	// sequence, Through is the origin's, and so is the same number on every
	// copy that holds the same entries. All five are absent while zero, so a
	// document written before them decodes as it did.
	Term        uint64 `bson:"term,omitempty"`
	Through     uint64 `bson:"through,omitempty"`
	BaseTerm    uint64 `bson:"base_term,omitempty"`
	BaseThrough uint64 `bson:"base_through,omitempty"`
	Opened      bool   `bson:"opened,omitempty"`
	// Incarnation names which bucket of the name this copy is (replication
	// dispatch 10, R8-A): a random number minted once per create — by the
	// owner before the append in a cluster, by the node itself on a node with
	// no cluster — and kept by every copy, a materialise and a replay alike.
	// Two copies are compared by their lineage only when their incarnations
	// are equal. 0 is a copy created before the field, and equals only 0.
	Incarnation uint64 `bson:"incarnation,omitempty"`
	// Refusals are the sequences this node, as the entry's origin, did not
	// apply for any reason but a store failure (FailedSeqs holds those): a
	// constraint's refusal, a handler's, a delete or update of a missing key
	// (replication dispatch 10, R8-E as corrected by N-1). An owner re-sending
	// such an entry from its log reads here that it refused it, and a replica
	// that refuses it too is not diverged. Sorted, no duplicates, pruned below
	// the log's first sequence like LargeCommits, and absent when empty.
	Refusals []Refusal `bson:"refusals,omitempty"`

	// Delivery state (claim-ack-spec.md §3.3). All ten are absent while zero,
	// so a bucket that never claims encodes exactly as it did before them
	// (TestIdleBucketStateIsByteIdentical). Each is a count or a cursor of keys
	// in the bucket's own range, which is what makes it travel with the range.

	// Leases is the number of outstanding leases ('h' keys).
	Leases int64 `bson:"leases,omitempty"`
	// Pending is the number of claimed entries ('e' keys). Entries never
	// counts them: the cursor-lost rule compares with Entries alone.
	Pending int64 `bson:"pending,omitempty"`
	// PendingBytes is the sum of len(key)+len(value) of the 'e' keys.
	PendingBytes int64 `bson:"pending_bytes,omitempty"`
	// Returned is the number of 'c' records.
	Returned int64 `bson:"returned,omitempty"`
	// Dead is the number of 'x' records.
	Dead int64 `bson:"dead,omitempty"`
	// DeadBytes is the sum of len(key)+len(value) of the 'x' keys.
	DeadBytes int64 `bson:"dead_bytes,omitempty"`
	// DeadSeq is a heap's next dead sequence.
	DeadSeq uint64 `bson:"dead_seq,omitempty"`
	// LeaseClock is the bucket's lease clock in ms; 0 exactly when Leases is 0.
	LeaseClock int64 `bson:"lease_clock,omitempty"`
	// ResumeAt and ResumeBelow are the remembered cursor (spec §4.6): while
	// ResumeBelow is above zero, that many live entries lie beyond the cursor
	// on the run's side of ResumeAt, and the cursor jumps to ResumeAt when the
	// last of them is taken. ResumeBelow == 0 implies ResumeAt == 0.
	ResumeAt    uint64 `bson:"resume_at,omitempty"`
	ResumeBelow int64  `bson:"resume_below,omitempty"`
}

// writtenBack records a live 0x04 key that now exists at position because an
// entry was put back: by a return, a release or a requeue (claim-ack-spec §4.6).
// The cursor moves onto it (fifo Head = min(Head, position), lifo Tail =
// max(Tail, position)), and a write-back that lands beyond the cursor arms the
// remembered cursor: ResumeAt keeps where the cursor was and ResumeBelow counts
// the live keys between them, so the take that empties that run puts the
// cursor straight back instead of walking the delete markers in between. While
// armed, a write-back on the run's side counts up.
func (st *BucketState) writtenBack(mode QueueMode, position uint64) {
	armed := st.ResumeBelow > 0
	if mode == QueueLIFO {
		before := st.Tail
		st.Tail = max(st.Tail, position)
		switch {
		case armed && position > st.ResumeAt:
			st.ResumeBelow++
		case !armed && position > before:
			st.ResumeAt, st.ResumeBelow = before, 1
		}
		return
	}
	before := st.Head
	st.Head = min(st.Head, position)
	switch {
	case armed && position < st.ResumeAt:
		st.ResumeBelow++
	case !armed && position < before:
		st.ResumeAt, st.ResumeBelow = before, 1
	}
}

// pushedLifo is called for a lifo push at position, before the push moves Tail
// (BUG-0580). A push that lands more than one above Tail while entries are live
// leaves delete markers between them, which a read from the raised Tail would
// walk; it arms the cursor at the pre-push Tail. While armed, every push counts
// up, because a lifo push lands above ResumeAt, inside the counted run, and is
// taken before the run's last entry: a push left out of the count would let it
// reach zero with entries still live above, and the jump back to ResumeAt would
// strand them with every counter still agreeing. A fifo push lands at Next,
// outside the counted range, and never counts; with nothing live there is
// nothing beneath to protect, so nothing arms.
func (st *BucketState) pushedLifo(position uint64) {
	switch {
	case st.ResumeBelow > 0:
		st.ResumeBelow++
	case st.Entries > 0 && position > st.Tail+1:
		st.ResumeAt, st.ResumeBelow = st.Tail, 1
	}
}

// tookLive records a live 0x04 key at position leaving the queue: taken by a pop
// or a claim, or deleted. A key on the run's side of ResumeAt counts down, and
// the last one puts the cursor back at ResumeAt and clears the cursor. It
// reports that jump, so a walk that has more to take can seek again.
func (st *BucketState) tookLive(mode QueueMode, position uint64) (jumped bool) {
	if st.ResumeBelow == 0 {
		return false
	}
	onTheRun := position < st.ResumeAt
	if mode == QueueLIFO {
		onTheRun = position > st.ResumeAt
	}
	if !onTheRun {
		return false
	}
	st.ResumeBelow--
	if st.ResumeBelow > 0 {
		return false
	}
	if mode == QueueLIFO {
		st.Tail = min(st.Tail, st.ResumeAt)
	} else {
		st.Head = max(st.Head, st.ResumeAt)
	}
	st.ResumeAt = 0
	return true
}

// Refusal is one sequence the origin refused, and whether a constraint every
// copy enforces alike refused it.
type Refusal struct {
	Seq        uint64 `bson:"s"`
	Constraint bool   `bson:"c,omitempty"`
}

// RefusalAt reports whether the origin refused seq, and whether a constraint
// did: a binary search of Refusals.
func (st BucketState) RefusalAt(seq uint64) (refused, constraint bool) {
	at, found := slices.BinarySearchFunc(st.Refusals, seq, func(r Refusal, target uint64) int { return cmp.Compare(r.Seq, target) })
	if !found {
		return false, false
	}
	return true, st.Refusals[at].Constraint
}

// Refuse records that the origin refused seq, keeping Refusals sorted, and
// drops every refusal below floor, the log's first sequence: an entry the log
// no longer holds is never re-sent, so nothing asks about it (Q-13).
func (st *BucketState) Refuse(seq uint64, constraint bool, floor uint64) {
	at, found := slices.BinarySearchFunc(st.Refusals, seq, func(r Refusal, target uint64) int { return cmp.Compare(r.Seq, target) })
	if !found {
		st.Refusals = slices.Insert(st.Refusals, at, Refusal{Seq: seq, Constraint: constraint})
	}
	st.Refusals = slices.DeleteFunc(st.Refusals, func(r Refusal) bool { return r.Seq < floor })
	if len(st.Refusals) == 0 {
		st.Refusals = nil
	}
}

// CommittedLarge reports whether the apply at seq committed a batch the
// smallest memtable might not commit whole.
func (st BucketState) CommittedLarge(seq uint64) bool {
	for _, committed := range st.LargeCommits {
		if committed == seq {
			return true
		}
	}
	return false
}

// Owe records the carried failures (spec §4.4 rule 5) that lie above the
// mark being replaced. A carried sequence at or below it is an ambiguous
// commit that did take effect — its own batch set the mark — and recording it
// would make replay apply it a second time. Call Owe before Advance.
func (st *BucketState) Owe(pending []uint64) {
	for _, seq := range pending {
		if seq > st.AppliedSeq {
			st.FailedSeqs = insertSorted(st.FailedSeqs, seq)
		}
	}
}

// Advance moves the mark to seq if seq is above it — a max, never an
// assignment, because replay applies recorded failures beneath the mark and
// an assignment there would move it backwards — and settles seq if it was a
// recorded failure.
func (st *BucketState) Advance(seq uint64) {
	if seq > st.AppliedSeq {
		st.AppliedSeq = seq
	}
	st.settle(seq)
}

// Follow records an applied entry in the copy's lineage (replication spec
// §20.8.1): the origin's sequence for it, when it is past the copy's
// position, and that this node originated it. Call it beside Advance, in the
// batch that carries the entry's effect.
func (st *BucketState) Follow(originSeq uint64, originated bool) {
	if originSeq > st.Through {
		st.Through = originSeq
	}
	if originated {
		st.Opened = true
	}
}

// settle removes seq from FailedSeqs.
func (st *BucketState) settle(seq uint64) {
	for i, failed := range st.FailedSeqs {
		if failed == seq {
			st.FailedSeqs = append(st.FailedSeqs[:i], st.FailedSeqs[i+1:]...)
			break
		}
	}
	if len(st.FailedSeqs) == 0 {
		st.FailedSeqs = nil
	}
}

// failed reports whether seq is a recorded failure.
func (st BucketState) failed(seq uint64) bool {
	for _, failed := range st.FailedSeqs {
		if failed == seq {
			return true
		}
	}
	return false
}

// insertSorted adds seq to an ascending, duplicate-free list.
func insertSorted(seqs []uint64, seq uint64) []uint64 {
	at := 0
	for at < len(seqs) && seqs[at] < seq {
		at++
	}
	if at < len(seqs) && seqs[at] == seq {
		return seqs
	}
	seqs = append(seqs, 0)
	copy(seqs[at+1:], seqs[at:])
	seqs[at] = seq
	return seqs
}

// LoadBucketState reads the bucket's state document; an absent one is the
// zero value. The slot is the bucket's recorded slot.
func LoadBucketState(snap storage.KVSnapshot, slot uint16, databaseID, id uint64) (BucketState, error) {
	encoded, err := snap.Get(storage.Key(slot, databaseID, id, storage.NSState))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return BucketState{}, nil
	}
	if err != nil {
		return BucketState{}, fmt.Errorf("reading the state of bucket %d: %w", id, err)
	}
	var st BucketState
	if err := bson.Unmarshal(encoded, &st); err != nil {
		return BucketState{}, fmt.Errorf("decoding the state of bucket %d: %w", id, err)
	}
	return st, nil
}

// Put writes the state document into b, which must be the batch that carries
// the write's effect: the effect and its applied_seq are never separated by a
// crash (§4.4 rule 1).
func (st BucketState) Put(b storage.KVBatch, slot uint16, databaseID, id uint64) error {
	encoded, err := bson.Marshal(st)
	if err != nil {
		return fmt.Errorf("encoding the state of bucket %d: %w", id, err)
	}
	if err := b.Set(storage.Key(slot, databaseID, id, storage.NSState), encoded); err != nil {
		return fmt.Errorf("writing the state of bucket %d: %w", id, err)
	}
	return nil
}

// Gate is §4.4 rule 2 plus the build doc's §2.1. Sequence 0 — a node with no
// WAL — always applies; a sequence above applied_seq applies; at or below it,
// a replayed entry applies only if it is a recorded failure (rule 5) and is
// otherwise skipped, and a live one is ErrApplyOutOfOrder, recorded or not,
// naming both sequences. The caller adds the bucket's name.
func Gate(st BucketState, seq uint64, replay bool) (bool, error) {
	if seq == 0 || seq > st.AppliedSeq {
		return true, nil
	}
	if replay {
		return st.failed(seq), nil
	}
	return false, fmt.Errorf("%w: sequence %d is at or below the bucket's applied sequence %d", ErrApplyOutOfOrder, seq, st.AppliedSeq)
}
