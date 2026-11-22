package store

import (
	"fmt"
	"strings"
)

// PutRequest is one document write to a hash or btree bucket (document-placement
// spec §1.3). It is a struct because the insert intent made it six values.
type PutRequest struct {
	// Key is the document's _id_: a string for a hash bucket, any encodable
	// key for a btree bucket.
	Key   interface{}
	Value interface{}
	// Version, when non-zero, is the write's read-repair version, stamped in
	// the same batch; zero leaves any earlier version in place.
	Version uint64
	// Below bounds a read repair: the document is written only if the key's
	// stored version is below it. 0 is no bound.
	Below uint64
	// RefuseExisting is the insert intent: a key the bucket already holds is
	// refused with *DocumentExistsError inside the apply, so the refusal is
	// ordered with every other write of the bucket.
	RefuseExisting bool
}

// PutBatchRequest is a bulk document write to a hash or btree bucket. Items
// are keyed by their "_id_".
type PutBatchRequest struct {
	Items   []map[string]interface{}
	Version uint64
	// RefuseExisting is PutRequest's: one held key refuses the whole batch, and
	// every position that names a held key is reported.
	RefuseExisting bool
}

// DocumentExistsError is an insert's refusal of a key the bucket already
// holds. It is returned from inside the apply before anything is added to the
// batch, so Apply consumes it as any refusal of the operation.
type DocumentExistsError struct {
	// Key is the refused key; for a batch, the first refused one. Nil when the
	// owner's verdict refused the write and the store did not read it.
	Key interface{}
	// Positions are the 1-based places in a batch whose key is held, in
	// order; nil for one document, or for a refusal the owner decided.
	Positions []int
}

func (e *DocumentExistsError) Error() string {
	if e.Key == nil {
		return "the document already exists: its owner refused the insert"
	}
	if len(e.Positions) == 0 {
		return fmt.Sprintf("document %v already exists", e.Key)
	}
	positions := make([]string, len(e.Positions))
	for index, position := range e.Positions {
		positions[index] = fmt.Sprint(position)
	}
	return fmt.Sprintf("document %v already exists (batch positions %s)", e.Key, strings.Join(positions, ", "))
}

// OwnerDecision is a replica's copy of its owner's verdict on an insert
// (document-placement build doc BD-4): a replica and a replay follow it
// instead of deciding again against their own copy.
type OwnerDecision uint8

const (
	// OwnerUndecided is no verdict to follow: the write's own node decides.
	// It is the owner's own apply, an entry the owner still owes to its
	// replay, and an entry whose outcome was not logged.
	OwnerUndecided OwnerDecision = iota
	// OwnerApplied is an insert the owner stored: the existence check is
	// skipped, so the copies agree afterwards.
	OwnerApplied
	// OwnerRefused is an insert the owner refused under a constraint: it is
	// refused here without a read, and consumed as the owner's was.
	OwnerRefused
)

// insertCheck is the existence check of one apply, from its request and the
// owner's verdict.
type insertCheck struct {
	refuse   bool
	decided  OwnerDecision
	noteHeld func()
}

func newInsertCheck(info ApplyInfo, refuseExisting bool) insertCheck {
	return insertCheck{refuse: refuseExisting, decided: info.OwnerDecided, noteHeld: info.NoteHeldID}
}

// refusedUnread reports an insert the owner refused: the replica refuses it
// without reading its key, because its own copy may hold what the owner's did
// not.
func (c insertCheck) refusedUnread() bool {
	return c.refuse && c.decided == OwnerRefused
}

// refuses reports whether an insert of a key the copy holds (or not) is
// refused. An insert the owner applied is put over a held key, and the held
// key is reported through NoteHeldID: this copy had an _id_ its owner did not.
func (c insertCheck) refuses(held bool) bool {
	if !c.refuse || !held {
		return false
	}
	if c.decided == OwnerApplied {
		// Nil on every path but a replica's apply and a replay of a
		// replica-origin entry (applyInfoFrom).
		if c.noteHeld != nil {
			c.noteHeld()
		}
		return false
	}
	return true
}
