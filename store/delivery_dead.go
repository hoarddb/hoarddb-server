package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The dead verbs shared by the queue and the heap (claim-ack-spec §4.12): list,
// requeue, discard and the restore-only write. None runs the return step or
// takes a now, so a dead verb never changes a lease (design §5.2).

// ErrDeadRecordInvalid is a dead record RestoreDead refuses: a reason that is
// neither DeadReasonMaxDeliveries nor DeadReasonCollided, fewer than one
// delivery, or an entry that is not one this bucket's engine stores. Nothing is
// written. The server words it as its ErrDeadOption.
var ErrDeadRecordInvalid = errors.New("the dead record is not one this bucket can hold")

// ErrDeadSelector is a requeue or discard that names a dead entry and asks for
// all of them, names neither, or gives After without All. The server refuses them at the door; this is
// the store's twin, so a copy of the logged entry decides alike.
var ErrDeadSelector = errors.New("a dead selector names one dead entry or all of them")

// DeadRecord is one dead entry.
type DeadRecord struct {
	// DeadID is the entry's key suffix: its handle for requeue and discard,
	// and what DeadList pages by. RestoreDead ignores it and makes a fresh one.
	DeadID []byte
	// Data is the stored entry: a document for fifo and lifo, {_id_, priority,
	// value} for a heap.
	Data bson.Raw
	// Deliveries is the claims recorded when it died.
	Deliveries int32
	// DeadAt is the effective now, in ms, of the apply that dead-lettered it.
	DeadAt int64
	// Reason is DeadReasonMaxDeliveries or DeadReasonCollided.
	Reason string
}

// DeadSelector names the dead entries a requeue or a discard moves: one by its
// DeadID, or All, from the start of the dead list.
type DeadSelector struct {
	DeadID []byte
	All    bool
	// After, with All, starts the walk past that dead id (exclusive), the way
	// DeadList's after pages. A request considers at most MaxDeadMoveLimit
	// entries and a refused one stays at the head, so a caller steps past
	// refusals by sending the last refused id back (BUG-0637). Nil starts at
	// the head.
	After []byte
}

// DeadRefusalCause says why a dead entry was not moved.
type DeadRefusalCause string

// The causes a requeue or a discard leaves an entry where it is.
const (
	// DeadNotFound: no dead entry has the named id. It may have been requeued
	// or discarded already.
	DeadNotFound DeadRefusalCause = "not_found"
	// DeadOriginOccupied: a live heap element holds the key the entry would go
	// back to. Requeueing would overwrite it.
	DeadOriginOccupied DeadRefusalCause = "origin_occupied"
)

// DeadRefusal is one entry a dead verb left where it was.
type DeadRefusal struct {
	DeadID []byte
	Cause  DeadRefusalCause
	// EntryID and Priority name a heap element whose origin key is occupied.
	EntryID  string
	Priority int64
}

// DeadMove is what a requeue or a discard did.
type DeadMove struct {
	// Done is how many dead entries it moved.
	Done int
	// Left is how many dead entries the bucket holds after it, refused ones
	// included.
	Left    int64
	Refused []DeadRefusal
	// Wrote is false when nothing changed.
	Wrote bool
}

// deadList is DeadList for either engine: one snapshot, no apply, no lock. It
// lists from after (exclusive) in key order and answers the last id listed when
// more remain. A visitor that returns ErrStopShort ends the list before the
// entry it was shown, which stays for the next page, and stoppedShort is true;
// any other error aborts it. A nil next with stoppedShort false is the only
// "list complete": when the very first visit is refused, next is the caller's
// own cursor unchanged (nil on a first page), so stoppedShort is what tells a
// refusal from an end (coordinator decision 2026-10-18).
func deadList(snap storage.KVSnapshot, ref storage.BucketRef, limit int, after []byte, visit func(DeadRecord) error) (next []byte, stoppedShort bool, err error) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	defer snap.Discard()
	return deadWalk(snap, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, limit, after, visit)
}

// deadWalk is deadList over a snapshot the caller keeps: a dump lists the dead
// entries from the snapshot its documents came from.
func deadWalk(snap storage.KVSnapshot, ref storage.BucketRef, limit int, after []byte, visit func(DeadRecord) error) (next []byte, stoppedShort bool, err error) {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	prefix := kindKey(slot, databaseID, id, kindDead)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	if after == nil {
		it.Seek(nil)
	} else {
		afterKey := deadKey(slot, databaseID, id, after)
		it.Seek(afterKey)
		if it.Valid() && bytes.Equal(it.Key(), afterKey) {
			it.Next()
		}
	}
	last := after
	for listed := 0; it.Valid(); listed++ {
		if listed == limit {
			return last, false, nil
		}
		value, err := it.Value()
		if err != nil {
			return nil, false, fmt.Errorf("reading a dead entry: %w", err)
		}
		record, err := decodeDeadRecord(value)
		if err != nil {
			return nil, false, fmt.Errorf("%w: %w", ErrDeliveryInvariant, err)
		}
		suffix := append([]byte(nil), it.Key()[len(prefix):]...)
		listing := DeadRecord{DeadID: suffix, Data: record.Data, Deliveries: record.Deliveries, DeadAt: int64(record.DeadAt), Reason: record.Reason}
		if err := visit(listing); err != nil {
			if errors.Is(err, ErrStopShort) {
				return last, true, nil
			}
			return nil, false, err
		}
		last = suffix
		it.Next()
	}
	if err := it.Err(); err != nil {
		return nil, false, fmt.Errorf("walking the dead list: %w", err)
	}
	return nil, false, nil
}

// deadMove is Requeue or Discard for either engine.
func deadMove(engine deliveryEngine, info ApplyInfo, sel DeadSelector, requeue bool) (DeadMove, error) {
	if sel.All == (sel.DeadID != nil) || (sel.After != nil && !sel.All) {
		return DeadMove{}, ErrDeadSelector
	}
	var move DeadMove
	err := engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		move = DeadMove{}
		op := newDeliveryOp(engine, opScope{snap: snap, st: st, batch: batch})
		if err := op.moveDead(sel, requeue, &move); err != nil {
			return err
		}
		move.Left, move.Wrote = st.Dead, move.Done > 0
		return nil
	})
	if err != nil {
		return DeadMove{}, err
	}
	return move, nil
}

// moveDead walks the selected dead entries in key order, from After when All names one. For All it stops at the
// delivery budget and at MaxDeadMoveLimit entries considered, the first entry
// written being exempt (claim-ack-spec §4.4 Edge 3).
func (op *deliveryOp) moveDead(sel DeadSelector, requeue bool, move *DeadMove) error {
	if !sel.All {
		value, err := op.scope.snap.Get(deadKey(op.slot, op.databaseID, op.id, sel.DeadID))
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			move.Refused = append(move.Refused, DeadRefusal{DeadID: sel.DeadID, Cause: DeadNotFound})
			return nil
		case err != nil:
			return fmt.Errorf("reading dead entry %s: %w", deadIDString(sel.DeadID), err)
		}
		_, err = op.moveOneDead(sel.DeadID, value, requeue, move)
		return err
	}
	prefix := kindKey(op.slot, op.databaseID, op.id, kindDead)
	it := op.scope.snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	if sel.After == nil {
		it.Seek(nil)
	} else {
		afterKey := deadKey(op.slot, op.databaseID, op.id, sel.After)
		it.Seek(afterKey)
		if it.Valid() && bytes.Equal(it.Key(), afterKey) {
			it.Next()
		}
	}
	for considered := 1; it.Valid(); considered++ {
		value, err := it.Value()
		if err != nil {
			return fmt.Errorf("reading a dead entry: %w", err)
		}
		stopped, err := op.moveOneDead(append([]byte(nil), it.Key()[len(prefix):]...), value, requeue, move)
		if err != nil || stopped || considered == MaxDeadMoveLimit {
			return err
		}
		// Never step past the last entry considered: the step walks the
		// delete markers of dead entries moved since the last compaction.
		it.Next()
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the dead list: %w", err)
	}
	return nil
}

// moveOneDead requeues or discards one dead entry. It reports stopped when the
// delivery budget has no room for it, leaving it on the list.
func (op *deliveryOp) moveOneDead(suffix, value []byte, requeue bool, move *DeadMove) (stopped bool, err error) {
	deadKeyBytes := deadKey(op.slot, op.databaseID, op.id, suffix)
	if !requeue {
		return op.discardDead(deadKeyBytes, value, move)
	}
	record, err := decodeDeadRecord(value)
	if err != nil {
		return false, fmt.Errorf("%w: dead entry %s: %w", ErrDeliveryInvariant, deadIDString(suffix), err)
	}
	origin, entry, err := op.deadOrigin(suffix, record)
	if err != nil {
		return false, err
	}
	if op.layout == claimLayoutHeap {
		// A queue's origin is a position that is never reused, so only a heap
		// can find its key taken, by a twin pushed after the entry died.
		key := op.originKey(origin)
		occupied, err := op.occupied(key)
		if err != nil {
			return false, err
		}
		if occupied {
			move.Refused = append(move.Refused, DeadRefusal{
				DeadID: suffix, Cause: DeadOriginOccupied,
				EntryID: string(origin[8:]), Priority: priorityOf(binary.BigEndian.Uint64(origin[:8])),
			})
			return false, nil
		}
	}
	var cost deliveryCharge
	cost.add(originPrefixBytes+len(origin), len(entry))
	cost.add(len(deadKeyBytes), 0)
	if !op.spend.admit(cost) {
		return true, nil
	}
	return false, op.requeueDead(deadKeyBytes, value, origin, entry, move)
}

// deadOrigin is the 0x04 suffix a dead entry goes back to, and the bytes it had
// there: the suffix without the dead sequence, and for a heap the value inside
// the {_id_, priority, value} document.
func (op *deliveryOp) deadOrigin(suffix []byte, record deadRecord) (origin, entry []byte, err error) {
	if op.layout != claimLayoutHeap {
		return suffix, record.Data, nil
	}
	stored, err := heapStoredValue(record.Data)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: dead entry %s: %w", ErrDeliveryInvariant, deadIDString(suffix), err)
	}
	return suffix[:len(suffix)-8], stored, nil
}

// requeueDead puts a dead entry back at its origin by the write-back rule
// (claim-ack-spec §4.6) and removes its dead key. It writes no 'c' record: the
// entry's return count starts again (OQ-C).
func (op *deliveryOp) requeueDead(deadKeyBytes, value, origin, entry []byte, move *DeadMove) error {
	st := op.scope.st
	key := op.originKey(origin)
	if err := op.set(key, entry, "a requeued entry"); err != nil {
		return err
	}
	op.over.put(key, entry, 0)
	st.Entries++
	st.LogicalBytes += int64(len(key) + len(entry))
	op.engine.wroteBack(st, origin)
	if err := op.removeDeadKey(deadKeyBytes, value); err != nil {
		return err
	}
	move.Done++
	return nil
}

// discardDead deletes a dead entry's key, if the budget has room for it.
func (op *deliveryOp) discardDead(deadKeyBytes, value []byte, move *DeadMove) (stopped bool, err error) {
	var cost deliveryCharge
	cost.add(len(deadKeyBytes), 0)
	if !op.spend.admit(cost) {
		return true, nil
	}
	if err := op.removeDeadKey(deadKeyBytes, value); err != nil {
		return false, err
	}
	move.Done++
	return false, nil
}

// removeDeadKey deletes a dead key and takes it out of the counts.
func (op *deliveryOp) removeDeadKey(deadKeyBytes, value []byte) error {
	if err := op.delete(deadKeyBytes, "a dead entry"); err != nil {
		return err
	}
	st := op.scope.st
	size := int64(len(deadKeyBytes) + len(value))
	st.Dead--
	st.DeadBytes -= size
	st.LogicalBytes -= size
	return nil
}

// restoreDead is RestoreDead for either engine (claim-ack-spec §4.12, §7.3). It
// writes only an 'x' key, never a live 0x04 key, so it never touches a cursor,
// and its record is validated on every copy before anything is written.
func restoreDead(engine deliveryEngine, info ApplyInfo, record DeadRecord) error {
	return engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		op := newDeliveryOp(engine, opScope{snap: snap, st: st, batch: batch})
		suffix, err := op.restoredSuffix(record)
		if err != nil {
			return err
		}
		encoded, err := encodeDeadRecord(deadRecord{Data: record.Data, Deliveries: record.Deliveries, DeadAt: primitive.DateTime(record.DeadAt), Reason: record.Reason})
		if err != nil {
			return err
		}
		return op.writeDead(suffix, encoded)
	})
}

// restoredSuffix validates record and makes the dead suffix it will have: on
// fifo and lifo a fresh position, so a later requeue has a free origin, and on
// a heap the record's own priority and id with the next dead sequence.
func (op *deliveryOp) restoredSuffix(record DeadRecord) ([]byte, error) {
	switch {
	case record.Reason != DeadReasonMaxDeliveries && record.Reason != DeadReasonCollided:
		return nil, fmt.Errorf("%w: reason %q is neither %q nor %q", ErrDeadRecordInvalid, record.Reason, DeadReasonMaxDeliveries, DeadReasonCollided)
	case record.Deliveries < 1:
		return nil, fmt.Errorf("%w: deliveries %d is less than 1", ErrDeadRecordInvalid, record.Deliveries)
	}
	if err := record.Data.Validate(); err != nil {
		return nil, fmt.Errorf("%w: data is not a BSON document: %w", ErrDeadRecordInvalid, err)
	}
	st := op.scope.st
	if op.layout != claimLayoutHeap {
		suffix := bigEndian(st.Next)
		st.Next++
		return suffix, nil
	}
	if _, err := heapStoredValue(record.Data); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDeadRecordInvalid, err)
	}
	entryID, _ := record.Data.Lookup("_id_").StringValueOK()
	priority, _ := record.Data.Lookup("priority").AsInt64OK()
	if entryID == "" || priority == math.MinInt64 {
		return nil, fmt.Errorf("%w: a heap entry needs an _id_ and a priority above %d", ErrDeadRecordInvalid, math.MinInt64)
	}
	origin := append(bigEndian(invertedPriority(priority)), entryID...)
	suffix := deadSuffix(op.layout, origin, st.DeadSeq)
	st.DeadSeq++
	return suffix, nil
}

// heapStoredValue is the value of a heap entry's {_id_, priority, value}
// document: the bytes the element holds under its 0x04 key.
func heapStoredValue(data bson.Raw) ([]byte, error) {
	stored, ok := data.Lookup("value").DocumentOK()
	if !ok {
		return nil, errors.New("a heap entry's data has no value document")
	}
	return stored, nil
}
