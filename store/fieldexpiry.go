package store

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// QueueTakePosition removes the element at position from a fifoStore or lifoStore field in
// write's batch, with its nested stores and one state save, and returns the element. found is
// false when no element sits at the position: it was popped, acked, dead-lettered, or it is
// leased and so held under a lease record instead. The cursors are left where they are: a pop
// seeks from the cursor and steps over a missing key, so a gap costs one step and no more.
// Positions are never reused, so a position names one element for the life of the field.
func QueueTakePosition(w FieldWrite, ref FieldRef, position uint64) (taken []byte, found bool, err error) {
	if ref.Engine != FieldFIFO && ref.Engine != FieldLIFO {
		return nil, false, wrongEngine("take by position", ref)
	}
	prefix := ref.StorePrefix()
	remainder := PositionRemainder(position)
	key := FieldElementKey(prefix, storage.NSQueue, remainder)
	value, err := w.Snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading the element at position %d: %w", position, err)
	}
	stateKey := FieldStateKey(prefix)
	state, _, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return nil, false, err
	}
	if state.Count == 0 {
		return nil, false, fmt.Errorf("%w: a field store counts no elements and holds one at position %d", ErrFieldCursorLost, position)
	}
	if err := w.removeElement(key, value); err != nil {
		return nil, false, err
	}
	if err := w.removeNested(ref, remainder); err != nil {
		return nil, false, err
	}
	state.Count--
	if err := w.saveFieldState(stateKey, state, true); err != nil {
		return nil, false, err
	}
	return bytes.Clone(value), true, nil
}

// FieldHasLeases reports whether any lease of the field is outstanding: its elements are then
// held under lease records, out of the queue, and one that is not at its position may be
// leased rather than gone. It reads one key and walks nothing.
func FieldHasLeases(snap storage.KVSnapshot, ref FieldRef) (bool, error) {
	queue, err := newFieldQueue(snap, nil, ref)
	if err != nil {
		return false, fmt.Errorf("reading the leases of a field store: %w", err)
	}
	var delivery fieldDeliveryState
	if _, err := queue.loadDelivery(&delivery); err != nil {
		return false, err
	}
	return delivery.Leases > 0 || delivery.Pending > 0, nil
}
