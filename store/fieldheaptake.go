package store

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-server/storage"
)

// errHeapEntryFound stops the walk of FieldHeapEntry at the entry it looks for.
var errHeapEntryFound = errors.New("the heap entry was found")

// FieldHeapEntry returns the element of a heap instance stored under entryID, writing
// nothing. A heap key is priority then entry id, so there is no index by id: the walk is
// in pop order and stops at the first match, which is O(elements) for an id that is
// absent or low in priority. If two priorities hold the same id, the higher one is the
// match, the same element a pop would reach first.
func FieldHeapEntry(snap storage.KVSnapshot, ref FieldRef, entryID string) (FieldEntry, bool, error) {
	if ref.Engine != FieldHeap {
		return FieldEntry{}, false, wrongEngine("take by entry id", ref)
	}
	state, _, err := loadFieldState(snap, FieldStateKey(ref.StorePrefix()))
	if err != nil || state.Count == 0 {
		return FieldEntry{}, false, err
	}
	var match FieldEntry
	err = FieldPeekN(snap, ref, int(min(state.Count, math.MaxInt32)), func(entry FieldEntry) error {
		if entry.EntryID != entryID {
			return nil
		}
		match = FieldEntry{Remainder: bytes.Clone(entry.Remainder), Value: bytes.Clone(entry.Value),
			Priority: entry.Priority, EntryID: entry.EntryID}
		return errHeapEntryFound
	})
	if errors.Is(err, errHeapEntryFound) {
		return match, true, nil
	}
	return FieldEntry{}, false, err
}

// FieldHeapTake removes the element of a heap instance stored under entryID, with its
// nested stores, in the parent's batch, and returns it. An absent entry is found false
// and writes nothing.
func FieldHeapTake(w FieldWrite, ref FieldRef, entryID string) (FieldEntry, bool, error) {
	entry, found, err := FieldHeapEntry(w.Snap, ref, entryID)
	if err != nil || !found {
		return FieldEntry{}, false, err
	}
	prefix := ref.StorePrefix()
	stateKey := FieldStateKey(prefix)
	state, existed, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return FieldEntry{}, false, err
	}
	if err := w.removeElement(FieldElementKey(prefix, storage.NSQueue, entry.Remainder), entry.Value); err != nil {
		return FieldEntry{}, false, err
	}
	if err := w.removeNested(ref, entry.Remainder); err != nil {
		return FieldEntry{}, false, err
	}
	state.Count--
	if err := w.saveFieldState(stateKey, state, existed); err != nil {
		return FieldEntry{}, false, fmt.Errorf("saving the heap state after taking %q: %w", entryID, err)
	}
	return entry, true, nil
}
