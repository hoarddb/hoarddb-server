package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Document-scoped field stores (nested buckets, GAP-22; spec §4.2–§4.4). A
// field store is not an engine object with a lifetime: it is a set of
// operations that add their effect to the parent bucket's Apply batch, so an
// element, its instance state and the parent's BucketState — applied_seq
// included — commit together, and replay's gate covers field writes with no
// mechanism of its own. An instance is a key range (store/fieldkey.go) and
// opens nothing.
//
// Every operation charges len(key)+len(value) of each key it writes or
// deletes, the instance state included when it is created, to the parent's
// LogicalBytes, and never touches Entries: that is the document count.

// FieldEngine is the engine of a declared store field.
type FieldEngine uint8

// The six field engines. Zero is no engine.
const (
	FieldHash FieldEngine = iota + 1
	FieldBTree
	FieldFIFO
	FieldLIFO
	FieldHeap
	FieldBlob
)

var fieldEngineNames = [...]string{FieldHash: "hash", FieldBTree: "btree", FieldFIFO: "fifo", FieldLIFO: "lifo", FieldHeap: "heap", FieldBlob: "blob"}

// String is the engine's catalog name: hash, btree, fifo, lifo, heap, blob.
func (e FieldEngine) String() string {
	if e == 0 || int(e) >= len(fieldEngineNames) {
		return fmt.Sprintf("FieldEngine(%d)", uint8(e))
	}
	return fieldEngineNames[e]
}

// ParseFieldEngine reads a catalog engine name.
func ParseFieldEngine(name string) (FieldEngine, error) {
	for engine, engineName := range fieldEngineNames {
		if engine != 0 && engineName == strings.ToLower(name) {
			return FieldEngine(engine), nil
		}
	}
	return 0, fmt.Errorf("%q is not a field-store engine (one of hash, btree, fifo, lifo, heap, blob)", name)
}

// ordered reports whether the engine keeps its elements under inner 0x04.
func (e FieldEngine) ordered() bool {
	return e == FieldFIFO || e == FieldLIFO || e == FieldHeap
}

// FieldRef names one store instance: the owner (a document's
// FieldOwnerPrefix, or an L2 element's FieldElementAnchor), the field's
// catalog id and its engine.
type FieldRef struct {
	Owner   []byte
	FieldID uint16
	Engine  FieldEngine
	// Nested is the layout of the L3 stores declared on this field's
	// elements, nil when there are none. Removing an element then removes
	// its L3 stores in the same batch: an L2 element's L3 stores live and
	// die with it (spec §7 item 3).
	Nested FieldLayout
}

// StorePrefix is the instance's key prefix, owner ‖ field id.
func (r FieldRef) StorePrefix() []byte {
	return FieldStorePrefix(r.Owner, r.FieldID)
}

func (r FieldRef) elementInner() byte {
	if r.Engine.ordered() {
		return storage.NSQueue
	}
	return storage.NSDocuments
}

// elementBounds is the instance's element namespace, never the whole store
// prefix: that would reach the state at 0x05 and the L3 stores at 0x07. The
// bounds lie inside one store field, so a list, pop or peek asks each table's
// bloom filter whether it holds a key of that field (for a store on a store's
// element, of the outer field) and opens only the tables that may.
func (r FieldRef) elementBounds() storage.IterOptions {
	prefix := r.StorePrefix()
	inner := r.elementInner()
	return storage.StoreIterOptions(FieldElementKey(prefix, inner, nil), FieldElementKey(prefix, inner+1, nil))
}

// FieldEntry is one element as a read returns it: its remainder and value,
// and the remainder decoded for the engine — the element key (hash, btree,
// blob), the position (fifo, lifo) or the priority and entry id (heap).
type FieldEntry struct {
	Remainder  []byte
	Value      []byte
	ElementKey string
	Position   uint64
	Priority   int64
	EntryID    string
}

// FieldWrite is the parent's apply that a mutating operation adds its effect
// to: the snapshot Apply read, the parent's state and the batch.
type FieldWrite struct {
	Snap  storage.KVSnapshot
	State *BucketState
	Batch storage.KVBatch
	// Payloads, when set, collects the payload paths of blob elements the
	// write removed with an L2 element, for the caller to unlink after the
	// commit.
	Payloads *[]string
}

// removeNested removes the L3 stores of the element at remainder, when the
// field declares any.
func (w FieldWrite) removeNested(ref FieldRef, remainder []byte) error {
	if len(ref.Nested) == 0 {
		return nil
	}
	payloads, err := deleteFieldStores(w, FieldElementAnchor(ref.StorePrefix(), remainder), ref.Nested)
	if err != nil {
		return fmt.Errorf("deleting the stores of a %s element: %w", ref.Engine, err)
	}
	if w.Payloads != nil {
		*w.Payloads = append(*w.Payloads, payloads...)
	}
	return nil
}

// ErrFieldCursorLost is an instance whose state counts elements its range
// does not hold at the cursor, which no write of this file leaves behind.
var ErrFieldCursorLost = errors.New("the field store's cursor points past its elements")

// fieldPopSeekHook, when set, is called with the key a pop or peek passes to
// Seek — not the first key it visits: popped keys are deleted, so the first
// key visited is the same whether the seek targets the cursor or the prefix,
// and an assertion on it could not fail. Nil in production.
var fieldPopSeekHook atomic.Pointer[func(seekKey []byte)]

func wrongEngine(verb string, ref FieldRef) error {
	return fmt.Errorf("a %s field store has no %s", ref.Engine, verb)
}

// loadFieldState reads an instance's state; absent is the zero value.
func loadFieldState(snap storage.KVSnapshot, stateKey []byte) (FieldInstanceState, bool, error) {
	encoded, err := snap.Get(stateKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return FieldInstanceState{}, false, nil
	}
	if err != nil {
		return FieldInstanceState{}, false, fmt.Errorf("reading field-store state: %w", err)
	}
	state, err := DecodeFieldInstanceState(stateKey, encoded)
	return state, true, err
}

// saveFieldState writes the state, charging its bytes the first time. A
// state whose count reached zero is kept: a position is never reused while
// the document lives (spec T-04).
func (w FieldWrite) saveFieldState(stateKey []byte, state FieldInstanceState, existed bool) error {
	if !existed {
		w.State.LogicalBytes += int64(len(stateKey) + fieldInstanceStateSize)
	}
	if err := w.Batch.Set(stateKey, EncodeFieldInstanceState(state)); err != nil {
		return fmt.Errorf("writing field-store state: %w", err)
	}
	return nil
}

// FieldPush appends element to a fifo or lifo instance and returns the
// position it took. The cursor arithmetic is QueueStore.pushEntries' on the
// instance state: cursors merge forward and never move backwards.
func FieldPush(w FieldWrite, ref FieldRef, element []byte) (uint64, error) {
	callReclaimVerbHook("field push")
	if ref.Engine != FieldFIFO && ref.Engine != FieldLIFO {
		return 0, wrongEngine("push without a priority", ref)
	}
	prefix := ref.StorePrefix()
	stateKey := FieldStateKey(prefix)
	state, existed, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return 0, err
	}
	position := state.Next
	key := FieldElementKey(prefix, storage.NSQueue, PositionRemainder(position))
	if err := w.Batch.Set(key, element); err != nil {
		return 0, fmt.Errorf("writing a field-store element of %d bytes at position %d: %w", len(element), position, err)
	}
	state.Next = max(state.Next, position+1)
	state.Tail = max(state.Tail, position)
	state.Count++
	w.State.LogicalBytes += int64(len(key) + len(element))
	return position, w.saveFieldState(stateKey, state, existed)
}

// FieldHeapPush stores element at (priority, entryID). An element already
// stored there is overwritten and the count does not grow, as a heap
// bucket's putElements does, so a redelivered push does not grow the
// instance.
func FieldHeapPush(w FieldWrite, ref FieldRef, priority int64, entryID string, element []byte) error {
	callReclaimVerbHook("field heap push")
	if ref.Engine != FieldHeap {
		return wrongEngine("push with a priority", ref)
	}
	// Refused for a field as for a heap bucket (store/heap.go:preparePush):
	// one priority range for every heap.
	if priority == math.MinInt64 {
		return fmt.Errorf("priority must be in range [%d, %d], got %d", math.MinInt64+1, math.MaxInt64, priority)
	}
	remainder, err := HeapRemainder(priority, entryID)
	if err != nil {
		return err
	}
	return w.putElement(ref, FieldElementKey(ref.StorePrefix(), storage.NSQueue, remainder), element)
}

// FieldPut upserts element under elementKey in a hash or btree instance; the
// count grows only when the key was absent.
func FieldPut(w FieldWrite, ref FieldRef, elementKey string, element []byte) error {
	if ref.Engine.ordered() || ref.Engine == FieldBlob {
		return wrongEngine("put", ref)
	}
	remainder, err := KeyedRemainder(elementKey)
	if err != nil {
		return err
	}
	return w.putElement(ref, FieldElementKey(ref.StorePrefix(), storage.NSDocuments, remainder), element)
}

// putElement writes key → element as an upsert and maintains the count.
func (w FieldWrite) putElement(ref FieldRef, key, element []byte) error {
	stateKey := FieldStateKey(ref.StorePrefix())
	state, existed, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return err
	}
	previous, err := w.Snap.Get(key)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		state.Count++
		w.State.LogicalBytes += int64(len(key) + len(element))
	case err != nil:
		return fmt.Errorf("reading field-store element: %w", err)
	default:
		w.State.LogicalBytes += int64(len(element) - len(previous))
	}
	if err := w.Batch.Set(key, element); err != nil {
		return fmt.Errorf("writing field-store element: %w", err)
	}
	return w.saveFieldState(stateKey, state, existed)
}

// FieldDelete removes elementKey from a hash or btree instance and reports
// whether it was there. Deleting an absent key writes nothing.
func FieldDelete(w FieldWrite, ref FieldRef, elementKey string) (bool, error) {
	if ref.Engine.ordered() || ref.Engine == FieldBlob {
		return false, wrongEngine("delete", ref)
	}
	remainder, err := KeyedRemainder(elementKey)
	if err != nil {
		return false, err
	}
	prefix := ref.StorePrefix()
	key := FieldElementKey(prefix, storage.NSDocuments, remainder)
	previous, err := w.Snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading field-store element: %w", err)
	}
	stateKey := FieldStateKey(prefix)
	state, existed, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return false, err
	}
	if err := w.removeElement(key, previous); err != nil {
		return false, err
	}
	if err := w.removeNested(ref, remainder); err != nil {
		return false, err
	}
	state.Count--
	return true, w.saveFieldState(stateKey, state, existed)
}

func (w FieldWrite) removeElement(key, value []byte) error {
	if err := w.Batch.Delete(key); err != nil {
		return fmt.Errorf("deleting field-store element: %w", err)
	}
	w.State.LogicalBytes -= int64(len(key) + len(value))
	return nil
}

// cursorSeek is the key a pop or peek seeks to: [..][0x04][head] for fifo, a
// reverse seek from [..][0x04][tail] for lifo, and the first key of
// [..][0x04] for heap — never the store prefix, which would walk every delete
// marker since the last compaction (the tombstone wall).
func cursorSeek(snap storage.KVSnapshot, ref FieldRef, state FieldInstanceState) storage.KVIterator {
	bounds := ref.elementBounds()
	seekKey := bounds.Lower
	switch ref.Engine {
	case FieldFIFO:
		seekKey = FieldElementKey(ref.StorePrefix(), storage.NSQueue, PositionRemainder(state.Head))
	case FieldLIFO:
		bounds.Reverse = true
		seekKey = FieldElementKey(ref.StorePrefix(), storage.NSQueue, PositionRemainder(state.Tail))
	}
	if hook := fieldPopSeekHook.Load(); hook != nil {
		(*hook)(seekKey)
	}
	it := snap.NewIterator(bounds)
	it.Seek(seekKey)
	return it
}

// FieldPop removes and returns the element at the cursor — oldest (fifo),
// newest (lifo) or highest priority (heap) — with its cursor move, in the
// parent's batch. An empty instance is found false, not an error.
func FieldPop(w FieldWrite, ref FieldRef) (FieldEntry, bool, error) {
	var popped FieldEntry
	removed, err := FieldPopN(w, ref, 1, func(entry FieldEntry) error {
		popped = entry
		return nil
	})
	if err != nil || removed == 0 {
		return FieldEntry{}, false, err
	}
	return popped, true, nil
}

// FieldPopN removes up to n elements in pop order in the parent's batch,
// with each element's nested stores, the cursor updated per element in walk
// order and one state save. It calls visit with each element before adding
// its deletes, and stops at the first error visit returns. One state load,
// one seek (cursorSeek), n iterator steps. A walk that ends unstopped having
// removed fewer than min(n, Count) is ErrFieldCursorLost: there is no short
// pop. It returns how many it removed.
func FieldPopN(w FieldWrite, ref FieldRef, n int, visit func(FieldEntry) error) (int, error) {
	return FieldPopLive(w, ref, n, nil, nil, visit)
}

// MaxExpiredPerVerb bounds how many expired elements one pop or peek steps over, and a pop
// removes, before it stops short. The skipped set is removed in the verb's own batch, which one
// commit must hold: a backlog larger than that would fail the commit after the record was
// appended and fence the pair. What is left is the sweep's (expiry spec 3.3) and the next verb's.
const MaxExpiredPerVerb = 64

// FieldPopLive is FieldPopN for a store whose elements can expire. An element that expired
// reports true for is skipped: it is removed in the same batch, the cursor moves past it as a
// pop moves it, and discard sees it instead of visit, so the caller can drop what it keeps for
// it. n counts only the elements visit saw, and the walk goes on past expired ones to find
// them. A nil expired is FieldPopN. It returns how many elements visit saw.
func FieldPopLive(w FieldWrite, ref FieldRef, n int, expired func(FieldEntry) bool, discard, visit func(FieldEntry) error) (int, error) {
	callReclaimVerbHook("field pop")
	if !ref.Engine.ordered() {
		return 0, wrongEngine("pop or peek", ref)
	}
	prefix := ref.StorePrefix()
	stateKey := FieldStateKey(prefix)
	state, _, err := loadFieldState(w.Snap, stateKey)
	if err != nil || state.Count == 0 {
		return 0, err
	}
	it := cursorSeek(w.Snap, ref, state)
	defer it.Close()
	delivered, removed, capped := 0, 0, false
	for delivered < n && it.Valid() {
		key := append([]byte(nil), it.Key()...)
		value, err := it.Value()
		if err != nil {
			return 0, fmt.Errorf("reading field-store element: %w", err)
		}
		entry, err := decodeFieldEntry(ref, key[len(prefix)+1:], value)
		if err != nil {
			return 0, &storage.KeyError{Operation: "reading a field-store element", Key: key, Cause: err}
		}
		skipped := expired != nil && expired(entry)
		if skipped && removed-delivered >= MaxExpiredPerVerb {
			capped = true
			break
		}
		visitor := visit
		if skipped {
			visitor = discard
		}
		if visitor != nil {
			if err := visitor(entry); err != nil {
				return 0, err
			}
		}
		if err := w.removeElement(key, entry.Value); err != nil {
			return 0, err
		}
		if err := w.removeNested(ref, entry.Remainder); err != nil {
			return 0, err
		}
		switch ref.Engine {
		case FieldLIFO:
			if entry.Position > 0 {
				state.Tail = entry.Position - 1
			}
		case FieldFIFO:
			state.Head = max(state.Head, entry.Position+1)
		}
		removed++
		if !skipped {
			delivered++
		}
		// Never step past the n-th element: the step walks the delete
		// markers beyond it, which a lifo keeps below its cursor (BUG-0580).
		if delivered < n {
			it.Next()
		}
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("walking the field-store elements: %w", err)
	}
	// Compared as int64, so an n below 1 removes nothing rather than reading
	// as a huge count.
	if !capped && int64(delivered) < min(int64(n), int64(state.Count)) && uint64(removed) < state.Count {
		return 0, fmt.Errorf("%w: a field store counts %d elements and holds %d at its cursor", ErrFieldCursorLost, state.Count, removed)
	}
	if removed == 0 {
		return 0, nil
	}
	state.Count -= uint64(removed)
	return delivered, w.saveFieldState(stateKey, state, true)
}

// FieldPeek returns the element FieldPop would take, writing nothing. It is
// FieldPeekN for one element.
func FieldPeek(snap storage.KVSnapshot, ref FieldRef) (FieldEntry, bool, error) {
	var first FieldEntry
	found := false
	err := FieldPeekN(snap, ref, 1, func(entry FieldEntry) error {
		first, found = entry, true
		return nil
	})
	return first, found, err
}

// FieldPeekN calls visit with up to n elements in the order n pops would take
// them, from one snapshot, writing nothing. It reads no value past the n-th
// and stops at the first error visit returns. One state load and one seek
// (cursorSeek, the seek FieldPop takes its element from), then iterator
// steps: O(log n + n) for fifo and lifo.
//
// A walk that ends without being stopped, having visited fewer than
// min(n, Count) elements, is ErrFieldCursorLost whether it found none or
// some: there is no short result, which a caller could not tell from the
// whole store.
func FieldPeekN(snap storage.KVSnapshot, ref FieldRef, n int, visit func(FieldEntry) error) error {
	return FieldPeekLive(snap, ref, n, nil, visit)
}

// FieldPeekLive is FieldPeekN for a store whose elements can expire: an element expired
// reports true for is stepped over and never visited, and n counts only the elements visit
// saw. It writes nothing; the pop that follows removes what a peek only skipped. A nil
// expired is FieldPeekN.
func FieldPeekLive(snap storage.KVSnapshot, ref FieldRef, n int, expired func(FieldEntry) bool, visit func(FieldEntry) error) error {
	callReclaimVerbHook("field peek")
	if !ref.Engine.ordered() {
		return wrongEngine("pop or peek", ref)
	}
	state, _, err := loadFieldState(snap, FieldStateKey(ref.StorePrefix()))
	if err != nil || state.Count == 0 {
		return err
	}
	it := cursorSeek(snap, ref, state)
	defer it.Close()
	storePrefixLen := len(ref.StorePrefix())
	visited, stepped, capped := 0, 0, false
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value, err := it.Value()
		if err != nil {
			return fmt.Errorf("reading field-store element: %w", err)
		}
		// key is valid only until the next step; the entry keeps its remainder.
		entry, err := decodeFieldEntry(ref, append([]byte(nil), key[storePrefixLen+1:]...), value)
		if err != nil {
			return &storage.KeyError{Operation: "reading a field-store element", Key: bytes.Clone(key), Cause: err}
		}
		if expired != nil && expired(entry) {
			if stepped-visited >= MaxExpiredPerVerb {
				capped = true
				break
			}
			stepped++
			continue
		}
		stepped++
		visited++
		if err := visit(entry); err != nil {
			return fmt.Errorf("peeking %d field-store elements: %w", n, err)
		}
		if visited == n {
			return nil
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the field-store elements: %w", err)
	}
	if !capped && uint64(visited) < min(uint64(n), state.Count) && uint64(stepped) < state.Count {
		return fmt.Errorf("%w: a field store counts %d elements and holds %d at its cursor", ErrFieldCursorLost, state.Count, stepped)
	}
	return nil
}

// FieldGet returns the element stored under elementKey in a hash or btree
// instance.
func FieldGet(snap storage.KVSnapshot, ref FieldRef, elementKey string) ([]byte, bool, error) {
	if ref.Engine.ordered() || ref.Engine == FieldBlob {
		return nil, false, wrongEngine("get", ref)
	}
	remainder, err := KeyedRemainder(elementKey)
	if err != nil {
		return nil, false, err
	}
	key := FieldElementKey(ref.StorePrefix(), storage.NSDocuments, remainder)
	value, err := snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading field-store element: %w", err)
	}
	return value, true, nil
}

// FieldLen is the instance's element count, read from its state: no scan.
func FieldLen(snap storage.KVSnapshot, ref FieldRef) (uint64, error) {
	state, _, err := loadFieldState(snap, FieldStateKey(ref.StorePrefix()))
	return state.Count, err
}

// FieldListRequest bounds one FieldList walk.
type FieldListRequest struct {
	// After is a remainder the walk starts strictly after; nil starts at the
	// first element.
	After []byte
	// Limit is the most entries returned; it must be positive.
	Limit int
	// MaxBytes stops the walk before an entry that would take the returned
	// remainders and values past it. The first entry is always returned, so
	// a caller paging by After always makes progress.
	MaxBytes int
}

// FieldList walks the instance's elements in key order: positions for fifo
// and lifo, highest priority first for heap, element keys for hash, btree
// and blob. hasMore reports whether an element lies after the last returned.
func FieldList(snap storage.KVSnapshot, ref FieldRef, req FieldListRequest) ([]FieldEntry, bool, error) {
	bounds := ref.elementBounds()
	it := snap.NewIterator(bounds)
	defer it.Close()
	it.Seek(append(append([]byte(nil), bounds.Lower...), req.After...))
	if req.After != nil && it.Valid() && bytes.Equal(it.Key()[len(bounds.Lower):], req.After) {
		it.Next()
	}
	var entries []FieldEntry
	size := 0
	for ; it.Valid(); it.Next() {
		if len(entries) == req.Limit {
			return entries, true, nil
		}
		remainder := append([]byte(nil), it.Key()[len(bounds.Lower):]...)
		value, err := it.Value()
		if err != nil {
			return nil, false, fmt.Errorf("reading field-store element: %w", err)
		}
		if len(entries) > 0 && size+len(remainder)+len(value) > req.MaxBytes {
			return entries, true, nil
		}
		entry, err := decodeFieldEntry(ref, remainder, value)
		if err != nil {
			return nil, false, &storage.KeyError{Operation: "reading a field-store element", Key: bytes.Clone(it.Key()), Cause: err}
		}
		entries = append(entries, entry)
		size += len(remainder) + len(value)
	}
	if err := it.Err(); err != nil {
		return nil, false, fmt.Errorf("walking the field-store elements: %w", err)
	}
	return entries, false, nil
}

// decodeFieldEntry reads an element's remainder for its engine.
func decodeFieldEntry(ref FieldRef, remainder, value []byte) (FieldEntry, error) {
	entry := FieldEntry{Remainder: remainder, Value: value}
	switch ref.Engine {
	case FieldFIFO, FieldLIFO:
		if len(remainder) != 8 {
			return FieldEntry{}, fmt.Errorf("the element remainder is %d bytes, want an 8-byte position", len(remainder))
		}
		entry.Position = binary.BigEndian.Uint64(remainder)
	case FieldHeap:
		if len(remainder) < 8 {
			return FieldEntry{}, fmt.Errorf("the heap element remainder is %d bytes, want a priority and an entry id", len(remainder))
		}
		entryID, err := decodeStringComponent(remainder[8:])
		if err != nil {
			return FieldEntry{}, fmt.Errorf("heap field-store element remainder: %w", err)
		}
		entry.Priority, entry.EntryID = priorityOf(binary.BigEndian.Uint64(remainder[:8])), entryID
	default:
		elementKey, err := decodeStringComponent(remainder)
		if err != nil {
			return FieldEntry{}, fmt.Errorf("field-store element remainder: %w", err)
		}
		entry.ElementKey = elementKey
	}
	return entry, nil
}

// decodeStringComponent inverts EncodeComponent for a string that fills
// encoded exactly.
func decodeStringComponent(encoded []byte) (string, error) {
	if len(encoded) == 0 || encoded[0] != tagString {
		return "", fmt.Errorf("a component of %d bytes is not an encoded string", len(encoded))
	}
	length, err := componentLen(encoded)
	if err != nil {
		return "", err
	}
	if length != len(encoded) {
		return "", fmt.Errorf("a component holds %d bytes after its string", len(encoded)-length)
	}
	return string(bytes.ReplaceAll(encoded[1:length-2], []byte{0x00, 0xFF}, []byte{0x00})), nil
}

// DecodeStringID inverts EncodeComponent for a document id that is a string, so a message
// can name the document by the id the user typed instead of its encoded bytes.
func DecodeStringID(encoded []byte) (string, error) {
	return decodeStringComponent(encoded)
}

// FieldLayout is a bucket's declared store fields by id: what the delete
// cascade needs to tell a blobStore field's element, whose metadata and
// payload go with it, from every other key, at any depth. A bucket that
// declares no blob field needs none.
type FieldLayout map[uint16]FieldEngine

// elementRemainderLen is the length of the element remainder rest begins
// with, for an element of engine: a position, a priority and an entry id, or
// an element key.
func elementRemainderLen(engine FieldEngine, rest []byte) (int, error) {
	switch engine {
	case FieldFIFO, FieldLIFO:
		if len(rest) < 8 {
			return 0, fmt.Errorf("a %s element remainder is 8 bytes, got %d", engine, len(rest))
		}
		return 8, nil
	case FieldHeap:
		if len(rest) < 9 {
			return 0, fmt.Errorf("a heap element remainder is a priority and an entry id, got %d bytes", len(rest))
		}
		entryID, err := componentLen(rest[8:])
		return 8 + entryID, err
	default:
		return componentLen(rest)
	}
}

// leafOf reads the store and inner byte a field-store key belongs to, given
// the key after its owner: an L3 key is the L2 field, 0x07, the L2 element's
// remainder, then the L3 store (fieldkey.go).
func leafOf(rest []byte, layout FieldLayout) (uint16, byte, error) {
	for {
		if len(rest) < 3 {
			return 0, 0, fmt.Errorf("the field-store key suffix holds no field id and inner byte")
		}
		fieldID, inner := binary.BigEndian.Uint16(rest), rest[2]
		if inner != storage.NSFieldStore {
			return fieldID, inner, nil
		}
		engine, declared := layout[fieldID]
		if !declared {
			return 0, 0, fmt.Errorf("the field-store key suffix names field %d, which the bucket does not declare", fieldID)
		}
		skip, err := elementRemainderLen(engine, rest[3:])
		if err != nil {
			return 0, 0, err
		}
		rest = rest[3+skip:]
	}
}

// FieldValueIsDocument reports whether the value under a field-store key is a
// BSON document: an element of a hash, btree, fifo, lifo or heap store, at any
// depth of the path. A blobStore element's value is its blob id and an
// instance state is fixed binary, so neither is. rest is the key after the
// bucket's [s][id][0x07]; layout names each declared field's engine, which
// reading the path of an L3 key needs. A key the layout cannot place is an
// error: no reader of this build reads it, and its value cannot be told apart.
func FieldValueIsDocument(rest []byte, layout FieldLayout) (bool, error) {
	ownerLength, err := componentLen(rest)
	if err != nil {
		return false, fmt.Errorf("reading the owner of a field-store key: %w", err)
	}
	fieldID, inner, err := leafOf(rest[ownerLength:], layout)
	if err != nil {
		return false, err
	}
	engine, declared := layout[fieldID]
	if !declared {
		return false, fmt.Errorf("the field-store key names field %d, which the bucket does not declare", fieldID)
	}
	if inner == storage.NSState || inner == InnerAggregate || (engine == FieldBlob && inner == storage.NSDocuments) {
		return false, nil
	}
	return true, nil
}

// deleteFieldStores deletes every key under owner — each store of one
// document (or, at L3, of one L2 element): its elements, its state and the
// stores of its elements — in the parent's batch, and takes their bytes off
// the parent's LogicalBytes. One bounded walk, because the layout is document
// first. A blobStore field's element also takes its bucket-level metadata
// with it, and its payload path is returned for the caller to unlink after
// the commit: the metadata is gone before the file is. A batch that cannot
// hold it all fails with storage.ErrBatchTooLarge and the whole delete is
// refused: nothing half-deleted, on every replica alike.
func deleteFieldStores(w FieldWrite, owner []byte, layout FieldLayout) ([]string, error) {
	it := w.Snap.NewIterator(storage.IterOptions{Lower: owner, Upper: prefixEnd(owner)})
	defer it.Close()
	var payloads []string
	for it.Seek(nil); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		value, err := it.Value()
		if err != nil {
			return nil, fmt.Errorf("reading a field-store key: %w", err)
		}
		if blobFieldCount(layout) > 0 {
			fieldID, inner, err := leafOf(key[len(owner):], layout)
			if err != nil {
				return nil, &storage.KeyError{Operation: "reading a field-store key", Key: key, Cause: err}
			}
			if layout[fieldID] == FieldBlob && inner == storage.NSDocuments {
				path, err := w.removeBlobMeta(owner, value)
				if err != nil {
					return nil, err
				}
				if path != "" {
					payloads = append(payloads, path)
				}
			}
		}
		if err := w.removeElement(key, value); err != nil {
			return nil, err
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("walking the document's field stores: %w", err)
	}
	return payloads, nil
}

func blobFieldCount(layout FieldLayout) int {
	count := 0
	for _, engine := range layout {
		if engine == FieldBlob {
			count++
		}
	}
	return count
}

// bucketOfOwner reads the slot, database id and bucket id every owner begins
// with: the first 18 bytes of its Scope.
func bucketOfOwner(owner []byte) (uint16, uint64, uint64) {
	return binary.BigEndian.Uint16(owner[0:2]), binary.BigEndian.Uint64(owner[2:10]), binary.BigEndian.Uint64(owner[10:18])
}

// blobMetaKeyOf is a blob field element's metadata key: the bucket's own
// [s][id][0x06][blob id], where a blob bucket's is. Migration, the reclaimer
// and the orphan pass find payload files only through bucket-level 0x06 keys;
// metadata anywhere else would be unlinked as an orphan, leaked on a drop and
// stranded by a migration (nested buckets S-1(c)).
func blobMetaKeyOf(owner []byte, blobID []byte) []byte {
	slot, databaseID, bucketID := bucketOfOwner(owner)
	return storage.Key(slot, databaseID, bucketID, storage.NSBlobMeta, blobID)
}

// removeBlobMeta deletes a blob field element's metadata in the batch and
// returns its payload path, "" for inline content.
func (w FieldWrite) removeBlobMeta(owner []byte, blobID []byte) (string, error) {
	metaKey := blobMetaKeyOf(owner, blobID)
	encoded, err := w.Snap.Get(metaKey)
	if err != nil {
		return "", fmt.Errorf("reading the metadata of blob %s: %w", blobID, err)
	}
	var meta blobMeta
	if err := bson.Unmarshal(encoded, &meta); err != nil {
		return "", fmt.Errorf("decoding the metadata of blob %s: %w", blobID, err)
	}
	if err := w.Batch.Delete(metaKey); err != nil {
		return "", fmt.Errorf("deleting the metadata of blob %s: %w", blobID, err)
	}
	w.State.LogicalBytes -= meta.Size
	return meta.Path, nil
}

// PreparedBlob is one blobStore field element made ready by PrepareFieldBlob: a
// minted blob id and its metadata, its payload already durable when the
// content is at least InlineThreshold.
type PreparedBlob struct {
	blobID  string
	meta    blobMeta
	encoded []byte
}

// BlobID is the prepared blob's minted id.
func (b PreparedBlob) BlobID() string { return b.blobID }

// PayloadPath is the prepared blob's payload file relative to the blob root,
// "" when its content is inline.
func (b PreparedBlob) PayloadPath() string { return b.meta.Path }

// PrepareFieldBlob writes a blob field element's payload, when it has one,
// under the parent bucket's slot and id — the name the orphan pass reads
// the metadata key from — before any batch names it.
func PrepareFieldBlob(root string, ref FieldRef, keys sealed.BlobKeySource, content []byte, metadata map[string]interface{}) (PreparedBlob, error) {
	slot, _, bucketID := bucketOfOwner(ref.Owner)
	blobID, meta, encoded, err := prepareBlob(payloadSite{root: root, slot: slot, bucketID: bucketID, keys: keys}, content, metadata)
	if err != nil {
		return PreparedBlob{}, err
	}
	return PreparedBlob{blobID: blobID, meta: meta, encoded: encoded}, nil
}

// FieldBlobPut maps elementKey to the prepared blob and writes its metadata
// at bucket level. An element already mapped is an overwrite: its old
// metadata goes in the same batch, and its payload path is returned for the
// caller to unlink after the commit, as a blob bucket's overwrite does.
func FieldBlobPut(w FieldWrite, ref FieldRef, elementKey string, blob PreparedBlob) (string, error) {
	callReclaimVerbHook("field blob put")
	if ref.Engine != FieldBlob {
		return "", wrongEngine("blob put", ref)
	}
	remainder, err := KeyedRemainder(elementKey)
	if err != nil {
		return "", err
	}
	prefix := ref.StorePrefix()
	key := FieldElementKey(prefix, storage.NSDocuments, remainder)
	stateKey := FieldStateKey(prefix)
	state, existed, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return "", err
	}
	replacedPath := ""
	previous, err := w.Snap.Get(key)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		state.Count++
		w.State.LogicalBytes += int64(len(key) + len(blob.blobID))
	case err != nil:
		return "", fmt.Errorf("reading field-store element: %w", err)
	default:
		if replacedPath, err = w.removeBlobMeta(ref.Owner, previous); err != nil {
			return "", err
		}
		w.State.LogicalBytes += int64(len(blob.blobID) - len(previous))
	}
	if err := w.Batch.Set(key, []byte(blob.blobID)); err != nil {
		return "", fmt.Errorf("mapping element %q to blob %s: %w", elementKey, blob.blobID, err)
	}
	if err := w.Batch.Set(blobMetaKeyOf(ref.Owner, []byte(blob.blobID)), blob.encoded); err != nil {
		return "", fmt.Errorf("writing the metadata of blob %s: %w", blob.blobID, err)
	}
	w.State.LogicalBytes += blob.meta.Size
	return replacedPath, w.saveFieldState(stateKey, state, existed)
}

// FieldBlobDelete unmaps elementKey and deletes its blob's metadata, and
// returns the payload path for the caller to unlink after the commit.
func FieldBlobDelete(w FieldWrite, ref FieldRef, elementKey string) (bool, string, error) {
	callReclaimVerbHook("field blob delete")
	if ref.Engine != FieldBlob {
		return false, "", wrongEngine("blob delete", ref)
	}
	remainder, err := KeyedRemainder(elementKey)
	if err != nil {
		return false, "", err
	}
	prefix := ref.StorePrefix()
	key := FieldElementKey(prefix, storage.NSDocuments, remainder)
	blobID, err := w.Snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("reading field-store element: %w", err)
	}
	stateKey := FieldStateKey(prefix)
	state, existed, err := loadFieldState(w.Snap, stateKey)
	if err != nil {
		return false, "", err
	}
	path, err := w.removeBlobMeta(ref.Owner, blobID)
	if err != nil {
		return false, "", err
	}
	if err := w.removeElement(key, blobID); err != nil {
		return false, "", err
	}
	state.Count--
	return true, path, w.saveFieldState(stateKey, state, existed)
}

// FieldBlobGet reads a blob field element: its content, checked against the
// checksum its put recorded, and its metadata. The caller reads under a
// snapshot taken before any overwrite could unlink the file.
func FieldBlobGet(snap storage.KVSnapshot, root string, ref FieldRef, keys sealed.BlobKeySource, elementKey string) (*StoredBlob, error) {
	if ref.Engine != FieldBlob {
		return nil, wrongEngine("blob get", ref)
	}
	remainder, err := KeyedRemainder(elementKey)
	if err != nil {
		return nil, err
	}
	blobID, err := snap.Get(FieldElementKey(ref.StorePrefix(), storage.NSDocuments, remainder))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading field-store element %q: %w", elementKey, err)
	}
	encoded, err := snap.Get(blobMetaKeyOf(ref.Owner, blobID))
	if err != nil {
		return nil, fmt.Errorf("reading the metadata of blob %s (element %q): %w", blobID, elementKey, err)
	}
	var meta blobMeta
	if err := bson.Unmarshal(encoded, &meta); err != nil {
		return nil, fmt.Errorf("decoding the metadata of blob %s (element %q): %w", blobID, elementKey, err)
	}
	slot, _, bucketID := bucketOfOwner(ref.Owner)
	return readBlobContent(payloadSite{root: root, slot: slot, bucketID: bucketID, keys: keys}, string(blobID), meta)
}

// FieldAncestor is one L2 element an L3 element's store hangs on.
type FieldAncestor struct {
	FieldID uint16
	Entry   FieldEntry
}

// FieldElementRecord is one element WalkFieldElements found: its document,
// the L2 elements above it (none at L2), its field and the element itself.
// A blobStore field's Entry.Value is the element's blob id.
type FieldElementRecord struct {
	DocumentKey string
	Ancestors   []FieldAncestor
	FieldID     uint16
	Entry       FieldEntry
}

// WalkFieldElements visits every field-store element of bucket (slot, databaseID, id) in
// snap, in key order — document by document, field by field — and skips the
// instance states. layout names each field's engine, which reading a key's
// remainder, and an L3 key's path, needs. It is one walk of the bucket's
// [s][id][0x07] range: a dump emits its elements from it, and describe counts
// them, per call, with no counter kept anywhere (Q10).
func WalkFieldElements(snap storage.KVSnapshot, ref storage.BucketRef, layout FieldLayout, fn func(FieldElementRecord) error) error {
	slot, databaseID, id := ref.Slot, ref.DatabaseID, ref.BucketID
	lower := storage.Key(slot, databaseID, id, storage.NSFieldStore)
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: prefixEnd(lower)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		record, isElement, err := parseFieldKey(key[len(lower):], layout)
		if err != nil {
			return &storage.KeyError{Operation: "parsing a field-store key", Key: key, Cause: err}
		}
		if !isElement {
			continue
		}
		value, err := it.Value()
		if err != nil {
			return fmt.Errorf("reading field-store element: %w", err)
		}
		record.Entry.Value = value
		if err := fn(record); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the field-store elements: %w", err)
	}
	return nil
}

// parseFieldKey reads a field-store key after [s][id][0x07]: the document
// key, then each store on the way down. It reports false for an instance
// state, which is not an element.
func parseFieldKey(rest []byte, layout FieldLayout) (FieldElementRecord, bool, error) {
	ownerLength, err := componentLen(rest)
	if err != nil {
		return FieldElementRecord{}, false, err
	}
	var record FieldElementRecord
	if record.DocumentKey, err = decodeStringComponent(rest[:ownerLength]); err != nil {
		return FieldElementRecord{}, false, err
	}
	rest = rest[ownerLength:]
	for {
		if len(rest) < 3 {
			return FieldElementRecord{}, false, fmt.Errorf("the field-store key suffix holds no field id and inner byte")
		}
		fieldID, inner := binary.BigEndian.Uint16(rest), rest[2]
		engine, declared := layout[fieldID]
		if !declared {
			return FieldElementRecord{}, false, fmt.Errorf("names field %d, which the bucket does not declare", fieldID)
		}
		rest = rest[3:]
		switch inner {
		case storage.NSState, InnerAggregate:
			return FieldElementRecord{}, false, nil
		case storage.NSFieldStore:
			length, err := elementRemainderLen(engine, rest)
			if err != nil {
				return FieldElementRecord{}, false, err
			}
			ancestor, err := decodeFieldEntry(FieldRef{Engine: engine}, rest[:length], nil)
			if err != nil {
				return FieldElementRecord{}, false, err
			}
			record.Ancestors = append(record.Ancestors, FieldAncestor{FieldID: fieldID, Entry: ancestor})
			rest = rest[length:]
		default:
			record.FieldID = fieldID
			record.Entry, err = decodeFieldEntry(FieldRef{Engine: engine}, rest, nil)
			return record, err == nil, err
		}
	}
}

// FieldBlobDumpDocument is a blobStore field element's metadata as a dump
// emits it: the document a blob bucket's dump emits for a blob, never its
// content.
func FieldBlobDumpDocument(snap storage.KVSnapshot, slot uint16, databaseID, id uint64, blobID []byte) ([]byte, error) {
	encoded, err := snap.Get(storage.Key(slot, databaseID, id, storage.NSBlobMeta, blobID))
	if err != nil {
		return nil, fmt.Errorf("reading the metadata of blob %s: %w", blobID, err)
	}
	return blobDumpDocument(string(blobID), encoded)
}
