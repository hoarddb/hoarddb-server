package wire

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// The page bounds of a store list: the cap server.FieldListRequest already clamps to, and
// the same default.
const (
	defaultFieldPage = 100
	maxFieldPage     = 1000
)

// FieldListRequest asks for one page of a store field's elements.
type FieldListRequest struct {
	StoreTarget
	// After is the cursor of the previous page; nil starts at the first element.
	After []byte
	// Limit is the page size, 1 to 1000; 0 means 100.
	Limit int
}

// FieldPage is one page of a store field.
type FieldPage struct {
	// Elements are the element documents in the store's own order.
	Elements [][]byte
	// Keys are the element keys (hash, btree) or entry ids (heap) of Elements; none for a
	// fifo or lifo store.
	Keys []string
	// Priorities are the priorities of Elements, heap only.
	Priorities []int64
	// More says another page follows; Cursor is then set.
	More bool
	// Cursor is the opaque resume point after the last element.
	Cursor []byte
}

// ListField returns one page of a fifoStore, lifoStore, heapStore, hashStore or btreeStore
// field of a document, in the order the store keeps it, and the cursor that continues it.
// It is a read: no position, no log record, nothing removed. The cost is the page, not the
// field.
func (n *Node) ListField(ctx context.Context, req FieldListRequest) (FieldPage, error) {
	call, err := n.fieldCall(ctx, req.StoreTarget, "list")
	if err != nil {
		return FieldPage{}, err
	}
	return n.listFieldOf(ctx, call, req.ID, req.After, req.Limit)
}

// listFieldOf reads a page of a resolved field; id is how the caller spelled the document,
// for refusal texts, and may be empty.
func (n *Node) listFieldOf(ctx context.Context, call fieldCall, id string, after []byte, limit int) (FieldPage, error) {
	limit, err := fieldPageLimit(limit, call)
	if err != nil {
		return FieldPage{}, err
	}
	if err := checkFieldCursor(after, call); err != nil {
		return FieldPage{}, err
	}
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	page, err := n.reader.ListField(ctx, FieldRead{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID, ID: id},
		FieldID: call.fieldID, Engine: call.engine, After: after, Limit: limit,
	})
	if err != nil {
		return FieldPage{}, fmt.Errorf("listing %s.%s of %q: %w", call.ref.name, call.fieldName(), id, err)
	}
	return FieldPage{Elements: page.Elements, Keys: page.Keys, Priorities: page.Priorities, More: page.More, Cursor: pageCursor(page)}, nil
}

// pageCursor is the cursor a page hands out: set exactly when another page follows.
func pageCursor(page FieldElements) []byte {
	if !page.More {
		return nil
	}
	return page.Next
}

func fieldPageLimit(limit int, call fieldCall) (int, error) {
	switch {
	case limit == 0:
		return defaultFieldPage, nil
	case limit < 0 || limit > maxFieldPage:
		return 0, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"a list of %d elements of %s.%s.%s is not one this build takes. Nothing was read. List between 1 and %d elements, or leave the limit out for %d",
			limit, call.ref.db, call.ref.name, call.fieldName(), maxFieldPage, defaultFieldPage)}
	}
	return limit, nil
}

// checkFieldCursor refuses a cursor that cannot be one of this field's pages. A queue's
// resume point is an 8-byte position and a heap's a priority and an entry id; a keyed
// field's is an element key, which any bytes can spell, so it is not checked.
func checkFieldCursor(after []byte, call fieldCall) error {
	if len(after) == 0 {
		return nil
	}
	fits := true
	switch call.engine {
	case store.FieldFIFO, store.FieldLIFO:
		fits = len(after) == 8
	case store.FieldHeap:
		fits = len(after) >= 9
	}
	if fits {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
		"the cursor of %s.%s.%s is %d bytes, which no page of a %s returned. Nothing was read. Continue with the cursor the previous page returned, or list again without one",
		call.ref.db, call.ref.name, call.fieldName(), len(after), call.engineType())}
}

// CountField returns the number of elements of a store field of a document, of any store
// type. It is a read of the field's own count: no scan, no position.
func (n *Node) CountField(ctx context.Context, target StoreTarget) (int64, error) {
	call, err := n.fieldCall(ctx, target, "count")
	if err != nil {
		return 0, err
	}
	return n.countFieldOf(ctx, call, target.ID)
}

func (n *Node) countFieldOf(ctx context.Context, call fieldCall, id string) (int64, error) {
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	total, err := n.reader.CountField(ctx, FieldRead{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID, ID: id},
		FieldID: call.fieldID, Engine: call.engine,
	})
	if err != nil {
		return 0, fmt.Errorf("counting %s.%s of %q: %w", call.ref.name, call.fieldName(), id, err)
	}
	return total, nil
}

// StoreGetRequest reads one element of a hashStore, btreeStore or heapStore field.
type StoreGetRequest struct {
	StoreTarget
	// Key is the element key of a hash or btree, the entry id of a heap; 1 to 1024 bytes.
	Key string
}

// StoreGot is what a keyed read found.
type StoreGot struct {
	// Found is false when the field holds no such key; Element is then nil.
	Found bool
	// Element is the element as BSON.
	Element []byte
}

// GetField returns the element a hash, btree or heap field of a document holds under key.
// It is a read: no position, no log record, nothing removed.
func (n *Node) GetField(ctx context.Context, req StoreGetRequest) (StoreGot, error) {
	call, err := n.fieldCall(ctx, req.StoreTarget, "get")
	if err != nil {
		return StoreGot{}, err
	}
	return n.getFieldOf(ctx, call, req.ID, req.Key)
}

// getFieldOf reads one element of a resolved field; id is how the caller spelled the
// document, for refusal texts, and may be empty.
func (n *Node) getFieldOf(ctx context.Context, call fieldCall, id, key string) (StoreGot, error) {
	if len(key) < 1 || len(key) > maxVerbKeyBytes || !utf8.ValidString(key) {
		return StoreGot{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"the key %q of %s.%s.%s is %d bytes. Nothing was read. Read a key of 1 to %d bytes of valid UTF-8",
			key, call.ref.db, call.ref.name, call.fieldName(), len(key), maxVerbKeyBytes)}
	}
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	element, found, err := n.reader.GetField(ctx, FieldRead{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID, ID: id},
		FieldID: call.fieldID, Engine: call.engine,
	}, key)
	if err != nil {
		return StoreGot{}, fmt.Errorf("reading %s.%s of %q: %w", call.ref.name, call.fieldName(), id, err)
	}
	return StoreGot{Found: found, Element: element}, nil
}
