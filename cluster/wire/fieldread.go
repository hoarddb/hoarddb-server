package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// FieldRead is a read of a whole store field that is not a peek: the document it belongs
// to and the field's id and engine, resolved from the declaration by the caller.
type FieldRead struct {
	Request ReadRequest
	FieldID uint16
	Engine  store.FieldEngine
	// After and Limit page a list: the cursor of the previous page, and the most elements.
	// A count ignores both.
	After []byte
	Limit int
}

// FieldElements is one page of a store field.
type FieldElements struct {
	// Elements are the element documents, copied out of the snapshot.
	Elements [][]byte
	// Keys are the element keys (hash, btree) or entry ids (heap) of Elements, in the same
	// order; none for a fifo or lifo store.
	Keys []string
	// Priorities are the priorities of Elements, heap only, in the same order as Keys.
	Priorities []int64
	// More says an element lies after the last returned.
	More bool
	// Next is the store's resume point after the last element, nil when there are none.
	Next []byte
}

// add appends one entry of a field of engine: its element, and the key (and for a heap the
// priority) a reader needs to tell elements apart.
func (e *FieldElements) add(engine store.FieldEngine, entry store.FieldEntry) {
	e.Elements = append(e.Elements, bytes.Clone(entry.Value))
	switch engine {
	case store.FieldHash, store.FieldBTree:
		e.Keys = append(e.Keys, entry.ElementKey)
	case store.FieldHeap:
		e.Keys = append(e.Keys, entry.EntryID)
		e.Priorities = append(e.Priorities, entry.Priority)
	}
}

// routeField is routeRead for a read of one store field of a document: on the owner it runs
// body on a snapshot of the owner's own store under the gate a document read goes through,
// checked before and after, so a lease, hold, owner or declaration that moved under the read
// is refused. A node that is not the owner sends the read to the owner and a held pair parks
// it, exactly as a document read does (BUG-0766); send does the forward. A deleted or absent
// document is NOT_FOUND unless absent says what an absent document answers. It takes no
// position and writes nothing.
//
// Every verb that reads a field goes through here (list, count, get, peek, lease_get, and
// nearest on the branch that has it), so one change to the routing is a change to all of them.
func routeField[T any](ctx context.Context, r *Reader, field fieldTarget, body func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (T, error),
	absent func() (T, error), send func(ctx context.Context, owner string, hopped ReadRequest) (T, error)) (T, error) {
	return routeFieldAdmitted(ctx, r, field, body, absent, send)
}

// routeFieldAdmitted is routeField for a read that needs the declaration the read was admitted with,
// as an aggregate read does: the owner resolves the aggregate from its own declaration, never from the
// sender's.
func routeFieldAdmitted[T any](ctx context.Context, r *Reader, field fieldTarget, body func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (T, error),
	absent func() (T, error), send func(ctx context.Context, owner string, hopped ReadRequest) (T, error)) (T, error) {
	req := field.Request
	return routeRead(ctx, r, req, readRoute[T]{
		local: func(admitted ReadVerdict) (T, error) {
			var none T
			if err := r.checkForwardedField(req, field, admitted); err != nil {
				return none, err
			}
			snapshot := r.cfg.KV.NewSnapshot()
			defer snapshot.Discard()
			state, err := ReadHeaderState(snapshot, DocumentKey{Ref: storage.BucketRef{Slot: req.Slot, DatabaseID: req.DBID, BucketID: req.BucketID}, EncID: req.EncID})
			if err != nil {
				return none, fmt.Errorf("reading the header of %s: %w", req.idText(), err)
			}
			if state != HeaderLive {
				if absent != nil {
					return absent()
				}
				return none, &cluster.Error{Code: cluster.CodeNotFound, Message: cluster.CodeNotFound.Text(cluster.CondDocumentNotFound, r.cfg.Gate.refusalArgs(req))}
			}
			return body(snapshot, store.FieldRef{Owner: storage.Key(req.Slot, req.DBID, req.BucketID, storage.NSFieldStore, req.EncID), FieldID: field.FieldID, Engine: field.Engine}, admitted)
		},
		forward: send,
	})
}

// fieldTarget is the document and field a store read names, the part of every field read
// the routing needs.
type fieldTarget struct {
	Request ReadRequest
	FieldID uint16
	Engine  store.FieldEngine
}

// ListField returns one page of the field's elements in the store's own order: arrival
// order for a fifoStore and a lifoStore, highest priority first for a heapStore, element
// key order for a hashStore and a btreeStore. It does not take the elements out.
func (r *Reader) ListField(ctx context.Context, field FieldRead) (FieldElements, error) {
	return routeField(ctx, r, field.target(), func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (FieldElements, error) {
		var page FieldElements
		entries, more, err := store.FieldList(snapshot, ref, store.FieldListRequest{After: field.After, Limit: field.Limit, MaxBytes: r.replyBytes(field.Request)})
		if err != nil {
			return FieldElements{}, fmt.Errorf("listing field %d of %s: %w", field.FieldID, field.Request.idText(), err)
		}
		isExpired := r.expiredOn(admitted.Expire, field.FieldID)
		for _, entry := range entries {
			if isExpired != nil && isExpired(entry) {
				continue
			}
			page.add(field.Engine, entry)
		}
		page.More = more
		if len(entries) > 0 {
			page.Next = bytes.Clone(entries[len(entries)-1].Remainder)
		}
		return page, nil
	}, nil, func(ctx context.Context, owner string, hopped ReadRequest) (FieldElements, error) {
		answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpList, FieldID: field.FieldID, Engine: field.Engine, After: field.After, Limit: field.Limit})
		return answer.Page, err
	})
}

// target is the part of a FieldRead the routing needs.
func (f FieldRead) target() fieldTarget {
	return fieldTarget{Request: f.Request, FieldID: f.FieldID, Engine: f.Engine}
}

// CountField returns the field's element count from its own state: no scan.
func (r *Reader) CountField(ctx context.Context, field FieldRead) (int64, error) {
	return routeField(ctx, r, field.target(), func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (int64, error) {
		if isExpired := r.expiredOn(admitted.Expire, field.FieldID); isExpired != nil && (ref.Engine == store.FieldFIFO || ref.Engine == store.FieldLIFO) {
			// An expiring queue's count is a walk: the elements past their time are not in it.
			total, err := store.FieldLen(snapshot, ref)
			if err != nil {
				return 0, fmt.Errorf("counting field %d of %s: %w", field.FieldID, field.Request.idText(), err)
			}
			var live int64
			if err := store.FieldPeekN(snapshot, ref, int(min(total, uint64(math.MaxInt32))), func(entry store.FieldEntry) error {
				if !isExpired(entry) {
					live++
				}
				return nil
			}); err != nil {
				return 0, fmt.Errorf("counting field %d of %s: %w", field.FieldID, field.Request.idText(), err)
			}
			return live, nil
		}
		count, err := store.FieldLen(snapshot, ref)
		if err != nil {
			return 0, fmt.Errorf("counting field %d of %s: %w", field.FieldID, field.Request.idText(), err)
		}
		return int64(count), nil
	}, nil, func(ctx context.Context, owner string, hopped ReadRequest) (int64, error) {
		answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpCount, FieldID: field.FieldID, Engine: field.Engine})
		return answer.Total, err
	})
}

// GetField returns the element a hashStore or btreeStore field holds under key, or a
// heapStore field holds under entry id key. found is false for an absent key. It is O(log n)
// in the store for a hash or btree and a walk of that one heap store, as a take by entry id
// is; it takes no position and writes nothing.
func (r *Reader) GetField(ctx context.Context, field FieldRead, key string) (element []byte, found bool, err error) {
	got, err := routeField(ctx, r, field.target(), func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (fieldGot, error) {
		var read fieldGot
		var readErr error
		if ref.Engine == store.FieldHeap {
			read.element, read.found, readErr = heapGet(snapshot, ref, key)
		} else {
			read.element, read.found, readErr = store.FieldGet(snapshot, ref, key)
			read.element = bytes.Clone(read.element)
		}
		if readErr != nil {
			return fieldGot{}, fmt.Errorf("reading key %q of field %d of %s: %w", key, field.FieldID, field.Request.idText(), readErr)
		}
		return read, nil
	}, nil, func(ctx context.Context, owner string, hopped ReadRequest) (fieldGot, error) {
		answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpGet, FieldID: field.FieldID, Engine: field.Engine, Key: key})
		return fieldGot{element: answer.Element, found: answer.Found}, err
	})
	return got.element, got.found, err
}

// fieldGot is what a keyed read of a field found.
type fieldGot struct {
	element []byte
	found   bool
}

// maxHeapGetSteps is the most heap entries one keyed read walks. A heap has no index by entry
// id, so a read of one walks the heap in pop order (build SPEC-GAP U85-6); past this many
// entries it is refused, so a reader cannot be made to scan a huge heap by one request. It is a
// variable so a test can lower it.
var maxHeapGetSteps = 100_000

// heapGet finds entry id in a heap by walking at most maxHeapGetSteps entries in pop order. A
// miss after a full walk of a larger heap is a refusal, not "not found": the entry may lie
// further on.
func heapGet(snapshot storage.KVSnapshot, ref store.FieldRef, entryID string) ([]byte, bool, error) {
	count, err := store.FieldLen(snapshot, ref)
	if err != nil || count == 0 {
		return nil, false, err
	}
	var element []byte
	err = store.FieldPeekN(snapshot, ref, int(min(count, uint64(maxHeapGetSteps))), func(entry store.FieldEntry) error {
		if entry.EntryID != entryID {
			return nil
		}
		element = bytes.Clone(entry.Value)
		return errHeapGetFound
	})
	switch {
	case errors.Is(err, errHeapGetFound):
		return element, true, nil
	case err != nil:
		return nil, false, err
	case count > uint64(maxHeapGetSteps):
		return nil, false, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"this heap holds %d entries, too large for a keyed read: a read by entry id walks at most %d. Nothing was read. Use list with a cursor to page through it",
			count, maxHeapGetSteps)}
	}
	return nil, false, nil
}

var errHeapGetFound = errors.New("the entry was found")

// expiredOn returns the predicate telling an element of fieldID is past its time at the owner's
// clock, from the expiry table of the declaration the read was admitted under; nil when the
// field does not expire its elements.
func (r *Reader) expiredOn(table machine.ExpireTable, fieldID uint16) func(store.FieldEntry) bool {
	for _, expiring := range table.Stores {
		if expiring.FieldID == fieldID {
			item := cluster.ExpireItem{FieldID: fieldID, Path: []byte(expiring.Path)}
			return expiredElement(item, clockOrSystem(r.cfg.WallMillis)())
		}
	}
	return nil
}
