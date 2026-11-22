package wire

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// PeekField is a store field to read without removing: the document it belongs to and
// the field's id and engine, resolved from the declaration by the caller.
type PeekField struct {
	Request ReadRequest
	FieldID uint16
	Engine  store.FieldEngine
	// Count is how many elements to return.
	Count int
}

// Peek returns the elements a pop would take next, in pop order, from the owner's own
// store. It takes no position and writes nothing (write section 10: reads never appear in
// the log). The gate is the one a document read goes through, checked before and after the
// snapshot, so a lease, hold, owner or declaration that moved under the read is refused. A
// node that is not the owner forwards the read to the owner and a held pair parks it, as a
// document read does (BUG-0766).
func (r *Reader) Peek(ctx context.Context, field PeekField) (FieldElements, error) {
	return routeField(ctx, r, fieldTarget{Request: field.Request, FieldID: field.FieldID, Engine: field.Engine},
		func(snapshot storage.KVSnapshot, ref store.FieldRef, admitted ReadVerdict) (FieldElements, error) {
			return r.peekElements(snapshot, ref, field, admitted.Expire)
		}, nil, func(ctx context.Context, owner string, hopped ReadRequest) (FieldElements, error) {
			answer, err := r.cfg.Forward.ForwardFieldRead(ctx, owner, FieldForward{Req: hopped, Op: FieldOpPeek, FieldID: field.FieldID, Engine: field.Engine, Count: field.Count})
			return answer.Page, err
		})
}

// peekReplyBytes is the most element bytes one peek reads: a frame holds maxSubScanBody
// bytes and the reply's own members and the array framing take the rest. A peek reads
// elements of up to 1 MiB each, so without this bound 100 of them are 100 MiB, built for a
// reply the frame cannot carry.
const peekReplyBytes = maxSubScanBody - 64<<10

// peekElementFraming is what one element adds to a reply besides its bytes: the array
// entries of elements, keys and priorities (type byte, index text, terminator, length).
const peekElementFraming = 48

// peekEntryBytes is what one peeked entry takes in the reply: its element, and the key and
// priority a keyed store sends with it. The remainder holds the key (encoded, so never shorter
// than the key) and a heap's priority.
func peekEntryBytes(entry store.FieldEntry) int {
	return len(entry.Value) + len(entry.Remainder) + peekElementFraming
}

func (r *Reader) peekElements(snapshot storage.KVSnapshot, ref store.FieldRef, field PeekField, expire machine.ExpireTable) (FieldElements, error) {
	req := field.Request
	limit := r.replyBytes(req)
	var elements FieldElements
	var held int
	err := r.peekLive(snapshot, ref, field, expire, func(entry store.FieldEntry) error {
		if held += peekEntryBytes(entry); held > limit {
			return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"the next %d elements of field %d of %s hold more than the %d bytes one reply carries. Nothing was read. Peek fewer elements",
				field.Count, field.FieldID, req.idText(), limit)}
		}
		elements.add(field.Engine, entry)
		return nil
	})
	if err != nil {
		return FieldElements{}, fmt.Errorf("peeking field %d of %s: %w", field.FieldID, req.idText(), err)
	}
	return elements, nil
}

// peekLive is FieldPeekN that steps over the elements of an expiring queue that are past the
// owner's clock, removing nothing: a peek is a read, and the next pop removes them.
func (r *Reader) peekLive(snapshot storage.KVSnapshot, ref store.FieldRef, field PeekField, expire machine.ExpireTable, visit func(store.FieldEntry) error) error {
	// expire is the table of the declaration the gate admitted this read under: the cache is
	// bounded and is never read a second time.
	for _, expiring := range expire.Stores {
		if expiring.FieldID == field.FieldID {
			item := cluster.ExpireItem{FieldID: expiring.FieldID, Path: []byte(expiring.Path)}
			return store.FieldPeekLive(snapshot, ref, field.Count, expiredElement(item, clockOrSystem(r.cfg.WallMillis)()), visit)
		}
	}
	return store.FieldPeekN(snapshot, ref, field.Count, visit)
}
