package wire

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// maxPeekCount is the most elements one peek returns (the pop limit cap of 100).
const maxPeekCount = 100

// fieldCall is a storeCall whose field's store type is known to take the verb asked.
type fieldCall struct {
	storeCall
	engine store.FieldEngine
}

// engineType is the declared store type of the field, as a developer wrote it.
func (c fieldCall) engineType() string { return c.engine.String() + "Store" }

// verbStoreTypes is, per verb, the store types it takes (conventions section 9.1), worded
// for a refusal.
var verbStoreTypes = map[string]string{
	"push": "a fifoStore, lifoStore or heapStore",
	"pop":  "a fifoStore, lifoStore or heapStore",
	"peek": "a fifoStore, lifoStore or heapStore",
	"put":  "a hashStore or btreeStore",
	"inc":  "a hashStore or btreeStore",
	"take": "a hashStore, btreeStore or heapStore",
	// A lease is a delivery of messages in order, which only the two queues have.
	"claim": "a fifoStore or lifoStore",
	"ack":   "a fifoStore or lifoStore",
	// Release and extend act on a lease, so they take what ack takes.
	"release": "a fifoStore or lifoStore",
	"extend":  "a fifoStore or lifoStore",
	// A lease read names a lease, which only a claim makes.
	"lease_get": "a fifoStore or lifoStore",
	// A keyed read names an element key or an entry id, which a queue has neither of.
	"get": "a hashStore, btreeStore or heapStore",
	// Reads of a whole field: a blobStore holds payload files, which a list cannot page.
	"list": "a fifoStore, lifoStore, heapStore, hashStore or btreeStore",
	// A nearest scores the elements of one store, so it takes what list takes.
	"nearest": "a fifoStore, lifoStore, heapStore, hashStore or btreeStore",
	"count":   "any store field",
	// An aggregate is a sum or count of element fields, which a blob element has none of.
	"aggregate": "a fifoStore, lifoStore, heapStore, hashStore or btreeStore",
}

// engineVerbs is, per store type, the verbs it takes, worded for a refusal.
var engineVerbs = map[store.FieldEngine]string{
	store.FieldFIFO:  "push, pop, peek, list, nearest, count, claim, ack, release, extend and lease_get",
	store.FieldLIFO:  "push, pop, peek, list, nearest, count, claim, ack, release, extend and lease_get",
	store.FieldHeap:  "push, pop, peek, get, list, nearest, count and take",
	store.FieldHash:  "put, inc, take, get, list, nearest and count",
	store.FieldBTree: "put, inc, take, get, list, nearest and count",
	store.FieldBlob:  "count; none of push, pop, peek, list, nearest, put or take in this build",
}

// takesVerb reports whether a store type takes verb.
func takesVerb(engine store.FieldEngine, verb string) bool {
	switch verb {
	case "push", "pop", "peek":
		return engine == store.FieldFIFO || engine == store.FieldLIFO || engine == store.FieldHeap
	case "put", "inc":
		return engine == store.FieldHash || engine == store.FieldBTree
	case "take", "get":
		return engine == store.FieldHash || engine == store.FieldBTree || engine == store.FieldHeap
	case "claim", "ack", "release", "extend", "lease_get":
		return leasable(engine)
	case "list", "nearest", "aggregate":
		return engine != store.FieldBlob
	case "count":
		return true
	}
	return false
}

// leasable reports whether a store type can be claimed: the two queues.
func leasable(engine store.FieldEngine) bool {
	return engine == store.FieldFIFO || engine == store.FieldLIFO
}

// fieldCall resolves target to a store field that takes verb, or refuses before any
// position naming the field's store type and the verbs that do fit it.
func (n *Node) fieldCall(ctx context.Context, target StoreTarget, verb string) (fieldCall, error) {
	call, err := n.storeCall(ctx, target)
	if err != nil {
		return fieldCall{}, err
	}
	return call.takes(verb)
}

// takes checks that the resolved field's store type takes verb. The ring dispatcher
// resolves a field by id and comes here too, so both surfaces refuse with one text.
func (c storeCall) takes(verb string) (fieldCall, error) {
	entry, _ := entryByID(c.ref.table, c.fieldID)
	engine, _ := engineOfType(entry.Type)
	if takesVerb(engine, verb) {
		return fieldCall{storeCall: c, engine: engine}, nil
	}
	message := fmt.Sprintf(
		"%s on %s.%s.%s: the field is a %s, and %s takes %s. Nothing was changed. A %s takes %s",
		verb, c.ref.db, c.ref.name, entry.Name, entry.Type, verb, verbStoreTypes[verb], entry.Type, engineVerbs[engine])
	if verb == "inc" {
		message += ". To add to a number inside a document field, use update's $inc for a document field (help update)"
	}
	return fieldCall{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: message}
}

// entryByID is the declared field with the given id.
func entryByID(table machine.FieldTable, id uint16) (machine.FieldEntry, bool) {
	for _, entry := range table.Entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return machine.FieldEntry{}, false
}

// PopRequest removes the next element of a store field.
type PopRequest struct {
	StoreTarget
}

// Popped is what a pop returns.
type Popped struct {
	// Found is false when the field was empty; Element is then nil.
	Found bool
	// Element is the removed element as BSON.
	Element []byte
	// Pos is the log position the pop holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// Pop removes and returns the next element of a fifoStore (oldest), lifoStore (newest) or
// heapStore (highest priority) field of a document. It is a logged, fenced owner write: the
// element is recorded in the dedupe entry, so a retry with the same request id returns it
// again instead of removing another.
func (n *Node) Pop(ctx context.Context, req PopRequest, options ...WriteOption) (Popped, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return Popped{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "pop")
	if err != nil {
		return Popped{}, err
	}
	answer, err := n.sendVerb(ctx, call, cluster.VerbPop, nil, opts)
	if err != nil {
		return Popped{}, err
	}
	return Popped{Found: len(answer.Result) > 0, Element: answer.Result, Pos: answer.Pos, Deduped: answer.Deduped}, nil
}

// PeekRequest reads the next elements of a store field without removing them.
type PeekRequest struct {
	StoreTarget
	// Count is how many elements to read, 1 to 100.
	Count int
}

// Peek returns up to req.Count elements of a fifoStore, lifoStore or heapStore field in
// the order pops would take them. It is a read: no position, no log record.
func (n *Node) Peek(ctx context.Context, req PeekRequest) ([][]byte, error) {
	if err := checkPeekCount(req.Count, req.Database, req.Bucket, req.Field); err != nil {
		return nil, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "peek")
	if err != nil {
		return nil, err
	}
	peeked, err := n.peekField(ctx, call, req.ID, req.Count)
	return peeked.Elements, err
}

// checkPeekCount refuses a peek count this build does not take.
func checkPeekCount(count int, db, bucket, field string) error {
	if count >= 1 && count <= maxPeekCount {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
		"a peek of %d elements on %s.%s.%s is not one this build takes. Peek between 1 and %d elements",
		count, db, bucket, field, maxPeekCount)}
}

// peekField reads the next count elements of a resolved field; id is how the caller
// spelled the document, for refusal texts, and may be empty.
func (n *Node) peekField(ctx context.Context, call fieldCall, id string, count int) (FieldElements, error) {
	slot := call.ref.slot(call.encID)
	n.own.Acquire(slot, call.ref.dbID)
	elements, err := n.reader.Peek(ctx, PeekField{
		Request: ReadRequest{Slot: slot, DBID: call.ref.dbID, BucketID: call.ref.entry.Decl.BucketID, EncID: call.encID, ID: id},
		FieldID: call.fieldID, Engine: call.engine, Count: count,
	})
	if err != nil {
		return FieldElements{}, fmt.Errorf("peeking %s.%s of %q: %w", call.ref.name, call.fieldName(), id, err)
	}
	return elements, nil
}

// fieldName is the declared name of the resolved field.
func (c storeCall) fieldName() string {
	entry, _ := entryByID(c.ref.table, c.fieldID)
	return entry.Name
}

// PopNRequest removes up to Count elements of a store field in one logged write.
type PopNRequest struct {
	StoreTarget
	// Count is how many elements to remove at most, 1 to 100.
	Count int
}

// PoppedN is what a pop of several elements returns.
type PoppedN struct {
	// Elements are the removed elements in pop order; none when the field was empty.
	Elements [][]byte
	// Pos is the log position the pop holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// PopN removes and returns up to req.Count elements of a fifoStore (oldest first), lifoStore
// (newest first) or heapStore (highest priority first) field in one record. The whole result
// must fit a dedupe entry: a pop whose elements would not is refused before a position, with
// the count that does fit. The elements are recorded in the dedupe entry, so a retry with the
// same request id returns them again instead of removing more.
func (n *Node) PopN(ctx context.Context, req PopNRequest, options ...WriteOption) (PoppedN, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return PoppedN{}, err
	}
	if err := checkPopN(req.Count, req.Database, req.Bucket, req.Field); err != nil {
		return PoppedN{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "pop")
	if err != nil {
		return PoppedN{}, err
	}
	arg, err := encodePopArg(req.Count)
	if err != nil {
		return PoppedN{}, err
	}
	answer, err := n.sendVerb(ctx, call, cluster.VerbPop, arg, opts)
	if err != nil {
		return PoppedN{}, err
	}
	elements, err := unpackPopped(answer.Result)
	if err != nil {
		return PoppedN{}, fmt.Errorf("reading the elements popped from %s.%s of %q: %w", call.ref.name, call.fieldName(), req.ID, err)
	}
	return PoppedN{Elements: elements, Pos: answer.Pos, Deduped: answer.Deduped}, nil
}

// checkPopN refuses a pop count this build does not take.
func checkPopN(count int, db, bucket, field string) error {
	if count >= 1 && count <= maxPeekCount {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
		"a pop of %d elements on %s.%s.%s is not one this build takes. Nothing was removed. Pop between 1 and %d elements",
		count, db, bucket, field, maxPeekCount)}
}
