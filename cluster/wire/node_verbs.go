package wire

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// sendVerb writes one STORE_VERB with arg through the owner write path.
func (n *Node) sendVerb(ctx context.Context, call fieldCall, verb byte, arg []byte, opts writeOptions) (WriteAnswer, error) {
	op := call.ref.template()
	op.Class, op.Fields = cluster.OpStoreVerb, []cluster.FieldOp{{FieldID: call.fieldID, Verb: verb, Value: arg}}
	answer, err := n.send(ctx, call.ref, call.encID, op, opts)
	if verb == cluster.VerbInc {
		n.Incs.count(answer, err)
	}
	return answer, err
}

// PutRequest sets one key of a hashStore or btreeStore field.
type PutRequest struct {
	StoreTarget
	// Key is the element key, 1 to 1024 bytes.
	Key string
	// Element is the document stored under Key; a key already present is replaced.
	Element bson.D
}

// Put sets a key of a hashStore or btreeStore field of a document, replacing the element
// the key held. It is a logged, fenced owner write.
func (n *Node) Put(ctx context.Context, req PutRequest, options ...WriteOption) error {
	opts, err := newWriteOptions(options)
	if err != nil {
		return err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "put")
	if err != nil {
		return err
	}
	raw, err := bson.Marshal(req.Element)
	if err != nil {
		return fmt.Errorf("encoding the element for %s.%s: %w", req.Bucket, req.Field, err)
	}
	arg, err := encodePutArg(req.Key, raw)
	if err != nil {
		return fmt.Errorf("putting into %s.%s: %w", req.Bucket, req.Field, err)
	}
	_, err = n.sendVerb(ctx, call, cluster.VerbPut, arg, opts)
	return err
}

// TakeRequest removes one element of a store field.
type TakeRequest struct {
	StoreTarget
	// Key is the element key of a hashStore or btreeStore, or the entry id of a heapStore;
	// 1 to 1024 bytes.
	Key string
}

// Taken is what a take returns.
type Taken struct {
	// Found is false when nothing was stored under the key; Element is then nil.
	Found bool
	// Element is the removed element as BSON.
	Element []byte
	// Pos is the log position the take holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// Take removes and returns one element of a hashStore or btreeStore field by key, or of a
// heapStore field by entry id. An absent key is not an error: Found is false. It is a
// logged, fenced owner write that records the removed element in the dedupe entry, so a
// retry with the same request id returns it again instead of removing another.
func (n *Node) Take(ctx context.Context, req TakeRequest, options ...WriteOption) (Taken, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return Taken{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "take")
	if err != nil {
		return Taken{}, err
	}
	var arg []byte
	if call.engine == store.FieldHeap {
		arg, err = encodeTakeEntryArg(req.Key)
	} else {
		arg, err = encodeTakeKeyArg(req.Key)
	}
	if err != nil {
		return Taken{}, fmt.Errorf("taking from %s.%s: %w", req.Bucket, req.Field, err)
	}
	answer, err := n.sendVerb(ctx, call, cluster.VerbTake, arg, opts)
	if err != nil {
		return Taken{}, err
	}
	return Taken{Found: len(answer.Result) > 0, Element: answer.Result, Pos: answer.Pos, Deduped: answer.Deduped}, nil
}

// IncRequest adds to the numbers of one element of a hashStore or btreeStore field.
type IncRequest struct {
	StoreTarget
	// Key is the element key, 1 to 1024 bytes; an absent key is created.
	Key string
	// Increments are dotted key paths to int32, int64 or finite double amounts.
	Increments bson.D
	// ReturnAfter asks for the element as applied.
	ReturnAfter bool
}

// Incremented is what an inc returns.
type Incremented struct {
	// Element is the element as applied, set only when the request asked for it.
	Element []byte
	// Pos is the log position the inc holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// Inc adds the increments to the element stored under key in a hashStore or btreeStore field,
// creating the element when the key is absent. It is a logged, fenced owner write: the add is
// computed at the owner under the pair's section and a copy repeats it from the record.
// A retry with the same request id returns the recorded answer, but only until the pair has applied
// replication.dedupe_window (8192) later records; a retry after that adds again.
func (n *Node) Inc(ctx context.Context, req IncRequest, options ...WriteOption) (Incremented, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return Incremented{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "inc")
	if err != nil {
		return Incremented{}, err
	}
	increments, err := bson.Marshal(req.Increments)
	if err != nil {
		return Incremented{}, fmt.Errorf("encoding the increments for %s.%s: %w", req.Bucket, req.Field, err)
	}
	arg, err := IncArg(req.Key, increments, req.ReturnAfter)
	if err != nil {
		return Incremented{}, fmt.Errorf("incrementing %s.%s: %w", req.Bucket, req.Field, err)
	}
	answer, err := n.sendVerb(ctx, call, cluster.VerbInc, arg, opts)
	if err != nil {
		return Incremented{}, err
	}
	return Incremented{Element: answer.Result, Pos: answer.Pos, Deduped: answer.Deduped}, nil
}
