package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The field-store verbs (nested buckets, GAP-22; spec §4.3, §10.2). A field
// write is an ordinary write to its parent bucket: it takes the bucket's
// mutex, runs inside the bucket's Apply with the bucket's failed-apply carry,
// and is placed, replicated, fenced and replayed as the bucket's writes are.
// No lock of its own exists (spec §4.7). A field read is a read of the
// bucket's snapshot, like any other.

// The verbs, by the words spec §10.3's messages use.
const (
	fieldVerbPush   = "push"
	fieldVerbPop    = "pop"
	fieldVerbPeek   = "peek"
	fieldVerbPut    = "put"
	fieldVerbGet    = "get"
	fieldVerbDelete = "delete"
	fieldVerbLen    = "len"
	fieldVerbList   = "list"
)

// orderedFieldVerbs are the verbs of fifo, lifo and heap fields; keyed ones
// (hash, btree, blob) take put, get and delete. len and list are every
// engine's.
var orderedFieldVerbs = map[string]bool{fieldVerbPush: true, fieldVerbPop: true, fieldVerbPeek: true}

var keyedFieldVerbs = map[string]bool{fieldVerbPut: true, fieldVerbGet: true, fieldVerbDelete: true}

// Field-store list bounds: a page is at most maxFieldListLimit elements and
// at most what fits in one response frame.
const (
	defaultFieldListLimit = 100
	maxFieldListLimit     = 1000
	maxFieldListBytes     = maxDumpDocumentBytes
)

// resolvedField is one addressed store instance: the declaration and the
// store's reference to it.
type resolvedField struct {
	declared catalogStore
	ref      store.FieldRef
	// ancestorKey is the L2 element an L3 store hangs on, which must exist;
	// nil at L2.
	ancestorKey []byte
}

// keyed reports whether the field's engine addresses elements by key, which
// is Apply's Keyed: such a write may be replayed beneath the mark when it
// commutes.
func (f resolvedField) keyed() bool {
	return f.ref.Engine == store.FieldHash || f.ref.Engine == store.FieldBTree || f.ref.Engine == store.FieldBlob
}

// resolveField checks a request's address against the bucket's declaration
// and the verb against the field's engine. An L3 store is addressed through
// the L2 element it hangs on: Path is [<L2 field>, <L2 element key>].
func resolveField(b *inMemoryBucket, req FieldRequest, verb string) (resolvedField, error) {
	var missing []string
	for _, component := range []struct{ name, value string }{{"bucket", req.Bucket}, {"key", req.Key}, {"field", req.Field}} {
		if component.value == "" {
			missing = append(missing, component.name)
		}
	}
	if len(missing) > 0 {
		return resolvedField{}, fmt.Errorf("%w: a field-store %s names the bucket, the parent document's key and the field, and this one has no %s. "+
			`For example: db.Post["p1"].Comments.%s(...)`, ErrFieldAddressIncomplete, verb, strings.Join(missing, " and no "), verb)
	}
	declaration := b.storeFields()
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), req.Key)
	if err != nil {
		return resolvedField{}, err
	}
	field := resolvedField{}
	parent := int32(0)
	if len(req.Path) > 0 {
		if owner, parent, field.ancestorKey, err = resolveAncestor(declaration, req, owner); err != nil {
			return resolvedField{}, err
		}
	}
	declared, found := declaration.find(req.Field, parent)
	if !found {
		for _, nested := range declaration.stores {
			if parent == 0 && nested.Parent != 0 && nested.Name == req.Field {
				holder := declaration.byID(nested.Parent)
				return resolvedField{}, fmt.Errorf("%w: store field %q is declared on the elements of %q, so it is addressed through one of them: "+
					`db.%s[%q].%s[<element key>].%s.%s(...)`, ErrFieldAddressIncomplete, req.Field, holder.Name, req.Bucket, req.Key, holder.Name, req.Field, verb)
			}
		}
		return resolvedField{}, fmt.Errorf("%w: Bucket %q in database %q declares no store field named %q%s.\nIt declares: %s.\n"+
			"Run 'describe bucket %s in %s' to see the declaration",
			ErrFieldNotDeclared, req.Bucket, req.Database, req.Field, onLevel(declaration, parent), declaredEnginesAt(declaration, parent), req.Bucket, req.Database)
	}
	engine, err := store.ParseFieldEngine(declared.Engine)
	if err != nil {
		return resolvedField{}, fmt.Errorf("the catalog declares store field %q with engine %q: %w", declared.Name, declared.Engine, err)
	}
	ordered := engine == store.FieldFIFO || engine == store.FieldLIFO || engine == store.FieldHeap
	if (ordered && keyedFieldVerbs[verb]) || (!ordered && orderedFieldVerbs[verb]) {
		return resolvedField{}, wrongVerbErr(req, declaration, verb, declared.Name, engine, ordered)
	}
	field.declared = declared
	field.ref = store.FieldRef{Owner: owner, FieldID: uint16(declared.ID), Engine: engine, Nested: declaration.layoutOf(declared.ID)}
	return field, nil
}

// resolveAncestor reads an L3 address's path: the L2 field, declared on the
// documents, and its element's key. It returns the L3 stores' owner, the L2
// field's id and the L2 element's key, which must exist.
func resolveAncestor(declaration *storeFieldSet, req FieldRequest, owner []byte) ([]byte, int32, []byte, error) {
	l2Name, _ := req.Path[0].(string)
	if len(req.Path) != 2 || l2Name == "" {
		return nil, 0, nil, fmt.Errorf("%w: an L3 store is addressed through its L2 field and element: path is [<field>, <element key>], got %v. "+
			`For example: db.%s[%q].Comments[0].%s(...)`, ErrFieldAddressIncomplete, req.Path, req.Bucket, req.Key, req.Field)
	}
	l2, found := declaration.find(l2Name, 0)
	if !found {
		return nil, 0, nil, fmt.Errorf("%w: Bucket %q in database %q declares no store field named %q.\nIt declares: %s.\n"+
			"Run 'describe bucket %s in %s' to see the declaration",
			ErrFieldNotDeclared, req.Bucket, req.Database, l2Name, declaredEngines(declaration), req.Bucket, req.Database)
	}
	engine, err := store.ParseFieldEngine(l2.Engine)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("the catalog declares store field %q with engine %q: %w", l2.Name, l2.Engine, err)
	}
	remainder, err := pathElementRemainder(engine, req.Path[1])
	if err != nil {
		return nil, 0, nil, fmt.Errorf("%w: element %v of %q: %w", ErrFieldAddressIncomplete, req.Path[1], l2Name, err)
	}
	l2Prefix := store.FieldStorePrefix(owner, uint16(l2.ID))
	inner := storage.NSDocuments
	if engine == store.FieldFIFO || engine == store.FieldLIFO || engine == store.FieldHeap {
		inner = storage.NSQueue
	}
	return store.FieldElementAnchor(l2Prefix, remainder), l2.ID, store.FieldElementKey(l2Prefix, inner, remainder), nil
}

// pathElementRemainder is an L2 element's remainder from how a path names
// it: an element key (hash, btree), a position (fifo, lifo), or
// {priority, id} (heap).
func pathElementRemainder(engine store.FieldEngine, element interface{}) ([]byte, error) {
	switch engine {
	case store.FieldFIFO, store.FieldLIFO:
		position, err := optionInt32(element)
		if err != nil {
			return nil, fmt.Errorf("a %s element is named by its position: %w", engine, err)
		}
		if position < 0 {
			return nil, fmt.Errorf("a position is never negative, got %d", position)
		}
		return store.PositionRemainder(uint64(position)), nil
	case store.FieldHeap:
		fields, err := optionMap(element)
		if err != nil {
			return nil, fmt.Errorf("a heap element is named by {priority, id}: %w", err)
		}
		priority, err := optionInt64(fields["priority"])
		entryID, _ := fields["id"].(string)
		if err != nil || entryID == "" {
			return nil, fmt.Errorf("a heap element is named by {priority, id}, got %v", element)
		}
		return store.HeapRemainder(priority, entryID)
	case store.FieldBlob:
		return nil, fmt.Errorf("a blob element's content is not a document and holds no store fields")
	default:
		elementKey, ok := element.(string)
		if !ok {
			return nil, fmt.Errorf("a %s element is named by its element key, a string, got %T", engine, element)
		}
		return store.KeyedRemainder(elementKey)
	}
}

// wrongVerbErr is spec §10.3's wrong-verb refusal.
func wrongVerbErr(req FieldRequest, declaration *storeFieldSet, verb, field string, engine store.FieldEngine, ordered bool) error {
	takes, family := "push, pop and peek", "put, get and delete, which are for hashStore, btreeStore and blobStore"
	if !ordered {
		takes, family = "put/get/delete by element key", "push, pop and peek, which are for fifoStore, lifoStore and heapStore"
	}
	engineWord := engine.String()
	return fmt.Errorf("%w: %q is not an operation on %q, which is declared %sStore. %s fields take %s; %s is one of %s.\n"+
		"Declared engines on %q: %s",
		ErrFieldWrongVerb, verb, field, engineWord, strings.ToUpper(engineWord[:1])+engineWord[1:], takes, verb, family,
		req.Bucket, declaredEngines(declaration))
}

// declaredEngines lists a bucket's L2 store fields as spec §10.3 prints them:
// "Comments (fifo), Photos (blob)".
func declaredEngines(declaration *storeFieldSet) string {
	return declaredEnginesAt(declaration, 0)
}

// onLevel names the L2 store whose elements parent's stores are declared on,
// for a message: "" at L2.
func onLevel(declaration *storeFieldSet, parent int32) string {
	if parent == 0 {
		return ""
	}
	return fmt.Sprintf(" on the elements of %q", declaration.byID(parent).Name)
}

// declaredEnginesAt lists the store fields declared on one level.
func declaredEnginesAt(declaration *storeFieldSet, parent int32) string {
	var listed []string
	for _, declared := range declaration.stores {
		if declared.Parent == parent {
			listed = append(listed, fmt.Sprintf("%s (%s)", declared.Name, declared.Engine))
		}
	}
	if len(listed) == 0 {
		return "no store fields"
	}
	return strings.Join(listed, ", ")
}

// parentDocumentKey is the parent document's own key, in its bucket's layout.
func parentDocumentKey(b *inMemoryBucket, documentKey string) ([]byte, error) {
	if b.bucketType == BucketTypeBTree {
		encoded, err := store.EncodeComponent(documentKey)
		if err != nil {
			return nil, fmt.Errorf("encoding document key %q: %w", documentKey, err)
		}
		return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, encoded), nil
	}
	return storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, []byte(documentKey)), nil
}

// requireParent refuses an operation on a document that does not exist:
// a missing parent is not an empty store (spec §4.3). A write checks inside
// its Apply operation, so the check and the write read one snapshot under the
// bucket lock; a check outside would race the parent's delete cascade.
func requireParent(snap storage.KVSnapshot, b *inMemoryBucket, req FieldRequest, field resolvedField, action string) error {
	documentKey, err := parentDocumentKey(b, req.Key)
	if err != nil {
		return err
	}
	if field.ancestorKey != nil {
		if err := requireAncestor(snap, req, field, action); err != nil {
			return err
		}
	}
	_, err = snap.Get(documentKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return fmt.Errorf("%w: No document with key %q in bucket %q, so there is no %q store to %s. "+
			"A store field exists for each document, so the document has to exist first.\n"+
			"Run 'db.%s.find({_id_: %q})' to check the key",
			ErrFieldParentNotFound, req.Key, req.Bucket, req.Field, action, req.Bucket, req.Key)
	}
	if err != nil {
		return fmt.Errorf("reading parent document %q: %w", req.Key, err)
	}
	return nil
}

// requireAncestor refuses an L3 operation whose L2 element does not exist:
// the missing-parent rule one level down.
func requireAncestor(snap storage.KVSnapshot, req FieldRequest, field resolvedField, action string) error {
	_, err := snap.Get(field.ancestorKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return fmt.Errorf("%w: document %q in bucket %q has no %q element %v, so there is no %q store to %s. "+
			"A store declared on %s's elements exists for each of them, so the element has to exist first",
			ErrFieldParentNotFound, req.Key, req.Bucket, req.Path[0], req.Path[1], req.Field, action, req.Path[0])
	}
	if err != nil {
		return fmt.Errorf("reading the %v element %v of %q: %w", req.Path[0], req.Path[1], req.Key, err)
	}
	return nil
}

// writeField runs one field write: address and verb checks under the bucket
// mutex, then the parent check and op inside the bucket's Apply. A refusal is
// returned as the error; a store failure answers Success false, as the bucket
// verbs do.
func (s *Server) writeField(ctx context.Context, req FieldRequest, verb string, op func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error) (*FieldResponse, error) {
	return s.writeFieldThen(ctx, req, verb, op, nil)
}

// writeFieldThen is writeField with after, which runs under the bucket lock
// once the apply returned, with its error: a blob field's file work, which
// must follow the commit (unlink) or undo a put that committed nothing. Its
// own failure is a payload left for the orphan pass, logged, never the
// write's failure. The field verbs have no context-free form, so a direct
// call is answered here, durably, once the bucket lock is released
// (ack_sync.go).
func (s *Server) writeFieldThen(ctx context.Context, req FieldRequest, verb string, op func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error, after func(applyErr error) error) (*FieldResponse, error) {
	return acknowledgeDurably(ctx, s, func(ctx context.Context) (*FieldResponse, error) {
		return s.writeFieldLocked(ctx, req, verb, op, after)
	})
}

// writeFieldLocked is writeFieldThen's write, under the bucket lock.
func (s *Server) writeFieldLocked(ctx context.Context, req FieldRequest, verb string, op func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error, after func(applyErr error) error) (*FieldResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()
	field, err := resolveField(bucket, req, verb)
	if err != nil {
		return nil, err
	}
	resp := &FieldResponse{Success: true}
	var nestedPayloads []string
	_, applyErr := store.Apply(store.ApplyRequest{
		KV: bucket.kv, Slot: bucket.slot, DatabaseID: bucket.databaseID, ID: bucket.id.Load(), Keyed: field.keyed(),
		Info: applyInfoFrom(ctx, s), Owed: bucket.owedApplies(),
		Op: func(snap storage.KVSnapshot, st *store.BucketState, b storage.KVBatch) error {
			if err := requireParent(snap, bucket, req, field, "write"); err != nil {
				return err
			}
			return op(store.FieldWrite{Snap: snap, State: st, Batch: b, Payloads: &nestedPayloads}, field, resp)
		},
	})
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "FIELD " + strings.ToUpper(verb), emptyPop: verb == fieldVerbPop && !resp.Found}, applyErr)
	if after != nil {
		if err := s.notePayloadLeft(req.Database, req.Bucket, after(applyErr)); err != nil {
			return nil, err
		}
	}
	// An L2 element removed with its L3 blob stores: their metadata went in
	// the batch, their files follow the commit.
	if applyErr == nil {
		var unlinked error
		for _, payload := range nestedPayloads {
			unlinked = errors.Join(unlinked, store.UnlinkPayloadAfterCommit(s.kv, s.blobRoot(), payload))
		}
		if err := s.notePayloadLeft(req.Database, req.Bucket, unlinked); err != nil {
			return nil, err
		}
	}
	switch {
	case applyErr == nil:
		return resp, nil
	case errors.Is(applyErr, store.ErrApplyFailed), errors.Is(applyErr, storage.ErrBatchTooLarge):
		return &FieldResponse{Success: false, Message: failureMessage("field "+verb+" failed", applyErr)}, nil
	default:
		return nil, applyErr
	}
}

// readField runs one field read against the bucket's read snapshot.
func (s *Server) readField(req FieldRequest, verb string, read func(snap storage.KVSnapshot, field resolvedField) error) error {
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return err
	}
	defer snap.Discard()
	field, err := resolveField(bucket, req, verb)
	if err != nil {
		return err
	}
	if err := requireParent(snap, bucket, req, field, "read"); err != nil {
		return err
	}
	err = read(snap, field)
	// The element's storage key is out of the client's text; the log keeps it.
	s.logKeyReadFailure(err)
	return err
}

// elementDocument is a push's or put's element as stored: an _id_ present.
// The coordinator mints the _id_ and fixes now() (resolveDocumentIDs), so
// every replica stores the same element; an _id_ is minted here only for a
// caller that reached the server without a coordinator. The element is a copy:
// a blob field's put keeps data, without the minted _id_, as its metadata.
func elementDocument(data map[string]interface{}) (map[string]interface{}, []byte, error) {
	data = maps.Clone(data)
	if data == nil {
		data = make(map[string]interface{})
	}
	if id, _ := data["_id_"].(string); id == "" {
		minted, err := ids.NewDocumentID()
		if err != nil {
			return nil, nil, fmt.Errorf("generating element id: %w", err)
		}
		data["_id_"] = minted
	}
	encoded, err := bson.Marshal(data)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding the element: %w", err)
	}
	return data, encoded, nil
}

// wirePlacement is the position and priority an entry carries on the wire.
// Only the engines that have one answer it, and then as a pointer, so that
// position 0 (the first element of a fifo or lifo) and priority 0 (a heap's
// default) arrive as a real zero and not as an absent member (BUG-0558); the
// other engines answer nil, which the encoder omits.
func wirePlacement(entry store.FieldEntry, engine store.FieldEngine) (position, priority *int64) {
	switch engine {
	case store.FieldFIFO, store.FieldLIFO:
		return presentInt64(int64(entry.Position)), nil
	case store.FieldHeap:
		return nil, presentInt64(entry.Priority)
	default:
		return nil, nil
	}
}

// presentInt64 points at n, for a wire member whose zero is a value.
func presentInt64(n int64) *int64 { return &n }

// FieldPushCtx appends an element to a fifo or lifo field, or inserts it at
// its priority in a heap field, whose entry id is the element's _id_.
func (s *Server) FieldPushCtx(ctx context.Context, req FieldRequest) (*FieldResponse, error) {
	element, encoded, err := fieldPushElement(req)
	if err != nil {
		return nil, err
	}
	return s.writeField(ctx, req, fieldVerbPush, func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error {
		resp.EntryID = element["_id_"].(string)
		if field.ref.Engine == store.FieldHeap {
			resp.Priority = presentInt64(req.Priority)
			return store.FieldHeapPush(w, field.ref, req.Priority, resp.EntryID, encoded)
		}
		position, err := store.FieldPush(w, field.ref, encoded)
		resp.Position = presentInt64(int64(position))
		return err
	})
}

// fieldPushElement is a push's element, judged before the write: an empty
// _id_, a malformed element and a name no path can reach are refused. A
// spread router asks it of a push it answers without a part (unbornAnswer).
func fieldPushElement(req FieldRequest) (map[string]interface{}, []byte, error) {
	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, nil, err
	}
	element, encoded, err := elementDocument(req.Data)
	if err != nil {
		return nil, nil, err
	}
	if err := refuseUnreachableNames(element, req.Database, req.Bucket); err != nil {
		return nil, nil, err
	}
	return element, encoded, nil
}

// fieldElementKeyRefusal is a put's or a delete's refusal of a request that
// names no element key, nil for any other verb or a named key.
func fieldElementKeyRefusal(verb string, req FieldRequest) error {
	if req.Element != "" {
		return nil
	}
	switch verb {
	case fieldVerbPut:
		return fmt.Errorf("%w: put names the element key: db.%s[%q].%s.put(\"key\", {...})", ErrFieldAddressIncomplete, req.Bucket, req.Key, req.Field)
	case fieldVerbDelete:
		return fmt.Errorf("%w: delete names the element key: db.%s[%q].%s.delete(\"key\")", ErrFieldAddressIncomplete, req.Bucket, req.Key, req.Field)
	}
	return nil
}

// FieldPopCtx removes and returns the next req.Limit elements of the field, in
// pop order, each charged to the result budget before it is removed (the
// bucket form is Server.PopCtx, whose comments give the reasons).
func (s *Server) FieldPopCtx(ctx context.Context, req FieldPopRequest) (*FieldResponse, error) {
	var limit int
	var commitErr error
	resp, err := s.writeFieldThen(ctx, req.FieldRequest, fieldVerbPop, func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error {
		var err error
		if limit, err = popLimit(req.Limit); err != nil {
			return err
		}
		ceiling, stamped := s.popCeiling(ctx, req.ResultCeiling)
		budget := s.newResultBudgetWithCeiling(queryMethodPop, ceiling, answeringApply(ctx), resultTooLargeError{
			query: fmt.Sprintf("pop({limit: %d}) on %s.%s[%q].%s", limit, req.Database, req.Bucket, req.Key, req.Field), shape: resultShapePop, limit: limit, onOwner: stamped,
		})
		defer budget.report()
		// Background, never ctx: see Server.PopCtx.
		walk := newQueryWalk(context.Background(), queryMethodPop, req.Database, req.Bucket)
		_, err = store.FieldPopN(w, field.ref, limit, func(entry store.FieldEntry) error {
			if err := walk.visit(); err != nil {
				return err
			}
			item := fieldListEntry(entry, field.ref.Engine)
			encoded, err := bson.Marshal(item)
			if err != nil {
				return fmt.Errorf("encoding the popped element: %w", err)
			}
			cost := int64(len(encoded)) + elementHeader(len(resp.Items))
			if len(resp.Items) == 0 {
				budget.chargeFirst(cost)
			} else if err := budget.chargeDocument(cost); err != nil {
				return err
			}
			resp.Items = append(resp.Items, item)
			return nil
		})
		// resp already holds the items charged before a refusal. That is
		// harmless only because writeFieldLocked answers a refused op with the
		// error and not with resp.
		resp.Found = len(resp.Items) > 0
		return err
	}, func(applyErr error) error {
		commitErr = applyErr
		return nil
	})
	// The generic answer to a batch over the store's bound is a batch put's
	// advice (fewer documents, ingest). A pop's is to pop fewer.
	if err == nil && !resp.Success && errors.Is(commitErr, storage.ErrBatchTooLarge) {
		resp.Message = popTooLargeMessage("field pop failed", limit, commitErr)
	}
	return resp, err
}

// FieldPutCtx upserts an element under its element key.
func (s *Server) FieldPutCtx(ctx context.Context, req FieldRequest) (*FieldResponse, error) {
	if err := fieldElementKeyRefusal(fieldVerbPut, req); err != nil {
		return nil, err
	}
	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}
	element, encoded, err := elementDocument(req.Data)
	if err != nil {
		return nil, err
	}
	if err := refuseUnreachableNames(element, req.Database, req.Bucket); err != nil {
		return nil, err
	}
	var written, replaced string
	return s.writeFieldThen(ctx, req, fieldVerbPut, func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error {
		resp.ElementKey = req.Element
		if field.ref.Engine != store.FieldBlob {
			return store.FieldPut(w, field.ref, req.Element, encoded)
		}
		// The payload is durable under its final name before the batch that
		// names it commits (shared-storage spec §3.5).
		blob, err := store.PrepareFieldBlob(s.blobRoot(), field.ref, s.blobKeys, req.Content, req.Data)
		if err != nil {
			return err
		}
		written, resp.BlobID = blob.PayloadPath(), blob.BlobID()
		replaced, err = store.FieldBlobPut(w, field.ref, req.Element, blob)
		return err
	}, func(applyErr error) error {
		switch {
		case applyErr == nil && replaced != "":
			return store.UnlinkPayloadAfterCommit(s.kv, s.blobRoot(), replaced)
		case applyErr != nil && written != "" && !errors.Is(applyErr, store.ErrApplyFailed):
			// Nothing committed, so nothing names the file. A store failure
			// may have committed after all: its file is the orphan pass's.
			return store.RemovePayload(s.blobRoot(), written)
		}
		return nil
	})
}

// FieldDeleteCtx deletes an element by its element key; Found says whether
// it was there.
func (s *Server) FieldDeleteCtx(ctx context.Context, req FieldRequest) (*FieldResponse, error) {
	if err := fieldElementKeyRefusal(fieldVerbDelete, req); err != nil {
		return nil, err
	}
	var removed string
	return s.writeFieldThen(ctx, req, fieldVerbDelete, func(w store.FieldWrite, field resolvedField, resp *FieldResponse) error {
		resp.ElementKey = req.Element
		if field.ref.Engine == store.FieldBlob {
			var err error
			resp.Found, removed, err = store.FieldBlobDelete(w, field.ref, req.Element)
			return err
		}
		existed, err := store.FieldDelete(w, field.ref, req.Element)
		resp.Found = existed
		return err
	}, func(applyErr error) error {
		if applyErr == nil && removed != "" {
			return store.UnlinkPayloadAfterCommit(s.kv, s.blobRoot(), removed)
		}
		return nil
	})
}

// FieldPeek returns the next req.Limit elements a pop would take, in pop
// order, without taking them: one snapshot, one seek, and each element charged
// to the result budget as the walk yields it (Server.Peek is the bucket form).
// Items carry each element's position, priority and entry id, which address its
// nested stores and which only pop, destructively, answered before. Found is
// set here, from the items.
func (s *Server) FieldPeek(req FieldPeekRequest) (*FieldResponse, error) {
	resp := &FieldResponse{Success: true}
	err := s.readField(req.FieldRequest, fieldVerbPeek, func(snap storage.KVSnapshot, field resolvedField) error {
		limit, err := peekLimit(req.Limit, "pop() them, or page a field with list({limit: 1000, after: ...})")
		if err != nil {
			return err
		}
		budget := s.newResultBudget(queryMethodPeek, resultTooLargeError{
			query: fmt.Sprintf("peek({limit: %d}) on %s.%s[%q].%s", limit, req.Database, req.Bucket, req.Key, req.Field), shape: resultShapePeek, limit: limit,
		})
		defer budget.report()
		walk := newQueryWalk(context.Background(), queryMethodPeek, req.Database, req.Bucket)
		err = store.FieldPeekN(snap, field.ref, limit, func(entry store.FieldEntry) error {
			if err := walk.visit(); err != nil {
				return err
			}
			item := fieldListEntry(entry, field.ref.Engine)
			encoded, err := bson.Marshal(item)
			if err != nil {
				return fmt.Errorf("encoding the peeked element: %w", err)
			}
			cost := int64(len(encoded)) + elementHeader(len(resp.Items))
			if len(resp.Items) == 0 {
				budget.chargeFirst(cost)
			} else if err := budget.chargeDocument(cost); err != nil {
				return err
			}
			resp.Items = append(resp.Items, item)
			return nil
		})
		resp.Found = len(resp.Items) > 0
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FieldGet reads an element by its element key.
func (s *Server) FieldGet(req FieldRequest) (*FieldResponse, error) {
	if req.Element == "" {
		return nil, fmt.Errorf("%w: get names the element key: db.%s[%q].%s.get(\"key\")", ErrFieldAddressIncomplete, req.Bucket, req.Key, req.Field)
	}
	resp := &FieldResponse{Success: true, ElementKey: req.Element}
	err := s.readField(req, fieldVerbGet, func(snap storage.KVSnapshot, field resolvedField) error {
		if field.ref.Engine == store.FieldBlob {
			stored, err := store.FieldBlobGet(snap, s.blobRoot(), field.ref, s.blobKeys, req.Element)
			if err != nil {
				return fmt.Errorf("reading element %q of %s on document %q in %s.%s: %w%s",
					req.Element, req.Field, req.Key, req.Database, req.Bucket, err, blobReadFix(err))
			}
			if stored == nil {
				return nil
			}
			resp.Found, resp.BlobID, resp.Content = true, stored.BlobID, stored.Content
			if len(stored.Metadata) > 0 {
				resp.Element, err = bson.Marshal(stored.Metadata)
			}
			return err
		}
		value, found, err := store.FieldGet(snap, field.ref, req.Element)
		resp.Found, resp.Element = found, bson.Raw(value)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FieldLen is the field's element count for one document, from its instance
// state: no scan.
func (s *Server) FieldLen(req FieldRequest) (*FieldResponse, error) {
	resp := &FieldResponse{Success: true, Found: true}
	err := s.readField(req, fieldVerbLen, func(snap storage.KVSnapshot, field resolvedField) error {
		count, err := store.FieldLen(snap, field.ref)
		resp.Len = int64(count)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FieldList returns one page of the field's elements in the engine's order.
func (s *Server) FieldList(req FieldListRequest) (*FieldListResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = defaultFieldListLimit
	}
	limit = min(limit, maxFieldListLimit)
	resp := &FieldListResponse{Success: true, Entries: []FieldListEntry{}}
	err := s.readField(req.FieldRequest, fieldVerbList, func(snap storage.KVSnapshot, field resolvedField) error {
		after, err := listCursor(req.After, field.ref.Engine)
		if err != nil {
			return err
		}
		entries, hasMore, err := store.FieldList(snap, field.ref, store.FieldListRequest{After: after, Limit: limit, MaxBytes: maxFieldListBytes})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			resp.Entries = append(resp.Entries, fieldListEntry(entry, field.ref.Engine))
		}
		resp.HasMore = hasMore
		if len(entries) > 0 {
			resp.Next = entries[len(entries)-1].Remainder
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// fieldListEntry is the wire record of one element, for list and for peek: a
// blobStore field's entry carries its blob id, every other engine the element
// document.
func fieldListEntry(entry store.FieldEntry, engine store.FieldEngine) FieldListEntry {
	listed := FieldListEntry{ElementKey: entry.ElementKey, EntryID: entry.EntryID}
	listed.Position, listed.Priority = wirePlacement(entry, engine)
	if engine == store.FieldBlob {
		listed.BlobID = string(entry.Value)
	} else {
		listed.Element = bson.Raw(entry.Value)
	}
	return listed
}

// listCursor reads a list's After: the cursor a previous page returned, an
// element key for a keyed field, or a position for fifo and lifo.
func listCursor(after bson.RawValue, engine store.FieldEngine) ([]byte, error) {
	switch after.Type {
	case 0, bson.TypeNull:
		return nil, nil
	case bson.TypeBinary:
		_, cursor := after.Binary()
		return cursor, nil
	case bson.TypeString:
		if engine == store.FieldHash || engine == store.FieldBTree || engine == store.FieldBlob {
			return store.KeyedRemainder(after.StringValue())
		}
	case bson.TypeInt32, bson.TypeInt64:
		position, _ := after.AsInt64OK()
		if (engine == store.FieldFIFO || engine == store.FieldLIFO) && position >= 0 {
			return store.PositionRemainder(uint64(position)), nil
		}
	}
	return nil, fmt.Errorf("%w: list's after is the next cursor of the previous page, an element key for a hash, btree or blob field, "+
		"or a position for a fifo or lifo field; a %s field cannot start after a %s", ErrFieldWrongVerb, engine, after.Type)
}

// cascadeTooLargeDetail is what a document whose cascade one batch cannot hold
// is told about it: each non-empty store field with its element count, and
// the verb that empties the first of them.
func cascadeTooLargeDetail(bucket *inMemoryBucket, req DeleteRequest) (counted, emptyWith string) {
	var counts []string
	snap := bucket.kv.NewSnapshot()
	defer snap.Discard()
	owner, err := store.FieldOwnerPrefix(bucket.slot, bucket.databaseID, bucket.id.Load(), req.Key)
	if err == nil {
		for _, declared := range bucket.storeFields().stores {
			if declared.Parent != 0 {
				continue
			}
			engine, _ := store.ParseFieldEngine(declared.Engine)
			count, _ := store.FieldLen(snap, store.FieldRef{Owner: owner, FieldID: uint16(declared.ID), Engine: engine})
			if count == 0 {
				continue
			}
			counts = append(counts, fmt.Sprintf("%s: %s", declared.Name, groupThousands(int(count))))
			if emptyWith == "" {
				verb := "pop()"
				if engine == store.FieldHash || engine == store.FieldBTree || engine == store.FieldBlob {
					verb = "delete(key) for each key list() returns"
				}
				emptyWith = fmt.Sprintf("db.%s[%q].%s.%s until len() is 0", req.Bucket, req.Key, declared.Name, verb)
			}
		}
	}
	return strings.Join(counts, ", "), emptyWith
}

// cascadeTooLargeMessage is S-3's refusal of a document delete whose store
// fields hold too many elements to delete in one batch, naming each
// non-empty field and its count, and the way out.
func cascadeTooLargeMessage(bucket *inMemoryBucket, req DeleteRequest) string {
	counted, emptyWith := cascadeTooLargeDetail(bucket, req)
	return fmt.Sprintf("Document %q in bucket %q was not deleted: its store fields hold too many elements to delete in one step (%s). "+
		"Empty them first — %s — then delete the document.", req.Key, req.Bucket, counted, emptyWith)
}

// expirySkippedMessage is what a TTL expiry answers, and the sweeper logs,
// for a document S-3 would refuse to delete: it has expired and it stays,
// whole, until its store fields fit one batch.
func expirySkippedMessage(bucket *inMemoryBucket, req DeleteRequest) string {
	counted, emptyWith := cascadeTooLargeDetail(bucket, req)
	return fmt.Sprintf("Document %q in bucket %q of database %q has expired and was not deleted: its store fields hold too many elements to delete in one step (%s). "+
		"It is left whole and every TTL sweep tries it again. Empty its store fields — %s — and the next sweep expires it, or delete the document yourself once they are empty.",
		req.Key, req.Bucket, req.Database, counted, emptyWith)
}

// storeNames is a bucket's L2 store field names, in declaration order.
func storeNames(b *inMemoryBucket) []string {
	var names []string
	for _, declared := range b.storeFields().stores {
		if declared.Parent == 0 {
			names = append(names, declared.Name)
		}
	}
	return names
}

// storeFieldInfos is a bucket's L2 store fields with their engines, in
// declaration order, from the catalog alone.
func storeFieldInfos(b *inMemoryBucket) []StoreFieldInfo {
	var fields []StoreFieldInfo
	for _, declared := range b.storeFields().stores {
		if declared.Parent == 0 {
			fields = append(fields, StoreFieldInfo{Name: declared.Name, Engine: declared.Engine})
		}
	}
	return fields
}

// storeStatsOf counts each declared store's elements over every document by
// one walk of the bucket's field-store range in snap, skipping instance
// states (Q10: a scan per call, on demand; a maintained counter would widen
// the state document every engine and replay share). O(the bucket's field
// elements), which is why only describe pays it.
func storeStatsOf(b *inMemoryBucket, snap storage.KVSnapshot) ([]StoreStats, error) {
	declared := b.storeFields().stores
	if len(declared) == 0 {
		return nil, nil
	}
	counts := make(map[uint16]int64, len(declared))
	err := store.WalkFieldElements(snap, storage.BucketRef{Slot: b.slot, DatabaseID: b.databaseID, BucketID: b.id.Load()}, b.fieldLayout(), func(element store.FieldElementRecord) error {
		counts[element.FieldID]++
		return nil
	})
	if err != nil {
		return nil, err
	}
	names := make(map[int32]string, len(declared))
	for _, field := range declared {
		names[field.ID] = field.Name
	}
	stats := make([]StoreStats, 0, len(declared))
	for _, field := range declared {
		stats = append(stats, StoreStats{Name: field.Name, Engine: field.Engine, Parent: names[field.Parent], Elements: counts[uint16(field.ID)]})
	}
	return stats, nil
}

// fieldLayout is the bucket's declared store fields by id, which a document
// delete passes to its cascade.
func (b *inMemoryBucket) fieldLayout() store.FieldLayout {
	return fieldLayoutOf(b.storeFields().stores)
}

// fieldLayoutOf is the layout of a declaration's store fields by id. A pull
// reads it from the declaration its source sent, which the landed bucket
// adopts, so its checks see the layout the bucket's reads will.
func fieldLayoutOf(declared []catalogStore) store.FieldLayout {
	if len(declared) == 0 {
		return nil
	}
	layout := make(store.FieldLayout, len(declared))
	for _, field := range declared {
		engine, err := store.ParseFieldEngine(field.Engine)
		if err == nil {
			layout[uint16(field.ID)] = engine
		}
	}
	return layout
}

// deleteDocument deletes a hash or btree document and its field stores, then
// unlinks the payloads of its blobStore field elements, whose metadata the
// same batch deleted. A payload that cannot be unlinked is the orphan pass's,
// logged, not a failed delete.
func (s *Server) deleteDocument(ctx context.Context, bucket *inMemoryBucket, req DeleteRequest) (spared bool, err error) {
	var payloads []string
	if bucket.bucketType == BucketTypeBTree {
		spared, payloads, err = bucket.btreeStore.DeleteDocumentBelow(applyInfoFrom(ctx, s), req.Key, bucket.fieldLayout(), req.RepairBound)
	} else {
		spared, payloads, err = bucket.hashStore.DeleteDocumentBelow(applyInfoFrom(ctx, s), req.Key, bucket.fieldLayout(), req.RepairBound)
	}
	if err != nil {
		return false, err
	}
	return spared, s.unlinkCascadedPayloads(req, payloads)
}

// unlinkCascadedPayloads is the second half of a document delete's cascade,
// a user's and a TTL expiry's alike: the payload files of the blobStore
// elements whose metadata the committed batch deleted.
func (s *Server) unlinkCascadedPayloads(req DeleteRequest, payloads []string) error {
	var unlinked error
	for _, payload := range payloads {
		unlinked = errors.Join(unlinked, store.UnlinkPayloadAfterCommit(s.kv, s.blobRoot(), payload))
	}
	return s.notePayloadLeft(req.Database, req.Bucket, unlinked)
}

// deleteFailureMessage is DeleteCtx's Success:false message for a hash or
// btree document: S-3's refusal when the delete cascade is too large for one
// batch, and the delete's own failure otherwise.
func deleteFailureMessage(bucket *inMemoryBucket, req DeleteRequest, err error) string {
	if errors.Is(err, storage.ErrBatchTooLarge) {
		return cascadeTooLargeMessage(bucket, req)
	}
	return failureMessage("document not found or delete failed", err)
}

// serveField decodes a field request, runs it and answers with the request's
// opcode; a refusal is an error frame, so its code survives a forward.
func serveField[Request any, Response any](frame *transport.Frame, run func(Request) (Response, error)) (*transport.Frame, error) {
	var req Request
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal %s request: %w", frame.Opcode, err))
	}
	resp, err := run(req)
	if err != nil {
		return errorResponse(frame, err)
	}
	payload, err := bson.Marshal(resp)
	if err != nil {
		return errorResponse(frame, fmt.Errorf("encoding the %s response: %w", frame.Opcode, err))
	}
	return transport.NewFrameWithID(frame.Opcode, frame.RequestID, payload), nil
}

func handleFieldPush(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, func(req FieldRequest) (*FieldResponse, error) { return s.FieldPushCtx(ctx, req) })
}

func handleFieldPop(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, func(req FieldPopRequest) (*FieldResponse, error) { return s.FieldPopCtx(ctx, req) })
}

func handleFieldPut(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, func(req FieldRequest) (*FieldResponse, error) { return s.FieldPutCtx(ctx, req) })
}

func handleFieldDelete(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, func(req FieldRequest) (*FieldResponse, error) { return s.FieldDeleteCtx(ctx, req) })
}

func handleFieldPeek(frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, s.FieldPeek)
}

func handleFieldGet(frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, s.FieldGet)
}

func handleFieldLen(frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, s.FieldLen)
}

func handleFieldList(frame *transport.Frame, s *Server) (*transport.Frame, error) {
	return serveField(frame, s.FieldList)
}
