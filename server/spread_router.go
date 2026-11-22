package server

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The router of a spread collection (document placement spread MVP note,
// section 2.1). A spread collection is a template bucket, which holds its
// declaration and one internal document, the born set, and up to K part
// buckets, which hold its documents. The template's owner is the collection's
// router: every client operation on the collection is acted on there, where
// the born set is authoritative, and a keyed one is sent on to the one part
// that holds its document. A part is an ordinary bucket: its writes are logged,
// replicated, replayed and migrated by today's code, which the router reaches
// only through Cluster.write, Cluster.serveRead and, on a node that does not
// route, Server.fencedDispatch.
//
// Step 2 routes keyed operations, and step 3 the reads that fan out over
// every part (find, count, distinct, search, describe, info, dump). The drop
// of a spread collection is step 4; until then it is refused here. Nothing is
// ever answered from the template's store as the collection's: it holds the
// born set and no client document.

// bornDocumentID is the _id_ of the template's one internal document, the born
// set: one int32 per part, b<hex4>, at 1 once the part is marked (its create
// may be in flight) and 2 once it is created (it may hold documents). The
// separator starts it, so it sorts and reads as nothing a client named.
const bornDocumentID = "\x1fborn"

// The two states a part has in the born set; an absent field is unborn.
const (
	partMarked  int32 = 1
	partCreated int32 = 2
)

// bornDroppingField is the born set's flag of a drop in progress (step 4).
const bornDroppingField = "dropping"

// bornField is part p's field in the born set.
func bornField(p int) string {
	const hexDigits = "0123456789abcdef"
	return string([]byte{'b', hexDigits[p>>12&15], hexDigits[p>>8&15], hexDigits[p>>4&15], hexDigits[p&15]})
}

// ErrNotSupportedOnSpread is under the refusal of an operation a spread bucket
// does not take, or does not take yet.
var ErrNotSupportedOnSpread = errors.New("not supported on a spread bucket")

// ErrSpreadMarkShort is under the refusal of a first write to a part whose
// mark in the born set too few copies of the template confirmed (R2):
// nothing was stored, and a retry is safe.
var ErrSpreadMarkShort = errors.New("the born set of a spread bucket was not confirmed on enough copies")

// ErrSpreadPartLost is under the refusal of an operation on a part the born
// set records created that no node holds (B9).
var ErrSpreadPartLost = errors.New("a part of a spread bucket is on no reachable node")

// ErrSpreadDropping is under the refusal of an operation on a spread bucket
// whose drop has begun (section 2.3).
var ErrSpreadDropping = errors.New("the spread bucket is being dropped")

// spreadTemplateHeld reports whether this node holds database/bucket as a
// spread template: a bucket recorded placement spread, which is never a part
// (a part is recorded whole). One registry lookup under the database's read
// lock and one flag read; nothing is decoded.
func (s *Server) spreadTemplateHeld(database, bucket string) bool {
	if bucket == "" || hasPartSeparator(bucket) {
		return false
	}
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	held, found := db.table.get(bucket)
	return found && held.rec.isSpread()
}

// spreadTemplate is what the router reads of a template once per operation:
// its parts (K, from its catalog entry, never from the constant) and its
// incarnation, which every part it creates carries as part_of.
type spreadTemplate struct {
	database, name string
	key            placementKey
	parts          int
	incarnation    uint64
	entry          catalogEntry
}

// loadSpreadTemplate reads the template's catalog entry and lineage.
func (s *Server) loadSpreadTemplate(database, bucket string) (spreadTemplate, error) {
	entry, found, err := s.readCatalogEntry(database, bucket)
	if err != nil {
		return spreadTemplate{}, err
	}
	if !found {
		return spreadTemplate{}, bucketNotFoundErr(database, bucket)
	}
	if entry.Parts == 0 {
		return spreadTemplate{}, fmt.Errorf("bucket %q in database %q is recorded spread with no parts", bucket, database)
	}
	state, err := s.bucketStateNamed(database, bucket)
	if err != nil {
		return spreadTemplate{}, fmt.Errorf("reading the incarnation of bucket %q in database %q: %w", bucket, database, err)
	}
	if state.Incarnation == 0 {
		return spreadTemplate{}, fmt.Errorf("bucket %q in database %q has no incarnation yet", bucket, database)
	}
	return spreadTemplate{database: database, name: bucket, key: placementOf(database, bucket),
		parts: int(entry.Parts), incarnation: state.Incarnation, entry: entry}, nil
}

// partOfKey is the part of the template that holds the document key.
func (t spreadTemplate) partOfKey(key string) (int, error) {
	encoded, err := store.EncodeComponent(key)
	if err != nil {
		return 0, fmt.Errorf("encoding the _id_ %q of bucket %q to find its part: %w", key, t.name, err)
	}
	return store.PartOf(encoded, t.parts)
}

// bornSet is the template's born document as stored, nil before its first
// mark.
type bornSet struct {
	raw bson.Raw
}

// state is part p's state: 0 (unborn), partMarked or partCreated.
func (b bornSet) state(p int) int32 {
	if b.raw == nil {
		return 0
	}
	value, err := b.raw.LookupErr(bornField(p))
	if err != nil {
		return 0
	}
	if number, ok := value.Int32OK(); ok {
		return number
	}
	if number, ok := value.Int64OK(); ok {
		return int32(number)
	}
	return 0
}

// dropping reports whether a drop of the collection has begun.
func (b bornSet) dropping() bool {
	if b.raw == nil {
		return false
	}
	value, err := b.raw.LookupErr(bornDroppingField)
	if err != nil {
		return false
	}
	flag, ok := value.BooleanOK()
	return ok && flag
}

// readBornSet reads the born set from this node's copy of the template, which
// the router holds: one local document read.
func (s *Server) readBornSet(database, template string) (bornSet, error) {
	found, err := s.Find(FindRequest{Database: database, Bucket: template, Key: bornDocumentID})
	if err != nil {
		return bornSet{}, fmt.Errorf("reading the parts of spread bucket %q in database %q: %w", template, database, err)
	}
	if !found.Found {
		return bornSet{}, nil
	}
	return bornSet{raw: found.Data}, nil
}

// spreadOpClass is what the router does with an opcode on a spread template
// (section 2.4). Anything not named is refused.
type spreadOpClass int

const (
	spreadRefuse spreadOpClass = iota
	// spreadKeyedWrite and spreadKeyedRead are routed to the part of their key.
	spreadKeyedWrite
	spreadKeyedRead
	// spreadBulkWrite is split by part.
	spreadBulkWrite
	// spreadPassThrough is answered as for any bucket: a create of the name
	// ("already exists") and a restore session, whose writes route here.
	spreadPassThrough
	// spreadFanOutRead is a read over every created part, merged at the
	// router (spread_fanout.go, spread_dump.go).
	spreadFanOutRead
	// spreadCollectionDrop is the router drop: step 4.
	spreadCollectionDrop
	// spreadDDL is a declaration change after create.
	spreadDDL
	// spreadWrongType is an opcode of another store type.
	spreadWrongType
)

// spreadClassOf classifies op for a spread template. TestSpreadDefaultDeny
// drives every client opcode through it.
func spreadClassOf(op transport.Opcode) spreadOpClass {
	switch op {
	case transport.OpPut, transport.OpUpsert, transport.OpDelete, transport.OpUpdate,
		transport.OpFieldPush, transport.OpFieldPop, transport.OpFieldPut, transport.OpFieldDelete:
		return spreadKeyedWrite
	case transport.OpGet, transport.OpExists, transport.OpGetFast, transport.OpConsistentGet,
		transport.OpFieldGet, transport.OpFieldPeek, transport.OpFieldLen, transport.OpFieldList:
		return spreadKeyedRead
	case transport.OpBatchPut, transport.OpBatchUpsert:
		return spreadBulkWrite
	case transport.OpCreateBucket, transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone:
		return spreadPassThrough
	case transport.OpScan, transport.OpCount, transport.OpDistinct, transport.OpSearch,
		transport.OpBucketStats, transport.OpBucketInfo,
		transport.OpDumpStart, transport.OpDumpChunk, transport.OpDumpDone:
		return spreadFanOutRead
	case transport.OpDeleteBucket:
		return spreadCollectionDrop
	case transport.OpAlterBucket, transport.OpDeclareStoreFields:
		return spreadDDL
	case transport.OpFIFOPush, transport.OpFIFOPop, transport.OpFIFOPeek,
		transport.OpLIFOPush, transport.OpLIFOPop, transport.OpLIFOPeek,
		transport.OpQueueClaim, transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend,
		transport.OpDeadList, transport.OpDeadRequeue, transport.OpDeadDiscard,
		transport.OpBTreeInsert, transport.OpBTreeLookup, transport.OpBTreeRange, transport.OpBTreeFirst, transport.OpBTreeLast,
		transport.OpHeapPush, transport.OpHeapPop, transport.OpBlobWrite, transport.OpBlobRead:
		return spreadWrongType
	}
	return spreadRefuse
}

// createsParts reports whether op may create the part of its key: only an
// insert and an upsert, and their bulk forms (R3, note d). Every other keyed
// operation of a key in an unborn part is answered not found without asking
// any node and without marking.
func createsParts(op transport.Opcode) bool {
	switch op {
	case transport.OpPut, transport.OpUpsert, transport.OpBatchPut, transport.OpBatchUpsert:
		return true
	}
	return false
}

// spreadOpVerb is how a refusal names op: the driver's verb where it has one.
func spreadOpVerb(op transport.Opcode) string {
	switch op {
	case transport.OpScan:
		return "find"
	case transport.OpCount:
		return "count"
	case transport.OpDistinct:
		return "distinct"
	case transport.OpSearch:
		return "search"
	case transport.OpBucketStats, transport.OpBucketInfo:
		return "describe"
	case transport.OpDumpStart, transport.OpDumpChunk, transport.OpDumpDone:
		return "dump"
	case transport.OpDeleteBucket:
		return "drop bucket"
	case transport.OpAlterBucket:
		return "alter bucket"
	case transport.OpDeclareStoreFields:
		return "declaring store fields"
	case transport.OpMigrate:
		return "migrate"
	case transport.OpVersionedGet:
		return "a versioned read"
	}
	return op.String()
}

// spreadRefusal is the answer of a class the router refuses. Every one is
// refused before anything is logged.
func spreadRefusal(op transport.Opcode, class spreadOpClass, database, bucket string) error {
	verb := spreadOpVerb(op)
	switch class {
	case spreadDDL:
		return spreadDeclarationRefusal(op, database, bucket)
	case spreadWrongType:
		return wrongTypeSentence(fmt.Sprintf("Bucket %q is a hash bucket (placement spread), and %s is not a hash bucket verb.", bucket, op), BucketTypeHash)
	}
	return &sentencedError{cause: ErrNotSupportedOnSpread, message: fmt.Sprintf(
		"%s is not supported on spread bucket %q (database %q)", verb, bucket, database)}
}

// spreadDeclarationRefusal is the refusal of a declaration change after create
// (ALTER_BUCKET, DECLARE_STORE_FIELDS): final, with the way to do it. The
// collection's declaration is copied into every part when the part is made, so
// it cannot change under parts that exist.
func spreadDeclarationRefusal(op transport.Opcode, database, bucket string) error {
	dump := fmt.Sprintf("dump --out ./dump-%s %s %s", bucket, database, bucket)
	drop := fmt.Sprintf("drop bucket %s in %s", bucket, database)
	restore := fmt.Sprintf("restore --dir ./dump-%s", bucket)
	if op == transport.OpAlterBucket {
		return &sentencedError{cause: ErrNotSupportedOnSpread, message: fmt.Sprintf(
			"The replication factor of spread bucket %q (database %q) is set when it is created, so 'alter bucket' is refused. "+
				"To change it: %s, then %s, then create bucket %s in %s { placement: spread, replication_factor: <N> } with the bucket's other fields, then %s",
			bucket, database, dump, drop, bucket, database, restore)}
	}
	return &sentencedError{cause: ErrNotSupportedOnSpread, message: fmt.Sprintf(
		"The store fields of spread bucket %q (database %q) are set when it is created, so declaring new ones is refused "+
			"(declaring fields it already has is accepted, which is what a restore of its own dump does). "+
			"To add a store field: %s, then %s, then create bucket %s in %s { placement: spread, <the old store fields and the new one> }, then %s",
		bucket, database, dump, drop, bucket, database, restore)}
}

// migrateOfSpreadRefusal is the operator's migrate of a spread collection
// (N10, NQ-8 deferred): its parts move with the rebalance.
func migrateOfSpreadRefusal(database, bucket string) error {
	return &sentencedError{cause: ErrNotSupportedOnSpread, message: fmt.Sprintf(
		"spread bucket %s (database %s) moves part by part with the rebalance; each part also moves on its first read or write, so it is not migrated as one bucket. "+
			"Nothing to run: 'nodes' shows the rebalance, and 'describe bucket %s in %s' shows where the bucket is",
		bucket, database, bucket, database)}
}

// spreadDocumentKey is the document key a keyed request names: its key, or
// for an insert or upsert its document's _id_ when it names no key. The door
// mints a missing id before the router runs, so a request with neither is
// refused.
func spreadDocumentKey(op transport.Opcode, payload []byte) (string, error) {
	if op == transport.OpGetFast {
		request, err := transport.UnmarshalGetFast(payload)
		if err != nil {
			return "", &sentencedError{cause: errors.Join(ErrMalformedPayload, err), message: fmt.Sprintf("decoding get-fast request: %v", err)}
		}
		return request.Key, nil
	}
	var keyed struct {
		Key  string   `bson:"key"`
		Data bson.Raw `bson:"data"`
	}
	if err := bson.Unmarshal(payload, &keyed); err != nil {
		return "", &sentencedError{cause: errors.Join(ErrMalformedPayload, err), message: fmt.Sprintf(
			"the %s request names no readable key: %v. Send the key as text", op, err)}
	}
	if keyed.Key != "" || (op != transport.OpPut && op != transport.OpUpsert) {
		return keyed.Key, nil
	}
	if keyed.Data != nil {
		if id, err := keyed.Data.LookupErr("_id_"); err == nil {
			if text, ok := id.StringValueOK(); ok {
				return text, nil
			}
		}
	}
	return "", nil
}

// withPartBucket is payload naming part instead of the template: the router's
// one rewrite (N2). The WAL key of the entry is the document's and stays.
func withPartBucket(op transport.Opcode, payload []byte, part string) ([]byte, error) {
	if op == transport.OpGetFast {
		request, err := transport.UnmarshalGetFast(payload)
		if err != nil {
			return nil, fmt.Errorf("decoding get-fast request to route it: %w", err)
		}
		request.Bucket = part
		return transport.MarshalGetFast(request)
	}
	return stampPayload(payload, bson.D{{Key: "bucket", Value: part}})
}

// displayedAnswer is a part's BSON answer with the part's name shown as its
// collection's, for the few answers whose message names the bucket: no part
// name reaches a client (B7). Each top-level string that names the part is
// rewritten; the common answer holds no separator byte and is returned as it
// is, undecoded.
func displayedAnswer(payload []byte, part, collection string) []byte {
	if bytes.IndexByte(payload, partSeparator) < 0 && !bytes.Contains(payload, []byte(`\x1f`)) {
		return payload
	}
	var document bson.D
	if err := bson.Unmarshal(payload, &document); err != nil {
		return payload
	}
	changed := false
	for index, element := range document {
		if text, ok := element.Value.(string); ok {
			if shown := displayPartIn(text, part, collection); shown != text {
				document[index].Value = shown
				changed = true
			}
		}
	}
	if !changed {
		return payload
	}
	encoded, err := bson.Marshal(document)
	if err != nil {
		return payload
	}
	return encoded
}

// answerNamesNoBucket reports whether a part's answer says the part does not
// exist: NOT_FOUND for the bucket, not for a document or a parent document.
func answerNamesNoBucket(opcode int32, payload []byte) bool {
	if transport.Opcode(opcode) != transport.OpError {
		return false
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(payload, &refusal); err != nil {
		return false
	}
	return transport.ErrorCode(refusal.Code) == transport.ErrNotFound && bytes.Contains([]byte(refusal.Message), []byte(" not found in database "))
}

// lostPartError is B9's refusal for a keyed operation: the born set says the
// part was created, and its owner holds no such bucket.
func lostPartError(database, bucket, holder string, write bool) error {
	nothing := "nothing was returned"
	if write {
		nothing = "nothing was stored"
	}
	return withOutcome(outcomeNotApplied, &sentencedError{cause: ErrSpreadPartLost, message: fmt.Sprintf(
		"Some documents of bucket %q (database %q) are on no reachable node (last held by %s). Bring %s back; %s. 'nodes' shows its state.",
		bucket, database, holder, holder, nothing)})
}

// droppingError is the refusal of any operation once a drop has begun.
func droppingError(database, bucket string) error {
	return withOutcome(outcomeNotApplied, &sentencedError{cause: ErrSpreadDropping, message: fmt.Sprintf(
		"Bucket %q (database %q) is being dropped. Run 'drop bucket %s in %s' again to finish it.",
		bucket, database, bucket, database)})
}

// markShortError is R2's refusal: the mark of a part was confirmed by fewer
// copies of the template than a majority of its declared factor.
func markShortError(database, bucket string, confirmed, factor int, cause error) error {
	message := fmt.Sprintf("Bucket %q (database %q) could not record where this document goes on enough copies (%d of %d confirmed); nothing was stored. "+
		"Retry once every node that holds the bucket is back; 'nodes' shows their state.", bucket, database, confirmed, factor)
	if cause != nil {
		message += " (" + explainError(cause) + ")"
	}
	return withOutcome(outcomeNotApplied, &sentencedError{cause: ErrSpreadMarkShort, message: message})
}
