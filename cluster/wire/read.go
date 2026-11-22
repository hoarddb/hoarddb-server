package wire

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// Document is one document as the owner read it.
type Document struct {
	// Slot is the document's slot.
	Slot uint16
	// EncID is the document's encoded _id_.
	EncID []byte
	// Body is the document as BSON: _id_ first, the declared scalars in field-id order,
	// then each store field as its marker text. List and get build it the same way.
	Body []byte
	// Decl is the declaration the owner read it under, which names the store fields.
	Decl machine.Declaration
	// ServedBy is the owner that answered (queries §1).
	ServedBy string
	// Expired is set on the answer of a keyed read that found the document past its 'expire at'
	// time: nothing else of the Document is filled, and the read is a not-found.
	Expired bool
	// Served says where the answer came from: servedLocal when this node's copy answered,
	// servedOwner otherwise.
	Served string
}

// The values of Document.Served and of the reply's served_from member.
const (
	servedLocal = "local"
	servedOwner = "owner"
)

// Holder parks a read a hold refuses and says where it goes. *Handoff is the real one:
// it is the only waiter, and the machine's held list and its waiter list move together.
type Holder interface {
	// Hold parks req until its hold ends or its budget runs out, and returns the release
	// the caller acts on exactly once.
	Hold(ctx context.Context, req HeldRequest) (HoldRelease, error)
}

// ReadForwarder sends a read to the owner it names.
type ReadForwarder interface {
	// ForwardRead sends req, whose Hop is already counted, to owner and returns its answer.
	ForwardRead(ctx context.Context, owner string, req ReadRequest) (Document, error)
	// ForwardFieldRead sends a store read (list, count, get, peek, lease_get), whose request
	// Hop is already counted, to owner and returns its answer (BUG-0766).
	ForwardFieldRead(ctx context.Context, owner string, forwarded FieldForward) (FieldAnswer, error)
}

// ReaderConfig is the collaborators of a Reader.
type ReaderConfig struct {
	// Gate is the admission, shared with the scanner.
	Gate *ReadGate
	// KV is the node's one shared Badger.
	KV storage.KV
	// Holder parks a read the pair's hold refuses.
	Holder Holder
	// PeerReachable says whether the other node of a pair's barrier answers.
	PeerReachable func(slot uint16, db uint64) bool
	// Forward reroutes a read to the owner.
	Forward ReadForwarder
	// Self is this node's id, printed as ServedBy.
	Self string
	// WallMillis is the clock a lease read tests deadlines against: the same wall clock
	// the writer stamps a claim with. Nil takes the system clock.
	WallMillis func() int64
	// NearestMaxElements is query.nearest_max_elements: the most elements one nearest call scans
	// in one store field; 0 takes defaultNearestMaxElements.
	NearestMaxElements int
	// StaleReads is cluster.stale_reads: when it returns true every keyed document read may
	// be answered by a ready copy, as if the call had asked for Local. Nil means false.
	StaleReads func() bool
	// CorruptKeyAdvice is added to a CORRUPT_KEY refusal: the advice for this node's kind
	// (ringDirAdvice). Empty adds none.
	CorruptKeyAdvice string
}

// defaultNearestMaxElements is the nearest ceiling when the node sets none.
const defaultNearestMaxElements = 100_000

// Reader answers keyed reads at the owner (write §11, queries §1). It keeps no state
// and takes no pair lock: a read consumes no position.
type Reader struct{ cfg ReaderConfig }

// NewReader returns a Reader over cfg.
func NewReader(cfg ReaderConfig) *Reader {
	if cfg.NearestMaxElements == 0 {
		cfg.NearestMaxElements = defaultNearestMaxElements
	}
	return &Reader{cfg: cfg}
}

// Get reads the document encID of bucket in database db from slot.
func (r *Reader) Get(ctx context.Context, slot uint16, db uint64, bucket uint64, encID []byte) (Document, error) {
	return r.Read(ctx, ReadRequest{Slot: slot, DBID: db, BucketID: bucket, EncID: encID})
}

// Read is Get with the whole request. The owner answers from its own store and nobody
// else does. A pair in a hold parks the read with the Holder, which is the only waiter,
// and the read acts once on the release: forwarded to the owner it names, or, when that
// is this node, admitted again one time. Nothing is answered until the lease, the hold,
// the owner and the declaration have been checked again after the document was read; a
// state that moves under a read twice in a row is refused, not looped on.
func (r *Reader) Read(ctx context.Context, req ReadRequest) (Document, error) {
	if doc, ok := r.readFromCopy(req); ok {
		return doc, nil
	}
	got, err := routeRead(ctx, r, req, readRoute[documentRead]{
		local: func(admitted ReadVerdict) (documentRead, error) {
			doc, found, err := r.load(req, admitted)
			return documentRead{doc: doc, found: found}, err
		},
		forward: func(ctx context.Context, owner string, hopped ReadRequest) (documentRead, error) {
			doc, err := r.cfg.Forward.ForwardRead(ctx, owner, hopped)
			return documentRead{doc: doc, found: true}, err
		},
	})
	if err != nil {
		return Document{}, err
	}
	if !got.found {
		message := cluster.CodeNotFound.Text(cluster.CondDocumentNotFound, r.cfg.Gate.refusalArgs(req))
		if got.doc.Expired {
			message = expiredMissText(message)
		}
		coded := &cluster.Error{Code: cluster.CodeNotFound, Message: message}
		if got.doc.Expired {
			return Document{}, &ringDetailed{err: coded, detail: cluster.Detail{Expired: true}}
		}
		return Document{}, coded
	}
	got.doc.Served = servedOwner
	return got.doc, nil
}

// readFromCopy answers req from this node's own copy when the call allows it (req.Local or the
// stale_reads setting) and the gate says a ready copy may answer. The document is read, then
// the verdict is taken again: a declaration that moved or a copy that lost readiness under
// the read sends it to the owner, so a reader never goes backwards across a declaration bump.
// A document the copy does not hold is not answered as absent: the copy may only be behind,
// and the owner is asked.
func (r *Reader) readFromCopy(req ReadRequest) (Document, bool) {
	if !req.Local && (r.cfg.StaleReads == nil || !r.cfg.StaleReads()) {
		return Document{}, false
	}
	verdict, ok := r.cfg.Gate.CopyVerdict(req)
	if !ok {
		return Document{}, false
	}
	doc, found, err := r.load(req, verdict)
	if err != nil || !found {
		return Document{}, false
	}
	confirmed, ok := r.cfg.Gate.CopyVerdict(req)
	if !ok || confirmed.Version != verdict.Version {
		return Document{}, false
	}
	doc.Served = servedLocal
	return doc, true
}

// documentRead is what one attempt of a document read found: the document, or that it is absent.
type documentRead struct {
	doc   Document
	found bool
}

// readRoute is the two things a read does besides being admitted, for the answer type T.
type readRoute[T any] struct {
	// local answers from this node's own store under the verdict the gate gave. A refusal it
	// returns is final; the answer is confirmed against the gate before it is given.
	local func(admitted ReadVerdict) (T, error)
	// forward sends the read to the owner. Its request has the hop already counted.
	forward func(ctx context.Context, owner string, req ReadRequest) (T, error)
}

// routeRead is the one admission loop of every read the gate guards: a document get, a store
// list, count and get, a peek and a lease read. A read answers only when the gate admits it at
// this node before and after route.local ran; a hold parks it with the Holder and acts once on
// the release; a read that this node does not own goes once to the owner (Hop), and a second
// reroute is refused. Sharing the loop is the point (BUG-0766): a verb that copied only the
// gate call refused what a document read waits for or forwards.
func routeRead[T any](ctx context.Context, r *Reader, req ReadRequest, route readRoute[T]) (T, error) {
	var none T
	budget := &readBudget{}
	admitted, err := r.cfg.Gate.admit(ctx, req, budget)
	if err != nil {
		return none, err
	}
	released := false
	// Two passes: the read, and the one re-entry after what moved under it.
	for pass := 0; pass < 2; pass++ {
		if admitted.Held {
			if released {
				return none, r.cfg.Gate.holdRefusal(req, admitted.HoldCause)
			}
			release, err := r.hold(ctx, req)
			if err != nil {
				return none, err
			}
			if !release.Local {
				return forwardRead(ctx, r, release.Owner, req, route.forward)
			}
			released = true
			if admitted, err = r.cfg.Gate.admit(ctx, req, budget); err != nil {
				return none, err
			}
			pass-- // the release is the re-entry, not a second look at what moved
			continue
		}
		if admitted.Forward {
			return forwardRead(ctx, r, admitted.Owner, req, route.forward)
		}
		answer, err := route.local(admitted)
		if err != nil {
			return none, err
		}
		confirmed, err := r.cfg.Gate.admit(ctx, req, budget)
		if err != nil {
			return none, err
		}
		if confirmed.Answerable() && confirmed.Version == admitted.Version {
			return answer, nil
		}
		admitted = confirmed
	}
	return none, r.cfg.Gate.refusal(cluster.CodeSchemaNotCurrent, req)
}

// forwardRead reroutes the read once, to owner, keeping its request id.
func forwardRead[T any](ctx context.Context, r *Reader, owner string, req ReadRequest, send func(context.Context, string, ReadRequest) (T, error)) (T, error) {
	req.Hop++
	if req.Hop > 1 {
		var none T
		return none, r.cfg.Gate.refusal(cluster.CodeNotOwner, req)
	}
	return send(ctx, owner, req)
}

// hold parks a read with the Holder; its budget is the caller's deadline.
func (r *Reader) hold(ctx context.Context, req ReadRequest) (HoldRelease, error) {
	var budget time.Duration
	if deadline, bounded := ctx.Deadline(); bounded {
		budget = max(time.Until(deadline), time.Nanosecond)
	}
	release, err := r.cfg.Holder.Hold(ctx, HeldRequest{
		Slot: req.Slot, DB: req.DBID, RequestID: req.RequestID, Read: true,
		PeerReachable: r.cfg.PeerReachable(req.Slot, req.DBID), Deadline: budget,
	})
	if err != nil {
		return HoldRelease{}, fmt.Errorf("holding the read of %s in slot %d: %w", req.idText(), req.Slot, err)
	}
	return release, nil
}

// load reads the document and builds its body from one snapshot.
func (r *Reader) load(req ReadRequest, admitted ReadVerdict) (Document, bool, error) {
	snapshot := r.cfg.KV.NewSnapshot()
	defer snapshot.Discard()
	stored, found, err := readStoredDocument(snapshot, req, r.cfg.CorruptKeyAdvice)
	if err != nil || !found {
		return Document{}, false, err
	}
	if expireField := admitted.Expire.BucketField; expireField != 0 {
		// The projection may have stepped over the field, so it is read by its own key.
		expiresAt, err := readScalar(snapshot, store.DocumentPrefix(req.Slot, req.DBID, req.BucketID, req.EncID), expireField)
		if err != nil {
			return Document{}, false, fmt.Errorf("reading the expire field of %s: %w", req.idText(), err)
		}
		if expiredAt(expiresAt, clockOrSystem(r.cfg.WallMillis)()) {
			return Document{Expired: true}, false, nil
		}
	}
	plan := scanPlan{db: req.DBID, bucketID: req.BucketID, table: admitted.Table, project: req.Fields}
	body, err := plan.body(snapshot, stored)
	if err != nil {
		return Document{}, false, fmt.Errorf("building the body of %s: %w", req.idText(), err)
	}
	return Document{Slot: req.Slot, EncID: stored.encID, Body: body, Decl: admitted.Decl, ServedBy: r.cfg.Self}, true, nil
}

// readStoredDocument reads the header and field keys of one document with one Seek. A
// document with no header, or a tombstone header, is absent. advice is added to a CORRUPT_KEY
// refusal (corruptKey).
func readStoredDocument(snapshot storage.KVSnapshot, req ReadRequest, advice string) (scannedDoc, bool, error) {
	prefix := storage.Key(req.Slot, req.DBID, req.BucketID, storage.NSDocuments, req.EncID)
	// Bounded to the document's keys so Badger skips every table and level whose range cannot
	// hold them, and asks the bloom filter of each table that is left whether it holds any.
	iter := snapshot.NewIterator(storage.DocumentIterOptions(prefix))
	defer iter.Close()
	doc := scannedDoc{slot: req.Slot, encID: bytes.Clone(req.EncID)}
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		tail := key[len(prefix):]
		if len(tail) != 0 && len(tail) != 2 {
			return scannedDoc{}, false, corruptKey(req.BucketID, key[documentPrefixLen:], fmt.Errorf("%d bytes follow the id", len(tail)), advice)
		}
		// A projected read steps over the scalars it does not return without fetching them.
		if len(tail) == 2 && !keepsField(req.Fields, uint16(tail[0])<<8|uint16(tail[1])) {
			continue
		}
		stored, err := iter.Value()
		if err != nil {
			return scannedDoc{}, false, fmt.Errorf("reading a value of %s in slot %d: %w", req.idText(), req.Slot, err)
		}
		if len(tail) == 0 {
			doc.header = stored
		} else {
			doc.fields = append(doc.fields, scannedField{id: uint16(tail[0])<<8 | uint16(tail[1]), stored: stored})
		}
	}
	if err := iter.Err(); err != nil {
		return scannedDoc{}, false, fmt.Errorf("reading %s in slot %d: %w", req.idText(), req.Slot, err)
	}
	tombstone := len(doc.header) == 1 && doc.header[0] == 0x00
	live := doc.header != nil && !tombstone
	return doc, live, nil
}

// Render is RenderDocument over the declaration the owner read the document under.
func (d Document) Render() string { return RenderDocument(d, d.Decl) }

// RenderDocument is the one text rendering of a document, shared by get and list
// (placement §8): the declared fields in field-id order with _id_ first, each as
// "Name: value"; a field with no key is not printed; a store field prints its marker
// and the element count from its state key, as in "Items: lifoStore (3 elements)".
func RenderDocument(d Document, decl machine.Declaration) string {
	// The body is checked first by the validator written for bytes from outside the process.
	if _, err := bson.Validate(d.Body, bson.Limits{Depth: bson.MaxNestingCeiling}); err != nil {
		return "(the document body is not valid BSON: " + err.Error() + ")"
	}
	elements, err := bson.Raw(d.Body).Elements()
	if err != nil {
		return "(the document body is not valid BSON: " + err.Error() + ")"
	}
	stores := storeFieldNames(decl)
	lines := make([]string, 0, len(elements))
	for _, element := range elements {
		name, scalar := element.Key(), element.Value()
		if text, isMarker := scalar.StringValueOK(); isMarker && stores[name] {
			lines = append(lines, name+": "+text)
			continue
		}
		lines = append(lines, name+": "+renderScalar(scalar))
	}
	return strings.Join(lines, "\n")
}

// renderScalar prints the common scalars the way a developer typed them and leaves
// the rest to the driver's relaxed-JSON text.
func renderScalar(scalar bson.RawValue) string {
	switch scalar.Type {
	case bson.TypeString:
		return strconv.Quote(scalar.StringValue())
	case bson.TypeInt32:
		return strconv.FormatInt(int64(scalar.Int32()), 10)
	case bson.TypeInt64:
		return strconv.FormatInt(scalar.Int64(), 10)
	case bson.TypeDouble:
		return strconv.FormatFloat(scalar.Double(), 'g', -1, 64)
	case bson.TypeBoolean:
		return strconv.FormatBool(scalar.Boolean())
	case bson.TypeNull:
		return "null"
	}
	return scalar.String()
}

// storeFieldNames is the set of declared store field names, in force or pending.
func storeFieldNames(decl machine.Declaration) map[string]bool {
	names := map[string]bool{}
	tables := []machine.FieldTable{decl.Fields}
	if decl.Pending != nil {
		tables = append(tables, decl.Pending.Table)
	}
	for _, table := range tables {
		for _, entry := range table.Entries {
			if isStoreType(entry.Type) {
				names[entry.Name] = true
			}
		}
	}
	return names
}
