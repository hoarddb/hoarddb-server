package wire

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// PairRecorder writes the pair record of conv §12 (applied position, WAL cursor and
// ordinal) into the apply's batch, so a position never exists without it. A node whose
// Writer has a PairAdvancer passes nil: the advancer adds the record to the same batch.
type PairRecorder interface {
	// RecordApplied sets the pair record for rec in batch without committing it.
	RecordApplied(batch storage.KVBatch, rec AppliedRecord) error
}

// DocumentApplier applies document operations to the shared Badger store: insert, upsert,
// delete, update with set, unset and inc, the store verbs push, pop, take and put on the fields their store type takes, and claim,
// ack, release and extend. The other
// classes arrive with their units and are refused before a position is taken.
type DocumentApplier struct {
	kv        storage.KV
	pairs     PairRecorder
	engines   FieldEngineSource
	resultCap int
	// wall is the owner's clock a precondition compares an expire time against; nil is the
	// system clock. The apply never reads it: a record carries its own stamped clock.
	wall func() int64
}

// ClaimSink receives the messages a claim leased. The dedupe entry records only the lease
// id and deadline, so this is the one place the leased messages leave the apply. A write
// carries its own sink through WriteRequest and AppliedRecord, so two attempts under one
// request id never share one. It is called with the pair lock held and must not block.
type ClaimSink func(elements [][]byte)

// NewDocumentApplier returns an applier that reads preconditions from kv and writes
// each pair record through pairs.
func NewDocumentApplier(kv storage.KV, pairs PairRecorder) *DocumentApplier {
	return &DocumentApplier{kv: kv, pairs: pairs}
}

// WithFieldEngines gives the applier the declarations' field engines, which the
// claim classes need to tell a fifo from a lifo. Without it they are refused
// before a position is taken.
func (a *DocumentApplier) WithFieldEngines(engines FieldEngineSource) *DocumentApplier {
	a.engines = engines
	return a
}

// WithWallClock sets the clock the owner's preconditions use to tell whether a document has
// expired. The default is the system clock.
func (a *DocumentApplier) WithWallClock(clock func() int64) *DocumentApplier {
	a.wall = clock
	return a
}

// WithResultCap tells the applier the largest result a dedupe entry can hold
// (replication.dedupe_window's MaxResult). A claim whose result would not fit is refused
// before a position is taken: after the append the apply would fail on every replay and
// poison the pair (spec-patch 84).
func (a *DocumentApplier) WithResultCap(maxResult int) *DocumentApplier {
	a.resultCap = maxResult
	return a
}

// Handles reports whether this build applies the class.
func (a *DocumentApplier) Handles(class byte) bool {
	switch class {
	case cluster.OpInsert, cluster.OpUpsert, cluster.OpDelete:
		return true
	case cluster.OpUpdate, cluster.OpStoreVerb, cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
		return a.engines != nil
	}
	return false
}

// ResolveEngines implements EngineResolver for the classes whose apply reads the declaration:
// the store verbs and the lease classes. It refuses only when the declaration cannot be read;
// a field that is retired or undeclared is Fill's to apply as an empty write or to fail, as before.
func (a *DocumentApplier) ResolveEngines(db uint64, op cluster.Op) error {
	if a.engines == nil || len(op.WrittenFields()) == 0 {
		return nil
	}
	switch op.Class {
	case cluster.OpStoreVerb, cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
	default:
		return nil
	}
	_, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
	if err == nil || errors.Is(err, errRetiredField) || errors.Is(err, errUndeclaredField) {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"this copy cannot resolve the store field of bucket %d in database %d yet (%v). Nothing was applied or appended. The owner ships the record again; if it keeps failing run 'nodes' to see whether the node that owns the database's declaration is up", op.BucketID, db, err)}
}

// Precondition is check 6a, read from the owner's own state under the pair lock. An
// insert needs the id absent (a tombstone is absent once its cleanup fits one batch); an
// upsert only needs that cleanup to fit; the store verbs and the claim classes need the
// document live. A delete of an absent document is not a failure.
func (a *DocumentApplier) Precondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	if len(op.EncID) == 0 {
		return 0, errors.New("the operation carries no document id; the coordinator mints it before routing and the owner never does")
	}
	switch op.Class {
	case cluster.OpInsert, cluster.OpUpsert:
		return a.insertPrecondition(slot, db, op)
	case cluster.OpUpdate:
		return a.updatePrecondition(slot, db, op)
	case cluster.OpDelete:
		if op.System {
			return a.systemDeletePrecondition(slot, db, op)
		}
		return 0, nil
	case cluster.OpStoreVerb:
		return a.verbPrecondition(slot, db, op)
	case cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
		return a.leasePrecondition(slot, db, op)
	}
	return 0, nil
}

// insertBatchReserve is what an insert leaves in its batch for the dedupe entry, the pair
// record and the replication bookkeeping the same batch carries.
const insertBatchReserve = 4 << 10

// insertShapeRefusal words why an insert or upsert can never be stored, or returns "".
// Such a record would be appended and then fail to apply on every replay, fencing the pair
// and stopping the node from restarting, so it is refused before a position (spec-patch 84).
func (a *DocumentApplier) insertShapeRefusal(slot uint16, db uint64, op cluster.Op) string {
	for _, field := range op.WrittenFields() {
		if field.FieldID < 2 {
			return fmt.Sprintf("field id %d is not a field of a document: 0 and 1 mark the header, which holds the _id_. "+
				"Nothing was written. Send the _id_ as the document's id and the other fields by their own ids", field.FieldID)
		}
	}
	if reason := a.expireFieldRefusal(db, op); reason != "" {
		return reason
	}
	path := a.insertPath(slot, db, op)
	if need := a.insertBatchBytes(path) + insertBatchReserve; path.Limit > 0 && need > path.Limit {
		return fmt.Sprintf("this document needs a batch of about %d bytes and one batch holds at most %d. "+
			"Nothing was written. Store the large value as a blobStore field, or split the document", need, path.Limit)
	}
	return ""
}

// valueThresholder is a KV that keeps large values outside the batch's byte count.
type valueThresholder interface{ ValueThreshold() int64 }

// insertBatchBytes is what the insert's own writes add to a batch: every value in full,
// except a value the store keeps out of line, which adds a pointer. A store that does not
// say where that line is has every value counted in full.
func (a *DocumentApplier) insertBatchBytes(path InsertPath) int64 {
	var threshold int64
	if sized, ok := a.kv.(valueThresholder); ok {
		threshold = sized.ValueThreshold()
	}
	doc := path.document()
	cost := func(stored []byte) int64 {
		if threshold > 0 && int64(len(stored)) >= threshold {
			return outOfLineValueBytes
		}
		return int64(len(stored))
	}
	total := int64(len(doc.header())+batchEntryOverhead) + cost(path.Insert.Header)
	for _, field := range path.Insert.Fields {
		total += int64(len(doc.header())+2+batchEntryOverhead) + cost(field.Stored)
	}
	return total
}

// outOfLineValueBytes is what a value kept in the value log adds to a batch: its pointer.
const outOfLineValueBytes = 14

func (a *DocumentApplier) insertPrecondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	if a.insertShapeRefusal(slot, db, op) != "" {
		return cluster.CodeInvalidArgument, nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	doc := DocumentKey{Ref: storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: op.BucketID}, EncID: op.EncID}
	state, err := ReadHeaderState(snapshot, doc)
	if err != nil {
		return 0, fmt.Errorf("reading the header of document %x: %w", op.EncID, err)
	}
	absent, err := a.documentAbsent(snapshot, slot, db, op, state)
	if err != nil {
		return 0, err
	}
	expired := absent && state == HeaderLive
	switch {
	case state == HeaderLive && !expired && op.Class == cluster.OpInsert:
		return cluster.CodeDocumentExists, nil
	case state == HeaderTombstone || expired:
		// A document past its expire time is replaced as a tombstone is (expiry spec 3.2).
		if err := checkCleanup(snapshot, a.insertPath(slot, db, op)); err != nil {
			var tooLarge *CleanupTooLargeError
			if errors.As(err, &tooLarge) {
				return cluster.CodeInvalidArgument, nil
			}
			return 0, err
		}
	}
	return 0, nil
}

func (a *DocumentApplier) verbPrecondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	if len(op.WrittenFields()) != 1 {
		return cluster.CodeInvalidArgument, nil
	}
	field := op.WrittenFields()[0]
	engine, code, err := a.preconditionEngine(db, op.BucketID, field.FieldID)
	if err != nil || code != 0 {
		return code, err
	}
	if op.System && systemTakesPosition(field.Verb, engine) {
		return a.systemTakePrecondition(slot, db, op, engine)
	}
	shape, fits := verbShapeOf(field.Verb, engine)
	if !fits {
		return cluster.CodeInvalidArgument, nil
	}
	arg, err := decodeVerbArg(shape, field.Value)
	if err != nil {
		return cluster.CodeInvalidArgument, nil
	}
	if shape == shapeElement {
		if refusal, err := a.pushExpiryRefusal(db, op, arg.Element); err != nil || refusal != "" {
			return cluster.CodeInvalidArgument, err
		}
	}
	code, err = a.liveTarget(slot, db, op)
	if err != nil || code != 0 {
		return code, err
	}
	code, err = a.resultFits(slot, db, op, engine, arg)
	if err != nil || code != 0 {
		return code, err
	}
	if shape == shapeInc {
		// W-I1 and W-I2 before the aggregates' dry run, which reads the same plan and needs it to apply.
		refusal, err := a.incRefusal(slot, db, op, engine, arg)
		if err != nil {
			return 0, err
		}
		if refusal != "" {
			return cluster.CodeInvalidArgument, nil
		}
	}
	refusal, err := a.aggregateRefusal(slot, db, op, engine, arg)
	if err != nil {
		return 0, err
	}
	if refusal != "" {
		return cluster.CodeInvalidArgument, nil
	}
	return 0, nil
}

// resultFits refuses a pop or take whose element would not fit a dedupe entry. The refusal
// is before a position is taken: after the append the apply would fail on every replay
// (spec-patch 84), and the element could not be handed out again on a retry.
func (a *DocumentApplier) resultFits(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine, arg verbArg) (cluster.Code, error) {
	verb := op.WrittenFields()[0].Verb
	if verb == cluster.VerbInc {
		return a.incResultFits(slot, db, op, engine, arg)
	}
	if a.resultCap <= 0 || (verb != cluster.VerbPop && verb != cluster.VerbTake) {
		return 0, nil
	}
	if verb == cluster.VerbPop && arg.Count > 0 {
		_, overflow, err := a.popFit(slot, db, op, engine, arg.Count)
		if err != nil {
			return 0, err
		}
		if overflow {
			return cluster.CodeResultTooLarge, nil
		}
		return 0, nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(slot, db, op.BucketID, op.EncID, op.WrittenFields()[0].FieldID, engine)
	var removed []byte
	var found bool
	var err error
	switch {
	case verb == cluster.VerbPop:
		// The element the apply would deliver: the first one that has not expired, since the apply
		// steps over the expired ones at this attempt's instant.
		var expired func(store.FieldEntry) bool
		if expired, err = a.expiredPredicate(db, op); err == nil {
			err = store.FieldPeekLive(snapshot, ref, 1, expired, func(entry store.FieldEntry) error {
				removed, found = entry.Value, true
				return nil
			})
		}
	case engine == store.FieldHeap:
		var entry store.FieldEntry
		entry, found, err = store.FieldHeapEntry(snapshot, ref, arg.EntryID)
		removed = entry.Value
	default:
		removed, found, err = store.FieldGet(snapshot, ref, arg.Key)
	}
	if err != nil {
		return 0, fmt.Errorf("reading the element a store verb on field %d would return: %w", op.WrittenFields()[0].FieldID, err)
	}
	if found && len(removed) > a.resultCap {
		return cluster.CodeResultTooLarge, nil
	}
	return 0, nil
}

// errPopFitStop ends the walk of popFit at the first element that does not fit.
var errPopFitStop = errors.New("the next element does not fit the dedupe entry")

// popFit walks the n elements a pop {n} would take and says how many leading ones fit one
// dedupe entry together, and whether any of the n did not. It reads at most one element past
// the cap, not the n of them.
func (a *DocumentApplier) popFit(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine, n int) (fits int, overflow bool, err error) {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(slot, db, op.BucketID, op.EncID, op.WrittenFields()[0].FieldID, engine)
	used := 0
	expired, err := a.expiredPredicate(db, op)
	if err != nil {
		return 0, false, err
	}
	err = store.FieldPeekLive(snapshot, ref, n, expired, func(entry store.FieldEntry) error {
		size := packedLen(len(entry.Value))
		if used+size > a.resultCap {
			overflow = true
			return errPopFitStop
		}
		used += size
		fits++
		return nil
	})
	if err != nil && !errors.Is(err, errPopFitStop) {
		return 0, false, fmt.Errorf("reading the elements a pop of %d on field %d would return: %w", n, op.WrittenFields()[0].FieldID, err)
	}
	return fits, overflow, nil
}

func (a *DocumentApplier) fieldRef(slot uint16, db, bucketID uint64, encID []byte, fieldID uint16, engine store.FieldEngine) store.FieldRef {
	return store.FieldRef{Owner: storage.Key(slot, db, bucketID, storage.NSFieldStore, encID), FieldID: fieldID, Engine: engine}
}

// preconditionEngine resolves a field's engine for check 6a. A field the bucket does not
// declare as a store is the caller's FIELD_TYPE refusal; a failed read of the declaration
// stays an error, which the writer answers as unavailable.
func (a *DocumentApplier) preconditionEngine(db, bucketID uint64, fieldID uint16) (store.FieldEngine, cluster.Code, error) {
	engine, err := a.engines.FieldEngine(db, bucketID, fieldID)
	switch {
	case errors.Is(err, errUndeclaredField):
		return 0, cluster.CodeFieldType, nil
	case err != nil:
		return 0, 0, fmt.Errorf("finding the engine of field %d: %w", fieldID, err)
	}
	return engine, 0, nil
}

// leasePrecondition refuses a claim, ack, release or extend that cannot apply: one on a
// field that is not a fifoStore or lifoStore would be appended and then fail on every
// replay, fencing the pair (spec-patch 84).
func (a *DocumentApplier) leasePrecondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	if len(op.WrittenFields()) != 1 {
		return cluster.CodeInvalidArgument, nil
	}
	engine, code, err := a.preconditionEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
	if err != nil || code != 0 {
		return code, err
	}
	if !leasable(engine) {
		return cluster.CodeInvalidArgument, nil
	}
	if size := leaseResultBytes(op.Class); a.resultCap > 0 && size > a.resultCap {
		return cluster.CodeResultTooLarge, nil
	}
	if op.Class == cluster.OpExtend && !extendLengthFits(op.Until) {
		// Until is still the length the client asked for: the stamp that makes it a
		// deadline comes after this check. ApplyLease refuses a length outside the bounds,
		// and a refusal there would come after the append.
		return cluster.CodeInvalidArgument, nil
	}
	code, err = a.liveTarget(slot, db, op)
	if err != nil || code != 0 {
		return code, err
	}
	if a.leaseAggregateRefusal(slot, db, op, engine) != "" {
		return cluster.CodeInvalidArgument, nil
	}
	return 0, nil
}

// liveTarget is CheckStoreVerbTarget over a fresh snapshot.
func (a *DocumentApplier) liveTarget(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	code, err := CheckStoreVerbTarget(snapshot, DocumentKey{Ref: storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: op.BucketID}, EncID: op.EncID})
	if err != nil {
		return 0, fmt.Errorf("checking the target of class 0x%02x: %w", op.Class, err)
	}
	if code != 0 {
		return code, nil
	}
	// An expired document is absent, so a write cannot resurrect it by moving its time or
	// pushing into it (expiry spec 3.2).
	expired, err := a.documentExpired(snapshot, slot, db, op)
	if err != nil {
		return 0, err
	}
	if expired {
		return cluster.CodeNotFound, nil
	}
	return 0, nil
}

// Explain words the refusals Precondition returns. The owner holds ids, not names, so the
// text names ids and the fix a developer can type with the names they know.
func (a *DocumentApplier) Explain(code cluster.Code, slot uint16, db uint64, op cluster.Op) (string, bool) {
	where := fmt.Sprintf("document %x in bucket %d", op.EncID, op.BucketID)
	if code == cluster.CodeNotFound && op.Class != cluster.OpInsert && op.Class != cluster.OpUpsert {
		snapshot := a.kv.NewSnapshot()
		expired, err := a.documentExpired(snapshot, slot, db, op)
		snapshot.Discard()
		if err == nil && expired {
			return fmt.Sprintf("%s has expired: its 'expire at' time has passed, so it reads as absent and cannot be changed. Nothing was written. Insert it again with a later expire time, or run 'describe bucket <bucket> in <database>' to see the expiry the bucket declares", where), true
		}
	}
	if op.Class == cluster.OpUpdate {
		return a.explainUpdate(code, slot, db, op, where)
	}
	if op.Class == cluster.OpDelete && code == cluster.CodeNotFound {
		return fmt.Sprintf("%s does not exist, so there is nothing to delete under the guard. Nothing was changed. Check the id with db.<bucket>.list({limit: 10}), or delete without 'if'", where), true
	}
	incResult := ""
	if code == cluster.CodeResultTooLarge && op.Class == cluster.OpStoreVerb {
		incResult = a.explainIncResult(slot, db, op, where)
	}
	switch {
	case code == cluster.CodeNotFound:
		return fmt.Sprintf("%s does not exist, so its store field cannot be changed. Nothing was written. Insert the document first with db.<bucket>.insert({_id_: \"<id>\", …})", where), true
	case code == cluster.CodeFieldType && storeClass(op.Class):
		return fmt.Sprintf("%s: the store field the write names is not declared in bucket %d any more, perhaps dropped or altered while the write was on its way. Nothing was written. Run 'show buckets in <database>' to see the bucket's current fields, then write to one of them", where, op.BucketID), true
	case code == cluster.CodeResultTooLarge && op.Class == cluster.OpStoreVerb && a.popNCount(op) > 0:
		return a.explainPopN(slot, db, op, where), true
	case incResult != "":
		return incResult, true
	case code == cluster.CodeResultTooLarge && op.Class == cluster.OpStoreVerb:
		return fmt.Sprintf("the element a pop or take would return from the store field on %s is larger than the %d bytes a dedupe entry holds for a retry. Nothing was removed. Read it with claim and remove it with ack, which carry no such limit, or raise replication.dedupe_window's result cap", where, a.resultCap), true
	case code == cluster.CodeResultTooLarge && op.Class == cluster.OpClaim:
		return fmt.Sprintf("a claim on %s would record %d bytes for its retry and a dedupe entry holds at most %d. Nothing was leased. Raise replication.dedupe_window's result cap, or claim fewer messages", where, claimResultBytes, a.resultCap), true
	case code == cluster.CodeResultTooLarge:
		return fmt.Sprintf("a %s on %s would record %d bytes for its retry and a dedupe entry holds at most %d. Nothing was changed. Raise replication.dedupe_window's result cap", leaseClassName(op.Class), where, leaseResultBytes(op.Class), a.resultCap), true
	case code != cluster.CodeInvalidArgument:
		return "", false
	case storeClass(op.Class):
		if text := a.explainInc(slot, db, op); text != "" {
			return fmt.Sprintf("%s: %s", where, text), true
		}
		if text := a.explainAggregate(slot, db, op); text != "" {
			return fmt.Sprintf("%s: %s", where, text), true
		}
		return a.explainStoreOp(slot, db, op, where), true
	case (op.Class == cluster.OpInsert || op.Class == cluster.OpUpsert) && a.insertShapeRefusal(slot, db, op) != "":
		return fmt.Sprintf("%s: %s", where, a.insertShapeRefusal(slot, db, op)), true
	case op.Class == cluster.OpInsert || op.Class == cluster.OpUpsert:
		return fmt.Sprintf("%s was deleted and the keys it left are too many to remove in the same batch as this write. Nothing was written. Finish the delete with db.<bucket>.delete({_id_: \"<id>\"}), wait for it, then write again", where), true
	}
	return tombstoneText(where), true
}

// popNCount is the n of a pop {n} op, or 0 for anything else.
func (a *DocumentApplier) popNCount(op cluster.Op) int {
	if len(op.WrittenFields()) != 1 || op.WrittenFields()[0].Verb != cluster.VerbPop || len(op.WrittenFields()[0].Value) == 0 {
		return 0
	}
	arg, err := decodeVerbArg(shapePop, op.WrittenFields()[0].Value)
	if err != nil {
		return 0
	}
	return arg.Count
}

// explainPopN words the refusal of a pop {n} whose elements would not fit one dedupe entry,
// naming the count that does.
func (a *DocumentApplier) explainPopN(slot uint16, db uint64, op cluster.Op, where string) string {
	n := a.popNCount(op)
	engine, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
	fits := 0
	if err == nil {
		fits, _, _ = a.popFit(slot, db, op, engine, n)
	}
	if fits == 0 {
		return fmt.Sprintf("the next element of the store field on %s is larger than the %d bytes a dedupe entry holds for a retry. Nothing was removed. Read it with claim and remove it with ack, which carry no such limit, or raise replication.dedupe_window's result cap", where, a.resultCap)
	}
	return fmt.Sprintf("a pop of %d from the store field on %s would record more than the %d bytes a dedupe entry holds for a retry. Nothing was removed. Pop %d or fewer, or use claim and ack, which carry no such limit, or raise replication.dedupe_window's result cap", n, where, a.resultCap, fits)
}

// storeClass reports whether the class works on one store field of a live document.
func storeClass(class byte) bool {
	switch class {
	case cluster.OpStoreVerb, cluster.OpClaim, cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
		return true
	}
	return false
}

func tombstoneText(where string) string {
	return fmt.Sprintf("%s is deleted and its remaining keys are still being removed, so its store fields cannot be changed. Nothing was written. Retry in a moment, or insert the document again", where)
}

// explainStoreOp words an INVALID_ARGUMENT of a store verb or lease, which has two causes
// that look alike: a deleted target, and a verb, arg or field that does not fit. It reads
// the target's state to tell them apart.
func (a *DocumentApplier) explainStoreOp(slot uint16, db uint64, op cluster.Op, where string) string {
	if op.Class == cluster.OpStoreVerb && len(op.WrittenFields()) == 1 {
		if arg, err := decodeVerbArg(shapeElement, op.WrittenFields()[0].Value); err == nil {
			if refusal, _ := a.pushExpiryRefusal(db, op, arg.Element); refusal != "" {
				return refusal
			}
		}
	}
	if code, err := a.liveTarget(slot, db, op); err == nil && code == cluster.CodeInvalidArgument {
		return tombstoneText(where)
	}
	if op.Class == cluster.OpStoreVerb {
		return a.explainVerb(db, op, where)
	}
	return a.explainLease(db, op, where)
}

// explainLease words a lease class refused for its field, which is not a fifoStore or
// lifoStore.
func (a *DocumentApplier) explainLease(db uint64, op cluster.Op, where string) string {
	kind := "field"
	if len(op.WrittenFields()) == 1 {
		if engine, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID); err == nil {
			if leasable(engine) && op.Class == cluster.OpExtend {
				return fmt.Sprintf("%s: an extend asks for a lease of %d ms and a lease is %d to %d ms. Nothing was changed. Send a length within that range",
					where, op.Until, store.MinLeaseMs, store.MaxLeaseMs)
			}
			kind = fmt.Sprintf("field, a %sStore,", engine)
		}
	}
	return fmt.Sprintf("%s: the store %s cannot be claimed, because only a fifoStore or lifoStore delivers messages in order. Nothing was leased. Claim a fifoStore or lifoStore field, or read a heapStore with pop and a hashStore or btreeStore with take", where, kind)
}

// claimResultBytes is the lease id and deadline a claim records for a retry (conv §18).
const claimResultBytes = 24

// leaseResultBytes is what a lease class records for a retry: a claim its lease id and
// deadline, a release the state it left the lease in, an extend that state and the new
// deadline, and an ack nothing.
func leaseResultBytes(class byte) int {
	switch class {
	case cluster.OpClaim:
		return claimResultBytes
	case cluster.OpRelease:
		return releaseResultBytes
	case cluster.OpExtend:
		return extendResultBytes
	}
	return 0
}

func leaseClassName(class byte) string {
	switch class {
	case cluster.OpRelease:
		return "release"
	case cluster.OpExtend:
		return "extend"
	}
	return "claim"
}

// Fill writes the operation's keys and the pair record into batch.
func (a *DocumentApplier) Fill(batch storage.KVBatch, rec AppliedRecord) error {
	_, err := a.FillWithResult(batch, rec)
	return err
}

// FillWithResult is Fill that also returns the bytes the dedupe entry records as
// the operation's result: a claim's lease id and deadline (conventions section
// 18), nil for every other class.
func (a *DocumentApplier) FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if source, ok := a.engines.(pairEngineSource); ok && rec.BootReplay {
		// A boot replay's record reads its declarations through a source bound to its own pair, so
		// the one rule that reads a declaration from this node's store sees which pair applies and
		// no other apply can borrow it (deferred-pair gate spec, section 5).
		replaying := *a
		replaying.engines = source.inPair(rec)
		return replaying.fillWithResult(batch, rec)
	}
	return a.fillWithResult(batch, rec)
}

func (a *DocumentApplier) fillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if len(rec.Op.EncID) == 0 {
		return nil, errors.New("the operation carries no document id; the coordinator mints it before routing and the owner never does")
	}
	var recorded []byte
	var err error
	switch rec.Op.Class {
	case cluster.OpInsert, cluster.OpUpsert:
		err = a.fillInsert(batch, rec)
	case cluster.OpDelete:
		if rec.Op.Return != cluster.ReturnNone {
			recorded, err = a.fillDeleteWithResult(batch, rec)
		} else {
			err = a.fillDelete(batch, rec)
		}
	case cluster.OpUpdate:
		if rec.Op.Return != cluster.ReturnNone {
			recorded, err = a.fillUpdateWithResult(batch, rec)
		} else {
			err = a.fillUpdate(batch, rec)
		}
	case cluster.OpStoreVerb:
		recorded, err = a.fillVerb(batch, rec)
	default:
		recorded, err = a.fillLease(batch, rec)
	}
	if err != nil {
		return nil, err
	}
	if a.pairs == nil {
		return recorded, nil
	}
	if err := a.pairs.RecordApplied(batch, rec); err != nil {
		return nil, fmt.Errorf("writing the pair record of position %s: %w", rec.Pos, err)
	}
	return recorded, nil
}

// fillLease applies a claim, ack, release or extend from a fresh snapshot of the
// owner's state, under the pair lock the caller holds.
func (a *DocumentApplier) fillLease(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if len(rec.Op.WrittenFields()) != 1 {
		return nil, fmt.Errorf("position %s: operation class 0x%02x names %d store fields, want 1", rec.Pos, rec.Op.Class, len(rec.Op.WrittenFields()))
	}
	engine, err := a.engines.FieldEngine(rec.DB, rec.Op.BucketID, rec.Op.WrittenFields()[0].FieldID)
	if errors.Is(err, errRetiredField) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("position %s: finding the engine of field %d: %w", rec.Pos, rec.Op.WrittenFields()[0].FieldID, err)
	}
	target := LeaseTarget{Slot: rec.Slot, DB: rec.DB, Engine: engine, Position: rec.Pos}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref, err := refOf(target, rec.Op)
	if err != nil {
		return nil, err
	}
	target.Aggregates = aggregateBatch(snapshot, batch, ref, rec.Op)
	target.LeaveKeys = len(rec.Op.Aggregates)
	target.LeaveBytes = target.LeaveKeys * int(aggregateKeyBytes(len(ref.StorePrefix())))
	if rec.Op.Class == cluster.OpClaim {
		var dropErr error
		if item, expires := expireItemFor(rec.Op, rec.Op.WrittenFields()[0].FieldID); expires {
			expiredAt := expiredElement(item, rec.Op.ExpireNow)
			target.Expired = func(entry []byte) bool { return expiredAt(store.FieldEntry{Value: entry}) }
			target.Discard = func(position uint64, entry []byte) {
				if dropErr == nil {
					dropErr = dropElementExpiry(batch, rec, item, position, entry)
				}
			}
		}
		outcome, err := ApplyClaim(snapshot, batch, target, rec.Op)
		if err == nil {
			err = dropErr
		}
		if err != nil {
			return nil, err
		}
		if err := target.Aggregates.Flush(); err != nil {
			return nil, fmt.Errorf("writing the aggregates of position %s: %w", rec.Pos, err)
		}
		if rec.ClaimSink != nil && !outcome.Replayed {
			rec.ClaimSink(outcome.Elements)
		}
		return outcome.Result(), nil
	}
	if item, expires := expireItemFor(rec.Op, rec.Op.WrittenFields()[0].FieldID); expires && rec.Op.Class == cluster.OpAck {
		var dropErr error
		target.Acked = func(position uint64, element []byte) {
			if dropErr == nil {
				dropErr = dropElementExpiry(batch, rec, item, position, element)
			}
		}
		outcome, err := ApplyLease(snapshot, batch, target, rec.Op)
		if err != nil {
			return nil, err
		}
		if dropErr != nil {
			return nil, dropErr
		}
		return leaseResult(rec.Op.Class, outcome), nil
	}
	outcome, err := ApplyLease(snapshot, batch, target, rec.Op)
	if err != nil {
		return nil, err
	}
	if err := target.Aggregates.Flush(); err != nil {
		return nil, fmt.Errorf("writing the aggregates of position %s: %w", rec.Pos, err)
	}
	return leaseResult(rec.Op.Class, outcome), nil
}

// insertPath is the insert the record describes, with the batch bound a tombstone's
// cleanup must fit.
func (a *DocumentApplier) insertPath(slot uint16, db uint64, op cluster.Op) InsertPath {
	fields := make([]store.DocumentField, len(op.WrittenFields()))
	for i, field := range op.WrittenFields() {
		fields[i] = store.DocumentField{FieldID: field.FieldID, Stored: field.Value}
	}
	return InsertPath{
		Insert: store.DocumentInsert{
			Ref:    storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: op.BucketID},
			EncID:  op.EncID,
			Header: op.IDValue,
			Fields: fields,
		},
		Limit: a.kv.MaxBatchBytes(),
	}
}

// fillInsert writes an insert, or an upsert: an upsert over a live document rewrites only
// the fields it carries and leaves the rest and every store (placement §6), and over a
// tombstone an insert or upsert takes the insert path, cleanup included. The precondition
// has already refused an insert over a live document.
func (a *DocumentApplier) fillInsert(batch storage.KVBatch, rec AppliedRecord) error {
	request := a.insertPath(rec.Slot, rec.DB, rec.Op)
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	state, err := ReadHeaderState(snapshot, request.document())
	if err != nil {
		return fmt.Errorf("writing the insert of position %s: %w", rec.Pos, err)
	}
	if state == HeaderLive && expiredOnRecord(snapshot, rec) {
		// A live document past its expire time is replaced as a tombstone is, judged at the
		// owner's clock the record carries, so every copy replaces it or none does.
		if err := insertOverStale(snapshot, batch, request); err != nil {
			return fmt.Errorf("replacing the expired document of position %s: %w", rec.Pos, err)
		}
		return applyExpiryDocument(snapshot, batch, rec)
	}
	if state == HeaderLive {
		// An upsert over a live document, or a replay of an insert the pair record had not
		// yet counted: the same keys are written again.
		if err := store.WriteInsert(batch, request.Insert); err != nil {
			return fmt.Errorf("rewriting the document of position %s: %w", rec.Pos, err)
		}
		return applyExpiryDocument(snapshot, batch, rec)
	}
	if _, err := WriteInsertPath(snapshot, batch, request); err != nil {
		return fmt.Errorf("writing the insert of position %s: %w", rec.Pos, err)
	}
	return applyExpiryDocument(snapshot, batch, rec)
}

func (a *DocumentApplier) fillDelete(batch storage.KVBatch, rec AppliedRecord) error {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	doc := DocumentKey{Ref: storage.BucketRef{Slot: rec.Slot, DatabaseID: rec.DB, BucketID: rec.Op.BucketID}, EncID: rec.Op.EncID}
	if err := ApplyDelete(snapshot, batch, doc, rec.Pos); err != nil {
		return fmt.Errorf("writing the delete of position %s: %w", rec.Pos, err)
	}
	return applyExpiryDocument(snapshot, batch, rec)
}

// verbTakes is what each store verb takes, by verb, in the words the refusal gives.
var verbTakes = map[byte]struct{ name, takes string }{
	cluster.VerbPush: {"push", "a fifoStore or lifoStore field with {v}, or a heapStore field with {p, e, v}"},
	cluster.VerbPop:  {"pop", "a fifoStore, lifoStore or heapStore field with no arg or {n}"},
	cluster.VerbPut:  {"put", "a hashStore or btreeStore field with {k, v}"},
	cluster.VerbInc:  {"inc", "a hashStore or btreeStore field with {k, v[, r]}"},
	cluster.VerbTake: {"take", "a hashStore or btreeStore field with {k}, or a heapStore field with {e}"},
}

// explainVerb words an INVALID_ARGUMENT of a store verb. The owner path holds ids, so it asks
// the declaration for the names and the field's declared type (the ring client does not check
// the type before it sends, so this text is the authority). It tells the two causes apart: a
// verb the field's store type does not take, and an arg that is not the layout the verb takes
// on that type.
func (a *DocumentApplier) explainVerb(db uint64, op cluster.Op, where string) string {
	limits := fmt.Sprintf("k and e are 1 to %d bytes and the arg at most %d bytes", maxVerbKeyBytes, maxVerbArgBytes)
	fields := op.WrittenFields()
	if len(fields) != 1 {
		return fmt.Sprintf("%s: the store verb does not fit the field or its arg is not the layout the verb takes. Nothing was written. %s", where, limits)
	}
	verb, known := verbTakes[fields[0].Verb]
	if !known {
		verb.name = fmt.Sprintf("verb %d", fields[0].Verb)
	}
	target, describe := fmt.Sprintf("field %d of bucket %d", fields[0].FieldID, op.BucketID), "describe bucket <bucket> in <database>"
	if namer, ok := a.engines.(expireNames); ok {
		if database, bucket, field, found := namer.ExpireNames(db, op.BucketID, fields[0].FieldID); found {
			target = fmt.Sprintf("field %q of bucket %q in database %q", field, bucket, database)
			describe = fmt.Sprintf("describe bucket %s in %s", bucket, database)
		}
	}
	engine, err := a.engines.FieldEngine(db, op.BucketID, fields[0].FieldID)
	if err != nil {
		return fmt.Sprintf("%s: %s cannot be applied to %s. Nothing was written. %s takes %s. Run '%s' to see the bucket's fields and their types. %s", where, verb.name, target, verb.name, verb.takes, describe, limits)
	}
	if _, fits := verbShapeOf(fields[0].Verb, engine); !fits {
		return fmt.Sprintf("%s: %s cannot be applied to %s, which is a %sStore. Nothing was written. %s takes %s. Run '%s' to see the bucket's fields and their types, then use a field of the type %s takes. %s",
			where, verb.name, target, engine, verb.name, verb.takes, describe, verb.name, limits)
	}
	return fmt.Sprintf("%s: the arg of %s is not the layout %s takes on a %sStore field. Nothing was written. %s takes %s. %s",
		where, target, verb.name, engine, verb.name, verb.takes, limits)
}

// fillVerb applies a push, put, pop or take; a pop or take returns the element it removed,
// which the dedupe entry records so a retry hands out the same one.
func (a *DocumentApplier) fillVerb(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	field := rec.Op.WrittenFields()[0]
	engine, err := a.engines.FieldEngine(rec.DB, rec.Op.BucketID, field.FieldID)
	if errors.Is(err, errRetiredField) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("position %s: finding the engine of field %d: %w", rec.Pos, field.FieldID, err)
	}
	if rec.Op.System && systemTakesPosition(field.Verb, engine) {
		return nil, a.fillSystemTake(batch, rec, engine)
	}
	shape, fits := verbShapeOf(field.Verb, engine)
	if !fits {
		return nil, fmt.Errorf("position %s: store verb %d does not fit a %s field", rec.Pos, field.Verb, engine)
	}
	arg, err := decodeVerbArg(shape, field.Value)
	if err != nil {
		return nil, fmt.Errorf("position %s: reading the arg of store verb %d: %w", rec.Pos, field.Verb, err)
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	// The bucket-wide byte counter the field store updates is not kept by the owner path,
	// so a throwaway state takes it.
	var discarded store.BucketState
	write := store.FieldWrite{Snap: snapshot, State: &discarded, Batch: batch}
	ref := a.fieldRef(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID, field.FieldID, engine)
	agg := aggregateBatch(snapshot, batch, ref, rec.Op)
	recorded, err := a.applyVerb(write, ref, shape, arg, agg, rec)
	if err != nil {
		return nil, err
	}
	if err := agg.Flush(); err != nil {
		return nil, fmt.Errorf("writing the aggregates of position %s: %w", rec.Pos, err)
	}
	return recorded, nil
}

// applyVerb is the store call of one verb with the aggregate deltas of the elements it adds and
// removes. agg is nil for a store with no aggregates.
func (a *DocumentApplier) applyVerb(write store.FieldWrite, ref store.FieldRef, shape verbShape, arg verbArg, agg *store.AggregateBatch, rec AppliedRecord) ([]byte, error) {
	var err error
	switch shape {
	case shapePop:
		if arg.Count > 0 {
			return a.fillPopN(write, ref, arg.Count, agg, rec)
		}
		var popped store.FieldEntry
		delivered, err := popLive(write, ref, 1, agg, rec, func(entry store.FieldEntry) error {
			popped = entry
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("writing the pop of position %s: %w", rec.Pos, err)
		}
		if delivered == 0 {
			return nil, nil
		}
		if err := aggAdd(agg, popped.Value, -1); err != nil {
			return nil, fmt.Errorf("position %s: %w", rec.Pos, err)
		}
		return popped.Value, nil
	case shapeElement:
		if err = aggAdd(agg, arg.Element, 1); err == nil {
			var position uint64
			position, err = store.FieldPush(write, ref, arg.Element)
			if err == nil {
				if item, expires := expireItemFor(rec.Op, ref.FieldID); expires {
					err = setElementExpiry(write.Batch, rec, item, position, arg.Element)
				}
			}
		}
	case shapeHeapPush:
		if agg != nil {
			old, found, readErr := heapElementAt(write.Snap, ref, arg.Priority, arg.EntryID)
			if readErr != nil {
				return nil, fmt.Errorf("position %s: %w", rec.Pos, readErr)
			}
			if found {
				err = aggAdd(agg, old, -1)
			}
		}
		if err == nil {
			err = aggAdd(agg, arg.Element, 1)
		}
		if err == nil {
			err = store.FieldHeapPush(write, ref, arg.Priority, arg.EntryID, arg.Element)
		}
	case shapePut:
		if agg != nil {
			old, found, readErr := store.FieldGet(write.Snap, ref, arg.Key)
			if readErr != nil {
				return nil, fmt.Errorf("position %s: reading the element a put replaces: %w", rec.Pos, readErr)
			}
			if found {
				err = aggAdd(agg, old, -1)
			}
		}
		if err == nil {
			err = aggAdd(agg, arg.Element, 1)
		}
		if err == nil {
			err = store.FieldPut(write, ref, arg.Key, arg.Element)
		}
	case shapeInc:
		return a.fillInc(write, ref, arg, agg, rec)
	case shapeTakeKey:
		return a.fillTakeKey(write, ref, arg.Key, agg, rec)
	case shapeTakeEntry:
		taken, found, takeErr := store.FieldHeapTake(write, ref, arg.EntryID)
		if takeErr != nil {
			return nil, fmt.Errorf("writing the take of position %s: %w", rec.Pos, takeErr)
		}
		if !found {
			return nil, nil
		}
		if err := aggAdd(agg, taken.Value, -1); err != nil {
			return nil, fmt.Errorf("position %s: %w", rec.Pos, err)
		}
		return taken.Value, nil
	}
	if err != nil {
		return nil, fmt.Errorf("writing the store verb of position %s: %w", rec.Pos, err)
	}
	return nil, nil
}

// fillPopN removes up to n elements and returns them packed, which the dedupe entry records
// so a retry hands out the same ones.
func (a *DocumentApplier) fillPopN(write store.FieldWrite, ref store.FieldRef, n int, agg *store.AggregateBatch, rec AppliedRecord) ([]byte, error) {
	var elements [][]byte
	if _, err := popLive(write, ref, n, agg, rec, func(entry store.FieldEntry) error {
		elements = append(elements, bytes.Clone(entry.Value))
		return aggAdd(agg, entry.Value, -1)
	}); err != nil {
		return nil, fmt.Errorf("writing the pop of %d of position %s: %w", n, rec.Pos, err)
	}
	return packPopped(elements), nil
}

// fillTakeKey removes key from a hash or btree field and returns the element it held.
func (a *DocumentApplier) fillTakeKey(write store.FieldWrite, ref store.FieldRef, key string, agg *store.AggregateBatch, rec AppliedRecord) ([]byte, error) {
	element, found, err := store.FieldGet(write.Snap, ref, key)
	if err != nil {
		return nil, fmt.Errorf("reading the element for the take of position %s: %w", rec.Pos, err)
	}
	if !found {
		return nil, nil
	}
	if err := aggAdd(agg, element, -1); err != nil {
		return nil, fmt.Errorf("position %s: %w", rec.Pos, err)
	}
	if _, err := store.FieldDelete(write, ref, key); err != nil {
		return nil, fmt.Errorf("writing the take of position %s: %w", rec.Pos, err)
	}
	return element, nil
}

// pushExpiryRefusal is W-E1: the words refusing a push whose element lacks a datetime at the
// path the field's expiry names, or "" when the field does not expire or the element is fine.
// The error is a failed read of the declaration. An engine source that knows no expiry
// tables, a field that is not declared to expire and a bucket with no expiry all answer "".
func (a *DocumentApplier) pushExpiryRefusal(db uint64, op cluster.Op, element []byte) (string, error) {
	if len(op.WrittenFields()) != 1 {
		return "", nil
	}
	table, err := a.expireTable(db, op)
	if err != nil {
		return "", err
	}
	for _, expiring := range table.Stores {
		if expiring.FieldID == op.WrittenFields()[0].FieldID {
			database, bucket, field := fmt.Sprintf("<database %d>", db), fmt.Sprintf("<bucket %d>", op.BucketID), fmt.Sprintf("<field %d>", expiring.FieldID)
			if namer, ok := a.engines.(expireNames); ok {
				if named, namedBucket, namedField, found := namer.ExpireNames(db, op.BucketID, expiring.FieldID); found {
					database, bucket, field = named, namedBucket, namedField
				}
			}
			return pushExpiryRefusal(database, bucket, field, string(expiring.Path), element), nil
		}
	}
	return "", nil
}

// expireTable is the expiry declaration the owner's precondition judges op under: the one the Writer
// admitted the write with, which is the only view consistent with the items the record will carry,
// and for a caller with no admission the cache's. An engine source that knows no expiry tables
// declares none.
func (a *DocumentApplier) expireTable(db uint64, op cluster.Op) (cluster.AdmittedExpire, error) {
	if op.AdmittedExpire != nil {
		return *op.AdmittedExpire, nil
	}
	source, ok := a.engines.(ExpireTableSource)
	if !ok {
		return cluster.AdmittedExpire{}, nil
	}
	table, err := source.ExpireTable(db, op.BucketID)
	if err != nil {
		return cluster.AdmittedExpire{}, fmt.Errorf("reading the expiry declaration of bucket %d: %w", op.BucketID, err)
	}
	admitted := cluster.AdmittedExpire{BucketField: table.BucketField}
	for _, expiring := range table.Stores {
		admitted.Stores = append(admitted.Stores, cluster.ExpireItem{FieldID: expiring.FieldID, Path: []byte(expiring.Path)})
	}
	return admitted, nil
}

// documentExpired reports whether the live document is past its expire time at the owner's
// clock. A bucket with no expiry, a document without the field and one whose field is not a
// datetime are never expired.
func (a *DocumentApplier) documentExpired(snapshot storage.KVSnapshot, slot uint16, db uint64, op cluster.Op) (bool, error) {
	table, err := a.expireTable(db, op)
	if err != nil || table.BucketField == 0 {
		return false, err
	}
	stored, err := readScalar(snapshot, store.DocumentPrefix(slot, db, op.BucketID, op.EncID), table.BucketField)
	if err != nil {
		return false, fmt.Errorf("reading the expire field of document %x: %w", op.EncID, err)
	}
	return expiredAt(stored, a.attemptNow(op)), nil
}

// expiredOnRecord is the apply's own question, with no clock of its own: was the live
// document past its expire time at the instant the owner stamped into the record? A record
// with no expire item for the bucket form never replaces a live document.
func expiredOnRecord(snapshot storage.KVSnapshot, rec AppliedRecord) bool {
	for _, item := range rec.Op.Expire {
		if len(item.Path) != 0 {
			continue
		}
		stored, err := readScalar(snapshot, store.DocumentPrefix(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID), item.FieldID)
		return err == nil && expiredAt(stored, rec.Op.ExpireNow)
	}
	return false
}

// popLive pops n elements of an expiring or ordinary queue. Elements past the owner's clock the
// record carries are skipped and removed in the same batch; every element removed, delivered or
// skipped, has its expiry entry dropped. visit sees only the delivered ones.
func popLive(write store.FieldWrite, ref store.FieldRef, n int, agg *store.AggregateBatch, rec AppliedRecord, visit func(store.FieldEntry) error) (int, error) {
	item, expires := expireItemFor(rec.Op, ref.FieldID)
	if !expires {
		return store.FieldPopLive(write, ref, n, nil, nil, visit)
	}
	drop := func(entry store.FieldEntry) error {
		return dropElementExpiry(write.Batch, rec, item, entry.Position, entry.Value)
	}
	// A skipped element leaves the store for good, so a maintained aggregate drops it too; a
	// delivered one is subtracted by the caller's visit.
	discard := func(entry store.FieldEntry) error {
		if err := aggAdd(agg, entry.Value, -1); err != nil {
			return fmt.Errorf("position %s: %w", rec.Pos, err)
		}
		return drop(entry)
	}
	return store.FieldPopLive(write, ref, n, expiredElement(item, rec.Op.ExpireNow), discard, func(entry store.FieldEntry) error {
		if err := drop(entry); err != nil {
			return err
		}
		return visit(entry)
	})
}

// expiredElement is the predicate of a queue walk: is the element's datetime at the item's path
// at or before nowMillis? An element with none never expires.
func expiredElement(item cluster.ExpireItem, nowMillis int64) func(store.FieldEntry) bool {
	return func(entry store.FieldEntry) bool {
		atMillis, _, ok := elementExpiry(entry.Value, item.Path)
		return ok && atMillis <= nowMillis
	}
}

// documentAbsent is the one definition of "the document is not there" for a write: no header,
// a tombstone, or a live document past its expire time at the owner's clock (expiry spec 3.2).
// state is the header state the caller already read from snapshot. The insert precondition and
// any later default fill ask this, so they agree on what an insert may land on.
func (a *DocumentApplier) documentAbsent(snapshot storage.KVSnapshot, slot uint16, db uint64, op cluster.Op, state HeaderState) (bool, error) {
	if state != HeaderLive {
		return true, nil
	}
	return a.documentExpired(snapshot, slot, db, op)
}

// attemptNow is the one instant an ordering attempt judges expiry at. The Writer reads the
// clock once per attempt and puts it in op.ExpireNow before the precondition; the same value is
// then stamped into the record, so the precondition and the apply, on the owner and on every
// copy, give one answer at the boundary instant. Only a caller outside the write path (a test,
// the explain text) has none, and falls back to the applier's clock.
func (a *DocumentApplier) attemptNow(op cluster.Op) int64 {
	if op.ExpireNow != 0 {
		return op.ExpireNow
	}
	return clockOrSystem(a.wall)()
}

// expiredPredicate is the test the apply of a pop of op's field uses to step over expired
// elements, from the expiry the write is judged under and its attempt's instant; nil when the
// field does not expire its elements. The size checks of a pop walk the same elements the apply
// delivers, or a result the check passed could exceed the dedupe cap after the append.
func (a *DocumentApplier) expiredPredicate(db uint64, op cluster.Op) (func(store.FieldEntry) bool, error) {
	table, err := a.expireTable(db, op)
	if err != nil {
		return nil, err
	}
	for _, expiring := range table.Stores {
		if expiring.FieldID == op.WrittenFields()[0].FieldID {
			return expiredElement(expiring, a.attemptNow(op)), nil
		}
	}
	return nil, nil
}

// explainInc is the text of an inc refused for the numbers of its element (W-I1, W-I2), "" when the op
// is not an inc or was refused for another reason.
func (a *DocumentApplier) explainInc(slot uint16, db uint64, op cluster.Op) string {
	if op.Class != cluster.OpStoreVerb || len(op.WrittenFields()) != 1 || op.WrittenFields()[0].Verb != cluster.VerbInc {
		return ""
	}
	engine, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
	if err != nil {
		return ""
	}
	arg, err := decodeVerbArg(shapeInc, op.WrittenFields()[0].Value)
	if err != nil {
		return ""
	}
	text, err := a.incRefusal(slot, db, op, engine, arg)
	if err != nil {
		return ""
	}
	return text
}

// explainAggregate is the text of an aggregate refusal of a store verb, "" when the op was
// refused for another reason. It repeats the dry run, because Explain is given the op alone.
func (a *DocumentApplier) explainAggregate(slot uint16, db uint64, op cluster.Op) string {
	if len(op.WrittenFields()) != 1 || len(op.Aggregates) == 0 {
		return ""
	}
	if op.Class != cluster.OpStoreVerb {
		engine, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
		if err != nil {
			return ""
		}
		return a.leaseAggregateRefusal(slot, db, op, engine)
	}
	engine, err := a.engines.FieldEngine(db, op.BucketID, op.WrittenFields()[0].FieldID)
	if err != nil {
		return ""
	}
	shape, fits := verbShapeOf(op.WrittenFields()[0].Verb, engine)
	if !fits {
		return ""
	}
	arg, err := decodeVerbArg(shape, op.WrittenFields()[0].Value)
	if err != nil {
		return ""
	}
	text, err := a.aggregateRefusal(slot, db, op, engine, arg)
	if err != nil {
		return ""
	}
	return text
}
