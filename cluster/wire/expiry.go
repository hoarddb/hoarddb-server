package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// expireItemsOf returns the expire items an operation needs, from the bucket's declaration
// (expiry spec 3.1). The owner calls it under the pair lock with the declaration it admitted
// the write against; the applier never reads a declaration.
//
// A bucket-form declaration (table.BucketField set) stamps one item for every insert and upsert,
// for an update that touches the field, and one for every delete (the applier reads the old value).
// A store-form declaration stamps one item for a store verb whose single field is a declared
// expiring queue. An operation is a document write or a store verb, never both, so a write
// returns at most one form. Nil when the table is the zero value or nothing applies.
func expireItemsOf(table machine.ExpireTable, op cluster.Op) []cluster.ExpireItem {
	if table.BucketField == 0 && len(table.Stores) == 0 {
		return nil
	}
	if op.Class == cluster.OpDelete {
		return deleteExpireItems(table)
	}
	if table.BucketField != 0 {
		switch op.Class {
		case cluster.OpInsert, cluster.OpUpsert:
			// Always: an insert or upsert over a document that has expired replaces it whole,
			// and the apply needs the clock to know that, whether or not the write names the field.
			return []cluster.ExpireItem{{FieldID: table.BucketField}}
		case cluster.OpUpdate:
			for _, field := range op.WrittenFields() {
				if field.FieldID == table.BucketField || renamesOnto(field, table.BucketField) {
					return []cluster.ExpireItem{{FieldID: table.BucketField}}
				}
			}
			return nil
		}
	}
	switch op.Class {
	// A release and an extend leave the element at its position and read no item; only a verb,
	// a claim (its filter) and an ack (it drops entries) do.
	case cluster.OpStoreVerb, cluster.OpClaim, cluster.OpAck:
		if len(op.WrittenFields()) != 1 {
			return nil
		}
		for _, store := range table.Stores {
			if store.FieldID == op.WrittenFields()[0].FieldID {
				return []cluster.ExpireItem{{FieldID: store.FieldID, Path: []byte(store.Path)}}
			}
		}
	}
	return nil
}

// deleteExpireItems is every declared item: the bucket field and each expiring store, because
// the applier of a delete drops the document's entry and the entries of its queue elements.
func deleteExpireItems(table machine.ExpireTable) []cluster.ExpireItem {
	items := make([]cluster.ExpireItem, 0, 1+len(table.Stores))
	if table.BucketField != 0 {
		items = append(items, cluster.ExpireItem{FieldID: table.BucketField})
	}
	for _, expiring := range table.Stores {
		items = append(items, cluster.ExpireItem{FieldID: expiring.FieldID, Path: []byte(expiring.Path)})
	}
	// The record's list is ascending by field id, one item per field.
	slices.SortFunc(items, func(left, right cluster.ExpireItem) int { return int(left.FieldID) - int(right.FieldID) })
	return items
}

// renamesOnto reports whether field is a rename whose target is fieldID: the update then
// writes the expire field though it does not name it as its own id.
func renamesOnto(field cluster.FieldOp, fieldID uint16) bool {
	return field.OpCode == cluster.UpdateRenameTo && len(field.Value) == 2 && binary.BigEndian.Uint16(field.Value) == fieldID
}

// datetimeTypeByte is the type byte of a stored BSON datetime scalar.
const datetimeTypeByte = byte(bsontype.DateTime)

// expiryDatetime reads the millis of a stored scalar BSON datetime ([0x09][int64 LE]); ok is
// false for an absent value or any other type, and then there is no entry: such a document
// never expires (expiry spec 2.1).
func expiryDatetime(stored []byte) (millis int64, ok bool) {
	if len(stored) != 9 || stored[0] != datetimeTypeByte {
		return 0, false
	}
	return int64(binary.LittleEndian.Uint64(stored[1:])), true
}

// elementExpiry reads the datetime at path (dot-separated keys) inside a queue element,
// which is a BSON document. holds says what is there when it is not a datetime, for the
// refusal text: "nothing", the BSON type name, or "bytes that are not a BSON document".
func elementExpiry(element, path []byte) (millis int64, holds string, ok bool) {
	if len(path) == 0 {
		return 0, "nothing", false
	}
	document := bson.Raw(element)
	if document.Validate() != nil {
		return 0, "bytes that are not a BSON document", false
	}
	// A valid document that lacks the key is the only other way the lookup fails.
	found, err := document.LookupErr(strings.Split(string(path), ".")...)
	if err != nil {
		return 0, "nothing", false
	}
	millis, isDatetime := found.DateTimeOK()
	if !isDatetime {
		return 0, "a " + found.Type.String(), false
	}
	return millis, "", true
}

func setExpiryEntry(batch storage.KVBatch, entry storage.ExpiryEntry) error {
	if err := batch.Set(storage.ExpiryKey(entry), []byte{}); err != nil {
		return fmt.Errorf("writing the expiry entry of document %x at %d: %w", entry.EncID, entry.AtMillis, err)
	}
	return nil
}

func dropExpiryEntry(batch storage.KVBatch, entry storage.ExpiryEntry) error {
	if err := batch.Delete(storage.ExpiryKey(entry)); err != nil {
		return fmt.Errorf("dropping the expiry entry of document %x at %d: %w", entry.EncID, entry.AtMillis, err)
	}
	return nil
}

// documentEntry is the queue entry of the document a record writes, expiring at atMillis.
func documentEntry(rec AppliedRecord, atMillis int64) storage.ExpiryEntry {
	return storage.ExpiryEntry{
		AtMillis: atMillis, Slot: rec.Slot, DBID: rec.DB, BucketID: rec.Op.BucketID,
		Kind: storage.ExpiryKindDocument, EncID: rec.Op.EncID,
	}
}

// elementEntry is the queue entry of the element at position of one store field.
func elementEntry(rec AppliedRecord, fieldID uint16, position uint64, atMillis int64) storage.ExpiryEntry {
	entry := documentEntry(rec, atMillis)
	entry.Kind, entry.FieldID, entry.Position = storage.ExpiryKindElement, fieldID, position
	return entry
}

// expireItemFor is the record's item for fieldID, or false when the write names none.
func expireItemFor(op cluster.Op, fieldID uint16) (cluster.ExpireItem, bool) {
	for _, stamp := range op.Expire {
		if stamp.FieldID == fieldID {
			return stamp, true
		}
	}
	return cluster.ExpireItem{}, false
}

// nextExpireScalar is what an update leaves in the expire field: its value and whether the
// field is present. It reads the old value from snapshot as fillUpdate does, so the entry
// the applier writes agrees with the key fillUpdate writes.
func nextExpireScalar(snapshot storage.KVSnapshot, prefix []byte, fieldID uint16, old []byte, op cluster.Op) ([]byte, error) {
	next := old
	for _, field := range op.WrittenFields() {
		switch {
		case field.FieldID == fieldID && field.OpCode == cluster.UpdateSet:
			next = field.Value
		case field.FieldID == fieldID && (field.OpCode == cluster.UpdateUnset || field.OpCode == cluster.UpdateRenameTo):
			next = nil
		case field.FieldID == fieldID && updateComputes(field.OpCode):
			computed, err := updateResult(field.OpCode, old, field.Value)
			if err != nil {
				return nil, fmt.Errorf("computing the %s of the expire field: %w", updateCodeName(field.OpCode), err)
			}
			if computed != nil {
				next = computed
			}
		case renamesOnto(field, fieldID):
			moved, err := readScalar(snapshot, prefix, field.FieldID)
			if err != nil {
				return nil, fmt.Errorf("reading the field renamed onto the expire field: %w", err)
			}
			if moved != nil {
				next = moved
			}
		}
	}
	return next, nil
}

// applyExpiryDocument runs the bucket-form rules for one record against snapshot (the
// state before the write) and batch (expiry spec 3.1): an insert or upsert sets one entry
// when the field is a datetime and drops the entry of the value it replaces; an update that
// changes the field drops the old entry and sets the new; an unset drops and sets none; a
// delete drops. The owner and every copy run this from the same record, in the document's
// own batch, so their queues agree.
func applyExpiryDocument(snapshot storage.KVSnapshot, batch storage.KVBatch, rec AppliedRecord) error {
	prefix := store.DocumentPrefix(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID)
	for _, stamp := range rec.Op.Expire {
		if len(stamp.Path) != 0 {
			if rec.Op.Class == cluster.OpDelete {
				if err := dropDeletedElementEntries(snapshot, batch, rec, stamp); err != nil {
					return fmt.Errorf("dropping the element entries of the delete at position %s: %w", rec.Pos, err)
				}
			}
			continue
		}
		old, err := readScalar(snapshot, prefix, stamp.FieldID)
		if err != nil {
			return fmt.Errorf("reading the expire field at position %s: %w", rec.Pos, err)
		}
		var next []byte
		switch rec.Op.Class {
		case cluster.OpInsert, cluster.OpUpsert:
			for _, field := range rec.Op.WrittenFields() {
				if field.FieldID == stamp.FieldID {
					next = field.Value
				}
			}
			// A write over a document that had expired replaced it whole, so its old entry goes
			// even when the new one has no time; one over a live document that does not carry
			// the field leaves the stored one, and its entry, alone.
			if next == nil && !expiredAt(old, rec.Op.ExpireNow) {
				continue
			}
		case cluster.OpUpdate:
			if next, err = nextExpireScalar(snapshot, prefix, stamp.FieldID, old, rec.Op); err != nil {
				return fmt.Errorf("at position %s: %w", rec.Pos, err)
			}
		}
		oldAt, hadOld := expiryDatetime(old)
		nextAt, hasNext := expiryDatetime(next)
		if hadOld && (!hasNext || nextAt != oldAt) {
			if err := dropExpiryEntry(batch, documentEntry(rec, oldAt)); err != nil {
				return fmt.Errorf("at position %s: %w", rec.Pos, err)
			}
		}
		if hasNext {
			// An entry for the same time is set again, not skipped: a re-insert over a
			// tombstone dropped it with the delete.
			if err := setExpiryEntry(batch, documentEntry(rec, nextAt)); err != nil {
				return fmt.Errorf("at position %s: %w", rec.Pos, err)
			}
		}
	}
	return nil
}

// deleteElementDropCap bounds how many element entries one delete drops. A delete only tombstones
// the header, so its batch must stay small however long the queue is; the entries past the cap are
// orphans, which the sweep verifies and drops at their time (expiry spec 3.1). The cap is part of
// the apply, so every copy drops the same ones.
const deleteElementDropCap = 256

// dropDeletedElementEntries drops the entries of the first deleteElementDropCap elements of a
// live document's expiring queue, in the delete's own batch.
func dropDeletedElementEntries(snapshot storage.KVSnapshot, batch storage.KVBatch, rec AppliedRecord, stamp cluster.ExpireItem) error {
	state, err := ReadHeaderState(snapshot, DocumentKey{Ref: storage.BucketRef{Slot: rec.Slot, DatabaseID: rec.DB, BucketID: rec.Op.BucketID}, EncID: rec.Op.EncID})
	if err != nil {
		return fmt.Errorf("reading the header of the deleted document at position %s: %w", rec.Pos, err)
	}
	if state != HeaderLive {
		return nil
	}
	prefix := store.FieldElementKey(store.StorePrefix(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID, stamp.FieldID), storage.NSQueue, nil)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer iter.Close()
	dropped := 0
	for iter.Seek(prefix); iter.Valid() && dropped < deleteElementDropCap; iter.Next() {
		key := iter.Key()
		if len(key) != len(prefix)+8 {
			continue
		}
		element, err := iter.Value()
		if err != nil {
			return fmt.Errorf("reading an element of field %d at position %s: %w", stamp.FieldID, rec.Pos, err)
		}
		if err := dropElementExpiry(batch, rec, stamp, binary.BigEndian.Uint64(key[len(prefix):]), element); err != nil {
			return fmt.Errorf("dropping an element entry of field %d at position %s: %w", stamp.FieldID, rec.Pos, err)
		}
		dropped++
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("walking the elements of field %d at position %s: %w", stamp.FieldID, rec.Pos, err)
	}
	return nil
}

// setElementExpiry sets the entry of a pushed element, when it holds a datetime at the item's path.
func setElementExpiry(batch storage.KVBatch, rec AppliedRecord, stamp cluster.ExpireItem, position uint64, element []byte) error {
	atMillis, _, ok := elementExpiry(element, stamp.Path)
	if !ok {
		return nil
	}
	return setExpiryEntry(batch, elementEntry(rec, stamp.FieldID, position, atMillis))
}

// dropElementExpiry drops the entry of an element a pop or an ack removed.
func dropElementExpiry(batch storage.KVBatch, rec AppliedRecord, stamp cluster.ExpireItem, position uint64, element []byte) error {
	atMillis, _, ok := elementExpiry(element, stamp.Path)
	if !ok {
		return nil
	}
	return dropExpiryEntry(batch, elementEntry(rec, stamp.FieldID, position, atMillis))
}

// expireNames is the optional face of a FieldEngineSource that can name what it resolves, so a
// refusal speaks of the bucket and the field the developer wrote and not of ids.
type expireNames interface {
	// ExpireNames returns the database, bucket and field names of an expiring store field; ok is
	// false when the declaration cannot say (a cold cache, a dropped bucket).
	ExpireNames(db, bucketID uint64, fieldID uint16) (database, bucket, field string, ok bool)
}

// pushExpiryRefusal words the refusal of a push whose element has no datetime at the path an
// expiring store names (W-E1), or "" when the element is fine. The push is refused before a
// position is taken: an element that silently never expires is an expiry that lies.
func pushExpiryRefusal(database, bucket, field, path string, element []byte) string {
	_, holds, ok := elementExpiry(element, []byte(path))
	if ok {
		return ""
	}
	return fmt.Sprintf("Element pushed to \"%s.%s\" has no datetime at %q (it holds %s). The store expires its elements at that path, so the push was refused and nothing was written. Add the field to the element, e.g. {%q: new Date()}, or run 'describe bucket %s in %s' to see the store's declaration.",
		bucket, field, path, holds, path, bucket, database)
}

// expiredAt reports whether a document's expire field (the stored scalar bytes, nil when
// absent) is at or before nowMillis. An absent field, or one that is not a datetime, never
// expires. It is the one comparison every read and the sweep use, so the boundary (at <= now
// is expired) is written once.
func expiredAt(stored []byte, nowMillis int64) bool {
	atMillis, ok := expiryDatetime(stored)
	return ok && atMillis <= nowMillis
}

// clockOrSystem returns clock, or the system clock when clock is nil.
func clockOrSystem(clock func() int64) func() int64 {
	if clock != nil {
		return clock
	}
	return func() int64 { return time.Now().UnixMilli() }
}

// expiredMissText is the ordinary document-not-found text followed by the one sentence that
// says why this miss is not an absence: a developer debugging a vanished session needs the word.
func expiredMissText(plain string) string {
	return plain + ". The document expired: its 'expire at' time has passed, so it reads as absent until the sweep removes it"
}

// systemTakesPosition reports whether a store verb is the owner's own take by position on a
// queue: the one removal the expiry sweep issues for an element. A client take on a queue is
// still refused; this shape exists only when the record's system flag is set, and a client
// body cannot carry that flag.
func systemTakesPosition(verb byte, engine store.FieldEngine) bool {
	return verb == cluster.VerbTake && (engine == store.FieldFIFO || engine == store.FieldLIFO)
}

// takePositionArg reads what a system take names: {k: "<position>@<at>"}, the queue position and
// the datetime the expiry entry promised the element there carries. The take applies only to an
// element that still carries it, so a stale entry (positions can be reused) can never take a
// newer, live element.
func takePositionArg(encodedArg []byte) (position uint64, atMillis int64, err error) {
	arg, err := decodeVerbArg(shapeTakeKey, encodedArg)
	if err != nil {
		return 0, 0, err
	}
	positionText, atText, found := strings.Cut(arg.Key, "@")
	if !found {
		return 0, 0, fmt.Errorf("the take %q names no datetime: it is {k: \"<position>@<millis>\"}", arg.Key)
	}
	if position, err = strconv.ParseUint(positionText, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("the position %q of a take is not a decimal number: %w", positionText, err)
	}
	if atMillis, err = strconv.ParseInt(atText, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("the datetime %q of a take is not a decimal number: %w", atText, err)
	}
	return position, atMillis, nil
}

// elementCarries reports whether element holds the datetime atMillis at path.
func elementCarries(element, path []byte, atMillis int64) bool {
	got, _, ok := elementExpiry(element, path)
	return ok && got == atMillis
}

// systemDeletePrecondition is check 6a for the sweep's delete of a document: the document must
// be live and past its expire time at the owner's clock, read under the pair lock. A document
// an update moved to a later time since the sweep looked is refused here, so the sweep can
// never delete a document that is no longer expired.
func (a *DocumentApplier) systemDeletePrecondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error) {
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	state, err := ReadHeaderState(snapshot, DocumentKey{Ref: storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: op.BucketID}, EncID: op.EncID})
	if err != nil {
		return 0, err
	}
	if state != HeaderLive {
		return cluster.CodeNotFound, nil
	}
	expired, err := a.documentExpired(snapshot, slot, db, op)
	if err != nil {
		return 0, err
	}
	if !expired {
		return cluster.CodeNotFound, nil
	}
	return 0, nil
}

// systemTakePrecondition is check 6a for the sweep's take of one queue element: the document
// must be live and an element must sit at the position now. An element that is leased, or was
// popped or acked since the sweep looked, is not there and the take is refused.
func (a *DocumentApplier) systemTakePrecondition(slot uint16, db uint64, op cluster.Op, engine store.FieldEngine) (cluster.Code, error) {
	position, atMillis, err := takePositionArg(op.WrittenFields()[0].Value)
	if err != nil {
		return cluster.CodeInvalidArgument, nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	state, err := ReadHeaderState(snapshot, DocumentKey{Ref: storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: op.BucketID}, EncID: op.EncID})
	if err != nil {
		return 0, err
	}
	if state != HeaderLive {
		return cluster.CodeNotFound, nil
	}
	ref := store.FieldRef{Owner: storage.Key(slot, db, op.BucketID, storage.NSFieldStore, op.EncID), FieldID: op.WrittenFields()[0].FieldID, Engine: engine}
	key := store.FieldElementKey(ref.StorePrefix(), storage.NSQueue, store.PositionRemainder(position))
	element, err := snapshot.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return cluster.CodeNotFound, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading the element at position %d: %w", position, err)
	}
	// Judged on the declared path, under the pair lock: the element there must still be the
	// one the entry named, and it must be past its time at this attempt's instant.
	table, err := a.expireTable(db, op)
	if err != nil {
		return 0, err
	}
	for _, expiring := range table.Stores {
		if expiring.FieldID == op.WrittenFields()[0].FieldID && elementCarries(element, expiring.Path, atMillis) {
			if atMillis > a.attemptNow(op) {
				// The clock stepped back after the sweep chose this entry: the element reads as live.
				return cluster.CodeNotFound, nil
			}
			return 0, nil
		}
	}
	return cluster.CodeNotFound, nil
}

// fillSystemTake applies the sweep's take: the element at the named position leaves the queue
// and its expiry entry is dropped, in the record's batch. An element that is not there writes
// nothing and keeps its entry: it may be leased, and the sweep decides what a missing one means.
func (a *DocumentApplier) fillSystemTake(batch storage.KVBatch, rec AppliedRecord, engine store.FieldEngine) error {
	field := rec.Op.WrittenFields()[0]
	position, atMillis, err := takePositionArg(field.Value)
	if err != nil {
		return fmt.Errorf("position %s: reading the arg of a system take: %w", rec.Pos, err)
	}
	stamp, expires := expireItemFor(rec.Op, field.FieldID)
	if !expires {
		return nil
	}
	snapshot := a.kv.NewSnapshot()
	defer snapshot.Discard()
	ref := a.fieldRef(rec.Slot, rec.DB, rec.Op.BucketID, rec.Op.EncID, field.FieldID, engine)
	current, err := snapshot.Get(store.FieldElementKey(ref.StorePrefix(), storage.NSQueue, store.PositionRemainder(position)))
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("reading the element of the system take of position %s: %w", rec.Pos, err)
	case !elementCarries(current, stamp.Path, atMillis) || atMillis > rec.Op.ExpireNow:
		// The element there is not the one the entry named, or it is not yet past the instant the
		// record carries: it is left alone, from the record and the stored element only, so every
		// copy and replay leaves it alone too.
		return nil
	}
	var discarded store.BucketState
	write := store.FieldWrite{Snap: snapshot, State: &discarded, Batch: batch}
	taken, found, err := store.QueueTakePosition(write, ref, position)
	if err != nil {
		return fmt.Errorf("writing the system take of position %s: %w", rec.Pos, err)
	}
	if !found {
		return nil
	}
	// The element leaves the store for good, so a maintained aggregate drops it too.
	agg := aggregateBatch(snapshot, batch, ref, rec.Op)
	if err := aggAdd(agg, taken, -1); err != nil {
		return fmt.Errorf("position %s: %w", rec.Pos, err)
	}
	if err := agg.Flush(); err != nil {
		return fmt.Errorf("writing the aggregates of the system take of position %s: %w", rec.Pos, err)
	}
	return dropElementExpiry(batch, rec, stamp, position, taken)
}

// admittedExpireOf is the precondition's view of an admitted expiry table.
func admittedExpireOf(table machine.ExpireTable) *cluster.AdmittedExpire {
	admitted := &cluster.AdmittedExpire{BucketField: table.BucketField}
	for _, expiring := range table.Stores {
		admitted.Stores = append(admitted.Stores, cluster.ExpireItem{FieldID: expiring.FieldID, Path: []byte(expiring.Path)})
	}
	return admitted
}

// expireFieldRefusal words why a write puts something other than a datetime (or null) in the
// bucket's expire field, or "" when it does not. The field is declared datetime but a scalar is
// stored as sent, so without this a string there would leave the document permanent without a word.
// Null is allowed: it is the way to say "this document does not expire".
func (a *DocumentApplier) expireFieldRefusal(db uint64, op cluster.Op) string {
	if op.Class != cluster.OpInsert && op.Class != cluster.OpUpsert && op.Class != cluster.OpUpdate {
		return ""
	}
	table, err := a.expireTable(db, op)
	if err != nil || table.BucketField == 0 {
		return ""
	}
	for _, field := range op.WrittenFields() {
		if field.FieldID != table.BucketField || (op.Class == cluster.OpUpdate && field.OpCode != cluster.UpdateSet) {
			continue
		}
		if _, isDatetime := expiryDatetime(field.Value); isDatetime || (len(field.Value) == 1 && field.Value[0] == byte(bsontype.Null)) || len(field.Value) == 0 {
			continue
		}
		holds := "bytes that are not a BSON value"
		if len(field.Value) > 0 {
			holds = "a " + bsontype.Type(field.Value[0]).String()
		}
		bucket, expireField := fmt.Sprintf("bucket %d", op.BucketID), fmt.Sprintf("field %d", field.FieldID)
		if namer, ok := a.engines.(expireNames); ok {
			if _, namedBucket, namedField, found := namer.ExpireNames(db, op.BucketID, field.FieldID); found {
				bucket, expireField = "bucket "+namedBucket, "field "+namedField
			}
		}
		return fmt.Sprintf("%s of %s is the bucket's expire field and holds %s, not a datetime, so the document would never expire. Nothing was written. Send a datetime, e.g. new Date(), or null to make the document permanent", expireField, bucket, holds)
	}
	return ""
}
