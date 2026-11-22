package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// documentPrefixLen is slot, database id, bucket id and the NSDocuments byte.
const documentPrefixLen = storage.ScopeNamespaceLen

// SlotScannerConfig wires a SlotScanner.
type SlotScannerConfig struct {
	// KV is the node's one shared Badger.
	KV storage.KV
	// Topology gives the committed epoch, this node's id and the slot table.
	Topology Topology
	// Schema gives the declaration that names a document's fields.
	Schema Schema
	// Gate admits the slots of a call's mask on this node, one decision with the keyed
	// read: ownership, holds, the lease and the schema (conv §15).
	Gate *ReadGate
	// WallMillis is the clock a document's expire time is compared against; nil takes the
	// system clock. The owner's clock is the only one a scan has (expiry spec 3.2).
	WallMillis func() int64
	// CorruptKeyAdvice is added to a CORRUPT_KEY refusal: the advice for this node's kind
	// (ringDirAdvice). Empty adds none.
	CorruptKeyAdvice string
}

// SlotScanner answers SLOT_LIST and SLOT_COUNT from this node's own Badger (queries
// section 2). It holds no state between calls: nothing per bucket, per slot or per
// cursor. A call costs one iterator Seek per slot in its mask plus the keys of the
// documents it reads.
type SlotScanner struct{ cfg SlotScannerConfig }

// NewSlotScanner returns a scanner.
func NewSlotScanner(cfg SlotScannerConfig) *SlotScanner { return &SlotScanner{cfg: cfg} }

// scanFilterTerm is one equality of a filter: field id and the wanted type-and-value bytes.
type scanFilterTerm struct {
	fieldID uint16
	want    []byte
}

// scanPlan is everything a walk needs, resolved once per call.
type scanPlan struct {
	db, bucketID uint64
	mask         cluster.SlotMask
	table        machine.FieldTable
	filter       []scanFilterTerm
	// project is the scalar field ids a body keeps, ascending; nil keeps every field and
	// also reads every store state (U87).
	project []uint16
	// need is the scalar field ids the walk must read the value of: the projection and the
	// filter's fields. nil reads every field.
	need []uint16
	// expireField is the datetime field the bucket expires at, 0 when it declares none; a
	// document whose value is at or before nowMillis is absent to the scan (expiry spec 3.2).
	expireField uint16
	nowMillis   int64
}

// expired reports whether the scan must treat doc as already gone.
func (p scanPlan) expired(doc scannedDoc) bool {
	if p.expireField == 0 {
		return false
	}
	for _, field := range doc.fields {
		if field.id == p.expireField {
			return expiredAt(field.stored, p.nowMillis)
		}
	}
	return false
}

// reads reports whether the value of scalar field id is needed by the call.
func (p scanPlan) reads(id uint16) bool { return keepsField(p.need, id) }

// scannedDoc is one live document as the walk found it.
type scannedDoc struct {
	slot   uint16
	encID  []byte
	header []byte
	fields []scannedField
}

type scannedField struct {
	id     uint16
	stored []byte
}

func scanError(code cluster.Code, cond cluster.Condition, args map[string]string) *cluster.Error {
	return &cluster.Error{Code: code, Message: code.Text(cond, args)}
}

// admit fences the call on the epoch, then hands the mask to the read gate, which decides
// ownership, holds and the lease for every slot the way a keyed read does.
func (s *SlotScanner) admit(ctx context.Context, epoch, db, bucketID uint64, mask *cluster.SlotMask) error {
	committed := s.cfg.Topology.Committed()
	ids := map[string]string{"db": strconv.FormatUint(db, 10), "bucket": strconv.FormatUint(bucketID, 10), "e": strconv.FormatUint(committed, 10)}
	if epoch != committed {
		ids["e_seen"] = strconv.FormatUint(epoch, 10)
		ids["slot"] = "of the mask"
		ids["owner"] = s.cfg.Topology.Self()
		return scanError(cluster.CodeStaleEpoch, cluster.CondStaleEpoch, ids)
	}
	if _, ok := s.cfg.Topology.Database(db); !ok {
		return scanError(cluster.CodeNotFound, cluster.CondBucketNotFound, ids)
	}
	return s.cfg.Gate.AdmitMask(ctx, db, bucketID, mask)
}

// plan reads the declaration and parses the filter.
func (s *SlotScanner) plan(db, bucketID uint64, filter []byte, mask cluster.SlotMask, project []uint16) (scanPlan, error) {
	view := s.cfg.Schema.View(db, bucketID)
	if !view.Present {
		return scanPlan{}, scanError(cluster.CodeDeclUnavailable, cluster.CondDeclUnavailable, map[string]string{
			"db": strconv.FormatUint(db, 10), "bucket": strconv.FormatUint(bucketID, 10),
		})
	}
	table, _ := machine.EffectiveTable(view.Decl, view.Published)
	plan := scanPlan{db: db, bucketID: bucketID, mask: mask, table: table, project: project, need: slices.Clone(project)}
	if expireField := machine.EffectiveExpire(view.Decl, view.Published).BucketField; expireField != 0 {
		// One more scalar key per document: the expire field is read even when the call
		// projects it away, so an expired document is skipped before it is counted.
		plan.expireField, plan.nowMillis = expireField, clockOrSystem(s.cfg.WallMillis)()
		if plan.need != nil {
			plan.need = append(plan.need, expireField)
		}
	}
	if len(filter) == 0 {
		return plan, nil
	}
	elements, err := bson.Raw(filter).Elements()
	if err != nil {
		return scanPlan{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("the filter is not a BSON document: %v", err)}
	}
	for _, element := range elements {
		entry, found := entryNamed(table, element.Key())
		if !found || isStoreType(entry.Type) {
			return scanPlan{}, scanError(cluster.CodeFieldType, cluster.CondFieldNotDeclared, map[string]string{
				"field": element.Key(), "db": strconv.FormatUint(db, 10), "bucket": strconv.FormatUint(bucketID, 10),
				"declared": declaredScalars(table),
			})
		}
		wanted := element.Value()
		plan.filter = append(plan.filter, scanFilterTerm{fieldID: entry.ID, want: append([]byte{byte(wanted.Type)}, wanted.Value...)})
		if plan.need != nil {
			plan.need = append(plan.need, entry.ID)
		}
	}
	return plan, nil
}

func entryNamed(table machine.FieldTable, name string) (machine.FieldEntry, bool) {
	for _, entry := range table.Entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return machine.FieldEntry{}, false
}

func entryWithID(table machine.FieldTable, id uint16) (machine.FieldEntry, bool) {
	for _, entry := range table.Entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return machine.FieldEntry{}, false
}

// isStoreType reports a declared store field (fifoStore, hashStore, ...).
func isStoreType(declared string) bool { return strings.HasSuffix(declared, "Store") }

func declaredScalars(table machine.FieldTable) string {
	var names []string
	for _, entry := range table.Entries {
		if !isStoreType(entry.Type) {
			names = append(names, entry.Name)
		}
	}
	return strings.Join(names, ", ")
}

// matches reports whether doc satisfies every equality of the filter.
func (p scanPlan) matches(doc scannedDoc) bool {
	for _, term := range p.filter {
		var stored []byte
		if term.fieldID == 1 {
			stored = doc.header
		} else {
			for _, field := range doc.fields {
				if field.id == term.fieldID {
					stored = field.stored
				}
			}
		}
		if !storedEqual(stored, term.want) {
			return false
		}
	}
	return true
}

// BSON type bytes of the numeric kinds a filter compares by value.
const (
	typeDouble byte = 0x01
	typeInt32  byte = 0x10
	typeInt64  byte = 0x12
)

// number is a decoded int32, int64 or double.
type number struct {
	isInt bool
	i     int64
	f     float64
}

// decodeNumber reads stored (bsonType:1 followed by the value bytes) as a number. It
// reports false for any other type or a wrong length.
func decodeNumber(stored []byte) (number, bool) {
	if len(stored) == 0 {
		return number{}, false
	}
	body := stored[1:]
	switch {
	case stored[0] == typeInt32 && len(body) == 4:
		return number{isInt: true, i: int64(int32(binary.LittleEndian.Uint32(body)))}, true
	case stored[0] == typeInt64 && len(body) == 8:
		return number{isInt: true, i: int64(binary.LittleEndian.Uint64(body))}, true
	case stored[0] == typeDouble && len(body) == 8:
		return number{f: math.Float64frombits(binary.LittleEndian.Uint64(body))}, true
	}
	return number{}, false
}

// equalsNumber is exact equality: an integer and a double are equal only when the double
// is integral and inside the int64 range; NaN equals nothing and -0 equals 0.
func (n number) equalsNumber(other number) bool {
	switch {
	case n.isInt && other.isInt:
		return n.i == other.i
	case !n.isInt && !other.isInt:
		return n.f == other.f
	case n.isInt:
		return other.equalsNumber(n)
	}
	const int64Limit = 1 << 63 // float64(1<<63) is exact; MaxInt64 itself is not a float64
	if n.f != math.Trunc(n.f) || n.f < -int64Limit || n.f >= int64Limit {
		return false
	}
	return int64(n.f) == other.i
}

// storedEqual compares a stored scalar with a filter value (both bsonType:1 followed by
// the value bytes). Numbers compare by value across int32, int64 and double; every other
// type compares by type and bytes, so a number never equals a non-number.
func storedEqual(stored, wanted []byte) bool {
	storedNumber, storedIsNumber := decodeNumber(stored)
	wantedNumber, wantedIsNumber := decodeNumber(wanted)
	if storedIsNumber || wantedIsNumber {
		return storedIsNumber && wantedIsNumber && storedNumber.equalsNumber(wantedNumber)
	}
	return bytes.Equal(stored, wanted)
}

// walk visits the live documents of the mask in (slot, encID) order, strictly after
// (resumeSlot, resumeID), until visit returns false. It seeks once per slot.
func (s *SlotScanner) walk(ctx context.Context, snapshot storage.KVSnapshot, plan scanPlan, resumeSlot uint16, resumeID []byte, visit func(scannedDoc) (bool, error)) error {
	iter := snapshot.NewIterator(storage.IterOptions{})
	defer iter.Close()
	for slot := resumeSlot; slot < storage.SlotCount; slot++ {
		if !plan.mask.Has(slot) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("scanning slot %d: %w", slot, err)
		}
		after := []byte(nil)
		if slot == resumeSlot {
			after = resumeID
		}
		more, err := walkSlot(iter, plan, slot, after, visit, s.cfg.CorruptKeyAdvice)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
	return nil
}

// walkSlot reads one slot with a single Seek. It returns false when visit asked to stop.
// advice is added to a CORRUPT_KEY refusal (corruptKey).
func walkSlot(iter storage.KVIterator, plan scanPlan, slot uint16, after []byte, visit func(scannedDoc) (bool, error), advice string) (bool, error) {
	prefix := storage.Key(slot, plan.db, plan.bucketID, storage.NSDocuments)
	iter.Seek(append(bytes.Clone(prefix), after...))
	var current scannedDoc
	finish := func() (bool, error) {
		if current.encID == nil || current.header == nil || (len(current.header) == 1 && current.header[0] == 0x00) || plan.expired(current) {
			return true, nil
		}
		return visit(current)
	}
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		rest := key[documentPrefixLen:]
		width, err := storage.EncodedComponentLen(rest)
		if err != nil {
			return false, corruptKey(plan.bucketID, rest, err, advice)
		}
		encID, tail := rest[:width], rest[width:]
		if len(after) > 0 && bytes.Compare(encID, after) <= 0 {
			continue
		}
		if !bytes.Equal(encID, current.encID) {
			if more, err := finish(); err != nil || !more {
				return false, err
			}
			current = scannedDoc{slot: slot, encID: bytes.Clone(encID)}
		}
		if len(tail) != 0 && len(tail) != 2 {
			return false, corruptKey(plan.bucketID, rest, fmt.Errorf("%d bytes follow the id", len(tail)), advice)
		}
		// A scalar the call neither returns nor filters on is stepped over unread: its
		// value is not fetched and not copied.
		if len(tail) == 2 && !plan.reads(binary.BigEndian.Uint16(tail)) {
			continue
		}
		stored, err := iter.Value()
		if err != nil {
			return false, fmt.Errorf("reading a value in slot %d: %w", slot, err)
		}
		if len(tail) == 0 {
			current.header = stored
		} else {
			current.fields = append(current.fields, scannedField{id: binary.BigEndian.Uint16(tail), stored: stored})
		}
	}
	if err := iter.Err(); err != nil {
		return false, fmt.Errorf("walking slot %d: %w", slot, err)
	}
	return finish()
}

// corruptKey is CORRUPT_KEY for a key whose id component does not decode. The text names the
// damage; advice is what this node's operator should do (ringDirAdvice for a member or a single
// node), added here because the shared text cannot know the node's kind (store-vouched-cut review
// R-1). An empty advice adds nothing.
func corruptKey(bucketID uint64, rest []byte, cause error, advice string) error {
	tag := "00"
	if len(rest) > 0 {
		tag = fmt.Sprintf("%02x", rest[0])
	}
	message := cluster.CodeCorruptKey.Text(cluster.CondCorruptKey, map[string]string{
		"bucket_id": strconv.FormatUint(bucketID, 10), "tag": tag, "offset": strconv.Itoa(documentPrefixLen),
	}) + " (" + cause.Error() + ")"
	if advice != "" {
		message += ". " + advice
	}
	return &cluster.Error{Code: cluster.CodeCorruptKey, Message: message}
}

// List answers a SLOT_LIST: up to req.Want live documents of the mask strictly after the
// resume position, in (slot, encID) order. Its byte share is a stop and never a refusal,
// except that a document that alone exceeds the share is still returned alone.
func (s *SlotScanner) List(ctx context.Context, req cluster.SlotListRequest) (cluster.SlotListReply, error) {
	if req.Want < 1 {
		return cluster.SlotListReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("a sub-scan wants %d documents, and it must want at least 1", req.Want)}
	}
	if err := s.admit(ctx, req.Epoch, req.DBID, req.BucketID, &req.Mask); err != nil {
		return cluster.SlotListReply{}, err
	}
	return s.page(ctx, req)
}

// ListAdmitted is List for a caller that admitted the same mask with Admit and pages
// through it: the page repeats only the epoch fence, which is one comparison, instead of
// admitting every slot of the mask again.
func (s *SlotScanner) ListAdmitted(ctx context.Context, req cluster.SlotListRequest) (cluster.SlotListReply, error) {
	if req.Want < 1 {
		return cluster.SlotListReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("a sub-scan wants %d documents, and it must want at least 1", req.Want)}
	}
	if committed := s.cfg.Topology.Committed(); req.Epoch != committed {
		return cluster.SlotListReply{}, scanError(cluster.CodeStaleEpoch, cluster.CondStaleEpoch, map[string]string{
			"db": strconv.FormatUint(req.DBID, 10), "bucket": strconv.FormatUint(req.BucketID, 10), "e": strconv.FormatUint(committed, 10),
			"e_seen": strconv.FormatUint(req.Epoch, 10), "slot": "of the mask", "owner": s.cfg.Topology.Self(),
		})
	}
	return s.page(ctx, req)
}

// Admit is the admission List runs before its first page, for a caller that pages with
// ListAdmitted.
func (s *SlotScanner) Admit(ctx context.Context, epoch, db, bucketID uint64, mask *cluster.SlotMask) error {
	return s.admit(ctx, epoch, db, bucketID, mask)
}

// page reads one page of an admitted call.
func (s *SlotScanner) page(ctx context.Context, req cluster.SlotListRequest) (cluster.SlotListReply, error) {
	plan, err := s.plan(req.DBID, req.BucketID, req.Filter, req.Mask, req.Fields)
	if err != nil {
		return cluster.SlotListReply{}, err
	}
	snapshot := s.cfg.KV.NewSnapshot()
	defer snapshot.Discard()
	var reply cluster.SlotListReply
	var size int64
	err = s.walk(ctx, snapshot, plan, req.ResumeSlot, req.ResumeID, func(doc scannedDoc) (bool, error) {
		if !plan.matches(doc) {
			return true, nil
		}
		body, err := plan.body(snapshot, doc)
		if err != nil {
			return false, err
		}
		entry := cluster.ListEntry{Slot: doc.slot, EncID: doc.encID, Body: body}
		width := int64(len(cluster.AppendEntry(nil, entry)))
		if len(reply.Entries) > 0 && size+width > req.MaxBytes {
			reply.More = true
			return false, nil
		}
		reply.Entries = append(reply.Entries, entry)
		size += width
		if len(reply.Entries) == req.Want {
			reply.More = true
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return cluster.SlotListReply{}, err
	}
	return reply, nil
}

// Count answers a SLOT_COUNT: the live documents of the mask that satisfy the filter.
func (s *SlotScanner) Count(ctx context.Context, req cluster.SlotCountRequest) (int64, error) {
	if err := s.admit(ctx, req.Epoch, req.DBID, req.BucketID, &req.Mask); err != nil {
		return 0, err
	}
	plan, err := s.plan(req.DBID, req.BucketID, req.Filter, req.Mask, nil)
	if err != nil {
		return 0, err
	}
	snapshot := s.cfg.KV.NewSnapshot()
	defer snapshot.Discard()
	var matched int64
	err = s.walk(ctx, snapshot, plan, 0, nil, func(doc scannedDoc) (bool, error) {
		if plan.matches(doc) {
			matched++
		}
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return matched, nil
}

// body is the document as BSON: _id_ first, the declared scalars in field-id order, then
// each store field that has held elements as the string "<type> (<n> element|elements)".
// A key of a retired field id is skipped; the sweep removes it later. A projected body
// holds _id_ and the projected scalars only and reads no store state.
func (p scanPlan) body(snapshot storage.KVSnapshot, doc scannedDoc) ([]byte, error) {
	var elements []byte
	elements = appendElement(elements, "_id_", doc.header)
	fields := slices.Clone(doc.fields)
	slices.SortFunc(fields, func(a, b scannedField) int { return int(a.id) - int(b.id) })
	for _, field := range fields {
		entry, live := entryWithID(p.table, field.id)
		if !live || len(field.stored) == 0 || !keepsField(p.project, field.id) {
			continue
		}
		elements = appendElement(elements, entry.Name, field.stored)
	}
	if p.project == nil {
		var err error
		if elements, err = p.appendStoreSummaries(snapshot, doc, elements); err != nil {
			return nil, err
		}
	}
	out := binary.LittleEndian.AppendUint32(make([]byte, 0, len(elements)+5), uint32(len(elements)+5))
	return append(append(out, elements...), 0x00), nil
}

// appendStoreSummaries adds each store field that has held elements to a body, in field-id
// order, with a state read per store field.
func (p scanPlan) appendStoreSummaries(snapshot storage.KVSnapshot, doc scannedDoc, elements []byte) ([]byte, error) {
	stores := slices.Clone(p.table.Entries)
	slices.SortFunc(stores, func(a, b machine.FieldEntry) int { return int(a.ID) - int(b.ID) })
	for _, entry := range stores {
		if !isStoreType(entry.Type) {
			continue
		}
		stateKey := store.FieldStateKey(store.StorePrefix(doc.slot, p.db, p.bucketID, doc.encID, entry.ID))
		encoded, err := snapshot.Get(stateKey)
		if errors.Is(err, storage.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading the state of store field %q: %w", entry.Name, err)
		}
		state, err := store.DecodeFieldInstanceState(stateKey, encoded)
		if err != nil {
			return nil, fmt.Errorf("decoding the state of store field %q: %w", entry.Name, err)
		}
		noun := "elements"
		if state.Count == 1 {
			noun = "element"
		}
		marker := fmt.Sprintf("%s (%d %s)", entry.Type, state.Count, noun)
		text := binary.LittleEndian.AppendUint32(nil, uint32(len(marker)+1))
		text = append(append(text, marker...), 0x00)
		elements = appendElement(elements, entry.Name, append([]byte{byte(bson.TypeString)}, text...))
	}
	return elements, nil
}

// appendElement adds one BSON element from a stored value (bsonType:1 followed by the value bytes).
func appendElement(dst []byte, name string, stored []byte) []byte {
	dst = append(dst, stored[0])
	dst = append(append(dst, name...), 0x00)
	return append(dst, stored[1:]...)
}
