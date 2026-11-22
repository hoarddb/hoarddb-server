package server

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// legacyCatalogFileName is the per-bucket layout's catalog file. This server
// never writes it; a data directory that holds it is refused at start
// (refuseLegacyCatalog).
const legacyCatalogFileName = "catalog.json"

// catalogFormatVersion is the storage layout this server writes: the catalog
// is keys in the node store. It is used only in the legacy layout's refusal —
// the old file is the upgrade trigger (spec §5.1), so the store carries no
// version key of its own.
const catalogFormatVersion = 2

// catalogEntry is a bucket's catalog record, the value at
// [0xFFFF][0][0x01][database "/" bucket]. The slot is recorded once, at
// create, and never recomputed for storage (spec §3.1.1). There is no state
// field: an entry exists or it does not (build doc SA-19).
type catalogEntry struct {
	database string
	bucket   string

	ID        int64          `bson:"id"`
	Slot      int32          `bson:"slot"`
	Type      BucketType     `bson:"type"`
	Indices   []catalogIndex `bson:"indices,omitempty"`
	CreatedAt time.Time      `bson:"created_at"`
	// NextIndexID is the id the bucket's next index would take: the highest
	// ever assigned plus one. It is written only when it differs from the
	// length of Indices, which it never does in 1.0 — no index is added or
	// dropped after create — so it costs nothing until the add-index feature,
	// and that feature is not a format change. An id is never len(Indices)
	// computed at write time.
	NextIndexID *int32 `bson:"next_index_id,omitempty"`
	// Stores are the bucket's declared store fields (nested buckets, GAP-22),
	// and NextStoreID the id the next declaration takes. Both are absent for a
	// bucket that declares none, which is every bucket created before them.
	Stores      []catalogStore `bson:"stores,omitempty"`
	NextStoreID int32          `bson:"next_store_id,omitempty"`
	// ReplicationFactor is the factor the bucket declared at create. Zero is
	// absent, and absent means inherit the node default: an entry written
	// before this field, and the entry of a bucket that declares none, are the
	// same bytes, so no upgrade rewrites the catalog. Every writer that builds
	// an entry from a literal must copy it, because a dropped factor is a legal
	// "inherit" and fails nothing.
	ReplicationFactor int32 `bson:"replication_factor,omitempty"`
	// DatabaseGeneration is the incarnation of the database the bucket was
	// created in (db_gen). Absent on an entry written before generations
	// existed, which reads as the database's legacy generation
	// (entryGeneration): 16 bytes of BSON per bucket created on this build.
	DatabaseGeneration int64 `bson:"db_gen,omitempty"`
	// Fields are the bucket's declared typed fields, in declaration order,
	// absent for a bucket that declares none — every bucket created before
	// them, so no upgrade rewrites an entry (typed-fields spec §8.1, §8.2).
	// Every writer that builds an entry from a literal must copy both:
	// a dropped declaration is an undeclared bucket, which fails nothing.
	Fields []catalogField `bson:"fields,omitempty"`
	// FieldTypes is the check mode: "warn" or "off"; absent is strict.
	FieldTypes string `bson:"field_types,omitempty"`
	// Placement is "spread" or "whole" for a hash bucket and absent for every
	// other type, which is always whole and records nothing (document
	// placement spec §3.1, §10.1). A hash entry written by this build always
	// carries it; an entry without it, or one that is spread with no Parts, is
	// from before spread collections and stops the load (isPreSpreadEntry), so
	// nothing is retrofitted. At 21 to 22 bytes of BSON it is the entry's
	// whole cost: the running record keeps the answer in one flag bit
	// (recSpread). Every writer that builds an entry from a literal must copy
	// it (TestEveryCatalogEntryWriterCopiesPlacement): a dropped value fails
	// the next start with the pre-spread refusal.
	Placement string `bson:"placement,omitempty"`
	// Parts is K, the number of parts of a spread template: 256 for every
	// template this build makes, written at create and never changed, and not
	// settable by a request (createBucketLocked fills it from the placement).
	// Absent on a whole bucket, a part and every other type: 11 bytes of BSON
	// on disk, nothing resident.
	Parts int32 `bson:"parts,omitempty"`
	// Part and PartOf are on a part's entry only: the part number (0 is
	// absent, and PartOf says which part 0 is) and the template's incarnation
	// the part belongs to. A part with a different PartOf is an orphan of a
	// dropped collection and is never adopted (spread MVP note, section 2.2).
	// 27 bytes of BSON together (part 10, part_of 17), nothing resident.
	Part   int32  `bson:"part,omitempty"`
	PartOf uint64 `bson:"part_of,omitempty"`
}

// The two placements a hash bucket can be recorded with.
const (
	placementSpread = "spread"
	placementWhole  = "whole"
)

// maxSpreadNameBytes is the longest name of a bucket recorded spread: the
// part names a later phase builds from it must stay within maxNameBytes
// (document placement spec §3.1).
const maxSpreadNameBytes = 250

// catalogField is one declared typed field or object member (spec §8.1): a
// name, its type, and whether it is required.
type catalogField struct {
	Name string
	catalogType
	Required bool

	// recorded holds the element's bytes when this binary cannot read it — a
	// type word it does not know, or a key, anywhere in its tree. Such an
	// element is held, never dropped, and written back byte for byte, as
	// catalogIndex.recorded is (spec §8.4).
	recorded []byte
}

// catalogType is one node of a declared type (spec §4.5): a word, an
// object's declared members, or an array's element type. Members is set only
// for object { … }, Elem only for array<…>; both are absent for every other
// word.
type catalogType struct {
	Type    string         `bson:"type"`
	Members []catalogField `bson:"members,omitempty"`
	Elem    *catalogType   `bson:"elem,omitempty"`
}

// catalogFieldRecord is a catalogField's encoding. catalogField embeds
// catalogType, and the BSON codec skips an embedded unexported struct, inline
// tag or not (bsoncodec.StructCodec, AllowUnexportedFields off): encoded
// directly, a field would lose its type. So the codec goes through this flat
// form, whose keys are spec §8.1's, in its order.
type catalogFieldRecord struct {
	Name     string         `bson:"name"`
	Type     string         `bson:"type"`
	Members  []catalogField `bson:"members,omitempty"`
	Elem     *catalogType   `bson:"elem,omitempty"`
	Required bool           `bson:"required,omitempty"`
}

// catalogFieldKeys and catalogTypeKeys are every key of a declared field and
// of an element type this binary reads.
var (
	catalogFieldKeys = map[string]bool{"name": true, "type": true, "members": true, "elem": true, "required": true}
	catalogTypeKeys  = map[string]bool{"type": true, "members": true, "elem": true}
)

// UnmarshalBSON reads one declared field, holding the bytes of one this
// binary cannot read whole. It never fails on such a field: the bucket must
// still open (spec §8.4).
func (cf *catalogField) UnmarshalBSON(data []byte) error {
	var decoded catalogFieldRecord
	readable := bson.Unmarshal(data, &decoded) == nil
	*cf = catalogField{Name: decoded.Name, Required: decoded.Required,
		catalogType: catalogType{Type: decoded.Type, Members: decoded.Members, Elem: decoded.Elem}}
	if !readable || !recordedTypeReadable(bson.Raw(data), catalogFieldKeys) {
		cf.recorded = append([]byte(nil), data...)
		if name, ok := bson.Raw(data).Lookup("name").StringValueOK(); ok {
			cf.Name = name
		}
	}
	return nil
}

// MarshalBSON writes a field this binary could not read back unchanged, and
// every other field from its tree.
func (cf catalogField) MarshalBSON() ([]byte, error) {
	if cf.recorded != nil {
		return cf.recorded, nil
	}
	encoded, err := bson.Marshal(catalogFieldRecord{Name: cf.Name, Type: cf.Type, Members: cf.Members, Elem: cf.Elem, Required: cf.Required})
	if err != nil {
		return nil, fmt.Errorf("encoding declared field %q: %w", cf.Name, err)
	}
	return encoded, nil
}

// recordedTypeReadable reports whether a recorded field or element type,
// and everything declared inside it, holds only keys and type words this
// binary knows.
func recordedTypeReadable(raw bson.Raw, keys map[string]bool) bool {
	elements, err := raw.Elements()
	if err != nil {
		return false
	}
	for _, element := range elements {
		if !keys[element.Key()] {
			return false
		}
	}
	// A field has a name; an element type does not. A field without one is
	// held whole, never re-encoded with an empty name.
	if keys["name"] {
		if name, ok := raw.Lookup("name").StringValueOK(); !ok || name == "" {
			return false
		}
	}
	word, ok := raw.Lookup("type").StringValueOK()
	if !ok {
		return false
	}
	if _, known := canonicalWord(word); !known {
		return false
	}
	if members, err := raw.LookupErr("members"); err == nil {
		if !recordedMembersReadable(members) {
			return false
		}
	}
	if elem, err := raw.LookupErr("elem"); err == nil {
		document, ok := elem.DocumentOK()
		if !ok || !recordedTypeReadable(document, catalogTypeKeys) {
			return false
		}
	}
	return true
}

// recordedMembersReadable reports whether an object's recorded member list
// is a list of fields this binary reads.
func recordedMembersReadable(members bson.RawValue) bool {
	list, ok := members.ArrayOK()
	if !ok {
		return false
	}
	values, err := list.Values()
	if err != nil {
		return false
	}
	for _, member := range values {
		document, ok := member.DocumentOK()
		if !ok || !recordedTypeReadable(document, catalogFieldKeys) {
			return false
		}
	}
	return true
}

// catalogStore is one declared store field. ID is assigned once and never
// reused or recomputed: every element key of the field carries it. It is a
// uint16 in value and stored int32 because BSON has no unsigned type, as Slot
// is. Parent is 0 for a store on the bucket's documents (L2) and the id of
// the L2 store whose elements declare it (L3). Engine is the catalog name of
// a store.FieldEngine.
type catalogStore struct {
	Name   string `bson:"name"`
	ID     int32  `bson:"id"`
	Engine string `bson:"engine"`
	Parent int32  `bson:"parent,omitempty"`
}

// catalogIndex is the recorded form of one declared secondary index: the
// index descriptor (store.IndexSpec) as BSON. The declaration is kept in the
// catalog because the index manager is re-attached on load, and a restart must
// not lose what says those indexes belong here.
//
// Field and Type are the form written before the descriptor existed. They are
// read — an entry with Field set and no Fields is a pre-build index — and never
// written again. Every other field is additive BSON, so a later addition (a
// descending component, say) is not a format change.
type catalogIndex struct {
	Field string `bson:"field,omitempty"`
	Type  string `bson:"type,omitempty"`

	ID     int32    `bson:"id"`
	Name   string   `bson:"name"`
	Fields []string `bson:"fields,omitempty"`
	Unique bool     `bson:"unique,omitempty"`
	// ExpireAfterSeconds is a pointer because 0 is a legal expiry.
	ExpireAfterSeconds *int64 `bson:"expire_after_seconds,omitempty"`
	// State is omitted when the index is ready, which almost every index is.
	State string `bson:"state,omitempty"`
	// Version is absent for an index declared before this build.
	Version int32 `bson:"version,omitempty"`

	// recorded holds the element's bytes when this binary cannot read it — a
	// version above its own, a key it does not know, a state it does not
	// have (spec §3.7). Such an element is held, never dropped, and every
	// encoder writes it back byte for byte (MarshalBSON), so a rollback after
	// an upgrade does not lose an index the newer binary declared (§4 F-11).
	recorded []byte
}

// catalogIndexKeys is every key of an index element this binary reads.
var catalogIndexKeys = map[string]bool{
	"field": true, "type": true, "id": true, "name": true, "fields": true, "unique": true,
	"expire_after_seconds": true, "state": true, "version": true,
}

// catalogIndexStates is every state this binary writes.
var catalogIndexStates = map[string]bool{
	string(store.IndexReady): true, string(store.IndexBuilding): true, string(store.IndexConflicted): true,
}

// UnmarshalBSON reads one index element, holding the bytes of one this binary
// cannot read. It never fails on such an element — the bucket must still open
// — and reads what it can of its id, name, fields and unique flag for the
// refusals that name it.
func (ci *catalogIndex) UnmarshalBSON(data []byte) error {
	type plainIndex catalogIndex
	var decoded plainIndex
	readable := bson.Unmarshal(data, &decoded) == nil
	*ci = catalogIndex(decoded)
	elements, err := bson.Raw(data).Elements()
	if err != nil {
		return fmt.Errorf("reading a catalog index element: %w", err)
	}
	for _, element := range elements {
		if !catalogIndexKeys[element.Key()] {
			readable = false
		}
	}
	if !readable {
		ci.readWhatIsReadable(bson.Raw(data))
	}
	if !readable || ci.Version > int32(store.IndexSpecVersion) || !catalogIndexStates[ci.State] {
		ci.recorded = append([]byte(nil), data...)
	}
	return nil
}

// readWhatIsReadable fills the fields a refusal names from an element whose
// typed decode failed.
func (ci *catalogIndex) readWhatIsReadable(raw bson.Raw) {
	if id, ok := raw.Lookup("id").Int32OK(); ok {
		ci.ID = id
	}
	if name, ok := raw.Lookup("name").StringValueOK(); ok {
		ci.Name = name
	}
	if unique, ok := raw.Lookup("unique").BooleanOK(); ok {
		ci.Unique = unique
	}
	if version, ok := raw.Lookup("version").Int32OK(); ok {
		ci.Version = version
	}
}

// MarshalBSON writes an element this binary could not read back unchanged,
// and every other element from its fields.
func (ci catalogIndex) MarshalBSON() ([]byte, error) {
	if ci.recorded != nil {
		return ci.recorded, nil
	}
	type plainIndex catalogIndex
	encoded, err := bson.Marshal(plainIndex(ci))
	if err != nil {
		return nil, fmt.Errorf("encoding catalog index %q: %w", ci.Name, err)
	}
	return encoded, nil
}

// catalogIndices converts a bucket's declared indexes into their recorded form.
func catalogIndices(b *inMemoryBucket) []catalogIndex {
	return recordIndexSpecs(b.declaredIndexes())
}

// recordIndexSpecs converts index descriptors into their recorded form.
func recordIndexSpecs(specs []store.IndexSpec) []catalogIndex {
	if len(specs) == 0 {
		return nil
	}
	out := make([]catalogIndex, 0, len(specs))
	for _, spec := range specs {
		if spec.State == store.IndexUnreadable {
			out = append(out, catalogIndex{recorded: spec.Recorded})
			continue
		}
		recorded := catalogIndex{
			ID:      int32(spec.ID),
			Name:    spec.Name,
			Fields:  spec.FieldNames(),
			Unique:  spec.Unique,
			State:   string(spec.State),
			Version: int32(spec.Version),
		}
		if spec.TTL != nil {
			expireAfter := spec.TTL.ExpireAfterSeconds
			recorded.ExpireAfterSeconds = &expireAfter
		}
		out = append(out, recorded)
	}
	return out
}

// encodeCatalogEntry is the one encoder of a catalog record. It writes the
// next index id as the higher of the recorded value and the one the current
// indexes imply: a recorded id is never recomputed downward, because an id
// freed by a dropped index must not be handed out again (BUG-0128).
func encodeCatalogEntry(entry catalogEntry) ([]byte, error) {
	next := int32(0)
	for _, recorded := range entry.Indices {
		if recorded.ID >= next {
			next = recorded.ID + 1
		}
	}
	if entry.NextIndexID != nil && *entry.NextIndexID > next {
		next = *entry.NextIndexID
	}
	entry.NextIndexID = nil
	if int(next) != len(entry.Indices) {
		entry.NextIndexID = &next
	}
	encoded, err := bson.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("encoding the catalog entry of bucket %q in database %q: %w", entry.bucket, entry.database, err)
	}
	return encoded, nil
}

// indexSpecOf reads one recorded index. position is its place in the list,
// which is the id a pre-build index's entries carry (build doc RD-4).
func indexSpecOf(position int, recorded catalogIndex) store.IndexSpec {
	if recorded.recorded != nil {
		fields := make([]store.IndexKeyPart, 0, len(recorded.Fields))
		for _, name := range recorded.Fields {
			fields = append(fields, store.IndexKeyPart{Field: name})
		}
		version := recorded.Version
		if version > 255 {
			version = 255
		}
		return store.IndexSpec{ID: uint16(recorded.ID), Name: recorded.Name, Fields: fields, Unique: recorded.Unique,
			State: store.IndexUnreadable, Version: uint8(version), Recorded: recorded.recorded}
	}
	if len(recorded.Fields) == 0 && recorded.Field != "" {
		fields := []store.IndexKeyPart{{Field: recorded.Field}}
		return store.IndexSpec{ID: uint16(position), Name: store.DefaultIndexName(fields), Fields: fields}
	}
	fields := make([]store.IndexKeyPart, 0, len(recorded.Fields))
	for _, name := range recorded.Fields {
		fields = append(fields, store.IndexKeyPart{Field: name})
	}
	spec := store.IndexSpec{
		ID:      uint16(recorded.ID),
		Name:    recorded.Name,
		Fields:  fields,
		Unique:  recorded.Unique,
		State:   store.IndexState(recorded.State),
		Version: uint8(recorded.Version),
	}
	if recorded.ExpireAfterSeconds != nil {
		spec.TTL = &store.TTLSpec{ExpireAfterSeconds: *recorded.ExpireAfterSeconds}
	}
	return spec
}

// indexOptions rebuilds the create-bucket options that declare the given
// indexes, so attaching a bucket re-attaches exactly the indexes it had. The
// descriptors travel as []store.IndexSpec, a form no client request can
// decode into: a catalogued declaration was validated when it was made, and
// running it through the client parser again would refuse what this binary
// itself recorded (a state, an id) and rebuild the rest from field names.
func indexOptions(indices []catalogIndex) map[string]interface{} {
	if len(indices) == 0 {
		return nil
	}
	specs := make([]store.IndexSpec, 0, len(indices))
	for position, recorded := range indices {
		specs = append(specs, indexSpecOf(position, recorded))
	}
	return map[string]interface{}{"indices": specs}
}

// slotMismatchError refuses a catalog entry whose recorded slot is not the one
// its name hashes to. Start is refused rather than the one bucket skipped: a
// node that served its other buckets would hide a corrupt catalog behind a
// working one (build doc SA-23; spec §3.1.1).
type slotMismatchError struct {
	database string
	bucket   string
	recorded uint16
	computed uint16
}

func (e *slotMismatchError) Error() string {
	return fmt.Sprintf("Bucket %q in database %q cannot be opened: its catalog entry records storage slot %d, "+
		"but its name hashes to slot %d. The entry was written wrong or edited by hand, and serving it would read "+
		"and write the wrong key range. Restore the bucket from a dump into a fresh data directory with "+
		"'hoarddb dump' and 'restore'.", e.bucket, e.database, e.recorded, e.computed)
}

// assertRecordedSlot is the attach assertion, and one of storage.SlotOf's two
// non-test callers; the other is the create path (slotForCreate).
func assertRecordedSlot(entry catalogEntry) error {
	computed := storage.SlotOf(storage.PlacementKey(entry.database, entry.bucket, nil))
	if entry.Slot < 0 || entry.Slot >= storage.SlotCount || uint16(entry.Slot) != computed {
		return &slotMismatchError{database: entry.database, bucket: entry.bucket, recorded: uint16(entry.Slot), computed: computed}
	}
	return nil
}

// LoadCatalog restores the database and bucket registry from the node store.
// Call it once after NewService and before serving.
//
// A bucket whose files cannot be opened is logged and left out of the
// registry with its catalog entry kept, so a later start can retry it. A
// bucket whose recorded slot disagrees with its name refuses the whole start.
func (qs *Service) LoadCatalog() error {
	return qs.core.LoadCatalog()
}

// CatalogLoaded reports whether LoadCatalog has run on this service. Start
// must come after it: the cluster's start-time sweep walks the catalog.
func (qs *Service) CatalogLoaded() bool {
	return qs.core.catalogLoaded.Load()
}

// LoadCatalog restores the database and bucket registry with one prefix scan
// of the catalog keys — after reading the pending database drops, whose
// unexpanded entries it skips (spec §5.2.2, N-04). Without that, a crash half
// way through an expansion would serve the dropped database's remaining
// buckets again, take writes to them, and then reclaim those writes.
func (s *Server) LoadCatalog() error {
	drops, err := s.readDatabaseDrops()
	if err != nil {
		return err
	}
	databases, entries, err := s.readCatalog(drops)
	if err != nil {
		return err
	}
	// One comparison per entry, in the pass that already reads every one:
	// nothing is attached from a data directory made before spread
	// collections (spread MVP note, B6).
	for _, entry := range entries {
		if isPreSpreadEntry(entry) {
			s.logger.Error("the catalog holds a bucket from before spread collections",
				"database", entry.database, "bucket", displayBucket(entry.bucket), "placement", entry.Placement)
			return fmt.Errorf("%w: set storage.data_dir (HOARDDB_DATA_DIR) to an empty directory; this node is using %q",
				errPreSpreadDataDirectory, s.config.Storage.DataDir)
		}
	}
	data := s.serverDataAccessor()
	for _, database := range databases {
		data.getOrCreateDB(database)
	}

	attached := 0
	for _, entry := range entries {
		db, held, err := data.claimBucket(entry.database, entry.bucket, entry.Type, entryGeneration(entry), false)
		if err != nil {
			return fmt.Errorf("loading bucket %q in database %q: %w", entry.bucket, entry.database, err)
		}
		served, attachErr := s.loadCatalogedBucket(held, entry)
		var mismatch *slotMismatchError
		if errors.As(attachErr, &mismatch) {
			return attachErr
		}
		if !served {
			// A bucket registered with no storage engine behind it would fail
			// every request deep in the store layer. The entry stays in the
			// catalog, so a later start retries the attach.
			db.mu.Lock()
			unlinked, unlinkErr := db.table.unlink(entry.bucket, held)
			db.mu.Unlock()
			if unlinkErr != nil {
				return fmt.Errorf("loading bucket %q in database %q: %w", entry.bucket, entry.database, unlinkErr)
			}
			if unlinked {
				s.retireRecords(db, []*bucketRec{held.rec})
			}
			continue
		}
		attached++
	}

	s.warnUnhonourableFactors(entries)
	s.catalogLoaded.Store(true)
	s.logger.Info("catalog loaded", "databases", len(databases), "buckets", attached)
	return nil
}

// loadCatalogedBucket attaches one catalogued bucket to the record LoadCatalog
// claimed for it, under the record's lock, and reports whether it is served.
// A bucket whose attach failed is logged here; its declarations are taken
// back out of the hoarddb_indexes gauge, since they will never be published.
func (s *Server) loadCatalogedBucket(held heldRecord, entry catalogEntry) (served bool, attachErr error) {
	held.rec.mu.Lock()
	defer held.rec.mu.Unlock()
	bucket := s.handleLocked(entry.database, held)
	resp, attachErr := s.attachBucketLocked(bucket, entry)
	if attachErr == nil && resp.Success {
		// Only a flagged bucket has indexes to report or rebuild.
		if held.rec.hasDecl() {
			s.reportUnreadableIndexes(bucket, entry)
			s.reportPathIndexes(bucket, entry)
			// An index that predates this build, or a rebuild a crash stopped, is
			// rebuilt before the bucket takes a write. A failure leaves it
			// building — search refuses it and the next start rebuilds it — and
			// does not keep the bucket from serving everything else.
			if rebuildErr := s.rebuildIndexesLocked(bucket, entry.database, entry.bucket); rebuildErr != nil {
				s.logger.Error("could not rebuild the bucket's indexes; search refuses them until the next start rebuilds them",
					"database", entry.database, "bucket", entry.bucket, "error", rebuildErr)
			}
		}
		return true, nil
	}
	message := ""
	if resp != nil {
		message = resp.Message
	}
	s.logger.Error("failed to attach bucket; leaving it out of the registry and its catalog entry in place",
		"database", entry.database, "bucket", entry.bucket, "type", entry.Type,
		"message", message, "error", attachErr)
	if decl := bucket.decl.Load(); decl != nil {
		decl.indexSpecs.uncount()
	}
	return false, attachErr
}

// readCatalog scans the catalog keys: the databases, from their own markers
// and from their buckets' entries, and every bucket entry, in key order. An
// entry of a dropped database at or below its drop's max_id is the
// expander's, not a bucket: it is skipped, and does not bring the database
// back either — a dropped database's marker went in its drop batch.
func (s *Server) readCatalog(drops map[string]databaseDropRecord) ([]string, []catalogEntry, error) {
	lower, upper := catalogPrefix()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()

	var (
		databases []string
		entries   []catalogEntry
	)
	seen := make(map[string]bool)
	for it.Seek(nil); it.Valid(); it.Next() {
		name := string(it.Key()[len(lower):])
		database, bucket, ok := strings.Cut(name, "/")
		if !ok {
			return nil, nil, fmt.Errorf("the catalog holds a key with no database separator, %q; the node store is damaged — restore it from a backup", name)
		}
		if bucket != "" {
			encoded, err := it.Value()
			if err != nil {
				return nil, nil, fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", bucket, database, err)
			}
			var entry catalogEntry
			if err := bson.Unmarshal(encoded, &entry); err != nil {
				return nil, nil, fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", bucket, database, err)
			}
			entry.database, entry.bucket = database, bucket
			if drop, dropped := drops[database]; dropped && drop.covers(entry) {
				continue
			}
			entries = append(entries, entry)
		}
		if !seen[database] {
			seen[database] = true
			databases = append(databases, database)
		}
	}
	if err := it.Err(); err != nil {
		return nil, nil, fmt.Errorf("walking the catalog: %w", err)
	}
	return databases, entries, nil
}

// readCatalogEntry reads one bucket's catalog entry.
func (s *Server) readCatalogEntry(database, bucket string) (catalogEntry, bool, error) {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(catalogKey(database, bucket))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return catalogEntry{}, false, nil
	}
	if err != nil {
		return catalogEntry{}, false, fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", bucket, database, err)
	}
	var entry catalogEntry
	if err := bson.Unmarshal(encoded, &entry); err != nil {
		return catalogEntry{}, false, fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", bucket, database, err)
	}
	entry.database, entry.bucket = database, bucket
	return entry, true, nil
}

// reportPathIndexes logs one Warn line per index of an attached bucket that
// names a path, recorded before such an index was refused: it is
// kept in the catalog and ignored (spec §4.9.7).
func (s *Server) reportPathIndexes(bucket *inMemoryBucket, entry catalogEntry) {
	for _, spec := range bucket.declaredIndexes() {
		if !indexesAPath(spec) {
			continue
		}
		s.logger.Warn(fmt.Sprintf("Index %s of bucket %q in database %q indexes a path (%s), which this release cannot index: "+
			"it read a top-level field of that literal name. It is kept in the catalog unchanged and ignored — search does not use it "+
			"and writes neither maintain nor enforce it — and a dump leaves it out. Queries on the path work by scanning; "+
			"to index the value, keep a top-level copy of it and index that.",
			spec.Name, entry.bucket, entry.database, strings.Join(spec.FieldNames(), ", ")),
			"database", entry.database, "bucket", entry.bucket, "index", spec.Name)
	}
}

// reportUnreadableIndexes logs one Error line per index of an attached bucket
// that this binary cannot read.
func (s *Server) reportUnreadableIndexes(bucket *inMemoryBucket, entry catalogEntry) {
	for _, spec := range bucket.declaredIndexes() {
		if spec.State != store.IndexUnreadable {
			continue
		}
		consequence := "search will not use it"
		if spec.Unique {
			consequence = "search will not use it and writes to the bucket are refused, because this binary cannot enforce it"
		}
		s.logger.Error(fmt.Sprintf("Index %s of bucket %q in database %q was written by a newer version of hoardDB "+
			"(descriptor version %d; this binary reads up to %d). It is kept unchanged, and %s. Run the newer version of hoardDB again.",
			spec.Name, entry.bucket, entry.database, spec.Version, store.IndexSpecVersion, consequence),
			"database", entry.database, "bucket", entry.bucket, "index", spec.Name, "version", spec.Version)
	}
}

// factorRegisteredHook, when set, runs at every registration of a bucket's
// factor, on the registering goroutine. A test reads its stack to assert the
// registry is filled synchronously, inside the load, because a fill that merely
// usually wins the race against Cluster.Start passes any test that waits.
// Nil in production.
var factorRegisteredHook atomic.Pointer[func()]

// registerBucketFactor tells the cluster the factor a bucket this node now
// serves declared, so placement follows it. It is called after the bucket is
// attached, on every path that attaches one, and is a no-op on a node with no
// cluster. Order is deliberate: a factor registered for a bucket whose attach
// then failed would place a bucket this node cannot serve.
func (s *Server) registerBucketFactor(database, bucket string, factor int32) {
	if hook := factorRegisteredHook.Load(); hook != nil {
		(*hook)()
	}
	if s.cluster == nil {
		return
	}
	s.cluster.recordBucketFactor(placementOf(database, bucket), factor)
}

// unregisterBucketFactor is registerBucketFactor's reverse, for a dropped
// bucket.
func (s *Server) unregisterBucketFactor(database, bucket string) {
	if s.cluster == nil {
		return
	}
	s.cluster.forgetBucketFactor(placementOf(database, bucket))
}
