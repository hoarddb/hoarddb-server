package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Store-field declarations (nested buckets, GAP-22): what a bucket declares,
// how the declaration is recorded in its catalog entry, and how its ids are
// assigned. An id is assigned once, from next_store_id, and never reused:
// every element key of a field carries its id, so a reused id would hand one
// field's elements to another.

// StoreDeclaration is one declared store field: its name, its engine word
// (fifoStore, or the catalog name fifo) and the store fields declared on its
// elements (L3). It is exported because the CLI parses its declarations into
// it and validates them with the server's own validator.
type StoreDeclaration struct {
	Name   string
	Engine string
	Fields []StoreDeclaration
}

// storeFieldSet is a bucket's declared store fields and the next id, as its
// catalog entry records them. A set is never modified: a declaration builds
// a new one, so a reader that loaded one reads it with no lock.
type storeFieldSet struct {
	stores []catalogStore
	next   int32
}

// noStoreFields is the set of a bucket that declares none.
var noStoreFields = &storeFieldSet{}

// storeFields is the bucket's declared store fields.
func (b *inMemoryBucket) storeFields() *storeFieldSet {
	if decl := b.decl.Load(); decl != nil {
		if set := decl.fieldStores.Load(); set != nil {
			return set
		}
	}
	return noStoreFields
}

// catalogStores is the bucket's declaration in its recorded form.
func catalogStores(b *inMemoryBucket) []catalogStore {
	return b.storeFields().stores
}

// maxStoreID is the largest id a store field can take: ids are two bytes in
// every element key.
const maxStoreID = math.MaxUint16

// withDeclared returns a new set holding set's stores and added, each given
// the next id in declaration order, an L3 store after the L2 store it is
// declared on and with that store's id as its Parent. It never assigns
// len(stores)+1 or a position: after a removal those would reuse an id. A
// declaration of a name already declared on the same level is an identity
// and takes no id, so a replayed or redelivered declare changes nothing (the
// validator has refused one with another engine).
func (set *storeFieldSet) withDeclared(added []StoreDeclaration) (*storeFieldSet, error) {
	grown := &storeFieldSet{stores: append([]catalogStore(nil), set.stores...), next: max(set.next, 1)}
	var assign func(declarations []StoreDeclaration, parent int32) error
	assign = func(declarations []StoreDeclaration, parent int32) error {
		for _, declared := range declarations {
			if existing, found := grown.find(declared.Name, parent); found {
				if err := assign(declared.Fields, existing.ID); err != nil {
					return err
				}
				continue
			}
			if grown.next > maxStoreID {
				return fmt.Errorf("%w: store field %q cannot be declared: this bucket has assigned all %d store field ids, and ids are never reused",
					ErrStoreDeclarationInvalid, declared.Name, maxStoreID)
			}
			engine, err := store.ParseFieldEngine(engineName(declared.Engine))
			if err != nil {
				return fmt.Errorf("%w: store field %q: %w", ErrStoreDeclarationInvalid, declared.Name, err)
			}
			id := grown.next
			grown.stores = append(grown.stores, catalogStore{Name: declared.Name, ID: id, Engine: engine.String(), Parent: parent})
			grown.next++
			if err := assign(declared.Fields, id); err != nil {
				return err
			}
		}
		return nil
	}
	if err := assign(added, 0); err != nil {
		return nil, err
	}
	return grown, nil
}

// find returns the store named name declared on the level parent.
func (set *storeFieldSet) find(name string, parent int32) (catalogStore, bool) {
	for _, declared := range set.stores {
		if declared.Name == name && declared.Parent == parent {
			return declared, true
		}
	}
	return catalogStore{}, false
}

// byID returns the declared store with id.
func (set *storeFieldSet) byID(id int32) catalogStore {
	for _, declared := range set.stores {
		if declared.ID == id {
			return declared
		}
	}
	return catalogStore{}
}

// layoutOf is the layout of the stores declared on the elements of the
// store with id parent, nil when there are none.
func (set *storeFieldSet) layoutOf(parent int32) store.FieldLayout {
	var layout store.FieldLayout
	for _, declared := range set.stores {
		if declared.Parent != parent {
			continue
		}
		engine, err := store.ParseFieldEngine(declared.Engine)
		if err != nil {
			continue
		}
		if layout == nil {
			layout = make(store.FieldLayout)
		}
		layout[uint16(declared.ID)] = engine
	}
	return layout
}

// engineName reads an engine word as the catalog records it: fifoStore,
// FIFOSTORE and fifo all read fifo.
func engineName(word string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(word)), "store")
}

// adoptRecordedStores sets a catalogued bucket's declaration from its entry:
// the recorded ids and next_store_id, never recomputed, because the bucket's
// keys already carry them. The recorded declaration passes the same validator
// a create does, so a reopen refuses what a create would (Q12).
func (s *Server) adoptRecordedStores(bucket *inMemoryBucket, entry catalogEntry) *CreateBucketResponse {
	recorded := &storeFieldSet{stores: entry.Stores, next: entry.NextStoreID}
	return s.adoptStoreFieldSet(bucket, entry.database, entry.bucket, recorded)
}

// adoptStoreFieldSet validates a recorded declaration and sets it on bucket.
func (s *Server) adoptStoreFieldSet(bucket *inMemoryBucket, database, name string, recorded *storeFieldSet) *CreateBucketResponse {
	if len(recorded.stores) == 0 {
		s.setStoreFields(bucket, nil)
		return nil
	}
	err := ValidateStoreDeclarations(StoreValidation{
		Database: database, Bucket: name, BucketType: bucket.bucketType,
		Added: recorded.declarations(0), Recorded: true,
	})
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: err.Error()}
	}
	for _, declared := range recorded.stores {
		if slices.Contains(reservedSinceRecorded, declared.Name) {
			s.logger.Warn(fmt.Sprintf("Store field %q of bucket %q in database %q has a name that is now reserved (it is the typed-field mode option, "+
				"so alter bucket %s { %s: … } would be read as the option). The store field keeps working; a new one cannot take the name. "+
				"To give its elements another name, copy them into a store field declared under another name and stop using this one.",
				declared.Name, name, database, name, declared.Name),
				"database", database, "bucket", name, "store_field", declared.Name)
		}
	}
	s.setStoreFields(bucket, recorded)
	return nil
}

// declarations rebuilds the nested declaration of the stores whose parent is
// parent.
func (set *storeFieldSet) declarations(parent int32) []StoreDeclaration {
	var declared []StoreDeclaration
	for _, recorded := range set.stores {
		if recorded.Parent == parent {
			declared = append(declared, StoreDeclaration{Name: recorded.Name, Engine: recorded.Engine, Fields: set.declarations(recorded.ID)})
		}
	}
	return declared
}

// prepareStoreFields reads a create request's Options["stores"], validates
// it, and sets the bucket's declaration: fresh ids from 1 for a new
// declaration, the recorded ids for a recorded one (a migrated bucket, whose
// keys already carry them). A non-nil response is the refusal.
func (s *Server) prepareStoreFields(bucket *inMemoryBucket, req CreateBucketRequest) *CreateBucketResponse {
	declared, recorded, err := parseStoreDeclarations(req.Options)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf("bucket %q: %v", req.Name, err)}
	}
	if recorded != nil {
		return s.adoptStoreFieldSet(bucket, req.Database, req.Name, recorded)
	}
	err = ValidateStoreDeclarations(StoreValidation{
		Database: req.Database, Bucket: req.Name, BucketType: req.Type,
		Added: declared,
	})
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: err.Error()}
	}
	if len(declared) == 0 {
		s.setStoreFields(bucket, nil)
		return nil
	}
	set, err := noStoreFields.withDeclared(declared)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: err.Error()}
	}
	s.setStoreFields(bucket, set)
	return nil
}

// storeOptions is the create options that carry a recorded declaration: the
// stores with their ids and parents, and next_store_id. A bucket built from
// them adopts the ids rather than assigning new ones.
func storeOptions(stores []catalogStore, next int32) map[string]interface{} {
	if len(stores) == 0 {
		return nil
	}
	list := make([]interface{}, 0, len(stores))
	for _, recorded := range stores {
		list = append(list, map[string]interface{}{"name": recorded.Name, "engine": recorded.Engine, "id": recorded.ID, "parent": recorded.Parent})
	}
	return map[string]interface{}{"stores": list, "next_store_id": next}
}

// parseStoreDeclarations reads Options["stores"]: a list of {name, engine,
// fields} for a new declaration, or, with Options["next_store_id"], the
// recorded form storeOptions writes. A malformed entry is an error naming it,
// never a silent skip: a dropped declaration is a store the user believes
// exists.
func parseStoreDeclarations(options map[string]interface{}) ([]StoreDeclaration, *storeFieldSet, error) {
	raw, ok := options["stores"]
	if !ok {
		return nil, nil, nil
	}
	entries, err := optionList(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: stores must be a list of {name, engine} declarations: %w", ErrStoreDeclarationInvalid, err)
	}
	next, recordedForm := options["next_store_id"]
	if !recordedForm {
		declared, err := parseDeclarationList(entries)
		return declared, nil, err
	}
	recorded := &storeFieldSet{}
	if recorded.next, err = optionInt32(next); err != nil {
		return nil, nil, fmt.Errorf("%w: next_store_id: %w", ErrStoreDeclarationInvalid, err)
	}
	for position, entry := range entries {
		fields, err := optionMap(entry)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: stores entry %d: %w", ErrStoreDeclarationInvalid, position, err)
		}
		name, _ := fields["name"].(string)
		engine, _ := fields["engine"].(string)
		id, idErr := optionInt32(fields["id"])
		parent, parentErr := optionInt32(fields["parent"])
		if name == "" || engine == "" || idErr != nil || id <= 0 || parentErr != nil {
			return nil, nil, fmt.Errorf("%w: recorded stores entry %d (%v) needs a name, an engine and a positive id", ErrStoreDeclarationInvalid, position, entry)
		}
		recorded.stores = append(recorded.stores, catalogStore{Name: name, ID: id, Engine: engine, Parent: parent})
	}
	return nil, recorded, nil
}

// parseDeclarationList reads a list of new {name, engine, fields}
// declarations.
func parseDeclarationList(entries []interface{}) ([]StoreDeclaration, error) {
	declared := make([]StoreDeclaration, 0, len(entries))
	for position, entry := range entries {
		fields, err := optionMap(entry)
		if err != nil {
			return nil, fmt.Errorf("%w: stores entry %d: %w", ErrStoreDeclarationInvalid, position, err)
		}
		name, _ := fields["name"].(string)
		engine, _ := fields["engine"].(string)
		for key := range fields {
			switch key {
			case "name", "engine", "fields":
			case "replication_factor":
				return nil, fmt.Errorf("%w: store field %q declares a replication_factor, and a store field has none of its own: "+
					"it lives in its bucket's key range and is replicated with the bucket. Set replication_factor on the bucket instead",
					ErrStoreDeclarationInvalid, name)
			default:
				return nil, fmt.Errorf("%w: store field %q declares %q, which is not a store-field option (a declaration is {name, engine, fields})",
					ErrStoreDeclarationInvalid, name, key)
			}
		}
		if engine == "" {
			return nil, fmt.Errorf("%w: stores entry %d (%v) has no engine: declare it as {name: %q, engine: \"fifoStore\"}", ErrStoreDeclarationInvalid, position, entry, name)
		}
		declaration := StoreDeclaration{Name: name, Engine: engine}
		if nested, ok := fields["fields"]; ok && nested != nil {
			list, err := optionList(nested)
			if err != nil {
				return nil, fmt.Errorf("%w: the fields of store %q: %w", ErrStoreDeclarationInvalid, name, err)
			}
			if declaration.Fields, err = parseDeclarationList(list); err != nil {
				return nil, err
			}
		}
		declared = append(declared, declaration)
	}
	return declared, nil
}

// optionList and optionMap accept every shape a request's options take: Go
// values from an in-process caller, and bson.A, bson.M or bson.D off the
// wire.
func optionList(raw interface{}) ([]interface{}, error) {
	switch list := raw.(type) {
	case []interface{}:
		return list, nil
	case bson.A:
		return []interface{}(list), nil
	case []map[string]interface{}:
		converted := make([]interface{}, 0, len(list))
		for _, entry := range list {
			converted = append(converted, entry)
		}
		return converted, nil
	}
	return nil, fmt.Errorf("got %T, want a list", raw)
}

func optionMap(raw interface{}) (map[string]interface{}, error) {
	switch fields := raw.(type) {
	case map[string]interface{}:
		return fields, nil
	case bson.M:
		return map[string]interface{}(fields), nil
	case bson.D:
		converted := make(map[string]interface{}, len(fields))
		for _, element := range fields {
			converted[element.Key] = element.Value
		}
		return converted, nil
	}
	return nil, fmt.Errorf("got %T, want a document", raw)
}

func optionInt64(raw interface{}) (int64, error) {
	switch number := raw.(type) {
	case int64:
		return number, nil
	case int32:
		return int64(number), nil
	case int:
		return int64(number), nil
	case float64:
		if number != math.Trunc(number) || number < math.MinInt64 || number >= math.MaxInt64 {
			return 0, fmt.Errorf("%v is not a whole number in range", number)
		}
		return int64(number), nil
	}
	return 0, fmt.Errorf("got %T, want a number", raw)
}

func optionInt32(raw interface{}) (int32, error) {
	var wide int64
	switch number := raw.(type) {
	case nil:
		return 0, nil
	case int32:
		return number, nil
	case int:
		wide = int64(number)
	case int64:
		wide = number
	case float64:
		if number != math.Trunc(number) {
			return 0, fmt.Errorf("%v is not a whole number", number)
		}
		wide = int64(number)
	default:
		return 0, fmt.Errorf("got %T, want a number", raw)
	}
	if wide < math.MinInt32 || wide > math.MaxInt32 {
		return 0, fmt.Errorf("%d is out of range", wide)
	}
	return int32(wide), nil
}

// reservedStoreFieldNames may not name a store field: the directory names a
// bucket once held (kept defensively, spec §4.5 rule 2 and §5.4), the
// declaration body's option keywords — so alter bucket X { type: fifo } is
// never read as a field named type (S-9) — and the document id field.
var reservedStoreFieldNames = []string{
	"badger", "segments", "blobs", "indexes", "key.wrapped", "keys.wrapped",
	"type", "indices", "acknowledge", "replication_factor", "field_types",
	"_id_",
}

// reservedSinceRecorded are the reserved names a catalog may already hold a
// store field under, because they were reserved after it could be declared:
// a recorded one is kept and warned about, never refused, since refusing it
// would leave its bucket out of the registry. field_types was reserved by
// typed fields (build doc SA-13).
var reservedSinceRecorded = []string{"field_types"}

// maxDeclarationDepth is spec §7's limit: a bucket, a store on its documents
// (L2), a store on those elements (L3).
const maxDeclarationDepth = 3

// StoreValidation is one declaration for ValidateStoreDeclarations: the
// bucket it is made on, what that bucket already declares (nil at create and
// from the CLI), and what is added.
type StoreValidation struct {
	Database   string
	Bucket     string
	BucketType BucketType
	Existing   []catalogStore
	Added      []StoreDeclaration
	// Recorded says Added is a declaration a catalog holds, not a new one: a
	// name in reservedSinceRecorded is kept.
	Recorded bool
}

// ValidateStoreDeclarations is the one owner of the store-field rules (spec
// §4.5, fable F-19): the create path, the attach path, the declare DDL and
// the CLI's parser all call it, so no entry point can accept what another
// refuses. Every refusal wraps ErrStoreDeclarationInvalid and says how to fix
// it.
func ValidateStoreDeclarations(v StoreValidation) error {
	if len(v.Added) == 0 {
		return nil
	}
	if v.BucketType != BucketTypeHash && v.BucketType != BucketTypeBTree {
		return fmt.Errorf("%w: bucket %q is a %s bucket, and only hash and btree buckets can declare store fields: "+
			"a store field belongs to one document, found by the key you hold, and a %s bucket's entries are not documents with keys of their own "+
			"(a queue entry is addressed by its position, a heap entry is destroyed by the pop that reads it, and a blob has no document body). "+
			"Declare the store on a hash bucket that references the %s entry by key: create bucket %sRefs { type: hash, %s %sStore }",
			ErrStoreDeclarationInvalid, v.Bucket, v.BucketType, v.BucketType, v.BucketType, v.Bucket, v.Added[0].Name, engineName(v.Added[0].Engine))
	}
	if err := v.checkDepth(v.Added, 2); err != nil {
		return err
	}
	return v.checkLevel(v.Added, 0)
}

// checkDepth refuses the first declaration deeper than spec §7's limit, with
// spec §10.3's message. L3 is built (S-13: the owner's GAP-22 row asks for
// depth 3 in 1.0), so the limit this build serves is the spec's.
func (v StoreValidation) checkDepth(declared []StoreDeclaration, depth int) error {
	for _, declaration := range declared {
		if depth > maxDeclarationDepth {
			return fmt.Errorf("%w: %s", ErrStoreDeclarationInvalid, depthMessage(declaration, v.Database, depth))
		}
		if err := v.checkDepth(declaration.Fields, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// depthMessage is spec §10.3's depth refusal with its values filled in.
func depthMessage(declaration StoreDeclaration, database string, depth int) string {
	return fmt.Sprintf("Field %q is declared %d levels deep; hoardDB nests declarations at most %d\n"+
		"(a bucket, a store on its documents, a store on those elements).\n"+
		"Give %q its own top-level bucket instead:\n"+
		"  hoarddb bucket create %s %s --type=%s\n"+
		"and reference it from the element by its key. A fourth level is usually a sign that\n"+
		"the deepest level grows on its own, which is what a top-level bucket is for.",
		declaration.Name, depth, maxDeclarationDepth, declaration.Name, database, declaration.Name, engineName(declaration.Engine))
}

// checkLevel checks the declarations made on one level: the bucket's
// documents (parent 0), or the elements of the store whose id is parent.
func (v StoreValidation) checkLevel(declared []StoreDeclaration, parent int32) error {
	seen := make(map[string]bool, len(declared))
	for _, declaration := range declared {
		if err := v.checkName(declaration.Name); err != nil {
			return err
		}
		if seen[declaration.Name] {
			return fmt.Errorf("%w: store field %q is declared twice in one statement on bucket %q; give each store field its own name",
				ErrStoreDeclarationInvalid, declaration.Name, v.Bucket)
		}
		seen[declaration.Name] = true
		engine, err := store.ParseFieldEngine(engineName(declaration.Engine))
		if err != nil {
			return fmt.Errorf("%w: %q is not a store engine, so store field %q cannot be declared with it: "+
				"declare one of fifoStore, lifoStore, hashStore, btreeStore, heapStore, blobStore",
				ErrStoreDeclarationInvalid, declaration.Engine, declaration.Name)
		}
		childParent := int32(-1)
		for _, existing := range v.Existing {
			if existing.Parent != parent || existing.Name != declaration.Name {
				continue
			}
			if existing.Engine != engine.String() {
				return fmt.Errorf("%w: store field %q on bucket %q is already declared as %sStore and cannot be re-declared as %sStore: "+
					"removing or replacing a store field is not supported in 1.0, because it would destroy every document's %q elements with no preview and no undo. "+
					"Declare the new store under another name",
					ErrStoreDeclarationInvalid, declaration.Name, v.Bucket, existing.Engine, engine, declaration.Name)
			}
			childParent = existing.ID
		}
		if engine == store.FieldBlob && len(declaration.Fields) > 0 {
			return fmt.Errorf("%w: store field %q is declared blobStore, so its elements are blob content and not documents, and cannot hold store fields (%s): "+
				"declare the store on a hashStore field whose elements reference the blob by key instead",
				ErrStoreDeclarationInvalid, declaration.Name, declaration.Fields[0].Name)
		}
		if err := v.checkLevel(declaration.Fields, childParent); err != nil {
			return err
		}
	}
	return nil
}

// checkName is spec §4.5 rules 1 and 2.
func (v StoreValidation) checkName(name string) error {
	if name == "" || strings.ContainsAny(name, ".\x00") || strings.HasPrefix(name, "$") {
		return fmt.Errorf("%w: %q cannot name a store field on bucket %q: a field name is non-empty, contains no \".\" or NUL, and does not start with \"$\". "+
			"Name it like a document field, for example Comments",
			ErrStoreDeclarationInvalid, name, v.Bucket)
	}
	if v.Recorded && slices.Contains(reservedSinceRecorded, name) {
		return nil
	}
	for _, reserved := range reservedStoreFieldNames {
		if name == reserved {
			return fmt.Errorf("%w: %q is reserved and cannot name a store field on bucket %q (reserved: %s). Choose another name, for example %sItems",
				ErrStoreDeclarationInvalid, name, v.Bucket, strings.Join(reservedStoreFieldNames, ", "), strings.Trim(name, "_"))
		}
	}
	return nil
}

// DeclareStoreFieldsCtx adds store fields to an existing bucket (alter
// bucket Post { Tags hashStore }). The body is additive: a name already
// declared with the same engine is an identity and changes nothing, so a
// replayed or redelivered declare succeeds as "already declared"; one with
// another engine is refused (Q3). The catalog entry is read, changed and
// written inside the bucket's own Apply, so the declaration and the bucket's
// applied_seq commit together — which is what lets replay's DDL gate compare
// a logged declare with the bucket's mark — and the entry is the one read,
// never a literal, so no field another feature adds to it is lost.
func (s *Server) DeclareStoreFieldsCtx(ctx context.Context, req DeclareStoreFieldsRequest) (*DeclareStoreFieldsResponse, error) {
	entries := make([]interface{}, 0, len(req.Stores))
	for _, entry := range req.Stores {
		entries = append(entries, entry)
	}
	added, err := parseDeclarationList(entries)
	if err != nil {
		return nil, fmt.Errorf("bucket %q: %w", req.Name, err)
	}
	bucket, unlock, err := s.lockBucket(req.Database, req.Name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	current := bucket.storeFields()
	err = ValidateStoreDeclarations(StoreValidation{
		Database: req.Database, Bucket: req.Name, BucketType: bucket.bucketType,
		Existing: current.stores, Added: added,
	})
	if err != nil {
		return nil, err
	}
	if err := refuseStoresNamedLikeTypedFields(bucket.declaredFieldTypes(), req.Name, added); err != nil {
		return nil, err
	}
	grown, err := current.withDeclared(added)
	if err != nil {
		return nil, err
	}
	if len(grown.stores) == len(current.stores) {
		return &DeclareStoreFieldsResponse{Success: true, Message: fmt.Sprintf("bucket %q already declares these store fields", req.Name)}, nil
	}
	_, applyErr := store.Apply(store.ApplyRequest{
		KV: bucket.kv, Slot: bucket.slot, DatabaseID: bucket.databaseID, ID: bucket.id.Load(), Keyed: false,
		Info: applyInfoFrom(ctx, s), Owed: bucket.owedApplies(),
		Op: func(snap storage.KVSnapshot, _ *store.BucketState, b storage.KVBatch) error {
			encoded, err := snap.Get(catalogKey(req.Database, req.Name))
			if err != nil {
				return fmt.Errorf("reading the catalog entry of bucket %q in database %q: %w", req.Name, req.Database, err)
			}
			var entry catalogEntry
			if err := bson.Unmarshal(encoded, &entry); err != nil {
				return fmt.Errorf("decoding the catalog entry of bucket %q in database %q: %w", req.Name, req.Database, err)
			}
			entry.Stores, entry.NextStoreID = grown.stores, grown.next
			entry.database, entry.bucket = req.Database, req.Name
			if encoded, err = encodeCatalogEntry(entry); err != nil {
				return err
			}
			return b.Set(catalogKey(req.Database, req.Name), encoded)
		},
	})
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Name, b: bucket, op: "DECLARE STORE FIELDS"}, applyErr)
	if applyErr != nil {
		if errors.Is(applyErr, store.ErrApplyFailed) {
			return &DeclareStoreFieldsResponse{Success: false, Message: failureMessage("declaring store fields failed", applyErr)}, nil
		}
		return nil, applyErr
	}
	// Only once the declaration is committed does the bucket claim it. A plain
	// bucket becomes flagged here, after its id was published: the
	// post-publication write of spec §5.3, under bucket.mu and no db.mu.
	s.setStoreFields(bucket, grown)
	return &DeclareStoreFieldsResponse{Success: true, Message: fmt.Sprintf("bucket %q now declares: %s", req.Name, declaredEngines(grown))}, nil
}

func handleDeclareStoreFields(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req DeclareStoreFieldsRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal declare store fields request: %w", err))
	}
	resp, err := s.DeclareStoreFieldsCtx(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	if resp.Success {
		s.audit.Log(AuditEntry{Event: EventSchemaAlter, Target: "bucket:" + req.Database + "." + req.Name, Result: "ok"})
	}
	payload, err := bson.Marshal(resp)
	if err != nil {
		return errorResponse(frame, fmt.Errorf("encoding the declare store fields response: %w", err))
	}
	return transport.NewFrameWithID(transport.OpDeclareStoreFields, frame.RequestID, payload), nil
}
