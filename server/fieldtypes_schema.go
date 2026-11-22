package server

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"weak"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// A bucket's typed-field declaration from the request to the holder
// (typed-fields spec §8.3, §8.4): the create reads it from its options,
// validates it and canonicalises its words; the attach adopts what the
// catalog recorded; both intern the schema, so identical declarations share
// one.

// prepareFieldTypes reads a create request's Options["fields"] and
// Options["field_types"], validates them against the bucket's store fields
// and unique indexes, and sets the bucket's declaration. A non-nil response is
// the refusal.
func (s *Server) prepareFieldTypes(bucket *inMemoryBucket, req CreateBucketRequest, specs []store.IndexSpec) *CreateBucketResponse {
	if recorded, recordedForm := req.Options["fields"].([]catalogField); recordedForm {
		mode, _ := req.Options["field_types"].(string)
		return s.adoptFieldTypes(bucket, catalogEntry{database: req.Database, bucket: req.Name, Fields: recorded, FieldTypes: mode}, specs)
	}
	declared, mode, err := parseFieldTypeOptions(req.Options, req.Name)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: err.Error()}
	}
	err = ValidateFieldDeclarations(FieldValidation{
		Database: req.Database, Bucket: req.Name, BucketType: req.Type,
		Stores: storeNames(bucket), Mode: mode, Fields: declared, Unique: uniqueIndexedFields(specs),
	})
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: err.Error()}
	}
	holder, err := s.fieldTypesHolder(canonicalCatalogFields(declared), canonicalMode(mode))
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q: recording its typed fields failed", req.Name), err)}
	}
	s.setFieldTypes(bucket, holder)
	return nil
}

// adoptRecordedFieldTypes sets a catalogued bucket's declaration from its
// entry, after its store fields and indexes.
func (s *Server) adoptRecordedFieldTypes(bucket *inMemoryBucket, entry catalogEntry) *CreateBucketResponse {
	return s.adoptFieldTypes(bucket, entry, bucket.declaredIndexes())
}

// adoptFieldTypes sets the declaration recorded in entry on bucket; specs are
// the bucket's indexes. A field (or a mode) this binary cannot read is held
// in the holder's recorded, so the bucket opens (spec §8.4). What it can read
// passes the validator a create uses, but a recorded declaration it refuses
// is logged, not refused: refusing the attach would leave the bucket out of
// the registry, and its documents unreachable, for a declaration a create or
// alter before this check let through. Such a bucket's writes are refused
// instead, with the validator's reason: a declaration the checks
// refuse is not one to enforce as recorded.
func (s *Server) adoptFieldTypes(bucket *inMemoryBucket, entry catalogEntry, specs []store.IndexSpec) *CreateBucketResponse {
	mode := entry.FieldTypes
	if fieldTypesModeKnown(mode) {
		mode = canonicalMode(mode)
	}
	validation := FieldValidation{
		Database: entry.database, Bucket: entry.bucket, BucketType: bucket.bucketType,
		Stores: storeNames(bucket), Fields: declarationsOf(readableFields(entry.Fields)), Unique: uniqueIndexedFields(specs),
	}
	if fieldTypesModeKnown(mode) {
		validation.Mode = mode
	}
	invalid := ValidateFieldDeclarations(validation)
	if invalid != nil {
		s.logger.Error(fmt.Sprintf("The typed fields recorded for bucket %q in database %q do not pass this build's checks: %s "+
			"The bucket is served, with its declaration kept as recorded, and every write to it is refused. Dump the bucket and "+
			"restore it into a new bucket created with a declaration that passes, then drop this one.", entry.bucket, entry.database, invalid),
			"database", entry.database, "bucket", entry.bucket)
	}
	holder, err := s.fieldTypesHolder(entry.Fields, mode)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q: adopting its typed fields failed", entry.bucket), err)}
	}
	if invalid != nil {
		if holder.unusable == nil {
			holder.unusable = &unusableDeclaration{}
		}
		holder.unusable.invalid = invalid
	}
	s.setFieldTypes(bucket, holder)
	return nil
}

// fieldTypesHolder builds the holder of a recorded declaration: nil when it
// declares nothing, the interned schema of what this binary can read, and the
// whole declaration when it cannot read all of it.
func (s *Server) fieldTypesHolder(fields []catalogField, mode string) (*bucketFieldTypes, error) {
	if len(fields) == 0 && mode == "" {
		return nil, nil
	}
	readable := readableFields(fields)
	key, err := internKey(readable, mode)
	if err != nil {
		return nil, err
	}
	holder := &bucketFieldTypes{schema: s.schemas.intern(key, func() *declaredSchema { return buildSchema(readable, mode) })}
	if len(readable) < len(fields) || !fieldTypesModeKnown(mode) {
		holder.unusable = &unusableDeclaration{recorded: append(make([]catalogField, 0, len(fields)), fields...)}
	}
	return holder, nil
}

// catalogForm is the declaration as the catalog records it: its fields and
// its mode. A nil holder is a bucket that declares none.
func (ft *bucketFieldTypes) catalogForm() ([]catalogField, string) {
	if ft == nil {
		return nil, ""
	}
	if recorded := ft.recordedFields(); recorded != nil {
		return recorded, ft.schema.mode
	}
	return ft.schema.catalogFields(), ft.schema.mode
}

// recreateDeclarationRefusal answers a create of a bucket that already exists
// and carries a declaration, as a re-create carrying another replication
// factor is answered: the same declaration (or none) is the idempotent
// "already exists", which a replay and a redelivery take; another one, or one
// the validator refuses, is refused naming what the bucket declares. Nil when
// the create may answer "already exists".
func recreateDeclarationRefusal(bucket *inMemoryBucket, req CreateBucketRequest) *CreateBucketResponse {
	if !createDeclaresTypedFields(req) {
		return nil
	}
	declared, mode, err := parseFieldTypeOptions(req.Options, req.Name)
	if err == nil {
		err = ValidateFieldDeclarations(FieldValidation{
			Database: req.Database, Bucket: req.Name, BucketType: bucket.bucketType,
			Stores: storeNames(bucket), Mode: mode, Fields: declared, Unique: uniqueIndexedFields(bucket.declaredIndexes()),
		})
	}
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf("bucket %q already exists, and this create's typed fields were refused: %s", req.Name, err)}
	}
	requested, requestedMode := canonicalCatalogFields(declared), canonicalMode(mode)
	existing, existingMode := bucket.declaredFieldTypes().catalogForm()
	requestedKey, err := internKey(requested, requestedMode)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q: comparing its typed fields failed", req.Name), err)}
	}
	existingKey, err := internKey(existing, existingMode)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q: comparing its typed fields failed", req.Name), err)}
	}
	if requestedKey == existingKey {
		return nil
	}
	return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
		"bucket %q already exists and declares %s; this create declares %s, and the bucket was not changed. "+
			"To change a bucket's typed fields, alter it: alter bucket %s { %s }",
		req.Name, declarationText(existing, existingMode), declarationText(requested, requestedMode), req.Name, alterBody(requested, requestedMode))}
}

// declarationText names a declaration in a message: its fields as they are
// typed, and its mode.
func declarationText(fields []catalogField, mode string) string {
	if len(fields) == 0 && mode == "" {
		return "no typed fields"
	}
	if mode == "" {
		mode = "strict"
	}
	if len(fields) == 0 {
		return fmt.Sprintf("no typed fields (field_types: %s)", mode)
	}
	return fmt.Sprintf("%s (field_types: %s)", fieldList(fields), mode)
}

// alterBody is the body of the alter that declares fields in mode.
func alterBody(fields []catalogField, mode string) string {
	body := fieldList(fields)
	if mode != "" {
		if body != "" {
			body += ", "
		}
		body += "field_types: " + mode
	}
	return body
}

// fieldList is fields as a declaration types them: Name string required, Age int.
func fieldList(fields []catalogField) string {
	entries := make([]string, 0, len(fields))
	for _, declared := range declarationsOf(fields) {
		entry := declared.Name + " " + FieldTypeText(declared)
		if declared.Required {
			entry += " required"
		}
		entries = append(entries, entry)
	}
	return strings.Join(entries, ", ")
}

// describeFieldTypes is a declaration as describe shows it: each field with
// its type's text form, and the mode by name, strict when none is recorded.
// A bucket that declares nothing shows neither.
func describeFieldTypes(holder *bucketFieldTypes) ([]FieldInfo, string) {
	if holder == nil {
		return nil, ""
	}
	fields, mode := holder.catalogForm()
	if mode == "" {
		mode = "strict"
	}
	var described []FieldInfo
	for _, declared := range declarationsOf(fields) {
		described = append(described, FieldInfo{Name: declared.Name, Type: FieldTypeText(declared), Required: declared.Required})
	}
	return described, mode
}

// fieldTypeOptions is the create options that carry a recorded declaration,
// as storeOptions is for store fields: a migrated bucket adopts it, unread
// fields included, rather than declaring it afresh. The fields travel as
// []catalogField, a form no client request decodes into.
func fieldTypeOptions(fields []catalogField, mode string) map[string]interface{} {
	if len(fields) == 0 && mode == "" {
		return nil
	}
	return map[string]interface{}{"fields": fields, "field_types": mode}
}

// readableFields is fields without the ones this binary cannot read.
func readableFields(fields []catalogField) []catalogField {
	readable := make([]catalogField, 0, len(fields))
	for _, field := range fields {
		if field.recorded == nil {
			readable = append(readable, field)
		}
	}
	return readable
}

// uniqueIndexedFields are the fields a usable unique index names: rule 6's
// input. An index on a path is unusable and was never declared on a typed
// field (spec §4.9.7).
func uniqueIndexedFields(specs []store.IndexSpec) []string {
	var names []string
	for _, spec := range usableIndexSpecs(specs) {
		if spec.Unique {
			names = append(names, spec.FieldNames()...)
		}
	}
	return names
}

// fieldTypesModeKnown reports whether mode is one this binary reads; absent
// is strict.
func fieldTypesModeKnown(mode string) bool {
	return mode == "" || slices.Contains(fieldTypesModes, strings.ToLower(mode))
}

// canonicalMode is the mode as the catalog records it: lower case, and
// nothing for strict, the default.
func canonicalMode(mode string) string {
	if mode = strings.ToLower(mode); mode == "strict" {
		return ""
	}
	return mode
}

// canonicalCatalogFields is a validated declaration as the catalog records
// it: canonical words, and a bare object or array for empty braces and for
// array<any> (spec §4.5.1).
func canonicalCatalogFields(declared []FieldDeclaration) []catalogField {
	if len(declared) == 0 {
		return nil
	}
	fields := make([]catalogField, 0, len(declared))
	for _, field := range declared {
		fields = append(fields, catalogField{Name: field.Name, catalogType: canonicalType(field), Required: field.Required})
	}
	return fields
}

func canonicalType(declared FieldDeclaration) catalogType {
	kind, _ := canonicalWord(declared.Type)
	node := catalogType{Type: kind.String(), Members: canonicalCatalogFields(declared.Members)}
	if declared.Elem != nil {
		if elem := canonicalType(*declared.Elem); elem.Type != kindAny.String() {
			node.Elem = &elem
		}
	}
	return node
}

// declarationsOf is a recorded declaration as the validator reads it.
func declarationsOf(fields []catalogField) []FieldDeclaration {
	if len(fields) == 0 {
		return nil
	}
	declared := make([]FieldDeclaration, 0, len(fields))
	for _, field := range fields {
		declaration := declarationOfType(field.catalogType)
		declaration.Name, declaration.Required = field.Name, field.Required
		declared = append(declared, declaration)
	}
	return declared
}

func declarationOfType(node catalogType) FieldDeclaration {
	declaration := FieldDeclaration{Type: node.Type, Members: declarationsOf(node.Members)}
	if node.Elem != nil {
		elem := declarationOfType(*node.Elem)
		declaration.Elem = &elem
	}
	return declaration
}

// buildSchema builds the schema a check reads from a readable declaration.
func buildSchema(fields []catalogField, mode string) *declaredSchema {
	return &declaredSchema{fields: membersOf(fields), mode: mode}
}

func membersOf(fields []catalogField) []memberNode {
	if len(fields) == 0 {
		return nil
	}
	members := make([]memberNode, 0, len(fields))
	for _, field := range fields {
		members = append(members, memberNode{name: field.Name, required: field.Required, typ: typeOf(field.catalogType)})
	}
	return members
}

func typeOf(node catalogType) typeNode {
	kind, _ := canonicalWord(node.Type)
	typ := typeNode{kind: kind, members: membersOf(node.Members)}
	if node.Elem != nil {
		elem := typeOf(*node.Elem)
		typ.elem = &elem
	}
	return typ
}

// catalogFields is the schema's declaration as the catalog records it.
func (schema *declaredSchema) catalogFields() []catalogField {
	return catalogFieldsOf(schema.fields)
}

func catalogFieldsOf(members []memberNode) []catalogField {
	if len(members) == 0 {
		return nil
	}
	fields := make([]catalogField, 0, len(members))
	for _, member := range members {
		fields = append(fields, catalogField{Name: member.name, catalogType: member.typ.catalogType(), Required: member.required})
	}
	return fields
}

func (node typeNode) catalogType() catalogType {
	recorded := catalogType{Type: node.kind.String(), Members: catalogFieldsOf(node.members)}
	if node.elem != nil {
		elem := node.elem.catalogType()
		recorded.Elem = &elem
	}
	return recorded
}

// internKey is the canonical form a schema is interned under: the encoded
// declaration and mode. It is the encoding and not the printed form because a
// field name may hold a space, a comma or a brace, so two different
// declarations can print alike; their encodings cannot.
func internKey(fields []catalogField, mode string) (string, error) {
	encoded, err := bson.Marshal(struct {
		Fields []catalogField `bson:"f"`
		Mode   string         `bson:"m"`
	}{fields, mode})
	if err != nil {
		return "", fmt.Errorf("encoding a typed-field declaration: %w", err)
	}
	return string(encoded), nil
}

// schemaInterner shares one immutable schema among the buckets that declare
// the same fields in the same mode (spec §8.4, D-9): the social model's
// 10^6 buckets of one post schema hold one schema, not 10^6. Its
// entries are weak, so a schema no bucket holds any more is collected; a
// lookup that meets its entry replaces it, and a sweep removes the rest. The
// lock is a leaf, taken under bucket.mu by a create, an attach or an alter
// only — never by a write.
type schemaInterner struct {
	mu    sync.Mutex
	byKey map[string]weak.Pointer[declaredSchema]
	// swept is the table's size after its last sweep. An insert sweeps only
	// once the table has doubled since, so a sweep's O(entries) is paid once
	// per as many inserts and interning stays amortised O(1): a sweep on every
	// insert made a catalog load of n distinct declarations O(n²).
	//
	// The bound is therefore twice the live count at the last sweep, not twice
	// the live count now: a drop never sweeps, so after a mass drop the table
	// keeps its dead entries (50,000 dropped left 50,001 with one live) until
	// as many distinct creates follow. Bounded, collected at the next sweep,
	// no leak.
	swept int
}

// intern returns the schema held under key, building and entering it when
// none is, or when the one entered was collected.
func (in *schemaInterner) intern(key string, build func() *declaredSchema) *declaredSchema {
	in.mu.Lock()
	defer in.mu.Unlock()
	if held, found := in.byKey[key]; found {
		if schema := held.Value(); schema != nil {
			return schema
		}
	}
	if len(in.byKey) >= 2*max(in.swept, 1) {
		for heldKey, held := range in.byKey {
			if held.Value() == nil {
				delete(in.byKey, heldKey)
			}
		}
		in.swept = len(in.byKey)
	}
	if in.byKey == nil {
		in.byKey = make(map[string]weak.Pointer[declaredSchema])
	}
	schema := build()
	in.byKey[key] = weak.Make(schema)
	return schema
}

// size is the number of entries the table holds, collected or not.
func (in *schemaInterner) size() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return len(in.byKey)
}

// parseFieldTypeOptions reads Options["fields"], the wire tree of spec §8.3,
// and Options["field_types"]. A malformed entry is an error naming it, never
// a silent skip: a dropped field is a type the user believes is checked.
func parseFieldTypeOptions(options map[string]interface{}, bucket string) ([]FieldDeclaration, string, error) {
	var mode string
	if raw, present := options["field_types"]; present {
		word, isText := raw.(string)
		if !isText {
			return nil, "", declarationRefusal("field_types on bucket %q must be one of strict, warn or off, written as text: { field_types: warn }.", bucket)
		}
		mode = word
	}
	raw, present := options["fields"]
	if !present || raw == nil {
		return nil, mode, nil
	}
	declared, err := wireTreeReader{bucket: bucket}.list(raw, "")
	return declared, mode, err
}

// wireTreeReader reads a create's wire tree for one bucket, which its
// refusals name.
type wireTreeReader struct {
	bucket string
}

// list reads one level of the tree: the bucket's fields, or an object's
// members (parent is its path).
func (r wireTreeReader) list(raw interface{}, parent string) ([]FieldDeclaration, error) {
	entries, err := optionList(raw)
	if err != nil {
		what := "fields"
		if parent != "" {
			what = fmt.Sprintf("the members of %q", parent)
		}
		return nil, declarationRefusal("%s on bucket %q must be a list of {name, type} declarations, for example [{name: \"Age\", type: \"int\"}].",
			what, r.bucket)
	}
	declared := make([]FieldDeclaration, 0, len(entries))
	for position, entry := range entries {
		keys, err := optionMap(entry)
		if err != nil {
			return nil, declarationRefusal("typed field %d on bucket %q%s must be a {name, type} declaration, for example {name: \"Age\", type: \"int\"}.",
				position+1, r.bucket, inParent(parent))
		}
		name, _ := keys["name"].(string)
		path := name
		if parent != "" {
			path = parent + "." + name
		}
		declaration, err := r.node(keys, fmt.Sprintf("typed field %q on bucket %q%s", name, r.bucket, inParent(parent)), path)
		if err != nil {
			return nil, err
		}
		declared = append(declared, declaration)
	}
	return declared, nil
}

// node reads one field, member or element type from its keys; label names it
// in a refusal and path is where it is declared. The keys are read in sorted
// order, so a declaration with two bad keys is refused for the same one every
// time.
func (r wireTreeReader) node(keys map[string]interface{}, label, path string) (FieldDeclaration, error) {
	var declaration FieldDeclaration
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		value := keys[key]
		var ok bool
		switch key {
		case "name":
			declaration.Name, ok = value.(string)
			if !ok {
				return FieldDeclaration{}, declarationRefusal("%s: its name must be text, for example {name: \"Age\", type: \"int\"}.", label)
			}
		case "type":
			if declaration.Type, ok = value.(string); !ok {
				return FieldDeclaration{}, declarationRefusal("%s: its type must be text, a type word such as \"int\" or \"string\".", label)
			}
		case "required":
			if declaration.Required, ok = value.(bool); !ok {
				return FieldDeclaration{}, declarationRefusal("%s: required must be true or false.", label)
			}
		case "members":
			members, err := r.list(value, path)
			if err != nil {
				return FieldDeclaration{}, err
			}
			declaration.Members = members
		case "elem":
			elemKeys, err := optionMap(value)
			if err != nil {
				return FieldDeclaration{}, declarationRefusal("the element type of %q on bucket %q must be a {type} declaration, for example {type: \"string\"}.",
					path, r.bucket)
			}
			elem, err := r.node(elemKeys, fmt.Sprintf("the element type of %q on bucket %q", path, r.bucket), path+"[]")
			if err != nil {
				return FieldDeclaration{}, err
			}
			declaration.Elem = &elem
		default:
			return FieldDeclaration{}, declarationRefusal("%s declares %q, which is not part of a typed-field declaration "+
				"(a declaration is {name, type, required, members, elem}). Remove it.", label, key)
		}
	}
	if _, typed := keys["type"]; !typed {
		// An element type takes no name (it is the type of every item), so its
		// example is the one that fits: a name there would be refused next.
		example := `{name: "Age", type: "int"}`
		if strings.HasSuffix(path, "[]") {
			example = `{type: "string"}`
		}
		return FieldDeclaration{}, declarationRefusal("%s has no type: give it one, for example %s.", label, example)
	}
	return declaration, nil
}

// inParent names where a declaration was made, for a refusal.
func inParent(parent string) string {
	if parent == "" {
		return ""
	}
	return fmt.Sprintf(" inside %q", parent)
}
