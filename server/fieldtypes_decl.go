package server

import (
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-server/store"
)

// Typed-field declarations (typed-fields spec §4.7, §8.3): the wire tree a
// create carries, and the one validator every entry point calls.

// FieldDeclaration is one declared typed field, object member or array
// element type, as a create's Options["fields"] carries it (spec §8.3):
// {name, type, required?, members?, elem?}. Members is set only for an object
// with braces and Elem only for an array with an element type; an element has
// no name. The type word may be any synonym of spec §3.1, in any case. It is
// exported because the CLI builds its declarations in it and validates them
// with the server's own validator.
type FieldDeclaration struct {
	Name     string             `bson:"name"`
	Type     string             `bson:"type"`
	Required bool               `bson:"required,omitempty"`
	Members  []FieldDeclaration `bson:"members,omitempty"`
	Elem     *FieldDeclaration  `bson:"elem,omitempty"`
}

// FieldTypeText is the canonical text form of a declared type (D-3), as
// describe prints it and as a declaration is typed: int, array<string>,
// object { City string required }. Words are printed canonical, and empty
// braces and array<any> as the bare word they mean (spec §4.5.1). A word
// this binary does not know is printed as given.
func FieldTypeText(declared FieldDeclaration) string {
	var text strings.Builder
	writeFieldType(&text, declared)
	return text.String()
}

func writeFieldType(text *strings.Builder, declared FieldDeclaration) {
	word := declared.Type
	if kind, known := canonicalWord(word); known {
		word = kind.String()
	}
	text.WriteString(word)
	if len(declared.Members) > 0 {
		text.WriteString(" { ")
		for position, member := range declared.Members {
			if position > 0 {
				text.WriteString(", ")
			}
			text.WriteString(member.Name)
			text.WriteString(" ")
			writeFieldType(text, member)
			if member.Required {
				text.WriteString(" required")
			}
		}
		text.WriteString(" }")
	}
	if declared.Elem != nil {
		if kind, known := canonicalWord(declared.Elem.Type); known && kind == kindAny {
			return
		}
		text.WriteString("<")
		writeFieldType(text, *declared.Elem)
		text.WriteString(">")
	}
}

// FieldValidation is one declaration for ValidateFieldDeclarations: the
// bucket it is made on, the names of the store fields on the bucket's
// documents (rule 3: a name is a typed field or a store field, never both),
// the mode, the typed fields, and the fields a unique index names (rule 6).
type FieldValidation struct {
	Database   string
	Bucket     string
	BucketType BucketType
	Stores     []string
	Mode       string
	Fields     []FieldDeclaration
	Unique     []string
}

// The limits of spec §4.5.3 and §4.7 rule 4. They bound the catalog entry and
// the in-memory schema (§9.3), and the declared depth is the recursion bound
// of every walk over a document.
const (
	maxFieldsPerLevel  = 256
	maxFieldTypeDepth  = 8
	maxDeclaredNodes   = 1024
	fieldTypeWordsLine = "string, int32, int (or integer, int64), double (or float), decimal, number, bool (or boolean), " +
		"datetime (or date, timestamp), bsontimestamp, binary, uuid, objectid, object, array, regex, null, any"
)

// reservedTopLevelFieldNames are the declaration body's option words (rule
// 2): a top-level field of one of these names would be read as the option.
// Inside an object's braces they are ordinary names. _id_ has its own text.
var reservedTopLevelFieldNames = []string{"type", "indices", "acknowledge", "replication_factor", "field_types"}

// uniqueEncodableKinds are the kinds a unique index can hold every value of
// (rule 6): store/indexkey.go:EncodeComponent encodes null, bool, numbers,
// dates, strings and binary, and number is left out because it admits a
// Decimal128.
var uniqueEncodableKinds = map[fieldKind]bool{
	kindString: true, kindInt32: true, kindInt: true, kindDouble: true,
	kindBool: true, kindDatetime: true, kindBinary: true, kindUUID: true,
}

// fieldTypesModes are the words field_types takes (spec §4.8).
var fieldTypesModes = []string{"strict", "warn", "off"}

// declarationRefusal is an E-9 refusal: its text is the whole message.
func declarationRefusal(format string, args ...interface{}) error {
	return &sentencedError{message: fmt.Sprintf(format, args...), cause: ErrFieldDeclarationInvalid}
}

// ValidateFieldDeclarations is the one owner of the typed-field rules (spec
// §4.7): the create, the attach, the alter and the CLI's parser all call it,
// so no entry point can accept what another refuses. Every refusal wraps
// ErrFieldDeclarationInvalid and is one of E-9's texts.
func ValidateFieldDeclarations(v FieldValidation) error {
	if v.Mode != "" && !slices.Contains(fieldTypesModes, strings.ToLower(v.Mode)) {
		return declarationRefusal("field_types %q is not a mode of bucket %q: the modes are strict (a write that does not match "+
			"its declaration is refused; the default), warn (it is stored and counted) and off (nothing is checked).", v.Mode, v.Bucket)
	}
	if len(v.Fields) == 0 {
		return nil
	}
	if v.BucketType == BucketTypeBlob {
		return declarationRefusal("Bucket %q is a blob bucket, and a blob's entries are content, not documents with fields, "+
			"so it cannot declare typed fields. Declare them on a hash bucket that references the blob by key.", v.Bucket)
	}
	walk := declarationWalk{bucket: v.Bucket}
	if err := walk.level(v.Fields, "", "", 0); err != nil {
		return err
	}
	if walk.nodes > maxDeclaredNodes {
		return declarationRefusal("Bucket %q declares %d typed nodes (fields, members and element types at every level); the limit is 1,024. "+
			"Declare the rarely checked parts as object or array without braces.", v.Bucket, walk.nodes)
	}
	for _, field := range v.Fields {
		if slices.Contains(v.Stores, field.Name) {
			return typedAndStoreFieldClash(field.Name, v.Bucket)
		}
	}
	return checkUnique(v.Fields, v.Unique)
}

// typedAndStoreFieldClash is rule 3's refusal, at a create and at an alter that
// adds a store field.
func typedAndStoreFieldClash(name, bucket string) error {
	return declarationRefusal("%q is declared on bucket %q both as a typed field and as a store field; a name is one or the other. "+
		"Rename one of them.", name, bucket)
}

// refuseStoresNamedLikeTypedFields is rule 3 at an alter that adds store
// fields: a store field on the bucket's documents may not take the name of a
// typed field the bucket declares.
func refuseStoresNamedLikeTypedFields(holder *bucketFieldTypes, bucket string, added []StoreDeclaration) error {
	fields, _ := holder.catalogForm()
	for _, declared := range added {
		if slices.ContainsFunc(fields, func(field catalogField) bool { return field.Name == declared.Name }) {
			return typedAndStoreFieldClash(declared.Name, bucket)
		}
	}
	return nil
}

// declarationWalk walks a declaration's tree once, counting its nodes.
type declarationWalk struct {
	bucket string
	nodes  int
}

// level checks the fields declared on one level: the bucket's top level
// (parent ""), or an object's members (parent its path). depth is the number
// of braces and brackets the level is inside.
func (w *declarationWalk) level(fields []FieldDeclaration, parent, top string, depth int) error {
	if len(fields) > maxFieldsPerLevel {
		return declarationRefusal("Bucket %q declares %d typed fields on one level; the limit is 256. Group related values in an object field.",
			w.bucket, len(fields))
	}
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		if parent == "" {
			if err := checkTopLevelName(field); err != nil {
				return err
			}
		}
		if err := checkFieldName(field.Name); err != nil {
			return err
		}
		if len(field.Name) > maxNameBytes {
			return declarationRefusal("Field name %s on bucket %q is too long: it is %s bytes and the limit is %d. Use a shorter field name.",
				echoName(field.Name), w.bucket, groupThousands(len(field.Name)), maxNameBytes)
		}
		path := field.Name
		if parent != "" {
			path = parent + "." + field.Name
		}
		if seen[field.Name] {
			return declarationRefusal("Field %q is declared twice on bucket %q; declare each field once.", path, w.bucket)
		}
		seen[field.Name] = true
		fieldTop := top
		if parent == "" {
			fieldTop = field.Name
		}
		if err := w.node(field, path, parent, fieldTop, depth); err != nil {
			return err
		}
	}
	return nil
}

// node checks one field, member or element type, and what it declares
// inside it. parent is the path of the object or array it is declared in,
// empty at the top level.
func (w *declarationWalk) node(declared FieldDeclaration, path, parent, top string, depth int) error {
	w.nodes++
	if depth > maxFieldTypeDepth {
		return declarationRefusal("%q is declared %d levels deep inside %q; a field's type nests at most 8 levels "+
			"(each object { … } and array<…> is one). Give the inner part its own top-level field, or its own bucket referenced by key.",
			path, depth, top)
	}
	kind, known := canonicalWord(declared.Type)
	if !known {
		if engine, isStore := storeKindWord(declared.Type); isStore && parent != "" {
			member := declared.Name
			if member == "" {
				member = path
			}
			return declarationRefusal("%q inside %q is declared %sStore; a store belongs to a document, and an object inside a document is not one. "+
				"Declare %s as a store field of the bucket, or as array<object { … }> inside %s.", member, parent, engine, member, parent)
		}
		return declarationRefusal("%q is not a field type, so field %q on bucket %q cannot be declared with it. The field types are: %s.",
			declared.Type, path, w.bucket, fieldTypeWordsLine)
	}
	if len(declared.Members) > 0 && kind != kindObject {
		return w.shapeRefusal(path, kind, "members")
	}
	if declared.Elem != nil && kind != kindArray {
		return w.shapeRefusal(path, kind, "an element type")
	}
	if len(declared.Members) > 0 {
		return w.level(declared.Members, path, top, depth+1)
	}
	if declared.Elem != nil {
		return w.node(*declared.Elem, path+"[]", path, top, depth+1)
	}
	return nil
}

func (w *declarationWalk) shapeRefusal(path string, kind fieldKind, what string) error {
	return declarationRefusal("Field %q on bucket %q is declared %s with %s, but only object takes members (object { City string }) "+
		"and only array takes an element type (array<string>).", path, w.bucket, kind, what)
}

// storeKindWord reads a store kind written where a type belongs (fifoStore,
// HASHSTORE), returning its engine's catalog name.
func storeKindWord(word string) (string, bool) {
	if !strings.HasSuffix(strings.ToLower(word), "store") {
		return "", false
	}
	engine, err := store.ParseFieldEngine(engineName(word))
	if err != nil {
		return "", false
	}
	return engine.String(), true
}

// checkTopLevelName is rule 2: _id_ and the option words are not top-level
// field names.
func checkTopLevelName(field FieldDeclaration) error {
	if field.Name == "_id_" {
		entry := "_id_ " + field.Type
		if field.Required {
			entry += " required"
		}
		return declarationRefusal("_id_ is the document key; hoardDB always stores it as a string and creates it when a document has none, "+
			"so it is never declared. Remove this entry (%q) from the declaration; nothing else changes.", entry)
	}
	if slices.Contains(reservedTopLevelFieldNames, field.Name) {
		return declarationRefusal("%q is an option of the declaration body (%s), so it cannot name a top-level field. "+
			"Name the field differently, for example %sValue, or declare it inside an object.",
			field.Name, strings.Join(reservedTopLevelFieldNames, ", "), field.Name)
	}
	return nil
}

// checkFieldName is rule 1, at every level: a name no path can be confused
// with. It is not StoreValidation.checkName, which also reserves the
// directory words a store field may not take (build doc §9 E-8).
func checkFieldName(name string) error {
	if name != "" && !strings.ContainsAny(name, ".[]\x00") && !strings.HasPrefix(name, "$") {
		return nil
	}
	suggestion := strings.TrimLeft(strings.Map(func(r rune) rune {
		if strings.ContainsRune(".[]\x00", r) {
			return -1
		}
		return r
	}, name), "$")
	if suggestion == "" {
		suggestion = "Field"
	}
	return declarationRefusal("%q cannot name a field: a field name is non-empty, contains no \".\", \"[\", \"]\" or NUL, and does not start with \"$\" "+
		"(\".\" and brackets are how a path names a value inside a value). Name it like a document field, for example %s.", name, suggestion)
}

// checkUnique is rule 6: a unique index on a declared field is allowed on a
// top-level field whose declared type an index can encode. A unique index on
// a field the declaration does not name is the index's own business.
func checkUnique(fields []FieldDeclaration, unique []string) error {
	for _, name := range unique {
		root, _, nested := strings.Cut(name, ".")
		root, _, positioned := strings.Cut(root, "[")
		index := slices.IndexFunc(fields, func(field FieldDeclaration) bool { return field.Name == root })
		if index < 0 {
			continue
		}
		kind, _ := canonicalWord(fields[index].Type)
		switch {
		case kind == kindArray || positioned:
			return declarationRefusal("%q declares unique on an array; unique applies only to a top-level field in this release.", name)
		case nested:
			return declarationRefusal("%q declares unique on an object member; unique applies only to a top-level field in this release.", name)
		case !uniqueEncodableKinds[kind]:
			return declarationRefusal("%q is declared %s unique, and an index cannot hold %s values, so every write carrying one would be refused. "+
				"unique applies to string, int32, int, double, bool, datetime, binary and uuid fields. Drop unique, or declare the field one of those types.",
				name, kind, kind)
		}
	}
	return nil
}
