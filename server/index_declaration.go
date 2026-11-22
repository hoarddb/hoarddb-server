package server

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// indexObjectKeys is every key an index object may carry.
var indexObjectKeys = []string{"fields", "field", "type", "name", "unique", "expire_after_seconds", "desc"}

// parseIndexSpecs reads an index declaration out of a create-bucket request's
// Options["indices"]: a list whose elements are field names ("Email") or
// objects ({fields: ["Email"]}, {field: "Email", unique: true, name: "…"}).
// Each index gets its id — its position in the declaration — and its default
// name unless it names itself. warnRange, when not nil, is told the name of
// each index declared with type "range", which is accepted and discarded.
//
// Nothing is ignored: a key the parser does not know is refused, because an
// ignored {unique: true} built a plain index and the create reported success
// (build doc §4 F-2(e)). A declaration of something this release does not
// build is refused with the reason, never built as something else.
//
// A []store.IndexSpec is a catalogued declaration on its way back to a bucket
// (indexOptions): it is taken as recorded. No client request decodes into that
// type, so the form is reachable only from this server's own attach paths.
func parseIndexSpecs(options map[string]interface{}, warnRange func(index string)) ([]store.IndexSpec, error) {
	if options == nil {
		return nil, nil
	}
	raw, ok := options["indices"]
	if !ok {
		return nil, nil
	}
	if recorded, ok := raw.([]store.IndexSpec); ok {
		return append([]store.IndexSpec(nil), recorded...), nil
	}

	// Requests arrive over the TCP transport as BSON, so a JSON-shaped list
	// shows up as bson.A and objects as bson.M rather than []interface{} /
	// map[string]interface{}. Accept every shape a client can send.
	var list []interface{}
	switch v := raw.(type) {
	case bson.A:
		list = []interface{}(v)
	case []interface{}:
		list = v
	case []string:
		list = make([]interface{}, 0, len(v))
		for _, name := range v {
			list = append(list, name)
		}
	case bson.M, map[string]interface{}:
		list = []interface{}{v}
	default:
		return nil, fmt.Errorf(`indices must be a list of field names or index objects: {indices: ["Email"]}`)
	}
	if len(list) > math.MaxUint16+1 {
		return nil, fmt.Errorf("a bucket can declare at most %d indexes, not %d: an index id is two bytes", math.MaxUint16+1, len(list))
	}

	specs := make([]store.IndexSpec, 0, len(list))
	byFields := make(map[string]string, len(list))
	byName := make(map[string]bool, len(list))
	for position, element := range list {
		declared, err := parseIndexElement(element)
		if err != nil {
			return nil, err
		}
		spec := declared.spec
		spec.ID = uint16(position)
		spec.Version = store.IndexSpecVersion
		if spec.Name == "" {
			spec.Name = store.DefaultIndexName(spec.Fields)
		}
		fieldList := strings.Join(spec.FieldNames(), "\x00")
		if other, taken := byFields[fieldList]; taken {
			return nil, fmt.Errorf("indexes %q and %q are both on the same fields (%s), and search could not tell which to use: declare one",
				other, spec.Name, strings.Join(spec.FieldNames(), ", "))
		}
		if byName[spec.Name] {
			return nil, fmt.Errorf("two indexes are named %q: give each index its own name", spec.Name)
		}
		byFields[fieldList], byName[spec.Name] = spec.Name, true
		if declared.rangeTyped && warnRange != nil {
			warnRange(spec.Name)
		}
		specs = append(specs, spec)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf(`indices listed no index: {indices: ["Email"]}`)
	}
	return specs, nil
}

// declaredIndex is one element of a declaration, read.
type declaredIndex struct {
	spec       store.IndexSpec
	rangeTyped bool
}

// parseIndexElement reads one element of an index declaration: a field name
// or an index object.
func parseIndexElement(element interface{}) (declaredIndex, error) {
	var object map[string]interface{}
	switch v := element.(type) {
	case string:
		object = map[string]interface{}{"field": v}
	case bson.M:
		object = map[string]interface{}(v)
	case map[string]interface{}:
		object = v
	default:
		return declaredIndex{}, fmt.Errorf(`indices must be field names or index objects, not %T: {indices: ["Email", {field: "Login", unique: true}]}`, element)
	}
	for key := range object {
		if !slices.Contains(indexObjectKeys, key) {
			return declaredIndex{}, fmt.Errorf("index key %q is not one this server reads; an index object takes %s, for example {field: \"Email\", unique: true}",
				key, strings.Join(indexObjectKeys, ", "))
		}
	}

	var declared declaredIndex
	fields, err := indexFieldList(object)
	if err != nil {
		return declaredIndex{}, err
	}
	declared.spec.Fields = fields
	label := strings.Join(declared.spec.FieldNames(), ", ")
	if raw, present := object["name"]; present {
		name, ok := raw.(string)
		if !ok || strings.TrimSpace(name) == "" {
			return declaredIndex{}, fmt.Errorf("the index on %s has a name that is not a non-empty string (%v)", label, raw)
		}
		declared.spec.Name = name
	}
	if raw, present := object["type"]; present {
		kind, _ := raw.(string)
		switch {
		case kind == "", strings.EqualFold(kind, "equality"):
		case strings.EqualFold(kind, "range"):
			declared.rangeTyped = true
		default:
			return declaredIndex{}, fmt.Errorf(`index type %q is not supported: an index is "equality" (the default) or "range", `+
				`for example {indices: [{field: "Email"}, {field: "Age", type: "range"}]}; a unique index is declared with unique: true, `+
				`{field: "Email", unique: true}, a compound index with fields: ["CustomerID", "CreatedAt"], and text indexes are not built yet`, kind)
		}
	}
	desc, err := indexFlag(object, "desc", label)
	if err != nil {
		return declaredIndex{}, err
	}
	if desc {
		return declaredIndex{}, fmt.Errorf("the index on %s asks for desc: true, and a descending component matters only to index-assisted sort, "+
			"which this release does not build; declare it without desc", label)
	}
	unique, err := indexFlag(object, "unique", label)
	if err != nil {
		return declaredIndex{}, err
	}
	declared.spec.Unique = unique
	ttl, err := indexExpiry(object, label)
	if err != nil {
		return declaredIndex{}, err
	}
	declared.spec.TTL = ttl
	if err := refuseUnbuiltIndex(declared.spec, label); err != nil {
		return declaredIndex{}, err
	}
	return declared, nil
}

// indexFieldList reads an index object's field or fields.
func indexFieldList(object map[string]interface{}) ([]store.IndexKeyPart, error) {
	rawField, hasField := object["field"]
	rawFields, hasFields := object["fields"]
	var names []string
	switch {
	case hasField && hasFields:
		return nil, fmt.Errorf(`an index object names both field and fields (%v, %v): use fields for a list, field for one`, rawField, rawFields)
	case hasField:
		name, ok := rawField.(string)
		if !ok {
			return nil, fmt.Errorf(`an index's field must be a field name, not %v: {field: "Email"}`, rawField)
		}
		names = []string{name}
	case hasFields:
		list, err := indexFieldNames(rawFields)
		if err != nil {
			return nil, err
		}
		names = list
	default:
		return nil, fmt.Errorf(`an index object names no field: {field: "Email"} or {fields: ["CustomerID", "CreatedAt"]}`)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf(`an index's fields list is empty: {fields: ["Email"]}`)
	}
	parts := make([]store.IndexKeyPart, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf(`an index names an empty field: {indices: ["Email"]}`)
		}
		if strings.ContainsAny(name, ".[]") {
			return nil, &pathIndexError{field: name}
		}
		if seen[name] {
			return nil, fmt.Errorf("an index names field %q twice: list each field once", name)
		}
		seen[name] = true
		parts = append(parts, store.IndexKeyPart{Field: name})
	}
	return parts, nil
}

// pathIndexError is E-17: an index declared on a path (typed-fields spec
// §4.9.7). Every index reads a top-level field by its literal name,
// so an index on Address.City would index every document as null and its
// unique form would refuse distinct values. prepareBucketLocked names the
// bucket.
type pathIndexError struct {
	field, bucket string
}

func (e *pathIndexError) Error() string {
	where := ""
	if e.bucket != "" {
		where = fmt.Sprintf(" for bucket %q", e.bucket)
	}
	return fmt.Sprintf("The index on %q%s was refused: this release indexes top-level fields only, and a nested path would be read as "+
		"a top-level field of that literal name. Queries on %s still work, by scanning the bucket.\n"+
		"To index it, keep a top-level copy of the value, for example %s, and index that: indices: [%q]",
		e.field, where, inertName(e.field), inertName(topLevelCopyName(e.field)), topLevelCopyName(e.field))
}

// topLevelCopyName suggests a top-level field to hold a copy of a path's
// value: the path's last member name, or the path with its punctuation
// removed when it ends in a position.
func topLevelCopyName(field string) string {
	if path, err := parseFieldPath(field); err == nil {
		for position := len(path.segs) - 1; position >= 0; position-- {
			if !path.segs[position].isIndex {
				return path.segs[position].name
			}
		}
	}
	return strings.NewReplacer(".", "_", "[", "_", "]", "").Replace(field)
}

// indexesAPath reports whether an index names a field no index can read: a
// path. Create refuses one (E-17); one recorded before the refusal is kept in
// the catalog and published with the bucket's indexes, so every encoder
// writes it back, but it is unusable: no index manager, planner, unique check,
// TTL sweep, rebuild or dump sees it (spec §4.9.7).
func indexesAPath(spec store.IndexSpec) bool {
	for _, part := range spec.Fields {
		if strings.ContainsAny(part.Field, ".[]") {
			return true
		}
	}
	return false
}

// usableIndexSpecs is specs without the indexes on a path. It returns specs
// itself, allocating nothing, when there are none, which is every bucket
// created after the refusal.
func usableIndexSpecs(specs []store.IndexSpec) []store.IndexSpec {
	if !slices.ContainsFunc(specs, indexesAPath) {
		return specs
	}
	return slices.DeleteFunc(slices.Clone(specs), indexesAPath)
}

// indexFieldNames reads a fields list in any shape a client can send.
func indexFieldNames(raw interface{}) ([]string, error) {
	var list []interface{}
	switch v := raw.(type) {
	case bson.A:
		list = []interface{}(v)
	case []interface{}:
		list = v
	case []string:
		return v, nil
	default:
		return nil, fmt.Errorf(`an index's fields must be a list of field names, not %v: {fields: ["CustomerID", "CreatedAt"]}`, raw)
	}
	names := make([]string, 0, len(list))
	for _, element := range list {
		name, ok := element.(string)
		if !ok {
			return nil, fmt.Errorf(`an index's fields must be field names, not %v: {fields: ["CustomerID", "CreatedAt"]}`, element)
		}
		names = append(names, name)
	}
	return names, nil
}

// indexFlag reads a boolean key of an index object; absent is false.
func indexFlag(object map[string]interface{}, key, label string) (bool, error) {
	raw, present := object[key]
	if !present {
		return false, nil
	}
	flag, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("the index on %s has %s: %v, which is not true or false", label, key, raw)
	}
	return flag, nil
}

// indexExpiry reads expire_after_seconds: a whole number of seconds, zero or
// more. A client's JSON number arrives as a float64.
func indexExpiry(object map[string]interface{}, label string) (*store.TTLSpec, error) {
	raw, present := object["expire_after_seconds"]
	if !present {
		return nil, nil
	}
	var seconds int64
	switch v := raw.(type) {
	case int32:
		seconds = int64(v)
	case int64:
		seconds = v
	case int:
		seconds = int64(v)
	case float64:
		if v != math.Trunc(v) || v < math.MinInt64 || v > math.MaxInt64 {
			return nil, fmt.Errorf("the index on %s has expire_after_seconds: %v, which is not a whole number of seconds", label, v)
		}
		seconds = int64(v)
	default:
		return nil, fmt.Errorf("the index on %s has expire_after_seconds: %v, which is not a number of seconds", label, raw)
	}
	if seconds < 0 {
		return nil, fmt.Errorf("the index on %s has expire_after_seconds: %d; an expiry is zero or more seconds", label, seconds)
	}
	return &store.TTLSpec{ExpireAfterSeconds: seconds}, nil
}

// refuseUnbuiltIndex refuses what a declaration asks for and no release will
// do: a unique TTL index, and a TTL index over several fields (spec §6.1).
func refuseUnbuiltIndex(spec store.IndexSpec, label string) error {
	if spec.Unique && spec.TTL != nil {
		return fmt.Errorf("the index on %s asks for both unique and expire_after_seconds: a TTL index cannot be unique, because documents "+
			"expiring out from under a unique index would make the rule lapse on a timer; declare two indexes, or drop one of the two", label)
	}
	if spec.TTL != nil && len(spec.Fields) > 1 {
		return fmt.Errorf("the index on %s has expire_after_seconds and %d fields: a TTL index is on one date field", label, len(spec.Fields))
	}
	return nil
}

// rangeTypeWarner returns the warning parseIndexSpecs gives for an index
// declared with type "range" on bucket.
func (s *Server) rangeTypeWarner(database, bucket string) func(index string) {
	return func(index string) {
		s.logger.Warn(fmt.Sprintf("Index %s on bucket %q was declared with type \"range\", which no longer means anything: "+
			"every index is one kind, and the type was discarded. Declare it without a type.", index, bucket),
			"database", database, "bucket", bucket, "index", index)
	}
}

// describeIndexes renders a bucket's indexes for describe: the name, then
// what the name does not say — "Email_1 (unique)", "CreatedAt_1 (ttl 3600s)" —
// and a state that is not ready. The response stays a []string (spec §3.6).
func describeIndexes(specs []store.IndexSpec) []string {
	if len(specs) == 0 {
		return nil
	}
	described := make([]string, 0, len(specs))
	for _, spec := range specs {
		line := spec.Name
		if spec.Unique {
			line += " (unique)"
		}
		if spec.TTL != nil {
			line += fmt.Sprintf(" (ttl %ds)", spec.TTL.ExpireAfterSeconds)
		}
		if indexesAPath(spec) {
			line += " — unusable: indexes a path; ignored"
		} else if spec.State != store.IndexReady {
			line += " — " + string(spec.State)
		}
		described = append(described, line)
	}
	return described
}

// publishedSpecs is a bucket's index descriptors, published for readers that
// take no bucket lock (resolveForRead): a writer replaces the list whole,
// under the bucket mutex, and never changes a published list in place.
type publishedSpecs struct {
	current atomic.Pointer[[]store.IndexSpec]
	// counted is set while this list is in hoarddb_indexes: a served bucket's
	// is, a read view's copy never is. Guarded by the bucket mutex.
	counted bool
}

// publish is store for a served bucket: the indexes it replaces leave the
// hoarddb_indexes gauge and the new ones enter it.
func (p *publishedSpecs) publish(specs []store.IndexSpec) {
	p.uncount()
	p.store(specs)
	countIndexStates(specs, 1)
	p.counted = true
}

// uncount takes a bucket that leaves the registry out of the gauge.
func (p *publishedSpecs) uncount() {
	if p.counted {
		countIndexStates(p.load(), -1)
		p.counted = false
	}
}

// countIndexStates moves hoarddb_indexes by delta for each of specs.
func countIndexStates(specs []store.IndexSpec, delta float64) {
	for _, spec := range specs {
		state := string(spec.State)
		if spec.State == store.IndexReady {
			state = "ready"
		}
		indexesByState.WithLabelValues(state).Add(delta)
	}
}

// load returns the published list; the caller must not modify it.
func (p *publishedSpecs) load() []store.IndexSpec {
	if specs := p.current.Load(); specs != nil {
		return *specs
	}
	return nil
}

// store publishes a copy of specs.
func (p *publishedSpecs) store(specs []store.IndexSpec) {
	if len(specs) == 0 {
		p.current.Store(nil)
		return
	}
	published := append([]store.IndexSpec(nil), specs...)
	p.current.Store(&published)
}
