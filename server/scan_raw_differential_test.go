package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// The raw-byte scan differential (bson-codec-spec 7.2, build doc step 7a,
// path "scan"). A filtered scan used to decode every visited document into a
// map and match the map. The oracle below is that map path, copied from
// server/scan_filter.go and server/scan.go as they stood before the move
// (next at 8fc40221), and the tests put the oracle and the server's path
// through the same documents and the same filters: the documents of the
// existing tests, the v1.0 corpus, and generated documents with every BSON
// type. They compare whether each document matches and every error, and, at
// the page level, the documents, their order, Total and HasMore.
//
// One difference is ruled, not a failure: where a document repeats a key on
// the path, the map keeps the last element and the resolver finds the first
// (spec 5.2, "On duplicate keys it returns the first match"). Every server
// write encodes a Go map, which cannot repeat a key, so no stored document
// does; the corpus has none. The tally counts them apart.

// --- the oracle: the map matcher, as it was ---

func oracleMatches(filter *compiledFilter, document map[string]interface{}) (bool, error) {
	return oracleMatchesWithin(filter, document, &matchBudget{remaining: maxMatchWork})
}

func oracleMatchesWithin(filter *compiledFilter, document map[string]interface{}, budget *matchBudget) (bool, error) {
	allMatch := true
	for _, clause := range filter.clauses {
		var matched bool
		var err error
		switch {
		case clause.err != nil:
			err = clause.err
		case clause.logical == "$and":
			matched, err = oracleMatchAll(document, clause.nested, budget)
		case clause.logical == "$or":
			matched, err = oracleMatchAny(document, clause.nested, budget)
		default:
			docValue, present := resolvePath(document, clause.path)
			matched, err = oracleMatchField(docValue, present, clause.condition, budget)
		}
		if err != nil {
			return false, err
		}
		if !matched {
			allMatch = false
		}
	}
	return allMatch, nil
}

func oracleMatchAll(document map[string]interface{}, filters []*compiledFilter, budget *matchBudget) (bool, error) {
	allMatch := true
	for _, filter := range filters {
		matched, err := oracleMatchesWithin(filter, document, budget)
		if err != nil {
			return false, err
		}
		if !matched {
			allMatch = false
		}
	}
	return allMatch, nil
}

func oracleMatchAny(document map[string]interface{}, filters []*compiledFilter, budget *matchBudget) (bool, error) {
	for _, filter := range filters {
		matched, err := oracleMatchesWithin(filter, document, budget)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func oracleMatchField(docValue interface{}, present bool, condition interface{}, budget *matchBudget) (bool, error) {
	if operators, isOperators := operatorDocument(condition); isOperators {
		allMatch := true
		for operator, operand := range operators {
			matched, err := oracleEvalOperator(operator, docValue, present, operand, budget)
			if err != nil {
				return false, err
			}
			if !matched {
				allMatch = false
			}
		}
		return allMatch, nil
	}
	if !present {
		return false, nil
	}
	return valuesEqual(docValue, condition), nil
}

func oracleEvalOperator(operator string, docValue interface{}, present bool, operand interface{}, budget *matchBudget) (bool, error) {
	switch operator {
	case "$gt", "$gte", "$lt", "$lte":
		if !present {
			return false, nil
		}
		ordering, comparable := compareValues(docValue, operand)
		if !comparable {
			return false, nil
		}
		switch operator {
		case "$gt":
			return ordering > 0, nil
		case "$gte":
			return ordering >= 0, nil
		case "$lt":
			return ordering < 0, nil
		default:
			return ordering <= 0, nil
		}
	case "$in":
		candidates, ok := asList(operand)
		if !ok {
			return false, fmt.Errorf("$in expects an array of values, got %T", operand)
		}
		if !present {
			return false, nil
		}
		for _, candidate := range candidates {
			if valuesEqual(docValue, candidate) {
				return true, nil
			}
		}
		return false, nil
	case "$nin":
		candidates, ok := asList(operand)
		if !ok {
			return false, fmt.Errorf("$nin expects an array of values, got %T", operand)
		}
		if !present {
			return false, nil
		}
		for _, candidate := range candidates {
			if valuesEqual(docValue, candidate) {
				return false, nil
			}
		}
		return true, nil
	case "$ne":
		if !present {
			return false, nil
		}
		return !valuesEqual(docValue, operand), nil
	case "$exists":
		boolOp, ok := operand.(bool)
		if !ok {
			return false, fmt.Errorf("$exists expects a boolean operand, got %T", operand)
		}
		return present == boolOp, nil
	case "$regex":
		pattern, ok := operand.(*boundedRegex)
		if !ok {
			return false, fmt.Errorf("$regex operand is %T; the filter was not prepared for matching", operand)
		}
		if !present {
			return false, nil
		}
		text, ok := docValue.(string)
		if !ok {
			return false, nil
		}
		return oracleRegexMatch(pattern, text, budget)
	default:
		return false, fmt.Errorf("unsupported filter operator %q: this version supports $gt, $gte, $lt, $lte, $in, $nin, $ne, $exists and $regex on a field", operator)
	}
}

// oracleRegexMatch is boundedRegex.match as it was: the charge first, then
// the match.
func oracleRegexMatch(pattern *boundedRegex, text string, budget *matchBudget) (bool, error) {
	work := pattern.steps * len(text)
	if work > budget.remaining {
		return false, refuseFilter("$regex on %q would scan %d bytes, past this server's work limit of %d pattern steps times bytes per document (this pattern has %d steps). Use a shorter or simpler pattern, filter on a shorter field, or narrow the query with an indexed field first", pattern.field, len(text), maxMatchWork, pattern.steps)
	}
	budget.remaining -= work
	return pattern.expression.MatchString(text), nil
}

// --- the oracle: the map scan page, as it was ---

func oracleScanFilteredPage(bucket *inMemoryBucket, req ScanRequest, page filteredPage, budget *resultBudget, walk *queryWalk) (*ScanResponse, error) {
	documents := []bson.Raw{}
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	var total int64
	skip := page.skip
	pageEnd := int64(page.skip) + int64(page.limit)

	visit := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		var document map[string]interface{}
		if err := bson.Unmarshal(rawEntry, &document); err != nil {
			return fmt.Errorf("decoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
		}
		matched, err := oracleMatches(filter, document)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
		if total >= int64(skip) && total < pageEnd {
			encoded := bson.Raw(rawEntry)
			if page.projection != nil {
				encoded, err = bson.Marshal(page.projection.apply(document))
				if err != nil {
					return fmt.Errorf("encoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
				}
			}
			if err := budget.chargeDocument(elementHeader(len(documents)) + int64(len(encoded))); err != nil {
				return err
			}
			documents = append(documents, encoded)
		}
		total++
		return nil
	}
	if err := walkBucketDocuments(bucket, req.Database, req.Bucket, visit); err != nil {
		return nil, err
	}
	return &ScanResponse{Found: true, Database: req.Database, Bucket: req.Bucket, Documents: documents, Total: total, HasMore: pageEnd < total}, nil
}

// --- the documents ---

// differentialDocuments are the valid documents both paths read: hand-built
// shapes, the existing tests' documents, generated documents with every BSON
// type, and, unless sampled under the race detector, the v1.0 corpus. A
// TrustStored document whose inside is malformed has no map to compare with,
// because the map decode refuses it; those are out of this differential
// (the resolver's own differential covers them, step 2b).
func differentialDocuments(t *testing.T) []probeDocument {
	t.Helper()
	var documents []probeDocument
	documents = append(documents, handBuiltDocuments()...)
	documents = append(documents, existingTestDocuments(t)...)
	documents = append(documents, generatedDocuments(11, 400)...)
	documents = append(documents, generatedDocuments(12, 400)...)
	documents = append(documents, longStringDocuments()...)
	stride := 1
	if raceEnabled {
		stride = 25
	}
	for position, record := range v1CorpusInputs(t) {
		if position%stride == 0 {
			documents = append(documents, record)
		}
	}
	valid := documents[:0]
	for _, document := range documents {
		if isValidDocument(document.raw) {
			valid = append(valid, document)
		}
	}
	return valid
}

// existingTestDocuments are the documents the scan, search and sort tests
// insert, encoded as the server stores them.
func existingTestDocuments(t *testing.T) []probeDocument {
	t.Helper()
	stamp := time.Date(2026, 9, 24, 18, 30, 15, 123_000_000, time.UTC)
	shapes := []map[string]interface{}{
		{"seq": int32(1), "big": int64(math.MaxInt64), "ratio": 0.1, "name": "alpha", "flag": true, "gone": nil},
		{"seq": int32(2), "nan": math.NaN(), "at": stamp, "tags": []interface{}{"x", int32(7), 2.5}},
		{"seq": int32(3), "blob": primitive.Binary{Subtype: 0x00, Data: []byte{0x00, 0xff, 0x10, 0x80}},
			"nested": map[string]interface{}{"z": int32(1), "a": "first", "m": map[string]interface{}{"deep": 1.5}}},
		{"seq": int32(4), "k1": "v1", "k2": "v2", "k3": "v3"},
		{"seq": int32(42), "payload": strings.Repeat("abcdefghij", 10), "label": "k0000042", "group": scanBandGroup},
		{"Address": map[string]interface{}{"City": "Oslo", "Zip": "0150"}, "Lines": []interface{}{
			map[string]interface{}{"Sku": "A", "Qty": int32(1)}, map[string]interface{}{"Sku": "B", "Qty": int64(3)}}},
		{"n": float64(1 << 53), "m": int64(1<<53 + 1), "neg": math.Copysign(0, -1), "inf": math.Inf(1), "u": int64(-5)},
		{"empty": map[string]interface{}{}, "none": []interface{}{}, "oid": primitive.NewObjectIDFromTimestamp(stamp)},
		{"when": primitive.NewDateTimeFromTime(stamp), "dec": primitive.NewDecimal128(1, 2), "ts": primitive.Timestamp{T: 5, I: 6}},
	}
	documents := make([]probeDocument, 0, len(shapes))
	for position, shape := range shapes {
		shape["_id_"] = fmt.Sprintf("test-%d", position)
		encoded, err := bson.Marshal(shape)
		if err != nil {
			t.Fatalf("encoding test document %d: %v", position, err)
		}
		documents = append(documents, probeDocument{fmt.Sprintf("test document %d", position), encoded})
	}
	return documents
}

// longStringDocuments carry strings long enough to reach the $regex work
// budget of one document.
func longStringDocuments() []probeDocument {
	return []probeDocument{
		{"long string", docOf(elemOf(0x02, "text", bsonString(strings.Repeat("a", 20000))...), int32Element("n", 1))},
		{"two long strings", docOf(elemOf(0x02, "text", bsonString(strings.Repeat("ab", 6000))...),
			elemOf(0x02, "more", bsonString(strings.Repeat("ba", 6000))...), int32Element("n", 2))},
	}
}

// --- the filters ---

// filterKey renders a probe path as a filter key, or false when no key can
// name it (a negative position, a path that begins with a position, a name
// the path grammar refuses).
func filterKey(path resolverPath) (string, bool) {
	if len(path) == 0 || path[0].isPosition {
		return "", false
	}
	var key strings.Builder
	for position, step := range path {
		if step.isPosition {
			if step.position < 0 {
				return "", false
			}
			fmt.Fprintf(&key, "[%d]", step.position)
			continue
		}
		if position > 0 {
			key.WriteByte('.')
		}
		key.WriteString(step.name)
	}
	if _, err := parseFieldPath(key.String()); err != nil {
		return "", false
	}
	return key.String(), true
}

// filterLiterals are operands of every class a filter carries, as a wire
// filter decodes them and as in-process callers pass them.
func filterLiterals() []interface{} {
	return []interface{}{
		int32(0), int32(-5), int32(7), int64(1<<53 + 1), int64(0), float64(2.5), float64(1 << 53), math.NaN(), math.Inf(-1),
		uint64(1<<63 + 5), math.Copysign(0, -1), 1,
		"", "s", "ssss", "alpha", "v1",
		primitive.DateTime(0), time.Unix(0, 0).UTC(), primitive.DateTime(-1),
		true, false, nil,
		bson.A{}, []interface{}{}, map[string]interface{}{}, primitive.ObjectID{}, primitive.Binary{Subtype: 0, Data: []byte{1, 2, 3}},
		primitive.Undefined{}, primitive.MinKey{}, primitive.MaxKey{}, primitive.Symbol("sym"), primitive.JavaScript("code"),
		primitive.Timestamp{}, primitive.Decimal128{}, primitive.Regex{Pattern: "a", Options: "i"}, primitive.Null{},
	}
}

// fieldConditions are the conditions a filter puts on one field whose value,
// in the map path, is value (nil when absent), each with a literal of
// another class beside it.
func fieldConditions(value interface{}, present bool, literal interface{}) []interface{} {
	conditions := []interface{}{
		literal,
		map[string]interface{}{"$gt": literal},
		map[string]interface{}{"$lte": literal},
		map[string]interface{}{"$ne": literal},
		map[string]interface{}{"$exists": true},
		map[string]interface{}{"$exists": false},
		map[string]interface{}{"$in": bson.A{literal, "zz"}},
		map[string]interface{}{"$nin": []interface{}{literal}},
	}
	if present {
		conditions = append(conditions,
			value,
			map[string]interface{}{"$gt": value},
			map[string]interface{}{"$gte": value},
			map[string]interface{}{"$lt": value},
			map[string]interface{}{"$lte": value},
			map[string]interface{}{"$ne": value},
			map[string]interface{}{"$in": bson.A{"zz", value}},
			map[string]interface{}{"$nin": bson.A{value}},
			map[string]interface{}{"$gte": value, "$lt": literal},
		)
		if text, isText := value.(string); isText {
			prefix := text
			if len(prefix) > 3 {
				prefix = prefix[:3]
			}
			conditions = append(conditions,
				map[string]interface{}{"$regex": "^" + regexp.QuoteMeta(prefix)},
				map[string]interface{}{"$regex": "z$"},
			)
		}
	}
	return conditions
}

// erroringFilters each carry one refusal that a document meets, placed where
// the order of evaluation decides whether the document reaches it: beside a
// clause that fails, beside one that matches, in every branch of a $or, and
// behind a $or branch that matches first. key names a field of the document.
func erroringFilters(key string, value interface{}) []map[string]interface{} {
	failing := map[string]interface{}{"zz_absent": int32(1)}
	matching := map[string]interface{}{key: map[string]interface{}{"$exists": true}}
	refusals := []map[string]interface{}{
		{key: map[string]interface{}{"$in": int32(5)}},
		{key: map[string]interface{}{"$nin": "x"}},
		{key: map[string]interface{}{"$exists": "yes"}},
		{key: map[string]interface{}{"$size": int32(1)}},
		{key: map[string]interface{}{"$gt": int32(1), "plain": int32(2)}},
		{"$where": "1"},
		{"$and": int32(5)},
		{"$or": []interface{}{int32(5)}},
		{"a..b": int32(1)},
		{"Lines[": int32(1)},
		{key: map[string]interface{}{"$regex": "a{1,400}b"}},
	}
	var filters []map[string]interface{}
	for _, refusal := range refusals {
		filters = append(filters, refusal)
		withFailing := map[string]interface{}{"zz_absent": int32(1)}
		for refusedKey, condition := range refusal {
			withFailing[refusedKey] = condition
		}
		filters = append(filters, withFailing)
		filters = append(filters,
			map[string]interface{}{"$and": []interface{}{failing, refusal}},
			map[string]interface{}{"$or": []interface{}{failing, refusal}},
			map[string]interface{}{"$or": []interface{}{matching, refusal}},
			map[string]interface{}{"$or": []interface{}{refusal, matching}},
		)
	}
	if text, isText := value.(string); isText && len(text) > 1000 {
		// Over the $regex budget only once both patterns have run.
		filters = append(filters,
			map[string]interface{}{key: map[string]interface{}{"$regex": "(a|b)+c"}, "more": map[string]interface{}{"$regex": "(a|b)+c"}},
			map[string]interface{}{"zz_absent": int32(1), key: map[string]interface{}{"$regex": "(a|b)+c"}, "more": map[string]interface{}{"$regex": "(a|b)+c"}},
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{key: map[string]interface{}{"$regex": "(a|b)+c"}},
				map[string]interface{}{"more": map[string]interface{}{"$regex": "(a|b)+c"}}}},
		)
	}
	return filters
}

// documentFilters derives the filters one document is matched against: for a
// sample of its paths, every condition on the path's own value and on
// literals of other classes; pairs of them as two clauses, $and and $or; and
// when withRefusals, the erroring filters on one of its fields.
func documentFilters(raw []byte, decoded map[string]interface{}, random *rand.Rand, pathsPerDocument int, withRefusals bool) []map[string]interface{} {
	literals := filterLiterals()
	var keys []string
	for _, path := range probePaths(raw) {
		if key, ok := filterKey(path); ok {
			keys = append(keys, key)
		}
	}
	random.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	if len(keys) > pathsPerDocument {
		keys = keys[:pathsPerDocument]
	}
	var single []map[string]interface{}
	for _, key := range keys {
		parsed, err := parseFieldPath(key)
		if err != nil {
			continue
		}
		value, present := resolvePath(decoded, parsed)
		literal := literals[random.Intn(len(literals))]
		for _, condition := range fieldConditions(value, present, literal) {
			single = append(single, map[string]interface{}{key: condition})
		}
	}
	filters := append([]map[string]interface{}{}, single...)
	for pair := 0; pair+1 < len(single) && pair < 40; pair += 2 {
		left, right := single[random.Intn(len(single))], single[random.Intn(len(single))]
		both := map[string]interface{}{}
		for key, condition := range left {
			both[key] = condition
		}
		for key, condition := range right {
			both[key] = condition
		}
		filters = append(filters, both,
			map[string]interface{}{"$and": []interface{}{left, right}},
			map[string]interface{}{"$or": bson.A{left, right}},
			map[string]interface{}{"$or": []interface{}{map[string]interface{}{"$and": []interface{}{left, right}}, right}},
		)
	}
	for key, value := range decoded {
		if _, err := parseFieldPath(key); err == nil && withRefusals {
			filters = append(filters, erroringFilters(key, value)...)
			break
		}
	}
	return filters
}

// --- the comparison ---

// matchTally counts what a run compared.
type matchTally struct {
	documents, evaluations, matched, refused, duplicateKeyRuled, disagreements int
}

func (tally matchTally) String() string {
	return fmt.Sprintf("%d documents, %d document-filter pairs: %d matched, %d refused alike, %d ruled duplicate-key divergences",
		tally.documents, tally.evaluations, tally.matched, tally.refused, tally.duplicateKeyRuled)
}

// repeatsAKey reports whether any document inside raw repeats a key.
func repeatsAKey(raw []byte) bool {
	framed, err := bson.TrustStored(raw)
	if err != nil {
		return false
	}
	seen := map[string]bool{}
	cursor := framed.Cursor()
	for cursor.Next() {
		key := string(cursor.Key())
		if seen[key] {
			return true
		}
		seen[key] = true
		value := cursor.Value()
		if (value.Type() == bsontype.EmbeddedDocument || value.Type() == bsontype.Array) && repeatsAKey(value.Bytes()) {
			return true
		}
	}
	return false
}

// filterMeetsRepeatedKey reports whether any field clause of filter, at any
// depth, resolves a name in a document of raw that repeats the name: the one
// place the map (last element) and the resolver (first) may answer apart.
func filterMeetsRepeatedKey(filter *compiledFilter, raw []byte) bool {
	for _, clause := range filter.clauses {
		for _, nested := range clause.nested {
			if filterMeetsRepeatedKey(nested, raw) {
				return true
			}
		}
		if clause.logical == "" && clause.err == nil && pathMeetsRepeatedKey(raw, clause.path) {
			return true
		}
	}
	return false
}

func pathMeetsRepeatedKey(raw []byte, path fieldPath) bool {
	segments := path.segs
	if path.simple() {
		segments = []pathSeg{{name: path.text}}
	}
	containerType, containerBytes := bsontype.EmbeddedDocument, raw
	for _, segment := range segments {
		framed, err := bson.TrustStored(containerBytes)
		if err != nil {
			return false
		}
		cursor := framed.Cursor()
		var reached bson.View
		found, repeated := false, false
		for position := 0; cursor.Next(); position++ {
			switch {
			case segment.isIndex && containerType == bsontype.Array && position == segment.index && !found:
				reached, found = cursor.Value(), true
			case !segment.isIndex && containerType == bsontype.EmbeddedDocument && cursor.KeyEquals(segment.name):
				if found {
					repeated = true
				} else {
					reached, found = cursor.Value(), true
				}
			}
		}
		if repeated {
			return true
		}
		if !found || (reached.Type() != bsontype.EmbeddedDocument && reached.Type() != bsontype.Array) {
			return false
		}
		containerType, containerBytes = reached.Type(), reached.Bytes()
	}
	return false
}

// sameRefusal compares two errors: both nil, or both refusals of the same
// kind with the same text.
func sameRefusal(oracleErr, serverErr error) bool {
	if oracleErr == nil || serverErr == nil {
		return oracleErr == nil && serverErr == nil
	}
	return errors.Is(oracleErr, ErrFilterRefused) == errors.Is(serverErr, ErrFilterRefused) &&
		oracleErr.Error() == serverErr.Error()
}

// compareMatchers runs one document through every derived filter on both
// paths, each filter compiled once and given to both.
func compareMatchers(t *testing.T, document probeDocument, random *rand.Rand, pathsPerDocument int, tally *matchTally) {
	t.Helper()
	var decoded map[string]interface{}
	if err := bson.Unmarshal(document.raw, &decoded); err != nil {
		return
	}
	tally.documents++
	repeats := repeatsAKey(document.raw)
	// Every eighth document meets the erroring filters: a refusal does not
	// depend on much of the document, and there are 66 of them.
	withRefusals := tally.documents%8 == 1
	for _, filterSpec := range documentFilters(document.raw, decoded, random, pathsPerDocument, withRefusals) {
		filter := compileFilter(filterSpec)
		tally.evaluations++
		// The map is decoded once per document above, not once per filter:
		// the decode is deterministic, and the comparison is of the matchers.
		wantMatch, wantErr := oracleMatches(filter, decoded)
		gotMatch, gotErr := storedDocumentMatches(document.raw, filter)
		if wantMatch == gotMatch && sameRefusal(wantErr, gotErr) {
			if wantMatch {
				tally.matched++
			}
			if wantErr != nil {
				tally.refused++
			}
			continue
		}
		// The ruled difference cannot reach a document the server stored:
		// no corpus record repeats a key, so one that does fails here.
		if repeats && wantErr == nil && gotErr == nil && filterMeetsRepeatedKey(filter, document.raw) &&
			!strings.HasPrefix(document.name, "v1 record") {
			tally.duplicateKeyRuled++
			continue
		}
		tally.disagreements++
		t.Errorf("%s, filter %v: the map path says (%v, %v), the server says (%v, %v)",
			document.name, filterSpec, wantMatch, wantErr, gotMatch, gotErr)
		if tally.disagreements >= maxReportedDisagreements {
			return
		}
	}
}

// maxReportedDisagreements stops a run after the first reports: one wrong
// comparison repeats across every document.
const maxReportedDisagreements = 20

// TestFilteredScanMatchesTheMapPath is the per-document half: every
// document, every derived filter, the same match and the same refusal as the
// map path.
func TestFilteredScanMatchesTheMapPath(t *testing.T) {
	random := rand.New(rand.NewSource(71))
	pathsPerDocument := 6
	if raceEnabled {
		pathsPerDocument = 3
	}
	var tally matchTally
	for _, document := range differentialDocuments(t) {
		compareMatchers(t, document, random, pathsPerDocument, &tally)
		if tally.disagreements >= maxReportedDisagreements {
			t.Fatalf("stopping after %d disagreements; %s", tally.disagreements, tally)
		}
	}
	if tally.matched == 0 || tally.refused == 0 || tally.evaluations-tally.matched == 0 {
		t.Fatalf("the run compared nothing in some class: %s", tally)
	}
	t.Log(tally)
}

// --- the page level ---

// seededDifferentialBuckets fills one bucket of each scannable store type with
// the documents of the differential, as the server stores them: each
// document's map decode, inserted, so what is stored is what a client could
// have written. It returns the bucket names.
func seededDifferentialBuckets(t *testing.T, s *Server, documents []probeDocument) []string {
	t.Helper()
	stores := []struct {
		name       string
		bucketType BucketType
	}{{"diffhash", BucketTypeHash}, {"diffbtree", BucketTypeBTree}, {"difffifo", BucketTypeFIFO}, {"difflifo", BucketTypeLIFO}, {"diffheap", BucketTypeHeap}}
	var names []string
	for _, store := range stores {
		createTestBucket(t, s, "app", store.name, store.bucketType)
		stored := 0
		for position, document := range documents {
			var fields map[string]interface{}
			if err := bson.Unmarshal(document.raw, &fields); err != nil {
				continue
			}
			key := fmt.Sprintf("d%05d", position)
			fields["_id_"] = key
			var err error
			var success bool
			if store.bucketType == BucketTypeHeap {
				var pushed *PushResponse
				pushed, err = s.Push(PushRequest{Database: "app", Bucket: store.name, Priority: int64(position % 17), Data: fields})
				success = err == nil && pushed.Success
			} else {
				var inserted *InsertResponse
				inserted, err = s.Insert(InsertRequest{Database: "app", Bucket: store.name, Key: key, Data: fields})
				success = err == nil && inserted.Success
			}
			if success {
				stored++
			}
		}
		if stored < len(documents)/2 {
			t.Fatalf("%s stored %d of %d documents; the page differential would read too little", store.name, stored, len(documents))
		}
		names = append(names, store.name)
	}
	return names
}

// pageDifferentialDocuments are the documents the page level stores: the
// hand-built and test shapes, generated documents, and every 40th corpus
// record.
func pageDifferentialDocuments(t *testing.T) []probeDocument {
	t.Helper()
	documents := append([]probeDocument{}, handBuiltDocuments()...)
	documents = append(documents, existingTestDocuments(t)...)
	documents = append(documents, generatedDocuments(21, 150)...)
	for position, record := range v1CorpusInputs(t) {
		if position%40 == 0 && (strings.HasSuffix(record.name, "(documents)") || strings.HasSuffix(record.name, "(queue)")) {
			documents = append(documents, record)
		}
	}
	valid := documents[:0]
	for _, document := range documents {
		if isValidDocument(document.raw) {
			valid = append(valid, document)
		}
	}
	return valid
}

// pageFilters are the filters the page level runs: a fixed set, and filters
// derived from a sample of the stored documents.
func pageFilters(documents []probeDocument, random *rand.Rand) []map[string]interface{} {
	filters := []map[string]interface{}{
		nil,
		{"seq": map[string]interface{}{"$gte": int32(2)}},
		{"_id_": map[string]interface{}{"$regex": "^d000"}},
		{"a": map[string]interface{}{"$exists": true}},
		{"a": map[string]interface{}{"$exists": false}, "b": map[string]interface{}{"$exists": true}},
		{"$or": []interface{}{map[string]interface{}{"a": int32(1)}, map[string]interface{}{"name": "alpha"}}},
		{"zz": map[string]interface{}{"$in": int32(5)}},
		{"$and": []interface{}{map[string]interface{}{"zz": int32(1)}, map[string]interface{}{"zz": map[string]interface{}{"$exists": "x"}}}},
	}
	for sample := 0; sample < 12; sample++ {
		document := documents[random.Intn(len(documents))]
		var decoded map[string]interface{}
		if err := bson.Unmarshal(document.raw, &decoded); err != nil {
			continue
		}
		derived := documentFilters(document.raw, decoded, random, 2, true)
		for pick := 0; pick < 4 && len(derived) > 0; pick++ {
			filters = append(filters, derived[random.Intn(len(derived))])
		}
	}
	return filters
}

// scanPageAnswer is a page as the comparison reads it.
type scanPageAnswer struct {
	documents [][]byte
	total     int64
	hasMore   bool
	err       error
}

func (answer scanPageAnswer) String() string {
	return fmt.Sprintf("%d documents of %d (more %v), error %v", len(answer.documents), answer.total, answer.hasMore, answer.err)
}

func samePage(oracle, server scanPageAnswer) bool {
	if !sameRefusal(oracle.err, server.err) || oracle.total != server.total || oracle.hasMore != server.hasMore ||
		len(oracle.documents) != len(server.documents) {
		return false
	}
	for position := range oracle.documents {
		if !bytes.Equal(oracle.documents[position], server.documents[position]) {
			return false
		}
	}
	return true
}

// runFilteredPage runs one page through either path, with the bucket view,
// budget and walk Scan gives it.
func runFilteredPage(t *testing.T, s *Server, req ScanRequest, page func(*inMemoryBucket, ScanRequest, filteredPage, *resultBudget, *queryWalk) (*ScanResponse, error)) scanPageAnswer {
	t.Helper()
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		t.Fatalf("resolving %s.%s: %v", req.Database, req.Bucket, err)
	}
	defer snap.Discard()
	projection, err := compileProjection(req.Projection)
	if err != nil {
		t.Fatalf("compiling projection %v: %v", req.Projection, err)
	}
	budget := s.newResultBudget(queryMethodScan, resultTooLargeError{query: "differential", shape: resultShapeFind, limit: req.Limit, skip: req.Skip})
	walk := newQueryWalk(context.Background(), queryMethodScan, req.Database, req.Bucket)
	response, err := page(bucket, req, filteredPage{skip: req.Skip, limit: req.Limit, projection: projection}, budget, walk)
	if err != nil {
		return scanPageAnswer{err: err}
	}
	answer := scanPageAnswer{total: response.Total, hasMore: response.HasMore}
	for _, document := range response.Documents {
		answer.documents = append(answer.documents, document)
	}
	return answer
}

// pageShapes are the skip, limit and projection of each page compared.
var pageShapes = []struct {
	skip, limit int
	projection  map[string]int
}{
	{0, 100, nil},
	{3, 5, nil},
	{0, 10000, nil},
	{1, 7, map[string]int{"seq": 1, "name": 1}},
	{0, 50, map[string]int{"payload": 0, "nested": 0}},
}

// TestFilteredScanPagesMatchTheMapPath is the page half: on every store type,
// the same documents, in the same order, the same Total and HasMore, and the
// same refusal.
func TestFilteredScanPagesMatchTheMapPath(t *testing.T) {
	s := newTestServer(t)
	documents := pageDifferentialDocuments(t)
	buckets := seededDifferentialBuckets(t, s, documents)
	random := rand.New(rand.NewSource(72))
	filters := pageFilters(documents, random)
	pages, nonEmpty := 0, 0
	for _, bucket := range buckets {
		for _, filterSpec := range filters {
			for _, shape := range pageShapes {
				req := ScanRequest{Database: "app", Bucket: bucket, Filter: filterSpec, Skip: shape.skip, Limit: shape.limit, Projection: shape.projection}
				want := runFilteredPage(t, s, req, oracleScanFilteredPage)
				got := runFilteredPage(t, s, req, scanFilteredPage)
				pages++
				if len(want.documents) > 0 {
					nonEmpty++
				}
				if !samePage(want, got) {
					t.Errorf("%s, filter %v, skip %d limit %d projection %v: the map path gives %s, the server gives %s",
						bucket, filterSpec, shape.skip, shape.limit, shape.projection, want, got)
				}
			}
		}
	}
	if nonEmpty == 0 {
		t.Fatalf("all %d pages were empty; the comparison read nothing", pages)
	}
	t.Logf("%d stored documents per bucket, %d pages compared, %d non-empty", len(documents), pages, nonEmpty)
}

// storedDocumentMatches is the server's per-document decision of a filtered
// scan, the subject of the differential.
func storedDocumentMatches(raw []byte, filter *compiledFilter) (bool, error) {
	document, err := bson.TrustStored(raw)
	if err != nil {
		return false, fmt.Errorf("framing a stored document: %w", err)
	}
	return filter.matchesDocument(document)
}

// --- the sorted scan (build doc step 7a, path "sorted scan") ---

// oracleScanSortedPage is scanSortedPage as it was: pass 1 decodes each
// visited document into a map, matches the map and takes the sort key from
// it; pass 2 takes the page by position.
func oracleScanSortedPage(bucket *inMemoryBucket, req ScanRequest, order sortPage, budget *resultBudget, walk *queryWalk) (*ScanResponse, error) {
	capacity := int64(order.skip) + int64(order.limit)
	selected := &sortCandidateHeap{ascending: order.ascending}
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	budget.refusal.shape = resultShapeRanking
	var position, total int64
	rankMatches := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		current := position
		position++
		var document map[string]interface{}
		if err := bson.Unmarshal(rawEntry, &document); err != nil {
			return fmt.Errorf("decoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
		}
		matched, err := oracleMatches(filter, document)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
		total++
		sortValue, _ := resolvePath(document, order.path)
		value, _ := resolveRawPath(bson.Raw(rawEntry), order.path)
		candidate := sortCandidate{key: sortKeyOf(sortValue), position: current, keyBytes: int64(len(value.Value)) + 16}
		return selected.offer(candidate, capacity, budget)
	}
	if err := walkBucketDocuments(bucket, req.Database, req.Bucket, rankMatches); err != nil {
		return nil, err
	}
	ranked := selected.candidates
	sort.Slice(ranked, func(i, j int) bool { return sortsBefore(ranked[i], ranked[j], order.ascending) })
	var page []sortCandidate
	if order.skip < len(ranked) {
		page = ranked[order.skip:]
	}
	budget.refusal.shape = resultShapeFind
	documents, err := oracleCollectSortedPage(bucket, req, page, order.projection, budget, walk)
	if err != nil {
		return nil, err
	}
	var message string
	if total > 100 {
		message = fmt.Sprintf("note: sorting %d documents without an index", total)
	}
	return &ScanResponse{Found: true, Database: req.Database, Bucket: req.Bucket, Documents: documents, Total: total, HasMore: total > capacity, Message: message}, nil
}

func oracleCollectSortedPage(bucket *inMemoryBucket, req ScanRequest, page []sortCandidate, projection *compiledProjection, budget *resultBudget, walk *queryWalk) ([]bson.Raw, error) {
	if len(page) == 0 {
		return nil, nil
	}
	pageIndex := make(map[int64]int, len(page))
	var lastPosition int64
	for index, candidate := range page {
		pageIndex[candidate.position] = index
		lastPosition = max(lastPosition, candidate.position)
	}
	documents := make([]bson.Raw, len(page))
	var position int64
	takePage := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		current := position
		position++
		if index, ok := pageIndex[current]; ok {
			encoded := bson.Raw(rawEntry)
			if projection != nil {
				var document map[string]interface{}
				if err := bson.Unmarshal(rawEntry, &document); err != nil {
					return fmt.Errorf("decoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
				}
				var err error
				if encoded, err = bson.Marshal(projection.apply(document)); err != nil {
					return fmt.Errorf("encoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
				}
			}
			if err := budget.chargeDocument(elementHeader(index) + int64(len(encoded))); err != nil {
				return err
			}
			documents[index] = encoded
		}
		if current >= lastPosition {
			return errSortedPageComplete
		}
		return nil
	}
	if err := walkBucketDocuments(bucket, req.Database, req.Bucket, takePage); err != nil && !errors.Is(err, errSortedPageComplete) {
		return nil, err
	}
	return documents, nil
}

// oracleSortKey is the key the map path ranked a document by, and what the
// budget charged for holding it.
func oracleSortKey(raw []byte, decoded map[string]interface{}, path fieldPath) (sortKey, int64) {
	sortValue, _ := resolvePath(decoded, path)
	value, _ := resolveRawPath(bson.Raw(raw), path)
	return sortKeyOf(sortValue), int64(len(value.Value)) + 16
}

// TestSortedScanKeysMatchTheMapPath is the per-document half of the sorted
// scan: for every document and every path into it, the key the server ranks
// it by is the map path's, class and value, and so is the budget's charge
// for it.
func TestSortedScanKeysMatchTheMapPath(t *testing.T) {
	documents, keys, ruled, disagreements := 0, 0, 0, 0
	for _, document := range differentialDocuments(t) {
		var decoded map[string]interface{}
		if err := bson.Unmarshal(document.raw, &decoded); err != nil {
			continue
		}
		documents++
		for _, probe := range probePaths(document.raw) {
			key, ok := filterKey(probe)
			if !ok {
				continue
			}
			path, err := parseFieldPath(key)
			if err != nil {
				continue
			}
			keys++
			wantKey, wantCharge := oracleSortKey(document.raw, decoded, path)
			gotKey, gotCharge, err := storedSortKey(document.raw, path)
			if err != nil {
				t.Fatalf("%s, sort key %q: %v", document.name, key, err)
			}
			if sameSortKey(wantKey, gotKey) && wantCharge == gotCharge {
				continue
			}
			if pathMeetsRepeatedKey(document.raw, path) && !strings.HasPrefix(document.name, "v1 record") {
				ruled++
				continue
			}
			t.Errorf("%s, sort key %q: the map path ranks by %+v (charge %d), the server by %+v (charge %d)",
				document.name, key, wantKey, wantCharge, gotKey, gotCharge)
			if disagreements++; disagreements >= maxReportedDisagreements {
				t.FailNow()
			}
		}
	}
	if documents == 0 || keys == 0 {
		t.Fatalf("compared %d keys of %d documents", keys, documents)
	}
	t.Logf("%d documents, %d sort keys compared, %d ruled duplicate-key divergences", documents, keys, ruled)
}

// sameSortKey compares two keys field by field, with NaN equal to NaN.
func sameSortKey(left, right sortKey) bool {
	sameFloat := left.number.float == right.number.float || (math.IsNaN(left.number.float) && math.IsNaN(right.number.float))
	return left.class == right.class && left.text == right.text && left.millis == right.millis && left.flag == right.flag &&
		left.number.kind == right.number.kind && left.number.signed == right.number.signed &&
		left.number.unsigned == right.number.unsigned && sameFloat
}

// runSortedPage runs one sorted page through either path.
func runSortedPage(t *testing.T, s *Server, req ScanRequest, page func(*inMemoryBucket, ScanRequest, sortPage, *resultBudget, *queryWalk) (*ScanResponse, error)) scanPageAnswer {
	t.Helper()
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		t.Fatalf("resolving %s.%s: %v", req.Database, req.Bucket, err)
	}
	defer snap.Discard()
	projection, err := compileProjection(req.Projection)
	if err != nil {
		t.Fatalf("compiling projection %v: %v", req.Projection, err)
	}
	field, ascending, err := singleSortField(req.Sort)
	if err != nil {
		t.Fatalf("sort %v: %v", req.Sort, err)
	}
	path, err := parseFieldPath(field)
	if err != nil {
		t.Fatalf("sort key %q: %v", field, err)
	}
	budget := s.newResultBudget(queryMethodScan, resultTooLargeError{query: "differential", shape: resultShapeFind, limit: req.Limit, skip: req.Skip, sortField: field})
	walk := newQueryWalk(context.Background(), queryMethodScan, req.Database, req.Bucket)
	response, err := page(bucket, req, sortPage{path: path, ascending: ascending, skip: req.Skip, limit: req.Limit, projection: projection}, budget, walk)
	if err != nil {
		return scanPageAnswer{err: err}
	}
	answer := scanPageAnswer{total: response.Total, hasMore: response.HasMore}
	for _, document := range response.Documents {
		answer.documents = append(answer.documents, document)
	}
	return answer
}

// TestSortedScanMatchesTheMapPath is the page half of the sorted scan: on
// every store type, for sort keys of every class and of mixed classes, both
// directions, with and without a filter and a projection, the same
// documents in the same order, Total and HasMore.
func TestSortedScanMatchesTheMapPath(t *testing.T) {
	s := newTestServer(t)
	documents := pageDifferentialDocuments(t)
	buckets := seededDifferentialBuckets(t, s, documents)
	sortFields := []string{"seq", "_id_", "name", "a", "b", "c", "0", "k", "nested.a", "Lines[1].Qty", "tags[0]", "arr[0]", "double", "string", "zz"}
	filters := []map[string]interface{}{
		nil,
		{"a": map[string]interface{}{"$exists": true}},
		{"$or": []interface{}{map[string]interface{}{"b": map[string]interface{}{"$exists": true}}, map[string]interface{}{"seq": map[string]interface{}{"$gt": int32(1)}}}},
		{"zz": map[string]interface{}{"$in": int32(5)}},
	}
	pages, nonEmpty := 0, 0
	for _, bucket := range buckets {
		for _, field := range sortFields {
			for _, direction := range []int{1, -1} {
				for _, filterSpec := range filters {
					for _, shape := range pageShapes {
						req := ScanRequest{Database: "app", Bucket: bucket, Filter: filterSpec, Sort: map[string]int{field: direction},
							Skip: shape.skip, Limit: shape.limit, Projection: shape.projection}
						want := runSortedPage(t, s, req, oracleScanSortedPage)
						got := runSortedPage(t, s, req, scanSortedPage)
						pages++
						if len(want.documents) > 0 {
							nonEmpty++
						}
						if !samePage(want, got) {
							t.Errorf("%s, sort {%s: %d}, filter %v, skip %d limit %d projection %v: the map path gives %s, the server gives %s",
								bucket, field, direction, filterSpec, shape.skip, shape.limit, shape.projection, want, got)
						}
					}
				}
			}
		}
	}
	if nonEmpty == 0 {
		t.Fatalf("all %d pages were empty; the comparison read nothing", pages)
	}
	t.Logf("%d stored documents per bucket, %d sorted pages compared, %d non-empty", len(documents), pages, nonEmpty)
}

// storedSortKey is the server's sort key for one stored document, and the
// budget's charge for it: the subject of the key differential.
func storedSortKey(raw []byte, path fieldPath) (sortKey, int64, error) {
	document, err := bson.TrustStored(raw)
	if err != nil {
		return sortKey{}, 0, fmt.Errorf("framing a stored document: %w", err)
	}
	return documentSortKey(document, libraryPath(path))
}

// TestStoredNaNEqualsANaNLiteralAsTheMapPathDoes pins viewEquals' NaN branch:
// a stored NaN equals a NaN operand, whatever the bits of either, and nothing
// else, under equality, $ne, $in and $nin, as valuesEqual has it. The random
// pairing of TestFilteredScanMatchesTheMapPath does not reliably put a stored
// NaN beside a NaN operand, so the row is written out.
func TestStoredNaNEqualsANaNLiteralAsTheMapPathDoes(t *testing.T) {
	nanBits := func(bits uint64) []byte {
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, bits)
		return out
	}
	documents := []probeDocument{
		{"quiet NaN", docOf(elemOf(0x01, "v", nanBits(math.Float64bits(math.NaN()))...))},
		{"negative NaN", docOf(elemOf(0x01, "v", nanBits(0xfff8000000000001)...))},
		{"signalling NaN", docOf(elemOf(0x01, "v", nanBits(0x7ff0000000000001)...))},
		{"NaN in a nested document", docOf(elemOf(0x03, "d", docOf(elemOf(0x01, "v", nanBits(math.Float64bits(math.NaN()))...))...))},
		{"a number", docOf(elemOf(0x01, "v", nanBits(math.Float64bits(2.5))...))},
		{"an int32", docOf(int32Element("v", 7))},
	}
	operands := []interface{}{math.NaN(), math.Float64frombits(0x7ff8000000000005), float32(math.NaN()), 2.5, int32(7)}
	matchedNaN := 0
	for _, document := range documents {
		var decoded map[string]interface{}
		if err := bson.Unmarshal(document.raw, &decoded); err != nil {
			t.Fatalf("%s: %v", document.name, err)
		}
		for _, key := range []string{"v", "d.v"} {
			for _, operand := range operands {
				for position, condition := range []interface{}{
					operand,
					map[string]interface{}{"$ne": operand},
					map[string]interface{}{"$in": bson.A{"zz", operand}},
					map[string]interface{}{"$nin": bson.A{operand}},
				} {
					filter := compileFilter(map[string]interface{}{key: condition})
					wantMatch, wantErr := oracleMatches(filter, decoded)
					gotMatch, gotErr := storedDocumentMatches(document.raw, filter)
					if wantMatch != gotMatch || !sameRefusal(wantErr, gotErr) {
						t.Errorf("%s, {%s: %v}: the map path says (%v, %v), the server says (%v, %v)",
							document.name, key, condition, wantMatch, wantErr, gotMatch, gotErr)
					}
					// Position 0 is the equality, which matches only NaN to NaN here.
					if wantMatch && position == 0 && strings.Contains(document.name, "NaN") {
						matchedNaN++
					}
				}
			}
		}
	}
	if matchedNaN == 0 {
		t.Fatal("no stored NaN matched a NaN operand; the row does not reach the NaN branch")
	}
}
