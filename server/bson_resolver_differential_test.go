package server

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
)

// The library's Document.Lookup with typed segments stands in for
// resolveRawPath at the raw readers (bson-codec-spec 5.2, build doc step 2b).
// These tests put both through the same bytes and the same paths and compare
// found, type and value bytes. The oracle is resolveRawPath itself, which
// walks bsoncore. The step 6 swap rewrites this file's imports like any other,
// and the call below passes the plain []byte, which both the vendor's and the
// library's Raw accept, so the file compiles and means the same on either tree.
//
// On a document the library's Validate accepts, the two must agree on every
// path. On a framed document whose inside is malformed (only a TrustStored
// Document can hold one), the library's cursor takes the strict reading:
// it refuses lengths the vendor's ReadElement accepts, such as a negative
// string length, and ends the walk there (coordinator ruling, 2026-10-24).
// Those cases are counted as the ruled divergence and classified by the
// library's own reason; any other disagreement fails the test.

// resolverStep is one step of a probe path, kept in a form that builds both
// the library's Segment and the server's pathSeg without reading either.
type resolverStep struct {
	name       string
	position   int
	isPosition bool
}

type resolverPath []resolverStep

func (path resolverPath) library() bson.Path {
	segments := make(bson.Path, 0, len(path))
	for _, step := range path {
		if step.isPosition {
			segments = append(segments, bson.Position(step.position))
		} else {
			segments = append(segments, bson.Name(step.name))
		}
	}
	return segments
}

// server builds the parsed path the way parseFieldPath does: a one-name key
// has no segments and resolves with one member lookup.
func (path resolverPath) server() fieldPath {
	if len(path) == 1 && !path[0].isPosition {
		return fieldPath{text: path[0].name}
	}
	return path.serverWithSegments()
}

// serverWithSegments forces the walking form, so a one-name path is also
// checked through the segment loop.
func (path resolverPath) serverWithSegments() fieldPath {
	segments := make([]pathSeg, 0, len(path))
	for _, step := range path {
		segments = append(segments, pathSeg{name: step.name, index: step.position, isIndex: step.isPosition})
	}
	return fieldPath{segs: segments}
}

func (path resolverPath) String() string {
	var rendered strings.Builder
	for _, step := range path {
		if step.isPosition {
			fmt.Fprintf(&rendered, "[%d]", step.position)
		} else {
			fmt.Fprintf(&rendered, ".%q", step.name)
		}
	}
	return rendered.String()
}

func nameStep(name string) resolverStep { return resolverStep{name: name} }
func positionStep(position int) resolverStep {
	return resolverStep{position: position, isPosition: true}
}

func (path resolverPath) with(step resolverStep) resolverPath {
	extended := make(resolverPath, len(path), len(path)+1)
	copy(extended, path)
	return append(extended, step)
}

// resolverAnswer is one resolver's answer, comparable with ==.
type resolverAnswer struct {
	found     bool
	valueType byte
	valueData string
}

func libraryAnswer(raw []byte, path resolverPath) (resolverAnswer, error) {
	document, err := bson.TrustStored(raw)
	if err != nil {
		return resolverAnswer{}, fmt.Errorf("framing: %w", err)
	}
	view, found, err := document.Lookup(path.library())
	if err != nil {
		return resolverAnswer{}, fmt.Errorf("Lookup: %w", err)
	}
	if !found {
		return resolverAnswer{}, nil
	}
	return resolverAnswer{found: true, valueType: byte(view.Type()), valueData: string(view.Bytes())}, nil
}

func serverAnswer(raw []byte, path fieldPath) resolverAnswer {
	value, found := resolveRawPath(raw, path)
	if !found {
		return resolverAnswer{}
	}
	return resolverAnswer{found: true, valueType: byte(value.Type), valueData: string(value.Value)}
}

func (answer resolverAnswer) String() string {
	if !answer.found {
		return "not found"
	}
	return fmt.Sprintf("found type 0x%02x, %d bytes %x", answer.valueType, len(answer.valueData), answer.valueData)
}

// --- probe paths, enumerated from the document's own shape ---

const (
	probeDepthLimit = 6
	probePathCap    = 1500
)

// probePaths lists paths into raw: every real path, and next to each
// container the near misses (a position one past the end and before the
// start, a decimal-string name where a position belongs, a position where a
// name belongs, a name no element has, the empty name) and a step into every
// scalar. It reads the document with the library's cursor, which the
// differential does not trust for the answer, only for the list.
func probePaths(raw []byte) []resolverPath {
	var paths []resolverPath
	var visit func(prefix resolverPath, containerBytes []byte, isArray bool, depth int)
	visit = func(prefix resolverPath, containerBytes []byte, isArray bool, depth int) {
		if len(paths) >= probePathCap {
			return
		}
		framed, err := bson.TrustStored(containerBytes)
		if err != nil {
			return
		}
		cursor := framed.Cursor()
		elementCount := 0
		type child struct {
			step      resolverStep
			view      bson.View
			viaKey    resolverStep
			isNested  bool
			nestedArr bool
		}
		var children []child
		for cursor.Next() {
			key := string(cursor.Key())
			view := cursor.Value()
			step := nameStep(key)
			if isArray {
				step = positionStep(elementCount)
			}
			viewType := view.Type()
			children = append(children, child{step: step, view: view, viaKey: nameStep(key),
				isNested:  viewType == bsontype.EmbeddedDocument || viewType == bsontype.Array,
				nestedArr: viewType == bsontype.Array})
			elementCount++
		}
		add := func(step resolverStep) { paths = append(paths, prefix.with(step)) }
		for _, member := range children {
			add(member.step)
			add(member.viaKey)
		}
		for position := -1; position <= elementCount+1; position++ {
			add(positionStep(position))
		}
		for position := 0; position <= elementCount; position++ {
			add(nameStep(strconv.Itoa(position)))
		}
		add(nameStep("zz"))
		add(nameStep(""))
		for _, member := range children {
			if member.isNested {
				if depth < probeDepthLimit {
					visit(prefix.with(member.step), member.view.Bytes(), member.nestedArr, depth+1)
				}
				continue
			}
			paths = append(paths, prefix.with(member.step).with(nameStep("x")))
			paths = append(paths, prefix.with(member.step).with(positionStep(0)))
		}
	}
	visit(nil, raw, false, 1)
	return paths
}

// --- documents ---

// probeDocument is a document with a note on what it probes: the same pair as
// namedInput, so the corpora of the validity differential load unchanged.
type probeDocument = namedInput

func int32Element(name string, number int) []byte { return elemOf(0x10, name, le32(number)...) }

func arrayOfInt32(keys []string, numbers []int) []byte {
	elements := make([][]byte, len(keys))
	for position, key := range keys {
		elements[position] = int32Element(key, numbers[position])
	}
	return docOf(elements...)
}

// handBuiltDocuments are the spec's named shapes: arrays, a document key "0",
// out-of-range positions, duplicate keys at the root and nested, an array
// whose keys are out of sequence, a code-with-scope on the way, the empty
// name.
func handBuiltDocuments() []probeDocument {
	inner := docOf(int32Element("v", 1), int32Element("v", 2))
	codeWithScope := append(le32(4+len(bsonString("c"))+len(inner)), bsonString("c")...)
	codeWithScope = append(codeWithScope, inner...)
	return []probeDocument{
		{"empty document", docOf()},
		{"duplicate keys at the root: first wins", docOf(int32Element("a", 1), int32Element("a", 2), int32Element("a", 3))},
		{"duplicate nested keys", docOf(elemOf(0x03, "d", docOf(int32Element("k", 10), int32Element("k", 20))...),
			elemOf(0x03, "d", docOf(int32Element("k", 30))...))},
		{"document with key 0", docOf(elemOf(0x03, "d", docOf(int32Element("0", 5), int32Element("1", 6))...))},
		{"array under a name, read by name 0", docOf(elemOf(0x04, "arr", arrayOfInt32([]string{"0", "1", "2"}, []int{7, 8, 9})...))},
		{"array keys out of sequence", docOf(elemOf(0x04, "arr", arrayOfInt32([]string{"2", "0", "1"}, []int{70, 80, 90})...))},
		{"array keys not numbers", docOf(elemOf(0x04, "arr", arrayOfInt32([]string{"x", "y", "z"}, []int{1, 2, 3})...))},
		{"array keys repeat", docOf(elemOf(0x04, "arr", arrayOfInt32([]string{"0", "0", "0"}, []int{1, 2, 3})...))},
		{"array of documents", docOf(elemOf(0x04, "lines", docOf(
			elemOf(0x03, "0", docOf(elemOf(0x02, "sku", bsonString("A")...), int32Element("qty", 1))...),
			elemOf(0x03, "1", docOf(elemOf(0x02, "sku", bsonString("B")...), int32Element("qty", 2))...))...))},
		{"array in array", docOf(elemOf(0x04, "m", docOf(
			elemOf(0x04, "0", arrayOfInt32([]string{"0", "1"}, []int{1, 2})...),
			elemOf(0x04, "1", arrayOfInt32([]string{"0"}, []int{3})...))...))},
		{"empty name", docOf(int32Element("", 4), elemOf(0x03, "", docOf(int32Element("", 5))...))},
		{"code with scope on the way", docOf(elemOf(0x0F, "w", codeWithScope...), int32Element("after", 3))},
		{"rich document", richDocument()},
		{"long names", docOf(int32Element(strings.Repeat("n", 300), 1), elemOf(0x03, strings.Repeat("m", 300), docOf(int32Element(strings.Repeat("n", 300), 2))...))},
	}
}

// generatedDocuments builds count documents from a seed, with every BSON
// type, nesting to depth 4, arrays with and without sequential keys,
// duplicate keys, and the names 0, 1, 2 and the empty name among the rest.
func generatedDocuments(seed int64, count int) []probeDocument {
	random := rand.New(rand.NewSource(seed))
	documents := make([]probeDocument, 0, count)
	for number := 0; number < count; number++ {
		documents = append(documents, probeDocument{fmt.Sprintf("generated %d/%d", seed, number), generateContainer(random, 1, false)})
	}
	return documents
}

var generatedNames = []string{"a", "b", "c", "0", "1", "2", "", "name", "a.b", "k"}

func generateContainer(random *rand.Rand, depth int, isArray bool) []byte {
	elementCount := random.Intn(7)
	shuffled := random.Intn(5) == 0
	elements := make([][]byte, 0, elementCount)
	for position := 0; position < elementCount; position++ {
		key := generatedNames[random.Intn(len(generatedNames))]
		if isArray && !shuffled {
			key = strconv.Itoa(position)
		}
		elements = append(elements, generateElement(random, depth, key))
	}
	return docOf(elements...)
}

func generateElement(random *rand.Rand, depth int, key string) []byte {
	const kinds = 21
	kind := random.Intn(kinds)
	if depth >= 4 && (kind == 8 || kind == 9 || kind == 20) {
		kind = 0
	}
	oid := make([]byte, 12)
	random.Read(oid)
	eight := make([]byte, 8)
	random.Read(eight)
	switch kind {
	case 0:
		return elemOf(0x10, key, le32(random.Intn(1000)-500)...)
	case 1:
		return elemOf(0x12, key, eight...)
	case 2:
		return elemOf(0x01, key, eight...)
	case 3:
		return elemOf(0x02, key, bsonString(strings.Repeat("s", random.Intn(20)))...)
	case 4:
		return elemOf(0x08, key, byte(random.Intn(2)))
	case 5:
		return elemOf(0x0A, key)
	case 6:
		return elemOf(0x09, key, eight...)
	case 7:
		return elemOf(0x07, key, oid...)
	case 8:
		return elemOf(0x03, key, generateContainer(random, depth+1, false)...)
	case 9:
		return elemOf(0x04, key, generateContainer(random, depth+1, true)...)
	case 10:
		return elemOf(0x05, key, append(append(le32(3), byte(random.Intn(6))), 1, 2, 3)...)
	case 11:
		return elemOf(0x0B, key, 'a', 0, 'i', 0)
	case 12:
		return elemOf(0x0E, key, bsonString("sym")...)
	case 13:
		return elemOf(0x11, key, eight...)
	case 14:
		return elemOf(0x13, key, append(eight, eight...)...)
	case 15:
		return elemOf(0x7F, key)
	case 16:
		return elemOf(0xFF, key)
	case 17:
		return elemOf(0x06, key)
	case 18:
		return elemOf(0x0C, key, append(bsonString("ns"), oid...)...)
	case 19:
		return elemOf(0x0D, key, bsonString("code")...)
	}
	scope := generateContainer(random, depth+1, false)
	codeWithScope := append(le32(4+len(bsonString("c"))+len(scope)), bsonString("c")...)
	return elemOf(0x0F, key, append(codeWithScope, scope...)...)
}

// mutatedDocuments are framed documents with one interior byte changed. The
// framing (length prefix and terminator) is kept, so TrustStored accepts
// them and the resolvers meet whatever the change made of the inside.
func mutatedDocuments(source probeDocument, random *rand.Rand, count int) []probeDocument {
	if len(source.raw) < 8 {
		return nil
	}
	var mutated []probeDocument
	replacements := []byte{0x00, 0x01, 0x02, 0x7f, 0x80, 0xfe, 0xff}
	if count < 0 {
		// Every interior byte against every replacement.
		for position := 4; position < len(source.raw)-1; position++ {
			for _, replacement := range replacements {
				changed := append([]byte{}, source.raw...)
				changed[position] = replacement
				mutated = append(mutated, probeDocument{fmt.Sprintf("%s, byte %d = 0x%02x", source.name, position, replacement), changed})
			}
		}
		return mutated
	}
	for number := 0; number < count; number++ {
		changed := append([]byte{}, source.raw...)
		position := 4 + random.Intn(len(changed)-5)
		changed[position] = replacements[random.Intn(len(replacements))]
		mutated = append(mutated, probeDocument{fmt.Sprintf("%s, byte %d mutated", source.name, position), changed})
	}
	return mutated
}

// --- the comparison ---

// resolverTally counts what a corpus did.
type resolverTally struct {
	documents, paths, agreed, bothFound int
	// divergences by the library's own reason, for the ruled strict reading.
	ruled map[string]int
}

// libraryWalkReason is the reason the library's cursor refuses the document
// somewhere, or "" when a whole recursive walk is clean.
func libraryWalkReason(raw []byte) string {
	framed, err := bson.TrustStored(raw)
	if err != nil {
		return "framing"
	}
	cursor := framed.Cursor()
	for cursor.Next() {
		value := cursor.Value()
		if value.Type() == bsontype.EmbeddedDocument || value.Type() == bsontype.Array {
			if reason := libraryWalkReason(value.Bytes()); reason != "" {
				return reason
			}
		}
	}
	var validation *bson.ValidationError
	if errors.As(cursor.Err(), &validation) {
		return validation.Reason
	}
	return ""
}

// checkResolvers runs every probe path of one document through both
// resolvers. strict says the document passed the library's Validate, so any
// disagreement is a failure; otherwise a disagreement in the direction
// "library does not find what resolveRawPath finds" is counted as the ruled
// divergence, provided the library's cursor refuses the document somewhere.
func checkResolvers(tb testing.TB, document probeDocument, strict bool, tally *resolverTally) {
	tb.Helper()
	tally.documents++
	for _, path := range probePaths(document.raw) {
		for _, serverPath := range []fieldPath{path.server(), path.serverWithSegments()} {
			tally.paths++
			want := serverAnswer(document.raw, serverPath)
			got, err := libraryAnswer(document.raw, path)
			if err != nil {
				tb.Errorf("%s %s: the library returned an error: %v", document.name, path, err)
				continue
			}
			if got == want {
				tally.agreed++
				if got.found {
					tally.bothFound++
				}
				continue
			}
			ruledDirection := !got.found && want.found
			reason := libraryWalkReason(document.raw)
			if !strict && ruledDirection && reason != "" {
				tally.ruled[divergenceCause(want, reason)]++
				continue
			}
			tb.Errorf("%s %s (strict=%v, library walk reason %q): resolveRawPath says %s, the library says %s",
				document.name, path, strict, reason, want, got)
		}
	}
}

// divergenceCause names a ruled divergence. The library's walk reason says
// where the library's cursor first refuses the document, which can be another
// element than the one the path met; a found value of type 0 is the vendor's
// reader taking the document terminator byte, in the middle of a document, for
// an element, which the library refuses.
func divergenceCause(want resolverAnswer, libraryReason string) string {
	if want.valueType == 0 {
		return "vendor reader returns a type byte 0 inside a document as an element; library refuses (" + libraryReason + ")"
	}
	return "vendor reader measures an element the library refuses (" + libraryReason + ")"
}

func newResolverTally() *resolverTally { return &resolverTally{ruled: map[string]int{}} }

func (tally *resolverTally) summary() string {
	reasons := make([]string, 0, len(tally.ruled))
	for reason := range tally.ruled {
		reasons = append(reasons, fmt.Sprintf("%d x %q", tally.ruled[reason], reason))
	}
	sort.Strings(reasons)
	return fmt.Sprintf("%d documents, %d paths x 2 path forms: %d agree (%d found by both), ruled divergences: [%s]",
		tally.documents, tally.paths, tally.agreed, tally.bothFound, strings.Join(reasons, "; "))
}

// validDepth is above every site limit, so Validate's depth check never
// decides which group a document falls in.
const validDepth = 110

func isValidDocument(raw []byte) bool {
	_, err := bson.Validate(raw, bson.Limits{Depth: validDepth})
	return err == nil
}

func TestLookupMatchesResolveRawPath(t *testing.T) {
	var documents []probeDocument
	documents = append(documents, handBuiltDocuments()...)
	documents = append(documents, generatedDocuments(1, 600)...)
	documents = append(documents, bsonoracleCorpusDocuments(t)...)
	documents = append(documents, vectorInputs(t)...)

	tally := newResolverTally()
	validDocuments := 0
	for _, document := range documents {
		if !isValidDocument(document.raw) {
			continue
		}
		validDocuments++
		checkResolvers(t, document, true, tally)
	}
	if validDocuments < 50000 {
		t.Errorf("only %d valid documents reached the differential; the corpus alone has 50000 records", validDocuments)
	}
	if tally.bothFound == 0 || tally.agreed == tally.bothFound {
		t.Errorf("the differential is vacuous: %d paths agree, %d of them found", tally.agreed, tally.bothFound)
	}
	t.Logf("valid: %s", tally.summary())
}

// bsonoracleCorpusDocuments are the v1.0 corpus records that are documents
// the library frames.
func bsonoracleCorpusDocuments(t *testing.T) []probeDocument {
	t.Helper()
	return v1CorpusInputs(t)
}

// TestLookupMatchesResolveRawPathOnMalformedBytes covers the one place the
// two are allowed to differ: a TrustStored document whose inside is not BSON.
// The library takes the strict reading and ends the walk at an element it
// cannot measure. Every disagreement must be that: the library not finding
// what resolveRawPath finds, with the library's cursor refusing the document.
func TestLookupMatchesResolveRawPathOnMalformedBytes(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	var sources []probeDocument
	sources = append(sources, handBuiltDocuments()...)
	sources = append(sources, generatedDocuments(2, 80)...)
	tally := newResolverTally()
	malformed := 0
	for sourceNumber, source := range sources {
		sweep := 12
		if sourceNumber < len(handBuiltDocuments()) && len(source.raw) < 200 {
			sweep = -1
		}
		for _, mutated := range mutatedDocuments(source, random, sweep) {
			if isValidDocument(mutated.raw) {
				checkResolvers(t, mutated, true, tally)
				continue
			}
			malformed++
			checkResolvers(t, mutated, false, tally)
		}
	}
	if malformed == 0 {
		t.Fatal("the byte mutation made no malformed document")
	}
	t.Logf("malformed (%d documents): %s", malformed, tally.summary())
}

// TestLookupNegativeStringLengthIsTheRuledDivergence pins the ruled case by
// name: a string, JavaScript or symbol element with a negative length, in a
// framed document, with an element after it. The library refuses every one
// (the strict reading) and finds nothing past it. The test records, per
// length, what resolveRawPath says, and fails if the library finds anything
// the vendor reader does not. It requires at least one length where the
// vendor reader goes on to find the element after it, or says so and skips.
func TestLookupNegativeStringLengthIsTheRuledDivergence(t *testing.T) {
	ruled := 0
	for _, typeByte := range []byte{0x02, 0x0D, 0x0E} {
		for length := -1; length >= -40; length-- {
			for _, tail := range [][]byte{nil, bytes.Repeat([]byte{0}, 8), bytes.Repeat([]byte{'x'}, 40)} {
				raw := docOf(elemOf(typeByte, "s", append(le32(length), tail...)...), int32Element("after", 9))
				if isValidDocument(raw) {
					t.Fatalf("type 0x%02x length %d: the library's Validate accepts a negative length", typeByte, length)
				}
				for _, name := range []string{"s", "after"} {
					path := resolverPath{nameStep(name)}
					want := serverAnswer(raw, path.server())
					got, err := libraryAnswer(raw, path)
					if err != nil {
						t.Fatal(err)
					}
					if got.found {
						t.Errorf("type 0x%02x length %d tail %d, path %s: the library found %s past a negative length", typeByte, length, len(tail), path, got)
					}
					if want.found {
						ruled++
					}
				}
			}
		}
	}
	t.Logf("negative lengths where resolveRawPath still finds the element or the one after it and the library does not: %d", ruled)
	if ruled == 0 {
		t.Log("resolveRawPath refuses every negative length tried too: the ruled divergence has no instance of this kind")
	}
}

// TestLookupPositionIsNotADecimalKey pins the build doc's acceptance case in
// the server's terms: a position segment counts the elements and never reads
// keys, and a name segment never opens an array.
func TestLookupPositionIsNotADecimalKey(t *testing.T) {
	scrambled := docOf(elemOf(0x04, "arr", arrayOfInt32([]string{"2", "0", "1"}, []int{70, 80, 90})...))
	withDocumentKeyZero := docOf(elemOf(0x03, "d", docOf(int32Element("0", 5))...))
	for _, testCase := range []struct {
		name  string
		raw   []byte
		path  resolverPath
		found bool
	}{
		{"position 0 is the first element, whatever its key", scrambled, resolverPath{nameStep("arr"), positionStep(0)}, true},
		{"name 0 does not open an array", scrambled, resolverPath{nameStep("arr"), nameStep("0")}, false},
		{"position 0 does not open a document with key 0", withDocumentKeyZero, resolverPath{nameStep("d"), positionStep(0)}, false},
		{"name 0 opens a document with key 0", withDocumentKeyZero, resolverPath{nameStep("d"), nameStep("0")}, true},
		{"a position past the end", scrambled, resolverPath{nameStep("arr"), positionStep(3)}, false},
		{"a negative position", scrambled, resolverPath{nameStep("arr"), positionStep(-1)}, false},
	} {
		got, err := libraryAnswer(testCase.raw, testCase.path)
		if err != nil {
			t.Fatalf("%s: %v", testCase.name, err)
		}
		want := serverAnswer(testCase.raw, testCase.path.server())
		if got != want || got.found != testCase.found {
			t.Errorf("%s: library %s, resolveRawPath %s, expected found=%v", testCase.name, got, want, testCase.found)
		}
	}
}
