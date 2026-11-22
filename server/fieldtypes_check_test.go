package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// schemaOf declares top-level fields from "Name word [required]" entries, the
// way the declaration will, for a test that checks documents against them.
func schemaOf(tb testing.TB, entries ...string) *bucketFieldTypes {
	tb.Helper()
	fields := make([]memberNode, 0, len(entries))
	for _, entry := range entries {
		words := strings.Fields(entry)
		kind, known := canonicalWord(words[1])
		if !known {
			tb.Fatalf("%q: %q is not a type word", entry, words[1])
		}
		fields = append(fields, memberNode{name: words[0], required: len(words) > 2 && words[2] == "required", typ: typeNode{kind: kind}})
	}
	return &bucketFieldTypes{schema: &declaredSchema{fields: fields}}
}

// TestUndeclaredFieldsAreAccepted is spec §4.4: a declaration constrains the
// fields it names and says nothing about the others.
func TestUndeclaredFieldsAreAccepted(t *testing.T) {
	declared := schemaOf(t, "Name string")
	document := wireDecoded(t, bson.D{
		{Key: "Name", Value: "Ada"}, {Key: "Extra", Value: int32(5)}, {Key: "Other", Value: bson.D{{Key: "x", Value: true}}},
	})
	if mismatch := checkDocument(declared, document); mismatch != nil {
		t.Errorf("an undeclared field was refused: %v", mismatch)
	}
	if mismatch := checkDocument(nil, document); mismatch != nil {
		t.Errorf("a bucket that declares nothing refused a document: %v", mismatch)
	}
}

// TestNullAndMissingSatisfyOptionalAndFailRequiredInADocument is the document
// half of spec §4.1 to §4.3.
func TestNullAndMissingSatisfyOptionalAndFailRequiredInADocument(t *testing.T) {
	declared := schemaOf(t, "Name string", "Age int required", "Meta any required")
	rows := []struct {
		name     string
		document map[string]interface{}
		refused  string
		reason   mismatchReason
	}{
		{"every field present", map[string]interface{}{"Name": "Ada", "Age": int32(36), "Meta": int32(1)}, "", mismatchNone},
		{"an optional field missing", map[string]interface{}{"Age": int32(36), "Meta": "m"}, "", mismatchNone},
		{"an optional field null", map[string]interface{}{"Name": nil, "Age": int64(36), "Meta": "m"}, "", mismatchNone},
		{"a required field missing", map[string]interface{}{"Name": "Ada", "Meta": "m"}, "Age", mismatchRequired},
		{"a required field null", map[string]interface{}{"Age": nil, "Meta": "m"}, "Age", mismatchRequired},
		{"any required, present and of any type", map[string]interface{}{"Age": int32(1), "Meta": map[string]interface{}{}}, "", mismatchNone},
		{"any required, missing", map[string]interface{}{"Age": int32(1)}, "Meta", mismatchRequired},
		{"any required, null", map[string]interface{}{"Age": int32(1), "Meta": nil}, "Meta", mismatchRequired},
		{"a wrong type", map[string]interface{}{"Name": int32(3), "Age": int32(36), "Meta": "m"}, "Name", mismatchWrongType},
	}
	for _, row := range rows {
		mismatch := checkDocument(declared, row.document)
		switch {
		case row.refused == "" && mismatch != nil:
			t.Errorf("%s: refused: %v", row.name, mismatch)
		case row.refused != "" && mismatch == nil:
			t.Errorf("%s: accepted; want %s refused", row.name, row.refused)
		case mismatch != nil && (mismatch.path != row.refused || mismatch.reason != row.reason):
			t.Errorf("%s: refused %q for reason %d; want %q for reason %d", row.name, mismatch.path, mismatch.reason, row.refused, row.reason)
		}
	}
}

// TestCheckDocumentNamesTheFirstDeclaredMismatch: with two bad fields every
// copy must report the same one, so the order is the declaration's, never a
// map's.
func TestCheckDocumentNamesTheFirstDeclaredMismatch(t *testing.T) {
	declared := schemaOf(t, "Alpha string", "Beta int", "Gamma bool")
	document := map[string]interface{}{"Alpha": "ok", "Beta": "no", "Gamma": "no"}
	for range 200 {
		mismatch := checkDocument(declared, document)
		if mismatch == nil || mismatch.path != "Beta" {
			t.Fatalf("checkDocument reported %v; want Beta, the first declared field that does not fit", mismatch)
		}
	}
}

func TestTypeMismatchCarriesThePathTheNodeTheReceivedWordAndTheReason(t *testing.T) {
	declared := schemaOf(t, "Age int32")
	mismatch := checkDocument(declared, map[string]interface{}{"Age": int64(1) << 40})
	if mismatch == nil {
		t.Fatal("an int64 beyond int32 was accepted")
	}
	if mismatch.path != "Age" || mismatch.declared.kind != kindInt32 || mismatch.received != "a 64-bit integer" || mismatch.reason != mismatchInt32Range {
		t.Errorf("mismatch = {path %q, declared %v, received %q, reason %d}; want Age, int32, a 64-bit integer, mismatchInt32Range",
			mismatch.path, mismatch.declared.kind, mismatch.received, mismatch.reason)
	}
}

// TestTypeMismatchIsAFieldTypeRefusalThatWasNotApplied: a type refusal is
// deterministic and decided before the apply, so it answers FIELD_TYPE, counts
// as a constraint's answer at replay, and the owner's error frame carries
// write_outcome not_applied exactly as TF-2's E-18 does
// (Cluster.handlerRefusalWithOutcome). This sends a mismatch through the same
// functions a handler's refusal takes.
func TestTypeMismatchIsAFieldTypeRefusalThatWasNotApplied(t *testing.T) {
	var owner *Cluster
	tc := newTestClusterTuned(t, 1, 1, nil, func(index int, cluster *Cluster) { owner = cluster })
	defer tc.stop()
	createTestHashBucket(t, tc)

	mismatch := checkDocument(schemaOf(t, "Age int"), map[string]interface{}{"Age": "thirty"})
	if mismatch == nil {
		t.Fatal("a string into int was accepted")
	}
	if !errors.Is(mismatch, ErrFieldType) || codeFor(mismatch) != transport.ErrFieldType {
		t.Fatalf("the refusal = %v (code %s); want FIELD_TYPE", mismatch, codeFor(mismatch))
	}
	frame := errorFrame(&transport.Frame{}, mismatch)
	if !refusedByConstraint(frame.Payload) {
		t.Error("a replay would not count the type refusal as the constraint's answer")
	}
	stamped := owner.handlerRefusalWithOutcome(ReplResponse{Opcode: int32(transport.OpError), Payload: frame.Payload}, transport.OpPut, &applyOutcome{}, testDatabase, testBucket)
	var answer ErrorResponse
	if err := bson.Unmarshal(stamped.Payload, &answer); err != nil {
		t.Fatalf("decoding the stamped refusal: %v", err)
	}
	if transport.ErrorCode(answer.Code) != transport.ErrFieldType || answer.WriteOutcome != outcomeNotApplied {
		t.Errorf("the owner's refusal = code %#x, write_outcome %q; want FIELD_TYPE and %q", answer.Code, answer.WriteOutcome, outcomeNotApplied)
	}
}

// undeclaredBucket is a package variable so a benchmark's nil test is not
// folded away: a write loads the pointer from the bucket.
var undeclaredBucket *bucketFieldTypes

// benchFields is a 20-field document: name, a value as the wire decodes it,
// and the word that declares it.
var benchFields = []struct {
	name  string
	value interface{}
	word  string
}{
	{"Name", "Ada Lovelace", "string"}, {"Age", int32(36), "int"}, {"Score", 9.5, "double"}, {"Active", true, "bool"},
	{"Created", primitive.DateTime(1759492800000), "datetime"}, {"Owner", primitive.NewObjectID(), "objectid"},
	{"Tags", bson.A{"a", "b", "c"}, "array"}, {"Address", bson.D{{Key: "City", Value: "Tirana"}}, "object"},
	{"Avatar", primitive.Binary{Subtype: 0, Data: []byte{1, 2, 3}}, "binary"}, {"Rank", int64(7), "int"},
	{"Email", "ada@example.com", "string"}, {"Phone", "0690000000", "string"}, {"Country", "AL", "string"},
	{"Visits", int32(12), "int32"}, {"Balance", primitive.NewDecimal128(0, 100), "decimal"}, {"Verified", false, "bool"},
	{"Joined", primitive.DateTime(1759492800001), "datetime"}, {"Notes", "none", "string"}, {"Level", int64(3), "number"},
	{"Extra", nil, "any"},
}

// benchDocument is benchFields decoded the way a handler decodes a frame, so
// its values are the wire's Go types and not a literal's (spec §9.4).
func benchDocument(tb testing.TB) map[string]interface{} {
	tb.Helper()
	encoded := make(bson.D, 0, len(benchFields))
	for _, field := range benchFields {
		encoded = append(encoded, bson.E{Key: field.name, Value: field.value})
	}
	return wireDecoded(tb, encoded)
}

// benchSchema declares the first fieldCount of benchFields.
func benchSchema(tb testing.TB, fieldCount int) *bucketFieldTypes {
	tb.Helper()
	entries := make([]string, 0, fieldCount)
	for _, field := range benchFields[:fieldCount] {
		entries = append(entries, field.name+" "+field.word)
	}
	return schemaOf(tb, entries...)
}

var checkSink *typeMismatch

// BenchmarkFieldTypeCheck is the checker's cost per document (spec §9.4):
// undeclared is the nil test, the others declare 1, 5 and all 20 fields of a
// wire-decoded 20-field document. The budget is 2 ns undeclared and 40 ns a
// declared field (stop-and-ask SA-7).
func BenchmarkFieldTypeCheck(b *testing.B) {
	document := benchDocument(b)
	b.Run("undeclared", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			checkSink = checkDocument(undeclaredBucket, document)
		}
	})
	for _, declared := range []int{1, 5, 20} {
		schema := benchSchema(b, declared)
		name := fmt.Sprintf("%dfields", declared)
		if declared == 1 {
			name = "1field"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				checkSink = checkDocument(schema, document)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(declared), "ns/field")
		})
	}
}

// gateNanoseconds is the undeclared check's budget (spec §9.1, D-22).
const gateNanoseconds = 2.0

// undeclaredCheckNanoseconds measures checkDocument on a bucket that declares
// nothing, against a 20-key document, with testing.Benchmark.
func undeclaredCheckNanoseconds(document map[string]interface{}) float64 {
	measured := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			checkSink = checkDocument(undeclaredBucket, document)
		}
	})
	return float64(measured.T.Nanoseconds()) / float64(measured.N)
}

// TestUndeclaredBucketPaysNothing is D-22: no allocation on the nil
// declaration, and at most 2 ns. The time half is asserted without -race: the
// detector instruments every call and a nil test then costs more than the
// budget by itself.
func TestUndeclaredBucketPaysNothing(t *testing.T) {
	document := benchDocument(t)
	if allocations := minAllocs(7, 1000, func() { checkSink = checkDocument(undeclaredBucket, document) }); allocations != 0 {
		t.Errorf("checking an undeclared bucket allocated %v times per call, want 0", allocations)
	}
	if raceEnabled {
		t.Skip("the 2 ns budget is asserted without -race: the detector's instrumentation outweighs a nil test")
	}
	if nanoseconds := undeclaredCheckNanoseconds(document); nanoseconds > gateNanoseconds {
		t.Errorf("the undeclared check took %.2f ns/op, want at most %v", nanoseconds, gateNanoseconds)
	}
}

// TestNameScanIsOutsideTheUndeclaredGate (deferred from TF-2): the E-18 scan
// of every top-level key runs on every bucket, and it is a separate function
// outside the 2 ns gate. On a 20-key document the undeclared check stays at
// the gate while the scan, measured separately, costs far more.
func TestNameScanIsOutsideTheUndeclaredGate(t *testing.T) {
	if raceEnabled {
		t.Skip("both costs are asserted without -race")
	}
	document := benchDocument(t)
	if len(document) != 20 {
		t.Fatalf("the document has %d keys, want 20", len(document))
	}
	gate := undeclaredCheckNanoseconds(document)
	scan := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			if err := refuseUnreachableNames(document, "app", "bench"); err != nil {
				b.Fatal(err)
			}
		}
	})
	scanNanoseconds := float64(scan.T.Nanoseconds()) / float64(scan.N)
	t.Logf("20-key document: undeclared check %.2f ns, name scan %.0f ns (%.1f ns a key)", gate, scanNanoseconds, scanNanoseconds/20)
	if gate > gateNanoseconds {
		t.Errorf("the undeclared check took %.2f ns on a 20-key document, want at most %v: the name scan must not run inside it", gate, gateNanoseconds)
	}
	if scanNanoseconds < 50*gate {
		t.Errorf("the name scan took %.0f ns against the gate's %.2f ns: the two are not separate costs", scanNanoseconds, gate)
	}
}

// TestPushCreatedArrayIsCheckedOnTheFastPath: $push and $addToSet create a
// missing array as []interface{} (spec §4.5.4), so an update's result holds
// both array types, and the check of either allocates nothing.
func TestPushCreatedArrayIsCheckedOnTheFastPath(t *testing.T) {
	declared := schemaOf(t, "Tags array")
	for name, array := range map[string]interface{}{
		"[]interface{}": []interface{}{"a", "b"},
		"primitive.A":   primitive.A{"a", "b"},
	} {
		document := map[string]interface{}{"Tags": array}
		if mismatch := checkDocument(declared, document); mismatch != nil {
			t.Errorf("%s under a bare array declaration was refused: %v", name, mismatch)
		}
		if allocations := minAllocs(7, 1000, func() { checkSink = checkDocument(declared, document) }); allocations != 0 {
			t.Errorf("checking a %s allocated %v times per call, want 0 (it took the slow path)", name, allocations)
		}
	}
}

// TestAnAcceptedDocumentAllocatesNothing: all twenty declared fields of a
// wire-decoded document pass without an allocation (spec §4.5.4).
func TestAnAcceptedDocumentAllocatesNothing(t *testing.T) {
	document, declared := benchDocument(t), benchSchema(t, len(benchFields))
	if mismatch := checkDocument(declared, document); mismatch != nil {
		t.Fatalf("the benchmark document is refused by its own declaration: %v", mismatch)
	}
	if allocations := minAllocs(7, 1000, func() { checkSink = checkDocument(declared, document) }); allocations != 0 {
		t.Errorf("an accepted document allocated %v times per call, want 0", allocations)
	}
}
