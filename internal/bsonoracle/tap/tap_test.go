//go:build bsonoracle

package tap

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// tapAddress and tapDocument are library-typed values that go through the
// library's entry points with the real tap installed (init installs it).
type tapAddress struct {
	Street string `bson:"street"`
	City   string `bson:"city"`
}

type tapDocument struct {
	ID       primitive.ObjectID   `bson:"_id"`
	Name     string               `bson:"name"`
	Age      int32                `bson:"age"`
	Score    float64              `bson:"score"`
	Created  primitive.DateTime   `bson:"created"`
	Tags     []string             `bson:"tags"`
	Address  tapAddress           `bson:"address"`
	Extra    any                  `bson:"extra"`
	Ordered  bson.D               `bson:"ordered"`
	Loose    map[string]any       `bson:"loose"`
	Price    primitive.Decimal128 `bson:"price"`
	Optional *int64               `bson:"optional,omitempty"`
}

func sampleDocument() tapDocument {
	return tapDocument{
		ID: primitive.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, Name: "Ada", Age: 36, Score: 97.25,
		Created: primitive.NewDateTimeFromTime(time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)),
		Tags:    []string{"a", "b"}, Address: tapAddress{Street: "12 St James's Square", City: "London"},
		Extra:   bson.D{{Key: "inner", Value: bson.A{int32(1), "two", primitive.Timestamp{T: 3, I: 4}}}},
		Ordered: bson.D{{Key: "z", Value: int64(1)}, {Key: "a", Value: primitive.Symbol("s")}},
		Loose:   map[string]any{"x": int32(7), "y": bson.D{{Key: "n", Value: nil}}, "nan": math.NaN()},
		Price:   primitive.NewDecimal128(0x3040000000000000, 99),
	}
}

func requireCleanTap(t *testing.T, callsBefore int) {
	t.Helper()
	if divergences := Divergences(); len(divergences) != 0 {
		t.Fatalf("the tap reported %d divergences; first: %s", len(divergences), divergences[0])
	}
	if Calls() <= callsBefore {
		t.Fatalf("the tap compared no call (%d before, %d after): it is not installed", callsBefore, Calls())
	}
}

// TestTapAgreesWhenTheLibraryAgrees puts values through every entry point with
// the tap installed. The library and the vendor agree on them, so no call
// errors and no divergence is recorded. A tap that cried wolf would fail here
// before it failed anywhere else.
func TestTapAgreesWhenTheLibraryAgrees(t *testing.T) {
	Reset()
	callsBefore := Calls()
	sample := sampleDocument()

	encoded, err := bson.Marshal(sample)
	if err != nil {
		t.Fatalf("Marshal of the sample struct: %v", err)
	}
	for _, document := range []any{
		bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: bson.D{{Key: "c", Value: primitive.MaxKey{}}}}},
		bson.M{"only": "one"}, map[string]any{"k": []any{int64(1), 2.5, nil}}, (map[string]any)(nil),
	} {
		if _, err := bson.Marshal(document); err != nil {
			t.Fatalf("Marshal of %T: %v", document, err)
		}
	}
	for _, value := range []any{int32(1), "text", primitive.ObjectID{1}, bson.A{int32(1), "x"}, bson.D{{Key: "k", Value: true}}, (*int32)(nil), (map[string]any)(nil), ([]string)(nil),
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), []byte{1, 2, 3}, sample} {
		if _, _, err := bson.MarshalValue(value); err != nil {
			t.Fatalf("MarshalValue of %T: %v", value, err)
		}
	}

	var intoStruct tapDocument
	if err := bson.Unmarshal(encoded, &intoStruct); err != nil {
		t.Fatalf("Unmarshal into the struct: %v", err)
	}
	var intoD bson.D
	if err := bson.Unmarshal(encoded, &intoD); err != nil {
		t.Fatalf("Unmarshal into D: %v", err)
	}
	var intoM bson.M
	if err := bson.Unmarshal(encoded, &intoM); err != nil {
		t.Fatalf("Unmarshal into M: %v", err)
	}
	var intoAny any
	if err := bson.Unmarshal(encoded, &intoAny); err != nil {
		t.Fatalf("Unmarshal into any: %v", err)
	}
	intoMap := map[string]any{}
	if err := bson.Unmarshal(encoded, intoMap); err != nil {
		t.Fatalf("Unmarshal into a map: %v", err)
	}
	var intoRaw bson.Raw
	if err := bson.Unmarshal(encoded, &intoRaw); err != nil {
		t.Fatalf("Unmarshal into Raw: %v", err)
	}

	scalar := bson.RawValue{Type: bsontype.Int32, Value: []byte{7, 0, 0, 0}}
	var seven int32
	if err := scalar.Unmarshal(&seven); err != nil {
		t.Fatalf("RawValue.Unmarshal into int32: %v", err)
	}
	var sevenAny any
	if err := scalar.Unmarshal(&sevenAny); err != nil {
		t.Fatalf("RawValue.Unmarshal into any: %v", err)
	}
	embedded := bson.RawValue{Type: bsontype.EmbeddedDocument, Value: encoded}
	var embeddedD bson.D
	if err := embedded.Unmarshal(&embeddedD); err != nil {
		t.Fatalf("RawValue.Unmarshal into D: %v", err)
	}

	stored, err := bson.TrustStored(encoded)
	if err != nil {
		t.Fatalf("TrustStored: %v", err)
	}
	var decoded tapDocument
	if err := stored.Decode(&decoded); err != nil {
		t.Fatalf("Document.Decode: %v", err)
	}
	for _, canonical := range []bool{true, false} {
		text, err := bson.MarshalExtJSON(sample, canonical, true)
		if err != nil {
			t.Fatalf("MarshalExtJSON (canonical %v): %v", canonical, err)
		}
		var fromText tapDocument
		if err := bson.UnmarshalExtJSON(text, canonical, &fromText); err != nil {
			t.Fatalf("UnmarshalExtJSON (canonical %v): %v", canonical, err)
		}
		var fromTextAny any
		if err := bson.UnmarshalExtJSON(text, canonical, &fromTextAny); err != nil {
			t.Fatalf("UnmarshalExtJSON into any (canonical %v): %v", canonical, err)
		}
	}
	requireCleanTap(t, callsBefore)
	t.Logf("%d calls compared with the vendor, no divergence", Calls()-callsBefore)
}

// TestTapFailsOnDivergence is the build doc's 4f row: with a stand-in that
// makes the library's result differ by one byte, the tap fails the call that
// made it, and the same call without the stand-in passes.
func TestTapFailsOnDivergence(t *testing.T) {
	sample := sampleDocument()
	encoded, err := bson.Marshal(sample)
	if err != nil {
		t.Fatalf("Marshal of the sample struct: %v", err)
	}

	corruptBytes := func(original []byte) []byte {
		corrupted := append([]byte{}, original...)
		corrupted[len(corrupted)-2] ^= 0x01
		return corrupted
	}
	corruptingOneByte := func(corrupt func(after bson.OracleEvent) bson.OracleEvent) bson.OracleTap {
		return func(before bson.OracleEvent) func(bson.OracleEvent) error {
			realAfter := Observe(before)
			return func(after bson.OracleEvent) error { return realAfter(corrupt(after)) }
		}
	}
	install := func(tap bson.OracleTap) {
		t.Helper()
		Reset()
		bson.SetOracleTap(tap)
		t.Cleanup(func() { bson.SetOracleTap(Observe) })
	}

	t.Run("Marshal", func(t *testing.T) {
		install(corruptingOneByte(func(after bson.OracleEvent) bson.OracleEvent {
			after.Bytes = corruptBytes(after.Bytes)
			return after
		}))
		if _, err := bson.Marshal(sample); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("Marshal with a one-byte difference returned %v, want a divergence error", err)
		}
		if len(Divergences()) == 0 {
			t.Error("the divergence was returned but not recorded")
		}
		install(Observe)
		if _, err := bson.Marshal(sample); err != nil {
			t.Fatalf("the same Marshal with the real tap: %v", err)
		}
	})

	t.Run("MarshalValue", func(t *testing.T) {
		install(corruptingOneByte(func(after bson.OracleEvent) bson.OracleEvent {
			after.Bytes = corruptBytes(after.Bytes)
			return after
		}))
		if _, _, err := bson.MarshalValue("a string"); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("MarshalValue with a one-byte difference returned %v, want a divergence error", err)
		}
	})

	t.Run("Unmarshal", func(t *testing.T) {
		install(corruptingOneByte(func(after bson.OracleEvent) bson.OracleEvent {
			after.Value.(*tapDocument).Age++
			return after
		}))
		var decoded tapDocument
		if err := bson.Unmarshal(encoded, &decoded); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("Unmarshal with a changed field returned %v, want a divergence error", err)
		}
		install(Observe)
		var again tapDocument
		if err := bson.Unmarshal(encoded, &again); err != nil {
			t.Fatalf("the same Unmarshal with the real tap: %v", err)
		}
	})

	t.Run("RawValue.Unmarshal and Document.Decode", func(t *testing.T) {
		install(corruptingOneByte(func(after bson.OracleEvent) bson.OracleEvent {
			switch target := after.Value.(type) {
			case *int32:
				*target++
			case *tapDocument:
				target.Name += "!"
			}
			return after
		}))
		var seven int32
		scalar := bson.RawValue{Type: bsontype.Int32, Value: []byte{7, 0, 0, 0}}
		if err := scalar.Unmarshal(&seven); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("RawValue.Unmarshal with a changed target returned %v, want a divergence error", err)
		}
		stored, err := bson.TrustStored(encoded)
		if err != nil {
			t.Fatal(err)
		}
		var decoded tapDocument
		if err := stored.Decode(&decoded); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("Document.Decode with a changed target returned %v, want a divergence error", err)
		}
	})

	t.Run("MarshalExtJSON and UnmarshalExtJSON", func(t *testing.T) {
		extJSONSample := bson.D{{Key: "n", Value: int32(7)}, {Key: "s", Value: "text"}, {Key: "when", Value: primitive.DateTime(1_700_000_000_000)}}
		wrongText := func(after bson.OracleEvent) bson.OracleEvent {
			after.Bytes = []byte(strings.Replace(string(after.Bytes), `"text"`, `"txet"`, 1))
			return after
		}
		install(corruptingOneByte(wrongText))
		if _, err := bson.MarshalExtJSON(extJSONSample, true, false); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("MarshalExtJSON with a wrong text returned %v, want a divergence error", err)
		}

		install(Observe)
		text, err := bson.MarshalExtJSON(extJSONSample, true, false)
		if err != nil {
			t.Fatalf("the same MarshalExtJSON with the real tap: %v", err)
		}
		if len(Divergences()) != 0 || Calls() == 0 {
			t.Fatalf("the real tap reported %v over %d calls; want no divergence from at least one compared call", Divergences(), Calls())
		}

		install(corruptingOneByte(func(after bson.OracleEvent) bson.OracleEvent {
			after.Value.(*tapDocument).Age++
			return after
		}))
		var decoded tapDocument
		if err := bson.UnmarshalExtJSON([]byte(`{"name":"Ada","age":{"$numberInt":"36"}}`), true, &decoded); err == nil || !strings.Contains(err.Error(), "diverges from the vendor") {
			t.Fatalf("UnmarshalExtJSON with a changed target returned %v, want a divergence error", err)
		}
		install(Observe)
		var again bson.D
		if err := bson.UnmarshalExtJSON(text, true, &again); err != nil {
			t.Fatalf("the same UnmarshalExtJSON with the real tap: %v", err)
		}
		if len(Divergences()) != 0 {
			t.Errorf("the real tap reported %v", Divergences())
		}
	})

	t.Run("a refusal the vendor does not share", func(t *testing.T) {
		install(Observe)
		overlong := []byte{0x10, 0, 0, 0, 0x02, 'a', 0, 0xFF, 0xFF, 0xFF, 0x7F, 'x', 0, 0}
		var decoded bson.D
		if err := bson.Unmarshal(overlong, &decoded); err == nil {
			t.Fatal("Unmarshal accepted a string longer than its document")
		}
		if len(Divergences()) != 0 {
			t.Errorf("both libraries refuse the bytes, yet the tap reported: %v", Divergences())
		}
	})
}

// TestTapRecordsTheGoTypeAndTheBytes checks $BSONORACLE_OUT: one line per
// distinct (function, Go type, bytes), the type spelled as Go spells it.
func TestTapRecordsTheGoTypeAndTheBytes(t *testing.T) {
	outputDirectory := filepath.Join(t.TempDir(), "records")
	t.Setenv(outputVariable, outputDirectory)
	Reset()

	document := bson.D{{Key: "recorded", Value: int32(42)}}
	// One call site for the repeated Marshal: the record key includes the site.
	marshalRecorded := func() []byte {
		encoded, err := bson.Marshal(document)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		return encoded
	}
	for range 3 {
		marshalRecorded()
	}
	var decoded bson.D
	encoded := marshalRecorded()
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(outputDirectory, "*.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("record files in %s: %v, %v; want exactly one", outputDirectory, files, err)
	}
	opened, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Close(); err != nil {
			t.Errorf("closing %s: %v", files[0], err)
		}
	})
	var pairs []recordedPair
	for scanner := bufio.NewScanner(opened); scanner.Scan(); {
		var pair recordedPair
		if err := json.Unmarshal(scanner.Bytes(), &pair); err != nil {
			t.Fatalf("a record line is not JSON: %v", err)
		}
		pairs = append(pairs, pair)
	}
	if len(pairs) != 2 {
		t.Fatalf("%d records, want 2 (one Marshal of primitive.D, one Unmarshal into primitive.D): %+v", len(pairs), pairs)
	}
	for _, pair := range pairs {
		if !strings.HasPrefix(pair.Site, "internal/bsonoracle/tap/tap_test.go:") {
			t.Errorf("record %+v: want the call site in this test file, not a frame of the library or the tap", pair)
		}
		if pair.GoType != "primitive.D" || len(pair.Bytes) == 0 {
			t.Errorf("record %+v: want Go type primitive.D and the document's bytes", pair)
		}
	}
	if pairs[0].Function != "Marshal" || pairs[1].Function != "Unmarshal" {
		t.Errorf("records are for %s and %s, want Marshal and Unmarshal", pairs[0].Function, pairs[1].Function)
	}
}

// TestTapLogsADivergenceWithItsCallSite checks the divergence log: a call the
// tap fails leaves one line in divergences.jsonl that names the call site.
func TestTapLogsADivergenceWithItsCallSite(t *testing.T) {
	outputDirectory := filepath.Join(t.TempDir(), "records")
	t.Setenv(outputVariable, outputDirectory)
	Reset()
	bson.SetOracleTap(func(before bson.OracleEvent) func(bson.OracleEvent) error {
		realAfter := Observe(before)
		return func(after bson.OracleEvent) error {
			after.Bytes = append([]byte{}, after.Bytes...)
			after.Bytes[len(after.Bytes)-2] ^= 0x01
			return realAfter(after)
		}
	})
	t.Cleanup(func() { bson.SetOracleTap(Observe) })

	if _, err := bson.Marshal(bson.D{{Key: "logged", Value: "yes"}}); err == nil {
		t.Fatal("Marshal with a corrupted result did not fail")
	}
	logged, err := os.ReadFile(filepath.Join(outputDirectory, "divergences.jsonl"))
	if err != nil {
		t.Fatalf("reading the divergence log: %v", err)
	}
	var entry struct {
		Site       string `json:"site"`
		Function   string `json:"function"`
		Divergence string `json:"divergence"`
	}
	if err := json.Unmarshal(logged, &entry); err != nil {
		t.Fatalf("the divergence log line is not JSON: %v\n%s", err, logged)
	}
	if entry.Function != "Marshal" || !strings.HasPrefix(entry.Site, "internal/bsonoracle/tap/tap_test.go:") || !strings.Contains(entry.Divergence, "diverges from the vendor") {
		t.Errorf("divergence log entry %+v: want Marshal, a call site in this file, and the divergence text", entry)
	}
}
