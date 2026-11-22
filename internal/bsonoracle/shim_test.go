package bsonoracle

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	libbson "github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/oracle/compare"
	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorprimitive "go.mongodb.org/mongo-driver/bson/primitive"
)

// refAddress and refDocument are the build doc's ref450 shape (§4.0): ten
// top-level fields, 450 ± 8 bytes encoded. The library defines its copy in
// lib:oracle/internal/benchdata, which a module outside the library cannot
// import, so the shim tests keep their own.
type refAddress struct {
	Street  string `bson:"street"`
	City    string `bson:"city"`
	Zip     string `bson:"zip"`
	Country string `bson:"country"`
}

type refDocument struct {
	ID      vendorprimitive.ObjectID `bson:"_id"`
	Name    string                   `bson:"name"`
	Email   string                   `bson:"email"`
	Notes   string                   `bson:"notes"`
	Age     int32                    `bson:"age"`
	Score   float64                  `bson:"score"`
	Active  bool                     `bson:"active"`
	Created vendorprimitive.DateTime `bson:"created"`
	Tags    []string                 `bson:"tags"`
	Address refAddress               `bson:"address"`
}

func ref450Document() refDocument {
	return refDocument{
		ID:      vendorprimitive.ObjectID{0x65, 0x2f, 0x1c, 0x00, 0xaa, 0xbb, 0xcc, 0xdd, 0x01, 0x02, 0x03, 0x04},
		Name:    "Ada Lovelace",
		Email:   "ada.lovelace@example.com",
		Notes:   strings.Repeat("Prefers the analytical engine to the difference engine. ", 2) + "Keeps notes on Bernoulli numbers by hand.",
		Age:     36,
		Score:   97.25,
		Active:  true,
		Created: vendorprimitive.NewDateTimeFromTime(time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)),
		Tags:    []string{"mathematics", "engines", "poetry"},
		Address: refAddress{Street: "12 St James's Square", City: "London", Zip: "SW1Y 4JH", Country: "GB"},
	}
}

// shimCase is one Go value the shim must encode exactly as the vendor's
// default registry does, and whose vendor encoding it must decode exactly as
// the vendor does.
type shimCase struct {
	name    string
	goValue any
}

func shimCases(t *testing.T) []shimCase {
	t.Helper()
	ref450 := ref450Document()
	ref450Bytes, err := vendorbson.Marshal(ref450)
	if err != nil {
		t.Fatalf("encoding ref450 with the vendor: %v", err)
	}
	if len(ref450Bytes) < 442 || len(ref450Bytes) > 458 {
		t.Fatalf("ref450 encodes to %d bytes; the build doc (§4.0) sets 450 ± 8", len(ref450Bytes))
	}
	var ref450D vendorbson.D
	if err := vendorbson.Unmarshal(ref450Bytes, &ref450D); err != nil {
		t.Fatalf("decoding ref450 into bson.D with the vendor: %v", err)
	}
	cases := []shimCase{
		{name: "ref450/struct", goValue: ref450},
		{name: "ref450/bson.D", goValue: ref450D},
		{name: "map with nested map and slice", goValue: map[string]any{
			"alpha": int32(1), "beta": int64(2), "gamma": map[string]any{"x": "y", "z": []any{1.5, "two", nil}},
		}},
	}
	return append(cases, vendorCorpusCases(t)...)
}

// corpusFile is the part of a BSON corpus file the shim tests read.
type corpusFile struct {
	Valid []struct {
		Description   string `json:"description"`
		CanonicalBSON string `json:"canonical_bson"`
	} `json:"valid"`
}

// vendorCorpusCases decodes every valid canonical document of the pinned
// vendor module's BSON corpus into a bson.D with the vendor. Step 1d replaces
// this with the shared corpus loader.
func vendorCorpusCases(t *testing.T) []shimCase {
	t.Helper()
	listOutput, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "go.mongodb.org/mongo-driver").Output()
	if err != nil {
		t.Fatalf("locating the vendor module with go list: %v", err)
	}
	corpusDir := filepath.Join(strings.TrimSpace(string(listOutput)), "testdata", "bson-corpus")
	corpusPaths, err := filepath.Glob(filepath.Join(corpusDir, "*.json"))
	if err != nil {
		t.Fatalf("listing %s: %v", corpusDir, err)
	}
	if len(corpusPaths) == 0 {
		t.Fatalf("no corpus file in %s; the vendor module's testdata is missing", corpusDir)
	}
	var cases []shimCase
	for _, corpusPath := range corpusPaths {
		corpusJSON, err := os.ReadFile(corpusPath)
		if err != nil {
			t.Fatalf("reading %s: %v", corpusPath, err)
		}
		var parsedCorpus corpusFile
		if err := json.Unmarshal(corpusJSON, &parsedCorpus); err != nil {
			t.Fatalf("parsing %s: %v", corpusPath, err)
		}
		for _, validCase := range parsedCorpus.Valid {
			canonicalBytes, err := hex.DecodeString(validCase.CanonicalBSON)
			if err != nil {
				t.Fatalf("%s %q: canonical_bson is not hex: %v", corpusPath, validCase.Description, err)
			}
			var corpusDocument vendorbson.D
			if err := vendorbson.Unmarshal(canonicalBytes, &corpusDocument); err != nil {
				t.Fatalf("%s %q: the vendor refuses its own valid case: %v", corpusPath, validCase.Description, err)
			}
			cases = append(cases, shimCase{
				name:    filepath.Base(corpusPath) + "/" + validCase.Description,
				goValue: corpusDocument,
			})
		}
	}
	return cases
}

// decodeTargets returns the types every case's vendor encoding is decoded
// into: the case's own Go type, bson.D and bson.M.
func decodeTargets(goValue any) []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(goValue),
		reflect.TypeOf(vendorbson.D{}),
		reflect.TypeOf(vendorbson.M{}),
	}
}

// checkShimMatchesVendor reports to tb every case where the shim's Marshal or
// Unmarshal differs from the vendor's default registry. It reports through
// tb.Errorf only, so a recording fake can stand in for tb in the canary.
func checkShimMatchesVendor(tb testing.TB, cases []shimCase) {
	tb.Helper()
	for _, shimCase := range cases {
		vendorBytes, vendorErr := vendorbson.Marshal(shimCase.goValue)
		shimBytes, shimErr := Marshal(shimCase.goValue)
		if (vendorErr == nil) != (shimErr == nil) {
			tb.Errorf("%s: Marshal error differs: vendor %v, shim %v", shimCase.name, vendorErr, shimErr)
			continue
		}
		if vendorErr != nil {
			continue
		}
		compare.Encoded(tb, shimCase.goValue, vendorBytes, shimBytes)
		for _, targetType := range decodeTargets(shimCase.goValue) {
			vendorTarget := reflect.New(targetType)
			shimTarget := reflect.New(targetType)
			vendorErr := vendorbson.Unmarshal(vendorBytes, vendorTarget.Interface())
			shimErr := Unmarshal(vendorBytes, shimTarget.Interface())
			if (vendorErr == nil) != (shimErr == nil) {
				tb.Errorf("%s into %v: Unmarshal error differs: vendor %v, shim %v", shimCase.name, targetType, vendorErr, shimErr)
				continue
			}
			compareDecodedValues(tb, shimCase.name, vendorTarget.Elem().Interface(), shimTarget.Elem().Interface())
		}
	}
}

// compareDecodedValues is compare.Decoded, except for a value holding a NaN:
// compare.Decoded at oracle/v0.1.0 reports two NaNs as different (it compares
// floats with ==, as reflect.DeepEqual does), so such a value is re-encoded by
// the vendor on both sides and compared as bytes.
func compareDecodedValues(tb testing.TB, caseName string, vendorDecoded, shimDecoded any) {
	tb.Helper()
	if !holdsNaN(reflect.ValueOf(vendorDecoded)) {
		compare.Decoded(tb, vendorDecoded, shimDecoded)
		return
	}
	if reflect.TypeOf(vendorDecoded) != reflect.TypeOf(shimDecoded) {
		tb.Errorf("%s: decoded types differ: vendor %T, shim %T", caseName, vendorDecoded, shimDecoded)
		return
	}
	vendorBytes, vendorErr := vendorbson.Marshal(vendorDecoded)
	shimBytes, shimErr := vendorbson.Marshal(shimDecoded)
	if vendorErr != nil || shimErr != nil {
		tb.Errorf("%s: re-encoding the decoded values: vendor %v, shim %v", caseName, vendorErr, shimErr)
		return
	}
	compare.Encoded(tb, vendorDecoded, vendorBytes, shimBytes)
}

// holdsNaN reports whether a NaN float is reachable from goValue.
func holdsNaN(goValue reflect.Value) bool {
	switch goValue.Kind() {
	case reflect.Float32, reflect.Float64:
		return math.IsNaN(goValue.Float())
	case reflect.Interface, reflect.Pointer:
		return !goValue.IsNil() && holdsNaN(goValue.Elem())
	case reflect.Slice, reflect.Array:
		for position := range goValue.Len() {
			if holdsNaN(goValue.Index(position)) {
				return true
			}
		}
	case reflect.Map:
		for mapEntry := goValue.MapRange(); mapEntry.Next(); {
			if holdsNaN(mapEntry.Value()) {
				return true
			}
		}
	case reflect.Struct:
		for fieldIndex := range goValue.NumField() {
			if holdsNaN(goValue.Field(fieldIndex)) {
				return true
			}
		}
	}
	return false
}

func TestShimRoundTripsVendorValuesUnchanged(t *testing.T) {
	cases := shimCases(t)
	checkShimMatchesVendor(t, cases)
	t.Logf("%d cases compared (ref450, a map case and the vendor corpus)", len(cases))
}

// TestConvertKeepsTheBytesOfVendorValues pins Convert: whatever the vendor
// decoded, its library form encodes, through the shim, to the bytes the vendor
// gave the original.
func TestConvertKeepsTheBytesOfVendorValues(t *testing.T) {
	for _, shimCase := range shimCases(t) {
		vendorBytes, err := vendorbson.Marshal(shimCase.goValue)
		if err != nil {
			t.Fatalf("%s: vendor encoding: %v", shimCase.name, err)
		}
		convertedBytes, err := Marshal(Convert(shimCase.goValue))
		if err != nil {
			t.Fatalf("%s: shim encoding of the converted value: %v", shimCase.name, err)
		}
		compare.Encoded(t, shimCase.goValue, vendorBytes, convertedBytes)
	}
}

// recordingTB stands in for testing.TB in a canary: Errorf records the
// failure instead of failing the test, so "the check reported a failure" is
// something the canary asserts.
type recordingTB struct {
	testing.TB
	failures []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// TestShimRefusesToBeTheLibrary is trap 1's canary: if the shim's vendor side
// were the library, the round-trip check must say so.
func TestShimRefusesToBeTheLibrary(t *testing.T) {
	cases := shimCases(t)
	vendorEncoder := encodeDocument
	encodeDocument = libbson.Marshal
	t.Cleanup(func() { encodeDocument = vendorEncoder })

	recorder := &recordingTB{TB: t}
	checkShimMatchesVendor(recorder, cases)
	if len(recorder.failures) == 0 {
		t.Fatal("the shim encoded through a stand-in for the library and the round-trip check reported nothing; it would compare the library with itself")
	}
	t.Logf("the round-trip check reported %d failures with the library's encoder in the shim; first: %s", len(recorder.failures), recorder.failures[0])
}
