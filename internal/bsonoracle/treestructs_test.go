package bsonoracle

import (
	"reflect"
	"strings"
	"testing"
	"time"

	libbson "github.com/hoarddb/hoarddb-bson/bson"
	libprimitive "github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// everyLibraryType holds each library type the fill knows, at the top level,
// behind a pointer, in a slice and in a map, so the harness's own fills are
// shown to go through both libraries before any package's rows rely on them.
type everyLibraryType struct {
	ID         libprimitive.ObjectID              `bson:"_id"`
	Created    libprimitive.DateTime              `bson:"created"`
	When       time.Time                          `bson:"when"`
	Amount     libprimitive.Decimal128            `bson:"amount"`
	Document   libbson.Raw                        `bson:"document,omitempty"`
	Element    libbson.RawValue                   `bson:"element,omitempty"`
	Ordered    libbson.D                          `bson:"ordered"`
	Unordered  libbson.M                          `bson:"unordered"`
	List       libbson.A                          `bson:"list"`
	Anything   interface{}                        `bson:"anything"`
	Pointer    *libprimitive.ObjectID             `bson:"pointer,omitempty"`
	IDs        []libprimitive.ObjectID            `bson:"ids"`
	ByName     map[string]libprimitive.DateTime   `bson:"by_name"`
	Width      int                                `bson:"width"`
	Unsigned   uint64                             `bson:"unsigned"`
	Small      uint8                              `bson:"small"`
	Ratio      float64                            `bson:"ratio"`
	Bytes      []byte                             `bson:"bytes"`
	Nested     struct{ Inner []string }           `bson:"nested"`
	Inline     inlineMembers                      `bson:",inline"`
	Positions  map[int64]string                   `bson:"positions"`
	Recursive  *everyLibraryType                  `bson:"recursive,omitempty"`
	Fixed      [2]int32                           `bson:"fixed"`
	Interfaces map[string]interface{}             `bson:"interfaces"`
	Documents  []libbson.D                        `bson:"documents"`
	Raws       map[string]libbson.Raw             `bson:"raws"`
	Times      []time.Time                        `bson:"times"`
	Pointers   []*libprimitive.Decimal128         `bson:"pointers"`
	Empty      struct{}                           `bson:"empty"`
	Omitted    string                             `bson:"omitted,omitempty"`
	Skipped    string                             `bson:"-"`
	unexported string                             //nolint:unused // shows an unexported field is left alone
	Mixed      map[string][]libprimitive.ObjectID `bson:"mixed"`
}

type inlineMembers struct {
	Region string `bson:"region"`
}

var harnessRows = []TreeStruct{
	{Name: "everyLibraryType", Zero: everyLibraryType{}},
	{Name: "inlineMembers", Zero: inlineMembers{}},
}

// TestTreeStructsEncodeLikeTheVendor runs the harness over its own rows; each
// package's bsondiff_treestructs_test.go runs it over that package's structs.
func TestTreeStructsEncodeLikeTheVendor(t *testing.T) {
	EncodeLikeTheVendor(t, harnessRows)
}

func TestTreeStructsDecodeLikeTheVendor(t *testing.T) {
	DecodeLikeTheVendor(t, harnessRows)
}

// canaryShape is the row the canaries put a wrong "library" behind.
type canaryShape struct {
	Count int    `bson:"count"`
	Name  string `bson:"name"`
}

var canaryRows = []TreeStruct{{Name: "canaryShape", Zero: canaryShape{}}}

// failureRecorder is a testing.TB that keeps what the harness reports. Helper
// and Errorf are all the harness and compare call on it.
type failureRecorder struct {
	testing.TB
	failures []string
}

func (recorder *failureRecorder) Helper() {}

func (recorder *failureRecorder) Errorf(format string, args ...any) {
	recorder.failures = append(recorder.failures, format)
}

func withLibrary(t *testing.T, marshal func(any) ([]byte, error), unmarshal func([]byte, any) error) {
	t.Helper()
	previousMarshal, previousUnmarshal := libraryMarshal, libraryUnmarshal
	libraryMarshal, libraryUnmarshal = marshal, unmarshal
	t.Cleanup(func() { libraryMarshal, libraryUnmarshal = previousMarshal, previousUnmarshal })
}

func TestTreeStructEncodeCanaryCatchesAChangedIntWidth(t *testing.T) {
	wideningMarshal := func(input any) ([]byte, error) {
		shape := reflect.Indirect(reflect.ValueOf(input)).Interface().(canaryShape)
		return libbson.Marshal(libbson.D{{Key: "count", Value: int64(shape.Count)}, {Key: "name", Value: shape.Name}})
	}
	withLibrary(t, wideningMarshal, libbson.Unmarshal)
	recorder := &failureRecorder{}
	EncodeLikeTheVendor(recorder, canaryRows)
	if len(recorder.failures) == 0 {
		t.Fatal("a library that writes an int as int64 where the vendor writes int32 passed the encode differential")
	}
}

func TestTreeStructEncodeCanaryCatchesReorderedFields(t *testing.T) {
	reorderingMarshal := func(input any) ([]byte, error) {
		shape := reflect.Indirect(reflect.ValueOf(input)).Interface().(canaryShape)
		return libbson.Marshal(libbson.D{{Key: "name", Value: shape.Name}, {Key: "count", Value: int32(shape.Count)}})
	}
	withLibrary(t, reorderingMarshal, libbson.Unmarshal)
	recorder := &failureRecorder{}
	EncodeLikeTheVendor(recorder, canaryRows)
	if len(recorder.failures) == 0 {
		t.Fatal("a library that swaps two struct fields passed the encode differential")
	}
}

func TestTreeStructDecodeCanaryCatchesAChangedValue(t *testing.T) {
	alteringUnmarshal := func(input []byte, target any) error {
		if err := libbson.Unmarshal(input, target); err != nil {
			return err
		}
		if shape, ok := target.(*canaryShape); ok && shape.Name != "" {
			shape.Name += "!"
		}
		return nil
	}
	withLibrary(t, libbson.Marshal, alteringUnmarshal)
	recorder := &failureRecorder{}
	DecodeLikeTheVendor(recorder, canaryRows)
	if len(recorder.failures) == 0 {
		t.Fatal("a library that decodes a different string passed the decode differential")
	}
}

func TestTreeStructDecodeCanaryCatchesAnErrorMismatch(t *testing.T) {
	refused := 0
	permissiveUnmarshal := func(input []byte, target any) error {
		// The canary is a library that accepts what it should refuse.
		if err := libbson.Unmarshal(input, target); err != nil {
			refused++
		}
		return nil
	}
	withLibrary(t, libbson.Marshal, permissiveUnmarshal)
	recorder := &failureRecorder{}
	DecodeLikeTheVendor(recorder, canaryRows)
	if refused == 0 {
		t.Fatal("no input of the canary row was refused by the library, so the canary tests nothing")
	}
	if len(recorder.failures) == 0 {
		t.Fatal("a library that accepts the cut-short document the vendor refuses passed the decode differential")
	}
}

func TestTreeStructRowThatNeverComparesFails(t *testing.T) {
	recorder := &failureRecorder{}
	unencodable := []TreeStruct{{Name: "a channel field", Zero: struct{ Signal chan int }{}}}
	EncodeLikeTheVendor(recorder, unencodable)
	DecodeLikeTheVendor(recorder, unencodable)
	if len(recorder.failures) != 2 || !strings.Contains(recorder.failures[0], "nothing was compared") {
		t.Fatalf("a row neither library can encode reported %q, want one encode and one decode failure saying nothing was compared", recorder.failures)
	}
}
