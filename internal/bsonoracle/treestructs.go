package bsonoracle

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	libbson "github.com/hoarddb/hoarddb-bson/bson"
	libprimitive "github.com/hoarddb/hoarddb-bson/bson/primitive"
	"github.com/hoarddb/hoarddb-bson/oracle/compare"
	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// The tree-struct differentials of bson-codec-spec 9.1 rows 1 and 2 (build
// doc step 6a): every wire and storage struct of the tree goes through the
// library and through the vendor, and the bytes (encode) and decoded values
// (decode) must agree. They were built only with a bsondiff tag until step 6e,
// because before the swap a struct's vendor-typed fields are not types the
// library encodes the way the vendor does; after the swap they run in every
// build.
//
// This file is not a _test.go file so that each package's own
// bsondiff_*_test.go can reach its unexported record types and share one
// harness. Only test files import this package (TestNoProductionPackageImportsTheShim).

// libraryMarshal and libraryUnmarshal are the library's side of the
// comparison. They are variables only so the canaries in treestructs_test.go
// can put a wrong "library" in and watch the comparison fail.
var (
	libraryMarshal   = libbson.Marshal
	libraryUnmarshal = libbson.Unmarshal
)

// TreeStruct is one row: a struct type of the tree, given by a zero value of
// it, and the name the failure messages use.
type TreeStruct struct {
	Name string
	Zero any
}

// fillLevel says how TreeStructValues fills a value.
type fillLevel int

const (
	fillZero    fillLevel = iota // the zero value, which exercises omitempty
	fillSmall                    // every field set, integers that fit an int32
	fillLarge                    // integers past int32, which changes the BSON width of an int
	fillExtreme                  // each kind's limit: a uint64 past int64 must fail in both libraries
)

func (level fillLevel) String() string {
	return [...]string{"zero", "small", "large", "extreme"}[level]
}

// maximumFillDepth stops a recursive type (a struct reachable from itself
// through a pointer, slice or map) from being filled forever.
const maximumFillDepth = 4

// TreeStructValues returns the row's struct filled at every level, as a
// value of the struct type (not a pointer), named by level.
func TreeStructValues(row TreeStruct) map[string]any {
	structType := reflect.TypeOf(row.Zero)
	values := map[string]any{}
	for _, level := range []fillLevel{fillZero, fillSmall, fillLarge, fillExtreme} {
		filled := reflect.New(structType).Elem()
		if level != fillZero {
			fillValue(filled, level, 0)
		}
		values[level.String()] = filled.Interface()
	}
	return values
}

var (
	timeType       = reflect.TypeOf(time.Time{})
	objectIDType   = reflect.TypeOf(libprimitive.ObjectID{})
	dateTimeType   = reflect.TypeOf(libprimitive.DateTime(0))
	decimal128Type = reflect.TypeOf(libprimitive.Decimal128{})
	rawType        = reflect.TypeOf(libbson.Raw(nil))
	rawValueType   = reflect.TypeOf(libbson.RawValue{})
	documentDType  = reflect.TypeOf(libbson.D(nil))
	documentMType  = reflect.TypeOf(libbson.M(nil))
	arrayAType     = reflect.TypeOf(libbson.A(nil))
)

// fillValue sets slot, which must be settable, to a deterministic non-zero
// value for level. Library scalar and container types get a value of their
// own BSON type, so the shim's codecs carry them on the vendor's side.
func fillValue(slot reflect.Value, level fillLevel, depth int) {
	// A library type is filled at any depth: a zero bson.Raw is not a
	// document, and both libraries refuse to write it.
	if special, ok := specialValue(slot.Type(), level); ok {
		slot.Set(special)
		return
	}
	if depth > maximumFillDepth {
		return
	}
	switch slot.Kind() {
	case reflect.Bool:
		slot.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		slot.SetInt(signedFor(slot.Type(), level))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		slot.SetUint(unsignedFor(slot.Type(), level))
	case reflect.Float32, reflect.Float64:
		if level == fillExtreme {
			slot.SetFloat(-math.MaxFloat32)
		} else {
			slot.SetFloat(1.5)
		}
	case reflect.String:
		if level == fillExtreme {
			slot.SetString("é <&>   \x01")
		} else {
			slot.SetString("text")
		}
	case reflect.Interface:
		if slot.NumMethod() == 0 {
			slot.Set(reflect.ValueOf("held in an interface"))
		}
	case reflect.Pointer:
		pointee := reflect.New(slot.Type().Elem())
		fillValue(pointee.Elem(), level, depth+1)
		slot.Set(pointee)
	case reflect.Slice:
		if slot.Type().Elem().Kind() == reflect.Uint8 {
			slot.SetBytes([]byte{1, 2, 3})
			return
		}
		elements := reflect.MakeSlice(slot.Type(), 2, 2)
		for position := range 2 {
			fillValue(elements.Index(position), level, depth+1)
		}
		slot.Set(elements)
	case reflect.Array:
		for position := range slot.Len() {
			fillValue(slot.Index(position), level, depth+1)
		}
	case reflect.Map:
		fillMap(slot, level, depth)
	case reflect.Struct:
		for fieldIndex := range slot.NumField() {
			if field := slot.Field(fieldIndex); field.CanSet() {
				fillValue(field, level, depth+1)
			}
		}
	}
}

// fillMap gives a map two members, under keys the vendor can encode: a
// string kind, or an integer kind it writes in decimal.
func fillMap(slot reflect.Value, level fillLevel, depth int) {
	keyType := slot.Type().Key()
	members := reflect.MakeMap(slot.Type())
	for position := range 2 {
		key := reflect.New(keyType).Elem()
		switch keyType.Kind() {
		case reflect.String:
			key.SetString(fmt.Sprintf("key%d", position))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			key.SetInt(int64(position + 1))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			key.SetUint(uint64(position + 1))
		default:
			return
		}
		member := reflect.New(slot.Type().Elem()).Elem()
		fillValue(member, level, depth+1)
		members.SetMapIndex(key, member)
	}
	slot.Set(members)
}

// specialValue is the value of a type whose zero-free fill by kind would be
// wrong: a time with sub-millisecond parts (BSON keeps milliseconds), raw
// bytes that are not a document, a struct of unexported fields.
func specialValue(valueType reflect.Type, level fillLevel) (reflect.Value, bool) {
	instant := time.UnixMilli(1_761_000_000_123).UTC()
	if level == fillExtreme {
		instant = time.UnixMilli(-62_135_596_800_000).UTC()
	}
	var special any
	switch valueType {
	case timeType:
		special = instant
	case objectIDType:
		special = libprimitive.ObjectID{0x65, 0x1f, 0, 1, 2, 3, 4, 5, 6, 7, 8, byte(level)}
	case dateTimeType:
		special = libprimitive.NewDateTimeFromTime(instant)
	case decimal128Type:
		special = libprimitive.NewDecimal128(0x3040000000000000, 15)
	case rawType:
		special = libbson.Raw(vendorbsoncore.BuildDocumentFromElements(nil, vendorbsoncore.AppendInt32Element(nil, "n", 7)))
	case rawValueType:
		special = libbson.RawValue{Type: libbson.TypeString, Value: vendorbsoncore.AppendString(nil, "raw value")}
	case documentDType:
		special = libbson.D{{Key: "first", Value: int32(1)}, {Key: "second", Value: "two"}}
	case documentMType:
		special = libbson.M{"first": int32(1), "second": "two"}
	case arrayAType:
		special = libbson.A{int32(1), "two"}
	default:
		return reflect.Value{}, false
	}
	return reflect.ValueOf(special).Convert(valueType), true
}

func signedFor(valueType reflect.Type, level fillLevel) int64 {
	bits := valueType.Bits()
	switch level {
	case fillSmall:
		return 7
	case fillLarge:
		if bits == 64 {
			return 1 << 40
		}
		return -(1 << (bits - 1))
	}
	return 1<<(bits-1) - 1
}

func unsignedFor(valueType reflect.Type, level fillLevel) uint64 {
	bits := valueType.Bits()
	switch level {
	case fillSmall:
		return 7
	case fillLarge:
		if bits == 64 {
			return 1 << 40
		}
		return 1<<bits - 1
	}
	return math.MaxUint64 >> (64 - bits)
}

// EncodeLikeTheVendor marshals every fill of every row with the library and
// with the vendor (through the shim), and fails tb unless both refuse it or
// both accept it with the same bytes (maps under spec 6.1's rule).
func EncodeLikeTheVendor(tb testing.TB, rows []TreeStruct) {
	tb.Helper()
	for _, row := range rows {
		compared := 0
		for level, filled := range TreeStructValues(row) {
			for _, input := range []any{filled, pointerTo(filled)} {
				libraryBytes, libraryErr := libraryMarshal(input)
				vendorBytes, vendorErr := Marshal(input)
				if (libraryErr == nil) != (vendorErr == nil) {
					tb.Errorf("%s (%s, %T): library error %v, vendor error %v", row.Name, level, input, libraryErr, vendorErr)
					continue
				}
				if libraryErr != nil {
					continue
				}
				compare.Encoded(&namedFailures{TB: tb, prefix: row.Name + " (" + level + ")"}, input, vendorBytes, libraryBytes)
				compared++
			}
		}
		if compared == 0 {
			tb.Errorf("%s: no fill encoded in either library, so nothing was compared", row.Name)
		}
	}
}

// DecodeLikeTheVendor decodes, into a new value of each row's type, the
// vendor's bytes of every fill, those bytes cut short, and those bytes with
// every top-level element retyped as a string, with the library and with the
// vendor. Both must refuse, or both accept with the same value.
func DecodeLikeTheVendor(tb testing.TB, rows []TreeStruct) {
	tb.Helper()
	for _, row := range rows {
		structType := reflect.TypeOf(row.Zero)
		compared := 0
		for level, filled := range TreeStructValues(row) {
			vendorBytes, err := Marshal(filled)
			if err != nil {
				continue
			}
			inputs := map[string][]byte{
				"well formed":      vendorBytes,
				"cut short":        vendorBytes[:len(vendorBytes)-1],
				"every key string": everyElementAsString(tb, vendorBytes),
			}
			for inputName, input := range inputs {
				if decodeBoth(tb, row.Name+" ("+level+", "+inputName+")", structType, input) {
					compared++
				}
			}
		}
		if compared == 0 {
			tb.Errorf("%s: no input decoded in either library, so no value was compared", row.Name)
		}
	}
}

// decodeBoth reports whether both libraries decoded the input, so that the
// values were compared.
func decodeBoth(tb testing.TB, name string, structType reflect.Type, input []byte) bool {
	tb.Helper()
	libraryTarget := reflect.New(structType)
	vendorTarget := reflect.New(structType)
	libraryErr := libraryUnmarshal(input, libraryTarget.Interface())
	vendorErr := Unmarshal(input, vendorTarget.Interface())
	if (libraryErr == nil) != (vendorErr == nil) {
		tb.Errorf("%s: library error %v, vendor error %v", name, libraryErr, vendorErr)
		return false
	}
	if libraryErr != nil {
		return false
	}
	ConvertTarget(vendorTarget.Interface())
	compare.Decoded(&namedFailures{TB: tb, prefix: name}, vendorTarget.Elem().Interface(), libraryTarget.Elem().Interface())
	return true
}

// everyElementAsString keeps a document's keys and replaces every value with
// a string, which is a type mismatch for every field that is not a string.
func everyElementAsString(tb testing.TB, documentBytes []byte) []byte {
	tb.Helper()
	elements, err := vendorbsoncore.Document(documentBytes).Elements()
	if err != nil {
		tb.Fatalf("reading the vendor's own bytes: %v", err)
	}
	var retyped []byte
	for _, element := range elements {
		retyped = vendorbsoncore.AppendStringElement(retyped, element.Key(), "retyped")
	}
	return vendorbsoncore.BuildDocumentFromElements(nil, retyped)
}

func pointerTo(value any) any {
	pointer := reflect.New(reflect.TypeOf(value))
	pointer.Elem().Set(reflect.ValueOf(value))
	return pointer.Interface()
}

// namedFailures prefixes compare's reports with the row and fill they came
// from.
type namedFailures struct {
	testing.TB
	prefix string
}

func (failures *namedFailures) Errorf(format string, args ...any) {
	failures.Helper()
	failures.TB.Errorf("%s: %s", failures.prefix, fmt.Sprintf(format, args...))
}
