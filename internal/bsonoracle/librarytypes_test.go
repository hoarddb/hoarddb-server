package bsonoracle

import (
	"testing"

	libbson "github.com/hoarddb/hoarddb-bson/bson"
	libbsontype "github.com/hoarddb/hoarddb-bson/bson/bsontype"
	libprimitive "github.com/hoarddb/hoarddb-bson/bson/primitive"
	"github.com/hoarddb/hoarddb-bson/oracle/compare"
	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorprimitive "go.mongodb.org/mongo-driver/bson/primitive"
)

// libraryScalars and vendorScalars hold the same values field for field, one
// struct with the library's types and one with the vendor's. The shim must
// encode the first to the bytes the vendor gives the second, and decode those
// bytes back into the first.
type libraryScalars struct {
	ID        libprimitive.ObjectID      `bson:"id"`
	When      libprimitive.DateTime      `bson:"when"`
	Stamp     libprimitive.Timestamp     `bson:"stamp"`
	Blob      libprimitive.Binary        `bson:"blob"`
	Pattern   libprimitive.Regex         `bson:"pattern"`
	Pointer   libprimitive.DBPointer     `bson:"pointer"`
	Code      libprimitive.JavaScript    `bson:"code"`
	Name      libprimitive.Symbol        `bson:"name"`
	Scoped    libprimitive.CodeWithScope `bson:"scoped"`
	Price     libprimitive.Decimal128    `bson:"price"`
	Missing   libprimitive.Undefined     `bson:"missing"`
	Nothing   libprimitive.Null          `bson:"nothing"`
	Low       libprimitive.MinKey        `bson:"low"`
	High      libprimitive.MaxKey        `bson:"high"`
	Ordered   libbson.D                  `bson:"ordered"`
	Unordered libbson.M                  `bson:"unordered"`
	List      libbson.A                  `bson:"list"`
	Document  libbson.Raw                `bson:"document"`
	Value     libbson.RawValue           `bson:"value"`
	Pointered *libprimitive.ObjectID     `bson:"pointered"`
	Slotted   any                        `bson:"slotted"`
}

type vendorScalars struct {
	ID        vendorprimitive.ObjectID      `bson:"id"`
	When      vendorprimitive.DateTime      `bson:"when"`
	Stamp     vendorprimitive.Timestamp     `bson:"stamp"`
	Blob      vendorprimitive.Binary        `bson:"blob"`
	Pattern   vendorprimitive.Regex         `bson:"pattern"`
	Pointer   vendorprimitive.DBPointer     `bson:"pointer"`
	Code      vendorprimitive.JavaScript    `bson:"code"`
	Name      vendorprimitive.Symbol        `bson:"name"`
	Scoped    vendorprimitive.CodeWithScope `bson:"scoped"`
	Price     vendorprimitive.Decimal128    `bson:"price"`
	Missing   vendorprimitive.Undefined     `bson:"missing"`
	Nothing   vendorprimitive.Null          `bson:"nothing"`
	Low       vendorprimitive.MinKey        `bson:"low"`
	High      vendorprimitive.MaxKey        `bson:"high"`
	Ordered   vendorbson.D                  `bson:"ordered"`
	Unordered vendorbson.M                  `bson:"unordered"`
	List      vendorbson.A                  `bson:"list"`
	Document  vendorbson.Raw                `bson:"document"`
	Value     vendorbson.RawValue           `bson:"value"`
	Pointered *vendorprimitive.ObjectID     `bson:"pointered"`
	Slotted   any                           `bson:"slotted"`
}

func scalarPair(t *testing.T) (libraryScalars, vendorScalars) {
	t.Helper()
	identifier := [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	innerDocument, err := vendorbson.Marshal(vendorbson.D{{Key: "n", Value: int32(5)}})
	if err != nil {
		t.Fatalf("building the Raw: %v", err)
	}
	libraryID, vendorID := libprimitive.ObjectID(identifier), vendorprimitive.ObjectID(identifier)
	library := libraryScalars{
		ID: libraryID, When: 1_700_000_000_123, Stamp: libprimitive.Timestamp{T: 7, I: 9},
		Blob:    libprimitive.Binary{Subtype: 0x80, Data: []byte{1, 2, 3}},
		Pattern: libprimitive.Regex{Pattern: "^a", Options: "i"}, Pointer: libprimitive.DBPointer{DB: "ns", Pointer: libraryID},
		Code: "f()", Name: "sym",
		Scoped: libprimitive.CodeWithScope{Code: "g()", Scope: libbson.D{{Key: "k", Value: int32(1)}}},
		Price:  libprimitive.NewDecimal128(0x3040000000000000, 12345),
		Ordered: libbson.D{{Key: "z", Value: int32(1)}, {Key: "a", Value: libbson.A{"x", libprimitive.DateTime(5)}},
			{Key: "nested", Value: libbson.D{{Key: "id", Value: libraryID}}}},
		Unordered: libbson.M{"only": libprimitive.Symbol("s")},
		List:      libbson.A{int64(3), libprimitive.Null{}, libbson.D{{Key: "d", Value: libprimitive.MaxKey{}}}},
		Document:  libbson.Raw(innerDocument),
		Value:     libbson.RawValue{Type: libbsontype.EmbeddedDocument, Value: innerDocument},
		Pointered: &libraryID, Slotted: libprimitive.Timestamp{T: 1, I: 2},
	}
	vendor := vendorScalars{
		ID: vendorID, When: 1_700_000_000_123, Stamp: vendorprimitive.Timestamp{T: 7, I: 9},
		Blob:    vendorprimitive.Binary{Subtype: 0x80, Data: []byte{1, 2, 3}},
		Pattern: vendorprimitive.Regex{Pattern: "^a", Options: "i"}, Pointer: vendorprimitive.DBPointer{DB: "ns", Pointer: vendorID},
		Code: "f()", Name: "sym",
		Scoped: vendorprimitive.CodeWithScope{Code: "g()", Scope: vendorbson.D{{Key: "k", Value: int32(1)}}},
		Price:  vendorprimitive.NewDecimal128(0x3040000000000000, 12345),
		Ordered: vendorbson.D{{Key: "z", Value: int32(1)}, {Key: "a", Value: vendorbson.A{"x", vendorprimitive.DateTime(5)}},
			{Key: "nested", Value: vendorbson.D{{Key: "id", Value: vendorID}}}},
		Unordered: vendorbson.M{"only": vendorprimitive.Symbol("s")},
		List:      vendorbson.A{int64(3), vendorprimitive.Null{}, vendorbson.D{{Key: "d", Value: vendorprimitive.MaxKey{}}}},
		Document:  vendorbson.Raw(innerDocument),
		Value:     vendorbson.RawValue{Type: vendorbson.TypeEmbeddedDocument, Value: innerDocument},
		Pointered: &vendorID, Slotted: vendorprimitive.Timestamp{T: 1, I: 2},
	}
	return library, vendor
}

// TestShimEncodesLibraryScalarsAsTheVendorDoes is the build doc's 3c row: a
// struct with library scalar fields, marshalled through the shim's vendor
// registry, gives the bytes of the same struct with vendor scalar fields.
func TestShimEncodesLibraryScalarsAsTheVendorDoes(t *testing.T) {
	library, vendor := scalarPair(t)
	vendorBytes, err := vendorbson.Marshal(vendor)
	if err != nil {
		t.Fatalf("vendor encoding of the vendor struct: %v", err)
	}
	shimBytes, err := Marshal(library)
	if err != nil {
		t.Fatalf("shim encoding of the library struct: %v", err)
	}
	compare.Encoded(t, vendor, vendorBytes, shimBytes)

	// The same values at the top level and in a slot, which is where a
	// library type reaches the registry by its dynamic type.
	slotted := map[string]any{
		"objectid": library.ID, "datetime": library.When, "timestamp": library.Stamp, "binary": library.Blob,
		"regex": library.Pattern, "dbpointer": library.Pointer, "code": library.Code, "symbol": library.Name,
		"scope": library.Scoped, "decimal": library.Price, "undefined": library.Missing, "null": library.Nothing,
		"minkey": library.Low, "maxkey": library.High, "d": library.Ordered, "m": library.Unordered, "a": library.List,
		"raw": library.Document, "rawvalue": library.Value,
	}
	vendorSlotted := map[string]any{
		"objectid": vendor.ID, "datetime": vendor.When, "timestamp": vendor.Stamp, "binary": vendor.Blob,
		"regex": vendor.Pattern, "dbpointer": vendor.Pointer, "code": vendor.Code, "symbol": vendor.Name,
		"scope": vendor.Scoped, "decimal": vendor.Price, "undefined": vendor.Missing, "null": vendor.Nothing,
		"minkey": vendor.Low, "maxkey": vendor.High, "d": vendor.Ordered, "m": vendor.Unordered, "a": vendor.List,
		"raw": vendor.Document, "rawvalue": vendor.Value,
	}
	vendorSlottedBytes, err := vendorbson.Marshal(vendorSlotted)
	if err != nil {
		t.Fatalf("vendor encoding of the slots: %v", err)
	}
	shimSlottedBytes, err := Marshal(slotted)
	if err != nil {
		t.Fatalf("shim encoding of the slots: %v", err)
	}
	compare.Encoded(t, vendorSlotted, vendorSlottedBytes, shimSlottedBytes)
}

// TestShimDecodesLibraryScalarsAsTheVendorDoes decodes the vendor's bytes into
// the library struct through the shim, and expects the library values the
// struct was built from. The slot field holds the library's Timestamp, as the
// library's own decoder would put there.
func TestShimDecodesLibraryScalarsAsTheVendorDoes(t *testing.T) {
	library, vendor := scalarPair(t)
	vendorBytes, err := vendorbson.Marshal(vendor)
	if err != nil {
		t.Fatalf("vendor encoding of the vendor struct: %v", err)
	}
	var decoded libraryScalars
	if err := Unmarshal(vendorBytes, &decoded); err != nil {
		t.Fatalf("shim decoding into the library struct: %v", err)
	}
	ConvertTarget(&decoded)
	// A null inside an array decodes to nil, whichever type wrote it.
	library.List[1] = nil
	compare.Decoded(t, library, decoded)
}

// TestConvertTurnsVendorContainersIntoLibraryContainers pins the converter on
// nested vendor values: the library's D, M, A and scalars come out, at every
// level, and encoding the result gives the vendor's bytes.
func TestConvertTurnsVendorContainersIntoLibraryContainers(t *testing.T) {
	identifier := vendorprimitive.ObjectID{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2}
	vendorValue := vendorbson.D{
		{Key: "list", Value: vendorbson.A{vendorprimitive.DateTime(1), vendorbson.D{{Key: "id", Value: identifier}}, []any{vendorprimitive.Symbol("s")}}},
		{Key: "plain", Value: map[string]any{"m": vendorbson.M{"x": vendorprimitive.Null{}}}},
		{Key: "scope", Value: vendorprimitive.CodeWithScope{Code: "c", Scope: vendorbson.D{{Key: "k", Value: vendorprimitive.MinKey{}}}}},
	}
	converted, isLibraryD := Convert(vendorValue).(libbson.D)
	if !isLibraryD {
		t.Fatalf("Convert(vendor D) is %T, want the library's D", Convert(vendorValue))
	}
	list, isLibraryA := converted[0].Value.(libbson.A)
	if !isLibraryA {
		t.Fatalf("the list is %T, want the library's A", converted[0].Value)
	}
	if _, isLibraryDateTime := list[0].(libprimitive.DateTime); !isLibraryDateTime {
		t.Errorf("the date is %T, want the library's DateTime", list[0])
	}
	if _, isLibraryD := list[1].(libbson.D); !isLibraryD {
		t.Errorf("the embedded document is %T, want the library's D", list[1])
	}
	if _, isPlainSlice := list[2].([]any); !isPlainSlice {
		t.Errorf("the plain slice is %T, want []any", list[2])
	}
	vendorBytes, err := vendorbson.Marshal(vendorValue)
	if err != nil {
		t.Fatalf("vendor encoding: %v", err)
	}
	shimBytes, err := Marshal(converted)
	if err != nil {
		t.Fatalf("shim encoding of the converted value: %v", err)
	}
	compare.Encoded(t, vendorValue, vendorBytes, shimBytes)
}
