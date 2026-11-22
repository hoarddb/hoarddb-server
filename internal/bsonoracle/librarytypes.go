package bsonoracle

import (
	"fmt"
	"reflect"

	libbson "github.com/hoarddb/hoarddb-bson/bson"
	libbsontype "github.com/hoarddb/hoarddb-bson/bson/bsontype"
	libprimitive "github.com/hoarddb/hoarddb-bson/bson/primitive"
	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorbsoncodec "go.mongodb.org/mongo-driver/bson/bsoncodec"
	vendorbsonrw "go.mongodb.org/mongo-driver/bson/bsonrw"
	vendorbsontype "go.mongodb.org/mongo-driver/bson/bsontype"
	vendorprimitive "go.mongodb.org/mongo-driver/bson/primitive"
)

// The shim's codecs for the library's types (bson-codec-spec 9.1). A library
// scalar or container is registered on the shim's private vendor registry,
// and its codec delegates to the vendor's own codec for the matching vendor
// type: the library value is turned into the vendor's, the vendor writes the
// bytes, and on the way back the vendor reads into its type and the result is
// turned into the library's. The vendor therefore stays the author of every
// byte, and a library type is never encoded by anything but the vendor.

func init() {
	registerLibraryTypes(vendorRegistry)
}

// libraryTypeCodec pairs a library type with the vendor type that stands for
// it and the two conversions between them.
type libraryTypeCodec struct {
	libraryType reflect.Type
	vendorType  reflect.Type
	toVendor    func(libraryValue any) any
	toLibrary   func(vendorValue any) any
}

func libraryTypeCodecs() []libraryTypeCodec {
	return []libraryTypeCodec{
		typeCodec(libprimitive.ObjectID{}, vendorprimitive.ObjectID{}),
		typeCodec(libprimitive.DateTime(0), vendorprimitive.DateTime(0)),
		typeCodec(libprimitive.Timestamp{}, vendorprimitive.Timestamp{}),
		typeCodec(libprimitive.Binary{}, vendorprimitive.Binary{}),
		typeCodec(libprimitive.Regex{}, vendorprimitive.Regex{}),
		typeCodec(libprimitive.DBPointer{}, vendorprimitive.DBPointer{}),
		typeCodec(libprimitive.JavaScript(""), vendorprimitive.JavaScript("")),
		typeCodec(libprimitive.Symbol(""), vendorprimitive.Symbol("")),
		typeCodec(libprimitive.CodeWithScope{}, vendorprimitive.CodeWithScope{}),
		typeCodec(libprimitive.Decimal128{}, vendorprimitive.Decimal128{}),
		typeCodec(libprimitive.Undefined{}, vendorprimitive.Undefined{}),
		typeCodec(libprimitive.Null{}, vendorprimitive.Null{}),
		typeCodec(libprimitive.MinKey{}, vendorprimitive.MinKey{}),
		typeCodec(libprimitive.MaxKey{}, vendorprimitive.MaxKey{}),
		typeCodec(libbson.D{}, vendorbson.D{}),
		typeCodec(libbson.M{}, vendorbson.M{}),
		typeCodec(libbson.A{}, vendorbson.A{}),
		typeCodec(libbson.Raw{}, vendorbson.Raw{}),
		typeCodec(libbson.RawValue{}, vendorbson.RawValue{}),
	}
}

func typeCodec(libraryZero, vendorZero any) libraryTypeCodec {
	return libraryTypeCodec{
		libraryType: reflect.TypeOf(libraryZero),
		vendorType:  reflect.TypeOf(vendorZero),
		toVendor: func(libraryValue any) any {
			vendorValue, _ := toVendorValue(libraryValue)
			return vendorValue
		},
		toLibrary: Convert,
	}
}

func registerLibraryTypes(registry *vendorbsoncodec.Registry) {
	for _, codec := range libraryTypeCodecs() {
		registry.RegisterTypeEncoder(codec.libraryType, vendorbsoncodec.ValueEncoderFunc(
			func(encodeContext vendorbsoncodec.EncodeContext, valueWriter vendorbsonrw.ValueWriter, libraryValue reflect.Value) error {
				vendorValue := reflect.ValueOf(codec.toVendor(libraryValue.Interface()))
				vendorEncoder, err := encodeContext.LookupEncoder(codec.vendorType)
				if err != nil {
					return fmt.Errorf("bsonoracle: finding the vendor encoder for %v, the stand-in of %v: %w", codec.vendorType, codec.libraryType, err)
				}
				return vendorEncoder.EncodeValue(encodeContext, valueWriter, vendorValue)
			}))
		registry.RegisterTypeDecoder(codec.libraryType, vendorbsoncodec.ValueDecoderFunc(
			func(decodeContext vendorbsoncodec.DecodeContext, valueReader vendorbsonrw.ValueReader, libraryTarget reflect.Value) error {
				vendorDecoder, err := decodeContext.LookupDecoder(codec.vendorType)
				if err != nil {
					return fmt.Errorf("bsonoracle: finding the vendor decoder for %v, the stand-in of %v: %w", codec.vendorType, codec.libraryType, err)
				}
				vendorHolder := reflect.New(codec.vendorType).Elem()
				if err := vendorDecoder.DecodeValue(decodeContext, valueReader, vendorHolder); err != nil {
					return fmt.Errorf("bsonoracle: vendor decoding into %v, the stand-in of %v: %w", codec.vendorType, codec.libraryType, err)
				}
				libraryTarget.Set(reflect.ValueOf(codec.toLibrary(vendorHolder.Interface())))
				return nil
			}))
	}
}

// toVendorValue is the library value's vendor stand-in, or the value itself
// and false when it is not one of the library's types. Only the value's own
// level is turned: a library value inside a D, an M or an A stays as it is,
// and the vendor reaches it through the registry when it encodes the
// container's element, which is why the stand-in needs no deep walk.
func toVendorValue(libraryValue any) (any, bool) {
	switch typed := libraryValue.(type) {
	case libprimitive.ObjectID:
		return vendorprimitive.ObjectID(typed), true
	case libprimitive.DateTime:
		return vendorprimitive.DateTime(typed), true
	case libprimitive.Timestamp:
		return vendorprimitive.Timestamp{T: typed.T, I: typed.I}, true
	case libprimitive.Binary:
		return vendorprimitive.Binary{Subtype: typed.Subtype, Data: typed.Data}, true
	case libprimitive.Regex:
		return vendorprimitive.Regex{Pattern: typed.Pattern, Options: typed.Options}, true
	case libprimitive.DBPointer:
		return vendorprimitive.DBPointer{DB: typed.DB, Pointer: vendorprimitive.ObjectID(typed.Pointer)}, true
	case libprimitive.JavaScript:
		return vendorprimitive.JavaScript(typed), true
	case libprimitive.Symbol:
		return vendorprimitive.Symbol(typed), true
	case libprimitive.CodeWithScope:
		return vendorprimitive.CodeWithScope{Code: vendorprimitive.JavaScript(typed.Code), Scope: typed.Scope}, true
	case libprimitive.Decimal128:
		high, low := typed.GetBytes()
		return vendorprimitive.NewDecimal128(high, low), true
	case libprimitive.Undefined:
		return vendorprimitive.Undefined{}, true
	case libprimitive.Null:
		return vendorprimitive.Null{}, true
	case libprimitive.MinKey:
		return vendorprimitive.MinKey{}, true
	case libprimitive.MaxKey:
		return vendorprimitive.MaxKey{}, true
	case libbson.D:
		if typed == nil {
			return vendorbson.D(nil), true
		}
		elements := make(vendorbson.D, len(typed))
		for position, element := range typed {
			elements[position] = vendorbson.E{Key: element.Key, Value: element.Value}
		}
		return elements, true
	case libbson.M:
		return vendorbson.M(typed), true
	case libbson.A:
		return vendorbson.A(typed), true
	case libbson.Raw:
		return vendorbson.Raw(typed), true
	case libbson.RawValue:
		return vendorbson.RawValue{Type: vendorbsontype.Type(typed.Type), Value: typed.Value}, true
	}
	return libraryValue, false
}

// toLibraryScalar is the library's form of a vendor scalar, or the value and
// false for anything that is not one. Containers are Convert's.
func toLibraryScalar(vendorValue any) (any, bool) {
	switch typed := vendorValue.(type) {
	case vendorprimitive.ObjectID:
		return libprimitive.ObjectID(typed), true
	case vendorprimitive.DateTime:
		return libprimitive.DateTime(typed), true
	case vendorprimitive.Timestamp:
		return libprimitive.Timestamp{T: typed.T, I: typed.I}, true
	case vendorprimitive.Binary:
		return libprimitive.Binary{Subtype: typed.Subtype, Data: typed.Data}, true
	case vendorprimitive.Regex:
		return libprimitive.Regex{Pattern: typed.Pattern, Options: typed.Options}, true
	case vendorprimitive.DBPointer:
		return libprimitive.DBPointer{DB: typed.DB, Pointer: libprimitive.ObjectID(typed.Pointer)}, true
	case vendorprimitive.JavaScript:
		return libprimitive.JavaScript(typed), true
	case vendorprimitive.Symbol:
		return libprimitive.Symbol(typed), true
	case vendorprimitive.CodeWithScope:
		return libprimitive.CodeWithScope{Code: libprimitive.JavaScript(typed.Code), Scope: Convert(typed.Scope)}, true
	case vendorprimitive.Decimal128:
		high, low := typed.GetBytes()
		return libprimitive.NewDecimal128(high, low), true
	case vendorprimitive.Undefined:
		return libprimitive.Undefined{}, true
	case vendorprimitive.Null:
		return libprimitive.Null{}, true
	case vendorprimitive.MinKey:
		return libprimitive.MinKey{}, true
	case vendorprimitive.MaxKey:
		return libprimitive.MaxKey{}, true
	case vendorbson.Raw:
		return libbson.Raw(typed), true
	case vendorbson.RawValue:
		return libbson.RawValue{Type: libbsontype.Type(typed.Type), Value: typed.Value}, true
	}
	return vendorValue, false
}

// Convert returns the library's form of a value the vendor decoded, keeping
// the container types of spec 5.3 D1 to D7: the vendor's D, M and A become the
// library's, a map[string]any or []any keeps its type with its elements
// converted, and a vendor scalar becomes the library scalar. Anything else is
// returned as it is. The vendor's value is not changed.
func Convert(vendorValue any) any {
	switch typed := vendorValue.(type) {
	case vendorbson.D:
		if typed == nil {
			return libbson.D(nil)
		}
		elements := make(libbson.D, len(typed))
		for position, element := range typed {
			elements[position] = libbson.E{Key: element.Key, Value: Convert(element.Value)}
		}
		return elements
	case vendorbson.M:
		if typed == nil {
			return libbson.M(nil)
		}
		members := make(libbson.M, len(typed))
		for key, member := range typed {
			members[key] = Convert(member)
		}
		return members
	case vendorbson.A:
		if typed == nil {
			return libbson.A(nil)
		}
		elements := make(libbson.A, len(typed))
		for position, element := range typed {
			elements[position] = Convert(element)
		}
		return elements
	case map[string]any:
		if typed == nil {
			return typed
		}
		members := make(map[string]any, len(typed))
		for key, member := range typed {
			members[key] = Convert(member)
		}
		return members
	case []any:
		if typed == nil {
			return typed
		}
		elements := make([]any, len(typed))
		for position, element := range typed {
			elements[position] = Convert(element)
		}
		return elements
	}
	libraryScalar, _ := toLibraryScalar(vendorValue)
	return libraryScalar
}

// ConvertTarget converts, in place, every interface-typed slot reachable from
// target, a pointer to what the vendor decoded, as Convert does. A typed field
// or element is left alone: a decode into a library type already produced the
// library's.
func ConvertTarget(target any) {
	convertSlots(reflect.ValueOf(target))
}

func convertSlots(slot reflect.Value) {
	switch slot.Kind() {
	case reflect.Interface:
		if slot.IsNil() || !slot.CanSet() {
			return
		}
		slot.Set(reflect.ValueOf(Convert(slot.Elem().Interface())))
	case reflect.Pointer:
		if !slot.IsNil() {
			convertSlots(slot.Elem())
		}
	case reflect.Struct:
		for fieldIndex := range slot.NumField() {
			if slot.Field(fieldIndex).CanSet() {
				convertSlots(slot.Field(fieldIndex))
			}
		}
	case reflect.Slice, reflect.Array:
		for position := range slot.Len() {
			convertSlots(slot.Index(position))
		}
	case reflect.Map:
		for mapEntry := slot.MapRange(); mapEntry.Next(); {
			member := reflect.New(slot.Type().Elem()).Elem()
			member.Set(mapEntry.Value())
			convertSlots(member)
			slot.SetMapIndex(mapEntry.Key(), member)
		}
	}
}
