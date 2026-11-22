package server

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// fieldKind is one declarable type word of typed-fields spec §3.1. The zero
// value is kindAny, so a node whose kind was never set accepts everything
// rather than refusing everything.
type fieldKind uint8

const (
	kindAny fieldKind = iota
	kindString
	kindInt32
	kindInt
	kindDouble
	kindDecimal
	kindNumber
	kindBool
	kindDatetime
	kindBSONTimestamp
	kindBinary
	kindUUID
	kindObjectID
	kindObject
	kindArray
	kindRegex
	kindNull
)

// kindWords is the canonical word of each kind, indexed by kind. It is the
// word the catalog records and `describe bucket` prints.
var kindWords = [...]string{
	kindAny: "any", kindString: "string", kindInt32: "int32", kindInt: "int", kindDouble: "double",
	kindDecimal: "decimal", kindNumber: "number", kindBool: "bool", kindDatetime: "datetime",
	kindBSONTimestamp: "bsontimestamp", kindBinary: "binary", kindUUID: "uuid", kindObjectID: "objectid",
	kindObject: "object", kindArray: "array", kindRegex: "regex", kindNull: "null",
}

// wordKinds maps every lower-case word spec §3.1 accepts, canonical or
// synonym, to its kind. The synonyms are the words cli/declare.go has always
// accepted, so no statement that parsed before stops parsing.
var wordKinds = map[string]fieldKind{
	"string": kindString, "int32": kindInt32, "int": kindInt, "integer": kindInt, "int64": kindInt,
	"double": kindDouble, "float": kindDouble, "decimal": kindDecimal, "number": kindNumber,
	"bool": kindBool, "boolean": kindBool, "datetime": kindDatetime, "date": kindDatetime, "timestamp": kindDatetime,
	"bsontimestamp": kindBSONTimestamp, "binary": kindBinary, "uuid": kindUUID, "objectid": kindObjectID,
	"object": kindObject, "array": kindArray, "regex": kindRegex, "null": kindNull, "any": kindAny,
}

// canonicalWord reads a type word, case-insensitively, synonyms included.
func canonicalWord(word string) (fieldKind, bool) {
	kind, known := wordKinds[strings.ToLower(word)]
	return kind, known
}

// String returns the kind's canonical word.
func (kind fieldKind) String() string { return kindWords[kind] }

// mismatchReason says why a value was refused, so the message can pick the
// right one of spec §6.2's texts and the right widening. mismatchNone is
// acceptance.
type mismatchReason uint8

const (
	mismatchNone mismatchReason = iota
	// mismatchWrongType is E-1: the value's type is not one the kind holds.
	mismatchWrongType
	// mismatchInt32Range is E-2: an int64 outside int32's range.
	mismatchInt32Range
	// mismatchDoubleMagnitude is E-3: an int64 above 2^53 in magnitude.
	mismatchDoubleMagnitude
	// mismatchDoubleForInteger is E-4: a double, whole or not, into an integer type.
	mismatchDoubleForInteger
	// mismatchDoubleForDecimal is E-4's decimal variant.
	mismatchDoubleForDecimal
	// mismatchUUIDText is E-8: text into a uuid field.
	mismatchUUIDText
	// mismatchRequired is E-5: a required field is missing or null.
	mismatchRequired
)

// exactDoubleLimit is the largest magnitude below which every integer is a
// double exactly (spec §3.3).
const exactDoubleLimit = int64(1) << 53

// valueClass is what a check needs to know about a value, read from its Go
// type with no allocation: the BSON type, the integer's value for the two
// integer types, and a binary's subtype and length. A bsonType of zero is a
// value hoardDB cannot encode.
type valueClass struct {
	bsonType bsontype.Type
	integer  int64
	subtype  byte
	length   int
}

// classify reads the BSON type from the Go type, which for a wire-decoded
// value is the type byte already decoded (spec §9.2). The switch holds the
// types a handler's decode produces, and []interface{}, which an update's
// $push and $addToSet create (spec §4.5.4). Any other Go type reaches here
// only from an in-process caller and takes the slow path.
func classify(value interface{}) valueClass {
	switch typed := value.(type) {
	case nil:
		return valueClass{bsonType: bson.TypeNull}
	case string:
		return valueClass{bsonType: bson.TypeString}
	case int32:
		return valueClass{bsonType: bson.TypeInt32, integer: int64(typed)}
	case int64:
		return valueClass{bsonType: bson.TypeInt64, integer: typed}
	case float64:
		return valueClass{bsonType: bson.TypeDouble}
	case bool:
		return valueClass{bsonType: bson.TypeBoolean}
	case primitive.DateTime:
		return valueClass{bsonType: bson.TypeDateTime}
	case primitive.Decimal128:
		return valueClass{bsonType: bson.TypeDecimal128}
	case primitive.Timestamp:
		return valueClass{bsonType: bson.TypeTimestamp}
	case primitive.ObjectID:
		return valueClass{bsonType: bson.TypeObjectID}
	case primitive.Regex:
		return valueClass{bsonType: bson.TypeRegex}
	case primitive.Binary:
		return valueClass{bsonType: bson.TypeBinary, subtype: typed.Subtype, length: len(typed.Data)}
	case map[string]interface{}:
		return valueClass{bsonType: bson.TypeEmbeddedDocument}
	case primitive.A, []interface{}:
		return valueClass{bsonType: bson.TypeArray}
	}
	return classifyByEncoding(value)
}

// classifyByEncoding asks the codec what BSON type the value would encode as.
// It is the one place a type is derived by encoding, because an int, a uint32,
// a time.Time, a []string, a struct, a bson.M and a bson.D each encode as a
// different type from the one their Go name suggests (a uint32 is an int64).
// No network client reaches it, so it is correct rather than fast.
func classifyByEncoding(value interface{}) valueClass {
	encodedType, encoded, err := bson.MarshalValue(value)
	if err != nil {
		return valueClass{}
	}
	switch encodedType {
	case bson.TypeInt32:
		if number, _, ok := bsoncore.ReadInt32(encoded); ok {
			return valueClass{bsonType: encodedType, integer: int64(number)}
		}
	case bson.TypeInt64:
		if number, _, ok := bsoncore.ReadInt64(encoded); ok {
			return valueClass{bsonType: encodedType, integer: number}
		}
	case bson.TypeBinary:
		if subtype, payload, _, ok := bsoncore.ReadBinary(encoded); ok {
			return valueClass{bsonType: encodedType, subtype: subtype, length: len(payload)}
		}
	default:
		return valueClass{bsonType: encodedType}
	}
	return valueClass{}
}

// accepts is the one rule of spec §3: it reports why a value does not satisfy
// a kind, or mismatchNone. It never converts and never reads a value beyond
// its type, an integer's magnitude and a binary's subtype and length. Null
// and a missing field are not its concern: an optional field takes them, and
// a required one refuses them, in checkDocument.
func accepts(kind fieldKind, value interface{}) mismatchReason {
	if kind == kindAny {
		return mismatchNone
	}
	class := classify(value)
	switch kind {
	case kindString:
		return requireType(class, bson.TypeString)
	case kindInt32:
		return acceptsInt32(class)
	case kindInt:
		return acceptsInt(class)
	case kindDouble:
		return acceptsDouble(class)
	case kindDecimal:
		return acceptsDecimal(class)
	case kindNumber:
		switch class.bsonType {
		case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
			return mismatchNone
		}
	case kindBool:
		return requireType(class, bson.TypeBoolean)
	case kindDatetime:
		return requireType(class, bson.TypeDateTime)
	case kindBSONTimestamp:
		return requireType(class, bson.TypeTimestamp)
	case kindBinary:
		return requireType(class, bson.TypeBinary)
	case kindUUID:
		return acceptsUUID(class)
	case kindObjectID:
		return requireType(class, bson.TypeObjectID)
	case kindObject:
		return requireType(class, bson.TypeEmbeddedDocument)
	case kindArray:
		return requireType(class, bson.TypeArray)
	case kindRegex:
		return requireType(class, bson.TypeRegex)
	case kindNull:
		return requireType(class, bson.TypeNull)
	}
	return mismatchWrongType
}

func requireType(class valueClass, want bsontype.Type) mismatchReason {
	if class.bsonType == want {
		return mismatchNone
	}
	return mismatchWrongType
}

// acceptsInt32 takes an int64 that fits: the encoding is the sender's, the
// range is the declaration's (spec §3.3).
func acceptsInt32(class valueClass) mismatchReason {
	switch class.bsonType {
	case bson.TypeInt32:
		return mismatchNone
	case bson.TypeInt64:
		if class.integer < -1<<31 || class.integer > 1<<31-1 {
			return mismatchInt32Range
		}
		return mismatchNone
	case bson.TypeDouble:
		return mismatchDoubleForInteger
	}
	return mismatchWrongType
}

func acceptsInt(class valueClass) mismatchReason {
	switch class.bsonType {
	case bson.TypeInt32, bson.TypeInt64:
		return mismatchNone
	case bson.TypeDouble:
		return mismatchDoubleForInteger
	}
	return mismatchWrongType
}

// acceptsDouble takes an integer a double holds exactly, which is every one
// up to 2^53 in magnitude.
func acceptsDouble(class valueClass) mismatchReason {
	switch class.bsonType {
	case bson.TypeDouble, bson.TypeInt32:
		return mismatchNone
	case bson.TypeInt64:
		if class.integer > exactDoubleLimit || class.integer < -exactDoubleLimit {
			return mismatchDoubleMagnitude
		}
		return mismatchNone
	}
	return mismatchWrongType
}

// acceptsDecimal takes a Decimal128 only (OC-5): the readers do not order an
// integer and a decimal as one kind of number.
func acceptsDecimal(class valueClass) mismatchReason {
	switch class.bsonType {
	case bson.TypeDecimal128:
		return mismatchNone
	case bson.TypeDouble:
		return mismatchDoubleForDecimal
	}
	return mismatchWrongType
}

// acceptsUUID takes binary subtype 4 of exactly 16 bytes. hoardDB's own
// document ids are UUID text, so a string gets its own reason (E-8).
func acceptsUUID(class valueClass) mismatchReason {
	switch {
	case class.bsonType == bson.TypeBinary && class.subtype == bson.TypeBinaryUUID && class.length == 16:
		return mismatchNone
	case class.bsonType == bson.TypeString:
		return mismatchUUIDText
	}
	return mismatchWrongType
}

// kindOf is the narrowest word that declares a value's type: the word a
// declaration would need to accept exactly what was received. A deprecated
// type has none but any.
func kindOf(value interface{}) fieldKind {
	class := classify(value)
	switch class.bsonType {
	case bson.TypeNull:
		return kindNull
	case bson.TypeString:
		return kindString
	case bson.TypeDouble:
		return kindDouble
	case bson.TypeInt32:
		return kindInt32
	case bson.TypeInt64:
		return kindInt
	case bson.TypeDecimal128:
		return kindDecimal
	case bson.TypeBoolean:
		return kindBool
	case bson.TypeDateTime:
		return kindDatetime
	case bson.TypeTimestamp:
		return kindBSONTimestamp
	case bson.TypeBinary:
		if acceptsUUID(class) == mismatchNone {
			return kindUUID
		}
		return kindBinary
	case bson.TypeObjectID:
		return kindObjectID
	case bson.TypeEmbeddedDocument:
		return kindObject
	case bson.TypeArray:
		return kindArray
	case bson.TypeRegex:
		return kindRegex
	}
	return kindAny
}

// numericKind reports whether a kind declares numbers only.
func numericKind(kind fieldKind) bool {
	switch kind {
	case kindInt32, kindInt, kindDouble, kindDecimal, kindNumber:
		return true
	}
	return false
}

// receivedWord names a value's BSON type with spec §6.2's <received> words.
// A refusal carries the type, never the value (RD-10). It replaces the
// words of typeLabel, so one vocabulary serves every refusal.
func receivedWord(value interface{}) string {
	class := classify(value)
	switch class.bsonType {
	case bson.TypeNull:
		return "null"
	case bson.TypeString:
		return "a string"
	case bson.TypeDouble:
		return "a double"
	case bson.TypeInt32:
		return "a 32-bit integer"
	case bson.TypeInt64:
		return "a 64-bit integer"
	case bson.TypeDecimal128:
		return "a 128-bit decimal"
	case bson.TypeBoolean:
		return "a boolean"
	case bson.TypeDateTime:
		return "a datetime"
	case bson.TypeTimestamp:
		return "a BSON timestamp"
	case bson.TypeBinary:
		if class.subtype == bson.TypeBinaryUUID && class.length == 16 {
			return "a UUID"
		}
		return fmt.Sprintf("binary data (subtype 0x%02X)", class.subtype)
	case bson.TypeObjectID:
		return "an object id"
	case bson.TypeEmbeddedDocument:
		return "a document"
	case bson.TypeArray:
		return "an array"
	case bson.TypeRegex:
		return "a regular expression"
	case bson.TypeUndefined, bson.TypeDBPointer, bson.TypeJavaScript, bson.TypeSymbol,
		bson.TypeCodeWithScope, bson.TypeMinKey, bson.TypeMaxKey:
		return fmt.Sprintf("a deprecated BSON type (%s)", class.bsonType)
	}
	return fmt.Sprintf("a value of type %T", value)
}
