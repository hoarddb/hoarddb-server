package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// The cases of the BSON corpus. Encode vectors are Go values the server's
// encoder writes; decode and reject vectors are bytes, built with the small
// helpers below so every length is computed, except where a length is the
// point of the case.

// encodeCase is one value written by the server's encoder.
type encodeCase struct {
	name     string
	notes    string
	document bson.D
}

func bsonSingle(field any) bson.D { return bson.D{{Key: "a", Value: field}} }

// nestedDocuments is a chain of embedded documents of the given total depth
// (the root counts 1), each under the key "a"; the innermost is empty.
func bsonNestedDocuments(depth int) bson.D {
	chain := bson.D{}
	for level := 1; level < depth; level++ {
		chain = bson.D{{Key: "a", Value: chain}}
	}
	return chain
}

// nestedArrays is a root document holding arrays nested to the given total
// depth (the root counts 1); the innermost array is empty.
func bsonNestedArrays(depth int) bson.D {
	chain := bson.A{}
	for level := 2; level < depth; level++ {
		chain = bson.A{chain}
	}
	return bson.D{{Key: "a", Value: chain}}
}

func bsonEncodeCases() ([]encodeCase, error) {
	// A bad literal in the table is a generator bug, found on every run.
	var literalErr error
	mustDecimal := func(text string) primitive.Decimal128 {
		parsed, err := primitive.ParseDecimal128(text)
		if err != nil && literalErr == nil {
			literalErr = fmt.Errorf("decimal128 literal %q: %w", text, err)
		}
		return parsed
	}
	mustObjectID := func(text string) primitive.ObjectID {
		parsed, err := primitive.ObjectIDFromHex(text)
		if err != nil && literalErr == nil {
			literalErr = fmt.Errorf("object id literal %q: %w", text, err)
		}
		return parsed
	}
	uuid := primitive.Binary{Subtype: 0x04, Data: []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}}
	cases := []encodeCase{
		{"int32/zero", "", bsonSingle(int32(0))},
		{"int32/one", "", bsonSingle(int32(1))},
		{"int32/minus-one", "", bsonSingle(int32(-1))},
		{"int32/max", "2^31-1.", bsonSingle(int32(math.MaxInt32))},
		{"int32/min", "-2^31.", bsonSingle(int32(math.MinInt32))},
		{"int64/zero", "an int64 stays int64 even when it fits int32; a Python int in the int32 range is written int32 (a $numberInt), not this.", bsonSingle(int64(0))},
		{"int64/small", "an int64 that fits int32, kept int64.", bsonSingle(int64(5))},
		{"int64/just-over-int32", "2^31: the first value that needs int64.", bsonSingle(int64(math.MaxInt32) + 1)},
		{"int64/just-under-int32", "-2^31-1: the first negative value that needs int64.", bsonSingle(int64(math.MinInt32) - 1)},
		{"int64/max", "2^63-1.", bsonSingle(int64(math.MaxInt64))},
		{"int64/min", "-2^63.", bsonSingle(int64(math.MinInt64))},
		{"double/zero", "", bsonSingle(0.0)},
		{"double/negative-zero", "bytes 8000000000000000: -0.0 and 0.0 are different bytes.", bsonSingle(math.Copysign(0, -1))},
		{"double/one", "", bsonSingle(1.0)},
		{"double/one-and-a-half", "", bsonSingle(1.5)},
		{"double/negative-one-and-a-half", "", bsonSingle(-1.5)},
		{"double/point-one", "not exactly representable: the bytes are the nearest double.", bsonSingle(0.1)},
		{"double/pi", "", bsonSingle(math.Pi)},
		{"double/nan", "the quiet NaN 0x7ff8000000000000, which is what Python's float('nan') writes; Go's math.NaN() is 0x7ff8000000000001, so this vector builds the bits explicitly.", bsonSingle(math.Float64frombits(0x7ff8000000000000))},
		{"double/infinity", "", bsonSingle(math.Inf(1))},
		{"double/negative-infinity", "", bsonSingle(math.Inf(-1))},
		{"double/subnormal-smallest", "5e-324, the smallest positive subnormal (bits 0000000000000001).", bsonSingle(math.SmallestNonzeroFloat64)},
		{"double/subnormal-largest", "the largest subnormal.", bsonSingle(math.Float64frombits(0x000fffffffffffff))},
		{"double/min-normal", "the smallest positive normal double.", bsonSingle(math.Float64frombits(0x0010000000000000))},
		{"double/max", "the largest finite double.", bsonSingle(math.MaxFloat64)},
		{"double/2^53-1", "9007199254740991, the largest integer every double below it also represents.", bsonSingle(float64(1<<53 - 1))},
		{"double/2^53", "9007199254740992.", bsonSingle(float64(1 << 53))},
		{"double/2^53+2", "9007199254740994: 2^53+1 has no double, the next one is +2 (float(2**53+1) is 2^53).", bsonSingle(float64(1<<53) + 2)},
		{"string/empty", "", bsonSingle("")},
		{"string/ascii", "", bsonSingle("hello")},
		{"string/multibyte-2-byte", "U+00E9.", bsonSingle("é")},
		{"string/multibyte-3-byte", "three CJK characters, 3 bytes each.", bsonSingle("日本語")},
		{"string/multibyte-4-byte", "U+1F600, outside the basic plane (a surrogate pair in UTF-16).", bsonSingle("😀")},
		{"string/max-codepoint", "U+10FFFF.", bsonSingle("\U0010FFFF")},
		{"string/noncharacter", "U+FFFE is a valid scalar value.", bsonSingle("￾")},
		{"string/embedded-nul", "a string may hold NUL: the length prefix, not a terminator, delimits it (the server's encoder allows it).", bsonSingle("a\x00b")},
		{"string/only-nul", "", bsonSingle("\x00")},
		{"string/control-characters", "", bsonSingle("\x01\x1f\x7f")},
		{"string/long-300", "longer than 255 bytes.", bsonSingle(strings.Repeat("x", 300))},
		{"bool/true", "", bsonSingle(true)},
		{"bool/false", "", bsonSingle(false)},
		{"null/null", "", bsonSingle(nil)},
		{"datetime/epoch", "", bsonSingle(primitive.DateTime(0))},
		{"datetime/one-ms", "", bsonSingle(primitive.DateTime(1))},
		{"datetime/minus-one-ms", "1969-12-31T23:59:59.999Z: a value below zero is -1, not 0 (see the datetime_floor vectors for how a driver gets it from an instant).", bsonSingle(primitive.DateTime(-1))},
		{"datetime/python-min", "0001-01-01T00:00:00Z, datetime.min in Python (-62135596800000).", bsonSingle(primitive.DateTime(-62135596800000))},
		{"datetime/python-max", "9999-12-31T23:59:59.999Z, datetime.max at millisecond precision (253402300799999).", bsonSingle(primitive.DateTime(253402300799999))},
		{"datetime/over-python-max", "one millisecond past Python's datetime.max: a Python decoder returns Opaque (driver-python-spec section 7.2), never OverflowError.", bsonSingle(primitive.DateTime(253402300800000))},
		{"datetime/under-python-min", "one millisecond before datetime.min: Opaque in Python.", bsonSingle(primitive.DateTime(-62135596800001))},
		{"datetime/int64-max", "the largest datetime BSON holds.", bsonSingle(primitive.DateTime(math.MaxInt64))},
		{"datetime/int64-min", "the smallest datetime BSON holds.", bsonSingle(primitive.DateTime(math.MinInt64))},
		{"datetime/2026", "2026-10-13T00:00:00Z.", bsonSingle(primitive.DateTime(1791849600000))},
		{"binary/generic-empty", "subtype 00, no bytes.", bsonSingle(primitive.Binary{Subtype: 0x00, Data: []byte{}})},
		{"binary/generic", "subtype 00.", bsonSingle(primitive.Binary{Subtype: 0x00, Data: []byte{0x00, 0x01, 0xfe, 0xff}})},
		{"binary/uuid-subtype-4", "a UUID: subtype 04, 16 bytes in RFC 4122 order.", bsonSingle(uuid)},
		{"binary/subtype-01-function", "other subtypes are Opaque in Python and must round trip unchanged.", bsonSingle(primitive.Binary{Subtype: 0x01, Data: []byte{0x01}})},
		{"binary/subtype-03-uuid-legacy", "the legacy UUID subtype; not a Python UUID.", bsonSingle(primitive.Binary{Subtype: 0x03, Data: uuid.Data})},
		{"binary/subtype-05-md5", "", bsonSingle(primitive.Binary{Subtype: 0x05, Data: bytes.Repeat([]byte{0xab}, 16)})},
		{"binary/subtype-80-user-defined", "", bsonSingle(primitive.Binary{Subtype: 0x80, Data: []byte("user")})},
		{"binary/subtype-ff", "", bsonSingle(primitive.Binary{Subtype: 0xff, Data: []byte{0xff}})},
		{"binary/generic-4096", "a payload past one byte of length.", bsonSingle(primitive.Binary{Subtype: 0x00, Data: bytes.Repeat([]byte{0x5a}, 4096)})},
		{"decimal128/zero", "", bsonSingle(mustDecimal("0"))},
		{"decimal128/negative-zero", "", bsonSingle(mustDecimal("-0"))},
		{"decimal128/zero-with-exponent", "zero with a positive exponent keeps the exponent.", bsonSingle(mustDecimal("0E+3"))},
		{"decimal128/one", "", bsonSingle(mustDecimal("1"))},
		{"decimal128/minus-one", "", bsonSingle(mustDecimal("-1"))},
		{"decimal128/one-point-five", "", bsonSingle(mustDecimal("1.5"))},
		{"decimal128/one-point-five-zero", "trailing zeros are part of the value: 1.50 and 1.5 are different bytes.", bsonSingle(mustDecimal("1.50"))},
		{"decimal128/point-one", "", bsonSingle(mustDecimal("0.1"))},
		{"decimal128/large", "", bsonSingle(mustDecimal("123456789012345678901234567890.123"))},
		{"decimal128/max-digits", "34 significant digits, the most decimal128 holds.", bsonSingle(mustDecimal("9999999999999999999999999999999999"))},
		{"decimal128/max-digits-fraction", "34 significant digits with a fraction.", bsonSingle(mustDecimal("1234567890123456789012345678901234E-20"))},
		{"decimal128/exponent-max", "the largest exponent of a one-digit coefficient that stays within 34 digits.", bsonSingle(mustDecimal("1E+6111"))},
		{"decimal128/exponent-min", "the smallest exponent (a subnormal-style value).", bsonSingle(mustDecimal("1E-6176"))},
		{"decimal128/largest-finite", "34 nines at the largest exponent.", bsonSingle(mustDecimal("9.999999999999999999999999999999999E+6144"))},
		{"decimal128/infinity", "", bsonSingle(mustDecimal("Infinity"))},
		{"decimal128/negative-infinity", "", bsonSingle(mustDecimal("-Infinity"))},
		{"decimal128/nan", "", bsonSingle(mustDecimal("NaN"))},
		{"objectid/typical", "", bsonSingle(mustObjectID("507f1f77bcf86cd799439011"))},
		{"objectid/zero", "", bsonSingle(mustObjectID("000000000000000000000000"))},
		{"objectid/max", "", bsonSingle(mustObjectID("ffffffffffffffffffffffff"))},
		{"timestamp/typical", "Opaque in Python.", bsonSingle(primitive.Timestamp{T: 1700000000, I: 7})},
		{"timestamp/max", "both halves at 2^32-1.", bsonSingle(primitive.Timestamp{T: math.MaxUint32, I: math.MaxUint32})},
		{"regex/typical", "Opaque in Python.", bsonSingle(primitive.Regex{Pattern: "^a.*b$", Options: "im"})},
		{"regex/empty", "", bsonSingle(primitive.Regex{})},
		{"javascript/typical", "Opaque in Python.", bsonSingle(primitive.JavaScript("function(){return 1}"))},
		{"javascript-with-scope/typical", "Opaque in Python.", bsonSingle(primitive.CodeWithScope{Code: "x + 1", Scope: bson.D{{Key: "x", Value: int32(1)}}})},
		{"symbol/typical", "deprecated type, Opaque in Python.", bsonSingle(primitive.Symbol("sym"))},
		{"undefined/undefined", "deprecated type, Opaque in Python.", bsonSingle(primitive.Undefined{})},
		{"minkey/minkey", "Opaque in Python.", bsonSingle(primitive.MinKey{})},
		{"maxkey/maxkey", "Opaque in Python.", bsonSingle(primitive.MaxKey{})},
		{"dbpointer/typical", "deprecated type, Opaque in Python.", bsonSingle(primitive.DBPointer{DB: "db.coll", Pointer: mustObjectID("507f1f77bcf86cd799439011")})},
		{"document/empty", "the smallest document: 05000000 00.", bson.D{}},
		{"document/one-field", "", bson.D{{Key: "a", Value: int32(1)}}},
		{"document/key-order", "keys are written in the order given, never sorted.", bson.D{{Key: "z", Value: int32(1)}, {Key: "a", Value: int32(2)}, {Key: "m", Value: int32(3)}}},
		{"document/empty-key", "the empty string is a legal key.", bson.D{{Key: "", Value: int32(1)}}},
		{"document/key-multibyte", "keys are UTF-8.", bson.D{{Key: "héllo日本", Value: int32(1)}}},
		{"document/key-dollar-dot", "the codec gives $ and . no meaning.", bson.D{{Key: "$x", Value: int32(1)}, {Key: "a.b", Value: int32(2)}}},
		{"document/key-long", "a 300-byte key.", bson.D{{Key: strings.Repeat("k", 300), Value: int32(1)}}},
		{"document/nested", "", bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: bson.D{{Key: "c", Value: int32(1)}}}}}}},
		{"document/nested-empty", "", bson.D{{Key: "a", Value: bson.D{}}}},
		{"document/nested-depth-2", "", bsonNestedDocuments(2)},
		{"document/nested-depth-99", "", bsonNestedDocuments(99)},
		{"document/nested-depth-100", "the deepest document a decoder must accept (the root counts 1).", bsonNestedDocuments(100)},
		{"array/empty", "", bsonSingle(bson.A{})},
		{"array/ints", "array keys are the decimal indexes 0, 1, 2.", bsonSingle(bson.A{int32(1), int32(2), int32(3)})},
		{"array/mixed", "an array holds any types, in order.", bsonSingle(bson.A{int32(1), "two", 3.0, true, nil, bson.A{}, bson.D{}})},
		{"array/of-documents", "", bsonSingle(bson.A{bson.D{{Key: "x", Value: int32(1)}}, bson.D{{Key: "x", Value: int32(2)}}})},
		{"array/eleven-elements", "keys 0 to 10: the index key grows to two digits.", bsonSingle(bson.A{int32(0), int32(1), int32(2), int32(3), int32(4), int32(5), int32(6), int32(7), int32(8), int32(9), int32(10)})},
		{"array/nested-depth-100", "arrays nested to the deepest depth a decoder must accept (the root document counts 1).", bsonNestedArrays(100)},
		{"document/realistic-row", "a stored document as a find returns it: _id_ first (wire.md).", bson.D{
			{Key: "_id_", Value: "u1"}, {Key: "name", Value: "Ada"}, {Key: "age", Value: int32(36)},
			{Key: "tags", Value: bson.A{"a", "b"}}, {Key: "created", Value: primitive.DateTime(1791849600000)},
			{Key: "balance", Value: mustDecimal("12.50")}, {Key: "visits", Value: int64(1) << 40},
		}},
		{"document/every-type", "one document with every type, in this order.", bson.D{
			{Key: "double", Value: 1.5}, {Key: "string", Value: "s"}, {Key: "document", Value: bson.D{{Key: "x", Value: int32(1)}}},
			{Key: "array", Value: bson.A{int32(1)}}, {Key: "binary", Value: primitive.Binary{Subtype: 0, Data: []byte{1}}},
			{Key: "undefined", Value: primitive.Undefined{}}, {Key: "objectid", Value: mustObjectID("507f1f77bcf86cd799439011")},
			{Key: "bool", Value: true}, {Key: "datetime", Value: primitive.DateTime(-1)}, {Key: "null", Value: nil},
			{Key: "regex", Value: primitive.Regex{Pattern: "a", Options: "i"}},
			{Key: "dbpointer", Value: primitive.DBPointer{DB: "d", Pointer: mustObjectID("507f1f77bcf86cd799439011")}},
			{Key: "javascript", Value: primitive.JavaScript("1")}, {Key: "symbol", Value: primitive.Symbol("s")},
			{Key: "javascript-with-scope", Value: primitive.CodeWithScope{Code: "1", Scope: bson.D{}}},
			{Key: "int32", Value: int32(1)}, {Key: "timestamp", Value: primitive.Timestamp{T: 1, I: 2}},
			{Key: "int64", Value: int64(1)}, {Key: "decimal128", Value: mustDecimal("1")},
			{Key: "maxkey", Value: primitive.MaxKey{}}, {Key: "minkey", Value: primitive.MinKey{}},
		}},
	}
	return cases, literalErr
}

// groupOf is the part of a vector name before the slash.
func bsonGroupOf(name string) string {
	group, _, _ := strings.Cut(name, "/")
	return group
}

func bsonEncodeVectors() ([]bsonVector, error) {
	cases, err := bsonEncodeCases()
	if err != nil {
		return nil, err
	}
	vectors := make([]bsonVector, 0, len(cases))
	for _, entry := range cases {
		raw, err := bson.Marshal(entry.document)
		if err != nil {
			return nil, fmt.Errorf("vector %q: the server's encoder: %w", entry.name, err)
		}
		typed, err := documentToTyped(entry.document)
		if err != nil {
			return nil, fmt.Errorf("vector %q: %w", entry.name, err)
		}
		vectors = append(vectors, bsonVector{
			Name: entry.name, Group: bsonGroupOf(entry.name), Kind: "encode", Notes: entry.notes,
			Document: typed, Hex: bsonHexOf(raw),
		})
	}
	return vectors, nil
}

// ---- byte builders for decode and reject inputs ----------------------------

func bsonLe32(number int32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(number))
	return out
}

func bsonCstring(text string) []byte { return append([]byte(text), 0) }

// lengthPrefixed is a BSON string body: int32 length (with the NUL), bytes, NUL.
func bsonLengthPrefixed(text []byte) []byte {
	out := bsonLe32(int32(len(text) + 1))
	out = append(out, text...)
	return append(out, 0)
}

// field is one element: type byte, key cstring, body.
func bsonField(typeByte byte, key string, body []byte) []byte {
	out := []byte{typeByte}
	out = append(out, bsonCstring(key)...)
	return append(out, body...)
}

// document wraps elements with the computed length and the terminator.
func bsonDocument(elements ...[]byte) []byte {
	body := bytes.Join(elements, nil)
	out := bsonLe32(int32(len(body) + 5))
	out = append(out, body...)
	return append(out, 0)
}

func bsonInt32Field(key string, number int32) []byte { return bsonField(0x10, key, bsonLe32(number)) }

func bsonDoubleBytes(bits uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, bits)
	return out
}

// withLength replaces the leading length of a document's bytes.
func bsonWithLength(raw []byte, length int32) []byte {
	out := append([]byte{}, raw...)
	copy(out, bsonLe32(length))
	return out
}

// nestedBytes is the wire form of nestedDocuments or nestedArrays at a depth,
// built from bytes so the depth does not depend on the encoder.
func bsonNestedBytes(depth int, arrays bool) []byte {
	if !arrays {
		chain := bsonDocument()
		for level := 2; level <= depth; level++ {
			chain = bsonDocument(bsonField(0x03, "a", chain))
		}
		return chain
	}
	innermost := bsonDocument()
	for level := depth - 1; level >= 2; level-- {
		innermost = bsonDocument(bsonField(0x04, "0", innermost))
	}
	return bsonDocument(bsonField(0x04, "a", innermost))
}

// ---- decode vectors --------------------------------------------------------

type decodeCase struct {
	name  string
	notes string
	raw   []byte
}

func bsonDecodeCases() []decodeCase {
	return []decodeCase{
		{"decode/duplicate-keys", "a repeated key: the server's ordered decoder keeps both; into a map the last wins (last_wins_document). A decoder returning a mapping must take the last value.",
			bsonDocument(bsonInt32Field("a", 1), bsonInt32Field("b", 2), bsonInt32Field("a", 3))},
		{"decode/array-keys-out-of-order", "array element keys are ignored on read: the elements come back in wire order whatever their keys say.",
			bsonDocument(bsonField(0x04, "a", bsonDocument(bsonInt32Field("1", 10), bsonInt32Field("0", 20))))},
		{"decode/array-keys-non-numeric", "keys that are not indexes are ignored too.",
			bsonDocument(bsonField(0x04, "a", bsonDocument(bsonInt32Field("x", 1), bsonInt32Field("y", 2))))},
		{"decode/array-keys-gap", "keys 0 and 2: positions are wire order, not the keys.",
			bsonDocument(bsonField(0x04, "a", bsonDocument(bsonInt32Field("0", 1), bsonInt32Field("2", 2))))},
		{"decode/double-nan-payload", "a NaN with a payload (0x7ff8000000000001, what Go's math.NaN writes) reads as NaN.",
			bsonDocument(bsonField(0x01, "a", bsonDoubleBytes(0x7ff8000000000001)))},
		{"decode/double-negative-nan", "the sign bit set (0xfff8000000000000) is still NaN.",
			bsonDocument(bsonField(0x01, "a", bsonDoubleBytes(0xfff8000000000000)))},
		{"decode/double-signaling-nan", "a signaling NaN (0x7ff0000000000001) is NaN.",
			bsonDocument(bsonField(0x01, "a", bsonDoubleBytes(0x7ff0000000000001)))},
		{"decode/boolean-0-and-1", "the only two legal boolean bytes.",
			bsonDocument(bsonField(0x08, "f", []byte{0}), bsonField(0x08, "t", []byte{1}))},
		{"decode/binary-subtype-2-old", "the deprecated old binary subtype carries an inner length; the decoder returns the payload without it.",
			bsonDocument(bsonField(0x05, "a", append(append(bsonLe32(8), 0x02), append(bsonLe32(4), 1, 2, 3, 4)...)))},
		{"decode/binary-subtype-4-wrong-length", "a UUID subtype with 15 bytes: the server's decoder takes any length. driver-python-spec section 7.2 is silent; a Python decoder must not raise a language-level exception (return Opaque).",
			bsonDocument(bsonField(0x05, "a", append(append(bsonLe32(15), 0x04), bytes.Repeat([]byte{7}, 15)...)))},
		{"decode/regex-options-unsorted", "options are kept as written.",
			bsonDocument(bsonField(0x0b, "a", append(bsonCstring("p"), bsonCstring("xi")...)))},
		{"decode/int32-negative", "two's complement.",
			bsonDocument(bsonField(0x10, "a", []byte{0xfe, 0xff, 0xff, 0xff}))},
		{"decode/string-nul-in-value", "a NUL inside a string value is data.",
			bsonDocument(bsonField(0x02, "a", bsonLengthPrefixed([]byte("x\x00y"))))},
	}
}

func bsonDecodeVectors() ([]bsonVector, error) {
	cases := bsonDecodeCases()
	vectors := make([]bsonVector, 0, len(cases))
	for _, entry := range cases {
		decoded, err := serverDecode(entry.raw)
		if err != nil {
			return nil, fmt.Errorf("vector %q: the server's decoder refuses a decode vector: %w", entry.name, err)
		}
		typed, err := documentToTyped(decoded)
		if err != nil {
			return nil, fmt.Errorf("vector %q: %w", entry.name, err)
		}
		reencoded, err := bson.Marshal(decoded)
		if err != nil {
			return nil, fmt.Errorf("vector %q: re-encoding: %w", entry.name, err)
		}
		vector := bsonVector{
			Name: entry.name, Group: "decode", Kind: "decode", Notes: entry.notes,
			Document: typed, Hex: bsonHexOf(entry.raw), ReencodesIdentically: boolPointer(bytes.Equal(reencoded, entry.raw)),
		}
		if entry.name == "decode/duplicate-keys" {
			collapsed, err := bsonLastWins(entry.raw, decoded)
			if err != nil {
				return nil, err
			}
			vector.LastWinsDocument = collapsed
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

// lastWins keeps the position of a key's first appearance and the value of its
// last, and checks the server's own map decoding agrees on every value.
func bsonLastWins(raw []byte, ordered bson.D) (json.RawMessage, error) {
	var collapsed bson.D
	position := map[string]int{}
	for _, entry := range ordered {
		if at, seen := position[entry.Key]; seen {
			collapsed[at].Value = entry.Value
			continue
		}
		position[entry.Key] = len(collapsed)
		collapsed = append(collapsed, entry)
	}
	var mapped bson.M
	if err := bson.Unmarshal(raw, &mapped); err != nil {
		return nil, fmt.Errorf("decoding into a map: %w", err)
	}
	for _, entry := range collapsed {
		if fmt.Sprint(mapped[entry.Key]) != fmt.Sprint(entry.Value) {
			return nil, fmt.Errorf("the server's map decoding keeps %v for %q, the last-wins rule says %v", mapped[entry.Key], entry.Key, entry.Value)
		}
	}
	return documentToTyped(collapsed)
}

// ---- reject vectors --------------------------------------------------------

type rejectCase struct {
	name   string
	reason string
	notes  string
	raw    []byte
}

func bsonRejectCases() []rejectCase {
	valid := bsonDocument(bsonInt32Field("a", 1))
	nestedDoc := func(inner []byte) []byte { return bsonDocument(bsonField(0x03, "a", inner)) }
	truncatedBody := func(typeByte byte, bodyBytes int) []byte {
		return bsonDocument(bsonField(typeByte, "a", bytes.Repeat([]byte{1}, bodyBytes)))
	}
	badString := func(text []byte) []byte { return bsonDocument(bsonField(0x02, "a", bsonLengthPrefixed(text))) }
	stringWithLength := func(declared int32, text []byte) []byte {
		return bsonDocument(bsonField(0x02, "a", append(bsonLe32(declared), text...)))
	}
	innerNoTerminator := append(append([]byte{}, bsonDocument(bsonInt32Field("b", 1))[:11]...), 0x01)

	return []rejectCase{
		{"reject/empty-input", "empty", "no bytes at all.", []byte{}},
		{"reject/truncated-header", "truncated_header", "three bytes: the length prefix itself is cut.", []byte{0x05, 0x00, 0x00}},
		{"reject/length-zero", "length_below_minimum", "declared length 0.", append(bsonLe32(0), 0)},
		{"reject/length-4", "length_below_minimum", "declared length 4 (the smallest document is 5).", append(bsonLe32(4), 0)},
		{"reject/length-negative", "length_negative", "declared length -1 (0xffffffff).", append(bsonLe32(-1), 0)},
		{"reject/length-int32-min", "length_negative", "declared length -2^31.", append(bsonLe32(math.MinInt32), 0)},
		{"reject/length-too-long", "length_exceeds_buffer", "the declared length is one more than the bytes given.", bsonWithLength(valid, int32(len(valid))+1)},
		{"reject/length-huge", "length_exceeds_buffer", "declared length 2^31-1 over a 12-byte buffer: a decoder must not allocate or read that much.", bsonWithLength(valid, math.MaxInt32)},
		{"reject/length-too-short", "length_below_consumed", "the declared length ends before the last element's body does.", bsonWithLength(valid, int32(len(valid))-1)},
		{"reject/trailing-garbage", "trailing_bytes", "a complete document followed by one byte: the declared length must equal the bytes consumed.", append(append([]byte{}, valid...), 0xff)},
		{"reject/trailing-zero", "trailing_bytes", "a complete document followed by a second terminator.", append(append([]byte{}, valid...), 0x00)},
		{"reject/two-documents", "trailing_bytes", "two complete documents back to back.", append(append([]byte{}, valid...), valid...)},
		{"reject/missing-terminator", "missing_terminator", "the last byte is 0x01, not the terminator, with a consistent length.", append(append([]byte{}, valid[:len(valid)-1]...), 0x01)},
		{"reject/missing-terminator-header-only", "missing_terminator", "length 5 and a non-zero last byte.", append(bsonLe32(5), 1)},
		{"reject/int32-truncated", "element_truncated", "an int32 element with a 3-byte body, length consistent.", truncatedBody(0x10, 3)},
		{"reject/int64-truncated", "element_truncated", "an int64 element with a 7-byte body.", truncatedBody(0x12, 7)},
		{"reject/double-truncated", "element_truncated", "a double element with a 7-byte body.", truncatedBody(0x01, 7)},
		{"reject/datetime-truncated", "element_truncated", "a datetime element with a 7-byte body.", truncatedBody(0x09, 7)},
		{"reject/timestamp-truncated", "element_truncated", "a timestamp element with a 7-byte body.", truncatedBody(0x11, 7)},
		{"reject/objectid-truncated", "element_truncated", "an object id with an 11-byte body.", truncatedBody(0x07, 11)},
		{"reject/decimal128-truncated", "element_truncated", "a decimal128 with a 15-byte body.", truncatedBody(0x13, 15)},
		{"reject/boolean-truncated", "element_truncated", "a boolean with no body.", truncatedBody(0x08, 0)},
		{"reject/boolean-byte-2", "invalid_boolean", "a boolean whose byte is 2: only 0 and 1 are legal.", bsonDocument(bsonField(0x08, "a", []byte{2}))},
		{"reject/unknown-type-0x20", "unknown_type", "type byte 0x20 is not a BSON type, so the element has no length. driver-python-spec section 7.2 does not list it; refuse it.", bsonDocument(bsonField(0x20, "a", []byte{0}))},
		{"reject/unknown-type-0x14", "unknown_type", "type byte 0x14 is not a BSON type.", bsonDocument(bsonField(0x14, "a", []byte{0}))},
		{"reject/key-unterminated", "key_unterminated", "an element key with no NUL before the end of the document.", append(append(bsonLe32(8), 0x10, 'a', 'b'), 0x00)},
		{"reject/string-length-zero", "string_length_invalid", "a string declared with length 0 (the minimum is 1, the NUL).", stringWithLength(0, nil)},
		{"reject/string-length-negative", "string_length_invalid", "a string declared with length -1.", stringWithLength(-1, nil)},
		{"reject/string-length-beyond-document", "string_length_invalid", "a string declared far longer than the document.", stringWithLength(1000, []byte("a\x00"))},
		{"reject/string-missing-nul", "string_unterminated", "a string whose last counted byte is not NUL.", stringWithLength(2, []byte("ab"))},
		{"reject/string-invalid-utf8", "invalid_utf8", "a string value holding the byte 0xff.", badString([]byte{0xff})},
		{"reject/string-overlong-utf8", "invalid_utf8", "an overlong encoding of NUL (0xc0 0x80).", badString([]byte{0xc0, 0x80})},
		{"reject/string-truncated-utf8", "invalid_utf8", "a three-byte sequence cut after two bytes.", badString([]byte{0xe2, 0x82})},
		{"reject/string-lone-surrogate", "invalid_utf8", "a UTF-16 surrogate encoded in three bytes (0xed 0xa0 0x80): not valid UTF-8.", badString([]byte{0xed, 0xa0, 0x80})},
		{"reject/string-above-max-codepoint", "invalid_utf8", "0xf4 0x90 0x80 0x80 is a code point above U+10FFFF.", badString([]byte{0xf4, 0x90, 0x80, 0x80})},
		{"reject/key-invalid-utf8", "invalid_utf8", "an element key holding the byte 0xff.", bsonDocument(bsonField(0x10, "\xff", bsonLe32(1)))},
		{"reject/javascript-invalid-utf8", "invalid_utf8", "a JavaScript element whose code is not UTF-8.", bsonDocument(bsonField(0x0d, "a", bsonLengthPrefixed([]byte{0xff})))},
		{"reject/binary-length-beyond-document", "binary_length_invalid", "a binary declaring 1000 bytes in a short document.", bsonDocument(bsonField(0x05, "a", append(bsonLe32(1000), 0x00, 0x01)))},
		{"reject/binary-length-negative", "binary_length_invalid", "a binary declaring length -1.", bsonDocument(bsonField(0x05, "a", append(bsonLe32(-1), 0x00)))},
		{"reject/binary-subtype-2-bad-inner-length", "binary_length_invalid", "old binary (subtype 2) whose inner length does not match.", bsonDocument(bsonField(0x05, "a", append(append(bsonLe32(8), 0x02), append(bsonLe32(99), 1, 2, 3, 4)...)))},
		{"reject/regex-pattern-unterminated", "cstring_unterminated", "a regex whose pattern has no NUL before the end.", append(append(bsonLe32(10), 0x0b, 'a', 0, 'p', 'q'), 0x00)},
		{"reject/dbpointer-truncated", "element_truncated", "a DBPointer with its object id cut.", bsonDocument(bsonField(0x0c, "a", append(bsonLengthPrefixed([]byte("ns")), 1, 2, 3)))},
		{"reject/nested-length-too-long", "length_exceeds_buffer", "an embedded document declaring more bytes than the parent holds.", nestedDoc(bsonWithLength(valid, int32(len(valid))+10))},
		{"reject/nested-length-too-short", "length_below_consumed", "an embedded document declaring fewer bytes than its elements take.", nestedDoc(bsonWithLength(valid, int32(len(valid))-1))},
		{"reject/nested-length-lie", "length_mismatch", "an embedded document whose declared length is 6 over a 12-byte body.", nestedDoc(bsonWithLength(valid, 6))},
		{"reject/nested-missing-terminator", "missing_terminator", "an embedded document whose last byte is not the terminator.", nestedDoc(innerNoTerminator)},
		{"reject/nested-trailing-garbage", "trailing_bytes", "an embedded document followed by a stray byte inside the parent.", bsonDocument(bsonField(0x03, "a", valid), []byte{0xff})},
		{"reject/array-length-lie", "length_mismatch", "an array whose declared length is 6 over a 12-byte body.", bsonDocument(bsonField(0x04, "a", bsonWithLength(valid, 6)))},
		{"reject/javascript-with-scope-length-lie", "length_mismatch", "a code-with-scope whose total length disagrees with its parts.", bsonDocument(bsonField(0x0f, "a", append(bsonLe32(999), append(bsonLengthPrefixed([]byte("1")), bsonDocument()...)...)))},
		{"reject/depth-101-documents", "depth_exceeded", "documents nested to depth 101 (the root counts 1): over the limit of 100.", bsonNestedBytes(101, false)},
		{"reject/depth-101-arrays", "depth_exceeded", "arrays nested to depth 101.", bsonNestedBytes(101, true)},
		{"reject/depth-1000-documents", "depth_exceeded", "documents nested to depth 1000: a recursive decoder must stop long before the language's own recursion limit.", bsonNestedBytes(1000, false)},
		{"reject/depth-1000-arrays", "depth_exceeded", "arrays nested to depth 1000.", bsonNestedBytes(1000, true)},
	}
}

func bsonRejectVectors() ([]bsonVector, error) {
	cases := bsonRejectCases()
	vectors := make([]bsonVector, 0, len(cases))
	for _, entry := range cases {
		_, err := serverDecode(entry.raw)
		observed := "refused"
		notes := entry.notes
		if err == nil {
			observed = "accepted"
			notes += " The server accepts this input; the spec is stricter than the server here, and the in-tree codecs follow the spec."
		}
		vectors = append(vectors, bsonVector{
			Name: entry.name, Group: "reject", Kind: "reject", Notes: notes,
			Hex: bsonHexOf(entry.raw), Refusal: "ProtocolError", Reason: entry.reason, ServerDecoder: observed,
		})
	}
	return vectors, nil
}

// ---- encode_refuse ---------------------------------------------------------

func bsonEncodeRefuseVectors() ([]bsonVector, error) {
	document := bson.D{{Key: "a\x00b", Value: int32(1)}}
	typed, err := documentToTyped(document)
	if err != nil {
		return nil, err
	}
	observed := "accepted"
	if _, err := bson.Marshal(document); err != nil {
		observed = "refused"
	}
	notes := "a key may not hold NUL: a key is a BSON cstring, and a NUL would end it early and shift every later byte (driver-python-spec section 7.2)."
	if observed == "accepted" {
		notes += " The server's encoder accepts this input and writes the NUL into the key; the spec is stricter than the server, and the in-tree codecs refuse before writing."
	}
	return []bsonVector{{
		Name: "encode_refuse/key-with-nul", Group: "encode_refuse", Kind: "encode_refuse", Notes: notes,
		Document: typed, Refusal: "InvalidArgumentError", ServerEncoder: observed,
	}}, nil
}
