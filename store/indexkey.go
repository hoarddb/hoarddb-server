package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Key encoding v2 (index-parity-spec §3.4 and §3.4.1, adopted unchanged by
// shared-storage-spec §3.8): typed, order-preserving and self-delimiting. It is
// the store's wire format for btree primary keys and every index entry, so a
// change to it is a data-format change.
//
// This is deliberately not store/btree.go:encodeKey or
// store/index_manager.go:encodeIndexValue: encodeKey writes raw
// Float64bits, which sorts negative floats backwards, and encodeIndexValue
// carries no type tag, so an int64 and an 8-byte string collide.

// Type tags. Order between types is the tag byte: arbitrary but fixed, and
// nothing may rely on it, because every scan comparison is type-bracketed.
// Tags 0x71 to 0x7F are unassigned, and 0x80 to 0xF0 are reserved for the
// types refused below. A new encodable type takes one of the unassigned tags.
const (
	tagNull     byte = storage.TagNull
	tagBool     byte = storage.TagBool
	tagNumber   byte = storage.TagNumber
	tagDateTime byte = storage.TagDateTime
	tagString   byte = storage.TagString
	tagBinary   byte = storage.TagBinary
	// tagObjectID is the seventh `_id_` tag (conventions §5): the tag, then the
	// twelve raw bytes. Its own tag keeps an ObjectID out of binary's range,
	// so an ObjectID and a binary id with the same bytes are two keys.
	tagObjectID byte = storage.TagObjectID
)

// Numeric value classes. They are value ranges, not type ids: every class
// 0x01 value is at most MaxInt64, every class 0x02 value is above it, and a
// class 0x03 value that ties with either on the image is out of uint64 range
// and therefore larger. So the class byte breaks an image tie in value order.
const (
	classSignedInteger   byte = 0x01 // an exact integer in [MinInt64, MaxInt64]
	classUnsignedInteger byte = 0x02 // an exact integer in (MaxInt64, MaxUint64]
	classOther           byte = 0x03 // non-integral, or outside uint64's range
)

// supportedKeyTypes is the one list of types EncodeComponent accepts, as a
// developer reads it in a refusal. Both refusals (the encoder's and the index
// write path's) name it, so the two cannot disagree about which types work.
const supportedKeyTypes = "null, bool, number, date, string, binary, objectid"

// ErrUnencodableKeyType is returned for a value with no tag: a decimal128, an
// array or a sub-document. Encoding one without a defined
// order would give a btree whose order nothing can explain.
var ErrUnencodableKeyType = errors.New("value type cannot be a btree key or an indexed field")

// ErrNonFiniteNumber is returned for NaN and ±Inf. NaN has no place in a total
// order — a scan matches it with no ordered comparison — so no index order
// could agree with a scan; ±Inf is refused with it for one rule, not two.
var ErrNonFiniteNumber = errors.New("NaN and infinite numbers cannot be a btree key or an indexed field")

// EncodeComponent encodes one value as tag(1) ‖ payload.
func EncodeComponent(v interface{}) ([]byte, error) {
	switch typed := v.(type) {
	case nil, primitive.Null:
		return []byte{tagNull}, nil
	case bool:
		if typed {
			return []byte{tagBool, 0x01}, nil
		}
		return []byte{tagBool, 0x00}, nil
	// Dates come before numbers: primitive.DateTime is an int64 underneath,
	// and the scan path never compares a date with a number.
	case primitive.DateTime:
		return encodeDateTime(int64(typed)), nil
	case time.Time:
		return encodeDateTime(typed.UnixMilli()), nil
	case string:
		return appendEscaped([]byte{tagString}, []byte(typed)), nil
	case []byte:
		return appendEscaped([]byte{tagBinary}, typed), nil
	case primitive.Binary:
		return appendEscaped([]byte{tagBinary}, typed.Data), nil
	case primitive.ObjectID:
		return append([]byte{tagObjectID}, typed[:]...), nil
	}
	if encoded, ok, err := encodeNumber(v); ok {
		return encoded, err
	}
	return nil, fmt.Errorf("%w: %T (supported: %s)", ErrUnencodableKeyType, v, supportedKeyTypes)
}

// EncodeKey encodes values in order and concatenates them with no separator.
// Fixed-length payloads are not terminated and variable-length ones are, so
// no value can inject a separator.
func EncodeKey(values []interface{}) ([]byte, error) {
	var key []byte
	for position, v := range values {
		component, err := EncodeComponent(v)
		if err != nil {
			return nil, fmt.Errorf("key component %d: %w", position, err)
		}
		key = append(key, component...)
	}
	return key, nil
}

// EncodeComponentDesc is EncodeComponent with every byte inverted: the same
// prefix-free code in the reverse order, so a descending component sorts
// bytewise in its declared order (ordered-index spec §3.1). The tag is inverted
// with the payload, which is what keeps null sorting last.
func EncodeComponentDesc(v interface{}) ([]byte, error) {
	encoded, err := EncodeComponent(v)
	if err != nil {
		return nil, fmt.Errorf("descending component: %w", err)
	}
	return invertBytes(encoded), nil
}

// invertBytes returns b with every byte XOR 0xFF; it never aliases b.
func invertBytes(b []byte) []byte {
	inverted := make([]byte, len(b))
	for i, c := range b {
		inverted[i] = c ^ 0xFF
	}
	return inverted
}

// EncodedComponentLen returns the length of the one component that body begins.
// The grammar lives in storage so cluster can step over a component without
// importing this package (store imports cluster/machine, which imports cluster).
func EncodedComponentLen(body []byte) (int, error) {
	return storage.EncodedComponentLen(body)
}

// encodeDateTime writes milliseconds as a sign-flipped int64, so earlier
// instants sort first, matching the scan path's date comparison.
func encodeDateTime(millis int64) []byte {
	encoded := make([]byte, 9)
	encoded[0] = tagDateTime
	binary.BigEndian.PutUint64(encoded[1:], uint64(millis)^(1<<63))
	return encoded
}

// appendEscaped appends raw with every 0x00 written as 0x00 0xFF, then the
// terminator 0x00 0x00. Byte order is preserved: a prefix sorts first, since
// its terminator's 0x00 0x00 is below any continuation.
func appendEscaped(dst, raw []byte) []byte {
	for _, b := range raw {
		if b == 0x00 {
			dst = append(dst, 0x00, 0xFF)
			continue
		}
		dst = append(dst, b)
	}
	return append(dst, 0x00, 0x00)
}

// encodeNumber encodes every numeric kind the scan path's toFloat64 accepts —
// int, int8…int64, uint…uint64, float32, float64 — as
// 0x30 ‖ imageBits(8) ‖ class(1) ‖ payload(8). ok is false for a non-number.
func encodeNumber(v interface{}) (encoded []byte, ok bool, err error) {
	reflected := reflect.ValueOf(v)
	var asFloat float64
	var class byte
	var payload uint64
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		signed := reflected.Int()
		asFloat = float64(signed)
		class, payload = classSignedInteger, uint64(signed)^(1<<63)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned := reflected.Uint()
		asFloat = float64(unsigned)
		class, payload = classSignedInteger, unsigned^(1<<63)
		if unsigned > math.MaxInt64 {
			class, payload = classUnsignedInteger, unsigned
		}
	case reflect.Float32, reflect.Float64:
		asFloat = reflected.Float()
		if math.IsNaN(asFloat) || math.IsInf(asFloat, 0) {
			return nil, true, fmt.Errorf("%w: %v", ErrNonFiniteNumber, asFloat)
		}
		class, payload = floatClass(asFloat)
	default:
		return nil, false, nil
	}

	image := orderedImage(asFloat)
	if class == classOther {
		payload = image
	}
	encoded = make([]byte, 18)
	encoded[0] = tagNumber
	binary.BigEndian.PutUint64(encoded[1:9], image)
	encoded[9] = class
	binary.BigEndian.PutUint64(encoded[10:18], payload)
	return encoded, true, nil
}

// floatClass classifies a finite float by the exact value it holds.
func floatClass(f float64) (byte, uint64) {
	if f != math.Trunc(f) {
		return classOther, 0
	}
	switch {
	case f >= -(1<<63) && f < 1<<63:
		return classSignedInteger, uint64(int64(f)) ^ (1 << 63)
	case f >= 1<<63 && f < 1<<64:
		return classUnsignedInteger, uint64(f)
	default:
		return classOther, 0
	}
}

// orderedImage maps a float64 to a uint64 whose unsigned order is the float's
// numeric order: a negative float has every bit inverted, any other has its
// sign bit set. -0.0 is folded into +0.0 first; the spec's formula applied to
// -0.0 as-is gives a different image from 0.0, which would contradict its own
// rule that -0.0, 0.0 and int64(0) are one key.
func orderedImage(f float64) uint64 {
	if f == 0 {
		f = 0
	}
	bits := math.Float64bits(f)
	if bits>>63 == 1 {
		return ^bits
	}
	return bits | 1<<63
}
