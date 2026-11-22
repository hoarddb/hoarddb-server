package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

func mustEncode(t *testing.T, v interface{}) []byte {
	t.Helper()
	encoded, err := EncodeComponent(v)
	if err != nil {
		t.Fatalf("EncodeComponent(%T %v): %v", v, v, err)
	}
	return encoded
}

// TestNumericKeyIsTheMathematicalValue: every spelling of one number is one
// key, and two different numbers are two keys. The CLI types every literal as
// float64 while a BSON document delivers int32 or int64, so a key that broke
// ties by Go type would miss every document an ingest wrote.
func TestNumericKeyIsTheMathematicalValue(t *testing.T) {
	cases := []struct {
		name      string
		spellings []interface{}
	}{
		{"thirty", []interface{}{int(30), int8(30), int16(30), int32(30), int64(30), uint(30), uint8(30), uint16(30), uint32(30), uint64(30), float32(30), float64(30)}},
		{"minus seven", []interface{}{int(-7), int8(-7), int16(-7), int32(-7), int64(-7), float32(-7), float64(-7)}},
		{"two to the 53rd", []interface{}{int64(1 << 53), uint64(1 << 53), float64(1 << 53)}},
		{"MaxInt64", []interface{}{int64(math.MaxInt64), uint64(math.MaxInt64)}},
		{"two to the 63rd", []interface{}{uint64(1 << 63), float64(1 << 63)}},
		{"MinInt64", []interface{}{int64(math.MinInt64), float64(math.MinInt64)}},
		{"one and a half", []interface{}{float32(1.5), float64(1.5)}},
	}
	for _, tc := range cases {
		want := mustEncode(t, tc.spellings[0])
		for _, spelling := range tc.spellings[1:] {
			if got := mustEncode(t, spelling); !bytes.Equal(got, want) {
				t.Fatalf("%s: %T encodes to %x, but %T to %x: one value, two keys", tc.name, spelling, got, tc.spellings[0], want)
			}
		}
	}

	distinct := []struct {
		name        string
		left, right interface{}
	}{
		{"2^53 and 2^53+1 as int64", int64(1 << 53), int64(1<<53 + 1)},
		{"MaxUint64 and 2^64 as float64", uint64(math.MaxUint64), float64(1 << 64)},
		{"0.1 and its float32 spelling", float64(0.1), float32(0.1)},
	}
	for _, tc := range distinct {
		if bytes.Equal(mustEncode(t, tc.left), mustEncode(t, tc.right)) {
			t.Fatalf("%s: two different values share a key", tc.name)
		}
	}
}

func TestNegativeZeroAndZeroAreOneKey(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	want := mustEncode(t, int64(0))
	for _, zero := range []interface{}{negativeZero, float64(0), float32(negativeZero), uint64(0)} {
		if got := mustEncode(t, zero); !bytes.Equal(got, want) {
			t.Fatalf("%T %v encodes to %x, want int64(0)'s %x", zero, zero, got, want)
		}
	}
}

// TestCrossTypeNumericOrderMatchesValueOrder: a class byte that were a type id
// would put uint64(2^53) above int64(2^53+1).
func TestCrossTypeNumericOrderMatchesValueOrder(t *testing.T) {
	ascending := []interface{}{
		float64(-1e300),
		int64(math.MinInt64),
		int32(-7),
		float64(-0.5),
		uint8(0),
		float32(0.5),
		// uint64(2^53) and int64(2^53+1) share a float64 image, so only the
		// class byte orders them — and a type-id class puts uint64 above.
		uint64(1 << 53),
		int64(1<<53 + 1),
		uint64(1<<53 + 2),
		int64(math.MaxInt64),
		uint64(1 << 63),
		uint64(math.MaxUint64),
		float64(1 << 64),
		float64(1e300),
	}
	for i := 1; i < len(ascending); i++ {
		lower, higher := mustEncode(t, ascending[i-1]), mustEncode(t, ascending[i])
		if bytes.Compare(lower, higher) >= 0 {
			t.Fatalf("%T %v must sort below %T %v, but its key %x does not sort below %x",
				ascending[i-1], ascending[i-1], ascending[i], ascending[i], lower, higher)
		}
	}
}

func TestNegativeFloatsSortBelowPositives(t *testing.T) {
	ascending := []float64{-1e10, -2.5, -1, -0.5, -1e-300, 0, 1e-300, 0.5, 1, 2.5, 1e10}
	for i := 1; i < len(ascending); i++ {
		if bytes.Compare(mustEncode(t, ascending[i-1]), mustEncode(t, ascending[i])) >= 0 {
			t.Fatalf("%v does not sort below %v", ascending[i-1], ascending[i])
		}
	}
}

func TestCompoundKeyEncodingIsUnambiguous(t *testing.T) {
	pairs := [][2][]interface{}{
		{{"ab", "c"}, {"a", "bc"}},
		{{"a\x00", "b"}, {"a", "\x00b"}},
		{{[]byte{0x00}, int64(1)}, {[]byte{}, []byte{0x00}}},
	}
	for _, pair := range pairs {
		left, err := EncodeKey(pair[0])
		if err != nil {
			t.Fatalf("EncodeKey(%q): %v", pair[0], err)
		}
		right, err := EncodeKey(pair[1])
		if err != nil {
			t.Fatalf("EncodeKey(%q): %v", pair[1], err)
		}
		if bytes.Equal(left, right) {
			t.Fatalf("%q and %q encode to the same compound key %x", pair[0], pair[1], left)
		}
	}
}

func TestNaNAndInfAreRefusedByTheEncoder(t *testing.T) {
	for _, refused := range []interface{}{math.NaN(), math.Inf(1), math.Inf(-1), float32(math.NaN()), float32(math.Inf(1))} {
		encoded, err := EncodeComponent(refused)
		if !errors.Is(err, ErrNonFiniteNumber) {
			t.Fatalf("EncodeComponent(%T %v) = %x, %v, want ErrNonFiniteNumber", refused, refused, encoded, err)
		}
	}
	if _, err := EncodeKey([]interface{}{"ok", math.NaN()}); !errors.Is(err, ErrNonFiniteNumber) {
		t.Fatalf("EncodeKey with a NaN component returned %v, want ErrNonFiniteNumber", err)
	}
}

// TestCrossTypeKeysAreDistinct is spec T-23(a), encoder half.
func TestCrossTypeKeysAreDistinct(t *testing.T) {
	keys := map[string]interface{}{"int64(1)": int64(1), `"1"`: "1", "true": true}
	seen := map[string]string{}
	for name, v := range keys {
		encoded := string(mustEncode(t, v))
		if other, clash := seen[encoded]; clash {
			t.Fatalf("%s and %s encode to the same key %x", name, other, encoded)
		}
		seen[encoded] = name
	}
}

// TestUntaggedTypesAreRefused: a type with no tag is refused, never encoded
// some other way.
func TestUntaggedTypesAreRefused(t *testing.T) {
	decimal, err := primitive.ParseDecimal128("1.5")
	if err != nil {
		t.Fatalf("building a decimal128: %v", err)
	}
	for _, refused := range []interface{}{decimal, []interface{}{1}, map[string]interface{}{"a": 1}, primitive.A{1}, primitive.D{{Key: "a", Value: 1}}} {
		if _, err := EncodeComponent(refused); !errors.Is(err, ErrUnencodableKeyType) {
			t.Fatalf("EncodeComponent(%T) returned %v, want ErrUnencodableKeyType", refused, err)
		}
	}
	// The refusal names every type the encoder takes, so a developer who hits it
	// at 3 a.m. can see that an ObjectID is one of them.
	_, err = EncodeComponent(decimal)
	if want := "supported: null, bool, number, date, string, binary, objectid"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("EncodeComponent(decimal128) error = %v, want it to contain %q", err, want)
	}
}

// TestEncodedComponentLenMatchesThePrefix: one component's length, read from
// its tag, is exactly the bytes the encoder wrote, with another component's
// bytes after it. A length that ignores the body passes a lone component and
// fails as soon as a neighbour follows, which is the situation the retired-field
// sweep and the list cursor are in. The ObjectID body is built by hand so this
// test checks the length grammar, not the encoder.
func TestEncodedComponentLenMatchesThePrefix(t *testing.T) {
	trailer := []byte{0xAB, 0x00, 0x00, 0x50, 0x00}
	values := []interface{}{
		nil, true, false, int64(7), float64(-1.5), uint64(1 << 63),
		primitive.DateTime(1700000000000), "plain", "a\x00b", "\x00\x00", "",
		[]byte{0x00, 0x01, 0x00}, primitive.Binary{Data: []byte("bin\x00")},
	}
	components := make([][]byte, 0, len(values)+1)
	for _, v := range values {
		components = append(components, mustEncode(t, v))
	}
	oid := primitive.ObjectID{0x65, 0x4a, 0x1f, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x00}
	components = append(components, append([]byte{0x70}, oid[:]...))

	for _, component := range components {
		body := append(append([]byte{}, component...), trailer...)
		got, err := EncodedComponentLen(body)
		if err != nil {
			t.Fatalf("EncodedComponentLen(%x) returned %v", body, err)
		}
		if got != len(component) {
			t.Fatalf("EncodedComponentLen(%x) = %d, want %d: the component is %x", body, got, len(component), component)
		}
	}

	for _, malformed := range [][]byte{nil, {0x99, 0x00, 0x00}, {tagString, 'a'}, {tagNumber, 0x01}, {tagBinary, 0x00, 0x42}} {
		if got, err := EncodedComponentLen(malformed); err == nil {
			t.Fatalf("EncodedComponentLen(%x) = %d with no error, want an error", malformed, got)
		}
	}
}

// TestObjectIDIsATag: an ObjectID is its own `_id_` type, tag 0x70 followed by
// its twelve raw bytes (conventions §5). Encoded as binary it would share the
// 0x60 range with a user's binary id and route to another slot.
func TestObjectIDIsATag(t *testing.T) {
	oid := primitive.ObjectID{0x65, 0x4a, 0x1f, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x00}
	encoded := mustEncode(t, oid)
	if want := append([]byte{0x70}, oid[:]...); !bytes.Equal(encoded, want) {
		t.Fatalf("ObjectID encodes to %x, want 70 then its 12 raw bytes %x", encoded, want)
	}
}

// TestBTreeKeyAcceptsObjectID: a btree keyed by an ObjectID stores the document
// and reads it back. The btree encodes its key with EncodeComponent, so the
// refusal goes with the encoder's.
func TestBTreeKeyAcceptsObjectID(t *testing.T) {
	bt := NewBTreeStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
	oid := primitive.NewObjectID()
	if err := bt.Put(ApplyInfo{}, oid, map[string]interface{}{"n": 1}, 0); err != nil {
		t.Fatalf("Put with an ObjectID key: %v", err)
	}
	value, err := bt.Get(oid)
	if err != nil || len(value) == 0 {
		t.Fatalf("Get with an ObjectID key returned %x, %v", value, err)
	}
	if n, err := bt.Len(); err != nil || n != 1 {
		t.Fatalf("Len after one ObjectID Put = %d (%v), want 1", n, err)
	}
}

type keyVector struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	Hex   string `json:"hex"`
}

// vectorValue rebuilds the Go value a vector row names.
func vectorValue(t *testing.T, vector keyVector) interface{} {
	t.Helper()
	parseInt := func(bits int) int64 {
		parsed, err := strconv.ParseInt(vector.Value, 10, bits)
		if err != nil {
			t.Fatalf("vector %s %q: %v", vector.Type, vector.Value, err)
		}
		return parsed
	}
	parseUint := func(bits int) uint64 {
		parsed, err := strconv.ParseUint(vector.Value, 10, bits)
		if err != nil {
			t.Fatalf("vector %s %q: %v", vector.Type, vector.Value, err)
		}
		return parsed
	}
	parseFloat := func(bits int) float64 {
		parsed, err := strconv.ParseFloat(vector.Value, bits)
		if err != nil {
			t.Fatalf("vector %s %q: %v", vector.Type, vector.Value, err)
		}
		return parsed
	}
	parseHex := func() []byte {
		raw, err := hex.DecodeString(vector.Value)
		if err != nil {
			t.Fatalf("vector %s %q: %v", vector.Type, vector.Value, err)
		}
		return raw
	}
	switch vector.Type {
	case "int":
		return int(parseInt(64))
	case "int8":
		return int8(parseInt(8))
	case "int16":
		return int16(parseInt(16))
	case "int32":
		return int32(parseInt(32))
	case "int64":
		return parseInt(64)
	case "uint":
		return uint(parseUint(64))
	case "uint8":
		return uint8(parseUint(8))
	case "uint16":
		return uint16(parseUint(16))
	case "uint32":
		return uint32(parseUint(32))
	case "uint64":
		return parseUint(64)
	case "float32":
		return float32(parseFloat(32))
	case "float64":
		return parseFloat(64)
	case "datetime":
		return primitive.DateTime(parseInt(64))
	case "time":
		return time.UnixMilli(parseInt(64)).UTC()
	case "string":
		return string(parseHex())
	case "binary":
		return parseHex()
	case "bool":
		return vector.Value == "true"
	case "null":
		return nil
	case "objectid":
		raw := parseHex()
		var oid primitive.ObjectID
		if len(raw) != len(oid) {
			t.Fatalf("vector objectid %q is %d bytes, want %d", vector.Value, len(raw), len(oid))
		}
		copy(oid[:], raw)
		return oid
	}
	t.Fatalf("vector type %q is not known to the test", vector.Type)
	return nil
}

// TestFrozenKeyVectors pins the encoding byte for byte. The vectors were
// computed once from the spec's formula by a separate program, not by this
// encoder, and there is no -update flag.
func TestFrozenKeyVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "index-key-vectors.json"))
	if err != nil {
		t.Fatalf("reading the key vectors: %v", err)
	}
	var file struct {
		Vectors []keyVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decoding the key vectors: %v", err)
	}
	covered := map[string]bool{}
	coveredTags := map[byte]bool{}
	for _, vector := range file.Vectors {
		encoded, err := EncodeComponent(vectorValue(t, vector))
		if err != nil {
			t.Errorf("%s %s: %v", vector.Type, vector.Value, err)
			continue
		}
		if got := hex.EncodeToString(encoded); got != vector.Hex {
			t.Errorf("%s %s encodes to %s, want %s", vector.Type, vector.Value, got, vector.Hex)
		}
		covered[vector.Type] = true
		coveredTags[encoded[0]] = true
	}
	for _, spelling := range []string{"int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "datetime", "time", "string", "binary", "bool", "null", "objectid"} {
		if !covered[spelling] {
			t.Errorf("the vector file has no %s row", spelling)
		}
	}
	// Every tag the encoder writes needs a row whose encoding starts with it, so
	// a new tag cannot ship without a frozen vector for its bytes.
	for _, tag := range []byte{tagNull, tagBool, tagNumber, tagDateTime, tagString, tagBinary, tagObjectID} {
		if !coveredTags[tag] {
			t.Errorf("the vector file has no row whose encoding starts with tag 0x%02x", tag)
		}
	}
}

// TestDescEncodingReversesOrder: a descending component sorts bytewise in the
// reverse of its ascending order, for every type the ascending encoder takes.
// Inverting only the payload would leave the tag in place, and null (tag 0x10)
// would then sort first descending instead of last.
func TestDescEncodingReversesOrder(t *testing.T) {
	values := []interface{}{nil, false, true, int64(-3), 0.0, int64(7), primitive.DateTime(0), primitive.DateTime(1700000000000),
		"", "a", "ab", "b", []byte{0x00}, []byte{0x00, 0x01}, primitive.NewObjectIDFromTimestamp(time.Unix(0, 0))}
	var asc, desc [][]byte
	for _, v := range values {
		a, err := EncodeComponent(v)
		if err != nil {
			t.Fatalf("EncodeComponent(%v): %v", v, err)
		}
		d, err := EncodeComponentDesc(v)
		if err != nil {
			t.Fatalf("EncodeComponentDesc(%v): %v", v, err)
		}
		asc, desc = append(asc, a), append(desc, d)
	}
	for i := range values {
		for j := range values {
			if got, want := bytes.Compare(desc[i], desc[j]), -bytes.Compare(asc[i], asc[j]); got != want {
				t.Fatalf("desc order of %v vs %v: got %d, want %d", values[i], values[j], got, want)
			}
		}
	}
}

// TestDescEncodingIsPrefixFreeAndMeasurable: a descending component is measured
// by storage.EncodedComponentLenDesc without being un-inverted, and no
// descending encoding is a prefix of another. Measuring with the ascending
// grammar reads the inverted tag as unknown, and a prefix would let one value's
// key be the start of another's.
func TestDescEncodingIsPrefixFreeAndMeasurable(t *testing.T) {
	values := []interface{}{nil, true, int64(-3), 2.5, primitive.DateTime(1700000000000), "", "a\x00b", "\x00\x00",
		[]byte{0xFF, 0x00}, primitive.NewObjectIDFromTimestamp(time.Unix(0, 0))}
	trailer := []byte{0xAB, 0xFF, 0x00, 0x00, 0x50, 0x00, 0xFF}
	var encodings [][]byte
	for _, v := range values {
		encoded, err := EncodeComponentDesc(v)
		if err != nil {
			t.Fatalf("EncodeComponentDesc(%v): %v", v, err)
		}
		body := append(append([]byte{}, encoded...), trailer...)
		got, err := storage.EncodedComponentLenDesc(body)
		if err != nil {
			t.Fatalf("EncodedComponentLenDesc(%x) for %v: %v", body, v, err)
		}
		if got != len(encoded) {
			t.Fatalf("EncodedComponentLenDesc(%x) for %v = %d, want %d", body, v, got, len(encoded))
		}
		encodings = append(encodings, encoded)
	}
	for i := range encodings {
		for j := range encodings {
			if i != j && bytes.HasPrefix(encodings[j], encodings[i]) {
				t.Fatalf("desc encoding of %v is a prefix of %v: %x", values[i], values[j], encodings[i])
			}
		}
	}
}

// TestMixedDirectionKeyOrder: a key with one ascending and one descending
// component sorts by the first ascending, then the second descending, with no
// separator. A separator byte would be a byte a value can inject; the
// terminators of the two directions already make the components self-delimiting.
func TestMixedDirectionKeyOrder(t *testing.T) {
	type entry struct {
		room string
		ts   primitive.DateTime
		key  []byte
	}
	var entries []entry
	for _, room := range []string{"r2", "r1"} {
		for _, ts := range []primitive.DateTime{2000, 3000, 1000} {
			roomKey, err := EncodeComponent(room)
			if err != nil {
				t.Fatalf("EncodeComponent(%q): %v", room, err)
			}
			tsKey, err := EncodeComponentDesc(ts)
			if err != nil {
				t.Fatalf("EncodeComponentDesc(%v): %v", ts, err)
			}
			key := append(roomKey, tsKey...)
			roomLen, err := EncodedComponentLen(key)
			if err != nil {
				t.Fatalf("EncodedComponentLen(%x): %v", key, err)
			}
			tsLen, err := storage.EncodedComponentLenDesc(key[roomLen:])
			if err != nil {
				t.Fatalf("EncodedComponentLenDesc(%x): %v", key[roomLen:], err)
			}
			if roomLen+tsLen != len(key) {
				t.Fatalf("key %x splits into %d+%d bytes, want %d: a separator would leave a stray byte", key, roomLen, tsLen, len(key))
			}
			entries = append(entries, entry{room: room, ts: ts, key: key})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	want := []entry{{room: "r1", ts: 3000}, {room: "r1", ts: 2000}, {room: "r1", ts: 1000},
		{room: "r2", ts: 3000}, {room: "r2", ts: 2000}, {room: "r2", ts: 1000}}
	for i := range want {
		if entries[i].room != want[i].room || entries[i].ts != want[i].ts {
			t.Fatalf("position %d after sorting is (%s, %v), want (%s, %v)", i, entries[i].room, entries[i].ts, want[i].room, want[i].ts)
		}
	}
}
