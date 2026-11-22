package server

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The library's Compare and CompareLiteral stand in for compareSortKeys over
// sortKeyOf at the sort's sites (bson-codec-spec 6.3, build doc step 3c). The
// oracle here is the server's own order applied to the package's decode of the
// same bytes: sortKeyOf(decodeNative(bytes)). It is the pair
// (sortKeyOf, compareSortKeys) in this package. The decode uses the package's
// own BSON import (sort_native_decode_test.go), the type sortKeyOf consumes on
// each tree: the vendor's before the step 6 swap, the library's after. Before
// the swap that decode is the vendor oracle; after it, the ordering oracle is
// the server's rank table and compareSortKeys, and the value decode is covered
// by the codec differentials.
//
// Only the sign is compared: compareSortKeys returns a negative, zero or
// positive int, and the library returns -1, 0 or 1.

// sortValue is one value of the differential: the element bytes that hold it
// under the key "v", or no element for a missing field.
type sortValue struct {
	name    string
	element []byte
}

func le64(word uint64) []byte {
	var out [8]byte
	binary.LittleEndian.PutUint64(out[:], word)
	return out[:]
}

func doubleValue(name string, number float64) sortValue {
	return sortValue{name, elemOf(0x01, "v", le64(math.Float64bits(number))...)}
}

func int32Value(name string, number int32) sortValue {
	return sortValue{name, elemOf(0x10, "v", le32(int(number))...)}
}

func int64Value(name string, number int64) sortValue {
	return sortValue{name, elemOf(0x12, "v", le64(uint64(number))...)}
}

func stringValue(name, text string) sortValue {
	return sortValue{name, elemOf(0x02, "v", bsonString(text)...)}
}

func dateValue(name string, milliseconds int64) sortValue {
	return sortValue{name, elemOf(0x09, "v", le64(uint64(milliseconds))...)}
}

func booleanValue(name string, flag bool) sortValue {
	flagByte := byte(0)
	if flag {
		flagByte = 1
	}
	return sortValue{name, elemOf(0x08, "v", flagByte)}
}

func decimalValue(name string, high, low uint64) sortValue {
	return sortValue{name, elemOf(0x13, "v", append(le64(low), le64(high)...)...)}
}

// sortValues is every value the order must place, grouped by class. The
// groups carry the names the task asks for by name: symbol, JavaScript,
// undefined, timestamp, MinKey and MaxKey, Decimal128, null, -0, NaN, and the
// integers and doubles around 2^53 and 2^63.
func sortValues() []sortValue {
	codeWithScope := func() []byte {
		scope := docOf(int32Element("s", 1))
		body := append(le32(4+len(bsonString("c"))+len(scope)), bsonString("c")...)
		return append(body, scope...)
	}()
	values := []sortValue{
		// missing and null
		{"missing", nil},
		{"null", elemOf(0x0A, "v")},

		// numbers
		int32Value("int32 -5", -5), int32Value("int32 0", 0), int32Value("int32 7", 7),
		int32Value("int32 max", math.MaxInt32), int32Value("int32 min", math.MinInt32),
		int64Value("int64 -1", -1), int64Value("int64 0", 0), int64Value("int64 7", 7),
		int64Value("int64 2^53-1", 1<<53-1), int64Value("int64 2^53", 1<<53), int64Value("int64 2^53+1", 1<<53+1),
		int64Value("int64 2^53+2", 1<<53+2), int64Value("int64 -2^53-1", -(1<<53)-1),
		int64Value("int64 max", math.MaxInt64), int64Value("int64 max-1", math.MaxInt64-1),
		int64Value("int64 min", math.MinInt64), int64Value("int64 min+1", math.MinInt64+1),
		doubleValue("double 0", 0), doubleValue("double -0", math.Copysign(0, -1)),
		doubleValue("double 0.5", 0.5), doubleValue("double -0.5", -0.5), doubleValue("double 7", 7), doubleValue("double -5", -5),
		doubleValue("double 2^53", 1<<53), doubleValue("double 2^53+2", 1<<53+2),
		doubleValue("double 2^63", 0x1p63), doubleValue("double -2^63", -0x1p63),
		doubleValue("double just under 2^63", math.Nextafter(0x1p63, 0)), doubleValue("double just over -2^63", math.Nextafter(-0x1p63, 0)),
		doubleValue("double 1e308", 1e308), doubleValue("double -1e308", -1e308),
		doubleValue("double smallest subnormal", math.SmallestNonzeroFloat64),
		doubleValue("double +Inf", math.Inf(1)), doubleValue("double -Inf", math.Inf(-1)),
		doubleValue("double NaN", math.NaN()),
		{"double NaN, negative with payload", elemOf(0x01, "v", le64(0xFFF8000000000123)...)},
		{"double NaN, signalling", elemOf(0x01, "v", le64(0x7FF0000000000001)...)},

		// strings
		stringValue("string empty", ""), stringValue("string a", "a"), stringValue("string ab", "ab"),
		stringValue("string b", "b"), stringValue("string A", "A"), stringValue("string e-acute", "é"),
		stringValue("string zz", "zz"), stringValue("string NUL inside", "a\x00b"),
		stringValue("string invalid UTF-8", "\xff\xfe"),

		// dates
		dateValue("date -1", -1), dateValue("date 0", 0), dateValue("date 1", 1),
		dateValue("date max", math.MaxInt64), dateValue("date min", math.MinInt64),

		// booleans
		booleanValue("false", false), booleanValue("true", true),

		// documents and arrays
		{"document empty", elemOf(0x03, "v", docOf()...)},
		{"document with a member", elemOf(0x03, "v", docOf(int32Element("a", 1))...)},
		{"document with other members", elemOf(0x03, "v", docOf(int32Element("z", 9), int32Element("y", 8))...)},
		{"array empty", elemOf(0x04, "v", docOf()...)},
		{"array with an element", elemOf(0x04, "v", docOf(int32Element("0", 1))...)},
		{"array with other elements", elemOf(0x04, "v", docOf(int32Element("0", 5), int32Element("1", 6))...)},

		// every type of the other class, two values each where the type has
		// more than one, so a tie within the type is tested
		{"binary 1", elemOf(0x05, "v", append(append(le32(3), 0), 1, 2, 3)...)},
		{"binary 2", elemOf(0x05, "v", append(append(le32(1), 0x80), 9)...)},
		{"undefined", elemOf(0x06, "v")},
		{"objectid 1", elemOf(0x07, "v", make([]byte, 12)...)},
		{"objectid 2", elemOf(0x07, "v", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}...)},
		{"regex 1", elemOf(0x0B, "v", 'a', 0, 'i', 0)},
		{"regex 2", elemOf(0x0B, "v", 'b', 0, 0)},
		{"dbpointer 1", elemOf(0x0C, "v", append(bsonString("ns"), make([]byte, 12)...)...)},
		{"dbpointer 2", elemOf(0x0C, "v", append(bsonString("other"), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}...)...)},
		{"javascript 1", elemOf(0x0D, "v", bsonString("f()")...)},
		{"javascript 2", elemOf(0x0D, "v", bsonString("g()")...)},
		{"symbol 1", elemOf(0x0E, "v", bsonString("s")...)},
		{"symbol 2", elemOf(0x0E, "v", bsonString("t")...)},
		{"javascript with scope", elemOf(0x0F, "v", codeWithScope...)},
		{"timestamp 1", elemOf(0x11, "v", le64(1)...)},
		{"timestamp 2", elemOf(0x11, "v", le64(1<<40)...)},
		decimalValue("decimal128 0", 0x3040000000000000, 0),
		decimalValue("decimal128 1", 0x3040000000000000, 1),
		decimalValue("decimal128 -1", 0xB040000000000000, 1),
		decimalValue("decimal128 1E+3", 0x3046000000000000, 1),
		decimalValue("decimal128 1000", 0x3040000000000000, 1000),
		decimalValue("decimal128 NaN", 0x7C00000000000000, 0),
		decimalValue("decimal128 +Inf", 0x7800000000000000, 0),
		decimalValue("decimal128 -Inf", 0xF800000000000000, 0),
		decimalValue("decimal128 largest", 0x5FFFED09BEAD87C0, 0x378D8E63FFFFFFFF),
		{"maxkey", elemOf(0x7F, "v")},
		{"minkey", elemOf(0xFF, "v")},
	}
	return values
}

// classRepresentatives is one value of each of the 8 classes, in the rank
// order of spec 6.3.
var classRepresentatives = []string{
	"int32 7", "string a", "date 1", "true", "document with a member", "array with an element", "symbol 1", "null",
}

// sortPair is one value's two readings: the library's View of the field (the
// zero View for a missing field) and the server's sort key from the vendor's
// decode of the same bytes.
type sortPair struct {
	name    string
	view    bson.View
	key     sortKey
	decoded interface{}
}

func readSortPair(tb testing.TB, value sortValue) sortPair {
	tb.Helper()
	var elements [][]byte
	if value.element != nil {
		elements = append(elements, value.element)
	}
	raw := docOf(elements...)
	document, err := bson.Validate(raw, bson.Limits{Depth: 100})
	if err != nil {
		tb.Fatalf("%s: the library refuses the document built for it: %v", value.name, err)
	}
	view, _, err := document.Lookup(bson.Path{bson.Name("v")})
	if err != nil {
		tb.Fatalf("%s: Lookup: %v", value.name, err)
	}
	decoded, err := decodeNative(raw)
	if err != nil {
		tb.Fatalf("%s: the server's BSON package cannot decode the document built for it: %v", value.name, err)
	}
	return sortPair{name: value.name, view: view, key: sortKeyOf(decoded["v"]), decoded: decoded["v"]}
}

func signOf(comparison int) int {
	switch {
	case comparison < 0:
		return -1
	case comparison > 0:
		return 1
	}
	return 0
}

func readSortPairs(tb testing.TB) []sortPair {
	tb.Helper()
	values := sortValues()
	pairs := make([]sortPair, 0, len(values))
	for _, value := range values {
		pairs = append(pairs, readSortPair(tb, value))
	}
	return pairs
}

func pairNamed(tb testing.TB, pairs []sortPair, name string) sortPair {
	tb.Helper()
	for _, pair := range pairs {
		if pair.name == name {
			return pair
		}
	}
	tb.Fatalf("no sort value named %q", name)
	return sortPair{}
}

// TestComparatorMatchesCompareSortKeysOnEveryClassPair compares every ordered
// pair of the value list: the sign of Compare on the two Views against the
// sign of compareSortKeys on the two vendor-decoded values' sort keys. The
// list holds a value of each of the 8 classes, so every ordered pair of
// classes is among the pairs, and the within-class rows of the build doc.
func TestComparatorMatchesCompareSortKeysOnEveryClassPair(t *testing.T) {
	pairs := readSortPairs(t)
	disagreements := 0
	for _, left := range pairs {
		for _, right := range pairs {
			got := bson.Compare(left.view, right.view)
			want := signOf(compareSortKeys(left.key, right.key))
			if got != want {
				disagreements++
				t.Errorf("Compare(%s, %s) = %d, compareSortKeys = %d (classes %#x and %#x)",
					left.name, right.name, got, want, left.key.class, right.key.class)
			}
		}
	}
	t.Logf("%d values, %d ordered pairs, %d disagreements", len(pairs), len(pairs)*len(pairs), disagreements)

	// Every ordered pair of the 8 classes, by name, so a rank that moves is
	// reported as two classes and not as two values.
	for leftClass, leftName := range classRepresentatives {
		for rightClass, rightName := range classRepresentatives {
			left, right := pairNamed(t, pairs, leftName), pairNamed(t, pairs, rightName)
			want := 0
			switch {
			case leftClass < rightClass:
				want = -1
			case leftClass > rightClass:
				want = 1
			}
			if got := bson.Compare(left.view, right.view); got != want {
				t.Errorf("class %d (%s) against class %d (%s): Compare = %d, spec 6.3 rank order says %d",
					leftClass+1, leftName, rightClass+1, rightName, got, want)
			}
			if serverSign := signOf(compareSortKeys(left.key, right.key)); serverSign != want {
				t.Errorf("class %d (%s) against class %d (%s): compareSortKeys = %d, spec 6.3 rank order says %d",
					leftClass+1, leftName, rightClass+1, rightName, serverSign, want)
			}
		}
	}
}

// TestComparatorExplicitRows holds the named rows (symbol, JavaScript,
// undefined, timestamp, MinKey and MaxKey, Decimal128, null against missing,
// -0, NaN, the boundaries past 2^53), each with the sign the order gives (left
// against right). The server's answer is checked against the sign
// too, so a row that is wrong in this table fails on the oracle first.
func TestComparatorExplicitRows(t *testing.T) {
	pairs := readSortPairs(t)
	rows := []struct {
		left, right string
		want        int
	}{
		// rank 7 types, ordered by type byte and tied within a type
		{"binary 1", "undefined", -1},
		{"undefined", "objectid 1", -1},
		{"objectid 1", "regex 1", -1},
		{"regex 1", "dbpointer 1", -1},
		{"dbpointer 1", "javascript 1", -1},
		{"javascript 1", "symbol 1", -1},
		{"symbol 1", "javascript with scope", -1},
		{"javascript with scope", "timestamp 1", -1},
		{"timestamp 1", "decimal128 1", -1},
		{"decimal128 1", "maxkey", -1},
		{"maxkey", "minkey", -1},
		{"symbol 1", "symbol 2", 0},
		{"javascript 1", "javascript 2", 0},
		{"timestamp 1", "timestamp 2", 0},
		{"binary 1", "binary 2", 0},
		{"objectid 1", "objectid 2", 0},
		{"regex 1", "regex 2", 0},
		{"dbpointer 1", "dbpointer 2", 0},
		{"maxkey", "maxkey", 0},
		{"minkey", "minkey", 0},
		{"undefined", "undefined", 0},
		// every rank-7 value sits above every number, string, date, boolean, document and array
		{"minkey", "array with an element", 1},
		{"undefined", "document empty", 1},
		{"timestamp 2", "true", 1},
		// and below null and missing
		{"minkey", "null", -1},
		{"minkey", "missing", -1},
		{"symbol 1", "null", -1},
		// null against missing
		{"null", "missing", 0},
		{"missing", "null", 0},
		{"null", "null", 0},
		{"missing", "missing", 0},
		{"missing", "int32 0", 1},
		{"null", "string empty", 1},
		// signed zero
		{"double -0", "double 0", 0},
		{"double -0", "int32 0", 0},
		{"double -0", "int64 0", 0},
		{"double -0", "double -0.5", 1},
		{"double -0", "double 0.5", -1},
		// NaN
		{"double NaN", "double NaN", 0},
		{"double NaN", "double NaN, negative with payload", 0},
		{"double NaN", "double NaN, signalling", 0},
		{"double NaN", "double -Inf", -1},
		{"double NaN", "int64 min", -1},
		{"double NaN", "int32 min", -1},
		{"double -Inf", "double NaN", 1},
		{"double NaN", "null", -1},
		// int and double boundaries
		{"int64 2^53", "double 2^53", 0},
		{"int64 2^53+1", "double 2^53", 1},
		{"int64 2^53+1", "double 2^53+2", -1},
		{"int64 2^53+2", "double 2^53+2", 0},
		{"int64 2^53", "int64 2^53+1", -1},
		{"int64 2^53-1", "double 2^53", -1},
		{"int64 -2^53-1", "double -5", -1},
		{"int64 max", "double 2^63", -1},
		{"int64 max-1", "int64 max", -1},
		{"int64 max", "double just under 2^63", 1},
		{"int64 min", "double -2^63", 0},
		{"int64 min+1", "double -2^63", 1},
		{"int64 min", "double just over -2^63", -1},
		{"double +Inf", "int64 max", 1},
		{"double -Inf", "int64 min", -1},
		{"double 1e308", "int64 max", 1},
		{"double smallest subnormal", "int32 0", 1},
		{"int32 max", "int64 7", 1},
		{"int32 7", "double 7", 0},
		// dates, strings, booleans
		{"date min", "date max", -1},
		{"false", "true", -1},
		{"string empty", "string a", -1},
		{"string a", "string ab", -1},
		{"string A", "string a", -1},
		{"string zz", "string e-acute", -1},
		{"string invalid UTF-8", "string e-acute", 1},
		{"string NUL inside", "string ab", -1},
		// documents and arrays tie
		{"document empty", "document with other members", 0},
		{"array empty", "array with other elements", 0},
		{"document with a member", "array empty", -1},
	}
	for _, row := range rows {
		left, right := pairNamed(t, pairs, row.left), pairNamed(t, pairs, row.right)
		if got := bson.Compare(left.view, right.view); got != row.want {
			t.Errorf("Compare(%s, %s) = %d, want %d", row.left, row.right, got, row.want)
		}
		if got := signOf(compareSortKeys(left.key, right.key)); got != row.want {
			t.Errorf("compareSortKeys(%s, %s) = %d, want %d: the row's expectation does not match the server", row.left, row.right, got, row.want)
		}
	}
}

// TestComparatorTiesEveryDecimal128 pins ruling O9: all Decimal128 values tie,
// whatever number they hold, and Decimal128 sits in the "other" class (rank 7,
// type byte 0x13): above every number, below null, above a timestamp, below
// MaxKey. The server's order does the same, and the test compares the two.
func TestComparatorTiesEveryDecimal128(t *testing.T) {
	pairs := readSortPairs(t)
	var decimals []sortPair
	for _, pair := range pairs {
		if len(pair.name) > 10 && pair.name[:10] == "decimal128" {
			decimals = append(decimals, pair)
		}
	}
	if len(decimals) < 8 {
		t.Fatalf("only %d Decimal128 values in the list", len(decimals))
	}
	for _, left := range decimals {
		for _, right := range decimals {
			if got := bson.Compare(left.view, right.view); got != 0 {
				t.Errorf("Compare(%s, %s) = %d, want 0: all Decimal128 values tie (O9)", left.name, right.name, got)
			}
			if got := compareSortKeys(left.key, right.key); got != 0 {
				t.Errorf("compareSortKeys(%s, %s) = %d, want 0", left.name, right.name, got)
			}
		}
	}
	decimal := pairNamed(t, pairs, "decimal128 1")
	for _, other := range []struct {
		name string
		want int
	}{
		{"int32 7", 1}, {"double +Inf", 1}, {"string a", 1}, {"array with an element", 1}, {"true", 1},
		{"timestamp 1", 1}, {"symbol 1", 1}, {"maxkey", -1}, {"minkey", -1}, {"null", -1}, {"missing", -1},
	} {
		right := pairNamed(t, pairs, other.name)
		if got := bson.Compare(decimal.view, right.view); got != other.want {
			t.Errorf("Compare(decimal128 1, %s) = %d, want %d", other.name, got, other.want)
		}
		if got := signOf(compareSortKeys(decimal.key, right.key)); got != other.want {
			t.Errorf("compareSortKeys(decimal128 1, %s) = %d, want %d", other.name, got, other.want)
		}
	}
	if decimal.key.class != sortRankOther<<8|0x13 {
		t.Errorf("the server's class for Decimal128 is %#x, want rank 7 with type byte 0x13", decimal.key.class)
	}
}

// literalRow is a library Literal beside the element bytes that hold the same
// value, so the oracle reads the value from bytes.
type literalRow struct {
	name    string
	literal bson.Literal
	value   sortValue
}

func literalRows() []literalRow {
	return []literalRow{
		{"int32 -5", bson.Int32Literal(-5), int32Value("", -5)},
		{"int32 7", bson.Int32Literal(7), int32Value("", 7)},
		{"int32 max", bson.Int32Literal(math.MaxInt32), int32Value("", math.MaxInt32)},
		{"int64 2^53+1", bson.Int64Literal(1<<53 + 1), int64Value("", 1<<53+1)},
		{"int64 max", bson.Int64Literal(math.MaxInt64), int64Value("", math.MaxInt64)},
		{"int64 min", bson.Int64Literal(math.MinInt64), int64Value("", math.MinInt64)},
		{"double 0", bson.DoubleLiteral(0), doubleValue("", 0)},
		{"double -0", bson.DoubleLiteral(math.Copysign(0, -1)), doubleValue("", math.Copysign(0, -1))},
		{"double 2^53", bson.DoubleLiteral(1 << 53), doubleValue("", 1<<53)},
		{"double 2^63", bson.DoubleLiteral(0x1p63), doubleValue("", 0x1p63)},
		{"double NaN", bson.DoubleLiteral(math.NaN()), doubleValue("", math.NaN())},
		{"double +Inf", bson.DoubleLiteral(math.Inf(1)), doubleValue("", math.Inf(1))},
		{"double 0.5", bson.DoubleLiteral(0.5), doubleValue("", 0.5)},
		{"string empty", bson.StringLiteral(""), stringValue("", "")},
		{"string a", bson.StringLiteral("a"), stringValue("", "a")},
		{"string e-acute", bson.StringLiteral("é"), stringValue("", "é")},
		{"date 0", bson.DateTimeLiteral(0), dateValue("", 0)},
		{"date min", bson.DateTimeLiteral(math.MinInt64), dateValue("", math.MinInt64)},
		{"false", bson.BooleanLiteral(false), booleanValue("", false)},
		{"true", bson.BooleanLiteral(true), booleanValue("", true)},
		{"null", bson.NullLiteral(), sortValue{"", elemOf(0x0A, "v")}},
	}
}

// TestComparatorLiteralMatchesCompareSortKeys compares CompareLiteral of each
// value against each literal with compareSortKeys on the vendor-decoded value
// and the vendor-decoded element bytes of the literal's value.
func TestComparatorLiteralMatchesCompareSortKeys(t *testing.T) {
	pairs := readSortPairs(t)
	rows := literalRows()
	compared := 0
	for _, row := range rows {
		row.value.name = row.name
		literalPair := readSortPair(t, row.value)
		for _, pair := range pairs {
			compared++
			got := bson.CompareLiteral(pair.view, row.literal)
			want := signOf(compareSortKeys(pair.key, literalPair.key))
			if got != want {
				t.Errorf("CompareLiteral(%s, %s) = %d, compareSortKeys = %d", pair.name, row.name, got, want)
			}
			if viaView := bson.Compare(pair.view, literalPair.view); viaView != got {
				t.Errorf("CompareLiteral(%s, %s) = %d but Compare against a View of the same value = %d", pair.name, row.name, got, viaView)
			}
		}
	}
	t.Logf("%d value-against-literal comparisons", compared)
	if got := bson.CompareLiteral(bson.View{}, bson.Literal{}); got != 0 {
		t.Errorf("the missing View against the zero Literal = %d, want 0 (null ties with missing)", got)
	}
}

// TestComparatorSortsLikeTheServer sorts the value list with both orders and
// compares the permutations: a transitivity or antisymmetry failure that no
// pairwise sign would show surfaces as a different order.
func TestComparatorSortsLikeTheServer(t *testing.T) {
	pairs := readSortPairs(t)
	library := append([]sortPair{}, pairs...)
	server := append([]sortPair{}, pairs...)
	stableSort(library, func(left, right sortPair) bool { return bson.Compare(left.view, right.view) < 0 })
	stableSort(server, func(left, right sortPair) bool { return compareSortKeys(left.key, right.key) < 0 })
	for position := range library {
		if library[position].name != server[position].name {
			t.Fatalf("position %d: the library's order puts %q, the server's puts %q", position, library[position].name, server[position].name)
		}
	}
}

// stableSort is an insertion sort, which is stable and needs a strict
// less-than only; the value list is small.
func stableSort(items []sortPair, less func(left, right sortPair) bool) {
	for position := 1; position < len(items); position++ {
		for earlier := position; earlier > 0 && less(items[earlier], items[earlier-1]); earlier-- {
			items[earlier], items[earlier-1] = items[earlier-1], items[earlier]
		}
	}
}
