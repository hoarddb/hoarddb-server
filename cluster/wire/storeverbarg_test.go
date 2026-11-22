package wire

import (
	"bytes"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

func u63Doc(t testing.TB, doc any) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func u63Element(label string) []byte {
	raw, err := bson.Marshal(map[string]string{"item": label})
	if err != nil {
		panic(err)
	}
	return raw
}

func TestU63ShapesFollowTheSpecTable(t *testing.T) {
	type pair struct {
		verb   byte
		engine store.FieldEngine
	}
	want := map[pair]verbShape{
		{cluster.VerbPush, store.FieldFIFO}: shapeElement, {cluster.VerbPush, store.FieldLIFO}: shapeElement,
		{cluster.VerbPush, store.FieldHeap}: shapeHeapPush,
		{cluster.VerbPop, store.FieldFIFO}:  shapePop, {cluster.VerbPop, store.FieldLIFO}: shapePop, {cluster.VerbPop, store.FieldHeap}: shapePop,
		{cluster.VerbPut, store.FieldHash}: shapePut, {cluster.VerbPut, store.FieldBTree}: shapePut,
		{cluster.VerbTake, store.FieldHash}: shapeTakeKey, {cluster.VerbTake, store.FieldBTree}: shapeTakeKey,
		{cluster.VerbTake, store.FieldHeap}: shapeTakeEntry,
	}
	for _, verb := range []byte{cluster.VerbPush, cluster.VerbPop, cluster.VerbTake, cluster.VerbPut} {
		for _, engine := range []store.FieldEngine{store.FieldHash, store.FieldBTree, store.FieldFIFO, store.FieldLIFO, store.FieldHeap, store.FieldBlob} {
			shape, ok := verbShapeOf(verb, engine)
			wantShape, fits := want[pair{verb, engine}]
			if ok != fits || shape != wantShape {
				t.Errorf("verb %d on %v = %v, %v; want %v, %v", verb, engine, shape, ok, wantShape, fits)
			}
		}
	}
}

func TestU63ArgsRoundTripAndAreSpecBSON(t *testing.T) {
	element := u63Element("a")
	push, err := encodeElementArg(element)
	if err != nil || !bytes.Equal(push, u63Doc(t, bson.D{{Key: "v", Value: element}})) {
		t.Fatalf("encodeElementArg = %x, %v", push, err)
	}
	if got, err := decodeVerbArg(shapeElement, push); err != nil || !bytes.Equal(got.Element, element) {
		t.Fatalf("decode {v} = %+v, %v", got, err)
	}
	heap, err := encodeHeapPushArg(-7, "e1", element)
	if err != nil || !bytes.Equal(heap, u63Doc(t, bson.D{{Key: "p", Value: int64(-7)}, {Key: "e", Value: "e1"}, {Key: "v", Value: element}})) {
		t.Fatalf("encodeHeapPushArg = %x, %v", heap, err)
	}
	if got, err := decodeVerbArg(shapeHeapPush, heap); err != nil || got.Priority != -7 || got.EntryID != "e1" || !bytes.Equal(got.Element, element) {
		t.Fatalf("decode {p,e,v} = %+v, %v", got, err)
	}
	put, err := encodePutArg("k1", element)
	if err != nil || !bytes.Equal(put, u63Doc(t, bson.D{{Key: "k", Value: "k1"}, {Key: "v", Value: element}})) {
		t.Fatalf("encodePutArg = %x, %v", put, err)
	}
	if got, err := decodeVerbArg(shapePut, put); err != nil || got.Key != "k1" || !bytes.Equal(got.Element, element) {
		t.Fatalf("decode {k,v} = %+v, %v", got, err)
	}
	takeKey, err := encodeTakeKeyArg("k1")
	if err != nil || !bytes.Equal(takeKey, u63Doc(t, bson.D{{Key: "k", Value: "k1"}})) {
		t.Fatalf("encodeTakeKeyArg = %x, %v", takeKey, err)
	}
	if got, err := decodeVerbArg(shapeTakeKey, takeKey); err != nil || got.Key != "k1" {
		t.Fatalf("decode {k} = %+v, %v", got, err)
	}
	takeEntry, err := encodeTakeEntryArg("e1")
	if err != nil || !bytes.Equal(takeEntry, u63Doc(t, bson.D{{Key: "e", Value: "e1"}})) {
		t.Fatalf("encodeTakeEntryArg = %x, %v", takeEntry, err)
	}
	if got, err := decodeVerbArg(shapeTakeEntry, takeEntry); err != nil || got.EntryID != "e1" {
		t.Fatalf("decode {e} = %+v, %v", got, err)
	}
	if _, err := decodeVerbArg(shapePop, nil); err != nil {
		t.Fatalf("a pop with no arg = %v, want accepted", err)
	}
}

// TestU63DecoderRefusesWhatIsNotCanonical: [GUARD canonical re-encode compare].
func TestU63DecoderRefusesWhatIsNotCanonical(t *testing.T) {
	element := u63Element("a")
	cases := map[string]struct {
		shape verbShape
		arg   []byte
	}{
		"heap push fields reordered":  {shapeHeapPush, u63Doc(t, bson.D{{Key: "e", Value: "e1"}, {Key: "p", Value: int64(1)}, {Key: "v", Value: element}})},
		"put fields reordered":        {shapePut, u63Doc(t, bson.D{{Key: "v", Value: element}, {Key: "k", Value: "k"}})},
		"duplicate field":             {shapeElement, u63Doc(t, bson.D{{Key: "v", Value: element}, {Key: "v", Value: element}})},
		"extra field":                 {shapeTakeKey, u63Doc(t, bson.D{{Key: "k", Value: "k"}, {Key: "x", Value: int32(1)}})},
		"widened priority":            {shapeHeapPush, u63Doc(t, bson.D{{Key: "p", Value: int32(1)}, {Key: "e", Value: "e1"}, {Key: "v", Value: element}})},
		"missing field":               {shapePut, u63Doc(t, bson.D{{Key: "k", Value: "k"}})},
		"trailing byte":               {shapeTakeKey, append(u63Doc(t, bson.D{{Key: "k", Value: "k"}}), 0)},
		"not a document":              {shapeElement, []byte{1, 2, 3}},
		"push arg given to put":       {shapePut, u63Doc(t, bson.D{{Key: "v", Value: element}})},
		"pop with an arg":             {shapePop, u63Doc(t, bson.D{{Key: "v", Value: element}})},
		"element as string not bytes": {shapeElement, u63Doc(t, bson.D{{Key: "v", Value: "text"}})},
	}
	for name, tc := range cases {
		if _, err := decodeVerbArg(tc.shape, tc.arg); err == nil {
			t.Errorf("%s: accepted %x", name, tc.arg)
		}
	}
}

func TestU63ArgLimitsNameTheFieldAndTheLimit(t *testing.T) {
	element := u63Element("a")
	long := strings.Repeat("k", 1025)
	for name, tc := range map[string]struct {
		build  func() ([]byte, error)
		decode verbShape
		field  string
		limit  string
	}{
		"empty key":          {func() ([]byte, error) { return encodePutArg("", element) }, shapePut, `"k"`, "1 to 1024"},
		"long key":           {func() ([]byte, error) { return encodeTakeKeyArg(long) }, shapeTakeKey, `"k"`, "1 to 1024"},
		"empty entry id":     {func() ([]byte, error) { return encodeTakeEntryArg("") }, shapeTakeEntry, `"e"`, "1 to 1024"},
		"long entry id":      {func() ([]byte, error) { return encodeHeapPushArg(1, long, element) }, shapeHeapPush, `"e"`, "1 to 1024"},
		"empty element":      {func() ([]byte, error) { return encodeElementArg(nil) }, shapeElement, `"v"`, "at least 1 byte"},
		"element over 1 MiB": {func() ([]byte, error) { return encodeElementArg(make([]byte, 1<<20)) }, shapeElement, "arg", "1048576"},
		"lowest priority":    {func() ([]byte, error) { return encodeHeapPushArg(math.MinInt64, "e", element) }, shapeHeapPush, `"p"`, "-9223372036854775807"},
	} {
		if _, err := tc.build(); err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), tc.limit) {
			t.Errorf("%s: encode error = %v, want it to name %s and %s", name, err, tc.field, tc.limit)
		}
	}
	// The decoder holds the same limits for a record that did not come from the encoder.
	for name, tc := range map[string]struct {
		shape verbShape
		arg   []byte
		field string
	}{
		"empty key":       {shapePut, u63Doc(t, bson.D{{Key: "k", Value: ""}, {Key: "v", Value: element}}), `"k"`},
		"long key":        {shapeTakeKey, u63Doc(t, bson.D{{Key: "k", Value: long}}), `"k"`},
		"empty entry id":  {shapeTakeEntry, u63Doc(t, bson.D{{Key: "e", Value: ""}}), `"e"`},
		"lowest priority": {shapeHeapPush, u63Doc(t, bson.D{{Key: "p", Value: int64(math.MinInt64)}, {Key: "e", Value: "e"}, {Key: "v", Value: element}}), `"p"`},
		"empty element":   {shapeElement, u63Doc(t, bson.D{{Key: "v", Value: []byte{}}}), `"v"`},
		"over 1 MiB":      {shapeElement, u63Doc(t, bson.D{{Key: "v", Value: make([]byte, 1<<20)}}), "1048576"},
	} {
		if _, err := decodeVerbArg(tc.shape, tc.arg); err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Errorf("%s: decode error = %v, want it to name %s", name, err, tc.field)
		}
	}
	// The boundary values are accepted.
	if _, err := encodeTakeKeyArg(strings.Repeat("k", 1024)); err != nil {
		t.Errorf("a 1024-byte key = %v, want accepted", err)
	}
}

// TestU63DecoderSurvivesMutation: 100k mutations of valid args never panic, and whatever
// is accepted re-encodes to the same bytes.
func TestU63DecoderSurvivesMutation(t *testing.T) {
	element := u63Element("a")
	seeds := map[verbShape][]byte{}
	seeds[shapeElement], _ = encodeElementArg(element)
	seeds[shapeHeapPush], _ = encodeHeapPushArg(5, "e1", element)
	seeds[shapePut], _ = encodePutArg("k1", element)
	seeds[shapeTakeKey], _ = encodeTakeKeyArg("k1")
	seeds[shapeTakeEntry], _ = encodeTakeEntryArg("e1")
	rng := rand.New(rand.NewSource(63))
	shapes := []verbShape{shapeElement, shapeHeapPush, shapePut, shapeTakeKey, shapeTakeEntry}
	accepted := 0
	for i := 0; i < 100_000; i++ {
		shape := shapes[i%len(shapes)]
		arg := bytes.Clone(seeds[shape])
		if len(arg) == 0 {
			t.Fatalf("seed for shape %v is empty: the encoder is not built", shape)
		}
		for flips := 1 + rng.Intn(3); flips > 0; flips-- {
			arg[rng.Intn(len(arg))] ^= byte(1 << rng.Intn(8))
		}
		if rng.Intn(8) == 0 {
			arg = arg[:rng.Intn(len(arg))]
		}
		decoded, err := decodeVerbArg(shape, arg)
		if err != nil {
			continue
		}
		accepted++
		var again []byte
		switch shape {
		case shapeElement:
			again, err = encodeElementArg(decoded.Element)
		case shapeHeapPush:
			again, err = encodeHeapPushArg(decoded.Priority, decoded.EntryID, decoded.Element)
		case shapePut:
			again, err = encodePutArg(decoded.Key, decoded.Element)
		case shapeTakeKey:
			again, err = encodeTakeKeyArg(decoded.Key)
		case shapeTakeEntry:
			again, err = encodeTakeEntryArg(decoded.EntryID)
		}
		if err != nil || !bytes.Equal(again, arg) {
			t.Fatalf("accepted %x for shape %v but it re-encodes to %x (%v)", arg, shape, again, err)
		}
	}
	t.Logf("%d of 100000 mutations were still valid", accepted)
}
