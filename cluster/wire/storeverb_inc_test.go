package wire

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// incRawDoc is a one-element BSON document of the type and value bytes given, built by hand so a
// test can send a value type IncArg refuses.
func incRawDoc(typ byte, key string, value []byte) []byte {
	body := append([]byte{typ}, key...)
	body = append(body, 0)
	body = append(body, value...)
	doc := binary.LittleEndian.AppendUint32(nil, uint32(4+len(body)+1))
	doc = append(doc, body...)
	return append(doc, 0)
}

// incArgBytes is the arg a sender writes for key and increments, without IncArg's checks.
func incArgBytes(t *testing.T, key string, increments []byte) []byte {
	t.Helper()
	return u63Doc(t, bson.D{{Key: "k", Value: key}, {Key: "v", Value: increments}})
}

func TestIncArgRoundTripsAndIsCanonical(t *testing.T) {
	increments := u63Doc(t, bson.D{{Key: "t", Value: int32(1)}, {Key: "s", Value: int32(30)}})
	plain, err := IncArg("2026-11-05T14", increments, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain, incArgBytes(t, "2026-11-05T14", increments)) {
		t.Fatal("IncArg without a return writes an r field; an absent flag must not be written")
	}
	got, err := decodeVerbArg(shapeInc, plain)
	if err != nil || got.Key != "2026-11-05T14" || got.Return || !bytes.Equal(got.Element, increments) {
		t.Fatalf("decode of IncArg(false) = %+v, %v", got, err)
	}
	after, err := IncArg("k", increments, true)
	if err != nil {
		t.Fatal(err)
	}
	got, err = decodeVerbArg(shapeInc, after)
	if err != nil || !got.Return {
		t.Fatalf("decode of IncArg(true) = %+v, %v", got, err)
	}
	// r: false names the same arg as no r at all, so an arg that writes it is refused.
	explicit := u63Doc(t, bson.D{{Key: "k", Value: "k"}, {Key: "v", Value: increments}, {Key: "r", Value: false}})
	if _, err := decodeVerbArg(shapeInc, explicit); err == nil {
		t.Fatal("an arg with r: false was accepted; two byte strings would name one increment")
	}
}

func TestIncOnAFifoStoreIsRefused(t *testing.T) {
	for _, engine := range []store.FieldEngine{store.FieldFIFO, store.FieldLIFO, store.FieldHeap} {
		if _, fits := verbShapeOf(cluster.VerbInc, engine); fits {
			t.Errorf("verbShapeOf fits inc to a %v: an inc appended to a queue fails on every replay", engine)
		}
	}
	for _, engine := range []store.FieldEngine{store.FieldHash, store.FieldBTree} {
		if shape, fits := verbShapeOf(cluster.VerbInc, engine); !fits || shape != shapeInc {
			t.Errorf("verbShapeOf(inc, %v) = %v, %v; the owner applies an inc on a keyed store", engine, shape, fits)
		}
	}
	for _, typ := range []string{"fifoStore", "lifoStore", "heapStore"} {
		table := machine.FieldTable{NextFieldID: 3, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: typ, Name: "Purchases"}}}
		call := storeCall{ref: bucketRef{db: "app", name: "User", table: table}, fieldID: 2}
		if _, err := call.takes("inc"); err == nil || !strings.Contains(err.Error(), "use update's $inc for a document field (help update)") {
			t.Errorf("inc on a %s: %v; want the refusal that names update's $inc", typ, err)
		}
	}
	for _, typ := range []string{"hashStore", "btreeStore"} {
		table := machine.FieldTable{NextFieldID: 3, Entries: []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: typ, Name: "Hits"}}}
		call := storeCall{ref: bucketRef{db: "app", name: "User", table: table}, fieldID: 2}
		if _, err := call.takes("inc"); err != nil {
			t.Errorf("inc on a %s refused: %v", typ, err)
		}
	}
}

func TestIncRefusesNonNumbersAndDeepPaths(t *testing.T) {
	many := bson.D{}
	for i := range 65 {
		many = append(many, bson.E{Key: "p" + strings.Repeat("x", i%3), Value: int32(1)})
	}
	refused := map[string][]byte{
		"a string":              u63Doc(t, bson.D{{Key: "s", Value: "x"}}),
		"a NaN":                 u63Doc(t, bson.D{{Key: "d", Value: math.NaN()}}),
		"an infinity":           u63Doc(t, bson.D{{Key: "d", Value: math.Inf(1)}}),
		"a path four deep":      u63Doc(t, bson.D{{Key: "a.b.c.d", Value: int32(1)}}),
		"an empty path segment": u63Doc(t, bson.D{{Key: "a..b", Value: int32(1)}}),
		"no paths":              u63Doc(t, bson.D{}),
		"65 paths":              u63Doc(t, many),
	}
	for name, increments := range refused {
		if _, err := IncArg("k", increments, false); err == nil {
			t.Errorf("IncArg took %s", name)
		}
		if _, err := decodeVerbArg(shapeInc, incArgBytes(t, "k", increments)); err == nil {
			t.Errorf("decode took %s", name)
		}
	}
	if _, err := IncArg("k", incRawDoc(0x13, "m", make([]byte, 16)), false); err == nil || !strings.Contains(err.Error(), "decimal") {
		t.Errorf("a decimal: %v; want a refusal that says decimal is refused", err)
	}
	for _, key := range []string{"", strings.Repeat("k", maxVerbKeyBytes+1)} {
		if _, err := IncArg(key, u63Doc(t, bson.D{{Key: "n", Value: int32(1)}}), false); err == nil {
			t.Errorf("IncArg took a %d-byte key", len(key))
		}
	}
	// The limits themselves are taken: a three-deep path, a fractional double, a key of 1024 bytes.
	limit := u63Doc(t, bson.D{{Key: "a.b.c", Value: int32(1)}, {Key: "x", Value: 1.5}})
	if _, err := IncArg(strings.Repeat("k", maxVerbKeyBytes), limit, false); err != nil {
		t.Errorf("a three-deep path and a 1024-byte key were refused: %v", err)
	}
}

// TestIncRefusesARepeatedPathAndAPathUnderAnother: [GUARD: {n:1, n:1} adding twice]. MongoDB refuses a
// path conflict in $inc, and so does inc, naming both paths.
func TestIncRefusesARepeatedPathAndAPathUnderAnother(t *testing.T) {
	cases := map[string]struct {
		increments []byte
		paths      []string
	}{
		"a repeated path":      {u63Doc(t, bson.D{{Key: "n", Value: int32(1)}, {Key: "n", Value: int32(1)}}), []string{`"n"`}},
		"a path under another": {u63Doc(t, bson.D{{Key: "d", Value: int32(1)}, {Key: "d.t", Value: int32(1)}}), []string{`"d"`, `"d.t"`}},
		"the parent last":      {u63Doc(t, bson.D{{Key: "d.t.s", Value: int32(1)}, {Key: "d.t", Value: int32(1)}}), []string{`"d.t.s"`, `"d.t"`}},
	}
	for name, tc := range cases {
		_, err := IncArg("k", tc.increments, false)
		if err == nil {
			t.Errorf("IncArg took %s", name)
			continue
		}
		for _, path := range tc.paths {
			if !strings.Contains(err.Error(), path) {
				t.Errorf("%s: the refusal %q does not name %s", name, err, path)
			}
		}
		if _, err := decodeVerbArg(shapeInc, incArgBytes(t, "k", tc.increments)); err == nil {
			t.Errorf("decode took %s", name)
		}
	}
	// siblings that merely share a name prefix are not a conflict
	if _, err := IncArg("k", u63Doc(t, bson.D{{Key: "d", Value: int32(1)}, {Key: "dd.t", Value: int32(1)}, {Key: "d2", Value: int32(1)}}), false); err != nil {
		t.Errorf("distinct paths were refused: %v", err)
	}
}

// TestIncWithNoPathNamesTheCommandThatWorks: an empty increments document is refused with a call that
// would have been accepted, not only the rule it broke.
func TestIncWithNoPathNamesTheCommandThatWorks(t *testing.T) {
	_, err := IncArg("k", u63Doc(t, bson.D{}), false)
	if err == nil || !strings.Contains(err.Error(), `db.User["ada"].Friends.inc("grace", {visits: 1})`) || !strings.Contains(err.Error(), "help inc") {
		t.Fatalf("empty increments: %v; want the refusal to show a working inc call and name help inc", err)
	}
}
