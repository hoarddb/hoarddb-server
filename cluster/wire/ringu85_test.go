package wire

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// TestU85PopArgIsAnOptionalNInOneToOneHundred: a pop with no arg is today's single pop; a
// pop with {n: int32} takes up to n. [GUARD the n range, the canonical compare]
func TestU85PopArgIsAnOptionalNInOneToOneHundred(t *testing.T) {
	arg, err := encodePopArg(3)
	if err != nil || !bytes.Equal(arg, u63Doc(t, bson.D{{Key: "n", Value: int32(3)}})) {
		t.Fatalf("encodePopArg(3) = %x, %v", arg, err)
	}
	got, err := decodeVerbArg(shapePop, arg)
	if err != nil || got.Count != 3 {
		t.Fatalf("decode {n: 3} = %+v, %v", got, err)
	}
	if got, err := decodeVerbArg(shapePop, nil); err != nil || got.Count != 0 {
		t.Fatalf("decode of no arg = %+v, %v, want a single pop (Count 0)", got, err)
	}
	for _, n := range []int{0, -1, 101, 1000} {
		if _, err := encodePopArg(n); err == nil || !strings.Contains(err.Error(), "1 to 100") {
			t.Errorf("encodePopArg(%d) = %v, want a refusal naming 1 to 100", n, err)
		}
	}
	for name, raw := range map[string][]byte{
		"n 0":            u63Doc(t, bson.D{{Key: "n", Value: int32(0)}}),
		"n 101":          u63Doc(t, bson.D{{Key: "n", Value: int32(101)}}),
		"n negative":     u63Doc(t, bson.D{{Key: "n", Value: int32(-4)}}),
		"n widened":      u63Doc(t, bson.D{{Key: "n", Value: int64(3)}}),
		"n as a string":  u63Doc(t, bson.D{{Key: "n", Value: "3"}}),
		"extra field":    u63Doc(t, bson.D{{Key: "n", Value: int32(3)}, {Key: "x", Value: int32(1)}}),
		"another letter": u63Doc(t, bson.D{{Key: "k", Value: int32(3)}}),
		"trailing byte":  append(u63Doc(t, bson.D{{Key: "n", Value: int32(3)}}), 0),
		"push arg":       u63Doc(t, bson.D{{Key: "v", Value: u63Element("a")}}),
	} {
		if _, err := decodeVerbArg(shapePop, raw); err == nil {
			t.Errorf("%s: a pop accepted %x", name, raw)
		}
	}
}

func TestU85PopArgSurvivesMutation(t *testing.T) {
	seed, err := encodePopArg(7)
	if err != nil || len(seed) == 0 {
		t.Fatalf("encodePopArg(7) = %x, %v", seed, err)
	}
	rng := rand.New(rand.NewPCG(85, 85))
	accepted := 0
	for range 100_000 {
		arg := u65Mutate(rng, seed)
		decoded, err := decodeVerbArg(shapePop, arg)
		if err != nil {
			continue
		}
		accepted++
		again := []byte(nil)
		if decoded.Count > 0 {
			again, err = encodePopArg(decoded.Count)
		}
		if err != nil || !bytes.Equal(again, arg) {
			t.Fatalf("accepted %x re-encodes as %x, %v", arg, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated pop arg was accepted, so the re-encode check never ran")
	}
}

// TestU85PopNArgRidesTheKeyedCodec: the keyed codec lets a pop carry {n} and still refuses
// an arg that is not one canonical BSON document.
func TestU85PopNArgRidesTheKeyedCodec(t *testing.T) {
	arg, err := encodePopArg(5)
	if err != nil || len(arg) == 0 {
		t.Fatalf("encodePopArg(5) = %x, %v", arg, err)
	}
	op := u65Header(cluster.Op{Class: cluster.OpStoreVerb, Fields: []cluster.FieldOp{{FieldID: 4, Verb: cluster.VerbPop, Value: arg}}})
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op})
	if err != nil {
		t.Fatalf("a pop {n: 5} was refused: %v", err)
	}
	decoded, err := DecodeRingKeyed(raw)
	if err != nil || !bytes.Equal(decoded.Op.Fields[0].Value, arg) {
		t.Fatalf("decoded %+v, %v", decoded, err)
	}
	op.Fields[0].Value = append(bytes.Clone(arg), 0)
	if _, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op}); err == nil {
		t.Fatal("a pop with trailing bytes after {n} was accepted")
	}
}

func TestU85ShapeTableHasPopAsOptionalN(t *testing.T) {
	for _, engine := range []store.FieldEngine{store.FieldFIFO, store.FieldLIFO, store.FieldHeap} {
		if shape, ok := verbShapeOf(cluster.VerbPop, engine); !ok || shape != shapePop {
			t.Errorf("pop on %v = %v, %v, want shapePop", engine, shape, ok)
		}
	}
}

func u85KeyedReply() RingReply {
	return RingReply{
		Found:      u65Bool(true),
		Elements:   [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}},
		Keys:       []string{"grace", "linus"},
		Priorities: []int64{9, -2},
		Epoch:      u65Int64(42),
		StateSeq:   u65Int64(41),
	}
}

// TestU85ReplyKeysAndPrioritiesFollowElements: keys and priorities are optional members
// right after elements, strings and int64, omitted when absent. [GUARD member order]
func TestU85ReplyKeysAndPrioritiesFollowElements(t *testing.T) {
	raw, err := EncodeRingReply(u85KeyedReply())
	if err != nil {
		t.Fatal(err)
	}
	want, err := bson.Marshal(bson.D{
		{Key: "found", Value: true},
		{Key: "elements", Value: []any{[]byte{5, 0, 0, 0, 0}, []byte{5, 0, 0, 0, 0}}},
		{Key: "keys", Value: []any{"grace", "linus"}},
		{Key: "priorities", Value: []any{int64(9), int64(-2)}},
		{Key: "epoch", Value: int64(42)},
		{Key: "state_seq", Value: int64(41)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("reply\n got %s\nwant %s", hex.EncodeToString(raw), hex.EncodeToString(want))
	}
	got, err := DecodeRingReply(raw)
	if err != nil || len(got.Keys) != 2 || got.Keys[1] != "linus" || len(got.Priorities) != 2 || got.Priorities[1] != -2 {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	plain := u85KeyedReply()
	plain.Keys, plain.Priorities = nil, nil
	rawPlain := mustU65Reply(t, plain)
	if bytes.Contains(rawPlain, []byte("keys")) || bytes.Contains(rawPlain, []byte("priorities")) {
		t.Fatalf("a reply without keys carries the members: %x", rawPlain)
	}
}

func TestU85ReplyKeysMustLineUpWithElements(t *testing.T) {
	cases := map[string]struct {
		edit func(*RingReply)
		said string
	}{
		"fewer keys than elements":       {func(r *RingReply) { r.Keys = r.Keys[:1]; r.Priorities = nil }, "keys"},
		"keys without elements":          {func(r *RingReply) { r.Elements = nil }, "keys"},
		"priorities without keys":        {func(r *RingReply) { r.Keys = nil }, "priorities"},
		"fewer priorities than elements": {func(r *RingReply) { r.Priorities = r.Priorities[:1] }, "priorities"},
		"empty key":                      {func(r *RingReply) { r.Keys[0] = "" }, "1 to 1024"},
		"key over 1024":                  {func(r *RingReply) { r.Keys[0] = strings.Repeat("k", 1025) }, "1 to 1024"},
	}
	for name, tc := range cases {
		reply := u85KeyedReply()
		tc.edit(&reply)
		_, err := EncodeRingReply(reply)
		if err == nil {
			t.Errorf("%s: encoder accepted it", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
	for name, doc := range map[string]bson.D{
		"empty keys array":     {{Key: "found", Value: true}, {Key: "elements", Value: []any{[]byte{5, 0, 0, 0, 0}}}, {Key: "keys", Value: []any{}}},
		"keys before elements": {{Key: "keys", Value: []any{"a"}}, {Key: "elements", Value: []any{[]byte{5, 0, 0, 0, 0}}}},
		"priorities as int32":  {{Key: "elements", Value: []any{[]byte{5, 0, 0, 0, 0}}}, {Key: "keys", Value: []any{"a"}}, {Key: "priorities", Value: []any{int32(1)}}},
	} {
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeRingReply(raw); err == nil {
			t.Errorf("%s: decoder accepted %x", name, raw)
		}
	}
}

func TestU85KeyedReplySurvivesMutation(t *testing.T) {
	seed := mustU65Reply(t, u85KeyedReply())
	rng := rand.New(rand.NewPCG(86, 86))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seed)
		reply, err := DecodeRingReply(mutated)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeRingReply(reply)
		if err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated reply was accepted, so the re-encode check never ran")
	}
}

func TestU85PackedPopResultRoundTripsAndSurvivesMutation(t *testing.T) {
	elements := [][]byte{u63Element("a"), u63Element(strings.Repeat("b", 200)), u63Element("c")}
	packed := packPopped(elements)
	got, err := unpackPopped(packed)
	if err != nil || len(got) != 3 || !bytes.Equal(got[1], elements[1]) {
		t.Fatalf("unpack = %d elements, %v", len(got), err)
	}
	if packPopped(nil) != nil {
		t.Fatal("no elements must pack to no result: an empty result means found=false")
	}
	total := 0
	for _, element := range elements {
		total += packedLen(len(element))
	}
	if total != len(packed) {
		t.Fatalf("packedLen sums to %d, the packed result is %d bytes", total, len(packed))
	}
	rng := rand.New(rand.NewPCG(87, 87))
	for range 100_000 {
		mutated := u65Mutate(rng, packed)
		parts, err := unpackPopped(mutated)
		if err != nil {
			continue
		}
		if again := packPopped(parts); !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-packs as %x", mutated, again)
		}
	}
}

// TestU85KeysMustBeUTF8: keys come back as BSON strings, which are UTF-8, so the ring takes no
// other key or entry id. [GUARD the UTF-8 check in checkVerbKey and in the store_get request]
func TestU85KeysMustBeUTF8(t *testing.T) {
	bad := "caf\xe9"
	element := u63Element("a")
	if _, err := encodePutArg(bad, element); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("put with a non-UTF-8 key = %v, want a refusal naming UTF-8", err)
	}
	if _, err := encodeTakeKeyArg(bad); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("take with a non-UTF-8 key = %v", err)
	}
	if _, err := encodeTakeEntryArg(bad); err == nil {
		t.Error("take of a non-UTF-8 entry id was accepted")
	}
	if _, err := encodeHeapPushArg(1, bad, element); err == nil {
		t.Error("heap push with a non-UTF-8 entry id was accepted")
	}
	forged := append([]byte{}, u63Doc(t, bson.D{{Key: "k", Value: "caf\xe9"}})...)
	if _, err := decodeVerbArg(shapeTakeKey, forged); err == nil {
		t.Errorf("the owner decoded a non-UTF-8 take key from %x", forged)
	}
	read := RingRead{Class: RingClassStoreGet, BucketID: 1, EncID: u65EncID, FieldID: 4, Key: bad}
	if _, err := EncodeRingKeyed(RingKeyedRequest{Read: &read}); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("store_get with a non-UTF-8 key = %v", err)
	}
	read.Key = "café"
	if _, err := EncodeRingKeyed(RingKeyedRequest{Read: &read}); err != nil {
		t.Errorf("a UTF-8 key was refused: %v", err)
	}
}

func TestU85NodeGetFieldRefusesANonUTF8Key(t *testing.T) {
	node, _ := u63Fresh(t)
	_, err := node.GetField(context.Background(), StoreGetRequest{StoreTarget: u62Target("Table"), Key: "caf\xe9"})
	u65AssertRefused(t, err, "UTF-8")
}
