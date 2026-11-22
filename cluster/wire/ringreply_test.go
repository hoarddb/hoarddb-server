package wire

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u65Int32(n int32) *int32 { return &n }
func u65Int64(n int64) *int64 { return &n }
func u65Bool(b bool) *bool    { return &b }

func u65Lease() []byte {
	return []byte{0, 0, 0, 0, 0, 0, 0x75, 0x30, 1, 2, 3, 4, 5, 6, 7, 8}
}

func u65WriteReply() RingReply {
	position := cluster.Position{Epoch: 42, Seq: 55401}
	return RingReply{
		WriteOutcome:    "applied",
		CopiesConfirmed: u65Int32(1),
		ServedBy:        "node-a",
		Position:        &position,
		ID:              bson.RawValue{Type: 0x02, Value: []byte{3, 0, 0, 0, 'p', '1', 0}},
		Epoch:           u65Int64(42),
		StateSeq:        u65Int64(41),
	}
}

func u65EveryReply() []RingReply {
	pop := RingReply{Found: u65Bool(true), Elements: [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}}, Epoch: u65Int64(42), StateSeq: u65Int64(41)}
	claim := u65WriteReply()
	claim.ID = bson.RawValue{}
	claim.Elements = [][]byte{{5, 0, 0, 0, 0}}
	claim.Lease = u65Lease()
	deduped := u65WriteReply()
	deduped.Deduped = true
	deduped.WriteOutcome = "applied_degraded"
	deduped.CopiesConfirmed = u65Int32(0)
	return []RingReply{
		{},
		u65WriteReply(),
		pop,
		claim,
		deduped,
		{Found: u65Bool(false)},
		{WriteOutcome: "unknown", CopiesConfirmed: u65Int32(0)},
	}
}

func TestU65ReplyHasExactlyTheNoteFieldsInOrder(t *testing.T) {
	raw, err := EncodeRingReply(RingReply{
		WriteOutcome:    "applied",
		CopiesConfirmed: u65Int32(1),
		ServedBy:        "node-a",
		Position:        &cluster.Position{Epoch: 42, Seq: 55401},
		ID:              bson.RawValue{Type: 0x02, Value: []byte{3, 0, 0, 0, 'p', '1', 0}},
		Found:           u65Bool(true),
		Elements:        [][]byte{{1, 2}},
		Lease:           u65Lease(),
		Deduped:         true,
		Epoch:           u65Int64(42),
		StateSeq:        u65Int64(41),
	})
	if err != nil {
		t.Fatal(err)
	}
	want, err := bson.Marshal(bson.D{
		{Key: "write_outcome", Value: "applied"},
		{Key: "copies_confirmed", Value: int32(1)},
		{Key: "served_by", Value: "node-a"},
		{Key: "position", Value: "42:55401"},
		{Key: "id", Value: "p1"},
		{Key: "found", Value: true},
		{Key: "elements", Value: []any{[]byte{1, 2}}},
		{Key: "lease", Value: u65Lease()},
		{Key: "deduped", Value: true},
		{Key: "epoch", Value: int64(42)},
		{Key: "state_seq", Value: int64(41)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("reply\n got %x\nwant %x", raw, want)
	}
}

func TestU65ReplyOmitsAbsentFields(t *testing.T) {
	raw, err := EncodeRingReply(RingReply{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, []byte{5, 0, 0, 0, 0}) {
		t.Fatalf("empty reply is %x, want the empty document", raw)
	}
	raw, err = EncodeRingReply(RingReply{Found: u65Bool(false), CopiesConfirmed: u65Int32(0)})
	if err != nil {
		t.Fatal(err)
	}
	var seen bson.D
	if err := bson.Unmarshal(raw, &seen); err != nil || len(seen) != 2 || seen[0].Key != "copies_confirmed" || seen[1].Key != "found" {
		t.Fatalf("a present zero is not an absent field: %v, %v", seen, err)
	}
}

func TestU65ReplyRoundTripsAndReEncodesIdentically(t *testing.T) {
	for i, reply := range u65EveryReply() {
		raw, err := EncodeRingReply(reply)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		got, err := DecodeRingReply(raw)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		again, err := EncodeRingReply(got)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("reply %d re-encodes as %x (%v), want %x", i, again, err, raw)
		}
	}
	got, err := DecodeRingReply(mustU65Reply(t, u65WriteReply()))
	if err != nil {
		t.Fatal(err)
	}
	if got.WriteOutcome != "applied" || *got.CopiesConfirmed != 1 || got.ServedBy != "node-a" ||
		got.Position == nil || got.Position.Epoch != 42 || got.Position.Seq != 55401 || *got.Epoch != 42 || *got.StateSeq != 41 || got.Found != nil {
		t.Fatalf("decoded %+v", got)
	}
}

func mustU65Reply(t *testing.T, reply RingReply) []byte {
	t.Helper()
	raw, err := EncodeRingReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestU65ReplyEncoderRefusals(t *testing.T) {
	cases := map[string]struct {
		edit func(*RingReply)
		said string
	}{
		"unknown outcome":      {func(r *RingReply) { r.WriteOutcome = "maybe" }, "applied_degraded"},
		"negative copies":      {func(r *RingReply) { r.CopiesConfirmed = u65Int32(-1) }, "copies_confirmed"},
		"long served_by":       {func(r *RingReply) { r.ServedBy = strings.Repeat("n", 257) }, "served_by"},
		"lease of 24 bytes":    {func(r *RingReply) { r.Lease = make([]byte, 24) }, "16"},
		"lease of 15 bytes":    {func(r *RingReply) { r.Lease = make([]byte, 15) }, "16"},
		"negative epoch":       {func(r *RingReply) { r.Epoch = u65Int64(-1) }, "epoch"},
		"negative state_seq":   {func(r *RingReply) { r.StateSeq = u65Int64(-1) }, "state_seq"},
		"element past the cap": {func(r *RingReply) { r.Elements = [][]byte{make([]byte, maxSubScanBody)} }, "frame"},
		"empty id value":       {func(r *RingReply) { r.ID = bson.RawValue{Type: 0x02} }, "id"},
		"epoch above int64":    {func(r *RingReply) { r.Position = &cluster.Position{Epoch: math.MaxInt64 + 1} }, "position"},
	}
	for name, tc := range cases {
		reply := u65WriteReply()
		tc.edit(&reply)
		_, err := EncodeRingReply(reply)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
}

func TestU65ReplyDecoderRefusesWhatIsNotCanonical(t *testing.T) {
	marshal := func(doc bson.D) []byte {
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	cases := map[string]struct {
		raw  []byte
		said string
	}{
		"reordered fields":               {marshal(bson.D{{Key: "epoch", Value: int64(1)}, {Key: "found", Value: true}}), "layout"},
		"extra field":                    {marshal(bson.D{{Key: "found", Value: true}, {Key: "extra", Value: int32(1)}}), "layout"},
		"duplicate field":                {marshal(bson.D{{Key: "found", Value: true}, {Key: "found", Value: true}}), "layout"},
		"widened copies":                 {marshal(bson.D{{Key: "copies_confirmed", Value: int64(1)}}), "layout"},
		"narrowed epoch":                 {marshal(bson.D{{Key: "epoch", Value: int32(1)}}), "layout"},
		"explicit false deduped":         {marshal(bson.D{{Key: "deduped", Value: false}}), "layout"},
		"empty elements":                 {marshal(bson.D{{Key: "elements", Value: []any{}}}), "layout"},
		"lease of 24":                    {marshal(bson.D{{Key: "lease", Value: make([]byte, 24)}}), "16"},
		"outcome unknown":                {marshal(bson.D{{Key: "write_outcome", Value: "maybe"}}), "applied_degraded"},
		"position padded":                {marshal(bson.D{{Key: "position", Value: "042:5"}}), "position"},
		"position junk":                  {marshal(bson.D{{Key: "position", Value: "x"}}), "position"},
		"negative epoch":                 {marshal(bson.D{{Key: "epoch", Value: int64(-3)}}), "epoch"},
		"not a document":                 {[]byte{1, 2, 3}, "BSON"},
		"id nested past the depth limit": {u65Doc(0x03, "id", u65Nest(20)), "too deep"},
		"valid document past the frame":  {u65Doc(0x05, "lease", append(binary.LittleEndian.AppendUint32(nil, maxSubScanBody), make([]byte, maxSubScanBody+1)...)), "over the limit"},
		"trailing bytes":                 {append(marshal(bson.D{{Key: "found", Value: true}}), 0), "BSON"},
	}
	for name, tc := range cases {
		_, err := DecodeRingReply(tc.raw)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
}

func TestU65ReplyDecoderSurvivesMutation(t *testing.T) {
	var seeds [][]byte
	for _, reply := range u65EveryReply() {
		seeds = append(seeds, mustU65Reply(t, reply))
	}
	rng := rand.New(rand.NewPCG(66, 66))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
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

func TestU65ReplyDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	// A document that claims 2 GiB, and an elements array whose binary claims 2 GiB.
	lie := binary.LittleEndian.AppendUint32(nil, 0x7fffffff)
	lie = append(lie, 0x05, 'l', 'e', 'a', 's', 'e', 0, 0xff, 0xff, 0xff, 0x7f, 0)
	arrayLie := binary.LittleEndian.AppendUint32(nil, 40)
	arrayLie = append(arrayLie, 0x04, 'e', 'l', 'e', 'm', 'e', 'n', 't', 's', 0, 0x20, 0, 0, 0, 0x05, '0', 0, 0xff, 0xff, 0xff, 0x7f, 0, 0, 0)
	for _, input := range [][]byte{lie, arrayLie} {
		if _, err := DecodeRingReply(input); err == nil {
			t.Fatalf("lying reply %x was accepted", input)
		}
	}
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingReply(lie)
			_, _ = DecodeRingReply(arrayLie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("two lying replies of %d and %d bytes allocated %d bytes", len(lie), len(arrayLie), perOp)
	}
	if _, err := DecodeRingReply(make([]byte, maxSubScanBody+1)); err == nil {
		t.Fatal("a reply past the frame ceiling was accepted")
	}
}

// u65Nest returns a document nested depth levels deep, each level holding {a: next}.
func u65Nest(depth int) []byte {
	doc := []byte{5, 0, 0, 0, 0}
	for range depth {
		doc = u65Doc(0x03, "a", doc)
	}
	return doc
}

func TestNearestReplyRoundTrips(t *testing.T) {
	cases := []RingReply{
		{
			Found:    u65Bool(true),
			Elements: [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}},
			Keys:     []string{"a", "b"},
			Scores:   []float64{1, 0.5},
			Scanned:  u65Int32(5),
			Skipped:  u65Int32(0),
		},
		{Scanned: u65Int32(0), Skipped: u65Int32(0)},
	}
	for i, in := range cases {
		raw, err := EncodeRingReply(in)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		got, err := DecodeRingReply(raw)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, in) {
			t.Fatalf("reply %d decoded as %+v, want %+v", i, got, in)
		}
	}
	raw, err := EncodeRingReply(cases[1])
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingReply(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Skipped == nil || *got.Skipped != 0 {
		t.Fatalf("a present zero skipped was lost: %+v", got)
	}
}

func TestNearestReplyRefusals(t *testing.T) {
	two := [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}}
	cases := []struct {
		name  string
		reply RingReply
		said  string
	}{
		{"scores not parallel to elements", RingReply{Elements: two, Scores: []float64{1, 0.5, 0.25}, Scanned: u65Int32(2), Skipped: u65Int32(0)}, "parallel to elements"},
		{"scanned without skipped", RingReply{Elements: two, Scores: []float64{1, 0.5}, Scanned: u65Int32(2)}, "go together"},
		{"skipped without scanned", RingReply{Elements: two, Scores: []float64{1, 0.5}, Skipped: u65Int32(0)}, "go together"},
		{"negative scanned", RingReply{Elements: two, Scores: []float64{1, 0.5}, Scanned: u65Int32(-1), Skipped: u65Int32(0)}, "scanned is -1"},
		{"negative skipped", RingReply{Elements: two, Scores: []float64{1, 0.5}, Scanned: u65Int32(2), Skipped: u65Int32(-2)}, "skipped is -2"},
		{"elements without scores", RingReply{Elements: two, Scanned: u65Int32(2), Skipped: u65Int32(0)}, "one score per element"},
		{"scores without counts", RingReply{Elements: two, Scores: []float64{1, 0.5}}, "not a nearest reply"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeRingReply(tc.reply)
			u65AssertRefused(t, err, tc.said)
		})
	}
}

func TestNearestReplyMemberOrderIsCanonical(t *testing.T) {
	entries := cluster.AppendEntry(nil, cluster.ListEntry{Slot: 0, EncID: []byte("a"), Body: []byte{1}})
	raw, err := EncodeRingReply(RingReply{
		Elements:   [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}},
		Keys:       []string{"a", "b"},
		Priorities: []int64{1, 2},
		Scores:     []float64{1, 0.5},
		Scanned:    u65Int32(5),
		Skipped:    u65Int32(3),
		Entries:    entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, member := range doc {
		order = append(order, member.Key)
	}
	want := []string{"elements", "keys", "priorities", "scores", "scanned", "skipped", "entries"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("member order %v, want %v", order, want)
	}
	if _, err := DecodeRingReply(raw); err != nil {
		t.Fatalf("the canonical order does not decode: %v", err)
	}
}
