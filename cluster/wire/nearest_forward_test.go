package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// nearestForwardQuery is the query every forwarded-nearest test sends.
func nearestForwardQuery() store.NearestRequest {
	return store.NearestRequest{Metric: store.NearestCosine, K: 3, Field: "Vector", Vector: u90Vector(1, 0)}
}

// nearestFifoElements writes n vector elements into the fifoStore (field 4) of document id, which
// insertWithStore already made, each padded to pad bytes.
func nearestFifoElements(world *u36World, id string, n, pad int) {
	world.t.Helper()
	prefix := store.StorePrefix(world.slot, testDB, testBucket, world.encID(id), 4)
	for i := 0; i < n; i++ {
		element, err := bson.Marshal(bson.D{
			{Key: "Vector", Value: primitive.Binary{Subtype: 0x00, Data: u90Vector(float32(i+1), 1)}},
			{Key: "pad", Value: strings.Repeat("x", pad)},
		})
		if err != nil {
			world.t.Fatalf("marshalling element %d: %v", i, err)
		}
		// One batch per element: the store bounds what one batch commits atomically.
		batch := world.fx.kv.NewBatch()
		if err := batch.Set(store.FieldElementKey(prefix, storage.NSQueue, store.PositionRemainder(uint64(i+1))), element); err != nil {
			world.t.Fatalf("Set element %d: %v", i, err)
		}
		if err := batch.Commit(false); err != nil {
			world.t.Fatalf("Commit element %d: %v", i, err)
		}
		batch.Discard()
	}
}

// TestANearestAtANonOwnerIsForwardedOnceWithItsQuery (port of BUG-0766 to nearest): the owner
// receives the metric, k, the vector field and the vector at hop 1 and its answer comes back whole.
func TestANearestAtANonOwnerIsForwardedOnceWithItsQuery(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 0)
	reader := bug0766Copy(world)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", RequestID: [16]byte{7}}
	got, err := reader.Nearest(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO}, nearestForwardQuery())
	if err != nil || !reflect.DeepEqual(got, u36OwnerNearest) {
		t.Fatalf("nearest through a non-owner: %+v, %v, want the owner's %+v", got, err, u36OwnerNearest)
	}
	if len(world.fwd.fields) != 1 {
		t.Fatalf("%d forwards, want 1", len(world.fwd.fields))
	}
	sent := world.fwd.fields[0]
	if sent.Req.Hop != 1 || sent.Req.RequestID != req.RequestID {
		t.Errorf("forward carries hop %d and request id %x, want hop 1 and the client's id", sent.Req.Hop, sent.Req.RequestID)
	}
	sent.Req = ReadRequest{}
	want := FieldForward{Op: FieldOpNearest, FieldID: 4, Engine: store.FieldFIFO, Metric: store.NearestCosine, K: 3, VectorField: "Vector", Vector: u90Vector(1, 0)}
	if !reflect.DeepEqual(sent, want) {
		t.Errorf("forward is %+v, want %+v", sent, want)
	}
}

// TestAForwardedNearestOfAFieldTheOwnerDoesNotDeclareIsRefused: the owner re-checks the field the
// sender resolved, and answers the declared one from its own store.
func TestAForwardedNearestOfAFieldTheOwnerDoesNotDeclareIsRefused(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 3, 0)
	nearestFifoElements(world, "p1", 3, 0)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", FromNode: true, Hop: 1, FrameEpoch: testEpoch}
	query := func(fieldID uint16, engine store.FieldEngine) FieldForward {
		return FieldForward{Req: req, Op: FieldOpNearest, FieldID: fieldID, Engine: engine, Metric: store.NearestCosine, K: 3, VectorField: "Vector", Vector: u90Vector(1, 0)}
	}
	for name, forwarded := range map[string]FieldForward{
		"declared a fifoStore": query(4, store.FieldHash),
		"not declared":         query(9, store.FieldFIFO),
		"a scalar":             query(3, store.FieldFIFO),
	} {
		if _, err := world.reader.ServeFieldForward(context.Background(), forwarded); err == nil {
			t.Errorf("%s: a stale sender declaration was served", name)
		} else {
			u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
		}
	}
	got, err := world.reader.ServeFieldForward(context.Background(), query(4, store.FieldFIFO))
	if err != nil || got.Nearest.Scanned != 3 || len(got.Nearest.Elements) != 3 || len(got.Nearest.Scores) != 3 {
		t.Fatalf("the declared field answers %+v, %v, want 3 scanned and 3 kept", got.Nearest, err)
	}
}

// TestANearestConfirmPassDiscardsTheFirstAnswer: a declaration that moves under the read makes
// the read run its body again on a new snapshot; the answer is the second run's alone, never
// the two appended (the confirm pass of BUG-0766).
func TestANearestConfirmPassDiscardsTheFirstAnswer(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 3, 0)
	nearestFifoElements(world, "p1", 3, 0)
	base := world.fx.schema.viewCount()
	bodyRuns := 0
	world.fx.schema.beforeView = func(n int, s *fakeSchema) {
		if n == base+2 { // the confirmation's declaration read, once
			// A new effective version, published and seen: the answer read under the old one is not confirmed.
			s.decl.Version++
			s.published, s.seen = s.decl.Version, s.decl.Version
			bodyRuns++
		}
	}
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1"}
	query := nearestForwardQuery()
	query.K = 10
	got, err := world.reader.Nearest(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO}, query)
	if err != nil {
		t.Fatalf("Nearest: %v", err)
	}
	if bodyRuns != 1 {
		t.Fatalf("the declaration moved %d times, the rig wants exactly 1", bodyRuns)
	}
	if len(got.Elements) != 3 || len(got.Scores) != 3 || got.Scanned != 3 {
		t.Fatalf("answer holds %d elements, %d scores, scanned %d after a confirm pass over 3 elements; a first answer was kept", len(got.Elements), len(got.Scores), got.Scanned)
	}
}

// TestAForwardedNearestAboveTheForwardedReplyCapIsRefusedInTrueWords: a peer link carries 8 MiB
// of elements, so the owner refuses what a client's frame would still carry, as peek does.
func TestAForwardedNearestAboveTheForwardedReplyCapIsRefusedInTrueWords(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 10, 0)
	nearestFifoElements(world, "p1", 10, 1<<20-100)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1"}
	query := nearestForwardQuery()
	query.K = 10
	client, err := world.reader.Nearest(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO}, query)
	if err != nil || len(client.Elements) != 10 {
		t.Fatalf("a client's nearest of 10 MiB: %d elements, %v, want all 10 (the client frame carries it)", len(client.Elements), err)
	}
	req.FromNode, req.Hop, req.FrameEpoch = true, 1, testEpoch
	_, err = world.reader.ServeFieldForward(context.Background(), FieldForward{Req: req, Op: FieldOpNearest, FieldID: 4, Engine: store.FieldFIFO,
		Metric: query.Metric, K: query.K, VectorField: query.Field, Vector: query.Vector})
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
		t.Fatalf("a forwarded nearest of 10 MiB: %v, want INVALID_ARGUMENT", err)
	}
	for _, want := range []string{fmt.Sprint(forwardedReplyBytes), "Nothing was returned", "smaller k"} {
		if !strings.Contains(coded.Message, want) {
			t.Errorf("the refusal %q does not contain %q", coded.Message, want)
		}
	}
}

// TestAForwardedNearestBodyRoundTrips: the request and the answer decode to what was encoded, no
// truncation decodes, and every length the body states is checked against what remains.
func TestAForwardedNearestBodyRoundTrips(t *testing.T) {
	req := ReadRequest{BucketID: 41, ID: "p1", EncID: []byte{1, 2, 3}, RequestID: [16]byte{4}, Hop: 1, ClientEpoch: 9}
	for _, forwarded := range []FieldForward{
		{Req: req, Op: FieldOpNearest, FieldID: 5, Engine: store.FieldHash, Metric: store.NearestL2, K: 100, VectorField: "Vector", Vector: u90Vector(1, 2, 3)},
		{Req: req, Op: FieldOpNearest, FieldID: 6, Engine: store.FieldFIFO, Metric: store.NearestDot, K: 1, VectorField: strings.Repeat("v", 255), Vector: bytes.Repeat([]byte{1, 0, 0, 0}, 4096)},
	} {
		raw, err := EncodeForwardReadBody(ForwardReadBody{Req: forwarded.Req, Field: &forwarded})
		if err != nil {
			t.Fatalf("encoding %d vector bytes: %v", len(forwarded.Vector), err)
		}
		got, err := DecodeForwardReadBody(raw)
		if err != nil || got.Field == nil || !reflect.DeepEqual(*got.Field, forwarded) {
			t.Fatalf("decodes to %+v, %v, want %+v", got.Field, err, forwarded)
		}
		for cut := 0; cut < len(raw); cut++ {
			if _, err := DecodeForwardReadBody(raw[:cut]); err == nil {
				t.Fatalf("a body cut to %d of %d bytes decoded", cut, len(raw))
			}
		}
		if _, err := DecodeForwardReadBody(append(bytes.Clone(raw), 0)); err == nil {
			t.Fatal("a body with a trailing byte decoded")
		}
	}
	good := FieldForward{Req: req, Op: FieldOpNearest, FieldID: 5, Engine: store.FieldHash, Metric: store.NearestCosine, K: 3, VectorField: "Vector", Vector: u90Vector(1, 0)}
	for name, mutate := range map[string]func(*FieldForward){
		"a vector over 16384 bytes":   func(f *FieldForward) { f.Vector = make([]byte, 16388) },
		"an empty vector":             func(f *FieldForward) { f.Vector = nil },
		"a vector of 3 bytes":         func(f *FieldForward) { f.Vector = []byte{1, 2, 3} },
		"a vector field of 256 bytes": func(f *FieldForward) { f.VectorField = strings.Repeat("v", 256) },
		"an empty vector field":       func(f *FieldForward) { f.VectorField = "" },
		"k of 0":                      func(f *FieldForward) { f.K = 0 },
		"k of 101":                    func(f *FieldForward) { f.K = 101 },
		"metric 0":                    func(f *FieldForward) { f.Metric = 0 },
		"metric 4":                    func(f *FieldForward) { f.Metric = 4 },
	} {
		bad := good
		mutate(&bad)
		if raw, err := EncodeForwardReadBody(ForwardReadBody{Req: bad.Req, Field: &bad}); err == nil {
			// An encoder may leave range checks to the decoder; the decoder must refuse then.
			if _, err := DecodeForwardReadBody(raw); err == nil {
				t.Errorf("%s was encoded and decoded", name)
			}
		}
	}

	answer := FieldAnswer{Nearest: NearestAnswer{
		FieldElements: FieldElements{Elements: [][]byte{[]byte("a"), []byte("bb")}, Keys: []string{"k1", "k2"}, Priorities: []int64{-3, 9}},
		Scores:        []float64{0.75, -0.5}, Scanned: 1 << 33, Skipped: 4,
	}}
	raw, err := EncodeFieldAnswer(FieldOpNearest, answer)
	if err != nil {
		t.Fatalf("encoding the answer: %v", err)
	}
	got, err := DecodeFieldAnswer(FieldOpNearest, raw)
	if err != nil || !reflect.DeepEqual(got, answer) {
		t.Fatalf("answer decodes to %+v, %v, want %+v", got, err, answer)
	}
	for cut := 0; cut < len(raw); cut++ {
		if _, err := DecodeFieldAnswer(FieldOpNearest, raw[:cut]); err == nil {
			t.Fatalf("an answer cut to %d of %d bytes decoded", cut, len(raw))
		}
	}
	if _, err := DecodeFieldAnswer(FieldOpNearest, append(bytes.Clone(raw), 0)); err == nil {
		t.Fatal("an answer with a trailing byte decoded")
	}
	// A score count that is not the element count, and one larger than the bytes that remain.
	for name, scores := range map[string]uint32{"fewer scores than elements": 1, "more scores than elements": 3, "a score count no body could hold": 1 << 31} {
		forged := forgedNearestScores(t, answer, scores)
		if _, err := DecodeFieldAnswer(FieldOpNearest, forged); err == nil {
			t.Errorf("%s decoded", name)
		}
	}
}

// forgedNearestScores is a nearest answer whose score group states count scores but carries
// the scores of answer.
func forgedNearestScores(t *testing.T, answer FieldAnswer, count uint32) []byte {
	t.Helper()
	honest, err := EncodeFieldAnswer(FieldOpNearest, answer)
	if err != nil {
		t.Fatal(err)
	}
	scores := answer.Nearest.Scores
	tail := 4 + 8*len(scores) + 16 // [count:4][scores][scanned:8][skipped:8]
	at := len(honest) - tail
	if binary.BigEndian.Uint32(honest[at:]) != uint32(len(scores)) {
		t.Fatalf("the score group is not where the layout says: the encoder's layout changed")
	}
	forged := bytes.Clone(honest)
	binary.BigEndian.PutUint32(forged[at:], count)
	return forged
}
