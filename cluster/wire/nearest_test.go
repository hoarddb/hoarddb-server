package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// u90Vector is the little-endian float32 bytes of values.
func u90Vector(values ...float32) []byte {
	out := make([]byte, 4*len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint32(out[4*i:], math.Float32bits(value))
	}
	return out
}

// u90Item is an element holding values as a BSON binary of subtype 0x00 under Vector.
func u90Item(label string, values ...float32) bson.D {
	return bson.D{{Key: "item", Value: label}, {Key: "Vector", Value: primitive.Binary{Subtype: 0x00, Data: u90Vector(values...)}}}
}

func u90Put(t *testing.T, node *Node, field, key string, element bson.D) {
	t.Helper()
	if err := node.Put(context.Background(), PutRequest{StoreTarget: u62Target(field), Key: key, Element: element}); err != nil {
		t.Fatalf("Put %s/%s: %v", field, key, err)
	}
}

// u90Table fills Table with the five-vector fixture of the store tests.
func u90Table(t *testing.T, node *Node) {
	t.Helper()
	for key, values := range map[string][]float32{"a": {1, 0}, "b": {0, 1}, "c": {-1, 0}, "d": {0.5, 0.5}, "e": {2, 0}} {
		u90Put(t, node, "Table", key, u90Item(key, values...))
	}
}

func u90Nearest(field string, k int) NearestRequest {
	return NearestRequest{StoreTarget: u62Target(field), Metric: RingMetricCosine, K: k, VectorField: "Vector", Vector: u90Vector(1, 0)}
}

func u90Codes(t *testing.T, err error, code cluster.Code) *cluster.Error {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != code {
		t.Fatalf("error = %v, want code %v", err, code)
	}
	return coded
}

func TestNearestNodeAnswersBestFirstWithKeys(t *testing.T) {
	node, _ := u63Fresh(t)
	u90Table(t, node)
	got, err := node.Nearest(context.Background(), u90Nearest("Table", 3))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Keys, ",") != "a,e,d" || got.Scanned != 5 || got.Skipped != 0 || len(got.Elements) != 3 || len(got.Scores) != 3 {
		t.Fatalf("answer = %+v, want keys a,e,d, scanned 5, skipped 0", got)
	}
	for i, want := range []float64{1, 1, 0.7071067811865475} {
		if math.Abs(got.Scores[i]-want) > 1e-9 {
			t.Errorf("score %d = %v, want %v", i, got.Scores[i], want)
		}
	}
	if u62Item(t, got.Elements[2]) != "d" {
		t.Errorf("third element = %s, want d", u62Item(t, got.Elements[2]))
	}
}

func TestNearestNodeOnQueuesAndHeapHasNoKeysOrEntryIds(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	for _, field := range []string{"Queue", "Stack"} {
		for _, entry := range []struct {
			label  string
			values []float32
		}{{"far", []float32{0, 1}}, {"near", []float32{1, 0}}} {
			if err := node.Push(ctx, PushRequest{StoreTarget: u62Target(field), Element: u90Item(entry.label, entry.values...)}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := node.Nearest(ctx, u90Nearest(field, 2))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Keys) != 0 || len(got.Elements) != 2 || u62Item(t, got.Elements[0]) != "near" {
			t.Fatalf("%s answer = %+v, want no keys and near first", field, got)
		}
	}
	for _, entry := range []struct {
		id       string
		priority int64
		values   []float32
	}{{"low", 1, []float32{0, 1}}, {"high", 9, []float32{1, 0}}} {
		if err := node.Push(ctx, PushRequest{StoreTarget: u62Target("Ranked"), Element: u90Item(entry.id, entry.values...), Priority: entry.priority, EntryID: entry.id}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := node.Nearest(ctx, u90Nearest("Ranked", 2))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Keys, ",") != "high,low" || len(got.Elements) != 2 {
		t.Fatalf("heap answer = %+v, want keys (entry ids) high,low", got)
	}
}

func TestNearestRefusesABlobStore(t *testing.T) {
	node, _ := u63Fresh(t)
	_, err := node.Nearest(context.Background(), u90Nearest("Files", 3))
	coded := u90Codes(t, err, cluster.CodeInvalidArgument)
	want := fmt.Sprintf("nearest on %s.Box.Files: the field is a blobStore, and nearest takes a fifoStore, lifoStore, heapStore, hashStore or btreeStore. "+
		"Nothing was changed. A blobStore takes count; none of push, pop, peek, list, nearest, put or take in this build", u30iDB)
	if coded.Message != want {
		t.Fatalf("message = %q, want %q", coded.Message, want)
	}
}

func TestNearestRefusesAboveTheCeilingBeforeReading(t *testing.T) {
	node, _ := u63Fresh(t)
	for _, key := range []string{"a", "b", "c", "d"} {
		u90Put(t, node, "Table", key, u90Item(key, 1, 0))
	}
	node.reader.cfg.NearestMaxElements = 3
	got, err := node.Nearest(context.Background(), u90Nearest("Table", 3))
	coded := u90Codes(t, err, cluster.CodeInvalidArgument)
	want := fmt.Sprintf("%s.Box.Table holds 4 elements and a nearest scans at most 3 in one call (query.nearest_max_elements, HOARDDB_QUERY_NEAREST_MAX_ELEMENTS). "+
		"Nothing was read. Split the entity across documents, keep fewer elements in this field, or raise query.nearest_max_elements", u30iDB)
	if coded.Message != want {
		t.Fatalf("message = %q, want %q", coded.Message, want)
	}
	if got.Scanned != 0 || len(got.Elements) != 0 {
		t.Fatalf("a refused call returned %+v", got)
	}
}

func TestNearestOfAnAbsentDocumentIsNotFound(t *testing.T) {
	node, _ := u63Fresh(t)
	req := u90Nearest("Table", 3)
	req.ID = "nobody"
	_, err := node.Nearest(context.Background(), req)
	_ = u90Codes(t, err, cluster.CodeNotFound)
}

func TestNearestMovesNoPosition(t *testing.T) {
	node, _ := u63Fresh(t)
	u90Table(t, node)
	before := fmt.Sprint(u63Position(node))
	if _, err := node.Nearest(context.Background(), u90Nearest("Table", 3)); err != nil {
		t.Fatal(err)
	}
	if after := fmt.Sprint(u63Position(node)); after != before {
		t.Fatalf("position moved from %s to %s", before, after)
	}
}

func TestNearestIsServedOverRingKeyed(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u90Table(t, env.node)
	for _, entry := range []struct {
		id       string
		priority int64
		values   []float32
	}{{"low", 1, []float32{0, 1}}, {"high", 9, []float32{1, 0}}} {
		if err := env.node.Push(context.Background(), PushRequest{StoreTarget: u62Target("Ranked"), Element: u90Item(entry.id, entry.values...), Priority: entry.priority, EntryID: entry.id}); err != nil {
			t.Fatal(err)
		}
	}
	request := func(field string) RingKeyedRequest {
		req := env.read(t, RingClassNearest, "b1", field, 0)
		req.Read.Nearest = &RingNearest{Metric: RingMetricCosine, K: 3, Field: "Vector", Vector: u90Vector(1, 0)}
		return req
	}
	reply := u66Reply(t, c.keyed(request("Table")))
	if reply.Found == nil || !*reply.Found || strings.Join(reply.Keys, ",") != "a,e,d" || len(reply.Scores) != 3 ||
		reply.Scanned == nil || *reply.Scanned != 5 || reply.Skipped == nil || *reply.Skipped != 0 {
		t.Fatalf("Table reply = %+v", reply)
	}
	ranked := u66Reply(t, c.keyed(request("Ranked")))
	if strings.Join(ranked.Keys, ",") != "high,low" || ranked.Priorities != nil || len(ranked.Scores) != 2 || ranked.Scanned == nil || *ranked.Scanned != 2 {
		t.Fatalf("Ranked reply = %+v, want entry ids high,low and no priorities", ranked)
	}
	queue := u66Reply(t, c.keyed(request("Queue")))
	if queue.Found == nil || *queue.Found || queue.Scanned == nil || *queue.Scanned != 0 || len(queue.Elements) != 0 {
		t.Fatalf("empty Queue reply = %+v, want found false and scanned 0", queue)
	}
	blob := request("Files")
	refusal := u66Refusal(t, c.keyed(blob), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "nearest takes a fifoStore") {
		t.Fatalf("blob refusal = %q", refusal.Message)
	}
}

func TestEngineVerbsNameNearest(t *testing.T) {
	node, _ := u63Fresh(t)
	for _, entry := range []struct{ field, want string }{
		{"Queue", "push, pop, peek, list, nearest, count, claim, ack, release, extend and lease_get"},
		{"Table", "put, inc, take, get, list, nearest and count"},
	} {
		verb := "put"
		if entry.field == "Table" {
			verb = "push"
		}
		var err error
		if verb == "put" {
			err = node.Put(context.Background(), PutRequest{StoreTarget: u62Target(entry.field), Key: "k", Element: u90Item("k", 1, 0)})
		} else {
			err = node.Push(context.Background(), PushRequest{StoreTarget: u62Target(entry.field), Element: u90Item("k", 1, 0)})
		}
		coded := u90Codes(t, err, cluster.CodeInvalidArgument)
		if !strings.HasSuffix(coded.Message, entry.want) {
			t.Errorf("%s refusal = %q, want the verbs %q", entry.field, coded.Message, entry.want)
		}
	}
}

// TestNearestRefusesAReplyAboveThePeekCeiling writes count elements of 1 MB, about 35 MB and above
// peekReplyBytes, and asks for all of them: the reply is refused before it is built and names the field.
// The same query for ten elements (about 10 MB) is answered.
func TestNearestRefusesAReplyAboveThePeekCeiling(t *testing.T) {
	node, _ := u63Fresh(t)
	count := peekReplyBytes/1_000_000 + 2
	pad := strings.Repeat("p", 1_000_000)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%02d", i)
		u90Put(t, node, "Table", key, bson.D{{Key: "item", Value: key}, {Key: "Vector", Value: primitive.Binary{Subtype: 0x00, Data: u90Vector(1, 0)}}, {Key: "Pad", Value: pad}})
	}
	fieldID := u63Field(t, node, "Table", store.FieldHash).FieldID
	_, err := node.Nearest(context.Background(), u90Nearest("Table", count))
	coded := u90Codes(t, err, cluster.CodeInvalidArgument)
	want := fmt.Sprintf("the nearest %d elements of field %d of b1 hold more than the %d bytes one reply carries. Nothing was returned. Ask for a smaller k",
		count, fieldID, peekReplyBytes)
	if coded.Message != want {
		t.Fatalf("message = %q, want %q", coded.Message, want)
	}
	got, err := node.Nearest(context.Background(), u90Nearest("Table", 10))
	if err != nil {
		t.Fatalf("K 10 refused: %v", err)
	}
	if len(got.Elements) != 10 {
		t.Fatalf("K 10 returned %d elements, want 10", len(got.Elements))
	}
}

func TestNearestNodeRefusesANonFiniteVectorAsInvalidArgument(t *testing.T) {
	node, _ := u63Fresh(t)
	u90Table(t, node)
	for name, vector := range map[string][]byte{
		"NaN":  u90Vector(1, float32(math.NaN())),
		"+Inf": u90Vector(float32(math.Inf(1)), 0),
		"-Inf": u90Vector(0, float32(math.Inf(-1))),
	} {
		req := u90Nearest("Table", 3)
		req.Vector = vector
		_, err := node.Nearest(context.Background(), req)
		coded := u90Codes(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(coded.Message, "NaN or infinite value at element") || !strings.Contains(coded.Message, "send finite float32 values") {
			t.Errorf("%s message = %q, want it to name the element and the fix", name, coded.Message)
		}
	}
}

// TestNearestThroughTheReaderRefusesWhereListRefuses guards the read gate on nearest: a
// nearest is a computed answer, so serving it from a pair the gate holds, from a node that is
// not the owner or without a lease would be a wrong answer, not a stale one. Every case runs
// the list twin and requires the same outcome: the same refusal, or, at a node that is not the
// owner on the first hop, the same forward to the owner and the owner's answer (BUG-0766).
func TestNearestThroughTheReaderRefusesWhereListRefuses(t *testing.T) {
	cases := []struct {
		name string
		hop  uint8
		want cluster.Code
		// forwards says the read goes to the owner once instead of being refused.
		forwards bool
		// rig returns the reader to call; it may change the world first.
		rig func(world *u36World) *Reader
	}{
		{"a held pair", 0, cluster.CodeUnavailable, false, func(world *u36World) *Reader {
			world.fx.setFlags(world.slot, flagSending|flagReplayed|flagStopProved)
			world.barrier.rel = HoldRelease{Owner: "n1", Local: true}
			close(world.barrier.release) // the hold ends at once and the pair is still held: the true refusal
			return world.reader
		}},
		{"a pair awaiting its seed", 0, cluster.CodeUnavailable, false, func(world *u36World) *Reader {
			world.fx.setFlags(world.slot, flagAwaiting|flagReplayed)
			world.barrier.rel = HoldRelease{Owner: "n1", Local: true}
			close(world.barrier.release)
			return world.reader
		}},
		{"a node that is not the owner, first hop", 0, cluster.Code(0), true, func(world *u36World) *Reader {
			copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
			_, reader := world.build(copyTopo, &cluster.PairSet{}, world.fx.lease)
			return reader
		}},
		{"a node that is not the owner, second hop", 1, cluster.CodeNotOwner, false, func(world *u36World) *Reader {
			copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
			_, reader := world.build(copyTopo, &cluster.PairSet{}, world.fx.lease)
			return reader
		}},
		{"a lapsed lease", 0, cluster.CodeLeaseLapsed, false, func(world *u36World) *Reader {
			world.fx.lease.valid.Store(false)
			return world.reader
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := newU36World(t)
			world.fx.mustWrite(world.slot, "p1")
			reader := tc.rig(world)
			field := FieldRead{
				Request: ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), Hop: tc.hop},
				FieldID: 1, Engine: store.FieldHash,
			}
			_, listErr := reader.ListField(context.Background(), field)
			answer, nearestErr := reader.Nearest(context.Background(), field, store.NearestRequest{Metric: store.NearestCosine, K: 3, Field: "Vector", Vector: u90Vector(1, 0)})
			if tc.forwards {
				if listErr != nil || nearestErr != nil {
					t.Fatalf("list %v and nearest %v, want both answered by the owner", listErr, nearestErr)
				}
				if len(answer.Elements) != 2 || answer.Scanned != u36OwnerNearest.Scanned {
					t.Fatalf("a forwarded nearest returned %+v, want the owner's answer", answer)
				}
				if world.fwd.count() != 2 {
					t.Fatalf("%d forwards for a list and a nearest, want 2", world.fwd.count())
				}
				return
			}
			u36Refusal(t, listErr, tc.want)
			u36Refusal(t, nearestErr, tc.want)
			if len(answer.Elements) != 0 || answer.Scanned != 0 {
				t.Fatalf("a refused nearest returned %+v", answer)
			}
			if world.fwd.count() != 0 {
				t.Fatalf("a refused read was forwarded %d times", world.fwd.count())
			}
		})
	}
}
