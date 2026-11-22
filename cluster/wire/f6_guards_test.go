package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// The tests of this file pin the guards of fetch-through that no other test could fail: each
// one was a mutation of the review that left every test green.

// TestDeclaredExpandChecksTheSessionMayReadTheFarBucket: [GUARD a declared reference is
// authorized like a named one: a session that may not read the far bucket gets the generic
// denial and nothing is read].
func TestDeclaredExpandChecksTheSessionMayReadTheFarBucket(t *testing.T) {
	world, ctx := referenceWorld(t)
	if _, err := world.server.node.Insert(ctx, u30iDB, "Message", bson.D{{Key: "_id_", Value: "m1"}, {Key: "Author", Value: "u2"}}); err != nil {
		t.Fatal(err)
	}
	world.server.authz = denyBucket{bucket: "User"}
	_, err := world.server.serveList(ctx, RingListRequest{Database: u30iDB, Bucket: "Message",
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author"}}}})
	if text := refusalText(t, err, cluster.CodePermissionDenied); !strings.Contains(text, "grant role") {
		t.Fatalf("the refusal is not the generic denial: %s", text)
	}
	if len(world.reads) != 0 {
		t.Fatalf("%d far reads happened for a session that may not read the far bucket", len(world.reads))
	}
}

// TestExpandRetriesOnceOnAStaleEpoch: [GUARD an owner that answers STALE_EPOCH is asked once more
// from the same rows; a second stale answer is the error].
func TestExpandRetriesOnceOnAStaleEpoch(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	inner := world.server.expandFetch
	var calls atomic.Int64
	stale := &cluster.Error{Code: cluster.CodeStaleEpoch, Message: "the table moved"}
	world.server.expandFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		if calls.Add(1) == 1 {
			return Document{}, stale
		}
		return inner(ctx, ref, encID, project, local)
	}
	answers, _, _, _, err := world.server.expandWithRetry(context.Background(), world.near, expandSpecFor(), [][]string{{"u1"}}, false)
	if err != nil || len(answers) != 1 || !bson.Raw(answers[0]).Lookup("Author", "expanded").Boolean() {
		t.Fatalf("after one stale answer: %d answers, %v", len(answers), err)
	}
	always := &cluster.Error{Code: cluster.CodeStaleEpoch, Message: "still moving"}
	world.server.expandFetch = func(context.Context, bucketRef, []byte, []uint16, bool) (Document, error) { return Document{}, always }
	if _, _, _, _, err := world.server.expandWithRetry(context.Background(), world.near, expandSpecFor(), [][]string{{"u1"}}, false); err == nil {
		t.Fatal("two stale answers in a row were not an error")
	}
}

// TestExpandStoreListCutResumesAtTheNextElement: [GUARD a store list cut by the byte ceiling
// resumes after the last element it returned: every element once, none skipped].
func TestExpandStoreListCutResumesAtTheNextElement(t *testing.T) {
	world := expandServe(t)
	world.server.expandBytes = 180
	room := world.bucket(t, "Room")
	encID, _ := store.EncodeComponent("r1")
	fieldID, _ := room.storeField("Members")
	seen := map[string]int{}
	var after []byte
	pages := 0
	for {
		reply, err := world.server.read(context.Background(), room, RingRead{Class: RingClassStoreList, EncID: encID, FieldID: fieldID, Limit: 10, After: after,
			Options: RingReadOptions{Expand: []RingExpandSpec{{Bucket: "User", Fields: []string{"Name"}}}}})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, key := range reply.Keys {
			seen[key]++
		}
		if !reply.More {
			break
		}
		after = reply.Cursor
		if pages > 10 {
			t.Fatal("paging does not end")
		}
	}
	if pages < 2 {
		t.Fatalf("the ceiling did not cut the list (%d page), so the test proves nothing", pages)
	}
	for _, key := range []string{"u1", "u3", "ghost"} {
		if seen[key] != 1 {
			t.Errorf("element %s was returned %d times over %d pages, want once", key, seen[key], pages)
		}
	}
}

// shiftingSchema answers the declaration at one version for the first view of a bucket and at
// the next for every later view, as a declaration that moves between a read's two admissions.
type shiftingSchema struct {
	Schema
	views atomic.Int64
}

func (s *shiftingSchema) View(db, bucketID uint64) SchemaView {
	view := s.Schema.View(db, bucketID)
	if s.views.Add(1) > 1 {
		view.Decl.Version++
		view.Published++
		view.Seen++
	}
	return view
}

// TestCopyVerdictRefusesAReadWhoseDeclarationVersionMoved: [GUARD the second CopyVerdict compares
// the version: a bump published between the two admissions sends the read to the owner].
func TestCopyVerdictRefusesAReadWhoseDeclarationVersionMoved(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	shifting := &shiftingSchema{Schema: world.fx.schema}
	copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
	gate := NewReadGate(ReadGateConfig{Topology: copyTopo, Pairs: &cluster.PairSet{}, Clock: fixedClock{}, Lease: world.fx.lease, Schema: shifting, Slots: world.states})
	reader := NewReader(ReaderConfig{Gate: gate, KV: world.fx.kv, Holder: world.barrier, PeerReachable: func(uint16, uint64) bool { return true },
		Forward: world.fwd, Self: "n2"})
	doc, err := world.localRead(reader, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Served == servedLocal {
		t.Fatalf("a copy answered across a declaration version that moved under the read: %+v", doc)
	}
	if shifting.views.Load() < 2 {
		t.Fatalf("the gate looked at the declaration %d times, so the second admission never ran", shifting.views.Load())
	}
}

// TestTheOwnerNeverAnswersALocalReadAsACopy: [GUARD holdsCopy excludes the owner: a local read on
// the owner takes the owner's path and says so].
func TestTheOwnerNeverAnswersALocalReadAsACopy(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	doc, err := world.localRead(world.reader, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Served != servedOwner {
		t.Fatalf("the owner's local read says %q, want owner", doc.Served)
	}
	if _, ok := world.gate.CopyVerdict(ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), Local: true}); ok {
		t.Fatal("the owner was given a copy verdict")
	}
}

// optionsTrailer is [len:2 BE][doc], the trailer's layout, for a document the encoder would never write.
func optionsTrailer(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return append(binary.BigEndian.AppendUint16(nil, uint16(len(raw))), raw...)
}

// TestTheDecoderRefusesEveryNonCanonicalTrailer: [GUARD one document has one encoding: an empty or
// all-default trailer, local:false, an unknown member and members out of order are refused].
func TestTheDecoderRefusesEveryNonCanonicalTrailer(t *testing.T) {
	expand := bson.A{bson.D{{Key: "field", Value: "Author"}}}
	cases := map[string]bson.D{
		"empty document":         {},
		"local false only":       {{Key: "local", Value: false}},
		"local false and expand": {{Key: "local", Value: false}, {Key: "expand", Value: expand}},
		"unknown member":         {{Key: "local", Value: true}, {Key: "zzz", Value: int32(1)}},
		"out of order":           {{Key: "expand", Value: expand}, {Key: "local", Value: true}},
		"empty expand array":     {{Key: "local", Value: true}, {Key: "expand", Value: bson.A{}}},
	}
	for name, doc := range cases {
		if _, _, err := DecodeRingReadOptions(optionsTrailer(t, doc)); err == nil {
			t.Errorf("%s: the trailer was accepted", name)
		}
	}
	good := optionsTrailer(t, bson.D{{Key: "local", Value: true}, {Key: "expand", Value: expand}})
	if _, used, err := DecodeRingReadOptions(good); err != nil || used != len(good) {
		t.Fatalf("the canonical trailer was refused after %d bytes: %v", used, err)
	}
	if !bytes.Equal(good[:2], binary.BigEndian.AppendUint16(nil, uint16(len(good)-2))) {
		t.Fatal("the fixture's length is wrong")
	}
}

// TestTheDecoderRefusesALyingTrailerLength: [GUARD a length longer than the bytes that follow it,
// and one over the 4096 ceiling, are refused before any document is read].
func TestTheDecoderRefusesALyingTrailerLength(t *testing.T) {
	good := optionsTrailer(t, bson.D{{Key: "local", Value: true}})
	longer := bytes.Clone(good)
	binary.BigEndian.PutUint16(longer, uint16(len(good)-2+10))
	if _, _, err := DecodeRingReadOptions(longer); err == nil || !strings.Contains(err.Error(), "claims") {
		t.Fatalf("a length 10 bytes past the end: %v", err)
	}
	huge := bytes.Clone(good)
	binary.BigEndian.PutUint16(huge, ringMaxOptionsTrailer)
	if _, _, err := DecodeRingReadOptions(huge); err == nil || !strings.Contains(err.Error(), "claims") {
		t.Fatalf("a length over the ceiling: %v", err)
	}
	if _, _, err := DecodeRingReadOptions(good[:1]); err == nil {
		t.Fatal("a trailer shorter than its length word was accepted")
	}
}

// TestACancelledCallerIsNotAPartialSuccess: [GUARD when the caller's context ends between reads
// the call returns that error, never a page whose unread ids came back as missing documents].
func TestACancelledCallerIsNotAPartialSuccess(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	world.server.expandOwner = func(bucketRef, uint16) string { return "only" }
	ctx, cancelCaller := context.WithCancel(context.Background())
	var once sync.Once
	world.server.expandFetch = func(context.Context, bucketRef, []byte, []uint16, bool) (Document, error) {
		once.Do(cancelCaller)
		// A read that does not look at its context, as the local path does, still answers.
		return Document{Body: []byte{5, 0, 0, 0, 0}, Served: servedOwner}, nil
	}
	ids := make([][]string, 200)
	for i := range ids {
		ids[i] = []string{fmt.Sprintf("y%d", i)}
	}
	for attempt := 0; attempt < 20; attempt++ {
		answers, _, _, _, err := world.server.expandRows(ctx, world.near, []RingExpandSpec{{Field: "Author", Bucket: "User"}}, ids, false)
		if err == nil || !errors.Is(err, context.Canceled) || answers != nil {
			t.Fatalf("attempt %d: a cancelled caller got %d answers and %v, want the context error and no page", attempt, len(answers), err)
		}
	}
}

// TestANodeOutsideTheCopySetNeverAnswersLocally: [GUARD holdsCopy names the copy set: a node that
// is neither the owner nor one of the slot's copies has no copy to read, whatever its slot state].
func TestANodeOutsideTheCopySetNeverAnswersLocally(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	outsider := &fakeTopology{committed: testEpoch, self: "n9", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
	gate := NewReadGate(ReadGateConfig{Topology: outsider, Pairs: &cluster.PairSet{}, Clock: fixedClock{}, Lease: world.fx.lease, Schema: world.fx.schema, Slots: world.states})
	if _, ok := gate.CopyVerdict(ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), Local: true}); ok {
		t.Fatal("a node outside the copy set was given a copy verdict")
	}
}
