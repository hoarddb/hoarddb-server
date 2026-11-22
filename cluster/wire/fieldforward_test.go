package wire

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// bug0766Reads runs every store read of document p1 on reader, naming each so a failure says which.
func bug0766Reads(world *u36World, reader *Reader, req ReadRequest) map[string]func() error {
	return map[string]func() error{
		"list": func() error {
			_, err := reader.ListField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO, Limit: 5})
			return err
		},
		"count": func() error {
			_, err := reader.CountField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO})
			return err
		},
		"get": func() error {
			_, _, err := reader.GetField(context.Background(), FieldRead{Request: req, FieldID: 5, Engine: store.FieldHash}, "k")
			return err
		},
		"peek": func() error {
			_, err := reader.Peek(context.Background(), PeekField{Request: req, FieldID: 4, Engine: store.FieldFIFO, Count: 1})
			return err
		},
		"lease_get": func() error {
			_, err := reader.LeaseElements(context.Background(), LeaseField{Request: req, FieldID: 4, Engine: store.FieldFIFO})
			return err
		},
		"nearest": func() error {
			_, err := reader.Nearest(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO}, store.NearestRequest{Metric: store.NearestCosine, K: 3, Field: "Vector", Vector: u90Vector(1, 0)})
			return err
		},
	}
}

// bug0766Copy is a node that is not the owner of world's slot, over the same Badger.
func bug0766Copy(world *u36World) *Reader {
	copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
	_, reader := world.build(copyTopo, &cluster.PairSet{}, world.fx.lease)
	return reader
}

// TestAStoreReadAtANonOwnerIsForwardedOnceWithTheFieldItNames (BUG-0766): each verb goes to the
// owner once with hop 1, naming the operation and the field, and returns the owner's answer.
func TestAStoreReadAtANonOwnerIsForwardedOnceWithTheFieldItNames(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 2)
	reader := bug0766Copy(world)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", RequestID: [16]byte{7}}

	page, err := reader.ListField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO, Limit: 5, After: []byte("c")})
	if err != nil || !page.More || string(page.Next) != "n" {
		t.Fatalf("list: %+v, %v, want the owner's page", page, err)
	}
	if total, err := reader.CountField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO}); err != nil || total != 42 {
		t.Fatalf("count: %d, %v, want the owner's 42", total, err)
	}
	if element, found, err := reader.GetField(context.Background(), FieldRead{Request: req, FieldID: 5, Engine: store.FieldHash}, "k1"); err != nil || !found || string(element) != "owner" {
		t.Fatalf("get: %q %v %v, want the owner's element", element, found, err)
	}
	if peek, err := reader.Peek(context.Background(), PeekField{Request: req, FieldID: 4, Engine: store.FieldFIFO, Count: 3}); err != nil || !peek.More {
		t.Fatalf("peek: %+v, %v, want the owner's page", peek, err)
	}
	if lease, err := reader.LeaseElements(context.Background(), LeaseField{Request: req, FieldID: 4, Engine: store.FieldFIFO, Lease: [16]byte{5}}); err != nil || !lease.Found || lease.Until != 7 {
		t.Fatalf("lease_get: %+v, %v, want the owner's lease", lease, err)
	}

	want := []FieldForward{
		{Op: FieldOpList, FieldID: 4, Engine: store.FieldFIFO, Limit: 5, After: []byte("c")},
		{Op: FieldOpCount, FieldID: 4, Engine: store.FieldFIFO},
		{Op: FieldOpGet, FieldID: 5, Engine: store.FieldHash, Key: "k1"},
		{Op: FieldOpPeek, FieldID: 4, Engine: store.FieldFIFO, Count: 3},
		{Op: FieldOpLease, FieldID: 4, Engine: store.FieldFIFO, Lease: [16]byte{5}},
	}
	if len(world.fwd.fields) != len(want) {
		t.Fatalf("%d forwards for %d reads", len(world.fwd.fields), len(want))
	}
	for i, got := range world.fwd.fields {
		if got.Req.Hop != 1 || got.Req.RequestID != req.RequestID {
			t.Errorf("forward %d carries hop %d and request id %x, want hop 1 and the client's id", i, got.Req.Hop, got.Req.RequestID)
		}
		got.Req = ReadRequest{}
		if !reflect.DeepEqual(got, want[i]) {
			t.Errorf("forward %d is %+v, want %+v", i, got, want[i])
		}
	}
}

// TestAStoreReadAtTheSecondHopIsRefusedNotOwner (BUG-0766): a read a peer already forwarded
// is never forwarded again, for any verb, so two nodes that disagree about the owner do not circle.
func TestAStoreReadAtTheSecondHopIsRefusedNotOwner(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 2)
	reader := bug0766Copy(world)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", Hop: 1}
	for name, read := range bug0766Reads(world, reader, req) {
		u36Refusal(t, read(), cluster.CodeNotOwner)
		if world.fwd.count() != 0 {
			t.Fatalf("%s forwarded a read that had already been forwarded", name)
		}
	}
}

// TestAStoreReadReleasedToAnotherOwnerIsForwardedNotAnswered (BUG-0766): a read parked at the old
// owner whose hold ends with the slot owned elsewhere goes to the new owner and never answers
// from the old owner's copy.
func TestAStoreReadReleasedToAnotherOwnerIsForwardedNotAnswered(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 2)
	world.fx.setFlags(world.slot, flagSending|flagReplayed|flagStopProved)
	world.barrier.rel = HoldRelease{Owner: "n2"}
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1"}
	done := make(chan int64, 1)
	go func() {
		total, _ := world.reader.CountField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO})
		done <- total
	}()
	<-world.barrier.parked
	world.fx.topo.table = cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers("n2", "n3")})
	world.fx.setFlags(world.slot, 0)
	close(world.barrier.release)
	if total := <-done; total != 42 || world.fwd.count() != 1 {
		t.Fatalf("the old owner answered %d after %d forwards, want the new owner's 42 after one", total, world.fwd.count())
	}
}

// TestAForwardedStoreReadOfAFieldTheOwnerDoesNotDeclareIsRefused (BUG-0766): the sender resolved
// the field from its own declaration cache; an owner whose declaration says that id is another
// store, or none, refuses instead of reading the wrong keys.
func TestAForwardedStoreReadOfAFieldTheOwnerDoesNotDeclareIsRefused(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 2)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", FromNode: true, Hop: 1, FrameEpoch: testEpoch}
	for _, forwarded := range []FieldForward{
		{Req: req, Op: FieldOpCount, FieldID: 4, Engine: store.FieldHash}, // declared a fifoStore
		{Req: req, Op: FieldOpCount, FieldID: 9, Engine: store.FieldFIFO}, // not declared
		{Req: req, Op: FieldOpCount, FieldID: 3, Engine: store.FieldFIFO}, // a scalar
		{Req: req, Op: FieldOpPeek, FieldID: 5, Engine: store.FieldFIFO, Count: 1},
	} {
		_, err := world.reader.ServeFieldForward(context.Background(), forwarded)
		u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
	}
	got, err := world.reader.ServeFieldForward(context.Background(), FieldForward{Req: req, Op: FieldOpCount, FieldID: 4, Engine: store.FieldFIFO})
	if err != nil || got.Total != 2 {
		t.Fatalf("the declared field answers %+v, %v, want its count of 2", got, err)
	}
}

// TestAForwardedStoreReadBodyRoundTrips (BUG-0766): every operation's request and answer decodes to
// what was encoded, and no truncation of either decodes or panics.
func TestAForwardedStoreReadBodyRoundTrips(t *testing.T) {
	req := ReadRequest{BucketID: 41, ID: "p1", EncID: []byte{1, 2, 3}, RequestID: [16]byte{4}, Hop: 1, ClientEpoch: 9}
	forwards := []FieldForward{
		{Req: req, Op: FieldOpList, FieldID: 4, Engine: store.FieldFIFO, Limit: 100, After: []byte{0, 0, 0, 0, 0, 0, 0, 7}},
		{Req: req, Op: FieldOpList, FieldID: 4, Engine: store.FieldFIFO},
		{Req: req, Op: FieldOpCount, FieldID: 5, Engine: store.FieldHash},
		{Req: req, Op: FieldOpGet, FieldID: 5, Engine: store.FieldBTree, Key: "k\x00é"},
		{Req: req, Op: FieldOpPeek, FieldID: 6, Engine: store.FieldHeap, Count: 100},
		{Req: req, Op: FieldOpLease, FieldID: 4, Engine: store.FieldLIFO, Lease: [16]byte{1, 2, 3}},
	}
	for _, forwarded := range forwards {
		raw, err := EncodeForwardReadBody(ForwardReadBody{Req: forwarded.Req, Field: &forwarded})
		if err != nil {
			t.Fatalf("encoding %+v: %v", forwarded, err)
		}
		got, err := DecodeForwardReadBody(raw)
		if err != nil || got.Field == nil || got.Stored || !reflect.DeepEqual(*got.Field, forwarded) {
			t.Fatalf("op %d decodes to %+v, %v, want %+v", forwarded.Op, got.Field, err, forwarded)
		}
		for cut := 0; cut < len(raw); cut++ {
			if _, err := DecodeForwardReadBody(raw[:cut]); err == nil {
				t.Fatalf("op %d: a body cut to %d of %d bytes decoded", forwarded.Op, cut, len(raw))
			}
		}
		if _, err := DecodeForwardReadBody(append(bytes.Clone(raw), 0)); err == nil {
			t.Fatalf("op %d: a body with a trailing byte decoded", forwarded.Op)
		}
	}
	answers := map[FieldOp]FieldAnswer{
		FieldOpList:  {Page: FieldElements{Elements: [][]byte{[]byte("a"), []byte("b"), []byte("ccc")}, Keys: []string{"k1", "k2", "k3"}, Priorities: []int64{-3, 0, 9}, More: true, Next: []byte("k3")}},
		FieldOpPeek:  {Page: FieldElements{Elements: [][]byte{[]byte("x")}}},
		FieldOpCount: {Total: 1 << 40},
		FieldOpGet:   {Element: []byte("owner"), Found: true},
		FieldOpLease: {Lease: LeaseRead{Found: true, Until: 99, Elements: [][]byte{[]byte("m1"), []byte("m2")}}},
	}
	for op, answer := range answers {
		raw, err := EncodeFieldAnswer(op, answer)
		if err != nil {
			t.Fatalf("encoding the answer of op %d: %v", op, err)
		}
		got, err := DecodeFieldAnswer(op, raw)
		if err != nil || !reflect.DeepEqual(got, answer) {
			t.Fatalf("op %d answer decodes to %+v, %v, want %+v", op, got, err, answer)
		}
		for cut := 0; cut < len(raw); cut++ {
			if _, err := DecodeFieldAnswer(op, raw[:cut]); err == nil {
				t.Fatalf("op %d: an answer cut to %d of %d bytes decoded", op, cut, len(raw))
			}
		}
	}
	refusal := EncodeForwardRefusal(&cluster.Error{Code: cluster.CodeNotFound, Message: "gone"})
	_, err := DecodeFieldAnswer(FieldOpCount, refusal)
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeNotFound || coded.Message != "gone" {
		t.Fatalf("the owner's refusal decodes to %v", err)
	}
}

// TestAForwardedStoreReadParkedAtTheOwnerIsNotForwardedAgain (BUG-0766): a read a peer already
// forwarded (hop 1) parks at the owner, and the hold ends with the slot owned by a third node.
// The gate's own hop check does not see this path, because the read was admitted as held, so
// forwardRead is the only guard: the read is refused NOT_OWNER and goes nowhere.
func TestAForwardedStoreReadParkedAtTheOwnerIsNotForwardedAgain(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 2)
	world.fx.setFlags(world.slot, flagSending|flagReplayed|flagStopProved)
	world.barrier.rel = HoldRelease{Owner: "n3"}
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", FromNode: true, Hop: 1, FrameEpoch: testEpoch}
	errs := make(chan error, 1)
	go func() {
		_, err := world.reader.ServeFieldForward(context.Background(), FieldForward{Req: req, Op: FieldOpCount, FieldID: 4, Engine: store.FieldFIFO})
		errs <- err
	}()
	<-world.barrier.parked
	world.fx.topo.table = cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers("n3", "n2")})
	world.fx.setFlags(world.slot, 0)
	close(world.barrier.release)
	select {
	case err := <-errs:
		u36Refusal(t, err, cluster.CodeNotOwner)
	case <-time.After(5 * time.Second):
		t.Fatal("the parked forwarded read never returned")
	}
	if world.fwd.count() != 0 {
		t.Fatalf("a read already forwarded once was forwarded %d more times", world.fwd.count())
	}
}
