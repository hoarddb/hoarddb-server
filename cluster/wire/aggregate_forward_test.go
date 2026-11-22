package wire

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// aggregateWorld gives the world's Posts store (field 4) the aggregates sum Amount and
// count by Status, as its own declaration holds them.
func aggregateWorld(t *testing.T) *u36World {
	world := newU36World(t)
	world.withStoreSchema()
	world.fx.schema.decl.Aggregates = machine.AggregateTable{Sets: []machine.AggregateSet{{FieldID: 4, NextID: 3, Entries: []machine.AggregateEntry{
		{ID: 1, Kind: machine.AggSum, Field: "Amount"}, {ID: 2, Kind: machine.AggCountBy, By: "Status"}}}}}
	world.insertWithStore("p1", 0, 0)
	return world
}

// TestAnAggregateReadAtANonOwnerIsForwardedOnceWithItsQuery: the owner receives the kind, the paths, the
// limit and the cursor at hop 1, and its answer comes back whole, as for every other field read.
func TestAnAggregateReadAtANonOwnerIsForwardedOnceWithItsQuery(t *testing.T) {
	world := aggregateWorld(t)
	reader := bug0766Copy(world)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", RequestID: [16]byte{7}}
	query := RingAggregateRead{Kind: AggregateReadCountBy, By: "Status"}
	got, err := reader.AggregateOf(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO}, query, 25, []byte("after"))
	if err != nil || string(got.Document) != "owner-aggregate" || !got.More || string(got.Cursor) != "c" {
		t.Fatalf("aggregate through a non-owner: %+v, %v, want the owner's answer", got, err)
	}
	if len(world.fwd.fields) != 1 {
		t.Fatalf("%d forwards, want 1", len(world.fwd.fields))
	}
	sent := world.fwd.fields[0]
	if sent.Req.Hop != 1 || sent.Op != FieldOpAggregate {
		t.Errorf("forward carries hop %d op %d, want hop 1 and FieldOpAggregate", sent.Req.Hop, sent.Op)
	}
	if sent.Aggregate != query || sent.Limit != 25 || string(sent.After) != "after" {
		t.Errorf("forward carries %+v limit %d after %q, want the query, 25 and after", sent.Aggregate, sent.Limit, sent.After)
	}
}

// TestAForwardedAggregateIsResolvedFromTheOwnersDeclaration: the owner reads the aggregate list from the
// declaration it admitted the read with; an aggregate its declaration does not keep is refused by name.
func TestAForwardedAggregateIsResolvedFromTheOwnersDeclaration(t *testing.T) {
	world := aggregateWorld(t)
	req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", FromNode: true, Hop: 1, FrameEpoch: testEpoch}
	forward := func(read RingAggregateRead) (FieldAnswer, error) {
		return world.reader.ServeFieldForward(context.Background(), FieldForward{Req: req, Op: FieldOpAggregate, FieldID: 4, Engine: store.FieldFIFO, Aggregate: read})
	}
	got, err := forward(RingAggregateRead{Kind: AggregateReadSum, Field: "Amount"})
	if err != nil || len(got.Aggregate.Document) == 0 {
		t.Fatalf("a declared sum answers %+v, %v", got, err)
	}
	_, err = forward(RingAggregateRead{Kind: AggregateReadSum, Field: "Other"})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "AGGREGATE_NOT_DECLARED") || !strings.Contains(refusal.Message, "sum Amount") {
		t.Fatalf("an undeclared sum = %v, want AGGREGATE_NOT_DECLARED listing the owner's aggregates", err)
	}
	if _, err := world.reader.ServeFieldForward(context.Background(), FieldForward{Req: req, Op: FieldOpAggregate, FieldID: 4, Engine: store.FieldHash, Aggregate: RingAggregateRead{Kind: AggregateReadDescribe}}); err == nil {
		t.Fatal("a stale sender engine was served")
	}
}

// TestAForwardedAggregateBodyRoundTrips: the request and the answer decode to what was encoded, no
// truncation decodes, and a malformed kind is refused.
func TestAForwardedAggregateBodyRoundTrips(t *testing.T) {
	req := ReadRequest{BucketID: 41, ID: "p1", EncID: []byte{1, 2, 3}, RequestID: [16]byte{4}, Hop: 1, ClientEpoch: 9}
	for _, forwarded := range []FieldForward{
		{Req: req, Op: FieldOpAggregate, FieldID: 5, Engine: store.FieldHash, Aggregate: RingAggregateRead{Kind: AggregateReadSumBy, Field: "Amount.Cents", By: "Shop.Region"}, Limit: 100, After: []byte{9, 9}},
		{Req: req, Op: FieldOpAggregate, FieldID: 6, Engine: store.FieldFIFO, Aggregate: RingAggregateRead{Kind: AggregateReadDescribe}},
		{Req: req, Op: FieldOpAggregate, FieldID: 6, Engine: store.FieldFIFO, Aggregate: RingAggregateRead{Kind: AggregateReadSum, Field: "A"}},
	} {
		raw, err := EncodeForwardReadBody(ForwardReadBody{Req: forwarded.Req, Field: &forwarded})
		if err != nil {
			t.Fatal(err)
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
	bad := FieldForward{Req: req, Op: FieldOpAggregate, FieldID: 5, Engine: store.FieldHash, Aggregate: RingAggregateRead{Kind: AggregateReadSum}}
	if raw, err := EncodeForwardReadBody(ForwardReadBody{Req: bad.Req, Field: &bad}); err == nil {
		if _, err := DecodeForwardReadBody(raw); err == nil {
			t.Error("a sum with no field was encoded and decoded")
		}
	}
	answer := FieldAnswer{Aggregate: AggregateAnswer{Document: []byte{5, 0, 0, 0, 0}, More: true, Cursor: []byte{1, 2}}}
	raw, err := EncodeFieldAnswer(FieldOpAggregate, answer)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFieldAnswer(FieldOpAggregate, raw)
	if err != nil || !reflect.DeepEqual(got, answer) {
		t.Fatalf("answer decodes to %+v, %v, want %+v", got, err, answer)
	}
	for cut := 0; cut < len(raw); cut++ {
		if _, err := DecodeFieldAnswer(FieldOpAggregate, raw[:cut]); err == nil {
			t.Fatalf("an answer cut to %d of %d bytes decoded", cut, len(raw))
		}
	}
	if _, err := DecodeFieldAnswer(FieldOpAggregate, append(bytes.Clone(raw), 0)); err == nil {
		t.Fatal("an answer with a trailing byte decoded")
	}
}
