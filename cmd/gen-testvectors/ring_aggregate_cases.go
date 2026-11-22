package main

import (
	"github.com/hoarddb/hoarddb-bson/bson"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// aggregateVectors are the maintained-aggregate exchanges: a create_bucket that declares them,
// a describe that lists them, the four reads of read class 0x8B and the refusals of an undeclared
// aggregate. Added last, so the request ids of every earlier vector stay what they were.
func (b *ringBuilder) aggregateVectors(stamp func(wire.RingReply) wire.RingReply) {
	fields := ringFieldTable["admin"]
	replyFields := ringFieldTable["admin reply"]
	create := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminCreateBucket, Database: "app", Name: "Property",
		Fields: []wire.RingAdminField{{Name: "Name", Type: "string"}, {Name: "Expenses", Type: "btreeStore"}},
		Aggregates: []wire.AggregateDecl{{Store: "Expenses", Kind: "sum", Field: "Amount"},
			{Store: "Expenses", Kind: "sum by", Field: "Amount", By: "Currency"}, {Store: "Expenses", Kind: "count by", By: "Category"}}}))
	b.exchange("ring admin: create a bucket with aggregates", "aggregates is a list of {store, kind, field, by} after fields: kind is sum, sum by or count by, field is the summed element path (absent for count by) and by the grouping path (absent for sum). The reply's aggregates list each one with its id and state.",
		transport.OpRingAdmin, fields, create, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminCreateBucket, BucketID: 0x1f4a2c9b7de30016, State: "active",
			DeclVersion: 1, SchemaVersionSeen: 1,
			Fields: []wire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Expenses", Type: "btreeStore"}},
			Aggregates: []wire.RingAdminReplyAggregate{{Store: "Expenses", ID: 1, Kind: "sum", Field: "Amount", State: "ready"},
				{Store: "Expenses", ID: 2, Kind: "sum by", Field: "Amount", By: "Currency", State: "ready"}, {Store: "Expenses", ID: 3, Kind: "count by", By: "Category", State: "ready"}}})))

	encID, err := store.EncodeComponent("p1")
	if err != nil {
		b.err = err
		return
	}
	read := func(aggregate wire.RingAggregateRead, limit uint16, after []byte) []byte {
		return b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000},
			Read: &wire.RingRead{Class: wire.RingClassAggregate, BucketID: 0x1f4a2c9b7de30016, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID, FieldID: 3,
				Limit: limit, After: after, Aggregate: &aggregate}}))
	}
	found := true
	doc := func(d bson.D) []byte { return b.must(bson.Marshal(d)) }
	b.exchange("ring store: sum", "Read class 0x8B names the store field, then kind 1 (sum), the summed path and no by. One number: an int64 until a double was added to the store, a double after; 0 for an empty store.",
		transport.OpRingKeyed, nil, read(wire.RingAggregateRead{Kind: wire.AggregateReadSum, Field: "Amount"}, 0, nil), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Aggregate: doc(bson.D{{Key: "sum", Value: int64(175)}})}))))
	b.exchange("ring store: sum by", "Kind 2 names both paths. The reply's values are in by order, each {by, sum, n}; more and cursor page a long list, the cursor being the last by key.",
		transport.OpRingKeyed, nil, read(wire.RingAggregateRead{Kind: wire.AggregateReadSumBy, Field: "Amount", By: "Currency"}, 100, nil), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Aggregate: doc(bson.D{{Key: "values", Value: bson.A{
			bson.D{{Key: "by", Value: "EUR"}, {Key: "sum", Value: int64(50)}, {Key: "n", Value: int64(1)}},
			bson.D{{Key: "by", Value: "USD"}, {Key: "sum", Value: int64(125)}, {Key: "n", Value: int64(2)}}}}})}))))
	b.exchange("ring store: count by", "Kind 3 names the by path only; each value is {by, n}.",
		transport.OpRingKeyed, nil, read(wire.RingAggregateRead{Kind: wire.AggregateReadCountBy, By: "Category"}, 0, nil), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Aggregate: doc(bson.D{{Key: "values", Value: bson.A{
			bson.D{{Key: "by", Value: "travel"}, {Key: "n", Value: int64(3)}}}}})}))))
	b.exchange("ring store: describe aggregates", "Kind 0 names nothing and lists every aggregate the store keeps with its state and value (a by form: how many distinct values it holds, and a warning above 10000).",
		transport.OpRingKeyed, nil, read(wire.RingAggregateRead{Kind: wire.AggregateReadDescribe}, 0, nil), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Aggregate: doc(bson.D{{Key: "aggregates", Value: bson.A{
			bson.D{{Key: "id", Value: int32(1)}, {Key: "aggregate", Value: "sum Amount"}, {Key: "state", Value: "ready"}, {Key: "sum", Value: int64(175)}}}}})}))))
	b.refused("ring store: aggregate not declared", "A read of an aggregate the store does not keep is INVALID_ARGUMENT with AGGREGATE_NOT_DECLARED first, the aggregates it does keep, and the statement that declares one.",
		transport.OpRingKeyed, nil, read(wire.RingAggregateRead{Kind: wire.AggregateReadSum, Field: "Amout"}, 0, nil),
		wire.RingError{Code: hc.CodeInvalidArgument, Message: "AGGREGATE_NOT_DECLARED: app.Property.Expenses keeps no \"sum Amout\" (it keeps: [sum Amount]). Nothing was read. An aggregate is declared when the bucket is created, as in 'create bucket Property in app { Expenses btreeStore with (sum Amount, sum Amount by Currency, count by Category) }'"},
		"AGGREGATE_NOT_DECLARED")
}
