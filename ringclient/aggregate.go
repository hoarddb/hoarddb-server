package ringclient

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster/wire"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// AggregateQuery names one maintained aggregate of a store field, or asks for all of them.
// Kind is "sum", "sum by", "count by" or "describe"; Field is the summed element path (sum and
// sum by), By the grouping path (sum by and count by). Limit and After page a by form: a Limit
// of 0 takes the server's default of 100, at most 10000.
type AggregateQuery struct {
	Kind, Field, By string
	Limit           int
	After           []byte
}

// AggregatePage is an aggregate read's answer: the BSON document ({sum}, {values: [...]} or
// {aggregates: [...]}), and for a by form whether more values follow and the cursor to resume.
type AggregatePage struct {
	Document bson.Raw
	More     bool
	Cursor   []byte
}

// AggregateNumber is a sum: an int64 until a double was added to the store, a float64 after.
type AggregateNumber struct {
	Int     int64
	Float   float64
	IsFloat bool
}

var aggregateKinds = map[string]byte{
	"describe": wire.AggregateReadDescribe, "sum": wire.AggregateReadSum, "sum by": wire.AggregateReadSumBy, "count by": wire.AggregateReadCountBy,
}

// Aggregate reads a maintained aggregate of a store field of document id. The aggregate must be
// declared on the field when the bucket is created; an undeclared one is refused naming the ones
// the field keeps. A hashStore, btreeStore, heapStore, fifoStore or lifoStore field takes it.
func (b *Bucket) Aggregate(ctx context.Context, id, field string, query AggregateQuery) (AggregatePage, error) {
	kind, ok := aggregateKinds[query.Kind]
	if !ok {
		return AggregatePage{}, fmt.Errorf("ringclient: Aggregate kind %q is not describe, sum, sum by or count by", query.Kind)
	}
	if query.Limit < 0 || query.Limit > 10000 {
		return AggregatePage{}, fmt.Errorf("ringclient: Aggregate asks for limit %d; ask for 0 (the default 100) to 10000", query.Limit)
	}
	reply, err := b.storeRead(ctx, id, field, wire.RingClassAggregate, func(read *wire.RingRead) {
		read.Aggregate = &wire.RingAggregateRead{Kind: kind, Field: query.Field, By: query.By}
		read.Limit, read.After = uint16(query.Limit), query.After
	})
	if err != nil {
		return AggregatePage{}, err
	}
	if len(reply.Aggregate) == 0 {
		return AggregatePage{}, fmt.Errorf("ringclient: the server answered an aggregate read of %s without a document", field)
	}
	return AggregatePage{Document: bson.Raw(reply.Aggregate), More: reply.More, Cursor: reply.Cursor}, nil
}

// Sum returns the maintained sum of elementField over a store field: 0 for an empty store.
func (b *Bucket) Sum(ctx context.Context, id, field, elementField string) (AggregateNumber, error) {
	page, err := b.Aggregate(ctx, id, field, AggregateQuery{Kind: "sum", Field: elementField})
	if err != nil {
		return AggregateNumber{}, err
	}
	sumAnswer, err := page.Document.LookupErr("sum")
	if err != nil {
		return AggregateNumber{}, fmt.Errorf("ringclient: the sum answer of %s has no sum: %w", field, err)
	}
	if whole, ok := sumAnswer.Int64OK(); ok {
		return AggregateNumber{Int: whole}, nil
	}
	if real, ok := sumAnswer.DoubleOK(); ok {
		return AggregateNumber{Float: real, IsFloat: true}, nil
	}
	return AggregateNumber{}, fmt.Errorf("ringclient: the sum answer of %s is a %s, not a number", field, sumAnswer.Type)
}

// SumBy returns one page of the sums of elementField per value of by, in by order.
func (b *Bucket) SumBy(ctx context.Context, id, field, elementField, by string, limit int, after []byte) (AggregatePage, error) {
	return b.Aggregate(ctx, id, field, AggregateQuery{Kind: "sum by", Field: elementField, By: by, Limit: limit, After: after})
}

// CountBy returns one page of the element counts per value of by, in by order.
func (b *Bucket) CountBy(ctx context.Context, id, field, by string, limit int, after []byte) (AggregatePage, error) {
	return b.Aggregate(ctx, id, field, AggregateQuery{Kind: "count by", By: by, Limit: limit, After: after})
}

// DescribeStore returns every aggregate a store field keeps and its current value.
func (b *Bucket) DescribeStore(ctx context.Context, id, field string) (bson.Raw, error) {
	page, err := b.Aggregate(ctx, id, field, AggregateQuery{Kind: "describe"})
	return page.Document, err
}
