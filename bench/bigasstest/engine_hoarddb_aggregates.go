package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The declared-aggregates variant of the ring adapter (-param aggregates=on). The ledger is
// seeded with scenarios -layout-variant common+agg: Account.Expenses declares a sum of amount,
// a sum of amount by category and a count by category, Account.Invoices a sum of amount
// (model.LedgerAggregates is the one definition, shared with the seeder). No request reads them:
// the mix is the same stream, and the cost the aggregates add is the extra state key each write
// maintains. The open-time probe refuses a seed of the other variant, and the post-run audit
// reads the maintained expense sum of every account the run wrote to (audit.go, SumReader).

// ringAggregatesMode is the word the settings and the record carry for the aggregates flag.
func ringAggregatesMode(aggregates bool) string {
	if aggregates {
		return "on"
	}
	return "off"
}

// aggregateDescriber is the part of the client the probe calls: the aggregates a bucket declares.
type aggregateDescriber interface {
	Aggregates(ctx context.Context, bucket string) ([]ringclient.AggregateInfo, error)
}

// Aggregates describes bucket and returns the aggregates it declares.
func (c clientReads) Aggregates(ctx context.Context, bucket string) ([]ringclient.AggregateInfo, error) {
	info, err := c.client.DescribeBucket(ctx, c.database, bucket)
	if err != nil {
		return nil, fmt.Errorf("describing bucket %s: %w", bucket, err)
	}
	return info.Aggregates, nil
}

// ledgerAggregateCount is how many aggregates a +agg ledger declares in all.
func ledgerAggregateCount() int {
	count := 0
	for _, list := range model.LedgerAggregates() {
		count += len(list)
	}
	return count
}

// probeAggregates checks that the ledger on the server declares the aggregates -param
// aggregates asks for: all of model.LedgerAggregates, every one ready, when on, none when off.
// The seeded schema has no marker, so the bucket's own declaration is the evidence. A layout
// with no aggregates says nothing.
func probeAggregates(ctx context.Context, describer aggregateDescriber, layout model.Layout, aggregates bool) error {
	if layout != model.Ledger {
		return nil
	}
	declared, err := describer.Aggregates(ctx, "Account")
	if err != nil {
		return fmt.Errorf("ring: reading the aggregates of bucket Account to probe the layout variant: %w", err)
	}
	want := 0
	if aggregates {
		want = ledgerAggregateCount()
	}
	if len(declared) != want {
		have, flag, fix := "common", "off", "common"
		if len(declared) > 0 {
			have = "common+agg"
		}
		if aggregates {
			flag, fix = "on", "common+agg"
		}
		return fmt.Errorf("bucket Account declares %d aggregates but -param aggregates=%s was given: this server holds the %s ledger. Reseed with -layout-variant %s or change the flag", len(declared), flag, have, fix)
	}
	for _, aggregate := range declared {
		if aggregate.State != "ready" {
			return fmt.Errorf("the %s aggregate of %s.%s is %s, not ready. Wait for the server to finish building it, then start the run again", aggregate.Kind, "Account", aggregate.Store, aggregate.State)
		}
	}
	return nil
}

// ReadSum implements SumReader: the maintained sum of the element field over one store of one
// document, which the server keeps in the same batch as each element. It is not counted in any
// round trip total.
func (o *ringOps) ReadSum(ctx context.Context, ref StoreRef, field string) (int64, error) {
	sum, err := o.client.Bucket(o.database, ref.Bucket).Sum(ctx, ref.Owner, ref.Store, field)
	if err != nil {
		return 0, fmt.Errorf("ring: reading the maintained sum of %s over %s of %s %s: %w", field, ref.Store, ref.Bucket, ref.Owner, err)
	}
	if sum.IsFloat {
		return 0, fmt.Errorf("ring: the maintained sum of %s over %s of %s %s is a double, but the ledger's amounts are whole numbers", field, ref.Store, ref.Bucket, ref.Owner)
	}
	return sum.Int, nil
}

// totalsPageLimit is the by values one sum-by frame asks for; the ledger has six categories, so
// one frame answers, and a seed with more is read page by page and counted per frame.
const totalsPageLimit = 100

// ledgerTotals answers ledger.totals_by_category from the declared aggregate "sum amount by
// category", whose rows also hold the count: one read of one state range, not a scan of the
// expenses. It needs the +agg seed; without it the server refuses the read naming the aggregates
// the field keeps, and this says how to get them.
func (o *ringOps) ledgerTotals(ctx context.Context, account string) ([]CategoryTotal, int, error) {
	if !o.aggregates {
		return nil, 0, fmt.Errorf("ring: ledger.totals_by_category reads the declared aggregates. Seed with -layout-variant common+agg and run with -param aggregates=on")
	}
	var totals []CategoryTotal
	var after []byte
	trips := 0
	for {
		page, err := o.bucket("Account").SumBy(ctx, account, "Expenses", "amount", "category", totalsPageLimit, after)
		trips++
		if err != nil {
			return nil, trips, fmt.Errorf("ring: reading the sum of amount by category of Expenses of %s: %w", account, err)
		}
		rows, err := parseSumByRows(page.Document)
		if err != nil {
			return nil, trips, fmt.Errorf("ring: the sum by category of %s: %w", account, err)
		}
		totals = append(totals, rows...)
		if !page.More {
			break
		}
		after = page.Cursor
	}
	sort.Slice(totals, func(i, j int) bool { return totals[i].Category < totals[j].Category })
	return totals, trips, nil
}

// parseSumByRows reads the rows of a sum-by answer: {values: [{by, sum, n}, ...]}.
func parseSumByRows(doc bson.Raw) ([]CategoryTotal, error) {
	values, err := doc.LookupErr("values")
	if err != nil {
		return nil, fmt.Errorf("the answer has no values: %w", err)
	}
	array, ok := values.ArrayOK()
	if !ok {
		return nil, fmt.Errorf("values is a %s, not an array", values.Type)
	}
	rows, err := array.Values()
	if err != nil {
		return nil, fmt.Errorf("reading the values array: %w", err)
	}
	out := make([]CategoryTotal, 0, len(rows))
	for i, row := range rows {
		document, ok := row.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("value %d is a %s, not a document", i, row.Type)
		}
		by, ok := document.Lookup("by").StringValueOK()
		if !ok {
			return nil, fmt.Errorf("value %d has a by that is not text", i)
		}
		sum, ok := document.Lookup("sum").Int64OK()
		if !ok {
			return nil, fmt.Errorf("value %d (%s) has a sum that is not a whole number, but the ledger's amounts are", i, by)
		}
		count, ok := document.Lookup("n").Int64OK()
		if !ok {
			return nil, fmt.Errorf("value %d (%s) has no count n", i, by)
		}
		out = append(out, CategoryTotal{Category: by, Total: sum, Count: count})
	}
	return out, nil
}
