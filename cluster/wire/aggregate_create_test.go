package wire

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

func aggregateFields() []FieldEntry {
	return []FieldEntry{{Type: "string", Name: "Name"}, {Type: "btreeStore", Name: "Expenses"}, {Type: "fifoStore", Name: "Tickets"}, {Type: "blobStore", Name: "Data"}}
}

// TestCreateBucketWritesTheAggregateLists: ids are minted from 1 per store field (id 0
// means none in the stamp) and the lists reach the stored declaration.
func TestCreateBucketWritesTheAggregateLists(t *testing.T) {
	ring := newU39Ring()
	_, err := u39Create(ring, 0x1f4a2c9b7de30015).Run(context.Background(), u39DB, "Property", aggregateFields(),
		AggregateDecl{Store: "Expenses", Kind: "sum", Field: "Amount"},
		AggregateDecl{Store: "Expenses", Kind: "sum by", Field: "Amount", By: "Currency"},
		AggregateDecl{Store: "Tickets", Kind: "count by", By: "Status"},
		AggregateDecl{Store: "Expenses", Kind: "count by", By: "Category"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	decl, found := ring.declaration("Property")
	if !found {
		t.Fatal("no declaration written")
	}
	want := machine.AggregateTable{Sets: []machine.AggregateSet{
		{FieldID: 3, NextID: 4, Entries: []machine.AggregateEntry{
			{ID: 1, Kind: machine.AggSum, Field: "Amount"},
			{ID: 2, Kind: machine.AggSumBy, Field: "Amount", By: "Currency"},
			{ID: 3, Kind: machine.AggCountBy, By: "Category"},
		}},
		{FieldID: 4, NextID: 2, Entries: []machine.AggregateEntry{{ID: 1, Kind: machine.AggCountBy, By: "Status"}}},
	}}
	if !reflect.DeepEqual(decl.Aggregates, want) {
		t.Fatalf("aggregates = %+v, want %+v", decl.Aggregates, want)
	}
}

// TestCreateRefusesDuplicateAndOverCapAggregates: the checks live on the server, so a wire
// client that skips the CLI cannot declare nine, a duplicate, a plain count, or an aggregate
// on a scalar or a blob.
func TestCreateRefusesDuplicateAndOverCapAggregates(t *testing.T) {
	nine := make([]AggregateDecl, 9)
	for i := range nine {
		nine[i] = AggregateDecl{Store: "Expenses", Kind: "sum", Field: string(rune('a' + i))}
	}
	cases := []struct {
		name string
		decl []AggregateDecl
		want string
	}{
		{"nine", nine, "at most 8"},
		{"duplicate", []AggregateDecl{{Store: "Expenses", Kind: "sum", Field: "Amount"}, {Store: "Expenses", Kind: "sum", Field: "Amount"}}, "AGGREGATE_DUPLICATE"},
		{"scalar", []AggregateDecl{{Store: "Name", Kind: "sum", Field: "Amount"}}, "fifoStore"},
		{"blob", []AggregateDecl{{Store: "Data", Kind: "sum", Field: "Amount"}}, "a blob element has no fields"},
		{"unknown store", []AggregateDecl{{Store: "Nope", Kind: "sum", Field: "Amount"}}, "Nope"},
		{"plain count", []AggregateDecl{{Store: "Expenses", Kind: "count"}}, "every store already counts its elements"},
		{"deep path", []AggregateDecl{{Store: "Expenses", Kind: "sum", Field: "a.b.c.d"}}, "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring := newU39Ring()
			_, err := u39Create(ring, 0x1f4a2c9b7de30015).Run(context.Background(), u39DB, "Property", aggregateFields(), tc.decl...)
			message := u39Code(t, err, cluster.CodeInvalidArgument)
			if !strings.Contains(message, tc.want) {
				t.Fatalf("refusal %q does not contain %q", message, tc.want)
			}
			if len(ring.writes) != 0 {
				t.Fatalf("a refused create wrote %d documents", len(ring.writes))
			}
		})
	}
}

// TestTheAdminWireCarriesAggregates: aggregates ride create_bucket and the bucket replies, and
// a request or reply without them still re-encodes to the same bytes.
func TestTheAdminWireCarriesAggregates(t *testing.T) {
	req := RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "Property",
		Fields:     []RingAdminField{{Name: "Expenses", Type: "btreeStore"}},
		Aggregates: []AggregateDecl{{Store: "Expenses", Kind: "sum by", Field: "Amount", By: "Currency"}}}
	raw, err := EncodeRingAdmin(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingAdmin(raw)
	if err != nil || !reflect.DeepEqual(got.Aggregates, req.Aggregates) {
		t.Fatalf("decoded %+v, %v; want the aggregates back", got.Aggregates, err)
	}
	if again, err := EncodeRingAdmin(got); err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("re-encode differs: %v", err)
	}
	if _, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminDescribeBucket, Database: "app", Bucket: "x", Aggregates: req.Aggregates}); err == nil {
		t.Fatal("describe_bucket accepted an aggregates member")
	}
	reply := RingAdminReply{Cmd: AdminDescribeBucket, BucketID: 7, State: "active", DeclVersion: 1, SchemaVersionSeen: 1,
		Fields:     []RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "Expenses", Type: "btreeStore"}},
		Aggregates: []RingAdminReplyAggregate{{Store: "Expenses", ID: 1, Kind: "sum by", Field: "Amount", By: "Currency", State: "ready"}}}
	rawReply, err := EncodeRingAdminReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	gotReply, err := DecodeRingAdminReply(rawReply)
	if err != nil || !reflect.DeepEqual(gotReply.Aggregates, reply.Aggregates) {
		t.Fatalf("decoded reply %+v, %v", gotReply.Aggregates, err)
	}
	if again, err := EncodeRingAdminReply(gotReply); err != nil || !bytes.Equal(again, rawReply) {
		t.Fatalf("reply re-encode differs: %v", err)
	}
}

// idleAggregateGateFirst and idleAggregateGateTotal set the idle gate (AGENTS.md rule 11): the first thousand
// buckets absorb what the node allocates once, and the next 20,000 are what is divided. Each run creates
// idleAggregateGateTotal buckets, so the two runs together create 42,000.
const (
	idleAggregateGateFirst = 1_000
	idleAggregateGateTotal = 21_000
)

// readHeapObjectCount is the number of live heap objects the collector traces, read after two forced
// collections so that only what stays live is counted.
func readHeapObjectCount() uint64 {
	runtime.GC()
	runtime.GC()
	samples := []metrics.Sample{{Name: "/gc/heap/objects:objects"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64()
}

// idleAggregateTracedObjects opens a fresh node, creates the gate's buckets through the production create path
// and returns the traced heap objects per bucket they leave behind. Both runs use the same fields (Name and an
// Expenses btreeStore); aggregates false passes nil declarations, which is what CreateBucket does, and true
// declares `sum Amount` on Expenses. The node is closed before the function returns so the second run starts
// from the same heap as the first.
func idleAggregateTracedObjects(t *testing.T, aggregates bool) float64 {
	t.Helper()
	ctx := context.Background()
	node, err := OpenNode(ctx, NodeConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			t.Errorf("closing the node: %v", err)
		}
	}()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	fields := []Field{{Name: "Name", Type: "string"}, {Name: "Expenses", Type: "btreeStore"}}
	var decls []AggregateDecl
	if aggregates {
		decls = []AggregateDecl{{Store: "Expenses", Kind: "sum", Field: "Amount"}}
	}
	create := func(from, to int) {
		for i := from; i < to; i++ {
			if _, err := node.CreateBucketWithAggregates(ctx, u30iDB, fmt.Sprintf("Idle%06d", i), fields, decls); err != nil {
				t.Fatalf("creating idle bucket %d (aggregates %t): %v", i, aggregates, err)
			}
		}
	}
	create(0, idleAggregateGateFirst)
	before := readHeapObjectCount()
	create(idleAggregateGateFirst, idleAggregateGateTotal)
	after := readHeapObjectCount()
	return float64(int64(after)-int64(before)) / float64(idleAggregateGateTotal-idleAggregateGateFirst)
}

// TestIdleAggregatesAddNoGCTracedObjects is the rule 11 gate for maintained aggregates. The assertion is on the
// MARGINAL cost: a bucket declaring one sum on a store, minus the same bucket with no declaration, must add under
// a tenth of a traced heap object. An absolute bound would measure the cluster/wire harness, not the feature:
// a bare bucket already costs about 0.24 traced objects at 10^5 buckets here (0.76 at 8,000, falling with N), so
// an absolute 0.1 fails with the aggregate declaration absent. Measured on the same harness, the declaration adds
// about 0.0045 objects per bucket at 21,000 buckets (0.0017 at 8,000). The server's standing gate
// (TestIdleBucketsAddNoGCTracedObjects) passes at about 0.07 absolute. The declaration is written to the store as
// field 12 and cached by a bounded declaration cache, so no per-bucket object survives the create.
func TestIdleAggregatesAddNoGCTracedObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 42,000 buckets across two runs; skipped under -short")
	}
	plain := idleAggregateTracedObjects(t, false)
	declared := idleAggregateTracedObjects(t, true)
	added := declared - plain
	t.Logf("%.4f traced heap objects per plain idle bucket, %.4f per bucket with a sum aggregate, difference %.4f (%d buckets per run)",
		plain, declared, added, idleAggregateGateTotal)
	if added >= 0.1 {
		t.Errorf("an idle bucket with a sum aggregate adds %.4f heap objects the collector traces over a plain one; the limit is 0.1", added)
	}
}
