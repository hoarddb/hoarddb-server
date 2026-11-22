package cli

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// restoreOrderDeclaration is the declaration both tests recreate: a scalar, the bucket's
// expiry field, each store type, one queue field that expires its elements and the two
// aggregate forms a non-blob store accepts (aggregate kind words are "sum", "sum by" and
// "count by"; "sum" needs a Field and "count by" needs a By).
func restoreOrderDeclaration() []ringclient.FieldDecl {
	return []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Due", Type: "datetime"},
		{Name: "Lines", Type: "fifoStore", ExpirePath: "at", Aggregates: []ringclient.AggregateDecl{
			{Kind: "sum", Field: "qty"},
		}},
		{Name: "Events", Type: "hashStore", Aggregates: []ringclient.AggregateDecl{
			{Kind: "count by", By: "kind"},
		}},
		{Name: "Tags", Type: "btreeStore"},
		{Name: "Scores", Type: "heapStore"},
		{Name: "Raw", Type: "blobStore"},
	}
}

// restoreFieldShapes reduces a declaration's fields to the part a restore must reproduce:
// the wording in declared order, without the ids the ring mints for them.
func restoreFieldShapes(fields []ringclient.FieldInfo) []string {
	shapes := make([]string, len(fields))
	for index, field := range fields {
		shapes[index] = field.Name + " " + field.Type + " " + field.Of
	}
	return shapes
}

// restoreAggregateShapes reduces a declaration's aggregates to what a create carries. State
// is left out because a rebuilt aggregate may still be catching up when describe answers,
// and the id is the ring's to mint.
func restoreAggregateShapes(aggregates []ringclient.AggregateInfo) []string {
	shapes := make([]string, len(aggregates))
	for index, aggregate := range aggregates {
		shapes[index] = aggregate.Store + "|" + aggregate.Kind + "|" + aggregate.Field + "|" + aggregate.By
	}
	return shapes
}

// TestRestoreRecreatesDeclarationExpiryAndAggregates: a bucket record taken from one ring
// declaration rebuilds a bucket that describes the same under another name, which is what
// a restore must do before it can put any entity back.
func TestRestoreRecreatesDeclarationExpiryAndAggregates(t *testing.T) {
	_, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "src", 1); err != nil {
		t.Fatalf("creating database src on the test host: %v", err)
	}
	if _, err := client.CreateBucketExpiring(ctx, "src", "Order", "Due", restoreOrderDeclaration()); err != nil {
		t.Fatalf("creating bucket src.Order on the test host: %v", err)
	}
	before, err := client.DescribeBucket(ctx, "src", "Order")
	if err != nil {
		t.Fatalf("describing src.Order: %v", err)
	}
	record := dumpBucketRecord(before)
	target := &ringRestoreRun{ctx: ctx, client: client}
	// The target name differs from the record's database, which is the --as path.
	if err := target.createDatabase(&dumpDB{Name: "src", RF: 1, Storage: "disk"}, "dst"); err != nil {
		t.Fatalf("creating database dst from the record: %v", err)
	}
	if err := target.createBucket(record, "dst"); err != nil {
		t.Fatalf("creating bucket dst.Order from the record: %v", err)
	}
	after, err := client.DescribeBucket(ctx, "dst", "Order")
	if err != nil {
		t.Fatalf("describing the restored bucket dst.Order: %v", err)
	}

	if got, want := restoreFieldShapes(after.Fields), restoreFieldShapes(before.Fields); !reflect.DeepEqual(got, want) {
		t.Errorf("the restored fields are %q, want %q", got, want)
	}
	if after.ExpireField != before.ExpireField {
		t.Errorf("the restored bucket expires at %q, want %q", after.ExpireField, before.ExpireField)
	}
	if !reflect.DeepEqual(after.ExpireStores, before.ExpireStores) {
		t.Errorf("the restored expiring stores are %+v, want %+v", after.ExpireStores, before.ExpireStores)
	}
	if got, want := restoreAggregateShapes(after.Aggregates), restoreAggregateShapes(before.Aggregates); !reflect.DeepEqual(got, want) {
		t.Errorf("the restored aggregates are %q, want %q", got, want)
	}

	lines := restoreBlobFieldLines(record, "dst")
	wantLine := fmt.Sprintf("Bucket %s field %s (blobStore): payloads are not in a dump, so this field restored empty.",
		terminalSafeText("dst.Order"), terminalSafeText("Raw"))
	if len(lines) != 1 || lines[0] != wantLine {
		t.Errorf("the blob lines of dst.Order are %q, want the one line %q", lines, wantLine)
	}
	if strings.HasSuffix(wantLine, "\n") {
		t.Fatalf("the blob line ends in a newline, and the restore adds the line break itself")
	}
}

// TestRestoreCreateDatabaseTwiceWithTheSameDeclarationIsANoOp: a create repeated with the
// record it came from succeeded already, so it must succeed again; the same name with
// another replication factor is the server's refusal and must reach the caller.
func TestRestoreCreateDatabaseTwiceWithTheSameDeclarationIsANoOp(t *testing.T) {
	_, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	target := &ringRestoreRun{ctx: ctx, client: client}
	record := &dumpDB{Name: "src", RF: 1, Storage: "disk"}
	if err := target.createDatabase(record, "dst"); err != nil {
		t.Fatalf("creating database dst the first time: %v", err)
	}
	if err := target.createDatabase(record, "dst"); err != nil {
		t.Fatalf("creating database dst again from the same record: %v", err)
	}
	otherRF := &dumpDB{Name: "src", RF: 2, Storage: "disk"}
	if err := target.createDatabase(otherRF, "dst"); err == nil {
		t.Fatalf("creating database dst with another replication factor succeeded, want a refusal")
	}
}
