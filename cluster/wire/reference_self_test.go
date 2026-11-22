package wire

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// TestASelfReferenceIsCreatedWrittenAndExpanded: [GUARD a bucket may be declared of itself: a
// manager, a parent comment, a friend of a user. The create resolves the target to the id it
// claims, describe names it, a write checks the id type, and expand reads the far document].
func TestASelfReferenceIsCreatedWrittenAndExpanded(t *testing.T) {
	world, ctx := referenceWorld(t)
	node := world.server.node
	if _, err := node.CreateBucket(ctx, u30iDB, "Employee", Field{Name: "Name", Type: "string"}, Field{Name: "Manager", Type: "string", Of: "Employee"}, Field{Name: "Reports", Type: "hashStore", Of: "Employee"}); err != nil {
		t.Fatalf("a self reference was refused: %v", err)
	}
	reply, err := world.server.describe(ctx, AdminDescribeBucket, u30iDB, "Employee")
	if err != nil {
		t.Fatal(err)
	}
	of := map[string]string{}
	for _, field := range reply.Fields {
		of[field.Name] = field.Of
	}
	if of["Manager"] != "Employee" || of["Reports"] != "Employee" {
		t.Fatalf("describe of = %v, want Employee for both", of)
	}
	employee := world.bucket(t, "Employee")
	if bucketID, ok := employee.entry.Decl.ReferenceOf(employee.table.Entries[2].ID); !ok || bucketID != employee.entry.Decl.BucketID {
		t.Fatalf("Manager refers to %d, %v, want the bucket's own id %d", bucketID, ok, employee.entry.Decl.BucketID)
	}
	// A write checks the id type.
	manager, _ := entryNamed(employee.table, "Manager")
	encID, _ := store.EncodeComponent("e2")
	number := append([]byte{byte(bson.TypeInt32)}, 1, 0, 0, 0)
	err = ringCheckDocument(employee, cluster.Op{Class: cluster.OpInsert, EncID: encID, IDValue: stringValue("e2"), Fields: []cluster.FieldOp{{FieldID: manager.ID, Value: number}}}, world.server.referenceNamer(context.Background(), employee))
	if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, "Manager") {
		t.Fatalf("the id-type refusal does not name Manager: %s", text)
	}
	// Expand reads the manager's own document.
	for _, doc := range []bson.D{
		{{Key: "_id_", Value: "e1"}, {Key: "Name", Value: "Ada"}},
		{{Key: "_id_", Value: "e2"}, {Key: "Name", Value: "Bob"}, {Key: "Manager", Value: "e1"}},
	} {
		if _, err := node.Insert(ctx, u30iDB, "Employee", doc); err != nil {
			t.Fatal(err)
		}
	}
	page, err := world.server.serveList(ctx, RingListRequest{Database: u30iDB, Bucket: "Employee", Fields: []string{"Name", "Manager"},
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Manager", Fields: []string{"Name"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := cluster.DecodeEntries(page.Entries)
	if err != nil {
		t.Fatal(err)
	}
	for i, entry := range entries {
		if idText(bson.Raw(entry.Body), "_id_") != "e2" {
			continue
		}
		if found, name := expandedDoc(t, page.Expanded[i], "Manager"); !found || name != "Ada" {
			t.Fatalf("Bob's manager expanded to (%v, %q), want Ada", found, name)
		}
		return
	}
	t.Fatal("e2 was not listed")
}

// TestCreatingASelfReferenceAgainIsIdempotent: [GUARD the retry of a self-referencing create
// answers with the same id, not BUCKET_EXISTS].
func TestCreatingASelfReferenceAgainIsIdempotent(t *testing.T) {
	world, ctx := referenceWorld(t)
	node := world.server.node
	first, err := node.CreateBucket(ctx, u30iDB, "Comment", Field{Name: "Parent", Type: "string", Of: "Comment"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := node.CreateBucket(ctx, u30iDB, "Comment", Field{Name: "Parent", Type: "string", Of: "Comment"})
	if err != nil || second != first {
		t.Fatalf("the repeated create = %d, %v, want %d", second, err, first)
	}
}
