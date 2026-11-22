package wire

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u41Node is a node with database app and three buckets whose declarations are cached.
func u41Node(t *testing.T) (*Node, uint64) {
	t.Helper()
	node, _ := u30iFresh(t)
	ctx := context.Background()
	for _, name := range []string{"Order", "Device"} {
		if _, err := node.CreateBucket(ctx, u30iDB, name, Field{Name: "Label", Type: "string"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"User", "Order", "Device"} {
		doc := u30iUser("a", "Ada", 36)
		if name != "User" {
			doc = bson.D{{Key: "_id_", Value: "a"}, {Key: "Label", Value: "x"}}
		}
		if _, err := node.Insert(ctx, u30iDB, name, doc); err != nil {
			t.Fatalf("insert into %s: %v", name, err)
		}
	}
	dbID, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	return node, dbID
}

// TestBumpTouchesNoCacheEntry: applying a bump invalidates nothing eagerly (conv 20 step 3).
// The cache keeps every entry, tagged with the version it was fetched at, and a lookup
// finds the tag stale and refetches that one entry on its own use.
func TestBumpTouchesNoCacheEntry(t *testing.T) {
	node, dbID := u41Node(t)
	ctx := context.Background()
	before := node.fetcher.Stats()
	if before.Entries < 3 {
		t.Fatalf("the cache holds %d entries, want at least the 3 buckets used", before.Entries)
	}
	if err := node.BumpSchema(ctx, dbID, 1, 2, cluster.BumpNote{}); err != nil {
		t.Fatal(err)
	}
	if got := node.topology.Published(dbID); got != 2 {
		t.Fatalf("published = %d after the bump returned, want 2", got)
	}
	after := node.fetcher.Stats()
	if after.Entries != before.Entries || after.Bytes != before.Bytes || after.Fetches != before.Fetches {
		t.Fatalf("the bump touched the cache: entries %d to %d, bytes %d to %d, fetches %d to %d", before.Entries, after.Entries, before.Bytes, after.Bytes, before.Fetches, after.Fetches)
	}
	if _, err := node.Get(ctx, u30iDB, "User", "a"); err != nil {
		t.Fatal(err)
	}
	if got := node.fetcher.Stats().Fetches; got != before.Fetches+1 {
		t.Fatalf("fetches = %d after one use, want %d: the stale entry is refetched lazily, once, on its own use", got, before.Fetches+1)
	}
}

// TestABumpPublishesOnceTheFSMHasApplied: the committed version and the published one move in
// the same Propose, and a bump built on a stale version changes neither.
func TestABumpPublishesOnceTheFSMHasApplied(t *testing.T) {
	node, dbID := u41Node(t)
	ctx := context.Background()
	if err := node.BumpSchema(ctx, dbID, 5, 6, cluster.BumpNote{}); err == nil {
		t.Fatal("a bump from a version that is not the committed one was accepted")
	} else {
		u30iCode(t, err, cluster.CodeSchemaNotCurrent)
	}
	if got, committed := node.topology.Published(dbID), node.topology.CommittedSchema(dbID); got != 1 || committed != 1 {
		t.Fatalf("a refused bump moved the versions: published %d, committed %d, want 1 and 1", got, committed)
	}
	if err := node.BumpSchema(ctx, dbID, 1, 2, cluster.BumpNote{}); err != nil {
		t.Fatal(err)
	}
	if got, committed := node.topology.Published(dbID), node.topology.CommittedSchema(dbID); got != 2 || committed != 2 {
		t.Fatalf("published %d, committed %d after the bump, want 2 and 2", got, committed)
	}
}
