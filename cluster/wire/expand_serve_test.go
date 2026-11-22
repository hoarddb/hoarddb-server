package wire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// expandServe adds Room and Message buckets to the expand world.
func expandServe(t *testing.T) *expandWorld {
	t.Helper()
	world := newExpandWorld(t, u70AllowAll{})
	node, ctx := world.server.node, context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Room", Field{Name: "Name", Type: "string"}, Field{Name: "Members", Type: "hashStore"}, Field{Name: "Log", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Message", Field{Name: "Author", Type: "string"}, Field{Name: "Text", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Room", bson.D{{Key: "_id_", Value: "r1"}, {Key: "Name", Value: "general"}}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{"u1", "u3", "ghost"} {
		if err := node.Put(ctx, PutRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "Room", ID: "r1", Field: "Members"}, Key: member, Element: bson.D{{Key: "since", Value: int32(1)}}}); err != nil {
			t.Fatal(err)
		}
	}
	authors := []string{"u2", "u0", "u2", "u5", "u6", "u1"}
	for i, author := range authors {
		if _, err := node.Insert(ctx, u30iDB, "Message", bson.D{{Key: "_id_", Value: fmt.Sprintf("m%d", i)}, {Key: "Author", Value: author}, {Key: "Text", Value: strings.Repeat("x", 100)}}); err != nil {
			t.Fatal(err)
		}
	}
	return world
}

func (w *expandWorld) bucket(t *testing.T, name string) bucketRef {
	t.Helper()
	ref, err := w.server.node.bucket(context.Background(), u30iDB, name)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func expandedDoc(t *testing.T, raw []byte, label string) (bool, string) {
	t.Helper()
	element := bson.Raw(raw).Lookup(label).Document()
	if !element.Lookup("expanded").Boolean() {
		return false, ""
	}
	return true, element.Lookup("doc", "Name").StringValue()
}

func entryBodies(t *testing.T, reply RingReply) map[string]string {
	t.Helper()
	entries, err := cluster.DecodeEntries(reply.Entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(reply.Expanded) {
		t.Fatalf("%d entries and %d expansions", len(entries), len(reply.Expanded))
	}
	out := map[string]string{}
	for i, entry := range entries {
		_, name := expandedDoc(t, reply.Expanded[i], "Author")
		out[idText(bson.Raw(entry.Body), "_id_")] = name
	}
	return out
}

// TestExpandOnAStoreReturnsFarFields: [GUARD the far read is keyed by the element key].
func TestExpandOnAStoreReturnsFarFields(t *testing.T) {
	world := expandServe(t)
	room := world.bucket(t, "Room")
	encID, _ := store.EncodeComponent("r1")
	fieldID, _ := room.storeField("Members")
	reply, err := world.server.read(context.Background(), room, RingRead{Class: RingClassStoreList, EncID: encID, FieldID: fieldID, Limit: 10,
		Options: RingReadOptions{Expand: []RingExpandSpec{{Bucket: "User", Fields: []string{"Name"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Keys) != 3 || len(reply.Expanded) != 3 || len(reply.Elements) != 3 {
		t.Fatalf("reply = %+v", reply)
	}
	want := map[string]string{"u1": "Name1", "u3": "Name3", "ghost": ""}
	for i, key := range reply.Keys {
		found, name := expandedDoc(t, reply.Expanded[i], "Members")
		if name != want[key] || found != (key != "ghost") {
			t.Errorf("%s expanded (%v, %q), want %q", key, found, name, want[key])
		}
	}
}

// TestExpandOnAFifoStoreIsRefused: [GUARD a fifo has no element keys].
func TestExpandOnAFifoStoreIsRefused(t *testing.T) {
	world := expandServe(t)
	room := world.bucket(t, "Room")
	encID, _ := store.EncodeComponent("r1")
	fieldID, _ := room.storeField("Log")
	_, err := world.server.read(context.Background(), room, RingRead{Class: RingClassStoreList, EncID: encID, FieldID: fieldID, Limit: 10,
		Options: RingReadOptions{Expand: []RingExpandSpec{{Bucket: "User"}}}})
	if err == nil || !strings.Contains(err.Error(), "element keys") {
		t.Fatalf("error = %v", err)
	}
	if len(world.reads) != 0 {
		t.Fatal("a refused expand read documents")
	}
}

// TestExpandOnABucketListAndOnGet: [GUARD each row shows its own author].
func TestExpandOnABucketListAndOnGet(t *testing.T) {
	world := expandServe(t)
	reply, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message",
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{"Name"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	wantAuthor := map[string]string{"m0": "Name2", "m1": "Name0", "m2": "Name2", "m3": "Name5", "m4": "Name6", "m5": "Name1"}
	got := entryBodies(t, reply)
	for id, name := range wantAuthor {
		if got[id] != name {
			t.Errorf("%s author = %q, want %q", id, got[id], name)
		}
	}
	if reads := world.server.node.ExpandStats().Reads.Load(); reads != 5 {
		t.Errorf("%d far reads for 5 distinct authors", reads)
	}
	message := world.bucket(t, "Message")
	encID, _ := store.EncodeComponent("m3")
	one, err := world.server.read(context.Background(), message, RingRead{Class: RingClassGetFields, EncID: encID, Fields: []string{"Author"},
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{"Name"}}}}})
	if err != nil || len(one.Expanded) != 1 {
		t.Fatalf("get = %+v, %v", one, err)
	}
	if _, name := expandedDoc(t, one.Expanded[0], "Author"); name != "Name5" {
		t.Fatalf("m3's author = %q", name)
	}
}

// TestExpandRespectsTheByteCeilingAtARowBoundary: [GUARD a cut page resumes at the next row].
func TestExpandRespectsTheByteCeilingAtARowBoundary(t *testing.T) {
	world := expandServe(t)
	world.server.expandBytes = 700
	seen := map[string]bool{}
	var cursor []byte
	pages := 0
	for {
		reply, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message", Cursor: cursor,
			Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "User", Fields: []string{"Name"}}}}})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for id, name := range entryBodies(t, reply) {
			if seen[id] {
				t.Fatalf("%s returned twice", id)
			}
			if name == "" {
				t.Fatalf("%s came back with its expansion dropped", id)
			}
			seen[id] = true
		}
		if !reply.More {
			break
		}
		cursor = reply.Cursor
		if pages > 10 {
			t.Fatal("paging does not end")
		}
	}
	if len(seen) != 6 || pages < 2 {
		t.Fatalf("%d rows over %d pages, want 6 over at least 2", len(seen), pages)
	}
}

// TestExpandCrossDatabase: [GUARD the far bucket is looked up in the database the spec names].
func TestExpandCrossDatabase(t *testing.T) {
	world := expandServe(t)
	node, ctx := world.server.node, context.Background()
	if _, err := node.CreateDatabase(ctx, "ref"); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, "ref", "Person", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, "ref", "Person", bson.D{{Key: "_id_", Value: "u2"}, {Key: "Name", Value: "Far Ada"}}); err != nil {
		t.Fatal(err)
	}
	reply, err := world.server.serveList(ctx, RingListRequest{Database: u30iDB, Bucket: "Message",
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Database: "ref", Bucket: "Person"}}}})
	if err != nil {
		t.Fatal(err)
	}
	got := entryBodies(t, reply)
	if got["m0"] != "Far Ada" || got["m1"] != "" {
		t.Fatalf("expansions = %v", got)
	}
}

// TestExpandOfANamedBucketThatDoesNotExistIsRefusedNotFound: [GUARD a missing far bucket is refused by name, never read as no author].
func TestExpandOfANamedBucketThatDoesNotExistIsRefusedNotFound(t *testing.T) {
	world := expandServe(t)
	_, err := world.server.serveList(context.Background(), RingListRequest{Database: u30iDB, Bucket: "Message",
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Bucket: "Nobody"}}}})
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeNotFound || !strings.Contains(err.Error(), "Nobody") {
		t.Fatalf("error = %v, want NOT_FOUND naming the bucket", err)
	}
}
