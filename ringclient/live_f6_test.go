package ringclient_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// f6Feed is a live server with User(Name), Message(Author of User, Text) and Room(Members of User).
func f6Feed(t *testing.T) *ringclient.Client {
	t.Helper()
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{{Name: "Name", Type: "string"}, {Name: "Age", Type: "int"}}); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []struct{ id, name string }{{"ada", "Ada"}, {"bob", "Bob"}} {
		if _, err := client.Bucket("app", "User").Insert(ctx, doc.id, ringclient.Doc{"Name": doc.name, "Age": int32(36)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.CreateBucket(ctx, "app", "Message", []ringclient.FieldDecl{{Name: "Author", Type: "string", Of: "User"}, {Name: "Text", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Room", []ringclient.FieldDecl{{Name: "Name", Type: "string"}, {Name: "Members", Type: "hashStore", Of: "User"}}); err != nil {
		t.Fatal(err)
	}
	messages := client.Bucket("app", "Message")
	for _, doc := range []struct{ id, author string }{{"m1", "ada"}, {"m2", "bob"}, {"m3", "ada"}, {"m4", "ghost"}} {
		if _, err := messages.Insert(ctx, doc.id, ringclient.Doc{"Author": doc.author, "Text": "hi"}); err != nil {
			t.Fatal(err)
		}
	}
	rooms := client.Bucket("app", "Room")
	if _, err := rooms.Insert(ctx, "r1", ringclient.Doc{"Name": "general"}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{"ada", "bob", "ghost"} {
		if _, err := rooms.Put(ctx, "r1", "Members", member, ringclient.Doc{"since": int32(1)}); err != nil {
			t.Fatal(err)
		}
	}
	return client
}

// TestLiveAFeedIsOneCallPerPage: [GUARD a page of messages with their authors is a single round trip].
func TestLiveAFeedIsOneCallPerPage(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	page, err := client.Bucket("app", "Message").List(ctx, nil, ringclient.PageRequest{Expand: []ringclient.ExpandRequest{{Field: "Author", Fields: []string{"Name"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 4 || len(page.Expanded) != 4 {
		t.Fatalf("%d entries and %d expansions", len(page.Entries), len(page.Expanded))
	}
	missing := 0
	for i, entry := range page.Entries {
		doc := page.Expanded[i].Lookup("Author").Document()
		if !doc.Lookup("expanded").Boolean() {
			missing++
			if entry.ID != "m4" {
				t.Errorf("%s has no author expansion", entry.ID)
			}
			continue
		}
		want := map[string]string{"m1": "Ada", "m2": "Bob", "m3": "Ada"}[entry.ID]
		if got := doc.Lookup("doc", "Name").StringValue(); got != want {
			t.Errorf("%s author = %q, want %q", entry.ID, got, want)
		}
	}
	if missing != 1 || page.Served != "owner" {
		t.Fatalf("missing %d, served %q", missing, page.Served)
	}
}

// TestLiveExpandOnAStoreAndOnGet: [GUARD room members and one message expand by key].
func TestLiveExpandOnAStoreAndOnGet(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	members, err := client.Bucket("app", "Room").StoreListExpanded(ctx, "r1", "Members", 0, nil, &ringclient.ExpandRequest{Fields: []string{"Name"}}, false)
	if err != nil || len(members.Keys) != 3 || len(members.Expanded) != 3 {
		t.Fatalf("members = %+v, %v", members, err)
	}
	for i, key := range members.Keys {
		found := members.Expanded[i].Lookup("Members").Document().Lookup("expanded").Boolean()
		if found != (key != "ghost") {
			t.Errorf("%s expanded = %v", key, found)
		}
	}
	doc, expanded, err := client.Bucket("app", "Message").GetExpanded(ctx, "m2", nil, []ringclient.ExpandRequest{{Field: "Author", Fields: []string{"Name"}}}, true)
	if err != nil || doc.Lookup("Text").StringValue() != "hi" {
		t.Fatalf("GetExpanded = %v, %v", doc, err)
	}
	if got := expanded.Lookup("Author", "doc", "Name").StringValue(); got != "Bob" {
		t.Fatalf("author = %q", got)
	}
}

// TestExpandCapIsRefusedBeforeSending: [GUARD nine expanded fields never leave the client].
func TestExpandCapIsRefusedBeforeSending(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	nine := make([]ringclient.ExpandRequest, 9)
	for i := range nine {
		nine[i] = ringclient.ExpandRequest{Field: "Author"}
	}
	_, err := client.Bucket("app", "Message").List(ctx, nil, ringclient.PageRequest{Expand: nine})
	if err == nil || !strings.Contains(err.Error(), "at most 8") {
		t.Fatalf("error = %v", err)
	}
}

// TestLiveWhereDropsRowsFromThePage: [GUARD where removes rows and says how many].
func TestLiveWhereDropsRowsFromThePage(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	page, err := client.Bucket("app", "Message").List(ctx, nil, ringclient.PageRequest{Expand: []ringclient.ExpandRequest{{Field: "Author", Fields: []string{"Name"}, Where: bson.D{{Key: "Name", Value: "Ada"}}}}})
	if err != nil || len(page.Entries) != 2 || page.ExpandFiltered != 2 {
		t.Fatalf("page = %d entries, %d filtered, %v", len(page.Entries), page.ExpandFiltered, err)
	}
}

// TestLiveDescribeShowsTheReference: [GUARD of is part of the declaration a client reads back].
func TestLiveDescribeShowsTheReference(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	info, err := client.DescribeBucket(ctx, "app", "Room")
	if err != nil {
		t.Fatal(err)
	}
	var of string
	for _, field := range info.Fields {
		if field.Name == "Members" {
			of = field.Of
		}
	}
	if of != "User" {
		t.Fatalf("Members of %q", of)
	}
	if _, _, err := client.Bucket("app", "Message").GetExpanded(ctx, "nobody", nil, nil, false); !errors.Is(err, ringclient.ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
}

// TestLiveAlterWithOfIsRefusedNotDropped: [GUARD the client carries 'of' on alter so the node
// refuses it; before, the client dropped it and the alter answered OK without the reference].
func TestLiveAlterWithOfIsRefusedNotDropped(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	_, err := client.AlterBucket(ctx, "app", "Message", ringclient.AlterChange{Add: []ringclient.FieldDecl{{Name: "Editor", Type: "string", Of: "User"}}})
	if err == nil || !strings.Contains(err.Error(), "alter cannot declare a reference") {
		t.Fatalf("alter with of = %v, want the refusal", err)
	}
	info, err := client.DescribeBucket(ctx, "app", "Message")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range info.Fields {
		if field.Name == "Editor" {
			t.Fatalf("the refused alter added %+v", field)
		}
	}
}

// TestLiveLocalAloneOnAStoreListIsRefused: [GUARD local with no expand is sent as local alone and
// refused, not turned into an empty expansion of the declared bucket].
func TestLiveLocalAloneOnAStoreListIsRefused(t *testing.T) {
	client, ctx := f6Feed(t), liveContext(t)
	_, err := client.Bucket("app", "Room").StoreListExpanded(ctx, "r1", "Members", 0, nil, nil, true)
	if err == nil || !strings.Contains(err.Error(), "drop local") {
		t.Fatalf("local alone = %v, want the refusal", err)
	}
}
