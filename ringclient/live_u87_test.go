package ringclient_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u87Users is a live server with User(Name, Bio, Age, Purchases) and the documents ada and bob.
func u87Users(t *testing.T) *ringclient.Bucket {
	t.Helper()
	client, ctx := liveClient(t), liveContext(t)
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Bio", Type: "string"}, {Name: "Age", Type: "int"}, {Name: "Purchases", Type: "fifoStore"},
	}); err != nil {
		t.Fatal(err)
	}
	users := client.Bucket("app", "User")
	for _, doc := range []struct{ id, name string }{{"ada", "Ada"}, {"bob", "Bob"}} {
		if _, err := users.Insert(ctx, doc.id, ringclient.Doc{"Name": doc.name, "Bio": "bio of " + doc.name, "Age": int32(36)}); err != nil {
			t.Fatal(err)
		}
	}
	return users
}

func u87Keys(raw bson.Raw) string {
	elements, err := raw.Elements()
	if err != nil {
		return err.Error()
	}
	var keys []string
	for _, element := range elements {
		keys = append(keys, element.Key())
	}
	return strings.Join(keys, ",")
}

// TestLiveGetFieldsReturnsOnlyWhatWasAsked: db.User["ada"].get({fields: ["Name"]}).
func TestLiveGetFieldsReturnsOnlyWhatWasAsked(t *testing.T) {
	users, ctx := u87Users(t), liveContext(t)
	got, err := users.GetFields(ctx, "ada", []string{"Age", "Name"})
	if err != nil || u87Keys(got) != "_id_,Name,Age" {
		t.Fatalf("GetFields = %v, %v, want _id_, Name, Age", got, err)
	}
	if got.Lookup("Name").StringValue() != "Ada" {
		t.Fatalf("the Name is %v", got.Lookup("Name"))
	}
	whole, err := users.Get(ctx, "ada")
	if err != nil || u87Keys(whole) != "_id_,Name,Bio,Age,Purchases" && u87Keys(whole) != "_id_,Name,Bio,Age" {
		t.Fatalf("Get = %v, %v: an unprojected get must still return every field", whole, err)
	}
	if _, err := users.GetFields(ctx, "nobody", []string{"Name"}); !errors.Is(err, ringclient.ErrNotFound) {
		t.Fatalf("a projected get of an absent document must be ErrNotFound, got %v", err)
	}
}

// TestLiveProjectionRefusalsCarryTheFixAndTheCode: an undeclared name and a store name.
func TestLiveProjectionRefusalsCarryTheFixAndTheCode(t *testing.T) {
	users, ctx := u87Users(t), liveContext(t)
	for name, tc := range map[string]struct {
		fields []string
		said   []string
	}{
		"undeclared": {[]string{"Nmae"}, []string{`"Nmae"`, "describe bucket User in app"}},
		"store":      {[]string{"Name", "Purchases"}, []string{`"Purchases"`, `db.User["<id>"].Purchases.list()`}},
	} {
		_, err := users.GetFields(ctx, "ada", tc.fields)
		var refusal *ringclient.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeFieldType {
			t.Fatalf("%s: GetFields error %v, want FIELD_TYPE", name, err)
		}
		_, listErr := users.List(ctx, nil, ringclient.PageRequest{Fields: tc.fields})
		if !errors.As(listErr, &refusal) || refusal.Code != cluster.CodeFieldType {
			t.Fatalf("%s: List error %v, want FIELD_TYPE", name, listErr)
		}
		for _, word := range tc.said {
			if !strings.Contains(refusal.Message, word) {
				t.Errorf("%s: refusal %q lacks %q", name, refusal.Message, word)
			}
		}
	}
	for name, fields := range map[string][]string{"empty": {}, "duplicate": {"Name", "Name"}} {
		if _, err := users.GetFields(ctx, "ada", fields); err == nil {
			t.Errorf("%s fields were sent", name)
		}
		if _, err := users.List(ctx, nil, ringclient.PageRequest{Fields: fields}); err == nil {
			t.Errorf("%s list fields were sent", name)
		}
	}
}

// TestLiveListFieldsProjectsEveryEntry: db.User.list({fields: ["Name"], limit: 1}) and its next page.
func TestLiveListFieldsProjectsEveryEntry(t *testing.T) {
	users, ctx := u87Users(t), liveContext(t)
	first, err := users.List(ctx, nil, ringclient.PageRequest{Limit: 1, Fields: []string{"Name"}})
	if err != nil || len(first.Entries) != 1 || !first.More {
		t.Fatalf("first page %+v, %v", first, err)
	}
	second, err := users.List(ctx, nil, ringclient.PageRequest{Limit: 1, Fields: []string{"Name"}, Cursor: first.Cursor})
	if err != nil || len(second.Entries) != 1 {
		t.Fatalf("second page %+v, %v", second, err)
	}
	for _, entry := range append(first.Entries, second.Entries...) {
		if u87Keys(entry.Body) != "_id_,Name" {
			t.Errorf("entry %s has %s, want _id_,Name", entry.ID, u87Keys(entry.Body))
		}
	}
	filtered, err := users.List(ctx, bson.D{{Key: "Bio", Value: "bio of Bob"}}, ringclient.PageRequest{Fields: []string{"Name"}})
	if err != nil || len(filtered.Entries) != 1 || filtered.Entries[0].ID != "bob" || u87Keys(filtered.Entries[0].Body) != "_id_,Name" {
		t.Fatalf("filter on Bio, returning Name: %+v, %v", filtered, err)
	}
}
