package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// dumpDocsOf returns one bucket's doc records, in file order.
func dumpDocsOf(records []dumpRecord, database, bucket string) []*dumpDoc {
	var found []*dumpDoc
	for _, record := range records {
		if doc, isDoc := record.(*dumpDoc); isDoc && doc.DB == database && doc.B == bucket {
			found = append(found, doc)
		}
	}
	return found
}

// dumpDocNamed returns the one doc record of an entity, and fails the test when the file
// holds none or more than one for that id.
func dumpDocNamed(t *testing.T, records []dumpRecord, database, bucket, id string) *dumpDoc {
	t.Helper()
	var found []*dumpDoc
	for _, doc := range dumpDocsOf(records, database, bucket) {
		if doc.ID == id {
			found = append(found, doc)
		}
	}
	if len(found) != 1 {
		t.Fatalf("the dump holds %d doc records for %s.%s id %q, want exactly 1", len(found), database, bucket, id)
	}
	return found[0]
}

// TestDumpOfADocumentWithStoreFieldsHoldsNoMarkerStrings: a List body of an unprojected
// read carries each store field as the marker text "<type> (<n> elements)", and a marker
// string in the doc record would be restored as the field's value (review F-04, section
// 3.2 item 4). The doc record holds the scalars only.
//
// The second subtest settles plan item 7.4 item 15 on the live host: PageRequest.Fields as
// an empty non-nil slice means "all", not "none". ringclient drops an empty slice from the
// wire request (its `fields` member is omitempty, cluster/wire/ringlist.go:ringListDoc), so
// the node reads no projection and appends the store markers
// (cluster/wire/slotscan.go:scanPlan.body calls appendStoreSummaries only when the
// projection is nil). The dump therefore drops every declared store field's member by name,
// from the declaration, whatever the projection returned.
func TestDumpOfADocumentWithStoreFieldsHoldsNoMarkerStrings(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dumpTestDatabase(t, client, "app")
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Purchases", Type: "fifoStore"},
	}); err != nil {
		t.Fatalf("creating app.User on the test host: %v", err)
	}
	user := client.Bucket("app", "User")
	if _, err := user.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
		t.Fatalf("inserting ada into app.User: %v", err)
	}
	for _, product := range []string{"tea", "cake", "jam"} {
		if _, err := user.Push(ctx, "ada", "Purchases", ringclient.Doc{"Product": product}); err != nil {
			t.Fatalf("pushing %q into ada's Purchases: %v", product, err)
		}
	}
	// A bucket with no scalar field at all: its List passes an empty non-nil Fields, which
	// is the case item 15 asks about.
	if _, err := client.CreateBucket(ctx, "app", "Events", []ringclient.FieldDecl{
		{Name: "Payload", Type: "fifoStore"},
	}); err != nil {
		t.Fatalf("creating app.Events on the test host: %v", err)
	}
	events := client.Bucket("app", "Events")
	if _, err := events.Insert(ctx, "e1", ringclient.Doc{}); err != nil {
		t.Fatalf("inserting e1 into app.Events: %v", err)
	}
	if _, err := events.Push(ctx, "e1", "Payload", ringclient.Doc{"Kind": "click"}); err != nil {
		t.Fatalf("pushing into e1's Payload: %v", err)
	}
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping a bucket with a store field: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	records := readDumpRecords(t, outPath)

	t.Run("the declared scalars come back without the store marker", func(t *testing.T) {
		doc := dumpDocNamed(t, records, "app", "User", "ada")
		var members bson.D
		if err := bson.Unmarshal(doc.F, &members); err != nil {
			t.Fatalf("the doc record's f does not decode: %v", err)
		}
		if len(members) != 1 || members[0].Key != "Name" || members[0].Value != "Ada" {
			t.Errorf("the doc record's f decodes to %v, want exactly {Name: \"Ada\"}", members)
		}
		for _, member := range members {
			if member.Key == "Purchases" {
				t.Errorf("the doc record carries the store field %q as %v, and a store field is not part of a document", member.Key, member.Value)
			}
			if text, isText := member.Value.(string); isText && strings.Contains(text, "fifoStore") {
				t.Errorf("the doc record's member %q holds the store marker %q", member.Key, text)
			}
		}
	})

	t.Run("a bucket with no scalar field drops the marker too", func(t *testing.T) {
		doc := dumpDocNamed(t, records, "app", "Events", "e1")
		if len(doc.F) != 5 {
			elements, err := doc.F.Elements()
			if err != nil {
				t.Fatalf("the doc record's f is not a BSON document: %v", err)
			}
			t.Fatalf("the doc record's f is %d bytes and holds %v; a bucket with no scalar field has an empty document (an empty non-nil Fields means \"all\", so the declaration is what removes the marker)", len(doc.F), elements)
		}
		if elements, err := doc.F.Elements(); err != nil || len(elements) != 0 {
			t.Errorf("the doc record's f holds %v (error %v), want an empty document", elements, err)
		}
	})
}

// TestDumpCarriesIdsAsTheRingHoldsThem: the doc record's id is the id string the ring
// holds, never re-rendered: an ObjectId-shaped string, a prefixed-looking string and a
// plain name all come back byte for byte (section 3.1, the doc row).
func TestDumpCarriesIdsAsTheRingHoldsThem(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dumpTestDatabase(t, client, "app")
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
	}); err != nil {
		t.Fatalf("creating app.User on the test host: %v", err)
	}
	user := client.Bucket("app", "User")
	wantIDs := []string{"507f1f77bcf86cd799439011", "~i:1", "alice"}
	for _, id := range wantIDs {
		if _, err := user.Insert(ctx, id, ringclient.Doc{"Name": id}); err != nil {
			t.Fatalf("inserting id %q into app.User: %v", id, err)
		}
	}
	outPath := filepath.Join(t.TempDir(), "ids.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping three ids: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	records := readDumpRecords(t, outPath)
	if got := len(dumpDocsOf(records, "app", "User")); got != len(wantIDs) {
		t.Fatalf("the dump holds %d doc records for app.User, want %d", got, len(wantIDs))
	}
	gotIDs := make([]string, 0, len(wantIDs))
	for _, doc := range dumpDocsOf(records, "app", "User") {
		gotIDs = append(gotIDs, doc.ID)
		var members bson.D
		if err := bson.Unmarshal(doc.F, &members); err != nil {
			t.Fatalf("the doc record of id %q does not decode: %v", doc.ID, err)
		}
		if len(members) != 1 || members[0].Key != "Name" || members[0].Value != doc.ID {
			t.Errorf("the doc record of id %q holds %v, want {Name: %q}", doc.ID, members, doc.ID)
		}
	}
	want := append([]string{}, wantIDs...)
	sort.Strings(want)
	sort.Strings(gotIDs)
	for index := range want {
		if gotIDs[index] != want[index] {
			t.Fatalf("the doc records carry the ids %v, want %v", gotIDs, want)
		}
	}
	if _, err := verifyDump(outPath); err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
}

// TestEmptyDumpIsAnError: a run that writes no bucket at all is a refusal, not a file that
// looks like a backup of nothing (section 5, 3.2 item 1). It says how to list or create a
// bucket, and it leaves no file and no .partial.
func TestEmptyDumpIsAnError(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dumpTestDatabase(t, client, "app")
	// A database with no bucket at all: the dump's scope is in it, and there is nothing in
	// the scope to write.
	if _, err := client.CreateDatabase(ctx, "hollow", 1); err != nil {
		t.Fatalf("creating database hollow on the test host: %v", err)
	}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "hollow.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "hollow", "--out", outPath)
	if code != ExitRefused {
		t.Fatalf("dumping a database with no bucket: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitRefused, stdout, stderr)
	}
	want := `The ring holds no buckets in "hollow", so there is nothing to dump. Run 'show buckets in <database>' to list what exists, or 'create bucket <name> in <database> { Name string }' to create one.`
	if !strings.Contains(stderr, want) {
		t.Errorf("the refusal does not say %q:\nstderr: %s", want, stderr)
	}
	requireNoDumpFile(t, outPath)
}

// TestDumpOfABucketWithOnlyStoreFields: a bucket whose fields are all stores has no scalar
// to project, so every doc record's f is an empty document and the id is the only thing it
// carries. The stub half pages 250 documents through a 100-document List, so an off-by-one
// page, a repeated page or a dropped entry shows here (plan 7.4 item 15).
func TestDumpOfABucketWithOnlyStoreFields(t *testing.T) {
	t.Run("on the ring", func(t *testing.T) {
		addr, client := newRingTestHost(t)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		dumpTestDatabase(t, client, "app")
		if _, err := client.CreateBucket(ctx, "app", "Events", []ringclient.FieldDecl{
			{Name: "Payload", Type: "fifoStore"},
		}); err != nil {
			t.Fatalf("creating app.Events on the test host: %v", err)
		}
		events := client.Bucket("app", "Events")
		for _, id := range []string{"e1", "e2"} {
			if _, err := events.Insert(ctx, id, ringclient.Doc{}); err != nil {
				t.Fatalf("inserting %q into app.Events: %v", id, err)
			}
			if _, err := events.Push(ctx, id, "Payload", ringclient.Doc{"Kind": "click"}); err != nil {
				t.Fatalf("pushing into %q's Payload: %v", id, err)
			}
		}
		outPath := filepath.Join(t.TempDir(), "events.hoarddump")

		code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--out", outPath)
		if code != ExitOK {
			t.Fatalf("dumping a bucket of stores: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
		}
		records := readDumpRecords(t, outPath)
		docs := dumpDocsOf(records, "app", "Events")
		if len(docs) != 2 {
			t.Fatalf("the dump holds %d doc records for app.Events, want 2", len(docs))
		}
		gotIDs := make([]string, 0, len(docs))
		for _, doc := range docs {
			gotIDs = append(gotIDs, doc.ID)
			if len(doc.F) != 5 {
				t.Errorf("the doc record of id %q holds f of %d bytes, want an empty document (5 bytes)", doc.ID, len(doc.F))
			}
		}
		sort.Strings(gotIDs)
		wantIDs := []string{"e1", "e2"}
		sort.Strings(wantIDs)
		for index := range wantIDs {
			if gotIDs[index] != wantIDs[index] {
				t.Fatalf("the doc records carry the ids %v, want %v", gotIDs, wantIDs)
			}
		}
	})

	t.Run("paging a stub of 250 documents", func(t *testing.T) {
		const documentCount = 250
		entries := make([]ringclient.Entry, documentCount)
		wantIDs := make([]string, documentCount)
		for index := range entries {
			id := fmt.Sprintf("d%03d", index)
			wantIDs[index] = id
			body, err := bson.Marshal(bson.D{
				{Key: dumpBucketIDField, Value: id},
				{Key: "Payload", Value: "fifoStore (1 element)"},
			})
			if err != nil {
				t.Fatalf("building the body of %q: %v", id, err)
			}
			entries[index] = ringclient.Entry{ID: id, Body: bson.Raw(body)}
		}
		stub := &dumpStub{
			databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
			bucketPages: map[string][]ringclient.BucketPage{
				"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Events", State: "active"}}, 500),
			},
			descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
			declarations: map[string]ringclient.BucketInfo{
				"app/Events": {Database: "app", Name: "Events", State: "active", Fields: []ringclient.FieldInfo{
					{Name: dumpBucketIDField, Type: "string"},
					{Name: "Payload", Type: "fifoStore"},
				}},
			},
			documentPages: map[string][]ringclient.EntryPage{
				"app/Events": dumpStubDocumentPages(entries, ringDumpListPageSize),
			},
		}
		outPath := filepath.Join(t.TempDir(), "many.hoarddump")

		code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
		if code != ExitOK {
			t.Fatalf("dumping 250 documents: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
		}
		docs := dumpDocsOf(readDumpRecords(t, outPath), "app", "Events")
		if len(docs) != documentCount {
			t.Fatalf("the dump holds %d doc records, want %d (every document exactly once)", len(docs), documentCount)
		}
		for index, doc := range docs {
			if doc.ID != wantIDs[index] {
				t.Fatalf("doc record %d carries id %q, want %q: the records must follow the pages in order", index+1, doc.ID, wantIDs[index])
			}
			if len(doc.F) != 5 {
				t.Errorf("the doc record of id %q holds %d bytes of f, want an empty document", doc.ID, len(doc.F))
			}
		}
		wantCalls := (documentCount + ringDumpListPageSize - 1) / ringDumpListPageSize
		if stub.calls.ListDocuments != wantCalls {
			t.Errorf("ListDocuments was called %d times for %d documents served %d per page, want %d", stub.calls.ListDocuments, documentCount, ringDumpListPageSize, wantCalls)
		}
		if stub.calls.largestDocumentPage > ringDumpListPageSize {
			t.Errorf("one ListDocuments answer held %d documents, and the dump asks for %d per page", stub.calls.largestDocumentPage, ringDumpListPageSize)
		}
		for requestIndex, fields := range stub.calls.documentFields {
			if fields == nil {
				t.Errorf("ListDocuments request %d carried a nil Fields, and a bucket of stores must send an empty non-nil one", requestIndex+1)
			}
			if len(fields) != 0 {
				t.Errorf("ListDocuments request %d carried the fields %v, and the bucket declares no scalar field", requestIndex+1, fields)
			}
		}
		for requestIndex, limit := range stub.calls.documentLimits {
			if limit != ringDumpListPageSize {
				t.Errorf("ListDocuments request %d asked for %d documents, want %d", requestIndex+1, limit, ringDumpListPageSize)
			}
		}
	})
}
