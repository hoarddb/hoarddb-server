package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// dumpBucketsOf returns the bucket records of one database, in file order.
func dumpBucketsOf(records []dumpRecord, database string) []*dumpBucket {
	var found []*dumpBucket
	for _, record := range records {
		if bucket, isBucket := record.(*dumpBucket); isBucket && bucket.DB == database {
			found = append(found, bucket)
		}
	}
	return found
}

// dumpBucketNamed returns the one bucket record of database, and fails the test when the
// file holds none or more than one.
func dumpBucketNamed(t *testing.T, records []dumpRecord, database, name string) *dumpBucket {
	t.Helper()
	var found []*dumpBucket
	for _, bucket := range dumpBucketsOf(records, database) {
		if bucket.Name == name {
			found = append(found, bucket)
		}
	}
	if len(found) != 1 {
		t.Fatalf("the dump holds %d bucket records for %s.%s, want exactly 1", len(found), database, name)
	}
	return found[0]
}

// TestDumpWritesDatabaseAndBucketRecordsInOrder: the file holds the header, then each
// database's db record followed by the bucket records of that database, and each bucket
// record carries what a restore needs: the declared fields in declared order without
// _id_, the type exactly as the ring reports it, the expire_field of a bucket that
// expires, a queue field's expire_path, and the blob field under unsupported
// (sections 3.1 and 3.2 items 2 and 3).
func TestDumpWritesDatabaseAndBucketRecordsInOrder(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, name := range []string{"app", "audit"} {
		dumpTestDatabase(t, client, name)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Age", Type: "int"},
		{Name: "Purchases", Type: "fifoStore"},
		{Name: "Friends", Type: "hashStore"},
		{Name: "Data", Type: "blobStore"},
	}); err != nil {
		t.Fatalf("creating app.User on the test host: %v", err)
	}
	if _, err := client.CreateBucketExpiring(ctx, "app", "Session", "SeenAt", []ringclient.FieldDecl{
		{Name: "SeenAt", Type: "datetime"},
		{Name: "Notes", Type: "lifoStore", ExpirePath: "At"},
	}); err != nil {
		t.Fatalf("creating the expiring bucket app.Session on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "audit", "Event", []ringclient.FieldDecl{
		{Name: "What", Type: "string"},
	}); err != nil {
		t.Fatalf("creating audit.Event on the test host: %v", err)
	}
	outPath := filepath.Join(t.TempDir(), "two.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping two databases: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if stdout != "" {
		t.Errorf("a text run wrote to stdout: %q (progress and the report go to stderr)", stdout)
	}
	records := readDumpRecords(t, outPath)
	if _, isHeader := records[0].(*dumpHeader); !isHeader {
		t.Fatalf("the first record of %s is a %T, and the first record must be the header", outPath, records[0])
	}

	// A db record opens a database's section, and every bucket record until the next db
	// record belongs to it. A bucket before any db record, or under another database's
	// section, fails here.
	var databases []*dumpDB
	current := ""
	for index, record := range records {
		switch typed := record.(type) {
		case *dumpDB:
			current = typed.Name
			databases = append(databases, typed)
		case *dumpBucket:
			if typed.DB != current {
				t.Fatalf("record %d is bucket %s.%s and it does not follow a db record for %q", index+1, typed.DB, typed.Name, typed.DB)
			}
		case *dumpHeader, *dumpSummaryRecord, *dumpFooter:
			current = ""
		}
	}

	// ShowDatabases lists by name, so the file holds app before audit.
	names := make([]string, len(databases))
	for index, database := range databases {
		names[index] = database.Name
	}
	if want := []string{"app", "audit"}; !reflect.DeepEqual(names, want) {
		t.Errorf("the file's databases are %v, want %v (in the order the server lists them)", names, want)
	}
	for _, database := range databases {
		described, err := client.DescribeDatabase(ctx, database.Name, ringclient.PageRequest{Limit: 1})
		if err != nil {
			t.Fatalf("describing %s on the test host: %v", database.Name, err)
		}
		if database.RF != described.RF || database.Storage != described.Storage {
			t.Errorf("db record %s says rf %d storage %q, and the server says rf %d storage %q",
				database.Name, database.RF, database.Storage, described.RF, described.Storage)
		}
	}

	user := dumpBucketNamed(t, records, "app", "User")
	wantUserFields := []dumpField{
		{Name: "Name", Type: "string"},
		{Name: "Age", Type: "int"},
		{Name: "Purchases", Type: "fifoStore"},
		{Name: "Friends", Type: "hashStore"},
		{Name: "Data", Type: "blobStore"},
	}
	if !reflect.DeepEqual(user.Fields, wantUserFields) {
		t.Errorf("app.User's fields are %+v, want the declared order without _id_: %+v", user.Fields, wantUserFields)
	}
	if want := []string{"Data"}; !reflect.DeepEqual(user.Unsupported, want) {
		t.Errorf("app.User's unsupported is %v, want %v (every blobStore field)", user.Unsupported, want)
	}
	if user.ExpireField != "" {
		t.Errorf("app.User's expire_field is %q, and the bucket declares no expire field", user.ExpireField)
	}
	if user.Aggregates == nil {
		t.Error("app.User's aggregates is nil, and a record's required array is empty, never nil")
	}

	session := dumpBucketNamed(t, records, "app", "Session")
	wantSessionFields := []dumpField{
		{Name: "SeenAt", Type: "datetime"},
		{Name: "Notes", Type: "lifoStore", ExpirePath: "At"},
	}
	if !reflect.DeepEqual(session.Fields, wantSessionFields) {
		t.Errorf("app.Session's fields are %+v, want %+v (the queue field's expire_path comes from the bucket's declaration)", session.Fields, wantSessionFields)
	}
	if session.ExpireField != "SeenAt" {
		t.Errorf("app.Session's expire_field is %q, want %q", session.ExpireField, "SeenAt")
	}

	event := dumpBucketNamed(t, records, "audit", "Event")
	if want := []dumpField{{Name: "What", Type: "string"}}; !reflect.DeepEqual(event.Fields, want) {
		t.Errorf("audit.Event's fields are %+v, want %+v", event.Fields, want)
	}
	if event.Unsupported == nil || len(event.Unsupported) != 0 {
		t.Errorf("audit.Event's unsupported is %v, want an empty non-nil array", event.Unsupported)
	}
	if event.Aggregates == nil || len(event.Aggregates) != 0 {
		t.Errorf("audit.Event's aggregates is %v, want an empty non-nil array", event.Aggregates)
	}

	for _, record := range records {
		bucket, isBucket := record.(*dumpBucket)
		if !isBucket {
			continue
		}
		for _, field := range bucket.Fields {
			if field.Name == "_id_" {
				t.Errorf("bucket %s.%s holds _id_ among its fields; the doc record's id carries it", bucket.DB, bucket.Name)
			}
		}
	}

	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Databases != 2 || verification.Buckets != 5 {
		t.Errorf("the verification reads %d databases and %d buckets, want 2 and 5 (the count includes the bucket dumpTestDatabase seeds in each database)", verification.Databases, verification.Buckets)
	}
	if verification.Entities != 0 || verification.Elements != 0 {
		t.Errorf("the verification reads %d entities and %d elements, and this step writes none", verification.Entities, verification.Elements)
	}
}

// TestDumpDoesNotListTheSystemBucket (plan 7.4 item 10): the dump of a database with one
// bucket holds exactly that bucket. The test also reads the server's own listing, so a
// page that started carrying the system bucket or a reverse-index row is named here.
func TestDumpDoesNotListTheSystemBucket(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// The database is created without dumpTestDatabase's seed bucket: this test pins
	// exactly which buckets a page and a dump hold.
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatalf("creating database app on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
	}); err != nil {
		t.Fatalf("creating app.User on the test host: %v", err)
	}

	var listed []string
	var cursor []byte
	for {
		page, err := client.ShowBuckets(ctx, "app", ringclient.PageRequest{Cursor: cursor})
		if err != nil {
			t.Fatalf("listing the buckets of app on the test host: %v", err)
		}
		for _, bucket := range page.Buckets {
			listed = append(listed, bucket.Name)
		}
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	t.Logf("ShowBuckets on the live host lists %v", listed)

	outPath := filepath.Join(t.TempDir(), "one.hoarddump")
	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping app: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	var dumped []string
	for _, bucket := range dumpBucketsOf(readDumpRecords(t, outPath), "app") {
		dumped = append(dumped, bucket.Name)
	}
	if want := []string{"User"}; !reflect.DeepEqual(dumped, want) {
		t.Errorf("the dump holds the buckets %v, want exactly %v", dumped, want)
	}
	if want := []string{"User"}; !reflect.DeepEqual(listed, want) {
		t.Errorf("the server's ShowBuckets lists %v, want exactly %v (a system bucket or reverse-index row in a page would need a skip rule)", listed, want)
	}
}

// TestDumpSkipsADroppingBucketWithAWarning (section 3.2 item 2): a bucket that is leaving
// is skipped, the run says so on stderr, and the file holds only the active bucket.
func TestDumpSkipsADroppingBucketWithAWarning(t *testing.T) {
	stub := &dumpStub{
		// The page holds a database --db does not name, so a dump that described every
		// listed database would ask the stub for a description it does not have and fail.
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}, {Name: "audit", RF: 1}}, 500),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{
				{Name: "User", State: "active"},
				{Name: "old", State: "dropping"},
			}, 500),
		},
		descriptions: map[string]ringclient.DatabaseDescription{
			"app": {Name: "app", RF: 1, Storage: "disk"},
		},
		declarations: map[string]ringclient.BucketInfo{
			"app/User": {Database: "app", Name: "User", State: "active", Fields: []ringclient.FieldInfo{{Name: "_id_", Type: "string"}}},
		},
	}
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping a database with a dropping bucket: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	want := "Bucket app.old is being dropped, so it is not in this dump."
	if !strings.Contains(stderr, want) {
		t.Errorf("the run does not say %q:\nstderr: %s", want, stderr)
	}
	if stub.calls.DescribeBucket != 1 {
		t.Errorf("DescribeBucket was called %d times, want 1 (a dropping bucket is skipped before its declaration is read)", stub.calls.DescribeBucket)
	}
	if want := []string{"app"}; !reflect.DeepEqual(stub.calls.describedDatabases, want) {
		t.Errorf("DescribeDatabase was called for %v, want %v (--db names the only database to describe)", stub.calls.describedDatabases, want)
	}
	records := readDumpRecords(t, outPath)
	if len(records) != 5 {
		t.Fatalf("the file holds %d records, want header, db, the one active bucket, summary and footer", len(records))
	}
	if _, isHeader := records[0].(*dumpHeader); !isHeader {
		t.Errorf("the first record is a %T, want the header", records[0])
	}
	if database, isDB := records[1].(*dumpDB); !isDB || database.Name != "app" {
		t.Errorf("the second record is %T %+v, want the db record of app", records[1], records[1])
	}
	bucket, isBucket := records[2].(*dumpBucket)
	if !isBucket || bucket.Name != "User" {
		t.Fatalf("the third record is %T, want the one bucket record of User", records[2])
	}
	if _, isSummary := records[3].(*dumpSummaryRecord); !isSummary {
		t.Errorf("the fourth record is a %T, want the summary", records[3])
	}
	if _, isFooter := records[4].(*dumpFooter); !isFooter {
		t.Errorf("the fifth record is a %T, want the footer", records[4])
	}
}

// TestDumpOfAMissingBucketNamesTheListing: --bucket names one bucket and describes it
// without scanning the database, so a bucket the ring does not hold is refused with the
// statement that lists what exists (section 3.2 item 2, AGENTS.md rule 3).
func TestDumpOfAMissingBucketNamesTheListing(t *testing.T) {
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
		descriptions:  map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
	}
	outPath := filepath.Join(t.TempDir(), "nope.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--bucket", "nope", "--out", outPath)
	if code != ExitRefused {
		t.Fatalf("dumping a missing bucket: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitRefused, stdout, stderr)
	}
	want := `dump: Bucket "nope" not found in database "app". Run 'show buckets in app' to list what exists.`
	if !strings.Contains(stderr, want) {
		t.Errorf("the refusal does not say %q:\nstderr: %s", want, stderr)
	}
	if stub.calls.ShowBuckets != 0 {
		t.Errorf("ShowBuckets was called %d times for --bucket, and a named bucket is described without scanning the database", stub.calls.ShowBuckets)
	}
	requireNoDumpFile(t, outPath)
}

// TestDumpAbortsTheFileWhenTheRingRefusesMidRun: a refusal after the writer opened aborts
// the file, so no .partial and no final file stay behind, and the run exits with the
// ring's class and the sentence that says no file was written.
func TestDumpAbortsTheFileWhenTheRingRefusesMidRun(t *testing.T) {
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
		descriptions:  map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		failures: map[string]error{"DescribeDatabase app": &ringclient.Error{
			Code: cluster.CodePermissionDenied, Message: `database "app" is not one this user may describe`}},
	}
	outPath := filepath.Join(t.TempDir(), "app.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if want := cluster.CodePermissionDenied.Exit(); code != want {
		t.Fatalf("a refused dump: exit %d, want %d (the ring's own class)\nstdout: %s\nstderr: %s", code, want, stdout, stderr)
	}
	want := `dump: database "app" is not one this user may describe. No dump file was written.`
	if !strings.Contains(stderr, want) {
		t.Errorf("the refusal does not say %q:\nstderr: %s", want, stderr)
	}
	requireNoDumpFile(t, outPath)
}

// TestDumpOfTenThousandTinyBucketsIsOneFile (size-gated, section 2.1): 10,000 buckets
// served 500 per page with cursors produce one file, the file verifies with 10,000
// buckets, and the run's heap growth stays inside 64 MiB, so a dump that collected the
// bucket names or their records would fail here.
func TestDumpOfTenThousandTinyBucketsIsOneFile(t *testing.T) {
	if os.Getenv("RINGCLI_LARGE_TESTS") != "1" {
		t.Skip("set RINGCLI_LARGE_TESTS=1")
	}
	const (
		bucketCount = 10000
		bucketPage  = 500
		heapBound   = 64 << 20
	)
	summaries := make([]ringclient.BucketSummary, bucketCount)
	declarations := make(map[string]ringclient.BucketInfo, bucketCount)
	for index := range summaries {
		name := fmt.Sprintf("b%05d", index)
		summaries[index] = ringclient.BucketSummary{Name: name, State: "active"}
		declarations["app/"+name] = ringclient.BucketInfo{Database: "app", Name: name, State: "active",
			Fields: []ringclient.FieldInfo{{Name: "_id_", Type: "string"}}}
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, bucketPage),
		bucketPages:   map[string][]ringclient.BucketPage{"app": dumpStubBucketPages(summaries, bucketPage)},
		descriptions:  map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations:  declarations,
	}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "many.hoarddump")

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping 10,000 buckets: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)
	t.Logf("heap in use: %d bytes before the run, %d bytes after it, growth %d bytes (bound %d bytes)",
		before.HeapInuse, after.HeapInuse, growth, int64(heapBound))
	if growth > heapBound {
		t.Errorf("the run grew the heap by %d bytes and a dump holds one page at a time, so the bound is %d bytes", growth, heapBound)
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("listing %s: %v", outDir, err)
	}
	if len(entries) != 1 {
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		t.Fatalf("the run left %v in %s; one dump is one file, with no .partial and no spill file", names, outDir)
	}
	if entries[0].Name() != filepath.Base(outPath) || !entries[0].Type().IsRegular() {
		t.Errorf("the run left %q (%v) in %s, want the one regular file %s", entries[0].Name(), entries[0].Type(), outDir, filepath.Base(outPath))
	}

	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump of 10,000 buckets does not pass the check restore --verify runs: %v", err)
	}
	if verification.Buckets != bucketCount {
		t.Errorf("the verification reads %d buckets, want %d", verification.Buckets, bucketCount)
	}
	if verification.Databases != 1 {
		t.Errorf("the verification reads %d databases, want 1", verification.Databases)
	}

	if want := bucketCount / bucketPage; stub.calls.ShowBuckets != want {
		t.Errorf("ShowBuckets was called %d times for %d buckets served %d per page, want %d (one page at a time, following the cursor)",
			stub.calls.ShowBuckets, bucketCount, bucketPage, want)
	}
	if stub.calls.largestBucketPage > bucketPage {
		t.Errorf("one ShowBuckets answer held %d buckets, and the stub serves %d per page", stub.calls.largestBucketPage, bucketPage)
	}
}

// dumpNumericDropText is the sentence a node builds when it has lost a bucket's declaration:
// it names the bucket and the database by numeric ids, so a dump must never show it to a
// developer (cluster/wire/node.go).
const dumpNumericDropText = "bucket 7 in database 2 is being dropped or was dropped: the record is an empty write"

// dumpDroppingStub scripts three active buckets in one database, each with a queue field.
// Bucket B is served two document pages of one entity each, so the dump writes its bucket
// record and its first entity before the second page is refused (Db4b).
func dumpDroppingStub(t *testing.T) *dumpStub {
	t.Helper()
	declaration := func(name string) ringclient.BucketInfo {
		return ringclient.BucketInfo{Database: "app", Name: name, State: "active", Fields: []ringclient.FieldInfo{
			{Name: dumpBucketIDField, Type: "string"},
			{Name: "Name", Type: "string"},
			{Name: "Queue", Type: dumpFifoStoreWord},
		}}
	}
	entity := func(id string) ringclient.Entry {
		return ringclient.Entry{ID: id, Body: dumpTestBody(t, id, id)}
	}
	return &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{
				{Name: "A", State: "active"},
				{Name: "B", State: "active"},
				{Name: "C", State: "active"},
			}, 3),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/A": declaration("A"),
			"app/B": declaration("B"),
			"app/C": declaration("C"),
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/A": dumpStubDocumentPages([]ringclient.Entry{entity("a1")}, 1),
			"app/B": dumpStubDocumentPages([]ringclient.Entry{entity("b1"), entity("b2")}, 1),
			"app/C": dumpStubDocumentPages([]ringclient.Entry{entity("c1")}, 1),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/A/a1/Queue": {{Elements: []bson.Raw{dumpTestElement(t, "am1")}}},
			"app/B/b1/Queue": {{Elements: []bson.Raw{dumpTestElement(t, "bm1")}}},
			"app/B/b2/Queue": {{Elements: []bson.Raw{dumpTestElement(t, "bm2")}}},
			"app/C/c1/Queue": {{Elements: []bson.Raw{dumpTestElement(t, "cm1")}}},
		},
	}
}

// TestBucketLeftDuringTheDump is the table of the small pure function: which ring answers
// mean the bucket this read names has left, and which do not. The two NOT_FOUND rows are
// the ones that need the forDocumentPage switch: a List or a count that finds nothing means
// the bucket, and a store page that finds nothing means the document was deleted after its
// List page (and is handled as before).
func TestBucketLeftDuringTheDump(t *testing.T) {
	tests := []struct {
		reason          string
		err             error
		forDocumentPage bool
		want            bool
	}{
		{
			reason: "BUCKET_DROPPING on a store page",
			err:    &ringclient.Error{Code: cluster.CodeBucketDropping, Message: "the bucket is leaving"},
			want:   true,
		},
		{
			reason:          "NOT_FOUND on a document page",
			err:             &ringclient.Error{Code: cluster.CodeNotFound, Message: `Bucket "Post" not found in database "app".`},
			forDocumentPage: true,
			want:            true,
		},
		{
			reason: "the node's numeric drop text under another code",
			err:    &ringclient.Error{Code: cluster.CodeUnavailable, Message: dumpNumericDropText},
			want:   true,
		},
		{
			reason: "NOT_FOUND on a store page is the deleted document",
			err:    &ringclient.Error{Code: cluster.CodeNotFound, Message: "document not found"},
			want:   false,
		},
		{
			reason:          "PERMISSION_DENIED",
			err:             &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "not allowed"},
			forDocumentPage: true,
			want:            false,
		},
		{
			reason:          "a plain error",
			err:             errors.New("the connection closed"),
			forDocumentPage: true,
			want:            false,
		},
		{
			reason:          "nil",
			err:             nil,
			forDocumentPage: true,
			want:            false,
		},
		{
			reason:          "OWNER_DOWN is a retry, not a departure",
			err:             &ringclient.Error{Code: cluster.CodeOwnerDown, Message: "the owner is down"},
			forDocumentPage: true,
			want:            false,
		},
	}
	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			if got := bucketLeftDuringTheDump(test.err, test.forDocumentPage); got != test.want {
				t.Errorf("bucketLeftDuringTheDump(%v, %v) = %v, want %v", test.err, test.forDocumentPage, got, test.want)
			}
		})
	}
}

// TestDumpOfABucketDroppedWhileItsStoresAreReadWarnsAndGoesOn: bucket B leaves the ring
// while its documents are read, once as the node's numeric drop sentence under UNAVAILABLE
// and once as BUCKET_DROPPING. The run keeps what it wrote of B, warns by name, goes on to C
// and exits 0 with a file that verifies; the server's numeric text never reaches the user
// (Db4b; section 3.5).
func TestDumpOfABucketDroppedWhileItsStoresAreReadWarnsAndGoesOn(t *testing.T) {
	dropped := func(code cluster.Code) error {
		return &ringclient.Error{Code: code, Message: dumpNumericDropText}
	}
	variants := []struct {
		reason  string
		failure error
	}{
		{reason: "the numeric drop text under UNAVAILABLE", failure: dropped(cluster.CodeUnavailable)},
		{reason: "BUCKET_DROPPING", failure: dropped(cluster.CodeBucketDropping)},
	}
	for _, variant := range variants {
		for _, parallel := range []string{"1", "4"} {
			t.Run(variant.reason+", parallel "+parallel, func(t *testing.T) {
				stub := dumpDroppingStub(t)
				// B's first document page is served; the second is refused, so the bucket
				// record and B's first entity are already in the file.
				stub.failuresFirst = map[string]failFirst{"ListDocuments app/B": {skip: 1, count: 1, err: variant.failure}}
				outPath := filepath.Join(t.TempDir(), "dropped.hoarddump")

				code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", parallel, "--out", outPath)
				if code != ExitOK {
					t.Fatalf("dumping a bucket that is dropped mid-run: exit %d, want %d (a departed bucket is a warning of the file's remaining part, never a failed run)\nstdout: %s\nstderr: %s",
						code, ExitOK, stdout, stderr)
				}
				if stdout != "" {
					t.Errorf("a text run wrote to stdout: %q (the report goes to stderr)", stdout)
				}
				warning := "Bucket app.B was dropped while the dump was reading it, so this file holds only what was read before the drop;"
				if got := strings.Count(stderr, warning); got != 1 {
					t.Errorf("the by-name warning appears %d times, want exactly once:\nstderr: %s", got, stderr)
				}
				if strings.Contains(stderr, "bucket 7") {
					t.Errorf("the report carries the server's numeric bucket text:\nstderr: %s", stderr)
				}
				verification, err := verifyDump(outPath)
				if err != nil {
					t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
				}
				// A and C complete (one entity each) and B's first entity: the part of B read
				// before the drop.
				if verification.Entities != 3 {
					t.Errorf("the verification reads %d entities, want 3 (all of A and C and B's first entity)", verification.Entities)
				}
				records := readDumpRecords(t, outPath)
				for _, name := range []string{"A", "B", "C"} {
					dumpBucketNamed(t, records, "app", name)
				}
				dumpDocNamed(t, records, "app", "A", "a1")
				dumpDocNamed(t, records, "app", "B", "b1")
				dumpDocNamed(t, records, "app", "C", "c1")
				if docs := dumpDocsOf(records, "app", "B"); len(docs) != 1 {
					t.Errorf("the file holds %d doc records of B, want 1: the bucket's second page was never read", len(docs))
				}
				chunks := dumpElemsOf(records, "app", "B", "b1", "Queue")
				if got, want := dumpChunkStrings(t, chunks, "Message"), []string{"bm1"}; !reflect.DeepEqual(got, want) {
					t.Errorf("B's queue holds %v, want %v: what was read before the drop is kept", got, want)
				}
			})
		}
	}
}

// TestDumpOfABucketDroppedBeforeItIsDescribedWarnsAndGoesOn: a bucket that was listed by
// ShowBuckets is gone by the time the dump describes it. The run warns by name and goes on
// with the other buckets; and a --bucket named bucket whose DescribeBucket answers NOT_FOUND
// keeps the refusal that names the listing statement and exit 2 (Db4b).
func TestDumpOfABucketDroppedBeforeItIsDescribedWarnsAndGoesOn(t *testing.T) {
	declaration := func(name string) ringclient.BucketInfo {
		return ringclient.BucketInfo{Database: "app", Name: name, State: "active", Fields: []ringclient.FieldInfo{
			{Name: dumpBucketIDField, Type: "string"},
			{Name: "Title", Type: "string"},
		}}
	}
	newStub := func() *dumpStub {
		return &dumpStub{
			databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
			bucketPages: map[string][]ringclient.BucketPage{
				"app": dumpStubBucketPages([]ringclient.BucketSummary{
					{Name: "Gone", State: "active"},
					{Name: "Kept", State: "active"},
				}, 2),
			},
			descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
			declarations: map[string]ringclient.BucketInfo{
				"app/Gone": declaration("Gone"),
				"app/Kept": declaration("Kept"),
			},
			documentPages: map[string][]ringclient.EntryPage{
				"app/Gone": {{Entries: []ringclient.Entry{{ID: "g1", Body: dumpTestBody(t, "g1", "gone")}}}},
				"app/Kept": {{Entries: []ringclient.Entry{{ID: "k1", Body: dumpTestBody(t, "k1", "kept")}}}},
			},
			failures: map[string]error{
				"DescribeBucket app/Gone": &ringclient.Error{Code: cluster.CodeNotFound,
					Message: `Bucket "Gone" not found in database "app". Run 'show buckets in app' to list what exists.`},
			},
		}
	}

	t.Run("a listed bucket", func(t *testing.T) {
		stub := newStub()
		outPath := filepath.Join(t.TempDir(), "gone.hoarddump")
		code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
		if code != ExitOK {
			t.Fatalf("dumping a database whose listed bucket is gone: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
		}
		const warning = "Bucket app.Gone was dropped before the dump could read it, so it is not in this file."
		if got := strings.Count(stderr, warning); got != 1 {
			t.Errorf("the by-name warning appears %d times, want exactly once:\nstderr: %s", got, stderr)
		}
		verification, err := verifyDump(outPath)
		if err != nil {
			t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
		}
		if verification.Buckets != 1 || verification.Entities != 1 {
			t.Errorf("the verification reads %d buckets and %d entities, want 1 and 1 (the bucket that was kept)", verification.Buckets, verification.Entities)
		}
		records := readDumpRecords(t, outPath)
		dumpBucketNamed(t, records, "app", "Kept")
		dumpDocNamed(t, records, "app", "Kept", "k1")
		if buckets := dumpBucketsOf(records, "app"); len(buckets) != 1 || buckets[0].Name != "Kept" {
			t.Errorf("the file's bucket records are %v, want only Kept: a bucket that vanished is not in the file", buckets)
		}
	})

	t.Run("a --bucket named bucket", func(t *testing.T) {
		stub := newStub()
		outDir := t.TempDir()
		outPath := filepath.Join(outDir, "named.hoarddump")
		code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--bucket", "Gone", "--out", outPath)
		if code != ExitRefused {
			t.Fatalf("dumping a --bucket named bucket that vanished: exit %d, want %d (a named bucket keeps the refusal)\nstdout: %s\nstderr: %s",
				code, ExitRefused, stdout, stderr)
		}
		const refusal = `Bucket "Gone" not found in database "app". Run 'show buckets in app' to list what exists.`
		if !strings.Contains(stderr, refusal) {
			t.Errorf("the refusal does not name the listing statement %q:\nstderr: %s", refusal, stderr)
		}
		if strings.Contains(stderr, "was dropped before the dump could read it") {
			t.Errorf("a --bucket named bucket gets the departed-bucket warning instead of the refusal:\nstderr: %s", stderr)
		}
		if entries, err := os.ReadDir(outDir); err != nil {
			t.Fatalf("listing %s: %v", outDir, err)
		} else if len(entries) != 0 {
			t.Errorf("the refused dump left %d files in %s", len(entries), outDir)
		}
	})
}

// TestDumpOfAnEntityCutByADropStillVerifies: a bucket is dropped in the middle of one
// entity, on the second page of its queue. Whatever the file holds, a run that exits 0 must
// leave a file that passes the check restore --verify runs, and the warning must say which
// part of the entity is in it. --parallel 1 writes the entity straight to the file, so its
// doc record is already there; --parallel 4 reads it into a buffer that is thrown away, so
// the entity is not.
func TestDumpOfAnEntityCutByADropStillVerifies(t *testing.T) {
	cases := []struct {
		parallel     string
		warning      string
		wantEntities int64
		wantB1       bool
	}{
		{"1", "Bucket app.B was dropped while the dump was reading it, so this file holds only what was read before the drop; the entity being read at that moment may be only partly in it.", 3, true},
		{"4", "Bucket app.B was dropped while the dump was reading it, so this file holds only what was read before the drop; the entities being read at that moment are not in it.", 2, false},
	}
	for _, tc := range cases {
		t.Run("parallel "+tc.parallel, func(t *testing.T) {
			stub := dumpDroppingStub(t)
			stub.elementPages["app/B/b1/Queue"] = dumpStubElementPages(ringclient.ElementPage{Elements: []bson.Raw{
				dumpTestElement(t, "bm1"), dumpTestElement(t, "bm1b"),
			}}, 1)
			stub.failuresFirst = map[string]failFirst{"StoreList app/B/b1/Queue": {skip: 1, count: 1,
				err: &ringclient.Error{Code: cluster.CodeBucketDropping, Message: dumpNumericDropText}}}
			outPath := filepath.Join(t.TempDir(), "cut.hoarddump")

			code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", tc.parallel, "--out", outPath)
			if code != ExitOK {
				t.Fatalf("exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
			}
			if got := strings.Count(stderr, tc.warning); got != 1 {
				t.Errorf("the warning %q appears %d times, want once:\nstderr: %s", tc.warning, got, stderr)
			}
			verification, err := verifyDump(outPath)
			if err != nil {
				t.Fatalf("a run that exited 0 left a file that fails restore --verify: %v", err)
			}
			if verification.Entities != tc.wantEntities {
				t.Errorf("the file counts %d entities, want %d", verification.Entities, tc.wantEntities)
			}
			held := false
			for _, doc := range dumpDocsOf(readDumpRecords(t, outPath), "app", "B") {
				held = held || doc.ID == "b1"
			}
			if held != tc.wantB1 {
				t.Errorf("the file holds b1's doc record: %v, want %v", held, tc.wantB1)
			}
		})
	}
}
