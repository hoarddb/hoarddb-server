package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringIngestLogin is the connection flags of the test host: its address and the root login
// newRingTestHost seeded.
func ringIngestLogin(addr string) []string {
	return []string{"-address", addr, "-user", "root", "-password=live-test-password", "-insecure"}
}

// runRingIngestForTest runs the verb the way main does, with its own streams.
func runRingIngestForTest(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	code = runRingIngest(ctx, args, ringIngestEnv{stdout: &out, stderr: &errOut})
	return code, out.String(), errOut.String()
}

// ingestOnHost runs the verb against the test host at addr.
func ingestOnHost(t *testing.T, addr string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runRingIngestForTest(t, append(append([]string{}, args...), ringIngestLogin(addr)...)...)
}

func writeBSONFile(t *testing.T, name string, documents ...bson.D) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, bsonFileOf(t, documents...), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeTextFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// listIDs returns the ids in app.users, sorted.
func listIDs(t *testing.T, client *ringclient.Client, bucket string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var ids []string
	var cursor []byte
	for {
		page, err := client.Bucket("app", bucket).List(ctx, nil, ringclient.PageRequest{Limit: 500, Cursor: cursor})
		if err != nil {
			t.Fatalf("list app.%s: %v", bucket, err)
		}
		for _, entry := range page.Entries {
			ids = append(ids, entry.ID)
		}
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	sort.Strings(ids)
	return ids
}

func TestIngestMongodumpBsonIntoRingBucketRendersObjectIDsAsHex(t *testing.T) {
	addr, client := newRingTestHost(t)
	var documents []bson.D
	var wantIDs []string
	for i := 0; i < 3; i++ {
		id := primitive.NewObjectID()
		wantIDs = append(wantIDs, id.Hex())
		documents = append(documents, bson.D{{Key: "_id", Value: id}, {Key: "name", Value: fmt.Sprintf("user%d", i)}, {Key: "age", Value: int32(20 + i)}})
	}
	path := writeBSONFile(t, "users.bson", documents...)
	code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.users")
	if code != ExitOK {
		t.Fatalf("exit %d, want 0:\n%s", code, stderr)
	}
	sort.Strings(wantIDs)
	got := listIDs(t, client, "users")
	if strings.Join(got, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("list returned ids %v, want the 24-hex strings %v", got, wantIDs)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stored, err := client.Bucket("app", "users").Get(ctx, wantIDs[0])
	if err != nil {
		t.Fatalf("get by the hex string: %v", err)
	}
	if stored.Lookup("name").Type != bson.TypeString {
		t.Errorf("stored document %v has no string name field", stored)
	}
	info, err := client.DescribeBucket(ctx, "app", "users")
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]string{}
	for _, field := range info.Fields {
		declared[field.Name] = field.Type
	}
	if declared["name"] != "string" || declared["age"] != "int" {
		t.Errorf("declared %v, want name string and age int (inferred from the file)", declared)
	}
	if !strings.Contains(stderr, "3 read, 3 written") {
		t.Errorf("the summary should say 3 read, 3 written:\n%s", stderr)
	}
	if strings.Contains(stderr, "legacy engine") || strings.Contains(stderr, "-ring") {
		t.Errorf("the run still prints the interim note about the legacy default:\n%s", stderr)
	}
}

func TestIngestRerunIsIdempotentOnTheRing(t *testing.T) {
	addr, client := newRingTestHost(t)
	path := writeBSONFile(t, "users.bson",
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "n", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "n", Value: int32(3)}},
	)
	for run := 1; run <= 2; run++ {
		if code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.users"); code != ExitOK {
			t.Fatalf("run %d: exit %d:\n%s", run, code, stderr)
		}
		if got := listIDs(t, client, "users"); len(got) != 3 {
			t.Fatalf("after run %d the bucket holds %d documents, want 3", run, len(got))
		}
	}
}

// TestIngestTenThousandDocumentsTwice is the acceptance of slice I1a: a file of 10,000
// documents loads in one run and a second run changes nothing.
func TestIngestTenThousandDocumentsTwice(t *testing.T) {
	addr, client := newRingTestHost(t)
	documents := make([]bson.D, 10000)
	for i := range documents {
		documents[i] = bson.D{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "name", Value: fmt.Sprintf("user%d", i)}, {Key: "n", Value: int64(i)}}
	}
	path := writeBSONFile(t, "users.bson", documents...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for run := 1; run <= 2; run++ {
		started := time.Now()
		if code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.users"); code != ExitOK {
			t.Fatalf("run %d: exit %d:\n%s", run, code, stderr)
		}
		t.Logf("run %d of 10,000 documents took %s", run, time.Since(started).Round(time.Millisecond))
		count, err := client.Bucket("app", "users").Count(ctx, nil)
		if err != nil || count != 10000 {
			t.Fatalf("after run %d: count %d, error %v; want 10000", run, count, err)
		}
	}
}

func TestIngestInfersDeclarationAndDryRunCreatesNothing(t *testing.T) {
	addr, client := newRingTestHost(t)
	path := writeBSONFile(t, "users.bson",
		bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}, {Key: "age", Value: int32(3)}, {Key: "score", Value: 1.5}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "name", Value: "Bo"}, {Key: "age", Value: 4.5}},
	)
	code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.users", "--dry-run")
	if code != ExitOK {
		t.Fatalf("exit %d:\n%s", code, stderr)
	}
	for _, want := range []string{"dry run", "2 read", "would create database app", "would create bucket app.users", "name string", "age number", "score double"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the dry-run report does not say %q:\n%s", want, stderr)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DescribeDatabase(ctx, "app", ringclient.PageRequest{Limit: 1}); err == nil {
		t.Errorf("a dry run created database app")
	}
}

func TestIngestOnErrorSkipWritesTheRejectsFile(t *testing.T) {
	addr, client := newRingTestHost(t)
	content := "{\"_id\":\"a\",\"n\":1}\nthis is not json\n{\"n\":3}\n{\"_id\":\"d\",\"n\":4}\n"
	path := writeTextFile(t, "users.jsonl", content)
	rejects := filepath.Join(t.TempDir(), "users.rejects.jsonl")

	// The default stops, before anything is written, naming the first bad document.
	code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.users")
	if code != ExitRefused {
		t.Fatalf("a file with a bad line, default --on-error stop: exit %d, want %d:\n%s", code, ExitRefused, stderr)
	}
	for _, want := range []string{"Document 2 of users.jsonl", "--on-error skip", "Nothing was written"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, stderr)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DescribeDatabase(ctx, "app", ringclient.PageRequest{Limit: 1}); err == nil {
		t.Fatalf("the stopped run created database app")
	}

	code, _, stderr = ingestOnHost(t, addr, path, "--into", "app.users", "--on-error", "skip", "--rejects", rejects)
	if code != ExitPartial {
		t.Fatalf("--on-error skip: exit %d, want %d (completed with documents skipped):\n%s", code, ExitPartial, stderr)
	}
	if got := listIDs(t, client, "users"); strings.Join(got, ",") != "a,d" {
		t.Errorf("loaded ids %v, want a and d", got)
	}
	saved, err := os.ReadFile(rejects)
	if err != nil {
		t.Fatalf("reading the rejects file: %v", err)
	}
	if string(saved) != "this is not json\n{\"n\":3}\n" {
		t.Errorf("rejects file holds %q, want the two skipped lines", saved)
	}
	if !strings.Contains(stderr, "2 skipped") || !strings.Contains(stderr, rejects) {
		t.Errorf("the summary should count 2 skipped and name the rejects file:\n%s", stderr)
	}
	// A rejects file is never overwritten.
	if code, _, stderr = ingestOnHost(t, addr, path, "--into", "app.users", "--on-error", "skip", "--rejects", rejects); code != ExitUsage || !strings.Contains(stderr, "already exists") {
		t.Errorf("a second run onto an existing rejects file: exit %d, stderr %q; want a usage refusal that says it exists", code, stderr)
	}
}

func TestRemovedFlagsNameTheirReplacement(t *testing.T) {
	_, stderr := removedFlagRun(t, "--type", "fifo")
	if want := "--type is gone: a bucket has no type. Declare what it holds with 'create bucket <name> in <db> { <Field> <type> }', then load the file with --into <database>.<bucket>."; !strings.Contains(stderr, want) {
		t.Errorf("--type: stderr %q does not contain %q", stderr, want)
	}
	_, stderr = removedFlagRun(t, "--id-as=extjson")
	if want := "--id-as is gone:"; !strings.Contains(stderr, want) || !strings.Contains(stderr, "~i:1") {
		t.Errorf("--id-as: stderr %q should say it is gone and show the id rule", stderr)
	}
	for _, name := range []string{"write-concern", "allow-expiring", "state", "no-resume", "resume-anyway", "accept-duplicates", "from-offset", "again", "checkpoint-every", "allow-dots", "max-buckets"} {
		code, stderr := removedFlagRun(t, "--"+name)
		if code != ExitUsage || !strings.Contains(stderr, "--"+name+" is gone:") {
			t.Errorf("--%s: exit %d, stderr %q; want a usage refusal that says it is gone", name, code, stderr)
		}
	}
}

// removedFlagRun runs ingest with a removed flag and no server: the refusal comes before any
// dial.
func removedFlagRun(t *testing.T, flagArgs ...string) (int, string) {
	t.Helper()
	path := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}})
	code, _, stderr := runRingIngestForTest(t, append([]string{path, "--into", "app.users"}, flagArgs...)...)
	return code, stderr
}

func TestIntoDBNamesThePerFileCommand(t *testing.T) {
	path := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}})
	want := "Loading a whole directory is not available. Load each file into its bucket: hoarddb ingest <dir>/<name>.bson --into <database>.<name>"
	code, _, stderr := runRingIngestForTest(t, filepath.Dir(path), "--into-db", "app")
	if code != ExitUsage || !strings.Contains(stderr, want) {
		t.Errorf("--into-db: exit %d, stderr %q; want a usage refusal with %q", code, stderr, want)
	}
	// A directory named without --into-db meets the same sentence.
	code, _, stderr = runRingIngestForTest(t, filepath.Dir(path), "--into", "app.users")
	if code != ExitUsage || !strings.Contains(stderr, want) {
		t.Errorf("a directory with --into: exit %d, stderr %q; want %q", code, stderr, want)
	}
}

func TestThreePartIntoSaysTheStoreFieldFormIsNotAvailable(t *testing.T) {
	path := writeBSONFile(t, "orders.bson", bson.D{{Key: "_id", Value: "a"}})
	want := "Loading a file into a store field of other documents is not available. Load the file into a bucket of its own with --into <database>.<bucket>, or push its elements from a driver."
	code, _, stderr := runRingIngestForTest(t, path, "--into", "app.User.Purchases")
	if code != ExitUsage || !strings.Contains(stderr, want) {
		t.Errorf("a three-part --into: exit %d, stderr %q; want a usage refusal with %q", code, stderr, want)
	}
}

func TestIngestExitCodes(t *testing.T) {
	addr, _ := newRingTestHost(t)
	good := writeBSONFile(t, "users.bson", bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(1)}})
	skippy := writeTextFile(t, "skippy.jsonl", "{\"_id\":\"a\"}\nnot json\n")

	if code, _, stderr := ingestOnHost(t, addr, good, "--into", "app.users"); code != ExitOK {
		t.Errorf("a clean load: exit %d, want 0:\n%s", code, stderr)
	}
	if code, _, stderr := ingestOnHost(t, addr, skippy, "--into", "app.skippy", "--on-error", "skip"); code != ExitPartial {
		t.Errorf("a load that skipped a document: exit %d, want 3:\n%s", code, stderr)
	}
	// 6: nothing to dial.
	if code, _, stderr := runRingIngestForTest(t, good, "--into", "app.users", "-address", "127.0.0.1:1", "-user", "root", "-password=x", "-insecure"); code != ExitConnect {
		t.Errorf("an address nobody listens on: exit %d, want 6:\n%s", code, stderr)
	}
	// 7: a password the server refuses.
	if code, _, stderr := runRingIngestForTest(t, good, "--into", "app.users", "-address", addr, "-user", "root", "-password=wrong", "-insecure"); code != ExitAuth {
		t.Errorf("a wrong password: exit %d, want 7:\n%s", code, stderr)
	}
	// 1: a usage mistake before any dial.
	if code, _, _ := runRingIngestForTest(t, good); code != ExitUsage {
		t.Errorf("no --into: exit %d, want 1", code)
	}
	// 2: the ring's state refuses: --no-create against a bucket that does not exist.
	if code, _, stderr := ingestOnHost(t, addr, good, "--into", "app.absent", "--no-create"); code != ExitRefused {
		t.Errorf("--no-create on a missing bucket: exit %d, want 2:\n%s", code, stderr)
	}
}

// TestIngestMixedIDTypesInOneFileKeepEveryDocument: an integer, the string "1", a double and a
// date are four documents, none overwriting another.
func TestIngestMixedIDTypesInOneFileKeepEveryDocument(t *testing.T) {
	addr, client := newRingTestHost(t)
	path := writeBSONFile(t, "mixed.bson",
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: "int"}},
		bson.D{{Key: "_id", Value: "1"}, {Key: "k", Value: "string"}},
		bson.D{{Key: "_id", Value: 2.5}, {Key: "k", Value: "double"}},
		bson.D{{Key: "_id", Value: primitive.DateTime(1700000000000)}, {Key: "k", Value: "date"}},
	)
	if code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.mixed"); code != ExitOK {
		t.Fatalf("exit %d:\n%s", code, stderr)
	}
	got := listIDs(t, client, "mixed")
	want := []string{"1", "~d:2.5", "~i:1", "~t:1700000000000"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("ids %v, want %v", got, want)
	}
}

// TestIngestRefusesStandardInputBecauseItReadsTheFileTwice: the declaration comes from a first
// pass over the file, so a pipe cannot be loaded, and the refusal says what to do.
func TestIngestRefusesStandardInputBecauseItReadsTheFileTwice(t *testing.T) {
	code, _, stderr := runRingIngestForTest(t, "-", "--into", "app.users", "--format", "bson")
	if code != ExitUsage || !strings.Contains(stderr, "reads the file twice") || !strings.Contains(stderr, "Save it to a file") {
		t.Errorf("standard input: exit %d, stderr %q; want a usage refusal that says to save it to a file", code, stderr)
	}
}

// TestIngestWithAMissingFileNamesIt: nothing is dialed for a file that cannot be opened.
func TestIngestWithAMissingFileNamesIt(t *testing.T) {
	code, _, stderr := runRingIngestForTest(t, filepath.Join(t.TempDir(), "absent.bson"), "--into", "app.users")
	if code != ExitUsage || !strings.Contains(stderr, "absent.bson") {
		t.Errorf("a missing file: exit %d, stderr %q; want a usage refusal naming it", code, stderr)
	}
}

// TestIngestFileThatNeedsFieldsBeyondTheRingStopsBeforeCreating: a field name the ring cannot
// declare refuses the whole file before the database or bucket exists.
func TestIngestFileThatNeedsFieldsBeyondTheRingStopsBeforeCreating(t *testing.T) {
	addr, client := newRingTestHost(t)
	path := writeBSONFile(t, "odd.bson", bson.D{{Key: "_id", Value: "a"}, {Key: "", Value: "x"}})
	code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.odd")
	if code != ExitRefused || !strings.Contains(stderr, "cannot be declared on the ring") || !strings.Contains(stderr, "Nothing was written") {
		t.Fatalf("exit %d, stderr %q; want a refusal that names the field and says nothing was written", code, stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DescribeDatabase(ctx, "app", ringclient.PageRequest{Limit: 1}); err == nil {
		t.Errorf("a refused file created database app")
	}
}

// TestIngestDuplicateIDsAreReportedAsAWarning: two documents with one _id load as one, and the
// run says so by position.
func TestIngestDuplicateIDsAreReportedAsAWarning(t *testing.T) {
	addr, client := newRingTestHost(t)
	path := writeBSONFile(t, "dups.bson",
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(2)}},
	)
	code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.dups")
	if code != ExitOK || !strings.Contains(stderr, "documents 1 and 2 of dups.bson have the same _id") {
		t.Fatalf("exit %d, stderr %q; want success and the duplicate warning", code, stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	doc, err := client.Bucket("app", "dups").Get(ctx, "a")
	if err != nil || doc.Lookup("n").Int32() != 2 {
		t.Errorf("the surviving document is %v (error %v), want the later one (n = 2)", doc, err)
	}
}

// TestIngestExistingBucketKeepsItsDeclaration: a bucket the developer declared is loaded as
// declared, and a field it does not declare is refused with the alter that adds it.
func TestIngestExistingBucketKeepsItsDeclaration(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "users", []ringclient.FieldDecl{{Name: "name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	fits := writeBSONFile(t, "fits.bson", bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}})
	if code, _, stderr := ingestOnHost(t, addr, fits, "--into", "app.users"); code != ExitOK {
		t.Fatalf("a file that fits: exit %d:\n%s", code, stderr)
	}
	extra := writeBSONFile(t, "extra.bson", bson.D{{Key: "_id", Value: "b"}, {Key: "name", Value: "Bo"}, {Key: "email", Value: "b@x"}})
	code, _, stderr := ingestOnHost(t, addr, extra, "--into", "app.users")
	if code != ExitRefused || !strings.Contains(stderr, "alter bucket users in app { add email string }") {
		t.Errorf("an undeclared field: exit %d, stderr %q; want a refusal with the alter statement", code, stderr)
	}
	if got := listIDs(t, client, "users"); strings.Join(got, ",") != "a" {
		t.Errorf("ids %v after the refused file, want only a", got)
	}
}

// TestIngestOfATruncatedFileRefusesWholeEvenWithSkip: a file that cannot be read past a point
// has no next document to skip to, so --on-error skip does not help, and the text says so
// without a doubled full stop.
func TestIngestOfATruncatedFileRefusesWholeEvenWithSkip(t *testing.T) {
	content := append(bsonFileOf(t, bson.D{{Key: "_id", Value: "a"}}), 0xff, 0xff, 0xff, 0x7f, 0x01)
	path := writeTextFile(t, "cut.bson", string(content))
	addr, _ := newRingTestHost(t)
	code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.cut", "--on-error", "skip")
	if code != ExitUsage {
		t.Fatalf("exit %d, want %d:\n%s", code, ExitUsage, stderr)
	}
	if strings.Contains(stderr, "..") {
		t.Errorf("the refusal has a doubled full stop:\n%s", stderr)
	}
	for _, want := range []string{"Nothing was written", "--on-error skip does not help"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, stderr)
		}
	}
}

// TestIngestOfAnEmptyFileSaysNoBucketWasCreated: nothing to load creates nothing, and the
// report says so, in text and in --json.
func TestIngestOfAnEmptyFileSaysNoBucketWasCreated(t *testing.T) {
	addr, client := newRingTestHost(t)
	path := writeTextFile(t, "empty.bson", "")
	code, stdout, stderr := ingestOnHost(t, addr, path, "--into", "app.empty", "--json")
	if code != ExitOK {
		t.Fatalf("exit %d:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "no bucket was created") || !strings.Contains(stderr, "no documents") {
		t.Errorf("stderr should say the file has no documents and no bucket was created:\n%s", stderr)
	}
	if !strings.Contains(stdout, `"empty_input":true`) || !strings.Contains(stdout, `"created_bucket":false`) {
		t.Errorf("--json should say empty_input and created_bucket false:\n%s", stdout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DescribeBucket(ctx, "app", "empty"); err == nil {
		t.Errorf("an empty file created bucket app.empty")
	}
}
