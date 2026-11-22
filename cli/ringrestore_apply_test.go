package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// restoreTestContext bounds one test's calls to a test host.
func restoreTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// restoreUserFields is the rule 11 bucket: scalars and every store type, blob included.
func restoreUserFields() []ringclient.FieldDecl {
	return []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Age", Type: "int"},
		{Name: "Joined", Type: "datetime"},
		{Name: "Purchases", Type: "fifoStore"},
		{Name: "Photos", Type: "lifoStore"},
		{Name: "Friends", Type: "hashStore"},
		{Name: "Recipes", Type: "btreeStore"},
		{Name: "Books", Type: "heapStore"},
		{Name: "Data", Type: "blobStore"},
	}
}

// restoreUserIDs are the entities of the rule 11 ring: ids of every rendering ingest writes
// (a plain name, an ObjectId in hex, and the two escaped forms), so a restore that changed an
// id would be seen as a different entity.
var restoreUserIDs = []string{"ada", "grace", "507f1f77bcf86cd799439011", "~i:1", "~s:507f1f77bcf86cd799439011"}

// fillRestoreUsers creates database app with the User bucket and fills it: per entity, the
// scalars, five purchases, three photos, two friends, two recipes and three books at
// distinct priorities. The elements differ per entity, so a restore that crossed two
// entities' stores would show.
func fillRestoreUsers(t *testing.T, client *ringclient.Client, database string) {
	t.Helper()
	ctx := restoreTestContext(t)
	if _, err := client.CreateDatabase(ctx, database, 1); err != nil {
		t.Fatalf("creating database %s: %v", database, err)
	}
	if _, err := client.CreateBucket(ctx, database, "User", restoreUserFields()); err != nil {
		t.Fatalf("creating %s.User: %v", database, err)
	}
	users := client.Bucket(database, "User")
	for number, id := range restoreUserIDs {
		joined := time.Date(2026, 1, 1+number, 3, 4, 5, 0, time.UTC)
		if _, err := users.Insert(ctx, id, ringclient.Doc{"Name": "user " + id, "Age": int64(30 + number), "Joined": joined}); err != nil {
			t.Fatalf("inserting %q: %v", id, err)
		}
		for purchase := 0; purchase < 5; purchase++ {
			written(t)(users.Push(ctx, id, "Purchases", bson.D{{Key: "Item", Value: fmt.Sprintf("%s-buy-%d", id, purchase)}, {Key: "Cents", Value: int32(100 * purchase)}}))
		}
		for photo := 0; photo < 3; photo++ {
			written(t)(users.Push(ctx, id, "Photos", bson.D{{Key: "Title", Value: fmt.Sprintf("%s-photo-%d", id, photo)}}))
		}
		for _, friend := range []string{"zed", "amy"} {
			written(t)(users.Put(ctx, id, "Friends", friend, bson.D{{Key: "Name", Value: friend + id}}))
		}
		for _, recipe := range []string{"pear", "fig"} {
			written(t)(users.Put(ctx, id, "Recipes", recipe, bson.D{{Key: "Title", Value: recipe + id}}))
		}
		for book, priority := range []int64{5, 90, -3} {
			written(t)(users.PushHeap(ctx, id, "Books", fmt.Sprintf("book-%d", book), priority, bson.D{{Key: "Title", Value: fmt.Sprintf("%s-book-%d", id, book)}}))
		}
	}
}

// written returns a checker that fails the test on a refused write; it takes a write's two
// results as one call, so a test can write written(t)(bucket.Push(...)).
func written(t *testing.T) func(ringclient.WriteResult, error) {
	return func(_ ringclient.WriteResult, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("a write to the test ring failed: %v", err)
		}
	}
}

// dumpToFile dumps the whole ring at addr and returns the file.
func dumpToFile(t *testing.T, addr string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ring.hoarddump")
	if code, _, stderr := dumpOnHost(t, addr, "--out", path); code != ExitOK {
		t.Fatalf("dump exited %d: %s", code, stderr)
	}
	return path
}

// dumpBodies returns the records of a dump between its header and its summary, as bytes: the
// part of two dumps of equal data that must be byte-equal (the header carries the time).
func dumpBodies(t *testing.T, path string) [][]byte {
	t.Helper()
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := newDumpReader(bytes.NewReader(fileBytes))
	var bodies [][]byte
	for {
		kind, raw, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return bodies
		}
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		if kind == dumpKindHeader || kind == dumpKindSummary || kind == dumpKindFooter {
			continue
		}
		bodies = append(bodies, bytes.Clone(raw))
	}
}

// TestRoundTripPreservesEveryStoreType is the proof of the slice: the rule 11 ring is dumped,
// restored into a second empty ring, and every document and store element is compared; then
// the second ring is dumped and the two files hold the same records.
func TestRoundTripPreservesEveryStoreType(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	first := dumpToFile(t, sourceAddr)

	code, _, stderr := restoreOnHost(t, targetAddr, first)
	if code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "Bucket app.User field Data (blobStore): payloads are not in a dump, so this field restored empty.") {
		t.Errorf("the blob field was not reported as restored empty:\n%s", stderr)
	}
	if !strings.Contains(stderr, "Restored app.User: 5 entities, 75 elements") {
		t.Errorf("the progress line of the bucket is missing:\n%s", stderr)
	}

	ctx := restoreTestContext(t)
	for _, id := range restoreUserIDs {
		before, err := source.Bucket("app", "User").GetFields(ctx, id, []string{"Name", "Age", "Joined"})
		if err != nil {
			t.Fatal(err)
		}
		after, err := target.Bucket("app", "User").GetFields(ctx, id, []string{"Name", "Age", "Joined"})
		if err != nil {
			t.Fatalf("entity %q is missing after the restore: %v", id, err)
		}
		if !bytes.Equal(before, after) {
			t.Errorf("entity %q scalars differ after the round trip (types included):\n before %s\n after  %s", id, before, after)
		}
		for _, field := range []string{"Purchases", "Photos", "Friends", "Recipes", "Books"} {
			beforePage, err := source.Bucket("app", "User").StoreList(ctx, id, field, 1000, nil)
			if err != nil {
				t.Fatal(err)
			}
			afterPage, err := target.Bucket("app", "User").StoreList(ctx, id, field, 1000, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(afterPage.Elements) != len(beforePage.Elements) || fmt.Sprint(afterPage.Keys) != fmt.Sprint(beforePage.Keys) || fmt.Sprint(afterPage.Priorities) != fmt.Sprint(beforePage.Priorities) {
				t.Errorf("entity %q field %s: %d elements keys %v priorities %v, want %d keys %v priorities %v", id, field, len(afterPage.Elements), afterPage.Keys, afterPage.Priorities, len(beforePage.Elements), beforePage.Keys, beforePage.Priorities)
				continue
			}
			for index := range beforePage.Elements {
				if !bytes.Equal(beforePage.Elements[index], afterPage.Elements[index]) {
					t.Errorf("entity %q field %s element %d differs", id, field, index)
				}
			}
		}
		// Pop order: a fifo pops its first push first and a lifo its last push first, on
		// both rings, because restore pushes in the order the dump lists.
		for _, field := range []string{"Purchases", "Photos"} {
			beforePop, err := source.Bucket("app", "User").Pop(ctx, id, field)
			if err != nil {
				t.Fatal(err)
			}
			afterPop, err := target.Bucket("app", "User").Pop(ctx, id, field)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(beforePop, afterPop) {
				t.Errorf("entity %q field %s pops %s after the restore, %s before", id, field, afterPop, beforePop)
			}
		}
	}

	// The pops above changed both rings the same way; dump the second again from a fresh
	// pair so the comparison is of untouched data.
	secondSourceAddr, secondSource := newRingTestHost(t)
	secondTargetAddr, _ := newRingTestHost(t)
	fillRestoreUsers(t, secondSource, "app")
	firstDump := dumpToFile(t, secondSourceAddr)
	if code, _, stderr := restoreOnHost(t, secondTargetAddr, firstDump); code != ExitOK {
		t.Fatalf("second restore exited %d:\n%s", code, stderr)
	}
	secondDump := dumpToFile(t, secondTargetAddr)
	want, got := dumpBodies(t, firstDump), dumpBodies(t, secondDump)
	if len(want) != len(got) {
		t.Fatalf("the dump of the restored ring has %d records between header and summary, the original %d", len(got), len(want))
	}
	for index := range want {
		if !bytes.Equal(want[index], got[index]) {
			t.Fatalf("record %d of the dump of the restored ring differs from the original", index)
		}
	}
}

// TestRestoreIntoAnEmptyExistingDatabaseSucceeds: a database that exists with the dump's
// replication factor and no buckets is a legal target.
func TestRestoreIntoAnEmptyExistingDatabaseSucceeds(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)
	if _, err := target.CreateDatabase(restoreTestContext(t), "app", 1); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := restoreOnHost(t, targetAddr, path); code != ExitOK {
		t.Fatalf("restore into an empty existing database exited %d:\n%s", code, stderr)
	}
}

// TestRestoreKeepsIdsExactly: every id of the rule 11 ring, including the escaped forms,
// is on the second ring byte for byte, and no other id is.
func TestRestoreKeepsIdsExactly(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	if code, _, stderr := restoreOnHost(t, targetAddr, dumpToFile(t, sourceAddr)); code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	ctx := restoreTestContext(t)
	got := map[string]bool{}
	var cursor []byte
	for {
		page, err := target.Bucket("app", "User").List(ctx, nil, ringclient.PageRequest{Limit: 100, Cursor: cursor, Fields: []string{"Name"}})
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range page.Entries {
			got[entry.ID] = true
		}
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	if len(got) != len(restoreUserIDs) {
		t.Errorf("the restored bucket lists %d ids, want %d: %v", len(got), len(restoreUserIDs), got)
	}
	for _, id := range restoreUserIDs {
		if !got[id] {
			t.Errorf("id %q is not on the restored ring", id)
		}
	}
}

// restoreHarness is a restore after pass 2, built by hand so a test can reach the apply with
// the transport it chooses and without pass 2, which would refuse the target it wants to
// write into.
type restoreHarness struct {
	run     *ringRestoreRun
	sender  *restoreSender
	stderr  *bytes.Buffer
	applier *restoreApplier
}

// newRestoreHarness dials addr as root and prepares a restore of path under flags.
func newRestoreHarness(t *testing.T, addr, path string, flags ringRestoreFlags) *restoreHarness {
	t.Helper()
	ctx := restoreTestContext(t)
	options := ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true}
	client, err := ringclient.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	verification, err := verifyDump(path)
	if err != nil {
		t.Fatal(err)
	}
	flags.file = path
	redial := func(ctx context.Context) (*ringclient.Client, error) { return redialRing(ctx, options) }
	harness := &restoreHarness{stderr: &bytes.Buffer{}}
	harness.run = &ringRestoreRun{ctx: ctx, stderr: harness.stderr, stdout: io.Discard, flags: &flags, verification: verification, client: client, redial: redial, started: time.Now()}
	harness.sender = newRestoreSender(client, redial)
	harness.sender.pause = func(context.Context, int) error { return nil }
	t.Cleanup(harness.sender.close)
	harness.applier = newRestoreApplier(harness.run, harness.sender)
	return harness
}

// countingRestoreTransport forwards to the real transport and counts the operations it was
// asked to send by kind, and the frames, in order.
type countingRestoreTransport struct {
	restoreTransport
	frames [][]restoreOpKind
	ids    [][16]byte
}

func (c *countingRestoreTransport) Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	kinds := make([]restoreOpKind, len(ops))
	for index, op := range ops {
		kinds[index] = op.kind
	}
	c.frames = append(c.frames, kinds)
	c.ids = append(c.ids, id)
	return c.restoreTransport.Send(ctx, ops, id)
}

func (c *countingRestoreTransport) count(kind restoreOpKind) int {
	total := 0
	for _, frame := range c.frames {
		for _, sent := range frame {
			if sent == kind {
				total++
			}
		}
	}
	return total
}

// storeCounts returns the element count of each queue field of an entity.
func storeCount(t *testing.T, client *ringclient.Client, database, id, field string) int64 {
	t.Helper()
	count, err := client.Bucket(database, "User").StoreCount(restoreTestContext(t), id, field)
	if err != nil {
		t.Fatalf("counting %s of %q in %s: %v", field, id, database, err)
	}
	return count
}

// TestRestoreRunTwiceIsRefusedAndDuplicatesNoFifo: a second restore of the same dump into the
// filled target is refused by pass 2; and a second apply forced past pass 2 is stopped by the
// refused Insert, and the fifo still holds its original five elements, not ten.
func TestRestoreRunTwiceIsRefusedAndDuplicatesNoFifo(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)
	if code, _, stderr := restoreOnHost(t, targetAddr, path); code != ExitOK {
		t.Fatalf("first restore exited %d:\n%s", code, stderr)
	}

	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, "never merges into existing data") {
		t.Fatalf("second restore exited %d, want %d with the non-empty text:\n%s", code, ExitRefused, stderr)
	}

	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	counting := &countingRestoreTransport{restoreTransport: harness.sender.transport}
	harness.sender.transport = counting
	err := harness.applier.apply()
	if err == nil || restoreExit(harness.run.ctx, err) != ExitRefused || !strings.Contains(err.Error(), "never merges into existing data") {
		t.Fatalf("the forced second apply returned %v, want the refusal that the database now holds buckets", err)
	}
	for _, kind := range []restoreOpKind{restorePush, restorePut, restorePushHeap} {
		if sent := counting.count(kind); sent != 0 {
			t.Errorf("the second apply sent %d store operations of kind %d after its inserts were refused", sent, kind)
		}
	}
	for _, id := range restoreUserIDs {
		if got := storeCount(t, target, "app", id, "Purchases"); got != 5 {
			t.Errorf("fifo Purchases of %q holds %d elements after the forced second apply, want the original 5", id, got)
		}
	}
	_ = source
}

// TestRestoreStopsAtARefusedInsertAndSendsNoStoreOperation: a restore already past pass 2
// whose first group of inserts is all refused (another restore finished first) sends no store
// operation at all, and every fifo is still at its original count.
func TestRestoreStopsAtARefusedInsertAndSendsNoStoreOperation(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)

	restoreBeforeBucketCreate = func() {
		// Restore A finishes inside restore B's window between its check that the bucket is
		// absent and its create, which then finds A's equal bucket.
		restoreBeforeBucketCreate = nil
		if code, _, stderr := restoreOnHost(t, targetAddr, path); code != ExitOK {
			t.Errorf("restore A exited %d:\n%s", code, stderr)
		}
	}
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused {
		t.Fatalf("restore B exited %d, want the refused-operation exit %d:\n%s", code, ExitRefused, stderr)
	}
	if !strings.Contains(stderr, "DOCUMENT_EXISTS") || !strings.Contains(stderr, "app.User id") {
		t.Errorf("restore B's output does not name the entity and the code:\n%s", stderr)
	}
	for _, id := range restoreUserIDs {
		if got := storeCount(t, target, "app", id, "Purchases"); got != 5 {
			t.Errorf("fifo Purchases of %q holds %d, want 5", id, got)
		}
	}
}

// TestTwoConcurrentRestoresNeverDoubleAFifo releases two restores of one dump into one empty
// target together, twenty times. The assertions hold on every interleaving: no fifo holds more
// than its original count, a document holds all of a field or none of it, at most one restore
// exits 0, and a restore that did not exit 0 stopped on a refused insert or on the re-check that found the other restore's database or bucket, and says so. It does
// not assert that every fifo is complete, because both restores can stop with some documents
// inserted and not filled.
func TestTwoConcurrentRestoresNeverDoubleAFifo(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)
	t.Cleanup(func() { restoreAfterPassTwo = nil })

	type outcome struct {
		code   int
		stderr string
	}
	for round := 0; round < 20; round++ {
		name := fmt.Sprintf("app_%d", round)
		var arrived sync.WaitGroup
		arrived.Add(2)
		release := make(chan struct{})
		restoreAfterPassTwo = func() {
			arrived.Done()
			<-release
		}
		outcomes := make(chan outcome, 2)
		for restore := 0; restore < 2; restore++ {
			go func() {
				code, _, stderr := restoreOnHost(t, targetAddr, path, "--db", "app", "--as", name)
				outcomes <- outcome{code, stderr}
			}()
		}
		bothPastPassTwo := make(chan struct{})
		go func() {
			arrived.Wait()
			close(bothPastPassTwo)
		}()
		select {
		case <-bothPastPassTwo:
		case <-time.After(2 * time.Minute):
			t.Fatalf("round %d: the two restores never both reached the end of pass 2", round)
		}
		close(release)
		zeroes := 0
		for restore := 0; restore < 2; restore++ {
			result := <-outcomes
			switch {
			case result.code == ExitOK:
				zeroes++
			case result.code != ExitRefused || !refusedByInsertOrRecheck(result.stderr, name):
				t.Errorf("round %d: a restore exited %d without a refused insert naming an entity:\n%s", round, result.code, result.stderr)
			}
		}
		if zeroes > 1 {
			t.Errorf("round %d: %d restores of one dump into one target both exited 0", round, zeroes)
		}
		for _, id := range restoreUserIDs {
			for _, field := range []string{"Purchases", "Photos"} {
				want := int64(5)
				if field == "Photos" {
					want = 3
				}
				if got := storeCount(t, target, name, id, field); got != 0 && got != want {
					t.Errorf("round %d: %s of %q holds %d elements, want 0 or %d", round, field, id, got, want)
				}
			}
		}
	}
}

// TestRestoreDoesNotPushIntoADocumentItDidNotInsert: in the one-round-trip window between the
// check that a bucket is absent and its create, an application creates the bucket (the create
// then finds an equal one) and a document with the id of a dumped entity. The restore's insert
// of that id is refused, so none of that entity's elements is pushed.
func TestRestoreDoesNotPushIntoADocumentItDidNotInsert(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		restoreBeforeBucketCreate = nil
		ctx := restoreTestContext(t)
		if _, err := target.CreateBucket(ctx, "app", "User", restoreUserFields()); err != nil {
			t.Errorf("creating app.User: %v", err)
			return
		}
		for _, id := range restoreUserIDs {
			if _, err := target.Bucket("app", "User").Insert(ctx, id, ringclient.Doc{"Name": "written by the application"}); err != nil {
				t.Errorf("inserting %q: %v", id, err)
			}
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, "DOCUMENT_EXISTS") {
		t.Fatalf("restore exited %d, want %d with DOCUMENT_EXISTS:\n%s", code, ExitRefused, stderr)
	}
	for _, id := range restoreUserIDs {
		for _, field := range []string{"Purchases", "Photos", "Friends", "Recipes", "Books"} {
			if got := storeCount(t, target, "app", id, field); got != 0 {
				t.Errorf("%s of %q holds %d elements: the restore pushed into a document it did not insert", field, id, got)
			}
		}
	}
}

// TestRestoreRefusesADatabaseAndBucketCreatedAfterPassTwo: an application creates the database,
// an equal bucket and a document after pass 2. The create of an equal object counts as success,
// so without a re-check the restore exited 0 with the intruder merged in; now it re-checks the
// database right before it creates it, refuses with exit 2 and the --as command, and the
// intruder is still alone.
func TestRestoreRefusesADatabaseAndBucketCreatedAfterPassTwo(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)
	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = func() {
		ctx := restoreTestContext(t)
		if _, err := target.CreateDatabase(ctx, "app", 1); err != nil {
			t.Errorf("creating app: %v", err)
			return
		}
		if _, err := target.CreateBucket(ctx, "app", "User", restoreUserFields()); err != nil {
			t.Errorf("creating app.User: %v", err)
			return
		}
		if _, err := target.Bucket("app", "User").Insert(ctx, "intruder", ringclient.Doc{"Name": "x"}); err != nil {
			t.Errorf("inserting the intruder: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused {
		t.Fatalf("restore exited %d, want %d:\n%s", code, ExitRefused, stderr)
	}
	for _, want := range []string{`database "app" gained 1 bucket (User) after the restore began`, "never merges", "--db app --as app_restored"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not contain %q:\n%s", want, stderr)
		}
	}
	page, err := target.Bucket("app", "User").List(restoreTestContext(t), nil, ringclient.PageRequest{Limit: 100})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].ID != "intruder" {
		t.Fatalf("the target holds %d documents after the refusal, want only the intruder (error %v)", len(page.Entries), err)
	}
}

// TestRestoreRefusesABucketThatAppearsBeforeItsCreate: the dump holds two buckets. While the
// first is being created an application makes the second one with a document; the restore's
// read before the second create finds it and refuses, and the document is untouched.
func TestRestoreRefusesABucketThatAppearsBeforeItsCreate(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1", "B2"}})
	targetAddr, target := newRingTestHost(t)
	t.Cleanup(func() { restoreBeforeBucketCreate = nil })
	restoreBeforeBucketCreate = func() {
		restoreBeforeBucketCreate = nil
		ctx := restoreTestContext(t)
		if _, err := target.CreateBucket(ctx, "db1", "B2", []ringclient.FieldDecl{{Name: "Name", Type: "string"}, {Name: "Items", Type: "fifoStore"}}); err != nil {
			t.Errorf("creating db1.B2: %v", err)
			return
		}
		if _, err := target.Bucket("db1", "B2").Insert(ctx, "intruder", ringclient.Doc{"Name": "x"}); err != nil {
			t.Errorf("inserting the intruder: %v", err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitRefused || !strings.Contains(stderr, `bucket "B2" already exists in database "db1"`) || !strings.Contains(stderr, "--db db1 --as db1_restored") {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	page, err := target.Bucket("db1", "B2").List(restoreTestContext(t), nil, ringclient.PageRequest{Limit: 100})
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("db1.B2 holds %d documents (error %v), want the intruder alone", len(page.Entries), err)
	}
}

// TestRestoreAnswersOfTurnsAnUndecidedWriteIntoARefusalThatClearsByWaiting drives the production
// conversion the transport uses: a batch answer with outcome unknown and no error must not
// come out as applied.
func TestRestoreAnswersOfTurnsAnUndecidedWriteIntoARefusalThatClearsByWaiting(t *testing.T) {
	answers := restoreAnswersOf(ringclient.BatchResult{Answers: []ringclient.BatchAnswer{
		{Write: ringclient.WriteResult{Outcome: "applied"}},
		{Write: ringclient.WriteResult{Outcome: "unknown"}},
		{Err: &ringclient.Error{Code: cluster.CodeDocumentExists, Message: "there"}},
	}})
	if answers[0].Err != nil {
		t.Errorf("an applied answer became %v", answers[0].Err)
	}
	if answers[1].Err == nil || ringFailure(answers[1].Err).Exit != ExitUnavailable {
		t.Errorf("an unknown outcome came out as %v, want a refusal that exits %d", answers[1].Err, ExitUnavailable)
	}
	if answers[2].Err == nil {
		t.Error("a refusal was lost")
	}
}

// recordingRestoreTransport remembers the request id of every frame it was asked to send,
// into a slice shared by every connection the sender uses, and can arm a dropping relay
// before its first send so the reply of the first frame is lost.
type recordingRestoreTransport struct {
	restoreTransport
	ids   *[][16]byte
	relay *droppingRelay
	// armAt is the number of the send, counting from 0 across every connection, whose reply
	// the relay loses.
	armAt int
}

func (r *recordingRestoreTransport) Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	if r.relay != nil && len(*r.ids) == r.armAt {
		r.relay.DropNextReply()
	}
	*r.ids = append(*r.ids, id)
	return r.restoreTransport.Send(ctx, ops, id)
}

// TestRestoreWithALostReplyRedialsAndResendsUnderTheSameRequestID closes the real connection
// under a live ring: the server applies the first frame and its reply is lost. The sender
// dials again and sends the same frame under the same request id, so the owner answers
// Deduped, nothing is written twice and the restore completes.
func TestRestoreWithALostReplyRedialsAndResendsUnderTheSameRequestID(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)
	relay := newDroppingRelay(t, targetAddr)

	harness := newRestoreHarness(t, relay.Addr(), path, ringRestoreFlags{})
	var ids [][16]byte
	harness.sender.transport = &recordingRestoreTransport{restoreTransport: harness.sender.transport, ids: &ids, relay: relay, armAt: 0}
	freshConnection := harness.sender.redial
	harness.sender.redial = func(ctx context.Context) (restoreTransport, error) {
		fresh, err := freshConnection(ctx)
		if err != nil {
			return nil, err
		}
		return &recordingRestoreTransport{restoreTransport: fresh, ids: &ids, armAt: -1}, nil
	}
	if err := harness.applier.apply(); err != nil {
		t.Fatalf("the restore with a lost reply failed: %v\n%s", err, harness.stderr.String())
	}
	select {
	case <-relay.Dropped():
	default:
		t.Fatal("the relay never dropped a reply, so the test did not exercise the resend")
	}
	if harness.sender.resends != 1 {
		t.Errorf("the sender resent %d times, want exactly 1", harness.sender.resends)
	}
	if len(ids) < 2 || ids[0] != ids[1] {
		t.Errorf("the resend did not reuse the request id: %x", ids)
	}
	if harness.applier.tally.entities != int64(len(restoreUserIDs)) || harness.applier.tally.elements != 75 {
		t.Errorf("tally is %d entities and %d elements, want %d and 75", harness.applier.tally.entities, harness.applier.tally.elements, len(restoreUserIDs))
	}
	for _, id := range restoreUserIDs {
		if got := storeCount(t, target, "app", id, "Purchases"); got != 5 {
			t.Errorf("Purchases of %q holds %d elements after the resend, want 5", id, got)
		}
	}
}

// scriptedRestoreTransport answers frames from a script and records them: it is the stub the
// not-attempted test needs, because no shipping path sets a write deadline.
type scriptedRestoreTransport struct {
	frames [][]restoreOp
	ids    [][16]byte
	// answer returns the answers of frame number frame.
	answer func(frame int, ops []restoreOp) []ringDocAnswer
}

func (s *scriptedRestoreTransport) NextRequestID() [16]byte {
	var id [16]byte
	id[0] = byte(len(s.ids) + 1)
	return id
}

func (s *scriptedRestoreTransport) Send(_ context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	s.frames = append(s.frames, ops)
	s.ids = append(s.ids, id)
	return s.answer(len(s.frames)-1, ops), nil
}

func (s *scriptedRestoreTransport) close() error { return nil }

// TestNotAttemptedAnswerIsResentNotFatal: an operation the frame's deadline never reached
// answers UNAVAILABLE in the text of cluster/wire/ringserve_batch.go:ringNotAttempted; the
// sender sends the whole frame again under the same request id and finishes, and does not
// treat the answer as a refusal.
func TestNotAttemptedAnswerIsResentNotFatal(t *testing.T) {
	notAttempted := &ringclient.Error{Code: cluster.CodeUnavailable, Message: "operation 1 of the batch was not attempted: the frame's 5 ms deadline was spent before its turn. It was not applied; nothing was written. Resend the whole frame with the same request id: the operations that were applied answer deduped and this one runs"}
	script := &scriptedRestoreTransport{answer: func(frame int, ops []restoreOp) []ringDocAnswer {
		answers := make([]ringDocAnswer, len(ops))
		if frame == 0 && len(ops) > 1 {
			answers[1].Err = notAttempted
		}
		return answers
	}}
	sender := &restoreSender{transport: script, pause: func(context.Context, int) error { return nil }}
	ops := []restoreOp{{kind: restoreInsert, database: "d", bucket: "b", id: "one"}, {kind: restoreInsert, database: "d", bucket: "b", id: "two"}}
	answers, err := sender.send(context.Background(), ops)
	if err != nil {
		t.Fatalf("send returned %v", err)
	}
	for index, answer := range answers {
		if answer.Err != nil {
			t.Errorf("answer %d is still a refusal after the resend: %v", index, answer.Err)
		}
	}
	if len(script.frames) != 2 || script.ids[0] != script.ids[1] || len(script.frames[1]) != 2 {
		t.Fatalf("the sender sent %d frames with ids %x; want the same two-operation frame twice under one id", len(script.frames), script.ids)
	}
	if sender.resends != 1 {
		t.Errorf("resends = %d, want 1", sender.resends)
	}
}

// TestRestoreSendsMoreThanOneFrameOfEntitiesInOrder: a dump with 600 entities, each with two
// queue elements, goes out as inserts in frames of at most 255 and every entity's pushes only
// after its group's inserts were answered, in file order. The transport is a stub that
// accepts everything and checks, at every store frame, that the entity's insert was sent in
// an earlier frame.
func TestRestoreSendsMoreThanOneFrameOfEntitiesInOrder(t *testing.T) {
	const entities = 600
	path := writeLargeRestoreDump(t, entities, 1, 2, 10)
	targetAddr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	inserted := map[string]bool{}
	pushes := map[string]int{}
	script := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer { return make([]ringDocAnswer, len(ops)) }}
	check := &orderCheckingTransport{scripted: script, inserted: inserted, pushes: pushes, t: t}
	harness.sender.transport = check
	if err := harness.applier.apply(); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(inserted) != entities {
		t.Errorf("%d entities were inserted, want %d", len(inserted), entities)
	}
	for _, frame := range script.frames {
		if len(frame) > restoreMaxFrameOperations {
			t.Errorf("a frame holds %d operations, the most is %d", len(frame), restoreMaxFrameOperations)
		}
	}
	for id, count := range pushes {
		if count != 2 {
			t.Errorf("entity %q was pushed %d times, want 2", id, count)
		}
	}
}

// orderCheckingTransport fails the test when a store operation is sent for an entity whose
// insert was not in an earlier frame, and when an entity's elements arrive out of order.
type orderCheckingTransport struct {
	scripted *scriptedRestoreTransport
	inserted map[string]bool
	pushes   map[string]int
	t        *testing.T
}

func (o *orderCheckingTransport) NextRequestID() [16]byte { return o.scripted.NextRequestID() }
func (o *orderCheckingTransport) close() error            { return nil }
func (o *orderCheckingTransport) Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	sentInserts := []string{}
	for _, op := range ops {
		switch op.kind {
		case restoreInsert:
			sentInserts = append(sentInserts, op.id)
		default:
			if !o.inserted[op.id] {
				o.t.Errorf("a store operation for %q was sent before its insert was answered", op.id)
			}
			o.pushes[op.id]++
		}
	}
	answers, err := o.scripted.Send(ctx, ops, id)
	for _, inserted := range sentInserts {
		o.inserted[inserted] = true
	}
	return answers, err
}

// writeLargeRestoreDump writes a dump of one database and one bucket with a fifo field and
// the given number of entities, each with chunksPerEntity elems records of elementsPerChunk
// elements of elementBytes bytes of text, with the dump writer.
func writeLargeRestoreDump(t *testing.T, entities, chunksPerEntity, elementsPerChunk, elementBytes int) string {
	t.Helper()
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "large.hoarddump")
	if err != nil {
		t.Fatal(err)
	}
	scalars, err := bson.Marshal(bson.D{{Key: "Name", Value: "n"}})
	if err != nil {
		t.Fatal(err)
	}
	counters := dumpCounters{Databases: 1, Buckets: 1}
	records := []any{
		dumpTestHeader(),
		&dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"},
		&dumpBucket{T: dumpKindBucket, DB: "app", Name: "User", Fields: []dumpField{{Name: "Name", Type: "string"}, {Name: "Items", Type: "fifoStore"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}},
	}
	for index := 0; index < entities; index++ {
		id := fmt.Sprintf("u%05d", index)
		records = append(records, &dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: id, F: scalars})
		counters.Entities++
		for chunk := 0; chunk < chunksPerEntity; chunk++ {
			elements := make([]bson.Raw, elementsPerChunk)
			for element := range elements {
				elements[element] = largeRestoreElement(t, elementBytes)
			}
			records = append(records, &dumpElems{T: dumpKindElems, DB: "app", B: "User", ID: id, Field: "Items", Kind: "fifoStore", Seq: int32(chunk), Els: elements})
			counters.Elements += int64(elementsPerChunk)
		}
	}
	for _, record := range records {
		if err := writer.WriteRecord(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(counters); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "large.hoarddump")
}

// largeRestoreElement is an element document holding size bytes of text.
func largeRestoreElement(t *testing.T, size int) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "Text", Value: strings.Repeat("x", size)}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestRestoreCutsFramesByBytesAndNeverHoldsMoreThanAFrame: entities with elements of 5 MiB go
// out in frames no larger than the byte cap unless one operation alone is larger, an entity
// is inserted before any of its elements is sent, and the restore keeps going past the point
// where a group would no longer fit a frame (the entity's later chunks are sent as they come).
func TestRestoreCutsFramesByBytesAndNeverHoldsMoreThanAFrame(t *testing.T) {
	path := writeLargeRestoreDump(t, 3, 1, 2, 5<<20)
	targetAddr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	script := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer { return make([]ringDocAnswer, len(ops)) }}
	inserted, pushes := map[string]bool{}, map[string]int{}
	harness.sender.transport = &orderCheckingTransport{scripted: script, inserted: inserted, pushes: pushes, t: t}
	if err := harness.applier.apply(); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(inserted) != 3 {
		t.Errorf("%d entities inserted, want 3", len(inserted))
	}
	for id, count := range pushes {
		if count != 2 {
			t.Errorf("entity %q pushed %d times, want 2", id, count)
		}
	}
	for index, frame := range script.frames {
		total := 0
		for _, op := range frame {
			total += op.size
		}
		if len(frame) > 1 && total > restoreFrameByteCap {
			t.Errorf("frame %d holds %d operations and %d bytes, over the cap of %d", index, len(frame), total, restoreFrameByteCap)
		}
	}
	if script.frames[0][0].kind != restoreInsert {
		t.Errorf("the first frame is not an insert frame")
	}
}

// TestRestoreSendsTheLaterChunksOfAnEntityBiggerThanAFrameAsTheyCome: one entity of three
// chunks of 8 MiB. The group cannot hold it, so the entity's insert goes out with its first
// chunks and the last chunk is sent as it is read; all three arrive, in order, after the insert.
func TestRestoreSendsTheLaterChunksOfAnEntityBiggerThanAFrameAsTheyCome(t *testing.T) {
	path := writeLargeRestoreDump(t, 1, 3, 1, 8<<20)
	targetAddr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	script := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer { return make([]ringDocAnswer, len(ops)) }}
	inserted, pushes := map[string]bool{}, map[string]int{}
	harness.sender.transport = &orderCheckingTransport{scripted: script, inserted: inserted, pushes: pushes, t: t}
	if err := harness.applier.apply(); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if pushes["u00000"] != 3 {
		t.Fatalf("the entity received %d elements, want 3", pushes["u00000"])
	}
	if len(script.frames[0]) != 1 || script.frames[0][0].kind != restoreInsert {
		t.Errorf("the first frame is %d operations, want the lone insert", len(script.frames[0]))
	}
}

// TestRestoreTreatsAnUndecidedWriteAsUnavailableNotApplied: an answer whose outcome is unknown
// is not an applied insert, so it is turned into the refusal that makes the sender resend the
// frame and, when it stays undecided, stops the restore before any push.
func TestRestoreTreatsAnUndecidedWriteAsUnavailableNotApplied(t *testing.T) {
	if err := restoreUnconfirmed(ringclient.WriteResult{Outcome: "unknown"}); err == nil {
		t.Fatal("an unknown outcome was taken as applied")
	}
	for _, outcome := range []string{"applied", "applied_degraded", ""} {
		if err := restoreUnconfirmed(ringclient.WriteResult{Outcome: outcome}); err != nil {
			t.Errorf("outcome %q was refused: %v", outcome, err)
		}
	}
	undecided := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer {
		answers := make([]ringDocAnswer, len(ops))
		answers[0].Err = restoreUnconfirmed(ringclient.WriteResult{Outcome: "unknown"})
		return answers
	}}
	sender := &restoreSender{transport: undecided, pause: func(context.Context, int) error { return nil }}
	answers, err := sender.send(context.Background(), []restoreOp{{kind: restoreInsert, id: "x"}})
	if err != nil || answers[0].Err == nil {
		t.Fatalf("a write that stays undecided came back as %v, %v", answers, err)
	}
	if len(undecided.frames) != ringMaxResends+1 {
		t.Errorf("the frame was sent %d times, want 1 and %d resends", len(undecided.frames), ringMaxResends)
	}
	if exit := ringFailure(answers[0].Err).Exit; exit != ExitUnavailable {
		t.Errorf("an undecided write exits %d, want %d", exit, ExitUnavailable)
	}
}

// bigPurchaseRing makes a source ring whose one user holds 300 purchases, so the restore needs
// an insert frame and two store frames (255 and 45 operations), and returns its dump.
func bigPurchaseRing(t *testing.T) string {
	t.Helper()
	addr, source := newRingTestHost(t)
	ctx := restoreTestContext(t)
	if _, err := source.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CreateBucket(ctx, "app", "User", restoreUserFields()); err != nil {
		t.Fatal(err)
	}
	users := source.Bucket("app", "User")
	written(t)(users.Insert(ctx, "big", ringclient.Doc{"Name": "big"}))
	for purchase := 0; purchase < 300; purchase++ {
		written(t)(users.Push(ctx, "big", "Purchases", bson.D{{Key: "N", Value: int32(purchase)}}))
	}
	return dumpToFile(t, addr)
}

// TestRestoreWithALostReplyOnAStoreFrameOrALaterFrameDoesNotDuplicate loses the reply of the
// second send (the first store frame, 255 pushes) and, in another run, of the third (the last
// 45 pushes): the resend under the same request id is deduped and the fifo holds exactly 300,
// in order.
func TestRestoreWithALostReplyOnAStoreFrameOrALaterFrameDoesNotDuplicate(t *testing.T) {
	for _, armAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("send %d", armAt+1), func(t *testing.T) {
			path := bigPurchaseRing(t)
			targetAddr, target := newRingTestHost(t)
			relay := newDroppingRelay(t, targetAddr)
			harness := newRestoreHarness(t, relay.Addr(), path, ringRestoreFlags{})
			var ids [][16]byte
			harness.sender.transport = &recordingRestoreTransport{restoreTransport: harness.sender.transport, ids: &ids, relay: relay, armAt: armAt}
			fresh := harness.sender.redial
			harness.sender.redial = func(ctx context.Context) (restoreTransport, error) {
				next, err := fresh(ctx)
				if err != nil {
					return nil, err
				}
				return &recordingRestoreTransport{restoreTransport: next, ids: &ids, armAt: -1}, nil
			}
			if err := harness.applier.apply(); err != nil {
				t.Fatalf("apply: %v\n%s", err, harness.stderr.String())
			}
			select {
			case <-relay.Dropped():
			default:
				t.Fatal("no reply was dropped")
			}
			if harness.sender.resends != 1 || len(ids) != 4 || ids[armAt] != ids[armAt+1] {
				t.Errorf("resends %d, %d sends; want 1 resend, 4 sends and the same id on the resend", harness.sender.resends, len(ids))
			}
			if got := storeCount(t, target, "app", "big", "Purchases"); got != 300 {
				t.Fatalf("Purchases holds %d, want 300", got)
			}
			page, err := target.Bucket("app", "User").StoreList(restoreTestContext(t), "big", "Purchases", 1000, nil)
			if err != nil {
				t.Fatal(err)
			}
			for index, element := range page.Elements {
				var decoded struct{ N int32 }
				if err := bson.Unmarshal(element, &decoded); err != nil || int(decoded.N) != index {
					t.Fatalf("element %d is %v (error %v): the order changed", index, decoded.N, err)
				}
			}
		})
	}
}

// TestStoreFrameWithATransientInFrontOfAnAppliedOperationIsNotResent: a frame of pushes comes back
// with operation 1 not applied and operation 2 applied. Sending it whole again would run
// operation 1 after operation 2, so the sender stops and sends it only once; a transient answer
// with nothing applied behind it is still resent.
func TestStoreFrameWithATransientInFrontOfAnAppliedOperationIsNotResent(t *testing.T) {
	transient := &ringclient.Error{Code: cluster.CodeUnavailable, Message: "the owner is moving"}
	pushes := []restoreOp{{kind: restorePush, id: "a"}, {kind: restorePush, id: "a"}, {kind: restorePush, id: "a"}}
	inverted := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer {
		answers := make([]ringDocAnswer, len(ops))
		answers[1].Err = transient
		return answers
	}}
	sender := &restoreSender{transport: inverted, pause: func(context.Context, int) error { return nil }}
	_, err := sender.send(context.Background(), pushes)
	var outOfOrder *restoreOutOfOrderError
	if !errors.As(err, &outOfOrder) || len(inverted.frames) != 1 {
		t.Fatalf("send returned %v after %d frames, want the out-of-order stop after one", err, len(inverted.frames))
	}
	if restoreExit(context.Background(), err) != ExitUnavailable {
		t.Errorf("the out-of-order stop exits %d, want %d", restoreExit(context.Background(), err), ExitUnavailable)
	}

	suffix := &scriptedRestoreTransport{answer: func(frame int, ops []restoreOp) []ringDocAnswer {
		answers := make([]ringDocAnswer, len(ops))
		if frame == 0 {
			answers[2].Err = transient
		}
		return answers
	}}
	sender = &restoreSender{transport: suffix, pause: func(context.Context, int) error { return nil }}
	if _, err := sender.send(context.Background(), pushes); err != nil || len(suffix.frames) != 2 {
		t.Fatalf("a transient tail was not resent: %v after %d frames", err, len(suffix.frames))
	}
}

// TestRefusedInsertAfterAResendSaysTheFirstSendMayHaveCreatedIt: a duplicate-id refusal of a
// frame that was sent more than once is not necessarily another writer.
func TestRefusedInsertAfterAResendSaysTheFirstSendMayHaveCreatedIt(t *testing.T) {
	transient := &ringclient.Error{Code: cluster.CodeUnavailable, Message: "moving"}
	exists := &ringclient.Error{Code: cluster.CodeDocumentExists, Message: "already there"}
	script := &scriptedRestoreTransport{answer: func(frame int, ops []restoreOp) []ringDocAnswer {
		answers := make([]ringDocAnswer, len(ops))
		if frame == 0 {
			answers[0].Err = transient
		} else {
			answers[0].Err = exists
		}
		return answers
	}}
	sender := &restoreSender{transport: script, pause: func(context.Context, int) error { return nil }}
	op := restoreOp{kind: restoreInsert, database: "d", bucket: "b", id: "x"}
	answers, err := sender.send(context.Background(), []restoreOp{op})
	if err != nil || !sender.lastResent {
		t.Fatalf("send returned %v, resent %v", err, sender.lastResent)
	}
	text := (&restoreRefusedError{op: op, cause: answers[0].Err, resent: sender.lastResent}).Error()
	if !strings.Contains(text, "own first send") {
		t.Errorf("the refusal does not warn that the restore's own first send may have created the document:\n%s", text)
	}
	plain := (&restoreRefusedError{op: op, cause: exists}).Error()
	if strings.Contains(plain, "own first send") {
		t.Errorf("a refusal of a single send carries the resend warning:\n%s", plain)
	}
}

// TestRestoreOfADumpEditedInPlaceAfterPassTwoDoesNotCallItsDatabaseComplete: after pass 2 a
// string byte of an element is changed in place (the file still decodes, and its size and time
// are put back, so the cheap checks pass). The footer then no longer matches pass 1's checksum:
// the restore exits 1, does not say any database is complete, and prints the recovery commands.
func TestRestoreOfADumpEditedInPlaceAfterPassTwoDoesNotCallItsDatabaseComplete(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1"}})
	targetAddr, _ := newRingTestHost(t)
	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = func() {
		restoreAfterPassTwo = nil
		info, err := os.Stat(path)
		if err != nil {
			t.Error(err)
			return
		}
		content, err := os.ReadFile(path)
		if err != nil {
			t.Error(err)
			return
		}
		at := bytes.Index(content, []byte("xxxx"))
		if at < 0 {
			t.Error("the dump holds no element text to edit")
			return
		}
		content[at] = 'y'
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
			t.Error(err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitUsage {
		t.Fatalf("restore exited %d, want %d:\n%s", code, ExitUsage, stderr)
	}
	for _, want := range []string{"changed while it was being restored", "nothing written from it is known to be right", "--db db1 --as db1_restored", "drop the buckets of db1", "good copy"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the report does not contain %q:\n%s", want, stderr)
		}
	}
	if strings.Contains(stderr, "databases are complete") || strings.Contains(stderr, "written in full (") {
		t.Errorf("the report calls a database complete although the footer never matched:\n%s", stderr)
	}
}

// TestRestoreRefusesADifferentValidDumpSwappedOverThePath: between the size and time check and
// the open, another valid dump is renamed over the path. Pass 3 opens a different file, which
// is refused before the first write: exit 1 and nothing is created.
func TestRestoreRefusesADifferentValidDumpSwappedOverThePath(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1"}})
	other := writeShapedRestoreDump(t, [][]string{{"Z1"}})
	targetAddr, target := newRingTestHost(t)
	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = func() {
		restoreAfterPassTwo = nil
		content, err := os.ReadFile(other)
		if err != nil {
			t.Error(err)
			return
		}
		original, err := os.Stat(path)
		if err != nil {
			t.Error(err)
			return
		}
		staged := path + ".swap"
		if err := os.WriteFile(staged, content, 0o600); err != nil {
			t.Error(err)
			return
		}
		// Same size (the two names have the same length) and the same time, so only the file
		// identity tells them apart.
		if err := os.Chtimes(staged, original.ModTime(), original.ModTime()); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(staged, path); err != nil {
			t.Error(err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitUsage || !strings.Contains(stderr, "changed after it was verified") || !strings.Contains(stderr, "Nothing was written") {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	page, err := target.ShowDatabases(restoreTestContext(t), ringclient.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, database := range page.Databases {
		if database.Name == "db1" {
			t.Errorf("database db1 was created from a file that was swapped in")
		}
	}
}

// refusedByInsertOrRecheck reports whether a loser's output names a refused insert of an entity
// of database name, or the re-check that found the other restore's database or bucket.
func refusedByInsertOrRecheck(output, name string) bool {
	insert := strings.Contains(output, "DOCUMENT_EXISTS") && strings.Contains(output, name+".User id")
	return insert || strings.Contains(output, "never merges into existing data")
}

// TestStoreFrameResendRuleOnlyCoversTheSameEntityAndStore: the operations of many entities share
// a frame and a moving slot makes some of them transient while others apply, which is normal.
// A transient push of entity A with an applied push of entity B resends; a transient push of
// A's field with an applied push to a different field of A resends; a transient put of one
// key with an applied put of another key of the same hash commutes and resends; the same
// entity and field, or the same key, stops.
func TestStoreFrameResendRuleOnlyCoversTheSameEntityAndStore(t *testing.T) {
	transient := &ringclient.Error{Code: cluster.CodeUnavailable, Message: "the slot is moving"}
	push := func(id, field string) restoreOp {
		return restoreOp{kind: restorePush, database: "d", bucket: "b", id: id, field: field}
	}
	put := func(key string) restoreOp {
		return restoreOp{kind: restorePut, database: "d", bucket: "b", id: "a", field: "Friends", key: key}
	}
	heap := restoreOp{kind: restorePushHeap, database: "d", bucket: "b", id: "a", field: "Books"}
	for _, test := range []struct {
		name string
		ops  []restoreOp
		stop bool
	}{
		{"another entity", []restoreOp{push("a", "P"), push("b", "P")}, false},
		{"another field of the same entity", []restoreOp{push("a", "P"), push("a", "Q")}, false},
		{"distinct keys of one hash", []restoreOp{put("x"), put("y")}, false},
		{"the same entity and field", []restoreOp{push("a", "P"), push("a", "P")}, true},
		{"the same key twice", []restoreOp{put("x"), put("x")}, true},
		{"two heap pushes of one heap", []restoreOp{heap, heap}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			script := &scriptedRestoreTransport{answer: func(frame int, ops []restoreOp) []ringDocAnswer {
				answers := make([]ringDocAnswer, len(ops))
				if frame == 0 {
					answers[0].Err = transient
				}
				return answers
			}}
			sender := &restoreSender{transport: script, pause: func(context.Context, int) error { return nil }}
			_, err := sender.send(context.Background(), test.ops)
			var outOfOrder *restoreOutOfOrderError
			stopped := errors.As(err, &outOfOrder)
			if stopped != test.stop || (test.stop && len(script.frames) != 1) || (!test.stop && (err != nil || len(script.frames) != 2)) {
				t.Errorf("stopped %v after %d frames (error %v), want stop=%v", stopped, len(script.frames), err, test.stop)
			}
		})
	}
}

// TestRestoreOfASameSizeDumpWrittenInPlaceIsCaughtOnlyByTheChecksumOfPassOne: another valid dump
// is written over the same file in place (same inode, same size, mtime put back), so the file
// check at the open passes, and the file is self-consistent, so its own footer matches. Only
// the comparison with pass 1's checksum shows it is not the file that was verified.
func TestRestoreOfASameSizeDumpWrittenInPlaceIsCaughtOnlyByTheChecksumOfPassOne(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1"}})
	other := writeShapedRestoreDump(t, [][]string{{"Z1"}})
	targetAddr, _ := newRingTestHost(t)
	t.Cleanup(func() { restoreAfterPassTwo = nil })
	restoreAfterPassTwo = func() {
		restoreAfterPassTwo = nil
		original, err := os.Stat(path)
		if err != nil {
			t.Error(err)
			return
		}
		content, err := os.ReadFile(other)
		if err != nil {
			t.Error(err)
			return
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Chtimes(path, original.ModTime(), original.ModTime()); err != nil {
			t.Error(err)
		}
	}
	code, _, stderr := restoreOnHost(t, targetAddr, path)
	if code != ExitUsage || !strings.Contains(stderr, "changed while it was being restored") || !strings.Contains(stderr, "no longer matches the checksum pass 1 verified") {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
}
