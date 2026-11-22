package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/store"
)

// dumpTestBody marshals one entity's scalar body for a stub List answer.
func dumpTestBody(t *testing.T, id, name string) bson.Raw {
	t.Helper()
	body, err := bson.Marshal(bson.D{
		{Key: dumpBucketIDField, Value: id},
		{Key: "Name", Value: name},
	})
	if err != nil {
		t.Fatalf("building the body of %s: %v", id, err)
	}
	return bson.Raw(body)
}

// dumpTestElement marshals one store element with a Message member.
func dumpTestElement(t *testing.T, message string) bson.Raw {
	t.Helper()
	body, err := bson.Marshal(bson.D{{Key: "Message", Value: message}})
	if err != nil {
		t.Fatalf("building the element %q: %v", message, err)
	}
	return bson.Raw(body)
}

// decodeDumpJSON decodes the one JSON object --json prints, and fails when stdout holds
// none or more than one: a script reading stdout must always get exactly one object.
func decodeDumpJSON(t *testing.T, stdout string) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		t.Fatalf("stdout holds no JSON object: %v\nstdout: %s", err, stdout)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("stdout holds more than one JSON object (second decode: %v)\nstdout: %s", err, stdout)
	}
	return object
}

// dumpJSONKeys is the sorted member names of one decoded object.
func dumpJSONKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// dumpJSONCount reads one whole-number member of a decoded object.
func dumpJSONCount(t *testing.T, object map[string]any, member string) int64 {
	t.Helper()
	number, isNumber := object[member].(float64)
	if !isNumber {
		t.Fatalf("the --json object has no number %q: %v", member, object)
	}
	return int64(number)
}

// dumpDoneLineAt is the index of the run's end line in its stderr, or -1.
func dumpDoneLineAt(stderr string) int {
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	for index, line := range lines {
		if strings.HasPrefix(line, "Done: ") {
			return index
		}
	}
	return -1
}

// TestDumpReadsEachEntityOnceAndNeverRechecks pins the read-once rule of section 3.2 item
// 7 (D14): each List page is asked for once, each store chain of each entity is walked
// once, and nothing is asked again about an entity that was already written. The second
// half pins the source interface itself: adding a QueueState, GetFields, Get, StoreCount
// or CountDocuments call to a dump must fail here, because a recheck must be a decision
// and not a helper someone reaches for.
func TestDumpReadsEachEntityOnceAndNeverRechecks(t *testing.T) {
	queuePages := func() []ringclient.ElementPage {
		return dumpStubElementPages(ringclient.ElementPage{Elements: []bson.Raw{
			dumpTestElement(t, "m1"), dumpTestElement(t, "m2"),
		}}, 1)
	}
	friendPages := func() []ringclient.ElementPage {
		return []ringclient.ElementPage{{
			Elements: []bson.Raw{dumpTestElement(t, "ada")},
			Keys:     []string{"ada"},
		}}
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "User", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/User": {Database: "app", Name: "User", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Name", Type: "string"},
				{Name: "Queue", Type: "fifoStore"},
				{Name: "Friends", Type: "hashStore"},
				{Name: "Data", Type: "blobStore"},
			}},
		},
		// Two List pages of one entity each, and two StoreList pages per queue chain: the
		// counts below are only equal to the scripted pages when every page is asked for
		// exactly once.
		documentPages: map[string][]ringclient.EntryPage{
			"app/User": dumpStubDocumentPages([]ringclient.Entry{
				{ID: "u1", Body: dumpTestBody(t, "u1", "one")},
				{ID: "u2", Body: dumpTestBody(t, "u2", "two")},
			}, 1),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/User/u1/Queue":   queuePages(),
			"app/User/u1/Friends": friendPages(),
			"app/User/u2/Queue":   queuePages(),
			"app/User/u2/Friends": friendPages(),
		},
	}
	outPath := filepath.Join(t.TempDir(), "once.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping two entities: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if got := stub.calls.ListDocuments; got != 2 {
		t.Errorf("ListDocuments was called %d times for a bucket of 2 one-entity pages, want 2: an entity is listed once", got)
	}
	wants := map[string]int{
		"app/User/u1/Queue":   2,
		"app/User/u1/Friends": 1,
		"app/User/u2/Queue":   2,
		"app/User/u2/Friends": 1,
	}
	for key, want := range wants {
		if got := stub.calls.storeListCallsFor(key); got != want {
			t.Errorf("StoreList was called %d times for %s, want %d: a chain is read once", got, key, want)
		}
	}
	if got, want := len(stub.calls.storeListFields), 6; got != want {
		t.Errorf("StoreList was called %d times over the dump, want %d (the four chains above and no recheck, no StoreCount, no call for the blobStore field): %v",
			got, want, stub.calls.storeListFields)
	}
	if got := stub.calls.DescribeBucket; got != 1 {
		t.Errorf("DescribeBucket was called %d times for one bucket, want 1", got)
	}
	if got := stub.calls.ShowBuckets; got != 1 {
		t.Errorf("ShowBuckets was called %d times for one bucket, want 1", got)
	}
	if got := stub.calls.DescribeDatabase; got != 1 {
		t.Errorf("DescribeDatabase was called %d times for one database, want 1", got)
	}
	// Once for the pre-dial check and once for the enumeration, one page each.
	if got := stub.calls.ShowDatabases; got != 2 {
		t.Errorf("ShowDatabases was called %d times, want 2 (the check and the enumeration)", got)
	}
	records := readDumpRecords(t, outPath)
	for _, id := range []string{"u1", "u2"} {
		if doc := dumpDocNamed(t, records, "app", "User", id); doc.ID != id {
			t.Errorf("the doc record for %s carries id %q", id, doc.ID)
		}
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Entities != 2 || verification.Elements != 6 {
		t.Errorf("the verification reads %d entities and %d elements, want 2 and 6 (two messages and one friend each)",
			verification.Entities, verification.Elements)
	}

	// The method set of ringDumpSource is exactly the plan's list of calls a dump makes
	// (the ring cli parity build spec, "Architecture decided by the coordinator"). A
	// method added here is a call a dump may make, so a recheck cannot arrive without
	// changing this list on purpose.
	wantMethods := []string{"DescribeBucket", "DescribeDatabase", "ListDocuments", "ShowBuckets", "ShowDatabases", "StoreList"}
	sourceType := reflect.TypeOf((*ringDumpSource)(nil)).Elem()
	gotMethods := make([]string, 0, sourceType.NumMethod())
	for index := 0; index < sourceType.NumMethod(); index++ {
		gotMethods = append(gotMethods, sourceType.Method(index).Name)
	}
	if !reflect.DeepEqual(gotMethods, wantMethods) {
		t.Errorf("ringDumpSource has the methods %v, want %v: a dump reads each entity once, so a new call is a decision (section 3.2 item 7)", gotMethods, wantMethods)
	}
}

// TestDumpUnderWritesExitsZero: writes land while the entity is being read. Its scalars
// change and a message arrives between the List entry the dump wrote and the store chain
// it reads next. The file still verifies, the exit is 0, no line names the entity (a dump
// does not warn per entity, section 3.6), and the end line is printed.
func TestDumpUnderWritesExitsZero(t *testing.T) {
	const entityID = "entity-under-write"
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Jobs", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Jobs": {Database: "app", Name: "Jobs", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Name", Type: "string"},
				{Name: "Queue", Type: "fifoStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/Jobs": {{Entries: []ringclient.Entry{{ID: entityID, Body: dumpTestBody(t, entityID, "before")}}}},
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/Jobs/" + entityID + "/Queue": {{Elements: []bson.Raw{dumpTestElement(t, "m1")}}},
		},
	}
	applied := false
	stub.beforeStoreList = func(database, bucket, id, field string) {
		if applied {
			return
		}
		applied = true
		// The entity's scalars change after its List entry was written to the file, and one
		// more message arrives before the chain is read.
		stub.documentPages["app/Jobs"][0].Entries[0].Body = dumpTestBody(t, entityID, "after")
		key := database + "/" + bucket + "/" + id + "/" + field
		stub.elementPages[key][0].Elements = append(stub.elementPages[key][0].Elements, dumpTestElement(t, "m2"))
	}
	outPath := filepath.Join(t.TempDir(), "underwrite.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping an entity that is written to while it is read: exit %d, want %d (section 3.5)\nstdout: %s\nstderr: %s",
			code, ExitOK, stdout, stderr)
	}
	if stdout != "" {
		t.Errorf("a text run wrote to stdout: %q (the report goes to stderr)", stdout)
	}
	for _, line := range strings.Split(stderr, "\n") {
		if strings.Contains(line, entityID) {
			t.Errorf("the run warns about the entity: %q (a dump does not detect a torn entity and does not warn per entity)", line)
		}
	}
	if doneAt := dumpDoneLineAt(stderr); doneAt < 0 {
		t.Fatalf("the run printed no end line:\nstderr: %s", stderr)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Entities != 1 || verification.Elements != 2 {
		t.Errorf("the verification reads %d entities and %d elements, want 1 and 2 (the appended message was read)", verification.Entities, verification.Elements)
	}
	records := readDumpRecords(t, outPath)
	doc := dumpDocNamed(t, records, "app", "Jobs", entityID)
	name, ok := doc.F.Lookup("Name").StringValueOK()
	if !ok || name != "before" {
		t.Errorf("the doc record holds Name %q, want %q: the scalars are what the List entry said, read once", name, "before")
	}
	chunks := dumpElemsOf(records, "app", "Jobs", entityID, "Queue")
	if got, want := dumpChunkStrings(t, chunks, "Message"), []string{"m1", "m2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Queue holds %v, want %v: the chain was read once, after the append", got, want)
	}
}

// TestDumpSaysQueueMessagesAreNotIncludedWhenItHoldsAQueueField: a dump that holds a
// fifoStore or lifoStore field says once, after its end line, that claimed and
// dead-lettered messages are not in the file, and --json carries the same sentence
// (section 3.1 "What the tool prints", rev 7).
func TestDumpSaysQueueMessagesAreNotIncludedWhenItHoldsAQueueField(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatalf("creating database app on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Jobs", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Photos", Type: "lifoStore"},
	}); err != nil {
		t.Fatalf("creating app.Jobs on the test host: %v", err)
	}
	jobs := client.Bucket("app", "Jobs")
	if _, err := jobs.Insert(ctx, "job-7", ringclient.Doc{"Name": "seven"}); err != nil {
		t.Fatalf("inserting job-7 into app.Jobs: %v", err)
	}
	if _, err := jobs.Push(ctx, "job-7", "Photos", ringclient.Doc{"Title": "beach"}); err != nil {
		t.Fatalf("pushing into job-7's Photos: %v", err)
	}
	outPath := filepath.Join(t.TempDir(), "queue.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--bucket", "Jobs", "--out", outPath, "--json")
	if code != ExitOK {
		t.Fatalf("dumping a bucket with a lifoStore field: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if got := strings.Count(stderr, dumpQueueLeftOutLine); got != 1 {
		t.Fatalf("the queue sentence appears %d times, want once:\nstderr: %s", got, stderr)
	}
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	doneAt := dumpDoneLineAt(stderr)
	if doneAt < 0 {
		t.Fatalf("the run printed no end line:\nstderr: %s", stderr)
	}
	if doneAt+1 >= len(lines) || lines[doneAt+1] != dumpQueueLeftOutLine {
		t.Errorf("the queue sentence does not follow the end line (end line %d of %d):\nstderr: %s", doneAt, len(lines), stderr)
	}
	object := decodeDumpJSON(t, stdout)
	if left, isBool := object["queue_messages_left_out"].(bool); !isBool || !left {
		t.Errorf("the --json object has queue_messages_left_out %v, want true: the dump holds a queue field", object["queue_messages_left_out"])
	}
	if text, isString := object["queue_messages_left_out_text"].(string); !isString || text != dumpQueueLeftOutLine {
		t.Errorf("the --json object carries queue_messages_left_out_text %v, want %q", object["queue_messages_left_out_text"], dumpQueueLeftOutLine)
	}
	if file, isString := object["file"].(string); !isString || file != outPath {
		t.Errorf("the --json object names the file %v, want %q", object["file"], outPath)
	}
}

// TestDumpOfABucketWithoutQueuesDoesNotPrintTheQueueLine: a run of scalars and keyed
// stores leaves nothing queued out, so neither the text nor the --json object carries the
// queue sentence (section 3.1, rev 7).
func TestDumpOfABucketWithoutQueuesDoesNotPrintTheQueueLine(t *testing.T) {
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Post", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Post": {Database: "app", Name: "Post", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Title", Type: "string"},
				{Name: "Tags", Type: "hashStore"},
				{Name: "Scores", Type: "btreeStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/Post": {{Entries: []ringclient.Entry{{ID: "p1", Body: dumpTestBody(t, "p1", "hello")}}}},
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/Post/p1/Tags": {{
				Elements: []bson.Raw{dumpTestElement(t, "go")},
				Keys:     []string{"go"},
			}},
			"app/Post/p1/Scores": {{
				Elements: []bson.Raw{dumpTestElement(t, "nine")},
				Keys:     []string{"nine"},
			}},
		},
	}
	outPath := filepath.Join(t.TempDir(), "noqueue.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath, "--json")
	if code != ExitOK {
		t.Fatalf("dumping a bucket without a queue field: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if strings.Contains(stderr, dumpQueueLeftOutLine) {
		t.Errorf("the run carries the queue sentence for a bucket of keyed stores:\nstderr: %s", stderr)
	}
	object := decodeDumpJSON(t, stdout)
	if _, present := object["queue_messages_left_out"]; present {
		t.Errorf("the --json object has queue_messages_left_out for a bucket of keyed stores: %v", object)
	}
	if _, present := object["queue_messages_left_out_text"]; present {
		t.Errorf("the --json object has queue_messages_left_out_text for a bucket of keyed stores: %v", object)
	}
}

// TestDumpNeverExitsThree: a fifo field whose one message is claimed and released until it
// is dead-lettered leaves the message out of the file. The dump does not know it is there,
// does not count it and does not change its exit: it wrote a whole file, so it exits 0
// (section 3.1, "A complete file is not a complete copy of every queue").
func TestDumpNeverExitsThree(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatalf("creating database app on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Jobs", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Queue", Type: "fifoStore"},
	}); err != nil {
		t.Fatalf("creating app.Jobs on the test host: %v", err)
	}
	jobs := client.Bucket("app", "Jobs")
	if _, err := jobs.Insert(ctx, "job-7", ringclient.Doc{"Name": "seven"}); err != nil {
		t.Fatalf("inserting job-7 into app.Jobs: %v", err)
	}
	if _, err := jobs.Push(ctx, "job-7", "Queue", ringclient.Doc{"Message": "poison"}); err != nil {
		t.Fatalf("pushing the message into job-7's Queue: %v", err)
	}
	// Each return counts as a delivery, and the fifth takes the message to the dead letters
	// instead of back to the queue (store/delivery.go, MaxDeliveries).
	for delivery := 1; delivery <= store.MaxDeliveries; delivery++ {
		lease, err := jobs.Claim(ctx, "job-7", "Queue", 1)
		if err != nil {
			t.Fatalf("claim %d of the message: %v", delivery, err)
		}
		if len(lease.Elements) != 1 {
			t.Fatalf("claim %d returned %d messages, want 1: after %d returns the message must still be in the queue", delivery, len(lease.Elements), delivery-1)
		}
		change, err := jobs.Release(ctx, "job-7", "Queue", lease.ID)
		if err != nil {
			t.Fatalf("release %d of the message: %v", delivery, err)
		}
		if !change.Live {
			t.Fatalf("release %d reports the lease was not live, so it did not count as a return", delivery)
		}
	}
	// The message is dead-lettered: it is in no list reply and no claim hands it out, and
	// there is no read that says how many are waiting.
	after, err := jobs.Claim(ctx, "job-7", "Queue", 1)
	if err != nil {
		t.Fatalf("claiming after the fifth return: %v", err)
	}
	if len(after.Elements) != 0 || after.ID != [16]byte{} {
		t.Fatalf("a claim after the fifth return handed out %d messages with lease %x, want none: the message is dead-lettered", len(after.Elements), after.ID)
	}
	outPath := filepath.Join(t.TempDir(), "dead.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--bucket", "Jobs", "--out", outPath)
	if code == ExitPartial {
		t.Fatalf("dumping a bucket with a dead-lettered message: exit %d, and a dump never says it left something out this way; it wrote a whole file\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if code != ExitOK {
		t.Fatalf("dumping a bucket with a dead-lettered message: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Elements != 0 {
		t.Errorf("the dump holds %d elements, want 0: the dead-lettered message is in no list reply", verification.Elements)
	}
	if chunks := dumpElemsOf(readDumpRecords(t, outPath), "app", "Jobs", "job-7", "Queue"); len(chunks) != 0 {
		t.Errorf("the dump holds %d elems records for Queue, want none", len(chunks))
	}
	if !strings.Contains(stderr, dumpQueueLeftOutLine) {
		t.Errorf("the run does not say that queue messages are left out:\nstderr: %s", stderr)
	}
}

// TestDumpInterruptRemovesThePartialFile: a cancelled context in the middle of a store
// chain aborts the writer, so nothing is left in the output directory, and the run exits 4
// with the sentence the sections 3.1 and 3.5 give for an interrupt.
func TestDumpInterruptRemovesThePartialFile(t *testing.T) {
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Jobs", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Jobs": {Database: "app", Name: "Jobs", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Name", Type: "string"},
				{Name: "Queue", Type: "fifoStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/Jobs": {{Entries: []ringclient.Entry{{ID: "job-1", Body: dumpTestBody(t, "job-1", "one")}}}},
		},
		// Two pages of the chain, so the cancellation lands after the first page was read
		// and before the chain ended.
		elementPages: map[string][]ringclient.ElementPage{
			"app/Jobs/job-1/Queue": dumpStubElementPages(ringclient.ElementPage{Elements: []bson.Raw{
				dumpTestElement(t, "m1"), dumpTestElement(t, "m2"),
			}}, 1),
		},
	}
	interruptCtx, interrupt := context.WithCancel(context.Background())
	defer interrupt()
	stub.beforeStoreList = func(_, _, _, _ string) {
		interrupt()
	}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "interrupted.hoarddump")
	var stdout, stderr bytes.Buffer
	code := runRingDumpWithSource(interruptCtx, []string{"--db", "app", "--out", outPath},
		ringDumpEnv{stdout: &stdout, stderr: &stderr}, stub, "root")

	if code != ExitInterrupted {
		t.Fatalf("an interrupted dump: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitInterrupted, stdout.String(), stderr.String())
	}
	if want := "dump: interrupted. No dump file was written. Run the same command again."; !strings.Contains(stderr.String(), want) {
		t.Errorf("the interrupt does not say %q:\nstderr: %s", want, stderr.String())
	}
	if dumpDoneLineAt(stderr.String()) >= 0 {
		t.Errorf("an interrupted dump printed its end line:\nstderr: %s", stderr.String())
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("listing %s: %v", outDir, err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		t.Errorf("an interrupted dump left %v in %s; the .partial file is removed and no final file is written", names, outDir)
	}
}

// TestDumpFailureLeavesNoFinalFile: a failure in the middle of the run aborts the file,
// whatever kind of error it is: the ring's own refusal keeps its class, and an error the
// ring did not answer is a refusal (section 3.5). Both say the file was not written.
func TestDumpFailureLeavesNoFinalFile(t *testing.T) {
	tests := []struct {
		reason      string
		failure     error
		wantExit    int
		wantReason  string
		wantClosing string
	}{
		{
			reason: "a refusal from the ring",
			failure: &ringclient.Error{Code: cluster.CodePermissionDenied,
				Message: `field "Queue" of "app"."Jobs" is not one this user may read`},
			wantExit:    cluster.CodePermissionDenied.Exit(),
			wantReason:  `field "Queue" of "app"."Jobs" is not one this user may read`,
			wantClosing: dumpRefusedClosing,
		},
		{
			reason:      "a lost connection, outcome unknown",
			failure:     io.ErrUnexpectedEOF,
			wantExit:    ExitUnavailable,
			wantReason:  "unexpected EOF",
			wantClosing: dumpUnavailableClosing,
		},
		{
			reason:      "an error the ring did not answer",
			failure:     errors.New("the disk filled while writing the elements"),
			wantExit:    ExitRefused,
			wantReason:  "the disk filled while writing the elements",
			wantClosing: dumpRefusedClosing,
		},
	}
	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			stub := &dumpStub{
				databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
				bucketPages: map[string][]ringclient.BucketPage{
					"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Jobs", State: "active"}}, 1),
				},
				descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
				declarations: map[string]ringclient.BucketInfo{
					"app/Jobs": {Database: "app", Name: "Jobs", State: "active", Fields: []ringclient.FieldInfo{
						{Name: dumpBucketIDField, Type: "string"},
						{Name: "Name", Type: "string"},
						{Name: "Queue", Type: "fifoStore"},
					}},
				},
				documentPages: map[string][]ringclient.EntryPage{
					"app/Jobs": {{Entries: []ringclient.Entry{{ID: "job-1", Body: dumpTestBody(t, "job-1", "one")}}}},
				},
				failures: map[string]error{"StoreList app/Jobs/job-1/Queue": test.failure},
			}
			outDir := t.TempDir()
			outPath := filepath.Join(outDir, "failed.hoarddump")

			code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
			if code != test.wantExit {
				t.Fatalf("%s: exit %d, want %d\nstdout: %s\nstderr: %s", test.reason, code, test.wantExit, stdout, stderr)
			}
			reason := test.wantReason + ". " + test.wantClosing
			if !strings.Contains(stderr, reason) {
				t.Errorf("%s: the failure does not say %q:\nstderr: %s", test.reason, reason, stderr)
			}
			if test.wantClosing == dumpRefusedClosing && strings.Contains(stderr, "again") {
				t.Errorf("%s: a refusal repeats identically, so it must not advise running again:\nstderr: %s", test.reason, stderr)
			}
			entries, err := os.ReadDir(outDir)
			if err != nil {
				t.Fatalf("listing %s: %v", outDir, err)
			}
			if len(entries) != 0 {
				names := make([]string, len(entries))
				for index, entry := range entries {
					names[index] = entry.Name()
				}
				t.Errorf("%s: the failed dump left %v in %s; it writes the final name only when the file is whole", test.reason, names, outDir)
			}
		})
	}
}

// TestDumpJSONObjectIsOneObject: --json prints exactly one object on stdout, with the
// final path, the four counters and the consistency word and statement, and nothing else
// when no queue field was dumped. A failed run prints one object too, so a script that
// reads stdout always gets an answer and never an empty line (section 3.1).
func TestDumpJSONObjectIsOneObject(t *testing.T) {
	newStub := func() *dumpStub {
		return &dumpStub{
			databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
			bucketPages: map[string][]ringclient.BucketPage{
				"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Post", State: "active"}}, 1),
			},
			descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
			declarations: map[string]ringclient.BucketInfo{
				"app/Post": {Database: "app", Name: "Post", State: "active", Fields: []ringclient.FieldInfo{
					{Name: dumpBucketIDField, Type: "string"},
					{Name: "Title", Type: "string"},
					{Name: "Tags", Type: "hashStore"},
				}},
			},
			documentPages: map[string][]ringclient.EntryPage{
				"app/Post": {{Entries: []ringclient.Entry{
					{ID: "p1", Body: dumpTestBody(t, "p1", "hello")},
					{ID: "p2", Body: dumpTestBody(t, "p2", "world")},
				}}},
			},
			elementPages: map[string][]ringclient.ElementPage{
				"app/Post/p1/Tags": {{
					Elements: []bson.Raw{dumpTestElement(t, "go"), dumpTestElement(t, "db")},
					Keys:     []string{"go", "db"},
				}},
			},
		}
	}
	outPath := filepath.Join(t.TempDir(), "json.hoarddump")

	code, stdout, stderr := runStubDump(t, newStub(), "--db", "app", "--out", outPath, "--json")
	if code != ExitOK {
		t.Fatalf("dumping with --json: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	object := decodeDumpJSON(t, stdout)
	wantKeys := []string{"buckets", "consistency", "databases", "elements", "entities", "file"}
	if got := dumpJSONKeys(object); !reflect.DeepEqual(got, wantKeys) {
		t.Errorf("the --json object has the members %v, want %v", got, wantKeys)
	}
	if file, isString := object["file"].(string); !isString || file != outPath {
		t.Errorf("the --json object names the file %v, want %q", object["file"], outPath)
	}
	for member, want := range map[string]int64{"databases": 1, "buckets": 1, "entities": 2, "elements": 2} {
		if got := dumpJSONCount(t, object, member); got != want {
			t.Errorf("the --json object counts %s %d, want %d", member, got, want)
		}
	}
	consistency, isObject := object["consistency"].(map[string]any)
	if !isObject {
		t.Fatalf("the --json object has consistency %v, want an object with the word and the statement", object["consistency"])
	}
	if word, isString := consistency["word"].(string); !isString || word != dumpConsistencyWord {
		t.Errorf("the --json consistency word is %v, want %q", consistency["word"], dumpConsistencyWord)
	}
	if statement, isString := consistency["statement"].(string); !isString || statement != dumpStartStatement {
		t.Errorf("the --json consistency statement is %v, want the dump start statement", consistency["statement"])
	}

	// The same run without --json prints nothing to stdout: the report goes to stderr.
	textPath := filepath.Join(t.TempDir(), "text.hoarddump")
	code, stdout, stderr = runStubDump(t, newStub(), "--db", "app", "--out", textPath)
	if code != ExitOK {
		t.Fatalf("dumping without --json: exit %d, want %d\nstderr: %s", code, ExitOK, stderr)
	}
	if stdout != "" {
		t.Errorf("a run without --json wrote to stdout: %q", stdout)
	}

	t.Run("a failed run still prints one object", func(t *testing.T) {
		failing := newStub()
		failing.failures = map[string]error{"ListDocuments app/Post": &ringclient.Error{
			Code: cluster.CodePermissionDenied, Message: `the user may not read "app"."Post"`}}
		failedPath := filepath.Join(t.TempDir(), "failed.hoarddump")

		code, stdout, stderr := runStubDump(t, failing, "--db", "app", "--out", failedPath, "--json")
		wantExit := cluster.CodePermissionDenied.Exit()
		if code != wantExit {
			t.Fatalf("a failed --json run: exit %d, want %d\nstdout: %s\nstderr: %s", code, wantExit, stdout, stderr)
		}
		object := decodeDumpJSON(t, stdout)
		wantKeys := []string{"error", "exit", "file"}
		if got := dumpJSONKeys(object); !reflect.DeepEqual(got, wantKeys) {
			t.Errorf("the failure object has the members %v, want %v", got, wantKeys)
		}
		if file, isString := object["file"].(string); !isString || file != "" {
			t.Errorf("the failure object names the file %v, want the empty string: nothing was written", object["file"])
		}
		if got := dumpJSONCount(t, object, "exit"); got != int64(wantExit) {
			t.Errorf("the failure object says exit %d, want %d", got, wantExit)
		}
		text, isString := object["error"].(string)
		if !isString || !strings.Contains(text, `the user may not read "app"."Post"`) {
			t.Errorf("the failure object carries error %v, want the ring's reason", object["error"])
		}
	})
}

// The closing sentences by failure class. A refusal repeats identically, so it gives no
// advice to run again (its text already names the fix); a lost connection leaves the
// outcome unknown, and the sentence says no complete file exists and a new run is safe.
const (
	dumpRefusedClosing     = "No dump file was written."
	dumpUnavailableClosing = "No complete dump file was written. You can start the dump again."
)

// TestDumpHelpNamesTheRestoreCommands: the help says how a file is checked and how it is
// put back, with the two commands that exist.
func TestDumpHelpNamesTheRestoreCommands(t *testing.T) {
	_, stdout, _ := runRingDumpForTest(t, "--help")
	for _, want := range []string{"hoarddb restore --verify <file>", "Restore a dump into an empty ring with 'hoarddb restore <file>'."} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the help does not say %q:\n%s", want, stdout)
		}
	}
}

// TestDumpToAMissingDirectoryNamesTheFix: an --out in a directory that does not exist is a
// usage error that names the directory and the two fixes, and prints no .partial path.
func TestDumpToAMissingDirectoryNamesTheFix(t *testing.T) {
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
	}
	missing := filepath.Join(t.TempDir(), "no-such-dir")
	out := filepath.Join(missing, "app.hoarddump")
	code, _, stderr := runStubDump(t, stub, "--db", "app", "--out", out)
	if code != ExitUsage {
		t.Fatalf("exit %d, want %d\nstderr: %s", code, ExitUsage, stderr)
	}
	want := "The directory " + missing + " does not exist. Create it (mkdir -p " + missing + ") or pick another --out."
	if !strings.Contains(stderr, want) {
		t.Errorf("stderr does not say %q:\n%s", want, stderr)
	}
	if strings.Contains(stderr, ".partial") {
		t.Errorf("stderr names the .partial file, which the operator never asked for:\n%s", stderr)
	}
}

// TestDumpOfAnExpiredButUnsweptElement is a characterization test (plan 2.1): it records
// what the dump does with a queue element that is past its expire time and that the
// background sweep has not removed. The owner's read leaves such an element out of a list
// reply (cluster/wire/fieldread.go:Reader.ListField), so the file holds the two live
// elements, the expired one is absent, and the summary counts two. The test does not depend
// on the sweep having run or not: an element the sweep already removed is absent the same way.
func TestDumpOfAnExpiredButUnsweptElement(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatalf("creating database app on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Device", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Log", Type: "fifoStore", ExpirePath: "ts"},
	}); err != nil {
		t.Fatalf("creating app.Device with an expiring Log on the test host: %v", err)
	}
	devices := client.Bucket("app", "Device")
	if _, err := devices.Insert(ctx, "dev-1", ringclient.Doc{"Name": "one"}); err != nil {
		t.Fatalf("inserting dev-1 into app.Device: %v", err)
	}
	for _, reading := range []struct {
		label   string
		expires time.Time
	}{
		{"first", ringclient.ExpiresIn(time.Hour)},
		{"expired", time.Now().Add(-time.Minute)},
		{"third", ringclient.ExpiresIn(time.Hour)},
	} {
		if _, err := devices.Push(ctx, "dev-1", "Log", ringclient.Doc{"label": reading.label, "ts": reading.expires}); err != nil {
			t.Fatalf("pushing the %s reading into dev-1's Log: %v", reading.label, err)
		}
	}
	outPath := filepath.Join(t.TempDir(), "expired.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--bucket", "Device", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping a bucket with an expired element: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Elements != 2 {
		t.Errorf("the summary counts %d elements, want 2: the expired element is not listed, so it is not counted", verification.Elements)
	}
	held := 0
	for _, chunk := range dumpElemsOf(readDumpRecords(t, outPath), "app", "Device", "dev-1", "Log") {
		for _, element := range chunk.Els {
			held++
			if strings.Contains(element.String(), "expired") {
				t.Errorf("the file holds the expired element %s", element)
			}
		}
	}
	if held != 2 {
		t.Errorf("the file holds %d elements for Log, want 2 (first and third)", held)
	}
}

// TestDumpHelpMentionsParallelAndItsMemory pins that the help lists --parallel with its
// range, says what it costs in memory, and does not promise a drift warning the dump no
// longer prints, so the help, the page and the changelog describe the same dump.
func TestDumpHelpMentionsParallelAndItsMemory(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runRingDump(context.Background(), []string{"--help"}, ringDumpEnv{stdout: &out, stderr: &errOut})
	if code != ExitOK {
		t.Fatalf("dump --help: exit %d, want %d\nstderr: %s", code, ExitOK, errOut.String())
	}
	for _, want := range []string{"-parallel", "1 to 64", "nobody writes to", "170 MiB", "not linear", "not measured"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("dump --help does not contain %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "document count changed") {
		t.Errorf("dump --help promises a warning about document counts, which a dump does not print:\n%s", out.String())
	}
}
