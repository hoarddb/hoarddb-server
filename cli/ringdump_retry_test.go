package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// dumpRetryStub scripts two one-entity document pages and a queue of two pages, so a
// refused page has a page after it and the second page of a chain carries a cursor that is
// not empty.
func dumpRetryStub(t *testing.T) *dumpStub {
	t.Helper()
	return &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Jobs", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Jobs": {Database: "app", Name: "Jobs", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Name", Type: "string"},
				{Name: "Queue", Type: dumpFifoStoreWord},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/Jobs": dumpStubDocumentPages([]ringclient.Entry{
				{ID: "job-1", Body: dumpTestBody(t, "job-1", "one")},
				{ID: "job-2", Body: dumpTestBody(t, "job-2", "two")},
			}, 1),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/Jobs/job-1/Queue": dumpStubElementPages(ringclient.ElementPage{Elements: []bson.Raw{
				dumpTestElement(t, "m1"), dumpTestElement(t, "m2"),
			}}, 1),
		},
	}
}

// dumpRetryOwnerDown and dumpRetryStateMoved are the two refusals a moving slot answers
// (section 3.5): the ring says the slot moved, or that its owner is down.
func dumpRetryOwnerDown() error {
	return &ringclient.Error{Code: cluster.CodeOwnerDown,
		Message: "the node that owns this slot is down. Run 'nodes' to see the members, then run the command again."}
}

func dumpRetryStateMoved() error {
	return &ringclient.Error{Code: cluster.CodeStateMoved,
		Message: "this slot is moving to another node. Run 'nodes' to see the members, then run the command again."}
}

// dumpShortRetryWait sets ringDumpPageRetryWait to a short interval for one test and
// restores it afterwards, so a retry test does not spend whole seconds waiting.
func dumpShortRetryWait(t *testing.T) {
	t.Helper()
	previous := ringDumpPageRetryWait
	ringDumpPageRetryWait = time.Millisecond
	t.Cleanup(func() { ringDumpPageRetryWait = previous })
}

// dumpRetryCursorsFor is the cursor of every StoreList call the stub logged for one
// field, in order: a parallel dump interleaves the fields of several entities, so a test
// cannot index the whole log.
func dumpRetryCursorsFor(calls dumpStubCalls, key string) [][]byte {
	cursors := make([][]byte, 0, len(calls.storeListFields))
	for index, asked := range calls.storeListFields {
		if asked == key {
			cursors = append(cursors, calls.storeListAfters[index])
		}
	}
	return cursors
}

// TestDumpRetriesAPageThatAnswersOwnerDown: the first two attempts at the bucket's first
// List page are refused, and the second StoreList page of a field refuses once. Every
// attempt that mattered was asked with the same cursor, the dump exits 0, and the records
// after the header are those of the same stub without failures, for --parallel 1 and
// --parallel 4: a worker retries its page the same way the serial path does (section 3.5,
// section 3.2 item 7: a retry of a call that returned nothing, never a re-read of an
// entity).
func TestDumpRetriesAPageThatAnswersOwnerDown(t *testing.T) {
	referenceDir := t.TempDir()
	referencePath := filepath.Join(referenceDir, "reference.hoarddump")
	if code, stdout, stderr := runStubDump(t, dumpRetryStub(t), "--db", "app", "--parallel", "1", "--out", referencePath); code != ExitOK {
		t.Fatalf("dumping the same fixture without failures: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	reference := readDumpRecords(t, referencePath)

	for _, parallel := range []string{"1", "4"} {
		t.Run("parallel "+parallel, func(t *testing.T) {
			dumpShortRetryWait(t)
			stub := dumpRetryStub(t)
			stub.failuresFirst = map[string]failFirst{
				"ListDocuments app/Jobs":         {count: 2, err: dumpRetryOwnerDown()},
				"StoreList app/Jobs/job-1/Queue": {skip: 1, count: 1, err: dumpRetryStateMoved()},
			}
			outDir := t.TempDir()
			outPath := filepath.Join(outDir, "retried.hoarddump")

			code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", parallel, "--out", outPath)
			if code != ExitOK {
				t.Fatalf("dumping a page that is refused twice and then answered: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
			}
			if stdout != "" {
				t.Errorf("the run wrote to stdout: %q (the report goes to stderr)", stdout)
			}

			calls := stub.callCounts()
			// Two refusals and the two document pages: the page was asked for again, not
			// skipped and not counted as an entity.
			if want := 4; calls.ListDocuments != want {
				t.Errorf("ListDocuments was called %d times, want %d: two refusals of the first page and the two pages themselves", calls.ListDocuments, want)
			}
			if got, want := len(calls.documentAfters), 4; got != want {
				t.Fatalf("the stub logged %d ListDocuments cursors, want %d: %v", got, want, calls.documentAfters)
			}
			// The two refusals and the call that answered the first page were given the same
			// cursor: a refused page is asked for again, not re-fetched from somewhere else.
			for attempt := 1; attempt <= 2; attempt++ {
				if !bytes.Equal(calls.documentAfters[0], calls.documentAfters[attempt]) {
					t.Errorf("the first document page was retried with cursor %v after it was refused with cursor %v; a retry passes the same cursor",
						calls.documentAfters[attempt], calls.documentAfters[0])
				}
			}
			if want := 3; calls.storeListCallsFor("app/Jobs/job-1/Queue") != want {
				t.Errorf("StoreList was called %d times for the two-page queue, want %d: the second page was refused once and asked for again",
					calls.storeListCallsFor("app/Jobs/job-1/Queue"), want)
			}
			// The store calls of one queue come in chain order inside an entity: its first page
			// (no cursor), the refused second page, and the retried second page.
			queueCursors := dumpRetryCursorsFor(calls, "app/Jobs/job-1/Queue")
			if got, want := len(queueCursors), 3; got != want {
				t.Fatalf("the stub logged %d cursors for the two-page queue, want %d: %v", got, want, queueCursors)
			}
			if !bytes.Equal(queueCursors[1], queueCursors[2]) {
				t.Errorf("the second queue page was retried with cursor %v after it was refused with cursor %v; a retry passes the same cursor",
					queueCursors[2], queueCursors[1])
			}
			if len(queueCursors[1]) == 0 {
				t.Errorf("the refused second queue page was asked for with an empty cursor, so the fixture no longer pins that a retry repeats a cursor")
			}

			retried := readDumpRecords(t, outPath)
			if len(retried) != len(reference) {
				t.Fatalf("the retried dump holds %d records and the same fixture without failures %d, want the same", len(retried), len(reference))
			}
			if len(retried) < 3 {
				t.Fatalf("the dump holds %d records, want at least a header, a summary and a footer", len(retried))
			}
			// The header's created time differs, and the footer's checksum covers the header,
			// so the records between them are what two dumps of one quiet ring share.
			if !reflect.DeepEqual(reference[1:len(reference)-1], retried[1:len(retried)-1]) {
				for index := 1; index < len(reference)-1; index++ {
					if !reflect.DeepEqual(reference[index], retried[index]) {
						t.Errorf("record %d differs from the dump without failures:\nretried:   %+v\nreference: %+v", index+1, retried[index], reference[index])
						break
					}
				}
			}
			retriedFooter, isFooter := retried[len(retried)-1].(*dumpFooter)
			referenceFooter, referenceIsFooter := reference[len(reference)-1].(*dumpFooter)
			if !isFooter || !referenceIsFooter {
				t.Fatalf("the last records of the dumps are %T and %T, want the footer", retried[len(retried)-1], reference[len(reference)-1])
			}
			if retriedFooter.Bytes != referenceFooter.Bytes {
				t.Errorf("the retried dump covers %d bytes and the dump without failures %d, want the same", retriedFooter.Bytes, referenceFooter.Bytes)
			}
		})
	}
}

// TestDumpStopsAfterFiveRefusedAttemptsOfAPage: a page the ring keeps refusing is asked
// for exactly five times and then ends the run as a failure of its own class, writing
// neither the final name nor a .partial file, and the line names the fix (section 3.5).
func TestDumpStopsAfterFiveRefusedAttemptsOfAPage(t *testing.T) {
	dumpShortRetryWait(t)
	stub := dumpRetryStub(t)
	stub.failures = map[string]error{"ListDocuments app/Jobs": dumpRetryOwnerDown()}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "refused.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitUnavailable {
		t.Fatalf("dumping a page that is always refused: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitUnavailable, stdout, stderr)
	}
	if got := stub.callCounts().ListDocuments; got != 5 {
		t.Errorf("ListDocuments was called %d times, want 5: the plan's five tries are the first call and four retries, and the fifth refusal ends the run", got)
	}
	if _, err := os.Stat(outPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused dump left %s: a failed run writes no final file (stat error: %v)", outPath, err)
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
		t.Errorf("the refused dump left %v in %s; it writes the final name only when the file is whole", names, outDir)
	}
	for _, want := range []string{
		"the documents of app.Jobs",
		"still answered OWNER_DOWN",
		"after 5 tries",
		"run 'nodes' to see the members",
		"No complete dump file was written.",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not say %q:\nstderr: %s", want, stderr)
		}
	}
}

// TestDumpDoesNotRetryOtherRefusals: a permission refusal, a NOT_OWNER and an error the
// ring did not answer each end the run on the first attempt with their own exit class. A
// retry is only for a page the ring refused because a slot moved or its owner is down.
func TestDumpDoesNotRetryOtherRefusals(t *testing.T) {
	tests := []struct {
		reason   string
		failure  error
		wantExit int
		wantText string
	}{
		{
			reason: "a permission refusal",
			failure: &ringclient.Error{Code: cluster.CodePermissionDenied,
				Message: `field "Queue" of "app"."Jobs" is not one this user may read`},
			wantExit: cluster.CodePermissionDenied.Exit(),
			wantText: `field "Queue" of "app"."Jobs" is not one this user may read`,
		},
		{
			reason:   "NOT_OWNER",
			failure:  &ringclient.Error{Code: cluster.CodeNotOwner, Message: "this node does not own the slot. Run the command again against the owner."},
			wantExit: cluster.CodeNotOwner.Exit(),
			wantText: "this node does not own the slot",
		},
		{
			reason:   "an error the ring did not answer",
			failure:  errors.New("the connection closed while the page was read"),
			wantExit: ExitRefused,
			wantText: "the connection closed while the page was read",
		},
	}
	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			dumpShortRetryWait(t)
			stub := dumpRetryStub(t)
			stub.failures = map[string]error{"ListDocuments app/Jobs": test.failure}
			outDir := t.TempDir()
			outPath := filepath.Join(outDir, "refused.hoarddump")

			code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
			if code != test.wantExit {
				t.Fatalf("%s: exit %d, want %d\nstdout: %s\nstderr: %s", test.reason, code, test.wantExit, stdout, stderr)
			}
			if got := stub.callCounts().ListDocuments; got != 1 {
				t.Errorf("%s: ListDocuments was called %d times, want 1: only a STATE_MOVED or OWNER_DOWN page is retried", test.reason, got)
			}
			if !strings.Contains(stderr, test.wantText) {
				t.Errorf("%s: the failure does not say %q:\nstderr: %s", test.reason, test.wantText, stderr)
			}
			if entries, err := os.ReadDir(outDir); err != nil {
				t.Fatalf("%s: listing %s: %v", test.reason, outDir, err)
			} else if len(entries) != 0 {
				t.Errorf("%s: the failed dump left %d files in %s", test.reason, len(entries), outDir)
			}
		})
	}
}

// TestDumpRetryWaitStopsOnInterrupt: the wait between two attempts selects on the run's
// context, so Ctrl-C during it ends the run at once with exit 4 instead of sleeping out the
// interval. The interval is a minute here, so a wait that ignores the context fails the
// test's own deadline instead of passing slowly.
func TestDumpRetryWaitStopsOnInterrupt(t *testing.T) {
	previous := ringDumpPageRetryWait
	ringDumpPageRetryWait = time.Minute
	t.Cleanup(func() { ringDumpPageRetryWait = previous })
	stub := dumpRetryStub(t)
	stub.failures = map[string]error{"ListDocuments app/Jobs": dumpRetryOwnerDown()}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "interrupted.hoarddump")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runRingDumpWithSource(ctx, []string{"--db", "app", "--out", outPath},
			ringDumpEnv{stdout: &stdout, stderr: &stderr}, stub, "root")
	}()

	// The refused call has returned; the dump is now in the wait before its second attempt.
	if !dumpWaitFor(t, "the first attempt at the refused page", 10*time.Second, func() bool {
		return stub.callCounts().ListDocuments >= 1
	}) {
		t.Fatalf("the dump made %d attempts at the refused page in 10s, want at least 1", stub.callCounts().ListDocuments)
	}
	cancel()

	select {
	case code := <-done:
		if code != ExitInterrupted {
			t.Fatalf("cancelling the run during the retry wait: exit %d, want %d\nstdout: %s\nstderr: %s",
				code, ExitInterrupted, stdout.String(), stderr.String())
		}
		if want := "dump: interrupted. No dump file was written. Run the same command again."; !strings.Contains(stderr.String(), want) {
			t.Errorf("the interrupt does not say %q (the retry wait must return the context's error):\nstderr: %s", want, stderr.String())
		}
		if got := stub.callCounts().ListDocuments; got != 1 {
			t.Errorf("the dump made %d attempts at the refused page after the run was cancelled, want 1: a cancelled run stops before its next attempt", got)
		}
		if entries, err := os.ReadDir(outDir); err != nil {
			t.Fatalf("listing %s: %v", outDir, err)
		} else if len(entries) != 0 {
			t.Errorf("the interrupted dump left %d files in %s", len(entries), outDir)
		}
	case <-time.After(10 * time.Second):
		// Do not read the run's buffers here: it may still be writing them.
		t.Fatalf("cancelling the run did not end the retry wait within 10s; the wait polls ringDumpPageRetryWait (%s) without selecting on the context", ringDumpPageRetryWait)
	}
}

// dumpRetryQuorumRefusal is a refusal a member answers while it has not seen a majority for
// three heartbeats, or has not renewed its lease: the stall of a moment, not a membership
// change.
func dumpRetryQuorumRefusal(code cluster.Code) error {
	if code == cluster.CodeLeaseLapsed {
		return &ringclient.Error{Code: code, Message: "node node-a stopped serving reads and writes 90 ms ago because it could not renew its lease. Run 'nodes', then retry."}
	}
	return &ringclient.Error{Code: code, Message: "this node cannot see a majority of the cluster's 3 voters, so it refuses reads and writes. Run 'nodes' to see which voters are down, and retry when a majority answers"}
}

// dumpShortQuorumRetry shortens the quorum retry's waits and bound for one test.
func dumpShortQuorumRetry(t *testing.T, bound time.Duration) {
	t.Helper()
	base, ceiling, previousBound := ringDumpQuorumBackoffBase, ringDumpQuorumBackoffMax, ringDumpQuorumBound
	ringDumpQuorumBackoffBase, ringDumpQuorumBackoffMax, ringDumpQuorumBound = time.Millisecond, 4*time.Millisecond, bound
	t.Cleanup(func() {
		ringDumpQuorumBackoffBase, ringDumpQuorumBackoffMax, ringDumpQuorumBound = base, ceiling, previousBound
	})
}

// TestDumpWaitsOutABriefLossOfQuorum: a page refused NO_QUORUM or LEASE_LAPSED twice and then
// answered is asked for again with the same cursor, the dump exits 0, and its records are the
// same as a dump that was never refused. Each entity is still read once.
func TestDumpWaitsOutABriefLossOfQuorum(t *testing.T) {
	referencePath := filepath.Join(t.TempDir(), "reference.hoarddump")
	if code, stdout, stderr := runStubDump(t, dumpRetryStub(t), "--db", "app", "--parallel", "1", "--out", referencePath); code != ExitOK {
		t.Fatalf("dumping the fixture without failures: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	reference := readDumpRecords(t, referencePath)

	for _, refused := range []cluster.Code{cluster.CodeNoQuorum, cluster.CodeLeaseLapsed} {
		t.Run(refused.String(), func(t *testing.T) {
			dumpShortQuorumRetry(t, 10*time.Second)
			stub := dumpRetryStub(t)
			stub.failuresFirst = map[string]failFirst{
				"ListDocuments app/Jobs":         {count: 2, err: dumpRetryQuorumRefusal(refused)},
				"StoreList app/Jobs/job-1/Queue": {skip: 1, count: 2, err: dumpRetryQuorumRefusal(refused)},
			}
			outPath := filepath.Join(t.TempDir(), "retried.hoarddump")
			code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", "1", "--out", outPath)
			if code != ExitOK {
				t.Fatalf("a page refused %s twice and then answered: exit %d, want %d\nstdout: %s\nstderr: %s", refused, code, ExitOK, stdout, stderr)
			}
			calls := stub.callCounts()
			if want := 4; calls.ListDocuments != want {
				t.Errorf("ListDocuments was called %d times, want %d: two refusals and the two pages", calls.ListDocuments, want)
			}
			if want := 4; calls.storeListCallsFor("app/Jobs/job-1/Queue") != want {
				t.Errorf("StoreList was called %d times for the queue, want %d: two pages and two refusals of the second", calls.storeListCallsFor("app/Jobs/job-1/Queue"), want)
			}
			// The header's created time and the footer's checksum differ; the records between
			// them are what two dumps of one quiet ring share.
			got := readDumpRecords(t, outPath)
			if len(got) != len(reference) || !reflect.DeepEqual(got[1:len(got)-1], reference[1:len(reference)-1]) {
				t.Errorf("the retried dump's records differ from the dump that was never refused")
			}
		})
	}
}

// TestDumpStopsWhenQuorumStaysLostPastTheBound: a member that answers NO_QUORUM on every try
// ends the dump after the bound with exit 8, no file, and the refusal's own text, which names
// the fix.
func TestDumpStopsWhenQuorumStaysLostPastTheBound(t *testing.T) {
	dumpShortQuorumRetry(t, 50*time.Millisecond)
	stub := dumpRetryStub(t)
	stub.failures = map[string]error{"ListDocuments app/Jobs": dumpRetryQuorumRefusal(cluster.CodeNoQuorum)}
	outDir := t.TempDir()
	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", "1", "--out", filepath.Join(outDir, "never.hoarddump"))
	if code != cluster.CodeNoQuorum.Exit() {
		t.Fatalf("a member that never regains its majority: exit %d, want %d\nstdout: %s\nstderr: %s", code, cluster.CodeNoQuorum.Exit(), stdout, stderr)
	}
	if want := "this node cannot see a majority of the cluster's 3 voters"; !strings.Contains(stderr, want) {
		t.Errorf("the refusal does not carry its own text %q:\n%s", want, stderr)
	}
	if entries, err := os.ReadDir(outDir); err != nil || len(entries) != 0 {
		t.Errorf("the refused dump left files in %s: %v, %v", outDir, entries, err)
	}
}

// TestDumpWaitsOutALossOfQuorumInItsListings: the listings and declarations a dump reads before
// any page are asked again like a page is, so a stall there does not end the run.
func TestDumpWaitsOutALossOfQuorumInItsListings(t *testing.T) {
	dumpShortQuorumRetry(t, 10*time.Second)
	stub := dumpRetryStub(t)
	refusal := dumpRetryQuorumRefusal(cluster.CodeNoQuorum)
	stub.failuresFirst = map[string]failFirst{
		"ShowDatabases":           {count: 2, err: refusal},
		"DescribeDatabase app":    {count: 2, err: refusal},
		"ShowBuckets app":         {count: 2, err: refusal},
		"DescribeBucket app/Jobs": {count: 2, err: refusal},
	}
	outPath := filepath.Join(t.TempDir(), "listed.hoarddump")
	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", "1", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("listings refused NO_QUORUM twice and then answered: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
}
