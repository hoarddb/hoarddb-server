package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// stubRestoreTarget answers the two reads of pass 2 from fixed data and counts how often it
// was asked, so a test can say what the target looks like without holding a bucket in the
// dropping state, which a live ring does not allow.
type stubRestoreTarget struct {
	exists  bool
	rf      int32
	storage string
	buckets []ringclient.BucketSummary
	calls   int
	// describeErr is the answer of DescribeBucket; nil says the bucket exists.
	describeErr error
}

func (s *stubRestoreTarget) DescribeDatabase(_ context.Context, database string, _ ringclient.PageRequest) (ringclient.DatabaseDescription, error) {
	s.calls++
	if !s.exists {
		return ringclient.DatabaseDescription{}, &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "no such database"}
	}
	return ringclient.DatabaseDescription{Name: database, RF: s.rf, Storage: s.storage}, nil
}

// describeErr is what DescribeBucket answers: nil means the bucket exists.
func (s *stubRestoreTarget) DescribeBucket(context.Context, string, string) (ringclient.BucketInfo, error) {
	s.calls++
	return ringclient.BucketInfo{}, s.describeErr
}

func (s *stubRestoreTarget) ShowBuckets(_ context.Context, _ string, page ringclient.PageRequest) (ringclient.BucketPage, error) {
	s.calls++
	return ringclient.BucketPage{Buckets: s.buckets}, nil
}

// stubBuckets returns count summaries named B1.., all in the given state.
func stubBuckets(count int, state string) []ringclient.BucketSummary {
	buckets := make([]ringclient.BucketSummary, count)
	for index := range buckets {
		buckets[index] = ringclient.BucketSummary{Name: fmt.Sprintf("B%d", index+1), ID: uint64(index + 1), State: state}
	}
	return buckets
}

// checkPassTwo runs pass 2 over the small test dump (database app, rf 1, disk) with flags.
func checkPassTwo(t *testing.T, flags *ringRestoreFlags, source restoreTargetSource) error {
	t.Helper()
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	flags.file = path
	return checkRestoreTargets(context.Background(), flags, source, path)
}

// refusalText returns the message of a pass-2 refusal and fails the test when err is not one.
func restoreRefusalText(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("pass 2 accepted the target, want a refusal")
	}
	failed := ringFailure(err)
	if failed.Exit != ExitRefused {
		t.Fatalf("refusal exit %d, want %d: %s", failed.Exit, ExitRefused, failed.Message)
	}
	return failed.Message
}

// TestRestoreRefusesANonEmptyTargetAndNamesTheFix runs a live restore of a real dump into
// the ring it came from: the database holds buckets, so nothing is written and the text
// names the database, the bucket count, the --as command and the drop commands.
func TestRestoreRefusesANonEmptyTargetAndNamesTheFix(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Post", "User"} {
		if _, err := client.CreateBucket(ctx, "app", name, []ringclient.FieldDecl{{Name: "Name", Type: "string"}}); err != nil {
			t.Fatal(err)
		}
	}
	dumpPath := t.TempDir() + "/app.hoarddump"
	if code, _, stderr := dumpOnHost(t, addr, "--out", dumpPath); code != ExitOK {
		t.Fatalf("dump exited %d: %s", code, stderr)
	}

	code, _, stderr := restoreOnHost(t, addr, dumpPath)
	if code != ExitRefused {
		t.Fatalf("restore into a non-empty target exited %d, want %d\n%s", code, ExitRefused, stderr)
	}
	for _, want := range []string{
		`database "app" already holds 2 buckets (Post, User)`,
		"Nothing was written.",
		"--db app --as app_restored",
		"drop bucket Post in app",
		"drop bucket User in app",
		"show buckets in app",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not contain %q:\n%s", want, stderr)
		}
	}
	if strings.Contains(stderr, "drop database app") {
		t.Errorf("the refusal offers drop database as a command, which does not exist in ring mode:\n%s", stderr)
	}
	page, err := client.ShowBuckets(ctx, "app", ringclient.PageRequest{})
	if err != nil || len(page.Buckets) != 2 {
		t.Fatalf("the target changed after a refused restore: %d buckets, error %v", len(page.Buckets), err)
	}
}

func TestRestoreRefusesADroppingBucketAndSaysWait(t *testing.T) {
	target := &stubRestoreTarget{exists: true, rf: 1, storage: "disk", buckets: stubBuckets(2, dumpBucketDroppingState)}
	text := restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{}, target))
	for _, want := range []string{`database "app" still has 2 buckets that are being dropped (B1, B2)`, "Nothing was written.", "show buckets in app", "--db app --as app_restored"} {
		if !strings.Contains(text, want) {
			t.Errorf("the wait text does not contain %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "drop bucket") {
		t.Errorf("the wait text tells the operator to drop what is already dropping:\n%s", text)
	}

	mixed := &stubRestoreTarget{exists: true, rf: 1, storage: "disk", buckets: append(stubBuckets(1, "active"), stubBuckets(1, dumpBucketDroppingState)[0])}
	mixed.buckets[1].Name = "Gone"
	text = restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{}, mixed))
	if !strings.Contains(text, "drop bucket B1 in app") || !strings.Contains(text, "Gone") || !strings.Contains(text, "still being dropped") {
		t.Errorf("a database with an active and a dropping bucket must get the drop command and the wait line:\n%s", text)
	}
}

func TestRestoreRefusesADifferentReplicationFactorAndNamesAs(t *testing.T) {
	target := &stubRestoreTarget{exists: true, rf: 2, storage: "disk"}
	text := restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{}, target))
	for _, want := range []string{"replication factor 2 and storage disk", "replication factor 1 and storage disk", "Nothing was written.", "--db app --as app_restored"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal does not contain %q:\n%s", want, text)
		}
	}
}

func TestRestorePassTwoAcceptsAnAbsentAndAnEmptyExistingDatabase(t *testing.T) {
	for name, target := range map[string]*stubRestoreTarget{
		"absent": {},
		"empty":  {exists: true, rf: 1, storage: "disk"},
	} {
		if err := checkPassTwo(t, &ringRestoreFlags{}, target); err != nil {
			t.Errorf("%s target refused: %v", name, err)
		}
	}
}

func TestRestoreLimitsTheListedBucketNamesToTen(t *testing.T) {
	target := &stubRestoreTarget{exists: true, rf: 1, storage: "disk", buckets: stubBuckets(25, "active")}
	text := restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{}, target))
	if !strings.Contains(text, "already holds 25 buckets") || !strings.Contains(text, "B10") || strings.Contains(text, "B11,") || !strings.Contains(text, "and 15 more") {
		t.Errorf("a target with 25 buckets must list ten names and say 15 more:\n%s", text)
	}
}

func TestRestoreRefusesADanglingLinkBeforeCreatingAnything(t *testing.T) {
	records := dumpVerifyTestRecords(t, "hashStore")
	bucket := records[2].(*dumpBucket)
	bucket.Fields = append(bucket.Fields, dumpField{Name: "Boss", Type: "string", Of: "#7"})
	path := writeVerifyTestDump(t, records, dumpVerifyTestCounts)
	target := &stubRestoreTarget{}
	text := restoreRefusalText(t, checkRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, target, path))
	for _, want := range []string{`Bucket "User" field "Boss" links to a bucket that was dropped before this dump was taken (#7)`, "Nothing was written.", "alter bucket"} {
		if !strings.Contains(text, want) {
			t.Errorf("the dangling-link refusal does not contain %q:\n%s", want, text)
		}
	}
}

func TestRestoreRefusesADatabaseTheDumpDoesNotHold(t *testing.T) {
	text := restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{database: "shop"}, &stubRestoreTarget{}))
	if !strings.Contains(text, `Database "shop" is not in this dump`) || !strings.Contains(text, "holds app") || !strings.Contains(text, "--db app") {
		t.Errorf("the refusal does not name what the dump holds:\n%s", text)
	}
}

func TestRestorePassTwoIgnoresADatabaseThatWasNotSelected(t *testing.T) {
	// The target holds a non-empty "app", but this run restores only "other", which the dump
	// does not hold: the refusal is about "other", and "app" is never read.
	target := &stubRestoreTarget{exists: true, rf: 1, storage: "disk", buckets: stubBuckets(3, "active")}
	err := checkPassTwo(t, &ringRestoreFlags{database: "other"}, target)
	if err == nil || !strings.Contains(ringFailure(err).Message, `Database "other" is not in this dump`) {
		t.Fatalf("got %v", err)
	}
	if target.calls != 0 {
		t.Errorf("pass 2 read the target %d times for a database it was not asked to restore", target.calls)
	}
}

// TestCheckBucketAbsentPinsEachDescribeAnswer: an existing bucket is refused (exit 2); no such
// bucket and no privilege both mean absent, as pass 2 treats a database; and any other failure
// (a lost connection, an overloaded node) stops the restore with that failure's own exit,
// because absence was not confirmed. Swallowing the last, or reading no privilege as existing,
// each fails here.
func TestCheckBucketAbsentPinsEachDescribeAnswer(t *testing.T) {
	flags := &ringRestoreFlags{file: "d.hoarddump"}
	check := func(describeErr error) error {
		run := &ringRestoreRun{ctx: context.Background(), flags: flags, target: &stubRestoreTarget{describeErr: describeErr}}
		return run.checkBucketAbsent(flags, "app", "User", "app")
	}
	if err := check(nil); err == nil || ringFailure(err).Exit != ExitRefused || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("an existing bucket gave %v, want the refusal with exit %d", err, ExitRefused)
	}
	for name, describeErr := range map[string]error{
		"not found":         &ringclient.Error{Code: cluster.CodeNotFound, Message: "no such bucket"},
		"permission denied": &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "no role"},
	} {
		if err := check(describeErr); err != nil {
			t.Errorf("%s was refused: %v", name, err)
		}
	}
	err := check(io.ErrUnexpectedEOF)
	if err == nil || ringFailure(err).Exit != ExitUnavailable {
		t.Errorf("a describe that failed with a lost connection gave %v, want a stop with exit %d", err, ExitUnavailable)
	}
	err = check(&ringclient.Error{Code: cluster.CodeUnavailable, Message: "overloaded"})
	if err == nil || ringFailure(err).Exit != ExitUnavailable {
		t.Errorf("an UNAVAILABLE describe gave %v, want a stop with exit %d", err, ExitUnavailable)
	}
}

// TestRestoreRefusalSaysOneBucketInTheSingular: a database holding one bucket is "1 bucket",
// not "1 buckets", in the refusal an operator reads first.
func TestRestoreRefusalSaysOneBucketInTheSingular(t *testing.T) {
	target := &stubRestoreTarget{exists: true, rf: 1, storage: "disk", buckets: stubBuckets(1, "active")}
	text := restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{}, target))
	if !strings.Contains(text, `already holds 1 bucket (B1)`) {
		t.Errorf("the refusal does not say 1 bucket:\n%s", text)
	}
	dropping := &stubRestoreTarget{exists: true, rf: 1, storage: "disk", buckets: stubBuckets(1, "dropping")}
	text = restoreRefusalText(t, checkPassTwo(t, &ringRestoreFlags{}, dropping))
	if !strings.Contains(text, `still has 1 bucket that is being dropped (B1)`) {
		t.Errorf("the wait refusal does not say 1 bucket:\n%s", text)
	}
}
