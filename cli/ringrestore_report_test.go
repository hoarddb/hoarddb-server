package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// writeShapedRestoreDump writes a dump of the given databases, each with the given buckets,
// each bucket with two entities that have one fifo element apiece, with the dump writer.
func writeShapedRestoreDump(t *testing.T, shape [][]string) string {
	t.Helper()
	return writeShapedRestoreDumpWithRF(t, shape, nil)
}

// writeShapedRestoreDumpWithRF is writeShapedRestoreDump with the replication factor of the
// databases given by index; a missing entry is 1.
func writeShapedRestoreDumpWithRF(t *testing.T, shape [][]string, factors map[int]int32) string {
	t.Helper()
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "shaped.hoarddump")
	if err != nil {
		t.Fatal(err)
	}
	scalars, err := bson.Marshal(bson.D{{Key: "Name", Value: "n"}})
	if err != nil {
		t.Fatal(err)
	}
	element := largeRestoreElement(t, 4)
	var counters dumpCounters
	write := func(record any) {
		if err := writer.WriteRecord(record); err != nil {
			t.Fatal(err)
		}
	}
	write(dumpTestHeader())
	for index, buckets := range shape {
		database := fmt.Sprintf("db%d", index+1)
		factor := int32(1)
		if set, ok := factors[index]; ok {
			factor = set
		}
		write(&dumpDB{T: dumpKindDB, Name: database, RF: factor, Storage: "disk"})
		counters.Databases++
		for _, bucket := range buckets {
			write(&dumpBucket{T: dumpKindBucket, DB: database, Name: bucket, Fields: []dumpField{{Name: "Name", Type: "string"}, {Name: "Items", Type: "fifoStore"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}})
			counters.Buckets++
			for _, id := range []string{"x", "y"} {
				write(&dumpDoc{T: dumpKindDoc, DB: database, B: bucket, ID: id, F: scalars})
				write(&dumpElems{T: dumpKindElems, DB: database, B: bucket, ID: id, Field: "Items", Kind: "fifoStore", Els: []bson.Raw{element}})
				counters.Entities++
				counters.Elements++
			}
		}
	}
	if err := writer.Close(counters); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "shaped.hoarddump")
}

// failingOn answers every frame as accepted except the first whose first operation is an
// insert into the named database and bucket, which it answers with a refusal on that operation.
func failingOn(database, bucket string) func(int, []restoreOp) []ringDocAnswer {
	failed := false
	return func(_ int, ops []restoreOp) []ringDocAnswer {
		answers := make([]ringDocAnswer, len(ops))
		if !failed && ops[0].kind == restoreInsert && ops[0].database == database && ops[0].bucket == bucket {
			failed = true
			answers[0].Err = &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "no role to write here"}
		}
		return answers
	}
}

// runFailedRestore applies path through a scripted transport and returns the exit code and
// the output of the report, as the verb prints them.
func runFailedRestore(t *testing.T, path string, flags ringRestoreFlags, answer func(int, []restoreOp) []ringDocAnswer) (int, string, *restoreHarness) {
	t.Helper()
	targetAddr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, targetAddr, path, flags)
	harness.sender.transport = &scriptedRestoreTransport{answer: answer}
	err := harness.applier.apply()
	if err == nil {
		t.Fatal("the scripted failure did not stop the restore")
	}
	code := harness.run.fail(err, harness.applier)
	return code, harness.stderr.String(), harness
}

// TestKilledRestoreReportsTheCompleteBucketsAndOneRetryCommandPerDatabase: a dump of three
// databases, the transport refuses a frame in the second. The report has no line for the
// finished first database, the retry command for the second renames it with --db and --as, and
// the third gets its own plain --db command.
func TestKilledRestoreReportsTheCompleteBucketsAndOneRetryCommandPerDatabase(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"A1"}, {"B1", "B2"}, {"C1"}})
	code, output, _ := runFailedRestore(t, path, ringRestoreFlags{}, failingOn("db2", "B2"))
	if code != ExitRefused {
		t.Errorf("exit %d, want %d", code, ExitRefused)
	}
	for _, want := range []string{
		"1 databases were written in full",
		`Database "db2" was created and is not complete.`,
		"Its complete buckets: B1.",
		`Bucket "B2" was created and is not complete`,
		"hoarddb restore " + path + " --db db2 --as db2_restored",
		"hoarddb restore " + path + " --db db3\n",
		"never merges",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("the report does not contain %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "--db db1") {
		t.Errorf("the report has a command for the finished database:\n%s", output)
	}
	if strings.Count(output, "--as") != 1 {
		t.Errorf("the report has %d --as commands, want exactly one (one database is in progress):\n%s", strings.Count(output, "--as"), output)
	}
}

// TestKilledRestoreReportsThePartialBucket: five buckets, the transport refuses the third's
// frame; the first two are named complete and the third is the partial one.
func TestKilledRestoreReportsThePartialBucket(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1", "B2", "B3", "B4", "B5"}})
	_, output, _ := runFailedRestore(t, path, ringRestoreFlags{}, failingOn("db1", "B3"))
	for _, want := range []string{"Its complete buckets: B1, B2.", `Bucket "B3" was created and is not complete`, "--db db1 --as db1_restored"} {
		if !strings.Contains(output, want) {
			t.Errorf("the report does not contain %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "B4") || strings.Contains(output, "B5") {
		t.Errorf("the report names buckets the restore never reached:\n%s", output)
	}
}

// TestKilledRestoreThenRestoreAsANewNameSucceeds: a restore of the rule 11 ring is stopped
// by a refused frame; running it again as it was is refused by pass 2, and the retry command
// the report prints, --as another name, restores everything.
func TestKilledRestoreThenRestoreAsANewNameSucceeds(t *testing.T) {
	sourceAddr, source := newRingTestHost(t)
	targetAddr, target := newRingTestHost(t)
	fillRestoreUsers(t, source, "app")
	path := dumpToFile(t, sourceAddr)

	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	harness.sender.transport = &failingAfterTransport{inner: harness.sender.transport, healthyFrames: 1}
	err := harness.applier.apply()
	if err == nil {
		t.Fatal("the transport that stops answering did not stop the restore")
	}
	harness.run.fail(err, harness.applier)
	report := harness.stderr.String()
	if !strings.Contains(report, "--db app --as app_restored") {
		t.Fatalf("the report does not print the retry command:\n%s", report)
	}

	if code, _, stderr := restoreOnHost(t, targetAddr, path); code != ExitRefused || !strings.Contains(stderr, "never merges") {
		t.Fatalf("running the stopped restore again as it was exited %d:\n%s", code, stderr)
	}
	if code, _, stderr := restoreOnHost(t, targetAddr, path, "--db", "app", "--as", "app_restored"); code != ExitOK {
		t.Fatalf("the printed retry command exited %d:\n%s", code, stderr)
	}
	for _, id := range restoreUserIDs {
		if got := storeCount(t, target, "app_restored", id, "Purchases"); got != 5 {
			t.Errorf("Purchases of %q in app_restored holds %d, want 5", id, got)
		}
	}
}

// failingAfterTransport forwards healthyFrames frames and then refuses the first operation of
// every later frame, which is a stand-in for a permission or quota refusal mid-restore.
type failingAfterTransport struct {
	inner         restoreTransport
	healthyFrames int
	sent          int
}

func (f *failingAfterTransport) NextRequestID() [16]byte { return f.inner.NextRequestID() }
func (f *failingAfterTransport) close() error            { return f.inner.close() }
func (f *failingAfterTransport) Send(ctx context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	f.sent++
	if f.sent > f.healthyFrames {
		answers := make([]ringDocAnswer, len(ops))
		answers[0].Err = &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "no role to write here"}
		return answers, nil
	}
	return f.inner.Send(ctx, ops, id)
}

// TestRestoreRefusedOperationStopsAndNamesTheEntity: a refused push stops the restore at once,
// no frame is sent after it, the exit is the refusal's class, and the text names the entity,
// the field, the operation and the server's code and words.
func TestRestoreRefusedOperationStopsAndNamesTheEntity(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1"}})
	frames := 0
	code, output, _ := runFailedRestore(t, path, ringRestoreFlags{}, func(_ int, ops []restoreOp) []ringDocAnswer {
		frames++
		answers := make([]ringDocAnswer, len(ops))
		if ops[0].kind == restorePush {
			answers[0].Err = &ringclient.Error{Code: cluster.CodePermissionDenied, Message: "no role to push here"}
		}
		return answers
	})
	if code != ExitRefused {
		t.Errorf("exit %d, want %d", code, ExitRefused)
	}
	for _, want := range []string{`a push into field "Items" of db1.B1 id "x" was refused: PERMISSION_DENIED: no role to push here`, "restore stopped"} {
		if !strings.Contains(output, want) {
			t.Errorf("the output does not contain %q:\n%s", want, output)
		}
	}
	if frames != 2 {
		t.Errorf("%d frames were sent, want the insert frame and the one refused push frame", frames)
	}
}

// TestInterruptedRestoreExitsFourAndReports: cancelling the run's context in the middle of a
// restore stops it at the next call, exits 4 and still says what was created.
func TestInterruptedRestoreExitsFourAndReports(t *testing.T) {
	path := writeShapedRestoreDump(t, [][]string{{"B1", "B2", "B3"}})
	targetAddr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	ctx, cancel := context.WithCancel(harness.run.ctx)
	defer cancel()
	harness.run.ctx = ctx
	frames := 0
	harness.sender.transport = &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer {
		frames++
		if frames == 3 {
			cancel()
		}
		return make([]ringDocAnswer, len(ops))
	}}
	err := harness.applier.apply()
	if err == nil {
		t.Fatal("the cancelled restore finished")
	}
	if code := harness.run.fail(err, harness.applier); code != ExitInterrupted {
		t.Fatalf("exit %d, want %d", code, ExitInterrupted)
	}
	output := harness.stderr.String()
	if !strings.Contains(output, "restore interrupted") || !strings.Contains(output, `Database "db1" was created and is not complete.`) {
		t.Errorf("the interrupted restore did not say what it left behind:\n%s", output)
	}
}

// TestRestoreWhoseDatabaseCreateFailsListsThatDatabaseAndTheRestAsNotStarted: the create of
// the second database is refused (a replication factor of 3 on a one-node ring), so the first
// is complete, the second and third get plain commands and nothing is renamed with --as.
func TestRestoreWhoseDatabaseCreateFailsListsThatDatabaseAndTheRestAsNotStarted(t *testing.T) {
	path := writeShapedRestoreDumpWithRF(t, [][]string{{"A1"}, {"B1"}, {"C1"}}, map[int]int32{1: 3})
	targetAddr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, targetAddr, path, ringRestoreFlags{})
	err := harness.applier.apply()
	if err == nil {
		t.Fatal("the create of a database with replication factor 3 succeeded on a one-node ring")
	}
	harness.run.fail(err, harness.applier)
	output := harness.stderr.String()
	for _, want := range []string{"1 databases were written in full", "--db db2\n", "--db db3\n"} {
		if !strings.Contains(output, want) {
			t.Errorf("the report does not contain %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "--as") || strings.Contains(output, "--db db1") {
		t.Errorf("the report renames a database that was not created, or repeats the finished one:\n%s", output)
	}
}
