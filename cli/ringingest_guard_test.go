package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// These tests keep, at run level on the ring verb, the guards the legacy ingest tests held and
// the ring port dropped: stored values keep their types, Ctrl-C stops a run in either pass,
// an unknown outcome exits 8, a schema change refused during a join says why, and a refused
// create shows the server's words.

// TestIngestStoredValuesKeepTheirTypes: dates stay dates and every value comes back as it
// went in, field for field, and the declaration the run made names those types.
func TestIngestStoredValuesKeepTheirTypes(t *testing.T) {
	addr, client := newRingTestHost(t)
	when := time.Date(2024, 5, 17, 10, 30, 0, 0, time.UTC)
	ref := primitive.NewObjectID()
	document := bson.D{
		{Key: "_id", Value: "a"},
		{Key: "name", Value: "Ada"},
		{Key: "age", Value: int32(36)},
		{Key: "balance", Value: 12.5},
		{Key: "active", Value: true},
		{Key: "born", Value: primitive.NewDateTimeFromTime(when)},
		{Key: "owner", Value: ref},
		{Key: "address", Value: bson.D{{Key: "city", Value: "Lyon"}}},
		{Key: "tags", Value: bson.A{"x", "y"}},
		{Key: "nothing", Value: nil},
	}
	path := writeBSONFile(t, "people.bson", document)
	if code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.people"); code != ExitOK {
		t.Fatalf("exit %d:\n%s", code, stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stored, err := client.Bucket("app", "people").Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		field string
		kind  bsontype.Type
	}{
		{"name", bsontype.String}, {"age", bsontype.Int32}, {"balance", bsontype.Double}, {"active", bsontype.Boolean},
		{"born", bsontype.DateTime}, {"owner", bsontype.ObjectID}, {"address", bsontype.EmbeddedDocument}, {"tags", bsontype.Array},
	} {
		if got := stored.Lookup(want.field).Type; got != want.kind {
			t.Errorf("field %q came back as %v, want %v", want.field, got, want.kind)
		}
	}
	if got := stored.Lookup("born").Time(); !got.Equal(when) {
		t.Errorf("born came back as %v, want %v: a date must stay the same instant", got, when)
	}
	if got := stored.Lookup("name").StringValue(); got != "Ada" {
		t.Errorf("name came back as %q", got)
	}
	if got := stored.Lookup("balance").Double(); got != 12.5 {
		t.Errorf("balance came back as %v", got)
	}
	if got := stored.Lookup("owner").ObjectID(); got != ref {
		t.Errorf("owner came back as %v, want %v", got, ref)
	}
	if got := stored.Lookup("address").Document().Lookup("city").StringValue(); got != "Lyon" {
		t.Errorf("address.city came back as %q", got)
	}
	info, err := client.DescribeBucket(ctx, "app", "people")
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]string{}
	for _, field := range info.Fields {
		declared[field.Name] = field.Type
	}
	for field, word := range map[string]string{"born": "datetime", "age": "int", "balance": "double", "active": "bool", "owner": "objectid", "address": "object", "tags": "array"} {
		if declared[field] != word {
			t.Errorf("field %q is declared %q, want %q", field, declared[field], word)
		}
	}
}

// tripContext reports context.Canceled from Err after a number of calls, so a test can stop a
// run at a chosen point of its two passes without a signal or a sleep. Dial and each document
// read call Err, so the point is counted in reads.
type tripContext struct {
	context.Context
	calls atomic.Int64
	trip  int64
}

func (c *tripContext) Err() error {
	if c.trip > 0 && c.calls.Add(1) > c.trip {
		return context.Canceled
	}
	if c.trip == 0 {
		c.calls.Add(1)
	}
	return c.Context.Err()
}

func manyDocumentsFile(t *testing.T, count int) string {
	t.Helper()
	documents := make([]bson.D, count)
	for index := range documents {
		documents[index] = bson.D{{Key: "_id", Value: fmt.Sprintf("d%05d", index)}, {Key: "n", Value: int32(index)}}
	}
	return writeBSONFile(t, "many.bson", documents...)
}

func runTripped(t *testing.T, addr string, trip int64, args ...string) (code int, stderr string, calls int64) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tripping := &tripContext{Context: ctx, trip: trip}
	code = runRingIngest(tripping, append(append([]string{}, args...), ringIngestLogin(addr)...), ringIngestEnv{stdout: &out, stderr: &errOut})
	return code, errOut.String(), tripping.calls.Load()
}

// TestIngestInterruptedInPassOneWritesNothing: Ctrl-C while the file is still being read exits
// 4, says nothing was written and creates no bucket.
func TestIngestInterruptedInPassOneWritesNothing(t *testing.T) {
	const documents = 1000
	addr, client := newRingTestHost(t)
	path := manyDocumentsFile(t, documents)
	code, stderr, _ := runTripped(t, addr, 200, path, "--into", "app.many")
	if code != ExitInterrupted {
		t.Fatalf("exit %d, want %d:\n%s", code, ExitInterrupted, stderr)
	}
	if !strings.Contains(stderr, "interrupted while reading many.bson; nothing was written.") {
		t.Errorf("the run does not say it stopped in the first pass:\n%s", stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, found, err := (ringClientTarget{client}).DescribeBucket(ctx, "app", "many"); err != nil || found {
		t.Errorf("the interrupted run left bucket app.many behind (found %v, err %v)", found, err)
	}
}

// TestIngestInterruptedInPassTwoStopsAndRunningAgainFinishes: Ctrl-C while documents are being
// written exits 4 with fewer documents than the file holds; running the same command again
// loads all of them and adds none twice.
func TestIngestInterruptedInPassTwoStopsAndRunningAgainFinishes(t *testing.T) {
	const documents = 1000
	path := manyDocumentsFile(t, documents)
	calibrationAddr, _ := newRingTestHost(t)
	code, stderr, total := runTripped(t, calibrationAddr, 0, path, "--into", "app.many")
	if code != ExitOK {
		t.Fatalf("the calibration run exited %d:\n%s", code, stderr)
	}
	addr, client := newRingTestHost(t)
	code, stderr, _ = runTripped(t, addr, total-documents/2, path, "--into", "app.many")
	if code != ExitInterrupted {
		t.Fatalf("exit %d, want %d:\n%s", code, ExitInterrupted, stderr)
	}
	if !strings.Contains(stderr, "ingest: interrupted after ") || !strings.Contains(stderr, "Run the same command again to finish") {
		t.Errorf("the run does not say it stopped in the second pass and how to finish:\n%s", stderr)
	}
	if loaded := len(listIDs(t, client, "many")); loaded >= documents {
		t.Errorf("the interrupted run loaded %d of %d documents; it should have stopped early", loaded, documents)
	}
	if code, _, stderr := ingestOnHost(t, addr, path, "--into", "app.many"); code != ExitOK {
		t.Fatalf("running again: exit %d:\n%s", code, stderr)
	}
	if loaded := len(listIDs(t, client, "many")); loaded != documents {
		t.Errorf("after running again %d documents are loaded, want %d", loaded, documents)
	}
}

// TestIngestUnknownOutcomeExitsEightAndSaysRunAgain: a frame that was sent and never answered
// ends the run with exit 8 and the sentence that makes running the command again safe.
func TestIngestUnknownOutcomeExitsEightAndSaysRunAgain(t *testing.T) {
	path := manyDocumentsFile(t, 30)
	flags, err := parseRingIngestArgs([]string{path, "--into", "app.users"}, ringIngestEnv{stdout: io.Discard, stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	input, err := openRingIngestInput(flags)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	run := &ringIngestRun{env: ringIngestEnv{stdout: io.Discard, stderr: &out}, out: &out, flags: flags, input: input, database: "app", bucket: "users", started: time.Now()}
	ctx := context.Background()
	scan, err := scanFile(input.open(ctx))
	if err != nil {
		t.Fatal(err)
	}
	sender, _, _ := scriptedSender(func(int, recordedFrame) ([]ringDocAnswer, error) {
		return nil, fmt.Errorf("ringclient: the connection to the ring ended: %w", io.ErrUnexpectedEOF)
	})
	outcome := run.write(ctx, scan, sender)
	code := run.finish(scan, preparedTarget{}, outcome)
	if code != ExitUnavailable {
		t.Fatalf("exit %d, want %d:\n%s", code, ExitUnavailable, out.String())
	}
	for _, want := range []string{"were sent and the ring never confirmed them", "Run the same command again: every document is an upsert"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the report does not say %q:\n%s", want, out.String())
		}
	}
}

// failingCreateTarget refuses every create with the error it was given.
type failingCreateTarget struct {
	*stubRingTarget
	createError error
}

func (f failingCreateTarget) CreateDatabase(context.Context, string) error { return f.createError }

func (f failingCreateTarget) CreateBucket(context.Context, string, string, []ringclient.FieldDecl) error {
	return f.createError
}

// TestIngestShowsTheServersReasonWhenACreateIsRefused: a create refused during a join, or for
// want of a role, reaches the user in the server's words, exits as a refusal and leaves nothing
// created.
func TestIngestShowsTheServersReasonWhenACreateIsRefused(t *testing.T) {
	const freezeText = "Schema changes are paused while n4 joins the cluster: 3 of 10 buckets of this membership change have been copied to their new places. " +
		"Nothing was created, dropped or restored; reads and writes to existing buckets carry on. Run this again once 'nodes' shows the join settled - it settles on its own."
	const denied = "this login may write to database app but not create buckets in it; ask an administrator for the db-manager role on app"
	for _, tc := range []struct {
		name      string
		existing  bool
		err       error
		wantExit  int
		wantWords []string
	}{
		{"create bucket during a join", true, &ringclient.Error{Code: cluster.CodeUnavailable, Message: freezeText}, cluster.CodeUnavailable.Exit(), []string{freezeText}},
		{"create database during a join", false, &ringclient.Error{Code: cluster.CodeUnavailable, Message: freezeText}, cluster.CodeUnavailable.Exit(), []string{freezeText}},
		{"create bucket without the role", true, &ringclient.Error{Code: cluster.CodePermissionDenied, Message: denied}, cluster.CodePermissionDenied.Exit(), []string{denied}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newStubRingTarget()
			if tc.existing {
				stub.databases["app"] = map[string][]ringclient.FieldDecl{}
			}
			target := failingCreateTarget{stubRingTarget: stub, createError: tc.err}
			_, err := prepareTarget(context.Background(), target, usersParams(), scanOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(1)}}))
			if err == nil {
				t.Fatal("a refused create did not stop the run")
			}
			var out bytes.Buffer
			code := refuseRingIngest(ringIngestEnv{stdout: io.Discard, stderr: &out}, nil, err)
			if code != tc.wantExit {
				t.Errorf("exit %d, want %d:\n%s", code, tc.wantExit, out.String())
			}
			for _, want := range tc.wantWords {
				if !strings.Contains(out.String(), want) {
					t.Errorf("the output does not carry the server's words %q:\n%s", want, out.String())
				}
			}
			if len(stub.createdBuckets) != 0 {
				t.Errorf("the run created %v although the create was refused", stub.createdBuckets)
			}
		})
	}
}

// TestIngestStoppedDuringAResendWaitIsInterrupted: a signal that arrives while a frame waits to
// be resent cancels the wait, and the send error then says "context canceled". The run is
// interrupted (exit 4, run it again), not unavailable (exit 8), whether the error is a plain
// failure or an unknown outcome.
func TestIngestStoppedDuringAResendWaitIsInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, err := range map[string]error{
		"plain":   fmt.Errorf("stopped while waiting to resend: %w", context.Canceled),
		"unknown": &ringOutcomeUnknownError{},
	} {
		t.Run(name, func(t *testing.T) {
			run := &ringIngestRun{}
			var outcome ringIngestOutcome
			run.stopOnSendError(ctx, &outcome, err)
			if !outcome.interrupted || outcome.stopped != "" {
				t.Fatalf("a send error with a cancelled context left interrupted=%v stopped=%q, want interrupted and no stop text", outcome.interrupted, outcome.stopped)
			}
		})
	}
	// With a live context the same errors still stop the run with their own exit.
	var outcome ringIngestOutcome
	(&ringIngestRun{}).stopOnSendError(context.Background(), &outcome, &ringOutcomeUnknownError{})
	if outcome.interrupted || outcome.stopExit != ExitUnavailable {
		t.Fatalf("an unknown outcome with a live context: interrupted=%v exit=%d, want exit %d", outcome.interrupted, outcome.stopExit, ExitUnavailable)
	}
}
