package server

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The modes strict, warn and off, their two counters and warn's hourly line
// (typed-fields spec §4.8, §9.5; build doc TF-6 step 6.2).

const (
	refusalsCounter = "hoarddb_field_type_refusals_total"
	acceptedCounter = "hoarddb_field_type_mismatches_accepted_total"
)

// typeCounters reads both counters.
func typeCounters(t *testing.T) (refused, accepted float64) {
	t.Helper()
	return counterValue(t, refusalsCounter), counterValue(t, acceptedCounter)
}

// requireCounterDeltas fails unless each counter moved by want since before.
func requireCounterDeltas(t *testing.T, what string, refusedBefore, acceptedBefore, wantRefused, wantAccepted float64) {
	t.Helper()
	refused, accepted := typeCounters(t)
	if refused-refusedBefore != wantRefused || accepted-acceptedBefore != wantAccepted {
		t.Errorf("%s: the refusal counter moved by %v and the accepted-mismatch counter by %v; want %v and %v",
			what, refused-refusedBefore, accepted-acceptedBefore, wantRefused, wantAccepted)
	}
}

// TestWarnCountsOnAStandaloneNode (build doc §9 E-2): a standalone node's
// live write carries neither the owner's nor the replica's marker, and it is
// the apply that answers the client, so warn counts it once, as strict counts
// its refusal once.
func TestWarnCountsOnAStandaloneNode(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Warned", ageFields(), "warn")
	createTypedBucket(t, s, "Strict", ageFields(), "")
	refusedBefore, acceptedBefore := typeCounters(t)

	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Warned", Key: "k", Data: map[string]interface{}{"Age": "thirty"}}); err != nil || !resp.Success {
		t.Fatalf("warn refused a mismatch: %+v %v", resp, err)
	}
	if document := documentOf(t, s, "app", "Warned", "k"); document["Age"] != "thirty" {
		t.Errorf("warn stored %v; want the document as sent", document)
	}
	_, err := s.Insert(InsertRequest{Database: "app", Bucket: "Strict", Key: "k", Data: map[string]interface{}{"Age": "thirty"}})
	requireTypeRefusal(t, "a mismatch in strict", err, `Field "Age"`)
	mustInsert(t, s, "app", "Warned", "fits", map[string]interface{}{"Age": int32(1)})
	requireCounterDeltas(t, "one mismatch in each mode", refusedBefore, acceptedBefore, 1, 1)
}

// TestWarnStoresAndCountsOnceOnAThreeNodeCluster (spec §9.5): every copy
// stores warn's mismatch, and the counters move once per write, on the apply
// that answers the client: not on the replicas, and not when a restarted
// owner replays its log (the strict refusal never moved its bucket's mark, so
// replay applies it again and refuses it again).
func TestWarnStoresAndCountsOnceOnAThreeNodeCluster(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createTypedOn(tc, 0, "Warned", ageFields(), "warn")
	createTypedOn(tc, 0, "Strict", ageFields(), "")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for _, bucket := range []string{"Warned", "Strict", testBucket} {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	refusedBefore, acceptedBefore := typeCounters(t)

	tc.ok(ownerIndexOf(t, tc, "Warned"), transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "Warned", Key: "k",
		Data: map[string]interface{}{"Age": "thirty"}, WriteConcern: "all"})
	strictOwner := ownerIndexOf(t, tc, "Strict")
	refusal := refusalOf(t, tc.frame(strictOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "Strict", Key: "k",
		Data: map[string]interface{}{"Age": "thirty"}, WriteConcern: "all"}))
	if transport.ErrorCode(refusal.Code) != transport.ErrFieldType {
		t.Fatalf("strict answered code %#x %q; want FIELD_TYPE", refusal.Code, refusal.Message)
	}
	// Every peer drains one queue in order, and an acknowledgement is a
	// high-water mark: once this write is confirmed by all, every copy has
	// applied the two before it.
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "after", Data: map[string]interface{}{"n": int32(1)}, WriteConcern: "all"})
	for index := range tc.nodes {
		if !holdsKeyLocallyIn(tc.nodes[index], "Warned", "k") {
			t.Errorf("node %d does not hold warn's mismatch", index+1)
		}
		if holdsKeyLocallyIn(tc.nodes[index], "Strict", "k") {
			t.Errorf("node %d holds strict's refused write", index+1)
		}
	}
	requireCounterDeltas(t, "one mismatch in each mode on three copies", refusedBefore, acceptedBefore, 1, 1)

	tc.restartNode(strictOwner)
	waitForServing(t, tc, testDatabase, "Strict", 3)
	requireCounterDeltas(t, "after the strict bucket's owner restarted and replayed its log", refusedBefore, acceptedBefore, 1, 1)
}

// TestOffChecksNothingAndKeepsTheDeclaration (spec §4.8): off stores every
// write unchecked and counts nothing, and the declaration is still recorded
// and described.
func TestOffChecksNothingAndKeepsTheDeclaration(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Loose", ageFields(), "off")
	refusedBefore, acceptedBefore := typeCounters(t)
	mustInsert(t, s, "app", "Loose", "k", map[string]interface{}{"Age": "thirty"})
	if resp, err := s.Push(PushRequest{Database: "app", Bucket: "Loose", Data: map[string]interface{}{"Age": 1.5}}); err != nil || !resp.Success {
		t.Errorf("off refused a push: %+v %v", resp, err)
	}
	requireCounterDeltas(t, "two mismatches in off", refusedBefore, acceptedBefore, 0, 0)
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "Loose"})
	if err != nil || stats.FieldTypes != "off" || len(stats.Fields) != 1 || stats.Fields[0].Name != "Age" || stats.Fields[0].Type != "int" {
		t.Errorf("describe shows %+v in mode %q (%v); want Age int in off", stats.Fields, stats.FieldTypes, err)
	}
}

// warnLogServer is a standalone node whose log is captured, stopped by the
// test itself.
func warnLogServer(t *testing.T) (*Service, *syncBuffer) {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.ReplicationFactor = 1
	logged := &syncBuffer{}
	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(logged, nil)))
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	return qs, logged
}

// warnLines are the hourly lines logged so far.
func warnLines(logged *syncBuffer) []string {
	var lines []string
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "in field_types: warn mode stored") {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestWarnLogLineIsAggregateHourlyAndNamesNoValue (spec §9.5, D-14): warn
// logs one aggregate line an hour at most, naming at most five buckets and
// never a value. No goroutine flushes it: the first counted mismatch after
// the hour boundary does, and so does a shutdown.
func TestWarnLogLineIsAggregateHourlyAndNamesNoValue(t *testing.T) {
	start := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	now := start
	previous := fieldTypesNow
	fieldTypesNow = func() time.Time { return now }
	t.Cleanup(func() { fieldTypesNow = previous })
	qs, logged := warnLogServer(t)
	s := qs.core
	const secret = "SECRET-VALUE-4711"

	buckets := []string{"B1", "B2", "B3", "B4", "B5", "B6", "B7"}
	for _, bucket := range buckets {
		createTypedBucket(t, s, bucket, ageFields(), "warn")
	}
	for round := range 2 {
		for _, bucket := range buckets {
			mustInsert(t, s, "app", bucket, fmt.Sprintf("k%d", round), map[string]interface{}{"Age": secret})
		}
	}
	if lines := warnLines(logged); len(lines) != 0 {
		t.Fatalf("warn logged within its first hour: %q", lines)
	}

	now = start.Add(time.Hour + time.Second)
	mustInsert(t, s, "app", "B1", "late", map[string]interface{}{"Age": secret})
	now = now.Add(time.Minute)
	mustInsert(t, s, "app", "B2", "later", map[string]interface{}{"Age": secret})
	lines := warnLines(logged)
	if len(lines) != 1 {
		t.Fatalf("after the hour boundary warn logged %d lines; want one: %q", len(lines), lines)
	}
	for _, want := range []string{"Since 2026-10-05T12:00:00Z, 14 writes to 7 buckets in field_types: warn mode stored a field of the wrong declared type",
		"(app.B1, app.B2, app.B3, app.B4, app.B5, and 2 more)", "Set field_types: strict on the bucket to refuse them from now on"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("the hourly line %q does not say %q", lines[0], want)
		}
	}

	if strings.Contains(lines[0], "To list") || strings.Contains(lines[0], "In the last hour") {
		t.Errorf("the hourly line %q still promises a list or says \"the last hour\"", lines[0])
	}

	if err := qs.Stop(); err != nil {
		t.Fatalf("stopping the server: %v", err)
	}
	lines = warnLines(logged)
	if len(lines) != 2 || !strings.Contains(lines[1], "2 writes to 2 buckets") {
		t.Errorf("at shutdown warn logged %q; want a second line for the two writes since the first", lines)
	}
	if strings.Contains(logged.String(), secret) {
		t.Error("the log carries a value a client wrote")
	}
}

// TestWarnLineGroupsBothNumbers: the counts read as the rest of the product's
// do, with thousands grouped (groupThousands), in the line's two numbers.
func TestWarnLineGroupsBothNumbers(t *testing.T) {
	window := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	warnings := &fieldTypeWarnings{windowStart: window, writes: 1234567, warnedBuckets: map[string]struct{}{"app.B": {}}, named: []string{"app.B"}}
	line := warnings.lineLocked()
	if !strings.Contains(line, "Since 2026-10-05T12:00:00Z, 1,234,567 writes to 1 bucket in field_types") {
		t.Errorf("the line reads %q; want the grouped write count and the window start", line)
	}
	many := &fieldTypeWarnings{windowStart: window, writes: 5, warnedBuckets: make(map[string]struct{}, warnDistinctBuckets), named: []string{"app.B"}}
	for index := range warnDistinctBuckets {
		many.warnedBuckets[fmt.Sprintf("app.B%d", index)] = struct{}{}
	}
	if line := many.lineLocked(); !strings.Contains(line, "to at least 1,000 buckets") {
		t.Errorf("the line reads %q; want \"at least 1,000 buckets\"", line)
	}
}
