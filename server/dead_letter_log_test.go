package server

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The paced dead-letter line (claim-ack build step 20, spec 11.2).

// deadLetterOnANode drives one entry of bucket through its deliveries on a
// single node, so the claim after them dead-letters it.
func deadLetterOnANode(t *testing.T, s *Server, clock *leaseClockAt, bucket string) {
	t.Helper()
	for delivery := 0; delivery < store.MaxDeliveries; delivery++ {
		if answer := claimOf(t, s, bucket, 1, 1000); !answer.Found {
			t.Fatalf("delivery %d of %s: %+v", delivery+1, bucket, answer)
		}
		clock.advance(2 * time.Second)
	}
	claimOf(t, s, bucket, 1, 1000)
}

// loggedLeaseServer is a standalone server whose log and dead-letter clock a
// test holds, with one single-entry fifo bucket per name.
func loggedLeaseServer(t *testing.T, buckets ...string) (*Server, *leaseClockAt, *fakeClock, *capturedLog) {
	t.Helper()
	s := newTestServer(t)
	clock := withLeaseClock(s)
	logger, captured := newLogCapture()
	s.logger = logger
	windowClock := newFakeClock()
	s.deadLetters.useClock(windowClock.Now)
	for _, bucket := range buckets {
		createTestBucket(t, s, "app", bucket, BucketTypeFIFO)
		pushPeekEntries(t, s, bucket, 1, "")
	}
	return s, clock, windowClock, captured
}

// TestDeadLetteringLogsDistinctBucketsOnTheOwnerOnly: two buckets that
// dead-letter in one window are both named, one at once and one when the
// window closes; the two replicas write no line, and a replay of the owner's
// own entry after a restart writes none either.
func TestDeadLetteringLogsDistinctBucketsOnTheOwnerOnly(t *testing.T) {
	logger, captured := newLogCapture()
	clusterTestLogger.Store(logger)
	t.Cleanup(func() { clusterTestLogger.Store(nil) })
	windowClock := newFakeClock()
	tc, clocks := leaseCluster(t, nil, nil, func(_ int, c *Cluster) { c.s.deadLetters.useClock(windowClock.Now) })
	defer tc.stop()
	const owner = 0
	deadLetterLines := func() []string { return captured.messages(slog.LevelWarn, "dead-lettered") }

	first := clusterPopSubject{kind: "logone", bucketType: BucketTypeFIFO}.createOn(t, tc, owner)
	second := clusterPopSubject{kind: "logtwo", bucketType: BucketTypeFIFO}.createOn(t, tc, owner)
	for _, subject := range []clusterPopSubject{first, second} {
		subject.push(t, tc, owner, 1, 1, 0)
		deadLetterOne(t, tc, clocks, owner, subject)
		claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), "the claim that dead-letters")
	}
	lines := deadLetterLines()
	if len(lines) != 1 || !containsAll(lines[0], []string{fmt.Sprintf("bucket %q", first.bucket), fmt.Sprintf("db.%s.dead()", first.bucket)}) || strings.Contains(lines[0], second.bucket) {
		t.Fatalf("two buckets dead-lettered in one window, on three nodes: want one line, naming only the first bucket; got %q", lines)
	}

	windowClock.Advance(deadLetterLogInterval)
	for _, node := range tc.nodes {
		node.server.core.flushDeadLetterLog()
	}
	lines = deadLetterLines()
	if len(lines) != 2 || !containsAll(lines[1], []string{fmt.Sprintf("bucket %q", second.bucket), fmt.Sprintf("db.%s.dead()", second.bucket)}) {
		t.Fatalf("after the window closed the held bucket was not named, once: %q", lines)
	}

	// The replay row: the owner's apply of the dead-lettering claim fails, so
	// it owes it and replays it from its own log after a restart. The replica
	// applies and the replay both reach the same decision and say nothing.
	replayed := clusterPopSubject{kind: "logthree", bucketType: BucketTypeFIFO}.createOn(t, tc, owner)
	replayed.push(t, tc, owner, 1, 1, 0)
	deadLetterOne(t, tc, clocks, owner, replayed)
	claimSeq := tc.quiescedSequences(t)[owner] + 1
	failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, func(seq uint64) bool { return seq == claimSeq })
	request := replayed.claimRequest(1, 60_000)
	request["writeconcern"] = "majority"
	tc.frame(owner, transport.OpQueueClaim, request)
	if failed.Load() != claimSeq {
		t.Fatalf("setup: the owner's apply at sequence %d never reached its store", claimSeq)
	}
	store.ApplyFaultHook.Store(nil)
	for _, replica := range []int{1, 2} {
		pollUntil(t, fmt.Sprintf("replica %d dead-letters the entry", replica+1), func() bool {
			return bucketStateOf(t, tc.nodes[replica].server.core, testDatabase, replayed.bucket).Dead == 1
		})
	}
	windowClock.Advance(deadLetterLogInterval)
	tc.restartNode(owner)
	tc.waitReady()
	pollUntil(t, "the owner replays the claim", func() bool {
		return bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, replayed.bucket).Dead == 1
	})
	tc.nodes[owner].server.core.flushDeadLetterLog()
	if lines = deadLetterLines(); len(lines) != 2 {
		t.Fatalf("a replica's apply and the owner's replay of a dead-lettering claim wrote lines: %q", lines[2:])
	}
}

// TestHeldDeadLetterNamesAreFlushedByTheSamplerTick: a burst in one window,
// then silence. Driving the sampler's tick by hand prints every held name,
// and nothing else is needed to print them.
func TestHeldDeadLetterNamesAreFlushedByTheSamplerTick(t *testing.T) {
	buckets := []string{"burstone", "bursttwo", "burstthree"}
	s, clock, windowClock, captured := loggedLeaseServer(t, buckets...)
	sampler := newIdleSampler(t, s)
	for _, bucket := range buckets {
		deadLetterOnANode(t, s, clock, bucket)
	}
	if lines := captured.messages(slog.LevelWarn, "dead-lettered"); len(lines) != 1 || !strings.Contains(lines[0], "burstone") {
		t.Fatalf("a burst in one window wrote %q, want one line naming the first bucket", lines)
	}

	sampler.sampleNow()
	if lines := captured.messages(slog.LevelWarn, "dead-lettered"); len(lines) != 1 {
		t.Fatalf("a tick inside the window printed the held names early: %q", lines)
	}
	windowClock.Advance(deadLetterLogInterval)
	sampler.sampleNow()
	lines := captured.messages(slog.LevelWarn, "dead-lettered")
	if len(lines) != 2 || !containsAll(lines[1], []string{`bucket "bursttwo"`, `bucket "burstthree"`, "db.bursttwo.dead()"}) || strings.Contains(lines[1], "burstone") {
		t.Fatalf("a tick after the window closed wrote %q, want one line naming the two held buckets", lines)
	}
	sampler.sampleNow()
	if lines := captured.messages(slog.LevelWarn, "dead-lettered"); len(lines) != 2 {
		t.Fatalf("a second tick printed the names again: %q", lines)
	}
}

// TestHardCapRefusalIsLoggedOnTheSameLine: a claim refused by the hard cap
// names its bucket and points to pop().
func TestHardCapRefusalIsLoggedOnTheSameLine(t *testing.T) {
	s, _, windowClock, captured := loggedLeaseServer(t)
	createTestBucket(t, s, "app", "capped", BucketTypeLIFO)
	pushedBigEntry(t, s, "capped", 1, 5<<20)

	_, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: "capped"})
	if err == nil {
		t.Fatal("the claim of an entry over the hard cap was answered")
	}
	lines := captured.messages(slog.LevelWarn, "over the largest size a claim moves")
	if len(lines) != 1 || !containsAll(lines[0], []string{`bucket "capped"`, `database "app"`, "db.capped.pop()"}) {
		t.Fatalf("a hard-cap refusal wrote %q, want one line naming the bucket and pop()", lines)
	}

	// A refusal held in a window is printed by the flush like a dead letter.
	createTestBucket(t, s, "app", "cappedtwo", BucketTypeLIFO)
	pushedBigEntry(t, s, "cappedtwo", 1, 5<<20)
	if _, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: "cappedtwo"}); err == nil {
		t.Fatal("the second claim of an entry over the hard cap was answered")
	}
	windowClock.Advance(deadLetterLogInterval)
	s.flushDeadLetterLog()
	if lines = captured.messages(slog.LevelWarn, "over the largest size a claim moves"); len(lines) != 2 || !strings.Contains(lines[1], `bucket "cappedtwo"`) {
		t.Fatalf("the held refusal was not named when its window closed: %q", lines)
	}
}

// TestDeadLetterLogNeverCarriesEntryContent: the line holds names and counts
// only, and the collector has nowhere to keep more.
func TestDeadLetterLogNeverCarriesEntryContent(t *testing.T) {
	s, clock, _, captured := loggedLeaseServer(t)
	createTestBucket(t, s, "app", "secrets", BucketTypeFIFO)
	const secret = "card-number-4111-1111-1111-1111"
	if resp, err := s.Push(PushRequest{Database: "app", Bucket: "secrets", Data: map[string]interface{}{"note": secret}}); err != nil || !resp.Success {
		t.Fatalf("pushing: %+v, %v", resp, err)
	}
	deadLetterOnANode(t, s, clock, "secrets")
	lines := captured.messages(slog.LevelWarn, "dead-lettered")
	if len(lines) != 1 {
		t.Fatalf("the dead-lettering wrote %q, want one line", lines)
	}
	if strings.Contains(lines[0], secret) || strings.Contains(lines[0], "note") {
		t.Errorf("the line carries entry content: %s", lines[0])
	}
	entry := reflect.TypeOf(deadLetterEntry{})
	for index := 0; index < entry.NumField(); index++ {
		if kind := entry.Field(index).Type.Kind(); kind != reflect.String && kind != reflect.Int {
			t.Errorf("deadLetterEntry.%s is a %s: the collector keeps names and counts only", entry.Field(index).Name, kind)
		}
	}
}

// TestDeadLetterLogNamesAtMostTheCapAndCountsTheRest: a window names
// deadLetterLogCap buckets and counts events of the rest, in fixed memory.
func TestDeadLetterLogNamesAtMostTheCapAndCountsTheRest(t *testing.T) {
	windowClock := newFakeClock()
	var collector deadLetterLog
	collector.useClock(windowClock.Now)
	var immediate []deadLetterReport
	for bucket := 0; bucket < deadLetterLogCap+3; bucket++ {
		immediate = append(immediate, collector.note("app", fmt.Sprintf("bucket%02d", bucket), 2, 0)...)
	}
	if len(immediate) != 1 || len(immediate[0].entries) != 1 {
		t.Fatalf("the window printed %d lines at once, want only its first event", len(immediate))
	}
	windowClock.Advance(deadLetterLogInterval)
	report, ok := collector.flush()
	if !ok || len(report.entries) != deadLetterLogCap-1 || report.further != 3 {
		t.Fatalf("the flush = %d held names (%v), %d further; want %d and 3", len(report.entries), ok, report.further, deadLetterLogCap-1)
	}
	if _, ok := collector.flush(); ok {
		t.Error("a second flush printed the names again")
	}
	if lines := collector.note("app", "bucket00", 1, 0); len(lines) != 1 {
		t.Errorf("the first event of the next window wrote %d lines, want 1", len(lines))
	}
}

// TestNoTimerOrGoroutinePerBucket (CA-34 pin): a thousand buckets with claims
// add no goroutine, timer or channel; the collector is one struct.
func TestNoTimerOrGoroutinePerBucket(t *testing.T) {
	s := newTestServer(t)
	withLeaseClock(s)
	before := runtime.NumGoroutine()
	const buckets = 1000
	for index := 0; index < buckets; index++ {
		name := fmt.Sprintf("many%04d", index)
		createTestBucket(t, s, "app", name, BucketTypeFIFO)
		pushPeekEntries(t, s, name, 1, "")
		claimOf(t, s, name, 1, 1000)
	}
	if after := runtime.NumGoroutine(); after > before+10 {
		t.Fatalf("%d buckets with claims took the goroutine count from %d to %d: something runs per bucket", buckets, before, after)
	}
}
