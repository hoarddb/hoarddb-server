package server

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TTL (GAP-14 build doc Steps 16–19): an expiry is an ordinary delete that
// carries its condition, evaluated inside the apply on every node.

// expireIfPayload is a delete of key carrying an expiry condition on field:
// the document goes only if the field still holds a date at or before
// dateBefore, or a number of seconds at or before secondsBefore.
func expireIfPayload(t *testing.T, database, bucket, key, field string, cutoff time.Time) []byte {
	t.Helper()
	payload, err := bson.Marshal(bson.M{
		"database": database, "bucket": bucket, "key": key,
		"expire_if": bson.M{
			"field":          field,
			"date_before":    primitive.NewDateTimeFromTime(cutoff),
			"seconds_before": cutoff.Unix(),
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return payload
}

// dispatchDelete sends a delete payload through dispatch and returns the
// response document.
func dispatchDelete(t *testing.T, s *Server, payload []byte) bson.Raw {
	t.Helper()
	frame, err := s.dispatch(context.Background(), transport.NewFrameWithID(transport.OpDelete, [16]byte{1}, payload))
	if err != nil {
		t.Fatalf("dispatch delete: %v", err)
	}
	if frame.Opcode == transport.OpError {
		t.Fatalf("the delete answered an error: %s", string(frame.Payload))
	}
	return bson.Raw(frame.Payload)
}

// TestExpiryDeleteSparesARefreshedDocument (§4 F-7, S-7): the sweep scans a
// snapshot, a client refreshes the session's CreatedAt, and the sweep's
// delete applies — it must spare the document it no longer applies to, and
// say so. A document still expired is deleted by the same condition.
func TestExpiryDeleteSparesARefreshedDocument(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "sessions", "CreatedAt")
	now := time.Now()
	for _, key := range []string{"refreshed", "stale"} {
		mustInsert(t, s, "app", "sessions", key, map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))})
	}
	cutoff := now.Add(-time.Hour)
	mustInsert(t, s, "app", "sessions", "refreshed", map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now)})

	answer := dispatchDelete(t, s, expireIfPayload(t, "app", "sessions", "refreshed", "CreatedAt", cutoff))
	if documentOf(t, s, "app", "sessions", "refreshed") == nil {
		t.Fatal("the expiry delete removed a document refreshed after the sweep's scan")
	}
	if spared, _ := answer.Lookup("spared").BooleanOK(); !spared {
		t.Errorf("the response to a spared document does not say so: %v", answer)
	}
	dispatchDelete(t, s, expireIfPayload(t, "app", "sessions", "stale", "CreatedAt", cutoff))
	if documentOf(t, s, "app", "sessions", "stale") != nil {
		t.Fatal("the expiry delete spared a document still expired")
	}
}

// TestPlainDeleteIsUnchanged: a delete without a condition deletes, as it
// always has.
func TestPlainDeleteIsUnchanged(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "sessions", "CreatedAt")
	mustInsert(t, s, "app", "sessions", "k", map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(time.Now())})
	payload, err := bson.Marshal(bson.M{"database": "app", "bucket": "sessions", "key": "k"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	dispatchDelete(t, s, payload)
	if documentOf(t, s, "app", "sessions", "k") != nil {
		t.Fatal("a plain delete left the document")
	}
}

// TestExpiryConditionIsEvaluatedByEveryReplica (S-7): the condition travels in
// the entry, with the bound the owner computed, so the owner and every
// replica decide alike — a conditional owner with unconditional replicas would
// diverge.
func TestExpiryConditionIsEvaluatedByEveryReplica(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "sessions", Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []interface{}{"CreatedAt"}}})
	now := time.Now()
	put := func(op transport.Opcode, at time.Time) {
		tc.ok(0, op, InsertRequest{Database: testDatabase, Bucket: "sessions", Key: "s1",
			Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(at)}, WriteConcern: "all"})
	}
	put(transport.OpPut, now.Add(-2*time.Hour))
	put(transport.OpUpsert, now)
	payload := expireIfPayload(t, testDatabase, "sessions", "s1", "CreatedAt", now.Add(-time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := tc.nodes[0].client.SendRequest(ctx, transport.OpDelete, payload)
	if err != nil || frame.Opcode == transport.OpError {
		t.Fatalf("the conditional delete: %v, %v", frame, err)
	}
	// A later write at w=all, so every replica has applied the delete before
	// the copies are read.
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "sessions", Key: "fence",
		Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now)}, WriteConcern: "all"})
	for i, node := range tc.nodes {
		found, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: "sessions", Key: "s1"})
		if err != nil || !found.Found {
			t.Errorf("node %d no longer holds the refreshed document (%+v, %v)", i+1, found, err)
		}
	}
}

// ttlDeclaration is a TTL index on field expiring after seconds.
func ttlDeclaration(field string, seconds int64) map[string]interface{} {
	return map[string]interface{}{"fields": []interface{}{field}, "expire_after_seconds": seconds}
}

// seedSessions creates app/sessions with a one-hour TTL on CreatedAt, and
// inserts count documents created two hours before now, plus one created now.
func seedSessions(t *testing.T, s *Server, now time.Time, count int) {
	t.Helper()
	createUniqueBucket(t, s, "app", "sessions", ttlDeclaration("CreatedAt", 3600))
	for i := 0; i < count; i++ {
		mustInsert(t, s, "app", "sessions", fmt.Sprintf("old-%03d", i),
			map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))})
	}
	mustInsert(t, s, "app", "sessions", "live", map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(now)})
}

func countOf(t *testing.T, s *Server, bucket string) int64 {
	t.Helper()
	counted, err := s.CountCtx(context.Background(), CountRequest{Database: "app", Bucket: bucket})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return counted.Count
}

// TestTTLExpiresWithinOneSweep (spec §10): one sweep deletes what has expired
// and nothing else.
func TestTTLExpiresWithinOneSweep(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	seedSessions(t, s, now, 3)
	pass := s.sweepTTL(context.Background(), now)
	if got := countOf(t, s, "sessions"); got != 1 || documentOf(t, s, "app", "sessions", "live") == nil {
		t.Fatalf("after one sweep (%+v) the bucket holds %d documents, want the live one only", pass, got)
	}
}

// TestSweepDrainsBacklogWithoutWaitingForTheNextTick (spec §10): a backlog
// larger than one round of the walk drains in the one pass, round after
// round, while the budget lasts.
func TestSweepDrainsBacklogWithoutWaitingForTheNextTick(t *testing.T) {
	previous := ttlSweepBatch
	ttlSweepBatch = 7
	t.Cleanup(func() { ttlSweepBatch = previous })
	s := newTestServer(t)
	now := time.Now()
	seedSessions(t, s, now, 50)
	pass := s.sweepTTL(context.Background(), now)
	if got := countOf(t, s, "sessions"); got != 1 || pass.exhausted {
		t.Fatalf("one pass (%+v) left %d documents, want the backlog drained to the live one", pass, got)
	}
}

// TestSweeperEnumeratesOnlyTTLBuckets (the rule-11 guard, S-9): a pass visits
// the buckets that declare a TTL index, never every bucket — at a million
// buckets and a 60 s interval a walk of the bucket registry is ~16,700 visits
// a second to decide there is nothing to do.
func TestSweeperEnumeratesOnlyTTLBuckets(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	for i := 0; i < 1000; i++ {
		options := map[string]interface{}{"indices": []interface{}{"Email"}}
		if i == 17 || i == 700 {
			options = map[string]interface{}{"indices": []interface{}{ttlDeclaration("CreatedAt", 60)}}
		}
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: fmt.Sprintf("b%04d", i), Type: BucketTypeHash, Options: options}); err != nil || !resp.Success {
			t.Fatalf("CreateBucket(%d) = %+v, %v", i, resp, err)
		}
	}
	if pass := s.sweepTTL(context.Background(), time.Now()); pass.visited != 2 {
		t.Fatalf("a pass visited %d buckets, want the 2 that declare a TTL index", pass.visited)
	}
}

// TestSweeperHoldsNoBucketLockWhileDeleting (spec §10, §4 F-8): each delete
// re-enters the write path and takes the bucket's lock itself; a sweeper
// holding it would wait on itself forever. The failure is a hang, so the
// sweep runs under a deadline.
func TestSweeperHoldsNoBucketLockWhileDeleting(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	seedSessions(t, s, now, 5)
	done := make(chan struct{})
	go func() {
		s.sweepTTL(context.Background(), now)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the sweep did not finish in 10 s: it holds a lock its own deletes need")
	}
}

// TestTTLRegistryForgetsADroppedBucket: a dropped bucket leaves the registry,
// so no pass visits it again.
func TestTTLRegistryForgetsADroppedBucket(t *testing.T) {
	s := newTestServer(t)
	seedSessions(t, s, time.Now(), 1)
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "sessions"}); err != nil || !resp.Success {
		t.Fatalf("DeleteBucket = %+v, %v", resp, err)
	}
	if pass := s.sweepTTL(context.Background(), time.Now()); pass.visited != 0 {
		t.Fatalf("a pass after the drop visited %d buckets, want none", pass.visited)
	}
}

// startedTTLServer is a standalone server that has been started, with its
// sweeper ticking every second.
func startedTTLServer(t *testing.T) *Service {
	t.Helper()
	t.Setenv("HOARDDB_TTL_SWEEP_SECONDS", "1")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = freeLoopbackAddr(t)
	qs, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return qs
}

// freeLoopbackAddr is an address nothing is listening on.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("releasing the probe: %v", err)
	}
	return addr
}

// TestSweeperStandaloneNodeExpires (spec §10, §4 F-8): most users run one
// node, with no cluster; a sweeper hung off the cluster passes every cluster
// test and expires nothing here.
func TestSweeperStandaloneNodeExpires(t *testing.T) {
	qs := startedTTLServer(t)
	defer func() { _ = qs.Stop() }()
	seedSessions(t, qs.core, time.Now(), 2)
	deadline := time.Now().Add(10 * time.Second)
	for countOf(t, qs.core, "sessions") != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("after 10 s of a one-second sweeper the bucket still holds %d documents", countOf(t, qs.core, "sessions"))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSweeperStopsOnShutdown: the sweeper's goroutine exits with the server.
func TestSweeperStopsOnShutdown(t *testing.T) {
	before := runtime.NumGoroutine()
	qs := startedTTLServer(t)
	seedSessions(t, qs.core, time.Now(), 1)
	if err := qs.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			if strings.Contains(string(buf[:n]), "ttl") {
				t.Fatalf("a TTL goroutine outlived Stop:\n%s", buf[:n])
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// sweepOnce seeds app/sessions with a one-hour TTL on CreatedAt and the given
// documents, runs one sweep at now, and returns the keys left, sorted.
func sweepOnce(t *testing.T, now time.Time, documents map[string]map[string]interface{}) []string {
	t.Helper()
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "sessions", ttlDeclaration("CreatedAt", 3600))
	for key, doc := range documents {
		mustInsert(t, s, "app", "sessions", key, doc)
	}
	s.sweepTTL(context.Background(), now)
	scanned, err := s.Scan(ScanRequest{Database: "app", Bucket: "sessions", Limit: 1000})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	return documentKeysOf(t, scanned.Documents)
}

// TestTTLNeverExpiresADocumentMissingTheField (spec §10, §6.2.1; guard 4,
// behavioural; the S1). One bucket, one sweep. Row 3 is the one a partial
// implementation deletes: documents 1 and 3 carry one 64-bit integer and
// differ only in the tag byte. The survivors are asserted by identity, not
// count.
func TestTTLNeverExpiresADocumentMissingTheField(t *testing.T) {
	now := time.Now()
	expired := primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))
	left := sweepOnce(t, now, map[string]map[string]interface{}{
		"1-expired-date":  {"CreatedAt": expired},
		"2-missing":       {"Name": "no CreatedAt"},
		"3-same-integer":  {"CreatedAt": int64(expired)},
		"4-explicit-null": {"CreatedAt": nil},
	})
	if strings.Join(left, ",") != "2-missing,3-same-integer,4-explicit-null" {
		t.Fatalf("after one sweep the bucket holds %v, want rows 2, 3 and 4 — only the expired date goes", left)
	}
}

// TestTTLIgnoresNonTemporalValues (spec §10): a boolean, a string, a binary
// beside an expired date — only the date's document goes.
func TestTTLIgnoresNonTemporalValues(t *testing.T) {
	now := time.Now()
	left := sweepOnce(t, now, map[string]map[string]interface{}{
		"date":   {"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))},
		"bool":   {"CreatedAt": true},
		"string": {"CreatedAt": "yesterday"},
		"binary": {"CreatedAt": primitive.Binary{Data: []byte{0x01}}},
	})
	if strings.Join(left, ",") != "binary,bool,string" {
		t.Fatalf("after one sweep the bucket holds %v, want the boolean, the string and the binary", left)
	}
}

// TestTTLExpiresNumericAndDateValuesInTheSameBucket (spec §10): expired and
// live of both kinds in one bucket — the expired of both go, the live of
// both stay.
func TestTTLExpiresNumericAndDateValuesInTheSameBucket(t *testing.T) {
	now := time.Now()
	left := sweepOnce(t, now, map[string]map[string]interface{}{
		"expired-date":    {"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour))},
		"live-date":       {"CreatedAt": primitive.NewDateTimeFromTime(now)},
		"expired-seconds": {"CreatedAt": now.Add(-2 * time.Hour).Unix()},
		"live-seconds":    {"CreatedAt": float64(now.Unix())},
	})
	if strings.Join(left, ",") != "live-date,live-seconds" {
		t.Fatalf("after one sweep the bucket holds %v, want the two live documents", left)
	}
}
