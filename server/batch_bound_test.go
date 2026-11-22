package server

import (
	"bytes"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// longHexRun matches the storage key the old refusal leaked to the client.
var longHexRun = regexp.MustCompile(`[0-9a-f]{16,}`)

// TestOverBudgetBatchRefusalSaysWhatToDo defends CLAUDE.md rule 3:
// a batch over the store's atomic bound is refused whole, and the client text
// names the bound, says nothing was written, says what to send instead, and
// keeps the sentinel that ingest matches to halve a batch. The hex storage key
// belongs in the server log.
func TestOverBudgetBatchRefusalSaysWhatToDo(t *testing.T) {
	engines := []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap}
	for _, engine := range engines {
		t.Run(string(engine), func(t *testing.T) {
			s := newTestServerFloorMemtable(t)
			createTestBucket(t, s, "app", "big", engine)

			payload := strings.Repeat("x", 100*1024)
			documents := make([]map[string]interface{}, 60)
			for i := range documents {
				documents[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%03d", i), "body": payload}
			}
			refusal, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "big", Data: documents})
			if err != nil {
				t.Fatalf("BatchInsert error = %v, want an in-band refusal", err)
			}
			if refusal.Success {
				t.Fatal("60 x 100 KiB was accepted; the atomic bound is about 5 MB")
			}

			byteBound := groupThousands(int(s.kv.MaxBatchBytes()))
			for _, want := range []string{
				storage.ErrBatchTooLarge.Error(),
				byteBound,
				"none of it was written",
				"Send fewer or smaller documents",
				"hoarddb ingest",
			} {
				if !strings.Contains(refusal.Message, want) {
					t.Errorf("refusal %q does not contain %q", refusal.Message, want)
				}
			}
			if hexKey := longHexRun.FindString(refusal.Message); hexKey != "" {
				t.Errorf("refusal shows the storage key %q to the client; it belongs in the log", hexKey)
			}
		})
	}
}

// TestEntryCountRefusalSaysTheEntryCountBound: for small documents the entry
// count, not the byte count, is what a batch runs out of first, so a caller told
// only about bytes would shrink the wrong thing.
func TestEntryCountRefusalSaysTheEntryCountBound(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	createTestBucket(t, s, "app", "tiny", BucketTypeHash)

	documents := make([]map[string]interface{}, 60000)
	for i := range documents {
		documents[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%06d", i), "n": int32(i)}
	}
	refusal, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "tiny", Data: documents})
	if err != nil {
		t.Fatalf("BatchInsert error = %v, want an in-band refusal", err)
	}
	if refusal.Success {
		t.Fatal("60,000 small documents were accepted in one batch; the entry bound is about 52,000")
	}
	for _, want := range []string{
		storage.ErrBatchTooLarge.Error(),
		"none of it was written",
		"entries",
		"Send fewer",
	} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("refusal %q does not contain %q", refusal.Message, want)
		}
	}
	if !strings.Contains(refusal.Message, "entry-count") {
		t.Errorf("refusal %q does not say the entry-count bound was the one reached", refusal.Message)
	}
	if hexKey := longHexRun.FindString(refusal.Message); hexKey != "" {
		t.Errorf("refusal shows the storage key %q to the client", hexKey)
	}
}

// TestOverBudgetBatchRefusalLogsTheStorageKey: the key left the client's message
// for the server log, where an operator can still find where the batch stopped.
func TestOverBudgetBatchRefusalLogsTheStorageKey(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	var logged bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	createTestBucket(t, s, "app", "big", BucketTypeHash)

	payload := strings.Repeat("x", 100*1024)
	documents := make([]map[string]interface{}, 60)
	for i := range documents {
		documents[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%03d", i), "body": payload}
	}
	if _, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "big", Data: documents}); err != nil {
		t.Fatalf("BatchInsert error = %v", err)
	}
	for _, want := range []string{"level=WARN", "database=app", "bucket=big", "count=60", "reached at key"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log %q does not contain %q", logged.String(), want)
		}
	}
}

// TestBatchOverFiveMBCommitsWholeAtTheDefaultMemtable: the shipped 128 MiB
// memtable bounds a batch at about 20 MB, so the 6 MB batch the 32 MiB floor
// refuses commits, all of it.
func TestBatchOverFiveMBCommitsWholeAtTheDefaultMemtable(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "big", BucketTypeHash)

	payload := strings.Repeat("x", 100*1024)
	documents := make([]map[string]interface{}, 60)
	for i := range documents {
		documents[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%03d", i), "body": payload}
	}
	response, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "big", Data: documents})
	if err != nil {
		t.Fatalf("BatchInsert error = %v", err)
	}
	if !response.Success || response.Count != 60 {
		t.Fatalf("60 x 100 KiB at the default memtable: success=%v count=%d message=%q, want all 60 committed", response.Success, response.Count, response.Message)
	}
}

// smallDocumentBatch is count small documents, the shape that reaches the
// store's entry-count bound before its byte bound.
func smallDocumentBatch(database, bucket string, count int) BatchInsertRequest {
	documents := make([]map[string]interface{}, count)
	for i := range documents {
		documents[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%06d", i), "n": int32(i)}
	}
	return BatchInsertRequest{Database: database, Bucket: bucket, Data: documents, WriteConcern: "all"}
}

// entryBoundIn requires frame to be the entry-count refusal and returns the
// store's entry bound it names.
func entryBoundIn(t *testing.T, frame *transport.Frame) int {
	t.Helper()
	answer := string(frame.Payload)
	refusal := errorAnswerOf(t, frame, "an over-bound batch")
	// On a cluster the owner's refusal is write_outcome unknown, not not_applied:
	// the bound follows each member's memtable, so a member with a larger one
	// applies what the owner refused (handlerRefusalWithOutcome).
	if transport.ErrorCode(refusal.Code) != transport.ErrBatchTooLarge || refusal.WriteOutcome != outcomeUnknown {
		t.Fatalf("the over-bound batch was answered code %d, write_outcome %q, want BATCH_TOO_LARGE and %q: %s", refusal.Code, refusal.WriteOutcome, outcomeUnknown, answer)
	}
	if !strings.Contains(answer, "none of it was written") || !strings.Contains(answer, "entry-count") {
		t.Fatalf("the batch was not refused for the entry-count bound: %s", answer)
	}
	named := regexp.MustCompile(`and ([0-9,]+) entries`).FindStringSubmatch(answer)
	if named == nil {
		t.Fatalf("the refusal names no entry bound: %s", answer)
	}
	bound, err := strconv.Atoi(strings.ReplaceAll(named[1], ",", ""))
	if err != nil {
		t.Fatalf("reading the entry bound %q: %v", named[1], err)
	}
	return bound
}

// walSequences is each node's last logged sequence.
func (tc *testCluster) walSequences() []uint64 {
	sequences := make([]uint64, len(tc.nodes))
	for i, n := range tc.nodes {
		sequences[i] = n.server.core.wal.LastSequence()
	}
	return sequences
}

// waitEveryNodeLogged waits until every node's log has grown past before, so
// the entry under test is in each log a restart replays.
func (tc *testCluster) waitEveryNodeLogged(t *testing.T, before []uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for index, n := range tc.nodes {
		for n.server.core.wal.LastSequence() <= before[index] {
			if time.Now().After(deadline) {
				t.Fatalf("node %d never logged the batch: its log stayed at sequence %d", index+1, before[index])
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// requireCounts requires every node's own copy of bucket to hold want, waiting
// up to 60 s for replicas still applying (a batch acknowledged at w=one). It
// reports and goes on, so a run shows the counts after every restart.
func (tc *testCluster) requireCounts(t *testing.T, bucket string, want int64, when string) {
	t.Helper()
	counts := make([]int64, len(tc.nodes))
	deadline := time.Now().Add(60 * time.Second)
	wrong := true
	for wrong && time.Now().Before(deadline) {
		wrong = false
		for index := range tc.nodes {
			counts[index] = tc.bucketCountOn(index, "app", bucket)
			wrong = wrong || counts[index] != want
		}
		if wrong {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if wrong {
		t.Errorf("%s: the nodes hold %v documents of bucket %q, want %d on every node", when, counts, bucket, want)
	}
}

// TestARefusedBatchStaysRefusedOnEveryNodeAfterRestarts covers the
// cluster case, the verifier's reproduction: a batch refused for the entry
// count, with the client told none of it was written, must stay unwritten on
// every node through a restart of each replica and of the owner. The live
// apply writes a read-repair version key per document and replay does not,
// so the same logged entry that was refused live fits at replay.
func TestARefusedBatchStaysRefusedOnEveryNodeAfterRestarts(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

	logged := tc.walSequences()
	entryBoundIn(t, tc.frame(0, transport.OpBatchPut, smallDocumentBatch("app", bucket, 60000)))
	tc.waitEveryNodeLogged(t, logged)
	tc.requireCounts(t, bucket, 0, "after the refusal")

	for _, restart := range []struct {
		index int
		who   string
	}{{2, "a replica"}, {1, "the other replica"}, {0, "the owner"}} {
		tc.restartNode(restart.index)
		tc.requireCounts(t, bucket, 0, "after "+restart.who+" restarted")
	}
}

// TestARefusedQueueBatchDoesNotDoubleAfterARestart is the queue
// variant: a push batch refused for the entry count, split and retried, holds
// the retried entries once after every node restarts. A queue keys an entry
// by position, so a refused batch that replay applied would add a second copy.
func TestARefusedQueueBatchDoesNotDoubleAfterARestart(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeFIFO})

	const pushed = 110000
	refused := smallDocumentBatch("app", bucket, pushed)
	// The refusal is the owner's local decision and needs no replica, so
	// w=all would only let a write-concern timeout answer first.
	refused.WriteConcern = "one"
	logged := tc.walSequences()
	entryBoundIn(t, tc.frame(0, transport.OpBatchPut, refused))
	tc.waitEveryNodeLogged(t, logged)
	for _, half := range [][]map[string]interface{}{refused.Data[:pushed/2], refused.Data[pushed/2:]} {
		tc.ok(0, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: bucket, Data: half, WriteConcern: "one"})
	}
	tc.requireCounts(t, bucket, pushed, "after the retry")
	for index := range tc.nodes {
		tc.restartNode(index)
	}
	tc.requireCounts(t, bucket, pushed, "after every node restarted")
}

// TestABatchJustUnderTheEntryBoundAppliesEverywhereAfterRestarts guards the
// other side of the refused-batch fix: a batch whose live apply fits the entry count
// applies on every node and is still there, once, after every node restarts.
func TestABatchJustUnderTheEntryBoundAppliesEverywhereAfterRestarts(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	probe := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: probe, Type: BucketTypeHash})
	boundProbe := smallDocumentBatch("app", probe, 60000)
	boundProbe.WriteConcern = "one"
	bound := entryBoundIn(t, tc.frame(0, transport.OpBatchPut, boundProbe))

	bucket := "under-" + probe
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	// A document is two entries live, itself and its read-repair version,
	// and the batch adds the state document and Badger's commit marker.
	underBound := (bound - 4) / 2
	// Acknowledged at w=one: under -race a batch this size outlasts the
	// write-concern budget, and requireCounts waits for every copy instead.
	underBatch := smallDocumentBatch("app", bucket, underBound)
	underBatch.WriteConcern = "one"
	frame := tc.ok(0, transport.OpBatchPut, underBatch)
	var answer BatchInsertResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the batch's answer: %v", err)
	}
	if !answer.Success {
		t.Fatalf("a batch of %d documents, %d entries live against a bound of %d, was refused: %s", underBound, 2*underBound+2, bound, answer.Message)
	}
	tc.requireCounts(t, bucket, int64(underBound), "after the batch")
	for index := range tc.nodes {
		tc.restartNode(index)
		tc.requireCounts(t, bucket, int64(underBound), fmt.Sprintf("after node %d restarted", index+1))
	}
}
