package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Document-placement build doc step 5: insert refuses an _id_ the bucket
// already holds (spec §6.3). MySQL's INSERT refuses a duplicate primary key
// with error 1062 and MongoDB's insertOne refuses a duplicate _id with E11000;
// neither replaces the stored row or document, and neither does hoardDB now.
// upsert is the replace verb.

// insertExistsServer is a standalone node with database "app" and bucket
// "User" of the given type, answering through NewHandler.
func insertExistsServer(t *testing.T, bucketType BucketType) (*Server, transport.RequestHandler) {
	t.Helper()
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: bucketType})
	return s, handler
}

// requireDocumentExists decodes a refusal and fails unless it is
// DOCUMENT_EXISTS with write_outcome not_applied.
func requireDocumentExists(t *testing.T, frame *transport.Frame, what string) ErrorResponse {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("%s answered %s %s; want an ERROR frame, DOCUMENT_EXISTS", what, frame.Opcode, string(frame.Payload))
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("%s: decoding the refusal: %v", what, err)
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrDocumentExists {
		t.Fatalf("%s: code %#x %q; want DOCUMENT_EXISTS (%#x)", what, refusal.Code, refusal.Message, int(transport.ErrDocumentExists))
	}
	if refusal.WriteOutcome != outcomeNotApplied {
		t.Errorf("%s: write_outcome %q; want %q", what, refusal.WriteOutcome, outcomeNotApplied)
	}
	return refusal
}

// documentExistsText is spec §6.3's refusal for an id in a bucket.
func documentExistsText(database, bucket, id string) string {
	return fmt.Sprintf("Document %q already exists in bucket %q (database %q). To replace it, run db.%s.upsert({_id_: %q, ...}); "+
		"to change some fields, run db.%s.update(%q, {$set: {...}}). "+
		"If you are retrying an insert whose answer you did not get, it may have been stored: retry with upsert.",
		id, bucket, database, bucket, id, bucket, id)
}

// TestInsertRefusesExistingID is T-01 (and T-64's step-5 assertion: a PUT
// with no intent field refuses): the second PUT of {_id_: "jane"} is refused
// DOCUMENT_EXISTS, not_applied, and the stored document is the first.
func TestInsertRefusesExistingID(t *testing.T) {
	s, handler := insertExistsServer(t, BucketTypeHash)
	first := sendFrame(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "User",
		Data: map[string]interface{}{"_id_": "jane", "v": int32(1)}})
	if first.Opcode == transport.OpError {
		t.Fatalf("the first insert of jane was refused: %s", first.Payload)
	}
	second := sendFrame(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "User",
		Data: map[string]interface{}{"_id_": "jane", "v": int32(2)}})
	requireDocumentExists(t, second, "the second insert of jane")
	if stored := documentOf(t, s, "app", "User", "jane"); fmt.Sprint(stored["v"]) != "1" {
		t.Fatalf("after the refused insert jane is %v; want the first document, v 1", stored)
	}
}

// TestInsertExistsMessageNamesTheFix is T-02: the refusal is spec §6.3's three
// sentences, built from the bucket, the database and the id only. It names
// the verbs that do the job (upsert, update) and the retry rule, and never
// echoes a field value of the document it refused.
func TestInsertExistsMessageNamesTheFix(t *testing.T) {
	const secret = "Zanzibar-7731-private"
	_, handler := insertExistsServer(t, BucketTypeHash)
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "User",
		Data: map[string]interface{}{"_id_": "jane", "Note": "first"}})
	refusal := requireDocumentExists(t, sendFrame(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "User",
		Data: map[string]interface{}{"_id_": "jane", "Note": secret}}), "the second insert of jane")
	if want := documentExistsText("app", "User", "jane"); refusal.Message != want {
		t.Errorf("refusal text:\n got: %s\nwant: %s", refusal.Message, want)
	}
	if strings.Contains(refusal.Message, secret) || strings.Contains(refusal.Message, "first") {
		t.Errorf("the refusal echoes the document's content: %q", refusal.Message)
	}
}

// TestBTreeInsertRefusesExistingKey is T-07: on a btree bucket BTREE_INSERT
// is insert, as PUT is, so each refuses a key the bucket holds.
func TestBTreeInsertRefusesExistingKey(t *testing.T) {
	s, handler := insertExistsServer(t, BucketTypeBTree)
	handlerSend(t, handler, transport.OpBTreeInsert, InsertRequest{Database: "app", Bucket: "User", Key: "k1",
		Data: map[string]interface{}{"v": int32(1)}})
	for _, op := range []transport.Opcode{transport.OpBTreeInsert, transport.OpPut} {
		requireDocumentExists(t, sendFrame(t, handler, op, InsertRequest{Database: "app", Bucket: "User", Key: "k1",
			Data: map[string]interface{}{"v": int32(2)}}), op.String()+" of the held key k1")
	}
	if stored := documentOf(t, s, "app", "User", "k1"); fmt.Sprint(stored["v"]) != "1" {
		t.Fatalf("after the refused inserts k1 is %v; want v 1", stored)
	}
}

// requireAtomicBatchRefusal checks spec §6.6's answer to a BATCH_PUT of
// [new1, existing, new2]: DOCUMENT_EXISTS, not_applied, and one result per
// position, the held one carrying the code and the others not applied.
func requireAtomicBatchRefusal(t *testing.T, frame *transport.Frame) {
	t.Helper()
	refusal := requireDocumentExists(t, frame, "the BATCH_PUT naming a held _id_")
	if !strings.Contains(refusal.Message, `"existing"`) {
		t.Errorf("the batch refusal does not name the held _id_: %q", refusal.Message)
	}
	if len(refusal.Results) != 3 {
		t.Fatalf("the refusal carries %d results; want 3, one per position: %+v", len(refusal.Results), refusal.Results)
	}
	const otherMessage = "not applied: another document in this batch already exists"
	for index, want := range []struct {
		id      string
		code    int32
		message string
	}{{"new1", 0, otherMessage}, {"existing", int32(transport.ErrDocumentExists), ""}, {"new2", 0, otherMessage}} {
		got := refusal.Results[index]
		if got.ID != want.id || got.WriteOutcome != outcomeNotApplied || got.Code != want.code {
			t.Errorf("result %d = %+v; want id %q, not_applied, code %#x", index+1, got, want.id, want.code)
		}
		if want.message != "" && got.Message != want.message {
			t.Errorf("result %d says %q; want %q", index+1, got.Message, want.message)
		}
	}
}

func atomicBatch() []map[string]interface{} {
	return []map[string]interface{}{
		{"_id_": "new1", "v": int32(1)}, {"_id_": "existing", "v": int32(2)}, {"_id_": "new2", "v": int32(3)},
	}
}

// TestBatchPutWithExistingIDIsAtomic is T-04: a BATCH_PUT of [new1, existing,
// new2] stores nothing and is refused whole (owner ruling R4-B2, as MySQL
// refuses a multi-row INSERT with a duplicate key). The bucket's mark moves
// past the refused entry, which is consumed as a unique-index refusal is, and
// on a two-node cluster the replica applies nothing of it either.
func TestBatchPutWithExistingIDIsAtomic(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		s, handler := insertExistsServer(t, BucketTypeHash)
		handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "User",
			Data: map[string]interface{}{"_id_": "existing", "v": int32(0)}})
		requireAtomicBatchRefusal(t, sendFrame(t, handler, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: atomicBatch()}))
		for _, id := range []string{"new1", "new2"} {
			if documentOf(t, s, "app", "User", id) != nil {
				t.Errorf("the refused batch stored %s", id)
			}
		}
		if stored := documentOf(t, s, "app", "User", "existing"); fmt.Sprint(stored["v"]) != "0" {
			t.Errorf("the refused batch changed existing to %v", stored)
		}
	})

	t.Run("two-node cluster", func(t *testing.T) {
		tc := newTestCluster(t, 2, 2)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		owner := ownerIndexOf(t, tc, testBucket)
		replica := (owner + 1) % 2
		requireEveryCopy(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"_id_": "existing", "v": int32(0)}, WriteConcern: "all"}), "the write of existing")
		ownerCore, replicaCore := tc.nodes[owner].server.core, tc.nodes[replica].server.core
		markBefore := bucketStateOf(t, ownerCore, testDatabase, testBucket).AppliedSeq
		replicaLogged := replicaCore.wal.LastSequence()

		requireAtomicBatchRefusal(t, tc.frame(owner, transport.OpBatchPut, BatchInsertRequest{Database: testDatabase, Bucket: testBucket, Data: atomicBatch()}))

		if mark := bucketStateOf(t, ownerCore, testDatabase, testBucket).AppliedSeq; mark <= markBefore {
			t.Errorf("the owner's mark stayed at %d after the refused batch; want it past the consumed entry", mark)
		}
		waitForWALSequence(t, tc.nodes[replica], replicaLogged+1)
		deadline := time.Now().Add(10 * time.Second)
		for bucketStateOf(t, replicaCore, testDatabase, testBucket).AppliedSeq < replicaCore.wal.LastSequence() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		for index, core := range map[int]*Server{owner: ownerCore, replica: replicaCore} {
			for _, id := range []string{"new1", "new2"} {
				if documentOf(t, core, testDatabase, testBucket, id) != nil {
					t.Errorf("node %d stored %s of the refused batch", index+1, id)
				}
			}
			if stored := documentOf(t, core, testDatabase, testBucket, "existing"); fmt.Sprint(stored["v"]) != "0" {
				t.Errorf("node %d holds existing as %v after the refused batch", index+1, stored)
			}
		}
	})
}

// TestConcurrentInsertsOfOneIDThroughTwoNodesOneWins is spec §6.4 (T-27's live
// half): two inserts of one _id_ sent at once through two entry nodes that are
// not the owner are forwarded to the owner, whose existence check inside the
// apply, ordered by the apply ticket, refuses the second. Every round gives
// exactly one success and one DOCUMENT_EXISTS, and every copy holds the
// winner's document.
func TestConcurrentInsertsOfOneIDThroughTwoNodesOneWins(t *testing.T) {
	const rounds = 200
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	owner := ownerIndexOf(t, tc, testBucket)
	entries := []int{(owner + 1) % 3, (owner + 2) % 3}
	clients := []*transport.DriverClient{tc.dial(entries[0]), tc.dial(entries[1])}
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	send := func(client *transport.DriverClient, id string, from int) *transport.Frame {
		payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"_id_": id, "from": int32(from)}, WriteConcern: "all"})
		if err != nil {
			t.Errorf("marshal: %v", err)
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		frame, err := client.SendRequest(ctx, transport.OpPut, payload)
		if err != nil {
			t.Errorf("insert of %s through node %d: %v", id, entries[from]+1, err)
			return nil
		}
		return frame
	}

	for round := 0; round < rounds; round++ {
		id := fmt.Sprintf("race-%03d", round)
		answers := make([]*transport.Frame, 2)
		var wg sync.WaitGroup
		for from := range clients {
			wg.Add(1)
			go func(from int) {
				defer wg.Done()
				answers[from] = send(clients[from], id, from)
			}(from)
		}
		wg.Wait()
		if answers[0] == nil || answers[1] == nil {
			t.FailNow()
		}
		won := -1
		for from, answer := range answers {
			if answer.Opcode != transport.OpError {
				if won != -1 {
					t.Fatalf("round %d: both inserts of %s succeeded", round, id)
				}
				won = from
				continue
			}
			var refusal ErrorResponse
			if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
				t.Fatalf("round %d: decoding the refusal: %v", round, err)
			}
			if transport.ErrorCode(refusal.Code) != transport.ErrDocumentExists {
				t.Fatalf("round %d: an insert of %s was refused %#x %q; want DOCUMENT_EXISTS", round, id, refusal.Code, refusal.Message)
			}
		}
		if won == -1 {
			t.Fatalf("round %d: both inserts of %s were refused", round, id)
		}
		for index := range tc.nodes {
			if stored := documentOf(t, tc.nodes[index].server.core, testDatabase, testBucket, id); fmt.Sprint(stored["from"]) != fmt.Sprint(won) {
				t.Fatalf("round %d: node %d holds %s as %v; want the winner's document (from %d)", round, index+1, id, stored, won)
			}
		}
	}
}

// TestOwedInsertReplayedBeneathMarkIsRefused is T-06, a replay witness (spec
// §6.5 items 3 and 4): replay runs an insert through the same handler as the
// live write, so an insert whose _id_ the bucket holds is refused there too.
//
// Above the mark (an insert logged and not applied before a crash, placed in
// the log by hand) the parent build replayed it as a replace; it is now
// refused, counted rejected_by_constraint, and the held document stays.
// Beneath the mark (an insert the owner's store failed, owed to its replay)
// the recorded-failure rule already refused it at the parent: the key was
// written since, so the owed write is not applied over it. That half is a
// regression guard.
func TestOwedInsertReplayedBeneathMarkIsRefused(t *testing.T) {
	t.Run("above the mark", func(t *testing.T) {
		recorder := recordClusterLogs(t)
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		owner := ownerIndexOf(t, tc, testBucket)
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"_id_": "x", "v": "held"}})

		replayed, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"_id_": "x", "v": "replayed"}})
		if err != nil {
			t.Fatalf("encoding the insert: %v", err)
		}
		restarted := restartNodeAfterEditingItsWAL(t, tc, owner, func(wal *storage.WAL) {
			if _, err := wal.AppendFrom(testBucket, byte(transport.OpPut), []byte("x"), replayed,
				storage.WALOrigin{Kind: storage.WALOriginOwn}); err != nil {
				t.Fatalf("placing the insert in the owner's log: %v", err)
			}
		})
		if stored := documentOf(t, restarted.server.core, testDatabase, testBucket, "x"); fmt.Sprint(stored["v"]) != "held" {
			t.Fatalf("after replay the owner holds x as %v; want the held document: the replayed insert replaced it", stored)
		}
		lines := recorder.matching(slog.LevelInfo, "replayed write-ahead log")
		if len(lines) == 0 {
			t.Fatal("the restarted owner logged no replay summary")
		}
		if last := lines[len(lines)-1].attrs; last["rejected_by_constraint"] != "1" || last["skipped"] != "0" {
			t.Fatalf("replay reported %v; want rejected_by_constraint=1 and skipped=0", last)
		}
	})

	t.Run("beneath the mark", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		owner := ownerIndexOf(t, tc, testBucket)
		ownerCore := tc.nodes[owner].server.core

		failed := failNextApplyOn(t, ownerCore.kv, anySeq)
		tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"_id_": "x", "v": "owed"}})
		if failed.Load() == 0 {
			t.Fatal("setup: the insert of x never reached the owner's store")
		}
		if stored := documentOf(t, ownerCore, testDatabase, testBucket, "x"); stored != nil {
			t.Fatalf("setup: the owner holds x (%v) although its store failed the insert", stored)
		}
		mustInsert(t, ownerCore, testDatabase, testBucket, "x", map[string]interface{}{"v": "held"})
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"_id_": "later", "v": "moves the mark"}})

		restarted := tc.restartNode(owner)
		if stored := documentOf(t, restarted.server.core, testDatabase, testBucket, "x"); fmt.Sprint(stored["v"]) != "held" {
			t.Fatalf("after replay the owner holds x as %v; want the held document: the owed insert replaced it", stored)
		}
	})
}
