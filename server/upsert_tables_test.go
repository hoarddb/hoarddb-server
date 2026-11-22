package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// writeOpcodesOfTheVerbs are the opcodes that put a whole document into a
// hash or btree bucket. Document-placement spec §1.4 lists a table every one of
// them must be in; a miss is silent (an id not minted, a limit skipped, a
// bypass of the internal-field refusals), so each row below is asserted.
var writeOpcodesOfTheVerbs = []transport.Opcode{transport.OpUpsert, transport.OpBatchUpsert, transport.OpBTreeInsert}

// upsertOpcodes are the opcodes this step adds.
var upsertOpcodes = []transport.Opcode{transport.OpUpsert, transport.OpBatchUpsert}

// TestEveryWriteOpcodeIsInEveryTable is T-48: UPSERT, BATCH_UPSERT and
// BTREE_INSERT are in every per-opcode table of document-placement spec §1.4.
// Falling through a table is an answer, so each sub-test asserts the answer
// the table must give, not that the table "does not fail".
func TestEveryWriteOpcodeIsInEveryTable(t *testing.T) {
	t.Run("Opcode.String", func(t *testing.T) {
		names := map[transport.Opcode]string{
			transport.OpUpsert: "UPSERT", transport.OpUpsertOK: "UPSERT_OK",
			transport.OpBatchUpsert: "BATCH_UPSERT", transport.OpBatchUpsertOK: "BATCH_UPSERT_OK",
			transport.OpBTreeInsert: "BTREE_INSERT",
		}
		for op, want := range names {
			if got := op.String(); got != want {
				t.Errorf("opcode 0x%02X String() = %q, want %q", uint8(op), got, want)
			}
		}
	})

	t.Run("Opcode.IsReadOnly", func(t *testing.T) {
		for _, op := range writeOpcodesOfTheVerbs {
			if op.IsReadOnly() {
				t.Errorf("%s is read-only: a write would be served locally, never replicated", op)
			}
		}
	})

	t.Run("replicationRequired", func(t *testing.T) {
		for _, op := range writeOpcodesOfTheVerbs {
			if !replicationRequired(op) {
				t.Errorf("replicationRequired(%s) = false: the write would not reach the replica set", op)
			}
		}
	})

	t.Run("authz opRules", func(t *testing.T) {
		for _, op := range writeOpcodesOfTheVerbs {
			rule, ruled := opRules[op]
			if !ruled || rule != (OpRule{ActionWrite, ScopeDatabase}) {
				t.Errorf("opRules[%s] = %+v (present %t), want a database-scoped write", op, rule, ruled)
			}
		}
		for _, op := range []transport.Opcode{transport.OpUpsertOK, transport.OpBatchUpsertOK} {
			rule, ruled := opRules[op]
			if !ruled || rule != (OpRule{ActionNone, ScopeNone}) {
				t.Errorf("opRules[%s] = %+v (present %t), want the response row {None, None}", op, rule, ruled)
			}
		}
	})

	t.Run("localHandlers", func(t *testing.T) {
		for _, op := range writeOpcodesOfTheVerbs {
			if !implementsOpcode(op) {
				t.Errorf("implementsOpcode(%s) = false: a forwarded entry would be refused", op)
			}
		}
	})

	t.Run("refuseClientAdmitted", func(t *testing.T) {
		for _, op := range []transport.Opcode{transport.OpUpsert, transport.OpBTreeInsert} {
			frame := transport.NewFrame(op, mustMarshalBSON(t, bson.D{{Key: "database", Value: "app"}, {Key: "admitted", Value: true}}))
			if err := refuseClientAdmitted(frame); !errors.Is(err, ErrAdmittedSetByClient) {
				t.Errorf("refuseClientAdmitted(%s with admitted: true) = %v, want ErrAdmittedSetByClient", op, err)
			}
		}
	})

	t.Run("refuseClientAdmitted for a batch is the typed refusal", func(t *testing.T) {
		// BatchInsertRequest has no admitted field, so a client-sent flag is
		// ignored and the typed-field check still runs on every document.
		s := newTestServer(t)
		createTypedBucket(t, s, "User", ageFields(), "")
		handler := NewHandler(s)
		payload := mustMarshalBSON(t, bson.D{
			{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "admitted", Value: true},
			{Key: "data", Value: bson.A{bson.M{"_id_": "k1", "Age": "thirty"}}},
		})
		answer := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpBatchUpsert, payload))
		if !answer.refused || !strings.Contains(answer.message, `Field "Age"`) {
			t.Errorf("a BATCH_UPSERT with admitted: true and a string Age answered %+v, want the FIELD_TYPE refusal", answer)
		}
	})

	t.Run("refuseClientRepairBound", func(t *testing.T) {
		for _, op := range []transport.Opcode{transport.OpUpsert, transport.OpBTreeInsert} {
			frame := transport.NewFrame(op, mustMarshalBSON(t, bson.D{{Key: "database", Value: "app"}, {Key: "repair_bound", Value: int64(5)}}))
			if err := refuseClientRepairBound(frame); !errors.Is(err, ErrRepairBoundSetByClient) {
				t.Errorf("refuseClientRepairBound(%s with repair_bound: 5) = %v, want ErrRepairBoundSetByClient", op, err)
			}
		}
	})

	t.Run("resolveWritePayload mints the id and fixes now()", func(t *testing.T) {
		single := marshalRequest(t, InsertRequest{Database: "app", Bucket: "events", Data: map[string]interface{}{"at": nowArg()}})
		batch := marshalRequest(t, BatchInsertRequest{Database: "app", Bucket: "events", Data: []map[string]interface{}{
			{"at": nowArg()}, {"at": nowArg()},
		}})
		for _, op := range []transport.Opcode{transport.OpUpsert, transport.OpBTreeInsert} {
			rewritten, err := resolveWritePayload(op, single, true)
			if err != nil || rewritten == nil {
				t.Fatalf("resolveWritePayload(%s) = %v, %v; want a rewrite with a minted _id_ and a fixed time", op, rewritten, err)
			}
			var req InsertRequest
			if err := bson.Unmarshal(rewritten, &req); err != nil {
				t.Fatalf("decoding the rewrite of %s: %v", op, err)
			}
			if id, _ := req.Data["_id_"].(string); id == "" {
				t.Errorf("%s: no _id_ was minted: %v", op, req.Data)
			}
			oneInstant(t, op.String()+" rewrite", datetimesIn(t, rewritten), 1)
		}
		rewritten, err := resolveWritePayload(transport.OpBatchUpsert, batch, true)
		if err != nil || rewritten == nil {
			t.Fatalf("resolveWritePayload(BATCH_UPSERT) = %v, %v; want a rewrite", rewritten, err)
		}
		var req BatchInsertRequest
		if err := bson.Unmarshal(rewritten, &req); err != nil {
			t.Fatalf("decoding the batch rewrite: %v", err)
		}
		for position, document := range req.Data {
			if id, _ := document["_id_"].(string); id == "" {
				t.Errorf("BATCH_UPSERT document %d got no minted _id_: %v", position+1, document)
			}
		}
		oneInstant(t, "BATCH_UPSERT rewrite", datetimesIn(t, rewritten), 2)
	})

	t.Run("document limit", func(t *testing.T) {
		const limit = 1000
		_, handler := documentLimitServer(t, limit)
		oversized := docOfEncodedSize(limit + 200)
		for _, op := range []transport.Opcode{transport.OpUpsert, transport.OpBTreeInsert} {
			frame := sendFrame(t, handler, op, InsertRequest{Database: "app", Bucket: "docs", Key: "big", Data: oversized})
			if frame.Opcode != transport.OpError {
				t.Errorf("%s of a %d-byte document under a %d-byte limit answered %s, want a refusal", op, limit+200, limit, frame.Opcode)
			}
		}
		frame := sendFrame(t, handler, transport.OpBatchUpsert, BatchInsertRequest{Database: "app", Bucket: "docs", Data: []map[string]interface{}{oversized}})
		if frame.Opcode != transport.OpError {
			t.Errorf("BATCH_UPSERT of an over-limit document answered %s, want a refusal", frame.Opcode)
		}
	})

	t.Run("an UPSERT never carries the deferred type limit", func(t *testing.T) {
		// Only a PUT whose bucket this node cannot resolve is deferred to the
		// owner (Cluster.refuseOwnerPutSize reads the mark for OpPut alone).
		s, _ := documentLimitServer(t, 1000)
		payload := marshalRequest(t, InsertRequest{Database: "app", Bucket: "unknown", Key: "k", Data: docOfEncodedSize(1200)})
		deferred, _ := s.refuseOversizedWrite(transport.NewFrame(transport.OpPut, payload))
		if !deferred {
			t.Fatal("control: a PUT into a bucket this node cannot resolve was not deferred; the premise of this test is gone")
		}
		for _, op := range upsertOpcodes {
			deferred, _ := s.refuseOversizedWrite(transport.NewFrame(op, payload))
			if deferred {
				t.Errorf("%s was marked TypeLimitDeferred: the owner would measure it as a blob put", op)
			}
		}
	})

	t.Run("blob bucket", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "files", Type: BucketTypeBlob})
		frame := sendFrame(t, handler, transport.OpUpsert, InsertRequest{Database: "app", Bucket: "files", Key: "k", Data: map[string]interface{}{"a": 1}})
		answer := readScanAnswer(t, frame)
		if !answer.refused || !strings.Contains(answer.message, "use put") {
			t.Errorf("UPSERT into a blob bucket answered %+v, want the refusal that says 'use put'", answer)
		}
	})

	t.Run("requestDepthLimit", func(t *testing.T) {
		if got, want := requestDepthLimit(transport.OpUpsert), requestDepthLimit(transport.OpPut); got != want {
			t.Errorf("requestDepthLimit(UPSERT) = %d, want %d (PUT's)", got, want)
		}
		if got, want := requestDepthLimit(transport.OpBatchUpsert), requestDepthLimit(transport.OpBatchPut); got != want {
			t.Errorf("requestDepthLimit(BATCH_UPSERT) = %d, want %d (BATCH_PUT's)", got, want)
		}
		if got, want := requestDepthLimit(transport.OpBTreeInsert), requestDepthLimit(transport.OpPut); got != want {
			t.Errorf("requestDepthLimit(BTREE_INSERT) = %d, want %d (PUT's)", got, want)
		}
	})

	t.Run("uniquePrecheck", func(t *testing.T) {
		s := newTestServer(t)
		if _, err := s.CreateDatabase("app"); err != nil {
			t.Fatalf("CreateDatabase: %v", err)
		}
		created, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Account", Type: BucketTypeHash,
			Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{map[string]interface{}{"field": "Email", "unique": true}}}})
		if err != nil || !created.Success {
			t.Fatalf("creating Account: %+v, %v", created, err)
		}
		mustInsert(t, s, "app", "Account", "first", map[string]interface{}{"Email": "a@example.test"})
		cluster := &Cluster{s: s}

		single := marshalRequest(t, InsertRequest{Database: "app", Bucket: "Account", Key: "second", Data: map[string]interface{}{"Email": "a@example.test"}})
		if _, refused := cluster.uniquePrecheck(ReplEntry{Op: int32(transport.OpUpsert), Bucket: "Account", Payload: single}); !refused {
			t.Error("uniquePrecheck let a duplicate unique value through on UPSERT: it would be logged, applied and shipped")
		}
		batch := marshalRequest(t, BatchInsertRequest{Database: "app", Bucket: "Account", Data: []map[string]interface{}{
			{"_id_": "third", "Email": "b@example.test"}, {"_id_": "fourth", "Email": "a@example.test"},
		}})
		refusal, refused := cluster.uniquePrecheck(ReplEntry{Op: int32(transport.OpBatchUpsert), Bucket: "Account", Payload: batch})
		if !refused {
			t.Fatal("uniquePrecheck let a duplicate unique value through on BATCH_UPSERT")
		}
		var answer ErrorResponse
		if err := bson.Unmarshal(refusal.Payload, &answer); err != nil || !strings.Contains(answer.Message, "a@example.test") {
			t.Errorf("the BATCH_UPSERT refusal = %q (%v), want it to name the duplicate value", answer.Message, err)
		}
		documents, err := precheckedDocuments(transport.OpBatchUpsert, batch)
		if err != nil || len(documents) != 2 || documents[1].position != 2 || documents[1].key != "fourth" {
			t.Errorf("precheckedDocuments(BATCH_UPSERT) = %+v, %v; want two documents, the second at position 2", documents, err)
		}
	})

	t.Run("isDataWrite", func(t *testing.T) {
		for _, op := range writeOpcodesOfTheVerbs {
			if !isDataWrite(op) {
				t.Errorf("isDataWrite(%s) = false: its refusal would not say whether the write happened", op)
			}
		}
	})

	t.Run("isOverBoundReply", func(t *testing.T) {
		applied := &applyOutcome{overBound: &batchTooLargeRefusal{}}
		if !isOverBoundReply(transport.OpBatchUpsertOK, applied) {
			t.Error("isOverBoundReply(BATCH_UPSERT_OK) = false: an over-bound batch would be answered as a success")
		}
	})

	t.Run("writeOpcodeFeatures", func(t *testing.T) {
		for _, op := range writeOpcodesOfTheVerbs {
			if feature, present := writeOpcodeFeatures[op]; !present || feature != everyBuildHasIt {
				t.Errorf("writeOpcodeFeatures[%s] = %q (present %t), want everyBuildHasIt", op, feature, present)
			}
		}
	})

	t.Run("batchKeyFloor and batchTooLargeReason", func(t *testing.T) {
		payload := marshalRequest(t, BatchInsertRequest{Database: "app", Bucket: "docs", Data: []map[string]interface{}{{"_id_": "a"}, {"_id_": "b"}, {"_id_": "c"}}})
		entry := ReplEntry{Op: int32(transport.OpBatchUpsert), Bucket: "docs", Payload: payload}
		if got := batchKeyFloor(entry); got != 8 {
			t.Errorf("batchKeyFloor(BATCH_UPSERT of 3) = %d, want 8 (two keys per document and two more)", got)
		}
		if reason := batchTooLargeReason(64, entry); !strings.Contains(reason, " keys") {
			t.Errorf("batchTooLargeReason(BATCH_UPSERT) = %q, want the batch reason that sizes the keys", reason)
		}
	})

	t.Run("batchCarried", func(t *testing.T) {
		// A recorded decision (build doc BD-8): BATCH carries no document-level
		// write today, not even PUT. Changing this is a stop-and-ask (SA-5).
		for _, op := range append([]transport.Opcode{transport.OpPut}, writeOpcodesOfTheVerbs...) {
			if batchCarried(op) {
				t.Errorf("batchCarried(%s) = true; BD-8 records that Phases 1 and 2 do not carry it", op)
			}
		}
	})
}
