package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// documentExistsWire is DOCUMENT_EXISTS as a driver reads it (build doc BD-1).
// The tests decode into the local structs below, not the server's own types,
// so they pin the wire shape a driver sees.
const documentExistsWire = 0x12

// wireResult is one entry of a bulk answer's results (build doc BD-5).
type wireResult struct {
	ID           string `bson:"id"`
	WriteOutcome string `bson:"write_outcome"`
	Code         int32  `bson:"code"`
	Message      string `bson:"message"`
}

// wireBatchAnswer is what a BATCH_PUT_OK or BATCH_UPSERT_OK carries.
type wireBatchAnswer struct {
	IDs     []string     `bson:"ids"`
	Count   int32        `bson:"count"`
	Success bool         `bson:"success"`
	Results []wireResult `bson:"results"`
}

// wireBatchRefusal is what an OpError frame of a refused bulk write carries.
type wireBatchRefusal struct {
	Code         int          `bson:"code"`
	Message      string       `bson:"message"`
	WriteOutcome string       `bson:"write_outcome"`
	Results      []wireResult `bson:"results"`
}

func decodeBatchAnswer(t *testing.T, frame *transport.Frame, want transport.Opcode) wireBatchAnswer {
	t.Helper()
	if frame.Opcode != want {
		t.Fatalf("answered %s, want %s: %s", frame.Opcode, want, frame.Payload)
	}
	var answer wireBatchAnswer
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the %s answer: %v", want, err)
	}
	return answer
}

func decodeBatchRefusal(t *testing.T, frame *transport.Frame) wireBatchRefusal {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("answered %s, want an ERROR frame: %s", frame.Opcode, frame.Payload)
	}
	var refusal wireBatchRefusal
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	return refusal
}

// repeatedIDBatch names "a" at positions 1 and 3.
func repeatedIDBatch(database, bucket string) BatchInsertRequest {
	return BatchInsertRequest{Database: database, Bucket: bucket, Data: []map[string]interface{}{
		{"_id_": "a", "v": int32(1)},
		{"_id_": "b", "v": int32(2)},
		{"_id_": "a", "v": int32(3)},
	}}
}

// requireRepeatedIDRefusal checks the refusal of repeatedIDBatch: the code, the
// outcome, and one result per position with only position 3 carrying a code.
func requireRepeatedIDRefusal(t *testing.T, refusal wireBatchRefusal) {
	t.Helper()
	if refusal.Code != documentExistsWire || refusal.WriteOutcome != outcomeNotApplied {
		t.Fatalf("code %d, write_outcome %q, want DOCUMENT_EXISTS (%d) and not_applied: %s", refusal.Code, refusal.WriteOutcome, documentExistsWire, refusal.Message)
	}
	for _, want := range []string{`"a"`, "positions 1, 3"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("the refusal %q does not name %s", refusal.Message, want)
		}
	}
	if len(refusal.Results) != 3 {
		t.Fatalf("results has %d entries, want one per position (3): %+v", len(refusal.Results), refusal.Results)
	}
	for index, entry := range refusal.Results {
		if entry.WriteOutcome != outcomeNotApplied {
			t.Errorf("results[%d].write_outcome = %q, want not_applied", index, entry.WriteOutcome)
		}
		if index != 2 && entry.Code != 0 {
			t.Errorf("results[%d].code = %d, want none: only the repeated position is refused", index, entry.Code)
		}
	}
	repeat := refusal.Results[2]
	if repeat.Code != documentExistsWire || repeat.ID != "a" ||
		!strings.Contains(repeat.Message, "the same `_id_` appears earlier in this batch at position 1") {
		t.Errorf("results[2] = %+v, want DOCUMENT_EXISTS for \"a\" saying the same `_id_` appears earlier in this batch at position 1", repeat)
	}
}

// TestBatchPutDuplicateInRequestRefusedAtDoor is T-04b. A BATCH_PUT naming one
// _id_ twice is refused whole before anything is logged. The cluster half
// proves the door (no log entry on any node); the standalone half proves the
// handler's own call, because a standalone node's door runs only for a now().
func TestBatchPutDuplicateInRequestRefusedAtDoor(t *testing.T) {
	t.Run("standalone node", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})

		frame := sendOrderFrame(t, rootSession(), handler, transport.OpBatchPut, mustMarshalBSON(t, repeatedIDBatch("app", "docs")))
		requireRepeatedIDRefusal(t, decodeBatchRefusal(t, frame))
		for _, id := range []string{"a", "b"} {
			if document := documentOf(t, s, "app", "docs", id); document != nil {
				t.Errorf("%q was stored by a refused batch: %v", id, document)
			}
		}
	})

	t.Run("cluster member", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		owner := tc.nodes[0].server.core.cluster
		bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

		before := tc.walSequences()
		requireRepeatedIDRefusal(t, decodeBatchRefusal(t, tc.frame(0, transport.OpBatchPut, repeatedIDBatch("app", bucket))))
		after := tc.walSequences()
		for index := range before {
			if after[index] != before[index] {
				t.Errorf("node %d logged an entry for the refused batch: sequence %d, was %d", index+1, after[index], before[index])
			}
		}
	})
}

// TestBatchUpsertDuplicateLastWins is T-53: a BATCH_UPSERT keeps the last
// occurrence of a repeated _id_ and answers one result per position. The
// cluster half is the one a door that rewrote the payload would break.
func TestBatchUpsertDuplicateLastWins(t *testing.T) {
	requireLastWins := func(t *testing.T, answer wireBatchAnswer) {
		t.Helper()
		if len(answer.IDs) != 3 {
			t.Errorf("ids has %d entries, want 3: %v", len(answer.IDs), answer.IDs)
		}
		if len(answer.Results) != 3 {
			t.Fatalf("results has %d entries, want one per position (3): %+v", len(answer.Results), answer.Results)
		}
		for index, entry := range answer.Results {
			if entry.WriteOutcome != outcomeApplied || entry.Code != 0 {
				t.Errorf("results[%d] = %+v, want applied with no code", index, entry)
			}
		}
		if !strings.Contains(answer.Results[0].Message, "replaced by the document at position 3 of this batch") {
			t.Errorf("results[0].message = %q, want it to say it was replaced by the document at position 3 of this batch", answer.Results[0].Message)
		}
	}

	t.Run("standalone node", func(t *testing.T) {
		s := newTestServer(t)
		handler := NewHandler(s)
		handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})

		frame := sendOrderFrame(t, rootSession(), handler, transport.OpBatchUpsert, mustMarshalBSON(t, repeatedIDBatch("app", "docs")))
		requireLastWins(t, decodeBatchAnswer(t, frame, transport.OpBatchUpsertOK))
		if document := documentOf(t, s, "app", "docs", "a"); document["v"] != int32(3) {
			t.Errorf("a reads %v, want the position-3 document (v 3)", document)
		}
	})

	t.Run("cluster member", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		owner := tc.nodes[0].server.core.cluster
		bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})

		requireLastWins(t, decodeBatchAnswer(t, tc.frame(0, transport.OpBatchUpsert, repeatedIDBatch("app", bucket)), transport.OpBatchUpsertOK))
	})
}

// TestBatchResultsOnePerPosition: a BATCH_PUT of three new ids answers three
// applied results carrying their ids, and three ids.
func TestBatchResultsOnePerPosition(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})

	request := BatchInsertRequest{Database: "app", Bucket: "docs", Data: []map[string]interface{}{
		{"_id_": "x", "v": int32(1)}, {"_id_": "y", "v": int32(2)}, {"_id_": "z", "v": int32(3)},
	}}
	answer := decodeBatchAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpBatchPut, mustMarshalBSON(t, request)), transport.OpBatchPutOK)
	if len(answer.IDs) != 3 || len(answer.Results) != 3 {
		t.Fatalf("ids %v, results %+v, want three of each", answer.IDs, answer.Results)
	}
	for index, want := range []string{"x", "y", "z"} {
		entry := answer.Results[index]
		if entry.ID != want || entry.WriteOutcome != outcomeApplied || entry.Code != 0 {
			t.Errorf("results[%d] = %+v, want applied for %q with no code", index, entry, want)
		}
	}
}

// TestBatchPutRepeatedIDFollowsTheBucketType: only a hash or btree bucket
// stores one document per _id_, so only there is a repeat inside a BATCH_PUT
// refused. A fifo, lifo or heap takes each document as its own element (spec
// section 6.6, "per-item push, as today"), at the door and in the handler.
func TestBatchPutRepeatedIDFollowsTheBucketType(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	for _, kind := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: string(kind), Type: kind})
		frame := sendOrderFrame(t, rootSession(), handler, transport.OpBatchPut, mustMarshalBSON(t, repeatedIDBatch("app", string(kind))))
		keyed := kind == BucketTypeHash || kind == BucketTypeBTree
		if keyed {
			if refusal := decodeBatchRefusal(t, frame); refusal.Code != documentExistsWire {
				t.Errorf("%s: repeated _id_ answered code %d, want DOCUMENT_EXISTS", kind, refusal.Code)
			}
			continue
		}
		if answer := decodeBatchAnswer(t, frame, transport.OpBatchPutOK); !answer.Success || len(answer.Results) != 3 {
			t.Errorf("%s: repeated _id_ answered %+v, want the batch pushed with three results", kind, answer)
		}
	}

	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketNameOwnedBy(t, owner, "app", tc.addrs[0])
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeFIFO})
	if answer := decodeBatchAnswer(t, tc.frame(0, transport.OpBatchPut, repeatedIDBatch("app", bucket)), transport.OpBatchPutOK); !answer.Success {
		t.Errorf("fifo on a cluster: repeated _id_ answered %+v, want the batch pushed", answer)
	}
}
