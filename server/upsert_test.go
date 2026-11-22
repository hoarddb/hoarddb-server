package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUpsertReplacesScalarsAndKeepsStores is T-03: an UPSERT of an existing
// _id_ replaces every scalar field of the document (a field the new document
// lacks is gone) and leaves the document's own stores alone. BATCH_UPSERT does
// the same, per document.
func TestUpsertReplacesScalarsAndKeepsStores(t *testing.T) {
	s := newPostServer(t)
	handler := NewHandler(s)
	ctx := rootSession()
	fieldOK(t)(s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": 1}}))
	fieldOK(t)(s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p2", Field: "Comments", Data: map[string]interface{}{"n": 2}}))
	// The seed documents hold only a title; give each a second scalar so the
	// replace has something to drop.
	for _, key := range []string{"p1", "p2"} {
		sendOrderFrame(t, ctx, handler, transport.OpUpsert,
			mustMarshalBSON(t, InsertRequest{Database: "app", Bucket: "Post", Key: key, Data: map[string]interface{}{"title": key, "draft": true}}))
	}

	t.Run("UPSERT", func(t *testing.T) {
		answer := readScanAnswer(t, sendOrderFrame(t, ctx, handler, transport.OpUpsert,
			mustMarshalBSON(t, InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "uno"}})))
		if answer.refused || answer.opcode != transport.OpUpsertOK {
			t.Fatalf("UPSERT answered %+v, want UPSERT_OK", answer)
		}
		document := documentOf(t, s, "app", "Post", "p1")
		if document["title"] != "uno" {
			t.Errorf("p1 title = %v, want uno", document["title"])
		}
		if _, kept := document["draft"]; kept {
			t.Errorf("p1 still holds draft after an UPSERT that did not send it: %v", document)
		}
		if got := fieldLen(t, s, "p1", "Comments"); got != 1 {
			t.Errorf("p1 Comments holds %d elements after the UPSERT, want its 1 kept", got)
		}
	})

	t.Run("BATCH_UPSERT", func(t *testing.T) {
		answer := readScanAnswer(t, sendOrderFrame(t, ctx, handler, transport.OpBatchUpsert,
			mustMarshalBSON(t, BatchInsertRequest{Database: "app", Bucket: "Post", Data: []map[string]interface{}{
				{"_id_": "p2", "title": "dos"}, {"_id_": "p3", "title": "tres"},
			}})))
		if answer.refused || answer.opcode != transport.OpBatchUpsertOK {
			t.Fatalf("BATCH_UPSERT answered %+v, want BATCH_UPSERT_OK", answer)
		}
		document := documentOf(t, s, "app", "Post", "p2")
		if document["title"] != "dos" {
			t.Errorf("p2 title = %v, want dos", document["title"])
		}
		if _, kept := document["draft"]; kept {
			t.Errorf("p2 still holds draft after a BATCH_UPSERT that did not send it: %v", document)
		}
		if got := fieldLen(t, s, "p2", "Comments"); got != 1 {
			t.Errorf("p2 Comments holds %d elements after the BATCH_UPSERT, want its 1 kept", got)
		}
		if documentOf(t, s, "app", "Post", "p3") == nil {
			t.Error("p3, new in the batch, was not stored")
		}
	})
}

// TestUpsertRefusedOnKeylessAndBlobBuckets (build doc BD-10): a queue or a
// heap has no key to replace, and a blob bucket's replace verb is put. Both
// UPSERT and BATCH_UPSERT are refused, with the verb to use instead.
func TestUpsertRefusedOnKeylessAndBlobBuckets(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	buckets := []struct {
		name string
		kind BucketType
		want string
	}{
		{"jobs", BucketTypeFIFO, "a queue has no key to replace; use push"},
		{"stack", BucketTypeLIFO, "a queue has no key to replace; use push"},
		{"ranked", BucketTypeHeap, "a queue has no key to replace; use push"},
		{"files", BucketTypeBlob, `use put, db.files.put("key", `},
	}
	for _, bucket := range buckets {
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket.name, Type: bucket.kind})
	}
	ctx := rootSession()
	for _, bucket := range buckets {
		requests := map[transport.Opcode]any{
			transport.OpUpsert: InsertRequest{Database: "app", Bucket: bucket.name, Key: "k", Data: map[string]interface{}{"a": 1}},
			transport.OpBatchUpsert: BatchInsertRequest{Database: "app", Bucket: bucket.name,
				Data: []map[string]interface{}{{"_id_": "k", "a": 1}}},
		}
		for op, request := range requests {
			t.Run(op.String()+" on "+string(bucket.kind), func(t *testing.T) {
				answer := readScanAnswer(t, sendOrderFrame(t, ctx, handler, op, mustMarshalBSON(t, request)))
				if !answer.refused {
					t.Fatalf("%s into the %s bucket answered %s, want a refusal", op, bucket.kind, answer.opcode)
				}
				if answer.code != int(transport.ErrInvalidArgument) {
					t.Errorf("refusal code = %d, want INVALID_ARGUMENT", answer.code)
				}
				if !strings.Contains(answer.message, bucket.want) {
					t.Errorf("refusal %q does not say %q", answer.message, bucket.want)
				}
			})
		}
	}
	// Nothing was stored by the refused writes.
	for _, bucket := range buckets[:3] {
		if got := countOf(t, s, bucket.name); got != 0 {
			t.Errorf("%s bucket %s holds %d entries after refused UPSERTs, want 0", bucket.kind, bucket.name, got)
		}
	}
}
