package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A heap files its elements by _id_, so a push of an _id_ the heap holds
// leaves one element; the cells below count elements on fifo and lifo only and
// pin no more of a heap than that the push is answered.

// phase1Cell is one cell of spec §6.3's verb matrix with string ids: a verb
// sent to a bucket of one type seeded with the element "k1", and what the
// answer and the bucket must show afterwards.
type phase1Cell struct {
	verb  string
	op    transport.Opcode
	req   func(bucket string) interface{}
	check func(t *testing.T, s *Server, bucket string, kind BucketType, answer *transport.Frame)
}

// phase1Answer decodes an answer that must not be an error frame.
func phase1Answer(t *testing.T, answer *transport.Frame) bson.M {
	t.Helper()
	if answer.Opcode == transport.OpError {
		t.Fatalf("refused: %s", decodeErrorFrame(t, answer, answer.Opcode.String()).Message)
	}
	var decoded bson.M
	if err := bson.Unmarshal(answer.Payload, &decoded); err != nil {
		t.Fatalf("decoding the answer: %v", err)
	}
	if success, held := decoded["success"].(bool); held && !success {
		t.Fatalf("answered success false: %s", matrixStringify(decoded))
	}
	return decoded
}

// phase1Refused fails unless answer is an error frame of code whose message
// holds every fragment.
func phase1Refused(t *testing.T, answer *transport.Frame, code transport.ErrorCode, fragments ...string) {
	t.Helper()
	if answer.Opcode != transport.OpError {
		t.Fatalf("answered %s %s; want an ERROR frame, code %#x", answer.Opcode, string(answer.Payload), int(code))
	}
	refusal := decodeErrorFrame(t, answer, "the cell")
	if transport.ErrorCode(refusal.Code) != code {
		t.Fatalf("refused with code %#x %q; want %#x", refusal.Code, refusal.Message, int(code))
	}
	for _, fragment := range fragments {
		if !strings.Contains(refusal.Message, fragment) {
			t.Errorf("the refusal %q does not say %q", refusal.Message, fragment)
		}
	}
}

// phase1Field is field of the document stored under key, or "absent".
func phase1Field(t *testing.T, s *Server, bucket, key, field string) string {
	t.Helper()
	doc := documentOf(t, s, "app", bucket, key)
	if doc == nil {
		return "absent"
	}
	return fmt.Sprint(doc[field])
}

func phase1Count(t *testing.T, s *Server, bucket string) int {
	t.Helper()
	return wrongTypeEntryCount(t, s, bucket)
}

func phase1Doc(id string, n int32) map[string]interface{} {
	return map[string]interface{}{"_id_": id, "n": n}
}

// keyedTypes are the types that store a document by key.
func keyedType(kind BucketType) bool { return kind == BucketTypeHash || kind == BucketTypeBTree }
func queueType(kind BucketType) bool {
	return kind == BucketTypeFIFO || kind == BucketTypeLIFO || kind == BucketTypeHeap
}

const blobPutAdvice = "a blob bucket is written with put"

// insertCell is the insert column of the matrix: refuses a held key on hash
// and btree, pushes on a queue or heap, and is refused on a blob bucket.
func insertCell(verb string, op transport.Opcode, key string, n int32) phase1Cell {
	return phase1Cell{verb: verb, op: op,
		req: func(b string) interface{} {
			return InsertRequest{Database: "app", Bucket: b, Data: phase1Doc(key, n)}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			switch {
			case keyedType(kind) && key == "k1":
				phase1Refused(t, answer, transport.ErrDocumentExists, documentExistsText("app", b, "k1"))
				if got := phase1Field(t, s, b, "k1", "n"); got != "1" {
					t.Errorf("the refused insert left k1 with n %s; want 1", got)
				}
			case keyedType(kind):
				phase1Answer(t, answer)
				if got := phase1Field(t, s, b, key, "n"); got != fmt.Sprint(n) {
					t.Errorf("the insert of %s stored n %s; want %d", key, got, n)
				}
			case queueType(kind):
				phase1Answer(t, answer)
				if got := phase1Count(t, s, b); kind != BucketTypeHeap && got != 2 {
					t.Errorf("the insert into a %s bucket left %d elements; want 2 (a push)", kind, got)
				}
			default:
				phase1Refused(t, answer, transport.ErrInvalidArgument, blobPutAdvice, `db.`+b+`.put("key", ...)`)
				if got := phase1Count(t, s, b); got != 1 {
					t.Errorf("the refused insert left the blob bucket with %d entries; want 1", got)
				}
			}
		}}
}

var phase1Cells = []phase1Cell{
	insertCell("insert of the held k1", transport.OpPut, "k1", 2),
	insertCell("insert of the absent k9", transport.OpPut, "k9", 9),
	// BTREE_INSERT is answered on every type as PUT is (handleBTreeInsert
	// reaches InsertCtx, which writes by the bucket's type); spec §6.3's "n/a
	// (refused, as today)" for the other types does not hold at the base.
	insertCell("BTREE_INSERT of the held k1", transport.OpBTreeInsert, "k1", 2),
	{verb: "upsert of the held k1", op: transport.OpUpsert,
		req: func(b string) interface{} {
			return InsertRequest{Database: "app", Bucket: b, Data: phase1Doc("k1", 2)}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			switch {
			case keyedType(kind):
				phase1Answer(t, answer)
				if got := phase1Field(t, s, b, "k1", "n"); got != "2" {
					t.Errorf("the upsert left k1 with n %s; want 2 (replaced)", got)
				}
			case queueType(kind):
				phase1Refused(t, answer, transport.ErrInvalidArgument, "a queue has no key to replace; use push")
			default:
				phase1Refused(t, answer, transport.ErrInvalidArgument, "use put")
			}
		}},
	{verb: "insert many naming the held k1", op: transport.OpBatchPut,
		req: func(b string) interface{} {
			return BatchInsertRequest{Database: "app", Bucket: b, Data: []map[string]interface{}{phase1Doc("k8", 8), phase1Doc("k1", 2)}}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			switch {
			case keyedType(kind):
				phase1Refused(t, answer, transport.ErrDocumentExists, `"k1"`)
				if got := phase1Field(t, s, b, "k8", "n"); got != "absent" {
					t.Errorf("the refused batch stored k8 (n %s); a BATCH_PUT is atomic", got)
				}
				if got := phase1Field(t, s, b, "k1", "n"); got != "1" {
					t.Errorf("the refused batch left k1 with n %s; want 1", got)
				}
			case queueType(kind):
				phase1Answer(t, answer)
				if got := phase1Count(t, s, b); kind != BucketTypeHeap && got != 3 {
					t.Errorf("the batch into a %s bucket left %d elements; want 3 (a push per item)", kind, got)
				}
			default:
				phase1Refused(t, answer, transport.ErrInvalidArgument)
			}
		}},
	{verb: "upsert many naming the held k1", op: transport.OpBatchUpsert,
		req: func(b string) interface{} {
			return BatchInsertRequest{Database: "app", Bucket: b, Data: []map[string]interface{}{phase1Doc("k1", 2), phase1Doc("k7", 7)}}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			if !keyedType(kind) {
				phase1Refused(t, answer, transport.ErrInvalidArgument)
				return
			}
			phase1Answer(t, answer)
			if got := phase1Field(t, s, b, "k1", "n") + "," + phase1Field(t, s, b, "k7", "n"); got != "2,7" {
				t.Errorf("after the batch upsert k1,k7 hold n %s; want 2,7", got)
			}
		}},
	{verb: "push of the held k1", op: transport.OpFIFOPush,
		req: func(b string) interface{} {
			return PushRequest{Database: "app", Bucket: b, Data: phase1Doc("k1", 2)}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			switch {
			case keyedType(kind):
				phase1Refused(t, answer, transport.ErrDocumentExists, documentExistsText("app", b, "k1"))
				if got := phase1Field(t, s, b, "k1", "n"); got != "1" {
					t.Errorf("the refused push left k1 with n %s; want 1", got)
				}
			case queueType(kind):
				phase1Answer(t, answer)
				if got := phase1Count(t, s, b); kind != BucketTypeHeap && got != 2 {
					t.Errorf("the push left %d elements; want 2", got)
				}
			default:
				phase1Refused(t, answer, transport.ErrInvalidArgument, "put")
			}
		}},
	{verb: "push of the absent k9", op: transport.OpFIFOPush,
		req: func(b string) interface{} {
			return PushRequest{Database: "app", Bucket: b, Data: phase1Doc("k9", 9)}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			switch {
			case keyedType(kind):
				phase1Answer(t, answer)
				if got := phase1Field(t, s, b, "k9", "n"); got != "9" {
					t.Errorf("the push of k9 stored n %s; want 9", got)
				}
			case queueType(kind):
				phase1Answer(t, answer)
			default:
				phase1Refused(t, answer, transport.ErrInvalidArgument)
			}
		}},
	{verb: "blob put of the held k1", op: transport.OpBlobWrite,
		req: func(b string) interface{} {
			return BlobWriteRequest{Database: "app", Bucket: b, Key: "k1", Data: []byte("replaced payload")}
		},
		check: func(t *testing.T, s *Server, b string, kind BucketType, answer *transport.Frame) {
			if kind != BucketTypeBlob {
				phase1Refused(t, answer, transport.ErrInvalidArgument)
				return
			}
			phase1Answer(t, answer)
			read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: b, Key: "k1"})
			if err != nil || !read.Found || string(read.Data) != "replaced payload" {
				t.Errorf("after the blob put k1 reads %+v, %v; want the replaced payload", read, err)
			}
		}},
}

// TestVerbMatrixPhase1 is T-49a: every string-id cell of spec §6.3's verb
// matrix, sent through the dispatcher to a bucket of every store type seeded
// with the element "k1". insert (PUT, and BTREE_INSERT on a btree) refuses a
// held key on hash and btree and pushes on a queue or a heap; a blob bucket is
// written with put (BLOB_WRITE), which replaces, and refuses insert and
// upsert; insert many is atomic; push on hash and btree is insert.
func TestVerbMatrixPhase1(t *testing.T) {
	s := newTestServer(t)
	for cellIndex, cell := range phase1Cells {
		for _, kind := range matrixTypes {
			t.Run(cell.verb+"/"+string(kind), func(t *testing.T) {
				bucket := fmt.Sprintf("p1_%d_%s", cellIndex, kind)
				createTestBucket(t, s, "app", bucket, kind)
				seedMatrixBucket(t, s, bucket, kind)
				answer, err := s.dispatch(context.Background(), requestFrame(t, cell.op, cell.req(bucket)))
				if err != nil {
					t.Fatalf("dispatching %s: %v", cell.op, err)
				}
				fireWrittenHook(answer)
				cell.check(t, s, bucket, kind, answer)
			})
		}
	}
}
