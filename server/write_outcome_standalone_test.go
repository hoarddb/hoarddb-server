package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0514: every refusal of a write says whether the write happened
// (clients/handbook/errors.md section 4). A refusal decided before anything
// was applied says write_outcome not_applied, on a standalone node as on a
// cluster; a read and a DDL request say nothing; an error that can follow a
// store's own failure never claims not_applied.

// refusalFrom sends one frame through handler and decodes the error it must be
// answered with.
func refusalFrom(t *testing.T, handler transport.RequestHandler, frame *transport.Frame, what string) ErrorResponse {
	t.Helper()
	answer, err := handler(context.Background(), frame)
	if err != nil {
		t.Fatalf("%s: handler returned a Go error: %v", what, err)
	}
	return errorAnswerOf(t, answer, what)
}

// writeRefusal is one write a node must refuse before applying anything.
type writeRefusal struct {
	name  string
	frame *transport.Frame
	// code is the refusal's code; 0 leaves it unchecked, for a refusal whose
	// code another bug owns (BUG-0515).
	code transport.ErrorCode
}

// requireNotApplied checks each refusal's code and that it says not_applied.
func requireNotApplied(t *testing.T, handler transport.RequestHandler, refusals []writeRefusal) {
	t.Helper()
	for _, refusal := range refusals {
		answer := refusalFrom(t, handler, refusal.frame, refusal.name)
		if refusal.code != 0 && transport.ErrorCode(answer.Code) != refusal.code {
			t.Errorf("%s was answered code %d, want %d: %s", refusal.name, answer.Code, refusal.code, answer.Message)
		}
		if answer.WriteOutcome != outcomeNotApplied {
			t.Errorf("%s was answered write_outcome %q, want %q: %s", refusal.name, answer.WriteOutcome, outcomeNotApplied, answer.Message)
		}
	}
}

// standaloneOutcomeServer is a standalone node with database app, a hash
// bucket users with a unique Email index and an int Born, a hash bucket User
// with a fifo store field Purchases and its document ada, and a blob bucket
// files.
func standaloneOutcomeServer(t *testing.T) (*Server, transport.RequestHandler) {
	t.Helper()
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash,
		Options: map[string]interface{}{
			"placement":   "whole",
			"indices":     []interface{}{map[string]interface{}{"fields": []interface{}{"Email"}, "unique": true}},
			"fields":      []interface{}{map[string]interface{}{"name": "Born", "type": "int"}},
			"field_types": "strict",
		}})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash,
		Options: map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifo"}}}})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "files", Type: BucketTypeBlob})
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "ada", Data: map[string]interface{}{"Email": "ada@example.org", "Name": "Ada"}})
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "ada", Data: map[string]interface{}{"Name": "Ada"}})
	return s, handler
}

// TestStandaloneWriteRefusalsCarryNotApplied: on a standalone node every
// write verb's refusal of a bucket that does not exist, and every refusal the
// handlers decide before the apply (a payload that is not BSON, a duplicate
// key, a mistyped document, a name no path can reach, an update its operators
// refuse, a field verb on a missing parent or one its store type does not
// answer, a blob write to a hash bucket), says not_applied, and nothing was
// stored. Fails before the fix: the field is absent.
func TestStandaloneWriteRefusalsCarryNotApplied(t *testing.T) {
	s, handler := standaloneOutcomeServer(t)
	frame := func(op transport.Opcode, req any) *transport.Frame {
		return transport.NewFrame(op, mustMarshalBSON(t, req))
	}
	doc := map[string]interface{}{"Name": "Ada"}
	missingField := FieldRequest{Database: "app", Bucket: "nope", Key: "ada", Field: "Purchases", Element: "x", Data: doc}
	requireNotApplied(t, handler, []writeRefusal{
		{"PUT to a missing bucket", frame(transport.OpPut, InsertRequest{Database: "app", Bucket: "nope", Key: "k", Data: doc}), transport.ErrNotFound},
		{"PUT to a missing database", frame(transport.OpPut, InsertRequest{Database: "nodb", Bucket: "nope", Key: "k", Data: doc}), transport.ErrNotFound},
		{"BATCH_PUT to a missing bucket", frame(transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "nope", Data: []map[string]interface{}{doc}}), transport.ErrNotFound},
		{"FIFO_PUSH to a missing bucket", frame(transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "nope", Data: doc}), transport.ErrNotFound},
		{"LIFO_PUSH to a missing bucket", frame(transport.OpLIFOPush, PushRequest{Database: "app", Bucket: "nope", Data: doc}), transport.ErrNotFound},
		{"HEAP_PUSH to a missing bucket", frame(transport.OpHeapPush, PushRequest{Database: "app", Bucket: "nope", Data: doc, Priority: 3}), transport.ErrNotFound},
		{"FIFO_POP of a missing bucket", frame(transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "nope"}), transport.ErrNotFound},
		{"LIFO_POP of a missing bucket", frame(transport.OpLIFOPop, PopRequest{Database: "app", Bucket: "nope"}), transport.ErrNotFound},
		{"HEAP_POP of a missing bucket", frame(transport.OpHeapPop, PopRequest{Database: "app", Bucket: "nope"}), transport.ErrNotFound},
		{"BTREE_INSERT to a missing bucket", frame(transport.OpBTreeInsert, InsertRequest{Database: "app", Bucket: "nope", Key: "k", Data: doc}), transport.ErrNotFound},
		{"BLOB_WRITE to a missing bucket", frame(transport.OpBlobWrite, BlobWriteRequest{Database: "app", Bucket: "nope", Key: "k", Data: []byte("bytes")}), transport.ErrNotFound},
		{"DELETE in a missing bucket", frame(transport.OpDelete, DeleteRequest{Database: "app", Bucket: "nope", Key: "k"}), transport.ErrNotFound},
		{"UPDATE in a missing bucket", frame(transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "nope", Key: "k", Set: doc}), transport.ErrNotFound},
		{"FIELD_PUSH in a missing bucket", frame(transport.OpFieldPush, missingField), transport.ErrNotFound},
		{"FIELD_POP in a missing bucket", frame(transport.OpFieldPop, missingField), transport.ErrNotFound},
		{"FIELD_PUT in a missing bucket", frame(transport.OpFieldPut, missingField), transport.ErrNotFound},
		{"FIELD_DELETE in a missing bucket", frame(transport.OpFieldDelete, missingField), transport.ErrNotFound},
		{"a PUT whose payload is not BSON", transport.NewFrame(transport.OpPut, []byte{0x01, 0x02, 0x03}), 0},
		{"a duplicate key", frame(transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "countess", Data: map[string]interface{}{"Email": "ada@example.org"}}), transport.ErrDuplicateKey},
		{"a mistyped document", frame(transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "mistyped", Data: map[string]interface{}{"Born": "eighteen fifteen"}}), transport.ErrFieldType},
		{"a name no path can reach", frame(transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "dotted", Data: map[string]interface{}{"a.b": int32(1)}}), transport.ErrFieldType},
		{"an update its operator refuses", frame(transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "users", Key: "ada", Mul: map[string]float64{"Name": 1.5}}), transport.ErrInvalidArgument},
		{"a field verb on a missing parent", frame(transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "User", Key: "nobody", Field: "Purchases", Data: doc}), transport.ErrNotFound},
		{"a field verb the store type does not answer", frame(transport.OpFieldPut, FieldRequest{Database: "app", Bucket: "User", Key: "ada", Field: "Purchases", Element: "x", Data: doc}), transport.ErrInvalidArgument},
		{"a blob write to a hash bucket", frame(transport.OpBlobWrite, BlobWriteRequest{Database: "app", Bucket: "users", Key: "k", Data: []byte("bytes")}), 0},
	})

	for _, key := range []string{"countess", "mistyped", "dotted"} {
		found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: key})
		if err != nil {
			t.Fatalf("reading %q back: %v", key, err)
		}
		if found.Found {
			t.Errorf("document %q is stored, and its refusal said not_applied", key)
		}
	}
}

// TestPermissionDeniedWriteCarriesNotApplied: the authorization gate in
// NewHandler refuses a write before anything is routed or applied, so its
// PERMISSION_DENIED says not_applied. A read and a DDL request it refuses say
// nothing. Fails before the fix: the gate's error is answered bare.
func TestPermissionDeniedWriteCarriesNotApplied(t *testing.T) {
	handler := NewHandler(userTestServer(t))
	reader := &transport.Session{}
	reader.SetIdentity(transport.Identity{Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	ctx := transport.WithSession(context.Background(), reader)
	send := func(op transport.Opcode, req any) ErrorResponse {
		t.Helper()
		answer, err := handler(ctx, transport.NewFrame(op, mustMarshalBSON(t, req)))
		if err != nil {
			t.Fatalf("%s: handler returned a Go error: %v", op, err)
		}
		return errorAnswerOf(t, answer, op.String()+" as reader")
	}
	doc := map[string]interface{}{"Name": "Ada"}
	for _, write := range []struct {
		op  transport.Opcode
		req any
	}{
		{transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "ada", Data: doc}},
		{transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "queue", Data: doc}},
		{transport.OpDelete, DeleteRequest{Database: "app", Bucket: "users", Key: "ada"}},
		{transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "users", Key: "ada", Set: doc}},
		{transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "User", Key: "ada", Field: "Purchases", Data: doc}},
	} {
		refusal := send(write.op, write.req)
		requireOutcome(t, refusal, write.op.String()+" as reader", outcomeNotApplied, transport.ErrPermissionDenied)
	}

	read := send(transport.OpBucketStats, BucketStatsRequest{Database: "other", Bucket: "users"})
	requireOutcome(t, read, "a read outside the grant", "", transport.ErrPermissionDenied)
	ddl := send(transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: "users"})
	requireOutcome(t, ddl, "a drop by a reader", "", transport.ErrPermissionDenied)
}

// TestOversizedWriteCarriesNotApplied: the document limit and the blob limit
// are judged in NewHandler, before routing and the apply, so their refusal
// says not_applied. Fails before the fix: the limit's error is answered bare.
func TestOversizedWriteCarriesNotApplied(t *testing.T) {
	s := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Storage.MaxDocumentBytes = config.MinDocumentBytes
	})
	s.config.Storage.MaxBlobBytes = 1 << 10
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "queue", Type: BucketTypeFIFO})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "files", Type: BucketTypeBlob})

	large := map[string]interface{}{"Bio": strings.Repeat("x", 2*config.MinDocumentBytes)}
	frame := func(op transport.Opcode, req any) *transport.Frame {
		return transport.NewFrame(op, mustMarshalBSON(t, req))
	}
	requireNotApplied(t, handler, []writeRefusal{
		{"a PUT over the document limit", frame(transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "big", Data: large}), transport.ErrInvalidArgument},
		{"a BATCH_PUT with a document over the limit", frame(transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "users", Data: []map[string]interface{}{large}}), transport.ErrInvalidArgument},
		{"a FIFO_PUSH over the document limit", frame(transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "queue", Data: large}), transport.ErrInvalidArgument},
		{"a BLOB_WRITE over the blob limit", frame(transport.OpBlobWrite, BlobWriteRequest{Database: "app", Bucket: "files", Key: "big", Data: make([]byte, 2<<10)}), transport.ErrInvalidArgument},
	})
	found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "big"})
	if err != nil {
		t.Fatalf("reading the refused document back: %v", err)
	}
	if found.Found {
		t.Error("a document refused for its size is stored")
	}
}

// TestReadAndDDLRefusalsCarryNoWriteOutcome: write_outcome belongs to the
// data writes. A read's refusal and a DDL request's refusal carry none
// (handbook section 4: absent means a read, a DDL request or an older server).
// This passes before the fix too; it guards the fix from stamping every error
// frame.
func TestReadAndDDLRefusalsCarryNoWriteOutcome(t *testing.T) {
	_, handler := standaloneOutcomeServer(t)
	frame := func(op transport.Opcode, req any) *transport.Frame {
		return transport.NewFrame(op, mustMarshalBSON(t, req))
	}
	for _, unstamped := range []struct {
		name  string
		frame *transport.Frame
	}{
		{"GET of a missing bucket", frame(transport.OpGet, FindRequest{Database: "app", Bucket: "nope", Key: "k"})},
		{"a GET whose payload is not BSON", transport.NewFrame(transport.OpGet, []byte{0x01, 0x02, 0x03})},
		{"FIFO_PEEK of a missing bucket", frame(transport.OpFIFOPeek, PeekRequest{Database: "app", Bucket: "nope"})},
		{"BLOB_READ of a missing bucket", frame(transport.OpBlobRead, BlobReadRequest{Database: "app", Bucket: "nope", Key: "k"})},
		{"FIELD_GET of a missing parent", frame(transport.OpFieldGet, FieldRequest{Database: "app", Bucket: "User", Key: "nobody", Field: "Purchases", Element: "x"})},
		{"BUCKET_STATS of a missing bucket", frame(transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: "nope"})},
		{"CREATE_DB of a database that exists", frame(transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})},
		{"DELETE_DB of a missing database", frame(transport.OpDeleteDB, DeleteDatabaseRequest{Name: "nodb"})},
		{"DELETE_BUCKET of a missing bucket", frame(transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: "nope"})},
		{"CREATE_BUCKET of a hash bucket as a fifo", frame(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeFIFO})},
	} {
		answer, err := handler(context.Background(), unstamped.frame)
		if err != nil {
			t.Fatalf("%s: handler returned a Go error: %v", unstamped.name, err)
		}
		if answer.Opcode != transport.OpError {
			continue // answered in band; nothing to stamp
		}
		if refusal := errorAnswerOf(t, answer, unstamped.name); refusal.WriteOutcome != "" {
			t.Errorf("%s was answered write_outcome %q; a read or a DDL request carries none: %s", unstamped.name, refusal.WriteOutcome, refusal.Message)
		}
	}
}

// TestStoreFailureOfAWriteIsNotCalledNotApplied: an error answered after the
// store itself failed the apply is not a refusal decided before the apply. A
// failed commit can still have reached the store's memory, so the error must
// never say not_applied: a client that retried a push or an $inc on it could
// apply it twice. This passes before the fix too; it is the guard against a
// fix that stamps every error of a write.
func TestStoreFailureOfAWriteIsNotCalledNotApplied(t *testing.T) {
	s, handler := standaloneOutcomeServer(t)
	for _, write := range []struct {
		name  string
		frame *transport.Frame
	}{
		{"an UPDATE the store failed", transport.NewFrame(transport.OpUpdate, mustMarshalBSON(t, UpdateRequest{Database: "app", Bucket: "users", Key: "ada", Set: map[string]interface{}{"Name": "Countess"}}))},
		{"a BLOB_WRITE the store failed", transport.NewFrame(transport.OpBlobWrite, mustMarshalBSON(t, BlobWriteRequest{Database: "app", Bucket: "files", Key: "k", Data: []byte("bytes")}))},
	} {
		failNextApplyOn(t, s.kv, anySeq)
		refusal := refusalFrom(t, handler, write.frame, write.name)
		if refusal.WriteOutcome == outcomeNotApplied {
			t.Errorf("%s was answered write_outcome %q; the store's own failure is not known to have stored nothing: %s",
				write.name, refusal.WriteOutcome, refusal.Message)
		}
	}
}

// TestClusterWriteRefusalsCarryNotApplied is the cluster variant: the same
// refusals, sent to the bucket's owner and to the other node, say not_applied
// — the document limit judged in NewHandler, a payload that is not BSON (which
// the authorization gate cannot scope), and a write to a bucket that does not
// exist — and a read's refusal says nothing. Fails before the fix for the
// first two: their errors are answered bare on a cluster too.
func TestClusterWriteRefusalsCarryNotApplied(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, func(_ int, cfg *config.Config) {
		cfg.Storage.MaxDocumentBytes = config.MinDocumentBytes
	})
	defer tc.stop()
	createTestHashBucket(t, tc)
	waitForServing(t, tc, testDatabase, testBucket, 2)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	large := map[string]interface{}{"Bio": strings.Repeat("x", 2*config.MinDocumentBytes)}
	doc := map[string]interface{}{"Name": "Ada"}

	for _, via := range []int{ownerIndex, 1 - ownerIndex} {
		role := "the owner"
		if via != ownerIndex {
			role = "the other node"
		}
		for _, write := range []struct {
			name string
			op   transport.Opcode
			req  any
			code transport.ErrorCode
		}{
			{"a PUT over the document limit", transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "big", Data: large}, transport.ErrInvalidArgument},
			// A bucket no member holds is NOT_FOUND once every member has said
			// so (BUG-0686); a copy still on its way stays UNAVAILABLE, which
			// missing_bucket_write_test.go pins.
			{"a PUT to a missing bucket", transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "nope", Key: "k", Data: doc}, transport.ErrNotFound},
			{"a FIFO_PUSH to a missing bucket", transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: "nope", Data: doc}, transport.ErrNotFound},
		} {
			what := fmt.Sprintf("%s through %s", write.name, role)
			refusal := errorAnswerOf(t, tc.frame(via, write.op, write.req), what)
			requireOutcome(t, refusal, what, outcomeNotApplied, write.code)
		}

		what := "a PUT whose payload is not BSON through " + role
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		answer, err := tc.nodes[via].client.SendRequest(ctx, transport.OpPut, []byte{0x01, 0x02, 0x03})
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if refusal := errorAnswerOf(t, answer, what); refusal.WriteOutcome != outcomeNotApplied {
			t.Errorf("%s was answered write_outcome %q, want %q: %s", what, refusal.WriteOutcome, outcomeNotApplied, refusal.Message)
		}

		what = "a GET of a missing bucket through " + role
		if refusal := errorAnswerOf(t, tc.frame(via, transport.OpGet, FindRequest{Database: testDatabase, Bucket: "nope", Key: "k"}), what); refusal.WriteOutcome != "" {
			t.Errorf("%s was answered write_outcome %q; a read carries none", what, refusal.WriteOutcome)
		}
	}
	for index := range tc.nodes {
		if holdsKeyLocallyIn(tc.nodes[index], testBucket, "big") {
			t.Errorf("node %d holds a document refused for its size", index+1)
		}
	}
}

// TestRefusedBeforeApplyStampsOnlyWhatNothingReached pins refusedBeforeApply's
// rule on each input it can meet: a data write's refusal with no apply, or
// with only the store's batch-too-large answer (nothing of the batch is
// written), says not_applied; one after an apply committed, or after the
// store failed one, keeps saying nothing; an outcome already said is kept; a
// read's and a DDL request's error frames are left alone, as is a success.
func TestRefusedBeforeApplyStampsOnlyWhatNothingReached(t *testing.T) {
	refusal := func(err error) *transport.Frame {
		return errorFrame(&transport.Frame{Opcode: transport.OpPut}, err)
	}
	refused := errors.New("refused")
	for _, check := range []struct {
		name    string
		op      transport.Opcode
		reply   *transport.Frame
		applied *applyOutcome
		want    string
	}{
		{"a refusal decided before any apply", transport.OpPut, refusal(refused), nil, outcomeNotApplied},
		{"a refusal whose apply nothing reached", transport.OpFIFOPush, refusal(refused), &applyOutcome{}, outcomeNotApplied},
		{"the store's batch-too-large answer", transport.OpBatchPut, refusal(refused), &applyOutcome{tooLarge: true}, outcomeNotApplied},
		{"an error after an apply committed", transport.OpFieldPut, refusal(refused), &applyOutcome{committed: true}, ""},
		{"an error after the store failed the apply", transport.OpUpdate, refusal(refused), &applyOutcome{storeFailure: refused}, ""},
		{"an error that says unknown already", transport.OpPut, refusal(withOutcome(outcomeUnknown, refused)), nil, outcomeUnknown},
		{"a read's error", transport.OpGet, refusal(refused), nil, ""},
		{"a DDL request's error", transport.OpCreateBucket, refusal(refused), nil, ""},
		{"a user-management error", transport.OpUserCreate, refusal(refused), nil, ""},
	} {
		stamped := refusedBeforeApply(check.op, check.reply, check.applied)
		if got := errorAnswerOf(t, stamped, check.name).WriteOutcome; got != check.want {
			t.Errorf("%s was given write_outcome %q, want %q", check.name, got, check.want)
		}
	}
	success := transport.NewFrame(transport.OpPut, mustMarshalBSON(t, InsertResponse{ID: "k", Success: true}))
	original := string(success.Payload)
	if stamped := refusedBeforeApply(transport.OpPut, success, nil); string(stamped.Payload) != original {
		t.Error("a success was rewritten")
	}
}

// TestEveryClientWriteVerbIsADataWriteOrNamedOtherwise: a write opcode added
// to the protocol must be placed. isDataWrite's verbs are writes that tell a
// client how many copies hold them (carriesWriteOutcome), and every other
// opcode that is not a read is DDL, user or cluster management, restore,
// session plumbing or a response, named here. A new write verb left out of
// both would answer its refusals without an outcome.
func TestEveryClientWriteVerbIsADataWriteOrNamedOtherwise(t *testing.T) {
	notDataWrites := map[transport.Opcode]bool{
		transport.OpCreateDB: true, transport.OpDeleteDB: true, transport.OpCreateBucket: true, transport.OpDeleteBucket: true,
		transport.OpDeclareStoreFields: true, transport.OpAlterBucket: true,
		transport.OpUserCreate: true, transport.OpUserAlter: true, transport.OpUserDrop: true, transport.OpUserList: true,
		transport.OpUserGet: true, transport.OpUserGrant: true, transport.OpUserRevoke: true,
		transport.OpClusterAdd: true, transport.OpClusterRemove: true, transport.OpMigrate: true,
		transport.OpGossip: true, transport.OpReplPush: true, transport.OpReplAck: true, transport.OpClusterPropose: true,
		transport.OpMigrateStart: true, transport.OpMigrateData: true, transport.OpMigrateDone: true,
		transport.OpMembershipChange: true, transport.OpAuthConfigPush: true, transport.OpAuthConfigAck: true,
		transport.OpRestoreStart: true, transport.OpRestoreChunk: true, transport.OpRestoreDone: true,
		// A batch's items run separately and may partly apply: the whole-batch
		// frame is never stamped not_applied; each answer carries its own outcome.
		transport.OpBatch: true, transport.OpBatchOK: true,
		// A restore's own write: a client's frame is refused before routing.
		transport.OpDeadRestore: true,
		// The control channel's probe pair and the entry-ring wire are served on
		// their own channels, never as a client's bucket write.
		transport.OpClusterProbe: true, transport.OpClusterProbeReply: true,
		transport.OpRingKeyed: true, transport.OpRingAdmin: true, transport.OpRingReply: true, transport.OpRingBatch: true,
		transport.OpAuth: true, transport.OpAuthOK: true, transport.OpAuthFail: true, transport.OpPong: true, transport.OpError: true,
		transport.OpStreamOpen: true, transport.OpStreamData: true, transport.OpStreamAck: true, transport.OpStreamEnd: true,
		// Response opcodes: a server's answer, never a client's request.
		transport.OpGetResponse: true, transport.OpExistsResponse: true, transport.OpBatchPutOK: true,
		transport.OpUpsertOK: true, transport.OpBatchUpsertOK: true,
		transport.OpFIFOPopResponse: true, transport.OpLIFOPopResponse: true, transport.OpBTreeResponse: true,
		transport.OpHeapPopResponse: true, transport.OpBlobReadResponse: true, transport.OpGetFastResponse: true,
	}
	for code := 0; code <= 0xFF; code++ {
		op := transport.Opcode(code)
		if op.String() == "UNKNOWN" {
			continue
		}
		switch {
		case isDataWrite(op) && (op.IsReadOnly() || !carriesWriteOutcome(op) || notDataWrites[op]):
			t.Errorf("%s is a data write and also a read, DDL or named not a data write", op)
		case !isDataWrite(op) && !op.IsReadOnly() && !notDataWrites[op]:
			t.Errorf("%s is a write that is neither a data write (isDataWrite) nor named here: place it", op)
		}
	}
}
