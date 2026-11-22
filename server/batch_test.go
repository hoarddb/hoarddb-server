package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The batch-request spec's door and protocol tests (§9 T-1 to T-18) on a node
// that does not route. The social layout is the bench's hu layout: one bucket
// per user, User_<id>, holding that user's document and its stores.

const batchTestDB = "social"

func userBucket(u int) string { return fmt.Sprintf("User_%d", u) }
func userKey(u int) string    { return fmt.Sprint(u) }

// newSocialServer is a node with users 1..users, each a User_<id> bucket whose
// document <id> declares Feed (fifo), Inbox (fifo) and Posts (hash).
func newSocialServer(t *testing.T, users int) *Server {
	t.Helper()
	s := newTestServer(t)
	seedSocialUsers(t, s, 1, users)
	return s
}

func seedSocialUsers(t *testing.T, s *Server, from, to int) {
	t.Helper()
	for u := from; u <= to; u++ {
		resp := createWithStores(t, s, batchTestDB, userBucket(u), BucketTypeHash,
			StoreDeclaration{Name: "Feed", Engine: "fifoStore"},
			StoreDeclaration{Name: "Inbox", Engine: "fifoStore"},
			StoreDeclaration{Name: "Posts", Engine: "hashStore"})
		if !resp.Success {
			t.Fatalf("creating %s: %s", userBucket(u), resp.Message)
		}
		inserted, err := s.Insert(InsertRequest{Database: batchTestDB, Bucket: userBucket(u), Key: userKey(u),
			Data: map[string]interface{}{"name": userKey(u), "posts_n": int64(0)}})
		if err != nil || !inserted.Success {
			t.Fatalf("inserting user %d: %+v, %v", u, inserted, err)
		}
	}
}

func userField(u int, field string) FieldRequest {
	return FieldRequest{Database: batchTestDB, Bucket: userBucket(u), Key: userKey(u), Field: field}
}

func batchItemOf(t testing.TB, op transport.Opcode, payload any) BatchItem {
	t.Helper()
	raw, err := bson.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s item: %v", op, err)
	}
	return BatchItem{Opcode: int32(op), Payload: raw}
}

func pushItem(t testing.TB, u int, field string, document map[string]interface{}) BatchItem {
	req := userField(u, field)
	req.Data = document
	return batchItemOf(t, transport.OpFieldPush, req)
}

func putItem(t testing.TB, u int, field, element string, document map[string]interface{}) BatchItem {
	req := userField(u, field)
	req.Element, req.Data = element, document
	return batchItemOf(t, transport.OpFieldPut, req)
}

func getItem(t testing.TB, u int, field, element string) BatchItem {
	req := userField(u, field)
	req.Element = element
	return batchItemOf(t, transport.OpFieldGet, req)
}

func lenItem(t testing.TB, u int, field string) BatchItem {
	return batchItemOf(t, transport.OpFieldLen, userField(u, field))
}

func incItem(t testing.TB, u int, key string) BatchItem {
	return batchItemOf(t, transport.OpUpdate, UpdateRequest{Database: batchTestDB, Bucket: userBucket(u), Key: key, Inc: map[string]int64{"posts_n": 1}})
}

func batchFrameOf(t testing.TB, items ...BatchItem) *transport.Frame {
	t.Helper()
	return transport.NewFrame(transport.OpBatch, mustMarshalBatch(t, BatchRequest{Items: items}))
}

func mustMarshalBatch(t testing.TB, req any) []byte {
	t.Helper()
	raw, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	return raw
}

// sendBatchOK sends a batch through the server's real client door and returns
// its answers, failing unless it was answered BATCH_OK.
func sendBatchOK(t testing.TB, ctx context.Context, s *Server, frame *transport.Frame) []BatchAnswer {
	t.Helper()
	reply, err := NewHandler(s)(ctx, frame)
	if err != nil {
		t.Fatalf("batch: handler returned a Go error: %v", err)
	}
	if reply.Opcode != transport.OpBatchOK {
		var refusal ErrorResponse
		_ = bson.Unmarshal(reply.Payload, &refusal)
		t.Fatalf("batch answered %s (code %d): %s", reply.Opcode, refusal.Code, refusal.Message)
	}
	var resp BatchResponse
	if err := bson.Unmarshal(reply.Payload, &resp); err != nil {
		t.Fatalf("decoding BATCH_OK: %v", err)
	}
	return resp.Answers
}

func batchAnswerError(t testing.TB, answer BatchAnswer, what string) ErrorResponse {
	t.Helper()
	if transport.Opcode(answer.Opcode) != transport.OpError {
		t.Fatalf("%s answered %s, want ERROR: %s", what, transport.Opcode(answer.Opcode), answer.Payload)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
		t.Fatalf("%s: decoding the error: %v", what, err)
	}
	return refusal
}

func batchAnswerField(t testing.TB, answer BatchAnswer, what string) FieldResponse {
	t.Helper()
	if transport.Opcode(answer.Opcode) == transport.OpError {
		t.Fatalf("%s answered ERROR: %s", what, answer.Payload)
	}
	var resp FieldResponse
	if err := bson.Unmarshal(answer.Payload, &resp); err != nil {
		t.Fatalf("%s: decoding the answer: %v", what, err)
	}
	return resp
}

func feedLen(t testing.TB, s *Server, u int, field string) int64 {
	t.Helper()
	resp, err := s.FieldLen(userField(u, field))
	if err != nil {
		t.Fatalf("FieldLen(User_%d.%s): %v", u, field, err)
	}
	return resp.Len
}

// TestBatchRefusesShape is T-1: every refusal of §3.2 and §3.4, each with its
// text and code, write_outcome only when a write is present, and nothing
// stored (every addressed field unchanged).
func TestBatchRefusesShape(t *testing.T) {
	s := newSocialServer(t, 3)
	push := func(u int) BatchItem { return pushItem(t, u, "Feed", map[string]interface{}{"n": u}) }
	many := func(n int) []BatchItem {
		items := make([]BatchItem, n)
		for i := range items {
			items[i] = push(1 + i%3)
		}
		return items
	}
	big := strings.Repeat("x", 6<<20)
	bigPush := func(u int) BatchItem { return pushItem(t, u, "Feed", map[string]interface{}{"blob": big}) }
	withPayload := func(op transport.Opcode, payload bson.D) BatchItem {
		raw, err := bson.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return BatchItem{Opcode: int32(op), Payload: raw}
	}
	rawBatch := func(items bson.A) *transport.Frame {
		return transport.NewFrame(transport.OpBatch, mustMarshalBatch(t, bson.D{{Key: "items", Value: items}}))
	}
	popReq := userField(2, "Feed")
	listReq := userField(2, "Inbox")
	cases := []struct {
		name     string
		frame    *transport.Frame
		code     transport.ErrorCode
		outcome  string
		contains []string
	}{
		{"empty", batchFrameOf(t), transport.ErrInvalidArgument, "",
			[]string{"A batch needs at least one item. Add items, or send the request on its own."}},
		{"1,001 items", batchFrameOf(t, many(1001)...), transport.ErrBatchTooLarge, outcomeNotApplied,
			[]string{"This batch has 1,001 items; a batch carries at most 1,000. Send it as 2 batches: items 1 to 1,000, then 1,001 to 1,001."}},
		{"over 16 MiB", batchFrameOf(t, bigPush(1), bigPush(2), bigPush(3)), transport.ErrBatchTooLarge, outcomeNotApplied,
			[]string{"This batch is 18.0 MiB; a batch carries at most 16 MiB. Send it as 2 batches, the first ending at item 2, or store large values in a blob field"}},
		{"one item over 16 MiB", batchFrameOf(t, push(1), pushItem(t, 2, "Feed", map[string]interface{}{"blob": strings.Repeat("y", 17<<20)})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{`Item 2 of 2, db.User_2["2"].Feed.push(…), is 17.0 MiB, more than a whole batch carries (16 MiB). Send it on its own.`}},
		{"nested batch", batchFrameOf(t, push(1), withPayload(transport.OpBatch, bson.D{{Key: "bucket", Value: "User_1"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 is itself a batch; a batch cannot carry another. Put its items in this batch."}},
		{"unknown opcode", batchFrameOf(t, push(1), withPayload(0xEE, bson.D{{Key: "bucket", Value: "User_1"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 names opcode 0xEE, which is not a hoardDB request. A batch carries push, put, delete, get, len and peek of a store field, and an update of one key."}},
		{"node-to-node opcode", batchFrameOf(t, push(1), withPayload(transport.OpReplPush, bson.D{{Key: "bucket", Value: "User_1"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 is REPL_PUSH, which only cluster members send."}},
		{"bucket verb", batchFrameOf(t, lenItem(t, 1, "Feed"), withPayload(transport.OpPut, bson.D{{Key: "database", Value: batchTestDB}, {Key: "bucket", Value: "User_1"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 is PUT, which a batch does not carry. Send it on its own."}},
		{"pop", batchFrameOf(t, push(1), batchItemOf(t, transport.OpFieldPop, popReq)), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{`Item 2 of 2, db.User_2["2"].Feed.pop(), is a pop, which a batch does not carry: a lost reply would lose the popped entries. Send it on its own (run 'use social' first).`}},
		{"list", batchFrameOf(t, lenItem(t, 1, "Feed"), batchItemOf(t, transport.OpFieldList, listReq)), transport.ErrInvalidArgument, "",
			[]string{`Item 2 of 2, a list of db.User_2["2"].Inbox, is paged and its answer can fill a reply by itself, so a batch does not carry it.`}},
		{"update with filter", batchFrameOf(t, push(1), batchItemOf(t, transport.OpUpdate, UpdateRequest{Database: batchTestDB, Bucket: "User_1", Filter: map[string]interface{}{"name": "1"}, Inc: map[string]int64{"posts_n": 1}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2, an update of db.User_1 with a filter, is a scan of the bucket, which a batch does not carry."}},
		{"update multi", batchFrameOf(t, push(1), batchItemOf(t, transport.OpUpdate, UpdateRequest{Database: batchTestDB, Bucket: "User_1", Multi: true, Inc: map[string]int64{"posts_n": 1}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"an update of every matching document of db.User_1, is a scan of the bucket"}},
		{"item writeconcern", batchFrameOf(t, push(1), withPayload(transport.OpFieldPush, bson.D{{Key: "database", Value: batchTestDB}, {Key: "bucket", Value: "User_2"}, {Key: "key", Value: "2"}, {Key: "field", Value: "Feed"}, {Key: "writeconcern", Value: "all"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 names a write concern (writeconcern); a batch takes one, on the batch. Remove it from the item and set writeconcern on the batch."}},
		{"item write_concern", batchFrameOf(t, push(1), withPayload(transport.OpFieldPush, bson.D{{Key: "database", Value: batchTestDB}, {Key: "bucket", Value: "User_2"}, {Key: "key", Value: "2"}, {Key: "field", Value: "Feed"}, {Key: "write_concern", Value: "all"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 names a write concern (write_concern)"}},
		{"opcode of the wrong type", rawBatch(bson.A{bson.D{{Key: "opcode", Value: 208.0}, {Key: "payload", Value: bson.D{{Key: "bucket", Value: "User_1"}}}}}), transport.ErrInvalidArgument, "",
			[]string{"Item 1 of 1: opcode must be a 32-bit integer from 0 to 255; it is a double."}},
		{"opcode out of range", rawBatch(bson.A{bson.D{{Key: "opcode", Value: int32(300)}, {Key: "payload", Value: bson.D{{Key: "bucket", Value: "User_1"}}}}}), transport.ErrInvalidArgument, "",
			[]string{"Item 1 of 1: opcode must be a 32-bit integer from 0 to 255; it is 300."}},
		{"missing payload", rawBatch(bson.A{bson.D{{Key: "opcode", Value: int32(transport.OpFieldPush)}}}), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 1 of 1 has no payload document. Each item is {opcode, payload}, where payload is the request the item would send on its own."}},
		{"no bucket", batchFrameOf(t, push(1), withPayload(transport.OpFieldPush, bson.D{{Key: "database", Value: batchTestDB}, {Key: "key", Value: "2"}, {Key: "field", Value: "Feed"}})), transport.ErrInvalidArgument, outcomeNotApplied,
			[]string{"Item 2 of 2 names no bucket; every request a batch carries is addressed to one bucket."}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refusal := refusalFrom(t, NewHandler(s), tc.frame, tc.name)
			requireOutcome(t, refusal, tc.name, tc.outcome, tc.code)
			for _, want := range append(tc.contains, batchNothingRun) {
				if !strings.Contains(refusal.Message, want) {
					t.Errorf("%s: message %q does not contain %q", tc.name, refusal.Message, want)
				}
			}
			for u := 1; u <= 3; u++ {
				if n := feedLen(t, s, u, "Feed"); n != 0 {
					t.Fatalf("%s: User_%d.Feed holds %d elements after a refused batch; nothing may be stored", tc.name, u, n)
				}
			}
		})
	}
}

// TestBatchSplitAdviceIsComputed: the split of a batch over the count is
// computed, for two batches and for many.
func TestBatchSplitAdviceIsComputed(t *testing.T) {
	if got, want := countSplitAdvice(1204), "Send it as 2 batches: items 1 to 1,000, then 1,001 to 1,204."; got != want {
		t.Errorf("countSplitAdvice(1204) = %q, want %q", got, want)
	}
	if got, want := countSplitAdvice(50204), "Send it as 51 batches of at most 1,000 items (items 1 to 1,000, 1,001 to 2,000, …, 50,001 to 50,204)."; got != want {
		t.Errorf("countSplitAdvice(50204) = %q, want %q", got, want)
	}
}

// TestBatchAuthorizesEveryItem is T-2: a reader's batch of nine gets and one
// push is refused whole, and a writer on database A cannot push to B by
// putting the push after one to A. Nothing is applied in either case.
func TestBatchAuthorizesEveryItem(t *testing.T) {
	s := newSocialServer(t, 2)
	seedOtherDatabase(t, s)
	reader := &transport.Session{}
	reader.SetIdentity(transport.Identity{Username: "reader", Roles: []string{"reader"}, Databases: []string{batchTestDB},
		Grants: []transport.ScopedGrant{{Role: "reader", DB: batchTestDB}}})
	items := make([]BatchItem, 0, 10)
	for i := 0; i < 9; i++ {
		items = append(items, getItem(t, 1, "Posts", fmt.Sprint(i)))
	}
	items = append(items, pushItem(t, 2, "Feed", map[string]interface{}{"n": 1}))
	answer, err := NewHandler(s)(transport.WithSession(context.Background(), reader), batchFrameOf(t, items...))
	if err != nil {
		t.Fatal(err)
	}
	refusal := errorAnswerOf(t, answer, "a reader's batch with a push")
	requireOutcome(t, refusal, "a reader's batch with a push", outcomeNotApplied, transport.ErrPermissionDenied)
	if !strings.Contains(refusal.Message, `Item 10 of 10 (db.User_2["2"].Feed.push(…)): permission denied`) {
		t.Errorf("the refusal does not name item 10 and the lone refusal: %s", refusal.Message)
	}

	writer := &transport.Session{}
	writer.SetIdentity(transport.Identity{Username: "alice", Roles: []string{"writer"}, Databases: []string{batchTestDB},
		Grants: []transport.ScopedGrant{{Role: "writer", DB: batchTestDB}}})
	other := FieldRequest{Database: "other", Bucket: "User_9", Key: "9", Field: "Feed", Data: map[string]interface{}{"n": 1}}
	answer, err = NewHandler(s)(transport.WithSession(context.Background(), writer),
		batchFrameOf(t, pushItem(t, 1, "Feed", map[string]interface{}{"n": 1}), batchItemOf(t, transport.OpFieldPush, other)))
	if err != nil {
		t.Fatal(err)
	}
	refusal = errorAnswerOf(t, answer, "a writer's batch into another database")
	requireOutcome(t, refusal, "a writer's batch into another database", outcomeNotApplied, transport.ErrPermissionDenied)
	if n := feedLen(t, s, 1, "Feed"); n != 0 {
		t.Fatalf("User_1.Feed holds %d elements; the push to the permitted database must not apply when the batch is refused", n)
	}
	resp, err := s.FieldLen(FieldRequest{Database: "other", Bucket: "User_9", Key: "9", Field: "Feed"})
	if err != nil || resp.Len != 0 {
		t.Fatalf("other.User_9.Feed = %+v, %v; nothing may be applied", resp, err)
	}
}

func seedOtherDatabase(t *testing.T, s *Server) {
	t.Helper()
	resp := createWithStores(t, s, "other", "User_9", BucketTypeHash, StoreDeclaration{Name: "Feed", Engine: "fifoStore"})
	if !resp.Success {
		t.Fatalf("creating other.User_9: %s", resp.Message)
	}
	if inserted, err := s.Insert(InsertRequest{Database: "other", Bucket: "User_9", Key: "9", Data: map[string]interface{}{"name": "9"}}); err != nil || !inserted.Success {
		t.Fatalf("inserting other.User_9: %+v, %v", inserted, err)
	}
}

// TestBatchItemGatesMatchLoneRequest is T-3: an item's gate refuses the batch
// with the lone request's text and code, and an update is held to this node's
// document limit, never the client's.
func TestBatchItemGatesMatchLoneRequest(t *testing.T) {
	s := newSocialServer(t, 2)
	s.config.Storage.MaxDocumentBytes = 1 << 20
	limit := int64(s.maxDocumentBytes())
	oversized := pushItem(t, 2, "Feed", map[string]interface{}{"blob": strings.Repeat("z", int(limit)+1)})
	lone, err := NewHandler(s)(context.Background(), transport.NewFrame(transport.OpFieldPush, oversized.Payload))
	if err != nil {
		t.Fatal(err)
	}
	loneRefusal := errorAnswerOf(t, lone, "the lone push")
	refusal := refusalFrom(t, NewHandler(s), batchFrameOf(t, pushItem(t, 1, "Feed", map[string]interface{}{"n": 1}), oversized), "a batch with an oversized push")
	if refusal.Code != loneRefusal.Code {
		t.Errorf("the batch's code is %d, the lone push's %d", refusal.Code, loneRefusal.Code)
	}
	if !strings.Contains(refusal.Message, strings.TrimSuffix(loneRefusal.Message, ".")) {
		t.Errorf("the batch's refusal %q does not carry the lone refusal %q", refusal.Message, loneRefusal.Message)
	}

	// The update limit is a stamp, not a refusal: an item that claims a
	// limit of 1 GiB is held to this node's.
	batch, err := decodeBatch(mustMarshalBatch(t, BatchRequest{Items: []BatchItem{
		batchItemOf(t, transport.OpUpdate, UpdateRequest{Database: batchTestDB, Bucket: "User_1", Key: "1", Inc: map[string]int64{"posts_n": 1}, MaxDocumentBytes: 1 << 30}),
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.admitBatchItems(context.Background(), transport.NewFrame(transport.OpBatch, nil), batch); err != nil {
		t.Fatal(err)
	}
	var stamped UpdateRequest
	if err := bson.Unmarshal(batch.items[0].payload, &stamped); err != nil {
		t.Fatal(err)
	}
	if int64(stamped.MaxDocumentBytes) != limit {
		t.Fatalf("the batched update runs held to %d bytes; the door must stamp this node's limit, %d", stamped.MaxDocumentBytes, limit)
	}
}

// TestBatchDepthMeasuredPerItem is T-4: an item payload at exactly its lone
// depth limit passes inside a batch; one level deeper is refused.
func TestBatchDepthMeasuredPerItem(t *testing.T) {
	s := newSocialServer(t, 1)
	nested := func(depth int) map[string]interface{} {
		doc := map[string]interface{}{"leaf": 1}
		for i := 1; i < depth; i++ {
			doc = map[string]interface{}{"n": doc}
		}
		return doc
	}
	// The payload's root is one level; data is the second; the element's own
	// levels follow. requestDepthLimit allows maxDocumentDepth + 1.
	atLimit := pushItem(t, 1, "Feed", nested(maxDocumentDepth))
	if _, err := validateRequest(transport.OpFieldPush, atLimit.Payload); err != nil {
		t.Fatalf("the item at its limit is refused alone: %v", err)
	}
	answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t, atLimit))
	batchAnswerField(t, answers[0], "the item at its depth limit")
	deeper := pushItem(t, 1, "Feed", nested(maxDocumentDepth+1))
	if _, err := validateRequest(transport.OpFieldPush, deeper.Payload); err == nil {
		t.Fatal("the deeper item is not refused alone; the test's depth is off")
	}
	refusal := refusalFrom(t, NewHandler(s), batchFrameOf(t, deeper), "a batch with a deep item")
	requireOutcome(t, refusal, "a batch with a deep item", outcomeNotApplied, transport.ErrInvalidArgument)
	if !strings.Contains(refusal.Message, "Item 1 of 1") {
		t.Errorf("the depth refusal does not name the item: %s", refusal.Message)
	}
}

// TestBatchItemIsValidatedNotOnlyMeasured holds the BATCH door to the
// boundary every lone frame meets (build step 7b): an item is validated, not
// only measured for depth. A boolean byte of 2 nests no deeper than 1, so a
// depth check alone lets it through; Validate refuses it, at the item gate
// (admitBatchItems) and at the envelope, naming the item.
func TestBatchItemIsValidatedNotOnlyMeasured(t *testing.T) {
	s := newSocialServer(t, 1)
	item := pushItem(t, 1, "Feed", map[string]interface{}{"flag": true})
	boolean := []byte("\x08flag\x00\x01")
	at := strings.Index(string(item.Payload), string(boolean))
	if at < 0 {
		t.Fatal("the item's payload does not hold the boolean element the test corrupts")
	}
	malformed := append(bson.Raw(nil), item.Payload...)
	malformed[at+len(boolean)-1] = 0x02
	item.Payload = malformed

	if _, err := validateRequest(transport.OpFieldPush, item.Payload); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("the item alone is not refused as malformed: %v", err)
	}
	batch, err := decodeBatch(mustMarshalBatch(t, BatchRequest{Items: []BatchItem{item}}))
	if err != nil {
		t.Fatalf("the envelope holding the item does not decode: %v", err)
	}
	if err := s.admitBatchItems(context.Background(), transport.NewFrame(transport.OpBatch, nil), batch); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("the item gate admits an item Validate refuses: %v", err)
	}
	refusal := refusalFrom(t, NewHandler(s), batchFrameOf(t, item), "a batch with a malformed item")
	requireOutcome(t, refusal, "a batch with a malformed item", outcomeNotApplied, transport.ErrInvalidArgument)
	if !strings.Contains(refusal.Message, "Item 1 of 1") || !strings.Contains(refusal.Message, "not well-formed BSON") {
		t.Errorf("the refusal does not name the item and the malformed value: %s", refusal.Message)
	}
}

// TestBatchPartialFailure is T-7: item 2's bucket is missing, so it is
// refused not_applied with the 'show buckets' fix while items 1 and 3 apply;
// an update of a missing key answers the verb's own success false.
func TestBatchPartialFailure(t *testing.T) {
	s := newSocialServer(t, 3)
	missing := FieldRequest{Database: batchTestDB, Bucket: "User_404", Key: "404", Field: "Feed", Data: map[string]interface{}{"n": 1}}
	answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t,
		pushItem(t, 1, "Feed", map[string]interface{}{"n": 1}),
		batchItemOf(t, transport.OpFieldPush, missing),
		pushItem(t, 3, "Feed", map[string]interface{}{"n": 3}),
		incItem(t, 2, "ghost"),
	))
	if len(answers) != 4 {
		t.Fatalf("%d answers for 4 items", len(answers))
	}
	batchAnswerField(t, answers[0], "item 1")
	refusal := batchAnswerError(t, answers[1], "item 2")
	requireOutcome(t, refusal, "item 2", outcomeNotApplied, transport.ErrNotFound)
	if !strings.Contains(refusal.Message, "show buckets") {
		t.Errorf("item 2's refusal does not name the fix: %s", refusal.Message)
	}
	batchAnswerField(t, answers[2], "item 3")
	if feedLen(t, s, 1, "Feed") != 1 || feedLen(t, s, 3, "Feed") != 1 {
		t.Fatal("items 1 and 3 did not apply")
	}
	if transport.Opcode(answers[3].Opcode) != transport.OpUpdate {
		t.Fatalf("the update of a missing key answered %s; it answers the verb's own reply", transport.Opcode(answers[3].Opcode))
	}
	if replySucceeded(transport.OpUpdate, answers[3].Payload) {
		t.Fatal("the update of a missing key answered success")
	}
	items := []batchItem{{op: transport.OpUpdate}}
	if got := classifyAnswer(items[0], batchAnswer{opcode: transport.OpUpdate, payload: answers[3].Payload}); got != batchItemVerbFailed {
		t.Fatalf("the update of a missing key is classified %s, want %s", got, batchItemVerbFailed)
	}
}

// TestBatchSameBucketOrder is T-8: 50 pushes to one fifo field in one batch,
// item 1 held at the apply hook until item 2 has had time to start, come back
// in batch order.
func TestBatchSameBucketOrder(t *testing.T) {
	s := newSocialServer(t, 1)
	release := make(chan struct{})
	held := make(chan struct{}, 1)
	hook := func(frame *transport.Frame) {
		if frame.Opcode != transport.OpFieldPush || !strings.Contains(string(frame.Payload), "first") {
			return
		}
		held <- struct{}{}
		<-release
	}
	dispatchApplyHook.Store(&hook)
	t.Cleanup(func() { dispatchApplyHook.Store(nil) })

	items := []BatchItem{pushItem(t, 1, "Feed", map[string]interface{}{"n": int32(0), "tag": "first"})}
	for n := 1; n < 50; n++ {
		items = append(items, pushItem(t, 1, "Feed", map[string]interface{}{"n": int32(n)}))
	}
	done := make(chan []BatchAnswer, 1)
	go func() { done <- sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...)) }()
	<-held
	if n := feedLen(t, s, 1, "Feed"); n != 0 {
		t.Fatalf("Feed holds %d elements while item 1 is held; a bucket's items apply in batch order", n)
	}
	close(release)
	<-done
	assertFeedOrder(t, s, 1, 50)
}

func assertFeedOrder(t *testing.T, s *Server, u, want int) {
	t.Helper()
	req := FieldPeekRequest{FieldRequest: userField(u, "Feed"), Limit: want}
	resp, err := s.FieldPeek(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != want {
		t.Fatalf("peek returned %d elements, want %d", len(resp.Items), want)
	}
	for i, entry := range resp.Items {
		n, _ := entry.Element.Lookup("n").AsInt64OK()
		if n != int64(i) {
			t.Fatalf("element %d holds n=%d; the pushes did not apply in batch order", i, n)
		}
	}
}

// TestBatchSameBucketOrderUnheld is T-8's unheld run, for -race -count=20.
func TestBatchSameBucketOrderUnheld(t *testing.T) {
	s := newSocialServer(t, 1)
	items := make([]BatchItem, 0, 50)
	for n := 0; n < 50; n++ {
		items = append(items, pushItem(t, 1, "Feed", map[string]interface{}{"n": int32(n)}))
	}
	sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
	assertFeedOrder(t, s, 1, 50)
}

// TestBatchReadSeesEarlierWrite is T-9 on one node: a put then a get of the
// same element; the get has not answered while the put is held, and then
// finds the element.
func TestBatchReadSeesEarlierWrite(t *testing.T) {
	s := newSocialServer(t, 1)
	release := make(chan struct{})
	held := make(chan struct{}, 1)
	var getStarted = make(chan struct{}, 1)
	hook := func(frame *transport.Frame) {
		if frame.Opcode == transport.OpFieldPut {
			held <- struct{}{}
			<-release
		}
	}
	dispatchApplyHook.Store(&hook)
	t.Cleanup(func() { dispatchApplyHook.Store(nil) })
	done := make(chan []BatchAnswer, 1)
	go func() {
		done <- sendBatchOK(t, context.Background(), s, batchFrameOf(t,
			putItem(t, 1, "Posts", "p-1", map[string]interface{}{"text": "hello"}),
			getItem(t, 1, "Posts", "p-1")))
		getStarted <- struct{}{}
	}()
	<-held
	select {
	case <-getStarted:
		t.Fatal("the batch answered while its put was held")
	default:
	}
	close(release)
	answers := <-done
	got := batchAnswerField(t, answers[1], "the get after the put")
	if !got.Found {
		t.Fatal("the get did not find the element its chain's put wrote")
	}
}

// TestBatchChainStopsAfterFailure is T-14: items 1 to 3 name bucket X and item
// 4 bucket Y; item 2 is an update of a missing key. Item 1 applies, item 2
// answers success false, item 3 is not run and names item 2, item 4 applies.
func TestBatchChainStopsAfterFailure(t *testing.T) {
	s := newSocialServer(t, 2)
	answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t,
		pushItem(t, 1, "Feed", map[string]interface{}{"n": 1}),
		incItem(t, 1, "ghost"),
		pushItem(t, 1, "Feed", map[string]interface{}{"n": 2}),
		pushItem(t, 2, "Feed", map[string]interface{}{"n": 1}),
	))
	batchAnswerField(t, answers[0], "item 1")
	if replySucceeded(transport.Opcode(answers[1].Opcode), answers[1].Payload) {
		t.Fatal("item 2, an update of a missing key, answered success")
	}
	stopped := batchAnswerError(t, answers[2], "item 3")
	requireOutcome(t, stopped, "item 3", outcomeNotApplied, transport.ErrUnavailable)
	want := `Not run: item 2 of 4 (db.User_1.update("ghost", …)), an earlier item for the same bucket, was not applied, and items of one bucket apply in order. Resend from item 2.`
	if stopped.Message != want {
		t.Errorf("item 3's message is %q, want %q", stopped.Message, want)
	}
	batchAnswerField(t, answers[3], "item 4")
	if n := feedLen(t, s, 1, "Feed"); n != 1 {
		t.Fatalf("User_1.Feed holds %d elements, want 1: item 3 must not apply", n)
	}
	if n := feedLen(t, s, 2, "Feed"); n != 1 {
		t.Fatalf("User_2.Feed holds %d elements, want 1: another bucket's chain is unaffected", n)
	}
}

// TestBatchReplyShapesMatchLoneReplies is T-6: for each carried verb, the
// answer's payload is the lone reply's, ids and times aside.
func TestBatchReplyShapesMatchLoneReplies(t *testing.T) {
	batched := newSocialServer(t, 1)
	lone := newSocialServer(t, 1)
	items := []BatchItem{
		putItem(t, 1, "Posts", "p-1", map[string]interface{}{"text": "hello"}),
		pushItem(t, 1, "Feed", map[string]interface{}{"n": 1}),
		getItem(t, 1, "Posts", "p-1"),
		getItem(t, 1, "Posts", "missing"),
		lenItem(t, 1, "Feed"),
		batchItemOf(t, transport.OpFieldPeek, FieldPeekRequest{FieldRequest: userField(1, "Feed"), Limit: 2}),
		incItem(t, 1, "1"),
		batchItemOf(t, transport.OpFieldDelete, FieldRequest{Database: batchTestDB, Bucket: "User_1", Key: "1", Field: "Posts", Element: "p-1"}),
	}
	answers := sendBatchOK(t, context.Background(), batched, batchFrameOf(t, items...))
	for i, item := range items {
		reply, err := NewHandler(lone)(context.Background(), transport.NewFrame(transport.Opcode(item.Opcode), item.Payload))
		if err != nil {
			t.Fatal(err)
		}
		if transport.Opcode(answers[i].Opcode) != reply.Opcode {
			t.Errorf("item %d (%s) answered %s, the lone request %s", i+1, transport.Opcode(item.Opcode), transport.Opcode(answers[i].Opcode), reply.Opcode)
			continue
		}
		if got, want := normaliseAnswer(t, answers[i].Payload), normaliseAnswer(t, reply.Payload); got != want {
			t.Errorf("item %d (%s): the batch answered %s, the lone request %s", i+1, transport.Opcode(item.Opcode), got, want)
		}
	}
}

// normaliseAnswer renders a reply with its entry ids blanked: they are minted
// per write and differ between two servers.
func normaliseAnswer(t *testing.T, payload []byte) string {
	t.Helper()
	var doc bson.M
	if err := bson.Unmarshal(payload, &doc); err != nil {
		t.Fatal(err)
	}
	blankMinted(doc)
	return fmt.Sprint(doc)
}

// blankMinted deletes every entry_id and _id_ in a decoded reply.
func blankMinted(value any) {
	switch v := value.(type) {
	case bson.M:
		delete(v, "entry_id")
		delete(v, "_id_")
		for _, inner := range v {
			blankMinted(inner)
		}
	case bson.A:
		for _, inner := range v {
			blankMinted(inner)
		}
	}
}
