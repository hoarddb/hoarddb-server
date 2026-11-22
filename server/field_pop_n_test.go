package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// popFieldRequest is a pop of a store field of p1 in app.Post.
func popFieldRequest(field string, limit int) FieldPopRequest {
	return FieldPopRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: field}, Limit: limit}
}

// fieldLeft is the n of every element left in a field, in pop order. It reads
// the store directly because the elements a test leaves behind can be larger
// than the result ceiling that the test is about.
func fieldLeft(t testing.TB, s *Server, field string) []int {
	t.Helper()
	var numbers []int
	err := s.readField(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: field}, fieldVerbPeek, func(snap storage.KVSnapshot, resolved resolvedField) error {
		return store.FieldPeekN(snap, resolved.ref, maxPeekLimit, func(entry store.FieldEntry) error {
			n, ok := bson.Raw(entry.Value).Lookup("n").AsInt64OK()
			if !ok {
				return fmt.Errorf("element %v has no integer n", bson.Raw(entry.Value))
			}
			numbers = append(numbers, int(n))
			return nil
		})
	})
	if err != nil {
		t.Fatalf("walking %s: %v", field, err)
	}
	return numbers
}

// TestFieldPopAnswersItemsNotElement: a field pop answers items that carry
// their identity, equal to a peek just before, and leaves element, position,
// priority and entry_id off the top level. Position is present at 0
// (BUG-0558).
func TestFieldPopAnswersItemsNotElement(t *testing.T) {
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			s := newTestServer(t)
			seedPeekPost(t, s)
			pushPeekFieldElements(t, s, engine.field, 5, "")
			peeked, err := s.FieldPeek(peekFieldRequest(engine.field, 1))
			if err != nil || len(peeked.Items) != 1 {
				t.Fatalf("peek: %+v, %v", peeked, err)
			}
			payload, err := bson.Marshal(popFieldRequest(engine.field, 0))
			if err != nil {
				t.Fatal(err)
			}
			answer, err := s.dispatch(context.Background(), transport.NewFrame(transport.OpFieldPop, payload))
			if err != nil || answer.Opcode != transport.OpFieldPop {
				t.Fatalf("dispatching the pop: %v, %v", answer, err)
			}
			raw := bson.Raw(answer.Payload)
			for _, member := range []string{"element", "position", "priority", "entry_id", "element_key"} {
				if raw.Lookup(member).Type != 0 {
					t.Errorf("the answer carries a top-level %q: %s", member, raw)
				}
			}
			var popped FieldResponse
			if err := bson.Unmarshal(answer.Payload, &popped); err != nil {
				t.Fatalf("decoding: %v", err)
			}
			if !popped.Found || len(popped.Items) != 1 {
				t.Fatalf("answer = %+v; want found with one item", popped)
			}
			item, want := popped.Items[0], peeked.Items[0]
			if !bytes.Equal(item.Element, want.Element) || item.EntryID != want.EntryID ||
				!reflect.DeepEqual(wirePlacementText(item.Position, item.Priority), wirePlacementText(want.Position, want.Priority)) {
				t.Fatalf("popped item %+v differs from the peeked %+v", item, want)
			}
			if engine.engine == "heap" && (item.Priority == nil || item.EntryID == "") {
				t.Errorf("a heap item lacks priority or entry id: %+v", item)
			}
			if engine.engine != "heap" && (item.Position == nil || *item.Position != int64(engine.popOrder[0]-1)) {
				t.Errorf("item position %v, want %d (present, also at 0)", item.Position, engine.popOrder[0]-1)
			}
		})
	}
}

// TestFieldPopLimitReturnsItemsInPopOrder: limit 3 of 5 takes the three a peek
// showed first.
func TestFieldPopLimitReturnsItemsInPopOrder(t *testing.T) {
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			s := newTestServer(t)
			seedPeekPost(t, s)
			pushPeekFieldElements(t, s, engine.field, 5, "")
			resp, err := s.FieldPopCtx(context.Background(), popFieldRequest(engine.field, 3))
			if err != nil || !resp.Found {
				t.Fatalf("pop limit 3 = %+v, %v", resp, err)
			}
			if got := fieldItemNumbers(t, resp.Items); !reflect.DeepEqual(got, engine.popOrder[:3]) {
				t.Fatalf("popped %v, want %v", got, engine.popOrder[:3])
			}
			if got := fieldLeft(t, s, engine.field); !reflect.DeepEqual(got, engine.popOrder[3:]) {
				t.Fatalf("left %v, want %v", got, engine.popOrder[3:])
			}
			past, err := s.FieldPopCtx(context.Background(), popFieldRequest(engine.field, 10))
			if err != nil || !past.Found || len(past.Items) != 2 {
				t.Fatalf("pop limit 10 of 2 = %+v, %v; want the two there are", past, err)
			}
			empty, err := s.FieldPopCtx(context.Background(), popFieldRequest(engine.field, 3))
			if err != nil || empty.Found || len(empty.Items) != 0 {
				t.Fatalf("pop of an empty field = %+v, %v; want found false", empty, err)
			}
		})
	}
}

// TestFieldPopLimitRefused: above the cap and negative are refused with the
// pop texts, and nothing is removed.
func TestFieldPopLimitRefused(t *testing.T) {
	qs, c := peekCeilingServer(t)
	seedPeekPost(t, qs.core)
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			pushPeekFieldElements(t, qs.core, engine.field, 3, "")
			for _, tc := range []struct {
				limit int
				want  string
			}{{101, "101 is above the pop maximum of 100"}, {-1, "-1 is not valid; pop({limit: N}) takes a whole number of 1 or more"}} {
				request := requestFrame(t, transport.OpFieldPop, popFieldRequest(engine.field, tc.limit))
				refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "the refused field pop")
				if refused.Code != int(transport.ErrInvalidArgument) || refused.WriteOutcome != outcomeNotApplied || !strings.Contains(refused.Message, tc.want) {
					t.Fatalf("limit %d answered code %d outcome %q: %s", tc.limit, refused.Code, refused.WriteOutcome, refused.Message)
				}
			}
			if left := fieldLeft(t, qs.core, engine.field); len(left) != 3 {
				t.Fatalf("a refused pop left %v", left)
			}
		})
	}
}

// TestFieldPopOverResultCeilingRemovesNothing: the field row of the centre of
// the spec. The refusal names the field, leaves as RESULT_TOO_LARGE, and every
// element is still there.
func TestFieldPopOverResultCeilingRemovesNothing(t *testing.T) {
	qs, c := peekCeilingServer(t)
	seedPeekPost(t, qs.core)
	pad := strings.Repeat("x", 400<<10)
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			pushPeekFieldElements(t, qs.core, engine.field, 5, pad)
			refusalsBefore := popRefusalsCounted()
			resp, err := qs.core.FieldPopCtx(context.Background(), popFieldRequest(engine.field, 5))
			if !errors.Is(err, ErrResultTooLarge) || resp != nil {
				t.Fatalf("limit 5 of 400 KiB elements = %+v, %v; want RESULT_TOO_LARGE and no items", resp, err)
			}
			if got := popRefusalsCounted() - refusalsBefore; got != 1 {
				t.Fatalf("the POP refusal counter moved by %v, want 1", got)
			}
			request := requestFrame(t, transport.OpFieldPop, popFieldRequest(engine.field, 5))
			refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "the oversized field pop")
			if refused.Code != int(transport.ErrResultTooLarge) || refused.MaxResultBytes != 1<<20 || refused.RetainedBytes <= 0 || refused.WriteOutcome != outcomeNotApplied {
				t.Fatalf("code %d, max_result_bytes %d, retained_bytes %d, outcome %q (%q)", refused.Code, refused.MaxResultBytes, refused.RetainedBytes, refused.WriteOutcome, refused.Message)
			}
			for _, want := range []string{`pop({limit: 5}) on app.Post["p1"].` + engine.field, "so nothing was removed", "pop({limit: 2}) fits"} {
				if !strings.Contains(refused.Message, want) {
					t.Errorf("message lacks %q: %s", want, refused.Message)
				}
			}
			if left := fieldLeft(t, qs.core, engine.field); !reflect.DeepEqual(left, engine.popOrder) {
				t.Fatalf("after the refusal the field holds %v, want all five in order %v", left, engine.popOrder)
			}
		})
	}
}

// TestFieldPopAnswersAnElementLargerThanTheCeiling: the first element is
// exempt, and two such elements are refused together and removed neither.
func TestFieldPopAnswersAnElementLargerThanTheCeiling(t *testing.T) {
	qs, _ := peekCeilingServer(t)
	seedPeekPost(t, qs.core)
	huge := strings.Repeat("h", 1536<<10)
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			pushPeekFieldElements(t, qs.core, engine.field, 4, huge)
			for _, limit := range []int{0, 1} {
				resp, err := qs.core.FieldPopCtx(context.Background(), popFieldRequest(engine.field, limit))
				if err != nil || !resp.Found || len(resp.Items) != 1 {
					t.Fatalf("pop limit %d of an element over the ceiling = %+v, %v; want the element", limit, resp, err)
				}
			}
			resp, err := qs.core.FieldPopCtx(context.Background(), popFieldRequest(engine.field, 2))
			if !errors.Is(err, ErrResultTooLarge) || resp != nil {
				t.Fatalf("pop limit 2 of two elements over the ceiling = %+v, %v; want RESULT_TOO_LARGE", resp, err)
			}
			if left := fieldLeft(t, qs.core, engine.field); len(left) != 2 {
				t.Fatalf("the refused pop left %v, want both elements", left)
			}
		})
	}
}

// TestFieldPopClientCannotRaiseTheCeiling (single node): a result_ceiling the
// client sends is ignored by a node that logs nothing.
func TestFieldPopClientCannotRaiseTheCeiling(t *testing.T) {
	qs, c := peekCeilingServer(t)
	seedPeekPost(t, qs.core)
	pushPeekFieldElements(t, qs.core, "Comments", 5, strings.Repeat("x", 400<<10))
	request := popFieldRequest("Comments", 5)
	request.ResultCeiling = 1 << 30
	frame := requestFrame(t, transport.OpFieldPop, request)
	refused := decodeErrorFrame(t, c.exchange(frame)[frame.RequestID], "a field pop that sets its own ceiling")
	if refused.Code != int(transport.ErrResultTooLarge) || refused.MaxResultBytes != 1<<20 {
		t.Fatalf("code %d, max_result_bytes %d (%q); want RESULT_TOO_LARGE at the node's 1 MiB", refused.Code, refused.MaxResultBytes, refused.Message)
	}
	if left := fieldLeft(t, qs.core, "Comments"); len(left) != 5 {
		t.Fatalf("the refused pop left %v", left)
	}
}

// TestPopLimitOnAnotherFieldVerbIsNotExpressible: only the peek and pop
// requests have a limit. A limit on FieldRequest would be a field push, put,
// get, delete and len accept and silently ignore.
func TestPopLimitOnAnotherFieldVerbIsNotExpressible(t *testing.T) {
	limitTagged := func(kind reflect.Type) bool {
		for index := 0; index < kind.NumField(); index++ {
			if strings.Split(kind.Field(index).Tag.Get("bson"), ",")[0] == "limit" {
				return true
			}
		}
		return false
	}
	if limitTagged(reflect.TypeOf(FieldRequest{})) {
		t.Fatal("FieldRequest carries a limit")
	}
	if !limitTagged(reflect.TypeOf(FieldPeekRequest{})) || !limitTagged(reflect.TypeOf(FieldPopRequest{})) {
		t.Fatal("FieldPeekRequest and FieldPopRequest must both carry a limit")
	}
}

// TestFieldPopOnHashFieldKeepsWrongVerbText: a pop of a hash or blob field is
// refused with wrongVerbErr's text, with and without a limit.
func TestFieldPopOnHashFieldKeepsWrongVerbText(t *testing.T) {
	s := newTestServer(t)
	seedPeekPost(t, s)
	for _, field := range []string{"Tags", "Files"} {
		for _, limit := range []int{0, 2} {
			resp, err := s.FieldPopCtx(context.Background(), popFieldRequest(field, limit))
			if !errors.Is(err, ErrFieldWrongVerb) || resp != nil {
				t.Fatalf("pop of %s with limit %d = %+v, %v; want ErrFieldWrongVerb", field, limit, resp, err)
			}
			if !strings.Contains(err.Error(), `"pop" is not an operation on "`+field+`"`) {
				t.Errorf("the refusal changed: %v", err)
			}
		}
	}
}

// TestFieldPopNRemovesNestedStoresAndUnlinksTheirFiles is the server half of
// the store's TestFieldPopNRemovesNestedStores: a pop of two comments removes
// the blob payload files of each one's L3 store, and leaves the third's.
func TestFieldPopNRemovesNestedStoresAndUnlinksTheirFiles(t *testing.T) {
	s := newTestServer(t)
	resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "Pics", Engine: "blobStore"}}})
	if !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if inserted, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !inserted.Success {
		t.Fatalf("inserting p1: %+v, %v", inserted, err)
	}
	ctx := context.Background()
	photo := bytes.Repeat([]byte("pic"), store.InlineThreshold)
	for position := int64(0); position < 3; position++ {
		fieldOK(t)(s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": int32(position)}}))
		fieldOK(t)(s.FieldPutCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Pics", Path: []interface{}{"Comments", position}, Element: "pic", Content: photo}))
	}
	before := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "Post").id.Load())
	if len(before) != 3 {
		t.Fatalf("precondition: %d payload files, want 3", len(before))
	}
	popped := fieldOK(t)(s.FieldPopCtx(ctx, popFieldRequest("Comments", 2)))
	if len(popped.Items) != 2 {
		t.Fatalf("popped %d comments, want 2", len(popped.Items))
	}
	if after := payloadFilesOf(t, s.blobRoot(), lookupBucket(s, "app", "Post").id.Load()); len(after) != 1 {
		t.Fatalf("after popping two comments %d payload files remain, want the third's one: %v", len(after), after)
	}
}

// TestAPopNWhoseSyncFailsAnswersEveryElement is the field row of
// TestAPopNWhoseSyncFailsAnswersEveryEntry.
func TestAPopNWhoseSyncFailsAnswersEveryElement(t *testing.T) {
	s, kv := newCountingServer(t)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "posts", Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})}); err != nil || !resp.Success {
		t.Fatalf("create posts: %+v, %v", resp, err)
	}
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "posts", Key: "p1", Data: map[string]interface{}{"title": "p1"}})
	for n := 1; n <= 5; n++ {
		handlerSend(t, handler, transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "posts", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": int32(n)}})
	}
	kv.failWith = errors.New("input/output error")
	request := FieldPopRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "posts", Key: "p1", Field: "Comments"}, Limit: 3}
	answer, err := handler(context.Background(), transport.NewFrame(transport.OpFieldPop, mustMarshalBSON(t, request)))
	kv.failWith = nil
	if err != nil || answer.Opcode == transport.OpError {
		t.Fatalf("handler = %v, %v; want the answer with its elements", answer, err)
	}
	var decoded FieldResponse
	if err := bson.Unmarshal(answer.Payload, &decoded); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(decoded.Items) != 3 || !strings.Contains(decoded.Message, "the 3 entries are in this answer, and may be served again") {
		t.Fatalf("answer = %+v; want three items and the plural message", decoded)
	}
}

// TestALostForwardedFieldPopNAdvisesPeekWithItsLimit: the field form of the
// advice decodes FieldPopRequest's inline address and its limit.
func TestALostForwardedFieldPopNAdvisesPeekWithItsLimit(t *testing.T) {
	payload := mustMarshalBSON(t, popFieldRequest("Comments", 5))
	advice := forwardLostAdvice(transport.OpFieldPop, "app", "Post", payload)
	if want := `db.Post["p1"].Comments.peek({limit: 5})`; !strings.Contains(advice, want) {
		t.Fatalf("advice for a lost field pop of five lacks %s: %s", want, advice)
	}
	single := forwardLostAdvice(transport.OpFieldPop, "app", "Post", mustMarshalBSON(t, popFieldRequest("Comments", 0)))
	if want := `db.Post["p1"].Comments.peek()`; !strings.Contains(single, want) {
		t.Fatalf("advice for a lost single field pop lacks %s: %s", want, single)
	}
}

// TestFieldPopTooLargeForOneBatchSpeaksOfPops: a store-field pop whose deletes
// are over the largest batch the store commits was answered with the batch
// put's advice, "Send fewer documents per batch ... load the file with the
// hoardDB ingest command", which is no use to a pop (pop-n D2 verification,
// open item d). It now says what a pop can do, as the bucket pop does: pop
// fewer at a time. Nothing is removed.
func TestFieldPopTooLargeForOneBatchSpeaksOfPops(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	seedPeekPost(t, s)
	for n := range longIDHeapEntries {
		id := fmt.Sprintf("%03d", n) + strings.Repeat("i", longIDBytes-3)
		resp, err := s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Ranked", Priority: int64(n),
			Data: map[string]interface{}{"_id_": id, "n": int32(n)}})
		if err != nil || !resp.Success {
			t.Fatalf("setup: pushing element %d: %+v, %v", n, resp, err)
		}
	}
	answer, err := s.FieldPopCtx(context.Background(), popFieldRequest("Ranked", longIDHeapEntries))
	if err != nil {
		t.Fatalf("pop({limit: %d}) at the 32 MiB floor: %v", longIDHeapEntries, err)
	}
	if answer.Success || answer.Found {
		t.Fatalf("setup: pop({limit: %d}) at the 32 MiB floor was answered %+v; it must not fit one batch", longIDHeapEntries, answer)
	}
	for _, want := range []string{"field pop failed", "nothing was removed", fmt.Sprintf("pop({limit: %d})", longIDHeapEntries/2)} {
		if !strings.Contains(answer.Message, want) {
			t.Errorf("the refusal %q lacks %q", answer.Message, want)
		}
	}
	for _, unwanted := range []string{"documents per batch", "ingest", "the limitations page of the user docs"} {
		if strings.Contains(answer.Message, unwanted) {
			t.Errorf("the refusal %q still carries the batch put's advice %q", answer.Message, unwanted)
		}
	}
	if left := fieldLeft(t, s, "Ranked"); len(left) != longIDHeapEntries {
		t.Errorf("%d elements are left after a refused pop, want all %d", len(left), longIDHeapEntries)
	}
}
