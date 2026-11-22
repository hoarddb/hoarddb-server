package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// peekEngine is a bucket type that has a pop order, with the numbers its pop
// takes first out of a bucket pushed 1..5 (the heap with priorities 10..50 in
// that order, so the highest priority is the last pushed).
type peekEngine struct {
	bucketType BucketType
	popOrder   []int
}

var peekEngines = []peekEngine{
	{BucketTypeFIFO, []int{1, 2, 3, 4, 5}},
	{BucketTypeLIFO, []int{5, 4, 3, 2, 1}},
	{BucketTypeHeap, []int{5, 4, 3, 2, 1}},
}

// pushPeekEntries pushes {n: 1..count} into a bucket, heap entries at priority
// 10*n.
func pushPeekEntries(t testing.TB, s *Server, bucket string, count int, pad string) {
	t.Helper()
	for n := 1; n <= count; n++ {
		resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(10 * n),
			Data: map[string]interface{}{"n": int32(n), "pad": pad}})
		if err != nil || !resp.Success {
			t.Fatalf("pushing %d into %s: %+v, %v", n, bucket, resp, err)
		}
	}
}

func peekedNumbers(t testing.TB, items []bson.Raw) []int {
	t.Helper()
	numbers := make([]int, 0, len(items))
	for _, item := range items {
		n, ok := item.Lookup("n").AsInt64OK()
		if !ok {
			t.Fatalf("peeked item %v has no integer n", item)
		}
		numbers = append(numbers, int(n))
	}
	return numbers
}

func peekRefusalsCounted() float64 {
	var metric dto.Metric
	if err := queryResultTooLarge.WithLabelValues(queryMethodPeek).Write(&metric); err != nil {
		return -1
	}
	return metric.GetCounter().GetValue()
}

// TestPeekWithNoLimitAnswersAOneItemList: no limit, limit 0 and limit 1 are
// the one request, and answer a list of the one entry pop would take.
func TestPeekWithNoLimitAnswersAOneItemList(t *testing.T) {
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", engine.bucketType)
			pushPeekEntries(t, s, "q", 5, "")
			for _, limit := range []int{0, 1} {
				resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: limit})
				if err != nil || !resp.Found {
					t.Fatalf("peek limit %d: %+v, %v", limit, resp, err)
				}
				if got := peekedNumbers(t, resp.Items); !reflect.DeepEqual(got, engine.popOrder[:1]) {
					t.Fatalf("peek limit %d = %v, want %v: the one entry pop takes", limit, got, engine.popOrder[:1])
				}
			}
		})
	}
}

// TestPeekLimitReturnsItemsInPopOrder: limit 3 answers the three entries three
// pops would take, in that order, and the response on the wire has an items
// array and no data key at all.
func TestPeekLimitReturnsItemsInPopOrder(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, nil)
	c := dialPipelined(t, addr)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := "q-" + string(engine.bucketType)
			createTestBucket(t, qs.core, "app", bucket, engine.bucketType)
			pushPeekEntries(t, qs.core, bucket, 5, "")
			request := requestFrame(t, transport.OpFIFOPeek, PeekRequest{Database: "app", Bucket: bucket, Limit: 3})
			answer := c.exchange(request)[request.RequestID]
			if answer.Opcode != transport.OpFIFOPeek {
				t.Fatalf("answered %s: %s", answer.Opcode, answer.Payload)
			}
			if _, err := bson.Raw(answer.Payload).LookupErr("data"); err == nil {
				t.Fatalf("the response carries a data key: %v", bson.Raw(answer.Payload))
			}
			var decoded PeekResponse
			if err := bson.Unmarshal(answer.Payload, &decoded); err != nil {
				t.Fatalf("decoding the response: %v", err)
			}
			if got := peekedNumbers(t, decoded.Items); !decoded.Found || !reflect.DeepEqual(got, engine.popOrder[:3]) {
				t.Fatalf("limit 3 = %v (found %v), want %v", got, decoded.Found, engine.popOrder[:3])
			}
		})
	}
}

// TestPeekLimitAboveCapRefused: the server owns the cap of 1000 and its text,
// and the bucket-level text names only the verbs a bucket queue has.
func TestPeekLimitAboveCapRefused(t *testing.T) {
	for _, engine := range peekEngines {
		t.Run("bucket/"+string(engine.bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", engine.bucketType)
			pushPeekEntries(t, s, "q", 3, "")
			resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: 1001})
			if !errors.Is(err, ErrPeekLimit) || resp != nil {
				t.Fatalf("limit 1001 = %+v, %v; want a refusal", resp, err)
			}
			if !strings.Contains(err.Error(), "1000") || !strings.Contains(err.Error(), "pop()") {
				t.Fatalf("the refusal does not name the cap and the verb that reads more: %v", err)
			}
			if engine.bucketType != BucketTypeFIFO && strings.Contains(err.Error(), "find()") {
				t.Fatalf("a %s's storage order is not its pop order, so the refusal must not point at find(): %v", engine.bucketType, err)
			}
			if resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: 1000}); err != nil || len(resp.Items) != 3 {
				t.Fatalf("limit 1000 = %+v, %v; want the 3 entries there are", resp, err)
			}
		})
	}
	t.Run("field", fieldPeekCapRefused)
}

func TestPeekNegativeLimitRefusedByServer(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	pushPeekEntries(t, s, "q", 3, "")
	resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: -1})
	if !errors.Is(err, ErrPeekLimit) || resp != nil {
		t.Fatalf("limit -1 = %+v, %v; want a refusal", resp, err)
	}
	t.Run("field", fieldPeekNegativeRefused)
}

// peekCeilingServer is a TCP server whose result ceiling is 1 MiB, so entries
// of a few hundred KiB cross it.
func peekCeilingServer(t *testing.T) (*Service, *pipelinedClient) {
	t.Helper()
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { setQueryCeiling(cfg, 1<<20) })
	return qs, dialPipelined(t, addr)
}

// TestPeekOverResultCeilingRefused: entries whose sum crosses the ceiling
// refuse the whole request: RESULT_TOO_LARGE with the peek message, no items,
// the PEEK label, and on the wire code 0x0E with both byte fields set.
func TestPeekOverResultCeilingRefused(t *testing.T) {
	qs, c := peekCeilingServer(t)
	pad := strings.Repeat("x", 400<<10)
	for _, engine := range peekEngines {
		t.Run("bucket/"+string(engine.bucketType), func(t *testing.T) {
			bucket := "big-" + string(engine.bucketType)
			createTestBucket(t, qs.core, "app", bucket, engine.bucketType)
			pushPeekEntries(t, qs.core, bucket, 5, pad)
			refusalsBefore := peekRefusalsCounted()

			resp, err := qs.core.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: 5})
			if !errors.Is(err, ErrResultTooLarge) || resp != nil {
				t.Fatalf("limit 5 of 400 KiB entries = %+v, %v; want RESULT_TOO_LARGE and no items", resp, err)
			}
			if got := peekRefusalsCounted() - refusalsBefore; got != 1 {
				t.Fatalf("the PEEK refusal counter moved by %v, want 1", got)
			}

			request := requestFrame(t, transport.OpFIFOPeek, PeekRequest{Database: "app", Bucket: bucket, Limit: 5})
			refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "the oversized peek")
			if refused.Code != int(transport.ErrResultTooLarge) {
				t.Fatalf("code %d (%q), want RESULT_TOO_LARGE 0x0E", refused.Code, refused.Message)
			}
			if refused.MaxResultBytes != 1<<20 || refused.RetainedBytes <= 0 || refused.RetainedBytes > 1<<20 {
				t.Errorf("max_result_bytes %d, retained_bytes %d; want the 1 MiB ceiling and what was held at most that", refused.MaxResultBytes, refused.RetainedBytes)
			}
			for _, want := range []string{"peek({limit: 5}) on app." + bucket, "peek({limit: 2}) fits", "HOARDDB_QUERY_MAX_RESULT_BYTES"} {
				if !strings.Contains(refused.Message, want) {
					t.Errorf("message lacks %q: %s", want, refused.Message)
				}
			}
			if strings.Contains(refused.Message, "skip") || strings.Contains(refused.Message, "projection") {
				t.Errorf("message gives find's advice for a peek: %s", refused.Message)
			}
		})
	}
	t.Run("field", fieldPeekOverResultCeilingRefused)
}

// TestPeekAnswersAnEntryLargerThanTheCeiling: the first entry is exempt from
// the ceiling, so peek() answers an entry pop() returns, and a second entry
// is what the ceiling refuses.
func TestPeekAnswersAnEntryLargerThanTheCeiling(t *testing.T) {
	qs, c := peekCeilingServer(t)
	huge := strings.Repeat("h", 1536<<10)
	for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		t.Run("bucket/"+string(bucketType), func(t *testing.T) {
			bucket := "huge-" + string(bucketType)
			createTestBucket(t, qs.core, "app", bucket, bucketType)
			// The entry a pop takes first is the huge one: pushed last for a
			// lifo, first for a fifo, at the top priority for a heap.
			first, second := huge, "small"
			if bucketType == BucketTypeLIFO {
				first, second = second, first
			}
			for n, pad := range []string{first, second} {
				resp, err := qs.core.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(100 - 10*n), Data: map[string]interface{}{"n": int32(n + 1), "pad": pad}})
				if err != nil || !resp.Success {
					t.Fatalf("push %d: %+v, %v", n+1, resp, err)
				}
			}
			for _, limit := range []int{0, 1} {
				resp, err := qs.core.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: limit})
				if err != nil || !resp.Found || len(resp.Items) != 1 {
					t.Fatalf("peek limit %d of an entry over the ceiling = %+v, %v; want the entry", limit, resp, err)
				}
			}
			request := requestFrame(t, transport.OpFIFOPeek, PeekRequest{Database: "app", Bucket: bucket, Limit: 2})
			refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "peek of two entries over the ceiling")
			if refused.Code != int(transport.ErrResultTooLarge) {
				t.Fatalf("limit 2 answered code %d (%q), want RESULT_TOO_LARGE", refused.Code, refused.Message)
			}
			if !strings.Contains(refused.Message, "after 1 entries") {
				t.Errorf("the refusal does not count the exempt first entry: %s", refused.Message)
			}
			popped, err := qs.core.Pop(PopRequest{Database: "app", Bucket: bucket})
			if err != nil || !popped.Found {
				t.Fatalf("pop of the entry peek answered: %+v, %v", popped, err)
			}
		})
	}
	t.Run("field", fieldPeekAnswersAnEntryLargerThanTheCeiling)
}

// TestPeekOnHashBTreeBlobRefuses: a bucket type with no pop order answers
// found false and says why and which verbs work, with a limit and without.
func TestPeekOnHashBTreeBlobRefuses(t *testing.T) {
	cases := []struct {
		bucketType BucketType
		wantVerbs  []string
	}{
		{BucketTypeHash, []string{`get("<key>")`, "find()"}},
		{BucketTypeBTree, []string{`get("<key>")`, "find()"}},
		{BucketTypeBlob, []string{`get("<key>")`, `info("<key>")`}},
	}
	for _, tc := range cases {
		for _, limit := range []int{0, 2} {
			t.Run(fmt.Sprintf("bucket/%s/limit=%d", tc.bucketType, limit), func(t *testing.T) {
				s := newTestServer(t)
				createTestBucket(t, s, "app", "b", tc.bucketType)
				_, err := s.Peek(PeekRequest{Database: "app", Bucket: "b", Limit: limit})
				requireWrongTypeRefusal(t, err, append([]string{"peek is not defined for " + string(tc.bucketType) + " buckets", "no order"}, tc.wantVerbs...)...)
			})
		}
	}
	t.Run("field", fieldPeekOnHashBlobRefuses)
}

// TestPeekThroughAMemberIsAnsweredByTheOwner: on a cluster, a peek of three
// sent through a member that is not the owner returns the owner's next three
// entries in pop order. The owner's stream to the member is frozen and two
// entries popped, so a member that answered from its own copy would still
// show them (extended from TestQueuePeekIsAnsweredByTheOwner).
func TestPeekThroughAMemberIsAnsweredByTheOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	memberIndex := (ownerIndex + 1) % 3
	bucket := bucketOwnedBy(t, tc, ownerIndex, "fifo")
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, bucket, 3)
	for n := 1; n <= 5; n++ {
		tc.ok(ownerIndex, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": int32(n)}, WriteConcern: "all"})
	}
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[memberIndex])))
	defer unfreeze()
	for range 2 {
		tc.ok(ownerIndex, transport.OpFIFOPop, bson.M{"database": testDatabase, "bucket": bucket, "write_concern": "one"})
	}

	var peeked PeekResponse
	frame := tc.ok(memberIndex, transport.OpFIFOPeek, PeekRequest{Database: testDatabase, Bucket: bucket, Limit: 3})
	if err := bson.Unmarshal(frame.Payload, &peeked); err != nil {
		t.Fatalf("decoding the peek: %v", err)
	}
	if got := peekedNumbers(t, peeked.Items); !reflect.DeepEqual(got, []int{3, 4, 5}) {
		t.Fatalf("peek of 3 through a lagging member = %v, want the owner's [3 4 5]", got)
	}
}

// TestPeekBudgetChargesCoverEncodedResponse: what the peek's budget holds is
// at least the element bytes of the response it encodes, so the ceiling
// bounds the frame (TestBudgetChargesEqualEncodedSize's method).
func TestPeekBudgetChargesCoverEncodedResponse(t *testing.T) {
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", engine.bucketType)
			pushPeekEntries(t, s, "q", 5, strings.Repeat("p", 37))
			lastReport := recordBudgets(t)
			resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: 5})
			if err != nil {
				t.Fatalf("peek: %v", err)
			}
			report := lastReport(queryMethodPeek)
			if want := encodedElementBytes(t, resp, "items"); report.retained < want {
				t.Fatalf("the budget holds %d bytes for items that encode to %d: the ceiling does not bound the frame", report.retained, want)
			}
		})
	}
}

// TestPeekReportsToTheBudgetHook: each peek reports its budget once, as every
// other budgeted query does, so queryBudgetHook sees it.
func TestPeekReportsToTheBudgetHook(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	pushPeekEntries(t, s, "q", 3, "")
	lastReport := recordBudgets(t)
	if _, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: 3}); err != nil {
		t.Fatalf("peek: %v", err)
	}
	if report := lastReport(queryMethodPeek); !report.reported || report.retained <= 0 {
		t.Fatalf("the peek reported %+v to the budget hook, want one report holding its items", report)
	}
}

// fieldPeekEngine is a store field with a pop order, and the numbers its pop
// takes first out of a field pushed 1..5 (the heap at priorities 10..50).
type fieldPeekEngine struct {
	field    string
	engine   string
	popOrder []int
}

var fieldPeekEngines = []fieldPeekEngine{
	{"Comments", "fifo", []int{1, 2, 3, 4, 5}},
	{"Photos", "lifo", []int{5, 4, 3, 2, 1}},
	{"Ranked", "heap", []int{5, 4, 3, 2, 1}},
}

// seedPeekPost creates bucket app.Post, which declares one store field of each
// engine, and its document p1.
func seedPeekPost(t testing.TB, s *Server) {
	t.Helper()
	resp := createWithStores(t.(*testing.T), s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore"},
		StoreDeclaration{Name: "Photos", Engine: "lifoStore"},
		StoreDeclaration{Name: "Ranked", Engine: "heapStore"},
		StoreDeclaration{Name: "Tags", Engine: "hashStore"},
		StoreDeclaration{Name: "Files", Engine: "blobStore"})
	if !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
}

// pushPeekFieldElements pushes {n: 1..count} into p1's field, at priority 10*n.
func pushPeekFieldElements(t testing.TB, s *Server, field string, count int, pad string) {
	t.Helper()
	for n := 1; n <= count; n++ {
		resp, err := s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: field, Priority: int64(10 * n),
			Data: map[string]interface{}{"n": int32(n), "pad": pad}})
		if err != nil || !resp.Success {
			t.Fatalf("pushing %d into %s: %+v, %v", n, field, resp, err)
		}
	}
}

func peekFieldRequest(field string, limit int) FieldPeekRequest {
	return FieldPeekRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: field}, Limit: limit}
}

func fieldItemNumbers(t testing.TB, items []FieldListEntry) []int {
	t.Helper()
	raws := make([]bson.Raw, 0, len(items))
	for _, item := range items {
		raws = append(raws, item.Element)
	}
	return peekedNumbers(t, raws)
}

// TestFieldPeekAnswersFoundAndItems: a peek of a non-empty field answers
// found true and one item. Items alone does not satisfy it: the CLI's
// not-found line is chosen by Found (cli/declare.go:fieldRead).
func TestFieldPeekAnswersFoundAndItems(t *testing.T) {
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			s := newTestServer(t)
			seedPeekPost(t, s)
			pushPeekFieldElements(t, s, engine.field, 5, "")
			resp, err := s.FieldPeek(peekFieldRequest(engine.field, 0))
			if err != nil || !resp.Found || len(resp.Items) != 1 {
				t.Fatalf("peek of a non-empty %s field = %+v, %v; want found true and one item", engine.engine, resp, err)
			}
			if got := fieldItemNumbers(t, resp.Items); !reflect.DeepEqual(got, engine.popOrder[:1]) {
				t.Fatalf("peek = %v, want %v", got, engine.popOrder[:1])
			}
		})
	}
	t.Run("an empty field is found false", func(t *testing.T) {
		s := newTestServer(t)
		seedPeekPost(t, s)
		resp, err := s.FieldPeek(peekFieldRequest("Comments", 3))
		if err != nil || resp.Found || len(resp.Items) != 0 {
			t.Fatalf("peek of an empty field = %+v, %v; want found false", resp, err)
		}
	})
}

// TestFieldPeekItemsCarryIdentity: each item says where its element is, equal
// to what pop returns for the same element, and pop and get still answer
// Element, Position, Priority and EntryID themselves.
func TestFieldPeekItemsCarryIdentity(t *testing.T) {
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			s := newTestServer(t)
			seedPeekPost(t, s)
			pushPeekFieldElements(t, s, engine.field, 5, "")
			peeked, err := s.FieldPeek(peekFieldRequest(engine.field, 5))
			if err != nil || len(peeked.Items) != 5 {
				t.Fatalf("peek of 5 = %+v, %v", peeked, err)
			}
			for index, item := range peeked.Items {
				popped, err := s.FieldPopCtx(context.Background(), FieldPopRequest{FieldRequest: peekFieldRequest(engine.field, 0).FieldRequest})
				if err != nil || !popped.Found {
					t.Fatalf("pop %d: %+v, %v", index, popped, err)
				}
				if len(popped.Items) != 1 || !bytes.Equal(item.Element, popped.Items[0].Element) || !slices.Equal(wirePlacementText(item.Position, item.Priority), wirePlacementText(popped.Items[0].Position, popped.Items[0].Priority)) || item.EntryID != popped.Items[0].EntryID {
					t.Fatalf("item %d = %+v, but pop answered %+v", index, item, popped)
				}
				if engine.engine == "heap" && (item.EntryID == "" || item.Priority == nil) {
					t.Fatalf("heap item %d carries no priority or entry id: %+v", index, item)
				}
				if engine.engine != "heap" && (item.Position == nil || *item.Position != int64(engine.popOrder[index]-1)) {
					t.Fatalf("item %d is at position %v, want %d", index, item.Position, engine.popOrder[index]-1)
				}
			}
		})
	}
	t.Run("get still answers Element", func(t *testing.T) {
		s := newTestServer(t)
		seedPeekPost(t, s)
		fieldOK(t)(s.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "go", Data: map[string]interface{}{"weight": 3}}))
		got, err := s.FieldGet(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "go"})
		if err != nil || !got.Found || len(got.Element) == 0 || got.ElementKey != "go" {
			t.Fatalf("get = %+v, %v; want Element and ElementKey", got, err)
		}
	})
}

// fieldPeekCapRefused is the field half of TestPeekLimitAboveCapRefused.
func fieldPeekCapRefused(t *testing.T) {
	s := newTestServer(t)
	seedPeekPost(t, s)
	for _, engine := range fieldPeekEngines {
		pushPeekFieldElements(t, s, engine.field, 3, "")
		t.Run(engine.engine, func(t *testing.T) {
			resp, err := s.FieldPeek(peekFieldRequest(engine.field, 1001))
			if !errors.Is(err, ErrPeekLimit) || resp != nil {
				t.Fatalf("limit 1001 = %+v, %v; want a refusal", resp, err)
			}
			if !strings.Contains(err.Error(), "1000") || !strings.Contains(err.Error(), "pop()") || !strings.Contains(err.Error(), "list({limit: 1000, after: ...})") {
				t.Fatalf("the refusal does not name the cap, pop() and paging list: %v", err)
			}
			if resp, err := s.FieldPeek(peekFieldRequest(engine.field, 1000)); err != nil || len(resp.Items) != 3 {
				t.Fatalf("limit 1000 = %+v, %v; want the 3 elements there are", resp, err)
			}
		})
	}
}

// fieldPeekNegativeRefused is the field half of TestPeekNegativeLimitRefusedByServer.
func fieldPeekNegativeRefused(t *testing.T) {
	s := newTestServer(t)
	seedPeekPost(t, s)
	for _, engine := range fieldPeekEngines {
		pushPeekFieldElements(t, s, engine.field, 3, "")
		t.Run(engine.engine, func(t *testing.T) {
			if resp, err := s.FieldPeek(peekFieldRequest(engine.field, -1)); !errors.Is(err, ErrPeekLimit) || resp != nil {
				t.Fatalf("limit -1 = %+v, %v; want a refusal", resp, err)
			}
		})
	}
}

// fieldPeekOverResultCeilingRefused is the field half of
// TestPeekOverResultCeilingRefused: the ceiling refusal: code 0x0E with both byte fields, the field's address in the
// message, no items.
func fieldPeekOverResultCeilingRefused(t *testing.T) {
	qs, c := peekCeilingServer(t)
	seedPeekPost(t, qs.core)
	pad := strings.Repeat("x", 400<<10)
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			pushPeekFieldElements(t, qs.core, engine.field, 5, pad)
			refusalsBefore := peekRefusalsCounted()
			resp, err := qs.core.FieldPeek(peekFieldRequest(engine.field, 5))
			if !errors.Is(err, ErrResultTooLarge) || resp != nil {
				t.Fatalf("limit 5 of 400 KiB elements = %+v, %v; want RESULT_TOO_LARGE and no items", resp, err)
			}
			if got := peekRefusalsCounted() - refusalsBefore; got != 1 {
				t.Fatalf("the PEEK refusal counter moved by %v, want 1", got)
			}
			request := requestFrame(t, transport.OpFieldPeek, peekFieldRequest(engine.field, 5))
			refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "the oversized field peek")
			if refused.Code != int(transport.ErrResultTooLarge) || refused.MaxResultBytes != 1<<20 || refused.RetainedBytes <= 0 {
				t.Fatalf("code %d, max_result_bytes %d, retained_bytes %d (%q); want 0x0E, the 1 MiB ceiling and what was held",
					refused.Code, refused.MaxResultBytes, refused.RetainedBytes, refused.Message)
			}
			for _, want := range []string{`peek({limit: 5}) on app.Post["p1"].` + engine.field, "peek({limit: 2}) fits", "HOARDDB_QUERY_MAX_RESULT_BYTES"} {
				if !strings.Contains(refused.Message, want) {
					t.Errorf("message lacks %q: %s", want, refused.Message)
				}
			}
		})
	}
}

// fieldPeekAnswersAnEntryLargerThanTheCeiling is the field half of
// TestPeekAnswersAnEntryLargerThanTheCeiling: the first element is exempt at
// the field level too.
func fieldPeekAnswersAnEntryLargerThanTheCeiling(t *testing.T) {
	qs, c := peekCeilingServer(t)
	seedPeekPost(t, qs.core)
	huge := strings.Repeat("h", 1536<<10)
	for _, engine := range fieldPeekEngines {
		t.Run(engine.engine, func(t *testing.T) {
			// The element a pop takes first is the huge one.
			first, second := huge, "small"
			if engine.engine == "lifo" {
				first, second = second, first
			}
			for n, pad := range []string{first, second} {
				resp, err := qs.core.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: engine.field, Priority: int64(100 - 10*n),
					Data: map[string]interface{}{"n": int32(n + 1), "pad": pad}})
				if err != nil || !resp.Success {
					t.Fatalf("push %d: %+v, %v", n+1, resp, err)
				}
			}
			for _, limit := range []int{0, 1} {
				resp, err := qs.core.FieldPeek(peekFieldRequest(engine.field, limit))
				if err != nil || !resp.Found || len(resp.Items) != 1 {
					t.Fatalf("peek limit %d of an element over the ceiling = %+v, %v; want the element", limit, resp, err)
				}
			}
			request := requestFrame(t, transport.OpFieldPeek, peekFieldRequest(engine.field, 2))
			refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "field peek of two elements over the ceiling")
			if refused.Code != int(transport.ErrResultTooLarge) || !strings.Contains(refused.Message, "after 1 entries") {
				t.Fatalf("limit 2 answered code %d (%q), want RESULT_TOO_LARGE counting the exempt first element", refused.Code, refused.Message)
			}
			popped, err := qs.core.FieldPopCtx(context.Background(), FieldPopRequest{FieldRequest: peekFieldRequest(engine.field, 0).FieldRequest})
			if err != nil || !popped.Found {
				t.Fatalf("pop of the element peek answered: %+v, %v", popped, err)
			}
		})
	}
}

// fieldPeekOnHashBlobRefuses is the field half of TestPeekOnHashBTreeBlobRefuses: a peek of a hash or blob
// field is refused by resolveField with wrongVerbErr's text, which is shared
// by every field verb and pinned in cli/testdata/golden/field-errors.golden.
func fieldPeekOnHashBlobRefuses(t *testing.T) {
	s := newTestServer(t)
	seedPeekPost(t, s)
	cases := []struct{ field, engine, family string }{
		{"Tags", "hashStore", "Hash fields take put/get/delete by element key; peek is one of push, pop and peek, which are for fifoStore, lifoStore and heapStore."},
		{"Files", "blobStore", "Blob fields take put/get/delete by element key; peek is one of push, pop and peek, which are for fifoStore, lifoStore and heapStore."},
	}
	for _, tc := range cases {
		for _, limit := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/limit=%d", tc.field, limit), func(t *testing.T) {
				resp, err := s.FieldPeek(peekFieldRequest(tc.field, limit))
				if !errors.Is(err, ErrFieldWrongVerb) || resp != nil {
					t.Fatalf("peek = %+v, %v; want ErrFieldWrongVerb", resp, err)
				}
				want := fmt.Sprintf(`"peek" is not an operation on %q, which is declared %s. %s`, tc.field, tc.engine, tc.family)
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the refusal changed:\n got %v\nwant it to contain %s", err, want)
				}
			})
		}
	}
}

// TestPeekLimitOnAnotherFieldVerbIsNotExpressible: only FieldPeekRequest has a
// limit. A limit on FieldRequest would be a field push, pop, put, get, delete
// and len accept and silently ignore.
func TestPeekLimitOnAnotherFieldVerbIsNotExpressible(t *testing.T) {
	limitTagged := func(kind reflect.Type) bool {
		for index := 0; index < kind.NumField(); index++ {
			if strings.Split(kind.Field(index).Tag.Get("bson"), ",")[0] == "limit" {
				return true
			}
		}
		return false
	}
	if limitTagged(reflect.TypeOf(FieldRequest{})) {
		t.Fatal("FieldRequest carries a limit: pop({limit: 3}) would answer one element with no error")
	}
	if !limitTagged(reflect.TypeOf(FieldPeekRequest{})) {
		t.Fatal("FieldPeekRequest carries no limit")
	}
	// A limit sent to a verb that takes none is ignored, not read as a pop's.
	s := newTestServer(t)
	seedPeekPost(t, s)
	pushPeekFieldElements(t, s, "Comments", 3, "")
	payload, err := bson.Marshal(bson.M{"database": "app", "bucket": "Post", "key": "p1", "field": "Comments", "limit": 3})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	answer, err := s.dispatch(context.Background(), transport.NewFrame(transport.OpFieldLen, payload))
	if err != nil || answer.Opcode != transport.OpFieldLen {
		t.Fatalf("dispatching the len: %v, %v", answer, err)
	}
	var counted FieldResponse
	if err := bson.Unmarshal(answer.Payload, &counted); err != nil || counted.Len != 3 || len(counted.Items) != 0 {
		t.Fatalf("len answered %+v, %v; want the count of 3 and no items", counted, err)
	}
}

// TestPeekIsOneConsistentSnapshot: while a writer pushes at the tail and pops
// at the head, every peek of a fifo is a run of consecutive entries, the prefix
// of one pop sequence. A peek assembled from more than one view would show a
// gap or an order that never existed. Bucket and field, 300 peeks of up to 100.
func TestPeekIsOneConsistentSnapshot(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	seedPeekPost(t, s)
	push := func(n int) {
		if resp, err := s.Push(PushRequest{Database: "app", Bucket: "q", Data: map[string]interface{}{"n": int32(n)}}); err != nil || !resp.Success {
			t.Errorf("push %d: %+v, %v", n, resp, err)
		}
		fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": int32(n)}}))
	}
	for n := 1; n <= 200; n++ {
		push(n)
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for n := 201; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			push(n)
			if _, err := s.Pop(PopRequest{Database: "app", Bucket: "q"}); err != nil {
				t.Errorf("pop: %v", err)
			}
			if _, err := s.FieldPopCtx(context.Background(), FieldPopRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments"}}); err != nil {
				t.Errorf("field pop: %v", err)
			}
		}
	}()
	defer func() {
		close(stop)
		writer.Wait()
	}()

	consecutive := func(numbers []int) bool {
		for i := 1; i < len(numbers); i++ {
			if numbers[i] != numbers[i-1]+1 {
				return false
			}
		}
		return true
	}
	for range 300 {
		bucket, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: 100})
		if err != nil || len(bucket.Items) != 100 {
			t.Fatalf("bucket peek = %d items, %v", len(bucket.Items), err)
		}
		if numbers := peekedNumbers(t, bucket.Items); !consecutive(numbers) {
			t.Fatalf("a bucket peek is not one pop sequence: %v", numbers)
		}
		field, err := s.FieldPeek(peekFieldRequest("Comments", 100))
		if err != nil || len(field.Items) != 100 {
			t.Fatalf("field peek = %d items, %v", len(field.Items), err)
		}
		if numbers := fieldItemNumbers(t, field.Items); !consecutive(numbers) {
			t.Fatalf("a field peek is not one pop sequence: %v", numbers)
		}
	}
}
