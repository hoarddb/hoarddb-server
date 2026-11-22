package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// popTestOpcode is the opcode a client sends to pop a bucket of this type.
func popTestOpcode(bucketType BucketType) transport.Opcode {
	switch bucketType {
	case BucketTypeLIFO:
		return transport.OpLIFOPop
	case BucketTypeHeap:
		return transport.OpHeapPop
	}
	return transport.OpFIFOPop
}

// peekAllNumbers is the n of every entry left in a bucket, in pop order. It
// walks the store directly, because a bucket of large entries would be refused
// by the result ceiling that the test is about.
func peekAllNumbers(t testing.TB, s *Server, bucket string) []int {
	t.Helper()
	var numbers []int
	visit := func(entry []byte) error {
		n, ok := bson.Raw(entry).Lookup("n").AsInt64OK()
		if !ok {
			return fmt.Errorf("entry %v has no integer n", bson.Raw(entry))
		}
		numbers = append(numbers, int(n))
		return nil
	}
	handle := lookupBucket(s, "app", bucket)
	var err error
	if handle.bucketType == BucketTypeHeap {
		err = handle.heapStore.PeekN(maxPeekLimit, visit)
	} else {
		err = handle.queueStore.PeekN(maxPeekLimit, visit)
	}
	if err != nil {
		t.Fatalf("walking %s: %v", bucket, err)
	}
	return numbers
}

// TestPopWithNoLimitAnswersAOneItemList: no limit, limit 0 and limit 1 are one
// request and answer a one-item list. The raw bytes carry no data member: it
// is deleted, not kept beside items.
func TestPopWithNoLimitAnswersAOneItemList(t *testing.T) {
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", engine.bucketType)
			pushPeekEntries(t, s, "q", 5, "")
			handler := NewHandler(s)
			for index, limit := range []int{0, 0, 1} {
				request := PopRequest{Database: "app", Bucket: "q", Limit: limit}
				answer := handlerSend(t, handler, popTestOpcode(engine.bucketType), request)
				reply := bson.Raw(answer.Payload)
				if _, hasData := reply.Lookup("data").DocumentOK(); hasData {
					t.Fatalf("limit %d answered a data member: %s", limit, reply)
				}
				var decoded PopResponse
				if err := bson.Unmarshal(answer.Payload, &decoded); err != nil {
					t.Fatalf("decoding the answer: %v", err)
				}
				if !decoded.Found || len(decoded.Items) != 1 {
					t.Fatalf("limit %d = %+v; want found with one item", limit, decoded)
				}
				if got := peekedNumbers(t, decoded.Items); got[0] != engine.popOrder[index] {
					t.Fatalf("pop %d (limit %d) took %d, want %d", index, limit, got[0], engine.popOrder[index])
				}
			}
		})
	}
}

// TestPopLimitReturnsItemsInPopOrder: limit 3 of 5 answers the three a peek
// showed first, and leaves the two a peek shows next.
func TestPopLimitReturnsItemsInPopOrder(t *testing.T) {
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", engine.bucketType)
			pushPeekEntries(t, s, "q", 5, "")
			resp, err := s.Pop(PopRequest{Database: "app", Bucket: "q", Limit: 3})
			if err != nil || !resp.Found {
				t.Fatalf("pop limit 3 = %+v, %v", resp, err)
			}
			if got := peekedNumbers(t, resp.Items); fmt.Sprint(got) != fmt.Sprint(engine.popOrder[:3]) {
				t.Fatalf("popped %v, want %v", got, engine.popOrder[:3])
			}
			if got := peekAllNumbers(t, s, "q"); fmt.Sprint(got) != fmt.Sprint(engine.popOrder[3:]) {
				t.Fatalf("left %v, want %v", got, engine.popOrder[3:])
			}
		})
	}
}

// TestPopLimitPastTheEnd: asking for more than there is takes what there is,
// found, and the next pop is empty.
func TestPopLimitPastTheEnd(t *testing.T) {
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", engine.bucketType)
			pushPeekEntries(t, s, "q", 3, "")
			resp, err := s.Pop(PopRequest{Database: "app", Bucket: "q", Limit: 10})
			if err != nil || !resp.Found || len(resp.Items) != 3 {
				t.Fatalf("pop limit 10 of 3 = %+v, %v; want three items", resp, err)
			}
			empty, err := s.Pop(PopRequest{Database: "app", Bucket: "q"})
			want := "queue is empty"
			if engine.bucketType == BucketTypeHeap {
				want = "heap is empty"
			}
			if err != nil || empty.Found || empty.Message != want {
				t.Fatalf("the next pop = %+v, %v; want %q", empty, err, want)
			}
		})
	}
}

// TestPopLimitAboveCapRefused: the server owns the cap and its text, nothing is
// removed, and the answer on the wire is INVALID_ARGUMENT with not_applied.
func TestPopLimitAboveCapRefused(t *testing.T) {
	qs, c := peekCeilingServer(t)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := "cap-" + string(engine.bucketType)
			createTestBucket(t, qs.core, "app", bucket, engine.bucketType)
			pushPeekEntries(t, qs.core, bucket, 3, "")
			for _, tc := range []struct{ limit, peekLimit int }{{101, 101}, {5000, 1000}} {
				request := requestFrame(t, popTestOpcode(engine.bucketType), PopRequest{Database: "app", Bucket: bucket, Limit: tc.limit})
				refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "the oversized pop")
				if refused.Code != int(transport.ErrInvalidArgument) || refused.WriteOutcome != outcomeNotApplied {
					t.Fatalf("limit %d answered code %d outcome %q (%q); want INVALID_ARGUMENT and not_applied", tc.limit, refused.Code, refused.WriteOutcome, refused.Message)
				}
				for _, want := range []string{fmt.Sprintf("%d is above the pop maximum of 100", tc.limit), fmt.Sprintf("peek({limit: %d})", tc.peekLimit)} {
					if !strings.Contains(refused.Message, want) {
						t.Errorf("message lacks %q: %s", want, refused.Message)
					}
				}
			}
			if got := peekAllNumbers(t, qs.core, bucket); len(got) != 3 {
				t.Fatalf("a refused pop left %v, want all three entries", got)
			}
		})
	}
}

// TestPopNegativeLimitRefusedByServer: a negative limit is refused, and
// nothing is removed.
func TestPopNegativeLimitRefusedByServer(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	pushPeekEntries(t, s, "q", 3, "")
	resp, err := s.Pop(PopRequest{Database: "app", Bucket: "q", Limit: -1})
	if !errors.Is(err, ErrPopLimit) || resp != nil {
		t.Fatalf("limit -1 = %+v, %v; want ErrPopLimit", resp, err)
	}
	if got := peekAllNumbers(t, s, "q"); len(got) != 3 {
		t.Fatalf("a refused pop left %v, want all three", got)
	}
}

// TestPopOverResultCeilingRemovesNothing is the centre of the spec: entries
// whose sum crosses the ceiling refuse the whole pop, as RESULT_TOO_LARGE with
// the pop message and both byte fields, counted once, and every entry is still
// there in its order afterwards. A refusal that left as found:false would read
// as an empty queue.
func TestPopOverResultCeilingRemovesNothing(t *testing.T) {
	qs, c := peekCeilingServer(t)
	pad := strings.Repeat("x", 400<<10)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := "big-" + string(engine.bucketType)
			createTestBucket(t, qs.core, "app", bucket, engine.bucketType)
			pushPeekEntries(t, qs.core, bucket, 5, pad)
			refusalsBefore := popRefusalsCounted()

			resp, err := qs.core.Pop(PopRequest{Database: "app", Bucket: bucket, Limit: 5})
			if !errors.Is(err, ErrResultTooLarge) || resp != nil {
				t.Fatalf("limit 5 of 400 KiB entries = %+v, %v; want RESULT_TOO_LARGE and no items", resp, err)
			}
			if got := popRefusalsCounted() - refusalsBefore; got != 1 {
				t.Fatalf("the POP refusal counter moved by %v, want 1", got)
			}

			request := requestFrame(t, popTestOpcode(engine.bucketType), PopRequest{Database: "app", Bucket: bucket, Limit: 5})
			refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "the oversized pop")
			if refused.Code != int(transport.ErrResultTooLarge) {
				t.Fatalf("code %d (%q), want RESULT_TOO_LARGE 0x0E", refused.Code, refused.Message)
			}
			if refused.MaxResultBytes != 1<<20 || refused.RetainedBytes <= 0 || refused.RetainedBytes > 1<<20 {
				t.Errorf("max_result_bytes %d, retained_bytes %d; want the 1 MiB ceiling and what was held at most that", refused.MaxResultBytes, refused.RetainedBytes)
			}
			if refused.WriteOutcome != outcomeNotApplied {
				t.Errorf("write_outcome %q, want %q", refused.WriteOutcome, outcomeNotApplied)
			}
			for _, want := range []string{"pop({limit: 5}) on app." + bucket, "so nothing was removed", "pop({limit: 2}) fits", "HOARDDB_QUERY_MAX_RESULT_BYTES"} {
				if !strings.Contains(refused.Message, want) {
					t.Errorf("message lacks %q: %s", want, refused.Message)
				}
			}
			left := peekAllNumbers(t, qs.core, bucket)
			if fmt.Sprint(left) != fmt.Sprint(engine.popOrder) {
				t.Fatalf("after the refusal the bucket holds %v, want all five in order %v", left, engine.popOrder)
			}
		})
	}
}

// TestPopAnswersAnEntryLargerThanTheCeiling: the first entry is exempt, so
// pop() and pop({limit: 1}) return an entry over the ceiling, and two such
// entries are refused together and removed neither.
func TestPopAnswersAnEntryLargerThanTheCeiling(t *testing.T) {
	qs, _ := peekCeilingServer(t)
	huge := strings.Repeat("h", 1536<<10)
	for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		t.Run(string(bucketType), func(t *testing.T) {
			bucket := "huge-" + string(bucketType)
			createTestBucket(t, qs.core, "app", bucket, bucketType)
			pushPeekEntries(t, qs.core, bucket, 4, huge)
			for _, limit := range []int{0, 1} {
				resp, err := qs.core.Pop(PopRequest{Database: "app", Bucket: bucket, Limit: limit})
				if err != nil || !resp.Found || len(resp.Items) != 1 {
					t.Fatalf("pop limit %d of an entry over the ceiling = %+v, %v; want the entry", limit, resp, err)
				}
			}
			resp, err := qs.core.Pop(PopRequest{Database: "app", Bucket: bucket, Limit: 2})
			if !errors.Is(err, ErrResultTooLarge) || resp != nil {
				t.Fatalf("pop limit 2 of two entries over the ceiling = %+v, %v; want RESULT_TOO_LARGE", resp, err)
			}
			if left := peekAllNumbers(t, qs.core, bucket); len(left) != 2 {
				t.Fatalf("the refused pop left %v, want the two entries it refused", left)
			}
		})
	}
}

// TestPopOnHashBTreeBlobRefuses: a bucket type with no pop order is refused as
// a wrong-type ERROR (BUG-0469; it was found false in band), with spec 8.5's
// text, with a limit and without.
func TestPopOnHashBTreeBlobRefuses(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeBlob} {
		for _, limit := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/limit=%d", bucketType, limit), func(t *testing.T) {
				s := newTestServer(t)
				createTestBucket(t, s, "app", "b", bucketType)
				_, err := s.Pop(PopRequest{Database: "app", Bucket: "b", Limit: limit})
				requireWrongTypeRefusal(t, err, popRefusal(bucketType))
			})
		}
	}
}

// dropBucketEntries deletes every entry key of a queue or heap bucket and
// leaves its state document alone: the disagreement ErrQueueCursorLost reports.
func dropBucketEntries(t *testing.T, s *Server, database, bucket string) {
	t.Helper()
	handle := lookupBucket(s, database, bucket)
	prefix := storage.Key(handle.slot, handle.databaseID, handle.id.Load(), storage.NSQueue)
	snap := s.kv.NewSnapshot()
	iterator := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.Key(handle.slot, handle.databaseID, handle.id.Load(), storage.NSQueue+1)})
	var keys [][]byte
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		keys = append(keys, append([]byte(nil), iterator.Key()...))
	}
	iterator.Close()
	snap.Discard()
	batch := s.kv.NewBatch()
	defer batch.Discard()
	for _, key := range keys {
		if err := batch.Delete(key); err != nil {
			t.Fatalf("deleting an entry key: %v", err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the deletes: %v", err)
	}
}

// TestPopNCursorLost: a state that counts entries the range does not hold is a
// refusal that names the failure and the restore fix, in band and with no
// error code, for a queue and for a heap. A heap used to answer empty.
func TestPopNCursorLost(t *testing.T) {
	for _, tc := range []struct {
		bucketType BucketType
		noun       string
	}{{BucketTypeFIFO, "queue"}, {BucketTypeLIFO, "queue"}, {BucketTypeHeap, "heap"}} {
		for _, limit := range []int{0, 5} {
			t.Run(fmt.Sprintf("%s/limit=%d", tc.bucketType, limit), func(t *testing.T) {
				s := newTestServer(t)
				createTestBucket(t, s, "app", "q", tc.bucketType)
				pushPeekEntries(t, s, "q", 3, "")
				dropBucketEntries(t, s, "app", "q")
				resp, err := s.Pop(PopRequest{Database: "app", Bucket: "q", Limit: limit})
				if err != nil || resp.Found || len(resp.Items) != 0 {
					t.Fatalf("pop = %+v, %v; want found false in band", resp, err)
				}
				for _, want := range []string{
					tc.noun + " pop failed: the " + tc.noun + "'s state counts 3 entries and none is at its cursor, so nothing was removed",
					"restore this bucket from a replica with 'hoarddb dump' on the replica and 'restore' here", "On a standalone node, restore it from a dump or backup"} {
					if !strings.Contains(resp.Message, want) {
						t.Errorf("message lacks %q: %s", want, resp.Message)
					}
				}
				if strings.Contains(resp.Message, "limit") {
					t.Errorf("the message leaks the store's wording: %s", resp.Message)
				}
			})
		}
	}
}

// TestPopBatchTooLargeNamesNoKeyAndTheFix: a heap pop whose deletes do not fit
// one batch is refused in band, removes nothing, and says to pop fewer entries
// without pasting the heap key (which is as long as the id) into the message.
func TestPopBatchTooLargeNamesNoKeyAndTheFix(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	createTestBucket(t, s, "app", "ranked", BucketTypeHeap)
	for n := 1; n <= 100; n++ {
		resp, err := s.Push(PushRequest{Database: "app", Bucket: "ranked", Priority: 1,
			Data: map[string]interface{}{"_id_": fmt.Sprintf("%06d", n) + strings.Repeat("a", 64000-6), "n": int32(n)}})
		if err != nil || !resp.Success {
			t.Fatalf("push %d: %+v, %v", n, resp, err)
		}
	}
	resp, err := s.Pop(PopRequest{Database: "app", Bucket: "ranked", Limit: 100})
	if err != nil || resp.Found {
		t.Fatalf("pop limit 100 = %+v, %v; want a refusal in band", resp, err)
	}
	if strings.Contains(resp.Message, strings.Repeat("61", 16)) || len(resp.Message) > 1500 {
		t.Fatalf("the message pastes the heap key (%d bytes): %.300s", len(resp.Message), resp.Message)
	}
	for _, want := range []string{"nothing was removed", "pop({limit: 50})"} {
		if !strings.Contains(resp.Message, want) {
			t.Errorf("message lacks %q: %s", want, resp.Message)
		}
	}
	if left := peekAllNumbers(t, s, "ranked"); len(left) != 100 {
		t.Fatalf("the refused pop left %d entries, want 100", len(left))
	}
	if one, err := s.Pop(PopRequest{Database: "app", Bucket: "ranked"}); err != nil || !one.Found || len(one.Items) != 1 {
		t.Fatalf("a single pop of the same heap = %+v, %v; want the one entry", one, err)
	}
}

// TestPopLimitSyncsOnce: a pop of five is one sync, not five.
func TestPopLimitSyncsOnce(t *testing.T) {
	s, kv := newCountingServer(t)
	handler := NewHandler(s)
	for n := 1; n <= 5; n++ {
		handlerSend(t, handler, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "jobs", Data: map[string]interface{}{"n": int32(n)}})
	}
	before := kv.syncs.Load()
	answer := handlerSend(t, handler, transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "jobs", Limit: 5})
	var decoded PopResponse
	if err := bson.Unmarshal(answer.Payload, &decoded); err != nil || len(decoded.Items) != 5 {
		t.Fatalf("pop limit 5 = %+v, %v; want five items", decoded, err)
	}
	if got := kv.syncs.Load() - before; got != 1 {
		t.Fatalf("a pop of five waited for %d syncs, want 1", got)
	}
}

// TestEmptyPopWaitsForNoSync: pop() and pop({limit: 5}) of an empty queue
// commit nothing and wait for no sync.
func TestEmptyPopWaitsForNoSync(t *testing.T) {
	s, kv := newCountingServer(t)
	handler := NewHandler(s)
	for _, limit := range []int{0, 5} {
		before := kv.syncs.Load()
		handlerSend(t, handler, transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "jobs", Limit: limit})
		if got := kv.syncs.Load() - before; got != 0 {
			t.Errorf("an empty pop with limit %d waited for %d syncs, want 0", limit, got)
		}
	}
}

// TestPopClientCannotRaiseTheCeiling (single node): a result_ceiling a client
// sends is ignored by a node that logs nothing, so the node's own 1 MiB holds.
func TestPopClientCannotRaiseTheCeiling(t *testing.T) {
	qs, c := peekCeilingServer(t)
	createTestBucket(t, qs.core, "app", "q", BucketTypeFIFO)
	pushPeekEntries(t, qs.core, "q", 5, strings.Repeat("x", 400<<10))
	request := requestFrame(t, transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "q", Limit: 5, ResultCeiling: 1 << 30})
	refused := decodeErrorFrame(t, c.exchange(request)[request.RequestID], "a pop that sets its own ceiling")
	if refused.Code != int(transport.ErrResultTooLarge) || refused.MaxResultBytes != 1<<20 {
		t.Fatalf("code %d, max_result_bytes %d (%q); want RESULT_TOO_LARGE at the node's 1 MiB", refused.Code, refused.MaxResultBytes, refused.Message)
	}
	if left := peekAllNumbers(t, qs.core, "q"); len(left) != 5 {
		t.Fatalf("the refused pop left %v", left)
	}
}

// TestAPopNWhoseSyncFailsAnswersEveryEntry is BUG-0548's path 1 with N: the
// answer holds all three entries, applied, with the plural message.
func TestAPopNWhoseSyncFailsAnswersEveryEntry(t *testing.T) {
	s, kv := newCountingServer(t)
	handler := NewHandler(s)
	for n := 1; n <= 5; n++ {
		handlerSend(t, handler, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "jobs", Data: map[string]interface{}{"n": int32(n)}})
	}
	kv.failWith = errors.New("input/output error")
	answer, err := handler(context.Background(), transport.NewFrame(transport.OpFIFOPop, mustMarshalBSON(t, PopRequest{Database: "app", Bucket: "jobs", Limit: 3})))
	kv.failWith = nil
	if err != nil || answer.Opcode == transport.OpError {
		t.Fatalf("handler = %v, %v; want the answer with its entries", answer, err)
	}
	var decoded PopResponse
	if err := bson.Unmarshal(answer.Payload, &decoded); err != nil {
		t.Fatalf("decoding the answer: %v", err)
	}
	if len(decoded.Items) != 3 || decoded.WriteOutcome != outcomeApplied {
		t.Fatalf("answer = %+v; want three items and write_outcome applied", decoded)
	}
	if !strings.Contains(decoded.Message, "the 3 entries are in this answer, and may be served again") {
		t.Errorf("message is not the plural form: %s", decoded.Message)
	}
	if left := peekAllNumbers(t, s, "jobs"); fmt.Sprint(left) != "[4 5]" {
		t.Fatalf("the queue holds %v, want [4 5]", left)
	}
}

// TestALostForwardedPopNAdvisesPeekWithItsLimit: a forwarded pop of five that
// is lost tells the caller to peek five, not one.
func TestALostForwardedPopNAdvisesPeekWithItsLimit(t *testing.T) {
	payload := mustMarshalBSON(t, PopRequest{Database: "app", Bucket: "jobs", Limit: 5})
	advice := forwardLostAdvice(transport.OpFIFOPop, "app", "jobs", payload)
	if !strings.Contains(advice, "db.jobs.peek({limit: 5})") {
		t.Fatalf("advice for a lost pop of five: %s", advice)
	}
	single := forwardLostAdvice(transport.OpFIFOPop, "app", "jobs", mustMarshalBSON(t, PopRequest{Database: "app", Bucket: "jobs"}))
	if !strings.Contains(single, "db.jobs.peek()") {
		t.Fatalf("advice for a lost single pop: %s", single)
	}
}

// TestPoppedShortMessagePluralises: the shortfall message keeps its text for
// one entry and counts the entries above one.
func TestPoppedShortMessagePluralises(t *testing.T) {
	one := poppedShortMessage(1, 2, 3, ErrWriteConcernTimeout)
	if !strings.Contains(one, "The entry is in this answer and nowhere else") {
		t.Errorf("single message changed: %s", one)
	}
	many := poppedShortMessage(3, 2, 3, ErrWriteConcernTimeout)
	if !strings.Contains(many, "The 3 entries are in this answer and nowhere else") {
		t.Errorf("plural message: %s", many)
	}
}

// TestPopStaysAWriteOpcode: no pop opcode is read-only or a peek, and every
// one replicates.
func TestPopStaysAWriteOpcode(t *testing.T) {
	for _, op := range []transport.Opcode{transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop, transport.OpFieldPop} {
		if op.IsReadOnly() || isPeekOpcode(op) || !replicationRequired(op) || !isPopOpcode(op) {
			t.Errorf("%s: readOnly=%v peek=%v replicated=%v pop=%v", op, op.IsReadOnly(), isPeekOpcode(op), replicationRequired(op), isPopOpcode(op))
		}
	}
}

// TestPopLimitRefusedForAReader: a user holding only reader@app is refused
// pop({limit: 3}) at the gate with PERMISSION_DENIED and the grant to ask for.
func TestPopLimitRefusedForAReader(t *testing.T) {
	s := userTestServer(t)
	reader := &transport.Session{}
	reader.SetIdentity(transport.Identity{Username: "ro", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	frame := transport.NewFrame(transport.OpFIFOPop, mustMarshalBSON(t, PopRequest{Database: "app", Bucket: "Jobs", Limit: 3}))
	answer, err := NewHandler(s)(transport.WithSession(context.Background(), reader), frame)
	if err != nil || answer.Opcode != transport.OpError {
		t.Fatalf("handler = %v, %v; want an error frame", answer, err)
	}
	if got := decodeErrorCode(t, answer.Payload); got != int(transport.ErrPermissionDenied) {
		t.Errorf("code %d, want PERMISSION_DENIED", got)
	}
	if message := decodeErrorMessage(t, answer.Payload); !strings.Contains(message, `user "ro" lacks "writer" on "app"`) ||
		!strings.Contains(message, "grant role writer@app to ro") {
		t.Errorf("message: %s", message)
	}
}
