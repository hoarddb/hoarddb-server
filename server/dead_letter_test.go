package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The dead verbs on a single node (claim/ack build step 11). A dead entry is
// made the way a restore makes one, through DEAD_RESTORE, so these tests need no
// five-claim ladder.

// deadEntryData is dead entry n as its engine stores it: a document for fifo
// and lifo, {_id_, priority, value} for a heap.
func deadEntryData(t *testing.T, bucketType BucketType, n int, pad string) bson.Raw {
	t.Helper()
	value := bson.M{"n": int32(n), "pad": pad}
	if bucketType != BucketTypeHeap {
		return mustMarshalBSON(t, value)
	}
	return mustMarshalBSON(t, bson.D{{Key: "_id_", Value: fmt.Sprintf("h%d", n)}, {Key: "priority", Value: int64(10 * n)}, {Key: "value", Value: value}})
}

// restoredDead restores dead entries 1..count into bucket, through the same
// handler a restore reaches.
func restoredDead(t *testing.T, s *Server, bucket string, bucketType BucketType, count int, pad string) {
	t.Helper()
	for n := 1; n <= count; n++ {
		answer, err := s.DeadRestoreCtx(context.Background(), DeadRestoreRequest{
			Database: "app", Bucket: bucket, Data: deadEntryData(t, bucketType, n, pad), Deliveries: store.MaxDeliveries,
			DeadAt: time.UnixMilli(1_800_000_000_000), Reason: store.DeadReasonMaxDeliveries,
		})
		if err != nil || !answer.Success {
			t.Fatalf("restoring dead entry %d into %s: %+v, %v", n, bucket, answer, err)
		}
	}
}

// deadNumbers are the n of each listed dead entry.
func deadNumbers(t testing.TB, items []DeadItem) []int {
	t.Helper()
	numbers := make([]int, 0, len(items))
	for _, item := range items {
		document := item.Data
		if value, ok := document.Lookup("value").DocumentOK(); ok {
			document = value
		}
		n, ok := document.Lookup("n").AsInt64OK()
		if !ok {
			t.Fatalf("dead item %v has no integer n", item.Data)
		}
		numbers = append(numbers, int(n))
	}
	return numbers
}

// TestDeadListIsARead pins the opcode tables for the list.
func TestDeadListIsARead(t *testing.T) {
	op := transport.OpDeadList
	if !op.IsReadOnly() || !isPeekOpcode(op) || replicationRequired(op) || op.String() != "DEAD_LIST" {
		t.Errorf("%s: readOnly=%v peek=%v replicated=%v", op, op.IsReadOnly(), isPeekOpcode(op), replicationRequired(op))
	}
	for _, write := range []transport.Opcode{transport.OpDeadRequeue, transport.OpDeadDiscard, transport.OpDeadRestore} {
		if write.IsReadOnly() || isPeekOpcode(write) || !replicationRequired(write) {
			t.Errorf("%s: readOnly=%v peek=%v replicated=%v; want a replicated write", write, write.IsReadOnly(), isPeekOpcode(write), replicationRequired(write))
		}
	}
}

// TestDeadVerbsHaveTheirRules: a Reader can list and is refused the writes, a
// Writer can move entries, and DEAD_RESTORE sent by a client is refused with the
// restore-only text whoever sends it: a restore-operator, root, a Writer.
func TestDeadVerbsHaveTheirRules(t *testing.T) {
	s := userTestServer(t)
	handler := NewHandler(s)
	session := func(role string) context.Context {
		sess := &transport.Session{}
		sess.SetIdentity(transport.Identity{Username: "u-" + role, Roles: []string{role}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: role, DB: "app"}}})
		return transport.WithSession(context.Background(), sess)
	}
	send := func(role string, op transport.Opcode, request any) *transport.Frame {
		answer, err := handler(session(role), transport.NewFrame(op, mustMarshalBSON(t, request)))
		if err != nil {
			t.Fatal(err)
		}
		return answer
	}
	denied := func(answer *transport.Frame) bool {
		return answer.Opcode == transport.OpError && decodeErrorCode(t, answer.Payload) == int(transport.ErrPermissionDenied)
	}
	move := DeadMoveRequest{Database: "app", Bucket: "Jobs", All: true}
	if denied(send("reader", transport.OpDeadList, DeadListRequest{Database: "app", Bucket: "Jobs"})) {
		t.Error("a reader was refused dead()")
	}
	for _, op := range []transport.Opcode{transport.OpDeadRequeue, transport.OpDeadDiscard} {
		if !denied(send("reader", op, move)) {
			t.Errorf("a reader was not refused %s", op)
		}
		if denied(send("writer", op, move)) {
			t.Errorf("a writer was refused %s", op)
		}
	}
	for _, role := range []string{"restore-operator", "root", "writer"} {
		answer := send(role, transport.OpDeadRestore, DeadRestoreRequest{Database: "app", Bucket: "Jobs", Data: mustMarshalBSON(t, bson.M{"n": 1}), Reason: store.DeadReasonCollided, Deliveries: 1})
		if !denied(answer) || !strings.Contains(decodeErrorMessage(t, answer.Payload), "restore-only: DEAD_RESTORE is sent by a restore, never by a client. Run: hoarddb restore <dir>") {
			t.Errorf("a %s sending DEAD_RESTORE got %s", role, answer.Payload)
		}
	}
}

// TestDeadListPagesInTheOrderOfTheList: a page of two, then its next, then the
// last, on every bucket type; the heap lists its highest priority first.
func TestDeadListPagesInTheOrderOfTheList(t *testing.T) {
	s := newTestServer(t)
	for _, bucketType := range leaseBucketTypes {
		bucket := string(bucketType)
		createTestBucket(t, s, "app", bucket, bucketType)
		restoredDead(t, s, bucket, bucketType, 5, "")
		want := []int{1, 2, 3, 4, 5}
		if bucketType == BucketTypeHeap {
			want = []int{5, 4, 3, 2, 1}
		}
		var got []int
		after, lastListed := "", ""
		for pages := 0; ; pages++ {
			page, err := s.DeadList(DeadListRequest{Database: "app", Bucket: bucket, Limit: 2, After: after})
			if err != nil || !page.Found {
				t.Fatalf("%s page %d = %+v, %v", bucket, pages, page, err)
			}
			got = append(got, deadNumbers(t, page.Items)...)
			lastListed = page.Items[len(page.Items)-1].DeadID
			if !strings.Contains(page.Message, "db."+bucket+" has 5 dead entries") {
				t.Errorf("%s: the listing carries no banner: %q", bucket, page.Message)
			}
			if page.Next == "" {
				break
			}
			if page.Next != page.Items[len(page.Items)-1].DeadID {
				t.Fatalf("%s: next %q is not the last dead_id listed", bucket, page.Next)
			}
			after = page.Next
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s listed %v, want %v", bucket, got, want)
		}
		empty, err := s.DeadList(DeadListRequest{Database: "app", Bucket: bucket, After: lastListed})
		if err != nil || empty.Found || empty.Next != "" {
			t.Errorf("%s: a list past the end = %+v, %v", bucket, empty, err)
		}
	}
	plain := "plain"
	createTestBucket(t, s, "app", plain, BucketTypeHash)
	_, err := s.DeadList(DeadListRequest{Database: "app", Bucket: plain})
	requireWrongTypeRefusal(t, err, "dead is not defined for hash buckets")
}

// TestDeadListChargesTheResultBudget: five 400 KiB dead entries under a 1 MiB
// ceiling are refused with RESULT_TOO_LARGE and the DEAD label, and the first
// entry alone is answered.
func TestDeadListChargesTheResultBudget(t *testing.T) {
	qs, _ := peekCeilingServer(t)
	s := qs.core
	pad := strings.Repeat("x", 400<<10)
	for _, bucketType := range leaseBucketTypes {
		t.Run(string(bucketType), func(t *testing.T) {
			bucket := "big-" + string(bucketType)
			createTestBucket(t, s, "app", bucket, bucketType)
			restoredDead(t, s, bucket, bucketType, 5, pad)
			before := queryRefusalsCounted(t, queryMethodDead)
			answer, err := s.DeadList(DeadListRequest{Database: "app", Bucket: bucket, Limit: 5})
			if !errors.Is(err, ErrResultTooLarge) || answer != nil {
				t.Fatalf("dead({limit: 5}) of 400 KiB entries = %+v, %v; want RESULT_TOO_LARGE", answer, err)
			}
			if !strings.Contains(err.Error(), "dead({limit: 2}) fits") || !strings.Contains(err.Error(), "the list is not complete") {
				t.Errorf("the refusal does not say what fits: %v", err)
			}
			if got := queryRefusalsCounted(t, queryMethodDead) - before; got != 1 {
				t.Errorf("the DEAD refusal counter moved by %v, want 1", got)
			}
			one, err := s.DeadList(DeadListRequest{Database: "app", Bucket: bucket, Limit: 1})
			if err != nil || len(one.Items) != 1 {
				t.Fatalf("dead({limit: 1}) = %+v, %v; want the first entry", one, err)
			}
		})
	}
}

// TestDeadListFirstRefusalIsNotACompleteAnswer (coordinator decision
// 2026-10-18): a list the visitor stopped before its first record has listed
// nothing, so it is answered with the ceiling's refusal and never as an empty
// page that reads as the end of the list.
func TestDeadListFirstRefusalIsNotACompleteAnswer(t *testing.T) {
	refusal := resultTooLargeError{query: "dead({limit: 10}) on app.Jobs", shape: resultShapeDead, ceiling: 16 << 20, limit: 10}
	answer, err := deadListAnswer(nil, nil, true, refusal, "")
	if answer != nil || !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("a first visit refused = %+v, %v; want RESULT_TOO_LARGE and no page", answer, err)
	}
	if !strings.Contains(err.Error(), "so the list is not complete") || !strings.Contains(err.Error(), "dead({limit: 1}) fits") {
		t.Errorf("the refusal does not name the fix: %v", err)
	}
	complete, err := deadListAnswer(nil, nil, false, refusal, "")
	if err != nil || complete.Found || complete.Next != "" {
		t.Errorf("an empty complete list = %+v, %v", complete, err)
	}
	partial, err := deadListAnswer([]DeadItem{{DeadID: "00"}}, []byte{0}, true, refusal, "")
	if err != nil || !partial.Found || partial.Next != "00" {
		t.Errorf("a list stopped after its first record = %+v, %v; want the page and its next", partial, err)
	}
}

// TestDeadListOptionsAreRefusedAtTheHandler: a limit out of bounds and an after
// that is not a dead id, with ErrDeadOption.
func TestDeadListOptionsAreRefusedAtTheHandler(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	for _, request := range []DeadListRequest{
		{Database: "app", Bucket: "q", Limit: 1001}, {Database: "app", Bucket: "q", Limit: -1}, {Database: "app", Bucket: "q", After: "xyz"},
	} {
		if _, err := s.DeadList(request); !errors.Is(err, ErrDeadOption) || codeFor(err) != transport.ErrInvalidArgument {
			t.Errorf("dead(%+v) = %v; want ErrDeadOption as INVALID_ARGUMENT", request, err)
		}
	}
	if page, err := s.DeadList(DeadListRequest{Database: "app", Bucket: "q", Limit: 1000}); err != nil || page.Found {
		t.Errorf("dead({limit: 1000}) = %+v, %v; want an empty page", page, err)
	}
}

// TestRequeueAndDiscardAnswerInBand: a requeue writes the entry back to the
// queue in pop order, a discard drops it, an unknown id is refused in band, and
// both or neither of dead_id and all is ErrDeadOption.
func TestRequeueAndDiscardAnswerInBand(t *testing.T) {
	s := newTestServer(t)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := "moves-" + string(engine.bucketType)
			createTestBucket(t, s, "app", bucket, engine.bucketType)
			restoredDead(t, s, bucket, engine.bucketType, 3, "")
			listed, err := s.DeadList(DeadListRequest{Database: "app", Bucket: bucket})
			if err != nil || len(listed.Items) != 3 {
				t.Fatalf("listing: %+v, %v", listed, err)
			}

			requeued, err := s.DeadRequeueCtx(context.Background(), DeadMoveRequest{Database: "app", Bucket: bucket, DeadID: listed.Items[0].DeadID})
			if err != nil || !requeued.Success || requeued.Done != 1 || requeued.Left != 2 {
				t.Fatalf("requeue of one = %+v, %v", requeued, err)
			}
			if got := peekAllNumbers(t, s, bucket); len(got) != 1 {
				t.Fatalf("the queue holds %v after a requeue of one entry", got)
			}
			again, err := s.DeadRequeueCtx(context.Background(), DeadMoveRequest{Database: "app", Bucket: bucket, DeadID: listed.Items[0].DeadID})
			if err != nil || again.Success || again.Done != 0 || len(again.Refused) != 1 ||
				!strings.Contains(again.Refused[0].Message, "no dead entry \""+listed.Items[0].DeadID+"\" in app."+bucket+": it may have been requeued or discarded already.") ||
				!strings.Contains(again.Refused[0].Message, "List what is there: db."+bucket+".dead()") {
				t.Fatalf("requeue of the same id again = %+v, %v; want it refused in band by name", again, err)
			}

			discarded, err := s.DeadDiscardCtx(context.Background(), DeadMoveRequest{Database: "app", Bucket: bucket, All: true})
			if err != nil || !discarded.Success || discarded.Done != 2 || discarded.Left != 0 {
				t.Fatalf("discard of all = %+v, %v", discarded, err)
			}
			none, err := s.DeadDiscardCtx(context.Background(), DeadMoveRequest{Database: "app", Bucket: bucket, All: true})
			if err != nil || !none.Success || none.Done != 0 {
				t.Fatalf("discard of none = %+v, %v", none, err)
			}
			for _, bad := range []DeadMoveRequest{
				{Database: "app", Bucket: bucket}, {Database: "app", Bucket: bucket, DeadID: listed.Items[1].DeadID, All: true}, {Database: "app", Bucket: bucket, DeadID: "not hex"},
			} {
				if _, err := s.DeadRequeueCtx(context.Background(), bad); !errors.Is(err, ErrDeadOption) {
					t.Errorf("requeue(%+v) = %v; want ErrDeadOption", bad, err)
				}
			}
		})
	}
}

// TestAPermanentlyRefusedHeapEntryDoesNotSpinTheLoop: a heap dead entry whose
// key a live element holds is refused by name on every requeue, so left never
// reaches 0. A caller that repeats "requeue all" while done is above 0 (the
// loop the answer's message prescribes) stops at the second request (done 0); one
// that looped on left would never stop (D3 verification, required note 1).
func TestAPermanentlyRefusedHeapEntryDoesNotSpinTheLoop(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "ranked", BucketTypeHeap)
	restoredDead(t, s, "ranked", BucketTypeHeap, 3, "")
	if resp, err := s.Push(PushRequest{Database: "app", Bucket: "ranked", Priority: 20, Data: map[string]interface{}{"_id_": "h2", "n": int32(2)}}); err != nil || !resp.Success {
		t.Fatalf("pushing the live twin of dead entry 2: %+v, %v", resp, err)
	}
	requests, moved := 0, 0
	var last *DeadMoveResponse
	for ; requests < 10; requests++ {
		answer, err := s.DeadRequeueCtx(context.Background(), DeadMoveRequest{Database: "app", Bucket: "ranked", All: true})
		if err != nil {
			t.Fatal(err)
		}
		last = answer
		if answer.Done == 0 {
			break
		}
		moved += answer.Done
	}
	if requests+1 != 2 || moved != 2 {
		t.Fatalf("the loop made %d requests and moved %d entries, want 2 requests moving 2", requests+1, moved)
	}
	if last.Left != 1 || len(last.Refused) != 1 || last.Success {
		t.Fatalf("the last answer = %+v; want one entry left and refused by name", last)
	}
	refused := last.Refused[0].Message
	for _, want := range []string{`requeue of "`, "the heap app.ranked already holds an element with id \"h2\" at priority 20", "db.ranked.discard({dead_id:", "or remove the live one first"} {
		if !strings.Contains(refused, want) {
			t.Errorf("the refusal %q lacks %q", refused, want)
		}
	}
	if !strings.Contains(last.Message, "1 dead entries are left in app.ranked, 1 of them refused by name above") || !strings.Contains(last.Message, "while done is above 0") {
		t.Errorf("the message does not say that left counts refused entries and what to loop on: %q", last.Message)
	}
}

// TestRequeueAllAfterReachesEntriesBehindARefusedHead (BUG-0637), through the
// DEAD_REQUEUE opcode: 100 refused dead heap entries at the head, 10 requeueable
// behind them. Without after every request answers done 0; with the last refused
// dead_id as after the 10 move, and the 100 stay dead.
func TestRequeueAllAfterReachesEntriesBehindARefusedHead(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	createTestBucket(t, s, "app", "ranked", BucketTypeHeap)
	restoreRange := func(from, to int) {
		for n := from; n <= to; n++ {
			answer, err := s.DeadRestoreCtx(context.Background(), DeadRestoreRequest{
				Database: "app", Bucket: "ranked", Data: deadEntryData(t, BucketTypeHeap, n, ""), Deliveries: store.MaxDeliveries,
				DeadAt: time.UnixMilli(1_800_000_000_000), Reason: store.DeadReasonMaxDeliveries,
			})
			if err != nil || !answer.Success {
				t.Fatalf("restoring dead entry %d: %+v, %v", n, answer, err)
			}
		}
	}
	requeue := func(after string) DeadMoveResponse {
		request := map[string]any{"database": "app", "bucket": "ranked", "all": true}
		if after != "" {
			request["after"] = after
		}
		var answer DeadMoveResponse
		if err := bson.Unmarshal(handlerSend(t, handler, transport.OpDeadRequeue, request).Payload, &answer); err != nil {
			t.Fatal(err)
		}
		return answer
	}
	// Higher priority sorts first: 101..200 are the head, 1..10 sit behind.
	restoreRange(101, 200)
	if setup := requeue(""); setup.Done != 100 {
		t.Fatalf("setup requeue = %+v, want 100 done", setup)
	}
	restoreRange(101, 200)
	restoreRange(1, 10)

	moved, after := 0, ""
	for range 5 {
		answer := requeue(after)
		moved += answer.Done
		if len(answer.Refused) == 0 {
			break
		}
		after = answer.Refused[len(answer.Refused)-1].DeadID
	}
	if moved != 10 {
		t.Fatalf("requeue all with after moved %d of the 10 entries behind the refused head, want 10", moved)
	}
	if last := requeue(after); last.Done != 0 || last.Left != 100 {
		t.Fatalf("a further request = %+v, want done 0 and the 100 refused left", last)
	}
}

// TestDeadMoveAfterIsRefusedWithoutAllAndWhenNotADeadID: after pages only an all
// request and must be a dead_id, refused before anything is logged.
func TestDeadMoveAfterIsRefusedWithoutAllAndWhenNotADeadID(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "jobs", BucketTypeFIFO)
	for _, bad := range []DeadMoveRequest{
		{Database: "app", Bucket: "jobs", DeadID: "00", After: "00"},
		{Database: "app", Bucket: "jobs", All: true, After: "not hex"},
	} {
		if _, err := s.DeadRequeueCtx(context.Background(), bad); !errors.Is(err, ErrDeadOption) {
			t.Errorf("requeue(%+v) = %v; want ErrDeadOption", bad, err)
		}
		if err := resolveDeadMove(mustMarshalBSON(t, bad), "requeue"); !errors.Is(err, ErrDeadOption) {
			t.Errorf("resolveDeadMove(%+v) = %v; want ErrDeadOption at the door", bad, err)
		}
	}
}

// TestDeadMessagesNameTheFix pins the banner of claim-ack-spec 9.5.
func TestDeadMessagesNameTheFix(t *testing.T) {
	want := "db.Jobs has 2 dead entries (claimed 5 times without an acknowledgement).\n" +
		"  Read them:     db.Jobs.dead()\n  Retry them:    db.Jobs.requeue({all: true})\n  Drop them:     db.Jobs.discard({all: true})"
	if got := deadBanner("Jobs", 2); got != want {
		t.Errorf("banner\n got %q\nwant %q", got, want)
	}
	if got := deadBanner("Jobs", 1); !strings.Contains(got, "has 1 dead entry (") {
		t.Errorf("banner for one: %q", got)
	}
}

// TestRestoreDeadRefusesWhatNoLivePathWrote: an unknown reason or fewer than one
// delivery is ErrDeadOption, and a hash bucket has no dead entries to restore.
func TestRestoreDeadRefusesWhatNoLivePathWrote(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	createTestBucket(t, s, "app", "h", BucketTypeHash)
	good := DeadRestoreRequest{Database: "app", Bucket: "q", Data: deadEntryData(t, BucketTypeFIFO, 1, ""), Deliveries: 1, Reason: store.DeadReasonCollided}
	for name, mutate := range map[string]func(*DeadRestoreRequest){
		"unknown reason": func(r *DeadRestoreRequest) { r.Reason = "poisoned" },
		"no deliveries":  func(r *DeadRestoreRequest) { r.Deliveries = 0 },
		"hash bucket":    func(r *DeadRestoreRequest) { r.Bucket = "h" },
	} {
		request := good
		mutate(&request)
		if _, err := s.DeadRestoreCtx(context.Background(), request); !errors.Is(err, ErrDeadOption) {
			t.Errorf("%s: restore = %v; want ErrDeadOption", name, err)
		}
	}
	if _, err := s.DeadRestoreCtx(context.Background(), good); err != nil {
		t.Errorf("a valid record was refused: %v", err)
	}
}

// TestDeadVerbsHaveNoRowForTheRead: the member-feature table gates the three
// writes and has no entry for the read.
func TestDeadVerbsHaveNoRowForTheRead(t *testing.T) {
	if _, present := writeOpcodeFeatures[transport.OpDeadList]; present {
		t.Error("writeOpcodeFeatures has an entry for DEAD_LIST, which is a read")
	}
	for _, op := range []transport.Opcode{transport.OpDeadRequeue, transport.OpDeadDiscard, transport.OpDeadRestore} {
		if writeOpcodeFeatures[op] != featureQueueClaim {
			t.Errorf("%s is gated on %q, want %q", op, writeOpcodeFeatures[op], featureQueueClaim)
		}
	}
}

// TestDeadIDsRoundTripAsHex pins the dead_id form: lowercase hex of the dead
// suffix, the same string a restore of the list would name.
func TestDeadIDsRoundTripAsHex(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeFIFO)
	restoredDead(t, s, "q", BucketTypeFIFO, 1, "")
	page, err := s.DeadList(DeadListRequest{Database: "app", Bucket: "q"})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("list: %+v, %v", page, err)
	}
	if raw, err := hex.DecodeString(page.Items[0].DeadID); err != nil || len(raw) != 8 || page.Items[0].DeadID != strings.ToLower(page.Items[0].DeadID) {
		t.Errorf("dead_id %q is not 16 lowercase hex characters", page.Items[0].DeadID)
	}
	if page.Items[0].Reason != store.DeadReasonMaxDeliveries || page.Items[0].Deliveries != store.MaxDeliveries || !page.Items[0].DeadAt.Equal(time.UnixMilli(1_800_000_000_000).UTC()) {
		t.Errorf("the listed record is %+v", page.Items[0])
	}
}
