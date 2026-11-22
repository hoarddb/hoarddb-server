package ringclient

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// getManyFake is a node that describes buckets and answers RING_GET_MANY frames. The bucket ids
// are 100 + the position of the bucket name in ids; answer decides every key.
type getManyFake struct {
	t      *testing.T
	decl   atomic.Int64
	ids    map[string]uint64
	frames atomic.Int64
	lone   atomic.Int64
	// describes counts describe_bucket calls by bucket name.
	describes sync.Map
	// answer returns the answer for key read of the frame; frame is its 1-based arrival number.
	answer func(frame int, read wire.RingRead, prologue wire.RingPrologue) wire.RingAnswer
	// onFrame, when set, runs before a frame is answered and may block it.
	onFrame func(frame int, request wire.RingGetManyRequest)
	// requests records every decoded frame in arrival order.
	lock     sync.Mutex
	requests []wire.RingGetManyRequest
}

func newGetManyFake(t *testing.T, buckets ...string) (*getManyFake, *fakeServer) {
	fake := &getManyFake{t: t, ids: map[string]uint64{}}
	fake.decl.Store(1)
	for i, name := range buckets {
		fake.ids[name] = uint64(100 + i)
	}
	fake.answer = func(_ int, read wire.RingRead, _ wire.RingPrologue) wire.RingAnswer { return fake.found(read) }
	return fake, startFakeServer(t, fake.handle)
}

// found is the answer of a document that exists: _id_ and the id of the bucket it came from.
func (f *getManyFake) found(read wire.RingRead) wire.RingAnswer {
	raw, err := bson.Marshal(bson.D{{Key: "_id_", Value: decodeID(read.EncID)}, {Key: "bucket", Value: int64(read.BucketID)}})
	if err != nil {
		f.t.Error(err)
	}
	yes := true
	return wire.RingAnswer{Reply: &wire.RingReply{Found: &yes, Elements: [][]byte{raw}}}
}

func (f *getManyFake) bucketName(id uint64) string {
	for name, bucketID := range f.ids {
		if bucketID == id {
			return name
		}
	}
	return ""
}

func (f *getManyFake) handle(conn *fakeConn, frame *transport.Frame) {
	switch frame.Opcode {
	case transport.OpRingAdmin:
		request, err := wire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			f.t.Error(err)
			return
		}
		count, _ := f.describes.LoadOrStore(request.Bucket, new(atomic.Int64))
		count.(*atomic.Int64).Add(1)
		adminReplyFrame(f.t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, BucketID: f.ids[request.Bucket], State: "active",
			DeclVersion: f.decl.Load(), Fields: userFields()})
	case transport.OpRingGetMany:
		// Each frame is served on its own goroutine, as a node does, so one parked frame does not hold the next.
		number := int(f.frames.Add(1))
		go f.serveMany(conn, frame, number)
	case transport.OpRingKeyed:
		f.lone.Add(1)
		conn.refuse(frame, cluster.CodeInvalidArgument, "the fake serves no lone request")
	}
}

func (f *getManyFake) serveMany(conn *fakeConn, frame *transport.Frame, number int) {
	{
		request, err := wire.DecodeRingGetMany(frame.Payload)
		if err != nil {
			f.t.Error(err)
			return
		}
		f.lock.Lock()
		f.requests = append(f.requests, request)
		f.lock.Unlock()
		if f.onFrame != nil {
			f.onFrame(number, request)
		}
		answers := make([]wire.RingAnswer, len(request.Reads))
		outcome := "ok"
		for i, read := range request.Reads {
			answers[i] = f.answer(number, read, request.Prologue)
			if answers[i].Error != nil {
				outcome = "partial"
			}
		}
		payload, err := wire.EncodeRingReply(wire.RingReply{Outcome: outcome, Answers: answers})
		if err != nil {
			f.t.Error(err)
			return
		}
		conn.reply(frame, payload)
	}
}

func dialFake(t *testing.T, server *fakeServer) *Client {
	t.Helper()
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func idList(prefix string, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s%04d", prefix, i)
	}
	return ids
}

func docID(t *testing.T, answer GetAnswer) string {
	t.Helper()
	if answer.Err != nil || !answer.Found {
		t.Fatalf("answer %+v is not a found document", answer)
	}
	return bson.Raw(answer.Doc).Lookup("_id_").StringValue()
}

// TestGetManyAnswersInRequestOrderAcrossBuckets: keys on two buckets and two databases, a miss and
// a repeated key come back one per key in the order sent, as the fake's own documents.
func TestGetManyAnswersInRequestOrderAcrossBuckets(t *testing.T) {
	fake, server := newGetManyFake(t, "User", "Post")
	fake.answer = func(_ int, read wire.RingRead, _ wire.RingPrologue) wire.RingAnswer {
		if decodeID(read.EncID) == "gone" {
			no := false
			return wire.RingAnswer{Reply: &wire.RingReply{Found: &no}}
		}
		return fake.found(read)
	}
	client := dialFake(t, server)
	keys := []Key{
		{Bucket: client.Bucket("app", "User"), ID: "ada"},
		{Bucket: client.Bucket("app", "Post"), ID: "p1"},
		{Bucket: client.Bucket("other", "User"), ID: "gone"},
		{Bucket: client.Bucket("app", "User"), ID: "ada"},
		{Bucket: client.Bucket("app", "Post"), ID: "p2", Fields: []string{"Name"}},
	}
	answers, err := client.GetMany(testContext(t), keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != len(keys) {
		t.Fatalf("%d answers for %d keys", len(answers), len(keys))
	}
	for i, want := range []string{"ada", "p1", "", "ada", "p2"} {
		if want == "" {
			if answers[i].Found || answers[i].Err != nil || answers[i].Doc != nil {
				t.Fatalf("key %d: a miss must be Found false with no error: %+v", i, answers[i])
			}
			continue
		}
		if got := docID(t, answers[i]); got != want {
			t.Fatalf("key %d answered %q, want %q", i, got, want)
		}
	}
	fake.lock.Lock()
	defer fake.lock.Unlock()
	if len(fake.requests) != 1 || fake.lone.Load() != 0 {
		t.Fatalf("%d frames and %d lone requests, want one frame", len(fake.requests), fake.lone.Load())
	}
	reads := fake.requests[0].Reads
	if reads[1].Class != wire.RingClassGet || reads[4].Class != wire.RingClassGetFields || len(reads[4].Fields) != 1 {
		t.Fatalf("classes %#x %#x, want get and get_fields", reads[1].Class, reads[4].Class)
	}
	if reads[0].BucketID != 100 || reads[1].BucketID != 101 {
		t.Fatalf("bucket ids %d %d, want each key under its own bucket's header", reads[0].BucketID, reads[1].BucketID)
	}
}

// TestGetManyOfNothingSendsNothing: no keys is no frame and no error.
func TestGetManyOfNothingSendsNothing(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	client := dialFake(t, server)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), nil, nil)
	if err != nil || len(answers) != 0 {
		t.Fatalf("answers %v, error %v", answers, err)
	}
	if fake.frames.Load() != 0 {
		t.Fatal("an empty get-many sent a frame")
	}
}

// TestGetManySplitsAbove255AndRejoinsInOrder: 256 keys go as a frame of 255 and a frame of 1, and
// both are in flight together (each waits for the other), and the answers rejoin in order.
func TestGetManySplitsAbove255AndRejoinsInOrder(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	var arrived sync.WaitGroup
	arrived.Add(2)
	fake.onFrame = func(_ int, _ wire.RingGetManyRequest) {
		arrived.Done()
		arrived.Wait() // returns only once the second frame has arrived: the frames were concurrent
	}
	client := dialFake(t, server)
	ids := idList("u", 256)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), ids, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if got := docID(t, answers[i]); got != id {
			t.Fatalf("answer %d is %q, want %q", i, got, id)
		}
	}
	var sizes []int
	fake.lock.Lock()
	for _, request := range fake.requests {
		sizes = append(sizes, len(request.Reads))
	}
	fake.lock.Unlock()
	sort.Ints(sizes)
	if len(sizes) != 2 || sizes[0] != 1 || sizes[1] != 255 {
		t.Fatalf("frame sizes %v, want [1 255]", sizes)
	}
}

// TestGetManyOf600KeysIsThreeFramesInOrder: 600 keys are 255 + 255 + 90, and a frame that
// answers last does not reorder the result.
func TestGetManyOf600KeysIsThreeFramesInOrder(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	fake.onFrame = func(_ int, request wire.RingGetManyRequest) {
		if len(request.Reads) == 255 && decodeID(request.Reads[0].EncID) == "u0000" {
			time.Sleep(50 * time.Millisecond) // the first frame answers after the others
		}
	}
	client := dialFake(t, server)
	ids := idList("u", 600)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), ids, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 600 {
		t.Fatalf("%d answers", len(answers))
	}
	for i, id := range ids {
		if got := docID(t, answers[i]); got != id {
			t.Fatalf("answer %d is %q, want %q", i, got, id)
		}
	}
	if fake.frames.Load() != 3 {
		t.Fatalf("%d frames, want 3", fake.frames.Load())
	}
}

// TestGetManyRedescribesAStaleBucketOnceAndResendsItsKeys: User keys answer SCHEMA_NOT_CURRENT and
// Post keys answer, so one describe of User and one more frame carrying only the User keys under
// the new declaration version; the Post keys are not read again. A second bucket id gone stale is
// handled the same way and describes its bucket once however many keys it has.
func TestGetManyRedescribesAStaleBucketOnceAndResendsItsKeys(t *testing.T) {
	fake, server := newGetManyFake(t, "User", "Post")
	fake.answer = func(frame int, read wire.RingRead, _ wire.RingPrologue) wire.RingAnswer {
		if fake.bucketName(read.BucketID) == "User" && read.DeclVersion < 2 {
			return wire.RingAnswer{Error: &wire.RingError{Code: cluster.CodeSchemaNotCurrent, Message: "the schema moved"}}
		}
		return fake.found(read)
	}
	client := dialFake(t, server)
	users, posts := client.Bucket("app", "User"), client.Bucket("app", "Post")
	// Learn both headers at declaration version 1, then move the declaration on.
	if _, err := users.Info(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := posts.Info(testContext(t)); err != nil {
		t.Fatal(err)
	}
	fake.decl.Store(2)
	keys := []Key{{Bucket: users, ID: "a"}, {Bucket: posts, ID: "p"}, {Bucket: users, ID: "b"}, {Bucket: posts, ID: "q"}}
	answers, err := client.GetMany(testContext(t), keys)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"a", "p", "b", "q"} {
		if got := docID(t, answers[i]); got != want {
			t.Fatalf("answer %d is %q, want %q", i, got, want)
		}
	}
	fake.lock.Lock()
	defer fake.lock.Unlock()
	if len(fake.requests) != 2 {
		t.Fatalf("%d frames, want the frame and one resend", len(fake.requests))
	}
	resent := fake.requests[1].Reads
	if len(resent) != 2 || decodeID(resent[0].EncID) != "a" || decodeID(resent[1].EncID) != "b" || resent[0].DeclVersion != 2 {
		t.Fatalf("the resend carries %d keys, want only the two User keys under declaration version 2: %+v", len(resent), resent)
	}
	count, _ := fake.describes.Load("User")
	if count.(*atomic.Int64).Load() != 2 { // once to learn, once after the refusal
		t.Fatalf("User described %d times, want 2", count.(*atomic.Int64).Load())
	}
	count, _ = fake.describes.Load("Post")
	if count.(*atomic.Int64).Load() != 1 {
		t.Fatalf("Post described %d times, want 1", count.(*atomic.Int64).Load())
	}
}

// TestGetManyAKeyStillRefusedAfterTheResendCarriesItsRefusal: a key still refused after the one
// re-describe carries its refusal and is not sent a third time.
func TestGetManyAKeyStillRefusedAfterTheResendCarriesItsRefusal(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	fake.answer = func(_ int, read wire.RingRead, _ wire.RingPrologue) wire.RingAnswer {
		if decodeID(read.EncID) == "never" || read.BucketID != 100 {
			return wire.RingAnswer{Error: &wire.RingError{Code: cluster.CodeSchemaNotCurrent, Message: "still moved"}}
		}
		return fake.found(read)
	}
	client := dialFake(t, server)
	users := client.Bucket("app", "User")
	if _, err := users.Info(testContext(t)); err != nil {
		t.Fatal(err)
	}
	answers, err := client.GetMany(testContext(t), []Key{{Bucket: users, ID: "ok"}, {Bucket: users, ID: "never"}})
	if err != nil {
		t.Fatal(err)
	}
	var refusal *Error
	if docID(t, answers[0]) != "ok" || !errors.As(answers[1].Err, &refusal) || refusal.Code != cluster.CodeSchemaNotCurrent {
		t.Fatalf("answers %+v", answers)
	}
	fake.lock.Lock()
	defer fake.lock.Unlock()
	if len(fake.requests) != 2 || len(fake.requests[1].Reads) != 1 {
		t.Fatalf("%d frames; the second must carry only the key still refused, once", len(fake.requests))
	}
}

// TestGetManyRedescribesAStaleBucketID: an unknown-bucket-id refusal after the bucket was dropped
// and created again (its header changed) re-describes the bucket and resends that key once.
func TestGetManyRedescribesAStaleBucketID(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	fake.answer = func(_ int, read wire.RingRead, _ wire.RingPrologue) wire.RingAnswer {
		if read.DeclVersion < 2 {
			fake.decl.Store(2)
			return wire.RingAnswer{Error: &wire.RingError{Code: cluster.CodePermissionDenied, Message: "no bucket has the id 100"}}
		}
		return fake.found(read)
	}
	client := dialFake(t, server)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), []string{"a", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if docID(t, answers[0]) != "a" || docID(t, answers[1]) != "b" || fake.frames.Load() != 2 {
		t.Fatalf("answers %+v, frames %d", answers, fake.frames.Load())
	}
}

// TestGetManyStaleBucketIDWithAnUnchangedHeaderIsNotResent: the refusal is true when a describe
// finds the header as it was.
func TestGetManyStaleBucketIDWithAnUnchangedHeaderIsNotResent(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	fake.answer = func(_ int, _ wire.RingRead, _ wire.RingPrologue) wire.RingAnswer {
		return wire.RingAnswer{Error: &wire.RingError{Code: cluster.CodePermissionDenied, Message: "no bucket has the id 100"}}
	}
	client := dialFake(t, server)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), []string{"a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var refusal *Error
	if !errors.As(answers[0].Err, &refusal) || refusal.Code != cluster.CodePermissionDenied {
		t.Fatalf("answer %+v", answers[0])
	}
	if fake.frames.Load() != 1 {
		t.Fatalf("%d frames, want 1: an unchanged header makes the refusal true", fake.frames.Load())
	}
}

// TestGetManySurfacesPerKeyErrorsTyped: a refused key is its own typed error, the others are
// answered, a miss is not an error, and the call itself succeeds.
func TestGetManySurfacesPerKeyErrorsTyped(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	fake.answer = func(_ int, read wire.RingRead, _ wire.RingPrologue) wire.RingAnswer {
		switch decodeID(read.EncID) {
		case "denied":
			return wire.RingAnswer{Error: &wire.RingError{Code: cluster.CodePermissionDenied, Message: "no"}}
		case "down":
			return wire.RingAnswer{Error: &wire.RingError{Code: cluster.CodeOwnerDown, Message: "the owner is down"}}
		case "miss":
			no := false
			return wire.RingAnswer{Reply: &wire.RingReply{Found: &no}}
		}
		answer := fake.found(read)
		answer.Reply.Served = "local"
		return answer
	}
	client := dialFake(t, server)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), []string{"ada", "denied", "miss", "down"}, nil)
	if err != nil {
		t.Fatalf("a per-key failure must not fail the call: %v", err)
	}
	var first, second *Error
	if docID(t, answers[0]) != "ada" || answers[0].ServedFrom != "local" {
		t.Fatalf("answer 0 %+v", answers[0])
	}
	if !errors.As(answers[1].Err, &first) || first.Code != cluster.CodePermissionDenied || answers[1].Found {
		t.Fatalf("answer 1 %+v", answers[1])
	}
	if answers[2].Found || answers[2].Err != nil || errors.Is(answers[2].Err, ErrNotFound) {
		t.Fatalf("a miss is Found false with no error: %+v", answers[2])
	}
	if !errors.As(answers[3].Err, &second) || second.Code != cluster.CodeOwnerDown {
		t.Fatalf("answer 3 %+v", answers[3])
	}
}

// TestGetManyReturnsAnUnknownOpcodeRefusalUnchanged: a server that does not know 0xE1 answers
// with its own refusal; the call returns it as it came, sends nothing else (no lone gets) and
// does not retry.
func TestGetManyReturnsAnUnknownOpcodeRefusalUnchanged(t *testing.T) {
	var frames, lone atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		switch frame.Opcode {
		case transport.OpRingAdmin:
			request, err := wire.DecodeRingAdmin(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, BucketID: 7, State: "active", DeclVersion: 1, Fields: userFields()})
		case transport.OpRingKeyed:
			lone.Add(1)
			conn.refuse(frame, cluster.CodeInvalidArgument, "lone")
		default:
			frames.Add(1)
			conn.refuse(frame, cluster.CodeInvalidArgument, "unknown opcode 0xe1 from an older server")
		}
	})
	client := dialFake(t, server)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), idList("u", 3), nil)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Message != "unknown opcode 0xe1 from an older server" || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("error %v, want the server's refusal as it was", err)
	}
	if answers != nil {
		t.Fatalf("answers %v with a frame error", answers)
	}
	if frames.Load() != 1 || lone.Load() != 0 {
		t.Fatalf("%d frames and %d lone gets, want one frame and no fallback", frames.Load(), lone.Load())
	}
}

// TestGetManyStopsWhenTheContextIsCancelled: a node that never answers leaves the call waiting
// until the context ends, and the error says the context did.
func TestGetManyStopsWhenTheContextIsCancelled(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	release := make(chan struct{})
	fake.onFrame = func(int, wire.RingGetManyRequest) { <-release }
	t.Cleanup(func() { close(release) })
	client := dialFake(t, server)
	if _, err := client.Bucket("app", "User").Info(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := client.Bucket("app", "User").GetMany(ctx, idList("u", 600), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v, want the context's deadline", err)
	}
}

// TestGetManySendsTheDeadlineAndLocalPerKey: WithWriteDeadline is the frame's deadline_ms and a
// key's Local is its own local option.
func TestGetManySendsTheDeadlineAndLocalPerKey(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	client := dialFake(t, server)
	users := client.Bucket("app", "User")
	_, err := client.GetMany(testContext(t), []Key{{Bucket: users, ID: "a", Local: true}, {Bucket: users, ID: "b"}}, WithWriteDeadline(250))
	if err != nil {
		t.Fatal(err)
	}
	fake.lock.Lock()
	defer fake.lock.Unlock()
	request := fake.requests[0]
	if request.Prologue.DeadlineMs != 250 || request.Prologue.Epoch != 0 {
		t.Fatalf("prologue %+v, want deadline 250 and epoch 0", request.Prologue)
	}
	if !request.Reads[0].Options.Local || request.Reads[1].Options.Local {
		t.Fatalf("local flags %v %v, want true false", request.Reads[0].Options.Local, request.Reads[1].Options.Local)
	}
}

// TestGetManyRefusesAKeyWithNoBucket: a key without a bucket is the caller's mistake and is
// named before anything is sent.
func TestGetManyRefusesAKeyWithNoBucket(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	client := dialFake(t, server)
	_, err := client.GetMany(testContext(t), []Key{{ID: "a"}})
	if err == nil || !strings.Contains(err.Error(), "key 0") || fake.frames.Load() != 0 {
		t.Fatalf("error %v, frames %d", err, fake.frames.Load())
	}
}

// TestGetManyAFailedBucketDescribeIsThatBucketsKeysOnly: a bucket that cannot be described fails
// its own keys and not the keys of another bucket.
func TestGetManyAFailedBucketDescribeIsThatBucketsKeysOnly(t *testing.T) {
	var frames atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		switch frame.Opcode {
		case transport.OpRingAdmin:
			request, err := wire.DecodeRingAdmin(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			if request.Bucket == "Missing" {
				conn.refuse(frame, cluster.CodeNotFound, `Bucket "Missing" not found. Run 'show buckets in app'`)
				return
			}
			adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, BucketID: 7, State: "active", DeclVersion: 1, Fields: userFields()})
		case transport.OpRingGetMany:
			frames.Add(1)
			request, err := wire.DecodeRingGetMany(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			yes := true
			raw, _ := bson.Marshal(bson.D{{Key: "_id_", Value: "ok"}})
			answers := make([]wire.RingAnswer, len(request.Reads))
			for i := range answers {
				answers[i] = wire.RingAnswer{Reply: &wire.RingReply{Found: &yes, Elements: [][]byte{raw}}}
			}
			payload, err := wire.EncodeRingReply(wire.RingReply{Outcome: "ok", Answers: answers})
			if err != nil {
				t.Error(err)
				return
			}
			conn.reply(frame, payload)
		}
	})
	client := dialFake(t, server)
	answers, err := client.GetMany(testContext(t), []Key{{Bucket: client.Bucket("app", "Missing"), ID: "x"}, {Bucket: client.Bucket("app", "User"), ID: "ok"}})
	if err != nil {
		t.Fatal(err)
	}
	var refusal *Error
	if !errors.As(answers[0].Err, &refusal) || refusal.Code != cluster.CodeNotFound || docID(t, answers[1]) != "ok" || frames.Load() != 1 {
		t.Fatalf("answers %+v, frames %d", answers, frames.Load())
	}
}
