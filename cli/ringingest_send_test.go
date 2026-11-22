package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// recordedFrame is one frame a stub transport was asked to send.
type recordedFrame struct {
	id   [16]byte
	docs []ringIngestDoc
}

// stubBatchTransport answers frames from a script: the decide function sees the frame (and how
// many it has seen) and returns the per-document answers or an error for the whole frame.
type stubBatchTransport struct {
	next    byte
	frames  *[]recordedFrame
	decide  func(attempt int, frame recordedFrame) ([]ringDocAnswer, error)
	attempt *int
}

func (s *stubBatchTransport) NextRequestID() [16]byte {
	s.next++
	return [16]byte{0xaa, s.next}
}

func (s *stubBatchTransport) Upserts(_ context.Context, _, _ string, docs []ringIngestDoc, id [16]byte) ([]ringDocAnswer, error) {
	frame := recordedFrame{id: id, docs: append([]ringIngestDoc(nil), docs...)}
	*s.frames = append(*s.frames, frame)
	*s.attempt++
	return s.decide(*s.attempt, frame)
}

func (s *stubBatchTransport) close() error { return nil }

func appliedAnswers(count int) []ringDocAnswer { return make([]ringDocAnswer, count) }

// scriptedSender builds a sender over a stub transport; redials hands out the same script.
func scriptedSender(decide func(attempt int, frame recordedFrame) ([]ringDocAnswer, error)) (*ringBatchSender, *[]recordedFrame, *int) {
	frames := []recordedFrame{}
	attempts, redials := 0, 0
	transport := &stubBatchTransport{frames: &frames, decide: decide, attempt: &attempts}
	sender := &ringBatchSender{
		database: "app", bucket: "users", transport: transport, maxDocuments: 100, maxBytes: 2 << 20,
		redial: func(context.Context) (ringBatchTransport, error) {
			redials++
			return transport, nil
		},
		pause: func(context.Context, int) error { return nil },
	}
	return sender, &frames, &redials
}

func numberedDocs(count int) []ringIngestDoc {
	docs := make([]ringIngestDoc, count)
	for i := range docs {
		docs[i] = ringIngestDoc{Ordinal: int64(i + 1), ID: fmt.Sprintf("id%d", i), Fields: ringclient.Doc{"n": int32(i)}, Bytes: 64}
	}
	return docs
}

// TestResendRebuildsAnIdenticalBatch: the frame sent after a lost connection holds the same
// documents in the same order under the same request id, because the owner refuses a request
// id that comes back with different content.
func TestResendRebuildsAnIdenticalBatch(t *testing.T) {
	sender, frames, redials := scriptedSender(func(attempt int, frame recordedFrame) ([]ringDocAnswer, error) {
		if attempt == 1 {
			return nil, fmt.Errorf("ringclient: the connection to the ring ended: %w", io.EOF)
		}
		return appliedAnswers(len(frame.docs)), nil
	})
	docs := numberedDocs(5)
	failures, err := sender.sendFrame(context.Background(), docs)
	if err != nil || len(failures) != 0 {
		t.Fatalf("sendFrame: failures %v, error %v; want none", failures, err)
	}
	if len(*frames) != 2 || *redials != 1 {
		t.Fatalf("%d frames and %d redials, want 2 and 1", len(*frames), *redials)
	}
	first, second := (*frames)[0], (*frames)[1]
	if first.id != second.id {
		t.Errorf("the resend used request id %x, the first send used %x; they must match", second.id, first.id)
	}
	if !reflect.DeepEqual(first.docs, second.docs) {
		t.Errorf("the resend holds different documents than the first send:\n first %+v\nsecond %+v", first.docs, second.docs)
	}
}

// TestResendGivesUpAfterThreeResendsAsOutcomeUnknown: four failed attempts in all, then the
// run is told the outcome is unknown, which is exit 8.
func TestResendGivesUpAfterThreeResendsAsOutcomeUnknown(t *testing.T) {
	sender, frames, _ := scriptedSender(func(int, recordedFrame) ([]ringDocAnswer, error) {
		return nil, fmt.Errorf("ringclient: the connection to the ring ended: %w", io.ErrUnexpectedEOF)
	})
	_, err := sender.sendFrame(context.Background(), numberedDocs(3))
	var unknown *ringOutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("sendFrame: error %v, want *ringOutcomeUnknownError", err)
	}
	if len(*frames) != 4 {
		t.Errorf("%d attempts, want 4 (the send and three resends)", len(*frames))
	}
	if unknown.Documents != 3 || unknown.FirstOrdinal != 1 || unknown.LastOrdinal != 3 {
		t.Errorf("the unknown outcome names %+v, want 3 documents, 1 to 3", unknown)
	}
}

// TestAPartialBatchReportsEachRefusedDocument: a refusal of one operation is that document's
// failure, the others are applied, and the failure names the document by its position.
func TestAPartialBatchReportsEachRefusedDocument(t *testing.T) {
	sender, _, _ := scriptedSender(func(_ int, frame recordedFrame) ([]ringDocAnswer, error) {
		answers := appliedAnswers(len(frame.docs))
		answers[1].Err = &ringclient.Error{Code: cluster.CodeFieldType, Message: `field "age" is a string and the bucket declares int`}
		return answers, nil
	})
	failures, err := sender.sendFrame(context.Background(), numberedDocs(4))
	if err != nil {
		t.Fatalf("sendFrame: %v", err)
	}
	if len(failures) != 1 || failures[0].Doc.Ordinal != 2 {
		t.Fatalf("failures %+v, want one, for document 2", failures)
	}
	if text := failures[0].text("users.bson"); !strings.Contains(text, "Document 2 of users.bson") || !strings.Contains(text, "FIELD_TYPE") || !strings.Contains(text, `"age"`) {
		t.Errorf("failure text %q should name document 2, the code and the ring's words", text)
	}
	if sender.stats.Applied != 3 {
		t.Errorf("applied = %d, want 3", sender.stats.Applied)
	}
}

// TestTransientAnswerIsResentUnderTheSameID: an operation answered with a code that clears by
// waiting (a slot that moved) is not a refusal of the document: the frame is sent again under
// the same id and the operations that were applied answer Deduped.
func TestTransientAnswerIsResentUnderTheSameID(t *testing.T) {
	sender, frames, redials := scriptedSender(func(attempt int, frame recordedFrame) ([]ringDocAnswer, error) {
		answers := appliedAnswers(len(frame.docs))
		if attempt == 1 {
			answers[2].Err = &ringclient.Error{Code: cluster.CodeOwnerDown, Message: "the owner is down"}
		} else {
			for i := range answers {
				answers[i].Deduped = i != 2
			}
		}
		return answers, nil
	})
	failures, err := sender.sendFrame(context.Background(), numberedDocs(4))
	if err != nil || len(failures) != 0 {
		t.Fatalf("failures %v, error %v; want none", failures, err)
	}
	if len(*frames) != 2 || (*frames)[0].id != (*frames)[1].id || *redials != 0 {
		t.Errorf("frames %d, same id %v, redials %d; want 2 frames under one id and no redial (the connection is fine)", len(*frames), len(*frames) == 2 && (*frames)[0].id == (*frames)[1].id, *redials)
	}
	if sender.stats.Applied != 1 || sender.stats.Deduped != 3 {
		t.Errorf("applied %d and deduped %d, want 1 and 3", sender.stats.Applied, sender.stats.Deduped)
	}
}

// TestTooLargeBatchIsHalvedAndResent: a frame the server refuses for its size is split in two
// and each half sent on its own, and the smaller size is kept for the frames that follow.
func TestTooLargeBatchIsHalvedAndResent(t *testing.T) {
	sender, frames, redials := scriptedSender(func(_ int, frame recordedFrame) ([]ringDocAnswer, error) {
		if len(frame.docs) > 3 {
			return nil, &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: "this RING_BATCH request is 40000000 bytes, over the 32 MiB frame limit, so it was not processed. Split it into smaller requests"}
		}
		return appliedAnswers(len(frame.docs)), nil
	})
	failures, err := sender.sendFrame(context.Background(), numberedDocs(8))
	if err != nil || len(failures) != 0 {
		t.Fatalf("failures %v, error %v; want none", failures, err)
	}
	if sender.stats.Applied != 8 {
		var sizes []int
		for _, frame := range *frames {
			sizes = append(sizes, len(frame.docs))
		}
		t.Errorf("frame sizes %v, applied %d; want every one of the 8 applied", sizes, sender.stats.Applied)
	}
	if sender.stats.Halvings == 0 || sender.maxDocuments >= 8 {
		t.Errorf("halvings %d and the frame limit is now %d; the limit must shrink and stay shrunk", sender.stats.Halvings, sender.maxDocuments)
	}
	if *redials != 0 {
		t.Errorf("a refusal of size redialed %d times; the connection is fine", *redials)
	}
}

// TestOneDocumentThatIsTooLargeAloneIsItsFailure: a frame of one document cannot be halved, so
// the document is refused by position, not the run.
func TestOneDocumentThatIsTooLargeAloneIsItsFailure(t *testing.T) {
	sender, _, _ := scriptedSender(func(_ int, frame recordedFrame) ([]ringDocAnswer, error) {
		return nil, &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: "this RING_BATCH request is 40000000 bytes, over the 32 MiB frame limit, so it was not processed"}
	})
	failures, err := sender.sendFrame(context.Background(), numberedDocs(1))
	if err != nil {
		t.Fatalf("sendFrame: %v", err)
	}
	if len(failures) != 1 || !strings.Contains(failures[0].text("users.bson"), "frame limit") {
		t.Errorf("failures %+v, want the one document refused with the ring's size words", failures)
	}
}

// TestAddFlushesAtTheDocumentAndByteLimits: the sender cuts frames at --batch documents or at
// the byte budget, whichever comes first.
func TestAddFlushesAtTheDocumentAndByteLimits(t *testing.T) {
	sender, frames, _ := scriptedSender(func(_ int, frame recordedFrame) ([]ringDocAnswer, error) {
		return appliedAnswers(len(frame.docs)), nil
	})
	sender.maxDocuments, sender.maxBytes = 3, 200
	for _, doc := range numberedDocs(7) { // 64 bytes each: three fit the byte budget exactly
		if _, err := sender.add(context.Background(), doc); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sender.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, frame := range *frames {
		sizes = append(sizes, len(frame.docs))
	}
	if !reflect.DeepEqual(sizes, []int{3, 3, 1}) {
		t.Errorf("frame sizes %v, want [3 3 1]", sizes)
	}
}

// TestResendAfterTheRealConnectionIsClosedRedialsAndDedupes is the test no stub can pass: a
// live ring applies the frame and a relay closes the connection before the reply reaches the
// client. The sender must dial again, send the same request id, receive Deduped answers, and
// leave the count where one application put it.
func TestResendAfterTheRealConnectionIsClosedRedialsAndDedupes(t *testing.T) {
	serverAddr, direct := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := direct.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := direct.CreateBucket(ctx, "app", "users", []ringclient.FieldDecl{{Name: "n", Type: "int"}, {Name: "b", Type: "string"}, {Name: "a", Type: "string"}, {Name: "c", Type: "bool"}}); err != nil {
		t.Fatal(err)
	}
	relay := newDroppingRelay(t, serverAddr)
	options := ringclient.Options{Addr: relay.Addr(), User: "root", Password: "live-test-password", Insecure: true}
	viaRelay, err := ringclient.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	dials := 1
	sender := newRingBatchSender(viaRelay, func(ctx context.Context) (*ringclient.Client, error) {
		dials++
		return redialRing(ctx, options)
	}, "app", "users", 100)
	defer sender.close()
	// Learn the header before the reply is dropped: the first request on a connection is the
	// describe, and the relay must lose the batch's reply, not that one.
	if _, err := viaRelay.DescribeBucket(ctx, "app", "users"); err != nil {
		t.Fatal(err)
	}
	relay.DropNextReply()

	docs := numberedDocs(5)
	for i := range docs {
		docs[i].Fields = ringclient.Doc{"n": int32(i), "b": "bee", "a": "ay", "c": true}
	}
	failures, err := sender.sendFrame(ctx, docs)
	if err != nil || len(failures) != 0 {
		t.Fatalf("sendFrame: failures %v, error %v", failures, err)
	}
	select {
	case <-relay.Dropped():
	default:
		t.Fatal("the relay never dropped a reply, so the test did not exercise a lost reply")
	}
	if dials != 2 {
		t.Errorf("dial count = %d, want 2: the first connection and one redial", dials)
	}
	if sender.stats.Deduped != 5 || sender.stats.Applied != 0 {
		t.Errorf("applied %d and deduped %d; want the resend answered Deduped for all 5 (the server applied the first send)", sender.stats.Applied, sender.stats.Deduped)
	}
	count, err := direct.Bucket("app", "users").Count(ctx, nil)
	if err != nil || count != 5 {
		t.Errorf("count = %d (error %v), want 5: a resend must not add documents", count, err)
	}
}

// TestOversizeFrameOnALiveRingIsHalvedAndEveryDocumentLands: three documents of 12 MiB do not
// fit one 32 MiB frame. The ring client refuses to encode such a frame (it says so before a
// byte is sent), and the sender must treat that as "too large", halve, and load all three,
// instead of stopping the run.
func TestOversizeFrameOnALiveRingIsHalvedAndEveryDocumentLands(t *testing.T) {
	_, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "big", []ringclient.FieldDecl{{Name: "s", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	sender := newRingBatchSender(client, func(context.Context) (*ringclient.Client, error) {
		return nil, errors.New("this test must not redial")
	}, "app", "big", 100)
	payload := strings.Repeat("x", 12<<20)
	docs := make([]ringIngestDoc, 3)
	for i := range docs {
		docs[i] = ringIngestDoc{Ordinal: int64(i + 1), ID: fmt.Sprintf("d%d", i), Fields: ringclient.Doc{"s": payload}, Bytes: len(payload)}
	}
	failures, err := sender.sendFrame(ctx, docs)
	if err != nil || len(failures) != 0 {
		t.Fatalf("sendFrame: failures %v, error %v; want all three documents loaded", failures, err)
	}
	if sender.stats.Applied != 3 || sender.stats.Halvings == 0 {
		t.Errorf("applied %d and halvings %d, want 3 applied after at least one halving", sender.stats.Applied, sender.stats.Halvings)
	}
	count, err := client.Bucket("app", "big").Count(ctx, nil)
	if err != nil || count != 3 {
		t.Errorf("count %d (error %v), want 3", count, err)
	}
}
