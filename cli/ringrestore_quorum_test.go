package cli

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// quorumScriptTransport answers each Send from a list of answer sets, then answers every
// operation applied.
type quorumScriptTransport struct {
	mu     sync.Mutex
	script [][]ringDocAnswer
	sends  int
	ids    [][16]byte
}

func (s *quorumScriptTransport) NextRequestID() [16]byte { return [16]byte{9} }
func (s *quorumScriptTransport) close() error            { return nil }
func (s *quorumScriptTransport) Send(_ context.Context, ops []restoreOp, id [16]byte) ([]ringDocAnswer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	s.ids = append(s.ids, id)
	if len(s.script) > 0 {
		next := s.script[0]
		s.script = s.script[1:]
		return next, nil
	}
	return make([]ringDocAnswer, len(ops)), nil
}

func quorumAnswers(count int, code cluster.Code) []ringDocAnswer {
	answers := make([]ringDocAnswer, count)
	for index := range answers {
		answers[index].Err = dumpRetryQuorumRefusal(code)
	}
	return answers
}

func quorumRestoreOps() []restoreOp {
	return []restoreOp{{kind: restoreInsert, database: "app", bucket: "Jobs", id: "job-1"}, {kind: restoreInsert, database: "app", bucket: "Jobs", id: "job-2"}}
}

// TestARestoreFrameRefusedForALostQuorumIsSentAgain: a frame answered NO_QUORUM or LEASE_LAPSED
// more often than the sender's few resends is sent again under the same request id until it is
// answered, because the refusal means nothing was applied.
func TestARestoreFrameRefusedForALostQuorumIsSentAgain(t *testing.T) {
	for _, refused := range []cluster.Code{cluster.CodeNoQuorum, cluster.CodeLeaseLapsed} {
		t.Run(refused.String(), func(t *testing.T) {
			dumpShortQuorumRetry(t, 10*time.Second)
			transport := &quorumScriptTransport{}
			for range ringMaxResends + 3 {
				transport.script = append(transport.script, quorumAnswers(2, refused))
			}
			sender := &restoreSender{transport: transport, pause: func(context.Context, int) error { return nil }}
			answers, err := sender.send(context.Background(), quorumRestoreOps())
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			if anyTransient(answers) || len(answers) != 2 || answers[0].Err != nil || answers[1].Err != nil {
				t.Fatalf("the frame is still refused after %d sends: %+v", transport.sends, answers)
			}
			if want := ringMaxResends + 4; transport.sends != want {
				t.Errorf("the frame was sent %d times, want %d", transport.sends, want)
			}
			for _, id := range transport.ids {
				if id != transport.ids[0] {
					t.Fatalf("a resend used another request id: %x", transport.ids)
				}
			}
		})
	}
}

// TestARestoreFrameRefusedForeverForALostQuorumKeepsItsRefusal: past the bound the answers are
// the refusal's own, so the restore stops with the text that names the fix.
func TestARestoreFrameRefusedForeverForALostQuorumKeepsItsRefusal(t *testing.T) {
	dumpShortQuorumRetry(t, 50*time.Millisecond)
	transport := &quorumScriptTransport{}
	for range 100000 {
		transport.script = append(transport.script, quorumAnswers(2, cluster.CodeNoQuorum))
	}
	sender := &restoreSender{transport: transport, pause: func(context.Context, int) error { return nil }}
	answers, err := sender.send(context.Background(), quorumRestoreOps())
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(answers) != 2 || !ringDumpQuorumRefusal(answers[0].Err) || !strings.Contains(answers[0].Err.Error(), "cannot see a majority") {
		t.Fatalf("answers after the bound: %+v", answers)
	}
}

// refusingCreator answers each create NO_QUORUM a set number of times, then succeeds.
type refusingCreator struct {
	databaseRefusals, bucketRefusals int
	databaseCalls, bucketCalls       int
}

func (c *refusingCreator) CreateDatabaseWithStorage(context.Context, string, int32, string) (ringclient.DatabaseInfo, error) {
	c.databaseCalls++
	if c.databaseCalls <= c.databaseRefusals {
		return ringclient.DatabaseInfo{}, dumpRetryQuorumRefusal(cluster.CodeNoQuorum)
	}
	return ringclient.DatabaseInfo{}, nil
}

func (c *refusingCreator) CreateBucketExpiring(context.Context, string, string, string, []ringclient.FieldDecl) (ringclient.BucketInfo, error) {
	c.bucketCalls++
	if c.bucketCalls <= c.bucketRefusals {
		return ringclient.BucketInfo{}, dumpRetryQuorumRefusal(cluster.CodeNoQuorum)
	}
	return ringclient.BucketInfo{}, nil
}

// TestARestoreCreateRefusedForALostQuorumIsAskedAgain covers create database and create bucket.
func TestARestoreCreateRefusedForALostQuorumIsAskedAgain(t *testing.T) {
	dumpShortQuorumRetry(t, 10*time.Second)
	creator := &refusingCreator{databaseRefusals: 2, bucketRefusals: 2}
	run := &ringRestoreRun{ctx: context.Background(), stderr: io.Discard, stdout: io.Discard, flags: &ringRestoreFlags{}, creator: creator}
	if err := run.createDatabase(&dumpDB{Name: "app", RF: 1}, "app"); err != nil {
		t.Fatalf("create database refused NO_QUORUM twice: %v", err)
	}
	if err := run.createBucket(&dumpBucket{Name: "Jobs"}, "app"); err != nil {
		t.Fatalf("create bucket refused NO_QUORUM twice: %v", err)
	}
	if creator.databaseCalls != 3 || creator.bucketCalls != 3 {
		t.Errorf("create database was asked %d times and create bucket %d, want 3 each", creator.databaseCalls, creator.bucketCalls)
	}
}

// quorumTarget refuses its first calls NO_QUORUM, then answers an empty database.
type quorumTarget struct {
	refusals int
	calls    int
}

func (q *quorumTarget) refuse() error {
	q.calls++
	if q.calls <= q.refusals {
		return dumpRetryQuorumRefusal(cluster.CodeNoQuorum)
	}
	return nil
}

func (q *quorumTarget) DescribeDatabase(context.Context, string, ringclient.PageRequest) (ringclient.DatabaseDescription, error) {
	return ringclient.DatabaseDescription{Name: "app", RF: 1}, q.refuse()
}

func (q *quorumTarget) ShowBuckets(context.Context, string, ringclient.PageRequest) (ringclient.BucketPage, error) {
	return ringclient.BucketPage{}, nil
}

func (q *quorumTarget) DescribeBucket(context.Context, string, string) (ringclient.BucketInfo, error) {
	return ringclient.BucketInfo{}, nil
}

// TestTheRestoreCheckOfTheTargetWaitsOutALostQuorum: the pass-2 reads are asked again too.
func TestTheRestoreCheckOfTheTargetWaitsOutALostQuorum(t *testing.T) {
	dumpShortQuorumRetry(t, 10*time.Second)
	target := &quorumTarget{refusals: 2}
	state, _, err := readTargetState(context.Background(), target, "app")
	if err != nil {
		t.Fatalf("readTargetState with a target refused NO_QUORUM twice: %v", err)
	}
	if !state.exists {
		t.Errorf("the database was not seen after the retries: %+v", state)
	}
}

// TestAStoreFrameWithTheLaterOpsHeldBehindALeaseLapseIsSentAgain: [GUARD BUG-0924] the server
// refuses every operation on a store behind a refused one (UNAVAILABLE, "was not attempted"), so a
// lease lapse on push B comes back with push C refused, not applied. The order guard then sees no
// operation applied behind the refusal and the frame is sent again under the same request id.
func TestAStoreFrameWithTheLaterOpsHeldBehindALeaseLapseIsSentAgain(t *testing.T) {
	dumpShortQuorumRetry(t, 10*time.Second)
	push := restoreOp{kind: restorePush, database: "app", bucket: "Jobs", id: "job-1", field: "Queue"}
	ops := []restoreOp{push, push, push}
	held := &ringclient.Error{Code: cluster.CodeUnavailable, Message: "operation 2 of the batch was not attempted: operation 1, on the same store of the same document, was refused"}
	transport := &quorumScriptTransport{script: [][]ringDocAnswer{
		{{}, {Err: dumpRetryQuorumRefusal(cluster.CodeLeaseLapsed)}, {Err: held}},
		{{Deduped: true}, {}, {}},
	}}
	sender := &restoreSender{transport: transport, pause: func(context.Context, int) error { return nil }}
	answers, err := sender.send(context.Background(), ops)
	if err != nil {
		t.Fatalf("send: %v (the held-back push must not read as applied behind the lapse)", err)
	}
	for index, answer := range answers {
		if answer.Err != nil {
			t.Fatalf("operation %d after the resend: %v", index, answer.Err)
		}
	}
	if transport.sends != 2 || transport.ids[0] != transport.ids[1] {
		t.Fatalf("sent %d times under ids %x, want twice under one id", transport.sends, transport.ids)
	}
}

// TestAFrameNotSentBecauseAnOperationCouldNotBeQueuedReportsNoOperationApplied: [GUARD BUG-0924]
// queueing an operation can fail before anything is sent (learning the bucket's header is a
// request of its own, refused LEASE_LAPSED in a blip). The frame is then not sent, so no
// operation in it may read as applied: an answer with no error behind the refusal would make
// the order guard stop the restore with an inversion that never happened, and the insert step
// would count the other entities as orphans. Every answer carries the refusal, so the frame is
// waited out and sent again whole.
func TestAFrameNotSentBecauseAnOperationCouldNotBeQueuedReportsNoOperationApplied(t *testing.T) {
	push := restoreOp{kind: restorePush, database: "app", bucket: "Jobs", id: "job-1", field: "Queue"}
	ops := []restoreOp{push, push, push}
	queued := 0
	answers := queueRestoreOps(ops, func(restoreOp) error {
		queued++
		if queued == 2 {
			return dumpRetryQuorumRefusal(cluster.CodeLeaseLapsed)
		}
		return nil
	})
	if len(answers) != len(ops) {
		t.Fatalf("got %d answers for %d operations", len(answers), len(ops))
	}
	for index, answer := range answers {
		if answer.Err == nil {
			t.Fatalf("operation %d answers applied, but the frame was never sent: %+v", index, answers)
		}
		if !ringDumpQuorumRefusal(answer.Err) {
			t.Fatalf("operation %d answers %v, want the lease lapse that kept the frame from being sent, so it is waited out", index, answer.Err)
		}
	}
	if index := 0; !strings.Contains(answers[index].Err.Error(), "not sent") || !strings.Contains(answers[index].Err.Error(), "operation 1") {
		t.Fatalf("an operation that was not sent says %q; want it to say it was not sent because operation 1 could not be queued", answers[index].Err)
	}
	if transientBeforeApplied(ops, answers) {
		t.Fatal("the order guard sees an operation applied behind the refusal in a frame that was never sent")
	}
	if got := queueRestoreOps(ops, func(restoreOp) error { return nil }); got != nil {
		t.Fatalf("every operation queued, want nil (send the frame), got %+v", got)
	}
}
