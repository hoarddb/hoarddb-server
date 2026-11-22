package harness_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// batchAllowAll admits every request: these tests are about routing, not grants.
type batchAllowAll struct{}

func (batchAllowAll) Authorize(context.Context, wire.RingAccess) error { return nil }

func batchNext(_ context.Context, frame *transport.Frame) (*transport.Frame, error) {
	return transport.NewFrameWithID(transport.OpError, frame.RequestID, []byte{5, 0, 0, 0, 0}), nil
}

// batchClient drives the ring handler of one node the way a connection would, without TLS.
type batchClient struct {
	t       *testing.T
	handler transport.RequestHandler
	header  wire.RingAdminReply
	nameID  uint16
}

func newBatchClient(t *testing.T, c *harness.Cluster, node string) *batchClient {
	t.Helper()
	handler, err := wire.NewRingHandler(c.Node(node), batchNext, batchAllowAll{})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: proofDB, Bucket: proofBucket})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := handler(proofCtx(t), transport.NewFrameWithID(transport.OpRingAdmin, [16]byte{1}, payload))
	if err != nil || reply.Opcode != transport.OpRingReply {
		t.Fatalf("describe_bucket on %s: %v %v", node, reply, err)
	}
	header, err := wire.DecodeRingAdminReply(reply.Payload)
	if err != nil {
		t.Fatal(err)
	}
	bc := &batchClient{t: t, handler: handler, header: header}
	for _, field := range header.Fields {
		if field.Name == "name" {
			bc.nameID = field.ID
		}
	}
	return bc
}

func (bc *batchClient) upsert(id string) cluster.Op {
	bc.t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		bc.t.Fatal(err)
	}
	return cluster.Op{
		Class: cluster.OpUpsert, BucketID: bc.header.BucketID, DeclVersion: bc.header.DeclVersion, SchemaVersionSeen: bc.header.SchemaVersionSeen,
		EncID: encoded, IDValue: append([]byte{0x02, byte(len(id) + 1), 0, 0, 0}, append([]byte(id), 0)...),
		Fields: []cluster.FieldOp{{FieldID: bc.nameID, Value: append([]byte{0x02, byte(len(id) + 1), 0, 0, 0}, append([]byte(id), 0)...)}},
	}
}

// lone sends op as a RING_KEYED and returns the code it was refused with (0 when it was applied).
func (bc *batchClient) lone(op cluster.Op) cluster.Code {
	bc.t.Helper()
	payload, err := wire.EncodeRingKeyed(wire.RingKeyedRequest{Op: op})
	if err != nil {
		bc.t.Fatal(err)
	}
	frame, err := bc.handler(proofCtx(bc.t), transport.NewFrameWithID(transport.OpRingKeyed, [16]byte{0xB2}, payload))
	if err != nil {
		bc.t.Fatal(err)
	}
	if frame.Opcode != transport.OpError {
		return 0
	}
	refusal, err := wire.DecodeRingError(frame.Payload)
	if err != nil {
		bc.t.Fatal(err)
	}
	return refusal.Code
}

func (bc *batchClient) send(id [16]byte, ops []cluster.Op) wire.RingReply {
	bc.t.Helper()
	payload, err := wire.EncodeRingBatch(wire.RingBatchRequest{Ops: ops})
	if err != nil {
		bc.t.Fatal(err)
	}
	frame, err := bc.handler(proofCtx(bc.t), transport.NewFrameWithID(transport.OpRingBatch, id, payload))
	if err != nil || frame.Opcode != transport.OpRingReply {
		bc.t.Fatalf("RING_BATCH answered %v %v", frame, err)
	}
	reply, err := wire.DecodeRingReply(frame.Payload)
	if err != nil {
		bc.t.Fatal(err)
	}
	if len(reply.Answers) != len(ops) {
		bc.t.Fatalf("%d answers for %d operations", len(reply.Answers), len(ops))
	}
	return reply
}

// batchIDs picks n document ids and where each is owned.
func batchIDs(t *testing.T, c *harness.Cluster, n int) (ids, owners []string) {
	t.Helper()
	probe := c.Node(c.IDs()[0])
	for i := range n {
		id := fmt.Sprintf("batch-%d", i)
		owner, _, err := probe.Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		ids, owners = append(ids, id), append(owners, owner)
	}
	return ids, owners
}

// TestARingBatchSentToOneNodeReachesEveryOwner: a frame sent to one node applies the operations
// that node owns and forwards the others, one hop each; every document lands on its owner, the
// answers come back in request order, and sending the frame again under the same request id
// applies nothing twice.
func TestARingBatchSentToOneNodeReachesEveryOwner(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	c.AwaitConverged(10 * time.Second)
	bc := newBatchClient(t, c, c.IDs()[0])
	ids, owners := batchIDs(t, c, 18)
	distinct := map[string]bool{}
	for _, owner := range owners {
		distinct[owner] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("all %d documents have one owner %v; the test needs more than one", len(ids), distinct)
	}
	ops := make([]cluster.Op, len(ids))
	for i, id := range ids {
		ops[i] = bc.upsert(id)
	}
	requestID := [16]byte{0xB1, 1}
	first := bc.send(requestID, ops)
	if first.Outcome != "ok" {
		for i, answer := range first.Answers {
			if answer.Error != nil {
				t.Errorf("operation %d (%s, owner %s): %s %s", i, ids[i], owners[i], answer.Error.Code, answer.Error.Message)
			}
		}
		t.Fatalf("outcome %q (background: %v)", first.Outcome, c.BackgroundErrors())
	}
	for i, id := range ids {
		if answer := first.Answers[i]; answer.Reply == nil || answer.Reply.WriteOutcome != "applied" || answer.Reply.Deduped {
			t.Fatalf("operation %d = %+v, want a fresh apply", i, answer)
		}
		if _, found, err := c.Node(owners[i]).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
			t.Fatalf("owner %s does not hold %s: found %v err %v", owners[i], id, found, err)
		}
	}
	again := bc.send(requestID, ops)
	for i, answer := range again.Answers {
		if answer.Reply == nil || !answer.Reply.Deduped || *answer.Reply.Position != *first.Answers[i].Reply.Position {
			t.Fatalf("operation %d on the retry = %+v, want the first answer, deduped", i, answer)
		}
	}
}

// TestARingBatchWithADeadOwnerAnswersItsOperationsAndAppliesTheRest: the owner of some
// documents is down and detection is off, so those operations are refused one by one while the
// frame's other operations apply.
func TestARingBatchWithADeadOwnerAnswersItsOperationsAndAppliesTheRest(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	c.AwaitConverged(10 * time.Second)
	bc := newBatchClient(t, c, "n1")
	ids, owners := batchIDs(t, c, 24)
	// One batch while every node is up: n1 learns which database holds the bucket's id, which it
	// could not ask a dead member for afterwards (the lone request has the same dependence).
	warm := bc.send([16]byte{0xB1, 3}, []cluster.Op{bc.upsert("batch-warm")})
	if warm.Answers[0].Reply == nil && warm.Answers[0].Error != nil && warm.Answers[0].Error.Code != cluster.CodeNotOwner {
		t.Logf("warm-up answered %s", warm.Answers[0].Error.Code)
	}
	c.Kill("n3")
	var ops []cluster.Op
	var kept []int
	// n1 is the node the frame goes to. A write it forwards to the live n2 is refused in this
	// setup (n1 cannot read the declaration n2 owns once n3 is gone), exactly as the lone write
	// is, so the sample is the documents n1 owns and the ones the dead n3 owns.
	for i, id := range ids {
		if owners[i] == "n1" || owners[i] == "n3" {
			ops, kept = append(ops, bc.upsert(id)), append(kept, i)
		}
	}
	reply := bc.send([16]byte{0xB1, 2}, ops)
	dead, live := 0, 0
	for slot, i := range kept {
		answer := reply.Answers[slot]
		switch owners[i] {
		case "n3":
			dead++
			if answer.Error == nil {
				t.Errorf("%s is owned by the dead n3 and was answered %+v", ids[i], answer.Reply)
				continue
			}
			// The batch must refuse it as the lone request does: the same code.
			if lone := bc.lone(ops[slot]); lone != answer.Error.Code {
				t.Errorf("%s: the batch answered %s and the lone request %s", ids[i], answer.Error.Code, lone)
			}
			if answer.Error.Code != cluster.CodeUnavailable {
				t.Errorf("%s: a write to the dead owner n3 was answered %s (%s), want UNAVAILABLE", ids[i], answer.Error.Code, answer.Error.Message)
			}
		default:
			live++
			if answer.Reply == nil || answer.Reply.WriteOutcome == "" {
				t.Errorf("%s (owner %s) was refused: %+v", ids[i], owners[i], answer.Error)
			}
		}
	}
	if dead == 0 || live == 0 {
		t.Fatalf("the sample had %d dead-owner and %d live-owner operations; it needs both", dead, live)
	}
	if reply.Outcome != "partial" {
		t.Fatalf("outcome %q, want partial", reply.Outcome)
	}
}
