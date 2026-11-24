package wire

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
)

// heldSender answers every write Held, as the Writer does for a pair in a hold (a restart
// barrier, a handoff barrier, a boot replay, an epoch the Handoff has not applied yet).
type heldSender struct{}

func (heldSender) Write(context.Context, WriteRequest) (WriteAnswer, error) {
	return WriteAnswer{Held: true, HoldCause: "the test holds every pair"}, nil
}

// ringInsertOp is the insert a ring client sends for document id with Name name.
func ringInsertOp(t *testing.T, ref bucketRef, id, name string) cluster.Op {
	t.Helper()
	_, fields, err := ref.documentOp(bson.D{{Key: "Name", Value: name}})
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	op := ref.template()
	op.Class, op.EncID, op.IDValue, op.Fields = cluster.OpInsert, encID, stringValue(id), fields
	return op
}

// requireNotAcknowledged fails when a ring insert that took no position was answered as a
// write: an acknowledgement says the document is stored, and a held insert stored nothing.
func requireNotAcknowledged(t *testing.T, reply RingReply, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("a held insert was acknowledged: outcome %q, copies %d, position %v; want an UNAVAILABLE refusal, nothing was written",
			reply.WriteOutcome, derefCopies(reply.CopiesConfirmed), reply.Position)
	}
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		t.Fatalf("a held insert answered %v, want an UNAVAILABLE refusal", err)
	}
}

func derefCopies(copies *int32) int32 {
	if copies == nil {
		return -1
	}
	return *copies
}

// TestARingInsertThatIsHeldIsNotAcknowledged: the owner's Writer answers Held for a pair in a
// hold; the ring handler must refuse the insert as Node.Insert does, not answer it applied at
// position 0:0 with no copy. [BREAK: ringServer.writeDocument ignores inserted.Held]
func TestARingInsertThatIsHeldIsNotAcknowledged(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	ref, err := node.bucket(ctx, u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	node.coord = NewCoordinator(CoordinatorConfig{Sender: heldSender{}})
	server := &ringServer{node: node, authz: u70AllowAll{}}
	reply, err := server.write(ctx, ref, [16]byte{7}, RingKeyedRequest{Op: ringInsertOp(t, ref, "held-1", "one")}, ringBatchPos{})
	requireNotAcknowledged(t, reply, err)
}

// TestARingInsertDuringTheRestartBarrierIsNotAcknowledgedAndLost: a node restarts and holds its
// pairs for a few seconds (the restart barrier, bug0760Retry). A ring insert that arrives in
// that window is either refused, so the client retries, or stored. Before the fix it was
// answered applied, copies 0, position 0:0, and the document never existed: an acknowledged
// write lost on a plain restart, with no join and no membership change.
// [BREAK: ringServer.writeDocument ignores inserted.Held]
func TestARingInsertDuringTheRestartBarrierIsNotAcknowledgedAndLost(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	bug0760Box(t, node, u30iDB)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := s0OpenClusterNode(t, dir, nil)
	ctx := context.Background()
	var ref bucketRef
	bug0760Retry(t, "resolving Box after the restart", func() error {
		var err error
		ref, err = reopened.bucket(ctx, u30iDB, "Box")
		return err
	})
	server := &ringServer{node: reopened, authz: u70AllowAll{}}
	// The ids share b1's pair, so every insert meets the same barrier.
	ids := sameSlotIDs(t, u30iDB, "Box", "b1", 5)
	var acknowledged []string
	for i, id := range ids {
		reply, err := server.write(ctx, ref, [16]byte{9, byte(i)}, RingKeyedRequest{Op: ringInsertOp(t, ref, id, "r")}, ringBatchPos{})
		if err == nil {
			t.Logf("insert %s: outcome %q copies %d position %v", id, reply.WriteOutcome, derefCopies(reply.CopiesConfirmed), reply.Position)
			acknowledged = append(acknowledged, id)
			continue
		}
		t.Logf("insert %s refused: %v", id, err)
	}
	// Wait for the barrier to end, then every acknowledged insert must be readable.
	bug0760Retry(t, "a write after the barrier", func() error {
		_, err := reopened.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: "after-barrier"}, {Key: "Name", Value: "x"}})
		return err
	})
	for _, id := range acknowledged {
		if _, err := reopened.Get(ctx, u30iDB, "Box", id); err != nil {
			t.Errorf("insert %s was acknowledged during the restart barrier and is not readable after it: %v", id, err)
		}
	}
}

// sameSlotIDs returns count ids other than anchor whose documents land in anchor's slot.
func sameSlotIDs(t *testing.T, db, bucket, anchor string, count int) []string {
	t.Helper()
	want, err := PlacementOf(db, bucket, anchor)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; len(ids) < count; i++ {
		if i > 50_000_000 {
			t.Fatalf("found %d ids in slot %d, want %d", len(ids), want, count)
		}
		id := fmt.Sprintf("held-%d", i)
		slot, err := PlacementOf(db, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		if slot == want {
			ids = append(ids, id)
		}
	}
	return ids
}

// degradedSender answers every write durable on the owner alone: the concern was missed, as
// when the pair's other copy is down (Writer.conclude, machine.Answer row B).
type degradedSender struct{}

func (degradedSender) Write(context.Context, WriteRequest) (WriteAnswer, error) {
	return WriteAnswer{Outcome: machine.OutcomeAppliedDegraded, CopiesConfirmed: 1, Pos: cluster.Position{Epoch: 4, Seq: 1}}, nil
}

// TestARingInsertAnswersTheOutcomeTheOwnerDecided: an insert the owner answers
// applied_degraded with one copy must reach the client as applied_degraded, as every other
// write class does through ringWriteReply. Answered "applied", it tells the client its concern
// was met when one copy holds the write. [BREAK: writeDocument hard-codes outcomeApplied]
func TestARingInsertAnswersTheOutcomeTheOwnerDecided(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	ref, err := node.bucket(ctx, u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	node.coord = NewCoordinator(CoordinatorConfig{Sender: degradedSender{}})
	server := &ringServer{node: node, authz: u70AllowAll{}}
	reply, err := server.write(ctx, ref, [16]byte{8}, RingKeyedRequest{Op: ringInsertOp(t, ref, "degraded-1", "one")}, ringBatchPos{})
	if err != nil {
		t.Fatal(err)
	}
	if reply.WriteOutcome != machine.OutcomeAppliedDegraded.String() || derefCopies(reply.CopiesConfirmed) != 1 {
		t.Fatalf("an insert the owner answered applied_degraded/1 reached the client as %q/%d", reply.WriteOutcome, derefCopies(reply.CopiesConfirmed))
	}
}

// TestARingBatchInsertIsHeldOrDegradedAsTheOwnerSaid: RING_BATCH runs each insert through the
// same handler, so a held insert is refused UNAVAILABLE in its answer slot and a degraded one
// carries applied_degraded. [BREAK: writeDocument ignores inserted.Held or hard-codes outcomeApplied]
func TestARingBatchInsertIsHeldOrDegradedAsTheOwnerSaid(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	ref, err := node.bucket(ctx, u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	server := &ringServer{node: node, authz: u70AllowAll{}, resultBudget: maxRingBatchBudget, elementCeiling: maxResultBytes + ringReplyEnvelopeBytes}

	node.coord = NewCoordinator(CoordinatorConfig{Sender: heldSender{}})
	held := server.serveBatch(ctx, [16]byte{10}, RingBatchRequest{Ops: []cluster.Op{ringInsertOp(t, ref, "batch-held", "one")}})
	if len(held.Answers) != 1 || held.Answers[0].Error == nil || held.Answers[0].Error.Code != cluster.CodeUnavailable {
		t.Fatalf("a held insert in a RING_BATCH answered %+v, want an UNAVAILABLE refusal in its slot", held.Answers)
	}

	node.coord = NewCoordinator(CoordinatorConfig{Sender: degradedSender{}})
	degraded := server.serveBatch(ctx, [16]byte{11}, RingBatchRequest{Ops: []cluster.Op{ringInsertOp(t, ref, "batch-degraded", "one")}})
	if len(degraded.Answers) != 1 || degraded.Answers[0].Reply == nil {
		t.Fatalf("a degraded insert in a RING_BATCH answered %+v, want a reply", degraded.Answers)
	}
	if got := degraded.Answers[0].Reply.WriteOutcome; got != machine.OutcomeAppliedDegraded.String() {
		t.Fatalf("a degraded insert in a RING_BATCH answered outcome %q, want applied_degraded", got)
	}
}
