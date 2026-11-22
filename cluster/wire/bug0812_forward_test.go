package wire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0812, forwarded writes: a node that does not own a pair forwards the write, and the owner's
// reply codec (EncodeForwardWriteAnswer) has no field for an ApplyFailure. serveForwardWrite must
// therefore refuse, through the reply's existing refusal form, an answer whose apply failed;
// otherwise the forwarding node decodes a clean answer and tells its client the write applied.
// cluster/wire has no two-node harness, so these tests drive the owner's serveForwardWrite and the
// forwarding side's decode (the tail of peerLinks.ForwardWrite) directly.

// bug0812FailFirstUpdate fails the apply of the first update after its append, as a disk error
// would; the replay of the pair then succeeds, the case that answered applied.
type bug0812FailFirstUpdate struct {
	*DocumentApplier
	failed atomic.Bool
}

func (a *bug0812FailFirstUpdate) FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	if rec.Op.Class == cluster.OpUpdate && a.failed.CompareAndSwap(false, true) {
		return nil, errors.New("disk on fire")
	}
	return a.DocumentApplier.FillWithResult(batch, rec)
}

// bug0812Forwarded is one FORWARD_WRITE frame for op on bucket User of database app, as the
// forwarding node would send it, and the owner's reply to it decoded the way the forwarder does.
func bug0812Forwarded(t *testing.T, node *Node, encID []byte, op cluster.Op) (WriteAnswer, error) {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	op.EncID = encID
	slot := storage.SlotOf(storage.PlacementKey(u30iDB, "User", encID))
	body, err := EncodeForwardWriteBody(WriteRequest{Op: op, RequestID: [16]byte{0x08, 0x12, 2}, Hop: 1})
	if err != nil {
		t.Fatalf("encoding the forwarded write: %v", err)
	}
	env := transport.Envelope{Slot: slot, DBID: ref.dbID, Epoch: uint64(node.topology.Committed())}
	reply, err := node.serveForwardWrite(context.Background(), env, body)
	if err != nil {
		t.Fatalf("serveForwardWrite returned a transport error instead of a reply: %v", err)
	}
	answer, err := DecodeForwardWriteReply(reply.Body)
	return answer, clientFacing(err, "owner")
}

func bug0812RequireUnavailable(t *testing.T, answer WriteAnswer, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("the owner's apply failed and the forwarder was told %+v with no error", answer)
	}
	message := u39Code(t, err, cluster.CodeUnavailable)
	if !strings.Contains(message, "durable") || !strings.Contains(message, "Retry") {
		t.Errorf("the refusal %q must say the write is durable and what to do", message)
	}
}

// TestAForwardedInsertWhoseApplyFailedIsRefusedToTheForwarder is (a) and (b): the owner, whose
// apply of the first insert fails and whose replay then succeeds, answers the forward with a
// refusal, and the forwarding side's decode returns it as an error. [BREAK: drop the ApplyFailure
// case of serveForwardWrite]
func TestAForwardedInsertWhoseApplyFailedIsRefusedToTheForwarder(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	defer release()
	s0Schema(t, node)
	documents, ok := node.writer.cfg.Applier.(*DocumentApplier)
	if !ok {
		t.Fatalf("the node's applier is %T, want *DocumentApplier", node.writer.cfg.Applier)
	}
	node.writer.cfg.Applier = &bug0809FailFirstInsert{DocumentApplier: documents}
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	id, fields, err := ref.documentOp(bson.D{{Key: "_id_", Value: "fwd-1"}, {Key: "Name", Value: "Ada"}})
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	op := ref.template()
	op.Class, op.Fields, op.IDValue = cluster.OpInsert, fields, stringValue(id)

	answer, err := bug0812Forwarded(t, node, encID, op)
	bug0812RequireUnavailable(t, answer, err)

	retry, err := bug0812Forwarded(t, node, encID, op)
	if err != nil || !retry.Deduped {
		t.Errorf("a retry with the same request id answered %+v, err %v; want deduped (the replay stored the record and its dedupe entry)", retry, err)
	}
}

// TestAForwardedUpdateWithReturnWhoseApplyFailedIsNotAnEmptyDocument is (c): an apply failure
// leaves the ordered result nil, so a forwarded update with Return used to come back applied with
// no document.
func TestAForwardedUpdateWithReturnWhoseApplyFailedIsNotAnEmptyDocument(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	defer release()
	s0Schema(t, node)
	if _, err := node.Insert(context.Background(), u30iDB, "User", bson.D{{Key: "_id_", Value: "fwd-2"}, {Key: "Name", Value: "Ada"}}); err != nil {
		t.Fatalf("the first insert: %v", err)
	}
	documents, ok := node.writer.cfg.Applier.(*DocumentApplier)
	if !ok {
		t.Fatalf("the node's applier is %T, want *DocumentApplier", node.writer.cfg.Applier)
	}
	node.writer.cfg.Applier = &bug0812FailFirstUpdate{DocumentApplier: documents}
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := ref.updateOps(UpdateRequest{Set: bson.D{{Key: "Name", Value: "Grace"}}})
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("fwd-2")
	if err != nil {
		t.Fatal(err)
	}
	op := ref.template()
	op.Class, op.Fields, op.Return = cluster.OpUpdate, fields, cluster.ReturnAfter

	answer, err := bug0812Forwarded(t, node, encID, op)
	t.Logf("the forwarded update with Return answered %+v, err %v", answer, err)
	bug0812RequireUnavailable(t, answer, err)
}
