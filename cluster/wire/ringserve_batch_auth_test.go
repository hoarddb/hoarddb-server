package wire

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestARingBatchNeverWidensASessionsGrant: [GUARD authorization is per operation]. A session
// that may write database app and not vault sends one frame with an app write, a vault write
// and an app write. The vault write is the uniform denial, which names neither database, and
// the other two apply.
func TestARingBatchNeverWidensASessionsGrant(t *testing.T) {
	app, vault, gate := u70Start(t)
	c := u70Login(t, app, "wapp", "write@app")
	one := app.doc(t, cluster.OpUpsert, "grant-1", "one")
	two := vault.doc(t, cluster.OpUpsert, "grant-2", "two")
	three := app.doc(t, cluster.OpUpsert, "grant-3", "three")
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(one, two, three)}), 3)
	if reply.Outcome != "partial" {
		t.Fatalf("outcome %q, want partial", reply.Outcome)
	}
	if reply.Answers[0].Reply == nil || reply.Answers[2].Reply == nil {
		t.Fatalf("the operations on app must apply: %+v", reply.Answers)
	}
	denied := reply.Answers[1].Error
	if denied == nil || denied.Code != cluster.CodePermissionDenied {
		t.Fatalf("the vault write = %+v, want PERMISSION_DENIED", reply.Answers[1])
	}
	if want := ringDenied().(*cluster.Error).Message; denied.Message != want {
		t.Fatalf("denial text %q is not the uniform text %q", denied.Message, want)
	}
	if _, err := vault.node.Get(context.Background(), "vault", "Box", "grant-2"); err == nil {
		t.Fatal("the denied operation wrote into vault")
	}
	if _, err := app.node.Get(context.Background(), u30iDB, "Box", "grant-3"); err != nil {
		t.Fatalf("the operation after the denial did not apply: %v", err)
	}
	withDatabase := 0
	for _, ask := range gate.asks() {
		if ask.Database != "" {
			withDatabase++
		}
	}
	if withDatabase != 3 {
		t.Fatalf("the authorizer was asked about a database %d times for 3 operations, want 3 (one per operation)", withDatabase)
	}
}

// TestARingBatchRetriedWithTheSameRequestIDAppliesOnce: operation 3 is refused (its document is
// missing), the frame is sent again under the same request id after the document exists, and
// operations 1 and 2 answer deduped while operation 3 now applies. Three operations leave three
// dedupe entries, one per (request id, index).
func TestARingBatchRetriedWithTheSameRequestIDAppliesOnce(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	update := env.doc(t, cluster.OpUpdate, "retry-3", "late")
	update.Op.IDValue = nil
	update.Op.Fields[0].OpCode = cluster.UpdateSet
	ops := ringBatchOpsOf(env.doc(t, cluster.OpUpsert, "retry-1", "one"), env.doc(t, cluster.OpUpsert, "retry-2", "two"), update)
	id := [16]byte{0xCC, 7}
	first := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ops}), 3)
	if first.Outcome != "partial" || first.Answers[0].Reply == nil || first.Answers[1].Reply == nil || first.Answers[2].Error == nil {
		t.Fatalf("first send: %+v, want 2 applied and the update refused", first)
	}
	u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, "retry-3", "created")))
	second := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ops}), 3)
	if second.Outcome != "ok" {
		t.Fatalf("second send outcome %q: %+v", second.Outcome, second.Answers)
	}
	for i := range 2 {
		replay := second.Answers[i].Reply
		if replay == nil || !replay.Deduped || *replay.Position != *first.Answers[i].Reply.Position {
			t.Fatalf("operation %d on the retry = %+v, want the first send's answer, deduped", i, second.Answers[i])
		}
	}
	if applied := second.Answers[2].Reply; applied == nil || applied.Deduped || applied.WriteOutcome != "applied" {
		t.Fatalf("operation 2 on the retry = %+v, want a fresh apply", second.Answers[2])
	}
	found := 0
	for i := range second.Answers {
		op := ops[i]
		entry, err := env.node.writer.cfg.Dedupe.Lookup(env.ref.slot(op.EncID), env.ref.dbID, env.ref.entry.Decl.BucketID,
			machine.DedupeKey{RequestID: id, OpIndex: uint8(i)})
		if err != nil || entry == nil {
			t.Fatalf("no dedupe entry for (request id, %d): %v", i, err)
		}
		if entry.Key.OpIndex == uint8(i) && entry.Key.RequestID == id {
			found++
		}
	}
	if found != 3 {
		t.Fatalf("3 operations left %d dedupe entries under their own (request id, index), want 3", found)
	}
}

// TestARingBatchRetriedWithADifferentOperationIsRefusedAndAppliesNothing: [GUARD a retried frame
// cannot be answered with somebody else's write]. The same request id with operation 1 replaced by a
// different write to the same document finds operation 1's dedupe entry, whose fingerprint names another operation: that operation is
// INVALID_ARGUMENT and the replacement document is not written.
func TestARingBatchRetriedWithADifferentOperationIsRefusedAndAppliesNothing(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	id := [16]byte{0xCC, 9}
	original := ringBatchOpsOf(env.doc(t, cluster.OpUpsert, "same-0", "zero"), env.doc(t, cluster.OpUpsert, "same-1", "one"))
	first := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: original}), 2)
	if first.Outcome != "ok" {
		t.Fatalf("first send %+v", first)
	}
	changed := ringBatchOpsOf(env.doc(t, cluster.OpUpsert, "same-0", "zero"), env.doc(t, cluster.OpUpsert, "same-1", "replacement"))
	second := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: changed}), 2)
	if second.Answers[0].Reply == nil || !second.Answers[0].Reply.Deduped {
		t.Fatalf("the unchanged operation 0 = %+v, want deduped", second.Answers[0])
	}
	refusal := second.Answers[1].Error
	if refusal == nil || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(refusal.Message, "already named a different operation") {
		t.Fatalf("operation 1 = %+v, want INVALID_ARGUMENT 'already named a different operation'", second.Answers[1])
	}
	got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "same-1", "", 0)))
	if got.Found == nil || !*got.Found || !bytes.Contains(got.Elements[0], []byte("one")) || bytes.Contains(got.Elements[0], []byte("replacement")) {
		t.Fatalf("the replacement was applied or the original lost: %+v", got)
	}
}
