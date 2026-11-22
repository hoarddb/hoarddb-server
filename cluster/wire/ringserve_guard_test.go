package wire

import (
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func guardSetName(nameID uint16, value int32) cluster.FieldOp {
	return cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateSet, Value: intValue(value)}
}

// A guard that does not hold is a successful reply: not_applied, the reason, the guarded
// field's name and the current document, with no position and nothing written.
func TestRingReplyForARefusedGuardNamesTheFieldAndHoldsTheCurrentDocument(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	u66Reply(t, c.keyed(env.update(t, "b1", guardSetName(nameID, 1))))

	req := env.update(t, "b1", guardSetName(nameID, 2))
	req.Op.Guard = []cluster.GuardTerm{{FieldID: nameID, Value: intValue(999)}}
	req.Op.Return = cluster.ReturnAfter
	reply := u66Reply(t, c.keyed(req))
	if reply.WriteOutcome != "not_applied" || reply.Reason != "guard" || reply.GuardField != "Name" || reply.Position != nil {
		t.Fatalf("reply %+v, want not_applied for the guard on Name with no position", reply)
	}
	document := bson.Raw(reply.Document)
	if id, err := document.LookupErr("_id_"); err != nil || id.StringValue() != "b1" {
		t.Fatalf("document %v has no _id_ b1 (names, not ids, and _id_ first): %v", document, err)
	}
	if name, err := document.LookupErr("Name"); err != nil || name.Int32() != 1 {
		t.Fatalf("document %v: Name = %v, %v, want the current 1", document, name, err)
	}
	if got := env.name(t, c); got.Int32() != 1 {
		t.Errorf("Name is %d after a refused guard, want 1", got.Int32())
	}
}

func TestAGuardedUpdateAppliesAndReturnsAfterAndBefore(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	u66Reply(t, c.keyed(env.update(t, "b1", guardSetName(nameID, 1))))
	// Name is 1: set it to 2 under a guard on 1 and read it after, then to 3 under a guard
	// on 2 and read it before.
	for _, step := range []struct {
		mode              byte
		held, set, wanted int32
	}{{cluster.ReturnAfter, 1, 2, 2}, {cluster.ReturnBefore, 2, 3, 2}} {
		req := env.update(t, "b1", guardSetName(nameID, step.set))
		req.Op.Guard = []cluster.GuardTerm{{FieldID: nameID, Value: intValue(step.held)}}
		req.Op.Return = step.mode
		reply := u66Reply(t, c.keyed(req))
		if reply.WriteOutcome != "applied" || reply.Position == nil {
			t.Fatalf("mode %d: reply %+v, want applied", step.mode, reply)
		}
		if name, err := bson.Raw(reply.Document).LookupErr("Name"); err != nil || name.Int32() != step.wanted {
			t.Fatalf("mode %d: document Name = %v, %v, want %d", step.mode, name, err, step.wanted)
		}
	}
}

func TestARetryOfAnAppliedGuardedUpdateIsDedupedWithItsDocumentOverTheRing(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	u66Reply(t, c.keyed(env.update(t, "b1", guardSetName(nameID, 1))))
	req := env.update(t, "b1", guardSetName(nameID, 2))
	req.Op.Guard = []cluster.GuardTerm{{FieldID: nameID, Value: intValue(1)}}
	req.Op.Return = cluster.ReturnAfter
	payload, err := EncodeRingKeyed(req)
	if err != nil {
		t.Fatal(err)
	}
	id := u66ID()
	first := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	retry := u66Reply(t, c.send(transport.OpRingKeyed, id, payload)) // the guard no longer holds
	if !retry.Deduped || retry.WriteOutcome != "applied" || *retry.Position != *first.Position {
		t.Fatalf("retry %+v, want the first answer back, deduped", retry)
	}
	if name, err := bson.Raw(retry.Document).LookupErr("Name"); err != nil || name.Int32() != 2 {
		t.Fatalf("the retry's document Name = %v, %v, want the recorded 2", name, err)
	}
}

func TestAGuardOnAStoreFieldOrAnUndeclaredIdIsRefused(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	u66Reply(t, c.keyed(env.update(t, "b1", guardSetName(nameID, 1))))
	for label, id := range map[string]uint16{"a store": env.fieldID(t, "Queue"), "an undeclared id": 99} {
		req := env.update(t, "b1", guardSetName(nameID, 2))
		req.Op.Guard = []cluster.GuardTerm{{FieldID: id, Value: intValue(1)}}
		refusal := u66Refusal(t, c.keyed(req), cluster.CodeFieldType)
		if !strings.Contains(refusal.Message, "claim or ack") {
			t.Errorf("%s: refusal %q does not say what to do instead", label, refusal.Message)
		}
	}
	if got := env.name(t, c); got.Int32() != 1 {
		t.Errorf("Name is %d after refused guards, want 1", got.Int32())
	}
}

func TestAGuardedDeleteOverTheRing(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	u66Reply(t, c.keyed(env.update(t, "b1", guardSetName(nameID, 1))))
	del := env.update(t, "b1")
	del.Op.Class, del.Op.Fields = cluster.OpDelete, nil
	del.Op.Guard = []cluster.GuardTerm{{FieldID: nameID, Value: intValue(999)}}
	if refused := u66Reply(t, c.keyed(del)); refused.WriteOutcome != "not_applied" {
		t.Fatalf("reply %+v, want not_applied", refused)
	}
	if got := env.name(t, c); got.Int32() != 1 {
		t.Fatalf("the document changed after a refused guarded delete: Name is %d", got.Int32())
	}
	del.Op.Guard = []cluster.GuardTerm{{FieldID: nameID, Value: intValue(1)}}
	del.Op.Return = cluster.ReturnBefore
	removed := u66Reply(t, c.keyed(del))
	if removed.WriteOutcome != "applied" {
		t.Fatalf("reply %+v, want applied", removed)
	}
	if name, err := bson.Raw(removed.Document).LookupErr("Name"); err != nil || name.Int32() != 1 {
		t.Fatalf("the delete's document Name = %v, %v, want the 1 it held", name, err)
	}
}

// A missing document is refused by name: the user's bucket and document id, and a fix
// command they can paste, not the owner's hex id, numeric bucket id or a placeholder.
func TestNotFoundOfAGuardedWriteNamesTheBucketAndTheDocument(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	update := env.update(t, "ghost", guardSetName(nameID, 1))
	update.Op.Guard = []cluster.GuardTerm{{FieldID: nameID, Value: intValue(1)}}
	del := env.update(t, "ghost")
	del.Op.Class, del.Op.Fields = cluster.OpDelete, nil
	del.Op.Guard = update.Op.Guard
	for label, req := range map[string]RingKeyedRequest{"update": update, "delete": del} {
		refusal := u66Refusal(t, c.keyed(req), cluster.CodeNotFound)
		for _, want := range []string{`"ghost"`, env.ref.name, env.ref.db} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("%s: %q does not name %s", label, refusal.Message, want)
			}
		}
		for _, banned := range []string{"<bucket>", "6768", "bucket " + strconv.FormatUint(req.Op.BucketID, 10)} {
			if strings.Contains(refusal.Message, banned) {
				t.Errorf("%s: %q still carries %q", label, refusal.Message, banned)
			}
		}
	}
}
