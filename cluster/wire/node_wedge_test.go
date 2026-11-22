package wire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u64Lease writes a hand-built claim, ack, release or extend naming fieldID through the
// owner write path, bypassing every check the Node API makes.
func u64Lease(node *Node, class byte, fieldID uint16) (WriteAnswer, error) {
	ctx := context.Background()
	call, err := node.storeCall(ctx, u62Target("Queue"))
	if err != nil {
		return WriteAnswer{}, err
	}
	opts, err := newWriteOptions(nil)
	if err != nil {
		return WriteAnswer{}, err
	}
	op := call.ref.template()
	op.Class, op.Fields = class, []cluster.FieldOp{{FieldID: fieldID}}
	switch class {
	case cluster.OpClaim:
		op.Count = 1
	case cluster.OpExtend:
		op.LeaseID, op.Until = [16]byte{1}, 1<<40
	default:
		op.LeaseID = [16]byte{1}
	}
	return node.send(ctx, call.ref, call.encID, op, opts)
}

func u64FieldID(t *testing.T, node *Node, field string) uint16 {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := entryNamed(ref.table, field)
	if !ok {
		t.Fatalf("field %q is not declared", field)
	}
	return entry.ID
}

// u64StillWritable proves the pair orders a write and a claim after the refusals.
func u64StillWritable(t *testing.T, node *Node) {
	t.Helper()
	u62Push(t, node, "Queue", "after")
	lease, err := node.Claim(context.Background(), ClaimRequest{StoreTarget: u62Target("Queue"), Count: 1})
	if err != nil || lease.ID == ([16]byte{}) || len(lease.Elements) != 1 {
		t.Fatalf("Claim after the refusals = %+v, %v; want one leased element", lease, err)
	}
}

// TestU64OwnerRefusesLeaseClassesOnANonQueueBeforeAPosition: [GUARD lease engine check in
// leasePrecondition]. Before the fix the record was appended, the apply failed on every
// replay and the (slot, database) pair was fenced for good.
func TestU64OwnerRefusesLeaseClassesOnANonQueueBeforeAPosition(t *testing.T) {
	node, _ := u63Fresh(t)
	classes := map[string]byte{"claim": cluster.OpClaim, "ack": cluster.OpAck, "release": cluster.OpRelease, "extend": cluster.OpExtend}
	fields := map[string]string{"Ranked": "heapStore", "Table": "hashStore", "Tree": "btreeStore", "Files": "blobStore"}
	before := u63Position(node)
	for className, class := range classes {
		for field, storeType := range fields {
			_, err := u64Lease(node, class, u64FieldID(t, node, field))
			u30iCode(t, err, cluster.CodeInvalidArgument)
			if !strings.Contains(err.Error(), storeType) || !strings.Contains(err.Error(), "fifoStore or lifoStore") || !strings.Contains(err.Error(), "Nothing") {
				t.Errorf("%s on %s: %q, want the store type %s, the types that do fit and that nothing changed", className, field, err, storeType)
			}
		}
		// A scalar field id is no store at all.
		_, err := u64Lease(node, class, u64FieldID(t, node, "Name"))
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeFieldType {
			t.Errorf("%s on a scalar: err = %v, want FIELD_TYPE", className, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v on refused records", before, after)
	}
	u64StillWritable(t, node)
}

// TestU64NodeClaimAndAckRefuseTheWrongStoreType: [GUARD fieldCall for claim and ack]. The
// Node API refuses with the store type before it builds a record.
func TestU64NodeClaimAndAckRefuseTheWrongStoreType(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	before := u63Position(node)
	for field, storeType := range map[string]string{"Ranked": "heapStore", "Table": "hashStore", "Tree": "btreeStore", "Files": "blobStore"} {
		_, err := node.Claim(ctx, ClaimRequest{StoreTarget: u62Target(field), Count: 1})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "the field is a "+storeType) || !strings.Contains(err.Error(), "fifoStore or lifoStore") {
			t.Errorf("claim on %s: %q", field, err)
		}
		err = node.Ack(ctx, AckRequest{StoreTarget: u62Target(field), Lease: [16]byte{1}})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "the field is a "+storeType) {
			t.Errorf("ack on %s: %q", field, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v", before, after)
	}
	u64StillWritable(t, node)
}

// TestU64StoreVerbOnATombstoneSaysSo: [GUARD Explain reads the target state]. A store verb
// or lease on a deleted document gets the tombstone sentence, not the verb-table one.
func TestU64StoreVerbOnATombstoneSaysSo(t *testing.T) {
	node, _ := u63Fresh(t)
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent("b1")
	if err != nil {
		t.Fatal(err)
	}
	doc := DocumentKey{Ref: storage.BucketRef{Slot: ref.slot(encID), DatabaseID: ref.dbID, BucketID: ref.entry.Decl.BucketID}, EncID: encID}
	u35Commit(t, node.kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		return ApplyDelete(snap, batch, doc, cluster.Position{Epoch: 1, Seq: 9999})
	})
	pushArg, err := encodeElementArg(u63Element("x"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = u63Raw(node, "Queue", cluster.VerbPush, pushArg)
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "is deleted") || strings.Contains(err.Error(), "does not fit the field") {
		t.Errorf("push on a tombstone: %q, want the tombstone sentence", err)
	}
	_, err = u64Lease(node, cluster.OpClaim, u64FieldID(t, node, "Queue"))
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "is deleted") {
		t.Errorf("claim on a tombstone: %q, want the tombstone sentence", err)
	}
}

// TestU64PushRefusesPriorityAndEntryIDOnAQueue: [GUARD push on fifo/lifo refuses heap
// options].
func TestU64PushRefusesPriorityAndEntryIDOnAQueue(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	before := u63Position(node)
	for _, field := range []string{"Queue", "Stack"} {
		for name, req := range map[string]PushRequest{
			"priority": {StoreTarget: u62Target(field), Element: u63Item("x"), Priority: 5},
			"entry id": {StoreTarget: u62Target(field), Element: u63Item("x"), EntryID: "e"},
		} {
			err := node.Push(ctx, req)
			u30iCode(t, err, cluster.CodeInvalidArgument)
			if !strings.Contains(err.Error(), "heapStore") || !strings.Contains(err.Error(), "Nothing") {
				t.Errorf("%s on %s: %q, want the heapStore pointer and that nothing was written", name, field, err)
			}
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v", before, after)
	}
	u62Push(t, node, "Queue", "plain")
}

// TestU64SendSurfacesAnApplyFailure: [GUARD send returns ApplyFailure]. A write that is
// durable but whose apply failed must not read as a success with a zero result.
func TestU64SendSurfacesAnApplyFailure(t *testing.T) {
	failure := errors.New("disk on fire")
	ref := bucketRef{db: "app", name: "Box"}
	err := applyFailureError(ref, WriteAnswer{Pos: cluster.Position{Epoch: 1, Seq: 7}, ApplyFailure: failure})
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want it to wrap the apply failure", err)
	}
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable || !strings.Contains(err.Error(), "same request id") {
		t.Fatalf("err = %v, want UNAVAILABLE naming the retry", err)
	}
	if applyFailureError(ref, WriteAnswer{}) != nil {
		t.Fatal("an answer without a failure produced an error")
	}
}

// u64FailingEngines is a FieldEngineSource whose declaration lookup fails with err.
type u64FailingEngines struct{ err error }

func (f u64FailingEngines) FieldEngine(uint64, uint64, uint16) (store.FieldEngine, error) {
	return 0, f.err
}

// TestU64UndeclaredFieldInThePreconditionIsAPlainRefusal: [GUARD errUndeclaredField to
// FIELD_TYPE]. A field dropped between admission and the precondition is the caller's
// refusal, not a poisoned log; a failed declaration read stays an error.
func TestU64UndeclaredFieldInThePreconditionIsAPlainRefusal(t *testing.T) {
	node, _ := u63Fresh(t)
	op := cluster.Op{Class: cluster.OpStoreVerb, BucketID: 1, EncID: []byte("b1"), Fields: []cluster.FieldOp{{FieldID: 9, Verb: cluster.VerbPop}}}
	undeclared := NewDocumentApplier(node.kv, nil).WithFieldEngines(u64FailingEngines{err: errors.Join(errUndeclaredField, errors.New("no field 9"))})
	for _, class := range []byte{cluster.OpStoreVerb, cluster.OpClaim} {
		op.Class = class
		code, err := undeclared.Precondition(1, 1, op)
		if err != nil || code != cluster.CodeFieldType {
			t.Fatalf("class 0x%02x: Precondition = %v, %v; want FIELD_TYPE and no error", class, code, err)
		}
		message, said := undeclared.Explain(code, 1, 1, op)
		if !said || strings.Contains(message, "poisoned") || !strings.Contains(message, "Nothing was written") {
			t.Fatalf("class 0x%02x: Explain = %q, %v", class, message, said)
		}
	}
	broken := NewDocumentApplier(node.kv, nil).WithFieldEngines(u64FailingEngines{err: errors.New("disk read failed")})
	op.Class = cluster.OpStoreVerb
	if code, err := broken.Precondition(1, 1, op); err == nil || code != 0 {
		t.Fatalf("a failed declaration read gave %v, %v; want the error", code, err)
	}
}
