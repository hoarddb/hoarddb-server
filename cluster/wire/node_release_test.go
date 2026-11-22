package wire

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// u67Peeked is the items a peek of field shows, in pop order.
func u67Peeked(t *testing.T, node *Node, field string) []string {
	t.Helper()
	elements, err := node.Peek(context.Background(), PeekRequest{StoreTarget: u62Target(field), Count: 10})
	if err != nil {
		t.Fatal(err)
	}
	var items []string
	for _, element := range elements {
		items = append(items, u62Item(t, element))
	}
	return items
}

func u67Claim(t *testing.T, node *Node, field string, count int) Lease {
	t.Helper()
	lease, err := node.Claim(context.Background(), ClaimRequest{StoreTarget: u62Target(field), Count: count})
	if err != nil || lease.ID == ([16]byte{}) {
		t.Fatalf("Claim = %+v, %v; want a lease", lease, err)
	}
	return lease
}

// TestU67ReleaseReturnsTheMessages: [GUARD Node.Release sends an OpRelease with the lease id].
func TestU67ReleaseReturnsTheMessages(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b")
	lease := u67Claim(t, node, "Queue", 1)
	if got := u67Peeked(t, node, "Queue"); len(got) != 1 || got[0] != "b" {
		t.Fatalf("after the claim the queue shows %v, want [b]", got)
	}
	request := [16]byte{0x67, 1}
	released, err := node.Release(ctx, ReleaseRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID}, WithRequestID(request))
	if err != nil || released.State != store.LeaseReleased || released.Pos.Zero() || released.Deduped {
		t.Fatalf("Release = %+v, %v; want released at a position", released, err)
	}
	if got := u67Peeked(t, node, "Queue"); len(got) != 2 {
		t.Fatalf("after the release the queue shows %v, want both messages back", got)
	}
	again, err := node.Release(ctx, ReleaseRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID}, WithRequestID(request))
	if err != nil || !again.Deduped || again.State != store.LeaseReleased || again.Pos != released.Pos {
		t.Fatalf("the retried release = %+v, %v; want the first answer marked deduped", again, err)
	}
	if got := u67Peeked(t, node, "Queue"); len(got) != 2 {
		t.Fatalf("the retry changed the queue to %v", got)
	}
	// A new request for the same lease is an answer, not an error: it is gone.
	gone, err := node.Release(ctx, ReleaseRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID})
	if err != nil || gone.State == store.LeaseReleased {
		t.Fatalf("a second release of the same lease = %+v, %v; want an unknown or expired answer", gone, err)
	}
	unknown, err := node.Release(ctx, ReleaseRequest{StoreTarget: u62Target("Queue"), Lease: [16]byte{9}})
	if err != nil || unknown.State != store.LeaseUnknown {
		t.Fatalf("release of a lease that never existed = %+v, %v; want unknown", unknown, err)
	}
}

// TestU67ExtendMovesTheDeadline: [GUARD Node.Extend sends LeaseMs and the owner stamps now+length].
func TestU67ExtendMovesTheDeadline(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a")
	lease := u67Claim(t, node, "Queue", 1)
	request := [16]byte{0x67, 2}
	extended, err := node.Extend(ctx, ExtendRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID, LeaseMs: 600_000}, WithRequestID(request))
	if err != nil || extended.State != store.LeaseExtended || extended.Until <= lease.Until+500_000 {
		t.Fatalf("Extend = %+v, %v; want a deadline about 10 minutes out, past the claim's %d", extended, err, lease.Until)
	}
	again, err := node.Extend(ctx, ExtendRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID, LeaseMs: 600_000}, WithRequestID(request))
	if err != nil || !again.Deduped || again.Until != extended.Until || again.State != store.LeaseExtended {
		t.Fatalf("the retried extend = %+v, %v; want the first answer %+v", again, err, extended)
	}
	if err := node.Ack(ctx, AckRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID}); err != nil {
		t.Fatalf("Ack after an extend: %v", err)
	}
}

// u67RawLease writes a hand-built release or extend through the owner write path,
// bypassing every check the Node API makes.
func u67RawLease(node *Node, class byte, fieldID uint16, until int64) (WriteAnswer, error) {
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
	op.Class, op.Fields, op.LeaseID, op.Until = class, []cluster.FieldOp{{FieldID: fieldID}}, [16]byte{1}, until
	return node.send(ctx, call.ref, call.encID, op, opts)
}

// TestU67ExtendOutsideTheBoundsIsRefusedBeforeAPosition: [GUARD extend length check in
// leasePrecondition]. A length the apply refuses would be appended and then fail on every
// replay, fencing the pair.
func TestU67ExtendOutsideTheBoundsIsRefusedBeforeAPosition(t *testing.T) {
	node, _ := u62Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a")
	lease := u67Claim(t, node, "Queue", 1)
	before := u63Position(node)
	for name, length := range map[string]int64{"zero": 0, "negative": -5, "too short": store.MinLeaseMs - 1, "too long": store.MaxLeaseMs + 1, "absolute": 1 << 40} {
		_, err := node.Extend(ctx, ExtendRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID, LeaseMs: length})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "Nothing was changed") || !strings.Contains(err.Error(), "1000") || !strings.Contains(err.Error(), "Box.Queue") {
			t.Errorf("%s: %q, want the field, the range and that nothing changed", name, err)
		}
		// The same length straight to the owner path, which the Node check cannot cover.
		fieldID := u64FieldID(t, node, "Queue")
		_, err = u67RawLease(node, cluster.OpExtend, fieldID, length)
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "Nothing was changed") || !strings.Contains(err.Error(), "ms") {
			t.Errorf("%s at the owner: %q, want the range worded", name, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v on refused extends", before, after)
	}
	if _, err := node.Extend(ctx, ExtendRequest{StoreTarget: u62Target("Queue"), Lease: lease.ID, LeaseMs: store.MinLeaseMs}); err != nil {
		t.Fatalf("the shortest legal extend: %v", err)
	}
	u64StillWritable(t, node)
}

// TestU67ReleaseAndExtendRefuseTheWrongStoreType: [GUARD fieldCall for release and extend].
func TestU67ReleaseAndExtendRefuseTheWrongStoreType(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	before := u63Position(node)
	for field, storeType := range map[string]string{"Ranked": "heapStore", "Table": "hashStore", "Tree": "btreeStore", "Files": "blobStore"} {
		_, err := node.Release(ctx, ReleaseRequest{StoreTarget: u62Target(field), Lease: [16]byte{1}})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "the field is a "+storeType) || !strings.Contains(err.Error(), "fifoStore or lifoStore") {
			t.Errorf("release on %s: %q", field, err)
		}
		_, err = node.Extend(ctx, ExtendRequest{StoreTarget: u62Target(field), Lease: [16]byte{1}, LeaseMs: 5000})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if !strings.Contains(err.Error(), "the field is a "+storeType) {
			t.Errorf("extend on %s: %q", field, err)
		}
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v", before, after)
	}
	u64StillWritable(t, node)
}

// TestU67ReleaseAndExtendRebuildFromTheLog: [GUARD the stamped Until is in the logged op].
// A node whose Badger is lost replays claim, extend and release and reaches the same queue.
func TestU67ReleaseAndExtendRebuildFromTheLog(t *testing.T) {
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	u62Schema(t, node)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b")
	first := u67Claim(t, node, "Queue", 1)
	if _, err := node.Extend(ctx, ExtendRequest{StoreTarget: u62Target("Queue"), Lease: first.ID, LeaseMs: 120_000}); err != nil {
		t.Fatal(err)
	}
	second := u67Claim(t, node, "Queue", 1)
	if _, err := node.Release(ctx, ReleaseRequest{StoreTarget: u62Target("Queue"), Lease: second.ID}); err != nil {
		t.Fatal(err)
	}
	want := u67Peeked(t, node, "Queue")
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	loseStoreForTheReplay(t, dir)
	reopened := u30iOpen(t, dir)
	if got := u67Peeked(t, reopened, "Queue"); strings.Join(got, ",") != strings.Join(want, ",") || len(got) != 1 {
		t.Fatalf("after the rebuild the queue shows %v, want %v", got, want)
	}
	// The extended lease is still the live one: acking it works after the rebuild.
	if err := reopened.Ack(ctx, AckRequest{StoreTarget: u62Target("Queue"), Lease: first.ID}); err != nil {
		t.Fatal(err)
	}
}

// TestU67ReleaseAndExtendResultMustFitTheDedupeEntry: [GUARD result cap in leasePrecondition].
// A result the dedupe entry cannot hold fails the apply after the append.
func TestU67ReleaseAndExtendResultMustFitTheDedupeEntry(t *testing.T) {
	kv := claim34New(t)
	claim34LiveHeader(t, kv)
	applier := NewDocumentApplier(kv, pairRecorder{}).WithFieldEngines(claim34Engines{engine: store.FieldFIFO}).WithResultCap(extendResultBytes - 1)
	release := claim34Lease(cluster.OpRelease, [16]byte{1}, 5)
	extend := claim34Lease(cluster.OpExtend, [16]byte{1}, 5)
	extend.Until = 60_000
	for name, tc := range map[string]struct {
		op   cluster.Op
		want cluster.Code
	}{"release fits": {release, 0}, "extend does not": {extend, cluster.CodeResultTooLarge}} {
		code, err := applier.Precondition(testSlot, testDB, tc.op)
		if err != nil || code != tc.want {
			t.Errorf("%s: Precondition = %v, %v; want %v", name, code, err, tc.want)
		}
	}
	words, said := applier.Explain(cluster.CodeResultTooLarge, testSlot, testDB, extend)
	if !said || !strings.Contains(words, "extend") || !strings.Contains(words, "Nothing was changed") {
		t.Errorf("Explain = %q, %v; want the extend named and that nothing changed", words, said)
	}
}
