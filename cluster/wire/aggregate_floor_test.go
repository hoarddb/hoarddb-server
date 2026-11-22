package wire

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

const floorLeases = 10

// floorBy is the by value of the i-th element: near the 1024-byte cap and distinct per element, so every element owns
// a key in every by aggregate.
func floorBy(i int) string { return fmt.Sprintf("%04d", i) + strings.Repeat("x", 1010) }

// floorCountKeys counts the elements whose count-by key (aggregate id 1) still exists.
func floorCountKeys(t *testing.T, node *Node, elements int) int {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, _ := ref.storeField("Q")
	encID, _ := store.EncodeComponent("l1")
	prefix := store.StorePrefix(ref.slot(encID), ref.dbID, ref.entry.Decl.BucketID, encID, fieldID)
	snapshot := node.kv.NewSnapshot()
	defer snapshot.Discard()
	left := 0
	for i := 0; i < elements; i++ {
		by, _ := store.EncodeComponent(floorBy(i))
		if _, err := snapshot.Get(store.AggregateKey(prefix, 1, by)); err == nil {
			left++
		}
	}
	return left
}

// TestAFullReturnStepAtTheMemtableFloorDeadLettersWithoutFencing: at the 32 MiB memtable floor, ten expired 100-entry
// leases each on its last delivery, on a store with eight by aggregates and by values near the cap. Dead-lettering
// all of them in one apply would write about 8.9 MB of aggregate keys, more than a batch holds, so the return step
// must charge those keys to its budget (LeaveBytes, wired by fillLease), dead-letter some leases per apply, leave
// the rest leased and drain with no fence and no entry left summed. Without the charge the first claim after
// expiry fails after the append and fences the pair.
func TestAFullReturnStepAtTheMemtableFloorDeadLettersWithoutFencing(t *testing.T) {
	node, err := OpenNode(context.Background(), NodeConfig{Dir: t.TempDir(), MemTableMB: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	decls := []AggregateDecl{{Store: "Q", Kind: "count by", By: "C"}}
	for i := 1; i <= 7; i++ {
		decls = append(decls, AggregateDecl{Store: "Q", Kind: "sum by", Field: fmt.Sprint("A", i), By: "C"})
	}
	if _, err := node.CreateBucketWithAggregates(ctx, u30iDB, "Ledger", []Field{{Name: "Name", Type: "string"}, {Name: "Q", Type: "fifoStore"}}, decls); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Ledger", bson.D{{Key: "_id_", Value: "l1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	target := StoreTarget{Database: u30iDB, Bucket: "Ledger", ID: "l1", Field: "Q"}
	elements := floorLeases * 100
	for i := 0; i < elements; i++ {
		element := bson.D{}
		for j := 1; j <= 7; j++ {
			element = append(element, bson.E{Key: fmt.Sprint("A", j), Value: int64(1)})
		}
		element = append(element, bson.E{Key: "C", Value: floorBy(i)})
		if err := node.Push(ctx, PushRequest{StoreTarget: target, Element: element}); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	notFenced := func(step string, err error) {
		t.Helper()
		if err != nil {
			t.Logf("%s: %.300v", step, err)
		}
		if node.pairs.Snapshot(node.slotOf(u30iDB, "Ledger", "l1"), 1).Flags&flagReplayNeeded != 0 {
			t.Fatalf("%s fenced the pair (err %v)", step, err)
		}
	}
	var leases []Lease
	for round := 0; round < store.MaxDeliveries; round++ {
		leases = leases[:0]
		for k := 0; k < floorLeases; k++ {
			lease, err := node.Claim(ctx, ClaimRequest{StoreTarget: target, Count: 100})
			notFenced("claim", err)
			if len(lease.Elements) != 100 {
				t.Fatalf("round %d lease %d holds %d elements, want 100", round, k, len(lease.Elements))
			}
			leases = append(leases, lease)
		}
		if round == store.MaxDeliveries-1 {
			break
		}
		for _, lease := range leases {
			_, err := node.Release(ctx, ReleaseRequest{StoreTarget: target, Lease: lease.ID})
			notFenced("release", err)
		}
	}
	for _, lease := range leases {
		_, err := node.Extend(ctx, ExtendRequest{StoreTarget: target, Lease: lease.ID, LeaseMs: 1000})
		notFenced("extend", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if left := floorCountKeys(t, node, elements); left != elements {
		t.Fatalf("%d of %d entries summed before the leases expired", left, elements)
	}
	first := -1
	for apply := 0; apply < 8; apply++ {
		_, err := node.Claim(ctx, ClaimRequest{StoreTarget: target, Count: 1})
		notFenced(fmt.Sprintf("claim after expiry %d", apply), err)
		left := floorCountKeys(t, node, elements)
		if first < 0 {
			first = left
		}
		if left == 0 {
			break
		}
	}
	if first <= 0 || first >= elements {
		t.Fatalf("the first apply after expiry left %d of %d entries summed: it must dead-letter some leases and stop at its budget, not all or none", first, elements)
	}
	if left := floorCountKeys(t, node, elements); left != 0 {
		t.Fatalf("%d entries still summed after the expired leases drained", left)
	}
}
