package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/store"
)

// expiringLedger is a node whose fifoStore Fifo expires its elements at "ts" and keeps a plain
// sum of Amount: an element that expires must leave the sum as a pop or an ack does.
func expiringLedger(t *testing.T) *Node {
	t.Helper()
	node := u30iOpen(t, t.TempDir())
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	fields := []Field{{Name: "Name", Type: "string"}, {Name: "Fifo", Type: "fifoStore"}}
	spec := ExpireSpec{StorePaths: map[string]string{"Fifo": "ts"}}
	if _, err := node.CreateBucketDeclared(ctx, u30iDB, "Ledger", spec, fields, []AggregateDecl{{Store: "Fifo", Kind: "sum", Field: "Amount"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Ledger", bson.D{{Key: "_id_", Value: "l1"}, {Key: "Name", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	return node
}

func expiringElement(amount int64, at time.Time) bson.D {
	return bson.D{{Key: "Amount", Value: amount}, {Key: "ts", Value: primitive.NewDateTimeFromTime(at)}}
}

// pushShortAndLong pushes an element that expires in 300 ms (Amount 10) and one that lives
// an hour (Amount 5), then waits until the first is past its time.
func pushShortAndLong(t *testing.T, node *Node) {
	t.Helper()
	ctx := context.Background()
	for _, element := range []bson.D{expiringElement(10, time.Now().Add(300*time.Millisecond)), expiringElement(5, time.Now().Add(time.Hour))} {
		if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Fifo"), Element: element}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(600 * time.Millisecond)
}

func TestAPopThatSkipsAnExpiredElementSubtractsItFromTheSum(t *testing.T) {
	node := expiringLedger(t)
	pushShortAndLong(t, node)
	popped, err := node.Pop(context.Background(), PopRequest{StoreTarget: aggTarget("Fifo")})
	if err != nil || !popped.Found || bson.Raw(popped.Element).Lookup("Amount").Int64() != 5 {
		t.Fatalf("pop: %+v, %v, want the live element (Amount 5)", popped, err)
	}
	if sum := aggPlainSum(t, node, "Fifo", aggID(t, node, "Fifo", 1, "")); sum != (store.AggNumber{}) {
		t.Fatalf("sum after the pop = %+v, want 0: the expired element it stepped over still counts", sum)
	}
}

func TestAClaimThatRemovesAnExpiredElementSubtractsItFromTheSum(t *testing.T) {
	node := expiringLedger(t)
	pushShortAndLong(t, node)
	lease, err := node.Claim(context.Background(), ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 1})
	if err != nil || len(lease.Elements) != 1 {
		t.Fatalf("claim: %d elements, %v, want the live one", len(lease.Elements), err)
	}
	if sum := aggPlainSum(t, node, "Fifo", aggID(t, node, "Fifo", 1, "")); sum.Whole != 5 {
		t.Fatalf("sum after the claim = %+v, want 5 (the leased element stays summed, the expired one is gone)", sum)
	}
}

func TestTheExpirySweepSubtractsAnExpiredElementFromTheSum(t *testing.T) {
	node := expiringLedger(t)
	pushShortAndLong(t, node)
	if _, err := node.expiry.Pass(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if sum := aggPlainSum(t, node, "Fifo", aggID(t, node, "Fifo", 1, "")); sum.Whole != 5 {
		t.Fatalf("sum after the sweep = %+v, want 5: the element the sweep removed still counts", sum)
	}
}
