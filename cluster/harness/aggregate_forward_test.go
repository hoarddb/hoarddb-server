package harness_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestAnAggregateThroughANonOwnerAnswersTheOwnersAnswer reads sum, count by and describe of documents with different
// owners through every node: the answers through a non-owner (forwarded) equal the owner's, a by form pages with a
// cursor, and an undeclared aggregate is refused by name.
func TestAnAggregateThroughANonOwnerAnswersTheOwnersAnswer(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	decls := []wire.AggregateDecl{{Store: "Queue", Kind: "sum", Field: "Amount"}, {Store: "Queue", Kind: "count by", By: "Cur"}}
	if _, err := leader.CreateBucketWithAggregates(proofCtx(t), proofDB, "agg",
		[]wire.Field{{Name: "name", Type: "string"}, {Name: "Queue", Type: "fifoStore"}}, decls); err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	target := func(id string) wire.StoreTarget {
		return wire.StoreTarget{Database: proofDB, Bucket: "agg", ID: id, Field: "Queue"}
	}
	nonOwnerReads := 0
	for i := 0; i < 2; i++ { // two documents with different owners; the VM harness is slow to keep a majority
		id := fmt.Sprintf("a-%d", i)
		owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), proofDB, "agg", id)
		if err != nil {
			t.Fatal(err)
		}
		node := c.Node(owner)
		for attempt := 0; ; attempt++ {
			_, err := node.Insert(proofCtx(t), proofDB, "agg", bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: id}})
			if err == nil {
				break
			}
			if attempt > 20 {
				t.Fatal(err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		for j, cur := range []string{"USD", "EUR", "USD"} {
			var err error
			for attempt := 0; attempt < 20; attempt++ {
				if err = node.Push(proofCtx(t), wire.PushRequest{StoreTarget: target(id), Element: bson.D{{Key: "Amount", Value: int64(10 * (j + 1))}, {Key: "Cur", Value: cur}}}); err == nil {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, read := range []wire.RingAggregateRead{{Kind: 1, Field: "Amount"}, {Kind: 3, By: "Cur"}, {Kind: 0}} {
			want, err := readAggregate(t, node, target(id), read, 0)
			if err != nil {
				t.Fatalf("%s at owner %s: %v", id, owner, err)
			}
			for _, through := range c.IDs() {
				if through == owner {
					continue
				}
				got, err := readAggregate(t, c.Node(through), target(id), read, 0)
				if err != nil || !bytes.Equal(got.Document, want.Document) || got.More != want.More {
					t.Errorf("%s kind %d through %s (owner %s): %s %v, want %s", id, read.Kind, through, owner, bson.Raw(got.Document), err, bson.Raw(want.Document))
				}
				nonOwnerReads++
			}
			t.Logf("%s kind %d owner %s: %s", id, read.Kind, owner, bson.Raw(want.Document))
		}
		// limit 1 paging through a non-owner
		for _, through := range c.IDs() {
			if through == owner {
				continue
			}
			got, err := readAggregate(t, c.Node(through), target(id), wire.RingAggregateRead{Kind: 3, By: "Cur"}, 1)
			if err != nil || !got.More || len(got.Cursor) == 0 {
				t.Errorf("paged through %s: %+v %v", through, got, err)
			}
			_, err = readAggregate(t, c.Node(through), target(id), wire.RingAggregateRead{Kind: 1, Field: "Nope"}, 0)
			if err == nil {
				t.Errorf("an undeclared aggregate through %s was answered", through)
			} else if i == 0 {
				t.Logf("undeclared through a non-owner: %v", err)
			}
			break
		}
	}
	t.Logf("%d non-owner reads compared", nonOwnerReads)
}

// readAggregate reads an aggregate and retries, for at most about 20 seconds, only a refusal that says the node
// cannot serve yet (no quorum seen, a lapsed lease, unavailable): a young cluster reaches those a moment after
// it starts. Any other refusal, and any answer, is returned as it came, so a wrong answer is never retried away.
func readAggregate(t *testing.T, node *wire.Node, target wire.StoreTarget, read wire.RingAggregateRead, limit int) (wire.AggregateAnswer, error) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		answer, err := node.AggregateRead(proofCtx(t), target, read, limit, nil)
		var refusal *cluster.Error
		notYet := errors.As(err, &refusal) && (refusal.Code == cluster.CodeNoQuorum || refusal.Code == cluster.CodeLeaseLapsed || refusal.Code == cluster.CodeUnavailable)
		if !notYet || attempt >= 100 {
			return answer, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}
