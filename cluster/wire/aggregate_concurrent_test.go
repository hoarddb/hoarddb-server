package wire

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// aggConcurrentWrites runs the two writes of one round at once and returns their errors.
func aggConcurrentWrites(writes [2]func() error) [2]error {
	var errs [2]error
	var wait sync.WaitGroup
	for i := range writes {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs[i] = writes[i]()
		}()
	}
	wait.Wait()
	return errs
}

// aggWrite is a write of one element whose Amount is amount into the Ledger document id: two writes of
// 2^61 + 1 into one store add up past the 2^62 bound an aggregate keeps, one alone does not.
func aggWrite(node *Node, verb, id, key string, amount int64) func() error {
	target := StoreTarget{Database: u30iDB, Bucket: "Ledger", ID: id, Field: "Table"}
	return func() error {
		ctx := context.Background()
		switch verb {
		case "inc":
			_, err := node.Inc(ctx, IncRequest{StoreTarget: target, Key: key, Increments: bson.D{{Key: "Amount", Value: amount}}})
			return err
		case "put":
			return node.Put(ctx, PutRequest{StoreTarget: target, Key: key, Element: expense(amount, "USD")})
		default:
			target.Field = "Stack"
			return node.Push(ctx, PushRequest{StoreTarget: target, Element: expense(amount, "USD")})
		}
	}
}

// aggBigAmount is 2^61 + 1: two of them in one sum pass 2^62, one does not.
const aggBigAmount = int64(1<<61 + 1)

// TestConcurrentWritesCannotOverflowAnAggregateAtApply: [GUARD two pipelined writes that each pass the
// dry run alone must not overflow together at apply, after the position is appended, where the failure
// repeats on every replay and fences the pair]. An aggregate is the sum of ONE document's store, so
// two documents never share a sum and both writes apply; two writes into one document's store
// conflict in the pipeline, drain, and the second is refused at its dry run. Run for inc, put and push.
func TestConcurrentWritesCannotOverflowAnAggregateAtApply(t *testing.T) {
	for _, verb := range []string{"inc", "put", "push"} {
		t.Run(verb, func(t *testing.T) {
			node := aggRig(t)
			ctx := context.Background()
			const rounds = 25
			for round := range rounds {
				one, two, solo := fmt.Sprintf("a%d", round), fmt.Sprintf("b%d", round), fmt.Sprintf("c%d", round)
				for _, id := range []string{one, two, solo} {
					if _, err := node.Insert(ctx, u30iDB, "Ledger", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}}); err != nil {
						t.Fatal(err)
					}
				}
				// Different documents: separate stores, separate sums, both apply.
				errs := aggConcurrentWrites([2]func() error{aggWrite(node, verb, one, "k1", aggBigAmount), aggWrite(node, verb, two, "k1", aggBigAmount)})
				if errs[0] != nil || errs[1] != nil {
					t.Fatalf("round %d: writes to two documents failed: %v, %v", round, errs[0], errs[1])
				}
				// One fresh document, two keys (a push has no key): exactly one is taken and exactly one
				// is refused, cleanly, before a position. The document starts empty, so a refusal here
				// is the pair's sum and not a sum the document already held.
				errs = aggConcurrentWrites([2]func() error{aggWrite(node, verb, solo, "k2", aggBigAmount), aggWrite(node, verb, solo, "k3", aggBigAmount)})
				refused := 0
				for _, err := range errs {
					if err == nil {
						continue
					}
					refused++
					if !strings.Contains(err.Error(), "AGGREGATE_RANGE") {
						t.Fatalf("round %d: the refusal does not name the aggregate range: %v", round, err)
					}
				}
				if refused != 1 {
					t.Fatalf("round %d: %d of two writes of 2^61+1 into one document's store were refused, want exactly 1: %v", round, refused, errs)
				}
				// The pair still orders: nothing fenced it. The probe goes to the document that was
				// refused into, in the store that was refused into, with an amount that still fits.
				if err := aggWrite(node, verb, solo, "after", 1)(); err != nil {
					t.Fatalf("round %d: a write after the pair of writes failed, so the pair is fenced: %v", round, err)
				}
			}
		})
	}
}
