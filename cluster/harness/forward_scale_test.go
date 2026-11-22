package harness_test

import (
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestForwardedWritesToOneOwnerScaleWithConcurrency is BUG-0769 on the harness: three nodes,
// RF 1, every upsert enters through n1 and is owned by n2, so every write is forwarded over
// one lane. Before the lane carried concurrent calls, 32 workers ran at 0.95 times the rate of
// one (the lane served one call at a time); the bound here is loose (1.5 times) because the
// harness has no real fsync and a shared host, and the rates are logged. Measured on the test host
// over 10 runs each (two sessions): without -race 1.97x to 3.15x (median 2.3x), with -race 2.6x to 3.0x; the
// pre-fix code gives 0.85x, so 1.5x separates them with margin. The acceptance
// bound of the bug (3 to 4 times on the benchmark VMs) is measured there, not here.
func TestForwardedWritesToOneOwnerScaleWithConcurrency(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "fw", 1); err != nil {
		t.Fatalf("creating the database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "fw", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	ids := c.IDs()
	entry, ownerID := c.Node(ids[0]), ids[1]

	const perLeg = 4800
	run := func(tag string, workers int) float64 {
		docs, err := rfPlaceIDs(rfIDPlan{
			DB: "fw", Tag: tag, Count: perLeg,
			SlotOf: func(id string) (uint16, error) { return wire.PlacementOf("fw", proofBucket, id) },
			Owned: func(id string) (bool, error) {
				o, _, err := entry.Owner(proofCtx(t), "fw", proofBucket, id)
				return o == ownerID, err
			},
		})
		if err != nil {
			t.Fatalf("placing the ids of %s: %v", tag, err)
		}
		per := perLeg / workers
		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := w * per; i < (w+1)*per; i++ {
					if _, err := entry.Upsert(proofCtx(t), "fw", proofBucket, bson.D{{Key: "_id_", Value: docs[i]}, {Key: "name", Value: "Ada"}}); err != nil {
						t.Errorf("upsert %s through %s: %v (background %v)", docs[i], ids[0], err, c.BackgroundErrors())
						return
					}
				}
			}()
		}
		wg.Wait()
		return float64(per*workers) / time.Since(start).Seconds()
	}
	// A warm-up leg opens the connections and the declaration caches before anything is timed.
	run("warm", 4)
	one, many := run("one", 1), run("many", 32)
	t.Logf("documents owned by %s, written through %s: 1 worker %.0f per second, 32 workers %.0f per second (%.2fx)", ownerID, ids[0], one, many, many/one)
	if many < 1.5*one {
		t.Fatalf("32 workers wrote %.0f per second through the forward lane against %.0f for one worker (%.2fx): the lane serves one forwarded write at a time (BUG-0769)",
			many, one, many/one)
	}
}
