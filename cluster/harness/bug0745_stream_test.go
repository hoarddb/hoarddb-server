package harness_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestAReplicationFrameDoesNotWaitForAnotherPairsCopyApply (BUG-0745): the owner ships
// a pair's record to a copy without waiting for that copy to append and apply it, and the
// copy applies records of different pairs concurrently, so one pair's slow copy-side
// append does not queue every other pair behind it. Eight inserts on eight pairs that
// share one owner and one copy, with that copy's appends slowed to 250 ms each, must all
// be confirmed by the copy in well under the 2 s a one-at-a-time stream needs.
func TestAReplicationFrameDoesNotWaitForAnotherPairsCopyApply(t *testing.T) {
	const pairs = 8
	const appendDelay = 250 * time.Millisecond
	c := proofSetup(t, harness.Options{WCTimeout: 10 * time.Second})
	first, owner, copyNode, _ := proofID(t, c, "bug0745")
	ids := []string{first}
	probe := c.Node(owner)
	for i := 0; len(ids) < pairs; i++ {
		if i > 4000 {
			t.Fatalf("could not place %d ids on owner %s with copy %s", pairs, owner, copyNode)
		}
		id := "bug0745-more-" + strconv.Itoa(i)
		o, copies, err := probe.Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if o == owner && len(copies) == 2 && copies[1] == copyNode {
			ids = append(ids, id)
		}
	}
	c.SlowDisk(copyNode, appendDelay)
	t.Cleanup(func() { c.SlowDisk(copyNode, 0) })

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, pairs)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			got, err := probe.Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
			if err != nil {
				errs <- err
				return
			}
			if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
				errs <- &notConfirmed{id: id, outcome: got.Outcome.String(), copies: int(got.CopiesConfirmed)}
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("insert through the owner: %v (background %v)", err, c.BackgroundErrors())
	}
	elapsed := time.Since(start)
	serialised := time.Duration(pairs) * appendDelay
	t.Logf("%d inserts on %d pairs with a %s copy append took %s (one at a time: %s)", pairs, pairs, appendDelay, elapsed.Round(time.Millisecond), serialised)
	if elapsed >= serialised/2 {
		t.Fatalf("%d inserts on %d pairs took %s with the copy's appends slowed to %s: the stream waits for the copy to apply one record before it ships the next (a one-at-a-time stream needs %s); want well under %s", pairs, pairs, elapsed.Round(time.Millisecond), appendDelay, serialised, serialised/2)
	}
}

type notConfirmed struct {
	id, outcome string
	copies      int
}

func (e *notConfirmed) Error() string {
	return "insert of " + e.id + " answered " + e.outcome + " with copies_confirmed " + strconv.Itoa(e.copies) + ", want applied and 2"
}
