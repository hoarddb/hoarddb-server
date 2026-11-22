package harness_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestBug0746Stress is the BUG-0746 reproduction at load (about four runs in ten fail; the
// deterministic proof is TestARestartedOwnerShipsAnAcknowledgedRecordToTheCopyThatNeverGotIt): 16 writers insert distinct ids into
// an RF 3 database for 10 s through the two nodes that stay up; the third closes at 3 s and
// restarts at 6 s. Every acknowledged insert (no error) must be on all three nodes after the
// settle. Knobs: BUG0746_RUNS, BUG0746_WRITERS.
func TestBug0746Stress(t *testing.T) {
	if os.Getenv("HOARDDB_BUG0746") == "" {
		t.Skip("a long reproduction; set HOARDDB_BUG0746=1")
	}
	writers := 16
	if v, err := strconv.Atoi(os.Getenv("BUG0746_WRITERS")); err == nil {
		writers = v
	}
	c := harness.NewCluster(t, 3, harness.Options{WCTimeout: 5 * time.Second, NoDetection: os.Getenv("BUG0746_DETECT") == ""})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "rf3", 3); err != nil {
		t.Fatalf("creating the RF 3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "rf3", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	var mu sync.Mutex
	acked := map[string]uint8{}
	ackPos := map[string]string{}
	var errs, total atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	via := []string{"n1", "n2"}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := "w" + strconv.Itoa(w) + "-" + strconv.Itoa(i)
				got, err := c.Node(via[w%2]).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
				total.Add(1)
				if err != nil {
					errs.Add(1)
					continue
				}
				mu.Lock()
				acked[id] = got.CopiesConfirmed
				ackPos[id] = got.Pos.String() + " " + got.Outcome.String()
				mu.Unlock()
			}
		}(w)
	}
	time.Sleep(3 * time.Second)
	c.Kill("n3")
	time.Sleep(3 * time.Second)
	c.Restart("n3")
	time.Sleep(4 * time.Second)
	close(stop)
	wg.Wait()
	t.Logf("%d attempts, %d acknowledged, %d errors", total.Load(), len(acked), errs.Load())

	deadline := time.Now().Add(40 * time.Second)
	var missing []string
	for {
		missing = missing[:0]
		for id, copies := range acked {
			if copies < 2 {
				continue
			}
			for _, n := range c.IDs() {
				if _, found, err := c.Node(n).LocalDocument(proofCtx(t), "rf3", proofBucket, id); err != nil || !found {
					missing = append(missing, fmt.Sprintf("%s on %s (copies %d)", id, n, copies))
				}
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if len(missing) > 0 {
		dbID := uint64(c.Node("n1").State().Databases[0].ID)
		for _, m := range missing {
			var id, node string
			if _, err := fmt.Sscanf(m, "%s on %s", &id, &node); err != nil {
				t.Fatalf("reading the missing entry %q: %v", m, err)
			}
			slot, _ := wire.PlacementOf("rf3", proofBucket, id)
			owner, _ := c.Node("n1").OwnerOfSlot(slot, dbID)
			line := fmt.Sprintf("DIAG %s slot %d owner %s ack %s:", id, slot, owner, ackPos[id])
			for _, n := range c.IDs() {
				pos, keys, err := c.Node(n).PairDump(slot, dbID)
				line += fmt.Sprintf(" %s=%s/%dkeys/%v/rec=%v", n, pos, len(keys), err, c.Node(n).SlotRecordState(slot, dbID))
			}
			t.Log(line)
		}
		t.Fatalf("%d acknowledged writes missing after the settle: %v", len(missing), missing)
	}
}
