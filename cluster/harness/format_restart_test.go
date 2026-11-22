package harness_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestAnAcknowledgedWriteSurvivesAKillAndRestart (storage-format slice A, F4 data-safety a):
// N acknowledged writes in record format 2, the owner killed without a graceful drain and
// restarted on its own directory, and all N read back. The harness Kill is a clean Close of
// the node (it does not drop unsynced Badger writes); the log-only rebuild of an unsynced
// batch is pinned by TestRetriedPopAfterRestartReturnsTheRecordedValue and
// TestAStampedRecordReplaysToTheSameState in cluster/wire.
func TestAnAcknowledgedWriteSurvivesAKillAndRestart(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "fmt2", 2); err != nil {
		t.Fatalf("creating the database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "fmt2", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	// Every write goes to a pair the same node owns, so that node's own log and store are
	// what the restart reads back; with RF 2 of 3 it holds no copy of the other pairs.
	const writes = 12
	probe := c.Node(c.IDs()[0])
	owner, _, err := probe.Owner(proofCtx(t), "fmt2", proofBucket, "doc-0")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; len(ids) < writes && i < 10_000; i++ {
		id := fmt.Sprintf("doc-%d", i)
		if placed, _, err := probe.Owner(proofCtx(t), "fmt2", proofBucket, id); err == nil && placed == owner {
			ids = append(ids, id)
		}
	}
	if len(ids) != writes {
		t.Fatalf("found %d ids owned by %s, want %d", len(ids), owner, writes)
	}
	for _, id := range ids {
		if _, err := c.Node(owner).Insert(proofCtx(t), "fmt2", proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}}); err != nil {
			t.Fatalf("insert %s through %s: %v (background %v)", id, owner, err, c.BackgroundErrors())
		}
	}
	c.Kill(owner)
	c.Restart(owner)
	for _, id := range ids {
		c.Await("the restarted node holds "+id, 30*time.Second, func() bool {
			_, found, err := c.Node(owner).LocalDocument(proofCtx(t), "fmt2", proofBucket, id)
			return err == nil && found
		})
	}
}
