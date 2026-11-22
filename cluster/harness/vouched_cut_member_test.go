package harness_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestAMemberWhoseStoreWasMovedAsideIsRefusedAndCommittedStateIsKept: [GUARD store-vouched-cut
// review R-1, a real seeded joiner: three nodes at RF 2 hold a few documents, a fourth joins and
// pulls its pairs by seed (data that never passed through its log), is activated and killed, and its
// store directory is moved aside. The restart is refused with the member clause, naming the store,
// writing nothing into it; the cluster's committed state is untouched; the directory put back opens
// again]. Red when the node takes the empty directory as a first start and serves its seeded pairs
// empty.
func TestAMemberWhoseStoreWasMovedAsideIsRefusedAndCommittedStateIsKept(t *testing.T) {
	const (
		database = "vc"
		bucket   = "Box"
		docs     = 6
	)
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second, Late: 1})
	bug0834Retry(t, "creating the database", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, database, 2)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	bug0834Retry(t, "creating the bucket", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateBucket(ctx, database, bucket, wire.Field{Name: "name", Type: "string"})
		return err
	})
	ids := make([]string, 0, docs)
	for i := range docs {
		id := fmt.Sprintf("doc-%d", i)
		owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), database, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		bug0834Retry(t, "inserting "+id, func(ctx context.Context) error {
			_, err := c.Node(owner).Insert(ctx, database, bucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
			return err
		})
		ids = append(ids, id)
	}

	joiner := c.AddNode()
	c.Await("the joiner to have pulled every pair", 5*time.Minute, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return c.Node(c.AwaitLeader(30*time.Second)).CheckSeeded(ctx, joiner) == nil
	})
	c.ActivateWhileSeedingMoves(joiner, 90*time.Second, 240*time.Second)
	stateBefore := c.AwaitConverged(60 * time.Second)

	c.Kill(joiner)
	store := filepath.Join(c.DataDir(joiner), "badger")
	aside := filepath.Join(t.TempDir(), "badger")
	if err := os.Rename(store, aside); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		err := c.TryRestart(joiner)
		if err == nil {
			t.Fatalf("start %d: the joiner opened on an empty store directory", attempt)
		}
		var refusal *wire.EmptyStoreAfterEarlierLifeError
		if !errors.As(err, &refusal) {
			t.Fatalf("start %d: the refusal is %T, want an EmptyStoreAfterEarlierLifeError: %v", attempt, err, err)
		}
		text := err.Error()
		for _, want := range []string{"the node store " + store + " holds no data file", "Nothing was written to the store",
			"This node is configured as a member of a cluster (cluster.enabled), so do not move " + c.DataDir(joiner) + " aside"} {
			if !strings.Contains(text, want) {
				t.Errorf("start %d: the refusal lacks %q: %s", attempt, want, text)
			}
		}
		for _, banned := range []string{"reseed", "re-seed", "remove-node", "empty its data directory", "leave it empty"} {
			if strings.Contains(text, banned) {
				t.Errorf("start %d: the refusal says %q: %s", attempt, banned, text)
			}
		}
		if _, statErr := os.Stat(filepath.Join(store, "VOUCHED")); !os.IsNotExist(statErr) {
			t.Fatalf("start %d: the refused start wrote a marker (%v), so the next start would pass", attempt, statErr)
		}
	}

	// Committed state is kept: the members still hold the documents and the same state.
	for _, id := range ids {
		for _, member := range c.Running() {
			owner, _, err := c.Node(member).Owner(proofCtx(t), database, bucket, id)
			if err != nil {
				t.Fatal(err)
			}
			if owner == joiner {
				continue // its owner is the refused node; the copy is checked after it returns
			}
			c.Await(member+" reads "+id, 30*time.Second, func() bool {
				_, err := c.Node(member).Get(proofCtx(t), database, bucket, id)
				return err == nil
			})
		}
	}
	if got := c.AwaitConverged(30 * time.Second); len(got.Databases) != len(stateBefore.Databases) {
		t.Fatalf("the committed state changed while the joiner was refused: %d databases, was %d", len(got.Databases), len(stateBefore.Databases))
	}

	// Put the directory back: the node opens and holds what it seeded.
	if err := os.RemoveAll(store); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(aside, store); err != nil {
		t.Fatal(err)
	}
	c.Restart(joiner)
	for _, id := range ids {
		c.Await("the restored joiner serves "+id, 30*time.Second, func() bool {
			_, err := c.Node(joiner).Get(proofCtx(t), database, bucket, id)
			return err == nil
		})
	}
}
