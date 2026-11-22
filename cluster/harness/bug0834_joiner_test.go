package harness_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// BUG-0834: a joiner that pulled its pairs while the database was empty, and was activated
// after the owner of one of them took acknowledged pushes, is that pair's second copy at 0:0
// until the pair's next write, while the copy that held the pushes leaves the copy set.

const (
	bug0834DB     = "jp"
	bug0834Bucket = "Box"
	bug0834RF     = 2
	bug0834Pushes = 6
)

// bug0834CatchUpBound is how long the joiner's copy is given to reach the owner's position
// after the activation. HOARDDB_BUG0834_WAIT overrides it (a Go duration) for a slow host.
func bug0834CatchUpBound(t *testing.T) time.Duration {
	t.Helper()
	bound := 30 * time.Second
	if text := os.Getenv("HOARDDB_BUG0834_WAIT"); text != "" {
		parsed, err := time.ParseDuration(text)
		if err != nil {
			t.Fatalf("HOARDDB_BUG0834_WAIT=%q: %v", text, err)
		}
		bound = parsed
	}
	return bound
}

// bug0834World is the construction after the activation: the slot whose owner did not change
// and whose copy set traded oldCopy for joiner, and the pushes the owner acknowledged before.
type bug0834World struct {
	c                         *harness.Cluster
	dbID                      uint64
	slot                      uint16
	id                        string
	owner, oldCopy, joiner    string
	ownerPos                  cluster.Position
	acked                     []string
	copiesBefore, copiesAfter []string
}

func bug0834Retry(t *testing.T, what string, attempt func(context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := attempt(ctx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func bug0834MemberIDs(members []cluster.Member) []string {
	ids := make([]string, 0, len(members))
	for _, member := range members {
		ids = append(ids, member.ID)
	}
	return ids
}

// bug0834Setup builds three nodes and an RF 2 database, joins a fourth that pulls while the
// database is empty, writes a document and six pushes to a pair whose owner the activation
// keeps and whose copy set gains the joiner, and activates the joiner. No write follows.
func bug0834Setup(t *testing.T, opts harness.Options) *bug0834World {
	t.Helper()
	return bug0834SetupWith(t, opts, nil)
}

// bug0834SetupWith is bug0834Setup with a hook that runs after the pushes and before the
// activation is requested, for a test that must disturb the cluster before the activation.
func bug0834SetupWith(t *testing.T, opts harness.Options, beforeActivation func(*bug0834World)) *bug0834World {
	t.Helper()
	opts.Late = 1
	c := harness.NewCluster(t, 3, opts)
	w := &bug0834World{c: c}
	bug0834Retry(t, "creating the database", func(ctx context.Context) error {
		var err error
		w.dbID, err = c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, bug0834DB, bug0834RF)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	w.joiner = c.AddNode()
	c.Await("the joiner to have pulled every pair", 5*time.Minute, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return c.Node(c.AwaitLeader(30*time.Second)).CheckSeeded(ctx, w.joiner) == nil
	})
	before := c.AwaitConverged(30 * time.Second)
	after := before
	after.Members = slices.Clone(before.Members)
	for i := range after.Members {
		if after.Members[i].ID == w.joiner {
			after.Members[i].State = cluster.MemberActive
		}
	}
	tableBefore, tableAfter := cluster.BuildTable(before), cluster.BuildTable(after)
	ownerIn := func(table *cluster.SlotTable, slot uint16) string {
		owner, _ := table.Owner(slot, bug0834RF)
		return owner.ID
	}
	bug0834Retry(t, "creating the bucket", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateBucket(ctx, bug0834DB, bug0834Bucket,
			wire.Field{Name: "Name", Type: "string"}, wire.Field{Name: "Queue", Type: "fifoStore"})
		return err
	})
	for i := 0; w.id == ""; i++ {
		candidate := fmt.Sprintf("d-%d", i)
		slot, err := wire.PlacementOf(bug0834DB, bug0834Bucket, candidate)
		if err != nil {
			t.Fatal(err)
		}
		copiesBefore := bug0834MemberIDs(tableBefore.Copies(slot, bug0834RF))
		copiesAfter := bug0834MemberIDs(tableAfter.Copies(slot, bug0834RF))
		if ownerIn(tableBefore, slot) == ownerIn(tableAfter, slot) && slices.Contains(copiesAfter, w.joiner) && !slices.Contains(copiesBefore, w.joiner) {
			w.id, w.slot, w.copiesBefore, w.copiesAfter = candidate, slot, copiesBefore, copiesAfter
		}
	}
	w.owner = ownerIn(tableBefore, w.slot)
	for _, id := range w.copiesBefore {
		if id != w.owner {
			w.oldCopy = id
		}
	}
	t.Logf("slot %d: owner %s before and after the activation; copies %v before, %v after", w.slot, w.owner, w.copiesBefore, w.copiesAfter)
	bug0834Retry(t, "inserting the document", func(ctx context.Context) error {
		_, err := c.Node(w.owner).Insert(ctx, bug0834DB, bug0834Bucket, bson.D{{Key: "_id_", Value: w.id}, {Key: "Name", Value: w.id}})
		return err
	})
	for i := 1; i <= bug0834Pushes; i++ {
		item := fmt.Sprintf("p%d", i)
		bug0834Retry(t, "pushing "+item, func(ctx context.Context) error {
			return c.Node(w.owner).Push(ctx, w.pushRequest(item))
		})
		w.acked = append(w.acked, item)
	}
	ownerPos, _, err := c.Node(w.owner).PairDump(w.slot, w.dbID)
	if err != nil {
		t.Fatalf("reading the owner's pair: %v", err)
	}
	w.ownerPos = ownerPos
	joinerPos, _, err := c.Node(w.joiner).PairDump(w.slot, w.dbID)
	t.Logf("before the activation: owner %s at %s, joiner %s at %s (%v)", w.owner, ownerPos, w.joiner, joinerPos, err)
	if beforeActivation != nil {
		beforeActivation(w)
	}
	c.ActivateWhileSeedingMoves(w.joiner, 90*time.Second, 240*time.Second)
	c.AwaitConverged(60 * time.Second)
	return w
}

func (w *bug0834World) target() wire.StoreTarget {
	return wire.StoreTarget{Database: bug0834DB, Bucket: bug0834Bucket, ID: w.id, Field: "Queue"}
}

func (w *bug0834World) pushRequest(item string) wire.PushRequest {
	return wire.PushRequest{StoreTarget: w.target(), Element: bson.D{{Key: "item", Value: item}}}
}

// heldOnce counts, in a pair dump, the acknowledged pushes that are not held exactly once.
func (w *bug0834World) heldOnce(entries []byte) int {
	notOnce := 0
	for _, item := range w.acked {
		if bytes.Count(entries, []byte(item+"\x00")) != 1 {
			notOnce++
		}
	}
	return notOnce
}

// joinerCaughtUp polls the joiner's copy of the pair for bound. It reports the last position
// read and how many acknowledged pushes it does not hold exactly once.
func (w *bug0834World) joinerCaughtUp(bound time.Duration) (cluster.Position, int, bool) {
	start := time.Now()
	for {
		pos, entries, err := w.c.Node(w.joiner).PairDump(w.slot, w.dbID)
		if err == nil && !pos.Less(w.ownerPos) {
			var joined []byte
			for _, entry := range entries {
				joined = append(joined, entry.Value...)
			}
			return pos, w.heldOnce(joined), true
		}
		if time.Since(start) >= bound {
			return pos, len(w.acked), false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestAJoinerThatBecomesASecondCopyOfAnUnmovedPairIsCaughtUpWithoutANewWrite (BUG-0834): after
// the activation, and with no further write to the pair, the joiner's copy reaches the owner's
// applied position within the bound and holds each acknowledged push once.
func TestAJoinerThatBecomesASecondCopyOfAnUnmovedPairIsCaughtUpWithoutANewWrite(t *testing.T) {
	w := bug0834Setup(t, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	bound := bug0834CatchUpBound(t)
	start := time.Now()
	pos, notOnce, reached := w.joinerCaughtUp(bound)
	if !reached {
		t.Fatalf("%s after the activation the joiner %s holds slot %d at %s and the owner %s at %s: the new copy was never sent the pair's tail",
			bound, w.joiner, w.slot, pos, w.owner, w.ownerPos)
	}
	if notOnce != 0 {
		t.Fatalf("the joiner reached %s, but %d of the %d acknowledged pushes are not held exactly once", pos, notOnce, len(w.acked))
	}
	t.Logf("the joiner reached %s %.1fs after the activation, holding each acknowledged push once", pos, time.Since(start).Seconds())
}

// TestAnOwnerKilledAfterAJoinersActivationKeepsEveryAcknowledgedPush (BUG-0834, the failover
// that grades it): after the activation, with no further write, the joiner is given the bound
// to catch up (the outcome is logged, not asserted, so the failover runs on either tree), then
// the owner is killed. Every acknowledged push must still be served once by the survivors.
func TestAnOwnerKilledAfterAJoinersActivationKeepsEveryAcknowledgedPush(t *testing.T) {
	w := bug0834Setup(t, harness.Options{FastDetection: true, WCTimeout: 5 * time.Second})
	pos, notOnce, reached := w.joinerCaughtUp(bug0834CatchUpBound(t))
	t.Logf("before the kill: joiner %s at %s (reached the owner's %s: %v, pushes not held once: %d); old copy %s",
		w.joiner, pos, w.ownerPos, reached, notOnce, w.oldCopy)
	w.killOwnerAndAudit(t)
}

// killOwnerAndAudit kills the owner, waits for the survivors to commit it down, and runs
// auditServed.
func (w *bug0834World) killOwnerAndAudit(t *testing.T) {
	t.Helper()
	epochBefore := w.c.Node(w.joiner).State().Epoch
	w.c.Kill(w.owner)
	w.c.Await("the survivors to commit the owner down (an epoch bump)", 60*time.Second, func() bool {
		for _, id := range w.c.Running() {
			if w.c.Node(id).State().Epoch <= epochBefore {
				return false
			}
		}
		return true
	})
	w.c.AwaitLeader(30 * time.Second)
	w.c.AwaitLeases(30 * time.Second)
	w.auditServed(t, 60*time.Second)
}

// auditServed fails the test unless the joiner's node serves every acknowledged push within
// bound: a served "is not in" answer is DATA LOSS (the insert was acknowledged), anything else
// not served is UNAVAILABLE.
func (w *bug0834World) auditServed(t *testing.T, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	var elements [][]byte
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		elements, lastErr = w.c.Node(w.joiner).Peek(ctx, wire.PeekRequest{StoreTarget: w.target(), Count: 100})
		cancel()
		if lastErr == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	newOwner, newCopies, ownerErr := w.c.Node(w.joiner).Owner(ctx, bug0834DB, bug0834Bucket, w.id)
	cancel()
	for _, id := range w.c.Running() {
		pos, entries, err := w.c.Node(id).PairDump(w.slot, w.dbID)
		var joined []byte
		for _, entry := range entries {
			joined = append(joined, entry.Value...)
		}
		t.Logf("after the kill: %s holds slot %d at %s, pushes not held once %d (%v)", id, w.slot, pos, w.heldOnce(joined), err)
	}
	t.Logf("after the kill: owner %s, copies %v (%v)", newOwner, newCopies, ownerErr)
	if lastErr != nil && (strings.Contains(lastErr.Error(), "not found") || strings.Contains(lastErr.Error(), "is not in")) {
		t.Fatalf("DATA LOSS: after the owner %s was killed the pair does not hold the acknowledged document: %v", w.owner, lastErr)
	}
	if lastErr != nil {
		t.Fatalf("UNAVAILABLE: %s after the owner %s was killed the pair's document is not served: %v", bound, w.owner, lastErr)
	}
	served := make([]string, 0, len(elements))
	for _, element := range elements {
		served = append(served, bson.Raw(element).Lookup("item").StringValue())
	}
	if !slices.Equal(served, w.acked) {
		t.Fatalf("DATA LOSS: after the owner %s was killed the new owner %s serves %v; the acknowledged pushes were %v", w.owner, newOwner, served, w.acked)
	}
	t.Logf("no loss: the new owner %s serves every acknowledged push %v", newOwner, served)
}
