package harness_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

const (
	proofDB     = "app"
	proofBucket = "users"
)

func proofCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// proofSetup forms a three-voter cluster with an RF=2 database and one bucket every
// node can resolve.
func proofSetup(t *testing.T, opts harness.Options) *harness.Cluster {
	t.Helper()
	c := harness.NewCluster(t, 3, opts)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatalf("creating the RF=2 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	c.AwaitLeases(30 * time.Second)
	return c
}

// proofRetryTransient runs send until it succeeds or fails with something that is not a
// LEASE_LAPSED or NO_QUORUM refusal. Those two pass by themselves and a loaded VM makes a
// healthy cluster raise them (BUG-0808), so the test waits for the cluster to be servable
// again and sends once more; any other refusal is what the test is about and is returned.
func proofRetryTransient(c *harness.Cluster, send func() error) error {
	var last error
	for range 20 {
		if last = send(); last == nil || !harness.IsTransientRefusal(last) {
			return last
		}
		c.AwaitLeases(30 * time.Second)
	}
	return last
}

// proofIDOwnedBy returns a document id whose slot ranks owner first and copy second.
func proofID(t *testing.T, c *harness.Cluster, seed string) (id, owner, copyNode, outsider string) {
	t.Helper()
	probe := c.Node(c.IDs()[0])
	for i := 0; i < 2000; i++ {
		id = fmt.Sprintf("%s-%d", seed, i)
		o, copies, err := probe.Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if len(copies) == 2 && o == copies[0] {
			for _, n := range c.IDs() {
				if n != copies[0] && n != copies[1] {
					return id, copies[0], copies[1], n
				}
			}
		}
	}
	t.Fatal("no id placed")
	return
}

func TestRF2InsertIsAppliedOnOwnerAndReplicatedToItsCopy(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	id, owner, copyNode, outsider := proofID(t, c, "a")
	got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("insert through the owner %s: %v (background: %v)", owner, err, c.BackgroundErrors())
	}
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("outcome %v copies_confirmed %d, want applied and 2", got.Outcome, got.CopiesConfirmed)
	}
	ownDoc, found, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
	if err != nil || !found {
		t.Fatalf("the owner %s does not hold %s: found %v err %v", owner, id, found, err)
	}
	copyDoc, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
	if err != nil || !found {
		t.Fatalf("the copy %s does not hold %s in its own store: found %v err %v", copyNode, id, found, err)
	}
	if fmt.Sprint(ownDoc.Fields) != fmt.Sprint(copyDoc.Fields) {
		t.Fatalf("owner holds %v, copy holds %v", ownDoc.Fields, copyDoc.Fields)
	}
	if _, found, _ := c.Node(outsider).LocalDocument(proofCtx(t), proofDB, proofBucket, id); found {
		t.Fatalf("%s holds a document it has no copy of", outsider)
	}
}

func proofInsert(t *testing.T, c *harness.Cluster, via, id, name string) wire.Inserted {
	t.Helper()
	var got wire.Inserted
	err := proofRetryTransient(c, func() (insertErr error) {
		got, insertErr = c.Node(via).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: name}})
		return insertErr
	})
	if err != nil {
		t.Fatalf("insert of %s through %s: %v (background: %v)", id, via, err, c.BackgroundErrors())
	}
	return got
}

func TestAWriteToANonOwnerIsForwardedToTheOwnerAndAnsweredOnce(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	for _, role := range []string{"outsider", "copy"} {
		id, owner, copyNode, outsider := proofID(t, c, "f-"+role)
		via := outsider
		if role == "copy" {
			via = copyNode
		}
		got := proofInsert(t, c, via, id, "Bea")
		if got.ID != id || got.Deduped {
			t.Fatalf("%s: forwarded insert answered %+v, want id %s not deduped", role, got, id)
		}
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
			t.Fatalf("%s: the owner's answer came back as %v with %d copies, want applied and 2: a forward relays the owner's answer", role, got.Outcome, got.CopiesConfirmed)
		}
		if _, found, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
			t.Fatalf("%s: the owner %s did not apply the forwarded write: found %v err %v", role, owner, found, err)
		}
		if role == "outsider" {
			if _, found, _ := c.Node(outsider).LocalDocument(proofCtx(t), proofDB, proofBucket, id); found {
				t.Fatalf("the forwarder %s applied a write it does not own", outsider)
			}
		}
		// A retry with the same id is refused by the owner: the first attempt was applied once.
		if _, err := c.Node(via).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Bea"}}); err == nil {
			t.Fatalf("%s: a second insert of %s was accepted; the first forward must have applied exactly once", role, id)
		}
	}
}

// proofRead reads id through node via (a keyed get, which forwards to the owner when via
// is not it) and returns whether it was found.
func proofRead(t *testing.T, c *harness.Cluster, via, id string) bool {
	t.Helper()
	_, err := c.Node(via).Get(proofCtx(t), proofDB, proofBucket, id)
	return err == nil
}

func TestAReadFromANonOwnerIsRoutedToTheOwner(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	id, _, _, outsider := proofID(t, c, "r")
	proofInsert(t, c, outsider, id, "Cy")
	if !proofRead(t, c, outsider, id) {
		t.Fatalf("a keyed get through %s did not reach the owner of %s", outsider, id)
	}
}

func TestTheCopyKilledMidStreamIsDegradedThenCatchesUp(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, copyNode, _ := proofID(t, c, "k")
	first := proofInsert(t, c, owner, id, "one")
	if first.Outcome != machine.OutcomeApplied {
		t.Fatalf("first write %v, want applied", first.Outcome)
	}
	c.Kill(copyNode)
	// The ids of later writes need the same pair: same owner, same copy.
	var more []string
	for i := 0; len(more) < 3; i++ {
		cand := fmt.Sprintf("k-more-%d", i)
		o, cs, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, cand)
		if err != nil {
			t.Fatal(err)
		}
		if o == owner && len(cs) == 2 && cs[1] == copyNode {
			more = append(more, cand)
		}
	}
	var last wire.Inserted
	for _, cand := range more {
		last = proofInsert(t, c, owner, cand, "while-down")
		if last.Outcome != machine.OutcomeAppliedDegraded || last.CopiesConfirmed != 1 {
			t.Fatalf("a write with the copy dead: outcome %v copies_confirmed %d, want applied_degraded and 1 (majority of RF 2 needs both)", last.Outcome, last.CopiesConfirmed)
		}
	}
	c.Restart(copyNode)
	c.AwaitLeases(20 * time.Second)
	for _, cand := range append([]string{id}, more...) {
		want, err := c.Node(owner).PairPosition(proofCtx(t), proofDB, proofBucket, cand)
		if err != nil {
			t.Fatal(err)
		}
		c.Await("the restarted copy to catch up "+cand+" to the owner's position "+want.String(), 20*time.Second, func() bool {
			got, err := c.Node(copyNode).PairPosition(proofCtx(t), proofDB, proofBucket, cand)
			return err == nil && !got.Less(want)
		})
	}
	for _, cand := range append([]string{id}, more...) {
		ownDoc, _, _ := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, cand)
		copyDoc, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, cand)
		if err != nil || !found || fmt.Sprint(ownDoc.Fields) != fmt.Sprint(copyDoc.Fields) {
			t.Fatalf("after catch-up the copy's %s is %v (found %v, err %v), the owner's is %v", cand, copyDoc.Fields, found, err, ownDoc.Fields)
		}
	}
}

// proofIDsOwnedBy returns count ids whose slots rank owner first and copyNode second.
func proofIDsOwnedBy(t *testing.T, c *harness.Cluster, owner, copyNode, seed string, count int) []string {
	t.Helper()
	var ids []string
	for i := 0; len(ids) < count; i++ {
		if i > 5000 {
			t.Fatalf("no %d ids placed with owner %s and copy %s", count, owner, copyNode)
		}
		cand := fmt.Sprintf("%s-%d", seed, i)
		o, cs, err := c.Node(owner).Owner(proofCtx(t), proofDB, proofBucket, cand)
		if err != nil {
			t.Fatal(err)
		}
		if o == owner && len(cs) == 2 && cs[1] == copyNode {
			ids = append(ids, cand)
		}
	}
	return ids
}

// proofInsertRetrying inserts through via and retries while the cluster settles after a
// failure: a refusal is not an acknowledgement, so retrying it cannot lose or duplicate a
// write (the id is fixed, and a repeat of an applied insert is refused DOCUMENT_EXISTS).
func proofInsertRetrying(t *testing.T, c *harness.Cluster, via, id string) (wire.Inserted, bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		got, err := c.Node(via).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "after-failover"}})
		if err == nil {
			return got, true
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("insert of %s through %s never acknowledged: %v", id, via, last)
	return wire.Inserted{}, false
}

// This covers exactly one thing: RF 2, writes acknowledged applied with both copies
// confirmed, and only the owner killed. It does not cover RF 3 with the owner and the one
// acking copy both lost, a minority-side owner, or applied_degraded writes (SPEC-GAP
// U57-15, U57-14).
func TestTheOwnerKilledAtRF2LosesNoWriteBothCopiesAcknowledged(t *testing.T) {
	c := proofSetup(t, harness.Options{FastDetection: true})
	_, owner, copyNode, outsider := proofID(t, c, "o")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "ledger", 6)
	acked := map[string]bool{}
	for _, id := range ids {
		got := proofInsert(t, c, owner, id, "before")
		if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
			t.Fatalf("before the kill %s answered %v/%d, want applied/2", id, got.Outcome, got.CopiesConfirmed)
		}
		acked[id] = true
	}
	epochBefore := c.Node(copyNode).State().Epoch
	c.Kill(owner)
	c.Await("the survivors to commit the owner down (an epoch bump)", 30*time.Second, func() bool {
		for _, id := range []string{copyNode, outsider} {
			if c.Node(id).State().Epoch <= epochBefore {
				return false
			}
		}
		return true
	})
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	for _, id := range ids {
		newOwner, _, err := c.Node(copyNode).Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || newOwner != copyNode {
			t.Fatalf("after the epoch bump the owner of %s is %q (err %v), want the copy %s", id, newOwner, err, copyNode)
		}
	}
	// Reads of every acknowledged write, through the new owner and through the outsider.
	for _, via := range []string{copyNode, outsider} {
		for id := range acked {
			deadline := time.Now().Add(30 * time.Second)
			for !proofRead(t, c, via, id) {
				if time.Now().After(deadline) {
					t.Fatalf("acknowledged write %s is not readable through %s after the failover", id, via)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	// New writes after the failover: through the new owner and through the outsider (which
	// forwards to it). The other copy is dead, so a majority of RF 2 cannot be reached and
	// the owner says so: applied_degraded with one copy confirmed, never applied.
	fresh := 0
	for i := 0; fresh < 4; i++ {
		id := fmt.Sprintf("post-%d", i)
		if o, _, err := c.Node(copyNode).Owner(proofCtx(t), proofDB, proofBucket, id); err != nil || o != copyNode {
			continue
		}
		via := []string{copyNode, outsider}[fresh%2]
		got, ok := proofInsertRetrying(t, c, via, id)
		if !ok {
			t.Fatalf("a write of %s through %s was never acknowledged after the failover", id, via)
		}
		if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Fatalf("write %s through %s answered %v with %d copies, want applied_degraded and 1 (the other copy is dead)", id, via, got.Outcome, got.CopiesConfirmed)
		}
		acked[id] = true
		fresh++
	}
	// The ledger: nothing acknowledged, before or after the failover, is missing.
	for _, via := range []string{copyNode, outsider} {
		for id := range acked {
			if !proofRead(t, c, via, id) {
				t.Fatalf("acknowledged write %s is not readable through %s at the end", id, via)
			}
		}
	}
}

func docOf(id, name string) bson.D {
	return bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: name}}
}

// TestAQuickRestartIsCaughtUpByTheOwnersRetry: with probing off, nothing tells the owner
// that the copy is back except its own retry of the send that failed, which must be enough.
func TestAQuickRestartIsCaughtUpByTheOwnersRetry(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true, NoProbes: true})
	id, owner, copyNode, _ := proofID(t, c, "q")
	c.Kill(copyNode)
	got := proofInsert(t, c, owner, id, "while-down")
	if got.Outcome != machine.OutcomeAppliedDegraded {
		t.Fatalf("write with the copy dead: %v, want applied_degraded", got.Outcome)
	}
	c.Restart(copyNode)
	c.Await("the restarted copy to receive the write with no probe to prompt the owner", 30*time.Second, func() bool {
		_, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		return err == nil && found
	})
}
