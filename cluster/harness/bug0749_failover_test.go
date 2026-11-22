package harness_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

const bug0749Bucket = "Carts"

// bug0749World is an RF 2 database on three voters with one cart whose Orders field is a
// fifoStore holding a, b, c, and the three roles its slot gives the nodes.
type bug0749World struct {
	c                       *harness.Cluster
	id                      string
	owner, copyNode, outsid string
}

func bug0749Setup(t *testing.T) *bug0749World {
	t.Helper()
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatalf("creating the RF=2 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, bug0749Bucket,
		wire.Field{Name: "name", Type: "string"}, wire.Field{Name: "Orders", Type: "fifoStore"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	// The placement is computed for this bucket: proofID is computed for "users" and puts
	// other buckets elsewhere.
	w := &bug0749World{c: c}
	probe := c.Node(c.IDs()[0])
	for i := 0; w.id == "" && i < 2000; i++ {
		cand := fmt.Sprintf("cart-%d", i)
		o, copies, err := probe.Owner(proofCtx(t), proofDB, bug0749Bucket, cand)
		if err != nil {
			t.Fatalf("placing %s: %v", cand, err)
		}
		if len(copies) == 2 && o == copies[0] {
			w.id, w.owner, w.copyNode = cand, copies[0], copies[1]
		}
	}
	if w.id == "" {
		t.Fatal("no cart id placed")
	}
	for _, n := range c.IDs() {
		if n != w.owner && n != w.copyNode {
			w.outsid = n
		}
	}
	if _, err := c.Node(w.owner).Insert(proofCtx(t), proofDB, bug0749Bucket, bson.D{{Key: "_id_", Value: w.id}, {Key: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("inserting the cart: %v", err)
	}
	for _, item := range []string{"a", "b", "c"} {
		if err := c.Node(w.owner).Push(proofCtx(t), wire.PushRequest{StoreTarget: w.target(), Element: bson.D{{Key: "item", Value: item}}}); err != nil {
			t.Fatalf("pushing %s: %v", item, err)
		}
	}
	return w
}

func (w *bug0749World) target() wire.StoreTarget {
	return wire.StoreTarget{Database: proofDB, Bucket: bug0749Bucket, ID: w.id, Field: "Orders"}
}

// copyCaughtUp waits until the copy holds every record the owner has applied.
func (w *bug0749World) copyCaughtUp(t *testing.T) {
	t.Helper()
	want, err := w.c.Node(w.owner).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
	if err != nil {
		t.Fatal(err)
	}
	w.c.Await("the copy to hold the owner's position "+want.String(), 20*time.Second, func() bool {
		got, err := w.c.Node(w.copyNode).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
		return err == nil && !got.Less(want)
	})
}

// awaitActive waits until every running node sees id committed active. The harness profile
// detects a fault in a few hundred milliseconds, so a restart that is slower than that finds
// the node committed down; the member then asks itself back (BUG-0754 fixed), and this wait
// is for that commit. Without it a promoted copy that is still down is skipped as owner and
// killOwner waits out its bound, which is the restart's timing and not the replay this test
// is about.
func (w *bug0749World) awaitActive(t *testing.T, id string) {
	t.Helper()
	// Active must hold for several detection periods: the commit that marks the killed node
	// down can land after the restart has already shown it active.
	settle := 4 * harness.P1.Detection()
	var activeSince time.Time
	w.c.Await(id+" to be committed active everywhere and stay so", 30*time.Second, func() bool {
		everywhere := true
		for _, running := range w.c.Running() {
			for _, member := range w.c.Node(running).State().Members {
				if member.ID == id && member.State != cluster.MemberActive {
					everywhere = false
				}
			}
		}
		if !everywhere {
			activeSince = time.Time{}
			return false
		}
		if activeSince.IsZero() {
			activeSince = time.Now()
		}
		return time.Since(activeSince) >= settle
	})
}

// killOwner stops the owner and waits until the copy is the owner of the cart.
func (w *bug0749World) killOwner(t *testing.T) {
	t.Helper()
	epochBefore := w.c.Node(w.copyNode).State().Epoch
	w.c.Kill(w.owner)
	w.c.Await("the survivors to commit the owner down (an epoch bump)", 30*time.Second, func() bool {
		for _, id := range []string{w.copyNode, w.outsid} {
			if w.c.Node(id).State().Epoch <= epochBefore {
				return false
			}
		}
		return true
	})
	w.c.AwaitLeader(10 * time.Second)
	w.c.AwaitLeases(20 * time.Second)
	w.c.Await("the copy to own the cart", 30*time.Second, func() bool {
		o, _, err := w.c.Node(w.copyNode).Owner(proofCtx(t), proofDB, bug0749Bucket, w.id)
		return err == nil && o == w.copyNode
	})
}

// retryPop sends the pop again under the same request id through via until the cluster
// has settled enough to answer it.
func (w *bug0749World) retryPop(t *testing.T, via string, request [16]byte) wire.Popped {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := w.c.Node(via).Pop(proofCtx(t), wire.PopRequest{StoreTarget: w.target()}, wire.WithRequestID(request))
		if err == nil {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("the retried pop through %s was never answered: %v", via, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func bug0749Item(t *testing.T, element []byte) string {
	t.Helper()
	return bson.Raw(element).Lookup("item").StringValue()
}

// TestAnOwnerKillDoesNotLetARetriedPopApplyTwice (BUG-0749): a client pops under a request
// id, the owner dies after its copy acknowledged, and the client retries the same id. The
// promoted copy must answer from the entry it wrote while applying the pop, not remove a
// second element.
func TestAnOwnerKillDoesNotLetARetriedPopApplyTwice(t *testing.T) {
	w := bug0749Setup(t)
	request := [16]byte{0x49, 0x07, 1}
	first, err := w.c.Node(w.owner).Pop(proofCtx(t), wire.PopRequest{StoreTarget: w.target()}, wire.WithRequestID(request))
	if err != nil || !first.Found || bug0749Item(t, first.Element) != "a" {
		t.Fatalf("the first pop = %+v, %v, want element a", first, err)
	}
	w.copyCaughtUp(t)
	w.killOwner(t)

	for _, via := range []string{w.outsid, w.copyNode} {
		again := w.retryPop(t, via, request)
		if !again.Deduped || !bytes.Equal(again.Element, first.Element) || again.Pos != first.Pos {
			t.Fatalf("the pop retried through %s = %+v, want the first answer %+v marked Deduped", via, again, first)
		}
	}
	next, err := w.c.Node(w.copyNode).Pop(proofCtx(t), wire.PopRequest{StoreTarget: w.target()})
	if err != nil || !next.Found || bug0749Item(t, next.Element) != "b" {
		t.Fatalf("the next pop under a fresh id = %+v, %v, want b: the retries consumed another element", next, err)
	}
}

// TestAnOwnerKillDoesNotLetARetriedClaimLeaseTwice is the claim form: the retry is sent to
// the new owner directly, because this build does not forward a claim.
func TestAnOwnerKillDoesNotLetARetriedClaimLeaseTwice(t *testing.T) {
	w := bug0749Setup(t)
	request := [16]byte{0x49, 0x07, 2}
	first, err := w.c.Node(w.owner).Claim(proofCtx(t), wire.ClaimRequest{StoreTarget: w.target(), Count: 1}, wire.WithRequestID(request))
	if err != nil || first.ID == ([16]byte{}) || len(first.Elements) != 1 || bug0749Item(t, first.Elements[0]) != "a" {
		t.Fatalf("the first claim = %+v, %v, want a lease on a", first, err)
	}
	w.copyCaughtUp(t)
	w.killOwner(t)

	deadline := time.Now().Add(30 * time.Second)
	var again wire.Lease
	for {
		again, err = w.c.Node(w.copyNode).Claim(proofCtx(t), wire.ClaimRequest{StoreTarget: w.target(), Count: 1}, wire.WithRequestID(request))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the retried claim was never answered: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !again.Deduped || again.ID != first.ID || again.Until != first.Until {
		t.Fatalf("the retried claim = %+v, want the first lease %+v marked Deduped", again, first)
	}
	next, err := w.c.Node(w.copyNode).Claim(proofCtx(t), wire.ClaimRequest{StoreTarget: w.target(), Count: 1})
	if err != nil || len(next.Elements) != 1 || bug0749Item(t, next.Elements[0]) != "b" {
		t.Fatalf("the next claim under a fresh id = %+v, %v, want b: the retry leased another message", next, err)
	}
}

// TestARestartedCopyKeepsTheFirstPopsResultWhenItIsPromoted (BUG-0749 review S2): a copy
// applies a pop, restarts, is promoted, and the same request id is retried. A copy persists
// no applied position, so its boot replays the log records above the seed's cut; if that
// replay re-applied the pop it would remove a second element and rewrite the entry with
// that element, and the retry would be answered with b instead of a.
func TestARestartedCopyKeepsTheFirstPopsResultWhenItIsPromoted(t *testing.T) {
	w := bug0749Setup(t)
	request := [16]byte{0x49, 0x07, 3}
	first, err := w.c.Node(w.owner).Pop(proofCtx(t), wire.PopRequest{StoreTarget: w.target()}, wire.WithRequestID(request))
	if err != nil || !first.Found || bug0749Item(t, first.Element) != "a" {
		t.Fatalf("the first pop = %+v, %v, want element a", first, err)
	}
	w.copyCaughtUp(t)

	w.c.Kill(w.copyNode)
	w.c.Restart(w.copyNode)
	w.awaitActive(t, w.copyNode)
	w.copyCaughtUp(t)
	w.killOwner(t)

	again := w.retryPop(t, w.copyNode, request)
	if !again.Deduped || !bytes.Equal(again.Element, first.Element) || again.Pos != first.Pos {
		t.Fatalf("the pop retried on the restarted, promoted copy = %+v, want the first answer %+v marked Deduped", again, first)
	}
	remaining, err := w.c.Node(w.copyNode).Peek(proofCtx(t), wire.PeekRequest{StoreTarget: w.target(), Count: 10})
	if err != nil || len(remaining) != 2 || bug0749Item(t, remaining[0]) != "b" || bug0749Item(t, remaining[1]) != "c" {
		t.Fatalf("after the restart and the retry the field holds %d elements (%v), want b and c: the restart replayed the pop", len(remaining), err)
	}
}
