package ring

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func threeNodes(t *testing.T) *Ring {
	t.Helper()
	r := New(3)
	for i := 1; i <= 3; i++ {
		n := Node{ID: fmt.Sprintf("node%d", i), Addr: fmt.Sprintf("127.0.0.1:500%d", i)}
		if err := r.AddNode(n); err != nil {
			t.Fatalf("AddNode(%s): %v", n.ID, err)
		}
	}
	return r
}

func TestEmptyRing(t *testing.T) {
	r := New(3)
	if _, err := r.Owner("bucket"); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("Owner on empty ring: want ErrNoNodes, got %v", err)
	}
	if _, err := r.Replicas("bucket", 3); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("Replicas on empty ring: want ErrNoNodes, got %v", err)
	}
	if r.IsReplicaSetComplete(3) {
		t.Fatal("empty ring must not report a complete replica set")
	}
}

func TestOwnerIsDeterministic(t *testing.T) {
	r := threeNodes(t)
	first, err := r.OwnerID("users")
	if err != nil {
		t.Fatalf("OwnerID: %v", err)
	}
	if !r.Has(first) {
		t.Fatalf("owner %q is not a member of the ring", first)
	}
	// Same lookup must be stable across repeated calls.
	for i := 0; i < 50; i++ {
		got, err := r.OwnerID("users")
		if err != nil {
			t.Fatalf("OwnerID: %v", err)
		}
		if got != first {
			t.Fatalf("owner changed between calls: %q then %q", first, got)
		}
	}
}

func TestReplicasOwnerFirstAndDistinct(t *testing.T) {
	r := threeNodes(t)
	owner, err := r.OwnerID("users")
	if err != nil {
		t.Fatalf("OwnerID: %v", err)
	}
	replicas, err := r.Replicas("users", 3)
	if err != nil {
		t.Fatalf("Replicas: %v", err)
	}
	if len(replicas) != 3 {
		t.Fatalf("RF=3 on a 3-node ring: want 3 replicas, got %d", len(replicas))
	}
	if replicas[0].ID != owner {
		t.Fatalf("first replica must be the owner: got %q, want %q", replicas[0].ID, owner)
	}
	seen := map[string]bool{}
	for _, n := range replicas {
		if seen[n.ID] {
			t.Fatalf("duplicate replica %q in %v", n.ID, replicas)
		}
		seen[n.ID] = true
		if n.Addr == "" {
			t.Fatalf("replica %q has no address; it could never be dialed", n.ID)
		}
	}
}

func TestReplicasClampToNodeCount(t *testing.T) {
	r := threeNodes(t)
	replicas, err := r.Replicas("users", 5)
	if err != nil {
		t.Fatalf("Replicas: %v", err)
	}
	if len(replicas) != 3 {
		t.Fatalf("asked for 5 replicas on a 3-node ring: want 3, got %d", len(replicas))
	}
	if r.IsReplicaSetComplete(5) {
		t.Fatal("a 3-node ring cannot satisfy RF=5")
	}
	if !r.IsReplicaSetComplete(3) {
		t.Fatal("a 3-node ring satisfies RF=3")
	}
}

func TestRF2IsPrefixOfRF3(t *testing.T) {
	// RF=2 must be a prefix of RF=3 for the same key: raising the replication
	// factor adds a copy, it does not move existing ones.
	r := threeNodes(t)
	two, err := r.Replicas("orders", 2)
	if err != nil {
		t.Fatalf("Replicas(2): %v", err)
	}
	three, err := r.Replicas("orders", 3)
	if err != nil {
		t.Fatalf("Replicas(3): %v", err)
	}
	for i, n := range two {
		if three[i].ID != n.ID {
			t.Fatalf("RF=2 is not a prefix of RF=3 at index %d: %v vs %v", i, two, three)
		}
	}
}

func TestPeersExcludesSelf(t *testing.T) {
	r := threeNodes(t)
	owner, err := r.OwnerID("users")
	if err != nil {
		t.Fatalf("OwnerID: %v", err)
	}
	peers, err := r.Peers("users", owner, 3)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("owner with RF=3 on 3 nodes: want 2 peers, got %d", len(peers))
	}
	for _, p := range peers {
		if p.ID == owner {
			t.Fatalf("Peers returned the excluded node %q", owner)
		}
	}
}

func TestDistribution(t *testing.T) {
	r := threeNodes(t)
	counts := map[string]int{}
	const keys = 3000
	for i := 0; i < keys; i++ {
		id, err := r.OwnerID(fmt.Sprintf("bucket-%d", i))
		if err != nil {
			t.Fatalf("OwnerID: %v", err)
		}
		counts[id]++
	}
	if len(counts) != 3 {
		t.Fatalf("expected all 3 nodes to own keys, got %d: %v", len(counts), counts)
	}
	// With 3000 keys over 3 nodes a uniform split is 1000 each. Allow a wide
	// 3x band: this test guards against a broken ring (everything on one node),
	// not against hash imbalance.
	for id, c := range counts {
		if c < keys/9 || c > keys/9*5 {
			t.Fatalf("node %s owns %d of %d keys, outside the sanity band: %v", id, c, keys, counts)
		}
	}
}

func TestRemovalMovesOnlyItsOwnKeys(t *testing.T) {
	// This is the property that makes consistent hashing worth using: when a
	// node leaves, keys owned by the surviving nodes must not move.
	r := threeNodes(t)
	const keys = 500
	before := map[string]string{}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("bucket-%d", i)
		id, err := r.OwnerID(k)
		if err != nil {
			t.Fatalf("OwnerID: %v", err)
		}
		before[k] = id
	}

	r.RemoveNode("node2")
	if r.Has("node2") {
		t.Fatal("node2 still reports as a member after removal")
	}
	if r.Len() != 2 {
		t.Fatalf("want 2 nodes after removal, got %d", r.Len())
	}

	movedFromRemoved := 0
	stillOnRemoved := 0
	movedSurvivors := 0
	for k, oldOwner := range before {
		newOwner, err := r.OwnerID(k)
		if err != nil {
			t.Fatalf("OwnerID: %v", err)
		}
		switch oldOwner {
		case "node2":
			// Keys the departed node owned must move to a survivor.
			if newOwner == "node2" {
				stillOnRemoved++
			} else {
				movedFromRemoved++
			}
		default:
			// Keys owned by a surviving node must not move.
			if newOwner != oldOwner {
				movedSurvivors++
			}
		}
	}
	if stillOnRemoved != 0 {
		t.Fatalf("%d keys are still owned by the removed node", stillOnRemoved)
	}
	if movedFromRemoved == 0 {
		t.Fatal("no keys were reassigned after removing a node; the ring did not change")
	}
	if movedSurvivors != 0 {
		t.Fatalf("%d keys changed owner among surviving nodes; only the removed node's keys may move", movedSurvivors)
	}
}

func TestRemoveUnknownNodeIsNoop(t *testing.T) {
	r := threeNodes(t)
	owner, _ := r.OwnerID("users")
	r.RemoveNode("does-not-exist")
	if r.Len() != 3 {
		t.Fatalf("removing an unknown node changed membership: len=%d", r.Len())
	}
	if after, _ := r.OwnerID("users"); after != owner {
		t.Fatalf("removing an unknown node changed placement: %q then %q", owner, after)
	}
}

func TestAddNodeUpdatesAddress(t *testing.T) {
	r := threeNodes(t)
	owner, _ := r.OwnerID("users")
	if err := r.AddNode(Node{ID: owner, Addr: "192.0.2.9:4433"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if r.Len() != 3 {
		t.Fatalf("re-adding an existing node changed membership: len=%d", r.Len())
	}
	replicas, err := r.Replicas("users", 3)
	if err != nil {
		t.Fatalf("Replicas: %v", err)
	}
	if replicas[0].Addr != "192.0.2.9:4433" {
		t.Fatalf("address not updated: got %q", replicas[0].Addr)
	}
	if after, _ := r.OwnerID("users"); after != owner {
		t.Fatalf("updating an address changed placement: %q then %q", owner, after)
	}
}

func TestEmptyNodeIDRejected(t *testing.T) {
	r := New(3)
	if err := r.AddNode(Node{Addr: "127.0.0.1:4433"}); err == nil {
		t.Fatal("AddNode accepted an empty node ID")
	}
}

func TestConcurrentReadsDuringMembershipChange(t *testing.T) {
	r := threeNodes(t)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := r.Replicas(fmt.Sprintf("bucket-%d", worker), 2); err != nil {
					// Only ErrNoNodes is acceptable, and only if the ring is
					// briefly empty, which this test never does.
					t.Errorf("Replicas during rebuild: %v", err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("extra%d", i)
		if err := r.AddNode(Node{ID: id, Addr: fmt.Sprintf("127.0.0.1:60%02d", i)}); err != nil {
			t.Fatalf("add node %s during concurrent reads: %v", id, err)
		}
		r.RemoveNode(id)
	}
	close(stop)
	wg.Wait()
}

func TestReplicasIsAPrefixOfTheNextFactor(t *testing.T) {
	// The whole per-bucket replication design rests on this: for a fixed member
	// set, Replicas(key, n) is a prefix of Replicas(key, n+1). If it holds, an
	// RF increase appends holders, an RF decrease truncates, and the owner —
	// Replicas(key, n)[0] — never moves. If it stops holding, every RF change
	// silently relocates live data.
	r := New(3)
	for i := 1; i <= 5; i++ {
		node := Node{ID: fmt.Sprintf("node%d", i), Addr: fmt.Sprintf("127.0.0.1:500%d", i)}
		if err := r.AddNode(node); err != nil {
			t.Fatalf("AddNode(%s): %v", node.ID, err)
		}
	}

	for keyIndex := 0; keyIndex < 1000; keyIndex++ {
		bucketKey := fmt.Sprintf("bucket-%04d", keyIndex)
		for factor := 1; factor <= 4; factor++ {
			smaller, err := r.Replicas(bucketKey, factor)
			if err != nil {
				t.Fatalf("Replicas(%q, %d): %v", bucketKey, factor, err)
			}
			larger, err := r.Replicas(bucketKey, factor+1)
			if err != nil {
				t.Fatalf("Replicas(%q, %d): %v", bucketKey, factor+1, err)
			}
			if len(smaller) != factor {
				t.Fatalf("Replicas(%q, %d) returned %d nodes", bucketKey, factor, len(smaller))
			}
			if len(larger) != factor+1 {
				t.Fatalf("Replicas(%q, %d) returned %d nodes", bucketKey, factor+1, len(larger))
			}
			for position := range smaller {
				if smaller[position].ID != larger[position].ID {
					t.Fatalf("Replicas(%q, %d) is not a prefix of Replicas(%q, %d) at index %d: %s vs %s",
						bucketKey, factor, bucketKey, factor+1, position,
						smaller[position].ID, larger[position].ID)
				}
			}
		}
	}

	clamped, err := r.Replicas("bucket-0000", 9)
	if err != nil {
		t.Fatalf("Replicas(clamped): %v", err)
	}
	if len(clamped) != 5 {
		t.Fatalf("a 5-node ring asked for 9 replicas: want 5, got %d", len(clamped))
	}
}

// TestOnlyIsTheOwnerOfAOneMemberRing pins Only to Owner's answer: on a ring of
// one member it is that member for every key, and on an empty ring or a ring of
// more it answers nothing.
func TestOnlyIsTheOwnerOfAOneMemberRing(t *testing.T) {
	if _, alone := New(3).Only(); alone {
		t.Error("an empty ring answered a member")
	}
	if _, alone := threeNodes(t).Only(); alone {
		t.Error("a ring of three answered one member")
	}
	r := New(3)
	lone := Node{ID: "node1", Addr: "127.0.0.1:5001"}
	if err := r.AddNode(lone); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	only, alone := r.Only()
	if !alone || only != lone {
		t.Fatalf("a one-member ring answered %v, %v; want %v", only, alone, lone)
	}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("db.bucket-%d", i)
		if owner, err := r.Owner(key); err != nil || owner != only {
			t.Fatalf("Owner(%q) = %v, %v; Only answered %v", key, owner, err, only)
		}
	}
}
