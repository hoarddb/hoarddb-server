package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 4 of the spread MVP on a cluster: the drop through every
// door, with a part owner down, removed, or a router that has not taken its
// template over (spread MVP note, section 2.3, test 10, S4-1 to S4-5).

// dropOn sends a client's drop of the collection through node index.
func (sc *spreadCluster) dropOn(index int) *transport.Frame {
	sc.t.Helper()
	return sc.frame(index, transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: sc.collection})
}

// requireGoneEverywhere waits until no node holds the template or any of its
// parts, and no node's bucket listing names the collection.
func (sc *spreadCluster) requireGoneEverywhere(when string) {
	sc.t.Helper()
	for index, node := range sc.nodes {
		if node.server == nil {
			continue
		}
		core := node.server.core
		spreadEventually(sc.t, 30*time.Second, fmt.Sprintf("node %d holds nothing of %s %s", index+1, sc.collection, when), func() bool {
			return !core.bucketStoreOpen("app", sc.collection) && partsHeldOf(core, sc.collection) == 0
		})
		reply := sc.frame(index, transport.OpListBuckets, ListBucketsRequest{Database: "app"})
		if reply.Opcode == transport.OpError || strings.Contains(string(reply.Payload), sc.collection) {
			sc.t.Errorf("node %d lists %s after its drop %s: %s %q", index+1, sc.collection, when, reply.Opcode, reply.Payload)
		}
	}
}

// seedingWriteConcern is the write-concern wait of a fixture whose seeding writes
// must not be refused for a slow second copy.
const seedingWriteConcern = 15 * time.Second

// TestDropThroughEveryDoor: a collection with documents in many parts, on
// three nodes at factor 2, is dropped through its router, a template replica
// and a node that holds no copy; afterwards no node holds the template or a
// part, none lists it, and every node records the template's drop, the ones
// that hold only parts of it included (S4-4: applied through dropOfNoCopy).
func TestDropThroughEveryDoor(t *testing.T) {
	for _, door := range []string{"the router", "a template replica", "a node with no copy"} {
		t.Run(door, func(t *testing.T) {
			// The seeding batches make every part at majority, and on a busy host a part's
			// create can take longer than the default five seconds to reach its second
			// copy. The seeding is fixture setup, so the concern waits longer here (under
			// the 20 s a test client waits for its answer); no assertion below times a write.
			tc := newTestClusterTuned(t, 3, 2, nil, func(_ int, c *Cluster) { c.wcTimeout = seedingWriteConcern })
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
			sc := &spreadCluster{testCluster: tc, collection: "User"}
			sc.createCollection(0, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}})
			set := sc.templateSet("User")
			index := map[string]int{"the router": set[0], "a template replica": set[1], "a node with no copy": sc.nonHolder("User")}[door]
			sc.insertMany(set[0], "doc", 600)
			incarnations := make([]uint64, len(sc.nodes))
			for i, node := range sc.nodes {
				if state, err := node.server.core.bucketStateNamed("app", "User"); err == nil {
					incarnations[i] = state.Incarnation
				}
			}
			requireDropped(t, sc.dropOn(index))
			sc.requireGoneEverywhere("through " + door)
			for i, node := range sc.nodes {
				if incarnations[i] == 0 {
					continue
				}
				spreadEventually(t, 30*time.Second, fmt.Sprintf("node %d records the template's drop", i+1), func() bool {
					recorded, err := node.server.core.dropRecorded("app", "User", incarnations[i])
					return err == nil && recorded
				})
			}
			// The template's drop reaches a node outside its set after the drop is answered
			// (its concern is the set's), so the record is waited for; one never written
			// still fails, at the bound.
			if holder := sc.nonHolder("User"); holder >= 0 {
				spreadEventually(t, 30*time.Second, "the node holding only parts records the template's drop", func() bool {
					recorded, err := sc.nodes[holder].server.core.droppedIncarnations("app", "User")
					return err == nil && len(recorded) > 0
				})
			}
			// The name is free, and the new collection is empty on every node.
			sc.createCollection(set[0], "User", nil)
			for i := range sc.nodes {
				if count, refused := sc.count(i); refused != "" || count != 0 {
					t.Errorf("count() through node %d after the re-create is %d (refused %q), want 0", i+1, count, refused)
				}
			}
		})
	}
}

// TestDropOnAOneMemberCluster is the one-member door of the router.
func TestDropOnAOneMemberCluster(t *testing.T) {
	sc := newSpreadCluster(t, 1, 1, "User", nil)
	sc.insertMany(0, "doc", 300)
	core := sc.nodes[0].server.core
	if partsHeldOf(core, "User") == 0 {
		t.Fatal("no part was made")
	}
	requireDropped(t, sc.dropOn(0))
	sc.requireGoneEverywhere("on one node")
	sc.createCollection(0, "User", nil)
	if count, refused := sc.count(0); refused != "" || count != 0 {
		t.Fatalf("count() after the re-create is %d (refused %q), want 0", count, refused)
	}
}

// TestDropResumesAfterAPartOwnerReturns: a part owner is down, the drop is
// refused with the retry line (exactly), every operation answers "being
// dropped" through every node, and once the owner is back a second drop
// finishes and nothing comes back after a re-create.
func TestDropResumesAfterAPartOwnerReturns(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "User", nil)
	router := sc.templateSet("User")[0]
	keys := sc.insertMany(router, "doc", 600)
	down := (router + 1) % 3
	base := sc.stopNodeKeepingData(down)
	requireRefusedWith(t, "the drop with a part owner down", sc.dropOn(router), droppingText)
	if !sc.nodes[router].server.core.bucketStoreOpen("app", "User") {
		t.Fatal("the template was dropped before every part was")
	}
	other := 3 - router - down
	for _, via := range []int{router, other} {
		requireRefusedWith(t, "a get of a collection being dropped", sc.frame(via, transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: keys[0]}), droppingText)
		requireRefusedWith(t, "a put into a collection being dropped", sc.frame(via, transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "late", Data: map[string]interface{}{"N": 1}}), droppingText)
		requireRefusedWith(t, "a count of a collection being dropped", sc.frame(via, transport.OpCount, CountRequest{Database: "app", Bucket: "User"}), droppingText)
	}
	sc.nodes[down] = sc.startNode(down, base, sc.token)
	spreadEventually(t, 60*time.Second, "the second drop finishes", func() bool {
		reply := sc.dropOn(router)
		return reply.Opcode != transport.OpError
	})
	sc.requireGoneEverywhere("after the resume")
	sc.createCollection(router, "User", nil)
	for _, key := range keys[:50] {
		if reply := sc.put(router, key, map[string]interface{}{"Gen": "new"}); reply.Opcode == transport.OpError {
			t.Fatalf("insert %s after the re-create: %s", key, refusalTextOrEmptyTB(t, reply))
		}
	}
	for index := range sc.nodes {
		spreadEventually(t, 30*time.Second, fmt.Sprintf("count() through node %d is the new writes only", index+1), func() bool {
			count, refused := sc.count(index)
			return refused == "" && count == 50
		})
	}
}

// TestDropWithAPartOwnerStoppedThenRemoved: the owner of parts is stopped and
// then removed from the cluster, at factor 1, so its parts are gone with it.
// The drop is refused while it is only stopped, and finishes after the
// removal: a part its new owner does not hold counts as dropped.
func TestDropWithAPartOwnerStoppedThenRemoved(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "User", nil)
	router := sc.templateSet("User")[0]
	sc.insertMany(router, "doc", 600)
	victim := (router + 1) % 3
	third := 3 - router - victim
	sc.stopNodeKeepingData(victim)
	requireRefusedWith(t, "the drop with a part owner stopped", sc.dropOn(router), droppingText)
	removeNodeFrom(t, sc.testCluster, third, victim)
	spreadEventually(t, 60*time.Second, "the drop finishes after the removal", func() bool {
		return sc.dropOn(router).Opcode != transport.OpError
	})
	sc.requireGoneEverywhere("after the removal")
}

// TestDropWithAPartOwnerRemovedLive: a running node is removed from the
// cluster, and the drop that follows reaches every part its new owner holds.
func TestDropWithAPartOwnerRemovedLive(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	router := sc.templateSet("User")[0]
	sc.insertMany(router, "doc", 600)
	victim := (router + 1) % 3
	third := 3 - router - victim
	removeNodeFrom(t, sc.testCluster, third, victim)
	spreadEventually(t, 60*time.Second, "the drop after the live removal", func() bool {
		owner, err := clusterOf(sc.testCluster, third).Owner(placementOf("app", "User"))
		if err != nil {
			return false
		}
		return sc.dropOn(sc.indexOf(owner.ID)).Opcode != transport.OpError
	})
	for _, index := range []int{router, third} {
		core := sc.nodes[index].server.core
		spreadEventually(t, 30*time.Second, fmt.Sprintf("node %d holds nothing of User", index+1), func() bool {
			return !core.bucketStoreOpen("app", "User") && partsHeldOf(core, "User") == 0
		})
	}
}

// TestDropAtARouterThatHasNotTakenOverIsRetried: a drop that reaches a
// template owner which has not taken the template over is refused with the
// hand-off, nothing marked and nothing dropped (its born set may lack parts
// the previous owner marked: dropping from it would leave them behind). Once
// the take-over lands the drop finishes, and the part the lagging born set
// lacked is gone too.
func TestDropAtARouterThatHasNotTakenOverIsRetried(t *testing.T) {
	sc, name, victim, lag, other := unsettledRouter(t)
	sc.collection = name
	requireHandOff(t, sc.dropOn(other), "a drop at the unsettled router")
	if state, err := sc.nodes[lag].server.core.readBornSet("app", name); err != nil || state.dropping() {
		t.Fatalf("the unsettled router marked the collection dropping: %v %v", state.dropping(), err)
	}
	takeOverPausedHook.Store(nil)
	clusterOf(sc.testCluster, lag).signalTakeOver()
	waitTakenOver(t, sc.nodes[lag].server.core, "app", name)
	spreadEventually(t, 60*time.Second, "the drop after the take-over", func() bool {
		return sc.dropOn(other).Opcode != transport.OpError
	})
	part := partName(name, spreadPartOf(t, victim))
	for _, index := range []int{lag, other} {
		core := sc.nodes[index].server.core
		spreadEventually(t, 30*time.Second, fmt.Sprintf("node %d holds neither %s nor its part", index+1, name), func() bool {
			return !core.bucketStoreOpen("app", name) && !core.bucketStoreOpen("app", part)
		})
	}
}

// TestDropRacingFanOutsNeverDeadlocks is the step 3 slot rule with the real
// drop: a drop takes the gate exclusively while fan-outs hold it shared and
// each holds a forward slot, on a pool of four. Sixteen clients count() the
// collection through all three nodes while it is dropped, re-created and
// refilled; every call answers (or is refused), no slot is left held, and no
// gate entry remains. A drop answered the retry line is sent again, as the
// line says, a bounded number of times.
func TestDropRacingFanOutsNeverDeadlocks(t *testing.T) {
	// How many times a round sends the drop again when it is answered the retry line.
	const dropRetries = 20
	tc := newTestClusterWith(t, 3, 2, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 4 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: "User"}
	sc.createCollection(0, "User", nil)
	sc.insertMany(0, "doc", 400)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for client := 0; client < 16; client++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			conn := tc.dial(client % 3)
			defer func() { _ = conn.Close() }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				op, request := transport.OpCount, any(CountRequest{Database: "app", Bucket: "User"})
				if client%4 == 3 {
					op, request = transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 50}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				_, err := conn.SendRequest(ctx, op, spreadMarshal(t, request))
				cancel()
				if err != nil {
					t.Errorf("a fan-out was not answered: %v", err)
					return
				}
			}
		}(client)
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for round := 0; round < 6; round++ {
			reply := sc.dropOn(round % 3)
			// A drop that stopped partway under the fan-outs answers the retry line, and a
			// second drop finishes it: that is the product's contract, so the client here
			// does what the line says. Any other refusal, and a drop still unfinished after
			// the retries, fails the round.
			for retry := 0; retry < dropRetries && refusalTextOrEmptyTB(t, reply) == droppingText; retry++ {
				time.Sleep(100 * time.Millisecond)
				reply = sc.dropOn(round % 3)
			}
			if reply.Opcode == transport.OpError {
				t.Errorf("round %d: the drop was refused: %s", round, refusalTextOrEmptyTB(t, reply))
				return
			}
			sc.createCollection(0, "User", nil)
			sc.insertMany(0, "doc", 100)
		}
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Minute):
		t.Fatal("drops racing fan-outs did not finish: the gate and the slots deadlocked")
	}
	close(stop)
	wg.Wait()
	for index := range tc.nodes {
		core := tc.nodes[index].server.core
		spreadEventually(t, 30*time.Second, fmt.Sprintf("node %d gives every slot and gate back", index+1), func() bool {
			return core.queryInFlight() == 0 && core.forwardInFlight() == 0 && core.routerGates.inFlight() == 0
		})
	}
}
