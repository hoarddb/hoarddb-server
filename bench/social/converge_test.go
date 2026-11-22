package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// digestWith is a sample digest whose only content is its hash, and one user's.
func digestWith(hash string) sampleDigest {
	return sampleDigest{hash: hash, perUser: map[int]string{7: hash}}
}

// TestConvergeWaitsThenPasses: a node that is behind for three comparisons passes
// on the fourth, and the time it took is recorded.
func TestConvergeWaitsThenPasses(t *testing.T) {
	var mu sync.Mutex
	probes := map[int]int{}
	probe := func(_ context.Context, node int) (sampleDigest, error) {
		mu.Lock()
		defer mu.Unlock()
		probes[node]++
		if node == 1 && probes[node] <= 3 {
			return digestWith("old"), nil
		}
		return digestWith("new"), nil
	}
	result, err := waitForConvergence(context.Background(), []string{"source", "replica", "other"}, probe, 2*time.Millisecond, time.Second)
	if err != nil || !result.Pass {
		t.Fatalf("result %+v, error %v; want a pass", result, err)
	}
	if result.Polls != 4 {
		t.Errorf("polls = %d; want 4 (behind for 3, level on the fourth)", result.Polls)
	}
	if result.ConvergeSeconds < 0.006 {
		t.Errorf("converge_seconds = %v; want at least the 3 pauses (6 ms) that the lagging node cost", result.ConvergeSeconds)
	}
	if lagging := result.Nodes[1].ConvergedAfterS; lagging == nil || *lagging < 0.006 {
		t.Errorf("replica converged_after_s = %v; want at least 6 ms", lagging)
	}
	if other := result.Nodes[2].ConvergedAfterS; other == nil || *other > *result.Nodes[1].ConvergedAfterS {
		t.Errorf("the node that was never behind reports %v; it must be no later than the lagging one", other)
	}
}

// TestConvergeFailsWhenANodeNeverCatchesUp: the check gives up at the timeout and
// names the node and the users it still returns differently.
func TestConvergeFailsWhenANodeNeverCatchesUp(t *testing.T) {
	probe := func(_ context.Context, node int) (sampleDigest, error) {
		if node == 2 {
			return digestWith("stale"), nil
		}
		return digestWith("current"), nil
	}
	result, err := waitForConvergence(context.Background(), []string{"source", "replica", "behind"}, probe, 3*time.Millisecond, 40*time.Millisecond)
	if err != nil || result.Pass {
		t.Fatalf("result %+v, error %v; want a failed check without an error", result, err)
	}
	if result.ConvergeSeconds < 0.04 || result.ConvergeSeconds > 0.5 {
		t.Errorf("gave up after %v s; want just past the 40 ms timeout", result.ConvergeSeconds)
	}
	failure := result.failure()
	if !strings.Contains(failure, "behind") || strings.Contains(failure, "replica (") {
		t.Errorf("failure %q must name the node that is behind and not the one that caught up", failure)
	}
	if len(result.Nodes[2].DifferingUsers) == 0 || result.Nodes[2].ConvergedAfterS != nil {
		t.Errorf("lagging node %+v; want differing users and no convergence time", result.Nodes[2])
	}
}

// TestConvergeToleratesANodeThatCannotYetBeRead: a recovering node refuses reads
// for a while. That is a node behind, not a failed check; the reference
// refusing is a failed check.
func TestConvergeToleratesANodeThatCannotYetBeRead(t *testing.T) {
	var mu sync.Mutex
	replicaProbes := 0
	probe := func(_ context.Context, node int) (sampleDigest, error) {
		mu.Lock()
		defer mu.Unlock()
		if node == 1 {
			if replicaProbes++; replicaProbes <= 2 {
				return sampleDigest{}, errors.New("not primary or secondary; cannot serve reads")
			}
		}
		return digestWith("level"), nil
	}
	result, err := waitForConvergence(context.Background(), []string{"source", "recovering"}, probe, time.Millisecond, time.Second)
	if err != nil || !result.Pass || result.Polls != 3 || result.Nodes[1].Error != "" {
		t.Errorf("result %+v, error %v; want a pass on the third comparison with the error cleared", result, err)
	}
	never := func(_ context.Context, node int) (sampleDigest, error) {
		if node == 1 {
			return sampleDigest{}, errors.New("connection refused")
		}
		return digestWith("level"), nil
	}
	result, err = waitForConvergence(context.Background(), []string{"source", "down"}, never, time.Millisecond, 10*time.Millisecond)
	if err != nil || result.Pass || !strings.Contains(result.failure(), "connection refused") {
		t.Errorf("a node that never answers: result %+v, error %v; want a failed check naming the cause", result, err)
	}
	_, err = waitForConvergence(context.Background(), []string{"source", "other"}, func(_ context.Context, node int) (sampleDigest, error) {
		if node == 0 {
			return sampleDigest{}, errors.New("reference down")
		}
		return digestWith("x"), nil
	}, time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Error("an unreadable reference was treated as a lagging node")
	}
}

// TestConvergeComparesEveryNode: with the first replica level and the second
// behind, the check fails. A check that compared only the first replica passes.
func TestConvergeComparesEveryNode(t *testing.T) {
	probe := func(_ context.Context, node int) (sampleDigest, error) {
		if node == 2 {
			return digestWith("behind"), nil
		}
		return digestWith("level"), nil
	}
	result, err := waitForConvergence(context.Background(), []string{"a", "b", "c"}, probe, time.Millisecond, 10*time.Millisecond)
	if err != nil || result.Pass {
		t.Errorf("result %+v, error %v; one replica behind must fail the check", result, err)
	}
}

func TestConvergeStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe := func(context.Context, int) (sampleDigest, error) { return digestWith("x"), nil }
	probe2 := func(_ context.Context, node int) (sampleDigest, error) {
		return digestWith(string(rune('a' + node))), nil
	}
	if result, err := waitForConvergence(ctx, []string{"a"}, probe, time.Millisecond, time.Second); err != nil || !result.Pass {
		t.Errorf("a single node is level with itself: %+v, %v", result, err)
	}
	if _, err := waitForConvergence(ctx, []string{"a", "b"}, probe2, time.Hour, time.Hour); err == nil {
		t.Error("a cancelled wait returned no error")
	}
}

// TestNodeConfigsPointAtOneNodeEach: the convergence check reads each node on
// its own; a config that still spread reads over the cluster would hide a node
// that is behind.
func TestNodeConfigsPointAtOneNodeEach(t *testing.T) {
	hoard, err := nodeConfigs(context.Background(), &config{Engine: engineHoardDB, Addrs: []string{"a:1", "b:1", "c:1"}, Routing: routingB})
	if err != nil || len(hoard) != 3 {
		t.Fatalf("hoardDB node configs = %d, %v; want one per node", len(hoard), err)
	}
	for index, want := range []string{"a:1", "b:1", "c:1"} {
		if len(hoard[index].Addrs) != 1 || hoard[index].Addrs[0] != want || hoard[index].Routing != routingA {
			t.Errorf("node %d config addrs=%v routing=%q; want only %s under RP-A", index, hoard[index].Addrs, hoard[index].Routing, want)
		}
	}
	mysqlNodes, err := nodeConfigs(context.Background(), &config{Engine: engineMySQL, MySQLDSN: "u@tcp(a:1)/s,u@tcp(b:1)/s", Routing: routingB})
	if err != nil || len(mysqlNodes) != 2 {
		t.Fatalf("MySQL node configs = %d, %v; want one per server", len(mysqlNodes), err)
	}
	for index, want := range []string{"u@tcp(a:1)/s", "u@tcp(b:1)/s"} {
		if mysqlNodes[index].MySQLDSN != want || mysqlNodes[index].mysqlReadRouting() != routingA {
			t.Errorf("server %d config dsn=%q routing=%q; want only that server, reads from it alone", index, mysqlNodes[index].MySQLDSN, mysqlNodes[index].mysqlReadRouting())
		}
	}
}

// TestMongoNodeOptionsConnectDirectly: one member, no discovery.
func TestMongoNodeOptionsConnectDirectly(t *testing.T) {
	concern, err := mongoWriteConcern("w1j")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config{MongoURI: "mongodb://a:1,b:1,c:1/?replicaSet=rs", MongoNode: "b:1", Conc: 2}
	clientOptions := mongoClientOptions(cfg, concern, readpref.Nearest())
	if len(clientOptions.Hosts) != 1 || clientOptions.Hosts[0] != "b:1" || clientOptions.Direct == nil || !*clientOptions.Direct || clientOptions.ReplicaSet != nil {
		t.Errorf("options hosts=%v direct=%v replicaSet=%v; want b:1 only, direct, no set name", clientOptions.Hosts, clientOptions.Direct, clientOptions.ReplicaSet)
	}
}

// seededPair is two fake servers seeded identically, as two nodes of a cluster that has caught up.
func seededPair(t *testing.T) (*socialWorld, *socialWorld) {
	t.Helper()
	return newFakeWorld(t), newFakeWorld(t)
}

// dropOneLike removes a like from a fake server, as a replica that missed it.
func dropOneLike(t *testing.T, world *socialWorld) {
	t.Helper()
	for address, store := range world.fake.stores {
		if strings.HasSuffix(address, "|Likes") && len(store.entries) > 0 {
			store.entries = store.entries[1:]
			return
		}
	}
	t.Fatal("the fake server holds no like to drop")
}

// TestConvergeOnEnginesComparesTheNodesContent: two nodes holding the same data
// pass at once; a node missing one like fails at the timeout and is named.
func TestConvergeOnEnginesComparesTheNodesContent(t *testing.T) {
	source, replica := seededPair(t)
	cfg := &config{Engine: engineHoardDB, SampleUsers: tinySpec.Users, Conc: 2, ConvergePoll: time.Millisecond, ConvergeTimeout: 30 * time.Millisecond}
	var out bytes.Buffer
	if err := convergeOn(context.Background(), cfg, tinySpec, []socialEngine{source.engine, replica.engine}, &out); err != nil {
		t.Fatalf("identical nodes failed to converge: %v", err)
	}
	var printed convergeResult
	if err := json.Unmarshal(out.Bytes(), &printed); err != nil || !printed.Pass || printed.Mode != modeConverge || printed.SampleUsers != tinySpec.Users {
		t.Errorf("printed %q (%v); want one passing converge line over %d users", out.String(), err, tinySpec.Users)
	}
	dropOneLike(t, replica)
	out.Reset()
	err := convergeOn(context.Background(), cfg, tinySpec, []socialEngine{source.engine, replica.engine}, &out)
	if err == nil || !strings.Contains(err.Error(), "convergence failed") {
		t.Errorf("a node missing a like converged: %v", err)
	}
	if !strings.Contains(out.String(), `"pass":false`) {
		t.Errorf("the failing result must still be printed, got %q", out.String())
	}
}
