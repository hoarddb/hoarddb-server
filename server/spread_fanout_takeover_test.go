package server

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The query slots across a take-over (the step 3 verifier's blocker 2): a part
// read that meets its part's take-over is answered on demand, which can wait
// out a bound and send the read to the part's previous owner. It must hold no
// query slot while it waits, or a fan-out that holds the forward pool and a
// part read that holds the execute pool wait on each other for ever.

// takeOverFanOuts is a three-node cluster at factor 2 with a query pool of
// pool, a spread collection routed by each of routers (node indexes, never
// node 3), node 3 removed (stopped first unless live), and every part's
// take-over held on every node, so each part read of a part that moved meets
// it. It sends perDoor concurrent count() and find() calls on each collection
// through every remaining node, each on its own connection, and returns those
// that did not answer within the bound, after which it lets the take-overs go.
func takeOverFanOuts(t *testing.T, pool int, routers []int, live bool, perDoor int) (hung int) {
	t.Helper()
	tc := newTestClusterWith(t, 3, 2, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = pool })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	const victim = 2
	names := map[int]string{}
	for i := 0; len(names) < len(routers); i++ {
		name := fmt.Sprintf("T%d", i)
		owner, err := clusterOf(tc, 0).Owner(placementOf("app", name))
		if err != nil {
			t.Fatal(err)
		}
		index := nodeIndexByAddr(t, tc.addrs, owner.ID)
		for _, router := range routers {
			if index == router && names[index] == "" {
				names[index] = name
			}
		}
	}
	for _, name := range names {
		sc := &spreadCluster{testCluster: tc, collection: name}
		sc.createCollection(0, name, nil)
		sc.insertMany(routers[0], "doc", 1500)
	}
	release := make(chan struct{})
	var once sync.Once
	letGo := func() { once.Do(func() { close(release) }) }
	var held sync.Map
	hook := func(nodeID, database, bucket string) {
		if hasPartSeparator(bucket) {
			held.Store(nodeID+"/"+bucket, true)
			<-release
		}
	}
	takeOverBeforeOpenHook.Store(&hook)
	t.Cleanup(func() { letGo(); takeOverBeforeOpenHook.Store(nil) })
	if !live {
		tc.stopNodeKeepingData(victim)
	}
	removeNodeFrom(t, tc, routers[0], victim)
	spreadEventually(t, 30*time.Second, "a part's take-over is held", func() bool {
		found := false
		held.Range(func(any, any) bool { found = true; return false })
		return found
	})
	type call struct {
		client *transport.DriverClient
		op     transport.Opcode
		req    any
		label  string
	}
	var calls []call
	for _, name := range names {
		for _, door := range []int{0, 1} {
			for i := 0; i < perDoor; i++ {
				client := tc.dial(door)
				t.Cleanup(func() { _ = client.Close() })
				op, req := transport.OpCount, any(CountRequest{Database: "app", Bucket: name})
				if i%2 == 1 {
					op, req = transport.OpScan, ScanRequest{Database: "app", Bucket: name, Sort: map[string]int{"N": -1}, Limit: 20}
				}
				calls = append(calls, call{client: client, op: op, req: req, label: fmt.Sprintf("%s of %s through node %d", op, name, door+1)})
			}
		}
	}
	answered := make(chan string, len(calls))
	for _, c := range calls {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			reply, err := c.client.SendRequest(ctx, c.op, spreadMarshal(t, c.req))
			switch {
			case err != nil:
				answered <- fmt.Sprintf("%s: %v", c.label, err)
			default:
				answered <- fmt.Sprintf("%s: %s %.160q", c.label, reply.Opcode, refusalTextOrEmptyTB(t, reply))
			}
		}()
	}
	bound := time.After(60 * time.Second)
	for i := range calls {
		select {
		case answer := <-answered:
			t.Log(answer)
		case <-bound:
			buf := make([]byte, 32<<20)
			stacks := string(buf[:runtime.Stack(buf, true)])
			for _, g := range strings.Split(stacks, "\n\n") {
				if strings.Contains(g, "queryGate).acquire") {
					t.Logf("waiting on a query slot:\n%s", g)
				}
			}
			letGo()
			return len(calls) - i
		}
	}
	letGo()
	for index := 0; index < 2; index++ {
		spreadEventually(t, 30*time.Second, fmt.Sprintf("node %d gives every query slot back", index+1), func() bool {
			s := tc.nodes[index].server.core
			return s.queryInFlight() == 0 && s.forwardInFlight() == 0
		})
	}
	return 0
}

// TestFanOutAtATakeOverPoolOneLiveRemoval is the verifier's reproduction: a
// pool of one, a live node removed, one count() at the router while its
// part's take-over is held. On the parent it never answered: the part read
// held the one execute slot and waited for the one forward slot, which the
// fan-out held.
func TestFanOutAtATakeOverPoolOneLiveRemoval(t *testing.T) {
	if hung := takeOverFanOuts(t, 1, []int{0}, true, 1); hung > 0 {
		t.Fatalf("%d fan-outs did not answer within 60 s with a pool of 1", hung)
	}
}

// TestFanOutAtATakeOverPoolOneStoppedNode is the same with the removed node
// stopped first.
func TestFanOutAtATakeOverPoolOneStoppedNode(t *testing.T) {
	if hung := takeOverFanOuts(t, 1, []int{0}, false, 1); hung > 0 {
		t.Fatalf("%d fan-outs did not answer within 60 s with a pool of 1", hung)
	}
}

// TestAFanOutsForwardTakesNoSecondForwardSlot: a read sent on under a
// fan-out's admission (a part read's on-demand forward to the part's previous
// owner) is answered while every forward slot of its node is held, as the
// fan-out's own slot is while it waits: it takes none of its own.
func TestAFanOutsForwardTakesNoSecondForwardSlot(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 1 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	c := clusterOf(tc, 0)
	var bucket string
	var owner = tc.addrs[1]
	for i := 0; bucket == ""; i++ {
		name := fmt.Sprintf("W%d", i)
		if node, err := c.Owner(placementOf("app", name)); err == nil && node.ID == owner {
			bucket = name
		}
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash, Options: map[string]interface{}{"placement": "whole"}})
	gate := c.s.queryGate()
	release, err := gate.acquire(context.Background(), gate.forwardPool())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ownerNode, err := c.Owner(placementOf("app", bucket))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(withFanOutAdmitted(context.Background()), 3*time.Second)
	defer cancel()
	reply, err := c.forwardRead(ctx, transport.NewFrame(transport.OpCount, spreadMarshal(t, CountRequest{Database: "app", Bucket: bucket})), ownerNode, bucket, clientReadForward)
	if err != nil || reply.Opcode != transport.OpCount {
		t.Fatalf("a forward under a fan-out's admission, with the forward pool full, answered %v %v: it waited for a slot of its own", reply, err)
	}
	if hook := reply.TakeWrittenHook(); hook != nil {
		hook()
	}
}

// TestFanOutsAtTakeOversAcrossTwoRouters: a pool of two, collections routed
// by both remaining nodes, every part's take-over held on both, and eight
// count() and find() calls on each collection through each node at once:
// local part reads at both routers and remote part reads in Batch pushes all
// meet a take-over, and every call answers.
func TestFanOutsAtTakeOversAcrossTwoRouters(t *testing.T) {
	if hung := takeOverFanOuts(t, 2, []int{0, 1}, true, 8); hung > 0 {
		t.Fatalf("%d of the fan-outs did not answer within 60 s with a pool of 2", hung)
	}
}

// TestALocalPartReadRunsUnderTheFanOutsAdmission: a part read on the router's
// own node carries the fan-out's admission, so the forward its part's
// take-over can make (to the previous owner) is answered while every forward
// slot of the node is held, as the fan-out's own slot is while it waits.
func TestALocalPartReadRunsUnderTheFanOutsAdmission(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 1 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	c := clusterOf(tc, 0)
	var bucket string
	for i := 0; bucket == ""; i++ {
		name := fmt.Sprintf("W%d", i)
		if node, err := c.Owner(placementOf("app", name)); err == nil && node.ID == tc.addrs[1] {
			bucket = name
		}
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash, Options: map[string]interface{}{"placement": "whole"}})
	owner, err := c.Owner(placementOf("app", bucket))
	if err != nil {
		t.Fatal(err)
	}
	gate := c.s.queryGate()
	release, err := gate.acquire(context.Background(), gate.forwardPool())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	read := partRead{part: bucket, frame: transport.NewFrame(transport.OpCount, spreadMarshal(t, CountRequest{Database: "app", Bucket: bucket}))}
	answer := readPartLocally(context.Background(), read, tc.addrs[0], func(ctx context.Context, read partRead) (*transport.Frame, error) {
		// What sendOnDemandAnswer does for a part this node takes over.
		return c.forwardRead(ctx, read.frame, owner, read.part, readForward{serveFromCopy: true})
	})
	if answer.err != nil || answer.frame == nil || answer.frame.Opcode != transport.OpCount {
		t.Fatalf("a local part read's forward with the forward pool full answered %+v: it waited for a slot the fan-out holds", answer)
	}
}
