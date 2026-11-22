package server

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// heldForwardedScans is what fills one peer pool's request lane: every
// socket but the control socket, each at the request lane's per-socket bound
// (query-pipelining spec §3.6; 3 x 32 = 96 since BUG-0723).
const heldForwardedScans = (transport.InternodePoolSize - 1) * transport.PeerRequestSocketFrames

// writeConcernRoles is T-15's cast: node indexes O, R and N, a bucket A
// placed on [O, R] (owner first) and a bucket B placed on [R, N].
type writeConcernRoles struct {
	nodeO, nodeR, nodeN int
	bucketA, bucketB    string
}

// castWriteConcernRoles picks O, R and N from what the ring actually places
// (placementOf). Node IDs are ephemeral ports, so which node
// follows which on the ring changes from run to run, and fixing the roles by
// index first would leave some rings with no bucket for them.
func castWriteConcernRoles(t *testing.T, c *Cluster, database string, addrs []string) writeConcernRoles {
	t.Helper()
	placedOn := map[[2]string]string{}
	for i := 0; i < 2048; i++ {
		name := fmt.Sprintf("bucket-%d", i)
		replicas, err := c.Replicas(placementOf(database, name))
		if err != nil {
			t.Fatalf("replicas of %q: %v", name, err)
		}
		if len(replicas) != 2 {
			t.Fatalf("bucket %q has %d replicas, want 2", name, len(replicas))
		}
		key := [2]string{replicas[0].ID, replicas[1].ID}
		if _, seen := placedOn[key]; !seen {
			placedOn[key] = name
		}
	}
	for o := range addrs {
		for r := range addrs {
			for n := range addrs {
				if o == r || r == n || o == n {
					continue
				}
				bucketA, okA := placedOn[[2]string{addrs[o], addrs[r]}]
				bucketB, okB := placedOn[[2]string{addrs[r], addrs[n]}]
				if okA && okB {
					return writeConcernRoles{nodeO: o, nodeR: r, nodeN: n, bucketA: bucketA, bucketB: bucketB}
				}
			}
		}
	}
	t.Fatalf("no assignment of %v has a bucket on [O, R] and one on [R, N] in 2048 names (placed: %v)", addrs, placedOn)
	return writeConcernRoles{}
}

// holdEveryScanWalk parks every SCAN walk at its first entry until the
// returned release is called, and counts the walks parked.
// queryVisitHook is process-global: in a multi-node test it fires on every
// node, so the test must be the only one scanning while it is set.
func holdEveryScanWalk(t *testing.T) (entered *atomic.Int64, release func()) {
	t.Helper()
	entered = &atomic.Int64{}
	released := make(chan struct{})
	var once atomic.Bool
	hook := func(method string) {
		if method != queryMethodScan {
			return
		}
		select {
		case <-released:
			return
		default:
		}
		entered.Add(1)
		<-released
	}
	queryVisitHook.Store(&hook)
	release = func() {
		if once.CompareAndSwap(false, true) {
			close(released)
		}
	}
	t.Cleanup(func() {
		release()
		queryVisitHook.Store(nil)
	})
	return entered, release
}

// TestForwardedQueriesDoNotStallWriteConcern is T-15 (rev 2.2, spec §13.2
// D-6). Three nodes O, R and N, RF = 2. Bucket A is owned by O with R as its
// replica; bucket B is on R and N and not on O, so O forwards B's reads over
// its pool to R. Clients on O hold 96 forwarded SCANs of B, which is exactly
// what O→R's request lane carries. A majority PUT to A, sent to O, must still
// complete within the write-concern timeout, because its ship to R (caller 1
// of spec §3.6's table) rides that pool's control lane.
func TestForwardedQueriesDoNotStallWriteConcern(t *testing.T) {
	tc := newTestClusterWith(t, 3, 2, func(_ int, cfg *config.Config) {
		// O's forwarded-result budget and R's execution slots must not be
		// what limits the held scans, or the request lane is never full.
		setQueryConcurrency(cfg, 2*heldForwardedScans)
		// Every scanner dials O from one address, above the per-address default.
		cfg.TCP.MaxConnsPerIP = 2 * heldForwardedScans
	})
	defer tc.stop()
	roles := castWriteConcernRoles(t, tc.nodes[0].server.core.cluster, "app", tc.addrs)
	nodeO, bucketA, bucketB := roles.nodeO, roles.bucketA, roles.bucketB
	t.Logf("O = node %d, R = node %d, N = node %d; A = %s, B = %s", roles.nodeO, roles.nodeR, roles.nodeN, bucketA, bucketB)
	tc.ok(nodeO, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	for _, bucket := range []string{bucketA, bucketB} {
		tc.ok(nodeO, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	}
	for i := 0; i < 5; i++ {
		tc.ok(nodeO, transport.OpPut, InsertRequest{Database: "app", Bucket: bucketB, Key: fmt.Sprintf("k%d", i), Data: map[string]interface{}{"n": int32(i)}})
	}

	scanners := make([]*queryConn, heldForwardedScans)
	for i := range scanners {
		scanners[i] = mustDialQueryConn(t, tc.driverAddrs[nodeO], testRootUser, testRootPassword)
	}
	entered, release := holdEveryScanWalk(t)
	answers := make([]<-chan frameOrError, len(scanners))
	for i, qc := range scanners {
		if err := qc.send(queryFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: bucketB})); err != nil {
			t.Fatalf("scanner %d: %v", i, err)
		}
		answers[i] = qc.readAsync()
	}
	deadline := time.Now().Add(5 * time.Second)
	for entered.Load() < heldForwardedScans && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	heldOnR := entered.Load()

	started := time.Now()
	put := tc.frame(nodeO, transport.OpPut, InsertRequest{
		Database: "app", Bucket: bucketA, Key: "during-the-scans",
		Data: map[string]interface{}{"n": int32(1)}, WriteConcern: "majority",
	})
	waited := time.Since(started)
	if put.Opcode == transport.OpError || waited >= DefaultWriteConcernTimeout {
		t.Errorf("a majority PUT to a bucket O owns answered %s after %v (write-concern timeout %v) while %d forwarded SCANs were held on O→R: "+
			"its ship to R waited behind forwarded reads for a socket of the shared pool (S-1): %s",
			put.Opcode, waited.Round(time.Millisecond), DefaultWriteConcernTimeout, heldOnR, put.Payload)
	}
	if heldOnR != heldForwardedScans {
		t.Errorf("R was walking %d of the %d forwarded SCANs when the PUT was sent: O→R's request lane did not carry them all",
			heldOnR, heldForwardedScans)
	}

	release()
	for i, answer := range answers {
		select {
		case got := <-answer:
			if got.err != nil {
				t.Errorf("scanner %d: %v", i, got.err)
			} else if got.frame.Opcode == transport.OpError {
				t.Errorf("scanner %d: the forwarded SCAN answered an error: %s", i, got.frame.Payload)
			}
		case <-time.After(20 * time.Second):
			t.Errorf("scanner %d: no answer 20s after the walks were released", i)
		}
	}
}
