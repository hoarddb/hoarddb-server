package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// These tests start three real servers on loopback with a real ring and a
// real cluster token. Nothing is mocked, because the point of this work was to
// replace an implementation that reported replication success without sending
// anything: only an end-to-end test can show that data actually arrives.

const (
	testRootUser     = "admin"
	testRootPassword = "cluster-test-password"
	testDatabase     = "bench"
	testBucket       = "docs"
)

type testNode struct {
	cfg    *config.Config
	addr   string
	server *Service
	client *transport.DriverClient
}

type testCluster struct {
	t     testing.TB
	nodes []*testNode
	addrs []string
	// driverAddrs is each node's TCP driver (client-facing) listen address,
	// separate from addrs (the internode/ring address, also node identity):
	// with QUIC gone the driver listener is the only client transport, so
	// multi-node tests on one host need one unique driver port per node the
	// way they already needed one internode port per node.
	driverAddrs []string
	rf          int
	token       string
	// fingerprints is each node's canonical TLS fingerprint, generated before
	// any listener starts so every node can pin every other one.
	fingerprints []string
	// ports holds each node's still-bound listeners, handed to its server.
	ports []*testNodePorts
	// started records which nodes have already consumed their first listener
	// set, so a restart re-binds the same addresses instead of reusing the
	// listeners Stop closed.
	started []bool
	// configure, when set, adjusts a node's config after the harness builds it
	// and before the node starts: the one way a test gives two nodes of one
	// cluster different settings (a sealed node beside a plaintext one).
	configure func(index int, cfg *config.Config)
	// tune, when set, adjusts a node's cluster after it is built and before it
	// starts, at every start: the one race-free moment to change a field the
	// running node's goroutines read, such as the write-concern timeout.
	tune func(index int, c *Cluster)
}

// clusterTestLogger, when set, is the logger every node started afterwards
// is built with.
var clusterTestLogger atomic.Pointer[slog.Logger]

func quietLogger() *slog.Logger {
	// A test that reads what a node logs installs its own logger for the nodes
	// it starts (clusterTestLogger).
	if logger := clusterTestLogger.Load(); logger != nil {
		return logger
	}
	// Set HOARDDB_TEST_VERBOSE to see server-side warnings when a cluster test
	// fails: without it the reason a peer rejected replication is discarded.
	if os.Getenv("HOARDDB_TEST_VERBOSE") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testNodePorts holds a node's reserved-and-still-bound listeners. The ports
// stay bound from reservation until the server closes them, so no outbound
// connection can claim one in the gap the old reserve-then-close helper left.
type testNodePorts struct {
	// ringAddr is the node's ring/identity address. Its internode port is
	// ringAddr's port + 1, which is the port internode is bound to, so a peer
	// that derives the internode address still dials the held listener.
	ringAddr      string
	internodeAddr string
	driverAddr    string
	internode     net.Listener
	driver        net.Listener
}

// reserveNodePorts binds a node's internode and driver ports and returns them
// with the ring address. The caller hands the listeners to the server
// (Service.driverListener / Service.internodeListener) rather than passing a
// released port string.
func reserveNodePorts(t testing.TB) *testNodePorts {
	t.Helper()
	internode := listenLoopback(t, "internode")
	tcpAddr, ok := internode.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("internode listener address %T is not a TCP address", internode.Addr())
	}
	if tcpAddr.Port <= 1 {
		t.Fatalf("internode port %d cannot serve as ring port+1", tcpAddr.Port)
	}
	ports := &testNodePorts{
		ringAddr:      net.JoinHostPort("127.0.0.1", strconv.Itoa(tcpAddr.Port-1)),
		internodeAddr: internode.Addr().String(),
		internode:     internode,
		driver:        listenLoopback(t, "driver"),
	}
	ports.driverAddr = ports.driver.Addr().String()
	// The server owns these once handed over; a second Close at test end is a
	// harmless no-op and catches a reservation that never reached a server.
	t.Cleanup(func() {
		_ = ports.internode.Close()
		_ = ports.driver.Close()
	})
	return ports
}

// rebind returns fresh listeners on the same addresses after Stop closed the
// previous ones. A restarted node must come back on the same ports — a fresh
// :0 reservation would change its identity and its peers would keep dialing
// the old address — so the addresses are re-bound explicitly and retried
// briefly for a socket that has not finished closing.
func (p *testNodePorts) rebind(t testing.TB) *testNodePorts {
	t.Helper()
	fresh := &testNodePorts{
		ringAddr:      p.ringAddr,
		internodeAddr: p.internodeAddr,
		internode:     listenLoopbackAddr(t, "internode", p.internodeAddr),
		driverAddr:    p.driverAddr,
		driver:        listenLoopbackAddr(t, "driver", p.driverAddr),
	}
	t.Cleanup(func() {
		_ = fresh.internode.Close()
		_ = fresh.driver.Close()
	})
	return fresh
}

// ringNode is the ports' node identity: the ring address, which is what the
// membership set records and peers derive the internode address from.
func (p *testNodePorts) ringNode() ring.Node {
	return ring.Node{ID: p.ringAddr, Addr: p.ringAddr}
}

// fabricatedMemberAddr returns a distinct address for a member that is
// recorded or proposed but never started. It binds nothing and is never
// handed to a server, so a dial to it fails fast (connection refused) instead
// of stalling on a handshake to a listener no one serves. The range is below
// the ephemeral range, which the kernel does not hand to outbound
// connections; nothing binds these ports in the test binary.
var fabricatedPort int32 = 21000

func fabricatedMemberAddr(t *testing.T) string {
	t.Helper()
	port := atomic.AddInt32(&fabricatedPort, 1)
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
}

// listenLoopback binds an ephemeral loopback TCP port and returns the
// listener, which stays bound until the caller (or t.Cleanup) closes it.
func listenLoopback(t testing.TB, what string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve %s port: %v", what, err)
	}
	return ln
}

// rebindWindow bounds how long a restarted node waits for its old port. The
// port is free between Stop and the rebind, and on a host running other test
// suites another process can take it (a listener, or an outbound connection
// whose source port falls in the ephemeral range) for as long as that
// connection lives, which is seconds, not the time a socket takes to close.
// The node must come back on the same address, so waiting is the
// only option that keeps its identity.
const rebindWindow = 30 * time.Second

// listenLoopbackAddr binds a specific loopback address, retrying for
// rebindWindow while something else holds it.
func listenLoopbackAddr(t testing.TB, what, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(rebindWindow)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebind %s on %s: still taken after %s, so another process on this host holds the port (find it with 'ss -tanp | grep %s'): %v",
				what, addr, rebindWindow, addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// preGeneratedNodeFingerprints creates each node's TLS key once, up front,
// and returns the canonical fingerprints. Generating before the cluster
// starts is what lets every node pin every other one instead of trusting an
// unverified first connection.
func preGeneratedNodeFingerprints(t testing.TB, dirs []string) []string {
	t.Helper()
	fingerprints := make([]string, len(dirs))
	for i, dir := range dirs {
		_, fingerprint, err := transport.LoadOrCreateKeys(filepath.Join(dir, "keys"))
		if err != nil {
			t.Fatalf("generate keys for %s: %v", dir, err)
		}
		fingerprints[i] = fingerprint
	}
	return fingerprints
}

// trustJoinerFingerprint seeds the newcomer's certificate into every cluster
// node's learned peer store, keyed by its internode address. A node added
// after the cluster started is not in anyone's configured seed_fingerprints;
// in production the operator configures the new node's fingerprint before
// adding it (or a future membership wire distributes it), while the tests
// pre-generate the key and seed the store directly. Without this the existing
// nodes refuse to dial the newcomer — the pin working as intended.
func trustJoinerFingerprint(t *testing.T, tc *testCluster, memberAddr, dir string) {
	t.Helper()
	paths, _, err := transport.LoadOrCreateKeys(filepath.Join(dir, "keys"))
	if err != nil {
		t.Fatalf("load joiner keys: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		t.Fatalf("load joiner certificate: %v", err)
	}
	pub, err := transport.ParseEd25519PublicKey(cert)
	if err != nil {
		t.Fatalf("parse joiner public key: %v", err)
	}
	internodeAddr, err := transport.InternodePeerAddr(memberAddr)
	if err != nil {
		t.Fatalf("derive internode address for %s: %v", memberAddr, err)
	}
	for _, n := range tc.nodes {
		// A stopped node trusts nothing new; it learns the joiner when it
		// returns, as it would in production.
		if n.server == nil || n.server.core.cluster == nil {
			continue
		}
		if err := n.server.core.cluster.knownServers.Trust(internodeAddr, pub); err != nil {
			t.Fatalf("trust joiner fingerprint on %s: %v", n.addr, err)
		}
	}
}

// TestReservedPortIsStillHeld is the acceptance test that a port
// reserved by the harness must be bound from the moment it is reserved, so no
// outbound connection can claim it in the gap. reserveNodePorts keeps the
// listener bound and hands it to the server, and a second net.Listen on either
// port must fail.
//
// The assertion before Start is the discriminating one: that is the window the
// bug describes, and a reserve-then-close harness fails it, because it has
// already given the port back. Asserted only after Start it would pass for that
// old helper too — by then any server holds the port — so both placements are
// checked rather than the later one alone.
func TestReservedPortIsStillHeld(t *testing.T) {
	ports := reserveNodePorts(t)
	assertPortsHeld(t, ports, "while reserved, before a server binds them")

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = ports.ringAddr
	cfg.Server.NodeID = ports.ringAddr
	cfg.TCP.Listen = ports.driver.Addr().String()
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Logging.Output = "stderr"

	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	srv.driverListener = ports.driver
	srv.internodeListener = ports.internode
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	assertPortsHeld(t, ports, "while the server runs")
}

// assertPortsHeld fails when a second listener can bind either of a node's
// reserved ports. when names the point in the test's life the assertion covers,
// so a failure says which half of the property broke: the window between
// reservation and start, or the port being handed back while serving.
func assertPortsHeld(t *testing.T, ports *testNodePorts, when string) {
	t.Helper()
	for _, reserved := range []struct {
		what string
		addr string
	}{
		{what: "driver", addr: ports.driverAddr},
		{what: "internode", addr: ports.internodeAddr},
	} {
		second, err := net.Listen("tcp", reserved.addr)
		if err != nil {
			continue
		}
		_ = second.Close()
		t.Fatalf("the %s port %s was not held %s: a second listener bound it",
			reserved.what, reserved.addr, when)
	}
}

// newTestCluster starts `count` nodes with replication factor rf on loopback.
func newTestCluster(t testing.TB, count, rf int) *testCluster {
	t.Helper()
	return newTestClusterWith(t, count, rf, nil)
}

// autoRebalanceOff is a config hook that keeps every node's background sweep
// off. It is set at construction because the sweep and the pending-copy worker
// read the flag from their first pass, so a write on a running node races.
func autoRebalanceOff(_ int, cfg *config.Config) {
	cfg.Server.AutoRebalance = false
}

// autoRebalanceOffOnNodeZero is autoRebalanceOff for the proposer alone, the
// node joinLiveReplicaBy drives the join from.
func autoRebalanceOffOnNodeZero(index int, cfg *config.Config) {
	if index == 0 {
		cfg.Server.AutoRebalance = false
	}
}

// newTestClusterWith is newTestCluster with a per-node config hook; see
// testCluster.configure.
func newTestClusterWith(t testing.TB, count, rf int, configure func(index int, cfg *config.Config)) *testCluster {
	t.Helper()
	return newTestClusterTuned(t, count, rf, configure, nil)
}

// newTestClusterTuned is newTestClusterWith with a hook on each node's cluster;
// see testCluster.tune.
func newTestClusterTuned(t testing.TB, count, rf int, configure func(index int, cfg *config.Config), tune func(index int, c *Cluster)) *testCluster {
	t.Helper()
	tc := startTestClusterTuned(t, count, rf, configure, tune)
	tc.waitReady()
	return tc
}

// startTestClusterTuned starts the nodes of newTestClusterTuned and returns at
// once, before any node has gossiped: for a test about what happens before the
// cluster is ready. newTestClusterTuned is this plus waitReady.
func startTestClusterTuned(t testing.TB, count, rf int, configure func(index int, cfg *config.Config), tune func(index int, c *Cluster)) *testCluster {
	t.Helper()
	return startTestClusterOn(t, reserveClusterPorts(t, count), rf, configure, tune)
}

// reserveClusterPorts reserves the ports of count nodes.
func reserveClusterPorts(t testing.TB, count int) []*testNodePorts {
	t.Helper()
	ports := make([]*testNodePorts, count)
	for i := range ports {
		ports[i] = reserveNodePorts(t)
	}
	return ports
}

// startTestClusterOn is startTestClusterTuned on ports already reserved, one
// set per node: for a test that chooses its nodes' ring addresses.
func startTestClusterOn(t testing.TB, ports []*testNodePorts, rf int, configure func(index int, cfg *config.Config), tune func(index int, c *Cluster)) *testCluster {
	t.Helper()
	count := len(ports)

	base := t.TempDir()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}

	tc := &testCluster{t: t, rf: rf, token: token, started: make([]bool, count), configure: configure, tune: tune}
	// A cluster left running keeps decoding BSON on its own goroutines for the
	// rest of the test binary's life, which races any later test that swaps
	// bson.DefaultRegistry. Callers may still stop it themselves.
	t.Cleanup(tc.stop)

	// Generate every node's TLS key before any listener starts: the
	// fingerprints are the trust anchor each node pins the others against, so
	// they must be known before the first node's config is built.
	dirs := make([]string, count)
	for i := range dirs {
		dirs[i] = filepath.Join(base, fmt.Sprintf("n%d", i+1))
		if err := os.MkdirAll(dirs[i], 0o755); err != nil {
			t.Fatalf("create node dir: %v", err)
		}
	}
	tc.fingerprints = preGeneratedNodeFingerprints(t, dirs)

	for _, nodePorts := range ports {
		tc.ports = append(tc.ports, nodePorts)
		tc.addrs = append(tc.addrs, nodePorts.ringAddr)
		tc.driverAddrs = append(tc.driverAddrs, nodePorts.driver.Addr().String())
	}

	for i := 0; i < count; i++ {
		tc.nodes = append(tc.nodes, tc.startNode(i, base, token))
	}
	return tc
}

// waitReady waits until the cluster is ready for a test's first request.
func (tc *testCluster) waitReady() {
	tc.t.Helper()
	count := len(tc.nodes)

	// Replication is not ready the instant a listener opens: peers gossip on a
	// background loop. Wait until every node knows every other node, and until
	// every node's member list is confirmed — for founders, once every founder
	// has answered and agreed (join-freeze spec §5.4, §16 F-A); until then a
	// node refuses every scoped request.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, n := range tc.nodes {
			if len(n.server.core.KnownNodes()) < count-1 || n.server.core.cluster.bootstrapped.Load() {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	tc.t.Fatalf("cluster did not converge: nodes did not all see each other and confirm their membership")
}

// startNode boots one node, its data directory and its client.
func (tc *testCluster) startNode(index int, base, token string) *testNode {
	tc.t.Helper()

	// A restart comes back on the same addresses: Stop closed the listeners
	// the server owned, so re-bind them rather than handing over closed ones
	//. Peers keep dialing the same internode address.
	ports := tc.ports[index]
	if tc.started[index] {
		ports = ports.rebind(tc.t)
		tc.ports[index] = ports
	}
	tc.started[index] = true

	dir := filepath.Join(base, fmt.Sprintf("n%d", index+1))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tc.t.Fatalf("create node dir: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{
		Token:     token,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		tc.t.Fatalf("save cluster token: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = tc.addrs[index]
	// Node identity is its address, so every node derives the same ring from
	// the same member list.
	cfg.Server.NodeID = tc.addrs[index]
	cfg.Server.ReplicationFactor = tc.rf
	cfg.HashRing.SeedNodes = append([]string(nil), tc.addrs...)
	cfg.HashRing.SeedFingerprints = append([]string(nil), tc.fingerprints...)
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Logging.Output = "stderr"
	// Multi-node tests put every node on one host; the TCP driver listener
	// defaults to a single address and would collide on the second node, so
	// each node gets its own reserved driver port.
	cfg.TCP.Listen = tc.driverAddrs[index]
	if tc.configure != nil {
		tc.configure(index, cfg)
	}

	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		tc.t.Fatalf("new node %d: %v", index, err)
	}
	// Hand the server the listeners reserved above, so the ports stay bound
	// from reservation to serve instead of being released for the bind.
	srv.driverListener = ports.driver
	srv.internodeListener = ports.internode
	// A node reads a healthy disk unless its test says otherwise (BUG-0553):
	// every pull is guarded by the disk mark, so the host's real filesystem
	// must not decide whether a test about migration passes. A disk test pins
	// its own probe, which replaces this one.
	setCapacityProbes(tc.t, srv.core, harnessProbes)
	if tc.tune != nil {
		tc.tune(index, srv.core.cluster)
	}
	// Re-attach whatever is already on disk before replay runs, the same
	// order cmd/server/main.go uses (NewService, then LoadCatalog, then
	// Start, which replays the WAL): a restart with nothing loaded first
	// left the replay gate unable to tell a re-created database's
	// bucket from one replay is about to reconstruct itself, because
	// nothing had filtered the dropped generation's entries out of memory
	// yet (server/repl_recover.go:databaseDropAtOrBelowMark).
	if err := srv.LoadCatalog(); err != nil {
		tc.t.Fatalf("load catalog on node %d: %v", index, err)
	}
	// A node restarting on the same address is the normal case, but the
	// previous listener's socket can linger for a moment after Stop, so bind
	// with a short retry rather than failing a restart outright.
	if err := startWithRetry(srv, 3*time.Second); err != nil {
		tc.t.Fatalf("start node %d: %v", index, err)
	}
	if err := srv.core.SeedRootUser(testRootUser, testRootPassword); err != nil {
		// A restarted node finds the user already present; that is expected
		// and must not fail the boot.
		tc.t.Logf("seed root user on node %d: %v", index, err)
	}

	return &testNode{
		cfg:    cfg,
		addr:   tc.addrs[index],
		server: srv,
		client: tc.dial(index),
	}
}

// harnessProbes is greenProbes with an empty disk, so a harness node is below
// any mark a test sets, down to 1%: the test of the harness itself lowers the
// mark to prove the real disk is not read.
func harnessProbes(p *capacityProbes) {
	greenProbes(p)
	p.diskStatfs = fixedDisk(diskFixture(0))
}

// startWithRetry starts a node, retrying briefly if its address is still held
// by a listener that was just stopped.
func startWithRetry(srv *Service, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	var err error
	for {
		if err = srv.Start(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// dial connects and authenticates a client against a node's driver listener.
func (tc *testCluster) dial(index int) *transport.DriverClient {
	tc.t.Helper()
	return tc.dialAddress(tc.driverAddrs[index])
}

// dialAddress connects and authenticates a client against an address, as an
// operator following one printed by the server would.
func (tc *testCluster) dialAddress(addr string) *transport.DriverClient {
	tc.t.Helper()

	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, addr)
	if err != nil {
		tc.t.Fatalf("client TLS config for %s: %v", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := transport.NewDriverClient(ctx, addr, tlsCfg)
	if err != nil {
		tc.t.Fatalf("dial %s: %v", addr, err)
	}

	payload, err := bson.Marshal(AuthRequest{Username: testRootUser, Password: testRootPassword})
	if err != nil {
		tc.t.Fatalf("marshal auth: %v", err)
	}
	frame, err := client.SendRequest(ctx, transport.OpAuth, payload)
	if err != nil {
		tc.t.Fatalf("auth against %s: %v", addr, err)
	}
	if frame.Opcode != transport.OpAuthOK {
		tc.t.Fatalf("auth against %s rejected with %s", addr, frame.Opcode)
	}
	return client
}

// frame sends a request to a node and returns the response frame.
func (tc *testCluster) frame(index int, op transport.Opcode, req any) *transport.Frame {
	tc.t.Helper()

	if tc.nodes[index].client == nil {
		tc.t.Fatalf("node %d (%s) has no client: was it stopped?", index+1, tc.addrs[index])
	}
	payload, err := bson.Marshal(req)
	if err != nil {
		tc.t.Fatalf("marshal %s: %v", op, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	frame, err := tc.nodes[index].client.SendRequest(ctx, op, payload)
	if err != nil {
		tc.t.Fatalf("send %s to node %d: %v", op, index+1, err)
	}
	return frame
}

// ok sends a request and fails the test if the node answers with an error.
func (tc *testCluster) ok(index int, op transport.Opcode, req any) *transport.Frame {
	tc.t.Helper()
	frame := tc.frame(index, op, req)
	if frame.Opcode == transport.OpError {
		tc.t.Fatalf("node %d answered %s with an error: %s", index+1, op, string(frame.Payload))
	}
	return frame
}

// writeConcerned writes one document with an explicit durability level.
func (tc *testCluster) writeConcerned(index int, key, wc string) *transport.Frame {
	return tc.frame(index, transport.OpPut, InsertRequest{
		Database:     testDatabase,
		Bucket:       testBucket,
		Key:          key,
		Data:         map[string]interface{}{"key": key},
		WriteConcern: wc,
	})
}

func (tc *testCluster) stop() {
	for _, n := range tc.nodes {
		if n.server != nil {
			_ = n.server.Stop()
			n.server = nil
		}
		if n.client != nil {
			_ = n.client.Close()
			n.client = nil
		}
	}
}

// readFromNode reads a key directly from one node's client. A read is answered
// from that node's own copy when it holds one, so this asks each replica what
// it actually stored rather than asking a coordinator to fetch for it.
func (tc *testCluster) readFromNode(index int, key string) (bool, string) {
	tc.t.Helper()
	frame := tc.frame(index, transport.OpGet, FindRequest{
		Database: testDatabase,
		Bucket:   testBucket,
		Key:      key,
	})
	if frame.Opcode == transport.OpError {
		return false, string(frame.Payload)
	}
	var resp FindResponse
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			tc.t.Fatalf("decode find response from node %d: %v", index+1, err)
		}
	}
	return resp.Found, resp.Message
}

// TestClusterPropagatesWritesToEveryReplica is the test the previous
// implementation could not have passed: it checks that a write accepted by one
// node is present in the storage of all three.
//
// It writes at w=all because that is the only write concern that promises what
// this test checks. It used to send no write concern at all, so it inherited the
// cluster default — WcMajority for RF>1 (cluster_state.go) — and then required all
// three nodes to hold every document the instant the last write returned. The
// third replica is explicitly allowed to be behind at that moment, and when it
// was, the test failed with "missing 1 of 30" on the node the write concern never
// named. The convergence of a majority-acknowledged write is a different property
// with its own test: TestReplicasConvergeAfterMajorityWrites.
//
// Re-aimed by step 7.6 (D-2): each write must be answered copies_confirmed 3, since `all` now succeeds on the copies in sync.
func TestClusterPropagatesWritesToEveryReplica(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	const docs = 30
	keys := make([]string, 0, docs)
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("k%02d", i)
		keys = append(keys, key)
		// Writes go to node 1 while placement is decided by the ring, so the
		// coordinator is frequently not the owner: forwarding is exercised.
		// w=all: the assertion below reads every node, so the write must have
		// reached every node before it is made.
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Key:          key,
			Data:         map[string]interface{}{"n": i},
			WriteConcern: "all",
		}), "put "+key)
	}

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	t.Logf("bucket %q owner is %s; replicas %v", testBucket, owner.ID,
		mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket))

	for index := range tc.nodes {
		missing := 0
		for _, key := range keys {
			found, message := tc.readFromNode(index, key)
			if !found {
				missing++
				if missing == 1 {
					t.Errorf("node %d (%s) is missing %s: %s", index+1, tc.addrs[index], key, message)
				}
			}
		}
		if missing > 0 {
			t.Errorf("node %d (%s) is missing %d of %d documents", index+1, tc.addrs[index], missing, docs)
		}
	}
}

// requireEveryCopy fails the test unless a write was answered as held by every
// copy of its bucket. Under owner decision D-2 `all` succeeds against the
// copies that are in sync, so a test that goes on to read every node checks the
// number the write was answered with, not the concern it was sent at.
func requireEveryCopy(t testing.TB, frame *transport.Frame, what string) {
	t.Helper()
	if frame.Opcode == transport.OpError {
		t.Fatalf("%s was refused: %s", what, frame.Payload)
	}
	confirmed, factor, hasConfirmed, hasFactor := copiesConfirmedOf(frame.Payload)
	if !hasConfirmed || !hasFactor || confirmed != factor {
		t.Fatalf("%s was answered copies_confirmed %d of replication_factor %d; the test reads every copy and needs all of them to hold it",
			what, confirmed, factor)
	}
}

func mustReplicaIDs(t *testing.T, c *Cluster, database, bucket string) []string {
	t.Helper()
	replicas, err := c.Replicas(placementOf(database, bucket))
	if err != nil {
		t.Fatalf("resolve replicas: %v", err)
	}
	ids := make([]string, 0, len(replicas))
	for _, r := range replicas {
		ids = append(ids, r.ID)
	}
	return ids
}

// TestWriteConcernReportsWhatItGotWhenReplicasAreDown checks that a write is
// never reported as held by copies that do not hold it. With two of three
// nodes stopped, `one`, `majority` and `all` are each confirmed by the owner
// alone and say so: copies_confirmed 1 beside replication_factor 3.
// Re-aimed by step 7.1 (spec §19.3.11, owner decision D-2): it asserted that
// `majority` and `all` fail when replicas are down; they now report the count.
func TestWriteConcernReportsWhatItGotWhenReplicasAreDown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	// Keep the owner up and stop the other two, so the coordinator is the node
	// that owns the bucket and the failures are purely about replica health.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := -1
	for i, n := range tc.nodes {
		if n.addr == owner.ID {
			ownerIndex = i
			break
		}
	}
	if ownerIndex < 0 {
		t.Fatalf("owner %s is not one of the test nodes", owner.ID)
	}

	for i := range tc.nodes {
		if i == ownerIndex {
			continue
		}
		_ = tc.nodes[i].client.Close()
		tc.nodes[i].client = nil
		if err := tc.nodes[i].server.Stop(); err != nil {
			t.Fatalf("stop node %d: %v", i+1, err)
		}
		tc.nodes[i].server = nil
	}

	for _, wc := range []string{"one", "majority", "all"} {
		frame := tc.writeConcerned(ownerIndex, "w-"+wc, wc)
		if frame.Opcode == transport.OpError {
			t.Errorf("w=%s with 2 of 3 nodes down was refused: %s", wc, frame.Payload)
			continue
		}
		confirmed, factor, _, _ := copiesConfirmedOf(frame.Payload)
		if confirmed != 1 || factor != 3 {
			t.Errorf("w=%s with 2 of 3 nodes down was answered copies_confirmed %d, replication_factor %d; want 1 and 3", wc, confirmed, factor)
		}
	}
}

// TestRestartedReplicaCatchesUp checks that a replica which missed writes while
// it was down is repaired from the owner's log instead of staying silently
// stale forever.
func TestRestartedReplicaCatchesUp(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	// Resolve the owner first: which node owns the bucket depends on the ring,
	// and taking down the owner would make the write fail with "server shutting
	// down" instead of testing catch-up. Hardcoding an index made this test
	// pass or fail depending on the ports the ring happened to hash.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	downID := ""
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			downID = id
			break
		}
	}
	if downID == "" {
		t.Fatalf("bucket %q has no replica other than its owner %s", testBucket, owner.ID)
	}
	downIndex := nodeIndexByAddr(t, tc.addrs, downID)

	// Take that replica down, keeping every write durable on the other two.
	_ = tc.nodes[downIndex].client.Close()
	tc.nodes[downIndex].client = nil
	if err := tc.nodes[downIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", downIndex+1, err)
	}
	tc.nodes[downIndex].server = nil

	// Coordinate from a node that is still up: the point is the owner's repair
	// of a lagging replica, not the client's choice of entry point. Sending to
	// the stopped node would panic on its nil client.
	coordIndex := tc.coordinatorIndex(t, downIndex, owner.ID)

	const docs = 10
	var keys []string
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("gap%02d", i)
		keys = append(keys, key)
		if frame := tc.writeConcerned(coordIndex, key, "majority"); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at w=majority failed with one node down: %s", key, string(frame.Payload))
		}
	}

	// Bring it back, then run the repair directly: in production this runs on
	// the background peer loop, and waiting 20s here would only slow the test.
	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted

	// The owner resolved above is the only node that replays entries.
	for i, n := range tc.nodes {
		if n.addr != owner.ID {
			continue
		}
		peer := tc.nodes[i].server.core.cluster.peerFor(
			mustNode(t, tc.nodes[downIndex].server.core.cluster, tc.addrs[downIndex]))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := replayToPeer(ctx, peer); err != nil {
			cancel()
			t.Fatalf("catch-up from owner to node %d failed: %v", downIndex+1, err)
		}
		cancel()
	}

	missing := 0
	for _, key := range keys {
		found, message := tc.readFromNode(downIndex, key)
		if !found {
			missing++
			if missing == 1 {
				t.Errorf("restarted node is still missing %s: %s", key, message)
			}
		}
	}
	if missing > 0 {
		t.Errorf("restarted node is missing %d of %d documents written while it was down", missing, docs)
	}
}

// TestReplicaReceivesEntriesRetriedAfterAFailedSend is the regression test for
// a permanent hole in a replica. A send that failed used to be dropped while
// the acknowledgement stayed a high-water mark, so the entries the peer missed
// sat below the acknowledgement and catch-up skipped straight over them: the
// node stayed permanently short of writes the cluster counted it as holding
// (measured: 2 of 10 documents never arrived).
//
// Nothing here calls catch-up. The sender must deliver the entries itself,
// because deletion of a failed batch is the bug.
func TestReplicaReceivesEntriesRetriedAfterAFailedSend(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	downID := ""
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			downID = id
			break
		}
	}
	if downID == "" {
		t.Fatalf("bucket %q has no replica other than its owner %s", testBucket, owner.ID)
	}
	downIndex := nodeIndexByAddr(t, tc.addrs, downID)

	// Take the replica down, then write. `majority` is satisfied by the owner
	// and the other replica, so these writes are reported as durable — which is
	// exactly why the node that missed them must eventually receive them.
	_ = tc.nodes[downIndex].client.Close()
	tc.nodes[downIndex].client = nil
	if err := tc.nodes[downIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", downIndex+1, err)
	}
	tc.nodes[downIndex].server = nil

	// Coordinate from a node that is still up, so the requests exercise the
	// forwarding path and the owner's shipping path.
	coordIndex := tc.coordinatorIndex(t, downIndex, owner.ID)

	const docs = 10
	var keys []string
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("gap%02d", i)
		keys = append(keys, key)
		if frame := tc.writeConcerned(coordIndex, key, "majority"); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at w=majority failed with one node down: %s", key, string(frame.Payload))
		}
	}

	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted

	deadline := time.Now().Add(30 * time.Second)
	missing := -1
	for time.Now().Before(deadline) {
		missing = 0
		for _, key := range keys {
			if found, _ := tc.readFromNode(downIndex, key); !found {
				missing++
			}
		}
		if missing == 0 {
			t.Logf("restarted replica received all %d entries from the retried batch", docs)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("restarted replica is missing %d of %d documents written while it was down; "+
		"a failed send must be retried, not dropped", missing, docs)
}

// TestClusterReplicationFactorThreeOnFiveNodes is the shape the three-node
// tests cannot reach: more members than replicas. Each bucket must land on
// exactly three of the five nodes, the other two must hold no copy, and a node
// outside the replica set must still answer a read by forwarding to the owner.
//
// A 3-node/RF=3 cluster cannot tell correct placement from a short-circuit that
// ships every write to every member, because "the replica set" and "the cluster"
// are the same set of nodes there. Here they are not.
func TestClusterReplicationFactorThreeOnFiveNodes(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	replicas := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket)
	if len(replicas) != 3 {
		t.Fatalf("RF=3 over 5 members placed %d replicas for bucket %q: %v",
			len(replicas), testBucket, replicas)
	}
	hosts := make(map[string]bool, len(replicas))
	for _, id := range replicas {
		hosts[id] = true
	}
	t.Logf("5 members, RF=3: bucket %q owner %s, replicas %v",
		testBucket, replicas[0], replicas)

	const docs = 20
	keys := make([]string, 0, docs)
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("k%02d", i)
		keys = append(keys, key)
		// Rotate the coordinator across the replica set so some writes are
		// applied by the owner and some are forwarded to it by a replica.
		from := nodeIndexByAddr(t, tc.addrs, replicas[i%len(replicas)])
		if frame := tc.frame(from, transport.OpPut, InsertRequest{
			Database: testDatabase,
			Bucket:   testBucket,
			Key:      key,
			Data:     map[string]interface{}{"n": i},
		}); frame.Opcode == transport.OpError {
			t.Fatalf("put %s via node %d rejected: %s", key, from+1, string(frame.Payload))
		}
	}

	// Placement, read straight out of each node's own storage. Server.Count
	// does not route, so this asks what the node actually holds.
	//
	// The writes above ran at the node default (majority = two of three), so
	// they are allowed to return before the third replica has its copy:
	// replication is asynchronous and the guarantee is eventual delivery, not
	// synchronous fan-out. Poll for that convergence rather than asserting it
	// instantly, and fail only if it does not happen.
	deadline := time.Now().Add(30 * time.Second)
	counts := make(map[int]int64, len(tc.nodes))
	for {
		converged := true
		for index := range tc.nodes {
			resp, err := tc.nodes[index].server.core.Count(CountRequest{
				Database: testDatabase,
				Bucket:   testBucket,
			})
			if err != nil {
				// Bucket DDL is placed on the replica set, not broadcast, so a
				// node outside the set does not have the bucket at all. It can
				// still serve reads and writes for it by forwarding (checked
				// below).
				if hosts[tc.addrs[index]] {
					t.Fatalf("replica node %d (%s) cannot count bucket %q: %v",
						index+1, tc.addrs[index], testBucket, err)
				}
				counts[index] = 0
				if time.Now().After(deadline) {
					t.Logf("non-replica node %d (%s) does not hold bucket %q: %v",
						index+1, tc.addrs[index], testBucket, err)
				}
				continue
			}

			counts[index] = resp.Count
			want := int64(0)
			if hosts[tc.addrs[index]] {
				want = docs
			}
			if resp.Count != want {
				converged = false
			}
		}

		if converged {
			break
		}
		if time.Now().After(deadline) {
			for index := range tc.nodes {
				want := int64(0)
				if hosts[tc.addrs[index]] {
					want = docs
				}
				if counts[index] != want {
					t.Errorf("node %d (%s) holds %d documents after 30s, want %d (is it a replica? %v; replicas %v)",
						index+1, tc.addrs[index], counts[index], want, hosts[tc.addrs[index]], replicas)
				}
			}
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Write concern is measured against the replica set, not the member count:
	// `all` means three copies here, and must not wait for five.
	for _, wc := range []string{"one", "majority", "all"} {
		frame := tc.writeConcerned(1, "wc-"+wc, wc)
		if frame.Opcode == transport.OpError {
			t.Errorf("w=%s write rejected by a healthy 5-member RF=3 cluster: %s", wc, string(frame.Payload))
		}
	}

	// Every node answers, including the two that hold no copy of the bucket:
	// those reads are forwarded to the owner.
	for index := range tc.nodes {
		missing := 0
		for _, key := range keys {
			if found, _ := tc.readFromNode(index, key); !found {
				missing++
			}
		}
		if missing > 0 {
			t.Errorf("node %d (%s) could not serve %d of %d reads", index+1, tc.addrs[index], missing, docs)
		}
	}
}

// coordinatorIndex picks a node to send client requests to that is neither the
// node taken down nor the bucket owner, so a test exercises forwarding to the
// owner and the owner's shipping path at once. Falls back to any live node when
// the cluster has no third member.
func (tc *testCluster) coordinatorIndex(t *testing.T, downIndex int, ownerID string) int {
	t.Helper()
	for i := range tc.nodes {
		if i == downIndex || tc.addrs[i] == ownerID {
			continue
		}
		if tc.nodes[i].client != nil {
			return i
		}
	}
	for i := range tc.nodes {
		if tc.nodes[i].client != nil {
			return i
		}
	}
	t.Fatalf("no live node to coordinate requests from")
	return -1
}

// nodeIndexByAddr maps a ring identity (an address in these tests) to the
// node's index in the test cluster.
func nodeIndexByAddr(t testing.TB, addrs []string, id string) int {
	t.Helper()
	for i, addr := range addrs {
		if addr == id {
			return i
		}
	}
	t.Fatalf("no test node with identity %s", id)
	return -1
}

func mustNode(t *testing.T, c *Cluster, id string) ring.Node {
	t.Helper()
	for _, m := range c.Members() {
		if m.ID == id {
			return m
		}
	}
	t.Fatalf("node %s not found in cluster", id)
	return ring.Node{}
}

// holdsKeyLocally reports whether a node's own store holds key in bucket.
func holdsKeyLocally(node *testNode, bucket, key string) bool {
	resp, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	return err == nil && resp != nil && resp.Found
}

// missingOnReplica polls until the replica's own store holds every key of keys
// that the owner's own store holds, or until the wait ends, and returns what
// is still missing.
func missingOnReplica(owner, replica *testNode, bucket string, keys []string, wait time.Duration) []string {
	deadline := time.Now().Add(wait)
	for {
		var missing []string
		for _, key := range keys {
			if holdsKeyLocally(owner, bucket, key) && !holdsKeyLocally(replica, bucket, key) {
				missing = append(missing, key)
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			return missing
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestEntryThatCouldNotBeQueuedIsOnTheReplicaAfterItReturns is spec
// T-44: a replica is down for longer than its queue holds, four writers at
// `one` keep writing, the replica comes back and a few later writes reach it
// first. On the starting tree every write past the queue was applied on the
// owner, answered with an error, and never delivered: the later writes moved
// the replica's position past them. The owner's timeout is shortened, as the
// bug file's probe did, so the starting tree's queue waits finish.
func TestEntryThatCouldNotBeQueuedIsOnTheReplicaAfterItReturns(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		if index == 0 {
			// One second, not 50 ms: replShipTimeout is half of this, and a
			// 64-entry replay frame outlasts 25 ms under -race, which turns
			// the replay into one frame per backoff step.
			c.wcTimeout = time.Second
		}
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, bson.M{"name": testDatabase, "write_concern": "one"})
	bucket := bucketOwnedBy(t, tc, 0, "unqueued")
	tc.ok(0, transport.OpCreateBucket, bson.M{"database": testDatabase, "name": bucket, "type": "hash", "write_concern": "one"})
	waitForServing(t, tc, testDatabase, bucket, 3)
	const replica = 1
	start := stopNode(t, tc, replica)

	const writers, writes = 4, 1300
	var next, errored atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		client := tc.dial(0)
		defer func() { _ = client.Close() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= writes {
					return
				}
				payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("u%04d", i),
					Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
				if err != nil {
					t.Errorf("marshal write %d: %v", i, err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				frame, err := client.SendRequest(ctx, transport.OpPut, payload)
				cancel()
				if err != nil {
					t.Errorf("write %d: %v", i, err)
					return
				}
				if frame.Opcode == transport.OpError {
					errored.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("%d of %d writes at one with a replica down were answered with an error", errored.Load(), writes)

	revived := start()
	keys := make([]string, 0, writes+40)
	for i := 0; i < writes; i++ {
		keys = append(keys, fmt.Sprintf("u%04d", i))
	}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("later%02d", i)
		keys = append(keys, key)
		tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
		time.Sleep(50 * time.Millisecond)
	}
	if missing := missingOnReplica(tc.nodes[0], revived, bucket, keys, 60*time.Second); len(missing) > 0 {
		t.Fatalf("%d of the %d keys the owner holds are missing on the replica 60 s after it returned (first %q)",
			len(missing), len(keys), missing[0])
	}
	if errored.Load() > 0 {
		t.Fatalf("%d writes at one were answered with an error because a replica was down; `one` needs only the owner", errored.Load())
	}
}

// TestLiveWritesDoNotOvertakeCatchUpAfterAnOwnerRestart is the case, made
// deterministic: a replica is down while its owner takes 1,000 writes of 8 KB,
// the owner restarts, the replica returns, and live writes start while the
// owner replays its backlog to the replica. The first replayed frame is held
// until 50 live writes have been routed and, where anything ships them, until
// the replica has acknowledged one. On the starting tree a live write shipped by
// the live sender moved the replica's position past the backlog, and the replay
// was then skipped as already applied.
func TestLiveWritesDoNotOvertakeCatchUpAfterAnOwnerRestart(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "overtaken")
	createBucketAt(tc, 0, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	const replica = 1
	replicaID := tc.addrs[replica]
	startReplica := stopNode(t, tc, replica)

	const backlog = 1000
	blob := strings.Repeat("b", 8<<10)
	keys := make([]string, 0, backlog+200)
	for i := 0; i < backlog; i++ {
		key := fmt.Sprintf("backlog%04d", i)
		keys = append(keys, key)
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
			Data: map[string]interface{}{"blob": blob}, WriteConcern: "one"})
	}

	// Counted on the owner as each live write reaches its routing.
	var owner atomic.Pointer[Cluster]
	var routed atomic.Int64
	var firstLive atomic.Uint64
	routedFifty := make(chan struct{})
	var fiftyOnce sync.Once
	enqueue := func(entry ReplEntry) {
		if entry.Bucket != bucket || !bytes.Contains(entry.Key, []byte("live")) {
			return
		}
		firstLive.CompareAndSwap(0, entry.Seq)
		if routed.Add(1) == 50 {
			fiftyOnce.Do(func() { close(routedFifty) })
		}
	}
	replicaEnqueueHook.Store(&enqueue)
	defer replicaEnqueueHook.Store(nil)
	var holdOnce sync.Once
	hold := func(peerID string, _ []ReplEntry) {
		if peerID != replicaID {
			return
		}
		holdOnce.Do(func() {
			select {
			case <-routedFifty:
			case <-time.After(30 * time.Second):
				return
			}
			// Where a live write can reach the replica before the replay, let it.
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if c := owner.Load(); c != nil {
					if p := c.peerFor(ring.Node{ID: replicaID, Addr: replicaID}); p.ackedThrough(c.self.ID) >= firstLive.Load() {
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	beforeReplayShipHook.Store(&hold)
	defer beforeReplayShipHook.Store(nil)

	startOwner := stopNode(t, tc, 0)
	startReplica()
	restartedOwner := startOwner()
	owner.Store(restartedOwner.server.core.cluster)

	var wg sync.WaitGroup
	var liveKeys sync.Map
	for w := 0; w < 8; w++ {
		client := tc.dial(0)
		defer func() { _ = client.Close() }()
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				key := fmt.Sprintf("live%d-%02d", w, i)
				payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
					Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
				if err != nil {
					t.Errorf("marshal %s: %v", key, err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				frame, err := client.SendRequest(ctx, transport.OpPut, payload)
				cancel()
				if err != nil || frame.Opcode == transport.OpError {
					t.Errorf("live write %s: %v", key, err)
					return
				}
				liveKeys.Store(key, true)
			}
		}(w)
	}
	wg.Wait()
	liveKeys.Range(func(key, _ any) bool {
		keys = append(keys, key.(string))
		return true
	})

	if missing := missingOnReplica(tc.nodes[0], tc.nodes[replica], bucket, keys, 30*time.Second); len(missing) > 0 {
		backlogMissing := 0
		for _, key := range missing {
			if strings.HasPrefix(key, "backlog") {
				backlogMissing++
			}
		}
		t.Fatalf("%d keys the owner holds are missing on the replica 30 s after the owner restarted, %d of them from the backlog of %d (first %q)",
			len(missing), backlogMissing, backlog, missing[0])
	}
}

// wipeKeepingIdentity stops node index and empties its data directory, the
// TLS identity under keys/ excepted: the wipe is of the node's data, and a new
// internode key would need its fingerprint pinned on every member first, which
// is an operator step this test is not about. It returns the base directory
// startNode brings the node back on.
func (tc *testCluster) wipeKeepingIdentity(index int) string {
	tc.t.Helper()
	base := tc.stopNodeKeepingData(index)
	dataDir := filepath.Join(base, fmt.Sprintf("n%d", index+1))
	identity := filepath.Join(tc.t.TempDir(), "keys")
	if err := os.Rename(filepath.Join(dataDir, "keys"), identity); err != nil {
		tc.t.Fatalf("setting node %d's keys aside: %v", index+1, err)
	}
	if err := os.RemoveAll(dataDir); err != nil {
		tc.t.Fatalf("wiping node %d's data directory: %v", index+1, err)
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		tc.t.Fatalf("re-creating node %d's data directory: %v", index+1, err)
	}
	if err := os.Rename(identity, filepath.Join(dataDir, "keys")); err != nil {
		tc.t.Fatalf("putting node %d's keys back: %v", index+1, err)
	}
	return base
}

// bringMembershipTo makes every running node of tc adopt via's membership.
func (tc *testCluster) bringMembershipTo(t *testing.T, via *Cluster) {
	t.Helper()
	for index, node := range tc.nodes {
		if node.server == nil || node.server.core.cluster == via {
			continue
		}
		// What via's answer to the node's next gossip would carry: its view,
		// and the epochs the node missed (step 9.4).
		member := node.server.core.cluster
		answer := GossipResponse{Nodes: via.memberNodes(), Members: via.memberIDs(), Epoch: via.Epoch(), History: via.historyAbove(member.Epoch())}
		if err := member.reconcileAnswer(via.self.ID, answer); err != nil {
			t.Fatalf("bringing node %d's membership to %s's: %v", index+1, via.self.ID, err)
		}
	}
}

// copyDirectory copies the regular files of src into dst, which it creates.
func copyDirectory(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatalf("creating %s: %v", dst, err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("reading %s: %v", src, err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(src, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dst, entry.Name()), contents, 0o600); err != nil {
			t.Fatalf("writing %s: %v", entry.Name(), err)
		}
	}
}

// incarnationLinks counts the links of the incarnation chain in a WAL
// directory, from the file itself — 25 bytes a link, packed in one binary
// value of a BSON document (storage/wal_incarnation.go) — so the test also
// compiles against a tree that has no chain. No file is no link.
func incarnationLinks(t *testing.T, walDir string) int {
	t.Helper()
	stored, err := os.ReadFile(filepath.Join(walDir, "incarnation.bson"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatalf("reading the incarnation chain: %v", err)
	}
	_, links, ok := bson.Raw(stored).Lookup("links").BinaryOK()
	if !ok {
		t.Fatalf("the incarnation chain in %s has no links value", walDir)
	}
	return len(links) / 25
}

// TestReplicaWithAPositionAboveTheSourceTailDoesNotSkipAcknowledge is
// T-83, A-4.5: the owner's log goes backwards under the same node
// id — its wal/ is put back from a copy taken 20 entries ago — and ten writes
// reach it while every replica's handshake is held. Each replica must apply the
// ten, once. The link the owner minted when it opened its log has the base it
// found there, so the replicas' positions are clamped below the ten however long
// the handshake waits. On the starting tree the replicas skipped and
// acknowledged them; with a base read at the handshake, they would skip them
// too. Since BUG-0481 part 2 the owner holds the ten until its replicas have
// answered, and gives them sequences above every position they hold.
func TestReplicaWithAPositionAboveTheSourceTailDoesNotSkipAcknowledge(t *testing.T) {
	var restoring atomic.Bool
	release := make(chan struct{})
	logged := &syncBuffer{}
	tc := newTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		if !restoring.Load() {
			return
		}
		// The restored owner: every push to each replica, its handshake
		// first, waits for release, and what it logs is kept.
		c.logger = slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
		for _, member := range c.Members() {
			if member.ID != c.self.ID {
				wrapPeerConn(c.peerFor(member), func(inner peerConn) peerConn { return &heldPushConn{peerConn: inner, release: release} })
			}
		}
	})
	defer tc.stop()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	push := func(element, concern string) {
		t.Helper()
		frame := tc.frame(ownerIndex, transport.OpFIFOPush, PushRequest{
			Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"e": element}, WriteConcern: concern,
		})
		if frame.Opcode == transport.OpError {
			t.Fatalf("push %s at %s: %s", element, concern, frame.Payload)
		}
	}
	for i := 0; i < 10; i++ {
		push(fmt.Sprintf("kept%02d", i), "all")
	}
	walDir := filepath.Join(tc.nodes[ownerIndex].cfg.Storage.DataDir, "wal")
	copied := filepath.Join(t.TempDir(), "wal")
	copyDirectory(t, walDir, copied)
	linksBefore := incarnationLinks(t, copied)
	for i := 0; i < 20; i++ {
		push(fmt.Sprintf("lost%02d", i), "all")
	}

	base := tc.stopNodeKeepingData(ownerIndex)
	if err := os.RemoveAll(walDir); err != nil {
		t.Fatalf("removing the owner's log: %v", err)
	}
	copyDirectory(t, copied, walDir)
	restoring.Store(true)
	tc.nodes[ownerIndex] = tc.startNode(ownerIndex, base, tc.token)
	restoring.Store(false)
	reused := make(chan struct{})
	go func() {
		defer close(reused)
		for i := 0; i < 10; i++ {
			frame := tc.frame(ownerIndex, transport.OpFIFOPush, PushRequest{
				Database: testDatabase, Bucket: testBucket, Data: map[string]interface{}{"e": fmt.Sprintf("reused%02d", i)}, WriteConcern: "one",
			})
			if frame.Opcode == transport.OpError {
				t.Errorf("push reused%02d at one: %s", i, frame.Payload)
			}
		}
	}()
	// The ten wait in the start hold while the handshakes are held; released
	// within the write-concern timeout, they are taken.
	time.Sleep(time.Second)
	close(release)
	<-reused
	push("after", "all")

	// The owner's own store is not restored with its log, so what it holds is
	// beside the point: every replica must hold every element pushed, once.
	const pushed = 10 + 20 + 10 + 1
	for index := range tc.nodes {
		if index == ownerIndex {
			continue
		}
		if got := tc.localCount(index); got != pushed {
			t.Errorf("node %d holds %d elements of the queue, want %d: the entries the restored owner wrote at sequences "+
				"the replica held other entries at were skipped, or applied twice", index+1, got, pushed)
		}
	}
	if links := incarnationLinks(t, walDir); links != linksBefore+1 {
		t.Errorf("the restored owner's chain has %d links; the copy had %d, and the open must add one", links, linksBefore)
	}
	for index := range tc.nodes {
		if index == ownerIndex {
			continue
		}
		if !strings.Contains(logged.String(), tc.addrs[index]) || !strings.Contains(logged.String(), "this log now ends at") {
			t.Errorf("the restored owner did not log the divergence naming %s; it logged:\n%s", tc.addrs[index], logged.String())
		}
	}
}

// TestReplicaAppliesAWriteFromASourceWhoseLogRestarted is T-82, with
// the wipe nobody had performed: an owner whose replicas hold a position above
// 50 in its log is stopped, its data directory emptied, and it comes back under
// the same node id, removed and re-added so it is seeded. Its new log starts
// again at 1, and moves past the replicas' positions before its first write
// (BUG-0481 part 2). On the starting tree every replica took the new writes for
// sequences it had applied already, skipped them, and acknowledged them: a
// write at all answered success and only the owner held it.
func TestReplicaAppliesAWriteFromASourceWhoseLogRestarted(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
	})
	defer tc.stop()
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	survivorIndex := (ownerIndex + 1) % 3
	ownerID := tc.addrs[ownerIndex]
	// The database is created through a survivor, so its create is in a log
	// the wipe leaves.
	tc.ok(survivorIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for i := 0; i < 55; i++ {
		if frame := tc.writeConcerned(ownerIndex, fmt.Sprintf("before%02d", i), "all"); frame.Opcode == transport.OpError {
			t.Fatalf("write %d at all: %s", i, frame.Payload)
		}
	}
	for index := range tc.nodes {
		if index == ownerIndex {
			continue
		}
		if position := appliedSnapshot(tc.nodes[index].server.core.cluster)[ownerID]; position <= 50 {
			t.Fatalf("setup: node %d's position for the owner is %d, want above 50", index+1, position)
		}
	}

	base := tc.wipeKeepingIdentity(ownerIndex)
	survivor := tc.nodes[survivorIndex].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, ownerID); err != nil {
		t.Fatalf("removing the wiped node: %v", err)
	}
	tc.bringMembershipTo(t, survivor)
	if err := survivor.proposeJoin(ctx, ring.Node{ID: ownerID, Addr: ownerID}); err != nil {
		t.Fatalf("adding the wiped node again: %v", err)
	}
	tc.bringMembershipTo(t, survivor)
	tc.nodes[ownerIndex] = tc.startNode(ownerIndex, base, tc.token)
	tc.bringMembershipTo(t, survivor)
	wiped := tc.nodes[ownerIndex].server.core
	if err := wiped.cluster.ensureMigrated(ctx, testDatabase, testBucket); err != nil {
		t.Fatalf("seeding the re-added node: %v", err)
	}
	// Since BUG-0481 part 2 the new log moves past every position a majority
	// of its peers hold before it takes a write, so it never starts below them;
	// the clamp this test was written for is the second line now.
	if last := wiped.wal.LastSequence(); last <= 50 {
		t.Fatalf("the re-added node's log is at %d; it must have moved past the replicas' positions, above 50", last)
	}
	// The re-added node took the bucket over in a term of its own (step
	// 10b.5); each replica takes that term before the term's entries apply to
	// it (step 10b.6), and a write at all counts only the copies that have.
	for index := range tc.nodes {
		if index != ownerIndex {
			waitFollowing(t, tc.nodes[index].server.core, wiped, testDatabase, testBucket)
		}
	}

	if frame := tc.writeConcerned(ownerIndex, "after-the-wipe", "all"); frame.Opcode == transport.OpError {
		t.Fatalf("a write at all through the re-added node: %s", frame.Payload)
	}
	for index := range tc.nodes {
		if found, _ := tc.readFromNode(index, "after-the-wipe"); !found {
			t.Errorf("node %d does not hold a write at all through the re-added node: a position from the wiped log "+
				"made it skip and acknowledge the new log's entry", index+1)
		}
	}
}

// TestCatchUpDeliversDatabaseLevelDDLToAPeerThatMissedIt: node 3
// is stopped; a `create database` and a `drop database` go through node 1,
// which is then restarted — its queue to node 3 is lost — and its log pruned
// before node 3 returns. Database DDL is addressed to every member, so the
// entries are there only if retention held them for node 3, and node 3 must
// list the new database and not the dropped one.
func TestCatchUpDeliversDatabaseLevelDDLToAPeerThatMissedIt(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, withoutRetentionLoop)
	defer tc.stop()
	const coordinator, missing = 0, 2

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: "keep"})
	waitListsDatabase(t, tc.nodes[missing], "keep", true)
	startMissing := stopNode(t, tc, missing)

	smallSegments(tc.nodes[coordinator])
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: "newdb"})
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: "keep"})
	restarted := tc.restartNode(coordinator)
	smallSegments(restarted)
	// The restarted coordinator takes no write of its own while node 3 is
	// down: one of its two peers is not a majority (BUG-0481 part 2). The pads
	// go through node 2, and reach the coordinator's log as the entries it
	// applies as a replica, which is all retention needs to have segments to
	// prune.
	const padder = 1
	before := restarted.server.core.wal.LastSequence()
	for i := 0; i < 4; i++ {
		tc.ok(padder, transport.OpCreateDB, CreateDatabaseRequest{Name: fmt.Sprintf("pad%02d", i)})
	}
	if !eventually(func() bool { return restarted.server.core.wal.LastSequence() >= before+4 }) {
		t.Fatalf("the restarted coordinator's log did not take the four pads node 2 sent it")
	}
	if err := restarted.server.core.cluster.pruneWAL(); err != nil {
		t.Fatalf("retention pass on the restarted coordinator: %v", err)
	}

	returned := startMissing()
	waitListsDatabase(t, returned, "newdb", true)
	waitListsDatabase(t, returned, "keep", false)
}

// TestGetFastOnACoordinatorThatDoesNotHoldTheBucketIsServedByTheOwner is
// The defect: the router read GET_FAST's compact binary payload as BSON, found no
// bucket in it, and ran the read on the coordinator's own storage, so a node
// that did not hold the bucket answered a miss for a key the owner holds. At
// replication factor 1 exactly one node holds the bucket; a GET_FAST over every
// other node's driver listener must answer what the owner holds.
func TestGetFastOnACoordinatorThatDoesNotHoldTheBucketIsServedByTheOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k01",
		Data: map[string]interface{}{"n": int32(7)}, WriteConcern: "one",
	})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner of %s.%s: %v", testDatabase, testBucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for index := range tc.nodes {
		answer, err := tc.nodes[index].client.SendGetFast(ctx, transport.GetFastRequest{Database: testDatabase, Bucket: testBucket, Key: "k01"})
		if err != nil {
			t.Fatalf("GET_FAST through node %d: %v", index+1, err)
		}
		if answer.Opcode != transport.OpGetFastResponse {
			t.Fatalf("node %d (owner is node %d) answered %s, want GET_FAST_RESPONSE", index+1, ownerIndex+1, answerText(answer))
		}
		status, document, err := transport.UnmarshalGetFastResponse(answer.Payload)
		if err != nil {
			t.Fatalf("node %d: decoding the GET_FAST answer: %v", index+1, err)
		}
		if status != transport.GetFastFound || len(document) == 0 {
			t.Errorf("node %d (owner is node %d) answered status %d with %d document bytes, want the owner's document", index+1, ownerIndex+1, status, len(document))
		}
	}
}
