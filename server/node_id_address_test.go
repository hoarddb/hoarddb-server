package server

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestNodesGivenDifferentSeedListsNeverRunOneIDAtTwoAddresses is BUG-0598 path
// 1: node A lists dup@A0, solo@A2 and node B lists dup@A1, solo@A2, each list
// valid on its own, with the node id defaulted (one hostname on every node).
// Member agreement compared ids only, so every node confirmed {dup, solo} and A
// and B both ran as dup, both owned its buckets and both accepted user changes
// as the auth master. The cluster must refuse the second dup instead, and say
// which id and which two addresses.
func TestNodesGivenDifferentSeedListsNeverRunOneIDAtTwoAddresses(t *testing.T) {
	const defaultedHostname = "build-host"
	ports := reserveClusterPorts(t, 3)
	a0, a1, a2 := ports[0].ringAddr, ports[1].ringAddr, ports[2].ringAddr
	seedsOfA := []string{"dup@" + a0, "solo@" + a2}
	seedsOfB := []string{"dup@" + a1, "solo@" + a2}
	tc := startTestClusterOn(t, ports, 2, func(index int, cfg *config.Config) {
		cfg.Server.NodeID = defaultedHostname
		cfg.HashRing.SeedNodes = seedsOfA
		if index == 1 {
			cfg.HashRing.SeedNodes = seedsOfB
		}
	}, nil)

	// A and solo agree on everything and confirm. B either confirms (two nodes
	// run as dup) or is refused by the member that lists dup elsewhere.
	nodeA, nodeB, nodeSolo := tc.nodes[0].server.core, tc.nodes[1].server.core, tc.nodes[2].server.core
	deadline := time.Now().Add(30 * time.Second)
	for {
		settled := !nodeA.cluster.bootstrapped.Load() && !nodeSolo.cluster.bootstrapped.Load() &&
			(!nodeB.cluster.bootstrapped.Load() || nodeB.cluster.membershipRefusal() != nil)
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the nodes did not settle: A confirmed %v, solo confirmed %v, B confirmed %v, B refused by %v",
				!nodeA.cluster.bootstrapped.Load(), !nodeSolo.cluster.bootstrapped.Load(),
				!nodeB.cluster.bootstrapped.Load(), nodeB.cluster.membershipRefusal())
		}
		time.Sleep(100 * time.Millisecond)
	}

	master, err := nodeA.authMaster()
	if err != nil {
		t.Fatalf("authMaster: %v", err)
	}
	if master.ID != "dup" {
		t.Fatalf("precondition: the auth master of {dup, solo} is %q; this test needs it to be dup", master.ID)
	}

	var accepted []string
	var refusalOfB string
	for index := range tc.nodes {
		frame := tc.frame(index, transport.OpUserCreate, clusterCreateRequest(t, "bug0598-"+tc.addrs[index], "user"+string(rune('a'+index))))
		if frame.Opcode != transport.OpError {
			accepted = append(accepted, tc.addrs[index])
			continue
		}
		if index == 1 {
			refusalOfB = string(frame.Payload)
		}
	}
	if len(accepted) > 1 {
		t.Fatalf("auth master dup: user changes accepted by %d nodes %s; at most one node may be the master",
			len(accepted), strings.Join(accepted, ", "))
	}
	for _, want := range []string{`"dup"`, a0, a1} {
		if !strings.Contains(refusalOfB, want) {
			t.Errorf("the user change refused on B does not name %s:\n%s", want, refusalOfB)
		}
	}
	membership, detail := nodeB.healthMembership()
	if membership != MembershipRefused {
		t.Errorf("B's membership = %q, want %q", membership, MembershipRefused)
	}
	for _, want := range []string{`"dup"`, a0, a1, "HOARDDB_SEED_NODES"} {
		if !strings.Contains(detail, want) {
			t.Errorf("B's membership detail does not name %s:\n%s", want, detail)
		}
	}
}

// TestANodeNeverTakesAListedIDAtAnotherAddress is BUG-0598 path 2: a node took
// the seed entry listed under its configured node id at another node's address,
// so two nodes ran as that id. config.Load fills the node id with the hostname,
// so this happened without HOARDDB_NODE_ID too, on hosts cloned from one image.
func TestANodeNeverTakesAListedIDAtAnotherAddress(t *testing.T) {
	const a0, a1, a2 = "127.0.0.1:24433", "127.0.0.1:24443", "127.0.0.1:24453"
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("an explicit node id listed at another address, this node unlisted", func(t *testing.T) {
		cfg := duplicateIDNodeConfig(t, a1, "dup", []string{"dup@" + a0, "solo@" + a2})
		cluster, err := NewCluster(newTestServer(t), cfg, discard)
		if err == nil {
			t.Logf("resolved self %s@%s", cluster.self.ID, cluster.self.Addr)
			cluster.Stop()
		}
		requireRefusalNames(t, "starting the node at "+a1+" as dup with dup listed at "+a0, err,
			`"dup"`, a0, a1, "HOARDDB_NODE_ID")
	})

	t.Run("an explicit node id listed at another address, this node listed under another id", func(t *testing.T) {
		// The node ran silently as other, the id its address is listed under,
		// while its operator had set it to run as dup: refused instead, naming
		// both entries.
		cfg := duplicateIDNodeConfig(t, a1, "dup", []string{"other@" + a1, "dup@" + a0, "solo@" + a2})
		cluster, err := NewCluster(newTestServer(t), cfg, discard)
		if err == nil {
			t.Logf("resolved self %s@%s", cluster.self.ID, cluster.self.Addr)
			cluster.Stop()
		}
		requireRefusalNames(t, "starting the node at "+a1+" (listed as other) as dup with dup listed at "+a0, err,
			`"dup"`, `"other"`, a0, a1, "HOARDDB_NODE_ID")
	})

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		t.Skipf("no hostname to default the node id to: %v", err)
	}

	t.Run("a hostname default listed first, this node listed under another id", func(t *testing.T) {
		s := newTestServer(t)
		cfg := loadDefaultedNodeConfig(t, a1, []string{hostname + "@" + a0, "other@" + a1, "solo@" + a2})
		cluster, err := NewCluster(s, cfg, discard)
		if err != nil {
			t.Fatalf("NewCluster: %v", err)
		}
		defer cluster.Stop()
		if cluster.self.ID != "other" || cluster.self.Addr != a1 {
			t.Fatalf("the node at %s, listed there as other, runs as %s@%s", a1, cluster.self.ID, cluster.self.Addr)
		}
	})

	t.Run("a hostname default listed at another address, this node unlisted", func(t *testing.T) {
		s := newTestServer(t)
		cfg := loadDefaultedNodeConfig(t, a1, []string{hostname + "@" + a0, "solo@" + a2})
		cluster, err := NewCluster(s, cfg, discard)
		if err == nil {
			t.Logf("resolved self %s@%s", cluster.self.ID, cluster.self.Addr)
			cluster.Stop()
		}
		requireRefusalNames(t, "starting the node at "+a1+" whose hostname is listed at "+a0, err,
			`"`+hostname+`"`, a0, a1, "HOARDDB_NODE_ID")
	})
}

// loadDefaultedNodeConfig is a node's configuration as config.Load builds it
// with no HOARDDB_NODE_ID, so the node id is the hostname default. It sets the
// environment Load reads, so the caller builds its test server first.
func loadDefaultedNodeConfig(t *testing.T, listen string, seeds []string) *config.Config {
	t.Helper()
	t.Setenv("HOARDDB_NODE_ID", "")
	t.Setenv("HOARDDB_LISTEN", listen)
	t.Setenv("HOARDDB_SEED_NODES", strings.Join(seeds, ","))
	t.Setenv("HOARDDB_SEED_FINGERPRINTS", "SHA256:"+strings.Repeat("ab", 32))
	cfg, err := config.LoadAllowingOldReplicatedMode("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	loaded := duplicateIDNodeConfig(t, listen, cfg.Server.NodeID, seeds)
	cfg.Storage.DataDir = loaded.Storage.DataDir
	return cfg
}

// TestASeedListNamingOneAddressUnderTwoIDsRefusesToStart is BUG-0598 path 3:
// a@A0, b@A0 made the node at A0 run as b and left a a member nobody runs, the
// owner of its placements and perhaps the auth master. Every node refuses the
// list, naming both ids and the address.
func TestASeedListNamingOneAddressUnderTwoIDsRefusesToStart(t *testing.T) {
	const a0, a1, a2 = "127.0.0.1:24433", "127.0.0.1:24443", "127.0.0.1:24453"
	seeds := []string{"a@" + a0, "b@" + a0, "c@" + a1, "solo@" + a2}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, listen := range []string{a0, a1} {
		cfg := duplicateIDNodeConfig(t, listen, "build-host", seeds)
		cluster, err := NewCluster(newTestServer(t), cfg, discard)
		if err == nil {
			cluster.Stop()
		}
		requireRefusalNames(t, "starting the node at "+listen+" with seeds "+strings.Join(seeds, ","), err,
			`"a"`, `"b"`, a0, "HOARDDB_SEED_NODES")
	}
}

// TestANodeIsNotConfirmedByItsOwnAnswer is BUG-0598 path 1 as real servers ran
// it: cmd/server skips a seed only when it equals HOARDDB_LISTEN, so a node
// listed as dup@<its own address> gossips with itself at start, and its own
// answer counted as the agreement of member dup. With two members that is
// every other member, so the node at the second dup address confirmed itself
// before solo could refuse it, and served as the auth master.
func TestANodeIsNotConfirmedByItsOwnAnswer(t *testing.T) {
	const a1, a2 = "127.0.0.1:24443", "127.0.0.1:24453"
	s := newTestServer(t)
	cfg := duplicateIDNodeConfig(t, a1, "build-host", []string{"dup@" + a1, "solo@" + a2})
	cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer cluster.Stop()
	s.cluster = cluster
	if !cluster.bootstrapped.Load() {
		t.Fatal("precondition: a node started from its seed list is not confirmed yet")
	}

	if err := cluster.reconcileRequest(GossipRequest{
		NodeID:       cluster.self.ID,
		Addr:         cluster.self.Addr,
		Epoch:        cluster.Epoch(),
		Nodes:        cluster.memberNodes(),
		Members:      cluster.memberIDs(),
		Bootstrapped: true,
	}); err != nil {
		t.Fatalf("reconciling with its own view: %v", err)
	}
	if !cluster.bootstrapped.Load() {
		t.Fatalf("node %s@%s confirmed its member list on its own answer; solo has not answered", cluster.self.ID, cluster.self.Addr)
	}
}
