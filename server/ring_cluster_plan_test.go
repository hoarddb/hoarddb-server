package server

import (
	"crypto/tls"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

const ringPlanSelf = "127.0.0.1:4433"

// ringPlanFixture is the node's own identity for planRingPeers and a config of node-a in
// cluster mode, listening on ringPlanSelf, with edit applied.
func ringPlanFixture(t *testing.T, edit func(*config.Config)) (*config.Config, *tls.Config, string) {
	t.Helper()
	identity, fingerprint, err := transport.MakeServerTLSConfig(filepath.Join(t.TempDir(), "keys"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Server.NodeID = "node-a"
	cfg.Server.Listen = ringPlanSelf
	cfg.Cluster.Enabled = true
	if edit != nil {
		edit(cfg)
	}
	return cfg, identity, fingerprint
}

func ringPlanFingerprint(t *testing.T) string {
	t.Helper()
	return ringClusterFingerprint(t, filepath.Join(t.TempDir(), "keys"))
}

// TestRingClusterBootstrapsOnlyWithNoOtherSeed: [GUARD a node bootstraps when its seed list is
// empty or names only itself, and joins when it names any other node; the documented "list the
// nodes that already exist" rule].
func TestRingClusterBootstrapsOnlyWithNoOtherSeed(t *testing.T) {
	pinB := "node-b=" + ringPlanFingerprint(t)
	for _, tc := range []struct {
		name  string
		seeds []string
		join  bool
	}{
		{"no seeds", nil, false},
		{"only itself", []string{"node-a@" + ringPlanSelf}, false},
		{"itself and another", []string{"node-a@" + ringPlanSelf, "node-b@127.0.0.1:4443"}, true},
		{"only another", []string{"node-b@127.0.0.1:4443"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, identity, fingerprint := ringPlanFixture(t, func(cfg *config.Config) {
				cfg.HashRing.SeedNodes = tc.seeds
				cfg.HashRing.SeedFingerprints = []string{pinB}
			})
			plan, err := planRingPeers(cfg, fingerprint, identity)
			if err != nil {
				t.Fatal(err)
			}
			if plan.join != tc.join {
				t.Fatalf("seeds %v: join = %v, want %v", tc.seeds, plan.join, tc.join)
			}
		})
	}
}

// TestRingClusterBootstrapAndJoinLeaveDifferentState: [GUARD the decision reaches the node: a
// bootstrapping node commits a cluster of one, a joining node commits nothing and waits].
func TestRingClusterBootstrapAndJoinLeaveDifferentState(t *testing.T) {
	bootstrapped := ringClusterService(t, ringClusterPorts(t), nil, nil)
	ringClusterStart(t, bootstrapped)
	if members := bootstrapped.ringNode.State().Members; len(members) != 1 {
		t.Fatalf("a node with no seeds committed members %+v, want itself", members)
	}
	if notice, _ := ringStartNotice(false, 1, bootstrapped.ringNode.State().StateSeq, "node-a", ringPlanSelf); notice == "" {
		t.Fatalf("a freshly minted cluster (state_seq %d) gets no notice", bootstrapped.ringNode.State().StateSeq)
	}
	joining := ringClusterService(t, ringClusterPorts(t), nil, func(cfg *config.Config) {
		cfg.HashRing.SeedNodes = []string{"node-b@127.0.0.1:1"}
		cfg.HashRing.SeedFingerprints = []string{"node-b=" + ringPlanFingerprint(t)}
	})
	ringClusterStart(t, joining)
	if members := joining.ringNode.State().Members; len(members) != 0 {
		t.Fatalf("a joining node committed members %+v before anyone added it", members)
	}
}

// TestRingClusterRefusesASelfSeedAtAnotherAddress: [GUARD a seed that names this node's id at an
// address other than cluster.advertise is refused naming both, not treated as another node].
func TestRingClusterRefusesASelfSeedAtAnotherAddress(t *testing.T) {
	cfg, identity, fingerprint := ringPlanFixture(t, func(cfg *config.Config) {
		cfg.HashRing.SeedNodes = []string{"node-a@localhost:4433"}
	})
	_, err := planRingPeers(cfg, fingerprint, identity)
	if err == nil {
		t.Fatal("a seed naming this node at localhost:4433 while it advertises " + ringPlanSelf + " was accepted")
	}
	for _, want := range []string{"localhost:4433", ringPlanSelf, "node-a"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

// TestRingClusterTakesTheMappedPinForm: [GUARD host:port=SHA256 is a pin for that address, not a
// node id, and it binds the seed listed at that address].
func TestRingClusterTakesTheMappedPinForm(t *testing.T) {
	fpB := ringPlanFingerprint(t)
	cfg, identity, fingerprint := ringPlanFixture(t, func(cfg *config.Config) {
		cfg.HashRing.SeedNodes = []string{"node-b@node-b.example:4433"}
		cfg.HashRing.SeedFingerprints = []string{"node-b.example:4433=" + fpB}
	})
	plan, err := planRingPeers(cfg, fingerprint, identity)
	if err != nil {
		t.Fatalf("the documented mapped form was refused: %v", err)
	}
	if got := plan.fingerprints["node-b"]; got != fpB {
		t.Fatalf("node-b is pinned to %q, want %q", got, fpB)
	}
	if _, wrong := plan.fingerprints["node-b.example:4433"]; wrong {
		t.Fatal("an address was read as a node id")
	}
}

// TestRingClusterRefusesBadPinEntries: [GUARD an empty id, one key under two ids, and one id under
// two keys are refused naming the entries].
func TestRingClusterRefusesBadPinEntries(t *testing.T) {
	fp := ringPlanFingerprint(t)
	other := ringPlanFingerprint(t)
	for _, tc := range []struct {
		name string
		pins []string
		want string
	}{
		{"empty id", []string{"=" + fp}, "empty node id"},
		{"one key under two ids", []string{"node-b=" + fp, "node-c=" + fp}, "node-b"},
		{"self key under another id", []string{"node-b=SELF"}, "node-a"},
		{"one id under two keys", []string{"node-b=" + fp, "node-b=" + other}, "two keys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, identity, fingerprint := ringPlanFixture(t, nil)
			for _, pin := range tc.pins {
				cfg.HashRing.SeedFingerprints = append(cfg.HashRing.SeedFingerprints, strings.ReplaceAll(pin, "SELF", fingerprint))
			}
			_, err := planRingPeers(cfg, fingerprint, identity)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("pins %v: error %v, want one naming %q", tc.pins, err, tc.want)
			}
		})
	}
}

// TestRingClusterRefusesAWitnessThatWouldBootstrap: [GUARD a witness with no other seed is refused
// naming hash_ring.seed_nodes].
func TestRingClusterRefusesAWitnessThatWouldBootstrap(t *testing.T) {
	for _, seeds := range [][]string{nil, {"node-a@" + ringPlanSelf}} {
		cfg, identity, fingerprint := ringPlanFixture(t, func(cfg *config.Config) {
			cfg.Cluster.Witness = true
			cfg.HashRing.SeedNodes = seeds
		})
		_, err := planRingPeers(cfg, fingerprint, identity)
		if err == nil || !strings.Contains(err.Error(), "hash_ring.seed_nodes") {
			t.Fatalf("witness with seeds %v: error %v, want one naming hash_ring.seed_nodes", seeds, err)
		}
	}
}

// ringPlanTLSServer serves TLS with a fresh key on loopback and returns its address and
// fingerprint; it answers nothing, the test only completes handshakes against it.
func ringPlanTLSServer(t *testing.T) (addr, fingerprint string) {
	t.Helper()
	identity, fp, err := transport.MakeServerTLSConfig(filepath.Join(t.TempDir(), "keys"))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, _ = conn.Read(make([]byte, 1))
				_ = conn.Close()
			}()
		}
	}()
	return ln.Addr().String(), fp
}

// TestRingClusterTrustHoldsADialToThePinnedMember: [GUARD the options the node opens with dial a
// member by id: node-b is held to node-b's key, and another pinned member's key at node-b's
// address is refused].
func TestRingClusterTrustHoldsADialToThePinnedMember(t *testing.T) {
	addrB, fpB := ringPlanTLSServer(t)
	addrC, fpC := ringPlanTLSServer(t)
	cfg, identity, fingerprint := ringPlanFixture(t, func(cfg *config.Config) {
		cfg.Server.Listen = ringClusterPorts(t)
		cfg.HashRing.SeedNodes = []string{"node-b@" + addrB}
		cfg.HashRing.SeedFingerprints = []string{"node-b=" + fpB, "node-c=" + fpC}
	})
	plan, err := planRingPeers(cfg, fingerprint, identity)
	if err != nil {
		t.Fatal(err)
	}
	options, listener, err := plan.clusterOptions(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	handshake := func(id, addr string) error {
		conf, err := options.TLS(id, addr)
		if err != nil {
			return err
		}
		raw, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = raw.Close() }()
		return tls.Client(raw, conf).Handshake()
	}
	if err := handshake("node-b", addrB); err != nil {
		t.Fatalf("a dial to node-b at its own address was refused: %v", err)
	}
	if err := handshake("node-b", addrC); err == nil {
		t.Fatal("a dial to node-b accepted node-c's pinned key")
	}
	if err := handshake("node-d", addrB); err == nil {
		t.Fatal("a dial to a member with no pin was given a TLS config")
	}
}

// TestRingClusterModeRunsNoLegacyRing: [GUARD with cluster.enabled the seed list drives the
// entry-ring cluster only: the earlier hash ring, its log and its gossip are not built].
func TestRingClusterModeRunsNoLegacyRing(t *testing.T) {
	qs := ringClusterService(t, ringClusterPorts(t), nil, func(cfg *config.Config) {
		cfg.HashRing.SeedNodes = []string{"node-b@127.0.0.1:1"}
		cfg.HashRing.SeedFingerprints = []string{"node-b=" + ringPlanFingerprint(t)}
	})
	if qs.core.cluster != nil {
		t.Fatal("cluster mode built the earlier hash ring from the same seed list")
	}
	if qs.core.wal != nil {
		t.Fatal("cluster mode opened the earlier ring's write-ahead log")
	}
}

// TestRingClusterSeedListNamingOnlyItselfBootstraps: [GUARD the headline rule through Start: a
// seed list that names only this node starts a cluster of one, it does not wait to be joined].
func TestRingClusterSeedListNamingOnlyItselfBootstraps(t *testing.T) {
	listen := ringClusterPorts(t)
	qs := ringClusterService(t, listen, nil, func(cfg *config.Config) {
		cfg.HashRing.SeedNodes = []string{"node-a@" + listen}
		cfg.HashRing.SeedFingerprints = []string{"node-c=" + ringPlanFingerprint(t)}
	})
	ringClusterStart(t, qs)
	if members := qs.ringNode.State().Members; len(members) != 1 {
		t.Fatalf("a node whose seed list names only itself committed members %+v, want itself", members)
	}
	if qs.ringJoin {
		t.Fatal("a node whose seed list names only itself planned to join")
	}
}
