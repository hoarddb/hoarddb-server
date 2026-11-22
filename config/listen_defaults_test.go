package config

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

func hostOf(t *testing.T, address string) string {
	t.Helper()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split %q: %v", address, err)
	}
	return host
}

// TestDefaultListenAddressesAreLoopback is SEC-1 (owner ruling 32). A binary
// started with no arguments listens on this machine only; the Dockerfile opts
// the container into every interface.
func TestDefaultListenAddressesAreLoopback(t *testing.T) {
	defaults := DefaultConfig()
	internode, err := transport.InternodePeerAddr(defaults.Server.Listen)
	if err != nil {
		t.Fatalf("derive internode address: %v", err)
	}
	listeners := map[string]string{
		"client (HOARDDB_TCP_LISTEN)":       defaults.TCP.Listen,
		"ring base (HOARDDB_LISTEN)":        defaults.Server.Listen,
		"internode (derived from the ring)": internode,
		"metrics (HOARDDB_METRICS_LISTEN)":  defaults.Metrics.Listen,
	}
	for name, address := range listeners {
		if host := hostOf(t, address); host != "127.0.0.1" {
			t.Errorf("%s defaults to %q, want 127.0.0.1", name, address)
		}
	}
}

func clusterConfig(t *testing.T, listen string, seeds ...string) *Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = listen
	cfg.HashRing.SeedNodes = seeds
	cfg.HashRing.SeedFingerprints = []string{"SHA256:" + strings.Repeat("ab", 32)}
	return cfg
}

// TestClusteredNodeOnLoopbackIsRefused: a node that advertises 127.0.0.1 to
// peers on other machines cannot be reached by them, so the start is refused
// and the message names the setting that fixes it.
func TestClusteredNodeOnLoopbackIsRefused(t *testing.T) {
	cfg := clusterConfig(t, DefaultConfig().Server.Listen, "192.0.2.2:4433", "192.0.2.3:4433")
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted a node on 127.0.0.1 with seeds on other hosts")
	}
	for _, want := range []string{"HOARDDB_LISTEN", "127.0.0.1", "192.0.2.2:4433"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestClusterListenAddressesThatAreReachableAreAccepted(t *testing.T) {
	tests := []struct {
		name   string
		listen string
		nodeID string
		seeds  []string
	}{
		{"node address with remote seeds", "192.0.2.1:4433", "", []string{"192.0.2.1:4433", "192.0.2.2:4433"}},
		{"wildcard with its node id listed as id@address", "0.0.0.0:4433", "node1", []string{"node1@192.0.2.1:4433", "node2@192.0.2.2:4433"}},
		{"wildcard with loopback seeds, a cluster on one machine", "0.0.0.0:4433", "", []string{"127.0.0.1:4433", "127.0.0.1:5433"}},
		{"hostname with remote seeds", "node1:4433", "", []string{"node1@node1:4433", "node2@node2:4433"}},
		{"node address with a loopback seed on another port", "192.0.2.1:4433", "", []string{"192.0.2.1:4433", "n2@127.0.0.1:5433"}},
		{"loopback with loopback seeds, a cluster on one machine", "127.0.0.1:4433", "", []string{"127.0.0.1:4433", "127.0.0.1:5433"}},
		{"loopback and no seeds, a single node", "127.0.0.1:4433", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := clusterConfig(t, tt.listen, tt.seeds...)
			cfg.Server.NodeID = tt.nodeID
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestClusteredNodeWithLoopbackInternodeOverrideIsRefused(t *testing.T) {
	cfg := clusterConfig(t, "192.0.2.1:4433", "192.0.2.2:4433")
	cfg.TCP.InternodeListen = "127.0.0.1:4434"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted a loopback internode listener with seeds on other hosts")
	}
	if !strings.Contains(err.Error(), "HOARDDB_TCP_INTERNODE_LISTEN") {
		t.Errorf("error %q does not name HOARDDB_TCP_INTERNODE_LISTEN", err)
	}
}

func TestExplicitWildcardListenOverridesTheLoopbackDefault(t *testing.T) {
	t.Setenv("HOARDDB_DATA_DIR", t.TempDir())
	t.Setenv("HOARDDB_TCP_LISTEN", "0.0.0.0:7433")
	t.Setenv("HOARDDB_LISTEN", "0.0.0.0:4433")
	t.Setenv("HOARDDB_METRICS_LISTEN", "0.0.0.0:9090")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TCP.Listen != "0.0.0.0:7433" || cfg.Server.Listen != "0.0.0.0:4433" || cfg.Metrics.Listen != "0.0.0.0:9090" {
		t.Errorf("explicit addresses not honoured: client %q, ring %q, metrics %q", cfg.TCP.Listen, cfg.Server.Listen, cfg.Metrics.Listen)
	}
}

func TestListensOnLoopbackOnly(t *testing.T) {
	tests := map[string]bool{
		"127.0.0.1:7433": true,
		"localhost:7433": true,
		"[::1]:7433":     true,
		"127.0.0.2:7433": true,
		"0.0.0.0:7433":   false,
		":7433":          false,
		"192.0.2.1:7433": false,
		"node1:4433":     false,
		"not an address": false,
	}
	for address, want := range tests {
		if got := ListensOnLoopbackOnly(address); got != want {
			t.Errorf("ListensOnLoopbackOnly(%q) = %v, want %v", address, got, want)
		}
	}
}

// TestListenThatCannotMatchItsSeedEntryIsRefused is BUG-0319. A wildcard
// listen address is advertised as 127.0.0.1, so with seeds on other machines
// the node can only find its own entry by an explicit HOARDDB_NODE_ID listed
// as id@address; otherwise the ring gains this node a second time on
// loopback. A real listen address with a loopback seed on its own port makes
// the same phantom from the other side.
func TestListenThatCannotMatchItsSeedEntryIsRefused(t *testing.T) {
	tests := []struct {
		name      string
		listen    string
		nodeID    string
		seeds     []string
		wantInErr []string
	}{
		{"wildcard with plain remote seeds", "0.0.0.0:4433", "",
			[]string{"192.0.2.1:4433", "192.0.2.2:4433"},
			[]string{"0.0.0.0:4433", "HOARDDB_NODE_ID", "<node-id>@<address>", "HOARDDB_LISTEN"}},
		{"wildcard with a node id the seeds do not list", "0.0.0.0:4433", "n1",
			[]string{"192.0.2.1:4433", "192.0.2.2:4433"},
			[]string{"0.0.0.0:4433", "HOARDDB_NODE_ID", "n1@<address>", "HOARDDB_LISTEN"}},
		{"real address with a loopback self-seed", "192.0.2.1:4433", "",
			[]string{"127.0.0.1:4433", "192.0.2.2:4433"},
			[]string{"127.0.0.1:4433", "192.0.2.1:4433", "HOARDDB_SEED_NODES"}},
		{"real address with a localhost self-seed and no remote seed", "192.0.2.1:4433", "",
			[]string{"localhost:4433", "127.0.0.1:5433"},
			[]string{"localhost:4433", "192.0.2.1:4433", "HOARDDB_SEED_NODES"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := clusterConfig(t, tt.listen, tt.seeds...)
			cfg.Server.NodeID = tt.nodeID
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted listen %s with seeds %v, which puts this node in the ring twice", tt.listen, tt.seeds)
			}
			for _, want := range tt.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not name %q: %v", want, err)
				}
			}
		})
	}
}

// TestWildcardSeedRulesKeepTheAddNodePath is BUG-0319's rework. Only the seed
// lists that give a node a second entry are refused. A wildcard node with an
// explicit id that the seeds do not list yet is the documented add-node first
// start ('join <id>@<address>' follows on a member), and a node with a saved
// member set restarts from that set whatever its seeds say.
func TestWildcardSeedRulesKeepTheAddNodePath(t *testing.T) {
	t.Run("add-node first start: explicit id not listed, id@address seeds", func(t *testing.T) {
		cfg := clusterConfig(t, "0.0.0.0:4433", "n1@192.0.2.1:4433", "n2@192.0.2.2:4433", "n3@192.0.2.3:4433")
		cfg.Server.NodeID = "n4"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate refused the add-node first start: %v", err)
		}
	})
	t.Run("hostname default id with id@address seeds is refused", func(t *testing.T) {
		cfg := clusterConfig(t, "0.0.0.0:4433", "n1@192.0.2.1:4433", "n2@192.0.2.2:4433")
		cfg.Server.NodeID = "host-a"
		cfg.Server.NodeIDDefaulted = true
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate accepted a wildcard node with the hostname default id and seeds on other machines")
		}
		for _, want := range []string{"HOARDDB_NODE_ID is not set", "<node-id>@<address>", "HOARDDB_LISTEN"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error does not name %q: %v", want, err)
			}
		}
	})
	t.Run("restart with a saved member set is left to that set", func(t *testing.T) {
		cfg := clusterConfig(t, "0.0.0.0:4433", "192.0.2.1:4433", "192.0.2.2:4433")
		if err := os.WriteFile(filepath.Join(cfg.Storage.DataDir, "membership.json"), []byte("{}"), 0600); err != nil {
			t.Fatalf("write saved member set: %v", err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate refused a restart that has a saved member set: %v", err)
		}
	})
}
