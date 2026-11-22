package main

import (
	"net"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// The root password of a cluster member (ring user store spec 6.5, step M2b.1): only a data node
// that starts a new cluster and has no Raft state may ask for one on a terminal; a joiner, a
// witness and a restarted member never prompt and never refuse for want of one.

// clusterStartConfig is a cluster member's configuration in a fresh working directory, its peer
// listener on a free loopback port.
func clusterStartConfig(t *testing.T, id string, seeds ...string) *config.Config {
	t.Helper()
	cfg := loadStartConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Server.Listen = listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.TCP.Listen = "127.0.0.1:0"
	// Left empty, the internode listener binds the ring port plus one, which nothing reserved.
	cfg.TCP.InternodeListen = "127.0.0.1:0"
	cfg.Metrics.Enabled = false
	cfg.Server.NodeID, cfg.Server.NodeIDDefaulted = id, false
	cfg.Cluster.Enabled = true
	cfg.HashRing.SeedNodes = seeds
	return cfg
}

// TestAClusterJoinerOrWitnessNeverPromptsForARootPassword: a member whose seed list names another
// node joins an existing cluster, whose first administrator exists or comes from another node, so
// it neither prompts nor refuses to start without a terminal. Break: let a cluster member prompt
// like a single node; red (the start refuses: no terminal).
func TestAClusterJoinerOrWitnessNeverPromptsForARootPassword(t *testing.T) {
	for _, witness := range []bool{false, true} {
		cfg := clusterStartConfig(t, "node-b", "node-a@127.0.0.1:4400")
		cfg.Cluster.Witness = witness
		for _, terminal := range []*scriptedTerminal{{}, {interactive: true, answers: []string{"typed-root-password", "typed-root-password"}}} {
			choice, err := chooseRootPassword(cfg, "admin", terminal)
			if err != nil {
				t.Fatalf("witness %v, interactive %v: a joiner refused to start without a root password: %v", witness, terminal.interactive, err)
			}
			if choice.source != rootUnchosen || len(terminal.prompts) != 0 {
				t.Fatalf("witness %v, interactive %v: a joiner chose %v after prompts %q, want no prompt", witness, terminal.interactive, choice.source, terminal.prompts)
			}
		}
	}
}

// TestTheFirstNodeOfANewClusterPromptsOnlyBeforeItHasState: the node that starts a new cluster
// asks for the root password on its first start, as a single node does, and never once its Raft
// state exists: its cluster's administrator is in that state. Break: decide by the seed list alone;
// red (the restart prompts).
func TestTheFirstNodeOfANewClusterPromptsOnlyBeforeItHasState(t *testing.T) {
	cfg := clusterStartConfig(t, "node-a")
	terminal := &scriptedTerminal{interactive: true, answers: []string{"typed-root-password", "typed-root-password"}}
	choice, err := chooseRootPassword(cfg, "admin", terminal)
	if err != nil || choice.source != rootPrompted || choice.password != "typed-root-password" {
		t.Fatalf("the first start of a new cluster chose %+v, %v; want the typed password", choice.source, err)
	}

	srv, err := prepare(cfg, discardSlog(), &strings.Builder{}, terminal2(choice.password))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatal(err)
	}
	again := &scriptedTerminal{interactive: true, answers: []string{"other-password", "other-password"}}
	choice, err = chooseRootPassword(cfg, "admin", again)
	if err != nil || choice.source != rootUnchosen || len(again.prompts) != 0 {
		t.Fatalf("a restart of the first node chose %v, %v after prompts %q; want no prompt", choice.source, err, again.prompts)
	}
}

// terminal2 answers the prompt with password twice.
func terminal2(password string) firstStartTerminal {
	return &scriptedTerminal{interactive: true, answers: []string{password, password}}
}
