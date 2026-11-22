package config

import (
	"strings"
	"testing"
)

// TestUnknownConfiguredWriteConcernFailsLoad guards the node
// default: HOARDDB_WRITE_CONCERN (or write_concern in the YAML) with a typo was
// read as `one`, silently overriding the majority default of a replicated node.
func TestUnknownConfiguredWriteConcernFailsLoad(t *testing.T) {
	cfg := clusterConfig(t, "127.0.0.1:4433", "127.0.0.1:4434")
	cfg.Server.WriteConcern = "majorty"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted write concern \"majorty\"")
	}
	for _, want := range []string{"HOARDDB_WRITE_CONCERN", "majorty", "majority", "all", "one"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestKnownConfiguredWriteConcernsLoad keeps the accepted spellings accepted.
func TestKnownConfiguredWriteConcernsLoad(t *testing.T) {
	for _, accepted := range []string{"", "1", "one", "majority", "ALL", "all_nodes"} {
		cfg := clusterConfig(t, "127.0.0.1:4433", "127.0.0.1:4434")
		cfg.Server.WriteConcern = accepted
		if err := cfg.Validate(); err != nil {
			t.Errorf("write concern %q refused: %v", accepted, err)
		}
	}
}

// TestLoadRefusesTheOldReplicatedModeWithTheRingFix: a seed list without cluster mode is refused
// by Load with the ring fix, not with the missing-fingerprint text that hides it. Break: call the
// refusal after Validate in Load.
func TestLoadRefusesTheOldReplicatedModeWithTheRingFix(t *testing.T) {
	t.Setenv("HOARDDB_SEED_NODES", "node1@127.0.0.1:4433")
	t.Setenv("HOARDDB_SEED_FINGERPRINTS", "")
	_, err := Load("")
	if err == nil {
		t.Fatal("Load accepted a seed list without cluster mode")
	}
	for _, want := range []string{"HOARDDB_SEED_NODES", "old replicated mode", "HOARDDB_CLUSTER_ENABLED=true"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestRingModeAndOneNodeAreNotRefusedAsTheOldMode: cluster.enabled with a factor and seeds is the
// ring, and a plain node is one node; neither is the old mode. Break: refuse on the factor alone.
func TestRingModeAndOneNodeAreNotRefusedAsTheOldMode(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.RefuseOldReplicatedMode(); err != nil {
		t.Errorf("a default config was refused: %v", err)
	}
	cfg.Cluster.Enabled = true
	cfg.HashRing.SeedNodes = []string{"node1@127.0.0.1:4433"}
	if err := cfg.RefuseOldReplicatedMode(); err != nil {
		t.Errorf("a ring config was refused: %v", err)
	}
}

// TestTheRefusalSaysHowToStartTheRingAndWhatBecomesOfTheOldData: the text names who lists whom
// in the seed list (the first node lists nobody, later nodes list the existing ones), the
// per-node fingerprint form, and that the ring does not read what the old mode wrote and that
// no command of this build moves it. It must not offer a step that needs the old engine. Break: drop any one sentence from RefuseOldReplicatedMode.
func TestTheRefusalSaysHowToStartTheRingAndWhatBecomesOfTheOldData(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.ReplicationFactor = 3
	err := cfg.RefuseOldReplicatedMode()
	if err == nil {
		t.Fatal("a replication factor of 3 without cluster mode was not refused")
	}
	for _, want := range []string{
		"first node lists nobody",
		"<node-id>@<host:port>",
		"<node-id>=SHA256:<64 hex>",
		"does not read the data the old mode wrote",
		"no command of this build moves that data into the ring",
		"keep the old data directory untouched and start the ring in a fresh one",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	// A plain hoarddb session is the ring session, so no step of the old text works any more.
	for _, gone := range []string{"plain 'hoarddb' session", "dump --out", "hoarddb ingest", "restore"} {
		if strings.Contains(err.Error(), gone) {
			t.Errorf("refusal %q still offers %q, which cannot reach the old engine", err, gone)
		}
	}
}

// TestARingNodeWithAReplicationFactorAboveOneRefusesAndGivesThePerDatabaseForm: in ring mode the
// factor is a database option, so HOARDDB_REPLICATION_FACTOR above 1 used to be ignored and the
// operator got one copy. It is refused, naming the variable and value and the statement; unset
// or 1 starts. Break: drop the cluster.enabled case from RefuseOldReplicatedMode.
func TestARingNodeWithAReplicationFactorAboveOneRefusesAndGivesThePerDatabaseForm(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Server.ReplicationFactor = 3
	err := cfg.RefuseOldReplicatedMode()
	if err == nil {
		t.Fatal("a ring node with a replication factor of 3 was not refused")
	}
	for _, want := range []string{"HOARDDB_REPLICATION_FACTOR=3", "create database <name> { replication_factor: 3 }", "unset HOARDDB_REPLICATION_FACTOR"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	for _, ok := range []int{0, 1} {
		cfg.Server.ReplicationFactor = ok
		if err := cfg.RefuseOldReplicatedMode(); err != nil {
			t.Errorf("a ring node with factor %d was refused: %v", ok, err)
		}
	}
}

// TestLoadRefusesARingNodeFactorButTheClientLoaderDoesNot: the refusal fires in Load, and the
// loader the CLI uses skips it.
func TestLoadRefusesARingNodeFactorButTheClientLoaderDoesNot(t *testing.T) {
	t.Setenv("HOARDDB_CLUSTER_ENABLED", "true")
	t.Setenv("HOARDDB_NODE_ID", "node-a")
	t.Setenv("HOARDDB_REPLICATION_FACTOR", "3")
	if _, err := Load(""); err == nil {
		t.Error("Load accepted a ring node with a replication factor of 3")
	}
	if _, err := LoadAllowingOldReplicatedMode(""); err != nil {
		t.Errorf("the client loader refused: %v", err)
	}
}
