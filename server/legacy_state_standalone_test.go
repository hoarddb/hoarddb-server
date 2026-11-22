package server

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
)

// legacyStateFileName is the file the previous build kept a node's positions
// in. Its fate is a property of the data directory, not of the cluster
// (BUG-0216), so a standalone start pays it attention too.
const legacyStateFileName = "replication_state.bson"

// standaloneServiceConfig is the config this bug is about: no replication
// factor above 1 and no seed nodes, so NewService builds no Cluster.
func standaloneServiceConfig(dataDir string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	return cfg
}

// requireLegacyFixturePositionsStored reads the three positions the legacy
// fixture holds straight out of the node store. A standalone node loads none
// of them into memory, so the store records are the only evidence that the
// start imported them.
func requireLegacyFixturePositionsStored(t *testing.T, kv storage.KV, when string) {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	stored, err := snapshot.Get(positionKey(legacyFixtureApplied))
	if err != nil {
		t.Fatalf("%s: the store holds no position for %s: %v", when, legacyFixtureApplied, err)
	}
	var position positionRecord
	if err := bson.Unmarshal(stored, &position); err != nil {
		t.Fatalf("%s: decoding the stored position of %s: %v", when, legacyFixtureApplied, err)
	}
	if position.AppliedThrough != 41 {
		t.Errorf("%s: the store's applied position of %s is %d, want the file's 41", when, legacyFixtureApplied, position.AppliedThrough)
	}
	for _, floor := range []struct {
		when  string
		key   []byte
		value uint64
	}{
		{when: "db1/users", key: seededKey(legacyFixtureSeeded, "db1", "users"), value: 30},
		{when: "logs by bare name", key: legacySeededKey(legacyFixtureBare, "logs"), value: 12},
	} {
		stored, err := snapshot.Get(floor.key)
		if err != nil {
			t.Fatalf("%s: the store holds no snapshot position for %s: %v", when, floor.when, err)
		}
		got, err := decodeFloor(stored)
		if err != nil {
			t.Fatalf("%s: decoding the stored snapshot position of %s: %v", when, floor.when, err)
		}
		if got != floor.value {
			t.Errorf("%s: the store's snapshot position of %s is %d, want the file's %d", when, floor.when, got, floor.value)
		}
	}
}

// TestAStandaloneStartImportsAndDeletesTheLegacyStateFile is BUG-0216: a node
// configured standalone (replication_factor 1, no seed nodes) over a
// replication_state.bson left by the previous build imports its positions into
// the node store and deletes the file, exactly as a clustered start does, and
// the positions are still there for a restart.
func TestAStandaloneStartImportsAndDeletesTheLegacyStateFile(t *testing.T) {
	dataDir := t.TempDir()
	statePath := filepath.Join(dataDir, legacyStateFileName)
	fixture, err := os.ReadFile(filepath.Join("testdata", "replication_state_before_positions.bson"))
	if err != nil {
		t.Fatalf("reading the legacy state fixture: %v", err)
	}
	if err := os.WriteFile(statePath, fixture, 0o644); err != nil {
		t.Fatalf("installing the legacy state fixture: %v", err)
	}

	first, err := NewService(standaloneServiceConfig(dataDir), quietLogger())
	if err != nil {
		t.Fatalf("a standalone start over a readable legacy state file: %v", err)
	}
	if first.core.cluster != nil {
		t.Fatalf("the config is not standalone: the start built a cluster")
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the standalone start left the legacy state file in place: %v", err)
	}
	if leftovers, err := filepath.Glob(filepath.Join(dataDir, legacyStateFileName+"*")); err != nil || len(leftovers) != 0 {
		t.Errorf("the data directory keeps a copy of the legacy state after a standalone start: %v (%v)", leftovers, err)
	}
	requireLegacyFixturePositionsStored(t, first.core.kv, "first start")
	if err := first.Stop(); err != nil {
		t.Fatalf("stopping the first standalone node: %v", err)
	}

	second, err := NewService(standaloneServiceConfig(dataDir), quietLogger())
	if err != nil {
		t.Fatalf("the second standalone start: %v", err)
	}
	stopService(t, second)
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the second standalone start found a legacy state file: %v", err)
	}
	requireLegacyFixturePositionsStored(t, second.core.kv, "second start")
}

// TestAStandaloneStartStopsOverAnUnreadableLegacyStateFile is BUG-0216's second
// claim: an unreadable file stops a standalone start with the same refusal a
// clustered start gives — the path, what the positions are for, and the
// mv remedy — and the server leaves the file where it is.
func TestAStandaloneStartStopsOverAnUnreadableLegacyStateFile(t *testing.T) {
	dataDir := t.TempDir()
	statePath := filepath.Join(dataDir, legacyStateFileName)
	unreadable := []byte("this is not a BSON document")
	if err := os.WriteFile(statePath, unreadable, 0o644); err != nil {
		t.Fatalf("writing the unreadable state file: %v", err)
	}

	service, err := NewService(standaloneServiceConfig(dataDir), quietLogger())
	if err == nil {
		if stopErr := service.Stop(); stopErr != nil {
			t.Logf("stopping the standalone node that should not have started: %v", stopErr)
		}
		t.Fatal("a standalone start over an unreadable legacy state file went ahead")
	}
	for _, want := range []string{
		statePath,
		"snapshot positions",
		"applied twice",
		"mv " + statePath + " " + statePath + ".unreadable",
		"encrypted",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	if kept, readErr := os.ReadFile(statePath); readErr != nil || string(kept) != string(unreadable) {
		t.Errorf("the refused start did not leave the file as it was: %q, %v", kept, readErr)
	}
	if _, statErr := os.Stat(statePath + ".unreadable"); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("the server moved the file aside itself; that is the operator's decision: %v", statErr)
	}
}
