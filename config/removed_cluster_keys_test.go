package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// removedClusterKeyCases are the cluster keys BUG-0838 removed: the cluster runs on its committed
// values, and nothing in a file or the environment ever reached them.
var removedClusterKeyCases = []struct{ key, env, value string }{
	{"cluster.heartbeat_ms", "HOARDDB_CLUSTER_HEARTBEAT_MS", "100"},
	{"cluster.step_down_ms", "HOARDDB_CLUSTER_STEP_DOWN_MS", "2000"},
	{"cluster.failover_ms", "HOARDDB_CLUSTER_FAILOVER_MS", "5000"},
	{"cluster.dead_ms", "HOARDDB_CLUSTER_DEAD_MS", "30000"},
	{"cluster.probe_interval_ms", "HOARDDB_CLUSTER_PROBE_INTERVAL_MS", "1000"},
	{"cluster.probe_timeout_ms", "HOARDDB_CLUSTER_PROBE_TIMEOUT_MS", "500"},
	{"cluster.probe_failures", "HOARDDB_CLUSTER_PROBE_FAILURES", "3"},
	{"cluster.stale_reads", "HOARDDB_CLUSTER_STALE_READS", "true"},
}

// TestRemovedClusterKeyInTheFileIsRefused: a file that still sets one of the removed keys is
// refused at start, naming the key, the real position and the fix (BUG-0838).
func TestRemovedClusterKeyInTheFileIsRefused(t *testing.T) {
	for _, removed := range removedClusterKeyCases {
		t.Run(removed.key, func(t *testing.T) {
			name := strings.TrimPrefix(removed.key, "cluster.")
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("cluster:\n  "+name+": "+removed.value+"\n"), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := Load(path)
			assertRemovedKeyRefusal(t, err, removed.key, "Remove it from the configuration file")
		})
	}
}

// TestRemovedClusterKeyInTheEnvironmentIsRefused is the same refusal for the variable form.
func TestRemovedClusterKeyInTheEnvironmentIsRefused(t *testing.T) {
	for _, removed := range removedClusterKeyCases {
		t.Run(removed.env, func(t *testing.T) {
			t.Setenv(removed.env, removed.value)
			_, err := Load("")
			assertRemovedKeyRefusal(t, err, removed.key, "Unset "+removed.env)
			if err != nil && !strings.Contains(err.Error(), removed.env) {
				t.Errorf("refusal does not name the variable %s: %v", removed.env, err)
			}
		})
	}
}

func assertRemovedKeyRefusal(t *testing.T, err error, key, fix string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Load accepted %s; it must be refused", key)
	}
	for _, want := range []string{`Key "` + key + `"`, "fixed in this release", fix} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal for %s lacks %q:\n%v", key, want, err)
		}
	}
}

// TestRemovedClusterKeyTableNamesTheDerivedVariables keeps the production table in step with
// the naming scheme and with the cases above.
func TestRemovedClusterKeyTableNamesTheDerivedVariables(t *testing.T) {
	if len(removedClusterKeys) != len(removedClusterKeyCases) {
		t.Fatalf("the table has %d keys, the tests cover %d", len(removedClusterKeys), len(removedClusterKeyCases))
	}
	for index, removed := range removedClusterKeys {
		if removed.env != EnvName(removed.key) {
			t.Errorf("%s: variable %q, the derived name is %q", removed.key, removed.env, EnvName(removed.key))
		}
		if removed.key != removedClusterKeyCases[index].key {
			t.Errorf("table entry %d is %s, the test case is %s", index, removed.key, removedClusterKeyCases[index].key)
		}
	}
}
