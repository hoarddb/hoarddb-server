package config

import (
	"fmt"
	"os"
	"strings"
)

// removedClusterKey is a cluster key that no longer exists. It never did anything: the
// cluster runs on its committed values, which start at a compiled-in default and change
// only through the test harness (BUG-0838). Before the first release there is no alias,
// so a start that still sets one is refused instead of ignoring it.
type removedClusterKey struct {
	// key is the path in the configuration file, env its variable.
	key, env string
}

// removedClusterKeys lists the removed keys in the order the refusal reports them.
var removedClusterKeys = []removedClusterKey{
	{"cluster.heartbeat_ms", "HOARDDB_CLUSTER_HEARTBEAT_MS"},
	{"cluster.step_down_ms", "HOARDDB_CLUSTER_STEP_DOWN_MS"},
	{"cluster.failover_ms", "HOARDDB_CLUSTER_FAILOVER_MS"},
	{"cluster.dead_ms", "HOARDDB_CLUSTER_DEAD_MS"},
	{"cluster.probe_interval_ms", "HOARDDB_CLUSTER_PROBE_INTERVAL_MS"},
	{"cluster.probe_timeout_ms", "HOARDDB_CLUSTER_PROBE_TIMEOUT_MS"},
	{"cluster.probe_failures", "HOARDDB_CLUSTER_PROBE_FAILURES"},
	{"cluster.stale_reads", "HOARDDB_CLUSTER_STALE_READS"},
}

// removedClusterKeyPosition is the sentence both refusals share: what is true now.
const removedClusterKeyPosition = "the cluster's lease, failure-detection and stale-read values are fixed in this release " +
	"(the values are on the defaults page) and no setting changes them"

// removedClusterFileKey returns the refusal for a configuration file key in the cluster
// section that was removed, or "" when the key is not one of them.
func removedClusterFileKey(path, line, key string) string {
	for _, removed := range removedClusterKeys {
		if removed.key == "cluster."+key {
			return fmt.Sprintf("Key %q (config file %q, line %s) is not a setting: %s. Remove it from the configuration file, then restart this node.",
				removed.key, path, line, removedClusterKeyPosition)
		}
	}
	return ""
}

// refuseRemovedClusterEnv returns an error naming every removed cluster variable that is
// set to a non-empty value, or nil.
func refuseRemovedClusterEnv() error {
	var problems []string
	for _, removed := range removedClusterKeys {
		if os.Getenv(removed.env) == "" {
			continue
		}
		problems = append(problems, fmt.Sprintf("Key %q is set through %s, but %s. Unset %s, then restart this node.",
			removed.key, removed.env, removedClusterKeyPosition, removed.env))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("environment: %s", strings.Join(problems, "\n"))
}
