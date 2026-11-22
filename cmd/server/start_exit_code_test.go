package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestAStartRefusedForAForeignSnapshotExitsWithTheDivergedStatus (review m1): the refusal of a
// snapshot another build wrote, however many layers wrap it on the way up from OpenRaft, maps to
// exit 70 so a service manager told RestartPreventExitStatus=70 does not loop on it; any other
// start failure stays 1. Break: return 1 for every error.
func TestAStartRefusedForAForeignSnapshotExitsWithTheDivergedStatus(t *testing.T) {
	foreign := &cluster.ForeignSnapshotError{Text: "this node's newest raft snapshot is version 2 so it cannot use it"}
	wrapped := fmt.Errorf("open the entry-ring node in /data/ring: %w", errors.Join(fmt.Errorf("opening the cluster state: %w", foreign), errors.New("close failed")))
	if got := startFailureExitCode(wrapped); got != cluster.DivergedExitCode {
		t.Errorf("a wrapped foreign snapshot refusal exits %d, want %d", got, cluster.DivergedExitCode)
	}
	if got := startFailureExitCode(errors.New("listen tcp: address already in use")); got != 1 {
		t.Errorf("an ordinary start failure exits %d, want 1", got)
	}
}

// TestAStartRefusedForAClusterVersionExitsWithTheDivergedStatus: a data directory at a cluster
// version this build cannot run (cluster-version spec 4.4) is refused at open, and the refusal,
// wrapped on its way up, maps to exit 70 like a foreign snapshot. Break: map only the foreign
// snapshot.
func TestAStartRefusedForAClusterVersionExitsWithTheDivergedStatus(t *testing.T) {
	unrunnable := &cluster.ClusterVersionError{Version: 9, Range: cluster.StateRange{Min: 2, Max: 2, Writes: 2}}
	wrapped := fmt.Errorf("open the entry-ring node in /data/ring: %w", fmt.Errorf("refusing the data directory: %w", unrunnable))
	if got := startFailureExitCode(wrapped); got != cluster.DivergedExitCode {
		t.Errorf("a wrapped cluster version refusal exits %d, want %d", got, cluster.DivergedExitCode)
	}
}

// TestAStartRefusedForAnUnreadableClusterVersionMarkerExitsWithTheDivergedStatus (review finding
// 20): a CLUSTER_VERSION marker this build did not write stops the open, and a restart cannot fix
// it, so the refusal, wrapped on its way up, maps to exit 70. Break: map only the version refusal.
func TestAStartRefusedForAnUnreadableClusterVersionMarkerExitsWithTheDivergedStatus(t *testing.T) {
	unreadable := &cluster.ClusterVersionMarkerError{Path: "/data/ring/CLUSTER_VERSION", Found: "hoarddb cluster version 07"}
	wrapped := fmt.Errorf("open the entry-ring node in /data/ring: %w", fmt.Errorf("refusing the data directory: %w", unreadable))
	if got := startFailureExitCode(wrapped); got != cluster.DivergedExitCode {
		t.Errorf("a wrapped unreadable marker refusal exits %d, want %d", got, cluster.DivergedExitCode)
	}
}
