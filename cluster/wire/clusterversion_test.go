package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// cvClosedNode is a node directory this build wrote and closed: FORMAT, the incarnation record,
// Badger, the log, the Raft state and the CLUSTER_VERSION marker at this build's level.
func cvClosedNode(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ring")
	if err := u30iOpen(t, dir).Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// cvOpenRefused opens dir with cfg and returns the cluster version refusal it must answer.
func cvOpenRefused(t *testing.T, cfg NodeConfig) *cluster.ClusterVersionError {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, cfg)
	if err == nil {
		_ = node.Close()
		t.Fatalf("OpenNode(%s) opened a directory at a cluster version this build cannot run", cfg.Dir)
	}
	var refusal *cluster.ClusterVersionError
	if !errors.As(err, &refusal) {
		t.Fatalf("OpenNode(%s) = %v, want a *cluster.ClusterVersionError wrapped in it", cfg.Dir, err)
	}
	return refusal
}

// cvWriteSnapshot adds a snapshot at cluster version version to dir's raft snapshot store, newer
// than anything the node wrote, as the library would write it.
func cvWriteSnapshot(t *testing.T, dir string, version uint8) {
	t.Helper()
	raw, err := cluster.EncodeSnapshot(cluster.ClusterState{ClusterVersion: version, Epoch: 1, StateSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	store, err := hraft.NewFileSnapshotStoreWithLogger(filepath.Join(dir, "raft", "snapshots"), 2, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := store.Create(1, 1_000_000, 1_000, hraft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestADirectoryAboveTheBuildsMaxIsRefusedAtOpen (A6): a directory whose CLUSTER_VERSION marker, or
// whose newest raft snapshot, is above this build's Max is refused by OpenNode with the 4.4 text in
// its no-release form, and every file in it is byte-identical afterwards, the incarnation record
// included, because the check runs before nextIncarnation. Against: the check left in OpenRaft's
// first look, where the incarnation file has already been rewritten.
func TestADirectoryAboveTheBuildsMaxIsRefusedAtOpen(t *testing.T) {
	build := cluster.BuildStateRange()
	above := build.Max + 1
	// The start line follows the release table: a row for the level above Max names the release.
	start := fmt.Sprintf("a build whose state_range includes %d (hoarddb-server -v prints it)", above)
	if release := cluster.ReleaseOf(above); release != "" {
		start = release + " or later (hoarddb-server -v prints state_range)"
	}
	want := func(fromMarker string) string {
		return fmt.Sprintf("the cluster version is %d and this build runs versions %d to %d: the cluster was finalized past this build.%s "+
			"Start this node on %s. Keep the data directory. Exit code 70.", above, build.Min, build.Max, fromMarker, start)
	}
	for name, raise := range map[string]func(t *testing.T, dir string){
		"marker": func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, "CLUSTER_VERSION"), fmt.Appendf(nil, "hoarddb cluster version %d\n", above), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"newest snapshot": func(t *testing.T, dir string) { cvWriteSnapshot(t, dir, above) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := cvClosedNode(t)
			raise(t, dir)
			before := treeDigest(t, dir)
			refusal := cvOpenRefused(t, NodeConfig{Dir: dir})
			fromMarker := ""
			if name == "marker" {
				fromMarker = " The version was read from the marker file " + filepath.Join(dir, "CLUSTER_VERSION") +
					"; if this node's raft state is at an older version, the marker is stale: the raft directory was lost or restored from an older backup." +
					" If it is, move " + filepath.Join(dir, "CLUSTER_VERSION") + " aside and start this build again."
			}
			if refusal.Error() != want(fromMarker) {
				t.Errorf("the refusal says\n %q\nwant\n %q", refusal.Error(), want(fromMarker))
			}
			if after := treeDigest(t, dir); !reflect.DeepEqual(before, after) {
				t.Errorf("the refused open changed the directory:\n before %v\n after  %v", before, after)
			}
		})
	}
}

// TestADirectoryBelowTheBuildsMinPassesTheFileCheck (A7, the file half): a directory this build
// wrote, at its level, is not refused by OpenNode's file check for a build whose range starts above
// it (a stub: the branch cannot fire in a build before its second release, whose Min is
// StateLevel). The marker and the newest snapshot are lower bounds of the cluster's committed
// version (owner ruling 2026-11-09), so a value below Min proves nothing; the lower bound is judged
// only by a holder of the committed version (cluster TestADirectoryBelowTheBuildsMinStartsFromTheLog
// and TestALaggingMemberUpgradedAfterAFinalizeCatchesUp). Against: the lower bound judged from the
// files, which refuses a node the cluster's entries would bring up.
func TestADirectoryBelowTheBuildsMinPassesTheFileCheck(t *testing.T) {
	dir := cvClosedNode(t)
	level := cluster.StateLevel
	stub := cluster.StateRange{Min: level + 1, Max: level + 1, Writes: level + 1}
	if err := cluster.CheckDataDirClusterVersion(dir, stub); err != nil {
		t.Fatalf("the file check refused a directory at %d for a build that runs %d to %d: %v", level, stub.Min, stub.Max, err)
	}
}

// TestDescribeNodeCarriesTheClusterVersion (A18): describe_node reports the applied cluster
// version as cluster_version, after refused_peers, and leaves it out at 0; the reply decodes back
// to the same value through the strict decoder ringclient uses. Against: the member absent (a CLI
// of this release would then refuse every later node's reply).
func TestDescribeNodeCarriesTheClusterVersion(t *testing.T) {
	node := &RingAdminNode{NodeID: "node-a", Version: "v", MemberState: "active", Members: 1, Voters: 1, DefaultRF: 1, Serving: ServingOK,
		StateRange: "2-2", RefusedPeers: []string{"node-e (raft channel) speaks 0-0 (writes 0)"}, ClusterVersion: 2, Host: &RingAdminHost{CPUs: 1}}
	raw, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeNode, Node: node})
	if err != nil {
		t.Fatal(err)
	}
	refused, version, host := bytes.Index(raw, []byte("refused_peers\x00")), bytes.Index(raw, []byte("cluster_version\x00")), bytes.Index(raw, []byte("host\x00"))
	if version < 0 || refused >= version || version >= host {
		t.Fatalf("cluster_version is at %d, refused_peers at %d and host at %d: want it between them", version, refused, host)
	}
	got, err := DecodeRingAdminReply(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Node.ClusterVersion != 2 {
		t.Fatalf("decoded cluster_version %d", got.Node.ClusterVersion)
	}
	node.ClusterVersion = 0
	raw, err = EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeNode, Node: node})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("cluster_version")) {
		t.Fatal("a node with no state wrote cluster_version")
	}
	node.ClusterVersion = 256
	if _, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeNode, Node: node}); err == nil || !strings.Contains(err.Error(), "cluster_version 256") {
		t.Fatalf("a cluster_version of 256 gave %v", err)
	}
	state := u83State(cluster.MemberActive, "node-a")
	state.ClusterVersion = 2
	reported := u83Fake(state, u83Liveness{"node-a", true}).describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()})
	if reported.ClusterVersion != 2 {
		t.Fatalf("describeNode reported cluster version %d for a state at 2", reported.ClusterVersion)
	}
}
