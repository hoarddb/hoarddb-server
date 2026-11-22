package wire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestFormat8DirectoryIsRefusedAndAFreshOneIsFormat9 pins the first release's ring format by its
// number (owner ruling 2026-11-21): the release writes format 9, so a ring directory of any
// development build, all of which stopped at format 8 or below, is refused at open by the marker,
// before the store or the raft state is read, in the text that names the directory, what wrote it
// and what to do. The numbers are literals on purpose: the other format tests follow ringFormat
// and would pass with the constant left at 8.
// Against: cutting the first release at the development format, so a development directory opens.
func TestFormat8DirectoryIsRefusedAndAFreshOneIsFormat9(t *testing.T) {
	development := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(development, "badger"))
	mustMkdir(t, filepath.Join(development, "wal"))
	mustWrite(t, filepath.Join(development, "badger", "000001.sst"), "a table of a development build")
	mustWrite(t, filepath.Join(development, "wal", "000001.seg"), "a log of a development build")
	mustWrite(t, filepath.Join(development, ringFormatFile), "hoarddb ring format 8\n")

	before := treeDigest(t, development)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: development})
	if err == nil {
		if closeErr := node.Close(); closeErr != nil {
			t.Errorf("closing the node that should not have opened: %v", closeErr)
		}
		t.Fatalf("OpenNode(%s) opened a format 8 directory", development)
	}
	want := "refusing the ring directory: the ring data directory " + development +
		" is not in the format this release reads, which is format 9 (FORMAT says: hoarddb ring format 8). " +
		"Nothing in the directory was read, changed or deleted. " +
		"This node is not a member of a cluster, so it can start with an empty ring directory: move " + development +
		" aside (keep it if you want it) and start the node again; it starts with no data"
	if err.Error() != want {
		t.Errorf("the refusal is\n  %q\nwant\n  %q", err.Error(), want)
	}
	// No other build was released, so the text names none and no format but the one it reads.
	for _, removed := range []string{"written in format", "build that wrote", "earlier build", "development build", "migrate", "(format 8)"} {
		if strings.Contains(err.Error(), removed) {
			t.Errorf("the refusal still says %q: %s", removed, err.Error())
		}
	}
	after := treeDigest(t, development)
	if len(after) != len(before) {
		t.Fatalf("the refused directory holds %d entries, had %d: the refusal changed it", len(after), len(before))
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Errorf("%s changed under the refusal", path)
		}
	}

	// The fix the text gives a single node can be followed: with the directory moved aside, the
	// node opens on the same path and marks it format 9.
	if err := os.Rename(development, development+".aside"); err != nil {
		t.Fatal(err)
	}
	fresh := development
	opened := u30iOpen(t, fresh)
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(fresh, ringFormatFile))
	if err != nil {
		t.Fatal(err)
	}
	if string(marker) != "hoarddb ring format 9\n" {
		t.Fatalf("a fresh ring directory is marked %q, want %q", marker, "hoarddb ring format 9\n")
	}
}

// TestAFormat8DirectoryIsRefusedForItsFormatWhateverClusterVersionItHolds pins the order of the two
// read-only checks at open: the ring format marker is judged before the cluster version. A
// development directory can also carry a CLUSTER_VERSION above this build's range, and its
// operator must be told the fact that decides everything else, that this build does not read the
// directory at all; the cluster-version refusal would send them to a later build that refuses it
// for its format again. The fixture is refused by either check alone, so only the order decides
// which text comes back.
// Against: OpenNode checking the cluster version before the ring format.
func TestAFormat8DirectoryIsRefusedForItsFormatWhateverClusterVersionItHolds(t *testing.T) {
	development := filepath.Join(t.TempDir(), "ring")
	mustMkdir(t, filepath.Join(development, "badger"))
	mustMkdir(t, filepath.Join(development, "wal"))
	mustWrite(t, filepath.Join(development, "badger", "000001.sst"), "a table of a development build")
	mustWrite(t, filepath.Join(development, ringFormatFile), "hoarddb ring format 8\n")
	mustWrite(t, filepath.Join(development, "CLUSTER_VERSION"), "hoarddb cluster version 255\n")

	// The cluster version alone refuses this directory: without that the test would pass in
	// either order and pin nothing.
	var versionRefusal *cluster.ClusterVersionError
	if err := cluster.CheckDataDirClusterVersion(development, cluster.BuildStateRange()); !errors.As(err, &versionRefusal) {
		t.Fatalf("the fixture's cluster version is not refused on its own (%v), so it cannot tell the two orders apart", err)
	}

	before := treeDigest(t, development)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: development})
	if err == nil {
		if closeErr := node.Close(); closeErr != nil {
			t.Errorf("closing the node that should not have opened: %v", closeErr)
		}
		t.Fatalf("OpenNode(%s) opened a format 8 directory at cluster version 255", development)
	}
	var formatRefusal *RingFormatRefusal
	if !errors.As(err, &formatRefusal) || formatRefusal.Format != "8" {
		t.Errorf("the refusal is not the ring format's: %v", err)
	}
	if errors.As(err, &versionRefusal) {
		t.Errorf("the cluster version was judged before the ring format: %v", err)
	}
	after := treeDigest(t, development)
	if len(after) != len(before) {
		t.Fatalf("the refused directory holds %d entries, had %d: the refusal changed it", len(after), len(before))
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Errorf("%s changed under the refusal", path)
		}
	}
}
