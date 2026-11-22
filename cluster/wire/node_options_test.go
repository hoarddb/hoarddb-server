package wire

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestANetworkedNodeWithoutItsListenerIsRefusedNotPanicked: a missing internode listener
// is an error that names the fix, from OpenNode and from the Close it runs on the way out.
func TestANetworkedNodeWithoutItsListenerIsRefusedNotPanicked(t *testing.T) {
	_, err := OpenNode(context.Background(), NodeConfig{Dir: t.TempDir(), Cluster: &ClusterOptions{}})
	if err == nil || !strings.Contains(err.Error(), "Listener") {
		t.Fatalf("OpenNode with no Listener: %v, want an error naming ClusterOptions.Listener", err)
	}
}

// TestANetworkedNodeWithoutPinnedTLSIsRefused: without TLS and the id=fingerprint map the
// port would trust any caller, so the node refuses to open unless a test says so.
func TestANetworkedNodeWithoutPinnedTLSIsRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, err = OpenNode(context.Background(), NodeConfig{Dir: t.TempDir(), Cluster: &ClusterOptions{Listener: ln}})
	if err == nil || !strings.Contains(err.Error(), "Fingerprints") {
		t.Fatalf("OpenNode with no TLS and no fingerprints: %v, want a refusal naming them", err)
	}
}

// TestTheIncarnationNeverGoesBackwards: a restart after the wall clock stepped back still
// gets a higher incarnation than the last run, or every peer would refuse the node as a
// stopped process speaking for a replaced identity.
func TestTheIncarnationNeverGoesBackwards(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_800_000_000, 0)
	first := func(at time.Time) int64 {
		node, err := OpenNode(context.Background(), NodeConfig{Dir: dir, Now: func() time.Time { return at }})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = node.Close() }()
		return node.Identity().Incarnation
	}
	one := first(now)
	two := first(now.Add(-time.Hour))
	if two <= one {
		t.Fatalf("incarnation %d after a restart with the clock an hour back, %d before: it must only grow", two, one)
	}
	_ = cluster.Member{}
}

// TestACorruptIncarnationRecordRefusesToStartNamingTheFile: guessing a value could undo the
// monotonic guarantee, so an unreadable record stops the node and says how to proceed.
func TestACorruptIncarnationRecordRefusesToStartNamingTheFile(t *testing.T) {
	dir := t.TempDir()
	for _, content := range []string{"", "not a number\n", "-5\n"} {
		if err := os.WriteFile(filepath.Join(dir, "incarnation"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
		if err == nil || !strings.Contains(err.Error(), filepath.Join(dir, "incarnation")) || !strings.Contains(err.Error(), "delete") {
			t.Fatalf("record %q: %v, want a refusal that names the file and says to delete it once the clock is right", content, err)
		}
	}
}
