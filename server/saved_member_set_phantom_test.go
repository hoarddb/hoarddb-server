package server

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
)

// startFromSavedMemberSet runs what a restart runs: config validation (as
// config.Load does, before the saved set is read), then NewCluster on a data
// directory holding a saved member set.
func startFromSavedMemberSet(t *testing.T, cfg *config.Config, saved []ring.Node) (*Cluster, error) {
	t.Helper()
	dir := t.TempDir()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	if err := saveMembershipRecord(membershipPathFor(dir), membershipFile{Epoch: 2, Members: saved}); err != nil {
		t.Fatalf("save member set: %v", err)
	}
	cfg.Storage.DataDir = dir
	cfg.HashRing.SeedFingerprints = []string{"SHA256:" + strings.Repeat("ab", 32)}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &Server{config: cfg, kv: openPositionTestStore(t, t.TempDir())}
	return NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// TestAJoinedWildcardNodeRestartsFromItsSavedMemberSet is BUG-0319's rework: a
// node added with 'join n4@<address>' on a wildcard listener restarts from its
// saved set, which lists it, whatever its seeds say.
func TestAJoinedWildcardNodeRestartsFromItsSavedMemberSet(t *testing.T) {
	saved := []ring.Node{
		{ID: "n1", Addr: "192.0.2.1:4433"}, {ID: "n2", Addr: "192.0.2.2:4433"},
		{ID: "n3", Addr: "192.0.2.3:4433"}, {ID: "n4", Addr: "192.0.2.9:4433"},
	}
	for _, seeds := range [][]string{
		{"n1@192.0.2.1:4433", "n2@192.0.2.2:4433", "n3@192.0.2.3:4433"},
		{"192.0.2.1:4433", "192.0.2.2:4433", "192.0.2.3:4433"},
	} {
		t.Run(strings.Join(seeds, ","), func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Server.Listen = "0.0.0.0:4433"
			cfg.Server.NodeID = "n4"
			cfg.HashRing.SeedNodes = seeds
			cluster, err := startFromSavedMemberSet(t, cfg, saved)
			if err != nil {
				t.Fatalf("a joined node's restart was refused: %v", err)
			}
			if cluster.LocalID() != "n4" || cluster.LocalAddr() != "192.0.2.9:4433" {
				t.Errorf("restarted as %s at %s, want n4 at 192.0.2.9:4433", cluster.LocalID(), cluster.LocalAddr())
			}
		})
	}
}

// TestASavedMemberSetHoldingThisNodeTwiceIsRefused is BUG-0319's rework: a
// node bootstrapped before the seed refusal saved the extra loopback entry,
// and a saved set outranks the seeds, so correcting the seeds alone left it
// in place (with HOARDDB_NODE_ID set, the node even ran as the extra entry).
func TestASavedMemberSetHoldingThisNodeTwiceIsRefused(t *testing.T) {
	tests := []struct {
		name      string
		listen    string
		nodeID    string
		defaulted bool
		seeds     []string
		saved     []ring.Node
	}{
		{
			name: "wildcard, saved from plain seeds, unchanged config", listen: "0.0.0.0:4433",
			nodeID: "host-a", defaulted: true, seeds: []string{"192.0.2.5:4433", "192.0.2.6:4433"},
			saved: []ring.Node{{ID: "192.0.2.5:4433", Addr: "192.0.2.5:4433"}, {ID: "192.0.2.6:4433", Addr: "192.0.2.6:4433"},
				{ID: "host-a", Addr: "127.0.0.1:4433"}},
		},
		{
			name: "wildcard, seeds corrected to id@address", listen: "0.0.0.0:4433",
			nodeID: "a", seeds: []string{"a@192.0.2.5:4433", "b@192.0.2.6:4433"},
			saved: []ring.Node{{ID: "192.0.2.5:4433", Addr: "192.0.2.5:4433"}, {ID: "192.0.2.6:4433", Addr: "192.0.2.6:4433"},
				{ID: "host-a", Addr: "127.0.0.1:4433"}},
		},
		{
			name: "real listen, saved with a loopback self-seed", listen: "192.0.2.5:4433",
			nodeID: "host-a", defaulted: true, seeds: []string{"127.0.0.1:4433", "192.0.2.6:4433"},
			saved: []ring.Node{{ID: "127.0.0.1:4433", Addr: "127.0.0.1:4433"}, {ID: "192.0.2.6:4433", Addr: "192.0.2.6:4433"},
				{ID: "host-a", Addr: "192.0.2.5:4433"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Server.Listen = tt.listen
			cfg.Server.NodeID = tt.nodeID
			cfg.Server.NodeIDDefaulted = tt.defaulted
			cfg.HashRing.SeedNodes = tt.seeds
			cluster, err := startFromSavedMemberSet(t, cfg, tt.saved)
			if err == nil {
				t.Fatalf("started as %s at %s on a saved set holding it twice: %v",
					cluster.LocalID(), cluster.LocalAddr(), memberIDsOf(cluster.Members()))
			}
			for _, want := range []string{"membership.json", "remove membership.json", "HOARDDB_NODE_ID", "<node-id>@<address>"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not name %q: %v", want, err)
				}
			}
		})
	}
}
