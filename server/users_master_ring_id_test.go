package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUsersFileRecordsTheRingIDOfTheMaster is BUG-0596: on a cluster whose
// nodes share a configured node id (one host, default ids) the users file
// must still name the master by the ring id the rest of the cluster knows it
// by, on the master and on every follower the push reaches.
func TestUsersFileRecordsTheRingIDOfTheMaster(t *testing.T) {
	const defaultedHostname = "build-host"
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) {
		cfg.Server.NodeID = defaultedHostname
	})
	defer tc.stop()

	masterIndex := tc.masterIndex(t)
	frame := tc.frame(masterIndex, transport.OpUserCreate, clusterCreateRequest(t, "bug0596", "alice"))
	if frame.Opcode == transport.OpError {
		t.Fatalf("create user through the master was refused: %s", frame.Payload)
	}

	wantMaster := tc.addrs[masterIndex]
	deadline := time.Now().Add(15 * time.Second)
	for index, node := range tc.nodes {
		for {
			recorded := node.server.core.auth.SnapshotConfig().Master
			if recorded == wantMaster {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d records master %q, want the ring id %q", index, recorded, wantMaster)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// resetPasswordClusterConfig is a stopped node of a two-node cluster whose
// node id is left to default to the host name, as on a one-host recipe.
func resetPasswordClusterConfig(t *testing.T) (dir string, cfg *config.Config) {
	t.Helper()
	dir, cfg = setupAuthDir(t)
	cfg.Server.NodeID = "build-host"
	cfg.Server.NodeIDDefaulted = true
	cfg.Server.Listen = "127.0.0.1:4001"
	cfg.Server.ReplicationFactor = 2
	cfg.HashRing.SeedNodes = []string{"127.0.0.1:4001", "127.0.0.1:4002"}
	return dir, cfg
}

func recordMasterInUsersFile(t *testing.T, dir, master string) {
	t.Helper()
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	loaded, err := LoadUsersConfig(UsersFilePath(dir), am.keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	loaded.Master = master
	if err := loaded.Save(UsersFilePath(dir), usersSealing{Keyring: am.keyring, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

// The master compares its own ring id, not its configured id, with the file.
func TestResetPasswordAllowsTheMasterNamedByRingID(t *testing.T) {
	dir, cfg := resetPasswordClusterConfig(t)
	recordMasterInUsersFile(t, dir, "127.0.0.1:4001")

	if _, _, err := ResetPassword(cfg, "root", "a-new-longer-password", false, discardLogger()); err != nil {
		t.Fatalf("ResetPassword on the recorded master = %v, want success without --force", err)
	}
}

// A follower sharing the master's configured id is refused, and the refusal
// names ring ids, not the host name.
func TestResetPasswordRefusesAFollowerThatSharesTheConfiguredID(t *testing.T) {
	dir, cfg := resetPasswordClusterConfig(t)
	recordMasterInUsersFile(t, dir, "127.0.0.1:4002")

	_, _, err := ResetPassword(cfg, "root", "a-new-longer-password", false, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword on a follower without --force = nil, want refusal")
	}
	for _, want := range []string{"127.0.0.1:4001", "127.0.0.1:4002"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "build-host") {
		t.Errorf("refusal %q names the configured host name, not a ring id", err)
	}
}

// recordedMasterAfterCreate creates a user through node 0 and returns the
// master its users file would carry (SnapshotConfig is what persistLocked
// writes and the push sends).
func recordedMasterAfterCreate(t *testing.T, tc *testCluster, username string) string {
	t.Helper()
	frame := tc.frame(0, transport.OpUserCreate, clusterCreateRequest(t, "bug0596-"+username, username))
	if frame.Opcode == transport.OpError {
		t.Fatalf("create user %s was refused: %s", username, frame.Payload)
	}
	return tc.nodes[0].server.core.auth.SnapshotConfig().Master
}

// A ring of one member is not an enabled cluster, but its node is still known
// by its ring id (the address, here) and reset-password resolves that id.
func TestUsersFileRecordsTheRingIDOnAOneMemberRing(t *testing.T) {
	tc := newTestClusterWith(t, 1, 2, func(_ int, cfg *config.Config) {
		cfg.Server.NodeID = "build-host"
	})
	defer tc.stop()

	if got := recordedMasterAfterCreate(t, tc, "alice"); got != tc.addrs[0] {
		t.Fatalf("one-member ring records master %q, want the ring id %q", got, tc.addrs[0])
	}
	cfg := *tc.nodes[0].cfg
	selfID, err := ringSelfID(&cfg)
	if err != nil {
		t.Fatalf("ringSelfID: %v", err)
	}
	if selfID != tc.addrs[0] {
		t.Fatalf("ringSelfID = %q, want %q", selfID, tc.addrs[0])
	}
}

// The recorded id must not depend on how many members the ring has when the
// node started: a ring that grows past one member keeps the same id.
func TestUsersFileKeepsTheRingIDWhenTheRingGrowsWhileRunning(t *testing.T) {
	switchJoinFreezeOff(t)
	tc := newTestClusterWith(t, 1, 2, func(_ int, cfg *config.Config) {
		cfg.Server.NodeID = "build-host"
	})
	defer tc.stop()

	if got := recordedMasterAfterCreate(t, tc, "alice"); got != tc.addrs[0] {
		t.Fatalf("before the ring grows, master %q, want %q", got, tc.addrs[0])
	}
	cluster := tc.nodes[0].server.core.cluster
	newcomerAddr := fabricatedMemberAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cluster.proposeJoin(ctx, ring.Node{ID: newcomerAddr, Addr: newcomerAddr}); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	if !cluster.Enabled() {
		t.Fatal("the ring did not grow past one member")
	}
	// The newcomer may now own the auth key, so the handler would refuse here;
	// the manager is called directly because the test is about what it records.
	credential, err := NewCredential("quorum-password-123", DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	auth := tc.nodes[0].server.core.auth
	if err := auth.CreateUser("bob", credential, nil, true); err != nil {
		t.Fatalf("CreateUser after the ring grew: %v", err)
	}
	if got := auth.SnapshotConfig().Master; got != tc.addrs[0] {
		t.Fatalf("after the ring grows, master %q, want %q", got, tc.addrs[0])
	}
}
