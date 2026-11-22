package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
)

// The offline password reset (ring user store spec 6.7, step M2b.3): a cluster member's users
// live in the cluster's replicated state, which a stopped node cannot change, so --reset-password
// refuses on a node whose configuration has cluster.enabled, before it prompts and before it writes
// anything. A node without a cluster keeps the reset as it is: it has a ring/raft directory too, so
// the refusal is decided by the configuration, never by that directory (F8).

const resetRefusal = "--reset-password does not work on a cluster node: users live in the cluster's replicated state, which a stopped node cannot change. " +
	"Start the cluster, log in on any node as another administrator (an account with user-manager or root), and run 'alter user <name> password'. " +
	"If no other administrator can log in, the cluster's users cannot be recovered; see \"Keep two administrators\" in the user-management page"

// dirListing is every file under dir with its size and modification time.
func dirListing(t *testing.T, dir string) map[string]string {
	t.Helper()
	listing := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		listing[path] = fmt.Sprintf("%s %s %d", info.ModTime().Format(time.RFC3339Nano), info.Mode(), info.Size())
		return nil
	})
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	return listing
}

// startedOnce starts srv and stops it, so its data directory is a real node's.
func startedOnce(t *testing.T, srv *server.Service) {
	t.Helper()
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatal(err)
	}
}

// TestResetPasswordOnAClusterMemberRefusesBeforePromptingAndWritesNothing: the refusal is the
// spec's text, it comes before the prompt (a test has no terminal, so a prompt would fail with
// another error), and the member's data directory is exactly as it was. Break: let the reset run on
// a cluster member; red.
func TestResetPasswordOnAClusterMemberRefusesBeforePromptingAndWritesNothing(t *testing.T) {
	cfg := clusterStartConfig(t, "node-a")
	cfg.Server.RootPassword = startRootPassword
	srv, err := prepare(cfg, discardSlog(), io.Discard, noTerminal())
	if err != nil {
		t.Fatal(err)
	}
	startedOnce(t, srv)
	before := dirListing(t, cfg.Storage.DataDir)

	err = runResetPassword(cfg, "admin", false, discardSlog())
	if err == nil || err.Error() != resetRefusal {
		t.Fatalf("--reset-password on a cluster member answered %v, want the refusal:\n%s", err, resetRefusal)
	}
	if _, _, err := server.ResetPassword(cfg, "admin", "a-new-root-password", true, discardSlog()); err == nil || err.Error() != resetRefusal {
		t.Fatalf("server.ResetPassword on a cluster member answered %v, want the refusal", err)
	}
	after := dirListing(t, cfg.Storage.DataDir)
	for path, was := range before {
		if after[path] != was {
			t.Errorf("%s changed: %q, was %q", path, after[path], was)
		}
	}
	for path := range after {
		if _, ok := before[path]; !ok {
			t.Errorf("%s was created", path)
		}
	}
}

// TestResetPasswordOnAZeroConfigNodeStillWorks (F8): a node without a cluster, started for real
// first so it has its own ring/raft, still resets offline, and the new password logs in. Break:
// decide by the presence of ring/raft; red.
func TestResetPasswordOnAZeroConfigNodeStillWorks(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	cfg.Server.RootUser = "admin"
	cfg.Metrics.Enabled = false
	srv, err := prepare(cfg, discardSlog(), io.Discard, noTerminal())
	if err != nil {
		t.Fatal(err)
	}
	startedOnce(t, srv)
	if _, err := os.Stat(filepath.Join(cfg.Storage.DataDir, "ring", "raft")); err != nil {
		t.Fatalf("a started zero-config node has no ring/raft, so this test would not cover F8: %v", err)
	}
	if _, _, err := server.ResetPassword(cfg, "admin", "a-new-root-password", false, discardSlog()); err != nil {
		t.Fatalf("the offline reset on a zero-config node: %v", err)
	}
	requireRootAuthenticates(t, cfg, "a-new-root-password")
}
