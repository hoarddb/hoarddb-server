package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// TestRaftStateOnDiskGivesUpOnAHeldLog: [GUARD the probe of the Raft state before the store's first
// start fails, naming the file, when another process holds the Raft log's lock, instead of waiting
// for ever; red when the bolt file is opened with no timeout] (review R2 F-10, F-12). A second
// process started on a ring directory whose store was moved aside while the first still runs is the
// case.
func TestRaftStateOnDiskGivesUpOnAHeldLog(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	held, err := bolt.Open(filepath.Join(dir, "raft", "raft.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	done := make(chan error, 1)
	go func() {
		_, err := RaftStateOnDisk(dir)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the probe read a Raft log another process holds")
		}
	case <-time.After(raftStateOpenTimeout + 10*time.Second):
		t.Fatal("the probe waited on the Raft log's lock and did not give up")
	}
}

// TestRaftStateOnDiskSeesASnapshotWithoutTheLog: [GUARD a Raft directory whose raft.db is gone and
// whose snapshot stayed holds state; red when the snapshot branch is skipped] (review R2 F-10,
// mutation X4).
func TestRaftStateOnDiskSeesASnapshotWithoutTheLog(t *testing.T) {
	dir := t.TempDir()
	snaps, err := hraft.NewFileSnapshotStoreWithLogger(snapshotStoreDir(dir), raftSnapshotsRetained, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := snaps.Create(hraft.SnapshotVersionMax, 7, 2, hraft.Configuration{}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write([]byte("a snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raft", "raft.db")); !os.IsNotExist(err) {
		t.Fatalf("SCENARIO NOT REACHED: raft.db exists (%v)", err)
	}
	hasState, err := RaftStateOnDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !hasState {
		t.Fatal("a Raft directory holding a snapshot was read as holding no state")
	}
}
