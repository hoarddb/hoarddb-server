package cluster

// Regression tests for the second fix round of the state-loaded rule (review N1 fix: M1 install
// snapshot on a quiet cluster, M2 damaged newest snapshot, m1 two-entry uncommitted tail).

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func restartOnAddr(t *testing.T, dir, id, addr string) *Raft {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	cfg := quickRaft(t, dir, id)
	_ = cfg.Listener.Close()
	cfg.Listener, cfg.Bootstrap = listener, false
	return openOrFail(t, cfg)
}

// A follower is down while the leader moves on and compacts; on restart the leader sends a
// snapshot. The cluster is then quiet. Does StateLoaded ever open?
func TestProbe3InstallSnapshotQuietCluster(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	follower, fcfg := joinSecond(t, leader)
	addr := fcfg.Listener.Addr().String()
	createDatabase(t, leader, "d0", 1)
	deadline := time.Now().Add(5 * time.Second)
	for lastLogIndex(t, follower) < lastLogIndex(t, leader) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	dir := follower.cfg.Dir
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	for i := int64(2); i <= 90; i++ {
		createDatabase(t, leader, fmt.Sprintf("d%d", i), i)
	}
	if err := leader.TakeSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, _ := leader.store.FirstIndex()
	t.Logf("leader first index %d last %d", first, lastLogIndex(t, leader))
	re := restartOnAddr(t, dir, "n2", addr)
	t.Logf("opened: openedLastCommand=%d", re.openedLastCommand)
	end := time.Now().Add(4 * time.Second)
	opened := false
	for time.Now().Before(end) {
		if re.StateLoaded() {
			opened = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ff, _ := re.store.FirstIndex()
	t.Logf("opened=%v LogApplied=%v handed=%v libApplied=%d fsmApplied=%d lastSnap=%s followerSeq=%d leaderSeq=%d followerDBs=%d logFirst=%d logLast=%d",
		opened, re.LogApplied(), re.LogHandedToFSM(), re.raft.AppliedIndex(), re.fsm.applied.Load(), re.raft.Stats()["last_snapshot_index"],
		re.StateSeq(), leader.StateSeq(), len(re.State().Databases), ff, lastLogIndex(t, re))
	if !opened {
		t.Errorf("WEDGE: healthy follower with the leader's full state never opens on a quiet cluster")
	}
}

func injectTail(t *testing.T, dir string, n int, stateSeq uint64) {
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft", "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	last, _ := store.LastIndex()
	var tail hraft.Log
	if err := store.GetLog(last, &tail); err != nil {
		t.Fatal(err)
	}
	for k := 1; k <= n; k++ {
		raw, err := EncodeCommand(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stateSeq, Body: fsmDoc(fsmStr("name", fmt.Sprintf("ghost%d", k)), fsmI64("id", int64(100+k)), fsmI32("rf", 1), fsmI64("schema_version", 1))})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.StoreLog(&hraft.Log{Index: last + uint64(k), Term: tail.Term - 1, Type: hraft.LogCommand, Data: raw}); err != nil {
			t.Fatal(err)
		}
	}
}

// Same as the B1 tail probe, but two uncommitted trailing commands; the leader commits one
// non-command entry.
func TestProbe3TwoEntryTail(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
			follower, fcfg := joinSecond(t, leader)
			addr := fcfg.Listener.Addr().String()
			createDatabase(t, leader, "d0", 1)
			deadline := time.Now().Add(5 * time.Second)
			for lastLogIndex(t, follower) < lastLogIndex(t, leader) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond)
			dir := follower.cfg.Dir
			if err := follower.Close(); err != nil {
				t.Fatal(err)
			}
			injectTail(t, dir, n, uint64(leader.State().StateSeq))
			re := restartOnAddr(t, dir, "n2", addr)
			if err := leader.raft.Barrier(5 * time.Second).Error(); err != nil {
				t.Fatal(err)
			}
			end := time.Now().Add(3 * time.Second)
			opened := false
			for time.Now().Before(end) {
				if re.StateLoaded() {
					opened = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("n=%d opened=%v handed=%v libApplied=%d openedLastCommand=%d fsmApplied=%d seq f=%d l=%d diverged=%v", n, opened, re.LogHandedToFSM(), re.raft.AppliedIndex(), re.openedLastCommand, re.fsm.applied.Load(), re.StateSeq(), leader.StateSeq(), re.IsDiverged())
			if !opened {
				t.Errorf("WEDGE with a %d-entry uncommitted tail", n)
			}
		})
	}
}

// Newest snapshot damaged; the library restores the older one and replays. restoredIndex is
// the newest's index.
func TestProbe3DamagedNewestSnapshot(t *testing.T) {
	dir := t.TempDir()
	r := openOrFail(t, quickRaft(t, dir, "n1"))
	for i := int64(1); i <= 3; i++ {
		createDatabase(t, r, fmt.Sprintf("a%d", i), i)
	}
	if err := r.TakeSnapshot(); err != nil {
		t.Fatal(err)
	}
	for i := int64(4); i <= 6; i++ {
		createDatabase(t, r, fmt.Sprintf("a%d", i), i)
	}
	if err := r.TakeSnapshot(); err != nil {
		t.Fatal(err)
	}
	want := r.StateSeq()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	snaps, _ := filepath.Glob(filepath.Join(snapshotDir(dir), "*", "state.bin"))
	if len(snaps) != 2 {
		t.Fatalf("snapshots: %v", snaps)
	}
	newest := snaps[0]
	for _, candidate := range snaps {
		if snapshotDirIndex(t, candidate) > snapshotDirIndex(t, newest) {
			newest = candidate
		}
	}
	t.Logf("damaging %s", newest)
	raw, err := os.ReadFile(newest)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(newest, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	var seen atomic.Int32
	cfg := quickRaft(t, dir, "n1")
	cfg.Bootstrap = false
	cfg.ApplyGate = func(kind byte) {
		if seen.Add(1) == 1 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	re := openOrFail(t, cfg)
	t.Logf("openedLastCommand=%d lastSnap=%s", re.openedLastCommand, re.raft.Stats()["last_snapshot_index"])
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if re.StateLoaded() {
			if re.StateSeq() != want || len(re.State().Databases) != 6 {
				t.Fatalf("EARLY: StateLoaded with seq %d want %d dbs %d", re.StateSeq(), want, len(re.State().Databases))
			}
			t.Logf("opened after load")
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("never opened")
}

// snapshotDirIndex is the log index in a snapshot directory named term-index-timestamp.
func snapshotDirIndex(t *testing.T, stateFile string) uint64 {
	t.Helper()
	var term, index, stamp uint64
	if _, err := fmt.Sscanf(filepath.Base(filepath.Dir(stateFile)), "%d-%d-%d", &term, &index, &stamp); err != nil {
		t.Fatalf("snapshot directory name of %s: %v", stateFile, err)
	}
	return index
}
