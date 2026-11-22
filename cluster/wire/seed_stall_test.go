package wire

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestTheSeedDriverRunsNoPassAgainstAHalfReplayedState (review N1): a restarted node replays six
// commands and its FSM stalls on the third create-database. The library has handed the whole log
// over by then, so a gate that opens on "handed and state_seq stable" lets a pass run against a
// state that lacks three databases, and the pass retires their pairs (with replication factor 1
// that is the only copy). No pass may start before the FSM applied the last command. Break: put
// back the stable-state_seq fallback in cluster.LoadLatch.
func TestTheSeedDriverRunsNoPassAgainstAHalfReplayedState(t *testing.T) {
	dir := t.TempDir()
	first := openDivergedRaft(t, dir)
	for id := int64(1); id <= 5; id++ {
		name := "d" + string(rune('0'+id))
		if _, err := first.Propose(cluster.Command{Version: cluster.CommandVersion, Kind: cluster.CommandCreateDatabase,
			BaseSeq: uint64(first.State().StateSeq), Body: plainCreate(name, id)}); err != nil {
			t.Fatal(err)
		}
	}
	want := first.StateSeq()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	var seen atomic.Int32
	cfg := divergedRaftConfig(t, dir)
	cfg.Bootstrap = false
	cfg.ApplyGate = func(kind byte) {
		if kind == cluster.CommandCreateDatabase && seen.Add(1) == 3 {
			time.Sleep(700 * time.Millisecond)
		}
	}
	raft, err := cluster.OpenRaft(cfg)
	if err != nil {
		_ = cfg.Listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raft.Close() })
	n := bareNode(raft)
	g, parts := seedingRig(t, n, true)
	n.cl.seeding.Store(parts)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		seqBefore := raft.StateSeq()
		if err := n.driveSeeding(t.Context()); err != nil {
			t.Fatalf("driveSeeding: %v", err)
		}
		if parts.passes.Load() > 0 {
			if seqBefore != want {
				t.Fatalf("a seeding pass ran at state_seq %d, the log ends at %d: it saw a half-replayed state", seqBefore, want)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("no seeding pass ever ran after the replay finished")
}
