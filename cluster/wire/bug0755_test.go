package wire

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestOpeningANodeDoesNotRaceTheRaftApplyOnSeeding (BUG-0755): the Raft apply hook reads
// clusterParts.seeding on the FSM goroutine while Node.open assigns it after openRaft has
// started the library. The test holds the apply of the log's last command (a setting, not a
// schema command, so its apply takes no pair lock) until a goroutine with no happens-before
// to the open goroutine releases it, the widest window a one-member node can be given. It
// does NOT fail deterministically under the detector: on a one-member node the open
// goroutine's own fsm.mu reads after the write order the held apply's read behind the
// write, and the detector reports the race only when a follower's apply lands between the
// write and that next read, which the three-node harness hits (2 of 4 full -race runs before
// the fix) and a hook cannot force without a synchronisation that would hide it. The guard
// stands for the mechanism; the harness -race gate is the evidence. [BREAK: none that this
// test catches on its own; see the harness gate]
func TestOpeningANodeDoesNotRaceTheRaftApplyOnSeeding(t *testing.T) {
	dir := t.TempDir()
	node := s0OpenClusterNode(t, dir, nil)
	s0Schema(t, node)
	s0Insert(t, node, "u1")
	// A setting is the last command in the log: not a schema command, so its apply takes no
	// pair lock that would order it after the open goroutine's writes.
	if err := node.SetSetting(context.Background(), "step_down_ms", 6000); err != nil {
		t.Fatal(err)
	}
	before := node.State().StateSeq
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	raftApplyGate = func(kind byte) {
		if kind == cluster.CommandSetSetting {
			<-gate
		}
	}
	t.Cleanup(func() { raftApplyGate = nil })
	// The release has no happens-before with the open goroutine's assignment of the field: a
	// goroutine spawned before the open, waking on its own clock, not on a signal from it.
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(gate)
	}()
	reopened := s0OpenClusterNode(t, dir, nil)
	s0Await(t, "the held apply to land", 15*time.Second, func() bool { return reopened.State().StateSeq >= before })
}
