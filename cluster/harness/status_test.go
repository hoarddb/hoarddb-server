package harness_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestSeedingStatusIsEmptyWhenEverySlotIsReady: on a settled cluster no node lists a row, and
// the view is the node's own (a retry of a ready pair is refused by naming it ready, where the
// NoSeeding value says absent). [BREAK: in seedview_node.go Status drop the ready filter]
func TestSeedingStatusIsEmptyWhenEverySlotIsReady(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	seed, owner, copyNode, _ := proofID(t, c, "statusready")
	slot, _ := gateSeededPair(t, c, seed, owner, copyNode)
	for _, id := range c.IDs() {
		if rows := c.Node(id).SeedingView().Status(); len(rows) != 0 {
			t.Errorf("%s lists %d seeding rows on a settled cluster: %+v", id, len(rows), rows)
		}
	}
	err := c.Node(copyNode).SeedingView().Retry(proofCtx(t), proofDB, int(slot))
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeSeedNotHeld || !strings.Contains(refusal.Message, "is ready on this node") {
		t.Fatalf("retry of a ready pair = %v, want SEED_NOT_HELD naming it ready", err)
	}
}

// TestSeedingStatusNamesTheWaitingMember: a copy that left its copy set keeps its range while
// the owner's answers are lost, and its status row says which member the gate waits for; when
// the answers return the row leaves. [BREAK: in seedview_node.go Status leave the wait text
// out of a retired row]
func TestSeedingStatusNamesTheWaitingMember(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	seed, owner, copyNode, _ := proofID(t, c, "statuswait")
	slot, dbID := gateSeededPair(t, c, seed, owner, copyNode)

	c.DropReplies(copyNode, owner, transport.ChannelData, true)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if err := leader.AlterDatabaseRF(proofCtx(t), proofDB, 1); err != nil {
		t.Fatalf("dropping the database to one copy: %v", err)
	}
	c.Await("the copy's record to be retired", 30*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotRetired
	})
	view := c.Node(copyNode).SeedingView()
	var row wire.SeedRow
	c.Await("the copy's status to hold a retired row that names the owner", 30*time.Second, func() bool {
		for _, candidate := range view.Status() {
			if candidate.Slot == int(slot) && candidate.State == wire.SeedRetired && strings.Contains(candidate.Refusal, owner) {
				row = candidate
				return true
			}
		}
		return false
	})
	if row.Database != proofDB {
		t.Errorf("row names database %q, want %q", row.Database, proofDB)
	}

	c.DropReplies(copyNode, owner, transport.ChannelData, false)
	if err := leader.SetSetting(proofCtx(t), "probe_interval_ms", 1000); err != nil {
		t.Fatalf("committing a setting to wake the gate: %v", err)
	}
	// Base on a gate VM: the test took 44.4 s in all; twice that, rounded up.
	c.Await("the retired row to leave once the owner answered", 90*time.Second, func() bool {
		return len(view.Status()) == 0
	})
}
