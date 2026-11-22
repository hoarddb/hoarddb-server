package wire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

func bug0764Scan(world *u36World, slots ...uint16) error {
	var mask cluster.SlotMask
	for _, slot := range slots {
		mask.Set(slot)
	}
	return world.gate.AdmitMask(context.Background(), testDB, testBucket, &mask)
}

// TestAScanOfAHeldOwnedSlotIsRetryableNotNotOwner (BUG-0764): a scan that meets a pair this node
// owns and holds is told to retry with the cause; it is never NOT_OWNER, which would send the
// caller to the owner, and the owner is the node that answered.
func TestAScanOfAHeldOwnedSlotIsRetryableNotNotOwner(t *testing.T) {
	cases := []struct {
		name  string
		setup func(world *u36World)
		cause string
	}{
		{"a primary handoff", func(world *u36World) { world.fx.setFlags(world.slot, flagSending|flagReplayed|flagStopProved) }, "handoff"},
		{"the restart barrier", func(world *u36World) { world.fx.setFlags(world.slot, flagAwaiting|flagReplayed) }, "restarted"},
		{"a log tail not replayed", func(world *u36World) { world.fx.setFlags(world.slot, flagStopProved) }, "replayed"},
		{"a copy still seeding", func(world *u36World) { world.states.set(world.slot, machine.SlotSeeding) }, "seeding"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := newU36World(t)
			tc.setup(world)
			err := bug0764Scan(world, world.slot)
			var coded *cluster.Error
			if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable {
				t.Fatalf("a scan of a held owned slot answers %v, want a retryable UNAVAILABLE", err)
			}
			text := err.Error()
			if !strings.Contains(text, tc.cause) || !strings.Contains(text, "Retry") || !strings.Contains(text, "Nothing was returned") {
				t.Errorf("the refusal must name %q, say nothing was returned and to retry: %s", tc.cause, text)
			}
			for _, bad := range []string{"is not the owner", "Send it to", "owns slot", "(any document of the slot)", "<"} {
				if strings.Contains(text, bad) {
					t.Errorf("the refusal of a held owned slot contains %q: %s", bad, text)
				}
			}
		})
	}
}

// TestAScanOfASlotAnotherNodeOwnsNamesThatNode (BUG-0764): NOT_OWNER stays for a slot that is
// someone else's and names that node, never this one.
func TestAScanOfASlotAnotherNodeOwnsNamesThatNode(t *testing.T) {
	world := newU36World(t)
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		owner, ok := world.fx.topo.table.Owner(slot, 2)
		if !ok || owner.ID == "n1" {
			continue
		}
		err := bug0764Scan(world, slot)
		u36Refusal(t, err, cluster.CodeNotOwner)
		if !strings.Contains(err.Error(), owner.ID+" owns slot") || strings.Contains(err.Error(), "n1 owns slot") {
			t.Fatalf("the refusal must name %s as the owner, not this node: %v", owner.ID, err)
		}
		return
	}
	t.Fatal("n1 owns every slot")
}

// TestAScanOfASlotNoNodeOwnsLeavesNoPlaceholder (BUG-0764): with every copy down there is no
// owner to name, and the refusal says so instead of printing the unfilled placeholder.
func TestAScanOfASlotNoNodeOwnsLeavesNoPlaceholder(t *testing.T) {
	world := newU36World(t)
	world.fx.topo.table = cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers()})
	err := bug0764Scan(world, world.slot)
	u36Refusal(t, err, cluster.CodeUnavailable)
	if strings.Contains(err.Error(), "<") || !strings.Contains(err.Error(), "no node owns it") {
		t.Fatalf("the refusal of an ownerless slot is %v", err)
	}
}
