package harness_test

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAReseedClearsTheCommittedBit: a copy that reported its pair ready has its bit set in the
// committed state; when the reseed turns the record seeding, the committed row already has the
// bit at 0, because the reseed's hook waited for the RETIRE. The reseed is driven through the
// same wire.seedParts.reseed a LOG_BELOW_HORIZON answer reaches.
// [BREAK: leave BeforeReset nil in startCluster]
func TestAReseedClearsTheCommittedBit(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	seed, owner, copyNode, _ := proofID(t, c, "clear")
	slot, err := wire.PlacementOf(proofDB, proofBucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	dbID := c.Node(owner).State().Databases[0].ID
	c.AwaitLeases(30 * time.Second)
	if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: seed}, {Key: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("insert of %s through the owner %s: %v (background: %v)", seed, owner, err, c.BackgroundErrors())
	}
	c.Await("the copy's pair to be ready", 30*time.Second, func() bool {
		return c.Node(copyNode).SlotRecordState(slot, uint64(dbID)) == machine.SlotReady
	})
	// The copy's own applied state must hold the bit: a lagging copy would show it clear for
	// the wrong reason.
	c.Await("the copy's ready bit to be committed and applied at the copy", 30*time.Second, func() bool {
		return seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot))
	})

	// Slow the pull so the pair stays seeding while the row is read.
	c.DelayChannel(copyNode, owner, transport.ChannelSeed, 5*time.Second)
	if err := c.Node(copyNode).ReseedPair(slot, uint64(dbID)); err != nil {
		t.Fatalf("reseeding the copy's pair: %v", err)
	}
	if got := c.Node(copyNode).SlotRecordState(slot, uint64(dbID)); got != machine.SlotSeeding {
		t.Fatalf("after the reseed the record is %v, want seeding", got)
	}
	if seedRowHas(c.Node(copyNode).State(), copyNode, dbID, int(slot)) {
		t.Fatalf("the record is seeding and the committed row of %s still has bit %d", copyNode, slot)
	}
	c.DelayChannel(copyNode, owner, transport.ChannelSeed, 0)
}
