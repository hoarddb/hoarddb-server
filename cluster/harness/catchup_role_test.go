package harness_test

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCatchUpIsServedOnlyToACopyOfThePair: option C of S2B-5. A member that holds no copy of
// the pair is refused with the retryable EPOCH_FENCED text and receives no records, and a
// copy of the pair is served. [GUARD: skip the role check in serveCatchUp]
func TestCatchUpIsServedOnlyToACopyOfThePair(t *testing.T) {
	c := harness.NewCluster(t, 5, harness.Options{NoDetection: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2)
	if err != nil {
		t.Fatal(err)
	}
	state := c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	id, owner, copyNode, _ := proofID(t, c, "role")
	proofInsert(t, c, owner, id, "kept")
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	ask := transport.Message{
		Env:  transport.Envelope{Version: transport.EnvelopeVersion, Kind: 0x03, DBID: dbID, Slot: slot, Epoch: uint64(state.Epoch)},
		Body: make([]byte, 24), // from the zero position: everything the owner holds
	}

	reply, err := c.CallAs(copyNode, owner, transport.ChannelData, ask)
	if err != nil || reply == nil {
		t.Fatalf("a copy of the pair was refused: %v", err)
	}
	chunk, err := wire.DecodeCatchUpChunkBody(reply.Body)
	if err != nil || len(chunk.Records) == 0 {
		t.Fatalf("a copy of the pair got %d records (%v), want the pair's log", len(chunk.Records), err)
	}

	// Five members, RF 2: three hold no copy. A node that joined last may have been a copy
	// at the epoch before, which is served by design, so at least two of the three are
	// refused, with one text and no records.
	refused := 0
	for _, member := range c.IDs() {
		if member == owner || member == copyNode {
			continue
		}
		reply, err := c.CallAs(member, owner, transport.ChannelData, ask)
		if err == nil {
			continue
		}
		if reply != nil || !strings.Contains(err.Error(), "refused CATCHUP_REQ from "+member) || !strings.Contains(err.Error(), "this node is at epoch") {
			t.Fatalf("%s: refusal %v with reply %v, want the EPOCH_FENCED text and no records", member, err, reply)
		}
		refused++
	}
	if refused < 2 {
		t.Fatalf("only %d of the 3 members with no copy were refused", refused)
	}
}
