package harness_test

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// concernCopyDown forms the RF 2 proof cluster with detection and probes off, picks an id,
// and kills its copy, so only the owner can confirm a write.
func concernCopyDown(t *testing.T, seed string) (c *harness.Cluster, id, owner, outsider string) {
	t.Helper()
	c = proofSetup(t, harness.Options{NoDetection: true, NoProbes: true})
	id, owner, copyNode, outsider := proofID(t, c, seed)
	// Each survivor resolves the bucket's declaration before the kill, so the writes below
	// are not at the mercy of which node holds the declaration.
	for i, via := range []string{owner, outsider} {
		proofInsert(t, c, via, seed+"-warm-"+string(rune('a'+i)), "warm")
	}
	c.Kill(copyNode)
	return c, id, owner, outsider
}

func concernInsert(t *testing.T, c *harness.Cluster, via, id string, options ...wire.WriteOption) wire.Inserted {
	t.Helper()
	var got wire.Inserted
	err := proofRetryTransient(c, func() (insertErr error) {
		got, insertErr = c.Node(via).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "ada"}}, options...)
		return insertErr
	})
	if err != nil {
		t.Fatalf("insert of %s through %s: %v (background: %v)", id, via, err, c.BackgroundErrors())
	}
	return got
}

// TestWithConcernOneAcksWithTheOwnerAlone: at RF 2 with the copy gone, a write that asks for
// concern one is applied on the owner's copy, where the default (majority) would wait out
// wc_timeout and answer applied_degraded.
// [BREAK: Coordinator.Insert builds its WriteRequest with Concern: 0].
func TestWithConcernOneAcksWithTheOwnerAlone(t *testing.T) {
	c, id, owner, _ := concernCopyDown(t, "one")
	got := concernInsert(t, c, owner, id, wire.WithConcern(machine.ConcernOne))
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 1 {
		t.Fatalf("a concern-one write with the copy down answered %v/%d, want applied/1", got.Outcome, got.CopiesConfirmed)
	}
}

// TestWithConcernIsKeptAcrossAForward: the same write entered at a node that does not own the
// pair is forwarded, and the owner still answers at concern one.
// [BREAK: node_cluster.go sets req.Hop, req.Concern = 1, 0 before the forward].
func TestWithConcernIsKeptAcrossAForward(t *testing.T) {
	c, id, owner, outsider := concernCopyDown(t, "fwd")
	got := concernInsert(t, c, outsider, id, wire.WithConcern(machine.ConcernOne))
	if got.ServedBy != owner {
		t.Fatalf("the write was ordered by %q, want the owner %s through the forward", got.ServedBy, owner)
	}
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 1 {
		t.Fatalf("a forwarded concern-one write answered %v/%d, want applied/1", got.Outcome, got.CopiesConfirmed)
	}
}

// TestAZeroConcernTakesTheDatabaseDefault: with no option the database's majority applies, so
// one confirmed copy at RF 2 is applied_degraded.
// [BREAK: writepath.go reads level = machine.ConcernOne when req.Concern is zero].
func TestAZeroConcernTakesTheDatabaseDefault(t *testing.T) {
	c, id, owner, _ := concernCopyDown(t, "zero")
	got := concernInsert(t, c, owner, id)
	if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
		t.Fatalf("a write with no concern and the copy down answered %v/%d, want applied_degraded/1", got.Outcome, got.CopiesConfirmed)
	}
	if wire.ConcernOf() != 0 || wire.ConcernOf(wire.WithConcern(machine.ConcernAll)) != machine.ConcernAll {
		t.Fatal("ConcernOf must read zero for no option and the level for WithConcern")
	}
}
