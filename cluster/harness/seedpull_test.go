package harness_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// t3Cluster forms three voters with an RF database holding count documents, and one late
// identity for the node the test adds.
func t3Cluster(t *testing.T, rf, count int, opts harness.Options) (*harness.Cluster, []string) {
	t.Helper()
	opts.Late, opts.NoDetection = 1, true
	c := harness.NewCluster(t, 3, opts)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, rf); err != nil {
		t.Fatalf("creating the RF=%d database: %v", rf, err)
	}
	c.AwaitConverged(10 * time.Second)
	var bucketErr error
	for range 20 {
		c.AwaitLeases(30 * time.Second)
		if _, bucketErr = leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); !harness.IsTransientRefusal(bucketErr) {
			break
		}
	}
	if bucketErr != nil {
		t.Fatalf("creating the bucket: %v", bucketErr)
	}
	c.AwaitLeases(30 * time.Second)
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%03d", i)
		t3InsertWithRetry(t, c, ids[i])
	}
	return c, ids
}

// t3Held lists the documents the added node holds at the committed table, and checks each
// one byte for byte against its owner's copy.
func t3Compare(t *testing.T, c *harness.Cluster, added string, ids []string) (held int) {
	t.Helper()
	for _, id := range ids {
		owner, copies, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatal(err)
		}
		holds := false
		for _, copyOf := range copies {
			holds = holds || copyOf == added
		}
		if !holds {
			continue
		}
		held++
		want, found, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || !found {
			t.Fatalf("the owner %s of %s lost it: found %v err %v", owner, id, found, err)
		}
		got, found, err := c.Node(added).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || !found {
			t.Fatalf("%s did not pull %s from %s: found %v err %v", added, id, owner, found, err)
		}
		if fmt.Sprint(got.Fields) != fmt.Sprint(want.Fields) {
			t.Errorf("%s holds %v for %s, its owner %s holds %v", added, got.Fields, id, owner, want.Fields)
		}
		served, err := c.Node(added).Get(proofCtx(t), proofDB, proofBucket, id)
		// A lease that lapses on a loaded host passes by itself (IsTransientRefusal): wait for the
		// leases and read again, as the other harness tests do.
		for try := 0; try < 3 && harness.IsTransientRefusal(err); try++ {
			c.AwaitLeases(20 * time.Second)
			served, err = c.Node(added).Get(proofCtx(t), proofDB, proofBucket, id)
		}
		if err != nil {
			t.Fatalf("a read of %s through %s after it was seeded: %v", id, added, err)
		}
		if !bytes.Contains(served.Body, []byte(id)) {
			t.Errorf("a read of %s through %s answered a body without the id: %x", id, added, served.Body)
		}
	}
	return held
}

func TestANodeAddedToADatabaseSeedsItsPairsThenServesEveryDocument(t *testing.T) {
	for _, rf := range []int{2, 3} {
		t.Run(fmt.Sprintf("RF%d", rf), func(t *testing.T) {
			c, ids := t3Cluster(t, rf, 40, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8, ChunkEntries: 2}})
			added := c.AddNode()
			// Base on a gate VM: the RF3 subtest took 77.2 s in all; twice that, rounded up.
			c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
			c.AwaitConverged(20 * time.Second)
			c.AwaitLeases(20 * time.Second)
			if held := t3Compare(t, c, added, ids); held == 0 {
				t.Fatalf("%s holds none of the %d documents after it joined; the test proves nothing", added, len(ids))
			}
			dbID := uint64(c.Node("n1").State().Databases[0].ID)
			for slot := range uint16(storage.SlotCount) {
				if c.Node(added).SlotRecordState(slot, dbID) != machine.SlotReady {
					continue
				}
				owner, ok := c.Node(added).OwnerOfSlot(slot, dbID)
				if !ok || owner == added {
					continue
				}
				_, want, err := c.Node(owner).PairDump(slot, dbID)
				if err != nil {
					t.Fatal(err)
				}
				_, got, err := c.Node(added).PairDump(slot, dbID)
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("slot %d: %s holds %d keys, owner %s holds %d", slot, added, len(got), owner, len(want))
				}
				for i := range want {
					if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
						t.Fatalf("slot %d key %d differs between %s and its owner %s", slot, i, added, owner)
					}
				}
			}
			if pending := c.Node(added).SeedingPairs(); pending != 0 {
				t.Errorf("%d pairs are still queued after the node was activated", pending)
			}
		})
	}
}

func TestActivateIsRefusedWhileTheJoinerStillHasPairsToPull(t *testing.T) {
	c, _ := t3Cluster(t, 2, 10, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	for _, source := range []string{"n1", "n2", "n3"} {
		c.DelayChannel("n4", source, transport.ChannelSeed, 2*time.Millisecond)
	}
	added := c.AddNode()
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	err := leader.Activate(proofCtx(t), added)
	if !errors.Is(err, wire.ErrStillSeeding) {
		t.Fatalf("activating a joiner that has pulled nothing: %v, want ErrStillSeeding", err)
	}
	for _, member := range leader.State().Members {
		if member.ID == added && member.State != cluster.MemberJoining {
			t.Errorf("%s is %s after a refused activation, want joining", added, member.State)
		}
	}
	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
}

func TestAPullerKilledMidStreamResumesAndEndsWithTheOwnersBytes(t *testing.T) {
	c, ids := t3Cluster(t, 2, 40, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8, ChunkEntries: 2}})
	for _, source := range []string{"n1", "n2", "n3"} {
		c.DelayChannel("n4", source, transport.ChannelSeed, 2*time.Millisecond)
	}
	added := c.AddNode()
	// 30 s was not enough on the cut: a slow host pulled fewer than 1138 of the 4138 pairs in it.
	c.Await("some pairs to be pulled and some left", 120*time.Second, func() bool {
		pending := c.Node(added).SeedingPairs()
		return pending > 0 && pending < 3000
	})
	c.Kill(added)
	c.Restart(added)
	// The cut seeds a pair slower than the base (a marker write per store sync), so a fixed 90 s
	// ran out with 2899 of 4138 pairs pulled; the bound is progress, not elapsed time.
	c.ActivateWhileSeedingMoves(added, quorumSeedingStall, quorumSeedingBudget)
	c.AwaitConverged(20 * time.Second)
	c.AwaitLeases(20 * time.Second)
	if held := t3Compare(t, c, added, ids); held == 0 {
		t.Fatalf("%s holds none of the documents after the restart", added)
	}
}

func TestBusySourcesAreRetriedUntilEveryPairIsPulled(t *testing.T) {
	c, ids := t3Cluster(t, 2, 20, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 6, IncomingMax: 1, BusyRetryAfter: 10 * time.Millisecond}})
	added := c.AddNode()
	// Base on a gate VM: the test took 55.4 s in all; twice that, rounded up.
	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
	c.AwaitConverged(20 * time.Second)
	c.AwaitLeases(20 * time.Second)
	if held := t3Compare(t, c, added, ids); held == 0 {
		t.Fatalf("%s holds none of the documents", added)
	}
}

func TestAnOversizedEntryIsAStalledStatusWithTheSourcesDetail(t *testing.T) {
	c, _ := t3Cluster(t, 2, 4, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 4, WindowBytes: 512}})
	big := strings.Repeat("x", 4000)
	for i := range 12 {
		if _, err := c.Node("n1").Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: fmt.Sprintf("big-%d", i)}, {Key: "name", Value: big}}); err != nil {
			t.Fatalf("inserting a large document: %v", err)
		}
	}
	added := c.AddNode()
	dbID := uint64(c.Node("n1").State().Databases[0].ID)
	var detail string
	c.Await("a stalled pull with the source's detail", 60*time.Second, func() bool {
		for slot := range uint16(storage.SlotCount) {
			if status, ok := c.Node(added).SeedStatus(slot, dbID); ok && status.State == wire.PullStalled && strings.Contains(status.Detail, "seed_window_bytes") {
				detail = status.Detail
				return true
			}
		}
		return false
	})
	if !strings.Contains(detail, "Raise replication.seed_window_bytes") {
		t.Errorf("the stalled status does not name the fix: %q", detail)
	}
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if err := leader.Activate(context.Background(), added); !errors.Is(err, wire.ErrStillSeeding) {
		t.Errorf("a joiner with a stalled pair was activated: %v", err)
	}
}

func TestASeedingCopyDoesNotCountInTheQuorumOfAWrite(t *testing.T) {
	c, _ := t3Cluster(t, 2, 12, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := c.AddNode()
	// Base on a gate VM: the test took 50.3 s in all; twice that, rounded up.
	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
	c.AwaitConverged(20 * time.Second)
	c.AwaitLeases(20 * time.Second)

	// A document the added node holds as a copy of a pair whose owner is another node.
	var id, owner string
	var slot uint16
	for probe := 0; probe < 4000 && id == ""; probe++ {
		candidate := fmt.Sprintf("q-%d", probe)
		ownerOf, copies, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, candidate)
		if err != nil {
			t.Fatal(err)
		}
		if len(copies) == 2 && copies[0] == ownerOf && copies[1] == added {
			id, owner = candidate, ownerOf
			if slot, err = wire.PlacementOf(proofDB, proofBucket, candidate); err != nil {
				t.Fatal(err)
			}
		}
	}
	if id == "" {
		t.Fatal("no document is placed with the added node as the copy")
	}
	dbID := uint64(c.Node("n1").State().Databases[0].ID)
	// Hold the copy in the seeding state: its pulls are delayed longer than the write.
	c.DelayChannel(added, owner, transport.ChannelSeed, 3*time.Second)
	if err := c.Node(added).ReseedPair(slot, dbID); err != nil {
		t.Fatalf("putting the copy back to seeding: %v", err)
	}
	if state := c.Node(added).SlotRecordState(slot, dbID); state != machine.SlotSeeding {
		t.Fatalf("the copy's record is %v, want seeding", state)
	}
	got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Cy"}})
	if err != nil {
		t.Fatalf("the write failed outright, want it applied degraded: %v", err)
	}
	if got.CopiesConfirmed != 1 || got.Outcome != machine.OutcomeAppliedDegraded {
		t.Fatalf("outcome %v confirmed %d copies, want applied_degraded with 1: the seeding copy must not count", got.Outcome, got.CopiesConfirmed)
	}
	// The copy answered AckNotApplied: it applied nothing, so it does not hold the document.
	if _, found, _ := c.Node(added).LocalDocument(proofCtx(t), proofDB, proofBucket, id); found {
		t.Error("the seeding copy applied a pushed write")
	}
}

func TestASeedingOwnerRefusesItsWritesAndNothingIsWritten(t *testing.T) {
	c, _ := t3Cluster(t, 2, 12, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := c.AddNode()
	// Base on a gate VM: the test took 54.9 s in all; twice that, rounded up.
	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
	c.AwaitConverged(20 * time.Second)
	c.AwaitLeases(20 * time.Second)

	var id, copyNode string
	var slot uint16
	for probe := 0; probe < 4000 && id == ""; probe++ {
		candidate := fmt.Sprintf("o-%d", probe)
		ownerOf, copies, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, candidate)
		if err != nil {
			t.Fatal(err)
		}
		if len(copies) == 2 && ownerOf == added {
			id, copyNode = candidate, copies[1]
			if slot, err = wire.PlacementOf(proofDB, proofBucket, candidate); err != nil {
				t.Fatal(err)
			}
		}
	}
	if id == "" {
		t.Fatal("no document is owned by the added node")
	}
	dbID := uint64(c.Node("n1").State().Databases[0].ID)
	for _, source := range []string{"n1", "n2", "n3"} {
		c.DelayChannel(added, source, transport.ChannelSeed, 3*time.Second)
	}
	if err := c.Node(added).ReseedPair(slot, dbID); err != nil {
		t.Fatalf("putting the owner's pair back to seeding: %v", err)
	}
	began := time.Now()
	_, err := c.Node(added).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Di"}})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable || !strings.Contains(refusal.Message, "still seeding") {
		t.Fatalf("a write to a seeding owner got %v, want UNAVAILABLE naming the seed", err)
	}
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("the refusal took %s: it must not park the caller", took)
	}
	if _, found, _ := c.Node(added).LocalDocument(proofCtx(t), proofDB, proofBucket, id); found {
		t.Error("the seeding owner stored a document it refused")
	}
	if _, found, _ := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); found {
		t.Error("the copy holds a document the seeding owner refused")
	}
	if _, err := c.Node(added).Get(proofCtx(t), proofDB, proofBucket, id); err == nil {
		t.Error("a seeding pair answered a read as the owner")
	}
}

// t3SeededJoiner adds a node and waits until it has pulled every pair without activating it.
func t3SeededJoiner(t *testing.T, c *harness.Cluster) string {
	t.Helper()
	added := c.AddNode()
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	deadline := time.Now().Add(90 * time.Second)
	for {
		err := leader.CheckSeeded(proofCtx(t), added)
		if err == nil {
			return added
		}
		if !errors.Is(err, wire.ErrStillSeeding) || time.Now().After(deadline) {
			t.Fatalf("waiting for %s to be seeded: %v", added, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func t3MovedRefusal(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeStateMoved
}

// TestAnActivationDecidedOnAnOlderStateIsRefusedWhateverMoved (review F7): the gate checked
// the copy sets of one state_seq; an RF change moves them without moving the epoch, and any
// command moves state_seq. The activation carries the state_seq the gate used, so the FSM
// refuses it with STATE_MOVED instead of ranking a joiner for pairs nobody checked.
func TestAnActivationDecidedOnAnOlderStateIsRefusedWhateverMoved(t *testing.T) {
	c, _ := t3Cluster(t, 2, 6, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	added := t3SeededJoiner(t, c)
	leader := c.Node(c.AwaitLeader(10 * time.Second))

	seq := leader.State().StateSeq
	if err := leader.AlterDatabaseRF(proofCtx(t), proofDB, 3); err != nil {
		t.Fatalf("raising the copy count: %v", err)
	}
	if err := leader.ActivateAtStateSeq(proofCtx(t), added, seq); !t3MovedRefusal(err) {
		t.Fatalf("an activation decided before an RF change got %v, want STATE_MOVED", err)
	}

	seq = leader.State().StateSeq
	if err := leader.SetSetting(proofCtx(t), "probe_interval_ms", 1000); err != nil {
		t.Fatal(err)
	}
	if err := leader.ActivateAtStateSeq(proofCtx(t), added, seq); !t3MovedRefusal(err) {
		t.Fatalf("an activation decided before another command got %v, want STATE_MOVED", err)
	}
	for _, member := range leader.State().Members {
		if member.ID == added && member.State != cluster.MemberJoining {
			t.Fatalf("%s is %v after refused activations, want joining", added, member.State)
		}
	}
	// Nothing moved since the gate looked at the new RF: activate the ordinary way. Base on a
	// gate VM: the test took 94.3 s in all; twice that, rounded up.
	c.ActivateWhileSeedingMoves(added, 90*time.Second, 240*time.Second)
	for _, member := range c.Node(c.AwaitLeader(10 * time.Second)).State().Members {
		if member.ID == added && member.State != cluster.MemberActive {
			t.Errorf("%s is %v after a clean activation, want active", added, member.State)
		}
	}
}

// t3InsertWithRetry inserts through n1, trying again when a lease lapsed or a quorum round was late under load: that is
// the formation of the cluster, not what the tests are about.
func t3InsertWithRetry(t *testing.T, c *harness.Cluster, id string) {
	t.Helper()
	var last error
	for range 20 {
		_, last = c.Node("n1").Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada " + id}})
		if last == nil {
			return
		}
		if !harness.IsTransientRefusal(last) {
			break
		}
		c.AwaitLeases(30 * time.Second)
	}
	t.Fatalf("insert of %s through n1: %v", id, last)
}
