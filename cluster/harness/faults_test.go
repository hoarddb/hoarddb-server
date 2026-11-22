package harness_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// proofFollowers returns the two nodes that are not the Raft leader.
func proofFollowers(t *testing.T, c *harness.Cluster) (a, b string) {
	t.Helper()
	leader := c.AwaitLeader(10 * time.Second)
	var rest []string
	for _, id := range c.IDs() {
		if id != leader {
			rest = append(rest, id)
		}
	}
	return rest[0], rest[1]
}

// TestPartitionIsSymmetric cuts two followers from each other (the leader keeps both, so
// the Raft group is undisturbed). A write on either side whose copy is across the cut
// cannot reach it: applied_degraded with one copy, in both directions. Healed, both copies
// catch up.
func TestPartitionIsSymmetric(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	a, b := proofFollowers(t, c)
	atoB := proofIDsOwnedBy(t, c, a, b, "ab", 1)
	btoA := proofIDsOwnedBy(t, c, b, a, "ba", 1)
	c.Partition([]string{a}, []string{b})
	for _, tc := range []struct{ via, id string }{{a, atoB[0]}, {b, btoA[0]}} {
		got := proofInsert(t, c, tc.via, tc.id, "cut")
		if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Fatalf("a write on %s with its copy across the cut: %v/%d, want applied_degraded/1: the partition must hold in this direction", tc.via, got.Outcome, got.CopiesConfirmed)
		}
	}
	c.Heal()
	for _, tc := range []struct{ owner, copyNode, id string }{{a, b, atoB[0]}, {b, a, btoA[0]}} {
		c.Await(fmt.Sprintf("%s to catch %s up after the heal", tc.copyNode, tc.id), 30*time.Second, func() bool {
			_, found, err := c.Node(tc.copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, tc.id)
			return err == nil && found
		})
	}
}

// TestKillLeavesTheDataDirIntact: a restarted node still holds what it acknowledged.
func TestKillLeavesTheDataDirIntact(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, copyNode, _ := proofID(t, c, "dir")
	proofInsert(t, c, owner, id, "kept")
	c.Kill(copyNode)
	c.Restart(copyNode)
	c.AwaitLeases(20 * time.Second)
	if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
		t.Fatalf("%s lost %s across a kill and restart on the same directory: found %v err %v", copyNode, id, found, err)
	}
}

// TestDelayedChannelIsSlowNotBroken: a 200 ms delay on the owner's data writes to the copy
// shows in the latency of a majority write and breaks nothing.
func TestDelayedChannelIsSlowNotBroken(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	ids := proofIDsOwnedBy(t, c, "n1", "n2", "delay", 2)
	// The first write warms the connection. It is not a latency baseline: on a loaded host it
	// can take longer than the delayed one, which says nothing about the delay.
	proofInsert(t, c, "n1", ids[0], "fast")
	c.DelayChannel("n1", "n2", transport.ChannelData, 200*time.Millisecond)
	began := time.Now()
	got := proofInsert(t, c, "n1", ids[1], "slow")
	elapsed := time.Since(began)
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("a delayed channel changed the outcome to %v/%d, want applied/2", got.Outcome, got.CopiesConfirmed)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("a write over a channel delayed by 200 ms took %s: the delay did not act", elapsed)
	}
}

// TestDroppedChannelIsRedialedBehindBackoff: the channel is cut and its next two dials
// refused; the copy still ends up holding the document.
func TestDroppedChannelIsRedialedBehindBackoff(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	ids := proofIDsOwnedBy(t, c, "n1", "n2", "drop", 1)
	c.DropChannel("n1", "n2", transport.ChannelData, 2)
	proofInsert(t, c, "n1", ids[0], "dropped")
	c.Await("the copy to hold the document after the dropped channel", 30*time.Second, func() bool {
		_, found, err := c.Node("n2").LocalDocument(proofCtx(t), proofDB, proofBucket, ids[0])
		return err == nil && found
	})
}

// TestDiskFaultRefusesTheWriteAndTheRetryIsAppliedOnce: while the owner's log refuses
// appends the write is refused (UNAVAILABLE, no outcome, no position), and once the disk
// works the same write is applied exactly once.
func TestDiskFaultRefusesTheWriteAndTheRetryIsAppliedOnce(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	ids := proofIDsOwnedBy(t, c, "n1", "n2", "disk", 1)
	c.FailDisk("n1", true)
	_, err := c.Node("n1").Insert(proofCtx(t), proofDB, proofBucket, docOf(ids[0], "no disk"))
	if err == nil {
		t.Fatal("an insert was acknowledged while the owner's log refused appends")
	}
	if _, found, _ := c.Node("n2").LocalDocument(proofCtx(t), proofDB, proofBucket, ids[0]); found {
		t.Fatal("the copy applied a write the owner never made durable")
	}
	c.FailDisk("n1", false)
	got := proofInsert(t, c, "n1", ids[0], "disk back")
	if got.Outcome != machine.OutcomeApplied {
		t.Fatalf("the retry after the disk came back: %+v", got)
	}
	if _, err := c.Node("n1").Insert(proofCtx(t), proofDB, proofBucket, docOf(ids[0], "again")); err == nil || !strings.Contains(err.Error(), "already in") {
		t.Fatalf("a second insert of %s: %v, want DOCUMENT_EXISTS: the retry must have applied once", ids[0], err)
	}
}

// TestTheWriteConcernTimeoutKnobIsLive: a degraded write waits exactly as long as
// replication.wc_timeout_ms says. The same silent copy answers fast under a short timeout
// and slow under a long one, so a hard-coded wait fails one of the two.
//
// The copy is reachable but its acknowledgements are lost (Stall), not killed: a killed
// copy refuses connections at once, the degraded-ack breaker (BUG-0762) then ends the wait after
// the failed sends, and the write no longer waits the knob. A copy that is up and silent gives
// neither trigger anything to short-circuit on the first write, so the timeout is what ends it.
func TestTheWriteConcernTimeoutKnobIsLive(t *testing.T) {
	for _, tc := range []struct {
		timeout time.Duration
		min     time.Duration
		max     time.Duration
	}{
		{250 * time.Millisecond, 0, 800 * time.Millisecond},
		{1800 * time.Millisecond, 1700 * time.Millisecond, 4 * time.Second},
	} {
		c := proofSetup(t, harness.Options{NoDetection: true, WCTimeout: tc.timeout, RaftTimeout: 600 * time.Millisecond})
		leader := c.AwaitLeader(10 * time.Second)
		var id, owner, copyNode string
		// A stalled leader stalls every lease of the cluster, which is a different fault.
		for attempt := 0; copyNode == "" || copyNode == leader; attempt++ {
			id, owner, copyNode, _ = proofID(t, c, fmt.Sprintf("wc%d", attempt))
		}
		// Each survivor resolves the bucket's declaration before the stall, as concernCopyDown does.
		for i, via := range []string{owner, c.IDs()[0]} {
			if via != copyNode {
				proofInsert(t, c, via, fmt.Sprintf("wc-warm-%d", i), "warm")
			}
		}
		c.Stall(copyNode, true)
		t.Cleanup(func() { c.Stall(copyNode, false) }) // a stalled node cannot close
		began := time.Now()
		got := proofInsert(t, c, owner, id, "degraded")
		elapsed := time.Since(began)
		if got.Outcome != machine.OutcomeAppliedDegraded {
			t.Fatalf("timeout %s: outcome %v, want applied_degraded", tc.timeout, got.Outcome)
		}
		if elapsed < tc.min || elapsed > tc.max {
			t.Fatalf("with wc_timeout %s a degraded write took %s, want between %s and %s", tc.timeout, elapsed, tc.min, tc.max)
		}
	}
}

// TestAStalledCopyDoesNotHoldBackTheOtherCopies: one copy stops answering with its
// connections open. The owner's replication to the other copies still goes, within one
// call timeout, instead of waiting on the silent peer forever.
func TestAStalledCopyDoesNotHoldBackTheOtherCopies(t *testing.T) {
	// Two product causes made this test fail one run in three, neither of them a Raft flap:
	// the replicator shipped to its copies one at a time, so a stalled copy held up the
	// healthy one, and a copy that fetched the declaration from the stalled node waited the
	// whole call timeout for it. Both are fixed; the test runs in every build.
	c := proofSetup(t, harness.Options{NoDetection: true, WCTimeout: 8 * time.Second, RaftTimeout: 600 * time.Millisecond})
	// A copy that has never seen the bucket's declaration cannot append a replicated record
	// (it refuses until it can fetch it), so a real copy has it cached by the time a node
	// stalls; warm every node's cache the way an earlier write would have.
	for _, id := range c.IDs() {
		c.Await("the node to resolve the bucket", 20*time.Second, func() bool {
			_, _, err := c.Node(id).LocalDocument(proofCtx(t), proofDB, proofBucket, "warm")
			return err == nil
		})
	}
	// Stall a node that is not the Raft leader: a stalled leader stalls the leases of the
	// whole cluster, which is a different fault.
	leader := c.AwaitLeader(10 * time.Second)
	var rest []string
	for _, id := range c.IDs() {
		if id != leader {
			rest = append(rest, id)
		}
	}
	owner, silent, healthy := leader, rest[0], rest[1]
	toStalled := proofIDsOwnedBy(t, c, owner, silent, "stall-a", 1)
	toHealthy := proofIDsOwnedBy(t, c, owner, healthy, "stall-b", 1)
	c.Stall(silent, true)
	t.Cleanup(func() { c.Stall(silent, false) })
	go func() {
		_, _ = c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, docOf(toStalled[0], "to the silent copy"))
	}()
	time.Sleep(300 * time.Millisecond) // let that write reach its blocked send
	began := time.Now()
	got := proofInsert(t, c, owner, toHealthy[0], "to the healthy copy")
	t.Logf("healthy write took %s outcome %v", time.Since(began), got.Outcome)
	if got.Outcome != machine.OutcomeApplied || time.Since(began) > 3500*time.Millisecond {
		t.Fatalf("a write to a healthy copy took %s and answered %v while another copy was silent: want applied within one call timeout", time.Since(began), got.Outcome)
	}
}

// TestAOneWayCutRefusesDialsInOneDirectionOnly cuts n1 to n2: n1's dials to n2 are refused on
// the data and the control channel, n2's dials to n1 succeed.
func TestAOneWayCutRefusesDialsInOneDirectionOnly(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	c.BlockOneWay("n1", "n2", true)
	for _, ch := range []transport.Channel{transport.ChannelData, transport.ChannelControl} {
		if conn, err := c.DialChannel("n1", "n2", ch); err == nil {
			_ = conn.Close()
			t.Fatalf("n1 dialed n2 on the %s channel through a one-way cut n1 to n2", ch)
		}
		conn, err := c.DialChannel("n2", "n1", ch)
		if err != nil {
			t.Fatalf("n2 could not dial n1 on the %s channel: %v: the cut must be one-way", ch, err)
		}
		_ = conn.Close()
	}
}

// TestAOneWayCutSeversLiveConnectionsInThatDirectionOnly: connections open before the cut, one
// each way, and only the one n1 dialed dies.
func TestAOneWayCutSeversLiveConnectionsInThatDirectionOnly(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	forward, err := c.DialChannel("n1", "n2", transport.ChannelControl)
	if err != nil {
		t.Fatalf("dialing n1 to n2: %v", err)
	}
	defer func() { _ = forward.Close() }()
	backward, err := c.DialChannel("n2", "n1", transport.ChannelControl)
	if err != nil {
		t.Fatalf("dialing n2 to n1: %v", err)
	}
	defer func() { _ = backward.Close() }()
	c.BlockOneWay("n1", "n2", true)
	// A severed connection is closed locally, so even setting its deadline fails; a live one
	// just times out the read.
	if err := forward.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err == nil {
		if _, err := forward.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the connection n1 dialed to n2 after the cut: read %v, want it severed", err)
		}
	}
	if err := backward.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("the connection n2 dialed to n1 was closed by a cut in the other direction: %v", err)
	}
	if _, err := backward.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the connection n2 dialed to n1 after the cut: read %v, want it alive (a read timeout)", err)
	}
}

// TestHealClearsAOneWayCut: after Heal the cut direction dials again, and BlockOneWay(false)
// lifts one direction without a Heal.
func TestHealClearsAOneWayCut(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	c.BlockOneWay("n1", "n2", true)
	c.Heal()
	conn, err := c.DialChannel("n1", "n2", transport.ChannelData)
	if err != nil {
		t.Fatalf("n1 could not dial n2 after Heal: %v", err)
	}
	_ = conn.Close()
	c.BlockOneWay("n1", "n2", true)
	c.BlockOneWay("n1", "n2", false)
	conn, err = c.DialChannel("n1", "n2", transport.ChannelData)
	if err != nil {
		t.Fatalf("n1 could not dial n2 after BlockOneWay(false): %v", err)
	}
	_ = conn.Close()
}

// TestAOneWayCutOfTheOwnersEgressLapsesItsLease: A10's core. Three voters. The owner of a slot
// can receive everything and send nothing it dials, so its lease request never reaches the
// leader and no grant comes back. Within step_down_ms plus heartbeat_ms its writes are
// LEASE_LAPSED and never applied.
//
// Found while writing it: the other nodes do NOT commit the owner down. They probe the owner
// by dialing it, which a cut of the owner's egress leaves alone, so the owner stays an
// active member at the same epoch with a lapsed lease (also when the owner is the Raft
// leader). A10's "the others commit it down" needs a fault on the probes (BlockOneWay from
// each observer to the owner), not on the owner's egress.
func TestAOneWayCutOfTheOwnersEgressLapsesItsLease(t *testing.T) {
	c := proofSetup(t, harness.Options{FastDetection: true})
	leader := c.AwaitLeader(10 * time.Second)
	owner, copyNode := "", ""
	for _, id := range c.IDs() {
		if id != leader && owner == "" {
			owner = id
		}
	}
	for _, id := range c.IDs() {
		if id != owner {
			copyNode = id
			break
		}
	}
	settings := c.Node(owner).State().Settings
	bound := time.Duration(settings.StepDownMs+settings.HeartbeatMs) * time.Millisecond
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "lapse", 2)
	if got := proofInsert(t, c, owner, ids[0], "before the cut"); got.Outcome != machine.OutcomeApplied {
		t.Fatalf("before the cut the owner answered %v, want applied", got.Outcome)
	}
	for _, other := range c.IDs() {
		if other != owner {
			c.BlockOneWay(owner, other, true)
		}
	}
	began := time.Now()
	c.Await("the owner's lease to lapse", bound+time.Second, func() bool { return !c.Node(owner).LeaseValid() })
	if elapsed := time.Since(began); elapsed > bound+500*time.Millisecond {
		t.Fatalf("the owner's lease lapsed after %s, want within step_down_ms+heartbeat_ms = %s", elapsed, bound)
	}
	_, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, docOf(ids[1], "after the cut"))
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeLeaseLapsed {
		t.Fatalf("a write on the owner with its egress cut: %v, want LEASE_LAPSED", err)
	}
	for _, id := range c.IDs() {
		if _, found, _ := c.Node(id).LocalDocument(proofCtx(t), proofDB, proofBucket, ids[1]); found {
			t.Fatalf("%s holds %s, a write the lapsed owner refused", id, ids[1])
		}
	}
}
