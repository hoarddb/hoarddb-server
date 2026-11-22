package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// cededWriteScenario names an owner, a bucket a join of a member that never
// starts moves away from it (moved), a bucket the owner keeps (kept), and a
// replica that stays in both buckets' sets.
type cededWriteScenario struct {
	owner, replica int
	newcomer       ring.Node
	moved, kept    string
}

func findCededWriteScenario(t *testing.T, tc *testCluster) cededWriteScenario {
	t.Helper()
	newcomerAddr := fabricatedMemberAddr(t)
	newcomer := ring.Node{ID: newcomerAddr, Addr: newcomerAddr}
	members := tc.nodes[0].server.core.cluster.Members()
	before := ringOf(t, members, 3)
	after := ringOf(t, append(append([]ring.Node{}, members...), newcomer), 3)
	sets := func(name string) (was, now []ring.Node) {
		key := placementOf(testDatabase, name)
		was, err := before.Replicas(key.encoded, 3)
		if err != nil {
			t.Fatal(err)
		}
		now, err = after.Replicas(key.encoded, 3)
		if err != nil {
			t.Fatal(err)
		}
		return was, now
	}
	for owner := range tc.nodes {
		for replica := range tc.nodes {
			if replica == owner {
				continue
			}
			sc := cededWriteScenario{owner: owner, replica: replica, newcomer: newcomer}
			for attempt := 0; attempt < 3000 && (sc.moved == "" || sc.kept == ""); attempt++ {
				name := fmt.Sprintf("ceded%04d", attempt)
				was, now := sets(name)
				if was[0].ID != tc.addrs[owner] || !nodeListed(was, tc.addrs[replica]) || !nodeListed(now, tc.addrs[replica]) {
					continue
				}
				switch {
				case sc.moved == "" && now[0].ID == newcomer.ID:
					sc.moved = name
				case sc.kept == "" && now[0].ID == tc.addrs[owner]:
					sc.kept = name
				}
			}
			if sc.moved != "" && sc.kept != "" {
				return sc
			}
		}
	}
	t.Fatal("test setup: no owner has a bucket the join moves to the newcomer and one it keeps, with one replica kept in both sets")
	return cededWriteScenario{}
}

// TestAWriteACededOwnerLogsAfterItsStreamReopenedIsDelivered is BUG-0528's
// failing-first test. A write decides it owns its bucket, a join that moves
// the bucket is adopted while the write is held inside its fence, and the
// owner's stream to a replica that stays in the set reopens and takes its
// boundary. The write is then logged above that boundary in the owner's term
// and applied, and finds the owner ceded. The sender's rule addresses it to
// the replica (a ceded owner finishes delivering its term, spec §20.8.3), so
// the replica must receive it: the stream must not pass over it, and a
// retention pass's advance notice must not acknowledge it undelivered. The
// newcomer never starts and auto_rebalance is off, so nothing copies the
// bucket: the replica can only hold the write if the stream delivered it.
func TestAWriteACededOwnerLogsAfterItsStreamReopenedIsDelivered(t *testing.T) {
	runCededWrite(t, false)
}

// TestALaterWriteDoesNotCarryTheStreamPastACededWrite is the same scene with
// a later write that overtakes the ceded one (BUG-0528, the verifier's F1).
// Once the ceded write has released its ship ticket's hold on the replica, a
// write to a bucket the owner keeps, at concern all, is queued for the
// replica and acknowledged. Its acknowledgement raises the replica's position
// in the owner's stream above the ceded write's sequence, so a reopen made
// after it replays from above the ceded write and never delivers it. The
// ceded write must be queued while its ticket still holds every later
// write's turn.
func TestALaterWriteDoesNotCarryTheStreamPastACededWrite(t *testing.T) {
	runCededWrite(t, true)
}

// runCededWrite runs the scene of both tests above; overtake writes the kept
// bucket while the ceded write is held after it released its ticket's hold.
func runCededWrite(t *testing.T, overtake bool) {
	t.Helper()
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	sc := findCededWriteScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range []string{sc.moved, sc.kept} {
		tc.ok(sc.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		tc.ok(sc.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "before",
			Data: map[string]interface{}{"n": 0}, WriteConcern: "all"})
	}

	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	replicaCluster := tc.nodes[sc.replica].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[sc.replica]))

	// Hold the next write to the moved bucket on its owner inside its fence,
	// after it has read its ownership and before it is logged.
	reached := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != ownerCluster.self.ID || op != transport.OpPut || bucket != sc.moved {
			return
		}
		held := false
		holdOnce.Do(func() { held = true })
		if held {
			close(reached)
			<-release
		}
	}
	afterOwnerViewHook.Store(&hold)
	defer afterOwnerViewHook.Store(nil)
	defer releaseOnce.Do(func() { close(release) })

	if overtake {
		// Once the ceded write has released its hold on every peer, a write
		// to the kept bucket is made and acknowledged by every copy before
		// the ceded write goes on.
		overtaken := func(nodeID, bucket string) {
			if nodeID != ownerCluster.self.ID || bucket != sc.moved {
				return
			}
			tc.ok(sc.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: sc.kept, Key: "overtaking",
				Data: map[string]interface{}{"n": 2}, WriteConcern: "all"})
		}
		afterMovedRouteHook.Store(&overtaken)
		defer afterMovedRouteHook.Store(nil)
	}

	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: sc.moved, Key: "in-flight",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	if err != nil {
		t.Fatalf("encoding the write: %v", err)
	}
	answered := make(chan struct{})
	client := tc.dial(sc.owner)
	defer func() { _ = client.Close() }()
	go func() {
		defer close(answered)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// Its answer is not asserted: the owner ceded the bucket while the
		// write was in flight, so the client is told the outcome is unknown.
		_, _ = client.SendRequest(ctx, transport.OpPut, payload)
	}()
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the write never reached its owner's fence")
	}

	generationBefore := toReplica.currentGeneration()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, sc.newcomer); err != nil {
		t.Fatalf("join %s: %v", sc.newcomer.ID, err)
	}
	pollUntil(t, "the replica adopts the join", func() bool { return replicaCluster.Epoch() == ownerCluster.Epoch() })
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by the newcomer", sc.moved, owner, err)
	}
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.kept)); err != nil || owner.ID != ownerCluster.self.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by its owner", sc.kept, owner, err)
	}
	// The owner's stream to the replica reopens after the adoption and takes
	// its boundary below the held write.
	pollUntil(t, "the owner's stream to the replica is streaming again after the join", func() bool {
		return toReplica.inGenerationAfter(generationBefore) && toReplica.caughtUp()
	})

	releaseOnce.Do(func() { close(release) })
	select {
	case <-answered:
	case <-time.After(30 * time.Second):
		t.Fatal("the held write was never answered")
	}
	// Retention passes ask the stream for advance notices: none may move the
	// replica's position past the write without delivering it.
	for range 5 {
		if err := ownerCluster.pruneWAL(); err != nil {
			t.Logf("a retention pass on the owner: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if overtake && !holdsKeyLocallyIn(tc.nodes[sc.replica], sc.kept, "overtaking") {
		t.Fatalf("setup: the replica does not hold the write acknowledged at concern all to %s", sc.kept)
	}
	if !eventually(func() bool { return holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, "in-flight") }) {
		t.Fatalf("the replica %s, kept in %s's set by the join, never received the write its ceded owner %s logged in its term "+
			"after the stream reopened; the stream acknowledges %d of the owner's %d logged entries",
			tc.addrs[sc.replica], sc.moved, tc.addrs[sc.owner], toReplica.ackedThrough(ownerCluster.self.ID), ownerCluster.s.wal.LastSequence())
	}
}

// TestAPopWhoseOwnershipMovesMidWriteDoesNotLoseItsEntry is BUG-0548's path
// 3. A pop decides it owns its bucket, a join that moves the bucket to a
// member that never starts is adopted while the pop is held inside its fence,
// and the pop is then logged and applied: the owner has removed the entry and
// queues the removal for the new set as a ceded entry (BUG-0528). The entry
// must be in the caller's hands. Before the fix the owner answered UNAVAILABLE
// (ownership moved) and dropped the answer that held the entry. The window is
// reached as runCededWrite reaches it: afterMovedRouteHook, which the bug file
// names, runs only once the branch has been taken, so it cannot open it.
func TestAPopWhoseOwnershipMovesMidWriteDoesNotLoseItsEntry(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	sc := findCededWriteScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	target := popTarget{kind: "fifo pop", bucket: sc.moved, op: transport.OpFIFOPop}
	createPopTarget(t, tc, sc.owner, target)
	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	replicaCluster := tc.nodes[sc.replica].server.core.cluster
	ownerCore := tc.nodes[sc.owner].server.core
	head := headOn(t, ownerCore, target)
	if head == "" {
		t.Fatal("test setup: the moved bucket has nothing to pop")
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != ownerCluster.self.ID || op != transport.OpFIFOPop || bucket != sc.moved {
			return
		}
		held := false
		holdOnce.Do(func() { held = true })
		if held {
			close(reached)
			<-release
		}
	}
	afterOwnerViewHook.Store(&hold)
	defer afterOwnerViewHook.Store(nil)
	defer releaseOnce.Do(func() { close(release) })

	payload, err := bson.Marshal(target.popRequest("one"))
	if err != nil {
		t.Fatalf("encoding the pop: %v", err)
	}
	type popAnswer struct {
		frame *transport.Frame
		err   error
	}
	answered := make(chan popAnswer, 1)
	client := tc.dial(sc.owner)
	defer func() { _ = client.Close() }()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frame, err := client.SendRequest(ctx, transport.OpFIFOPop, payload)
		answered <- popAnswer{frame: frame, err: err}
	}()
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the pop never reached its owner's fence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, sc.newcomer); err != nil {
		t.Fatalf("join %s: %v", sc.newcomer.ID, err)
	}
	pollUntil(t, "the replica adopts the join", func() bool { return replicaCluster.Epoch() == ownerCluster.Epoch() })
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("setup: %s is owned by %v (%v) after the join, not by the newcomer", sc.moved, owner, err)
	}

	releaseOnce.Do(func() { close(release) })
	var answer popAnswer
	select {
	case answer = <-answered:
	case <-time.After(30 * time.Second):
		t.Fatal("the held pop was never answered")
	}
	if answer.err != nil {
		t.Fatalf("the held pop's answer did not arrive: %v", answer.err)
	}
	requirePopKeptItsEntry(t, "a fifo pop whose bucket moved while it was in flight", answer.frame, ownerCore, target, head)
	if poppedMarker(answer.frame) == head {
		// Only this node holds the removal when it is answered; the factor is
		// still the bucket's.
		confirmed, factor, _, _ := copiesConfirmedOf(answer.frame.Payload)
		outcome, _ := bson.Raw(answer.frame.Payload).Lookup("write_outcome").StringValueOK()
		if confirmed != 1 || factor != 3 || outcome != outcomeApplied {
			t.Errorf("the pop whose bucket moved was answered copies_confirmed %d, replication_factor %d, write_outcome %q; want 1, 3 and %q",
				confirmed, factor, outcome, outcomeApplied)
		}
	}
}

// inGenerationAfter reports whether the stream has changed state since
// generation.
func (p *peer) inGenerationAfter(generation uint64) bool {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.generation > generation
}

// TestACededOwnerDoesNotPassARefusedReplica is BUG-0604, the ceded twin of
// BUG-0313: a write the owner logged in its term and finished after its bucket
// moved is refused to a replica the owner knows as plaintext. The member's
// stream must stall at the entry, so a retention pass does not move its cursor
// past it, and the member must receive it by replay once it says it is sealed.
func TestACededOwnerDoesNotPassARefusedReplica(t *testing.T) {
	tc := newMixedCluster(t, 3, "on", "on", "on")
	defer tc.stop()
	sc := findCededWriteScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range []string{sc.moved, sc.kept} {
		tc.ok(sc.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		tc.ok(sc.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "before",
			Data: markerDocument(), WriteConcern: "all"})
	}
	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	replicaCluster := tc.nodes[sc.replica].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[sc.replica]))

	reached := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID != ownerCluster.self.ID || op != transport.OpPut || bucket != sc.moved {
			return
		}
		held := false
		holdOnce.Do(func() { held = true })
		if held {
			close(reached)
			<-release
		}
	}
	afterOwnerViewHook.Store(&hold)
	defer afterOwnerViewHook.Store(nil)
	defer releaseOnce.Do(func() { close(release) })

	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: sc.moved, Key: "in-flight", Data: markerDocument(), WriteConcern: "one"})
	if err != nil {
		t.Fatal(err)
	}
	answered := make(chan struct{})
	client := tc.dial(sc.owner)
	defer func() { _ = client.Close() }()
	go func() {
		defer close(answered)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// The ceded write is answered unknown with the ownership-moved
		// error; the test reads the copies, not the answer.
		_, _ = client.SendRequest(ctx, transport.OpPut, payload)
	}()
	<-reached
	generationBefore := toReplica.currentGeneration()
	// A sealed cluster refuses to join a member that never starts, so the
	// join is adopted directly on every node.
	epoch := ownerCluster.Epoch() + 1
	members := append(append([]ring.Node{}, ownerCluster.Members()...), sc.newcomer)
	for _, n := range tc.nodes {
		if err := n.server.core.cluster.adoptMembership(epoch, members); err != nil {
			t.Fatalf("adopting the join: %v", err)
		}
	}
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("setup: %s is owned by %v (%v), not the newcomer", sc.moved, owner, err)
	}
	pollUntil(t, "replica adopts", func() bool { return replicaCluster.Epoch() == ownerCluster.Epoch() })
	pollUntil(t, "stream again", func() bool { return toReplica.inGenerationAfter(generationBefore) && toReplica.caughtUp() })

	// The owner learns its replica is plaintext before the ceded write is queued.
	ownerCluster.sealedPeers.record(tc.addrs[sc.replica], claimPlaintext)
	releaseOnce.Do(func() { close(release) })
	<-answered
	ceded := ownerCluster.s.wal.LastSequence()
	for range 5 {
		if err := ownerCluster.pruneWAL(); err != nil {
			t.Fatalf("retention pass: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cursor := toReplica.ackedThrough(ownerCluster.self.ID)
	t.Logf("ceded seq %d, owner's cursor for the replica %d, replica holds it: %v", ceded, cursor, holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, "in-flight"))
	if cursor >= ceded && !holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, "in-flight") {
		t.Errorf("while refused: the owner's cursor for %s is %d, past the ceded entry %d it never received", tc.addrs[sc.replica], cursor, ceded)
	}
	ownerCluster.sealedPeers.record(tc.addrs[sc.replica], claimSealed)
	ownerCluster.wakeSenderOf(tc.addrs[sc.replica])
	if !eventually(func() bool { return holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, "in-flight") }) {
		t.Errorf("after re-seal: the replica never received the ceded entry %d; cursor %d", ceded, toReplica.ackedThrough(ownerCluster.self.ID))
	}
}
