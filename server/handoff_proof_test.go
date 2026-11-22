package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// T-133 (spec §20.16): the proof of §20.6's invariant as a test. T-106's
// harness — eight writers at one, majority and all, a FIFO bucket and a hash
// bucket, retention passes every 50 ms over 256-byte log segments, checkpoints
// that freeze delivery and read each node's own store with multiplicity — run
// through four owner changes on four founders at factor 3. Unlike T-106, each
// change lands while the writers write, so entries are in flight across it.

// proofAck is one write a client was told succeeded: how many copies the
// answer said hold it, and which node owned the bucket on the coordinator's
// ring both before the write was sent and when it was answered.
type proofAck struct {
	bucket, id string
	queue      bool
	confirmed  int32
	ownerAtAck string
	// ownerAtSend is the coordinator ring's owner when the write was sent
	// (ownerAtAck is "" when that owner changed before the answer, so which
	// node answered is unknown).
	ownerAtSend string
	// forwardedTo is the node the coordinator actually forwarded the write
	// to (BUG-0547); "" when the coordinator answered it as the owner.
	forwardedTo string
}

// mayBeLostAtOne reports whether an acknowledgement is one spec §20.6 lets
// a removed owner take with it: a write answered copies_confirmed 1 whose
// answering owner is a node removed while stopped. heldByRunningNode is
// whether any running node holds the write.
//
// The spec's row is keyed on copies_confirmed 1, "acknowledged at one, or
// copies_confirmed: 1", so a majority or all write answered 1 is covered:
// the proof's majority writers are answered 1 in the killed-owner phase,
// and a write answered 2 or more is not. The exemption needs positive
// evidence of the answerer: the ring's owner when that did not change in
// flight; when it did, the node the coordinator actually forwarded the
// write to, and only if no running node holds the write (a new owner that
// had answered would hold it). The sender's owner is not evidence: after an
// owner change the coordinator may have forwarded to the new owner, which
// is why BUG-0547 added the forward target.
func mayBeLostAtOne(ack proofAck, removedStopped map[string]bool, heldByRunningNode bool) bool {
	if ack.confirmed != 1 {
		return false
	}
	answerer := ack.ownerAtAck
	if answerer == "" && !heldByRunningNode {
		answerer = ack.forwardedTo
	}
	return answerer != "" && removedStopped[answerer]
}

// ownerChangeProof is T-133's cluster, writers and bookkeeping.
type ownerChangeProof struct {
	t            *testing.T
	tc           *testCluster
	coordinator  int
	queue, keyed string
	// nodesMu guards nodes and tc.nodes against the retention passes.
	nodesMu sync.Mutex
	// nodes is every node the run started, founders and joiners, by ID; a
	// stopped node's server is nil.
	nodes map[string]*testNode
	// removedStopped are nodes removed while they were stopped: a write
	// answered copies_confirmed 1 whose owner was one of them may be lost
	// (spec §20.6's second row).
	removedStopped map[string]bool
	pause          sync.RWMutex
	ackMu          sync.Mutex
	acks           []proofAck
	// forwardMu guards forwardTargets: per writer id, the node the
	// coordinator actually forwarded the write to (BUG-0547).
	forwardMu      sync.Mutex
	forwardTargets map[string]string
	// tried is every hash key a writer sent, answered or not: copies of the
	// owner's term must agree on each.
	tried    []string
	writes   atomic.Int64
	gate     *shipGate
	stop     chan struct{}
	done     sync.WaitGroup
	refusals atomic.Int64
}

func (p *ownerChangeProof) ownerOf(c *Cluster, bucket string) string {
	owner, err := c.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		return ""
	}
	return owner.ID
}

// recordForwardTarget notes the node the coordinator actually forwarded a
// write to, keyed by writer id (BUG-0547): the answer does not name it, and
// the ring's owner read before and after the call can disagree with it.
func (p *ownerChangeProof) recordForwardTarget(entry ReplEntry, targetID string) {
	id, ok := writerIDOf(transport.Opcode(entry.Op), entry.Payload)
	if !ok {
		return
	}
	p.forwardMu.Lock()
	p.forwardTargets[id] = targetID
	p.forwardMu.Unlock()
}

// forwardTarget is the node recorded for id, or "" when the coordinator
// answered the write itself or nothing was recorded.
func (p *ownerChangeProof) forwardTarget(id string) string {
	p.forwardMu.Lock()
	defer p.forwardMu.Unlock()
	return p.forwardTargets[id]
}

// writerIDOf is the writer's id in a forwarded entry's payload: an insert's
// key, or a queue push's data._id_. It reports false for any other op.
func writerIDOf(op transport.Opcode, payload []byte) (string, bool) {
	switch op {
	case transport.OpPut:
		id, ok := bson.Raw(payload).Lookup("key").StringValueOK()
		return id, ok
	case transport.OpFIFOPush:
		data, ok := bson.Raw(payload).Lookup("data").DocumentOK()
		if !ok {
			return "", false
		}
		id, ok := data.Lookup("_id_").StringValueOK()
		return id, ok
	}
	return "", false
}

// writer writes through the coordinator until the run stops: every write its
// own id, alternating the two buckets, at the writer's concern.
func (p *ownerChangeProof) writer(w int) {
	defer p.done.Done()
	client := p.tc.dial(p.coordinator)
	defer func() { _ = client.Close() }()
	coordinator := clusterOf(p.tc, p.coordinator)
	concern := []string{"one", "majority", "all"}[w%3]
	for i := 0; ; i++ {
		select {
		case <-p.stop:
			return
		default:
		}
		id := fmt.Sprintf("w%d-%06d", w, i)
		queue := i%2 == 0
		bucket, op := p.keyed, transport.OpPut
		var request any = InsertRequest{Database: testDatabase, Bucket: p.keyed, Key: id, Data: map[string]interface{}{"n": i}, WriteConcern: concern}
		if queue {
			bucket, op = p.queue, transport.OpFIFOPush
			request = PushRequest{Database: testDatabase, Bucket: p.queue, Data: map[string]interface{}{"_id_": id}, WriteConcern: concern}
		}
		payload, err := bson.Marshal(request)
		if err != nil {
			p.t.Errorf("encoding %s: %v", id, err)
			return
		}
		p.pause.RLock()
		ownerBefore := p.ownerOf(coordinator, bucket)
		if !queue {
			p.ackMu.Lock()
			p.tried = append(p.tried, id)
			p.ackMu.Unlock()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		frame, err := client.SendRequest(ctx, op, payload)
		cancel()
		if err == nil && frame.Opcode != transport.OpError {
			confirmed, _, _, _ := copiesConfirmedOf(frame.Payload)
			ownerAtAck := p.ownerOf(coordinator, bucket)
			if ownerAtAck != ownerBefore {
				// The owner changed while the write was in flight: the ring reads
				// cannot say which node answered, so the recorded forward target is
				// the only evidence of it (BUG-0547).
				ownerAtAck = ""
			}
			forwardedTo := p.forwardTarget(id)
			p.ackMu.Lock()
			p.acks = append(p.acks, proofAck{bucket: bucket, id: id, queue: queue, confirmed: confirmed, ownerAtAck: ownerAtAck, ownerAtSend: ownerBefore, forwardedTo: forwardedTo})
			p.ackMu.Unlock()
		} else {
			p.refusals.Add(1)
		}
		p.writes.Add(1)
		p.pause.RUnlock()
	}
}

// retentionPasses runs a retention pass on every running node every 50 ms.
func (p *ownerChangeProof) retentionPasses() {
	defer p.done.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}
		p.nodesMu.Lock()
		for _, node := range p.nodes {
			if node.server != nil {
				// A pass that cannot run this tick runs at the next; the test
				// asserts on what the copies hold, not on the pass.
				if err := node.server.core.cluster.pruneWAL(); err != nil {
					p.t.Logf("a retention pass on %s: %v", node.addr, err)
				}
			}
		}
		p.nodesMu.Unlock()
	}
}

// waitWrites waits until n more writes have been answered.
func (p *ownerChangeProof) waitWrites(n int64, within time.Duration) {
	p.t.Helper()
	target := p.writes.Load() + n
	deadline := time.Now().Add(within)
	for p.writes.Load() < target {
		if time.Now().After(deadline) {
			p.t.Fatalf("only %d of %d writes were answered in %v", p.writes.Load()-(target-n), n, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// members is every running current member, by ID, as the coordinator's
// membership names them.
func (p *ownerChangeProof) members() map[string]*Server {
	members := make(map[string]*Server)
	for _, member := range clusterOf(p.tc, p.coordinator).Members() {
		if node := p.nodes[member.ID]; node != nil && node.server != nil {
			members[member.ID] = node.server.core
		}
	}
	return members
}

// queueOrder is a FIFO bucket's element ids in one node's own store, in the
// order the node serves them; nil when the node holds no copy.
func queueOrder(node *testNode, bucket string) ([]string, error) {
	if !node.server.core.bucketStoreOpen(testDatabase, bucket) {
		return nil, nil
	}
	held, snap, err := node.server.core.resolveForRead(testDatabase, bucket)
	if err != nil {
		return nil, fmt.Errorf("resolving %s on %s: %w", bucket, node.addr, err)
	}
	defer snap.Discard()
	var ids []string
	walkErr := held.queueStore.Walk(func(entry []byte) error {
		id, ok := bson.Raw(entry).Lookup("_id_").StringValueOK()
		if !ok {
			return fmt.Errorf("an element of %s on %s has no _id_", bucket, node.addr)
		}
		ids = append(ids, id)
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walking %s on %s: %w", bucket, node.addr, walkErr)
	}
	return ids, nil
}

// checkpoint pauses the writers, waits a bounded time for the hand-off to
// settle and for every stream to catch up, holds every sender before its next
// ship, and asserts the four properties over every write acknowledged so far.
// The membership changes run between checkpoints, with the writers writing.
func (p *ownerChangeProof) checkpoint(phase string) {
	p.t.Helper()
	p.pause.Lock()
	defer p.pause.Unlock()
	p.nodesMu.Lock()
	members := p.members()
	buckets := []string{p.queue, p.keyed}
	settled := holdsWithin(90*time.Second, func() bool { return handOffUnsettled(members, testDatabase, buckets) == "" })
	if !settled {
		// Asserted, not filtered on: a copy that never settles fails the proof.
		p.t.Errorf("%s: the hand-off did not settle within 90 s: %s", phase, handOffUnsettled(members, testDatabase, buckets))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	for _, s := range members {
		for _, member := range s.cluster.otherMembers() {
			if err := catchUpPeer(ctx, s.cluster.peerFor(member)); err != nil {
				p.t.Errorf("%s: the stream from %s to %s did not catch up: %v", phase, s.cluster.self.ID, member.ID, err)
			}
		}
	}
	cancel()
	p.gate.close()
	p.verify(phase, members)
	p.gate.open()
	p.nodesMu.Unlock()
}

// underLoad runs a membership change while the writers keep writing: what a
// ceded owner had not delivered is in flight when the change lands.
func (p *ownerChangeProof) underLoad(change func()) {
	p.nodesMu.Lock()
	defer p.nodesMu.Unlock()
	change()
}

// proofCopies is what each running node holds of the two buckets.
type proofCopies struct {
	queues map[string][]string
	counts map[string]map[string]int
}

func (p *ownerChangeProof) readCopies(phase string) proofCopies {
	p.t.Helper()
	copies := proofCopies{queues: make(map[string][]string), counts: make(map[string]map[string]int)}
	for id, node := range p.nodes {
		if node.server == nil {
			continue
		}
		order, err := queueOrder(node, p.queue)
		if err != nil {
			p.t.Fatalf("%s: %v", phase, err)
		}
		copies.queues[id] = order
		counts := make(map[string]int, len(order))
		for _, element := range order {
			counts[element]++
		}
		copies.counts[id] = counts
	}
	return copies
}

func (p *ownerChangeProof) holds(copies proofCopies, id string, ack proofAck) bool {
	if ack.queue {
		return copies.counts[id][ack.id] > 0
	}
	node := p.nodes[id]
	return node.server != nil && holdsKeyLocallyIn(node, ack.bucket, ack.id)
}

// verify asserts, over every write acknowledged so far: (1) a write answered
// copies_confirmed N is on at least N running nodes, unless it was answered 1
// by an owner removed while stopped; (2) a write answered 2 or more is on the
// bucket's owner, and one answered 1 is too unless the node that answered it
// was removed while stopped; (3) no node holds a queue element twice; (4)
// every copy that follows its owner's term equals the owner's.
func (p *ownerChangeProof) verify(phase string, members map[string]*Server) {
	p.t.Helper()
	copies := p.readCopies(phase)
	for id, counts := range copies.counts {
		for element, n := range counts {
			if n > 1 {
				p.t.Errorf("%s: %s holds queue element %s %d times", phase, id, element, n)
			}
		}
	}
	placer := clusterOf(p.tc, p.coordinator)
	owners := map[string]string{p.queue: p.ownerOf(placer, p.queue), p.keyed: p.ownerOf(placer, p.keyed)}
	p.ackMu.Lock()
	acks := slices.Clone(p.acks)
	tried := slices.Clone(p.tried)
	p.ackMu.Unlock()
	short, offOwner := 0, 0
	byConfirmed := make(map[int32]int)
	for _, ack := range acks {
		byConfirmed[ack.confirmed]++
		if ack.confirmed < 1 {
			p.t.Fatalf("%s: %s in %s was acknowledged without copies_confirmed", phase, ack.id, ack.bucket)
		}
		holders := 0
		for id := range copies.queues {
			if p.holds(copies, id, ack) {
				holders++
			}
		}
		mayBeLost := mayBeLostAtOne(ack, p.removedStopped, holders > 0)
		if int32(holders) < ack.confirmed && !mayBeLost {
			if short < 10 {
				p.t.Errorf("%s: %s in %s was answered copies_confirmed %d (owner %q) and %d running nodes hold it", phase, ack.id, ack.bucket, ack.confirmed, ack.ownerAtAck, holders)
			}
			short++
		}
		if !mayBeLost && !p.holds(copies, owners[ack.bucket], ack) {
			if offOwner < 10 {
				p.t.Errorf("%s: %s in %s was answered copies_confirmed %d (owner %q then) and is not on the bucket's owner %s",
					phase, ack.id, ack.bucket, ack.confirmed, ack.ownerAtAck, owners[ack.bucket])
			}
			offOwner++
		}
	}
	for _, bucket := range []string{p.queue, p.keyed} {
		set, err := placer.Replicas(placementOf(testDatabase, bucket))
		if err != nil {
			p.t.Fatalf("%s: placing %s: %v", phase, bucket, err)
		}
		owner := set[0].ID
		ownerTerm, _, ownerErr := members[owner].bucketLineage(testDatabase, bucket)
		for _, member := range set[1:] {
			term, _, err := members[member.ID].bucketLineage(testDatabase, bucket)
			if ownerErr != nil || err != nil || term != ownerTerm {
				continue
			}
			p.compareWithOwner(phase, bucket, owner, member.ID, copies, tried)
		}
	}
	p.t.Logf("%s: %d writes acknowledged (by copies_confirmed %v), %d refused or failed; %d short, %d not on the owner; %d running nodes",
		phase, len(acks), byConfirmed, p.refusals.Load(), short, offOwner, len(copies.queues))
	if short > 0 || offOwner > 0 {
		p.t.Errorf("%s: %d acknowledged writes are on fewer nodes than they were told and %d are not on the bucket's owner", phase, short, offOwner)
	}
}

// compareWithOwner asserts that a copy in its owner's term equals the
// owner's: the same queue elements in the same order, and the same answer for
// every hash key a writer sent.
func (p *ownerChangeProof) compareWithOwner(phase, bucket, owner, member string, copies proofCopies, tried []string) {
	p.t.Helper()
	if bucket == p.queue {
		if !slices.Equal(copies.queues[owner], copies.queues[member]) {
			p.t.Errorf("%s: %s's copy of %s (%d elements) is not its owner %s's (%d elements), though both are in one term",
				phase, member, bucket, len(copies.queues[member]), owner, len(copies.queues[owner]))
		}
		return
	}
	differ := 0
	for _, key := range tried {
		if holdsKeyLocallyIn(p.nodes[owner], bucket, key) != holdsKeyLocallyIn(p.nodes[member], bucket, key) {
			differ++
		}
	}
	if differ > 0 {
		p.t.Errorf("%s: %s's copy of %s differs from its owner %s's on %d keys, though both are in one term", phase, member, bucket, owner, differ)
	}
}

// copiesMade is how many whole copies each running founder has made, take-
// overs and copies afresh alike, as its hoarddb_handoff_copies_total counter
// serves it.
func (p *ownerChangeProof) copiesMade() map[string]int64 {
	copies := make(map[string]int64, len(p.tc.addrs))
	for _, addr := range p.tc.addrs {
		if node := p.nodes[addr]; node.server != nil {
			copies[addr] = int64(replicationGauge(p.t, node.server.core, "hoarddb_handoff_copies_total"))
		}
	}
	return copies
}

// assertNoneCopied asserts that no founder whose copies stayed in both
// buckets' sets made a whole copy since before: while every previous owner
// stays reachable, a ceded owner finishes delivering its term (spec §20.8.3),
// so a node that held the bucket throughout follows the new owner without
// being copied (T-125's criterion). A founder the join left out of a set is
// marked outside, and a copy that returns from outside is copied again by
// design (spec §20.8.6), so it is not asserted on.
func (p *ownerChangeProof) assertNoneCopied(phase string, before map[string]int64, leftOut map[string]bool) {
	p.t.Helper()
	for addr, copies := range p.copiesMade() {
		if !leftOut[addr] && copies != before[addr] {
			p.t.Errorf("%s: %s, whose copies stayed in both buckets' sets, made %d whole copies, though every previous owner stayed reachable to finish delivering its term",
				phase, addr, copies-before[addr])
		}
	}
}

// leftOutByJoin is the founders a join of joinerID leaves out of either
// bucket's set.
func leftOutByJoin(t *testing.T, founders []string, joinerID string, buckets ...string) map[string]bool {
	t.Helper()
	joined := ringWith(t, 3, append(slices.Clone(founders), joinerID)...)
	leftOut := make(map[string]bool)
	for _, bucket := range buckets {
		set, err := joined.Replicas(placementOf(testDatabase, bucket).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", bucket, err)
		}
		for _, founder := range founders {
			if !nodeListed(set, founder) {
				leftOut[founder] = true
			}
		}
	}
	return leftOut
}

// joinNode proposes ports' node through the coordinator, starts it with
// auto_rebalance on, and brings every running node to the new membership.
func (p *ownerChangeProof) joinNode(ports *testNodePorts) *testNode {
	p.t.Helper()
	via := clusterOf(p.tc, p.coordinator)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeJoin(ctx, ports.ringNode()); err != nil {
		p.t.Fatalf("proposing the join of %s: %v", ports.ringAddr, err)
	}
	joiner := startJoinTarget(p.t, p.tc, ports, p.tc.token)
	smallSegments(joiner)
	p.nodes[joiner.addr] = joiner
	p.bringEveryNodeTo(via)
	return joiner
}

// removeNode removes id through the coordinator and brings every running
// node, the removed one included, to the new membership.
func (p *ownerChangeProof) removeNode(id string) {
	p.t.Helper()
	via := clusterOf(p.tc, p.coordinator)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeRemoval(ctx, id); err != nil {
		p.t.Fatalf("removing %s: %v", id, err)
	}
	p.bringEveryNodeTo(via)
}

func (p *ownerChangeProof) bringEveryNodeTo(via *Cluster) {
	p.t.Helper()
	for _, node := range p.nodes {
		if node.server == nil || node.server.core.cluster == via {
			continue
		}
		member := node.server.core.cluster
		answer := GossipResponse{Nodes: via.memberNodes(), Members: via.memberIDs(), Epoch: via.Epoch(), History: via.historyAbove(member.Epoch())}
		if err := member.reconcileAnswer(via.self.ID, answer); err != nil {
			p.t.Fatalf("bringing %s to %s's membership: %v", member.self.ID, via.self.ID, err)
		}
	}
}

// proofLayout is T-133's choice of names and roles on the founders' ring.
type proofLayout struct {
	queue, keyed string
	owner        int // owns both buckets on the founders' ring
	coordinator  int // writes go through it; never removed
	delayed      int // the replica whose stream from the owner is held before the owner is killed
}

// chooseProofLayout picks two names that a join of first moves to it from one
// founder, and a coordinator and a delayed replica among the others in both
// buckets' sets.
func chooseProofLayout(t *testing.T, tc *testCluster, first *testNodePorts) proofLayout {
	t.Helper()
	founders := ringWith(t, 3, tc.addrs...)
	joined := ringWith(t, 3, append(slices.Clone(tc.addrs), first.ringAddr)...)
	var layout proofLayout
	owner := ""
	movesToTheJoiner := func(name string) bool {
		was := ownerOn(founders, name)
		if ownerOn(joined, name) != first.ringAddr || (owner != "" && was != owner) {
			return false
		}
		owner = was
		return true
	}
	layout.queue = bucketNamed(t, "proofq", movesToTheJoiner)
	layout.keyed = bucketNamed(t, "proofh", movesToTheJoiner)
	layout.owner = nodeIndexByAddr(t, tc.addrs, owner)
	inBoth := func(index int) bool {
		for _, name := range []string{layout.queue, layout.keyed} {
			set, err := founders.Replicas(placementOf(testDatabase, name).encoded, 3)
			if err != nil || !nodeListed(set, tc.addrs[index]) {
				return false
			}
		}
		return true
	}
	var others []int
	for index := range tc.addrs {
		if index != layout.owner && inBoth(index) {
			others = append(others, index)
		}
	}
	if len(others) < 2 {
		t.Fatalf("setup: only %d founders besides the owner are in both buckets' sets", len(others))
	}
	layout.coordinator, layout.delayed = others[0], others[1]
	return layout
}

// portsDroppingAMember reserves ports for a node whose join puts it in both
// buckets' sets on the ring of members, which drops a member from each.
func portsDroppingAMember(t *testing.T, members []string, buckets ...string) *testNodePorts {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		ports := reserveNodePorts(t)
		joined := ringWith(t, 3, append(slices.Clone(members), ports.ringAddr)...)
		inEvery := true
		for _, bucket := range buckets {
			set, err := joined.Replicas(placementOf(testDatabase, bucket).encoded, 3)
			inEvery = inEvery && err == nil && nodeListed(set, ports.ringAddr)
		}
		if inEvery {
			return ports
		}
		_ = ports.internode.Close()
		_ = ports.driver.Close()
	}
	t.Fatal("setup: no reserved port put a joiner in both buckets' sets in 50 attempts")
	return nil
}

// TestNoAcknowledgedWriteIsLostOrAppliedTwiceAcrossOwnerChanges is T-133, the
// proof as a test (spec §20.16, with rev 7's A-10.10 assertions). Four founders
// at factor 3, a FIFO bucket and a hash bucket owned by one founder, eight
// writers at one, majority and all through a founder that is never removed,
// retention passes every 50 ms. A checkpoint follows each phase: a join that
// moves both buckets to the joiner; the removal of that new owner while it
// runs; the removal of the owner after it was killed with one replica's stream
// held; a node dropped from both sets by a join and returned by the joiner's
// removal before its copy was released. Guards, each broken by hand: serve
// before the take-over is complete; choose the source by member order; ship by
// present ownership instead of by term; let a copy marked outside apply;
// delete the source of a pull.
func TestNoAcknowledgedWriteIsLostOrAppliedTwiceAcrossOwnerChanges(t *testing.T) {
	tc := newTestClusterWith(t, 4, 3, withoutRetentionLoop)
	defer tc.stop()
	firstJoiner := reserveNodePorts(t)
	layout := chooseProofLayout(t, tc, firstJoiner)
	p := &ownerChangeProof{t: t, tc: tc, coordinator: layout.coordinator, queue: layout.queue, keyed: layout.keyed,
		nodes: make(map[string]*testNode), removedStopped: make(map[string]bool), forwardTargets: make(map[string]string), gate: newShipGate(), stop: make(chan struct{})}
	for index, node := range tc.nodes {
		p.nodes[tc.addrs[index]] = node
	}
	tc.ok(layout.coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(layout.coordinator, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: layout.queue, Type: BucketTypeFIFO})
	tc.ok(layout.coordinator, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: layout.keyed, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, layout.queue, 3)
	waitForServing(t, tc, testDatabase, layout.keyed, 3)
	for _, node := range tc.nodes {
		smallSegments(node)
	}

	hook := p.gate.before
	beforeShipHook.Store(&hook)
	defer beforeShipHook.Store(nil)
	forwardHook := p.recordForwardTarget
	forwardTargetHook.Store(&forwardHook)
	defer forwardTargetHook.Store(nil)
	defer askNodeToMigrateHook.Store(nil)
	const writers = 8
	for w := 0; w < writers; w++ {
		p.done.Add(1)
		go p.writer(w)
	}
	p.done.Add(1)
	go p.retentionPasses()
	stopped := false
	defer func() {
		if !stopped {
			close(p.stop)
			p.done.Wait()
		}
	}()

	p.waitWrites(300, 60*time.Second)
	p.checkpoint("steady")
	leftOut := leftOutByJoin(t, tc.addrs, firstJoiner.ringAddr, layout.queue, layout.keyed)
	copiesBefore := p.copiesMade()
	var joiner *testNode
	p.underLoad(func() { joiner = p.joinNode(firstJoiner) })

	// A join that moves both buckets to the joiner, with the writers writing.
	p.waitWrites(400, 120*time.Second)
	p.checkpoint("a join that moves both buckets")
	p.assertNoneCopied("a join that moves both buckets", copiesBefore, leftOut)
	copiesBefore = p.copiesMade()
	p.underLoad(func() { p.removeNode(joiner.addr) })

	// The removal of the new owner while it runs: the buckets go back to the
	// founder that owned them, and the removed joiner keeps its copy.
	p.waitWrites(400, 120*time.Second)
	p.checkpoint("the removal of the new owner while it runs")
	p.assertNoneCopied("the removal of the new owner while it runs", copiesBefore, leftOut)

	// The owner, its stream to one replica held, is killed and removed.
	ownerID := tc.addrs[layout.owner]
	releaseDelayed := holdStreamPushesOnly(peerOf(t, tc, layout.owner, layout.delayed))
	p.waitWrites(200, 120*time.Second)
	p.underLoad(func() {
		p.removedStopped[ownerID] = true
		tc.stopNodeKeepingData(layout.owner)
		releaseDelayed()
		p.removeNode(ownerID)
	})
	p.waitWrites(400, 120*time.Second)
	p.checkpoint("the removal of a killed owner with one replica delayed")

	// A join drops a node from each bucket's set, its release held; the
	// joiner's removal while it runs returns it.
	var members []string
	for _, member := range clusterOf(tc, layout.coordinator).Members() {
		members = append(members, member.ID)
	}
	secondJoiner := portsDroppingAMember(t, members, layout.queue, layout.keyed)
	dropped := holdReleasesOf(t, members, secondJoiner.ringAddr, layout.queue, layout.keyed)
	p.underLoad(func() { joiner = p.joinNode(secondJoiner) })
	p.waitWrites(400, 120*time.Second)
	p.underLoad(func() {
		for bucket, member := range dropped {
			if !p.nodes[member].server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Errorf("setup: %s, dropped from %s's set, no longer holds its copy", member, bucket)
			}
		}
		p.removeNode(joiner.addr)
		askNodeToMigrateHook.Store(nil)
	})
	p.waitWrites(400, 120*time.Second)
	p.checkpoint("a node dropped from the set and returned")

	stopped = true
	close(p.stop)
	p.done.Wait()
	t.Logf("%d writes, %d acknowledged, %d refused or failed", p.writes.Load(), len(p.acks), p.refusals.Load())
}

// holdReleasesOf holds, through askNodeToMigrateHook, the release of every
// copy a join of joinerID drops from the buckets' sets on the ring of members,
// and returns the dropped member of each bucket. The test removes the hook.
func holdReleasesOf(t *testing.T, members []string, joinerID string, buckets ...string) map[string]string {
	t.Helper()
	before := ringWith(t, 3, members...)
	after := ringWith(t, 3, append(slices.Clone(members), joinerID)...)
	dropped := make(map[string]string, len(buckets))
	for _, bucket := range buckets {
		key := placementOf(testDatabase, bucket).encoded
		was, _ := before.Replicas(key, 3)
		now, _ := after.Replicas(key, 3)
		for _, member := range was {
			if !nodeListed(now, member.ID) {
				dropped[bucket] = member.ID
			}
		}
		if dropped[bucket] == "" {
			t.Fatalf("setup: the join of %s drops no member from %s's set", joinerID, bucket)
		}
	}
	refuse := func(fromID, _, _, bucket string) error {
		if dropped[bucket] == fromID {
			return errors.New("test: the dropped member's release is held until it returns to the set")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&refuse)
	return dropped
}
