package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 7.1: `majority` and `all` are counted over the copies that are in sync
// (spec §19.3.11).

// confirmedWrite is one write's answer: its copy count, or the error it was
// refused with.
type confirmedWrite struct {
	confirmed int32
	refusal   string
	took      time.Duration
}

// writeConfirmed writes key to the test bucket through client at concern and
// reads the copy count from the answer.
func writeConfirmed(client *transport.DriverClient, key, concern string) (confirmedWrite, error) {
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
		Data: map[string]interface{}{"key": key}, WriteConcern: concern})
	if err != nil {
		return confirmedWrite{}, fmt.Errorf("encoding %s: %w", key, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	frame, err := client.SendRequest(ctx, transport.OpPut, payload)
	if err != nil {
		return confirmedWrite{}, fmt.Errorf("writing %s: %w", key, err)
	}
	answer := confirmedWrite{took: time.Since(started)}
	if frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
			return confirmedWrite{}, fmt.Errorf("decoding the refusal of %s: %w", key, err)
		}
		answer.refusal = refusal.Message
		return answer, nil
	}
	answer.confirmed, _, _, _ = copiesConfirmedOf(frame.Payload)
	return answer, nil
}

// ownerAndReplica creates the test hash bucket on three nodes at factor 3 and
// returns its owner's index and the index of one of its replicas.
func ownerAndReplica(t *testing.T, tc *testCluster) (int, int) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	return ownerIndex, (ownerIndex + 1) % len(tc.nodes)
}

// TestConcurrentWritersAtMajorityAreConfirmedByAReplica is T-103 (rev 3 review
// F-7). Sixteen writers, half at `majority` and half at `all`, never leave an
// idle instant, and the owner's link to one replica is slow. A replica whose
// stream is streaming is in sync under that load, so every `majority` answer
// is confirmed by at least one replica and every `all` answer by both.
func TestConcurrentWritersAtMajorityAreConfirmedByAReplica(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, replicaIndex := ownerAndReplica(t, tc)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex]))
	wrapPeerConn(toReplica, func(inner peerConn) peerConn { return &delayedConn{peerConn: inner, delay: 500 * time.Microsecond} })

	const writers, perWriter = 16, 60
	var wg sync.WaitGroup
	failures := make(chan string, writers*perWriter)
	for w := 0; w < writers; w++ {
		client := tc.dial(ownerIndex)
		defer func() { _ = client.Close() }()
		concern, least := "majority", int32(2)
		if w%2 == 1 {
			concern, least = "all", 3
		}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%02d-%03d", w, i)
				answer, err := writeConfirmed(client, key, concern)
				switch {
				case err != nil:
					failures <- err.Error()
				case answer.refusal != "":
					failures <- fmt.Sprintf("%s at %s was refused: %s", key, concern, answer.refusal)
				case answer.confirmed < least:
					failures <- fmt.Sprintf("%s at %s was answered copies_confirmed %d, want at least %d", key, concern, answer.confirmed, least)
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)
	count := 0
	for failure := range failures {
		if count < 10 {
			t.Error(failure)
		}
		count++
	}
	if count > 0 {
		t.Errorf("%d of %d writes were not confirmed by the copies their concern names", count, writers*perWriter)
	}
}

// TestAStoppedReplicaCostsOneFailedShipNotTheTimeout is T-104. A replica of
// the bucket is stopped. The first `all` write after it is answered as soon
// as the ship to the stopped node fails — copies_confirmed 2, far inside the
// write-concern timeout — and the writes after it do not wait at all.
// Guard: compute `need` once, before the wait (the first write then waits the
// timeout).
func TestAStoppedReplicaCostsOneFailedShipNotTheTimeout(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, replicaIndex := ownerAndReplica(t, tc)
	ownerClient := tc.nodes[ownerIndex].client
	if answer, err := writeConfirmed(ownerClient, "before", "all"); err != nil || answer.confirmed != 3 {
		t.Fatalf("test setup: the write before the stop was answered %+v (%v), want copies_confirmed 3", answer, err)
	}
	tc.stopNodeKeepingData(replicaIndex)
	timeout := tc.nodes[ownerIndex].server.core.cluster.wcTimeout

	first, err := writeConfirmed(ownerClient, "first-after", "all")
	if err != nil {
		t.Fatal(err)
	}
	if first.refusal != "" || first.confirmed != 2 {
		t.Fatalf("the first all write after a replica stopped was answered copies_confirmed %d, refusal %q after %v; want 2 and a success",
			first.confirmed, first.refusal, first.took)
	}
	if first.took >= timeout/2 {
		t.Errorf("the first all write after a replica stopped took %v; a ship to a stopped node fails at once, and the write-concern timeout is %v",
			first.took, timeout)
	}
	for i := 0; i < 20; i++ {
		answer, err := writeConfirmed(ownerClient, fmt.Sprintf("later-%02d", i), "all")
		if err != nil {
			t.Fatal(err)
		}
		if answer.refusal != "" || answer.confirmed != 2 || answer.took >= time.Second {
			t.Fatalf("all write %d after the first was answered copies_confirmed %d, refusal %q, after %v; want 2 at once",
				i, answer.confirmed, answer.refusal, answer.took)
		}
	}
}

// TestAFrozenReplicaCostsOneShipTimeoutNotTheWriteConcernTimeout is T-105
// (S-7, rev 3 review F-11). The owner's connection to a replica is frozen: a
// push sent on it is never answered. The first `all` write after the freeze
// waits for that ship to give up — replShipTimeout, half the write-concern
// timeout — and is then answered copies_confirmed 2, a success. The hundred
// writes after it do not wait. Guard: a ship deadline equal to the
// write-concern timeout (the first write is then answered with the timeout).
func TestAFrozenReplicaCostsOneShipTimeoutNotTheWriteConcernTimeout(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, replicaIndex := ownerAndReplica(t, tc)
	ownerClient := tc.nodes[ownerIndex].client
	if answer, err := writeConfirmed(ownerClient, "before", "all"); err != nil || answer.confirmed != 3 {
		t.Fatalf("test setup: the write before the freeze was answered %+v (%v), want copies_confirmed 3", answer, err)
	}
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex])))
	defer unfreeze()

	first, err := writeConfirmed(ownerClient, "first-after", "all")
	if err != nil {
		t.Fatal(err)
	}
	if first.refusal != "" || first.confirmed != 2 {
		t.Fatalf("the first all write after a replica froze was answered copies_confirmed %d, refusal %q after %v; want 2 and a success",
			first.confirmed, first.refusal, first.took)
	}
	if ship := ownerCluster.replShipTimeout(); first.took < ship || first.took > ownerCluster.wcTimeout-ownerCluster.wcTimeout/10 {
		t.Errorf("the first all write after a replica froze took %v; want at least the ship timeout %v and well under the write-concern timeout %v",
			first.took, ship, ownerCluster.wcTimeout)
	}
	started := time.Now()
	for i := 0; i < 100; i++ {
		answer, err := writeConfirmed(ownerClient, fmt.Sprintf("later-%03d", i), "all")
		if err != nil {
			t.Fatal(err)
		}
		if answer.refusal != "" || answer.confirmed != 2 || answer.took >= time.Second {
			t.Fatalf("all write %d after the first was answered copies_confirmed %d, refusal %q, after %v; want 2 without waiting for the frozen replica",
				i, answer.confirmed, answer.refusal, answer.took)
		}
	}
	t.Logf("the first write took %v; the next hundred took %v", first.took, time.Since(started))
}

// popTarget is one kind of pop BUG-0548 covers, on a bucket of the test
// database: a bucket pop of a fifo, a lifo or a heap bucket, or the pop of a
// fifo store field of document p1.
type popTarget struct {
	kind   string
	bucket string
	op     transport.Opcode
	field  bool
}

// popRequest is the target's pop at concern.
func (target popTarget) popRequest(concern string) bson.M {
	request := bson.M{"database": testDatabase, "bucket": target.bucket, "writeconcern": concern}
	if target.field {
		request["key"], request["field"] = "p1", "Comments"
	}
	return request
}

// createPopTarget creates target's bucket through node index, waits until
// every node serves it, and pushes two entries at concern all, each marked
// with its n.
func createPopTarget(t *testing.T, tc *testCluster, index int, target popTarget) {
	t.Helper()
	switch {
	case target.field:
		tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: target.bucket, Type: BucketTypeHash,
			Options: storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})})
	case target.op == transport.OpFIFOPop:
		tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: target.bucket, Type: BucketTypeFIFO})
	case target.op == transport.OpLIFOPop:
		tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: target.bucket, Type: BucketTypeLIFO})
	default:
		tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: target.bucket, Type: BucketTypeHeap})
	}
	waitForServing(t, tc, testDatabase, target.bucket, len(tc.nodes))
	if target.field {
		tc.ok(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: target.bucket, Key: "p1",
			Data: map[string]interface{}{"title": "p1"}, WriteConcern: "all"})
	}
	for priority, n := range []string{"first", "second"} {
		marker := target.bucket + "-" + n
		switch {
		case target.field:
			tc.ok(index, transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: target.bucket, Key: "p1", Field: "Comments",
				Data: map[string]interface{}{"n": marker}, WriteConcern: "all"})
		case target.op == transport.OpHeapPop:
			tc.ok(index, transport.OpHeapPush, PushRequest{Database: testDatabase, Bucket: target.bucket,
				Data: map[string]interface{}{"n": marker}, Priority: int64(priority + 1), WriteConcern: "all"})
		default:
			pushOp := transport.OpFIFOPush
			if target.op == transport.OpLIFOPop {
				pushOp = transport.OpLIFOPush
			}
			tc.ok(index, pushOp, PushRequest{Database: testDatabase, Bucket: target.bucket,
				Data: map[string]interface{}{"n": marker}, WriteConcern: "all"})
		}
	}
}

// markerOf is the n of an entry document, or "" when it has none.
func markerOf(entry bson.Raw) string {
	n, _ := entry.Lookup("n").StringValueOK()
	return n
}

// poppedMarker is the n of the entry a pop's answer carries, or "" when it
// carries none: an error, an empty pop, or a pop whose entry was dropped.
func poppedMarker(answer *transport.Frame) string {
	if answer.Opcode == transport.OpError {
		return ""
	}
	items, ok := bson.Raw(answer.Payload).Lookup("items").ArrayOK()
	if !ok {
		return ""
	}
	elements, err := items.Elements()
	if err != nil || len(elements) == 0 {
		return ""
	}
	entry, ok := elements[0].Value().DocumentOK()
	if !ok {
		return ""
	}
	// A field pop's item wraps the element document; a bucket pop's is the entry.
	if element, wrapped := entry.Lookup("element").DocumentOK(); wrapped {
		return markerOf(element)
	}
	return markerOf(entry)
}

// headOn is the n of the entry the next pop of target would take on s, read
// from s's own store, or "" when there is none.
func headOn(t *testing.T, s *Server, target popTarget) string {
	t.Helper()
	if target.field {
		peeked, err := s.FieldPeek(FieldPeekRequest{FieldRequest: FieldRequest{Database: testDatabase, Bucket: target.bucket, Key: "p1", Field: "Comments"}})
		if err != nil {
			t.Fatalf("peeking %s on its owner: %v", target.kind, err)
		}
		if len(peeked.Items) == 0 {
			return ""
		}
		return markerOf(peeked.Items[0].Element)
	}
	peeked, err := s.Peek(PeekRequest{Database: testDatabase, Bucket: target.bucket})
	if err != nil {
		t.Fatalf("peeking %s on its owner: %v", target.kind, err)
	}
	if len(peeked.Items) == 0 {
		return ""
	}
	return markerOf(peeked.Items[0])
}

// requirePopKeptItsEntry fails unless the entry head, at the head of target
// on its owner before the pop, is in the pop's answer or still at the head of
// the owner's queue: a pop may lose its answer, never the entry it removed.
func requirePopKeptItsEntry(t *testing.T, what string, answer *transport.Frame, owner *Server, target popTarget, head string) {
	t.Helper()
	if poppedMarker(answer) == head {
		return
	}
	if now := headOn(t, owner, target); now != head {
		t.Errorf("%s removed the entry %q, which is in neither the answer nor the owner's queue (its head is now %q); the answer was %s: %.400s",
			what, head, now, answer.Opcode, answer.Payload)
	}
}

// TestAPopWhoseWriteConcernTimesOutDoesNotLoseItsEntry is BUG-0548's path 2.
// A replica whose stream is in sync and confirms nothing holds every pop at
// concern all past the write-concern timeout, after its owner logged, applied
// and shipped it: the removal stands. Each pop's entry must then be in the
// caller's hands, sent to the owner or forwarded through a node that does not
// own the bucket, for every kind of pop. Before the fix the owner answered
// TIMEOUT and dropped the answer that held the entry.
func TestAPopWhoseWriteConcernTimesOutDoesNotLoseItsEntry(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 3, nil, func(_ int, c *Cluster) { c.wcTimeout = 2 * time.Second })
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	targets := []popTarget{
		{kind: "fifo pop", bucket: bucketOwnedBy(t, tc, owner, "fifo"), op: transport.OpFIFOPop},
		{kind: "lifo pop", bucket: bucketOwnedBy(t, tc, owner, "lifo"), op: transport.OpLIFOPop},
		{kind: "heap pop", bucket: bucketOwnedBy(t, tc, owner, "heap"), op: transport.OpHeapPop},
		{kind: "store-field pop", bucket: bucketOwnedBy(t, tc, owner, "posts"), op: transport.OpFieldPop, field: true},
	}
	for _, target := range targets {
		createPopTarget(t, tc, owner, target)
	}

	// The sender to the held replica waits before each frame it ships, which
	// no ship timeout bounds: the stream stays streaming, so the replica is
	// in sync and counted by concern all, and confirms nothing.
	heldID := tc.addrs[held]
	release := make(chan struct{})
	var once sync.Once
	releaseHeld := func() { once.Do(func() { close(release) }) }
	defer releaseHeld()
	hold := func(peerID string) func() {
		if peerID == heldID {
			<-release
		}
		return func() {}
	}
	beforeShipHook.Store(&hold)
	defer beforeShipHook.Store(nil)

	ownerCore := tc.nodes[owner].server.core
	for _, target := range targets {
		for _, via := range []int{owner, coordinator} {
			what := fmt.Sprintf("a %s at concern all through node %d", target.kind, via+1)
			head := headOn(t, ownerCore, target)
			if head == "" {
				t.Fatalf("test setup: %s has nothing to pop", target.kind)
			}
			answer := tc.frame(via, target.op, target.popRequest("all"))
			requirePopKeptItsEntry(t, what, answer, ownerCore, target, head)
			if poppedMarker(answer) != head {
				continue
			}
			// The entry is answered as a write with fewer copies than asked
			// is: the owner and the replica that is not held hold the removal.
			confirmed, factor, _, _ := copiesConfirmedOf(answer.Payload)
			outcome, _ := bson.Raw(answer.Payload).Lookup("write_outcome").StringValueOK()
			if confirmed != 2 || factor != 3 || outcome != outcomeApplied {
				t.Errorf("%s was answered copies_confirmed %d, replication_factor %d, write_outcome %q; want 2, 3 and %q",
					what, confirmed, factor, outcome, outcomeApplied)
			}
		}
	}
}

// holdShipsTo makes the senders to the node whose ID is heldID wait before
// each frame they ship, which no ship timeout bounds: that replica's stream
// stays streaming, so it is in sync and counted by concern all, and it
// confirms nothing. The caller defers the release it returns after the
// cluster's stop, so the held senders are released before the cluster waits
// for them.
func holdShipsTo(heldID string) (release func()) {
	held := make(chan struct{})
	hold := func(peerID string) func() {
		if peerID == heldID {
			<-held
		}
		return func() {}
	}
	beforeShipHook.Store(&hold)
	return func() {
		beforeShipHook.Store(nil)
		close(held)
	}
}

// TestAnEmptyPopAndAPutKeepTheirTimeoutWhenTheConcernIsNotMet pins the edge
// of BUG-0548's fix: only a pop that removed an entry is answered success
// when its write falls short. A pop of an empty queue removed nothing and a
// put's answer holds no removed data, so both keep the write-concern timeout
// (TIMEOUT, write_outcome applied), sent to the owner and through a node that
// does not own the bucket. So does a claim that was logged and took nothing (it
// dead-lettered its bucket's entry); a claim of an empty queue is not logged
// at all (claim-ack-spec 5.3), so it has no timeout to keep. Guards: answer
// every pop that reaches the shortfall with its response, found or not; answer
// a claim whatever its found says.
func TestAnEmptyPopAndAPutKeepTheirTimeoutWhenTheConcernIsNotMet(t *testing.T) {
	ownerClock := &leaseClockAt{}
	ownerClock.millis.Store(leaseClusterBase)
	tc := newTestClusterTuned(t, 3, 3, nil, func(index int, c *Cluster) {
		c.wcTimeout = 2 * time.Second
		if index == 0 {
			c.s.leaseClock = ownerClock.now
		}
	})
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	empty := bucketOwnedBy(t, tc, owner, "empty")
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: empty, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, empty, len(tc.nodes))
	docs := bucketOwnedBy(t, tc, owner, "docs")
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: docs, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, docs, len(tc.nodes))
	// A claim that dead-letters its bucket's one entry takes nothing, so its
	// answer holds no entry: one such bucket per route, each claimed until its
	// next claim dead-letters the entry.
	dying := map[int]clusterPopSubject{}
	for _, via := range []int{owner, coordinator} {
		subject := clusterPopSubject{kind: fmt.Sprintf("dying%d", via), bucketType: BucketTypeFIFO}.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 1, 0)
		for claims := 0; claims < store.MaxDeliveries; claims++ {
			claimAnswerOf(t, tc.frame(owner, transport.OpQueueClaim, subject.claimRequest(1, 1_000)), "a claim before the hold")
			ownerClock.advance(2 * time.Second)
		}
		dying[via] = subject
	}
	defer holdShipsTo(tc.addrs[held])()

	for _, via := range []int{owner, coordinator} {
		what := fmt.Sprintf("an empty pop at concern all through node %d", via+1)
		refusal := errorAnswerOf(t, tc.frame(via, transport.OpFIFOPop, bson.M{"database": testDatabase, "bucket": empty, "writeconcern": "all"}), what)
		requireOutcome(t, refusal, what, outcomeApplied, transport.ErrTimeout)

		what = fmt.Sprintf("a put at concern all through node %d", via+1)
		refusal = errorAnswerOf(t, tc.frame(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: docs,
			Key: fmt.Sprintf("held-%d", via), Data: map[string]interface{}{"n": via}, WriteConcern: "all"}), what)
		requireOutcome(t, refusal, what, outcomeApplied, transport.ErrTimeout)

		// The claim rows (claim/ack build step 15). A claim of an empty
		// queue is answered by its owner without logging it (claim-ack-spec
		// 5.3): nothing is written, so nothing times out.
		what = fmt.Sprintf("an empty claim at concern all through node %d", via+1)
		idle := claimAnswerOf(t, tc.frame(via, transport.OpQueueClaim, bson.M{"database": testDatabase, "bucket": empty, "writeconcern": "all"}), what)
		if idle.Found || idle.WriteOutcome != "" {
			t.Errorf("%s answered found %v, write_outcome %q; want the unlogged empty answer", what, idle.Found, idle.WriteOutcome)
		}
		// A logged claim that took nothing keeps its timeout.
		what = fmt.Sprintf("a claim that only dead-lettered, at concern all through node %d", via+1)
		refusal = errorAnswerOf(t, tc.frame(via, transport.OpQueueClaim, dying[via].claimRequest(1, 1_000)), what)
		requireOutcome(t, refusal, what, outcomeApplied, transport.ErrTimeout)
		if dead := bucketStateOf(t, tc.nodes[owner].server.core, testDatabase, dying[via].bucket).Dead; dead != 1 {
			t.Errorf("setup: %s left %d dead entries, want 1", what, dead)
		}
	}
}
