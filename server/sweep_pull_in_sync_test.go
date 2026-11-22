package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0729: a pull of a bucket on a replica made the replica refuse the
// owner's stream's CREATE_BUCKET of it (errSnapshotPending), or an entry its
// snapshot covered (awaiting copy), which failed the whole frame: the stream
// left the in-sync set, and every write the owner took meanwhile was
// acknowledged by its own copy alone on an RF=2 bucket.

// departureCounter counts the in-sync departures of every stream in the
// process, through leftInSyncHook, and keeps their reasons.
type departureCounter struct {
	count   atomic.Int64
	mu      sync.Mutex
	reasons []string
}

func countDepartures(t *testing.T) *departureCounter {
	t.Helper()
	counter := &departureCounter{}
	hook := func(from, to, reason string) {
		counter.count.Add(1)
		counter.mu.Lock()
		counter.reasons = append(counter.reasons, fmt.Sprintf("%s -> %s: %s", from, to, reason))
		counter.mu.Unlock()
	}
	leftInSyncHook.Store(&hook)
	t.Cleanup(func() { leftInSyncHook.Store(nil) })
	return counter
}

func (d *departureCounter) requireNone(t *testing.T, when string) {
	t.Helper()
	if n := d.count.Load(); n != 0 {
		d.mu.Lock()
		defer d.mu.Unlock()
		t.Fatalf("%s: a stream left the in-sync set %d times, so writes in that window were acknowledged by fewer copies than "+
			"the bucket's factor: %v", when, n, d.reasons)
	}
}

// inSyncSampler samples every stream of the cluster's nodes until stopped and
// counts the samples that found one out of the in-sync set.
type inSyncSampler struct {
	out  atomic.Int64
	stop chan struct{}
	done sync.WaitGroup
}

func sampleInSync(tc *testCluster) *inSyncSampler {
	s := &inSyncSampler{stop: make(chan struct{})}
	var peers []*peer
	for i := range tc.nodes {
		c := clusterOf(tc, i)
		for _, member := range c.otherMembers() {
			peers = append(peers, c.peerFor(member))
		}
	}
	s.done.Add(1)
	go func() {
		defer s.done.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				for _, p := range peers {
					if !p.inSync() {
						s.out.Add(1)
					}
				}
			}
		}
	}()
	return s
}

func (s *inSyncSampler) finish() int64 {
	close(s.stop)
	s.done.Wait()
	return s.out.Load()
}

// writeLoad is the burst of bucket creation the bug was seen under: writers
// create buckets and write to each through one node, and every write's
// copies_confirmed is kept.
type writeLoad struct {
	t       *testing.T
	client  *transport.DriverClient
	mu      sync.Mutex
	written map[string][]string
	short   []string
	failed  []string
}

func (w *writeLoad) send(op transport.Opcode, request any) ([]byte, bool) {
	payload, err := bson.Marshal(request)
	if err != nil {
		w.t.Errorf("encoding %s: %v", op, err)
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := w.client.SendRequest(ctx, op, payload)
	if err != nil || frame.Opcode == transport.OpError {
		w.mu.Lock()
		defer w.mu.Unlock()
		if err != nil {
			w.failed = append(w.failed, fmt.Sprintf("%s: %v", op, err))
		} else {
			w.failed = append(w.failed, fmt.Sprintf("%s: %s", op, bson.Raw(frame.Payload)))
		}
		return nil, false
	}
	return frame.Payload, true
}

// write sends one write and records it, and records it as short when it was
// confirmed by fewer than want copies.
func (w *writeLoad) write(bucket, id string, op transport.Opcode, request any, want int32) {
	payload, ok := w.send(op, request)
	if !ok {
		return
	}
	confirmed, _, _, _ := copiesConfirmedOf(payload)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written[bucket] = append(w.written[bucket], id)
	if confirmed < want {
		w.short = append(w.short, fmt.Sprintf("%s/%s confirmed by %d", bucket, id, confirmed))
	}
}

// run creates buckets buckets with writers writers and writes keys to each,
// at the default concern, which at factor 2 is both copies.
func (w *writeLoad) run(prefix string, buckets, writers, keys int) {
	var wg sync.WaitGroup
	next := atomic.Int64{}
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= buckets {
					return
				}
				bucket := fmt.Sprintf("%s%04d", prefix, i)
				if _, ok := w.send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash}); !ok {
					continue
				}
				for k := range keys {
					key := fmt.Sprintf("k%d", k)
					w.write(bucket, key, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
						Data: map[string]interface{}{"n": k}}, 2)
				}
			}
		}()
	}
	wg.Wait()
}

// pushHeld pushes count elements to the held FIFO bucket.
func (w *writeLoad) pushHeld(bucket string, from, count int) {
	for i := from; i < from+count; i++ {
		id := fmt.Sprintf("e%04d", i)
		w.write(bucket, id, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket,
			Data: map[string]interface{}{"_id_": id}}, 2)
	}
}

// requireClean fails on any refused or short write.
func (w *writeLoad) requireClean(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.failed) > 0 {
		t.Errorf("%d writes were refused, first: %v", len(w.failed), w.failed[0])
	}
	if len(w.short) > 0 {
		t.Fatalf("%d writes on a factor-2 bucket were acknowledged by fewer than 2 copies, first %v", len(w.short), w.short[:min(5, len(w.short))])
	}
}

// requireIdenticalCopies waits for both streams to catch up, then checks that
// both nodes hold every key written to every hash bucket and the same queue
// elements, each once, in the held bucket.
func requireIdenticalCopies(t *testing.T, tc *testCluster, w *writeLoad, held string) {
	t.Helper()
	for i := range tc.nodes {
		c := clusterOf(tc, i)
		for _, member := range c.otherMembers() {
			caughtUpWithin(t, c.peerFor(member), 30*time.Second)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for bucket, keys := range w.written {
		if bucket == held {
			continue
		}
		for _, key := range keys {
			for i, node := range tc.nodes {
				if !holdsKeyLocally(node, bucket, key) {
					t.Errorf("node %d does not hold %s/%s", i+1, bucket, key)
				}
			}
		}
	}
	var copies [2]map[string]int
	for i, node := range tc.nodes {
		ids, err := queueElementIDs(node, held)
		if err != nil {
			t.Fatalf("reading the held bucket on node %d: %v", i+1, err)
		}
		copies[i] = ids
	}
	for _, id := range w.written[held] {
		for i := range tc.nodes {
			if copies[i][id] != 1 {
				t.Errorf("node %d holds element %s of the held bucket %d times, want once", i+1, id, copies[i][id])
			}
		}
	}
	for i := range tc.nodes {
		if len(copies[i]) != len(w.written[held]) {
			t.Errorf("node %d holds %d distinct elements of the held bucket, %d were written", i+1, len(copies[i]), len(w.written[held]))
		}
	}
	first, firstErr := queueOrder(tc.nodes[0], held)
	second, secondErr := queueOrder(tc.nodes[1], held)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("reading the order of the held queue: %v", errors.Join(firstErr, secondErr))
	}
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Errorf("the two copies of the held queue are in different orders:\n node 1 %v\n node 2 %v", first, second)
	}
}

// requireLandingReleased fails when a pull's landing mark outlived the pull.
func requireLandingReleased(t *testing.T, c *Cluster, bucket string) {
	t.Helper()
	if c.pullLanding(testDatabase, bucket) != nil || c.landingPulls.Load() != 0 {
		t.Fatalf("a pull of %s ended and its landing is still marked (%d landing): every later entry of the bucket waits for it",
			bucket, c.landingPulls.Load())
	}
}

// sweepPullCase is TestSweepPullOfNewBucketKeepsPeerInSync's body: the
// replica's sweep pull of a bucket (ensureMigrated, the sweep's path) is held
// in flight, registered, while the owner creates the bucket and its stream
// delivers the create, under a burst of bucket creation; the pull is let go
// after holdFor.
func sweepPullCase(t *testing.T, holdFor time.Duration) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	ownerIndex := ownerIndexOf(t, tc, held)
	replica := clusterOf(tc, 1-ownerIndex)
	departures := countDepartures(t)

	reached, release := holdBucketAt(t, &pullStartedHook, held)
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pulled <- replica.ensureMigrated(ctx, testDatabase, held)
	}()
	select {
	case <-reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the replica's sweep pull never started")
	}
	if !replica.pullPending(testDatabase, held) {
		t.Fatal("the held pull is not registered as pending: the interleaving under test is not built")
	}

	sampler := sampleInSync(tc)
	load := &writeLoad{t: t, client: tc.nodes[ownerIndex].client, written: make(map[string][]string)}
	if _, ok := load.send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: held, Type: BucketTypeFIFO}); !ok {
		t.Fatalf("creating the held bucket: %v", load.failed)
	}
	load.pushHeld(held, 0, 20)
	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		load.run("U", 200, 8, 2)
	}()
	// The pull stays in flight across the burst and for holdFor, writes to the
	// held bucket going on all the while.
	deadline := time.After(holdFor)
	for pushed := 20; ; pushed += 5 {
		select {
		case <-deadline:
		default:
			load.pushHeld(held, pushed, 5)
			continue
		}
		break
	}
	<-loadDone
	load.pushHeld(held, 100000, 5)
	release()
	select {
	case err := <-pulled:
		if err != nil {
			t.Errorf("the sweep pull ended with %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the sweep pull never ended")
	}
	requireLandingReleased(t, replica, held)
	load.pushHeld(held, 200000, 5)
	out := sampler.finish()

	departures.requireNone(t, "a sweep pull of a bucket was in flight while the owner's stream delivered its create")
	if out != 0 {
		t.Fatalf("%d samples found a stream out of the in-sync set", out)
	}
	load.requireClean(t)
	requireIdenticalCopies(t, tc, load, held)
}

// TestSweepPullOfNewBucketKeepsPeerInSync is BUG-0729's acceptance test: the
// sweep's pull of a just-created bucket on the replica, held in flight while
// the owner's stream delivers the bucket's create under a burst of 200 bucket
// creations with writes, leaves the stream in the in-sync set, every write is
// confirmed by both copies, and both nodes end with the same contents.
func TestSweepPullOfNewBucketKeepsPeerInSync(t *testing.T) {
	sweepPullCase(t, 0)
}

// TestSweepPullHeldPastTheRetryIntervalKeepsPeerInSync holds the pull for
// several of the sender's retry intervals, so that the stream staying in sync
// does not depend on the pull finishing within one retry.
func TestSweepPullHeldPastTheRetryIntervalKeepsPeerInSync(t *testing.T) {
	sweepPullCase(t, 6*replRetryInterval)
}

// TestPullLandingWindowDoesNotFailTheStream covers the pulls that are not the
// sweep's: a pull started by any caller (pullBucket, the primitive every
// caller ends in) that has recorded its snapshot position and is putting the
// copy in place when the owner's stream delivers the bucket's create. Before
// the fix the replica answered the create "awaiting copy", which failed the
// frame; a landing that ends within the replica's wait must cost the stream
// nothing.
func TestPullLandingWindowDoesNotFailTheStream(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	ownerIndex := ownerIndexOf(t, tc, held)
	replicaIndex := 1 - ownerIndex
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, replicaIndex)
	departures := countDepartures(t)

	// The owner's sender to the replica is held before its next ship, so the
	// create reaches the replica only after the pull has recorded its position.
	gate := newShipGate()
	replicaID := replica.self.ID
	shipHook := func(peerID string) func() {
		if peerID != replicaID {
			return func() {}
		}
		return gate.before(peerID)
	}
	beforeShipHook.Store(&shipHook)
	defer beforeShipHook.Store(nil)
	gate.close()
	gateOpen := sync.OnceFunc(gate.open)
	defer gateOpen()

	load := &writeLoad{t: t, client: tc.nodes[ownerIndex].client, written: make(map[string][]string)}
	created := make(chan struct{})
	go func() {
		defer close(created)
		load.send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: held, Type: BucketTypeFIFO})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !owner.s.bucketStoreOpen(testDatabase, held) {
		if time.Now().After(deadline) {
			t.Fatal("the owner never opened the held bucket")
		}
		time.Sleep(5 * time.Millisecond)
	}

	reached, releaseLanding := holdBucketAt(t, &beforeMaterializeHook, held)
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pulled <- replica.pullBucket(ctx, testDatabase, held, owner.self)
	}()
	select {
	case <-reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the pull never reached its landing")
	}
	sampler := sampleInSync(tc)
	gateOpen()
	// The create is with the replica now, inside the landing window; the
	// landing ends well within the replica's wait for it.
	time.Sleep(200 * time.Millisecond)
	releaseLanding()
	select {
	case err := <-pulled:
		if err != nil {
			t.Errorf("the pull ended with %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the pull never ended")
	}
	requireLandingReleased(t, replica, held)
	<-created
	load.pushHeld(held, 0, 20)
	out := sampler.finish()

	departures.requireNone(t, "a pull of a bucket was landing while the owner's stream delivered its create")
	if out != 0 {
		t.Fatalf("%d samples found a stream out of the in-sync set", out)
	}
	load.requireClean(t)
	requireIdenticalCopies(t, tc, load, held)
}

// TestPullLandingPastTheWaitLosesNothing is the window the fix leaves: a
// landing that outlasts the replica's wait for it. The create is refused then
// (errSnapshotPending) and the frame fails, as before the fix; what must hold
// is that nothing is lost. Elements are pushed at concern one before the pull,
// so its snapshot holds them and their entries arrive in the create's frame,
// covered by its position: admitted instead of refused, the create would let
// them be passed over as held by a snapshot whose landing is then refused as
// already served, and the replica would never hold them.
func TestPullLandingPastTheWaitLosesNothing(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	ownerIndex := ownerIndexOf(t, tc, held)
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, 1-ownerIndex)

	gate := newShipGate()
	replicaID := replica.self.ID
	shipHook := func(peerID string) func() {
		if peerID != replicaID {
			return func() {}
		}
		return gate.before(peerID)
	}
	beforeShipHook.Store(&shipHook)
	defer beforeShipHook.Store(nil)
	gate.close()
	gateOpen := sync.OnceFunc(gate.open)
	defer gateOpen()

	load := &writeLoad{t: t, client: tc.nodes[ownerIndex].client, written: make(map[string][]string)}
	created := make(chan struct{})
	go func() {
		defer close(created)
		load.send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: held, Type: BucketTypeFIFO})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !owner.s.bucketStoreOpen(testDatabase, held) {
		if time.Now().After(deadline) {
			t.Fatal("the owner never opened the held bucket")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := range 5 {
		id := fmt.Sprintf("before%d", i)
		load.write(held, id, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: held,
			Data: map[string]interface{}{"_id_": id}, WriteConcern: "one"}, 0)
	}

	reached, releaseLanding := holdBucketAt(t, &beforeMaterializeHook, held)
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pulled <- replica.pullBucket(ctx, testDatabase, held, owner.self)
	}()
	select {
	case <-reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the pull never reached its landing")
	}
	gateOpen()
	time.Sleep(2*replica.landingWait() + 500*time.Millisecond)
	releaseLanding()
	select {
	case err := <-pulled:
		if err != nil {
			t.Errorf("the pull ended with %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the pull never ended")
	}
	<-created
	load.pushHeld(held, 0, 5)
	requireIdenticalCopies(t, tc, load, held)
	// The refused frame took the stream out; once the landing has ended it
	// must come back, or every later write stays single-copy.
	toReplica := owner.peerFor(mustNode(t, owner, replicaID))
	for deadline := time.Now().Add(20 * time.Second); !toReplica.inSync(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the stream to the replica did not return to the in-sync set after the landing ended")
		}
	}
}

// TestStreamCreateRefusedOnlyWhileAPullLands pins the rule refuseUnservable
// applies to a create from another node's stream: admitted while a pull of
// the bucket is only pending — the pull finds the bucket served under the
// fence and stops — and refused while one lands, when admitting it would let
// the entries its snapshot position covers pass as held by a copy that is then
// refused. A create of this node's own keeps the refusal for the whole pull.
// The landing case is a race a cluster test cannot build: the stream's own
// check (R-4) refuses a covered create first, unless the position is recorded
// between that check and the create's fence.
func TestStreamCreateRefusedOnlyWhileAPullLands(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	replica := clusterOf(tc, 1-ownerIndexOf(t, tc, held))
	payload, err := bson.Marshal(CreateBucketRequest{Database: testDatabase, Name: held, Type: BucketTypeFIFO})
	if err != nil {
		t.Fatalf("encoding the create: %v", err)
	}
	create := ReplEntry{Seq: 1, Op: int32(transport.OpCreateBucket), Bucket: held, Payload: payload}

	release, claimed := replica.claimPull(testDatabase, held)
	if !claimed {
		t.Fatal("could not register a pull of the held bucket")
	}
	defer release()
	if err := replica.refuseUnservable(create, true); err != nil {
		t.Errorf("a stream's create was refused while a pull of its bucket was only pending: %v", err)
	}
	if err := replica.refuseUnservable(create, false); !errors.Is(err, errSnapshotPending) {
		t.Errorf("this node's own create was not refused while a pull of its bucket was pending: %v", err)
	}
	endLanding := replica.beginLanding(testDatabase, held)
	if err := replica.refuseUnservable(create, true); !errors.Is(err, errSnapshotPending) {
		t.Errorf("a stream's create was not refused while a pull of its bucket was landing: %v", err)
	}
	endLanding()
	if err := replica.refuseUnservable(create, true); err != nil {
		t.Errorf("a stream's create was refused after the landing ended: %v", err)
	}
}
