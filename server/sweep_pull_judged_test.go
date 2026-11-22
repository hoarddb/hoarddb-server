package server

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0729, round 2: the receiver judged a stream entry without the bucket's
// fence, so a pull could record its position and begin landing between the
// judgement and the create's apply; the create was then refused and the frame
// failed. The entry is re-judged once after waiting, and a pull marks its
// landing before it records its position.

// TestCreateJudgedBeforeTheLandingBeginsKeepsPeerInSync builds the race the stress
// test hits by chance: the stream's create is judged (no landing, no recorded
// position) and only then takes its fence, after a pull has recorded its
// position and begun landing. The landing ends well within the wait.
func TestCreateJudgedBeforeTheLandingBeginsKeepsPeerInSync(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	ownerIndex := ownerIndexOf(t, tc, held)
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, 1-ownerIndex)
	departures := countDepartures(t)

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
		// Held on purpose past its concern's timeout; its outcome is not judged.
		creator := &writeLoad{t: t, client: tc.nodes[ownerIndex].client, written: make(map[string][]string)}
		creator.send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: held, Type: BucketTypeFIFO})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !owner.s.bucketStoreOpen(testDatabase, held) {
		if time.Now().After(deadline) {
			t.Fatal("the owner never opened the held bucket")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := range 3 {
		id := fmt.Sprintf("before%d", i)
		load.write(held, id, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: held,
			Data: map[string]interface{}{"_id_": id}, WriteConcern: "one"}, 0)
	}

	judged, releaseJudged := holdBucketAt(t, &afterReplicaJudgedHook, held)
	sampler := sampleInSync(tc)
	gateOpen()
	select {
	case <-judged:
	case <-time.After(20 * time.Second):
		t.Fatal("the stream's create was never judged on the replica")
	}

	landing, releaseLanding := holdBucketAt(t, &beforeMaterializeHook, held)
	pulled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pulled <- replica.pullBucket(ctx, testDatabase, held, owner.self)
	}()
	select {
	case <-landing:
	case <-time.After(20 * time.Second):
		t.Fatal("the pull never reached its landing")
	}
	releaseJudged()
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
	<-created
	load.pushHeld(held, 0, 10)
	out := sampler.finish()
	departures.requireNone(t, "the stream's create was judged before a pull's landing began and applied after")
	if out != 0 {
		t.Fatalf("%d samples found a stream out of the in-sync set", out)
	}
	load.requireClean(t)
	requireIdenticalCopies(t, tc, load, held)
}

// TestFailedLandingForgetsItsPositionBeforeItEnds drives a landing
// whose materialise fails while the stream's create waits for it: the position
// must be forgotten before the landing ends, so the waiting create is applied
// (not refused as awaiting copy).
func TestFailedLandingForgetsItsPositionBeforeItEnds(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	ownerIndex := ownerIndexOf(t, tc, held)
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, 1-ownerIndex)
	departures := countDepartures(t)

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

	var failOnce atomic.Bool
	failHook := func(uint64) error {
		if failOnce.CompareAndSwap(false, true) {
			return errors.New("verifier: injected materialise failure")
		}
		return nil
	}
	materializeCatalogHook.Store(&failHook)
	defer materializeCatalogHook.Store(nil)

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
	// Hold the position's forget: the failed pull must forget the position
	// before it ends the landing, so the landing must outlast this hold.
	landed := replica.pullLanding(testDatabase, held)
	if landed == nil {
		t.Fatal("no landing marked while the pull is held at its landing")
	}
	sampler := sampleInSync(tc)
	gateOpen()
	time.Sleep(200 * time.Millisecond)
	replica.positionMu.Lock() // forgetSeeded takes it
	releaseLanding()
	select {
	case <-landed:
		replica.positionMu.Unlock()
		t.Fatal("the landing ended while the failed pull's position was not yet forgotten")
	case <-time.After(time.Second):
	}
	replica.positionMu.Unlock()
	select {
	case err := <-pulled:
		if err == nil {
			t.Fatal("the injected materialise failure did not fail the pull: the interleaving under test is not built")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the pull never ended")
	}
	requireLandingReleased(t, replica, held)
	<-created
	load.pushHeld(held, 0, 20)
	out := sampler.finish()
	departures.requireNone(t, "a landing whose materialise failed while the stream's create waited for it")
	if out != 0 {
		t.Fatalf("%d samples found a stream out of the in-sync set", out)
	}
	load.requireClean(t)
	requireIdenticalCopies(t, tc, load, held)
}

// TestPullMarksItsLandingBeforeItsPositionIsVisible polls, from the pull's
// start until it is held at its landing, for a recorded snapshot position
// with no landing marked. A create admitted in that gap (between the pull's
// fence release and its landing mark) is refused errSnapshotPending with
// nothing left to wait for.
func TestPullMarksItsLandingBeforeItsPositionIsVisible(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const held = "Held"
	ownerIndex := ownerIndexOf(t, tc, held)
	owner := clusterOf(tc, ownerIndex)
	replica := clusterOf(tc, 1-ownerIndex)
	// The owner's stream to the replica is held, so the replica does not
	// get the bucket from it and the pull has something to land.
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
	for deadline := time.Now().Add(10 * time.Second); !owner.s.bucketStoreOpen(testDatabase, held); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the owner never opened the held bucket")
		}
	}
	reached, releaseLanding := holdBucketAt(t, &beforeMaterializeHook, held)
	var stop atomic.Bool
	var seededUnmarked, polls atomic.Int64
	polling := make(chan struct{})
	go func() {
		defer close(polling)
		for !stop.Load() {
			polls.Add(1)
			if replica.seededThroughFor(owner.self.ID, testDatabase, held) > 0 && replica.pullLanding(testDatabase, held) == nil {
				seededUnmarked.Add(1)
			}
		}
	}()
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
	stop.Store(true)
	<-polling
	releaseLanding()
	if err := <-pulled; err != nil {
		t.Errorf("the pull ended with %v", err)
	}
	gateOpen()
	<-created
	if seededUnmarked.Load() > 0 {
		t.Fatalf("%d of %d polls saw the snapshot position recorded with no landing marked", seededUnmarked.Load(), polls.Load())
	}
}

// TestCreationBurstWithPullsKeepsPeersInSync is the burst at a size that runs
// in 15 to 30 seconds (each bucket's create queues behind the earlier ones on
// one serial stream per node pair): RF=2, every bucket's create judged on its replica and then
// held until a pull of the bucket, forced on that replica, has reached its
// landing; the landing is then held within the wait while the create applies.
func TestCreationBurstWithPullsKeepsPeersInSync(t *testing.T) {
	creationBurstWithPulls(t, 3, 2, 150, func() time.Duration { return time.Duration(rand.IntN(20)) * time.Millisecond }, true)
}

// TestCreationBurstWithPullsLong is the 500-bucket variant of the verifier's
// stress runs, for RF=2 and RF=3. Opt in with HOARDDB_LONG_TESTS=1.
func TestCreationBurstWithPullsLong(t *testing.T) {
	if os.Getenv("HOARDDB_LONG_TESTS") == "" || testing.Short() {
		t.Skip("500 buckets and slowed landings; set HOARDDB_LONG_TESTS=1")
	}
	for _, rf := range []int{2, 3} {
		t.Run(fmt.Sprintf("rf%d", rf), func(t *testing.T) {
			creationBurstWithPulls(t, 3, rf, 500, func() time.Duration { return time.Duration(rand.IntN(300)) * time.Millisecond }, true)
		})
	}
}

// creationBurstWithPulls is the burst: buckets FIFO buckets created on their
// owners by writers. The stream's create of each bucket is held on its replica
// after it is judged and before it is applied (afterReplicaJudgedHook); there
// a pull of the bucket from its owner is forced on the other members
// (pullBucket, not ensureMigrated: the stream has not served the bucket yet,
// but a created bucket's ensureMigrated would take the already-open path). The
// create is let go once a landing of the bucket is reached, and each landing is
// held for landing() before it materialises, so the create applies while a
// landing is in flight: the interleaving BUG-0729 lost (the create refused as
// awaiting a copy, the frame failed, the stream left the in-sync set).
func creationBurstWithPulls(t *testing.T, nodes, rf, buckets int, landing func() time.Duration, wantNoDeparture bool) {
	tc := newTestClusterWith(t, nodes, rf, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	departures := countDepartures(t)

	// A bucket's create is held at most this long for its pull to reach a
	// landing; a pull that does not is a stuck interleaving, counted, not waited on.
	const landingWait = 30 * time.Second
	var (
		landed, started, stuck atomic.Int64
		pullErr, pullOK        atomic.Int64
		pulls                  sync.WaitGroup
	)
	// One gate per bucket: its first judged create starts the pulls, and its
	// first landing lets that create go.
	type bucketGate struct {
		judged, landedOnce sync.Once
		reached            chan struct{}
	}
	gates := make(map[string]*bucketGate, buckets)
	for i := range buckets {
		gates[fmt.Sprintf("S%04d", i)] = &bucketGate{reached: make(chan struct{})}
	}
	landHook := func(_, name string) {
		landed.Add(1)
		if g := gates[name]; g != nil {
			g.landedOnce.Do(func() { close(g.reached) })
		}
		time.Sleep(landing())
	}
	beforeMaterializeHook.Store(&landHook)
	defer beforeMaterializeHook.Store(nil)

	c0 := clusterOf(tc, 0)
	type placed struct {
		owner   int
		members []int
	}
	where := make([]placed, buckets)
	for i := range buckets {
		name := fmt.Sprintf("S%04d", i)
		set, err := c0.Replicas(placementOf(testDatabase, name))
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		for j, n := range set {
			idx := nodeIndexByAddr(t, tc.addrs, n.ID)
			if j == 0 {
				where[i].owner = idx
			}
			where[i].members = append(where[i].members, idx)
		}
	}

	var (
		mu     sync.Mutex
		pushed = make(map[string][]string)
		short  []string
		failed []string
	)
	judgedHook := func(_, name string) {
		g := gates[name]
		if g == nil {
			return
		}
		g.judged.Do(func() {
			i := bucketIndex(name)
			owner := clusterOf(tc, where[i].owner).self
			for _, m := range where[i].members[1:] {
				pulls.Add(1)
				started.Add(1)
				go func(c *Cluster) {
					defer pulls.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					defer cancel()
					if err := c.pullBucket(ctx, testDatabase, name, owner); err != nil {
						pullErr.Add(1)
					} else {
						pullOK.Add(1)
					}
				}(clusterOf(tc, m))
			}
			select {
			case <-g.reached:
			case <-time.After(landingWait):
				stuck.Add(1)
			}
		})
	}
	afterReplicaJudgedHook.Store(&judgedHook)
	defer afterReplicaJudgedHook.Store(nil)

	const elements = 3
	next := atomic.Int64{}
	var writers sync.WaitGroup
	for range 16 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= buckets {
					return
				}
				name := fmt.Sprintf("S%04d", i)
				load := &writeLoad{t: t, client: tc.nodes[where[i].owner].client, written: make(map[string][]string)}
				if _, ok := load.send(transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeFIFO}); !ok {
					mu.Lock()
					failed = append(failed, load.failed...)
					mu.Unlock()
					continue
				}
				for e := range elements {
					id := fmt.Sprintf("e%02d", e)
					load.write(name, id, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: name,
						Data: map[string]interface{}{"_id_": id}}, 2)
				}
				mu.Lock()
				pushed[name] = load.written[name]
				short = append(short, load.short...)
				failed = append(failed, load.failed...)
				mu.Unlock()
			}
		}()
	}
	writers.Wait()
	pulls.Wait()
	t.Logf("pulls started %d, landed %d, pullBucket ok %d err %d, creates held past %s %d, departures %d",
		started.Load(), landed.Load(), pullOK.Load(), pullErr.Load(), landingWait, stuck.Load(), departures.count.Load())
	if stuck.Load() > 0 || landed.Load() < int64(buckets) {
		t.Fatalf("the interleaving under test was not built: %d of %d buckets had a pull reach its landing, %d creates waited out %s for one",
			landed.Load(), buckets, stuck.Load(), landingWait)
	}
	if wantNoDeparture {
		departures.requireNone(t, "a creation burst with forced pulls whose landings end within the wait")
		if len(short) > 0 {
			t.Fatalf("%d writes acknowledged by fewer than 2 copies, first %v", len(short), short[:min(5, len(short))])
		}
	}
	if len(failed) > 0 {
		t.Errorf("%d requests failed, first %v", len(failed), failed[0])
	}
	// Every stream returns to the in-sync set and catches up.
	for i := range tc.nodes {
		c := clusterOf(tc, i)
		for _, member := range c.otherMembers() {
			p := c.peerFor(member)
			for deadline := time.Now().Add(60 * time.Second); !p.inSync(); time.Sleep(20 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("stream %s -> %s did not return to the in-sync set", c.self.ID, member.ID)
				}
			}
			caughtUpWithin(t, p, 60*time.Second)
		}
	}
	want := make([]string, elements)
	for e := range elements {
		want[e] = fmt.Sprintf("e%02d", e)
	}
	bad := 0
	for i := range buckets {
		name := fmt.Sprintf("S%04d", i)
		acked := pushed[name]
		for _, m := range where[i].members {
			got, err := queueOrder(tc.nodes[m], name)
			if err != nil {
				t.Fatalf("reading %s on node %d: %v", name, m, err)
			}
			// Every acknowledged element present, each once, in push order.
			if fmt.Sprint(got) != fmt.Sprint(acked) {
				bad++
				if bad <= 10 {
					t.Errorf("%s on node %d (owner %d) holds %v, acknowledged %v", name, m, where[i].owner, got, acked)
				}
			}
		}
	}
	if bad > 0 {
		t.Fatalf("%d copies differ from what was acknowledged", bad)
	}
}

// bucketIndex is the number in a burst bucket's name, "S0007" being 7.
func bucketIndex(name string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(name, "S"))
	if err != nil {
		panic("bucketIndex: " + name) // invariant: only the burst's own names reach it
	}
	return n
}
