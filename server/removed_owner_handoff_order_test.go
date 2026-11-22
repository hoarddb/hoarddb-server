package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0698: a removed node's commit and its successor's arrive in parallel,
// and the removed node starts its hand-off as soon as it has adopted its
// removal. Its ask to take a bucket can therefore reach the successor before
// the successor has adopted the epoch that makes it the owner. Such an ask is
// not the owner's hand-off yet, and the bucket must still be awaited at the
// successor until its copy lands (BUG-0688's invariant): a read is the retry
// naming the removed node, never "not found".

// earlyAskFixture is three nodes at factor 1 with the successor of a removed
// node's buckets held on the old epoch until released. Its proposer is the
// third member, so the commit reaches the removed node, which starts its
// hand-off, while the successor's adoption waits.
type earlyAskFixture struct {
	tc          *testCluster
	removedID   string
	successor   int
	other       int
	successorID string
	oldEpoch    uint64

	releaseAdoption func()
	proposed        chan error
}

// newEarlyAskFixture builds the cluster and the buckets: each name in names is
// created with one acknowledged entry "held", owned by node 1 before the
// removal and by the same successor after it.
func newEarlyAskFixture(t *testing.T, prefixes ...string) (*earlyAskFixture, []string) {
	t.Helper()
	tc := newTestCluster(t, 3, 1)
	t.Cleanup(tc.stop)
	before := ringWith(t, 1, tc.addrs...)
	after := ringWith(t, 1, tc.addrs[1], tc.addrs[2])
	first := bucketNamed(t, prefixes[0], func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	successorAddr := ownerOn(after, first)
	buckets := []string{first}
	for _, prefix := range prefixes[1:] {
		buckets = append(buckets, bucketNamed(t, prefix, func(name string) bool {
			return ownerOn(before, name) == tc.addrs[0] && ownerOn(after, name) == successorAddr
		}))
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range buckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 1)
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	}
	successor := nodeIndexByAddr(t, tc.addrs, successorAddr)
	f := &earlyAskFixture{
		tc:          tc,
		removedID:   tc.addrs[0],
		successor:   successor,
		other:       3 - successor,
		successorID: successorAddr,
		oldEpoch:    clusterOf(tc, successor).Epoch(),
		proposed:    make(chan error, 1),
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	f.releaseAdoption = func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(f.releaseAdoption)
	holdAdoption := func(nodeID string, epoch uint64) error {
		if nodeID == f.successorID && epoch > f.oldEpoch {
			<-release
		}
		return nil
	}
	adoptMembershipHook.Store(&holdAdoption)
	t.Cleanup(func() { adoptMembershipHook.Store(nil) })
	return f, buckets
}

// remove proposes the removal at the third member, in the background: its
// commit fan-out waits on the held successor.
func (f *earlyAskFixture) remove(t *testing.T) {
	t.Helper()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		f.proposed <- clusterOf(f.tc, f.other).proposeRemoval(ctx, f.removedID)
	}()
	t.Cleanup(func() {
		f.releaseAdoption()
		if err := <-f.proposed; err != nil {
			t.Errorf("removing node 1 through node %d: %v", f.other+1, err)
		}
	})
}

// adoptAndWait releases the successor's adoption and waits until it has the
// new epoch. It runs in a hook, on a node's goroutine, so it reports with
// Errorf.
func (f *earlyAskFixture) adoptAndWait(t *testing.T) {
	t.Helper()
	f.releaseAdoption()
	c := clusterOf(f.tc, f.successor)
	if !holdsWithin(takeOverWait, func() bool { return c.Epoch() > f.oldEpoch }) {
		t.Errorf("setup: the successor never adopted the removal once released (epoch %d)", c.Epoch())
	}
}

// readsNeverNotFound reads bucket through the successor and the third member
// and fails on any answer but the entry or a retry: the one naming the removed
// node while nothing has started, or the hand-off's own once the successor's
// take-over copies the bucket (BUG-0693's accepted unnamed retry).
func (f *earlyAskFixture) readsNeverNotFound(t *testing.T, when, bucket string) {
	t.Helper()
	get := FindRequest{Database: testDatabase, Bucket: bucket, Key: "held"}
	for _, door := range []struct {
		what  string
		index int
	}{{"a get at the successor", f.successor}, {"a get through the third member", f.other}} {
		frame := f.tc.frame(door.index, transport.OpGet, get)
		if frame.Opcode == transport.OpError {
			refusedAwaitingCopyOrRetry(t, door.what+" of "+bucket+" "+when, frame)
			if text := errorFrameText(frame); !strings.Contains(text, "Retry in a moment") {
				t.Errorf("%s of %s %s was answered %q; want a retry", door.what, bucket, when, text)
			}
			continue
		}
		foundHeld(t, door.what+" of "+bucket+" "+when, frame)
	}
}

// TestAHandOffAskedBeforeTheSuccessorAdoptsTheEpochIsStillAwaited is BUG-0698:
// three nodes at factor 1; the removed node's ask to take its bucket reaches
// the successor while the successor is still on the old epoch, where the
// removed node is the owner; the successor then adopts the removal, and the
// removed node's next ask is held. A read of the bucket at the successor and
// through the third member must be the retry naming the removed node, and the
// entry once the next ask lands. Red before the fix: the successor refused the
// early ask as a replica seeding from its owner and recorded nothing, the
// removed node counted the refusal as named and sent its notice that it had
// named every bucket, both survivors settled, and both reads answered
// `bucket "…" not found … to create it. This name was placed on … before …
// was removed`. Guard: an ask the owner did not record does not count as
// named.
func TestAHandOffAskedBeforeTheSuccessorAdoptsTheEpochIsStillAwaited(t *testing.T) {
	f, buckets := newEarlyAskFixture(t, "e0698")
	bucket := buckets[0]
	c := clusterOf(f.tc, f.successor)

	var asks atomic.Int64
	var epochAtFirstAsk atomic.Uint64
	secondAsk := make(chan struct{})
	releaseSecond := make(chan struct{})
	var secondOnce, releaseOnce sync.Once
	letSecondGo := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
	t.Cleanup(letSecondGo)
	orderAsks := func(fromID, memberID, _, name string) error {
		if fromID != f.removedID || memberID != f.successorID || name != bucket {
			return nil
		}
		if asks.Add(1) == 1 {
			epochAtFirstAsk.Store(c.Epoch())
			return nil
		}
		secondOnce.Do(func() { close(secondAsk) })
		<-releaseSecond
		return nil
	}
	askNodeToMigrateHook.Store(&orderAsks)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })
	// The copy question that follows the first ask waits for the successor's
	// adoption, so the removed node's notice, if it sends one, arrives after
	// it, as on the VMs.
	var adopted sync.Once
	adoptBeforeQuestion := func(askerID, nodeID string) error {
		if askerID == f.removedID && nodeID == f.successorID {
			adopted.Do(func() { f.adoptAndWait(t) })
		}
		return nil
	}
	askHolderHook.Store(&adoptBeforeQuestion)
	t.Cleanup(func() { askHolderHook.Store(nil) })

	f.remove(t)
	select {
	case <-secondAsk:
	case <-time.After(takeOverWait):
		t.Fatalf("setup: the removed node never asked its successor for %s a second time (%d asks)", bucket, asks.Load())
	}
	if at := epochAtFirstAsk.Load(); at != f.oldEpoch {
		t.Fatalf("setup: the first ask reached the successor at epoch %d; want the old epoch %d", at, f.oldEpoch)
	}
	closed := holdsWithin(2*time.Second, func() bool { return !c.readWindowOpen(c.Epoch()) })
	t.Logf("the settling window at the successor closed before the reads: %v", closed)
	f.readsNeverNotFound(t, "before the removed node's next ask", bucket)
	if settled, epoch := c.settled.Load(), c.Epoch(); settled >= epoch {
		t.Errorf("the successor reported itself settled at epoch %d while the removed node's next ask for %s was held and its copy had not landed",
			epoch, bucket)
	}

	letSecondGo()
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(f.tc, f.successor, testDatabase, bucket, "held") }) {
		t.Errorf("\"held\" in %s never read back through the successor once the removed node asked again", bucket)
	}
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(f.tc, f.other, testDatabase, bucket, "held") }) {
		t.Errorf("\"held\" in %s never read back through the third member once the removed node asked again", bucket)
	}
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor never settled at epoch %d once the removed node's bucket landed", c.Epoch())
	}
}

// TestAPeerAskTheOwnerDidNotRecordIsNotFlaggedTaken is the exactness of
// BUG-0698's flag: a member asks a bucket's owner to take a name no removed
// node ever owned, so the owner records no hand-off. Its refusal must not say
// the hand-off was taken, which a removed node would count as named. Red
// before the flag followed the record: every refusal from the owner's branch
// was flagged.
func TestAPeerAskTheOwnerDidNotRecordIsNotFlaggedTaken(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	t.Cleanup(tc.stop)
	placement := ringWith(t, 1, tc.addrs...)
	bucket := bucketNamed(t, "u0698", func(name string) bool { return ownerOn(placement, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	asker := clusterOf(tc, 1)
	_, members := asker.epochMembers()
	var owner ring.Node
	for _, member := range members {
		if member.ID == tc.addrs[0] {
			owner = member
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), takeOverWait)
	defer cancel()
	err := asker.askNodeToMigrate(ctx, owner, testDatabase, bucket)
	var refused *migrateRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("asking the owner to take %s, which nobody holds, was answered %v; want its refusal", bucket, err)
	}
	if refused.handOffTaken {
		t.Errorf("the owner's refusal %q says it took the hand-off of %s, which no removed node owned and it recorded nothing of", refused.message, bucket)
	}
}

// TestAnEarlyAndALateHandOffAreBothAwaited is BUG-0698 with two buckets: the
// removed node's ask for the first reaches the successor on the old epoch, its
// ask for the second after the successor adopted the removal, and its next ask
// for the first is held. Neither bucket is ever "not found" through either
// survivor, and both land.
func TestAnEarlyAndALateHandOffAreBothAwaited(t *testing.T) {
	f, buckets := newEarlyAskFixture(t, "f0698", "l0698")
	c := clusterOf(f.tc, f.successor)

	var mu sync.Mutex
	asked := make(map[string]int)
	var early string
	retryEarly := make(chan struct{})
	releaseEarly := make(chan struct{})
	var retryOnce, releaseOnce sync.Once
	letEarlyGo := func() { releaseOnce.Do(func() { close(releaseEarly) }) }
	t.Cleanup(letEarlyGo)
	orderAsks := func(fromID, memberID, _, name string) error {
		if fromID != f.removedID || memberID != f.successorID {
			return nil
		}
		mu.Lock()
		asked[name]++
		times := asked[name]
		if early == "" {
			early = name
		}
		isEarly := name == early
		mu.Unlock()
		switch {
		case isEarly && times == 1:
			if at := c.Epoch(); at != f.oldEpoch {
				t.Errorf("setup: the first ask reached the successor at epoch %d; want the old epoch %d", at, f.oldEpoch)
			}
		case isEarly:
			retryOnce.Do(func() { close(retryEarly) })
			<-releaseEarly
		case times == 1:
			f.adoptAndWait(t)
		}
		return nil
	}
	askNodeToMigrateHook.Store(&orderAsks)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })

	f.remove(t)
	select {
	case <-retryEarly:
	case <-time.After(takeOverWait):
		t.Fatalf("setup: the removed node never asked its successor for its first bucket a second time")
	}
	mu.Lock()
	earlyBucket := early
	mu.Unlock()
	late := buckets[0]
	if late == earlyBucket {
		late = buckets[1]
	}
	closed := holdsWithin(2*time.Second, func() bool { return !c.readWindowOpen(c.Epoch()) })
	t.Logf("the settling window at the successor closed before the reads: %v", closed)
	f.readsNeverNotFound(t, "asked before the successor adopted the removal", earlyBucket)
	f.readsNeverNotFound(t, "asked after the successor adopted the removal", late)

	letEarlyGo()
	for _, bucket := range []string{earlyBucket, late} {
		if !holdsWithin(takeOverWait, func() bool { return foundThrough(f.tc, f.successor, testDatabase, bucket, "held") }) {
			t.Errorf("\"held\" in %s never read back through the successor", bucket)
		}
	}
	if !holdsWithin(takeOverWait, func() bool { return c.settled.Load() >= c.Epoch() }) {
		t.Errorf("the successor never settled at epoch %d once both buckets landed", c.Epoch())
	}
}
