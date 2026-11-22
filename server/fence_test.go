package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 5 of the shared-storage build: the apply fence belongs to Server, is
// reference-counted, and takes its mode from the opcode. These are the fence
// halves of spec T-27(a).

func handlerSend(t *testing.T, handler transport.RequestHandler, op transport.Opcode, req any) *transport.Frame {
	t.Helper()
	frame, err := handler(context.Background(), transport.NewFrame(op, mustMarshalBSON(t, req)))
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if frame.Opcode == transport.OpError {
		t.Fatalf("%s refused: %s", op, string(frame.Payload))
	}
	return frame
}

// TestStandaloneDropWaitsForInFlightWrites: on a standalone node — the
// zero-config default, which no fence covered before — a drop does not return
// while a write to the bucket is mid-apply.
func TestStandaloneDropWaitsForInFlightWrites(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash})

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func(frame *transport.Frame) {
		if frame.Opcode != transport.OpPut {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	}
	dispatchApplyHook.Store(&hook)
	t.Cleanup(func() { dispatchApplyHook.Store(nil) })

	insertDone := make(chan struct{})
	go func() {
		defer close(insertDone)
		_, _ = handler(context.Background(), transport.NewFrame(transport.OpPut,
			mustMarshalBSON(t, InsertRequest{Database: "app", Bucket: "users", Key: "k", Data: map[string]interface{}{"n": 1}})))
	}()
	<-entered

	dropDone := make(chan struct{})
	go func() {
		defer close(dropDone)
		_, _ = handler(context.Background(), transport.NewFrame(transport.OpDeleteBucket,
			mustMarshalBSON(t, DeleteBucketRequest{Database: "app", Name: "users"})))
	}()

	select {
	case <-dropDone:
		close(release)
		<-insertDone
		t.Fatal("the drop returned while a write to the bucket was still applying: it did not wait for the bucket's fence")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	<-insertDone
	select {
	case <-dropDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the drop did not return after the in-flight write finished")
	}
}

// TestFenceMapIsBoundedByInFlight is N-12: the fence map holds an entry per
// name with an operation in flight, not one per name ever written.
func TestFenceMapIsBoundedByInFlight(t *testing.T) {
	const names = 10000
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	for i := 0; i < names; i++ {
		name := fmt.Sprintf("q%05d", i)
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeFIFO})
		handlerSend(t, handler, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: name, Data: map[string]interface{}{"n": i}})
		handlerSend(t, handler, transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: name})
	}
	s.fenceMapMu.Lock()
	remaining := len(s.fences)
	s.fenceMapMu.Unlock()
	if remaining != 0 {
		t.Fatalf("after %d names were created, written and dropped with nothing in flight, the fence map holds %d entries, want 0", names, remaining)
	}
}

// TestDropInsideTheClusterFenceCompletes is C-01: the drop reaches its handler
// inside the fence applyAsOwner and applyAsReplica already hold, so a handler
// that took the fence itself would never return. Caught as a failure with a
// deadline rather than as a hung suite.
func TestDropInsideTheClusterFenceCompletes(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)

	// Writers are paced: the subject is a drop arriving while writes hold the
	// fence, not a cluster saturated until every request queues behind quorum
	// waits, which slows a drop for reasons that have nothing to do with it.
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			client := tc.dial(ownerIndex)
			defer func() { _ = client.Close() }()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, _ = client.SendRequest(ctx, transport.OpPut, mustMarshalBSON(t, InsertRequest{
					Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("w%d-%d", w, i),
					Data: map[string]interface{}{"n": i},
				}))
				cancel()
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}
	time.Sleep(200 * time.Millisecond)

	// The drop asks for w=one: its subject is the owner's and the replica's
	// applies completing inside their own holds. A write the owner accepts
	// after the drop fails on the replica and holds back the acknowledgement
	// of the batch it shares with the drop, which is a separate defect.
	dropper := tc.dial(ownerIndex)
	defer func() { _ = dropper.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frame, err := dropper.SendRequest(ctx, transport.OpDeleteBucket, mustMarshalBSON(t, map[string]interface{}{
		"database": testDatabase, "name": testBucket, "write_concern": "one",
	}))
	close(stop)
	writers.Wait()
	if err != nil {
		t.Fatalf("a drop with writers in flight did not return within 5 s on the owner: %v", err)
	}
	if frame.Opcode == transport.OpError {
		t.Fatalf("the drop was refused: %s", string(frame.Payload))
	}
	replica := tc.nodes[1-ownerIndex].server.core
	deadline := time.Now().Add(5 * time.Second)
	for replica.bucketStoreOpen(testDatabase, testBucket) {
		if time.Now().After(deadline) {
			t.Fatal("the replica did not apply the drop within 5 s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMaterializeAfterDropIsDiscarded is N-11: a pull held between receiving
// its stream and attaching it, and a drop of the name meanwhile, must not
// bring the name back when the pull resumes.
func TestMaterializeAfterDropIsDiscarded(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1}})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	targetIndex := -1
	for i, addr := range tc.addrs {
		if addr != owner.ID {
			targetIndex = i
			break
		}
	}
	target := tc.nodes[targetIndex].server.core

	held := make(chan struct{})
	release := make(chan struct{})
	hook := func(database, bucket string) {
		close(held)
		<-release
	}
	beforeMaterializeHook.Store(&hook)
	t.Cleanup(func() { beforeMaterializeHook.Store(nil) })

	pullDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pullDone <- target.cluster.pullBucket(ctx, testDatabase, testBucket, mustNode(t, target.cluster, owner.ID))
	}()
	select {
	case <-held:
	case <-time.After(15 * time.Second):
		t.Fatal("the pull never reached the point between its stream and its attach")
	}

	// The drop, taken on the target through the fenced path a local drop takes.
	if _, err := target.fencedDispatch(context.Background(), transport.NewFrame(transport.OpDeleteBucket,
		mustMarshalBSON(t, DeleteBucketRequest{Database: testDatabase, Name: testBucket}))); err != nil {
		t.Fatalf("drop on the target: %v", err)
	}
	close(release)
	pullErr := <-pullDone

	if target.bucketStoreOpen(testDatabase, testBucket) {
		t.Fatalf("a pull whose name was dropped while it was in flight brought the name back (pull returned %v)", pullErr)
	}
	if pullErr == nil {
		t.Fatal("the discarded pull reported success")
	}
}
