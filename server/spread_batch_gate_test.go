package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for the router gate across a BATCH (spread MVP note, rev 2.1, section
// 2.3 and row 5 of section 5): the gate is taken once per BATCH at a router,
// shared, held until its last item ends, never re-entered, and never taken
// while a fence is held or while an answer from another node is awaited.

// spreadBatchNode is a zero-config node with app.User spread, a Feed store,
// and documents doc-0 to doc-<count-1>.
func spreadBatchNode(t *testing.T, documents int) *spreadNode {
	t.Helper()
	n := newSpreadNode(t, map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
	for i := 0; i < documents; i++ {
		reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("doc-%d", i), Data: map[string]interface{}{"N": int64(i)}})
		if reply.Opcode == transport.OpError {
			t.Fatalf("seeding doc-%d: %s", i, refusalText(t, reply))
		}
	}
	return n
}

// feedPush is a push to doc-<i>'s Feed in bucket.
func feedPush(t testing.TB, bucket string, i int) BatchItem {
	return docKeyItem(t, transport.OpFieldPush, bucket, fmt.Sprintf("doc-%d", i), "Feed", "", map[string]interface{}{"n": int64(i)})
}

// feedLenItem is a len of doc-<i>'s Feed in bucket.
func feedLenItem(t testing.TB, bucket string, i int) BatchItem {
	return docKeyItem(t, transport.OpFieldLen, bucket, fmt.Sprintf("doc-%d", i), "Feed", "", nil)
}

// countGateTakes counts the shared and exclusive takes of key's gate until
// the test ends.
func countGateTakes(t *testing.T, key placementKey) (shared, exclusive *atomic.Int64) {
	t.Helper()
	shared, exclusive = new(atomic.Int64), new(atomic.Int64)
	hook := func(taken placementKey, isExclusive bool) {
		switch {
		case taken != key:
		case isExclusive:
			exclusive.Add(1)
		default:
			shared.Add(1)
		}
	}
	routerGateTakenHook.Store(&hook)
	t.Cleanup(func() { routerGateTakenHook.Store(nil) })
	return shared, exclusive
}

// mixedBatch is writes, reads and an update of the collection's documents.
func mixedBatch(t testing.TB, bucket string, count int) []BatchItem {
	items := make([]BatchItem, 0, count)
	for i := 0; len(items) < count; i++ {
		switch i % 3 {
		case 0:
			items = append(items, feedPush(t, bucket, i))
		case 1:
			items = append(items, feedLenItem(t, bucket, i-1))
		default:
			items = append(items, incrementItem(t, bucket, fmt.Sprintf("doc-%d", i), "N"))
		}
	}
	return items
}

// TestSpreadBatchTakesTheRouterGateOnce: a BATCH of 12 items of one
// collection takes the collection's gate once, shared, where it took it
// once per item.
func TestSpreadBatchTakesTheRouterGateOnce(t *testing.T) {
	n := spreadBatchNode(t, 12)
	shared, exclusive := countGateTakes(t, placementOf("app", "User"))
	answers, refusal := decodeBatchReply(t, n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: mixedBatch(t, "User", 12)})))
	if refusal != nil {
		t.Fatalf("the batch was refused: %+v", refusal)
	}
	for i, answer := range answers {
		if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			t.Errorf("item %d answered %s %q", i+1, transport.Opcode(answer.Opcode), answer.Payload)
		}
	}
	if got := shared.Load(); got != 1 {
		t.Errorf("the batch took the gate %d times, want 1", got)
	}
	if exclusive.Load() != 0 {
		t.Errorf("the batch took the gate exclusively")
	}
	if held := n.s.routerGates.inFlight(); held != 0 {
		t.Errorf("%d gates are left held after the batch", held)
	}
}

// TestSpreadBatchHoldsTheGateUntilItsLastItem: while a BATCH is between two
// items, an exclusive taker (a drop) waits for it, and takes the gate when the
// batch has answered: the batch sees the collection wholly before or wholly
// after the drop, never half.
func TestSpreadBatchHoldsTheGateUntilItsLastItem(t *testing.T) {
	n := spreadBatchNode(t, 12)
	key := placementOf("app", "User")
	reached := make(chan struct{})
	proceed := make(chan struct{})
	var taken atomic.Int64
	hook := func(int) {
		if taken.Add(1) == 3 {
			close(reached)
			<-proceed
		}
	}
	batchSlotTakenHook.Store(&hook)
	t.Cleanup(func() { batchSlotTakenHook.Store(nil) })

	answered := make(chan struct{})
	go func() {
		defer close(answered)
		answers, refusal := decodeBatchReply(t, n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: mixedBatch(t, "User", 6)})))
		if refusal != nil || len(answers) != 6 {
			t.Errorf("the batch answered %d items, refusal %+v", len(answers), refusal)
		}
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the batch never reached its third item")
	}
	dropTakerHas := make(chan struct{})
	go func() {
		release := n.s.holdRouterGate(key, true)
		close(dropTakerHas)
		release()
	}()
	select {
	case <-dropTakerHas:
		t.Fatal("an exclusive taker got the gate between two items of a BATCH")
	case <-time.After(150 * time.Millisecond):
	}
	// A lone read arriving behind the waiting taker waits too (the gate
	// prefers writers) and goes on once the batch and the taker are done.
	loneDone := make(chan struct{})
	go func() {
		defer close(loneDone)
		if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: "doc-1"})); !found.Found {
			t.Errorf("the lone read found nothing")
		}
	}()
	close(proceed)
	for name, done := range map[string]chan struct{}{"batch": answered, "drop taker": dropTakerHas, "lone read": loneDone} {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("the %s never finished", name)
		}
	}
}

// TestSpreadBatchRacingWritesReadsAndDropsNeverDeadlocks runs BATCHes, lone
// writes, a fan-out read and exclusive takers (a drop stand-in) together
// under the race detector: all finish, no gate and no query slot is left.
func TestSpreadBatchRacingWritesReadsAndDropsNeverDeadlocks(t *testing.T) {
	n := spreadBatchNode(t, 60)
	key := placementOf("app", "User")
	deadline := time.Now().Add(60 * time.Second)
	stop := make(chan struct{})
	var takers sync.WaitGroup
	for range 2 {
		takers.Add(1)
		go func() {
			defer takers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				release := n.s.holdRouterGate(key, true)
				time.Sleep(200 * time.Microsecond)
				release()
			}
		}()
	}
	var workers sync.WaitGroup
	run := func(work func(round int)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for round := 0; round < 25; round++ {
				work(round)
			}
		}()
	}
	for range 4 {
		run(func(round int) {
			answers, refusal := decodeBatchReply(t, n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: mixedBatch(t, "User", 18)})))
			if refusal != nil || len(answers) != 18 {
				t.Errorf("a batch answered %d items, refusal %+v", len(answers), refusal)
			}
		})
	}
	for range 3 {
		run(func(round int) {
			n.send(transport.OpUpsert, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("doc-%d", round%60), Data: map[string]interface{}{"N": int64(round)}})
		})
	}
	run(func(round int) {
		if reply := n.send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"}); reply.Opcode == transport.OpError {
			t.Errorf("count: %s", refusalText(t, reply))
		}
	})
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Until(deadline)):
		t.Fatal("the batches, writes, reads and drop takers did not finish: a deadlock")
	}
	close(stop)
	takers.Wait()
	if held := n.s.routerGates.inFlight(); held != 0 {
		t.Errorf("%d gates are left held", held)
	}
	if slots := n.s.queryInFlight(); slots != 0 {
		t.Errorf("%d execute slots are left held", slots)
	}
	if slots := n.s.forwardInFlight(); slots != 0 {
		t.Errorf("%d forward slots are left held", slots)
	}
	_ = context.Background
}

// feedCollections creates spread collections app.<prefix>0.. until two of
// them have different routers, each with a Feed store and documents doc-0 to
// doc-<documents-1>, and returns the two names and their routers' indexes.
func feedCollections(t *testing.T, sc *spreadCluster, prefix string, documents int) (names [2]string, routers [2]int) {
	t.Helper()
	found := 0
	for i := 0; found < 2; i++ {
		if i > 12 {
			t.Fatal("twelve collections had one router; the test needs two")
		}
		name := fmt.Sprintf("%s%d", prefix, i)
		sc.createCollection(0, name, map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
		router := sc.templateSet(name)[0]
		if found == 1 && router == routers[0] {
			continue
		}
		names[found], routers[found] = name, router
		found++
		for d := 0; d < documents; d++ {
			reply := sc.frame(0, transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: fmt.Sprintf("doc-%d", d), WriteConcern: "all",
				Data: map[string]interface{}{"N": int64(d)}})
			if reply.Opcode == transport.OpError {
				t.Fatalf("seeding %s/doc-%d: %s", name, d, refusalText(t, reply))
			}
		}
	}
	return names, routers
}

// TestSpreadBatchThroughAnOutsiderTakesTheRouterGateOnce: a BATCH entered at
// a node outside the template's set reaches the router as one sub-batch,
// which takes the gate once for all its items.
func TestSpreadBatchThroughAnOutsiderTakesTheRouterGateOnce(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
	for i := 0; i < 12; i++ {
		if reply := sc.put(0, fmt.Sprintf("doc-%d", i), map[string]interface{}{"N": int64(i)}); reply.Opcode == transport.OpError {
			t.Fatalf("seeding: %s", refusalText(t, reply))
		}
	}
	outsider := sc.nonHolder("User")
	if outsider < 0 {
		t.Skip("every node holds the template")
	}
	shared, _ := countGateTakes(t, placementOf("app", "User"))
	answers, refusal := decodeBatchReply(t, sc.frame(outsider, transport.OpBatch, BatchRequest{Items: mixedBatch(t, "User", 12)}))
	if refusal != nil {
		t.Fatalf("the batch was refused: %+v", refusal)
	}
	for i, answer := range answers {
		if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			t.Errorf("item %d answered %s %q", i+1, transport.Opcode(answer.Opcode), answer.Payload)
		}
	}
	if got := shared.Load(); got != 1 {
		t.Errorf("the router took the gate %d times for the batch, want 1", got)
	}
}

// TestSpreadBatchesAcrossTwoRoutersNeverDeadlock is the cross-node case of
// the gate: collection A is routed at one node and B at another, BATCHes of
// both collections enter at each router, and exclusive takers (drops) queue on
// both gates all the while. A batch that held its own router's gate while it
// waited for the other router's would close a cycle through the two queued
// takers; all must finish.
func TestSpreadBatchesAcrossTwoRoutersNeverDeadlock(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "Anchor", nil)
	names, routers := feedCollections(t, sc, "Cross", 24)
	// Each item takes a moment, so a BATCH holds its gates long enough for
	// the crossing to happen.
	slow := func(int) { time.Sleep(2 * time.Millisecond) }
	batchSlotTakenHook.Store(&slow)
	t.Cleanup(func() { batchSlotTakenHook.Store(nil) })
	stop := make(chan struct{})
	var takers sync.WaitGroup
	for _, index := range routers {
		for _, name := range names {
			takers.Add(1)
			go func() {
				defer takers.Done()
				s := sc.nodes[index].server.core
				for {
					select {
					case <-stop:
						return
					default:
					}
					release := s.holdRouterGate(placementOf("app", name), true)
					time.Sleep(time.Millisecond)
					release()
				}
			}()
		}
	}
	items := func(first, second string) []BatchItem {
		var batch []BatchItem
		for d := 0; d < 12; d++ {
			batch = append(batch, feedPush(t, first, d), feedPush(t, second, d))
		}
		return batch
	}
	var senders sync.WaitGroup
	for side, index := range routers {
		order := [2]string{names[side], names[1-side]}
		for range 4 {
			senders.Add(1)
			go func() {
				defer senders.Done()
				for round := 0; round < 10; round++ {
					reply, err := sc.nodes[index].client.SendRequest(context.Background(), transport.OpBatch, spreadMarshal(t, BatchRequest{Items: items(order[0], order[1])}))
					if err != nil {
						t.Errorf("batch at node %d: %v", index+1, err)
						return
					}
					answers, refusal := decodeBatchReply(t, reply)
					if refusal != nil {
						t.Errorf("batch at node %d refused: %+v", index+1, refusal)
						return
					}
					for i, answer := range answers {
						if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
							t.Errorf("item %d answered %s %q", i+1, transport.Opcode(answer.Opcode), answer.Payload)
							return
						}
					}
				}
			}()
		}
	}
	finished := make(chan struct{})
	go func() { senders.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(90 * time.Second):
		t.Fatal("batches across two routers with queued drop takers did not finish: a deadlock")
	}
	close(stop)
	takers.Wait()
	for i, node := range sc.nodes {
		if held := node.server.core.routerGates.inFlight(); held != 0 {
			t.Errorf("node %d has %d gates left held", i+1, held)
		}
	}
}

// TestSpreadBatchesNamingTwoCollectionsInOppositeOrdersNeverDeadlock: BATCHes
// that name the same two collections in opposite orders take the two gates in
// one order (the key order), so with a queued drop on each gate none holds
// one while it waits for the other. Taken in the order of the items, the
// first BATCH holds A and waits for B behind B's queued drop, which waits for
// the second BATCH, which holds B and waits for A behind A's queued drop.
func TestSpreadBatchesNamingTwoCollectionsInOppositeOrdersNeverDeadlock(t *testing.T) {
	n := spreadBatchNode(t, 12)
	stores := map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}}
	for _, name := range []string{"Second"} {
		create := map[string]interface{}{"placement": "spread"}
		for key, value := range stores {
			create[key] = value
		}
		if reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: create}); reply.Opcode == transport.OpError {
			t.Fatalf("creating %s: %s", name, refusalText(t, reply))
		}
		for d := 0; d < 12; d++ {
			if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: fmt.Sprintf("doc-%d", d), Data: map[string]interface{}{"N": int64(d)}}); reply.Opcode == transport.OpError {
				t.Fatalf("seeding %s: %s", name, refusalText(t, reply))
			}
		}
	}
	buckets := [2]string{"User", "Second"}
	stop := make(chan struct{})
	var takers sync.WaitGroup
	for _, name := range buckets {
		takers.Add(1)
		go func() {
			defer takers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				release := n.s.holdRouterGate(placementOf("app", name), true)
				time.Sleep(300 * time.Microsecond)
				release()
			}
		}()
	}
	var senders sync.WaitGroup
	for side := range buckets {
		order := [2]string{buckets[side], buckets[1-side]}
		for range 3 {
			senders.Add(1)
			go func() {
				defer senders.Done()
				for round := 0; round < 40; round++ {
					items := []BatchItem{feedPush(t, order[0], 0), feedPush(t, order[1], 1), feedLenItem(t, order[0], 0), feedLenItem(t, order[1], 1)}
					if _, refusal := decodeBatchReply(t, n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: items}))); refusal != nil {
						t.Errorf("a batch was refused: %+v", refusal)
						return
					}
				}
			}()
		}
	}
	finished := make(chan struct{})
	go func() { senders.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		t.Fatal("BATCHes naming two collections in opposite orders did not finish: a deadlock")
	}
	close(stop)
	takers.Wait()
	if held := n.s.routerGates.inFlight(); held != 0 {
		t.Errorf("%d gates are left held", held)
	}
}

// TestSpreadBatchHoldsNoGateWhileItsRemoteItemsAreAwaited is the cross-node
// cycle made deterministic. Node B's gate for collection B is held shared by
// a stand-in for a BATCH of node B, whose own sub-batch needs node A's gate
// for collection A; a drop is queued on B's gate. A BATCH entered at node A
// names both collections: its sub-batch for B waits behind that drop. If node
// A held A's gate for the BATCH meanwhile, a drop queued on A's gate would
// wait for the BATCH, B's stand-in would wait behind that drop, and the ring
// of waits would never end. Node A takes its gate only after B has answered,
// so the drop on A's gate is granted at once.
func TestSpreadBatchHoldsNoGateWhileItsRemoteItemsAreAwaited(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "Anchor", nil)
	names, routers := feedCollections(t, sc, "Cyc", 4)
	a, b := sc.nodes[routers[0]].server.core, sc.nodes[routers[1]].server.core
	keyA, keyB := placementOf("app", names[0]), placementOf("app", names[1])
	var bTakes atomic.Int64
	hook := func(key placementKey, exclusive bool) {
		if key == keyB && !exclusive {
			bTakes.Add(1)
		}
	}
	routerGateTakenHook.Store(&hook)
	t.Cleanup(func() { routerGateTakenHook.Store(nil) })

	releaseStandIn := b.holdRouterGate(keyB, false)
	dropB := make(chan struct{})
	go func() {
		release := b.holdRouterGate(keyB, true)
		release()
		close(dropB)
	}()
	time.Sleep(50 * time.Millisecond)
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		answers, refusal := decodeBatchReply(t, sc.frame(routers[0], transport.OpBatch, BatchRequest{Items: []BatchItem{feedPush(t, names[0], 0), feedPush(t, names[1], 0)}}))
		if refusal != nil || len(answers) != 2 {
			t.Errorf("the batch answered %d items, refusal %+v", len(answers), refusal)
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for bTakes.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the batch's sub-batch never reached node B's gate")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	dropA := make(chan struct{})
	go func() {
		release := a.holdRouterGate(keyA, true)
		release()
		close(dropA)
	}()
	time.Sleep(50 * time.Millisecond)
	standInDone := make(chan struct{})
	go func() {
		release := a.holdRouterGate(keyA, false)
		release()
		releaseStandIn()
		close(standInDone)
	}()
	for name, done := range map[string]chan struct{}{"drop of A": dropA, "node B's stand-in": standInDone, "drop of B": dropB, "the batch": answered} {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatalf("%s never finished: the batch holds a gate while it waits for another node's", name)
		}
	}
}
