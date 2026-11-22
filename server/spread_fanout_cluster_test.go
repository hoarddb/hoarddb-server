package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 3 of the spread MVP on a cluster: the fan-out reads at the
// doors of a node that routes (spread MVP note, section 3; B1, B8, B9, R1).

// insertMany inserts count documents keyed prefix-<i> through node index, in
// BATCH_PUTs of 200, which the router splits by part.
func (sc *spreadCluster) insertMany(index int, prefix string, count int) []string {
	sc.t.Helper()
	keys := make([]string, 0, count)
	for start := 0; start < count; start += 200 {
		batch := make([]map[string]interface{}, 0, 200)
		for i := start; i < min(start+200, count); i++ {
			key := fmt.Sprintf("%s-%05d", prefix, i)
			keys = append(keys, key)
			batch = append(batch, map[string]interface{}{"_id_": key, "N": int32(i)})
		}
		sc.ok(index, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: sc.collection, Data: batch})
	}
	return keys
}

// count is count() of the collection through node index; refused is its
// refusal, empty when it answered.
func (sc *spreadCluster) count(index int) (count int64, refused string) {
	sc.t.Helper()
	reply := sc.frame(index, transport.OpCount, CountRequest{Database: "app", Bucket: sc.collection})
	if reply.Opcode == transport.OpError {
		return 0, refusalTextOrEmptyTB(sc.t, reply) + " " + string(reply.Payload)
	}
	var counted CountResponse
	if err := bson.Unmarshal(reply.Payload, &counted); err != nil {
		sc.t.Fatal(err)
	}
	return counted.Count, ""
}

// findAll pages through the collection with find() through node index and
// returns every _id_ it saw, failing on a refusal.
func (sc *spreadCluster) findAll(index, page int) []string {
	sc.t.Helper()
	var ids []string
	for skip := 0; ; skip += page {
		var answer ScanResponse
		decodeAnswer(sc.t, sc.frame(index, transport.OpScan, ScanRequest{Database: "app", Bucket: sc.collection, Skip: skip, Limit: page}), &answer)
		for _, document := range answer.Documents {
			id, _ := document.Lookup("_id_").StringValueOK()
			ids = append(ids, id)
		}
		if !answer.HasMore {
			return ids
		}
	}
}

// TestSpreadFindThroughAnyMemberReturnsEveryDocument is test 3 (B1) at
// factor 2: through the router, a replica of the template and a node that
// holds no copy of it, count() and a paged find() give every document once,
// and never the born set.
func TestSpreadFindThroughAnyMemberReturnsEveryDocument(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	set := sc.templateSet("User")
	keys := sc.insertMany(sc.nonHolder("User"), "doc", 3000)
	doors := map[string]int{"the router": set[0], "a template replica": set[1], "a node with no copy": sc.nonHolder("User")}
	for door, index := range doors {
		if count, refused := sc.count(index); refused != "" || count != int64(len(keys)) {
			t.Errorf("count() through %s is %d (refused %q), want %d", door, count, refused, len(keys))
		}
		ids := sc.findAll(index, 500)
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				t.Errorf("find() through %s returned %q twice", door, id)
			}
			if strings.Contains(id, unitSeparator) {
				t.Errorf("find() through %s returned the born set", door)
			}
			seen[id] = true
		}
		if len(seen) != len(keys) {
			t.Errorf("find() through %s returned %d distinct documents, want %d", door, len(seen), len(keys))
		}
	}
}

// TestSpreadReadRefusesWhenAPartOwnerIsDown is test 6: a node that owns
// parts, and not the template, is down. count() and find() are refused,
// UNAVAILABLE, naming the node, with no document returned: never a partial
// answer.
func TestSpreadReadRefusesWhenAPartOwnerIsDown(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "User", nil)
	router := sc.templateSet("User")[0]
	sc.insertMany(router, "doc", 600)
	down := (router + 1) % 3
	sc.stopNodeKeepingData(down)
	for _, op := range []transport.Opcode{transport.OpCount, transport.OpScan} {
		var request any = CountRequest{Database: "app", Bucket: "User"}
		if op == transport.OpScan {
			request = ScanRequest{Database: "app", Bucket: "User", Limit: 1000}
		}
		reply := sc.frame(router, op, request)
		text := refusalTextOrEmptyTB(t, reply)
		if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != transport.ErrUnavailable || !strings.Contains(text, sc.addrs[down]) ||
			!strings.Contains(text, "nothing was returned") || bsonNamesAPart(reply.Payload) {
			t.Errorf("%s with a part owner down answered %s %q, want UNAVAILABLE naming %s", op, reply.Opcode, reply.Payload, sc.addrs[down])
		}
	}
}

// TestFanOutReadThroughOneBatchPushAt256Parts is test 17 (B8, open item 4):
// all 256 parts on two owners, the query pool pinned to 4. A count() and a
// find() at the router send the other owner exactly one Batch push each,
// naming every part it owns, and every entry is answered.
func TestFanOutReadThroughOneBatchPushAt256Parts(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 4 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: "User"}
	sc.createCollection(0, "User", nil)
	router := sc.templateSet("User")[0]
	keys := sc.insertMany(router, "doc", 3000)
	if got := createdParts(t, sc.nodes[router].server.core); got != defaultSpreadParts {
		t.Fatalf("%d parts created, want %d", got, defaultSpreadParts)
	}
	remote := 1 - router
	remoteParts := 0
	for p := 0; p < defaultSpreadParts; p++ {
		owner, err := clusterOf(tc, router).Owner(placementOf("app", partName("User", p)))
		if err != nil {
			t.Fatal(err)
		}
		if owner.ID == tc.addrs[remote] {
			remoteParts++
		}
	}
	var mu sync.Mutex
	var pushes []int
	hook := func(entries []ReplEntry, target string) {
		mu.Lock()
		defer mu.Unlock()
		if target == tc.addrs[remote] {
			pushes = append(pushes, len(entries))
		}
	}
	forwardBatchTargetHook.Store(&hook)
	t.Cleanup(func() { forwardBatchTargetHook.Store(nil) })
	if count, refused := sc.count(router); refused != "" || count != int64(len(keys)) {
		t.Fatalf("count() is %d (refused %q), want %d", count, refused, len(keys))
	}
	var page ScanResponse
	decodeAnswer(t, sc.frame(router, transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 1000}), &page)
	if len(page.Documents) != 1000 || page.Total != int64(len(keys)) {
		t.Fatalf("find() returned %d of total %d", len(page.Documents), page.Total)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(pushes) != 2 || pushes[0] != remoteParts || pushes[1] != remoteParts {
		t.Fatalf("the router sent the other owner pushes of %v entries, want two pushes (one per read) of %d", pushes, remoteParts)
	}
	for index := range tc.nodes {
		if held := tc.nodes[index].server.core.queryInFlight(); held != 0 {
			t.Errorf("node %d holds %d execute slots after the reads", index+1, held)
		}
	}
}

// TestBatchPushOfPartCountsAnswersWithASmallPool is test 18 (R1, the
// reviewer's TestProbeBatchPushOfCountsAtTheOwner on part names): with the
// pool at 4, one Batch push of 12 COUNT entries naming parts reaches their
// owner, which answers every entry within 5 s and holds no execute slot
// afterwards.
func TestBatchPushOfPartCountsAnswersWithASmallPool(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 4 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: "User"}
	sc.createCollection(0, "User", nil)
	sc.insertMany(0, "doc", 1500)
	owner := sc.nodes[1].server.core
	var entries []ReplEntry
	for p := 0; p < defaultSpreadParts && len(entries) < 12; p++ {
		part := partName("User", p)
		if !owner.bucketStoreOpen("app", part) || !owner.cluster.actsAsOwner(placementOf("app", part)) {
			continue
		}
		entries = append(entries, ReplEntry{Op: int32(transport.OpCount), Bucket: part, Payload: spreadMarshal(t, CountRequest{Database: "app", Bucket: part})})
	}
	if len(entries) != 12 {
		t.Fatalf("node 2 owns %d parts, want at least 12", len(entries))
	}
	c := owner.cluster
	req := ReplPushRequest{Token: c.localToken(), FromNode: tc.addrs[0], Entries: entries, ReturnResponse: true, Batch: true, Epoch: c.Epoch()}
	done := make(chan *transport.Frame, 1)
	go func() {
		reply, err := handleReplPush(peerContext(), transport.NewFrame(transport.OpReplPush, spreadMarshal(t, req)), owner)
		if err != nil {
			t.Errorf("the push: %v", err)
		}
		done <- reply
	}()
	select {
	case reply := <-done:
		if reply == nil {
			t.Fatal("the push answered nothing")
		}
		if hook := reply.TakeWrittenHook(); hook != nil {
			hook()
		}
		var answered ReplPushResponse
		if err := bson.Unmarshal(reply.Payload, &answered); err != nil {
			t.Fatal(err)
		}
		if len(answered.BatchAnswers) != len(entries) {
			t.Fatalf("%d answers for %d entries: %+v", len(answered.BatchAnswers), len(entries), answered)
		}
		for i, answer := range answered.BatchAnswers {
			if !answer.Success || len(answer.Responses) != 1 || transport.Opcode(answer.Responses[0].Opcode) != transport.OpCount {
				t.Errorf("entry %d answered %+v", i, answer)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("one Batch push of 12 part counts did not answer in 5 s; execute slots in use: %d", owner.queryInFlight())
	}
	if held := owner.queryInFlight(); held != 0 {
		t.Fatalf("%d execute slots held after the push answered", held)
	}
}

// peerContext is the context of a frame from a cluster member.
func peerContext() context.Context {
	session := &transport.Session{Peer: true}
	session.SetIdentity(transport.Identity{Username: NodeUser})
	return transport.WithSession(context.Background(), session)
}

// TestFanOutsAcrossTwoRoutersNeverDeadlock is test 20 (R1, RC-2): two
// collections whose routers are the two nodes, the pool at 4, 16 concurrent
// count() calls on each through both nodes. Every call answers, with the
// right count: a router waiting on part owners holds a forward slot, never an
// execute slot one of those owners needs.
func TestFanOutsAcrossTwoRoutersNeverDeadlock(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 4 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	names := map[int]string{}
	for i := 0; len(names) < 2; i++ {
		name := fmt.Sprintf("C%d", i)
		owner, err := clusterOf(tc, 0).Owner(placementOf("app", name))
		if err != nil {
			t.Fatal(err)
		}
		index := nodeIndexByAddr(t, tc.addrs, owner.ID)
		if _, taken := names[index]; !taken {
			names[index] = name
		}
	}
	collections := map[string]int{}
	for _, name := range names {
		sc := &spreadCluster{testCluster: tc, collection: name}
		sc.createCollection(0, name, nil)
		collections[name] = len(sc.insertMany(0, "doc", 1200))
	}
	// One connection per call: a client sends one request at a time, so
	// calls sharing one would never be in flight together.
	type call struct {
		name   string
		want   int
		door   int
		client *transport.DriverClient
	}
	var calls []call
	for name, want := range collections {
		for door := range tc.nodes {
			for i := 0; i < 16; i++ {
				client := tc.dial(door)
				t.Cleanup(func() { _ = client.Close() })
				calls = append(calls, call{name: name, want: want, door: door, client: client})
			}
		}
	}
	var wg sync.WaitGroup
	errs := make(chan string, len(calls))
	start := make(chan struct{})
	for _, c := range calls {
		name, want, door := c.name, c.want, c.door
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			reply, err := c.client.SendRequest(ctx, transport.OpCount, spreadMarshal(t, CountRequest{Database: "app", Bucket: name}))
			if err != nil {
				errs <- fmt.Sprintf("count of %s through node %d: %v", name, door+1, err)
				return
			}
			var counted CountResponse
			if reply.Opcode == transport.OpError || bson.Unmarshal(reply.Payload, &counted) != nil || counted.Count != int64(want) {
				errs <- fmt.Sprintf("count of %s through node %d answered %s %q, want %d", name, door+1, reply.Opcode, reply.Payload, want)
			}
		}()
	}
	close(start)
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		t.Fatal("64 concurrent count() calls over two routers did not all answer in 60 s: the fan-outs deadlocked")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for index := range tc.nodes {
		if held := tc.nodes[index].server.core.queryInFlight(); held != 0 {
			t.Errorf("node %d holds %d execute slots after every call answered", index+1, held)
		}
	}
}

// TestRouterLostAfterAMarkKeepsEveryDocument is test 22 (R2) at factor 3:
// first writes acknowledged to 20 parts, the router lost straight after. The
// new router's count(), find() and point reads see every acknowledged
// document, and a re-insert of each is DOCUMENT_EXISTS.
func TestRouterLostAfterAMarkKeepsEveryDocument(t *testing.T) {
	sc := newSpreadCluster(t, 3, 3, "User", nil)
	router := sc.templateSet("User")[0]
	entry := (router + 1) % 3
	acked := make([]string, 0, 20)
	for p := 0; len(acked) < 20; p += 11 {
		key := keysInPart(t, fmt.Sprintf("first%d", p), p, 1)[0]
		if reply := sc.put(entry, key, map[string]interface{}{"N": int32(p)}); reply.Opcode == transport.OpError {
			t.Fatalf("insert %s: %s", key, refusalText(t, reply))
		}
		acked = append(acked, key)
	}
	sc.stopNodeKeepingData(router)
	removeNodeFrom(t, sc.testCluster, entry, router)
	newRouter, err := clusterOf(sc.testCluster, entry).Owner(placementOf("app", "User"))
	if err != nil {
		t.Fatal(err)
	}
	waitTakenOver(t, sc.nodes[sc.indexOf(newRouter.ID)].server.core, "app", "User")
	spreadEventually(t, 60*time.Second, "count() through the new router sees every acknowledged document", func() bool {
		count, refused := sc.count(entry)
		return refused == "" && count == int64(len(acked))
	})
	spreadEventually(t, 60*time.Second, "find() through the new router sees every acknowledged document", func() bool {
		var page ScanResponse
		reply := sc.frame(entry, transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 100})
		return reply.Opcode != transport.OpError && bson.Unmarshal(reply.Payload, &page) == nil && len(page.Documents) == len(acked)
	})
	for _, key := range acked {
		if _, found := sc.get(entry, key); !found.Found {
			t.Errorf("%s is not found through the new router", key)
		}
		again := sc.put(entry, key, map[string]interface{}{"N": 0})
		if again.Opcode != transport.OpError || errorCodeOf(t, again) != transport.ErrDocumentExists {
			t.Errorf("re-inserting the acknowledged %s answered %s %q, want DOCUMENT_EXISTS", key, again.Opcode, again.Payload)
		}
	}
}

// TestFanOutAtAnUnsettledRouterIsRetried is the step 2 verifier's carried
// item 1: a describe, count() or find() that reaches a template owner which
// has not taken the template over is refused with the hand-off, never
// answered from the template's copy, whose born set may lack a part and whose
// store holds no client document.
func TestFanOutAtAnUnsettledRouterIsRetried(t *testing.T) {
	sc, name, _, lag, other := unsettledRouter(t)
	for _, door := range []int{lag, other} {
		for label, read := range map[string]struct {
			op      transport.Opcode
			request any
		}{
			"describe": {transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: name}},
			"count":    {transport.OpCount, CountRequest{Database: "app", Bucket: name}},
			"find":     {transport.OpScan, ScanRequest{Database: "app", Bucket: name}},
		} {
			reply := sc.frame(door, read.op, read.request)
			text := refusalTextOrEmptyTB(t, reply)
			if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != transport.ErrUnavailable || strings.Contains(text, "not found") {
				t.Errorf("%s through node %d at an unsettled router answered %s %q, want the hand-off", label, door+1, reply.Opcode, reply.Payload)
			}
		}
	}
}

// TestDescribeWithoutTheRouterNeverAnswersFromATemplateCopy: with the router
// down, a describe through a template replica is refused, never answered
// from the replica's copy of the template (the born set, counted as the
// collection's documents) as an unreachable whole bucket's owner is.
func TestDescribeWithoutTheRouterNeverAnswersFromATemplateCopy(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	set := sc.templateSet("User")
	sc.insertMany(set[1], "doc", 200)
	sc.stopNodeKeepingData(set[0])
	reply := sc.frame(set[1], transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: "User"})
	if reply.Opcode != transport.OpError {
		t.Fatalf("describe through a template replica with the router down answered %s %q, want a refusal", reply.Opcode, reply.Payload)
	}
}
