package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 2 of the spread MVP on a cluster: the router at the doors
// of a node that routes (spread MVP note, section 2.1, B1, R2, R3).

// spreadCluster is a test cluster with a database app and a spread
// collection in it.
type spreadCluster struct {
	*testCluster
	collection string
}

// newSpreadCluster starts count nodes at factor rf and creates app.<name>
// spread through node 0, with options added to the create's.
func newSpreadCluster(t *testing.T, count, rf int, name string, options map[string]interface{}) *spreadCluster {
	t.Helper()
	tc := newTestCluster(t, count, rf)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: name}
	sc.createCollection(0, name, options)
	return sc
}

// createCollection creates app.<name> spread through node index.
func (sc *spreadCluster) createCollection(index int, name string, options map[string]interface{}) {
	sc.t.Helper()
	create := map[string]interface{}{"placement": "spread"}
	for key, value := range options {
		create[key] = value
	}
	reply := sc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: create})
	if !strings.Contains(string(reply.Payload), "spread") {
		sc.t.Fatalf("creating app.%s spread answered %q", name, reply.Payload)
	}
	// Every member of the template's set has applied the create before a test
	// reads through it.
	sc.waitHeldBy(name, sc.templateSet(name))
}

// put inserts one document through node index.
func (sc *spreadCluster) put(index int, key string, data map[string]interface{}) *transport.Frame {
	sc.t.Helper()
	return sc.frame(index, transport.OpPut, InsertRequest{Database: "app", Bucket: sc.collection, Key: key, Data: data})
}

// get reads one document through node index.
func (sc *spreadCluster) get(index int, key string) (*transport.Frame, FindResponse) {
	sc.t.Helper()
	reply := sc.frame(index, transport.OpGet, FindRequest{Database: "app", Bucket: sc.collection, Key: key})
	var found FindResponse
	if reply.Opcode != transport.OpError {
		if err := bson.Unmarshal(reply.Payload, &found); err != nil {
			sc.t.Fatalf("decoding a get's answer: %v", err)
		}
	}
	return reply, found
}

// indexOf is the index of the node whose ID is id.
func (sc *spreadCluster) indexOf(id string) int {
	sc.t.Helper()
	return nodeIndexByAddr(sc.t, sc.addrs, id)
}

// templateSet is the template's owner, then its other holders, as node
// indexes, on node 0's view; the rest of the nodes hold no copy.
func (sc *spreadCluster) templateSet(name string) []int {
	sc.t.Helper()
	c := clusterOf(sc.testCluster, 0)
	key := placementOf("app", name)
	owner, err := c.Owner(key)
	if err != nil {
		sc.t.Fatal(err)
	}
	replicas, err := c.Replicas(key)
	if err != nil {
		sc.t.Fatal(err)
	}
	set := []int{sc.indexOf(owner.ID)}
	for _, node := range replicas {
		if node.ID != owner.ID {
			set = append(set, sc.indexOf(node.ID))
		}
	}
	return set
}

// nonHolder is a node outside the template's set, or -1.
func (sc *spreadCluster) nonHolder(name string) int {
	set := sc.templateSet(name)
	for index := range sc.nodes {
		held := false
		for _, member := range set {
			held = held || member == index
		}
		if !held {
			return index
		}
	}
	return -1
}

// waitHeldBy waits until every node of indexes holds app.<name>.
func (sc *spreadCluster) waitHeldBy(name string, indexes []int) {
	sc.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for _, index := range indexes {
		for !sc.nodes[index].server.core.bucketStoreOpen("app", name) {
			if time.Now().After(deadline) {
				sc.t.Fatalf("node %d never held app.%s", index+1, name)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// localPartCount is how many documents node index's own copy of part p
// holds, read past every door: -1 when it holds no copy.
func (sc *spreadCluster) localPartCount(index, p int) int64 {
	sc.t.Helper()
	s := sc.nodes[index].server.core
	part := partName(sc.collection, p)
	if !s.bucketStoreOpen("app", part) {
		return -1
	}
	return templateCount(sc.t, s, "app", part)
}

// TestSpreadDocumentsLandOnEveryMember is test 2: 3 nodes at factor 2, 3,000
// inserts through n2, and every member owns documents. Balance is true by
// construction (the reviewer's dist/main.go); this guards the wiring: the
// router writes each document to its part's owner, wherever the client came
// in.
func TestSpreadDocumentsLandOnEveryMember(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	const total = 3000
	var wg sync.WaitGroup
	failures := make(chan string, total)
	work := make(chan int)
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				key := fmt.Sprintf("u%05d", i)
				payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"Name": key}})
				if err != nil {
					failures <- err.Error()
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				reply, err := sc.nodes[1].client.SendRequest(ctx, transport.OpPut, payload)
				cancel()
				if err != nil {
					failures <- err.Error()
				} else if reply.Opcode == transport.OpError {
					failures <- string(reply.Payload)
				}
			}
		}()
	}
	for i := 0; i < total; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Fatalf("an insert through n2 failed: %s", failure)
	}
	c := clusterOf(sc.testCluster, 0)
	owned := make([]int64, len(sc.nodes))
	var sum int64
	for p := 0; p < defaultSpreadParts; p++ {
		owner, err := c.Owner(placementOf("app", partName("User", p)))
		if err != nil {
			t.Fatal(err)
		}
		index := sc.indexOf(owner.ID)
		if count := sc.localPartCount(index, p); count > 0 {
			owned[index] += count
			sum += count
		}
	}
	for index, count := range owned {
		if count == 0 {
			t.Errorf("node %d owns no document of the collection: %v", index+1, owned)
		}
	}
	if sum != total {
		t.Errorf("the part owners hold %d documents, want %d", sum, total)
	}
	// Any node answers a point read: owner, template replica or not.
	for index := range sc.nodes {
		if _, found := sc.get(index, "u01234"); !found.Found {
			t.Errorf("get through node %d did not find the document", index+1)
		}
	}
	if got := templateCount(t, sc.nodes[sc.templateSet("User")[0]].server.core, "app", "User"); got != 1 {
		t.Errorf("the template holds %d documents, want 1 (the born set)", got)
	}
}

// TestSpreadReadAtATemplateReplicaIsRouted is B1 at factor 2: a read sent to
// a node that holds a copy of the template is not answered from that copy,
// which holds only the born set. A point read finds the document; count and
// find, a later step's fan-out, are refused, never answered 1 or with the
// born document.
func TestSpreadReadAtATemplateReplicaIsRouted(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	set := sc.templateSet("User")
	for i := 0; i < 50; i++ {
		if reply := sc.put(set[0], fmt.Sprintf("k%d", i), map[string]interface{}{"N": i}); reply.Opcode == transport.OpError {
			t.Fatalf("insert: %s", refusalText(t, reply))
		}
	}
	replica := set[1]
	sc.waitHeldBy("User", []int{replica})
	for i := 0; i < 50; i++ {
		reply, found := sc.get(replica, fmt.Sprintf("k%d", i))
		if !found.Found {
			t.Fatalf("get k%d through the template's replica answered %s %q", i, reply.Opcode, reply.Payload)
		}
	}
	// The template's replica routes count() and find() too: they cover the
	// parts (step 3), never the replica's template copy and its born set.
	if count, refused := sc.count(replica); refused != "" || count != 50 {
		t.Errorf("count through the template's replica is %d (refused %q), want 50", count, refused)
	}
	var page ScanResponse
	decodeAnswer(t, sc.frame(replica, transport.OpScan, ScanRequest{Database: "app", Bucket: "User"}), &page)
	if len(page.Documents) != 50 || page.Total != 50 {
		t.Errorf("find through the template's replica returned %d of total %d, want 50", len(page.Documents), page.Total)
	}
	for _, document := range page.Documents {
		if id, _ := document.Lookup("_id_").StringValueOK(); strings.Contains(id, unitSeparator) {
			t.Error("find through the template's replica returned the born set")
		}
	}
}

// TestSpreadKeepsTheVerbRules is test 4: the insert, upsert and bulk verbs
// keep their rules on a spread bucket, through any node.
func TestSpreadKeepsTheVerbRules(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", map[string]interface{}{
		"fields": []interface{}{map[string]interface{}{"name": "Age", "type": "int"}},
	})
	if reply := sc.put(0, "ada", map[string]interface{}{"Age": 36}); reply.Opcode == transport.OpError {
		t.Fatalf("insert ada: %s", refusalText(t, reply))
	}
	again := sc.put(1, "ada", map[string]interface{}{"Age": 37})
	if again.Opcode != transport.OpError || errorCodeOf(t, again) != transport.ErrDocumentExists {
		t.Fatalf("a second insert of ada through another node answered %s %q, want DOCUMENT_EXISTS", again.Opcode, again.Payload)
	}
	upsert := sc.frame(2, transport.OpUpsert, InsertRequest{Database: "app", Bucket: "User", Key: "ada", Data: map[string]interface{}{"Age": 38}})
	if upsert.Opcode == transport.OpError {
		t.Fatalf("upsert: %s", refusalText(t, upsert))
	}
	if _, found := sc.get(1, "ada"); !found.Found || !strings.Contains(found.Data.String(), "38") {
		t.Fatalf("after the upsert ada reads %s", found.Data)
	}

	// A BATCH_PUT that repeats an _id_ is refused whole and logs nothing.
	// Through a node with no copy of the template, whose door cannot tell the
	// bucket's type and so leaves the repeat to the router.
	before := sc.walSequences()
	repeat := sc.frame(sc.nonHolder("User"), transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: []map[string]interface{}{
		{"_id_": "r1", "Age": 1}, {"_id_": "r2", "Age": 2}, {"_id_": "r1", "Age": 3}}})
	if repeat.Opcode != transport.OpError {
		t.Fatalf("a BATCH_PUT with a repeated _id_ answered %s %q", repeat.Opcode, repeat.Payload)
	}
	if after := sc.walSequences(); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("a refused BATCH_PUT logged something: %v then %v", before, after)
	}
	for _, key := range []string{"r1", "r2"} {
		if _, found := sc.get(0, key); found.Found {
			t.Fatalf("%s of a refused BATCH_PUT was stored", key)
		}
	}

	// BATCH_UPSERT keeps the last of a repeat.
	if reply := sc.frame(2, transport.OpBatchUpsert, BatchInsertRequest{Database: "app", Bucket: "User", Data: []map[string]interface{}{
		{"_id_": "u1", "Age": 1}, {"_id_": "u1", "Age": 2}}}); reply.Opcode == transport.OpError {
		t.Fatalf("BATCH_UPSERT: %s", refusalText(t, reply))
	}
	if _, found := sc.get(0, "u1"); !found.Found || !strings.Contains(found.Data.String(), `"Age": {"$numberInt":"2"}`) && !strings.Contains(found.Data.String(), "2") {
		t.Fatalf("BATCH_UPSERT kept %s, want the last", found.Data)
	}

	// Results come back in request order across parts; one refused part is
	// not_applied, the others applied.
	bad := keyOutsideParts(t, "bad", nil)
	keys := []string{"b-1", bad, "b-2", "b-3"}
	avoid := map[int]bool{spreadPartOf(t, bad): true}
	for i, key := range keys {
		if key != bad && avoid[spreadPartOf(t, key)] {
			keys[i] = keyOutsideParts(t, key, avoid)
		}
	}
	documents := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		age := interface{}(1)
		if key == bad {
			age = "not a number"
		}
		documents = append(documents, map[string]interface{}{"_id_": key, "Age": age})
	}
	mixed := sc.frame(1, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: documents})
	if mixed.Opcode != transport.OpError {
		t.Fatalf("a BATCH_PUT with one refused part answered %s %q", mixed.Opcode, mixed.Payload)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(mixed.Payload, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.WriteOutcome != outcomeNotApplied || len(refusal.Results) != len(keys) {
		t.Fatalf("the refusal is %+v, want not_applied with %d results", refusal, len(keys))
	}
	for position, key := range keys {
		result := refusal.Results[position]
		want := outcomeApplied
		if key == bad {
			want = outcomeNotApplied
		}
		if result.ID != key || result.WriteOutcome != want {
			t.Errorf("result %d is %+v, want %s %s", position, result, key, want)
		}
		_, found := sc.get(2, key)
		if found.Found != (want == outcomeApplied) {
			t.Errorf("%s found=%v after the bulk write, want %v", key, found.Found, want == outcomeApplied)
		}
	}
	if strings.Contains(string(mixed.Payload), unitSeparator) {
		t.Errorf("the bulk refusal names a part: %q", mixed.Payload)
	}
}

// errorCodeOf is an error frame's code.
func errorCodeOf(t testing.TB, frame *transport.Frame) transport.ErrorCode {
	t.Helper()
	var refused ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refused); err != nil {
		t.Fatalf("decoding the error frame: %v", err)
	}
	return transport.ErrorCode(refused.Code)
}

// TestPartCreatedLazilyAndOnce is test 5: 50 concurrent first writes to one
// part, beside first writes to ten other parts, make the part once, with 50
// documents, and every part's mark reaches 2: marks are $max on separate
// fields, so concurrent marks of different parts never lose one another.
func TestPartCreatedLazilyAndOnce(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	target := spreadPartOf(t, "anchor")
	keys := keysInPart(t, "same", target, 50)
	others := map[int]string{}
	for i := 0; len(others) < 10; i++ {
		key := fmt.Sprintf("other-%d", i)
		if p := spreadPartOf(t, key); p != target {
			others[p] = key
		}
	}
	for _, key := range others {
		keys = append(keys, key)
	}
	var wg sync.WaitGroup
	errs := make(chan string, len(keys))
	for i, key := range keys {
		wg.Add(1)
		go func(index int, key string) {
			defer wg.Done()
			payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "User", Key: key, Data: map[string]interface{}{"N": index}})
			if err != nil {
				errs <- err.Error()
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			reply, err := sc.nodes[index%3].client.SendRequest(ctx, transport.OpPut, payload)
			if err != nil {
				errs <- err.Error()
			} else if reply.Opcode == transport.OpError {
				errs <- string(reply.Payload)
			}
		}(i, key)
	}
	wg.Wait()
	close(errs)
	for failure := range errs {
		t.Fatalf("a concurrent first write failed: %s", failure)
	}
	router := sc.nodes[sc.templateSet("User")[0]].server.core
	born, err := router.readBornSet("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	if born.state(target) != partCreated {
		t.Errorf("the shared part's mark is %d", born.state(target))
	}
	for p := range others {
		if born.state(p) != partCreated {
			t.Errorf("part %d's mark is %d after its first write: a concurrent mark lost it", p, born.state(p))
		}
	}
	for _, key := range keys {
		if _, found := sc.get(0, key); !found.Found {
			t.Errorf("%s is not found", key)
		}
	}
	owner, err := clusterOf(sc.testCluster, 0).Owner(placementOf("app", partName("User", target)))
	if err != nil {
		t.Fatal(err)
	}
	if count := sc.localPartCount(sc.indexOf(owner.ID), target); count != 50 {
		t.Errorf("the shared part holds %d documents, want 50", count)
	}
	creates := 0
	for _, node := range sc.nodes {
		creates = max(creates, walCreatesOf(t, node.server.core, partName("User", target)))
	}
	if creates != 1 {
		t.Errorf("the shared part was created %d times in a log, want once", creates)
	}
}

// walCreatesOf counts the creates of bucket in s's log.
func walCreatesOf(t testing.TB, s *Server, bucket string) int {
	t.Helper()
	entries, err := s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	creates := 0
	for _, entry := range entries {
		if entry.Bucket == bucket && transport.Opcode(entry.Op) == transport.OpCreateBucket {
			creates++
		}
	}
	return creates
}

// TestTemplateNeverLogsAClientDocument: whatever a client sends a spread
// bucket, every log holds, under the template's name, only its create and
// the router's writes of the born set, and the template's store holds only
// the born set.
func TestTemplateNeverLogsAClientDocument(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}},
	})
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("t%d", i)
		index := i % 3
		sc.put(index, key, map[string]interface{}{"N": i})
		sc.frame(index, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: key, Inc: map[string]int64{"N": 1}})
		sc.frame(index, transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "User", Key: key, Field: "Purchases", Data: map[string]interface{}{"sku": i}})
		sc.frame(index, transport.OpUpsert, InsertRequest{Database: "app", Bucket: "User", Key: key + "u", Data: map[string]interface{}{"N": i}})
		if i%4 == 0 {
			sc.frame(index, transport.OpDelete, DeleteRequest{Database: "app", Bucket: "User", Key: key})
		}
	}
	sc.frame(1, transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: []map[string]interface{}{{"N": 1}, {"N": 2}, {"N": 3}}})
	for index, node := range sc.nodes {
		entries, err := node.server.core.wal.ReadFrom(1)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Bucket != "User" {
				continue
			}
			op := transport.Opcode(entry.Op)
			bornWrite := (op == transport.OpUpdate || op == transport.OpPut) && strings.Contains(string(entry.Data), bornDocumentID)
			if op != transport.OpCreateBucket && !bornWrite {
				t.Errorf("node %d logged %s under the template's name: %q", index+1, op, entry.Data)
			}
		}
		if node.server.core.bucketStoreOpen("app", "User") {
			if got := templateCount(t, node.server.core, "app", "User"); got != 1 {
				t.Errorf("node %d's copy of the template holds %d documents, want the born set alone", index+1, got)
			}
		}
	}
}

// TestOneMemberClusterRoutes is the one-member door (B2) of a clustered node
// that was always alone: its writes and reads come in by
// Server.dispatchInFence and Cluster.readAtOneMemberDoor, and are routed.
func TestOneMemberClusterRoutes(t *testing.T) {
	sc := newSpreadCluster(t, 1, 1, "User", nil)
	core := sc.nodes[0].server.core
	if core.cluster == nil || core.cluster.Enabled() {
		t.Fatal("a one-node cluster is expected to take the one-member door")
	}
	for i := 0; i < 100; i++ {
		if reply := sc.put(0, fmt.Sprintf("m%d", i), map[string]interface{}{"N": i}); reply.Opcode == transport.OpError {
			t.Fatalf("insert: %s", refusalText(t, reply))
		}
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("m%d", i)
		if _, found := sc.get(0, key); !found.Found {
			t.Fatalf("%s is not found", key)
		}
		inPart, err := core.Find(FindRequest{Database: "app", Bucket: partName("User", spreadPartOf(t, key)), Key: key})
		if err != nil || !inPart.Found {
			t.Fatalf("%s is not in its part", key)
		}
	}
	if got := templateCount(t, core, "app", "User"); got != 1 {
		t.Fatalf("the template holds %d documents, want 1", got)
	}
}

// TestVersionedGetOfATemplateIsRefused is test 24 (note b): VERSIONED_GET is
// dispatched before any routing, so its refusal of a spread template is in
// handleVersionedGet, and the born set is never handed out.
func TestVersionedGetOfATemplateIsRefused(t *testing.T) {
	sc := newSpreadCluster(t, 2, 2, "User", nil)
	if reply := sc.put(0, "ada", map[string]interface{}{"N": 1}); reply.Opcode == transport.OpError {
		t.Fatalf("insert: %s", refusalText(t, reply))
	}
	set := sc.templateSet("User")
	holder := sc.nodes[set[0]].server.core
	asker := sc.addrs[set[1]]
	frame := transport.NewFrame(transport.OpVersionedGet, spreadMarshal(t, VersionedGetRequest{Token: sc.token, Database: "app", Bucket: "User", Key: bornDocumentID, FromNode: asker}))
	reply, err := handleVersionedGet(context.Background(), frame, holder)
	if err != nil {
		t.Fatal(err)
	}
	var answer VersionedGetResponse
	if err := bson.Unmarshal(reply.Payload, &answer); err != nil {
		t.Fatal(err)
	}
	if answer.Found || len(answer.Data) > 0 || !strings.Contains(answer.Error, "spread bucket") {
		t.Fatalf("a versioned read of the template answered %+v", answer)
	}
}

// TestBatchThroughTheRouterReachesRemoteParts is N1: a BATCH sent to the
// router runs its chain for the collection on the router with the chain's
// owner in its context, and a part on another node would be refused as
// outside the chain; the router clears it for the part's write, so every item
// reaches its part.
func TestBatchThroughTheRouterReachesRemoteParts(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "User", nil)
	router := sc.templateSet("User")[0]
	c := clusterOf(sc.testCluster, router)
	var remote []string
	for i := 0; len(remote) < 6; i++ {
		key := fmt.Sprintf("batch-%d", i)
		owner, err := c.Owner(placementOf("app", partName("User", spreadPartOf(t, key))))
		if err != nil {
			t.Fatal(err)
		}
		if owner.ID == c.self.ID {
			continue
		}
		if reply := sc.put(router, key, map[string]interface{}{"N": 1}); reply.Opcode == transport.OpError {
			t.Fatalf("insert %s: %s", key, refusalText(t, reply))
		}
		remote = append(remote, key)
	}
	items := make([]BatchItem, 0, len(remote))
	for _, key := range remote {
		items = append(items, BatchItem{Opcode: int32(transport.OpUpdate), Payload: spreadMarshal(t, UpdateRequest{Database: "app", Bucket: "User", Key: key, Set: map[string]interface{}{"N": 2}})})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reply, err := sc.nodes[router].client.SendRequest(ctx, transport.OpBatch, spreadMarshal(t, BatchRequest{Items: items}))
	if err != nil {
		t.Fatal(err)
	}
	if reply.Opcode != transport.OpBatchOK {
		t.Fatalf("the batch answered %s %q", reply.Opcode, reply.Payload)
	}
	var answers struct {
		Answers []BatchAnswer `bson:"answers"`
	}
	if err := bson.Unmarshal(reply.Payload, &answers); err != nil {
		t.Fatal(err)
	}
	for position, answer := range answers.Answers {
		if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			t.Errorf("item %d (%s) answered %s %q", position+1, remote[position], transport.Opcode(answer.Opcode), answer.Payload)
		}
	}
	for _, key := range remote {
		if _, found := sc.get(router, key); !found.Found || !strings.Contains(found.Data.String(), "2") {
			t.Errorf("%s reads %s after the batch", key, found.Data)
		}
	}
}
