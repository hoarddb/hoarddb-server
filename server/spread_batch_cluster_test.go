package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 5 tests on a cluster: the doors, the failures and the log of a spread
// BATCH (spread MVP note, rev 2.1, section 5 row 5; the encryption addendum's
// S5-1).

// answerError decodes a BATCH item's ERROR answer.
func batchItemRefusal(t testing.TB, answer BatchAnswer, what string) ErrorResponse {
	t.Helper()
	return batchAnswerError(t, answer, what)
}

// TestSpreadBatchAtAOneMemberClusterMatchesAWholeBucket is the one-member
// door (B2): a clustered node that is alone on its ring routes a BATCH to its
// own parts, and answers as a whole bucket does.
func TestSpreadBatchAtAOneMemberClusterMatchesAWholeBucket(t *testing.T) {
	keys := spreadKeys(t, 120)
	sc := newSpreadCluster(t, 1, 1, "User", nil)
	seedClusterTargets(t, sc, keys)
	whole, spread := clusterTargets(t, sc, keys, 0)
	for _, scenario := range differentialScenarios(keys) {
		t.Run(scenario.name, func(t *testing.T) {
			requireSameBatch(t, whole, spread, scenario.build, keys[:14])
		})
	}
}

// TestSpreadBatchEnteredAtANonRouterReachesRemoteParts: a BATCH entered at a
// node outside the template's set is sent to the router as one sub-batch,
// whose items go on to parts on every node (the chain's owner is cleared for
// the part's write, N1); every item is applied and each document reads back
// through another node.
func TestSpreadBatchEnteredAtANonRouterReachesRemoteParts(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
	for i := 0; i < 30; i++ {
		if reply := sc.put(0, fmt.Sprintf("doc-%d", i), map[string]interface{}{"N": int64(i)}); reply.Opcode == transport.OpError {
			t.Fatalf("seeding: %s", refusalText(t, reply))
		}
	}
	router := sc.templateSet("User")[0]
	coordinator := (router + 1) % 3
	c := clusterOf(sc.testCluster, router)
	holders := map[string]bool{}
	for i := 0; i < 30; i++ {
		owner, err := c.Owner(placementOf("app", partName("User", spreadPartOf(t, fmt.Sprintf("doc-%d", i)))))
		if err != nil {
			t.Fatal(err)
		}
		holders[owner.ID] = true
	}
	if len(holders) != 3 {
		t.Fatalf("the documents' parts are on %d nodes, want all 3", len(holders))
	}
	items := make([]BatchItem, 0, 30)
	for i := 0; i < 30; i++ {
		items = append(items, feedPush(t, "User", i))
	}
	answers, refusal := decodeBatchReply(t, sc.frame(coordinator, transport.OpBatch, BatchRequest{Items: items}))
	if refusal != nil {
		t.Fatalf("the batch was refused: %+v", refusal)
	}
	for i, answer := range answers {
		if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			t.Errorf("item %d answered %s %q", i+1, transport.Opcode(answer.Opcode), answer.Payload)
		}
	}
	for i := 0; i < 30; i++ {
		length := sc.frame((router+2)%3, transport.OpFieldLen, FieldRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("doc-%d", i), Field: "Feed"})
		if got := normaliseAnswer(t, length.Payload); !strings.Contains(got, "len:1") {
			t.Errorf("doc-%d's Feed reads %s after the batch", i, got)
		}
	}
}

// TestSpreadBatchAtAnUnsettledRouterIsRefusedWithTheHandOff is step 2's second
// blocker for a BATCH: the router has not taken its template over, so its
// born set may lag. The first item is refused with the hand-off line, retryable
// and not applied, the items after it of the collection are not run, and
// nothing is stored.
func TestSpreadBatchAtAnUnsettledRouterIsRefusedWithTheHandOff(t *testing.T) {
	sc, name, victim, _, other := unsettledRouter(t)
	items := []BatchItem{
		setItem(t, name, victim, map[string]interface{}{"N": int64(2)}),
		setItem(t, name, victim, map[string]interface{}{"N": int64(3)}),
	}
	answers, refusal := decodeBatchReply(t, sc.frame(other, transport.OpBatch, BatchRequest{Items: items}))
	if refusal != nil {
		t.Fatalf("the batch was refused whole: %+v", refusal)
	}
	requireHandOff(t, &transport.Frame{Opcode: transport.Opcode(answers[0].Opcode), Payload: answers[0].Payload}, "the first item at the unsettled router")
	second := batchItemRefusal(t, answers[1], "the second item")
	if second.WriteOutcome != outcomeNotApplied || !strings.Contains(second.Message, "Not run") {
		t.Errorf("the second item answered %+v, want not run and not applied", second)
	}
}

// TestSpreadBatchRefusesEveryItemOfADroppingCollection: once a drop has begun
// (the born set says dropping), a BATCH is refused at its first item with the
// dropping line, nothing is applied, and the rest are not run.
func TestSpreadBatchRefusesEveryItemOfADroppingCollection(t *testing.T) {
	n := spreadBatchNode(t, 6)
	if resp, err := n.s.Update(UpdateRequest{Database: "app", Bucket: "User", Key: bornDocumentID, Set: map[string]interface{}{bornDroppingField: true}, Admitted: true}); err != nil || !resp.Success {
		t.Fatalf("marking the collection dropping: %+v, %v", resp, err)
	}
	answers, refusal := decodeBatchReply(t, n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: []BatchItem{feedPush(t, "User", 0), feedPush(t, "User", 1), feedLenItem(t, "User", 0)}})))
	if refusal != nil {
		t.Fatalf("the batch was refused whole: %+v", refusal)
	}
	first := batchItemRefusal(t, answers[0], "the first item")
	if !strings.Contains(first.Message, "is being dropped") || first.WriteOutcome != outcomeNotApplied {
		t.Errorf("the first item answered %+v, want the dropping line and not_applied", first)
	}
	for i := 1; i < 3; i++ {
		if later := batchItemRefusal(t, answers[i], "a later item"); !strings.Contains(later.Message, "Not run") {
			t.Errorf("item %d answered %+v, want not run", i+1, later)
		}
	}
}

// TestSpreadBatchWithAPartOwnerDown: a part owner that does not answer
// refuses its item, the collection's one chain stops there, and the items
// before it stay applied. The later items are answered not run and not
// applied, and none of them is stored.
func TestSpreadBatchWithAPartOwnerDown(t *testing.T) {
	sc := newSpreadCluster(t, 3, 1, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
	router := sc.templateSet("User")[0]
	victim := (router + 1) % 3
	c := clusterOf(sc.testCluster, router)
	var onRouter, onVictim []int
	for i := 0; len(onRouter) < 3 || len(onVictim) < 1; i++ {
		owner, err := c.Owner(placementOf("app", partName("User", spreadPartOf(t, fmt.Sprintf("doc-%d", i)))))
		if err != nil {
			t.Fatal(err)
		}
		switch owner.ID {
		case c.self.ID:
			onRouter = append(onRouter, i)
		case sc.addrs[victim]:
			onVictim = append(onVictim, i)
		default:
			continue
		}
		if reply := sc.put(router, fmt.Sprintf("doc-%d", i), map[string]interface{}{"N": int64(i)}); reply.Opcode == transport.OpError {
			t.Fatalf("seeding doc-%d: %s", i, refusalText(t, reply))
		}
	}
	sc.stopNodeKeepingData(victim)
	order := []int{onRouter[0], onRouter[1], onVictim[0], onRouter[2]}
	items := make([]BatchItem, len(order))
	for position, doc := range order {
		items[position] = feedPush(t, "User", doc)
	}
	answers, refusal := decodeBatchReply(t, sc.frame(router, transport.OpBatch, BatchRequest{Items: items}))
	if refusal != nil {
		t.Fatalf("the batch was refused whole: %+v", refusal)
	}
	for position := 0; position < 2; position++ {
		if !replySucceeded(transport.Opcode(answers[position].Opcode), answers[position].Payload) {
			t.Errorf("item %d, before the down owner's, answered %s %q", position+1, transport.Opcode(answers[position].Opcode), answers[position].Payload)
		}
	}
	down := batchItemRefusal(t, answers[2], "the item of the down owner")
	if down.WriteOutcome != outcomeUnknown && down.WriteOutcome != outcomeNotApplied {
		t.Errorf("the down owner's item says write_outcome %q, want unknown or not_applied", down.WriteOutcome)
	}
	if strings.Contains(down.Message, unitSeparator) {
		t.Errorf("the down owner's refusal shows a part name: %q", down.Message)
	}
	after := batchItemRefusal(t, answers[3], "the item after the failure")
	if after.WriteOutcome != outcomeNotApplied || !strings.Contains(after.Message, "Not run") {
		t.Errorf("the item after the failure answered %+v, want not run and not_applied", after)
	}
	length := sc.frame(router, transport.OpFieldLen, FieldRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("doc-%d", onRouter[2]), Field: "Feed"})
	if got := normaliseAnswer(t, length.Payload); !strings.Contains(got, "len:0") {
		t.Errorf("the item after the failure was stored: %s", got)
	}
}

// TestSpreadBatchLogsEachItemUnderItsPartOnly is S5-1: a routed BATCH item is
// logged exactly as the same verb on that part is, one entry naming the part
// bucket (written by its owner), and no document value reaches an entry under
// the template's name, which holds only its create and the born-set marks. The
// BATCH envelope itself is logged nowhere.
func TestSpreadBatchLogsEachItemUnderItsPartOnly(t *testing.T) {
	const secret = "SECRET-VALUE-4711"
	sc := newSpreadCluster(t, 3, 1, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
	for i := 0; i < 24; i++ {
		if reply := sc.put(0, fmt.Sprintf("doc-%d", i), map[string]interface{}{"N": int64(i)}); reply.Opcode == transport.OpError {
			t.Fatalf("seeding: %s", refusalText(t, reply))
		}
	}
	before := make([]int, len(sc.nodes))
	for index, node := range sc.nodes {
		entries, err := node.server.core.wal.ReadFrom(1)
		if err != nil {
			t.Fatal(err)
		}
		before[index] = len(entries)
	}
	items := make([]BatchItem, 0, 48)
	for i := 0; i < 24; i++ {
		push := docKeyItem(t, transport.OpFieldPush, "User", fmt.Sprintf("doc-%d", i), "Feed", "", map[string]interface{}{"secret": secret})
		items = append(items, push, setItem(t, "User", fmt.Sprintf("doc-%d", i), map[string]interface{}{"Note": secret}))
	}
	answers, refusal := decodeBatchReply(t, sc.frame(1, transport.OpBatch, BatchRequest{Items: items}))
	if refusal != nil {
		t.Fatalf("the batch was refused: %+v", refusal)
	}
	for i, answer := range answers {
		if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			t.Fatalf("item %d answered %s %q", i+1, transport.Opcode(answer.Opcode), answer.Payload)
		}
	}
	logged := map[string]int{}
	for index, node := range sc.nodes {
		entries, err := node.server.core.wal.ReadFrom(1)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries[before[index]:] {
			op := transport.Opcode(entry.Op)
			if op == transport.OpBatch {
				t.Errorf("node %d logged the BATCH envelope", index+1)
			}
			if entry.Bucket == "User" {
				t.Errorf("node %d logged %s under the template's name: %q", index+1, op, entry.Data)
				continue
			}
			if !hasPartSeparator(entry.Bucket) {
				t.Errorf("node %d logged %s under %q, which is neither the template nor a part", index+1, op, entry.Bucket)
			}
			logged[op.String()]++
		}
		for _, entry := range entries {
			if entry.Bucket == "User" && strings.Contains(string(entry.Data), secret) {
				t.Errorf("node %d holds a document value under the template's name: %q", index+1, entry.Data)
			}
		}
	}
	if logged[transport.OpFieldPush.String()] != 24 || logged[transport.OpUpdate.String()] != 24 || len(logged) != 2 {
		t.Errorf("the batch logged %v, want one FIELD_PUSH and one UPDATE per document under their parts", logged)
	}
}

// TestSpreadBatchReadsWithSmallPoolsNeverDeadlock: a read item of a BATCH holds
// one execute slot at the node that runs it, as a whole bucket's does, while
// the router reads its part from another node. That node answers a keyed read
// with no slot, so with pools of 1 on both nodes BATCHes of reads that cross
// in both directions all finish and no slot is left held.
func TestSpreadBatchReadsWithSmallPoolsNeverDeadlock(t *testing.T) {
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) { cfg.Query.Concurrency = 1 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: "Anchor"}
	names, routers := feedCollections(t, sc, "Pool", 24)
	reads := func(first, second string) []BatchItem {
		var batch []BatchItem
		for d := 0; d < 24; d++ {
			batch = append(batch, feedLenItem(t, first, d), feedLenItem(t, second, d))
		}
		return batch
	}
	var senders sync.WaitGroup
	for side, index := range routers {
		order := [2]string{names[side], names[1-side]}
		for range 3 {
			senders.Add(1)
			go func() {
				defer senders.Done()
				for round := 0; round < 6; round++ {
					answers, refusal := decodeBatchReply(t, sc.frame(index, transport.OpBatch, BatchRequest{Items: reads(order[0], order[1])}))
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
		t.Fatal("BATCHes of reads with pools of 1 did not finish")
	}
	for i, node := range tc.nodes {
		if held := node.server.core.queryInFlight(); held != 0 {
			t.Errorf("node %d holds %d execute slots after the batches", i+1, held)
		}
		if held := node.server.core.forwardInFlight(); held != 0 {
			t.Errorf("node %d holds %d forward slots after the batches", i+1, held)
		}
	}
}

// TestSpreadBatchMixedWithRemoteItemsKeepsRequestOrder: a BATCH of more items
// than the window, half of one collection run here and half of a whole bucket
// owned by another node, answers every item in request order. The router's
// local items start once the remote ones have answered (batchPlan.holdLocal),
// and the window must not hold them back.
func TestSpreadBatchMixedWithRemoteItemsKeepsRequestOrder(t *testing.T) {
	sc := newSpreadCluster(t, 2, 1, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}})
	router := sc.templateSet("User")[0]
	other := 1 - router
	whole := ""
	for i := 0; i < 500 && whole == ""; i++ {
		candidate := fmt.Sprintf("Plain%d", i)
		if owner, err := clusterOf(sc.testCluster, router).Owner(placementOf("app", candidate)); err == nil && owner.ID == sc.addrs[other] {
			whole = candidate
		}
	}
	if whole == "" {
		t.Fatal("no whole bucket name is owned by the other node")
	}
	sc.ok(router, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: whole, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "stores": []interface{}{map[string]interface{}{"name": "Feed", "engine": "fifoStore"}}}})
	for d := 0; d < 80; d++ {
		for _, bucket := range []string{"User", whole} {
			if reply := sc.frame(router, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: fmt.Sprintf("doc-%d", d), WriteConcern: "all", Data: map[string]interface{}{"N": int64(d)}}); reply.Opcode == transport.OpError {
				t.Fatalf("seeding %s/doc-%d: %s", bucket, d, refusalText(t, reply))
			}
		}
	}
	var items []BatchItem
	for d := 0; d < 80; d++ {
		items = append(items, feedPush(t, "User", d), feedPush(t, whole, d))
	}
	answers, refusal := decodeBatchReply(t, sc.frame(router, transport.OpBatch, BatchRequest{Items: items}))
	if refusal != nil {
		t.Fatalf("the batch was refused: %+v", refusal)
	}
	if len(answers) != len(items) {
		t.Fatalf("%d answers for %d items", len(answers), len(items))
	}
	for i, answer := range answers {
		if !replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			t.Fatalf("item %d answered %s %q", i+1, transport.Opcode(answer.Opcode), answer.Payload)
		}
		// Each push answers the position it took in its document's Feed: 0,
		// the first and only. A shuffled answer would put one elsewhere.
		if got := normaliseAnswer(t, answer.Payload); !strings.Contains(got, "position:0") {
			t.Errorf("item %d answered %s", i+1, got)
		}
	}
}
