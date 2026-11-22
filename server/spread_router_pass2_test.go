package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 2's second pass (spread MVP note rev 2.1): the doors re-check, inside
// the fence, that a write which passed the router's check as a whole bucket
// did not meet a spread template, and a router that has not taken its template
// over never answers a write from a born set that may lag. Adopted from the
// independent verifier's probes.

// TestDropRecreateSpreadNeverLandsAWriteInTheTemplate: a put to a whole User
// passes the router's check (not spread), waits at the owner's fence while
// User is dropped and created again spread, then reaches the fence. Inside it
// the owner sees a spread template and refuses the write, retryable and not
// applied: it never lands in the template's store, where it would be
// acknowledged and then unreadable once its part exists.
func TestDropRecreateSpreadNeverLandsAWriteInTheTemplate(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole"}})
	ownerNode, err := clusterOf(tc, 0).Owner(placementOf("app", "User"))
	if err != nil {
		t.Fatal(err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerNode.ID)
	owner := clusterOf(tc, ownerIndex)
	waitForServing(t, tc, "app", "User", 2)
	held, release := holdAt(t, &beforeOwnerFenceHook, owner.self.ID, transport.OpPut, "User")
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	write := sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "victim", Data: map[string]interface{}{"N": 1}})
	awaitHeld(t, held, "the write")
	tc.ok(ownerIndex, transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: "User"})
	tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "spread"}})
	close(release)
	released = true
	var answer backgroundAnswer
	select {
	case answer = <-write:
	case <-time.After(60 * time.Second):
		t.Fatal("the write was never answered")
	}
	core := tc.nodes[ownerIndex].server.core
	if !core.spreadTemplateHeld("app", "User") {
		t.Fatal("the re-create did not land spread: the test proves nothing")
	}
	if answer.err == nil && answer.frame.Opcode != transport.OpError {
		t.Errorf("the write that met a spread template at its fence was acknowledged: %s", answer.payload())
	}
	if answer.err == nil && answer.frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		if err := bson.Unmarshal(answer.frame.Payload, &refusal); err != nil {
			t.Fatal(err)
		}
		if refusal.WriteOutcome != outcomeNotApplied || transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
			t.Errorf("the refusal is %+v, want UNAVAILABLE and not_applied", refusal)
		}
	}
	inTemplate, err := core.Find(FindRequest{Database: "app", Bucket: "User", Key: "victim"})
	if err != nil {
		t.Fatal(err)
	}
	if inTemplate.Found {
		t.Fatal("a client document was stored in the spread template's store")
	}
}

// TestDropRecreateSpreadAtTheOneMemberDoor is the same race at the
// one-member door (Server.dispatchInFence): the router's check runs before the
// fence, so the fence re-checks.
func TestDropRecreateSpreadAtTheOneMemberDoor(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	send := func(op transport.Opcode, request any) *transport.Frame {
		t.Helper()
		reply, err := handler(context.Background(), transport.NewFrame(op, spreadMarshal(t, request)))
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	send(transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	if reply := send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole"}}); reply.Opcode == transport.OpError {
		t.Fatalf("create: %s", refusalText(t, reply))
	}
	held, release := make(chan struct{}), make(chan struct{})
	hook := func(frame *transport.Frame) {
		if frame.Opcode == transport.OpPut {
			close(held)
			<-release
		}
	}
	beforeOneMemberFenceHook.Store(&hook)
	t.Cleanup(func() { beforeOneMemberFenceHook.Store(nil) })
	answered := make(chan *transport.Frame, 1)
	go func() {
		reply, err := handler(context.Background(), transport.NewFrame(transport.OpPut,
			spreadMarshal(t, InsertRequest{Database: "app", Bucket: "User", Key: "victim", Data: map[string]interface{}{"N": 1}})))
		if err != nil {
			reply = errorFrame(transport.NewFrame(transport.OpPut, nil), err)
		}
		answered <- reply
	}()
	<-held
	beforeOneMemberFenceHook.Store(nil)
	send(transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: "User"})
	if reply := send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "spread"}}); reply.Opcode == transport.OpError {
		t.Fatalf("re-create: %s", refusalText(t, reply))
	}
	close(release)
	var reply *transport.Frame
	select {
	case reply = <-answered:
	case <-time.After(30 * time.Second):
		t.Fatal("the write was never answered")
	}
	if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != transport.ErrUnavailable {
		t.Errorf("the write that met a spread template at its fence answered %s %q", reply.Opcode, reply.Payload)
	}
	if inTemplate, err := s.Find(FindRequest{Database: "app", Bucket: "User", Key: "victim"}); err != nil || inTemplate.Found {
		t.Fatalf("a client document was stored in the spread template's store: %+v, %v", inTemplate, err)
	}
}

// unsettledRouter sets up R2's quorum at factor 3 against a lagging copy: a
// collection owned by node o, a document written while node lag was down (its
// part's marks confirmed by 2 of 3 copies, lag's born set short of them), then
// o lost and lag back with its take-over paused, so lag is the template's
// owner on the members' view and has not taken it over. It returns the
// cluster, the collection, the document's key, lag and the third node.
func unsettledRouter(t *testing.T) (sc *spreadCluster, name, victim string, lag, other int) {
	t.Helper()
	sc = newSpreadCluster(t, 3, 3, "Seed", nil)
	c0 := clusterOf(sc.testCluster, 0)
	seedOwner, err := c0.Owner(placementOf("app", "Seed"))
	if err != nil {
		t.Fatal(err)
	}
	o := sc.indexOf(seedOwner.ID)
	lag = (o + 1) % 3
	other = 3 - o - lag
	// Collections owned by o: whichever moves to lag once o is removed is the
	// one under test.
	var names []string
	for i := 0; len(names) < 24 && i < 2000; i++ {
		candidate := fmt.Sprintf("C%d", i)
		owner, err := c0.Owner(placementOf("app", candidate))
		if err != nil {
			t.Fatal(err)
		}
		if owner.ID == seedOwner.ID {
			names = append(names, candidate)
		}
	}
	victims := map[string]string{}
	for _, candidate := range names {
		sc.createCollection(0, candidate, nil)
		for i := 0; ; i++ {
			key := fmt.Sprintf("victim-%d", i)
			partOwner, err := c0.Owner(placementOf("app", partName(candidate, spreadPartOf(t, key))))
			if err != nil {
				t.Fatal(err)
			}
			if partOwner.ID == sc.addrs[other] {
				victims[candidate] = key
				break
			}
		}
	}
	baseLag := sc.stopNodeKeepingData(lag)
	for _, candidate := range names {
		reply := sc.frame(other, transport.OpPut, InsertRequest{Database: "app", Bucket: candidate, Key: victims[candidate], Data: map[string]interface{}{"N": 1}})
		if reply.Opcode == transport.OpError {
			t.Fatalf("insert into %s with one copy down: %s", candidate, refusalText(t, reply))
		}
	}
	sc.stopNodeKeepingData(o)
	lagID := sc.addrs[lag]
	paused := func(nodeID string) bool { return nodeID == lagID }
	takeOverPausedHook.Store(&paused)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	sc.nodes[lag] = sc.startNode(lag, baseLag, sc.token)
	removeNodeFrom(t, sc.testCluster, other, o)
	lagCluster := clusterOf(sc.testCluster, lag)
	spreadEventually(t, 20*time.Second, "the lagging node learns the removal", func() bool {
		owner, err := lagCluster.Owner(placementOf("app", "Seed"))
		return err == nil && owner.ID != seedOwner.ID
	})
	moved := 0
	for _, candidate := range names {
		owner, err := clusterOf(sc.testCluster, other).Owner(placementOf("app", candidate))
		if err != nil {
			t.Fatal(err)
		}
		if owner.ID != lagID {
			continue
		}
		moved++
		born, err := sc.nodes[lag].server.core.readBornSet("app", candidate)
		if err != nil {
			t.Fatal(err)
		}
		if born.state(spreadPartOf(t, victims[candidate])) == partCreated {
			continue
		}
		return sc, candidate, victims[candidate], lag, other
	}
	t.Fatalf("of %d collections, %d moved to the lagging node and none has its born set short: the test proves nothing", len(names), moved)
	return nil, "", "", 0, 0
}

// requireHandOff fails unless reply is the hand-off refusal: retryable, not
// applied, and never "not found".
func requireHandOff(t *testing.T, reply *transport.Frame, what string) {
	t.Helper()
	if reply.Opcode != transport.OpError {
		t.Fatalf("%s answered %s %q, want the hand-off refusal", what, reply.Opcode, reply.Payload)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(reply.Payload, &refusal); err != nil {
		t.Fatal(err)
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable || refusal.WriteOutcome != outcomeNotApplied || strings.Contains(refusal.Message, "not found") {
		t.Fatalf("%s answered %+v, want UNAVAILABLE, not_applied and no not-found", what, refusal)
	}
}

// TestKeyedWriteAtAnUnsettledRouterIsRetried: a delete or keyed update that
// reaches a template owner which has not taken the template over is refused
// with the hand-off, retryable and not applied, instead of being answered
// "not found" from a born set that lacks the document's part. Once the
// take-over lands, the document is there and the delete applies.
func TestKeyedWriteAtAnUnsettledRouterIsRetried(t *testing.T) {
	sc, name, victim, lag, other := unsettledRouter(t)
	requireHandOff(t, sc.frame(other, transport.OpDelete, DeleteRequest{Database: "app", Bucket: name, Key: victim}), "a delete at the unsettled router")
	requireHandOff(t, sc.frame(other, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: name, Key: victim, Set: map[string]interface{}{"N": 2}}),
		"an update at the unsettled router")
	takeOverPausedHook.Store(nil)
	clusterOf(sc.testCluster, lag).signalTakeOver()
	waitTakenOver(t, sc.nodes[lag].server.core, "app", name)
	reply := sc.frame(other, transport.OpGet, FindRequest{Database: "app", Bucket: name, Key: victim})
	var found FindResponse
	if err := bson.Unmarshal(reply.Payload, &found); err != nil || !found.Found {
		t.Fatalf("after the take-over the document reads %s %q", reply.Opcode, reply.Payload)
	}
	if deleted := sc.frame(other, transport.OpDelete, DeleteRequest{Database: "app", Bucket: name, Key: victim}); !replySucceeded(deleted.Opcode, deleted.Payload) {
		t.Fatalf("after the take-over the delete answered %s %q", deleted.Opcode, deleted.Payload)
	}
}

// TestForwardedReadAtTemplateReplicaIsMovedOnRetry: a forwarded read that
// reaches a template replica which is not the owner on its view (view skew)
// is refused with retry, never answered from the template's store, and the
// born set is never handed out.
func TestForwardedReadAtTemplateReplicaIsMovedOnRetry(t *testing.T) {
	sc := newSpreadCluster(t, 3, 2, "User", nil)
	if reply := sc.put(0, "ada", map[string]interface{}{"N": 1}); reply.Opcode == transport.OpError {
		t.Fatalf("insert: %s", refusalText(t, reply))
	}
	set := sc.templateSet("User")
	c := clusterOf(sc.testCluster, set[1])
	req := &ReplPushRequest{ReturnResponse: true, FromNode: sc.addrs[set[0]]}
	for _, key := range []string{"ada", bornDocumentID} {
		entry := ReplEntry{Op: int32(transport.OpGet), Bucket: "User", Payload: spreadMarshal(t, FindRequest{Database: "app", Bucket: "User", Key: key})}
		resp, _, err := c.serveForwardedEntry(context.Background(), req, entry, 0)
		if err == nil || !strings.Contains(err.Error(), "moved to another node") {
			t.Fatalf("a forwarded read of %q at a template replica answered %s %q, %v; want the retry", key, transport.Opcode(resp.Opcode), resp.Payload, err)
		}
	}
}

// TestLostPartRefusesKeyedOps is B9 for keyed operations: a part the born set
// records created that is no longer held refuses a read and a write, and the
// router never creates it again (B4.3). On a node with no cluster the text
// names this node's data, not a node to bring back.
func TestLostPartRefusesKeyedOps(t *testing.T) {
	n := newSpreadNode(t, nil)
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "lost", Data: map[string]interface{}{"N": 1}}); reply.Opcode == transport.OpError {
		t.Fatal(refusalText(t, reply))
	}
	p := spreadPartOf(t, "lost")
	drop := transport.NewFrame(transport.OpDeleteBucket, spreadMarshal(t, DeleteBucketRequest{Database: "app", Name: partName("User", p)}))
	if reply, err := n.s.fencedDispatch(context.Background(), drop); err != nil || reply.Opcode == transport.OpError {
		t.Fatalf("dropping the part past the router: %v %q", err, reply.Payload)
	}
	sibling := keysInPart(t, "lostsib", p, 1)[0]
	for label, reply := range map[string]*transport.Frame{
		"get": n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: "lost"}),
		"put": n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: sibling, Data: map[string]interface{}{"N": 1}}),
	} {
		text := refusalTextOrEmpty(t, reply)
		if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != transport.ErrUnavailable || !strings.Contains(text, "not held") ||
			strings.Contains(text, "Bring this node back") || strings.Contains(string(reply.Payload), unitSeparator) {
			t.Errorf("a %s in a lost part answered %s %q", label, reply.Opcode, text)
		}
	}
	if n.s.bucketStoreOpen("app", partName("User", p)) {
		t.Error("the lost part was created again")
	}
}

// TestUnbornAnswersMatchAWholeBucketsMiss: every keyed verb's answer for a key
// in an unborn part is the answer a whole bucket gives for a missing key,
// names aside.
func TestUnbornAnswersMatchAWholeBucketsMiss(t *testing.T) {
	stores := []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}
	n := newSpreadNode(t, map[string]interface{}{"stores": stores})
	if whole := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "Whole", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "stores": stores}}); whole.Opcode == transport.OpError {
		t.Fatalf("create whole: %s", refusalText(t, whole))
	}
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "anchor", Data: map[string]interface{}{"N": 1}}); reply.Opcode == transport.OpError {
		t.Fatal(refusalText(t, reply))
	}
	key := keyOutsideParts(t, "missing", map[int]bool{spreadPartOf(t, "anchor"): true})
	field := func(bucket string) any {
		return FieldRequest{Database: "app", Bucket: bucket, Key: key, Field: "Purchases", Data: map[string]interface{}{"x": 1}}
	}
	verbs := []struct {
		op      transport.Opcode
		request func(bucket string) any
	}{
		{transport.OpGet, func(b string) any { return FindRequest{Database: "app", Bucket: b, Key: key} }},
		{transport.OpExists, func(b string) any { return FindRequest{Database: "app", Bucket: b, Key: key} }},
		{transport.OpConsistentGet, func(b string) any { return FindRequest{Database: "app", Bucket: b, Key: key} }},
		{transport.OpDelete, func(b string) any { return DeleteRequest{Database: "app", Bucket: b, Key: key} }},
		{transport.OpUpdate, func(b string) any {
			return UpdateRequest{Database: "app", Bucket: b, Key: key, Set: map[string]interface{}{"N": 2}}
		}},
		{transport.OpFieldPush, field}, {transport.OpFieldPop, field}, {transport.OpFieldPut, field}, {transport.OpFieldDelete, field},
		{transport.OpFieldGet, field}, {transport.OpFieldPeek, field}, {transport.OpFieldLen, field}, {transport.OpFieldList, field},
		{transport.OpFieldPut, func(b string) any {
			return FieldRequest{Database: "app", Bucket: b, Key: key, Field: "Purchases", Element: "e1", Data: map[string]interface{}{"x": 1}}
		}},
		{transport.OpFieldPush, func(b string) any {
			return FieldRequest{Database: "app", Bucket: b, Key: key, Field: "Nope", Data: map[string]interface{}{"x": 1}}
		}},
	}
	for _, verb := range verbs {
		spread, whole := n.send(verb.op, verb.request("User")), n.send(verb.op, verb.request("Whole"))
		got := strings.ReplaceAll(fmt.Sprintf("%s %s", spread.Opcode, bson.Raw(spread.Payload)), "User", "B")
		want := strings.ReplaceAll(fmt.Sprintf("%s %s", whole.Opcode, bson.Raw(whole.Payload)), "Whole", "B")
		if got != want {
			t.Errorf("%s of a missing key:\n spread %s\n whole  %s", verb.op, got, want)
		}
	}
}

// TestBulkWritesWithRepeatsMatchAWholeBucket: a bulk write answers as a whole
// bucket does, ids, count and per-position results, across parts and within
// one: a repeat's "replaced by" names its request position, not its position
// in the part's share.
func TestBulkWritesWithRepeatsMatchAWholeBucket(t *testing.T) {
	n := newSpreadNode(t, nil)
	if whole := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "Whole", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole"}}); whole.Opcode == transport.OpError {
		t.Fatal(refusalText(t, whole))
	}
	a := "rep-a"
	b := keyOutsideParts(t, "rep-b", map[int]bool{spreadPartOf(t, a): true})
	c := keysInPart(t, "rep-c", spreadPartOf(t, a), 1)[0]
	cases := map[string][]map[string]interface{}{
		"across parts": {{"_id_": a, "N": 1}, {"_id_": b, "N": 2}, {"_id_": a, "N": 3}},
		"one part":     {{"_id_": c, "N": 1}, {"_id_": a, "N": 2}, {"_id_": c, "N": 3}, {"_id_": b, "N": 4}},
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, op := range []transport.Opcode{transport.OpBatchUpsert, transport.OpBatchPut} {
		for _, name := range names {
			spread := n.send(op, BatchInsertRequest{Database: "app", Bucket: "User", Data: cases[name]})
			whole := n.send(op, BatchInsertRequest{Database: "app", Bucket: "Whole", Data: cases[name]})
			if spread.Opcode != whole.Opcode {
				t.Errorf("%s %s: spread answered %s, whole %s", op, name, spread.Opcode, whole.Opcode)
				continue
			}
			var spreadAnswer, wholeAnswer BatchInsertResponse
			if err := bson.Unmarshal(spread.Payload, &spreadAnswer); err != nil {
				t.Fatal(err)
			}
			if err := bson.Unmarshal(whole.Payload, &wholeAnswer); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(spreadAnswer.IDs, spreadAnswer.Count, spreadAnswer.Results) != fmt.Sprint(wholeAnswer.IDs, wholeAnswer.Count, wholeAnswer.Results) {
				t.Errorf("%s %s:\n spread %v %d %v\n whole  %v %d %v", op, name, spreadAnswer.IDs, spreadAnswer.Count, spreadAnswer.Results,
					wholeAnswer.IDs, wholeAnswer.Count, wholeAnswer.Results)
			}
		}
	}
}

// TestKeyedWriteAtAnUnsettledOneMemberRouterIsRetried is the same at the
// one-member door: the third node is removed too (while it runs, so the two
// members can agree), so the lagging node is alone on its ring, owns the
// template and has not taken it over; its writes come in by
// Server.dispatchInFence and are answered with the hand-off.
func TestKeyedWriteAtAnUnsettledOneMemberRouterIsRetried(t *testing.T) {
	sc, name, victim, lag, other := unsettledRouter(t)
	removeNodeFrom(t, sc.testCluster, lag, other)
	lagCluster := clusterOf(sc.testCluster, lag)
	spreadEventually(t, 20*time.Second, "the lagging node is alone on its ring", lagCluster.aloneAfterARemoval)
	requireHandOff(t, sc.frame(lag, transport.OpDelete, DeleteRequest{Database: "app", Bucket: name, Key: victim}), "a delete at the unsettled one-member router")
	requireHandOff(t, sc.frame(lag, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: name, Key: victim, Set: map[string]interface{}{"N": 2}}),
		"an update at the unsettled one-member router")
}
