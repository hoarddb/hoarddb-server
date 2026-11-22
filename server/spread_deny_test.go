package server

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// spreadDenyPayload is a request naming app.User in every shape a bucket-scoped
// opcode reads its scope from, with a key in no created part.
func spreadDenyPayload(t testing.TB, op transport.Opcode) []byte {
	t.Helper()
	if op == transport.OpGetFast {
		payload, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "User", Key: "deny-absent"})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	request := map[string]interface{}{"database": "app", "bucket": "User", "name": "User", "key": "deny-absent"}
	switch op {
	case transport.OpPut, transport.OpUpsert:
		request["data"] = map[string]interface{}{"N": 1}
	case transport.OpBatchPut, transport.OpBatchUpsert:
		request["data"] = []interface{}{map[string]interface{}{"N": 1}, map[string]interface{}{"N": 2}}
	case transport.OpUpdate:
		request["set"] = map[string]interface{}{"N": 2}
	case transport.OpCreateBucket:
		request["type"] = "hash"
	case transport.OpAlterBucket:
		request["replication_factor"] = int32(1)
	case transport.OpDeclareStoreFields:
		request["stores"] = []interface{}{map[string]interface{}{"name": "Fresh", "engine": "fifoStore"}}
	case transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend:
		request["lease_id"] = strings.Repeat("ab", 20)
		request["lease_ms"] = int64(60000)
	case transport.OpDeadRequeue, transport.OpDeadDiscard:
		request["all"] = true
	case transport.OpFieldPush, transport.OpFieldPut, transport.OpFieldGet, transport.OpFieldPop, transport.OpFieldPeek,
		transport.OpFieldLen, transport.OpFieldList, transport.OpFieldDelete:
		request["field"] = "Purchases"
		request["data"] = map[string]interface{}{"sku": 1}
	}
	return spreadMarshal(t, request)
}

// spreadDenyRow is what §2.4 says the router does with an opcode's class, as
// a check of the answer.
func spreadDenyAnswerMatches(t testing.TB, class spreadOpClass, reply *transport.Frame) (bool, string) {
	t.Helper()
	text := string(reply.Payload)
	if reply.Opcode == transport.OpError {
		var refused ErrorResponse
		if err := bson.Unmarshal(reply.Payload, &refused); err == nil {
			text = refused.Message
		}
	}
	if strings.Contains(text, unitSeparator) {
		return false, "the answer names a part"
	}
	unsupported := reply.Opcode == transport.OpError && (strings.Contains(text, "not supported on spread bucket") || strings.Contains(text, "set when it is created"))
	switch class {
	case spreadKeyedWrite, spreadKeyedRead, spreadBulkWrite, spreadPassThrough, spreadFanOutRead:
		return !unsupported, "a routed, fanned-out or passed opcode was refused as unsupported"
	case spreadCollectionDrop, spreadDDL, spreadRefuse:
		return unsupported, "an opcode the router refuses was not refused as unsupported"
	case spreadWrongType:
		return reply.Opcode == transport.OpError && errorCodeOf(t, reply) == transport.ErrInvalidArgument && strings.Contains(text, "hash bucket"),
			"a verb of another store type was not refused with the hash bucket's wrong-type text"
	}
	return false, "an unknown class"
}

// bucketScopedClientOpcodes is every opcode of authz.go's table a client
// sends naming a bucket, which is what §2.4's rows have to cover: the table
// scopes them all by their database, so the database-level ones are left out
// (database DDL and the bucket listing, §2.4's last row), and DEAD_RESTORE,
// which NewHandler refuses from any client before routing. VERSIONED_GET and
// MIGRATE are scoped to nothing in the table and are answered before routing:
// TestVersionedGetOfATemplateIsRefused and TestMigrateOfASpreadBucketIsRefused.
func bucketScopedClientOpcodes() []transport.Opcode {
	var ops []transport.Opcode
	for op, rule := range opRules {
		if rule.Scope != ScopeDatabase {
			continue
		}
		switch op {
		case transport.OpCreateDB, transport.OpDeleteDB, transport.OpListBuckets, transport.OpDeadRestore:
			continue
		}
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })
	return ops
}

// TestSpreadDefaultDeny is test 8 (B5): every bucket-scoped client opcode of
// authz.go's table, sent to a spread template, is routed, split, fanned out
// over the parts (step 3), passed, refused as a later step's or refused with
// the wrong-type text, as §2.4's
// row for its class says, through a node with no cluster, through the
// template's owner and through a node that holds no copy of it. Whatever is
// sent, the template's store holds the born set alone, and every log holds
// under its name only its create and the born set's writes.
func TestSpreadDefaultDeny(t *testing.T) {
	ops := bucketScopedClientOpcodes()
	// The drop is the one opcode that succeeds: it ends the collection the rest
	// of the rows are sent to. It has its own tests (spread_drop_test.go).
	ops = slices.DeleteFunc(ops, func(op transport.Opcode) bool { return op == transport.OpDeleteBucket })
	classes := map[spreadOpClass]int{}
	for _, op := range ops {
		classes[spreadClassOf(op)]++
		if spreadClassOf(op) == spreadRefuse {
			t.Errorf("%s is bucket-scoped and in no row of §2.4: classify it in spreadClassOf", op)
		}
	}
	t.Logf("%d opcodes in authz.go's table, %d bucket-scoped client opcodes checked, by class %v", len(opRules), len(ops), classes)

	t.Run("a node with no cluster", func(t *testing.T) {
		n := newSpreadNode(t, map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}})
		for _, op := range ops {
			reply := n.sendRaw(op, spreadDenyPayload(t, op))
			if ok, why := spreadDenyAnswerMatches(t, spreadClassOf(op), reply); !ok {
				t.Errorf("%s: %s: %s %q", op, why, reply.Opcode, reply.Payload)
			}
		}
		if got := templateCount(t, n.s, "app", "User"); got > 1 {
			t.Errorf("the template's store holds %d documents", got)
		}
	})

	t.Run("a cluster, through the owner and a node with no copy", func(t *testing.T) {
		sc := newSpreadCluster(t, 3, 2, "User", map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}})
		doors := map[string]int{"the owner": sc.templateSet("User")[0], "a node with no copy": sc.nonHolder("User")}
		for door, index := range doors {
			for _, op := range ops {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				reply, err := sc.nodes[index].client.SendRequest(ctx, op, spreadDenyPayload(t, op))
				cancel()
				if err != nil {
					t.Fatalf("%s through %s: %v", op, door, err)
				}
				if ok, why := spreadDenyAnswerMatches(t, spreadClassOf(op), reply); !ok {
					t.Errorf("%s through %s: %s: %s %q", op, door, why, reply.Opcode, reply.Payload)
				}
			}
		}
		for index, node := range sc.nodes {
			entries, err := node.server.core.wal.ReadFrom(1)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				op := transport.Opcode(entry.Op)
				bornWrite := (op == transport.OpUpdate || op == transport.OpPut) && strings.Contains(string(entry.Data), bornDocumentID)
				if entry.Bucket == "User" && op != transport.OpCreateBucket && !bornWrite {
					t.Errorf("node %d logged %s under the template's name", index+1, op)
				}
			}
			if node.server.core.bucketStoreOpen("app", "User") {
				if got := templateCount(t, node.server.core, "app", "User"); got > 1 {
					t.Errorf("node %d's template holds %d documents", index+1, got)
				}
			}
		}
	})
}

// TestBatchItemNamingAPartIsRefusedWhole is test 9's BATCH half (B7, rev
// 2.1): a BATCH envelope is unscoped, so NewHandler never sees its items; the
// per-item gate refuses an item naming a part, and the whole batch with it,
// before any item runs, so nothing is placed outside its part.
func TestBatchItemNamingAPartIsRefusedWhole(t *testing.T) {
	n := newSpreadNode(t, map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": "Purchases", "engine": "fifoStore"}}})
	if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "ada", Data: map[string]interface{}{"N": 1}}); reply.Opcode == transport.OpError {
		t.Fatalf("insert: %s", refusalText(t, reply))
	}
	// The part exists, as the router would have made it, so an item that
	// named it and were let through would be stored there.
	template, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	part := partName("User", 3)
	if !n.s.bucketStoreOpen("app", part) {
		createPartByHand(t, n.s, "User", 3, template.incarnation)
	}
	if resp, err := n.s.Insert(InsertRequest{Database: "app", Bucket: part, Key: "stray", Data: map[string]interface{}{"N": 3}}); err != nil || !resp.Success {
		t.Fatalf("seeding the part: %+v, %v", resp, err)
	}
	ok := BatchItem{Opcode: int32(transport.OpUpdate), Payload: spreadMarshal(t, UpdateRequest{Database: "app", Bucket: "User", Key: "ada", Set: map[string]interface{}{"N": 9}})}
	misplaced := BatchItem{Opcode: int32(transport.OpUpdate), Payload: spreadMarshal(t, UpdateRequest{Database: "app", Bucket: part, Key: "stray", Set: map[string]interface{}{"Misplaced": true}})}
	element := BatchItem{Opcode: int32(transport.OpFieldPush), Payload: spreadMarshal(t, FieldRequest{Database: "app", Bucket: part, Key: "stray", Field: "Purchases", Data: map[string]interface{}{"sku": 1}})}
	reply := n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: []BatchItem{ok, misplaced, element}}))
	if reply.Opcode != transport.OpError {
		t.Fatalf("a BATCH with an item naming a part answered %s %q", reply.Opcode, reply.Payload)
	}
	if strings.Contains(string(reply.Payload), unitSeparator) || !strings.Contains(refusalText(t, reply), `"User"`) {
		t.Errorf("the refusal is not NOT FOUND of the displayed name: %q", reply.Payload)
	}
	if found := findAnswer(t, n.send(transport.OpGet, FindRequest{Database: "app", Bucket: "User", Key: "ada"})); !found.Found || strings.Contains(found.Data.String(), "9") {
		t.Errorf("an item of the refused batch was applied: ada reads %s", found.Data)
	}
	if inPart, err := n.s.Find(FindRequest{Database: "app", Bucket: part, Key: "stray"}); err != nil || strings.Contains(inPart.Data.String(), "Misplaced") {
		t.Errorf("an item naming a part changed the part's document: %+v, %v", inPart, err)
	}
}

// TestRouterGateNeverDeadlocks is test 15 (B4.4), on the real gate and the
// real fences: 32 goroutines run routed first writes (a mark under the
// template's fence, the part's create under its fence exclusively, the write
// under it shared), routed reads, drops (the gate exclusively, then each part's
// fence and the template's), and background exclusive fence holds (a
// snapshot, a take-over's open), and all finish by the deadline under -race.
// The mark carries withRouterGateHeld and never takes the gate again, as
// Cluster.routeSpreadWrite skips a template write that carries it; a mark
// that takes the gate again deadlocks behind a queued drop (the reviewer's
// rw/main.go).
func TestRouterGateNeverDeadlocks(t *testing.T) {
	s := &Server{}
	template := placementOf("app", "User")
	const parts = 8
	partKeys := make([]placementKey, parts)
	for p := range partKeys {
		partKeys[p] = placementOf("app", partName("User", p))
	}
	var bornMu sync.Mutex
	born := make([]bool, parts)

	mark := func(ctx context.Context) {
		if !routerGateHeld(ctx, template) {
			release := s.holdRouterGate(template, false)
			defer release()
		}
		fence := s.acquireFence(template, fenceShared)
		fence.release()
	}
	routedWrite := func(p int) {
		release := s.holdRouterGate(template, false)
		defer release()
		bornMu.Lock()
		first := !born[p]
		bornMu.Unlock()
		if first {
			ctx := routerInternalContext(context.Background(), template)
			mark(ctx)
			create := s.acquireFence(partKeys[p], fenceExclusive)
			create.release()
			mark(ctx)
			bornMu.Lock()
			born[p] = true
			bornMu.Unlock()
		}
		write := s.acquireFence(partKeys[p], fenceShared)
		write.release()
	}
	routedRead := func() {
		release := s.holdRouterGate(template, false)
		release()
	}
	drop := func() {
		release := s.holdRouterGate(template, true)
		defer release()
		for p := 0; p < parts; p++ {
			fence := s.acquireFence(partKeys[p], fenceExclusive)
			fence.release()
		}
		fence := s.acquireFence(template, fenceExclusive)
		fence.release()
		bornMu.Lock()
		clear(born)
		bornMu.Unlock()
	}
	background := func(i int) {
		key := template
		if i%2 == 1 {
			key = partKeys[i%parts]
		}
		fence := s.acquireFence(key, fenceExclusive)
		fence.release()
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				switch k := (worker*7 + i*13) % 20; {
				case k == 0:
					drop()
				case k < 3:
					background(worker + i)
				case k < 6:
					routedRead()
				default:
					routedWrite((worker + i) % parts)
				}
			}
		}(worker)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("routed writes, reads and drops did not finish: the gate and the fences deadlocked")
	}
	if left := s.routerGates.inFlight(); left != 0 {
		t.Fatalf("%d gate entries left", left)
	}
}
