package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Pop-n build step 7: a pop of N on a cluster is one logged write, judged on
// every copy by the result ceiling its owner stamped on it.

// popPad is the size of a large entry in the ceiling rows: three of them cross
// 1 MiB, and ten fit 4 MiB where eleven do not.
const popPad = 400 << 10

// clusterPopSubject is one kind of pop a cluster row runs: a bucket pop of a
// fifo, a lifo or a heap bucket, or the pop of a fifo store field of document
// p1. bucket is empty until createOn names it.
type clusterPopSubject struct {
	kind       string
	bucketType BucketType
	field      bool
	bucket     string
}

var (
	fifoPopSubject  = clusterPopSubject{kind: "fifo", bucketType: BucketTypeFIFO}
	lifoPopSubject  = clusterPopSubject{kind: "lifo", bucketType: BucketTypeLIFO}
	heapPopSubject  = clusterPopSubject{kind: "heap", bucketType: BucketTypeHeap}
	fieldPopSubject = clusterPopSubject{kind: "field", bucketType: BucketTypeHash, field: true}
	everyPopSubject = []clusterPopSubject{fifoPopSubject, lifoPopSubject, heapPopSubject, fieldPopSubject}
)

// createOn creates the subject's bucket, placed on the node at owner, through
// that node, and waits until every node serves it. A field subject's bucket
// holds document p1 with an empty Comments fifo.
func (s clusterPopSubject) createOn(t *testing.T, tc *testCluster, owner int) clusterPopSubject {
	t.Helper()
	s.bucket = bucketOwnedBy(t, tc, owner, "pop"+s.kind)
	create := CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: s.bucketType}
	if s.field {
		create.Options = storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})
	}
	tc.ok(owner, transport.OpCreateBucket, create)
	waitForServing(t, tc, testDatabase, s.bucket, len(tc.nodes))
	if s.field {
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "p1",
			Data: map[string]interface{}{"title": "p1"}, WriteConcern: "all"})
	}
	return s
}

// push adds the entries {n: from} to {n: from+count-1} through node via at
// concern all, each padded with pad bytes. A heap entry's priority is 10n, so
// a heap pops the newest first, as a lifo does.
func (s clusterPopSubject) push(t *testing.T, tc *testCluster, via, from, count, pad int) {
	t.Helper()
	for n := from; n < from+count; n++ {
		document := map[string]interface{}{"n": int32(n), "pad": strings.Repeat("x", pad)}
		var op transport.Opcode
		var request any
		switch {
		case s.field:
			op, request = transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: s.bucket, Key: "p1", Field: "Comments",
				Data: document, WriteConcern: "all"}
		case s.bucketType == BucketTypeHeap:
			op, request = transport.OpHeapPush, PushRequest{Database: testDatabase, Bucket: s.bucket, Data: document,
				Priority: int64(10 * n), WriteConcern: "all"}
		case s.bucketType == BucketTypeLIFO:
			op, request = transport.OpLIFOPush, PushRequest{Database: testDatabase, Bucket: s.bucket, Data: document, WriteConcern: "all"}
		default:
			op, request = transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: s.bucket, Data: document, WriteConcern: "all"}
		}
		if frame := tc.frame(via, op, request); frame.Opcode == transport.OpError {
			tc.dumpStreamsOnConcernFailure(t, s.bucket)
			t.Fatalf("node %d answered %s with an error: %s", via+1, op, string(frame.Payload))
		}
	}
}

// dumpStreamsOnConcernFailure logs, for a concern-all push that was refused,
// what decides whether a replica counted toward it (BUG-0487): each node's
// stream to each other node (state, stall reason, in-sync flag, the position
// that peer has acknowledged of this node's log) and every node's log end and
// applied sequence of bucket. It reads state and waits for nothing, so the
// test fails exactly as it did without it. The log ends bound the push's own
// sequence: it is the highest one on the owner.
func (tc *testCluster) dumpStreamsOnConcernFailure(t *testing.T, bucket string) {
	t.Helper()
	for from := range tc.nodes {
		for to := range tc.nodes {
			if from == to {
				continue
			}
			stream := peerOf(t, tc, from, to)
			state, reason := stream.streamStatus()
			cursor, opened := stream.cursor(clusterOf(tc, from).self.ID)
			t.Logf("stream node %d -> node %d: state %v (%q), in sync %v, acknowledged through %d (cursor opened %v)",
				from+1, to+1, state, reason, stream.inSync(), cursor, opened)
		}
	}
	for index, n := range tc.nodes {
		t.Logf("node %d: log ends at %d, applied sequence of %s is %d", index+1, n.server.core.wal.LastSequence(), bucket,
			bucketStateOf(t, n.server.core, testDatabase, bucket).AppliedSeq)
	}
}

// popOp is the opcode a client pops the subject with.
func (s clusterPopSubject) popOp() transport.Opcode {
	if s.field {
		return transport.OpFieldPop
	}
	return popTestOpcode(s.bucketType)
}

// pop is a pop of limit entries of the subject at concern all, as raw BSON a
// test can add members to.
func (s clusterPopSubject) pop(limit int) bson.M {
	request := bson.M{"database": testDatabase, "bucket": s.bucket, "limit": limit, "writeconcern": "all"}
	if s.field {
		request["key"], request["field"] = "p1", "Comments"
	}
	return request
}

// peek is a peek of limit entries of the subject and its opcode. A fifo and a
// heap bucket are both peeked with FIFO_PEEK.
func (s clusterPopSubject) peek(limit int) (transport.Opcode, bson.M) {
	request := bson.M{"database": testDatabase, "bucket": s.bucket, "limit": limit}
	switch {
	case s.field:
		request["key"], request["field"] = "p1", "Comments"
		return transport.OpFieldPeek, request
	case s.bucketType == BucketTypeLIFO:
		return transport.OpLIFOPeek, request
	}
	return transport.OpFIFOPeek, request
}

// heldOn is the _id_ of every entry of the subject the node at index holds, in
// pop order, read from its own store. It does not peek, because a peek is
// refused at the node's ceiling and these rows hold more than that.
func (s clusterPopSubject) heldOn(t *testing.T, tc *testCluster, index int) []string {
	t.Helper()
	core := tc.nodes[index].server.core
	ids := []string{}
	collect := func(entry []byte) error {
		id, ok := bson.Raw(entry).Lookup("_id_").StringValueOK()
		if !ok {
			return fmt.Errorf("an entry of %s has no _id_: %.200v", s.bucket, bson.Raw(entry))
		}
		ids = append(ids, id)
		return nil
	}
	if s.field {
		err := core.readField(FieldRequest{Database: testDatabase, Bucket: s.bucket, Key: "p1", Field: "Comments"}, fieldVerbPeek,
			func(snap storage.KVSnapshot, field resolvedField) error {
				return store.FieldPeekN(snap, field.ref, maxPeekLimit, func(entry store.FieldEntry) error { return collect(entry.Value) })
			})
		if err != nil {
			t.Fatalf("node %d: walking %s: %v", index+1, s.bucket, err)
		}
		return ids
	}
	handle, err := core.resolveBucket(testDatabase, s.bucket)
	if err != nil {
		t.Fatalf("node %d no longer serves %s: %v", index+1, s.bucket, err)
	}
	if s.bucketType == BucketTypeHeap {
		err = handle.heapStore.PeekN(maxPeekLimit, collect)
	} else {
		err = handle.queueStore.PeekN(maxPeekLimit, collect)
	}
	if err != nil {
		t.Fatalf("node %d: walking %s: %v", index+1, s.bucket, err)
	}
	return ids
}

// requireCopiesAgree waits up to 20 s for every node to hold the subject's
// entries as the first node does, fails naming each node's count when they
// never agree, and returns the entries they agree on.
func (s clusterPopSubject) requireCopiesAgree(t *testing.T, tc *testCluster, when string) []string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		held := make([][]string, len(tc.nodes))
		agree := true
		for index := range tc.nodes {
			held[index] = s.heldOn(t, tc, index)
			agree = agree && slices.Equal(held[index], held[0])
		}
		if agree {
			return held[0]
		}
		if time.Now().After(deadline) {
			counts := make([]int, len(held))
			for index, ids := range held {
				counts[index] = len(ids)
			}
			t.Fatalf("%s: the copies of %s differ: node by node they hold %v entries; every copy of a logged pop must remove the same ones",
				when, s.bucket, counts)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// loggedPops is the payload of every pop of the subject's bucket that the
// node at index logged above sequence after.
func (s clusterPopSubject) loggedPops(t *testing.T, tc *testCluster, index int, after uint64) []bson.Raw {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(after + 1)
	if err != nil {
		t.Fatalf("node %d: reading its log above %d: %v", index+1, after, err)
	}
	var payloads []bson.Raw
	for _, entry := range entries {
		if entry.Bucket == s.bucket && isPopOpcode(transport.Opcode(entry.Op)) {
			payloads = append(payloads, bson.Raw(entry.Data))
		}
	}
	return payloads
}

// stampedCeilings is the value of every member of a logged payload whose name
// is result_ceiling in any case: the decoder matches a member that differs
// only in case, so each one is a ceiling an applier could read.
func stampedCeilings(t *testing.T, payload bson.Raw) []int64 {
	t.Helper()
	elements, err := payload.Elements()
	if err != nil {
		t.Fatalf("reading a logged pop: %v", err)
	}
	var ceilings []int64
	for _, element := range elements {
		if strings.EqualFold(element.Key(), "result_ceiling") {
			ceiling, _ := element.Value().AsInt64OK()
			ceilings = append(ceilings, ceiling)
		}
	}
	return ceilings
}

// poppedNumbers is the n of every item a pop's answer holds, in its order; a
// field pop's item wraps its element.
func poppedNumbers(t *testing.T, answer *transport.Frame, what string) []int {
	t.Helper()
	if answer.Opcode == transport.OpError {
		t.Fatalf("%s was refused: %s", what, answer.Payload)
	}
	items, ok := bson.Raw(answer.Payload).Lookup("items").ArrayOK()
	if !ok {
		t.Fatalf("%s answered no items: %.300s", what, bson.Raw(answer.Payload))
	}
	values, err := items.Values()
	if err != nil {
		t.Fatalf("%s: reading its items: %v", what, err)
	}
	numbers := make([]int, 0, len(values))
	for _, value := range values {
		entry := value.Document()
		if element, wrapped := entry.Lookup("element").DocumentOK(); wrapped {
			entry = element
		}
		n, ok := entry.Lookup("n").AsInt64OK()
		if !ok {
			t.Fatalf("%s answered an item with no n: %.200v", what, entry)
		}
		numbers = append(numbers, int(n))
	}
	return numbers
}

// quiescedSequences is every node's last logged sequence, read once no node's
// log has moved for 300 ms: a background write sharing the log would
// otherwise be counted as the pop's.
func (tc *testCluster) quiescedSequences(t *testing.T) []uint64 {
	t.Helper()
	previous := tc.walSequences()
	deadline := time.Now().Add(20 * time.Second)
	for {
		time.Sleep(300 * time.Millisecond)
		current := tc.walSequences()
		if slices.Equal(current, previous) {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("the nodes' logs never stopped moving: %v then %v", previous, current)
		}
		previous = current
	}
}

// waitAppliedPast waits until every node has applied an entry of bucket above
// the sequence before names for it: a refusal moves the bucket's mark too.
func (tc *testCluster) waitAppliedPast(t *testing.T, bucket string, before []uint64) {
	t.Helper()
	for index, n := range tc.nodes {
		pollUntil(t, fmt.Sprintf("node %d applies the pop of %s", index+1, bucket), func() bool {
			return bucketStateOf(t, n.server.core, testDatabase, bucket).AppliedSeq > before[index]
		})
	}
}

// ceilingCluster is three nodes at factor 3, the node at each index with the
// result ceiling ceilings names for it, and the test database.
func ceilingCluster(t *testing.T, ceilings map[int]int, tune func(int, *Cluster)) *testCluster {
	t.Helper()
	tc := newTestClusterTuned(t, 3, 3, func(index int, cfg *config.Config) {
		if ceiling, set := ceilings[index]; set {
			setQueryCeiling(cfg, ceiling)
		}
	}, tune)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	return tc
}

// TestPopLimitIsOneWALEntry: pop({limit: 5}) through the owner, and again
// through a member that forwards it, moves every node's log by exactly one
// entry each time, and five entries are gone from every copy. Guard: split the
// pop into five single pops in Cluster.handleClientFrame.
func TestPopLimitIsOneWALEntry(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	for _, kind := range []clusterPopSubject{fifoPopSubject, fieldPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 10, 0)
		for round, via := range []int{owner, member} {
			what := fmt.Sprintf("a %s pop({limit: 5}) through node %d", subject.kind, via+1)
			before := tc.quiescedSequences(t)
			if popped := poppedNumbers(t, tc.frame(via, subject.popOp(), subject.pop(5)), what); len(popped) != 5 {
				t.Fatalf("%s answered %v, want five entries", what, popped)
			}
			tc.waitEveryNodeLogged(t, before)
			tc.waitAppliedPast(t, subject.bucket, before)
			after := tc.quiescedSequences(t)
			for index := range tc.nodes {
				if moved := after[index] - before[index]; moved != 1 {
					t.Errorf("%s moved node %d's log by %d entries, want exactly 1", what, index+1, moved)
				}
			}
			if held, want := subject.requireCopiesAgree(t, tc, what), 10-5*(round+1); len(held) != want {
				t.Fatalf("after %s every copy holds %d entries, want %d", what, len(held), want)
			}
		}
	}
}

// TestPopLimitReplicasRemoveTheSameEntries: pushes and pops of different
// counts, interleaved through different members, leave every copy holding the
// same entries, and the copies removed exactly what the pops asked for. Guard:
// a replica ignores the limit and removes one entry.
func TestPopLimitReplicasRemoveTheSameEntries(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	for _, kind := range everyPopSubject {
		subject := kind.createOn(t, tc, 0)
		subject.push(t, tc, 0, 1, 6, 0)
		next, removed := 7, 0
		for round := 0; round < 6; round++ {
			pushVia, popVia, limit := round%3, (round+1)%3, round%3+1
			subject.push(t, tc, pushVia, next, 3, 0)
			next += 3
			what := fmt.Sprintf("%s pop({limit: %d}) through node %d", subject.kind, limit, popVia+1)
			if popped := poppedNumbers(t, tc.frame(popVia, subject.popOp(), subject.pop(limit)), what); len(popped) != limit {
				t.Fatalf("%s answered %v", what, popped)
			}
			removed += limit
		}
		if held, want := subject.requireCopiesAgree(t, tc, subject.kind), next-1-removed; len(held) != want {
			t.Fatalf("%s: every copy holds %d entries after %d pushed and %d popped, want %d", subject.kind, len(held), next-1, removed, want)
		}
	}
}

// TestPopCeilingIsStampedAtTheOwner: the owner and one other node run a 4 MiB
// result ceiling, and the third node 1 MiB. A pop is sent through the small
// node (a coordinator and a replica that are small) and through the large
// node that does not own the bucket (one replica small). A pop that fits the
// owner's ceiling but not the small node's removes the same entries on every
// copy; one over the owner's is refused on every copy and removes nothing;
// every logged pop carries the owner's ceiling; no copy is reported diverged;
// and a peek of the same count, which the owner answers, fits exactly when the
// pop does. Guards: (a) the handler reads its own node's ceiling on a logged
// entry; (b) the coordinator stamps the ceiling instead of the owner.
func TestPopCeilingIsStampedAtTheOwner(t *testing.T) {
	const owner, small, large = 0, 1, 2
	tc := ceilingCluster(t, map[int]int{owner: 4 << 20, small: 1 << 20, large: 4 << 20}, nil)
	defer tc.stop()
	divergedBefore := divergencesCounted(t)
	loggedBefore := tc.quiescedSequences(t)

	subjects := []clusterPopSubject{fifoPopSubject.createOn(t, tc, owner), fieldPopSubject.createOn(t, tc, owner)}
	for _, subject := range subjects {
		subject.push(t, tc, owner, 1, 16, popPad)
		left := 16
		for _, via := range []int{small, large} {
			what := fmt.Sprintf("a %s pop through node %d", subject.kind, via+1)
			peekOp, peekOver := subject.peek(11)
			requireCeilingRefusal(t, tc.frame(via, peekOp, peekOver), what+": peek({limit: 11})", "")
			requireCeilingRefusal(t, tc.frame(via, subject.popOp(), subject.pop(11)), what+": pop({limit: 11})", outcomeNotApplied)
			if held := subject.requireCopiesAgree(t, tc, what+" refused"); len(held) != left {
				t.Fatalf("%s over the owner's ceiling left %d entries on every copy, want %d: it removed some", what, len(held), left)
			}

			peekOp, peekWithin := subject.peek(5)
			if peeked := tc.frame(via, peekOp, peekWithin); peeked.Opcode == transport.OpError {
				t.Fatalf("%s: peek({limit: 5}) within the owner's ceiling was refused: %s", what, peeked.Payload)
			}
			if popped := poppedNumbers(t, tc.frame(via, subject.popOp(), subject.pop(5)), what+": pop({limit: 5})"); len(popped) != 5 {
				t.Fatalf("%s: pop({limit: 5}) answered %v", what, popped)
			}
			left -= 5
			if held := subject.requireCopiesAgree(t, tc, what+" within the owner's ceiling"); len(held) != left {
				t.Fatalf("%s within the owner's ceiling left %d entries on every copy, want %d", what, len(held), left)
			}
		}
	}

	for _, subject := range subjects {
		for index := range tc.nodes {
			logged := subject.loggedPops(t, tc, index, loggedBefore[index])
			if len(logged) != 4 {
				t.Errorf("node %d logged %d pops of %s, want 4", index+1, len(logged), subject.bucket)
			}
			for _, payload := range logged {
				if ceilings := stampedCeilings(t, payload); !slices.Equal(ceilings, []int64{4 << 20}) {
					t.Errorf("node %d logged a pop of %s carrying result_ceiling %v, want the owner's [%d] alone", index+1, subject.bucket, ceilings, 4<<20)
				}
			}
		}
	}
	if moved := divergencesCounted(t) - divergedBefore; moved != 0 {
		t.Errorf("%v copies were reported diverged; a pop judged by its owner's stamp is decided alike on every copy", moved)
	}
}

// requireCeilingRefusal fails unless answer is a RESULT_TOO_LARGE refusal at
// the owner's 4 MiB ceiling, with write_outcome outcome when outcome is set.
func requireCeilingRefusal(t *testing.T, answer *transport.Frame, what, outcome string) {
	t.Helper()
	refusal := errorAnswerOf(t, answer, what)
	if transport.ErrorCode(refusal.Code) != transport.ErrResultTooLarge || refusal.MaxResultBytes != 4<<20 {
		t.Fatalf("%s was answered code %d at max_result_bytes %d: %s; want RESULT_TOO_LARGE at the owner's %d",
			what, refusal.Code, refusal.MaxResultBytes, refusal.Message, 4<<20)
	}
	if outcome != "" && refusal.WriteOutcome != outcome {
		t.Errorf("%s was answered write_outcome %q, want %q", what, refusal.WriteOutcome, outcome)
	}
}

// TestPopLimitAboveCapIsNotLogged: a limit above 100, or below zero, sent
// through a member that does not own the bucket is refused at the door,
// INVALID_ARGUMENT and not_applied, and no node's log moves. Guard: remove the
// door's check (the owner's handler still refuses, after logging).
func TestPopLimitAboveCapIsNotLogged(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	for _, kind := range []clusterPopSubject{fifoPopSubject, fieldPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 3, 0)
		for _, limit := range []int{101, -1} {
			what := fmt.Sprintf("a %s pop({limit: %d}) through node %d", subject.kind, limit, member+1)
			before := tc.quiescedSequences(t)
			refusal := errorAnswerOf(t, tc.frame(member, subject.popOp(), subject.pop(limit)), what)
			requireOutcome(t, refusal, what, outcomeNotApplied, transport.ErrInvalidArgument)
			if limit > 0 && !strings.Contains(refusal.Message, "101 is above the pop maximum of 100") {
				t.Errorf("%s was refused without the cap's text: %s", what, refusal.Message)
			}
			if after := tc.quiescedSequences(t); !slices.Equal(after, before) {
				t.Errorf("%s moved the nodes' logs from %v to %v; a limit the door can refuse must never be logged", what, before, after)
			}
		}
		if held := subject.requireCopiesAgree(t, tc, subject.kind); len(held) != 3 {
			t.Fatalf("the refused %s pops left %d entries, want 3", subject.kind, len(held))
		}
	}
}

// TestPopLimitReplayRemovesTheSameEntries: the owner, at a 4 MiB ceiling,
// logs a pop of five 400 KiB entries and its apply fails before the commit;
// the replicas remove the five. The owner restarts at a 1 MiB ceiling, and its
// replay, judged by the ceiling it stamped, removes the same five. Guard: stamp
// nothing, so replay is judged by the node's own ceiling and refuses.
func TestPopLimitReplayRemovesTheSameEntries(t *testing.T) {
	for _, kind := range []clusterPopSubject{fifoPopSubject, fieldPopSubject} {
		t.Run(kind.kind, func(t *testing.T) {
			const owner = 0
			tc := ceilingCluster(t, map[int]int{0: 4 << 20, 1: 4 << 20, 2: 4 << 20}, nil)
			defer tc.stop()
			subject := kind.createOn(t, tc, owner)
			subject.push(t, tc, owner, 1, 5, popPad)

			popSeq := tc.quiescedSequences(t)[owner] + 1
			failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, func(seq uint64) bool { return seq == popSeq })
			request := subject.pop(5)
			request["writeconcern"] = "majority"
			// The owner's apply failed, so the answer is not the pop's; it is
			// not asserted.
			tc.frame(owner, subject.popOp(), request)
			if failed.Load() != popSeq {
				t.Fatalf("setup: the owner's apply at sequence %d never reached its store", popSeq)
			}
			store.ApplyFaultHook.Store(nil)
			for _, replica := range []int{1, 2} {
				pollUntil(t, fmt.Sprintf("replica %d removes the five", replica+1), func() bool { return len(subject.heldOn(t, tc, replica)) == 0 })
			}
			if held := subject.heldOn(t, tc, owner); len(held) != 5 {
				t.Fatalf("setup: the owner holds %d entries before its restart, want the five its failed apply kept", len(held))
			}

			tc.configure = func(index int, cfg *config.Config) {
				ceiling := 4 << 20
				if index == owner {
					ceiling = 1 << 20
				}
				setQueryCeiling(cfg, ceiling)
			}
			tc.restartNode(owner)
			tc.waitReady()
			if held := subject.requireCopiesAgree(t, tc, "after the owner's replay"); len(held) != 0 {
				t.Fatalf("after its replay at a 1 MiB ceiling the owner holds %d entries the replicas removed", len(held))
			}
		})
	}
}

// TestPopResponseCarriesItemsAndCopyCount: a cluster pop({limit: 3}) answers
// its three items in pop order and how many copies hold the removal, through
// the owner and through a member that forwards it. Guard: re-encode the answer
// in Cluster.stampWriteOutcome through a struct without items.
func TestPopResponseCarriesItemsAndCopyCount(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	for _, kind := range everyPopSubject {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 6, 0)
		want := [][]int{{1, 2, 3}, {4, 5, 6}}
		if subject.bucketType == BucketTypeLIFO || subject.bucketType == BucketTypeHeap {
			want = [][]int{{6, 5, 4}, {3, 2, 1}}
		}
		for round, via := range []int{owner, member} {
			what := fmt.Sprintf("a %s pop({limit: 3}) through node %d", subject.kind, via+1)
			answer := tc.frame(via, subject.popOp(), subject.pop(3))
			if popped := poppedNumbers(t, answer, what); !slices.Equal(popped, want[round]) {
				t.Errorf("%s answered items %v, want %v", what, popped, want[round])
			}
			if confirmed, factor, _, _ := copiesConfirmedOf(answer.Payload); confirmed != 3 || factor != 3 {
				t.Errorf("%s answered copies_confirmed %d, replication_factor %d; want 3 and 3", what, confirmed, factor)
			}
		}
	}
}

// TestPopIgnoresTheCallersCancellation is the M-1 row: the caller of a pop
// goes away after its entry is logged and before its owner applies it. The
// owner's apply must not see that: every copy removes the same three entries,
// and the owner's outcome is applied. Guard: hand newQueryWalk the request's
// context in Server.PopCtx or Server.FieldPopCtx; the owner's apply then fails
// alone and keeps the entries every replica removed.
func TestPopIgnoresTheCallersCancellation(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner = 0
	ownerCluster := tc.nodes[owner].server.core.cluster
	for _, kind := range []clusterPopSubject{fifoPopSubject, fieldPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 5, 0)

		ctx, cancel := context.WithCancel(context.Background())
		var cancelOnce sync.Once
		gone := func(nodeID string, repl ReplEntry) {
			if nodeID == ownerCluster.self.ID && repl.Bucket == subject.bucket && isPopOpcode(transport.Opcode(repl.Op)) {
				cancelOnce.Do(cancel)
			}
		}
		outcomes := make(chan int32, 1)
		enqueued := func(entry ReplEntry) {
			if entry.Bucket == subject.bucket && isPopOpcode(transport.Opcode(entry.Op)) {
				select {
				case outcomes <- entry.OwnerOutcome:
				default:
				}
			}
		}
		before := tc.quiescedSequences(t)
		beforeLocalApplyHook.Store(&gone)
		replicaEnqueueHook.Store(&enqueued)
		// The caller is gone, so its answer is not asserted.
		_, _ = ownerCluster.handleClientFrame(ctx, transport.NewFrame(subject.popOp(), mustMarshalBSON(t, subject.pop(3))))
		beforeLocalApplyHook.Store(nil)
		replicaEnqueueHook.Store(nil)
		cancel()
		// Every copy has applied the pop before the copies are compared: until
		// the replicas apply it, they agree with an owner that removed nothing.
		tc.waitAppliedPast(t, subject.bucket, before)

		select {
		case outcome := <-outcomes:
			if outcome != ownerOutcomeApplied {
				t.Errorf("the owner's outcome for a %s pop whose caller had gone is %d, want applied (%d)", subject.kind, outcome, ownerOutcomeApplied)
			}
		default:
			t.Fatalf("setup: the %s pop was never routed to the replicas", subject.kind)
		}
		if held := subject.requireCopiesAgree(t, tc, subject.kind+" pop whose caller had gone"); len(held) != 2 {
			t.Fatalf("every copy holds %d entries after a %s pop of three whose caller had gone, want 2", len(held), subject.kind)
		}
	}
}

// TestPopClientCannotRaiseTheCeilingOnACluster is the cluster half of
// TestPopClientCannotRaiseTheCeiling: a client's result_ceiling of 1 GiB, sent
// through a 1 MiB member, is logged on every node as the 4 MiB owner's, not
// the client's and not the coordinator's, and the payload carries no other
// member an applier would read as one, whatever its case. Guard: stamp the
// ceiling only when the payload has none.
func TestPopClientCannotRaiseTheCeilingOnACluster(t *testing.T) {
	const owner, coordinator = 0, 1
	tc := ceilingCluster(t, map[int]int{owner: 4 << 20, coordinator: 1 << 20, 2: 1 << 20}, nil)
	defer tc.stop()
	for _, kind := range []clusterPopSubject{fifoPopSubject, fieldPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 2, 0)
		before := tc.quiescedSequences(t)
		for _, member := range []string{"result_ceiling", "Result_Ceiling"} {
			request := subject.pop(1)
			request[member] = int64(1 << 30)
			poppedNumbers(t, tc.frame(coordinator, subject.popOp(), request), fmt.Sprintf("a %s pop naming %s", subject.kind, member))
		}
		for index := range tc.nodes {
			logged := subject.loggedPops(t, tc, index, before[index])
			if len(logged) != 2 {
				t.Fatalf("node %d logged %d pops of %s, want 2", index+1, len(logged), subject.bucket)
			}
			for _, payload := range logged {
				if ceilings := stampedCeilings(t, payload); !slices.Equal(ceilings, []int64{4 << 20}) {
					t.Errorf("node %d logged a %s pop whose client asked for 1 GiB with result_ceiling %v, want the owner's [%d] alone",
						index+1, subject.kind, ceilings, 4<<20)
				}
			}
		}
	}
}

// TestPopOverResultCeilingRemovesNothingOnACluster is the cluster variant of
// TestPopOverResultCeilingRemovesNothing: through a member that does not own
// the bucket, a pop over the 1 MiB ceiling is answered RESULT_TOO_LARGE,
// not_applied, in the words that name the owner's setting; every copy keeps
// all five entries; and the POP label moves by one, though three copies
// charged the entries. Guard: every copy counts its refusal (three).
func TestPopOverResultCeilingRemovesNothingOnACluster(t *testing.T) {
	tc := ceilingCluster(t, map[int]int{0: 1 << 20, 1: 1 << 20, 2: 1 << 20}, nil)
	defer tc.stop()
	const owner, member = 0, 1
	for _, kind := range []clusterPopSubject{fifoPopSubject, fieldPopSubject} {
		subject := kind.createOn(t, tc, owner)
		subject.push(t, tc, owner, 1, 5, popPad)
		what := fmt.Sprintf("a %s pop({limit: 5}) of 400 KiB entries through node %d", subject.kind, member+1)
		before := tc.quiescedSequences(t)
		counted := popRefusalsCounted()

		refusal := errorAnswerOf(t, tc.frame(member, subject.popOp(), subject.pop(5)), what)
		requireOutcome(t, refusal, what, outcomeNotApplied, transport.ErrResultTooLarge)
		for _, want := range []string{"so nothing was removed", "pop({limit: 2}) fits",
			"raise HOARDDB_QUERY_MAX_RESULT_BYTES on the bucket's owner (1 MiB)"} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("%s: the refusal lacks %q: %s", what, want, refusal.Message)
			}
		}
		if held := subject.requireCopiesAgree(t, tc, what); len(held) != 5 {
			t.Fatalf("%s left %d entries on every copy, want all five", what, len(held))
		}
		tc.waitAppliedPast(t, subject.bucket, before)
		if moved := popRefusalsCounted() - counted; moved != 1 {
			t.Errorf("%s moved the POP refusal counter by %v, want 1: it counts once per request, where it is answered", what, moved)
		}
	}
}

// TestAPopNWhoseWriteConcernTimesOutAnswersEveryEntry is BUG-0548's path 2
// with N: a pop({limit: 3}) whose concern all times out, because one replica
// in sync confirms nothing, is answered with its three entries, two copies of
// three, write_outcome applied and the plural message, through the owner and
// through a member that forwards it. Guard: the singular message.
func TestAPopNWhoseWriteConcernTimesOutAnswersEveryEntry(t *testing.T) {
	tc := ceilingCluster(t, nil, func(_ int, c *Cluster) { c.wcTimeout = 2 * time.Second })
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	subjects := []clusterPopSubject{fifoPopSubject.createOn(t, tc, owner), fieldPopSubject.createOn(t, tc, owner)}
	for _, subject := range subjects {
		subject.push(t, tc, owner, 1, 6, 0)
	}
	defer holdShipsTo(tc.addrs[held])()

	for _, subject := range subjects {
		for _, via := range []int{owner, coordinator} {
			what := fmt.Sprintf("a %s pop({limit: 3}) at concern all through node %d", subject.kind, via+1)
			answer := tc.frame(via, subject.popOp(), subject.pop(3))
			if popped := poppedNumbers(t, answer, what); len(popped) != 3 {
				t.Fatalf("%s answered %v, want its three entries", what, popped)
			}
			confirmed, factor, _, _ := copiesConfirmedOf(answer.Payload)
			outcome, _ := bson.Raw(answer.Payload).Lookup("write_outcome").StringValueOK()
			message, _ := bson.Raw(answer.Payload).Lookup("message").StringValueOK()
			if confirmed != 2 || factor != 3 || outcome != outcomeApplied {
				t.Errorf("%s answered copies_confirmed %d, replication_factor %d, write_outcome %q; want 2, 3 and %q",
					what, confirmed, factor, outcome, outcomeApplied)
			}
			if !strings.Contains(message, "The 3 entries are in this answer and nowhere else") {
				t.Errorf("%s answered without the plural message: %s", what, message)
			}
		}
	}
}

// TestPopCeilingRefusalWhoseConcernTimesOutIsNotApplied pins BUG-0557's rule
// for the first refusal whose words promise that nothing was removed: a pop
// over the ceiling whose concern all then times out is answered with the
// refusal and not_applied, never TIMEOUT and applied, through the owner and
// through a member. Guard: revert BUG-0557's branch in Cluster.answerShortfall.
func TestPopCeilingRefusalWhoseConcernTimesOutIsNotApplied(t *testing.T) {
	tc := ceilingCluster(t, map[int]int{0: 1 << 20, 1: 1 << 20, 2: 1 << 20}, func(_ int, c *Cluster) { c.wcTimeout = time.Second })
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 5, popPad)
	defer holdShipsTo(tc.addrs[held])()

	for _, via := range []int{owner, coordinator} {
		what := fmt.Sprintf("a pop over the ceiling at concern all through node %d", via+1)
		refusal := errorAnswerOf(t, tc.frame(via, subject.popOp(), subject.pop(5)), what)
		requireOutcome(t, refusal, what, outcomeNotApplied, transport.ErrResultTooLarge)
		if !strings.Contains(refusal.Message, "so nothing was removed") {
			t.Errorf("%s was answered with another message: %s", what, refusal.Message)
		}
	}
	if held := subject.heldOn(t, tc, owner); len(held) != 5 {
		t.Fatalf("the owner holds %d entries after two refused pops, want 5", len(held))
	}
}

// TestPopLimitRefusedForAReaderOnACluster is TestPopLimitRefusedForAReader
// through a member of a cluster: a reader's pop({limit: 3}) is refused at the
// gate with PERMISSION_DENIED, no node's log moves, and every copy keeps its
// three entries. Guard: OpFIFOPop at ActionRead in server/authz.go.
func TestPopLimitRefusedForAReaderOnACluster(t *testing.T) {
	tc := ceilingCluster(t, nil, nil)
	defer tc.stop()
	const owner, member = 0, 1
	subject := fifoPopSubject.createOn(t, tc, owner)
	subject.push(t, tc, owner, 1, 3, 0)
	grant := UserRole{Role: "reader", DB: testDatabase}
	tc.ok(clusterMasterIndex(t, tc), transport.OpUserCreate, clusterVerbsCreateRequest(t, grant))
	reader := dialReplicatedUser(t, tc.driverAddrs[member], clusterVerbsUsername(grant))

	before := tc.quiescedSequences(t)
	if err := reader.send(transport.NewFrame(subject.popOp(), mustMarshalBSON(t, subject.pop(3)))); err != nil {
		t.Fatal(err)
	}
	answer, err := reader.read()
	if err != nil {
		t.Fatal(err)
	}
	refusal := errorAnswerOf(t, answer, "a reader's pop({limit: 3})")
	if transport.ErrorCode(refusal.Code) != transport.ErrPermissionDenied {
		t.Errorf("a reader's pop was answered code %d, want PERMISSION_DENIED: %s", refusal.Code, refusal.Message)
	}
	if after := tc.quiescedSequences(t); !slices.Equal(after, before) {
		t.Errorf("a reader's refused pop moved the nodes' logs from %v to %v", before, after)
	}
	if held := subject.requireCopiesAgree(t, tc, "a reader's pop"); len(held) != 3 {
		t.Fatalf("a reader's refused pop left %d entries, want 3", len(held))
	}
}

// TestPopCeilingClampsTheStamp: a logged pop is judged by its stamp clamped to
// the range the setting is clamped to, so no stamp makes an applier hold more
// than 24 MiB or less than 1 MiB; an unlogged pop, or a logged one with no
// stamp, is judged by the node's own ceiling. Guards: either bound of the
// clamp in Server.popCeiling.
func TestPopCeilingClampsTheStamp(t *testing.T) {
	s := newTestServer(t)
	logged := withApplySeq(context.Background(), 7)
	for _, row := range []struct {
		what      string
		ctx       context.Context
		requested int64
		want      int64
		stamped   bool
	}{
		{"an unlogged pop naming a ceiling", context.Background(), 4 << 20, s.maxResultBytes(), false},
		{"a logged pop with no stamp", logged, 0, s.maxResultBytes(), false},
		{"a logged pop stamped within the range", logged, 4 << 20, 4 << 20, true},
		{"a logged pop stamped below 1 MiB", logged, 1000, config.MinMaxResultBytes, true},
		{"a logged pop stamped above 24 MiB", logged, 1 << 30, config.MaxMaxResultBytes, true},
	} {
		if ceiling, stamped := s.popCeiling(row.ctx, row.requested); ceiling != row.want || stamped != row.stamped {
			t.Errorf("%s (%d) is judged at %d, stamped %v; want %d, %v", row.what, row.requested, ceiling, stamped, row.want, row.stamped)
		}
	}
}

// TestFieldEmptyPopWaitsForNoSync is the field form of TestEmptyPopWaitsForNoSync:
// pop() and pop({limit: 5}) of an empty store field commit nothing and wait
// for no sync. Guard: emptyPop false for a field pop in
// Server.writeFieldLocked.
func TestFieldEmptyPopWaitsForNoSync(t *testing.T) {
	s, kv := newCountingServer(t)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "posts", Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})}); err != nil || !resp.Success {
		t.Fatalf("create posts: %+v, %v", resp, err)
	}
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "posts", Key: "p1", Data: map[string]interface{}{"title": "p1"}})
	for _, limit := range []int{0, 5} {
		before := kv.syncs.Load()
		handlerSend(t, handler, transport.OpFieldPop, FieldPopRequest{FieldRequest: FieldRequest{Database: "app", Bucket: "posts", Key: "p1", Field: "Comments"}, Limit: limit})
		if got := kv.syncs.Load() - before; got != 0 {
			t.Errorf("an empty field pop with limit %d waited for %d syncs, want 0", limit, got)
		}
	}
}
