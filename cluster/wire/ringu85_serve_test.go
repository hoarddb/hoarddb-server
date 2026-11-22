package wire

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u85StoreGet(env *u66Env, t *testing.T, id, field, key string) RingKeyedRequest {
	req := env.read(t, RingClassStoreGet, id, field, 0)
	req.Read.Key = key
	return req
}

func u85Pop(t *testing.T, env *u66Env, field string, n int) RingKeyedRequest {
	return env.verb(t, "b1", field, cluster.VerbPop, u66Arg(t, func() ([]byte, error) { return encodePopArg(n) }))
}

func u85Items(t *testing.T, reply RingReply) string {
	t.Helper()
	var labels []string
	for _, element := range reply.Elements {
		labels = append(labels, u62Item(t, element))
	}
	return strings.Join(labels, ",")
}

// TestU85StoreGetReadsOneElementByKeyOrEntryID: a hash, btree or heap element is read by
// its key with no position and no log record. [GUARD the per-engine read, found=false on a miss]
func TestU85StoreGetReadsOneElementByKeyOrEntryID(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, field := range []string{"Table", "Tree"} {
		for _, key := range []string{"k1", "k2"} {
			put := u66Arg(t, func() ([]byte, error) { return encodePutArg(key, u66Item(t, field+"-"+key)) })
			u66Reply(t, c.keyed(env.verb(t, "b1", field, cluster.VerbPut, put)))
		}
	}
	for _, entry := range []struct {
		id       string
		priority int64
	}{{"e1", 5}, {"e2", 1}} {
		push := u66Arg(t, func() ([]byte, error) {
			return encodeHeapPushArg(entry.priority, entry.id, u66Item(t, "heap-"+entry.id))
		})
		u66Reply(t, c.keyed(env.verb(t, "b1", "Ranked", cluster.VerbPush, push)))
	}
	before := u63Position(env.node)
	for _, tc := range []struct{ field, key, want string }{
		{"Table", "k2", "Table-k2"}, {"Tree", "k1", "Tree-k1"}, {"Ranked", "e2", "heap-e2"}, {"Ranked", "e1", "heap-e1"},
	} {
		reply := u66Reply(t, c.keyed(u85StoreGet(env, t, "b1", tc.field, tc.key)))
		if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 || u62Item(t, reply.Elements[0]) != tc.want ||
			reply.WriteOutcome != "" || reply.Position != nil || len(reply.Keys) != 0 {
			t.Fatalf("store_get %s/%s = %+v, want found with %q and no write members", tc.field, tc.key, reply, tc.want)
		}
	}
	for _, tc := range []struct{ field, key string }{{"Table", "absent"}, {"Tree", "absent"}, {"Ranked", "absent"}} {
		reply := u66Reply(t, c.keyed(u85StoreGet(env, t, "b1", tc.field, tc.key)))
		if reply.Found == nil || *reply.Found || len(reply.Elements) != 0 {
			t.Fatalf("store_get of absent %s/%s = %+v, want found=false", tc.field, tc.key, reply)
		}
	}
	if after := u63Position(env.node); after != before {
		t.Fatalf("a store_get moved the pair from %+v to %+v: a read takes no position", before, after)
	}
	u66Refusal(t, c.keyed(u85StoreGet(env, t, "nobody", "Table", "k1")), cluster.CodeNotFound)
}

// TestU85StoreGetOfAQueueOrBlobNamesTheVerbThatFits: [GUARD takesVerb get]
func TestU85StoreGetOfAQueueOrBlobNamesTheVerbThatFits(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for field, said := range map[string][]string{
		"Queue": {"fifoStore", "hashStore", "lease_get"}, "Stack": {"lifoStore", "btreeStore", "lease_get"}, "Files": {"blobStore", "hashStore", "count"},
	} {
		refusal := u66Refusal(t, c.keyed(u85StoreGet(env, t, "b1", field, "k")), cluster.CodeInvalidArgument)
		for _, word := range said {
			if !strings.Contains(refusal.Message, word) {
				t.Errorf("store_get on %s: %q does not name %q", field, refusal.Message, word)
			}
		}
	}
	scalar := u66Refusal(t, c.keyed(u85StoreGet(env, t, "b1", "Name", "k")), cluster.CodeFieldType)
	if scalar.Message == "" {
		t.Fatal("store_get on a scalar field gave no message")
	}
}

// TestU85ListAndPeekCarryKeysAndPriorities: [GUARD keys parallel to elements, per engine]
func TestU85ListAndPeekCarryKeysAndPriorities(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, key := range []string{"k3", "k1", "k2"} {
		for _, field := range []string{"Table", "Tree"} {
			put := u66Arg(t, func() ([]byte, error) { return encodePutArg(key, u66Item(t, field+"-"+key)) })
			u66Reply(t, c.keyed(env.verb(t, "b1", field, cluster.VerbPut, put)))
		}
	}
	for _, entry := range []struct {
		id       string
		priority int64
	}{{"low", 1}, {"high", 9}, {"mid", 5}} {
		push := u66Arg(t, func() ([]byte, error) { return encodeHeapPushArg(entry.priority, entry.id, u66Item(t, entry.id)) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Ranked", cluster.VerbPush, push)))
	}
	for _, label := range []string{"a", "b"} {
		push := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, push)))
		u66Reply(t, c.keyed(env.verb(t, "b1", "Stack", cluster.VerbPush, push)))
	}
	for _, field := range []string{"Table", "Tree"} {
		list := u66Reply(t, c.keyed(env.read(t, RingClassStoreList, "b1", field, 2)))
		if strings.Join(list.Keys, ",") != "k1,k2" || len(list.Elements) != 2 || !list.More || len(list.Priorities) != 0 ||
			u62Item(t, list.Elements[0]) != field+"-k1" {
			t.Fatalf("%s list page 1 = %+v", field, list)
		}
		next := env.read(t, RingClassStoreList, "b1", field, 2)
		next.Read.After = list.Cursor
		second := u66Reply(t, c.keyed(next))
		if strings.Join(second.Keys, ",") != "k3" || u62Item(t, second.Elements[0]) != field+"-k3" {
			t.Fatalf("%s list page 2 = %+v", field, second)
		}
	}
	for _, class := range []byte{RingClassStoreList, RingClassPeek} {
		heap := u66Reply(t, c.keyed(env.read(t, class, "b1", "Ranked", 2)))
		if strings.Join(heap.Keys, ",") != "high,mid" || len(heap.Priorities) != 2 || heap.Priorities[0] != 9 || heap.Priorities[1] != 5 ||
			u85Items(t, heap) != "high,mid" {
			t.Fatalf("heap class 0x%02x = %+v, want keys high,mid and priorities 9,5", class, heap)
		}
	}
	for _, field := range []string{"Queue", "Stack"} {
		for _, class := range []byte{RingClassStoreList, RingClassPeek} {
			queue := u66Reply(t, c.keyed(env.read(t, class, "b1", field, 5)))
			if len(queue.Elements) != 2 || len(queue.Keys) != 0 || len(queue.Priorities) != 0 {
				t.Fatalf("%s class 0x%02x = %+v: a queue has no keys", field, class, queue)
			}
		}
	}
}

// TestU85PopNTakesSeveralInOrderInOneRecord: [GUARD the n elements, the order, found]
func TestU85PopNTakesSeveralInOrderInOneRecord(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, label := range []string{"1", "2", "3", "4", "5"} {
		push := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, push)))
		u66Reply(t, c.keyed(env.verb(t, "b1", "Stack", cluster.VerbPush, push)))
	}
	for _, entry := range []struct {
		id       string
		priority int64
	}{{"low", 1}, {"high", 9}, {"mid", 5}} {
		push := u66Arg(t, func() ([]byte, error) { return encodeHeapPushArg(entry.priority, entry.id, u66Item(t, entry.id)) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Ranked", cluster.VerbPush, push)))
	}
	popped := u66Reply(t, c.keyed(u85Pop(t, env, "Queue", 3)))
	if popped.Found == nil || !*popped.Found || u85Items(t, popped) != "1,2,3" || popped.WriteOutcome != "applied" || popped.Position == nil {
		t.Fatalf("pop {n: 3} = %+v, want 1,2,3 applied", popped)
	}
	rest := u66Reply(t, c.keyed(u85Pop(t, env, "Queue", 10)))
	if u85Items(t, rest) != "4,5" {
		t.Fatalf("pop {n: 10} of the 2 left = %+v", rest)
	}
	empty := u66Reply(t, c.keyed(u85Pop(t, env, "Queue", 3)))
	if empty.Found == nil || *empty.Found || len(empty.Elements) != 0 || empty.WriteOutcome != "applied" {
		t.Fatalf("pop {n: 3} of an empty queue = %+v, want found=false", empty)
	}
	if stack := u66Reply(t, c.keyed(u85Pop(t, env, "Stack", 2))); u85Items(t, stack) != "5,4" {
		t.Fatalf("a stack pop {n: 2} = %+v, want the newest first: 5,4", stack)
	}
	if heap := u66Reply(t, c.keyed(u85Pop(t, env, "Ranked", 2))); u85Items(t, heap) != "high,mid" {
		t.Fatalf("a heap pop {n: 2} = %+v, want high,mid", heap)
	}
	single := u66Reply(t, c.keyed(env.verb(t, "b1", "Stack", cluster.VerbPop, nil)))
	if single.Found == nil || !*single.Found || len(single.Elements) != 1 || u62Item(t, single.Elements[0]) != "3" {
		t.Fatalf("a pop with no arg = %+v, want today's single element 3", single)
	}
}

// TestU85RetriedPopNReturnsTheSameElementsWithoutPoppingAgain: [GUARD every element is the dedupe result]
func TestU85RetriedPopNReturnsTheSameElementsWithoutPoppingAgain(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, label := range []string{"1", "2", "3", "4"} {
		push := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, push)))
	}
	payload, err := EncodeRingKeyed(u85Pop(t, env, "Queue", 3))
	if err != nil {
		t.Fatal(err)
	}
	id := u66ID()
	first := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	retry := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	if u85Items(t, first) != "1,2,3" || u85Items(t, retry) != "1,2,3" || !retry.Deduped || *first.Position != *retry.Position {
		t.Fatalf("first %+v, retry %+v: the retry must return 1,2,3 deduped at the same position", first, retry)
	}
	left := u66Reply(t, c.keyed(env.read(t, RingClassPeek, "b1", "Queue", 10)))
	if u85Items(t, left) != "4" {
		t.Fatalf("a retried pop-n removed again: %+v", left)
	}
}

// TestU85PopNTooBigForItsDedupeEntryIsRefusedBeforeAPosition: [GUARD the whole result fits]
func TestU85PopNTooBigForItsDedupeEntryIsRefusedBeforeAPosition(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	label := strings.Repeat("x", 40)
	element := u66Item(t, label)
	packed := len(element) + 1
	fits := maxResultBytes / packed
	for range 30 {
		push := u66Arg(t, func() ([]byte, error) { return encodeElementArg(element) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, push)))
	}
	before := u63Position(env.node)
	refusal := u66Refusal(t, c.keyed(u85Pop(t, env, "Queue", 30)), cluster.CodeResultTooLarge)
	for _, word := range []string{fmt.Sprintf("Pop %d or fewer", fits), "claim", "ack", "Nothing was removed"} {
		if !strings.Contains(refusal.Message, word) {
			t.Errorf("the refusal %q does not say %q", refusal.Message, word)
		}
	}
	if after := u63Position(env.node); after != before {
		t.Fatalf("the pair moved from %+v to %+v: the refusal must come before a position", before, after)
	}
	if got := u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0))); got.Count == nil || *got.Count != 30 {
		t.Fatalf("a refused pop removed something: %+v", got)
	}
	ok := u66Reply(t, c.keyed(u85Pop(t, env, "Queue", fits)))
	if len(ok.Elements) != fits {
		t.Fatalf("pop {n: %d}, the count the refusal named, returned %d elements", fits, len(ok.Elements))
	}
}

// TestU85OwnerRefusesAPopArgItNeverSends: the node never sends n outside 1 to 100, but the
// owner still checks. [GUARD decodeVerbArg in the precondition]
func TestU85OwnerRefusesAPopArgItNeverSends(t *testing.T) {
	node, _ := u63Fresh(t)
	u62Push(t, node, "Queue", "a", "b")
	before := u63Position(node)
	for name, arg := range map[string][]byte{
		"n 0":     u63Doc(t, map[string]int32{"n": 0}),
		"n 101":   u63Doc(t, map[string]int32{"n": 101}),
		"garbage": {1, 2, 3},
	} {
		_, err := u63Raw(node, "Queue", cluster.VerbPop, arg)
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if after := u63Position(node); after != before {
			t.Fatalf("%s: the pair moved from %+v to %+v", name, before, after)
		}
	}
}

// TestU85PopNRebuildsFromTheLogAndReplayingTwiceChangesNothing: [GUARD fillVerb pops n in the log]
func TestU85PopNRebuildsFromTheLogAndReplayingTwiceChangesNothing(t *testing.T) {
	node, dir := u63Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "1", "2", "3", "4", "5")
	u62Push(t, node, "Stack", "1", "2", "3", "4", "5")
	request := [16]byte{0x85, 1}
	first, err := node.PopN(ctx, PopNRequest{StoreTarget: u62Target("Queue"), Count: 3}, WithRequestID(request))
	if err != nil || len(first.Elements) != 3 {
		t.Fatalf("PopN = %+v, %v", first, err)
	}
	if _, err := node.PopN(ctx, PopNRequest{StoreTarget: u62Target("Stack"), Count: 2}); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	loseStoreForTheReplay(t, dir)
	reopened := u30iOpen(t, dir)
	slot := reopened.slotOf(u30iDB, "Box", "b1")
	once := reopened.pairs.Snapshot(slot, 1)
	if err := reopened.tail.Scan(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if twice := reopened.pairs.Snapshot(slot, 1); twice != once {
		t.Fatalf("pair after a second scan = %+v, want %+v", twice, once)
	}
	queue, err := reopened.Peek(ctx, PeekRequest{StoreTarget: u62Target("Queue"), Count: 5})
	stack, err2 := reopened.Peek(ctx, PeekRequest{StoreTarget: u62Target("Stack"), Count: 5})
	if err != nil || err2 != nil || len(queue) != 2 || u62Item(t, queue[0]) != "4" || len(stack) != 3 || u62Item(t, stack[0]) != "3" {
		t.Fatalf("after the rebuild queue = %d elements, stack = %d elements (%v, %v)", len(queue), len(stack), err, err2)
	}
	retry, err := reopened.PopN(ctx, PopNRequest{StoreTarget: u62Target("Queue"), Count: 3}, WithRequestID(request))
	if err != nil || !retry.Deduped || retry.Pos != first.Pos || len(retry.Elements) != 3 || !bytes.Equal(retry.Elements[2], first.Elements[2]) {
		t.Fatalf("retry after the rebuild = %+v, %v, want the 3 elements deduped at %v", retry, err, first.Pos)
	}
}

// TestU85ListResumesAfterTheLargestLegalKey: a cursor is the encoded key, and each NUL of a
// 1024-byte key takes two bytes, so a legal cursor is longer than twice the key.
// [GUARD ringMaxAfter derived from the key limit]
func TestU85ListResumesAfterTheLargestLegalKey(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nuls := strings.Repeat("\x00", maxVerbKeyBytes)
	for _, field := range []string{"Table", "Tree"} {
		for _, key := range []string{nuls, "z"} {
			put := u66Arg(t, func() ([]byte, error) { return encodePutArg(key, u66Item(t, "v")) })
			u66Reply(t, c.keyed(env.verb(t, "b1", field, cluster.VerbPut, put)))
		}
	}
	for _, entry := range []struct {
		id       string
		priority int64
	}{{nuls, 5}, {"z", 1}} {
		push := u66Arg(t, func() ([]byte, error) { return encodeHeapPushArg(entry.priority, entry.id, u66Item(t, "h")) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Ranked", cluster.VerbPush, push)))
	}
	for _, field := range []string{"Table", "Tree", "Ranked"} {
		first := u66Reply(t, c.keyed(env.read(t, RingClassStoreList, "b1", field, 1)))
		if len(first.Elements) != 1 || !first.More || len(first.Keys) != 1 || first.Keys[0] != nuls {
			t.Fatalf("%s page 1 = %+v, want the NUL key and more", field, first)
		}
		next := env.read(t, RingClassStoreList, "b1", field, 1)
		next.Read.After = first.Cursor
		second := u66Reply(t, c.keyed(next))
		if len(second.Keys) != 1 || second.Keys[0] != "z" {
			t.Fatalf("%s page 2 = %+v, want the element after the NUL key", field, second)
		}
	}
}

// TestU85HeapStoreGetIsBoundedPerRequest: a heap has no index by entry id, so a keyed read walks
// it, and a reader must not walk without limit. [GUARD the step budget in Reader.GetField]
func TestU85HeapStoreGetIsBoundedPerRequest(t *testing.T) {
	old := maxHeapGetSteps
	maxHeapGetSteps = 3
	t.Cleanup(func() { maxHeapGetSteps = old })
	node, _ := u63Fresh(t)
	for i, id := range []string{"a", "b", "c", "d", "e"} {
		u63HeapPush(t, node, id, int64(100-i))
	}
	ctx := context.Background()
	for _, id := range []string{"a", "c"} {
		got, err := node.GetField(ctx, StoreGetRequest{StoreTarget: u62Target("Ranked"), Key: id})
		if err != nil || !got.Found || u62Item(t, got.Element) != id {
			t.Fatalf("GetField %s within the budget = %+v, %v", id, got, err)
		}
	}
	for _, id := range []string{"e", "absent"} {
		_, err := node.GetField(ctx, StoreGetRequest{StoreTarget: u62Target("Ranked"), Key: id})
		u30iCode(t, err, cluster.CodeInvalidArgument)
		for _, word := range []string{"too large for a keyed read", "list", "3"} {
			if !strings.Contains(err.Error(), word) {
				t.Errorf("GetField %s: %q does not say %q", id, err, word)
			}
		}
	}
}
