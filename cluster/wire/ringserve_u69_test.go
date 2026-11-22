package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"runtime"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u69Pair is the pair record of the pair a document of the test bucket lives in.
func u69Pair(env *u66Env, id string, t *testing.T) cluster.Pair {
	t.Helper()
	return env.node.pairs.Snapshot(env.ref.slot(u66EncID(t, id)), env.ref.dbID)
}

// u69Moved reports whether the pair took a position or was fenced between before and after.
// Its flags may change: the first touch of a pair marks it replayed and stop-proved.
func u69Moved(before, after cluster.Pair) bool {
	return before.AppliedPos != after.AppliedPos || before.AppendedSeq != after.AppendedSeq ||
		before.Ordinal != after.Ordinal || after.Flags&flagReplayNeeded != 0
}

// u69RefusedWithoutMoving sends req, expects a refusal with code, and checks that the pair
// took no position and is still writable by a good insert.
func u69RefusedWithoutMoving(t *testing.T, env *u66Env, c *u66Conn, id string, req RingKeyedRequest, code cluster.Code) RingError {
	t.Helper()
	return u69FrameRefusedWithoutMoving(t, env, c, id, func() *transport.Frame { return c.keyed(req) }, code)
}

// u69FrameRefusedWithoutMoving is u69RefusedWithoutMoving for a request sent by send.
func u69FrameRefusedWithoutMoving(t *testing.T, env *u66Env, c *u66Conn, id string, send func() *transport.Frame, code cluster.Code) RingError {
	t.Helper()
	before := u69Pair(env, id, t)
	refusal := u66Refusal(t, send(), code)
	if after := u69Pair(env, id, t); u69Moved(before, after) {
		t.Fatalf("the pair moved: %+v before, %+v after", before, after)
	}
	// The same pair takes an ordinary write afterwards.
	u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, id+"-ok", "fine")))
	if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, id+"-ok", "", 0))); got.Found == nil || !*got.Found {
		t.Fatalf("the pair is not writable after the refusal: %+v", got)
	}
	return refusal
}

func TestU69InsertRefusesAFieldEntryThatIsTheHeader(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, class := range []byte{cluster.OpInsert, cluster.OpUpsert} {
		for _, fieldID := range []uint16{1} {
			id := fmt.Sprintf("hdr%d", class)
			req := env.doc(t, class, id, "x")
			req.Op.Fields = append(req.Op.Fields, cluster.FieldOp{FieldID: fieldID, Value: stringValue("again")})
			refusal := u69RefusedWithoutMoving(t, env, c, id, req, cluster.CodeInvalidArgument)
			if !strings.Contains(refusal.Message, "_id_") {
				t.Errorf("class 0x%02x field %d: the refusal does not name _id_: %q", class, fieldID, refusal.Message)
			}
		}
	}
}

// TestU69InsertRefusesFieldIDZeroOnTheWire patches the id of a field entry to 0 after
// encoding, because the encoder refuses to write it.
func TestU69InsertRefusesFieldIDZeroOnTheWire(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	req := env.doc(t, cluster.OpInsert, "zero", "x")
	req.Op.Fields = append(req.Op.Fields, cluster.FieldOp{FieldID: 0x7A7B, Value: stringValue("again")})
	payload, err := EncodeRingKeyed(req)
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(payload, []byte{0x7A, 0x7B})
	if at < 0 {
		t.Fatal("the field id is not in the payload")
	}
	payload[at], payload[at+1] = 0, 0
	u69FrameRefusedWithoutMoving(t, env, c, "zero", func() *transport.Frame {
		return c.send(transport.OpRingKeyed, u66ID(), payload)
	}, cluster.CodeInvalidArgument)
}

// TestU69NodePathRefusesAFieldEntryThatIsTheHeader sends the same entry through Node.send,
// the path the ring handler and the Node API share.
func TestU69NodePathRefusesAFieldEntryThatIsTheHeader(t *testing.T) {
	env := u66Start(t)
	ctx := context.Background()
	for _, class := range []byte{cluster.OpInsert, cluster.OpUpsert} {
		op := env.ref.template()
		op.Class, op.EncID, op.IDValue = class, u66EncID(t, "nodepath"), stringValue("nodepath")
		op.Fields = []cluster.FieldOp{{FieldID: 1, Value: stringValue("again")}}
		opts, err := newWriteOptions(nil)
		if err != nil {
			t.Fatal(err)
		}
		before := u69Pair(env, "nodepath", t)
		if _, err := env.node.send(ctx, env.ref, op.EncID, op, opts); err == nil {
			t.Fatalf("class 0x%02x: a field entry with id 1 was accepted", class)
		}
		if after := u69Pair(env, "nodepath", t); u69Moved(before, after) {
			t.Fatalf("class 0x%02x: the pair moved: %+v, %+v", class, before, after)
		}
	}
	if _, err := env.node.Insert(ctx, u30iDB, "Box", bson.D{{Key: "_id_", Value: "nodepath"}, {Key: "Name", Value: "ok"}}); err != nil {
		t.Fatalf("the pair is not writable: %v", err)
	}
}

// u69StartSmallBatch serves a node whose memtable is at its 32 MiB floor, so one apply
// batch holds about 5 MiB, on the same TLS listener as u66Start.
func u69StartSmallBatch(t *testing.T) *u66Env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	node, err := OpenNode(ctx, NodeConfig{Dir: t.TempDir(), MemTableMB: 32})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf("closing the node: %v", err)
		}
	})
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Box", Field{Name: "Name", Type: "string"}, Field{Name: "Note2", Type: "string"},
		Field{Name: "Note3", Type: "string"}, Field{Name: "Note4", Type: "string"}, Field{Name: "Note5", Type: "string"},
		Field{Name: "Note6", Type: "string"}, Field{Name: "Note7", Type: "string"}, Field{Name: "Note8", Type: "string"},
		Field{Name: "Queue", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	return u66Serve(t, node)
}

func TestU69InsertLargerThanOneBatchIsRefusedBeforeAPosition(t *testing.T) {
	env := u69StartSmallBatch(t)
	c := env.login(t)
	if limit := env.node.kv.MaxBatchBytes(); limit > 6<<20 {
		t.Fatalf("batch bound %d is too large for this test's values", limit)
	}
	// Eight values of 700 KiB are each under Badger's value threshold, so they count in
	// full against the batch: 5.6 MiB against about 5 MiB.
	many := func(class byte, id string) RingKeyedRequest {
		req := env.doc(t, class, id, strings.Repeat("x", 700<<10))
		for i := 2; i <= 8; i++ {
			req.Op.Fields = append(req.Op.Fields, cluster.FieldOp{FieldID: env.fieldID(t, fmt.Sprintf("Note%d", i)), Value: stringValue(strings.Repeat("x", 700<<10))})
		}
		return req
	}
	for _, class := range []byte{cluster.OpInsert, cluster.OpUpsert} {
		id := fmt.Sprintf("big-%d", class)
		refusal := u69RefusedWithoutMoving(t, env, c, id, many(class, id), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "batch") {
			t.Errorf("the refusal does not say a batch is the limit: %q", refusal.Message)
		}
	}
	// A single value past the bound is kept in the value log and is not refused.
	u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, "one-big-value", strings.Repeat("y", 6<<20))))
}

// TestU69InsertBatchBoundOnAFaultStore checks the same bound where every value counts in full.
func TestU69InsertBatchBoundOnAFaultStore(t *testing.T) {
	kv := kvfault.New()
	kv.SetMaxBatchBytes(64 << 10)
	applier := NewDocumentApplier(kv, nil)
	op := cluster.Op{Class: cluster.OpInsert, BucketID: 7, EncID: u66EncID(t, "a"), IDValue: stringValue("a")}
	op.Fields = []cluster.FieldOp{{FieldID: 2, Value: stringValue(strings.Repeat("x", 40<<10))}}
	if code, err := applier.Precondition(1, 1, op); err != nil || code != 0 {
		t.Fatalf("a 40 KiB document under a 64 KiB bound: code %v, %v", code, err)
	}
	op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 3, Value: stringValue(strings.Repeat("x", 40<<10))})
	if code, err := applier.Precondition(1, 1, op); err != nil || code != cluster.CodeInvalidArgument {
		t.Fatalf("two 40 KiB values under a 64 KiB bound: code %v, %v, want INVALID_ARGUMENT", code, err)
	}
}

// u69PushBig pushes count elements of about size bytes to b1's Queue.
func u69PushBig(t *testing.T, env *u66Env, c *u66Conn, count, size int) {
	t.Helper()
	element := u66Item(t, strings.Repeat("e", size))
	arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(element) })
	for range count {
		u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, arg)))
	}
}

func TestU69PeekStopsAtTheReplyBoundInsteadOfBuildingIt(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u69PushBig(t, env, c, 40, 1000<<10)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	refusal := u66Refusal(t, c.keyed(env.read(t, RingClassPeek, "b1", "Queue", 100)), cluster.CodeInvalidArgument)
	runtime.ReadMemStats(&after)
	if !strings.Contains(refusal.Message, "Nothing was read") {
		t.Errorf("the refusal does not come from the read: %q", refusal.Message)
	}
	// 40 elements are 40 MiB (329 MiB allocated before the bound, 67 MiB after); the refusal is made
	// before the element that passes the 32 MiB bound is copied.
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 120<<20 {
		t.Errorf("a 40-byte peek allocated %d MiB", spent>>20)
	}
	// A peek inside the bound still works on the same connection.
	small := u66Reply(t, c.keyed(env.read(t, RingClassPeek, "b1", "Queue", 5)))
	if len(small.Elements) != 5 {
		t.Fatalf("a peek of 5 returned %d elements", len(small.Elements))
	}
}

func TestU69ClaimReplyIsBoundedByTheDeliveryBudget(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u69PushBig(t, env, c, 12, 1000<<10)
	claim := env.header()
	claim.Class, claim.EncID, claim.Count = cluster.OpClaim, u66EncID(t, "b1"), 100
	claim.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, "Queue")}}
	claimed := u66Reply(t, c.keyed(RingKeyedRequest{Op: claim}))
	if len(claimed.Lease) != 16 || len(claimed.Elements) == 0 || len(claimed.Elements) >= 12 {
		t.Fatalf("a claim of 100 on twelve 1 MB messages leased %d with lease %x: the store's delivery budget stops it short", len(claimed.Elements), claimed.Lease)
	}
	total := 0
	for _, element := range claimed.Elements {
		total += len(element)
	}
	if total >= maxSubScanBody {
		t.Fatalf("a claim reply carries %d bytes, past a frame", total)
	}
}

func TestU69AReusedRequestIDWithAnotherBodyIsRefused(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	send := func(id [16]byte, req RingKeyedRequest) *transport.Frame {
		payload, err := EncodeRingKeyed(req)
		if err != nil {
			t.Fatal(err)
		}
		return c.send(transport.OpRingKeyed, id, payload)
	}
	id := u66ID()
	first := u66Reply(t, send(id, env.doc(t, cluster.OpInsert, "mine", "Mine")))

	del := env.header()
	del.Class, del.EncID = cluster.OpDelete, u66EncID(t, "mine")
	push := env.verb(t, "mine", "Queue", cluster.VerbPush, u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, "lost")) }))
	sameDocOtherValue := env.doc(t, cluster.OpInsert, "mine", "Changed")
	for name, req := range map[string]RingKeyedRequest{
		"a delete of the same document": {Op: del},
		"a push":                        push,
		"the same document, new value":  sameDocOtherValue,
	} {
		before := u69Pair(env, "mine", t)
		refusal := u66Refusal(t, send(id, req), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "different operation") {
			t.Errorf("%s: refusal %q does not say the id named another operation", name, refusal.Message)
		}
		if after := u69Pair(env, "mine", t); u69Moved(before, after) {
			t.Errorf("%s: the pair moved", name)
		}
	}
	// Nothing the refused requests asked for happened.
	if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "mine", "", 0))); got.Found == nil || !*got.Found ||
		bson.Raw(got.Elements[0]).Lookup("Name").StringValue() != "Mine" {
		t.Fatalf("the document changed: %+v", got)
	}
	if peeked := u66Reply(t, c.keyed(env.read(t, RingClassPeek, "mine", "Queue", 5))); len(peeked.Elements) != 0 {
		t.Fatal("the push was applied")
	}
	// The exact retry is still answered from the entry.
	again := u66Reply(t, send(id, env.doc(t, cluster.OpInsert, "mine", "Mine")))
	if !again.Deduped || *again.Position != *first.Position {
		t.Fatalf("the exact retry: %+v, first %+v", again, first)
	}
}

func TestU69TheFingerprintIgnoresWhatTheOwnerStamps(t *testing.T) {
	op := cluster.Op{Class: cluster.OpClaim, BucketID: 4, EncID: []byte{1}, Count: 2, Fields: []cluster.FieldOp{{FieldID: 3}}}
	stamped := op
	stamped.Now, stamped.Nonce, stamped.DeclVersion, stamped.SchemaVersionSeen = 99, [16]byte{7}, 5, 6
	stamped.PrevEnd = cluster.Position{Epoch: 1, Seq: 2}
	if opFingerprint(op) != opFingerprint(stamped) {
		t.Fatal("a stamped op hashes differently from the op the client sent")
	}
	changed := op
	changed.Count = 3
	if opFingerprint(op) == opFingerprint(changed) {
		t.Fatal("a claim of another size hashes the same")
	}
}

// TestU69ConcurrentClaimsWithOneRequestIDShareNoCapture races the same claim, same request
// id, from two goroutines. The pair applies it once; the attempt that applied it gets the
// messages with the lease, the other is answered from the dedupe entry with the same lease
// and none. Before the capture went through the call both could end up with the other's.
func TestU69ConcurrentClaimsWithOneRequestIDShareNoCapture(t *testing.T) {
	env := u66Start(t)
	ctx := context.Background()
	call, err := env.node.fieldCall(ctx, StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}, "claim")
	if err != nil {
		t.Fatal(err)
	}
	for round := range 30 {
		for _, label := range []string{"x", "y"} {
			if err := env.node.Push(ctx, PushRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}, Element: u63Item(fmt.Sprintf("%s%d", label, round))}); err != nil {
				t.Fatal(err)
			}
		}
		id := [16]byte{0xC1, byte(round)}
		type attempt struct {
			lease  Lease
			answer WriteAnswer
			err    error
		}
		results := make(chan attempt, 2)
		start := make(chan struct{})
		for range 2 {
			go func() {
				<-start
				lease, answer, err := env.node.claimField(ctx, call, 2, writeOptions{requestID: id})
				results <- attempt{lease, answer, err}
			}()
		}
		close(start)
		first, second := <-results, <-results
		for _, one := range []attempt{first, second} {
			if one.err != nil {
				t.Fatalf("round %d: %v", round, one.err)
			}
		}
		if first.lease.ID != second.lease.ID {
			t.Fatalf("round %d: the two attempts hold different leases %x and %x", round, first.lease.ID, second.lease.ID)
		}
		withElements := 0
		for _, one := range []attempt{first, second} {
			if len(one.lease.Elements) > 0 {
				withElements++
				if one.lease.Deduped {
					t.Fatalf("round %d: the deduped attempt carries elements", round)
				}
			}
		}
		if withElements != 1 {
			t.Fatalf("round %d: %d attempts carry the leased elements, want exactly the one that applied", round, withElements)
		}
		if err := env.node.Ack(ctx, AckRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "Box", ID: "b1", Field: "Queue"}, Lease: first.lease.ID}); err != nil {
			t.Fatal(err)
		}
	}
}

// u69NameReader counts the reverse-index reads and fails them for the databases in broken.
type u69NameReader struct {
	DeclarationReader
	mu     sync.Mutex
	reads  int
	broken map[uint64]bool
}

func (r *u69NameReader) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	r.mu.Lock()
	r.reads++
	down := r.broken[read.DBID]
	r.mu.Unlock()
	if down {
		return "", false, errors.New("the node that owns this database's bucket index is down")
	}
	return r.DeclarationReader.ReadBucketName(ctx, owner, read)
}

func (r *u69NameReader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// u69WatchNames puts a counting, breakable reader under the node's fetcher.
func u69WatchNames(env *u66Env) *u69NameReader {
	watcher := &u69NameReader{DeclarationReader: env.node.fetcher.cfg.Reader, broken: map[uint64]bool{}}
	env.node.fetcher.cfg.Reader = watcher
	return watcher
}

func TestU69AnUnreachableDatabaseDoesNotHideAnExistingBucket(t *testing.T) {
	env := u66Start(t)
	ctx := context.Background()
	other, err := env.node.CreateDatabase(ctx, "zzother")
	if err != nil {
		t.Fatal(err)
	}
	env.node.topology.Refresh()
	watcher := u69WatchNames(env)
	watcher.broken[other] = true
	c := env.login(t)
	// Map order is random, so repeat until each order has been tried many times over.
	for range 30 {
		env.node.bucketIDs.reset()
		got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "b1", "", 0)))
		if got.Found == nil || !*got.Found {
			t.Fatalf("an existing bucket is unreadable while database zzother is down: %+v", got)
		}
	}
	// A bucket id that exists nowhere is not claimed absent while a database cannot answer.
	unknown := env.read(t, RingClassGet, "b1", "", 0)
	unknown.Read.BucketID ^= 0x5A5A
	refusal := u66Refusal(t, c.keyed(unknown), cluster.CodeUnavailable)
	if strings.Contains(refusal.Message, "zzother") {
		t.Errorf("the refusal names a database the session may not hold: %q", refusal.Message)
	}
}

func TestU69BucketIDsAreLookedUpOnceAndStopAtTheFirstMatch(t *testing.T) {
	env := u66Start(t)
	ctx := context.Background()
	for i := range 5 {
		if _, err := env.node.CreateDatabase(ctx, fmt.Sprintf("extra%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	env.node.topology.Refresh()
	watcher := u69WatchNames(env)
	c := env.login(t)
	for range 20 {
		u66Reply(t, c.keyed(env.read(t, RingClassGet, "b1", "", 0)))
	}
	// Six databases: the first request reads at most six indexes, and the other nineteen none.
	if reads := watcher.count(); reads > 6 {
		t.Fatalf("20 requests for one bucket made %d reverse-index reads, want at most 6 (one scan) and then none", reads)
	}
}

func TestU69ACachedBucketIDIsCheckedAgainstTheDeclarationBeforeItIsTrusted(t *testing.T) {
	env := u66Start(t)
	ctx := context.Background()
	if _, err := env.node.CreateBucket(ctx, u30iDB, "Other", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	c := env.login(t)
	id := env.ref.entry.Decl.BucketID
	// A name that does not exist, a bucket of another id, and a database that does not exist.
	for _, poisoned := range []struct{ db, name string }{{u30iDB, "Gone"}, {u30iDB, "Other"}, {"gone", "Box"}} {
		env.node.bucketIDs.put(id, poisoned.db, poisoned.name)
		got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "b1", "", 0)))
		if got.Found == nil || !*got.Found {
			t.Fatalf("a stale cache entry %v made a bucket unreadable: %+v", poisoned, got)
		}
	}
}

func TestU69TheBucketIDCacheIsBounded(t *testing.T) {
	cache := newBucketIDCache(4)
	for id := uint64(1); id <= 10; id++ {
		cache.put(id, "db", fmt.Sprint(id))
	}
	if cache.len() != 4 {
		t.Fatalf("a cache of 4 holds %d", cache.len())
	}
	if _, _, ok := cache.get(1); ok {
		t.Fatal("the oldest entry survived")
	}
	if _, name, ok := cache.get(10); !ok || name != "10" {
		t.Fatal("the newest entry is gone")
	}
	cache.get(7)
	cache.put(11, "db", "11")
	if _, _, ok := cache.get(7); !ok {
		t.Fatal("a recently read entry was evicted before an older one")
	}
}

// u69Nested is a document {v: {v: ... {v: 1}}} depth levels deep.
func u69Nested(depth int) []byte {
	doc := bsoncore.BuildDocument(nil, bsoncore.AppendInt32Element(nil, "v", 1))
	for range depth - 1 {
		doc = bsoncore.BuildDocument(nil, bsoncore.AppendDocumentElement(nil, "v", doc))
	}
	return doc
}

func TestU69AVerbArgIsValidatedBeforeItIsUnmarshalled(t *testing.T) {
	// The ring codec and the owner's decoder each read client bytes; both must run the
	// library's bounded validator first, so a deep or lying document is refused by it
	// and never reaches the unmarshaller.
	deep := u69Nested(100)
	if err := ringCheckVerbArg(cluster.VerbPush, deep); err == nil || !strings.Contains(err.Error(), "valid BSON") {
		t.Errorf("ringCheckVerbArg of a 100-deep document: %v, want the validator's refusal", err)
	}
	var doc putArgDoc
	if err := decodeVerbDoc(deep, &doc); err == nil || !strings.Contains(err.Error(), "valid BSON") {
		t.Errorf("decodeVerbDoc of a 100-deep document: %v, want the validator's refusal", err)
	}
	lying := append([]byte(nil), u66Item(t, "x")...)
	lying[0] += 40 // the length says more than the bytes hold
	if err := ringCheckVerbArg(cluster.VerbPush, lying); err == nil || !strings.Contains(err.Error(), "valid BSON") {
		t.Errorf("ringCheckVerbArg of a lying length: %v, want the validator's refusal", err)
	}
	good, err := encodeElementArg(u66Item(t, "ok"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ringCheckVerbArg(cluster.VerbPush, good); err != nil {
		t.Errorf("a good arg is refused: %v", err)
	}
}

// TestU69ARingReplyFitsAFrameWithItsHeader sizes a reply to the last bytes below the
// payload ceiling: a frame's length counts its opcode and request id too, so the payload
// may be at most 17 bytes under maxSubScanBody.
func TestU69ARingReplyFitsAFrameWithItsHeader(t *testing.T) {
	limit := transport.MaxFrameLength - 1 - transport.RequestIDLen
	sized := func(payloadBytes int) (RingReply, int) {
		reply := RingReply{Found: ringTrue(), Elements: [][]byte{make([]byte, payloadBytes)}}
		raw, err := bson.Marshal(ringReplyDocOf(reply))
		if err != nil {
			t.Fatal(err)
		}
		return reply, len(raw)
	}
	_, overhead := sized(0)
	for _, over := range []int{1, 8, 17} {
		reply, size := sized(limit + over - overhead)
		if size != limit+over {
			t.Fatalf("sized a reply of %d bytes, wanted %d", size, limit+over)
		}
		if _, err := EncodeRingReply(reply); err == nil {
			t.Errorf("a reply %d bytes over the frame's payload room was encoded", over)
		}
	}
	reply, size := sized(limit - overhead)
	if size != limit {
		t.Fatalf("sized a reply of %d bytes, wanted %d", size, limit)
	}
	raw, err := EncodeRingReply(reply)
	if err != nil {
		t.Fatalf("a reply that exactly fills the frame is refused: %v", err)
	}
	if got := len(transport.NewFrameWithID(transport.OpRingReply, [16]byte{}, raw).Encode()); got != 5+transport.MaxFrameLength {
		t.Errorf("the frame is %d bytes, want %d (version, length and a full 32 MiB body)", got, 5+transport.MaxFrameLength)
	}
}
