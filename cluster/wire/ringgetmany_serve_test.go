package wire

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// getManyWorld is the expand world's node (users u0..u6 in app.User) behind a ring server whose
// get-many reads and bucket lookups are counted. Every read still goes through the node's read
// path unless a test swaps fetch.
type getManyWorld struct {
	*expandWorld
	fetches atomic.Int32
	lookups atomic.Int32
	readsOf sync.Map // enc id -> *atomic.Int32
}

func newGetManyWorld(t *testing.T, authz RingAuthorizer) *getManyWorld {
	t.Helper()
	world := &getManyWorld{expandWorld: newExpandWorld(t, authz)}
	node := world.server.node
	world.server.resultBudget = maxRingBatchBudget
	world.server.manyFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		world.fetches.Add(1)
		counter, _ := world.readsOf.LoadOrStore(string(encID), &atomic.Int32{})
		counter.(*atomic.Int32).Add(1)
		return node.readDocumentFrom(ctx, ref, encID, project, local)
	}
	world.server.bucketLookup = func(ctx context.Context, bucketID uint64, admit func(database, bucket string) error) (bucketRef, error) {
		world.lookups.Add(1)
		return node.bucketByID(ctx, bucketID, admit)
	}
	return world
}

func (w *getManyWorld) readsOfID(t *testing.T, id string) int32 {
	t.Helper()
	counter, ok := w.readsOf.Load(string(u66EncID(t, id)))
	if !ok {
		return 0
	}
	return counter.(*atomic.Int32).Load()
}

// getManyRead is the get of id in ref, a get_fields when fields are named.
func getManyRead(t *testing.T, ref bucketRef, id string, fields ...string) RingRead {
	t.Helper()
	read := RingRead{Class: RingClassGet, BucketID: ref.entry.Decl.BucketID, DeclVersion: ref.version, SchemaVersionSeen: ref.entry.Seen, EncID: u66EncID(t, id)}
	if len(fields) > 0 {
		read.Class, read.Fields = RingClassGetFields, fields
	}
	return read
}

func (w *getManyWorld) serve(reads ...RingRead) RingReply {
	return w.server.serveGetMany(context.Background(), RingGetManyRequest{Reads: reads})
}

// getManyID is the _id_ of a found answer, or "" for found:false; it fails on an error answer.
func getManyID(t *testing.T, index int, answer RingAnswer) string {
	t.Helper()
	if answer.Error != nil {
		t.Fatalf("key %d answered %s: %s", index, answer.Error.Code, answer.Error.Message)
	}
	reply := answer.Reply
	if reply.Found == nil {
		t.Fatalf("key %d has no found member: %+v", index, reply)
	}
	if !*reply.Found {
		if len(reply.Elements) != 0 {
			t.Fatalf("key %d is found:false with %d elements", index, len(reply.Elements))
		}
		return ""
	}
	if len(reply.Elements) != 1 {
		t.Fatalf("key %d found with %d elements, want 1", index, len(reply.Elements))
	}
	return bson.Raw(reply.Elements[0]).Lookup("_id_").StringValue()
}

func getManyError(t *testing.T, index int, answer RingAnswer, code cluster.Code) RingError {
	t.Helper()
	if answer.Error == nil {
		t.Fatalf("key %d answered a reply (found %v, %d elements), want %s", index, answer.Reply.Found != nil && *answer.Reply.Found, len(answer.Reply.Elements), code)
	}
	if answer.Error.Code != code {
		t.Fatalf("key %d answered %s (%q), want %s", index, answer.Error.Code, answer.Error.Message, code)
	}
	return *answer.Error
}

// TestGetManyAnswersInRequestOrderWithMissesAsFoundFalse: three buckets, a deleted document and
// a key asked twice: one answer per key, in request order; the miss is found:false; the repeated
// key is read once and answered twice.
func TestGetManyAnswersInRequestOrderWithMissesAsFoundFalse(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	node, ctx := world.server.node, context.Background()
	for _, name := range []string{"Post", "Room"} {
		if _, err := node.CreateBucket(ctx, u30iDB, name, Field{Name: "Title", Type: "string"}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"a", "b"} {
			if _, err := node.Insert(ctx, u30iDB, name, bson.D{{Key: "_id_", Value: name + "-" + id}, {Key: "Title", Value: "t"}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := node.Delete(ctx, u30iDB, "Post", "Post-b"); err != nil {
		t.Fatal(err)
	}
	post, room := world.bucket(t, "Post"), world.bucket(t, "Room")
	reads := []RingRead{
		getManyRead(t, room, "Room-b"), getManyRead(t, world.near, "u3"), getManyRead(t, post, "Post-b"),
		getManyRead(t, post, "Post-a", "Title"), getManyRead(t, world.near, "u3"), getManyRead(t, world.near, "nobody"),
		getManyRead(t, room, "Room-a"),
	}
	reply := world.serve(reads...)
	want := []string{"Room-b", "u3", "", "Post-a", "u3", "", "Room-a"}
	if len(reply.Answers) != len(want) {
		t.Fatalf("%d answers for %d keys", len(reply.Answers), len(want))
	}
	for i, answer := range reply.Answers {
		if got := getManyID(t, i, answer); got != want[i] {
			t.Errorf("answer %d is %q, want %q", i, got, want[i])
		}
	}
	if reply.Outcome != ringBatchOK {
		t.Errorf("outcome %q, want ok: a miss is data, not an error", reply.Outcome)
	}
	if reply.Epoch == nil || reply.StateSeq == nil {
		t.Errorf("the reply carries no epoch or state_seq: %+v", reply)
	}
	if reply.Answers[0].Reply.Epoch != nil {
		t.Errorf("an answer carries an epoch; only the top level does")
	}
	if n := world.readsOfID(t, "u3"); n != 1 {
		t.Errorf("u3 asked twice was read %d times, want 1", n)
	}
	if titled := bson.Raw(reply.Answers[3].Reply.Elements[0]); titled.Lookup("Title").StringValue() != "t" {
		t.Errorf("the get_fields answer is %s", titled)
	}
}

// TestGetManyIsOneRequestOnTheWire: 100 keys are one frame in and one RING_REPLY out.
func TestGetManyIsOneRequestOnTheWire(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	reads := make([]RingRead, 100)
	for i := range reads {
		id := "b1"
		if i%2 == 1 {
			id = fmt.Sprintf("absent-%d", i)
		}
		reads[i] = *env.read(t, RingClassGet, id, "", 0).Read
	}
	payload, err := EncodeRingGetMany(RingGetManyRequest{Reads: reads})
	if err != nil {
		t.Fatal(err)
	}
	reply := ringBatchReply(t, c.send(transport.OpRingGetMany, u66ID(), payload), len(reads))
	for i, answer := range reply.Answers {
		want := "b1"
		if i%2 == 1 {
			want = ""
		}
		if got := getManyID(t, i, answer); got != want {
			t.Fatalf("answer %d is %q, want %q", i, got, want)
		}
	}
}

// getManyDenyBucket refuses every ask about one bucket and allows everything else.
type getManyDenyBucket struct{ bucket string }

func (g getManyDenyBucket) Authorize(_ context.Context, access RingAccess) error {
	if access.Bucket == g.bucket {
		return errU70NoGrant
	}
	return nil
}

// TestGetManyAuthorizesEachKey: a key on a bucket the session may not read answers the uniform
// PERMISSION_DENIED; the others are answered and the outcome is partial.
func TestGetManyAuthorizesEachKey(t *testing.T) {
	world := newGetManyWorld(t, getManyDenyBucket{bucket: "Secret"})
	node, ctx := world.server.node, context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Secret", Field{Name: "Title", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Secret", bson.D{{Key: "_id_", Value: "s1"}, {Key: "Title", Value: "x"}}); err != nil {
		t.Fatal(err)
	}
	secret := world.bucket(t, "Secret")
	reply := world.serve(getManyRead(t, world.near, "u1"), getManyRead(t, secret, "s1"), getManyRead(t, world.near, "u2"), getManyRead(t, secret, "s1", "Title"))
	if reply.Outcome != ringBatchPartial {
		t.Fatalf("outcome %q, want partial", reply.Outcome)
	}
	for _, i := range []int{1, 3} {
		refusal := getManyError(t, i, reply.Answers[i], cluster.CodePermissionDenied)
		if refusal.Message != ringRefusalOf(ringDenied()).Message || strings.Contains(refusal.Message, "Secret") {
			t.Errorf("key %d refusal %q is not the uniform text", i, refusal.Message)
		}
	}
	if getManyID(t, 0, reply.Answers[0]) != "u1" || getManyID(t, 2, reply.Answers[2]) != "u2" {
		t.Errorf("the keys the session may read were not answered: %+v", reply.Answers)
	}
	if world.fetches.Load() != 2 {
		t.Errorf("%d reads, want 2: a refused key must not be read", world.fetches.Load())
	}
}

// TestGetManyAnswersAnAbsentOrDroppedBucketUniformly: a key whose bucket id no database has, or
// whose bucket was dropped, answers the uniform PERMISSION_DENIED and the others are answered;
// 255 keys naming one unknown id make one lookup, not 255 (the refusal memo, R7).
func TestGetManyAnswersAnAbsentOrDroppedBucketUniformly(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	node, ctx := world.server.node, context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Gone", Field{Name: "Title", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	gone := world.bucket(t, "Gone")
	if err := node.DropBucket(ctx, u30iDB, "Gone"); err != nil {
		t.Fatal(err)
	}
	unknown := getManyRead(t, world.near, "u1")
	unknown.BucketID = 0x5eed_0000_0000_0001
	dropped := getManyRead(t, gone, "g1")

	reply := world.serve(getManyRead(t, world.near, "u0"), unknown, dropped, getManyRead(t, world.near, "u4"))
	for _, i := range []int{1, 2} {
		if refusal := getManyError(t, i, reply.Answers[i], cluster.CodePermissionDenied); refusal.Message != ringRefusalOf(ringDenied()).Message {
			t.Errorf("key %d refusal %q is not the uniform text", i, refusal.Message)
		}
	}
	if getManyID(t, 0, reply.Answers[0]) != "u0" || getManyID(t, 3, reply.Answers[3]) != "u4" {
		t.Fatalf("the keys of live buckets were not answered")
	}

	reads := make([]RingRead, maxGetManyKeys)
	for i := range reads {
		reads[i] = unknown
	}
	reads[100] = getManyRead(t, world.near, "u5")
	world.lookups.Store(0)
	reply = world.serve(reads...)
	if n := world.lookups.Load(); n != 2 {
		t.Fatalf("255 keys over one unknown id and one live bucket made %d bucket lookups, want 2", n)
	}
	for i, answer := range reply.Answers {
		if i == 100 {
			if getManyID(t, i, answer) != "u5" {
				t.Fatalf("key 100 was not answered")
			}
			continue
		}
		getManyError(t, i, answer, cluster.CodePermissionDenied)
	}
}

// TestGetManyAcrossDatabasesAnswersEachKey: one frame over two databases; each key is read in
// its own database. (Different replication factors need a cluster: step G7.)
func TestGetManyAcrossDatabasesAnswersEachKey(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	node, ctx := world.server.node, context.Background()
	if _, err := node.CreateDatabase(ctx, "ref"); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, "ref", "User", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, "ref", "User", bson.D{{Key: "_id_", Value: "u1"}, {Key: "Name", Value: "Other Ada"}}); err != nil {
		t.Fatal(err)
	}
	other, err := node.bucket(ctx, "ref", "User")
	if err != nil {
		t.Fatal(err)
	}
	reply := world.serve(getManyRead(t, other, "u1"), getManyRead(t, world.near, "u1"))
	names := []string{"Other Ada", "Name1"}
	for i, answer := range reply.Answers {
		getManyID(t, i, answer)
		if got := bson.Raw(answer.Reply.Elements[0]).Lookup("Name").StringValue(); got != names[i] {
			t.Errorf("key %d read %q, want %q", i, got, names[i])
		}
	}
}

// TestGetManyEveryKeyGoesThroughTheReadPath: with no fetch swapped in, each distinct key is read
// by the node's Reader (one snapshot or more each), never forwarded around it: a handler that
// called ForwardRead itself would skip the gate and the hold, and on this one-node cluster it
// would fail for want of a peer.
func TestGetManyEveryKeyGoesThroughTheReadPath(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	world.server.manyFetch = nil
	reader := world.server.node.reader
	counted := &u71KV{KV: reader.cfg.KV}
	reader.cfg.KV = counted
	var reads []RingRead
	for i := 0; i < 7; i++ {
		reads = append(reads, getManyRead(t, world.near, fmt.Sprintf("u%d", i)))
	}
	reply := world.serve(reads...)
	for i, answer := range reply.Answers {
		if got := getManyID(t, i, answer); got != fmt.Sprintf("u%d", i) {
			t.Fatalf("answer %d is %q", i, got)
		}
	}
	if n := counted.snapshots.Load(); n < int64(len(reads)) {
		t.Fatalf("%d snapshots for %d keys: some key did not go through the read path", n, len(reads))
	}
}

// TestGetManyHonoursLocalPerKey: local is passed to the read of the key that asked for it and to
// no other, and the answer says where it was served: on the owner, a local key falls back to the
// owner and says so, and a key that did not ask says nothing.
func TestGetManyHonoursLocalPerKey(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	inner := world.server.manyFetch
	var mu sync.Mutex
	localOf := map[string]bool{}
	world.server.manyFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		mu.Lock()
		localOf[string(encID)] = local
		mu.Unlock()
		return inner(ctx, ref, encID, project, local)
	}
	local := getManyRead(t, world.near, "u2")
	local.Options.Local = true
	reply := world.serve(getManyRead(t, world.near, "u1"), local)
	if !localOf[string(u66EncID(t, "u2"))] || localOf[string(u66EncID(t, "u1"))] {
		t.Fatalf("local per key = %v, want only u2", localOf)
	}
	getManyID(t, 0, reply.Answers[0])
	getManyID(t, 1, reply.Answers[1])
	if reply.Answers[0].Reply.Served != "" || reply.Answers[1].Reply.Served != servedOwner {
		t.Fatalf("served_from = %q and %q, want \"\" and owner", reply.Answers[0].Reply.Served, reply.Answers[1].Reply.Served)
	}
	stats := world.server.node.GetManyStats()
	if stats.Frames.Load() != 1 || stats.Keys.Load() != 2 || stats.Errors.Load() != 0 || stats.ServedLocal.Load() != 0 {
		t.Fatalf("stats frames=%d keys=%d errors=%d local=%d", stats.Frames.Load(), stats.Keys.Load(), stats.Errors.Load(), stats.ServedLocal.Load())
	}
}

// TestGetManyAKeyErrorIsThatKeysAnswer: an owner that fails one key fails only that key; the
// others, on that owner too, are answered and the outcome is partial.
func TestGetManyAKeyErrorIsThatKeysAnswer(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	inner := world.server.manyFetch
	down := &cluster.Error{Code: cluster.CodeOwnerDown, Message: "the owner of slot 9 is down"}
	failing := string(u66EncID(t, "u3"))
	world.server.manyFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		if string(encID) == failing {
			return Document{}, down
		}
		return inner(ctx, ref, encID, project, local)
	}
	var reads []RingRead
	for i := 0; i < 7; i++ {
		reads = append(reads, getManyRead(t, world.near, fmt.Sprintf("u%d", i)))
	}
	reply := world.serve(reads...)
	if reply.Outcome != ringBatchPartial {
		t.Fatalf("outcome %q, want partial", reply.Outcome)
	}
	for i, answer := range reply.Answers {
		if i == 3 {
			if refusal := getManyError(t, i, answer, cluster.CodeOwnerDown); !strings.Contains(refusal.Message, "slot 9 is down") {
				t.Errorf("key 3 refusal %q is not the owner's", refusal.Message)
			}
			continue
		}
		getManyID(t, i, answer)
	}
	if n := world.server.node.GetManyStats().Errors.Load(); n != 1 {
		t.Errorf("errors counter %d, want 1", n)
	}
}

// TestGetManyWithMoreThanSixteenHeldKeysOnOneOwner: 30 keys parked in a hold on owner A take at
// most A's share of the frame's 16 slots; owner B's keys are read while A's are parked, and A's
// keys beyond its slots wait for the release (spec section 3.4).
func TestGetManyWithMoreThanSixteenHeldKeysOnOneOwner(t *testing.T) {
	world := newGetManyWorld(t, u70AllowAll{})
	ids := map[string][]string{}
	ownerOf := func(_ bucketRef, slot uint16) string { return fmt.Sprintf("node-%c", 'A'+rune(slot%2)) }
	for n := 0; len(ids["node-A"]) < 30 || len(ids["node-B"]) < 10; n++ {
		id := fmt.Sprintf("h%d", n)
		owner := ownerOf(world.near, world.near.slot(u66EncID(t, id)))
		if (owner == "node-A" && len(ids[owner]) < 30) || (owner == "node-B" && len(ids[owner]) < 10) {
			ids[owner] = append(ids[owner], id)
		}
	}
	release := make(chan struct{})
	var mu sync.Mutex
	inFlightA, peakA, total, peak, doneB := 0, 0, 0, 0, 0
	bDone := make(chan struct{})
	world.server.manyOwner = ownerOf
	world.server.manyFetch = func(ctx context.Context, ref bucketRef, encID []byte, _ []uint16, _ bool) (Document, error) {
		owner := ownerOf(ref, ref.slot(encID))
		mu.Lock()
		total++
		peak = max(peak, total)
		if owner == "node-A" {
			inFlightA++
			peakA = max(peakA, inFlightA)
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			defer mu.Unlock()
			total--
			if owner == "node-A" {
				inFlightA--
			} else if doneB++; doneB == 10 {
				close(bDone)
			}
		}()
		if owner == "node-A" {
			select {
			case <-release:
			case <-ctx.Done():
				return Document{}, ctx.Err()
			}
		}
		return Document{}, &cluster.Error{Code: cluster.CodeNotFound, Message: "absent"}
	}
	var reads []RingRead
	for _, id := range ids["node-A"] {
		reads = append(reads, getManyRead(t, world.near, id))
	}
	for _, id := range ids["node-B"] {
		reads = append(reads, getManyRead(t, world.near, id))
	}
	replies := make(chan RingReply, 1)
	go func() { replies <- world.serve(reads...) }()
	select {
	case <-bDone:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("owner B's keys were not read while owner A's keys were parked")
	}
	// B is done, so A has the whole frame: 16 of its 30 keys park and the other 14 wait for the
	// release. Nothing else may be launched meanwhile.
	parkedA := 0
	for wait := time.Now().Add(5 * time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		mu.Lock()
		parkedA = inFlightA
		mu.Unlock()
		if parkedA >= ringBatchInFlight {
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	parkedA = inFlightA
	mu.Unlock()
	if parkedA != ringBatchInFlight {
		close(release)
		t.Fatalf("%d of owner A's keys parked while the hold lasted, want %d", parkedA, ringBatchInFlight)
	}
	close(release)
	reply := <-replies
	for i, answer := range reply.Answers {
		if getManyID(t, i, answer) != "" {
			t.Fatalf("key %d was found", i)
		}
	}
	if peakA > ringBatchInFlight || peak > getManyFrameInFlight {
		t.Fatalf("peak in flight %d on owner A and %d in the frame, want at most %d and %d", peakA, peak, ringBatchInFlight, getManyFrameInFlight)
	}
}

// TestGetManyHandlerAnswersOnTheWire replaces the stub's pin: a decoded frame is answered with a
// RING_REPLY of one answer per key, and a frame that does not decode is still one refusal.
func TestGetManyHandlerAnswersOnTheWire(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	reads := []RingRead{*env.read(t, RingClassGet, "b1", "", 0).Read, *env.read(t, RingClassGet, "zz", "", 0).Read}
	payload, err := EncodeRingGetMany(RingGetManyRequest{Reads: reads})
	if err != nil {
		t.Fatal(err)
	}
	reply := ringBatchReply(t, c.send(transport.OpRingGetMany, u66ID(), payload), 2)
	if getManyID(t, 0, reply.Answers[0]) != "b1" || getManyID(t, 1, reply.Answers[1]) != "" {
		t.Fatalf("answers %+v", reply.Answers)
	}
	zero := append([]byte(nil), payload[:RingPrologueLen]...)
	refusal := u66Refusal(t, c.send(transport.OpRingGetMany, u66ID(), append(zero, 0)), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "carries 0 keys") {
		t.Fatalf("refusal %q is not the codec's", refusal.Message)
	}
}

// getManyPadded is a document of id whose body is about size bytes.
func getManyPadded(t *testing.T, id string, size int) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id_", Value: id}, {Key: "Pad", Value: strings.Repeat("x", size)}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// getManyPaddedWorld answers every key with a padded document whose size sizeOf names, after
// delayOf, without touching storage, and counts the reads launched.
func getManyPaddedWorld(t *testing.T, budget int64, sizeOf func(id string) int, delayOf func(id string) time.Duration) (*getManyWorld, *atomic.Int32) {
	t.Helper()
	world := newGetManyWorld(t, u70AllowAll{})
	world.server.resultBudget = budget
	launched := &atomic.Int32{}
	world.server.manyFetch = func(ctx context.Context, _ bucketRef, encID []byte, _ []uint16, _ bool) (Document, error) {
		launched.Add(1)
		id, err := store.DecodeStringID(encID)
		if err != nil {
			return Document{}, err
		}
		select {
		case <-time.After(delayOf(id)):
		case <-ctx.Done():
			return Document{}, ctx.Err()
		}
		return Document{Body: getManyPadded(t, id, sizeOf(id)), Served: servedOwner}, nil
	}
	return world, launched
}

// getManyAnswerSize is what a found answer of body charges to the budget.
func getManyAnswerSize(t *testing.T, body []byte) int64 {
	t.Helper()
	return getManyAnswerBytes(RingReply{Found: ringTrue(), Elements: [][]byte{body}})
}

func getManyKeptBytes(t *testing.T, reply RingReply) int64 {
	t.Helper()
	var kept int64
	for _, answer := range reply.Answers {
		if answer.Reply != nil && len(answer.Reply.Elements) > 0 {
			kept += getManyAnswerSize(t, answer.Reply.Elements[0])
		}
	}
	return kept
}

func getManyReadsOf(t *testing.T, ref bucketRef, ids ...string) []RingRead {
	t.Helper()
	reads := make([]RingRead, len(ids))
	for i, id := range ids {
		reads[i] = getManyRead(t, ref, id)
	}
	return reads
}

// TestGetManyReplyBudgetAnswersTheKeyThatDoesNotFit: the answers kept fit the budget; a key whose
// answer does not fit what is left answers RESULT_TOO_LARGE naming its index, bucket and id; a
// later key that fits is still answered; and once the budget is full no further read is launched.
func TestGetManyReplyBudgetAnswersTheKeyThatDoesNotFit(t *testing.T) {
	t.Run("a later key that fits is answered", func(t *testing.T) {
		sizes := map[string]int{"k0": 6000, "k1": 6000, "k2": 500}
		delays := map[string]time.Duration{"k0": 0, "k1": 60 * time.Millisecond, "k2": 120 * time.Millisecond}
		budget := getManyAnswerSize(t, getManyPadded(t, "k0", 6000)) + 3000
		world, _ := getManyPaddedWorld(t, budget, func(id string) int { return sizes[id] }, func(id string) time.Duration { return delays[id] })
		reply := world.serve(getManyReadsOf(t, world.near, "k0", "k1", "k2")...)
		if getManyID(t, 0, reply.Answers[0]) != "k0" || getManyID(t, 2, reply.Answers[2]) != "k2" {
			t.Fatalf("the keys that fit were not answered: %+v", reply.Answers)
		}
		refusal := getManyError(t, 1, reply.Answers[1], cluster.CodeResultTooLarge)
		for _, want := range []string{"key 1 of 3", `bucket "User"`, `id "k1"`, "does not fit what is left", "Reads change nothing", "GetFields"} {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("refusal %q does not say %q", refusal.Message, want)
			}
		}
		if kept := getManyKeptBytes(t, reply); kept > budget {
			t.Fatalf("kept %d bytes of answers over a %d-byte budget", kept, budget)
		}
		if reply.Outcome != ringBatchPartial {
			t.Fatalf("outcome %q, want partial", reply.Outcome)
		}
	})
	t.Run("no read is launched once the budget is full", func(t *testing.T) {
		const keys, size = 200, 4000
		budget := 10 * getManyAnswerSize(t, getManyPadded(t, "k000", size))
		world, launched := getManyPaddedWorld(t, budget, func(string) int { return size }, func(string) time.Duration { return time.Millisecond })
		ids := make([]string, keys)
		for i := range ids {
			ids[i] = fmt.Sprintf("k%03d", i)
		}
		reply := world.serve(getManyReadsOf(t, world.near, ids...)...)
		kept, notRead := 0, 0
		for i, answer := range reply.Answers {
			if answer.Error == nil {
				getManyID(t, i, answer)
				kept++
				continue
			}
			refusal := getManyError(t, i, answer, cluster.CodeResultTooLarge)
			if !strings.Contains(refusal.Message, fmt.Sprintf("key %d of %d", i, keys)) {
				t.Fatalf("refusal %q does not name key %d", refusal.Message, i)
			}
			if strings.Contains(refusal.Message, "was not read") {
				notRead++
			}
		}
		if got := getManyKeptBytes(t, reply); got > budget {
			t.Fatalf("kept %d bytes over a %d-byte budget", got, budget)
		}
		if kept != 10 {
			t.Fatalf("%d answers kept, want the 10 that fill the budget", kept)
		}
		if n := int(launched.Load()); n > kept+getManyFrameInFlight || notRead == 0 || n+notRead != keys {
			t.Fatalf("%d reads launched and %d keys answered not read for %d keys with %d kept: reads went on after the budget was full", n, notRead, keys, kept)
		}
	})
}

// TestGetManyAnswerThatFillsTheBudgetExactlyIsKept: an answer of exactly the bytes left is kept
// and one byte more is not; a document larger than the whole budget answers the "read it with
// Get" text, since no frame can carry it.
func TestGetManyAnswerThatFillsTheBudgetExactlyIsKept(t *testing.T) {
	first, second := getManyPadded(t, "k0", 3000), getManyPadded(t, "k1", 2000)
	exact := getManyAnswerSize(t, first) + getManyAnswerSize(t, second)
	sizes := map[string]int{"k0": 3000, "k1": 2000}
	delay := func(id string) time.Duration {
		if id == "k1" {
			return 50 * time.Millisecond
		}
		return 0
	}
	world, _ := getManyPaddedWorld(t, exact, func(id string) int { return sizes[id] }, delay)
	reply := world.serve(getManyReadsOf(t, world.near, "k0", "k1")...)
	if getManyID(t, 0, reply.Answers[0]) != "k0" || getManyID(t, 1, reply.Answers[1]) != "k1" {
		t.Fatal("answers that fill the budget exactly were not both kept")
	}

	world.server.resultBudget = exact - 1
	reply = world.serve(getManyReadsOf(t, world.near, "k0", "k1")...)
	getManyID(t, 0, reply.Answers[0])
	if refusal := getManyError(t, 1, reply.Answers[1], cluster.CodeResultTooLarge); !strings.Contains(refusal.Message, "does not fit what is left") {
		t.Fatalf("one byte over: refusal %q", refusal.Message)
	}

	world.server.resultBudget = getManyAnswerSize(t, first) - 1
	reply = world.serve(getManyReadsOf(t, world.near, "k0")...)
	refusal := getManyError(t, 0, reply.Answers[0], cluster.CodeResultTooLarge)
	for _, want := range []string{"key 0 of 1", `bucket "User"`, `id "k0"`, "is larger than the", "no frame can return it", "Read this document with Get"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("refusal %q does not say %q", refusal.Message, want)
		}
	}
}

// TestGetManyFrameMemoryIsBounded: 255 keys of 64 KiB documents under a 256 KiB budget. The bytes
// of answers the handler holds never pass the budget and at most 16 reads are in flight, so the
// frame holds at most the budget plus 16 documents (R2). A handler that kept every answer and
// trimmed afterwards would hold all 255.
func TestGetManyFrameMemoryIsBounded(t *testing.T) {
	const size, budget = 64 << 10, 256 << 10
	world, launched := getManyPaddedWorld(t, budget, func(string) int { return size }, func(string) time.Duration { return time.Millisecond })
	inner := world.server.manyFetch
	var mu sync.Mutex
	inFlight, peakInFlight := 0, 0
	var peakHeld int64
	world.server.manyFetch = func(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
		mu.Lock()
		inFlight++
		peakInFlight = max(peakInFlight, inFlight)
		mu.Unlock()
		defer func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}()
		return inner(ctx, ref, encID, project, local)
	}
	world.server.manyHeld = func(held int64) {
		mu.Lock()
		peakHeld = max(peakHeld, held)
		mu.Unlock()
	}
	ids := make([]string, maxGetManyKeys)
	for i := range ids {
		ids[i] = fmt.Sprintf("m%03d", i)
	}
	reply := world.serve(getManyReadsOf(t, world.near, ids...)...)
	if len(reply.Answers) != maxGetManyKeys {
		t.Fatalf("%d answers", len(reply.Answers))
	}
	if peakHeld == 0 || peakHeld > budget {
		t.Fatalf("the handler held %d bytes of answers at its peak, want 1 to %d", peakHeld, budget)
	}
	if peakInFlight > getManyFrameInFlight {
		t.Fatalf("%d reads in flight at once, want at most %d", peakInFlight, getManyFrameInFlight)
	}
	if bound := int64(budget + getManyFrameInFlight*size); peakHeld+int64(peakInFlight)*size > bound {
		t.Fatalf("peak %d held plus %d in flight is over the %d-byte bound", peakHeld, peakInFlight, bound)
	}
	if n := launched.Load(); n >= maxGetManyKeys {
		t.Fatalf("all %d keys were read under a budget that holds 3 of them", n)
	}
}

// getManyBlockingWorld answers each key after up to wait, or when its context ends with what
// ended returns: ctx.Err() for a read that honours its context, or a hold's own refusal.
func getManyBlockingWorld(t *testing.T, wait time.Duration, ended func(ctx context.Context) error) *getManyWorld {
	t.Helper()
	world := newGetManyWorld(t, u70AllowAll{})
	world.server.manyFetch = func(ctx context.Context, _ bucketRef, encID []byte, _ []uint16, _ bool) (Document, error) {
		world.fetches.Add(1)
		id, err := store.DecodeStringID(encID)
		if err != nil {
			return Document{}, err
		}
		if strings.HasPrefix(id, "fast") {
			return Document{Body: getManyPadded(t, id, 10), Served: servedOwner}, nil
		}
		select {
		case <-time.After(wait):
			return Document{Body: getManyPadded(t, id, 10), Served: servedOwner}, nil
		case <-ctx.Done():
			return Document{}, ended(ctx)
		}
	}
	return world
}

func getManyDeadlineIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("slow%02d", i)
	}
	return ids
}

// getManyDeadlineTexts counts the two deadline answers and fails on any other error answer or
// on Go's own context text.
func getManyDeadlineTexts(t *testing.T, reply RingReply, deadlineMs int) (cutOff, notRead int) {
	t.Helper()
	for i, answer := range reply.Answers {
		if answer.Error == nil {
			continue
		}
		refusal := getManyError(t, i, answer, cluster.CodeUnavailable)
		if strings.Contains(refusal.Message, "context deadline exceeded") || strings.Contains(refusal.Message, "context canceled") {
			t.Fatalf("key %d answers Go's context text: %q", i, refusal.Message)
		}
		tail := fmt.Sprintf("the frame's deadline of %d ms was spent. Reads change nothing, so resend it: in a smaller frame or with a longer deadline", deadlineMs)
		switch {
		case refusal.Message == fmt.Sprintf("key %d of %d was cut off while it was being read: %s", i, len(reply.Answers), tail):
			cutOff++
		case refusal.Message == fmt.Sprintf("key %d of %d was not read: %s", i, len(reply.Answers), tail):
			notRead++
		default:
			t.Fatalf("key %d answers %q, not a deadline text", i, refusal.Message)
		}
	}
	return cutOff, notRead
}

// TestGetManyDeadlineSpentAnswersTheTailNotRead: the frame's deadline_ms bounds every key (R3):
// keys in flight when it passes answer "cut off while it was being read", keys never launched
// answer "was not read", and the frame is answered soon after the deadline.
func TestGetManyDeadlineSpentAnswersTheTailNotRead(t *testing.T) {
	world := getManyBlockingWorld(t, 3*time.Second, func(ctx context.Context) error { return ctx.Err() })
	const deadline = 150
	start := time.Now()
	reply := world.server.serveGetMany(context.Background(), RingGetManyRequest{
		Prologue: RingPrologue{DeadlineMs: deadline}, Reads: getManyReadsOf(t, world.near, append([]string{"fast0"}, getManyDeadlineIDs(60)...)...)})
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the frame took %v with a %d ms deadline", took, deadline)
	}
	if getManyID(t, 0, reply.Answers[0]) != "fast0" {
		t.Fatal("a key read before the deadline lost its answer")
	}
	cutOff, notRead := getManyDeadlineTexts(t, reply, deadline)
	if cutOff == 0 || cutOff > getManyFrameInFlight || notRead == 0 || cutOff+notRead != 60 {
		t.Fatalf("%d keys cut off and %d not read of 60 slow keys", cutOff, notRead)
	}
	if reply.Outcome != ringBatchPartial {
		t.Fatalf("outcome %q", reply.Outcome)
	}

	world = getManyBlockingWorld(t, 20*time.Millisecond, func(ctx context.Context) error { return ctx.Err() })
	reply = world.serve(getManyReadsOf(t, world.near, getManyDeadlineIDs(40)...)...)
	for i, answer := range reply.Answers {
		getManyID(t, i, answer)
	}
}

// TestGetManyDeadlineCutsAKeyParkedInAHold: a read parked in a hold ends with the hold's own
// refusal when the frame's time is spent; the key answers the cut-off text, which says resending
// is safe, not the hold's.
func TestGetManyDeadlineCutsAKeyParkedInAHold(t *testing.T) {
	overdue := &cluster.Error{Code: cluster.CodeUnavailable, Message: "slot 7 of database 1 is waiting for the previous owner to stop and for this node to catch up, and that had not finished when this request's deadline passed. Nothing was returned. Retry in a few seconds"}
	world := getManyBlockingWorld(t, 3*time.Second, func(context.Context) error { return overdue })
	reply := world.server.serveGetMany(context.Background(), RingGetManyRequest{
		Prologue: RingPrologue{DeadlineMs: 80}, Reads: getManyReadsOf(t, world.near, "slow00", "fast1", "slow01")})
	getManyID(t, 1, reply.Answers[1])
	if cutOff, notRead := getManyDeadlineTexts(t, reply, 80); cutOff != 2 || notRead != 0 {
		t.Fatalf("%d cut off and %d not read, want the 2 parked keys cut off", cutOff, notRead)
	}
}

// getManySlowAllow allows everything, slowly, so admission spends a frame's deadline.
type getManySlowAllow struct{ delay time.Duration }

func (g getManySlowAllow) Authorize(context.Context, RingAccess) error {
	time.Sleep(g.delay)
	return nil
}

// TestGetManyDeadlineBoundsAdmissionToo: admission can take a lookup per key, so a deadline that
// passes during it leaves the keys not yet admitted unread, with the "was not read" text.
func TestGetManyDeadlineBoundsAdmissionToo(t *testing.T) {
	world := newGetManyWorld(t, getManySlowAllow{delay: 5 * time.Millisecond})
	ids := make([]string, 40)
	for i := range ids {
		ids[i] = fmt.Sprintf("u%d", i%7)
	}
	reply := world.server.serveGetMany(context.Background(), RingGetManyRequest{Prologue: RingPrologue{DeadlineMs: 40}, Reads: getManyReadsOf(t, world.near, ids...)})
	cutOff, notRead := getManyDeadlineTexts(t, reply, 40)
	if notRead == 0 || cutOff != 0 {
		t.Fatalf("%d not read and %d cut off: the deadline did not stop admission", notRead, cutOff)
	}
}

// TestGetManyDeadlineKeepsAnErrorThatIsNotTheDeadline: only a read that ended with the context's
// deadline, or with the refusal of a hold that ran out of the caller's time, is relabelled as
// cut off; a real error (here an owner that is down) keeps its own text even when it settles after
// the frame's deadline.
func TestGetManyDeadlineKeepsAnErrorThatIsNotTheDeadline(t *testing.T) {
	down := &cluster.Error{Code: cluster.CodeUnavailable, Message: "every copy of slot 7 of database 1 is down, so no node owns it. Nothing was done"}
	world := getManyBlockingWorld(t, 3*time.Second, func(context.Context) error { return down })
	reply := world.server.serveGetMany(context.Background(), RingGetManyRequest{
		Prologue: RingPrologue{DeadlineMs: 80}, Reads: getManyReadsOf(t, world.near, "slow00")})
	refusal := getManyError(t, 0, reply.Answers[0], cluster.CodeUnavailable)
	if !strings.Contains(refusal.Message, "is down, so no node owns it") || strings.Contains(refusal.Message, "deadline") {
		t.Fatalf("a down owner's refusal was relabelled: %q", refusal.Message)
	}
}

// TestGetManyDeadlineBeatsFullWhenNamingAKeyNeverRead: a key never launched in a frame that is
// both full and out of time answers the deadline text: the time is what the caller can change
// most simply, and it is the first thing the handler checks.
func TestGetManyDeadlineBeatsFullWhenNamingAKeyNeverRead(t *testing.T) {
	budget := int64(64 << 10)
	world, _ := getManyPaddedWorld(t, budget, func(id string) int {
		if id == "k000" || id == "k001" {
			return 40 << 10
		}
		return 100
	}, func(id string) time.Duration {
		if id == "k000" || id == "k001" {
			return 0
		}
		return 3 * time.Second
	})
	ids := make([]string, 40)
	for i := range ids {
		ids[i] = fmt.Sprintf("k%03d", i)
	}
	reply := world.server.serveGetMany(context.Background(), RingGetManyRequest{
		Prologue: RingPrologue{DeadlineMs: 150}, Reads: getManyReadsOf(t, world.near, ids...)})
	// The two settle in either order; whichever is second does not fit and fills the frame.
	if refused := (reply.Answers[0].Error != nil) != (reply.Answers[1].Error != nil); !refused {
		t.Fatalf("want exactly one of the two 40 KiB keys refused: %+v %+v", reply.Answers[0].Error, reply.Answers[1].Error)
	}
	unread := 0
	for i := 19; i < len(ids); i++ {
		refusal := getManyError(t, i, reply.Answers[i], cluster.CodeUnavailable)
		if !strings.Contains(refusal.Message, "was not read: the frame's deadline of 150 ms was spent") {
			t.Fatalf("key %d never launched answers %q, want the deadline text", i, refusal.Message)
		}
		unread++
	}
	if unread == 0 {
		t.Fatal("no key was left unread")
	}
}

// getManyDenyClass refuses one access class on one bucket and allows everything else.
type getManyDenyClass struct {
	bucket string
	class  RingClass
}

func (g getManyDenyClass) Authorize(_ context.Context, access RingAccess) error {
	if access.Bucket == g.bucket && access.Class == g.class {
		return errU70NoGrant
	}
	return nil
}

// TestGetManyRefusalMemoIsPerClass: a bucket the session may read but not read by field (a
// get_fields grant it lacks) refuses the get_fields key only; the refusal is remembered per
// class, so a get of the same bucket after it is answered, in either order.
func TestGetManyRefusalMemoIsPerClass(t *testing.T) {
	world := newGetManyWorld(t, getManyDenyClass{bucket: "User", class: RingClassGetFields})
	for _, fieldsFirst := range []bool{true, false} {
		reads := []RingRead{getManyRead(t, world.near, "u1", "Name"), getManyRead(t, world.near, "u2")}
		if !fieldsFirst {
			reads[0], reads[1] = reads[1], reads[0]
		}
		reply := world.serve(reads...)
		denied, plain := 0, 1
		if !fieldsFirst {
			denied, plain = 1, 0
		}
		getManyError(t, denied, reply.Answers[denied], cluster.CodePermissionDenied)
		if got := getManyID(t, plain, reply.Answers[plain]); got != "u2" {
			t.Fatalf("fieldsFirst=%v: the get answered %q, want u2", fieldsFirst, got)
		}
	}
}

// TestGetManyDeadlineCutsAForwardedKeyThatEndsWithAnIOTimeout: a key forwarded to its owner is
// bounded by the frame's deadline through the data channel's connection deadline, so it ends with
// the network's i/o timeout and not with the context's error; it is still the frame's time that
// ran out, and the key says so.
func TestGetManyDeadlineCutsAForwardedKeyThatEndsWithAnIOTimeout(t *testing.T) {
	timeout := fmt.Errorf("forwarding the read to n2: calling n2 on the data channel: read frame header: %w", &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded})
	world := getManyBlockingWorld(t, 3*time.Second, func(context.Context) error { return timeout })
	reply := world.server.serveGetMany(context.Background(), RingGetManyRequest{
		Prologue: RingPrologue{DeadlineMs: 80}, Reads: getManyReadsOf(t, world.near, "slow00")})
	if cutOff, notRead := getManyDeadlineTexts(t, reply, 80); cutOff != 1 || notRead != 0 {
		t.Fatalf("%d cut off and %d not read, want the forwarded key cut off", cutOff, notRead)
	}
}
