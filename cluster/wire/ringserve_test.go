package wire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

const u66Secret = "u66-secret"

// u66Next stands in for the server's AUTH handler: the right secret authenticates, and
// any other frame that reaches it is a frame the ring handler did not take.
func u66Next(_ context.Context, frame *transport.Frame) (*transport.Frame, error) {
	if frame.Opcode != transport.OpAuth {
		return transport.NewFrameWithID(transport.OpError, frame.RequestID, []byte{5, 0, 0, 0, 0}), nil
	}
	if string(frame.Payload) != u66Secret {
		return transport.NewFrameWithID(transport.OpAuthFail, frame.RequestID, nil), nil
	}
	return transport.NewFrameWithID(transport.OpAuthOK, frame.RequestID, nil), nil
}

// u66AuthNext sets the session identity the transport demands before it marks a session
// authenticated.
func u66AuthNext(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	reply, err := u66Next(ctx, frame)
	if err == nil && reply.Opcode == transport.OpAuthOK {
		if sess, ok := transport.SessionFromContext(ctx); ok {
			sess.SetIdentity(transport.Identity{Username: "u66"})
		}
	}
	return reply, err
}

type u66Env struct {
	node *Node
	addr string
	ref  bucketRef
}

// u66Start serves a node with bucket Box on a real TLS listener on a port the kernel picks.
func u66Start(t *testing.T) *u66Env {
	t.Helper()
	node, _ := u63Fresh(t)
	return u66Serve(t, node)
}

// u66Serve puts node behind the ring handler on a TLS listener on a port the kernel picks.
func u66Serve(t *testing.T, node *Node) *u66Env { return u66ServeWith(t, node) }

// u66ServeWith is u66Serve with ring handler options.
func u66ServeWith(t *testing.T, node *Node, options ...RingOption) *u66Env {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg, _, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRingHandler(node, u66AuthNext, u70AllowAll{}, options...)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := transport.ListenTCP("127.0.0.1:0", tlsCfg, transport.NewConnLimiter(64, 64), transport.TCPConfig{},
		handler, &transport.SessionDeps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return &u66Env{node: node, addr: listener.Addr().String(), ref: ref}
}

type u66Conn struct {
	t    *testing.T
	conn *tls.Conn
	br   *bufio.Reader
}

func (e *u66Env) dial(t *testing.T) *u66Conn {
	t.Helper()
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "U66 test client"}}, e.addr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", e.addr, tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &u66Conn{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func (e *u66Env) login(t *testing.T) *u66Conn {
	t.Helper()
	c := e.dial(t)
	if reply := c.send(transport.OpAuth, [16]byte{1}, []byte(u66Secret)); reply.Opcode != transport.OpAuthOK {
		t.Fatalf("AUTH answered %s", reply.Opcode)
	}
	return c
}

func (c *u66Conn) write(op transport.Opcode, id [16]byte, payload []byte) {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.conn.Write(transport.NewFrameWithID(op, id, payload).Encode()); err != nil {
		c.t.Fatal(err)
	}
}

func (c *u66Conn) read() *transport.Frame {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(20 * time.Second))
	frame, err := transport.DecodeFrame(c.br)
	if err != nil {
		c.t.Fatal(err)
	}
	return frame
}

func (c *u66Conn) send(op transport.Opcode, id [16]byte, payload []byte) *transport.Frame {
	c.t.Helper()
	c.write(op, id, payload)
	reply := c.read()
	if reply.RequestID != id {
		c.t.Fatalf("reply carries request id %x, want %x", reply.RequestID, id)
	}
	return reply
}

var u66NextID byte

func u66ID() [16]byte {
	u66NextID++
	return [16]byte{0xAA, u66NextID}
}

// keyed sends one RING_KEYED request and returns the frame that answered it.
func (c *u66Conn) keyed(req RingKeyedRequest) *transport.Frame {
	c.t.Helper()
	payload, err := EncodeRingKeyed(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return c.send(transport.OpRingKeyed, u66ID(), payload)
}

// ok decodes a RING_REPLY and fails the test on any other answer.
func u66Reply(t *testing.T, frame *transport.Frame) RingReply {
	t.Helper()
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("answered %s %x, want RING_REPLY", frame.Opcode, frame.Payload)
	}
	reply, err := DecodeRingReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

// refused decodes an OpError and checks its ring code.
func u66Refusal(t *testing.T, frame *transport.Frame, code cluster.Code) RingError {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("answered %s %x, want OpError", frame.Opcode, frame.Payload)
	}
	refusal, err := DecodeRingError(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if refusal.Code != code {
		t.Fatalf("code %s (%q), want %s", refusal.Code, refusal.Message, code)
	}
	return refusal
}

func (e *u66Env) header() cluster.Op {
	return cluster.Op{BucketID: e.ref.entry.Decl.BucketID, DeclVersion: e.ref.version, SchemaVersionSeen: e.ref.entry.Seen}
}

func (e *u66Env) fieldID(t *testing.T, name string) uint16 {
	t.Helper()
	entry, ok := entryNamed(e.ref.table, name)
	if !ok {
		t.Fatalf("no field %s", name)
	}
	return entry.ID
}

func u66EncID(t *testing.T, id string) []byte {
	t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func (e *u66Env) doc(t *testing.T, class byte, id, name string) RingKeyedRequest {
	op := e.header()
	op.Class, op.EncID, op.IDValue = class, u66EncID(t, id), stringValue(id)
	op.Fields = []cluster.FieldOp{{FieldID: e.fieldID(t, "Name"), Value: stringValue(name)}}
	return RingKeyedRequest{Op: op}
}

func (e *u66Env) verb(t *testing.T, id, field string, verb byte, arg []byte) RingKeyedRequest {
	op := e.header()
	op.Class, op.EncID = cluster.OpStoreVerb, u66EncID(t, id)
	op.Fields = []cluster.FieldOp{{FieldID: e.fieldID(t, field), Verb: verb, Value: arg}}
	return RingKeyedRequest{Op: op}
}

func (e *u66Env) read(t *testing.T, class byte, id, field string, limit uint16) RingKeyedRequest {
	read := RingRead{Class: class, BucketID: e.ref.entry.Decl.BucketID, DeclVersion: e.ref.version, SchemaVersionSeen: e.ref.entry.Seen, EncID: u66EncID(t, id), Limit: limit}
	if field != "" {
		read.FieldID = e.fieldID(t, field)
	}
	return RingKeyedRequest{Read: &read}
}

func u66Item(t *testing.T, label string) []byte {
	t.Helper()
	raw, err := bson.Marshal(u63Item(label))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func u66Arg(t *testing.T, build func() ([]byte, error)) []byte {
	t.Helper()
	arg, err := build()
	if err != nil {
		t.Fatal(err)
	}
	return arg
}

func TestU66InsertThenGetOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	wrote := u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, "ada", "Ada")))
	if wrote.WriteOutcome != "applied" || wrote.CopiesConfirmed == nil || *wrote.CopiesConfirmed != 1 || wrote.ServedBy != "n1" ||
		wrote.Position == nil || wrote.ID.Type != 0x02 || wrote.Epoch == nil || wrote.StateSeq == nil || wrote.Deduped {
		t.Fatalf("insert reply %+v", wrote)
	}
	got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "ada", "", 0)))
	if got.Found == nil || !*got.Found || len(got.Elements) != 1 || got.WriteOutcome != "" || got.Position != nil {
		t.Fatalf("get reply %+v", got)
	}
	body := bson.Raw(got.Elements[0])
	if body.Lookup("_id_").StringValue() != "ada" || body.Lookup("Name").StringValue() != "Ada" {
		t.Fatalf("get body %v", body)
	}
	missing := u66Reply(t, c.keyed(env.read(t, RingClassGet, "nobody", "", 0)))
	if missing.Found == nil || *missing.Found || len(missing.Elements) != 0 {
		t.Fatalf("a get of an absent document is found=false, got %+v", missing)
	}
}

func TestU66UpsertAndDeleteOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u66Reply(t, c.keyed(env.doc(t, cluster.OpUpsert, "up", "First")))
	u66Reply(t, c.keyed(env.doc(t, cluster.OpUpsert, "up", "Second")))
	got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "up", "", 0)))
	if bson.Raw(got.Elements[0]).Lookup("Name").StringValue() != "Second" {
		t.Fatalf("upsert did not change the field: %v", bson.Raw(got.Elements[0]))
	}
	del := env.header()
	del.Class, del.EncID = cluster.OpDelete, u66EncID(t, "up")
	if reply := u66Reply(t, c.keyed(RingKeyedRequest{Op: del})); reply.WriteOutcome != "applied" {
		t.Fatalf("delete reply %+v", reply)
	}
	if gone := u66Reply(t, c.keyed(env.read(t, RingClassGet, "up", "", 0))); gone.Found == nil || *gone.Found {
		t.Fatalf("deleted document still found: %+v", gone)
	}
}

func TestU66FifoPushPeekPop(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, label := range []string{"one", "two"} {
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		if reply := u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, arg))); reply.WriteOutcome != "applied" || reply.Position == nil || reply.Found != nil {
			t.Fatalf("push reply %+v", reply)
		}
	}
	peeked := u66Reply(t, c.keyed(env.read(t, RingClassPeek, "b1", "Queue", 5)))
	if peeked.Found == nil || !*peeked.Found || len(peeked.Elements) != 2 || peeked.WriteOutcome != "" {
		t.Fatalf("peek reply %+v", peeked)
	}
	popped := u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPop, nil)))
	if popped.Found == nil || !*popped.Found || len(popped.Elements) != 1 || !bytes.Equal(popped.Elements[0], u66Item(t, "one")) || popped.WriteOutcome != "applied" {
		t.Fatalf("pop reply %+v", popped)
	}
	u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPop, nil)))
	empty := u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPop, nil)))
	if empty.Found == nil || *empty.Found || len(empty.Elements) != 0 {
		t.Fatalf("pop of an empty queue is found=false, got %+v", empty)
	}
}

func TestU66HashPutTake(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	put := u66Arg(t, func() ([]byte, error) { return encodePutArg("k1", u66Item(t, "v1")) })
	u66Reply(t, c.keyed(env.verb(t, "b1", "Table", cluster.VerbPut, put)))
	take := u66Arg(t, func() ([]byte, error) { return encodeTakeKeyArg("k1") })
	taken := u66Reply(t, c.keyed(env.verb(t, "b1", "Table", cluster.VerbTake, take)))
	if taken.Found == nil || !*taken.Found || len(taken.Elements) != 1 || !bytes.Equal(taken.Elements[0], u66Item(t, "v1")) {
		t.Fatalf("take reply %+v", taken)
	}
	again := u66Reply(t, c.keyed(env.verb(t, "b1", "Table", cluster.VerbTake, take)))
	if again.Found == nil || *again.Found {
		t.Fatalf("take of an absent key is found=false, got %+v", again)
	}
}

func TestU66ClaimThenAck(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	for _, label := range []string{"a", "b", "c"} {
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		u66Reply(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPush, arg)))
	}
	claim := env.header()
	claim.Class, claim.EncID, claim.Count = cluster.OpClaim, u66EncID(t, "b1"), 2
	claim.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, "Queue")}}
	claimed := u66Reply(t, c.keyed(RingKeyedRequest{Op: claim}))
	if len(claimed.Lease) != 16 || len(claimed.Elements) != 2 || !bytes.Equal(claimed.Elements[0], u66Item(t, "a")) || claimed.WriteOutcome != "applied" {
		t.Fatalf("claim reply %+v", claimed)
	}
	ack := env.header()
	ack.Class, ack.EncID = cluster.OpAck, u66EncID(t, "b1")
	ack.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, "Queue")}}
	copy(ack.LeaseID[:], claimed.Lease)
	if reply := u66Reply(t, c.keyed(RingKeyedRequest{Op: ack})); reply.WriteOutcome != "applied" || len(reply.Lease) != 0 {
		t.Fatalf("ack reply %+v", reply)
	}
	rest := u66Reply(t, c.keyed(env.read(t, RingClassPeek, "b1", "Queue", 10)))
	if len(rest.Elements) != 1 || !bytes.Equal(rest.Elements[0], u66Item(t, "c")) {
		t.Fatalf("after the ack one element is left, got %+v", rest)
	}
}

func TestU66ARetryWithTheSameRequestIDIsAnsweredFromTheDedupeWindow(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	payload, err := EncodeRingKeyed(env.verb(t, "b1", "Queue", cluster.VerbPush, u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, "once")) })))
	if err != nil {
		t.Fatal(err)
	}
	id := u66ID()
	first := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	second := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	if first.Deduped || !second.Deduped || *first.Position != *second.Position {
		t.Fatalf("first %+v, retry %+v: the retry must be deduped at the same position", first, second)
	}
	peeked := u66Reply(t, c.keyed(env.read(t, RingClassPeek, "b1", "Queue", 10)))
	if len(peeked.Elements) != 1 {
		t.Fatalf("a retried push applied %d times", len(peeked.Elements))
	}
}

func TestU66PipelinedRepliesMatchTheirRequestByID(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	ids := map[[16]byte]string{}
	for _, name := range []string{"p1", "p2", "p3", "p4"} {
		payload, err := EncodeRingKeyed(env.doc(t, cluster.OpInsert, name, name))
		if err != nil {
			t.Fatal(err)
		}
		id := u66ID()
		ids[id] = name
		c.write(transport.OpRingKeyed, id, payload)
	}
	for range ids {
		frame := c.read()
		reply := u66Reply(t, frame)
		name, ok := ids[frame.RequestID]
		if !ok {
			t.Fatalf("a reply carries request id %x nobody sent", frame.RequestID)
		}
		if !bytes.Equal(reply.ID.Value, stringValue(name)[1:]) {
			t.Fatalf("reply for %s carries id %x", name, reply.ID.Value)
		}
		delete(ids, frame.RequestID)
	}
}

func TestU66RefusalsCarryTheirCodeMessageAndDetail(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, "dup", "x")))

	duplicate := u66Refusal(t, c.keyed(env.doc(t, cluster.OpInsert, "dup", "x")), cluster.CodeDocumentExists)
	if duplicate.Detail != nil || !strings.Contains(duplicate.Message, "already") {
		t.Fatalf("duplicate refusal %+v", duplicate)
	}

	unknown := env.read(t, RingClassGet, "dup", "", 0)
	unknown.Read.BucketID ^= 0x5A5A
	if refusal := u66Refusal(t, c.keyed(unknown), cluster.CodePermissionDenied); !strings.Contains(refusal.Message, "bucket") {
		t.Fatalf("an unknown bucket id is refused as a forbidden one, naming the bucket id: %q", refusal.Message)
	}

	wrongVerb := u66Arg(t, func() ([]byte, error) { return encodePutArg("k", u66Item(t, "v")) })
	if refusal := u66Refusal(t, c.keyed(env.verb(t, "b1", "Queue", cluster.VerbPut, wrongVerb)), cluster.CodeInvalidArgument); !strings.Contains(refusal.Message, "fifoStore") {
		t.Fatalf("put on a fifo is refused naming the store type: %q", refusal.Message)
	}

	stale := env.read(t, RingClassGet, "dup", "", 0)
	stale.Prologue.Epoch = env.node.topology.Committed() + 7
	refusal := u66Refusal(t, c.keyed(stale), cluster.CodeStaleEpoch)
	if refusal.Detail == nil || refusal.Detail.Epoch != env.node.topology.Committed() || refusal.Detail.Owner != "n1" {
		t.Fatalf("a stale epoch names the node's epoch and owner in its detail: %+v", refusal.Detail)
	}
	current := env.read(t, RingClassGet, "dup", "", 0)
	current.Prologue.Epoch = env.node.topology.Committed()
	u66Reply(t, c.keyed(current))
}

func u66BSON(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestU66AnUnauthenticatedConnectionReachesNothing(t *testing.T) {
	env := u66Start(t)
	c := env.dial(t)
	reply := c.keyed(env.doc(t, cluster.OpInsert, "sneak", "x"))
	if reply.Opcode != transport.OpError {
		t.Fatalf("an unauthenticated insert answered %s", reply.Opcode)
	}
	var legacy struct {
		Code    int32  `bson:"code"`
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(reply.Payload, &legacy); err != nil || legacy.Code != int32(transport.ErrNotAuthenticated) {
		t.Fatalf("refusal %+v, %v", legacy, err)
	}
	if _, err := env.node.Get(context.Background(), u30iDB, "Box", "sneak"); err == nil {
		t.Fatal("the unauthenticated insert was applied")
	}
	// PING is still served before AUTH.
	if pong := c.send(transport.OpPing, u66ID(), nil); pong.Opcode != transport.OpPong {
		t.Fatalf("PING answered %s", pong.Opcode)
	}
}

func TestU66GarbageAfterThePrologueIsRefusedAndTheConnectionSurvives(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	prologue := make([]byte, RingPrologueLen)
	for name, payload := range map[string][]byte{
		"empty":            nil,
		"short prologue":   {1, 2, 3},
		"prologue only":    prologue,
		"unknown class":    append(bytes.Clone(prologue), 0x77, 1, 2, 3),
		"truncated insert": append(bytes.Clone(prologue), cluster.OpInsert, 0, 0),
		"truncated get":    append(bytes.Clone(prologue), RingClassGet, 0xFF, 0xFF),
		"junk":             append(bytes.Clone(prologue), bytes.Repeat([]byte{0xDE, 0xAD}, 300)...),
	} {
		refusal := u66Refusal(t, c.send(transport.OpRingKeyed, u66ID(), payload), cluster.CodeInvalidArgument)
		if refusal.Message == "" {
			t.Errorf("%s: no message", name)
		}
	}
	for _, opcode := range []transport.Opcode{transport.OpRingList, transport.OpRingCount, transport.OpRingTopology} {
		u66Refusal(t, c.send(opcode, u66ID(), []byte{1, 2, 3}), cluster.CodeInvalidArgument)
	}
	u66Reply(t, c.keyed(env.read(t, RingClassGet, "b1", "", 0)))
}

func TestU66BadConnectionsLeaveNoGoroutinesBehind(t *testing.T) {
	env := u66Start(t)
	warm := env.login(t)
	u66Reply(t, warm.keyed(env.read(t, RingClassGet, "b1", "", 0)))
	_ = warm.conn.Close()
	baseline := u66SettledGoroutines(0)
	for range 20 {
		c := env.login(t)
		// Garbage past the transport framing: the server closes the connection.
		_, _ = c.conn.Write(bytes.Repeat([]byte{0xFF}, 64))
		_ = c.conn.Close()
		d := env.login(t)
		d.write(transport.OpRingKeyed, u66ID(), []byte{9, 9, 9})
		_ = d.conn.Close()
		e := env.dial(t)
		_, _ = e.conn.Write([]byte{0, 0, 0})
		_ = e.conn.Close()
	}
	if now := u66SettledGoroutines(baseline); now > baseline {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutines: %d after the bad connections, %d before\n%s", now, baseline, buf[:runtime.Stack(buf, true)])
	}
}

// u66SettledGoroutines returns the goroutine count once it is at or below want, or after
// ten seconds; with want 0 it returns the count once it stops changing.
func u66SettledGoroutines(want int) int {
	deadline := time.Now().Add(10 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		runtime.Gosched()
		now := runtime.NumGoroutine()
		if want > 0 && now <= want {
			return now
		}
		if want == 0 && now == last {
			return now
		}
		last = now
		time.Sleep(50 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}

func TestU66InsertRefusesAnIDOrFieldItCannotStore(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	insertWith := func(edit func(*cluster.Op)) *transport.Frame {
		req := env.doc(t, cluster.OpInsert, "odd", "x")
		edit(&req.Op)
		return c.keyed(req)
	}
	notString := insertWith(func(op *cluster.Op) {
		op.IDValue = []byte{0x10, 1, 0, 0, 0} // int32 1
	})
	if refusal := u66Refusal(t, notString, cluster.CodeInvalidArgument); !strings.Contains(refusal.Message, "string") {
		t.Fatalf("a non-string _id_ is refused naming the type: %q", refusal.Message)
	}
	wrongKey := insertWith(func(op *cluster.Op) { op.EncID = u66EncID(t, "other") })
	if refusal := u66Refusal(t, wrongKey, cluster.CodeInvalidArgument); !strings.Contains(refusal.Message, "encoding") {
		t.Fatalf("a key that is not the _id_'s encoding is refused: %q", refusal.Message)
	}
	undeclared := insertWith(func(op *cluster.Op) { op.Fields[0].FieldID = 4000 })
	u66Refusal(t, undeclared, cluster.CodeFieldType)
	storeField := insertWith(func(op *cluster.Op) { op.Fields[0].FieldID = env.fieldID(t, "Queue") })
	u66Refusal(t, storeField, cluster.CodeFieldType)
	badValue := insertWith(func(op *cluster.Op) { op.Fields[0].Value = []byte{0x02, 0xFF, 0xFF, 0xFF, 0x7F, 'x'} })
	if refusal := u66Refusal(t, badValue, cluster.CodeInvalidArgument); !strings.Contains(refusal.Message, "BSON") {
		t.Fatalf("a malformed value is refused: %q", refusal.Message)
	}
	if gone := u66Reply(t, c.keyed(env.read(t, RingClassGet, "odd", "", 0))); *gone.Found {
		t.Fatal("a refused insert was stored")
	}
}

func TestU66DocumentCheckSurvivesMutation(t *testing.T) {
	env := u66Start(t)
	seed := env.doc(t, cluster.OpInsert, "mut", "Mutant").Op
	rng := rand.New(rand.NewPCG(66, 66))
	accepted := 0
	for range 100_000 {
		op := seed
		op.Fields = []cluster.FieldOp{{FieldID: seed.Fields[0].FieldID, Value: u65Mutate(rng, seed.Fields[0].Value)}}
		op.IDValue = u65Mutate(rng, seed.IDValue)
		if len(op.Fields[0].Value) == 0 {
			continue
		}
		if ringCheckDocument(env.ref, op, func(uint16) string { return "User" }) == nil {
			accepted++
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated document was accepted, so the accept path never ran")
	}
}

// U79: the duplicate-insert refusal arrives with the document id and the bucket and
// database names filled in, so the text equals the conventions Errors row.
func TestU79DocumentExistsNamesTheDocumentBucketAndDatabase(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u66Reply(t, c.keyed(env.doc(t, cluster.OpInsert, "dup", "x")))

	refusal := u66Refusal(t, c.keyed(env.doc(t, cluster.OpInsert, "dup", "x")), cluster.CodeDocumentExists)
	want := fmt.Sprintf(`document "dup" is already in %s.Box, and insert never replaces. Run 'db.Box.upsert({_id_: "dup", …})' to replace it, or 'db.Box["dup"].get()' to read what is there`, u30iDB)
	if refusal.Message != want {
		t.Fatalf("message\n got: %s\nwant: %s", refusal.Message, want)
	}
}
