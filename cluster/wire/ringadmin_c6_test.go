package wire

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func c6Requests() []RingAdminRequest {
	return []RingAdminRequest{
		{Cmd: AdminJoin, NodeID: "node-b", DataAddr: "node-b.example:4433"},
		{Cmd: AdminJoin, NodeID: "node-w", DataAddr: "node-w.example:4433", Witness: true},
		{Cmd: AdminPromote, NodeID: "node-b"},
		{Cmd: AdminLeave, NodeID: "node-b"},
		{Cmd: AdminRemoveNode, NodeID: "node-b"},
		{Cmd: AdminReplaceNode, NodeID: "node-b", DataAddr: "node-b2.example:4433"},
		{Cmd: AdminReplaceNode, NodeID: "node-b", DataAddr: "node-b.example:4433", AcceptLoss: true},
	}
}

func c6Replies() []RingAdminReply {
	var replies []RingAdminReply
	for _, cmd := range []string{AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode} {
		replies = append(replies, RingAdminReply{Cmd: cmd, Membership: &RingAdminMembership{Epoch: 2, StateSeq: 9, Result: "Added node-b as a member (epoch 2)"}})
	}
	replies = append(replies, RingAdminReply{Cmd: AdminPromote, Membership: &RingAdminMembership{Epoch: 0, StateSeq: 0, Result: "x"}})
	return replies
}

// TestC6MembershipRequestsRoundTrip: [GUARD every command, with and without its optional
// witness, encodes and decodes to the same value, and the cmd list names the five].
func TestC6MembershipRequestsRoundTrip(t *testing.T) {
	for _, req := range c6Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatalf("%+v: %v", req, err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Fatalf("round trip of %+v gave %+v, %v", req, got, err)
		}
	}
	_, err := EncodeRingAdmin(RingAdminRequest{Cmd: "drop_database"})
	for _, cmd := range []string{AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode} {
		if err == nil || !strings.Contains(err.Error(), cmd) {
			t.Errorf("the unknown-cmd refusal does not name %s: %v", cmd, err)
		}
	}
}

// TestC6MembershipRequestRefusals: [GUARD every member is bounded and named in the refusal
// with the fix: name caps, UTF-8, control characters, host:port shape, members a command does
// not take].
func TestC6MembershipRequestRefusals(t *testing.T) {
	long := strings.Repeat("n", ringMaxNodeText+1)
	cases := []struct {
		name string
		req  RingAdminRequest
		want []string
	}{
		{"join without id", RingAdminRequest{Cmd: AdminJoin, DataAddr: "h.example:4433"}, []string{"join", "node_id", "256"}},
		{"long id", RingAdminRequest{Cmd: AdminPromote, NodeID: long}, []string{"promote", "node_id", "256"}},
		{"control in id", RingAdminRequest{Cmd: AdminLeave, NodeID: "a\x00b"}, []string{"leave", "node_id", "control character"}},
		{"invalid utf8 id", RingAdminRequest{Cmd: AdminRemoveNode, NodeID: "a\xffb"}, []string{"remove_node", "node_id", "UTF-8"}},
		{"join without addr", RingAdminRequest{Cmd: AdminJoin, NodeID: "b"}, []string{"join", "data_addr", "host:port"}},
		{"addr without port", RingAdminRequest{Cmd: AdminJoin, NodeID: "b", DataAddr: "h.example"}, []string{"data_addr", "host:port"}},
		{"addr port zero", RingAdminRequest{Cmd: AdminJoin, NodeID: "b", DataAddr: "h.example:0"}, []string{"data_addr", "port"}},
		{"addr port too big", RingAdminRequest{Cmd: AdminJoin, NodeID: "b", DataAddr: "h.example:70000"}, []string{"data_addr", "port"}},
		{"addr without host", RingAdminRequest{Cmd: AdminReplaceNode, NodeID: "b", DataAddr: ":4433"}, []string{"data_addr", "host"}},
		{"long addr", RingAdminRequest{Cmd: AdminJoin, NodeID: "b", DataAddr: long + ":4433"}, []string{"data_addr", "256"}},
		{"control in addr", RingAdminRequest{Cmd: AdminJoin, NodeID: "b", DataAddr: "h\n.example:4433"}, []string{"data_addr", "control character"}},
		{"replace without addr", RingAdminRequest{Cmd: AdminReplaceNode, NodeID: "b"}, []string{"replace_node", "data_addr"}},
		{"replace with witness", RingAdminRequest{Cmd: AdminReplaceNode, NodeID: "b", DataAddr: "h.example:4433", Witness: true}, []string{"replace_node", "witness"}},
		{"promote with addr", RingAdminRequest{Cmd: AdminPromote, NodeID: "b", DataAddr: "h.example:4433"}, []string{"promote", "data_addr", "node_id"}},
		{"leave with witness", RingAdminRequest{Cmd: AdminLeave, NodeID: "b", Witness: true}, []string{"leave", "witness"}},
		{"remove with database", RingAdminRequest{Cmd: AdminRemoveNode, NodeID: "b", Database: "app"}, []string{"remove_node", "database"}},
		{"join with limit", RingAdminRequest{Cmd: AdminJoin, NodeID: "b", DataAddr: "h.example:4433", Limit: u75Int32(3)}, []string{"join", "limit"}},
		{"describe_node with node id", RingAdminRequest{Cmd: AdminDescribeNode, NodeID: "b"}, []string{"describe_node", "node_id"}},
		{"create_database with witness", RingAdminRequest{Cmd: AdminCreateDatabase, Name: "app", Witness: true}, []string{"create_database", "witness"}},
	}
	for _, tc := range cases {
		_, err := EncodeRingAdmin(tc.req)
		var coded *cluster.Error
		if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: encoded or wrong error: %v", tc.name, err)
			continue
		}
		for _, word := range tc.want {
			if !strings.Contains(coded.Message, word) {
				t.Errorf("%s: the refusal %q does not mention %q", tc.name, coded.Message, word)
			}
		}
	}
}

// TestC6MembershipDecoderIsStrict: [GUARD canonical layout: order, extras, written defaults,
// wrong types, trailing bytes, and a well-formed body over 8 KiB refused before it is unpacked].
func TestC6MembershipDecoderIsStrict(t *testing.T) {
	doc := func(elements ...bson.E) []byte {
		raw, err := bson.Marshal(bson.D(elements))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	join := bson.E{Key: "cmd", Value: AdminJoin}
	id := bson.E{Key: "node_id", Value: "node-b"}
	addr := bson.E{Key: "data_addr", Value: "node-b.example:4433"}
	cases := map[string][]byte{
		"extra member":       doc(join, id, addr, bson.E{Key: "extra", Value: int32(1)}),
		"reordered":          doc(join, addr, id),
		"duplicate":          doc(join, id, id, addr),
		"witness false":      doc(join, id, addr, bson.E{Key: "witness", Value: false}),
		"witness as int":     doc(join, id, addr, bson.E{Key: "witness", Value: int32(1)}),
		"id as int":          doc(join, bson.E{Key: "node_id", Value: int32(4)}, addr),
		"promote with addr":  doc(bson.E{Key: "cmd", Value: AdminPromote}, id, addr),
		"leave no id":        doc(bson.E{Key: "cmd", Value: AdminLeave}),
		"replace witness":    doc(bson.E{Key: "cmd", Value: AdminReplaceNode}, id, addr, bson.E{Key: "witness", Value: true}),
		"accept_loss false":  doc(bson.E{Key: "cmd", Value: AdminReplaceNode}, id, addr, bson.E{Key: "accept_loss", Value: false}),
		"accept_loss as int": doc(bson.E{Key: "cmd", Value: AdminReplaceNode}, id, addr, bson.E{Key: "accept_loss", Value: int32(1)}),
		"trailing bytes":     append(doc(join, id, addr), make([]byte, 9<<10)...),
		"remove extra field": doc(bson.E{Key: "cmd", Value: AdminRemoveNode}, id, bson.E{Key: "force", Value: true}),
	}
	for name, raw := range cases {
		if _, err := DecodeRingAdmin(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A well-formed document over 8 KiB is refused by size, before it is unpacked.
	padded := doc(join, id, addr, bson.E{Key: "pad", Value: strings.Repeat("p", 9<<10)})
	if _, err := DecodeRingAdmin(padded); err == nil || !strings.Contains(err.Error(), "8192") {
		t.Errorf("a %d byte join was refused with %v, want the 8192 byte cap named", len(padded), err)
	}
	if _, err := DecodeRingAdmin(doc(join, id, addr, bson.E{Key: "witness", Value: true})); err != nil {
		t.Errorf("the canonical witness join was refused: %v", err)
	}
}

// TestC6MembershipRepliesRoundTripAndRefuse: [GUARD a reply carries epoch, state_seq and one
// result line, each bounded, and nothing else; a membership member on another command's reply
// is refused].
func TestC6MembershipRepliesRoundTripAndRefuse(t *testing.T) {
	for _, reply := range c6Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatalf("%+v: %v", reply.Membership, err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil || !reflect.DeepEqual(got, reply) {
			t.Fatalf("round trip of %+v gave %+v, %v", reply.Membership, got, err)
		}
	}
	bad := map[string]RingAdminReply{
		"no membership":     {Cmd: AdminJoin},
		"negative epoch":    {Cmd: AdminJoin, Membership: &RingAdminMembership{Epoch: -1, Result: "x"}},
		"negative seq":      {Cmd: AdminJoin, Membership: &RingAdminMembership{StateSeq: -1, Result: "x"}},
		"empty result":      {Cmd: AdminLeave, Membership: &RingAdminMembership{}},
		"long result":       {Cmd: AdminLeave, Membership: &RingAdminMembership{Result: strings.Repeat("r", ringMaxMemberResult+1)}},
		"newline in result": {Cmd: AdminLeave, Membership: &RingAdminMembership{Result: "a\nb"}},
		"bad utf8 result":   {Cmd: AdminLeave, Membership: &RingAdminMembership{Result: "a\xffb"}},
		"on show_buckets":   {Cmd: AdminShowBuckets, Buckets: []RingAdminListed{}, Membership: &RingAdminMembership{Result: "x"}},
		"with a bucket id":  {Cmd: AdminJoin, BucketID: 3, Membership: &RingAdminMembership{Result: "x"}},
		"with a node":       {Cmd: AdminJoin, Node: &RingAdminNode{NodeID: "a"}, Membership: &RingAdminMembership{Result: "x"}},
	}
	for name, reply := range bad {
		if _, err := EncodeRingAdminReply(reply); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
	extra, err := bson.Marshal(bson.D{{Key: "cmd", Value: AdminJoin}, {Key: "epoch", Value: int64(1)}, {Key: "state_seq", Value: int64(1)},
		{Key: "result", Value: "x"}, {Key: "more", Value: true}})
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := bson.Marshal(bson.D{{Key: "cmd", Value: AdminJoin}, {Key: "state_seq", Value: int64(1)}, {Key: "epoch", Value: int64(1)}, {Key: "result", Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := bson.Marshal(bson.D{{Key: "cmd", Value: AdminJoin}, {Key: "epoch", Value: int32(1)}, {Key: "state_seq", Value: int64(1)}, {Key: "result", Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"extra member": extra, "reordered": reordered, "epoch as int32": narrow} {
		if _, err := DecodeRingAdminReply(raw); err == nil {
			t.Errorf("reply %s: accepted", name)
		}
	}
}

// TestC6MembershipDecodersSurviveMutation: [GUARD 100000 mutations: no panic, an accepted
// body re-encodes to the same bytes].
func TestC6MembershipDecodersSurviveMutation(t *testing.T) {
	var requests, replies [][]byte
	for _, req := range c6Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, raw)
	}
	for _, reply := range c6Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, raw)
	}
	rng := rand.New(rand.NewPCG(6, 6))
	var acceptedRequests, acceptedReplies int
	for range 100_000 {
		mutated := u65Mutate(rng, requests[rng.IntN(len(requests))])
		if req, err := DecodeRingAdmin(mutated); err == nil {
			acceptedRequests++
			if again, err := EncodeRingAdmin(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, replies[rng.IntN(len(replies))])
		if reply, err := DecodeRingAdminReply(mutated); err == nil {
			acceptedReplies++
			if again, err := EncodeRingAdminReply(reply); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedRequests == 0 || acceptedReplies == 0 {
		t.Fatalf("accepted %d requests and %d replies: the re-encode check never ran", acceptedRequests, acceptedReplies)
	}
}

// TestC6MembershipDecodeAllocationIsBounded: [GUARD a body that lies about its length, and a
// string that lies about its own, allocate a bounded amount before they are refused].
func TestC6MembershipDecodeAllocationIsBounded(t *testing.T) {
	lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'c', 'm', 'd', 0}
	lie = append(lie, byte(len(AdminJoin)+1), 0, 0, 0)
	lie = append(lie, AdminJoin...)
	lie = append(lie, 0, 0x02, 'n', 'o', 'd', 'e', '_', 'i', 'd', 0, 0xff, 0xff, 0xff, 0x7f, 0)
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingAdmin(lie)
			_, _ = DecodeRingAdminReply(lie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("a lying membership body of %d bytes allocated %d bytes", len(lie), perOp)
	}
}

// TestC6MembershipClassBytesNeverReachTheWire: [GUARD the five classes are 0xF0 to 0xF4, above every transport opcode and above the free opcode range 0xE0 to 0xEF, are
// listed by RingClasses, collide with no other class and are refused as the class byte of a
// RING_KEYED body].
func TestC6MembershipClassBytesNeverReachTheWire(t *testing.T) {
	want := map[RingClass]bool{0xF0: true, 0xF1: true, 0xF2: true, 0xF3: true, 0xF4: true}
	got := []RingClass{RingClassJoin, RingClassPromote, RingClassLeave, RingClassRemoveNode, RingClassReplaceNode}
	seen := map[RingClass]int{}
	for _, class := range RingClasses() {
		seen[class]++
	}
	for _, class := range got {
		if !want[class] {
			t.Errorf("class 0x%02x is outside 0xF0 to 0xF4", class)
		}
		if class <= 0xEF || transport.Opcode(class) <= transport.OpRingReply {
			t.Errorf("class 0x%02x is not above the highest opcode (%s) and the free range 0xE0 to 0xEF, which wire opcodes will take", class, transport.OpRingReply)
		}
		if seen[class] != 1 {
			t.Errorf("class 0x%02x is listed %d times by RingClasses", class, seen[class])
		}
		body := append(make([]byte, RingPrologueLen), class)
		body = append(body, make([]byte, 64)...)
		if _, err := DecodeRingKeyed(body); err == nil {
			t.Errorf("class 0x%02x was accepted as the class byte of a RING_KEYED body", class)
		}
	}
	for class, count := range seen {
		if count != 1 {
			t.Errorf("class 0x%02x is listed %d times", class, count)
		}
	}
	for _, class := range u70KeyedClasses() {
		if want[class] {
			t.Errorf("membership class 0x%02x is counted as keyed", class)
		}
	}
}

// c6Ops is a fake MembershipOps that records its calls.
type c6Ops struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (o *c6Ops) record(call string) (MembershipOutcome, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, call)
	if o.err != nil {
		return MembershipOutcome{}, o.err
	}
	return MembershipOutcome{Epoch: 3, StateSeq: 11, Result: "done " + call}, nil
}

func (o *c6Ops) Join(_ context.Context, id, addr string, witness bool) (MembershipOutcome, error) {
	call := "join " + id + " " + addr
	if witness {
		call += " witness"
	}
	return o.record(call)
}
func (o *c6Ops) Promote(_ context.Context, id string) (MembershipOutcome, error) {
	return o.record("promote " + id)
}
func (o *c6Ops) Leave(_ context.Context, id string) (MembershipOutcome, error) {
	return o.record("leave " + id)
}
func (o *c6Ops) RemoveNode(_ context.Context, id string) (MembershipOutcome, error) {
	return o.record("remove_node " + id)
}
func (o *c6Ops) ReplaceNode(_ context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error) {
	if acceptLoss {
		return o.record("replace_node " + id + " " + addr + " accept_loss")
	}
	return o.record("replace_node " + id + " " + addr)
}

// c6Gate is an authorizer that records every ask and refuses the membership classes when deny is set.
type c6Gate struct {
	mu    sync.Mutex
	asked []RingAccess
	deny  bool
}

func (g *c6Gate) Authorize(_ context.Context, access RingAccess) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.asked = append(g.asked, access)
	if g.deny {
		return errors.New("c6: no grant")
	}
	return nil
}

func c6Serve(t *testing.T, gate RingAuthorizer, request RingAdminRequest, options ...RingOption) *transport.Frame {
	t.Helper()
	node, _ := u63Fresh(t)
	handler, err := NewRingHandler(node, u66AuthNext, gate, options...)
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(request)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := handler(context.Background(), transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// TestC6DefaultNodeRefusesEveryMembershipCommandWithTheFix: [GUARD a handler built without
// WithMembership refuses all five with the one typed refusal that names the two settings and
// the restart, and nothing is changed].
func TestC6DefaultNodeRefusesEveryMembershipCommandWithTheFix(t *testing.T) {
	const want = "membership changes are not available on this node yet: set cluster.enabled: true and restart; the join, promote, leave, remove-node and replace-node commands arrive with the cluster membership work"
	for _, req := range c6Requests() {
		refusal := u66Refusal(t, c6Serve(t, u70AllowAll{}, req), cluster.CodeInvalidArgument)
		if refusal.Message != want {
			t.Errorf("%s: refusal %q, want %q", req.Cmd, refusal.Message, want)
		}
	}
	_, err := NoMembership{}.Join(context.Background(), "a", "h.example:4433", false)
	if !errors.Is(err, ErrMembershipDisabled) {
		t.Errorf("NoMembership.Join returned %v, want ErrMembershipDisabled", err)
	}
	for _, call := range []func() error{
		func() error { _, err := NoMembership{}.Promote(context.Background(), "a"); return err },
		func() error { _, err := NoMembership{}.Leave(context.Background(), "a"); return err },
		func() error { _, err := NoMembership{}.RemoveNode(context.Background(), "a"); return err },
		func() error {
			_, err := NoMembership{}.ReplaceNode(context.Background(), "a", "h.example:4433", false)
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrMembershipDisabled) {
			t.Errorf("a NoMembership method returned %v, want ErrMembershipDisabled", err)
		}
	}
}

// TestC6HandlerAsksOnceClassOnlyAndDispatches: [GUARD each command is authorized as its own
// class with no database and no bucket, exactly once, and then reaches the matching
// MembershipOps method with the request's members, and the reply carries the outcome].
func TestC6HandlerAsksOnceClassOnlyAndDispatches(t *testing.T) {
	classes := map[string]RingClass{AdminJoin: RingClassJoin, AdminPromote: RingClassPromote, AdminLeave: RingClassLeave,
		AdminRemoveNode: RingClassRemoveNode, AdminReplaceNode: RingClassReplaceNode}
	wantCalls := []string{"join node-b node-b.example:4433", "join node-w node-w.example:4433 witness", "promote node-b", "leave node-b",
		"remove_node node-b", "replace_node node-b node-b2.example:4433", "replace_node node-b node-b.example:4433 accept_loss"}
	for i, req := range c6Requests() {
		gate, ops := &c6Gate{}, &c6Ops{}
		reply := u75Reply(t, c6Serve(t, gate, req, WithMembership(ops)))
		if len(gate.asked) != 1 || gate.asked[0] != (RingAccess{Class: classes[req.Cmd]}) {
			t.Errorf("%s: asked %+v, want one class-only ask of 0x%02x", req.Cmd, gate.asked, classes[req.Cmd])
		}
		if len(ops.calls) != 1 || ops.calls[0] != wantCalls[i] {
			t.Errorf("%s: ops saw %q, want %q", req.Cmd, ops.calls, wantCalls[i])
		}
		if reply.Cmd != req.Cmd || reply.Membership == nil || reply.Membership.Epoch != 3 || reply.Membership.StateSeq != 11 || reply.Membership.Result != "done "+wantCalls[i] {
			t.Errorf("%s: reply %+v", req.Cmd, reply)
		}
	}
}

// TestC6DeniedSessionLearnsNothingAndChangesNothing: [GUARD a refused session gets the one
// PERMISSION_DENIED text for all five commands, whether or not membership is enabled, naming
// no node and no setting, and the ops are never called].
func TestC6DeniedSessionLearnsNothingAndChangesNothing(t *testing.T) {
	var texts []string
	for _, req := range c6Requests() {
		for _, enabled := range []bool{false, true} {
			ops := &c6Ops{}
			var options []RingOption
			if enabled {
				options = []RingOption{WithMembership(ops)}
			}
			refusal := u66Refusal(t, c6Serve(t, &c6Gate{deny: true}, req, options...), cluster.CodePermissionDenied)
			texts = append(texts, refusal.Message)
			for _, leak := range []string{req.NodeID, req.DataAddr, "cluster.enabled", "allow_membership"} {
				if leak != "" && strings.Contains(refusal.Message, leak) {
					t.Errorf("%s: the denial names %q: %s", req.Cmd, leak, refusal.Message)
				}
			}
			if len(ops.calls) != 0 {
				t.Errorf("%s: a denied session reached the ops: %q", req.Cmd, ops.calls)
			}
		}
	}
	for _, text := range texts {
		if text != texts[0] {
			t.Fatalf("the denial text varies: %q and %q", texts[0], text)
		}
	}
}

// TestC6OpsRefusalsReachTheCallerUnchanged: [GUARD a typed refusal from the ops is the refusal
// the session sees, with its code and text].
func TestC6OpsRefusalsReachTheCallerUnchanged(t *testing.T) {
	ops := &c6Ops{err: &cluster.Error{Code: cluster.CodeConflict, Message: "node \"node-b\" is still active. Run 'leave node-b'"}}
	refusal := u66Refusal(t, c6Serve(t, u70AllowAll{}, RingAdminRequest{Cmd: AdminRemoveNode, NodeID: "node-b"}, WithMembership(ops)), cluster.CodeConflict)
	if refusal.Message != "node \"node-b\" is still active. Run 'leave node-b'" {
		t.Errorf("refusal text %q", refusal.Message)
	}
}

// TestC6StrayMembersOnTheWireAreNamed: [GUARD a body (not the Go encoder) that carries a member
// the command does not take is refused naming that member and the fix, not as a wrong layout].
func TestC6StrayMembersOnTheWireAreNamed(t *testing.T) {
	cmd := func(name string) bson.E { return bson.E{Key: "cmd", Value: name} }
	id := bson.E{Key: "node_id", Value: "node-b"}
	addr := bson.E{Key: "data_addr", Value: "node-b.example:4433"}
	cases := []struct {
		name     string
		elements []bson.E
		stray    string
	}{
		{"join with fields", []bson.E{cmd(AdminJoin), id, addr, {Key: "fields", Value: bson.A{}}}, "fields"},
		{"join with a database", []bson.E{cmd(AdminJoin), {Key: "database", Value: "app"}, id, addr}, "database"},
		{"promote with an address", []bson.E{cmd(AdminPromote), id, addr}, "data_addr"},
		{"leave with witness", []bson.E{cmd(AdminLeave), id, {Key: "witness", Value: true}}, "witness"},
		{"remove_node with a limit", []bson.E{cmd(AdminRemoveNode), id, {Key: "limit", Value: int32(3)}}, "limit"},
		{"replace_node with witness", []bson.E{cmd(AdminReplaceNode), id, addr, {Key: "witness", Value: true}}, "witness"},
		{"join with accept_loss", []bson.E{cmd(AdminJoin), id, addr, {Key: "accept_loss", Value: true}}, "accept_loss"},
	}
	for _, tc := range cases {
		raw, err := bson.Marshal(bson.D(tc.elements))
		if err != nil {
			t.Fatal(err)
		}
		_, err = DecodeRingAdmin(raw)
		var coded *cluster.Error
		if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: accepted or wrong error: %v", tc.name, err)
			continue
		}
		if !strings.Contains(coded.Message, "also sets "+tc.stray) || !strings.Contains(coded.Message, "Remove "+tc.stray) {
			t.Errorf("%s: the refusal does not name %s and the fix: %q", tc.name, tc.stray, coded.Message)
		}
	}
}

// c6PeerGate refuses a session on the internode listener, as the server's authorizer does.
type c6PeerGate struct{}

func (c6PeerGate) Authorize(ctx context.Context, _ RingAccess) error {
	if sess, ok := transport.SessionFromContext(ctx); !ok || sess.Peer {
		return errors.New("c6: internode session")
	}
	return nil
}

// TestC6APeerSessionNeverReachesTheOps: [GUARD a request on an internode session is denied
// with the one text and the MembershipOps is not called].
func TestC6APeerSessionNeverReachesTheOps(t *testing.T) {
	node, _ := u63Fresh(t)
	ops := &c6Ops{}
	handler, err := NewRingHandler(node, u66AuthNext, c6PeerGate{}, WithMembership(ops))
	if err != nil {
		t.Fatal(err)
	}
	ctx := transport.WithSession(context.Background(), &transport.Session{Peer: true})
	for _, req := range c6Requests() {
		body, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := handler(ctx, transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
		if err != nil {
			t.Fatal(err)
		}
		u66Refusal(t, frame, cluster.CodePermissionDenied)
	}
	if len(ops.calls) != 0 {
		t.Fatalf("a peer session reached the ops: %q", ops.calls)
	}
}
