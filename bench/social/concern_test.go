package main

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// concernRecorder is a hoardDB engine whose connections reach a fake server and
// whose every wire is kept, so a test can read back what was sent.
type concernRecorder struct {
	engine *hoardEngine
	mu     sync.Mutex // seeding opens connections from several goroutines
	wires  []*fakeWire
}

func newConcernRecorder(concern string) *concernRecorder {
	recorder := &concernRecorder{}
	fake := newFakeServer()
	cfg := &config{Engine: engineHoardDB, Database: "social", Users: tinySpec.Users, Conc: 4, OpTimeout: time.Second, WriteConcern: concern}
	recorder.engine = &hoardEngine{cfg: cfg, dial: func(string) (wireCaller, func(), error) {
		wire := fake.dial(nil, nil)
		recorder.mu.Lock()
		recorder.wires = append(recorder.wires, wire)
		recorder.mu.Unlock()
		return wire, func() {}, nil
	}}
	return recorder
}

// sent is every request of every connection, in the order each connection sent them.
func (r *concernRecorder) sent() []fakeRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	var all []fakeRequest
	for _, wire := range r.wires {
		all = append(all, wire.log...)
	}
	return all
}

// cannotCarryConcern are the mutations whose request types have no write-concern
// field on the wire (server.UpdateRequest, ClaimRequest, LeaseRequest): they run
// at the node's default. A mutation missing from this list that also lacks the
// field is a new gap, and the test names it.
var cannotCarryConcern = map[transport.Opcode]bool{
	transport.OpUpdate:     true,
	transport.OpQueueClaim: true,
	transport.OpQueueAck:   true,
}

var mutatingOps = map[transport.Opcode]bool{
	transport.OpPut: true, transport.OpUpsert: true, transport.OpFieldPut: true, transport.OpFieldPush: true, transport.OpFieldDelete: true,
	transport.OpFieldPop: true, transport.OpFIFOPush: true, transport.OpUpdate: true, transport.OpQueueClaim: true,
	transport.OpQueueAck: true,
}

// writeConcernOf reads the WriteConcern field of a request, promoted or not.
func writeConcernOf(request interface{}) (string, bool) {
	field := reflect.ValueOf(request).FieldByName("WriteConcern")
	if !field.IsValid() {
		return "", false
	}
	return field.String(), true
}

// TestWriteConcernSentOnEveryHoardMutation seeds a dataset and runs every
// mutating operation (W1 to W6, N1, Q1) with -write-concern majority and
// checks that every request that changes data and can carry a concern did.
func TestWriteConcernSentOnEveryHoardMutation(t *testing.T) {
	recorder := newConcernRecorder("majority")
	ctx := context.Background()
	graph := newSocialGraph(tinySpec, 7)
	if _, err := recorder.engine.setupSocial(ctx, tinySpec); err != nil {
		t.Fatalf("setting up: %v", err)
	}
	if err := recorder.engine.seedSocial(ctx, graph, 2); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	ops, err := recorder.engine.newSocialOps()
	if err != nil {
		t.Fatalf("opening a connection: %v", err)
	}
	defer ops.close()
	post := postRecord{Author: userID(3), Seq: 100, Text: "t", Created: time.Now().UTC()}
	steps := map[string]func() error{
		"W1": func() error { _, err := ops.publishPost(ctx, post, noLedger{}); return err },
		"W2": func() error { return ops.likePost(ctx, 4, 3, 1) },
		"W3": func() error { return ops.follow(ctx, 4, 5) },
		"W4": func() error { return ops.unfollow(ctx, 4, 5) },
		"W5": func() error { return ops.commentOnPost(ctx, 4, 3, 1, "hello") },
		"W6": func() error { return ops.uploadAvatar(ctx, 4, []byte("avatar")) },
		"N1": func() error { _, err := ops.readNotifications(ctx, 3, 5); return err },
		"Q1": func() error { _, err := ops.queueRound(ctx, 1); return err },
	}
	for name, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	seen := map[transport.Opcode]int{}
	for _, request := range recorder.sent() {
		if !mutatingOps[request.op] {
			continue
		}
		seen[request.op]++
		concern, carries := writeConcernOf(request.request)
		switch {
		case carries && concern != "majority":
			t.Errorf("%s was sent with write concern %q; every mutation must carry %q", request.op, concern, "majority")
		case !carries && !cannotCarryConcern[request.op]:
			t.Errorf("%s (%T) has no write-concern field and is not in the known list; it would run at the node's default", request.op, request.request)
		}
	}
	for _, op := range []transport.Opcode{transport.OpUpsert, transport.OpFieldPut, transport.OpFieldPush, transport.OpFieldDelete, transport.OpFieldPop, transport.OpFIFOPush} {
		if seen[op] == 0 {
			t.Errorf("no %s was sent: the test no longer exercises that mutation", op)
		}
	}
}

func TestWriteConcernUnsetSendsNoConcern(t *testing.T) {
	recorder := newConcernRecorder("")
	ctx := context.Background()
	if _, err := recorder.engine.setupSocial(ctx, tinySpec); err != nil {
		t.Fatalf("setting up: %v", err)
	}
	if err := recorder.engine.seedSocial(ctx, newSocialGraph(tinySpec, 7), 2); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	for _, request := range recorder.sent() {
		if concern, carries := writeConcernOf(request.request); carries && concern != "" {
			t.Fatalf("%s carried write concern %q although none was asked for: the node's default must apply", request.op, concern)
		}
	}
}

func TestParseWriteConcern(t *testing.T) {
	for in, want := range map[string]string{"": "", "1": "one", "One": "one", " majority ": "majority", "ALL": "all"} {
		if got, err := parseWriteConcern(in); err != nil || got != want {
			t.Errorf("parseWriteConcern(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := parseWriteConcern("quorum"); err == nil {
		t.Error("quorum accepted; hoardDB would refuse it as an unknown write concern")
	}
}
