package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// opcodeNoBuildDefines stands for a write opcode a newer build adds: no build
// defines it, so this one does not implement it, as an older owner would not.
const opcodeNoBuildDefines transport.Opcode = 0xEE

// forwardedOpcodeCluster is three nodes at factor 3 holding testBucket, with
// the index of its owner and of a coordinator that is not the owner.
func forwardedOpcodeCluster(t *testing.T) (tc *testCluster, owner, coordinator int) {
	t.Helper()
	tc = newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner = ownerIndex(t, tc, testBucket)
	return tc, owner, (owner + 1) % len(tc.nodes)
}

// forwardToOwner sends one write to the owner the way a coordinator forwards
// it (handleReplPush's forwarded branch) and returns the owner's answer.
func forwardToOwner(t *testing.T, tc *testCluster, owner, coordinator int, op transport.Opcode, request any) ReplPushResponse {
	t.Helper()
	ownerCluster := tc.nodes[owner].server.core.cluster
	push := mustMarshalBSON(t, ReplPushRequest{
		Token:          ownerCluster.localToken(),
		FromNode:       tc.addrs[coordinator],
		ReturnResponse: true,
		Epoch:          ownerCluster.Epoch(),
		Entries:        []ReplEntry{{Op: int32(op), Bucket: testBucket, Payload: mustMarshalBSON(t, request)}},
	})
	answer, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), tc.nodes[owner].server.core)
	if err != nil {
		t.Fatalf("forwarding %s to the owner: %v", op, err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(answer.Payload, &resp); err != nil {
		t.Fatalf("decoding the owner's answer to %s: %v", op, err)
	}
	return resp
}

// queuedPast reports whether the owner has queued an entry at or past seq for
// any of its replicas.
func queuedPast(c *Cluster, seq uint64) bool {
	c.peersMu.Lock()
	peers := make([]*peer, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.peersMu.Unlock()
	for _, p := range peers {
		p.streamMu.Lock()
		queued := p.queuedThrough
		p.streamMu.Unlock()
		if queued >= seq {
			return true
		}
	}
	return false
}

// TestAnOwnerRefusesAForwardedOpcodeItDoesNotImplementBeforeItsLog is
// BUG-0561's reproduction. An older owner in a mixed-version cluster receives a
// write whose opcode a newer coordinator implements and it does not. It must
// refuse it before its log: an entry logged and shipped is applied by the newer
// replicas while the client is told nothing happened, and the owner and the
// replicas then hold different data with nothing to say so.
func TestAnOwnerRefusesAForwardedOpcodeItDoesNotImplementBeforeItsLog(t *testing.T) {
	tc, owner, coordinator := forwardedOpcodeCluster(t)
	ownerCore := tc.nodes[owner].server.core
	logged := ownerCore.wal.LastSequence()

	resp := forwardToOwner(t, tc, owner, coordinator, opcodeNoBuildDefines,
		InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "newer", Data: map[string]interface{}{"n": 1}})

	if now := ownerCore.wal.LastSequence(); now != logged {
		t.Errorf("the owner's log went from %d to %d for opcode 0x%02X, which it does not implement: "+
			"the entry is logged and shipped before the owner finds out it cannot run it", logged, now, uint8(opcodeNoBuildDefines))
	}
	if queuedPast(ownerCore.cluster, logged+1) {
		t.Errorf("the owner queued an entry past sequence %d for a replica: an opcode it does not implement reaches the copies", logged)
	}
	answered := resp.Error
	for _, entry := range resp.Responses {
		var entryAnswer bson.M
		if err := bson.Unmarshal(entry.Payload, &entryAnswer); err != nil {
			t.Fatalf("decoding the owner's answer for the entry: %v", err)
		}
		answered += fmt.Sprintf(" [entry answered %s: %v]", transport.Opcode(entry.Opcode), entryAnswer)
	}
	if resp.Success || resp.WriteOutcome != outcomeNotApplied {
		t.Errorf("the owner answered success=%t write_outcome=%q error=%q; want a refusal with write_outcome %q",
			resp.Success, resp.WriteOutcome, answered, outcomeNotApplied)
	}
	for _, want := range []string{"0xEE", tc.addrs[owner], "upgrade"} {
		if !strings.Contains(resp.Error, want) {
			t.Errorf("the refusal %q does not name %q: it must name the opcode and the owner and say what to do", resp.Error, want)
		}
	}
}

// TestAnOwnerStillLogsAppliesAndShipsAForwardedOpcodeItImplements is
// BUG-0561's green path: the refusal of an unimplemented opcode leaves a
// forwarded write the owner implements appended, applied and shipped.
func TestAnOwnerStillLogsAppliesAndShipsAForwardedOpcodeItImplements(t *testing.T) {
	tc, owner, coordinator := forwardedOpcodeCluster(t)
	ownerCore := tc.nodes[owner].server.core
	logged := ownerCore.wal.LastSequence()

	resp := forwardToOwner(t, tc, owner, coordinator, transport.OpPut,
		InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "implemented", Data: map[string]interface{}{"key": "implemented"}})

	if !resp.Success || len(resp.Responses) != 1 || transport.Opcode(resp.Responses[0].Opcode) == transport.OpError {
		t.Fatalf("the owner refused a forwarded PUT: success=%t refusal=%q error=%q responses=%d",
			resp.Success, resp.Refusal, resp.Error, len(resp.Responses))
	}
	if now := ownerCore.wal.LastSequence(); now != logged+1 {
		t.Errorf("the owner's log went from %d to %d for a forwarded PUT; want one entry appended", logged, now)
	}
	deadline := time.Now().Add(10 * time.Second)
	for index := range tc.nodes {
		for {
			found, _ := tc.readFromNode(index, "implemented")
			if found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d (%s) does not hold the forwarded PUT 10s after its owner applied it: it was not shipped",
					index+1, tc.addrs[index])
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TestDispatchHandlesExactlyTheOpcodesItSaysItImplements is BUG-0561's drift
// test. An owner refuses a forwarded write by implementsOpcode before its log,
// and dispatch's default arm refuses one after it: every opcode dispatch runs
// must be in the implemented set, and the set must hold nothing dispatch
// refuses. Otherwise an owner logs and ships an opcode it then refuses, or
// refuses one it runs. Every value of the opcode byte is asked.
func TestDispatchHandlesExactlyTheOpcodesItSaysItImplements(t *testing.T) {
	s := newTestServer(t)
	implemented := 0
	for value := 0; value <= 0xFF; value++ {
		op := transport.Opcode(value)
		answer, err := s.dispatch(context.Background(), transport.NewFrame(op, nil))
		refused := err == nil && answer != nil && answer.Opcode == transport.OpError &&
			strings.Contains(string(answer.Payload), "this server does not implement it")
		switch {
		case implementsOpcode(op) && refused:
			t.Errorf("opcode 0x%02X (%s) is in the implemented set, but dispatch answers that it does not implement it", value, op)
		case !implementsOpcode(op) && !refused:
			t.Errorf("dispatch runs opcode 0x%02X (%s), which is not in the implemented set: "+
				"an owner would refuse it when forwarded although it can run it; add it to localHandlers", value, op)
		}
		if implementsOpcode(op) {
			implemented++
		}
	}
	if implemented == 0 {
		t.Fatal("the implemented set is empty")
	}
}
