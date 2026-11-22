package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Over a real client connection: every refusal the router builds or relays
// is a frame the client can read. A client of a node alone on its ring is
// always connected to the router, so a malformed frame there is the answer
// every such client gets (the step 3 verifier's blocker 1).

// overTheWire sends one request through a fresh connection to node index and
// fails the test if the frame cannot be read.
func overTheWire(t *testing.T, tc *testCluster, client *transport.DriverClient, op transport.Opcode, request any) *transport.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	reply, err := client.SendRequest(ctx, op, spreadMarshal(t, request))
	if err != nil {
		t.Fatalf("%s over the wire: %v", op, err)
	}
	return reply
}

// requireRefusalOverTheWire fails unless reply is a readable refusal with
// code and text, naming no part.
func requireRefusalOverTheWire(t *testing.T, label string, reply *transport.Frame, code transport.ErrorCode, text string) {
	t.Helper()
	got := refusalTextOrEmptyTB(t, reply)
	if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != code || !strings.Contains(got, text) || bsonNamesAPart(reply.Payload) {
		t.Errorf("%s over the wire answered %s %q, want %s naming %q", label, reply.Opcode, reply.Payload, code, text)
	}
}

// TestPartRefusalReachesAClientOverTheWire: a part's own refusal of its share
// of a find (past the result ceiling) reaches a client of the router as the
// refusal a whole bucket gives, not a frame the client cannot read.
func TestPartRefusalReachesAClientOverTheWire(t *testing.T) {
	tc := newTestClusterWith(t, 1, 1, func(_ int, cfg *config.Config) { cfg.Query.MaxResultBytes = 64 << 10 })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	client := tc.dial(0)
	t.Cleanup(func() { _ = client.Close() })
	for _, placement := range []string{"whole", "spread"} {
		name := "B" + placement
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: map[string]interface{}{"placement": placement}})
		// One part holds every document, so a part's own find refuses.
		for _, key := range keysInPart(t, "k", 9, 300) {
			tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: key, Data: map[string]interface{}{"Pad": strings.Repeat("x", 400)}})
		}
		reply := overTheWire(t, tc, client, transport.OpScan, ScanRequest{Database: "app", Bucket: name, Limit: 5000})
		requireRefusalOverTheWire(t, placement+" find past the result ceiling", reply, transport.ErrResultTooLarge, "app."+name)
	}
}

// TestLostPartRefusalReachesAClientOverTheWire: count() and find() of a
// collection with a lost part are refused over the wire with the lost-part
// text.
func TestLostPartRefusalReachesAClientOverTheWire(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: "User"}
	sc.createCollection(0, "User", nil)
	sc.insertMany(0, "doc", 100)
	part := partName("User", spreadPartOf(t, "doc-00000"))
	if resp, err := tc.nodes[0].server.core.DeleteBucket(DeleteBucketRequest{Database: "app", Name: part}); err != nil || !resp.Success {
		t.Fatalf("dropping a part past the router: %+v %v", resp, err)
	}
	client := tc.dial(0)
	t.Cleanup(func() { _ = client.Close() })
	requireRefusalOverTheWire(t, "count() with a lost part", overTheWire(t, tc, client, transport.OpCount, CountRequest{Database: "app", Bucket: "User"}),
		transport.ErrUnavailable, "not held")
	requireRefusalOverTheWire(t, "find() with a lost part", overTheWire(t, tc, client, transport.OpScan, ScanRequest{Database: "app", Bucket: "User"}),
		transport.ErrUnavailable, "not held")
}

// TestRelayedDumpRefusalReachesAClientOverTheWire: a part owner's refusal of
// a dump frame (its one dump slot held by another collection's part) is
// relayed to the client of the router as a readable refusal naming the other
// collection.
func TestRelayedDumpRefusalReachesAClientOverTheWire(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	for _, name := range []string{"User", "Other"} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}})
		// Part 0 holds more than a chunk, so its owner's slot stays claimed
		// while the first chunk is out.
		for _, key := range keysInPart(t, "big", 0, 12) {
			tc.ok(0, transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: key, Data: map[string]interface{}{"Pad": strings.Repeat("y", 512<<10)}})
		}
	}
	first, second := tc.dial(0), tc.dial(0)
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	for _, step := range []struct {
		client *transport.DriverClient
		op     transport.Opcode
		req    any
	}{
		{first, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "User", AcceptStores: true, AcceptDead: true}},
		{first, transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "User"}},
		{second, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "Other", AcceptStores: true, AcceptDead: true}},
	} {
		if reply := overTheWire(t, tc, step.client, step.op, step.req); reply.Opcode == transport.OpError {
			t.Fatalf("%s: %s", step.op, refusalTextOrEmptyTB(t, reply))
		}
	}
	busy := overTheWire(t, tc, second, transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "Other"})
	requireRefusalOverTheWire(t, "a second collection's dump meeting the first's part", busy, transport.ErrConflict, "app/User")
}
