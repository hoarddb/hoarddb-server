package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestUnscopedOpcodesNameNoScope is the regression test: an opcode whose rule is not
// scoped to a database or bucket names no scope for the router either, whatever
// fields its payload carries, so it runs on the node the client reached.
func TestUnscopedOpcodesNameNoScope(t *testing.T) {
	payload, err := bson.Marshal(bson.M{"database": "app", "bucket": "users", "name": "users"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	for op, rule := range opRules {
		if rule.Scope != ScopeNone || hasWireLayoutPayload(op) {
			continue
		}
		frame := transport.NewFrame(op, payload)
		bucket, database, err := scopeOfFrame(frame, decodeFrameMeta(frame.Payload))
		if err != nil {
			t.Errorf("%s: scopeOfFrame: %v", op, err)
			continue
		}
		if bucket != "" || database != "" {
			t.Errorf("%s has no scope in opRules but the router reads bucket %q in database %q", op, bucket, database)
		}
	}
}

// TestLoginIsAnsweredByTheNodeItReaches is the regression test on a cluster: an AUTH
// is answered by the node the client connected to, whatever database or bucket
// fields its payload carries, so a login naming a bucket another node owns
// succeeds with the right password.
func TestLoginIsAnsweredByTheNodeItReaches(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	otherOwned := ""
	for i := 0; i < 32 && otherOwned == ""; i++ {
		name := fmt.Sprintf("b%d", i)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, name))
		if err != nil {
			t.Fatalf("resolving the owner of %q: %v", name, err)
		}
		if owner.ID != tc.addrs[0] {
			otherOwned = name
		}
	}
	if otherOwned == "" {
		t.Fatal("no bucket of 32 is owned by the second node; the test needs one")
	}

	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test driver client"}}, tc.driverAddrs[0])
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := transport.NewDriverClient(ctx, tc.driverAddrs[0], tlsCfg)
	if err != nil {
		t.Fatalf("dial node 1: %v", err)
	}
	defer func() { _ = client.Close() }()

	payload, err := bson.Marshal(bson.M{
		"username": testRootUser, "password": testRootPassword,
		"database": testDatabase, "bucket": otherOwned,
	})
	if err != nil {
		t.Fatalf("marshal auth: %v", err)
	}
	answer, err := client.SendRequest(ctx, transport.OpAuth, payload)
	if err != nil {
		t.Fatalf("send auth: %v", err)
	}
	if answer.Opcode != transport.OpAuthOK {
		t.Fatalf("AUTH naming bucket %q owned by the other node answered %s %q, want AUTH_OK from the node reached",
			otherOwned, answer.Opcode, answer.Payload)
	}
}
