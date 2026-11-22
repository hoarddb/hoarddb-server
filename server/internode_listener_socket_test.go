package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The handler-level tests in internode_listener_authz_test.go fabricate a
// peer-flagged session. These drive a real internode listener on a real
// three-node cluster, so the listener, the transport's auth gate and the
// server's AUTH handler are all in the path (the owed behavioural
// test).

// dialInternodeUnauthenticated opens a TLS connection to a node's internode
// listener and sends nothing. The caller sends its own AUTH.
func dialInternodeUnauthenticated(t *testing.T, tc *testCluster, index int) *transport.DriverClient {
	t.Helper()
	addr := tc.ports[index].internodeAddr
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "server test client on the internode listener"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config for %s: %v", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := transport.NewDriverClient(ctx, addr, tlsCfg)
	if err != nil {
		t.Fatalf("dial the internode listener %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// sendOnInternode sends one frame on an internode connection and returns the
// answer.
func sendOnInternode(t *testing.T, client *transport.DriverClient, op transport.Opcode, request any) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("marshal %s: %v", op, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	answer, err := client.SendRequest(ctx, op, payload)
	if err != nil {
		t.Fatalf("send %s on the internode listener: %v", op, err)
	}
	return answer
}

// internodeAuthFailure decodes an AUTH_FAIL payload.
func internodeAuthFailure(t *testing.T, frame *transport.Frame) AuthFailResponse {
	t.Helper()
	var failure AuthFailResponse
	if err := bson.Unmarshal(frame.Payload, &failure); err != nil {
		t.Fatalf("decoding the AUTH_FAIL payload: %v", err)
	}
	return failure
}

// TestInternodeListenerRefusesAUserAccountOverTheSocket: the root user's
// correct password, and a resumption token the root user earned on the driver
// port, are both refused on the internode listener, and the connection stays
// unauthenticated afterwards.
func TestInternodeListenerRefusesAUserAccountOverTheSocket(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	t.Run("password", func(t *testing.T) {
		conn := dialInternodeUnauthenticated(t, tc, 0)
		answer := sendOnInternode(t, conn, transport.OpAuth, AuthRequest{Username: testRootUser, Password: testRootPassword})
		if answer.Opcode != transport.OpAuthFail {
			t.Fatalf("the root user's correct password on the internode listener answered %s, want AUTH_FAIL", answer.Opcode)
		}
		failure := internodeAuthFailure(t, answer)
		if !strings.Contains(failure.Message, "driver port") {
			t.Fatalf("the refusal %q does not send the client to the driver port", failure.Message)
		}

		// A wrong password for the same account, and an account that does
		// not exist, must be refused with the same words: the refusal must
		// not say whether the password or the username was right.
		for _, probe := range []AuthRequest{
			{Username: testRootUser, Password: "not-the-password"},
			{Username: "no-such-account", Password: "anything"},
		} {
			probeConn := dialInternodeUnauthenticated(t, tc, 0)
			probeAnswer := sendOnInternode(t, probeConn, transport.OpAuth, probe)
			if probeAnswer.Opcode != transport.OpAuthFail {
				t.Fatalf("AUTH as %q on the internode listener answered %s, want AUTH_FAIL", probe.Username, probeAnswer.Opcode)
			}
			probeFailure := internodeAuthFailure(t, probeAnswer)
			if probeFailure.Message != failure.Message || probeFailure.Reason != failure.Reason {
				t.Fatalf("AUTH as %q answered (%d, %q); the correct password answered (%d, %q). The refusal tells a right credential from a wrong one",
					probe.Username, probeFailure.Reason, probeFailure.Message, failure.Reason, failure.Message)
			}
		}

		// The refused connection holds no session: an ordinary opcode on it
		// is refused, and the database is not created anywhere.
		create := sendOnInternode(t, conn, transport.OpCreateDB, CreateDatabaseRequest{Name: "byuseroninternode"})
		if create.Opcode != transport.OpError {
			t.Fatalf("CREATE_DB on a refused internode connection answered %s, want an error", create.Opcode)
		}
		listing := tc.ok(0, transport.OpListDBs, struct{}{})
		if strings.Contains(string(listing.Payload), "byuseroninternode") {
			t.Fatal("a database was created from a connection the internode listener refused to authenticate")
		}
	})

	t.Run("token", func(t *testing.T) {
		// A valid resumption token for the root user, minted by the node's
		// own auth manager exactly as a driver-port login mints one.
		claims, err := tc.nodes[0].server.core.auth.ClaimsFor(testRootUser)
		if err != nil {
			t.Fatalf("claims for the root user: %v", err)
		}
		token, err := tc.nodes[0].server.core.auth.GenerateToken(claims)
		if err != nil {
			t.Fatalf("minting a token for the root user: %v", err)
		}

		conn := dialInternodeUnauthenticated(t, tc, 0)
		answer := sendOnInternode(t, conn, transport.OpAuth, AuthRequest{Token: token})
		if answer.Opcode != transport.OpAuthFail {
			t.Fatalf("the root user's token on the internode listener answered %s, want AUTH_FAIL", answer.Opcode)
		}
		create := sendOnInternode(t, conn, transport.OpCreateDB, CreateDatabaseRequest{Name: "bytokenoninternode"})
		if create.Opcode != transport.OpError {
			t.Fatalf("CREATE_DB after a refused token answered %s, want an error", create.Opcode)
		}
	})
}

// TestInternodeListenerStillAdmitsAMemberOverTheSocket: the node identity
// with the cluster token authenticates and is exempt from the user gate; the
// node identity with a wrong token does not.
func TestInternodeListenerStillAdmitsAMemberOverTheSocket(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	wrong := dialInternodeUnauthenticated(t, tc, 0)
	wrongAnswer := sendOnInternode(t, wrong, transport.OpAuth, AuthRequest{Username: NodeUser, Password: "not-the-cluster-token"})
	if wrongAnswer.Opcode != transport.OpAuthFail {
		t.Fatalf("the node identity with a wrong cluster token answered %s, want AUTH_FAIL", wrongAnswer.Opcode)
	}

	member := dialInternodeUnauthenticated(t, tc, 0)
	memberAnswer := sendOnInternode(t, member, transport.OpAuth, AuthRequest{Username: NodeUser, Password: tc.token})
	if memberAnswer.Opcode != transport.OpAuthOK {
		t.Fatalf("the node identity with the cluster token answered %s, want AUTH_OK: a member can no longer join", memberAnswer.Opcode)
	}
	// A member's session is exempt from the user gate for the opcode a member
	// sends as itself, MIGRATE, and for no client opcode.
	create := sendOnInternode(t, member, transport.OpCreateDB, CreateDatabaseRequest{Name: "bymember"})
	if create.Opcode != transport.OpError || decodeErrorCode(t, create.Payload) != int(transport.ErrPermissionDenied) {
		t.Fatalf("a raw CREATE_DB on a member's internode session answered %s %s, want PERMISSION_DENIED", create.Opcode, string(create.Payload))
	}
	migrate := sendOnInternode(t, member, transport.OpMigrate, MigrateRequest{Database: "nosuchdb", Bucket: "nosuchbucket"})
	if migrate.Opcode == transport.OpError && decodeErrorCode(t, migrate.Payload) == int(transport.ErrPermissionDenied) {
		t.Fatalf("MIGRATE on a member's internode session was refused by the gate: %s", string(migrate.Payload))
	}

	// Once a member, a user's credentials still cannot replace the identity.
	swap := sendOnInternode(t, member, transport.OpAuth, AuthRequest{Username: testRootUser, Password: testRootPassword})
	if swap.Opcode != transport.OpAuthFail {
		t.Fatalf("a user AUTH on an authenticated member session answered %s, want AUTH_FAIL", swap.Opcode)
	}
}

// TestForwardedClientFramesStillWorkAfterTheInternodeFix: a client connected
// to a node that does not own the bucket writes and reads through it. RF=1 on
// three nodes, so two of the three nodes must forward over the internode
// connection.
func TestForwardedClientFramesStillWorkAfterTheInternodeFix(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)

	forwarded := 0
	for index := range tc.nodes {
		if index == ownerIndex {
			continue
		}
		forwarded++
		key := "via-node-" + string(rune('1'+index))
		tc.ok(index, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: key, Data: map[string]interface{}{"key": key},
		})
		for reader := range tc.nodes {
			if found, message := tc.readFromNode(reader, key); !found {
				t.Fatalf("key %q written through non-owner node %d is not readable through node %d: %s", key, index+1, reader+1, message)
			}
		}
	}
	if forwarded != 2 {
		t.Fatalf("expected two non-owner coordinators on a three-node RF=1 cluster, exercised %d", forwarded)
	}

	// Dump and restore have their own gate (requireRootIdentity), which runs
	// again on the owner under the member's internode session. Dump through
	// a node that does not own the source, restore through a node that does
	// not own the target.
	dumpCoordinator := (ownerIndex + 1) % len(tc.nodes)
	manifest, dumped := dumpBucketOverWire(t, tc, dumpCoordinator, testDatabase, testBucket, 2)
	if manifest.SourceNode != owner.ID {
		t.Fatalf("manifest.source_node = %q, want the owner %q: the dump was not forwarded", manifest.SourceNode, owner.ID)
	}

	restoredBucket := "docs_restored"
	restoredOwner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, restoredBucket))
	if err != nil {
		t.Fatalf("resolve the restore target's owner: %v", err)
	}
	restoreCoordinator := (nodeIndexByAddr(t, tc.addrs, restoredOwner.ID) + 1) % len(tc.nodes)
	// The target exists before the restore: a restore forwarded to an owner
	// that has no such bucket is refused ("no local copy"), on the pre-fix
	// tree too, which is a separate question from the one this test asks.
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: restoredBucket, Type: BucketTypeHash})
	restored := restoreBucketOverWire(t, tc, restoreCoordinator, RestoreStartRequest{
		Database: testDatabase, Bucket: restoredBucket,
		FormatVersion: manifest.FormatVersion, StoreType: manifest.StoreType, Bytes: int64(len(dumped)),
	}, dumped, 2)
	if restored.Applied != 2 {
		t.Fatalf("forwarded restore applied %d documents, want 2", restored.Applied)
	}
	for reader := range tc.nodes {
		frame := tc.frame(reader, transport.OpCount, CountRequest{Database: testDatabase, Bucket: restoredBucket})
		if frame.Opcode == transport.OpError {
			t.Fatalf("counting the restored bucket through node %d: %s", reader+1, string(frame.Payload))
		}
	}
}
