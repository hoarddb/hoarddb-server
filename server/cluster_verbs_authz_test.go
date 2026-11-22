package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// These tests are the behavioural half of the authorization-gate fix. The structural guard
// (TestNoOpcodeIsDispatchedBeforeAuthorizeWithoutARecordedGate) proves no
// opcode sits above s.authorize without a recorded gate; these prove what a
// session without cluster-manager actually receives when it asks for a
// membership or placement change over the driver listener, on one node and on
// three.

const clusterVerbsTestPassword = "cluster-verbs-password-123"

// clusterVerbsLowPrivilegeGrants are the sessions the bug names: a reader,
// and a writer confined to one database.
var clusterVerbsLowPrivilegeGrants = []UserRole{
	{Role: "reader", DB: testDatabase},
	{Role: "writer", DB: testDatabase},
}

// clusterVerbsUsername names the user holding exactly one grant.
func clusterVerbsUsername(grant UserRole) string {
	return "verbs-" + grant.Role + "-" + grant.DB
}

// clusterVerbsCreateRequest builds a user-create request with a fresh
// credential: the store refuses a salt it has seen.
func clusterVerbsCreateRequest(t *testing.T, grant UserRole) *UserCreateRequest {
	t.Helper()
	credential, err := NewCredential(clusterVerbsTestPassword, DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	return &UserCreateRequest{
		RequestID:  fmt.Sprintf("create-%s-%d", clusterVerbsUsername(grant), time.Now().UnixNano()),
		Username:   clusterVerbsUsername(grant),
		Credential: UserCredential{Algo: credential.Algo, Salt: credential.Salt, Hash: credential.Hash, Params: credential.Params},
		Roles:      []UserRole{grant},
	}
}

// clusterVerbRequest is one operator verb and the payload a low-privilege
// session sends for it.
type clusterVerbRequest struct {
	opcode  transport.Opcode
	request any
}

// clusterVerbRequests names the three verbs. removeTarget is a real member on
// a cluster, so an unrefused remove would change the membership the test then
// checks; addTarget is a closed loopback port, so an unrefused add fails fast
// instead of waiting on a dial.
func clusterVerbRequests(removeTarget string) []clusterVerbRequest {
	return []clusterVerbRequest{
		{transport.OpClusterAdd, ClusterAddRequest{Address: "127.0.0.1:1"}},
		{transport.OpClusterRemove, ClusterRemoveRequest{NodeID: removeTarget}},
		{transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: testBucket}},
	}
}

// requireClusterAdminRefusal sends one verb on conn and fails the test unless
// the answer is an error frame naming the cluster-manager privilege.
func requireClusterAdminRefusal(t *testing.T, conn *queryConn, verb clusterVerbRequest) {
	t.Helper()
	payload, err := bson.Marshal(verb.request)
	if err != nil {
		t.Fatalf("encoding %s: %v", verb.opcode, err)
	}
	if err := conn.send(transport.NewFrame(verb.opcode, payload)); err != nil {
		t.Fatalf("sending %s: %v", verb.opcode, err)
	}
	if err := conn.conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("setting the read deadline: %v", err)
	}
	answer, err := conn.read()
	if err != nil {
		t.Fatalf("reading the answer to %s: %v", verb.opcode, err)
	}
	// Errorf, not Fatalf: each verb is its own finding, and an answer of
	// either kind leaves the connection usable for the next verb.
	if answer.Opcode != transport.OpError {
		t.Errorf("%s from a session without cluster-manager was dispatched to its handler (answered %s); want a permission refusal",
			verb.opcode, answerText(answer))
		return
	}
	message := refusalMessage(t, answer)
	if !strings.Contains(message, "permission denied") || !strings.Contains(message, `"cluster-manager"`) {
		t.Errorf("%s was refused with %q; want a permission refusal naming \"cluster-manager\"", verb.opcode, message)
	}
}

// TestLowPrivilegeSessionIsRefusedClusterVerbsOnOneNode: a standalone node
// with the driver listener refuses add-node, remove-node and migrate from a
// reader session and from a one-database writer session.
func TestLowPrivilegeSessionIsRefusedClusterVerbsOnOneNode(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	root := dialTCPServer(t, addr)
	root.login()
	for _, grant := range clusterVerbsLowPrivilegeGrants {
		var created UserMutationResponse
		root.call(transport.OpUserCreate, clusterVerbsCreateRequest(t, grant), &created)
	}

	for _, grant := range clusterVerbsLowPrivilegeGrants {
		t.Run(clusterVerbsUsername(grant), func(t *testing.T) {
			conn := mustDialQueryConn(t, addr, clusterVerbsUsername(grant), clusterVerbsTestPassword)
			for _, verb := range clusterVerbRequests("127.0.0.1:1") {
				requireClusterAdminRefusal(t, conn, verb)
			}
		})
	}
}

// TestLowPrivilegeSessionIsRefusedClusterVerbsOnThreeNodes: on a three-node
// cluster every member refuses the three verbs from both sessions, and the
// membership epoch does not move.
func TestLowPrivilegeSessionIsRefusedClusterVerbsOnThreeNodes(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	masterIndex := clusterMasterIndex(t, tc)
	for _, grant := range clusterVerbsLowPrivilegeGrants {
		tc.ok(masterIndex, transport.OpUserCreate, clusterVerbsCreateRequest(t, grant))
	}

	epochsBefore := make([]uint64, len(tc.nodes))
	for i, node := range tc.nodes {
		epochsBefore[i] = node.server.core.cluster.Epoch()
	}

	for _, grant := range clusterVerbsLowPrivilegeGrants {
		username := clusterVerbsUsername(grant)
		for nodeIndex, addr := range tc.driverAddrs {
			t.Run(fmt.Sprintf("%s/node%d", username, nodeIndex+1), func(t *testing.T) {
				conn := dialReplicatedUser(t, addr, username)
				for _, verb := range clusterVerbRequests(tc.addrs[(nodeIndex+2)%len(tc.addrs)]) {
					requireClusterAdminRefusal(t, conn, verb)
				}
			})
		}
	}

	for i, node := range tc.nodes {
		if epoch := node.server.core.cluster.Epoch(); epoch != epochsBefore[i] {
			t.Errorf("node %d's membership epoch moved from %d to %d after only refused requests", i+1, epochsBefore[i], epoch)
		}
	}
}

// dialReplicatedUser logs in as username on addr, retrying while the user the
// master created has not yet reached that member.
func dialReplicatedUser(t *testing.T, addr, username string) *queryConn {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := dialQueryConn(addr, username, clusterVerbsTestPassword)
		if err == nil {
			t.Cleanup(func() { _ = conn.conn.Close() })
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s could not log in on %s within the retry window: %v", username, addr, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
