package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAdoptedMemberReceivesReplication is the regression test for
// the cluster-join-correctness-build record defect A: a node joined after boot
// must get its replication sender, or a majority write whose replica set
// includes it stalls forever on the write-concern deadline.
//
// Reproduces the exact live failure: one node boots alone (RF=2), a joiner
// is added, then a bucket is created — before the fix the create itself
// failed with "write concern not satisfied: 1 of 2 copies confirmed"
// because the owner had no WAL sender for the joiner.
func TestAdoptedMemberReceivesReplication(t *testing.T) {
	tc := newTestCluster(t, 1, 2)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	oc0 := tc.nodes[0].server.core.cluster
	if err := oc0.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}

	joiner := startJoinTarget(t, tc, ports, tc.token)
	jc := joiner.server.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joining node resync: %v", err)
	}
	if jc.Epoch() != InitialEpoch+1 {
		t.Fatalf("joining node epoch = %d, want %d", jc.Epoch(), InitialEpoch+1)
	}

	// The bucket is created after the join, so its replica set spans both
	// members and the create must be acknowledged by the joiner. Name it so
	// that *this* node (node 0) owns it: only then does the write need
	// node 0's WAL sender for the adopted joiner — which is the path defect
	// A fixed. Before the fix this frame timed out on the write concern.
	bucketName := ""
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("docs%02d", i)
		if owner, err := oc0.Owner(placementOf(testDatabase, candidate)); err == nil && owner.ID == oc0.self.ID {
			bucketName = candidate
			break
		}
	}
	if bucketName == "" {
		t.Skip("no bucket name in 100 hashes places ownership on node 0; placement changed")
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: bucketName, Type: BucketTypeHash,
	})

	tc.ok(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: bucketName, Key: "k1",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "majority",
	})

	// The document must be readable from both nodes: replication wrote to
	// the joiner, not just a ring entry that knows about it. `ja` is the
	// joiner's ring (internode) address, not its driver port — dial the
	// driver client startJoinTarget already authenticated for us.
	payload, _ := bson.Marshal(&FindRequest{Database: testDatabase, Bucket: bucketName, Key: "k1"})
	resp, err := joiner.client.SendRequest(ctx, transport.OpGet, payload)
	if err != nil {
		t.Fatalf("read from joiner: %v", err)
	}
	if resp.Opcode == transport.OpError {
		t.Fatalf("read from joiner failed: %s", resp.Payload)
	}
	var found FindResponse
	if err := bson.Unmarshal(resp.Payload, &found); err != nil {
		t.Fatalf("decode joiner read: %v", err)
	}
	if !found.Found {
		t.Fatal("joiner has no copy of the document; replication did not reach it")
	}
}

// TestClusterAddResolvesBareAddress is the regression test for defect B:
// `join host:port` (no identity) must record the target's announced
// identity — not identity=address, or a NODE_ID-configured node can never
// adopt the entry, and the joiner must actually converge onto the new
// epoch instead of being unable to recognise itself. The probe must also
// fail loudly for an unreachable target.
func TestClusterAddResolvesBareAddress(t *testing.T) {
	tc := newTestCluster(t, 1, 2)
	defer tc.stop()

	ports := reserveNodePorts(t)
	ja := ports.ringAddr

	// Joiner announcing a real NODE_ID. Its seeds deliberately do NOT list
	// its own address: memberList would then re-derive self identity from
	// the bare seed and announce address identity instead, which is the
	// address-identity mode (also legitimate, but not what this test pins).
	jonerDir := t.TempDir()
	if err := SaveClusterToken(jonerDir, ClusterToken{
		Token:     tc.token,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("save joiner token: %v", err)
	}
	// The bare-address probe dials the joiner, so node 0 must pin it; the
	// joiner pins the seed it was configured with.
	trustJoinerFingerprint(t, tc, ja, jonerDir)
	joinerCfg := config.DefaultConfig()
	joinerCfg.Storage.DataDir = jonerDir
	joinerCfg.Server.Listen = ja
	joinerCfg.Server.NodeID = "itest-join"
	joinerCfg.Server.ReplicationFactor = 2
	joinerCfg.HashRing.SeedNodes = []string{tc.addrs[0]}
	joinerCfg.HashRing.SeedFingerprints = []string{tc.fingerprints[0]}
	joinerCfg.Metrics.Enabled = false
	joinerCfg.Audit.Enabled = false
	joinerCfg.Logging.Output = "stderr"
	// The driver listener is unconditional now (no TCP.Enabled to skip it
	// with); give this joiner its own port like startJoinTarget does, so it
	// does not collide with any other node's fixed default (7433).
	joinerCfg.TCP.Listen = ports.driver.Addr().String()
	joiner, err := NewService(joinerCfg, quietLogger())
	if err != nil {
		t.Fatalf("new joiner: %v", err)
	}
	joiner.driverListener = ports.driver
	joiner.internodeListener = ports.internode
	setCapacityProbes(t, joiner.core, harnessProbes)
	if err := startWithRetry(joiner, 3*time.Second); err != nil {
		t.Fatalf("start joiner: %v", err)
	}
	t.Cleanup(func() { _ = joiner.Stop() })

	// Bare address: the CLI form `join 127.0.0.1:4443`.
	respFrame := tc.frame(0, transport.OpClusterAdd, &ClusterAddRequest{Address: ja})
	if respFrame.Opcode == transport.OpError {
		t.Fatalf("cluster add with a bare address failed: %s", respFrame.Payload)
	}
	var addResp ClusterAddResponse
	if err := bson.Unmarshal(respFrame.Payload, &addResp); err != nil {
		t.Fatalf("decode add response: %v", err)
	}
	if !addResp.Success {
		t.Fatalf("cluster add not successful: %s", addResp.Error)
	}

	// The ring must contain the target's announced identity, not the
	// address string it was dialed by.
	oc0 := tc.nodes[0].server.core.cluster
	recorded := false
	for _, m := range oc0.memberNodes() {
		if m.Addr == ja {
			recorded = true
			if m.ID != "itest-join" {
				t.Fatalf("member recorded as %q, want %q (the target's announced NODE_ID)", m.ID, "itest-join")
			}
		}
	}
	if !recorded {
		t.Fatal("added member not found in the member set")
	}

	// The circle closes: the joiner can reconcile onto the epoch-2 set
	// that names itself — before the probe this failed with "node identity
	// X is not a member" and the join never converged.
	jc := joiner.core.cluster
	if err := jc.reconcileMembership(tc.addrs[0], oc0.memberNodes(), oc0.memberIDs(), oc0.Epoch()); err != nil {
		t.Fatalf("joiner resync after bare-address add: %v", err)
	}
	if jc.Epoch() != InitialEpoch+1 {
		t.Fatalf("joiner epoch = %d, want %d (it must recognise itself in the adopted set)", jc.Epoch(), InitialEpoch+1)
	}

	// Negative: an unreachable bare address fails with the explicit remedy.
	dead := fabricatedMemberAddr(t)
	deadFrame := tc.frame(0, transport.OpClusterAdd, &ClusterAddRequest{Address: dead})
	if deadFrame.Opcode != transport.OpError {
		t.Fatalf("add of unreachable %s answered %s; want a named error", dead, deadFrame.Opcode)
	}
	if !strings.Contains(string(deadFrame.Payload), "identity") {
		t.Fatalf("unreachable-add error %q does not explain the explicit identity form", deadFrame.Payload)
	}
}
