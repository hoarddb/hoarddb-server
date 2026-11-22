package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// waitForFeatureConvergence polls until every node has learned every other
// node's advertised feature set. Gossip is a background loop, so a test that
// edits a record before the exchange has happened would be editing nothing.
func waitForFeatureConvergence(t *testing.T, tc *testCluster) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		converged := true
		for _, node := range tc.nodes {
			c := node.server.core.cluster
			c.featuresMu.RLock()
			for _, member := range c.Members() {
				if member.ID != c.self.ID && !c.memberFeatures[member.ID][featureBucketRF] {
					converged = false
				}
			}
			c.featuresMu.RUnlock()
		}
		if converged {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("nodes did not learn each other's advertised features")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stopThirdNodeAndEditWhatIsKnown stops node 3 — only so the state holds
// still, since gossip would re-advertise its feature within seconds — and lets
// edit change what the live nodes believe about it.
func stopThirdNodeAndEditWhatIsKnown(t *testing.T, tc *testCluster, edit func(c *Cluster, stale string)) string {
	t.Helper()
	stale := tc.addrs[2]
	stopNode(t, tc, 2)
	for _, node := range tc.nodes {
		if node.server != nil {
			c := node.server.core.cluster
			c.featuresMu.Lock()
			edit(c, stale)
			c.featuresMu.Unlock()
		}
	}
	return stale
}

// TestPeerWithoutBucketRFFeatureRefusesTheCreate is T-12. "Additive is safe" is
// false for CreateBucketRequest.ReplicationFactor, exactly: bucket DDL is
// replicated as the original opcode and payload, which each replica decodes and
// dispatches. A member that predates the field drops it, creates the bucket, and
// places it from its own node factor — not a missing feature, but two nodes
// that disagree about which nodes hold a bucket.
//
// Ported from e89e479, re-aimed (review F-3): the check is made once, by the
// owner, before the create is logged, so "refused" is "no catalog entry, no
// attached bucket on any node, and nothing in any log". A member that has never
// been heard from is not evidence that it lacks the feature.
func TestPeerWithoutBucketRFFeatureRefusesTheCreate(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	wide := bucketOwnedBy(t, tc, 0, "wide")

	// Node 3 answered gossip and advertised nothing: what a build that predates
	// the field looks like.
	stale := stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) {
		c.memberFeatures[stale] = map[string]bool{}
	})

	refusal := createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: wide, Type: BucketTypeHash, ReplicationFactor: 3,
	}))
	if refusal == "" {
		t.Fatalf("a create carrying replication_factor 3 was accepted while %s cannot honour it", stale)
	}
	if !strings.Contains(refusal, stale) {
		t.Errorf("the refusal does not name the member that cannot honour the field: %q", refusal)
	}
	for index, node := range tc.nodes {
		if node.server == nil {
			continue
		}
		if _, catalogued, _ := node.server.core.readCatalogEntry(testDatabase, wide); catalogued || node.server.core.bucketStoreOpen(testDatabase, wide) {
			t.Errorf("node %d holds a bucket whose create was refused", index+1)
		}
		if got := walCreates(t, node.server.core.wal, wide); got != 0 {
			t.Errorf("node %d logged %d creates of a bucket whose create was refused", index+1, got)
		}
	}

	// A create that declares no factor is unaffected: every node computes the
	// same placement from the same default, and an older member has nothing to
	// drop.
	plain := bucketOwnedBy(t, tc, 0, "plain")
	if refusal := createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: plain, Type: BucketTypeHash,
	})); refusal != "" {
		t.Fatalf("a create that declares no factor was refused: %s", refusal)
	}
}

// TestAMemberNotYetHeardFromDoesNotRefuseAFactor is review F-3(c): unknown is not
// unsupported. After a restart every member this node has not yet exchanged
// gossip with — including one that is down — would otherwise make it refuse
// factor DDL with a message telling the operator to upgrade a member that may
// run this build.
func TestAMemberNotYetHeardFromDoesNotRefuseAFactor(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	wide := bucketOwnedBy(t, tc, 0, "wide")
	stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) { delete(c.memberFeatures, stale) })

	refusal := createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: wide, Type: BucketTypeHash, ReplicationFactor: 3,
	}))
	if refusal != "" {
		t.Fatalf("a create was refused because a member had not been heard from: %s", refusal)
	}
}

// TestBucketRFCapabilityIsAdvertisedAtAuth pins the client-facing half of the
// same exchange. A driver that wants to set a factor asserts the capability and
// learns at AUTH whether this server honours it, rather than sending a field
// that is silently dropped. Ported from e89e479 as is.
func TestBucketRFCapabilityIsAdvertisedAtAuth(t *testing.T) {
	if !containsString(serverCapabilities(), featureBucketRF) {
		t.Errorf("the server's AUTH capabilities do not advertise %q: %v", featureBucketRF, serverCapabilities())
	}
}
