package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// replicaIDs is the ids of the replica set of database/bucket on r.
func replicaIDs(t *testing.T, r *ring.Ring, rf int, bucket string) []string {
	t.Helper()
	replicas, err := r.Replicas(placementOf(testDatabase, bucket).encoded, rf)
	if err != nil {
		t.Fatalf("placing %s: %v", bucket, err)
	}
	ids := make([]string, 0, len(replicas))
	for _, node := range replicas {
		ids = append(ids, node.ID)
	}
	return ids
}

// TestAHolderOutsideTheSetKeepsItsCopyWhenTheOwnerCreatedAnEmptyOne is arm (a)
// of the create-beside-copy case (replication dispatch 10a.7; arm (b) is 10b.7's). A bucket
// is held before a fourth node joins and owns it. The joiner, which knows the
// database, is asked to create the bucket while no holder can answer it — the
// case the earlier catalog question passed over. The name's replica set changed since
// the epoch every member settled at, so the create is refused with retry,
// naming the unheard members, and nothing is created. Once the holders answer,
// the joiner's pull lands and the document written before the join is read at
// the owner; no holder released its copy before that.
//
// Two arms: three members at factor 2, where both old holders end outside the
// new set; and factor = N (two members at factor 3 and the joiner), where
// nothing is released and the danger is the owner serving an empty created
// copy (the bug's criterion 2). The join freeze, which refuses the create
// first, is switched off. Guard: pass over unheard members whatever the set
// did — the joiner then creates an empty copy beside the holders.
func TestAHolderOutsideTheSetKeepsItsCopyWhenTheOwnerCreatedAnEmptyOne(t *testing.T) {
	for _, arm := range []struct {
		name           string
		members, rf    int
		holdersOutside bool
	}{{"both holders leave the set", 3, 2, true}, {"factor equals the node count", 2, 3, false}} {
		t.Run(arm.name, func(t *testing.T) {
			switchJoinFreezeOff(t)
			createdBesideHolders(t, arm.members, arm.rf, arm.holdersOutside)
		})
	}
	// Arm (b), step 10b.7: the empty created copy does land (the owner's
	// question passed over), and the holders keep theirs — outside the new
	// set, and inside it at factor = N, where each holder's hand-off asks the
	// other to migrate.
	for _, arm := range []struct {
		name           string
		members, rf    int
		holdersOutside bool
	}{{"the created copy lands, both holders outside the set", 3, 2, true}, {"the created copy lands, factor equals the node count", 2, 3, false}} {
		t.Run(arm.name, func(t *testing.T) {
			switchJoinFreezeOff(t)
			createdOverHolders(t, arm.members, arm.rf, arm.holdersOutside)
		})
	}
}

// createdOverHolders is arm (b): the joiner creates an empty copy, another
// incarnation, beside the holders' copies. A holder outside the new set asks
// every member what its copy is and finds none that covers its own — the
// members hold another incarnation — so it keeps its copy and reports the
// divergence. At factor = N the holders stay in the set, and a holder's
// hand-off asks the other to migrate: a peer's request never re-seeds a copy
// of another incarnation (only an operator's migrate does), so neither is
// replaced by the owner's empty copy. The document written before the join
// stays on the holders. Guards: release without the cover test — a holder
// outside the set deletes the only copy of the document; re-seed on a peer's
// request — the holders in the set take the empty copy.
func createdOverHolders(t *testing.T, memberCount, rf int, holdersOutside bool) {
	h := setUpBesideHolders(t, memberCount, rf, holdersOutside)
	ownerQuestionPassOverHook.Store(true)
	t.Cleanup(func() { ownerQuestionPassOverHook.Store(false) })
	created := h.tc.frameOn(h.joinerNode, transport.OpCreateBucket, bson.M{
		"database": testDatabase, "name": h.bucket, "type": string(BucketTypeHash), "write_concern": "one",
	})
	if created.Opcode == transport.OpError {
		t.Fatalf("setup: the create at the joiner with the owner's question passed over was refused: %s", created.Payload)
	}
	if !h.joiner.s.bucketStoreOpen(testDatabase, h.bucket) || holdsKeyLocally(h.joinerNode, h.bucket, "held") {
		t.Fatalf("setup: the joiner does not hold an empty created copy of %s", h.bucket)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var holders []*testNode
	for _, id := range replicaIDs(t, h.before, h.rf, h.bucket) {
		holders = append(holders, h.tc.nodes[nodeIndexByAddr(t, h.tc.addrs, id)])
	}
	// Each holder hands the bucket off as its sweep would, for as long as a
	// release would take: a release, if any, happens on the first rounds.
	for round := 0; round < 5; round++ {
		for _, holder := range holders {
			c := holder.server.core.cluster
			replicas, err := c.Replicas(placementOf(testDatabase, h.bucket))
			if err != nil {
				t.Fatalf("placing %s on %s: %v", h.bucket, c.self.ID, err)
			}
			if err := c.handOffBucket(ctx, testDatabase, h.bucket, replicas); err != nil && round == 0 {
				t.Logf("%s's hand-off of %s: %v", c.self.ID, h.bucket, err)
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, holder := range holders {
		if !holdsKeyLocally(holder, h.bucket, "held") {
			t.Errorf("holder %s no longer holds the document written before the join: its copy was released, or replaced by a member's copy of another incarnation",
				holder.server.core.cluster.self.ID)
		}
	}
	reported := holdsWithin(10*time.Second, func() bool {
		for _, holder := range holders {
			if holder.server.core.cluster.divergencesSeen.Load() > 0 {
				return true
			}
		}
		return false
	})
	if !reported {
		t.Error("no holder reported that a member holds another incarnation of the name")
	}
}

// besideHolders is the create-beside-copy layout: a bucket held by its first
// set, a joiner that owns it, knows the database and holds nothing of it, and
// the founders' streams to the joiner held.
type besideHolders struct {
	tc         *testCluster
	rf         int
	bucket     string
	before     *ring.Ring
	after      *ring.Ring
	joinerNode *testNode
	joiner     *Cluster
}

func setUpBesideHolders(t *testing.T, memberCount, rf int, holdersOutside bool) *besideHolders {
	t.Helper()
	tc := newTestClusterWith(t, memberCount, rf, func(_ int, cfg *config.Config) { noSweepNoRetention(cfg) })
	t.Cleanup(tc.stop)
	members := tc.nodes[0].server.core.cluster.Members()
	before := ringOf(t, members, rf)
	// Some joiner addresses admit no name whose new set leaves out every old
	// holder: the random ports are drawn again until one does.
	var (
		ports    *testNodePorts
		after    *ring.Ring
		joinerID string
		bucket   string
	)
	for attempt := 0; attempt < 20 && bucket == ""; attempt++ {
		ports = reserveNodePorts(t)
		joined := append(append([]ring.Node{}, members...), ports.ringNode())
		after, joinerID = ringOf(t, joined, rf), joined[len(joined)-1].ID
		for i := 0; i < 2000 && bucket == ""; i++ {
			name := fmt.Sprintf("released%03d", i)
			old, placed := replicaIDs(t, before, rf, name), replicaIDs(t, after, rf, name)
			outside := !slices.ContainsFunc(old, func(id string) bool { return slices.Contains(placed, id) })
			if placed[0] == joinerID && outside == holdersOutside {
				bucket = name
			}
		}
	}
	if bucket == "" {
		t.Fatal("setup: no joiner address of twenty admits a bucket name that satisfies the test's placement")
	}
	// A bucket the joiner owns and pulls, so it learns the database.
	learned := bucketNamed(t, "learned", func(name string) bool { return ownerOn(after, name) == joinerID && name != bucket })

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, name := range []string{bucket, learned} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, name, min(rf, memberCount))
	}
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "majority"})

	proposer := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	for _, node := range tc.nodes {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(ports.ringNode())))
	}
	joinerNode := startJoinTarget(t, tc, ports, tc.token, noSweepNoRetention)
	t.Cleanup(func() { _ = joinerNode.client.Close() })
	joiner := joinerNode.server.core.cluster
	adoptFrom(t, joiner, proposer)
	if err := joiner.ensureMigrated(ctx, testDatabase, learned); err != nil {
		t.Fatalf("setup: the joiner's pull of %s: %v", learned, err)
	}
	return &besideHolders{tc: tc, rf: rf, bucket: bucket, before: before, after: after, joinerNode: joinerNode, joiner: joiner}
}

func createdBesideHolders(t *testing.T, memberCount, rf int, holdersOutside bool) {
	h := setUpBesideHolders(t, memberCount, rf, holdersOutside)
	tc, bucket, before, joinerNode, joiner := h.tc, h.bucket, h.before, h.joinerNode, h.joiner

	// No holder can answer the joiner's question while it is asked to create
	// the bucket.
	for _, node := range tc.nodes {
		node.server.core.cluster.mismatch.Store(true)
	}
	created := tc.frameOn(joinerNode, transport.OpCreateBucket, bson.M{
		"database": testDatabase, "name": bucket, "type": string(BucketTypeHash), "write_concern": "one",
	})
	for _, node := range tc.nodes {
		node.server.core.cluster.mismatch.Store(false)
	}
	if created.Opcode != transport.OpError {
		t.Fatalf("the create at the joiner, which owns %s and holds no copy, was answered %s while no holder could answer: "+
			"an empty copy now stands beside the holders'", bucket, created.Opcode)
	}
	refused := refusalOf(t, created)
	if refused.Code != int(transport.ErrUnavailable) || refused.WriteOutcome != outcomeNotApplied {
		t.Errorf("the create was refused with code %d, outcome %q; want UNAVAILABLE, not_applied: %s", refused.Code, refused.WriteOutcome, refused.Message)
	}
	for _, node := range tc.nodes {
		if id := node.server.core.cluster.self.ID; !strings.Contains(refused.Message, id) {
			t.Errorf("the refusal does not name the unheard member %s: %s", id, refused.Message)
		}
	}
	if joiner.s.bucketStoreOpen(testDatabase, bucket) {
		t.Fatal("the joiner holds a copy of the bucket after the refused create")
	}
	for _, id := range replicaIDs(t, before, rf, bucket) {
		if !holdsKeyLocally(tc.nodes[nodeIndexByAddr(t, tc.addrs, id)], bucket, "held") {
			t.Fatalf("holder %s released its copy before the owner's pull landed", id)
		}
	}

	// The holders answer again: the owner's pull lands, and the document is
	// read at the owner.
	pullNow(t, joiner, testDatabase, bucket)
	if !holdsKeyLocally(joinerNode, bucket, "held") {
		t.Errorf("the owner's copy of %s lacks the document written before the join", bucket)
	}
}
