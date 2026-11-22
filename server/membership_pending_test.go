package server

import (
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// recordNode is one node's data directory and node store, from which a test
// builds the node's Cluster, and builds it again for a restart, without
// starting it.
type recordNode struct {
	dir  string
	kv   storage.KV
	cfg  *config.Config
	self string
}

// newRecordNode makes a data directory for a node seeded with itself and two
// fabricated members: a cluster of three at the first epoch.
func newRecordNode(t *testing.T) *recordNode {
	t.Helper()
	dir := t.TempDir()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	self := fabricatedMemberAddr(t)
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = self
	cfg.Server.NodeID = self
	cfg.Server.ReplicationFactor = 3
	cfg.HashRing.SeedNodes = []string{self, fabricatedMemberAddr(t), fabricatedMemberAddr(t)}
	return &recordNode{dir: dir, kv: openPositionTestStore(t, t.TempDir()), cfg: cfg, self: self}
}

// open builds the node's Cluster from what its directory and store hold; the
// test's end stops it.
func (node *recordNode) open(t *testing.T) *Cluster {
	t.Helper()
	s := &Server{config: node.cfg, kv: node.kv}
	c, err := NewCluster(s, node.cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	c.skipStopCheckpoint = true
	// The node has no log, so it runs no sender: an adoption here exercises
	// the record and the view, not replication.
	c.peersMu.Lock()
	for _, member := range c.members {
		c.peerStarted[member.ID] = true
	}
	c.peersMu.Unlock()
	t.Cleanup(c.Stop)
	return c
}

// setPending puts an acceptor record in memory, as the acceptor would.
func setPending(c *Cluster, pending *pendingChange) {
	c.membershipFileMu.Lock()
	c.pending = pending
	c.membershipFileMu.Unlock()
}

// pendingOf reads the acceptor record in memory.
func pendingOf(c *Cluster) *pendingChange {
	c.membershipFileMu.Lock()
	defer c.membershipFileMu.Unlock()
	return c.pending.clone()
}

// recordedPending reads the acceptor record membership.json holds.
func recordedPending(t *testing.T, c *Cluster) *pendingChange {
	t.Helper()
	record, found, err := readMembershipRecord(c.membershipPath())
	if err != nil || !found {
		t.Fatalf("reading membership.json: found=%v err=%v", found, err)
	}
	return record.Pending
}

// acceptanceOf is an acceptance for the epoch after c's of the set without
// the member at index leaving, at ballot (round, node).
func acceptanceOf(c *Cluster, round uint64, leaving int) *pendingChange {
	epoch, members := c.epochMembers()
	accepted := ballot{Round: round, Node: c.self.ID}
	kept := slices.Delete(slices.Clone(members), leaving, leaving+1)
	return &pendingChange{Epoch: epoch + 1, Promised: accepted, Accepted: &accepted, Members: kept, Change: members[leaving]}
}

// TestAPromiseSurvivesARestart: a node that promised ballot 5 still holds the
// promise after a restart, so it cannot promise below it. Guard: Pending in
// membershipRecordOf, and its restore in NewCluster.
func TestAPromiseSurvivesARestart(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	promise := &pendingChange{Epoch: c.Epoch() + 1, Promised: ballot{Round: 5, Node: "127.0.0.1:1"}}
	setPending(c, promise)
	if err := c.writeMembershipRecord(); err != nil {
		t.Fatalf("writing the record: %v", err)
	}
	c.Stop()

	restarted := node.open(t)
	got := pendingOf(restarted)
	if got == nil || got.Epoch != promise.Epoch || got.Promised != promise.Promised {
		t.Fatalf("after a restart the node holds %+v; it promised %+v before it", got, promise)
	}
	for _, round := range []uint64{4, 5} {
		resp, err := restarted.prepareMembershipChange(MembershipChangeRequest{FromNode: promise.Promised.Node, Phase: phasePrepare,
			Epoch: promise.Epoch, Ballot: ballot{Round: round, Node: promise.Promised.Node},
			BaseMembers: memberIDsOf(restarted.Members())})
		if err == nil || resp.Answer != string(answerOutpromised) {
			t.Errorf("after a restart a prepare at round %d, at or below the promise of round 5, was answered %q (%v)",
				round, resp.Answer, err)
		}
	}
}

// TestASealedPeerWriteKeepsThePromise: the sealed-peer write rewrites the
// whole record from memory, and must carry the promise. Guard: the pending
// change in writeMembershipRecord's write.
func TestASealedPeerWriteKeepsThePromise(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	promise := &pendingChange{Epoch: c.Epoch() + 1, Promised: ballot{Round: 3, Node: "127.0.0.1:1"}}
	setPending(c, promise)
	_, members := c.epochMembers()
	sealedPeer := members[1].ID
	if members[1].ID == c.self.ID {
		sealedPeer = members[2].ID
	}
	c.sealedPeers.record(sealedPeer, claimSealed)
	if err := c.persistSealedPeers(); err != nil {
		t.Fatalf("persisting the sealed peers: %v", err)
	}

	record, found, err := readMembershipRecord(c.membershipPath())
	if err != nil || !found {
		t.Fatalf("reading membership.json: found=%v err=%v", found, err)
	}
	if !slices.Contains(record.SealedPeers, sealedPeer) {
		t.Fatalf("test setup: the sealed-peer write did not record %s: %v", sealedPeer, record.SealedPeers)
	}
	if record.Pending == nil || record.Pending.Promised != promise.Promised {
		t.Errorf("the sealed-peer write left the record with the pending change %+v; the node had promised %+v",
			record.Pending, promise)
	}
}

// TestAnAdoptionClearsThePendingChange: adopting the epoch an acceptance was
// for drops the acceptance, on disk and in memory, in the one record write
// that moves the epoch. Guard: the clear in adoptMembershipLocked.
func TestAnAdoptionClearsThePendingChange(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	acceptance := acceptanceOf(c, 2, 2)
	setPending(c, acceptance)
	if err := c.writeMembershipRecord(); err != nil {
		t.Fatalf("writing the record: %v", err)
	}

	var saves atomic.Int32
	original := syncMembershipFile
	syncMembershipFile = func(file *os.File) error {
		saves.Add(1)
		return original(file)
	}
	t.Cleanup(func() { syncMembershipFile = original })
	if err := c.adoptMembership(acceptance.Epoch, acceptance.Members); err != nil {
		t.Fatalf("adopting epoch %d: %v", acceptance.Epoch, err)
	}
	syncMembershipFile = original

	if saves.Load() != 1 {
		t.Errorf("the adoption wrote membership.json %d times, want once", saves.Load())
	}
	if got := pendingOf(c); got != nil {
		t.Errorf("after adopting epoch %d the node still holds in memory %+v", acceptance.Epoch, got)
	}
	if got := recordedPending(t, c); got != nil {
		t.Errorf("after adopting epoch %d membership.json still holds %+v", acceptance.Epoch, got)
	}
}

// TestAMembershipFileWithoutPendingLoads: a record from before the field, or
// from a node that holds no acceptance, loads with none.
func TestAMembershipFileWithoutPendingLoads(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	if got := recordedPending(t, c); got != nil {
		t.Fatalf("test setup: a fresh record holds %+v", got)
	}
	c.Stop()
	if got := pendingOf(node.open(t)); got != nil {
		t.Errorf("a record without a pending change loaded with %+v", got)
	}
}

// TestAStalePendingIsNotRestored: an acceptance for an epoch other than the
// loaded one + 1 is no writer's of this build, and is dropped at start.
// Guard: the epoch check in restorePending.
func TestAStalePendingIsNotRestored(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	stale := acceptanceOf(c, 4, 2)
	stale.Epoch += 3
	setPending(c, stale)
	if err := c.writeMembershipRecord(); err != nil {
		t.Fatalf("writing the record: %v", err)
	}
	c.Stop()
	if got := pendingOf(node.open(t)); got != nil {
		t.Errorf("a record whose pending change is for epoch %d, at epoch %d, restored %+v", stale.Epoch, InitialEpoch, got)
	}
}

// TestARemovedNodeRestartsWithNoPendingChange: the node a removal takes out
// held an acceptance of it; its removal drops the acceptance, on disk and in
// memory, and keeps the rest of its record; and a removed node whose file
// still holds one restores none. Guards: the clear in markRemoved, and the
// removal check in restorePending.
func TestARemovedNodeRestartsWithNoPendingChange(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	epoch, members := c.epochMembers()
	selfIndex := slices.IndexFunc(members, func(member ring.Node) bool { return member.ID == c.self.ID })
	other := members[(selfIndex+1)%len(members)].ID
	acceptance := acceptanceOf(c, 2, selfIndex)
	setPending(c, acceptance)
	c.sealedPeers.record(other, claimSealed)
	joinedAt := map[string]uint64{other: InitialEpoch}
	c.joinedAt.Store(&joinedAt)
	c.settleMu.Lock()
	c.settledOf = map[string]uint64{other: InitialEpoch}
	c.settleMu.Unlock()
	if err := c.writeMembershipRecord(); err != nil {
		t.Fatalf("writing the record: %v", err)
	}

	if err := c.markRemoved(acceptance.Epoch, acceptance.Members); err != nil {
		t.Fatalf("marking the node removed at epoch %d: %v", acceptance.Epoch, err)
	}
	if got := pendingOf(c); got != nil {
		t.Errorf("after its removal the node still holds in memory %+v", got)
	}
	record, found, err := readMembershipRecord(c.membershipPath())
	if err != nil || !found {
		t.Fatalf("reading membership.json: found=%v err=%v", found, err)
	}
	if record.Pending != nil {
		t.Errorf("after its removal membership.json still holds %+v", record.Pending)
	}
	if record.Epoch != epoch || !slices.Contains(record.SealedPeers, other) || record.JoinedAt[other] != InitialEpoch ||
		record.SettledOf[other] != InitialEpoch {
		t.Errorf("the removal's rewrite changed more than the pending change: epoch %d (want %d), sealed %v, joined_at %v, settled_of %v",
			record.Epoch, epoch, record.SealedPeers, record.JoinedAt, record.SettledOf)
	}
	c.Stop()
	restarted := node.open(t)
	if got := pendingOf(restarted); got != nil {
		t.Errorf("the removed node restarted holding %+v", got)
	}
	if restarted.restoredAcceptance || len(restarted.recoveryArm) != 0 {
		t.Errorf("the removed node restarted with a recovery armed")
	}

	// A file that still holds an acceptance, as a crash between the removal
	// record and the rewrite leaves it.
	if err := saveMembershipRecord(membershipPathFor(node.dir), membershipRecordOf(membershipState{
		epoch: epoch, members: members, pending: acceptance,
	})); err != nil {
		t.Fatalf("writing a record that holds an acceptance: %v", err)
	}
	if got := pendingOf(node.open(t)); got != nil {
		t.Errorf("a removed node whose membership.json holds an acceptance restored %+v", got)
	}
}

// TestAnAcceptanceKeepsTheSealedPeersAndJoinEpochs: the acceptor is a writer
// of membership.json, and its save keeps the sealed peers, joined_at and
// settled_of the record holds. Guard: the acceptor saves through
// writeMembershipRecordLocked.
func TestAnAcceptanceKeepsTheSealedPeersAndJoinEpochs(t *testing.T) {
	node := newRecordNode(t)
	c := node.open(t)
	_, members := c.epochMembers()
	var others []ring.Node
	for _, member := range members {
		if member.ID != c.self.ID {
			others = append(others, member)
		}
	}
	proposer, leaving := others[0], others[1]
	c.sealedPeers.record(proposer.ID, claimSealed)
	joinedAt := map[string]uint64{proposer.ID: InitialEpoch}
	c.joinedAt.Store(&joinedAt)
	c.settleMu.Lock()
	c.settledOf = map[string]uint64{proposer.ID: InitialEpoch}
	c.settleMu.Unlock()
	if err := c.writeMembershipRecord(); err != nil {
		t.Fatalf("writing the record: %v", err)
	}
	proposed := slices.DeleteFunc(slices.Clone(members), func(member ring.Node) bool { return member.ID == leaving.ID })
	if _, err := c.acceptMembershipChange(acceptRequestOf(proposer.ID, InitialEpoch+1, proposed,
		Peer{ID: leaving.ID, Addr: leaving.Addr})); err != nil {
		t.Fatalf("accepting the removal of %s: %v", leaving.ID, err)
	}
	c.Stop()

	record, found, err := readMembershipRecord(membershipPathFor(node.dir))
	if err != nil || !found {
		t.Fatalf("reading membership.json: found=%v err=%v", found, err)
	}
	if record.Pending == nil || record.Pending.Accepted == nil {
		t.Fatalf("the acceptance is not on disk: %+v", record.Pending)
	}
	if !slices.Contains(record.SealedPeers, proposer.ID) || record.JoinedAt[proposer.ID] != InitialEpoch ||
		record.SettledOf[proposer.ID] != InitialEpoch {
		t.Errorf("the acceptance's save dropped part of the record: sealed %v, joined_at %v, settled_of %v",
			record.SealedPeers, record.JoinedAt, record.SettledOf)
	}
}

// TestNoMembershipPhaseIsReadAsABucketRequest: an older build reads the
// routing keys of any frame (bucket, database, name, key, id, writeconcern,
// write_concern) and would route a membership change that carried one as a
// bucket request. No phase's request, fully populated, carries any. Guard:
// the request's BSON tags.
func TestNoMembershipPhaseIsReadAsABucketRequest(t *testing.T) {
	routing := []string{"bucket", "database", "name", "key", "id", "writeconcern", "write_concern"}
	change := &Peer{ID: "127.0.0.1:1", Addr: "127.0.0.1:1", Store: claimSealed}
	for _, phase := range []string{phasePrepare, phaseAccept, phaseCommit} {
		payload, err := bson.Marshal(MembershipChangeRequest{Token: "token", FromNode: "127.0.0.1:2", Phase: phase,
			Epoch: 2, Ballot: ballot{Round: 3, Node: "127.0.0.1:2"}, BaseMembers: []string{"127.0.0.1:1", "127.0.0.1:2"},
			Members: []Peer{*change, {ID: "127.0.0.1:2", Addr: "127.0.0.1:2"}}, Change: change})
		if err != nil {
			t.Fatalf("encoding the %s request: %v", phase, err)
		}
		meta := decodeFrameMeta(payload)
		if bucket, database := meta.scope(transport.OpMembershipChange); bucket != "" || database != "" {
			t.Errorf("a %s request is scoped to bucket %q in database %q", phase, bucket, database)
		}
		if level, err := meta.writeConcern(); level != 0 || err != nil {
			t.Errorf("a %s request names write concern %v (%v)", phase, level, err)
		}
		if len(meta.Key.Value) != 0 || len(meta.ID.Value) != 0 {
			t.Errorf("a %s request carries a key or an id", phase)
		}
		elements, err := bson.Raw(payload).Elements()
		if err != nil {
			t.Fatalf("reading the %s request's keys: %v", phase, err)
		}
		for _, element := range elements {
			if slices.Contains(routing, strings.ToLower(element.Key())) {
				t.Errorf("a %s request has the top-level key %q, which an older build routes by", phase, element.Key())
			}
		}
	}
}
