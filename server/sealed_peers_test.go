package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// legacyPeer and legacyMembershipFile are the two records as a binary from
// before the sealed flag declares them: what an old node decodes a new node's
// bytes into.
type legacyPeer struct {
	ID   string `bson:"id"`
	Addr string `bson:"addr,omitempty"`
}

type legacyMembershipFile struct {
	Epoch   uint64      `json:"epoch"`
	Members []ring.Node `json:"members"`
}

// TestPeerRecordStoreClaimIsAdditiveOnTheWire: a binary from before the claim
// decodes a new node's record, a record that binary wrote reads as saying
// nothing — not as plaintext — and a plaintext node of this version says so,
// so the two are told apart.
func TestPeerRecordStoreClaimIsAdditiveOnTheWire(t *testing.T) {
	for _, claim := range []storeClaim{claimSealed, claimPlaintext} {
		encoded, err := bson.Marshal(Peer{ID: "n1", Addr: "127.0.0.1:4433", Store: claim})
		if err != nil {
			t.Fatalf("encoding a %s node's record: %v", claim, err)
		}
		said, err := bson.Raw(encoded).LookupErr("store")
		if err != nil || said.StringValue() != string(claim) {
			t.Errorf("a %s node's record says %v (%v); a node of this version always says what its store is", claim, said, err)
		}
		var old legacyPeer
		if err := bson.Unmarshal(encoded, &old); err != nil || old.ID != "n1" || old.Addr != "127.0.0.1:4433" {
			t.Errorf("a binary from before the claim cannot decode a %s node's record: %+v, %v", claim, old, err)
		}
	}

	before, err := bson.Marshal(legacyPeer{ID: "n1", Addr: "127.0.0.1:4433"})
	if err != nil {
		t.Fatalf("encoding the earlier record: %v", err)
	}
	var decoded Peer
	if err := bson.Unmarshal(before, &decoded); err != nil {
		t.Fatalf("decoding the earlier record: %v", err)
	}
	if decoded.Store != claimUnreported {
		t.Errorf("a record with no claim decoded as %q; absent must mean only that the node does not say", decoded.Store)
	}
}

// TestMembershipFileCarriesTheSealedPeers: the sealed set round-trips through
// membership.json, a file from before the field loads with none, and a binary
// from before the field still reads a file that has it.
func TestMembershipFileCarriesTheSealedPeers(t *testing.T) {
	path := filepath.Join(t.TempDir(), membershipFileName)
	members := []ring.Node{{ID: "a", Addr: "127.0.0.1:1"}, {ID: "b", Addr: "127.0.0.1:2"}}
	if err := saveMembershipRecord(path, membershipFile{Epoch: 3, Members: members, SealedPeers: []string{"b"}}); err != nil {
		t.Fatalf("saving the record: %v", err)
	}
	record, found, err := readMembershipRecord(path)
	if err != nil || !found {
		t.Fatalf("reading the record back: found=%v err=%v", found, err)
	}
	if record.Epoch != 3 || len(record.Members) != 2 || !slices.Equal(record.SealedPeers, []string{"b"}) {
		t.Errorf("the record did not round-trip: %+v", record)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the file: %v", err)
	}
	var old legacyMembershipFile
	if err := json.Unmarshal(raw, &old); err != nil || old.Epoch != 3 || len(old.Members) != 2 {
		t.Errorf("a pre-flag binary cannot read a membership file with sealed peers: %+v, %v", old, err)
	}

	if err := saveMembershipRecord(path, membershipFile{Epoch: 4, Members: members}); err != nil {
		t.Fatalf("saving a record with no sealed peers: %v", err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the file: %v", err)
	}
	if strings.Contains(string(raw), "sealed_peers") {
		t.Errorf("a record with no sealed peers writes the key, changing the file a plaintext cluster keeps: %s", raw)
	}
	record, _, err = readMembershipRecord(path)
	if err != nil || len(record.SealedPeers) != 0 {
		t.Errorf("a file with no sealed peers loaded some: %+v, %v", record, err)
	}
}

// TestNodeSpeaksOnlyForItselfAboutSealing: a node sets the claim on its own
// record and on no other member's, and a receiver takes a node's word about
// that node only — a claim relayed about a third node, in a request or in a
// response, is not recorded. A sender that says nothing runs an earlier
// version and is recorded as saying nothing.
func TestNodeSpeaksOnlyForItselfAboutSealing(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "", "")
	defer tc.stop()
	sealed, plaintext := tc.nodes[0].server.core.cluster, tc.nodes[1].server.core.cluster

	if sealed.selfPeer().Store != claimSealed || plaintext.selfPeer().Store != claimPlaintext {
		t.Fatalf("selfPeer: sealed node says %q, plaintext node says %q", sealed.selfPeer().Store, plaintext.selfPeer().Store)
	}
	for _, listed := range sealed.memberNodes() {
		if (listed.Store != claimUnreported) != (listed.ID == tc.addrs[0]) {
			t.Errorf("the sealed node's member list says %q for %s; only its own record may carry a claim", listed.Store, listed.ID)
		}
	}

	member, third := mustNode(t, sealed, tc.addrs[1]), mustNode(t, sealed, tc.addrs[2])
	sealed.sealedPeers.record(member.ID, claimSealed)
	announced := sealed.learnStoreFromGossip(GossipRequest{
		NodeID: member.ID, Addr: member.Addr,
		Nodes: []Peer{{ID: member.ID, Addr: member.Addr}, {ID: "vouched-for", Addr: "127.0.0.1:10", Store: claimSealed}},
	})
	if announced != claimUnreported {
		t.Errorf("a sender whose own record carries no claim was read as %q", announced)
	}
	if known := sealed.sealedPeers.knowledge(member.ID); known.claim != claimUnreported || !known.answered {
		t.Errorf("a member that said nothing: %+v, want answered and unreported", known)
	}
	if known := sealed.sealedPeers.knowledge("vouched-for"); known.answered {
		t.Errorf("a node another node vouched for in a request was recorded: %+v", known)
	}
	sealed.learnStoreFromGossip(GossipRequest{NodeID: "stranger", Nodes: []Peer{{ID: "stranger", Store: claimSealed}}})
	if known := sealed.sealedPeers.knowledge("stranger"); known.answered {
		t.Errorf("a sender outside the member set was recorded; the table must hold members only")
	}

	sealed.sealedPeers.record(third.ID, claimPlaintext)
	sealed.learnStoreFromGossipResponse(member, GossipResponse{
		Success: true,
		Self:    Peer{ID: member.ID, Addr: member.Addr, Store: claimPlaintext},
		Nodes:   []Peer{{ID: member.ID, Addr: member.Addr}, {ID: third.ID, Addr: third.Addr, Store: claimSealed}},
	})
	if known := sealed.sealedPeers.knowledge(third.ID); known.sealed() {
		t.Errorf("a claim one node relayed about another in a gossip response was recorded")
	}
	if known := sealed.sealedPeers.knowledge(member.ID); known.claim != claimPlaintext {
		t.Errorf("the responder's own claim was not recorded: %+v", known)
	}

	refusal := sealed.refuseKnownPlaintextPeer(member, "replicate PUT for bucket \"b\"")
	var plaintextPeer *plaintextPeerError
	if !errors.As(refusal, &plaintextPeer) {
		t.Fatalf("a plaintext peer was not refused: %v", refusal)
	}
	if err := plaintext.refuseKnownPlaintextPeer(mustNode(t, plaintext, tc.addrs[0]), "replicate"); err != nil {
		t.Errorf("a plaintext node refused a peer: %v", err)
	}
}

// TestSealedNodeRemembersItsSealedPeersAcrossARestart: what a node learnt is
// in membership.json, and a restart reads it back — with the peer down, so
// nothing but the file could have told it.
func TestSealedNodeRemembersItsSealedPeersAcrossARestart(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "on")
	defer tc.stop()
	first := tc.nodes[0].server.core.cluster
	deadline := time.Now().Add(15 * time.Second)
	for !first.sealedPeers.knowledge(tc.addrs[1]).sealed() {
		if time.Now().After(deadline) {
			t.Fatalf("node 1 never learnt that node 2 is sealed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	record, found, err := readMembershipRecord(first.membershipPath())
	if err != nil || !found {
		t.Fatalf("reading node 1's membership record: found=%v err=%v", found, err)
	}
	if !slices.Equal(record.SealedPeers, []string{tc.addrs[1]}) {
		t.Fatalf("node 1's membership record names %v as sealed, want [%s]", record.SealedPeers, tc.addrs[1])
	}

	_ = tc.nodes[1].client.Close()
	tc.nodes[1].client = nil
	if err := tc.nodes[1].server.Stop(); err != nil {
		t.Fatalf("stopping node 2: %v", err)
	}
	tc.nodes[1].server = nil

	restarted := tc.restartNode(0).server.core.cluster
	if known := restarted.sealedPeers.knowledge(tc.addrs[1]); !known.sealed() {
		t.Errorf("after a restart with node 2 down, node 1 knows it as %+v; the record on disk says sealed", known)
	}
}

// TestOwnerDoesNotQueueForAPeerItLearntIsPlaintextMidWrite kills the mutant
// that removes the second check, at the enqueue. The ship list was fixed
// before the local apply; the check before it passed, and between the two the
// owner learns its replica is plaintext. The write must come back refused,
// naming the replica, rather than queued for it.
func TestOwnerDoesNotQueueForAPeerItLearntIsPlaintextMidWrite(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketPlaced(t, owner, "midwrite", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	learnt := func(_ string, entry ReplEntry) {
		if transport.Opcode(entry.Op) == transport.OpPut && entry.Bucket == bucket {
			owner.sealedPeers.record(tc.addrs[1], claimPlaintext)
		}
	}
	beforeLocalApplyHook.Store(&learnt)
	written := tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "one"})
	beforeLocalApplyHook.Store(nil)
	owner.sealedPeers.record(tc.addrs[1], claimSealed)

	if written.Opcode != transport.OpError {
		t.Fatalf("the owner reported the write done although it had learnt its replica %s is plaintext before queueing it", tc.addrs[1])
	}
	assertPlaintextPeerRefusal(t, "put queued after the replica turned plaintext", string(written.Payload), tc.addrs[1])
	// The entry is held in the owner's log, not dropped: the replica, sealed
	// again above, receives it by replay (BUG-0313). That is pinned by
	// TestARefusedReplicaIsNotPassedByALaterWrite; this test no longer
	// requires it never to arrive.
}

// refusedMidWrite is the setup of TestOwnerDoesNotQueueForAPeerItLearntIsPlaintextMidWrite
// kept open for what follows it (BUG-0313): a sealed two-node cluster whose
// owner learns, after a put's ship list is fixed and before it is queued, that
// its replica is plaintext, so the put is applied on the owner and refused to
// the replica. The owner still believes the replica plaintext when it returns.
type refusedMidWrite struct {
	tc        *testCluster
	owner     *Cluster
	bucket    string
	refused   uint64
	toReplica *peer
}

func newRefusedMidWrite(t *testing.T) refusedMidWrite {
	t.Helper()
	tc := newMixedCluster(t, 2, "on", "on")
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner := tc.nodes[0].server.core.cluster
	bucket := bucketPlaced(t, owner, "refusedstall", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "before", Data: markerDocument(), WriteConcern: "all"})
	toReplica := streamTo(t, tc, 0, 1)
	if !waitCursorAtLeast(toReplica, owner.s.wal.LastSequence(), 20*time.Second) {
		t.Fatalf("setup: the replica never acknowledged the writes before the refused one")
	}

	learnt := func(nodeID string, entry ReplEntry) {
		if nodeID == owner.self.ID && transport.Opcode(entry.Op) == transport.OpPut && entry.Bucket == bucket {
			owner.sealedPeers.record(tc.addrs[1], claimPlaintext)
		}
	}
	beforeLocalApplyHook.Store(&learnt)
	written := tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "refused", Data: markerDocument(), WriteConcern: "all"})
	beforeLocalApplyHook.Store(nil)
	refused := owner.s.wal.LastSequence()

	if written.Opcode != transport.OpError {
		t.Fatalf("setup: the owner reported the write done although it had learnt its replica %s is plaintext before queueing it", tc.addrs[1])
	}
	assertPlaintextPeerRefusal(t, "put refused to a replica learnt plaintext", string(written.Payload), tc.addrs[1])
	// The client is told how many copies hold the write: the owner's alone.
	var answer ErrorResponse
	if err := bson.Unmarshal(written.Payload, &answer); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if answer.WriteOutcome != outcomeApplied || answer.CopiesConfirmed != 1 || answer.ReplicationFactor != 2 {
		t.Errorf("the refused write is answered write_outcome=%q copies_confirmed=%d replication_factor=%d; "+
			"only the owner holds it, so want applied, 1 of 2", answer.WriteOutcome, answer.CopiesConfirmed, answer.ReplicationFactor)
	}
	if locallyFound(t, tc.nodes[1], bucket, "refused") {
		t.Fatalf("setup: the refused entry reached the replica")
	}
	return refusedMidWrite{tc: tc, owner: owner, bucket: bucket, refused: refused, toReplica: toReplica}
}

// assertNeverPassedUndelivered fails when the owner's cursor for the replica
// is at or past the refused entry while the replica lacks it. The cursor is
// read first: a replica applies an entry before it acknowledges it.
func (r refusedMidWrite) assertNeverPassedUndelivered(t *testing.T, when string) {
	t.Helper()
	cursor := r.toReplica.ackedThrough(r.owner.self.ID)
	if cursor >= r.refused && !locallyFound(t, r.tc.nodes[1], r.bucket, "refused") {
		t.Fatalf("%s: the owner's cursor for %s is %d, past the refused entry %d, and the replica does not hold it: "+
			"nothing will ever send it", when, r.tc.addrs[1], cursor, r.refused)
	}
}

// awaitDeliveredAfterReseal records the replica sealed again, as a peer that
// was upgraded or re-encrypted says it is, and requires the refused entry to
// reach it.
func (r refusedMidWrite) awaitDeliveredAfterReseal(t *testing.T) {
	t.Helper()
	r.owner.sealedPeers.record(r.tc.addrs[1], claimSealed)
	r.owner.wakeSenderOf(r.tc.addrs[1])
	deadline := time.Now().Add(20 * time.Second)
	for !locallyFound(t, r.tc.nodes[1], r.bucket, "refused") {
		r.assertNeverPassedUndelivered(t, "after the re-seal")
		if time.Now().After(deadline) {
			t.Fatalf("the replica %s, sealed again, never received the entry %d the owner refused it: the owner's cursor for it is %d",
				r.tc.addrs[1], r.refused, r.toReplica.ackedThrough(r.owner.self.ID))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestARefusedReplicaIsNotPassedByALaterWrite is BUG-0313's first route: a
// replica refused an entry for being plaintext is sealed again, and the next
// all-concern write to the bucket is acknowledged by it. That acknowledgement
// must not carry the owner's cursor past the refused entry; the replica gets
// the refused entry, and owner and replica hold the same keys.
func TestARefusedReplicaIsNotPassedByALaterWrite(t *testing.T) {
	r := newRefusedMidWrite(t)
	defer r.tc.stop()

	r.owner.sealedPeers.record(r.tc.addrs[1], claimSealed)
	after := r.tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: r.bucket, Key: "after", Data: markerDocument(), WriteConcern: "all"})
	if after.Opcode == transport.OpError {
		t.Fatalf("the write after the re-seal: %s", printable(string(after.Payload)))
	}
	deadline := time.Now().Add(20 * time.Second)
	for !locallyFound(t, r.tc.nodes[1], r.bucket, "after") {
		if time.Now().After(deadline) {
			t.Fatalf("setup: the write after the re-seal never reached the replica")
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.assertNeverPassedUndelivered(t, "after the next all-concern write")
	r.awaitDeliveredAfterReseal(t)
}

// TestARetentionPassDoesNotAdvanceAReplicaPastARefusedEntry is BUG-0313's
// second route: while the replica is still refused, a retention pass asks
// every idle stream for an advance notice. The notice must not carry the
// owner's cursor past the refused entry; once the replica is sealed again it
// gets the entry.
func TestARetentionPassDoesNotAdvanceAReplicaPastARefusedEntry(t *testing.T) {
	r := newRefusedMidWrite(t)
	defer r.tc.stop()

	if err := r.owner.pruneWAL(); err != nil {
		t.Fatalf("retention pass: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.assertNeverPassedUndelivered(t, "after a retention pass while the replica is refused")
		time.Sleep(50 * time.Millisecond)
	}
	r.awaitDeliveredAfterReseal(t)
}

// TestAMemberAddedDuringAWriteIsNotShippedAPlaintextEntry pins the shape the
// comment on refuseShipToPlaintextReplica describes: the owner's ship list is
// the delivery set computed before the local apply (addressInFence), so a
// plaintext member that joins while the write is in flight is not in that list
// and the entry is not shipped to it. The member is reached only by catch-up,
// whose own guard refuses the sealed history rather than replaying it
// (refuseReplayToPlaintextPeer). It is a characterization on the unfixed tree:
// it passes without any re-read of the set.
func TestAMemberAddedDuringAWriteIsNotShippedAPlaintextEntry(t *testing.T) {
	tc := newMixedCluster(t, 2, "on", "on")
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	scenario := findSeededReplicaScenario(t, tc, newcomer, "addedmember")
	owner := tc.nodes[scenario.ownerIndex]
	ownerCluster := owner.server.core.cluster

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(scenario.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: scenario.bucket, Type: BucketTypeHash})
	tc.ok(scenario.ownerIndex, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: scenario.bucket, Key: "before", Data: markerDocument(), WriteConcern: "all",
	})

	// Hold the next write to the bucket after addressInFence has fixed its
	// ship list and before it is logged: the window a membership change has
	// to land in.
	reached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWrite()
	held := false
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if held || nodeID != ownerCluster.self.ID || op != transport.OpPut || bucket != scenario.bucket {
			return
		}
		held = true
		close(reached)
		<-release
	}
	afterOwnerViewHook.Store(&hold)
	defer afterOwnerViewHook.Store(nil)

	payload, err := bson.Marshal(InsertRequest{
		Database: testDatabase, Bucket: scenario.bucket, Key: "in-flight", Data: markerDocument(), WriteConcern: "all",
	})
	if err != nil {
		t.Fatalf("encoding the in-flight write: %v", err)
	}
	answered := make(chan *transport.Frame, 1)
	client := tc.dial(scenario.ownerIndex)
	defer func() { _ = client.Close() }()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		frame, err := client.SendRequest(ctx, transport.OpPut, payload)
		if err != nil {
			t.Errorf("sending the in-flight write: %v", err)
			answered <- nil
			return
		}
		answered <- frame
	}()

	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("setup: the write never reached its owner after the ship list was read")
	}

	joiner := startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) { cfg.Server.AutoRebalance = false })
	joinAndResync(t, tc, 0, joiner, newcomer)
	releaseWrite()

	select {
	case frame := <-answered:
		if frame == nil {
			t.Fatal("the in-flight write was never answered")
		}
		if frame.Opcode == transport.OpError {
			// The join can leave the second copy still resyncing, so the
			// owner may answer TIMEOUT with write_outcome applied: the write
			// is stored, not refused (spec §20.12.3). Every other refusal is
			// a real failure.
			refusal := refusalOf(t, frame)
			if transport.ErrorCode(refusal.Code) != transport.ErrTimeout || refusal.WriteOutcome != outcomeApplied {
				t.Fatalf("the in-flight write was refused after the member was added: %s", printable(string(frame.Payload)))
			}
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the in-flight write was never answered")
	}

	// The member joined after the ship list was read, so the entry cannot be
	// in it: nothing of the sealed bucket reached the plaintext member.
	if locallyFound(t, joiner, scenario.bucket, "in-flight") {
		t.Errorf("the in-flight write reached the plaintext member %s added after the ship list was read", newcomer.ID)
	}
	assertHoldsNothing(t, joiner, scenario.bucket)

	// A member added mid-write is reached by catch-up, whose guard refuses the
	// sealed history rather than replaying it.
	if ownerCluster.refuseReplayToPlaintextPeer(newcomer, 1) == nil {
		t.Errorf("catch-up would replay a sealed entry to the plaintext member %s added mid-write", newcomer.ID)
	}
}

// TestSealedSourceRefusesAPullItCannotAttribute kills the mutants that serve a
// pull by a node that is not a member, or by one that names no one, and checks
// each refusal says which case it is. A refused pull takes no snapshot.
func TestSealedSourceRefusesAPullItCannotAttribute(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "on")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	source := tc.nodes[0].server.core
	bucket := bucketPlaced(t, source.cluster, "attributed", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})

	start := func(req MigrateStartRequest) MigrateStartResponse {
		t.Helper()
		req.Token, req.Database, req.Bucket = tc.token, testDatabase, bucket
		payload, err := bson.Marshal(req)
		if err != nil {
			t.Fatalf("encoding the migrate start: %v", err)
		}
		frame, err := handleMigrateStart(context.Background(), transport.NewFrame(transport.OpMigrateStart, payload), source)
		if err != nil {
			t.Fatalf("migrate start: %v", err)
		}
		var resp MigrateStartResponse
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			t.Fatalf("decoding the migrate start response: %v", err)
		}
		return resp
	}
	for _, refused := range []struct {
		name string
		req  MigrateStartRequest
		want string
	}{
		{name: "names no one", req: MigrateStartRequest{}, want: "did not say who it is"},
		{name: "not a member", req: MigrateStartRequest{FromNode: "stranger", Store: claimSealed}, want: "not a member"},
		{name: "a member of an earlier version", req: MigrateStartRequest{FromNode: tc.addrs[1]}, want: "does not report"},
		{name: "a plaintext member", req: MigrateStartRequest{FromNode: tc.addrs[1], Store: claimPlaintext}, want: "is not encrypted"},
	} {
		t.Run(refused.name, func(t *testing.T) {
			resp := start(refused.req)
			if resp.Ok {
				t.Fatalf("a sealed source served a pull by a node that %s", refused.name)
			}
			t.Logf("refused with: %s", resp.Error)
			if !strings.Contains(resp.Error, refused.want) {
				t.Errorf("the refusal does not say %q", refused.want)
			}
			if source.cluster.pendingCount(migrateKey(testDatabase, bucket)) != 0 {
				t.Errorf("a refused pull left a snapshot prepared")
			}
		})
	}
	// The puller names the incarnation it recorded for the source (A-4.2).
	opened := openedStream(t, tc.nodes[1].server.core.cluster, tc.addrs[0]).Incarnation
	if resp := start(MigrateStartRequest{FromNode: tc.addrs[1], Store: claimSealed, Incarnation: opened, Epoch: source.cluster.Epoch()}); !resp.Ok {
		t.Errorf("a sealed member's pull was refused: %s", resp.Error)
	}
	clearAllPending(source.cluster, migrateKey(testDatabase, bucket))
}

// TestRecreatingAnExistingBucketAtALargerFactorSaysItExists is BUG-0206: on a
// sealed cluster with a plaintext member, a create of a bucket the owner
// already serves, at a larger factor, was refused as "would replicate to a
// plaintext peer" -- the guard asked about the declared factor's wider
// delivery set before the owner found the bucket exists and logs nothing. The
// answer is the one an unsealed cluster gives (service.go createBucketLocked:
// success, "already exists ... it was not changed"), and a create of a NEW
// bucket at that factor is still refused for the plaintext member.
func TestRecreatingAnExistingBucketAtALargerFactorSaysItExists(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner := tc.nodes[0].server.core.cluster
	existing := bucketPlaced(t, owner, "recreated", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: existing, Type: BucketTypeHash, ReplicationFactor: 1})

	answer := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: existing, Type: BucketTypeHash, ReplicationFactor: 3})
	t.Logf("re-create at factor 3 answered %s: %s", answer.Opcode, printable(string(answer.Payload)))
	if answer.Opcode == transport.OpError {
		t.Fatalf("a re-create of existing bucket %q at factor 3 was refused, not answered that it exists: %s", existing, printable(string(answer.Payload)))
	}
	var created CreateBucketResponse
	if err := bson.Unmarshal(answer.Payload, &created); err != nil {
		t.Fatalf("decoding the answer: %v", err)
	}
	if !created.Success || !strings.Contains(created.Message, "already exists") || !strings.Contains(created.Message, "was not changed") {
		t.Errorf("the answer is %+v; want success saying the bucket already exists and was not changed", created)
	}

	fresh := bucketPlaced(t, owner, "freshlarger", ownedBy(tc.addrs[0]))
	refused := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fresh, Type: BucketTypeHash, ReplicationFactor: 3})
	if refused.Opcode != transport.OpError {
		t.Fatalf("a create of new bucket %q at factor 3 reaching a plaintext member was not refused: %s", fresh, printable(string(refused.Payload)))
	}
	assertPlaintextPeerRefusal(t, "create of a new bucket at factor 3", string(refused.Payload), tc.addrs[2])
}

// TestACreateThatFoundTheBucketOpenIsGuardedIfADropLandsFirst keeps the guard
// BUG-0206 defers for an open bucket on the path that matters: the create is
// held after the owner saw the bucket open and before its fence, the bucket is
// dropped, and the create is let go. It then places a new bucket at factor 3,
// which reaches the plaintext member, so it must be refused and leave nothing.
func TestACreateThatFoundTheBucketOpenIsGuardedIfADropLandsFirst(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner := clusterOf(tc, 0)
	bucket := bucketPlaced(t, owner, "droppedmid", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: 1})

	held, release := holdAt(t, &beforeOwnerFenceHook, owner.self.ID, transport.OpCreateBucket, bucket)
	answered := sendInBackground(t, owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, ReplicationFactor: 3})
	awaitHeld(t, held, "the re-create before its fence")
	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	close(release)

	select {
	case answer := <-answered:
		if answer.err == nil && answer.frame.Opcode != transport.OpError {
			t.Fatalf("a create of a dropped name at factor 3, reaching a plaintext member, was accepted: %s", printable(string(answer.frame.Payload)))
		}
		if answer.frame != nil {
			assertPlaintextPeerRefusal(t, "create of a name dropped after the open hint", string(answer.frame.Payload), tc.addrs[2])
		} else {
			t.Fatalf("the create failed without an answer: %v", answer.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the create was never answered")
	}
	if owner.s.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the refused create left bucket %q open on the owner", bucket)
	}
}
