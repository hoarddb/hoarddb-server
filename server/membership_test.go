package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
)

// TestLocateSelfRefusalAdvisesWildcardFirst is BUG-0611: a node that is its
// own listed record on another address must be told the wildcard listen first.
// That is the fix when its address changed (a VM moved to a new IP) or when its
// host name resolves on this host to another address (a 127.0.1.1 hosts entry);
// the listed-address option stays for a node that does listen there.
func TestLocateSelfRefusalAdvisesWildcardFirst(t *testing.T) {
	members := []ring.Node{{ID: "n1", Addr: "127.0.0.1:24433"}, {ID: "n2", Addr: "127.0.0.1:24443"}}
	listen := "127.0.0.2:24443"
	self := ring.Node{ID: "n2", Addr: normalizeAddr(listen)}
	_, err := locateSelf(members, self, listen, "membership.json")
	if err == nil {
		t.Fatalf("a concrete listener at %s was accepted as the record listed at %s", listen, members[1].Addr)
	}
	message := err.Error()
	wildcard := strings.Index(message, "HOARDDB_LISTEN=0.0.0.0:24443")
	listed := strings.Index(message, "HOARDDB_LISTEN=127.0.0.1:24443")
	if wildcard < 0 {
		t.Fatalf("the refusal offers no wildcard listen:\n%s", message)
	}
	if listed < 0 {
		t.Fatalf("the refusal dropped the listed-address option:\n%s", message)
	}
	if wildcard > listed {
		t.Errorf("the refusal offers the listed address before the wildcard (wildcard at %d, listed at %d); a moved VM and a locally-resolving host name need the wildcard first:\n%s",
			wildcard, listed, message)
	}
}

// TestMembershipFileRoundTrip covers the durable membership record on its own:
// a written record reads back, a missing record is a first start, and a
// corrupt proposal is refused rather than persisted.
func TestMembershipFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, membershipFileName)

	if _, _, found, err := loadMembership(path); err != nil || found {
		t.Fatalf("loading a missing membership record: found=%v err=%v", found, err)
	}

	members := []ring.Node{
		{ID: "a@1", Addr: "127.0.0.1:1"},
		{ID: "b@1", Addr: "127.0.0.1:2"},
	}
	if err := saveMembershipRecord(path, membershipFile{Epoch: 2, Members: members}); err != nil {
		t.Fatalf("save: %v", err)
	}
	epoch, loaded, found, err := loadMembership(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !found || epoch != 2 || len(loaded) != 2 {
		t.Fatalf("reloaded epoch=%d members=%d found=%v, want 2/2/true", epoch, len(loaded), found)
	}
	if loaded[0].ID != "a@1" || loaded[1].Addr != "127.0.0.1:2" {
		t.Fatalf("reloaded members are wrong: %+v", loaded)
	}

	if err := saveMembershipRecord(path, membershipFile{Epoch: 0, Members: members}); err == nil {
		t.Error("persisting epoch 0 was accepted")
	}
	if err := saveMembershipRecord(path, membershipFile{Epoch: 2, Members: nil}); err == nil {
		t.Error("persisting an empty member set was accepted")
	}
}

// TestMembershipAdoptsNewerEpochViaWire exercises the whole resync path: a node
// is moved onto a newer epoch by a proposal from an existing member, the record
// is persisted, a peer that was behind converges to the same epoch over a real
// gossip round trip, and the node restarts still on the newer epoch.
func TestMembershipAdoptsNewerEpochViaWire(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	n0 := tc.nodes[0].server.core.cluster
	if got := n0.Epoch(); got != InitialEpoch {
		t.Fatalf("fresh node epoch = %d, want %d", got, InitialEpoch)
	}
	if got := len(n0.Members()); got != 3 {
		t.Fatalf("fresh node members = %d, want 3", got)
	}

	// A fourth node, proposed by an existing member (never self).
	daddr := fabricatedMemberAddr(t)
	extra := ring.Node{ID: daddr, Addr: daddr}
	newMembers := append([]ring.Node{}, n0.Members()...)
	newMembers = append(newMembers, extra)
	proposer := ring.Node{}
	for _, m := range n0.Members() {
		if m.ID != n0.self.ID {
			proposer = m
			break
		}
	}
	if proposer.ID == "" {
		t.Fatal("no non-self member to act as proposer")
	}

	if err := n0.reconcileMembership(proposer.ID, nodesToPeers(newMembers), memberIDsOf(newMembers), InitialEpoch+1); err != nil {
		t.Fatalf("reconcile to epoch 2: %v", err)
	}
	if got := n0.Epoch(); got != InitialEpoch+1 {
		t.Fatalf("after adoption epoch = %d, want %d", got, InitialEpoch+1)
	}
	if got := len(n0.Members()); got != 4 {
		t.Fatalf("after adoption members = %d, want 4", got)
	}
	if n0.mismatch.Load() {
		t.Error("adoption left the divergence flag set")
	}
	// The newcomer must be reachable as a peer.
	if p := n0.peerFor(extra); p == nil || p.node.Addr != daddr {
		t.Fatalf("adopted member is not a reachable peer: %+v", p)
	}

	// The record is durable.
	epoch, persisted, found, err := loadMembership(n0.membershipPath())
	if err != nil || !found {
		t.Fatalf("loaded membership after adoption: found=%v err=%v", found, err)
	}
	if epoch != InitialEpoch+1 || len(persisted) != 4 {
		t.Fatalf("persisted membership epoch=%d members=%d, want %d/4", epoch, len(persisted), InitialEpoch+1)
	}

	// A peer that is still on the old epoch converges over the real wire: the
	// behind node's gossip round trip carries our epoch 2 view back and it
	// adopts it (the proposer is a member of its current epoch).
	n1 := tc.nodes[1].server.core.cluster
	peerTo0 := n1.peerFor(mustNode(t, n1, tc.addrs[0]))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := peerTo0.gossip(ctx); err != nil {
		t.Fatalf("gossip from node 1 to node 0: %v", err)
	}
	if got := n1.Epoch(); got != InitialEpoch+1 {
		t.Fatalf("node 1 did not converge via wire: epoch = %d, want %d", got, InitialEpoch+1)
	}
	if got := len(n1.Members()); got != 4 {
		t.Fatalf("node 1 converged to %d members, want 4", got)
	}

	// Restart the proposing node: the persisted record, not the stale config,
	// decides its membership.
	_ = tc.nodes[0].client.Close()
	tc.nodes[0].client = nil
	if err := tc.nodes[0].server.Stop(); err != nil {
		t.Fatalf("stop node 0: %v", err)
	}
	restarted := tc.startNode(0, filepath.Dir(tc.nodes[0].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[0].cfg
	tc.nodes[0] = restarted
	if got := restarted.server.core.cluster.Epoch(); got != InitialEpoch+1 {
		t.Fatalf("restarted node epoch = %d, want %d", got, InitialEpoch+1)
	}
	if got := len(restarted.server.core.cluster.Members()); got != 4 {
		t.Fatalf("restarted node members = %d, want 4", got)
	}
}

// TestMembershipRefusesUninvitedProposal: a node claiming a newer epoch but not
// a member of our current epoch must be refused outright — adopting a
// stranger's membership is how a removed or rogue node would drag the cluster
// to a shape it never agreed to.
func TestMembershipRefusesUninvitedProposal(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	n0 := tc.nodes[0].server.core.cluster
	beforeEpoch := n0.Epoch()
	beforeMembers := len(n0.Members())

	stranger := ring.Node{ID: "stranger@9.9.9.9:1", Addr: "9.9.9.9:1"}
	claimed := append([]ring.Node{n0.self}, n0.Members()...)
	claimed = append(claimed, stranger)

	err := n0.reconcileMembership(stranger.ID, nodesToPeers(claimed), memberIDsOf(claimed), 50)
	if err == nil {
		t.Fatal("adoption of an uninvited higher epoch was accepted")
	}
	if !n0.mismatch.Load() {
		t.Error("an uninvited proposal did not set the divergence flag")
	}
	if got := n0.Epoch(); got != beforeEpoch {
		t.Fatalf("epoch changed to %d after refusal, want %d", got, beforeEpoch)
	}
	if got := len(n0.Members()); got != beforeMembers {
		t.Fatalf("members changed to %d after refusal, want %d", got, beforeMembers)
	}
}

// TestMembershipSameEpochDivergenceRecovers: differing member sets at the same
// epoch is a genuine operator divergence (two views, no committed change) and
// must fail loud; agreeing again must clear the flag so writes resume.
func TestMembershipSameEpochDivergenceRecovers(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	n0 := tc.nodes[0].server.core.cluster
	proposer := ring.Node{}
	for _, m := range n0.Members() {
		if m.ID != n0.self.ID {
			proposer = m
			break
		}
	}

	// A different set at the same epoch: one member is missing.
	partial := append([]ring.Node{}, n0.Members()...)[:2]
	if err := n0.reconcileMembership(proposer.ID, nodesToPeers(partial), memberIDsOf(partial), InitialEpoch); err == nil {
		t.Fatal("same-epoch divergence was not flagged")
	}
	if !n0.mismatch.Load() {
		t.Error("divergence did not set the flag")
	}

	// The full set again at the same epoch clears it.
	if err := n0.reconcileMembership(proposer.ID, nodesToPeers(n0.Members()), memberIDsOf(n0.Members()), InitialEpoch); err != nil {
		t.Fatalf("re-agreeing failed: %v", err)
	}
	if n0.mismatch.Load() {
		t.Error("re-agreeing did not clear the divergence flag")
	}
}

// TestMembershipPeerBehindDoesNotRegress: a peer on an older epoch answers nil
// (we stay ahead) and must never be adopted onto — a node never regresses its
// membership to match a peer that is behind.
func TestMembershipPeerBehindDoesNotRegress(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	n0 := tc.nodes[0].server.core.cluster
	proposer := ring.Node{}
	for _, m := range n0.Members() {
		if m.ID != n0.self.ID {
			proposer = m
			break
		}
	}

	// n0 is at epoch 1; a peer "at epoch 0 with a smaller set" is behind.
	behind := []ring.Node{proposer}
	if err := n0.reconcileMembership(proposer.ID, nodesToPeers(behind), memberIDsOf(behind), 0); err != nil {
		t.Fatalf("a behind peer regressed us or errored: %v", err)
	}
	if got := n0.Epoch(); got != InitialEpoch {
		t.Fatalf("epoch regressed to %d, want %d", got, InitialEpoch)
	}
	if got := len(n0.Members()); got != 3 {
		t.Fatalf("members regressed to %d, want 3", got)
	}
	if n0.mismatch.Load() {
		t.Error("a behind peer set the divergence flag")
	}
}
