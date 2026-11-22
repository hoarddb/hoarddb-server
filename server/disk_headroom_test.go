package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ring"
)

// These tests report a near-full disk through one server's probe set
// (setCapacityProbes, capacity_probes.go) without filling the host
// filesystem: a tiny hoardDB cluster uses well under 1% of /tmp, so a valid
// integer high-water mark can never be exceeded by real usage there. The
// override is per Server, so one node of several can be made red while the
// others stay green.

// TestJoinRefusesWhenProposerDiskAtCapacity proves 1e: the add-node flow
// refuses before gathering votes when the proposer is above the disk high-water
// mark, with the recovery step named, and a removal is never refused on
// headroom (it frees room).
func TestJoinRefusesWhenProposerDiskAtCapacity(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	oc := tc.nodes[0].server.core.cluster
	// The harness config is config.DefaultConfig(), whose DiskHighWaterPct is
	// 85; set 50 anyway, so the test's meaning does not rest on the default.
	tc.nodes[0].server.core.config.Server.DiskHighWaterPct = 50
	setCapacityProbes(t, tc.nodes[0].server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })

	ja := fabricatedMemberAddr(t)
	err := oc.proposeJoin(context.Background(), ring.Node{ID: ja, Addr: ja})
	if !errors.Is(err, ErrMemberDiskFull) {
		t.Fatalf("join on a near-full node = %v, want ErrMemberDiskFull", err)
	}
	if !strings.Contains(err.Error(), "HOARDDB_DISK_HIGH_WATER") {
		t.Fatalf("the error must name the recovery step, got: %v", err)
	}

	// Same node, over the edge: removal is not blocked by headroom — the
	// unknown target still fails on membership, not on disk.
	if err := oc.proposeRemoval(context.Background(), "ghost"); !errors.Is(err, ErrNotMember) {
		t.Fatalf("removal should not be refused for headroom, got %v", err)
	}
}

// TestVoteRefusesWhenVoterDiskAtCapacity proves the distributed half: one voter
// over the watermark refuses the proposal, so the proposer cannot gather a
// majority off that node.
func TestVoteRefusesWhenVoterDiskAtCapacity(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	oc0 := tc.nodes[0].server.core.cluster
	oc1 := tc.nodes[1].server.core.cluster
	tc.nodes[1].server.core.config.Server.DiskHighWaterPct = 50

	ja := fabricatedMemberAddr(t)
	peer := Peer{ID: ja, Addr: ja}
	startEpoch := oc1.Epoch()
	req := acceptRequestOf(oc0.self.ID, oc0.Epoch()+1, append(oc0.Members(), ring.Node{ID: ja, Addr: ja}), peer)

	// Report node1's disk full: the shape is a valid adjacent join, so the
	// refusal must come from the headroom gate, not from validation.
	voter := tc.nodes[1].server.core
	setCapacityProbes(t, voter, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(88)) })
	if _, err := oc1.acceptMembershipChange(req); !errors.Is(err, ErrMemberDiskFull) {
		t.Fatalf("a member at capacity answered the accept with %v, want ErrMemberDiskFull", err)
	}

	// Control: the identical request is accepted the moment headroom returns.
	// Injected rather than left to the real statfs: this asserts "a voter with
	// headroom accepts", and the real usage of the filesystem behind TMPDIR is
	// not headroom — 19% on this host's tmpfs, 79% on a container's overlay, 80%
	// on this host's root filesystem. Anything at or over the 50% watermark
	// below fails the control on a machine with nothing wrong with it, so the
	// value is stated instead of measured.
	setCapacityProbes(t, voter, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	if _, err := oc1.acceptMembershipChange(req); err != nil {
		t.Fatalf("a member with headroom refused the same join: %v", err)
	}
	// An acceptance acts on nothing: the epoch moves only when the change
	// is chosen and committed.
	if epoch := oc1.Epoch(); epoch != startEpoch {
		t.Errorf("accepting the join moved the member from epoch %d to %d", startEpoch, epoch)
	}
}

// TestDiskGuardReadsTheServerProbe (build doc BT-1) proves the guard and the
// refusal text both read the server's own probe member, not a real statfs: the
// refusal names the injected 92%, which the host's disk never reports.
func TestDiskGuardReadsTheServerProbe(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	node := tc.nodes[0].server.core
	oc := node.cluster

	setCapacityProbes(t, node, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })
	ja := fabricatedMemberAddr(t)
	err := oc.proposeJoin(context.Background(), ring.Node{ID: ja, Addr: ja})
	if !errors.Is(err, ErrMemberDiskFull) {
		t.Fatalf("join at an injected 92%% against the default mark 85 = %v, want ErrMemberDiskFull", err)
	}
	if !strings.Contains(err.Error(), "92% used") {
		t.Fatalf("the refusal must report the injected usage, not the host's: %v", err)
	}

	setCapacityProbes(t, node, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	if !oc.diskHeadroomOK() {
		t.Fatal("guard refused at an injected 10% against the mark 85")
	}
}

// TestJoinCommitsOverOneRedVoter (spec T-21) characterises the vote; (a) and
// (b) are the facts the user page states, and (c) is kept by the owner's
// BUG-0104 ruling: a red node still adopts every epoch. What it must not do
// after adopting, land a bucket, is TestRedNodeDoesNotLandABucketByAnyPath's.
// This test records what the tree does, so the page's "a majority can still
// admit one" is pinned to the code rather than asserted.
func TestJoinCommitsOverOneRedVoter(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	nodeA := tc.nodes[0].server.core
	nodeC := tc.nodes[2].server.core
	// Only C is red. A and B are pinned green rather than left to the host's
	// real disk, which could itself be over the default mark of 85.
	setCapacityProbes(t, nodeA, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	setCapacityProbes(t, tc.nodes[1].server.core, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(10)) })
	setCapacityProbes(t, nodeC, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(88)) })

	ca, cc := nodeA.cluster, nodeC.cluster
	startEpoch := ca.Epoch()
	ja := fabricatedMemberAddr(t)
	peer := Peer{ID: ja, Addr: ja}
	req := acceptRequestOf(ca.self.ID, startEpoch+1, append(ca.Members(), ring.Node{ID: ja, Addr: ja}), peer)

	// (a) C, over the mark, votes against A's request.
	if _, err := cc.acceptMembershipChange(req); !errors.Is(err, ErrMemberDiskFull) {
		t.Fatalf("(a) red voter C voted %v, want ErrMemberDiskFull", err)
	}

	// (b) A's proposal commits anyway, on the votes of A and B.
	if err := ca.proposeJoin(context.Background(), ring.Node{ID: ja, Addr: ja}); err != nil {
		t.Fatalf("(b) join over one red voter of three = %v, want it to commit on a majority", err)
	}
	if got := ca.Epoch(); got != startEpoch+1 {
		t.Fatalf("(b) proposer epoch = %d, want %d", got, startEpoch+1)
	}

	// (c) C adopts A's newer epoch while still over the mark. Gossip may have
	// delivered it already; reconcileMembership at an equal epoch with an equal
	// set is then a no-op, so the assertion holds either way.
	newMembers := ca.memberNodes()
	if err := cc.reconcileMembership(ca.self.ID, newMembers, memberIDsOf(peersToNodes(newMembers)), ca.Epoch()); err != nil {
		t.Fatalf("(c) C reconciling to A's epoch: %v", err)
	}
	if got := cc.Epoch(); got != startEpoch+1 {
		t.Fatalf("(c) red node C is on epoch %d, want %d (it adopts the join it voted against)", got, startEpoch+1)
	}
	if cc.diskHeadroomOK() {
		t.Fatal("(c) C must still be over the mark when it adopts the epoch")
	}
}

// TestDiskZeroBlocksIsUnavailable (spec T-18) proves a filesystem that reports
// no blocks reaches the guard through its error branch: the decision is still
// "allow" (the guard fails open by design), but no longer because 0 blocks
// was read as 0% used. The gauge half: the disk series are absent and the
// state is unavailable, where the old arithmetic published a healthy 0.
func TestDiskZeroBlocksIsUnavailable(t *testing.T) {
	s := newTestServer(t)
	guard := &Cluster{s: s}
	for _, tt := range []struct {
		name string
		stat diskStat
	}{
		{name: "zero blocks", stat: diskStat{Blocks: 0, Bavail: 0, Frsize: 4096}},
		{name: "zero fragment size", stat: diskStat{Blocks: 100, Bavail: 50, Frsize: 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setCapacityProbes(t, s, func(p *capacityProbes) { p.diskStatfs = fixedDisk(tt.stat) })
			if _, err := s.readDiskStat(s.config.Storage.DataDir); err == nil {
				t.Fatalf("readDiskStat(%+v) = nil error, want the zero-blocks refusal", tt.stat)
			}
			if !guard.diskHeadroomOK() {
				t.Fatalf("guard refused on an unmeasurable filesystem %+v; it must fail open", tt.stat)
			}

			setCapacityProbes(t, s, greenProbes)
			cs := newIdleSampler(t, s)
			cs.sampleNow()
			setCapacityProbes(t, s, func(p *capacityProbes) {
				greenProbes(p)
				p.diskStatfs = fixedDisk(tt.stat)
			})
			cs.sampleNow()
			gathered := gatherSampler(t, cs)
			for _, family := range []string{familyDiskUsed, familyDiskSize} {
				if len(gathered[family]) != 0 {
					t.Fatalf("%s is published for an unmeasurable filesystem", family)
				}
			}
			if _, ok := seriesValue(gathered, familyRatio, "resource", resourceDisk); ok {
				t.Fatal("the disk ratio is published for an unmeasurable filesystem")
			}
			if row := rowFor(t, cs, resourceDisk); row.state != stateUnavailable {
				t.Fatalf("disk state %v, want unavailable", row.state)
			}
		})
	}
}
