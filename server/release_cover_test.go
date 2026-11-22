package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.7 (spec §20.8.7, R8-B, R8-G): a copy outside its bucket's set is
// released only when every member of the set covers it — the owner's answer
// alone is not enough.

// TestAReleaseWaitsForEveryMemberToHoldWhatItHolds (review M-1): four nodes at
// factor 3, and a join that moves a replica slot, not the owner — the owner O
// and a member M stay in the set, the holder H leaves it. O's stream to M is
// held, and five writes at majority are acknowledged by O and H
// (copies_confirmed 2). The newcomer pulls from O and holds them; M does not.
// H, handing the bucket off, keeps its copy while M lacks the writes, and
// releases it once M has caught up. Guard: compare only the owner's answer —
// H releases while M lacks writes it acknowledged.
func TestAReleaseWaitsForEveryMemberToHoldWhatItHolds(t *testing.T) {
	tc := newTestClusterWith(t, 4, 3, func(_ int, cfg *config.Config) { noSweepNoRetention(cfg) })
	defer tc.stop()
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	var owner, member, holder int
	bucket := bucketNamed(t, "cover", func(name string) bool {
		old, next := replicaIDs(t, before, 3, name), replicaIDs(t, after, 3, name)
		if old[0] != next[0] || !slices.Contains(next, newcomer.ID) {
			return false
		}
		var stays, leaves []string
		for _, id := range old[1:] {
			if slices.Contains(next, id) {
				stays = append(stays, id)
			} else {
				leaves = append(leaves, id)
			}
		}
		if len(stays) != 1 || len(leaves) != 1 {
			return false
		}
		owner = nodeIndexByAddr(t, tc.addrs, old[0])
		member = nodeIndexByAddr(t, tc.addrs, stays[0])
		holder = nodeIndexByAddr(t, tc.addrs, leaves[0])
		return true
	})

	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	releaseMember := holdStreamPushesOnly(peerOf(t, tc, owner, member))
	defer releaseMember()
	const writes = 5
	keys := make([]string, 0, writes)
	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("c%d", i)
		frame := tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
			Data: map[string]interface{}{"n": i}, WriteConcern: "majority"})
		if frame.Opcode == transport.OpError {
			t.Fatalf("setup: write %s at majority: %s", key, frame.Payload)
		}
		if confirmed, ok := bson.Raw(frame.Payload).Lookup("copies_confirmed").AsInt64OK(); !ok || confirmed != 2 {
			t.Fatalf("setup: write %s was answered copies_confirmed %d (%v), want 2: the owner and the holder", key, confirmed, ok)
		}
		keys = append(keys, key)
	}
	lacking := func(node *testNode) []string {
		var missing []string
		for _, key := range keys {
			if !holdsKeyLocally(node, bucket, key) {
				missing = append(missing, key)
			}
		}
		return missing
	}
	if missing := lacking(tc.nodes[holder]); len(missing) > 0 {
		t.Fatalf("setup: the holder, node %d, lacks %v", holder+1, missing)
	}
	if missing := lacking(tc.nodes[member]); len(missing) != writes {
		t.Fatalf("setup: the member whose stream is held, node %d, holds writes it should lack (lacks only %v)", member+1, missing)
	}

	joiner := joinNewcomer(t, tc, owner, ports)
	holderCluster := clusterOf(tc, holder)
	key := placementOf(testDatabase, bucket)
	replicas, err := holderCluster.Replicas(key)
	if err != nil {
		t.Fatalf("placing %s on the holder: %v", bucket, err)
	}
	if nodeListed(replicas, tc.addrs[holder]) {
		t.Fatalf("setup: the holder, node %d, is still in the set of %s after the join", holder+1, bucket)
	}
	handOff := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return holderCluster.handOffBucket(ctx, testDatabase, bucket, replicas)
	}

	// M's stream is held: H hands off round after round and keeps its copy.
	for round := 0; round < 5; round++ {
		err := handOff()
		if round == 0 {
			t.Logf("the holder's hand-off while M is behind: %v", err)
		}
		if !holderCluster.s.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("the holder, node %d, released its copy while the member node %d lacks %v, acknowledged with copies_confirmed 2 by the owner and the holder",
				holder+1, member+1, lacking(tc.nodes[member]))
		}
		time.Sleep(300 * time.Millisecond)
	}
	if missing := lacking(joiner.node); len(missing) > 0 {
		t.Fatalf("setup: the newcomer, asked to migrate by the holder, lacks %v after its pull from the owner", missing)
	}

	// M catches up; H's next hand-off releases.
	releaseMember()
	if !holdsWithin(20*time.Second, func() bool { return len(lacking(tc.nodes[member])) == 0 }) {
		t.Fatalf("the member, node %d, never received %v once its stream was released", member+1, lacking(tc.nodes[member]))
	}
	released := holdsWithin(20*time.Second, func() bool {
		if err := handOff(); err != nil {
			return false
		}
		return !holderCluster.s.bucketStoreOpen(testDatabase, bucket)
	})
	if !released {
		t.Errorf("the holder, node %d, kept its copy after every member of the set came to hold what it holds: %v", holder+1, handOff())
	}
}

// TestAReleaseWaitsForTheOwnersCopyToCoverIt (review m-3, R8-B): the cover
// test a holder outside the set applies to each member's answer, and the WARN
// it writes on a release that concedes entries. A copy is covered by one of
// its incarnation in its term at or past its position, or in a later term; the
// release over a later term opened from the copy's term below the copy's
// position names how many entries are given up, and over a term opened from a
// later term still says only "a copy of an older term". A copy of another
// incarnation never covers. Guard: no cover test — the copy is released over
// a member behind it and over another incarnation.
func TestAReleaseWaitsForTheOwnersCopyToCoverIt(t *testing.T) {
	own := ReplCopy{Holds: true, Incarnation: 7, Term: 4, Through: 20}
	cases := []struct {
		name        string
		member      ReplCopy
		covers      bool
		warnSays    string
		warnLacks   string
		warnsAtFull bool
	}{
		{name: "the copy's term, at its position", member: ReplCopy{Holds: true, Incarnation: 7, Term: 4, Through: 20}, covers: true},
		{name: "the copy's term, behind its position", member: ReplCopy{Holds: true, Incarnation: 7, Term: 4, Through: 15}},
		{name: "a later term opened from the copy's term below its position",
			member: ReplCopy{Holds: true, Incarnation: 7, Term: 6, Through: 3, BaseTerm: 4, BaseThrough: 15}, covers: true,
			warnSays: "held 5 entries its owner took the bucket over without", warnsAtFull: true},
		{name: "a later term opened from the copy's term at its position",
			member: ReplCopy{Holds: true, Incarnation: 7, Term: 6, Through: 3, BaseTerm: 4, BaseThrough: 20}, covers: true},
		{name: "a later term opened from a later term still",
			member: ReplCopy{Holds: true, Incarnation: 7, Term: 6, Through: 3, BaseTerm: 5, BaseThrough: 9}, covers: true,
			warnSays: "of an older term than its owner's", warnLacks: "entries its owner took", warnsAtFull: true},
		{name: "a later term opened from a term below the copy's (BUG-0475)",
			member: ReplCopy{Holds: true, Incarnation: 7, Term: 6, Through: 3, BaseTerm: 3, BaseThrough: 9}, covers: true,
			warnSays: "at least 20 entries", warnLacks: "held 20 entries", warnsAtFull: true},
		{name: "another incarnation in a later term",
			member: ReplCopy{Holds: true, Incarnation: 8, Term: 6, Through: 3, BaseTerm: 4, BaseThrough: 20}},
		{name: "another incarnation in the copy's term, ahead of it",
			member: ReplCopy{Holds: true, Incarnation: 8, Term: 4, Through: 30}},
		{name: "no copy", member: ReplCopy{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := coversForRelease(tc.member, own); got != tc.covers {
				t.Fatalf("a member at (incarnation %d, term %d, through %d, base %d/%d) covers a copy at (7, 4, 20): %v, want %v",
					tc.member.Incarnation, tc.member.Term, tc.member.Through, tc.member.BaseTerm, tc.member.BaseThrough, got, tc.covers)
			}
			if !tc.covers {
				return
			}
			var logs bytes.Buffer
			c := &Cluster{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))}
			c.noteConcession(testDatabase, "b", own, tc.member)
			written := logs.String()
			if !tc.warnsAtFull {
				if written != "" {
					t.Errorf("a release that concedes nothing wrote: %s", written)
				}
				return
			}
			if !strings.Contains(written, tc.warnSays) {
				t.Errorf("the release's WARN does not say %q: %s", tc.warnSays, written)
			}
			if tc.warnLacks != "" && strings.Contains(written, tc.warnLacks) {
				t.Errorf("the release's WARN counts entries it cannot know of: %s", written)
			}
		})
	}
}
