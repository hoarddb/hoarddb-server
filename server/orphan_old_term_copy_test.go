package server

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0545: a replay of the owner's log delivers an orphaned create to the
// members that stay in its bucket's set, in the old owner's term. With
// auto-rebalance off, the confirm pass counted those copies as not following
// the new owner but never listed them, so the operator was given nothing to
// migrate and the pause never lifted.

// stallOwnerStreams stalls every stream of the orphan's owner, so each is
// opened again from the log and replays the orphaned create to the members
// that were and still are in its bucket's set.
func (s *autoOffOrphan) stallOwnerStreams() {
	s.owner.peersMu.RLock()
	defer s.owner.peersMu.RUnlock()
	for _, p := range s.owner.peers {
		p.stall("test: the owner's streams are opened again from the log after an orphaned create")
	}
}

// nodeNamed is the test node whose ring address is id, the joiner included.
func (s *autoOffOrphan) nodeNamed(t *testing.T, id string) *testNode {
	t.Helper()
	if id == s.ports.ringAddr {
		return s.joiner
	}
	for index, addr := range s.tc.addrs {
		if addr == id {
			return s.tc.nodes[index]
		}
	}
	t.Fatalf("no node is %s", id)
	return nil
}

// logOrphanCopies logs every member's answer about the orphan's copy.
func (s *autoOffOrphan) logOrphanCopies(t *testing.T) {
	t.Helper()
	for _, member := range s.members() {
		answer, err := member.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: s.orphan})
		t.Logf("%s: copy of %s %+v (err %v), hand-off %+v", member.self.ID, s.orphan, answer, err, member.ownHandOff())
	}
}

// migrateListedUntilLifted does what the operator is told: it runs 'migrate'
// once for every copy any member's hand-off lists, on the node it names, until
// every member lifts the pause or within has passed.
func (s *autoOffOrphan) migrateListedUntilLifted(t *testing.T, within time.Duration, what string) {
	t.Helper()
	var migrated []PlacementToCopy
	for deadline := time.Now().Add(within); !allLifted(s.members()); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			s.logOrphanCopies(t)
			t.Logf("migrated %v", migrated)
			waitLifted(t, s.members(), time.Millisecond, what)
		}
		wakeAll(s.members())
		for _, member := range s.members() {
			progress := member.ownHandOff()
			if progress == nil {
				continue
			}
			for _, copyToMake := range progress.Unconfirmed {
				if slices.Contains(migrated, copyToMake) {
					continue
				}
				migrated = append(migrated, copyToMake)
				frame := s.tc.frameOn(s.nodeNamed(t, copyToMake.Node), transport.OpMigrate,
					MigrateRequest{Database: copyToMake.Database, Bucket: copyToMake.Bucket})
				if frame.Opcode == transport.OpError {
					t.Fatalf("migrate %s %s on %s: %s", copyToMake.Database, copyToMake.Bucket, copyToMake.Node, string(frame.Payload))
				}
			}
		}
	}
}

// oldTermHolders is the members of the orphan's set, other than its new
// owner, that hold it in another term than the new owner's opened copy now;
// none while the new owner has not opened its copy.
func (s *autoOffOrphan) oldTermHolders() []string {
	ref := ReplBucketRef{Database: testDatabase, Bucket: s.orphan}
	newOwner := s.joiner.server.core.cluster
	owned, err := newOwner.answerCopy(ref)
	if err != nil || !owned.Holds || !owned.Opened {
		return nil
	}
	set, err := newOwner.Replicas(placementOf(testDatabase, s.orphan))
	if err != nil {
		return nil
	}
	var holders []string
	for _, member := range s.members() {
		if member == newOwner || !slices.Contains(nodeIDs(set), member.self.ID) {
			continue
		}
		if held, err := member.answerCopy(ref); err == nil && held.Holds && held.Term != owned.Term {
			holders = append(holders, member.self.ID)
		}
	}
	return holders
}

// TestARefusalListsTheMigrateOfACopyInTheOldOwnersTerm: once the orphan's new
// owner holds it, a member whose copy the replay left in the old owner's term
// is listed with the exact command that settles it, and a refusal through
// another member prints it.
func TestARefusalListsTheMigrateOfACopyInTheOldOwnersTerm(t *testing.T) {
	s := setUpAutoOffOrphan(t)
	defer s.tc.stop()
	s.stallOwnerStreams()
	for _, copyToMake := range s.listedCopies(t) {
		if copyToMake.Node != s.ports.ringAddr {
			continue
		}
		if frame := s.tc.frameOn(s.joiner, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: s.orphan}); frame.Opcode == transport.OpError {
			t.Fatalf("migrate %s on the joiner: %s", s.orphan, string(frame.Payload))
		}
	}
	var refused string
	var holders []string
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		wakeAll(s.members())
		frame := s.tc.frame(1, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "after_old_term", Type: BucketTypeHash})
		if frame.Opcode != transport.OpError {
			t.Fatalf("a create was accepted while the orphan's copies are unsettled")
		}
		refused = refusalOf(t, frame).Message
		holders = s.oldTermHolders()
		for _, holder := range holders {
			if strings.Contains(refused, "'migrate "+testDatabase+" "+s.orphan+"' on "+holder) {
				return
			}
		}
	}
	s.logOrphanCopies(t)
	t.Fatalf("the refusal names no 'migrate %s %s' on any of %v, which hold it in the old owner's term:\n%s",
		testDatabase, s.orphan, holders, refused)
}

// TestAnOrphanCreateSettlesAfterItsOwnersStreamsReplayTheLog: with
// auto-rebalance off, the orphan's owner stalls every stream after the create
// is answered, so the streams replay the create from the log. Running the
// migrate commands the members list lifts the pause.
func TestAnOrphanCreateSettlesAfterItsOwnersStreamsReplayTheLog(t *testing.T) {
	s := setUpAutoOffOrphan(t)
	defer s.tc.stop()
	s.stallOwnerStreams()
	s.migrateListedUntilLifted(t, 30*time.Second, "after the orphan's streams replayed the log")
}
