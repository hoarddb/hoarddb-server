package server

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0545's operator path: 'migrate' on a member that already holds the
// bucket says whether its copy follows its owner, and makes a copy in another
// term than its owner's follow it.

// followerOfTestBucket creates testBucket with one document written to every
// member and returns the index of a member that is not its owner, once that
// member's copy is in its owner's opened term, with that term.
func followerOfTestBucket(t *testing.T, tc *testCluster) (int, uint64) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.frame(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "seed",
		Data: map[string]interface{}{"i": 1}, WriteConcern: "all",
	})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	set, err := clusterOf(tc, 0).Replicas(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("placing %s: %v", testBucket, err)
	}
	follower := slices.IndexFunc(tc.addrs, func(addr string) bool { return addr != set[0].ID })
	owner := clusterOf(tc, slices.Index(tc.addrs, set[0].ID))
	ref := ReplBucketRef{Database: testDatabase, Bucket: testBucket}
	var term uint64
	pollUntil(t, "the follower's copy is in its owner's opened term", func() bool {
		owned, err := owner.answerCopy(ref)
		if err != nil || !owned.Holds || !owned.Opened {
			return false
		}
		held, err := clusterOf(tc, follower).answerCopy(ref)
		term = owned.Term
		return err == nil && held.Holds && held.Term == owned.Term
	})
	return follower, term
}

// TestAMigrateOnAFollowingHolderSaysItAlreadyFollows: a listed 'migrate' run
// on a member whose copy already follows its owner says so, rather than
// advising a repair as if nothing were known about the copy.
func TestAMigrateOnAFollowingHolderSaysItAlreadyFollows(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	follower, term := followerOfTestBucket(t, tc)
	migrated := migrateAnswer(t, tc, follower)
	want := fmt.Sprintf("bucket %q in %q on %s already follows its owner's term %d; nothing to do",
		testBucket, testDatabase, clusterOf(tc, follower).self.ID, term)
	if !migrated.Success || !strings.HasPrefix(migrated.Message, want) {
		t.Fatalf("migrate on a member that follows its owner answered %+v; want a success starting %q", migrated, want)
	}
}

// TestAMigrateOnAHolderInAnotherTermTakesTheOwnersTerm: with the worker held,
// 'migrate' on a member whose copy is in another term than its owner's waits
// the write-concern timeout and refuses with the command to run again; once
// the worker runs, the same command makes the copy follow its owner.
func TestAMigrateOnAHolderInAnotherTermTakesTheOwnersTerm(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 3, nil, func(_ int, c *Cluster) { c.wcTimeout = 2 * time.Second })
	defer tc.stop()
	follower, term := followerOfTestBucket(t, tc)
	member := clusterOf(tc, follower)
	set, err := member.Replicas(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("placing %s: %v", testBucket, err)
	}
	takeOverOffHook.Store(true)
	defer takeOverOffHook.Store(false)
	const otherTerm = 99
	if err := member.s.rewriteLineageIf(testDatabase, testBucket, func(st *store.BucketState) bool {
		st.Term = otherTerm
		return true
	}); err != nil {
		t.Fatalf("moving the member's copy to another term: %v", err)
	}

	frame := tc.frame(follower, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: testBucket})
	if frame.Opcode != transport.OpError {
		t.Fatalf("migrate with the worker held answered %s; want a refusal", answerText(frame))
	}
	want := fmt.Sprintf("bucket %q in database %q on %s is in term %d and its owner %s is in term %d; this node is taking its owner's term. "+
		"Run 'migrate %s %s' on %s again in a moment",
		testBucket, testDatabase, member.self.ID, otherTerm, set[0].ID, term, testDatabase, testBucket, member.self.ID)
	if got := refusalOf(t, frame).Message; !strings.Contains(got, want) {
		t.Fatalf("the refusal is\n%s\nwant it to contain\n%s", got, want)
	}

	takeOverOffHook.Store(false)
	frame = tc.frame(follower, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: testBucket})
	if frame.Opcode == transport.OpError {
		t.Fatalf("migrate with the worker running was refused: %s", answerText(frame))
	}
	var migrated MigrateResponse
	if err := bson.Unmarshal(frame.Payload, &migrated); err != nil {
		t.Fatalf("decoding the migrate answer: %v", err)
	}
	want = fmt.Sprintf("bucket %q in %q on %s now follows its owner's term %d", testBucket, testDatabase, member.self.ID, term)
	if !migrated.Success || migrated.Message != want {
		t.Fatalf("migrate answered %+v; want %q", migrated, want)
	}
}

// TestNoMemberIsListedBehindAnOwnerThatHasNotOpened: while the owner's copy is
// not opened, no member is listed as behind its term: the owner's own copy to
// make comes first, and the others follow from it.
func TestNoMemberIsListedBehindAnOwnerThatHasNotOpened(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	follower, _ := followerOfTestBucket(t, tc)
	c := clusterOf(tc, follower)
	set, err := c.Replicas(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("placing %s: %v", testBucket, err)
	}
	own, err := c.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: testBucket})
	if err != nil {
		t.Fatalf("reading the follower's copy: %v", err)
	}
	answers := make(map[string]ReplCopy)
	var others []string
	for _, member := range set {
		switch member.ID {
		case set[0].ID:
			answers[member.ID] = ReplCopy{Holds: true, Term: own.Term + 1, Incarnation: own.Incarnation}
		case c.self.ID:
			others = append(others, member.ID)
		default:
			answers[member.ID] = ReplCopy{Holds: true, Term: own.Term, Incarnation: own.Incarnation}
			others = append(others, member.ID)
		}
	}
	held := heldBucket{database: testDatabase, bucket: testBucket}
	if behind := c.membersBehindOwnersTerm(held, answers); len(behind) != 0 {
		t.Fatalf("with the owner's copy not opened, %v were listed behind its term", behind)
	}
	// Control: the same copies behind an opened owner are listed.
	opened := answers[set[0].ID]
	opened.Opened = true
	answers[set[0].ID] = opened
	if behind := c.membersBehindOwnersTerm(held, answers); !slices.Equal(behind, others) {
		t.Fatalf("with the owner's copy opened, %v were listed behind its term; want %v", behind, others)
	}
}
