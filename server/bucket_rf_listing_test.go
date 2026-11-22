package server

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// What `show buckets` says of a bucket's copies (bucket-replication-rev8-spec
// §12.1; the G4 to G7 build doc step 9): the owner's rows carry InSync, State
// and the pending copies, read from the owner's memory with no request to
// another member.

// rowOf is the row of bucket in node index's cluster-wide listing of database.
func rowOf(t *testing.T, tc *testCluster, index int, database, bucket string) BucketInfo {
	t.Helper()
	row, listed := rowIfListed(t, tc, index, database, bucket)
	if !listed {
		t.Fatalf("node %d does not list bucket %q of %q", index+1, bucket, database)
	}
	return row
}

// rowIfListed is rowOf for a poll: a bucket not listed yet is no failure.
func rowIfListed(t *testing.T, tc *testCluster, index int, database, bucket string) (BucketInfo, bool) {
	t.Helper()
	for _, row := range listBucketsAt(t, tc, index, database).Buckets {
		if row.Name == bucket {
			return row, true
		}
	}
	return BucketInfo{}, false
}

// ownerIndexIn is the index of the node that owns database.bucket.
func ownerIndexIn(t *testing.T, tc *testCluster, database, bucket string) int {
	t.Helper()
	owner, err := clusterOf(tc, 0).Owner(placementOf(database, bucket))
	if err != nil {
		t.Fatalf("placing %s.%s: %v", database, bucket, err)
	}
	return nodeIndexByAddr(t, tc.addrs, owner.ID)
}

// countReplicationRequests counts, until the returned stop is called, the
// requests this process's nodes send that ask for an answer: the exchanges an
// operation costs. It returns the count so far.
func countReplicationRequests(t *testing.T) (count func() int64) {
	t.Helper()
	var asked atomic.Int64
	hook := func(req *ReplPushRequest) {
		if req.ReturnResponse {
			asked.Add(1)
		}
	}
	previous := beforePeerCallHook.Swap(&hook)
	t.Cleanup(func() { beforePeerCallHook.Store(previous) })
	return asked.Load
}

// TestShowBucketsSendsNoInternodeRequest is T-41: a cluster-wide listing costs
// one exchange per other member whatever it lists, the counts of the owner's
// row come in that same exchange, and a node that holds a copy but does not
// own the bucket prints its own "owner elsewhere" row, not the owner's counts
// (the documented limit of the merge, SA-14).
func TestShowBucketsSendsNoInternodeRequest(t *testing.T) {
	tc, _ := listingCluster(t)
	defer tc.stop()
	owner := ownerIndexIn(t, tc, listingDatabase, sharedBucket)
	pollUntil(t, "the owner's row of the shared bucket counts three copies", func() bool {
		row, listed := rowIfListed(t, tc, owner, listingDatabase, sharedBucket)
		return listed && row.InSync == 3
	})
	asked := countReplicationRequests(t)

	for index := range tc.nodes {
		before := asked()
		rows := listBucketsAt(t, tc, index, listingDatabase).Buckets
		if exchanges := asked() - before; exchanges != int64(len(tc.nodes)-1) {
			t.Errorf("node %d's listing of %d buckets cost %d exchanges, want %d: one per other member and none for the counts",
				index+1, len(rows), exchanges, len(tc.nodes)-1)
		}
		shared := rowOf(t, tc, index, listingDatabase, sharedBucket)
		if index == owner {
			if shared.State != StateOK || shared.InSync != 3 {
				t.Errorf("the owner, node %d, lists the shared bucket as %q with %d in sync, want ok and 3", index+1, shared.State, shared.InSync)
			}
			continue
		}
		if shared.State != StateOwnerElsewhere || shared.InSync != 0 || len(shared.PendingCopies) != 0 {
			t.Errorf("holder node %d lists the shared bucket as %q with %d in sync and pending %v, want its own owner-elsewhere row",
				index+1, shared.State, shared.InSync, shared.PendingCopies)
		}
	}

	// A node that holds nothing of a bucket lists the owner's row, counts and all.
	solo := ownerIndexIn(t, tc, soloDatabase, "only")
	for index := range tc.nodes {
		if index == solo {
			continue
		}
		row := rowOf(t, tc, index, soloDatabase, "only")
		if row.State != StateOK || row.InSync != 1 {
			t.Errorf("node %d, which holds nothing of solo.only, lists %q with %d in sync, want the owner's row: ok and 1", index+1, row.State, row.InSync)
		}
	}
}

// TestTheOwnersRowSaysWhyACopyIsMissing pins the three reasons a row is not
// "ok": a stalled addressee is named, a factor above the member count says how
// many members there are, and a node with no one to copy to says standalone.
func TestTheOwnersRowSaysWhyACopyIsMissing(t *testing.T) {
	t.Run("a stalled addressee is named", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		bucket := bucketOwnedBy(t, tc, 0, "stalled")
		createBucketAt(tc, 0, testDatabase, bucket, 3)
		waitForServing(t, tc, testDatabase, bucket, 3)
		pollUntil(t, "node 1 lists three copies in sync", func() bool {
			row, listed := rowIfListed(t, tc, 0, testDatabase, bucket)
			return listed && row.InSync == 3
		})

		stalled := peerOf(t, tc, 0, 2)
		stalled.streamMu.Lock()
		stalled.leaveInSyncLocked(errors.New("stalled for this test").Error(), "")
		stalled.streamMu.Unlock()

		row := rowOf(t, tc, 0, testDatabase, bucket)
		if row.State != StateUnderReplicated || row.InSync != 2 || !slices.Equal(row.NotInSync, []string{tc.addrs[2]}) {
			t.Errorf("the owner lists %q, %d in sync, not in sync %v, want under-replicated, 2 and [%s]", row.State, row.InSync, row.NotInSync, tc.addrs[2])
		}
	})

	t.Run("fewer members than the factor", func(t *testing.T) {
		tc := newTestCluster(t, 2, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		bucket := bucketOwnedBy(t, tc, 0, "short")
		// A create refuses a factor above the member count, so the bucket
		// inherits the nodes' factor of 3.
		createBucketAt(tc, 0, testDatabase, bucket, 0)
		pollUntil(t, "node 1 lists the bucket under-replicated", func() bool {
			row, listed := rowIfListed(t, tc, 0, testDatabase, bucket)
			return listed && row.State == StateUnderReplicated && row.InSync == 2 && len(row.NotInSync) == 0
		})
	})

	t.Run("no one to copy to", func(t *testing.T) {
		s := newTestServerWith(t, nil)
		createTestBucket(t, s, "app", "solo", BucketTypeHash)
		listing, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
		if err != nil {
			t.Fatalf("listing: %v", err)
		}
		if row := listing.Buckets[0]; row.State != StateStandalone || row.InSync != 1 {
			t.Errorf("a node with no cluster lists %q with %d in sync, want standalone and 1", row.State, row.InSync)
		}
	})
}

// TestPendingCopiesAreListedWithAutoRebalanceOffInTheOwnersRow is T-176's G7
// half (SA-5): with auto-rebalance off the copy stays pending, and the owner's
// row says so, names the member and carries the time the alter applied, so the
// CLI can print the age. A node that is not the owner says only that.
func TestPendingCopiesAreListedWithAutoRebalanceOffInTheOwnersRow(t *testing.T) {
	tc, buckets, member, _ := pendingOnAMemberWithAutoRebalanceOff(t, 1, "listed")
	defer tc.stop()
	bucket := buckets[0]
	started := time.Now().Unix()

	row := rowOf(t, tc, 0, testDatabase, bucket)
	if row.State != StateCopying || row.InSync != 2 {
		t.Errorf("the owner lists %q with %d in sync, want copying and 2: the pending member counts toward neither", row.State, row.InSync)
	}
	if len(row.PendingCopies) != 1 || row.PendingCopies[0].Node != tc.addrs[member] {
		t.Fatalf("the owner lists pending copies %+v, want only node %d", row.PendingCopies, member+1)
	}
	if since := row.PendingCopies[0].Since; since < started-60 || since > started+1 {
		t.Errorf("the pending copy's time is %d, want about %d: when the alter applied", since, started)
	}

	// The member holds nothing yet, so its listing carries the owner's row.
	if memberRow := rowOf(t, tc, member, testDatabase, bucket); memberRow.State != StateCopying || len(memberRow.PendingCopies) != 1 {
		t.Errorf("node %d, still to receive its copy, lists %q with pending %v, want the owner's row: copying, one pending copy",
			member+1, memberRow.State, memberRow.PendingCopies)
	}
}
