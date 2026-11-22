package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0686: a mutation of a bucket that exists nowhere is NOT_FOUND with the
// standard missing-bucket text, at the bucket's owner and through any other
// node, as BUG-0685 made the alter. It was UNAVAILABLE ("has no local copy"),
// the class a driver retries until its deadline.

// clientFrameAt sends a request to node index through its cluster's handler
// with no session, as alterFrame does, and returns the answer frame.
func clientFrameAt(t *testing.T, tc *testCluster, index int, op transport.Opcode, req any) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("encoding %s: %v", op, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := tc.nodes[index].server.core.cluster.handleClientFrame(ctx, transport.NewFrame(op, payload))
	if err != nil {
		t.Fatalf("node %d's handler failed %s: %v", index+1, op, err)
	}
	return frame
}

// requireNoMutationLogged fails for any entry of op for bucket in any running
// node's log.
func requireNoMutationLogged(t *testing.T, tc *testCluster, op transport.Opcode, bucket string) {
	t.Helper()
	for index, node := range tc.nodes {
		if node.server == nil {
			continue
		}
		if logged := walOpCount(t, node.server.core.wal, op, bucket); logged != 0 {
			t.Errorf("node %d logged %d refused %s of bucket %q", index+1, logged, op, bucket)
		}
	}
}

// missingBucketMutations are the mutations the fence meets at an owner that
// holds no copy: a hash put and delete, an update, a queue push, a field-store
// write and a blob write.
func missingBucketMutations(bucket string) []struct {
	name string
	op   transport.Opcode
	req  any
} {
	document := map[string]interface{}{"n": 1}
	return []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"put", transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: document}},
		{"delete", transport.OpDelete, DeleteRequest{Database: testDatabase, Bucket: bucket, Key: "k"}},
		{"update", transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Key: "k", Set: document}},
		{"fifo push", transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: document}},
		{"field push", transport.OpFieldPush, FieldRequest{Database: testDatabase, Bucket: bucket, Key: "k", Field: "Purchases", Data: document}},
		{"blob write", transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: []byte("x")}},
	}
}

func TestAWriteToAMissingBucketIsNotFoundAtTheOwnerAndThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "ghost")

	for _, mutation := range missingBucketMutations(bucket) {
		t.Run(mutation.name+" at the owner", func(t *testing.T) {
			requireNotFoundNotApplied(t, clientFrameAt(t, tc, 0, mutation.op, mutation.req), bucket)
		})
		t.Run(mutation.name+" through a non-owner", func(t *testing.T) {
			requireNotFoundNotApplied(t, clientFrameAt(t, tc, 1, mutation.op, mutation.req), bucket)
		})
		requireNoMutationLogged(t, tc, mutation.op, bucket)
	}
}

// TestAReadOfAMissingBucketIsNotFoundAtTheOwnerAndThroughANonOwner pins the
// read side, which already answered NOT_FOUND when BUG-0686 was filed: the
// retryable answer must not spread to it.
func TestAReadOfAMissingBucketIsNotFoundAtTheOwnerAndThroughANonOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "ghost")

	reads := []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"get", transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}},
		{"count", transport.OpCount, CountRequest{Database: testDatabase, Bucket: bucket}},
		{"peek", transport.OpFIFOPeek, PeekRequest{Database: testDatabase, Bucket: bucket}},
		{"field get", transport.OpFieldGet, FieldRequest{Database: testDatabase, Bucket: bucket, Key: "k", Field: "Friends", Element: "e"}},
	}
	for _, read := range reads {
		for _, node := range []struct {
			name  string
			index int
		}{{"at the owner", 0}, {"through a non-owner", 1}} {
			t.Run(read.name+" "+node.name, func(t *testing.T) {
				frame := clientFrameAt(t, tc, node.index, read.op, read.req)
				refused := refusalOf(t, frame)
				if refused.Code != int(transport.ErrNotFound) {
					t.Errorf("code %d, want NOT_FOUND (%d): %q", refused.Code, transport.ErrNotFound, refused.Message)
				}
			})
		}
	}
}

// TestAWriteIsRetriedNotNotFoundWhenAMemberHoldsTheBucket guards the fix's
// other side: an owner that has not copied a bucket a member holds, and one
// whose pull is in flight, keep the retryable refusal (R8-D).
func TestAWriteIsRetriedNotNotFoundWhenAMemberHoldsTheBucket(t *testing.T) {
	switchJoinFreezeOff(t)
	fixture := setUpJoinerReadingUnpulledBuckets(t)
	put := func(bucket string) *transport.Frame {
		return fixture.tc.frameOn(fixture.joinerNode, transport.OpPut,
			InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 2}})
	}

	t.Run("a member holds a copy the owner has not pulled", func(t *testing.T) {
		requireRetryRefusal(t, put(fixture.hashBucket), "a put at an owner beside a member's copy")
	})
	t.Run("the pull is in flight", func(t *testing.T) {
		done, release := fixture.holdPull(t, fixture.landingBucket)
		defer awaitPull(t, done, release)
		requireRetryRefusal(t, put(fixture.landingBucket), "a put at an owner whose pull is in flight")
	})
}

// TestAWriteToAMissingNameIsRetriedNotNotFoundWhileAMemberIsUnheard is the
// alter's edge for a put (BUG-0685): an owner that cannot hear from every
// member must not call the bucket not found, since the unheard member may hold
// it. Guard: read an unheard member as "holds none".
func TestAWriteToAMissingNameIsRetriedNotNotFoundWhileAMemberIsUnheard(t *testing.T) {
	tc, owner, moved, downID := unheardMemberFixture(t)
	refused := requireRetryRefusal(t, clientFrameAt(t, tc, owner, transport.OpPut,
		InsertRequest{Database: testDatabase, Bucket: moved, Key: "k", Data: map[string]interface{}{"n": 1}}),
		"a put to a name whose set changed while a member is unheard")
	if !strings.Contains(refused.Message, downID) {
		t.Errorf("the refusal does not name the unheard member %s: %s", downID, refused.Message)
	}
}

// TestAWriteIsRetriedNotNotFoundWhileAPullIsInFlightAndAMemberIsUnheard pins
// the pull branch of the miss-path question. Three founders at factor 2 that
// never changed, one stopped: the name's set is unchanged since the settled
// floor, so the unheard member is passed over and the survey alone reads the
// bucket as not found (founder_settled_test.go). A pull of the name in flight
// on the owner must still answer retry. Guard: drop the pullPending check from
// refuseUnheldBesideCopy and the marked put answers NOT_FOUND. The unmarked put
// is the control: the same node, name and stopped member answer not found.
func TestAWriteIsRetriedNotNotFoundWhileAPullIsInFlightAndAMemberIsUnheard(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	const down = 2
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	placed := ringWith(t, 2, tc.addrs...)
	running := func(name string) bool { return ownerOn(placed, name) != tc.addrs[down] }
	pulled := bucketNamed(t, "pulled", running)
	absent := bucketNamed(t, "absent", func(name string) bool { return running(name) && name != pulled })
	eventually(func() bool {
		return ownSettledFloor(clusterOf(tc, 0)) == InitialEpoch && ownSettledFloor(clusterOf(tc, 1)) == InitialEpoch
	})
	tc.stopNodeKeepingData(down)
	owner := func(name string) int { return nodeIndexByAddr(t, tc.addrs, ownerOn(placed, name)) }
	put := func(bucket string) *transport.Frame {
		return clientFrameAt(t, tc, owner(bucket), transport.OpPut,
			InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	}

	requireNotFoundNotApplied(t, put(absent), absent)

	pullingNode := clusterOf(tc, owner(pulled))
	marker := migrateKey(testDatabase, pulled)
	pullingNode.migrateMu.Lock()
	pullingNode.migrateActive[marker] = make(chan struct{})
	pullingNode.migrateMu.Unlock()
	defer func() {
		pullingNode.migrateMu.Lock()
		delete(pullingNode.migrateActive, marker)
		pullingNode.migrateMu.Unlock()
	}()
	requireRetryRefusal(t, put(pulled), "a put at an owner whose pull is in flight, with a member unheard")
}
