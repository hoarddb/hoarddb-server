package server

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// sourceConn is the connection a puller holds to its source in these tests: it
// hands each request to the source's own MIGRATE_START handler and counts them.
type sourceConn struct {
	peerConn
	source *Server
	asked  int
}

func (conn *sourceConn) SendRequest(ctx context.Context, _ transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	conn.asked++
	return handleMigrateStart(ctx, transport.NewFrame(op, payload), conn.source)
}

// refusedForGoodPair returns a cluster whose owner's stream to the other member
// cannot open and never will: the member holds a position under an incarnation
// of the owner's log that the log does not hold, with no log id (the last row
// of spec section 20.4). It returns the owner, the puller's id and a bucket
// the owner holds.
func refusedForGoodPair(t *testing.T) (tc *testCluster, owner *Server, pullerID, bucket string) {
	t.Helper()
	tc = newTestCluster(t, 2, 2)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	pullerIndex := 1 - ownerIndex
	owner = tc.nodes[ownerIndex].server.core
	pullerID = tc.addrs[pullerIndex]
	toPuller := streamTo(t, tc, ownerIndex, pullerIndex)
	caughtUpWithin(t, toPuller, 20*time.Second)

	foreign := make([]byte, 16)
	if _, err := rand.Read(foreign); err != nil {
		t.Fatalf("setup: reading random bytes: %v", err)
	}
	puller := tc.nodes[pullerIndex].server.core.cluster
	puller.appliedMu.Lock()
	puller.recordedStream[tc.addrs[ownerIndex]] = recordedStream{incarnation: foreign}
	puller.appliedMu.Unlock()
	toPuller.resetStream("test: the handshake is asked again")
	pollUntil(t, "the owner's stream to the puller is stalled on the refusal", func() bool {
		reason, stalled := toPuller.stalledReason()
		return stalled && strings.Contains(reason, "no log id")
	})
	return tc, owner, pullerID, testBucket
}

func startPullFrom(t *testing.T, owner *Server, pullerID, bucket string) (MigrateStartResponse, time.Duration, int) {
	t.Helper()
	conn := &sourceConn{source: owner}
	startReq := func() ([]byte, error) {
		return bson.Marshal(MigrateStartRequest{
			Token: owner.cluster.localToken(), Database: testDatabase, Bucket: bucket, FromNode: pullerID,
			Epoch: owner.cluster.Epoch(), Store: owner.storeClaim(),
		})
	}
	began := time.Now()
	response, err := startPull(context.Background(), conn, startReq, ring.Node{ID: "source"}, func() {})
	if err != nil {
		t.Fatalf("startPull: %v", err)
	}
	return response, time.Since(began), conn.asked
}

// TestAPermanentHandshakeRefusalIsNotReportedAsTriedAgain is BUG-0310, first
// half: a source whose stream to the puller is refused for good answers a pull
// with the refusal and the fix, not with "not yet ... the pull is tried again".
func TestAPermanentHandshakeRefusalIsNotReportedAsTriedAgain(t *testing.T) {
	tc, owner, pullerID, bucket := refusedForGoodPair(t)
	defer tc.stop()

	response, _, _ := startPullFrom(t, owner, pullerID, bucket)
	if response.Ok {
		t.Fatalf("the source served a pull to a puller whose stream cannot open")
	}
	if strings.Contains(response.Error, "tried again") {
		t.Errorf("a refusal that nothing but an operator clears reads as retryable: %s", response.Error)
	}
	for _, want := range []string{pullerID, "no log id", "remove-node"} {
		if !strings.Contains(response.Error, want) {
			t.Errorf("the refusal does not name %q: %s", want, response.Error)
		}
	}
}

// TestAPermanentHandshakeRefusalCostsNoWaitPerBucket is BUG-0310, second half:
// a pull does not wait pullNotYetWait on a source whose refusal is permanent,
// so a sweep over many buckets does not pay it for each.
func TestAPermanentHandshakeRefusalCostsNoWaitPerBucket(t *testing.T) {
	tc, owner, pullerID, bucket := refusedForGoodPair(t)
	defer tc.stop()

	for range 3 {
		_, waited, asked := startPullFrom(t, owner, pullerID, bucket)
		if waited >= pullNotYetWait/5 || asked != 1 {
			t.Fatalf("a pull from a source refused for good asked %d times and waited %v; want one ask and no wait", asked, waited)
		}
	}
}

// TestARefusalForGoodEndsWithTheNextFailureThatIsNot: a stream whose latest
// attempt failed for another reason (the peer is down, say) is "not yet"
// again, so a pull waits for it as before.
func TestARefusalForGoodEndsWithTheNextFailureThatIsNot(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	toPeer := streamTo(t, tc, 0, 1)

	toPeer.markFailed(errHandshakeRefusedForGood)
	if toPeer.handshakeRefusedForGood() == nil {
		t.Fatalf("a refusal for good was not recorded")
	}
	toPeer.markFailed(context.DeadlineExceeded)
	if refusal := toPeer.handshakeRefusedForGood(); refusal != nil {
		t.Errorf("a later failure of another kind left the refusal in place: %v", refusal)
	}
}

// TestASweepAsksARefusingSourceOnceNotOncePerBucket is BUG-0310, criterion 1:
// a pass that meets a source refused for good skips that source's other
// buckets, so the cost of the refusal is one ask per source, not one per
// bucket.
func TestASweepAsksARefusingSourceOnceNotOncePerBucket(t *testing.T) {
	tc, owner, _, _ := refusedForGoodPair(t)
	defer tc.stop()
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	pullerIndex := 1 - ownerIndex

	const buckets = 12
	var held []heldBucket
	ownerCluster := owner.cluster
	for attempt := 0; len(held) < buckets && attempt < 2000; attempt++ {
		name := fmt.Sprintf("swept%04d", attempt)
		if node, err := ownerCluster.Owner(placementOf(testDatabase, name)); err == nil && node.ID == ownerCluster.self.ID {
			createBucketAtOne(tc, ownerIndex, testDatabase, name)
			held = append(held, heldBucket{database: testDatabase, bucket: name})
		}
	}
	if len(held) < buckets {
		t.Fatalf("setup: found %d bucket names owned by the refusing source, want %d", len(held), buckets)
	}

	var asked atomic.Int32
	counting := func(database, bucket string) {
		if slices.Contains(held, heldBucket{database: database, bucket: bucket}) {
			asked.Add(1)
		}
	}
	pullStartedHook.Store(&counting)
	defer pullStartedHook.Store(nil)

	puller := tc.nodes[pullerIndex].server.core.cluster
	left := puller.sweepPass(context.Background(), sweepRound{}, held)
	if len(left) != buckets {
		t.Fatalf("the pass settled %d of %d buckets from a source refused for good; want none settled", buckets-len(left), buckets)
	}
	if got := asked.Load(); got != 1 {
		t.Errorf("one pass over %d buckets asked the refusing source %d times; want 1", buckets, got)
	}
}

// TestTheLostChainRefusalNamesTheSourceNotThisNode: the refusal is read on the
// puller (migrate's output, the sweep's WARN) as well as on the source, so
// "this node" would send an operator to empty the puller's data directory,
// which may hold the only good copy. It names the source, and the command that
// has its id.
func TestTheLostChainRefusalNamesTheSourceNotThisNode(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	toPuller := streamTo(t, tc, 0, 1)
	sourceID, pullerID := tc.addrs[0], tc.addrs[1]

	chain := tc.nodes[0].server.core.wal.Incarnations()
	chain[0].Legacy = true
	foreign := make([]byte, 16)
	if _, err := rand.Read(foreign); err != nil {
		t.Fatalf("setup: reading random bytes: %v", err)
	}
	_, err := toPuller.judge(ReplPosition{Known: true, AppliedThrough: 1, Incarnation: foreign}, chain)
	if err == nil {
		t.Fatalf("a position under an unknown incarnation, against a chain with a legacy first link, was not refused")
	}
	text := err.Error()
	for _, want := range []string{
		sourceID + "'s wal/incarnation.bson is missing",
		"Restore that file on " + sourceID,
		"'remove-node " + sourceID + "'",
		"empty " + sourceID + "'s data directory",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the lost-chain refusal does not say %q: %s", want, text)
		}
	}
	if strings.Contains(text, "this node") {
		t.Errorf("the lost-chain refusal says \"this node\", which the puller reads as itself: %s", text)
	}
	if strings.Contains(text, "empty "+pullerID) {
		t.Errorf("the lost-chain refusal sends the operator to empty the puller %s: %s", pullerID, text)
	}
}
