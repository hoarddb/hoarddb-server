package server

import (
	"bytes"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// forgetIncarnationOf leaves receiver holding, for source, what a build from
// before the handshake recorded: a position with no incarnation and no log id.
// It is what loading such a 'p' record leaves in memory
// (replication_positions.go), set by hand because this build cannot write one.
func forgetIncarnationOf(receiver *Cluster, source string) {
	receiver.appliedMu.Lock()
	defer receiver.appliedMu.Unlock()
	receiver.recordedStream[source] = recordedStream{}
}

// TestAnIdlessPositionMeetingAFreshChainIsRebasedFromZero: A peer
// that recorded its position in this node's log before the handshake existed
// holds it with no incarnation id. If this node's log was then re-created — its
// wal/ wiped or lost across the upgrade — the chain has no legacy link, and the
// record is a position in a log that is gone: the wipe row, rebased to
// valid_through 0. On the starting tree the handshake refused it with "this
// build does not write such a record", on every attempt, so the stream to that
// peer never opened again.
func TestAnIdlessPositionMeetingAFreshChainIsRebasedFromZero(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := 1 - ownerIndex
	for _, key := range []string{"before-a", "before-b", "before-c"} {
		if frame := tc.writeConcerned(ownerIndex, key, "all"); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at all: %s", key, frame.Payload)
		}
	}
	owner := tc.nodes[ownerIndex].server.core
	replica := tc.nodes[replicaIndex].server.core
	ownerID := tc.addrs[ownerIndex]
	chain := owner.wal.Incarnations()
	if chain[0].Legacy {
		t.Fatalf("setup: a test cluster's log was created by this build, and its first link is legacy")
	}

	recorded := replica.cluster.recordedPosition(ownerID)
	if !recorded.Known || recorded.AppliedThrough == 0 {
		t.Fatalf("setup: the replica holds no position for the owner after three writes at all: %+v", recorded)
	}
	idless := ReplPosition{Known: true, AppliedThrough: recorded.AppliedThrough}
	rebase, err := streamTo(t, tc, ownerIndex, replicaIndex).judge(idless, chain)
	if err != nil {
		t.Fatalf("an id-less position, against a chain with no legacy link, was refused: %v; "+
			"want the wipe row, a rebase to valid_through 0", err)
	}
	if rebase == nil || rebase.ValidThrough != 0 || rebase.StartAt != 0 {
		t.Fatalf("an id-less position, against a chain with no legacy link, was judged %+v; want a rebase to valid_through 0", rebase)
	}

	forgetIncarnationOf(replica.cluster, ownerID)
	if frame := tc.writeConcerned(ownerIndex, "after-the-idless-record", "1"); frame.Opcode == transport.OpError {
		t.Fatalf("write after the replica's record lost its incarnation: %s", frame.Payload)
	}
	current := currentIncarnation(tc, ownerIndex)
	pollUntil(t, "the owner's stream to the replica has opened again, under the owner's current incarnation", func() bool {
		return bytes.Equal(replica.cluster.recordedPosition(ownerID).Incarnation, current)
	})
	pollUntil(t, "the replica holds the write made after its record lost its incarnation", func() bool {
		return holdsKey(t, replica, testDatabase, testBucket, "after-the-idless-record")
	})
}
