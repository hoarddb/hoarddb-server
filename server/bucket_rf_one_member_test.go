package server

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// oneMemberAlterBuckets is how many buckets each case alters: enough that the
// joiner owns several of them on the joined ring, so a founder copy wrongly
// marked outside holds the join's freeze.
const oneMemberAlterBuckets = 20

// TestAnAlterOnAOneMemberClusterLeavesItsCopyServedAndTheJoinSettles is
// BUG-0623. A cluster node that is the ring's only member does not route, so
// a client's alter reaches handleAlterBucket through fencedDispatch with
// nothing stamped. Taken down the member's path, its epoch 0 finds no ring
// (row 6), the node's only copy is marked outside its set, the answer says
// previous_factor 0, and the next join's freeze never lifts. Each case is a
// lone node in one of the two ways a cluster gets there (a founder before any
// join, the survivor of a remove-node), altering every bucket by the
// operator's natural action there: to the factor it already has, or lower.
// Guard: an alter applied as a member's marks the copy outside (Outside != 0)
// and answers previous_factor 0, and the join that follows does not settle.
func TestAnAlterOnAOneMemberClusterLeavesItsCopyServedAndTheJoinSettles(t *testing.T) {
	cases := []struct {
		name        string
		members     int
		defaultRF   int
		alterTo     int32
		removeToOne bool
	}{
		{name: "a founder altering to the factor it inherits", members: 1, defaultRF: 1, alterTo: 1},
		{name: "a founder lowering its default of three to one", members: 1, defaultRF: 3, alterTo: 1},
		{name: "the last member after remove-node lowering three to one", members: 2, defaultRF: 3, alterTo: 1, removeToOne: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestCluster(t, tt.members, tt.defaultRF)
			defer tc.stop()
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			s := &settlingCluster{tc: tc}
			for index := 0; index < oneMemberAlterBuckets; index++ {
				name := fmt.Sprintf("lone%03d", index)
				s.buckets = append(s.buckets, name)
				tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
				tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": index}})
			}
			if tt.removeToOne {
				for _, name := range s.buckets {
					waitForServing(t, tc, testDatabase, name, tt.members)
				}
				removeNodeFrom(t, tc, 0, 1)
				tc.stopNodeKeepingData(1)
				// The survivor owns every bucket now and refuses an alter, by design
				// ("ownership is moving to this node ... Retry in a moment"), until its
				// take-over of the bucket has opened a term; wait on that, not a sleep.
				for _, name := range s.buckets {
					waitTakeOverSettled(t, tc.nodes[0].server.core, testDatabase, name)
				}
			}
			lone := clusterOf(tc, 0)
			if lone.Enabled() {
				t.Fatalf("node 1 routes with %d members: the case needs a one-member cluster", len(lone.Members()))
			}

			for _, name := range s.buckets {
				answer := alterOn(t, tc, 0, name, tt.alterTo)
				if !answer.Success {
					t.Fatalf("the alter of %q to %d was refused: %s", name, tt.alterTo, answer.Message)
				}
				if answer.PreviousFactor != int32(tt.defaultRF) {
					t.Errorf("the alter of %q to %d answered previous_factor %d; the bucket had %d",
						name, tt.alterTo, answer.PreviousFactor, tt.defaultRF)
				}
				if len(answer.Pending) != 0 || len(answer.Releasing) != 0 {
					t.Errorf("the alter of %q on a lone node names other members: pending %v, releasing %v", name, answer.Pending, answer.Releasing)
				}
				if marks := lone.placementMarksOf(placementOf(testDatabase, name)); marks.Outside != 0 {
					t.Errorf("the alter of %q marked the lone node's only copy outside its set (epoch %d)", name, marks.Outside)
				}
			}

			s.ports = reserveNodePorts(t)
			s.join(t, 0)
			if !joinSettles(s, founderGrowSettleBound) {
				report := lone.joinFreeze.Load()
				t.Fatalf("the join after the alters did not settle within %v: the lone node's report stalls %v waiting on %v, %d buckets of the change copied",
					founderGrowSettleBound, report.Stalled, report.Awaited, report.Copied)
			}
		})
	}
}

// alterOn sends a client's alter of bucket's replication factor to node index
// and returns the decoded answer.
func alterOn(t *testing.T, tc *testCluster, index int, bucket string, factor int32) AlterBucketResponse {
	t.Helper()
	reply := tc.frame(index, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: factor})
	if reply.Opcode == transport.OpError {
		t.Fatalf("the alter of %q to %d: %s", bucket, factor, reply.Payload)
	}
	var answer AlterBucketResponse
	if err := bson.Unmarshal(reply.Payload, &answer); err != nil {
		t.Fatalf("decoding the answer to the alter of %q: %v", bucket, err)
	}
	return answer
}
