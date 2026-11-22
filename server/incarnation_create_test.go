package server

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.6 (R8-A, R8-E; review M-7): a holder refuses a create
// of another incarnation of a bucket it holds, and the sender stalls on it.

// appliedFromSource is what c has recorded as applied from source.
func appliedFromSource(c *Cluster, source string) uint64 {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	return c.appliedFrom[source]
}

// TestACreateOfAnotherIncarnationIsRefusedByAHolder: a create of the same name
// and database generation as a held copy, but another incarnation, is pushed
// to the holder. It is refused, the copy is untouched, the refusal is recorded
// and does not move the holder's position for the sender, so it never counts
// toward the create's concern. Guard: answer "already exists".
func TestACreateOfAnotherIncarnationIsRefusedByAHolder(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const bucket = "incarnate"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	owner := ownerIndexOf(t, tc, bucket)
	replica := (owner + 1) % 3
	held := tc.nodes[replica].server.core
	generation, live, err := held.openBucketIdentity(testDatabase, bucket)
	if err != nil || live == 0 {
		t.Fatalf("setup: the held copy's identity: %d %d %v", generation, live, err)
	}
	other := live - 1
	if live == 1 {
		other = math.MaxInt64
	}
	payload, err := bson.Marshal(bson.D{
		{Key: "database", Value: testDatabase}, {Key: "name", Value: bucket}, {Key: "type", Value: string(BucketTypeHash)},
		{Key: "db_gen", Value: generation}, {Key: "dropped_generations", Value: []int64{}},
		{Key: "term", Value: int64(InitialEpoch)}, {Key: "incarnation", Value: int64(other)},
	})
	if err != nil {
		t.Fatal(err)
	}
	const seq = 1 << 40
	source := tc.addrs[owner]
	c := clusterOf(tc, replica)
	_, err = c.applyAsReplica(context.Background(), ReplEntry{Seq: seq, Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: payload}, true, source, true)
	if !errors.Is(err, errAnotherIncarnation) {
		t.Fatalf("a create of incarnation %d pushed to a holder of incarnation %d was answered %v; want the another_incarnation refusal", other, live, err)
	}
	if got := lineageOf(t, held, testDatabase, bucket).Incarnation; got != live {
		t.Errorf("the held copy is incarnation %d after the refused create, want %d", got, live)
	}
	if resp, err := held.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}); err != nil || !resp.Found {
		t.Errorf("the held copy lost its document to the refused create: %v", err)
	}
	if position := appliedFromSource(c, source); position >= seq {
		t.Errorf("the holder recorded the refused create as applied from %s (position %d): it would count toward the create's concern", source, position)
	}
	if record, recorded := c.divergenceFrom(source); !recorded || record.Seq != seq || record.Reason != refusalAnotherIncarnation {
		t.Errorf("the refusal is not recorded for %s at sequence %d: %+v %v", source, seq, record, recorded)
	}
}

// TestAStraysWritesNeverReachAHolderOfAnotherIncarnation (review M-7): a joiner
// owns a bucket it has not copied, and creates it anyway — the join freeze and
// the owner's question both forced off — so the members' copies and the
// joiner's are two incarnations. The members refuse the stray create; a write
// of the stray's at `one` then never reaches their copies, and the joiner's
// stream to them stalls on the recorded refusal, before and after a holder
// restarts. Arms: a FIFO stray over a hash bucket, and a hash stray over a
// hash bucket. Guard: acknowledge the refusal and move on — the same-type
// arm's put is then stored in the holders' copy.
func TestAStraysWritesNeverReachAHolderOfAnotherIncarnation(t *testing.T) {
	for _, arm := range []struct {
		name  string
		stray BucketType
	}{{"a FIFO stray over a hash bucket", BucketTypeFIFO}, {"a hash stray over a hash bucket", BucketTypeHash}} {
		t.Run(arm.name, func(t *testing.T) {
			switchJoinFreezeOff(t)
			ownerQuestionPassOverHook.Store(true)
			defer ownerQuestionPassOverHook.Store(false)
			var bucket string
			s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
				placed := ring.New(3)
				for _, node := range joined {
					if err := placed.AddNode(node); err != nil {
						t.Fatal(err)
					}
				}
				joiner := joined[len(joined)-1].ID
				bucket = bucketNamed(t, "stray", func(name string) bool {
					owner, err := placed.Owner(placementOf(testDatabase, name).encoded)
					return err == nil && owner.ID == joiner
				})
				return []string{bucket}
			})
			joinerID := s.joiner.self.ID
			before := make([]int64, len(s.tc.nodes))
			for index := range s.tc.nodes {
				before[index] = s.tc.bucketCountOn(index, testDatabase, bucket)
			}

			s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, bson.M{
				"database": testDatabase, "name": bucket, "type": string(arm.stray), "write_concern": "one"})
			if !s.joiner.s.bucketStoreOpen(testDatabase, bucket) {
				t.Fatal("setup: the joiner did not create its stray copy")
			}
			if arm.stray == BucketTypeFIFO {
				s.tc.frameOn(s.joinerNode, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"stray": 1}, WriteConcern: "one"})
			} else {
				s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "stray", Data: map[string]interface{}{"stray": 1}, WriteConcern: "one"})
			}

			untouched := func(when string) {
				t.Helper()
				for index, node := range s.tc.nodes {
					holder := node.server.core
					if record, recorded := holder.cluster.divergenceFrom(joinerID); !recorded || record.Reason != refusalAnotherIncarnation {
						t.Errorf("%s: node %d has no another_incarnation record for the joiner's stream: %+v", when, index+1, record)
					}
					if got := s.tc.bucketCountOn(index, testDatabase, bucket); got != before[index] {
						t.Errorf("%s: node %d holds %d documents of %s, %d before the stray: the stray's write reached it", when, index+1, got, bucket, before[index])
					}
					if resp, err := holder.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: "stray"}); err == nil && resp.Found {
						t.Errorf("%s: node %d stores the stray's document", when, index+1)
					}
				}
			}
			// Waited for, not required: untouched says what is wrong if the
			// members never refuse.
			eventually(func() bool {
				for _, node := range s.tc.nodes {
					if _, recorded := node.server.core.cluster.divergenceFrom(joinerID); !recorded {
						return false
					}
				}
				return true
			})
			time.Sleep(time.Second)
			untouched("after the refused create")

			s.tc.restartNode(0)
			time.Sleep(2 * time.Second)
			untouched("after a restart of a holder")
		})
	}
}
