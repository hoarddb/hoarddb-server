package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Owner ruling 53 (option C), exit (i): a holder that refused a create of
// another incarnation stalls the sender's stream (10a.6, review M-7). When that
// sender's stream carries a drop of the refused incarnation, the stall ends on
// its own: the holder never applies the stuck create, its another_incarnation
// record is gone, and replication from the sender resumes for every bucket.
// Nothing is lost — the holder's copy is another incarnation, and it keeps it.

// TestADropOfTheRefusedIncarnationEndsTheStall is the 10a.3–10a.6 verifier's
// probe: a joiner that owns a bucket it has not copied creates it anyway (the
// join freeze and the owner's question forced off), both holders refuse the
// stray create and record it, and the bucket is then dropped through the
// joiner. Within a bounded wait neither holder has the record, both still hold
// their own copy with its documents and none of the stray's, and a write at
// `all` to another bucket the joiner owns reaches both — after a holder
// restarts too. Guard: ship the stuck create and the stray's entries to a
// holder that refused it (no skip at the sender) — the record stands and the
// other bucket's create never reaches the holders.
//
// Arms (the coordinator's ruling on the 10a verification, B-1): the bucket
// dropped through the joiner; its database dropped through the joiner, whose
// stream carries that drop; and its database dropped through another member,
// whose stream does. A database drop whose generations cover the create's
// drops the refused incarnation too: the sender then ships the create to
// nobody (G-10: its generation is dropped there), and the holder's apply of
// the database drop, past the create, settles the record. Guard: database DDL
// applied past a recorded divergence does not settle it — both database arms
// keep the record.
func TestADropOfTheRefusedIncarnationEndsTheStall(t *testing.T) {
	for _, exit := range []string{"bucket drop through the owner", "database drop through the owner", "database drop through another member"} {
		t.Run(exit, func(t *testing.T) { runRefusedIncarnationExit(t, exit) })
	}
}

func runRefusedIncarnationExit(t *testing.T, exit string) {
	switchJoinFreezeOff(t)
	ownerQuestionPassOverHook.Store(true)
	defer ownerQuestionPassOverHook.Store(false)
	var bucket, other string
	s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
		placed := ringOf(t, joined, 3)
		joiner := joined[len(joined)-1].ID
		bucket = bucketNamed(t, "stray", func(name string) bool { return ownerOn(placed, name) == joiner })
		other = bucketNamed(t, "after", func(name string) bool { return ownerOn(placed, name) == joiner })
		return []string{bucket}
	})
	joinerID := s.joiner.self.ID
	// Each holder's own document, written into its copy alone: the joiner
	// owns the bucket and holds none, so a client's write would be refused.
	for index, node := range s.tc.nodes {
		if _, err := node.server.core.Insert(InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held", Data: map[string]interface{}{"n": index}}); err != nil {
			t.Fatalf("setup: writing node %d's own document: %v", index+1, err)
		}
	}
	before := make([]int64, len(s.tc.nodes))
	for index := range s.tc.nodes {
		before[index] = s.tc.bucketCountOn(index, testDatabase, bucket)
	}

	s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, bson.M{"database": testDatabase, "name": bucket, "type": string(BucketTypeHash),
		"write_concern": "one"})
	if !s.joiner.s.bucketStoreOpen(testDatabase, bucket) {
		t.Fatal("setup: the joiner did not create its stray copy")
	}
	s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "stray",
		Data: map[string]interface{}{"stray": 1}, WriteConcern: "one"})
	recorded := func() bool {
		for _, node := range s.tc.nodes {
			if record, recorded := node.server.core.cluster.divergenceFrom(joinerID); !recorded || record.Reason != refusalAnotherIncarnation {
				return false
			}
		}
		return true
	}
	if !eventually(recorded) {
		t.Fatal("setup: the holders never recorded the refused stray create")
	}

	var dropped *transport.Frame
	switch exit {
	case "bucket drop through the owner":
		dropped = s.tc.frameOn(s.joinerNode, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": bucket, "write_concern": "one"})
	case "database drop through the owner":
		dropped = s.tc.frameOn(s.joinerNode, transport.OpDeleteDB, bson.M{"name": testDatabase, "write_concern": "one"})
	default:
		dropped = s.tc.frame(0, transport.OpDeleteDB, bson.M{"name": testDatabase, "write_concern": "one"})
	}
	databaseDropped := exit != "bucket drop through the owner"
	if dropped.Opcode == transport.OpError {
		t.Fatalf("setup: the drop through the joiner was refused: %s", errorMessageOf(t, dropped))
	}
	cleared := func() bool {
		for _, node := range s.tc.nodes {
			if _, recorded := node.server.core.cluster.divergenceFrom(joinerID); recorded {
				return false
			}
		}
		return true
	}
	if !eventually(cleared) {
		for index, node := range s.tc.nodes {
			if record, recorded := node.server.core.cluster.divergenceFrom(joinerID); recorded {
				t.Errorf("node %d still records the refused create from the joiner: %+v", index+1, record)
			}
		}
		t.Fatalf("a %s did not end the stall (ruling 53)", exit)
	}

	var holdsItsOwnCopy func(when string)
	holdsItsOwnCopy = func(when string) {
		t.Helper()
		for index, node := range s.tc.nodes {
			if !node.server.core.bucketStoreOpen(testDatabase, bucket) {
				t.Errorf("%s: node %d lost its own copy of %s to the stray's drop", when, index+1, bucket)
				continue
			}
			if got := s.tc.bucketCountOn(index, testDatabase, bucket); got != before[index] {
				t.Errorf("%s: node %d holds %d documents of %s, %d before the stray", when, index+1, got, bucket, before[index])
			}
			if resp, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: "stray"}); err == nil && resp.Found {
				t.Errorf("%s: node %d stores the stray's document", when, index+1)
			}
		}
	}
	if databaseDropped {
		// The database is gone everywhere; a new one of the name carries the
		// later bucket, through the joiner's stream.
		holdsItsOwnCopy = func(string) {}
		s.tc.frameOn(s.joinerNode, transport.OpCreateDB, bson.M{"name": testDatabase, "write_concern": "one"})
	}
	holdsItsOwnCopy("after the drop")

	s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, bson.M{"database": testDatabase, "name": other, "type": string(BucketTypeHash),
		"write_concern": "one"})
	reaches := func(when, key string) {
		t.Helper()
		put := s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: other, Key: key,
			Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
		if put.Opcode == transport.OpError {
			t.Fatalf("%s: the put to %s through the joiner was refused: %s", when, other, errorMessageOf(t, put))
		}
		for index, node := range s.tc.nodes {
			holder := node.server.core
			if !eventually(func() bool {
				resp, err := holder.Find(FindRequest{Database: testDatabase, Bucket: other, Key: key})
				return err == nil && resp.Found
			}) {
				t.Errorf("%s: the joiner's write to %s never reached node %d: its stream is still stalled", when, other, index+1)
			}
		}
	}
	reaches("after the drop", "first")

	s.tc.restartNode(0)
	time.Sleep(time.Second)
	if record, recorded := s.tc.nodes[0].server.core.cluster.divergenceFrom(joinerID); recorded {
		t.Errorf("after a restart node 1 records the refused create again: %+v", record)
	}
	holdsItsOwnCopy("after a restart of a holder")
	reaches("after a restart of a holder", "second")
}

// errorMessageOf decodes an error frame's message.
func errorMessageOf(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding an error frame: %v", err)
	}
	return refusal.Message
}
