package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRefusedWriteWithTimedOutConcernIsNotApplied is BUG-0557. A write the
// owner's handler refuses (a $inc of a string) is logged, applied as a
// refusal and shipped; when a replica that is in sync confirms nothing, the
// write-concern wait then times out. The answer must be the handler's
// refusal with write_outcome not_applied, sent to the owner and forwarded
// through a node that does not own the bucket: nothing was written, so a
// retry is safe. Before the fix the owner answered TIMEOUT with
// write_outcome applied, which tells the caller never to resend a write that
// never happened. The control, a $inc the handler accepts under the same held
// replica, still answers TIMEOUT / applied.
func TestRefusedWriteWithTimedOutConcernIsNotApplied(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 3, nil, func(_ int, c *Cluster) { c.wcTimeout = time.Second })
	defer tc.stop()
	const owner, held, coordinator = 0, 1, 2
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, owner, "counters")
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, len(tc.nodes))
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "holder",
		Data: map[string]interface{}{"Name": "ada", "Visits": 1}, WriteConcern: "all"})
	defer holdShipsTo(tc.addrs[held])()

	increment := func(field string) bson.M {
		return bson.M{"database": testDatabase, "bucket": bucket, "key": "holder", "inc": bson.M{field: 1}, "writeconcern": "all"}
	}
	for _, via := range []int{owner, coordinator} {
		what := fmt.Sprintf("a $inc of a string at concern all through node %d", via+1)
		refusal := errorAnswerOf(t, tc.frame(via, transport.OpUpdate, increment("Name")), what)
		if refusal.WriteOutcome != outcomeNotApplied || transport.ErrorCode(refusal.Code) == transport.ErrTimeout {
			t.Errorf("%s was answered code %d, write_outcome %q; want the handler's refusal, %q: %s",
				what, refusal.Code, refusal.WriteOutcome, outcomeNotApplied, refusal.Message)
		}
		if strings.Contains(refusal.Message, "write concern") {
			t.Errorf("%s was answered with the write concern's message, not the handler's refusal: %s", what, refusal.Message)
		}

		what = fmt.Sprintf("a $inc of a number at concern all through node %d", via+1)
		accepted := errorAnswerOf(t, tc.frame(via, transport.OpUpdate, increment("Visits")), what)
		requireOutcome(t, accepted, what, outcomeApplied, transport.ErrTimeout)
	}
}
