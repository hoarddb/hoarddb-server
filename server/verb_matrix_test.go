package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// matrixTypes is the column order of the verb matrix.
var matrixTypes = []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHash, BucketTypeBTree, BucketTypeHeap, BucketTypeBlob}

// matrixProbe sends one verb to a bucket seeded with one entry under the key
// "k1" and says whether the server answered it. A probe is built so that a type
// which has the verb answers it positively (a hit, success true, a non-empty
// list), so "not answered" means refused, by an error frame or an in-band
// refusal alike.
type matrixProbe struct {
	opcode   transport.Opcode
	request  func(bucket string) interface{}
	answered func(answer bson.M) bool
}

// matrixRow is one row of the verb matrix: a label, the probes whose answers must
// all agree, and the expected answer per store type, in matrixTypes order.
type matrixRow struct {
	label    string
	probes   []matrixProbe
	answered [6]bool
}

func matrixSucceeded(answer bson.M) bool { v, _ := answer["success"].(bool); return v }
func matrixFound(answer bson.M) bool     { v, _ := answer["found"].(bool); return v }
func matrixExists(answer bson.M) bool    { v, _ := answer["exists"].(bool); return v }
func matrixUnrefused(answer bson.M) bool {
	message, _ := answer["message"].(string)
	return message == ""
}
func matrixListed(field string) func(bson.M) bool {
	return func(answer bson.M) bool { list, _ := answer[field].(bson.A); return len(list) > 0 }
}

func matrixKeyed(bucket string) interface{} {
	return FindRequest{Database: "app", Bucket: bucket, Key: "k1"}
}

// matrixUnknownLease is a well-formed lease id that no bucket issued: the
// lease verbs answer it in band with lease_state unknown, which says the type
// answered the verb. A type that does not answer the verb is an error frame.
func matrixUnknownLease(bucket string) interface{} {
	return LeaseRequest{Database: "app", Bucket: bucket, LeaseID: strings.Repeat("0", 40)}
}

func matrixUnknownLeaseState(answer bson.M) bool {
	state, _ := answer["lease_state"].(string)
	return state == "unknown"
}

func matrixWholeBucket(bucket string) interface{} {
	return BTreeRangeRequest{Database: "app", Bucket: bucket}
}

var matrixRows = []matrixRow{
	{label: "put (keyed insert, refuses a key the bucket holds)", answered: [6]bool{true, true, true, true, true, false},
		probes: []matrixProbe{{transport.OpPut, func(b string) interface{} {
			return InsertRequest{Database: "app", Bucket: b, Key: "k2", Data: map[string]interface{}{"n": int32(2)}}
		}, matrixSucceeded}}},
	{label: "insert (keyless write, the server assigns the key)", answered: [6]bool{true, true, true, true, true, false},
		probes: []matrixProbe{{transport.OpPut, func(b string) interface{} {
			return InsertRequest{Database: "app", Bucket: b, Data: map[string]interface{}{"n": int32(2)}}
		}, matrixSucceeded}}},
	{label: "insert many", answered: [6]bool{true, true, true, true, true, false},
		probes: []matrixProbe{{transport.OpBatchPut, func(b string) interface{} {
			return BatchInsertRequest{Database: "app", Bucket: b, Data: []map[string]interface{}{{"n": int32(2)}, {"n": int32(3)}}}
		}, func(a bson.M) bool { count, _ := a["count"].(int32); return matrixSucceeded(a) && count == 2 }}}},
	{label: "upsert (replaces the scalar fields of the document under its `_id_`, keeps its stores)", answered: [6]bool{false, false, true, true, false, false},
		probes: []matrixProbe{{transport.OpUpsert, func(b string) interface{} {
			return InsertRequest{Database: "app", Bucket: b, Key: "k1", Data: map[string]interface{}{"n": int32(2)}}
		}, matrixSucceeded}}},
	{label: "upsert many", answered: [6]bool{false, false, true, true, false, false},
		probes: []matrixProbe{{transport.OpBatchUpsert, func(b string) interface{} {
			return BatchInsertRequest{Database: "app", Bucket: b, Data: []map[string]interface{}{{"_id_": "k1", "n": int32(2)}, {"_id_": "k2", "n": int32(3)}}}
		}, func(a bson.M) bool { count, _ := a["count"].(int32); return matrixSucceeded(a) && count == 2 }}}},
	{label: "get", answered: [6]bool{false, false, true, true, false, true},
		probes: []matrixProbe{{transport.OpGet, matrixKeyed, matrixFound}}},
	{label: "exists", answered: [6]bool{false, false, true, true, false, true},
		probes: []matrixProbe{{transport.OpExists, matrixKeyed, matrixExists}}},
	{label: "delete", answered: [6]bool{false, false, true, true, false, true},
		probes: []matrixProbe{{transport.OpDelete, func(b string) interface{} {
			return DeleteRequest{Database: "app", Bucket: b, Key: "k1"}
		}, matrixSucceeded}}},
	{label: "update", answered: [6]bool{false, false, true, true, false, false},
		probes: []matrixProbe{{transport.OpUpdate, func(b string) interface{} {
			return UpdateRequest{Database: "app", Bucket: b, Key: "k1", Set: map[string]interface{}{"n": int32(9)}}
		}, matrixSucceeded}}},
	{label: "push", answered: [6]bool{true, true, true, true, true, false},
		probes: []matrixProbe{{transport.OpFIFOPush, func(b string) interface{} {
			return PushRequest{Database: "app", Bucket: b, Data: map[string]interface{}{"n": int32(2)}}
		}, matrixSucceeded}}},
	{label: "pop", answered: [6]bool{true, true, false, false, true, false},
		probes: []matrixProbe{{transport.OpFIFOPop, func(b string) interface{} {
			return PopRequest{Database: "app", Bucket: b}
		}, matrixFound}}},
	{label: "peek", answered: [6]bool{true, true, false, false, true, false},
		probes: []matrixProbe{{transport.OpFIFOPeek, func(b string) interface{} {
			return PeekRequest{Database: "app", Bucket: b}
		}, matrixFound}}},
	{label: "claim", answered: [6]bool{true, true, false, false, true, false},
		probes: []matrixProbe{{transport.OpQueueClaim, func(b string) interface{} {
			return ClaimRequest{Database: "app", Bucket: b, Limit: 1}
		}, matrixFound}}},
	{label: "ack, release, extend", answered: [6]bool{true, true, false, false, true, false},
		probes: []matrixProbe{
			{transport.OpQueueAck, matrixUnknownLease, matrixUnknownLeaseState},
			{transport.OpQueueRelease, matrixUnknownLease, matrixUnknownLeaseState},
			{transport.OpQueueExtend, func(b string) interface{} {
				return LeaseRequest{Database: "app", Bucket: b, LeaseID: strings.Repeat("0", 40), LeaseMs: 60_000}
			}, matrixUnknownLeaseState},
		}},
	{label: "dead, requeue, discard", answered: [6]bool{true, true, false, false, true, false},
		probes: []matrixProbe{
			{transport.OpDeadList, func(b string) interface{} {
				return DeadListRequest{Database: "app", Bucket: b}
			}, matrixUnrefused},
			{transport.OpDeadRequeue, func(b string) interface{} {
				return DeadMoveRequest{Database: "app", Bucket: b, All: true}
			}, matrixSucceeded},
			{transport.OpDeadDiscard, func(b string) interface{} {
				return DeadMoveRequest{Database: "app", Bucket: b, All: true}
			}, matrixSucceeded},
		}},
	{label: "range, first, last", answered: [6]bool{false, false, false, true, false, false},
		probes: []matrixProbe{
			{transport.OpBTreeRange, matrixWholeBucket, matrixListed("entries")},
			{transport.OpBTreeFirst, matrixWholeBucket, matrixListed("entries")},
			{transport.OpBTreeLast, matrixWholeBucket, matrixListed("entries")},
		}},
	{label: "put blob, get blob", answered: [6]bool{false, false, false, false, false, true},
		probes: []matrixProbe{
			{transport.OpBlobWrite, func(b string) interface{} {
				return BlobWriteRequest{Database: "app", Bucket: b, Key: "k2", Data: []byte("payload")}
			}, matrixSucceeded},
			{transport.OpBlobRead, matrixKeyed, matrixFound},
		}},
	{label: "find, distinct", answered: [6]bool{true, true, true, true, true, false},
		probes: []matrixProbe{
			{transport.OpScan, func(b string) interface{} { return ScanRequest{Database: "app", Bucket: b} },
				func(a bson.M) bool { return matrixFound(a) && matrixListed("documents")(a) }},
			{transport.OpDistinct, func(b string) interface{} { return DistinctRequest{Database: "app", Bucket: b, Field: "n"} }, matrixListed("values")},
		}},
	{label: "count", answered: [6]bool{true, true, true, true, true, true},
		probes: []matrixProbe{{transport.OpCount, func(b string) interface{} {
			return CountRequest{Database: "app", Bucket: b}
		}, matrixUnrefused}}},
	{label: "search (secondary index)", answered: [6]bool{true, true, true, true, true, false},
		probes: []matrixProbe{{transport.OpSearch, func(b string) interface{} {
			return SearchRequest{Database: "app", Bucket: b, Filter: map[string]interface{}{"n": int32(1)}}
		}, matrixListed("results")}}},
	{label: "stats", answered: [6]bool{true, true, true, true, true, true},
		probes: []matrixProbe{{transport.OpBucketStats, func(b string) interface{} {
			return BucketStatsRequest{Database: "app", Bucket: b}
		}, matrixUnrefused}}},
	{label: "consistent get (quorum read)", answered: [6]bool{false, false, true, true, false, false},
		probes: []matrixProbe{{transport.OpConsistentGet, matrixKeyed, matrixFound}}},
}

// seedMatrixBucket gives a bucket of any store type one entry under "k1" (a
// queue's element carries it as its _id_), through the verb the type answers.
func seedMatrixBucket(t *testing.T, s *Server, bucket string, bucketType BucketType) {
	t.Helper()
	doc := map[string]interface{}{"_id_": "k1", "n": int32(1)}
	switch bucketType {
	case BucketTypeHash, BucketTypeBTree:
		resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: "k1", Data: doc})
		if err != nil || !resp.Success {
			t.Fatalf("seeding %s %s: %+v, %v", bucketType, bucket, resp, err)
		}
	case BucketTypeBlob:
		resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: bucket, Key: "k1", Data: []byte("payload")})
		if err != nil || !resp.Success {
			t.Fatalf("seeding %s %s: %+v, %v", bucketType, bucket, resp, err)
		}
	default:
		resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Data: doc})
		if err != nil || !resp.Success {
			t.Fatalf("seeding %s %s: %+v, %v", bucketType, bucket, resp, err)
		}
	}
}

// matrixAnswerOf runs one probe through the dispatcher and reports whether the server
// answered it, with the answer for the failure message.
func matrixAnswerOf(t *testing.T, s *Server, probe matrixProbe, bucket string) (bool, string) {
	t.Helper()
	frame := requestFrame(t, probe.opcode, probe.request(bucket))
	reply, err := s.dispatch(context.Background(), frame)
	if err != nil {
		t.Fatalf("dispatching %s: %v", probe.opcode, err)
	}
	// A query holds an execution slot until its answer is written; no frame is
	// written here, and a node with few CPUs has few slots, so without this the
	// matrix's own queries would starve the next one for ever.
	fireWrittenHook(reply)
	if reply.Opcode == transport.OpError {
		return false, "error frame: " + decodeErrorFrame(t, reply, probe.opcode.String()).Message
	}
	var answer bson.M
	if err := bson.Unmarshal(reply.Payload, &answer); err != nil {
		t.Fatalf("decoding the answer to %s: %v", probe.opcode, err)
	}
	return probe.answered(answer), reply.Opcode.String() + " " + matrixStringify(answer)
}

func matrixStringify(answer bson.M) string {
	encoded, err := bson.MarshalExtJSON(answer, false, false)
	if err != nil {
		return "(undecodable)"
	}
	return string(encoded)
}

// TestVerbMatrixMatchesTheTable sends every verb of matrixRows to a bucket of every store
// type through the dispatcher and asserts the answered-or-refused cell. The table is the
// per-type behaviour of the single-store opcodes the server still answers beside the ring
// verbs (BUG-0516 found the handbook disagreeing with it; the handbook no longer documents
// those opcodes, so the table is checked against the server alone).
func TestVerbMatrixMatchesTheTable(t *testing.T) {
	s := newTestServer(t)
	labelWords := strings.NewReplacer(" ", "_", ",", "", "(", "", ")", "")
	for _, row := range matrixRows {
		for typeIndex, bucketType := range matrixTypes {
			for probeIndex, probe := range row.probes {
				bucket := labelWords.Replace(row.label) + "_" + string(bucketType) + "_" + string(rune('a'+probeIndex))
				createTestBucket(t, s, "app", bucket, bucketType)
				seedMatrixBucket(t, s, bucket, bucketType)
				got, detail := matrixAnswerOf(t, s, probe, bucket)
				if got != row.answered[typeIndex] {
					t.Errorf("%s on a %s bucket (%s): answered=%v, the table says %v; the server said %s",
						row.label, bucketType, probe.opcode, got, row.answered[typeIndex], detail)
				}
			}
		}
	}
}
