package server

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// opClass is what T-107 expects of an opcode that replicationRequired is true
// for.
type opClass int

const (
	// dataMutation: a write to a bucket's data, through the replicated write
	// path. Its response carries copies_confirmed and replication_factor.
	dataMutation opClass = iota
	// definition: database or bucket DDL. Its response carries neither.
	definition
	// notRouted: a response opcode, an internode opcode, or an operator or
	// user-management verb that dispatch serves before cluster routing; it
	// never reaches the replicated write path as a client's bucket write.
	notRouted
)

// writeOutcomeClasses is every opcode replicationRequired is true for. An
// opcode added without an entry fails the test: someone has to say whether
// its response reports its copies.
var writeOutcomeClasses = map[transport.Opcode]opClass{
	transport.OpPut:         dataMutation,
	transport.OpDelete:      dataMutation,
	transport.OpBatchPut:    dataMutation,
	transport.OpUpsert:      dataMutation,
	transport.OpBatchUpsert: dataMutation,
	transport.OpFIFOPush:    dataMutation,
	transport.OpFIFOPop:     dataMutation,
	transport.OpLIFOPush:    dataMutation,
	transport.OpLIFOPop:     dataMutation,
	transport.OpBTreeInsert: dataMutation,
	transport.OpHeapPush:    dataMutation,
	transport.OpHeapPop:     dataMutation,
	transport.OpUpdate:      dataMutation,
	transport.OpBlobWrite:   dataMutation,
	transport.OpFieldPush:   dataMutation,
	transport.OpFieldPop:    dataMutation,
	transport.OpFieldPut:    dataMutation,
	transport.OpFieldDelete: dataMutation,

	transport.OpQueueClaim:   dataMutation,
	transport.OpQueueAck:     dataMutation,
	transport.OpQueueRelease: dataMutation,
	transport.OpQueueExtend:  dataMutation,

	transport.OpDeadRequeue: dataMutation,
	transport.OpDeadDiscard: dataMutation,
	// A restore sends it; a client frame is refused before routing.
	transport.OpDeadRestore: notRouted,

	transport.OpCreateDB:           definition,
	transport.OpDeleteDB:           definition,
	transport.OpCreateBucket:       definition,
	transport.OpDeleteBucket:       definition,
	transport.OpDeclareStoreFields: definition,
	transport.OpAlterBucket:        definition,

	// A batch's items run separately and may partly apply, so the whole-batch
	// frame gets no write_outcome stamp; each answer carries its own
	// copies_confirmed and replication_factor (batch-request-spec.md, Reply).
	transport.OpBatch:   notRouted,
	transport.OpBatchOK: notRouted,

	transport.OpGetResponse:      notRouted,
	transport.OpExistsResponse:   notRouted,
	transport.OpBatchPutOK:       notRouted,
	transport.OpUpsertOK:         notRouted,
	transport.OpBatchUpsertOK:    notRouted,
	transport.OpFIFOPopResponse:  notRouted,
	transport.OpLIFOPopResponse:  notRouted,
	transport.OpBTreeResponse:    notRouted,
	transport.OpHeapPopResponse:  notRouted,
	transport.OpBlobReadResponse: notRouted,
	transport.OpGetFastResponse:  notRouted,
	transport.OpGossip:           notRouted,
	// The control channel's probe pair and the entry-ring wire: each is served
	// on its own channel (the transport's control handshake, ring_node.go), never
	// routed as a bucket write through the replicated path.
	transport.OpClusterProbe:      notRouted,
	transport.OpClusterProbeReply: notRouted,
	transport.OpRingKeyed:         notRouted,
	transport.OpRingAdmin:         notRouted,
	transport.OpRingReply:         notRouted,
	transport.OpRingBatch:         notRouted,
	transport.OpReplPush:          notRouted,
	transport.OpReplAck:           notRouted,
	transport.OpMigrateStart:      notRouted,
	transport.OpMigrateData:       notRouted,
	transport.OpMigrateDone:       notRouted,
	transport.OpClusterPropose:    notRouted,
	transport.OpMembershipChange:  notRouted,
	transport.OpClusterAdd:        notRouted,
	transport.OpMigrate:           notRouted,
	transport.OpClusterRemove:     notRouted,
	transport.OpUserCreate:        notRouted,
	transport.OpUserAlter:         notRouted,
	transport.OpUserDrop:          notRouted,
	transport.OpUserList:          notRouted,
	transport.OpUserGet:           notRouted,
	transport.OpUserGrant:         notRouted,
	transport.OpUserRevoke:        notRouted,
	transport.OpAuthConfigPush:    notRouted,
	transport.OpAuthConfigAck:     notRouted,
}

// writeOutcomeCase is one request of T-107 and the Go type its response
// decodes into.
type writeOutcomeCase struct {
	op       transport.Opcode
	request  bson.M
	response func() any
}

// claimedLease stands, in a lease verb's request, for the lease_id the claim
// before it answered.
const claimedLease = "the lease of the claim before"

// copiesConfirmedOf reads the two numbers from an encoded response, and
// reports whether each key is present as a number.
func copiesConfirmedOf(payload []byte) (confirmed, factor int32, hasConfirmed, hasFactor bool) {
	raw := bson.Raw(payload)
	var confirmed64, factor64 int64
	if value, err := raw.LookupErr("copies_confirmed"); err == nil {
		confirmed64, hasConfirmed = value.AsInt64OK()
	}
	if value, err := raw.LookupErr("replication_factor"); err == nil {
		factor64, hasFactor = value.AsInt64OK()
	}
	return int32(confirmed64), int32(factor64), hasConfirmed, hasFactor
}

// TestEveryDataMutationResponseCarriesItsCopyCountAndNoDDLResponseDoes is T-107
// (spec §19.3.11). Three nodes at factor 3; every data mutation, sent at `all`
// through node 1 (so most are forwarded), is answered with copies_confirmed 3
// and replication_factor 3, and its Go response type declares both: a type
// that does not drops them when a typed client decodes and re-encodes it. No
// DDL response carries either, nor a peek's.
func TestEveryDataMutationResponseCarriesItsCopyCountAndNoDDLResponseDoes(t *testing.T) {
	for op := 0; op < 256; op++ {
		code := transport.Opcode(op)
		if code.String() == "UNKNOWN" || !replicationRequired(code) {
			continue
		}
		if _, classified := writeOutcomeClasses[code]; !classified {
			t.Errorf("%s (0x%02X) is routed as a write and T-107's table does not say whether its response carries copies_confirmed", code, op)
		}
	}

	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const all = "all"
	at := func(fields bson.M) bson.M {
		fields["database"] = testDatabase
		fields["write_concern"] = all
		return fields
	}
	field := func(fields bson.M) bson.M {
		fields["bucket"] = "posts"
		fields["key"] = "p1"
		return at(fields)
	}
	bucketOf := func(name string, fields bson.M) bson.M {
		fields["bucket"] = name
		return at(fields)
	}

	ddl := []writeOutcomeCase{
		{transport.OpCreateDB, bson.M{"name": testDatabase}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "hashes", "type": string(BucketTypeHash)}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "fifos", "type": string(BucketTypeFIFO)}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "lifos", "type": string(BucketTypeLIFO)}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "btrees", "type": string(BucketTypeBTree)}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "heaps", "type": string(BucketTypeHeap)}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "blobs", "type": string(BucketTypeBlob)}, nil},
		{transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "posts", "type": string(BucketTypeHash)}, nil},
	}
	declare := declareReq(testDatabase, "posts", StoreDeclaration{Name: "Comments", Engine: "fifoStore"}, StoreDeclaration{Name: "Tags", Engine: "hashStore"})
	ddl = append(ddl, writeOutcomeCase{transport.OpDeclareStoreFields, bson.M{"database": declare.Database, "name": declare.Name, "stores": declare.Stores}, nil})

	insert := func() any { return &InsertResponse{} }
	push := func() any { return &PushResponse{} }
	pop := func() any { return &PopResponse{} }
	fieldResponse := func() any { return &FieldResponse{} }
	claim := func() any { return &ClaimResponse{} }
	lease := func() any { return &LeaseResponse{} }
	data := []writeOutcomeCase{
		{transport.OpPut, bucketOf("hashes", bson.M{"key": "k1", "data": bson.M{"n": 1}}), insert},
		{transport.OpUpdate, bucketOf("hashes", bson.M{"key": "k1", "set": bson.M{"n": 2}}), func() any { return &UpdateResponse{} }},
		{transport.OpBatchPut, bucketOf("hashes", bson.M{"data": []bson.M{{"_id_": "b1"}, {"_id_": "b2"}}}), func() any { return &BatchInsertResponse{} }},
		{transport.OpUpsert, bucketOf("hashes", bson.M{"key": "k1", "data": bson.M{"n": 3}}), insert},
		{transport.OpBatchUpsert, bucketOf("hashes", bson.M{"data": []bson.M{{"_id_": "b1"}, {"_id_": "b3"}}}), func() any { return &BatchInsertResponse{} }},
		{transport.OpDelete, bucketOf("hashes", bson.M{"key": "k1"}), func() any { return &DeleteResponse{} }},
		{transport.OpFIFOPush, bucketOf("fifos", bson.M{"data": bson.M{"n": 1}}), push},
		{transport.OpFIFOPop, bucketOf("fifos", bson.M{}), pop},
		{transport.OpLIFOPush, bucketOf("lifos", bson.M{"data": bson.M{"n": 1}}), push},
		{transport.OpLIFOPop, bucketOf("lifos", bson.M{}), pop},
		{transport.OpBTreeInsert, bucketOf("btrees", bson.M{"key": "a", "data": bson.M{"n": 1}}), insert},
		{transport.OpHeapPush, bucketOf("heaps", bson.M{"data": bson.M{"n": 1}, "priority": 3}), push},
		{transport.OpHeapPop, bucketOf("heaps", bson.M{}), pop},
		// A lease verb names the lease of the claim before it (claimedLease).
		{transport.OpFIFOPush, bucketOf("fifos", bson.M{"data": bson.M{"n": 2}}), push},
		{transport.OpQueueClaim, bucketOf("fifos", bson.M{}), claim},
		{transport.OpQueueExtend, bucketOf("fifos", bson.M{"lease_id": claimedLease, "lease_ms": 60_000}), lease},
		{transport.OpQueueRelease, bucketOf("fifos", bson.M{"lease_id": claimedLease}), lease},
		{transport.OpQueueClaim, bucketOf("fifos", bson.M{}), claim},
		{transport.OpQueueAck, bucketOf("fifos", bson.M{"lease_id": claimedLease}), lease},
		{transport.OpDeadRequeue, bucketOf("fifos", bson.M{"all": true}), func() any { return &DeadMoveResponse{} }},
		{transport.OpDeadDiscard, bucketOf("fifos", bson.M{"all": true}), func() any { return &DeadMoveResponse{} }},
		{transport.OpBlobWrite, bucketOf("blobs", bson.M{"key": "b", "data": []byte("bytes")}), func() any { return &BlobWriteResponse{} }},
		{transport.OpPut, bucketOf("posts", bson.M{"key": "p1", "data": bson.M{"title": "p1"}}), insert},
		{transport.OpFieldPush, field(bson.M{"field": "Comments", "data": bson.M{"text": "c1"}}), fieldResponse},
		{transport.OpFieldPop, field(bson.M{"field": "Comments"}), fieldResponse},
		{transport.OpFieldPut, field(bson.M{"field": "Tags", "element": "go", "data": bson.M{"n": 1}}), fieldResponse},
		{transport.OpFieldDelete, field(bson.M{"field": "Tags", "element": "go"}), fieldResponse},
	}
	peeks := []writeOutcomeCase{
		{transport.OpFIFOPeek, bucketOf("fifos", bson.M{}), nil},
		{transport.OpLIFOPeek, bucketOf("lifos", bson.M{}), nil},
	}
	dropDDL := []writeOutcomeCase{
		{transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": "blobs"}, nil},
		{transport.OpDeleteDB, bson.M{"name": testDatabase}, nil},
	}

	noCount := func(c writeOutcomeCase, what string) {
		t.Helper()
		frame := tc.ok(0, c.op, c.request)
		if _, _, hasConfirmed, hasFactor := copiesConfirmedOf(frame.Payload); hasConfirmed || hasFactor {
			t.Errorf("%s's response, %s, carries copies_confirmed or replication_factor: %s", c.op, what, bson.Raw(frame.Payload))
		}
	}
	for _, c := range ddl {
		noCount(c, "bucket or database DDL")
	}
	for _, name := range []string{"hashes", "fifos", "lifos", "btrees", "heaps", "blobs", "posts"} {
		waitForServing(t, tc, testDatabase, name, 3)
	}
	covered := make(map[transport.Opcode]bool)
	lastLease := ""
	for _, c := range data {
		covered[c.op] = true
		if c.request["lease_id"] == claimedLease {
			c.request["lease_id"] = lastLease
		}
		frame := tc.ok(0, c.op, c.request)
		what := fmt.Sprintf("%s on %v", c.op, c.request["bucket"])
		if c.op == transport.OpQueueClaim {
			lastLease, _ = bson.Raw(frame.Payload).Lookup("lease_id").StringValueOK()
		}
		confirmed, factor, hasConfirmed, hasFactor := copiesConfirmedOf(frame.Payload)
		if !hasConfirmed || !hasFactor {
			t.Errorf("%s was answered without copies_confirmed or replication_factor: %s", what, bson.Raw(frame.Payload))
			continue
		}
		if confirmed != 3 || factor != 3 {
			t.Errorf("%s at all on a healthy factor-3 bucket was answered copies_confirmed %d, replication_factor %d; want 3 and 3", what, confirmed, factor)
		}
		typed := c.response()
		if err := bson.Unmarshal(frame.Payload, typed); err != nil {
			t.Fatalf("%s: decoding the response into %T: %v", what, typed, err)
		}
		again, err := bson.Marshal(typed)
		if err != nil {
			t.Fatalf("%s: encoding %T: %v", what, typed, err)
		}
		if _, _, keptConfirmed, keptFactor := copiesConfirmedOf(again); !keptConfirmed || !keptFactor {
			t.Errorf("%T does not declare copies_confirmed and replication_factor under those names: a typed client loses them", typed)
		}
	}
	for op, class := range writeOutcomeClasses {
		if class == dataMutation && !covered[op] {
			t.Errorf("T-107 sends no %s", op)
		}
	}
	for _, c := range peeks {
		noCount(c, "a peek")
	}
	for _, c := range dropDDL {
		noCount(c, "bucket or database DDL")
	}
}
