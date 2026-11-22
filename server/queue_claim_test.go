package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The lease verbs on a single node (claim/ack build step 10). A node that logs
// nothing reads its own clock, so these tests drive that clock through the
// per-node seam Server.leaseClock.

// leaseClockAt is a lease clock a test moves by hand.
type leaseClockAt struct{ millis atomic.Int64 }

func (clock *leaseClockAt) now() time.Time { return time.UnixMilli(clock.millis.Load()) }

func (clock *leaseClockAt) advance(by time.Duration) { clock.millis.Add(by.Milliseconds()) }

// withLeaseClock gives s a clock at a fixed instant that only the test moves.
func withLeaseClock(s *Server) *leaseClockAt {
	clock := &leaseClockAt{}
	clock.millis.Store(1_800_000_000_000)
	s.leaseClock = clock.now
	return clock
}

// leaseBucketTypes are the three buckets that issue leases.
var leaseBucketTypes = []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap}

// leaseServer is a server with a controllable lease clock and one bucket of each
// type holding five entries {n: 1..5}.
func leaseServer(t *testing.T) (*Server, *leaseClockAt) {
	t.Helper()
	s := newTestServer(t)
	clock := withLeaseClock(s)
	for _, bucketType := range leaseBucketTypes {
		createTestBucket(t, s, "app", string(bucketType), bucketType)
		pushPeekEntries(t, s, string(bucketType), 5, "")
	}
	return s, clock
}

// claimOf claims limit entries of bucket for leaseMs.
func claimOf(t *testing.T, s *Server, bucket string, limit int, leaseMs int64) *ClaimResponse {
	t.Helper()
	answer, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: bucket, Limit: limit, LeaseMs: leaseMs})
	if err != nil {
		t.Fatalf("claim %d of %s: %v", limit, bucket, err)
	}
	return answer
}

// TestClaimIsAWriteNotARead pins the opcode tables (claim-ack-spec 9.1).
func TestClaimIsAWriteNotARead(t *testing.T) {
	for _, op := range []transport.Opcode{transport.OpQueueClaim, transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend} {
		if op.IsReadOnly() || isPeekOpcode(op) || !replicationRequired(op) || !isDataWrite(op) {
			t.Errorf("%s: readOnly=%v peek=%v replicated=%v dataWrite=%v", op, op.IsReadOnly(), isPeekOpcode(op), replicationRequired(op), isDataWrite(op))
		}
		if op.String() == "UNKNOWN" || !strings.HasPrefix(op.String(), "QUEUE_") {
			t.Errorf("opcode 0x%02X is named %q", uint8(op), op)
		}
	}
}

// TestClaimAndLeaseVerbsHaveTheirRules: a Reader is refused with the grant-role
// text pop gives it, and a Writer is not refused by the gate.
func TestClaimAndLeaseVerbsHaveTheirRules(t *testing.T) {
	s := userTestServer(t)
	handler := NewHandler(s)
	reader := &transport.Session{}
	reader.SetIdentity(transport.Identity{Username: "ro", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	writer := &transport.Session{}
	writer.SetIdentity(transport.Identity{Username: "rw", Roles: []string{"writer"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "writer", DB: "app"}}})
	requests := map[transport.Opcode]any{
		transport.OpQueueClaim:   ClaimRequest{Database: "app", Bucket: "Jobs"},
		transport.OpQueueAck:     LeaseRequest{Database: "app", Bucket: "Jobs", LeaseID: strings.Repeat("0", 40)},
		transport.OpQueueRelease: LeaseRequest{Database: "app", Bucket: "Jobs", LeaseID: strings.Repeat("0", 40)},
		transport.OpQueueExtend:  LeaseRequest{Database: "app", Bucket: "Jobs", LeaseID: strings.Repeat("0", 40), LeaseMs: 5000},
	}
	for op, request := range requests {
		frame := transport.NewFrame(op, mustMarshalBSON(t, request))
		refused, err := handler(transport.WithSession(context.Background(), reader), frame)
		if err != nil || refused.Opcode != transport.OpError || decodeErrorCode(t, refused.Payload) != int(transport.ErrPermissionDenied) {
			t.Errorf("%s as a reader = %v, %v; want PERMISSION_DENIED", op, refused, err)
			continue
		}
		if message := decodeErrorMessage(t, refused.Payload); !strings.Contains(message, `user "ro" lacks "writer" on "app"`) || !strings.Contains(message, "grant role writer@app to ro") {
			t.Errorf("%s as a reader: message %q", op, message)
		}
		allowed, err := handler(transport.WithSession(context.Background(), writer), frame)
		if err != nil || (allowed.Opcode == transport.OpError && decodeErrorCode(t, allowed.Payload) == int(transport.ErrPermissionDenied)) {
			t.Errorf("%s as a writer was refused by the gate: %v, %v", op, allowed, err)
		}
	}
}

// TestClaimLimitDefaultsToOneAndCapsAt100: no limit and limit 0 claim one entry,
// 100 is accepted, and a negative or larger limit is refused with the text of
// claim-ack-spec 9.5, on a single node (the handler's check).
func TestClaimLimitDefaultsToOneAndCapsAt100(t *testing.T) {
	s, _ := leaseServer(t)
	for _, limit := range []int{0, 0} {
		answer := claimOf(t, s, "fifo", limit, 0)
		if !answer.Found || len(answer.Items) != 1 {
			t.Fatalf("claim limit %d = %+v; want one entry", limit, answer)
		}
	}
	if answer := claimOf(t, s, "fifo", 100, 0); !answer.Found || len(answer.Items) != 3 {
		t.Fatalf("claim limit 100 of three left = %+v; want three entries", answer)
	}
	_, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: "fifo", Limit: 500})
	want := "invalid claim limit: 500 is above the claim maximum of 100. Claim takes at most 100 at a time: repeat db.fifo.claim({limit: 100}), or read them without taking any with db.fifo.peek({limit: 500})"
	if !errors.Is(err, ErrClaimLimit) || err.Error() != want {
		t.Fatalf("claim limit 500 = %v\nwant ErrClaimLimit with %q", err, want)
	}
	if _, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: "fifo", Limit: -1}); !errors.Is(err, ErrClaimLimit) {
		t.Fatalf("claim limit -1 = %v; want ErrClaimLimit", err)
	}
	if code := codeFor(err); code != transport.ErrInvalidArgument {
		t.Fatalf("the code of a refused claim limit is %v, want INVALID_ARGUMENT", code)
	}
}

// TestLeaseOutOfBoundsRefusedAtTheDoor: a lease length outside 1s..1h, a lease
// id that is not 40 hex characters and an extend with no length are refused
// with INVALID_ARGUMENT and not_applied on a single node (the handler's check).
func TestLeaseOutOfBoundsRefusedAtTheDoor(t *testing.T) {
	s, _ := leaseServer(t)
	handler := NewHandler(s)
	lease := claimOf(t, s, "fifo", 1, 0).LeaseID
	cases := []struct {
		name    string
		op      transport.Opcode
		request any
		want    string
	}{
		{"claim lease above the maximum", transport.OpQueueClaim, ClaimRequest{Database: "app", Bucket: "fifo", LeaseMs: 7_200_000},
			`invalid lease: 7200000 ms is above the lease maximum of 1h. A lease of more than an hour is not granted: claim with lease "1h" and call db.fifo.extend({lease_id: "…", lease: "1h"}) while the work runs`},
		{"claim lease below the minimum", transport.OpQueueClaim, ClaimRequest{Database: "app", Bucket: "fifo", LeaseMs: 500}, "below the lease minimum of 1s"},
		{"extend with no length", transport.OpQueueExtend, LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: lease}, "extend needs a new length"},
		{"extend above the maximum", transport.OpQueueExtend, LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: lease, LeaseMs: 3_600_001}, "above the lease maximum of 1h"},
		{"ack with a short lease id", transport.OpQueueAck, LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: "abc"}, "is not a lease id"},
		{"release with a lease id that is not hex", transport.OpQueueRelease, LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: strings.Repeat("z", 40)}, "is not a lease id"},
	}
	for _, refused := range cases {
		t.Run(refused.name, func(t *testing.T) {
			answer, err := handler(context.Background(), transport.NewFrame(refused.op, mustMarshalBSON(t, refused.request)))
			if err != nil {
				t.Fatal(err)
			}
			refusal := errorAnswerOf(t, answer, refused.name)
			requireOutcome(t, refusal, refused.name, outcomeNotApplied, transport.ErrInvalidArgument)
			if !strings.Contains(refusal.Message, refused.want) {
				t.Errorf("message %q lacks %q", refusal.Message, refused.want)
			}
		})
	}
}

// TestItemIDThatIsADocumentIDIsRefused: an ack that names the documents' _id_
// values, not the handles claim returned, is refused with the message that says
// so, on every bucket type, before the store is read.
func TestItemIDThatIsADocumentIDIsRefused(t *testing.T) {
	s, _ := leaseServer(t)
	for _, bucketType := range leaseBucketTypes {
		bucket := string(bucketType)
		answer := claimOf(t, s, bucket, 2, 0)
		documentID := bson.Raw(answer.Items[0]).Lookup("_id_").StringValue()
		_, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: answer.LeaseID, ItemIDs: []string{documentID}})
		if !errors.Is(err, ErrLeaseOption) || !strings.Contains(err.Error(), "not a handle claim returned") || !strings.Contains(err.Error(), "not the documents' _id_ values") {
			t.Errorf("%s: ack by document id = %v; want ErrLeaseOption saying these are claim's item_ids", bucket, err)
		}
		if _, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: answer.LeaseID, ItemIDs: answer.ItemIDs[:1]}); err != nil {
			t.Errorf("%s: ack by item id = %v; want it accepted", bucket, err)
		}
	}
}

// getCountingKV counts the point reads a snapshot makes.
type getCountingKV struct {
	*syncCountingKV
	gets atomic.Int64
}

func (kv *getCountingKV) NewSnapshot() storage.KVSnapshot {
	return &getCountingSnapshot{KVSnapshot: kv.syncCountingKV.NewSnapshot(), gets: &kv.gets}
}

type getCountingSnapshot struct {
	storage.KVSnapshot
	gets *atomic.Int64
}

func (snapshot *getCountingSnapshot) Get(key []byte) ([]byte, error) {
	snapshot.gets.Add(1)
	return snapshot.KVSnapshot.Get(key)
}

// leaseKVServer is a server on a kv that counts syncs and point reads and can
// fail its syncs and bound its batches, holding a fifo "jobs" of five entries.
func leaseKVServer(t *testing.T) (*Server, *getCountingKV, *leaseClockAt) {
	t.Helper()
	counted := &getCountingKV{syncCountingKV: &syncCountingKV{Store: kvfault.New()}}
	s := newKVFaultServer(t.TempDir(), counted)
	clock := withLeaseClock(s)
	createDurabilityBucket(t, s, "jobs", BucketTypeFIFO)
	pushPeekEntries(t, s, "jobs", 5, "")
	return s, counted, clock
}

// TestAckWithTooManyItemIDsIsRefusedOnASingleNode: 101 item ids on a node whose
// door never runs are refused by the handler with no read made for them.
func TestAckWithTooManyItemIDsIsRefusedOnASingleNode(t *testing.T) {
	s, kv, _ := leaseKVServer(t)
	lease := claimOf(t, s, "jobs", 1, 0).LeaseID
	ack := func(itemIDs []string, leaseID string) error {
		_, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: "jobs", LeaseID: leaseID, ItemIDs: itemIDs})
		return err
	}
	before := kv.gets.Load()
	_ = ack(nil, "not a lease id")
	baseline := kv.gets.Load() - before

	itemIDs := make([]string, 101)
	for index := range itemIDs {
		itemIDs[index] = fmt.Sprintf("%016x", index)
	}
	before = kv.gets.Load()
	err := ack(itemIDs, lease)
	if !errors.Is(err, ErrLeaseOption) || !strings.Contains(err.Error(), "ack names 101 item ids and takes at most 100") {
		t.Fatalf("ack of 101 item ids = %v; want ErrLeaseOption naming the 100", err)
	}
	if reads := kv.gets.Load() - before; reads != baseline {
		t.Fatalf("the refused ack made %d point reads, a request refused at the lock makes %d: the store was read for ids it should have refused", reads, baseline)
	}
}

// TestClaimRefusalsNameTheFix: the type texts of claim-ack-spec 9.5 for each of
// the four verbs on a hash, btree and blob bucket, as ERROR frames, and no refusal
// carries entry content.
func TestClaimRefusalsNameTheFix(t *testing.T) {
	s := newTestServer(t)
	leaseRequest := func(bucket string) LeaseRequest {
		return LeaseRequest{Database: "app", Bucket: bucket, LeaseID: strings.Repeat("a", 40), LeaseMs: 5000}
	}
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeBlob} {
		bucket := "plain-" + string(bucketType)
		createTestBucket(t, s, "app", bucket, bucketType)
		_, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: bucket})
		requireWrongTypeRefusal(t, err, fmt.Sprintf("claim is not defined for %s buckets: a %s has no order to take from.", bucketType, bucketType))
		verbs := map[string]func(context.Context, LeaseRequest) (*LeaseResponse, error){"ack": s.AckCtx, "release": s.ReleaseCtx, "extend": s.ExtendCtx}
		for verb, run := range verbs {
			_, err := run(context.Background(), leaseRequest(bucket))
			requireWrongTypeRefusal(t, err, fmt.Sprintf("%s is not defined for %s buckets: only fifo, lifo and heap buckets issue leases.", verb, bucketType))
			if err != nil && strings.Contains(err.Error(), "unknown") {
				t.Errorf("%s on a %s bucket reads as an unknown lease: %v", verb, bucketType, err)
			}
		}
	}

	leased, _ := leaseServer(t)
	entry := bson.Raw(claimOf(t, leased, "fifo", 1, 0).Items[0]).String()
	unknown, err := leased.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: strings.Repeat("a", 40)})
	if err != nil || unknown.Success || unknown.LeaseState != "unknown" || unknown.WriteOutcome != "" ||
		!strings.Contains(unknown.Message, "lease aaaa…aa is unknown on app.fifo") || !strings.Contains(unknown.Message, "Claim again with db.fifo.claim()") {
		t.Errorf("ack of an unknown lease = %+v, %v", unknown, err)
	}
	if strings.Contains(unknown.Message, entry) {
		t.Error("a refusal carries entry content")
	}
}

// TestClaimAnswersItemsHandlesAndTheLease: the answer of claim-ack-spec 9.2 on
// every bucket type, the entries in pop order, a handle for each, deliveries
// absent while every value is 1 and the counts of what is left.
func TestClaimAnswersItemsHandlesAndTheLease(t *testing.T) {
	s, clock := leaseServer(t)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			answer := claimOf(t, s, string(engine.bucketType), 3, 60_000)
			if !answer.Found || fmt.Sprint(peekedNumbers(t, answer.Items)) != fmt.Sprint(engine.popOrder[:3]) {
				t.Fatalf("claim of 3 = %+v; want entries %v", answer, engine.popOrder[:3])
			}
			if len(answer.ItemIDs) != 3 || len(answer.LeaseID) != 40 || answer.LeaseMs != 60_000 || answer.Deliveries != nil || answer.Pending != 3 || answer.Dead != 0 {
				t.Fatalf("claim answer %+v", answer)
			}
			if want := clock.now().Add(60 * time.Second).UTC(); !answer.LeaseDeadline.Equal(want) {
				t.Fatalf("lease deadline %v, want %v", answer.LeaseDeadline, want)
			}
			if left := peekAllNumbers(t, s, string(engine.bucketType)); fmt.Sprint(left) != fmt.Sprint(engine.popOrder[3:]) {
				t.Fatalf("a claim left %v on the queue, want %v", left, engine.popOrder[3:])
			}
		})
	}
}

// TestAckReleaseExtendRoundTrip: ack removes what it names, release gives the
// rest back with a delivery count of 2, extend moves the deadline.
func TestAckReleaseExtendRoundTrip(t *testing.T) {
	s, clock := leaseServer(t)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := string(engine.bucketType)
			claimed := claimOf(t, s, bucket, 3, 0)
			acked, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: claimed.LeaseID, ItemIDs: claimed.ItemIDs[:1]})
			if err != nil || !acked.Success || acked.LeaseState != "acked" || acked.Acked != 1 || acked.Remaining != 2 {
				t.Fatalf("ack of one = %+v, %v", acked, err)
			}
			extended, err := s.ExtendCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: claimed.LeaseID, LeaseMs: 120_000})
			if err != nil || !extended.Success || extended.LeaseState != "extended" || extended.LeaseMs != 120_000 || !extended.LeaseDeadline.Equal(clock.now().Add(2*time.Minute).UTC()) {
				t.Fatalf("extend = %+v, %v", extended, err)
			}
			released, err := s.ReleaseCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: claimed.LeaseID})
			if err != nil || !released.Success || released.LeaseState != "released" || released.Released != 2 {
				t.Fatalf("release = %+v, %v", released, err)
			}
			again := claimOf(t, s, bucket, 2, 0)
			if !again.Found || fmt.Sprint(again.Deliveries) != "[2 2]" {
				t.Fatalf("claim after a release = %+v; want the two returned entries at delivery 2", again)
			}
			late, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: claimed.LeaseID})
			if err != nil || late.Success || late.LeaseState != "unknown" {
				t.Fatalf("ack of a released lease = %+v, %v; want unknown", late, err)
			}
			clock.advance(2 * time.Hour)
			expired, err := s.AckCtx(context.Background(), LeaseRequest{Database: "app", Bucket: bucket, LeaseID: again.LeaseID})
			if err != nil || expired.Success || expired.LeaseState != "expired" || expired.WriteOutcome != "" || !strings.Contains(expired.Message, "expired at") {
				t.Fatalf("ack after the deadline = %+v, %v; want expired in band with no write_outcome", expired, err)
			}
		})
	}
}

// pushedBigEntry pushes one entry of size bytes to bucket.
func pushedBigEntry(t *testing.T, s *Server, bucket string, priority int64, size int) {
	t.Helper()
	resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: priority, Data: map[string]interface{}{"n": int32(99), "pad": strings.Repeat("x", size)}})
	if err != nil || !resp.Success {
		t.Fatalf("pushing %d bytes into %s: %+v, %v", size, bucket, resp, err)
	}
}

// TestClaimOverTheCeilingStopsShort: a claim of 10 whose third 400 KiB entry
// passes the 1 MiB ceiling answers the first two, commits them, and is never
// RESULT_TOO_LARGE; the CLAIM refusal counter does not move.
func TestClaimOverTheCeilingStopsShort(t *testing.T) {
	qs, _ := peekCeilingServer(t)
	pad := strings.Repeat("x", 400<<10)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := "big-" + string(engine.bucketType)
			createTestBucket(t, qs.core, "app", bucket, engine.bucketType)
			pushPeekEntries(t, qs.core, bucket, 5, pad)
			refusalsBefore := queryRefusalsCounted(t, queryMethodClaim)
			answer, err := qs.core.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: bucket, Limit: 10})
			if err != nil || !answer.Found || fmt.Sprint(peekedNumbers(t, answer.Items)) != fmt.Sprint(engine.popOrder[:2]) {
				t.Fatalf("claim of 10 at a 1 MiB ceiling = %+v, %v; want the first two entries", answer, err)
			}
			if answer.Pending != 2 {
				t.Errorf("pending %d, want 2: the stopped claim committed what it took", answer.Pending)
			}
			if got := queryRefusalsCounted(t, queryMethodClaim) - refusalsBefore; got != 0 {
				t.Errorf("a claim that stopped short moved the CLAIM refusal counter by %v", got)
			}
			if left := peekAllNumbers(t, qs.core, bucket); fmt.Sprint(left) != fmt.Sprint(engine.popOrder[2:]) {
				t.Fatalf("the claim left %v, want %v", left, engine.popOrder[2:])
			}
		})
	}
}

// queryRefusalsCounted is the value of queryResultTooLarge for a method.
func queryRefusalsCounted(t *testing.T, method string) float64 {
	t.Helper()
	var metric dto.Metric
	if err := queryResultTooLarge.WithLabelValues(method).Write(&metric); err != nil {
		t.Fatalf("reading the %s refusal counter: %v", method, err)
	}
	return metric.GetCounter().GetValue()
}

// TestClaimHardCapAnswerIsNotAppliedAndCommitsTheReturn: the entry at the front
// is over the delivery hard cap, on a bucket that holds an expired lease behind
// it. The answer is INVALID_ARGUMENT with write_outcome not_applied, on a
// standalone node too, and the expired lease was returned in the same apply
// (claim-ack-spec 9.4, Q-2).
func TestClaimHardCapAnswerIsNotAppliedAndCommitsTheReturn(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeLIFO, BucketTypeHeap} {
		t.Run(string(bucketType), func(t *testing.T) {
			s := newTestServer(t)
			clock := withLeaseClock(s)
			bucket := "capped"
			createTestBucket(t, s, "app", bucket, bucketType)
			pushedBigEntry(t, s, bucket, 1, 10)
			held := claimOf(t, s, bucket, 1, 1000)
			if !held.Found {
				t.Fatalf("claiming the small entry: %+v", held)
			}
			pushedBigEntry(t, s, bucket, 100, 5<<20)
			clock.advance(5 * time.Second)

			handler := NewHandler(s)
			answer, err := handler(context.Background(), transport.NewFrame(transport.OpQueueClaim, mustMarshalBSON(t, ClaimRequest{Database: "app", Bucket: bucket})))
			if err != nil {
				t.Fatal(err)
			}
			refusal := errorAnswerOf(t, answer, "a claim of an entry over the hard cap")
			requireOutcome(t, refusal, "a claim of an entry over the hard cap", outcomeNotApplied, transport.ErrInvalidArgument)
			for _, want := range []string{"claim refused: the entry at the front of app.capped needs", "claim moves entries of at most 4.74 MiB", "Nothing was taken; expired leases were returned", "db.capped.pop()"} {
				if !strings.Contains(refusal.Message, want) {
					t.Errorf("message lacks %q: %s", want, refusal.Message)
				}
			}
			engine, _ := leaseEngineOf(lookupBucket(s, "app", bucket))
			if counted, err := engine.entries(); err != nil || counted != 2 {
				t.Fatalf("after the refusal the bucket counts %d entries (%v), want 2: the expired lease should have been returned", counted, err)
			}
		})
	}
}

// TestClaimSyncsOnceAndWaitsForNoSyncWhenEmpty: a claim of five is one sync,
// and a claim that finds nothing to take and writes nothing waits for none.
func TestClaimSyncsOnceAndWaitsForNoSyncWhenEmpty(t *testing.T) {
	s, kv, _ := leaseKVServer(t)
	handler := NewHandler(s)
	before := kv.syncs.Load()
	answer := handlerSend(t, handler, transport.OpQueueClaim, ClaimRequest{Database: "app", Bucket: "jobs", Limit: 5})
	var claimed ClaimResponse
	if err := bson.Unmarshal(answer.Payload, &claimed); err != nil || len(claimed.Items) != 5 {
		t.Fatalf("claim of five = %+v, %v", claimed, err)
	}
	if got := kv.syncs.Load() - before; got != 1 {
		t.Fatalf("a claim of five waited for %d syncs, want 1", got)
	}
	before = kv.syncs.Load()
	handlerSend(t, handler, transport.OpQueueClaim, ClaimRequest{Database: "app", Bucket: "jobs", Limit: 5})
	if got := kv.syncs.Load() - before; got != 0 {
		t.Fatalf("a claim that found nothing waited for %d syncs, want 0", got)
	}
}

// TestAPopOrClaimThatOnlyDeadLettersStillSyncs (B-7): the only leased entry is
// at its last delivery and its lease has ended, so the next pop or claim answers
// empty and still wrote (it dead-lettered the entry). It waits for one sync.
func TestAPopOrClaimThatOnlyDeadLettersStillSyncs(t *testing.T) {
	for _, verb := range []string{"pop", "claim"} {
		t.Run(verb, func(t *testing.T) {
			s, kv, clock := leaseKVServer(t)
			handler := NewHandler(s)
			if drained, err := s.PopCtx(context.Background(), PopRequest{Database: "app", Bucket: "jobs", Limit: 100}); err != nil || len(drained.Items) != 5 {
				t.Fatalf("draining the queue: %+v, %v", drained, err)
			}
			// One entry, claimed with a 1s lease five times over: each expiry
			// returns it, and the expiry after the fifth claim dead-letters it.
			pushPeekEntries(t, s, "jobs", 1, "")
			for delivery := 1; delivery <= store.MaxDeliveries; delivery++ {
				if got := claimOf(t, s, "jobs", 1, 1000); !got.Found {
					t.Fatalf("delivery %d found nothing: %+v", delivery, got)
				}
				clock.advance(5 * time.Second)
			}
			before := kv.syncs.Load()
			var answer *transport.Frame
			if verb == "pop" {
				answer = handlerSend(t, handler, transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "jobs"})
				var popped PopResponse
				if err := bson.Unmarshal(answer.Payload, &popped); err != nil || popped.Found {
					t.Fatalf("the pop = %+v, %v; want it empty", popped, err)
				}
			} else {
				answer = handlerSend(t, handler, transport.OpQueueClaim, ClaimRequest{Database: "app", Bucket: "jobs"})
				var claimed ClaimResponse
				if err := bson.Unmarshal(answer.Payload, &claimed); err != nil || claimed.Found || claimed.Dead != 1 {
					t.Fatalf("the claim = %+v, %v; want it empty with one dead entry", claimed, err)
				}
			}
			if got := kv.syncs.Load() - before; got != 1 {
				t.Fatalf("a %s that only dead-lettered waited for %d syncs, want 1", verb, got)
			}
		})
	}
}

// TestClaimReportsItsStoreErrorToTheOutcomeSink: a commit the store fails is
// reported to the apply's outcome sink as a store failure, and a batch over the
// bound as tooLarge, before the answer is worded, so both stay recognisable by
// type.
func TestClaimReportsItsStoreErrorToTheOutcomeSink(t *testing.T) {
	t.Run("commit failure", func(t *testing.T) {
		s, kv, _ := leaseKVServer(t)
		fail := func(target storage.KV, _ uint16, _ uint64, _ uint64) error {
			if target != storage.KV(kv) {
				return nil
			}
			return errInjectedStoreFailure
		}
		store.ApplyFaultHook.Store(&fail)
		t.Cleanup(func() { store.ApplyFaultHook.Store(nil) })
		ctx, outcome := withApplyOutcome(context.Background())
		answer, err := s.ClaimCtx(ctx, ClaimRequest{Database: "app", Bucket: "jobs", Limit: 2})
		if err != nil || answer.Found || !strings.Contains(answer.Message, "claim failed") {
			t.Fatalf("a claim whose commit fails = %+v, %v", answer, err)
		}
		if !outcome.noted || !errors.Is(outcome.storeFailure, store.ErrApplyFailed) || outcome.committed {
			t.Fatalf("the outcome sink holds %+v; want a store failure noted", outcome)
		}
	})
	t.Run("batch over the bound", func(t *testing.T) {
		s, kv, _ := leaseKVServer(t)
		kv.SetMaxBatchBytes(300)
		ctx, outcome := withApplyOutcome(context.Background())
		answer, err := s.ClaimCtx(ctx, ClaimRequest{Database: "app", Bucket: "jobs", Limit: 5})
		if err != nil || answer.Found {
			t.Fatalf("a claim whose batch is over the bound = %+v, %v", answer, err)
		}
		if !outcome.noted || !outcome.tooLarge || !strings.Contains(answer.Message, "does not fit in the largest batch the store commits") {
			t.Fatalf("the outcome sink holds %+v and the answer says %q; want tooLarge noted and the batch wording", outcome, answer.Message)
		}
	})
}

// TestClientLeaseStampsAreOverwritten (single node): a lease_now and a
// lease_nonce in a client's payload are ignored by a node that logs nothing, so
// a client cannot choose its lease time or its nonce.
func TestClientLeaseStampsAreOverwritten(t *testing.T) {
	s, clock := leaseServer(t)
	clientNonce := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	answer, err := s.ClaimCtx(context.Background(), ClaimRequest{Database: "app", Bucket: "fifo", Limit: 1, LeaseMs: 30_000, LeaseNow: 1 << 50, LeaseNonce: clientNonce})
	if err != nil || !answer.Found {
		t.Fatalf("claim = %+v, %v", answer, err)
	}
	if want := clock.now().Add(30 * time.Second).UTC(); !answer.LeaseDeadline.Equal(want) {
		t.Fatalf("the lease deadline is %v, want the node's clock plus 30s = %v: the client chose its lease time", answer.LeaseDeadline, want)
	}
	if nonce, _ := hex.DecodeString(answer.LeaseID); string(nonce[12:]) == string(clientNonce) {
		t.Fatalf("lease id %s carries the client's nonce", answer.LeaseID)
	}
}

// loggedContext is the context of a replicated apply: it carries a sequence, so
// a handler trusts the stamps in the payload.
func loggedContext(seq uint64) context.Context {
	return withApplySeq(context.Background(), seq)
}

// TestAMalformedLoggedLeaseVerbIsRefusedOnEveryCopy: a logged claim, ack,
// release or extend with no lease_now is ErrMalformedPayload, and a logged claim
// with no or a short nonce too, deterministically, whatever the node's clock
// says; a logged pop without lease_now skips the return step.
func TestAMalformedLoggedLeaseVerbIsRefusedOnEveryCopy(t *testing.T) {
	s, clock := leaseServer(t)
	lease := claimOf(t, s, "fifo", 1, 1000).LeaseID
	seq := uint64(0)
	nextCtx := func() context.Context { seq++; return loggedContext(seq) }

	_, err := s.ClaimCtx(nextCtx(), ClaimRequest{Database: "app", Bucket: "fifo", Limit: 1, LeaseNonce: make([]byte, 8)})
	if !errors.Is(err, ErrMalformedPayload) || !strings.Contains(err.Error(), "lease_now") {
		t.Fatalf("a logged claim with no lease_now = %v; want ErrMalformedPayload naming lease_now", err)
	}
	for _, nonce := range [][]byte{nil, make([]byte, 7)} {
		if _, err := s.ClaimCtx(nextCtx(), ClaimRequest{Database: "app", Bucket: "fifo", Limit: 1, LeaseNow: clock.now().UnixMilli(), LeaseNonce: nonce}); !errors.Is(err, ErrMalformedPayload) {
			t.Fatalf("a logged claim with a %d-byte nonce = %v; want ErrMalformedPayload", len(nonce), err)
		}
	}
	for verb, run := range map[string]func(context.Context, LeaseRequest) (*LeaseResponse, error){"ack": s.AckCtx, "release": s.ReleaseCtx, "extend": s.ExtendCtx} {
		if _, err := run(nextCtx(), LeaseRequest{Database: "app", Bucket: "fifo", LeaseID: lease, LeaseMs: 5000}); !errors.Is(err, ErrMalformedPayload) {
			t.Errorf("a logged %s with no lease_now = %v; want ErrMalformedPayload", verb, err)
		}
	}

	clock.advance(time.Hour)
	popped, err := s.PopCtx(nextCtx(), PopRequest{Database: "app", Bucket: "fifo", Limit: 5})
	if err != nil || fmt.Sprint(peekedNumbers(t, popped.Items)) != "[2 3 4 5]" {
		t.Fatalf("a logged pop with no lease_now took %+v, %v; want the four unclaimed entries and no return of the expired lease", popped, err)
	}
	stamped, err := s.PopCtx(nextCtx(), PopRequest{Database: "app", Bucket: "fifo", Limit: 5, LeaseNow: clock.now().UnixMilli()})
	if err != nil || fmt.Sprint(peekedNumbers(t, stamped.Items)) != "[1]" {
		t.Fatalf("a logged pop stamped with the later time took %+v, %v; want the expired lease's entry", stamped, err)
	}
}

// TestPopReturnsExpiredLeasesThroughTheWire: a pop on a node whose clock passed
// a lease's deadline takes the returned entries first, in the order a pop takes
// them.
func TestPopReturnsExpiredLeasesThroughTheWire(t *testing.T) {
	s, clock := leaseServer(t)
	handler := NewHandler(s)
	for _, engine := range peekEngines {
		t.Run(string(engine.bucketType), func(t *testing.T) {
			bucket := string(engine.bucketType)
			held := claimOf(t, s, bucket, 2, 1000)
			if !held.Found {
				t.Fatalf("claim: %+v", held)
			}
			clock.advance(10 * time.Second)
			answer := handlerSend(t, handler, popTestOpcode(engine.bucketType), PopRequest{Database: "app", Bucket: bucket, Limit: 5})
			var popped PopResponse
			if err := bson.Unmarshal(answer.Payload, &popped); err != nil || len(popped.Items) != 5 {
				t.Fatalf("pop of five = %+v, %v", popped, err)
			}
			if got := peekedNumbers(t, popped.Items); fmt.Sprint(got) != fmt.Sprint(engine.popOrder) {
				t.Fatalf("pop took %v after the lease expired, want all five in pop order %v", got, engine.popOrder)
			}
		})
	}
}

// TestNoDeliveryMetricHasABucketLabel (rule 11): no delivery metric, and none of
// requestsTotal's labels, names a bucket or a database.
func TestNoDeliveryMetricHasABucketLabel(t *testing.T) {
	collectors := []prometheus.Collector{
		queueClaims, queueClaimedEntries, queueLeaseSettlements, queueEntriesReturned, queueEntriesDeadLettered,
		queueLeaseClockSteps, queueClaimsEntryTooLarge, requestsTotal, requestDuration, queryResultTooLarge,
	}
	described := 0
	for _, collector := range collectors {
		descriptions := make(chan *prometheus.Desc, 16)
		go func() { collector.Describe(descriptions); close(descriptions) }()
		for description := range descriptions {
			described++
			text := description.String()
			for _, banned := range []string{"bucket", "database", "db"} {
				if _, labels, found := strings.Cut(text, "variableLabels"); found && strings.Contains(strings.ToLower(labels), banned) {
					t.Errorf("%s has a label naming %q", text, banned)
				}
			}
		}
	}
	if described < len(collectors) {
		t.Fatalf("only %d descriptors were read from %d collectors", described, len(collectors))
	}
}

// TestFindAndScanDoNotSeeClaimedOrDeadEntries (pin): a claimed entry is not in a
// scan while it is claimed (claim-ack-spec 8.1).
func TestFindAndScanDoNotSeeClaimedOrDeadEntries(t *testing.T) {
	s, _ := leaseServer(t)
	for _, bucketType := range leaseBucketTypes {
		bucket := string(bucketType)
		claimOf(t, s, bucket, 2, 0)
		scanned, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket})
		if err != nil || !scanned.Found || scanned.Total != 3 || len(scanned.Documents) != 3 {
			t.Errorf("scan of %s with two entries claimed = %+v, %v; want the three that are not claimed", bucket, scanned, err)
		}
	}
}
