package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"github.com/hoarddb/hoarddb-server/storage"
	"slices"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// The fixture every record test shares. The goldens below were derived by hand
// from claim-ack-spec §3.2 (and the lease tag from an independent XXH64), not
// by running the code, so a layout change moves a literal on purpose.
const (
	recordSlot         uint16 = 0x0123
	recordBucket       uint64 = 0x0456
	recordNameHash     uint64 = 0x1122334455667788
	recordIncarnation  uint64 = 77
	recordDeadlineMs   int64  = 1_700_000_030_000
	recordLeaseHex            = "e464f1900000018bcfe5dd300102030405060708"
	recordTagHex              = "e464f190"
	recordHeapID              = "job-1"
	recordHeapPriority int64  = 5
)

var (
	recordNonce       = [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	recordEntry       = []byte{5, 0, 0, 0, 0}
	recordQueueSuffix = []byte{0, 0, 0, 0, 0, 0, 0, 9}
)

func recordLease() [20]byte {
	return composeLeaseID(recordNameHash, recordIncarnation, recordDeadlineMs, recordNonce)
}

func recordHeapSuffix() []byte {
	var inverted [8]byte
	binary.BigEndian.PutUint64(inverted[:], invertedPriority(recordHeapPriority))
	return append(inverted[:], recordHeapID...)
}

func hexOf(t *testing.T, raw []byte) string {
	t.Helper()
	return hex.EncodeToString(raw)
}

// TestDeliveryRecordLayoutIsFrozen is the on-disk format pin of claim-ack-spec
// §3.2 and §3.4: one key and one value of every kind, for a queue and for a
// heap where the suffix differs.
func TestDeliveryRecordLayoutIsFrozen(t *testing.T) {
	lease := recordLease()
	heapSuffix := recordHeapSuffix()
	headerValue := leaseHeader{Deadline: recordDeadlineMs, Count: 3, Bytes: 4096}.encode()
	queueDead, err := encodeDeadRecord(deadRecord{Data: recordEntry, Deliveries: 5, DeadAt: primitive.DateTime(recordDeadlineMs), Reason: DeadReasonMaxDeliveries})
	if err != nil {
		t.Fatalf("encoding the queue dead record: %v", err)
	}
	heapData := bson.Raw{0x15, 0, 0, 0, 0x02, '_', 'i', 'd', '_', 0, 6, 0, 0, 0, 'j', 'o', 'b', '-', '1', 0, 0}
	heapDead, err := encodeDeadRecord(deadRecord{Data: heapData, Deliveries: 5, DeadAt: primitive.DateTime(recordDeadlineMs), Reason: DeadReasonCollided})
	if err != nil {
		t.Fatalf("encoding the heap dead record: %v", err)
	}
	for _, tc := range []struct {
		name string
		got  []byte
		want string
	}{
		{"lease id", lease[:], recordLeaseHex},
		{"h key", leaseHeaderKey(recordSlot, 1, recordBucket, lease), "0123000000000000000100000000000004560d68e464f1900000018bcfe5dd300102030405060708"},
		{"h value", headerValue, "0000018bcfe5dd30000000030000000000001000"},
		{"d key", deadlineKey(recordSlot, 1, recordBucket, recordDeadlineMs, lease), "0123000000000000000100000000000004560d640000018bcfe5dd30e464f1900000018bcfe5dd300102030405060708"},
		{"e key, queue", claimedKey(recordSlot, 1, recordBucket, lease, recordQueueSuffix), "0123000000000000000100000000000004560d65e464f1900000018bcfe5dd3001020304050607080000000000000009"},
		{"e key, heap", claimedKey(recordSlot, 1, recordBucket, lease, heapSuffix), "0123000000000000000100000000000004560d65e464f1900000018bcfe5dd3001020304050607087ffffffffffffffa6a6f622d31"},
		{"e value", claimedValue(2, recordEntry), "020500000000"},
		{"c key, queue", returnCountKey(recordSlot, 1, recordBucket, recordQueueSuffix), "0123000000000000000100000000000004560d630000000000000009"},
		{"c key, heap", returnCountKey(recordSlot, 1, recordBucket, heapSuffix), "0123000000000000000100000000000004560d637ffffffffffffffa6a6f622d31"},
		{"c value", returnCountValue(3), "03"},
		{"x key, queue", deadKey(recordSlot, 1, recordBucket, deadSuffix(claimLayoutQueue, recordQueueSuffix, 0)), "0123000000000000000100000000000004560d780000000000000009"},
		{"x key, heap, sequence 2", deadKey(recordSlot, 1, recordBucket, deadSuffix(claimLayoutHeap, heapSuffix, 2)), "0123000000000000000100000000000004560d787ffffffffffffffa6a6f622d310000000000000002"},
		{"x value, queue", queueDead, "4c00000003646174610005000000001064656c69766572696573000500000009646561645f61740030dde5cf8b01000002726561736f6e000f0000006d61785f64656c697665726965730000"},
		{"x value, heap", heapDead, "5600000003646174610015000000025f69645f00060000006a6f622d3100001064656c69766572696573000500000009646561645f61740030dde5cf8b01000002726561736f6e0009000000636f6c6c696465640000"},
	} {
		if got := hexOf(t, tc.got); got != tc.want {
			t.Errorf("%s encodes as\n  %s\nwant the frozen layout\n  %s", tc.name, got, tc.want)
		}
	}

	// The suffixes the goldens assume are the ones the engines write.
	queue := NewQueueStore(nil, storage.BucketRef{Slot: recordSlot, DatabaseID: 1, BucketID: recordBucket}, new(OwedApplies), QueueFIFO)
	if got := originSuffix(queue.positionKey(9)); !bytes.Equal(got, recordQueueSuffix) {
		t.Errorf("a queue key's origin suffix is %x, the goldens assume %x", got, recordQueueSuffix)
	}
	heap := NewHeapStore(nil, recordSlot, 1, recordBucket, new(OwedApplies))
	if got := originSuffix(heap.makeKey(recordHeapID, recordHeapPriority)); !bytes.Equal(got, heapSuffix) {
		t.Errorf("a heap key's origin suffix is %x, the goldens assume %x", got, heapSuffix)
	}

	// And every value reads back.
	decoded, err := decodeLeaseHeader(headerValue)
	if err != nil || decoded != (leaseHeader{Deadline: recordDeadlineMs, Count: 3, Bytes: 4096}) {
		t.Errorf("the lease header read back as %+v, %v", decoded, err)
	}
	if returns, entry, err := parseClaimedValue(claimedValue(2, recordEntry)); err != nil || returns != 2 || !bytes.Equal(entry, recordEntry) {
		t.Errorf("the claimed value read back as (%d, %x, %v)", returns, entry, err)
	}
	if returns, err := parseReturnCountValue(returnCountValue(3)); err != nil || returns != 3 {
		t.Errorf("the return count read back as (%d, %v)", returns, err)
	}
	record, err := decodeDeadRecord(heapDead)
	if err != nil || record.Reason != DeadReasonCollided || record.Deliveries != 5 || !bytes.Equal(record.Data, heapData) {
		t.Errorf("the dead record read back as %+v, %v", record, err)
	}
}

// TestDeliveryRecordsRefuseMalformedValues: a value that is not the layout is an
// error and never a zero.
func TestDeliveryRecordsRefuseMalformedValues(t *testing.T) {
	if _, err := decodeLeaseHeader(make([]byte, 19)); err == nil {
		t.Error("a 19-byte lease header decoded")
	}
	if _, _, err := parseClaimedValue(nil); err == nil {
		t.Error("an empty claimed value decoded")
	}
	if _, err := parseReturnCountValue([]byte{0}); err == nil {
		t.Error("a return count of 0 decoded: r is at least 1")
	}
	if _, err := parseReturnCountValue(nil); err == nil {
		t.Error("an empty return count decoded")
	}
	if _, err := decodeDeadRecord([]byte{1, 2, 3}); err == nil {
		t.Error("a malformed dead record decoded")
	}
}

// TestDeliveryKeysSortByDeadlineThenLease: the first 'd' key is the soonest
// deadline, which is what makes the return step one seek. 255 and 256 are the
// pair a little-endian deadline puts in the wrong order.
func TestDeliveryKeysSortByDeadlineThenLease(t *testing.T) {
	leaseA, leaseB := recordLease(), composeLeaseID(recordNameHash, recordIncarnation, recordDeadlineMs, [8]byte{9})
	if bytes.Compare(leaseA[:], leaseB[:]) >= 0 {
		t.Fatalf("the fixture's leases are not in order: %x, %x", leaseA, leaseB)
	}
	ordered := [][]byte{
		deadlineKey(recordSlot, 1, recordBucket, 255, leaseB),
		deadlineKey(recordSlot, 1, recordBucket, 256, leaseA),
		deadlineKey(recordSlot, 1, recordBucket, 256, leaseB),
		deadlineKey(recordSlot, 1, recordBucket, 1<<40, leaseA),
	}
	shuffled := [][]byte{ordered[3], ordered[1], ordered[2], ordered[0]}
	slices.SortFunc(shuffled, bytes.Compare)
	for index := range ordered {
		if !bytes.Equal(shuffled[index], ordered[index]) {
			t.Fatalf("'d' keys sorted as %x, want soonest deadline first, then lease id: %x", shuffled, ordered)
		}
	}
}

// TestLeaseIDIsComposedFromStampedInputs is the pure half of CA-13: the lease id
// is a function of its four inputs and reads no clock and no entropy, so every
// copy of an apply composes the same 20 bytes.
func TestLeaseIDIsComposedFromStampedInputs(t *testing.T) {
	first, second := recordLease(), recordLease()
	if first != second {
		t.Fatalf("the same inputs composed %x and then %x", first, second)
	}
	if got := hexOf(t, first[:]); got != recordLeaseHex {
		t.Fatalf("the lease id is %s, want %s: [tag 4][deadline 8][nonce 8]", got, recordLeaseHex)
	}
	if got := hexOf(t, first[:4]); got != recordTagHex {
		t.Errorf("the tag is %s, want %s: the high four bytes of xxhash64(name hash, incarnation)", got, recordTagHex)
	}
	if leaseTag(recordNameHash, recordIncarnation) != binary.BigEndian.Uint32(first[:4]) {
		t.Errorf("leaseTag disagrees with the composed id's tag")
	}
	if got := int64(binary.BigEndian.Uint64(first[4:12])); got != recordDeadlineMs {
		t.Errorf("the deadline sits at bytes 4 to 12 as %d, want %d", got, recordDeadlineMs)
	}
	if !bytes.Equal(first[12:], recordNonce[:]) {
		t.Errorf("the nonce sits at bytes 12 to 20 as %x, want %x", first[12:], recordNonce)
	}
	other := composeLeaseID(recordNameHash, recordIncarnation+1, recordDeadlineMs, recordNonce)
	if bytes.Equal(other[:4], first[:4]) {
		t.Errorf("a different incarnation kept tag %x: a recreated bucket's leases would be accepted", first[:4])
	}
	other = composeLeaseID(recordNameHash+1, recordIncarnation, recordDeadlineMs, recordNonce)
	if bytes.Equal(other[:4], first[:4]) {
		t.Errorf("a different bucket name kept tag %x: another bucket's leases would be accepted", first[:4])
	}

	text := leaseIDString(first)
	if len(text) != 40 || text != strings.ToLower(text) {
		t.Fatalf("the lease id text is %q, want 40 lowercase hex characters", text)
	}
	if parsed, err := parseLeaseID(text); err != nil || parsed != first {
		t.Errorf("parseLeaseID(%q) = %x, %v", text, parsed, err)
	}
	for name, bad := range map[string]string{
		"39 characters": text[:39],
		"41 characters": text + "0",
		"not hex":       strings.Repeat("g", 40),
		"empty":         "",
	} {
		if _, err := parseLeaseID(bad); err == nil {
			t.Errorf("parseLeaseID accepted %s: %q", name, bad)
		}
	}
}

// TestBucketNameHashSeparatesDatabaseAndBucket: the name hash is
// xxhash64(database 0x00 bucket), so a boundary moved across the separator is a
// different bucket.
func TestBucketNameHashSeparatesDatabaseAndBucket(t *testing.T) {
	if got, want := BucketNameHash("db", "orders"), xxhash.Sum64([]byte("db\x00orders")); got != want {
		t.Errorf("BucketNameHash = %#x, want xxhash64(\"db\\x00orders\") = %#x", got, want)
	}
	if BucketNameHash("ab", "c") == BucketNameHash("a", "bc") {
		t.Error("BucketNameHash(ab, c) equals BucketNameHash(a, bc): the separator is missing")
	}
}

// TestLeaseClockIsMonotonicWithinTheTolerance is claim-ack-spec §4.8: a stamp
// below the bucket's lease clock by no more than five seconds is absorbed.
func TestLeaseClockIsMonotonicWithinTheTolerance(t *testing.T) {
	const clock = int64(1_000_000)
	for _, tc := range []struct {
		name        string
		leaseClock  int64
		stamped     int64
		wantNow     int64
		wantStepped bool
	}{
		{"no clock takes the stamp", 0, 777, 777, false},
		{"a stamp at the clock", clock, clock, clock, false},
		{"a stamp above the clock", clock, clock + 10, clock + 10, false},
		{"a step back of 4 s keeps the clock", clock, clock - 4_000, clock, false},
		{"a step back of exactly 5 s keeps the clock", clock, clock - 5_000, clock, false},
		{"a step back of 6 s takes the stamp and reports it", clock, clock - 6_000, clock - 6_000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := BucketState{LeaseClock: tc.leaseClock}
			now, stepped := effectiveNow(tc.stamped, &state)
			if now != tc.wantNow || stepped != tc.wantStepped {
				t.Fatalf("effectiveNow(%d) over lease clock %d = (%d, %v), want (%d, %v)", tc.stamped, tc.leaseClock, now, stepped, tc.wantNow, tc.wantStepped)
			}
		})
	}
}

// TestLeaseClockRecoversFromAForwardJump (the clock half): the owner's clock
// jumped a day ahead and was corrected, so the stamped time is far below the
// clock the jump left, and it wins. A max with no tolerance would hold the
// bucket's time a day ahead. The guard row, that a lease granted in the window
// is returned within MaxLeaseMs of the correction, is step 7's.
func TestLeaseClockRecoversFromAForwardJump(t *testing.T) {
	const day = int64(24 * 3600 * 1000)
	jumped := BucketState{LeaseClock: recordDeadlineMs + day}
	now, stepped := effectiveNow(recordDeadlineMs+1_000, &jumped)
	if now != recordDeadlineMs+1_000 || !stepped {
		t.Fatalf("after a day's forward jump and its correction effectiveNow = (%d, %v), want the stamped %d and a reported step", now, stepped, recordDeadlineMs+1_000)
	}

	// The guard row (claim/ack build step 7): a lease granted during a +1 day
	// window is returned within MaxLeaseMs of the correction, at once here,
	// because its deadline lies a day ahead of the corrected clock, further
	// than any grant or extend can set.
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String()+"/a lease granted in the window", func(t *testing.T) {
			s := newDeliverySubject(t, kind, kvfault.New())
			s.push(0, 3)
			s.claim(2, testNow+day)
			taken, result := s.popAt(3, testNow+10_000)
			if result.Delivery.Returned != 2 || result.Delivery.ClockStep <= 5_000 || len(taken) != 3 {
				t.Fatalf("the first pop after the correction took %v with %+v, want the window's lease returned, the step reported, and all 3 taken", taken, result.Delivery)
			}
			if st := s.state(); st.Leases != 0 || st.LeaseClock != 0 {
				t.Fatalf("after the correction: leases %d, lease clock %d, want 0, 0", st.Leases, st.LeaseClock)
			}
			s.invariants()
		})
	}
}

// TestDeadSuffixIsUniquePerHeapSequence: a stable _id_ pushed again after it
// died, or a collided element and its live twin both dying, would land on one
// 'x' key without the sequence (claim-ack-spec §4.7).
func TestDeadSuffixIsUniquePerHeapSequence(t *testing.T) {
	origin := recordHeapSuffix()
	first, second := deadSuffix(claimLayoutHeap, origin, 0), deadSuffix(claimLayoutHeap, origin, 1)
	if bytes.Equal(first, second) {
		t.Fatalf("two dead entries of one heap key share the dead suffix %x", first)
	}
	if !bytes.HasPrefix(first, origin) || len(first) != len(origin)+8 {
		t.Errorf("a heap dead suffix is %x, want the origin suffix %x then an 8-byte sequence", first, origin)
	}
	if got := deadSuffix(claimLayoutQueue, recordQueueSuffix, 7); !bytes.Equal(got, recordQueueSuffix) {
		t.Errorf("a queue dead suffix is %x, want the origin suffix %x: positions are never reused", got, recordQueueSuffix)
	}
	if got := deadIDString(first); got != hex.EncodeToString(first) {
		t.Errorf("dead_id is %q, want the lowercase hex of the dead suffix", got)
	}
}

// TestDeadRecordWrapperFitsTheFootprintConstant: deadWrapperBytes, which the
// claim hard cap counts, is at least what the 'x' record adds around the stored
// entry, for a queue entry and for a heap's {_id_, priority, value} document at
// the longest id a claim takes.
func TestDeadRecordWrapperFitsTheFootprintConstant(t *testing.T) {
	stored := bson.Raw(recordEntry)
	queueRecord, err := encodeDeadRecord(deadRecord{Data: stored, Deliveries: 5, DeadAt: primitive.DateTime(recordDeadlineMs), Reason: DeadReasonMaxDeliveries})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if wrapper := len(queueRecord) - len(stored); wrapper > deadWrapperBytes {
		t.Errorf("a queue dead record adds %d bytes around its entry, deadWrapperBytes is %d", wrapper, deadWrapperBytes)
	}

	longID := strings.Repeat("i", maxClaimIDBytes)
	heapData, err := bson.Marshal(bson.D{{Key: "_id_", Value: longID}, {Key: "priority", Value: int64(5)}, {Key: "value", Value: stored}})
	if err != nil {
		t.Fatalf("encoding the heap data: %v", err)
	}
	heapRecord, err := encodeDeadRecord(deadRecord{Data: heapData, Deliveries: 5, DeadAt: primitive.DateTime(recordDeadlineMs), Reason: DeadReasonMaxDeliveries})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	// The id is a suffix copy the footprint counts separately; the wrapper is the rest.
	if wrapper := len(heapRecord) - len(stored) - len(longID); wrapper > deadWrapperBytes {
		t.Errorf("a heap dead record adds %d bytes around its value and id, deadWrapperBytes is %d", wrapper, deadWrapperBytes)
	}
}

// TestDeliveryConstantsAreFrozen pins the shared constants the server and the
// generator read one definition of (claim-ack-spec §9.3).
func TestDeliveryConstantsAreFrozen(t *testing.T) {
	for name, pair := range map[string][2]int64{
		"MaxClaimLimit":    {MaxClaimLimit, 100},
		"DefaultLeaseMs":   {DefaultLeaseMs, 30_000},
		"MinLeaseMs":       {MinLeaseMs, 1_000},
		"MaxLeaseMs":       {MaxLeaseMs, 3_600_000},
		"MaxDeliveries":    {MaxDeliveries, 5},
		"MaxDeadMoveLimit": {MaxDeadMoveLimit, 100},
		"MaxDeadListLimit": {MaxDeadListLimit, 1000},
		"MaxItemIDsPerAck": {MaxItemIDsPerAck, MaxClaimLimit},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s is %d, want %d", name, pair[0], pair[1])
		}
	}
	if DeadReasonMaxDeliveries != "max_deliveries" || DeadReasonCollided != "collided" {
		t.Errorf("the dead reasons are %q and %q", DeadReasonMaxDeliveries, DeadReasonCollided)
	}
}
