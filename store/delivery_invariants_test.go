package store

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// invariantKind is the engine whose bucket the invariant checker reads: it
// decides which cursor the reachability invariant uses.
type invariantKind int

const (
	invariantFIFO invariantKind = iota
	invariantLIFO
	invariantHeap
)

func (kind invariantKind) String() string {
	return [...]string{"fifo", "lifo", "heap"}[kind]
}

// cursorViolations is the cursor half of the invariants, over a state and the
// positions of its live 0x04 keys: the half a model with no store can run too.
// Heap has no cursor, so it has none of these.
func cursorViolations(st BucketState, positions []uint64, kind invariantKind) []string {
	var violations []string
	complain := func(format string, args ...any) {
		violations = append(violations, fmt.Sprintf(format, args...))
	}
	var onTheRun int64
	for _, position := range positions {
		if position >= st.Next {
			complain("live entry at position %d is at or past next %d", position, st.Next)
		}
		switch kind {
		case invariantFIFO:
			if position < st.Head {
				complain("fifo entry at position %d lies below head %d: the cursor cannot reach it", position, st.Head)
			}
			if st.ResumeBelow > 0 && position < st.ResumeAt {
				onTheRun++
			}
		case invariantLIFO:
			if position > st.Tail {
				complain("lifo entry at position %d lies above tail %d: the cursor cannot reach it", position, st.Tail)
			}
			if st.ResumeBelow > 0 && position > st.ResumeAt {
				onTheRun++
			}
		}
	}
	if st.ResumeBelow == 0 && st.ResumeAt != 0 {
		complain("resume_at is %d with resume_below 0", st.ResumeAt)
	}
	if st.ResumeBelow < 0 {
		complain("resume_below is %d", st.ResumeBelow)
	}
	if st.ResumeBelow > 0 && onTheRun != st.ResumeBelow {
		complain("resume_below is %d, the run beyond resume_at %d holds %d live keys", st.ResumeBelow, st.ResumeAt, onTheRun)
	}
	return violations
}

// deliveryInvariantViolations walks the bucket's whole range, not its
// counters, and returns every claim-ack-spec §3.3 invariant the range and the
// state document disagree on. Later steps extend it with the 'h', 'd', 'e',
// 'c' and 'x' kinds; every randomized sequence calls it after every step.
//
// The reachability invariant is the one a counter cannot show: a live 0x04
// key the cursor can never reach is stranded, and Entries still counts it.
func deliveryInvariantViolations(kv storage.KV, slot uint16, id uint64, kind invariantKind) []string {
	snap := kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, slot, 1, id)
	if err != nil {
		return []string{fmt.Sprintf("loading the state: %v", err)}
	}
	var violations []string
	prefix := storage.Key(slot, 1, id, storage.NSQueue)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	var live, logicalBytes int64
	var positions []uint64
	liveKeys := map[string]bool{}
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			violations = append(violations, fmt.Sprintf("reading a 0x04 value: %v", err))
			continue
		}
		live++
		liveKeys[string(it.Key()[len(prefix):])] = true
		logicalBytes += int64(len(it.Key()) + len(value))
		if kind != invariantHeap {
			positions = append(positions, positionOf(it.Key()))
		}
	}
	if err := it.Err(); err != nil {
		violations = append(violations, fmt.Sprintf("walking the 0x04 keys: %v", err))
	}
	if live != st.Entries {
		violations = append(violations, fmt.Sprintf("state counts %d entries, the range holds %d live 0x04 keys", st.Entries, live))
	}
	records := deliveryRecordViolations(snap, slot, id, st, liveKeys, kind)
	violations = append(violations, records.violations...)
	// LogicalBytes is the bytes of the 0x04, 'e' and 'x' keys (spec §3.3).
	if want := logicalBytes + records.dataBytes; want != st.LogicalBytes {
		violations = append(violations, fmt.Sprintf("state counts %d logical bytes, the 0x04, 'e' and 'x' keys hold %d", st.LogicalBytes, want))
	}
	if kind == invariantHeap {
		if st.ResumeBelow != 0 || st.ResumeAt != 0 {
			violations = append(violations, fmt.Sprintf("a heap has no cursor and carries resume_at %d, resume_below %d", st.ResumeAt, st.ResumeBelow))
		}
		return violations
	}
	return append(violations, cursorViolations(st, positions, kind)...)
}

// recordCheck is what the NSDelivery walk found: the violations, and the bytes
// of the kinds LogicalBytes counts ('e' and 'x').
type recordCheck struct {
	violations []string
	dataBytes  int64
}

// deliveryRecordViolations walks the bucket's NSDelivery range and checks the
// record invariants of claim-ack-spec §3.3 against st: one 'h' and one 'd' per
// lease with the same deadline, every 'e' key under a lease that has a header,
// each header's count and bytes equal to its 'e' keys, a 'c' record only for a
// live entry, a well-formed dead record behind every 'x' key, and the state's
// counters equal to what the keys hold. live is the origin suffixes of the
// live 0x04 keys.
func deliveryRecordViolations(snap storage.KVSnapshot, slot uint16, id uint64, st BucketState, live map[string]bool, kind invariantKind) recordCheck {
	var check recordCheck
	complain := func(format string, args ...any) {
		check.violations = append(check.violations, fmt.Sprintf(format, args...))
	}
	headerAt := map[[leaseIDBytes]byte]leaseHeader{}
	deadlineAt := map[[leaseIDBytes]byte]int64{}
	type leaseTally struct{ count, bytes int64 }
	tallies := map[[leaseIDBytes]byte]*leaseTally{}
	var claimed, claimedBytes, counts, dead, deadBytes int64
	prefix := storage.Key(slot, 1, id, storage.NSDelivery)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()
		value, err := it.Value()
		if err != nil {
			complain("reading a delivery record: %v", err)
			continue
		}
		rest := key[len(prefix)+1:]
		var lease [leaseIDBytes]byte
		switch key[len(prefix)] {
		case kindLeaseHeader:
			copy(lease[:], rest)
			header, err := decodeLeaseHeader(value)
			if err != nil {
				complain("lease %x: %v", lease, err)
				continue
			}
			headerAt[lease] = header
		case kindDeadline:
			copy(lease[:], rest[8:])
			deadlineAt[lease] = int64(binary.BigEndian.Uint64(rest[:8]))
		case kindClaimed:
			copy(lease[:], rest)
			if _, _, err := parseClaimedValue(value); err != nil {
				complain("claimed entry of lease %x: %v", lease, err)
			}
			tally := tallies[lease]
			if tally == nil {
				tally = &leaseTally{}
				tallies[lease] = tally
			}
			tally.count++
			tally.bytes += int64(len(key) + len(value))
			claimed++
			claimedBytes += int64(len(key) + len(value))
			// Positions are never reused, so a queue entry is never live and
			// claimed at once; a heap twin, pushed again while claimed, is.
			if origin := rest[leaseIDBytes:]; kind != invariantHeap && live[string(origin)] {
				complain("queue entry %x is both claimed and live", origin)
			}
		case kindReturnCount:
			counts++
			if _, err := parseReturnCountValue(value); err != nil {
				complain("return count of %x: %v", rest, err)
			}
			if !live[string(rest)] {
				complain("a return count for %x, which is not a live entry", rest)
			}
		case kindDead:
			dead++
			deadBytes += int64(len(key) + len(value))
			record, err := decodeDeadRecord(value)
			switch {
			case err != nil:
				complain("dead entry %x: %v", rest, err)
			case record.Deliveries < 1 || (record.Reason != DeadReasonMaxDeliveries && record.Reason != DeadReasonCollided):
				complain("dead entry %x has deliveries %d and reason %q", rest, record.Deliveries, record.Reason)
			}
		default:
			complain("a delivery record of unknown kind %q", key[len(prefix)])
		}
	}
	if err := it.Err(); err != nil {
		complain("walking the delivery records: %v", err)
	}
	var counted int64
	for lease, header := range headerAt {
		deadline, ok := deadlineAt[lease]
		if !ok || deadline != header.Deadline {
			complain("lease %x has header deadline %d and 'd' deadline %d (present %v)", lease, header.Deadline, deadline, ok)
		}
		tally := tallies[lease]
		if tally == nil || tally.count != int64(header.Count) || tally.bytes != header.Bytes {
			complain("lease %x header counts %d entries of %d bytes, its 'e' keys hold %+v", lease, header.Count, header.Bytes, tally)
		}
		counted += int64(header.Count)
	}
	for lease := range deadlineAt {
		if _, ok := headerAt[lease]; !ok {
			complain("lease %x has a 'd' key and no header", lease)
		}
	}
	for lease := range tallies {
		if _, ok := headerAt[lease]; !ok {
			complain("lease %x has 'e' keys and no header", lease)
		}
	}
	if int64(len(headerAt)) != st.Leases || int64(len(deadlineAt)) != st.Leases {
		complain("state counts %d leases, the range holds %d headers and %d deadlines", st.Leases, len(headerAt), len(deadlineAt))
	}
	if claimed != st.Pending || counted != st.Pending {
		complain("state counts %d pending, the range holds %d 'e' keys and the headers count %d", st.Pending, claimed, counted)
	}
	if claimedBytes != st.PendingBytes {
		complain("state counts %d pending bytes, the 'e' keys hold %d", st.PendingBytes, claimedBytes)
	}
	if (st.LeaseClock == 0) != (st.Leases == 0) {
		complain("lease clock %d with %d leases", st.LeaseClock, st.Leases)
	}
	if counts != st.Returned {
		complain("state counts %d returned, the range holds %d 'c' keys", st.Returned, counts)
	}
	if dead != st.Dead || deadBytes != st.DeadBytes {
		complain("state counts %d dead of %d bytes, the 'x' keys are %d of %d bytes", st.Dead, st.DeadBytes, dead, deadBytes)
	}
	check.dataBytes = claimedBytes + deadBytes
	return check
}

// assertDeliveryInvariants fails t on the first violation set.
func assertDeliveryInvariants(t testing.TB, kv storage.KV, slot uint16, id uint64, kind invariantKind) {
	t.Helper()
	if violations := deliveryInvariantViolations(kv, slot, id, kind); len(violations) > 0 {
		t.Fatalf("%s bucket breaks its delivery invariants:\n  %s", kind, strings.Join(violations, "\n  "))
	}
}

// plainTrafficEngine is one engine driven by TestDeliveryStateInvariantsOnPlainTraffic.
type plainTrafficEngine struct {
	kind   invariantKind
	kv     storage.KV
	slot   uint16
	id     uint64
	push   func(n int) error
	popN   func(n int) (int, error)
	delete func(position int) error
}

func plainTrafficEngines(t *testing.T) []plainTrafficEngine {
	build := func(kind invariantKind) plainTrafficEngine {
		kv := kvfault.New()
		engine := plainTrafficEngine{kind: kind, kv: kv, slot: 1, id: 1}
		entry := func(n int) []byte { return queueEntry(t, n) }
		if kind == invariantHeap {
			heap := NewHeapStore(kv, 1, 1, 1, new(OwedApplies))
			engine.push = func(n int) error {
				// Priorities collide on purpose, so ties exercise the id order.
				_, err := heap.PushItem(ApplyInfo{}, int64(n%4), map[string]interface{}{"_id_": fmt.Sprintf("e%06d", n)})
				return err
			}
			engine.popN = func(n int) (int, error) {
				popped, err := heap.PopN(ApplyInfo{}, n, 0, func([]byte) error { return nil })
				return popped.Removed, err
			}
			engine.delete = heap.DeleteAtPosition
			return engine
		}
		mode := QueueFIFO
		if kind == invariantLIFO {
			mode = QueueLIFO
		}
		queue := NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), mode)
		engine.push = func(n int) error { _, err := queue.Push(ApplyInfo{}, entry(n)); return err }
		engine.popN = func(n int) (int, error) {
			popped, err := queue.PopN(ApplyInfo{}, n, 0, func([]byte) error { return nil })
			return popped.Removed, err
		}
		engine.delete = queue.DeleteAtPosition
		return engine
	}
	return []plainTrafficEngine{build(invariantFIFO), build(invariantLIFO), build(invariantHeap)}
}

// TestDeliveryStateInvariantsOnPlainTraffic (claim-ack build step 2): a seeded
// sequence of push, pop, PopN and DeleteAtPosition, with the invariant checker
// after every step. It is the plain traffic every later step's model extends.
func TestDeliveryStateInvariantsOnPlainTraffic(t *testing.T) {
	for _, engine := range plainTrafficEngines(t) {
		t.Run(engine.kind.String(), func(t *testing.T) {
			random := rand.New(rand.NewSource(20261018))
			pushed := 0
			for step := 0; step < 1000; step++ {
				var err error
				switch action := random.Intn(10); {
				case action < 4:
					err = engine.push(pushed)
					pushed++
				case action < 6:
					_, err = engine.popN(1)
				case action < 8:
					_, err = engine.popN(1 + random.Intn(4))
				default:
					snap := engine.kv.NewSnapshot()
					st, loadErr := LoadBucketState(snap, engine.slot, 1, engine.id)
					snap.Discard()
					if loadErr != nil {
						t.Fatalf("step %d: loading the state: %v", step, loadErr)
					}
					if st.Entries == 0 {
						continue
					}
					err = engine.delete(random.Intn(int(st.Entries)))
				}
				if err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				assertDeliveryInvariants(t, engine.kv, engine.slot, engine.id, engine.kind)
			}
		})
	}
}

// TestDeliveryInvariantCheckerSeesAStrandedKey is the checker's own test: a
// live 0x04 key planted beyond the cursor must fail it, or every sequence that
// calls it proves nothing.
func TestDeliveryInvariantCheckerSeesAStrandedKey(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO} {
		t.Run(kind.String(), func(t *testing.T) {
			mode := QueueFIFO
			if kind == invariantLIFO {
				mode = QueueLIFO
			}
			kv := kvfault.New()
			queue := NewQueueStore(kv, storage.BucketRef{Slot: 1, DatabaseID: 1, BucketID: 1}, new(OwedApplies), mode)
			pushNumbered(t, queue, 0, 6)
			for range 3 {
				popNumber(t, queue)
			}
			assertDeliveryInvariants(t, kv, 1, 1, kind)

			// fifo: head is 3, so position 1 is below it. lifo: tail is 2, so
			// position 4 is above it. The counters are left alone, as a bug that
			// forgets to move a cursor would leave them.
			stranded := uint64(1)
			if kind == invariantLIFO {
				stranded = 4
			}
			batch := kv.NewBatch()
			defer batch.Discard()
			if err := batch.Set(queue.positionKey(stranded), []byte{5, 0, 0, 0, 0}); err != nil {
				t.Fatalf("planting a key: %v", err)
			}
			if err := batch.Commit(false); err != nil {
				t.Fatalf("committing the planted key: %v", err)
			}
			violations := deliveryInvariantViolations(kv, 1, 1, kind)
			if !slices.ContainsFunc(violations, func(v string) bool { return strings.Contains(v, "cursor cannot reach") }) {
				t.Fatalf("a stranded key was not reported; violations: %v", violations)
			}
		})
	}
}

// TestInvariantCheckerAcceptsARestoredMaxDeliveriesRecord (§4.12): RestoreDead
// asks of a max_deliveries record only a known reason and one delivery or more,
// because a bucket's own max_deliveries (D-18) can be lower than the default.
// The checker must not ask for more than the rule it checks.
func TestInvariantCheckerAcceptsARestoredMaxDeliveriesRecord(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			record := DeadRecord{Data: s.deadData(0, 0), Deliveries: 1, DeadAt: testNow, Reason: DeadReasonMaxDeliveries}
			if err := s.restoreRecord(record); err != nil {
				t.Fatalf("RestoreDead of one delivery: %v", err)
			}
			s.invariants()
		})
	}
}
