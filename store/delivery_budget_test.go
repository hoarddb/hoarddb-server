package store

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
)

// stopShortEngine is one engine whose takeN a test drives directly.
type stopShortEngine struct {
	name string
	// build returns the five-entry store, takeN over it with the engine's own
	// take action, what is left of it in pop order, and its commit counter.
	build func(t *testing.T) (takeN func(n int, allowStop bool, visit func([]byte) error) (int, error), remaining func() []int, kv *countingKV)
	// state loads the bucket's state document.
	state func(t *testing.T, kv *countingKV) BucketState
	// nextPop is the entry the next pop takes.
	nextPop func(t *testing.T) int
}

// takeN is one take walk in its own apply, with the take action given: the
// walk PopN and ClaimN run inside their operations, driven bare so its stop
// rule is tested apart from either verb.
func takeN(engine deliveryEngine, info ApplyInfo, n int, allowStop bool, visit func(entry []byte) error,
	take func(st *BucketState, batch storage.KVBatch, key, entry []byte) error) (int, error) {
	removed := 0
	err := engine.apply(info, func(snap storage.KVSnapshot, st *BucketState, batch storage.KVBatch) error {
		if st.Entries == 0 {
			return nil
		}
		var err error
		removed, err = engine.walkTake(opScope{snap: snap, st: st, batch: batch}, takeWalk{
			n: n, allowStop: allowStop, take: take,
			visit: func(_, entry []byte) error { return visit(entry) },
		})
		return err
	})
	return removed, err
}

func stopShortEngines() []stopShortEngine {
	queueEngine := func(mode QueueMode) stopShortEngine {
		var q *QueueStore
		return stopShortEngine{
			name: modeName(mode),
			build: func(t *testing.T) (func(int, bool, func([]byte) error) (int, error), func() []int, *countingKV) {
				var kv *countingKV
				q, kv = newTestQueue(t, mode)
				pushNumbered(t, q, 0, 5)
				return func(n int, allowStop bool, visit func([]byte) error) (int, error) {
						return takeN(q, ApplyInfo{}, n, allowStop, visit, removeQueueEntry)
					}, func() []int {
						return entryNumbers(t, peekAll(t, q, 10))
					}, kv
			},
			state:   func(t *testing.T, _ *countingKV) BucketState { return queueState(t, q) },
			nextPop: func(t *testing.T) int { return popNumber(t, q) },
		}
	}
	var heap *HeapStore
	heapEngine := stopShortEngine{
		name: "heap",
		build: func(t *testing.T) (func(int, bool, func([]byte) error) (int, error), func() []int, *countingKV) {
			var kv *countingKV
			heap, kv = newCountedHeap(t)
			for n := 0; n < 5; n++ {
				// Distinct priorities, highest first, so the pop order is the push order.
				if _, err := heap.PushItem(ApplyInfo{}, int64(100-n), map[string]interface{}{"_id_": fmt.Sprintf("e%d", n), "n": int32(n)}); err != nil {
					t.Fatalf("PushItem(%d): %v", n, err)
				}
			}
			return func(n int, allowStop bool, visit func([]byte) error) (int, error) {
					return takeN(heap, ApplyInfo{}, n, allowStop, visit, removeElement)
				}, func() []int {
					var left []int
					if err := heap.PeekN(10, func(entry []byte) error {
						left = append(left, entryNumber(t, entry))
						return nil
					}); err != nil {
						t.Fatalf("PeekN: %v", err)
					}
					return left
				}, kv
		},
		state: func(t *testing.T, kv *countingKV) BucketState {
			snap := kv.NewSnapshot()
			defer snap.Discard()
			st, err := LoadBucketState(snap, heap.slot, 1, heap.id)
			if err != nil {
				t.Fatalf("loading the heap's state: %v", err)
			}
			return st
		},
		nextPop: func(t *testing.T) int {
			value, err := heap.PopItem(ApplyInfo{})
			if err != nil || value == nil {
				t.Fatalf("PopItem: %v (empty %v)", err, value == nil)
			}
			return entryNumber(t, value)
		},
	}
	return []stopShortEngine{queueEngine(QueueFIFO), queueEngine(QueueLIFO), heapEngine}
}

// stopAtTheThird is a visitor that stops the walk at its third entry.
func stopAtTheThird() (visit func([]byte) error, visited *int) {
	count := 0
	return func([]byte) error {
		count++
		if count == 3 {
			return fmt.Errorf("the budget is spent: %w", ErrStopShort)
		}
		return nil
	}, &count
}

// TestTakeNStopShortKeepsWhatWasTaken (claim-ack-spec §4.9, OQ-S9): a visitor
// that stops the walk with ErrStopShort at the third of five ends it before
// that entry's take, keeps the two before it in one commit, and is neither an
// error nor a lost cursor. The entries and the cursor that remain are a bucket
// two pops have drawn from.
func TestTakeNStopShortKeepsWhatWasTaken(t *testing.T) {
	for _, engine := range stopShortEngines() {
		t.Run(engine.name, func(t *testing.T) {
			takeN, remaining, kv := engine.build(t)
			before := remaining()
			kv.commits.Store(0)
			visit, visited := stopAtTheThird()
			removed, err := takeN(5, true, visit)
			if err != nil {
				t.Fatalf("takeN stopped by ErrStopShort returned %v, want nil", err)
			}
			if removed != 2 || *visited != 3 {
				t.Fatalf("takeN removed %d after visiting %d, want 2 after 3", removed, *visited)
			}
			if commits := kv.commits.Load(); commits != 1 {
				t.Fatalf("takeN committed %d batches, want 1", commits)
			}
			if left := remaining(); !slices.Equal(left, before[2:]) {
				t.Fatalf("after the stop the bucket holds %v, want %v", left, before[2:])
			}
			if st := engine.state(t, kv); st.Entries != 3 {
				t.Fatalf("Entries is %d after taking 2 of 5, want 3", st.Entries)
			}
			if next := engine.nextPop(t); next != before[2] {
				t.Fatalf("the next pop took entry %d, want %d: the cursor did not follow the take", next, before[2])
			}
		})
	}
}

// TestPopNTreatsErrStopShortAsARefusal keeps pop-n's no-short-take rule: PopN
// never lets a walk end short, so a visitor that returns ErrStopShort is an
// ordinary abort and nothing is removed.
func TestPopNTreatsErrStopShortAsARefusal(t *testing.T) {
	for _, engine := range bucketPopNEngines()[:3] {
		t.Run(engine.name, func(t *testing.T) {
			subject := engine.build(t)
			subject.push(0, 5)
			before := entryNumbersFromSubject(t, subject)
			subject.kv.commits.Store(0)
			visit, _ := stopAtTheThird()
			removed, err := subject.popN(ApplyInfo{}, 5, visit)
			if !errors.Is(err, ErrStopShort) {
				t.Fatalf("PopN whose visitor stopped it = (%d, %v), want an error wrapping ErrStopShort", removed, err)
			}
			if commits := subject.kv.commits.Load(); commits != 0 {
				t.Fatalf("PopN committed %d batches after its visitor refused, want none", commits)
			}
			if after := entryNumbersFromSubject(t, subject); !slices.Equal(after, before) {
				t.Fatalf("PopN left %v, want the bucket unchanged: %v", after, before)
			}
		})
	}
}

// entryNumbersFromSubject lists what a subject holds, in pop order.
func entryNumbersFromSubject(t *testing.T, subject popNSubject) []int {
	t.Helper()
	var numbers []int
	for _, name := range subject.ids(subject.peekN(10)) {
		var number int
		if _, err := fmt.Sscanf(name[len(name)-1:], "%d", &number); err != nil {
			t.Fatalf("reading an entry id %q: %v", name, err)
		}
		numbers = append(numbers, number)
	}
	return numbers
}

// TestClaimableConditions is the hard cap of claim-ack-spec §4.4 Edge 2: an
// entry is claimable only when its whole delivery footprint, its 'e' key and
// (on a heap) its id all fit. The footprints are built from the cap itself, so
// a footprint that counted the value alone is wrong at the boundary.
func TestClaimableConditions(t *testing.T) {
	if maxClaimEntryBytes != 4_967_628 {
		t.Fatalf("maxClaimEntryBytes is %d at the floor memtable, want 4,967,628", maxClaimEntryBytes)
	}
	fixed := int64(claimFixedBytes)
	queueSuffix, heapSuffix := 8, 8+36
	for _, tc := range []struct {
		name      string
		layout    claimLayout
		suffixLen int
		// repeats is the number of keys and values that repeat the suffix.
		repeats int64
		overBy  int64 // footprint minus the cap
		want    claimRefusal
	}{
		{name: "queue at the cap", layout: claimLayoutQueue, suffixLen: queueSuffix, repeats: 3, overBy: 0, want: claimOK},
		{name: "queue one over", layout: claimLayoutQueue, suffixLen: queueSuffix, repeats: 3, overBy: 1, want: claimFootprintTooLarge},
		{name: "queue below", layout: claimLayoutQueue, suffixLen: queueSuffix, repeats: 3, overBy: -1000, want: claimOK},
		{name: "heap at the cap", layout: claimLayoutHeap, suffixLen: heapSuffix, repeats: 4, overBy: 0, want: claimOK},
		{name: "heap one over", layout: claimLayoutHeap, suffixLen: heapSuffix, repeats: 4, overBy: 1, want: claimFootprintTooLarge},
		{name: "heap below", layout: claimLayoutHeap, suffixLen: heapSuffix, repeats: 4, overBy: -1000, want: claimOK},
		// A value under the cap whose id repeats four times over it.
		{name: "heap long id pushes the footprint over", layout: claimLayoutHeap, suffixLen: 8 + maxClaimIDBytes, repeats: 4, overBy: 1, want: claimFootprintTooLarge},
		{name: "heap id of 8,192", layout: claimLayoutHeap, suffixLen: 8 + 8192, repeats: 4, overBy: -1000, want: claimOK},
		{name: "heap id of 8,193", layout: claimLayoutHeap, suffixLen: 8 + 8193, repeats: 4, overBy: -1000, want: claimIDTooLong},
		{name: "heap id of 64,960", layout: claimLayoutHeap, suffixLen: 8 + 64_960, repeats: 4, overBy: -1000, want: claimIDTooLong},
		{name: "heap id of 64,970", layout: claimLayoutHeap, suffixLen: 8 + 64_970, repeats: 4, overBy: -1000, want: claimIDTooLong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valueLen := maxClaimEntryBytes - fixed - tc.repeats*int64(tc.suffixLen) + tc.overBy
			if valueLen < 0 {
				t.Fatalf("the row's value length is %d: the row is not buildable", valueLen)
			}
			if got := claimable(tc.layout, int(valueLen), tc.suffixLen); got != tc.want {
				t.Fatalf("claimable(value %d, suffix %d) = %v, want %v (cap %d, fixed %d)", valueLen, tc.suffixLen, got, tc.want, maxClaimEntryBytes, fixed)
			}
		})
	}
}

// TestClaimEntryKeyLimitStaysAboveTheIDBound is the tripwire's own pin: the
// 'e' key of the longest claimable heap id fits Badger's key limit, so the
// key-limit check cannot fire before the id bound does. If the id bound rises
// past this, the key-limit check becomes the live one.
func TestClaimEntryKeyLimitStaysAboveTheIDBound(t *testing.T) {
	longest := entryKeyBytes + 8 + maxClaimIDBytes
	if longest > storage.MaxKeyBytes {
		t.Fatalf("a claimable heap id of %d bytes has an 'e' key of %d bytes, over the %d-byte key limit: the id bound must come down or the key-limit check is live",
			maxClaimIDBytes, longest, storage.MaxKeyBytes)
	}
}

// TestDeliveryChargeCommitsAtTheBudget measures Badger's per-key overhead on
// the real store, asserts deliveryKeyOverhead covers it, and shows that a
// batch charged exactly the delivery budget commits while one charged well
// over it does not. The budget's own factor of two is a separate argument and
// needs no overhead (claim-ack-spec §4.4).
func TestDeliveryChargeCommitsAtTheBudget(t *testing.T) {
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: config.MinMemtableMB}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})

	// Fill one batch with 40-byte keys and 100-byte values until the store says
	// it is full, and read the bytes each key cost from what fit.
	const keyLen, valueLen = 40, 100
	accepted := fillBatch(t, kv, keyLen, valueLen, 1<<30)
	perKey := float64(kv.MaxBatchBytes()) / float64(accepted)
	measured := perKey - (keyLen + valueLen)
	t.Logf("a batch of %d-byte keys and %d-byte values holds %d keys: %.2f bytes each, %.2f beyond key and value", keyLen, valueLen, accepted, perKey, measured)
	if float64(deliveryKeyOverhead) < measured {
		t.Fatalf("deliveryKeyOverhead is %d, Badger measured %.2f bytes per key beyond key and value", deliveryKeyOverhead, measured)
	}

	budgetBytes, budgetKeys := DeliveryBudget()
	bigValue := int(budgetBytes / 100)
	for _, tc := range []struct {
		name     string
		valueLen int
		budget   int64
		over     int64 // multiple of the budget that must not commit
		keysOf   func(charge deliveryCharge) int64
	}{
		{name: "bytes", valueLen: bigValue, budget: budgetBytes, over: 3, keysOf: func(c deliveryCharge) int64 { return c.Bytes }},
		// Four times: Badger's count bound is 52,428 keys at the floor, so three
		// times the 13,107-key budget (39,321) still commits.
		{name: "keys", valueLen: 0, budget: budgetKeys, over: 4, keysOf: func(c deliveryCharge) int64 { return c.Keys }},
	} {
		t.Run(tc.name+" at the budget", func(t *testing.T) {
			batch := kv.NewBatch()
			defer batch.Discard()
			var charge deliveryCharge
			for tc.keysOf(charge) < tc.budget {
				key := []byte(fmt.Sprintf("charge-%s-%09d", tc.name, charge.Keys))
				if err := batch.Set(key, make([]byte, tc.valueLen)); err != nil {
					t.Fatalf("Set #%d refused with the charge at %+v of the budget (%d bytes, %d keys): %v", charge.Keys, charge, budgetBytes, budgetKeys, err)
				}
				charge.add(len(key), tc.valueLen)
			}
			if err := batch.Commit(false); err != nil {
				t.Fatalf("a batch charged %+v of the budget (%d bytes, %d keys) did not commit: %v", charge, budgetBytes, budgetKeys, err)
			}
		})
		t.Run(tc.name+" far over the budget", func(t *testing.T) {
			batch := kv.NewBatch()
			defer batch.Discard()
			var charge deliveryCharge
			var setErr error
			for tc.keysOf(charge) < tc.over*tc.budget && setErr == nil {
				key := []byte(fmt.Sprintf("over-%s-%09d", tc.name, charge.Keys))
				if setErr = batch.Set(key, make([]byte, tc.valueLen)); setErr == nil {
					charge.add(len(key), tc.valueLen)
				}
			}
			if setErr == nil {
				setErr = batch.Commit(false)
			}
			if !errors.Is(setErr, storage.ErrBatchTooLarge) {
				t.Fatalf("a batch charged %d times the %s budget (%+v) ended with %v, want ErrBatchTooLarge", tc.over, tc.name, charge, setErr)
			}
		})
	}
}

// fillBatch adds keys of keyLen and values of valueLen to a batch of kv until
// it refuses one, discards the batch, and returns how many it took.
func fillBatch(t *testing.T, kv *storage.BadgerKV, keyLen, valueLen int, ceiling int) int {
	t.Helper()
	batch := kv.NewBatch()
	defer batch.Discard()
	value := make([]byte, valueLen)
	for count := 0; count < ceiling; count++ {
		key := []byte(fmt.Sprintf("%0*d", keyLen, count))
		if err := batch.Set(key, value); err != nil {
			if !errors.Is(err, storage.ErrBatchTooLarge) {
				t.Fatalf("Set #%d: %v", count, err)
			}
			return count
		}
	}
	t.Fatalf("a batch took %d keys without refusing one", ceiling)
	return 0
}

// TestDeliveryBudgetIsTheFloorMemtables pins the two numbers at the 32 MiB
// floor, from which every member computes the same budget.
func TestDeliveryBudgetIsTheFloorMemtables(t *testing.T) {
	budgetBytes, budgetKeys := DeliveryBudget()
	if want := config.MemtableBatchBytes(config.MinMemtableMB) / 2; budgetBytes != want || budgetBytes != 2_516_582 {
		t.Errorf("the budget is %d bytes, want %d (2,516,582 at the floor)", budgetBytes, want)
	}
	if want := storage.BatchEntryBound(config.MemtableBatchBytes(config.MinMemtableMB)) / 4; budgetKeys != want {
		t.Errorf("the budget is %d keys, want %d", budgetKeys, want)
	}
}
