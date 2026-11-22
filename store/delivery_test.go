package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// The claim/ack store tests (claim-ack build steps 6 and 7). Every test runs on
// fifo, lifo and heap unless it says otherwise; entry n of a subject is the
// document {_id_, n}, pushed in order, and a heap gives entry n the priority
// 1000-n, so its pop order is the push order, as a fifo's is.

// testNameHash is the name hash every subject's leases are tagged with.
var testNameHash = BucketNameHash("app", "Jobs")

// testLeaseMs is the lease length the tests claim with unless they say otherwise.
const testLeaseMs = 30_000

// testNow is the stamped time the tests start at: any positive millisecond
// count works, and a round one reads well in a failure.
const testNow = int64(1_000_000_000)

// deliverySubject is one engine a delivery test drives.
type deliverySubject struct {
	name   string
	kind   invariantKind
	kv     *countingKV
	slot   uint16
	id     uint64
	layout claimLayout
	t      testing.TB
	queue  *QueueStore
	heap   *HeapStore
	nonces uint64
}

func (s *deliverySubject) mode() QueueMode {
	if s.kind == invariantLIFO {
		return QueueLIFO
	}
	return QueueFIFO
}

// newDeliverySubject builds kind over store, wrapped to count what it costs.
func newDeliverySubject(t testing.TB, kind invariantKind, store storage.KV) *deliverySubject {
	t.Helper()
	s := &deliverySubject{name: kind.String(), kind: kind, kv: &countingKV{KV: store}, slot: 1, id: 1, t: t}
	if kind == invariantHeap {
		s.layout = claimLayoutHeap
		s.heap = NewHeapStore(s.kv, s.slot, 1, s.id, new(OwedApplies))
		return s
	}
	s.queue = NewQueueStore(s.kv, storage.BucketRef{Slot: s.slot, DatabaseID: 1, BucketID: s.id}, new(OwedApplies), s.mode())
	return s
}

// deliverySubjects are fifo, lifo and heap on fresh in-memory stores.
func deliverySubjects(t testing.TB) []*deliverySubject {
	return []*deliverySubject{
		newDeliverySubject(t, invariantFIFO, kvfault.New()),
		newDeliverySubject(t, invariantLIFO, kvfault.New()),
		newDeliverySubject(t, invariantHeap, kvfault.New()),
	}
}

// openFloorBadger is a Badger store at the floor memtable, closed with the test.
func openFloorBadger(t testing.TB) *storage.BadgerKV {
	t.Helper()
	store, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return store
}

// badgerDeliverySubjects are fifo, lifo and heap, each on its own Badger store
// at the floor memtable.
func badgerDeliverySubjects(t testing.TB) []*deliverySubject {
	return []*deliverySubject{
		newDeliverySubject(t, invariantFIFO, openFloorBadger(t)),
		newDeliverySubject(t, invariantLIFO, openFloorBadger(t)),
		newDeliverySubject(t, invariantHeap, openFloorBadger(t)),
	}
}

// numbered is entry n with a body of pad bytes.
func numbered(t testing.TB, n, pad int) map[string]interface{} {
	t.Helper()
	doc := map[string]interface{}{"_id_": fmt.Sprintf("e%04d", n), "n": n}
	if pad > 0 {
		doc["pad"] = strings.Repeat("p", pad)
	}
	return doc
}

// pushPadded pushes entries from to to-1 with bodies of pad bytes.
func (s *deliverySubject) pushPadded(from, to, pad int) {
	s.t.Helper()
	for n := from; n < to; n++ {
		if s.heap != nil {
			if _, err := s.heap.PushItem(ApplyInfo{}, int64(1000-n), numbered(s.t, n, pad)); err != nil {
				s.t.Fatalf("PushItem(%d): %v", n, err)
			}
			continue
		}
		encoded, err := bson.Marshal(numbered(s.t, n, pad))
		if err != nil {
			s.t.Fatalf("encoding entry %d: %v", n, err)
		}
		if _, err := s.queue.Push(ApplyInfo{}, encoded); err != nil {
			s.t.Fatalf("Push(%d): %v", n, err)
		}
	}
}

func (s *deliverySubject) push(from, to int) { s.pushPadded(from, to, 0) }

// number reads n back from a delivered entry.
func (s *deliverySubject) number(entry []byte) int {
	s.t.Helper()
	n, ok := bson.Raw(entry).Lookup("n").Int32OK()
	if !ok {
		s.t.Fatalf("entry %v has no n", bson.Raw(entry))
	}
	return int(n)
}

// claimArgs is a claim of n at now. Each subject numbers its nonces, as an
// owner's crypto/rand would make them distinct, and two subjects that claim in
// the same order get the same ones, as two copies of one log do.
func (s *deliverySubject) claimArgs(n int, now int64) ClaimArgs {
	s.nonces++
	var nonce [8]byte
	binary.BigEndian.PutUint64(nonce[:], s.nonces)
	return ClaimArgs{N: n, LeaseMs: testLeaseMs, Now: now, Nonce: nonce, NameHash: testNameHash}
}

func (s *deliverySubject) claimN(info ApplyInfo, args ClaimArgs, visit func(entry []byte) error) (Claim, error) {
	if s.heap != nil {
		return s.heap.ClaimN(info, args, visit)
	}
	return s.queue.ClaimN(info, args, visit)
}

// claim claims n at now and returns the claim and the entry numbers it took.
func (s *deliverySubject) claim(n int, now int64) (Claim, []int) {
	s.t.Helper()
	var taken []int
	claim, err := s.claimN(ApplyInfo{}, s.claimArgs(n, now), func(entry []byte) error {
		taken = append(taken, s.number(entry))
		return nil
	})
	if err != nil {
		s.t.Fatalf("%s ClaimN(%d): %v", s.name, n, err)
	}
	if len(taken) != len(claim.Items) {
		s.t.Fatalf("%s ClaimN(%d) visited %d entries and claimed %d", s.name, n, len(taken), len(claim.Items))
	}
	return claim, taken
}

func (s *deliverySubject) ack(args LeaseArgs, itemIDs [][]byte) (LeaseResult, error) {
	if s.heap != nil {
		return s.heap.Ack(ApplyInfo{}, args, itemIDs)
	}
	return s.queue.Ack(ApplyInfo{}, args, itemIDs)
}

func (s *deliverySubject) extend(args LeaseArgs, leaseMs int64) (LeaseResult, error) {
	if s.heap != nil {
		return s.heap.Extend(ApplyInfo{}, args, leaseMs)
	}
	return s.queue.Extend(ApplyInfo{}, args, leaseMs)
}

func (s *deliverySubject) leaseArgs(lease [leaseIDBytes]byte, now int64) LeaseArgs {
	return LeaseArgs{LeaseID: lease, Now: now, NameHash: testNameHash}
}

func (s *deliverySubject) popN(info ApplyInfo, n int, now int64, visit func(entry []byte) error) (PopResult, error) {
	if s.heap != nil {
		return s.heap.PopN(info, n, now, visit)
	}
	return s.queue.PopN(info, n, now, visit)
}

// pop pops n entries with a plain pop walk and returns their numbers.
func (s *deliverySubject) pop(n int) []int {
	s.t.Helper()
	var taken []int
	visit := func(entry []byte) error {
		taken = append(taken, s.number(entry))
		return nil
	}
	if _, err := s.popN(ApplyInfo{}, n, 0, visit); err != nil {
		s.t.Fatalf("%s PopN(%d): %v", s.name, n, err)
	}
	return taken
}

// peekNumbers is every available entry, in pop order.
func (s *deliverySubject) peekNumbers() []int {
	s.t.Helper()
	var seen []int
	visit := func(entry []byte) error {
		seen = append(seen, s.number(entry))
		return nil
	}
	var err error
	if s.heap != nil {
		err = s.heap.PeekN(1<<20, visit)
	} else {
		err = s.queue.PeekN(1<<20, visit)
	}
	if err != nil {
		s.t.Fatalf("%s PeekN: %v", s.name, err)
	}
	return seen
}

func (s *deliverySubject) state() BucketState {
	s.t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, s.slot, 1, s.id)
	if err != nil {
		s.t.Fatalf("loading the state: %v", err)
	}
	return st
}

// keysOfKind is every key under the bucket's NSDelivery prefix with kind, and
// its value.
func (s *deliverySubject) keysOfKind(kind byte) map[string][]byte {
	s.t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	prefix := kindKey(s.slot, 1, s.id, kind)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	found := map[string][]byte{}
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			s.t.Fatalf("reading a %q key: %v", kind, err)
		}
		found[string(it.Key())] = value
	}
	if err := it.Err(); err != nil {
		s.t.Fatalf("walking the %q keys: %v", kind, err)
	}
	return found
}

// liveEntryKeys is how many 0x04 keys the range holds.
func (s *deliverySubject) liveEntryKeys() int {
	s.t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	prefix := storage.Key(s.slot, 1, s.id, storage.NSQueue)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer it.Close()
	live := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		live++
	}
	return live
}

func (s *deliverySubject) invariants() {
	s.t.Helper()
	assertDeliveryInvariants(s.t, s.kv, s.slot, s.id, s.kind)
}

// claimedNumbers are the numbers of the entries under lease, read from its 'e'
// keys.
func (s *deliverySubject) claimedNumbers(lease [leaseIDBytes]byte) []int {
	s.t.Helper()
	var numbers []int
	for key, value := range s.keysOfKind(kindClaimed) {
		if !bytes.HasPrefix([]byte(key), claimedKey(s.slot, 1, s.id, lease, nil)) {
			continue
		}
		_, entry, err := parseClaimedValue(value)
		if err != nil {
			s.t.Fatalf("an 'e' value: %v", err)
		}
		numbers = append(numbers, s.number(entry))
	}
	slices.Sort(numbers)
	return numbers
}

func sortedCopy(numbers []int) []int {
	sorted := slices.Clone(numbers)
	slices.Sort(sorted)
	return sorted
}

// TestClaimMovesEntriesUnderALeaseAndPopSkipsThem (CA-1, CA-2): a claim of 3
// from 5 moves them under 'e'; the count and the 0x04 range drop by 3, Pending
// counts them, and a pop of 5 takes the two left, the 4th and 5th in pop order.
func TestClaimMovesEntriesUnderALeaseAndPopSkipsThem(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			claim, taken := s.claim(3, testNow)
			st := s.state()
			if st.Entries != 2 || st.Pending != 3 || st.Leases != 1 {
				t.Fatalf("after claiming 3 of 5: entries %d, pending %d, leases %d, want 2, 3, 1", st.Entries, st.Pending, st.Leases)
			}
			if live := s.liveEntryKeys(); live != 2 {
				t.Fatalf("the range holds %d 0x04 keys after the claim, want 2", live)
			}
			if got := s.claimedNumbers(claim.LeaseID); !slices.Equal(got, sortedCopy(taken)) {
				t.Fatalf("the lease holds %v, the claim took %v", got, taken)
			}
			// A pop asked for more than is available takes the two left, in pop
			// order, and none of the claimed: its walk never reaches 0x0D.
			wantLeft := map[invariantKind][]int{invariantFIFO: {3, 4}, invariantLIFO: {1, 0}, invariantHeap: {3, 4}}[s.kind]
			if popped := s.pop(5); !slices.Equal(popped, wantLeft) {
				t.Fatalf("a pop of 5 after the claim took %v, want %v", popped, wantLeft)
			}
			s.invariants()
		})
	}
}

// TestClaimTakesInPopOrderPerType (CA-1): fifo oldest first, lifo newest
// first, heap highest priority first with ties in entry-id order.
func TestClaimTakesInPopOrderPerType(t *testing.T) {
	want := map[invariantKind][]int{invariantFIFO: {0, 1, 2}, invariantLIFO: {5, 4, 3}, invariantHeap: {0, 1, 2}}
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 6)
			claim, taken := s.claim(3, testNow)
			if !slices.Equal(taken, want[s.kind]) {
				t.Fatalf("the claim took %v, want %v", taken, want[s.kind])
			}
			for index, item := range claim.Items {
				if item.Deliveries != 1 {
					t.Fatalf("item %d has deliveries %d on its first claim, want 1", index, item.Deliveries)
				}
			}
		})
	}
	t.Run("heap/ties", func(t *testing.T) {
		heap, _ := newCountedHeap(t)
		for _, id := range []string{"tie-c", "top", "tie-a", "tie-b"} {
			priority := int64(5)
			if id == "top" {
				priority = 9
			}
			if _, err := heap.PushItem(ApplyInfo{}, priority, map[string]interface{}{"_id_": id}); err != nil {
				t.Fatalf("PushItem(%s): %v", id, err)
			}
		}
		var ids []string
		if _, err := heap.ClaimN(ApplyInfo{}, ClaimArgs{N: 4, LeaseMs: testLeaseMs, Now: testNow, NameHash: testNameHash}, func(entry []byte) error {
			ids = append(ids, heapEntryID(t, entry))
			return nil
		}); err != nil {
			t.Fatalf("ClaimN: %v", err)
		}
		if want := []string{"top", "tie-a", "tie-b", "tie-c"}; !slices.Equal(ids, want) {
			t.Fatalf("the heap claim took %v, want %v", ids, want)
		}
	})
}

// TestClaimNIsOneCommit: a claim of 5 is one batch.
func TestClaimNIsOneCommit(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			before := s.kv.commits.Load()
			if claim, _ := s.claim(5, testNow); len(claim.Items) != 5 {
				t.Fatalf("claimed %d, want 5", len(claim.Items))
			}
			if commits := s.kv.commits.Load() - before; commits != 1 {
				t.Fatalf("a claim of 5 committed %d batches, want 1", commits)
			}
		})
	}
}

// TestClaimNRefusedByTheVisitorMovesNothing: a visitor error other than
// ErrStopShort aborts the apply with nothing moved and nothing committed.
func TestClaimNRefusedByTheVisitorMovesNothing(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			before := s.kv.commits.Load()
			visited := 0
			_, err := s.claimN(ApplyInfo{}, s.claimArgs(5, testNow), func([]byte) error {
				if visited++; visited == 3 {
					return errVisitorRefused
				}
				return nil
			})
			if !errors.Is(err, errVisitorRefused) {
				t.Fatalf("ClaimN with a refusing visitor = %v, want the visitor's error", err)
			}
			if commits := s.kv.commits.Load() - before; commits != 0 {
				t.Fatalf("a refused claim committed %d batches, want 0", commits)
			}
			if claimed := len(s.keysOfKind(kindClaimed)); claimed != 0 {
				t.Fatalf("a refused claim left %d 'e' keys", claimed)
			}
			if st := s.state(); st.Entries != 5 || st.Pending != 0 {
				t.Fatalf("after a refused claim: entries %d, pending %d, want 5, 0", st.Entries, st.Pending)
			}
		})
	}
}

// TestClaimNStopShortKeepsWhatWasTaken: a visitor that returns ErrStopShort at
// the 3rd of 5 keeps the first 2 under one lease and leaves 3.
func TestClaimNStopShortKeepsWhatWasTaken(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			visited := 0
			claim, err := s.claimN(ApplyInfo{}, s.claimArgs(5, testNow), func([]byte) error {
				if visited++; visited == 3 {
					return fmt.Errorf("the ceiling: %w", ErrStopShort)
				}
				return nil
			})
			if err != nil || len(claim.Items) != 2 {
				t.Fatalf("ClaimN stopped at the 3rd = %d items, %v, want 2, nil", len(claim.Items), err)
			}
			st := s.state()
			if st.Entries != 3 || st.Pending != 2 || st.Leases != 1 {
				t.Fatalf("after a short claim: entries %d, pending %d, leases %d, want 3, 2, 1", st.Entries, st.Pending, st.Leases)
			}
			s.invariants()
		})
	}
}

// TestClaimOf100x64KiBCommitsShort (CA-4): 100 entries of 64 KiB do not fit one
// delivery budget, so the claim stops short and commits, on Badger at the
// floor memtable.
func TestClaimOf100x64KiBCommitsShort(t *testing.T) {
	for _, s := range badgerDeliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.pushPadded(0, 100, 64<<10)
			claim, _ := s.claim(100, testNow)
			if claimed := len(claim.Items); claimed == 0 || claimed >= 100 {
				t.Fatalf("a claim of 100 entries of 64 KiB claimed %d, want fewer than 100 and at least 1", claimed)
			}
			if st := s.state(); st.Pending != int64(len(claim.Items)) || st.Entries != 100-int64(len(claim.Items)) {
				t.Fatalf("after the claim: entries %d, pending %d, claimed %d", st.Entries, st.Pending, len(claim.Items))
			}
		})
	}
}

// bigStore is an in-memory store whose batch bound holds an entry above the
// claim's hard cap.
func bigStore() *kvfault.Store {
	store := kvfault.New()
	store.SetMaxBatchBytes(64 << 20)
	return store
}

// TestClaimOfAnEntryAboveTheHardCapIsRefusedByName (§4.4 Edge 2): a 5 MB entry
// at the front is refused by name, nothing claimed, the mark moved; behind a
// small entry the walk stops short before it. A heap element whose value is
// under the cap and whose footprint with its id is over it is refused too.
func TestClaimOfAnEntryAboveTheHardCapIsRefusedByName(t *testing.T) {
	const big = 5_000_000
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String()+"/front", func(t *testing.T) {
			s := newDeliverySubject(t, kind, bigStore())
			s.pushPadded(0, 1, big)
			claim, err := s.claimN(ApplyInfo{Seq: 4}, s.claimArgs(1, testNow), func([]byte) error { return nil })
			if err != nil || len(claim.Items) != 0 || claim.Refused == nil {
				t.Fatalf("a claim of a 5 MB front entry = %+v, %v, want a refusal by name and no error", claim, err)
			}
			if claim.Refused.Footprint <= maxClaimEntryBytes || claim.Refused.FootprintLimit != maxClaimEntryBytes || claim.Refused.IDTooLong {
				t.Fatalf("the refusal names %+v, want a footprint above %d", *claim.Refused, maxClaimEntryBytes)
			}
			if kind == invariantHeap && claim.Refused.EntryID != "e0000" {
				t.Fatalf("the heap refusal names id %q, want e0000", claim.Refused.EntryID)
			}
			st := s.state()
			if st.Entries != 1 || st.Pending != 0 || st.Leases != 0 || st.AppliedSeq != 4 {
				t.Fatalf("after the refusal: entries %d, pending %d, leases %d, applied %d, want 1, 0, 0, 4", st.Entries, st.Pending, st.Leases, st.AppliedSeq)
			}
		})
		t.Run(kind.String()+"/behind", func(t *testing.T) {
			s := newDeliverySubject(t, kind, bigStore())
			s.push(0, 1)
			s.pushPadded(1, 2, big)
			if kind == invariantLIFO {
				// The lifo takes the newest first: put the small one on top.
				s = newDeliverySubject(t, kind, bigStore())
				s.pushPadded(0, 1, big)
				s.push(1, 2)
			}
			claim, _ := s.claim(2, testNow)
			if len(claim.Items) != 1 || claim.Refused != nil {
				t.Fatalf("a claim of 2 with the big entry second = %d items, refused %+v, want 1 and no refusal", len(claim.Items), claim.Refused)
			}
		})
	}
	t.Run("heap/footprint with a long id", func(t *testing.T) {
		heap := NewHeapStore(bigStore(), 1, 1, 1, new(OwedApplies))
		id := strings.Repeat("i", 8000)
		value := map[string]interface{}{"_id_": id, "pad": strings.Repeat("v", int(maxClaimEntryBytes)-20_000)}
		if _, err := heap.PushItem(ApplyInfo{}, 1, value); err != nil {
			t.Fatalf("PushItem: %v", err)
		}
		claim, err := heap.ClaimN(ApplyInfo{}, ClaimArgs{N: 1, LeaseMs: testLeaseMs, Now: testNow, NameHash: testNameHash}, func([]byte) error { return nil })
		if err != nil || claim.Refused == nil || claim.Refused.IDTooLong || claim.Refused.EntryID != id {
			t.Fatalf("a heap element under the cap by value and over it by footprint = %+v, %v, want a footprint refusal naming it", claim, err)
		}
	})
}

// TestClaimOfAHeapEntryWithALongIDIsRefusedByName (§4.4 Edge 2, Badger at the
// floor): a 12 KiB id is refused at the front and ends the walk behind a small
// element; a 64,970-byte id, pushable but whose 'e' key would pass Badger's key
// limit, is refused by name and never fails the apply.
func TestClaimOfAHeapEntryWithALongIDIsRefusedByName(t *testing.T) {
	for _, idLen := range []int{12 << 10, 64_970} {
		t.Run(fmt.Sprintf("id of %d bytes/front", idLen), func(t *testing.T) {
			heap := NewHeapStore(openFloorBadger(t), 1, 1, 1, new(OwedApplies))
			id := strings.Repeat("i", idLen)
			if _, err := heap.PushItem(ApplyInfo{}, 5, map[string]interface{}{"_id_": id}); err != nil {
				t.Fatalf("PushItem: %v", err)
			}
			claim, err := heap.ClaimN(ApplyInfo{}, ClaimArgs{N: 3, LeaseMs: testLeaseMs, Now: testNow, NameHash: testNameHash}, func([]byte) error { return nil })
			if errors.Is(err, ErrApplyFailed) || err != nil {
				t.Fatalf("a claim of an element with a %d-byte id failed: %v", idLen, err)
			}
			if claim.Refused == nil || !claim.Refused.IDTooLong || claim.Refused.EntryID != id || len(claim.Items) != 0 {
				t.Fatalf("a claim of an element with a %d-byte id = %d items, refused %+v, want refused by its id", idLen, len(claim.Items), claim.Refused)
			}
		})
		t.Run(fmt.Sprintf("id of %d bytes/behind", idLen), func(t *testing.T) {
			heap := NewHeapStore(openFloorBadger(t), 1, 1, 1, new(OwedApplies))
			if _, err := heap.PushItem(ApplyInfo{}, 9, map[string]interface{}{"_id_": "small"}); err != nil {
				t.Fatalf("PushItem: %v", err)
			}
			if _, err := heap.PushItem(ApplyInfo{}, 5, map[string]interface{}{"_id_": strings.Repeat("i", idLen)}); err != nil {
				t.Fatalf("PushItem: %v", err)
			}
			claim, err := heap.ClaimN(ApplyInfo{}, ClaimArgs{N: 3, LeaseMs: testLeaseMs, Now: testNow, NameHash: testNameHash}, func([]byte) error { return nil })
			if err != nil || len(claim.Items) != 1 || claim.Refused != nil {
				t.Fatalf("a claim with the long id behind a small element = %d items, refused %+v, %v, want 1, none, nil", len(claim.Items), claim.Refused, err)
			}
		})
	}
}

// TestClaimNReplayIsIdempotent: a replayed claim at or below the mark moves
// nothing; a claim whose commit was dropped re-applies to the same entries.
func TestClaimNReplayIsIdempotent(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			first, err := s.claimN(ApplyInfo{Seq: 7}, s.claimArgs(2, testNow), func([]byte) error { return nil })
			if err != nil || len(first.Items) != 2 {
				t.Fatalf("ClaimN at sequence 7 = %d items, %v", len(first.Items), err)
			}
			replayed, err := s.claimN(ApplyInfo{Seq: 7, Replay: true}, s.claimArgs(2, testNow), func([]byte) error {
				t.Fatalf("the replay of sequence 7 visited an entry")
				return nil
			})
			if err != nil || len(replayed.Items) != 0 {
				t.Fatalf("the replay of sequence 7 = %d items, %v, want nothing", len(replayed.Items), err)
			}
			if st := s.state(); st.Entries != 3 || st.Pending != 2 || st.Leases != 1 {
				t.Fatalf("after the replay: entries %d, pending %d, leases %d, want 3, 2, 1", st.Entries, st.Pending, st.Leases)
			}

			kind := s.kind
			twin := newDeliverySubject(t, kind, kvfault.New())
			twin.push(0, 5)
			fail := func(kv storage.KV, _ uint16, _ uint64, seq uint64) error {
				if kv == storage.KV(twin.kv) && seq == 9 {
					return errors.New("injected commit failure")
				}
				return nil
			}
			// The entry at sequence 9 is one logged payload: its re-apply
			// carries the same stamps.
			logged := twin.claimArgs(2, testNow)
			ApplyFaultHook.Store(&fail)
			_, err = twin.claimN(ApplyInfo{Seq: 9}, logged, func([]byte) error { return nil })
			ApplyFaultHook.Store(nil)
			if !errors.Is(err, ErrApplyFailed) {
				t.Fatalf("a claim whose commit is dropped = %v, want ErrApplyFailed", err)
			}
			again, err := twin.claimN(ApplyInfo{Seq: 9}, logged, func([]byte) error { return nil })
			if err != nil || again.LeaseID != first.LeaseID || len(again.Items) != 2 || !bytes.Equal(again.Items[0].ItemID, first.Items[0].ItemID) {
				t.Fatalf("the re-apply of sequence 9 = %+v, %v, want the lease and items of the first claim %+v", again, err, first)
			}
		})
	}
}

// TestClaimNCursorLost: a state that counts entries its range does not hold
// is ErrQueueCursorLost, with nothing claimed.
func TestClaimNCursorLost(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			batch := s.kv.NewBatch()
			prefix := storage.Key(s.slot, 1, s.id, storage.NSQueue)
			snap := s.kv.NewSnapshot()
			it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
			for it.Seek(nil); it.Valid(); it.Next() {
				if err := batch.Delete(append([]byte(nil), it.Key()...)); err != nil {
					t.Fatalf("dropping an entry: %v", err)
				}
			}
			it.Close()
			snap.Discard()
			if err := batch.Commit(false); err != nil {
				t.Fatalf("committing the drops: %v", err)
			}
			_, err := s.claimN(ApplyInfo{}, s.claimArgs(2, testNow), func([]byte) error { return nil })
			if !errors.Is(err, ErrQueueCursorLost) {
				t.Fatalf("a claim of a range that lost its entries = %v, want ErrQueueCursorLost", err)
			}
			if st := s.state(); st.Entries != 3 || st.Pending != 0 || st.Leases != 0 {
				t.Fatalf("after the lost claim: entries %d, pending %d, leases %d, want 3, 0, 0", st.Entries, st.Pending, st.Leases)
			}
		})
	}
}

// TestClaimNReadsOnlyTheEntryNamespace: keys planted under the bucket's other
// namespaces, 0x0D kinds that are no lease's among them, are never visited,
// moved or counted by a claim.
func TestClaimNReadsOnlyTheEntryNamespace(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			planted := plantOutsideTheEntries(t, s.kv, s.slot, s.id)
			claim, taken := s.claim(100, testNow)
			if len(claim.Items) != 5 || len(taken) != 5 {
				t.Fatalf("a claim of 100 over 5 entries and planted keys took %d", len(taken))
			}
			snap := s.kv.NewSnapshot()
			defer snap.Discard()
			for key, value := range planted {
				stored, err := snap.Get([]byte(key))
				if err != nil || !bytes.Equal(stored, value) {
					t.Fatalf("planted key %x = %q (%v), want %q", key, stored, err, value)
				}
			}
			if st := s.state(); st.Pending != 5 || st.Entries != 0 {
				t.Fatalf("after the claim: entries %d, pending %d, want 0, 5", st.Entries, st.Pending)
			}
		})
	}
}

// TestAckDeletesExactlyTheLeasesEntries (CA-9): an ack with no item ids
// removes every entry of its lease and nothing of another lease.
func TestAckDeletesExactlyTheLeasesEntries(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 6)
			first, _ := s.claim(2, testNow)
			second, secondTaken := s.claim(2, testNow+1)
			result, err := s.ack(s.leaseArgs(first.LeaseID, testNow+2), nil)
			if err != nil || result.State != LeaseAcked || result.Acked != 2 {
				t.Fatalf("ack of the first lease = %+v, %v, want acked 2", result, err)
			}
			st := s.state()
			if st.Leases != 1 || st.Pending != 2 || st.Entries != 2 {
				t.Fatalf("after the ack: leases %d, pending %d, entries %d, want 1, 2, 2", st.Leases, st.Pending, st.Entries)
			}
			if got := s.claimedNumbers(second.LeaseID); !slices.Equal(got, sortedCopy(secondTaken)) {
				t.Fatalf("the second lease holds %v after the first was acked, want %v", got, secondTaken)
			}
			if got := s.claimedNumbers(first.LeaseID); len(got) != 0 {
				t.Fatalf("the acked lease still holds %v", got)
			}
			s.invariants()
		})
	}
}

// TestAckWithItemIDsLeavesTheRestUnderTheLease (CA-9): an ack naming one item
// removes that one and leaves the rest under the lease, with its count lowered.
func TestAckWithItemIDsLeavesTheRestUnderTheLease(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			claim, taken := s.claim(3, testNow)
			result, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+1), [][]byte{claim.Items[1].ItemID, []byte("not-an-item")})
			if err != nil || result.State != LeaseAcked || result.Acked != 1 || result.Missing != 1 || result.Remaining != 2 {
				t.Fatalf("ack of one item and one unknown = %+v, %v, want acked 1, missing 1, remaining 2", result, err)
			}
			want := sortedCopy([]int{taken[0], taken[2]})
			if got := s.claimedNumbers(claim.LeaseID); !slices.Equal(got, want) {
				t.Fatalf("the lease holds %v, want %v", got, want)
			}
			if st := s.state(); st.Pending != 2 || st.Leases != 1 {
				t.Fatalf("after the ack: pending %d, leases %d, want 2, 1", st.Pending, st.Leases)
			}
			s.invariants()
			rest, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+2), [][]byte{claim.Items[0].ItemID, claim.Items[2].ItemID})
			if err != nil || rest.Acked != 2 || rest.Remaining != 0 {
				t.Fatalf("ack of the other two = %+v, %v", rest, err)
			}
			if st := s.state(); st.Pending != 0 || st.Leases != 0 || st.LeaseClock != 0 {
				t.Fatalf("after acking every item: pending %d, leases %d, lease clock %d, want all 0", st.Pending, st.Leases, st.LeaseClock)
			}
			s.invariants()
		})
	}
}

// TestAckWithDuplicateItemIDsCountsEachOnce (§4.3 view rule): the 'e' Gets read
// the snapshot, so an id named twice must be counted once.
func TestAckWithDuplicateItemIDsCountsEachOnce(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, _ := s.claim(3, testNow)
			id := claim.Items[0].ItemID
			result, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+1), [][]byte{id, id})
			if err != nil || result.Acked != 1 || result.Remaining != 2 {
				t.Fatalf("ack naming one item twice = %+v, %v, want acked 1, remaining 2", result, err)
			}
			if st := s.state(); st.Pending != 2 {
				t.Fatalf("pending is %d after acking one item, want 2", st.Pending)
			}
			s.invariants()
		})
	}
}

// TestAckOfALeaseOfAnotherBucketIsUnknown (CA-9, CA-21): an 'h' key whose tag
// is not this bucket's, planted in this bucket's own range, answers unknown and
// changes nothing. Another bucket's lease, and a lease of a dropped and
// recreated bucket of the name, are pins: their headers are not in this range.
func TestAckOfALeaseOfAnotherBucketIsUnknown(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name+"/planted foreign tag", func(t *testing.T) {
			s.push(0, 3)
			claim, _ := s.claim(2, testNow)
			// The planted lease is this one's, its tag replaced: every key of it
			// is in this range, only the tag says it is not this bucket's.
			foreign := claim.LeaseID
			binary.BigEndian.PutUint32(foreign[0:4], ^leaseTag(testNameHash, s.state().Incarnation))
			header := s.keysOfKind(kindLeaseHeader)[string(leaseHeaderKey(s.slot, 1, s.id, claim.LeaseID))]
			batch := s.kv.NewBatch()
			if err := batch.Set(leaseHeaderKey(s.slot, 1, s.id, foreign), header); err != nil {
				t.Fatalf("planting: %v", err)
			}
			if err := batch.Commit(false); err != nil {
				t.Fatalf("planting: %v", err)
			}
			before := s.state()
			result, err := s.ack(s.leaseArgs(foreign, testNow+1), nil)
			if err != nil || result.State != LeaseUnknown || result.Acked != 0 {
				t.Fatalf("ack of a lease with a foreign tag = %+v, %v, want unknown", result, err)
			}
			if after := s.state(); after.Pending != before.Pending || after.Leases != before.Leases {
				t.Fatalf("the unknown ack changed pending %d->%d, leases %d->%d", before.Pending, after.Pending, before.Leases, after.Leases)
			}
			if got := len(s.claimedNumbers(claim.LeaseID)); got != 2 {
				t.Fatalf("the real lease holds %d entries after the unknown ack, want 2", got)
			}
		})
	}
	t.Run("another bucket's lease", func(t *testing.T) {
		store := kvfault.New()
		jobs := newDeliverySubject(t, invariantFIFO, store)
		other := newDeliverySubject(t, invariantFIFO, store)
		other.id = 2
		other.queue = NewQueueStore(other.kv, storage.BucketRef{Slot: other.slot, DatabaseID: 1, BucketID: other.id}, new(OwedApplies), QueueFIFO)
		jobs.push(0, 2)
		other.push(0, 2)
		claim, _ := other.claim(1, testNow)
		result, err := jobs.ack(jobs.leaseArgs(claim.LeaseID, testNow+1), nil)
		if err != nil || result.State != LeaseUnknown {
			t.Fatalf("ack of another bucket's lease = %+v, %v, want unknown", result, err)
		}
		if st := other.state(); st.Pending != 1 {
			t.Fatalf("the other bucket's lease lost its entry: pending %d", st.Pending)
		}
	})
	t.Run("a recreated bucket's lease", func(t *testing.T) {
		s := newDeliverySubject(t, invariantFIFO, kvfault.New())
		s.push(0, 2)
		claim, _ := s.claim(1, testNow)
		recreated := newDeliverySubject(t, invariantFIFO, kvfault.New())
		recreated.push(0, 2)
		if result, err := recreated.ack(recreated.leaseArgs(claim.LeaseID, testNow+1), nil); err != nil || result.State != LeaseUnknown {
			t.Fatalf("ack of the dropped bucket's lease = %+v, %v, want unknown", result, err)
		}
	})
}

// TestExtendMovesOnlyTheDeadline (CA-11): the 'e' keys stay, the 'd' key
// moves and the 'h' header carries the new deadline.
func TestExtendMovesOnlyTheDeadline(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			claim, _ := s.claim(2, testNow)
			claimedBefore := s.keysOfKind(kindClaimed)
			result, err := s.extend(s.leaseArgs(claim.LeaseID, testNow+10_000), 60_000)
			if err != nil || result.State != LeaseExtended || result.Deadline != testNow+70_000 {
				t.Fatalf("extend = %+v, %v, want extended to %d", result, err, testNow+70_000)
			}
			claimedAfter := s.keysOfKind(kindClaimed)
			if len(claimedAfter) != len(claimedBefore) {
				t.Fatalf("extend moved the 'e' keys: %d before, %d after", len(claimedBefore), len(claimedAfter))
			}
			for key, value := range claimedBefore {
				if !bytes.Equal(claimedAfter[key], value) {
					t.Fatalf("extend rewrote 'e' key %x", key)
				}
			}
			deadlines := s.keysOfKind(kindDeadline)
			if _, ok := deadlines[string(deadlineKey(s.slot, 1, s.id, testNow+70_000, claim.LeaseID))]; !ok || len(deadlines) != 1 {
				t.Fatalf("after extend the 'd' keys are %d and the new one is present %v", len(deadlines), ok)
			}
			header, err := decodeLeaseHeader(s.keysOfKind(kindLeaseHeader)[string(leaseHeaderKey(s.slot, 1, s.id, claim.LeaseID))])
			if err != nil || header.Deadline != testNow+70_000 || header.Count != 2 {
				t.Fatalf("after extend the header is %+v, %v", header, err)
			}
			s.invariants()
		})
	}
}

// TestLeaseIDIsComposedInTheApply (CA-13): two copies with the same payload and
// state compose the same 20 bytes; a different incarnation changes the tag.
func TestLeaseIDIsComposedInTheApply(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String(), func(t *testing.T) {
			copies := make([]Claim, 3)
			for index := range copies {
				s := newDeliverySubject(t, kind, kvfault.New())
				if index == 2 {
					setIncarnation(t, s, 42)
				}
				s.push(0, 3)
				copies[index], _ = s.claim(2, testNow)
			}
			if copies[0].LeaseID != copies[1].LeaseID {
				t.Fatalf("two copies composed %x and %x", copies[0].LeaseID, copies[1].LeaseID)
			}
			if bytes.Equal(copies[0].LeaseID[:4], copies[2].LeaseID[:4]) {
				t.Fatalf("another incarnation composed the same tag %x", copies[2].LeaseID[:4])
			}
			if !bytes.Equal(copies[0].LeaseID[4:], copies[2].LeaseID[4:]) {
				t.Fatalf("another incarnation changed more than the tag: %x and %x", copies[0].LeaseID, copies[2].LeaseID)
			}
			if deadline := int64(binary.BigEndian.Uint64(copies[0].LeaseID[4:12])); deadline != testNow+testLeaseMs || copies[0].Deadline != deadline {
				t.Fatalf("the lease id carries deadline %d and the claim %d, want %d", deadline, copies[0].Deadline, testNow+testLeaseMs)
			}
		})
	}
}

// setIncarnation writes the bucket's incarnation the way a create does.
func setIncarnation(t testing.TB, s *deliverySubject, incarnation uint64) {
	t.Helper()
	op := func(_ storage.KVSnapshot, st *BucketState, _ storage.KVBatch) error {
		st.Incarnation = incarnation
		return nil
	}
	var err error
	if s.heap != nil {
		err = s.heap.apply(ApplyInfo{}, op)
	} else {
		err = s.queue.apply(ApplyInfo{}, op)
	}
	if err != nil {
		t.Fatalf("setting the incarnation: %v", err)
	}
}

// TestLeaseClockClearsWithTheLastLease (§4.8): the lease clock is the
// effective now while a lease is out, and 0 once the last one is acked.
func TestLeaseClockClearsWithTheLastLease(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			first, _ := s.claim(1, testNow)
			second, _ := s.claim(1, testNow+5)
			if st := s.state(); st.LeaseClock != testNow+5 {
				t.Fatalf("with two leases out the lease clock is %d, want %d", st.LeaseClock, testNow+5)
			}
			if _, err := s.ack(s.leaseArgs(first.LeaseID, testNow+6), nil); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if st := s.state(); st.LeaseClock != testNow+6 {
				t.Fatalf("with one lease out the lease clock is %d, want %d", st.LeaseClock, testNow+6)
			}
			if _, err := s.ack(s.leaseArgs(second.LeaseID, testNow+7), nil); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if st := s.state(); st.LeaseClock != 0 || st.Leases != 0 {
				t.Fatalf("after the last ack the lease clock is %d with %d leases, want 0", st.LeaseClock, st.Leases)
			}
		})
	}
}

// deliveryRun is a seeded delivery sequence over one subject.
type deliveryRun struct {
	s      *deliverySubject
	random *rand.Rand
	now    int64
	pushed int
	leases [][leaseIDBytes]byte
	items  map[[leaseIDBytes]byte][][]byte
	trail  []string
}

func (r *deliveryRun) note(format string, args ...any) {
	r.trail = append(r.trail, fmt.Sprintf(format, args...))
	if len(r.trail) > 16 {
		r.trail = r.trail[1:]
	}
}

func (r *deliveryRun) pickLease() ([leaseIDBytes]byte, bool) {
	if len(r.leases) == 0 {
		return [leaseIDBytes]byte{}, false
	}
	return r.leases[r.random.Intn(len(r.leases))], true
}

func (r *deliveryRun) forget(lease [leaseIDBytes]byte) {
	r.leases = slices.DeleteFunc(r.leases, func(held [leaseIDBytes]byte) bool { return held == lease })
	delete(r.items, lease)
}

// step runs one seeded verb: push, pop and PopN at a time (so with the return
// step), delete, claim, ack (whole or by items), extend, release, and the
// clock jumping past every deadline, after which the next verb returns every
// lease, and an entry claimed and returned often enough is dead-lettered.
func (r *deliveryRun) step() error {
	s := r.s
	r.now += int64(r.random.Intn(50))
	switch action := r.random.Intn(100); {
	case action < 26:
		count := 1 + r.random.Intn(3)
		r.note("push %d", count)
		s.push(r.pushed, r.pushed+count)
		r.pushed += count
	case action < 36:
		n := 1 + r.random.Intn(3)
		r.note("pop %d", n)
		s.popAt(n, r.now)
	case action < 40:
		r.note("expire")
		r.now += 2 * testLeaseMs
	case action < 45:
		st := s.state()
		if st.Entries == 0 || s.heap != nil {
			return nil
		}
		// DeleteAtPosition reaches the entries at or after Head (BUG-0619).
		index := r.random.Intn(int(st.Entries))
		r.note("delete %d", index)
		if err := s.queue.DeleteAtPosition(index); err != nil && !strings.Contains(err.Error(), "out of range") {
			return err
		}
	case action < 49:
		return r.deadVerb()
	case action < 66:
		n := 1 + r.random.Intn(5)
		r.note("claim %d", n)
		claim, _ := s.claim(n, r.now)
		if len(claim.Items) > 0 {
			r.leases = append(r.leases, claim.LeaseID)
			ids := make([][]byte, 0, len(claim.Items))
			for _, item := range claim.Items {
				ids = append(ids, item.ItemID)
			}
			r.items[claim.LeaseID] = ids
		}
	case action < 76:
		lease, ok := r.pickLease()
		if !ok {
			return nil
		}
		r.note("ack %x", lease[16:])
		result, err := s.ack(s.leaseArgs(lease, r.now), nil)
		if err != nil {
			return err
		}
		if result.State != "" {
			r.forget(lease)
		}
	case action < 82:
		lease, ok := r.pickLease()
		if !ok {
			return nil
		}
		r.note("release %x", lease[16:])
		if _, err := s.release(s.leaseArgs(lease, r.now)); err != nil {
			return err
		}
		r.forget(lease)
	case action < 92:
		lease, ok := r.pickLease()
		if !ok || len(r.items[lease]) == 0 {
			return nil
		}
		ids := r.items[lease]
		picked := ids[r.random.Intn(len(ids))]
		r.note("ack item of %x", lease[16:])
		result, err := s.ack(s.leaseArgs(lease, r.now), [][]byte{picked, picked})
		if err != nil {
			return err
		}
		switch {
		case result.State != LeaseAcked:
			r.forget(lease)
		default:
			r.items[lease] = slices.DeleteFunc(ids, func(id []byte) bool { return bytes.Equal(id, picked) })
			if result.Remaining == 0 {
				r.forget(lease)
			}
		}
	default:
		lease, ok := r.pickLease()
		if !ok {
			return nil
		}
		r.note("extend %x", lease[16:])
		if _, err := s.extend(s.leaseArgs(lease, r.now), int64(1_000+r.random.Intn(60_000))); err != nil {
			return err
		}
	}
	return nil
}

// deadVerb runs one dead verb of a seeded sequence: a restore of an entry the
// sequence has not pushed yet (so a heap push later makes its live twin), or a
// requeue or a discard of one listed dead entry or of all of them.
func (r *deliveryRun) deadVerb() error {
	s := r.s
	listed, _ := s.deadPage(5, nil)
	pick := r.random.Intn(5)
	if len(listed) == 0 || pick == 0 {
		r.note("restore %d", r.pushed)
		s.restore(r.pushed, 0)
		return nil
	}
	sel := DeadSelector{DeadID: listed[r.random.Intn(len(listed))].DeadID}
	if pick == 4 {
		sel = DeadSelector{All: true}
	}
	verb, name := s.requeue, "requeue"
	if pick%2 == 0 {
		verb, name = s.discard, "discard"
	}
	r.note("%s %+v", name, sel)
	_, err := verb(sel)
	return err
}

// TestDeliveryStateInvariants is claim-ack-spec §3.3's invariants, reachability
// among them, after every step of a seeded sequence of push, pop, PopN,
// delete, claim, ack, release, extend, expiry and dead-lettering on every
// engine, checked by walking the range, not by reading the counters.
func TestDeliveryStateInvariants(t *testing.T) {
	steps := 10_000
	if testing.Short() || raceEnabled {
		steps = 2_000
	}
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			run := &deliveryRun{s: s, random: rand.New(rand.NewSource(20261018)), now: testNow, items: map[[leaseIDBytes]byte][][]byte{}}
			var reached BucketState
			for step := 0; step < steps; step++ {
				if err := run.step(); err != nil {
					t.Fatalf("step %d (%v): %v", step, run.trail, err)
				}
				if violations := deliveryInvariantViolations(s.kv, s.slot, s.id, s.kind); len(violations) > 0 {
					t.Fatalf("step %d (%v) breaks the invariants:\n  %s", step, run.trail, strings.Join(violations, "\n  "))
				}
				if step%100 == 99 {
					// The in-memory store's iterators cost every key it has
					// ever held; compaction, which a real store also runs,
					// keeps 10,000 steps to seconds and changes no answer.
					s.kv.KV.(*kvfault.Store).Compact()
				}
				st := s.state()
				reached.Leases = max(reached.Leases, st.Leases)
				reached.Returned = max(reached.Returned, st.Returned)
				reached.Dead = max(reached.Dead, st.Dead)
				reached.ResumeBelow = max(reached.ResumeBelow, st.ResumeBelow)
			}
			// A sequence that never reached a state proves nothing about it.
			if reached.Leases == 0 || reached.Returned == 0 || reached.Dead == 0 || (s.kind != invariantHeap && reached.ResumeBelow == 0) {
				t.Fatalf("the sequence never reached every delivery state: at most %d leases, %d returned, %d dead, resume_below %d",
					reached.Leases, reached.Returned, reached.Dead, reached.ResumeBelow)
			}
		})
	}
}

// The return step, the overlay, release and dead-lettering (claim/ack build
// step 7).

// expired is a time past every lease the tests claim at testNow.
const expired = testNow + 10*testLeaseMs

func (s *deliverySubject) release(args LeaseArgs) (LeaseResult, error) {
	if s.heap != nil {
		return s.heap.Release(ApplyInfo{}, args)
	}
	return s.queue.Release(ApplyInfo{}, args)
}

// popAt pops n at now, running the return step, and returns the numbers taken.
func (s *deliverySubject) popAt(n int, now int64) ([]int, PopResult) {
	s.t.Helper()
	var taken []int
	result, err := s.popN(ApplyInfo{}, n, now, func(entry []byte) error {
		taken = append(taken, s.number(entry))
		return nil
	})
	if err != nil {
		s.t.Fatalf("%s PopN(%d) at %d: %v", s.name, n, now, err)
	}
	return taken, result
}

// returnDue runs the return step at now without taking anything: an ack of a
// lease no bucket issued, which commits the return step's work.
func (s *deliverySubject) returnDue(now int64) LeaseResult {
	s.t.Helper()
	result, err := s.ack(s.leaseArgs([leaseIDBytes]byte{0xee}, now), nil)
	if err != nil || result.State != LeaseUnknown {
		s.t.Fatalf("%s ack of no lease at %d = %+v, %v, want unknown", s.name, now, result, err)
	}
	return result
}

// withDeliveryBudget sets the delivery budget for one test: the seam the spec
// names for a return step that is bounded before it reaches every lease.
func withDeliveryBudget(t testing.TB, bytes, keys int64) {
	t.Helper()
	savedBytes, savedKeys := deliveryBudgetBytes, deliveryBudgetKeys
	deliveryBudgetBytes, deliveryBudgetKeys = bytes, keys
	t.Cleanup(func() { deliveryBudgetBytes, deliveryBudgetKeys = savedBytes, savedKeys })
}

// firstInPopOrder is entry numbers 0..n-1's pop order for kind.
func firstInPopOrder(kind invariantKind, pushed, n int) []int {
	order := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if kind == invariantLIFO {
			order = append(order, pushed-1-i)
			continue
		}
		order = append(order, i)
	}
	return order
}

// TestPopReturnsExpiredLeasesFirst (CA-3): every entry under an expired lease,
// Entries 0: the pop's own apply returns them and takes one; the empty check
// comes after the return step.
func TestPopReturnsExpiredLeasesFirst(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.claim(3, testNow)
			if st := s.state(); st.Entries != 0 {
				t.Fatalf("after claiming everything entries is %d", st.Entries)
			}
			taken, result := s.popAt(1, expired)
			if want := firstInPopOrder(s.kind, 3, 1); !slices.Equal(taken, want) || result.Delivery.Returned != 3 {
				t.Fatalf("the pop took %v and returned %d, want %v and 3", taken, result.Delivery.Returned, want)
			}
			if st := s.state(); st.Entries != 2 || st.Leases != 0 || st.Pending != 0 {
				t.Fatalf("after the pop: entries %d, leases %d, pending %d, want 2, 0, 0", st.Entries, st.Leases, st.Pending)
			}
			s.invariants()
		})
	}
}

// TestClaimOnAQueueWhoseEntriesAreAllUnderExpiredLeasesReturnsThem (CA-3).
func TestClaimOnAQueueWhoseEntriesAreAllUnderExpiredLeasesReturnsThem(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.claim(3, testNow)
			claim, taken := s.claim(3, expired)
			if want := firstInPopOrder(s.kind, 3, 3); !slices.Equal(taken, want) || claim.Delivery.Returned != 3 {
				t.Fatalf("the claim took %v and returned %d, want %v and 3", taken, claim.Delivery.Returned, want)
			}
			s.invariants()
		})
	}
}

// TestPushDoesNotRunTheReturnStep: a push leaves an expired lease in place.
func TestPushDoesNotRunTheReturnStep(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, _ := s.claim(2, testNow)
			s.push(3, 4)
			if st := s.state(); st.Leases != 1 || st.Pending != 2 || len(s.claimedNumbers(claim.LeaseID)) != 2 {
				t.Fatalf("a push after the deadline changed the lease: leases %d, pending %d", st.Leases, st.Pending)
			}
		})
	}
}

// TestPopInTheApplyThatReturnsTakesTheReturnedEntriesFirst (CA-5, on Badger): a
// fifo of 10, claim 0 to 2, pop 3 to 5, the lease expires; one pop takes 0 and
// Head ends at 1. A walk of the snapshot alone takes 6 and leaves 0 to 2
// stranded below Head with every count agreeing. lifo and heap rows take their
// own front.
func TestPopInTheApplyThatReturnsTakesTheReturnedEntriesFirst(t *testing.T) {
	for _, s := range badgerDeliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 10)
			s.claim(3, testNow)
			s.pop(3)
			taken, result := s.popAt(1, expired)
			want := map[invariantKind]int{invariantFIFO: 0, invariantLIFO: 9, invariantHeap: 0}[s.kind]
			if len(taken) != 1 || taken[0] != want || result.Delivery.Returned != 3 {
				t.Fatalf("the pop that returned the lease took %v (returned %d), want entry %d", taken, result.Delivery.Returned, want)
			}
			st := s.state()
			if s.kind == invariantFIFO && st.Head != 1 {
				t.Fatalf("head is %d after the pop, want 1", st.Head)
			}
			if st.Entries != 6 {
				t.Fatalf("entries is %d after the pop, want 6", st.Entries)
			}
			s.invariants()
		})
	}
}

// TestClaimInTheApplyThatReturnsSeesTheReturnedEntries (CA-5): the returned
// entries are claimed again by the apply that returns them, with deliveries 2.
func TestClaimInTheApplyThatReturnsSeesTheReturnedEntries(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 6)
			_, first := s.claim(2, testNow)
			s.pop(1)
			claim, taken := s.claim(2, expired)
			if !slices.Equal(sortedCopy(taken), sortedCopy(first)) {
				t.Fatalf("the claim that returned %v took %v", first, taken)
			}
			for _, item := range claim.Items {
				if item.Deliveries != 2 {
					t.Fatalf("a returned entry was claimed with deliveries %d, want 2", item.Deliveries)
				}
			}
			s.invariants()
		})
	}
}

// pushHeapTwin pushes entry n of a heap subject again: the same _id_ and
// priority, so the same key.
func (s *deliverySubject) pushHeapTwin(n int) {
	s.t.Helper()
	if _, err := s.heap.PushItem(ApplyInfo{}, int64(1000-n), numbered(s.t, n, 0)); err != nil {
		s.t.Fatalf("pushing the twin of %d: %v", n, err)
	}
}

// TestTwoReturnsToOneHeapKeyInOneApplyCollide (CA-5, CA-8): A is claimed, its
// twin A' is pushed and claimed under a second lease, and one apply returns
// both: the first goes back, the second finds the key the first just wrote,
// through the overlay, and dies collided.
func TestTwoReturnsToOneHeapKeyInOneApplyCollide(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 1)
	s.claim(1, testNow)
	s.pushHeapTwin(0)
	s.claim(1, testNow+1)
	result := s.returnDue(expired)
	if result.Delivery.Returned != 1 || result.Delivery.DeadLettered != 1 {
		t.Fatalf("the return of both leases = %+v, want 1 returned and 1 dead", result.Delivery)
	}
	st := s.state()
	if st.Dead != 1 || st.Entries != 1 || s.liveEntryKeys() != 1 {
		t.Fatalf("after the returns: dead %d, entries %d, live keys %d, want 1, 1, 1", st.Dead, st.Entries, s.liveEntryKeys())
	}
	for _, value := range s.keysOfKind(kindDead) {
		record, err := decodeDeadRecord(value)
		if err != nil || record.Reason != DeadReasonCollided || record.Deliveries != 1 {
			t.Fatalf("the dead record is %+v, %v, want collided after 1 delivery", record, err)
		}
	}
	s.invariants()
}

// TestExpiredLeaseReturnsFIFO (§4.6): a returned fifo entry is again the
// oldest, ahead of everything pushed since.
func TestExpiredLeaseReturnsFIFO(t *testing.T) {
	s := newDeliverySubject(t, invariantFIFO, kvfault.New())
	s.push(0, 5)
	s.claim(2, testNow)
	s.push(5, 6)
	s.pop(1)
	s.returnDue(expired)
	if got, want := s.peekNumbers(), []int{0, 1, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("after the return the fifo reads %v, want %v", got, want)
	}
	s.invariants()
}

// TestExpiredLeaseReturnsLIFO (§4.6): a returned lifo entry goes below
// everything pushed since, in its original stack order.
func TestExpiredLeaseReturnsLIFO(t *testing.T) {
	s := newDeliverySubject(t, invariantLIFO, kvfault.New())
	s.push(0, 5)
	s.claim(2, testNow)
	s.push(5, 7)
	s.pop(1)
	s.returnDue(expired)
	if got, want := s.peekNumbers(), []int{5, 4, 3, 2, 1, 0}; !slices.Equal(got, want) {
		t.Fatalf("after the return the lifo reads %v, want %v", got, want)
	}
	s.invariants()
}

// TestExpiredLeaseReturnsHeap (§4.6): a returned heap element takes its place
// by priority.
func TestExpiredLeaseReturnsHeap(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 5)
	s.claim(2, testNow)
	s.push(5, 6)
	s.pop(1)
	s.returnDue(expired)
	if got, want := s.peekNumbers(), []int{0, 1, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("after the return the heap reads %v, want %v", got, want)
	}
	s.invariants()
}

// wallSubject is a queue on Badger with entries pushed in batches, so 30,000
// entries cost seconds, not minutes.
func wallSubject(t *testing.T, kind invariantKind, entries int) *deliverySubject {
	t.Helper()
	s := newDeliverySubject(t, kind, openFloorBadger(t))
	const perBatch = 1_000
	for from := 0; from < entries; from += perBatch {
		batch := make([][]byte, 0, perBatch)
		for n := from; n < from+perBatch; n++ {
			batch = append(batch, queueEntry(t, n))
		}
		if err := s.queue.PushBatch(ApplyInfo{}, batch); err != nil {
			t.Fatalf("pushing from %d: %v", from, err)
		}
	}
	return s
}

// maxVisitsOfAReturnedRun is "O(100)" for a run of 100 returned entries: the
// 'd' and 'e' keys of the lease and the run itself, a few versions each. The
// wall it must not walk is about 20,000 delete markers.
const maxVisitsOfAReturnedRun = 1_000

// TestPopAfterReturnedEntriesDoesNotWalkTheWall (§4.3 A-3, §4.6, CA-6): 30,000
// entries, claim 100, pop 19,900, the lease returns, pop the 100: the taking
// apply and the next pop each visit O(100) stored versions, not the ~20,000
// delete markers between the returned run and the cursor. The split row returns
// half the run in an earlier apply (the snapshot side) and half in the taking
// apply (the overlay side).
func TestPopAfterReturnedEntriesDoesNotWalkTheWall(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO} {
		for _, split := range []bool{false, true} {
			name := kind.String() + "/one lease"
			if split {
				name = kind.String() + "/run split between the snapshot and the overlay"
			}
			t.Run(name, func(t *testing.T) {
				s := wallSubject(t, kind, 30_000)
				var claimed []int
				if split {
					_, first := s.claim(50, testNow)
					_, second := s.claim(50, testNow+10_000)
					claimed = append(first, second...)
				} else {
					_, claimed = s.claim(100, testNow)
				}
				for popped := 0; popped < 19_900; popped += 1_000 {
					s.pop(min(1_000, 19_900-popped))
				}
				takeAt := expired
				if split {
					// The first lease alone has expired: it goes back to the
					// snapshot, then the second goes back in the taking apply.
					s.returnDue(testNow + testLeaseMs + 1)
					takeAt = testNow + 10_000 + testLeaseMs + 1
				}
				s.kv.visits.Store(0)
				taken, _ := s.popAt(100, takeAt)
				if !slices.Equal(sortedCopy(taken), sortedCopy(claimed)) {
					t.Fatalf("the taking pop took %d entries, not the %d returned", len(taken), len(claimed))
				}
				if visits := s.kv.visits.Load(); visits > maxVisitsOfAReturnedRun {
					t.Fatalf("the apply that returned and took the run visited %d stored versions, want at most %d", visits, maxVisitsOfAReturnedRun)
				}
				s.kv.visits.Store(0)
				s.popAt(1, takeAt)
				if visits := s.kv.visits.Load(); visits > maxVisitsOfAReturnedRun {
					t.Fatalf("the pop after the run visited %d stored versions, want at most %d", visits, maxVisitsOfAReturnedRun)
				}
				s.invariants()
			})
		}
	}
}

// claimReturnCycle claims n at now and lets it expire into a return; it
// returns what the claim saw and the return's report.
func (s *deliverySubject) claimReturnCycle(n int, now int64) (Claim, LeaseResult) {
	s.t.Helper()
	claim, _ := s.claim(n, now)
	return claim, s.returnDue(now + 2*testLeaseMs)
}

// TestDeliveriesCountsEachReturn (CA-7): 1, 2, 3 across three expiries.
func TestDeliveriesCountsEachReturn(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 2)
			for want := 1; want <= 3; want++ {
				claim, _ := s.claimReturnCycle(1, testNow+int64(want)*10*testLeaseMs)
				if len(claim.Items) != 1 || claim.Items[0].Deliveries != want {
					t.Fatalf("claim %d delivered %+v, want deliveries %d", want, claim.Items, want)
				}
			}
			s.invariants()
		})
	}
}

// TestReturnedCountCostsNothingWhenNoneReturned (CA-7, rule 11): a bucket whose
// entries were claimed and acked but never returned has no 'c' key, no
// returned field, and pays no 'c' lookup.
func TestReturnedCountCostsNothingWhenNoneReturned(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 6)
			claim, _ := s.claim(2, testNow)
			if _, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+1), nil); err != nil {
				t.Fatalf("ack: %v", err)
			}
			s.claim(2, testNow+2)
			s.popAt(2, testNow+3)
			if gets := s.kv.returnCountGets.Load(); gets != 0 {
				t.Fatalf("a bucket with nothing returned looked up %d 'c' records", gets)
			}
			if counts := len(s.keysOfKind(kindReturnCount)); counts != 0 {
				t.Fatalf("a bucket with nothing returned holds %d 'c' keys", counts)
			}
			snap := s.kv.NewSnapshot()
			defer snap.Discard()
			encoded, err := snap.Get(storage.Key(s.slot, 1, s.id, storage.NSState))
			if err != nil {
				t.Fatalf("reading the state: %v", err)
			}
			if _, err := bson.Raw(encoded).LookupErr("returned"); err == nil {
				t.Fatalf("the state document carries returned with nothing returned")
			}
		})
	}
}

// TestHeapPushOverAReturnedKeyDropsItsCount (§4.10): a push over a returned
// element replaces it with one that was never delivered, so its 'c' record
// goes, and the next claim delivers it with deliveries 1.
func TestHeapPushOverAReturnedKeyDropsItsCount(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 1)
	s.claimReturnCycle(1, testNow)
	if st := s.state(); st.Returned != 1 {
		t.Fatalf("after the return the bucket counts %d returned, want 1", st.Returned)
	}
	s.pushHeapTwin(0)
	if st := s.state(); st.Returned != 0 || len(s.keysOfKind(kindReturnCount)) != 0 {
		t.Fatalf("after the push over it the bucket counts %d returned", st.Returned)
	}
	claim, _ := s.claim(1, expired+10*testLeaseMs)
	if len(claim.Items) != 1 || claim.Items[0].Deliveries != 1 {
		t.Fatalf("the pushed element was claimed with %+v, want deliveries 1", claim.Items)
	}
	s.invariants()
}

// TestPopOfAReturnedEntryDeletesItsCount (§4.10): a pop of a returned entry
// takes its 'c' record with it, from an earlier apply and from its own.
func TestPopOfAReturnedEntryDeletesItsCount(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name+"/returned earlier", func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.claimReturnCycle(2, testNow)
			s.popAt(3, expired+10*testLeaseMs)
			if st := s.state(); st.Returned != 0 || len(s.keysOfKind(kindReturnCount)) != 0 {
				t.Fatalf("after popping the returned entries the bucket counts %d returned", st.Returned)
			}
			s.invariants()
		})
	}
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String()+"/returned by the pop", func(t *testing.T) {
			s := newDeliverySubject(t, kind, kvfault.New())
			s.push(0, 3)
			s.claim(2, testNow)
			s.popAt(3, expired)
			if st := s.state(); st.Returned != 0 || len(s.keysOfKind(kindReturnCount)) != 0 {
				t.Fatalf("after popping what the pop returned the bucket counts %d returned", st.Returned)
			}
			s.invariants()
		})
	}
}

// TestKthReturnDeadLetters (§4.7, CA-8): at the 5th return the entry goes to
// the dead list, not the queue.
func TestKthReturnDeadLetters(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 2)
			var last LeaseResult
			for cycle := 1; cycle <= MaxDeliveries; cycle++ {
				_, last = s.claimReturnCycle(1, testNow+int64(cycle)*10*testLeaseMs)
			}
			if last.Delivery.DeadLettered != 1 || last.Delivery.Returned != 0 {
				t.Fatalf("the 5th return = %+v, want 1 dead-lettered", last.Delivery)
			}
			st := s.state()
			if st.Dead != 1 || st.Entries != 1 || st.Pending != 0 || st.Returned != 0 {
				t.Fatalf("after the 5th return: dead %d, entries %d, pending %d, returned %d, want 1, 1, 0, 0", st.Dead, st.Entries, st.Pending, st.Returned)
			}
			for _, value := range s.keysOfKind(kindDead) {
				record, err := decodeDeadRecord(value)
				if err != nil || record.Reason != DeadReasonMaxDeliveries || record.Deliveries != MaxDeliveries {
					t.Fatalf("the dead record is %+v, %v, want max_deliveries after %d", record, err, MaxDeliveries)
				}
			}
			s.invariants()
		})
	}
}

// TestHeapReturnCollisionDeadLettersCollided (§4.7): an element whose key a
// pushed twin holds when it comes back is dead-lettered collided; the twin
// stays.
func TestHeapReturnCollisionDeadLettersCollided(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 1)
	s.claim(1, testNow)
	s.pushHeapTwin(0)
	result := s.returnDue(expired)
	if result.Delivery.DeadLettered != 1 || result.Delivery.Collided != 1 {
		t.Fatalf("the return = %+v, want 1 dead-lettered, of which 1 collided", result.Delivery)
	}
	if st := s.state(); st.Dead != 1 || st.Entries != 1 {
		t.Fatalf("after the collision: dead %d, entries %d, want 1, 1", st.Dead, st.Entries)
	}
	s.invariants()
}

// TestHeapCollisionTwiceKeepsBothDeadEntries (§4.7, A-5): one origin suffix
// dies twice; the dead sequence keeps both dead entries.
func TestHeapCollisionTwiceKeepsBothDeadEntries(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 1)
	for round := int64(0); round < 2; round++ {
		at := testNow + round*10*testLeaseMs
		s.claim(1, at)
		s.pushHeapTwin(0)
		s.returnDue(at + 2*testLeaseMs)
	}
	if st := s.state(); st.Dead != 2 || len(s.keysOfKind(kindDead)) != 2 || st.Entries != 1 {
		t.Fatalf("after two collisions: dead %d, 'x' keys %d, entries %d, want 2, 2, 1", st.Dead, len(s.keysOfKind(kindDead)), st.Entries)
	}
	s.invariants()
}

// TestAckAfterDeadlineAnswersExpired (CA-9): an ack after the deadline answers
// expired and acknowledges nothing, including for a lease the return step,
// bounded by a small budget, has not reached in that apply.
func TestAckAfterDeadlineAnswersExpired(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			s.claim(2, testNow)
			second, _ := s.claim(2, testNow+1)
			withDeliveryBudget(t, 1, 1)
			result, err := s.ack(s.leaseArgs(second.LeaseID, expired), nil)
			if err != nil || result.State != LeaseExpired || result.Acked != 0 {
				t.Fatalf("ack after the deadline = %+v, %v, want expired", result, err)
			}
			if result.Delivery.Returned != 2 {
				t.Fatalf("the bounded return step returned %d, want the first lease's 2", result.Delivery.Returned)
			}
			if got := len(s.claimedNumbers(second.LeaseID)); got != 2 {
				t.Fatalf("the second lease holds %d entries after the expired ack, want 2: the return step had not reached it", got)
			}
			s.invariants()
		})
	}
}

// TestAckOfALeaseTheReturnStepReturnedInThisApplyAnswersExpired (§4.3 view
// rule, R-1): an ack late by any margin, with the full budget, has its own
// lease returned by its own return step, and the header Get reads the
// snapshot, so it answers expired, not unknown.
func TestAckOfALeaseTheReturnStepReturnedInThisApplyAnswersExpired(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, _ := s.claim(2, testNow)
			result, err := s.ack(s.leaseArgs(claim.LeaseID, expired), nil)
			if err != nil || result.State != LeaseExpired || result.Delivery.Returned != 2 {
				t.Fatalf("a late ack = %+v, %v, want expired with its entries returned", result, err)
			}
			if st := s.state(); st.Leases != 0 || st.Entries != 3 {
				t.Fatalf("after the late ack: leases %d, entries %d, want 0, 3", st.Leases, st.Entries)
			}
			s.invariants()
		})
	}
}

// TestDoubleAckBeforeAndAfterTheOriginalDeadline: a second ack of an acked
// lease is unknown, before and after its deadline, and removes nothing.
func TestDoubleAckBeforeAndAfterTheOriginalDeadline(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, _ := s.claim(2, testNow)
			if result, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+1), nil); err != nil || result.State != LeaseAcked {
				t.Fatalf("the first ack = %+v, %v", result, err)
			}
			for _, at := range []int64{testNow + 2, expired} {
				result, err := s.ack(s.leaseArgs(claim.LeaseID, at), nil)
				if err != nil || result.State != LeaseUnknown || result.Acked != 0 {
					t.Fatalf("the second ack at %d = %+v, %v, want unknown", at, result, err)
				}
			}
			if st := s.state(); st.Entries != 1 || st.Pending != 0 {
				t.Fatalf("after the acks: entries %d, pending %d, want 1, 0", st.Entries, st.Pending)
			}
		})
	}
}

// TestUnknownLeaseAckStillReturnsExpiredLeases (CA-3, CA-9): the ack of an
// unknown lease is an in-band answer, so the apply commits the return step.
func TestUnknownLeaseAckStillReturnsExpiredLeases(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.claim(2, testNow)
			result := s.returnDue(expired)
			if result.Delivery.Returned != 2 {
				t.Fatalf("the unknown ack returned %d, want 2", result.Delivery.Returned)
			}
			if st := s.state(); st.Leases != 0 || st.Entries != 3 {
				t.Fatalf("after the unknown ack: leases %d, entries %d, want 0, 3", st.Leases, st.Entries)
			}
			s.invariants()
		})
	}
}

// TestReleaseReturnsNowAndCountsTowardK (CA-10): released entries are available
// at once and their next delivery counts the release.
func TestReleaseReturnsNowAndCountsTowardK(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, taken := s.claim(2, testNow)
			result, err := s.release(s.leaseArgs(claim.LeaseID, testNow+1))
			if err != nil || result.State != LeaseReleased || result.Released != 2 {
				t.Fatalf("release = %+v, %v, want released 2", result, err)
			}
			if st := s.state(); st.Leases != 0 || st.Entries != 3 || st.Pending != 0 {
				t.Fatalf("after the release: leases %d, entries %d, pending %d", st.Leases, st.Entries, st.Pending)
			}
			again, retaken := s.claim(2, testNow+2)
			if !slices.Equal(retaken, taken) || again.Items[0].Deliveries != 2 {
				t.Fatalf("the claim after the release took %v with %+v, want %v with deliveries 2", retaken, again.Items, taken)
			}
			if result, err := s.release(s.leaseArgs(claim.LeaseID, testNow+3)); err != nil || result.State != LeaseUnknown {
				t.Fatalf("a second release = %+v, %v, want unknown", result, err)
			}
			s.invariants()
		})
	}
}

// TestExtendOfAnExpiredLeaseAnswersExpired (CA-11): an expired lease is not
// revived; its entries are back in the queue.
func TestExtendOfAnExpiredLeaseAnswersExpired(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			claim, _ := s.claim(2, testNow)
			result, err := s.extend(s.leaseArgs(claim.LeaseID, expired), 60_000)
			if err != nil || result.State != LeaseExpired {
				t.Fatalf("extend after the deadline = %+v, %v, want expired", result, err)
			}
			if st := s.state(); st.Leases != 0 || st.Entries != 3 {
				t.Fatalf("after the extend: leases %d, entries %d, want 0, 3", st.Leases, st.Entries)
			}
			s.invariants()
		})
	}
}

// TestReturnStepNeverExceedsTheBatchBound (CA-4, Badger at the floor): two
// leases of 100 entries of 40 KiB each, claimed under a raised budget as a
// member before the floor might have, are each larger than the budget. Every
// verb commits; the leases drain one per apply, the sooner deadline first.
func TestReturnStepNeverExceedsTheBatchBound(t *testing.T) {
	for _, s := range badgerDeliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.pushPadded(0, 200, 40<<10)
			saved := deliveryBudgetBytes
			deliveryBudgetBytes = 9 << 20
			first, _ := s.claim(100, testNow)
			second, _ := s.claim(100, testNow+1)
			deliveryBudgetBytes = saved
			if len(first.Items) != 100 || len(second.Items) != 100 {
				t.Fatalf("the two leases hold %d and %d entries, want 100 each", len(first.Items), len(second.Items))
			}
			s.returnDue(expired)
			if st := s.state(); st.Leases != 1 || len(s.claimedNumbers(second.LeaseID)) != 100 {
				t.Fatalf("after the first return: leases %d, the second lease holds %d, want 1 and 100", st.Leases, len(s.claimedNumbers(second.LeaseID)))
			}
			s.returnDue(expired + 1)
			if st := s.state(); st.Leases != 0 || st.Entries != 200 {
				t.Fatalf("after the second return: leases %d, entries %d, want 0, 200", st.Leases, st.Entries)
			}
			s.invariants()
		})
	}
}

// TestClaimReservesItsReturn (§4.4 C-1, Badger at the floor): a claim charges
// each entry's return, so a lease filled to the budget returns in about half
// of it. A small lease and a full one expire together and one apply returns
// both: the small one is the exempt unit, and the full one fits what is left
// only because its claim reserved its return. Then the full one's entries go
// round to K, back to 'c' each time and to 'x' at the last, every apply
// committing.
func TestClaimReservesItsReturn(t *testing.T) {
	for _, s := range badgerDeliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.pushPadded(0, 120, 64<<10)
			small, _ := s.claim(10, testNow)
			full, _ := s.claim(100, testNow+1)
			if len(full.Items) >= 100-10 {
				t.Fatalf("the full claim took %d entries: the budget did not stop it", len(full.Items))
			}
			result := s.returnDue(expired)
			if want := len(small.Items) + len(full.Items); result.Delivery.Returned != want {
				t.Fatalf("one apply returned %d entries, want both leases' %d", result.Delivery.Returned, want)
			}
			if st := s.state(); st.Returned != int64(len(small.Items)+len(full.Items)) {
				t.Fatalf("after the return the bucket counts %d returned", st.Returned)
			}
			s.invariants()

			// Round to K with the full lease's entries alone.
			k := newDeliverySubject(t, s.kind, openFloorBadger(t))
			k.pushPadded(0, len(full.Items), 64<<10)
			for cycle := 1; cycle <= MaxDeliveries; cycle++ {
				claim, back := k.claimReturnCycle(len(full.Items), testNow+int64(cycle)*10*testLeaseMs)
				if len(claim.Items) != len(full.Items) {
					t.Fatalf("cycle %d claimed %d, want %d", cycle, len(claim.Items), len(full.Items))
				}
				if cycle < MaxDeliveries && back.Delivery.Returned != len(full.Items) {
					t.Fatalf("cycle %d returned %+v to 'c'", cycle, back.Delivery)
				}
				if cycle == MaxDeliveries && back.Delivery.DeadLettered != len(full.Items) {
					t.Fatalf("cycle %d dead-lettered %+v, want all %d", cycle, back.Delivery, len(full.Items))
				}
			}
			k.invariants()
		})
	}
}

// TestFirstUnitExemptionsDoNotStack (§4.4 Edge 1): when the return step
// returns a large lease it is the apply's exempt unit, and a claim's own
// large first entry must fit what is left: it stays, and the claim answers no
// items with Returned above zero.
func TestFirstUnitExemptionsDoNotStack(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String(), func(t *testing.T) {
			s := newDeliverySubject(t, kind, bigStore())
			s.pushPadded(0, 2, 1_500_000)
			s.claim(1, testNow)
			claim, err := s.claimN(ApplyInfo{}, s.claimArgs(1, expired), func([]byte) error { return nil })
			if err != nil || len(claim.Items) != 0 || claim.Refused != nil || claim.Delivery.Returned != 1 {
				t.Fatalf("the claim after the large return = %d items, refused %+v, %+v, %v, want none taken and 1 returned", len(claim.Items), claim.Refused, claim.Delivery, err)
			}
			if st := s.state(); st.Entries != 2 || st.Leases != 0 {
				t.Fatalf("after the claim: entries %d, leases %d, want 2, 0", st.Entries, st.Leases)
			}
			s.invariants()
		})
	}
}

// TestHardCapRefusalCommitsTheReturnStep (Q-2, A-12): a heap whose front
// element is over the hard cap and which holds an expired lease: the claim is
// refused by name and the expired lease is returned in the same apply.
func TestHardCapRefusalCommitsTheReturnStep(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, bigStore())
	s.push(10, 11)
	s.claim(1, testNow)
	if _, err := s.heap.PushItem(ApplyInfo{}, 5000, numbered(t, 1, 5_000_000)); err != nil {
		t.Fatalf("PushItem: %v", err)
	}
	claim, err := s.claimN(ApplyInfo{}, s.claimArgs(1, expired), func([]byte) error { return nil })
	if err != nil || claim.Refused == nil || claim.Delivery.Returned != 1 {
		t.Fatalf("the claim = refused %+v, %+v, %v, want a refusal by name and the lease returned", claim.Refused, claim.Delivery, err)
	}
	if st := s.state(); st.Leases != 0 || st.Entries != 2 {
		t.Fatalf("after the refusal: leases %d, entries %d, want 0, 2", st.Leases, st.Entries)
	}
	s.invariants()
}

// TestClaimEntryHeadroomHoldsAtTheFloor (§4.4 Edge 2, build doc step 7 item
// 11): a heap element with an 8 KiB id whose delivery footprint is exactly
// maxClaimEntryBytes goes round to its K-th return on Badger at the floor. Every
// apply commits, and the most any one of them adds beyond maxClaimEntryBytes,
// with Badger's per-key charge, is the headroom a claimed entry needs; it must
// be at most 32 KiB (stop-and-ask 5).
func TestClaimEntryHeadroomHoldsAtTheFloor(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, openFloorBadger(t))
	id := strings.Repeat("i", maxClaimIDBytes)
	suffixLen := 8 + len(id)
	valueFor := func(pad int) []byte {
		encoded, err := bson.Marshal(map[string]interface{}{"_id_": id, "n": 0, "pad": strings.Repeat("v", pad)})
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}
		return encoded
	}
	target := int(maxClaimEntryBytes) - 4*suffixLen - claimFixedBytes
	pad := target - (len(valueFor(0)) - 0)
	if got := claimFootprint(claimLayoutHeap, len(valueFor(pad)), suffixLen); got != maxClaimEntryBytes {
		t.Fatalf("the element's footprint is %d, want exactly %d", got, maxClaimEntryBytes)
	}
	if _, err := s.heap.PushItem(ApplyInfo{}, 1, map[string]interface{}{"_id_": id, "n": 0, "pad": strings.Repeat("v", pad)}); err != nil {
		t.Fatalf("pushing the element: %v", err)
	}
	worst := int64(math.MinInt64)
	measure := func(what string, apply func()) {
		s.kv.batchBytes.Store(0)
		apply()
		// The state document is written into every batch too.
		snap := s.kv.NewSnapshot()
		state, err := snap.Get(storage.Key(s.slot, 1, s.id, storage.NSState))
		snap.Discard()
		if err != nil {
			t.Fatalf("reading the state: %v", err)
		}
		written := s.kv.batchBytes.Load() + int64(len(state))
		if beyond := written - maxClaimEntryBytes; beyond > worst {
			worst = beyond
		}
		t.Logf("%s wrote %d bytes, %d beyond the cap", what, written, written-maxClaimEntryBytes)
	}
	for cycle := 1; cycle <= MaxDeliveries; cycle++ {
		at := testNow + int64(cycle)*10*testLeaseMs
		measure(fmt.Sprintf("claim %d", cycle), func() {
			if claim, _ := s.claim(1, at); len(claim.Items) != 1 {
				t.Fatalf("claim %d took nothing: %+v", cycle, claim.Refused)
			}
		})
		measure(fmt.Sprintf("return %d", cycle), func() { s.returnDue(at + 2*testLeaseMs) })
	}
	if st := s.state(); st.Dead != 1 {
		t.Fatalf("after %d returns the element is not dead: dead %d", MaxDeliveries, st.Dead)
	}
	t.Logf("the worst apply wrote %d bytes beyond maxClaimEntryBytes (negative: under it); claimEntryHeadroom is %d", worst, claimEntryHeadroom)
	if worst > 32<<10 {
		t.Fatalf("an apply of a claimed element at the cap wrote %d bytes beyond it, more than 32 KiB (stop-and-ask 5)", worst)
	}
}

// TestReleaseOverTheBudgetIsTheExemptUnit (§4.4): a release whose own lease
// does not fit the whole budget is the apply's exempt unit, and the return step
// returns nothing in that apply; an expired lease waits for the next verb. The
// return step a release runs has no exempt unit of its own (owner ruling
// 2026-10-18), which TestReleaseNeverExceedsTheBatchBound pins for the case
// where the release's lease does fit.
func TestReleaseOverTheBudgetIsTheExemptUnit(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 4)
			// The first lease ends between the second claim and the release.
			s.claim(1, testNow)
			own, _ := s.claim(2, testNow+testLeaseMs-1)
			withDeliveryBudget(t, 64, 64)
			result, err := s.release(s.leaseArgs(own.LeaseID, testNow+testLeaseMs+1))
			if err != nil || result.State != LeaseReleased || result.Released != 2 || result.Delivery.Returned != 0 {
				t.Fatalf("release over the budget = %+v, %v, want released 2 and nothing returned", result, err)
			}
			if st := s.state(); st.Leases != 1 || st.Pending != 1 {
				t.Fatalf("after the release: leases %d, pending %d, want the expired lease still out", st.Leases, st.Pending)
			}
			s.invariants()
		})
	}
}

// TestReleaseNeverExceedsTheBatchBound (owner ruling 2026-10-18, claim-ack-spec
// §4.4 "Release asks the return step to leave room"): the return step a release
// runs has no exempt unit, so an expired lease whose entry is larger than what
// the release leaves free stays out, and the release always fits the floor
// batch. With the exemption the large entry was returned in the same apply as a
// near-budget lease, and the release was refused as too large on every retry.
func TestReleaseNeverExceedsTheBatchBound(t *testing.T) {
	for _, s := range badgerDeliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			const largeEntryPad = 4 << 20
			// Pop order puts the large entry first on every engine: fifo and a
			// heap by number, a lifo by being pushed last.
			if s.kind == invariantLIFO {
				s.pushPadded(1, 60, 40<<10)
				s.pushPadded(0, 1, largeEntryPad)
			} else {
				s.pushPadded(0, 1, largeEntryPad)
				s.pushPadded(1, 60, 40<<10)
			}
			large, _ := s.claim(1, testNow)
			if len(large.Items) != 1 {
				t.Fatalf("the large claim took %d entries, want 1", len(large.Items))
			}
			// The second lease is filled to the budget and outlives the first.
			own, _ := s.claim(100, testNow+testLeaseMs-1)
			if len(own.Items) < 30 {
				t.Fatalf("the near-budget claim took %d entries, want it filled to the budget", len(own.Items))
			}
			before := s.kv.batchBytes.Load()
			result, err := s.release(s.leaseArgs(own.LeaseID, testNow+testLeaseMs+1))
			wrote, bound := s.kv.batchBytes.Load()-before, int64(config.MemtableBatchBytes(config.MinMemtableMB))
			if wrote > bound {
				t.Errorf("the release wrote %d bytes, %d over the floor memtable's batch bound", wrote, wrote-bound)
			}
			if err != nil {
				t.Fatalf("release of a near-budget lease beside an expired large one: %v", err)
			}
			if result.State != LeaseReleased || result.Released != len(own.Items) || result.Delivery.Returned != 0 {
				t.Fatalf("release = %+v, want released %d and the large entry still out", result, len(own.Items))
			}
			if st := s.state(); st.Leases != 1 || st.Pending != 1 {
				t.Fatalf("after the release: leases %d, pending %d, want the large lease still out", st.Leases, st.Pending)
			}
			s.invariants()
		})
	}
}

// TestDeleteOfAReturnedEntryDropsItsCount (claim/ack build stop-and-ask 7): a
// remover of a 0x04 key deletes the returned entry's 'c' record with it. The
// seeded model deletes on queues only, so the heap's drop in
// HeapStore.deleteWhere is pinned here.
func TestDeleteOfAReturnedEntryDropsItsCount(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.claim(1, testNow)
			s.returnDue(expired)
			if got := len(s.keysOfKind(kindReturnCount)); got != 1 {
				t.Fatalf("after the return there are %d 'c' records, want 1", got)
			}
			// The returned entry is the first in pop order on fifo and a heap;
			// a lifo pops the newest, so its returned entry sits at the tail.
			position := 0
			if s.kind == invariantLIFO {
				position = 2
			}
			var err error
			if s.heap != nil {
				err = s.heap.DeleteAtPosition(position)
			} else {
				err = s.queue.DeleteAtPosition(position)
			}
			if err != nil {
				t.Fatalf("deleting the returned entry at %d: %v", position, err)
			}
			if got := len(s.keysOfKind(kindReturnCount)); got != 0 || s.state().Returned != 0 {
				t.Fatalf("after the delete: %d 'c' records, Returned %d, want none", got, s.state().Returned)
			}
			s.invariants()
		})
	}
}

// TestAckOfALeaseTheForwardJumpGuardReturnedAnswersExpired (claim-ack-spec
// §4.5, §4.11): a lease granted while the owner's clock was a day ahead has a
// deadline further than MaxLeaseMs from a corrected now. The return step's
// guard returns it in the ack's own apply, and the ack must answer expired. If
// namedLease treated it as live, the ack would count its entries a second time
// (acked 2 after the 2 returned).
func TestAckOfALeaseTheForwardJumpGuardReturnedAnswersExpired(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 2)
			const day = 24 * 60 * 60 * 1000
			claim, _ := s.claim(2, testNow+day)
			if len(claim.Items) != 2 {
				t.Fatalf("the claim took %d entries, want 2", len(claim.Items))
			}
			result, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+10_000), nil)
			if err != nil {
				t.Fatalf("ack after the clock was corrected: %v", err)
			}
			if result.State != LeaseExpired || result.Acked != 0 || result.Delivery.Returned != 2 {
				t.Fatalf("ack = %+v, want expired, nothing acked, the 2 entries returned", result)
			}
			if st := s.state(); st.Leases != 0 || st.Entries != 2 || st.Pending != 0 {
				t.Fatalf("after the ack: leases %d, entries %d, pending %d, want 0, 2, 0", st.Leases, st.Entries, st.Pending)
			}
			s.invariants()
		})
	}
}
