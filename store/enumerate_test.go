package store

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// countKVKeys counts the live keys of a shared store in [lower, upper).
func countKVKeys(t *testing.T, kv storage.KV, lower, upper []byte) int {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	n := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		n++
	}
	return n
}

// TestDumpEmitsNoVersionKeys asserts the store-layer half of
// dump-restore-spec.md §7.1: the "v:" read-repair version side-keys
// (store/version.go) live outside a store's own document prefix, so
// IterateDocuments must never see them, even when they are known to exist.
func TestDumpEmitsNoVersionKeys(t *testing.T) {
	t.Run("hash", func(t *testing.T) {
		kv := kvfault.New()
		h := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("k%d", i)
			if err := h.Put(ApplyInfo{}, id, map[string]interface{}{"n": i}, uint64(i+1)); err != nil {
				t.Fatalf("Put(%s): %v", id, err)
			}
		}
		// Prove the version keys actually exist, so excluding them is not
		// vacuously true.
		if got := countKVKeys(t, kv, storage.Key(1, 1, 1, storage.NSVersions), storage.Key(1, 1, 1, storage.NSVersions+1)); got != 3 {
			t.Fatalf("test setup: %d version keys in the store, want 3", got)
		}

		var emitted int
		if err := h.IterateDocuments(func(key, value []byte) error {
			emitted++
			return nil
		}); err != nil {
			t.Fatalf("IterateDocuments: %v", err)
		}
		if emitted != 3 {
			t.Fatalf("IterateDocuments emitted %d documents, want 3 (version keys counted as documents)", emitted)
		}
		if count, err := h.DocCount(); err != nil || count != 3 {
			t.Fatalf("DocCount = %d, %v, want 3, nil", count, err)
		}
	})

	t.Run("btree", func(t *testing.T) {
		kv := kvfault.New()
		bt := NewBTreeStore(kv, 1, 1, 1, new(OwedApplies))
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("k%d", i)
			if err := bt.Put(ApplyInfo{}, id, map[string]interface{}{"n": i}, uint64(i+1)); err != nil {
				t.Fatalf("Put(%s): %v", id, err)
			}
		}
		if got := countKVKeys(t, kv, storage.Key(1, 1, 1, storage.NSVersions), storage.Key(1, 1, 1, storage.NSVersions+1)); got != 3 {
			t.Fatalf("test setup: %d version keys in the store, want 3", got)
		}

		var emitted int
		if err := bt.IterateDocuments(func(key, value []byte) error {
			emitted++
			return nil
		}); err != nil {
			t.Fatalf("IterateDocuments: %v", err)
		}
		if emitted != 3 {
			t.Fatalf("IterateDocuments emitted %d documents, want 3 (version keys counted as documents)", emitted)
		}
		if count, err := bt.DocCount(); err != nil || count != 3 {
			t.Fatalf("DocCount = %d, %v, want 3, nil", count, err)
		}
	})
}

// TestIterateDocumentsAgreesWithDocCount is the per-engine round-trip check
// the build's "Done when" list asks for: on a populated bucket,
// IterateDocuments and DocCount must count the same documents, for every
// Badger-backed engine (hash, btree, heap). The segment-backed queues and
// blob get their own versions in the storage package, since they are not
// store.Enumerator implementations that live in this package.
func TestIterateDocumentsAgreesWithDocCount(t *testing.T) {
	t.Run("hash", func(t *testing.T) {
		h := NewHashStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
		const n = 25
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("k%02d", i)
			if err := h.Put(ApplyInfo{}, id, map[string]interface{}{"n": i}, 0); err != nil {
				t.Fatalf("Put(%s): %v", id, err)
			}
		}

		count, err := h.DocCount()
		if err != nil {
			t.Fatalf("DocCount: %v", err)
		}
		if count != n {
			t.Fatalf("DocCount = %d, want %d", count, n)
		}

		seen := map[string]bool{}
		if err := h.IterateDocuments(func(key, value []byte) error {
			seen[string(key)] = true
			return nil
		}); err != nil {
			t.Fatalf("IterateDocuments: %v", err)
		}
		if int64(len(seen)) != count {
			t.Fatalf("IterateDocuments emitted %d keys, DocCount says %d", len(seen), count)
		}
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("k%02d", i)
			if !seen[id] {
				t.Fatalf("IterateDocuments never emitted key %q", id)
			}
		}
	})

	t.Run("btree", func(t *testing.T) {
		bt := NewBTreeStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
		const n = 25
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("k%02d", i)
			if err := bt.Put(ApplyInfo{}, id, map[string]interface{}{"n": i}, 0); err != nil {
				t.Fatalf("Put(%s): %v", id, err)
			}
		}

		count, err := bt.DocCount()
		if err != nil {
			t.Fatalf("DocCount: %v", err)
		}
		if count != n {
			t.Fatalf("DocCount = %d, want %d", count, n)
		}

		seen := map[string]bool{}
		if err := bt.IterateDocuments(func(key, value []byte) error {
			// The emitted key is a string key's display form: the id itself.
			seen[string(key)] = true
			return nil
		}); err != nil {
			t.Fatalf("IterateDocuments: %v", err)
		}
		if int64(len(seen)) != count {
			t.Fatalf("IterateDocuments emitted %d keys, DocCount says %d", len(seen), count)
		}
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("k%02d", i)
			if !seen[id] {
				t.Fatalf("IterateDocuments never emitted key %q", id)
			}
		}
	})

	t.Run("heap", func(t *testing.T) {
		h := NewHeapStore(kvfault.New(), 1, 1, 1, new(OwedApplies))
		const n = 10
		wantPriority := map[string]int64{}
		for i := 0; i < n; i++ {
			priority := int64(i * 3)
			id, err := h.PushItem(ApplyInfo{}, priority, map[string]interface{}{"n": i})
			if err != nil {
				t.Fatalf("PushItem %d: %v", i, err)
			}
			wantPriority[id] = priority
		}

		count, err := h.DocCount()
		if err != nil {
			t.Fatalf("DocCount: %v", err)
		}
		if count != n {
			t.Fatalf("DocCount = %d, want %d", count, n)
		}

		type dumpedEntry struct {
			ID       string   `bson:"_id_"`
			Priority int64    `bson:"priority"`
			Value    bson.Raw `bson:"value"`
		}
		seen := map[string]int64{}
		if err := h.IterateDump(func(key, value []byte) error {
			var entry dumpedEntry
			if err := bson.Unmarshal(value, &entry); err != nil {
				return fmt.Errorf("unmarshaling dumped heap entry: %w", err)
			}
			if string(key) != entry.ID {
				t.Fatalf("key %q != emitted document _id_ %q", key, entry.ID)
			}
			seen[entry.ID] = entry.Priority
			return nil
		}, ignoreDead); err != nil {
			t.Fatalf("IterateDump: %v", err)
		}
		if int64(len(seen)) != count {
			t.Fatalf("IterateDocuments emitted %d entries, DocCount says %d", len(seen), count)
		}
		for id, wantP := range wantPriority {
			gotP, ok := seen[id]
			if !ok {
				t.Fatalf("IterateDocuments never emitted heap entry %q", id)
			}
			if gotP != wantP {
				t.Fatalf("heap entry %q priority = %d, want %d", id, gotP, wantP)
			}
		}
	})
}

// The dump walk and claimed entries (claim/ack build step 9).

// ignoreDead is the dead callback of a walk that counts documents only.
func ignoreDead(DeadRecord) error { return nil }

// dumpNumbers are the entry numbers IterateDump emits as documents, in order. A heap's
// documents are {_id_, priority, value}, so its number is read from the value.
func (s *deliverySubject) dumpNumbers() []int {
	s.t.Helper()
	var numbers []int
	err := s.enumerator().IterateDump(func(_, value []byte) error {
		if s.heap != nil {
			stored, err := heapStoredValue(value)
			if err != nil {
				return err
			}
			value = stored
		}
		numbers = append(numbers, s.number(value))
		return nil
	}, ignoreDead)
	if err != nil {
		s.t.Fatalf("%s IterateDump: %v", s.name, err)
	}
	return numbers
}

func (s *deliverySubject) enumerator() DumpEnumerator {
	if s.heap != nil {
		return s.heap
	}
	return s.queue
}

// TestIterateDocumentsEmitsPendingInPopOrder (§7.1): claimed entries are
// emitted with the available ones, in pop order, without the return-count
// prefix; a dump that read 0x04 only would drop them while its count matched.
func TestIterateDocumentsEmitsPendingInPopOrder(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 6)
			// Returned once, so its 'e' value carries r = 1 to strip.
			s.claimReturnCycle(2, testNow)
			claim, _ := s.claim(2, testNow+10*testLeaseMs)
			if len(claim.Items) != 2 || s.state().Pending != 2 {
				t.Fatalf("setup: claimed %d, pending %d, want 2 and 2", len(claim.Items), s.state().Pending)
			}
			if got, want := s.dumpNumbers(), firstInPopOrder(s.kind, 6, 6); !slices.Equal(got, want) {
				t.Fatalf("the dump walk emitted %v, want %v", got, want)
			}
			// Claimed entries in the middle of the order, too: claim the third.
			s.claim(1, testNow+10*testLeaseMs+1)
			if got, want := len(s.dumpNumbers()), 6; got != want {
				t.Fatalf("with three claimed the dump walk emitted %d entries, want %d", got, want)
			}
		})
	}
}

// TestIterateDumpSendsAHeapTwinToTheDeadList (A-8): a claimed element and a live
// one with the same id and priority: the live one is the document, the claimed
// one a collided dead record, and restore-by-push of the documents keeps both.
func TestIterateDumpSendsAHeapTwinToTheDeadList(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 3)
	s.claim(1, testNow)
	s.pushHeapTwin(0)
	before := time.Now().UnixMilli()
	var documents []int
	var dead []DeadRecord
	err := s.heap.IterateDump(func(_, value []byte) error {
		stored, err := heapStoredValue(value)
		if err != nil {
			return err
		}
		documents = append(documents, s.number(stored))
		return nil
	}, func(record DeadRecord) error {
		dead = append(dead, record)
		return nil
	})
	if err != nil {
		t.Fatalf("IterateDump: %v", err)
	}
	if !slices.Equal(documents, []int{0, 1, 2}) {
		t.Fatalf("the documents are %v, want the twin and the two others once each", documents)
	}
	if len(dead) != 1 || dead[0].Reason != DeadReasonCollided || dead[0].Deliveries != 1 || s.deadNumber(dead[0]) != 0 || dead[0].DeadAt < before {
		t.Fatalf("the dead records are %+v, want the claimed copy of entry 0, collided after 1 delivery, dated now", dead)
	}
}

// seekRecorder remembers the lower bound of every iterator opened, to show which
// key kinds a walk looked at.
type seekRecorder struct {
	storage.KV
	lowers *[][]byte
}

func (r seekRecorder) NewSnapshot() storage.KVSnapshot {
	return recordingSnapshot{KVSnapshot: r.KV.NewSnapshot(), lowers: r.lowers}
}

type recordingSnapshot struct {
	storage.KVSnapshot
	lowers *[][]byte
}

func (r recordingSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	*r.lowers = append(*r.lowers, opts.Lower)
	return r.KVSnapshot.NewIterator(opts)
}

// TestIterateDocumentsReadsNoDeliveryKeyWhenNothingIsPending (rule 11): with no
// claimed entry the walk opens no iterator on the 'e' range; with one it opens
// exactly one.
func TestIterateDocumentsReadsNoDeliveryKeyWhenNothingIsPending(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String(), func(t *testing.T) {
			var lowers [][]byte
			s := newDeliverySubject(t, kind, seekRecorder{KV: kvfault.New(), lowers: &lowers})
			s.push(0, 3)
			claim, _ := s.claim(1, testNow)
			if _, err := s.ack(s.leaseArgs(claim.LeaseID, testNow+1), nil); err != nil {
				t.Fatalf("ack: %v", err)
			}
			claimedRange := func() int {
				n := 0
				for _, lower := range lowers {
					if bytes.HasPrefix(lower, kindKey(s.slot, 1, s.id, kindClaimed)) {
						n++
					}
				}
				return n
			}
			lowers = nil
			s.dumpNumbers()
			if got := claimedRange(); got != 0 {
				t.Fatalf("a bucket with nothing pending opened %d iterators on 'e'", got)
			}
			s.claim(1, testNow+2)
			lowers = nil
			s.dumpNumbers()
			if got := claimedRange(); got != 1 {
				t.Fatalf("a bucket with an entry pending opened %d iterators on 'e', want 1", got)
			}
		})
	}
}

// TestDocCountCountsWhatIterateDocumentsEmits: on a mixed bucket, available and
// claimed, and for a heap with a twin that goes to the dead list instead.
func TestDocCountCountsWhatIterateDocumentsEmits(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 5)
			s.claim(2, testNow)
			want := int64(5)
			if s.heap != nil {
				s.pushHeapTwin(0)
				want = 5
			}
			count, err := s.enumerator().DocCount()
			if err != nil || count != want || int64(len(s.dumpNumbers())) != count {
				t.Fatalf("DocCount = %d, %v with %d documents emitted, want %d", count, err, len(s.dumpNumbers()), want)
			}
		})
	}
}

// dumpDeadRecords are the dead records IterateDump hands its dead callback, in
// the order it hands them.
func (s *deliverySubject) dumpDeadRecords() []DeadRecord {
	s.t.Helper()
	var records []DeadRecord
	err := s.enumerator().IterateDump(func(_, _ []byte) error { return nil }, func(record DeadRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		s.t.Fatalf("%s IterateDump: %v", s.name, err)
	}
	return records
}

// TestIterateDumpListsDeadEntriesInKeyOrder (§7.2): every 'x' record reaches
// the dead callback in key order with its deliveries, date and reason, and
// none is a document. A dump that walked 0x04 and 'e' only would lose them
// under a matching document count.
func TestIterateDumpListsDeadEntriesInKeyOrder(t *testing.T) {
	for _, s := range deliverySubjects(t) {
		t.Run(s.name, func(t *testing.T) {
			s.t = t
			s.push(0, 3)
			s.restore(12, 0)
			s.restore(10, 0)
			s.restore(11, 0)
			listed, _ := s.deadPage(100, nil)
			dumped := s.dumpDeadRecords()
			if len(listed) != 3 || len(dumped) != 3 {
				t.Fatalf("the dead list holds %d and the dump sent %d, want 3 and 3", len(listed), len(dumped))
			}
			for i := range listed {
				if !bytes.Equal(dumped[i].DeadID, listed[i].DeadID) || !bytes.Equal(dumped[i].Data, listed[i].Data) ||
					dumped[i].Deliveries != listed[i].Deliveries || dumped[i].DeadAt != listed[i].DeadAt || dumped[i].Reason != listed[i].Reason {
					t.Fatalf("dead record %d: the dump sent %+v, the dead list says %+v", i, dumped[i], listed[i])
				}
			}
			if got := len(s.dumpNumbers()); got != 3 {
				t.Fatalf("the dump emitted %d documents with dead entries present, want 3: a dead entry is not a document", got)
			}
		})
	}
}

// TestIterateDumpReadsNoDeadKeyWhenNothingIsDead (rule 11, §7.4): a bucket with
// no dead entry opens no iterator on the 'x' range; one with a dead entry opens
// one.
func TestIterateDumpReadsNoDeadKeyWhenNothingIsDead(t *testing.T) {
	for _, kind := range []invariantKind{invariantFIFO, invariantLIFO, invariantHeap} {
		t.Run(kind.String(), func(t *testing.T) {
			var lowers [][]byte
			s := newDeliverySubject(t, kind, seekRecorder{KV: kvfault.New(), lowers: &lowers})
			s.push(0, 3)
			deadRange := func() int {
				n := 0
				for _, lower := range lowers {
					if bytes.HasPrefix(lower, kindKey(s.slot, 1, s.id, kindDead)) {
						n++
					}
				}
				return n
			}
			lowers = nil
			s.dumpNumbers()
			if got := deadRange(); got != 0 {
				t.Fatalf("a bucket with no dead entry opened %d iterators on 'x'", got)
			}
			s.restore(10, 0)
			lowers = nil
			s.dumpNumbers()
			if got := deadRange(); got != 1 {
				t.Fatalf("a bucket with a dead entry opened %d iterators on 'x', want 1", got)
			}
		})
	}
}

// TestIterateDumpSendsATwinAndTheDeadListToOneCallback: a heap with a collided
// twin and a restored dead entry sends both, the twin first.
func TestIterateDumpSendsATwinAndTheDeadListToOneCallback(t *testing.T) {
	s := newDeliverySubject(t, invariantHeap, kvfault.New())
	s.push(0, 3)
	s.claim(1, testNow)
	s.pushHeapTwin(0)
	s.restore(10, 0)
	dead := s.dumpDeadRecords()
	if len(dead) != 2 || dead[0].Reason != DeadReasonCollided || dead[1].Reason != DeadReasonMaxDeliveries || s.deadNumber(dead[1]) != 10 {
		t.Fatalf("the dead records are %+v, want the collided twin of entry 0 and then restored entry 10", dead)
	}
}

type fixedDocuments struct{ documents [][]byte }

func (f fixedDocuments) IterateDocuments(fn func(key, value []byte) error) error {
	for _, document := range f.documents {
		if err := fn([]byte("k"), document); err != nil {
			return err
		}
	}
	return nil
}

func (f fixedDocuments) DocCount() (int64, error) { return int64(len(f.documents)), nil }

// TestDocumentsOnlyWalksDocumentsAndNeverSendsDead: the wrapper that gives a
// hash, btree or blob engine the dump walk's shape sends its documents and
// calls no dead callback.
func TestDocumentsOnlyWalksDocumentsAndNeverSendsDead(t *testing.T) {
	walk := DocumentsOnly(fixedDocuments{documents: [][]byte{{1}, {2}}})
	var seen int
	err := walk.IterateDump(func(key, _ []byte) error {
		seen++
		if string(key) != "k" {
			t.Errorf("key %q, want k", key)
		}
		return nil
	}, func(DeadRecord) error {
		t.Error("a documents-only walk called dead")
		return nil
	})
	if count, countErr := walk.DocCount(); err != nil || seen != 2 || countErr != nil || count != 2 {
		t.Fatalf("walk = %v, %d documents, DocCount %d, %v; want 2 documents and DocCount 2", err, seen, count, countErr)
	}
}
