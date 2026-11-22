package main

// The four engines queueforest measures the same workload against. files is
// the layout under test: one directory per item, its fifo and lifo segment
// stores inside it, driven through legacyqueue.NewSegmentStore -- the same call
// server/service.go makes, per the plan's "harness drives the storage layer
// directly" (the nested-store model is not built, so no client can declare this
// layout). badger, mongo and mysql hold the same per-item queues as key
// ranges in one shared instance instead of a directory each, per
// the shared-storage-instance-spec record §2 and §3.4 -- the badger arm
// implements §3.4's exact seek-to-cursor key design (the plan's Q2
// disposition: a simplified prefix scan would penalise Badger for a
// tombstone-wall cost its real design avoids).
//
// Every target implements queueForestTarget. Push/Peek/Pop/DeleteAtPosition
// return a queueOpResult rather than erroring the run: R5-02 in the rev-5
// review found the plan's own uniform selection model produces
// "position out of range" deletes on queues a prior op already emptied, and
// the harness's job is to count that, not abort on it.

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hoarddb/hoarddb-server/bench/bucketdensity/legacyqueue"
)

// queueOpResult is what one operation costs and whether it succeeded. A
// failure is data (a failed-delete count, an empty-queue peek), not a reason
// to stop the run.
type queueOpResult struct {
	Duration time.Duration
	Err      error
}

// censusResult is the F-04 stand-in: a timed loop of the same open call
// LoadCatalog pays per bucket today, reported separately from directory
// enumeration (which measures a different bottleneck and must not be
// presented as a census -- see runCensusAndWalk in queueforest.go).
type censusResult struct {
	Mechanism     string  `json:"mechanism"`
	Applicable    bool    `json:"applicable"`
	OpensPerItem  int     `json:"opens_per_item"`
	TotalOpens    int     `json:"total_opens"`
	TotalMillis   float64 `json:"total_ms"`
	MillisPerItem float64 `json:"ms_per_item"`
}

// queueForestTarget is one engine under test. Prepare runs once, before the
// workload; Close runs once, after it (and before Census, so a held/lru
// files target's Census measures a real reopen rather than a no-op on an
// already-open store).
type queueForestTarget interface {
	Name() string
	Prepare(items []forestItem) error
	Push(ref queueRef, entry []byte) queueOpResult
	Peek(ref queueRef) queueOpResult
	Pop(ref queueRef) queueOpResult
	DeleteAtPosition(ref queueRef, pos int) queueOpResult
	Census(items []forestItem) (censusResult, error)
	Close() error
}

func queueID(ref queueRef) int {
	return ref.ItemIndex*2 + int(ref.Kind)
}

// --- files: the layout under test -------------------------------------

// queueLockStripes bounds a striped mutex, one per queue under
// -open=ondemand, so a concurrent reader and writer that land on the same
// queue at the same instant cannot open two independent SegmentStore
// instances on the same directory at once. The real product serializes this
// with SegmentStore's own internal ss.mu, held for the store's whole life;
// "ondemand" reopens per operation and has no such instance to hold a lock
// on between calls, so the harness recreates the guarantee at the directory
// boundary instead. It does not change what is measured: it prevents a rare
// race (recover() reading a state.bson mid-rename) that would otherwise
// occasionally corrupt a queue's cursors rather than just cost time.
const queueLockStripes = 4096

type lruEntry struct {
	id    int
	store *legacyqueue.SegmentStore
}

// filesTarget is one directory per item with the item's fifo and lifo
// segment stores inside it. -open decides whether a queue's SegmentStore is
// reopened every operation ("ondemand", the arm criterion 1 is judged on),
// held for the whole run ("held"), or kept in a bounded LRU ("lru").
type filesTarget struct {
	items       []forestItem
	segmentSize int64
	openMode    string // "ondemand" | "held" | "lru"
	lruCapacity int

	queueLocks [queueLockStripes]sync.Mutex

	mu       sync.Mutex
	resident map[int]*legacyqueue.SegmentStore // held mode: every queue, filled once in Prepare
	lru      *list.List
	lruIndex map[int]*list.Element
}

func newFilesTarget(segmentSize int64, openMode string, lruCapacity int) *filesTarget {
	t := &filesTarget{
		segmentSize: segmentSize,
		openMode:    openMode,
		lruCapacity: lruCapacity,
		resident:    make(map[int]*legacyqueue.SegmentStore),
	}
	if openMode == "lru" {
		t.lru = list.New()
		t.lruIndex = make(map[int]*list.Element)
	}
	return t
}

func (t *filesTarget) Name() string { return "files/" + t.openMode }

func (t *filesTarget) queueDir(ref queueRef) string {
	item := t.items[ref.ItemIndex]
	if ref.Kind == queueA {
		return filepath.Join(item.Dir, "A")
	}
	return filepath.Join(item.Dir, "B")
}

func (t *filesTarget) openQueue(ref queueRef) (*legacyqueue.SegmentStore, error) {
	return legacyqueue.NewSegmentStore(t.queueDir(ref), t.segmentSize, ref.Kind.storeType())
}

func (t *filesTarget) Prepare(items []forestItem) error {
	t.items = items
	for _, item := range items {
		if err := os.MkdirAll(item.Dir, 0o755); err != nil {
			return fmt.Errorf("creating item directory %q: %w", item.Dir, err)
		}
		if err := os.WriteFile(filepath.Join(item.Dir, "Name.BSON"), item.NameDoc, 0o644); err != nil {
			return fmt.Errorf("writing Name.BSON for item %q: %w", item.ID, err)
		}
	}
	if t.openMode != "held" {
		return nil
	}
	for _, item := range items {
		for _, kind := range []queueKind{queueA, queueB} {
			ref := queueRef{ItemIndex: item.Index, Kind: kind}
			store, err := t.openQueue(ref)
			if err != nil {
				return fmt.Errorf("holding open queue %d at prepare: %w", queueID(ref), err)
			}
			t.resident[queueID(ref)] = store
		}
	}
	return nil
}

// withStore resolves a *legacyqueue.SegmentStore for held or lru mode. ondemand
// does not go through this path -- see ondemandOp.
func (t *filesTarget) withStore(ref queueRef) (*legacyqueue.SegmentStore, error) {
	id := queueID(ref)
	if t.openMode == "held" {
		store, ok := t.resident[id]
		if !ok {
			return nil, fmt.Errorf("held queue %d was not opened in Prepare", id)
		}
		return store, nil
	}
	return t.withLRUStore(id, ref)
}

// withLRUStore holds one mutex across lookup, eviction and open so two
// goroutines racing on the same miss cannot both decide to evict the same
// tail entry. -reader-concurrency defaults to 1, so this is a two-goroutine
// critical section in practice, not a bottleneck against the fsync costs
// every open and close already pays.
func (t *filesTarget) withLRUStore(id int, ref queueRef) (*legacyqueue.SegmentStore, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if elem, ok := t.lruIndex[id]; ok {
		t.lru.MoveToFront(elem)
		return elem.Value.(*lruEntry).store, nil
	}
	if t.lru.Len() >= t.lruCapacity {
		tail := t.lru.Back()
		evicted := tail.Value.(*lruEntry)
		t.lru.Remove(tail)
		delete(t.lruIndex, evicted.id)
		if err := evicted.store.Close(); err != nil {
			return nil, fmt.Errorf("closing evicted lru queue %d: %w", evicted.id, err)
		}
	}
	store, err := t.openQueue(ref)
	if err != nil {
		return nil, err
	}
	elem := t.lru.PushFront(&lruEntry{id: id, store: store})
	t.lruIndex[id] = elem
	return store, nil
}

// ondemandOp times the whole open-act-close cycle as one operation, because
// that unconditional Close (fsync plus a state.bson rewrite) is
// what R5-01 found the plan's own F-07 correction missed for this arm.
func (t *filesTarget) ondemandOp(ref queueRef, act func(*legacyqueue.SegmentStore) error) queueOpResult {
	lock := &t.queueLocks[queueID(ref)%queueLockStripes]
	lock.Lock()
	defer lock.Unlock()

	start := time.Now()
	store, err := t.openQueue(ref)
	if err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("ondemand open: %w", err)}
	}
	actErr := act(store)
	closeErr := store.Close()
	dur := time.Since(start)
	if actErr != nil {
		return queueOpResult{Duration: dur, Err: actErr}
	}
	if closeErr != nil {
		return queueOpResult{Duration: dur, Err: fmt.Errorf("ondemand close: %w", closeErr)}
	}
	return queueOpResult{Duration: dur}
}

func (t *filesTarget) heldOrLRUOp(ref queueRef, act func(*legacyqueue.SegmentStore) error) queueOpResult {
	store, err := t.withStore(ref)
	if err != nil {
		return queueOpResult{Err: err}
	}
	start := time.Now()
	err = act(store)
	return queueOpResult{Duration: time.Since(start), Err: err}
}

func (t *filesTarget) op(ref queueRef, act func(*legacyqueue.SegmentStore) error) queueOpResult {
	if t.openMode == "ondemand" {
		return t.ondemandOp(ref, act)
	}
	return t.heldOrLRUOp(ref, act)
}

func (t *filesTarget) Push(ref queueRef, entry []byte) queueOpResult {
	return t.op(ref, func(s *legacyqueue.SegmentStore) error {
		_, err := s.Push(entry)
		return err
	})
}

func (t *filesTarget) Peek(ref queueRef) queueOpResult {
	return t.op(ref, func(s *legacyqueue.SegmentStore) error {
		_, err := s.Peek()
		return err
	})
}

func (t *filesTarget) Pop(ref queueRef) queueOpResult {
	return t.op(ref, func(s *legacyqueue.SegmentStore) error {
		_, err := s.Pop()
		return err
	})
}

func (t *filesTarget) DeleteAtPosition(ref queueRef, pos int) queueOpResult {
	return t.op(ref, func(s *legacyqueue.SegmentStore) error {
		return s.DeleteAtPosition(pos)
	})
}

// Census times the F-04 stand-in: a fresh, timed open-then-close of every
// queue directory, two calls per item -- what LoadCatalog actually pays
// today (one manifest read the harness has no manifest to reproduce, plus a
// flat per-bucket open), at the now-flat per-bucket open cost, not a tree walk.
func (t *filesTarget) Census(items []forestItem) (censusResult, error) {
	start := time.Now()
	opened := 0
	for _, item := range items {
		for _, kind := range []queueKind{queueA, queueB} {
			ref := queueRef{ItemIndex: item.Index, Kind: kind}
			store, err := t.openQueue(ref)
			if err != nil {
				return censusResult{}, fmt.Errorf("census open of queue %d: %w", queueID(ref), err)
			}
			if err := store.Close(); err != nil {
				return censusResult{}, fmt.Errorf("census close of queue %d: %w", queueID(ref), err)
			}
			opened++
		}
	}
	elapsed := millis(time.Since(start))
	perItem := 0.0
	if len(items) > 0 {
		perItem = elapsed / float64(len(items))
	}
	return censusResult{
		Mechanism:     "legacyqueue.NewSegmentStore loop, 2 calls/item (LoadCatalog stand-in, F-04)",
		Applicable:    true,
		OpensPerItem:  2,
		TotalOpens:    opened,
		TotalMillis:   elapsed,
		MillisPerItem: perItem,
	}, nil
}

func (t *filesTarget) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for id, store := range t.resident {
		if err := store.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing held queue %d: %w", id, err)
		}
	}
	t.resident = make(map[int]*legacyqueue.SegmentStore)
	if t.lru != nil {
		for elem := t.lru.Front(); elem != nil; elem = elem.Next() {
			entry := elem.Value.(*lruEntry)
			if err := entry.store.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("closing lru queue %d: %w", entry.id, err)
			}
		}
		t.lru.Init()
		t.lruIndex = make(map[int]*list.Element)
	}
	return firstErr
}

// --- badger-shared: the §3.4 key design in one instance -----------------

const (
	nsQueueEntry byte = 0x04
	nsQueueState byte = 0x05
)

func badgerEntryKey(id uint32, position uint64) []byte {
	key := make([]byte, 4+1+8)
	binary.BigEndian.PutUint32(key[0:4], id)
	key[4] = nsQueueEntry
	binary.BigEndian.PutUint64(key[5:13], position)
	return key
}

// isQueueEntryKey reports whether key is an entry key (namespace 0x04)
// belonging to this exact queue id. A forward walk that lands on this
// queue's own 5-byte state key (namespace 0x05, which sorts immediately
// after 0x04 for the same id -- §3.4's namespace byte comes right after the
// queue-entry namespace) must not be decoded as a position: that is what an
// exhausted queue's next key after its head looks like, not a next entry.
func isQueueEntryKey(key []byte, id uint32) bool {
	return len(key) == 13 && binary.BigEndian.Uint32(key[0:4]) == id && key[4] == nsQueueEntry
}

func badgerEntryPosition(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[5:13])
}

func badgerStateKey(id uint32) []byte {
	key := make([]byte, 4+1)
	binary.BigEndian.PutUint32(key[0:4], id)
	key[4] = nsQueueState
	return key
}

// badgerStateDoc is the shared-storage-instance-spec record §3.4's state
// document shape, minus applied_seq: this harness has no replication WAL to
// fence, so the field would be a constant zero forever and is left out
// rather than carried as dead weight.
type badgerStateDoc struct {
	Head    uint64 `bson:"head"`
	Tail    uint64 `bson:"tail"`
	Next    uint64 `bson:"next"`
	Entries int64  `bson:"entries"`
}

func encodeBadgerState(head, tail, next uint64, entries int64) ([]byte, error) {
	encoded, err := bson.Marshal(badgerStateDoc{Head: head, Tail: tail, Next: next, Entries: entries})
	if err != nil {
		return nil, fmt.Errorf("encoding queue state: %w", err)
	}
	return encoded, nil
}

// badgerQueueState is the in-memory cursor set §3.4 describes as living in
// the state document. Keeping it in memory during the run and persisting it
// in the same transaction as every write -- rather than reading it back from
// Badger before every op -- mirrors how SegmentStore itself keeps head/tail
// in memory and only durably saves them opportunistically; reading state
// back from disk before every push would price a cost neither engine's real
// design pays.
type badgerQueueState struct {
	mu      sync.Mutex
	id      uint32
	head    uint64
	tail    uint64
	next    uint64
	entries int64
}

type badgerTarget struct {
	db     *badger.DB
	synced bool
	states []badgerQueueState
}

func newBadgerTarget(dataDir string, synced bool) (*badgerTarget, error) {
	opts := productionBadgerOptions(filepath.Join(dataDir, "badger-shared")).WithSyncWrites(synced)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening the shared badger instance: %w", err)
	}
	return &badgerTarget{db: db, synced: synced}, nil
}

func (t *badgerTarget) Name() string {
	if t.synced {
		return "badger-shared/synced"
	}
	return "badger-shared/unsynced"
}

func (t *badgerTarget) Prepare(items []forestItem) error {
	t.states = make([]badgerQueueState, len(items)*2)
	for i := range t.states {
		t.states[i].id = uint32(i)
	}
	return nil
}

func (t *badgerTarget) Push(ref queueRef, entry []byte) queueOpResult {
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()

	position := st.next
	start := time.Now()
	err := t.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(badgerEntryKey(st.id, position), entry); err != nil {
			return fmt.Errorf("setting entry: %w", err)
		}
		encoded, encErr := encodeBadgerState(st.head, position, st.next+1, st.entries+1)
		if encErr != nil {
			return encErr
		}
		return txn.Set(badgerStateKey(st.id), encoded)
	})
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: err}
	}
	st.tail = position
	st.next++
	st.entries++
	return queueOpResult{Duration: dur}
}

// cursorSeek is the §3.4 seek-to-cursor position for a peek/pop: head for
// fifo (forward), tail for lifo (reverse).
func cursorSeek(kind queueKind, st *badgerQueueState) uint64 {
	if kind == queueA {
		return st.head
	}
	return st.tail
}

func (t *badgerTarget) Peek(ref queueRef) queueOpResult {
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.entries == 0 {
		return queueOpResult{Err: fmt.Errorf("peek: queue %d is empty", st.id)}
	}
	seek := badgerEntryKey(st.id, cursorSeek(ref.Kind, st))

	start := time.Now()
	err := t.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = ref.Kind == queueB
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(seek)
		if !it.Valid() {
			return fmt.Errorf("peek: no live entry at the cursor for queue %d", st.id)
		}
		_, copyErr := it.Item().ValueCopy(nil)
		if copyErr != nil {
			return fmt.Errorf("reading entry value: %w", copyErr)
		}
		return nil
	})
	return queueOpResult{Duration: time.Since(start), Err: err}
}

func (t *badgerTarget) Pop(ref queueRef) queueOpResult {
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.entries == 0 {
		return queueOpResult{Err: fmt.Errorf("pop: queue %d is empty", st.id)}
	}
	if ref.Kind == queueA {
		return t.popFIFOLocked(st)
	}
	return t.popLIFOLocked(st)
}

// popFIFOLocked seeks to the stored head position -- never to the namespace
// start -- so it never walks a tombstone run behind it, per §3.4's
// seek-to-cursor rule. Called with st.mu held.
func (t *badgerTarget) popFIFOLocked(st *badgerQueueState) queueOpResult {
	start := time.Now()
	var newHead uint64
	err := t.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(badgerEntryKey(st.id, st.head))
		if !it.Valid() {
			return fmt.Errorf("pop: no live entry at or after head %d in queue %d", st.head, st.id)
		}
		key := it.Item().KeyCopy(nil)
		if !isQueueEntryKey(key, st.id) {
			// The seek landed past this queue's last entry -- onto its own
			// state key or the next id's first entry -- which means head was
			// stale (nothing live at or after it), not that an entry exists.
			return fmt.Errorf("pop: no live entry at or after head %d in queue %d", st.head, st.id)
		}
		newHead = badgerEntryPosition(key) + 1
		if err := txn.Delete(key); err != nil {
			return fmt.Errorf("deleting entry: %w", err)
		}
		encoded, encErr := encodeBadgerState(newHead, st.tail, st.next, st.entries-1)
		if encErr != nil {
			return encErr
		}
		return txn.Set(badgerStateKey(st.id), encoded)
	})
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: err}
	}
	st.head = newHead
	st.entries--
	return queueOpResult{Duration: dur}
}

// popLIFOLocked reverse-seeks to the stored tail position. Called with
// st.mu held.
func (t *badgerTarget) popLIFOLocked(st *badgerQueueState) queueOpResult {
	start := time.Now()
	var newTail uint64
	err := t.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(badgerEntryKey(st.id, st.tail))
		if !it.Valid() {
			return fmt.Errorf("pop: no live entry at or before tail %d in queue %d", st.tail, st.id)
		}
		key := it.Item().KeyCopy(nil)
		pos := badgerEntryPosition(key)
		if pos > 0 {
			newTail = pos - 1
		}
		if err := txn.Delete(key); err != nil {
			return fmt.Errorf("deleting entry: %w", err)
		}
		encoded, encErr := encodeBadgerState(st.head, newTail, st.next, st.entries-1)
		if encErr != nil {
			return encErr
		}
		return txn.Set(badgerStateKey(st.id), encoded)
	})
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: err}
	}
	st.tail = newTail
	st.entries--
	return queueOpResult{Duration: dur}
}

// DeleteAtPosition walks forward from head counting live entries -- the same
// O(pos) shape SegmentStore.DeleteAtPosition has, since Badger's own
// deletion is physical (no logical tombstone the walk must skip) -- and
// applies §3.4's R-05 head-advance rule when the deleted entry was the head:
// any deletion at or after head that leaves no live key before it must move
// head past the deleted run in the same batch, or the next pop's seek lands
// back on a tombstone wall the rule exists to avoid.
func (t *badgerTarget) DeleteAtPosition(ref queueRef, pos int) queueOpResult {
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if pos < 0 || int64(pos) >= st.entries {
		return queueOpResult{Err: fmt.Errorf("position %d out of range [0, %d) for queue %d", pos, st.entries, st.id)}
	}

	start := time.Now()
	newHead := st.head
	err := t.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(badgerEntryKey(st.id, st.head))
		for step := 0; step < pos; step++ {
			if !it.Valid() {
				return fmt.Errorf("position %d not found during scan of queue %d", pos, st.id)
			}
			it.Next()
		}
		if !it.Valid() {
			return fmt.Errorf("position %d not found during scan of queue %d", pos, st.id)
		}
		deletedKey := it.Item().KeyCopy(nil)
		deletedPos := badgerEntryPosition(deletedKey)
		if err := txn.Delete(deletedKey); err != nil {
			return fmt.Errorf("deleting entry: %w", err)
		}
		if deletedPos == st.head {
			it.Next()
			nextKey := []byte(nil)
			if it.Valid() {
				nextKey = it.Item().KeyCopy(nil)
			}
			if isQueueEntryKey(nextKey, st.id) {
				newHead = badgerEntryPosition(nextKey)
			} else {
				// No live entry follows in this queue -- either the
				// iterator ran past every key (!it.Valid()) or it landed on
				// this queue's own state key (ns 0x05) or the next queue's
				// first entry (a different id). Either way this queue is
				// now empty, and head means nothing until the next Push
				// sets it again via a fresh position.
				newHead = st.next
			}
		}
		encoded, encErr := encodeBadgerState(newHead, st.tail, st.next, st.entries-1)
		if encErr != nil {
			return encErr
		}
		return txn.Set(badgerStateKey(st.id), encoded)
	})
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: err}
	}
	st.head = newHead
	st.entries--
	return queueOpResult{Duration: dur}
}

// Census is O(1) for badger-shared: the instance is opened once, before the
// workload, not per item -- so there is nothing to time per item, which is
// itself the arm's structural answer to the census question.
func (t *badgerTarget) Census([]forestItem) (censusResult, error) {
	return censusResult{
		Mechanism:  "badger-shared holds one instance opened once; there is no per-item open to time",
		Applicable: false,
	}, nil
}

func (t *badgerTarget) Close() error {
	if err := t.db.Close(); err != nil {
		return fmt.Errorf("closing the shared badger instance: %w", err)
	}
	return nil
}

// --- mongodb: a key-prefixed range in one collection ---------------------

// mongoEntryID is the plan's Q5 disposition: a key-prefixed range in one
// collection, the closest analogue to both the segment design and the
// badger-shared key scheme. Fixed-width hex keeps lexical _id ordering equal
// to numeric position ordering, which is what makes a $gte/$lte range query
// a seek rather than a collection scan.
func mongoEntryID(id uint32, position uint64) string {
	return fmt.Sprintf("%08x:%016x", id, position)
}

type mongoEntryDoc struct {
	ID    string `bson:"_id"`
	Entry []byte `bson:"entry"`
}

// mongoQueueState mirrors badgerQueueState: kept in memory for the run,
// not read back from Mongo before every op. Unlike badger-shared this
// target does not also persist a state document per op -- it is not run in
// this pass (the plan's own text: "built now, run later"), and the
// simplification is recorded rather than silently carried into a fair
// comparison later.
type mongoQueueState struct {
	mu      sync.Mutex
	head    uint64
	tail    uint64
	next    uint64
	entries int64
}

type mongoTarget struct {
	client  *mongo.Client
	entries *mongo.Collection
	ctx     context.Context
	states  []mongoQueueState
}

func newMongoTarget(ctx context.Context, uri string) (*mongoTarget, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connecting to mongodb: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("pinging mongodb: %w", err)
	}
	entries := client.Database("QueueForest").Collection("entries")
	if err := entries.Drop(ctx); err != nil {
		return nil, fmt.Errorf("dropping the entries collection: %w", err)
	}
	return &mongoTarget{client: client, entries: entries, ctx: ctx}, nil
}

func (t *mongoTarget) Name() string { return "mongo" }

func (t *mongoTarget) Prepare(items []forestItem) error {
	t.states = make([]mongoQueueState, len(items)*2)
	return nil
}

func (t *mongoTarget) Push(ref queueRef, entry []byte) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()

	position := st.next
	start := time.Now()
	_, err := t.entries.InsertOne(t.ctx, mongoEntryDoc{ID: mongoEntryID(id, position), Entry: entry})
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: fmt.Errorf("mongo push: %w", err)}
	}
	st.tail = position
	st.next++
	st.entries++
	return queueOpResult{Duration: dur}
}

// cursorFilter bounds a range query to this queue's own key range -- without
// the upper/lower bound, a queue that is exhausted mid-scan would let the
// query fall through into the next queue's keys, since a plain $gte has no
// natural stopping point at a queue boundary.
func cursorFilter(id uint32, kind queueKind, st *mongoQueueState) (bson.M, int) {
	if kind == queueA {
		return bson.M{"_id": bson.M{"$gte": mongoEntryID(id, st.head), "$lt": mongoEntryID(id+1, 0)}}, 1
	}
	return bson.M{"_id": bson.M{"$gte": mongoEntryID(id, 0), "$lte": mongoEntryID(id, st.tail)}}, -1
}

func (t *mongoTarget) Peek(ref queueRef) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.entries == 0 {
		return queueOpResult{Err: fmt.Errorf("peek: queue %d is empty", id)}
	}
	filter, sortDir := cursorFilter(id, ref.Kind, st)
	findOpts := options.FindOne().SetSort(bson.D{{Key: "_id", Value: sortDir}})

	start := time.Now()
	var doc mongoEntryDoc
	err := t.entries.FindOne(t.ctx, filter, findOpts).Decode(&doc)
	if err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mongo peek: %w", err)}
	}
	return queueOpResult{Duration: time.Since(start)}
}

func (t *mongoTarget) Pop(ref queueRef) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.entries == 0 {
		return queueOpResult{Err: fmt.Errorf("pop: queue %d is empty", id)}
	}
	filter, sortDir := cursorFilter(id, ref.Kind, st)
	findOpts := options.FindOneAndDelete().SetSort(bson.D{{Key: "_id", Value: sortDir}})

	start := time.Now()
	var doc mongoEntryDoc
	err := t.entries.FindOneAndDelete(t.ctx, filter, findOpts).Decode(&doc)
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: fmt.Errorf("mongo pop: %w", err)}
	}
	poppedID, decErr := decodeMongoEntryID(doc.ID)
	if decErr != nil {
		return queueOpResult{Duration: dur, Err: decErr}
	}
	if ref.Kind == queueA {
		st.head = poppedID.position + 1
	} else if poppedID.position > 0 {
		st.tail = poppedID.position - 1
	}
	st.entries--
	return queueOpResult{Duration: dur}
}

type mongoDecodedID struct {
	queue    uint32
	position uint64
}

func decodeMongoEntryID(id string) (mongoDecodedID, error) {
	var decoded mongoDecodedID
	if _, err := fmt.Sscanf(id, "%08x:%016x", &decoded.queue, &decoded.position); err != nil {
		return mongoDecodedID{}, fmt.Errorf("decoding mongo entry id %q: %w", id, err)
	}
	return decoded, nil
}

func (t *mongoTarget) DeleteAtPosition(ref queueRef, pos int) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if pos < 0 || int64(pos) >= st.entries {
		return queueOpResult{Err: fmt.Errorf("position %d out of range [0, %d) for queue %d", pos, st.entries, id)}
	}

	filter := bson.M{"_id": bson.M{"$gte": mongoEntryID(id, st.head), "$lt": mongoEntryID(id+1, 0)}}
	findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetSkip(int64(pos)).SetLimit(1)

	start := time.Now()
	cursor, err := t.entries.Find(t.ctx, filter, findOpts)
	if err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mongo delete scan: %w", err)}
	}
	defer func() { _ = cursor.Close(t.ctx) }()
	if !cursor.Next(t.ctx) {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("position %d not found during scan of queue %d", pos, id)}
	}
	var doc mongoEntryDoc
	if err := cursor.Decode(&doc); err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("decoding scanned entry: %w", err)}
	}
	if _, err := t.entries.DeleteOne(t.ctx, bson.M{"_id": doc.ID}); err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mongo delete: %w", err)}
	}
	dur := time.Since(start)

	deletedPos, decErr := decodeMongoEntryID(doc.ID)
	if decErr != nil {
		return queueOpResult{Duration: dur, Err: decErr}
	}
	if deletedPos.position == st.head {
		st.head = st.head + 1 // R-05: advance past a head deletion; a run of several is a repeated single-step advance across calls, same as the segment store's own per-delete cost.
	}
	st.entries--
	return queueOpResult{Duration: dur}
}

func (t *mongoTarget) Census([]forestItem) (censusResult, error) {
	return censusResult{
		Mechanism:  "mongo holds one client connection opened once; there is no per-item open to time",
		Applicable: false,
	}, nil
}

func (t *mongoTarget) Close() error {
	if err := t.client.Disconnect(t.ctx); err != nil {
		return fmt.Errorf("disconnecting from mongodb: %w", err)
	}
	return nil
}

// --- mysql: a key-prefixed range in one table -----------------------------

type mysqlQueueState struct {
	mu      sync.Mutex
	head    uint64
	tail    uint64
	next    uint64
	entries int64
}

type mysqlTarget struct {
	db     *sql.DB
	states []mysqlQueueState
}

func newMySQLTarget(dsn string) (*mysqlTarget, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening mysql: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("pinging mysql: %w", err)
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS queueforest_entries"); err != nil {
		return nil, fmt.Errorf("dropping queueforest_entries: %w", err)
	}
	const createTable = `CREATE TABLE queueforest_entries (
		queue_id INT UNSIGNED NOT NULL,
		position BIGINT UNSIGNED NOT NULL,
		entry LONGBLOB NOT NULL,
		PRIMARY KEY (queue_id, position)
	) ENGINE=InnoDB`
	if _, err := db.Exec(createTable); err != nil {
		return nil, fmt.Errorf("creating queueforest_entries: %w", err)
	}
	return &mysqlTarget{db: db}, nil
}

func (t *mysqlTarget) Name() string { return "mysql" }

func (t *mysqlTarget) Prepare(items []forestItem) error {
	t.states = make([]mysqlQueueState, len(items)*2)
	return nil
}

func (t *mysqlTarget) Push(ref queueRef, entry []byte) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()

	position := st.next
	start := time.Now()
	_, err := t.db.Exec("INSERT INTO queueforest_entries (queue_id, position, entry) VALUES (?, ?, ?)", id, position, entry)
	dur := time.Since(start)
	if err != nil {
		return queueOpResult{Duration: dur, Err: fmt.Errorf("mysql push: %w", err)}
	}
	st.tail = position
	st.next++
	st.entries++
	return queueOpResult{Duration: dur}
}

func (t *mysqlTarget) Peek(ref queueRef) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.entries == 0 {
		return queueOpResult{Err: fmt.Errorf("peek: queue %d is empty", id)}
	}
	query, args := mysqlCursorQuery("entry", id, ref.Kind, st, false)

	start := time.Now()
	var entry []byte
	err := t.db.QueryRow(query, args...).Scan(&entry)
	if err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql peek: %w", err)}
	}
	return queueOpResult{Duration: time.Since(start)}
}

// mysqlCursorQuery builds the seek-to-cursor point query: the primary key is
// (queue_id, position), so a >=/<= bound on position with ORDER BY ... LIMIT
// 1 is a single index seek, not a scan.
func mysqlCursorQuery(selectCols string, id uint32, kind queueKind, st *mysqlQueueState, forDelete bool) (string, []interface{}) {
	suffix := ""
	if forDelete {
		suffix = " FOR UPDATE"
	}
	if kind == queueA {
		return fmt.Sprintf("SELECT %s FROM queueforest_entries WHERE queue_id = ? AND position >= ? ORDER BY position ASC LIMIT 1%s", selectCols, suffix),
			[]interface{}{id, st.head}
	}
	return fmt.Sprintf("SELECT %s FROM queueforest_entries WHERE queue_id = ? AND position <= ? ORDER BY position DESC LIMIT 1%s", selectCols, suffix),
		[]interface{}{id, st.tail}
}

func (t *mysqlTarget) Pop(ref queueRef) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.entries == 0 {
		return queueOpResult{Err: fmt.Errorf("pop: queue %d is empty", id)}
	}

	start := time.Now()
	txn, err := t.db.Begin()
	if err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql pop begin: %w", err)}
	}
	query, args := mysqlCursorQuery("position", id, ref.Kind, st, true)
	var poppedPos uint64
	if err := txn.QueryRow(query, args...).Scan(&poppedPos); err != nil {
		_ = txn.Rollback()
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql pop select: %w", err)}
	}
	if _, err := txn.Exec("DELETE FROM queueforest_entries WHERE queue_id = ? AND position = ?", id, poppedPos); err != nil {
		_ = txn.Rollback()
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql pop delete: %w", err)}
	}
	if err := txn.Commit(); err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql pop commit: %w", err)}
	}
	dur := time.Since(start)

	if ref.Kind == queueA {
		st.head = poppedPos + 1
	} else if poppedPos > 0 {
		st.tail = poppedPos - 1
	}
	st.entries--
	return queueOpResult{Duration: dur}
}

func (t *mysqlTarget) DeleteAtPosition(ref queueRef, pos int) queueOpResult {
	id := uint32(queueID(ref))
	st := &t.states[queueID(ref)]
	st.mu.Lock()
	defer st.mu.Unlock()
	if pos < 0 || int64(pos) >= st.entries {
		return queueOpResult{Err: fmt.Errorf("position %d out of range [0, %d) for queue %d", pos, st.entries, id)}
	}

	start := time.Now()
	txn, err := t.db.Begin()
	if err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql delete begin: %w", err)}
	}
	var deletedPos uint64
	scanErr := txn.QueryRow(
		"SELECT position FROM queueforest_entries WHERE queue_id = ? AND position >= ? ORDER BY position ASC LIMIT 1 OFFSET ? FOR UPDATE",
		id, st.head, pos,
	).Scan(&deletedPos)
	if scanErr != nil {
		_ = txn.Rollback()
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("position %d not found during scan of queue %d: %w", pos, id, scanErr)}
	}
	if _, err := txn.Exec("DELETE FROM queueforest_entries WHERE queue_id = ? AND position = ?", id, deletedPos); err != nil {
		_ = txn.Rollback()
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql delete: %w", err)}
	}
	if err := txn.Commit(); err != nil {
		return queueOpResult{Duration: time.Since(start), Err: fmt.Errorf("mysql delete commit: %w", err)}
	}
	dur := time.Since(start)

	if deletedPos == st.head {
		st.head++ // R-05: advance past a head deletion, same one-step rule as the badger and mongo arms.
	}
	st.entries--
	return queueOpResult{Duration: dur}
}

func (t *mysqlTarget) Census([]forestItem) (censusResult, error) {
	return censusResult{
		Mechanism:  "mysql holds one connection pool opened once; there is no per-item open to time",
		Applicable: false,
	}, nil
}

func (t *mysqlTarget) Close() error {
	if err := t.db.Close(); err != nil {
		return fmt.Errorf("closing mysql: %w", err)
	}
	return nil
}
