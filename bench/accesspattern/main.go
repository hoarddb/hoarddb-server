// bench/accesspattern measures what hoardDB's per-engine key encoding buys, and
// what a traditional engine pays instead. It backs the pitch in
// the product-pitch record:24-31 ("sort 10B rows on every request" versus
// "LIFO: pop top of stack, O(1)"), which until now had no measurement behind it.
//
// Each leg pairs one hoardDB engine against the work a document engine redoes
// per request for the same question:
//
//	fifo   the oldest element      vs  a scan and a sort to find the oldest
//	lifo   the newest element      vs  a scan and a sort to find the newest
//	heap   the top-priority item   vs  a scan and a sort to find the top
//	btree  the low end of a range vs  a scan and a sort to find the low end
//	write  the cost of the layout vs  the cost of maintaining an index
//
// Both sides of a read leg do the SAME work on the SAME data with the SAME
// engine underneath — the native side is hoardDB's keyspace, the scan-and-sort
// side is "what the answer would have cost if the answer had been computed at
// query time rather than written at write time". That is the whole claim: the
// access pattern is decided once, at bucket creation, and paid for in the key
// encoding, not re-derived by a planner on every request.
//
// The honest boundary, which no number here should be quoted past: the
// scan-and-sort side is a Go sort over the values the store returns, and it is
// deliberately naive — no index, no optimizer, no cache, no prepared plan. A
// mature competitor with a real covering index on that column would narrow the
// gap on the READ legs. What no index closes is the WRITE leg, which is
// measured separately: an index is paid on every write, and the engine-native
// layout does not pay it at all. The two halves are reported separately,
// because quoting the product as a head-to-head against a tuned engine would be
// the same mistake bench/RESULTS.md:281-283 records from 2026-09-13.
//
// It is deliberately NOT a Go test and NOT wired into CI: a run takes minutes
// and moves real bytes. Run it by hand:
//
//	go run ./bench/accesspattern \
//	    -n=100000 \
//	    -legs=fifo,lifo,heap,btree,write \
//	    -repeats=3 \
//	    -out=/tmp/accesspattern.json
//
// Four properties make the numbers mean what they claim:
//
//  1. Every run starts from a fresh data directory, so a leg never inherits
//     another leg's keys. Badger cannot free stale keys cheaply, and a shared
//     directory is exactly how a shared-dir leg cost -21.6% insert on
//     2026-09-13 (bench/RESULTS.md).
//  2. Every read leg reports BOTH wall time and bytes read from the kernel
//     (/proc/self/io read_bytes), not wall time alone. The two can disagree: a
//     full scan is CPU-bound and reads everything; a native seek reads a few
//     KB. Counting bytes is what separates "the answer is in the key order"
//     from "the answer is in the cache" (docs/AGENTS.md: filter byte counters,
//     not clocks).
//  3. The fill is synced before either side is measured, so a read that would
//     otherwise arrive as "not yet in the memtable" is not timed as one.
//  4. The answer each leg returns is checked, not just timed. Both sides of
//     every pair must return the same answer, or the ratio measures two
//     different questions. A read path that returns nothing is not a fast read
//     path — that is the 2026-09-16 rule.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	// slot, databaseID and id are fixed because the engines key on them; each leg gets its
	// own directory, so two legs in one run cannot collide.
	slot       = uint16(0)
	databaseID = uint64(1)
	id         = uint64(1)

	// batchSize is how many elements one fill batch writes, so a large run does
	// not open one transaction per element.
	batchSize = 1000
)

func main() {
	n := flag.Int("n", 100000, "elements per leg")
	legList := flag.String("legs", "fifo,lifo,heap,btree,write", "legs to run, comma-separated")
	repeats := flag.Int("repeats", 3, "interleaved runs per leg")
	dataDir := flag.String("data-dir", "", "parent for the per-run directories; empty = a temp dir")
	out := flag.String("out", "", "write the raw per-sample record here as JSON (not committed)")
	flag.Parse()

	if *n < 1 {
		fatal("--n must be at least 1")
	}
	legs, err := parseLegs(*legList)
	if err != nil {
		fatal("%v", err)
	}

	root := *dataDir
	if root == "" {
		root, err = os.MkdirTemp("", "hoarddb-accesspattern-")
		if err != nil {
			fatal("temp dir: %v", err)
		}
		// A temp directory left behind is noise, not a failure worth aborting
		// a completed run over — the caller passed no -data-dir to keep.
		defer func() { _ = os.RemoveAll(root) }()
	}

	fmt.Printf("access-pattern benchmark: n=%d legs=%s repeats=%d\n", *n, strings.Join(legNames(legs), ","), *repeats)
	fmt.Println("Each read leg: hoardDB's native key order vs the same question answered by")
	fmt.Println("a scan-and-sort at query time. Each side also reports how many elements")
	fmt.Println("it had to touch, which is the claim's own content.")
	// Badger logs its LSM level table on every open, which buries the results.
	// It already goes to stderr, so `2>/dev/null` when you want only the table
	// — there is no logging switch to add here and the store's options are
	// shared with the server, so the benchmark does not touch them.

	record := &Record{
		N:          *n,
		Legs:       strings.Join(legNames(legs), ","),
		Repeats:    *repeats,
		Host:       hostFacts(),
		Recorded:   time.Now().UTC().Format(time.RFC3339),
		MemTableMB: benchMemTableMB,
	}
	var results []LegResult

	// Interleaved by design: a leg's samples alternate with every other leg's,
	// so a host that drifts (thermal, a neighbour, compaction) spreads that
	// drift across all legs instead of loading it onto whichever leg ran last.
	for r := 0; r < *repeats; r++ {
		for _, leg := range legs {
			dir := filepath.Join(root, fmt.Sprintf("%s-r%d", leg.name, r))
			// The write leg runs in its own pair of directories, so it cannot
			// inherit another leg's keys — its indexed half writes twice as
			// many.
			var run LegResult
			var err error
			if leg.writeLeg {
				run, err = leg.runWrite(dir, *n)
			} else {
				run, err = leg.run(dir, *n)
			}
			if err != nil {
				// A leg that cannot run is recorded with its reason and the run
				// continues: the report must not imply coverage it lacks.
				results = append(results, LegResult{Leg: leg.name, Repeat: r, Error: err.Error()})
				fmt.Printf("  %-6s r%d  FAILED: %v\n", leg.name, r, err)
				continue
			}
			run.Repeat = r
			results = append(results, run)
			fmt.Printf("  %-6s r%d  %s %10s  %s %10s  %6s   %s %d / %d\n",
				leg.name, r,
				leg.leftLabel(), fmtDur(run.Native.Duration),
				leg.rightLabel(), fmtDur(run.ScanSort.Duration),
				fmtRatio(run.Ratio),
				leg.workLabel(), run.Native.Touched, run.ScanSort.Touched)
			_ = os.RemoveAll(dir)
		}
	}

	record.Results = results

	fmt.Println("\nMedian of the runs:")
	for _, leg := range legs {
		med := medianFor(results, leg.name)
		if med == nil {
			fmt.Printf("  %-6s  no successful run\n", leg.name)
			continue
		}
		fmt.Printf("  %-6s  %s %10s  %s %10s  %6s   %s %d / %d\n",
			leg.name,
			leg.leftLabel(), fmtDur(med.Native.Duration),
			leg.rightLabel(), fmtDur(med.ScanSort.Duration),
			fmtRatio(med.Ratio),
			leg.workLabel(), med.Native.Touched, med.ScanSort.Touched)
	}

	// Elements touched is the claim's real content — it is what says the answer
	// was in the key order rather than computed from every row — so it gets its
	// own line rather than sitting under the time ratio.
	fmt.Println("\nElements touched (the answer is in the key order, not computed per request):")
	for _, leg := range legs {
		med := medianFor(results, leg.name)
		if med == nil {
			continue
		}
		if leg.writeLeg {
			fmt.Printf("  %-6s  %s wrote %d key(s); %s wrote %d\n",
				leg.name, leg.leftLabel(), med.Native.Touched, leg.rightLabel(), med.ScanSort.Touched)
			continue
		}
		fmt.Printf("  %-6s  %s read %d element(s); %s read %d\n",
			leg.name, leg.leftLabel(), med.Native.Touched, leg.rightLabel(), med.ScanSort.Touched)
	}

	if *out != "" {
		blob, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			fatal("marshal record: %v", err)
		}
		if err := os.WriteFile(*out, append(blob, '\n'), 0o600); err != nil {
			fatal("write record: %v", err)
		}
		fmt.Printf("\nraw per-sample record: %s\n", *out)
	}

	fmt.Println("\nHonest boundary: the scan-and-sort side is a naive Go sort over the values")
	fmt.Println("the store returns — no index, no optimizer, no cache. A competitor with a")
	fmt.Println("real covering index narrows the read legs. What no index closes is the")
	fmt.Println("write leg: an index is paid on every write, and the engine-native layout")
	fmt.Println("does not pay it. Quote the two halves separately, never the product as a")
	fmt.Println("head-to-head against a tuned engine.")
}

// leg is one comparison: a hoardDB engine's own order against the same question
// answered at query time.
type leg struct {
	name string
	// writeLeg marks the one leg where both sides write, so the report does
	// not call its counters reads.
	writeLeg bool
	// fill writes the data set once; both sides then work on the same
	// directory.
	fill func(kv storage.KV, n int) error
	// left answers the question the way hoardDB's keyspace answers it.
	left func(q *stores) (string, error)
	// right answers the same question by scanning and sorting at query time.
	right func(q *stores) (string, error)
	// leftSecond and rightSecond are the write leg's two halves, which write
	// rather than answer a question; see writeleg.go.
	leftSecond  func(kv storage.KV, n int) error
	rightSecond func(kv storage.KV, n int) error
}

func (l leg) leftLabel() string {
	if l.writeLeg {
		return "layout"
	}
	return "native"
}

func (l leg) rightLabel() string {
	if l.writeLeg {
		return "indexed"
	}
	return "scan+sort"
}

// workLabel names what the two counts are counting, so a reader is never left
// guessing whether a number is elements read or keys written.
func (l leg) workLabel() string {
	if l.writeLeg {
		return "keys"
	}
	return "read"
}

// stores is the engines one leg needs, all on one shared store, exactly as a
// node's shared-storage layout (GAP-31) puts them.
type stores struct {
	kv    storage.KV
	queue *store.QueueStore
	heap  *store.HeapStore
	btree *store.BTreeStore
	// count, when non-nil, is incremented once per element a side reads. It is
	// how a leg reports how much it had to touch, which is the claim's content
	// and the only counter that moves on a memory-mapped store.
	count *int
}

// touched records that this side read one element.
func (s *stores) touched() {
	if s.count != nil {
		*s.count++
	}
}

func (l leg) run(dir string, n int) (LegResult, error) {
	kv, err := storage.OpenBadgerKV(dir, benchTuning(), nil)
	if err != nil {
		return LegResult{}, fmt.Errorf("open %s: %w", dir, err)
	}
	defer func() { _ = kv.Close() }()

	if err := l.fill(kv, n); err != nil {
		return LegResult{}, fmt.Errorf("fill: %w", err)
	}
	// The fill's own writes must not land inside either measurement: a read
	// that arrives as "not yet in the memtable" answers a different question
	// than one served from the key order.
	if err := kv.Sync(); err != nil {
		return LegResult{}, fmt.Errorf("sync after fill: %w", err)
	}
	q := &stores{
		kv:    kv,
		queue: store.NewQueueStore(kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, new(store.OwedApplies), store.QueueFIFO),
		heap:  store.NewHeapStore(kv, slot, databaseID, id, new(store.OwedApplies)),
		btree: store.NewBTreeStore(kv, slot, databaseID, id, new(store.OwedApplies)),
	}

	// touched counts the elements each side had to read, which is the quantity
	// the claim is about and the only counter that moves on this store (see
	// the note above readBytes).
	var leftTouched, rightTouched int
	q.count = &leftTouched
	leftStart := time.Now()
	leftAnswer, err := l.left(q)
	if err != nil {
		return LegResult{}, fmt.Errorf("%s: %w", l.leftLabel(), err)
	}
	leftDur := time.Since(leftStart)
	// Every native side answers from exactly one key — a Peek, a First, a
	// bounded Range — so if one ever reports touching more than one element, the
	// engine's own order has stopped being the reason, and the ratio would be
	// measuring something other than what it claims.
	if leftTouched > 1 {
		return LegResult{}, fmt.Errorf("the native side touched %d elements; a native answer must come from one key", leftTouched)
	}
	q.count = &rightTouched
	rightStart := time.Now()
	rightAnswer, err := l.right(q)
	if err != nil {
		return LegResult{}, fmt.Errorf("%s: %w", l.rightLabel(), err)
	}
	rightDur := time.Since(rightStart)

	// Both sides must answer identically, or the ratio measures two different
	// questions and means nothing.
	if leftAnswer != rightAnswer {
		return LegResult{}, fmt.Errorf("the two sides disagree: %s returned %q, %s returned %q — not the same question",
			l.leftLabel(), leftAnswer, l.rightLabel(), rightAnswer)
	}
	if leftAnswer == "" {
		return LegResult{}, errors.New("the leg returned no answer at all — a fast empty answer is not a result")
	}

	var ratio float64
	if leftDur > 0 {
		ratio = float64(rightDur) / float64(leftDur)
	}
	return LegResult{
		Leg: l.name,
		Native: Sample{
			Duration: leftDur,
			Touched:  leftTouched,
			Answer:   leftAnswer,
		},
		ScanSort: Sample{
			Duration: rightDur,
			Touched:  rightTouched,
			Answer:   rightAnswer,
		},
		Ratio: ratio,
	}, nil
}

// allLegs returns the five comparisons. Every data set is fixed by
// construction, so a run is reproducible and the two sides of a pair genuinely
// ask the same question.
func allLegs() []leg {
	legs := []leg{
		{
			name: "fifo",
			fill: func(kv storage.KV, n int) error {
				return fillQueue(store.NewQueueStore(kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, new(store.OwedApplies), store.QueueFIFO), n)
			},
			// The FIFO store's own key order is the queue's order
			// ([s][id][0x04][position:8], store/queue.go:60), so the oldest
			// entry is the FIRST key: one seek, no comparison, no sort.
			left: func(q *stores) (string, error) {
				q.touched() // one seek, one element: the answer IS the key order
				v, err := q.queue.Peek()
				if err != nil {
					return "", err
				}
				return digest(v), nil
			},
			// The same answer computed at query time: read every element and
			// sort by the order the engine was supposed to have been giving us
			// for free.
			right: func(q *stores) (string, error) {
				var all [][]byte
				if err := q.queue.Walk(func(e []byte) error {
					q.touched()
					all = append(all, e)
					return nil
				}); err != nil {
					return "", err
				}
				all = sortIntoOrder(all)
				if len(all) == 0 {
					return "", nil
				}
				return digest(all[0]), nil
			},
		},
		{
			name: "lifo",
			fill: func(kv storage.KV, n int) error {
				return fillQueue(store.NewQueueStore(kv, storage.BucketRef{Slot: slot, DatabaseID: databaseID, BucketID: id}, new(store.OwedApplies), store.QueueLIFO), n)
			},
			// LIFO's key order gives the newest entry last, so "the most recent"
			// is the LAST key: still one seek, with Reverse.
			left: func(q *stores) (string, error) {
				q.touched() // one seek, one element: the answer IS the key order
				v, err := q.queue.Peek()
				if err != nil {
					return "", err
				}
				return digest(v), nil
			},
			right: func(q *stores) (string, error) {
				var all [][]byte
				if err := q.queue.Walk(func(e []byte) error {
					q.touched()
					all = append(all, e)
					return nil
				}); err != nil {
					return "", err
				}
				// No sort here, and that is the point of this leg rather than an
				// oversight: Walk already visits in POP order, newest first for
				// lifo (store/queue.go:fromCursor), so the naive side has nothing
				// to reorder. What it still has to do is read every element to
				// learn what a walk of the key order gives for free — so this leg
				// measures the READ the native order removes, not a sort.
				if len(all) == 0 {
					return "", nil
				}
				return digest(all[0]), nil
			},
		},
		{
			name: "heap",
			fill: func(kv storage.KV, n int) error {
				return fillHeap(store.NewHeapStore(kv, slot, databaseID, id, new(store.OwedApplies)), n)
			},
			// The heap key is [inverted priority:8][entry id] (store/heap.go:17),
			// so the highest priority is the FIRST key and Peek is a seek to the
			// first key — no in-memory heap, no O(entries) rebuild on open, and
			// no sort to find the top element.
			left: func(q *stores) (string, error) {
				q.touched() // a seek to the first key: the order is stored
				v, err := q.heap.PeekItem()
				if err != nil {
					return "", err
				}
				return digest(v), nil
			},
			// "The most urgent item" with no key order to exploit: read
			// everything and sort by priority to find the top.
			right: func(q *stores) (string, error) {
				entries, err := heapDump(q.heap, q.touched)
				if err != nil {
					return "", err
				}
				sort.SliceStable(entries, func(i, j int) bool {
					return entries[i].priority > entries[j].priority
				})
				if len(entries) == 0 {
					return "", nil
				}
				return digest(entries[0].value), nil
			},
		},
		{
			name: "btree",
			fill: func(kv storage.KV, n int) error {
				return fillBTree(store.NewBTreeStore(kv, slot, databaseID, id, new(store.OwedApplies)), n)
			},
			// The btree's documents are [s][id][0x01][EncodeComponent(user key)]
			// (store/btree.go:26), so the store's own key order IS the btree's
			// order: a range is a bounded seek, not a scan and a sort.
			left: func(q *stores) (string, error) {
				// The whole key space, so the low end of the range IS the
				// first key: the seek a range scan starts with.
				q.touched() // limit 1 stops the walk at one entry
				entries, err := q.btree.Range(nil, nil, 1)
				if err != nil {
					return "", err
				}
				if len(entries) == 0 {
					return "", nil
				}
				return digest(entries[0].Value), nil
			},
			// The same range answered without an order to exploit: every
			// document, and a sort to find the low end.
			right: func(q *stores) (string, error) {
				docs, err := btreeDump(q.btree, q.touched)
				if err != nil {
					return "", err
				}
				sort.Slice(docs, func(i, j int) bool {
					return docs[i].id < docs[j].id
				})
				if len(docs) == 0 {
					return "", nil
				}
				return digest(docs[0].value), nil
			},
		},
	}
	// The write leg is appended rather than inlined: it has no answering sides,
	// and keeping it in writeleg.go stops a reader of allLegs() from expecting
	// every leg to answer a question.
	return append(legs, writeLeg())
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "accesspattern: "+format+"\n", args...)
	os.Exit(1)
}

func parseLegs(spec string) ([]leg, error) {
	want := map[string]bool{}
	for _, name := range strings.Split(spec, ",") {
		if name = strings.TrimSpace(name); name != "" {
			want[name] = true
		}
	}
	var legs []leg
	for _, l := range allLegs() {
		if want[l.name] {
			legs = append(legs, l)
			delete(want, l.name)
		}
	}
	if len(legs) == 0 {
		return nil, fmt.Errorf("no known leg in %q; known: %s", spec, strings.Join(legNames(allLegs()), ","))
	}
	var unknown []string
	for name := range want {
		unknown = append(unknown, name)
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("unknown leg(s): %s; known: %s", strings.Join(unknown, ","), strings.Join(legNames(allLegs()), ","))
	}
	return legs, nil
}

func legNames(legs []leg) []string {
	names := make([]string, 0, len(legs))
	for _, l := range legs {
		names = append(names, l.name)
	}
	return names
}

func medianFor(results []LegResult, name string) *LegResult {
	var left, right []time.Duration
	var leftT, rightT []int
	for _, r := range results {
		if r.Leg != name || r.Error != "" {
			continue
		}
		left = append(left, r.Native.Duration)
		right = append(right, r.ScanSort.Duration)
		leftT = append(leftT, r.Native.Touched)
		rightT = append(rightT, r.ScanSort.Touched)
	}
	if len(left) == 0 {
		return nil
	}
	medianDur := func(ds []time.Duration) time.Duration {
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[len(ds)/2]
	}
	medianInt := func(is []int) int {
		sort.Ints(is)
		return is[len(is)/2]
	}
	ld, rd := medianDur(left), medianDur(right)
	lt, rt := medianInt(leftT), medianInt(rightT)
	var ratio float64
	if ld > 0 {
		ratio = float64(rd) / float64(ld)
	}
	return &LegResult{
		Leg:      name,
		Native:   Sample{Duration: ld, Touched: lt},
		ScanSort: Sample{Duration: rd, Touched: rt},
		Ratio:    ratio,
	}
}

// NOTE ON WHY THIS HARNESS COUNTS ELEMENTS, NOT BYTES
//
// The obvious way to show "the answer is in the key order, not the cache" is to
// count bytes read. That does not work against a memory-mapped store, and this
// harness tried it: /proc/self/io's read_bytes counts block-device I/O, so it
// stayed at 0 for every read, and rchar counts read(2) calls, so it moved only
// by the 98 bytes of reading the counter file itself — Badger serves its reads
// from mmap'd files through the page cache, which neither counter reports. The
// same is true of the write side: Badger writes through mmap, so wchar stayed
// at 0 for a leg that moved megabytes.
//
// So this harness counts ELEMENTS TOUCHED, which is exact, host-independent, and
// is in fact the claim's own content: the native side reads the one element it
// needs, and the query-time side reads every element to find out which one it
// needed. The write leg counts the keys a document costs and the bytes the
// bucket ends up holding, both of which a shared filesystem reports truthfully.
//
// docs/AGENTS.md says to filter byte counters rather than clocks. The other half
// of that rule is this one: a counter has to MOVE on the code under test, and
// the way to know is to run it once and look. syscw and wchar do not move on
// Badger; 100 mmap'd page faults do not move rchar either.

// ioCount is kept because it is the check on whether a counter moves at all
// before a number is built on it; no leg calls it any more.
func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fus", float64(d.Nanoseconds())/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func fmtRatio(f float64) string {
	switch {
	case f >= 100:
		return fmt.Sprintf("%.0fx", f)
	case f >= 10:
		return fmt.Sprintf("%.1fx", f)
	default:
		return fmt.Sprintf("%.2fx", f)
	}
}

func hostFacts() HostFacts {
	host := HostFacts{}
	if blob, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(blob), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					host.MemTotalMB, _ = strconv.Atoi(fields[1])
					host.MemTotalMB /= 1024
				}
			}
		}
	}
	if blob, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		host.Cores = strings.Count(string(blob), "processor\t:")
	}
	return host
}
