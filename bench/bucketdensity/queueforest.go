package main

// -mode=queueforest is the instrument for
// the queue-layout-spike-plan-2026-09-22 record: does a bucket that
// holds its items as directories-with-files-inside (one directory per item,
// its fifo and lifo queues underneath) cost less, at scale, than holding the
// same shape as key ranges in one shared engine. no client can
// declare this layout (the nested-store model is not built), so the harness builds the per-item directories itself
// through legacyqueue.NewSegmentStore -- the call server/service.go made before
// the shared-storage layout retired the segment store --
// and drives the same generated workload against three comparison engines
// (badger-shared, mongo, mysql) that hold the queues as ranges instead.
//
// Every operation is one call -- one Push, one Peek, one Pop, one
// DeleteAtPosition -- because per-operation cost is the whole point; a
// generator that batches would measure something else. Every operation
// picks its queue uniformly at random from the whole item set, writes,
// peeks and the destructive phases alike (the "selection" row in the plan's
// workload table) -- a generator that fills one item before moving to the
// next measures locality, not this workload. The two destructive phases
// (drain, delete) choose their item subset once and then pick randomly
// within it, so a queue's share of deletes actually accumulates toward the
// 25% compaction trigger rather than spreading so thin across every queue
// that nothing ever crosses it.
//
// T1a and T3 (the full entry range, the full peek ratio) are built here but
// were not run in the pass this file's report section covers: the LIFO pop, the FIFO walk, the tombstone count and the
// dirty-segment sync are open defects in the segment store this
// mode drives directly, and a run at those op counts would both take days
// (the LIFO pop alone) and measure the defects, not the layout. T1b, T1c and T2
// use tiny per-queue entry counts (1-2) specifically because that keeps every
// one of those defects' costs negligible, per the dispatch that commissioned
// this file.
import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// --- shapes shared with queueforest_targets.go ----------------------------

type queueKind int

const (
	queueA queueKind = iota // fifo
	queueB                  // lifo
)

func (k queueKind) storeType() string {
	if k == queueA {
		return "fifo"
	}
	return "lifo"
}

func (k queueKind) String() string {
	return k.storeType()
}

// forestItem is one item: a directory (for files) or just an index (for the
// key-range engines) plus the two queues underneath it.
type forestItem struct {
	Index   int
	ID      string
	Dir     string // absolute path, files target only
	NameDoc []byte // the BSON-encoded Name document, files target only
}

type queueRef struct {
	ItemIndex int
	Kind      queueKind
}

// --- config ----------------------------------------------------------------

// queueForestConfig is every knob this mode takes. It is a struct because
// the parameter list crossed six the moment the workload phases needed their
// own ratios (AGENTS.md coding standard 7).
type queueForestConfig struct {
	Target      string  `json:"target"`       // files | badger | mongo | mysql
	OpenMode    string  `json:"open_mode"`    // ondemand | held | lru (files only)
	LRUCapacity int     `json:"lru_capacity"` // files/lru only
	Items       int     `json:"items"`
	MinEntries  int     `json:"min_entries_per_queue"`
	MaxEntries  int     `json:"max_entries_per_queue"`
	PeekRatio   float64 `json:"peek_ratio_intended"`
	PopMixRatio float64 `json:"pop_mix_ratio"` // fraction of reader ops that are destructive pops, not peeks (Q3)

	InterleavedDeleteRatio float64 `json:"interleaved_delete_ratio"`
	DrainItemRatio         float64 `json:"drain_item_ratio"`
	DrainPopRatio          float64 `json:"drain_pop_ratio"`
	DeleteItemRatio        float64 `json:"delete_item_ratio"`
	DeleteEntryRatio       float64 `json:"delete_entry_ratio"`
	DeleteFromHeadFraction float64 `json:"fifo_head_delete_fraction"` // fraction of the delete-phase's fifo queues trimmed from the oldest end (plan choice 3)

	SegmentSize      int64  `json:"segment_size_bytes"`
	NonDefaultSeg    bool   `json:"segment_size_is_non_default"`
	ShardedParent    bool   `json:"sharded_parent"`
	ReaderGoroutines int    `json:"reader_goroutines"`
	FDRlimit         uint64 `json:"fd_rlimit"`  // 0 = leave inherited; else syscall.Setrlimit before the run (F-03: -fd-limit does not reach an in-process harness)
	ProbeOnly        bool   `json:"probe_only"` // T1c: open every queue and hold it; no workload
	Seed             int64  `json:"seed"`
	BadgerSynced     bool   `json:"badger_synced"`
	MongoURI         string `json:"-"`
	MySQLDSN         string `json:"-"`

	EntryRepresentation string `json:"entry_representation"`
	MinFreeInodes       int64  `json:"min_free_inodes"`
}

type queueForestFlagSet struct {
	target                 *string
	openMode               *string
	items                  *int
	minEntries             *int
	maxEntries             *int
	peekRatio              *float64
	popMixRatio            *float64
	interleavedDeleteRatio *float64
	drainItemRatio         *float64
	drainPopRatio          *float64
	deleteItemRatio        *float64
	deleteEntryRatio       *float64
	deleteFromHeadFraction *float64
	segmentSize            *int64
	shardedParent          *bool
	readerGoroutines       *int
	fdRlimit               *uint64
	probeOnly              *bool
	seed                   *int64
	badgerSynced           *bool
	mongoURI               *string
	mysqlDSN               *string
	minFreeInodes          *int64
}

func registerQueueForestFlags() *queueForestFlagSet {
	return &queueForestFlagSet{
		target:                 flag.String("target", "files", "queueforest: files | badger | mongo | mysql"),
		openMode:               flag.String("open", "ondemand", "queueforest: ondemand | held | lru:N -- files only, ignored (as \"shared\") by the other three targets"),
		items:                  flag.Int("items", 10000, "queueforest: number of items"),
		minEntries:             flag.Int("min-entries", 10, "queueforest: minimum entries per queue"),
		maxEntries:             flag.Int("max-entries", 5000, "queueforest: maximum entries per queue"),
		peekRatio:              flag.Float64("peek-ratio", 5, "queueforest: intended peek count as a multiple of push count; the achieved ratio is what gets reported"),
		popMixRatio:            flag.Float64("pop-mix-ratio", 0.02, "queueforest: fraction of the reader's ops that are destructive pops instead of peeks (Q3's bounded pop mix)"),
		interleavedDeleteRatio: flag.Float64("interleaved-delete-ratio", 0.10, "queueforest: interleaved deletes as a fraction of pushes, during ingest"),
		drainItemRatio:         flag.Float64("drain-item-ratio", 0.10, "queueforest: fraction of items selected for the drain phase"),
		drainPopRatio:          flag.Float64("drain-pop-ratio", 0.30, "queueforest: fraction of a drain-selected queue's live entries popped"),
		deleteItemRatio:        flag.Float64("delete-item-ratio", 0.10, "queueforest: fraction of items selected for the delete phase (disjoint from the drain subset)"),
		deleteEntryRatio:       flag.Float64("delete-entry-ratio", 0.30, "queueforest: fraction of a delete-selected queue's live entries removed"),
		deleteFromHeadFraction: flag.Float64("delete-head-fraction", 0.5, "queueforest: fraction of the delete phase's fifo queues trimmed from the oldest end, to exercise the head-tombstone case (plan choice 3)"),
		segmentSize:            flag.Int64("segment-size", 64<<20, "queueforest: segment size in bytes for the files target; the server passes 64 MiB, so any other value must be reported as a non-default configuration"),
		shardedParent:          flag.Bool("shard-parent", true, "queueforest: two hex levels above each item directory, so no single directory holds every item"),
		readerGoroutines:       flag.Int("reader-concurrency", 1, "queueforest: concurrent reader goroutines running peeks/pops throughout all phases"),
		fdRlimit:               flag.Uint64("queueforest-fd-rlimit", 0, "queueforest: syscall.Setrlimit(RLIMIT_NOFILE, N) before the run (0 = leave inherited); -fd-limit does not reach this in-process harness (F-03)"),
		probeOnly:              flag.Bool("probe-only", false, "queueforest: open every queue under -open and hold it; run no workload (T1c's descriptor-wall probe)"),
		seed:                   flag.Int64("queueforest-seed", 20260922, "queueforest: RNG seed, for a reproducible workload"),
		badgerSynced:           flag.Bool("badger-sync", true, "queueforest: WithSyncWrites on the badger-shared target, to match the files target's unconditional per-push fsync (F-06)"),
		mongoURI:               flag.String("mongo-uri", os.Getenv("HOARDDB_BENCH_MONGO_URI"), "queueforest: mongodb URI for -target=mongo (defaults to $HOARDDB_BENCH_MONGO_URI)"),
		mysqlDSN:               flag.String("mysql-dsn", os.Getenv("HOARDDB_BENCH_MYSQL_DSN"), "queueforest: mysql DSN for -target=mysql (defaults to $HOARDDB_BENCH_MYSQL_DSN)"),
		minFreeInodes:          flag.Int64("min-free-inodes", 3000000, "queueforest: abort the run if the data directory's filesystem free-inode count falls below this -- the files target at 8 inodes/item can exhaust a shared host's inodes long before it exhausts disk bytes or MemAvailable, which the existing guards do not watch"),
	}
}

func (f *queueForestFlagSet) config() (queueForestConfig, error) {
	if err := validateTarget(*f.target); err != nil {
		return queueForestConfig{}, err
	}
	openMode, lruCapacity, err := parseOpenMode(*f.openMode)
	if err != nil {
		return queueForestConfig{}, err
	}
	if *f.items <= 0 {
		return queueForestConfig{}, fmt.Errorf("-items must be positive, got %d", *f.items)
	}
	if *f.minEntries < 0 || *f.maxEntries < *f.minEntries {
		return queueForestConfig{}, fmt.Errorf("-min-entries/-max-entries: %d..%d is not a usable range", *f.minEntries, *f.maxEntries)
	}
	if *f.readerGoroutines < 0 {
		return queueForestConfig{}, fmt.Errorf("-reader-concurrency must not be negative, got %d", *f.readerGoroutines)
	}
	if *f.probeOnly && openMode != "held" {
		return queueForestConfig{}, fmt.Errorf("-probe-only needs -open=held: it exists to hold every queue open and observe the descriptor wall (T1c), and %q does not hold anything open between operations", *f.openMode)
	}
	cfg := queueForestConfig{
		Target:                 *f.target,
		OpenMode:               openMode,
		LRUCapacity:            lruCapacity,
		Items:                  *f.items,
		MinEntries:             *f.minEntries,
		MaxEntries:             *f.maxEntries,
		PeekRatio:              *f.peekRatio,
		PopMixRatio:            *f.popMixRatio,
		InterleavedDeleteRatio: *f.interleavedDeleteRatio,
		DrainItemRatio:         *f.drainItemRatio,
		DrainPopRatio:          *f.drainPopRatio,
		DeleteItemRatio:        *f.deleteItemRatio,
		DeleteEntryRatio:       *f.deleteEntryRatio,
		DeleteFromHeadFraction: *f.deleteFromHeadFraction,
		SegmentSize:            *f.segmentSize,
		NonDefaultSeg:          *f.segmentSize != 64<<20,
		ShardedParent:          *f.shardedParent,
		ReaderGoroutines:       *f.readerGoroutines,
		FDRlimit:               *f.fdRlimit,
		ProbeOnly:              *f.probeOnly,
		Seed:                   *f.seed,
		BadgerSynced:           *f.badgerSynced,
		MongoURI:               *f.mongoURI,
		MySQLDSN:               *f.mysqlDSN,
		EntryRepresentation:    "BSON document {_id_: string, payload: string}, per-item Name.BSON is the same shape",
		MinFreeInodes:          *f.minFreeInodes,
	}
	return cfg, nil
}

func validateTarget(target string) error {
	switch target {
	case "files", "badger", "mongo", "mysql":
		return nil
	default:
		return fmt.Errorf("-target=%q is none of \"files\", \"badger\", \"mongo\", \"mysql\"", target)
	}
}

func parseOpenMode(raw string) (mode string, lruCapacity int, err error) {
	if raw == "ondemand" || raw == "held" {
		return raw, 0, nil
	}
	var n int
	if _, scanErr := fmt.Sscanf(raw, "lru:%d", &n); scanErr == nil && n > 0 {
		return "lru", n, nil
	}
	return "", 0, fmt.Errorf("-open=%q is none of \"ondemand\", \"held\", \"lru:N\"", raw)
}

// --- item / queue generation -----------------------------------------------

type forestEntryDoc struct {
	ID      string `bson:"_id_"`
	Payload string `bson:"payload"`
}

// newItemID is a random 16-byte hex id, drawn from the workload's own
// deterministic RNG so a run is reproducible from -queueforest-seed. It is
// not a strict UUID v7 (no embedded timestamp, no version/variant bits) --
// the plan's own layout diagram names UUID v7 only for the property this
// shares with it, random distribution across the sharded parent's hex
// prefix, which is what the cardinality tiers actually exercise. Recorded as
// a deviation, not hidden.
func newItemID(rng *mathrand.Rand) string {
	raw := make([]byte, 16)
	if _, err := rng.Read(raw); err != nil {
		// math/rand.Rand.Read is documented to always return len(p), nil;
		// a non-nil error here would mean the stdlib contract broke, which
		// this harness cannot recover from and a caller could not act on.
		panic(fmt.Sprintf("math/rand.Rand.Read: %v", err))
	}
	return hex.EncodeToString(raw)
}

func randomPayload(rng *mathrand.Rand, minLen, maxLen int) string {
	n := minLen
	if maxLen > minLen {
		n += rng.Intn(maxLen - minLen + 1)
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, n)
	for i := range out {
		out[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(out)
}

// buildItems generates the item set and, for the files target, the sharded
// parent directories and Name.BSON documents. entryCounts[itemIndex][kind]
// is decided here too, so the ingest schedule below can be sized exactly.
func buildItems(cfg queueForestConfig, dataDir string, rng *mathrand.Rand) ([]forestItem, [][2]int, error) {
	items := make([]forestItem, cfg.Items)
	entryCounts := make([][2]int, cfg.Items)
	for i := 0; i < cfg.Items; i++ {
		id := newItemID(rng)
		item := forestItem{Index: i, ID: id}
		if cfg.Target == "files" {
			item.Dir = itemDir(dataDir, cfg.ShardedParent, id)
			nameDoc, err := bson.Marshal(bson.M{"name": randomPayload(rng, 10, 50)})
			if err != nil {
				return nil, nil, fmt.Errorf("encoding Name.BSON for item %q: %w", id, err)
			}
			item.NameDoc = nameDoc
		}
		items[i] = item
		entryCounts[i] = [2]int{
			cfg.MinEntries + rng.Intn(cfg.MaxEntries-cfg.MinEntries+1),
			cfg.MinEntries + rng.Intn(cfg.MaxEntries-cfg.MinEntries+1),
		}
	}
	return items, entryCounts, nil
}

func itemDir(dataDir string, sharded bool, id string) string {
	if sharded && len(id) >= 4 {
		return filepath.Join(dataDir, "items", id[0:2], id[2:4], id)
	}
	return filepath.Join(dataDir, "items", id)
}

// --- workload schedule -------------------------------------------------

type opKind string

const (
	opPush   opKind = "push"
	opPeek   opKind = "peek"
	opPop    opKind = "pop"
	opDelete opKind = "delete"
)

// liveCounts tracks, per queue, how many entries are currently live -- the
// harness's own bookkeeping, independent of any target, so a delete/pop can
// pick a valid position without asking the target first (which would cost a
// read neither production engine pays before every op).
type liveCounts struct {
	counts []int32
}

func newLiveCounts(n int) *liveCounts { return &liveCounts{counts: make([]int32, n)} }

func (l *liveCounts) add(id int, delta int32) int32 {
	return atomic.AddInt32(&l.counts[id], delta)
}

func (l *liveCounts) get(id int) int32 {
	return atomic.LoadInt32(&l.counts[id])
}

// --- stats -------------------------------------------------------------

type forestStats struct {
	mu                 sync.Mutex
	durationsByOp      map[opKind][]time.Duration
	queueOpCounts      []int32
	failedDeletes      int64
	failedPops         int64
	failedPeeks        int64
	pushCount          int64
	peekCount          int64
	popCount           int64
	deleteCount        int64
	compactionSuspects []compactionSuspect
	// writerProgress counts every writer-side op (ingest push/delete, drain
	// pop, delete-phase delete) but never a reader op. The reader paces
	// itself against this, not a fixed pre-computed budget: a failed peek on
	// an empty queue costs no I/O at all, so a reader given a fixed op
	// budget and no other limit burns through it in microseconds, entirely
	// before the writer -- bound by real fsync cost -- has pushed anything
	// to land on. Pacing against writer progress spreads the reader's ops
	// across the whole run instead of front-loading them into the first
	// instant, and keeps growing through drain and delete, not just ingest.
	writerProgress atomic.Int64
}

// compactionSuspect flags a delete whose latency is far enough above the
// running delete median to be a rewrite, not an ordinary tombstone -- the
// only signal available from outside storage/, which this mode does not
// touch. Recorded per the dispatch's item 7 ("state what the harness cannot
// see"): this is an inference from latency, not a hook into
// SegmentStore.compact, and the report says so.
type compactionSuspect struct {
	QueueID    int     `json:"queue_id"`
	DurationMs float64 `json:"duration_ms"`
	Phase      string  `json:"phase"`
}

func newForestStats(queueCount int) *forestStats {
	return &forestStats{
		durationsByOp: make(map[opKind][]time.Duration),
		queueOpCounts: make([]int32, queueCount),
	}
}

func (s *forestStats) record(kind opKind, ref queueRef, result queueOpResult, phase string) {
	id := queueID(ref)
	atomic.AddInt32(&s.queueOpCounts[id], 1)
	s.mu.Lock()
	s.durationsByOp[kind] = append(s.durationsByOp[kind], result.Duration)
	s.mu.Unlock()

	if phase != "reader" {
		s.writerProgress.Add(1)
	}

	switch kind {
	case opPush:
		atomic.AddInt64(&s.pushCount, 1)
	case opPeek:
		atomic.AddInt64(&s.peekCount, 1)
		if result.Err != nil {
			atomic.AddInt64(&s.failedPeeks, 1)
		}
	case opPop:
		atomic.AddInt64(&s.popCount, 1)
		if result.Err != nil {
			atomic.AddInt64(&s.failedPops, 1)
		}
	case opDelete:
		atomic.AddInt64(&s.deleteCount, 1)
		if result.Err != nil {
			atomic.AddInt64(&s.failedDeletes, 1)
		} else if result.Duration > deleteCompactionSuspectFloor {
			s.mu.Lock()
			s.compactionSuspects = append(s.compactionSuspects, compactionSuspect{
				QueueID: id, DurationMs: millis(result.Duration), Phase: phase,
			})
			s.mu.Unlock()
		}
	}
}

// deleteCompactionSuspectFloor is well above the ~5-20 ms an ordinary
// held-store positional delete costs on this class of host (R5-03, R5-06)
// and well below the multi-second cost R5-03 measured for a firing rewrite,
// so it separates the two without needing a hook into storage/.
const deleteCompactionSuspectFloor = 200 * time.Millisecond

func (s *forestStats) opDistribution() (min, mean, max int32) {
	if len(s.queueOpCounts) == 0 {
		return 0, 0, 0
	}
	min, max = s.queueOpCounts[0], s.queueOpCounts[0]
	var total int64
	for _, c := range s.queueOpCounts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
		total += int64(c)
	}
	return min, int32(total / int64(len(s.queueOpCounts))), max
}

// --- reader goroutine ----------------------------------------------------

// runReader runs from before the ingest phase to after the delete phase,
// issuing peeks (and, at popMixRatio, destructive pops) against a uniformly
// random queue from the whole item set, until doneCh closes -- which is
// what the rev-5 review's discriminating-criteria section requires of
// criterion 4: it must genuinely overlap the writer and stay alive through
// drain and delete, not just ingest.
//
// It paces itself against stats.writerProgress rather than a fixed
// pre-computed op count: a failed peek on an empty queue costs no I/O, so
// an unpaced reader with a fixed budget burns through it in microseconds,
// entirely before the writer -- bound by real fsync cost -- has pushed
// anything for most queues to land on. Capping at
// peekRatio*writerProgress(now) spreads the reader's ops across the whole
// run instead of front-loading them into the first instant.
func runReader(target queueForestTarget, items int, peekRatio float64, shareOfReaders float64, popMixRatio float64, stats *forestStats, doneCh <-chan struct{}, rng *mathrand.Rand) {
	var issued int64
	for {
		select {
		case <-doneCh:
			return
		default:
		}
		allowed := int64(float64(stats.writerProgress.Load()) * peekRatio * shareOfReaders)
		if issued >= allowed {
			select {
			case <-doneCh:
				return
			case <-time.After(time.Millisecond):
				continue
			}
		}
		ref := randomQueueRef(items, rng)
		if rng.Float64() < popMixRatio {
			result := target.Pop(ref)
			stats.record(opPop, ref, result, "reader")
		} else {
			result := target.Peek(ref)
			stats.record(opPeek, ref, result, "reader")
		}
		issued++
	}
}

func randomQueueRef(items int, rng *mathrand.Rand) queueRef {
	kind := queueA
	if rng.Intn(2) == 1 {
		kind = queueB
	}
	return queueRef{ItemIndex: rng.Intn(items), Kind: kind}
}

// --- ingest phase --------------------------------------------------------

// buildIngestSchedule returns the ingest phase as one shuffled sequence of
// pushes and interleaved deletes. Every queue's pushes are still issued in
// its own order (position 0, then 1, then 2, ...) -- Push has no positional
// argument, it always appends -- but which queue gets the next push is
// randomized across the whole set, per the plan's selection rule.
func buildIngestSchedule(entryCounts [][2]int, deleteRatio float64, rng *mathrand.Rand) ([]queueRef, int) {
	total := 0
	for _, counts := range entryCounts {
		total += counts[0] + counts[1]
	}
	schedule := make([]queueRef, 0, total)
	for itemIdx, counts := range entryCounts {
		for k, kind := range [2]queueKind{queueA, queueB} {
			for n := 0; n < counts[k]; n++ {
				schedule = append(schedule, queueRef{ItemIndex: itemIdx, Kind: kind})
			}
		}
	}
	rng.Shuffle(len(schedule), func(i, j int) {
		schedule[i], schedule[j] = schedule[j], schedule[i]
	})
	deleteCount := int(float64(total) * deleteRatio)
	return schedule, deleteCount
}

// phaseRunner bundles what every phase function needs -- target, live-count
// bookkeeping, stats, the RNG and the inode floor -- into one value instead
// of a parameter list that crossed six the moment the inode guard needed
// somewhere to live (AGENTS.md coding standard 7).
type phaseRunner struct {
	target        queueForestTarget
	items         int
	live          *liveCounts
	stats         *forestStats
	rng           *mathrand.Rand
	dataDir       string
	minFreeInodes int64
	minFreeDisk   int64
	guard         *hostGuard
	trippedReason string
}

// guardsOK is every guard that must actually stop a phase mid-run, not just
// report a trip reason after the fact. The host memory guard was previously
// only read at the end of runQueueForest -- guard.trippedReason() -- which
// meant a phase never actually stopped when MemAvailable crossed the floor;
// it just kept running and reported the trip afterward. Free disk bytes had
// no check at all during a phase, only the guard-floor refusal before a run
// starts. It is cheap to call (~5000 times cheaper than a push's fsync) so a
// phase checks it every few thousand operations, not once.
func (p *phaseRunner) guardsOK() bool {
	if p.trippedReason != "" {
		return false
	}
	if p.guard != nil && p.guard.tripped.Load() {
		p.trippedReason = p.guard.trippedReason()
		return false
	}
	freeInodes, _, err := statfsInodes(p.dataDir)
	if err != nil {
		p.trippedReason = fmt.Sprintf("could not read free inodes: %v", err)
		return false
	}
	if freeInodes < p.minFreeInodes {
		p.trippedReason = fmt.Sprintf("free inodes fell to %d, below the %d floor; stopping with the ops already issued", freeInodes, p.minFreeInodes)
		return false
	}
	freeDisk, err := freeDiskBytes(p.dataDir)
	if err != nil {
		p.trippedReason = fmt.Sprintf("could not read free disk bytes: %v", err)
		return false
	}
	if freeDisk < p.minFreeDisk {
		p.trippedReason = fmt.Sprintf("free disk fell to %s, below the %s floor; stopping with the ops already issued", humanBytes(freeDisk), humanBytes(p.minFreeDisk))
		return false
	}
	return true
}

// inodeCheckEvery bounds how often a phase calls statfs to check the inode
// floor -- often enough to stop well short of exhausting a shared host's
// inodes, rare enough that the check's own cost is noise next to a push's
// multi-millisecond fsync.
const inodeCheckEvery = 5000

// runIngestPhase issues the shuffled push schedule, interleaving
// deleteCount deletes at random points, each against a uniformly random
// queue and a uniformly random position within that queue's entries pushed
// so far. R5-02 found this fires compaction well before the delete phase and
// produces failed deletes on queues a prior delete already emptied; both are
// counted, not treated as errors.
func runIngestPhase(p *phaseRunner, schedule []queueRef, deleteCount, entryPayloadMin, entryPayloadMax int) error {
	deletePositions := make(map[int]bool, deleteCount)
	if deleteCount > 0 && len(schedule) > 0 {
		for len(deletePositions) < deleteCount {
			deletePositions[p.rng.Intn(len(schedule)+deleteCount)] = true
		}
	}
	deletesIssued := 0
	scheduleIdx := 0
	totalSteps := len(schedule) + deleteCount
	for step := 0; step < totalSteps; step++ {
		if step%inodeCheckEvery == 0 && !p.guardsOK() {
			return nil
		}
		if deletePositions[step] && deletesIssued < deleteCount {
			ref := randomQueueRef(p.items, p.rng)
			runIngestDelete(p, ref, queueID(ref))
			deletesIssued++
			continue
		}
		if scheduleIdx >= len(schedule) {
			continue
		}
		ref := schedule[scheduleIdx]
		scheduleIdx++
		entry, err := bson.Marshal(forestEntryDoc{ID: newItemID(p.rng), Payload: randomPayload(p.rng, entryPayloadMin, entryPayloadMax)})
		if err != nil {
			return fmt.Errorf("encoding entry: %w", err)
		}
		result := p.target.Push(ref, entry)
		p.stats.record(opPush, ref, result, "ingest")
		if result.Err == nil {
			p.live.add(queueID(ref), 1)
		}
	}
	// Any deletes not yet placed because the random step index landed past
	// the schedule's end (possible when deleteCount is a large fraction of a
	// short schedule) are issued now, so the achieved delete count is never
	// silently short of what -interleaved-delete-ratio asked for.
	for ; deletesIssued < deleteCount; deletesIssued++ {
		if !p.guardsOK() {
			return nil
		}
		ref := randomQueueRef(p.items, p.rng)
		runIngestDelete(p, ref, queueID(ref))
	}
	return nil
}

func runIngestDelete(p *phaseRunner, ref queueRef, id int) {
	count := p.live.get(id)
	if count <= 0 {
		result := p.target.DeleteAtPosition(ref, 0)
		p.stats.record(opDelete, ref, result, "ingest")
		return
	}
	pos := p.rng.Intn(int(count))
	result := p.target.DeleteAtPosition(ref, pos)
	p.stats.record(opDelete, ref, result, "ingest")
	if result.Err == nil {
		p.live.add(id, -1)
	}
}

// --- drain and delete phases ---------------------------------------------

// selectItemSubset picks fraction*items distinct item indices at random.
// The drain and delete phases must use disjoint subsets ("a separate random
// 10% of items") so a queue's deletes accumulate toward the 25% compaction
// trigger instead of being split across two phases that dilute it.
func selectItemSubset(items int, fraction float64, exclude map[int]bool, rng *mathrand.Rand) []int {
	want := int(float64(items) * fraction)
	chosen := make([]int, 0, want)
	seen := make(map[int]bool, want)
	for len(chosen) < want && len(seen)+len(exclude) < items {
		idx := rng.Intn(items)
		if seen[idx] || exclude[idx] {
			continue
		}
		seen[idx] = true
		chosen = append(chosen, idx)
	}
	return chosen
}

// runDrainPhase pops drainPopRatio of each selected item's queues' live
// entries, drawing the pop *count* per queue up front (so the phase can
// interleave randomly across the whole selected subset, per the selection
// rule) and then executing one pop per step against a uniformly chosen
// still-remaining queue.
func runDrainPhase(p *phaseRunner, itemIdxs []int, popRatio float64) {
	type queueBudget struct {
		ref       queueRef
		remaining int
	}
	var budgets []queueBudget
	for _, itemIdx := range itemIdxs {
		for _, kind := range [2]queueKind{queueA, queueB} {
			ref := queueRef{ItemIndex: itemIdx, Kind: kind}
			count := int(p.live.get(queueID(ref)))
			toPop := int(float64(count) * popRatio)
			if toPop > 0 {
				budgets = append(budgets, queueBudget{ref: ref, remaining: toPop})
			}
		}
	}
	for step := 0; len(budgets) > 0; step++ {
		if step%inodeCheckEvery == 0 && !p.guardsOK() {
			return
		}
		i := p.rng.Intn(len(budgets))
		ref := budgets[i].ref
		result := p.target.Pop(ref)
		p.stats.record(opPop, ref, result, "drain")
		if result.Err == nil {
			p.live.add(queueID(ref), -1)
		}
		budgets[i].remaining--
		if budgets[i].remaining <= 0 {
			budgets[i] = budgets[len(budgets)-1]
			budgets = budgets[:len(budgets)-1]
		}
	}
}

// runDeletePhase removes deleteEntryRatio of each selected item's queues'
// live entries via DeleteAtPosition. For fifoHeadFraction of the fifo
// queues in this subset, every delete targets position 0 -- the oldest
// end -- which is the head-tombstone case the plan's choice 3 requires
// (peekFIFO's per-entry open only shows its cost when tombstones accumulate at the
// head, not when they are scattered through the middle).
func runDeletePhase(p *phaseRunner, itemIdxs []int, deleteRatio, fifoHeadFraction float64) {
	type queueBudget struct {
		ref       queueRef
		fromHead  bool
		remaining int
	}
	var budgets []queueBudget
	for _, itemIdx := range itemIdxs {
		for _, kind := range [2]queueKind{queueA, queueB} {
			ref := queueRef{ItemIndex: itemIdx, Kind: kind}
			count := int(p.live.get(queueID(ref)))
			toDelete := int(float64(count) * deleteRatio)
			if toDelete <= 0 {
				continue
			}
			fromHead := kind == queueA && p.rng.Float64() < fifoHeadFraction
			budgets = append(budgets, queueBudget{ref: ref, fromHead: fromHead, remaining: toDelete})
		}
	}
	for step := 0; len(budgets) > 0; step++ {
		if step%inodeCheckEvery == 0 && !p.guardsOK() {
			return
		}
		i := p.rng.Intn(len(budgets))
		ref := budgets[i].ref
		count := int(p.live.get(queueID(ref)))
		var pos int
		if budgets[i].fromHead || count <= 1 {
			pos = 0
		} else {
			pos = p.rng.Intn(count)
		}
		result := p.target.DeleteAtPosition(ref, pos)
		p.stats.record(opDelete, ref, result, "delete")
		if result.Err == nil {
			p.live.add(queueID(ref), -1)
		}
		budgets[i].remaining--
		if budgets[i].remaining <= 0 {
			budgets[i] = budgets[len(budgets)-1]
			budgets = budgets[:len(budgets)-1]
		}
	}
}

// --- report ----------------------------------------------------------------

type opTypeReport struct {
	Count        int64       `json:"count"`
	Failed       int64       `json:"failed"`
	Latency      statSummary `json:"latency"`
	P99Millis    float64     `json:"p99_ms"`
	OpsPerSecond float64     `json:"ops_per_second"`
}

type queueForestReport struct {
	Host   hostFacts         `json:"host"`
	Floors guardFloors       `json:"guard_floors"`
	Config queueForestConfig `json:"config"`
	Target string            `json:"target_name"`

	Items      int `json:"items"`
	QueueCount int `json:"queue_count"`

	PreflightMillis preflightResult `json:"preflight"`
	ProbeOnly       bool            `json:"probe_only"`

	OpsByType map[opKind]opTypeReport `json:"ops_by_type"`

	AchievedPeekRatio   float64 `json:"achieved_peek_ratio"`
	AchievedPopMixRatio float64 `json:"achieved_pop_mix_ratio"`

	QueueOpDistribution struct {
		Min  int32 `json:"min"`
		Mean int32 `json:"mean"`
		Max  int32 `json:"max"`
	} `json:"queue_op_distribution"`

	FailedDeletes int64 `json:"failed_deletes"`
	FailedPops    int64 `json:"failed_pops"`

	CompactionSuspects []compactionSuspect `json:"compaction_suspects"`

	FDHighWater        int64 `json:"fd_high_water"`
	MappingHighWater   int64 `json:"mapping_high_water"`
	RSSHighWaterBytes  int64 `json:"rss_high_water_bytes"`
	GoroutineHighWater int64 `json:"goroutine_high_water"`

	DeviceReadBytesIngest  int64 `json:"device_read_bytes_ingest"`
	DeviceWriteBytesIngest int64 `json:"device_write_bytes_ingest"`
	DeviceReadBytesDrain   int64 `json:"device_read_bytes_drain"`
	DeviceWriteBytesDrain  int64 `json:"device_write_bytes_drain"`
	DeviceReadBytesDelete  int64 `json:"device_read_bytes_delete"`
	DeviceWriteBytesDelete int64 `json:"device_write_bytes_delete"`

	InodesFreeBefore    int64 `json:"inodes_free_before"`
	InodesFreeAfter     int64 `json:"inodes_free_after"`
	DiskFreeBeforeBytes int64 `json:"disk_free_before_bytes"`
	DiskFreeAfterBytes  int64 `json:"disk_free_after_bytes"`

	Census      censusResult  `json:"census"`
	WalkDirCost walkDirResult `json:"walkdir_directory_enumeration"`

	WallSeconds  float64  `json:"wall_seconds"`
	HostLoadNote string   `json:"host_load_note"`
	CutShort     string   `json:"cut_short,omitempty"`
	Notes        []string `json:"notes"`
}

type preflightResult struct {
	PushMs100             float64 `json:"push_ms_per_100"`
	OpenPeekCloseMs100    float64 `json:"open_peek_close_ms_per_100"`
	PopMsShallow          float64 `json:"pop_ms_shallow_depth"`
	PopMsDeep             float64 `json:"pop_ms_deep_depth"`
	ShallowDepth          int     `json:"shallow_depth"`
	DeepDepth             int     `json:"deep_depth"`
	ProjectedTotalSeconds float64 `json:"projected_total_seconds"`
}

type walkDirResult struct {
	Applicable  bool    `json:"applicable"`
	Entries     int     `json:"entries_visited"`
	TotalMillis float64 `json:"total_ms"`
}

func buildOpTypeReport(durations []time.Duration, failed int64) opTypeReport {
	summary := durationStats(durations)
	rep := opTypeReport{
		Count:   int64(len(durations)),
		Failed:  failed,
		Latency: summary,
	}
	if len(durations) > 0 {
		sorted := append([]time.Duration(nil), durations...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		rep.P99Millis = millis(nearestRank(sorted, 0.99))
	}
	if summary.MeanMillis > 0 {
		rep.OpsPerSecond = 1000 / summary.MeanMillis
	}
	return rep
}

// --- inode / disk helpers ---------------------------------------------

// readLoadAverage reports /proc/loadavg verbatim so the report can say
// plainly whether the host was quiet, per the dispatch's guard: "if the host
// was not quiet, say so plainly."
func readLoadAverage() (string, error) {
	raw, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "", fmt.Errorf("reading /proc/loadavg: %w", err)
	}
	return string(raw), nil
}

func statfsInodes(path string) (free, total int64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, fmt.Errorf("statfs %q for inode counts: %w", path, err)
	}
	return int64(stat.Ffree), int64(stat.Files), nil
}

// runWalkDirCensus times a filepath.WalkDir over the item tree: raw
// directory-entry enumeration throughput, a different bottleneck from
// LoadCatalog's manifest-read-plus-flat-opens (F-04). It must be reported
// under this name, never as a census -- see the plan's Q1 disposition and
// the rev-2 review's F-04.
func runWalkDirCensus(root string) (walkDirResult, error) {
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return walkDirResult{}, nil
	}
	start := time.Now()
	entries := 0
	err := filepath.WalkDir(root, func(_ string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		return nil
	})
	if err != nil {
		return walkDirResult{}, fmt.Errorf("walking %q for the directory-enumeration figure: %w", root, err)
	}
	return walkDirResult{Applicable: true, Entries: entries, TotalMillis: millis(time.Since(start))}, nil
}

// --- high water sampler --------------------------------------------------

type highWaterSampler struct {
	stop          chan struct{}
	wg            sync.WaitGroup
	maxFDs        atomic.Int64
	maxMappings   atomic.Int64
	maxRSS        atomic.Int64
	maxGoroutines atomic.Int64
}

func startHighWaterSampler(interval time.Duration) *highWaterSampler {
	s := &highWaterSampler{stop: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
			}
			reading, err := readSelf()
			if err != nil {
				continue
			}
			bumpMax(&s.maxFDs, int64(reading.Process.FileDescriptors))
			bumpMax(&s.maxMappings, int64(reading.Process.Mappings))
			bumpMax(&s.maxRSS, reading.Process.RSSBytes)
			bumpMax(&s.maxGoroutines, int64(reading.Goroutines))
		}
	}()
	return s
}

func bumpMax(target *atomic.Int64, candidate int64) {
	for {
		current := target.Load()
		if candidate <= current {
			return
		}
		if target.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func (s *highWaterSampler) stopAndReport() (fds, mappings, rss, goroutines int64) {
	close(s.stop)
	s.wg.Wait()
	return s.maxFDs.Load(), s.maxMappings.Load(), s.maxRSS.Load(), s.maxGoroutines.Load()
}

// --- entry point -----------------------------------------------------------

func runQueueForest(cfg runConfig, qfc queueForestConfig, outPath string) error {
	if err := refuseTmpfs(cfg.DataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("creating the queueforest data directory %q: %w", cfg.DataDir, err)
	}
	if qfc.FDRlimit > 0 {
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: rlimitFrom(qfc.FDRlimit), Max: rlimitFrom(qfc.FDRlimit)}); err != nil {
			return fmt.Errorf("setting RLIMIT_NOFILE to %d: %w", qfc.FDRlimit, err)
		}
		fmt.Printf("RLIMIT_NOFILE lowered to %d in-process, to reach the descriptor wall deliberately (F-03)\n", qfc.FDRlimit)
	}

	facts, err := collectHostFacts(cfg.DataDir)
	if err != nil {
		return err
	}
	printHostFacts(facts)

	report := queueForestReport{
		Host: facts, Floors: cfg.Floors, Config: qfc,
		Items: qfc.Items, ProbeOnly: qfc.ProbeOnly,
		OpsByType: make(map[opKind]opTypeReport),
	}
	report.InodesFreeBefore, _, err = statfsInodes(cfg.DataDir)
	if err != nil {
		return err
	}
	report.DiskFreeBeforeBytes, err = freeDiskBytes(cfg.DataDir)
	if err != nil {
		return err
	}
	if loadAvg, loadErr := readLoadAverage(); loadErr == nil {
		report.HostLoadNote = loadAvg
	}
	report.Notes = append(report.Notes, fmt.Sprintf("entry representation: %s", qfc.EntryRepresentation))
	if qfc.NonDefaultSeg {
		report.Notes = append(report.Notes, fmt.Sprintf("segment size %d bytes is a non-default configuration; the server passes 64 MiB (server/service.go:370)", qfc.SegmentSize))
	}

	rng := mathrand.New(mathrand.NewSource(qfc.Seed))
	items, entryCounts, err := buildItems(qfc, cfg.DataDir, rng)
	if err != nil {
		return err
	}
	report.QueueCount = len(items) * 2

	target, err := buildTarget(qfc, cfg.DataDir)
	if err != nil {
		return err
	}
	report.Target = target.Name()

	guard := startHostGuard(cfg.Floors.AvailMemoryBytes)
	defer guard.stop()
	sampler := startHighWaterSampler(50 * time.Millisecond)

	wallStart := time.Now()

	if err := target.Prepare(items); err != nil {
		_ = target.Close()
		return fmt.Errorf("preparing the %s target: %w", target.Name(), err)
	}

	if qfc.ProbeOnly {
		fmt.Printf("\n=== queueforest probe: %d items held open under -open=%s, no workload ===\n", qfc.Items, qfc.OpenMode)
		report.WallSeconds = time.Since(wallStart).Seconds()
		return finishQueueForestRun(&report, target, items, cfg.DataDir, sampler, cfg, guard, outPath)
	}

	stats := newForestStats(len(items) * 2)
	live := newLiveCounts(len(items) * 2)
	phase := &phaseRunner{
		target: target, items: len(items), live: live, stats: stats, rng: rng,
		dataDir: cfg.DataDir, minFreeInodes: qfc.MinFreeInodes,
		minFreeDisk: cfg.Floors.FreeDiskBytes, guard: guard,
	}

	totalPushes := 0
	for _, c := range entryCounts {
		totalPushes += c[0] + c[1]
	}

	doneCh := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(qfc.ReaderGoroutines)
	shareOfReaders := 1.0
	if qfc.ReaderGoroutines > 0 {
		shareOfReaders = 1.0 / float64(qfc.ReaderGoroutines)
	}
	for g := 0; g < qfc.ReaderGoroutines; g++ {
		readerRNG := mathrand.New(mathrand.NewSource(rng.Int63()))
		go func() {
			defer readerWG.Done()
			runReader(target, len(items), qfc.PeekRatio, shareOfReaders, qfc.PopMixRatio, stats, doneCh, readerRNG)
		}()
	}

	fmt.Printf("\n=== queueforest ingest: %d items, %d queues, ~%d pushes ===\n", qfc.Items, len(items)*2, totalPushes)
	before, err := readSelf()
	if err != nil {
		close(doneCh)
		readerWG.Wait()
		return err
	}
	schedule, deleteCount := buildIngestSchedule(entryCounts, qfc.InterleavedDeleteRatio, rng)
	if err := runIngestPhase(phase, schedule, deleteCount, 10, 100); err != nil {
		close(doneCh)
		readerWG.Wait()
		return err
	}
	afterIngest, err := readSelf()
	if err != nil {
		close(doneCh)
		readerWG.Wait()
		return err
	}
	report.DeviceReadBytesIngest = afterIngest.IO.ReadBytes - before.IO.ReadBytes
	report.DeviceWriteBytesIngest = afterIngest.IO.WriteBytes - before.IO.WriteBytes

	fmt.Printf("=== queueforest drain: %.0f%% of items, %.0f%% of their live entries popped ===\n", qfc.DrainItemRatio*100, qfc.DrainPopRatio*100)
	drainSubset := selectItemSubset(len(items), qfc.DrainItemRatio, nil, rng)
	runDrainPhase(phase, drainSubset, qfc.DrainPopRatio)
	afterDrain, err := readSelf()
	if err != nil {
		close(doneCh)
		readerWG.Wait()
		return err
	}
	report.DeviceReadBytesDrain = afterDrain.IO.ReadBytes - afterIngest.IO.ReadBytes
	report.DeviceWriteBytesDrain = afterDrain.IO.WriteBytes - afterIngest.IO.WriteBytes

	fmt.Printf("=== queueforest delete: %.0f%% of a separate set of items, %.0f%% of their live entries deleted ===\n", qfc.DeleteItemRatio*100, qfc.DeleteEntryRatio*100)
	excludeSet := make(map[int]bool, len(drainSubset))
	for _, idx := range drainSubset {
		excludeSet[idx] = true
	}
	deleteSubset := selectItemSubset(len(items), qfc.DeleteItemRatio, excludeSet, rng)
	runDeletePhase(phase, deleteSubset, qfc.DeleteEntryRatio, qfc.DeleteFromHeadFraction)
	afterDelete, err := readSelf()
	if err != nil {
		close(doneCh)
		readerWG.Wait()
		return err
	}
	report.DeviceReadBytesDelete = afterDelete.IO.ReadBytes - afterDrain.IO.ReadBytes
	report.DeviceWriteBytesDelete = afterDelete.IO.WriteBytes - afterDrain.IO.WriteBytes

	close(doneCh)
	readerWG.Wait()

	report.WallSeconds = time.Since(wallStart).Seconds()
	report.CutShort = guard.trippedReason()
	if report.CutShort == "" {
		report.CutShort = phase.trippedReason
	}

	populateOpStats(&report, stats)

	return finishQueueForestRun(&report, target, items, cfg.DataDir, sampler, cfg, guard, outPath)
}

func populateOpStats(report *queueForestReport, stats *forestStats) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	report.OpsByType[opPush] = buildOpTypeReport(stats.durationsByOp[opPush], 0)
	report.OpsByType[opPeek] = buildOpTypeReport(stats.durationsByOp[opPeek], stats.failedPeeks)
	report.OpsByType[opPop] = buildOpTypeReport(stats.durationsByOp[opPop], stats.failedPops)
	report.OpsByType[opDelete] = buildOpTypeReport(stats.durationsByOp[opDelete], stats.failedDeletes)

	if stats.pushCount > 0 {
		report.AchievedPeekRatio = float64(stats.peekCount) / float64(stats.pushCount)
	}
	if stats.peekCount+stats.popCount > 0 {
		report.AchievedPopMixRatio = float64(stats.popCount) / float64(stats.peekCount+stats.popCount)
	}
	report.FailedDeletes = stats.failedDeletes
	report.FailedPops = stats.failedPops
	report.CompactionSuspects = stats.compactionSuspects

	min, mean, max := stats.opDistribution()
	report.QueueOpDistribution.Min = min
	report.QueueOpDistribution.Mean = mean
	report.QueueOpDistribution.Max = max
}

// finishQueueForestRun closes the target (so a held/lru files target's
// Census below measures a real reopen, not a no-op on an already-open
// store), runs the F-04 census and the separately-labelled WalkDir
// directory-enumeration figure, takes the final fd/RSS high-water and inode
// readings, prints and writes the report.
func finishQueueForestRun(report *queueForestReport, target queueForestTarget, items []forestItem, dataDir string, sampler *highWaterSampler, cfg runConfig, guard *hostGuard, outPath string) error {
	if err := target.Close(); err != nil {
		return fmt.Errorf("closing the %s target: %w", target.Name(), err)
	}

	census, err := target.Census(items)
	if err != nil {
		return fmt.Errorf("running the census (F-04 stand-in) on the %s target: %w", target.Name(), err)
	}
	report.Census = census

	if report.Config.Target == "files" {
		walkResult, err := runWalkDirCensus(filepath.Join(dataDir, "items"))
		if err != nil {
			return err
		}
		report.WalkDirCost = walkResult
	}

	fds, mappings, rss, goroutines := sampler.stopAndReport()
	report.FDHighWater = fds
	report.MappingHighWater = mappings
	report.RSSHighWaterBytes = rss
	report.GoroutineHighWater = goroutines

	inodesFree, _, err := statfsInodes(cfg.DataDir)
	if err != nil {
		return err
	}
	report.InodesFreeAfter = inodesFree
	diskFree, err := freeDiskBytes(cfg.DataDir)
	if err != nil {
		return err
	}
	report.DiskFreeAfterBytes = diskFree

	if report.CutShort == "" {
		report.CutShort = guard.trippedReason()
	}

	printQueueForestReport(*report)

	if outPath != "" {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding the queueforest report: %w", err)
		}
		if err := os.WriteFile(outPath, encoded, 0o644); err != nil {
			return fmt.Errorf("writing the queueforest report to %q: %w", outPath, err)
		}
		fmt.Printf("\nJSON report: %s\n", outPath)
	}
	return nil
}

func buildTarget(qfc queueForestConfig, dataDir string) (queueForestTarget, error) {
	switch qfc.Target {
	case "files":
		return newFilesTarget(qfc.SegmentSize, qfc.OpenMode, qfc.LRUCapacity), nil
	case "badger":
		return newBadgerTarget(dataDir, qfc.BadgerSynced)
	case "mongo":
		if qfc.MongoURI == "" {
			return nil, errors.New("-target=mongo needs -mongo-uri or $HOARDDB_BENCH_MONGO_URI")
		}
		return newMongoTarget(context.Background(), qfc.MongoURI)
	case "mysql":
		if qfc.MySQLDSN == "" {
			return nil, errors.New("-target=mysql needs -mysql-dsn or $HOARDDB_BENCH_MYSQL_DSN")
		}
		return newMySQLTarget(qfc.MySQLDSN)
	default:
		return nil, fmt.Errorf("-target=%q is none of \"files\", \"badger\", \"mongo\", \"mysql\"", qfc.Target)
	}
}

func printQueueForestReport(report queueForestReport) {
	fmt.Printf("\n=== queueforest report: target=%s items=%d queues=%d ===\n", report.Target, report.Items, report.QueueCount)
	fmt.Printf("wall: %.1fs   cut short: %s   loadavg: %s", report.WallSeconds, orNone(report.CutShort), report.HostLoadNote)
	for _, kind := range []opKind{opPush, opPeek, opPop, opDelete} {
		op := report.OpsByType[kind]
		fmt.Printf("  %-6s n=%9d failed=%6d  p50=%9.3fms p99=%9.3fms max=%10.3fms  %10.1f ops/s\n",
			kind, op.Count, op.Failed, op.Latency.P50Millis, op.P99Millis, op.Latency.MaxMillis, op.OpsPerSecond)
	}
	fmt.Printf("  achieved peek ratio: %.3fx   achieved pop-mix ratio: %.4f\n", report.AchievedPeekRatio, report.AchievedPopMixRatio)
	fmt.Printf("  per-queue ops: min=%d mean=%d max=%d\n", report.QueueOpDistribution.Min, report.QueueOpDistribution.Mean, report.QueueOpDistribution.Max)
	fmt.Printf("  fd high water=%d  mappings high water=%d  rss high water=%s  goroutines high water=%d\n",
		report.FDHighWater, report.MappingHighWater, humanBytes(report.RSSHighWaterBytes), report.GoroutineHighWater)
	fmt.Printf("  inodes free: %d -> %d (delta %d)\n", report.InodesFreeBefore, report.InodesFreeAfter, report.InodesFreeBefore-report.InodesFreeAfter)
	fmt.Printf("  disk free: %s -> %s\n", humanBytes(report.DiskFreeBeforeBytes), humanBytes(report.DiskFreeAfterBytes))
	fmt.Printf("  device read/write  ingest=%s/%s drain=%s/%s delete=%s/%s\n",
		humanBytes(report.DeviceReadBytesIngest), humanBytes(report.DeviceWriteBytesIngest),
		humanBytes(report.DeviceReadBytesDrain), humanBytes(report.DeviceWriteBytesDrain),
		humanBytes(report.DeviceReadBytesDelete), humanBytes(report.DeviceWriteBytesDelete))
	if report.Census.Applicable {
		fmt.Printf("  census (F-04 stand-in): %d opens, %.1fms total, %.4fms/item\n", report.Census.TotalOpens, report.Census.TotalMillis, report.Census.MillisPerItem)
	} else if report.Census.Mechanism != "" {
		fmt.Printf("  census: %s\n", report.Census.Mechanism)
	}
	if report.WalkDirCost.Applicable {
		fmt.Printf("  directory-enumeration (not a census): %d entries, %.1fms total\n", report.WalkDirCost.Entries, report.WalkDirCost.TotalMillis)
	}
	fmt.Printf("  compaction-latency suspects (delete ops over %s): %d\n", deleteCompactionSuspectFloor, len(report.CompactionSuspects))
}

func orNone(s string) string {
	if s == "" {
		return "no"
	}
	return s
}
