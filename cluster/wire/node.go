package wire

import (
	"bytes"
	"cmp"
	"context"
	entropy "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// NodeConfig opens one node. Only Dir is required.
type NodeConfig struct {
	// Dir holds everything the node keeps: <Dir>/raft, <Dir>/badger and <Dir>/wal.
	Dir string
	// NodeID is the node's name in the ring; empty takes "n1".
	NodeID string
	// DedupeWindow is replication.dedupe_window in applied records; 0 takes 8192.
	DedupeWindow uint64
	// GuardObserved, when set, is called once per guarded update or delete the node orders,
	// with true when the guard held and false when it refused; the server counts them.
	GuardObserved func(applied bool)
	// DrainObserved, when set, is called once for each time an owner write waited for a pair's
	// pending list to drain, with how long it waited; the server counts them. It names no document
	// or bucket and must not block.
	DrainObserved func(waited time.Duration)
	// ReplWindow is replication.repl_window: the frames a copy may hold unacknowledged
	// before the owner stops shipping to it; 0 takes 64.
	ReplWindow int
	// MemTableMB sizes Badger's memtable; 0 takes 64.
	MemTableMB int
	// BlockCacheMB and IndexCacheMB size Badger's block and index caches
	// (storage.badger_block_cache_mb, storage.badger_index_cache_mb); 0 takes the store's
	// defaults of 128 and 64.
	BlockCacheMB, IndexCacheMB int
	// TableSync is how Badger makes a new table file durable: storage.TableSyncMsync or
	// storage.TableSyncStepwise (storage.badger_table_sync). Empty takes msync, the safe
	// default; a benchmark sets it explicitly and records it.
	TableSync string
	// WALPreallocate fills each log segment with zeros before the log uses it, so a
	// flush writes data only (storage.wal_preallocate, resolved by the server). False
	// leaves segments to grow, as every earlier build did.
	WALPreallocate bool
	// AtRest, when set, hands the node the keys its store and its log are sealed under; nil runs
	// both in plaintext (ring encryption at rest spec §8).
	AtRest AtRestKeys
	// StorageNames are the database names the host's storage directories take; the node
	// refuses to create a database with one (namepolicy.CheckDatabase). Empty reserves none.
	StorageNames []namepolicy.StorageDir
	// Now reads the wall clock for the incarnation; nil takes time.Now. It is a seam for tests
	// that step the clock back.
	Now func() time.Time
	// ClientAddr is where drivers connect to this node (the committed member row's
	// client_addr); empty means drivers use the internode address.
	ClientAddr string
	// Cluster, when set, makes the node a member of a networked cluster: the real peer
	// transport replaces NoPeers and NoCopies. Nil is the one-voter node of every earlier
	// build, unchanged.
	Cluster *ClusterOptions
	// Advertise is the address peers are told to dial: the data address committed in this
	// node's member row. Empty takes the address the peer listener is bound to. A node that
	// binds a wildcard address must set it, or it would commit 0.0.0.0. Ignored without Cluster.
	Advertise string
	// Voters is cluster.voters, the most voting members the cluster runs (3 or 5, witnesses
	// included); a promotion that would pass it is refused. 0 sets no cap, which a test that
	// builds a larger group relies on; the server always sets it.
	Voters int
	// Witness registers this node as a witness when it joins: it votes and stores no data.
	// A bootstrapping node writes its own member row without the flag (SPEC-GAP C2-1); the
	// server refuses a witness that would bootstrap, the node itself does not check.
	Witness bool
	// ReclaimKeysPerSecond is storage.reclaim_keys_per_second: the node-wide budget of the
	// background sweeps of retired fields and dropped buckets. 0 takes 10000.
	ReclaimKeysPerSecond int
	// RetentionCheck is replication.retention_check_seconds: the period of the node's one
	// retention ticker, which drives the dedupe sweep on the reclaim loop. 0 takes a minute.
	RetentionCheck time.Duration
	// NearestMaxElements is query.nearest_max_elements: the most elements one nearest call scans
	// in one store field; 0 takes 100000.
	NearestMaxElements int
}

// Field declares one field of a bucket: a name and a type word (string, int, fifoStore, ...).
type Field struct {
	Name string
	Type string
	// Of is "<Bucket>" or "<Database>.<Bucket>" when the field is declared of another
	// bucket's ids; empty otherwise.
	Of string
}

// Node is one voter running the entry-ring path in this process: the committed cluster
// state in Raft, the retained log, the shared Badger and the owner write and read paths
// over them. It is a one-voter cluster, so nothing here waits on a peer; everything
// else is the code a multi-node cluster runs.
type Node struct {
	// Aggregates counts aggregate reads and refusals, node-wide.
	Aggregates AggregateStats

	// getManyStats are the RING_GET_MANY counters; atomics, read by tests.
	getManyStats GetManyStats

	// Incs counts inc calls by outcome, node-wide (hoarddb_store_inc_total{outcome}).
	Incs IncStats

	// expandStats are the fetch-through counters; atomics, read by tests and the metrics exposition.
	expandStats ExpandStats
	cfg         NodeConfig
	raft        *cluster.Raft
	kv          *storage.BadgerKV
	wal         *storage.WAL
	pairs       *cluster.PairSet
	bumps       *cluster.BumpApplier
	topology    *ClusterTopology
	handoff     *Handoff
	fetcher     *Fetcher
	schema      *CacheSchema
	writer      *Writer
	reader      *Reader
	scanner     *SlotScanner
	creator     *CreateBucket
	alterer     *AlterBucket
	dropper     *DropBucket
	coord       *Coordinator
	own         *PairOwnership
	// restarted is true when a previous process of this node had joined a cluster (BUG-0743).
	restarted bool
	tail      *TailScanner
	// recovering shares one recoverPair among the writers of a pair that all got the same failure.
	recovering pairFlights
	// live is how describe_node learns the leader and the lease; the real one reads the
	// Raft group and the served lease, a test may replace it.
	live  nodeLiveness
	lease ServeChecker
	// cl holds what a networked node has beyond a one-voter one; nil for a one-voter node.
	cl *clusterParts
	// incarnation is this process's incarnation, committed with its membership.
	incarnation int64
	// writes forwards a write this node may not order; nil for a one-voter node.
	writes WriteForwarder

	// bucketIDs remembers where recent bucket ids were found; see bucketIDCache.
	bucketIDs *bucketIDCache
	// seeding is the operator's view of the seeding engine; it reads as NoSeeding until set.
	seeding seedingHolder
	// retire is the node's one background sweep of retired fields and dropped buckets, and
	// of dead dedupe entries on each retention tick.
	retire *RetireSweep
	dedupe *DedupeSweeper
	// expiry is the node's expiry pass, run on the retention tick (expiry spec 3.3).
	expiry *ExpirySweeper
	// floors are the log floors that catch-ups and seeds hold, which TruncateLogBelow honours.
	floors floorRegistry
	// buffers is the node's whole live-write buffer budget: the replicator offers a frame a
	// seeding pair cannot apply, and the puller drains it after the snapshot. One object for
	// every pull in flight; nil on a one-voter node, which starts no seed.
	buffers *seedBuffers
	// reclaim is the node's one reclaim budget (storage.reclaim_keys_per_second), charged by
	// the sweeps and by the range reset of a reseed.
	reclaim *reclaimBudget
	// retention is the node's one retention ticker (replication.retention_check_seconds):
	// it wakes the reclaim loop for the dedupe sweep; slice B hangs the log retention pass
	// on the same tick.
	retention *time.Ticker
	// bootReplay finishes the boot replay of the pairs that waited for a declaration (BUG-0760):
	// bootReplayStop ends it and bootReplayDone closes when it has. Both are nil when no pair waited.
	bootReplayStop context.CancelFunc
	bootReplayDone chan struct{}
	// stopCtx ends when Close starts, so a declaration read of the replay does not outlast it.
	stopCtx   context.Context
	stopReads context.CancelFunc
	closeOnce sync.Once
	closeErr  error

	// floor is the log-floor tracker the advancers register first writes in and the store's
	// syncs sample (store-vouched-cut-spec.md §3.6). Built in open before wire.
	floor *logFloor
}

const (
	defaultDedupeWindow = 8192
	defaultMemTableMB   = 64
	bootTimeout         = 30 * time.Second
	maxResultBytes      = 1024
)

// OpenNode starts a node on cfg.Dir: a fresh directory bootstraps a new one-voter
// cluster, an existing one is reopened and its log tail replayed. The Raft channel
// listens on a loopback port the kernel picks.
func OpenNode(ctx context.Context, cfg NodeConfig) (*Node, error) {
	if cfg.Dir == "" {
		return nil, errors.New("opening a node needs a directory: set NodeConfig.Dir to where the node keeps its data")
	}
	if cfg.NodeID == "" {
		cfg.NodeID = "n1"
	}
	if len(cfg.ClientAddr) > cluster.MaxClientAddrBytes {
		return nil, fmt.Errorf("the client address is %d bytes and the cluster keeps at most %d (a host name of at most %d characters, a colon and the port): set tcp.advertise to a shorter host:port", len(cfg.ClientAddr), cluster.MaxClientAddrBytes, cluster.MaxClientAddrBytes-6)
	}
	if cfg.DedupeWindow == 0 {
		cfg.DedupeWindow = defaultDedupeWindow
	}
	if cfg.MemTableMB == 0 {
		cfg.MemTableMB = defaultMemTableMB
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RetentionCheck <= 0 {
		cfg.RetentionCheck = defaultRetentionCheck
	}
	// The marker is read before anything touches the directory: an older format is refused
	// with nothing read, changed or deleted, and a fresh directory gets the marker as its
	// first durable write (storage-format spec §5.1).
	if err := ensureRingFormat(cfg.Dir); err != nil {
		var refusal *RingFormatRefusal
		if errors.As(err, &refusal) {
			refusal.Member = cfg.Cluster != nil
		}
		return nil, fmt.Errorf("refusing the ring directory: %w", err)
	}
	// Before nextIncarnation, which rewrites <Dir>/incarnation, and before Badger and the log
	// open: a directory at a cluster version this build cannot run is refused with nothing in
	// it written (cluster-version spec 4.4, check 1).
	if err := cluster.CheckDataDirClusterVersion(cfg.Dir, declaredRange(cfg.Cluster)); err != nil {
		return nil, fmt.Errorf("refusing the data directory: %w", err)
	}
	stopCtx, stopReads := context.WithCancel(context.Background())
	node := &Node{stopCtx: stopCtx, stopReads: stopReads, cfg: cfg, pairs: &cluster.PairSet{}, bumps: &cluster.BumpApplier{}, bucketIDs: newBucketIDCache(bucketIDCacheSize), retire: NewRetireSweep(), reclaim: newReclaimBudget(reclaimBudgetConfig{KeysPerSecond: cfg.ReclaimKeysPerSecond})}
	incarnation, err := nextIncarnation(cfg)
	if err != nil {
		return nil, err
	}
	node.incarnation = incarnation
	// The storage refusals about the store's and the log's directories end in this node's advice,
	// never in an emptied directory (review R3-2); Close clears them.
	dirAdvice := ringDirAdvice(cfg.Dir, cfg.Cluster != nil)
	storage.SetDirectoryAdvice(filepath.Join(cfg.Dir, "badger"), dirAdvice)
	storage.SetDirectoryAdvice(filepath.Join(cfg.Dir, "wal"), dirAdvice)
	if err := node.open(ctx); err != nil {
		return nil, errors.Join(err, node.Close())
	}
	return node, nil
}

// beforeTailScan, when set, runs right before the boot replay, after the records are loaded:
// the seam a test uses to see what committed state the replay runs against. Nil in production.
var beforeTailScan func(*Node)

// raftApplyGate, when set, is handed to the Raft as RaftConfig.ApplyGate: the seam a test
// uses to hold the FSM so a boot runs against an unapplied state. Nil in production.
var raftApplyGate func(kind byte)

// heldApplies is HoldAppliesForTest's gate; nil in production.
var heldApplies func(node string, kind byte)

// HoldAppliesForTest makes every node opened from now on in this process call gate with its id and
// the command kind before its Raft FSM applies each command (nil removes it for later opens; a node
// keeps the gate it opened with). A gate that blocks holds that node's committed state where it is,
// so a test can make a node's seeding driver look at an older state (BUG-0877: a replaced node
// replaying the cluster's log). Public because the harness lives in another package.
func HoldAppliesForTest(gate func(node string, kind byte)) {
	heldApplies = gate
}

func (n *Node) open(ctx context.Context) error {
	var err error
	if beforeStoreOpen != nil {
		beforeStoreOpen(n.cfg.Dir)
	}
	// The ring store opens with the vouched cut: it never holds a later commit without an earlier
	// one, and the boot scan below re-applies what the cut discarded (BUG-0827).
	// A store first start is refused when the node has run before (refuseEmptyStoreAfterEarlierLife).
	// The store key is asked for inside the open, after its refusals and before the marker or
	// Badger writes a file, so no key is created beside a store the open refused (ring encryption
	// at rest spec §5.3; review R of the cut, R-4 item 5).
	storeDir := filepath.Join(n.cfg.Dir, "badger")
	if n.kv, err = storage.OpenVouchedBadgerKVChecked(storeDir, storage.BadgerTuning{MemTableMB: n.cfg.MemTableMB, BlockCacheMB: n.cfg.BlockCacheMB, IndexCacheMB: n.cfg.IndexCacheMB, TableSync: n.cfg.TableSync}, func() ([]byte, error) { return n.ringStoreKey(storeDir) }, n.refuseEmptyStoreAfterEarlierLife); err != nil {
		// The store names the damage; what to do depends on whether this node is a cluster member.
		return fmt.Errorf("opening Badger in %s: %w", n.cfg.Dir, ringRefusalWithAdvice(err, n.cfg.Dir, n.cfg.Cluster != nil))
	}
	// The store's runtime refusals (a damaged read, a write stalled behind a damaged table) end in
	// this node's advice, not the legacy store's (review R2 F-4).
	n.kv.SetRepairAdvice(n.ringAdvice())
	if n.wal == nil { // the first-start check may have opened it already
		if err := n.openLog(); err != nil {
			return err
		}
	}
	// Before wire: wire builds the advancers, and one built with no tracker would register nothing.
	n.floor = newLogFloor(n.wal)
	if n.cfg.Cluster != nil {
		if err := n.peerNet(); err != nil {
			return err
		}
	}
	if err := n.openRaft(); err != nil {
		return err
	}
	n.topology = NewClusterTopology(n.cfg.NodeID, n.raft)
	n.topology.bumps = n.bumps
	if n.restarted, err = n.bootedBefore(); err != nil {
		return err
	}
	if err := n.wire(); err != nil {
		return err
	}
	from, err := LoadLocalPairRecords(n.kv, n.pairs, n.cl != nil)
	if err != nil {
		return fmt.Errorf("loading the pair records: %w", err)
	}
	if err := n.loadSeedRecords(); err != nil {
		return err
	}
	// The scan starts at or below every log record whose commit is above the cut: the marker's
	// floor covers the first writes of pairs no record's cursor covers (I-8, §3.6).
	start := min(n.kv.VouchedFloor(), from)
	// The boot hold is registered before the floor source is set, so no sync during the scan can
	// carry a floor above a record the boot still owes (trap 5). LoadLocalPairRecords' synced
	// rewrite above ran with no source and kept the marker's floor.
	n.floor.holdBoot(start)
	n.kv.SetLogFloorSource(n.floor.sample)
	if report := n.kv.VouchedOpenReport(); report.DiscardedCommits > 0 {
		slog.Info(fmt.Sprintf("store: discarded %d commits above the sync marker (version %d) and kept everything through %d; "+
			"%d log(s) truncated, %d removed. The node re-applies them from its log, starting at log sequence %d.",
			report.DiscardedCommits, report.Vouched, report.Kept, report.LogsTruncated, report.LogsRemoved, start))
	}
	if beforeTailScan != nil {
		beforeTailScan(n)
	}
	waiting := newReplayDeferrals(nil, n.own.Defer)
	scanStarted := time.Now()
	err = n.tail.ScanBoot(ctx, start, waiting)
	if err != nil {
		// A refused record names the damage or the build; the advice is this node's to add.
		return ringRefusalWithAdvice(err, n.cfg.Dir, n.cfg.Cluster != nil)
	}
	waitingPairs, lowestWaiting, _ := waiting.stopped()
	logBootReplay(waiting.reappliedCount(), time.Since(scanStarted), len(waitingPairs))
	if len(waitingPairs) > 0 {
		n.floor.raiseBoot(lowestWaiting)
	} else {
		n.floor.releaseBoot()
	}
	// One store sync, so the replayed tail is vouched and a second kill does not replay it again;
	// it also gives a store whose floor the log's tail fell below a real floor.
	if err := n.kv.Sync(); err != nil {
		return fmt.Errorf("syncing the node store after the boot replay: %w", err)
	}
	// The sweep starts last: the pairs are loaded and replayed, so a note replayed from the
	// Raft log finds them and the dedupe sweep judges entries against a loaded ordinal, and
	// it runs on its own context so a short-lived ctx cannot stop it. The retention ticker
	// is the sweep's only tick; it is one timer per node, not a goroutine.
	n.retire.setOnSwept(n.dropper.finishSwept)
	n.retention = time.NewTicker(n.cfg.RetentionCheck)
	n.retire.Start(context.Background(), RetireDeps{
		KV: n.kv, Pairs: n.pairs, Declarations: n.fetcher, BucketName: n.fetcher.readName,
		Published: n.topology.Published, KeysPerSecond: n.cfg.ReclaimKeysPerSecond,
		Tick: n.retention.C, TickInterval: n.cfg.RetentionCheck, Dedupe: n.dedupe, Expiry: n.expiry, Budget: n.reclaim,
		OnError: func(err error) {
			if n.cl != nil {
				n.cl.report(err)
			}
		},
	})
	if n.cl != nil {
		n.startCluster()
	}
	if len(waitingPairs) > 0 {
		n.startBootReplay(waitingPairs, lowestWaiting)
	}
	return nil
}

// openLog opens the node's log, sealed under the keys AtRest hands it when the node seals.
func (n *Node) openLog() error {
	logDir := filepath.Join(n.cfg.Dir, "wal")
	logKeys, err := n.ringLogKeys(logDir)
	if err != nil {
		return err
	}
	if n.wal, err = storage.OpenWALWithOptions(logDir, logKeys, storage.WALOptions{Preallocate: n.cfg.WALPreallocate}); err != nil {
		return fmt.Errorf("opening the log in %s: %w", n.cfg.Dir, err)
	}
	return nil
}

// refuseEmptyStoreAfterEarlierLife is the store's first-start check (review R-1, R2 F-3): it runs
// when the store holds no data file. Every start of this build opens the store, writing its marker,
// before the log and the Raft are created, so a Raft that holds state or a log that holds records
// beside an empty store means the store was moved aside or deleted. The incarnation record is not a
// signal: nextIncarnation writes it before the store opens, so a crash inside a real first start
// leaves it. The Raft is read before it is opened (cluster.RaftStateOnDisk); the log is opened here
// when it exists and kept for the open.
//
// A cluster member is refused: it cannot tell its seeded pairs, which never passed through the
// log, from the log-borne ones. A single node is never seeded and nothing truncates its log, so it
// rebuilds the store by the boot replay when its Raft state is there and its log begins at sequence
// 1, and is refused when it does not, or when its log is missing or empty while its Raft holds a
// database (owner ruling 2026-11-19). It runs only when the store is
// empty: O(1) per start.
func (n *Node) refuseEmptyStoreAfterEarlierLife() error {
	raftState, err := cluster.RaftStateOnDisk(n.cfg.Dir)
	if err != nil {
		return fmt.Errorf("checking for an earlier run before starting an empty node store: %w", err)
	}
	var lastLog uint64
	if _, err := os.Stat(filepath.Join(n.cfg.Dir, "wal")); err == nil {
		if err := n.openLog(); err != nil {
			return err
		}
		lastLog = n.wal.LastSequence()
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking for an earlier run before starting an empty node store: %w", err)
	}
	if !raftState && lastLog == 0 {
		return nil
	}
	store := filepath.Join(n.cfg.Dir, "badger")
	member := n.cfg.Cluster != nil
	if member {
		return &RingStoreRefusal{
			Err:    &EmptyStoreAfterEarlierLifeError{Store: store, RaftState: raftState, LastLog: lastLog},
			Advice: ClusterMemberRingAdvice(n.cfg.Dir),
		}
	}
	if raftState && lastLog == 0 {
		// The log is absent or empty. A node that declared a database has lost the writes the log
		// held, and a rebuild from nothing would open as an empty store; one that never declared
		// one has nothing to lose.
		declared, err := cluster.RaftHoldsDatabase(n.cfg.Dir)
		if err != nil {
			return fmt.Errorf("checking the raft state for a database before starting an empty node store: %w", err)
		}
		if declared {
			return &RingStoreRefusal{
				Err:    &EmptyStoreAfterEarlierLifeError{Store: store, RaftState: true, LogMissing: true},
				Advice: singleNodeKeepAdvice(n.cfg.Dir),
			}
		}
		return nil
	}
	if lastLog > 0 && !raftState {
		return &RingStoreRefusal{
			Err:    &EmptyStoreAfterEarlierLifeError{Store: store, LastLog: lastLog, NoRaft: true},
			Advice: singleNodeKeepAdvice(n.cfg.Dir),
		}
	}
	if lastLog > 0 {
		first, err := n.wal.FirstSequence()
		if err != nil {
			return fmt.Errorf("reading the first sequence of the log before rebuilding the node store: %w", err)
		}
		if first != 1 {
			return &RingStoreRefusal{
				Err:    &EmptyStoreAfterEarlierLifeError{Store: store, RaftState: raftState, LastLog: lastLog, LogFirst: first},
				Advice: singleNodeKeepAdvice(n.cfg.Dir),
			}
		}
	}
	slog.Warn(fmt.Sprintf("the node store %s held no data file, and this node is not a member of a cluster: its log holds "+
		"every write from sequence 1 through %d, so the store is rebuilt from the log on this start", store, lastLog))
	return nil
}

// The pace of the boot-replay retry: it starts at the minimum, doubles after every pass that
// leaves a pair waiting, and stops at the maximum. A change of the committed state_seq, which is
// when a declaration can become readable, retries at once. Variables so a test can pace it.
var (
	bootReplayMin = 200 * time.Millisecond
	bootReplayMax = 5 * time.Second
	// bootReplayReportEvery is how often the same failure is reported again.
	bootReplayReportEvery = time.Minute
	// bootReplayPass, when set, runs at the start of every pass of the retry: the seam a test
	// uses to count the passes or to hold one. Nil in production.
	bootReplayPass func()
)

// startBootReplay finishes, in the background, the replay of the pairs whose boot replay stopped
// at a record that needs a declaration (BUG-0760). A restart replays the log before the node has
// the committed state, and a declaration is read from the ring through that state, so it
// cannot be read yet. Option (b) of the bug: the open returns, each pair stays fenced
// (flagReplayNeeded: no read, no ordered write, no copy apply) and this goroutine rescans from
// the lowest cursor a pair stopped at, through the boot form of the scan, so a pair that still
// cannot replay stops again by itself and never holds another: every pair a pass no longer stops
// is released in that pass. Rescanning from the lowest cursor re-reads records of other
// pairs, which their applied position skips.
//
// What each permanent failure does:
//   - a declaration slot with no live owner: the pair stops again each pass, stays fenced and
//     is retried at the capped pace; it serves nothing, which loses nothing;
//   - a bucket that reads as not found: not a drop, because the owner answers from its raw Badger
//     (it may be seeding, catching up, or stopped in this very replay). The pair stops and
//     retries. Only a dropping declaration, or the 'b' record the drop's bump wrote, makes the
//     record an empty write, and then the pair is applied past it and released;
//   - a pair that is not ready (the reconciler retired it): it stays stopped and fenced, and the
//     retire gate passes nothing for it while it is deferred, because its fence lags its log
//     (a record acknowledged degraded may sit only in this node's log). Stuck, not lost.
//
// The alternatives were worse. Reading the declaration from the local Badger (a) needs the
// database's name to find the slot, and the name is in the committed state too; and the
// declaration slot may be owned by another node. Moving the whole scan after the first commit
// index (c) would change the order the retire sweep and the appended-sequence rule rely on.
// logBootReplay writes the INFO line a boot replay pass ends with: the records re-applied, the
// seconds, and the pairs still waiting for a declaration, if any.
func logBootReplay(reapplied int, took time.Duration, waiting int) {
	line := fmt.Sprintf("store: the boot replay re-applied %d records in %.3f s", reapplied, took.Seconds())
	if waiting > 0 {
		line += fmt.Sprintf("; %d pairs refuse reads and writes until their replay finishes", waiting)
	}
	slog.Info(line + ".")
}

func (n *Node) startBootReplay(waiting map[pairKey]struct{}, lowest uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	n.bootReplayStop = cancel
	n.bootReplayDone = make(chan struct{})
	go func() {
		defer close(n.bootReplayDone)
		pace, lastText, lastReport := bootReplayMin, "", time.Time{}
		var earlyWake time.Time // when a state change last cut a wait short
		report := func(err error) {
			if n.cl == nil || err == nil {
				return
			}
			if text := err.Error(); text != lastText || time.Since(lastReport) >= bootReplayReportEvery {
				lastText, lastReport = text, time.Now()
				n.cl.report(fmt.Errorf("finishing the replay of the log tail (retrying): %w. Nothing is lost: the pair serves nothing until the replay finishes, and the replay retries by itself every few seconds, so a restart of this node does not make it finish sooner. Check that the node that owns the database's declaration is up ('nodes' lists the members) and that this node has applied the cluster state", err))
			}
		}
		for {
			if bootReplayPass != nil {
				bootReplayPass()
			}
			seq := n.stateSeq()
			pass := newReplayDeferrals(waiting, n.own.Defer)
			passStarted := time.Now()
			err := n.tail.ScanBoot(ctx, lowest, pass)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				stillWaiting, stillLowest, cause := pass.stopped()
				logBootReplay(pass.reappliedCount(), time.Since(passStarted), len(stillWaiting))
				released := 0
				for key := range waiting {
					if _, stuck := stillWaiting[key]; !stuck {
						n.own.Release(key.slot, key.db)
						released++
					}
				}
				if len(stillWaiting) == 0 {
					// No pair waits: the boot hold ends, and one store sync vouches the replayed
					// tail so a second kill does not defer these pairs again (§3.6). The node keeps
					// serving if it fails.
					n.floor.releaseBoot()
					if err := n.kv.Sync(); err != nil && n.cl != nil {
						n.cl.report(fmt.Errorf("syncing the node store after the boot replay released its last pair: %w", err))
					}
					return
				}
				n.floor.raiseBoot(stillLowest)
				if released > 0 {
					pace = bootReplayMin
				}
				waiting, lowest = stillWaiting, stillLowest
				report(cause)
			} else {
				report(err)
			}
			if !n.waitBootReplay(ctx, pace, seq, &earlyWake) {
				return
			}
			pace = min(pace*2, bootReplayMax)
		}
	}()
}

// stateSeq is the committed state_seq this node has applied, 0 before any.
func (n *Node) stateSeq() int64 {
	if n.raft == nil {
		return 0
	}
	return n.raft.StateSeq()
}

// waitBootReplay waits up to pace, and returns early when the committed state moved past seq.
// It reports false when ctx ended.
// A state change cuts the wait short at most once per pace, so a cluster that changes state
// all the time cannot make a stuck pair rescan the tail at the rate of the changes.
func (n *Node) waitBootReplay(ctx context.Context, pace time.Duration, seq int64, earlyWake *time.Time) bool {
	timer := time.NewTimer(pace)
	defer timer.Stop()
	poll := time.NewTicker(bootReplayPoll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-poll.C:
			if n.stateSeq() != seq && time.Since(*earlyWake) >= pace {
				*earlyWake = time.Now()
				return true
			}
		}
	}
}

// bootReplayPoll is how often the wait reads state_seq: one atomic-sized read, not a scan.
const bootReplayPoll = 25 * time.Millisecond

func (n *Node) openRaft() error {
	raftConfig := cluster.RaftConfig{
		NodeID: n.cfg.NodeID, Dir: n.cfg.Dir, Incarnation: n.incarnation, Bootstrap: true,
		HeartbeatTimeout: 200 * time.Millisecond, ElectionTimeout: 200 * time.Millisecond, LeaderLease: 100 * time.Millisecond,
		Schema: schemaPublisher{bumps: n.bumps, pairs: n.pairs, retire: n.retire},
	}
	var listener net.Listener
	if n.cl != nil {
		listener = n.cl.net.RaftListener()
		raftConfig.Dial = n.cl.net.DialRaft
		raftConfig.DataAddr = n.dataAddr()
		// Only a networked node publishes one: a one-voter node has no peers and no real data address,
		// and the cluster refuses a client address on any other host than the data address.
		raftConfig.ClientAddr = n.cfg.ClientAddr
		if raftConfig.ClientAddr != "" {
			if err := cluster.ValidateClientAddr(raftConfig.ClientAddr, raftConfig.DataAddr); err != nil {
				return fmt.Errorf("the client address from tcp.advertise (or tcp.listen) does not fit this node's address %q (cluster.advertise, else cluster.listen): %w. "+
					"Set tcp.advertise to a host:port on that same host, or change cluster.advertise to match", raftConfig.DataAddr, err)
			}
		}
		raftConfig.Bootstrap = !n.cl.opts.Join
		raftConfig.OnApply = n.cl.onApply
		raftConfig.ApplyGate = raftApplyGate
		if held := heldApplies; held != nil && raftApplyGate == nil {
			id := n.cfg.NodeID
			raftConfig.ApplyGate = func(kind byte) { held(id, kind) }
		}
		raftConfig.PeerFingerprint = n.cl.opts.Fingerprints
		raftConfig.StateRange = n.cl.opts.StateRange
		raftConfig.Limits = n.cl.opts.Limits.raft()
		raftConfig.Logger = n.cl.opts.RaftLogger
		if timeout := n.cl.opts.RaftTimeout; timeout > 0 {
			raftConfig.HeartbeatTimeout, raftConfig.ElectionTimeout, raftConfig.LeaderLease = timeout, timeout, timeout/2
		}
	} else {
		var err error
		if listener, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			return fmt.Errorf("listening for the cluster channel: %w", err)
		}
		raftConfig.DataAddr = listener.Addr().String()
	}
	raftConfig.Listener = listener
	var err error
	raft, err := cluster.OpenRaft(raftConfig)
	n.raft = raft
	if n.cl != nil && err == nil {
		n.cl.rf.Store(raft)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("opening the cluster state in %s: %w", n.cfg.Dir, err), listener.Close())
	}
	return nil
}

// wire builds the owner and read paths over the opened stores. Nothing here holds
// anything per bucket.
func (n *Node) wire() error {
	window := machine.WindowConfig{Window: n.cfg.DedupeWindow, MaxResult: maxResultBytes}
	n.dedupe = NewDedupeSweeper(n.kv, n.pairs, window)
	n.dedupe.advice = n.ringAdvice()
	n.pairs.OnDrop(n.dedupe.Forget)
	n.expiry = NewExpirySweeper(n.kv, n, nil).WithPairLock(heldPairLock(n.pairs)).WithExpiryTable(n.expiryTable)
	applier := NewDocumentApplier(n.kv, nil).WithFieldEngines(declaredEngines{node: n}).WithResultCap(maxResultBytes)
	if n.cl != nil {
		// The one live-write budget of the node, from replication.seed_buffer_bytes. Every pull
		// in flight divides it (seedBuffers.Begin), so a seeding pair's frames wait here for
		// the drain instead of being lost to the owner's retry alone.
		n.buffers = newSeedBuffers(n.cl.opts.Seed.withDefaults().BufferBytes)
	}
	peers, err := n.peers(applier, window)
	if err != nil {
		return err
	}
	n.writes = peers.writes
	n.handoff = NewHandoff(HandoffConfig{
		Self: n.cfg.NodeID, Pairs: n.pairs, Link: peers.link, CatchUp: peers.catchUp, Drain: peers.drain, Clock: bootclock.New(), Restarted: n.restarted, Opened: n.tailOpened,
		Deferred: n.pairDeferred,
	})
	topology := WithHandoffTarget(n.topology, n.handoff)
	n.fetcher = NewFetcher(FetcherConfig{
		Topology: topology, Reader: peers.decls,
		DatabaseName: n.topology.DatabaseName, Published: n.topology.Published,
		Cache: machine.CacheConfig{MaxEntries: 4096, MaxBytes: 64 << 20, MaxDeclBytes: 4 << 20},
	})
	n.schema = NewCacheSchema(n.fetcher)
	schema := n.schema
	lease := peers.lease
	n.lease = lease
	n.live = nodeLive{node: n, clock: bootclock.New()}
	n.writer = NewWriter(WriterConfig{
		Topology: topology, Pairs: n.pairs, Log: n.logFor(), KV: n.kv, Applier: applier, Clock: bootclock.New(),
		Lease: lease, Dedupe: NewKVDedupeReader(n.kv), Schema: schema, Replication: peers.replication,
		WCTimeout: n.wcTimeout(), DedupeWindow: window, NotReady: peers.notReady, Advancer: peers.advancer, Settled: n.handoffApplied,
		GuardObserved: n.cfg.GuardObserved, DrainObserved: n.cfg.DrainObserved, Floor: n.floor,
	})
	gate := NewReadGate(ReadGateConfig{
		Topology: topology, Pairs: n.pairs, Clock: bootclock.New(), Lease: lease, Schema: schema, Slots: peers.slots, Settled: n.handoffApplied,
		Undecided: n.undecidedCause,
	})
	n.reader = NewReader(ReaderConfig{
		Gate: gate, KV: n.kv, Holder: n.handoff, PeerReachable: peers.reachable,
		Forward: peers.reads, Self: n.cfg.NodeID, NearestMaxElements: n.cfg.NearestMaxElements,
		StaleReads: n.topology.StaleReads, CorruptKeyAdvice: n.ringAdvice(),
	})
	n.scanner = NewSlotScanner(SlotScannerConfig{KV: n.kv, Topology: topology, Schema: schema, Gate: gate, CorruptKeyAdvice: n.ringAdvice()})
	n.own = NewRestartedPairOwnership(n.pairs, PairOwnershipConfig{Restarted: n.restarted, Hold: n.holdRestarted, Settled: n.handoffSettled})
	n.tail = NewTailScanner(n.wal, n.writer, func(slot uint16, db uint64) cluster.Position {
		return n.pairs.Snapshot(slot, db).AppliedPos
	})
	sender := nodeSender{node: n}
	n.coord = NewCoordinator(CoordinatorConfig{Sender: sender})
	n.creator = NewCreateBucket(CreateBucketConfig{
		Sender: sender, Declarations: n.fetcher, Topology: topology,
		DatabaseName: n.topology.DatabaseName, Published: n.topology.Published,
	})
	n.dropper = n.newDropper()
	n.alterer = NewAlterBucket(AlterBucketConfig{
		Sender: sender, Declarations: n.fetcher, Topology: topology, DatabaseName: n.topology.DatabaseName,
		Committed: n.committedSchema, Published: n.topology.Published, Bumper: n, Queue: &AlterQueue{},
	})
	return nil
}

// newDropper builds a DropBucket over the node's own collaborators.
func (n *Node) newDropper() *DropBucket {
	return NewDropBucket(DropBucketConfig{
		Sender: nodeSender{node: n}, Declarations: n.fetcher, Topology: n.topology, DatabaseName: n.topology.DatabaseName,
		BucketName: n.fetcher.readName, Committed: n.committedSchema, Published: n.topology.Published, Bumper: n, Sweeper: n.retire, ReverseState: n.fetcher.readReverse,
	})
}

// DropBucket drops bucket name of database db and its documents (conv 21): a dropping marker,
// one bump, a sweep of every held pair, then the declaration and the reverse-index document.
// It is safe to repeat; a repeat of an interrupted drop finishes it.
func (n *Node) DropBucket(ctx context.Context, db, name string) error {
	dbID, err := n.database(db)
	if err != nil {
		return err
	}
	if err := n.dropper.Run(ctx, dbID, name); err != nil {
		return fmt.Errorf("dropping bucket %q in database %q: %w", name, db, err)
	}
	return nil
}

// StartDropBucket drops bucket name of database db without waiting for its documents: the bucket
// is dropping and fenced when it returns, and the node deletes the documents in the background and
// then frees the name. It returns the bucket's id.
func (n *Node) StartDropBucket(ctx context.Context, db, name string) (uint64, error) {
	return n.StartDropBucketExpecting(ctx, db, name, 0)
}

// StartDropBucketExpecting is StartDropBucket for a caller that knows the id of the bucket it
// means; a bucket with another id is refused and left alone. Zero means any.
func (n *Node) StartDropBucketExpecting(ctx context.Context, db, name string, expect uint64) (uint64, error) {
	dbID, err := n.database(db)
	if err != nil {
		return 0, err
	}
	id, err := n.dropper.StartExpecting(ctx, dbID, name, expect)
	if err != nil {
		return 0, fmt.Errorf("dropping bucket %q in database %q: %w", name, db, err)
	}
	return id, nil
}

// committedSchema is the database's committed schema_version, read from the committed state
// and not from the view of the last refresh.
func (n *Node) committedSchema(dbID uint64) int64 {
	n.topology.Refresh()
	return n.topology.CommittedSchema(dbID)
}

// schemaPublisher is the node's cluster.SchemaPublisher: a bump is published over the pairs
// this node holds.
type schemaPublisher struct {
	bumps  *cluster.BumpApplier
	pairs  *cluster.PairSet
	retire *RetireSweep
}

// Stage implements cluster.SchemaPublisher.
func (p schemaPublisher) Stage(dbID uint64, version int64) { p.bumps.Stage(dbID, version) }

// Publish implements cluster.SchemaPublisher.
func (p schemaPublisher) Publish(dbID uint64, to int64) { p.bumps.Apply(dbID, to, p.pairs) }

// Forget implements cluster.SchemaPublisher.
func (p schemaPublisher) Forget(dbID uint64) { p.bumps.Forget(dbID) }

// Note implements cluster.SchemaPublisher.
func (p schemaPublisher) Note(dbID uint64, to int64, note cluster.BumpNote) {
	p.retire.Note(dbID, to, note)
}

// handoffSettled reports whether the Handoff has been fed the epoch the topology reports.
func (n *Node) handoffSettled() bool {
	return n.handoff == nil || n.handoff.Applied(n.topology.Committed())
}

// handoffApplied reports whether the Handoff has been fed committed. It is the gates' form
// of handoffSettled, for a caller that already read the epoch it judged ownership on.
func (n *Node) handoffApplied(committed uint64) bool {
	return n.handoff == nil || n.handoff.Applied(committed)
}

// nodeSender is the Sender every write of the node goes through: it makes the pair
// orderable, writes, and recovers the pair when the apply failed after the append.
type nodeSender struct{ node *Node }

// Write implements Sender.
func (s nodeSender) Write(ctx context.Context, req WriteRequest) (WriteAnswer, error) {
	n := s.node
	n.own.Acquire(req.Slot, req.DBID)
	answer, err := n.writer.Write(ctx, req)
	if err != nil {
		// A refused append fences the pair until its log is replayed (write path section 2
		// row 8); replaying now lets the next write reuse the position instead of wedging.
		// A pair that waits for its boot replay is not recovered here: that goroutine owns it. Nor is one
		// whose log was closed under the write: the node is closing, the pair stays fenced and the next
		// boot replays it, and a scan of the closed log would only join its own failure into the client's error.
		if flags := n.pairs.Snapshot(req.Slot, req.DBID).Flags; flags&flagReplayNeeded != 0 && flags&flagBootReplay == 0 && !errors.Is(err, storage.ErrLogClosed) {
			if recoverErr := n.recoverPair(ctx, req.Slot, req.DBID); recoverErr != nil {
				return answer, errors.Join(err, fmt.Errorf("replaying the pair after the refused append failed too: %w", recoverErr))
			}
		}
		return answer, err
	}
	if answer.Forward && n.writes != nil {
		return n.forwardWrite(ctx, req)
	}
	if answer.Held {
		// A pair fenced for a replay that failed (a declaration that could not be read, BUG-0771)
		// is replayed again by the next write, not only by the write that failed: nothing else
		// retries it, and a pair that waits for its boot replay belongs to that goroutine.
		if flags := n.pairs.Snapshot(req.Slot, req.DBID).Flags; flags&flagReplayNeeded != 0 && flags&flagBootReplay == 0 {
			if recoverErr := n.recoverPair(ctx, req.Slot, req.DBID); recoverErr != nil {
				return answer, fmt.Errorf("the pair is fenced until its log is replayed and the replay failed again: %w. This write was not ordered, so nothing new was written; what the log already holds stays in it. The replay runs again on the next write", recoverErr)
			}
		}
		answer.HoldCause = n.handoff.HoldCause(req.Slot, req.DBID)
		if answer.HoldCause == "" && n.pairs.Snapshot(req.Slot, req.DBID).Flags&flagBootReplay != 0 {
			answer.HoldCause = bootReplayHoldCause
		}
		if answer.HoldCause == "" && n.pairs.Snapshot(req.Slot, req.DBID).Flags&flagReplayed == 0 {
			answer.HoldCause = n.undecidedCause()
		}
	}
	if answer.ApplyFailure != nil {
		if recoverErr := n.recoverPair(ctx, req.Slot, req.DBID); recoverErr != nil {
			return answer, fmt.Errorf("the write is durable at %s and its apply failed (%v); replaying the pair failed too: %w", answer.Pos, answer.ApplyFailure, recoverErr)
		}
	}
	return answer, nil
}

// recoverPair replays the log from the pair's record after a failed apply, then lets the
// pair order again (write path §2 row 9).
//
// A row 9 answers every pending write of the pair with an apply failure, up to pipelineCap of them, and
// each asks for this recovery: callers that arrive while a recovery of the pair is running wait for it
// and share its result instead of each scanning the whole tail. That is safe only because the recovery
// drains the pair before it reads where its scan ends (the log's synced sequence, read when the scan
// starts): every entry pending when the fence went up has popped by then, and a popped entry is either
// covered by that sequence or failed its own flush and was never claimed durable. No entry is pushed
// while the pair is fenced, so a joiner can never be owed a record above the scan's end. The recovery
// ends with Recovered, which lifts the fence for everyone who joined. A joiner answers with an error when
// its own ctx ends and leaves the flight to its leader.
func (n *Node) recoverPair(ctx context.Context, slot uint16, db uint64) error {
	return n.recovering.do(ctx, pairKey{slot, db}, func() error {
		n.pairs.Snapshot(slot, db) // drains the pair's pending list before the scan's end is read
		from, err := n.recoverFrom(slot, db)
		if err != nil {
			return err
		}
		if err := n.tail.Scan(ctx, from); err != nil {
			return err
		}
		// A scan that returned cleanly can leave the owed record unapplied. The practical case is a pair
		// whose copy set no longer names this node: the advancer refuses the owed replay (errOwedNotHeld)
		// and Writer.replay skips that refusal so the scans of other pairs go on. The other is a last
		// appended position the scan finds no record at. The flag is cleared only by the replay that
		// created the record, so a flag still set means the write is not applied, and lifting the fence
		// would let the next write skip it (BUG-0809).
		if snap := n.pairs.Snapshot(slot, db); snap.Flags&flagFirstApplyFailed != 0 {
			return fmt.Errorf("slot %d db %d: the first write to the pair, at %s, is saved in this node's log but is not applied here. The usual cause is that this node holds no record of the pair and is no longer in its copy set, so it cannot apply the write; otherwise the replay did not find the write in the log. The pair stays fenced on this node so the write is never skipped. When this node left the copy set, the pair's writes belong to the nodes of its copy set: run 'status' to see which pairs each node holds",
				slot, db, cluster.Position{Epoch: snap.AppendedEpoch, Seq: snap.AppendedSeq})
		}
		n.own.Recovered(slot, db)
		// The replay moved the pair's TailCursor without queueing anything for the copies, and a lane
		// reads no further than what was queued (BUG-0865): queue the pair at its new tail, as a
		// restart or a takeover does, so a record that failed to apply here (a copy promoted before its
		// replay) does not wait for the pair's next write to reach the copies.
		if n.cl != nil {
			if owner, known := n.ownerOf(slot, db); known && owner == n.cfg.NodeID {
				n.cl.repl.ShipTail(slot, db)
			}
		}
		return nil
	})
}

// pairFlights runs one call of a function per pair at a time and hands its result to every caller
// that arrived while it ran.
type pairFlights struct {
	mu     sync.Mutex
	flying map[pairKey]*pairFlight
}

type pairFlight struct {
	done chan struct{}
	err  error
}

func (f *pairFlights) do(ctx context.Context, key pairKey, run func() error) error {
	f.mu.Lock()
	if flight, ok := f.flying[key]; ok {
		f.mu.Unlock()
		select {
		case <-flight.done:
			return flight.err
		case <-ctx.Done():
			// The flight goes on for its leader; this caller answers within its own deadline and the pair
			// stays fenced for the next write to recover.
			return fmt.Errorf("waiting for the replay of the pair that another write started: %w", ctx.Err())
		}
	}
	if f.flying == nil {
		f.flying = make(map[pairKey]*pairFlight)
	}
	flight := &pairFlight{done: make(chan struct{})}
	f.flying[key] = flight
	f.mu.Unlock()

	// Whatever run does, the flight ends: a panic is re-raised to this caller after joiners are released
	// with an error, so one panic cannot leave every later recovery of the pair waiting for ever.
	flight.err = errors.New("the replay of the pair that another write started did not finish")
	defer func() {
		f.mu.Lock()
		delete(f.flying, key)
		f.mu.Unlock()
		close(flight.done)
	}()
	flight.err = run()
	return flight.err
}

// recoverFrom is the cursor recoverPair replays from: the pair record's WAL cursor, or 1
// when the pair has no record. On a cluster node the record is the seed record, read
// through the Reconciler (seeding-completion plan §0.2); a single node keeps the 32-byte
// record. The horizon clamp ("start at the horizon" on ErrBelowHorizon) is the
// storage-format plan's F8, not this function's.
func (n *Node) recoverFrom(slot uint16, db uint64) (uint64, error) {
	if n.cl != nil {
		record, found := n.cl.seeding.Load().recon.Record(slot, db)
		if !found {
			return 1, nil
		}
		return max(1, record.WALCursor), nil
	}
	snapshot := n.kv.NewSnapshot()
	raw, err := snapshot.Get(LocalPairRecordKey(slot, db))
	snapshot.Discard()
	if err == nil && len(raw) == localPairRecordLen {
		return max(1, binary.BigEndian.Uint64(raw[16:24])), nil
	} else if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return 0, fmt.Errorf("reading the pair record of slot %d: %w", slot, err)
	}
	return 1, nil
}

// declaredEngines resolves a store field's engine from its declared type.
type declaredEngines struct{ node *Node }

// ExpireNames implements expireNames from the cached declaration; it reads no ring and fills nothing.
func (e declaredEngines) ExpireNames(db, bucketID uint64, fieldID uint16) (database, bucket, field string, ok bool) {
	view := e.node.schema.View(db, bucketID)
	database, named := e.node.topology.DatabaseName(db)
	if !view.Present || view.Name == "" || !named {
		return "", "", "", false
	}
	table, _ := machine.EffectiveTable(view.Decl, view.Published)
	entry, found := entryWithID(table, fieldID)
	if !found {
		return "", "", "", false
	}
	return database, view.Name, entry.Name, true
}

// view returns the cached declaration of bucketID, refetching it when the cache is cold.
func (e declaredEngines) view(db, bucketID uint64) (SchemaView, error) {
	schemaView := e.node.schema.View(db, bucketID)
	if !schemaView.Present {
		// A cold cache is reached by a replay (boot or recoverPair), by a copy applying a shipped
		// record (a copy admits no client write, so nothing filled its cache) and by an owner whose
		// bounded cache evicted the entry between admission and apply.
		// The refetch is a ring read: the slot of the declaration may belong to another node and
		// the route needs the committed state, which a boot replay does not have yet. Such a
		// replay defers the pair (Writer.replay), so a failure here is never fatal to the open.
		ctx, cancel := context.WithTimeout(e.node.stopCtx, bootTimeout)
		defer cancel()
		if err := e.node.schema.Refetch(ctx, db, bucketID); err != nil {
			if errors.Is(err, ErrBucketNotFound) && !errors.Is(err, ErrBucketDropping) {
				// A bare not-found may not be read as "dropped", on any path: the owner of the
				// declaration slot answers from its raw Badger, which lacks the document while it
				// seeds, catches up after a membership change or failover, or has its own declaration
				// pair stopped in a replay, and an empty write here would drop an acknowledged record
				// for good (BUG-0760 for the boot replay, BUG-0771 for a copy apply and recoverPair).
				// Only the 'b' record the drop's bump wrote on this node proves it; without one the
				// record is refused and retried: a copy does not ack it, a replay leaves the pair
				// fenced. A record whose drop leaves no proof on this node stays refused (BUG-0772):
				// stuck, never lost.
				proven, lookupErr := e.node.hasBucketRecord(db, bucketID)
				if lookupErr != nil {
					return SchemaView{}, fmt.Errorf("looking for the drop record of bucket %d in database %d: %w: %w", bucketID, db, ErrDeclUnavailable, lookupErr)
				}
				if !proven {
					return SchemaView{}, fmt.Errorf("bucket %d in database %d reads as not found, and this node holds no record of its drop, so the record is not applied as an empty write. Nothing was lost: it is retried until the node that owns the database's declaration answers ('nodes' lists the members): %w: %w", bucketID, db, ErrDeclUnavailable, err)
				}
			}
			if errors.Is(err, ErrBucketDropping) || errors.Is(err, ErrBucketNotFound) {
				// A record naming a bucket that is dropping, or whose drop is proven, is an empty
				// write, as one naming a retired field is: its keys are swept with the rest of the
				// bucket (SPEC-GAP U42-3).
				return SchemaView{}, fmt.Errorf("bucket %d in database %d is being dropped or was dropped: %w: %w", bucketID, db, errRetiredField, errUndeclaredField)
			}
			return SchemaView{}, fmt.Errorf("the declaration of bucket %d in database %d is not cached and reading it failed: %w", bucketID, db, err)
		}
		if schemaView = e.node.schema.View(db, bucketID); !schemaView.Present {
			return SchemaView{}, fmt.Errorf("the declaration of bucket %d in database %d is not cached", bucketID, db)
		}
	}
	return schemaView, nil
}

// ExpireTable implements ExpireTableSource: the bucket's expiry declaration, the zero table when
// it has none. The owner reads it to refuse a push that would never expire and to stamp the
// record's expire items.
func (e declaredEngines) ExpireTable(db, bucketID uint64) (machine.ExpireTable, error) {
	schemaView, err := e.view(db, bucketID)
	if err != nil {
		return machine.ExpireTable{}, err
	}
	return machine.EffectiveExpire(schemaView.Decl, schemaView.Published), nil
}

// FieldEngine implements FieldEngineSource.
func (e declaredEngines) FieldEngine(db, bucketID uint64, fieldID uint16) (store.FieldEngine, error) {
	schemaView, err := e.view(db, bucketID)
	if err != nil {
		return 0, err
	}
	return fieldEngineOf(schemaView, db, bucketID, fieldID)
}

// fieldEngineOf is the engine of fieldID in the declaration schemaView holds.
func fieldEngineOf(schemaView SchemaView, db, bucketID uint64, fieldID uint16) (store.FieldEngine, error) {
	table, _ := machine.EffectiveTable(schemaView.Decl, schemaView.Published)
	entry, ok := entryWithID(table, fieldID)
	if !ok {
		if slices.ContainsFunc(table.Retired, func(retired machine.RetiredID) bool { return retired.ID == fieldID }) {
			return 0, fmt.Errorf("field %d of bucket %d in database %d was retired by an alter: %w: %w", fieldID, bucketID, db, errRetiredField, errUndeclaredField)
		}
		return 0, fmt.Errorf("bucket %d in database %d declares no field %d: %w", bucketID, db, fieldID, errUndeclaredField)
	}
	engine, ok := engineOfType(entry.Type)
	if !ok {
		return 0, fmt.Errorf("field %q of bucket %d is declared %q, which is not a store: %w", entry.Name, bucketID, entry.Type, errUndeclaredField)
	}
	return engine, nil
}

func engineOfType(declared string) (store.FieldEngine, bool) {
	switch declared {
	case "hashStore":
		return store.FieldHash, true
	case "btreeStore":
		return store.FieldBTree, true
	case "fifoStore":
		return store.FieldFIFO, true
	case "lifoStore":
		return store.FieldLIFO, true
	case "heapStore":
		return store.FieldHeap, true
	case "blobStore":
		return store.FieldBlob, true
	}
	return 0, false
}

// CreateDatabase applies the shared name policy, then creates database name with one copy of every slot (RF 1) and returns its
// id. Creating a database that exists is a CONFLICT whose text names the fix.
func (n *Node) CreateDatabase(ctx context.Context, name string) (uint64, error) {
	if err := databaseNameRefusal(name, n.cfg.StorageNames); err != nil {
		return 0, err
	}
	state := n.topology.Refresh()
	id := state.NextDatabaseID
	body := bsoncore.BuildDocument(nil, slices.Concat(
		bsoncore.AppendStringElement(nil, "name", name),
		bsoncore.AppendInt64Element(nil, "id", id),
		bsoncore.AppendInt32Element(nil, "rf", 1),
		bsoncore.AppendInt64Element(nil, "schema_version", 1),
	))
	if _, err := n.propose(ctx, cluster.CommandCreateDatabase, body); err != nil {
		return 0, fmt.Errorf("creating database %q: %w", name, err)
	}
	return uint64(id), nil
}

// database resolves a database name or says how to create it.
func (n *Node) database(name string) (uint64, error) {
	id, ok := n.topology.DatabaseID(name)
	if !ok {
		return 0, &cluster.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf(
			"database %q not found. Run 'show databases' to list what exists, or 'create database %s' to create it", name, name)}
	}
	return id, nil
}

// CreateBucket declares bucket name in database db with the given fields and returns its
// id. It is the reverse-first create of conv §21: safe to repeat.
func (n *Node) CreateBucket(ctx context.Context, db, name string, fields ...Field) (uint64, error) {
	return n.CreateBucketWithAggregates(ctx, db, name, fields, nil)
}

// CreateBucketExpiring is CreateBucket for a bucket that declares expiry: the datetime field of
// the bucket form and the element path of each fifoStore or lifoStore field.
func (n *Node) CreateBucketExpiring(ctx context.Context, db, name string, spec ExpireSpec, fields ...Field) (uint64, error) {
	return n.CreateBucketDeclared(ctx, db, name, spec, fields, nil)
}

// CreateBucketWithAggregates is CreateBucket for a bucket whose store fields declare
// maintained aggregates (aggregate spec 2.1). Every aggregate names its store field.
func (n *Node) CreateBucketWithAggregates(ctx context.Context, db, name string, fields []Field, aggregates []AggregateDecl) (uint64, error) {
	return n.CreateBucketDeclared(ctx, db, name, ExpireSpec{}, fields, aggregates)
}

// CreateBucketDeclared is CreateBucket for a bucket that may declare expiry (spec), 'of'
// references (on its fields) and maintained aggregates together.
func (n *Node) CreateBucketDeclared(ctx context.Context, db, name string, spec ExpireSpec, fields []Field, aggregates []AggregateDecl) (uint64, error) {
	dbID, err := n.database(db)
	if err != nil {
		return 0, err
	}
	entries := make([]FieldEntry, len(fields))
	for i, field := range fields {
		entries[i] = FieldEntry{Name: field.Name, Type: field.Type}
	}
	references, err := n.resolveReferences(ctx, db, name, fields)
	if err != nil {
		return 0, err
	}
	bucketID, err := n.creator.RunWithReferences(ctx, dbID, name, entries, references, spec, aggregates...)
	if err != nil {
		return 0, fmt.Errorf("creating bucket %q in database %q: %w", name, db, err)
	}
	return bucketID, nil
}

// AlterBucket adds and drops fields of bucket name in database db and returns the field
// table in force afterwards (conv 20): the new table is written as a pending field, then one
// BUMP_SCHEMA publishes it. It is safe to repeat; a repeat after an interrupted alter
// finishes it. Adding a field rewrites no document.
func (n *Node) AlterBucket(ctx context.Context, db, name string, change AlterChange) (machine.FieldTable, error) {
	dbID, err := n.database(db)
	if err != nil {
		return machine.FieldTable{}, err
	}
	if change.SetExpire != "" {
		if err := n.refuseExpiryOnDocuments(ctx, db, name, change.SetExpire); err != nil {
			return machine.FieldTable{}, err
		}
	}
	table, err := n.alterer.Run(ctx, dbID, name, change)
	if err != nil {
		return machine.FieldTable{}, fmt.Errorf("altering bucket %q in database %q: %w", name, db, err)
	}
	return table, nil
}

// bucketRef is a resolved bucket: what an operation on it needs.
type bucketRef struct {
	dbID    uint64
	db      string
	name    string
	entry   machine.Entry
	table   machine.FieldTable
	version int64
}

func (n *Node) bucket(ctx context.Context, db, name string) (bucketRef, error) {
	dbID, err := n.database(db)
	if err != nil {
		return bucketRef{}, err
	}
	entry, err := n.fetcher.Lookup(ctx, dbID, name, n.topology.Published(dbID))
	if err != nil {
		return bucketRef{}, fmt.Errorf("looking up bucket %q in database %q: %w", name, db, err)
	}
	table, version := machine.EffectiveTable(entry.Decl, n.topology.Published(dbID))
	return bucketRef{dbID: dbID, db: db, name: name, entry: entry, table: table, version: version}, nil
}

func (b bucketRef) template() cluster.Op {
	return cluster.Op{BucketID: b.entry.Decl.BucketID, DeclVersion: b.version, SchemaVersionSeen: b.entry.Seen}
}

func (b bucketRef) slot(encID []byte) uint16 {
	return storage.SlotOf(storage.PlacementKey(b.db, b.name, encID))
}

// documentOp encodes a document's scalar fields as the owner stores them: the type byte
// then the BSON value. A field the declaration does not have, or one that is a store, is
// FIELD_TYPE and nothing is written.
func (b bucketRef) documentOp(doc bson.D) (string, []cluster.FieldOp, error) {
	var id string
	fields := make([]cluster.FieldOp, 0, len(doc))
	for _, element := range doc {
		if element.Key == "_id_" {
			text, ok := element.Value.(string)
			if !ok {
				return "", nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
					"the _id_ of a document in %s.%s is a %T; this build takes a string _id_. Nothing was written. Send _id_ as a string", b.db, b.name, element.Value)}
			}
			id = text
			continue
		}
		entry, ok := entryNamed(b.table, element.Key)
		if !ok || isStoreType(entry.Type) {
			return "", nil, &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
				"field": element.Key, "db": b.db, "bucket": b.name, "declared": declaredScalars(b.table),
			})}
		}
		kind, raw, err := bson.MarshalValue(element.Value)
		if err != nil {
			return "", nil, fmt.Errorf("encoding field %q of a document in %s.%s: %w", element.Key, b.db, b.name, err)
		}
		fields = append(fields, cluster.FieldOp{FieldID: entry.ID, Value: append([]byte{byte(kind)}, raw...)})
	}
	slices.SortFunc(fields, func(a, c cluster.FieldOp) int { return int(a.FieldID) - int(c.FieldID) })
	return id, fields, nil
}

// WriteOption changes how one write is sent.
type WriteOption func(*writeOptions)

type writeOptions struct {
	requestID [16]byte
	deadline  time.Duration
	// concern is the write concern asked for; zero takes the database's default.
	concern machine.Concern
	// claimSink receives what a claim sent with these options leased.
	claimSink ClaimSink
	// opIndex and batchSize place the write in its client batch (RING_BATCH): the dedupe entry
	// is keyed by (request id, opIndex). Both are zero for a lone write.
	opIndex   uint8
	batchSize int
}

// WithRequestID sends the write under a request id of the caller's choosing. A retry with
// the same id is answered from the first attempt (conv §18); without it every call draws a
// fresh id.
func WithRequestID(id [16]byte) WriteOption {
	return func(o *writeOptions) { o.requestID = id }
}

// WithConcern asks for a write concern of level on this write instead of the database's
// default (conv §17): ConcernOne is answered `applied` on the owner's copy alone, which a
// later failover may discard. The zero level keeps the database default.
func WithConcern(level machine.Concern) WriteOption {
	return func(o *writeOptions) { o.concern = level }
}

// ConcernOf is the concern the options ask for: zero when none does, the database default.
// A harness records it with each acknowledgement.
func ConcernOf(options ...WriteOption) machine.Concern {
	var opts writeOptions
	for _, option := range options {
		option(&opts)
	}
	return opts.concern
}

func newWriteOptions(options []WriteOption) (writeOptions, error) {
	var opts writeOptions
	for _, option := range options {
		option(&opts)
	}
	if opts.requestID == ([16]byte{}) {
		if _, err := entropy.Read(opts.requestID[:]); err != nil {
			return writeOptions{}, fmt.Errorf("drawing a request id: %w", err)
		}
	}
	return opts, nil
}

// Inserted is what an insert returns.
type Inserted struct {
	// ID is the document's _id_, minted when the document carried none.
	ID string
	// Pos is the log position the insert holds.
	Pos cluster.Position
	// Deduped says the answer was recomposed from an earlier attempt of the same request.
	Deduped bool
	// Outcome is the write_outcome: applied, applied_degraded or unknown.
	Outcome machine.Outcome
	// CopiesConfirmed counts the owner, so an acknowledged insert is never below 1.
	CopiesConfirmed uint8
	// ServedBy is the node that ordered the write: the answering owner, not the entry node
	// when the write was forwarded. It is one string on this in-memory reply, not on the wire.
	ServedBy string
}

// Insert adds a document to bucket in db. A document without _id_ gets one derived from
// the request id; an insert of an existing _id_ is DOCUMENT_EXISTS.
func (n *Node) Insert(ctx context.Context, db, bucket string, doc bson.D, options ...WriteOption) (Inserted, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return Inserted{}, err
	}
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return Inserted{}, err
	}
	id, fields, err := ref.documentOp(doc)
	if err != nil {
		return Inserted{}, err
	}
	template := ref.template()
	template.Fields = fields
	answer, err := n.coord.Insert(ctx, InsertRequest{
		Database: db, Bucket: bucket, DBID: ref.dbID, ID: id, Template: template, RequestID: opts.requestID, Deadline: opts.deadline, Concern: opts.concern,
	})
	if err != nil {
		return Inserted{}, err
	}
	if answer.Held {
		return Inserted{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"the insert into %s.%s was not ordered here because %s. Nothing was written. Retry it in a few seconds: a retry with the same request id is safe", db, bucket, WriteAnswer{Held: true, HoldCause: answer.HoldCause}.notOrderedBecause())}
	}
	if answer.Forward {
		return Inserted{}, fmt.Errorf("inserting into %s.%s: %w", db, bucket, ErrNoPeers)
	}
	return Inserted{ID: answer.ID, Pos: answer.Pos, Deduped: answer.Deduped, Outcome: answer.Outcome, CopiesConfirmed: answer.CopiesConfirmed, ServedBy: cmp.Or(answer.ServedBy, n.cfg.NodeID)}, nil
}

// send writes one resolved operation and turns a hold or a forward into the error a
// one-voter node can only mean by it.
func (n *Node) send(ctx context.Context, ref bucketRef, encID []byte, op cluster.Op, opts writeOptions) (WriteAnswer, error) {
	op.EncID = encID
	answer, err := nodeSender{node: n}.Write(ctx, WriteRequest{
		Slot: ref.slot(encID), DBID: ref.dbID, Op: op, RequestID: opts.requestID, OpIndex: opts.opIndex, BatchSize: opts.batchSize, Deadline: opts.deadline, Concern: opts.concern, ClaimSink: opts.claimSink,
	})
	if err != nil {
		return WriteAnswer{}, fmt.Errorf("writing to %s.%s: %w", ref.db, ref.name, err)
	}
	if answer.Held || answer.Forward {
		return WriteAnswer{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"the write to %s.%s was not ordered here because %s. Nothing was written. Retry it in a few seconds: a retry with the same request id is safe", ref.db, ref.name, answer.notOrderedBecause())}
	}
	if err := applyFailureError(ref, answer); err != nil {
		return WriteAnswer{}, err
	}
	return answer, nil
}

// applyFailureError turns an answer whose apply failed after the append into the error the
// caller must see: the position is durable, so a retry with the same request id is safe,
// but the result is not there and a zero Lease or Taken would read as an empty success.
func applyFailureError(ref bucketRef, answer WriteAnswer) error {
	return bucketApplyFailureError(ref.db, ref.name, answer)
}

// bucketApplyFailureError is applyFailureError for a caller that has the names and no bucketRef:
// the coordinator, which sees the request and not the resolved bucket.
func bucketApplyFailureError(db, bucket string, answer WriteAnswer) error {
	if answer.ApplyFailure == nil {
		return nil
	}
	return errors.Join(&cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"the write to %s.%s is durable at %s but applying it failed, so it has no result. Retry it with the same request id to read its answer once the pair has recovered",
		db, bucket, answer.Pos)}, answer.ApplyFailure)
}

// Upsert writes the fields of doc to the document with its _id_: the insert path when it is
// absent or deleted, otherwise only the fields it carries change.
func (n *Node) Upsert(ctx context.Context, db, bucket string, doc bson.D, options ...WriteOption) (Inserted, error) {
	opts, err := newWriteOptions(options)
	if err != nil {
		return Inserted{}, err
	}
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return Inserted{}, err
	}
	id, fields, err := ref.documentOp(doc)
	if err != nil {
		return Inserted{}, err
	}
	if id == "" {
		return Inserted{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"an upsert into %s.%s needs an _id_. Nothing was written. Send db.%s.upsert({_id_: \"<id>\", …}), or db.%s.insert({…}) to have one made", db, bucket, bucket, bucket)}
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		return Inserted{}, fmt.Errorf("encoding the _id_ %q: %w", id, err)
	}
	op := ref.template()
	op.Class, op.IDValue, op.Fields = cluster.OpUpsert, stringValue(id), fields
	answer, err := n.send(ctx, ref, encID, op, opts)
	if err != nil {
		return Inserted{}, err
	}
	return Inserted{ID: id, Pos: answer.Pos, Deduped: answer.Deduped, Outcome: answer.Outcome, CopiesConfirmed: answer.CopiesConfirmed, ServedBy: cmp.Or(answer.ServedBy, n.cfg.NodeID)}, nil
}

// Get reads one document by _id_. An absent or deleted one is NOT_FOUND.
func (n *Node) Get(ctx context.Context, db, bucket, id string) (Document, error) {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return Document{}, err
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		return Document{}, fmt.Errorf("encoding the _id_ %q: %w", id, err)
	}
	doc, err := n.readDocument(ctx, ref, encID, nil)
	if err != nil {
		return Document{}, fmt.Errorf("reading %q from %s.%s: %w", id, db, bucket, err)
	}
	return doc, nil
}

// GetLocal is Get with the per-call local flag: a ready copy of the document on this node may
// answer, and Document.Served says whether it did.
func (n *Node) GetLocal(ctx context.Context, db, bucket, id string) (Document, error) {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return Document{}, err
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		return Document{}, fmt.Errorf("encoding the _id_ %q: %w", id, err)
	}
	doc, err := n.readDocumentFrom(ctx, ref, encID, nil, true)
	if err != nil {
		return Document{}, fmt.Errorf("reading %q from %s.%s: %w", id, db, bucket, err)
	}
	return doc, nil
}

// GetFields is Get returning only _id_ and the named scalar fields. The names are checked
// against the bucket's declaration before the document is read, and no store state is read.
func (n *Node) GetFields(ctx context.Context, db, bucket, id string, fields []string) (Document, error) {
	if err := checkFieldNames("fields", fields); err != nil {
		return Document{}, err
	}
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return Document{}, err
	}
	project, err := ref.projection(fields)
	if err != nil {
		return Document{}, err
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		return Document{}, fmt.Errorf("encoding the _id_ %q: %w", id, err)
	}
	doc, err := n.readDocument(ctx, ref, encID, project)
	if err != nil {
		return Document{}, fmt.Errorf("reading %q from %s.%s: %w", id, db, bucket, err)
	}
	return doc, nil
}

// readDocument reads the document encID of ref from its slot's owner. project is the
// scalar field ids to return, or nil for the whole body.
func (n *Node) readDocument(ctx context.Context, ref bucketRef, encID []byte, project []uint16) (Document, error) {
	return n.readDocumentFrom(ctx, ref, encID, project, false)
}

// readDocumentFrom is readDocument that lets a ready copy on this node answer when local is
// true (the per-call form of cluster.stale_reads).
func (n *Node) readDocumentFrom(ctx context.Context, ref bucketRef, encID []byte, project []uint16, local bool) (Document, error) {
	slot := ref.slot(encID)
	n.own.Acquire(slot, ref.dbID)
	return n.reader.Read(ctx, ReadRequest{Slot: slot, DBID: ref.dbID, BucketID: ref.entry.Decl.BucketID, EncID: encID, Fields: project, Local: local})
}

// Delete removes a document. Deleting one that is not there is not an error.
func (n *Node) Delete(ctx context.Context, db, bucket, id string, options ...WriteOption) error {
	opts, err := newWriteOptions(options)
	if err != nil {
		return err
	}
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return err
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		return fmt.Errorf("encoding the _id_ %q: %w", id, err)
	}
	op := ref.template()
	op.Class = cluster.OpDelete
	_, err = n.send(ctx, ref, encID, op, opts)
	return err
}

// storeField resolves the named store field of a bucket.
func (b bucketRef) storeField(field string) (uint16, error) {
	entry, ok := entryNamed(b.table, field)
	if !ok || !isStoreType(entry.Type) {
		return 0, &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
			"field": field, "db": b.db, "bucket": b.name, "declared": declaredScalars(b.table),
		})}
	}
	return entry.ID, nil
}

// StoreTarget names one store field of one document.
type StoreTarget struct {
	Database, Bucket, ID, Field string
}

// storeCall is a StoreTarget resolved to what an operation on its field needs.
type storeCall struct {
	ref     bucketRef
	encID   []byte
	fieldID uint16
}

func (n *Node) storeCall(ctx context.Context, target StoreTarget) (storeCall, error) {
	ref, err := n.bucket(ctx, target.Database, target.Bucket)
	if err != nil {
		return storeCall{}, err
	}
	fieldID, err := ref.storeField(target.Field)
	if err != nil {
		return storeCall{}, err
	}
	encID, err := store.EncodeComponent(target.ID)
	if err != nil {
		return storeCall{}, fmt.Errorf("encoding the _id_ %q: %w", target.ID, err)
	}
	return storeCall{ref: ref, encID: encID, fieldID: fieldID}, nil
}

// PushRequest is one element appended to a store field.
type PushRequest struct {
	StoreTarget
	// Element is the document pushed.
	Element bson.D
	// Priority and EntryID place the element in a heapStore (the highest priority pops
	// first); a fifoStore or lifoStore takes neither.
	Priority int64
	EntryID  string
}

// Push adds an element to a fifoStore or lifoStore field of a document (appended), or to a
// heapStore field under an entry id and a priority.
func (n *Node) Push(ctx context.Context, req PushRequest, options ...WriteOption) error {
	opts, err := newWriteOptions(options)
	if err != nil {
		return err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "push")
	if err != nil {
		return err
	}
	if call.engine != store.FieldHeap && (req.Priority != 0 || req.EntryID != "") {
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"push to %s.%s.%s: Priority and EntryID place an element in a heapStore, and this field is a %s, which keeps arrival order. Nothing was written. Push without them, or push to a heapStore field",
			req.Database, req.Bucket, req.Field, call.engineType())}
	}
	raw, err := bson.Marshal(req.Element)
	if err != nil {
		return fmt.Errorf("encoding the element for %s.%s: %w", req.Bucket, req.Field, err)
	}
	var arg []byte
	if call.engine == store.FieldHeap {
		arg, err = encodeHeapPushArg(req.Priority, req.EntryID, raw)
	} else {
		arg, err = encodeElementArg(raw)
	}
	if err != nil {
		return fmt.Errorf("pushing to %s.%s: %w", req.Bucket, req.Field, err)
	}
	_, err = n.sendVerb(ctx, call, cluster.VerbPush, arg, opts)
	return err
}

// Lease is what a claim returns: the lease to ack, when it runs out and the messages it covers.
type Lease struct {
	// ID names the lease; the zero value means the field had nothing to lease.
	ID [16]byte
	// Until is the lease's deadline in Unix milliseconds.
	Until int64
	// Elements are the leased messages in claim order, each a BSON document. A retry
	// answered from the recorded entry (Deduped) returns the lease id and deadline without
	// them: the owner records only those 24 bytes (conv §18). A client that lost the first
	// reply sends the claim again with the same request id, takes the lease id from the
	// answer and reads the messages with LeaseElements (the lease_get read, 0x87) while the
	// lease is live; once the lease is acked, released or expired that read answers
	// not found and the messages are back in the field to claim again.
	Elements [][]byte
	// Deduped says the lease was recomposed from an earlier attempt of the same request.
	Deduped bool
}

// ClaimRequest leases messages of a store field.
type ClaimRequest struct {
	StoreTarget
	// Count is how many messages to lease, 1 to 65535.
	Count int
}

// claimCapture collects the messages one claim leased.
type claimCapture struct {
	mu       sync.Mutex
	elements [][]byte
}

// keep stores a copy of what a claim leased. The apply hands over slices of its snapshot,
// which stay valid only until it returns.
func (c *claimCapture) keep(elements [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.elements = c.elements[:0]
	for _, element := range elements {
		c.elements = append(c.elements, bytes.Clone(element))
	}
}

// Claim leases up to req.Count messages of a fifoStore or lifoStore field of a document
// to the caller and returns them with the lease.
func (n *Node) Claim(ctx context.Context, req ClaimRequest, options ...WriteOption) (Lease, error) {
	if err := checkClaimCount(req.Count, req.Database, req.Bucket, req.Field); err != nil {
		return Lease{}, err
	}
	opts, err := newWriteOptions(options)
	if err != nil {
		return Lease{}, err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "claim")
	if err != nil {
		return Lease{}, err
	}
	lease, _, err := n.claimField(ctx, call, req.Count, opts)
	return lease, err
}

// checkClaimCount refuses a claim size this build does not take.
func checkClaimCount(count int, db, bucket, field string) error {
	if count >= 1 && count <= math.MaxUint16 {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
		"a claim of %d messages on %s.%s.%s is not one this build takes. Nothing was leased. Claim between 1 and %d messages",
		count, db, bucket, field, math.MaxUint16)}
}

// claimField leases count messages of a resolved field under the request id in opts and
// returns the lease with the owner's answer to the write.
func (n *Node) claimField(ctx context.Context, call fieldCall, count int, opts writeOptions) (Lease, WriteAnswer, error) {
	capture := &claimCapture{}
	opts.claimSink = capture.keep
	op := call.ref.template()
	op.Class, op.Count, op.Fields = cluster.OpClaim, uint16(count), []cluster.FieldOp{{FieldID: call.fieldID}}
	answer, err := n.send(ctx, call.ref, call.encID, op, opts)
	if err != nil {
		return Lease{}, WriteAnswer{}, err
	}
	lease := Lease{Deduped: answer.Deduped}
	if len(answer.Result) == claimResultBytes {
		copy(lease.ID[:], answer.Result[:16])
		lease.Until = int64(binary.BigEndian.Uint64(answer.Result[16:]))
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.elements) > 0 {
		lease.Elements = capture.elements
	}
	return lease, answer, nil
}

// AckRequest completes a lease.
type AckRequest struct {
	StoreTarget
	// Lease is the id Claim returned.
	Lease [16]byte
}

// Ack completes a lease: the elements it covers are deleted.
func (n *Node) Ack(ctx context.Context, req AckRequest, options ...WriteOption) error {
	opts, err := newWriteOptions(options)
	if err != nil {
		return err
	}
	call, err := n.fieldCall(ctx, req.StoreTarget, "ack")
	if err != nil {
		return err
	}
	op := call.ref.template()
	op.Class, op.LeaseID, op.Fields = cluster.OpAck, req.Lease, []cluster.FieldOp{{FieldID: call.fieldID}}
	_, err = n.send(ctx, call.ref, call.encID, op, opts)
	return err
}

// Listed is one document of a List.
type Listed struct {
	// EncID is the document's encoded _id_.
	EncID []byte
	// Body is the document as BSON, _id_ first.
	Body []byte
}

const (
	listPageDocs  = 256
	listPageBytes = 4 << 20
)

// List returns up to limit documents of bucket that equal every field of filter, in
// (slot, id) order; limit 0 means all. It reads this node's slots one page at a time.
func (n *Node) List(ctx context.Context, db, bucket string, filter bson.D, limit int) ([]Listed, error) {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if len(filter) > 0 {
		if raw, err = bson.Marshal(filter); err != nil {
			return nil, fmt.Errorf("encoding the filter for %s.%s: %w", db, bucket, err)
		}
	}
	mask := ownedSlots(n.topology, ref.dbID)
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		if mask.Has(slot) {
			n.own.Acquire(slot, ref.dbID)
		}
	}
	epoch := n.topology.Committed()
	if err := n.scanner.Admit(ctx, epoch, ref.dbID, ref.entry.Decl.BucketID, &mask); err != nil {
		return nil, fmt.Errorf("listing %s.%s: %w", db, bucket, err)
	}
	var listed []Listed
	var resumeSlot uint16
	var resumeID []byte
	for {
		want := listPageDocs
		if limit > 0 {
			want = min(want, limit-len(listed))
		}
		reply, err := n.scanner.ListAdmitted(ctx, cluster.SlotListRequest{
			Epoch: epoch, DBID: ref.dbID, BucketID: ref.entry.Decl.BucketID, Filter: raw, Mask: mask,
			ResumeSlot: resumeSlot, ResumeID: resumeID, Want: want, MaxBytes: listPageBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("listing %s.%s: %w", db, bucket, err)
		}
		for _, entry := range reply.Entries {
			listed = append(listed, Listed{EncID: entry.EncID, Body: entry.Body})
		}
		if !reply.More || len(reply.Entries) == 0 || (limit > 0 && len(listed) >= limit) {
			return listed, nil
		}
		last := reply.Entries[len(reply.Entries)-1]
		resumeSlot, resumeID = last.Slot, last.EncID
	}
}

// Close stops the node and releases its files. It is safe to call twice.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		var errs []error
		n.stopReads()
		if n.bootReplayStop != nil {
			n.bootReplayStop()
			<-n.bootReplayDone
		}
		if n.retention != nil {
			n.retention.Stop()
		}
		n.retire.Close()
		errs = append(errs, n.stopCluster())
		if n.raft != nil {
			errs = append(errs, n.raft.Close())
		}
		if n.wal != nil {
			errs = append(errs, n.wal.Close())
		}
		if n.kv != nil {
			errs = append(errs, n.kv.Close())
			syncs, advances, markerWrite := n.kv.VouchedCounts()
			slog.Info(fmt.Sprintf("store: %d store syncs, %d sync-marker advances (marker writes took %.3f s in all) since this node opened.",
				syncs, advances, markerWrite.Seconds()))
		}
		if err := errors.Join(errs...); err != nil {
			n.closeErr = fmt.Errorf("closing the node in %s: %w", n.cfg.Dir, err)
		}
		storage.ClearDirectoryAdvice(filepath.Join(n.cfg.Dir, "badger"))
		storage.ClearDirectoryAdvice(filepath.Join(n.cfg.Dir, "wal"))
	})
	return n.closeErr
}

// BumpSchema commits BUMP_SCHEMA{db_id, from, to}. It returns once this node's FSM has applied
// the command, which includes the node's own publication over its pair locks (conv 20 step
// 6). A command built on a version that is no longer the committed one is refused
// SCHEMA_NOT_CURRENT, and one built on an older state_seq STATE_MOVED; neither changed anything.
func (n *Node) BumpSchema(ctx context.Context, dbID uint64, from, to int64, note cluster.BumpNote) error {
	if _, err := n.propose(ctx, cluster.CommandBumpSchema, cluster.BumpSchemaNoteBody(int64(dbID), from, to, note)); err != nil {
		return fmt.Errorf("committing the schema bump of database %d from %d to %d: %w", dbID, from, to, err)
	}
	return nil
}

// hasBucketRecord reports whether this node holds a 'b' record for the bucket, in any slot: the
// record the drop's bump wrote, which is the proof that the bucket was dropped. It reads the
// store directly because the retire sweep is not started during the boot replay.
func (n *Node) hasBucketRecord(dbID, bucketID uint64) (bool, error) {
	prefix := storage.NodeKey(storage.NSReclaim, []byte{reclaimBucketKind})
	snapshot := n.kv.NewSnapshot()
	defer snapshot.Discard()
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer iter.Close()
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		record, err := DecodeBucketKey(iter.Key())
		if err == nil && record.DBID == dbID && record.BucketID == bucketID {
			return true, nil
		}
	}
	if err := iter.Err(); err != nil {
		return false, fmt.Errorf("reading the dropped-bucket records: %w", err)
	}
	return false, nil
}
