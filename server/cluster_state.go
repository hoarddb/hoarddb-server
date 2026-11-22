package server

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// NodeUser is the username a node presents when authenticating to a peer.
// A node authenticates with the cluster token rather than a user password:
// the token is already present on every node and is exactly the credential
// that authorises cluster membership.
const NodeUser = authrules.NodeUser

const (
	// maxReplBatch caps how many entries are shipped to one peer in a single
	// REPL_PUSH frame.
	maxReplBatch = 64
	// maxReplBatchBytes caps the payload bytes of one REPL_PUSH frame, whichever
	// of the two bounds is reached first. A count alone let 64 entries of 600 KiB
	// make a 38 MiB frame, over transport.MaxFrameLength; the receiver refuses
	// such a frame, the sender retries the same batch for ever and nothing behind
	// it is delivered. An entry that is alone over the bound ships
	// alone.
	maxReplBatchBytes = 8 << 20
	// replFrameOverhead is what a REPL_PUSH frame holds besides its entries'
	// payloads -- token, node id, field names -- reserved when a request is
	// admitted, so that an entry which was accepted can always be shipped alone.
	replFrameOverhead = 4 << 10
	// replFlushInterval bounds how long the first entry of a batch waits for
	// batch-mates before being sent.
	replFlushInterval = 2 * time.Millisecond
	// replRetryInterval is how long a stalled sender first waits before it
	// tries its peer again; the wait doubles to replRetryCeiling. A failed send
	// is never dropped: its entries are in the log.
	replRetryInterval = 500 * time.Millisecond
	// peerDialTimeout bounds one node-to-node connection attempt.
	peerDialTimeout = 3 * time.Second
	// peerRetryInterval is the backoff between peer verification attempts.
	peerRetryInterval = 2 * time.Second
	// entryQueueDepth is how many un-shipped entries a peer's queue holds
	// before its stream stalls and the sender reads them from the log instead.
	// Bounding it is what keeps a slow replica from consuming unbounded memory;
	// a full queue never makes a writer wait.
	entryQueueDepth = 1024
)

// replEntryBytes is what an entry adds to the frame that carries it.
func replEntryBytes(entry ReplEntry) int {
	return len(entry.Payload) + len(entry.Key) + len(entry.Bucket)
}

// replBatchEnd returns where the batch that starts at entries[start] ends: at
// most maxReplBatch entries and maxReplBatchBytes of payload, and at least one
// entry, so that an entry over the byte bound is sent alone and not never.
func replBatchEnd(entries []ReplEntry, start int) int {
	end := start
	batchBytes := 0
	for end < len(entries) && end-start < maxReplBatch {
		size := replEntryBytes(entries[end])
		if end > start && batchBytes+size > maxReplBatchBytes {
			break
		}
		batchBytes += size
		end++
	}
	return end
}

// Cluster holds ring placement, peer connections and replication state for a
// multi-node hoardDB deployment.
//
// A Server with a nil Cluster is a single node and takes exactly the code path
// it always has: no WAL, no routing, no replication.
type Cluster struct {
	s         *Server
	logger    *slog.Logger
	self      ring.Node
	rf        int
	wcTimeout time.Duration
	// wcOverride is the operator's configured durability level, or zero when
	// they set none. It is not a derived default: the default is resolved per
	// bucket by defaultWriteConcern, because the node's factor says nothing
	// about how many copies a given bucket asked for.
	wcOverride WriteConcernLevel

	// originFormatNoted is set once this process has checked the data
	// directory's rollback notice (wal_format_notice.go), so an append pays for
	// the check once and not on every write.
	originFormatNoted atomic.Bool

	viewMu sync.RWMutex
	r      *ring.Ring
	// bucketFactors is the replication factor each bucket declared, by
	// placement key, for the buckets that declared one; a bucket absent from it
	// inherits rf. It is guarded by viewMu and mutated in place — a copy per
	// DDL would make creating 10^6 buckets quadratic — and it is filled
	// where a bucket becomes served (registerBucketFactor), never lazily: it
	// must be complete before Start, or start-time relocation takes a replica
	// of a bucket declared above rf for a misplaced one and releases its copy.
	bucketFactors map[placementKey]int32

	// instance is this process's instance nonce, drawn at start and sent on
	// every gossip exchange; instances is what this node has seen of other
	// processes' nonces (node_instance.go, BUG-0600).
	instance  []byte
	instances instanceTracker

	// featuresMu guards memberFeatures: what each member advertised on the
	// last gossip exchange it took part in, by node ID (cluster_features.go).
	featuresMu     sync.RWMutex
	memberFeatures map[string]map[string]bool
	// memtableMu guards peerMemtables: each gossiping peer whose memtable
	// differs from this node's, at the size it last said, so a mismatch is
	// logged once per change and listed in status.
	memtableMu    sync.Mutex
	peerMemtables map[string]int
	// batchStallLogged and batchStallBucket are the last entry this node
	// refused as too large for its memtable, so the owner's re-sends log it
	// once (guarded by memtableMu).
	batchStallLogged uint64
	batchStallBucket string
	// batchSizeRefusals is each source's entry this node refused as too large for
	// its store, as recorded durably (guarded by memtableMu).
	batchSizeRefusals map[string]batchSizeRefusal
	// divergences is each source's entry this node refused for a reason its
	// owner did not share, loaded at start and written synced before the
	// refusal is answered (divergence.go). divergenceMu is a leaf.
	divergenceMu   sync.Mutex
	divergences    map[string]divergenceRecord
	divergedLogged map[divergedPair]struct{}
	// heldIDLogged is noteHeldID's once per bucket and sender, guarded by
	// divergenceMu.
	heldIDLogged map[divergedPair]struct{}

	// sweepGeneration numbers the rebalance sweeps, so a retrying sweep gives
	// way to a newer one instead of racing it (migrate.go: sweepUntilSettled).
	sweepGeneration atomic.Uint64

	// residue is what the newest sweep generation could not settle, and the
	// epoch it swept, under sweepLeftMu: only that generation writes it
	// (recordResidue), and the sweep is re-armed for exactly these buckets.
	// orphans, under the same lock, is each create this node logged as owner
	// that a membership change overtook, by the epoch it was decided at: one is
	// registered from the create's view of ownership to its re-check, and kept
	// if the re-check found the bucket moved (join-freeze spec §8.1, review
	// F-11). Normally empty. fullSweepGeneration is the generation of the
	// newest full sweep, stored with that generation under the same lock
	// (claimFullSweep): a rearm starts only once that sweep has recorded, so it
	// never supersedes one. rearmRunning makes the rearm single-flight.
	// sweepRearms counts the re-armed sweeps that actually started.
	//
	// demoted, under the same lock, is each copy this node holds that a
	// decrease took out of its bucket's set (bucket-replication-rev8-spec
	// §8.3): rearmDemoted works it until the copy is released, and it is kept
	// out of the residue, the settle mark and the hand-off counts, so a copy
	// that waits on a down survivor never holds the join freeze. In memory
	// only: start-time relocation releases what a restart forgot. Normally
	// empty. lastDemotedAt (same lock) and demotedRunning pace and
	// single-flight its pass apart from the rearm's.
	sweepLeftMu         sync.Mutex
	residue             sweepResidue
	handOff             *HandOffProgress
	orphanCopies        map[heldBucket][]PlacementToCopy
	released            releaseCount
	orphans             map[heldBucket]uint64
	demoted             map[heldBucket]struct{}
	lastDemotedAt       time.Time
	demotedRunning      atomic.Bool
	lastRearmAt         time.Time
	fullSweepGeneration atomic.Uint64
	rearmRunning        atomic.Bool
	sweepRearms         atomic.Int64
	// retrying is the list a sweep generation is retrying now (retrySettling),
	// under sweepLeftMu, so that a drop can take its name off it (R8-C).
	retrying sweepResidue
	// roundsActive counts the sweep rounds between their start and their last
	// record of what they left, and droppedInRound is each name a drop took off
	// the lists in that time, numbered by dropSerial (forgetPendingName). A
	// round's lists are older than a drop that landed while it ran, so it
	// leaves those names out when it writes them (BUG-0659), unless a bucket
	// of another id has attached under the name since (BUG-0662). All three under
	// sweepLeftMu; the map is nil with no round active, so nothing is kept at
	// rest and a membership change adds no work.
	roundsActive   int
	dropSerial     uint64
	droppedInRound map[heldBucket]droppedNote

	// tombstoneSweepAt is when the drop-record sweep last ran, and
	// dropRecordsMaybe whether this node may hold a drop record: true at start
	// and whenever one is written, false once a sweep found none left
	// (drop_record.go). Both keep the sweep off a node at rest; nothing that
	// answers a question reads them.
	tombstoneSweepAt  atomic.Int64
	dropRecordsMaybe  atomic.Bool
	tombstoneSweeping atomic.Bool

	// settled is this node's own Settled epoch (join-freeze spec §3.1): the
	// highest epoch whose newest sweep generation ended with nothing left, on
	// that epoch, with no orphan of an earlier one. It only rises (markSettled),
	// and it is persisted in the placement pass record, under passRecordMu,
	// which orders the pass's own write of that record against markSettled's.
	settled      atomic.Uint64
	passRecordMu sync.Mutex

	// unsettled is the placement marks of the buckets whose placement changed
	// for this node and has not settled (placement_pass.go), by placement key:
	// nothing at rest. passFinished is the epoch the last placement pass
	// finished for, and passRunning closes when the running pass finishes.
	// placementMu guards the three and is a leaf.
	placementMu sync.Mutex
	unsettled   map[placementKey]placementMarks
	// marksWriteMu serialises every writer of a bucket's 'h' record that merges
	// into the marks it holds, from its read of them to their publish: the
	// placement pass's batches, writePlacementMarks, an alter's outside mark
	// and the backstop's. Without it a pass that read the marks before another
	// writer recorded one committed its older merge over it (BUG-0589). It is
	// taken after a bucket's fence and its record's lock, and before
	// placementMu, passRecordMu and viewMu.
	marksWriteMu sync.Mutex
	passFinished uint64
	passRunning  chan struct{}
	// unsettledCount and passFinishedAt mirror len(unsettled) and
	// passFinished, written under placementMu (noteUnsettledLocked), so the
	// check every write makes inside its fence (step 10b.3) reads two atomics
	// and takes the mutex only when something is unsettled.
	unsettledCount atomic.Int64
	passFinishedAt atomic.Uint64

	// awaiting is the owner's awaiting records of the factor alter
	// (bucket_rf_alter.go), by placement key: the members an increase newly
	// placed whose copy is not seeded yet. Nothing at rest; the 'a' records are
	// loaded at start, after the WAL replay. awaitingMu guards it and is a
	// leaf, never held across a store write or a call into the cluster;
	// awaitingCount mirrors len(awaiting), written under it, so a reader takes
	// the mutex only when something is pending.
	awaitingMu    sync.Mutex
	awaiting      map[placementKey]map[string]awaitingCopy
	awaitingCount atomic.Int64
	// pendingCopies is the member's pending-copy worker's queue
	// (bucket_rf_worker.go): one entry per needs-a-copy record, filled at
	// start after the WAL replay.
	pendingCopies pendingCopyQueue
	// underReplicated is the owner's count of buckets with fewer copies in sync
	// than their factor (bucket_rf_gauge.go).
	underReplicated underReplicatedGauge

	// streamChanged is closed and replaced whenever any peer's stream changes
	// state (stream_state.go), so a waiter re-reads the states it cares about.
	// streamChangedMu guards it and is taken under a peer's streamMu.
	streamChangedMu sync.Mutex
	streamChanged   chan struct{}

	peersMu sync.RWMutex
	peers   map[string]*peer
	// peerStarted tracks which peer IDs have their runSender/verifyPeer
	// goroutines running, guarded by peersMu. Start() starts them for
	// boot-time members; adoptMembership starts them for members adopted
	// later (a node joined after boot), so the two paths share one spawn
	// point (ensurePeerRunning) and can never double-spawn.
	peerStarted map[string]bool

	tokenMu sync.RWMutex
	token   ClusterToken
	// tokenFile is the token file as it was when token was read from it. A
	// token that does not match is checked against the file only when the
	// file has changed since (ValidateToken).
	tokenFile tokenFileStamp

	// knownServers records peers whose certificate verified against a pinned
	// fingerprint, keyed by internode address, so a later dial does not need
	// the operator's list. fingerprints is the anchor itself: every internode
	// dial is verified against it and an unpinned peer is refused.
	knownServers *transport.KnownServers
	fingerprints *transport.FingerprintSet

	// appliedMu guards appliedFrom, seededThrough and legacySeeded.
	// appliedFrom is the highest sequence this node has applied per source
	// node — the deduplication guard for re-sent entries. seededThrough is,
	// per source and per (database, bucket), the sequence a snapshot pulled
	// onto this node was taken at: every entry of that bucket from that source
	// at or below it is contents this node already holds, not work to apply
	//. legacySeeded is, per source and bucket name, such a position
	// loaded from a state file written before positions named their database;
	// it covers every database's bucket of that name.
	appliedMu     sync.Mutex
	appliedFrom   map[string]uint64
	seededThrough map[string]seedFloors
	legacySeeded  map[string]map[string]uint64
	// rebasedAt is, per source, this node's own last log sequence when that
	// source's position was last rebased: log entries of the source at or below
	// it are not counted when a position is rebuilt from the log. seedCeiling
	// is the highest snapshot position held for a source, so a checkpoint knows
	// without a scan whether every one has been passed. Both are guarded by
	// appliedMu.
	rebasedAt   map[string]uint64
	seedCeiling map[string]uint64
	// startHold holds this node's own writes after a start until a majority
	// of its peers has said where its log stands (start_hold.go).
	startHold *startHold
	// recordedStream is, per source, the incarnation and log id of the
	// source's log that this node's position for it was learned under
	// (stream_handshake.go). Guarded by appliedMu.
	recordedStream map[string]recordedStream
	// snapshotCaps is, per source, the pulls in flight from it and how far
	// each one's snapshot position is still valid after the rebases that ran
	// since it read the source's incarnation (snapshot_cap.go). Only pulls
	// in flight are held, never a bucket at rest. Guarded by appliedMu.
	snapshotCaps map[string][]*snapshotCap
	// streamFromMu serialises, per source, every push of that source's stream:
	// a rebase moves a position down, and a push still in flight from before
	// it must not move it back up between the rebase's read and its write.
	// Guarded by streamFromMuLock; one mutex per member that ever streamed here.
	streamFromMuLock sync.Mutex
	streamFromMu     map[string]*sync.Mutex
	// checkpointOwed is, per source, the highest position moved by a skip under a
	// snapshot position that no committed checkpoint covers yet. Guarded by
	// appliedMu.
	checkpointOwed map[string]uint64
	// orphanVoids is, per source, the entries of its stream this node logged
	// past a stopped run's entry and never applied (replica_orphans.go), and
	// orphanVoidOwed the sources whose void is in memory and not yet in a
	// synced checkpoint. Both are guarded by appliedMu.
	orphanVoids    map[string]orphanVoid
	orphanVoidOwed map[string]struct{}
	// positionMu serialises position checkpoints end to end, from reading the
	// positions to committing them, so an older checkpoint cannot land on top
	// of a newer one. It is taken before appliedMu and never held across the
	// network.
	positionMu sync.Mutex
	// skipStopCheckpoint makes Stop leave the positions as the last checkpoint
	// and the log have them, which is what a killed node does.
	skipStopCheckpoint bool
	// beforeCheckpointCommit runs in checkpointPositions after it has read the
	// store and before it commits, so a test can land a write in that window.
	beforeCheckpointCommit func()

	// shipOrder makes shipped entries reach each peer's queue in sequence
	// order, which the server's apply fences do not.
	shipOrder *shipOrder

	// stranded counts the entries replays pass over because ownership of their
	// bucket moved, once each (stranded.go). Its zero value is ready to use.
	stranded strandedLedger
	// entriesOfOlderTerm counts the entries this node, as a receiver, passed
	// over because they were of a term below its copy's (lineage_rule.go): the
	// receiver's tally of the sender's stranded entries.
	entriesOfOlderTerm atomic.Int64

	// retentionHeldWarnEvery is how often a retention pass may repeat its
	// warning that a member holds the log (retention.go). A test shortens it.
	// retentionWarnMu guards retentionWarnedAt, when it last warned; zero
	// until the first warning of this process.
	retentionHeldWarnEvery time.Duration
	retentionWarnMu        sync.Mutex
	retentionWarnedAt      time.Time

	// metrics is the registry of the replication gauges, computed at scrape
	// over this node's members (replication_metrics.go). /metrics serves it
	// beside the default registry.
	metrics *prometheus.Registry
	// membershipChanges counts the membership changes this node proposed, by
	// outcome (membership_change.go).
	membershipChanges *prometheus.CounterVec
	// onDemand is what the on-demand read records (takeover.go): two
	// aggregate series, never one per bucket.
	onDemand onDemandMetrics
	// onDemandInternodeWaiting counts the forwarded reads waiting here for a
	// take-over, against onDemandInternodeWaiters (takeover.go).
	onDemandInternodeWaiting atomic.Int64

	// unapplied holds WAL retention below every entry appended and not yet
	// applied. Its zero value is ready to use.
	unapplied unappliedMark

	// mismatch is set when a peer reports a member set that disagrees with
	// ours. Placement would differ between nodes, which risks splitting
	// writes across inconsistent replica sets, so writes fail closed. With
	// membership epochs (§6.1 of the rebalance design) a mismatch is a
	// resync signal, not a permanent state: reconcileMembership clears it by
	// adopting the newer epoch when the peer is trustworthy.
	mismatch atomic.Bool

	// epoch and members are the authoritative membership view, which the ring
	// r is built from; viewMu guards all three, so a reader sees a ring with
	// the epoch it belongs to (ownerView).
	epoch   uint64
	members []ring.Node

	// removedAt is the epoch of the first membership this node saw that does
	// not contain it, or 0 (step 6.5b): a removed node refuses every write.
	// removedConnectTo names a member a client is told to connect to instead,
	// guarded by viewMu. Both are restored at start from the 'x' record
	// (removed_node.go).
	removedAt        atomic.Uint64
	removedConnectTo string
	// removedHandOffRunning is set while a removed node's pass naming the
	// buckets it holds to their sets runs (removed_handoff.go), so one runs at
	// a time.
	removedHandOffRunning atomic.Bool
	// removedPass is what that pass tells a member that probes this removed
	// node (removed_handoff.go): its notice's epoch, a count per owner of the
	// buckets still pending, and the members to tell again. O(members),
	// never per bucket. Guarded by removedPassMu, a leaf.
	removedPassMu sync.Mutex
	removedPass   removedPassLevel
	// removedHandOffs is what this member heard of each removed node's
	// hand-off (removed_settle.go): one entry per removed node, never per
	// bucket. Guarded by removedHandOffMu, a leaf.
	removedHandOffMu sync.Mutex
	removedHandOffs  map[string]*removedHandOff

	// history is the member set of each of the last epochs this node adopted,
	// and ringsAt the rings built from them on demand (membership_history.go).
	// Guarded by historyMu, a leaf.
	historyMu sync.Mutex
	history   map[uint64][]ring.Node
	ringsAt   map[uint64]*ring.Ring
	// historyNamedAnotherNode is set, and never cleared, once the history
	// holds a member set naming a node other than this one. A node removed
	// within the history is such a node, so while it is false
	// aloneAfterARemoval answers false on one atomic load: the one-member
	// door asks it on every request (BUG-0630), and a node that was always
	// alone pays nothing more.
	historyNamedAnotherNode atomic.Bool
	// viewChanges counts the changes of the view (ring, epoch, members, the
	// removal mark) and of the history, each counted after it is made, and
	// loneAnswer is aloneAfterARemoval's answer with the count it was
	// computed at: an answer whose count is not the current one is computed
	// again (BUG-0630).
	viewChanges atomic.Uint64
	loneAnswer  atomic.Pointer[aloneAnswer]

	// gossipWake is closed and replaced to make every verifyPeer loop gossip
	// now instead of at its next timer (spec §19.3.7): an epoch refusal on
	// either side closes it, so a member that is behind learns the epoch in a
	// round trip. Guarded by gossipWakeMu; lastGossipWake bounds how often,
	// and gossipWakeDeferred says a wake inside that bound is scheduled.
	gossipWakeMu       sync.Mutex
	gossipWake         chan struct{}
	lastGossipWake     time.Time
	gossipWakeDeferred bool

	// missedLoggedAt is when this node last said it missed more membership
	// changes than the cluster remembers, in Unix nanoseconds (step 9.4).
	missedLoggedAt atomic.Int64

	// transition is set while this node proposes a membership change: one
	// proposal at a time per node, an operator's or a recovery's. A
	// concurrent proposal is rejected with ErrEpochInProgress, never queued.
	// It does not stop this node answering other members' changes.
	transition atomic.Bool
	// oldMembershipPeers is the members that answered a membership change
	// with an error frame, each logged once.
	oldMembershipPeers sync.Map

	// adoptMu serialises every adoption of an epoch (adoptMembership), and a
	// catch-up on missed epochs (adoptInOrder) holds it throughout. Every
	// peer's gossip, answers and requests alike, can find this node behind at
	// the same moment, and one epoch's placement pass must have started before
	// another catch-up reads whether there is a pass to wait for; a
	// vote or a commit adopts under it too, forward only.
	adoptMu sync.Mutex

	// sealedPeers is what each peer has said about its own store: whether it is
	// encrypted at rest. A sealed node hands a bucket's data only to a peer
	// that has said so (sealed_peers.go). membershipFileMu serialises every
	// write of membership.json, which carries the sealed set beside the epoch.
	sealedPeers *sealedPeerTable
	// peerDrivers is where each peer has said clients reach it (peer_drivers.go).
	peerDrivers      *peerDriverTable
	membershipFileMu sync.Mutex
	// pending is this node's promise and acceptance for the next epoch
	// (membership_paxos.go), as membership.json records it. Guarded by
	// membershipFileMu: it is read and written only with the record.
	pending *pendingChange
	// restoredAcceptance says NewCluster restored an acceptance this node
	// had not adopted, so the recovery that finishes it arms at start.
	restoredAcceptance bool
	// recoveryArm wakes the recovery goroutine with the time of an
	// acceptance (membership_recovery.go). One slot: an arm never blocks.
	recoveryArm chan time.Time

	// joinedAt is the epoch at which this node learnt each current member
	// joined, 0 (absent) for a founder, and of a joiner removed before its
	// join settled (deriveJoinedAt); persisted in membership.json (join-freeze
	// spec §8.3). The map is never changed in place: an adoption, under
	// membershipFileMu, stores a new one, so the report reads it with no lock.
	joinedAt atomic.Pointer[map[string]uint64]

	// joinFreeze is the published report on whether schema changes are
	// paused (join-freeze spec §8.4), replaced whole by recomputeJoinFreeze,
	// which recomputeMu serialises; joinFreezeRecomputedAt rate-limits the
	// recomputes a progress count asks for, and handOffHeardStale says a
	// member's hand-off heard on gossip changed since the last recompute, so a
	// rate-limited skip is made up at the next gossip.
	joinFreeze             atomic.Pointer[joinFreezeReport]
	recomputeMu            sync.Mutex
	joinFreezeRecomputedAt atomic.Int64
	handOffHeardStale      atomic.Bool

	// settledOf is the best Settled epoch this node has heard for each other
	// member, guarded by settleMu, a leaf: nothing is taken under it but
	// viewMu's read lock, a peer's livenessMu and sweepLeftMu, and it is never
	// taken under membershipFileMu (join-freeze spec §8.2, §12).
	settleMu  sync.Mutex
	settledOf map[string]uint64
	// handOffOf is each other member's hand-off progress as it last said it
	// on gossip, guarded by settleMu (join-freeze spec §8.5).
	handOffOf map[string]HandOffProgress

	// bootstrapped is set while this node's member list comes only from its
	// seed settings and no member has confirmed it: a node started for joining,
	// or a founder of a new cluster before every founder has answered and
	// agreed (join-freeze spec §5.4, §16 F-A). While set the node serves no request that
	// names a database or bucket.
	bootstrapped atomic.Bool

	// viewAgreed is the members of this node's view that have agreed with it
	// on epoch and member set since boot, viewAgreedConfirmed those of them
	// that were not bootstrapped themselves, and viewDisputed is set once any
	// member of the view has answered anything else (noteViewAgreement says
	// when the mark clears).
	// Memory only, dropped when the mark clears: a restart while bootstrapped
	// asks again, which is the safe side. nonMemberViewLoggedAt rate-limits
	// the note about a non-member's view. viewConfirmMu, a leaf, guards these
	// and membershipRefusals, and the decision to clear bootstrapped.
	viewConfirmMu         sync.Mutex
	viewAgreed            map[string]struct{}
	viewAgreedConfirmed   map[string]struct{}
	viewDisputed          atomic.Bool
	nonMemberViewLoggedAt map[string]time.Time
	// membershipRefusals is, while bootstrapped, each member this node will
	// not exchange membership with and why (an encryption-mode mismatch,
	// sealed_peers.go:joinRefusal): such a member never agrees, so the node is
	// never confirmed, and it says so instead of asking for 'join'.
	membershipRefusals map[string]error

	// migrateMu guards migratePending: snapshots produced for in-flight bucket
	// migrations, keyed by database+"\x00"+bucket and then by the pulling node. A target pulls a bucket by
	// MigrateStart/Data/Done over the peer session; the source holds the
	// snapshot bytes here until the target verifies (Done) and clears it.
	// migrateActive, under the same lock, tracks an in-flight pull per bucket
	// with a channel closed when it completes, so a notify storm cannot start
	// two pulls of one bucket and concurrent callers wait for the real outcome
	// instead of assuming success.
	migrateMu sync.Mutex
	// databaseDDLMu is held by the node a client reached from the moment it
	// reads a database's incarnations to stamp a create or drop of it until
	// that entry is logged and applied here, so two creates through this node
	// cannot both find the database absent and mint two incarnations.
	databaseDDLMu  sync.Mutex
	migratePending map[string]map[string]*pendingSnapshot
	migrateActive  map[string]chan struct{}
	// pulls, under the same lock, records every pull this node is running as
	// the target, by migrateKey, so a drop of the name can cancel it and the
	// pull, re-checking under its shared fence hold, discards its stream
	// instead of bringing the dropped name back (spec §5.3, N-11).
	pulls map[string]map[*pullRecord]struct{}
	// replacingPulls counts the pulls among pulls that replace a copy this
	// node holds (pullRecord.replacing), so refuseUnservable, which every
	// replicated entry passes, reads one atomic while none runs.
	replacingPulls atomic.Int64
	// landing, under the same lock, holds by migrateKey a channel for every
	// pull that has recorded its snapshot position and is putting its copy in
	// place, closed when it has (beginLanding). landingPulls counts them, so a
	// stream entry reads one atomic while none lands (BUG-0729).
	landing      map[string]map[chan struct{}]struct{}
	landingPulls atomic.Int64

	// takeOver is the take-over worker's bookkeeping (takeover.go): the
	// buckets named to it, and the status of each take-over in progress.
	// takeOverCopies counts the whole copies it made, takeOverFallbacks the
	// take-overs that settled by the fallback, without the previous owner,
	// entriesReconciled the entries this node applied from a sender that is
	// not the bucket's owner while the bucket carried a hand-off mark — the
	// hand-off collectors (replication_metrics.go) — and divergencesSeen the
	// take-overs it found two live incarnations for.
	takeOver          takeOverState
	takeOverCopies    atomic.Int64
	takeOverFallbacks atomic.Int64
	entriesReconciled atomic.Int64
	divergencesSeen   atomic.Int64
	// diskRefusals counts the copies refused since the last disk-refusal
	// WARN, and diskRefusalWarnedAt is when that WARN was last written, in
	// Unix nanoseconds (disk_headroom.go:refuseLandingOnFullDisk).
	diskRefusals        atomic.Int64
	diskRefusalWarnedAt atomic.Int64
	// releaseWait is how long this node's copies outside their sets have
	// waited to be released, for the WARN of A-10.8 (release_wait.go).
	releaseWait releaseWaitState

	ctx    context.Context
	cancel context.CancelFunc
	// storeWG is the goroutines that read or write the node store or the log,
	// waited without a bound by Stop because Server.Stop closes both right
	// after. That includes the membership recovery, the auth-config loop and
	// the peer verifiers: each adopts a membership or writes the auth config,
	// and each runs on ctx, which Stop cancels first (BUG-0680).
	// storeMu orders every storeWG.Add against Stop, which sets storeStopping
	// before it waits, so no goroutine starts after the wait has returned.
	storeWG       sync.WaitGroup
	storeMu       sync.Mutex
	storeStopping bool

	// startCalled is set when Start begins, and memberWorkers starts the
	// workers every member runs (startMemberWorkers) once: at Start on a node
	// with other members, or at the adoption that gives a node started alone
	// its first one (BUG-0498).
	startCalled   atomic.Bool
	memberWorkers sync.Once
	// takeOverWorker starts the take-over worker once: with the member
	// workers, or alone at Start on a node left alone by a removal
	// (BUG-0502), which a later adoption must not start a second time.
	takeOverWorker sync.Once

	// authPushCh wakes the auth-config replication loop after a local
	// mutation so the change propagates immediately; the loop also retries on
	// the configured interval (spec 5.3.3). Buffered depth 1: a pending wake
	// covers any number of mutations.
	authPushCh chan struct{}
}

// NewCluster builds cluster state for a multi-node server. Membership is the
// union of the configured seed nodes and this node; every node must arrive at
// the same member set or placement would disagree (verified at join time).
func NewCluster(s *Server, cfg *config.Config, logger *slog.Logger) (*Cluster, error) {
	if s == nil {
		return nil, fmt.Errorf("cluster: nil server")
	}
	if cfg == nil {
		return nil, fmt.Errorf("cluster: nil config")
	}
	if logger == nil {
		logger = slog.Default()
	}

	rf := cfg.Server.ReplicationFactor
	if rf < 1 {
		rf = 1
	}

	self, err := selfNode(cfg)
	if err != nil {
		return nil, err
	}

	// Membership is the persisted epoch record for a node that has already
	// joined and rebalanced; configuration seed nodes are the bootstrap for a
	// fresh data directory at epoch 1. A persisted member set is authoritative
	// over seed nodes, so a long-lived node cannot be silently regressed by a
	// stale config after a cluster shape change.
	membershipPath := membershipPathFor(cfg.Storage.DataDir)
	persisted, found, err := readMembershipRecord(membershipPath)
	if err != nil {
		return nil, err
	}
	epoch, members := persisted.Epoch, persisted.Members
	if found {
		if clash, clashed := findMemberIDClash(members); clashed {
			return nil, fmt.Errorf("cluster: the member set in %s lists node id %q at two addresses, %s and %s; "+
				"two nodes under one id would both own its buckets and both act as the auth master. "+
				"On the node that should not be %q, remove membership.json and start it with its own HOARDDB_NODE_ID, "+
				"listed once in HOARDDB_SEED_NODES as <node-id>@<address>",
				membershipPath, clash.id, clash.firstAddr, clash.secondAddr, clash.id)
		}
		self, err = locateSelf(members, self, cfg.Server.Listen, membershipPath)
		if err != nil {
			return nil, err
		}
		if err := refuseSavedPhantomSelf(cfg.Server.Listen, self, members, membershipPath); err != nil {
			return nil, err
		}
	} else {
		var implicit bool
		members, self, implicit, err = memberList(cfg, self)
		if err != nil {
			return nil, err
		}
		if implicit {
			logger.Warn("this node was not listed in the seed nodes; "+
				"membership is implicit and all nodes must still agree on the member set",
				"node_id", self.ID, "addr", self.Addr)
		}
		epoch = InitialEpoch
	}

	tokenFile, err := statTokenFile(cfg.Storage.DataDir)
	if err != nil {
		return nil, fmt.Errorf("cluster: load cluster token: %w", err)
	}
	tok, err := s.credentials.LoadClusterToken(cfg.Storage.DataDir)
	if err != nil {
		return nil, fmt.Errorf("cluster: load cluster token: %w", err)
	}
	if tok == nil || tok.Token == "" {
		return nil, fmt.Errorf("cluster: cluster token is empty; the cluster.token in the data directory has no token field")
	}

	// Only an explicit setting is recorded. Deriving a node-wide default from
	// the node's factor here gave a bucket declared at 3 the concern `one` on a
	// node started with seeds and a factor of 1.
	var wcOverride WriteConcernLevel
	if cfg.Server.WriteConcern != "" {
		wcOverride, err = ParseWriteConcern(cfg.Server.WriteConcern)
		if err != nil {
			return nil, fmt.Errorf("cluster: HOARDDB_WRITE_CONCERN: %w", err)
		}
	}

	// Peers are pinned, not trusted on first sight: the data directory holds
	// the fingerprints learned from verified connections, and the operator's
	// seed_fingerprints are the anchor for the first one. These are
	// built before the cluster context so no error path can leak it.
	knownServers, err := transport.NewKnownServers(filepath.Join(cfg.Storage.DataDir, "cluster", "known_servers"))
	if err != nil {
		return nil, fmt.Errorf("cluster: load known_servers: %w", err)
	}
	fingerprints, err := transport.NewFingerprintSet(cfg.HashRing.SeedFingerprints, knownServers)
	if err != nil {
		return nil, fmt.Errorf("cluster: seed_fingerprints: %w", err)
	}

	// The cluster context exists from construction, not from Start(): a
	// node that boots alone is !Enabled at Start (no senders spawned then),
	// but a later join adopts members and starts their verifyPeer/sender
	// goroutines — those must find a live context, or the first thing they
	// check panics on nil.
	instance, err := newInstanceNonce()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		s:                 s,
		logger:            logger,
		self:              self,
		rf:                rf,
		wcOverride:        wcOverride,
		wcTimeout:         DefaultWriteConcernTimeout,
		streamChanged:     make(chan struct{}),
		gossipWake:        make(chan struct{}),
		peers:             make(map[string]*peer),
		peerStarted:       make(map[string]bool),
		bucketFactors:     make(map[placementKey]int32),
		unsettled:         make(map[placementKey]placementMarks),
		awaiting:          make(map[placementKey]map[string]awaitingCopy),
		pendingCopies:     newPendingCopyQueue(),
		underReplicated:   newUnderReplicatedGauge(),
		memberFeatures:    make(map[string]map[string]bool),
		peerMemtables:     make(map[string]int),
		token:             *tok,
		tokenFile:         tokenFile,
		knownServers:      knownServers,
		fingerprints:      fingerprints,
		appliedFrom:       make(map[string]uint64),
		batchSizeRefusals: make(map[string]batchSizeRefusal),
		divergences:       make(map[string]divergenceRecord),
		seededThrough:     make(map[string]seedFloors),
		legacySeeded:      make(map[string]map[string]uint64),
		rebasedAt:         make(map[string]uint64),
		seedCeiling:       make(map[string]uint64),
		recordedStream:    make(map[string]recordedStream),
		snapshotCaps:      make(map[string][]*snapshotCap),
		streamFromMu:      make(map[string]*sync.Mutex),
		checkpointOwed:    make(map[string]uint64),
		orphanVoids:       make(map[string]orphanVoid),
		orphanVoidOwed:    make(map[string]struct{}),
		shipOrder:         newShipOrder(),
		epoch:             epoch,
		members:           members,
		migratePending:    make(map[string]map[string]*pendingSnapshot),
		migrateActive:     make(map[string]chan struct{}),
		pulls:             make(map[string]map[*pullRecord]struct{}),
		landing:           make(map[string]map[chan struct{}]struct{}),
		takeOver:          newTakeOverState(),
		sealedPeers:       newSealedPeerTable(persisted.SealedPeers),
		peerDrivers:       newPeerDriverTable(),

		settledOf:   persisted.SettledOf,
		authPushCh:  make(chan struct{}, 1),
		recoveryArm: make(chan time.Time, 1),
		ctx:         ctx,
		cancel:      cancel,
		instance:    instance,
		instances:   instanceTracker{selfID: self.ID, own: string(instance)},
	}

	c.startHold = newStartHold(c)
	joinedAt := persisted.JoinedAt
	c.joinedAt.Store(&joinedAt)

	// Rebuild the placement ring from the member set. Fail fast if our own
	// identity is not on it: otherwise this node would never consider itself
	// the owner of anything, and every write would be forwarded to a peer
	// instead of being placed here.
	if err := c.rebuildRing(members); err != nil {
		return nil, err
	}
	// A node that recorded its removal starts removed, on the membership it
	// was last shown (step 9.3).
	removal, removed, err := readRemovalRecord(s.kv)
	if err != nil {
		return nil, err
	}
	if removed {
		if err := c.startRemoved(removal); err != nil {
			return nil, err
		}
	}
	c.restorePending(persisted.Pending, removed)

	c.retentionHeldWarnEvery = retentionHeldWarnInterval
	c.onDemand = newOnDemandMetrics()
	if c.metrics, err = newReplicationMetrics(c); err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	if found {
		c.bootstrapped.Store(persisted.Bootstrapped)
	} else {
		// A seed set of this node alone is a single-node cluster, which no
		// member could ever confirm: it is never bootstrapped.
		c.bootstrapped.Store(len(members) > 1)
		if err := saveMembershipRecord(membershipPathFor(cfg.Storage.DataDir), membershipRecordOf(membershipState{
			epoch:        epoch,
			members:      members,
			bootstrapped: c.bootstrapped.Load(),
		})); err != nil {
			return nil, err
		}
	}

	if len(members) < rf {
		logger.Warn("replica set is incomplete: fewer cluster members than the replication factor, "+
			"so writes needing more copies than exist will fail honestly rather than pretend",
			"members", len(members), "replication_factor", rf)
	}

	// A 2-member cluster can permanently lose the ability to change membership
	// at all: a join or removal needs a majority of the *current* member count
	// (commitMembershipChange), and a majority of 2 is 2 — so if either node
	// dies, the survivor can never again add, remove, or replace a member,
	// including the dead one, without that dead node coming back. This is the
	// same quorum-loss property every majority-consensus system has (Raft
	// included); it is not a bug to fix here, but it is easy to back into
	// without realizing it, so say so loudly at every startup while the
	// cluster remains in this shape — including after a removal has shrunk a
	// larger cluster down to two, not just at first bootstrap.
	if len(members) == 2 {
		logger.Warn("this cluster has exactly two members: losing either node permanently blocks all future membership changes "+
			"(a majority of 2 is 2 — the survivor could never remove, replace, or add a node without the dead one coming back). "+
			"Use an odd member count of three or more for production, or add a third node now",
			"members", len(members))
	}

	// Restore what this node has already applied from each source. Without it a
	// restart forgets, and a sender whose own position reset re-sends entries the
	// node already holds — which duplicates elements in a FIFO or LIFO bucket.
	if err := c.loadPositions(); err != nil {
		return nil, err
	}
	if err := c.loadMembershipHistory(); err != nil {
		return nil, err
	}
	if err := c.loadPlacementMarks(); err != nil {
		return nil, err
	}
	// Unknown until the first drop-record sweep looks.
	c.dropRecordsMaybe.Store(true)
	// A node that founds a cluster of itself alone is confirmed at once, and
	// is settled at the first epoch as every founder is (owner ruling 52).
	if !found && len(members) == 1 {
		c.settleAtFounding(epoch)
	}
	// The listeners open before Start, so the first report is made here: a
	// node with none would refuse every schema change as "membership changed"
	// (review F-15).
	c.recomputeJoinFreeze()

	logger.Info("cluster initialised",
		"node_id", self.ID,
		"addr", self.Addr,
		"members", len(members),
		"epoch", epoch,
		"replication_factor", rf,
		"write_concern", writeConcernDescription(wcOverride),
	)
	return c, nil
}

// selfNode derives this node's ring identity from configuration.
func selfNode(cfg *config.Config) (ring.Node, error) {
	addr := strings.TrimSpace(cfg.Server.Listen)
	if addr == "" {
		return ring.Node{}, fmt.Errorf("cluster: server listen address is empty; set HOARDDB_LISTEN to this node's reachable address")
	}
	addr = normalizeAddr(addr)
	id := strings.TrimSpace(cfg.Server.NodeID)
	if id == "" {
		id = addr
	}
	return ring.Node{ID: id, Addr: addr}, nil
}

// normalizeAddr turns a wildcard listen address into one that is actually
// dialable, so that a node advertised as 0.0.0.0:4433 does not make peers try
// to dial 0.0.0.0.
func normalizeAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// memberList parses configured seed nodes and resolves this node's ring
// identity. The second return value is the resolved self: the identity this
// node must use, which is the one its address is listed under. A node whose
// node ID defaults to its hostname would otherwise appear as an extra member
// with a different identity, and every node would compute a different ring —
// or, worse, a node would consider itself not to be a ring member at all and
// quietly never take ownership of a bucket.
//
// The resolved self is returned rather than written into the caller's value:
// mutating a parameter would leave the caller with the unresolved identity.
func memberList(cfg *config.Config, self ring.Node) ([]ring.Node, ring.Node, bool, error) {
	var listed []ring.Node
	for _, spec := range cfg.HashRing.SeedNodes {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		n, err := parseMember(spec)
		if err != nil {
			return nil, ring.Node{}, false, err
		}
		listed = append(listed, n)
	}
	// Refused before self is resolved: an entry dropped as a repeat would
	// otherwise still give this node its id, and two nodes would run as one
	// (BUG-0595).
	if clash, clashed := findMemberIDClash(listed); clashed {
		return nil, ring.Node{}, false, fmt.Errorf("cluster: HOARDDB_SEED_NODES lists node id %q at two addresses, %s and %s; "+
			"two nodes under one id would both own its buckets and both act as the auth master. "+
			"Give each node its own HOARDDB_NODE_ID and list it once as <node-id>@<address>, "+
			"or list each address once in HOARDDB_SEED_NODES",
			clash.id, clash.firstAddr, clash.secondAddr)
	}

	if clash, clashed := findMemberAddrClash(listed); clashed {
		return nil, ring.Node{}, false, fmt.Errorf("cluster: HOARDDB_SEED_NODES lists the address %s under two node ids, %q and %q; "+
			"one node runs as only one of them, and the other would be a member nobody runs, owning buckets no node serves. "+
			"List each address once in HOARDDB_SEED_NODES, under the node id the node at %s runs as",
			clash.addr, clash.firstID, clash.secondID, clash.addr)
	}

	self, implicit, err := resolveListedSelf(cfg, self, listed)
	if err != nil {
		return nil, ring.Node{}, false, err
	}
	if err := refusePhantomSelf(cfg, self, implicit, listed); err != nil {
		return nil, ring.Node{}, false, err
	}

	seen := make(map[string]struct{}, len(listed))
	var order []ring.Node
	for _, n := range listed {
		// A repeat here is the same node at the same address listed twice:
		// harmless, and listed once.
		if _, dup := seen[n.ID]; dup {
			continue
		}
		seen[n.ID] = struct{}{}
		order = append(order, n)
	}
	if implicit {
		order = append(order, self)
	}
	return order, self, implicit, nil
}

// resolveListedSelf finds the seed entry that is this node: the one listed at
// its address, or, on a wildcard listen address that can never equal the
// advertised one, the one listed under its explicitly configured node id
// (BUG-0318). An id match never overrides this node's own listed address, and
// a hostname default never matches by id: cloned hosts share a hostname, and
// either would let two nodes run under one id (BUG-0598). A node that matches
// no entry is implicit, and refused when its id is listed for another address.
func resolveListedSelf(cfg *config.Config, self ring.Node, listed []ring.Node) (ring.Node, bool, error) {
	explicitID := !cfg.Server.NodeIDDefaulted && strings.TrimSpace(cfg.Server.NodeID) != ""
	var atAddr, underID *ring.Node
	for i := range listed {
		if listed[i].Addr == self.Addr && atAddr == nil {
			atAddr = &listed[i]
		}
		if explicitID && listed[i].ID == self.ID && underID == nil {
			underID = &listed[i]
		}
	}
	switch {
	case atAddr != nil && underID != nil && underID.Addr != atAddr.Addr:
		return ring.Node{}, false, fmt.Errorf("cluster: this node listens on %s, which HOARDDB_SEED_NODES lists as node id %q, "+
			"but its HOARDDB_NODE_ID %q is listed at %s, another node's address; two nodes under one id would both own its buckets "+
			"and both act as the auth master. Set HOARDDB_NODE_ID=%s on this node, the id its address is listed under",
			self.Addr, atAddr.ID, self.ID, underID.Addr, atAddr.ID)
	case atAddr != nil:
		return *atAddr, false, nil
	case underID != nil && config.ListensOnWildcard(cfg.Server.Listen):
		// The wildcard case: adopt the advertised address, the one peers dial.
		return *underID, false, nil
	}
	for _, n := range listed {
		if n.ID == self.ID {
			return ring.Node{}, false, fmt.Errorf("cluster: this node runs as node id %q at %s, but HOARDDB_SEED_NODES lists %q at %s, "+
				"another node's address; two nodes under one id would both own its buckets and both act as the auth master. "+
				"Give this node its own HOARDDB_NODE_ID (the default is the hostname, which cloned hosts share) and list it as <node-id>@%s, "+
				"or, if this node is %q, set HOARDDB_LISTEN=%s (or a wildcard such as 0.0.0.0 with HOARDDB_NODE_ID=%s)",
				self.ID, self.Addr, n.ID, n.Addr, self.Addr, n.ID, n.Addr, n.ID)
		}
	}
	return self, true, nil
}

// refusePhantomSelf refuses a member set that holds this node twice
// (BUG-0319): an implicit wildcard self on 127.0.0.1 when a seed on another
// machine is listed by address only, or when no explicit node id could match
// its entry, and a loopback seed on this node's own port beside its real
// listen address. An implicit wildcard node with an explicit id and only
// id@address seeds is a node being added with 'join', and is accepted.
// config.Validate refuses the same lists first; this keeps memberList honest
// for any other caller.
func refusePhantomSelf(cfg *config.Config, self ring.Node, implicit bool, listed []ring.Node) error {
	listen := strings.TrimSpace(cfg.Server.Listen)
	if config.ListensOnWildcard(listen) {
		if !implicit {
			return nil
		}
		explicitID := !cfg.Server.NodeIDDefaulted && strings.TrimSpace(cfg.Server.NodeID) != ""
		entry := "<node-id>@<address>"
		if explicitID {
			entry = self.ID + "@<address>"
		}
		for _, n := range listed {
			if config.ListensOnLoopbackOnly(n.Addr) {
				continue
			}
			addressOnly := n.ID == n.Addr
			if !addressOnly && explicitID {
				continue
			}
			return fmt.Errorf("cluster: this node listens on %s and is advertised as %s, and no seed entry can be its own "+
				"(seed %s is on another machine and %s): if one of the seeds is this node, the ring holds it twice, there and on "+
				"loopback, a member no peer can reach. Set HOARDDB_NODE_ID and list every node as <node-id>@<address> in "+
				"HOARDDB_SEED_NODES on every node, this one as %s, or set HOARDDB_LISTEN to the address HOARDDB_SEED_NODES lists for it",
				listen, self.Addr, n.Addr, phantomReason(addressOnly), entry)
		}
		return nil
	}
	if config.ListensOnLoopbackOnly(self.Addr) {
		return nil
	}
	// A listen address without a port names no seed entry; binding it fails
	// with its own error.
	_, ownPort, err := net.SplitHostPort(self.Addr)
	if err != nil {
		return nil
	}
	for _, n := range listed {
		if _, port, err := net.SplitHostPort(n.Addr); err == nil && port == ownPort && config.ListensOnLoopbackOnly(n.Addr) {
			return fmt.Errorf("cluster: HOARDDB_SEED_NODES lists %s, a loopback address on this node's own port, but this node "+
				"listens on %s: the loopback entry would be a second ring member for it that no peer can reach. List this node "+
				"in HOARDDB_SEED_NODES on every node as %s instead of %s",
				n.Addr, self.Addr, self.Addr, n.Addr)
		}
	}
	return nil
}

// memberAddrClash is one address listed under two different node ids.
type memberAddrClash struct {
	addr, firstID, secondID string
}

// findMemberAddrClash returns the first address members list under two
// different node ids.
func findMemberAddrClash(members []ring.Node) (memberAddrClash, bool) {
	idAt := make(map[string]string, len(members))
	for _, m := range members {
		first, listed := idAt[m.Addr]
		if !listed {
			idAt[m.Addr] = m.ID
			continue
		}
		if first != m.ID {
			return memberAddrClash{addr: m.Addr, firstID: first, secondID: m.ID}, true
		}
	}
	return memberAddrClash{}, false
}

// memberIDClash is one node id listed at two different addresses.
type memberIDClash struct {
	id, firstAddr, secondAddr string
}

// findMemberIDClash returns the first node id members list at two different
// addresses. The same id at the same address twice is one node and no clash.
func findMemberIDClash(members []ring.Node) (memberIDClash, bool) {
	addrOf := make(map[string]string, len(members))
	for _, m := range members {
		first, listed := addrOf[m.ID]
		if !listed {
			addrOf[m.ID] = m.Addr
			continue
		}
		if first != m.Addr {
			return memberIDClash{id: m.ID, firstAddr: first, secondAddr: m.Addr}, true
		}
	}
	return memberIDClash{}, false
}

// parseMember accepts "host:port" (identity is the address) or the explicit
// "id@host:port" form for clusters that want stable node IDs independent of
// address.
func parseMember(spec string) (ring.Node, error) {
	// strings.Cut returns (before, after, found): with no "@" the whole spec is
	// the address, which is the common "identity is the address" form.
	before, after, hasID := strings.Cut(spec, "@")

	id := strings.TrimSpace(before)
	addr := id
	if hasID {
		addr = strings.TrimSpace(after)
		if id == "" {
			return ring.Node{}, fmt.Errorf("cluster: member %q has an empty node id", spec)
		}
	}
	if addr == "" {
		return ring.Node{}, fmt.Errorf("cluster: member %q has an empty address", spec)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return ring.Node{}, fmt.Errorf("cluster: member %q is not host:port: %w", spec, err)
	}
	return ring.Node{ID: id, Addr: addr}, nil
}

// Enabled reports whether this server is part of a multi-node cluster and
// therefore routes and replicates. A single node is never routed: it writes
// straight to its own storage exactly as before.
func (c *Cluster) Enabled() bool {
	if c == nil {
		return false
	}
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return c.r.Len() > 1
}

// Members returns the cluster members in deterministic order.
func (c *Cluster) Members() []ring.Node {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return append([]ring.Node(nil), c.members...)
}

// authQuorumProbeTimeout bounds one member reachability probe. It is short
// because the check gates every user mutation, and a peer that cannot answer
// within it is not reachable for the purpose of committing a config it must
// honour (spec 5.2).
const authQuorumProbeTimeout = 2 * time.Second

// QuorumReachable reports how many of the current term's members are
// reachable (including this node) and how many are needed for a strict
// majority (⌊n/2⌋+1). It is a reachability check, not a consensus log: it
// probes members until it can prove a majority or runs out.
//
// The member set is the live membership view, not the auth config's recorded
// term: when the two disagree mid-join, who can actually be reached is the
// only answer that keeps a mutation from being committed where a healed
// partition would discard it.
func (c *Cluster) QuorumReachable(ctx context.Context) (reachable, needed, total int) {
	members := c.Members()
	total = len(members)
	needed = total/2 + 1
	reachable = 1 // this node
	for _, m := range members {
		if m.ID == c.self.ID {
			continue
		}
		if reachable >= needed {
			break
		}
		if c.memberReachable(ctx, m) {
			reachable++
		}
	}
	return reachable, needed, total
}

// memberReachable probes one member with the same lightweight frame the
// heartbeat uses. A failed probe drops the cached connection so the next call
// redials rather than reusing a socket the peer has already closed.
func (c *Cluster) memberReachable(ctx context.Context, m ring.Node) bool {
	p := c.peerFor(m)
	cli, err := p.dial(ctx)
	if err != nil {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, authQuorumProbeTimeout)
	defer cancel()
	frame, err := cli.SendRequest(probeCtx, transport.LaneControl, transport.OpPing, nil)
	if err != nil {
		p.dropClient()
		return false
	}
	return frame.Opcode == transport.OpPong
}

// writeConcernDescription renders what an unqualified write takes: the
// operator's configured level, or the rule, because the number depends on which
// bucket is written to.
func writeConcernDescription(override WriteConcernLevel) string {
	if override != 0 {
		return override.String()
	}
	return "per bucket (majority above replication factor 1)"
}

// defaultWriteConcern is the durability a write to the bucket placed by key
// takes when the request names none: majority for a bucket whose factor is above
// one, otherwise one. A configured level outranks it — the per-bucket rule is
// the default, not a policy that outranks the operator.
func (c *Cluster) defaultWriteConcern(key placementKey) WriteConcernLevel {
	return c.defaultWriteConcernFor(c.effectiveRF(key))
}

// defaultWriteConcernFor is defaultWriteConcern for a factor the caller names.
func (c *Cluster) defaultWriteConcernFor(factor int) WriteConcernLevel {
	if c.wcOverride != 0 {
		return c.wcOverride
	}
	if factor > 1 {
		return WcMajority
	}
	return WcOne
}

// LocalID returns this node's ring identity.
func (c *Cluster) LocalID() string { return c.self.ID }

// LocalAddr returns this node's advertised address.
func (c *Cluster) LocalAddr() string { return c.self.Addr }

// Owner returns the node responsible for the bucket placed by key.
func (c *Cluster) Owner(key placementKey) (ring.Node, error) {
	owner, _, err := c.ownerView(key)
	return owner, err
}

// ownerView returns the owner of the bucket placed by key and the membership
// epoch of the ring that placed it, read under one hold (step 9.4): the epoch
// an owner logs an entry under is the one its ownership was decided in.
//
// A ring of one member answers with it (ring.Ring.Only) without the ring's
// lookup, which allocates the key's bytes on every call: the one-member door
// asks this of every request on a node a removal left alone (BUG-0630).
func (c *Cluster) ownerView(key placementKey) (owner ring.Node, epoch uint64, err error) {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	if only, alone := c.r.Only(); alone {
		return only, c.epoch, nil
	}
	owner, err = c.r.Owner(key.encoded)
	return owner, c.epoch, err
}

// Replicas returns the ordered replica set of the bucket placed by key, owner
// first.
func (c *Cluster) Replicas(key placementKey) ([]ring.Node, error) {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return c.r.Replicas(key.encoded, c.effectiveRFLocked(key))
}

// effectiveRF is the number of copies of the bucket placed by key: the factor
// it declared, else the node's. A node that has never held the bucket has no
// entry and answers with its own default, so no caller may take "this node is
// in the set" for "this node holds the bucket".
func (c *Cluster) effectiveRF(key placementKey) int {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return c.effectiveRFLocked(key)
}

// effectiveRFLocked is effectiveRF for a caller holding viewMu.
func (c *Cluster) effectiveRFLocked(key placementKey) int {
	if factor, declared := c.bucketFactors[key]; declared {
		return int(factor)
	}
	return c.rf
}

// recordBucketFactor registers the factor a served bucket declared. A factor
// of zero is "declared nothing" and removes the entry, so a bucket re-created
// without one does not inherit the last incarnation's.
func (c *Cluster) recordBucketFactor(key placementKey, factor int32) {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	if factor == 0 {
		delete(c.bucketFactors, key)
		return
	}
	c.bucketFactors[key] = factor
}

// forgetBucketFactor removes a dropped bucket's entry.
func (c *Cluster) forgetBucketFactor(key placementKey) {
	c.recordBucketFactor(key, 0)
}

// replicasAtFactor is Replicas for a factor the caller names, for delivering an
// entry to the set a bucket had rather than the set it has.
func (c *Cluster) replicasAtFactor(key placementKey, factor int) ([]ring.Node, error) {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return c.r.Replicas(key.encoded, factor)
}

// Hosts reports whether this node holds a copy of the bucket placed by key,
// meaning it can serve reads for it without a network hop.
func (c *Cluster) Hosts(key placementKey) bool {
	return c.peerHosts(c.self.ID, key)
}

// localToken returns the current cluster token.
func (c *Cluster) localToken() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.token.Token
}

// internodeClientConfig builds the pinned client TLS config for a peer at
// internodeAddr. Every internode dial uses it, so there is no path that
// accepts any certificate.
func (c *Cluster) internodeClientConfig(internodeAddr string) (*tls.Config, error) {
	return c.fingerprints.ClientConfig(internodeAddr)
}

// ValidateToken reports whether a presented token authorises cluster
// membership. The common case is an in-memory constant-time comparison. A
// token that does not match is checked against the file only when the file
// has changed since the token in memory was read from it, which is what a
// rotation does: a run of wrong tokens costs one stat each, and no file read
// or exclusive lock that every member check would wait on.
func (c *Cluster) ValidateToken(presented string) bool {
	if presented == "" {
		return false
	}

	c.tokenMu.RLock()
	current := c.token
	known := c.tokenFile
	c.tokenMu.RUnlock()

	if tokenMatches(current, presented) {
		return true
	}
	dataDir := c.s.config.Storage.DataDir
	onDisk, err := statTokenFile(dataDir)
	if err != nil {
		c.logger.Warn("cluster token file cannot be checked for a rotation", "error", err)
		return false
	}
	if onDisk.same(known) {
		return false
	}

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	// Re-check under the write lock: another goroutine may have reloaded it.
	if tokenMatches(c.token, presented) {
		return true
	}
	if onDisk.same(c.tokenFile) {
		return false
	}
	reloaded, err := c.s.credentials.LoadClusterToken(dataDir)
	if err != nil {
		c.logger.Warn("cluster token reload failed", "error", err)
		return false
	}
	c.token = *reloaded
	c.tokenFile = onDisk
	if tokenMatches(c.token, presented) {
		c.logger.Info("cluster token rotated; accepted the current token")
		return true
	}
	return false
}

// tokenFileStamp identifies one version of the cluster token file without
// reading it: a rotation rewrites the file, which changes its modification
// time.
type tokenFileStamp struct {
	modTime time.Time
	size    int64
}

// same reports whether two stamps name the same version of the file.
func (a tokenFileStamp) same(b tokenFileStamp) bool {
	return a.size == b.size && a.modTime.Equal(b.modTime)
}

// statTokenFile stamps the cluster token file in dataDir.
func statTokenFile(dataDir string) (tokenFileStamp, error) {
	info, err := os.Stat(clusterTokenCredential.path(dataDir))
	if err != nil {
		return tokenFileStamp{}, fmt.Errorf("stat the cluster token file: %w", err)
	}
	return tokenFileStamp{modTime: info.ModTime(), size: info.Size()}, nil
}

// tokenMatches compares a presented cluster token with the stored one in
// constant time. An expires_at in the file is ignored: members present the same
// token for the life of the cluster, and an earlier build stamped a generated
// one to expire after a day, which locked a running cluster out of itself
// (BUG-0657). A token is revoked by replacing the file.
func tokenMatches(t ClusterToken, presented string) bool {
	if t.Token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(t.Token), []byte(presented)) == 1
}

// peerFor returns the peer record for a node, creating it on first use.
func (c *Cluster) peerFor(n ring.Node) *peer {
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	if p, ok := c.peers[n.ID]; ok {
		if p.node.Addr != n.Addr {
			c.logger.Warn("peer address changed", "node_id", n.ID, "old", p.node.Addr, "new", n.Addr)
			p.node.Addr = n.Addr
		}
		return p
	}
	p := &peer{
		c:       c,
		node:    n,
		applied: make(map[string]uint64),
		// ackCh must exist from the start: waiters take a reference to the
		// current channel and park on it, and the first acknowledgement closes
		// it to wake them. A nil channel here left the first waiter parked on
		// a channel nothing would ever close.
		ackCh:  make(chan struct{}),
		sendCh: make(chan pendingEntry, entryQueueDepth),
		stopCh: make(chan struct{}),
		wake:   make(chan struct{}, 1),
	}
	c.peers[n.ID] = p
	return p
}

// Start connects to peers and begins shipping replication traffic. Peers that
// are down are retried in the background rather than blocking startup: a node
// must be able to start while its peers are unavailable.
func (c *Cluster) Start() {
	// Before the single-node return: a node started alone that joins a
	// cluster later accepts changes as a member, and must finish one whose
	// proposer it outlives. Idle until an acceptance arms it.
	// On storeWG: an adoption it finishes writes the membership history, and
	// Stop must wait for that before the store closes (BUG-0680).
	if c.startStoreGoroutine() {
		go c.runMembershipRecovery()
	}
	// Set before Enabled is read, as adoptMembershipLocked swaps the view
	// before it reads this: an adoption racing this Start is seen by one of
	// the two, or by both, and startMemberWorkers runs its body once.
	c.startCalled.Store(true)
	// Before the single-node return: a node removed from two members has a
	// ring of one, and stays a source for that one (step 9.3). A member's
	// pull is refused until this node's stream to it has opened in this
	// process, and only then can the hand-off be taken (BUG-0500).
	if c.removed() {
		for _, n := range c.Members() {
			c.peerFor(n)
			c.ensurePeerRunning(n)
		}
		c.startRemovedHandOff()
	}
	// A pass a stop interrupted, or the first on this build at an epoch past
	// the first, runs again now that the catalog is loaded. Before the
	// single-node return: a node whose pass has not finished refuses every
	// membership change, and its take-over worker waits on the pass, so a
	// survivor left alone by a removal and restarted mid-pass would never take
	// a bucket over (BUG-0506). A node that was always alone is at the first
	// epoch, which has no pass.
	if !c.placementPassFinished() {
		c.startPlacementPass(c.Epoch())
	}
	if !c.Enabled() {
		// A node left alone by a removal can own buckets whose only copy is
		// on the removed node, which asks it to take each one over; an
		// operator's migrate waits on the same worker (BUG-0502). It needs
		// only the take-over worker: a lone node logs no writes to prune and
		// pushes no auth config. A node that was always alone starts nothing.
		if c.aloneAfterARemoval() {
			c.startTakeOverWorker()
		}
		c.logger.Info("cluster disabled: single-node mode")
		return
	}
	// LIST_NODES reports the cluster as a whole, so a member lists itself. The
	// join handshake that used to gossip with the node's own seed entry did this
	// by side effect; with it gone the node records itself, with the
	// fingerprint of the certificate it serves.
	c.s.recordNode(c.self.ID, c.self.Addr)
	c.s.recordObservedFingerprint(c.self.ID, c.s.fingerprint)
	for _, n := range c.Members() {
		if n.ID == c.self.ID {
			continue
		}
		c.peerFor(n)
		c.ensurePeerRunning(n)
	}

	c.startMemberWorkers()

	// Buckets placed by their bare name before placement keys include the database move to the replica
	// set of their placement key (relocate.go).
	c.startRelocation()

	c.resumeSettling()
}

// startMemberWorkers starts, once per process, the workers every member of a
// multi-member cluster runs for as long as it runs. Start calls it on a node
// that starts with other members; adoptMembershipLocked calls it when a node
// started alone adopts its first other member, because nothing else starts
// them there: without the take-over worker such a founder never takes the
// term of a bucket its join moved, and its own hand-off waits on itself
// (BUG-0498). A node that stays alone never starts them, so its cost is one
// atomic load per adoption; Start gives a node left alone by a removal the
// take-over worker only (BUG-0502). Each worker is one goroutine per node.
func (c *Cluster) startMemberWorkers() {
	c.memberWorkers.Do(func() {
		// WAL retention prunes segments every replica has confirmed and
		// every local store has been synced past, keeping the log bounded
		// while never removing entries a lagging replica could still need.
		// Zero disables the loop (config), which is the pre-retention
		// behaviour.
		if c.s.config.Server.WALRetentionSeconds > 0 && c.startStoreGoroutine() {
			go c.runRetentionLoop()
		}

		// Auth-config replication: every member runs the loop; only the
		// master pushes. It wakes immediately on a local mutation and
		// otherwise retries unacked members on the configured interval (spec
		// 5.3.3). It writes the auth config to the store, so it is on storeWG
		// (BUG-0680), and an adoption that calls this while Stop runs is
		// refused.
		if c.startStoreGoroutine() {
			go c.authConfigLoop()
		}

		c.startTakeOverWorker()

		// The pending-copy worker copies in the buckets an increase placed on
		// this node while it held nothing (bucket_rf_worker.go). A node left
		// alone by a removal is placed nothing new, so Start does not give it
		// one.
		if c.startStoreGoroutine() {
			go c.runPendingCopyWorker()
		}
		if c.startStoreGoroutine() {
			go c.runUnderReplicatedGauge()
		}
	})
	c.signalTakeOver()
}

// startTakeOverWorker starts, once per process, the take-over worker
// (takeover.go), which takes over and settles the buckets whose marks the
// store holds from before this start, and every bucket named to it from now
// on. It is on storeWG, and refused once Stop is waiting.
func (c *Cluster) startTakeOverWorker() {
	c.takeOverWorker.Do(func() {
		if c.startStoreGoroutine() {
			go c.runTakeOver()
		}
	})
}

// resumeSettling runs the sweep of the current membership change again at
// start when this node had not settled it: a node stopped in the middle of its
// hand-offs, or one that gave up, takes them up again (join-freeze spec §8.1).
// The sweep walks the catalog, so it needs the catalog loaded: a sweep of an
// empty catalog would end with nothing left and mark the node settled.
func (c *Cluster) resumeSettling() {
	epoch := c.Epoch()
	if c.settled.Load() >= epoch {
		return
	}
	// A confirmed node still at the first epoch is a founder (a joiner is
	// confirmed only by adopting a later one), and a founder is settled there
	// (owner ruling 52). Its record is written unsynced and once, so a power
	// loss or a crash right after the confirmation can lose it; no sweep would
	// restore it, because there is no ring before the first epoch.
	if epoch == InitialEpoch && !c.bootstrapped.Load() {
		c.settleAtFounding(epoch)
		return
	}
	if _, recorded := c.ringAt(epoch - 1); !recorded {
		return
	}
	if !c.s.catalogLoaded.Load() {
		c.logger.Error("the cluster started before the catalog was loaded, so this node does not resume settling the last membership change; "+
			"it settles at the next one. This is a defect in the start order: load the catalog before starting the cluster",
			"epoch", epoch)
		return
	}
	c.logger.Info("resuming the hand-off of the last membership change, which this node had not finished", "epoch", epoch)
	c.maybeSweep(epoch)
}

// ensurePeerRunning starts a peer's runSender and verifyPeer goroutines
// exactly once per peer ID. Start() uses it for boot-time members, and
// adoptMembership uses it for members adopted after boot: without the
// sender, the owner can never ship WAL entries to a later-joined peer, and
// a majority write whose replica set includes that peer stalls forever on
// the write-concern deadline instead of failing fast.
func (c *Cluster) ensurePeerRunning(n ring.Node) {
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	if c.peerStarted[n.ID] {
		return
	}
	c.peerStarted[n.ID] = true
	p := c.peers[n.ID] // created by the caller's peerFor
	if c.startStoreGoroutine() {
		go p.runSender()
	}
	// On storeWG: a gossip answer it reconciles adopts a membership, which
	// writes the history (BUG-0680).
	if c.startStoreGoroutine() {
		go c.verifyPeer(n)
	}
}

// verifyPeer handshakes with a peer and checks member agreement, retrying in
// the background until the node stops.
func (c *Cluster) verifyPeer(n ring.Node) {
	defer c.storeWG.Done()
	p := c.peerFor(n)
	for {
		if err := c.ctx.Err(); err != nil {
			return
		}
		// Read before the exchange, so a wake that lands during it is not lost.
		woken := c.gossipWakeSignal()
		if err := p.gossip(c.ctx); err != nil {
			if alert, downFor := p.recordUnreachable(time.Now()); alert {
				c.logger.Warn("peer has been unreachable for a while — this is informational only, hoardDB never removes a node automatically; run 'remove-node' against another member if it is not coming back",
					"node_id", n.ID, "addr", n.Addr, "unreachable_for", downFor.Round(time.Second), "error", err)
			} else {
				c.logger.Debug("peer not reachable yet", "node_id", n.ID, "addr", n.Addr, "error", err)
			}
			// A paused join's report names the members it waits on as down.
			c.recomputeJoinFreezeForCounts()
			select {
			case <-time.After(peerRetryInterval):
			case <-woken:
			case <-c.ctx.Done():
				return
			}
			continue
		}

		if downFor := p.recordReachable(time.Now()); downFor > 0 {
			c.logger.Warn("peer reachable again", "node_id", n.ID, "addr", n.Addr, "was_unreachable_for", downFor.Round(time.Second))
			c.recomputeJoinFreeze()
		}
		// What a sweep gave up on may settle now: because this peer was down,
		// or for a reason that passed with no peer down at all. rearmSweep is
		// single-flight and rate-limited, and costs a lock with nothing left.
		c.rearmSweep()

		// The peer answers: a stalled sender tries it now instead of waiting out
		// its back-off. The sender is the only thing that ships to it.
		p.wakeSender()

		select {
		case <-time.After(10 * peerRetryInterval):
		case <-woken:
		case <-c.ctx.Done():
			return
		}
	}
}

// stopAfterPeersClosedHook, when set, runs in Cluster.Stop right after it has
// closed the peers it found, before it waits. Tests add a peer there, as a
// membership adoption on a goroutine still running does. Nil in production.
var stopAfterPeersClosedHook atomic.Pointer[func(c *Cluster)]

// clusterStopSlowAfter is how long Stop waits for the goroutines that use the
// node store before it logs that they are still stopping (and how often it
// logs it again).
// A sweepBound so a test can shorten it without waiting out the real bound.
var clusterStopSlowAfter = newSweepBound(3 * time.Second)

// startStoreGoroutine registers a goroutine that uses the node store or the
// log on storeWG, and reports whether it may start: not once Stop is waiting.
// The caller launches the goroutine, which calls storeWG.Done when it ends.
func (c *Cluster) startStoreGoroutine() bool {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	if c.storeStopping {
		return false
	}
	c.storeWG.Add(1)
	return true
}

// Stop shuts down replication: in-flight batches are flushed and connections
// are closed.
func (c *Cluster) Stop() {
	if c == nil || c.cancel == nil {
		return
	}
	c.cancel()

	// Before the peers are listed: ensurePeerRunning registers a sender under
	// peersMu, so a sender that got in before this point is in the list below
	// and is closed, and one that comes after is refused.
	c.storeMu.Lock()
	c.storeStopping = true
	c.storeMu.Unlock()

	c.peersMu.RLock()
	peers := make([]*peer, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.peersMu.RUnlock()

	for _, p := range peers {
		p.close()
	}
	if hook := stopAfterPeersClosedHook.Load(); hook != nil {
		(*hook)(c)
	}

	// First the goroutines that use the node store or the log, with no bound:
	// Server.Stop closes both as soon as this returns, and a goroutine that
	// still reads the store then panics inside Badger and takes the process
	// down. Closing the peers and cancelling the context above is what ends
	// them, so the wait ends by itself; a slow one is only logged.
	storeDone := make(chan struct{})
	go func() {
		c.storeWG.Wait()
		close(storeDone)
	}()
	slow := time.NewTicker(clusterStopSlowAfter.get())
	defer slow.Stop()
	for waiting := true; waiting; {
		select {
		case <-storeDone:
			waiting = false
		case <-slow.C:
			c.logger.Warn("replication goroutines that use the node store are still stopping; the store stays open until they have")
		}
	}

	// The clean stop's checkpoint: the store is still open here (Server.Stop
	// closes it after the cluster), and a restart then starts from these
	// positions and needs the log only for what came after.
	if c.skipStopCheckpoint {
		return
	}
	if _, err := c.checkpointAllPositions(true); err != nil {
		c.logger.Warn("could not checkpoint replication positions at stop; the next start rebuilds them from the log", "error", err)
	}
}

// catchUpReadBytes bounds how much of the log one read of a replay holds. The
// replay reads the log in pieces this size, outside the append lock, instead of
// decoding all of it at once.
const catchUpReadBytes = 4 << 20

// peerHosts reports whether a node holds a copy of the bucket placed by key.
func (c *Cluster) peerHosts(nodeID string, key placementKey) bool {
	replicas, err := c.Replicas(key)
	if err != nil {
		return false
	}
	for _, r := range replicas {
		if r.ID == nodeID {
			return true
		}
	}
	return false
}

// peer is a node-to-node connection with pipelined entry shipping.
type peer struct {
	c    *Cluster
	node ring.Node
	// divergedOn is the peer's last refusal of this node's entry as diverged
	// or of another incarnation (R8-E), nil once a ship succeeds: the
	// retention warning names it as why the log is held.
	divergedOn atomic.Pointer[string]

	mu     sync.Mutex
	client peerConn
	// confirmedOn and confirmedGeneration are the connection, and its socket
	// generation, on which the peer last said its store is sealed
	// (confirmSealed). Guarded by mu.
	confirmedOn         peerConn
	confirmedGeneration uint64

	// ackMu guards applied. applied is the highest replication sequence this
	// peer has confirmed per source node; this node's own entry is its stream's
	// cursor, set by the handshake to the position the peer persisted
	// (setCursor) and raised by its acknowledgements. Sequence numbers are
	// assigned by the source, so they are only comparable within one source's
	// stream.
	ackMu   sync.Mutex
	applied map[string]uint64
	ackCh   chan struct{}
	// lastAckAt is when the peer last raised this node's cursor, for
	// hoarddb_replication_lag_seconds. Guarded by ackMu.
	lastAckAt time.Time

	sendCh chan pendingEntry
	stopCh chan struct{}
	once   sync.Once

	// streamMu guards the stream's state (stream_state.go): where this node's
	// stream to the peer stands, why it last left streaming, its generation
	// (one more at every change of state, so a sender working for an older one
	// notices), whether the log up to the boundary it entered streaming at has
	// been shipped, the highest sequence queued since, and whether a retention
	// pass has asked for an advance notice (advanceWanted). wake is how
	// anything that changes what the sender should do tells it so; it is
	// buffered so a pending wake covers any number.
	streamMu    sync.Mutex
	state       streamState
	stallReason string
	// refusedForGood is the last handshake's refusal when no retry clears it
	// (errHandshakeRefusedForGood), nil otherwise.
	refusedForGood  error
	generation      uint64
	boundaryShipped bool
	queuedThrough   uint64
	advanceWanted   bool
	wake            chan struct{}
	// owedSince is when an entry was first addressed to the peer since it was
	// last owed nothing (replication_metrics.go); zero while it is owed
	// nothing.
	owedSince time.Time
	// outSince is when the stream last left the in-sync set, zero while it
	// is in; outReplayed counts the entries replayed to it since. They make
	// the stream's two log lines (leaveInSyncLocked, returnToSyncLocked).
	outSince    time.Time
	outReplayed uint64
	// everStreamed is set when the stream first enters streaming. Until then
	// a handshake nobody answered (a refused first dial at cluster
	// formation) takes the peer out of the set without an event: it was never
	// in it. outQuiet marks that departure so its return is not logged either
	// (BUG-0731).
	everStreamed bool
	outQuiet     bool
	// logReads counts the sender's reads of the log for this peer. A stalled
	// sender reads none until the peer answers its probe.
	logReads atomic.Uint64
	// wrapConn, when set, wraps every connection installed for this peer. It is
	// a test seam, guarded by mu: a frozen or delayed peer has to stay frozen or
	// delayed across a redial. Nil in production.
	wrapConn func(peerConn) peerConn

	// livenessMu guards unreachableSince and lastAlertAt: how long this peer
	// has been continuously failing verifyPeer's gossip check, so a
	// persistent outage escalates from quiet retry logging to a loud,
	// repeated alert instead of retrying silently forever at Debug level.
	// This is detection only — hoardDB never removes a node automatically
	// (the rebalance-design record §6.3); an operator decides whether and when
	// to run 'remove-node'.
	livenessMu       sync.Mutex
	unreachableSince time.Time // zero when the peer is currently reachable
	lastAlertAt      time.Time // when the loud "still unreachable" alert last fired

	// horizonMu guards lastHorizonWarnAt, which rate-limits the "behind the
	// retained WAL horizon" log line (see ship's horizon check): the
	// condition does not resolve itself between retries, so logging it every
	// replRetryInterval would scroll the log forever over one unchanging
	// fact.
	horizonMu         sync.Mutex
	lastHorizonWarnAt time.Time

	// refusedMu guards refused: the create of another incarnation this peer
	// refused (10a.6), which stalls the stream, and the drop of that
	// incarnation this node has logged since, if any (owner ruling 53).
	// Written and read by the stream's sender. Nil while no create is refused.
	refusedMu sync.Mutex
	refused   *refusedCreate
}

// peerUnreachableAlertThreshold is how long a peer must be continuously
// unreachable before verifyPeer escalates from quiet retry logging to a loud
// alert. Below this, a network blip or a slow restart is ordinary.
const peerUnreachableAlertThreshold = 30 * time.Second

// peerUnreachableAlertInterval is how often the loud alert repeats while a
// peer stays unreachable past the threshold, so a long outage does not
// scroll off the log after a single line.
const peerUnreachableAlertInterval = 5 * time.Minute

// recordUnreachable marks this peer as failing a liveness check right now
// and reports whether the caller should log a loud alert: the first time it
// crosses peerUnreachableAlertThreshold continuously down, and every
// peerUnreachableAlertInterval after that.
func (p *peer) recordUnreachable(now time.Time) (alert bool, downFor time.Duration) {
	p.livenessMu.Lock()
	defer p.livenessMu.Unlock()
	if p.unreachableSince.IsZero() {
		p.unreachableSince = now
	}
	downFor = now.Sub(p.unreachableSince)
	if downFor < peerUnreachableAlertThreshold {
		return false, downFor
	}
	if p.lastAlertAt.IsZero() || now.Sub(p.lastAlertAt) >= peerUnreachableAlertInterval {
		p.lastAlertAt = now
		return true, downFor
	}
	return false, downFor
}

// downSince reports since when the peer has been recorded unreachable.
func (p *peer) downSince() (time.Time, bool) {
	p.livenessMu.Lock()
	defer p.livenessMu.Unlock()
	return p.unreachableSince, !p.unreachableSince.IsZero()
}

// recordReachable clears any unreachable tracking and reports how long the
// peer was down, or zero if it was not marked unreachable.
func (p *peer) recordReachable(now time.Time) time.Duration {
	p.livenessMu.Lock()
	defer p.livenessMu.Unlock()
	if p.unreachableSince.IsZero() {
		return 0
	}
	downFor := now.Sub(p.unreachableSince)
	p.unreachableSince = time.Time{}
	p.lastAlertAt = time.Time{}
	return downFor
}

// horizonWarnInterval rate-limits the "behind the retained WAL horizon" log
// line (see ship). The condition does not resolve itself between retries —
// an operator must restore or rebuild the peer — so logging it every
// replRetryInterval would scroll the log forever over one unchanging fact.
const horizonWarnInterval = 5 * time.Minute

// warnHorizon logs a WAL-horizon refusal at most once per horizonWarnInterval
// per peer. It deliberately never touches the connection: this is a
// data-plane fact about the peer's own log position, not a transport fault,
// and the shared connection pool may carry unrelated in-flight traffic.
func (p *peer) warnHorizon(msg string, args ...interface{}) {
	p.logRateLimited(slog.LevelWarn, msg, args...)
}

// logRateLimited logs one of the stream's refusals (spec §19.3.3: below the
// horizon, diverged, chain lost) at most once per horizonWarnInterval per
// peer: none of them resolves itself between the sender's retries.
func (p *peer) logRateLimited(level slog.Level, msg string, args ...interface{}) {
	p.horizonMu.Lock()
	due := time.Since(p.lastHorizonWarnAt) >= horizonWarnInterval
	if due {
		p.lastHorizonWarnAt = time.Now()
	}
	p.horizonMu.Unlock()
	if !due {
		return
	}
	p.c.logger.Log(p.c.ctx, level, msg, append([]interface{}{"node_id", p.node.ID}, args...)...)
}

// pendingEntry is one WAL entry queued for shipment to a peer.
type pendingEntry struct {
	entry ReplEntry
}

// advanceAck records that a peer has applied through seq for the given source.
func (p *peer) advanceAck(source string, seq uint64) {
	p.ackMu.Lock()
	if cur, ok := p.applied[source]; !ok || seq > cur {
		p.applied[source] = seq
		if source == p.c.self.ID {
			p.lastAckAt = time.Now()
		}
		ch := p.ackCh
		p.ackCh = make(chan struct{})
		p.ackMu.Unlock()
		if ch != nil {
			close(ch)
		}
		return
	}
	p.ackMu.Unlock()
}

// cursor returns this node's stream cursor for the peer, and whether there is
// one: a peer that has not completed a handshake with this process has none,
// because only the position it reports says what it holds.
func (p *peer) cursor(self string) (uint64, bool) {
	p.ackMu.Lock()
	defer p.ackMu.Unlock()
	cursor, opened := p.applied[self]
	return cursor, opened
}

// ackedThrough returns the highest sequence this peer has confirmed for a
// source.
func (p *peer) ackedThrough(source string) uint64 {
	p.ackMu.Lock()
	defer p.ackMu.Unlock()
	return p.applied[source]
}

// setCursor sets this node's stream cursor for the peer to the position the
// peer reported at the handshake. Unlike an acknowledgement it can move down:
// a peer emptied while this node ran reports less than it once acknowledged.
func (p *peer) setCursor(seq uint64) {
	p.ackMu.Lock()
	p.applied[p.c.self.ID] = seq
	ch := p.ackCh
	p.ackCh = make(chan struct{})
	p.ackMu.Unlock()
	close(ch)
}

// waitForAck blocks until the peer confirms the given sequence for a source.
func (p *peer) waitForAck(ctx context.Context, source string, seq uint64) error {
	for {
		if p.ackedThrough(source) >= seq {
			return nil
		}
		p.ackMu.Lock()
		if p.ackCh == nil {
			// Defensive: every peer is created with a channel, and creating it
			// here under the lock keeps waiters sharing one channel.
			p.ackCh = make(chan struct{})
		}
		ch := p.ackCh
		p.ackMu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s to reach seq %d: %w", p.node.ID, seq, ctx.Err())
		case <-ch:
		case <-p.stopCh:
			return fmt.Errorf("peer %s stopped before reaching seq %d", p.node.ID, seq)
		}
	}
}

// close stops the sender loop, closes the connection and removes the peer's
// internode in-flight series, so the gauge holds no series for a peer this
// node no longer talks to.
func (p *peer) close() {
	p.once.Do(func() { close(p.stopCh) })
	p.dropClient()
	forgetInternodeInFlight(p.node.ID)
}

// dropClient closes and forgets the connection so the next call redials.
func (p *peer) dropClient() {
	p.mu.Lock()
	cli := p.client
	p.client = nil
	p.mu.Unlock()
	if cli != nil {
		_ = cli.Close()
	}
}

// phantomReason says why a wildcard node cannot match a remote seed entry.
func phantomReason(addressOnly bool) string {
	if addressOnly {
		return "is listed by address only, which a wildcard listener never equals"
	}
	return "HOARDDB_NODE_ID is not set, and the host name default is never matched by id"
}

// refuseSavedPhantomSelf refuses a saved member set that holds this node a
// second time (BUG-0319). A node bootstrapped before the seed refusal existed
// saved the extra entry, and a saved set outranks the seeds, so fixing the
// seeds alone leaves it in place: a wildcard node located at 127.0.0.1 beside
// members on other machines, or a loopback member on this node's own port
// beside its real address. A set that lists this node once is never refused.
func refuseSavedPhantomSelf(listen string, self ring.Node, members []ring.Node, membershipPath string) error {
	var phantom string
	if config.ListensOnWildcard(listen) {
		if !config.ListensOnLoopbackOnly(self.Addr) {
			return nil
		}
		for _, m := range members {
			if !config.ListensOnLoopbackOnly(m.Addr) {
				phantom = self.Addr
				break
			}
		}
	} else if !config.ListensOnLoopbackOnly(self.Addr) {
		_, ownPort, err := net.SplitHostPort(self.Addr)
		if err != nil {
			// An address without a port shares no port with a loopback member.
			return nil
		}
		for _, m := range members {
			if _, port, err := net.SplitHostPort(m.Addr); err == nil && port == ownPort && config.ListensOnLoopbackOnly(m.Addr) {
				phantom = m.Addr
				break
			}
		}
	}
	if phantom == "" {
		return nil
	}
	return fmt.Errorf("cluster: the member set in %s holds this node a second time, at %s, a loopback address no peer can reach "+
		"(members %v); it was saved from a seed list that could not name this node. To rebuild it: stop every node, remove "+
		"membership.json from each node's data directory, set HOARDDB_NODE_ID on each and list every member as "+
		"<node-id>@<address> in HOARDDB_SEED_NODES, the same list on every node, then start them again",
		membershipPath, phantom, memberIDsOf(members))
}
