package wire

import (
	"context"
	entropy "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The pair flag bits of write path Formats 1, as cluster.Pair.Flags documents them.
const (
	flagSending uint8 = 1 << iota
	flagAwaiting
	flagReplayNeeded
	flagReplayed
	flagStopProved
	// flagRestarted marks an awaiting hold opened inside the stop bound after a restart,
	// so its refusal reads as the restart hold (BUG-0747).
	flagRestarted
	// flagBootReplay marks a pair whose boot replay waits for a declaration (BUG-0760), set
	// together with flagReplayNeeded so the refusals can say what is being waited for.
	flagBootReplay
	// flagFirstApplyFailed marks a pair with no record whose last appended record, the one at
	// AppendedEpoch:AppendedSeq, failed its apply (row 9). It is that record's apply that creates the
	// pair's record, so recoverPair's replay creates it there (BUG-0809), and nowhere else: every other
	// replay of a record-less pair is a no-op. In memory only, so a boot never creates a record.
	flagFirstApplyFailed
)

// DatabaseInfo is what the owner needs of one database's committed row.
type DatabaseInfo struct {
	// Name is the database's name, for the refusal texts that name it.
	Name string
	// RF is the configured replication factor, never the copies in sync.
	RF int
	// DefaultConcern is the level a request without one asks for (conv §17).
	DefaultConcern machine.Concern
	// Memory is true when the database is a memory database (Database.Storage == cluster.StorageMemory).
	Memory bool
}

// Topology is the committed cluster state the owner path reads. Every method is a
// read of committed state: none takes a pair lock or does I/O.
type Topology interface {
	// Committed is the committed epoch.
	Committed() uint64
	// Self is this node's id.
	Self() string
	// Members is the cluster's member count; 1 means no lease.
	Members() int
	// Table is the slot table of the committed epoch.
	Table() *cluster.SlotTable
	// Database is the row of database db.
	Database(db uint64) (DatabaseInfo, bool)
	// Target is the highest position a reachable ready holder reports for the pair
	// (conv §10 rule 2); the zero Position when this node was already the owner.
	Target(slot uint16, db uint64) cluster.Position
}

// Appender is the node's one retained log. *storage.WAL implements it.
type Appender interface {
	// AppendFrom appends one entry, returns its node-local cursor once it is fsynced, and shares that fsync with concurrent appends.
	AppendFrom(bucket string, op byte, key, data []byte, origin storage.WALOrigin) (uint64, error)
	// AppendUnsyncedFrom writes one entry and returns its cursor without waiting for the fsync: the entry is durable only once SyncThrough has returned for that cursor or a later one.
	AppendUnsyncedFrom(bucket string, op byte, key, data []byte, origin storage.WALOrigin) (uint64, error)
	// SyncThrough returns once every entry at or below seq is durable, sharing the group commit.
	SyncThrough(seq uint64) error
	// SyncedSequence is the highest sequence known durable: a lower bound, which a failed SyncThrough leaves the caller to reason from.
	SyncedSequence() uint64
}

// BatchSource starts the unsynced Badger batch of an apply. storage.KV implements it.
type BatchSource interface {
	// NewBatch starts one atomic Badger batch.
	NewBatch() storage.KVBatch
}

// AppliedRecord is everything the apply of one position writes: the operation's keys
// and, in the same batch, the pair record's applied position, WAL cursor and ordinal.
type AppliedRecord struct {
	Slot    uint16
	DB      uint64
	Op      cluster.Op
	Pos     cluster.Position
	Cursor  uint64
	Ordinal uint64
	// RequestID and OpIndex name the operation's dedupe entry (conv §18).
	RequestID [16]byte
	OpIndex   uint8
	// ClaimSink receives what a claim leased; nil for a replay or a copy, which have no
	// caller waiting.
	ClaimSink ClaimSink
	// BootReplay marks a record applied by a boot replay pass (Writer.replay with deferrals: the
	// scan at the open and every later pass of Node.startBootReplay, whatever pair the record is
	// of, deferred or not), the one apply allowed to read a declaration in its own pair from this
	// node's store.
	BootReplay bool
}

// Applier turns one operation into keys. It evaluates no schema fence: the record
// carries the field ids and values the owner used (conv §20).
type Applier interface {
	// Handles reports whether Fill can apply an operation of this class.
	Handles(class byte) bool
	// Precondition is check 6a, read from the owner's own state under the pair lock. A
	// non-zero code is the operation's own refusal; an error is a failed read.
	Precondition(slot uint16, db uint64, op cluster.Op) (cluster.Code, error)
	// Fill writes the operation's keys and the pair record into batch. It never commits.
	Fill(batch storage.KVBatch, rec AppliedRecord) error
}

// SchemaView is the owner's declaration cache entry for one bucket at one instant.
type SchemaView struct {
	Present, Missing bool
	Decl             machine.Declaration
	// Name is the bucket's name when the cache holds it, for the refusal texts that name it.
	Name string
	// Seen is the entry's schema_version_seen.
	Seen int64
	// Published is this node's published schema_version, re-read on every call.
	Published int64
}

// Schema is the declaration cache (conv §20).
type Schema interface {
	// View reads the cache. It does no I/O, so the owner can call it inside the section.
	View(db, bucketID uint64) SchemaView
	// Refetch fills the cache from the ring. It may block on I/O, so the owner calls it
	// only outside the section.
	Refetch(ctx context.Context, db, bucketID uint64) error
}

// DedupeReader point-reads the dedupe entry of one operation (conv §18).
type DedupeReader interface {
	// Lookup returns the stored entry, live or not, or nil when there is none. It takes
	// no pair lock, because the owner calls it inside the section; whether the entry is
	// still inside the window is the writer's call, made with the pair's ordinal.
	Lookup(slot uint16, db, bucketID uint64, key machine.DedupeKey) (*machine.DedupeEntry, error)
}

// ResultFiller is implemented by an Applier whose operations answer with data that
// cannot be recomputed (pop, take, claim; conv §18). The writer prefers it to Fill, and
// stores the result in the write's dedupe entry so a retried operation returns it.
type ResultFiller interface {
	// FillWithResult is Fill and also returns the operation's result.
	FillWithResult(batch storage.KVBatch, rec AppliedRecord) ([]byte, error)
}

// EngineResolver is implemented by an Applier that resolves a store field's engine from a
// declaration it may have to read from the ring. A copy calls it before it appends a shipped
// record (BUG-0771): a declaration that cannot be read yet refuses the record with nothing
// appended, instead of failing the apply after the append and fencing the pair for a replay
// no copy runs. A record that Fill would apply as an empty write (a retired field, a dropped
// bucket with proof) resolves without error.
type EngineResolver interface {
	// ResolveEngines returns nil when Fill can resolve what the record needs, or a retryable
	// UNAVAILABLE refusal that names the fix.
	ResolveEngines(db uint64, op cluster.Op) error
}

// RefusalExplainer is implemented by an Applier whose precondition refusals have words of
// their own. Without it a refused precondition is worded by the code alone, which for
// INVALID_ARGUMENT and NOT_FOUND would name the wrong thing.
type RefusalExplainer interface {
	// Explain words the refusal code the applier's Precondition returned for op in the pair
	// (slot, db), or reports false when it has nothing more to say than the code. The pair
	// lets it read the state the refusal came from, because one code has several causes.
	Explain(code cluster.Code, slot uint16, db uint64, op cluster.Op) (string, bool)
}

// ServeChecker is the node's lease (conv §15). The real one wraps machine.LeaseState.
type ServeChecker interface {
	// Serve re-checks the lease at check.At for the committed epoch.
	Serve(check machine.ServeCheck) machine.LeaseDecision
}

// Replication hands positions to the copies and reports what they confirmed.
type Replication interface {
	// Enqueue queues pos for every copy. It is called under the pair lock and must not wait.
	// cursor is the log sequence the owner just appended pos at: a copy the stream has not
	// shipped this pair to yet starts there instead of at the start of the retained log.
	// prevEnd is the previous end an epoch's opener carries, zero for any other record.
	Enqueue(slot uint16, db uint64, pos, prevEnd cluster.Position, cursor uint64)
	// Await blocks until copies other than the owner acked pos or ctx ends, and returns
	// how many had.
	Await(ctx context.Context, slot uint16, db uint64, pos cluster.Position, copies int) int
	// NoteWaitTimeout reports that a concern wait for pos ran the whole wc_timeout and ended
	// with copies missing, so the stream can stop making later writes wait for them
	// (the entry ring degraded ack breaker spec). It is never called for a wait the
	// client's own deadline cut short. waited is how long the wait ran.
	NoteWaitTimeout(slot uint16, db uint64, pos cluster.Position, waited time.Duration)
	// Acked is how many copies other than the owner acked a position at or past pos now.
	Acked(slot uint16, db uint64, pos cluster.Position) int
	// Covered reports a HANDOFF_READY drain that covers pos, or an ack from the copy
	// that owns the pair now (write path §9).
	Covered(slot uint16, db uint64, pos cluster.Position) bool
}

// WriteRequest is one operation reaching the owner.
type WriteRequest struct {
	Slot      uint16
	DBID      uint64
	Op        cluster.Op
	RequestID [16]byte
	// Deadline is the client's remaining budget; 0 means none.
	Deadline time.Duration

	// ClientEpoch is the epoch a client prologue carried, 0 for "route it for me".
	ClientEpoch uint64
	// FromNode marks a node frame, whose FrameEpoch check 1 compares instead.
	FromNode   bool
	FrameEpoch uint64
	// Hop is the forward count of check 3.
	Hop uint8
	// OpIndex is the operation's index in its client batch, 0 for a single operation.
	OpIndex uint8
	// BatchSize is the number of operations in the client batch, 0 or 1 for a single one.
	BatchSize int
	// Concern is the write concern asked for; zero takes the database's default.
	Concern machine.Concern
	// ClaimSink receives the messages a claim leased by this attempt; nil when nobody
	// waits for them.
	ClaimSink ClaimSink
	// Rewrite, when set, runs at the owner inside the critical section, under the pair lock
	// and before the checks, and returns the fields the operation writes in place of Op.Fields
	// (an Update's set of field ids). It is the read-modify-write of conv 20 step 2: it reads
	// the owner's own state, so two writers cannot both build from the same old value. It runs
	// once per ordering attempt, must not block on anything but the owner's own Badger, and
	// must not take a pair lock. A node that is not the owner never calls it, so a request
	// carrying one answers Held or Forward and is not sent over the wire.
	Rewrite func() ([]cluster.FieldOp, error)
}

// WriteAnswer is the owner's answer to one write. A refusal is not an answer: Write
// returns it as a *cluster.Error and no position was consumed.
type WriteAnswer struct {
	// Outcome is machine.Answer's decision; zero when Held or Forward.
	Outcome machine.Outcome
	// Pos is the position the write holds; zero when Held or Forward.
	Pos cluster.Position
	// CopiesConfirmed counts the owner, so an acknowledged write is never below 1.
	CopiesConfirmed uint8
	// Result is the result of a pop, take or claim: the one it just produced, or the
	// recorded one when the answer is deduped.
	Result []byte
	// Deduped says the answer was recomposed from a recorded entry and nothing applied.
	Deduped bool
	// Held says the pair is in a hold: the write took no position and the caller parks it.
	Held bool
	// HoldCause says why the pair is held, filled by the node that parks the write; empty
	// when Held is false or the Writer alone answered.
	HoldCause string
	// Forward says this node is not the owner: the caller forwards the write, once.
	Forward bool
	// ServedBy names the owner that ordered a forwarded write; empty when this node did.
	ServedBy string
	// ApplyFailure is set when the Badger batch failed after the append. The position is
	// durable and the answer stands; the pair now needs a replay (write path §2 row 9).
	ApplyFailure error
	// NotApplied says the op's guard did not hold: nothing was written and no position was
	// taken. Result is then the current document's scalars (EncodeScalarSet) and GuardField
	// the first field id that differed.
	NotApplied bool
	// GuardField is the field id of the first guard term that differed.
	GuardField uint16
	// DocumentTruncated says a deduped retry's recorded document was over the dedupe cap, so
	// Result is empty and the caller runs a get.
	DocumentTruncated bool
}

// WriterConfig is the collaborators of a Writer.
type WriterConfig struct {
	Topology    Topology
	Pairs       *cluster.PairSet
	Log         Appender
	KV          BatchSource
	Applier     Applier
	Clock       machine.Clock
	Lease       ServeChecker
	Dedupe      DedupeReader
	Schema      Schema
	Replication Replication
	// WCTimeout is replication.wc_timeout_ms.
	WCTimeout time.Duration
	// DedupeWindow is replication.dedupe_window and the stored result cap.
	DedupeWindow machine.WindowConfig
	// NotReady, when set, returns why the node's seed record for the pair is not ready ("" when
	// it is); a rank-1 node whose pair is not ready orders nothing and refuses the write. Nil
	// takes every pair as ready (a node with no seed records). The check is made when the write
	// is admitted; a pair that leaves ready after that is stopped by the hold in held(), so the
	// window is one admission.
	NotReady func(slot uint16, db uint64) string
	// Advancer moves the pair record with each applied position; nil takes a
	// LocalAdvancer over Pairs and KV. U44's Reconciler.ApplyAdvance is the other one.
	Advancer PairAdvancer
	// WallMillis is the wall clock a claim, ack, release or extend is stamped with at the
	// owner before the append (conv §9.1); nil takes the system clock.
	WallMillis func() int64
	// StampIndexes, when set, gives the ordering owner the index stamp of an operation it
	// is about to append, called under the pair lock next to the clock stamp so the owner
	// and every copy index identically (storage-format spec §1.2). Nil stamps nothing; the
	// index units fill it.
	StampIndexes func(op cluster.Op) (cluster.IndexStamp, error)
	// Settled reports whether the Handoff has been fed the given committed epoch (BUG-0758).
	// A rank-1 pair orders nothing while it is false, whatever flags it carries. Nil is always
	// settled.
	Settled func(committed uint64) bool
	// GuardObserved, when set, is called once per guarded write after the answer is decided,
	// with true when the guard held and the write applied and false when it refused. It is
	// never called for a deduped answer or an op with no guard. It must not block.
	GuardObserved func(applied bool)
	// DrainObserved, when set, is called once for each time the first section of an RF 1 owner
	// write waited for the pair's pending list to drain, with the wait. Nil observes nothing. It
	// must not block, and it is called with the pair lock held.
	DrainObserved func(waited time.Duration)
	// Floor is the node's log-floor tracker, handed to the LocalAdvancer NewWriter builds when no
	// Advancer is given; nil registers no hold (store-vouched-cut-spec.md §3.6).
	Floor *logFloor
}

// Writer is the owner write path of write path §2 and §3. It keeps no state of its own:
// every pair's state is in Pairs and is touched under that pair's lock only.
type Writer struct {
	cfg WriterConfig
}

// NewWriter returns a Writer over cfg.
func NewWriter(cfg WriterConfig) *Writer {
	if cfg.Advancer == nil {
		local := NewLocalAdvancer(cfg.Pairs, cfg.KV)
		local.floor = cfg.Floor
		cfg.Advancer = local
	}
	if cfg.WallMillis == nil {
		cfg.WallMillis = func() int64 { return time.Now().UnixMilli() }
	}
	return &Writer{cfg: cfg}
}

// maxBatchOps is the op index width of conv §18.
const maxBatchOps = 255

// checkBatch refuses a client batch whose op index would wrap (conv §18).
func (w *Writer) checkBatch(n int) error { return checkBatchSize(n) }

// checkBatchSize is checkBatch without a Writer, for the client side of the same cap.
func checkBatchSize(n int) error {
	if _, ok := machine.BatchOpCount(n); ok {
		return nil
	}
	return &cluster.Error{
		Code: cluster.CodeInvalidArgument,
		Message: fmt.Sprintf("a batch of %d operations is too large: a batch carries at most %d operations, because each is numbered in one byte and a larger index would wrap onto another operation's dedupe entry. Nothing was written. Split it into batches of %d or fewer",
			n, maxBatchOps, maxBatchOps),
	}
}

// rank1 reports whether this node is the owner of slot at the committed epoch.
func (w *Writer) rank1(slot uint16, rf int) bool {
	owner, ok := w.cfg.Topology.Table().Owner(slot, rf)
	return ok && owner.ID == w.cfg.Topology.Self()
}

// noteBehind tells the lease that a request named an epoch above the committed one.
func (w *Writer) noteBehind(req WriteRequest, res machine.AdmissionResult) {
	if res.MarkBehind {
		markBehind(w.cfg.Lease, seenEpoch(req.FromNode, req.FrameEpoch, req.ClientEpoch), !req.FromNode)
	}
}

func (w *Writer) leaseValid() bool { return w.leaseDecision().Valid }

// leaseDecision is one serve check. A refusal that names the lapse is given this decision, so
// the figures it prints are the ones the refusal was made on.
func (w *Writer) leaseDecision() machine.LeaseDecision {
	return w.cfg.Lease.Serve(machine.ServeCheck{At: w.cfg.Clock.BootNow(), CommittedEpoch: w.cfg.Topology.Committed()})
}

// holdOpen is check 2a and 2b for a pair this node would order: a barrier side, a
// pair that has not replayed its log tail, a predecessor not yet proved stopped, or
// an applied position short of the target.
func holdOpen(p cluster.Pair, rank1 bool, target cluster.Position) bool {
	if p.Flags&(flagSending|flagAwaiting) != 0 {
		return true
	}
	if !rank1 {
		return false
	}
	return p.Flags&(flagReplayNeeded) != 0 || p.Flags&flagReplayed == 0 || p.Flags&flagStopProved == 0 ||
		p.AppliedPos.Less(target)
}

// held is holdOpen plus the pair's record: a rank-1 node whose record for the pair is not
// ready is in the barrier and orders nothing (conv §8 check 2, seeding section 3). A copy that
// is not rank 1 is not asked here: its seeding is refused where it applies, as AckNotApplied.
func (w *Writer) held(p cluster.Pair, rank1 bool, slot uint16, db uint64) bool {
	// Same rule as ReadGate.evaluate (BUG-0758): committed is read after rank1's table. Callers
	// hold the pair lock, and a feed publishes its barrier flags under that lock before the
	// applied epoch is stored, so a true Settled here is judged on a pair read after the feed.
	if rank1 && w.cfg.Settled != nil && !w.cfg.Settled(w.cfg.Topology.Committed()) {
		return true
	}
	if holdOpen(p, rank1, w.cfg.Topology.Target(slot, db)) {
		return true
	}
	return rank1 && w.cfg.NotReady != nil && w.cfg.NotReady(slot, db) != ""
}

// notReadyWhy is the cause a rank-1 pair is not ready ("" when it is, or this node is not
// rank 1 or has no seed records).
func (w *Writer) notReadyWhy(slot uint16, db uint64, rf int) string {
	if w.cfg.NotReady == nil || !w.rank1(slot, rf) {
		return ""
	}
	return w.cfg.NotReady(slot, db)
}

// mayOrder is the only gate on assignment (write path §2): rank 1, no hold,
// stopProved, replayed, applied through the target, and a lease unless alone.
func (w *Writer) mayOrder(slot uint16, db uint64) bool {
	dbRow, ok := w.cfg.Topology.Database(db)
	if !ok {
		return false
	}
	return w.mayOrderPair(w.cfg.Pairs.Snapshot(slot, db), slot, db, dbRow)
}

// mayOrderPair is mayOrder over a pair state the caller already holds under its lock.
func (w *Writer) mayOrderPair(p cluster.Pair, slot uint16, db uint64, dbRow DatabaseInfo) bool {
	rank1 := w.rank1(slot, dbRow.RF)
	if !rank1 || w.held(p, rank1, slot, db) {
		return false
	}
	return w.leaseValid() || w.cfg.Topology.Members() == 1
}

// appendedAt is the pair's highest appended seq at the committed epoch (write path §3.1):
// the append the pair's marker says was made at that epoch, floored by the applied seq
// when the applied position is of that epoch; an append made at an earlier epoch counts
// for nothing, so the new epoch starts at seq 1 with the opener.
func appendedAt(p cluster.Pair, committed uint64) uint64 {
	appended := uint64(0)
	if p.AppendedEpoch == committed {
		// An append counts only for the epoch it was made at (write path §3.1): the marker
		// is what keeps an append learned before an epoch move from carrying into the new one,
		// and what lets a record-less pair at 0:0 whose replay learned the log's highest seq
		// order after it instead of reusing a position the log holds.
		appended = p.AppendedSeq
	}
	if p.AppliedPos.Epoch == committed {
		// An orderable pair applied everything it appended, so the applied seq is a floor
		// even when a restart reloaded the pair record and replayed nothing.
		appended = max(appended, p.AppliedPos.Seq)
	}
	return appended
}

func writtenFieldIDs(op cluster.Op) []uint16 {
	ids := make([]uint16, len(op.Fields), len(op.Fields)+len(op.Guard))
	for i, field := range op.Fields {
		ids[i] = field.FieldID
	}
	// A guard on a field the declaration retired or never had is refused with the same
	// FIELD_TYPE a write to it is.
	for _, term := range op.Guard {
		ids = append(ids, term.FieldID)
	}
	return ids
}

// admission reads the world once and fills machine.Admission for checks 1 to 5. Dedupe
// and the precondition are not read here: they are I/O and run only for an operation
// that got that far (see evaluate).
func (w *Writer) admission(p cluster.Pair, req WriteRequest, dbRow DatabaseInfo, readmitted bool) machine.Admission {
	rank1 := w.rank1(req.Slot, dbRow.RF)
	view := w.cfg.Schema.View(req.DBID, req.Op.BucketID)
	seen := req.Op.SchemaVersionSeen
	if readmitted {
		seen = view.Seen
	}
	lease := w.leaseDecision()
	return machine.Admission{
		FromNode:        req.FromNode,
		FrameEpoch:      req.FrameEpoch,
		Committed:       w.cfg.Topology.Committed(),
		ClientEpoch:     req.ClientEpoch,
		Hold:            w.held(p, rank1, req.Slot, req.DBID),
		Rank1:           rank1,
		LeaseValid:      lease.Valid,
		Lease:           lease,
		QuorumLost:      w.quorumLost(),
		Members:         w.cfg.Topology.Members(),
		Present:         view.Present,
		Missing:         view.Missing,
		Decl:            view.Decl,
		Seen:            seen,
		Published:       view.Published,
		ReAdmitted:      readmitted,
		PayloadBucketID: req.Op.BucketID,
		AdmittedVersion: req.Op.DeclVersion,
		WrittenFieldIDs: writtenFieldIDs(req.Op),
		Hop:             req.Hop,
	}
}

// admitAndReEnterOnce is checks 1 to 5 with the one transparent re-admission of check 4:
// when the miss is only schema_version_seen < s, it leaves the section, refetches the
// declaration outside it and evaluates once more with the new seen. No position is
// taken, so nothing is undone. A refetch that fails is DECL_UNAVAILABLE, whose text is
// the whole contract; the cause is the ring's and is not this path's to decide.
func (w *Writer) admitAndReEnterOnce(ctx context.Context, req WriteRequest) (machine.Admission, machine.AdmissionResult) {
	dbRow, ok := w.cfg.Topology.Database(req.DBID)
	if !ok {
		return machine.Admission{}, machine.AdmissionResult{Code: cluster.CodeNotFound}
	}
	var adm machine.Admission
	var res machine.AdmissionResult
	check := func(p *cluster.Pair, readmitted bool) {
		adm = w.admission(*p, req, dbRow, readmitted)
		res = machine.Check(adm)
		w.noteBehind(req, res)
	}
	evaluate := func(readmitted bool) {
		if _, pipelined := w.pipelinedAdvancer(dbRow); pipelined {
			// Checks 1 to 5 read flags and the committed state only, and the section orders nothing,
			// so it must not wait for the pair's pending writes: the first section decides again.
			w.cfg.Pairs.WithPipeline(req.Slot, req.DBID, func(p *cluster.Pair, _ *cluster.Pipeline) bool {
				check(p, readmitted)
				return true
			})
			return
		}
		w.cfg.Pairs.With(req.Slot, req.DBID, func(p *cluster.Pair) { check(p, readmitted) })
	}
	evaluate(false)
	if !res.ReAdmit {
		return adm, res
	}
	if err := w.cfg.Schema.Refetch(ctx, req.DBID, req.Op.BucketID); err != nil {
		return adm, machine.AdmissionResult{Code: refetchCode(err)}
	}
	evaluate(true)
	return adm, res
}

// ordered is what the critical section hands to the answer.
type ordered struct {
	readmit      bool
	held         bool
	forward      bool
	deduped      *machine.DedupeEntry
	durable      bool
	result       []byte
	pos          cluster.Position
	committed    uint64
	applyFailure error
	appendErr    error
	// notSynced is the answer of a write whose batch committed and whose sync failed
	// (storage.ErrCommittedNotSynced): applied once, not known to survive a restart.
	notSynced error
	// guard is set when the guard did not hold: the write took no position.
	guard *GuardVerdict
}

// evaluate is checks 1 to 6a under the pair lock, with Published re-read at this
// instant: the second evaluation of the fence, immediately before the append.
func (w *Writer) evaluate(p cluster.Pair, req WriteRequest, dbRow DatabaseInfo, readmitted bool) (machine.AdmissionResult, *machine.DedupeEntry, cluster.Code, machine.Admission, *GuardVerdict, error) {
	adm := w.admission(p, req, dbRow, readmitted)
	res := machine.Check(adm)
	w.noteBehind(req, res)
	if !res.Admitted() {
		return res, nil, 0, adm, nil, nil
	}
	entry, err := w.cfg.Dedupe.Lookup(req.Slot, req.DBID, req.Op.BucketID, machine.DedupeKey{RequestID: req.RequestID, OpIndex: req.OpIndex})
	if err != nil {
		return res, nil, 0, adm, nil, fmt.Errorf("reading the dedupe entry of request %x: %w", req.RequestID, err)
	}
	// The pair's ordinal is the window's clock (conv §18), and it is read under the same
	// lock the apply moves it under.
	window := machine.Window{Ordinal: p.Ordinal}
	if verdict := window.Apply(machine.LookupGiven{Entry: entry}, w.cfg.DedupeWindow); verdict.Live {
		if verdict.Recorded.Fingerprint != opFingerprint(req.Op) {
			return res, nil, 0, adm, nil, errRequestIDReused
		}
		adm.Deduped = true
		return machine.Check(adm), verdict.Recorded, 0, adm, nil, nil
	}
	// The precondition judges expiry under the declaration this write was admitted with, the one the
	// stamp will use, not under a second read of the cache.
	judged := req.Op
	judged.AdmittedExpire = admittedExpireOf(machine.EffectiveExpire(adm.Decl, adm.Published))
	// The aggregate stamp is taken from the same admitted declaration and rides the dry run of check 6a.
	judged.Aggregates = aggregateStampFor(adm.Decl, req.Op)
	code, err := w.cfg.Applier.Precondition(req.Slot, req.DBID, judged)
	if err != nil {
		return res, nil, 0, adm, nil, fmt.Errorf("reading the precondition of request %x: %w", req.RequestID, err)
	}
	if code != 0 {
		adm.PreconditionFails = true
		return machine.Check(adm), nil, code, adm, nil, nil
	}
	guard, err := w.checkGuard(req, judged)
	if err != nil {
		return res, nil, 0, adm, nil, err
	}
	if guard.NotFound {
		adm.PreconditionFails = true
		return machine.Check(adm), nil, cluster.CodeNotFound, adm, nil, nil
	}
	if guard.Refused {
		return res, nil, 0, adm, &guard, nil
	}
	return res, nil, 0, adm, nil, nil
}

// checkGuard asks the applier whether the op's guard holds, under the pair lock the caller
// holds and after the dedupe lookup and the precondition, so a retry of an applied write is
// answered before its guard is read. An op with no guard is not asked. An applier that
// cannot read a guard refuses the op: applying it unguarded would answer "applied" to a
// condition nobody checked.
func (w *Writer) checkGuard(req WriteRequest, judged cluster.Op) (GuardVerdict, error) {
	if len(req.Op.Guard) == 0 {
		return GuardVerdict{}, nil
	}
	guarder, ok := w.cfg.Applier.(Guarder)
	if !ok {
		return GuardVerdict{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: "this node cannot check a guard, so nothing was written. Send the write without 'if'"}
	}
	verdict, err := guarder.CheckGuard(req.Slot, req.DBID, judged)
	if err != nil {
		return GuardVerdict{}, fmt.Errorf("reading the guard of request %x: %w", req.RequestID, err)
	}
	return verdict, nil
}

// preconditionRefusal is the refusal of check 6a: the applier's own words when it has
// them, otherwise the code's.
func (w *Writer) preconditionRefusal(code cluster.Code, req WriteRequest) error {
	if explainer, ok := w.cfg.Applier.(RefusalExplainer); ok {
		if message, said := explainer.Explain(code, req.Slot, req.DBID, req.Op); said {
			return &cluster.Error{Code: code, Message: message}
		}
	}
	return w.refusal(code, req, cluster.Position{}, nil)
}

// order is the critical section: checks, assignment, append, apply, enqueue (write
// path §2 rows 1 to 10). The advancer takes the pair lock, runs stage under it and
// releases it before the answer waits; the writer never holds the lock when it calls the
// advancer, because the lock is not reentrant.
func (w *Writer) order(ctx context.Context, req WriteRequest, dbRow DatabaseInfo, readmitted bool) (ordered, error) {
	if advancer, pipelined := w.pipelinedAdvancer(dbRow); pipelined {
		return w.orderPipelined(ctx, advancer, req, dbRow, readmitted)
	}
	var out ordered
	var refusal error
	var pair *cluster.Pair
	staged := false
	step := AdvanceStep{
		Ctx: ctx,
		// stage appended exactly when its outcome is durable (an append error, a refusal or a dedupe
		// answer leaves out.durable false).
		Appended: func() bool { return out.durable },
		Stage: func(batch storage.KVBatch, p *cluster.Pair) (AdvanceTo, error) {
			pair, staged = p, true
			var to AdvanceTo
			out, to, refusal = w.stage(p, batch, req, dbRow, readmitted)
			if refusal != nil || !out.durable || out.applyFailure != nil {
				return AdvanceTo{}, errNothingStaged
			}
			return to, nil
		},
		Commit: func(batch storage.KVBatch, sync bool) error {
			if err := batch.Commit(sync); err != nil {
				if errors.Is(err, storage.ErrCommittedNotSynced) {
					// Not row 9: the batch is in the store and the advancer installs it, so the
					// write is applied once and no replay may apply it again (BUG-0809).
					out.notSynced = notSyncedAnswer(out.pos, req.RequestID, err)
					return err
				}
				// Row 9: the position is durable and stays taken. The pair stops serving
				// until its replay re-applies the record.
				pair.Flags |= flagReplayNeeded
				pair.Flags &^= flagReplayed
				w.markFirstApplyFailed(pair, req.Slot, req.DBID)
				out.applyFailure, out.result = fmt.Errorf("committing the batch of position %s: %w", out.pos, err), nil
				return out.applyFailure
			}
			return nil
		},
	}
	err := w.cfg.Advancer.Advance(req.Slot, req.DBID, step)
	switch {
	case !staged:
		return ordered{}, fmt.Errorf("advancing slot %d database %d for request %x: %w", req.Slot, req.DBID, req.RequestID, err)
	case refusal != nil:
		return ordered{}, refusal
	case out.notSynced != nil:
		return ordered{}, out.notSynced
	case err == nil || errors.Is(err, errNothingStaged) || out.applyFailure != nil:
		return out, nil
	}
	return ordered{}, fmt.Errorf("advancing slot %d database %d for request %x: %w", req.Slot, req.DBID, req.RequestID, err)
}

// stage is the body of the critical section, run under the pair lock by the advancer. A
// durable result with no applyFailure has filled batch and names the record to move to.
func (w *Writer) stage(p *cluster.Pair, batch storage.KVBatch, req WriteRequest, dbRow DatabaseInfo, readmitted bool) (ordered, AdvanceTo, error) {
	plan, out, proceed, err := w.planStage(p, req, dbRow, readmitted, 0, w.cfg.Topology.Committed)
	if !proceed {
		return out, AdvanceTo{}, err
	}
	pos, committed := plan.pos, plan.committed
	cursor, err := w.cfg.Log.AppendFrom("", plan.op.Class, plan.key, plan.record, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: committed})
	if err != nil {
		// Row 8: fence the pair. No further position is taken until a replay, and
		// appendedSeq is unchanged, so this position is reused.
		p.Flags |= flagReplayNeeded
		return ordered{appendErr: err, pos: pos, committed: committed}, AdvanceTo{}, nil
	}
	p.AppendedSeq, p.AppendedEpoch = pos.Seq, committed
	// The copies read the position from the log, so queueing it before the apply is safe.
	w.cfg.Replication.Enqueue(req.Slot, req.DBID, pos, plan.op.PrevEnd, cursor)

	out = ordered{durable: true, pos: pos, committed: committed}
	rec := AppliedRecord{
		Slot: req.Slot, DB: req.DBID, Op: plan.op, Pos: pos, Cursor: cursor, Ordinal: p.Ordinal + 1,
		RequestID: req.RequestID, OpIndex: req.OpIndex, ClaimSink: req.ClaimSink,
	}
	result, err := w.fill(batch, rec)
	if err != nil {
		// Row 9: the position is durable and stays taken. The pair stops serving until
		// its replay re-applies the record.
		p.Flags |= flagReplayNeeded
		p.Flags &^= flagReplayed
		w.markFirstApplyFailed(p, req.Slot, req.DBID)
		out.applyFailure = err
		return out, AdvanceTo{}, nil
	}
	out.result = result
	return out, AdvanceTo{Applied: pos, WALCursor: cursor, Ordinal: rec.Ordinal}, nil
}

// stagePlan is what the checks of a write assign: the operation as it will be logged, its position and
// its log entry.
type stagePlan struct {
	op        cluster.Op
	pos       cluster.Position
	committed uint64
	key       []byte
	record    []byte
}

// planStage is stage up to the append, and the one place both owner paths (the locked one and the RF 1
// pipeline) run the write's checks: the rewrite, checks 1 to 6a in the order dedupe, precondition, guard,
// then the order decision, with one clock read for the precondition, the guard and the expiry and lease
// stamps, the expire and aggregate stamps from the admitted declaration, and the encoding. proceed false
// means the write is refused, held, forwarded, deduped, re-admitted or its guard did not hold (out.guard),
// none of which takes a position, and out and err are what stage returns for it. ahead is the number of this pair's owner writes that are
// appended and not applied yet: the dedupe window's clock is the pair's ordinal as it will be when they
// apply (write path I7). committedNow is read once, where the epoch is assigned, so a caller that
// decided something on an epoch can hand the same value in.
func (w *Writer) planStage(p *cluster.Pair, req WriteRequest, dbRow DatabaseInfo, readmitted bool, ahead uint64, committedNow func() uint64) (plan stagePlan, out ordered, proceed bool, err error) {
	if req.Rewrite != nil {
		fields, err := req.Rewrite()
		if err != nil {
			return stagePlan{}, ordered{}, false, err
		}
		req.Op.Fields = fields
	}
	view := *p
	view.Ordinal += ahead
	// The one clock read of this ordering attempt: the precondition, the guard, the record's ExpireNow and
	// the lease stamps all use it, so none of them can disagree at a boundary instant.
	attemptNow := w.cfg.WallMillis()
	probe := req
	probe.Op.ExpireNow = attemptNow
	res, entry, code, admitted, verdict, err := w.evaluate(view, probe, dbRow, readmitted)
	if errors.Is(err, errRequestIDReused) {
		return stagePlan{}, ordered{}, false, err
	}
	if err != nil {
		return stagePlan{}, ordered{}, false, w.refusal(cluster.CodeUnavailable, req, cluster.Position{}, err)
	}
	switch {
	case res.Code != 0:
		return stagePlan{}, ordered{}, false, w.refusalFor(res.Code, req, cluster.Position{}, nil, admitted.Lease)
	case res.ReAdmit:
		return stagePlan{}, ordered{readmit: true}, false, nil
	case res.Hold:
		return stagePlan{}, ordered{held: true}, false, nil
	case res.Forward:
		return stagePlan{}, ordered{forward: true}, false, nil
	case res.Deduped:
		return stagePlan{}, ordered{deduped: entry}, false, nil
	case res.PreconditionRefused:
		probe.Op.Aggregates = aggregateStampFor(admitted.Decl, req.Op)
		probe.Op.AdmittedExpire = admittedExpireOf(machine.EffectiveExpire(admitted.Decl, admitted.Published))
		return stagePlan{}, ordered{}, false, w.preconditionRefusal(code, probe)
	}

	if !w.mayOrderPair(view, req.Slot, req.DBID, dbRow) {
		return stagePlan{}, ordered{held: true}, false, nil
	}
	if verdict != nil {
		// Refused before a position: no record, no batch, no dedupe entry (conv section 18), and on the
		// pipelined path no pending entry.
		return stagePlan{}, ordered{guard: verdict}, false, nil
	}

	committed := committedNow()
	lease := w.leaseDecision()
	seq := machine.PairSeq{
		Epoch:       committed,
		AppendedSeq: appendedAt(view, committed),
		PrevEnd:     view.AppliedPos,
		AppliedPos:  view.AppliedPos,
		Target:      w.cfg.Topology.Target(req.Slot, req.DBID),
		Owner:       true,
		Replayed:    true,
		LeaseOK:     lease.Valid,
		Members:     w.cfg.Topology.Members(),
	}
	decision, _ := seq.Apply(machine.OrderRequest{RequestID: req.RequestID})
	if decision.Assign == nil {
		if decision.Refusal != 0 {
			return stagePlan{}, ordered{}, false, w.refusalFor(decision.Refusal, req, cluster.Position{}, nil, lease)
		}
		return stagePlan{}, ordered{held: true}, false, nil
	}

	op, err := w.stamp(req.Op, attemptNow)
	if err != nil {
		return stagePlan{}, ordered{}, false, fmt.Errorf("stamping the operation of request %x: %w", req.RequestID, err)
	}
	// The guard was checked above and is never logged: a copy or a replay applies the record
	// as a function of the log. The copy of the op is this function's own; the caller's keeps it.
	op.Guard = nil
	// The index stamp is the owner's alone and is taken here, under the pair lock, from the
	// table the write was admitted with, so every copy indexes exactly as the owner did.
	if w.cfg.StampIndexes != nil {
		if op.Stamp, err = w.cfg.StampIndexes(op); err != nil {
			return stagePlan{}, ordered{}, false, fmt.Errorf("stamping the indexes of request %x: %w", req.RequestID, err)
		}
	}
	// The expire items come from the declaration this write was admitted under, read under
	// this same lock. The declaration cache is bounded and can lose or change the entry between
	// admission and here, so it is never read again: a second read could stamp nothing and let
	// a document that should expire live for ever.
	if op.Expire = expireItemsOf(machine.EffectiveExpire(admitted.Decl, admitted.Published), op); len(op.Expire) > 0 {
		// The clock rides the record, so a copy and a replay judge "expired" at this instant.
		op.ExpireNow = attemptNow
	} else {
		op.ExpireNow = 0
	}
	// The aggregate stamp is the one the dry run of check 6a used, from the admitted declaration.
	op.Aggregates = aggregateStampFor(admitted.Decl, req.Op)
	op.PrevEnd = cluster.Position{}
	if decision.Assign.Pos.Seq == 1 {
		op.PrevEnd = decision.Assign.PrevEnd
	}
	body, err := cluster.EncodeOp(op)
	if err != nil {
		return stagePlan{}, ordered{}, false, fmt.Errorf("encoding the operation of request %x: %w", req.RequestID, err)
	}
	pos := decision.Assign.Pos
	// A ring entry carries no label: the key's database id and the body's bucket id name the
	// bucket, and EntryBucketLabel derives the operator's form at read time.
	return stagePlan{
		op: op, pos: pos, committed: committed,
		key:    storage.PositionKey(req.Slot, req.DBID, storage.LogPosition(pos)),
		record: storage.EncodeWALRecord(req.OpIndex, req.RequestID, body),
	}, ordered{}, true, nil
}

// stamp gives a claim, ack, release or extend the owner's clock and a claim its nonce
// before the append, so the log record carries everything a replay needs and a replay
// reads no clock (conv §9.1).
func (w *Writer) stamp(op cluster.Op, now int64) (cluster.Op, error) {
	if op.Class == cluster.OpClaim {
		var nonce [16]byte
		if _, err := entropy.Read(nonce[:]); err != nil {
			return op, fmt.Errorf("drawing the nonce of a claim: %w", err)
		}
		return StampClaim(op, now, nonce), nil
	}
	return StampLease(op, now), nil
}

// fill writes one record's keys, its pair-independent dedupe entry and the applier's own
// records into batch, and returns the operation's result. It commits nothing: the
// advancer adds the pair record and the caller commits, so the keys, the dedupe entry and
// the record land together or not at all (write §3.3, §5).
func (w *Writer) fill(batch storage.KVBatch, rec AppliedRecord) ([]byte, error) {
	return fillApplied(batch, w.cfg.Applier, rec, w.cfg.DedupeWindow)
}

// notSyncedAnswer is the answer of a write whose batch committed and whose sync failed: the
// pair-lock spec's unknown outcome (F3, F12), an error with no refusal code. The write is applied
// and readable, but a crash before the store reaches the disk can lose the batch, and with it the
// pair's record, so the client cannot be told it is kept. A retry with the same request id is
// deduped while the batch is there, and applied once if a restart lost it.
func notSyncedAnswer(pos cluster.Position, requestID [16]byte, err error) error {
	return fmt.Errorf("position %s of request %x is applied, but the node store could not sync it to disk, so whether it survives a restart is not known; retry with the same request id: %w", pos, requestID, err)
}

// RecordlessReporter is implemented by an advancer whose pairs may have no record yet (the
// Reconciler's): a replay skips such a pair, so a row 9 on it must be marked for its recovery.
type RecordlessReporter interface {
	// Recordless reports whether the pair has no record. The caller holds the pair's lock.
	Recordless(slot uint16, db uint64) bool
}

// markFirstApplyFailed marks a row 9 on a pair with no record (flagFirstApplyFailed). The caller
// holds the pair's lock and has just set flagReplayNeeded. It asks the advancer only here, on the
// failure path, so a write that applies pays nothing for it.
func (w *Writer) markFirstApplyFailed(p *cluster.Pair, slot uint16, db uint64) {
	if reporter, ok := w.cfg.Advancer.(RecordlessReporter); ok && reporter.Recordless(slot, db) {
		p.Flags |= flagFirstApplyFailed
	}
}

// fillApplied is the one apply of a record on any node: the applier's keys, with the
// result when it returns one, then the dedupe entry in the same batch. The owner's
// fill, its replay and a copy's commit all call it, so a copy holds the entry the owner
// wrote, byte for byte, and a retry after a failover is answered rather than applied
// again (write §5; BUG-0749). A record without a request id names no client operation,
// so it gets no entry: nothing could ever retry it. rec.Ordinal must be at least 1: it
// counts this record, and the window cut below is one less.
func fillApplied(batch storage.KVBatch, applier Applier, rec AppliedRecord, window machine.WindowConfig) ([]byte, error) {
	var result []byte
	var err error
	if filler, ok := applier.(ResultFiller); ok {
		result, err = filler.FillWithResult(batch, rec)
	} else {
		err = applier.Fill(batch, rec)
	}
	if err != nil {
		return nil, fmt.Errorf("building the batch of position %s: %w", rec.Pos, err)
	}
	if rec.RequestID == ([16]byte{}) {
		return result, nil
	}
	// rec.Ordinal already counts this record, so the window starts one below it and the
	// machine's own increment lands on the same number the pair record was given.
	cut := machine.Window{Ordinal: rec.Ordinal - 1}
	ref := storage.BucketRef{Slot: rec.Slot, DatabaseID: rec.DB, BucketID: rec.Op.BucketID}
	recorded := result
	if returnsDocument(rec.Op) && len(result) > window.MaxResult {
		// A document over the cap is not refused after the append the way a pop's element is
		// (that would fence the pair): the entry keeps a mark, the first answer carries the
		// whole document, and a retry is told to run a get.
		recorded = []byte{resultTruncated}
	}
	write := machine.DedupeWrite{Key: machine.DedupeKey{RequestID: rec.RequestID, OpIndex: rec.OpIndex}, Pos: rec.Pos, Fingerprint: opFingerprint(rec.Op), Result: recorded}
	if _, err := RecordDedupe(batch, ref, cut, write, window); err != nil {
		return nil, fmt.Errorf("building the batch of position %s: %w", rec.Pos, err)
	}
	return result, nil
}

// returnsDocument says op is an update or delete asked to answer with a document.
func returnsDocument(op cluster.Op) bool {
	return (op.Class == cluster.OpUpdate || op.Class == cluster.OpDelete) && op.Return != cluster.ReturnNone
}

// Write orders one operation on its pair and answers it (write path §2, §3, §7, §9).
// It refuses with a *cluster.Error and consumes no position for rows 1 to 6a; it
// returns Held or Forward for an operation it may not order; otherwise the answer is
// machine.Answer of the state it reports.
func (w *Writer) Write(ctx context.Context, req WriteRequest) (WriteAnswer, error) {
	// Conventions section 8 is silent on the batch cap and the class check, so they
	// sit right after check 1: the epoch is the first thing a frame is judged on.
	if code := w.epochRefusal(req); code != 0 {
		return WriteAnswer{}, w.refusal(code, req, cluster.Position{}, nil)
	}
	if err := w.checkBatch(req.BatchSize); err != nil {
		return WriteAnswer{}, err
	}
	if !w.cfg.Applier.Handles(req.Op.Class) {
		return WriteAnswer{}, &cluster.Error{
			Code:    cluster.CodeInvalidArgument,
			Message: fmt.Sprintf("operation class %#02x is not built yet, so nothing was written", req.Op.Class),
		}
	}
	dbRow, ok := w.cfg.Topology.Database(req.DBID)
	if !ok {
		return WriteAnswer{}, w.refusal(cluster.CodeNotFound, req, cluster.Position{}, nil)
	}

	if why := w.notReadyWhy(req.Slot, req.DBID, dbRow.RF); why != "" {
		// A pair that cannot become ready must not park the owner's callers for good (T3 review F5).
		// A retired record is refused on purpose even when the committed table keeps the pair
		// here: while retired the pair took no replicated write, so it is not a copy until the
		// gate, the delete and the new seed have run. What must not happen is a retire the
		// table never asked for. A pass over a state older than the live one used to retire
		// the record a live write had just created (BUG-0757); Reconciler.reconcile now leaves
		// such a pair to the pass over the newer state.
		return WriteAnswer{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"slot %d of database %d is %s on this node, so it orders no write. Nothing was written. Run 'status seeding' to see its progress, then retry: a retry with the same request id is safe", req.Slot, req.DBID, why)}
	}

	adm, res := w.admitAndReEnterOnce(ctx, req)
	switch {
	case res.Code != 0:
		return WriteAnswer{}, w.refusalFor(res.Code, req, cluster.Position{}, nil, adm.Lease)
	case res.Hold:
		return WriteAnswer{Held: true}, nil
	case res.Forward:
		return WriteAnswer{Forward: true}, nil
	}
	req.Op.SchemaVersionSeen = adm.Seen

	readmitted := adm.ReAdmitted
	for {
		out, err := w.order(ctx, req, dbRow, readmitted)
		if err != nil {
			return WriteAnswer{}, err
		}
		if !out.readmit {
			return w.conclude(ctx, req, dbRow, out)
		}
		// A bump landed between the first evaluation and the append and left the
		// bucket's own table alone: the one re-admission, outside the section.
		if err := w.cfg.Schema.Refetch(ctx, req.DBID, req.Op.BucketID); err != nil {
			return WriteAnswer{}, w.refusal(refetchCode(err), req, cluster.Position{}, nil)
		}
		req.Op.SchemaVersionSeen = w.cfg.Schema.View(req.DBID, req.Op.BucketID).Seen
		readmitted = true
	}
}

// epochRefusal is conv §8 check 1 alone, so the refusals that precede the admission
// proper cannot outrun it.
func (w *Writer) epochRefusal(req WriteRequest) cluster.Code {
	res := machine.Check(machine.Admission{
		FromNode: req.FromNode, FrameEpoch: req.FrameEpoch,
		ClientEpoch: req.ClientEpoch, Committed: w.cfg.Topology.Committed(),
	})
	w.noteBehind(req, res)
	return res.Code
}

// levelTarget is the confirmed count, owner included, a level needs.
func levelTarget(level machine.Concern, rf int) int {
	switch level {
	case machine.ConcernMajority:
		return int(machine.MajorityOf(uint8(rf)))
	case machine.ConcernAll:
		return rf
	default:
		return 1
	}
}

// conclude is the part of the answer that runs outside the pair lock: wait for the
// concern, re-read the lease, and let machine.Answer decide.
func (w *Writer) conclude(ctx context.Context, req WriteRequest, dbRow DatabaseInfo, out ordered) (WriteAnswer, error) {
	switch {
	case out.held:
		return WriteAnswer{Held: true}, nil
	case out.forward:
		return WriteAnswer{Forward: true}, nil
	case out.guard != nil:
		w.observeGuard(false)
		return WriteAnswer{NotApplied: true, GuardField: out.guard.FieldID, Result: out.guard.Scalars}, nil
	}

	level := req.Concern
	if level == 0 {
		level = dbRow.DefaultConcern
	}
	target := levelTarget(level, dbRow.RF)
	state := machine.WriteState{Durable: out.durable, Level: level, RF: uint8(dbRow.RF)}
	answer := WriteAnswer{ApplyFailure: out.applyFailure}

	if out.durable {
		answer.Result = out.result
	}
	if out.deduped != nil {
		answer.Deduped, answer.Pos, answer.Result = true, out.deduped.Pos, out.deduped.Result
		if returnsDocument(req.Op) {
			if _, truncated := splitGuardResult(answer.Result); truncated {
				answer.Result, answer.DocumentTruncated = nil, true
			}
		}
		state.Durable = true
		acked := w.cfg.Replication.Acked(req.Slot, req.DBID, out.deduped.Pos)
		state.CopiesConfirmed = confirmed(acked)
		state.LevelMet = int(state.CopiesConfirmed) >= target
	} else if out.durable {
		answer.Pos = out.pos
		acked := 0
		if need := target - 1; need > 0 {
			budget := w.waitBudget(req)
			waitCtx, cancel := context.WithTimeout(ctx, budget)
			acked = w.cfg.Replication.Await(waitCtx, req.Slot, req.DBID, out.pos, need)
			// Only a wait that ran the whole wc_timeout is evidence about a copy: Await also returns
			// early (every missing copy already suspect or not a copy), and a client deadline ended
			// first says nothing about a peer. Reporting either would extend a hold with no new evidence.
			if acked < need && budget == w.cfg.WCTimeout && ctx.Err() == nil && waitCtx.Err() != nil {
				w.cfg.Replication.NoteWaitTimeout(req.Slot, req.DBID, out.pos, budget)
			}
			cancel()
		}
		state.CopiesConfirmed = confirmed(acked)
		state.LevelMet = int(state.CopiesConfirmed) >= target
		state.WCTimeout = !state.LevelMet
	}

	var answerLease machine.LeaseDecision
	if state.Durable {
		state.OwnerNow = w.rank1(req.Slot, dbRow.RF)
		if !state.OwnerNow {
			state.DrainedOrAcked = w.cfg.Replication.Covered(req.Slot, req.DBID, answer.Pos)
		}
		answerLease = w.leaseDecision()
		state.LeaseValidAtAnswer = answerLease.Valid || w.cfg.Topology.Members() == 1
	}

	outcome, code := machine.Answer(state)
	if code != 0 {
		return WriteAnswer{}, w.refusalFor(code, req, out.pos, out.appendErr, answerLease)
	}
	answer.Outcome, answer.CopiesConfirmed = outcome, state.CopiesConfirmed
	if len(req.Op.Guard) > 0 && out.deduped == nil && out.durable {
		w.observeGuard(true)
	}
	return answer, nil
}

func (w *Writer) observeGuard(applied bool) {
	if w.cfg.GuardObserved != nil {
		w.cfg.GuardObserved(applied)
	}
}

// confirmed is the owner plus the copies that acked: the owner's own append returned,
// so the count never starts at 0.
func confirmed(acked int) uint8 {
	return uint8(min(1+acked, 255))
}

// waitBudget is the concern wait: wc_timeout_ms or the client's deadline, whichever
// comes first.
func (w *Writer) waitBudget(req WriteRequest) time.Duration {
	if req.Deadline > 0 && req.Deadline < w.cfg.WCTimeout {
		return req.Deadline
	}
	return w.cfg.WCTimeout
}

// Replay applies one record read back from the log after a restart. It evaluates no
// schema fence: the record carries the field ids and the declaration version the owner
// used, and a bump committed after an acknowledged record must not make its replay
// refuse it (conv §20). A record at or before the pair's applied position is skipped.
func (w *Writer) Replay(rec cluster.WALRecord) error {
	return w.replay(rec, nil)
}

// replayDeferrals is the set of pairs whose boot replay stopped at a record that needs a
// declaration the node cannot read yet (BUG-0760). One pass of the boot scan fills it from one
// goroutine; the goroutine that finishes the replay reads it between passes.
type replayDeferrals struct {
	mu     sync.Mutex
	cursor map[pairKey]uint64 // the log cursor of the first record each pair has not applied
	cause  map[pairKey]error  // why the pair stopped
	// prior are the pairs an earlier pass stopped: one of them that is not ready now (the
	// reconciler retired it) stays stopped, and fenced, rather than failing the pass.
	prior map[pairKey]struct{}
	// onStop runs, under no lock of ours, when a pair stops: the node marks it so no admission
	// path decides it before the replay has finished.
	onStop func(slot uint16, db uint64)
	// reapplied counts the records this pass committed, for the boot replay's INFO line.
	reapplied int
}

// countReapplied counts one record this pass committed.
func (d *replayDeferrals) countReapplied() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reapplied++
}

// reappliedCount returns the records this pass committed.
func (d *replayDeferrals) reappliedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reapplied
}

func newReplayDeferrals(prior map[pairKey]struct{}, onStop func(slot uint16, db uint64)) *replayDeferrals {
	return &replayDeferrals{cursor: map[pairKey]uint64{}, cause: map[pairKey]error{}, prior: prior, onStop: onStop}
}

// skips reports whether the rest of the pair's records are left alone in this pass.
func (d *replayDeferrals) skips(slot uint16, db uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := pairKey{slot, db}
	_, stopped := d.cursor[key]
	return stopped
}

// add records that the pair stopped at the record read at cursor, for cause.
func (d *replayDeferrals) add(slot uint16, db, cursor uint64, cause error) {
	d.mu.Lock()
	d.cursor[pairKey{slot, db}] = cursor
	d.cause[pairKey{slot, db}] = cause
	d.mu.Unlock()
	if d.onStop != nil {
		d.onStop(slot, db)
	}
}

// wasStopped reports whether an earlier pass stopped the pair.
func (d *replayDeferrals) wasStopped(slot uint16, db uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, was := d.prior[pairKey{slot, db}]
	return was
}

// stopped lists the pairs that stopped in this pass, the lowest cursor among them (0 when none
// stopped) and the cause of the first, in key order, for a report.
func (d *replayDeferrals) stopped() (keys map[pairKey]struct{}, lowest uint64, cause error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys = map[pairKey]struct{}{}
	var first *pairKey
	for key, cursor := range d.cursor {
		keys[key] = struct{}{}
		if lowest == 0 || cursor < lowest {
			lowest = cursor
		}
		if first == nil || key.slot < first.slot || (key.slot == first.slot && key.db < first.db) {
			k := key
			first = &k
		}
	}
	if first != nil {
		cause = fmt.Errorf("slot %d of database %d: %w", first.slot, first.db, d.cause[*first])
	}
	return keys, lowest, cause
}

// replay is Replay. With a non-nil d it is the boot replay: a record whose declaration is not
// readable yet stops its pair instead of failing the open. The pair is fenced (flagReplayNeeded,
// the flag a failed apply sets), every later record of it is skipped here, in log order, and
// Node.finishBootReplay applies them all, in the same order, once the declaration can be read.
// A record is never dropped: the pair record keeps its cursor, so the log keeps the tail.
func (w *Writer) replay(rec cluster.WALRecord, d *replayDeferrals) error {
	// Scanning the tail is how appendedSeq is learned (TailScanner), so it is raised for every
	// record before the advancer runs, under the pair's lock on its own: a record-less pair's
	// replay stages nothing (ApplyAdvance's Replay case), and a raise inside the Stage would
	// leave its AppendedSeq at 0 while the log holds the position. The raise carries the
	// record's own epoch, never the committed one: at a boot the committed state is not
	// applied yet (epoch 0 on a multi-node node until a leader's commit index arrives), and
	// appendedAt counts the append only once that epoch is the committed one, so an older or
	// a not-yet-committed epoch learned here is inert.
	w.cfg.Pairs.With(rec.Slot, rec.DB, func(p *cluster.Pair) {
		if rec.Pos.Epoch > p.AppendedEpoch || (rec.Pos.Epoch == p.AppendedEpoch && rec.Pos.Seq > p.AppendedSeq) {
			p.AppendedSeq, p.AppendedEpoch = rec.Pos.Seq, rec.Pos.Epoch
		}
	})
	if d != nil && d.skips(rec.Slot, rec.DB) {
		return nil
	}
	op, err := cluster.DecodeOp(rec.Body)
	if err != nil {
		return fmt.Errorf("replaying position %s of slot %d: %w", rec.Pos, rec.Slot, err)
	}
	step := AdvanceStep{
		Replay: true,
		Pos:    rec.Pos,
		Stage: func(batch storage.KVBatch, p *cluster.Pair) (AdvanceTo, error) {
			if !p.AppliedPos.Less(rec.Pos) {
				return AdvanceTo{}, errNothingStaged
			}
			applied := AppliedRecord{
				Slot: rec.Slot, DB: rec.DB, Op: op, Pos: rec.Pos, Cursor: uint64(rec.Cursor), Ordinal: p.Ordinal + 1,
				RequestID: rec.RequestID, OpIndex: rec.OpIndex, BootReplay: d != nil,
			}
			if _, err := w.fill(batch, applied); err != nil {
				return AdvanceTo{}, err
			}
			return AdvanceTo{Applied: rec.Pos, WALCursor: uint64(rec.Cursor), Ordinal: applied.Ordinal}, nil
		},
		Commit: func(batch storage.KVBatch, sync bool) error {
			if err := batch.Commit(sync); err != nil {
				return fmt.Errorf("committing the batch of position %s: %w", rec.Pos, err)
			}
			if d != nil {
				d.countReapplied()
			}
			return nil
		},
	}
	if err := w.cfg.Advancer.Advance(rec.Slot, rec.DB, step); err != nil && !errors.Is(err, errNothingStaged) {
		if d != nil && errors.Is(err, ErrDeclUnavailable) {
			// Nothing was staged or committed for this record, so the pair's applied position and
			// record are where the last good record left them.
			w.cfg.Pairs.With(rec.Slot, rec.DB, func(p *cluster.Pair) {
				p.Flags |= flagReplayNeeded | flagBootReplay
				p.Flags &^= flagReplayed
			})
			d.add(rec.Slot, rec.DB, uint64(rec.Cursor), err)
			return nil
		}
		if errors.Is(err, errOwedNotHeld) {
			// Another pair's owed record, refused because this node left its copy set: that pair stays
			// fenced with its flag set and reports the refusal from its own recovery (recoverPair);
			// failing here would fail every scan that passes the record.
			return nil
		}
		if d != nil && errors.Is(err, ErrPairNotReady) && d.wasStopped(rec.Slot, rec.DB) {
			// The reconciler retired a pair whose tail is unreplayed. Its fence lags the log (the
			// applied position stopped at the first unreplayed record), so the pair is not released
			// as if its data were somebody else's: it stays stopped and fenced, and the retire gate
			// passes nothing for it while it is deferred (gateDeps.Deferred).
			d.add(rec.Slot, rec.DB, uint64(rec.Cursor), err)
			return nil
		}
		return fmt.Errorf("replaying position %s of slot %d: %w", rec.Pos, rec.Slot, err)
	}
	return nil
}

// refusalConditions names the text each code the owner path can raise reads.
var refusalConditions = map[cluster.Code]cluster.Condition{
	cluster.CodeNotFound:         cluster.CondBucketNotFound,
	cluster.CodeStaleEpoch:       cluster.CondStaleEpoch,
	cluster.CodeEpochFenced:      cluster.CondEpochFenced,
	cluster.CodeNotOwner:         cluster.CondNotOwner,
	cluster.CodeLeaseLapsed:      cluster.CondLeaseLapsed,
	cluster.CodeNoQuorum:         cluster.CondNoQuorum,
	cluster.CodeSchemaNotCurrent: cluster.CondSchemaNotCurrent,
	cluster.CodeBucketDropping:   cluster.CondBucketDropping,
	cluster.CodeFieldType:        cluster.CondFieldNotDeclared,
	cluster.CodeDocumentExists:   cluster.CondDocumentExists,
	cluster.CodeDeclUnavailable:  cluster.CondDeclUnavailable,
	cluster.CodeUnavailable:      cluster.CondPoisonedLog,
}

// fillSchemaNotCurrent sets the arguments of the owner-fence SCHEMA_NOT_CURRENT text and
// returns the condition to render. The owner path holds ids, so the names come from the
// declaration cache entry and the database row; when either is gone (the bucket was dropped
// meanwhile) it returns the unnamed condition, which names no id and quotes no command.
func fillSchemaNotCurrent(args map[string]string, schema Schema, topo Topology, dbID, bucketID uint64) cluster.Condition {
	view := schema.View(dbID, bucketID)
	_, declared := machine.EffectiveTable(view.Decl, view.Published)
	args["d"], args["s"] = strconv.FormatInt(declared, 10), strconv.FormatInt(view.Published, 10)
	row, dbKnown := topo.Database(dbID)
	if !view.Present || view.Name == "" || !dbKnown || row.Name == "" {
		return cluster.CondSchemaNotCurrentUnnamed
	}
	args["db"], args["bucket"] = row.Name, view.Name
	return cluster.CondSchemaNotCurrent
}

// refusal is the coded error of a write that took no position. The owner path holds
// ids, not names, so a placeholder it cannot fill stays visible in the text rather
// than hiding; the schema fence resolves its names with fillSchemaNotCurrent.
// cause, when set, is wrapped so errors.Is still reaches it.
func (w *Writer) refusal(code cluster.Code, req WriteRequest, pos cluster.Position, cause error) error {
	return w.refusalFor(code, req, pos, cause, machine.LeaseDecision{})
}

// refusalFor is refusal for a write refused on lease, which is the lease decision the refusal was
// made on: LEASE_LAPSED prints its figures, and a second serve check could race a renewal.
func (w *Writer) refusalFor(code cluster.Code, req WriteRequest, pos cluster.Position, cause error, lease machine.LeaseDecision) error {
	code, seen := behindRefusal(w.cfg.Lease, code, w.cfg.Topology.Committed(), req.FromNode)
	args := map[string]string{
		"node":   w.cfg.Topology.Self(),
		"slot":   strconv.FormatUint(uint64(req.Slot), 10),
		"db":     strconv.FormatUint(req.DBID, 10),
		"bucket": strconv.FormatUint(req.Op.BucketID, 10),
		"e":      strconv.FormatUint(w.cfg.Topology.Committed(), 10),
		"epoch":  strconv.FormatUint(w.cfg.Topology.Committed(), 10),
		"seq":    strconv.FormatUint(pos.Seq, 10),
	}
	args["v"] = strconv.Itoa(w.cfg.Topology.Members())
	if seen != 0 {
		args["e_seen"], args["kind"], args["owner"] = strconv.FormatUint(seen, 10), "request", w.cfg.Topology.Self()
	}
	if !pos.Zero() {
		args["epoch"] = strconv.FormatUint(pos.Epoch, 10)
	}
	if cause != nil {
		args["cause"] = cause.Error()
	}
	condition := refusalConditions[code]
	if code == cluster.CodeSchemaNotCurrent {
		condition = fillSchemaNotCurrent(args, w.cfg.Schema, w.cfg.Topology, req.DBID, req.Op.BucketID)
	}
	if code == cluster.CodeLeaseLapsed {
		fillLeaseLapsed(args, lease, w.cfg.Clock.BootNow(), w.cfg.Topology)
		args["id"] = "0x" + hex.EncodeToString(req.Op.EncID)
		if row, ok := w.cfg.Topology.Database(req.DBID); ok && row.Name != "" {
			args["db"] = row.Name
		}
		if name := w.cfg.Schema.View(req.DBID, req.Op.BucketID).Name; name != "" {
			args["bucket"] = name
		}
	}
	coded := &cluster.Error{Code: code, Message: code.Text(condition, args)}
	if cause == nil {
		return coded
	}
	return errors.Join(coded, cause)
}

// QuorumReporter is implemented by a ServeChecker that knows whether this node has lost
// sight of a voter majority (membership 8). A lease that is still valid can outlive the
// majority that granted it by up to step_down_ms; this closes that window.
type QuorumReporter interface {
	// QuorumLost reports that no voter majority was confirmed within the quorum bound.
	QuorumLost() bool
}

func (w *Writer) quorumLost() bool {
	reporter, ok := w.cfg.Lease.(QuorumReporter)
	return ok && reporter.QuorumLost()
}
