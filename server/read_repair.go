package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Versions and quorum reads (item #4). Design: the read-repair-design record.
//
// A write is stamped with a version packed from the bucket's term and the
// origin's log sequence of the entry that produced it (packVersion), so a
// quorum read can tell which copy of a key is freshest and repair a stale
// replica, across owner changes too. The version lives in a count-neutral
// side-key beside the document, never inside the user data, so the read path
// keeps handing stored bytes through untouched.

// The split of a version: the term in the high bits, the sequence in the low
// ones. 19 + 44 = 63 bits, because a version travels as a BSON integer, which
// is signed: the encoder refuses a uint64 above 2^63 - 1, so a 20th term bit
// would make every consistent get of a bucket past term 2^19 fail to encode.
const (
	versionTermBits = 19
	versionSeqBits  = 44
)

// ErrVersionLimit is a write or a term refused because its version cannot be
// packed: the log or the membership has outgrown the bits a version gives it.
// A retry never succeeds; the cluster has to be dumped and restored.
var ErrVersionLimit = errors.New("versions can no longer order writes")

// versionLimitsHook, when set, returns the term and sequence limits in place of
// 2^19 and 2^44, so a test reaches them with real writes and real membership
// changes. Nil in production.
var versionLimitsHook atomic.Pointer[func() (termLimit, seqLimit uint64)]

// The seams of a consistent get's reads (dispatch 11, step 11.4b). Each is
// given the node it runs on; all are nil in production.
var (
	// afterOwnerVersionedReadHook runs in handleConsistentGet between the
	// owner's answer and the next holder's, on the coordinator.
	afterOwnerVersionedReadHook atomic.Pointer[func(nodeID, database, bucket string)]
	// betweenVersionedReadsHook runs in versionedRead between the document's
	// read and the lineage's, on the answering node.
	betweenVersionedReadsHook atomic.Pointer[func(nodeID, database, bucket string)]
	// ownerLogTailHook substitutes the log tail the owner's answer compares
	// with its position, so a test can make a log look short.
	ownerLogTailHook atomic.Pointer[func(nodeID string, tail uint64) uint64]
	// beforeRepairDeleteHook runs in handleConsistentGet after the last
	// holder's read and before the repair delete is sent, on the coordinator:
	// the one place a write can land that no read saw and that the repair
	// meets.
	beforeRepairDeleteHook atomic.Pointer[func(nodeID, database, bucket string)]
	// beforeRepairPutHook runs in compareCopies after every holder's read and
	// before the repair put is sent, on the coordinator: the window a client
	// write can land in that no read saw and that the repair put meets
	// (BUG-0480).
	beforeRepairPutHook atomic.Pointer[func(nodeID, database, bucket string)]
)

// versionLimits are the first term and the first sequence a version cannot
// hold.
func versionLimits() (termLimit, seqLimit uint64) {
	if hook := versionLimitsHook.Load(); hook != nil {
		return (*hook)()
	}
	return 1 << versionTermBits, 1 << versionSeqBits
}

// versionLimitError is the limit a version ran into: the log's or the
// term's.
type versionLimitError struct {
	logFull bool
}

func (e *versionLimitError) Error() string {
	if e.logFull {
		return "this node's log has reached 2^44 entries, and versions can no longer order writes"
	}
	return "the cluster has passed 524,287 membership changes, and versions can no longer order writes"
}

func (e *versionLimitError) Unwrap() error { return ErrVersionLimit }

// packVersion is a write's read-repair version: max(term, 1) in the high 19
// bits and seq, the origin's log sequence of the entry, in the low 44. Two
// writes of one name are ordered by term first, because a name has one owner
// per term and that owner's log only grows; so an owner change cannot let an
// older value outrank a newer one, whichever log was longer. A term of 0 — a
// copy that never changed owner since lineages — packs as 1, so every version
// written by this build is above every bare sequence written before it. It
// refuses a term from 2^19 and a sequence from 2^44.
func packVersion(term, seq uint64) (uint64, error) {
	termLimit, seqLimit := versionLimits()
	term = max(term, 1)
	if term >= termLimit {
		return 0, &versionLimitError{}
	}
	if seq >= seqLimit {
		return 0, &versionLimitError{logFull: true}
	}
	return term<<versionSeqBits | seq, nil
}

// versionLimitRefusal is the refusal of a write to, or a term of,
// database/bucket that cannot be versioned, with the way out (rule 3).
func versionLimitRefusal(database, bucket string, err error) error {
	return &sentencedError{cause: err, message: fmt.Sprintf("bucket %q in database %q: %v. "+
		"Dump each database with 'hoarddb dump' and restore it into a new cluster", bucket, database, err)}
}

type versionCtxKey struct{}

// withWriteVersion attaches a write's packed version to a context so the
// store stamping path can write the read-repair version side-key. It is set in
// applyLocal, after the WAL has assigned the sequence, and in replay — client
// and single-node dispatches carry no version and write without one.
func withWriteVersion(ctx context.Context, version uint64) context.Context {
	return context.WithValue(ctx, versionCtxKey{}, version)
}

// writeVersionFrom returns the version attached by withWriteVersion, or 0
// when the dispatch did not come from the replicated write path. Every
// replicating hash and btree writer stamps it: one that wrote 0 would leave
// the previous write's version in place.
func writeVersionFrom(ctx context.Context) uint64 {
	version, _ := ctx.Value(versionCtxKey{}).(uint64)
	return version
}

type applySeqCtxKey struct{}

type replayCtxKey struct{}

// withApplySeq attaches this node's own WAL sequence for the entry being
// applied: what the bucket's applied_seq, the create floor and the DDL gate
// compare against. On an owner it is the write's sequence; on a replica it is
// the sequence this node's own log gave the entry, which is not the owner's
// (build doc SA-17) — that one is the write version, withWriteVersion's.
func withApplySeq(ctx context.Context, seq uint64) context.Context {
	return context.WithValue(ctx, applySeqCtxKey{}, seq)
}

// applySeqFrom returns the apply sequence, or 0 when there is none: a
// standalone node has no WAL.
func applySeqFrom(ctx context.Context) uint64 {
	seq, _ := ctx.Value(applySeqCtxKey{}).(uint64)
	return seq
}

// withReplay marks a dispatch as WAL replay, where an entry at or below its
// bucket's applied_seq is skipped rather than refused (store.Gate).
func withReplay(ctx context.Context) context.Context {
	return context.WithValue(ctx, replayCtxKey{}, true)
}

// isReplay reports whether a dispatch is WAL replay.
func isReplay(ctx context.Context) bool {
	replay, _ := ctx.Value(replayCtxKey{}).(bool)
	return replay
}

type recordFailuresCtxKey struct{}

type commutesCtxKey struct{}

// withRecordFailures marks an apply whose store failure must be recorded in
// its bucket's failed_seqs (spec §4.4 rule 5): the owner's live apply and
// replay. A replica's live apply never carries it — the owner re-sends the
// entry, and recording the first attempt too would make replay apply it twice
// (build doc §1 trap 4 (d)).
func withRecordFailures(ctx context.Context) context.Context {
	return context.WithValue(ctx, recordFailuresCtxKey{}, true)
}

// recordsFailures reports whether withRecordFailures marked the apply.
func recordsFailures(ctx context.Context) bool {
	record, _ := ctx.Value(recordFailuresCtxKey{}).(bool)
	return record
}

// withCommutes attaches replay's answer to SA-27's question for the entry
// being replayed: can it apply beneath its bucket's mark without diverging
// from the replicas.
func withCommutes(ctx context.Context, commutes func(appliedSeq uint64) bool) context.Context {
	return context.WithValue(ctx, commutesCtxKey{}, commutes)
}

type ownerCommittedCtxKey struct{}

// withOwnerCommitted marks a replica's apply of an entry its owner reports it
// committed, so the store leaves a batch-too-large refusal of it unconsumed:
// it is this member's smaller memtable, not the operation's answer.
func withOwnerCommitted(ctx context.Context) context.Context {
	return context.WithValue(ctx, ownerCommittedCtxKey{}, true)
}

// ownerCommitted reports whether withOwnerCommitted marked the apply.
func ownerCommitted(ctx context.Context) bool {
	committed, _ := ctx.Value(ownerCommittedCtxKey{}).(bool)
	return committed
}

type ownerDecidedCtxKey struct{}

// withOwnerDecided carries the owner's verdict on an entry to a replica's
// apply or a replay of it, so an insert follows the owner instead of deciding
// again against this copy (document-placement spec §6.5).
func withOwnerDecided(ctx context.Context, decided store.OwnerDecision) context.Context {
	return context.WithValue(ctx, ownerDecidedCtxKey{}, decided)
}

// ownerDecidedFrom is the verdict withOwnerDecided attached, or none.
func ownerDecidedFrom(ctx context.Context) store.OwnerDecision {
	decided, _ := ctx.Value(ownerDecidedCtxKey{}).(store.OwnerDecision)
	return decided
}

// ownerDecisionOf maps the owner's outcome for an entry to the verdict a
// replica follows (build doc BD-4). An owed entry is one the owner's store
// failed and still owes to its replay: the owner has not decided it, so the
// replica decides as it always has (BUG-0247). Unknown is no verdict either
// (BUG-0249).
func ownerDecisionOf(outcome int32) store.OwnerDecision {
	switch outcome {
	case ownerOutcomeApplied:
		return store.OwnerApplied
	case ownerOutcomeRefusedByConstraint:
		return store.OwnerRefused
	}
	return store.OwnerUndecided
}

type applyOutcomeCtxKey struct{}

// applyOutcome carries a store failure out of a handler, whose own failures
// travel inside its response frame and never as a Go error.
type applyOutcome struct {
	storeFailure error
	// tooLarge is the store's refusal of a batch over its atomic bound. The
	// bound follows each node's memtable, so a replica with a larger one
	// applies the entry this node refused.
	tooLarge bool
	// overBound is a batch put refused as too large, with what the client sent
	// (noteOverBoundBatch). It is read only where the answer to a client is
	// built (overBoundAnswer); an apply of a replica or a replay reads tooLarge.
	overBound *batchTooLargeRefusal
	// anotherIncarnation is a replica's refusal of a create of a bucket it
	// holds in another incarnation (10a.6): the held and the created one.
	anotherIncarnation *incarnationMismatch
	// lineage is the store's answer by the receiver's rule (10b.2): the copy
	// already holds the entry, or it is of another term.
	lineage error
	// committed is an apply of the request that the store carried out. An
	// error answered after it is not a refusal of a write that never
	// happened, so refusedBeforeApply leaves it as it is.
	committed bool
	// heldUnknownID is an insert the owner applied over an _id_ this copy
	// already held (store.ApplyInfo.NoteHeldID): the put went ahead, so the
	// copies agree, and the caller logs and counts that they did not.
	heldUnknownID bool
	// noted is set once noteApplyOutcome ran: the handler reached the store
	// (or settled the record itself), so a failure it answered is not a
	// refusal made before the store.
	noted bool
}

// incarnationMismatch is a held copy's incarnation and a create's.
type incarnationMismatch struct {
	held, created uint64
}

// ownerLacksWrite reports whether this node, the owner, does not hold a write
// it logged and shipped: its store failed the apply, or refused the batch as
// too large where a replica may not. Its replicas may hold it.
func (o *applyOutcome) ownerLacksWrite() bool {
	return o.storeFailure != nil || o.tooLarge
}

// withApplyOutcome attaches a sink noteApplyOutcome fills when the store
// failed the apply. The replica path reads it: spec §4.4 rule 5 has a replica
// answer the owner's push with Success false for a store failure, so that the
// owner re-sends the entry, and a failure reported only inside the handler's
// response frame would be acknowledged as applied.
func withApplyOutcome(ctx context.Context) (context.Context, *applyOutcome) {
	outcome := &applyOutcome{}
	return context.WithValue(ctx, applyOutcomeCtxKey{}, outcome), outcome
}

// applyOutcomeFrom returns the sink withApplyOutcome attached, or nil.
func applyOutcomeFrom(ctx context.Context) *applyOutcome {
	outcome, _ := ctx.Value(applyOutcomeCtxKey{}).(*applyOutcome)
	return outcome
}

// applyInfoFrom is what the store needs to know about the entry being
// applied: this node's sequence for it, whether it is replay, whether a store
// failure is recorded, whether its owner committed it and its owner's verdict
// on it, its origin's sequence and whether this node is the origin, the log's
// first sequence, and — on replay — whether it commutes with the bucket's
// later writes.
func applyInfoFrom(ctx context.Context, s *Server) store.ApplyInfo {
	applied := lineageFrom(ctx)
	info := store.ApplyInfo{
		Seq:            applySeqFrom(ctx),
		Replay:         isReplay(ctx),
		Record:         recordsFailures(ctx),
		OwnerCommitted: ownerCommitted(ctx),
		OwnerDecided:   ownerDecidedFrom(ctx),
		OriginSeq:      applied.originSeq,
		Origin:         applied.originated,
	}
	// Only an insert the owner applied can find a key its owner did not hold.
	if outcome := applyOutcomeFrom(ctx); outcome != nil && info.OwnerDecided == store.OwnerApplied {
		info.NoteHeldID = func() { outcome.heldUnknownID = true }
	}
	if judged, ok := entryTermFrom(ctx); ok {
		info.Term, info.CheckLineage, info.CheckPosition = judged.term, true, judged.checkPosition
	}
	if s.wal != nil {
		info.LogFloor = s.wal.FirstSequence
	}
	if commutes, ok := ctx.Value(commutesCtxKey{}).(func(uint64) bool); ok {
		info.Commutes = commutes
	}
	if info.Record {
		info.NoteCommitAbove = largeCommitBytes
		info.NoteCommitAboveEntries = largeCommitEntries
	}
	if stop := stopRecorderFrom(ctx); stop != nil {
		info.RecordStop = stop.record
	}
	return info
}

// ConsistentGetRequest is the opt-in quorum read.
type ConsistentGetRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Key      string `bson:"key"`
}

// ConsistentGetResponse returns the freshest document found across a quorum of
// holders, whether a stale holder was repaired, and the version used.
type ConsistentGetResponse struct {
	Found    bool     `bson:"found"`
	Data     bson.Raw `bson:"data,omitempty"`
	Version  uint64   `bson:"version"`
	Repaired bool     `bson:"repaired"`
	Message  string   `bson:"message,omitempty"`
}

// VersionedGetRequest is the node-to-node "read this key and its version."
type VersionedGetRequest struct {
	Token    string `bson:"token"`
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Key      string `bson:"key"`
	// FromNode and Store are the asking node and its own word on whether its
	// store is encrypted at rest (Peer.Store). The answer is a document the
	// asker may write to its own store as a repair, so a sealed node answers
	// only an asker that says sealed. Additive: an asker from before
	// the fields says nothing and is refused by a sealed node.
	FromNode string     `bson:"from_node,omitempty"`
	Store    storeClaim `bson:"store,omitempty"`
}

// VersionedGetResponse carries the raw document and its read-repair version,
// and what the asking coordinator judges the answer by (dispatch 11, step
// 11.3). Every field after Error is additive and node-to-node.
type VersionedGetResponse struct {
	Found   bool     `bson:"found"`
	Data    bson.Raw `bson:"data,omitempty"`
	Version uint64   `bson:"version"`
	Error   string   `bson:"error,omitempty"`
	// WrongType says Error is the holder's refusal of the verb as the wrong
	// store type, which the coordinator answers as that holder does.
	WrongType bool `bson:"wrong_type,omitempty"`
	// NotFound says Error is the holder's answer that the bucket, or its
	// database, does not exist on it. Unlike an unreachable holder it did
	// answer, so the coordinator weighs it as an answer.
	NotFound bool `bson:"not_found,omitempty"`
	// Incarnation, Term and Through are the copy's lineage, read on the
	// snapshot the document was read on.
	Incarnation uint64 `bson:"incarnation,omitempty"`
	Term        uint64 `bson:"term,omitempty"`
	Through     uint64 `bson:"through,omitempty"`
	// Epoch is the answering node's membership epoch when it answered.
	Epoch uint64 `bson:"epoch,omitempty"`
	// Settled says the answering node owns the bucket on its ring and has
	// taken it over: its copy is opened and not marked moved.
	Settled bool `bson:"settled,omitempty"`
	// Owes says the copy owes an apply (its failed_seqs is not empty).
	Owes bool `bson:"owes,omitempty"`
	// LogShort says the owner's log ends below the copy's position: the log
	// went backwards, and the position covers writes it no longer holds.
	LogShort bool `bson:"log_short,omitempty"`
	// Packs is set by every node of a build that packs versions from term
	// and sequence; an answer without it is from an older build.
	Packs bool `bson:"packs,omitempty"`
	// Store is the answering node's own word on whether its store is
	// encrypted at rest (Peer.Store).
	Store storeClaim `bson:"store,omitempty"`
}

// versionedAnswer is one holder's answer to a quorum read of a key: the
// document and its version, and the copy's lineage read on the same
// snapshot, with what the coordinator judges it by. Six or more results make
// a struct.
type versionedAnswer struct {
	found                      bool
	data                       bson.Raw
	version                    uint64
	incarnation, term, through uint64
	epoch                      uint64
	settled, owes, logShort    bool
	packs                      bool
	store                      storeClaim
}

// versionedFind returns the stored document and its read-repair version for a
// key, without stripping anything, for the internal quorum path. A bucket this
// node does not have is an error, which the coordinator treats as "not a holder
// of the data yet" and skips without repairing (that is the sweep's job).
func (s *Server) versionedFind(database, bucket, key string) (bool, bson.Raw, uint64, error) {
	answer, err := s.versionedRead(database, bucket, key)
	return answer.found, answer.data, answer.version, err
}

// versionedRead reads a key, its version and the copy's lineage from one
// snapshot: a position read from another snapshot could cover a put applied
// between the two reads, and a consistent get would then read the put's key
// as absent at a position past it. On a cluster member the answer also
// carries this node's epoch, and on the bucket's owner whether it has taken
// the bucket over and whether its log is shorter than its position. A
// standalone node has no lineage to report.
func (s *Server) versionedRead(database, bucket, key string) (versionedAnswer, error) {
	b, snap, err := s.resolveForRead(database, bucket)
	if err != nil {
		return versionedAnswer{}, err
	}
	defer snap.Discard()
	answer, err := readStoredVersion(b, key)
	if err != nil || s.cluster == nil {
		return answer, err
	}
	c := s.cluster
	if hook := betweenVersionedReadsHook.Load(); hook != nil {
		(*hook)(c.self.ID, database, bucket)
	}
	st, err := store.LoadBucketState(snap, b.slot, b.databaseID, b.id.Load())
	if err != nil {
		return versionedAnswer{}, fmt.Errorf("reading the lineage of bucket %q in database %q: %w", bucket, database, err)
	}
	answer.incarnation, answer.term, answer.through = st.Incarnation, st.Term, st.Through
	// A failed apply is carried in memory until the bucket's next batch
	// writes it into failed_seqs, so the state document alone can say a copy
	// owes nothing that does.
	answer.owes, answer.packs = len(st.FailedSeqs) > 0 || s.bucketOwesApplies(database, bucket), true
	answer.epoch, answer.store = c.Epoch(), s.storeClaim()
	placement := placementOf(database, bucket)
	if c.actsAsOwner(placement) {
		awaits, err := c.ownerReadAwaitsTakeOverState(placement, answer.epoch, database, bucket, &st)
		if err != nil {
			return versionedAnswer{}, fmt.Errorf("answering a versioned read: %w", err)
		}
		answer.settled = c.copyIsOpened(placement, st, c.placementMarksOf(placement)) && !awaits
		// The tail is read after the snapshot: read before it, a write between
		// the two reads would leave a healthy owner's position above it.
		tail := c.walLastSequence()
		if hook := ownerLogTailHook.Load(); hook != nil {
			tail = (*hook)(c.self.ID, tail)
		}
		answer.logShort = tail < st.Through
	}
	return answer, nil
}

// readStoredVersion reads a hash or btree document and its version side-key
// from a bucket view built over one snapshot.
func readStoredVersion(b *inMemoryBucket, key string) (versionedAnswer, error) {
	var (
		raw []byte
		err error
	)
	switch b.bucketType {
	case BucketTypeHash:
		raw, err = b.hashStore.Get(key)
	case BucketTypeBTree:
		raw, err = b.btreeStore.Get(key)
	default:
		return versionedAnswer{}, wrongTypeSentence(fmt.Sprintf("consistent get is not supported for bucket type %s.", b.bucketType), b.bucketType)
	}
	if err != nil || raw == nil {
		return versionedAnswer{}, err
	}
	var version uint64
	if b.bucketType == BucketTypeHash {
		version, err = b.hashStore.GetVersion(key)
	} else {
		version, err = b.btreeStore.GetVersion(key)
	}
	if err != nil {
		return versionedAnswer{}, err
	}
	return versionedAnswer{found: true, data: bson.Raw(raw), version: version}, nil
}

// handleVersionedGet answers a peer's raw read of a key plus its version.
// It is node-to-node traffic, so it authenticates with the cluster token.
func handleVersionedGet(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req VersionedGetRequest
	noteRequestDecode("handleVersionedGet", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, err), nil
	}
	// A node with no cluster has no token a peer could hold, and nothing sends
	// it this opcode: a standalone consistent get is answered locally
	// (handleConsistentGet). It refuses like its node-to-node siblings rather
	// than answering a raw read nobody authorized.
	if s.cluster == nil {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{Error: "replication is not enabled on this node"}, nil)
	}
	if !s.cluster.ValidateToken(req.Token) {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{Error: "invalid cluster token"}, nil)
	}
	if err := s.cluster.refuseSenderNotOfConnection(ctx, transport.OpVersionedGet, req.FromNode); err != nil {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{Error: err.Error()}, nil)
	}
	// A spread template holds no client document, only the router's born
	// set, and a versioned read of it would hand that out (spread MVP note,
	// section 2.4): dispatched before any routing, so refused here.
	if s.spreadTemplateHeld(req.Database, req.Bucket) {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{
			Error: spreadRefusal(transport.OpVersionedGet, spreadRefuse, req.Database, req.Bucket).Error()}, nil)
	}
	action := fmt.Sprintf("answer a versioned read of bucket %q in database %q", req.Bucket, req.Database)
	if err := s.cluster.refuseToAnswerPeer(req.FromNode, req.Store, action, ""); err != nil {
		return nodeResponse(transport.OpVersionedGet, frame, VersionedGetResponse{Error: err.Error()}, nil)
	}
	answer, err := s.versionedRead(req.Database, req.Bucket, req.Key)
	resp := VersionedGetResponse{Found: answer.found, Version: answer.version,
		Incarnation: answer.incarnation, Term: answer.term, Through: answer.through, Epoch: answer.epoch,
		Settled: answer.settled, Owes: answer.owes, LogShort: answer.logShort, Packs: answer.packs, Store: answer.store}
	if answer.found {
		resp.Data = answer.data
	}
	if err != nil {
		resp.Error = err.Error()
		resp.WrongType = errors.Is(err, ErrWrongBucketType)
		resp.NotFound = errors.Is(err, ErrBucketNotFound) || errors.Is(err, ErrDatabaseNotFound)
	}
	return nodeResponse(transport.OpVersionedGet, frame, resp, nil)
}

// holderRead is one holder's answer to a quorum read of a key, or why it
// could not answer.
type holderRead struct {
	versionedAnswer
	err error
}

// ErrConsistentGetRefused is a consistent get that cannot compare the copies
// now: a member runs an older build, the members disagree on the membership,
// or the bucket's owner did not answer and the other copies do not agree. A
// retry once the cause has passed can succeed.
var ErrConsistentGetRefused = errors.New("a consistent get cannot compare the copies of the bucket now")

// consistentRead is one consistent get's holders and their answers.
type consistentRead struct {
	req ConsistentGetRequest
	// ownerID is the bucket's owner on the coordinator's ring.
	ownerID string
	reads   map[string]holderRead
}

// handleConsistentGet is the opt-in quorum read (`consistent get`). It reads
// the key from every distinct holder in the bucket's replica set, the owner
// first, and compares only copies of the incarnation the owner serves. While a
// member runs an older build, or a holder answers at another membership epoch,
// it refuses; while the owner cannot be read, it answers only if every other
// holder answered and they agree. A settled owner's absence outranks every
// copy at or below its position, which a bounded delete then removes;
// otherwise it returns the freshest copy and repairs any holder that returned
// a stale or missing copy by propagating the fresh value through the owner.
func handleConsistentGet(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req ConsistentGetRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, err), nil
	}
	if req.Key == "" {
		return nodeResponse(transport.OpConsistentGet, frame, ConsistentGetResponse{Message: "key required"}, nil)
	}

	// A standalone node has no quorum; answer the local raw read.
	if s.cluster == nil {
		found, data, version, err := s.versionedFind(req.Database, req.Bucket, req.Key)
		// A missing bucket or a wrong store type is the refusal every other
		// verb answers (BUG-0469), not a found:false a driver reads as a miss.
		if errors.Is(err, ErrWrongBucketType) || errors.Is(err, ErrBucketNotFound) || errors.Is(err, ErrDatabaseNotFound) {
			return errorResponse(frame, err)
		}
		if err != nil {
			s.logKeyReadFailure(err)
			return nodeResponse(transport.OpConsistentGet, frame, ConsistentGetResponse{Message: err.Error()}, nil)
		}
		return nodeResponse(transport.OpConsistentGet, frame, ConsistentGetResponse{Found: found, Data: data, Version: version}, nil)
	}
	c := s.cluster

	replicas, err := c.Replicas(placementOf(req.Database, req.Bucket))
	if err != nil {
		return nodeResponse(transport.OpConsistentGet, frame, ConsistentGetResponse{Message: err.Error()}, nil)
	}
	if len(replicas) == 0 {
		return nodeResponse(transport.OpConsistentGet, frame, ConsistentGetResponse{Message: "bucket has no holders"}, nil)
	}
	// The gossip gate: refused before any holder is read.
	if member := c.memberWithout(featurePackedVersions); member != "" {
		return errorResponse(frame, upgradeRefusal(req, member))
	}
	read := consistentRead{req: req, ownerID: replicas[0].ID, reads: c.readHolders(ctx, req, replicas)}
	// The store type is the bucket's own, so one holder that refused the verb
	// for it speaks for all: answered as the standalone path does, not as an
	// unanswered holder.
	if refusal := read.wrongTypeRefusal(); refusal != nil {
		return errorResponse(frame, refusal)
	}
	if refusal := read.notFoundRefusal(); refusal != nil {
		return errorResponse(frame, refusal)
	}
	if refusal := read.refuseUnpacked(); refusal != nil {
		return errorResponse(frame, refusal)
	}
	if refusal := c.refuseMixedStores(read); refusal != nil {
		return errorResponse(frame, refusal)
	}
	if refusal := read.refuseOtherEpoch(c.Epoch()); refusal != nil {
		return errorResponse(frame, refusal)
	}
	owner, ownerAnswered := read.reads[read.ownerID]
	if !ownerAnswered || owner.err != nil {
		resp, refusal := read.answerWithoutOwner()
		if refusal != nil {
			return errorResponse(frame, refusal)
		}
		return nodeResponse(transport.OpConsistentGet, frame, resp, nil)
	}
	reference, leftOut, refusal := read.ownersIncarnation()
	if refusal != nil {
		return errorResponse(frame, refusal)
	}
	resp, refusal := c.answerWithOwner(ctx, read, reference)
	if refusal != nil {
		return errorResponse(frame, refusal)
	}
	resp.Message = joinMessages(resp.Message, leftOut)
	return nodeResponse(transport.OpConsistentGet, frame, resp, nil)
}

// answerWithOwner is the read once the owner has answered, over the answers of
// its incarnation. The owner's answer has authority over absence only while it
// is settled — it owns the bucket on its own ring, its copy is opened and not
// marked moved — and its term is at least every other copy's: otherwise
// ownership is moving and the read is refused, as a plain read at that owner
// is. An owner that owes an apply answers by the comparison and repairs
// nothing: a put repair would be logged after the apply it owes. An owner
// whose log is shorter than its position answers by the comparison, which
// repairs by put. An owner that found the key answers by the comparison too.
func (c *Cluster) answerWithOwner(ctx context.Context, r consistentRead, reference map[string]holderRead) (ConsistentGetResponse, error) {
	owner := reference[r.ownerID]
	behind := false
	for _, h := range reference {
		behind = behind || (h.err == nil && h.term > owner.term)
	}
	switch {
	case !owner.settled || behind:
		return ConsistentGetResponse{}, &sentencedError{cause: ErrHandoffInProgress, message: fmt.Sprintf(
			"bucket %q in database %q: ownership of the bucket is moving to %s, which has not taken it over yet; "+
				"nothing was read or repaired. Retry in a moment", r.req.Bucket, r.req.Database, r.ownerID)}
	case owner.owes:
		resp := c.compareCopies(ctx, r.req, reference, nil)
		resp.Message = joinMessages(resp.Message, fmt.Sprintf(
			"the owner %s owes an apply on this bucket, so nothing was repaired; restart hoarddb-server on %s to replay it", r.ownerID, r.ownerID))
		return resp, nil
	case owner.logShort, owner.found:
		return c.compareCopies(ctx, r.req, reference, &RepairReadPosition{Owner: r.ownerID,
			Incarnation: owner.incarnation, Term: owner.term, Through: owner.through}), nil
	}
	return c.answerOwnersAbsence(ctx, r, reference)
}

// answerOwnersAbsence is the read when the settled owner found no document:
// its absence is read at its position, the version of the last entry of its
// term its copy applied, from the snapshot the absence was read on. The owner
// applies every write before it ships it, so a copy at or below that position
// was written by an entry the owner applied, and a later entry removed it: the
// answer is "not found", and one delete bounded by the position removes it
// wherever it is still held — on a holder that did not answer too, since the
// delete is a logged entry. A write that lands after the owner's read is above
// the position on every copy, so the bounded delete spares it. A copy above
// the position was written after the owner's read: it is the answer, and
// nothing is repaired, by delete or by put. A replica's absence is never
// trusted this way: a replica can refuse a write its owner applied.
func (c *Cluster) answerOwnersAbsence(ctx context.Context, r consistentRead, reference map[string]holderRead) (ConsistentGetResponse, error) {
	owner := reference[r.ownerID]
	position, err := packVersion(owner.term, owner.through)
	if err != nil {
		return ConsistentGetResponse{}, versionLimitRefusal(r.req.Database, r.req.Bucket, err)
	}
	unanswered := unansweredHoldersMessage(r.req.Key, r.reads)
	var newest *holderRead
	for _, id := range sortedHolders(reference) {
		if h := reference[id]; h.err == nil && h.found && (newest == nil || h.version > newest.version) {
			newest = &h
		}
	}
	switch {
	case newest == nil:
		return ConsistentGetResponse{Message: unanswered}, nil
	case newest.version > position:
		return ConsistentGetResponse{Found: true, Data: newest.data, Version: newest.version}, nil
	}
	if hook := beforeRepairDeleteHook.Load(); hook != nil {
		(*hook)(c.self.ID, r.req.Database, r.req.Bucket)
	}
	repaired := false
	if err := c.repairKeyByDelete(ctx, r.req, position+1); err != nil {
		c.logger.Warn("read repair failed", "bucket", r.req.Bucket, "key", r.req.Key, "error", err)
	} else {
		repaired = true
	}
	return ConsistentGetResponse{Repaired: repaired, Message: unanswered}, nil
}

// readHolders asks every holder for the key, the owner first.
func (c *Cluster) readHolders(ctx context.Context, req ConsistentGetRequest, replicas []ring.Node) map[string]holderRead {
	reads := make(map[string]holderRead, len(replicas))
	for _, r := range replicas {
		var (
			answer versionedAnswer
			err    error
		)
		if r.ID == c.self.ID {
			answer, err = c.s.versionedRead(req.Database, req.Bucket, req.Key)
		} else {
			answer, err = c.readVersionedAnswer(ctx, r, req)
		}
		reads[r.ID] = holderRead{versionedAnswer: answer, err: err}
		if hook := afterOwnerVersionedReadHook.Load(); hook != nil && r.ID == replicas[0].ID {
			(*hook)(c.self.ID, req.Database, req.Bucket)
		}
	}
	return reads
}

// wrongTypeRefusal is the first holder's refusal of the verb as the wrong store
// type, or nil when no holder gave one.
func (r consistentRead) wrongTypeRefusal() error {
	for _, id := range sortedHolders(r.reads) {
		if err := r.reads[id].err; errors.Is(err, ErrWrongBucketType) {
			return err
		}
	}
	return nil
}

// notFoundRefusal is the refusal a read gets when every holder answered that
// the bucket (or its database) does not exist, the one GET and a standalone
// consistent get give, or nil otherwise. It needs every holder: a holder that
// has the bucket, one that did not answer, or one from before the not_found
// flag is a divergence or an unknown, and the comparison still judges those.
// The first holder's own sentence is kept, so a missing database reads as one;
// the flag does not tell the two apart, and both are NOT_FOUND on the wire.
func (r consistentRead) notFoundRefusal() error {
	holders := sortedHolders(r.reads)
	for _, id := range holders {
		if err := r.reads[id].err; !errors.Is(err, ErrBucketNotFound) && !errors.Is(err, ErrDatabaseNotFound) {
			return nil
		}
	}
	if len(holders) == 0 {
		return nil
	}
	return r.reads[holders[0]].err
}

// upgradeRefusal is the refusal of a consistent get while member runs a
// build that does not pack versions alike.
func upgradeRefusal(req ConsistentGetRequest, member string) error {
	return &sentencedError{cause: ErrConsistentGetRefused, message: fmt.Sprintf(
		"bucket %q in database %q: upgrade %s to this build; a consistent get compares versions every member writes alike",
		req.Bucket, req.Database, member)}
}

// refuseUnpacked is the per-read gate: an answering holder whose answer says
// it does not pack versions runs an older build, which the gossip gate misses
// while that member has not been heard from.
func (r consistentRead) refuseUnpacked() error {
	for _, id := range sortedHolders(r.reads) {
		if h := r.reads[id]; h.err == nil && !h.packs {
			return upgradeRefusal(r.req, id)
		}
	}
	return nil
}

// refuseMixedStores refuses a read whose holders do not all keep their store
// the way this node does: one sealed (encrypted at rest) beside one in
// plaintext. A sealed node never places its data on a plaintext one, so a
// plaintext owner can lack a write a sealed copy holds, and its absence would
// remove that copy. Which side is complete cannot be told, so the read is
// refused. A holder's store is its answer's word, or, for one that did not
// answer — a sealed node refuses a plaintext coordinator — what it last said
// on gossip.
func (c *Cluster) refuseMixedStores(r consistentRead) error {
	own := c.s.storeClaim()
	for _, id := range sortedHolders(r.reads) {
		h := r.reads[id]
		claim := h.store
		if h.err != nil {
			known := c.sealedPeers.knowledge(id)
			if !known.answered {
				continue
			}
			claim = known.claim
		}
		if claim == claimUnreported || claim == own {
			continue
		}
		sealed, plaintext := id, c.self.ID
		if own == claimSealed {
			sealed, plaintext = c.self.ID, id
		}
		return &sentencedError{cause: ErrConsistentGetRefused, message: fmt.Sprintf(
			"bucket %q in database %q: its holders do not keep their stores alike: %s is encrypted at rest and %s is not, "+
				"and a consistent get cannot tell which copy is complete across that boundary. Finish sealing every node: "+
				"on %s, dump its data ('hoarddb dump'), start it on a fresh data directory with encryption.mode=on, and "+
				"restore it ('hoarddb restore'); see Configuration, Encryption at rest", r.req.Bucket, r.req.Database, sealed, plaintext, plaintext)}
	}
	return nil
}

// refuseOtherEpoch is the epoch check: copies are compared only under one
// membership. A holder that answered at another epoch than the coordinator
// reads a different ring, and a membership change is adopted by each member
// in turn, so the read is refused for the moments that takes.
func (r consistentRead) refuseOtherEpoch(epoch uint64) error {
	for _, id := range sortedHolders(r.reads) {
		if h := r.reads[id]; h.err == nil && h.epoch != epoch {
			return &sentencedError{cause: ErrConsistentGetRefused, message: fmt.Sprintf(
				"bucket %q in database %q: %s answered at membership epoch %d and this node is at %d; "+
					"retry once every member has adopted the latest membership", r.req.Bucket, r.req.Database, id, h.epoch, epoch)}
		}
	}
	return nil
}

// ownersIncarnation keeps the answers of the incarnation the owner serves and
// describes the holders it leaves out. A copy of another incarnation — a
// stray, or one a drop has not reached — is never the answer and never
// repaired. Incarnation 0, a copy with no lineage, is never the reference
// while a holder reports a minted one: the read is refused.
func (r consistentRead) ownersIncarnation() (map[string]holderRead, string, error) {
	owner := r.reads[r.ownerID]
	kept := make(map[string]holderRead, len(r.reads))
	var leftOut []string
	for _, id := range sortedHolders(r.reads) {
		h := r.reads[id]
		if h.err == nil && h.incarnation != owner.incarnation {
			if owner.incarnation == 0 {
				return nil, "", &sentencedError{cause: ErrConsistentGetRefused, message: fmt.Sprintf(
					"bucket %q in database %q: its owner %s holds a copy with no incarnation and %s holds incarnation %d; %s",
					r.req.Bucket, r.req.Database, r.ownerID, id, h.incarnation, twoIncarnationsRemedy)}
			}
			leftOut = append(leftOut, fmt.Sprintf("%s holds incarnation %d", id, h.incarnation))
			continue
		}
		kept[id] = h
	}
	if len(leftOut) == 0 {
		return kept, "", nil
	}
	return kept, fmt.Sprintf("consistent get of %q compared only copies of incarnation %d, the one its owner %s serves, and left out %s; %s",
		r.req.Key, owner.incarnation, r.ownerID, strings.Join(leftOut, " and "), twoIncarnationsRemedy), nil
}

// twoIncarnationsRemedy is the way out of two live incarnations of one name.
const twoIncarnationsRemedy = "they are two buckets of one name, created apart: drop the bucket, " +
	"or remove the node that holds the copy that is not wanted ('remove-node <node>')"

// answerWithoutOwner is the read while the bucket's owner cannot be read: it
// answers only if every other holder answered and all of them agree on the
// incarnation, on whether the key is there, and on its version. Otherwise it
// refuses, naming the owner, and repairs nothing: an answer from what is left
// could be a copy the owner has since deleted or overwritten. A write
// acknowledged at majority or all reached a second holder, so it is either
// what the others agree on or the reason they disagree.
func (r consistentRead) answerWithoutOwner() (ConsistentGetResponse, error) {
	var agreed *holderRead
	var incarnations []string
	disagree := false
	for _, id := range sortedHolders(r.reads) {
		if id == r.ownerID {
			continue
		}
		h := r.reads[id]
		if h.err != nil {
			disagree = true
			continue
		}
		incarnations = append(incarnations, fmt.Sprintf("incarnation %d on %s", h.incarnation, id))
		if agreed == nil {
			agreed = &h
			continue
		}
		if h.incarnation != agreed.incarnation || h.found != agreed.found || h.version != agreed.version {
			disagree = true
		}
	}
	if agreed == nil || disagree {
		message := fmt.Sprintf("bucket %q in database %q: its owner %s did not answer, and the other copies do not agree; retry when %s is reachable",
			r.req.Bucket, r.req.Database, r.ownerID, r.ownerID)
		if len(incarnations) > 1 {
			message += fmt.Sprintf(" (the copies read: %s)", strings.Join(incarnations, ", "))
		}
		return ConsistentGetResponse{}, &sentencedError{cause: ErrConsistentGetRefused, message: message}
	}
	if !agreed.found {
		return ConsistentGetResponse{}, nil
	}
	return ConsistentGetResponse{Found: true, Data: agreed.data, Version: agreed.version}, nil
}

// compareCopies is the comparison of the copies of one incarnation: the
// highest version wins, and when repairFrom is set every holder that answered
// with a stale or missing copy is repaired by one put of the winner through
// the owner, bounded by the winner's version so it never overwrites a write
// made after the reads, and carrying repairFrom, the owner's position as it
// was read, so the owner drops it if a delete followed the read (BUG-0603).
// "Not found" is believed only when every holder answered.
func (c *Cluster) compareCopies(ctx context.Context, req ConsistentGetRequest, reads map[string]holderRead, repairFrom *RepairReadPosition) ConsistentGetResponse {
	bestID := ""
	var best holderRead
	for _, id := range sortedHolders(reads) {
		h := reads[id]
		if h.err != nil || !h.found {
			continue
		}
		if bestID == "" || h.version > best.version {
			best, bestID = h, id
		}
	}
	if bestID == "" {
		// A holder that errored or refused (an unreachable peer, a bad token,
		// the encryption-boundary refusal) has not said the key is absent.
		return ConsistentGetResponse{Message: unansweredHoldersMessage(req.Key, reads)}
	}
	repaired := false
	for id, h := range reads {
		if repairFrom == nil || id == bestID || h.err != nil || (h.found && h.version >= best.version) {
			continue
		}
		if hook := beforeRepairPutHook.Load(); hook != nil {
			(*hook)(c.self.ID, req.Database, req.Bucket)
		}
		// A single coordinated put of the fresh value through the owner
		// re-ships to every replica.
		applied, err := c.repairKey(ctx, req, best.data, repairBounds{read: best.version, from: repairFrom})
		if err != nil {
			c.logger.Warn("read repair failed", "bucket", req.Bucket, "key", req.Key, "error", err)
		}
		repaired = applied
		break
	}
	return ConsistentGetResponse{Found: true, Data: best.data, Version: best.version, Repaired: repaired}
}

// sortedHolders is the holders' IDs in order, so a choice between equal
// answers and a refusal's wording are the same on every run.
func sortedHolders(reads map[string]holderRead) []string {
	ids := make([]string, 0, len(reads))
	for id := range reads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// joinMessages joins the non-empty parts of a response's message.
func joinMessages(parts ...string) string {
	var kept []string
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, "; ")
}

// unansweredHoldersMessage describes the holders of a consistent get that
// could not answer, or returns "" when every holder did. Holder IDs are sorted
// so the error shown is the same on every run.
func unansweredHoldersMessage(key string, reads map[string]holderRead) string {
	var failedIDs []string
	for id, h := range reads {
		if h.err != nil {
			failedIDs = append(failedIDs, id)
		}
	}
	if len(failedIDs) == 0 {
		return ""
	}
	sort.Strings(failedIDs)
	firstID := failedIDs[0]
	return fmt.Sprintf("consistent get of %q: %d of %d holders could not answer (%s: %v); the key may exist; "+
		"retry once those holders are reachable",
		key, len(failedIDs), len(reads), firstID, reads[firstID].err)
}

// readVersionedFromPeer asks one holder for a key's raw document and version.
func (c *Cluster) readVersionedFromPeer(ctx context.Context, node ring.Node, req ConsistentGetRequest) (bool, uint64, bson.Raw, error) {
	answer, err := c.readVersionedAnswer(ctx, node, req)
	return answer.found, answer.version, answer.data, err
}

// readVersionedAnswer asks one holder for a key's raw document, its version
// and the copy's lineage.
func (c *Cluster) readVersionedAnswer(ctx context.Context, node ring.Node, req ConsistentGetRequest) (versionedAnswer, error) {
	p := c.peerFor(node)
	cli, err := p.dial(ctx)
	if err != nil {
		return versionedAnswer{}, err
	}
	payload, err := bson.Marshal(VersionedGetRequest{
		Token: c.localToken(), Database: req.Database, Bucket: req.Bucket, Key: req.Key,
		FromNode: c.self.ID, Store: c.s.storeClaim(),
	})
	if err != nil {
		return versionedAnswer{}, fmt.Errorf("encoding versioned get for %q: %w", req.Key, err)
	}
	frame, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpVersionedGet, payload)
	if err != nil {
		return versionedAnswer{}, err
	}
	var resp VersionedGetResponse
	if len(frame.Payload) > 0 {
		if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
			return versionedAnswer{}, fmt.Errorf("decoding %s's versioned get for %q: %w", node.ID, req.Key, err)
		}
	}
	// The document is validated as a client's is: a repair stores it, and a
	// stored document is not measured again. It travels on as resp.Data.
	if len(resp.Data) > 0 {
		if _, err := validateAt(resp.Data, maxDocumentDepth); err != nil {
			return versionedAnswer{}, fmt.Errorf("%s's versioned get for %q carries a document that was not read: %w", node.ID, req.Key, err)
		}
	}
	if resp.WrongType {
		return versionedAnswer{}, &sentencedError{message: resp.Error, cause: ErrWrongBucketType}
	}
	if resp.NotFound {
		return versionedAnswer{}, &sentencedError{message: resp.Error, cause: ErrBucketNotFound}
	}
	if resp.Error != "" {
		return versionedAnswer{}, errors.New(resp.Error)
	}
	return versionedAnswer{found: resp.Found, data: resp.Data, version: resp.Version,
		incarnation: resp.Incarnation, term: resp.Term, through: resp.Through, epoch: resp.Epoch,
		settled: resp.Settled, owes: resp.Owes, logShort: resp.LogShort, packs: resp.Packs, store: resp.Store}, nil
}

// repairWriteError is a refusal a repair write was answered with. Cluster.write
// reports a refusal in two ways, a Go error and a refusal frame in the reply,
// and a repair path that checked only the error would report a repair the owner
// refused. frameError returns non-nil only for an OpError frame.
func repairWriteError(resp ReplResponse) error {
	return frameError(&transport.Frame{Opcode: transport.Opcode(resp.Opcode), Payload: resp.Payload})
}

// repairKey propagates the freshest value of a key, read at version read,
// through the bucket's owner so every replica converges to it. It is a logged
// put, so it reuses the durability and replication path: the owner appends the
// WAL (giving the repair a new version), applies it, and ships to all
// replicas, and a restart replays the same value. It carries the exclusive
// bound read+1, which each copy compares with its own stored version when it
// applies the put: a client write that landed after the read is logged before
// the repair and is above the bound on every copy, so the repair leaves it
// (BUG-0480). A client delete leaves no version to compare with, so the put
// also carries the owner's position as the read saw it, and the owner drops
// the repair before logging it if its log after that position holds a write of
// the key (BUG-0603). Write concern is one because propagation to replicas
// happens on ship regardless; the repair must not block on a down replica. It
// reports whether the owner logged the repair; a dropped one is not an error.
func (c *Cluster) repairKey(ctx context.Context, req ConsistentGetRequest, fresh bson.Raw, bounds repairBounds) (bool, error) {
	var doc map[string]interface{}
	if err := bson.Unmarshal(fresh, &doc); err != nil {
		return false, fmt.Errorf("decoding fresh value for repair: %w", err)
	}
	// Admitted: the value is one an owner already stores, so the name scan does
	// not refuse the repair of a legacy document (typed-fields spec §5.6).
	payload, err := bson.Marshal(InsertRequest{Database: req.Database, Bucket: req.Bucket, Key: req.Key, Data: doc,
		Admitted: true, RepairBound: bounds.read + 1, RepairRead: bounds.from})
	if err != nil {
		return false, fmt.Errorf("encoding repair put for %s: %w", req.Key, err)
	}
	entry := ReplEntry{Op: int32(transport.OpUpsert), Bucket: req.Bucket, Key: []byte(req.Key), Payload: payload}
	resp, err := c.write(ctx, entry, placementOf(req.Database, req.Bucket), WcOne)
	if err != nil {
		return false, fmt.Errorf("repair put for %s: %w", req.Key, err)
	}
	// A repair the owner refused comes back as a refusal frame with a nil Go
	// error, so checking only err would report a repair that did not happen.
	if refused := repairWriteError(resp); refused != nil {
		return false, fmt.Errorf("repair put for %s: %w", req.Key, refused)
	}
	var answer InsertResponse
	if err := bson.Unmarshal(resp.Payload, &answer); err != nil {
		return false, fmt.Errorf("decoding the answer to the repair put for %s: %w", req.Key, err)
	}
	return answer.WriteOutcome != outcomeNotApplied, nil
}

// repairBounds is what a put repair is bounded by: the version the read saw,
// which each copy compares in its apply, and the owner's position the read
// saw, which the owner compares with its log before logging the repair.
type repairBounds struct {
	read uint64
	from *RepairReadPosition
}

// repairKeyByDelete removes a key the bucket's owner no longer holds from the
// copies that still hold it below bound: an ordinary logged delete through the
// owner, carrying the exclusive bound each copy compares with its own stored
// version when it applies the delete. Write concern is one, as a put repair's
// is: the delete reaches every replica by the stream.
func (c *Cluster) repairKeyByDelete(ctx context.Context, req ConsistentGetRequest, bound uint64) error {
	payload, err := bson.Marshal(DeleteRequest{Database: req.Database, Bucket: req.Bucket, Key: req.Key, RepairBound: bound})
	if err != nil {
		return fmt.Errorf("encoding repair delete for %s: %w", req.Key, err)
	}
	entry := ReplEntry{Op: int32(transport.OpDelete), Bucket: req.Bucket, Key: []byte(req.Key), Payload: payload}
	resp, err := c.write(ctx, entry, placementOf(req.Database, req.Bucket), WcOne)
	if err != nil {
		return fmt.Errorf("repair delete for %s: %w", req.Key, err)
	}
	// A repair the owner refused comes back as a refusal frame with a nil Go
	// error, so checking only err would report a repair that did not happen.
	if refused := repairWriteError(resp); refused != nil {
		return fmt.Errorf("repair delete for %s: %w", req.Key, refused)
	}
	return nil
}
