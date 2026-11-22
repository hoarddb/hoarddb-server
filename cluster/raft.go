package cluster

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Raft tuning fixed by membership Formats 3: a catch-up costs one snapshot
// plus a few dozen entries.
const (
	raftTrailingLogs      = 64
	raftSnapshotThreshold = 128
	raftSnapshotsRetained = 2
	raftApplyTimeout      = 10 * time.Second
	raftStartTimeout      = 15 * time.Second
	raftMaxPool           = 3
)

// afterRaftStreamStart is a test seam: OpenRaft calls it once the Raft listener accepts, before the
// open has read the Raft directory. Nil in production.
var afterRaftStreamStart func()

// betweenAddVoterAndSetVoters is a test seam: PromoteWithin calls it with the id once the library's
// AddVoter has committed and before SET_VOTERS is proposed, with voterMu held. Nil in production.
var betweenAddVoterAndSetVoters func(id string)

// HoldPromotionsForTest makes every PromoteWithin in this process call gate between the library's
// AddVoter and the SET_VOTERS proposal (nil removes it), so a test can hold the window in which the
// library counts a node as a voter and the committed state does not. Public because the harness
// lives in another package; set and clear it while no promotion runs.
func HoldPromotionsForTest(gate func(id string)) {
	betweenAddVoterAndSetVoters = gate
}

// RaftConfig opens one node's seat in the cluster state group.
type RaftConfig struct {
	// NodeID is this node's identity in the ring and in the library.
	NodeID string
	// Dir is the node's data directory; the Raft log, stable store and
	// snapshots live under <Dir>/raft.
	Dir string
	// Listener carries the Raft channel. The caller binds it, so a test can
	// use port 0.
	Listener net.Listener
	// StateRange overrides the state-level range this node declares; nil takes the build's
	// (BuildStateRange). Tests use it to stand in for a build at another level.
	StateRange *StateRange
	// DataAddr is this node's data-channel address, written into the ring.
	DataAddr string
	// ClientAddr is where drivers connect to this node, written into the ring beside
	// DataAddr; empty means drivers use DataAddr.
	ClientAddr string
	// Incarnation is this process's incarnation (conventions section 6.2).
	Incarnation int64
	// Bootstrap is true when no operator join was issued for this node. The
	// node then starts a new cluster, but only when there is no existing
	// state and SeedNodes is empty (membership section 4, "Bootstrap guard").
	Bootstrap bool
	// SeedNodes is the start-up contact list. It never bootstraps: a seed
	// list means "find the cluster", not "make one".
	SeedNodes []string
	// Logger receives the library's log; nil logs warnings to stderr.
	Logger hclog.Logger
	// HeartbeatTimeout, ElectionTimeout and LeaderLease override the
	// library's one-second defaults when non-zero.
	HeartbeatTimeout, ElectionTimeout, LeaderLease time.Duration
	// Limits bound the connections the Raft channel holds; zero fields take the defaults.
	Limits RaftLimits
	// PeerFingerprint, when set, binds keys to members: it returns the fingerprint the
	// member id must authenticate with (the bootstrap seed map `id=FP`). With it set,
	// the Raft channel and AdmitPeer admit only a current member whose connection was
	// authenticated with that key, and require the committed cluster id once it exists.
	PeerFingerprint func(id string) (string, bool)
	// Dial opens the connection of an outbound Raft stream; nil dials plain TCP. A node
	// that pins TLS, or a harness that injects faults, supplies it so the Raft channel
	// travels the same path as the other three.
	Dial func(addr string, timeout time.Duration) (net.Conn, error)
	// OnApply, when set, is called after every committed command or restored snapshot, from
	// the FSM goroutine with no lock held. It must not block: it wakes whoever watches the
	// committed state.
	OnApply func()
	// ApplyGate, when set, is called with the command kind before every Apply touches the
	// state: a test seam that holds the FSM at a chosen point so a boot can be driven
	// against an unapplied state. Nil in production.
	ApplyGate func(kind byte)
	// Schema, when set, publishes each committed schema_version on this node the way conv
	// 20 step 6 says; nil leaves publication to the caller (a test that has no pairs).
	Schema SchemaPublisher
}

// SchemaPublisher is what the FSM calls when a database's committed schema_version moves.
// *BumpApplier does the publication; the Publish step needs the node's PairSet, so a node
// supplies the pair of them as one value.
type SchemaPublisher interface {
	// Stage records version as the published one when none is recorded. It runs inside the
	// apply, under the state lock, and must not block.
	Stage(dbID uint64, version int64)
	// Publish makes the committed version to the published one, pair by pair. It runs on the
	// FSM goroutine after the state lock is released, and waits for pair locks.
	Publish(dbID uint64, to int64)
	// Forget drops the record of a dropped database.
	Forget(dbID uint64)
	// Note is told of the bucket a committed BUMP_SCHEMA was about, after Publish. It runs on the
	// FSM goroutine with no state lock held and must not wait on a pair lock.
	Note(dbID uint64, to int64, note BumpNote)
}

// Raft is one node's seat in the cluster state group. The library owns who
// votes; the cluster FSM owns who holds data (membership section 1).
type Raft struct {
	cfg    RaftConfig
	fsm    *clusterFSM
	raft   *hraft.Raft
	stream *streamLayer
	// refused is the ledger of peers an acceptor turned away for their state range; now is its
	// test clock (nil: time.Now).
	refused   refusedLedger
	now       func() time.Time
	store     *raftboltdb.BoltStore
	closeOnce sync.Once
	closeErr  error
	// voterMu is held by Promote and RemoveServer across their two steps (the library's
	// configuration change, then SET_VOTERS), so ReconcileVoters never repairs a change that is
	// still being made and makes the caller's own proposal STATE_MOVED.
	voterMu sync.Mutex
	// orphans are the configured voters with no member row already reported by ReconcileVoters; guarded by voterMu.
	orphans map[string]bool
	// hadState is true when the Raft directory held state before this process opened it.
	hadState bool
	// awaitingAdmission is the BUG-0870 guard's latch: true from an open with no Raft state in join
	// mode until a member admits this node (Admit). While it is true AdmitPeer refuses every channel,
	// the Raft channel included. It lives in memory only: a process that dies before its first Raft
	// write starts held again, and the retried join admits it again.
	awaitingAdmission atomic.Bool
	// openedLastCommand is the index of the last command entry the retained log held when the
	// node opened, 0 when it held none (a snapshot covers the rest). The library restores a
	// snapshot synchronously and applies the rest of the log afterwards, on the FSM goroutine,
	// so until the FSM has applied this index the state a reader sees is an intermediate one.
	openedLastCommand uint64
	// loadGate backs StateLoaded.
	loadGate loadGate
	// joinsMu guards joins: the ids whose join is running on this leader, between the library's
	// AddNonvoter and the end of the registration, so the orphan sweep leaves them alone.
	joinsMu sync.Mutex
	joins   map[string]struct{}
	// beforeOrphanCheck and afterOrphanCheck are test seams on the orphan sweep: the first runs
	// before the sweep takes joinsMu, the second inside it between the check and the removal.
	beforeOrphanCheck, afterOrphanCheck func(id string)
	// afterVoterCount is a test seam: PromoteWithin calls it with the count the cap judged.
	afterVoterCount func(voters int)
}

// applyOutcome is what the FSM returns to the proposer's ApplyFuture:
// rejection is a decision, so the error rides in the response and nothing is
// retried.
type applyOutcome struct {
	applied Result
	err     error
}

// clusterFSM is the library's FSM. Apply is a pure function of the log
// entry: no clock, no I/O.
type clusterFSM struct {
	mu    sync.RWMutex
	state ClusterState
	// onApply wakes the node's watcher after the state moved; nil when nobody watches.
	onApply func()
	// schema publishes committed schema versions; nil publishes nothing.
	schema SchemaPublisher
	// applyGate is a test seam: when set, Apply calls it with the command kind before it touches
	// the state, so a test can hold a committed entry unapplied.
	applyGate atomic.Pointer[func(kind byte)]
	// applied is the index of the last entry Apply finished with. The library's own
	// AppliedIndex moves when a batch is handed to the FSM goroutine, before it is applied.
	applied atomic.Uint64
	// diverged is the first committed entry this node refused for a reason other than
	// STATE_MOVED, or found stamped above its own state (section 3 of the mixed-build spec).
	// Not persisted: a node restarted on a fixed build replays the entry and does not set it.
	// Once set the node snapshots nothing, reports its log as not applied and moves no data.
	diverged atomic.Pointer[Divergence]
	// hideField makes this FSM stand in for an older build that does not read the named body
	// fields (nil in production). Set before the FSM sees an entry.
	hideField func(field string) bool
	// logger receives the one line that says a divergence was recorded; nil logs nothing.
	logger hclog.Logger
	// divergedOnce and divergedCh make the channel Raft.Diverged returns: closed by the
	// compare-and-swap that wins the record, so it is closed exactly once. Created lazily
	// because tests build an FSM without a Raft.
	divergedOnce sync.Once
	divergedCh   chan struct{}
	// leaderOf names the leader of a term, or "" when this node does not know it; set once
	// the library exists, so a divergence recorded while it replays names no leader.
	leaderOf atomic.Pointer[func(term uint64) string]
	// levels is the range this node declares: the cluster versions it can run. Zero (an FSM a
	// test builds without a Raft) is the build's range.
	levels StateRange
	// version is the applied cluster version, stored whenever Apply or Restore replaces the
	// state, so the gates read it without the state lock.
	version atomic.Uint32
	// markerDir is the directory that holds the CLUSTER_VERSION marker, "" for an FSM with no
	// directory; markerAt is the version the marker records. Both are touched only before the
	// library starts and on the FSM goroutine.
	markerDir string
	markerAt  uint8
	// unrunnable is the refusal of a BOOTSTRAP, a SET_CLUSTER_VERSION or a snapshot at a cluster
	// version this node cannot run, when one stopped it; OpenRaft returns it, typed, when the open
	// itself fails on it.
	unrunnable atomic.Pointer[ClusterVersionError]
}

// openFailure is what OpenRaft returns for an open that failed with err: the cluster version
// refusal that stopped the replay, when one did, so the server exits with DivergedExitCode and the
// operator reads the text that names the build to start; err otherwise.
func (f *clusterFSM) openFailure(err error) error {
	if refusal := f.unrunnable.Load(); refusal != nil {
		return fmt.Errorf("%w (%w)", refusal, err)
	}
	return err
}

// levelRange is the range the appliers judge a cluster version against.
func (f *clusterFSM) levelRange() StateRange {
	if f.levels == (StateRange{}) {
		return BuildStateRange()
	}
	return f.levels
}

// followVersion records the applied cluster version after the state was replaced and rewrites
// the marker when it rose above what the marker records. It runs on the FSM goroutine with no lock
// held. The marker never moves down: a replay passes through versions below the one the log ends
// at (its own older snapshot, a BOOTSTRAP), and the marker is a lower bound of that final one. A
// marker that cannot be written is logged and left behind: the Restore and Apply checks still
// judge the version once the stores are open.
func (f *clusterFSM) followVersion(version uint8) {
	f.version.Store(uint32(version))
	if f.markerDir == "" || version <= f.markerAt {
		return
	}
	if err := writeClusterVersionMarker(f.markerDir, version); err != nil {
		if f.logger != nil {
			f.logger.Error("the cluster version marker was not rewritten; the version is still checked from the raft state at the next start", "cluster_version", version, "error", err)
		}
		return
	}
	f.markerAt = version
}

// divergedChan is the channel closed at the first divergence.
func (f *clusterFSM) divergedChan() chan struct{} {
	f.divergedOnce.Do(func() { f.divergedCh = make(chan struct{}) })
	return f.divergedCh
}

// noteDivergence records the first divergence; later ones do not replace it, so the record
// names the entry that started it. It logs text 1 once, closes the channel Raft.Diverged
// returns, and so is the only place the operator of a node that stops reads why.
//
// cause is "ahead stamp", "decode error" or "refusal CODE"; the reason is the refusal's own
// message, which already says what was found.
func (f *clusterFSM) noteDivergence(entry *hraft.Log, kind byte, stateSeq int64, cause, reason string) {
	f.recordDivergence(entry, kind, stateSeq, cause, reason, FatalText)
}

// recordDivergence is noteDivergence with the line it logs made by line from the record and the
// leader of its term.
func (f *clusterFSM) recordDivergence(entry *hraft.Log, kind byte, stateSeq int64, cause, reason string, line func(Divergence, string) string) {
	record := &Divergence{Index: entry.Index, Term: entry.Term, StateSeq: stateSeq, Kind: kind, Reason: reason}
	record.BaseSeq, _ = rawBaseSeq(entry.Data)
	if !f.diverged.CompareAndSwap(nil, record) {
		return
	}
	if f.logger != nil {
		leader := ""
		if known := f.leaderOf.Load(); known != nil {
			leader = (*known)(record.Term)
		}
		f.logger.Error(line(*record, leader), "cause", cause)
	}
	close(f.divergedChan())
}

// noteSnapshotDivergence records a divergence from a snapshot this build cannot read. The record
// has no entry to name; its reason is text 2, which is also what the log line says.
func (f *clusterFSM) noteSnapshotDivergence(text string) {
	f.mu.RLock()
	stateSeq := f.state.StateSeq
	f.mu.RUnlock()
	if !f.diverged.CompareAndSwap(nil, &Divergence{StateSeq: stateSeq, Reason: text}) {
		return
	}
	if f.logger != nil {
		f.logger.Error(text, "cause", "snapshot")
	}
	close(f.divergedChan())
}

// divergedApplyRefusal is the outcome of an entry a flagged node declines to apply.
func divergedApplyRefusal() error {
	return &Error{Code: CodeUnavailable, Message: "this node's cluster state disagrees with the cluster's (see the earlier error), so it applies no more entries. Start it on the build the proposing member runs and keep its data directory"}
}

// refusalCause names a refused entry's cause for the divergence line.
func refusalCause(err error) string {
	var refusal *Error
	if errors.As(err, &refusal) {
		return "refusal " + refusal.Code.String()
	}
	return "refusal"
}

// isStateMoved reports whether err is the one refusal every node makes for a command a
// concurrent command overtook: harmless, and the only refusal a healthy node can see.
func isStateMoved(err error) bool {
	var refusal *Error
	return errors.As(err, &refusal) && refusal.Code == CodeStateMoved
}

func (f *clusterFSM) snapshotState() ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

// userView reads the user table, the pepper key id and state_seq under the read lock, sharing the
// committed slices instead of cloning the state: a whole clone copies members, databases and seed
// bitmaps (3.3 MB at 1,000 databases, see dryRunOnce) for a reader that wants none of them.
func (f *clusterFSM) userView() UserView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return UserView{Users: f.state.Users, Requests: f.state.UserRequests, PepperKID: f.state.PepperKID, DropSeq: f.state.UserDropSeq, StateSeq: f.state.StateSeq}
}

// Apply decodes and applies one committed command.
func (f *clusterFSM) Apply(entry *hraft.Log) any {
	defer f.applied.Store(entry.Index)
	// A flagged node applies nothing more: a later entry stamped at the frozen state_seq would
	// otherwise be accepted, and the counter could climb back to the cluster's holding another
	// state (section 3.3). Returning a refusal per entry keeps the single FSM goroutine moving,
	// where blocking here would hang snapshots and shutdown. The state is not touched.
	if f.diverged.Load() != nil {
		return applyOutcome{err: divergedApplyRefusal()}
	}
	// The stamp is read from the raw header before the decode, so an entry from a build whose
	// command version this build refuses still proves the disagreement.
	if stamp, ok := rawBaseSeq(entry.Data); ok {
		f.mu.RLock()
		stateSeq := f.state.StateSeq
		f.mu.RUnlock()
		if stamp > uint64(stateSeq) {
			f.noteDivergence(entry, entry.Data[1], stateSeq, "ahead stamp", (&Diverged{BaseSeq: stamp, StateSeq: stateSeq}).Error())
		}
	}
	cmd, err := DecodeCommand(entry.Data)
	if err != nil {
		// Every entry in the log was written by EncodeCommand, so one that does not decode
		// came from another encoder.
		f.mu.RLock()
		stateSeq := f.state.StateSeq
		f.mu.RUnlock()
		var kind byte
		if len(entry.Data) > 1 {
			kind = entry.Data[1]
		}
		f.noteDivergence(entry, kind, stateSeq, "decode error", err.Error())
		return applyOutcome{err: err}
	}
	if gate := f.applyGate.Load(); gate != nil {
		(*gate)(cmd.Kind)
	}
	f.mu.Lock()
	var prior map[int64]int64
	if f.schema != nil && schemaCommand(cmd.Kind) {
		prior = schemaVersions(f.state)
	}
	applied, err := f.state.applyCommandWith(cmd, f.hideField, f.levelRange())
	var unrunnable *ClusterVersionError
	switch {
	case errors.As(err, &unrunnable):
		// A BOOTSTRAP or SET_CLUSTER_VERSION at a version this build cannot run: the 4.4 text is
		// the whole line, since it already names the build to start and the exit status.
		f.unrunnable.CompareAndSwap(nil, unrunnable)
		f.recordDivergence(entry, cmd.Kind, f.state.StateSeq, "cluster version", err.Error(), func(Divergence, string) string { return unrunnable.Error() })
	case err != nil && !isStateMoved(err):
		f.noteDivergence(entry, cmd.Kind, f.state.StateSeq, refusalCause(err), err.Error())
	}
	var moved []schemaMove
	if err == nil && prior != nil {
		moved = f.stageSchema(prior)
	}
	version := f.state.ClusterVersion
	f.mu.Unlock()
	if err == nil {
		f.followVersion(version)
	}
	// The publication waits for pair locks, so it runs with no state lock held: the FSM
	// goroutine holds nothing a writer under a pair lock could need (conv 20 step 6).
	f.publish(moved)
	if err == nil && f.schema != nil && cmd.Kind == CommandBumpSchema {
		f.noteBump(cmd.Body)
	}
	if f.onApply != nil {
		f.onApply()
	}
	return applyOutcome{applied: applied, err: err}
}

// schemaCommand reports whether a command can move a database's schema_version or remove it.
func schemaCommand(kind byte) bool {
	return kind == CommandBumpSchema || kind == CommandCreateDatabase || kind == CommandDropDatabase
}

func schemaVersions(st ClusterState) map[int64]int64 {
	versions := make(map[int64]int64, len(st.Databases))
	for _, row := range st.Databases {
		versions[row.ID] = row.SchemaVersion
	}
	return versions
}

// schemaMove is one database whose committed version differs from the one before the apply:
// to is the new version, and 0 means the database is gone.
type schemaMove struct {
	dbID int64
	to   int64
}

// stageSchema runs under the state lock. For every database whose committed version moved it
// records the version the node was publishing, so no reader ever sees a committed version
// that has no published one behind it, and it returns the moves to publish after unlocking.
func (f *clusterFSM) stageSchema(prior map[int64]int64) []schemaMove {
	var moved []schemaMove
	now := make(map[int64]bool, len(f.state.Databases))
	for _, row := range f.state.Databases {
		now[row.ID] = true
		before, existed := prior[row.ID]
		if !existed {
			before = row.SchemaVersion
		}
		f.schema.Stage(uint64(row.ID), before)
		if row.SchemaVersion != before {
			moved = append(moved, schemaMove{dbID: row.ID, to: row.SchemaVersion})
		}
	}
	for id := range prior {
		if !now[id] {
			moved = append(moved, schemaMove{dbID: id})
		}
	}
	return moved
}

// noteBump tells the publisher which bucket an applied bump was about, after the version is
// published, so a sweep never starts on a node that still fences the old table.
func (f *clusterFSM) noteBump(body []byte) {
	note, ok := ParseBumpNote(body)
	if !ok {
		return
	}
	fields, err := newCommandFields("BUMP_SCHEMA", body)
	if err != nil {
		return
	}
	dbID, err := fields.int64("db_id")
	if err != nil {
		return
	}
	to, err := fields.int64("to")
	if err != nil {
		return
	}
	f.schema.Note(uint64(dbID), to, note)
}

func (f *clusterFSM) publish(moved []schemaMove) {
	for _, move := range moved {
		if move.to == 0 {
			f.schema.Forget(uint64(move.dbID))
			continue
		}
		f.schema.Publish(uint64(move.dbID), move.to)
	}
}

// Snapshot encodes the state now, so the library can keep applying entries
// while Persist writes the file.
func (f *clusterFSM) Snapshot() (hraft.FSMSnapshot, error) {
	// It runs on the FSM goroutine in order with Apply, so the flag is exact at this point: a
	// frozen state is never written, and the refused entry stays in the log for the restart to
	// replay (a snapshot would let the library compact it away).
	if f.diverged.Load() != nil {
		return nil, errors.New("not snapshotting: this node's state disagrees with the cluster's (read the node's log for the line that names the entry)")
	}
	raw, err := EncodeSnapshot(f.snapshotState())
	if err != nil {
		return nil, fmt.Errorf("snapshot cluster state: %w", err)
	}
	return &fileSnapshot{raw: raw}, nil
}

// maxSnapshotBytes bounds a restore read; the state is O(databases + members).
const maxSnapshotBytes = 1 << 28

// Restore replaces the state with a snapshot file. A file that fails its
// magic, version or checksum is refused, and the library falls back to an
// older snapshot or the log.
func (f *clusterFSM) Restore(src io.ReadCloser) error {
	raw, err := io.ReadAll(io.LimitReader(src, maxSnapshotBytes))
	if closeErr := src.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	st, err := DecodeSnapshot(raw)
	if err != nil {
		// A later build's snapshot can fail the decode on a field this build does not know; its
		// version, the second field, still says which build to run (4.4 check 2), as the open
		// check reads it.
		if version, ok := snapshotClusterVersion(raw); ok {
			if refusal := f.refuseSnapshotVersion(version); refusal != nil {
				return fmt.Errorf("restore snapshot: %w (%w)", refusal, err)
			}
		}
		if phrase, foreign := SnapshotBuildDifference(err); foreign {
			// The leader's retries would leave this node frozen at its old state for good:
			// stop it, as a refused entry does.
			f.noteSnapshotDivergence(snapshotDifferenceText("the snapshot the leader sent", phrase))
		}
		return fmt.Errorf("restore snapshot: %w", err)
	}
	// Check 2 of cluster-version spec 4.4: a snapshot at a version this build cannot run stops the
	// node before the state is replaced.
	if refusal := f.refuseSnapshotVersion(st.ClusterVersion); refusal != nil {
		return fmt.Errorf("restore snapshot: %w", refusal)
	}
	f.mu.Lock()
	previous := f.state
	f.state = st
	var moved []schemaMove
	if f.schema != nil {
		// A database the old state had and the snapshot does not was dropped meanwhile.
		now := make(map[int64]bool, len(st.Databases))
		for _, row := range st.Databases {
			now[row.ID] = true
		}
		for _, row := range previous.Databases {
			if !now[row.ID] {
				moved = append(moved, schemaMove{dbID: row.ID})
			}
		}
		for _, row := range st.Databases {
			f.schema.Stage(uint64(row.ID), row.SchemaVersion)
			moved = append(moved, schemaMove{dbID: row.ID, to: row.SchemaVersion})
		}
	}
	f.mu.Unlock()
	f.followVersion(st.ClusterVersion)
	f.publish(moved)
	if f.onApply != nil {
		f.onApply()
	}
	return nil
}

// refuseSnapshotVersion judges a restored snapshot's cluster version and, when this build cannot
// run it, records the refusal that stops the node and returns it. Only the upper bound: the
// version only rises, so a snapshot above Max proves the cluster is past this build. Below Min
// proves nothing, whoever sent the snapshot: the node's own is history its log may have moved
// past, and the leader's is its newest snapshot, not its state now (InstallSnapshot sends the
// stored file, which can predate a finalize indefinitely). The lower bound is judged only by a
// holder of the committed version (cluster-version spec 4.4).
func (f *clusterFSM) refuseSnapshotVersion(version uint8) *ClusterVersionError {
	levels := f.levelRange()
	if version <= levels.Max {
		return nil
	}
	refusal := &ClusterVersionError{Version: version, Range: levels}
	f.unrunnable.CompareAndSwap(nil, refusal)
	f.noteSnapshotDivergence(refusal.Error())
	return refusal
}

// fileSnapshot writes the snapshot file (membership Formats 3: header, ClusterState body, CRC) into
// the library's sink.
type fileSnapshot struct{ raw []byte }

// Persist writes the file, or cancels the sink.
func (s *fileSnapshot) Persist(sink hraft.SnapshotSink) error {
	if _, err := sink.Write(s.raw); err != nil {
		return errors.Join(fmt.Errorf("write snapshot: %w", err), sink.Cancel())
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	return nil
}

// Release has nothing to free: the bytes are already in memory.
func (s *fileSnapshot) Release() {}

// OpenRaft opens the stores, the stream layer and the library, and
// bootstraps a new cluster only when both guards allow it.
func OpenRaft(cfg RaftConfig) (*Raft, error) {
	if cfg.NodeID == "" || cfg.Dir == "" || cfg.Listener == nil {
		return nil, fmt.Errorf("open raft: NodeID, Dir and Listener are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: os.Stderr})
	}
	raftDir := filepath.Join(cfg.Dir, "raft")
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		return nil, fmt.Errorf("create raft directory %s: %w", raftDir, err)
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("open raft log in %s: %w", raftDir, err)
	}
	snaps, err := hraft.NewFileSnapshotStoreWithLogger(snapshotStoreDir(cfg.Dir), raftSnapshotsRetained, cfg.Logger)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open snapshot store: %w", err), store.Close())
	}

	node := &Raft{cfg: cfg, fsm: &clusterFSM{schema: cfg.Schema, onApply: cfg.OnApply, logger: cfg.Logger, markerDir: cfg.Dir}, store: store}
	node.fsm.levels = node.Range()
	if node.fsm.markerAt, err = readClusterVersionMarker(cfg.Dir); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	node.fsm.divergedChan() // before NewRaft: a Restore or a replayed entry can close it during the open
	if cfg.ApplyGate != nil {
		node.fsm.applyGate.Store(&cfg.ApplyGate)
	}
	// Held until the open has read the Raft directory (BUG-0870 guard, review F1): the listener was
	// bound before OpenRaft ran and a leader redials a restarted voter continuously, so a connection
	// can be waiting in the backlog when the stream starts, and an admitted Raft connection is
	// long-lived. The decision below opens the latch for a node that bootstraps or holds state.
	node.awaitingAdmission.Store(true)
	// Always set: without PeerFingerprint AdmitPeer is the handshake rules and the state-range check.
	hooks := streamHooks{local: node.localIdentity, committedIncarnation: node.committedIncarnation,
		admit: func(h transport.Handshake) error { return node.AdmitPeer(h, false) }}
	node.stream = newStreamLayer(cfg.Listener, hooks, cfg.Limits, cfg.Logger)
	node.stream.dial = cfg.Dial
	node.stream.start()
	if afterRaftStreamStart != nil {
		afterRaftStreamStart()
	}
	trans := hraft.NewNetworkTransportWithConfig(&hraft.NetworkTransportConfig{
		Stream: node.stream, MaxPool: raftMaxPool, Timeout: raftApplyTimeout, Logger: cfg.Logger,
	})

	// The library dials its own configuration, not the committed state, so the seat must be the
	// address peers can reach: the advertised one, else what the listener is bound to.
	ownSeat := hraft.ServerAddress(cfg.DataAddr)
	if ownSeat == "" {
		ownSeat = trans.LocalAddr()
	}
	if err := refuseWildcardSeat(string(ownSeat)); err != nil {
		return nil, errors.Join(err, trans.Close(), store.Close())
	}

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(cfg.NodeID)
	conf.TrailingLogs, conf.SnapshotThreshold = raftTrailingLogs, raftSnapshotThreshold
	conf.Logger = cfg.Logger
	if cfg.HeartbeatTimeout > 0 {
		conf.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		conf.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.LeaderLease > 0 {
		conf.LeaderLeaseTimeout = cfg.LeaderLease
	}

	hasState, err := hraft.HasExistingState(store, store, snaps)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("check for existing raft state: %w", err), trans.Close(), store.Close())
	}
	// Guard one: the library's own record of existing state. Guard two: the
	// operator's intent, and no seed list. A node that loses <Dir>/raft
	// passes guard one, which is why the cluster id travels inside
	// BOOTSTRAP and a second bootstrap is refused CLUSTER_MISMATCH.
	node.hadState = hasState
	mayBootstrap := cfg.Bootstrap && len(cfg.SeedNodes) == 0
	// The BUG-0870 guard. A node that opens to join with no Raft state cannot tell from its disk whether
	// it is new or a member whose ring directory was moved aside; the second, caught up by the leader
	// as the voter it was, is counted at its old match index (the library never lowers it) and has
	// forgotten its term and vote, so the cluster can lose a committed entry. Its peers are refused
	// until a member's identity request admits it, which only a join (and a replace) sends.
	node.awaitingAdmission.Store(!mayBootstrap && !hasState)
	if mayBootstrap && !hasState {
		seat := hraft.Configuration{Servers: []hraft.Server{{
			Suffrage: hraft.Voter, ID: conf.LocalID, Address: ownSeat,
		}}}
		if err := hraft.BootstrapCluster(conf, store, store, snaps, trans, seat); err != nil {
			return nil, errors.Join(fmt.Errorf("bootstrap raft: %w", err), trans.Close(), store.Close())
		}
	}
	if node.openedLastCommand, err = lastCommandIndex(store); err != nil {
		return nil, errors.Join(fmt.Errorf("read the raft log's last command: %w", err), trans.Close(), store.Close())
	}
	metas, err := snaps.List()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("list the raft snapshots: %w", err), trans.Close(), store.Close())
	}
	// Check 4 of cluster-version spec 4.4, the second line of defence behind OpenNode's: a
	// directory at a cluster version this build cannot run is refused before the library replays
	// anything, with the text that names the build to start.
	if err := CheckDataDirClusterVersion(cfg.Dir, node.Range()); err != nil {
		return nil, errors.Join(err, trans.Close(), store.Close())
	}
	if len(metas) > 0 {
		if err := refuseForeignSnapshot(snaps, metas[0]); err != nil {
			return nil, errors.Join(err, trans.Close(), store.Close())
		}
	}
	node.raft, err = hraft.NewRaft(conf, node.fsm, store, store, snaps, trans)
	if err != nil {
		return nil, errors.Join(node.fsm.openFailure(fmt.Errorf("start raft: %w", err)), trans.Close(), store.Close())
	}
	leaderOf := node.leaderOfTerm
	node.fsm.leaderOf.Store(&leaderOf)
	if mayBootstrap {
		if err := node.commitBootstrap(); err != nil {
			return nil, errors.Join(node.fsm.openFailure(err), node.Close())
		}
	}
	// A refusal met while the open applied its own log (commitBootstrap waits for it on a
	// bootstrapping node) is returned typed, so the server exits 70 with the text.
	if refusal := node.fsm.unrunnable.Load(); refusal != nil {
		return nil, errors.Join(refusal, node.Close())
	}
	return node, nil
}

// refuseForeignSnapshot is the first look at the newest snapshot, before the library is started
// on it. Left to the library, a snapshot this build cannot read is skipped for an older one (and
// the log is then applied onto a state the cluster has moved past), or ends in a panic in
// NewRaft. A snapshot that fails the library's own checksum on Open, or this build's length or
// checksum, is damage and stays the library's; every other refusal is a build difference.
// The cost is one read of a state that is O(members x databases), once at start.
func refuseForeignSnapshot(snaps hraft.SnapshotStore, newest *hraft.SnapshotMeta) error {
	_, src, err := snaps.Open(newest.ID)
	if err != nil {
		return nil // damage, or a file that is not there: the library reports it
	}
	raw, err := io.ReadAll(io.LimitReader(src, maxSnapshotBytes))
	if closeErr := src.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil
	}
	_, err = DecodeSnapshot(raw)
	if phrase, foreign := SnapshotBuildDifference(err); foreign {
		return &ForeignSnapshotError{Text: snapshotDifferenceText(fmt.Sprintf("this node's newest raft snapshot (%s)", newest.ID), phrase)}
	}
	return nil
}

// commitBootstrap proposes BOOTSTRAP when this node is the sole voter and no
// BOOTSTRAP has ever applied. The cluster id is minted here, before the
// proposal, and travels inside it. A node that crashed between the library's
// bootstrap and this commit finds state_seq 0 on restart and finishes the job:
// nothing was ever committed, so no peer can hold another id.
func (r *Raft) commitBootstrap() error {
	conf, err := r.configuration()
	if err != nil {
		return err
	}
	if len(conf.Servers) != 1 || conf.Servers[0].ID != hraft.ServerID(r.cfg.NodeID) {
		return nil
	}
	if err := r.waitForLeadership(); err != nil {
		return err
	}
	if err := r.raft.Barrier(raftApplyTimeout).Error(); err != nil {
		return fmt.Errorf("wait for the log to be applied before bootstrapping: %w", err)
	}
	if r.State().StateSeq != 0 {
		return nil
	}
	var clusterID [16]byte
	if _, err := rand.Read(clusterID[:]); err != nil {
		return fmt.Errorf("mint a cluster id: %w", err)
	}
	self := Member{ID: r.cfg.NodeID, Incarnation: r.cfg.Incarnation, DataAddr: r.cfg.DataAddr, ClientAddr: r.cfg.ClientAddr}
	// The cluster starts at this build's highest level (cluster-version spec Q7), as CockroachDB's
	// bootstrap does: a new cluster has no older member to wait for.
	if _, err := r.Propose(Command{Version: CommandVersion, Kind: CommandBootstrap, Body: BootstrapBody(clusterID, self, r.Range().Max)}); err != nil {
		return fmt.Errorf("commit BOOTSTRAP: %w", err)
	}
	return nil
}

func (r *Raft) waitForLeadership() error {
	deadline := time.Now().Add(raftStartTimeout)
	for r.raft.State() != hraft.Leader {
		if time.Now().After(deadline) {
			return fmt.Errorf("node %s did not become leader of its own single-node group within %s: check that its data directory %s is not shared with another process", r.cfg.NodeID, raftStartTimeout, r.cfg.Dir)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

func (r *Raft) configuration() (hraft.Configuration, error) {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return hraft.Configuration{}, fmt.Errorf("read the raft configuration: %w", err)
	}
	return future.Configuration(), nil
}

// StateSeq is the state_seq of the committed state this node has applied: one lock-free
// read for a watcher that only needs to know whether the state moved.
func (r *Raft) StateSeq() int64 {
	r.fsm.mu.RLock()
	defer r.fsm.mu.RUnlock()
	return r.fsm.state.StateSeq
}

// MemberAddr is the internode address the cluster committed for member id.
func (r *Raft) MemberAddr(id string) (string, bool) {
	r.fsm.mu.RLock()
	defer r.fsm.mu.RUnlock()
	if i, ok := r.fsm.state.memberIndex(id); ok {
		return r.fsm.state.Members[i].DataAddr, true
	}
	return "", false
}

// MemberIDs lists the ids of every committed member.
func (r *Raft) MemberIDs() []string {
	r.fsm.mu.RLock()
	defer r.fsm.mu.RUnlock()
	ids := make([]string, len(r.fsm.state.Members))
	for i, m := range r.fsm.state.Members {
		ids[i] = m.ID
	}
	return ids
}

// Identity is what this node says about itself in a handshake: its id, incarnation and
// the committed cluster id, epoch and state_seq.
func (r *Raft) Identity() transport.Handshake {
	id := r.localIdentity()
	return id.Range.Stamp(transport.Handshake{
		NodeID: id.NodeID, Incarnation: id.Incarnation, ClusterID: id.ClusterID, Epoch: id.Epoch, StateSeq: id.StateSeq,
		SentAtNs: time.Now().UnixNano(),
	})
}

// ClusterVersion is the cluster version of the state this node has applied, 0 when it holds no
// state. It is the applied version, not the committed one: a lagging node writes the older
// vocabulary, which every member reads.
func (r *Raft) ClusterVersion() uint8 { return uint8(r.fsm.version.Load()) }

// Range is the state-level range this node declares and holds peers to.
func (r *Raft) Range() StateRange {
	if r.cfg.StateRange != nil {
		return *r.cfg.StateRange
	}
	return BuildStateRange()
}

// CheckPeer applies the handshake rules to a peer on any channel: a different cluster id
// when both sides know theirs, or an incarnation older than the committed one, is refused.
func (r *Raft) CheckPeer(hello transport.Handshake) error { return r.stream.checkIdentity(hello) }

// AdmitPeer is the admission of a node that binds keys to members (RaftConfig.
// PeerFingerprint). The peer must authenticate with the key recorded for the id it claims;
// once this node knows the cluster id the peer must carry the same non-zero id; and the
// peer must be a committed member. needState is true for the data and control channels,
// which a node that has applied no cluster state yet does not serve; the Raft channel
// takes the leader that is adding a joiner on the strength of its pinned key alone.
// Without PeerFingerprint it is the handshake rules only.
func (r *Raft) AdmitPeer(hello transport.Handshake, needState bool) error {
	if err := r.stream.checkIdentity(hello); err != nil {
		return err
	}
	// Before the pinned-key branch, so a node without PeerFingerprint (the development mode) is held
	// as well.
	if r.awaitingAdmission.Load() {
		return r.notAdmittedRefusal(hello)
	}
	if r.cfg.PeerFingerprint != nil {
		if err := r.checkPinnedKey(hello); err != nil {
			return err
		}
	}
	if err := r.checkStateRange(hello); err != nil {
		return err
	}
	if r.cfg.PeerFingerprint == nil {
		return nil
	}
	r.fsm.mu.RLock()
	local, members := r.fsm.state.ClusterID, len(r.fsm.state.Members)
	_, isMember := r.fsm.state.memberIndex(hello.NodeID)
	r.fsm.mu.RUnlock()
	if members == 0 {
		if needState {
			return fmt.Errorf("refused %s: this node has joined no cluster yet and serves only the Raft channel until its leader reaches it", hello.NodeID)
		}
		return nil
	}
	if local != ([16]byte{}) && hello.ClusterID != local {
		return &Error{Code: CodeClusterMismatch, Message: CodeClusterMismatch.Text(CondClusterMismatch, map[string]string{
			"node": hello.NodeID, "node_cluster": fmt.Sprintf("%x", hello.ClusterID[:4]), "this_cluster": fmt.Sprintf("%x", local[:4]), "seed_addr": r.cfg.DataAddr,
		})}
	}
	if !isMember {
		return fmt.Errorf("refused %s: it is not a member of this cluster. Run 'join %s@<host:port>' on the leader, then retry", hello.NodeID, hello.NodeID)
	}
	return nil
}

// checkPinnedKey refuses a connection whose key is not the one recorded for the node id it claims.
func (r *Raft) checkPinnedKey(hello transport.Handshake) error {
	// An empty fingerprint is no key: a connection that authenticated with nothing must
	// never equal a source that recorded nothing.
	if want, known := r.cfg.PeerFingerprint(hello.NodeID); !known || hello.PeerFingerprint == "" || want == "" || want != hello.PeerFingerprint {
		return &Error{Code: CodeClusterMismatch, Message: fmt.Sprintf(
			"refused connection from %q: it authenticated with key %s, which is not the key recorded for that node id. Pin the node's real key under its own id in seed_fingerprints (id=SHA256:...)", hello.NodeID, hello.PeerFingerprint)}
	}
	return nil
}

// AdmitIdentity is the admission of the identity channel: the pinned key alone. The caller must
// authenticate with the key recorded for the node id it claims, and nothing else is asked of
// it, because the node is asked who it is precisely when it belongs to no cluster, or to
// another one, and must still be able to say so (membership node-side plan section 1.4).
//
// With no PeerFingerprint there is no key to hold the caller to, so any caller that completes the
// TLS handshake is admitted and the claimed id is not checked, exactly as AdmitPeer does. That is
// the development mode only: a networked node refuses to open without Fingerprints unless
// ClusterOptions.InsecureTestOnly is set (cluster/wire:OpenNode), so production never reaches it.
func (r *Raft) AdmitIdentity(hello transport.Handshake) error {
	if r.cfg.PeerFingerprint == nil {
		return nil
	}
	return r.checkPinnedKey(hello)
}

// IsLeader reports whether this node is the Raft leader now.
func (r *Raft) IsLeader() bool { return r.raft.State() == hraft.Leader }

// Term is this node's current Raft term, 0 when the library does not report one.
func (r *Raft) Term() uint64 {
	term, err := strconv.ParseUint(r.raft.Stats()["term"], 10, 64)
	if err != nil {
		return 0
	}
	return term
}

// VerifyLeader runs the leader's quorum round and returns how long it took. A node that
// is not the leader, or that cannot reach a quorum, gets the error.
func (r *Raft) VerifyLeader() (time.Duration, error) {
	began := time.Now()
	if err := r.raft.VerifyLeader().Error(); err != nil {
		return 0, fmt.Errorf("confirming that %s still leads: %w", r.cfg.NodeID, err)
	}
	return time.Since(began), nil
}

// Leader returns the node id of the leader this node knows, or empty when there is none.
func (r *Raft) Leader() string {
	_, id := r.raft.LeaderWithID()
	return string(id)
}

// TakeSnapshot makes the library snapshot the FSM now and waits for it: a test seam for a boot
// that restores a snapshot with no command after it.
func (r *Raft) TakeSnapshot() error { return r.raft.Snapshot().Error() }

// LogHandedToFSM reports whether the library has handed every command the log held at open to
// the FSM goroutine (its AppliedIndex moves on the hand-over, before the apply). It is weaker
// than LogApplied and is the driver's fallback when the FSM can never reach that index.
func (r *Raft) LogHandedToFSM() bool { return r.raft.AppliedIndex() >= r.openedLastCommand }

// LogApplied reports whether the FSM has applied every command the node's own log held when
// it opened, counting the restored snapshot's index as applied (a snapshot at the tip of the
// log restarted with no command after it reaches Apply for nothing), and is false on a diverged node whatever it applied. Until then State and StateSeq describe an intermediate state of a log being
// re-applied (a database not created yet, a member not yet active), and a decision taken
// from it would be taken against a cluster that never existed; a restarted node that
// reconciles against such a state retires every pair of a database the state does not
// hold yet (seeding-completion plan §0.3, "after the committed state is loaded").
func (r *Raft) LogApplied() bool {
	return r.fsm.diverged.Load() == nil && r.appliedFloor() >= r.openedLastCommand
}

// AppendWithoutDryRun appends raw command bytes to the log exactly as a leader of another build
// would, with no dry run, and waits for the entry to commit and apply. A refusal by the FSM is
// not an error here: it is what the committed entry did. It exists so tests in this and the wire
// package can commit an entry this build refuses. It is exported only because a _test.go helper
// here would be invisible to the wire tests; TestNoProductionFileCallsAppendWithoutDryRun fails
// if any non-test file calls it.
func (r *Raft) AppendWithoutDryRun(raw []byte) error {
	return r.raft.Apply(raw, raftApplyTimeout).Error()
}

// Diverged returns a channel closed, once, when this node records its first divergence. The
// server layer selects on it next to its signal channel and exits; a divergence recorded while
// OpenRaft replayed the log is already closed when OpenRaft returns.
func (r *Raft) Diverged() <-chan struct{} { return r.fsm.divergedChan() }

// leaderOfTerm is the id of the leader of term when this node knows it: the leader it sees
// now, and only while the library is still in that term. A different term has a different
// leader, and naming the wrong one would send the operator to the wrong node.
func (r *Raft) leaderOfTerm(term uint64) string {
	if r.raft.Stats()["term"] != strconv.FormatUint(term, 10) {
		return ""
	}
	return r.Leader()
}

// IsDiverged reports whether this node refused a committed entry the leader that stamped it
// cannot have appended, or met one stamped above its own state: it and that leader disagree
// about the cluster state. While true the node snapshots nothing, reports LogApplied false,
// proposes nothing and runs no seed pass, so a frozen state is never used to retire a pair.
func (r *Raft) IsDiverged() bool { return r.fsm.diverged.Load() != nil }

// Divergence is the record of the first entry that set the flag, and false when none has.
func (r *Raft) Divergence() (Divergence, bool) {
	record := r.fsm.diverged.Load()
	if record == nil {
		return Divergence{}, false
	}
	return *record, true
}

// lastCommandIndex is the index of the last command entry in the retained log, 0 when the
// log holds none; configuration and no-op entries never reach the FSM, so they are not
// what the FSM's applied index is compared against. The scan runs backwards from the end
// and stops at the first command, which is the last entry or close to it.
func lastCommandIndex(store *raftboltdb.BoltStore) (uint64, error) {
	last, err := store.LastIndex()
	if err != nil {
		return 0, err
	}
	first, err := store.FirstIndex()
	if err != nil {
		return 0, err
	}
	for index := last; index >= first && index > 0; index-- {
		var entry hraft.Log
		if err := store.GetLog(index, &entry); err != nil {
			if errors.Is(err, hraft.ErrLogNotFound) {
				// A hole between the first and the last index: answering 0 would let the seed
				// driver reconcile against an intermediate state, so the open refuses instead.
				return 0, fmt.Errorf("the raft log holds indexes %d to %d but index %d is missing: the log's tail was compacted while this read scanned it, or the log is damaged. Retry; if the node will not start with this error, keep its data directory (at replication factor 1 it may hold the only copy of the data) and do not delete it", first, last, index)
			}
			return 0, err
		}
		if entry.Type == hraft.LogCommand {
			return index, nil
		}
	}
	return 0, nil
}

// Restarted reports whether this process opened a Raft directory that already held
// state, that is, whether a previous process of this node had joined a cluster. A first
// boot, bootstrap or join, is false: its first epoch is not one it missed. A restarted
// node may own a pair by a move it never saw, so it must not serve that pair on trust
// (BUG-0743).
func (r *Raft) Restarted() bool { return r.hadState }

// Admit ends the BUG-0870 guard's hold: from now on AdmitPeer judges peers by the usual rules, so the
// leader that is adding this node can replicate to it. wire.Node.serveIdentity calls it for an
// identity request with the admit bit, which only a member's join (or replace) of this node sends.
// It is a no-op for a node that was never held.
func (r *Raft) Admit() { r.awaitingAdmission.Store(false) }

// ConfiguredVoter reports whether the library's Raft configuration counts id as a voter. Promotion
// changes the configuration before it commits SET_VOTERS, so a node can vote here while the committed
// voters list does not name it yet; a check that must not miss a voter reads both.
func (r *Raft) ConfiguredVoter(id string) (bool, error) {
	conf, err := r.configuration()
	if err != nil {
		return false, err
	}
	for _, server := range conf.Servers {
		if string(server.ID) == id {
			return server.Suffrage == hraft.Voter, nil
		}
	}
	return false, nil
}

// AwaitingAdmission reports whether this node opened to join with no Raft state and no member has
// admitted it yet, so it refuses every peer connection.
func (r *Raft) AwaitingAdmission() bool { return r.awaitingAdmission.Load() }

// notAdmittedRefusal is the text of a peer connection refused while the node waits for admission. The
// node cannot tell a new node from a member that lost its data, so it names the statement for each
// (BUG-0870).
func (r *Raft) notAdmittedRefusal(hello transport.Handshake) error {
	return errors.New(strings.NewReplacer("<peer>", hello.NodeID, "<channel>", transport.Channel(hello.Channel).String(), "<node>", r.cfg.NodeID, "<addr>", r.cfg.DataAddr).Replace(
		"refused <peer> on the <channel> channel: this node holds no cluster state and no member has admitted it, so it takes no cluster traffic yet. " +
			"If <node> is a new node, run 'join <node>@<addr>' on any member. If <node> was a member that lost its data, keep the ring directory you moved aside, " +
			"wait until 'nodes' shows <node> down, then run 'replace-node <node> --with <node>@<addr>' on any member"))
}

// State returns a copy of the committed cluster state as this node has
// applied it.
func (r *Raft) State() ClusterState { return r.fsm.snapshotState() }

// Users returns the user table as this node has applied it, without cloning the rest of the state
// (ring user store spec 6.3, B16). The view's slices are shared with the committed state and must
// never be written.
func (r *Raft) Users() UserView { return r.fsm.userView() }

// PepperView is what the cluster pepper's hand-out reads of the applied state (ring user store
// spec 5.3): the cluster id the key is bound to, the kid every stored hash names, and the member
// rows (a copy, O(members)), so a holder re-checks its asker and a fetcher picks whom to ask. It
// carries no user row and no pepper.
type PepperView struct {
	ClusterID [16]byte
	PepperKID string
	Members   []Member
}

// Member returns the row of member id.
func (v PepperView) Member(id string) (Member, bool) {
	for _, m := range v.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// PepperIdentity is the cluster id and the state's pepper kid, read under the FSM read lock with
// no copy: what a login needs to pick and check its key (ring user store spec 5.5).
func (r *Raft) PepperIdentity() (clusterID [16]byte, pepperKID string) {
	r.fsm.mu.RLock()
	defer r.fsm.mu.RUnlock()
	return r.fsm.state.ClusterID, r.fsm.state.PepperKID
}

// PepperView reads the pepper facts under the FSM read lock, without cloning the rest of the
// state.
func (r *Raft) PepperView() PepperView {
	r.fsm.mu.RLock()
	defer r.fsm.mu.RUnlock()
	return PepperView{ClusterID: r.fsm.state.ClusterID, PepperKID: r.fsm.state.PepperKID, Members: slices.Clone(r.fsm.state.Members)}
}

func (r *Raft) localIdentity() localIdentity {
	st := r.fsm.snapshotState()
	return localIdentity{
		NodeID: r.cfg.NodeID, Incarnation: uint64(r.cfg.Incarnation), ClusterID: st.ClusterID,
		Epoch: uint64(st.Epoch), StateSeq: uint64(st.StateSeq), DataAddr: r.cfg.DataAddr, Range: r.Range(),
	}
}

// committedIncarnation is the incarnation the cluster committed for id.
func (r *Raft) committedIncarnation(id string) (uint64, bool) {
	r.fsm.mu.RLock()
	defer r.fsm.mu.RUnlock()
	if i, ok := r.fsm.state.memberIndex(id); ok {
		return uint64(r.fsm.state.Members[i].Incarnation), true
	}
	return 0, false
}

// Propose commits one command through the Raft group and returns what the
// FSM did with it. Only the leader proposes; a rejection (STATE_MOVED,
// CONFLICT, ...) comes back as the FSM's own error and is never retried.
func (r *Raft) Propose(cmd Command) (Result, error) {
	return r.ProposeContext(context.Background(), cmd)
}

// ProposeContext is Propose that gives up waiting when ctx ends. The command may nevertheless
// commit later, so the give-up is an undecided *NotLeaderError (no leader hint), which a relay
// reports as NO_QUORUM and a proposer treats as unknown, never as a refusal.
func (r *Raft) ProposeContext(ctx context.Context, cmd Command) (Result, error) {
	raw, err := EncodeCommand(cmd)
	if err != nil {
		return Result{}, err
	}
	if err := r.DryRun(ctx, cmd); err != nil {
		return Result{}, err
	}
	future := r.raft.Apply(raw, raftApplyTimeout)
	settled := make(chan error, 1)
	// The future resolves on commit, on lost leadership or when the library shuts down, so
	// this goroutine always ends.
	go func() { settled <- future.Error() }()
	select {
	case err := <-settled:
		if err != nil {
			return Result{}, r.proposeError(err)
		}
	case <-ctx.Done():
		return Result{}, &NotLeaderError{
			Refusal:   &Error{Code: CodeUnavailable, Message: fmt.Sprintf("node %s did not see the command commit before its time ran out, so it may still commit: %v", r.cfg.NodeID, context.Cause(ctx))},
			Undecided: true,
		}
	}
	outcome, ok := future.Response().(applyOutcome)
	if !ok {
		return Result{}, fmt.Errorf("the cluster FSM answered %T, not an outcome", future.Response())
	}
	return outcome.applied, outcome.err
}

// divergedProposalRefusal is what a node that has recorded a divergence answers a proposal:
// it must not append, or a node that has detected its own disagreement would make healthy
// nodes refuse its entry.
func divergedProposalRefusal() error {
	return &Error{Code: CodeUnavailable, Message: "this node's cluster state disagrees with the cluster's (see the earlier error) and it is stopping, so it cannot change the cluster. Send the command to another member."}
}

// DryRun is step G1 and premise P1 of the mixed-build spec: it runs cmd through ApplyCommand on a
// copy of this leader's applied state before anything is appended, so a command that this
// build's own rules refuse never becomes a log entry, and a refusal that a committed entry
// carries therefore came from a build that read something this one does not. It is called
// once per stamp, by ProposeContext (the single call into the library's Apply) and by a relay's
// authority check. A node that does not lead answers *NotLeaderError first, and never a
// refusal computed against its own state. A diverged node refuses with text 4. A stamp above
// the applied state comes back STATE_MOVED (a proposer's stamp can only be ahead of a state
// that is stale), and a final refusal is repeated after a barrier when the FSM may trail the log
// (a new leader), so a stale NOT_FOUND or CONFLICT is not returned as the cluster's answer.
func (r *Raft) DryRun(ctx context.Context, cmd Command) error {
	if r.raft.State() != hraft.Leader {
		return r.proposeError(hraft.ErrNotLeader)
	}
	if r.IsDiverged() {
		return divergedProposalRefusal()
	}
	err := r.dryRunOnce(cmd)
	if err == nil || isStateMoved(err) {
		return err
	}
	if err := r.awaitFSMCaughtUp(ctx); err != nil {
		return err
	}
	return r.dryRunOnce(cmd)
}

// dryRunOnce applies cmd to a copy of the applied state and maps an ahead stamp to STATE_MOVED.
// The copy is shallow, taken under the read lock: ApplyCommand clones the state before it
// mutates anything and assigns only to its own receiver, and the FSM replaces its state the same
// way (Restore swaps it whole), so the slices the copy shares are never written. A State() clone
// first would clone twice (3.3 MB and 1.1 ms of leader CPU per command at 1,000 databases).
func (r *Raft) dryRunOnce(cmd Command) error {
	r.fsm.mu.RLock()
	state := r.fsm.state
	r.fsm.mu.RUnlock()
	_, err := state.applyCommandWith(cmd, nil, r.fsm.levelRange())
	var ahead *Diverged
	if errors.As(err, &ahead) {
		return stateMovedRefusal(ahead.StateSeq, ahead.BaseSeq)
	}
	return err
}

// awaitFSMCaughtUp returns once the FSM has applied every command the log holds now. A barrier
// would do the same, but it appends an entry for every refusal a client can provoke; the wait
// reads the last command's index instead and polls the FSM's own applied index.
func (r *Raft) awaitFSMCaughtUp(ctx context.Context) error {
	last, err := lastCommandIndex(r.store)
	if err != nil {
		return fmt.Errorf("read the raft log's last command: %w", err)
	}
	deadline := time.Now().Add(raftApplyTimeout)
	for r.appliedFloor() < last {
		if r.raft.State() != hraft.Leader {
			return r.proposeError(hraft.ErrNotLeader)
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			// A plain refusal, not a NotLeaderError: this node still leads, so a relay must not
			// report NO_QUORUM and a proposer must not retry the same wait for thirty seconds.
			return &Error{Code: CodeUnavailable, Message: fmt.Sprintf("node %s was waiting for its state to catch up with the log (applied %d of %d) when its time ran out, so nothing was changed. Run the command again in a moment: %v", r.cfg.NodeID, r.fsm.applied.Load(), last, context.Cause(ctx))}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil
}

// NotLeaderError is the refusal of a node that does not lead the Raft group. It unwraps to
// the UNAVAILABLE *Error that names the fix, so callers that branch on the code are
// unchanged, and it carries what a relay needs: the leader to try next and whether the
// command may nevertheless have been appended.
type NotLeaderError struct {
	// Refusal is the UNAVAILABLE error with the words a person reads.
	Refusal *Error
	// Leader is the id of the leader this node knows, empty when none is elected yet.
	Leader string
	// Undecided is true when leadership was lost after the command was appended: it may
	// still commit under the next leader. False means nothing was appended.
	Undecided bool
}

// Error returns the refusal's message.
func (e *NotLeaderError) Error() string { return e.Refusal.Message }

// Unwrap lets errors.As find the *Error.
func (e *NotLeaderError) Unwrap() error { return e.Refusal }

func (r *Raft) proposeError(err error) error {
	lost := errors.Is(err, hraft.ErrLeadershipLost)
	if errors.Is(err, hraft.ErrNotLeader) || lost || errors.Is(err, hraft.ErrLeadershipTransferInProgress) {
		// The second result is the leader's id, not an error; an empty
		// address means no leader is known, which the message below says.
		leader, _ := r.raft.LeaderWithID()
		where := "no leader is elected yet; retry in a moment"
		if leader != "" {
			where = fmt.Sprintf("the leader is at %s; send the command there", leader)
		}
		return &NotLeaderError{
			Refusal:   &Error{Code: CodeUnavailable, Message: fmt.Sprintf("node %s is not the Raft leader, so it cannot change the cluster: %s", r.cfg.NodeID, where)},
			Leader:    string(leader),
			Undecided: lost,
		}
	}
	return fmt.Errorf("commit through raft: %w", err)
}

// propose builds a command against the current state_seq and commits it.
// A command refused STATE_MOVED changed nothing (something else committed between reading
// state_seq and the append), so it is stamped again, a bounded number of times: a join, a
// promotion or a removal must not be left half done by a concurrent relayed command, because
// ClusterState.Voters is what the relay's voter check reads.
func (r *Raft) propose(kind byte, body []byte) (Result, error) {
	var result Result
	var err error
	for range proposeStamps {
		result, err = r.Propose(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(r.State().StateSeq), Body: body})
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeStateMoved {
			break
		}
	}
	return result, err
}

// proposeStamps bounds how often a command is stamped again after STATE_MOVED.
const proposeStamps = 3

// JoinRequest describes a node that asked to join.
type JoinRequest struct {
	// Member carries the id, incarnation, data address, client address and witness flag.
	Member Member
	// RaftAddr is where the joiner's Raft channel listens.
	RaftAddr string
	// PlacingSet is the 1024-byte bitmap of the slots the joiner will pull.
	PlacingSet []byte
	// Range is the state-level range the joiner's identity reply declared; REGISTER_MEMBER carries
	// its Min and Max and is refused when the cluster version is outside them. Zero takes this
	// node's own range: test-only, for the tests of this package that join a node of their own
	// build without asking it. Production never sends zero: wire.Node.runJoin always sets it from
	// the identity reply, and a joiner whose reply declares no range is refused by its range
	// check before RegisterJoiner runs (harness TestALevelZeroJoinerIsRefusedByName), so the
	// fallback cannot stand a leader's range in for a joiner's.
	Range StateRange
}

// Join adds the joiner to the library as a nonvoter and commits
// REGISTER_MEMBER{joining}. Registered but not ranked: it moves no slot, so
// the epoch does not change.
//
// Join, AddJoiner and RegisterJoiner do not judge the joiner's state range: the join rule (a
// refusal before anything is committed, with the text that names the release) is in
// wire.Node.runJoin, which asks the node who it is first. A caller that skips it still gets no
// serving member from a node on another build: every acceptor refuses that node's connections
// (AdmitPeer), the Raft channel included, so it can neither replicate nor be read from.
func (r *Raft) Join(req JoinRequest) (Result, error) {
	if err := r.AddJoiner(req.Member.ID, req.RaftAddr); err != nil {
		return Result{}, err
	}
	defer r.EndJoin(req.Member.ID)
	return r.RegisterJoiner(req)
}

// AddJoiner is the first step of a join: the library adds id as a nonvoter at addr, and the join is
// marked in flight on this leader until EndJoin, so the orphan sweep of ReconcileVoters does not
// remove the server before its member row commits. A failed add leaves nothing and no mark.
func (r *Raft) AddJoiner(id, addr string) error {
	if r.IsDiverged() {
		return divergedProposalRefusal()
	}
	if err := refuseWildcardSeat(addr); err != nil {
		return fmt.Errorf("join of %q: %w", id, err)
	}
	r.joinsMu.Lock()
	if r.joins == nil {
		r.joins = map[string]struct{}{}
	}
	r.joins[id] = struct{}{}
	r.joinsMu.Unlock()
	if err := r.raft.AddNonvoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, raftApplyTimeout).Error(); err != nil {
		r.EndJoin(id)
		return r.proposeError(err)
	}
	return nil
}

// EndJoin clears the in-flight mark AddJoiner set. Call it once the registration has committed or
// been refused and rolled back.
func (r *Raft) EndJoin(id string) {
	r.joinsMu.Lock()
	delete(r.joins, id)
	r.joinsMu.Unlock()
}

// joinInFlight reports whether a join of id is running on this leader.
func (r *Raft) joinInFlight(id string) bool {
	r.joinsMu.Lock()
	defer r.joinsMu.Unlock()
	_, running := r.joins[id]
	return running
}

// RegisterJoiner is the second step of a join: REGISTER_MEMBER{joining}. Registered but not
// ranked, it moves no slot, so the epoch does not change. A refused registration takes the
// nonvoter AddJoiner added back out of the Raft configuration, so the leader does not keep
// dialling a server no row names; if that removal fails too, the orphan sweep repairs it.
func (r *Raft) RegisterJoiner(req JoinRequest) (Result, error) {
	levels := req.Range
	if levels == (StateRange{}) {
		levels = r.Range()
	}
	result, err := r.propose(CommandRegisterMember, RegisterMemberBody(req.Member, req.PlacingSet, levels))
	if err != nil {
		if removeErr := r.raft.RemoveServer(hraft.ServerID(req.Member.ID), 0, raftApplyTimeout).Error(); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("take %s back out of the Raft configuration: %w", req.Member.ID, removeErr))
		}
		return Result{}, err
	}
	return result, nil
}

// IDAt is the id of the server whose address is addr, for a dial that carries no id (the Raft
// stream layer dials by address) to learn whose key to demand. It reads the committed member rows
// first, then the Raft configuration, which the library persists: a nonvoter that a leader added
// and then died before its row committed is still named there, so a dial to it is held to its
// own key. It names no one for an address neither holds (found false, no error); an error means
// the configuration could not be read just now (leadership could not be verified), which is not the
// same fact and is retried by the caller's next dial. Nothing is remembered in memory.
func (r *Raft) IDAt(addr string) (id string, found bool, err error) {
	r.fsm.mu.RLock()
	for i := range r.fsm.state.Members {
		if r.fsm.state.Members[i].DataAddr == addr {
			member := r.fsm.state.Members[i].ID
			r.fsm.mu.RUnlock()
			return member, true, nil
		}
	}
	r.fsm.mu.RUnlock()
	conf, err := r.configuration()
	if err != nil {
		return "", false, fmt.Errorf("reading the Raft configuration: %w", err)
	}
	for _, server := range conf.Servers {
		if string(server.Address) == addr {
			return string(server.ID), true, nil
		}
	}
	return "", false, nil
}

// Promote makes a registered nonvoter a voter: the library's AddVoter, which
// promotes it once it has caught up, then exactly one SET_VOTERS. A voter is
// not counted until its match index is current, so promotion costs no
// availability. SET_VOTERS moves no slot, so the epoch does not change.
func (r *Raft) Promote(id string) (Result, error) { return r.PromoteWithin(id, 0) }

// PromoteWithin is Promote that refuses (P1) when the promotion would make more than limit
// voters; a limit of 0 is no limit. The count is the voters of the Raft configuration plus the
// witnesses the state records as voters (a witness votes by SET_VOTERS alone and is never in the
// configuration), read under voterMu before AddVoter. The library commits one configuration change
// at a time through one leader, so the configuration is the linearisable count; the state's voter
// list can lag it by one SET_VOTERS after a leadership change, which ReconcileVoters repairs and
// this count tolerates because it does not read the list. A voter that is promoted again is not
// counted twice.
func (r *Raft) PromoteWithin(id string, limit int) (Result, error) {
	if r.IsDiverged() {
		return Result{}, divergedProposalRefusal()
	}
	r.voterMu.Lock()
	defer r.voterMu.Unlock()
	if limit > 0 {
		// A leader that has just been elected may not have applied entries the leaders before it
		// committed, and a witness is counted from the applied state alone (it is never in the
		// configuration). Wait for the log to be applied so a SET_VOTERS{witness} that is
		// committed and not yet applied is counted.
		if err := r.raft.Barrier(raftApplyTimeout).Error(); err != nil {
			return Result{}, r.proposeError(err)
		}
	}
	conf, err := r.configuration()
	if err != nil {
		return Result{}, err
	}
	state := r.State()
	row, member := state.memberIndex(id)
	if !member {
		return Result{}, &Error{Code: CodeNotFound, Message: fmt.Sprintf("Node %q has not joined this cluster. Run 'join %s@<host:port>' on the leader first, then promote it", id, id)}
	}
	witness := state.Members[row].Witness
	var server *hraft.Server
	for i := range conf.Servers {
		if string(conf.Servers[i].ID) == id {
			server = &conf.Servers[i]
		}
	}
	if server == nil {
		return Result{}, &Error{Code: CodeNotFound, Message: fmt.Sprintf("Node %q has not joined this cluster. Run 'join %s@<host:port>' on the leader first, then promote it", id, id)}
	}
	listed := slices.Contains(state.Voters, id)
	if listed && (witness || server.Suffrage == hraft.Voter) {
		// Already a voter in both books: the committed facts, and nothing to commit.
		return Result{Epoch: state.Epoch, StateSeq: state.StateSeq}, nil
	}
	if limit > 0 && !listed && (witness || server.Suffrage != hraft.Voter) {
		voters := r.countVoters(conf)
		if r.afterVoterCount != nil {
			r.afterVoterCount(voters)
		}
		if voters+1 > limit {
			return Result{}, &VoterCapError{Refusal: voterCapRefusal(id, voters+1, limit)}
		}
	}
	if !witness {
		// A member that moved (a replaced node) is dialled where the cluster committed it: the
		// library re-points an existing id's address when AddVoter names a new one.
		address := server.Address
		if committed := state.Members[row].DataAddr; committed != "" && hraft.ServerAddress(committed) != address {
			address = hraft.ServerAddress(committed)
		}
		if err := r.raft.AddVoter(server.ID, address, 0, raftApplyTimeout).Error(); err != nil {
			return Result{}, r.proposeError(err)
		}
		if gate := betweenAddVoterAndSetVoters; gate != nil {
			gate(id)
		}
	}
	// A witness votes by SET_VOTERS alone: it is a nonvoter in the library's configuration.
	return r.propose(CommandSetVoters, SetVotersBody(id, true))
}

// countVoters is the number of voters the cap counts: the configuration's voters plus the
// witnesses the state records as voters.
func (r *Raft) countVoters(conf hraft.Configuration) int {
	voters := 0
	for _, server := range conf.Servers {
		if server.Suffrage == hraft.Voter {
			voters++
		}
	}
	state := r.State()
	for _, id := range state.Voters {
		if i, member := state.memberIndex(id); member && state.Members[i].Witness {
			voters++
		}
	}
	return voters
}

// VoterCapError is P1: a promotion that would pass cluster.voters. It unwraps to the
// INVALID_ARGUMENT *Error, so a caller that branches on the code is unchanged, and a caller that
// wants to treat a full voter set as an answer (a join that finds no free slot) can tell it apart.
type VoterCapError struct{ Refusal *Error }

// Error returns the refusal's message.
func (e *VoterCapError) Error() string { return e.Refusal.Message }

// Unwrap lets errors.As find the *Error.
func (e *VoterCapError) Unwrap() error { return e.Refusal }

// voterCapRefusal is P1: the promotion would pass cluster.voters.
func voterCapRefusal(id string, would, limit int) *Error {
	return &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf(
		"promote %s would make %d voters and cluster.voters is %d. %d voters is the number this cluster runs: leave %s a non-voting member, "+
			"or free a slot first with 'leave <voter>' or 'remove-node <dead voter>' (those two arrive in the next release; until then the voters stay as they are)", id, would, limit, limit, id)}
}

// RemoveServer takes a server out of the library's configuration and, when it
// was a voter, commits SET_VOTERS{voting: false}. The planned-leave sequence
// calls it after the leaver's HANDOFF_READY.
func (r *Raft) RemoveServer(id string) (Result, error) {
	if r.IsDiverged() {
		return Result{}, divergedProposalRefusal()
	}
	r.voterMu.Lock()
	defer r.voterMu.Unlock()
	wasVoter := false
	for _, voter := range r.State().Voters {
		wasVoter = wasVoter || voter == id
	}
	if err := r.raft.RemoveServer(hraft.ServerID(id), 0, raftApplyTimeout).Error(); err != nil {
		return Result{}, r.proposeError(err)
	}
	if !wasVoter {
		st := r.State()
		return Result{Epoch: st.Epoch, StateSeq: st.StateSeq}, nil
	}
	return r.propose(CommandSetVoters, SetVotersBody(id, false))
}

// ReenterAsJoiner is the Raft half of replace-node (BUG-0870 design section 5.2 step 6): the server
// id, whose node lost its log, leaves the configuration and comes back as a nonvoter at addr. The
// library counts a voter's acknowledgements by the highest index it ever matched and never lowers
// it while the voter stays in the configuration; a configuration without id as a voter rebuilds that
// count without it, so once the node is promoted again it is counted from what it has really
// stored. AddNonvoter alone would not do: it leaves a server that is already a voter a voter. A
// server the configuration does not name is only added. The member row is not touched here.
func (r *Raft) ReenterAsJoiner(id, addr string) error {
	conf, err := r.configuration()
	if err != nil {
		return err
	}
	if slices.ContainsFunc(conf.Servers, func(server hraft.Server) bool { return string(server.ID) == id }) {
		if _, err := r.RemoveServer(id); err != nil {
			return fmt.Errorf("taking %s out of the Raft configuration: %w", id, err)
		}
	}
	if err := r.AddJoiner(id, addr); err != nil {
		return fmt.Errorf("adding %s back as a nonvoter: %w", id, err)
	}
	// The member row exists, so the orphan sweep keeps the server without the in-flight mark.
	r.EndJoin(id)
	return nil
}

// Close stops the library and every goroutine it started, the stream layer
// and the log file. It is safe to call twice.
func (r *Raft) Close() error {
	r.closeOnce.Do(func() {
		var shutdownErr error
		if r.raft != nil {
			shutdownErr = r.raft.Shutdown().Error()
		}
		r.closeErr = errors.Join(shutdownErr, r.stream.Close(), r.store.Close())
	})
	return r.closeErr
}

// refuseWildcardSeat refuses a Raft seat nobody can dial: a wildcard host ([::], 0.0.0.0 or none)
// names the dialling machine itself, so every peer would connect to itself (BUG-0761). A data
// directory already written with such a seat is not repaired: pre-1.0 there is no installed
// base, so delete the node's raft directory and bootstrap or join again.
func refuseWildcardSeat(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err == nil && host != "" {
		if ip := net.ParseIP(host); ip == nil || !ip.IsUnspecified() {
			return nil
		}
	}
	return fmt.Errorf("this node's cluster address is %q, a wildcard that peers cannot dial (they would connect to themselves). "+
		"Set HOARDDB_CLUSTER_ADVERTISE=<host:port> (cluster.advertise) to the address the other nodes reach this node at", addr)
}
