package wire

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// ErrNoPeers is what the parts of the cluster protocol that need another node answer on a
// node that has none: a one-voter cluster never sends a barrier, a catch-up or a forward.
var ErrNoPeers = errors.New("this node has no peers")

// databaseRow is one committed database as the owner path needs it.
type databaseRow struct {
	name    string
	info    DatabaseInfo
	version int64
}

// topologyView is the committed state at one state_seq, built once per change so that a
// write reads it with no copy and no lock.
type topologyView struct {
	epoch   uint64
	seq     int64
	members int
	// stepDownMs is the committed step_down_ms, which the LEASE_LAPSED text prints.
	stepDownMs int64
	// voters and selfState are what describe_node reports, computed once per committed change.
	voters    int
	selfState string
	// clusterVersion is the cluster version of this state, which describe_node reports.
	clusterVersion uint8
	// databases are the committed rows by id ascending, for RING_TOPOLOGY.
	databases []cluster.Database
	table     *cluster.SlotTable
	// witnesses are the voting-or-not members that hold no documents, by id; they are not
	// in the table, and RING_TOPOLOGY lists them apart.
	witnesses []cluster.Member
	// committedVoters is the length of the committed voter list, which counts a voter that is joining
	// or dead and so is on neither TOPOLOGY blob.
	committedVoters int32
	byID            map[uint64]databaseRow
	byName          map[string]uint64
	// anyMemory is true when a committed database is a memory database, so a write path with
	// no memory database reads one flag and nothing else.
	anyMemory bool
	// staleReads is cluster.stale_reads at this state_seq, so a keyed read asks one atomic
	// load and never clones the committed state.
	staleReads bool
}

// ClusterTopology is the Topology of a node over the committed cluster state. Every read
// is one atomic load; Refresh rebuilds the view after a command committed.
type ClusterTopology struct {
	self   string
	source StateSource
	view   atomic.Pointer[topologyView]
	seq    atomic.Int64
	// refreshMu keeps two readers that both saw a stale view from rebuilding it at once.
	refreshMu sync.Mutex
	// bumps holds the published schema_version of each database. Nil takes the committed
	// version, which is right for a topology that has no bump to publish.
	bumps *cluster.BumpApplier
}

// NewClusterTopology returns a topology for node self over source and loads the state.
func NewClusterTopology(self string, source StateSource) *ClusterTopology {
	topo := &ClusterTopology{self: self, source: source}
	topo.Refresh()
	return topo
}

// Refresh rebuilds the view from the committed state when its state_seq moved and returns
// the state it read. It is called after every command this node proposes.
func (t *ClusterTopology) Refresh() cluster.ClusterState {
	state := t.source.State()
	if t.view.Load() != nil && t.seq.Load() == state.StateSeq {
		return state
	}
	view := &topologyView{
		epoch:          uint64(state.Epoch),
		seq:            state.StateSeq,
		members:        len(state.Members),
		voters:         len(state.Voters),
		selfState:      selfMemberState(t.self, state.Members),
		clusterVersion: state.ClusterVersion,
		databases:      slices.SortedFunc(slices.Values(state.Databases), func(a, b cluster.Database) int { return cmp.Compare(a.ID, b.ID) }),
		table:          cluster.BuildTable(state),
		staleReads:     state.Settings.StaleReads,
		stepDownMs:     state.Settings.StepDownMs,
		byID:           make(map[uint64]databaseRow, len(state.Databases)),
		byName:         make(map[string]uint64, len(state.Databases)),
	}
	for _, member := range state.Members {
		if member.Witness && (member.State == cluster.MemberActive || member.State == cluster.MemberDown || member.State == cluster.MemberLeaving) {
			view.witnesses = append(view.witnesses, member)
		}
	}
	view.committedVoters = int32(len(state.Voters))
	slices.SortFunc(view.witnesses, func(a, b cluster.Member) int { return cmp.Compare(a.ID, b.ID) })
	for _, row := range state.Databases {
		memory := row.Storage == cluster.StorageMemory
		view.anyMemory = view.anyMemory || memory
		view.byID[uint64(row.ID)] = databaseRow{
			name:    row.Name,
			info:    DatabaseInfo{Name: row.Name, RF: int(row.RF), DefaultConcern: machine.ConcernMajority, Memory: memory},
			version: row.SchemaVersion,
		}
		view.byName[row.Name] = uint64(row.ID)
	}
	t.view.Store(view)
	t.seq.Store(state.StateSeq)
	return state
}

// seqSource is a StateSource that can say cheaply whether the state moved.
type seqSource interface{ StateSeq() int64 }

// current is the view, rebuilt first when the committed state moved since it was built.
// A node that learns state from its peers (a follower) never calls Refresh itself, so
// every reader checks; for a source without a cheap sequence the view is only what the
// last Refresh built.
func (t *ClusterTopology) current() *topologyView {
	if cheap, ok := t.source.(seqSource); ok && cheap.StateSeq() != t.seq.Load() {
		t.refreshMu.Lock()
		t.Refresh()
		t.refreshMu.Unlock()
	}
	return t.view.Load()
}

// StepDownMs is cluster.step_down_ms as committed.
func (t *ClusterTopology) StepDownMs() int64 { return t.current().stepDownMs }

// StaleReads is cluster.stale_reads as committed: true lets a ready copy answer keyed reads.
func (t *ClusterTopology) StaleReads() bool { return t.current().staleReads }

// Committed implements Topology.
func (t *ClusterTopology) Committed() uint64 { return t.current().epoch }

// Self implements Topology.
func (t *ClusterTopology) Self() string { return t.self }

// Members implements Topology.
func (t *ClusterTopology) Members() int { return t.current().members }

// Table implements Topology.
func (t *ClusterTopology) Table() *cluster.SlotTable { return t.current().table }

// Database implements Topology.
func (t *ClusterTopology) Database(db uint64) (DatabaseInfo, bool) {
	row, ok := t.current().byID[db]
	return row.info, ok
}

// MemoryDatabase reports whether db is a memory database; an unknown db is false.
func (t *ClusterTopology) MemoryDatabase(db uint64) bool {
	row, ok := t.current().byID[db]
	return ok && row.info.Memory
}

// AnyMemory reports whether the committed state holds a memory database.
func (t *ClusterTopology) AnyMemory() bool { return t.current().anyMemory }

// DatabaseAtEpoch is Database and Committed read from one view, so the replication factor
// and the epoch belong to the same committed state.
func (t *ClusterTopology) DatabaseAtEpoch(db uint64) (DatabaseInfo, uint64, bool) {
	view := t.current()
	row, ok := view.byID[db]
	return row.info, view.epoch, ok
}

// Target implements Topology. The committed state holds no catch-up target, so a bare
// ClusterTopology reports none; WithHandoffTarget supplies the Handoff's.
func (t *ClusterTopology) Target(uint16, uint64) cluster.Position { return cluster.Position{} }

// DatabaseName is the name a database id placed under.
func (t *ClusterTopology) DatabaseName(db uint64) (string, bool) {
	row, ok := t.current().byID[db]
	return row.name, ok
}

// DatabaseID is the id a database name resolves to.
func (t *ClusterTopology) DatabaseID(name string) (uint64, bool) {
	id, ok := t.current().byName[name]
	return id, ok
}

// Published is the database's schema_version, the one this node publishes. It trails the
// committed version for the length of a bump's pair-lock pass (conv 20 step 6) and equals it
// otherwise.
func (t *ClusterTopology) Published(db uint64) int64 {
	if t.bumps != nil {
		if version, ok := t.bumps.Published(db); ok {
			return version
		}
	}
	return t.current().byID[db].version
}

// Committed schema_version of the database in the last refreshed view, which may be ahead of
// Published while a bump is being published.
func (t *ClusterTopology) CommittedSchema(db uint64) int64 {
	return t.current().byID[db].version
}

// LocalDeclarationReader reads declaration and reverse-index documents from this node's own
// Badger (conv 20): the keyed read at the owner, assigning no position. A slot another
// node owns needs the transport, which a one-voter cluster never uses.
type LocalDeclarationReader struct {
	kv     storage.KV
	self   string
	advice string // added to a CORRUPT_KEY refusal (corruptKey)
}

// NewLocalDeclarationReader returns a reader over kv for node self; advice is added to a
// CORRUPT_KEY refusal (the node's ringDirAdvice).
func NewLocalDeclarationReader(kv storage.KV, self, advice string) *LocalDeclarationReader {
	return &LocalDeclarationReader{kv: kv, self: self, advice: advice}
}

var _ DeclarationReader = (*LocalDeclarationReader)(nil)

func (r *LocalDeclarationReader) document(owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	if owner.ID != r.self {
		return nil, false, fmt.Errorf("slot %d belongs to %s and this node has no connection to it: %w", read.Slot, owner.ID, ErrNoPeers)
	}
	snapshot := r.kv.NewSnapshot()
	defer snapshot.Discard()
	doc, found, err := readStoredDocument(snapshot, ReadRequest{Slot: read.Slot, DBID: read.DBID, BucketID: store.BucketsBucketID, EncID: read.EncodedID}, r.advice)
	if err != nil || !found {
		return nil, false, err
	}
	stored := make(map[uint16][]byte, len(doc.fields))
	for _, field := range doc.fields {
		stored[field.id] = field.stored
	}
	return stored, true, nil
}

// ReadDeclaration implements DeclarationReader.
func (r *LocalDeclarationReader) ReadDeclaration(_ context.Context, owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	return r.document(owner, read)
}

// ReadBucketName implements DeclarationReader: the name is field 7 of the reverse document.
func (r *LocalDeclarationReader) ReadBucketName(_ context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	stored, found, err := r.document(owner, read)
	if err != nil || !found {
		return "", false, err
	}
	name, err := storedString(stored[createFieldName])
	if err != nil {
		return "", false, fmt.Errorf("reading the name of the reverse-index document %x: %w", read.EncodedID, err)
	}
	return name, true, nil
}

// ReadReverseState returns the state word of the reverse-index document of read.
func (r *LocalDeclarationReader) ReadReverseState(_ context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	stored, found, err := r.document(owner, read)
	if err != nil || !found {
		return "", false, err
	}
	state, err := storedString(stored[createFieldState])
	if err != nil {
		return "", false, fmt.Errorf("reading the state of the reverse-index document %x: %w", read.EncodedID, err)
	}
	return state, true, nil
}

// storedString decodes a stored string value: the type byte 0x02, a 4-byte little-endian
// length counting the trailing zero, the bytes and the zero.
func storedString(stored []byte) (string, error) {
	if len(stored) < 6 || stored[0] != 0x02 {
		return "", fmt.Errorf("a stored string has %d bytes and starts with %#02x, want type 0x02 and a length", len(stored), firstByte(stored))
	}
	length := int(stored[1]) | int(stored[2])<<8 | int(stored[3])<<16 | int(stored[4])<<24
	body := stored[5:]
	if length < 1 || length != len(body) || body[length-1] != 0 {
		return "", fmt.Errorf("a stored string says %d bytes and carries %d", length, len(body))
	}
	return string(bytes.Clone(body[:length-1])), nil
}

func firstByte(raw []byte) byte {
	if len(raw) == 0 {
		return 0
	}
	return raw[0]
}

// ServedLease is the node's lease (conv §15) behind a lock: machine.LeaseState is not safe
// for concurrent use. A one-voter cluster never asks for a grant, so every check answers
// from the empty state, and the owner path does not need the lease when it is alone.
type ServedLease struct {
	mu    sync.Mutex
	state machine.LeaseState
	cfg   machine.LeaseConfig
}

// NewServedLease returns a lease with cfg.
func NewServedLease(cfg machine.LeaseConfig) *ServedLease {
	return &ServedLease{cfg: cfg}
}

// Serve implements ServeChecker.
func (l *ServedLease) Serve(check machine.ServeCheck) machine.LeaseDecision {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state.Apply(check, l.cfg)
}

// NoCopies is the Replication of a pair with no copy: the owner is the only holder, so
// nothing is queued, nothing is awaited and no copy has acked. It is the whole truth for
// RF 1; a database with more copies needs the Replicator.
type NoCopies struct{}

// Enqueue implements Replication.
func (NoCopies) Enqueue(uint16, uint64, cluster.Position, cluster.Position, uint64) {}

// Await implements Replication.
func (NoCopies) Await(context.Context, uint16, uint64, cluster.Position, int) int { return 0 }

// NoteWaitTimeout implements Replication.
func (NoCopies) NoteWaitTimeout(uint16, uint64, cluster.Position, time.Duration) {}

// Acked implements Replication.
func (NoCopies) Acked(uint16, uint64, cluster.Position) int { return 0 }

// Covered implements Replication.
func (NoCopies) Covered(uint16, uint64, cluster.Position) bool { return false }

// NoPeers is the Link, catch-up, drain and read forwarder of a node with no peers. Every
// call that would reach another node says so.
type NoPeers struct{}

var (
	_ HandoffLink    = NoPeers{}
	_ HandoffCatchUp = NoPeers{}
	_ HandoffDrain   = NoPeers{}
	_ ReadForwarder  = NoPeers{}
)

// SendHandoffReq implements HandoffLink.
func (NoPeers) SendHandoffReq(_ context.Context, peer string, _ uint64, _ HandoffReq) error {
	return fmt.Errorf("sending HANDOFF_REQ to %s: %w", peer, ErrNoPeers)
}

// SendHandoffStopped implements HandoffLink.
func (NoPeers) SendHandoffStopped(_ context.Context, peer string, _ uint64, _ HandoffStopped) error {
	return fmt.Errorf("sending HANDOFF_STOPPED to %s: %w", peer, ErrNoPeers)
}

// SendHandoffReady implements HandoffLink.
func (NoPeers) SendHandoffReady(_ context.Context, peer string, _ uint64, _ HandoffReady) error {
	return fmt.Errorf("sending HANDOFF_READY to %s: %w", peer, ErrNoPeers)
}

// QueryPosition implements HandoffLink.
func (NoPeers) QueryPosition(_ context.Context, peer string, _ PositionQuery) (PositionReply, error) {
	return PositionReply{}, fmt.Errorf("asking %s for a position: %w", peer, ErrNoPeers)
}

// CatchUp implements HandoffCatchUp.
func (NoPeers) CatchUp(_ context.Context, slot uint16, db uint64, _ cluster.Position, _ uint64) error {
	return fmt.Errorf("catching up slot %d of database %d: %w", slot, db, ErrNoPeers)
}

// IsCurrent implements HandoffCatchUp.
func (NoPeers) IsCurrent(uint16, uint64) bool { return false }

// NoteDrained implements HandoffDrain.
func (NoPeers) NoteDrained(uint16, uint64, cluster.Position) {}

// ForwardRead implements ReadForwarder.
func (NoPeers) ForwardRead(_ context.Context, owner string, req ReadRequest) (Document, error) {
	return Document{}, fmt.Errorf("forwarding the read of %s to %s: %w", req.idText(), owner, ErrNoPeers)
}

// ForwardFieldRead implements ReadForwarder.
func (NoPeers) ForwardFieldRead(_ context.Context, owner string, forwarded FieldForward) (FieldAnswer, error) {
	return FieldAnswer{}, fmt.Errorf("forwarding the read of %s to %s: %w", forwarded.Req.idText(), owner, ErrNoPeers)
}

// SingleVoterSlots reports every slot ready while the cluster has one member: a node that
// never seeds from a peer holds each of its slots from the start. With more members a
// slot may still be seeding, and this node cannot tell, so it reports seeding and the
// read gate refuses rather than serving a slot that may be incomplete.
type SingleVoterSlots struct{ Topology Topology }

// Serves implements SlotStates: a one-voter cluster has no copy to answer.
func (SingleVoterSlots) Serves(uint16, uint64) bool { return false }

// State implements SlotStates.
func (s SingleVoterSlots) State(uint16, uint64) machine.SlotState {
	if s.Topology.Members() == 1 {
		return machine.SlotReady
	}
	return machine.SlotSeeding
}

// ownedSlots is the mask of the slots of database db that the table gives this node.
func ownedSlots(topo Topology, db uint64) cluster.SlotMask {
	var mask cluster.SlotMask
	info, ok := topo.Database(db)
	if !ok {
		return mask
	}
	table := topo.Table()
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		if owner, held := table.Owner(slot, info.RF); held && owner.ID == topo.Self() {
			mask.Set(slot)
		}
	}
	return mask
}

// selfMemberState is the state word of the member row for self, or empty when there is none.
func selfMemberState(self string, members []cluster.Member) string {
	for _, member := range members {
		if member.ID == self {
			return member.State.String()
		}
	}
	return ""
}
