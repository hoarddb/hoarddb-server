package cluster

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/namepolicy"
)

// CommandVersion is the first byte of every command payload.
const CommandVersion byte = 0x01

// The Raft command kinds (conventions section 13), a different number space
// from the transport message kinds. 0x08 to 0x0A are withdrawn, 0x11 to 0x16 are the user
// commands (fsm_users.go) and 0x17 to 0x1F are unassigned; an unknown kind is refused
// INVALID_ARGUMENT.
const (
	CommandBootstrap      byte = 0x01
	CommandSetMemberState byte = 0x02
	CommandRegisterMember byte = 0x03
	CommandRemoveMember   byte = 0x04
	CommandCreateDatabase byte = 0x05
	CommandAlterDatabase  byte = 0x06
	CommandDropDatabase   byte = 0x07
	CommandSetSetting     byte = 0x0B
	CommandSeeded         byte = 0x0C
	CommandRetire         byte = 0x0D
	CommandBumpSchema     byte = 0x0E
	CommandSetVoters      byte = 0x0F
	// CommandSetClusterVersion moves the committed cluster version (SET_CLUSTER_VERSION,
	// cluster-version spec 4.3). Nothing in this release proposes it: finalize is a later step.
	CommandSetClusterVersion byte = 0x10
	// 0x11 to 0x16 are the user commands, in fsm_users.go.
)

// commandHeaderBytes is [version:1][kind:1][base_seq:8].
const commandHeaderBytes = 10

// Command is one replicated change to ClusterState, encoded as
// [version:1][kind:1][base_seq:8 BE][BSON body] (membership Formats 1).
// BaseSeq is the state_seq the proposer built the command against.
type Command struct {
	Version, Kind byte
	BaseSeq       uint64
	Body          []byte
}

// Result reports what an accepted command did to the two counters.
type Result struct {
	Epoch    int64
	StateSeq int64
	// EpochMoved is true when the command moved the ring projection.
	EpochMoved bool
}

func knownCommandKind(kind byte) bool {
	switch kind {
	case CommandBootstrap, CommandSetMemberState, CommandRegisterMember, CommandRemoveMember,
		CommandCreateDatabase, CommandAlterDatabase, CommandDropDatabase, CommandSetSetting,
		CommandSeeded, CommandRetire, CommandBumpSchema, CommandSetVoters, CommandSetClusterVersion:
		return true
	}
	return UserCommandKind(kind)
}

func commandRefusal(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func checkCommandHeader(version, kind byte) error {
	if version != CommandVersion {
		return commandRefusal(CodeInvalidArgument, "command version %d is not supported: this build reads version %d", version, CommandVersion)
	}
	if !knownCommandKind(kind) {
		return commandRefusal(CodeInvalidArgument, "command kind 0x%02x is not a command: use 0x01-0x07 or 0x0B-0x16", kind)
	}
	return nil
}

func checkCommandShape(version, kind byte, body []byte) error {
	if err := checkCommandHeader(version, kind); err != nil {
		return err
	}
	if len(body) < 5 || int(binary.LittleEndian.Uint32(body)) != len(body) {
		return commandRefusal(CodeInvalidArgument, "command body is %d bytes but its length prefix says otherwise: it must be exactly one BSON document with no trailing bytes", len(body))
	}
	if err := bsoncore.Document(body).Validate(); err != nil {
		return commandRefusal(CodeInvalidArgument, "command body is not a valid BSON document (%v): build it with a BSON encoder", err)
	}
	return nil
}

// EncodeCommand writes [version][kind][base_seq BE][body]. It refuses an
// unsupported version, an unknown kind, and a body that is not exactly one
// well-formed BSON document, all with INVALID_ARGUMENT.
func EncodeCommand(cmd Command) ([]byte, error) {
	if err := checkCommandShape(cmd.Version, cmd.Kind, cmd.Body); err != nil {
		return nil, err
	}
	raw := make([]byte, commandHeaderBytes, commandHeaderBytes+len(cmd.Body))
	raw[0], raw[1] = cmd.Version, cmd.Kind
	binary.BigEndian.PutUint64(raw[2:], cmd.BaseSeq)
	return append(raw, cmd.Body...), nil
}

// DecodeCommand reads the bytes EncodeCommand writes and refuses everything
// else with INVALID_ARGUMENT. It never panics, and a payload it accepts
// encodes back to the same bytes. It checks the shape only; the fields of
// the body are checked by ApplyCommand against the state.
func DecodeCommand(raw []byte) (Command, error) {
	if len(raw) < commandHeaderBytes {
		return Command{}, commandRefusal(CodeInvalidArgument, "command is %d bytes, shorter than its %d-byte header: it must be [version][kind][base_seq:8][BSON]", len(raw), commandHeaderBytes)
	}
	cmd := Command{Version: raw[0], Kind: raw[1], BaseSeq: binary.BigEndian.Uint64(raw[2:]), Body: slices.Clone(raw[commandHeaderBytes:])}
	if err := checkCommandShape(cmd.Version, cmd.Kind, cmd.Body); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

// commandFields reads one command body. Each field is read by name, in any
// order; a repeated, missing, mistyped or unread field is refused.
type commandFields struct {
	where string
	vals  map[string]bsoncore.Value
	used  map[string]bool
	// hide and hidden are the old-build seam (hideField): a field hide names is never visible to
	// a reader, as if this build did not know it, and finish reports it unread. Nil in
	// production.
	hide   func(field string) bool
	hidden []string
	// observe is the vocabulary-recording seam of the state golden: it is told every field a
	// reader asks for, present or not, with the BSON type it wants. Nil in production.
	observe func(where, key string, want bsontype.Type)
}

func newCommandFields(where string, doc bsoncore.Document) (*commandFields, error) {
	return newCommandFieldsHiding(where, doc, nil)
}

// newCommandFieldsHiding is newCommandFields for a build that does not read the fields hide
// names; a nil hide hides none. Test code reaches it only through applyCommandWith.
func newCommandFieldsHiding(where string, doc bsoncore.Document, hide func(field string) bool) (*commandFields, error) {
	return newCommandFieldsObserving(where, doc, hide, nil)
}

// newCommandFieldsObserving is newCommandFieldsHiding with the recording seam; a nil observe
// records nothing.
func newCommandFieldsObserving(where string, doc bsoncore.Document, hide func(field string) bool, observe func(where, key string, want bsontype.Type)) (*commandFields, error) {
	if err := doc.Validate(); err != nil {
		return nil, commandRefusal(CodeInvalidArgument, "%s is not a valid BSON document (%v)", where, err)
	}
	elems, err := doc.Elements()
	if err != nil {
		return nil, commandRefusal(CodeInvalidArgument, "%s cannot be read (%v)", where, err)
	}
	f := &commandFields{where: where, vals: make(map[string]bsoncore.Value, len(elems)), used: make(map[string]bool, len(elems)), hide: hide, observe: observe}
	for _, e := range elems {
		if _, dup := f.vals[e.Key()]; dup {
			return nil, commandRefusal(CodeInvalidArgument, "%s carries the field %q twice: write each field once", where, e.Key())
		}
		if hide != nil && hide(e.Key()) {
			f.hidden = append(f.hidden, e.Key())
			continue
		}
		f.vals[e.Key()] = e.Value()
	}
	return f, nil
}

// find returns a present field of the wanted type, or ok false when absent.
func (f *commandFields) find(key string, want bsontype.Type) (v bsoncore.Value, ok bool, err error) {
	if f.observe != nil {
		f.observe(f.where, key, want)
	}
	v, ok = f.vals[key]
	if !ok {
		return v, false, nil
	}
	f.used[key] = true
	if v.Type != want {
		return v, false, commandRefusal(CodeInvalidArgument, "%s field %q has BSON type %s, want %s", f.where, key, v.Type, want)
	}
	return v, true, nil
}

func (f *commandFields) missing(key string) error {
	return commandRefusal(CodeInvalidArgument, "%s is missing the field %q: send every field the command needs", f.where, key)
}

func (f *commandFields) str(key string) (string, error) {
	v, ok, err := f.find(key, bson.TypeString)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", f.missing(key)
	}
	return stateString(f.where, key, v.Data)
}

func (f *commandFields) int64(key string) (int64, error) {
	v, ok, err := f.find(key, bson.TypeInt64)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, f.missing(key)
	}
	return int64(binary.LittleEndian.Uint64(v.Data)), nil
}

func (f *commandFields) optInt64(key string) (n int64, present bool, err error) {
	if _, ok := f.vals[key]; !ok {
		return 0, false, nil
	}
	n, err = f.int64(key)
	return n, err == nil, err
}

func (f *commandFields) int32(key string) (int32, error) {
	v, ok, err := f.find(key, bson.TypeInt32)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, f.missing(key)
	}
	return int32(binary.LittleEndian.Uint32(v.Data)), nil
}

func (f *commandFields) boolean(key string) (bool, error) {
	v, ok, err := f.find(key, bson.TypeBoolean)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, f.missing(key)
	}
	if v.Data[0] > 1 {
		return false, commandRefusal(CodeInvalidArgument, "%s field %q is not a canonical boolean: its byte must be 0 or 1", f.where, key)
	}
	return v.Data[0] == 1, nil
}

// optStr reads a string whose absence is reported, not refused.
func (f *commandFields) optStr(key string) (value string, present bool, err error) {
	if _, ok := f.vals[key]; !ok {
		return "", false, nil
	}
	value, err = f.str(key)
	return value, err == nil, err
}

// optBool reads a boolean whose absence means false.
func (f *commandFields) optBool(key string) (bool, error) {
	if _, ok := f.vals[key]; !ok {
		return false, nil
	}
	return f.boolean(key)
}

// binary returns a generic-subtype binary of exactly size bytes, copied.
func (f *commandFields) binary(key string, size int) ([]byte, error) {
	v, ok, err := f.find(key, bson.TypeBinary)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, f.missing(key)
	}
	if len(v.Data) < 5 || v.Data[4] != 0 || len(v.Data)-5 != size {
		return nil, commandRefusal(CodeInvalidArgument, "%s field %q must be %d bytes of generic binary (a seed bitmap is exactly %d bytes, 8192 bits)", f.where, key, size, seedBitmapBytes)
	}
	return slices.Clone(v.Data[5:]), nil
}

// documents reads an array of embedded documents keyed "0", "1", ... in order, each read by its
// own commandFields named "<where> <key>[]". An absent array is refused like any missing field.
func (f *commandFields) documents(key string) ([]*commandFields, error) {
	v, ok, err := f.find(key, bson.TypeArray)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, f.missing(key)
	}
	elems, err := bsoncore.Document(v.Data).Elements()
	if err != nil {
		return nil, commandRefusal(CodeInvalidArgument, "%s field %q cannot be read (%v)", f.where, key, err)
	}
	rows := make([]*commandFields, 0, len(elems))
	for i, e := range elems {
		if e.Key() != strconv.Itoa(i) {
			return nil, commandRefusal(CodeInvalidArgument, "%s field %q has index key %q where %q is expected", f.where, key, e.Key(), strconv.Itoa(i))
		}
		if e.Value().Type != bson.TypeEmbeddedDocument {
			return nil, commandRefusal(CodeInvalidArgument, "%s field %q[%d] has BSON type %s, want %s", f.where, key, i, e.Value().Type, bson.TypeEmbeddedDocument)
		}
		row, err := newCommandFieldsObserving(f.where+" "+key+"[]", bsoncore.Document(e.Value().Data), f.hide, f.observe)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (f *commandFields) document(key string) (*commandFields, error) {
	v, ok, err := f.find(key, bson.TypeEmbeddedDocument)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, f.missing(key)
	}
	return newCommandFieldsObserving(f.where+" "+key, bsoncore.Document(v.Data), f.hide, f.observe)
}

// finish refuses a field nothing read. It names the first unread field in
// sorted order: the map has no order of its own, and the same body must
// always get the same refusal.
func (f *commandFields) finish() error {
	unread := slices.Clone(f.hidden)
	for key := range f.vals {
		if !f.used[key] {
			unread = append(unread, key)
		}
	}
	if len(unread) == 0 {
		return nil
	}
	slices.Sort(unread)
	// Not "remove it": the field may have been applied by every member that reads it, so an
	// operator who obeyed would send the cluster another body (section 6.1, text 3).
	return commandRefusal(CodeInvalidArgument, "%s carries the field %q, which this build does not read. If another member runs a newer build, start this node on that build; do not remove the field, the cluster may already have applied it", f.where, unread[0])
}

// optClientAddr reads client_addr, whose absence means the member publishes none. A field
// written empty is refused, as the state would not keep it.
func (f *commandFields) optClientAddr() (string, error) {
	if _, ok := f.vals["client_addr"]; !ok {
		return "", nil
	}
	text, err := f.str("client_addr")
	if err != nil {
		return "", err
	}
	if text == "" {
		return "", commandRefusal(CodeInvalidArgument, "%s field \"client_addr\" is empty: leave it out when the node has none", f.where)
	}
	return text, nil
}

// checkClientAddr applies ValidateClientAddr to a command's client address against its data address.
func (f *commandFields) checkClientAddr(clientAddr, dataAddr string) error {
	if clientAddr == "" {
		return nil
	}
	if err := ValidateClientAddr(clientAddr, dataAddr); err != nil {
		return commandRefusal(CodeInvalidArgument, "%s: %v", f.where, err)
	}
	return nil
}

// memberText reads a member id or data address: non-empty and at most MaxMemberTextBytes, because
// the TOPOLOGY reply carries both behind that bound and one longer row would fail it for every driver.
func (f *commandFields) memberText(key string) (string, error) {
	text, err := f.nonEmpty(key)
	if err != nil {
		return "", err
	}
	if len(text) > MaxMemberTextBytes {
		return "", commandRefusal(CodeInvalidArgument, "%s field %q is %d bytes and at most %d are kept (an address is a host name of at most %d characters, a colon and the port): use a shorter name", f.where, key, len(text), MaxMemberTextBytes, MaxMemberTextBytes-6)
	}
	return text, nil
}

func (f *commandFields) nonEmpty(key string) (string, error) {
	text, err := f.str(key)
	if err != nil {
		return "", err
	}
	if text == "" {
		return "", commandRefusal(CodeInvalidArgument, "%s field %q is empty: name the thing", f.where, key)
	}
	return text, nil
}

// clone copies every slice but the user table and the request list, so a command that fails half
// way leaves the original untouched. Users and UserRequests are shared: they are replaced whole by
// a user applier, which builds new slices and never writes into the shared ones (ring user store
// spec 4.1, F9), so a non-user command costs nothing per user.
func (s ClusterState) clone() ClusterState {
	next := s
	next.Voters = slices.Clone(s.Voters)
	next.Members = slices.Clone(s.Members)
	next.Databases = slices.Clone(s.Databases)
	next.SeedBitmaps = make([]SeedBitmap, len(s.SeedBitmaps))
	for i, b := range s.SeedBitmaps {
		next.SeedBitmaps[i] = SeedBitmap{Node: b.Node, DBID: b.DBID, Bits: slices.Clone(b.Bits)}
	}
	return next
}

// ringEntry is what the slot table sees of a ranked member: rank 1 is
// ranked but skipped as owner (down), 2 is an owner candidate (active,
// leaving). Members that are not ranked (joining, dead, witness) have no
// entry.
type ringEntry struct {
	id   string
	rank uint8
}

func ringProjection(st ClusterState) []ringEntry {
	var entries []ringEntry
	for _, m := range st.Members {
		switch {
		case m.Witness || !rankedState(m.State):
		case m.State == MemberDown:
			entries = append(entries, ringEntry{m.ID, 1})
		default:
			entries = append(entries, ringEntry{m.ID, 2})
		}
	}
	return entries
}

// ApplyCommand applies one committed command. It is a pure function of the
// state and the command: no clock, no I/O. A refused command changes
// nothing. An accepted one bumps state_seq by one, and epoch by one only when
// the ring projection (who is ranked, who is an owner candidate) changed;
// BOOTSTRAP sets epoch 1. A command whose base_seq is not state_seq is
// refused STATE_MOVED when it is stamped below state_seq, identically on every node, and a
// command stamped above it returns *Diverged (see Diverged): no leader that applied the log
// can have stamped it.
//
// It judges a cluster version against this build's range (BuildStateRange); a node that declares
// another range applies through applyCommandWith.
func (s *ClusterState) ApplyCommand(cmd Command) (Result, error) {
	return s.applyCommandWith(cmd, nil, BuildStateRange())
}

// applyCommandWith is ApplyCommand for a build that does not read the body fields hide names
// (nil: all are read) and that runs the cluster versions levels.Min to levels.Max. hide is the
// one seam the tests use to stand in for an older build in the same process; production passes
// nil. levels is the node's declared range: the cluster versions BOOTSTRAP and SET_CLUSTER_VERSION
// are judged against it, the one build-dependent input of an applier (cluster-version spec 4.3),
// passed in so the appliers still read only the state, the command and their arguments.
func (s *ClusterState) applyCommandWith(cmd Command, hide func(field string) bool, levels StateRange) (Result, error) {
	return s.applyCommandObserving(cmd, hide, nil, levels)
}

// applyCommandObserving is applyCommandWith that also reports every field a reader asks for to
// observe (nil: none). The state golden uses it to record the vocabulary; production passes nil.
func (s *ClusterState) applyCommandObserving(cmd Command, hide func(field string) bool, observe func(where, key string, want bsontype.Type), levels StateRange) (Result, error) {
	// Before the shape check: a stamp above this state proves a disagreement whatever else is
	// wrong with the command, and a version this build refuses must still be told apart.
	if cmd.BaseSeq > uint64(s.StateSeq) {
		return Result{}, &Diverged{BaseSeq: cmd.BaseSeq, StateSeq: s.StateSeq}
	}
	if err := checkCommandShape(cmd.Version, cmd.Kind, cmd.Body); err != nil {
		return Result{}, err
	}
	if cmd.BaseSeq != uint64(s.StateSeq) {
		return Result{}, stateMovedRefusal(s.StateSeq, cmd.BaseSeq)
	}
	fields, err := newCommandFieldsObserving("command body", cmd.Body, hide, observe)
	if err != nil {
		return Result{}, err
	}
	next := s.clone()
	before := ringProjection(next)
	if err := next.applyKind(cmd.Kind, fields, levels); err != nil {
		return Result{}, err
	}
	if err := fields.finish(); err != nil {
		return Result{}, err
	}
	next.StateSeq++
	moved := false
	switch {
	case cmd.Kind == CommandBootstrap:
		next.Epoch, moved = 1, true
	case !slices.Equal(before, ringProjection(next)):
		next.Epoch++
		moved = true
	}
	if moved {
		next.stampJoinedEpoch(before)
	}
	*s = next
	return Result{Epoch: next.Epoch, StateSeq: next.StateSeq, EpochMoved: moved}, nil
}

// stateMovedRefusal is the STATE_MOVED refusal of a command stamped at baseSeq against a state at
// stateSeq: nothing changed, and the proposer stamps it again.
func stateMovedRefusal(stateSeq int64, baseSeq uint64) error {
	return commandRefusal(CodeStateMoved, "%s", CodeStateMoved.Text(CondStateMoved, map[string]string{
		"n": strconv.FormatInt(stateSeq, 10), "m": strconv.FormatUint(baseSeq, 10),
	}))
}

// stampJoinedEpoch records the epoch at which a member first entered the
// ranking.
func (s *ClusterState) stampJoinedEpoch(before []ringEntry) {
	for _, e := range ringProjection(*s) {
		if slices.ContainsFunc(before, func(b ringEntry) bool { return b.id == e.id }) {
			continue
		}
		if i, ok := s.memberIndex(e.id); ok {
			s.Members[i].JoinedEpoch = s.Epoch
		}
	}
}

func (s *ClusterState) applyKind(kind byte, f *commandFields, levels StateRange) error {
	switch kind {
	case CommandBootstrap:
		return s.applyBootstrap(f, levels)
	case CommandSetClusterVersion:
		return s.applySetClusterVersion(f, levels)
	case CommandSetMemberState:
		return s.applySetMemberState(f)
	case CommandRegisterMember:
		return s.applyRegisterMember(f)
	case CommandRemoveMember:
		return s.applyRemoveMember(f)
	case CommandCreateDatabase:
		return s.applyCreateDatabase(f)
	case CommandAlterDatabase:
		return s.applyAlterDatabase(f)
	case CommandDropDatabase:
		return s.applyDropDatabase(f)
	case CommandSetSetting:
		return s.applySetSetting(f)
	case CommandSeeded:
		return s.applySeeded(f, true)
	case CommandRetire:
		return s.applySeeded(f, false)
	case CommandBumpSchema:
		return s.applyBumpSchema(f)
	case CommandUserBootstrap:
		return s.applyUserBootstrap(f)
	case CommandUserCreate:
		return s.applyUserCreate(f)
	case CommandUserAlter:
		return s.applyUserAlter(f)
	case CommandUserDrop:
		return s.applyUserDrop(f)
	case CommandUserGrant:
		return s.applyUserRole(f, true)
	case CommandUserRevoke:
		return s.applyUserRole(f, false)
	default:
		return s.applySetVoters(f)
	}
}

func (s *ClusterState) memberIndex(id string) (int, bool) {
	return slices.BinarySearchFunc(s.Members, id, func(m Member, id string) int { return cmp.Compare(m.ID, id) })
}

func (s *ClusterState) databaseByID(id int64) (int, bool) {
	i := slices.IndexFunc(s.Databases, func(d Database) bool { return d.ID == id })
	return i, i >= 0
}

func unknownMember(id string) error {
	return commandRefusal(CodeNotFound, "member %q is not in the cluster. Run 'nodes' to list the members, or 'join %s@<host:port>' to add it", id, id)
}

func unknownDatabaseID(id int64) error {
	return commandRefusal(CodeNotFound, "database id %d is not in the cluster. Run 'show databases' to list what exists", id)
}

func conflict(format string, args ...any) error {
	return commandRefusal(CodeConflict, format, args...)
}

// defaultSettings are the committed timing defaults of conventions section 11.
var defaultSettings = Settings{
	StaleReads: false, HeartbeatMs: 100, StepDownMs: 2000, FailoverMs: 5000,
	DeadMs: 600000, ProbeIntervalMs: 1000, ProbeTimeoutMs: 500, ProbeFailures: 3,
}

func (s *ClusterState) applyBootstrap(f *commandFields, levels StateRange) error {
	idBytes, err := f.binary("cluster_id", len(s.ClusterID))
	if err != nil {
		return err
	}
	version, err := f.levelField("cluster_version")
	if err != nil {
		return err
	}
	self, err := f.document("self")
	if err != nil {
		return err
	}
	id, err := self.memberText("id")
	if err != nil {
		return err
	}
	incarnation, err := self.int64("incarnation")
	if err != nil {
		return err
	}
	addr, err := self.memberText("data_addr")
	if err != nil {
		return err
	}
	clientAddr, err := self.optClientAddr()
	if err != nil {
		return err
	}
	if err := self.checkClientAddr(clientAddr, addr); err != nil {
		return err
	}
	witness, err := self.optBool("witness")
	if err != nil {
		return err
	}
	if err := self.finish(); err != nil {
		return err
	}
	if len(s.Members) != 0 || s.StateSeq != 0 {
		return commandRefusal(CodeClusterMismatch, "this cluster is already bootstrapped (state_seq %d): a second bootstrap would mint a second cluster. Start the node with 'join <node>@<addr of a node in this cluster>' instead", s.StateSeq)
	}
	// The version comes from the body, never from the build: every node that replays this entry
	// must reach the same state, whatever it runs. A node that cannot run it stops (check 3 of
	// cluster-version spec 4.4). Only the upper bound: the version only rises, so a BOOTSTRAP below
	// this build's Min is history a later SET_CLUSTER_VERSION has moved past (a joiner replaying
	// the log, a restart replaying its own); the lower bound is judged only by a holder of the
	// committed version (cluster-version spec 4.4), never on what this node replays.
	if version > levels.Max {
		return &ClusterVersionError{Version: version, Range: levels, Bootstrapped: true}
	}
	s.ClusterVersion = version
	copy(s.ClusterID[:], idBytes)
	s.NextDatabaseID = 1
	s.Settings = defaultSettings
	// BOOTSTRAP is the one command besides SET_VOTERS that lists a voter:
	// the library is bootstrapped with exactly this one, and the 490-byte
	// minimal state of membership Formats 2 carries it. The row is registered by this commit, whose
	// state_seq is the one after the apply.
	s.Voters = []string{id}
	s.Members = []Member{{ID: id, Incarnation: incarnation, DataAddr: addr, ClientAddr: clientAddr, State: MemberActive, Suffrage: "voter", Witness: witness, JoinedStateSeq: s.StateSeq + 1}}
	return nil
}

func (s *ClusterState) clearSeedBits(node string) {
	s.SeedBitmaps = slices.DeleteFunc(s.SeedBitmaps, func(b SeedBitmap) bool { return b.Node == node })
}

func (s *ClusterState) applySetMemberState(f *commandFields) error {
	id, err := f.nonEmpty("id")
	if err != nil {
		return err
	}
	word, err := f.str("state")
	if err != nil {
		return err
	}
	incarnation, hasIncarnation, err := f.optInt64("incarnation")
	if err != nil {
		return err
	}
	clearBits, err := f.optBool("clear_bits")
	if err != nil {
		return err
	}
	to, err := decodeStateWord(word)
	if err != nil {
		return err
	}
	i, ok := s.memberIndex(id)
	if !ok {
		return unknownMember(id)
	}
	m := &s.Members[i]
	// expect_state makes the command a compare-and-set: a replay of it after the member moved
	// on (a replace, a later activation) finds another state and changes nothing, so a retried
	// proposal cannot undo a newer one or clear the seed bits of a re-seed.
	if _, has := f.vals["expect_state"]; has {
		want, err := f.str("expect_state")
		if err != nil {
			return err
		}
		if want != m.State.String() {
			return conflict("node %q is %s now, not the %s this change was built on, so it was not applied. Run 'nodes' to see its state and run the change again if it is still wanted", id, m.State, want)
		}
	}
	if hasIncarnation && incarnation != m.Incarnation {
		if incarnation < m.Incarnation {
			return conflict("node %q is at incarnation %d and this command carries the older %d, so it is a stale replace. Run 'nodes' and replace from the incarnation it prints", id, m.Incarnation, incarnation)
		}
		if (m.State != MemberDown && m.State != MemberDead) || to != MemberJoining {
			return commandRefusal(CodeConflict, "%s", CodeConflict.Text(CondConflict, map[string]string{
				"node": id, "state": m.State.String(), "e": strconv.FormatInt(s.Epoch, 10), "addr": "<host:port>",
			}))
		}
		m.Incarnation = incarnation
		m.JoinedStateSeq = s.StateSeq + 1
	}
	m.State = to
	if clearBits {
		s.clearSeedBits(id)
	}
	return nil
}

func (s *ClusterState) applyRegisterMember(f *commandFields) error {
	id, err := f.memberText("id")
	if err != nil {
		return err
	}
	incarnation, err := f.int64("incarnation")
	if err != nil {
		return err
	}
	addr, err := f.memberText("data_addr")
	if err != nil {
		return err
	}
	clientAddr, err := f.optClientAddr()
	if err != nil {
		return err
	}
	if err := f.checkClientAddr(clientAddr, addr); err != nil {
		return err
	}
	witness, err := f.optBool("witness")
	if err != nil {
		return err
	}
	// The placing set is a snapshot the joiner pulls from; ClusterState
	// holds no field for it, so it is validated and not stored.
	if _, err := f.binary("placing_set", seedBitmapBytes); err != nil {
		return err
	}
	// The joiner's range, as its identity reply said it: a join judged at one cluster version can
	// commit after a finalize moved it, and a member that cannot run the version it is registered
	// at would never activate and would block every later finalize (cluster-version spec 4.2).
	minLevel, err := f.levelField("min_level")
	if err != nil {
		return err
	}
	maxLevel, err := f.levelField("max_level")
	if err != nil {
		return err
	}
	if incarnation < 1 {
		return commandRefusal(CodeInvalidArgument, "incarnation %d is not an incarnation: the first one is 1", incarnation)
	}
	if minLevel > maxLevel {
		return commandRefusal(CodeInvalidArgument, "%s has min_level %d above max_level %d: a range runs from its lowest level to its highest", f.where, minLevel, maxLevel)
	}
	if s.ClusterVersion < minLevel || s.ClusterVersion > maxLevel {
		return joinVersionRefused(id, addr, s.ClusterVersion, minLevel, maxLevel)
	}
	for _, held := range s.Members {
		if held.DataAddr == addr && held.ID != id {
			return commandRefusal(CodeConflict, "member %q already lives at %s, so %q cannot register there: two ids on one address would be dialled with one key. Check the address of the join command", held.ID, addr, id)
		}
	}
	i, exists := s.memberIndex(id)
	if !exists {
		fresh := Member{ID: id, Incarnation: incarnation, DataAddr: addr, ClientAddr: clientAddr, State: MemberJoining, Suffrage: "nonvoter", Witness: witness, JoinedStateSeq: s.StateSeq + 1}
		s.Members = slices.Insert(s.Members, i, fresh)
		return nil
	}
	m := &s.Members[i]
	switch {
	case incarnation == m.Incarnation:
		m.DataAddr, m.ClientAddr, m.Witness = addr, clientAddr, witness
	case incarnation > m.Incarnation && m.State == MemberDead:
		// A return after stage 2: a reused identity holds nothing.
		m.Incarnation, m.DataAddr, m.ClientAddr, m.Witness, m.State = incarnation, addr, clientAddr, witness, MemberJoining
		m.JoinedStateSeq = s.StateSeq + 1
		s.clearSeedBits(id)
	default:
		return commandRefusal(CodeConflict, "%s", CodeConflict.Text(CondConflict, map[string]string{
			"node": id, "state": m.State.String(), "e": strconv.FormatInt(s.Epoch, 10), "addr": addr,
		}))
	}
	return nil
}

// levelField reads a state level carried as an int32: 1 to 255.
func (f *commandFields) levelField(key string) (uint8, error) {
	level, err := f.int32(key)
	if err != nil {
		return 0, err
	}
	if level < 1 || level > 255 {
		return 0, commandRefusal(CodeInvalidArgument, "%s field %q is %d, and a state level is 1 to 255", f.where, key, level)
	}
	return uint8(level), nil
}

// joinVersionRefused is text F8 of cluster-version spec 7.4: a join judged at one cluster version
// whose registration met another, because a finalize committed between the identity ask and the
// registration. Nothing is registered.
func joinVersionRefused(id, addr string, version, minLevel, maxLevel uint8) error {
	upgrade := fmt.Sprintf("a build whose state_range includes %d", version)
	if release := ReleaseOf(version); release != "" {
		upgrade = release + " or later"
	}
	return commandRefusal(CodeClusterMismatch, "cannot join %s@%s: the cluster moved to version %d while the join was checked, and %s runs versions %d to %d. Upgrade %s to %s, then run 'join %s@%s' again. Nothing was registered.",
		id, addr, version, id, minLevel, maxLevel, id, upgrade, id, addr)
}

// applySetClusterVersion is SET_CLUSTER_VERSION (cluster-version spec 4.3). Refusals 1, 2, 4 and
// 5 read only the state and the command, and the state an entry meets at apply is the state its
// dry run met (a stamp that is not state_seq is STATE_MOVED before any of them runs), so they
// fire only in a dry run and never stop a node. Refusal 3 reads levels, the node's own range, and
// is the one meant to fire at apply: a node that cannot run the new version stops.
func (s *ClusterState) applySetClusterVersion(f *commandFields, levels StateRange) error {
	from, err := f.levelField("from")
	if err != nil {
		return err
	}
	to, err := f.levelField("to")
	if err != nil {
		return err
	}
	rows, err := f.documents("members")
	if err != nil {
		return err
	}
	listed := make([]Member, 0, len(rows))
	for _, row := range rows {
		id, err := row.nonEmpty("id")
		if err != nil {
			return err
		}
		incarnation, err := row.int64("incarnation")
		if err != nil {
			return err
		}
		if err := row.finish(); err != nil {
			return err
		}
		if n := len(listed); n > 0 && id <= listed[n-1].ID {
			return commandRefusal(CodeInvalidArgument, "%s lists node %q after %q: members are sorted by id and each listed once", f.where, id, listed[n-1].ID)
		}
		listed = append(listed, Member{ID: id, Incarnation: incarnation})
	}
	switch {
	case from != s.ClusterVersion:
		return conflict("the cluster runs version %d, not %d", s.ClusterVersion, from)
	case to <= from:
		return commandRefusal(CodeInvalidArgument, "finalize moves the cluster to a higher version, and %d is not above %d", to, from)
	case to > levels.Max:
		// Typed, so a node that stops on it reads 4.4's text (finalized past this build, keep the
		// directory, exit 70) and not the generic divergence text, which names the proposer's build.
		return &ClusterVersionError{Version: to, Range: levels}
	}
	for _, m := range s.Members {
		if m.State == MemberJoining || m.State == MemberLeaving {
			return conflict("node %s is %s", m.ID, m.State)
		}
	}
	if changed := membersChanged(s.Members, listed); changed != "" {
		return conflict("the members changed since finalize checked them (%s)", changed)
	}
	s.ClusterVersion = to
	return nil
}

// membersChanged names the first difference between the committed members and the ones a
// finalize listed, by id and incarnation, or "" when they are the same. Both are sorted by id.
func membersChanged(committed, listed []Member) string {
	for i := 0; i < len(committed) || i < len(listed); i++ {
		switch {
		case i >= len(listed) || (i < len(committed) && committed[i].ID < listed[i].ID):
			return fmt.Sprintf("node %s is a member and was not checked", committed[i].ID)
		case i >= len(committed) || listed[i].ID < committed[i].ID:
			return fmt.Sprintf("node %s was checked and is not a member", listed[i].ID)
		case committed[i].Incarnation != listed[i].Incarnation:
			return fmt.Sprintf("node %s is at incarnation %d, not the %d that was checked", committed[i].ID, committed[i].Incarnation, listed[i].Incarnation)
		}
	}
	return ""
}

func (s *ClusterState) applyRemoveMember(f *commandFields) error {
	id, err := f.nonEmpty("id")
	if err != nil {
		return err
	}
	i, ok := s.memberIndex(id)
	if !ok {
		return unknownMember(id)
	}
	s.Members = slices.Delete(s.Members, i, i+1)
	s.clearSeedBits(id)
	return nil
}

// MemoryRFRefusal is the create-time and alter-time refusal of a memory database with more than one copy (memory spec section 2.1). M2-2 deletes it.
const MemoryRFRefusal = "`storage: memory` keeps one copy in this build. Create it with `{ replication_factor: 1, storage: memory }`, or create it on disk with the copies you want."

// MemoryStorageRefusal is the refusal of a create database with `storage: memory` while the in-memory
// store is not built: the database would be kept on disk and so would not be lost on restart as its
// name says. Servers, the ring client and the CLI refuse with this one text; the state machine still
// applies the storage byte of a row already written, so a cluster that holds one keeps opening.
func MemoryStorageRefusal(database string) string {
	return "storage: memory is not available yet: this build keeps every database on disk, so a memory database would not be lost on restart as its name says. Create it with storage: disk (the default): create database " + database
}

func (s *ClusterState) applyCreateDatabase(f *commandFields) error {
	name, err := f.nonEmpty("name")
	if err != nil {
		return err
	}
	id, err := f.int64("id")
	if err != nil {
		return err
	}
	rf, err := f.int32("rf")
	if err != nil {
		return err
	}
	version, err := f.int64("schema_version")
	if err != nil {
		return err
	}
	word, hasStorage, err := f.optStr("storage")
	if err != nil {
		return err
	}
	storage := StorageDisk
	if hasStorage {
		if storage, err = ParseStorageWord(word); err != nil {
			return err
		}
	}
	if rf < 1 {
		return commandRefusal(CodeInvalidArgument, "replication factor %d is not a copy count: use 1 or more", rf)
	}
	if err := databaseNamePolicy(name); err != nil {
		return err
	}
	if storage == StorageMemory && rf > 1 {
		return commandRefusal(CodeInvalidArgument, "%s", MemoryRFRefusal)
	}
	// RF above 1 needs at least 3 voters (decisions section 4): two copies across two
	// voters lose quorum when either stops, so the factor would claim copies the cluster
	// cannot keep. The data-member bound below is the other half of the same rule.
	if rf > 1 && len(s.Voters) < 3 {
		return commandRefusal(CodeRFUnsupported, "%s", CodeRFUnsupported.Text(CondRFUnsupportedCreateVoters, map[string]string{
			"rf": strconv.Itoa(int(rf)), "v": strconv.Itoa(len(s.Voters)), "db": name,
		}))
	}
	if data := len(ringProjection(*s)); int(rf) > data {
		return commandRefusal(CodeRFUnsupported, "%s", CodeRFUnsupported.Text(CondRFUnsupportedCreateData, map[string]string{
			"rf": strconv.Itoa(int(rf)), "d": strconv.Itoa(data), "db": name,
		}))
	}
	if version != 1 {
		return commandRefusal(CodeInvalidArgument, "a new database starts at schema_version 1, not %d", version)
	}
	if id != s.NextDatabaseID {
		return conflict("database id %d is not the next free id %d: another database was created first. Re-run 'create database %s' to take the next id", id, s.NextDatabaseID, name)
	}
	if slices.ContainsFunc(s.Databases, func(d Database) bool { return d.Name == name }) {
		return conflict("database %q already exists. Run 'show databases' to list what exists, or pick another name", name)
	}
	at, _ := slices.BinarySearchFunc(s.Databases, name, func(d Database, name string) int { return cmp.Compare(d.Name, name) })
	s.Databases = slices.Insert(s.Databases, at, Database{Name: name, ID: id, RF: rf, SchemaVersion: version, Storage: storage})
	s.NextDatabaseID++
	return nil
}

// echoClipped quotes at most 64 bytes of a name a refusal repeats, so the text does not grow
// with what a caller sent.
func echoClipped(name string) string {
	if len(name) <= 64 {
		return strconv.Quote(name)
	}
	return strconv.Quote(name[:64]) + "…"
}

// databaseNamePolicy applies the rules of package namepolicy that do not depend on the node's
// configuration: reserved names, length and path safety. The storage-directory rule needs the
// node's data layout, which a pure state machine does not have, so the callers add it.
func databaseNamePolicy(name string) error {
	violation := namepolicy.CheckDatabase(name, nil)
	switch {
	case violation == nil:
		return nil
	case violation.Rule == namepolicy.RuleReserved:
		return commandRefusal(CodeReservedName, "database name %s is reserved for internal use (the auth master key lives there). Run 'create database <name>' with another name", echoClipped(name))
	case violation.Rule == namepolicy.RuleTooLong:
		return commandRefusal(CodeInvalidArgument, "database name %s is %d bytes and the limit is %d. Run 'create database <name>' with a shorter name", echoClipped(name), len(name), namepolicy.MaxBytes)
	}
	return commandRefusal(CodeInvalidArgument, "invalid database name %s: it must be 1 to %d bytes, with no '/', '\\' or NUL, and not '.' or '..'. Run 'create database <name>' with another name", echoClipped(name), namepolicy.MaxBytes)
}

func (s *ClusterState) applyAlterDatabase(f *commandFields) error {
	id, err := f.int64("id")
	if err != nil {
		return err
	}
	rf, err := f.int32("rf")
	if err != nil {
		return err
	}
	if rf < 1 {
		return commandRefusal(CodeInvalidArgument, "replication factor %d is not a copy count: use 1 or more", rf)
	}
	i, ok := s.databaseByID(id)
	if !ok {
		return unknownDatabaseID(id)
	}
	if s.Databases[i].Storage == StorageMemory && rf > 1 {
		return commandRefusal(CodeInvalidArgument, "%s", MemoryRFRefusal)
	}
	s.Databases[i].RF = rf
	return nil
}

func schemaNotCurrent(db Database, got int64) error {
	return commandRefusal(CodeSchemaNotCurrent, "database %q is at schema_version %d and this command was built on %d. Nothing was changed. Re-run it", db.Name, db.SchemaVersion, got)
}

func (s *ClusterState) applyDropDatabase(f *commandFields) error {
	id, err := f.int64("id")
	if err != nil {
		return err
	}
	version, err := f.int64("schema_version")
	if err != nil {
		return err
	}
	i, ok := s.databaseByID(id)
	if !ok {
		return unknownDatabaseID(id)
	}
	if s.Databases[i].SchemaVersion != version {
		return schemaNotCurrent(s.Databases[i], version)
	}
	s.Databases = slices.Delete(s.Databases, i, i+1)
	s.SeedBitmaps = slices.DeleteFunc(s.SeedBitmaps, func(b SeedBitmap) bool { return b.DBID == id })
	return nil
}

func (s *ClusterState) applySetSetting(f *commandFields) error {
	key, err := f.str("key")
	if err != nil {
		return err
	}
	if key == "stale_reads" {
		on, err := f.boolean("value")
		if err != nil {
			return err
		}
		if err := expectSetting(f, key, s.Settings.StaleReads, f.boolean); err != nil {
			return err
		}
		s.Settings.StaleReads = on
		return nil
	}
	if key == "probe_failures" {
		count, err := f.int32("value")
		if err != nil {
			return err
		}
		if count < 1 {
			return commandRefusal(CodeInvalidArgument, "probe_failures %d is not a count: use 1 or more", count)
		}
		if err := expectSetting(f, key, s.Settings.ProbeFailures, f.int32); err != nil {
			return err
		}
		s.Settings.ProbeFailures = count
		return nil
	}
	target := map[string]*int64{
		"heartbeat_ms": &s.Settings.HeartbeatMs, "step_down_ms": &s.Settings.StepDownMs,
		"failover_ms": &s.Settings.FailoverMs, "dead_ms": &s.Settings.DeadMs,
		"probe_interval_ms": &s.Settings.ProbeIntervalMs, "probe_timeout_ms": &s.Settings.ProbeTimeoutMs,
	}[key]
	if target == nil {
		return commandRefusal(CodeInvalidArgument, "setting %s is not a cluster setting: use stale_reads, heartbeat_ms, step_down_ms, failover_ms, dead_ms, probe_interval_ms, probe_timeout_ms or probe_failures", echoClipped(key))
	}
	millis, err := f.int64("value")
	if err != nil {
		return err
	}
	if millis < 1 {
		return commandRefusal(CodeInvalidArgument, "%s %d is not a duration: use 1 or more milliseconds", key, millis)
	}
	if err := expectSetting(f, key, *target, f.int64); err != nil {
		return err
	}
	*target = millis
	return nil
}

// expectSetting makes SET_SETTING a compare-and-set when the body carries expect_value: the
// change applies only while the setting still holds the value the sender saw, so a retried
// proposal cannot overwrite a newer change. read reads expect_value with the setting's type.
func expectSetting[T comparable](f *commandFields, key string, current T, read func(string) (T, error)) error {
	if _, has := f.vals["expect_value"]; !has {
		return nil
	}
	want, err := read("expect_value")
	if err != nil {
		return err
	}
	if want != current {
		return conflict("setting %s is %v now, not the %v this change was built on, so it was not applied. Read the setting again and run the change again if it is still wanted", echoClipped(key), current, want)
	}
	return nil
}

// applySeeded ORs the bits into the (node, database) bitmap when set is true
// and clears them when it is false. A lost, duplicated or replayed commit
// converges either way.
func (s *ClusterState) applySeeded(f *commandFields, set bool) error {
	node, err := f.nonEmpty("node")
	if err != nil {
		return err
	}
	dbID, err := f.int64("db_id")
	if err != nil {
		return err
	}
	bits, err := f.binary("bits", seedBitmapBytes)
	if err != nil {
		return err
	}
	if _, ok := s.memberIndex(node); !ok {
		return unknownMember(node)
	}
	if _, ok := s.databaseByID(dbID); !ok {
		return unknownDatabaseID(dbID)
	}
	at, found := slices.BinarySearchFunc(s.SeedBitmaps, SeedBitmap{Node: node, DBID: dbID}, compareBitmaps)
	if !found {
		if !set {
			return nil
		}
		s.SeedBitmaps = slices.Insert(s.SeedBitmaps, at, SeedBitmap{Node: node, DBID: dbID, Bits: make([]byte, seedBitmapBytes)})
	}
	row := s.SeedBitmaps[at].Bits
	for i, b := range bits {
		if set {
			row[i] |= b
		} else {
			row[i] &^= b
		}
	}
	return nil
}

func (s *ClusterState) applyBumpSchema(f *commandFields) error {
	dbID, err := f.int64("db_id")
	if err != nil {
		return err
	}
	from, err := f.int64("from")
	if err != nil {
		return err
	}
	to, err := f.int64("to")
	if err != nil {
		return err
	}
	i, ok := s.databaseByID(dbID)
	if !ok {
		return unknownDatabaseID(dbID)
	}
	if s.Databases[i].SchemaVersion != from {
		return schemaNotCurrent(s.Databases[i], from)
	}
	if to <= from {
		return commandRefusal(CodeInvalidArgument, "schema_version %d does not move past %d: a bump goes forward", to, from)
	}
	if _, _, err := readBumpNote(f); err != nil {
		return err
	}
	s.Databases[i].SchemaVersion = to
	return nil
}

// readBumpNote reads the optional note of a BUMP_SCHEMA and refuses a half-formed one: a
// bucket with neither a drop nor retired ids, both at once, a retired list that is not
// ascending ids of 2 or more, or members with no bucket. The same body must always get the
// same answer on every node.
func readBumpNote(f *commandFields) (BumpNote, bool, error) {
	id, present, err := f.optInt64("bucket_id")
	if err != nil {
		return BumpNote{}, false, err
	}
	if !present {
		for _, key := range []string{"drop", "retired", "epoch"} {
			if _, stray := f.vals[key]; stray {
				return BumpNote{}, false, commandRefusal(CodeInvalidArgument, "%s carries %q without a bucket_id: name the bucket the bump is about", f.where, key)
			}
		}
		return BumpNote{}, false, nil
	}
	note := BumpNote{BucketID: uint64(id)}
	if note.BucketID == 0 {
		return BumpNote{}, false, commandRefusal(CodeInvalidArgument, "%s names bucket id 0, which no bucket has", f.where)
	}
	if note.Drop, err = f.optBool("drop"); err != nil {
		return BumpNote{}, false, err
	}
	raw, hasRetired, err := f.find("retired", bson.TypeBinary)
	if err != nil {
		return BumpNote{}, false, err
	}
	if hasRetired {
		if note.Retired, err = unpackRetired(f.where, raw.Data); err != nil {
			return BumpNote{}, false, err
		}
		epoch, err := f.int64("epoch")
		if err != nil {
			return BumpNote{}, false, err
		}
		note.Epoch = uint64(epoch)
	}
	if note.Drop == hasRetired {
		return BumpNote{}, false, commandRefusal(CodeInvalidArgument, "%s names bucket %016x with drop %v and %d retired ids; a bump either drops the bucket or retires fields, one of the two", f.where, note.BucketID, note.Drop, len(note.Retired))
	}
	return note, true, nil
}

// unpackRetired reads a generic binary of big-endian field ids, ascending and each 2 or more
// (id 1 is _id_ and lives in the header, never in a field key).
func unpackRetired(where string, data []byte) ([]uint16, error) {
	if len(data) < 5 || data[4] != 0 {
		return nil, commandRefusal(CodeInvalidArgument, "%s field \"retired\" must be generic binary", where)
	}
	packed := data[5:]
	if len(packed) == 0 || len(packed)%2 != 0 || len(packed)/2 > MaxBumpRetired {
		return nil, commandRefusal(CodeInvalidArgument, "%s field \"retired\" holds %d bytes; send 1 to %d field ids of 2 bytes each", where, len(packed), MaxBumpRetired)
	}
	ids := make([]uint16, 0, len(packed)/2)
	for i := 0; i < len(packed); i += 2 {
		id := uint16(packed[i])<<8 | uint16(packed[i+1])
		if id < 2 || (len(ids) > 0 && id <= ids[len(ids)-1]) {
			return nil, commandRefusal(CodeInvalidArgument, "%s field \"retired\" must list field ids of 2 or more, ascending and each once", where)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// applySetVoters is the one writer of Voters after the bootstrap. Adding a
// voter needs its member row; removing one does not, because the leader
// proposes the removal after the library's RemoveServer, which follows the
// member's own removal in a leave.
func (s *ClusterState) applySetVoters(f *commandFields) error {
	id, err := f.nonEmpty("id")
	if err != nil {
		return err
	}
	voting, err := f.boolean("voting")
	if err != nil {
		return err
	}
	i, isMember := s.memberIndex(id)
	if voting && !isMember {
		return unknownMember(id)
	}
	at, listed := slices.BinarySearch(s.Voters, id)
	switch {
	case voting && !listed:
		s.Voters = slices.Insert(s.Voters, at, id)
	case !voting && listed:
		s.Voters = slices.Delete(s.Voters, at, at+1)
	}
	if isMember {
		s.Members[i].Suffrage = map[bool]string{true: "voter", false: "nonvoter"}[voting]
	}
	return nil
}
