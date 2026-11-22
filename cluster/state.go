package cluster

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// stateVersion is the "v" field every ClusterState document carries.
const stateVersion = 1

// seedBitmapBytes is the body of one SeedBitmap: 8192 bits. It repeats
// machine.BitmapBytes because cluster/machine imports this package.
const seedBitmapBytes = 1024

// MaxClientAddrBytes bounds Member.ClientAddr: the TOPOLOGY members blob carries it behind a
// length, and a row longer than the blob takes would make every TOPOLOGY reply refused.
const MaxClientAddrBytes = 256

// MaxMemberTextBytes bounds a member id and data address, the same 256 bytes the TOPOLOGY members blob
// reads them with: a longer committed row would make every TOPOLOGY reply unencodable.
const MaxMemberTextBytes = 256

// ClusterState is the one replicated document of the cluster: who is a
// member, which databases exist, the settings and the user table. It holds no
// bucket declaration (conventions section 13): a declaration is a document in
// the ring, so this state does not grow with the number of buckets.
type ClusterState struct {
	// ClusterVersion is the committed cluster version: the level of the vocabulary every member
	// writes to the others. BOOTSTRAP sets it to the bootstrapping build's highest level and only
	// SET_CLUSTER_VERSION moves it (cluster-version spec 4.2). It is 0 only before the bootstrap.
	ClusterVersion uint8
	// ClusterID is minted by the first bootstrap.
	ClusterID [16]byte
	Epoch     int64
	StateSeq  int64
	// NextDatabaseID is the committed allocator; database ids are never reused.
	NextDatabaseID int64
	// Voters is written only by SET_VOTERS.
	Voters      []string
	Members     []Member
	Databases   []Database
	SeedBitmaps []SeedBitmap
	Settings    Settings
	// Users is the user table, sorted by name (ring user store spec 4.1). clone shares it: only a
	// user command replaces it, with a new slice, so a non-user command never copies it.
	Users []UserRow
	// PepperKID names the cluster pepper every row's hash was made with: empty until
	// USER_BOOTSTRAP sets it from its row, never changed after.
	PepperKID string
	// UserDropSeq is the state_seq of the last committed USER_DROP, 0 before any. Only USER_DROP
	// moves it, so it only rises; USER_CREATE carries the value its proposer read and is refused
	// when a drop committed since, so a retried create never re-creates a dropped account (owner
	// ruling 2026-11-20, review R3).
	UserDropSeq int64
	// UserRequests are the request ids of the last authrules.RequestListSize applied user
	// commands, oldest first, each with the state_seq that applied it. Shared by clone like Users.
	UserRequests []UserRequest
}

// Member is one row of ClusterState.members.
type Member struct {
	ID          string
	Incarnation int64
	DataAddr    string
	// ClientAddr is where drivers connect to this member; empty means DataAddr.
	ClientAddr string
	State      MemberState
	// Suffrage mirrors the Raft configuration ("voter" or "nonvoter"); RING_TOPOLOGY reads it
	// as the voter flag, so SET_VOTERS and BOOTSTRAP must keep it true.
	Suffrage    string
	Witness     bool
	JoinedEpoch int64
	// JoinedStateSeq is the state_seq of the commit that registered this identity (REGISTER_MEMBER,
	// BOOTSTRAP, or the joining commit of a replace). A join retried after a lost reply is answered
	// from it, so the same position comes back across a leader change.
	JoinedStateSeq int64
}

// DriverAddr is the address a driver connects to: the client address the member published, or
// empty when it published none. It never falls back to the data address, which is the internode
// listener and does not speak the driver protocol; a caller skips a member with no driver address.
func (m Member) DriverAddr() string { return m.ClientAddr }

// Database is one row of ClusterState.databases.
type Database struct {
	Name          string
	ID            int64
	RF            int32
	SchemaVersion int64
	Storage       byte // StorageDisk or StorageMemory; fixed at create
}

const (
	// StorageDisk is the default: the database lives in the durable store behind the fsynced log.
	StorageDisk byte = 0
	// StorageMemory keeps the database's documents in the node's volatile store (memory spec section 2.2).
	StorageMemory byte = 1
	// StorageWordDisk is the word a developer writes and the wire carries for StorageDisk.
	StorageWordDisk = "disk"
	// StorageWordMemory is the word a developer writes and the wire carries for StorageMemory.
	StorageWordMemory = "memory"
)

// StorageWord is "disk" or "memory".
func (d Database) StorageWord() string {
	if d.Storage == StorageMemory {
		return StorageWordMemory
	}
	return StorageWordDisk
}

// storageWord pairs a storage word with its byte.
type storageWord struct {
	word    string
	storage byte
}

// storageWordTable is every word ParseStorageWord accepts. It is the only place a word is added,
// and the vocabulary golden records it, so a new word cannot ship without a StateLevel bump.
func storageWordTable() []storageWord {
	return []storageWord{{StorageWordDisk, StorageDisk}, {StorageWordMemory, StorageMemory}}
}

// ParseStorageWord returns the byte for "disk" or "memory"; any other word is an INVALID_ARGUMENT error.
func ParseStorageWord(word string) (byte, error) {
	for _, accepted := range storageWordTable() {
		if accepted.word == word {
			return accepted.storage, nil
		}
	}
	return 0, &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf("storage %q is not a kind. Use storage: disk or storage: memory", word)}
}

// SeedBitmap is one (node, database) row of ClusterState.seed_bitmaps. Bit s
// of Bits means "(s, db) is ready"; Bits is always 1024 bytes.
type SeedBitmap struct {
	Node string
	DBID int64
	Bits []byte
}

// Settings is the committed timing configuration, named and measured like the
// cluster.* configuration keys.
type Settings struct {
	StaleReads      bool
	HeartbeatMs     int64
	StepDownMs      int64
	FailoverMs      int64
	DeadMs          int64
	ProbeIntervalMs int64
	ProbeTimeoutMs  int64
	ProbeFailures   int32
}

func stateRefusal(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

// buildDifference marks a refusal that says the bytes were written by another build and not
// damaged: a field, a version or a value this build does not know. phrase finishes the sentence
// "the snapshot ..." in text 2 of the mixed-build spec.
type buildDifference struct {
	phrase string
	err    error
}

func (e *buildDifference) Error() string { return e.err.Error() }
func (e *buildDifference) Unwrap() error { return e.err }

// differs wraps a refusal as a build difference.
func differs(err error, phraseFormat string, args ...any) error {
	return &buildDifference{phrase: fmt.Sprintf(phraseFormat, args...), err: err}
}

func compareBitmaps(a, b SeedBitmap) int {
	if c := cmp.Compare(a.Node, b.Node); c != 0 {
		return c
	}
	return cmp.Compare(a.DBID, b.DBID)
}

// EncodeClusterState writes the canonical document: one fixed field order,
// every array sorted (members and voters by id, databases by name, bitmaps by
// node then database id), so one logical state is one byte string. It refuses
// a duplicate row, a member state outside the five, and a bitmap that is not
// 1024 bytes, with INVALID_ARGUMENT.
func EncodeClusterState(st ClusterState) ([]byte, error) {
	// The decoder of every build refuses 0, so a state no BOOTSTRAP has set is not written: a
	// snapshot of it would stop every node that restores it.
	if st.ClusterVersion == 0 {
		return nil, stateRefusal("cluster state field \"cluster_version\" is 0: a state is written only after BOOTSTRAP has set its cluster version (1 to 255)")
	}
	sorted := st
	sorted.Voters = slices.Clone(st.Voters)
	sorted.Members = slices.Clone(st.Members)
	sorted.Databases = slices.Clone(st.Databases)
	sorted.SeedBitmaps = slices.Clone(st.SeedBitmaps)
	slices.Sort(sorted.Voters)
	slices.SortFunc(sorted.Members, func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(sorted.Databases, func(a, b Database) int { return cmp.Compare(a.Name, b.Name) })
	slices.SortFunc(sorted.SeedBitmaps, compareBitmaps)

	for i := 1; i < len(sorted.Voters); i++ {
		if sorted.Voters[i] == sorted.Voters[i-1] {
			return nil, stateRefusal("voter %q appears twice in the cluster state: list each voter once", sorted.Voters[i])
		}
	}
	for i, m := range sorted.Members {
		if i > 0 && m.ID == sorted.Members[i-1].ID {
			return nil, stateRefusal("member %q appears twice in the cluster state: a member id is unique", m.ID)
		}
		if _, err := DecodeMemberState(byte(m.State)); err != nil {
			return nil, fmt.Errorf("member %q: %w", m.ID, err)
		}
		if err := checkMemberRow(m); err != nil {
			return nil, err
		}
	}
	for i := 1; i < len(sorted.Databases); i++ {
		if sorted.Databases[i].Name == sorted.Databases[i-1].Name {
			return nil, stateRefusal("database %q appears twice in the cluster state: a database name is unique", sorted.Databases[i].Name)
		}
	}
	users, err := canonicalUsers(st)
	if err != nil {
		return nil, err
	}
	sorted.Users = users
	for i, b := range sorted.SeedBitmaps {
		if len(b.Bits) != seedBitmapBytes {
			return nil, stateRefusal("seed bitmap for node %q database %d is %d bytes, want %d (8192 bits)", b.Node, b.DBID, len(b.Bits), seedBitmapBytes)
		}
		if i > 0 && compareBitmaps(b, sorted.SeedBitmaps[i-1]) == 0 {
			return nil, stateRefusal("seed bitmap for node %q database %d appears twice in the cluster state", b.Node, b.DBID)
		}
	}
	return encodeClusterStateInGivenOrder(sorted), nil
}

// checkMemberRow refuses a row the wire or the voter flag could not carry: an id or data address over
// MaxMemberTextBytes, a client address over MaxClientAddrBytes, or a suffrage that is not one of the
// two words SET_VOTERS writes. Encode and decode both call it, so the bytes of a state have one spelling.
func checkMemberRow(m Member) error {
	switch {
	case len(m.ID) > MaxMemberTextBytes:
		return stateRefusal("member id %q is %d bytes and at most %d are kept", m.ID[:32]+"...", len(m.ID), MaxMemberTextBytes)
	case len(m.DataAddr) > MaxMemberTextBytes:
		return stateRefusal("member %q has a data address of %d bytes and at most %d are kept (a host name of at most %d characters, a colon and the port)", m.ID, len(m.DataAddr), MaxMemberTextBytes, MaxMemberTextBytes-6)
	case len(m.ClientAddr) > MaxClientAddrBytes:
		return stateRefusal("member %q has a client address of %d bytes and at most %d are kept (a host name of at most %d characters, a colon and the port)", m.ID, len(m.ClientAddr), MaxClientAddrBytes, MaxClientAddrBytes-6)
	case m.Suffrage != "voter" && m.Suffrage != "nonvoter":
		return stateRefusal("member %q has suffrage %q: it is \"voter\" or \"nonvoter\", the words SET_VOTERS writes", m.ID, m.Suffrage)
	}
	return nil
}

// encodeClusterStateInGivenOrder writes the arrays in the order given,
// without sorting or checking. Only EncodeClusterState and tests call it;
// tests use it to build bytes the canonical encoder would never write.
func encodeClusterStateInGivenOrder(st ClusterState) []byte {
	var body []byte
	body = bsoncore.AppendInt32Element(body, "v", stateVersion)
	// Second, directly after v: a reader of a later build's snapshot finds it before any field it
	// may not know, so the open check can name the version (cluster-version spec 3 and 4.4).
	body = bsoncore.AppendInt32Element(body, "cluster_version", int32(st.ClusterVersion))
	body = bsoncore.AppendBinaryElement(body, "cluster_id", bsontype.BinaryGeneric, st.ClusterID[:])
	body = bsoncore.AppendInt64Element(body, "epoch", st.Epoch)
	body = bsoncore.AppendInt64Element(body, "state_seq", st.StateSeq)
	body = bsoncore.AppendInt64Element(body, "next_database_id", st.NextDatabaseID)

	voters := make([][]byte, len(st.Voters))
	for i, id := range st.Voters {
		voters[i] = bsoncore.AppendStringElement(nil, strconv.Itoa(i), id)
	}
	body = bsoncore.AppendArrayElement(body, "voters", bsoncore.BuildDocument(nil, bytes.Join(voters, nil)))

	members := make([][]byte, len(st.Members))
	for i, m := range st.Members {
		var row []byte
		row = bsoncore.AppendStringElement(row, "id", m.ID)
		row = bsoncore.AppendInt64Element(row, "incarnation", m.Incarnation)
		row = bsoncore.AppendStringElement(row, "data_addr", m.DataAddr)
		if m.ClientAddr != "" {
			// Written only when set: a cluster that publishes none pays nothing for the field.
			row = bsoncore.AppendStringElement(row, "client_addr", m.ClientAddr)
		}
		row = bsoncore.AppendStringElement(row, "state", m.State.String())
		row = bsoncore.AppendStringElement(row, "suffrage", m.Suffrage)
		row = bsoncore.AppendBooleanElement(row, "witness", m.Witness)
		row = bsoncore.AppendInt64Element(row, "joined_epoch", m.JoinedEpoch)
		row = bsoncore.AppendInt64Element(row, "joined_state_seq", m.JoinedStateSeq)
		members[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, row))
	}
	body = bsoncore.AppendArrayElement(body, "members", bsoncore.BuildDocument(nil, bytes.Join(members, nil)))

	databases := make([][]byte, len(st.Databases))
	for i, d := range st.Databases {
		var row []byte
		row = bsoncore.AppendStringElement(row, "name", d.Name)
		row = bsoncore.AppendInt64Element(row, "id", d.ID)
		row = bsoncore.AppendInt32Element(row, "rf", d.RF)
		row = bsoncore.AppendInt64Element(row, "schema_version", d.SchemaVersion)
		row = bsoncore.AppendStringElement(row, "storage", d.StorageWord())
		databases[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, row))
	}
	body = bsoncore.AppendArrayElement(body, "databases", bsoncore.BuildDocument(nil, bytes.Join(databases, nil)))

	bitmaps := make([][]byte, len(st.SeedBitmaps))
	for i, b := range st.SeedBitmaps {
		var row []byte
		row = bsoncore.AppendStringElement(row, "node", b.Node)
		row = bsoncore.AppendInt64Element(row, "db_id", b.DBID)
		row = bsoncore.AppendBinaryElement(row, "bits", bsontype.BinaryGeneric, b.Bits)
		bitmaps[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bsoncore.BuildDocument(nil, row))
	}
	body = bsoncore.AppendArrayElement(body, "seed_bitmaps", bsoncore.BuildDocument(nil, bytes.Join(bitmaps, nil)))

	s := st.Settings
	var settings []byte
	settings = bsoncore.AppendBooleanElement(settings, "stale_reads", s.StaleReads)
	settings = bsoncore.AppendInt64Element(settings, "heartbeat_ms", s.HeartbeatMs)
	settings = bsoncore.AppendInt64Element(settings, "step_down_ms", s.StepDownMs)
	settings = bsoncore.AppendInt64Element(settings, "failover_ms", s.FailoverMs)
	settings = bsoncore.AppendInt64Element(settings, "dead_ms", s.DeadMs)
	settings = bsoncore.AppendInt64Element(settings, "probe_interval_ms", s.ProbeIntervalMs)
	settings = bsoncore.AppendInt64Element(settings, "probe_timeout_ms", s.ProbeTimeoutMs)
	settings = bsoncore.AppendInt32Element(settings, "probe_failures", s.ProbeFailures)
	body = bsoncore.AppendDocumentElement(body, "settings", bsoncore.BuildDocument(nil, settings))
	body = appendUsers(body, st.Users, st.PepperKID, st.UserDropSeq, st.UserRequests)

	return bsoncore.BuildDocument(nil, body)
}

// stateFields reads the elements of one document strictly: each expected key
// once, in order, and nothing else.
type stateFields struct {
	where string
	elems []bsoncore.Element
	pos   int
}

func newStateFields(where string, doc bsoncore.Document) (*stateFields, error) {
	if err := doc.Validate(); err != nil {
		return nil, stateRefusal("%s is not a valid BSON document (%v): it must be written by EncodeClusterState", where, err)
	}
	elems, err := doc.Elements()
	if err != nil {
		return nil, stateRefusal("%s cannot be read (%v): it must be written by EncodeClusterState", where, err)
	}
	return &stateFields{where: where, elems: elems}, nil
}

func (f *stateFields) take(key string, want bsontype.Type) (bsoncore.Value, error) {
	if f.pos >= len(f.elems) {
		return bsoncore.Value{}, stateRefusal("%s is missing field %q: every field is written, in the canonical order", f.where, key)
	}
	e := f.elems[f.pos]
	got := e.Key()
	if got != key {
		if strings.Contains(got, "bucket") {
			return bsoncore.Value{}, stateRefusal("%s carries the field %q, but cluster state holds no bucket declarations: a declaration is a document in the ring, so drop the field", f.where, got)
		}
		return bsoncore.Value{}, differs(stateRefusal("%s has field %q where %q is expected: fields appear once, in the canonical order, with no unknown field", f.where, got, key),
			"has the field %q, which this build does not read", got)
	}
	f.pos++
	v := e.Value()
	if v.Type != want {
		return bsoncore.Value{}, stateRefusal("%s field %q has BSON type %s, want %s", f.where, key, v.Type, want)
	}
	return v, nil
}

func (f *stateFields) finish() error {
	if f.pos < len(f.elems) {
		extra := f.elems[f.pos].Key()
		if strings.Contains(extra, "bucket") {
			return stateRefusal("%s carries the field %q, but cluster state holds no bucket declarations: a declaration is a document in the ring, so drop the field", f.where, extra)
		}
		return differs(stateRefusal("%s has an unknown field %q after its last known field", f.where, extra),
			"has the field %q, which this build does not read", extra)
	}
	return nil
}

// The accessors below read a value's raw bytes themselves and check every
// length first, because bsoncore's own accessors panic on a short or
// mistyped value and this input comes off the network.

func (f *stateFields) int64(key string) (int64, error) {
	v, err := f.take(key, bson.TypeInt64)
	if err != nil {
		return 0, err
	}
	if len(v.Data) != 8 {
		return 0, stateRefusal("%s field %q is %d bytes, want an 8-byte int64", f.where, key, len(v.Data))
	}
	return int64(binary.LittleEndian.Uint64(v.Data)), nil
}

func (f *stateFields) int32(key string) (int32, error) {
	v, err := f.take(key, bson.TypeInt32)
	if err != nil {
		return 0, err
	}
	if len(v.Data) != 4 {
		return 0, stateRefusal("%s field %q is %d bytes, want a 4-byte int32", f.where, key, len(v.Data))
	}
	return int32(binary.LittleEndian.Uint32(v.Data)), nil
}

// stateString reads a BSON string value: a length that matches the bytes, one
// trailing NUL, no other NUL, valid UTF-8.
func stateString(where, key string, raw []byte) (string, error) {
	if len(raw) < 5 || int(binary.LittleEndian.Uint32(raw)) != len(raw)-4 {
		return "", stateRefusal("%s field %q has a string length prefix that disagrees with its bytes", where, key)
	}
	body := raw[4:]
	if body[len(body)-1] != 0 {
		return "", stateRefusal("%s field %q is a string without its trailing NUL byte", where, key)
	}
	body = body[:len(body)-1]
	if bytes.IndexByte(body, 0) >= 0 {
		return "", stateRefusal("%s field %q contains an embedded NUL byte: ids, names and addresses are plain text", where, key)
	}
	if !utf8.Valid(body) {
		return "", stateRefusal("%s field %q is not valid UTF-8", where, key)
	}
	return string(body), nil
}

func (f *stateFields) str(key string) (string, error) {
	v, err := f.take(key, bson.TypeString)
	if err != nil {
		return "", err
	}
	return stateString(f.where, key, v.Data)
}

// optClientAddr reads client_addr when the next field is that one. It is written only when
// set, so a written empty value or one over MaxClientAddrBytes is a second spelling and is
// refused.
func (f *stateFields) optClientAddr() (string, error) {
	if f.pos >= len(f.elems) || f.elems[f.pos].Key() != "client_addr" {
		return "", nil
	}
	text, err := f.str("client_addr")
	if err != nil {
		return "", err
	}
	if text == "" || len(text) > MaxClientAddrBytes {
		return "", stateRefusal("%s field \"client_addr\" is %d bytes: it is written only when set, and holds at most %d bytes", f.where, len(text), MaxClientAddrBytes)
	}
	return text, nil
}

func (f *stateFields) boolean(key string) (bool, error) {
	v, err := f.take(key, bson.TypeBoolean)
	if err != nil {
		return false, err
	}
	if len(v.Data) != 1 || v.Data[0] > 1 {
		return false, stateRefusal("%s field %q is not a canonical boolean: its byte must be 0 or 1", f.where, key)
	}
	return v.Data[0] == 1, nil
}

// binary returns a generic-subtype binary of exactly size bytes, copied.
func (f *stateFields) binary(key string, size int) ([]byte, error) {
	v, err := f.take(key, bson.TypeBinary)
	if err != nil {
		return nil, err
	}
	if len(v.Data) < 5 || int(binary.LittleEndian.Uint32(v.Data)) != len(v.Data)-5 {
		return nil, stateRefusal("%s field %q has a binary length prefix that disagrees with its bytes", f.where, key)
	}
	if subtype := v.Data[4]; subtype != 0 {
		return nil, stateRefusal("%s field %q has binary subtype 0x%02x, want 0x00", f.where, key, subtype)
	}
	payload := v.Data[5:]
	if len(payload) != size {
		return nil, stateRefusal("%s field %q is %d bytes, want %d", f.where, key, len(payload), size)
	}
	return slices.Clone(payload), nil
}

// rows reads an array field whose elements all have type rowType and are
// keyed "0", "1", ... in order, and hands each element's raw value to read.
func (f *stateFields) rows(key string, rowType bsontype.Type, read func(i int, raw []byte) error) error {
	v, err := f.take(key, bson.TypeArray)
	if err != nil {
		return err
	}
	inner, err := newStateFields(f.where+" "+key, bsoncore.Document(v.Data))
	if err != nil {
		return err
	}
	for i, e := range inner.elems {
		if e.Key() != strconv.Itoa(i) {
			return stateRefusal("%s %s has index key %q where %q is expected", f.where, key, e.Key(), strconv.Itoa(i))
		}
		ev := e.Value()
		if ev.Type != rowType {
			return stateRefusal("%s %s[%d] has BSON type %s, want %s", f.where, key, i, ev.Type, rowType)
		}
		if err := read(i, ev.Data); err != nil {
			return err
		}
	}
	return nil
}

func decodeStateWord(word string) (MemberState, error) {
	for s := MemberJoining; s <= MemberDead; s++ {
		if s.String() == word {
			return s, nil
		}
	}
	return 0, differs(stateRefusal("member state %q is not a state: use joining, active, down, leaving or dead", word),
		"holds the value %q for member state, which this build does not accept", word)
}

// DecodeClusterState reads a document written by EncodeClusterState and
// refuses everything else with INVALID_ARGUMENT: trailing bytes, an unknown,
// repeated, missing or reordered field, any bucket field, a wrong type, an
// unknown member state, an array that is not strictly sorted, and a bitmap
// that is not 1024 bytes. There is one accepted byte string per state.
func DecodeClusterState(raw []byte) (ClusterState, error) {
	var st ClusterState
	if len(raw) < 5 || int(binary.LittleEndian.Uint32(raw)) != len(raw) {
		return st, stateRefusal("cluster state is %d bytes but its length prefix says otherwise: it must be exactly one BSON document with no trailing bytes", len(raw))
	}
	top, err := newStateFields("cluster state", raw)
	if err != nil {
		return st, err
	}
	version, err := top.int32("v")
	if err != nil {
		return st, err
	}
	if version != stateVersion {
		return st, differs(stateRefusal("cluster state version %d is not supported: this build reads version %d", version, stateVersion),
			"is version %d and this build reads version %d", version, stateVersion)
	}
	if st.ClusterVersion, err = decodeClusterVersion(top); err != nil {
		return st, err
	}
	id, err := top.binary("cluster_id", len(st.ClusterID))
	if err != nil {
		return st, err
	}
	copy(st.ClusterID[:], id)
	if st.Epoch, err = top.int64("epoch"); err != nil {
		return st, err
	}
	if st.StateSeq, err = top.int64("state_seq"); err != nil {
		return st, err
	}
	if st.NextDatabaseID, err = top.int64("next_database_id"); err != nil {
		return st, err
	}

	err = top.rows("voters", bson.TypeString, func(i int, raw []byte) error {
		s, err := stateString("cluster state", "voters", raw)
		if err != nil {
			return err
		}
		if i > 0 && s == st.Voters[i-1] {
			return stateRefusal("voter %q appears twice in the cluster state: list each voter once", s)
		}
		if i > 0 && s < st.Voters[i-1] {
			return stateRefusal("cluster state voters are not strictly sorted by id at %q: write them with EncodeClusterState", s)
		}
		st.Voters = append(st.Voters, s)
		return nil
	})
	if err != nil {
		return st, err
	}

	err = top.rows("members", bson.TypeEmbeddedDocument, func(i int, raw []byte) error {
		m, err := decodeStateMember(raw)
		if err != nil {
			return fmt.Errorf("members[%d]: %w", i, err)
		}
		if i > 0 && m.ID == st.Members[i-1].ID {
			return stateRefusal("member %q appears twice in the cluster state: list each member once, a member id is unique", m.ID)
		}
		if i > 0 && m.ID < st.Members[i-1].ID {
			return stateRefusal("cluster state members are not strictly sorted by id at %q: write them with EncodeClusterState", m.ID)
		}
		st.Members = append(st.Members, m)
		return nil
	})
	if err != nil {
		return st, err
	}

	err = top.rows("databases", bson.TypeEmbeddedDocument, func(i int, raw []byte) error {
		d, err := decodeStateDatabase(raw)
		if err != nil {
			return fmt.Errorf("databases[%d]: %w", i, err)
		}
		if i > 0 && d.Name == st.Databases[i-1].Name {
			return stateRefusal("database %q appears twice in the cluster state: list each database once, a database name is unique", d.Name)
		}
		if i > 0 && d.Name < st.Databases[i-1].Name {
			return stateRefusal("cluster state databases are not strictly sorted by name at %q: write them with EncodeClusterState", d.Name)
		}
		st.Databases = append(st.Databases, d)
		return nil
	})
	if err != nil {
		return st, err
	}

	err = top.rows("seed_bitmaps", bson.TypeEmbeddedDocument, func(i int, raw []byte) error {
		b, err := decodeStateBitmap(raw)
		if err != nil {
			return fmt.Errorf("seed_bitmaps[%d]: %w", i, err)
		}
		if i > 0 && compareBitmaps(b, st.SeedBitmaps[i-1]) == 0 {
			return stateRefusal("seed bitmap for node %q database %d appears twice in the cluster state: list each (node, db_id) once", b.Node, b.DBID)
		}
		if i > 0 && compareBitmaps(b, st.SeedBitmaps[i-1]) < 0 {
			return stateRefusal("cluster state seed_bitmaps are not strictly sorted by (node, db_id) at (%q, %d): write them with EncodeClusterState", b.Node, b.DBID)
		}
		st.SeedBitmaps = append(st.SeedBitmaps, b)
		return nil
	})
	if err != nil {
		return st, err
	}

	sv, err := top.take("settings", bson.TypeEmbeddedDocument)
	if err != nil {
		return st, err
	}
	if st.Settings, err = decodeStateSettings(sv.Data); err != nil {
		return st, err
	}
	if err := decodeUsers(top, &st); err != nil {
		return st, err
	}
	if err := top.finish(); err != nil {
		return st, err
	}
	return st, nil
}

// decodeClusterVersion reads the cluster_version field: 1 to 255, since a state that is written
// has been bootstrapped and a level is one byte.
func decodeClusterVersion(top *stateFields) (uint8, error) {
	version, err := top.int32("cluster_version")
	if err != nil {
		return 0, err
	}
	if version < 1 || version > 255 {
		return 0, stateRefusal("cluster state field \"cluster_version\" is %d, and a cluster version is 1 to 255", version)
	}
	return uint8(version), nil
}

func decodeStateMember(raw []byte) (Member, error) {
	var m Member
	f, err := newStateFields("member", bsoncore.Document(raw))
	if err != nil {
		return m, err
	}
	if m.ID, err = f.str("id"); err != nil {
		return m, err
	}
	if m.Incarnation, err = f.int64("incarnation"); err != nil {
		return m, err
	}
	if m.DataAddr, err = f.str("data_addr"); err != nil {
		return m, err
	}
	if m.ClientAddr, err = f.optClientAddr(); err != nil {
		return m, err
	}
	word, err := f.str("state")
	if err != nil {
		return m, err
	}
	if m.State, err = decodeStateWord(word); err != nil {
		return m, err
	}
	if m.Suffrage, err = f.str("suffrage"); err != nil {
		return m, err
	}
	if m.Witness, err = f.boolean("witness"); err != nil {
		return m, err
	}
	if m.JoinedEpoch, err = f.int64("joined_epoch"); err != nil {
		return m, err
	}
	if m.JoinedStateSeq, err = f.int64("joined_state_seq"); err != nil {
		return m, err
	}
	if err := checkMemberRow(m); err != nil {
		return m, err
	}
	return m, f.finish()
}

func decodeStateDatabase(raw []byte) (Database, error) {
	var d Database
	f, err := newStateFields("database", bsoncore.Document(raw))
	if err != nil {
		return d, err
	}
	if d.Name, err = f.str("name"); err != nil {
		return d, err
	}
	if d.ID, err = f.int64("id"); err != nil {
		return d, err
	}
	if d.RF, err = f.int32("rf"); err != nil {
		return d, err
	}
	if d.SchemaVersion, err = f.int64("schema_version"); err != nil {
		return d, err
	}
	word, err := f.str("storage")
	if err != nil {
		return d, err
	}
	if d.Storage, err = ParseStorageWord(word); err != nil {
		return d, err
	}
	return d, f.finish()
}

func decodeStateBitmap(raw []byte) (SeedBitmap, error) {
	var b SeedBitmap
	f, err := newStateFields("seed bitmap", bsoncore.Document(raw))
	if err != nil {
		return b, err
	}
	if b.Node, err = f.str("node"); err != nil {
		return b, err
	}
	if b.DBID, err = f.int64("db_id"); err != nil {
		return b, err
	}
	if b.Bits, err = f.binary("bits", seedBitmapBytes); err != nil {
		return b, err
	}
	return b, f.finish()
}

func decodeStateSettings(raw []byte) (Settings, error) {
	var s Settings
	f, err := newStateFields("settings", bsoncore.Document(raw))
	if err != nil {
		return s, err
	}
	if s.StaleReads, err = f.boolean("stale_reads"); err != nil {
		return s, err
	}
	for _, field := range []struct {
		key string
		dst *int64
	}{
		{"heartbeat_ms", &s.HeartbeatMs}, {"step_down_ms", &s.StepDownMs}, {"failover_ms", &s.FailoverMs},
		{"dead_ms", &s.DeadMs}, {"probe_interval_ms", &s.ProbeIntervalMs}, {"probe_timeout_ms", &s.ProbeTimeoutMs},
	} {
		if *field.dst, err = f.int64(field.key); err != nil {
			return s, err
		}
	}
	if s.ProbeFailures, err = f.int32("probe_failures"); err != nil {
		return s, err
	}
	return s, f.finish()
}
