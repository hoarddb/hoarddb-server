package cluster

import (
	"bytes"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"testing"

	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// fsmDoc builds a command body from already encoded elements.
func fsmDoc(elems ...[]byte) []byte {
	return vendorbsoncore.BuildDocument(nil, bytes.Join(elems, nil))
}

func fsmStr(key, val string) []byte     { return vendorbsoncore.AppendStringElement(nil, key, val) }
func fsmI64(key string, v int64) []byte { return vendorbsoncore.AppendInt64Element(nil, key, v) }
func fsmI32(key string, v int32) []byte { return vendorbsoncore.AppendInt32Element(nil, key, v) }
func fsmBool(key string, v bool) []byte { return vendorbsoncore.AppendBooleanElement(nil, key, v) }
func fsmBin(key string, b []byte) []byte {
	return vendorbsoncore.AppendBinaryElement(nil, key, vendorbson.TypeBinaryGeneric, b)
}

func fsmBitsAt(slots ...int) []byte {
	bits := make([]byte, seedBitmapBytes)
	for _, slot := range slots {
		bits[slot/8] |= 1 << (slot % 8)
	}
	return bits
}

// fsmVersion is BOOTSTRAP's cluster_version at this build's level.
func fsmVersion() []byte { return fsmI32("cluster_version", int32(StateLevel)) }

// fsmLevels is REGISTER_MEMBER's min_level and max_level: the joiner runs this build's range.
func fsmLevels() []byte {
	levels := BuildStateRange()
	return append(fsmI32("min_level", int32(levels.Min)), fsmI32("max_level", int32(levels.Max))...)
}

func fsmMemberDoc(id string, inc int64, addr string) []byte {
	return fsmDoc(fsmStr("id", id), fsmI64("incarnation", inc), fsmStr("data_addr", addr))
}

func fsmBootstrapBody(id string) []byte {
	id16 := stateClusterID()
	return fsmDoc(fsmBin("cluster_id", id16[:]), fsmVersion(),
		vendorbsoncore.AppendDocumentElement(nil, "self", fsmMemberDoc(id, 1, id+":1")))
}

func fsmSetMemberStateBody(id, state string) []byte {
	return fsmDoc(fsmStr("id", id), fsmStr("state", state))
}

func fsmReplaceBody(id string, incarnation int64, clear bool) []byte {
	return fsmDoc(fsmStr("id", id), fsmStr("state", "joining"), fsmI64("incarnation", incarnation), fsmBool("clear_bits", clear))
}

func fsmRegisterBody(id string, inc int64, addr string, witness bool) []byte {
	return fsmDoc(fsmStr("id", id), fsmI64("incarnation", inc), fsmStr("data_addr", addr),
		fsmBool("witness", witness), fsmBin("placing_set", fsmBitsAt(1, 2, 3)), fsmLevels())
}

func fsmCreateDatabaseBody(name string, id int64, rf int32) []byte {
	return fsmDoc(fsmStr("name", name), fsmI64("id", id), fsmI32("rf", rf), fsmI64("schema_version", 1))
}

func fsmNodeBitsBody(node string, dbID int64, bits []byte) []byte {
	return fsmDoc(fsmStr("node", node), fsmI64("db_id", dbID), fsmBin("bits", bits))
}

func fsmVotersBody(id string, voting bool) []byte {
	return fsmDoc(fsmStr("id", id), fsmBool("voting", voting))
}

// fsmApply proposes cmd against the state's current state_seq and demands
// that it is accepted.
func fsmApply(t testing.TB, st *ClusterState, kind byte, body []byte) Result {
	t.Helper()
	res, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq), Body: body})
	if err != nil {
		t.Fatalf("kind 0x%02x refused: %v", kind, err)
	}
	return res
}

func fsmMember(st ClusterState, id string) (Member, bool) {
	for _, m := range st.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// fsmCluster builds a state through the commands themselves: members n1, n2,
// n3 active and voters, database "app" id 1 rf 2, and n2 seeded for slots 5
// and 6 of it. Epoch and state_seq are whatever the commands made.
func fsmCluster(t testing.TB) ClusterState {
	t.Helper()
	var st ClusterState
	fsmApply(t, &st, CommandBootstrap, fsmBootstrapBody("n1"))
	for _, id := range []string{"n2", "n3"} {
		fsmApply(t, &st, CommandRegisterMember, fsmRegisterBody(id, 1, id+":1", false))
		fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody(id, "active"))
	}
	fsmApply(t, &st, CommandSetVoters, fsmVotersBody("n2", true))
	fsmApply(t, &st, CommandSetVoters, fsmVotersBody("n3", true))
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("app", st.NextDatabaseID, 2))
	fsmApply(t, &st, CommandSeeded, fsmNodeBitsBody("n2", 1, fsmBitsAt(5, 6)))
	return st
}

func fsmEncoded(t *testing.T, st ClusterState) []byte {
	t.Helper()
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatalf("state does not encode: %v", err)
	}
	return raw
}

// fsmKindCases is one valid command per kind (except BOOTSTRAP) against
// fsmCluster, so a test can run every kind without a vacuous pass.
func fsmKindCases(st ClusterState) map[byte][]byte {
	return map[byte][]byte{
		CommandSetMemberState: fsmSetMemberStateBody("n3", "down"),
		CommandRegisterMember: fsmRegisterBody("n4", 1, "n4:1", false),
		CommandRemoveMember:   fsmDoc(fsmStr("id", "n3")),
		CommandCreateDatabase: fsmCreateDatabaseBody("shop", st.NextDatabaseID, 2),
		CommandAlterDatabase:  fsmDoc(fsmI64("id", 1), fsmI32("rf", 3)),
		CommandDropDatabase:   fsmDoc(fsmI64("id", 1), fsmI64("schema_version", 1)),
		CommandSetSetting:     fsmDoc(fsmStr("key", "stale_reads"), fsmBool("value", true)),
		CommandSeeded:         fsmNodeBitsBody("n3", 1, fsmBitsAt(9)),
		CommandRetire:         fsmNodeBitsBody("n2", 1, fsmBitsAt(5)),
		CommandBumpSchema:     fsmDoc(fsmI64("db_id", 1), fsmI64("from", 1), fsmI64("to", 2)),
		CommandSetVoters:      fsmVotersBody("n3", true),
	}
}

func fsmWantCode(t *testing.T, err error, want Code) {
	t.Helper()
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != want {
		t.Fatalf("got %v, want a %s refusal", err, want)
	}
}

func TestCommandPayloadLayout(t *testing.T) {
	body := fsmSetMemberStateBody("n2", "down")
	cmd := Command{Version: CommandVersion, Kind: CommandSetMemberState, BaseSeq: 1, Body: body}
	got, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := stateHex(t,
		"01", "02", "0000000000000001",
		"20000000",
		"02696400", "03000000", "6e3200",
		"02737461746500", "05000000", "646f776e00",
		"00")
	if len(got) != 42 || !bytes.Equal(got, want) {
		t.Fatalf("SET_MEMBER_STATE is %d bytes %x, want the 42 bytes of membership Formats 1:\n%x", len(got), got, want)
	}
	back, err := DecodeCommand(got)
	if err != nil || back.Kind != cmd.Kind || back.BaseSeq != 1 || !bytes.Equal(back.Body, body) {
		t.Fatalf("decode gave %+v, %v", back, err)
	}

	withClear, err := EncodeCommand(Command{Version: CommandVersion, Kind: CommandSetMemberState, BaseSeq: 1,
		Body: fsmDoc(fsmStr("id", "n2"), fsmStr("state", "down"), fsmBool("clear_bits", true))})
	if err != nil || len(withClear) != 55 {
		t.Fatalf("a payload carrying clear_bits is %d bytes (%v), want 13 more than 42", len(withClear), err)
	}

	// The stale base_seq reaches the proposer as STATE_MOVED, state intact.
	st := fsmCluster(t)
	before := fsmEncoded(t, st)
	_, err = st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSetMemberState, BaseSeq: 1, Body: body})
	fsmWantCode(t, err, CodeStateMoved)
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a refused command changed the state")
	}
}

func TestBaseSeqMismatchIsRejectedEverywhere(t *testing.T) {
	for kind, body := range fsmKindCases(fsmCluster(t)) {
		fresh := fsmCluster(t)
		if _, err := fresh.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(fresh.StateSeq), Body: body}); err != nil {
			t.Fatalf("kind 0x%02x: the body is not valid against the test cluster, so the guard would pass for the wrong reason: %v", kind, err)
		}
		for _, skew := range []int64{-1, 1} {
			st := fsmCluster(t)
			before := fsmEncoded(t, st)
			_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq + skew), Body: body})
			if skew > 0 {
				// A stamp above the state is a disagreement, not a stale proposer (BUG-0781).
				fsmWantDiverged(t, err)
			} else {
				fsmWantCode(t, err, CodeStateMoved)
			}
			if !bytes.Equal(before, fsmEncoded(t, st)) {
				t.Fatalf("kind 0x%02x with base_seq skew %d changed the state", kind, skew)
			}
		}
	}
	var empty ClusterState
	_, err := empty.ApplyCommand(Command{Version: CommandVersion, Kind: CommandBootstrap, BaseSeq: 7, Body: fsmBootstrapBody("n1")})
	fsmWantDiverged(t, err)
	if empty.StateSeq != 0 || len(empty.Members) != 0 {
		t.Fatal("a stale BOOTSTRAP changed the state")
	}
}

func TestLeaveIsTwoCommits(t *testing.T) {
	st := fsmCluster(t)
	tableBefore := BuildTable(st).Encode()
	epoch, seq := st.Epoch, st.StateSeq

	res := fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n3", "leaving"))
	if st.Epoch != epoch || res.EpochMoved || st.StateSeq != seq+1 {
		t.Fatalf("the first leave commit moved epoch %d to %d (moved=%v), state_seq %d to %d: want state_seq only", epoch, st.Epoch, res.EpochMoved, seq, st.StateSeq)
	}
	if !bytes.Equal(tableBefore, BuildTable(st).Encode()) {
		t.Fatal("a leaver that stays ranked changed the slot table at the first commit")
	}

	res = fsmApply(t, &st, CommandRemoveMember, fsmDoc(fsmStr("id", "n3")))
	if st.Epoch != epoch+1 || !res.EpochMoved || st.StateSeq != seq+2 {
		t.Fatalf("the second leave commit left epoch %d (want %d), state_seq %d (want %d)", st.Epoch, epoch+1, st.StateSeq, seq+2)
	}
	if _, ok := fsmMember(st, "n3"); ok {
		t.Fatal("n3 is still a member after REMOVE_MEMBER")
	}
	if res.Epoch != st.Epoch || res.StateSeq != st.StateSeq {
		t.Fatalf("Result %+v disagrees with the state", res)
	}
}

func TestReplaceClearsBitsInTheJoiningCommit(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandSeeded, fsmNodeBitsBody("n1", 1, fsmBitsAt(7)))
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n2", "down"))
	epoch, seq := st.Epoch, st.StateSeq

	res := fsmApply(t, &st, CommandSetMemberState, fsmReplaceBody("n2", 2, true))
	if st.StateSeq != seq+1 {
		t.Fatalf("the replace took %d commits, want exactly one", st.StateSeq-seq)
	}
	for _, b := range st.SeedBitmaps {
		if b.Node == "n2" {
			t.Fatalf("n2 still has a SEEDED bitmap for database %d after its joining commit", b.DBID)
		}
	}
	if !slices.ContainsFunc(st.SeedBitmaps, func(b SeedBitmap) bool { return b.Node == "n1" }) {
		t.Fatal("clear_bits cleared another identity's bitmap")
	}
	m, _ := fsmMember(st, "n2")
	if m.State != MemberJoining || m.Incarnation != 2 {
		t.Fatalf("n2 is %v at incarnation %d, want joining at 2", m.State, m.Incarnation)
	}
	if st.Epoch != epoch+1 || !res.EpochMoved {
		t.Fatalf("a ranked down member entering joining left epoch %d, want %d", st.Epoch, epoch+1)
	}
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n2", "active"))
	if st.Epoch != epoch+2 {
		t.Fatalf("the active commit left epoch %d, want %d", st.Epoch, epoch+2)
	}

	// Without clear_bits the bits stay; from dead the joining commit moves nothing.
	other := fsmCluster(t)
	fsmApply(t, &other, CommandSetMemberState, fsmSetMemberStateBody("n2", "dead"))
	epoch = other.Epoch
	fsmApply(t, &other, CommandSetMemberState, fsmReplaceBody("n2", 2, false))
	if other.Epoch != epoch {
		t.Fatalf("joining from dead moved epoch %d to %d", epoch, other.Epoch)
	}
	if !slices.ContainsFunc(other.SeedBitmaps, func(b SeedBitmap) bool { return b.Node == "n2" }) {
		t.Fatal("bits were cleared although clear_bits was absent")
	}
}

func TestReplaceIsRefusedWhileTheIdentityIsStillActive(t *testing.T) {
	st := fsmCluster(t)
	before := fsmEncoded(t, st)
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSetMemberState, BaseSeq: uint64(st.StateSeq), Body: fsmReplaceBody("n2", 2, true)})
	fsmWantCode(t, err, CodeConflict)
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a refused replace changed the state")
	}
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n2", "down"))
	_, err = st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSetMemberState, BaseSeq: uint64(st.StateSeq), Body: fsmReplaceBody("n2", 0, true)})
	fsmWantCode(t, err, CodeConflict)
}

func TestDataAddrChangeBumpsNothing(t *testing.T) {
	st := fsmCluster(t)
	tableBefore := BuildTable(st).Encode()
	epoch, seq := st.Epoch, st.StateSeq

	res := fsmApply(t, &st, CommandRegisterMember, fsmRegisterBody("n2", 1, "elsewhere:9", false))
	m, _ := fsmMember(st, "n2")
	if m.DataAddr != "elsewhere:9" || m.State != MemberActive {
		t.Fatalf("n2 is %v at %q, want active at the new address", m.State, m.DataAddr)
	}
	if st.Epoch != epoch || res.EpochMoved {
		t.Fatalf("a data_addr change moved epoch %d to %d", epoch, st.Epoch)
	}
	if st.StateSeq != seq+1 {
		t.Fatalf("state_seq is %d, want %d", st.StateSeq, seq+1)
	}
	if !bytes.Equal(tableBefore, BuildTable(st).Encode()) {
		t.Fatal("a data_addr change moved a slot")
	}
}

func TestSetVotersIsTheOnlyWriterOfVoters(t *testing.T) {
	st := fsmCluster(t)
	for kind, body := range fsmKindCases(st) {
		if kind == CommandSetVoters {
			continue
		}
		probe := fsmCluster(t)
		want := slices.Clone(probe.Voters)
		fsmApply(t, &probe, kind, body)
		if !slices.Equal(probe.Voters, want) {
			t.Fatalf("kind 0x%02x changed voters from %v to %v", kind, want, probe.Voters)
		}
	}
	// Removing a voter's member row leaves the list to SET_VOTERS.
	gone := fsmCluster(t)
	fsmApply(t, &gone, CommandRemoveMember, fsmDoc(fsmStr("id", "n2")))
	if !slices.Equal(gone.Voters, []string{"n1", "n2", "n3"}) {
		t.Fatalf("REMOVE_MEMBER rewrote voters to %v", gone.Voters)
	}
	fsmApply(t, &gone, CommandSetVoters, fsmVotersBody("n2", false))
	if !slices.Equal(gone.Voters, []string{"n1", "n3"}) {
		t.Fatalf("SET_VOTERS{n2,false} left voters %v after the member was gone", gone.Voters)
	}

	epoch, seq := st.Epoch, st.StateSeq
	tableBefore := BuildTable(st).Encode()
	res := fsmApply(t, &st, CommandSetVoters, fsmVotersBody("n3", true))
	if !slices.Equal(st.Voters, []string{"n1", "n2", "n3"}) {
		t.Fatalf("voters are %v, want n1 n2 n3", st.Voters)
	}
	if st.Epoch != epoch || res.EpochMoved || st.StateSeq != seq+1 {
		t.Fatalf("a promotion moved epoch %d to %d or state_seq %d to %d", epoch, st.Epoch, seq, st.StateSeq)
	}
	if !bytes.Equal(tableBefore, BuildTable(st).Encode()) {
		t.Fatal("a promotion changed the slot table")
	}
	fsmApply(t, &st, CommandSetVoters, fsmVotersBody("n3", true))
	if !slices.Equal(st.Voters, []string{"n1", "n2", "n3"}) {
		t.Fatalf("a repeated SET_VOTERS duplicated a voter: %v", st.Voters)
	}
}

func TestBootstrapBuildsTheMinimalState(t *testing.T) {
	var st ClusterState
	res := fsmApply(t, &st, CommandBootstrap, fsmBootstrapBody("n1"))
	if st.Epoch != 1 || st.StateSeq != 1 || st.NextDatabaseID != 1 || !res.EpochMoved {
		t.Fatalf("bootstrap left epoch %d, state_seq %d, next_database_id %d", st.Epoch, st.StateSeq, st.NextDatabaseID)
	}
	want := stateMinimal()
	want.Members[0].DataAddr = "n1:1"
	want.ClusterVersion = StateLevel // the version the body carries
	if got := fsmEncoded(t, st); !bytes.Equal(got, fsmEncoded(t, want)) {
		t.Fatalf("bootstrap differs from the spec's minimal state:\n got %+v\nwant %+v", st, want)
	}
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandBootstrap, BaseSeq: 1, Body: fsmBootstrapBody("n9")})
	fsmWantCode(t, err, CodeClusterMismatch)
}

func TestEpochMovesOnlyWhenTheRingMoves(t *testing.T) {
	type step struct {
		kind  byte
		body  []byte
		moved bool
	}
	st := fsmCluster(t)
	for i, s := range []step{
		{CommandRegisterMember, fsmRegisterBody("n4", 1, "n4:1", false), false}, // a new member is unranked
		{CommandSetMemberState, fsmSetMemberStateBody("n4", "active"), true},    // joining to active
		{CommandSetMemberState, fsmSetMemberStateBody("n4", "down"), true},      // active to down
		{CommandSetMemberState, fsmSetMemberStateBody("n4", "leaving"), true},   // down to leaving
		{CommandSetMemberState, fsmSetMemberStateBody("n4", "down"), true},      // leaving to down
		{CommandSetMemberState, fsmSetMemberStateBody("n4", "dead"), true},      // down to dead
		{CommandRegisterMember, fsmRegisterBody("w", 1, "w:1", true), false},    // a witness registers
		{CommandSetMemberState, fsmSetMemberStateBody("w", "active"), false},    // a witness is never ranked
		{CommandCreateDatabase, fsmCreateDatabaseBody("shop", 2, 2), false},
		{CommandAlterDatabase, fsmDoc(fsmI64("id", 2), fsmI32("rf", 3)), false},
		{CommandBumpSchema, fsmDoc(fsmI64("db_id", 2), fsmI64("from", 1), fsmI64("to", 2)), false},
		{CommandSetSetting, fsmDoc(fsmStr("key", "dead_ms"), fsmI64("value", 1000)), false},
		{CommandRetire, fsmNodeBitsBody("n2", 1, fsmBitsAt(5)), false},
		{CommandDropDatabase, fsmDoc(fsmI64("id", 2), fsmI64("schema_version", 2)), false},
		{CommandRemoveMember, fsmDoc(fsmStr("id", "n4")), false}, // dead was already out of the ring
	} {
		epoch, seq := st.Epoch, st.StateSeq
		res := fsmApply(t, &st, s.kind, s.body)
		if res.EpochMoved != s.moved || (st.Epoch != epoch) != s.moved || (s.moved && st.Epoch != epoch+1) {
			t.Fatalf("step %d (kind 0x%02x): epoch %d to %d, want moved=%v", i, s.kind, epoch, st.Epoch, s.moved)
		}
		if st.StateSeq != seq+1 {
			t.Fatalf("step %d (kind 0x%02x): state_seq %d to %d, want one bump", i, s.kind, seq, st.StateSeq)
		}
	}
}

func TestSeededOrsRetireClears(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandSeeded, fsmNodeBitsBody("n2", 1, fsmBitsAt(6, 40)))
	fsmApply(t, &st, CommandSeeded, fsmNodeBitsBody("n2", 1, fsmBitsAt(6, 40)))
	row := func() []byte {
		for _, b := range st.SeedBitmaps {
			if b.Node == "n2" && b.DBID == 1 {
				return b.Bits
			}
		}
		t.Fatal("no bitmap row for n2")
		return nil
	}
	if !bytes.Equal(row(), fsmBitsAt(5, 6, 40)) {
		t.Fatalf("SEEDED did not OR idempotently: %x", row()[:8])
	}
	fsmApply(t, &st, CommandRetire, fsmNodeBitsBody("n2", 1, fsmBitsAt(6, 41)))
	if !bytes.Equal(row(), fsmBitsAt(5, 40)) {
		t.Fatalf("RETIRE did not clear exactly the named bits: %x", row()[:8])
	}
	before := fsmEncoded(t, st)
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSeeded, BaseSeq: uint64(st.StateSeq), Body: fsmNodeBitsBody("n2", 1, make([]byte, 1023))})
	fsmWantCode(t, err, CodeInvalidArgument)
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a short bitmap changed the state")
	}
}

func TestDatabaseCommands(t *testing.T) {
	st := fsmCluster(t)
	refuse := func(kind byte, body []byte, want Code) {
		t.Helper()
		before := fsmEncoded(t, st)
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq), Body: body})
		fsmWantCode(t, err, want)
		if !bytes.Equal(before, fsmEncoded(t, st)) {
			t.Fatalf("a refused 0x%02x changed the state", kind)
		}
	}
	refuse(CommandCreateDatabase, fsmCreateDatabaseBody("app", 2, 2), CodeConflict)  // name taken
	refuse(CommandCreateDatabase, fsmCreateDatabaseBody("shop", 9, 2), CodeConflict) // id is not next_database_id
	refuse(CommandCreateDatabase, fsmCreateDatabaseBody("shop", 2, 0), CodeInvalidArgument)
	refuse(CommandBumpSchema, fsmDoc(fsmI64("db_id", 1), fsmI64("from", 5), fsmI64("to", 6)), CodeSchemaNotCurrent)
	refuse(CommandDropDatabase, fsmDoc(fsmI64("id", 1), fsmI64("schema_version", 4)), CodeSchemaNotCurrent)
	refuse(CommandAlterDatabase, fsmDoc(fsmI64("id", 77), fsmI32("rf", 2)), CodeNotFound)

	fsmApply(t, &st, CommandDropDatabase, fsmDoc(fsmI64("id", 1), fsmI64("schema_version", 1)))
	if len(st.Databases) != 0 || len(st.SeedBitmaps) != 0 || st.NextDatabaseID != 2 {
		t.Fatalf("drop left %d databases, %d bitmaps, next_database_id %d", len(st.Databases), len(st.SeedBitmaps), st.NextDatabaseID)
	}
}

func TestUnknownKindAndVersionAreRefused(t *testing.T) {
	st := fsmCluster(t)
	for _, kind := range []byte{0x00, 0x08, 0x09, 0x0A, 0x17, 0x1F, 0xFF} {
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: uint64(st.StateSeq), Body: fsmDoc()})
		fsmWantCode(t, err, CodeInvalidArgument)
		if _, err := EncodeCommand(Command{Version: CommandVersion, Kind: kind, Body: fsmDoc()}); err == nil {
			t.Fatalf("EncodeCommand accepted kind 0x%02x", kind)
		}
	}
	_, err := st.ApplyCommand(Command{Version: 2, Kind: CommandSetVoters, BaseSeq: uint64(st.StateSeq), Body: fsmVotersBody("n3", true)})
	fsmWantCode(t, err, CodeInvalidArgument)
}

func TestBodiesAreCheckedStrictly(t *testing.T) {
	st := fsmCluster(t)
	for name, body := range map[string][]byte{
		"missing field":    fsmDoc(fsmStr("id", "n3")),
		"unknown field":    fsmDoc(fsmStr("id", "n3"), fsmBool("voting", true), fsmBool("extra", true)),
		"duplicate field":  fsmDoc(fsmStr("id", "n3"), fsmBool("voting", true), fsmBool("voting", false)),
		"wrong type":       fsmDoc(fsmStr("id", "n3"), fsmStr("voting", "yes")),
		"unknown member":   fsmVotersBody("zz", true),
		"empty id":         fsmVotersBody("", true),
		"not a document":   {1, 2, 3},
		"trailing garbage": append(fsmVotersBody("n3", true), 0),
	} {
		before := fsmEncoded(t, st)
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSetVoters, BaseSeq: uint64(st.StateSeq), Body: body})
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if !bytes.Equal(before, fsmEncoded(t, st)) {
			t.Fatalf("%s: a refused command changed the state", name)
		}
	}
}

// finish walks the field map, so the name it reports must not depend on
// map order: the same body must always be refused with the same message.
func TestFsmUnreadFieldNameIsDeterministic(t *testing.T) {
	st := fsmCluster(t)
	body := fsmDoc(fsmStr("id", "n3"), fsmBool("voting", true), fsmBool("zz", true), fsmBool("aa", true), fsmBool("mm", true))
	var first string
	for i := 0; i < 200; i++ {
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandSetVoters, BaseSeq: uint64(st.StateSeq), Body: body})
		if err == nil {
			t.Fatalf("attempt %d: a body with unread fields was accepted", i)
		}
		msg := err.Error()
		if i == 0 {
			if !strings.Contains(msg, `"aa"`) {
				t.Fatalf("first unread field should be \"aa\", got: %s", msg)
			}
			first = msg
			continue
		}
		if msg != first {
			t.Fatalf("attempt %d message %q differs from %q", i, msg, first)
		}
	}
}

func fsmTryDecode(raw []byte) (cmd Command, panicked any, err error) {
	defer func() { panicked = recover() }()
	cmd, err = DecodeCommand(raw)
	return cmd, nil, err
}

func fsmMutate(r *rand.Rand, golden []byte) []byte {
	out := slices.Clone(golden)
	switch r.Intn(5) {
	case 0:
		out[r.Intn(len(out))] = byte(r.Intn(256))
	case 1:
		i := r.Intn(len(out))
		out = slices.Delete(out, i, i+1)
	case 2:
		i := r.Intn(len(out) + 1)
		out = slices.Insert(out, i, byte(r.Intn(256)))
	case 3:
		out = out[:r.Intn(len(out)+1)]
	default:
		out[10+r.Intn(4)] = byte(r.Intn(256)) // the BSON length prefix
	}
	return out
}

func TestDecodeCommandNeverPanicsAndRoundTrips(t *testing.T) {
	st := fsmCluster(t)
	var goldens [][]byte
	for kind, body := range fsmKindCases(st) {
		raw, err := EncodeCommand(Command{Version: CommandVersion, Kind: kind, BaseSeq: 3, Body: body})
		if err != nil {
			t.Fatalf("kind 0x%02x does not encode: %v", kind, err)
		}
		goldens = append(goldens, raw)
	}
	r := rand.New(rand.NewSource(20261031))
	accepted := 0
	const mutations = 20000
	for n := 0; n < mutations; n++ {
		raw := fsmMutate(r, goldens[n%len(goldens)])
		cmd, panicked, err := fsmTryDecode(raw)
		if panicked != nil {
			t.Fatalf("mutation %d: DecodeCommand panicked (%v) on %x", n, panicked, raw)
		}
		if err != nil {
			fsmWantCode(t, err, CodeInvalidArgument)
			continue
		}
		accepted++
		again, encErr := EncodeCommand(cmd)
		if encErr != nil || !bytes.Equal(again, raw) {
			t.Fatalf("mutation %d: decoded %x but re-encoded %x (%v)", n, raw, again, encErr)
		}
		// Applying whatever decoded must not panic either.
		probe := fsmCluster(t)
		cmd.BaseSeq = uint64(probe.StateSeq)
		if _, applyErr := probe.ApplyCommand(cmd); applyErr == nil {
			fsmEncoded(t, probe)
		}
	}
	t.Logf("%d mutations, %d decoded", mutations, accepted)
}

func TestMemoryDatabaseRefusesRFAboveOne(t *testing.T) {
	st := fsmCluster(t)
	body := fsmDoc(fsmStr("name", "cache"), fsmI64("id", st.NextDatabaseID), fsmI32("rf", 2), fsmI64("schema_version", 1), fsmStr("storage", "memory"))
	before := fsmEncoded(t, st)
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq), Body: body})
	fsmWantCode(t, err, CodeInvalidArgument)
	if err == nil || err.Error() != MemoryRFRefusal {
		t.Fatalf("err = %v, want exactly %q", err, MemoryRFRefusal)
	}
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a refused create changed the state")
	}
}

func TestCreateDatabaseWithoutStorageIsDisk(t *testing.T) {
	st := fsmCluster(t)
	id := st.NextDatabaseID
	fsmApply(t, &st, CommandCreateDatabase, fsmCreateDatabaseBody("plain", id, 1))
	i, ok := st.databaseByID(id)
	if !ok {
		t.Fatal("the created database is not in the state")
	}
	if got := st.Databases[i]; got.Storage != StorageDisk || got.StorageWord() != "disk" {
		t.Fatalf("Storage = %d (%q), want StorageDisk (disk)", got.Storage, got.StorageWord())
	}
}

func TestCreateDatabaseRefusesAnUnknownStorageWord(t *testing.T) {
	st := fsmCluster(t)
	before := fsmEncoded(t, st)
	body := fsmDoc(fsmStr("name", "odd"), fsmI64("id", st.NextDatabaseID), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storage", "ram"))
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq), Body: body})
	fsmWantCode(t, err, CodeInvalidArgument)
	want := `storage "ram" is not a kind. Use storage: disk or storage: memory`
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want exactly %q", err, want)
	}
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a refused create changed the state")
	}
	if _, err := ParseStorageWord("disk"); err != nil {
		t.Fatalf("disk: %v", err)
	}
	if b, err := ParseStorageWord("memory"); err != nil || b != StorageMemory {
		t.Fatalf("memory: %d, %v", b, err)
	}
}

// TestAlterOfAMemoryDatabaseToTwoCopiesIsRefused: alter holds the same one-copy rule as create, with
// the same text; a disk database still takes the new factor.
// [BREAK: delete the storage branch of applyAlterDatabase]
func TestAlterOfAMemoryDatabaseToTwoCopiesIsRefused(t *testing.T) {
	st := fsmCluster(t)
	memoryID := st.NextDatabaseID
	fsmApply(t, &st, CommandCreateDatabase, fsmDoc(fsmStr("name", "cache"), fsmI64("id", memoryID), fsmI32("rf", 1), fsmI64("schema_version", 1), fsmStr("storage", "memory")))
	before := fsmEncoded(t, st)
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandAlterDatabase, BaseSeq: uint64(st.StateSeq), Body: fsmDoc(fsmI64("id", memoryID), fsmI32("rf", 2))})
	fsmWantCode(t, err, CodeInvalidArgument)
	if err == nil || err.Error() != MemoryRFRefusal {
		t.Fatalf("err = %v, want exactly %q", err, MemoryRFRefusal)
	}
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a refused alter changed the state")
	}
	fsmApply(t, &st, CommandAlterDatabase, fsmDoc(fsmI64("id", memoryID), fsmI32("rf", 1)))
	fsmApply(t, &st, CommandAlterDatabase, fsmDoc(fsmI64("id", 1), fsmI32("rf", 3)))
	if i, _ := st.databaseByID(1); st.Databases[i].RF != 3 {
		t.Fatalf("a disk database alter to rf 3 left rf = %d", st.Databases[i].RF)
	}
}
