package cluster

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// c16Rewrite rebuilds an encoded state with edit applied to the elements of its first
// member row, so a test can write the bytes the canonical encoder never would.
func c16Rewrite(t *testing.T, raw []byte, edit func([]vendorbsoncore.Element) []vendorbsoncore.Element) []byte {
	t.Helper()
	return stateRebuild(t, raw, func(top []vendorbsoncore.Element) []vendorbsoncore.Element {
		for i, element := range top {
			if element.Key() != "members" {
				continue
			}
			array, ok := element.Value().ArrayOK()
			if !ok {
				t.Fatal("members is not an array")
			}
			rows, err := array.Values()
			if err != nil {
				t.Fatal(err)
			}
			row, ok := rows[0].DocumentOK()
			if !ok {
				t.Fatal("a member row is not a document")
			}
			fields, err := row.Elements()
			if err != nil {
				t.Fatal(err)
			}
			var body []byte
			for _, field := range edit(fields) {
				body = append(body, field...)
			}
			var rebuilt []byte
			rebuilt = append(rebuilt, vendorbsoncore.AppendDocumentElement(nil, "0", vendorbsoncore.BuildDocument(nil, body))...)
			for index, other := range rows[1:] {
				rebuilt = append(rebuilt, vendorbsoncore.AppendDocumentElement(nil, strconv.Itoa(index+1), other.Document())...)
			}
			top[i] = vendorbsoncore.AppendArrayElement(nil, "members", vendorbsoncore.BuildDocument(nil, rebuilt))
		}
		return top
	})
}

// TestC16ClientAddrRoundTripsAndCostsNothingWhenEmpty: [GUARD a member row writes client_addr only when
// it is set, so a state with none is byte-identical to one written before the field existed].
func TestC16ClientAddrRoundTripsAndCostsNothingWhenEmpty(t *testing.T) {
	plain, err := EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(plain, []byte("client_addr")) {
		t.Fatal("a member with no client address wrote the field")
	}
	st := stateFull()
	st.Members[0].ClientAddr = "n1:7434"
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("n1:7434")) {
		t.Fatal("the client address is not in the encoded state")
	}
	back, err := DecodeClusterState(raw)
	if err != nil {
		t.Fatal(err)
	}
	if back.Members[0].ClientAddr != "n1:7434" || back.Members[1].ClientAddr != "" {
		t.Fatalf("decoded client addresses %q, %q", back.Members[0].ClientAddr, back.Members[1].ClientAddr)
	}
	if again, err := EncodeClusterState(back); err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("re-encoding gave different bytes (%v)", err)
	}
	if len(raw) != len(plain)+len("client_addr")+1+1+4+len("n1:7434")+1 {
		t.Errorf("the field costs %d bytes", len(raw)-len(plain))
	}
}

// TestC16StateRefusesANonCanonicalClientAddr: [GUARD an empty client_addr, one out of order, one of the
// wrong type and one over the bound are refused, so the bytes of a state have one spelling].
func TestC16StateRefusesANonCanonicalClientAddr(t *testing.T) {
	st := stateFull()
	st.Members[0].ClientAddr = "n1:7434"
	good, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeClusterState(good); err != nil {
		t.Fatalf("the control was refused: %v", err)
	}
	swap := func(fields []vendorbsoncore.Element, key string, with vendorbsoncore.Element) []vendorbsoncore.Element {
		out := make([]vendorbsoncore.Element, 0, len(fields))
		for _, field := range fields {
			if field.Key() == key {
				field = with
			}
			out = append(out, field)
		}
		return out
	}
	cases := map[string][]byte{
		"written empty": c16Rewrite(t, good, func(f []vendorbsoncore.Element) []vendorbsoncore.Element {
			return swap(f, "client_addr", vendorbsoncore.AppendStringElement(nil, "client_addr", ""))
		}),
		"wrong type": c16Rewrite(t, good, func(f []vendorbsoncore.Element) []vendorbsoncore.Element {
			return swap(f, "client_addr", vendorbsoncore.AppendInt64Element(nil, "client_addr", 1))
		}),
		"before data_addr": c16Rewrite(t, good, func(f []vendorbsoncore.Element) []vendorbsoncore.Element {
			f[2], f[3] = f[3], f[2]
			return f
		}),
		"over the bound": c16Rewrite(t, good, func(f []vendorbsoncore.Element) []vendorbsoncore.Element {
			return swap(f, "client_addr", vendorbsoncore.AppendStringElement(nil, "client_addr", strings.Repeat("a", MaxClientAddrBytes+1)))
		}),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeClusterState(raw)
			stateWantInvalid(t, err)
		})
	}
}

func c16Register(id string, client string) []byte {
	elems := [][]byte{fsmStr("id", id), fsmI64("incarnation", 1), fsmStr("data_addr", id+":1"), fsmBool("witness", false)}
	if client != "" {
		elems = append(elems, fsmStr("client_addr", client))
	}
	return fsmDoc(append(elems, fsmBin("placing_set", fsmBitsAt(1)), fsmLevels())...)
}

// TestC16RegisterMemberCommitsTheClientAddr: [GUARD REGISTER_MEMBER stores client_addr, a re-register
// of the same incarnation replaces it (empty clears it), and it moves neither epoch nor a slot].
func TestC16RegisterMemberCommitsTheClientAddr(t *testing.T) {
	st := fsmCluster(t)
	epoch, table := st.Epoch, BuildTable(st).Encode()
	res := fsmApply(t, &st, CommandRegisterMember, c16Register("n2", "n2:7434"))
	row, _ := fsmMember(st, "n2")
	if row.ClientAddr != "n2:7434" || row.DataAddr != "n2:1" {
		t.Fatalf("n2 has client %q and data %q", row.ClientAddr, row.DataAddr)
	}
	if st.Epoch != epoch || res.EpochMoved || !bytes.Equal(table, BuildTable(st).Encode()) {
		t.Fatal("a client address change moved the ring")
	}
	fsmApply(t, &st, CommandRegisterMember, c16Register("n2", ""))
	if row, _ = fsmMember(st, "n2"); row.ClientAddr != "" {
		t.Fatalf("a re-register without a client address left %q", row.ClientAddr)
	}
	fsmApply(t, &st, CommandRegisterMember, c16Register("n9", "n9:7434"))
	if row, _ = fsmMember(st, "n9"); row.ClientAddr != "n9:7434" || row.State != MemberJoining {
		t.Fatalf("a new member has client %q in state %v", row.ClientAddr, row.State)
	}
	built := RegisterMemberBody(Member{ID: "n8", Incarnation: 1, DataAddr: "n8:1", ClientAddr: "n8:7434"}, fsmBitsAt(1), BuildStateRange())
	fsmApply(t, &st, CommandRegisterMember, built)
	if row, _ = fsmMember(st, "n8"); row.ClientAddr != "n8:7434" {
		t.Fatalf("RegisterMemberBody dropped the client address: %q", row.ClientAddr)
	}
	if bytes.Contains(RegisterMemberBody(Member{ID: "n7", Incarnation: 1, DataAddr: "n7:1"}, fsmBitsAt(1), BuildStateRange()), []byte("client_addr")) {
		t.Error("a member with no client address wrote the field")
	}
}

// TestC16ADeadIdentityReturnsWithItsNewClientAddr: [GUARD the return after stage 2 takes the new client address
// with the new data address].
func TestC16ADeadIdentityReturnsWithItsNewClientAddr(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandRegisterMember, c16Register("n2", "n2:7434"))
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n2", "leaving"))
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n2", "dead"))
	body := fsmDoc(fsmStr("id", "n2"), fsmI64("incarnation", 2), fsmStr("data_addr", "n2:2"), fsmBool("witness", false),
		fsmStr("client_addr", "n2:7435"), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels())
	fsmApply(t, &st, CommandRegisterMember, body)
	row, _ := fsmMember(st, "n2")
	if row.ClientAddr != "n2:7435" || row.State != MemberJoining || row.Incarnation != 2 {
		t.Fatalf("n2 came back as %+v", row)
	}
}

// TestC16BootstrapCommitsTheClientAddr: [GUARD BOOTSTRAP's self row carries client_addr].
func TestC16BootstrapCommitsTheClientAddr(t *testing.T) {
	var st ClusterState
	self := Member{ID: "n1", Incarnation: 1, DataAddr: "n1:1", ClientAddr: "n1:7434"}
	fsmApply(t, &st, CommandBootstrap, BootstrapBody(stateClusterID(), self, StateLevel))
	if row, _ := fsmMember(st, "n1"); row.ClientAddr != "n1:7434" {
		t.Fatalf("the bootstrap member has client %q", row.ClientAddr)
	}
	if bytes.Contains(BootstrapBody(stateClusterID(), Member{ID: "n1", Incarnation: 1, DataAddr: "n1:1"}, StateLevel), []byte("client_addr")) {
		t.Error("a bootstrap with no client address wrote the field")
	}
}

// TestC16ABadClientAddrIsRefusedAndChangesNothing: [GUARD an empty, over-long or mistyped client_addr in a
// command is INVALID_ARGUMENT and leaves the state as it was].
func TestC16ABadClientAddrIsRefusedAndChangesNothing(t *testing.T) {
	st := fsmCluster(t)
	host := strings.Repeat("a", MaxClientAddrBytes-len(":7434"))
	long := strings.Repeat("a", MaxClientAddrBytes-len(":7434")+1) + ":7434"
	fsmApply(t, &st, CommandRegisterMember, fsmDoc(fsmStr("id", "n4"), fsmI64("incarnation", 1), fsmStr("data_addr", host+":1"),
		fsmStr("client_addr", host+":7434"), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels())) // the control: the bound itself is accepted
	bodies := map[string][]byte{
		"empty": fsmDoc(fsmStr("id", "n5"), fsmI64("incarnation", 1), fsmStr("data_addr", "n5:1"), fsmStr("client_addr", ""), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels()),
		"too long": fsmDoc(fsmStr("id", "n5"), fsmI64("incarnation", 1), fsmStr("data_addr", long[:len(long)-5]+":1"),
			fsmStr("client_addr", long), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels()),
		"mistyped":  fsmDoc(fsmStr("id", "n5"), fsmI64("incarnation", 1), fsmStr("data_addr", "n5:1"), fsmI64("client_addr", 1), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels()),
		"duplicate": fsmDoc(fsmStr("id", "n5"), fsmI64("incarnation", 1), fsmStr("data_addr", "n5:1"), fsmStr("client_addr", "a:1"), fsmStr("client_addr", "a:1"), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels()),
	}
	for name, body := range bodies {
		before := fsmEncoded(t, st)
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandRegisterMember, BaseSeq: uint64(st.StateSeq), Body: body})
		fsmWantCode(t, err, CodeInvalidArgument)
		if !bytes.Equal(before, fsmEncoded(t, st)) {
			t.Fatalf("%s: a refused command changed the state", name)
		}
	}
	var empty ClusterState
	selfDoc := fsmDoc(fsmStr("id", "n1"), fsmI64("incarnation", 1), fsmStr("data_addr", "n1:1"), fsmStr("client_addr", ""))
	id16 := stateClusterID()
	_, err := empty.ApplyCommand(Command{Version: CommandVersion, Kind: CommandBootstrap, Body: fsmDoc(fsmBin("cluster_id", id16[:]), fsmVersion(),
		vendorbsoncore.AppendDocumentElement(nil, "self", selfDoc))})
	fsmWantCode(t, err, CodeInvalidArgument)
}
