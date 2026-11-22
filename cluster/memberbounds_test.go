package cluster

import (
	"bytes"
	"strings"
	"testing"

	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// TestC5AMemberIDAndDataAddrAreBounded: [GUARD a REGISTER_MEMBER or BOOTSTRAP with a member id or data address over
// 256 bytes is refused and changes nothing, so no committed row can make a TOPOLOGY reply unencodable].
func TestC5AMemberIDAndDataAddrAreBounded(t *testing.T) {
	long := strings.Repeat("a", MaxMemberTextBytes+1)
	exact := strings.Repeat("b", MaxMemberTextBytes)
	st := fsmCluster(t)
	fsmApply(t, &st, CommandRegisterMember, fsmRegisterBody(exact, 1, exact, false)) // the control: the bound itself is accepted
	for name, body := range map[string][]byte{
		"data_addr": fsmRegisterBody("n7", 1, long, false),
		"id":        fsmRegisterBody(long, 1, "n7:1", false),
	} {
		before := fsmEncoded(t, st)
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandRegisterMember, BaseSeq: uint64(st.StateSeq), Body: body})
		fsmWantCode(t, err, CodeInvalidArgument)
		if !bytes.Equal(before, fsmEncoded(t, st)) {
			t.Fatalf("%s: a refused join changed the state", name)
		}
	}
	var empty ClusterState
	id16 := stateClusterID()
	for name, self := range map[string][]byte{
		"data_addr": fsmMemberDoc("n1", 1, long),
		"id":        fsmMemberDoc(long, 1, "n1:1"),
	} {
		_, err := empty.ApplyCommand(Command{Version: CommandVersion, Kind: CommandBootstrap,
			Body: fsmDoc(fsmBin("cluster_id", id16[:]), fsmVersion(), vendorbsoncore.AppendDocumentElement(nil, "self", self))})
		if err == nil {
			t.Errorf("a bootstrap with an over-long %s was accepted", name)
		}
	}
}

// TestC5TheStateCodecsBoundMemberTextAndSuffrage: [GUARD EncodeClusterState and DecodeClusterState refuse an id or
// data address over 256 bytes, a client address over the bound, and any suffrage but voter or nonvoter].
func TestC5TheStateCodecsBoundMemberTextAndSuffrage(t *testing.T) {
	long := strings.Repeat("a", MaxMemberTextBytes+1)
	mutations := map[string]func(*ClusterState){
		"id":              func(s *ClusterState) { s.Members[0].ID = long; s.Voters = nil },
		"data_addr":       func(s *ClusterState) { s.Members[0].DataAddr = long },
		"client_addr":     func(s *ClusterState) { s.Members[0].ClientAddr = strings.Repeat("a", MaxClientAddrBytes+1) },
		"suffrage Voter":  func(s *ClusterState) { s.Members[0].Suffrage = "Voter" },
		"suffrage empty":  func(s *ClusterState) { s.Members[0].Suffrage = "" },
		"suffrage member": func(s *ClusterState) { s.Members[0].Suffrage = "witness" },
	}
	for name, mutate := range mutations {
		st := stateFull()
		mutate(&st)
		if _, err := EncodeClusterState(st); err == nil {
			t.Errorf("%s: EncodeClusterState accepted it", name)
		}
		_, err := DecodeClusterState(encodeClusterStateInGivenOrder(st))
		if err == nil {
			t.Errorf("%s: DecodeClusterState accepted it", name)
		}
	}
	exact := stateFull()
	exact.Members[0].DataAddr = strings.Repeat("a", MaxMemberTextBytes)
	if _, err := EncodeClusterState(exact); err != nil {
		t.Errorf("the bound itself was refused: %v", err)
	}
}

// TestC5TheBoundMessageIsTrue: [GUARD the refusal of a long address says what fits: a host name of at most 250
// characters, a colon and a port, which is what 256 bytes holds].
func TestC5TheBoundMessageIsTrue(t *testing.T) {
	st := stateFull()
	st.Members[0].DataAddr = strings.Repeat("a", MaxMemberTextBytes+1)
	_, err := EncodeClusterState(st)
	if err == nil || !strings.Contains(err.Error(), "250 characters") {
		t.Fatalf("the refusal %v does not say what fits", err)
	}
	if host := strings.Repeat("a", MaxMemberTextBytes-len(":65535")); len(host)+len(":65535") != MaxMemberTextBytes {
		t.Fatalf("a %d-character host plus :65535 is not %d bytes", len(host), MaxMemberTextBytes)
	}
}
