package cluster

import (
	"bytes"
	"testing"

	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func c5RegisterWith(id, dataAddr, clientAddr string) []byte {
	return fsmDoc(fsmStr("id", id), fsmI64("incarnation", 1), fsmStr("data_addr", dataAddr), fsmStr("client_addr", clientAddr),
		fsmBool("witness", false), fsmBin("placing_set", fsmBitsAt(1)), fsmLevels())
}

// TestC5AClientAddrIsCheckedAtTheFSM: [GUARD REGISTER_MEMBER refuses a client address that is not host:port with a
// port 1-65535, one with a scheme, user info, whitespace or NUL, an unspecified or zoned host, and one on any host but
// the member's data address host, because a driver sends its password to whatever the row says; a different port on
// the same host is accepted].
func TestC5AClientAddrIsCheckedAtTheFSM(t *testing.T) {
	refused := []struct{ name, data, client string }{
		{"no port", "n5:1", "n5"},
		{"port 0", "n5:1", "n5:0"},
		{"port 65536", "n5:1", "n5:65536"},
		{"port a word", "n5:1", "n5:http"},
		{"a scheme", "n5:1", "https://n5:7434"},
		{"user info", "n5:1", "user@n5:7434"},
		{"a space", "n5:1", "n5 :7434"},
		{"a tab", "n5:1", "n5\t:7434"},
		{"a NUL", "n5:1", "n5\x00:7434"},
		{"no host", "n5:1", ":7434"},
		{"another host", "n5:1", "other.example:7434"},
		{"loopback on a remote node", "n5:1", "127.0.0.1:7434"},
		{"4-in-6 loopback on a remote node", "n5:1", "[::ffff:127.0.0.1]:7434"},
		{"the wildcard", "0.0.0.0:4433", "0.0.0.0:7434"},
		{"the v6 wildcard", "[::]:4433", "[::]:7434"},
		{"a zone", "[fe80::1%eth0]:4433", "[fe80::1%eth0]:7434"},
		{"a data address that is not host:port", "n5", "n5:7434"},
	}
	for _, c := range refused {
		st := fsmCluster(t)
		before := fsmEncoded(t, st)
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandRegisterMember, BaseSeq: uint64(st.StateSeq), Body: c5RegisterWith("n5", c.data, c.client)})
		if err == nil {
			t.Errorf("%s: client %q for data %q was accepted", c.name, c.client, c.data)
			continue
		}
		fsmWantCode(t, err, CodeInvalidArgument)
		if !bytes.Equal(before, fsmEncoded(t, st)) {
			t.Errorf("%s: a refused command changed the state", c.name)
		}
	}
	for _, c := range []struct{ name, data, client string }{
		{"another port, same host", "n5:1", "n5:7434"},
		{"host case", "n5:1", "N5:7434"},
		{"the top port", "n5:1", "n5:65535"},
		{"loopback with a loopback data address", "127.0.0.1:4433", "127.0.0.1:7434"},
		{"a v6 address", "[2001:db8::1]:4433", "[2001:db8::1]:7434"},
	} {
		st := fsmCluster(t)
		fsmApply(t, &st, CommandRegisterMember, c5RegisterWith("n5", c.data, c.client))
		if row, _ := fsmMember(st, "n5"); row.ClientAddr != c.client {
			t.Errorf("%s: client address %q was not kept (%q)", c.name, c.client, row.ClientAddr)
		}
	}
}

// TestC5AReRegisterCannotRedirectTheClientAddr: [GUARD a REGISTER_MEMBER of an existing member at the same incarnation
// cannot point its client address at another host, and a refused one leaves the row as it was].
func TestC5AReRegisterCannotRedirectTheClientAddr(t *testing.T) {
	st := fsmCluster(t)
	fsmApply(t, &st, CommandRegisterMember, c5RegisterWith("n2", "n2:1", "n2:7434"))
	before := fsmEncoded(t, st)
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandRegisterMember, BaseSeq: uint64(st.StateSeq), Body: c5RegisterWith("n2", "n2:1", "elsewhere.example:7434")})
	fsmWantCode(t, err, CodeInvalidArgument)
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a refused re-register changed the state")
	}
}

// TestC5ABootstrapChecksTheClientAddrToo: [GUARD BOOTSTRAP applies the same rule as REGISTER_MEMBER to self].
func TestC5ABootstrapChecksTheClientAddrToo(t *testing.T) {
	id16 := stateClusterID()
	bootstrap := func(client string) error {
		var st ClusterState
		self := fsmDoc(fsmStr("id", "n1"), fsmI64("incarnation", 1), fsmStr("data_addr", "n1:1"), fsmStr("client_addr", client))
		_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandBootstrap,
			Body: fsmDoc(fsmBin("cluster_id", id16[:]), fsmVersion(), vendorbsoncore.AppendDocumentElement(nil, "self", self))})
		return err
	}
	if err := bootstrap("n1:7434"); err != nil {
		t.Fatalf("a client address on the node's own host was refused: %v", err)
	}
	for _, bad := range []string{"other.example:7434", "n1", "127.0.0.1:7434", "https://n1:7434"} {
		if bootstrap(bad) == nil {
			t.Errorf("bootstrap accepted client address %q", bad)
		}
	}
}
