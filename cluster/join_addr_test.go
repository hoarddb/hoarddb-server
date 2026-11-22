package cluster

import (
	"io"
	"net"
	"strings"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// TestJoinRefusesAnAddressAnotherMemberHolds: [GUARD two ids on one committed address are refused,
// and the refused joiner is taken back out of the Raft configuration, not left a phantom nonvoter].
func TestJoinRefusesAnAddressAnotherMemberHolds(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	cfg := quickRaft(t, t.TempDir(), "n2")
	cfg.Bootstrap = false
	openOrFail(t, cfg)
	_, err := leader.Join(JoinRequest{
		Member:     Member{ID: "n2", Incarnation: 1, DataAddr: "n1:1"},
		RaftAddr:   cfg.Listener.Addr().String(),
		PlacingSet: make([]byte, 1024),
	})
	if err == nil || !strings.Contains(err.Error(), "n1") {
		t.Fatalf("a join at the address member n1 holds gave %v, want a refusal naming n1", err)
	}
	future := leader.raft.GetConfiguration()
	if future.Error() != nil {
		t.Fatal(future.Error())
	}
	if servers := future.Configuration().Servers; len(servers) != 1 {
		t.Fatalf("the refused joiner stayed in the Raft configuration: %v", servers)
	}
}

// TestIDAtNamesTheMemberAtAnAddress: [GUARD a dial learns whose key to demand from the committed
// address, and from the Raft configuration for a joiner the leader has added but not committed; an
// unknown address names no one].
func TestIDAtNamesTheMemberAtAnAddress(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	if id, ok, err := leader.IDAt("n1:1"); err != nil || !ok || id != "n1" {
		t.Fatalf("IDAt(n1:1) = %q, %v, %v", id, ok, err)
	}
	if _, ok, err := leader.IDAt("n9:1"); err != nil || ok {
		t.Fatal("an address no member holds named someone")
	}
	_, cfg := joinSecond(t, leader)
	addr := cfg.Listener.Addr().String()
	if id, ok, err := leader.IDAt(addr); err != nil || !ok || id != "n2" {
		t.Fatalf("IDAt(%s) = %q, %v, %v after the join", addr, id, ok, err)
	}
}

// TestAPhantomNonvoterIsDialledUnderItsOwnPin: a leader that added a nonvoter and died before the
// member row committed leaves the server in the persisted Raft configuration; the next process of
// that seat resolves the address to the joiner's id from it, so the dial is held to the joiner's
// own key and not to "any pinned key". [BREAK: IDAt reads the member rows only].
func TestAPhantomNonvoterIsDialledUnderItsOwnPin(t *testing.T) {
	dir := t.TempDir()
	first := quickRaft(t, dir, "n1")
	leader, err := OpenRaft(first)
	if err != nil {
		t.Fatal(err)
	}
	phantom := quickRaft(t, t.TempDir(), "n2")
	phantom.Bootstrap = false
	openOrFail(t, phantom)
	addr := phantom.Listener.Addr().String()
	if err := leader.raft.AddNonvoter("n2", hraft.ServerAddress(addr), 0, raftApplyTimeout).Error(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}

	second := quickRaft(t, dir, "n1")
	second.Bootstrap = false
	restarted := openOrFail(t, second)
	if len(restarted.State().Members) > 1 {
		t.Fatalf("the phantom has a member row: %+v", restarted.State().Members)
	}
	if id, ok, err := restarted.IDAt(addr); err != nil || !ok || id != "n2" {
		t.Fatalf("IDAt(%s) = %q, %v, %v: a phantom nonvoter in the persisted configuration must name its own id", addr, id, ok, err)
	}
}

// forward relays every connection it accepts to target, so a test can move a member's address.
func forward(t *testing.T, target string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			in, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				out, err := net.Dial("tcp", target)
				if err != nil {
					_ = in.Close()
					return
				}
				go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
				_, _ = io.Copy(in, out)
				_ = in.Close()
			}()
		}
	}()
	return listener.Addr().String()
}

// TestPromoteRePointsAMovedDataAddr: a member whose committed address moved is promoted at the new
// address, not the one the Raft configuration still holds. [BREAK: Promote re-adds the configured address].
func TestPromoteRePointsAMovedDataAddr(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	_, cfg := joinSecond(t, leader)
	moved := forward(t, cfg.Listener.Addr().String())
	body := RegisterMemberBody(Member{ID: "n2", Incarnation: 1, DataAddr: moved}, make([]byte, 1024), BuildStateRange())
	if _, err := leader.propose(CommandRegisterMember, body); err != nil {
		t.Fatalf("moving the committed address: %v", err)
	}
	if _, err := leader.Promote("n2"); err != nil {
		t.Fatalf("promoting at the moved address: %v", err)
	}
	conf, err := leader.configuration()
	if err != nil {
		t.Fatal(err)
	}
	for _, server := range conf.Servers {
		if server.ID == "n2" && string(server.Address) != moved {
			t.Fatalf("n2 is configured at %s, the committed address is %s", server.Address, moved)
		}
	}
}
