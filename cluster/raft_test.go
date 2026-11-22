package cluster

import (
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	"github.com/hoarddb/hoarddb-server/transport"
)

// quickRaft is a test configuration with short timers: the library's
// defaults (one second) would make every test slow.
func quickRaft(t *testing.T, dir, id string) RaftConfig {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return RaftConfig{
		NodeID: id, Dir: dir, Listener: listener, DataAddr: id + ":1", Incarnation: 1,
		Bootstrap: true, Logger: hclog.NewNullLogger(),
		HeartbeatTimeout: 50 * time.Millisecond, ElectionTimeout: 50 * time.Millisecond, LeaderLease: 50 * time.Millisecond,
	}
}

// openOrFail opens cfg. A node opened to join is admitted at once: the tests of this package join it
// through Raft.Join, which asks no identity, so nothing else would end the BUG-0870 guard's hold. The
// guard itself is tested on OpenRaft directly (openHeld, raft_admission_test.go) and through the
// identity channel in cluster/harness.
func openOrFail(t *testing.T, cfg RaftConfig) *Raft {
	t.Helper()
	r := openHeld(t, cfg)
	if !cfg.Bootstrap {
		r.Admit()
	}
	return r
}

// openHeld opens cfg and leaves a node opened to join with no Raft state held.
func openHeld(t *testing.T, cfg RaftConfig) *Raft {
	t.Helper()
	r, err := OpenRaft(cfg)
	if err != nil {
		_ = cfg.Listener.Close()
		t.Fatalf("OpenRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// noRaftGoroutines fails if a goroutine of the library, or of our stream
// layer, outlives Close.
func noRaftGoroutines(t *testing.T) {
	t.Helper()
	// A Raft goroutine ends only when the library resolves its last future, and a loaded host
	// (the gate runs the other packages beside this one) can take well over a few seconds. The
	// wait polls, so a quiet host returns at once; a goroutine that never ends still fails here.
	deadline := time.Now().Add(15 * time.Second)
	for {
		stacks := make([]byte, 1<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		leaked := ""
		for _, g := range strings.Split(string(stacks), "\n\n") {
			if strings.Contains(g, "hashicorp/raft") || strings.Contains(g, "cluster.(*streamLayer)") {
				if !strings.Contains(g, "testing.tRunner") {
					leaked = g
				}
			}
		}
		if leaked == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a Raft goroutine outlived Close:\n%s", leaked)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForLibraryReader waits until the library's connection reader is
// running, so the test that follows holds a goroutine it must see ended.
func waitForLibraryReader(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stacks := make([]byte, 1<<20)
		if strings.Contains(string(stacks[:runtime.Stack(stacks, true)]), "NetworkTransport).handleCommand") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the library never started reading the held connection")
}

func createDatabase(t *testing.T, r *Raft, name string, id int64) Result {
	t.Helper()
	res, err := r.Propose(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq),
		Body: fsmCreateDatabaseBody(name, id, 1)})
	if err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	return res
}

func TestSingleNodeOpensBootstrapsProposesAndRestarts(t *testing.T) {
	dir := t.TempDir()
	r := openOrFail(t, quickRaft(t, dir, "n1"))
	st := r.State()
	if st.Epoch != 1 || st.StateSeq != 1 || len(st.Members) != 1 || st.Members[0].ID != "n1" || st.ClusterID == [16]byte{} {
		t.Fatalf("after bootstrap: %+v", st)
	}
	if got := createDatabase(t, r, "app", 1); got.StateSeq != 2 || got.EpochMoved {
		t.Fatalf("create database result %+v", got)
	}
	if err := r.raft.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	createDatabase(t, r, "logs", 2)
	want := r.State()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	noRaftGoroutines(t)

	again := openOrFail(t, quickRaft(t, dir, "n1"))
	// The restart restores the snapshot and replays the log after it.
	deadline := time.Now().Add(5 * time.Second)
	for again.State().StateSeq != want.StateSeq && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got, wantRaw, gotRaw := again.State(), stateMustEncode(t, want), stateMustEncode(t, again.State())
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("state after restart differs:\n got %+v\nwant %+v", got, want)
	}
}

func TestBootstrapIsGuardedTwice(t *testing.T) {
	// Guard one: existing state. A restart that asks for a bootstrap again
	// must neither mint a second cluster id nor fail.
	dir := t.TempDir()
	r := openOrFail(t, quickRaft(t, dir, "n1"))
	first := r.State()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := quickRaft(t, dir, "n1")
	r = openOrFail(t, cfg)
	deadline := time.Now().Add(5 * time.Second)
	for r.State().StateSeq == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := r.State(); got.ClusterID != first.ClusterID || got.StateSeq != first.StateSeq || got.Epoch != 1 {
		t.Fatalf("a restart re-bootstrapped: first %+v, now %+v", first, got)
	}

	// Guard two: a seed list never bootstraps, and neither does a node that
	// was told to join. They open, hold no state and elect no leader.
	for name, mod := range map[string]func(*RaftConfig){
		"seed list": func(c *RaftConfig) { c.SeedNodes = []string{"n9@seed.invalid:7000"} },
		"join":      func(c *RaftConfig) { c.Bootstrap = false },
	} {
		cfg := quickRaft(t, t.TempDir(), "n2")
		mod(&cfg)
		idle := openOrFail(t, cfg)
		time.Sleep(300 * time.Millisecond)
		if st := idle.State(); st.StateSeq != 0 || len(st.Members) != 0 {
			t.Errorf("%s: the node bootstrapped: %+v", name, st)
		}
		if _, err := idle.Propose(Command{Version: CommandVersion, Kind: CommandBootstrap, Body: fsmBootstrapBody("n2")}); err == nil {
			t.Errorf("%s: a node with no cluster accepted a command", name)
		}
	}
}

// joinSecond opens a second node that was told to join and registers it
// through the leader.
func joinSecond(t *testing.T, leader *Raft) (*Raft, RaftConfig) {
	t.Helper()
	cfg := quickRaft(t, t.TempDir(), "n2")
	cfg.Bootstrap = false
	second := openOrFail(t, cfg)
	if _, err := leader.Join(JoinRequest{
		Member:     Member{ID: "n2", Incarnation: 1, DataAddr: cfg.Listener.Addr().String()},
		RaftAddr:   cfg.Listener.Addr().String(),
		PlacingSet: make([]byte, 1024),
	}); err != nil {
		t.Fatalf("join: %v", err)
	}
	return second, cfg
}

func TestPromotionCommitsOneSetVoters(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second, _ := joinSecond(t, leader)
	before := leader.State()
	if len(before.Voters) != 1 || before.Members[1].Suffrage != "nonvoter" {
		t.Fatalf("a joiner must not vote before promotion: %+v", before)
	}
	res, err := leader.Promote("n2")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	after := leader.State()
	if after.StateSeq != before.StateSeq+1 || res.StateSeq != after.StateSeq {
		t.Errorf("promotion must commit exactly one entry: state_seq %d -> %d", before.StateSeq, after.StateSeq)
	}
	if after.Epoch != before.Epoch || res.EpochMoved {
		t.Errorf("promotion moves no slot, so the epoch stays %d: now %d", before.Epoch, after.Epoch)
	}
	if len(after.Voters) != 2 || after.Voters[1] != "n2" {
		t.Errorf("voters = %v, want n1 and n2", after.Voters)
	}
	deadline := time.Now().Add(5 * time.Second)
	for second.State().StateSeq != after.StateSeq && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if second.State().StateSeq != after.StateSeq {
		t.Errorf("the follower did not apply the promotion: %+v", second.State())
	}

	removed, err := leader.RemoveServer("n2")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := leader.State(); len(got.Voters) != 1 || removed.StateSeq != after.StateSeq+1 {
		t.Errorf("RemoveServer must commit one SET_VOTERS{voting:false}: %+v", got)
	}
}

func TestPromotingAnUnknownServerNamesTheFix(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	_, err := leader.Promote("ghost")
	if err == nil || !strings.Contains(err.Error(), "ghost") || !strings.Contains(err.Error(), "join") {
		t.Fatalf("want an error that names the node and the join verb, got %v", err)
	}
}

func TestNoGoroutineOutlivesClose(t *testing.T) {
	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second, _ := joinSecond(t, leader)
	// A peer holds a live connection to the leader and never closes it, as a
	// pooled connection of a peer that is still running would. The library
	// keeps a reader goroutine for it until the connection ends, so Close
	// must end it: the layer tracks every connection for that reason.
	held := dialWithHandshake(t, leader.cfg.Listener.Addr().String(), transport.Handshake{
		Channel: transport.HandshakeChannelRaft, ClusterID: leader.State().ClusterID, NodeID: "n2", Incarnation: 1,
	})
	if _, err := held.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	waitForLibraryReader(t)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Close(); err != nil {
		t.Fatalf("a second Close must be harmless: %v", err)
	}
	noRaftGoroutines(t)
}

// streamUnderTest listens on loopback and knows node n2 at incarnation 2.
func streamUnderTest(t *testing.T) (*streamLayer, string) {
	t.Helper()
	return streamUnderTestLimits(t, RaftLimits{})
}

// streamUnderTestLimits is streamUnderTest with explicit Raft channel limits.
func streamUnderTestLimits(t *testing.T, limits RaftLimits) (*streamLayer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	local := localIdentity{NodeID: "n1", Incarnation: 1, ClusterID: [16]byte{9}, Epoch: 3, StateSeq: 8}
	layer := newStreamLayer(listener, streamHooks{
		local: func() localIdentity { return local },
		committedIncarnation: func(id string) (uint64, bool) {
			if id == "n2" {
				return 2, true
			}
			return 0, false
		},
	}, limits, hclog.NewNullLogger())
	layer.start()
	t.Cleanup(func() { _ = layer.Close() })
	return layer, listener.Addr().String()
}

func dialWithHandshake(t *testing.T, addr string, h transport.Handshake) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw, err := h.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestStaleIncarnationIsRefusedAtHandshake(t *testing.T) {
	layer, addr := streamUnderTest(t)
	accepted := make(chan net.Conn, 4)
	go func() {
		// Stands in for the library, which closes every connection it was handed when the layer
		// closes; the layer's Close waits for that.
		var held []net.Conn
		for {
			conn, err := layer.Accept()
			if err != nil {
				for _, c := range held {
					_ = c.Close()
				}
				return
			}
			held = append(held, conn)
			accepted <- conn
		}
	}()
	hello := func(node string, incarnation uint64) transport.Handshake {
		return transport.Handshake{Channel: transport.HandshakeChannelRaft, ClusterID: [16]byte{9}, NodeID: node, Incarnation: incarnation}
	}

	stale := dialWithHandshake(t, addr, hello("n2", 1))
	_ = stale.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := stale.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !strings.Contains(err.Error(), "reset") {
		t.Fatalf("a stale incarnation must be closed at the handshake, read gave %v", err)
	}
	select {
	case <-accepted:
		t.Fatal("a stale incarnation was handed to Raft")
	case <-time.After(150 * time.Millisecond):
	}

	for name, h := range map[string]transport.Handshake{
		"equal incarnation": hello("n2", 2),
		"newer":             hello("n2", 5),
		"unknown node":      hello("n7", 1),
	} {
		conn := dialWithHandshake(t, addr, h)
		if _, err := conn.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-accepted:
			one := make([]byte, 1)
			if _, err := io.ReadFull(got, one); err != nil || one[0] != 'x' {
				t.Errorf("%s: handshake bytes leaked into the Raft stream (%q, %v)", name, one, err)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("%s: refused", name)
		}
	}

	for name, h := range map[string]transport.Handshake{
		"other cluster": {Channel: transport.HandshakeChannelRaft, ClusterID: [16]byte{8}, NodeID: "n2", Incarnation: 2},
		"other channel": {Channel: transport.HandshakeChannelData, ClusterID: [16]byte{9}, NodeID: "n2", Incarnation: 2},
	} {
		conn := dialWithHandshake(t, addr, h)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestStreamDialSendsTheLocalHandshake(t *testing.T) {
	layer, _ := streamUnderTest(t)
	peer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	got := make(chan transport.Handshake, 1)
	go func() {
		conn, err := peer.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		h, _ := transport.ReadHandshake(conn)
		got <- h
	}()
	conn, err := layer.Dial(hraft.ServerAddress(peer.Addr().String()), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	select {
	case h := <-got:
		if h.NodeID != "n1" || h.Incarnation != 1 || h.Epoch != 3 || h.StateSeq != 8 || h.ClusterID != [16]byte{9} || h.Channel != transport.HandshakeChannelRaft {
			t.Fatalf("handshake = %+v", h)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no handshake arrived")
	}
}

// TestStreamZeroClusterIDIsAcceptedOnlyToLearn pins the rule: a side that does
// not know its cluster id yet (zero) is accepted, so a joiner can learn it
// from the log; two ids that are both known and differ are refused.
func TestStreamZeroClusterIDIsAcceptedOnlyToLearn(t *testing.T) {
	known, other := [16]byte{9}, [16]byte{8}
	check := func(local, remote [16]byte) error {
		layer := &streamLayer{hooks: streamHooks{
			local:                func() localIdentity { return localIdentity{ClusterID: local} },
			committedIncarnation: func(string) (uint64, bool) { return 0, false },
		}}
		return layer.checkInbound(transport.Handshake{Channel: transport.HandshakeChannelRaft, ClusterID: remote, NodeID: "n2"})
	}
	if err := check(known, [16]byte{}); err != nil {
		t.Errorf("a joiner that has not learned its cluster id was refused: %v", err)
	}
	if err := check([16]byte{}, known); err != nil {
		t.Errorf("a node that has not learned its own cluster id refused a member: %v", err)
	}
	if err := check([16]byte{}, [16]byte{}); err != nil {
		t.Errorf("two fresh nodes were refused: %v", err)
	}
	err := check(known, other)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != CodeClusterMismatch {
		t.Fatalf("two known, different cluster ids must be refused CLUSTER_MISMATCH, got %v", err)
	}
	if !strings.Contains(refusal.Message, `"n2"`) || !strings.Contains(refusal.Message, "join") {
		t.Errorf("the refusal must name the node and the fix: %q", refusal.Message)
	}
}

// wildcardRaft is quickRaft with the listener bound to the wildcard address, the setup
// config/cluster.go demands an advertise address for.
func wildcardRaft(t *testing.T, id, advertise string) RaftConfig {
	t.Helper()
	cfg := quickRaft(t, t.TempDir(), id)
	if err := cfg.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listener, cfg.DataAddr = listener, advertise
	return cfg
}

// BUG-0761: the library dials its own configuration, so the bootstrap voter must be seated at
// the advertised address, not at the wildcard the listener is bound to.
func TestTheBootstrapSeatIsTheAdvertisedAddress(t *testing.T) {
	const advertised = "n1.example.test:4433"
	r := openOrFail(t, wildcardRaft(t, "n1", advertised))
	conf := r.raft.GetConfiguration()
	if err := conf.Error(); err != nil {
		t.Fatal(err)
	}
	servers := conf.Configuration().Servers
	if len(servers) != 1 || string(servers[0].Address) != advertised {
		t.Fatalf("the bootstrap seat is %+v, want one voter at %q (the listener is %s)", servers, advertised, r.cfg.Listener.Addr())
	}
}

// BUG-0761: a wildcard seat can never be dialled, so a node that cannot name an address
// refuses to start and says which setting fixes it.
func TestAWildcardSeatIsRefusedWithTheFixNamed(t *testing.T) {
	for name, bootstrap := range map[string]bool{"bootstrap": true, "join": false} {
		cfg := wildcardRaft(t, "n1", "")
		cfg.Bootstrap = bootstrap
		r, err := OpenRaft(cfg)
		if err == nil {
			_ = r.Close()
			t.Fatalf("%s: a node with only a wildcard address opened", name)
		}
		_ = cfg.Listener.Close()
		if !strings.Contains(err.Error(), "HOARDDB_CLUSTER_ADVERTISE") {
			t.Errorf("%s: the refusal does not name the setting: %v", name, err)
		}
	}
}
