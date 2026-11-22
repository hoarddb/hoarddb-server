package cluster

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hoarddb/hoarddb-server/transport"
)

func rangeHello(channel byte, node string, r StateRange) transport.Handshake {
	return r.Stamp(transport.Handshake{Channel: channel, NodeID: node, Incarnation: 1})
}

// openRangeRaft opens a node whose log the test can read, declaring declared (nil: the build's).
func openRangeRaft(t *testing.T, declared *StateRange) (*Raft, *lockedLog) {
	t.Helper()
	logs := &lockedLog{}
	cfg := quickRaft(t, t.TempDir(), "n1")
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: logs})
	cfg.StateRange = declared
	return openOrFail(t, cfg), logs
}

// lockedLog is a log sink the test reads while the acceptor goroutines write it.
type lockedLog struct {
	mu   sync.Mutex
	text bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.Write(p)
}

// String returns what was written so far.
func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.String()
}

func TestAPeerWhoseRangeDoesNotContainTheClusterVersionIsRefusedOnEveryChannelButIdentity(t *testing.T) {
	cluster := StateRange{Min: 3, Max: 3, Writes: 3}
	r, logs := openRangeRaft(t, &cluster)
	peers := map[string]StateRange{
		"level 0":      {},
		"below":        {Min: 2, Max: 2, Writes: 2},
		"above":        {Min: 4, Max: 4, Writes: 4},
		"writes above": {Min: 2, Max: 3, Writes: 4},
	}
	channels := []byte{transport.HandshakeChannelData, transport.HandshakeChannelControl, transport.HandshakeChannelSeed, transport.HandshakeChannelRaft}
	for name, peer := range peers {
		for _, channel := range channels {
			word := transport.Channel(channel).String()
			err := r.AdmitPeer(rangeHello(channel, "n2", peer), false)
			if err == nil {
				t.Fatalf("%s peer admitted on the %s channel", name, word)
			}
			want := fmt.Sprintf("refused %s connection from node \"n2\"", word)
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%s peer on %s: %q does not contain %q", name, word, err, want)
			}
		}
		if err := r.AdmitIdentity(rangeHello(transport.HandshakeChannelIdentity, "n2", peer)); err != nil {
			t.Fatalf("%s peer refused on the identity channel, which is exempt: %v", name, err)
		}
	}
	if !strings.Contains(logs.String(), "refused data connection from node") {
		t.Fatalf("the refusal was not logged at WARN: %q", logs.String())
	}
	if err := r.AdmitPeer(rangeHello(transport.HandshakeChannelData, "n2", cluster), false); err != nil {
		t.Fatalf("a peer at the cluster's own range was refused: %v", err)
	}
}

func TestTheRangeRefusalNamesTheReleaseWhenKnownAndTheFallbackWhenNot(t *testing.T) {
	cluster := StateRange{Min: 1, Max: 1, Writes: 1}
	peer := StateRange{Min: 0, Max: 0, Writes: 5}
	saved := stateVersions
	t.Cleanup(func() { stateVersions = saved })
	// The build's own table records a release for level 1, so the fallback is shown on a stub.
	stateVersions = []StateVersionRow{{Level: 1, FirstRelease: ""}}
	unknown := StateRangeRefusal("data", "n2", cluster, peer)
	if !strings.Contains(unknown, "this build records no release for version 1; run hoarddb-server -v on a cluster member and start n2 on that build or later") {
		t.Fatalf("fallback text missing: %s", unknown)
	}
	stateVersions = []StateVersionRow{{Level: 1, FirstRelease: "1.2.0"}}
	known := StateRangeRefusal("data", "n2", cluster, peer)
	for _, want := range []string{"(first released in 1.2.0)", "Upgrade n2 to 1.2.0 or later;", "it speaks state versions 0-0 (writes 5)", "this cluster runs version 1"} {
		if !strings.Contains(known, want) {
			t.Fatalf("%q missing from %s", want, known)
		}
	}
	zero := StateRangeRefusal("raft", "n2", cluster, StateRange{})
	if !strings.Contains(zero, "it sends no state version, so it is a build from before this check; upgrade it to 1.2.0 or later") {
		t.Fatalf("level 0 text: %s", zero)
	}
}

func TestTheRefusedPeersLedgerIsBoundedByMembers(t *testing.T) {
	r, _ := openRangeRaft(t, nil)
	bad := StateRange{Min: 9, Max: 9, Writes: 9}
	// openRangeRaft commits no members, and the ledger's bound is at least one row,
	// so a fresh node holds one row whoever knocks.
	for i := 0; i < 20; i++ {
		if err := r.AdmitPeer(rangeHello(transport.HandshakeChannelData, fmt.Sprintf("x%d", i), bad), false); err == nil {
			t.Fatal("a peer at another version was admitted")
		}
	}
	rows := r.RefusedPeers()
	if len(rows) != 1 {
		t.Fatalf("the ledger holds %d rows on a one-member cluster, want 1: %+v", len(rows), rows)
	}
	if rows[0].Range != bad || rows[0].Channel != "data" {
		t.Fatalf("row %+v lost what it records", rows[0])
	}
}

func TestTheRefusalIsLoggedOncePerMinutePerMember(t *testing.T) {
	r, logs := openRangeRaft(t, nil)
	clock := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return clock }
	bad := rangeHello(transport.HandshakeChannelData, "n2", StateRange{Min: 9, Max: 9, Writes: 9})
	for i := 0; i < 5; i++ {
		_ = r.AdmitPeer(bad, false)
		clock = clock.Add(10 * time.Second)
	}
	if got := strings.Count(logs.String(), "refused data connection from node"); got != 1 {
		t.Fatalf("%d WARN lines in 50 seconds, want 1:\n%s", got, logs.String())
	}
	clock = clock.Add(time.Minute)
	_ = r.AdmitPeer(bad, false)
	if got := strings.Count(logs.String(), "refused data connection from node"); got != 2 {
		t.Fatalf("%d WARN lines after the minute, want 2", got)
	}
}

func TestTheRaftListenerRefusesALevelZeroPeerWithoutPinnedKeys(t *testing.T) {
	r, _ := openRangeRaft(t, nil)
	dialWithHandshake(t, r.cfg.Listener.Addr().String(), rangeHello(transport.HandshakeChannelRaft, "n2", StateRange{}))
	deadline := time.Now().Add(5 * time.Second)
	for len(r.RefusedPeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a handshake with no state range was not refused on the Raft listener")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := r.RefusedPeers()[0]; got.NodeID != "n2" || got.Channel != "raft" || got.Range != (StateRange{}) {
		t.Fatalf("ledger row %+v", got)
	}
}

// TestTheStreamLayerAdmitsNothingBeforeItIsStarted: OpenRaft assigns the layer to the Raft before
// the accept loop runs, so the admit hook (Raft.AdmitPeer) never reads a nil stream. Scratch
// proof of the break: start the loop inside newStreamLayer again and this test reads a hook call
// before start (and `go test -race` on TestAJoinOnAKnownJoiningRowAlsoRunsTheRangeCheck reports the race).
func TestTheStreamLayerAdmitsNothingBeforeItIsStarted(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int32
	layer := newStreamLayer(listener, streamHooks{
		local:                func() localIdentity { return localIdentity{NodeID: "n1", Range: BuildStateRange()} },
		committedIncarnation: func(string) (uint64, bool) { return 0, false },
		admit:                func(transport.Handshake) error { admitted.Add(1); return nil },
	}, RaftLimits{}, hclog.NewNullLogger())
	t.Cleanup(func() { _ = layer.Close() })
	conn := dialWithHandshake(t, listener.Addr().String(), rangeHello(transport.HandshakeChannelRaft, "n2", BuildStateRange()))
	_ = conn
	time.Sleep(200 * time.Millisecond)
	if admitted.Load() != 0 {
		t.Fatal("the admit hook ran before the layer was started")
	}
	layer.start()
	deadline := time.Now().Add(5 * time.Second)
	for admitted.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the admit hook never ran after start")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTheLedgerForgetsAMemberThatLaterConnectsWithinRange(t *testing.T) {
	r, _ := openRangeRaft(t, nil)
	bad := rangeHello(transport.HandshakeChannelData, "n2", StateRange{Min: 9, Max: 9, Writes: 9})
	good := rangeHello(transport.HandshakeChannelData, "n2", BuildStateRange())
	if err := r.AdmitPeer(bad, false); err == nil {
		t.Fatal("a peer at another version was admitted")
	}
	if len(r.RefusedPeers()) != 1 {
		t.Fatalf("ledger %+v, want one row", r.RefusedPeers())
	}
	if err := r.AdmitPeer(good, false); err != nil {
		t.Fatal(err)
	}
	if rows := r.RefusedPeers(); len(rows) != 0 {
		t.Fatalf("n2 now connects within range and is still listed as refused: %+v", rows)
	}
}

func TestTheRangeRefusalOfANewerPeerDoesNotTellItToUpgrade(t *testing.T) {
	// A later-release acceptor at cluster version 1 (range 1-2) meets a peer whose oldest version
	// is above it: the peer writes inside this build's range, so it reaches the "newer" branch, not
	// the writes-above-Max one that a release-1 acceptor shows.
	cluster := StateRange{Min: 1, Max: 2, Writes: 1}
	newer := StateRange{Min: 2, Max: 3, Writes: 2}
	saved := stateVersions
	t.Cleanup(func() { stateVersions = saved })
	stateVersions = []StateVersionRow{{Level: 1, FirstRelease: "1.2.0"}}
	text := StateRangeRefusal("data", "n2", cluster, newer)
	if strings.Contains(text, "Upgrade n2") {
		t.Fatalf("a newer peer was told to upgrade: %s", text)
	}
	for _, want := range []string{"it is newer than this cluster", "A newer build cannot join an older cluster until the cluster is upgraded", "start n2 on 1.2.0"} {
		if !strings.Contains(text, want) {
			t.Fatalf("%q missing from %s", want, text)
		}
	}
}

func TestTheRangeRefusalOfAPeerThatWritesAboveTheClusterIsNotAnUpgradeOrder(t *testing.T) {
	cluster := StateRange{Min: 1, Max: 1, Writes: 1}
	// The peer's lower bound is not above the cluster, so only its upper bound and its
	// writes version say it is the newer side.
	newer := StateRange{Min: 1, Max: 2, Writes: 2}
	saved := stateVersions
	t.Cleanup(func() { stateVersions = saved })
	stateVersions = []StateVersionRow{{Level: 1, FirstRelease: "1.2.0"}}
	text := StateRangeRefusal("data", "n2", cluster, newer)
	if strings.Contains(text, "Upgrade n2") {
		t.Fatalf("a peer that writes above the cluster was told to upgrade: %s", text)
	}
	for _, want := range []string{"n2 is too new for it: start n2 on a build whose state_range includes 1 (1.2.0 carries it)", "this node's build is the older one"} {
		if !strings.Contains(text, want) {
			t.Fatalf("%q missing from %s", want, text)
		}
	}
}

// TestAPeerThatWritesAboveThisBuildsMaxIsToldThisNodeIsTheOlderBuild pins the acceptor text a
// release-1 node shows a finalized release-2 peer (cluster-version spec 4.8). The text freezes
// once release 1 ships: it must not tell the operator to start the healthy upgraded peer on the
// old build, nor call this node's own version the cluster's. It keeps the other reading too: a
// peer whose build is too new for a cluster that still runs this node's version.
func TestAPeerThatWritesAboveThisBuildsMaxIsToldThisNodeIsTheOlderBuild(t *testing.T) {
	saved := stateVersions
	t.Cleanup(func() { stateVersions = saved })
	stateVersions = []StateVersionRow{{Level: 1, FirstRelease: ""}}
	node := StateRange{Min: 1, Max: 1, Writes: 1}
	finalized := StateRange{Min: 1, Max: 2, Writes: 2}
	want := `refused data connection from node "n2": it writes cluster version 2, above the highest this build runs (1). ` +
		`If the cluster has moved to version 2 ('status' against n2 prints its Cluster version), this node's build is the older one: ` +
		`upgrade this node, and every member still on its build, to a build whose state_range includes 2, then restart it. ` +
		`If the cluster still runs version 1, n2 is too new for it: start n2 on a build whose state_range includes 1. ` +
		`hoarddb-server -v prints state_range on each.`
	if got := StateRangeRefusal("data", "n2", node, finalized); got != want {
		t.Fatalf("release-1 node meeting a finalized release-2 peer:\n got %s\nwant %s", got, want)
	}
	stateVersions = []StateVersionRow{{Level: 1, FirstRelease: "1.1"}, {Level: 2, FirstRelease: "1.2"}}
	want = `refused raft connection from node "n2": it writes cluster version 2, above the highest this build runs (1). ` +
		`If the cluster has moved to version 2 ('status' against n2 prints its Cluster version), this node's build is the older one: ` +
		`upgrade this node, and every member still on its build, to 1.2 or later, then restart it. ` +
		`If the cluster still runs version 1, n2 is too new for it: start n2 on a build whose state_range includes 1 (1.1 carries it). ` +
		`hoarddb-server -v prints state_range on each.`
	if got := StateRangeRefusal("raft", "n2", node, finalized); got != want {
		t.Fatalf("with both releases recorded:\n got %s\nwant %s", got, want)
	}
}

func TestTheRaftAcceptorLogsARangeRefusalOncePerMinute(t *testing.T) {
	r, logs := openRangeRaft(t, nil)
	for i := 0; i < 3; i++ {
		dialWithHandshake(t, r.cfg.Listener.Addr().String(), rangeHello(transport.HandshakeChannelRaft, "n2", StateRange{}))
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(r.RefusedPeers()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if got := strings.Count(logs.String(), "refused"); got != 1 {
		t.Fatalf("%d WARN lines for three refused dials in a row, want 1:\n%s", got, logs.String())
	}
	// The line is spec 9.4 text 1, the level 0 form, naming the node.
	if want := `refused raft connection from node "n2": it sends no state version`; !strings.Contains(logs.String(), want) {
		t.Fatalf("the WARN line is not spec 9.4 text 1, want %q in:\n%s", want, logs.String())
	}
}
