package server

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringClusterPorts returns a loopback address whose port and the next one are both free, so a
// test can name server.listen with its legacy internode listener at port+1.
func ringClusterPorts(t *testing.T) string {
	t.Helper()
	for range 50 {
		first, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := first.Addr().(*net.TCPAddr)
		next, nextErr := net.Listen("tcp", net.JoinHostPort("127.0.0.1", itoa(addr.Port+1)))
		_ = first.Close()
		if nextErr != nil {
			continue
		}
		_ = next.Close()
		return addr.String()
	}
	t.Fatal("no two adjacent free loopback ports")
	return ""
}

func itoa(n int) string {
	digits := []byte{}
	for ; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}

// ringClusterBuffer is a log sink the test reads while the server writes.
type ringClusterBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *ringClusterBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *ringClusterBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// ringClusterService builds a cluster-mode service on listen, with edit applied last.
func ringClusterService(t *testing.T, listen string, logs io.Writer, edit func(*config.Config)) *Service {
	t.Helper()
	var logger *slog.Logger
	if logs != nil {
		logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	return newCapacityService(t, func(cfg *config.Config) {
		cfg.Server.NodeID = "node-a"
		cfg.Server.Listen = listen
		cfg.TCP.Listen = "127.0.0.1:0"
		cfg.Cluster.Enabled = true
		if edit != nil {
			edit(cfg)
		}
		if len(cfg.HashRing.SeedNodes) > 0 {
			// Seeds switch the legacy cluster on, which wants the token cmd/server writes first.
			if _, _, err := (CredentialKeys{}).InitClusterToken(cfg.Storage.DataDir); err != nil {
				t.Fatalf("writing the cluster token: %v", err)
			}
		}
	}, logger)
}

func ringClusterStart(t *testing.T, qs *Service) {
	t.Helper()
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = qs.Stop() })
}

// ringClusterRead dials addr with the client certificate in keyDir (made if absent) and returns the error the first read gives. A timeout means the server accepted the
// certificate and is waiting for the handshake frame.
func ringClusterRead(t *testing.T, addr string, keyDir string) error {
	t.Helper()
	identity, _, err := transport.MakeServerTLSConfig(keyDir)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		MinVersion: tls.VersionTLS13, NextProtos: []string{transport.ALPNFor(transport.ProtocolMajor)},
		Certificates: identity.Certificates, InsecureSkipVerify: true, //nolint:gosec // the server is the system under test
	})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	_, err = conn.Read(make([]byte, 1))
	return err
}

func ringClusterFingerprint(t *testing.T, keyDir string) string {
	t.Helper()
	_, fingerprint, err := transport.MakeServerTLSConfig(keyDir)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func ringClusterTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// TestRingClusterZeroConfigBindsNoPeerListener: [GUARD cluster.enabled false opens the node
// exactly as before: server.listen is not bound by the ring node].
func TestRingClusterZeroConfigBindsNoPeerListener(t *testing.T) {
	listen := ringClusterPorts(t)
	qs := ringClusterService(t, listen, nil, func(cfg *config.Config) { cfg.Cluster.Enabled = false })
	ringClusterStart(t, qs)
	if conn, err := net.DialTimeout("tcp", listen, time.Second); err == nil {
		_ = conn.Close()
		t.Fatalf("server.listen %s is bound with cluster.enabled false: the peer listener must not exist by default", listen)
	}
	if qs.ringNode == nil {
		t.Fatal("the one-voter ring node did not open")
	}
	if got := qs.ringNode.Identity().DataAddr; got != "" {
		t.Fatalf("a one-voter node reports data address %q; it has no peer address", got)
	}
}

// TestRingClusterEnabledBindsTheDefaultPeerAddress: [GUARD cluster.listen empty binds
// server.listen, pinned: a stranger's key is refused, a pinned key is not; the legacy internode
// listener stays at port+1; Stop closes the peer listener].
func TestRingClusterEnabledBindsTheDefaultPeerAddress(t *testing.T) {
	listen := ringClusterPorts(t)
	peerKeys := filepath.Join(t.TempDir(), "keys")
	qs := ringClusterService(t, listen, nil, func(cfg *config.Config) {
		cfg.HashRing.SeedFingerprints = []string{"node-b=" + ringClusterFingerprint(t, peerKeys)}
	})
	ringClusterStart(t, qs)
	state := qs.ringNode.State()
	if len(state.Members) != 1 || state.Members[0].ID != "node-a" || state.Members[0].DataAddr != listen {
		t.Fatalf("committed members %+v, want node-a at %s", state.Members, listen)
	}
	if len(state.Voters) != 1 {
		t.Fatalf("voters %v, want node-a alone", state.Voters)
	}
	if err := ringClusterRead(t, listen, peerKeys); !ringClusterTimeout(err) {
		t.Fatalf("a pinned key was refused at the peer listener: %v", err)
	}
	if err := ringClusterRead(t, listen, filepath.Join(qs.config.Storage.DataDir, "keys")); !ringClusterTimeout(err) {
		t.Fatalf("the node's own key was refused at its own peer listener (a node must admit itself): %v", err)
	}
	if err := ringClusterRead(t, listen, filepath.Join(t.TempDir(), "stranger")); ringClusterTimeout(err) || err == nil {
		t.Fatalf("a key nobody pinned got past the peer listener (read error %v)", err)
	}
	internode := qs.internodeTCPLn.Addr().String()
	host, port, _ := net.SplitHostPort(listen)
	if want := net.JoinHostPort(host, itoa(atoiPort(port)+1)); internode != want {
		t.Fatalf("the legacy internode listener moved to %s, want %s", internode, want)
	}
	if err := qs.Stop(); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.DialTimeout("tcp", listen, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("the peer listener outlived Stop")
	}
}

func atoiPort(port string) int {
	n := 0
	for _, digit := range port {
		n = n*10 + int(digit-'0')
	}
	return n
}

// TestRingClusterAdvertisesTheConfiguredAddress: [GUARD the committed data address is
// cluster.advertise, not the bound address].
func TestRingClusterAdvertisesTheConfiguredAddress(t *testing.T) {
	listen := ringClusterPorts(t)
	qs := ringClusterService(t, listen, nil, func(cfg *config.Config) { cfg.Cluster.Advertise = "node-a.example.test:4433" })
	ringClusterStart(t, qs)
	if got := qs.ringNode.Identity().DataAddr; got != "node-a.example.test:4433" {
		t.Fatalf("identity data address %q, want the advertised one", got)
	}
	if got := qs.ringNode.State().Members[0].DataAddr; got != "node-a.example.test:4433" {
		t.Fatalf("committed data address %q, want the advertised one", got)
	}
}

// TestRingClusterRefusesASeedWithNoPin: [GUARD a configured seed with no fingerprint under its id
// stops the start with the exact fix, and leaves no peer port bound and no ring directory].
func TestRingClusterRefusesASeedWithNoPin(t *testing.T) {
	listen := ringClusterPorts(t)
	other := "node-c=" + ringClusterFingerprint(t, filepath.Join(t.TempDir(), "keys"))
	qs := ringClusterService(t, listen, nil, func(cfg *config.Config) {
		cfg.HashRing.SeedNodes = []string{"node-b@127.0.0.1:1"}
		cfg.HashRing.SeedFingerprints = []string{other}
	})
	err := qs.Start()
	if err == nil {
		_ = qs.Stop()
		t.Fatal("Start accepted a seed with no pinned fingerprint")
	}
	for _, want := range []string{`"node-b"`, "node-b=SHA256:", "on this node and on node-b, then restart both"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	if conn, dialErr := net.DialTimeout("tcp", listen, time.Second); dialErr == nil {
		_ = conn.Close()
		t.Fatal("a refused start left the peer listener bound")
	}
	if _, statErr := os.Stat(filepath.Join(qs.config.Storage.DataDir, ringDirName)); statErr == nil {
		t.Fatal("a refused start created the ring directory")
	}
}

// TestRingClusterRefusesAPinThatContradictsThisNodesKey: [GUARD an entry for this node's own id
// with another key is refused, not trusted].
func TestRingClusterRefusesAPinThatContradictsThisNodesKey(t *testing.T) {
	other := "node-a=" + ringClusterFingerprint(t, filepath.Join(t.TempDir(), "keys"))
	qs := ringClusterService(t, ringClusterPorts(t), nil, func(cfg *config.Config) {
		cfg.HashRing.SeedFingerprints = []string{other}
	})
	err := qs.Start()
	if err == nil {
		_ = qs.Stop()
		t.Fatal("Start accepted a pin for this node's id under another key")
	}
	if !strings.Contains(err.Error(), "this node's own key") {
		t.Fatalf("refusal %q does not name the conflict", err)
	}
}

// TestRingClusterStartupLine: [GUARD the start-up line names id, data_addr, bootstrap or join,
// and the fingerprint].
func TestRingClusterStartupLine(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		edit func(*testing.T, *config.Config)
	}{
		{"bootstrap", "mode=bootstrap", func(*testing.T, *config.Config) {}},
		{"join", "mode=join", func(t *testing.T, cfg *config.Config) {
			cfg.HashRing.SeedNodes = []string{"node-b@127.0.0.1:1"}
			cfg.HashRing.SeedFingerprints = []string{"node-b=" + ringClusterFingerprint(t, filepath.Join(t.TempDir(), "keys"))}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listen := ringClusterPorts(t)
			var logs ringClusterBuffer
			qs := ringClusterService(t, listen, &logs, func(cfg *config.Config) { tc.edit(t, cfg) })
			ringClusterStart(t, qs)
			var line string
			for _, candidate := range strings.Split(logs.String(), "\n") {
				if strings.Contains(candidate, "entry-ring node open") {
					line = candidate
				}
			}
			if joined := strings.Contains(tc.mode, "join"); joined != (len(qs.ringNode.State().Members) == 0) {
				t.Errorf("mode %s but the node committed members %+v: only a joining node waits with none", tc.mode, qs.ringNode.State().Members)
			}
			for _, want := range []string{"id=node-a", "data_addr=" + listen, tc.mode, "fingerprint=SHA256:"} {
				if !strings.Contains(line, want) {
					t.Errorf("start-up line %q does not contain %q", line, want)
				}
			}
		})
	}
}

// TestRingNodeConfigCarriesTheClusterKeys: [GUARD the node settings come from the server's own
// keys: the dedupe window, and the advertised address and witness flag only in cluster mode].
func TestRingNodeConfigCarriesTheClusterKeys(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = "/data"
	cfg.Replication.DedupeWindow = 77
	cfg.Replication.ReplWindow = 9
	cfg.Cluster.Enabled, cfg.Cluster.Witness = true, true
	cfg.Cluster.Advertise = "node-a.example.test:4433"
	got := ringNodeConfig(cfg)
	if got.ReplWindow != 9 {
		t.Fatalf("ring node config window %d, want replication.repl_window 9", got.ReplWindow)
	}
	if got.DedupeWindow != 77 || !got.Witness || got.Advertise != "node-a.example.test:4433" {
		t.Fatalf("ring node config %+v", got)
	}
	cfg.Cluster.Enabled = false
	if got := ringNodeConfig(cfg); got.Witness || got.Advertise != "" {
		t.Fatalf("a node outside cluster mode took the cluster keys: %+v", got)
	}
}

// TestRingStartNoticeSaysOnlyWhatIsTrue: [GUARD a joined member restarting with its seeds still
// configured is not told it waits to be joined; a node that just minted a cluster is told so once].
func TestRingStartNoticeSaysOnlyWhatIsTrue(t *testing.T) {
	for _, tc := range []struct {
		name      string
		joining   bool
		members   int
		stateSeq  int64
		wantTopic string
	}{
		{"waiting to be joined", true, 0, 0, "serves its peers only once a member admits it"},
		{"joined member restarts", true, 3, 40, ""},
		{"fresh bootstrap", false, 1, 1, "started a new cluster"},
		{"bootstrap node restarts", false, 1, 25, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			notice, fix := ringStartNotice(tc.joining, tc.members, tc.stateSeq, "node-d", "192.0.2.4:4433")
			if tc.wantTopic == "" && (notice != "" || fix != "") {
				t.Fatalf("notice %q, fix %q, want none", notice, fix)
			}
			if tc.wantTopic != "" && (!strings.Contains(notice, tc.wantTopic) || !strings.Contains(fix, "hash_ring.seed_nodes")) {
				t.Fatalf("notice %q, fix %q, want the %q notice naming hash_ring.seed_nodes", notice, fix, tc.wantTopic)
			}
			// BUG-0870 guard: the node cannot tell a new node from a member that lost its data, so the
			// fix names the statement for the first and the truth for the second.
			if tc.joining && tc.wantTopic != "" {
				for _, want := range []string{"'join node-d@192.0.2.4:4433'", "if node-d was a member that lost its data", "keep the ring directory you moved aside", "'replace-node node-d --with node-d@192.0.2.4:4433'"} {
					if !strings.Contains(fix, want) {
						t.Errorf("fix %q lacks %q", fix, want)
					}
				}
			}
		})
	}
}

// TestRingClusterPublishesTheClientAddress: [GUARD a cluster node started with tcp.advertise commits it as its client
// address, on the host of its cluster.advertise; with tcp.advertise unset and a loopback tcp.listen it commits none].
func TestRingClusterPublishesTheClientAddress(t *testing.T) {
	qs := ringClusterService(t, ringClusterPorts(t), nil, func(cfg *config.Config) {
		cfg.Cluster.Advertise = "node-a.example.test:4433"
		cfg.TCP.Advertise = "node-a.example.test:7433"
	})
	ringClusterStart(t, qs)
	row := qs.ringNode.State().Members[0]
	if row.ClientAddr != "node-a.example.test:7433" || row.DataAddr != "node-a.example.test:4433" {
		t.Fatalf("committed member %+v, want client node-a.example.test:7433 beside data node-a.example.test:4433", row)
	}
	plain := ringClusterService(t, ringClusterPorts(t), nil, nil)
	ringClusterStart(t, plain)
	if got := plain.ringNode.State().Members[0].ClientAddr; got != "" {
		t.Fatalf("a loopback tcp.listen with no tcp.advertise committed client address %q", got)
	}
}

// TestRingClusterRefusesAnAdvertiseOnAnotherHost: [GUARD tcp.advertise on a different host than cluster.advertise stops
// the start and the message names tcp.advertise and the host rule].
func TestRingClusterRefusesAnAdvertiseOnAnotherHost(t *testing.T) {
	qs := ringClusterService(t, ringClusterPorts(t), nil, func(cfg *config.Config) {
		cfg.Cluster.Advertise = "node-a.example.test:4433"
		cfg.TCP.Advertise = "elsewhere.example.test:7433"
	})
	err := qs.Start()
	if err == nil {
		_ = qs.Stop()
		t.Fatal("a node started with a client address on another host")
	}
	for _, want := range []string{"tcp.advertise", "elsewhere.example.test:7433", "node-a.example.test:4433", "cluster.advertise"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not mention %q", err, want)
		}
	}
}

// TestRingNodeWarnsThatTCPAdvertiseWaitsForACluster: [GUARD tcp.advertise on a node that is not in a ring cluster logs a
// WARN saying it takes effect once cluster.enabled is on, and a cluster node logs none].
func TestRingNodeWarnsThatTCPAdvertiseWaitsForACluster(t *testing.T) {
	var logs ringClusterBuffer
	alone := ringClusterService(t, ringClusterPorts(t), &logs, func(cfg *config.Config) {
		cfg.Cluster.Enabled = false
		cfg.TCP.Advertise = "db1.example.test:7433"
	})
	ringClusterStart(t, alone)
	text := logs.String()
	if !strings.Contains(text, "tcp.advertise") || !strings.Contains(text, "cluster.enabled") || !strings.Contains(text, "level=WARN") {
		t.Fatalf("no warning that tcp.advertise is unused:\n%s", text)
	}
	var clusterLogs ringClusterBuffer
	member := ringClusterService(t, ringClusterPorts(t), &clusterLogs, func(cfg *config.Config) {
		cfg.Cluster.Advertise = "node-a.example.test:4433"
		cfg.TCP.Advertise = "node-a.example.test:7433"
	})
	ringClusterStart(t, member)
	if strings.Contains(clusterLogs.String(), "tcp.advertise") {
		t.Fatalf("a cluster node warned about tcp.advertise:\n%s", clusterLogs.String())
	}
}

// TestRingNodeConfigCarriesTheRetentionCheck: replication.retention_check_seconds reaches the
// ring node as the ticker that drives the retention pass and the dedupe sweep.
func TestRingNodeConfigCarriesTheRetentionCheck(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = "/data"
	cfg.Replication.RetentionCheckSeconds = 120
	if got := ringNodeConfig(cfg).RetentionCheck; got != 120*time.Second {
		t.Fatalf("ring node config RetentionCheck = %v, want replication.retention_check_seconds 120s", got)
	}
}
