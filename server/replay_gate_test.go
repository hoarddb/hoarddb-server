package server

import (
	"bufio"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// divergedRingDirectory writes, into dataDir's ring directory, a node whose log holds a refused
// entry (a CREATE_DATABASE carrying a field this build does not read, appended with no dry run
// as another build's leader would), and closes it. The node first starts and stops once, so its
// store and its log exist as on any node that ran: a Raft that holds a database beside no store
// and no log is a node that lost both, which the ring open refuses before any replay (review R3-1),
// and is not the divergence these tests are about.
func divergedRingDirectory(t *testing.T, dataDir, nodeID string) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Server.NodeID = nodeID
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	cfg.Metrics.Enabled = false
	firstLife, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := firstLife.Start(); err != nil {
		t.Fatalf("the node's first start: %v", err)
	}
	if err := firstLife.Stop(); err != nil {
		t.Fatalf("the node's first stop: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	raft, err := cluster.OpenRaft(cluster.RaftConfig{
		NodeID: nodeID, Dir: dataDir + "/ring", Listener: listener, DataAddr: listener.Addr().String(),
		Bootstrap: true, Logger: hclog.NewNullLogger(), Incarnation: time.Now().UnixNano(),
		HeartbeatTimeout: 100 * time.Millisecond, ElectionTimeout: 100 * time.Millisecond, LeaderLease: 50 * time.Millisecond,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	body := bson.D{{Key: "name", Value: "x"}, {Key: "id", Value: int64(1)}, {Key: "rf", Value: int32(1)}, {Key: "schema_version", Value: int64(1)}, {Key: "storagex", Value: "memory"}}
	doc, err := bson.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cluster.EncodeCommand(cluster.Command{Version: cluster.CommandVersion, Kind: cluster.CommandCreateDatabase, BaseSeq: uint64(raft.State().StateSeq), Body: doc})
	if err != nil {
		t.Fatal(err)
	}
	if err := raft.AppendWithoutDryRun(raw); err != nil {
		t.Fatal(err)
	}
	if err := raft.Close(); err != nil {
		t.Fatal(err)
	}
}

// replayProbe is a client that hammers a node's driver port with a ring frame and records every
// answer that is not UNAVAILABLE "replaying its log".
type replayProbe struct {
	addr      string
	unavail   atomic.Int64
	mu        sync.Mutex
	served    []string
	stop      chan struct{}
	done      sync.WaitGroup
	closeOnce sync.Once
}

func startReplayProbe(t *testing.T, addr string) *replayProbe {
	p := &replayProbe{addr: addr, stop: make(chan struct{})}
	p.done.Add(1)
	go func() {
		defer p.done.Done()
		for {
			select {
			case <-p.stop:
				return
			default:
			}
			p.round()
			time.Sleep(time.Millisecond)
		}
	}()
	t.Cleanup(p.finish)
	return p
}

func (p *replayProbe) finish() {
	p.closeOnce.Do(func() { close(p.stop) })
	p.done.Wait()
}

func (p *replayProbe) round() {
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "replay gate probe"}}, p.addr)
	if err != nil {
		return
	}
	dialer := &net.Dialer{Timeout: time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", p.addr, tlsCfg)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	exchange := func(op transport.Opcode, payload []byte) *transport.Frame {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write(transport.NewFrame(op, payload).Encode()); err != nil {
			return nil
		}
		frame, err := transport.DecodeFrame(reader)
		if err != nil {
			return nil
		}
		return frame
	}
	login, err := bson.Marshal(&LoginRequest{Username: "admin", Password: "test123"})
	if err != nil {
		return
	}
	reply := exchange(transport.OpAuth, login)
	if reply == nil {
		return
	}
	if p.gated(reply) {
		return
	}
	if reply.Opcode != transport.OpAuthOK {
		p.record(reply)
		return
	}
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		body, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})
		if err != nil {
			return
		}
		reply = exchange(transport.OpRingAdmin, body)
		if reply == nil {
			return
		}
		if p.gated(reply) {
			time.Sleep(time.Millisecond)
			continue
		}
		p.record(reply)
		return
	}
}

// gated reports whether reply is the gate's UNAVAILABLE "replaying its log", and counts it.
func (p *replayProbe) gated(reply *transport.Frame) bool {
	var refusal ErrorResponse
	if reply.Opcode == transport.OpError && bson.Unmarshal(reply.Payload, &refusal) == nil &&
		refusal.Code == int(transport.ErrUnavailable) && containsReplaying(refusal.Message) {
		p.unavail.Add(1)
		return true
	}
	return false
}

func (p *replayProbe) record(reply *transport.Frame) {
	p.mu.Lock()
	p.served = append(p.served, reply.Opcode.String())
	p.mu.Unlock()
}

func containsReplaying(message string) bool {
	const want = "replaying its log"
	for i := 0; i+len(want) <= len(message); i++ {
		if message[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

// TestADivergedNodeServesNothingDuringReplay (test 13, the M1 gate of 6.3): a data directory
// whose log holds a refused entry never answers a client frame in ten starts in a loop; every
// frame that reaches the driver port while the node replays is UNAVAILABLE "replaying its log",
// and the node reports its divergence once the log is applied. The server is started with the
// driver listener already bound, so a client can connect the moment it serves.
// Break: hand the driver listener the ring handler directly, with no gate in front of it.
func TestADivergedNodeServesNothingDuringReplay(t *testing.T) {
	sawGate := int64(0)
	for round := range 10 {
		dataDir := t.TempDir()
		divergedRingDirectory(t, dataDir, "n1")
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = dataDir
		cfg.Server.NodeID = "n1"
		cfg.Server.Listen = "127.0.0.1:0"
		cfg.TCP.Listen = "127.0.0.1:0"
		cfg.Metrics.Enabled = false
		qs, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		if err := qs.SeedRootUser("admin", "test123"); err != nil {
			t.Fatal(err)
		}
		driver, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		qs.driverListener = driver
		probe := startReplayProbe(t, driver.Addr().String())
		if err := qs.Start(); err != nil {
			t.Fatalf("round %d: Start: %v", round, err)
		}
		select {
		case <-qs.Diverged():
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: the node never reported its divergence", round)
		}
		// A diverged node stays up until the caller exits it, and must answer nothing while it
		// does: give the probe time to log in (argon2) and be refused, then stop it.
		for waited := 0; waited < 300 && probe.unavail.Load() == 0; waited++ {
			time.Sleep(10 * time.Millisecond)
		}
		probe.finish()
		probe.mu.Lock()
		served := append([]string(nil), probe.served...)
		probe.mu.Unlock()
		if len(served) != 0 {
			t.Fatalf("round %d: a diverged node answered a client frame with %v", round, served)
		}
		sawGate += probe.unavail.Load()
		if err := qs.Stop(); err != nil {
			t.Fatal(err)
		}
	}
	if sawGate == 0 {
		t.Fatal("no client frame reached the gate in ten starts: the test proved nothing")
	}
}

// divergedService is a started server on a directory whose log holds a refused entry.
func divergedService(t *testing.T, logs io.Writer) *Service {
	t.Helper()
	dataDir := t.TempDir()
	divergedRingDirectory(t, dataDir, "n1")
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Server.NodeID = "n1"
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	cfg.Metrics.Enabled = false
	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := qs.Start(); err != nil {
		t.Fatal(err)
	}
	return qs
}

// TestTheServerChecksDivergedAfterStartAndExitsWithinTheDeadline (test 15, review m6 and round-5
// m5): a divergence recorded while the log replayed, before the caller started waiting, ends the
// wait at once with exit status 70 (no signal is sent), the server is stopped, and the stop of a
// node that will not stop is bounded by its deadline. Break one: wait on the signal channel only
// (the first half hangs); break two: stop with no deadline (the second half hangs).
func TestTheServerChecksDivergedAfterStartAndExitsWithinTheDeadline(t *testing.T) {
	var logs syncBuffer
	qs := divergedService(t, &logs)
	t.Cleanup(func() { _ = qs.Stop() })
	exited := make(chan int, 1)
	go func() { exited <- qs.WaitForExit(make(chan os.Signal)) }()
	select {
	case code := <-exited:
		if code != 70 {
			t.Fatalf("a diverged node exits %d, want 70", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("a node that diverged during replay kept waiting for a signal")
	}
	if qs.ringNode != nil {
		t.Fatal("the diverged node left its ring node open: it was not stopped")
	}
	if !strings.Contains(logs.String(), "exit_code=70") {
		t.Errorf("the server's own log does not name the exit code:\n%s", logs.String())
	}

	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	begun := time.Now()
	err := stopWithin(func() error { <-stuck; return nil }, 100*time.Millisecond)
	if err == nil || time.Since(begun) > 5*time.Second {
		t.Fatalf("a stop that never returns was not bounded by its deadline (error %v after %s)", err, time.Since(begun))
	}
}

// TestAStartedDivergedNodeAnswersHealthCheckNotHealthy (review n2, wiring): the reason Service
// installs on the server reaches HEALTH_CHECK on a real diverged ring node. Break: drop the
// ringLoading.Store in openRingNode.
func TestAStartedDivergedNodeAnswersHealthCheckNotHealthy(t *testing.T) {
	var logs syncBuffer
	qs := divergedService(t, &logs)
	t.Cleanup(func() { _ = qs.Stop() })
	select {
	case <-qs.Diverged():
	case <-time.After(10 * time.Second):
		t.Fatal("the node never reported its divergence")
	}
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}
	if health.Healthy || len(health.Reasons) == 0 || !strings.HasPrefix(health.Reasons[0], "diverged") {
		t.Fatalf("a diverged node: healthy %v, reasons %v", health.Healthy, health.Reasons)
	}
}
