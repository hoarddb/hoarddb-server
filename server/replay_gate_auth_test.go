package server

import (
	"context"
	"io"
	"log/slog"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestTheReplayGateDoesNotCountAnAuthAsAFailedLogin (review M1): while the gate is shut an AUTH
// frame is answered UNAVAILABLE without its credential being looked at, so the per-IP ledger must
// not count it; more AUTHs than tcp.auth_max_fails during one replay would otherwise lock the
// operator out after the node is ready. Break: drop the MarkCredentialUnchecked call.
func TestTheReplayGateDoesNotCountAnAuthAsAFailedLogin(t *testing.T) {
	reached := false
	gate := &replayGate{ready: func() bool { return false }, next: func(context.Context, *transport.Frame) (*transport.Frame, error) {
		reached = true
		return transport.NewFrame(transport.OpAuthOK, nil), nil
	}}
	resp, err := gate.handle(context.Background(), transport.NewFrame(transport.OpAuth, []byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	if reached {
		t.Fatal("an AUTH frame reached the handler while the gate was shut")
	}
	if !resp.CredentialUnchecked() {
		t.Fatal("the gate's refusal of an AUTH is not marked credential-unchecked, so the ledger counts it as a failed login")
	}
}

// TestTheReplayGateLetsAWaitingNodeBeInspected (review M1): the read-only liveness, health and
// describe_node frames pass while the gate is shut, so the CLI can say what a waiting node is
// doing; every data frame and every other admin command stays gated.
// Break: exempt nothing (the first half fails); exempt every admin command (the second half fails).
func TestTheReplayGateLetsAWaitingNodeBeInspected(t *testing.T) {
	describeNode, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDescribeNode})
	if err != nil {
		t.Fatal(err)
	}
	showDatabases, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminShowDatabases})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		frame  *transport.Frame
		passes bool
	}{
		{"ping", transport.NewFrame(transport.OpPing, nil), true},
		{"health check", transport.NewFrame(transport.OpHealthCheck, nil), true},
		{"describe_node", transport.NewFrame(transport.OpRingAdmin, describeNode), true},
		{"show_databases", transport.NewFrame(transport.OpRingAdmin, showDatabases), false},
		{"a ring read", transport.NewFrame(transport.OpRingList, nil), false},
		{"a keyed request", transport.NewFrame(transport.OpRingKeyed, nil), false},
		{"a ring batch", transport.NewFrame(transport.OpRingBatch, nil), false},
		{"a ring get-many", transport.NewFrame(transport.OpRingGetMany, nil), false},
		{"a legacy put", transport.NewFrame(transport.OpPut, nil), false},
		{"an admin frame with no body", transport.NewFrame(transport.OpRingAdmin, nil), false},
	}
	for _, c := range cases {
		reached := false
		gate := &replayGate{ready: func() bool { return false }, next: func(_ context.Context, f *transport.Frame) (*transport.Frame, error) {
			reached = true
			return transport.NewFrameWithID(transport.OpRingReply, f.RequestID, nil), nil
		}}
		if _, err := gate.handle(context.Background(), c.frame); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if reached != c.passes {
			t.Errorf("%s: reached the handler = %v, want %v", c.name, reached, c.passes)
		}
	}
}

// TestASignalReceivedBeforeWaitForExitStopsTheServer (review M2): main registers its signal
// channel before Start, so a SIGTERM that arrives during Start's wait is already buffered when
// WaitForExit begins; it must go through Stop and exit 0. The registration order and the abort of
// the boot wait are tested in cmd/server (TestRunServiceRegistersSignalsBeforeStart).
func TestASignalReceivedBeforeWaitForExitStopsTheServer(t *testing.T) {
	dataDir := t.TempDir()
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
	if err := qs.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qs.Stop() })
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	exited := make(chan int, 1)
	go func() { exited <- qs.WaitForExit(signals) }()
	select {
	case code := <-exited:
		if code != 0 {
			t.Fatalf("a signal received before the wait exits %d, want 0", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("WaitForExit ignored a signal that was already buffered")
	}
	if qs.ringNode != nil {
		t.Fatal("the server was not stopped")
	}
}
