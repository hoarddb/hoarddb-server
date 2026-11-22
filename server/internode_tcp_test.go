package server

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestInternodeTCPListenerServesGossip proves the two-policy design on one
// real node: the internode TCP port (PeerMode) serves a node-to-node
// opcode to a NodeUser-authenticated peer, while the driver port refuses
// the same opcode by name on the same process.
func TestInternodeTCPListenerServesGossip(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.Server.NodeID = "itest-node"
	cfg.Server.ReplicationFactor = 2 // cluster enabled, so NodeUser auth is meaningful
	cfg.TCP.Listen = "127.0.0.1:0"
	cfg.TCP.InternodeListen = "127.0.0.1:0" // ephemeral for the test; production derives it
	token := "itest-cluster-token"
	if err := SaveClusterToken(cfg.Storage.DataDir, ClusterToken{
		Token:     token,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	qs, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := qs.SeedRootUser("admin", "test123"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = qs.Stop() })
	if qs.internodeTCPLn == nil {
		t.Fatal("internode TCP listener not started")
	}
	internodeAddr := qs.internodeTCPLn.Addr().String()
	if internodeAddr == "" {
		t.Fatal("internode listener has no bound address")
	}

	// Peer side: the pooled NodeUser-authenticated TCP client the cluster
	// dial uses (transport.NewTCPPeerClient) delivers gossip to the
	// internode port. AUTH success is the first assertion — it proves the
	// cluster-token credential works through the real server's gate over
	// TCP.
	tlsCfg := newTestInternodeTLSConfig(t, internodeAddr, []string{qs.core.fingerprint})
	peer, err := transport.NewTCPPeerClient(context.Background(), internodeAddr, tlsCfg, token, 1)
	if err != nil {
		t.Fatalf("NewTCPPeerClient: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	gossipPayload, err := bson.Marshal(&GossipRequest{
		Token:   token,
		NodeID:  "itest-node",
		Addr:    cfg.Server.Listen,
		Epoch:   1,
		Nodes:   []Peer{{ID: "itest-node", Addr: cfg.Server.Listen}},
		Members: []string{"itest-node"},
	})
	if err != nil {
		t.Fatalf("marshal gossip: %v", err)
	}
	resp, err := peer.SendRequest(context.Background(), transport.LaneControl, transport.OpGossip, gossipPayload)
	if err != nil {
		t.Fatalf("gossip over internode TCP: %v", err)
	}
	if resp.Opcode == transport.OpError {
		t.Fatalf("internode port refused gossip: %s", resp.Payload)
	}
	if resp.Opcode != transport.OpGossip {
		t.Fatalf("gossip response opcode = %s, want GOSSIP (the internode handler answered)", resp.Opcode)
	}

	// Same opcode, driver port, same process: refused by name. Two ports,
	// two policies — a peer that dials the driver port still gets the
	// named refusal.
	driverAddr := qs.tcpListener.Addr().String()
	client := dialTCPServer(t, driverAddr)
	client.login()
	_ = client.conn.SetDeadline(time.Now().Add(10 * time.Second))
	frame := transport.NewFrame(transport.OpGossip, gossipPayload)
	if _, err := client.conn.Write(frame.Encode()); err != nil {
		t.Fatalf("write gossip to driver port: %v", err)
	}
	driverResp, err := transport.DecodeFrame(client.br)
	if err != nil {
		t.Fatalf("read driver-port response: %v", err)
	}
	if driverResp.Opcode != transport.OpError {
		t.Fatalf("driver port answered gossip with %s; want the named refusal", driverResp.Opcode)
	}
	var errResp ErrorResponse
	_ = bson.Unmarshal(driverResp.Payload, &errResp)
	if !strings.Contains(errResp.Message, "GOSSIP") {
		t.Fatalf("driver-port refusal %q does not name the refused opcode", errResp.Message)
	}
}
