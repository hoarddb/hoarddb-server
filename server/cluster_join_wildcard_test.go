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

// TestJoinFromWildcardNodeWithRealMembershipAddressIsNotRefused is SEC-1 F6.
// A node listening on 0.0.0.0 with HOARDDB_NODE_ID and id@address seeds has
// self.Addr normalised to 127.0.0.1 on first boot, but the membership lists it
// at its real address, which is the address its peers dial. The loopback
// refusal must look at the membership, not at self.Addr, or the recipe the
// refusal message itself endorses is refused until a restart.
func TestJoinFromWildcardNodeWithRealMembershipAddressIsNotRefused(t *testing.T) {
	s := newTestServer(t)
	dir := s.config.Storage.DataDir
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = "0.0.0.0:4433"
	cfg.Server.NodeID = "n1"
	cfg.Server.ReplicationFactor = 1
	cfg.HashRing.SeedNodes = []string{"n1@192.0.2.1:4433", "n2@192.0.2.2:4433"}
	cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	s.cluster = cluster

	payload, err := bson.Marshal(&ClusterAddRequest{Address: "n3@192.0.2.3:4433"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := handleClusterAdd(ctx, &transport.Frame{Opcode: transport.OpClusterAdd, Payload: payload}, s)
	// The join itself cannot complete (192.0.2.2 and 192.0.2.3 do not exist, so
	// no majority answers); what matters is that the loopback refusal did not
	// fire, whether the failure comes back as an error or as a reply.
	outcome := ""
	if err != nil {
		outcome = err.Error()
	}
	if reply != nil {
		outcome += string(reply.Payload)
	}
	if strings.Contains(outcome, "only this machine can reach") {
		t.Fatalf("a node whose membership address is 192.0.2.1:4433 was refused as loopback: %s", outcome)
	}
}
