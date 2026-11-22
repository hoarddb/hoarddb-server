package server

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// duplicateIDNodeConfig is one node's configuration for the BUG-0595 tests: its
// own data directory holding a cluster token, listening on listen.
func duplicateIDNodeConfig(t *testing.T, listen, nodeID string, seeds []string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = listen
	cfg.Server.NodeID = nodeID
	cfg.Server.ReplicationFactor = 3
	cfg.HashRing.SeedNodes = seeds
	return cfg
}

// requireRefusalNames fails the test unless err is a refusal whose message
// carries every one of wants.
func requireRefusalNames(t *testing.T, what string, err error, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s was accepted; want a refusal naming %s", what, strings.Join(wants, ", "))
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: refusal does not name %q:\n%v", what, want, err)
		}
	}
}

// TestASeedListNamingTwoNodesUnderOneIDNeverYieldsTwoAuthMasters is BUG-0595,
// the BUG-0592 verifier's finding: seeds dup@A0, dup@A1, solo@A2 (an operator
// typo) let the nodes at A0 and A1 both run as "dup", own every placement on
// it, and both accept user changes as the auth master. With the node id
// defaulted (the hostname) and with HOARDDB_NODE_ID=dup,dup,solo. The verifier
// started the three nodes and counted the masters; a node now refuses to start
// on that seed list, so no node runs and no two can be master, and the refusal
// has to say which id, which addresses and what to change.
func TestASeedListNamingTwoNodesUnderOneIDNeverYieldsTwoAuthMasters(t *testing.T) {
	addrs := []string{"127.0.0.1:24433", "127.0.0.1:24443", "127.0.0.1:24453"}
	seeds := []string{"dup@" + addrs[0], "dup@" + addrs[1], "solo@" + addrs[2]}
	for _, variant := range []struct {
		name    string
		nodeIDs []string
	}{
		{"default node ids", []string{"build-host", "build-host", "build-host"}},
		{"explicit node ids", []string{"dup", "dup", "solo"}},
	} {
		t.Run(variant.name, func(t *testing.T) {
			s := newTestServer(t)
			for i, addr := range addrs {
				cfg := duplicateIDNodeConfig(t, addr, variant.nodeIDs[i], seeds)
				cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
				if err == nil {
					cluster.Stop()
				}
				requireRefusalNames(t, "starting the node at "+addr+" with seeds "+strings.Join(seeds, ","), err,
					`"dup"`, addrs[0], addrs[1], "HOARDDB_NODE_ID", "HOARDDB_SEED_NODES")
			}
		})
	}
}

// TestASeedListRepeatingOneNodeAtOneAddressStillStarts: the same id at the same
// address listed twice is one node, and stays harmless (BUG-0595's fix refuses
// only a repeated id at a different address).
func TestASeedListRepeatingOneNodeAtOneAddressStillStarts(t *testing.T) {
	s := newTestServer(t)
	cfg := duplicateIDNodeConfig(t, "127.0.0.1:24433", "n1",
		[]string{"n1@127.0.0.1:24433", "n1@127.0.0.1:24433", "n2@127.0.0.1:24443"})
	cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("a seed list repeating n1 at one address was refused: %v", err)
	}
	defer cluster.Stop()
	if got := memberIDsOf(cluster.Members()); len(got) != 2 {
		t.Fatalf("members = %v, want n1 and n2 once each", got)
	}
}

// TestAPersistedMemberSetNamingOneIDAtTwoAddressesRefusesToStart is BUG-0595
// for membership.json, which is authoritative over the seeds once written: a
// record listing dup at two addresses (edited by hand, or copied from another
// node) must stop the node as the seed list does, not let it pick one.
func TestAPersistedMemberSetNamingOneIDAtTwoAddressesRefusesToStart(t *testing.T) {
	addrs := []string{"127.0.0.1:24433", "127.0.0.1:24443", "127.0.0.1:24453"}
	s := newTestServer(t)
	cfg := duplicateIDNodeConfig(t, addrs[1], "dup", []string{"dup@" + addrs[1], "solo@" + addrs[2]})
	path := membershipPathFor(cfg.Storage.DataDir)
	if err := saveMembershipRecord(path, membershipFile{Epoch: InitialEpoch, Members: []ring.Node{
		{ID: "dup", Addr: addrs[0]}, {ID: "dup", Addr: addrs[1]}, {ID: "solo", Addr: addrs[2]},
	}}); err != nil {
		t.Fatalf("write membership record: %v", err)
	}
	cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		cluster.Stop()
	}
	requireRefusalNames(t, "starting on a member set listing dup twice", err,
		`"dup"`, addrs[0], addrs[1], filepath.Base(path))
}

// TestAJoinUnderAnIDAMemberHoldsIsRefusedNamingThatMember is BUG-0595's join
// path: adding a node under an id a member already holds at another address
// would be the same two-nodes-one-id ring. It was already refused, but as "the
// node is already a cluster member", which is not true and names nobody; the
// refusal must name the member that holds the id and where it is, and must come
// before the join dials the newcomer, which may not be running yet.
func TestAJoinUnderAnIDAMemberHoldsIsRefusedNamingThatMember(t *testing.T) {
	s := newTestServer(t)
	cfg := duplicateIDNodeConfig(t, "192.0.2.1:4433", "n1", []string{"n1@192.0.2.1:4433", "n2@192.0.2.2:4433"})
	cluster, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer cluster.Stop()
	s.cluster = cluster
	newcomer := ring.Node{ID: "n2", Addr: "192.0.2.9:4433"}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requireRefusalNames(t, "proposing n2 at 192.0.2.9:4433", cluster.proposeJoin(ctx, newcomer),
		"192.0.2.2:4433", "192.0.2.9:4433", "HOARDDB_NODE_ID")

	payload, err := bson.Marshal(&ClusterAddRequest{Address: newcomer.ID + "@" + newcomer.Addr})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	reply, err := handleClusterAdd(ctx, &transport.Frame{Opcode: transport.OpClusterAdd, Payload: payload}, s)
	if err != nil {
		t.Fatalf("handleClusterAdd: %v", err)
	}
	if reply.Opcode != transport.OpError {
		t.Fatalf("'join %s@%s' answered %v, want a refusal", newcomer.ID, newcomer.Addr, reply.Opcode)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(reply.Payload, &refusal); err != nil {
		t.Fatalf("decode refusal: %v", err)
	}
	if refusal.Code != int(transport.ErrAlreadyExists) {
		t.Errorf("refusal code = %d, want ALREADY_EXISTS (%d)", refusal.Code, transport.ErrAlreadyExists)
	}
	for _, want := range []string{"192.0.2.2:4433", "192.0.2.9:4433", "HOARDDB_NODE_ID"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("'join %s@%s' refusal does not name %q:\n%s", newcomer.ID, newcomer.Addr, want, refusal.Message)
		}
	}
}
