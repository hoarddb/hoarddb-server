package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestJoinStatementPrintsTheServersResultLine: join and promote print the server's line as it is,
// send the node and address the statement names, and let a refusal through unchanged.
func TestJoinStatementPrintsTheServersResultLine(t *testing.T) {
	session, fake := u52Session()
	fake.membership = ringclient.MembershipResult{Epoch: 2, StateSeq: 9, Result: "Added node-b@127.0.0.1:4443 as a member: joining, then active when it holds the slots it will own (epoch 2)"}
	ctx := context.Background()
	for _, statement := range []string{"join node-b@127.0.0.1:4443", "promote node-b"} {
		output, err := session.Execute(ctx, statement)
		if err != nil || strings.Join(output.Lines, "\n") != fake.membership.Result {
			t.Errorf("%s printed %q, %v; want the server's line %q", statement, output.Lines, err, fake.membership.Result)
		}
	}
	if got := strings.Join(fake.calls, " | "); got != "join node-b@127.0.0.1:4443 witness=false | Join | promote node-b | Promote" {
		t.Errorf("the session sent %s", got)
	}
	fake.failures = map[string]error{"Join": &ringclient.Error{Code: 0x1D, Message: "node node-b is already a member at 127.0.0.1:1, so it cannot be joined again"}}
	_, err := session.Execute(ctx, "join node-b@127.0.0.1:4443")
	if err == nil || !strings.Contains(err.Error(), "already a member") || ringFailure(err).Exit != 8 {
		t.Errorf("a refusal came back as %v, want the server's text and exit 8", err)
	}
}

// ringJoinPorts returns a loopback address whose port and the next one are both free: server.listen
// takes the first and the legacy internode listener the second.
func ringJoinPorts(t *testing.T) string {
	t.Helper()
	for range 50 {
		first, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := first.Addr().(*net.TCPAddr)
		next, nextErr := net.Listen("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(addr.Port+1)))
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

// ringJoinNode is one real server of the transcript's cluster.
type ringJoinNode struct {
	id, listen, driver, fingerprint string
	service                         *server.Service
}

// ringJoinCluster starts node-a on its own and node-b, node-w (a witness) and node-c with
// node-a in their seed list, every key pinned on every node that must trust it, and returns them.
func ringJoinCluster(t *testing.T) []*ringJoinNode {
	t.Helper()
	nodes := []*ringJoinNode{{id: "node-a"}, {id: "node-b"}, {id: "node-w"}, {id: "node-c"}}
	dirs := map[string]string{}
	for _, node := range nodes {
		dirs[node.id] = t.TempDir()
		_, fingerprint, err := transport.MakeServerTLSConfig(filepath.Join(dirs[node.id], "keys"))
		if err != nil {
			t.Fatal(err)
		}
		node.fingerprint, node.listen = fingerprint, ringJoinPorts(t)
	}
	for _, node := range nodes {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		node.driver = listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		cfg := config.DefaultConfig()
		cfg.Storage.DataDir = dirs[node.id]
		cfg.Metrics.Enabled, cfg.Audit.Enabled = false, false
		cfg.Server.NodeID, cfg.Server.Listen, cfg.TCP.Listen = node.id, node.listen, node.driver
		cfg.Cluster.Enabled = true
		cfg.Cluster.Witness = node.id == "node-w"
		if node.id == "node-a" {
			// The first node lists nobody and pins the three keys it will be asked to join.
			for _, other := range nodes[1:] {
				cfg.HashRing.SeedFingerprints = append(cfg.HashRing.SeedFingerprints, other.id+"="+other.fingerprint)
			}
		} else {
			cfg.HashRing.SeedNodes = []string{"node-a@" + nodes[0].listen}
			cfg.HashRing.SeedFingerprints = []string{"node-a=" + nodes[0].fingerprint}
			if _, _, err := (server.CredentialKeys{}).InitClusterToken(cfg.Storage.DataDir); err != nil {
				t.Fatal(err)
			}
		}
		service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("%s: %v", node.id, err)
		}
		if node.id == "node-a" {
			// The cluster's first administrator comes from the root task, as cmd/server arms it.
			if err := service.BootstrapUsersFrom("root", "live-test-password", true, "HOARDDB_ROOT_PASSWORD"); err != nil {
				t.Fatal(err)
			}
		}
		if err := service.Start(); err != nil {
			t.Fatalf("starting %s: %v", node.id, err)
		}
		t.Cleanup(func() { _ = service.Stop() })
		node.service = service
	}
	return nodes
}

// TestRingJoinTranscript is queries section 8: three joins and nodes on real servers, with real
// pinned keys, print what the golden holds. node-b votes, the witness votes and holds no
// documents, and node-c joins without a vote because the cluster already has its three.
func TestRingJoinTranscript(t *testing.T) {
	nodes := ringJoinCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	client := ringUsersDial(t, nodes[0].driver, "root", "live-test-password", 30*time.Second)
	session := NewRingSession(NewRingAPI(client))
	statements := []string{"join node-b@" + nodes[1].listen, "join node-w@" + nodes[2].listen, "join node-c@" + nodes[3].listen, "nodes"}
	transcript := ringRecord(ctx, session, statements)
	for _, node := range nodes[1:] {
		// The addresses are real and different on every run; the golden holds <ADDR>.
		transcript = strings.ReplaceAll(transcript, node.listen, "<ADDR>")
	}
	normal := ringNormalise(transcript, nodes[0].listen)
	assertRingGolden(t, "join-promote", normal)

	lines := strings.Split(transcript, "\n")
	for _, want := range []string{
		"Added node-b@<ADDR> as a member: joining, then active when it holds the slots it will own (epoch 2)",
		"Added node-w@<ADDR> as a witness: it votes and holds no documents (epoch 2)",
		"Added node-c@<ADDR> as a member: joining (not a voter: the cluster already has its 3 voters) (epoch 3)",
	} {
		if !containsLine(lines, want) {
			t.Errorf("the transcript lacks the line %q:\n%s", want, transcript)
		}
	}
	sum := sha256.Sum256([]byte(normal))
	t.Logf("join-promote.golden sha256 %s", hex.EncodeToString(sum[:]))
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}
