package server

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
)

// TestEveryMembershipWriterKeepsJoinedAtAndSettledOf is review F-3: on an
// encrypted cluster a member rewrites membership.json when it learns that a
// joiner's store is sealed, which happens at the joiner's first gossip after
// the join. That write must keep what the join recorded beside the epoch, or
// after a restart the joiner looks like a founder.
func TestEveryMembershipWriterKeepsJoinedAtAndSettledOf(t *testing.T) {
	tc := newMixedCluster(t, 3, "on", "on", "on")
	defer tc.stop()
	// The proposer learns the joiner's store before the join, from its probe
	// (cluster_join.go:Cluster.admitNewcomer); another member learns it after,
	// at the joiner's first gossip -- the write this test is about.
	proposer, member := clusterOf(tc, 0), clusterOf(tc, 1)

	ports := reserveNodePorts(t)
	joinerID := ports.ringNode().ID
	startJoinTarget(t, tc, ports, tc.token, func(cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		sealNode(t, cfg, "on", 0x90)
	})

	// What steps 6 and 7 of the build derive, put in place by hand: this test
	// is about the writers, not the derivation. The join's own adoption writes
	// both; the sealed-peer write that follows it must keep them.
	settledPeer := tc.addrs[2]
	joinedAt := map[string]uint64{joinerID: InitialEpoch + 1}
	member.joinedAt.Store(&joinedAt)
	member.settleMu.Lock()
	member.settledOf = map[string]uint64{settledPeer: InitialEpoch}
	member.settleMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}

	// The joiner is a member now, so its first gossip records its sealed store
	// (recordPeerStore -> persistSealedPeers) and rewrites the file.
	deadline := time.Now().Add(20 * time.Second)
	for {
		record, found, err := readMembershipRecord(member.membershipPath())
		if err != nil || !found {
			t.Fatalf("reading node 2's membership record: found=%v err=%v", found, err)
		}
		if slices.Contains(record.SealedPeers, joinerID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node 2 never recorded the joiner %s as sealed; its record names %v", joinerID, record.SealedPeers)
		}
		time.Sleep(50 * time.Millisecond)
	}

	restarted := tc.restartNode(1).server.core.cluster
	if restartedAt := restarted.joinedAtSnapshot()[joinerID]; restartedAt != InitialEpoch+1 {
		t.Errorf("after the sealed-peer write and a restart, node 2 has the joiner joined at %d, want %d: a writer dropped joined_at",
			restartedAt, InitialEpoch+1)
	}
	// Gossip raises the value put in by hand once the members settle the join
	// (join-freeze step 6), so the restart must keep at least it.
	if settled := restarted.settledOfSnapshot()[settledPeer]; settled < InitialEpoch {
		t.Errorf("after the sealed-peer write and a restart, node 2 has %s settled at %d, want at least %d: a writer dropped settled_of",
			settledPeer, settled, InitialEpoch)
	}
}

// TestMembershipJSONHasOneWriter keeps membership.json to one builder and one
// writer (join-freeze build doc, trap 3): saveMembershipRecord is called only
// by writeMembershipRecordLocked and by NewCluster's first write, which builds
// its record with membershipRecordOf; and no production code but
// membershipRecordOf fills in a membershipFile by hand.
func TestMembershipJSONHasOneWriter(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package's sources: %v", err)
	}
	fileSet := token.NewFileSet()
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, source, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", source, err)
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch expr := node.(type) {
				case *ast.CallExpr:
					checkMembershipWriteCall(t, fileSet, function.Name.Name, expr)
				case *ast.CompositeLit:
					if typeName, ok := expr.Type.(*ast.Ident); ok && typeName.Name == "membershipFile" &&
						len(expr.Elts) > 0 && function.Name.Name != "membershipRecordOf" {
						t.Errorf("%s: %s builds a membershipFile by hand; build it with membershipRecordOf so no field is dropped",
							fileSet.Position(expr.Pos()), function.Name.Name)
					}
				}
				return true
			})
		}
	}
}

// checkMembershipWriteCall fails the test for a call of saveMembershipRecord
// from anywhere but the one writer and NewCluster's first write.
func checkMembershipWriteCall(t *testing.T, fileSet *token.FileSet, caller string, call *ast.CallExpr) {
	t.Helper()
	callee, ok := call.Fun.(*ast.Ident)
	if !ok || callee.Name != "saveMembershipRecord" {
		return
	}
	switch caller {
	case "writeMembershipRecordLocked":
	case "NewCluster":
		built, ok := call.Args[1].(*ast.CallExpr)
		if builder, isIdent := built.Fun.(*ast.Ident); !ok || !isIdent || builder.Name != "membershipRecordOf" {
			t.Errorf("%s: NewCluster writes membership.json with a record not built by membershipRecordOf", fileSet.Position(call.Pos()))
		}
	default:
		t.Errorf("%s: %s writes membership.json; only writeMembershipRecord may (and NewCluster's first write)",
			fileSet.Position(call.Pos()), caller)
	}
}

// TestSaveMembershipRecordSyncsTheFileAndTheDirectory: a power
// cut is not available to a test, so this records the two syncs through the
// seams and checks their order: the temporary file is synced before it is
// renamed over membership.json, and the directory after, which is what makes
// the rename itself durable.
func TestSaveMembershipRecordSyncsTheFileAndTheDirectory(t *testing.T) {
	dir := t.TempDir()
	path := membershipPathFor(dir)
	var events []string
	recordInPlace := func() bool {
		record, found, err := readMembershipRecord(path)
		return err == nil && found && record.Epoch == 7
	}
	originalFileSync, originalDirectorySync := syncMembershipFile, syncMembershipDirectory
	t.Cleanup(func() { syncMembershipFile, syncMembershipDirectory = originalFileSync, originalDirectorySync })
	syncMembershipFile = func(file *os.File) error {
		events = append(events, fmt.Sprintf("file %s, record in place %v", filepath.Base(file.Name()), recordInPlace()))
		return file.Sync()
	}
	syncMembershipDirectory = func(syncedDir string) error {
		events = append(events, fmt.Sprintf("directory %s, record in place %v", syncedDir, recordInPlace()))
		return nil
	}

	members := []ring.Node{{ID: "a", Addr: "127.0.0.1:1"}, {ID: "b", Addr: "127.0.0.1:2"}}
	if err := saveMembershipRecord(path, membershipRecordOf(membershipState{epoch: 7, members: members})); err != nil {
		t.Fatalf("saveMembershipRecord: %v", err)
	}
	want := []string{
		"file " + membershipFileName + ".tmp, record in place false",
		"directory " + dir + ", record in place true",
	}
	if strings.Join(events, "\n") != strings.Join(want, "\n") {
		t.Errorf("syncs were\n%s\nwant\n%s", strings.Join(events, "\n"), strings.Join(want, "\n"))
	}
}

// TestAViewOnceDisputedIsNotConfirmedByLaterAgreement is D4 of the
// join-freeze verification: the dispute condition of noteViewAgreement is not
// covered by the other routes' tests. A member of this node's view that once
// answered with another member set at its epoch has shown the view is not the
// cluster's; agreement heard afterwards — here from every other member, each
// confirmed, which meets both routes — must not confirm it. Only adopting a
// membership that lists this node (a join) does. Driven without a network:
// NewCluster, then reconcileMembershipWith as gossip would call it.
func TestAViewOnceDisputedIsNotConfirmedByLaterAgreement(t *testing.T) {
	dir := t.TempDir()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	self, first, second := fabricatedMemberAddr(t), fabricatedMemberAddr(t), fabricatedMemberAddr(t)
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = self
	cfg.Server.NodeID = self
	cfg.Server.ReplicationFactor = 3
	cfg.HashRing.SeedNodes = []string{self, first, second}
	s := &Server{config: cfg, kv: openPositionTestStore(t, t.TempDir())}
	c, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if !c.bootstrapped.Load() {
		t.Fatal("test setup: a node from three seeds is not bootstrapped")
	}
	view := c.memberNodes()
	confirmedPeer := peerGossip{bootstrapped: false}

	// first answers with a member set without second, at this node's epoch.
	disputing := []Peer{{ID: self, Addr: self}, {ID: first, Addr: first}}
	if err := c.reconcileMembershipWith(first, disputing, nil, InitialEpoch, confirmedPeer); err != nil {
		t.Fatalf("a bootstrapped node's reconcile of a disagreeing member: %v", err)
	}
	for _, peer := range []string{first, second} {
		if err := c.reconcileMembershipWith(peer, view, nil, InitialEpoch, confirmedPeer); err != nil {
			t.Fatalf("reconciling an agreeing %s: %v", peer, err)
		}
	}
	if !c.bootstrapped.Load() {
		t.Errorf("a view a member disputed was confirmed by agreement heard afterwards; only a join may confirm it")
	}
}

// TestAFreshNodeFromSeedsIsBootstrappedAndASingleNodeIsNot: a node whose member
// list comes from seeds that name others is bootstrapped until a member confirms
// it; a node whose seeds name only itself, or none, is a single-node cluster
// that nothing could ever confirm.
func TestAFreshNodeFromSeedsIsBootstrappedAndASingleNodeIsNot(t *testing.T) {
	cases := []struct {
		name             string
		seeds            int
		wantBootstrapped bool
	}{
		{name: "three seeds", seeds: 3, wantBootstrapped: true},
		{name: "only itself", seeds: 1, wantBootstrapped: false},
		{name: "no seeds", seeds: 0, wantBootstrapped: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			token, err := GenerateClusterToken()
			if err != nil {
				t.Fatalf("generate cluster token: %v", err)
			}
			if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatalf("save cluster token: %v", err)
			}
			self := fabricatedMemberAddr(t)
			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = dir
			cfg.Server.Listen = self
			cfg.Server.NodeID = self
			cfg.Server.ReplicationFactor = 3
			if testCase.seeds > 0 {
				cfg.HashRing.SeedNodes = []string{self}
			}
			for len(cfg.HashRing.SeedNodes) < testCase.seeds {
				cfg.HashRing.SeedNodes = append(cfg.HashRing.SeedNodes, fabricatedMemberAddr(t))
			}

			s := &Server{config: cfg, kv: openPositionTestStore(t, t.TempDir())}
			c, err := NewCluster(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatalf("NewCluster: %v", err)
			}
			record, found, err := readMembershipRecord(membershipPathFor(dir))
			if err != nil || !found {
				t.Fatalf("reading the first membership record: found=%v err=%v", found, err)
			}
			if record.Bootstrapped != testCase.wantBootstrapped || c.bootstrapped.Load() != testCase.wantBootstrapped {
				t.Errorf("a fresh node with %d seed(s): membership.json says bootstrapped=%v and memory %v, want %v",
					testCase.seeds, record.Bootstrapped, c.bootstrapped.Load(), testCase.wantBootstrapped)
			}
		})
	}
}
