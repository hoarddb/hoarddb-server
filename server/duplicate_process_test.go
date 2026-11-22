package server

import (
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAClonedDataDirectoryNeverRunsAsTheOriginalNode is BUG-0600 part 2: a
// second process started on a copy of a member's data directory, listening on
// another concrete address, found itself in the copied membership.json by node
// id, took the original's listed address as its own and ran as that member.
// With the auth master copied, both processes accepted user changes. The copy
// must refuse to start, naming the id, both addresses and the fix.
func TestAClonedDataDirectoryNeverRunsAsTheOriginalNode(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	masterIndex := tc.masterIndex(t)
	copied := tc.copyNodeDataDir(t, masterIndex)

	clone, ports, err := tc.startProcessOnCopy(t, masterIndex, copied, "127.0.0.1")
	if err == nil {
		cloneIndex := tc.addProcess(clone, ports)
		accepted := tc.userChangesAccepted(t, "bug0600-p2", masterIndex, cloneIndex)
		t.Fatalf("a second process on a copy of %s's data directory, listening on %s, started as %s; "+
			"user changes accepted by %d processes (%s); want the copy refused at start",
			tc.addrs[masterIndex], ports.ringAddr, clone.core.cluster.self.ID, len(accepted), strings.Join(accepted, ", "))
	}
	requireRefusalNames(t, "starting a second process on a copy of "+tc.addrs[masterIndex]+"'s data directory at "+ports.ringAddr, err,
		`"`+tc.addrs[masterIndex]+`"`, ports.ringAddr, "HOARDDB_NODE_ID", "data directory")
}

// TestTwoProcessesUnderOneNodeIDAreReported is BUG-0600 part 1: a copy on a
// wildcard listener cannot be refused at start (BUG-0318: its address never
// equals the listed one), so two processes run under one node id. Nothing in
// the cluster noticed. A member must report it: status names the id, nodes
// marks its row, the duplicate counter moves, and the duplicated id stops
// accepting user changes on both processes, so there are never two auth masters.
func TestTwoProcessesUnderOneNodeIDAreReported(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	masterIndex := tc.masterIndex(t)
	witness := (masterIndex + 1) % 3
	duplicatedID := tc.addrs[masterIndex]
	detectionsBefore := duplicateDetections(t)
	copied := tc.copyNodeDataDir(t, masterIndex)

	clone, ports, err := tc.startProcessOnCopy(t, masterIndex, copied, "0.0.0.0")
	if err != nil {
		t.Fatalf("a copy on a wildcard listener must start (it cannot tell its address from the listed one): %v", err)
	}
	cloneIndex := tc.addProcess(clone, ports)

	deadline := time.Now().Add(45 * time.Second)
	for {
		tc.wakeEveryGossip()
		statusNote := tc.statusDuplicateNote(t, witness, duplicatedID)
		nodesNote := tc.nodesDuplicateNote(t, witness, duplicatedID)
		detections := duplicateDetections(t) - detectionsBefore
		if statusNote != "" && nodesNote != "" && detections > 0 {
			for _, want := range []string{duplicatedID, "two processes", "HOARDDB_NODE_ID", "data directory"} {
				if !strings.Contains(statusNote, want) {
					t.Errorf("status on %s does not name %q:\n%s", tc.addrs[witness], want, statusNote)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("two processes have run as %s for 45 s and member %s has not reported it: status %q, nodes %q, %v new detections",
				duplicatedID, tc.addrs[witness], statusNote, nodesNote, detections)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Each process under the duplicated id hears it from the witness, and
	// neither accepts a user change while both run.
	deadline = time.Now().Add(30 * time.Second)
	for attempt := 0; ; attempt++ {
		tc.wakeEveryGossip()
		accepted := tc.userChangesAccepted(t, "bug0600-p1-"+string(rune('a'+attempt%26)), masterIndex, cloneIndex)
		if len(accepted) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("two processes run as the auth master %s, and 30 s after the witness reported it user changes are still accepted by %s",
				duplicatedID, strings.Join(accepted, ", "))
		}
		time.Sleep(200 * time.Millisecond)
	}
	refusal := tc.userChangeRefusal(t, "bug0600-p1-final", cloneIndex)
	for _, want := range []string{duplicatedID, "two processes", "HOARDDB_NODE_ID"} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the user change refused on the copy does not name %q:\n%s", want, refusal)
		}
	}
}

// TestARestartedNodeIsNotReportedAsADuplicate is the false-positive guard of
// BUG-0600: a node restarted several times in quick succession shows its peers
// a new instance each time, and that is a restart, not a second process.
func TestARestartedNodeIsNotReportedAsADuplicate(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	masterIndex := tc.masterIndex(t)
	restartedID := tc.addrs[masterIndex]
	detectionsBefore := duplicateDetections(t)

	for restart := 0; restart < 4; restart++ {
		tc.restartNode(masterIndex)
		for wake := 0; wake < 3; wake++ {
			tc.wakeEveryGossip()
			time.Sleep(150 * time.Millisecond)
		}
	}

	// Long enough for an interleaving of two instances to count, and for the
	// restarted master to be confirmed and take user changes again.
	quiet := time.Now().Add(6 * time.Second)
	acceptedAgain := false
	for attempt := 0; time.Now().Before(quiet) || !acceptedAgain; attempt++ {
		if time.Now().After(quiet.Add(30 * time.Second)) {
			t.Fatalf("the restarted master %s did not accept a user change again: %s",
				restartedID, tc.userChangeRefusal(t, "bug0600-restart-last", masterIndex))
		}
		tc.wakeEveryGossip()
		for index := range tc.nodes {
			if note := tc.statusDuplicateNote(t, index, restartedID); note != "" {
				t.Fatalf("after restarts of %s, status on %s reports a duplicate: %s", restartedID, tc.addrs[index], note)
			}
			if note := tc.nodesDuplicateNote(t, index, restartedID); note != "" {
				t.Fatalf("after restarts of %s, nodes on %s marks it a duplicate: %s", restartedID, tc.addrs[index], note)
			}
		}
		if !acceptedAgain {
			refusal := tc.userChangeRefusal(t, "bug0600-restart-"+string(rune('a'+attempt%26)), masterIndex)
			if strings.Contains(refusal, "two processes") {
				t.Fatalf("after restarts of %s, a user change is refused as a duplicate: %s", restartedID, refusal)
			}
			acceptedAgain = refusal == ""
		}
		time.Sleep(200 * time.Millisecond)
	}
	if detections := duplicateDetections(t) - detectionsBefore; detections != 0 {
		t.Fatalf("restarting %s four times counted %v duplicate detections, want 0", restartedID, detections)
	}
}

// masterIndex is the index of the node every node places the auth master on.
func (tc *testCluster) masterIndex(t *testing.T) int {
	t.Helper()
	master, err := tc.nodes[0].server.core.authMaster()
	if err != nil {
		t.Fatalf("authMaster: %v", err)
	}
	for index, addr := range tc.addrs {
		if addr == master.ID {
			return index
		}
	}
	t.Fatalf("the auth master %s is not one of the nodes %v", master.ID, tc.addrs)
	return -1
}

// copyNodeDataDir copies a member's whole data directory, keys and
// membership.json included, as a disk image or file-level backup would. The
// node is stopped for the copy so the store's files are consistent, and
// started again on its own directory.
func (tc *testCluster) copyNodeDataDir(t *testing.T, index int) string {
	t.Helper()
	source := tc.nodes[index].cfg.Storage.DataDir
	base := tc.stopNodeKeepingData(index)
	copied := filepath.Join(t.TempDir(), "copy")
	copyTree(t, source, copied)
	if _, err := os.Stat(filepath.Join(copied, "membership.json")); err != nil {
		t.Fatalf("precondition: the copied data directory holds no membership.json: %v", err)
	}
	tc.nodes[index] = tc.startNode(index, base, tc.token)
	return copied
}

// startProcessOnCopy starts a second process with node index's configuration
// on a copy of its data directory, listening on listenHost at fresh ports.
func (tc *testCluster) startProcessOnCopy(t *testing.T, index int, dataDir, listenHost string) (*Service, *testNodePorts, error) {
	t.Helper()
	ports := reserveNodePorts(t)
	cfg := *tc.nodes[index].cfg
	cfg.Storage.DataDir = dataDir
	_, ringPort, err := net.SplitHostPort(ports.ringAddr)
	if err != nil {
		t.Fatalf("ring address %s: %v", ports.ringAddr, err)
	}
	cfg.Server.Listen = net.JoinHostPort(listenHost, ringPort)
	cfg.TCP.Listen = ports.driverAddr
	srv, err := NewService(&cfg, quietLogger())
	if err != nil {
		return nil, ports, err
	}
	srv.driverListener = ports.driver
	srv.internodeListener = ports.internode
	setCapacityProbes(t, srv.core, harnessProbes)
	if err := srv.LoadCatalog(); err != nil {
		t.Fatalf("load catalog on the copy: %v", err)
	}
	if err := startWithRetry(srv, 3*time.Second); err != nil {
		t.Fatalf("start the copy: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return srv, ports, nil
}

// addProcess makes a started process addressable by index, as a node, and
// returns its index.
func (tc *testCluster) addProcess(srv *Service, ports *testNodePorts) int {
	tc.driverAddrs = append(tc.driverAddrs, ports.driverAddr)
	tc.nodes = append(tc.nodes, &testNode{cfg: srv.config, addr: ports.ringAddr, server: srv})
	index := len(tc.nodes) - 1
	tc.nodes[index].client = tc.dial(index)
	return index
}

// wakeEveryGossip runs a gossip round on every running node now.
func (tc *testCluster) wakeEveryGossip() {
	for _, n := range tc.nodes {
		if n.server != nil && n.server.core.cluster != nil {
			n.server.core.cluster.wakeGossip()
		}
	}
}

// userChangesAccepted creates a user through each node in indexes and returns
// the ring addresses of the nodes that accepted it.
func (tc *testCluster) userChangesAccepted(t *testing.T, prefix string, indexes ...int) []string {
	t.Helper()
	var accepted []string
	for _, index := range indexes {
		if tc.userChangeRefusal(t, prefix+"-"+tc.nodes[index].addr, index) == "" {
			accepted = append(accepted, tc.nodes[index].addr)
		}
	}
	return accepted
}

// userChangeRefusal creates a user through a node and returns its refusal,
// or "" when it accepted the change.
func (tc *testCluster) userChangeRefusal(t *testing.T, requestID string, index int) string {
	t.Helper()
	username := "u" + strings.NewReplacer(":", "", ".", "", "-", "").Replace(requestID)
	frame := tc.frame(index, transport.OpUserCreate, clusterCreateRequest(t, requestID, username))
	if frame.Opcode == transport.OpError {
		return string(frame.Payload)
	}
	return ""
}

// statusDuplicateNote is what status on a node says about nodeID answering
// from two processes, or "".
func (tc *testCluster) statusDuplicateNote(t *testing.T, index int, nodeID string) string {
	t.Helper()
	frame := tc.ok(index, transport.OpHealthCheck, HealthRequest{})
	var health bson.M
	if err := bson.Unmarshal(frame.Payload, &health); err != nil {
		t.Fatalf("decode status from %s: %v", tc.nodes[index].addr, err)
	}
	notes, _ := health["duplicates"].(bson.A)
	for _, note := range notes {
		if text, ok := note.(string); ok && strings.Contains(text, nodeID) {
			return text
		}
	}
	return ""
}

// nodesDuplicateNote is the duplicate mark on nodeID's row of nodes on a
// node, or "".
func (tc *testCluster) nodesDuplicateNote(t *testing.T, index int, nodeID string) string {
	t.Helper()
	frame := tc.ok(index, transport.OpListNodes, NodeListRequest{})
	var listing bson.M
	if err := bson.Unmarshal(frame.Payload, &listing); err != nil {
		t.Fatalf("decode nodes from %s: %v", tc.nodes[index].addr, err)
	}
	rows, _ := listing["nodes"].(bson.A)
	for _, row := range rows {
		node, ok := row.(bson.M)
		if !ok || node["nodeid"] != nodeID {
			continue
		}
		if mark, ok := node["duplicate"].(string); ok {
			return mark
		}
	}
	return ""
}

// duplicateDetections reads hoarddb_duplicate_node_id_detected_total, 0 when
// it is not exported.
func duplicateDetections(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "hoarddb_duplicate_node_id_detected_total" {
			continue
		}
		var total float64
		for _, metric := range family.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
		return total
	}
	return 0
}

// copyTree copies the directory tree at src to dst, regular files and
// directories, keeping each file's mode.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, contents, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("copying %s to %s: %v", src, dst, err)
	}
}

// TestARestartKeepsItsListedRecordUnderAnotherName guards BUG-0600 part 2's
// refusal: a node joined as <id>@<a name for its own address>, or listening on
// a wildcard (BUG-0318), still finds its record by id on restart. Only a
// concrete listener at another endpoint is refused.
func TestARestartKeepsItsListedRecordUnderAnotherName(t *testing.T) {
	members := []ring.Node{{ID: "n1", Addr: "127.0.0.1:24433"}, {ID: "n2", Addr: "localhost:24443"}}
	cases := []struct {
		name    string
		listen  string
		refused bool
	}{
		{name: "joined under a host name for its own address", listen: "127.0.0.1:24443"},
		{name: "on a wildcard listener", listen: "0.0.0.0:24453"},
		{name: "a concrete listener on another port", listen: "127.0.0.1:24453", refused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			self := ring.Node{ID: "n2", Addr: normalizeAddr(tc.listen)}
			found, err := locateSelf(members, self, tc.listen, "membership.json")
			if tc.refused {
				requireRefusalNames(t, "n2 listening on "+tc.listen, err, `"n2"`, "localhost:24443", tc.listen, "HOARDDB_NODE_ID")
				return
			}
			if err != nil {
				t.Fatalf("n2 listening on %s was refused its own record: %v", tc.listen, err)
			}
			if found != members[1] {
				t.Fatalf("n2 listening on %s resolved to %v, want %v", tc.listen, found, members[1])
			}
		})
	}
}
