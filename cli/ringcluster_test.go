package cli

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// c3Cluster is a four-member view as node-b sees it: node-c is down.
func c3Cluster() (ringclient.NodeDescription, *ringclient.Topology) {
	node := u84ClusterNode()
	view := &ringclient.Topology{Epoch: 4, StateSeq: 9, Members: []ringclient.Member{
		{ID: "node-a", Addr: "192.0.2.10:7433", State: "active"},
		{ID: "node-b", Addr: "192.0.2.11:7433", State: "active"},
		{ID: "node-c", Addr: "192.0.2.12:7433", State: "down"},
		{ID: "node-d", Addr: "192.0.2.13:7433", State: "joining"},
	}}
	return node, view
}

func c3Run(t *testing.T, session *RingSession, statement string) string {
	t.Helper()
	output, err := session.Execute(context.Background(), statement)
	if err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	return strings.Join(output.Lines, "\n") + "\n"
}

// TestC3NodesPrintsTheMembers: [GUARD columns NODE STATE ADDRESS ROLE, one row per member in the view's
// order, ROLE is member for all, the Voters line comes from describe_node, the last line is epoch and state_seq.]
func TestC3NodesPrintsTheMembers(t *testing.T) {
	session, fake := u52Session()
	fake.node, fake.view = c3Cluster()
	want := "NODE    STATE    ADDRESS          ROLE\n" +
		"node-a  active   192.0.2.10:7433  member\n" +
		"node-b  active   192.0.2.11:7433  member\n" +
		"node-c  down     192.0.2.12:7433  member\n" +
		"node-d  joining  192.0.2.13:7433  member\n" +
		"Voters: 3 of 4 members\n" +
		"epoch 4, state_seq 9\n"
	if got := c3Run(t, session, "nodes"); got != want {
		t.Errorf("nodes:\n got  %q\n want %q", got, want)
	}
}

// TestC3NodesOnASingleNode: [GUARD one member in a formed cluster says it is the only member and names the join statement.]
func TestC3NodesOnASingleNode(t *testing.T) {
	session, _ := u52Session()
	got := c3Run(t, session, "nodes")
	for _, want := range []string{"node-a  active  192.0.2.10:7433  member\n", "Voters: 1 of 1 members\n", "epoch 1, state_seq 3\n",
		"This node is the only member of its cluster. Add others with 'join <node>@<addr>' (for example 'join node2@node2:4433').\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("nodes on one node lacks %q:\n%s", want, got)
		}
	}
}

// TestC3NodesWithoutMembersNamesTheFix: [GUARD an empty view is a refusal with the fix, not an empty table.]
func TestC3NodesWithoutMembersNamesTheFix(t *testing.T) {
	session, fake := u52Session()
	fake.view = &ringclient.Topology{Epoch: 1, StateSeq: 1}
	_, err := session.Execute(context.Background(), "nodes")
	if err == nil || !strings.Contains(err.Error(), "this node is not in a cluster, so there are no members to show. Set 'cluster.enabled: true' (or HOARDDB_CLUSTER_ENABLED=1), set 'cluster.listen' to this node's address, then restart it; 'status' shows this node's own view") {
		t.Errorf("empty view: %v", err)
	}
}

// TestC3NodesOmitsVotersWhenTheNodeCannotSay: [GUARD no invented data: describe_node failing leaves the Voters line out.]
func TestC3NodesOmitsVotersWhenTheNodeCannotSay(t *testing.T) {
	session, fake := u52Session()
	fake.node, fake.view = c3Cluster()
	fake.failures["DescribeNode"] = &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: "no"}
	got := c3Run(t, session, "nodes")
	if strings.Contains(got, "Voters") || !strings.HasSuffix(got, "epoch 4, state_seq 9\n") {
		t.Errorf("nodes without describe_node:\n%s", got)
	}
}

// TestC3NodesErrorsKeepTheServerRefusal: [GUARD a refused topology call keeps its code.]
func TestC3NodesErrorsKeepTheServerRefusal(t *testing.T) {
	session, fake := u52Session()
	fake.failures["ClusterView"] = &ringclient.Error{Code: cluster.CodeUnavailable, Message: "down"}
	_, err := session.Execute(context.Background(), "nodes")
	var refused *RingError
	if !errors.As(ringFailure(err), &refused) || refused.Code != cluster.CodeUnavailable.String() {
		t.Errorf("nodes failure: %v", err)
	}
}

// TestC3StatusInACluster: [GUARD the five facts in order, the leader from describe_node, voters of members.]
func TestC3StatusInACluster(t *testing.T) {
	session, fake := u52Session()
	fake.node, fake.view = c3Cluster()
	want := "Node:    node-a (active)\n" +
		"Epoch:   4\n" +
		"Leader:  node-b\n" +
		"Serving: ok\n" +
		"Voters:  3 of 4 members\n"
	if got := c3Run(t, session, "status"); got != want {
		t.Errorf("status:\n got  %q\n want %q", got, want)
	}
}

// TestC3StatusNamesTheFixWhenNotServing: [GUARD no_quorum and lease_lapsed each print their own fix line and Leader none.]
func TestC3StatusNamesTheFixWhenNotServing(t *testing.T) {
	for serving, want := range map[string]string{
		"no_quorum":    "Serving: no quorum: no leader is known. A majority of the voters must be up and reach each other; run 'nodes' to see which members are down\n",
		"lease_lapsed": "Serving: lease lapsed: reads and writes are refused until it renews. Run 'nodes' to see which members are down\n",
		"epoch_behind": "Serving: behind the cluster: this node learned of a later epoch and refuses requests until it has applied it.",
		"replaying":    "Serving: replaying its log: this node is applying its saved cluster state and serves nothing yet.",
		"diverged":     "Serving: diverged: this node refused a committed entry its leader cannot have appended and is stopping.",
	} {
		session, fake := u52Session()
		fake.node, _ = c3Cluster()
		fake.node.Serving, fake.node.Leader = serving, ""
		got := c3Run(t, session, "status")
		if !strings.Contains(got, want) || !strings.Contains(got, "Leader:  none\n") {
			t.Errorf("status %s:\n%s", serving, got)
		}
	}
}

// TestC3StatusOnASingleNode: [GUARD a one-member cluster with a leader says it is the only member and names join, never 'not in a cluster'.]
func TestC3StatusOnASingleNode(t *testing.T) {
	session, _ := u52Session()
	got := c3Run(t, session, "status")
	if !strings.Contains(got, "Voters:  1 of 1 members\n") ||
		!strings.HasSuffix(got, "This node is the only member of its cluster. Add others with 'join <node>@<addr>' (for example 'join node2@node2:4433').\n") {
		t.Errorf("status on one node:\n%s", got)
	}
}

// TestC3StatusWithoutDescribeNode: [GUARD an older server gets a refusal that names the next command.]
func TestC3StatusWithoutDescribeNode(t *testing.T) {
	session, fake := u52Session()
	fake.failures["DescribeNode"] = &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: "no"}
	_, err := session.Execute(context.Background(), "status")
	if err == nil || !strings.Contains(err.Error(), "does not answer describe_node") || !strings.Contains(err.Error(), "Run 'nodes'") {
		t.Errorf("status without describe_node: %v", err)
	}
}

// TestC3OtherClusterStatementsStayUnavailable: [GUARD only nodes, status, join, promote and
// replace-node are enabled; join and promote arrived with membership slice A, replace-node with
// BUG-0870.]
func TestC3OtherClusterStatementsStayUnavailable(t *testing.T) {
	for _, statement := range []string{"leave node-b", "remove-node node-b"} {
		session, _ := u52Session()
		if _, err := session.Execute(context.Background(), statement); err == nil || !strings.Contains(err.Error(), "not available yet") {
			t.Errorf("%s: %v", statement, err)
		}
	}
}

// TestC3NodeAddressLabelsTheClusterFallback: [GUARD a member with no client address shows its cluster address
// marked (cluster), so an operator does not take it for the driver address; a member with one prints it unlabeled.]
func TestC3NodeAddressLabelsTheClusterFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		member ringclient.Member
		want   string
	}{
		{"client address printed plain", ringclient.Member{ID: "node-a", Addr: "192.0.2.2:4433", DataAddr: "192.0.2.2:7433"}, "192.0.2.2:4433"},
		{"cluster fallback labelled", ringclient.Member{ID: "node-b", DataAddr: "192.0.2.2:4433"}, "192.0.2.2:4433 (cluster)"},
		{"no address at all stays empty", ringclient.Member{ID: "node-c"}, ""},
	} {
		if got := nodeAddress(tc.member); got != tc.want {
			t.Errorf("%s: nodeAddress = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestC3NodesLabelsAClusterFallbackInTheTable: [GUARD the ADDRESS column widens for the label and only the
// member without a client address carries it.]
func TestC3NodesLabelsAClusterFallbackInTheTable(t *testing.T) {
	session, fake := u52Session()
	fake.node, _ = c3Cluster()
	fake.view = &ringclient.Topology{Epoch: 2, StateSeq: 5, Members: []ringclient.Member{
		{ID: "node-a", Addr: "192.0.2.10:7433", State: "active"},
		{ID: "node-b", DataAddr: "192.0.2.11:4433", State: "active"},
	}}
	got := c3Run(t, session, "nodes")
	gapToAddress := strings.Repeat(" ", len("192.0.2.11:4433 (cluster)")-len("192.0.2.10:7433"))
	for _, want := range []string{
		"node-a  active  192.0.2.10:7433" + gapToAddress + "  member\n",
		"node-b  active  192.0.2.11:4433 (cluster)  member\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("nodes lacks %q:\n%s", want, got)
		}
	}
}

// TestC3HelpListsNodesAndStatus: [GUARD both are in help all, completed after the first letters and offered as topics.]
func TestC3HelpListsNodesAndStatus(t *testing.T) {
	all := HelpAll()
	for _, want := range []string{"\n  nodes ", "\n  status "} {
		if !strings.Contains(all, want) {
			t.Errorf("help all lacks %q", strings.TrimSpace(want))
		}
	}
	if got := Completions("nod", ""); len(got) != 1 || got[0] != "nodes" {
		t.Errorf("completing nod: %v", got)
	}
	if got := Completions("stat", ""); len(got) != 1 || got[0] != "status" {
		t.Errorf("completing stat: %v", got)
	}
	page, err := HelpFor("nodes")
	if err != nil || strings.Contains(page, "Not available yet") {
		t.Errorf("help nodes: %v %q", err, page)
	}
}

// TestLiveNodesAndStatusOnARealNode runs both statements against the real in-process ring server (one node).
func TestLiveNodesAndStatusOnARealNode(t *testing.T) {
	session, ctx := u52LiveSession(t)
	nodes, err := session.Execute(ctx, "nodes")
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	text := strings.Join(nodes.Lines, "\n")
	t.Logf("\n%s", text)
	for _, want := range []string{"NODE", "STATE", "ADDRESS", "ROLE", "member", "active", "epoch ", "state_seq ", "only member of its cluster"} {
		if !strings.Contains(text, want) {
			t.Errorf("live nodes lacks %q:\n%s", want, text)
		}
	}
	status, err := session.Execute(ctx, "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	text = strings.Join(status.Lines, "\n")
	t.Logf("\n%s", text)
	for _, want := range []string{"Node:", "Epoch:", "Leader:", "Voters:", "only member of its cluster"} {
		if !strings.Contains(text, want) {
			t.Errorf("live status lacks %q:\n%s", want, text)
		}
	}
	// The value column is padded to the longest label, which grows with the rows a node reports,
	// so the fact is checked as a whole line with any run of spaces before the value.
	if !regexp.MustCompile(`(?m)^Serving:\s+ok$`).MatchString(text) {
		t.Errorf("live status does not say Serving is ok:\n%s", text)
	}
}

// TestStatusShowsTheClusterVersion is the CLI half of cluster-version test A18: status prints the
// cluster version describe_node reports, and prints nothing for a node that holds no state yet.
func TestStatusShowsTheClusterVersion(t *testing.T) {
	session, fake := u52Session()
	fake.node, fake.view = c3Cluster()
	fake.node.ClusterVersion = 2
	if got := c3Run(t, session, "status"); !regexp.MustCompile(`(?m)^Cluster version:\s+2$`).MatchString(got) {
		t.Errorf("status does not print the cluster version:\n%s", got)
	}
	fake.node.ClusterVersion = 0
	if got := c3Run(t, session, "status"); strings.Contains(got, "Cluster version") {
		t.Errorf("status prints a cluster version for a node with no state:\n%s", got)
	}
}

// TestTheClusterVersionFactNamesItsRelease: status prints "<V> (<release>)" when this CLI's
// release table records the release that first carried V, and the number alone when it records
// none (always, before the first release). Against: the number alone whatever the table says.
func TestTheClusterVersionFactNamesItsRelease(t *testing.T) {
	releases := func(level uint8) string {
		if level == 1 {
			return "hoardDB 1.1"
		}
		return ""
	}
	if got := clusterVersionText(1, releases); got != "1 (hoardDB 1.1)" {
		t.Errorf("a recorded release prints %q", got)
	}
	if got := clusterVersionText(2, releases); got != "2" {
		t.Errorf("a level with no release prints %q", got)
	}
}

func TestStatusShowsTheRangeAndRefusedPeers(t *testing.T) {
	session, fake := u52Session()
	fake.node, fake.view = c3Cluster()
	fake.node.StateRange = "1-1"
	fake.node.RefusedPeers = []string{"node-c (raft channel) speaks 0-0 (writes 0)"}
	want := "Node:          node-a (active)\n" +
		"Epoch:         4\n" +
		"Leader:        node-b\n" +
		"Serving:       ok\n" +
		"Voters:        3 of 4 members\n" +
		"State version: 1-1\n" +
		"Refused peers (another build; hoarddb-server -v prints state_level on each):\n" +
		"  node-c (raft channel) speaks 0-0 (writes 0)\n"
	if got := c3Run(t, session, "status"); got != want {
		t.Errorf("status:\n got  %q\n want %q", got, want)
	}
}

// TestStatusOnANodeWithNoClusterStateKeepsTheSetupNote: [GUARD only a node with no cluster state (no members,
// epoch 0, no leader) is told to set cluster.enabled; a bootstrapped cluster of one is not.]
func TestStatusOnANodeWithNoClusterStateKeepsTheSetupNote(t *testing.T) {
	session, fake := u52Session()
	fake.node = ringclient.NodeDescription{NodeID: "node-a", Serving: "no_quorum"}
	got := c3Run(t, session, "status")
	if !strings.HasSuffix(got, ringAloneSetupNote+"\n") {
		t.Errorf("status on a node with no cluster state:\n%s", got)
	}
	session, _ = u52Session()
	got = c3Run(t, session, "status")
	if strings.Contains(got, "not in a cluster") || strings.Contains(got, "cluster.enabled") {
		t.Errorf("status on a bootstrapped cluster of one names a fix for a problem it does not have:\n%s", got)
	}
}

// TestStatusShowsThePasswordKey (ring user store spec 5.2, B22): status prints the node's
// password_key in words, so a script after 'join' can wait for "held", and prints nothing for a
// node that is not a cluster member.
func TestStatusShowsThePasswordKey(t *testing.T) {
	session, fake := u52Session()
	fake.node, fake.view = c3Cluster()
	for status, want := range map[string]string{
		"held":     `(?m)^Password key:\s+held$`,
		"fetching": `(?m)^Password key:\s+fetching \(logins wait until this node receives the cluster's key\)$`,
		"none_yet": `(?m)^Password key:\s+none yet \(the cluster has no users\)$`,
		"witness":  `(?m)^Password key:\s+witness \(serves no logins\)$`,
	} {
		fake.node.PasswordKey = status
		if got := c3Run(t, session, "status"); !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("status for password_key %s does not match %s:\n%s", status, want, got)
		}
	}
	fake.node.PasswordKey = ""
	if got := c3Run(t, session, "status"); strings.Contains(got, "Password key") {
		t.Errorf("status prints a password key for a node that is not a cluster member:\n%s", got)
	}
}
