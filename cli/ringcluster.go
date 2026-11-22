package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringAloneSetupNote closes nodes and status on a node that holds no cluster state: no epoch, no leader. cluster.enabled
// and cluster.listen are the keys that make it form one, so the text names them at the moment of asking.
const ringAloneSetupNote = "This node is not in a cluster. Set 'cluster.enabled: true' (or HOARDDB_CLUSTER_ENABLED=1) " +
	"and 'cluster.listen' to this node's address, then restart it to form one."

// ringAloneMemberNote closes nodes and status on a bootstrapped cluster of one: cluster.enabled is already set, so the
// only fix worth naming is the statement that adds a member.
const ringAloneMemberNote = "This node is the only member of its cluster. Add others with 'join <node>@<addr>' (for example 'join node2@node2:4433')."

// ringAloneNote picks the closing line for a node with at most one member. A node that has an epoch or a leader holds
// cluster state, so telling it to enable clustering names a fix for a problem it does not have.
func ringAloneNote(epoch int64, leader string) string {
	if epoch >= 1 || leader != "" {
		return ringAloneMemberNote
	}
	return ringAloneSetupNote
}

// clusterVersionText is status's cluster version fact: the number, and the release that first
// carried it when this CLI's release table records one (cluster-version spec 7.3,
// "Cluster version: <V> (<release>)"). A CLI is the node's release or newer, so its table knows
// every release the node can be at; a level no release records prints the number alone.
func clusterVersionText(version int32, releaseOf func(level uint8) string) string {
	if version < 1 || version > 255 {
		return fmt.Sprint(version)
	}
	if release := releaseOf(uint8(version)); release != "" {
		return fmt.Sprintf("%d (%s)", version, terminalSafe(release))
	}
	return fmt.Sprint(version)
}

// nodes lists the members the node holds, one row each, then the voter count and the view's numbers.
//
// SPEC-GAP C3-1: the topology reply carries id, state and data address only, so ROLE cannot tell a data node
// from a witness and the Voters line cannot name which members vote. ROLE is "member" for every row and the
// Voters line is the count describe_node reports; both change when unit C5 puts a voter flag on the wire.
func (s *RingSession) nodes(ctx context.Context) (RingOutput, error) {
	view, err := s.api.ClusterView(ctx)
	if err != nil {
		return RingOutput{}, err
	}
	if len(view.Members) == 0 {
		return RingOutput{}, ringRefusal("", "this node is not in a cluster, so there are no members to show. "+
			"Set 'cluster.enabled: true' (or HOARDDB_CLUSTER_ENABLED=1), set 'cluster.listen' to this node's address, "+
			"then restart it; 'status' shows this node's own view")
	}
	rows := make([][3]string, len(view.Members))
	widths := [3]int{len("NODE"), len("STATE"), len("ADDRESS")}
	for i, member := range view.Members {
		rows[i] = [3]string{terminalSafe(member.ID), terminalSafe(member.State), terminalSafe(nodeAddress(member))}
		for column, cell := range rows[i] {
			widths[column] = max(widths[column], len(cell))
		}
	}
	line := func(cells [3]string, role string) string {
		return fmt.Sprintf("%-*s  %-*s  %-*s  %s", widths[0], cells[0], widths[1], cells[1], widths[2], cells[2], role)
	}
	lines := []string{line([3]string{"NODE", "STATE", "ADDRESS"}, "ROLE")}
	for _, cells := range rows {
		lines = append(lines, line(cells, "member"))
	}
	// A node that cannot describe itself leaves the Voters line out rather than guess it.
	node, describeErr := s.api.DescribeNode(ctx)
	if describeErr == nil && node.Voters > 0 {
		lines = append(lines, fmt.Sprintf("Voters: %d of %d members", node.Voters, len(view.Members)))
	}
	lines = append(lines, fmt.Sprintf("epoch %d, state_seq %d", view.Epoch, view.StateSeq))
	if len(view.Members) == 1 {
		lines = append(lines, ringAloneNote(max(view.Epoch, node.Epoch), node.Leader))
	}
	return RingOutput{Lines: lines}, nil
}

// nodeAddress is the address 'nodes' prints for a member: the one a driver connects to when the member
// published one. Otherwise it prints the cluster address, which is also what 'join' takes, marked (cluster)
// so an operator does not read it as a driver address. It is for reading; nothing dials it, because the
// cluster address serves no driver.
func nodeAddress(member ringclient.Member) string {
	if member.Addr != "" {
		return member.Addr
	}
	if member.DataAddr == "" {
		return ""
	}
	return member.DataAddr + " (cluster)"
}

// clusterStatus is the 'status' statement: the node's own view of the cluster, from describe_node.
func (s *RingSession) clusterStatus(ctx context.Context) (RingOutput, error) {
	node, err := s.api.DescribeNode(ctx)
	if err != nil {
		return RingOutput{}, &RingError{Message: "this server does not answer describe_node, so there is no status to show. " +
			"Run 'nodes' to list the members", Exit: ExitRefused, Err: err}
	}
	name := terminalSafe(node.NodeID)
	if node.MemberState != "" {
		name += " (" + terminalSafe(node.MemberState) + ")"
	}
	leader := "none"
	if node.Leader != "" {
		leader = terminalSafe(node.Leader)
	}
	facts := []fact{{"Node", name}, {"Epoch", fmt.Sprint(node.Epoch)}, {"Leader", leader}, {"Serving", ringServing(node.Serving)},
		{"Voters", fmt.Sprintf("%d of %d members", node.Voters, node.Members)}}
	if node.StateRange != "" {
		facts = append(facts, fact{"State version", terminalSafe(node.StateRange)})
	}
	if node.ClusterVersion > 0 {
		facts = append(facts, fact{"Cluster version", clusterVersionText(node.ClusterVersion, cluster.ReleaseOf)})
	}
	if node.PasswordKey != "" {
		facts = append(facts, fact{"Password key", passwordKeyText(node.PasswordKey)})
	}
	lines := strings.Split(strings.TrimRight(formatFacts(facts), "\n"), "\n")
	if len(node.RefusedPeers) > 0 {
		lines = append(lines, "Refused peers (another build; hoarddb-server -v prints state_level on each):")
		for _, line := range node.RefusedPeers {
			lines = append(lines, "  "+terminalSafe(line))
		}
	}
	if node.Members <= 1 {
		lines = append(lines, ringAloneNote(node.Epoch, node.Leader))
	}
	return RingOutput{Lines: lines}, nil
}

// passwordKeyText words describe_node's password_key (ring user store spec 5.2); "held" stays the
// bare word a script after 'join' waits for.
func passwordKeyText(status string) string {
	switch status {
	case "fetching":
		return "fetching (logins wait until this node receives the cluster's key)"
	case "none_yet":
		return "none yet (the cluster has no users)"
	case "witness":
		return "witness (serves no logins)"
	}
	return terminalSafe(status)
}

// ringServing words describe_node's serving value; a not-serving node is told which command shows why.
func ringServing(serving string) string {
	switch serving {
	case "ok":
		return "ok"
	case "no_quorum":
		return "no quorum: no leader is known. A majority of the voters must be up and reach each other; run 'nodes' to see which members are down"
	case "lease_lapsed":
		return "lease lapsed: reads and writes are refused until it renews. Run 'nodes' to see which members are down"
	case "epoch_behind":
		return "behind the cluster: this node learned of a later epoch and refuses requests until it has applied it. It catches up by itself in seconds; if it does not, run 'nodes' to see whether it still reaches the leader"
	case "replaying":
		return "replaying its log: this node is applying its saved cluster state and serves nothing yet. It finishes by itself; if it does not, read the server log"
	case "diverged":
		return "diverged: this node refused a committed entry its leader cannot have appended and is stopping. Start it on the build the leader runs; keep its data directory"
	}
	return terminalSafe(serving)
}
