package wire

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The refusal texts of the membership node-side plan (sections 1.5, 2.2, 3.8 and 6). Each names
// the thing, then the exact statement or key that fixes it; every statement it quotes is one the
// CLI grammar accepts (cli.TestOneGrammarForEveryQuotedFix).

func membershipRefusal(code cluster.Code, format string, args ...any) error {
	return &cluster.Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// filled is a refusal text whose placeholders (<node>, <addr>) stay in the source, so the CLI's
// grammar guard parses every statement the text quotes, and are replaced when the text is built.
func filled(code cluster.Code, template string, pairs ...string) error {
	return &cluster.Error{Code: code, Message: strings.NewReplacer(pairs...).Replace(template)}
}

// identityKeyBelongsToAnother is I1: the key pinned under id answered at addr, and says it is another node.
func identityKeyBelongsToAnother(id, addr, other string) error {
	return filled(cluster.CodeClusterMismatch,
		"cannot join <node>@<addr>: the node at <addr> presented the key pinned under <node> but says it is <other>. "+
			"One id has one key: run 'nodes' to see the members, correct hash_ring.seed_fingerprints on this node (and on <node>) "+
			"so <node>=SHA256:<hex> is that node's own key, then restart and retry 'join <node>@<addr>'",
		"<node>", id, "<addr>", addr, "<other>", other)
}

// identityMissingPin is I2: no fingerprint is pinned under the id the operator typed.
func identityMissingPin(id, addr string) error {
	return filled(cluster.CodeInvalidArgument,
		"cannot join <node>@<addr>: no fingerprint is pinned for <node>, so this node cannot tell its key from a stranger's. "+
			"Add <node>=SHA256:<<node>'s fingerprint> to hash_ring.seed_fingerprints (HOARDDB_SEED_FINGERPRINTS) on this node and on every member, "+
			"then restart them, and retry 'join <node>@<addr>'. <node> prints its fingerprint on its start-up line",
		"<node>", id, "<addr>", addr)
}

// identityHoldsOwnCluster is I3: the node holds Raft state and names no cluster.
func identityHoldsOwnCluster(id, addr string) error {
	return filled(cluster.CodeClusterMismatch,
		"cannot join <node>@<addr>: it has bootstrapped a cluster of its own (it holds Raft state and names no cluster), "+
			"so joining it here would run two clusters from one data directory. Stop <node>, start it on a fresh data directory "+
			"with hash_ring.seed_nodes naming this cluster, then retry 'join <node>@<addr>'",
		"<node>", id, "<addr>", addr)
}

// identityAddressDiffers is I4: the node advertises an address other than the one typed.
func identityAddressDiffers(id, typed, advertised string) error {
	return filled(cluster.CodeInvalidArgument,
		"cannot join <node>@<typed>: <node> advertises <addr> (cluster.advertise, else cluster.listen), and its peers will dial that. "+
			"Run 'join <node>@<addr>', or set cluster.advertise on <node> to <typed> and restart it",
		"<node>", id, "<typed>", typed, "<addr>", advertised)
}

// identityClientAddrRefused is I6: the node published a client address the cluster would refuse.
func identityClientAddrRefused(id, addr, client string, cause error) error {
	return filled(cluster.CodeInvalidArgument,
		"cannot join <node>@<addr>: <node> published the client address <client> and the cluster refuses it (<cause>). Nothing was changed. "+
			"Set tcp.advertise on <node> to host:port on the machine it runs on, restart it, then run 'join <node>@<addr>'",
		"<node>", id, "<addr>", addr, "<client>", client, "<cause>", cause.Error())
}

// identityUnreachable is I5: the node did not answer the identity request.
func identityUnreachable(id, addr string, cause error) error {
	return filled(cluster.CodeUnavailable,
		"cannot join <node>@<addr>: <addr> did not answer the identity request (<cause>). Nothing was changed. "+
			"Check that <node> is running with cluster.enabled: true and that <addr> is reachable from this node. "+
			"If it is running, it may hold a cluster of its own or be pinned to other keys, and then it refuses this node's connection before it can answer: "+
			"start <node> on a fresh data directory with hash_ring.seed_fingerprints naming this node's key and <node>=SHA256:<hex> for its own, then retry 'join <node>@<addr>'",
		"<node>", id, "<addr>", addr, "<cause>", fmt.Sprint(cause))
}

// joinStateRangeRefused is the leader's refusal of a joiner that cannot act at the cluster's version
// (mixed-build spec 9.3). Nothing has been registered when it is returned. Before the committed
// cluster version exists the cluster's version is the leader's own Writes, and every build declares
// Min == Max == Writes, so a joiner above it is refused in the same shape (Phase 2 admits no range
// that merely contains the version).
func joinStateRangeRefused(id, addr, build string, joiner, leader cluster.StateRange) error {
	return joinRangeText(joinRefusal{
		ID: id, Addr: addr, Build: build, Joiner: joiner, Leader: leader,
		Release: cluster.ReleaseOf(leader.Writes),
	})
}

// joinRefusal is the join rule's view of one refused joiner: what the text names, and the release
// table's answer for the cluster version (empty when the table records none).
type joinRefusal struct {
	ID, Addr, Build string
	Joiner, Leader  cluster.StateRange
	Release         string
}

// joinRangeText words the refusal of a joiner that cannot act at the leader's version. The release
// is a field, not a lookup, so a test can name a release this build does not record yet.
func joinRangeText(r joinRefusal) error {
	id, addr, build, joiner, release := r.ID, r.Addr, r.Build, r.Joiner, r.Release
	version := r.Leader.Writes
	runs := "node <node> runs build " + build
	if build == "" {
		runs = "node <node> runs a build from before the version check"
	}
	if joiner == (cluster.StateRange{}) {
		runs += ", which sends no state version"
	} else if joiner.Max < version {
		runs += fmt.Sprintf(", which supports cluster versions %d to %d", joiner.Min, joiner.Max)
	} else {
		runs += fmt.Sprintf(", which supports cluster versions %d to %d and is newer than this cluster", joiner.Min, joiner.Max)
	}
	const retry = "then run 'join <node>@<addr>' again."
	var fix string
	switch {
	case joiner.Max > version && joiner != (cluster.StateRange{}):
		// A newer build cannot join an older cluster until the cluster is upgraded (phase 3 admits it).
		if release != "" {
			fix = "A newer build cannot join an older cluster until the cluster is upgraded. Start <node> on " + release + ", the release that carries version " + fmt.Sprint(version) + ", or upgrade every member to <node>'s build first, " + retry
		} else {
			fix = fmt.Sprintf("A newer build cannot join an older cluster until the cluster is upgraded. This build records no release for version %d: run hoarddb-server -v on a cluster member and start <node> on the same build as the cluster, or upgrade every member to <node>'s build first, ", version) + retry
		}
	case release != "":
		fix = "Upgrade <node> to " + release + " or later, " + retry
	default:
		fix = fmt.Sprintf("This build records no release for version %d: run hoarddb-server -v on a cluster member and start <node> on that build or later, ", version) + retry
	}
	parenthetical := ""
	if release != "" {
		parenthetical = " (first released in " + release + ")"
	}
	return filled(cluster.CodeClusterMismatch,
		fmt.Sprintf("cannot join <node>@<addr>: %s, and this cluster runs version %d%s. %s", runs, version, parenthetical, fix),
		"<node>", id, "<addr>", addr)
}

// identityOtherCluster is the conventions CLUSTER_MISMATCH text for a node that belongs to another cluster.
func identityOtherCluster(id, addr string, theirs, ours [16]byte) error {
	return &cluster.Error{Code: cluster.CodeClusterMismatch, Message: cluster.CodeClusterMismatch.Text(cluster.CondClusterMismatch, map[string]string{
		"node": id, "node_cluster": fmt.Sprintf("%x", theirs[:4]), "this_cluster": fmt.Sprintf("%x", ours[:4]), "seed_addr": addr,
	})}
}

// AwaitingAdmissionNotice is the start-up warning of a cluster node that opened with no cluster state
// to join one, and the fix it names. The node cannot tell from its disk whether it is new or a member
// whose ring directory was moved aside, so it says what holds for both: it serves its peers only once
// a member admits it (the BUG-0870 guard), the statement that admits a new node, and the one that
// brings back a member that lost its data.
func AwaitingAdmissionNotice(node, addr string) (notice, fix string) {
	return "this node holds no cluster state, so it serves its peers only once a member admits it",
		strings.NewReplacer("<node>", node, "<addr>", addr).Replace(
			"if <node> is a new node, run 'join <node>@<addr>' on any member; " +
				"if <node> was a member that lost its data, keep the ring directory you moved aside, wait until 'nodes' shows <node> down, " +
				"then run 'replace-node <node> --with <node>@<addr>' on any member; " +
				"if this is the first node, remove the other nodes from hash_ring.seed_nodes (HOARDDB_SEED_NODES) and restart; the first node lists nobody, or only itself")
}

// voterLostItsState refuses a retried join of a joining row that already votes when the node holds no
// Raft state (BUG-0870 guard, review F2).
func voterLostItsState(id, addr string) error {
	return filled(cluster.CodeConflict,
		"cannot join <node>@<addr>: <node> is a voter of this cluster and the node at <addr> holds no cluster state, so it lost the Raft log it voted with. "+
			"Nothing was changed, and it stays out of the cluster. Run 'replace-node <node> --with <node>@<addr>' to bring it back under its own id: "+
			"it re-enters the cluster as a non-voter and pulls its slots from the other copies",
		"<node>", id, "<addr>", addr)
}
