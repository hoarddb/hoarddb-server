package wire

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// RingNodeFacts are the facts describe_node reports that the node cannot know itself.
type RingNodeFacts struct {
	// Version is the server build.
	Version string
	// StartedAt is when the server process started.
	StartedAt time.Time
	// Host reads the machine, or returns nil when it cannot be read; nil leaves host out.
	Host func() *RingAdminHost
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// RingOption configures NewRingHandler.
type RingOption func(*ringServer)

// WithNodeFacts supplies the facts of the server process to describe_node.
func WithNodeFacts(facts RingNodeFacts) RingOption {
	return func(s *ringServer) {
		if facts.Version == "" {
			facts.Version = unknownVersion
		}
		s.facts = facts
	}
}

// WithRingResultBudget sets how many bytes of answers one RING_BATCH reply keeps whole, the
// node's query.max_result_bytes. A value above 16 MiB is clamped to 16 MiB, which keeps the
// reply under the frame ceiling; a value of 0 or less keeps the 16 MiB default.
func WithRingResultBudget(bytes int64) RingOption {
	return func(s *ringServer) {
		if bytes > 0 {
			s.resultBudget = min(bytes, maxRingBatchBudget)
		}
	}
}

// unknownVersion is what a node served without WithNodeFacts calls its build.
const unknownVersion = "unknown"

// defaultNodeFacts are the facts of a handler built without WithNodeFacts (tests and tools that
// serve a Node directly): it started now and has no build or host to report.
func defaultNodeFacts() RingNodeFacts {
	return RingNodeFacts{Version: unknownVersion, StartedAt: time.Now()}
}

// nodeLiveness is what describe_node asks of the Raft group and the lease.
type nodeLiveness interface {
	// Leader is the node id of the leader this node knows, or empty.
	Leader() string
	// LeaseValid reports whether a keyed request would find the node's lease valid.
	LeaseValid() bool
}

// The values of RingAdminNode.Serving.
const (
	// ServingOK means a keyed request would be served.
	ServingOK = "ok"
	// ServingNoQuorum means the lease is valid and no leader is known.
	ServingNoQuorum = "no_quorum"
	// ServingLeaseLapsed means the lease is not valid.
	ServingLeaseLapsed = "lease_lapsed"
	// ServingEpochBehind means the node learned the cluster moved to a later epoch and refuses
	// requests until its own state reaches it: its lease did not lapse.
	ServingEpochBehind = "epoch_behind"
	// ServingReplaying means the node is still applying its Raft log and answers no client frame.
	ServingReplaying = "replaying"
	// ServingDiverged means the node refused a committed entry its leader cannot have appended
	// and is about to exit; it answers no client frame.
	ServingDiverged = "diverged"
)

// RingAdminNode is the reply of describe_node.
type RingAdminNode struct {
	NodeID, Version string
	UptimeMS        int64
	MemberState     string
	Members, Voters int32
	Epoch           int64
	Leader          string
	DefaultRF       int32
	Serving         string
	// StateRange is the state-level range the node declares ("min-max", for example "1-1"), empty
	// when the reply comes from a build that sends none. RefusedPeers is one line per peer the
	// node's acceptors refused for a range that does not overlap its own.
	StateRange   string
	RefusedPeers []string
	// ClusterVersion is the cluster version of the state the node has applied, 0 when it holds
	// none (and then left out of the reply).
	ClusterVersion int32
	// PasswordKey is the node's cluster password key status (ring user store spec 5.2): held,
	// fetching, none_yet or witness; empty on a node that is not a cluster member (and then left out).
	PasswordKey string
	// Host is nil when the server cannot read the machine.
	Host *RingAdminHost
}

// RingAdminHost is the optional host part of describe_node.
type RingAdminHost struct {
	CPUs, HostCPUs int32
	Load           *RingAdminLoad
	MemUsed        int64
	MemTotal       int64
	MemSource      string
	Process        *RingAdminProcess
}

// RingAdminLoad is the host's load average.
type RingAdminLoad struct{ One, Five, Fifteen float64 }

// RingAdminProcess is the memory the process uses and the limit it is measured against.
type RingAdminProcess struct{ Used, Limit int64 }

// The wire documents of describe_node. Member order is the spec's table and is the canonical
// layout; an optional member is a pointer or a zero value that omitempty leaves out.
type adminNodeRequestDoc struct {
	Cmd string `bson:"cmd"`
}

type adminNodeReplyDoc struct {
	Cmd          string   `bson:"cmd"`
	NodeID       string   `bson:"node_id"`
	Version      string   `bson:"version"`
	UptimeMS     int64    `bson:"uptime_ms"`
	MemberState  string   `bson:"member_state"`
	Members      int32    `bson:"members"`
	Voters       int32    `bson:"voters"`
	Epoch        int64    `bson:"epoch"`
	Leader       string   `bson:"leader"`
	DefaultRF    int32    `bson:"default_rf"`
	Serving      string   `bson:"serving"`
	StateRange   string   `bson:"state_range,omitempty"`
	RefusedPeers []string `bson:"refused_peers,omitempty"`
	// ClusterVersion is in the first release's reply, so a CLI of that release decodes every later
	// node's describe_node (replies are decoded strictly: re-encoded and compared).
	ClusterVersion int32         `bson:"cluster_version,omitempty"`
	PasswordKey    string        `bson:"password_key,omitempty"`
	Host           *adminHostDoc `bson:"host,omitempty"`
}

type adminHostDoc struct {
	CPUs         int32    `bson:"cpus"`
	HostCPUs     int32    `bson:"host_cpus,omitempty"`
	Load1        *float64 `bson:"load_1,omitempty"`
	Load5        *float64 `bson:"load_5,omitempty"`
	Load15       *float64 `bson:"load_15,omitempty"`
	MemUsed      int64    `bson:"mem_used,omitempty"`
	MemTotal     int64    `bson:"mem_total,omitempty"`
	MemSource    string   `bson:"mem_source,omitempty"`
	ProcessUsed  *int64   `bson:"process_used,omitempty"`
	ProcessLimit *int64   `bson:"process_limit,omitempty"`
}

func adminNodeReplyDocOf(reply RingAdminReply) adminNodeReplyDoc {
	node := reply.Node
	doc := adminNodeReplyDoc{Cmd: reply.Cmd, NodeID: node.NodeID, Version: node.Version, UptimeMS: node.UptimeMS, MemberState: node.MemberState,
		Members: node.Members, Voters: node.Voters, Epoch: node.Epoch, Leader: node.Leader, DefaultRF: node.DefaultRF, Serving: node.Serving,
		StateRange: node.StateRange, RefusedPeers: node.RefusedPeers, ClusterVersion: node.ClusterVersion, PasswordKey: node.PasswordKey}
	if host := node.Host; host != nil {
		hostDoc := adminHostDoc{CPUs: host.CPUs, HostCPUs: host.HostCPUs, MemUsed: host.MemUsed, MemTotal: host.MemTotal, MemSource: host.MemSource}
		if host.Load != nil {
			hostDoc.Load1, hostDoc.Load5, hostDoc.Load15 = &host.Load.One, &host.Load.Five, &host.Load.Fifteen
		}
		if host.Process != nil {
			hostDoc.ProcessUsed, hostDoc.ProcessLimit = &host.Process.Used, &host.Process.Limit
		}
		doc.Host = &hostDoc
	}
	return doc
}

func adminNodeOf(doc adminNodeReplyDoc) (*RingAdminNode, error) {
	node := &RingAdminNode{NodeID: doc.NodeID, Version: doc.Version, UptimeMS: doc.UptimeMS, MemberState: doc.MemberState,
		Members: doc.Members, Voters: doc.Voters, Epoch: doc.Epoch, Leader: doc.Leader, DefaultRF: doc.DefaultRF, Serving: doc.Serving,
		StateRange: doc.StateRange, RefusedPeers: doc.RefusedPeers, ClusterVersion: doc.ClusterVersion, PasswordKey: doc.PasswordKey}
	if doc.Host == nil {
		return node, nil
	}
	hostDoc := doc.Host
	host := &RingAdminHost{CPUs: hostDoc.CPUs, HostCPUs: hostDoc.HostCPUs, MemUsed: hostDoc.MemUsed, MemTotal: hostDoc.MemTotal, MemSource: hostDoc.MemSource}
	loads := 0
	for _, load := range []*float64{hostDoc.Load1, hostDoc.Load5, hostDoc.Load15} {
		if load != nil {
			loads++
		}
	}
	switch loads {
	case 0:
	case 3:
		host.Load = &RingAdminLoad{One: *hostDoc.Load1, Five: *hostDoc.Load5, Fifteen: *hostDoc.Load15}
	default:
		return nil, ringRefusal("a describe_node host has %d of load_1, load_5 and load_15. Send all three or none", loads)
	}
	if (hostDoc.ProcessUsed == nil) != (hostDoc.ProcessLimit == nil) {
		return nil, ringRefusal("a describe_node host has only one of process_used and process_limit. Send both or neither")
	}
	if hostDoc.ProcessUsed != nil {
		host.Process = &RingAdminProcess{Used: *hostDoc.ProcessUsed, Limit: *hostDoc.ProcessLimit}
	}
	node.Host = host
	return node, nil
}

func checkAdminNode(node *RingAdminNode) error {
	text := func(member, word string, min, max int) error {
		if len(word) < min || len(word) > max || !utf8.ValidString(word) {
			return ringRefusal("a describe_node reply has %s of %d bytes (or not valid UTF-8) and it is %d to %d bytes of UTF-8", member, len(word), min, max)
		}
		return nil
	}
	for _, check := range []error{
		text("node_id", node.NodeID, 1, ringMaxName), text("version", node.Version, 1, ringMaxName),
		text("member_state", node.MemberState, 0, ringMaxAdminState), text("leader", node.Leader, 0, ringMaxName),
		text("state_range", node.StateRange, 0, 32),
	} {
		if check != nil {
			return check
		}
	}
	if len(node.RefusedPeers) > maxAdminRefusedPeers {
		return ringRefusal("a describe_node reply lists %d refused peers and at most %d", len(node.RefusedPeers), maxAdminRefusedPeers)
	}
	for _, line := range node.RefusedPeers {
		if len(line) == 0 || len(line) > 255 || !utf8.ValidString(line) {
			return ringRefusal("a describe_node refused peer line is %d bytes (or not valid UTF-8) and it is 1 to 255 bytes of UTF-8", len(line))
		}
	}
	if node.ClusterVersion < 0 || node.ClusterVersion > 255 {
		return ringRefusal("a describe_node reply has cluster_version %d and a cluster version is 0 (none yet) to 255", node.ClusterVersion)
	}
	switch node.PasswordKey {
	case "", PasswordKeyHeld, PasswordKeyFetching, PasswordKeyNoneYet, PasswordKeyWitness:
	default:
		return ringRefusal("a describe_node reply has password_key %q and it is %s, %s, %s or %s, or absent", node.PasswordKey, PasswordKeyHeld, PasswordKeyFetching, PasswordKeyNoneYet, PasswordKeyWitness)
	}
	if node.UptimeMS < 0 || node.Members < 0 || node.Voters < 0 || node.Epoch < 0 {
		return ringRefusal("a describe_node reply has uptime_ms %d, members %d, voters %d and epoch %d, each 0 or more", node.UptimeMS, node.Members, node.Voters, node.Epoch)
	}
	if node.DefaultRF < 1 {
		return ringRefusal("a describe_node reply has default_rf %d and a replication factor is 1 or more", node.DefaultRF)
	}
	switch node.Serving {
	case ServingOK, ServingNoQuorum, ServingLeaseLapsed, ServingEpochBehind, ServingReplaying, ServingDiverged:
	default:
		return ringRefusal("a describe_node reply has serving %q and it is %s, %s, %s, %s, %s or %s", node.Serving, ServingOK, ServingNoQuorum, ServingLeaseLapsed, ServingEpochBehind, ServingReplaying, ServingDiverged)
	}
	if node.Host == nil {
		return nil
	}
	return checkAdminHost(node.Host)
}

func checkAdminHost(host *RingAdminHost) error {
	if host.CPUs < 1 {
		return ringRefusal("a describe_node host has cpus %d and it is 1 or more. Leave host out when the CPUs are not known", host.CPUs)
	}
	if host.HostCPUs < 0 {
		return ringRefusal("a describe_node host has host_cpus %d and it is 1 or more, or left out", host.HostCPUs)
	}
	if load := host.Load; load != nil {
		for _, one := range []float64{load.One, load.Five, load.Fifteen} {
			if math.IsNaN(one) || math.IsInf(one, 0) || one < 0 {
				return ringRefusal("a describe_node host has a load that is not a number of 0 or more. Send load_1, load_5 and load_15 as numbers of 0 or more")
			}
		}
	}
	if host.MemUsed < 0 || host.MemTotal < 0 || (host.MemUsed > 0 && host.MemTotal == 0) {
		return ringRefusal("a describe_node host has mem_used %d and mem_total %d. mem_used needs mem_total, and neither is negative", host.MemUsed, host.MemTotal)
	}
	if host.MemUsed > host.MemTotal {
		return ringRefusal("a describe_node host has mem_used %d above mem_total %d", host.MemUsed, host.MemTotal)
	}
	if (host.MemTotal > 0) != (host.MemSource != "") {
		return ringRefusal("a describe_node host has mem_total %d and mem_source %q. mem_source goes with mem_total, and only then", host.MemTotal, host.MemSource)
	}
	if host.MemSource != "" && host.MemSource != "host" && host.MemSource != "cgroup" {
		return ringRefusal("a describe_node host has mem_source %q and it is host or cgroup", host.MemSource)
	}
	if process := host.Process; process != nil && (process.Used < 0 || process.Limit < 1) {
		return ringRefusal("a describe_node host has process_used %d and process_limit %d. Send both, the limit 1 or more, or neither", process.Used, process.Limit)
	}
	return nil
}

// refusedPeerLine is one refused peer as describe_node reports it. A handshake id may be 255 bytes,
// and the reply validates each line at 255, so the id is clipped to fit with the range kept.
func refusedPeerLine(row cluster.RefusedPeer) string {
	id := row.NodeID
	const room = 160
	if len(id) > room {
		id = strings.ToValidUTF8(id[:room], "") + "..."
	}
	return fmt.Sprintf("%s (%s channel) speaks %d-%d (writes %d)", id, row.Channel, row.Range.Min, row.Range.Max, row.Range.Writes)
}

// maxAdminRefusedPeers bounds the refused-peer lines of describe_node; the ledger is bounded by
// the member count and a cluster has far fewer than this.
const maxAdminRefusedPeers = 64

// defaultReplicationFactor is the factor create_database gives a database that names none.
const defaultReplicationFactor = 1

// behindMarked is the optional part of nodeLiveness that knows the node was told the cluster
// moved past its epoch. A fake liveness without it is never behind.
type behindMarked interface {
	// Behind reports that the node is refusing requests because it saw a later epoch.
	Behind() bool
}

// loadingMarked is the optional part of a nodeLiveness that knows the node has not loaded its
// log or has diverged; a fake without it never reports either.
type loadingMarked interface {
	// Loading is ServingDiverged, ServingReplaying or empty.
	Loading() string
}

func (n *Node) loadingStage() string {
	marked, ok := n.live.(loadingMarked)
	if !ok {
		return ""
	}
	return marked.Loading()
}

func (n *Node) behindEpoch() bool {
	marked, ok := n.live.(behindMarked)
	return ok && marked.Behind()
}

// nodeLive is the nodeLiveness of a running Node: the Raft group's leader and the node's own
// lease, checked the way a keyed request checks it.
type nodeLive struct {
	node  *Node
	clock bootclock.Clock
}

// Loading implements loadingMarked.
func (l nodeLive) Loading() string {
	switch {
	case l.node.raft.IsDiverged():
		return ServingDiverged
	case !l.node.raft.StateLoaded():
		return ServingReplaying
	}
	return ""
}

// Leader implements nodeLiveness.
func (l nodeLive) Leader() string { return l.node.raft.Leader() }

// LeaseValid implements nodeLiveness.
func (l nodeLive) LeaseValid() bool {
	return l.node.lease.Serve(machine.ServeCheck{At: l.clock.BootNow(), CommittedEpoch: l.node.topology.Committed()}).Valid
}

// describeNode answers describe_node. It reads the committed view (one atomic load), the Raft
// leader and the lease, and the facts of the server process; it opens no bucket and reads no
// key, so its cost does not depend on how many buckets the node holds.
func (n *Node) describeNode(facts RingNodeFacts) RingAdminNode {
	view := n.topology.view.Load()
	now := time.Now
	if facts.Now != nil {
		now = facts.Now
	}
	leader := n.live.Leader()
	serving := ServingOK
	switch stage := n.loadingStage(); {
	// Before everything else: a node that has not loaded its log, or has diverged, answers no
	// client frame whatever its lease and leader say, on one member as on many.
	case stage != "":
		serving = stage
	// A one-member group serves a keyed request whatever its leader or lease say (machine.Check asks for the lease only above one).
	case view.members <= 1:
	case n.behindEpoch():
		serving = ServingEpochBehind
	case !n.live.LeaseValid():
		serving = ServingLeaseLapsed
	case leader == "":
		serving = ServingNoQuorum
	}
	node := RingAdminNode{
		NodeID: n.cfg.NodeID, Version: facts.Version, UptimeMS: max(0, now().Sub(facts.StartedAt).Milliseconds()),
		MemberState: view.selfState, Members: int32(view.members), Voters: int32(view.voters), Epoch: int64(view.epoch),
		Leader: leader, DefaultRF: defaultReplicationFactor, Serving: serving,
		StateRange: fmt.Sprintf("%d-%d", n.stateRange().Min, n.stateRange().Max), ClusterVersion: int32(view.clusterVersion),
		PasswordKey: n.PasswordKeyStatus(),
	}
	for _, row := range n.RefusedPeers() {
		node.RefusedPeers = append(node.RefusedPeers, refusedPeerLine(row))
	}
	if len(node.RefusedPeers) > maxAdminRefusedPeers {
		node.RefusedPeers = node.RefusedPeers[:maxAdminRefusedPeers]
	}
	if facts.Host != nil {
		node.Host = facts.Host()
	}
	return node
}

// Behind implements behindMarked: the lease knows whether this node saw a later epoch.
func (l nodeLive) Behind() bool {
	reporter, ok := l.node.lease.(BehindReporter)
	if !ok {
		return false
	}
	_, behind := reporter.BehindEpoch(l.node.topology.Committed())
	return behind
}
