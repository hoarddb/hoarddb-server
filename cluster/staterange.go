package cluster

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// RangeOf reads the state-level range a handshake declares. All three zero is level 0: a sender
// from before the check, or an identity dial, which declares none on purpose.
func RangeOf(hello transport.Handshake) StateRange {
	return StateRange{Min: hello.Min, Max: hello.Max, Writes: hello.Writes}
}

// Stamp returns hello with this build's range in it. The handshake is the transport's and the
// range is the cluster's, so every production construction goes through here.
func (r StateRange) Stamp(hello transport.Handshake) transport.Handshake {
	hello.Min, hello.Max, hello.Writes = r.Min, r.Max, r.Writes
	return hello
}

// releaseClause is the parenthesis of the refusal texts that names the first release of level,
// or says that this build records none and how to find the build.
func releaseClause(level uint8, node string) string {
	if release := ReleaseOf(level); release != "" {
		return fmt.Sprintf("(first released in %s)", release)
	}
	return fmt.Sprintf("(this build records no release for version %d; run hoarddb-server -v on a cluster member and start %s on that build or later)", level, node)
}

// StateRangeRefusal is the text of a peer whose range does not overlap this build's, as 9.4 text 1
// of the mixed-build spec words it. channel is the channel word the connection used.
func StateRangeRefusal(channel, node string, cluster, peer StateRange) string {
	clusterLevel := cluster.Writes
	if peer == (StateRange{}) {
		if release := ReleaseOf(clusterLevel); release != "" {
			return fmt.Sprintf("refused %s connection from node %q: it sends no state version, so it is a build from before this check; upgrade it to %s or later. This cluster runs version %d. Builds that differ are never mixed before the first release.", channel, node, release, clusterLevel)
		}
		return fmt.Sprintf("refused %s connection from node %q: it sends no state version, so it is a build from before this check; start %s on the build the cluster runs (hoarddb-server -v prints state_level on each). This cluster runs version %d. Builds that differ are never mixed before the first release.", channel, node, node, clusterLevel)
	}
	// A peer that writes above this build's Max has seen a version this build cannot run. This node
	// cannot tell from the handshake whether the cluster moved past it or the peer's build is too
	// new for the cluster, so the text gives the operator the check and the fix of each. The text
	// ships frozen in release 1 (cluster-version spec 4.8), where it is the only branch a valid
	// later-release range can reach.
	if peer.Valid() && peer.Writes > cluster.Max {
		return peerWritesAboveMaxRefusal(channel, node, cluster, peer)
	}
	// Newer is decided by the peer's top level, as the join rule decides it (wire.joinStateRangeRefused):
	// a peer that reaches above the cluster cannot act at the cluster and must not be told to upgrade.
	if peer.Max > clusterLevel {
		start := fmt.Sprintf("run hoarddb-server -v on a cluster member and start %s on that build", node)
		if release := ReleaseOf(clusterLevel); release != "" {
			start = fmt.Sprintf("start %s on %s, the release that carries version %d", node, release, clusterLevel)
		}
		return fmt.Sprintf("refused %s connection from node %q: it speaks state versions %d-%d (writes %d) and it is newer than this cluster, which runs version %d. A newer build cannot join an older cluster until the cluster is upgraded: %s, or upgrade every member to its build first. hoarddb-server -v prints state_level on each.",
			channel, node, peer.Min, peer.Max, peer.Writes, clusterLevel, start)
	}
	upgrade := ""
	if release := ReleaseOf(clusterLevel); release != "" {
		upgrade = fmt.Sprintf(" Upgrade %s to %s or later;", node, release)
	}
	return fmt.Sprintf("refused %s connection from node %q: it speaks state versions %d-%d (writes %d) and this cluster runs version %d %s, which it cannot act at.%s hoarddb-server -v prints state_level on each. Builds that differ are never mixed before the first release.",
		channel, node, peer.Min, peer.Max, peer.Writes, clusterLevel, releaseClause(clusterLevel, node), upgrade)
}

// peerWritesAboveMaxRefusal is StateRangeRefusal's text for a peer whose Writes is above this
// build's Max. It names this node's version as this node's, never as the cluster's: the peer may
// hold a newer committed cluster version than this node does.
func peerWritesAboveMaxRefusal(channel, node string, own, peer StateRange) string {
	upgradeTo := fmt.Sprintf("a build whose state_range includes %d", peer.Writes)
	if release := ReleaseOf(peer.Writes); release != "" {
		upgradeTo = release + " or later"
	}
	startOn := fmt.Sprintf("a build whose state_range includes %d", own.Writes)
	if release := ReleaseOf(own.Writes); release != "" {
		startOn += fmt.Sprintf(" (%s carries it)", release)
	}
	return fmt.Sprintf("refused %s connection from node %q: it writes cluster version %d, above the highest this build runs (%d). "+
		"If the cluster has moved to version %d ('status' against %s prints its Cluster version), this node's build is the older one: "+
		"upgrade this node, and every member still on its build, to %s, then restart it. "+
		"If the cluster still runs version %d, %s is too new for it: start %s on %s. "+
		"hoarddb-server -v prints state_range on each.",
		channel, node, peer.Writes, own.Max, peer.Writes, node, upgradeTo, own.Writes, node, node, startOn)
}

// RefusedPeer is one row of the ledger of peers an acceptor refused for their range.
type RefusedPeer struct {
	NodeID  string
	Channel string
	Range   StateRange
	At      time.Time
}

// refusedPeerLogEvery is the shortest gap between two WARN lines about one member id.
const refusedPeerLogEvery = time.Minute

// refusedLedger remembers the last refusal of each member id, bounded by the member count so a
// peer that retries every second is one row and a cluster of three never holds a thousand.
type refusedLedger struct {
	mu      sync.Mutex
	rows    map[string]RefusedPeer
	lastLog map[string]time.Time
}

// note records a refusal and reports whether it should be logged now (once a minute per id).
// members is the committed member count, the bound on the ledger; the oldest row goes first.
func (l *refusedLedger) note(row RefusedPeer, members int) (logNow bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rows == nil {
		l.rows, l.lastLog = map[string]RefusedPeer{}, map[string]time.Time{}
	}
	l.rows[row.NodeID] = row
	for limit := max(members, 1); len(l.rows) > limit; {
		oldest, first := "", true
		for id, existing := range l.rows {
			if first || existing.At.Before(l.rows[oldest].At) {
				oldest, first = id, false
			}
		}
		delete(l.rows, oldest)
		delete(l.lastLog, oldest)
	}
	if last, ok := l.lastLog[row.NodeID]; ok && row.At.Sub(last) < refusedPeerLogEvery {
		return false
	}
	l.lastLog[row.NodeID] = row.At
	return true
}

// forget drops a member's row once it connects within range, so `status` stops listing a node that
// was fixed.
func (l *refusedLedger) forget(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.rows, id)
	delete(l.lastLog, id)
}

func (l *refusedLedger) snapshot() []RefusedPeer {
	l.mu.Lock()
	defer l.mu.Unlock()
	rows := make([]RefusedPeer, 0, len(l.rows))
	for _, row := range l.rows {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].NodeID < rows[j].NodeID })
	return rows
}

// RefusedPeers lists the peers this node's acceptors refused for a state range that does not
// overlap its own, one row per member id, newest refusal each; `status` prints it.
func (r *Raft) RefusedPeers() []RefusedPeer { return r.refused.snapshot() }

// checkStateRange refuses a peer whose range does not overlap this build's and records it. It
// runs after the pinned key is checked, so the id it keys the ledger by is authenticated.
func (r *Raft) checkStateRange(hello transport.Handshake) error {
	peerRange := RangeOf(hello)
	if r.Range().Overlaps(peerRange) {
		r.refused.forget(hello.NodeID)
		return nil
	}
	channel := transport.Channel(hello.Channel).String()
	r.fsm.mu.RLock()
	members := len(r.fsm.state.Members)
	r.fsm.mu.RUnlock()
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	text := StateRangeRefusal(channel, hello.NodeID, r.Range(), peerRange)
	if r.refused.note(RefusedPeer{NodeID: hello.NodeID, Channel: channel, Range: peerRange, At: now()}, members) {
		r.stream.log.Warn(text)
	}
	return &rangeRefusal{&Error{Code: CodeClusterMismatch, Message: text}}
}

// rangeRefusal marks the refusal checkStateRange already logged (once a minute per member), so the
// Raft acceptor does not log it again for every connection.
type rangeRefusal struct{ refusal *Error }

// Error returns the refusal text.
func (e *rangeRefusal) Error() string { return e.refusal.Message }

// Unwrap lets errors.As still find the *Error a caller matches on.
func (e *rangeRefusal) Unwrap() error { return e.refusal }
