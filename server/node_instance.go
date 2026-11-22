package server

import (
	"crypto/rand"
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// A node id is the only identity a member has, and a copy of a node's data
// directory carries it: membership.json, the TLS key and the cluster token
// all come along. Two processes started from one data directory therefore look
// alike in every field a peer could compare (BUG-0600). What tells them apart
// is that each process draws its own random instance nonce at start and sends
// it on every gossip exchange. A restart changes the nonce once and the old one
// never comes back, because the old process is gone; two live processes under
// one id keep showing a peer both nonces, interleaved.

// featureInstanceNonce is advertised by a build that puts its instance nonce on
// gossip (GossipRequest.Instance, GossipResponse.Instance), so a peer can tell
// an older build's omission from a build that sent none.
const featureInstanceNonce = "instance_nonce"

// instanceNonceBytes is the size of an instance nonce: 128 random bits, so two
// processes never draw the same one.
const instanceNonceBytes = 16

// maxInstanceSightings bounds the nonces remembered per node id. Two are
// enough to see two processes interleave; the spares cover a third process and
// a restart of one of the two while both run.
const maxInstanceSightings = 4

const (
	// duplicateMinSpan is how long the interleaving of two nonces must last
	// before it counts as two processes. A restart can deliver one late
	// message from the old process after the new one's first, both within
	// milliseconds; two live processes keep interleaving every gossip round.
	duplicateMinSpan = peerRetryInterval
	// duplicateLiveWindow is how long a detection stays live without new
	// evidence: three full gossip rounds (verifyPeer waits 10 retry intervals
	// between exchanges with a peer that answers). After one of the two
	// processes stops, the mark clears this long after its last message.
	duplicateLiveWindow = 30 * peerRetryInterval
)

// duplicateNodeMark is the mark on a node's row of 'nodes' while two
// processes run under its id.
const duplicateNodeMark = "DUPLICATE IDENTITY - two processes are running as this node id; 'status' says what to do"

// duplicateNodeIDDetected counts detections of two live processes under one
// node id, here or at a peer: one per detection, never per bucket or per peer
// series.
var duplicateNodeIDDetected = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "hoarddb_duplicate_node_id_detected_total",
	Help: "Times this node saw two live processes running under one node id (a copied data directory); the WARN line names the id and what to do.",
})

func init() {
	prometheus.MustRegister(duplicateNodeIDDetected)
}

// newInstanceNonce draws this process's instance nonce.
func newInstanceNonce() ([]byte, error) {
	nonce := make([]byte, instanceNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("cluster: drawing this process's instance nonce: %w", err)
	}
	return nonce, nil
}

// instanceSighting is one nonce seen under a node id.
type instanceSighting struct {
	nonce string
	// seenAt says where the nonce came from, for the operator: the address a
	// dial answered at, or the host a gossip request came from.
	seenAt              string
	firstSeen, lastSeen time.Time
	// interleaves counts the times this nonce came back after a nonce first
	// seen later than it had been seen since; firstInterleave is the first.
	interleaves     int
	firstInterleave time.Time
}

// duplicateIdentity is a detection: two nonces under one id, and when the last
// evidence of it arrived.
type duplicateIdentity struct {
	nodeID       string
	older, newer instanceSighting
	lastEvidence time.Time
}

// nodeInstances is what this node remembers of one peer id's nonces.
type nodeInstances struct {
	sightings []*instanceSighting
	last      string
	duplicate *duplicateIdentity
}

// instanceTracker holds the nonces this node has seen per node id, the
// detections they led to, and the members that told this node that two
// processes run under its own id. Its zero value is ready to use; NewCluster
// sets the node's own id and nonce. Per node it holds O(members) entries of at
// most maxInstanceSightings each, and nothing per bucket.
type instanceTracker struct {
	mu     sync.Mutex
	selfID string
	own    string
	byID   map[string]*nodeInstances
	// selfDuplicate is another process seen under this node's own id.
	selfDuplicate *duplicateIdentity
	// reportedBy is, per member, when it last answered that it sees two
	// processes under this node's id.
	reportedBy map[string]time.Time
}

// observe records a nonce seen under nodeID and returns the detection when it
// makes one live that was not, so the caller logs and counts it once.
func (t *instanceTracker) observe(nodeID string, nonce []byte, seenAt string, at time.Time) *duplicateIdentity {
	if nodeID == "" || len(nonce) != instanceNonceBytes {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if nodeID == t.selfID {
		return t.observeSelf(string(nonce), seenAt, at)
	}
	if t.byID == nil {
		t.byID = make(map[string]*nodeInstances)
	}
	record, known := t.byID[nodeID]
	if !known {
		t.forgetSilent(at)
		record = &nodeInstances{}
		t.byID[nodeID] = record
	}
	older, newer, interleaved := record.see(string(nonce), seenAt, at)
	if !interleaved {
		return nil
	}
	fresh := record.duplicate == nil || !duplicateLive(record.duplicate, at)
	if fresh {
		record.duplicate = &duplicateIdentity{nodeID: nodeID, older: *older, newer: *newer}
	}
	record.duplicate.lastEvidence = at
	if !fresh {
		return nil
	}
	detected := *record.duplicate
	return &detected
}

// observeSelf handles a nonce under this node's own id. Its own nonce is this
// process gossiping with itself (a node listed at its own address does at
// start). Any other is a live process running as this node: this process's
// previous incarnation stopped before it started, so no message of its can
// arrive now.
func (t *instanceTracker) observeSelf(nonce, seenAt string, at time.Time) *duplicateIdentity {
	if nonce == t.own {
		return nil
	}
	fresh := t.selfDuplicate == nil || !duplicateLive(t.selfDuplicate, at)
	if fresh {
		t.selfDuplicate = &duplicateIdentity{
			nodeID: t.selfID,
			older:  instanceSighting{nonce: t.own, seenAt: "in this process"},
			newer:  instanceSighting{nonce: nonce, seenAt: seenAt, firstSeen: at},
		}
	}
	t.selfDuplicate.newer.lastSeen = at
	t.selfDuplicate.lastEvidence = at
	if !fresh {
		return nil
	}
	detected := *t.selfDuplicate
	return &detected
}

// forgetSilent drops the ids nothing has been heard from for ten live
// windows: members removed since, or ids a node ran under once. It runs only
// when a new id appears, and is O(ids).
func (t *instanceTracker) forgetSilent(at time.Time) {
	for nodeID, record := range t.byID {
		if record.lastHeard().Before(at.Add(-10 * duplicateLiveWindow)) {
			delete(t.byID, nodeID)
		}
	}
}

// see records one sighting. It reports an interleaving that has lasted
// duplicateMinSpan, with the nonce that came back (older) and the one first
// seen after it (newer).
func (r *nodeInstances) see(nonce, seenAt string, at time.Time) (older, newer *instanceSighting, interleaved bool) {
	seen := r.find(nonce)
	if seen == nil {
		r.add(&instanceSighting{nonce: nonce, seenAt: seenAt, firstSeen: at, lastSeen: at})
		r.last = nonce
		return nil, nil, false
	}
	if nonce != r.last {
		for _, other := range r.sightings {
			if other == seen || !other.firstSeen.After(seen.firstSeen) || !other.lastSeen.After(seen.lastSeen) {
				continue
			}
			// A nonce first seen after this one, and seen since this one
			// last was: this one came back. A restarted process never does.
			if seen.interleaves == 0 {
				seen.firstInterleave = at
			}
			seen.interleaves++
			newer = other
			break
		}
	}
	seen.lastSeen, seen.seenAt = at, seenAt
	r.last = nonce
	if newer == nil || at.Sub(seen.firstInterleave) < duplicateMinSpan {
		return nil, nil, false
	}
	return seen, newer, true
}

func (r *nodeInstances) find(nonce string) *instanceSighting {
	for _, sighting := range r.sightings {
		if sighting.nonce == nonce {
			return sighting
		}
	}
	return nil
}

// add keeps at most maxInstanceSightings, dropping the one heard from least
// recently.
func (r *nodeInstances) add(sighting *instanceSighting) {
	if len(r.sightings) == maxInstanceSightings {
		stalest := 0
		for i, candidate := range r.sightings {
			if candidate.lastSeen.Before(r.sightings[stalest].lastSeen) {
				stalest = i
			}
		}
		r.sightings = slices.Delete(r.sightings, stalest, stalest+1)
	}
	r.sightings = append(r.sightings, sighting)
}

func (r *nodeInstances) lastHeard() time.Time {
	var latest time.Time
	for _, sighting := range r.sightings {
		if sighting.lastSeen.After(latest) {
			latest = sighting.lastSeen
		}
	}
	return latest
}

func duplicateLive(d *duplicateIdentity, at time.Time) bool {
	return at.Sub(d.lastEvidence) <= duplicateLiveWindow
}

// noteReport records that member reporter answered that it sees two processes
// under this node's id, and reports whether no member had said so within the
// live window.
func (t *instanceTracker) noteReport(reporter string, at time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	fresh := len(t.liveReporters(at)) == 0
	if t.reportedBy == nil {
		t.reportedBy = make(map[string]time.Time)
	}
	t.reportedBy[reporter] = at
	return fresh
}

// liveReporters is the members whose report is within the live window, sorted.
// The caller holds mu.
func (t *instanceTracker) liveReporters(at time.Time) []string {
	var reporters []string
	for reporter, reportedAt := range t.reportedBy {
		if at.Sub(reportedAt) <= duplicateLiveWindow {
			reporters = append(reporters, reporter)
		}
	}
	sort.Strings(reporters)
	return reporters
}

// liveDuplicate reports whether two processes are running under nodeID as
// this node sees it: seen interleaving here, or, for this node's own id, seen
// directly or reported by a member.
func (t *instanceTracker) liveDuplicate(nodeID string, at time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if nodeID == t.selfID {
		return t.selfNote(at) != ""
	}
	record, known := t.byID[nodeID]
	return known && record.duplicate != nil && duplicateLive(record.duplicate, at)
}

// notes is one sentence per node id this node sees two processes under, this
// node's own id first, then by id.
func (t *instanceTracker) notes(at time.Time) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var notes []string
	if note := t.selfNote(at); note != "" {
		notes = append(notes, note)
	}
	ids := make([]string, 0, len(t.byID))
	for nodeID, record := range t.byID {
		if record.duplicate != nil && duplicateLive(record.duplicate, at) {
			ids = append(ids, nodeID)
		}
	}
	sort.Strings(ids)
	for _, nodeID := range ids {
		notes = append(notes, peerDuplicateNote(t.byID[nodeID].duplicate))
	}
	return notes
}

// selfNote is the sentence about another process under this node's own id, or
// "". The caller holds mu.
func (t *instanceTracker) selfNote(at time.Time) string {
	reporters := t.liveReporters(at)
	direct := t.selfDuplicate != nil && duplicateLive(t.selfDuplicate, at)
	if !direct && len(reporters) == 0 {
		return ""
	}
	var seen string
	switch {
	case direct:
		seen = fmt.Sprintf("this one (instance %x) and another seen %s (instance %x)",
			t.own, t.selfDuplicate.newer.seenAt, t.selfDuplicate.newer.nonce)
	case len(reporters) == 1:
		seen = fmt.Sprintf("member %s sees two instances under this id; this one is instance %x", reporters[0], t.own)
	default:
		seen = fmt.Sprintf("members %s see two instances under this id; this one is instance %x",
			strings.Join(reporters, ", "), t.own)
	}
	return fmt.Sprintf("two processes are running as node %s: %s. Both own %s's buckets, so user changes are refused on this node "+
		"until only one runs. If this process is the copy (a cloned disk or a copied data directory), stop it, wipe its data directory, "+
		"give it its own HOARDDB_NODE_ID and run 'join <node-id>@<address>' on a member for it; otherwise stop the other one the same way",
		t.selfID, seen, t.selfID)
}

// peerDuplicateNote is the sentence about two processes under a peer's id.
func peerDuplicateNote(d *duplicateIdentity) string {
	return fmt.Sprintf("two processes are running as node %s: one seen %s (instance %x), another seen %s (instance %x). "+
		"Both own %s's buckets and both may act as the auth master. Stop the copy (a cloned disk or a copied data directory), "+
		"wipe its data directory, give it its own HOARDDB_NODE_ID and run 'join <node-id>@<address>' on a member for it",
		d.nodeID, d.older.seenAt, d.older.nonce, d.newer.seenAt, d.newer.nonce, d.nodeID)
}

// observeInstance records a peer's nonce from a gossip exchange, and logs and
// counts a detection it makes.
func (c *Cluster) observeInstance(nodeID string, nonce []byte, seenAt string) {
	detected := c.instances.observe(nodeID, nonce, seenAt, time.Now())
	if detected == nil {
		return
	}
	duplicateNodeIDDetected.Inc()
	note := peerDuplicateNote(detected)
	if detected.nodeID == c.self.ID {
		note = c.instances.notes(time.Now())[0]
	}
	c.logger.Warn(note, "node_id", detected.nodeID,
		"seen", []string{detected.older.seenAt, detected.newer.seenAt},
		"instances", []string{fmt.Sprintf("%x", detected.older.nonce), fmt.Sprintf("%x", detected.newer.nonce)})
}

// noteDuplicateReported records a member's answer that it sees two processes
// under this node's id, and logs and counts it the first time in a window.
func (c *Cluster) noteDuplicateReported(reporter string) {
	if !c.instances.noteReport(reporter, time.Now()) {
		return
	}
	duplicateNodeIDDetected.Inc()
	c.logger.Warn(c.instances.notes(time.Now())[0], "node_id", c.self.ID, "reported_by", reporter)
}

// duplicateSelfRefusal refuses a user change while two processes run under
// this node's id: both would accept it as the auth master.
func (c *Cluster) duplicateSelfRefusal() error {
	c.instances.mu.Lock()
	note := c.instances.selfNote(time.Now())
	c.instances.mu.Unlock()
	if note == "" {
		return nil
	}
	return &sentencedError{message: note + ". Nothing was changed.", cause: ErrDuplicateNodeID}
}

// gossipSource names where an inbound gossip request came from: the host of
// the connection, since the sender's own Addr is the listed one either way.
func gossipSource(remote string) string {
	if remote == "" {
		return "in gossip from an unknown address"
	}
	if host, _, err := net.SplitHostPort(remote); err == nil {
		remote = host
	}
	return "in gossip from " + remote
}
