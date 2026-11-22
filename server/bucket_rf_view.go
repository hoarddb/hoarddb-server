package server

import (
	"slices"
	"sort"

	"github.com/hoarddb/hoarddb-server/ring"
)

// What a bucket's owner knows of its copies, for `show buckets` and
// `describe bucket` (bucket-replication-rev8-spec §12). Everything here is
// read from the owner's memory, the stream states and the awaiting map: no
// request goes to another member, and nothing is kept per bucket.

// replicationView is one bucket's copies as its owner sees them.
type replicationView struct {
	// set is the bucket's replica set, owner first; empty for a node alone.
	set       []ring.Node
	inSync    int
	state     string
	notInSync []string
	pending   []PendingCopy
}

// memberCount is the number of members in this node's view of the cluster,
// this node included.
func (c *Cluster) memberCount() int {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	return len(c.members)
}

// replicationViewOf is the view of database.bucket from this node. A node that
// does not own the bucket says so and holds no counts; the owner's view is
// the number of copies in sync (itself, and each addressee whose stream is
// not stalled), who is not, and who still awaits a copy. An awaiting member
// is addressed nothing but the alter, so it is not counted in sync. The zero
// view, for a ring or a lineage this node cannot read, makes a row print as an
// older server's does.
func (c *Cluster) replicationViewOf(database, bucket string) replicationView {
	key := placementOf(database, bucket)
	if !c.ownsBucket(key) {
		return replicationView{state: StateOwnerElsewhere}
	}
	if c.memberCount() <= 1 {
		return replicationView{inSync: 1, state: StateStandalone}
	}
	set, err := c.Replicas(key)
	if err != nil {
		return replicationView{}
	}
	match, err := c.awaitingMatchOfCopy(database, bucket)
	if err != nil {
		return replicationView{}
	}
	pending := c.pendingCopiesMatching(key, match)

	view := replicationView{set: set, inSync: 1, pending: pending}
	for _, member := range set {
		if member.ID == c.self.ID || slices.ContainsFunc(pending, func(copyPending PendingCopy) bool { return copyPending.Node == member.ID }) {
			continue
		}
		if c.peerFor(member).inSync() {
			view.inSync++
		} else {
			view.notInSync = append(view.notInSync, member.ID)
		}
	}
	switch {
	case len(pending) > 0:
		view.state = StateCopying
	case len(view.notInSync) > 0 || len(set) < c.effectiveRF(key):
		view.state = StateUnderReplicated
	default:
		view.state = StateOK
	}
	return view
}

// pendingCopiesMatching is the awaiting members of the bucket placed by key for
// match, with when each one's alter applied, ordered by member. Nil for a match
// that applies to nothing.
func (c *Cluster) pendingCopiesMatching(key placementKey, match awaitingMatch) []PendingCopy {
	if !match.applies || c.awaitingCount.Load() == 0 {
		return nil
	}
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	var pending []PendingCopy
	for member, copyOf := range c.awaiting[key] {
		if copyOf.term == match.term && copyOf.generation == match.generation {
			pending = append(pending, PendingCopy{Node: member, Since: copyOf.since})
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Node < pending[j].Node })
	return pending
}

// replicationViewOf is the view of database.bucket from this node: the
// standalone one when the server is not in a cluster.
func (s *Server) replicationViewOf(database, bucket string) replicationView {
	if s.cluster == nil {
		return replicationView{inSync: 1, state: StateStandalone}
	}
	return s.cluster.replicationViewOf(database, bucket)
}
