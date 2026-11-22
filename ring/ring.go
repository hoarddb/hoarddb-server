// Package ring provides consistent-hash placement for hoardDB cluster nodes.
//
// A Ring maps a key to the node that owns it, and to the ordered list of nodes
// that hold replicas of it. It hashes whatever key it is given; the server
// places a bucket by its (database, bucket) placement key, not by its bare
// name (server/placement.go). hoardDB's replication model
// is "per-bucket owner plus the next RF-1 nodes on the ring hold full copies",
// so the two operations that matter are Owner and Replicas.
//
// The implementation wraps github.com/buraksezer/consistent (MIT), which
// maintains the hash circle and the virtual-node set. This package owns the
// node identity -> address mapping, the ordering guarantee (owner first), and
// the rebuild-on-membership-change policy.
package ring

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/buraksezer/consistent"
)

// ErrNoNodes is returned when a placement is requested on an empty ring.
var ErrNoNodes = errors.New("ring has no nodes")

// Default virtual-node and partition counts. These control how evenly keys are
// spread, not how many replicas exist: Config.ReplicationFactor in the
// underlying library is the number of virtual nodes per physical node.
//
// PartitionCount is prime to spread keys uniformly, and virtualNodes is
// deliberately high so that removing one node redistributes only its share.
const (
	PartitionCount = 271
	virtualNodes   = 20
	loadFactor     = 1.25
)

// Node is a single cluster member: a stable identity plus a dial address.
//
// String returns the ID because the underlying library keys its circle by
// Member.String(); identity must be stable across restarts, so ID is never
// derived from the address at lookup time.
type Node struct {
	ID   string
	Addr string
}

// String implements consistent.Member.
func (n Node) String() string { return n.ID }

// Ring is a thread-safe consistent hash ring.
//
// Membership changes rebuild the underlying circle rather than mutating it in
// place. Add/Remove are rare (cluster bootstrap and failover) while Owner and
// Replicas sit on the hot path of every write, so reads take only an RLock and
// never wait on a rebuild in progress.
type Ring struct {
	mu     sync.RWMutex
	nodes  map[string]Node
	hashes *consistent.Consistent
	rf     int
}

// New returns an empty ring whose Replicas default to rf copies.
// An rf below 1 is treated as 1.
func New(rf int) *Ring {
	if rf < 1 {
		rf = 1
	}
	r := &Ring{
		nodes: make(map[string]Node),
		rf:    rf,
	}
	r.rebuildLocked()
	return r
}

// AddNode adds or replaces a node. Re-adding the same ID with a new address
// updates the address without disturbing placement.
func (r *Ring) AddNode(n Node) error {
	if n.ID == "" {
		return fmt.Errorf("node ID must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[n.ID] = n
	r.rebuildLocked()
	return nil
}

// RemoveNode removes a node by ID. Removing an unknown ID is a no-op.
// With the ring empty, Owner and Replicas return ErrNoNodes.
func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; !ok {
		return
	}
	delete(r.nodes, id)
	r.rebuildLocked()
}

// Has reports whether a node ID is a member of the ring.
func (r *Ring) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.nodes[id]
	return ok
}

// Only returns the ring's member when it has exactly one, which owns every
// key: Owner's answer for any key, without hashing it, and without
// allocating.
func (r *Ring) Only() (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) != 1 {
		return Node{}, false
	}
	var only Node
	for _, member := range r.nodes {
		only = member
	}
	return only, true
}

// Len returns the number of physical nodes on the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Nodes returns the members in deterministic (ID) order.
func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodesLocked()
}

// nodesLocked returns members sorted by ID. Callers must hold at least RLock.
func (r *Ring) nodesLocked() []Node {
	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Owner returns the node responsible for key.
func (r *Ring) Owner(key string) (Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) == 0 {
		return Node{}, ErrNoNodes
	}
	member := r.hashes.LocateKey([]byte(key))
	if member == nil {
		return Node{}, fmt.Errorf("ring lookup %q: no owner", key)
	}
	return member.(Node), nil
}

// OwnerID returns the ID of the node responsible for key.
func (r *Ring) OwnerID(key string) (string, error) {
	n, err := r.Owner(key)
	if err != nil {
		return "", err
	}
	return n.ID, nil
}

// Replicas returns the nodes that hold copies of key: the owner first, then the
// next nodes clockwise around the ring, up to n distinct nodes.
//
// n <= 0 means the ring's default replication factor. The count is clamped to
// the number of members, so a 3-node ring asked for 5 replicas returns 3 — the
// caller decides whether a short replica set is acceptable (see IsReplicaSetComplete).
func (r *Ring) Replicas(key string, n int) ([]Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) == 0 {
		return nil, ErrNoNodes
	}
	if n <= 0 {
		n = r.rf
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	members, err := r.hashes.GetClosestN([]byte(key), n)
	if err != nil {
		return nil, fmt.Errorf("ring replica lookup %q: %w", key, err)
	}
	// Defensive dedupe: the library walks the circle with a modular index, so a
	// wrap-around can repeat a member. Order is preserved and the owner stays
	// first, which callers rely on.
	seen := make(map[string]struct{}, len(members))
	out := make([]Node, 0, len(members))
	for _, m := range members {
		node := m.(Node)
		if _, dup := seen[node.ID]; dup {
			continue
		}
		seen[node.ID] = struct{}{}
		out = append(out, node)
	}
	return out, nil
}

// Peers returns the replica-set members for key other than excludeID, which is
// the set a coordinating node must push to when it is part of the set.
func (r *Ring) Peers(key, excludeID string, n int) ([]Node, error) {
	replicas, err := r.Replicas(key, n)
	if err != nil {
		return nil, err
	}
	out := make([]Node, 0, len(replicas))
	for _, node := range replicas {
		if node.ID == excludeID {
			continue
		}
		out = append(out, node)
	}
	return out, nil
}

// IsReplicaSetComplete reports whether the ring has enough members to satisfy
// the requested replication factor. A cluster configured for RF=3 with two
// nodes is under-replicated, and callers that promise durability must not
// pretend otherwise.
func (r *Ring) IsReplicaSetComplete(rf int) bool {
	if rf <= 0 {
		rf = r.rf
	}
	return r.Len() >= rf
}

// rebuildLocked reconstructs the hash circle from the current membership.
// Callers must hold the write lock.
func (r *Ring) rebuildLocked() {
	members := make([]consistent.Member, 0, len(r.nodes))
	for _, n := range r.nodesLocked() {
		members = append(members, n)
	}
	r.hashes = consistent.New(members, Config())
}
