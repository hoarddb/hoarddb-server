package wire

import "github.com/hoarddb/hoarddb-server/cluster"

// Diverged returns a channel closed once, when this node records its first divergence from the
// cluster (the entry-ring spec's FAIL-STOP): the server selects on it and exits.
func (n *Node) Diverged() <-chan struct{} { return n.raft.Diverged() }

// Divergence is the record of the first divergence, and false when there is none.
func (n *Node) Divergence() (cluster.Divergence, bool) { return n.raft.Divergence() }

// LogApplied reports whether the node's own log has been applied to its state: until then a
// reader sees a state that is still being rebuilt. It is false on a diverged node.
func (n *Node) LogApplied() bool { return n.raft.LogApplied() }

// StateLoaded reports whether the node's state is loaded and it is not diverged: LogApplied, or
// the fallback the seed driver uses (cluster.LoadLatch). A server gates its clients on it.
func (n *Node) StateLoaded() bool { return n.raft.StateLoaded() }

// IsDiverged reports whether the node has recorded a divergence.
func (n *Node) IsDiverged() bool { return n.raft.IsDiverged() }

// BootTimeout is how long the node waits on one boot-time step (a proposal, a pull); the server
// bounds its wait for the node's log to be applied by the same figure.
const BootTimeout = bootTimeout
