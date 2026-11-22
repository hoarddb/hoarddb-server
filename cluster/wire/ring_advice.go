package wire

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// What an operator is told to do with a ring directory the node refused. The store's refusals
// name the damage only (storage.IsVouchedOpenRefusal); the node adds this, because what is safe
// depends on what the node is, and only the node knows (store-vouched-cut review R-1). Neither
// text sends the node to be seeded by peers: a cluster member started on an empty directory keeps
// its key and its id, so its peers admit it and the leader counts it as a voter at its old match
// index while it has forgotten its term, its vote and its log, which can lose committed cluster
// state (BUG-0870 design note §2); a single node has no peers.
//
// The decision is NodeConfig.Cluster, which the server sets exactly when cluster.enabled is set
// (server/ring_node.go: ringNodeSettings), so a cluster of one with cluster.enabled is told the
// member text, which errs on the safe side. It is the encryption branch's rule.

// ClusterMemberRingAdvice is the member clause (owner ruling 2026-11-19, review R2 F-2: no restore
// from a backup, since a voter put back from an older copy has forgotten the tail of its Raft log
// and the leader never lowers the index it matched). It is the one copy: the server's encryption
// refusals (server/ring_keys.go, server/encryption_status.go, server/nodestore.go) give it too.
func ClusterMemberRingAdvice(ringDir string) string {
	return fmt.Sprintf("This node is configured as a member of a cluster (cluster.enabled), so do not move %[1]s aside "+
		"while the cluster runs: that can lose committed cluster state. Stop this node, keep %[1]s, start it again on an empty ring directory "+
		"with the same node id, keys and address, and run 'replace-node <node> --with <node>@<addr>' on any member", ringDir)
}

// singleNodeKeepAdvice is a single node's advice when a part of its ring directory is missing and
// is to be put back, and what the storage refusals about its directories end in: nothing else
// holds the data, so the directory is kept.
func singleNodeKeepAdvice(ringDir string) string {
	return fmt.Sprintf("This node is not a member of a cluster, so no other node holds this data: keep %s", ringDir)
}

// singleNodeEmptyRingAdvice is a single node's advice when its ring directory is not in the format
// this release reads (RingFormatRefusal): no other build was released, so nothing reads the
// directory, and a single node has no peer that counts on its log, so it starts on an empty one.
func singleNodeEmptyRingAdvice(ringDir string) string {
	return fmt.Sprintf("This node is not a member of a cluster, so it can start with an empty ring directory: move %s "+
		"aside (keep it if you want it) and start the node again; it starts with no data", ringDir)
}

// singleNodeRebuildAdvice is a single node's advice when its store is the problem. Nothing seeds or
// truncates a single node in this build, so its log holds every write and the store is rebuilt from
// it by the boot replay (refuseEmptyStoreAfterEarlierLife; owner ruling 2026-11-19, review R2 F-3).
func singleNodeRebuildAdvice(ringDir string) string {
	return fmt.Sprintf("This node is not a member of a cluster, so it can rebuild its store from its own log: move %s "+
		"aside (keep it) and start the node; it rebuilds the store from %s when the log holds every write from "+
		"sequence 1, and refuses to start, saying so, when it does not",
		filepath.Join(ringDir, "badger"), filepath.Join(ringDir, "wal"))
}

// ringDirAdvice is the advice for the node's kind that the storage refusals about its store's and
// its log's directories end in.
func ringDirAdvice(ringDir string, member bool) string {
	if member {
		return ClusterMemberRingAdvice(ringDir)
	}
	return singleNodeKeepAdvice(ringDir)
}

// singleNodeUnreadAdvice is a single node's advice when this release cannot read a record of its
// log or its sync marker's format. The store may otherwise be whole and the directory is the only
// copy, so the text says to keep it, names a dump or backup as the way back to the data, and asks
// for the evidence the refusal carries: report is "the sequence number above" for a log record and
// "the text above" for the marker, whose refusal has no sequence (review F1 of 2026-11-21).
func singleNodeUnreadAdvice(ringDir, report string) string {
	return fmt.Sprintf("This node is not a member of a cluster, so no other node holds this data and %[1]s is the only "+
		"copy: do not delete it. If you have a dump or backup, move %[1]s aside (keep it), start the node again (it "+
		"starts with no data), and restore the dump into it. If you have none, the same steps start the node with no "+
		"data and leave this directory unread; keep it and report %[2]s", ringDir, report)
}

// unreadRingAdvice is the advice for the node's kind when this release cannot read its log or its
// sync marker's format: rebuilding the store would not help, and no other build reads the
// directory (owner ruling 2026-11-21). A member is never told to start empty or to move it aside.
func unreadRingAdvice(ringDir string, member bool, report string) string {
	if member {
		return ClusterMemberRingAdvice(ringDir)
	}
	return singleNodeUnreadAdvice(ringDir, report)
}

// storeAdvice is the advice for the node's kind when its store is the problem.
func storeAdvice(ringDir string, member bool) string {
	if member {
		return ClusterMemberRingAdvice(ringDir)
	}
	return singleNodeRebuildAdvice(ringDir)
}

// RingStoreRefusal is a refusal of the ring node's store with the advice for this node added: the
// store's own text names the damage, then the advice follows. Unwrap gives the store's refusal, so
// errors.As still finds its type.
type RingStoreRefusal struct {
	Err    error  // the store's refusal
	Advice string // the advice for this node
}

// Error is the store's text, then the advice.
func (r *RingStoreRefusal) Error() string { return r.Err.Error() + ". " + r.Advice }

// Unwrap returns the store's refusal.
func (r *RingStoreRefusal) Unwrap() error { return r.Err }

// EmptyStoreAfterEarlierLifeError refuses a store first start (no data file) on a node whose ring
// directory shows it has run before. On a cluster member the signals are a Raft that holds state or
// a log that holds records: the store was moved aside or deleted, and starting it empty would serve
// as empty every pair this node holds by a seed, since seeded data never passes through the log. A
// single node is never seeded and rebuilds its store from its log; it is refused only when its log
// does not begin at sequence 1 (LogFirst), since the writes before it are then in neither, or when
// its Raft state, which holds the declarations the log names, is gone (NoRaft). Nothing is written
// to the store.
type EmptyStoreAfterEarlierLifeError struct {
	Store     string // the store directory, <ring dir>/badger
	RaftState bool   // the Raft holds a term, a log entry or a snapshot
	LastLog   uint64 // the log's last sequence; 0 when it holds none
	LogFirst  uint64 // a single node's log's first sequence, above 1; 0 for a member's refusal
	// NoRaft is a single node whose log holds records and whose Raft state is gone: the log names
	// databases whose declarations only the Raft state holds.
	NoRaft bool
	// LogMissing is a single node whose log is absent or empty while its Raft state holds a
	// database: the writes the log held are gone, so the store cannot be rebuilt.
	LogMissing bool
}

// Error names the store, the evidence and the way back.
func (e *EmptyStoreAfterEarlierLifeError) Error() string {
	if e.LogMissing {
		return fmt.Sprintf("the node store %s holds no data file, and this node's log is missing while its Raft state holds a "+
			"database: the store cannot be rebuilt from a log that is not there. Nothing was written to the store. If %s "+
			"or %s was moved aside or deleted, put it back", e.Store, e.Store, filepath.Join(filepath.Dir(e.Store), "wal"))
	}
	if e.NoRaft {
		return fmt.Sprintf("the node store %s holds no data file, and this node's Raft state is gone while its log holds "+
			"records through sequence %d: the log cannot be replayed without the declarations the Raft state holds, so "+
			"the store cannot be rebuilt. Nothing was written to the store. If %s was moved aside or deleted, put it back",
			e.Store, e.LastLog, e.Store)
	}
	if e.LogFirst > 1 {
		return fmt.Sprintf("the node store %s holds no data file, and this node's log begins at sequence %d, not 1: the "+
			"writes before %d are in neither the store nor the log, so starting would lose them. Nothing was written "+
			"to the store. If %s was moved aside or deleted, put it back", e.Store, e.LogFirst, e.LogFirst, e.Store)
	}
	var evidence []string
	if e.RaftState {
		evidence = append(evidence, "its Raft state holds a term, a log entry or a snapshot")
	}
	if e.LastLog > 0 {
		evidence = append(evidence, fmt.Sprintf("its log holds records through sequence %d", e.LastLog))
	}
	return fmt.Sprintf("the node store %s holds no data file, but this node has run before: %s. "+
		"Starting it empty would serve this node's data as empty: data copied in by a seed never passes through the "+
		"log, so nothing would bring it back. Nothing was written to the store. If %s was moved aside or deleted, "+
		"put it back", e.Store, strings.Join(evidence, ", and "), e.Store)
}

// ringAdvice is storeAdvice for this node: the advice for a damaged store, by its ring directory
// and whether it is configured as a cluster member.
func (n *Node) ringAdvice() string { return storeAdvice(n.cfg.Dir, n.cfg.Cluster != nil) }

// ringRefusalWithAdvice adds the advice for this node to a refusal of its ring directory: a
// refusal of the store that names the state (storage.IsVouchedOpenRefusal) or the boot replay's
// refusal of a record (cluster.ReplayRecordError). Any other error is returned as it is.
func ringRefusalWithAdvice(err error, ringDir string, member bool) error {
	var record *cluster.ReplayRecordError
	var otherFormat *storage.VouchedMarkerFormatError
	switch {
	case errors.As(err, &record):
		// The log is the problem: rebuilding the store would not help.
		return &RingStoreRefusal{Err: err, Advice: unreadRingAdvice(ringDir, member, "the sequence number above")}
	case errors.As(err, &otherFormat):
		return &RingStoreRefusal{Err: err, Advice: unreadRingAdvice(ringDir, member, "the text above")}
	case storage.IsVouchedOpenRefusal(err):
		return &RingStoreRefusal{Err: err, Advice: storeAdvice(ringDir, member)}
	}
	return err
}
