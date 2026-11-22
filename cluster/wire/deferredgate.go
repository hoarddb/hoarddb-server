package wire

import (
	"errors"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// pairReplayingError is what a node answers a peer about a pair whose boot replay has not
// finished (PairOwnership.isDeferred): an error, as a node that is still opening answers, and
// never a "not ready" state. A copy that says "not ready" is accounted for and left out by
// Handoff.Target, which would let a new owner open below writes this node acknowledged and
// still holds in its log; a copy that answers an error is a copy that has not answered, and
// the handoff, the catch-up or the seed waits for it (deferred-pair gate spec, section 2).
//
// The peer channel carries the text of a refusal and not its code, so the askers must keep
// reading it as "no answer": the text holds neither the EPOCH_FENCED nor the
// LOG_BELOW_HORIZON marker that catchUpFenced and isBelowHorizon look for.
func pairReplayingError(node string, slot uint16, db uint64) error {
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: cluster.CodeUnavailable.Text(cluster.CondPairReplaying, map[string]string{
		"slot": strconv.Itoa(int(slot)), "db": strconv.FormatUint(db, 10), "node": node,
	})}
}

// pairReplayingMarker is the part of the CondPairReplaying text that no other refusal says.
const pairReplayingMarker = "is replaying its log after a restart"

// isPairReplaying reports the gate's refusal (pairReplayingError). A node in this process
// answers with the coded error; one across the peer channel answers with a wire error that
// carries the text and not the code, as isBelowHorizon reads LOG_BELOW_HORIZON.
func isPairReplaying(err error) bool {
	var coded *cluster.Error
	if errors.As(err, &coded) && coded.Code != cluster.CodeUnavailable {
		return false
	}
	return strings.Contains(err.Error(), pairReplayingMarker)
}

// pairDeferred reports whether the pair's boot replay has not finished. The Handoff, the
// Replicator and the SeedSource are built before n.own, so they are handed this method and
// not the set; none of them asks before the node is wired. One lookup in a map that holds
// only the deferred pairs: nothing per bucket, per pair or per store.
func (n *Node) pairDeferred(slot uint16, db uint64) bool {
	if n.cl != nil && n.cl.opts.UngatedDeferredPairsTestOnly {
		return false
	}
	return n.own.isDeferred(slot, db)
}

// BootReplayPending reports whether the pair's boot replay has not finished on this node,
// whatever the gate does with that; public because the harness lives in another package.
func (n *Node) BootReplayPending(slot uint16, db uint64) bool {
	return n.own.isDeferred(slot, db)
}

// StoreOpenReport is what the open of this node's store found and did, as of that open: a first
// start, or the vouched cut with the commits it discarded. Public because the
// harness lives in another package; it reads the report once and adds nothing at rest.
func (n *Node) StoreOpenReport() storage.VouchedOpenReport {
	return n.kv.VouchedOpenReport()
}

// HandoffTarget is the catch-up target this node's handoff last published for a pair it takes
// over, false when it has published none; public because the harness lives in another package
// and judges by it whether a new owner opened below what an old copy acknowledged.
func (n *Node) HandoffTarget(slot uint16, db uint64) (cluster.Position, bool) {
	return n.handoff.CachedTarget(slot, db)
}

// HoldBootReplaysForTest makes every pass of every node's background boot replay in this
// process call gate first (nil removes it), so a test can keep pairs deferred for as long as
// it needs them deferred. It is the BUG-0760 tests' seam (bootReplayPass), public because the
// harness lives in another package. Set it and clear it only while no node that has pairs
// waiting is running: the passes read it without a lock.
func HoldBootReplaysForTest(gate func()) {
	bootReplayPass = gate
}
