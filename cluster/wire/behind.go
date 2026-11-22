package wire

import (
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Timing of the suspicion a client can raise. A client prologue is user input, so a mark
// from one lasts a moment and the next cannot start for a while: a client sending a forged
// epoch silences the node for clientMarkTTL in every clientMarkTTL+clientMarkQuiet, not for
// as long as it keeps asking.
const (
	clientMarkTTL   = 200 * time.Millisecond
	clientMarkQuiet = 2 * time.Second
)

// behindMark is a node's record that it saw proof the cluster moved past its committed
// epoch: a data frame, a client prologue or a lease grant naming a higher one (conventions
// section 7). While it holds, the node serves nothing.
//
// A member's mark ends when the node's own committed epoch reaches it, or when a lease
// grantor that was asked after the mark was set answers with an epoch below it (Confirm):
// that grantor had already committed whatever the member's frame was stamped with, so the
// frame lied. A grant issued before the mark proves nothing and clears nothing. A client's
// mark also expires on its own (see clientMarkTTL).
//
// Cost: a few words per node.
type behindMark struct {
	mu          sync.Mutex
	memberSeen  uint64
	memberAt    time.Duration
	clientSeen  uint64
	clientUntil time.Duration
	clientQuiet time.Duration
}

// MarkMember records that a member's frame or a lease grant named epoch at now.
func (b *behindMark) MarkMember(epoch uint64, now time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if epoch > b.memberSeen {
		b.memberSeen = epoch
	}
	b.memberAt = max(b.memberAt, now)
}

// MarkClient records that a client prologue named epoch at now, unless a client mark ended
// less than clientMarkQuiet ago.
func (b *behindMark) MarkClient(epoch uint64, now time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now < b.clientQuiet {
		return
	}
	b.clientSeen, b.clientUntil, b.clientQuiet = epoch, now+clientMarkTTL, now+clientMarkTTL+clientMarkQuiet
}

// Behind reports the epoch the node is behind, and whether it is, for its committed epoch
// at now.
func (b *behindMark) Behind(committed uint64, now time.Duration) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.memberSeen > committed:
		return b.memberSeen, true
	case b.clientSeen > committed && now < b.clientUntil:
		return b.clientSeen, true
	}
	return 0, false
}

// Confirm clears a mark above epoch that was set before sentAt, the time a lease request
// whose answer named epoch was sent.
func (b *behindMark) Confirm(epoch uint64, sentAt time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.memberSeen > epoch && b.memberAt < sentAt {
		b.memberSeen = 0
	}
	if b.clientSeen > epoch && b.clientUntil-clientMarkTTL < sentAt {
		b.clientSeen = 0
	}
}

// BehindMarker is the part of a lease that can be told the cluster moved on. The owner and
// read paths call it when machine.Check says MarkBehind; a lease that does not hold the
// mark (a one-member node, which has no peer to be behind) does not implement it.
type BehindMarker interface {
	// MarkBehind records that a request or frame named epoch above the committed one;
	// fromClient says it came from a client prologue rather than a member.
	MarkBehind(epoch uint64, fromClient bool)
}

// BehindReporter is the part of a lease that knows why it refuses.
type BehindReporter interface {
	// BehindEpoch is the epoch this node was told it is behind, and whether it is.
	BehindEpoch(committed uint64) (uint64, bool)
}

// markBehind tells the lease of a node that machine.Check refused a request as ahead.
func markBehind(lease ServeChecker, epoch uint64, fromClient bool) {
	if marker, ok := lease.(BehindMarker); ok {
		marker.MarkBehind(epoch, fromClient)
	}
}

// seenEpoch is the epoch a request carried: the frame's for a node frame, the prologue's
// for a client.
func seenEpoch(fromNode bool, frame, client uint64) uint64 {
	if fromNode {
		return frame
	}
	return client
}

// behindRefusal rewords a lease refusal that is really "this node is behind": the lease did
// not lapse, the node learned the cluster moved and is catching up, and the text must send
// the caller to retry (a client to refresh its table, a member to try again), not to chase
// a renewal. It returns the code to use and the epoch the node was told, or the code
// unchanged.
func behindRefusal(lease ServeChecker, code cluster.Code, committed uint64, fromNode bool) (cluster.Code, uint64) {
	if code != cluster.CodeLeaseLapsed {
		return code, 0
	}
	reporter, ok := lease.(BehindReporter)
	if !ok {
		return code, 0
	}
	epoch, behind := reporter.BehindEpoch(committed)
	switch {
	case !behind:
		return code, 0
	case fromNode:
		return cluster.CodeEpochFenced, epoch
	}
	return cluster.CodeStaleEpoch, epoch
}
