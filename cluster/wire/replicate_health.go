package wire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
)

// The breaker's constants are fixed in code, not configuration (design note section 3):
// the project starts with zero config, and a key is added when a measured case needs one.
const (
	// suspectBase is about one failure-detection time. It is the first hold, and the least time
	// an acknowledgement must trail a mark before it clears it.
	suspectBase = 2 * time.Second
	// suspectMax caps a hold, so a peer that never answers costs one wait per 30 s, not one per write.
	suspectMax = 30 * time.Second
	// suspectForget is the quiet after which a peer's strikes are forgotten and its record dropped.
	suspectForget = 60 * time.Second
	// suspectSendFailures is the run of failed sends to one peer that marks it: one reset
	// connection must not.
	suspectSendFailures = 3
)

// peerHealth is what the replicator knows of one peer's answers. There is one per peer, never
// per pair, bucket or store, and only for a peer that was ever marked or failed a send. The
// peer is suspect while now is before until. The record is guarded by Replicator.mu.
type peerHealth struct {
	until time.Time
	// markedAt is when the current suspect episode began.
	markedAt time.Time
	// lastStrike is when strikes last rose, so a peer that keeps failing escalates once per base hold.
	lastStrike time.Time
	// lastEvent is the last mark, clear or failed send, for the sweep.
	lastEvent time.Time
	// lastMark is the last mark or clear; strikes are forgotten suspectForget after it, so failed
	// sends that never reach the threshold do not keep them alive.
	lastMark time.Time
	// lastApplied is when the peer last gave an applied ack on any pair: a peer that answers on
	// one pair is not gone because another pair is stuck (seeding, a deferred fence, a schema
	// it cannot resolve yet).
	lastApplied time.Time
	strikes     int
	// sendFails counts consecutive failed sends to the peer, over every pair.
	sendFails int
}

func (r *Replicator) suspectLocked(peer string, now time.Time) bool {
	h := r.health[peer]
	return h != nil && now.Before(h.until)
}

// markSuspectLocked records evidence that peer is not answering: a concern wait ended at wc_timeout
// with its acknowledgement missing, or sends to it keep failing. The first evidence of an episode
// wakes the writers already waiting; later evidence only extends the hold.
func (r *Replicator) markSuspectLocked(peer string, now time.Time, cause string) {
	h := r.health[peer]
	if h == nil {
		h = &peerHealth{}
		r.health[peer] = h
	}
	wasSuspect := now.Before(h.until)
	if now.Sub(h.lastMark) >= suspectForget {
		h.strikes = 0
	}
	struck := false
	if h.strikes == 0 || now.Sub(h.lastStrike) >= suspectBase {
		h.strikes++
		h.lastStrike = now
		struck = true
	}
	hold := min(suspectBase<<min(h.strikes-1, 16), suspectMax)
	h.until = now.Add(hold)
	h.lastEvent, h.lastMark = now, now
	switch {
	case !wasSuspect:
		h.markedAt = now
		r.wakeSuspectLocked()
		slog.Info(fmt.Sprintf("replication to %s is suspect: %s. Writes stop waiting for it for %s and answer applied_degraded without it; sends to it continue, and the mark clears when it acks, a probe sees it return or the cluster marks it active", peer, cause, hold),
			"peer", peer, "reason", cause, "hold", hold.String())
	case struck:
		slog.Info(fmt.Sprintf("replication to %s is still suspect: %s. The hold is now %s", peer, cause, hold),
			"peer", peer, "reason", cause, "hold", hold.String())
	}
}

// clearSuspectLocked ends a hold because the peer was seen answering. Strikes stay, so a peer
// that flaps is held longer each time until suspectForget of quiet.
func (r *Replicator) clearSuspectLocked(peer string, now time.Time, reason string) {
	h := r.health[peer]
	if h == nil {
		return
	}
	h.sendFails = 0
	if now.Before(h.until) {
		h.until = time.Time{}
		h.lastEvent, h.lastMark = now, now
		r.wakeSuspectLocked()
		slog.Info(fmt.Sprintf("replication to %s is no longer suspect: %s. Writes wait for it again", peer, reason),
			"peer", peer, "reason", reason)
	}
}

// clearSuspect is clearSuspectLocked for a caller that does not hold the lock: the supervisor,
// when the committed state moves a member from down to active.
func (r *Replicator) clearSuspect(peer, reason string) {
	now := r.cfg.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearSuspectLocked(peer, now, reason)
}

// noteSendFailure counts one failed send to peer (one call that failed, however many pairs it
// carried). A run of suspectSendFailures marks the peer, and each failure after that extends the
// hold, so every write that is still sent to a suspect peer is a probe. A send that failed only
// because the replicator is shutting down says nothing about the peer.
//
// A transport backoff refusal (peer.ErrBackoff) is not an attempt: after one failed dial the
// transport refuses every call for its backoff, so a burst of them reaches any threshold in
// milliseconds for a peer that may be alive. Only real attempts count, and a dead peer's refused
// redials still mark it within about 350 ms.
func (r *Replicator) noteSendFailure(ctx context.Context, peerID string, err error) {
	if ctx.Err() != nil || errors.Is(err, peer.ErrBackoff) {
		return
	}
	now := r.cfg.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.health[peerID]
	if h == nil {
		h = &peerHealth{}
		r.health[peerID] = h
	}
	h.sendFails++
	h.lastEvent = now
	if h.sendFails >= suspectSendFailures {
		r.markSuspectLocked(peerID, now, fmt.Sprintf("%d sends to it failed in a row", suspectSendFailures))
	}
}

// noteSendSuccess ends the run of failed sends to peer: failures must be consecutive.
func (r *Replicator) noteSendSuccess(peer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h := r.health[peer]; h != nil {
		h.sendFails = 0
	}
}

// noteAppliedAckLocked is what an applied ack from peer, for any pair, says about its health: it
// ends the run of failed sends and, when the mark is at least suspectBase old, clears it. An
// earlier ack can be the late answer of the very write that timed out and proves nothing new.
func (r *Replicator) noteAppliedAckLocked(peer string, now time.Time) {
	h := r.health[peer]
	if h == nil {
		h = &peerHealth{}
		r.health[peer] = h
	}
	h.lastApplied, h.lastEvent = now, now
	h.sendFails = 0
	if r.suspectLocked(peer, now) && now.Sub(h.markedAt) >= suspectBase {
		r.clearSuspectLocked(peer, now, "an applied ack from it arrived")
	}
}

// wakeSuspectLocked wakes every Await at once: the one channel is closed and replaced, so the cost
// is O(1) however many pairs are waiting.
func (r *Replicator) wakeSuspectLocked() {
	close(r.suspectChanged)
	r.suspectChanged = make(chan struct{})
}

// sweepHealthLocked drops the records of peers that are not suspect and have been quiet for suspectForget.
func (r *Replicator) sweepHealthLocked(now time.Time) {
	for peer, h := range r.health {
		if !now.Before(h.until) && now.Sub(h.lastEvent) >= suspectForget {
			delete(r.health, peer)
		}
	}
}

// NoteWaitTimeout records that a concern wait of the given length for pos ended at wc_timeout:
// every current copy that had not acked it, had given no applied ack on any pair during the wait,
// and was not already suspect, is marked. A wait cut short by the
// client's own deadline is never reported here, because it says nothing about a peer.
func (r *Replicator) NoteWaitTimeout(slot uint16, db uint64, pos cluster.Position, waited time.Duration) {
	now := r.cfg.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := r.pairs[pairKey{slot, db}]
	for _, peer := range r.cfg.Copies(slot, db) {
		if st := ps.peerLocked(peer); st != nil && st.holds && !st.acked.Less(pos) {
			continue
		}
		if h := r.health[peer]; h != nil && now.Sub(h.lastApplied) < waited {
			continue // it answered on another pair during the wait: this pair is stuck, not the peer
		}
		if !r.suspectLocked(peer, now) {
			r.markSuspectLocked(peer, now, "a concern wait ran the whole wc_timeout with its acknowledgement missing")
		}
	}
}

// peerLocked is the send state of peer, or nil; a nil pair has none.
func (ps *pairSend) peerLocked(peer string) *peerState {
	if ps == nil {
		return nil
	}
	return ps.peers[peer]
}

// waitableLocked is how many current copies have not acked pos and are not suspect: the copies
// a concern wait can still expect an answer from.
func (r *Replicator) waitableLocked(key pairKey, pos cluster.Position, now time.Time) int {
	ps := r.pairs[key]
	waitable := 0
	for _, peer := range r.cfg.Copies(key.slot, key.db) {
		if st := ps.peerLocked(peer); st != nil && st.holds && !st.acked.Less(pos) {
			continue
		}
		if !r.suspectLocked(peer, now) {
			waitable++
		}
	}
	return waitable
}

// copySetChanged wakes every waiting write to re-read the copy set: a committed state may have
// removed a copy it was waiting for (a member committed down is no longer a copy), and nothing
// else would end that wait before wc_timeout.
func (r *Replicator) copySetChanged() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wakeSuspectLocked()
}
