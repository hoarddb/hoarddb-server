package server

import (
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"time"
)

// Recovery of a lost proposer's change (spec §6.4). An acceptance acts on
// nothing, so a change whose proposer crashed or was cut off after some
// members accepted it would half-happen later and by surprise. Every node
// runs one recovery goroutine, idle unless this node holds an acceptance it
// has not adopted; then, some time after the acceptance, it proposes the
// accepted set itself, and the value rule makes it finish whatever was
// chosen.

// The recovery's delays: the first try after an acceptance, its jitter, and
// the cap a failing recovery's doubling backs off to. They are atomic so a test
// can lower one while a recovery goroutine reads it.
var (
	membershipRecoveryAfter  = newSweepBound(20 * time.Second)
	membershipRecoveryJitter = newSweepBound(10 * time.Second)
	membershipRecoveryCap    = newSweepBound(5 * time.Minute)
)

// armRecovery tells the recovery goroutine that this node recorded an
// acceptance now. It never blocks: a pending arm already says so, and the
// goroutine reads the record when its timer fires. A removed node arms
// nothing: it is no acceptor.
func (c *Cluster) armRecovery() {
	if c.removed() {
		return
	}
	select {
	case c.recoveryArm <- time.Now():
	default:
	}
}

// runMembershipRecovery is the recovery goroutine, started by Start and
// stopped by Stop: it waits on the cluster's context, an arm and its timer.
func (c *Cluster) runMembershipRecovery() {
	defer c.storeWG.Done()
	var timer *time.Timer
	var fired <-chan time.Time
	arm := func(from time.Time, delay time.Duration) {
		if timer != nil {
			timer.Stop()
		}
		wait := time.Until(from.Add(delay + recoveryJitter()))
		timer = time.NewTimer(max(wait, 0))
		fired = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	delay := membershipRecoveryAfter.get()
	if c.restoredAcceptance {
		arm(time.Now(), delay)
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case acceptedAt := <-c.recoveryArm:
			delay = membershipRecoveryAfter.get()
			arm(acceptedAt, delay)
		case <-fired:
			fired = nil
			next, rearm := c.recoverAcceptance(delay)
			if rearm {
				delay = next
				arm(time.Now(), delay)
			}
		}
	}
}

// recoveryJitter is a uniform delay below membershipRecoveryJitter, so two
// members that accepted at one moment do not propose at the same moment.
func recoveryJitter() time.Duration {
	jitter := membershipRecoveryJitter.get()
	if jitter <= 0 {
		return 0
	}
	return mathrand.N(jitter)
}

// recoverAcceptance finishes this node's acceptance, if it still holds one it
// has not adopted, and says whether and after what delay to try again: at the
// same delay when another change from this node was running, at double the
// delay (up to the cap) when the recovery did not commit.
func (c *Cluster) recoverAcceptance(delay time.Duration) (time.Duration, bool) {
	proposal, holds := c.recoveryProposal()
	if !holds {
		return 0, false
	}
	change := describeChange(proposal.base, proposal.own)
	_, err := c.changeMembership(c.ctx, proposal)
	if _, stillHolds := c.recoveryProposal(); !stillHolds {
		return 0, false
	}
	if errors.Is(err, ErrEpochInProgress) && !errors.Is(err, errSelfAcceptRefused) {
		return delay, true
	}
	next := min(2*delay, membershipRecoveryCap.get())
	reason := "the change was not finished"
	if err != nil {
		reason = err.Error()
	}
	c.logger.Warn(fmt.Sprintf("could not finish the membership change (%s) this node accepted; it tries again in %s",
		change, next.Round(time.Millisecond)), "epoch", proposal.epoch, "reason", reason)
	return next, true
}

// recoveryProposal is the change a recovery proposes: the set this node
// accepted for the epoch after its own, against its committed set; false when
// it holds no such acceptance, or is removed.
func (c *Cluster) recoveryProposal() (membershipProposal, bool) {
	if c.removed() {
		return membershipProposal{}, false
	}
	c.membershipFileMu.Lock()
	pending := c.pending.clone()
	c.membershipFileMu.Unlock()
	epoch, base := c.epochMembers()
	if pending == nil || pending.Accepted == nil || pending.Epoch != epoch+1 {
		return membershipProposal{}, false
	}
	return membershipProposal{epoch: pending.Epoch, base: base, recovery: true,
		own: proposedSet{members: pending.Members, change: pending.Change}}, true
}
