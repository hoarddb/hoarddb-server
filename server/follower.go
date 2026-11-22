package server

import (
	"context"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
)

// Every other copy follows (replication spec §20.8.6; build doc step 10b.6).
//
// A member of a bucket's set whose copy is marked moved, or left the set and
// came back (outside), or diverged from its owner, asks the ring's owner what
// its copy is and settles by spec §20.8.6's table:
//
//	the owner has not opened             asks again, with back-off
//	another incarnation than this copy   nothing is copied over it (I-H1); the
//	                                     copy keeps its record and is reported
//	this copy outside, owing, or diverged copied afresh from the owner
//	the owner's term is the copy's       settled: the mark is cleared
//	the owner's base is the copy's       accepts the owner's term: nothing is
//	  content (A-10.2: a term with no     copied
//	  entry is transparent)
//	the base is in the copy's term,      waits for the term's origin to finish
//	  above its position                 delivering, up to the write-concern
//	                                     timeout; then copied afresh
//	anything else                        copied afresh from the owner
//
// A copy copied afresh is dropped under its exclusive fence — a release, no
// drop entry — and pulled from the owner as a new replica is seeded.

// followOwners settles this node's copies of buckets it is a member of the
// set of and does not own, all of one owner: the owner is asked about them in
// frames, and woken once at the end when a copy took its term.
func (c *Cluster) followOwners(ctx context.Context, followedBuckets []roundItem, outcomes map[heldBucket]copyOutcome) {
	owner := followedBuckets[0].owner
	var asking []roundItem
	var owns []ReplCopy
	for index, followed := range followedBuckets {
		if ctx.Err() != nil {
			leavePending(outcomes, followedBuckets[index:])
			leavePending(outcomes, asking)
			return
		}
		if hook := takeOverBeforeReadHook.Load(); hook != nil {
			(*hook)(c.self.ID, followed.held.database, followed.held.bucket)
		}
		own, err := c.answerCopy(ReplBucketRef{Database: followed.held.database, Bucket: followed.held.bucket})
		if err != nil {
			c.noteOutcome(outcomes, followed.held, copyPending, err)
			continue
		}
		// A copy with no mark is asked about too: a bucket named by a
		// refusal of the owner's term, or one that landed after the placement
		// pass, has none.
		if !own.Holds {
			outcomes[followed.held] = copySettled
			continue
		}
		asking = append(asking, followed)
		owns = append(owns, own)
	}
	if len(asking) == 0 {
		return
	}
	answers, err := c.askFrames(ctx, owner, refsOf(asking), c.wcTimeout)
	if err != nil {
		for _, followed := range asking {
			c.noteOutcome(outcomes, followed.held, copyPending, fmt.Errorf("asking the owner %s what its copies are: %w", owner.ID, err))
		}
		return
	}
	accepted := false
	for index, followed := range asking {
		outcome, took, err := c.followOwner(ctx, followed, owns[index], answers[index])
		accepted = accepted || took
		c.noteOutcome(outcomes, followed.held, outcome, err)
	}
	if accepted {
		// The owner's stream to this node, stalled on entries of the term these
		// copies had not taken, may deliver now: one question wakes it.
		wakeCtx, cancel := context.WithTimeout(ctx, originWakeWait)
		_, err := c.askCopiesOnce(wakeCtx, owner, nil)
		cancel()
		if err != nil {
			c.logger.Debug("the owner did not answer the question that wakes its stream after an accept; its stream retries on its own",
				"owner", owner.ID, "error", err)
		}
	}
}

// followOwner settles one copy by its owner's answer, and reports whether the
// copy took the owner's term.
func (c *Cluster) followOwner(ctx context.Context, followed roundItem, own, answer ReplCopy) (copyOutcome, bool, error) {
	held, key, owner := followed.held, followed.key, followed.owner
	record, diverged := c.divergenceNaming(held.database, held.bucket)
	if !answer.Holds || !answer.Opened || answer.Moved != 0 {
		return copyPending, false, nil
	}
	if answer.Incarnation != own.Incarnation {
		c.warnTwoIncarnations(key, held, []string{owner.ID, c.self.ID})
		return copyPending, false, fmt.Errorf("%w: this node holds incarnation %d and its owner %s holds incarnation %d",
			errNotYetDecided, own.Incarnation, owner.ID, answer.Incarnation)
	}
	// The backstop (bucket-replication-rev8-spec §11.3): a factor this copy
	// missed, its alter lost with an owner that left, is learned from the
	// owner's answer. A copy it marks outside is asked about again by the next
	// round, which reads the registry at the learned factor and so classifies
	// it afresh: copied afresh if the factor places it, settled as outside its
	// set if not. Followed on here, the switch below would copy a demoted copy
	// afresh, releasing it before any member was shown to hold what it holds.
	// Only an owner that advertises bucket_rf_alter is taken at its word: an
	// older owner's answer carries no factor, which would read as inherit.
	if answer.Factor != own.Factor && c.memberAdvertises(owner.ID, featureBucketRFAlter) {
		marked, err := c.s.learnFactor(held, own.Factor, answer.Factor)
		if err != nil || marked {
			return copyPending, false, err
		}
	}
	// A ceded owner finishes delivering its term before it takes the new one
	// (spec §20.8.3): once it has taken it, nothing it originated ships, and a
	// member its stream had not reached would be copied afresh.
	if own.Opened && own.Term != answer.Term && !c.termDelivered(key, own) && !c.originWaitOver(key) {
		return copyAwaitsStream, false, nil
	}
	ownTerm, ownThrough := copyLineage(own.Term, own.Through, own.BaseTerm, own.BaseThrough)
	if ownTerm == answer.BaseTerm && ownThrough > answer.BaseThrough {
		// The owner took the bucket over without this copy's last entries: they
		// were on nodes the take-over could not reach (spec §20.6, the row for
		// a write at one). They go with the copy.
		c.logger.Warn(fmt.Sprintf("bucket %q in database %q on this node holds %d entries its owner %s took the bucket over without; "+
			"they are given up, and the copy is copied afresh from the owner", held.bucket, held.database, ownThrough-answer.BaseThrough, owner.ID),
			"database", held.database, "bucket", held.bucket, "entries", ownThrough-answer.BaseThrough)
	}
	switch {
	case own.Outside != 0 || own.Owes || (diverged && record.Reason == refusalDiverged):
		outcome, err := c.copyAfresh(ctx, held, key, owner)
		return outcome, false, err
	case own.Term == answer.Term && own.Moved == 0:
		c.endTakeOver(key)
		return copySettled, false, nil
	case own.Term == answer.Term:
		outcome, err := c.finishTakeOver(key, c.settleFollower(held, key))
		return outcome, false, err
	case answer.BaseTerm == ownTerm && answer.BaseThrough == ownThrough:
		outcome, err := c.finishTakeOver(key, c.acceptTerm(held, key, own, answer, owner))
		return outcome, err == nil, err
	case behindTheBase(own, answer) && !c.followerWaitOver(key, own.Through):
		c.wakeOriginOf(ctx, held, own)
		return copyAwaitsStream, false, nil
	}
	outcome, err := c.copyAfresh(ctx, held, key, owner)
	return outcome, false, err
}

// behindTheBase reports whether the owner's base is further into a term this
// copy is in: on the copy's lineage, or on its own term and position. A copy
// that accepted a term and has applied none of its entries is transparent
// when it is compared for equality with a base (A-10.2), but it is still
// behind in that term when the owner's base is further into it, and the
// term's origin is still delivering what it lacks.
func behindTheBase(own, answer ReplCopy) bool {
	ownTerm, ownThrough := copyLineage(own.Term, own.Through, own.BaseTerm, own.BaseThrough)
	return (answer.BaseTerm == ownTerm && answer.BaseThrough > ownThrough) ||
		(answer.BaseTerm == own.Term && answer.BaseThrough > own.Through)
}

// settleFollower clears the marks of a copy that already follows its owner's
// term, under the bucket's exclusive fence, in one synced batch.
func (c *Cluster) settleFollower(held heldBucket, key placementKey) error {
	fence := c.s.acquireFence(key, fenceExclusive)
	defer fence.release()
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(placementMarkKey(key)); err != nil {
		return fmt.Errorf("clearing the marks of bucket %q in database %q: %w", held.bucket, held.database, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("clearing the marks of bucket %q in database %q: %w", held.bucket, held.database, err)
	}
	c.forgetPlacementMarks(key)
	return nil
}

// acceptTerm takes the owner's term on a copy whose content is the term's
// base (spec §20.8.6; rev 7, A-10.3): under the bucket's exclusive fence, one
// synced batch writes the owner's term, through 0, the owner's base, not
// opened, applied_seq = the log's last sequence and no failed sequence (an
// entry of the old term logged and not applied must not be replayed onto the
// new one: trap 8), and deletes the marks. The copy is re-read under the fence:
// an entry applied since the question makes it no longer the base, and it is
// asked again. The caller wakes the owner's stream to this node, stalled on
// the entries of the term this copy had not taken.
func (c *Cluster) acceptTerm(held heldBucket, key placementKey, asked, answer ReplCopy, owner ring.Node) error {
	fence := c.s.acquireFence(key, fenceExclusive)
	changed := false
	last := c.walLastSequence()
	err := c.s.rewriteLineageIf(held.database, held.bucket, func(st *store.BucketState) bool {
		if st.Term != asked.Term || st.Through != asked.Through || st.Incarnation != answer.Incarnation {
			changed = true
			return false
		}
		st.Term, st.Through, st.Opened = answer.Term, 0, false
		st.BaseTerm, st.BaseThrough = answer.BaseTerm, answer.BaseThrough
		st.AppliedSeq, st.FailedSeqs = max(st.AppliedSeq, last), nil
		return true
	})
	fence.release()
	if err != nil {
		return fmt.Errorf("accepting %s's term on bucket %q in database %q: %w", owner.ID, held.bucket, held.database, err)
	}
	if changed {
		return fmt.Errorf("%w: the copy of bucket %q in database %q moved while its owner %s was asked", errNotYetDecided, held.bucket, held.database, owner.ID)
	}
	return nil
}

// copyAfresh replaces this node's copy with one pulled from the bucket's
// owner, as a new replica is seeded: its divergence record, if any, then
// settles under the pull's snapshot position. With auto_rebalance off it is
// left to an operator's migrate (A-10.6).
func (c *Cluster) copyAfresh(ctx context.Context, held heldBucket, key placementKey, owner ring.Node) (copyOutcome, error) {
	if !c.mayCopyWhole(key) {
		status := c.takeOverStatusOf(key)
		status.needsCopy = true
		c.setTakeOverStatus(key, status)
		return copyPending, nil
	}
	// The pull releases this node's copy once it has fetched and checked the
	// whole of the owner's and it fits beside this one (BUG-0664): a pull
	// refused before then — the disk (BUG-0551), the owner, the network, the
	// copy's content — leaves this copy serving as it was.
	plan := pullPlan{database: held.database, bucket: held.bucket, source: owner, mustBePlaced: true,
		replaces: &copyReplacement{why: "to copy it afresh from its owner"}}
	if err := c.pullBucketWith(ctx, plan); err != nil {
		return copyPending, fmt.Errorf("copying bucket %q in database %q afresh from its owner %s: %w", held.bucket, held.database, owner.ID, err)
	}
	c.takeOverCopies.Add(1)
	c.logger.Info("copied a replica afresh from its owner: it could not be shown to hold the owner's term",
		"database", held.database, "bucket", held.bucket, "owner", owner.ID)
	return c.finishTakeOver(key, nil)
}

// termDelivered reports whether every member this node, a ceded owner, still
// ships its term's entries of the bucket to has acknowledged its stream
// through the copy's position: through is this node's own sequence of the
// term's last entry, so each has received every entry of the term addressed
// to it. The members are the ceded replay's own (cededAddressees, spec §6.4
// S2): the current set's members that were in the set before the change, and
// not a member awaiting its copy, which is shipped nothing of the bucket but
// the alter (rule F5).
func (c *Cluster) termDelivered(key placementKey, own ReplCopy) bool {
	var match awaitingMatch
	if database, bucket, named := key.names(); named {
		match = c.awaitingMatchAt(database, bucket, own.Term)
	}
	waitedOn, recorded, err := c.cededAddressees(key, c.Epoch()-1, c.effectiveRF(key), match)
	if err != nil {
		return false
	}
	if !recorded {
		return true
	}
	delivered := true
	for _, member := range waitedOn {
		if c.peerFor(member).ackedThrough(c.self.ID) < own.Through {
			// A stream that failed while the member was away waits out its
			// back-off; the member is waited on now.
			c.wakeSenderOf(member.ID)
			delivered = false
		}
	}
	return delivered
}

// originWakeWait bounds the question a follower asks its term's origin to
// wake the origin's stream to it.
const originWakeWait = time.Second

// wakeOriginOf asks the origin of the copy's term — the bucket's owner at the
// epoch the term was opened — about the bucket, which wakes the origin's
// stream to this node (answerQuestion): a follower that waits for the
// entries the origin is still delivering must not wait out the stream's
// back-off too.
func (c *Cluster) wakeOriginOf(ctx context.Context, held heldBucket, own ReplCopy) {
	opened, recorded := c.ringAt(own.Term)
	if !recorded {
		return
	}
	origin, err := opened.Owner(placementOf(held.database, held.bucket).encoded)
	if err != nil || origin.ID == c.self.ID {
		return
	}
	if _, err := c.askOne(ctx, origin, held, originWakeWait); err != nil {
		c.logger.Debug("the origin of a copy's term did not answer the question that wakes its stream",
			"origin", origin.ID, "database", held.database, "bucket", held.bucket, "error", err)
	}
}
