package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// A removed node that runs names each bucket it holds to the bucket's set
// (BUG-0500; bucket-replication spec §20.8.7).
//
// Step 9.3 made a removed node a passive source: the members pull what they
// lack from it. A member learns what it lacks from its own catalog, its own
// placement marks, or a holder that names the bucket to it. At factor 2 or
// more a surviving holder's sweep names it; at factor 1 the removed node held
// the only copy, so no member's catalog lists the bucket and nothing named it,
// and the bucket answered "bucket not found" for good.
//
// So a removed node walks its catalog once — when it adopts its removal, and
// at start while it stays removed — and for each bucket it holds sends every
// member of the bucket's set, owner first, the same "migrate this bucket" ask
// handOffBucket sends. The owner takes the bucket over through its take-over
// worker, which copies the most complete copy in and never copies one
// incarnation over another (R8-A); the other members then pull from the
// owner. The removed node never releases or deletes a copy (§20.8.7 (5)):
// this is handOffBucket without its release half. After the asks it asks the
// owner what its copy is, since "it serves the bucket" is no proof: a bucket
// is done when the owner's opened copy of this node's incarnation covers this
// one, when the cluster dropped this incarnation (R8-C), or when the owner
// serves another incarnation (R8-A: never replaced by this one; named in a
// warning, and the data directory is then not called safe to empty). The rest
// are asked again with the relocation back-off until they are, or the node
// stops. A member that already serves the bucket answers at once, so asking
// again is harmless.
//
// Cost: no per-bucket state at rest and no per-bucket goroutine — one
// goroutine per removed node, holding the list of buckets not yet settled (a
// database and a bucket name each) and, for each, the owner it was named to
// (BUG-0701), both gone when the pass ends. One catalog walk per adopted epoch
// on the removed node, O(buckets it holds), one ask per bucket and set member
// and one copy question per bucket, one at a time; a later membership wakes
// the running pass instead of waiting out its back-off. The notice is
// O(members) per epoch the pass names everything at; what a probe is answered
// from is a count of pending buckets per owner, O(members) (BUG-0702). A
// member pays only for what it answers.

// startRemovedHandOff launches the pass that names every bucket this removed
// node holds to its set, unless one is running: the running one places each
// bucket afresh on every attempt, so it follows a later membership too, and is
// woken to do so now (BUG-0701). The catalog is walked on the goroutine, never
// under the caller's membership locks.
func (c *Cluster) startRemovedHandOff() {
	if !c.removed() {
		return
	}
	if !c.removedHandOffRunning.CompareAndSwap(false, true) {
		c.wakeRemovedPass()
		return
	}
	if !c.startStoreGoroutine() {
		c.removedHandOffRunning.Store(false)
		return
	}
	go func() {
		defer c.storeWG.Done()
		defer c.removedHandOffRunning.Store(false)
		c.nameHeldBucketsToTheirSets(c.ctx)
	}()
}

// nameHeldBucketsToTheirSets asks every bucket this removed node holds of its
// set until each set serves it, or ctx ends. Every copy a removed node holds
// is outside its set, so the buckets it holds are its misplaced buckets.
//
// A bucket counts as named while its owner is the node it was named to
// (namedTo): a later membership change that moves it on before its copy lands
// (a second removal, of that owner, or a join) takes the record of the ask
// with it, so the bucket is named afresh to its new owner (BUG-0701). The
// notice carries the epoch whose owners every pending bucket was named to,
// and goes only from a round that confirmed that at one epoch; a member counts
// it only for that epoch or an earlier one, so after a change it waits for
// the notice of its own epoch. Once nothing is pending the notice is
// namedAllEnded, which covers every epoch.
func (c *Cluster) nameHeldBucketsToTheirSets(ctx context.Context) {
	pending := c.misplacedBuckets()
	notice := newNamedAllNotice(c)
	if len(pending) == 0 {
		c.endRemovedPass(notice)
		notice.sendUntilHeard(ctx)
		return
	}
	// With auto-rebalance disabled nothing moves on its own; the operator is
	// told what to run instead, and the members are told nothing more will be
	// named.
	if !c.s.config.Server.AutoRebalance {
		c.logger.Warn("this removed node holds buckets and auto-rebalance is disabled, so it does not hand them over; "+
			"run 'migrate <database> <bucket>' on each bucket's owner ('nodes' on a member lists the members), and keep this node's data directory until each one reads there",
			"buckets", len(pending), "first_database", pending[0].database, "first_bucket", pending[0].bucket)
		c.endRemovedPass(notice)
		notice.sendUntilHeard(ctx)
		return
	}
	c.logger.Info("this removed node is handing the buckets it holds to their members; it keeps every copy. "+
		"It asks one bucket and member at a time, so on a node with many buckets this takes a while", "buckets", len(pending))
	namedTo := make(map[heldBucket]string, len(pending))
	wake := c.removedPassWake()
	wait := relocationRetryFloor
	superseded := 0
	for {
		c.retellForgotten(namedTo, notice)
		epoch := c.Epoch()
		round := c.nameToSetsPass(ctx, pending)
		pending, superseded = round.pending, superseded+round.superseded
		maps.Copy(namedTo, round.named)
		// Every bucket's owner at one epoch has been named it: the members
		// are told, so a successor that held its settle mark for this node's
		// names can raise it once its take-over has found their copies (owner
		// ruling R7). Nothing is sent from a round that did not confirm it: a
		// member told an earlier epoch's notice keeps waiting for this one.
		switch owners, all := namedToOwners(pending, namedTo, round.owners); {
		case len(pending) == 0:
			c.endRemovedPass(notice)
			notice.send(ctx)
		case all && c.Epoch() == epoch:
			c.raiseRemovedPass(notice, epoch, owners)
			notice.send(ctx)
		}
		if len(pending) == 0 && notice.heardByAll() {
			c.reportHandOffEnd(superseded)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if len(pending) > 0 {
			c.logger.Warn("some buckets this removed node holds are not served by their members yet; asking again",
				"buckets", len(pending), "first_database", pending[0].database, "first_bucket", pending[0].bucket,
				"error", round.lastErr, "retry_in", wait.String())
		}
		// A later membership, or a member that forgot the notice, wakes the
		// pass to ask again at once.
		select {
		case <-ctx.Done():
			return
		case <-wake:
			wait = relocationRetryFloor
		case <-time.After(wait):
			wait = min(2*wait, relocationRetryCeiling)
		}
	}
}

// namedToOwners reports whether every pending bucket was named to its owner
// of this round, and counts them by that owner. O(pending).
func namedToOwners(pending []heldBucket, namedTo, owners map[heldBucket]string) (map[string]int, bool) {
	counted := make(map[string]int)
	for _, held := range pending {
		owner := owners[held]
		if owner == "" || namedTo[held] != owner {
			return nil, false
		}
		counted[owner]++
	}
	return counted, true
}

// removedPassLevel is this removed node's hand-off as a member that probes it
// is told it (BUG-0702).
type removedPassLevel struct {
	// at is the highest epoch whose owners the pass named every bucket it
	// held to, namedAllEnded once nothing is pending, or 0.
	at uint64
	// pendingOwners counts the buckets pending at at by their owner then. A
	// member that owns none of them holds nothing of this node's hand-off
	// that a restart could have lost, so it may be told at at once.
	pendingOwners map[string]int
	// forgot names the members that probed without the notice while they own
	// a pending bucket: the pass names those buckets to them again before it
	// tells them again.
	forgot map[string]bool
	// wake, buffered, wakes the pass from its back-off.
	wake chan struct{}
}

// removedPassWake is the channel that wakes the pass, made on first use.
func (c *Cluster) removedPassWake() chan struct{} {
	c.removedPassMu.Lock()
	defer c.removedPassMu.Unlock()
	if c.removedPass.wake == nil {
		c.removedPass.wake = make(chan struct{}, 1)
	}
	return c.removedPass.wake
}

// wakeRemovedPass wakes the pass if it waits; a wake already pending is
// enough.
func (c *Cluster) wakeRemovedPass() {
	select {
	case c.removedPassWake() <- struct{}{}:
	default:
	}
}

// raiseRemovedPass records that every pending bucket was named to its owner
// at epoch, with owners counting them, and raises the notice to epoch.
func (c *Cluster) raiseRemovedPass(notice *namedAllNotice, epoch uint64, owners map[string]int) {
	c.removedPassMu.Lock()
	if level := &c.removedPass; level.at != namedAllEnded && epoch >= level.at {
		level.at, level.pendingOwners = epoch, owners
	}
	c.removedPassMu.Unlock()
	notice.raise(epoch)
}

// endRemovedPass records that nothing is left to hand over, and raises the
// notice to namedAllEnded.
func (c *Cluster) endRemovedPass(notice *namedAllNotice) {
	c.removedPassMu.Lock()
	c.removedPass.at, c.removedPass.pendingOwners = namedAllEnded, nil
	c.removedPassMu.Unlock()
	notice.raise(namedAllEnded)
}

// retellForgotten names each pending bucket again to a member that forgot the
// notice, and tells it the notice again after that. O(pending) when a member
// forgot, else O(1).
func (c *Cluster) retellForgotten(namedTo map[heldBucket]string, notice *namedAllNotice) {
	c.removedPassMu.Lock()
	forgot := c.removedPass.forgot
	c.removedPass.forgot = nil
	c.removedPassMu.Unlock()
	if len(forgot) == 0 {
		return
	}
	maps.DeleteFunc(namedTo, func(_ heldBucket, owner string) bool { return forgot[owner] })
	for member := range forgot {
		delete(notice.heard, member)
	}
}

// answerRemovedProbe is this removed node's answer to a member's probe: its
// notice, when the member owns no bucket still pending at the notice's epoch,
// so nothing of this hand-off can have been lost in its restart; otherwise,
// if the member holds no notice as high, the member is named its buckets
// again and then told (BUG-0702). A member on a later epoch than this node's
// wakes this node's gossip, so it follows.
func (c *Cluster) answerRemovedProbe(req ReplPushRequest) ReplPushResponse {
	if req.Epoch > c.Epoch() {
		c.wakeGossip()
	}
	c.removedPassMu.Lock()
	defer c.removedPassMu.Unlock()
	level := &c.removedPass
	switch {
	case level.at == 0:
		return ReplPushResponse{Success: true}
	case level.at == namedAllEnded || level.pendingOwners[req.FromNode] == 0:
		return ReplPushResponse{Success: true, NamedAllAt: level.at}
	case req.Handoff.HeardNamedAllAt < level.at:
		if level.forgot == nil {
			level.forgot = make(map[string]bool)
		}
		level.forgot[req.FromNode] = true
		if level.wake != nil {
			select {
			case level.wake <- struct{}{}:
			default:
			}
		}
	}
	return ReplPushResponse{Success: true}
}

// reportHandOffEnd says how the pass ended. The data directory is called safe
// to empty only when no bucket was left with its only copy here: one whose
// owner serves another incarnation keeps its writes on this node alone, and
// was named when it was found (handOffSuperseded).
func (c *Cluster) reportHandOffEnd(superseded int) {
	if superseded == 0 {
		c.logger.Info("every bucket this removed node holds is served by its members; its data directory can be emptied")
		return
	}
	c.logger.Warn(fmt.Sprintf("this removed node handed over every bucket it could, but %d of them were not handed over because their owners serve "+
		"another incarnation of the name (each was named in a warning above). This node's data directory holds the only copy of their writes: "+
		"do not empty it while you need them", superseded), "buckets", superseded)
}

// handOffOutcome is how one bucket's ask ended.
type handOffOutcome int

const (
	// handOffPending: not settled yet; the bucket is asked again.
	handOffPending handOffOutcome = iota
	// handOffServed: the owner serves an opened copy of this node's
	// incarnation that holds everything this copy holds, or this node holds
	// no copy any more.
	handOffServed
	// handOffDropped: the cluster dropped this node's incarnation (R8-C).
	handOffDropped
	// handOffSuperseded: the owner serves another incarnation of the name,
	// which is never replaced by this one (R8-A). This copy is not handed
	// over, and its writes exist only here.
	handOffSuperseded
)

// handOffRound is what one pass over the pending buckets left: the buckets
// still pending with the owner each was asked of, and the buckets whose owner
// took the ask, with that owner.
type handOffRound struct {
	pending    []heldBucket
	owners     map[heldBucket]string
	named      map[heldBucket]string
	superseded int
	lastErr    error
}

// namedAllNotice is a removed node's notice to the members that it has named
// every bucket it holds (ReplHandoff.NamedAllAt), at the epoch at, with the
// members that have acknowledged it at that epoch. O(members).
type namedAllNotice struct {
	c     *Cluster
	at    uint64
	heard map[string]bool
}

func newNamedAllNotice(c *Cluster) *namedAllNotice {
	return &namedAllNotice{c: c, heard: make(map[string]bool)}
}

// raise moves the notice to epoch when that is higher, so every member is
// told again.
func (n *namedAllNotice) raise(epoch uint64) {
	if epoch > n.at {
		n.at = epoch
		clear(n.heard)
	}
}

// send tells every member that has not acknowledged the notice yet, once.
func (n *namedAllNotice) send(ctx context.Context) {
	if n.at == 0 {
		return
	}
	for _, member := range n.c.Members() {
		if member.ID == n.c.self.ID || n.heard[member.ID] {
			continue
		}
		if resp, err := n.c.sendRemovedNotice(ctx, member, &ReplHandoff{NamedAllAt: n.at}); err == nil && resp.Success {
			n.heard[member.ID] = true
		}
	}
}

// heardByAll reports whether every member has acknowledged the notice.
func (n *namedAllNotice) heardByAll() bool {
	if n.at == 0 {
		return false
	}
	for _, member := range n.c.Members() {
		if member.ID != n.c.self.ID && !n.heard[member.ID] {
			return false
		}
	}
	return true
}

// sendUntilHeard sends the notice until every member has acknowledged it, with
// the relocation back-off, or ctx ends.
func (n *namedAllNotice) sendUntilHeard(ctx context.Context) {
	wait := relocationRetryFloor
	for {
		n.send(ctx)
		if n.heardByAll() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, relocationRetryCeiling)
	}
}

// nameToSetsPass asks each pending bucket's set once.
func (c *Cluster) nameToSetsPass(ctx context.Context, pending []heldBucket) handOffRound {
	round := handOffRound{owners: make(map[heldBucket]string), named: make(map[heldBucket]string)}
	for _, held := range pending {
		if err := ctx.Err(); err != nil {
			round.pending = append(round.pending, held)
			round.lastErr = err
			continue
		}
		outcome, owner, named, err := c.nameToSet(ctx, held)
		if named {
			round.named[held] = owner
		}
		switch outcome {
		case handOffPending:
			c.logger.Debug("a removed node's bucket is not served by its members yet", "database", held.database, "bucket", held.bucket, "error", err)
			round.pending = append(round.pending, held)
			round.owners[held] = owner
			round.lastErr = err
		case handOffSuperseded:
			round.superseded++
		}
	}
	return round
}

// nameToSet asks every member of held's set, owner first, to migrate it (the
// others pull from the owner), and then asks the owner what its copy is: an
// answer of "it serves the bucket" is not enough, because the owner may serve
// another incarnation, created after this copy. named reports that the owner
// took the ask (it served the bucket, or refused it with its take-over holding
// the name), or there was nothing to name; owner is the node asked as the
// owner, "" when none was.
func (c *Cluster) nameToSet(ctx context.Context, held heldBucket) (outcome handOffOutcome, owner string, named bool, err error) {
	if !c.s.bucketStoreOpen(held.database, held.bucket) {
		return handOffServed, "", true, nil
	}
	ref := ReplBucketRef{Database: held.database, Bucket: held.bucket}
	own, err := c.answerCopy(ref)
	if err != nil {
		return handOffPending, "", false, fmt.Errorf("reading this node's copy of bucket %q in database %q: %w", held.bucket, held.database, err)
	}
	replicas, err := c.Replicas(placementOf(held.database, held.bucket))
	if err != nil {
		return handOffPending, "", false, fmt.Errorf("placing bucket %q in database %q: %w", held.bucket, held.database, err)
	}
	ownerNode := replicas[0]
	owner = ownerNode.ID
	var askErr error
	for index, member := range replicas {
		err := c.askNodeToMigrate(ctx, member, held.database, held.bucket)
		// The owner took the ask: its take-over has the name, and holds its
		// settle mark until it finds the copy. A refusal the owner gave
		// without recording the name is not named: an ask that reached the
		// owner before it adopted the epoch that makes it the owner was
		// refused as a replica's seeding, and the notice that followed let it
		// stop awaiting a bucket whose copy had not landed (BUG-0698).
		var refused *migrateRefusedError
		named = named || index == 0 && (err == nil || errors.As(err, &refused) && refused.handOffTaken)
		// The member dropped the incarnation this node holds. A removed node
		// deletes nothing, and a dropped incarnation is nothing to hand over.
		// The answer names the incarnation the member tried to land, which a
		// non-owner pulls from the owner, so it can be another incarnation
		// than this copy's: that drop says nothing about this copy, and the
		// owner's copy question below settles it (BUG-0512).
		var droppedThere *droppedHereError
		if errors.As(err, &droppedThere) && droppedThere.incarnation == own.Incarnation {
			c.noteDroppedHere(held, member.ID, own.Incarnation)
			return handOffDropped, owner, true, nil
		}
		if err != nil {
			askErr = fmt.Errorf("asking %s to take bucket %q in database %q from this removed node: %w", member.ID, held.bucket, held.database, err)
			break
		}
	}
	ownerCopy, err := c.askOne(ctx, ownerNode, held, c.wcTimeout)
	if err != nil {
		return handOffPending, owner, named, errors.Join(askErr, fmt.Errorf("asking %s what its copy of bucket %q in database %q is: %w", owner, held.bucket, held.database, err))
	}
	switch {
	case slices.Contains(ownerCopy.Dropped, own.Incarnation):
		c.noteDroppedHere(held, owner, own.Incarnation)
		return handOffDropped, owner, true, nil
	case ownerCopy.Holds && ownerCopy.Incarnation != own.Incarnation:
		c.logger.Warn(fmt.Sprintf("bucket %q in database %q was not handed over: its owner %s serves another incarnation of the name, created after "+
			"this node's copy, and a copy is never put over another incarnation. The writes in this node's copy exist only here: keep this node's "+
			"data directory while you need them; emptying it discards them", held.bucket, held.database, owner),
			"database", held.database, "bucket", held.bucket, "owner", owner,
			"incarnation", own.Incarnation, "owner_incarnation", ownerCopy.Incarnation)
		return handOffSuperseded, owner, true, nil
	case askErr != nil:
		return handOffPending, owner, named, askErr
	case ownerCopy.Opened && ownerCopy.Moved == 0 && coversForRelease(ownerCopy, own):
		return handOffServed, owner, true, nil
	default:
		return handOffPending, owner, named, fmt.Errorf("%s answered for bucket %q in database %q but has not taken over everything this node's copy holds yet",
			owner, held.bucket, held.database)
	}
}

// noteDroppedHere logs that a bucket this removed node holds was dropped by
// the cluster, so it is not handed over and nothing more is asked of it.
func (c *Cluster) noteDroppedHere(held heldBucket, member string, incarnation uint64) {
	c.logger.Info("the cluster dropped the incarnation of a bucket this removed node holds; it is not handed over",
		"database", held.database, "bucket", held.bucket, "member", member, "incarnation", incarnation)
}
