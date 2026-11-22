package server

import (
	"strings"
	"sync/atomic"
	"time"
)

// The gauge of buckets with fewer copies in sync than their factor
// (bucket-replication-rev8-spec §12.3). One unlabelled value per node: the
// buckets this node owns whose InSync is below their stored factor, a pending
// copy included. It is recomputed by one pass over the node's catalog after a
// transition (a stream leaving or returning to the in-sync set, an awaiting
// record written or cleared, a membership adoption), coalesced to at most one
// pass per interval, and never on a write: a write pays nothing for it. The
// pass is O(buckets in the catalog) with one ring lookup per bucket and runs
// off every path a client waits on; its time at 10^6 buckets is not
// measured (spec §13).

// underReplicatedPassInterval is the least time between two passes. An atomic,
// so a test shortens it.
var underReplicatedPassInterval = newSweepBound(10 * time.Second)

// underReplicatedNamed is how many buckets the warning names; the count covers
// the rest.
const underReplicatedNamed = 10

// underReplicatedGauge is the gauge's value and what wakes its pass.
type underReplicatedGauge struct {
	value atomic.Int64
	// passes counts the passes made, which a test reads to show a write makes
	// none.
	passes atomic.Int64
	// adopted says a membership was adopted since the last pass, which then
	// warns once if buckets are short of copies.
	adopted atomic.Bool
	wake    chan struct{}
}

func newUnderReplicatedGauge() underReplicatedGauge {
	return underReplicatedGauge{wake: make(chan struct{}, 1)}
}

// noteReplicationTransition asks for a pass. It never blocks and takes no lock,
// so a stream's state change may call it under its own mutex.
func (c *Cluster) noteReplicationTransition() {
	select {
	case c.underReplicated.wake <- struct{}{}:
	default:
	}
}

// noteMembershipAdopted asks for a pass that warns when it finds buckets short
// of copies.
func (c *Cluster) noteMembershipAdopted() {
	c.underReplicated.adopted.Store(true)
	c.noteReplicationTransition()
}

// runUnderReplicatedGauge is the gauge's goroutine: one per node, on storeWG. It
// passes once at start and then once after each transition, at most one per
// interval; transitions that come while it waits are one pass.
func (c *Cluster) runUnderReplicatedGauge() {
	defer c.storeWG.Done()
	for {
		started := time.Now()
		c.recomputeUnderReplicated()
		select {
		case <-c.ctx.Done():
			return
		case <-c.underReplicated.wake:
		}
		if wait := underReplicatedPassInterval.get() - time.Since(started); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		select {
		case <-c.underReplicated.wake:
		default:
		}
	}
}

// recomputeUnderReplicated is one pass: it stores the count, and logs the
// adoption's warning if a membership was adopted since the last pass. It
// returns the count.
func (c *Cluster) recomputeUnderReplicated() int {
	count, names := c.countUnderReplicated()
	c.underReplicated.value.Store(int64(count))
	c.underReplicated.passes.Add(1)
	if c.underReplicated.adopted.Swap(false) && count > 0 {
		c.logger.Warn("this node owns buckets with fewer copies in sync than their replication factor after a membership change: "+
			"a member is down or has not copied them yet. 'show buckets in <database>' says which copies are missing and why, and 'nodes' lists the members",
			"node_id", c.self.ID, "buckets", count, "first", strings.Join(names, ", "))
	}
	return count
}

// countUnderReplicated counts the owned buckets short of copies and names the
// first ten. A node alone, and a removed one, owns nothing it could copy.
func (c *Cluster) countUnderReplicated() (count int, names []string) {
	if c.removed() || c.memberCount() <= 1 {
		return 0, nil
	}
	c.forEachKnownBucket(func(database, bucket string) {
		if !c.s.bucketStoreOpen(database, bucket) {
			return
		}
		if state := c.replicationViewOf(database, bucket).state; state != StateCopying && state != StateUnderReplicated {
			return
		}
		count++
		if len(names) < underReplicatedNamed {
			names = append(names, database+"."+bucket)
		}
	})
	return count, names
}
