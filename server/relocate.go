package server

import (
	"context"
	"fmt"
	"time"
)

// The load-time relocation that goes with keying placement by database.
//
// Placement was keyed by the bare bucket name until placement moved to the
// (database, bucket) key, and is keyed by that key since. A node that held data before the
// fix holds some buckets on the replica set of their bare name, which is not
// the replica set of their placement key. The membership sweep does not move
// them: it runs only when an epoch is adopted, and an upgrade changes no
// membership. So every start looks once for the buckets this node holds but
// is not a replica of, and hands each to its replica set through the sweep's
// own hand-off (handOffBucket). The owner pulls the bucket; when this node was
// the pull's source it drops its copy once the pull verifies
// (handleMigrateDone), and the other replicas then pull from the owner. When it
// was not — at RF 2 or more the owner may already hold the bucket — it
// releases its copy once every member has answered that it serves the bucket.
//
// A node holds only buckets it is a replica of once relocation is done, so on
// a node with nothing to move the pass is one ring lookup per held bucket and
// no RPC.

// relocationRetryFloor and relocationRetryCeiling bound the wait between
// passes over the buckets a pass could not hand off — a member not up yet in
// a cluster restarting for the upgrade is the common reason.
const (
	relocationRetryFloor   = time.Second
	relocationRetryCeiling = 30 * time.Second
)

// heldBucket names one bucket in this node's catalog by database and name.
type heldBucket struct {
	database string
	bucket   string
}

// startRelocation launches relocateMisplaced for this start, or, with
// auto-rebalance disabled, only reports what it would move and how to move it.
func (c *Cluster) startRelocation() {
	if c.removed() {
		return
	}
	if !c.s.config.Server.AutoRebalance {
		if misplaced := c.misplacedBuckets(); len(misplaced) > 0 {
			c.logger.Warn("this node holds buckets outside their replica set and auto-rebalance is disabled; no data was moved "+
				"(run 'migrate <database> <bucket>' on each bucket's owner — 'nodes' lists the members)",
				"buckets", len(misplaced), "first_database", misplaced[0].database, "first_bucket", misplaced[0].bucket)
		}
		return
	}
	if !c.startStoreGoroutine() {
		return
	}
	go func() {
		defer c.storeWG.Done()
		c.relocateMisplaced(c.ctx)
	}()
}

// relocateMisplaced hands every bucket this node holds but is not a replica of
// to its replica set, retrying the ones it could not hand off until it has or
// ctx ends.
func (c *Cluster) relocateMisplaced(ctx context.Context) {
	// A removed node holds every copy outside its set and moves none (step
	// 9.3): the members pull what they lack from it, once its own pass names
	// each bucket to them (startRemovedHandOff).
	if c.removed() {
		return
	}
	pending := c.misplacedBuckets()
	if len(pending) == 0 {
		return
	}
	c.logger.Info("relocating buckets this node holds outside their replica set", "buckets", len(pending))
	wait := relocationRetryFloor
	for {
		pending = c.relocatePass(ctx, pending)
		if len(pending) == 0 {
			c.logger.Info("relocation of misplaced buckets finished")
			return
		}
		c.logger.Warn("some buckets could not be relocated yet; retrying",
			"buckets", len(pending), "retry_in", wait.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, relocationRetryCeiling)
	}
}

// misplacedBuckets returns the buckets this node holds whose replica set,
// by their placement key, does not include this node.
func (c *Cluster) misplacedBuckets() []heldBucket {
	var misplaced []heldBucket
	c.forEachKnownBucket(func(database, bucket string) {
		if c.s.bucketStoreOpen(database, bucket) && !c.stillReplica(placementOf(database, bucket)) {
			misplaced = append(misplaced, heldBucket{database: database, bucket: bucket})
		}
	})
	return misplaced
}

// relocatePass tries each pending bucket once and returns the ones that
// failed.
func (c *Cluster) relocatePass(ctx context.Context, pending []heldBucket) []heldBucket {
	var failed []heldBucket
	for _, held := range pending {
		if ctx.Err() != nil {
			failed = append(failed, held)
			continue
		}
		if err := c.relocateBucket(ctx, held); err != nil {
			c.logger.Warn("could not relocate a bucket to its replica set",
				"database", held.database, "bucket", held.bucket, "error", err)
			failed = append(failed, held)
		}
	}
	return failed
}

// relocateBucket hands held to its replica set and releases this node's copy
// once every member serves it (handOffBucket). The owner pulls from any member
// holding it; the other replicas pull from the owner, so they are asked owner
// first. A bucket this node no longer holds, or has become a replica of since
// the pass began (a membership change), needs nothing.
func (c *Cluster) relocateBucket(ctx context.Context, held heldBucket) error {
	key := placementOf(held.database, held.bucket)
	if !c.s.bucketStoreOpen(held.database, held.bucket) || c.stillReplica(key) {
		return nil
	}
	replicas, err := c.Replicas(key)
	if err != nil {
		return fmt.Errorf("placing bucket %q in database %q: %w", held.bucket, held.database, err)
	}
	return c.handOffBucket(ctx, held.database, held.bucket, replicas)
}
