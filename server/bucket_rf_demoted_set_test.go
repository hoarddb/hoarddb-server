package server

import (
	"fmt"
	"testing"
)

// The demoted set's membership (bucket-replication-rev8-spec §8.3; the G4 to
// G7 build doc step 4, SA-6). Kept apart from the decrease's other tests,
// which run on the tree before the set existed.

// demotedListed reports whether c's demoted set lists the bucket.
func demotedListed(c *Cluster, bucket string) bool {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	_, listed := c.demoted[heldBucket{database: testDatabase, bucket: bucket}]
	return listed
}

// TestADemotedCopyPlacedAgainLeavesTheDemotedSet is the build doc's SA-6
// (spec §8.3): the decrease's apply puts the demoted copy in the set, and a
// later increase that places the node again takes it out, so the set never
// keeps an entry its release would pass over for ever. Auto-rebalance is off,
// so nothing releases the copy between the two alters. A guard: the set is
// new in this step.
func TestADemotedCopyPlacedAgainLeavesTheDemotedSet(t *testing.T) {
	ds := newDecreaseSet(t, 3, autoRebalanceOff)
	defer ds.tc.stop()
	demoted := clusterOf(ds.tc, ds.walk[2])
	lowerUntilAccepted(t, ds, 0, 2)
	pollUntil(t, fmt.Sprintf("node %d lists its copy of %q as demoted", ds.walk[2]+1, ds.bucket), func() bool {
		return demotedListed(demoted, ds.bucket)
	})
	for _, kept := range ds.walk[:2] {
		if demotedListed(clusterOf(ds.tc, kept), ds.bucket) {
			t.Errorf("node %d, which the decrease keeps, lists its copy of %q as demoted", kept+1, ds.bucket)
		}
	}

	alterAccepted(t, ds.alterTo(t, 0, 3))
	pollUntil(t, fmt.Sprintf("node %d takes its copy of %q out of the demoted set once an increase places it again", ds.walk[2]+1, ds.bucket), func() bool {
		return !demotedListed(demoted, ds.bucket)
	})
}
