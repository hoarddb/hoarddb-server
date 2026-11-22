package server

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// releaseWaitWarnAfter is how long this node's copies outside their sets
	// may wait for their members to hold what they hold before an operator is
	// told (replication spec §20.8.7; rev 7, A-10.8).
	releaseWaitWarnAfter = 5 * time.Minute
	// releaseWaitWarnEvery spaces the warnings that follow.
	releaseWaitWarnEvery = time.Hour
)

// releaseWaitState is one node-wide record of its copies outside their
// replica sets that are kept because some member does not hold what they
// hold yet: when the wait began and when it was last reported. It is O(1), not
// per bucket (rule 11): the count and the members come from each sweep pass.
// mu is a leaf.
type releaseWaitState struct {
	mu       sync.Mutex
	since    time.Time
	warnedAt time.Time
}

// noteReleaseWait records what a sweep pass left: waiting copies outside their
// sets, and the members they wait on. A wait that has lasted past
// releaseWaitWarnAfter is reported, then at most once per
// releaseWaitWarnEvery, with the remedies of spec §20.12.2. A pass that leaves
// none ends the wait.
func (c *Cluster) noteReleaseWait(waiting int, members []string, now time.Time) {
	c.releaseWait.mu.Lock()
	if waiting == 0 {
		c.releaseWait.since, c.releaseWait.warnedAt = time.Time{}, time.Time{}
		c.releaseWait.mu.Unlock()
		return
	}
	if c.releaseWait.since.IsZero() {
		c.releaseWait.since = now
	}
	due := now.Sub(c.releaseWait.since) > releaseWaitWarnAfter &&
		(c.releaseWait.warnedAt.IsZero() || now.Sub(c.releaseWait.warnedAt) >= releaseWaitWarnEvery)
	if due {
		c.releaseWait.warnedAt = now
	}
	waited := now.Sub(c.releaseWait.since).Round(time.Minute)
	c.releaseWait.mu.Unlock()
	if !due {
		return
	}
	named := strings.Join(members, ", ")
	c.logger.Warn(fmt.Sprintf("%d bucket copies on this node are outside their replica sets and have been kept for %s, "+
		"waiting for %s to hold what they hold: they are released once every member of each set does. "+
		"Bring %s back if it is down, or remove it with 'remove-node <node>' if it is not coming back: writes that only it holds are then lost",
		waiting, waited, named, named),
		"copies", waiting, "waiting_on", members, "waited", waited)
}
