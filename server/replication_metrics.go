package server

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// What an operator sees of the replication stream (spec §19.3.13; build doc
// step 8.1), of a join (join-freeze spec §6.2) and of a hand-off (spec
// §20.14): six gauges, computed at scrape over this node's current members
// and atomics, three hand-off counters, the membership change counter, and
// the stranded counter (stranded.go). None but the membership change counter
// carries a label, and that one only its outcome: a series per bucket or per
// database is unusable at 10^6 buckets, and a series per peer is not
// needed to answer "is the cluster in sync" — the log lines name the peer
// (rule 11). A scrape costs O(members); nothing here reads a bucket.
//
// The gauges are on a registry of the cluster's own, served beside the
// default one (metricsGatherer), because several nodes run in one test
// process and each must report its own streams.

// newReplicationMetrics builds the cluster's registry: its five stream and
// join gauges, the four hand-off collectors and the membership change
// counter.
func newReplicationMetrics(c *Cluster) (*prometheus.Registry, error) {
	registry := prometheus.NewRegistry()
	gauges := []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_replication_peers_not_in_sync",
			Help: "Members whose replication stream from this node is stalled, or is still replaying the log to them after a stall: they count toward no write concern until the log up to the tail has been sent to them. 0 is healthy.",
		}, func() float64 { return float64(c.streamReadings(time.Now()).notInSync) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_replication_lag_entries",
			Help: "The largest, over members, of this node's synced log tail minus the position that member has confirmed. A member that is streaming with nothing queued and nothing unconfirmed counts 0, so traffic addressed to other members is not lag.",
		}, func() float64 { return float64(c.streamReadings(time.Now()).lagEntries) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_replication_lag_seconds",
			Help: "For the member that is owed entries and has gone longest without confirming any of them: how long, in seconds. 0 when every member has confirmed everything sent to it.",
		}, func() float64 { return c.streamReadings(time.Now()).lagSeconds }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_cluster_members_joining",
			Help: "Members whose join this node has not seen every member settle: schema changes are paused while it is above 0 ('nodes' says what the join waits on). 0 is healthy.",
		}, func() float64 { return float64(len(c.joinFreeze.Load().Joiners)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_cluster_peers_unreachable",
			Help: "Members whose membership gossip from this node is failing. A member down past 30 seconds is also named in this node's log.",
		}, func() float64 { return float64(c.unreachableMembers()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_handoffs_in_progress",
			Help: "Buckets on this node with a hand-off record. After a join or a remove-node it reads 0 on every node once every node's placement pass " +
				"has run and every bucket that moved has been taken over; read every node, because a node with no copy yet reads 0, and every node " +
				"reads 0 until its placement pass after the change has marked something.",
		}, func() float64 { return float64(c.unsettledCount.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hoarddb_buckets_under_replicated",
			Help: "Buckets this node owns with fewer copies in sync than their replication factor, a copy still pending included; 0 is healthy, and a node alone reads 0. " +
				"Recomputed after a stream, a pending copy or a membership changes, at most every 10 seconds, never on a write. A decrease is intentional and does not count.",
		}, func() float64 { return float64(c.underReplicated.value.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "hoarddb_handoff_copies_total",
			Help: "Buckets this node copied whole to take them over or to follow their new owner. Rising after a join or a remove-node is " +
				"expected for buckets that gained a holder with no copy; rising otherwise means copies were behind and were replaced.",
		}, func() float64 { return float64(c.takeOverCopies.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "hoarddb_handoffs_without_previous_owner_total",
			Help: "Buckets this node took over without their previous owner, by asking every member for the most complete copy. " +
				"It rises after the previous owner was removed while down; a write only that owner held is then lost.",
		}, func() float64 { return float64(c.takeOverFallbacks.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "hoarddb_handoff_entries_reconciled_total",
			Help: "Entries this node applied from a node that no longer owns their bucket, while the bucket was changing owner: " +
				"an old owner finishing the delivery of its writes. It stops rising once the hand-off settles.",
		}, func() float64 { return float64(c.entriesReconciled.Load()) }),
	}
	for _, gauge := range gauges {
		if err := registry.Register(gauge); err != nil {
			return nil, fmt.Errorf("registering a replication gauge: %w", err)
		}
	}
	// One counter per node, labelled by outcome only, never by bucket or
	// peer: what an operator asks of it is whether changes fail and how.
	c.membershipChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_membership_changes_total",
		Help: "Membership changes this node proposed, an operator's or a recovery's, by outcome: committed, finished_another, " +
			"already_committed, refused, no_majority, indeterminate. An indeterminate change may still complete; 'nodes' says whether it did.",
	}, []string{"outcome"})
	for _, outcome := range []changeOutcome{changeCommitted, changeFinishedAnother, changeAlreadyCommitted,
		changeRefused, changeNoMajority, changeIndeterminate} {
		c.membershipChanges.WithLabelValues(string(outcome))
	}
	if err := registry.Register(c.membershipChanges); err != nil {
		return nil, fmt.Errorf("registering the membership change counter: %w", err)
	}
	// The on-demand read's two aggregate series (takeover.go:onDemandMetrics):
	// labelled by outcome only, never by bucket or database (rule 11).
	for _, collector := range []prometheus.Collector{c.onDemand.reads, c.onDemand.waits} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("registering the on-demand read metrics: %w", err)
		}
	}
	return registry, nil
}

// streamReadings is one pass over the members' streams.
type streamReadings struct {
	notInSync  int
	lagEntries uint64
	lagSeconds float64
}

// streamReadings reads every member's stream. A member this node has no peer
// for has no stream and counts as not in sync, as it counts toward no write
// concern.
func (c *Cluster) streamReadings(now time.Time) streamReadings {
	synced := c.s.wal.SyncedSequence()
	var readings streamReadings
	for _, member := range c.otherMembers() {
		c.peersMu.RLock()
		p, known := c.peers[member.ID]
		c.peersMu.RUnlock()
		if !known {
			readings.notInSync++
			continue
		}
		lag := p.lagAt(now, synced)
		if lag.notInSync {
			readings.notInSync++
		}
		readings.lagEntries = max(readings.lagEntries, lag.entries)
		readings.lagSeconds = max(readings.lagSeconds, lag.owedFor.Seconds())
	}
	return readings
}

// unreachableMembers counts the members whose gossip is failing.
func (c *Cluster) unreachableMembers() int {
	unreachable := 0
	for _, member := range c.otherMembers() {
		c.peersMu.RLock()
		p, known := c.peers[member.ID]
		c.peersMu.RUnlock()
		if !known {
			continue
		}
		if _, down := p.downSince(); down {
			unreachable++
		}
	}
	return unreachable
}

// streamLag is one stream's reading.
type streamLag struct {
	notInSync bool
	entries   uint64
	owedFor   time.Duration
}

// lagAt reads the stream to p against this node's synced tail. A stream that
// is streaming, has shipped its boundary, has nothing queued and has had
// everything it queued confirmed is idle: it is owed nothing, whatever the gap
// between the tail and its cursor, because entries below the tail that it was
// not sent were not addressed to it (spec §19.3.13). Any other stream reads
// the gap, and, if something was addressed to it since it was last idle, how
// long it has gone without confirming any of it.
func (p *peer) lagAt(now time.Time, synced uint64) streamLag {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	cursor, lastAck := p.confirmed()
	if p.idleLocked(cursor) {
		return streamLag{}
	}
	lag := streamLag{notInSync: !p.inSyncLocked()}
	if synced > cursor {
		lag.entries = synced - cursor
	}
	if !p.owedSince.IsZero() {
		lag.owedFor = now.Sub(maxTime(p.owedSince, lastAck))
	}
	return lag
}

// confirmed reads this node's cursor for the peer and when it last moved.
func (p *peer) confirmed() (uint64, time.Time) {
	p.ackMu.Lock()
	defer p.ackMu.Unlock()
	return p.applied[p.c.self.ID], p.lastAckAt
}

// idleLocked reports whether the stream is owed nothing. The caller holds
// streamMu.
func (p *peer) idleLocked(cursor uint64) bool {
	return p.state == streamStreaming && p.boundaryShipped && len(p.sendCh) == 0 && cursor >= p.queuedThrough
}

// noteOwed marks the peer owed from now, unless it already is.
func (p *peer) noteOwed() {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	p.noteOwedLocked()
}

// noteOwedLocked is noteOwed for a caller holding streamMu.
func (p *peer) noteOwedLocked() {
	if p.owedSince.IsZero() {
		p.owedSince = time.Now()
	}
}

// settleIfIdle clears the owed mark once the stream is idle.
func (p *peer) settleIfIdle() {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	p.settleIfIdleLocked()
}

// settleIfIdleLocked is settleIfIdle for a caller holding streamMu.
func (p *peer) settleIfIdleLocked() {
	if cursor, _ := p.confirmed(); p.idleLocked(cursor) {
		p.owedSince = time.Time{}
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
