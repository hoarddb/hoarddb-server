package storage

import (
	"sync/atomic"
	"time"
)

// WALFlushBuckets is how many buckets WALStats.FlushesAtMost has.
const WALFlushBuckets = 12

// walFlushBucketBounds are the upper bounds of the group-commit flush
// histogram: 0.5 ms to 1 s, doubling, so an idle flush, a preallocated one and
// a growing one each land in a bucket of their own (spec §8).
var walFlushBucketBounds = [WALFlushBuckets]time.Duration{
	500 * time.Microsecond, time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond,
	8 * time.Millisecond, 16 * time.Millisecond, 32 * time.Millisecond, 64 * time.Millisecond,
	128 * time.Millisecond, 256 * time.Millisecond, 512 * time.Millisecond, time.Second,
}

// WALFlushBucketBounds returns the upper bound of each of WALStats.FlushesAtMost's
// buckets, in order.
func WALFlushBucketBounds() [WALFlushBuckets]time.Duration {
	return walFlushBucketBounds
}

// WALStats is a snapshot of one node's log counters, for the metrics a
// collector exports at scrape time. Every field is node-wide: the log is one
// per node, never per bucket. The counters only grow, from 0 at the open.
type WALStats struct {
	// Preallocated is whether the log was opened with preallocated segments.
	Preallocated bool
	// SpareReady is whether a filled spare segment is waiting for the next
	// rotation.
	SpareReady bool
	// Flushes is the number of group-commit flushes, and FlushTime the time
	// they took in total.
	Flushes   uint64
	FlushTime time.Duration
	// FlushesAtMost[i] is the number of group-commit flushes that took at most
	// WALFlushBucketBounds()[i]: cumulative, as a Prometheus histogram's
	// buckets are. Flushes counts the ones above the last bound too.
	FlushesAtMost [WALFlushBuckets]uint64
	// Rotations is the number of segments retired by a rotation.
	Rotations uint64
	// RotationWaits is the number of rotations that waited for a fill in
	// flight or filled the spare themselves, holding every write meanwhile,
	// and RotationWaitTime the time those took.
	RotationWaits    uint64
	RotationWaitTime time.Duration
	// ZeroFillBytes is the zeros written into spares, and ZeroFillTime the
	// time fills took, failed ones included.
	ZeroFillBytes uint64
	ZeroFillTime  time.Duration
	// SpareFillFailures is the number of fills that failed. A fill Close
	// cancelled is not a failure.
	SpareFillFailures uint64
	// BytesAppended is the framed length of every entry written successfully
	// since the open, the frame word included, so a sealed entry counts its
	// longer on-disk form. Zero fill and segment headers are not in it: those
	// have ZeroFillBytes and are not entries.
	BytesAppended uint64
}

// walCounters are the atomics behind WALStats, added to without a lock. A
// flush is counted in flushes before its bucket, and Stats reads the buckets
// before flushes, so a snapshot never has more flushes in its buckets than in
// Flushes, which a histogram's +Inf bucket requires.
type walCounters struct {
	flushes          atomic.Uint64
	flushNanos       atomic.Int64
	flushBuckets     [WALFlushBuckets]atomic.Uint64
	rotations        atomic.Uint64
	rotationWaits    atomic.Uint64
	rotationWaitNano atomic.Int64
	zeroFillBytes    atomic.Uint64
	zeroFillNanos    atomic.Int64
	fillFailures     atomic.Uint64
	bytesAppended    atomic.Uint64
}

// observeFlush counts one group-commit flush that took took.
func (c *walCounters) observeFlush(took time.Duration) {
	c.flushes.Add(1)
	c.flushNanos.Add(int64(took))
	for i, bound := range walFlushBucketBounds {
		if took <= bound {
			c.flushBuckets[i].Add(1)
			return
		}
	}
}

// observeRotationWait counts one rotation that waited for its spare.
func (c *walCounters) observeRotationWait(took time.Duration) {
	c.rotationWaits.Add(1)
	c.rotationWaitNano.Add(int64(took))
}

// Stats returns a snapshot of the log's counters. It takes no lock but the
// spare's leaf lock, briefly, so a scrape never waits behind a rotation.
func (w *WAL) Stats() WALStats {
	w.spareMu.Lock()
	spareReady := w.spareState == spareReady
	w.spareMu.Unlock()

	c := &w.counters
	var atMost [WALFlushBuckets]uint64
	var below uint64
	for i := range c.flushBuckets {
		below += c.flushBuckets[i].Load()
		atMost[i] = below
	}
	return WALStats{
		Preallocated:      w.preallocate,
		SpareReady:        spareReady,
		Flushes:           c.flushes.Load(),
		FlushTime:         time.Duration(c.flushNanos.Load()),
		Rotations:         c.rotations.Load(),
		RotationWaits:     c.rotationWaits.Load(),
		RotationWaitTime:  time.Duration(c.rotationWaitNano.Load()),
		ZeroFillBytes:     c.zeroFillBytes.Load(),
		ZeroFillTime:      time.Duration(c.zeroFillNanos.Load()),
		FlushesAtMost:     atMost,
		SpareFillFailures: c.fillFailures.Load(),
		BytesAppended:     c.bytesAppended.Load(),
	}
}
