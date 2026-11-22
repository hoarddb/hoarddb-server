package server

import (
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Stranded entries (spec §19.3.12, §19.3.13; build doc step 6.5).
//
// A data entry this node logged as a bucket's owner is shipped only while this
// node owns the bucket (R-OWNS). One it did not deliver before ownership moved
// is stranded: the new owner holds its effect by snapshot, under its own
// positions, and shipping the entry late could apply it twice. A replay counts
// each such entry it passes over, once per entry however many replays pass it,
// in hoarddb_replication_entries_stranded_total.
//
// What is counted is a data entry — not the create or the drop of a bucket,
// whose effect a new holder takes by its own pull or by the drop's delivery —
// that a replay passes over because ownership moved. A replica that lacks such
// an entry has its replay read it, so the count is never below what any one
// replica lacks; it can be above, because a replay to a member that never held
// the bucket passes over entries the bucket's old replicas did receive.

// replicationEntriesStranded is the counter, summed over every node of the
// process. Each Cluster also counts its own (strandedLedger.total), which is
// what a test of one node reads.
var replicationEntriesStranded = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "hoarddb_replication_entries_stranded_total",
	Help: "Entries this node originated and did not deliver before it stopped owning their bucket; they are not shipped late. Each is counted once.",
})

func init() {
	prometheus.MustRegister(replicationEntriesStranded)
}

// seqRange is a closed range of this node's log sequences.
type seqRange struct{ first, last uint64 }

// strandedLedger remembers which sequences a replay has counted as stranded,
// so an entry passed over by many replays is counted once. It holds the
// counted sequences themselves, as sorted ranges merged where adjacent — never
// a stretch a replay merely read: an entry read while this node still owned
// its bucket, whose ship was then refused, is stranded only after the adoption
// and must be counted then (the verifier's S3 on dispatch 6). Ranges below the
// log's first sequence are dropped, because no replay reads them again, so the
// ledger holds at most one range per stranded entry the retained log still
// holds, and one for a run of adjacent ones.
type strandedLedger struct {
	mu      sync.Mutex
	counted []seqRange
	total   atomic.Uint64
}

// count records the stranded sequences a replay found, returns how many of
// them no earlier replay had counted, and forgets what lies below logFirst,
// the log's first sequence (0 when it could not be read: nothing is forgotten).
func (l *strandedLedger) count(stranded []uint64, logFirst uint64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(logFirst)
	newly := 0
	for _, seq := range stranded {
		if l.addLocked(seq) {
			newly++
		}
	}
	if newly > 0 {
		l.total.Add(uint64(newly))
		replicationEntriesStranded.Add(float64(newly))
	}
	return newly
}

// pruneLocked drops every range wholly below logFirst.
func (l *strandedLedger) pruneLocked(logFirst uint64) {
	kept := 0
	for kept < len(l.counted) && l.counted[kept].last < logFirst {
		kept++
	}
	l.counted = slices.Delete(l.counted, 0, kept)
}

// addLocked records seq as counted and reports whether it was not already,
// merging it with an adjacent range on either side.
func (l *strandedLedger) addLocked(seq uint64) bool {
	// The first range that ends at or after seq.
	at, _ := slices.BinarySearchFunc(l.counted, seq, func(r seqRange, target uint64) int {
		switch {
		case r.last < target:
			return -1
		case r.last > target:
			return 1
		default:
			return 0
		}
	})
	if at < len(l.counted) && l.counted[at].first <= seq {
		return false
	}
	joinsBefore := at > 0 && l.counted[at-1].last+1 == seq
	joinsAfter := at < len(l.counted) && seq+1 == l.counted[at].first
	switch {
	case joinsBefore && joinsAfter:
		l.counted[at-1].last = l.counted[at].last
		l.counted = slices.Delete(l.counted, at, at+1)
	case joinsBefore:
		l.counted[at-1].last = seq
	case joinsAfter:
		l.counted[at].first = seq
	default:
		l.counted = slices.Insert(l.counted, at, seqRange{first: seq, last: seq})
	}
	return true
}

// strandedNote is what one replay found stranded, for its one WARN.
type strandedNote struct {
	entries int
	buckets []string
}

// add records newly counted entries and up to five distinct bucket names.
func (n *strandedNote) add(newly int, buckets []string) {
	if newly == 0 {
		return
	}
	n.entries += newly
	for _, bucket := range buckets {
		if len(n.buckets) < 5 && !slices.Contains(n.buckets, bucket) {
			n.buckets = append(n.buckets, bucket)
		}
	}
}

// warn logs the replay's one line, if it stranded anything.
func (n *strandedNote) warn(logger *slog.Logger, peerID string) {
	if n.entries == 0 {
		return
	}
	logger.Warn("entries this node logged as a bucket's owner were not delivered before the bucket moved to another owner; "+
		"they are not shipped from here (a new owner holds them by its copy, or a replica that was behind lacks them until it is re-seeded)",
		"node_id", peerID, "entries", n.entries, "buckets", n.buckets)
}
