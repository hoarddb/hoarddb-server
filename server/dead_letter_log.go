package server

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// deadLetterLogCap is how many distinct buckets one window names
// (claim-ack-spec 11.2). A fixed array bounds the collector at O(10) memory
// whatever the number of buckets.
const deadLetterLogCap = 10

// deadLetterLogInterval is the length of one window of the paced dead-letter
// line. A variable so a test can shorten it.
var deadLetterLogInterval = time.Minute

// deadLetterEntry is one bucket's counts in the open window. The printed
// fields are what an earlier line already reported, so a later line carries
// only the difference. It holds names and counts and never entry content.
type deadLetterEntry struct {
	database, bucket string
	// deadLettered counts entries moved to the dead list; refused counts
	// claims the hard cap refused.
	deadLettered, refused               int
	printedDeadLettered, printedRefused int
}

// deadLetterReport is one line the collector hands back to be written.
type deadLetterReport struct {
	// entries are the buckets the line names, with the counts it adds.
	entries []deadLetterEntry
	// further counts events from buckets the full window could not name.
	further int
}

// deadLetterLog paces the operator's line about dead-lettered entries and
// hard-cap refusals, over every bucket of the node: the first event of a
// window prints at once, the rest are held up to deadLetterLogCap buckets and
// printed when the window closes, by the capacity sampler's tick and not by a
// later event, because a burst followed by silence is the normal shape of an
// incident. Its mutex is a leaf. The zero value is ready.
type deadLetterLog struct {
	mu sync.Mutex
	// clock is a test seam, set under mu by useClock; nil reads the wall
	// clock. The sampler's tick reads it from its own goroutine.
	clock   func() time.Time
	open    bool
	opened  time.Time
	held    [deadLetterLogCap]deadLetterEntry
	heldLen int
	further int
}

// useClock replaces the wall clock; for tests.
func (l *deadLetterLog) useClock(clock func() time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clock = clock
}

// now is the window's clock; the caller holds mu.
func (l *deadLetterLog) now() time.Time {
	if l.clock != nil {
		return l.clock()
	}
	return time.Now()
}

// note records one answering apply's effect on a bucket and returns the lines
// due now: the previous window's held names if it has closed, and this event's
// own line if it opens a new window.
func (l *deadLetterLog) note(database, bucket string, deadLettered, refused int) []deadLetterReport {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var due []deadLetterReport
	if closed, ok := l.closeLocked(now); ok {
		due = append(due, closed)
	}
	opening := !l.open
	if opening {
		l.open, l.opened = true, now
	}
	entry := l.entryLocked(database, bucket)
	if entry == nil {
		l.further++
		return due
	}
	entry.deadLettered += deadLettered
	entry.refused += refused
	if opening {
		entry.printedDeadLettered, entry.printedRefused = entry.deadLettered, entry.refused
		due = append(due, deadLetterReport{entries: []deadLetterEntry{*entry}})
	}
	return due
}

// flush returns the held names of a window that has closed. The capacity
// sampler calls it on every tick.
func (l *deadLetterLog) flush() (deadLetterReport, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeLocked(l.now())
}

// closeLocked ends the open window if its time is up and reports what no line
// has carried yet.
func (l *deadLetterLog) closeLocked(now time.Time) (deadLetterReport, bool) {
	if !l.open || now.Sub(l.opened) < deadLetterLogInterval {
		return deadLetterReport{}, false
	}
	var report deadLetterReport
	for _, entry := range l.held[:l.heldLen] {
		entry.deadLettered -= entry.printedDeadLettered
		entry.refused -= entry.printedRefused
		if entry.deadLettered > 0 || entry.refused > 0 {
			report.entries = append(report.entries, entry)
		}
	}
	report.further = l.further
	l.open, l.heldLen, l.further = false, 0, 0
	l.held = [deadLetterLogCap]deadLetterEntry{}
	return report, len(report.entries) > 0 || report.further > 0
}

// entryLocked finds the bucket's entry in the window, adds one while there is
// room, and returns nil when the window is full.
func (l *deadLetterLog) entryLocked(database, bucket string) *deadLetterEntry {
	for index := range l.held[:l.heldLen] {
		if l.held[index].database == database && l.held[index].bucket == bucket {
			return &l.held[index]
		}
	}
	if l.heldLen == deadLetterLogCap {
		return nil
	}
	l.held[l.heldLen] = deadLetterEntry{database: database, bucket: bucket}
	l.heldLen++
	return &l.held[l.heldLen-1]
}

// message words a report: each bucket by name with its count and the command
// that reads what happened, and how many events from unnamed buckets the
// window held back. It carries names and counts, never entry content.
func (r deadLetterReport) message() string {
	var dead, refused []string
	for _, entry := range r.entries {
		if entry.deadLettered > 0 {
			dead = append(dead, fmt.Sprintf("bucket %q in database %q: %d (read them with db.%s.dead())",
				entry.bucket, entry.database, entry.deadLettered, entry.bucket))
		}
		if entry.refused > 0 {
			refused = append(refused, fmt.Sprintf("bucket %q in database %q: %d (take the entry with db.%s.pop())",
				entry.bucket, entry.database, entry.refused, entry.bucket))
		}
	}
	var parts []string
	if len(dead) > 0 {
		parts = append(parts, "Entries were dead-lettered after their last delivery and wait on a dead list, where nothing delivers them again. "+
			strings.Join(dead, "; ")+". Put them back with requeue({all: true}) or drop them with discard({all: true}).")
	}
	if len(refused) > 0 {
		parts = append(parts, "Claims were refused because the entry at the front of the bucket is over the largest size a claim moves. "+
			strings.Join(refused, "; ")+".")
	}
	if r.further > 0 {
		parts = append(parts, fmt.Sprintf("%d more events in other buckets are not named here (at most %d buckets are named a minute); "+
			"hoarddb_queue_entries_dead_lettered_total and hoarddb_queue_claims_entry_too_large_total count them all.", r.further, deadLetterLogCap))
	}
	return strings.Join(parts, " ")
}

// logDeadLetters writes the lines a collector call returned.
func (s *Server) logDeadLetters(due ...deadLetterReport) {
	for _, report := range due {
		s.logger.Warn(report.message())
	}
}

// noteDeadLettered records the dead-letterings and the hard-cap refusal of one
// answering apply for the paced line. Callers have checked answeringApply, so
// a replica's apply and a replay never come here.
func (s *Server) noteDeadLettered(database, bucket string, deadLettered, refused int) {
	if deadLettered == 0 && refused == 0 {
		return
	}
	s.logDeadLetters(s.deadLetters.note(database, bucket, deadLettered, refused)...)
}

// flushDeadLetterLog writes the held names of a closed window; the capacity
// sampler's tick calls it so no goroutine or timer is added for the line.
func (s *Server) flushDeadLetterLog() {
	if report, ok := s.deadLetters.flush(); ok {
		s.logDeadLetters(report)
	}
}
