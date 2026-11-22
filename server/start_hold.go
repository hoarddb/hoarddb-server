package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The start hold (BUG-0481 part 2, owner ruling 2026-10-17).
//
// A node's log sequence is the version of every write it owns, and its peers
// hold positions in that log. A node whose log went backwards — a VM snapshot
// restore rolls the log and the data back together, so nothing on the node
// shows it; a lost wal/ directory under kept data — would give its next writes
// sequences its peers already hold other writes at: the new write of a key
// then carries the version of the write it replaced, or a lower one, and a
// consistent get and its repair can prefer the older value.
//
// So after a start this node logs no write of its own until a majority of its
// peers has answered its stream handshake. Each answer carries the highest
// position the peer holds in this node's log (ReplPosition.HighWater, under
// whichever incarnation it was learned). Once a majority has answered, the log
// is moved past the highest of them (storage.WAL.FastForward) before the
// first write is given a sequence. Until then a write waits, for as long as it
// would wait for its copies (the write-concern timeout), and is then refused
// with the peers not yet heard from named. A peer that answers after the hold has opened and
// reports a higher position moves the log again, and that is logged.
//
// The ordering that makes it durable: a handshake reports its peer's answer
// and waits for the hold to open before it asks the peer to rebase, because a
// rebase lowers the peer's position and the high-water mark would be lost to
// a crash between the two. The move itself is synced before the hold opens.
//
// Cost: one handshake answer per peer at start, which the node exchanges
// anyway, and an atomic load per write once open. Nothing per bucket, and a
// membership change costs nothing: the hold is per process.

// ErrStartHold refuses a write while this node has not heard from a majority
// of its peers since it started.
var ErrStartHold = errors.New("this node has not heard from a majority of its peers since it started")

// startHold is a node's hold on its own writes after a start.
type startHold struct {
	c *Cluster
	// began is when the service armed the hold, before it opened its
	// listeners, in Unix nanoseconds. Zero is a cluster no service started,
	// which has no stream to learn from and holds nothing.
	began atomic.Int64
	// open is set once the log has been moved past every reported position
	// of a majority; opened is closed at the same moment.
	open   atomic.Bool
	opened chan struct{}

	mu sync.Mutex
	// heard is, per peer, the highest position it reported in this node's
	// log. Guarded by mu.
	heard map[string]uint64
	// relogs are the drops start-up replay learned and applied, which the
	// hold logs as this node's own once it opens (BUG-0610). Written by
	// replay before arm, read by the goroutine arm starts. Guarded by mu.
	relogs []learnedDrop
}

// learnedDrop is a database's generations that start-up replay learned
// dropped.
type learnedDrop struct {
	database    string
	generations []int64
}

// deferRelog records generations of database, learned and applied by
// start-up replay, for the hold to log once it opens. Kept in memory only
// (owner ruling 2026-10-18): a stop before then loses it, and the peers learn
// the drop from this node's refusals instead (Server.learnTombstones).
func (h *startHold) deferRelog(database string, generations []int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.relogs = append(h.relogs, learnedDrop{database: database, generations: generations})
}

// relogWhenOpen logs the drops replay learned, once the hold has opened, and
// ships them as any database DDL. It returns early when the cluster stops
// first: those re-logs are lost, as deferRelog says.
func (h *startHold) relogWhenOpen(drops []learnedDrop) {
	defer h.c.storeWG.Done()
	select {
	case <-h.opened:
	case <-h.c.ctx.Done():
		return
	}
	for _, drop := range drops {
		if err := h.c.relogDrop(withPeerEntry(h.c.ctx, false), drop.database, drop.generations); err != nil {
			h.c.logger.Warn("could not log the drop this node's start-up replay learned; its peers learn it when this node refuses an entry of the database",
				"database", drop.database, "generations", drop.generations, "error", err)
		}
	}
}

func newStartHold(c *Cluster) *startHold {
	return &startHold{c: c, opened: make(chan struct{}), heard: make(map[string]uint64)}
}

// arm begins the hold's clock and opens it at once on a node with no peer.
// The service arms it before it accepts a connection, so no write of a
// client's can be logged before it.
func (h *startHold) arm() {
	h.began.CompareAndSwap(0, time.Now().UnixNano())
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.openIfMajorityLocked(); err != nil {
		h.c.logger.Error("could not open this node's writes after its start", "error", err)
	}
	if len(h.relogs) > 0 {
		drops := h.relogs
		h.relogs = nil
		// The re-log appends to the log, so it is a node-store goroutine.
		if h.c.startStoreGoroutine() {
			go h.relogWhenOpen(drops)
		}
	}
}

// peers is every member but this node.
func (h *startHold) peers() []string {
	var ids []string
	for _, member := range h.c.Members() {
		if member.ID != h.c.self.ID {
			ids = append(ids, member.ID)
		}
	}
	return ids
}

// heardOf reports whether peer has answered a handshake since the start.
func (h *startHold) heardOf(peer string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, heard := h.heard[peer]
	return heard
}

// heardFrom records what peer answered, and opens the hold, or moves the log
// again once it is open, as that answer requires.
func (h *startHold) heardFrom(peer string, highWater uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.heard[peer] = max(h.heard[peer], highWater)
	if !h.open.Load() {
		return h.openIfMajorityLocked()
	}
	before := h.c.walLastSequence()
	moved, err := h.c.s.wal.FastForward(highWater)
	if err != nil {
		return fmt.Errorf("moving this node's log past position %d, which %s holds: %w", highWater, peer, err)
	}
	if moved {
		h.c.logger.Warn(fmt.Sprintf("%s holds this node's log through %d and answered only after this node began taking writes, "+
			"when its log was at %d: the log has moved past %d, and writes this node took since it started may be ordered below writes %s holds. "+
			"To make every copy identical, remove %s ('remove-node %s'), empty its data directory and add it again",
			peer, highWater, before, highWater, peer, peer, peer))
	}
	return nil
}

// openIfMajorityLocked opens the hold when a majority of the current peers
// has answered, after moving the log past the highest position any answer
// reported. The caller holds mu.
func (h *startHold) openIfMajorityLocked() error {
	if h.open.Load() {
		return nil
	}
	peers := h.peers()
	answered := 0
	var highWater uint64
	for _, peer := range peers {
		if position, ok := h.heard[peer]; ok {
			answered++
			highWater = max(highWater, position)
		}
	}
	if needed := len(peers)/2 + 1; len(peers) > 0 && answered < needed {
		return nil
	}
	before := h.c.walLastSequence()
	moved, err := h.c.s.wal.FastForward(highWater)
	if err != nil {
		return fmt.Errorf("moving this node's log past position %d, which a peer holds, before taking writes: %w", highWater, err)
	}
	if moved {
		h.c.logger.Warn(fmt.Sprintf("this node's log ended at %d and its peers hold it through %d: the log went backwards since they "+
			"last heard from it (a restored snapshot of this node, or a lost log). It has moved past %d before taking any write, so no "+
			"write reuses a sequence a peer holds; writes the peers hold that this node lost are still on them",
			before, highWater, highWater))
	}
	h.open.Store(true)
	close(h.opened)
	// A take-over waits for the hold before it opens a term (completeTakeOver):
	// it is woken now rather than at its next back-off.
	h.c.signalTakeOver()
	if len(peers) > 0 {
		h.c.logger.Info("this node takes writes: a majority of its peers has answered since it started",
			"answered", answered, "peers", len(peers), "log_moved", moved)
	}
	return nil
}

// startHoldRecheck is how often a waiting handshake looks again at whether
// the answers already heard are a majority: a membership change, such as the
// removal of a peer that is gone, can make them one with nothing else
// arriving to say so.
const startHoldRecheck = 500 * time.Millisecond

// await blocks a handshake until the hold is open, so its rebase cannot lower
// a peer's position before the log has moved past it. It returns false when
// stop or the cluster ended first.
func (h *startHold) await(stop <-chan struct{}) bool {
	ticker := time.NewTicker(startHoldRecheck)
	defer ticker.Stop()
	for {
		select {
		case <-h.opened:
			return true
		case <-stop:
			return false
		case <-h.c.ctx.Done():
			return false
		case <-ticker.C:
			h.mu.Lock()
			err := h.openIfMajorityLocked()
			h.mu.Unlock()
			if err != nil {
				h.c.logger.Error("could not open this node's writes after its start", "error", err)
			}
		}
	}
}

// shut reports whether the hold is armed and has not opened: a node that
// started and has not heard from a majority of its peers.
func (h *startHold) shut() bool {
	return h.began.Load() != 0 && !h.open.Load()
}

// admit holds a write of this node's own until the hold is open, for at most
// the write-concern timeout, and then refuses it naming the peers not heard
// from.
func (h *startHold) admit(ctx context.Context) error {
	if h.open.Load() || h.began.Load() == 0 {
		return nil
	}
	// A membership change since the last answer can make the answers already
	// heard a majority.
	h.mu.Lock()
	err := h.openIfMajorityLocked()
	h.mu.Unlock()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStartHold, err)
	}
	if h.open.Load() {
		return nil
	}
	timer := time.NewTimer(h.c.wcTimeout)
	defer timer.Stop()
	select {
	case <-h.opened:
		return nil
	case <-timer.C:
		return h.refusal()
	case <-ctx.Done():
		return fmt.Errorf("waiting for a majority of this node's peers to answer since it started: %w", ctx.Err())
	}
}

// refusal is the write's answer while the hold has outlasted the
// write-concern timeout: what is missing, and what to do (rule 3).
func (h *startHold) refusal() error {
	h.mu.Lock()
	peers := h.peers()
	var unheard []string
	for _, peer := range peers {
		if _, ok := h.heard[peer]; !ok {
			unheard = append(unheard, peer)
		}
	}
	h.mu.Unlock()
	slices.Sort(unheard)
	needed := len(peers)/2 + 1
	since := time.Since(time.Unix(0, h.began.Load())).Round(time.Second)
	return &sentencedError{cause: ErrStartHold, message: fmt.Sprintf(
		"this node started %s ago and %d of its %d peers have answered it; it takes no write until %d have, so that it never "+
			"gives a write a log sequence a peer already holds (a node restored from a snapshot cannot tell otherwise). Not answered: %s. "+
			"Start them or restore the network to them; writes resume as soon as enough answer. A peer that is gone for good is removed "+
			"with 'remove-node <node-id>'. Nothing was written",
		since, len(peers)-len(unheard), len(peers), needed, strings.Join(unheard, ", "))}
}

// passMarksUnderANewLog is the local half of the start hold (BUG-0609). Every
// bucket's applied_seq, a replica's too, and every database marker's
// created_seq are positions in this node's own log; the apply and replay gates
// read an entry at or below them as already applied. Peers hold positions only
// of the entries this node owns, so the start hold cannot move a log past the
// marks of buckets this node only replicates. When the open found no entry
// and no skip (a lost wal/ or lost segments under kept data, or a restore of
// the data alone), the log is moved past the highest mark before anything is
// appended. The test is the log's last sequence alone, not its incarnation
// chain: OpenWAL writes the chain before this runs, so a crash during the walk
// leaves a chain that looks old over a log that is still empty, and lost
// segments under a kept chain look the same. A log that keeps only its older
// segments is not covered; that needs the node-level record of BUG-0609's
// option 2.
//
// Cost: nothing on an open that finds an entry or a skip. On one that does
// not, one walk of the catalog and one state read per bucket, O(buckets);
// repeated on each start until something is appended. No per-bucket state,
// metric or goroutine, and a membership change costs nothing.
func (s *Server) passMarksUnderANewLog() error {
	if s.wal.LastSequence() != 0 {
		return nil
	}
	s.newLogMarkScans.Add(1)
	highest, err := s.highestLocalMark()
	if err != nil {
		return fmt.Errorf("reading the applied marks of this node's buckets under a new log: %w", err)
	}
	moved, err := s.wal.FastForward(highest)
	if err != nil {
		return fmt.Errorf("moving a new log past position %d, which this node's data records as applied: %w", highest, err)
	}
	if moved {
		s.logger.Warn(fmt.Sprintf("this node's write-ahead log holds no entry, but its data records entries applied through log position %d "+
			"(the wal/ directory or its segment files were removed, or not restored with the data). The new log starts after %d, so no entry is "+
			"taken for one already applied. Entries the lost log held and the store had not applied are gone from this node; "+
			"to make its copies whole, remove it ('remove-node %s'), empty its data directory and add it again", highest, highest, s.config.Server.NodeID))
	}
	return nil
}

// highestLocalMark is the highest position in this node's log that its data
// records: every bucket's applied_seq and every database marker's created_seq.
// A bucket of a dropped database not yet reclaimed counts as well; its state
// is still there and a mark too high costs only a few unused sequences.
func (s *Server) highestLocalMark() (uint64, error) {
	lower, upper := catalogPrefix()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	var highest uint64
	for it.Seek(nil); it.Valid(); it.Next() {
		name := string(it.Key()[len(lower):])
		encoded, err := it.Value()
		if err != nil {
			return 0, fmt.Errorf("reading the catalog entry %q: %w", name, err)
		}
		if strings.HasSuffix(name, "/") {
			var marker databaseMarker
			if err := bson.Unmarshal(encoded, &marker); err != nil {
				return 0, fmt.Errorf("decoding the catalog entry of database %q: %w", strings.TrimSuffix(name, "/"), err)
			}
			highest = max(highest, uint64(marker.CreatedSeq))
			continue
		}
		var entry catalogEntry
		if err := bson.Unmarshal(encoded, &entry); err != nil {
			return 0, fmt.Errorf("decoding the catalog entry %q: %w", name, err)
		}
		// The entry's database is the key's, not a stored field.
		database, _, _ := strings.Cut(name, "/")
		state, err := store.LoadBucketState(snap, uint16(entry.Slot), storage.LegacyDatabaseID(database), uint64(entry.ID))
		if err != nil {
			return 0, fmt.Errorf("reading the state of bucket %q: %w", name, err)
		}
		highest = max(highest, state.AppliedSeq)
	}
	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("walking the catalog: %w", err)
	}
	return highest, nil
}
