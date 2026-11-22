package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A write the node logged to its write-ahead log is durable by the time it is
// applied: the log is synced first, and replay covers anything the store
// lost. Every other write commits to a store opened with Badger's SyncWrites
// off, which is store-wide, and would be answered while a power cut or a
// kernel crash can still lose it. Owner ruling 43: such a write is
// synced before it is answered. Whether the node has a log does not decide
// it: a node with one that is alone in its ring — a seed before its peers
// join, seeds that name only itself, a cluster removed down to one member —
// does not route (Cluster.Enabled), so its writes are dispatched locally and
// never logged. What decides is whether this write was logged, which an
// apply sequence in its context says.
//
// The sync is taken where the write is answered, after the bucket lock is
// released, never inside the apply: the lock would make every writer of a
// bucket wait out the previous writer's sync, and no two writes of one bucket
// could share one. ackSyncGroup lets every write committed before a sync
// starts be answered by that one sync (group commit).
//
// A write is answered in three places, and only these acknowledge durably:
// the client handler (NewHandler), once per frame, on a node that does not
// route; the in-process methods that take no context (Insert, Push,
// CreateBucket, ...); and writeFieldThen, for the store-field verbs. A node
// that routes answers through Cluster.handleClientFrame, which logs every
// write before it applies it.

// commitNoteCtxKey carries the *commitNote of the write being answered.
type commitNoteCtxKey struct{}

// commitNote records that a write being answered committed to the store. An
// apply that commits nothing — an empty pop, a refusal — leaves it unset, and
// its answer waits for no sync. It is atomic because a restore chunk applies
// its documents under one frame's note.
type commitNote struct{ committed atomic.Bool }

// noteCommitted marks the write ctx answers as having committed to the store.
// Without a note in ctx — replay, replication, a logged write, a background
// sweep — it does nothing.
func noteCommitted(ctx context.Context) {
	if note, ok := ctx.Value(commitNoteCtxKey{}).(*commitNote); ok {
		note.committed.Store(true)
	}
}

// acknowledgeDurably runs write and, when it committed to the store, returns
// only once that commit is durable. It syncs nothing for a write that carries
// an apply sequence, which was logged before it was applied, nor when the
// operator traded the sync away with storage.unsafe_ack_before_sync. A nested
// call, inside a write already being answered, leaves the sync to the outer
// one, so a frame that applies many documents syncs once.
func acknowledgeDurably[A any](ctx context.Context, s *Server, write func(context.Context) (A, error)) (A, error) {
	if s.config.Storage.UnsafeAckBeforeSync || applySeqFrom(ctx) != 0 || ctx.Value(commitNoteCtxKey{}) != nil {
		return write(ctx)
	}
	note := &commitNote{}
	s.ackSyncs.writeStarted()
	answer, err := write(context.WithValue(ctx, commitNoteCtxKey{}, note))
	s.ackSyncs.writeEnded()
	if !note.committed.Load() {
		return answer, err
	}
	if syncErr := s.ackSyncs.syncAfter(s.kv); syncErr != nil {
		var unanswered A
		// The commit is in memory and a read may see it, so a retry is not
		// known to be safe: the outcome is unknown, not "not applied".
		return unanswered, withOutcome(outcomeUnknown, &writeNotDurableError{syncErr: syncErr})
	}
	return answer, err
}

// errWriteNotDurable is a write that committed but whose sync failed: it is
// not acknowledged, though a read may see it until the node restarts.
var errWriteNotDurable = errors.New("the write was applied but could not be written to disk, so it is not acknowledged and may be lost if the host loses power")

// writeNotDurableError is errWriteNotDurable with the error of the sync that
// failed, worded for a write whose effect a read can find.
type writeNotDurableError struct{ syncErr error }

func (e *writeNotDurableError) Error() string {
	return fmt.Sprintf("%v (%v). Check the node's disk (free space, I/O errors in the kernel log); "+
		"read the document back before retrying, because the write may be stored", errWriteNotDurable, e.syncErr)
}

func (e *writeNotDurableError) Unwrap() []error { return []error{errWriteNotDurable, e.syncErr} }

// forPop is the same failure worded for a pop (BUG-0548). A pop's answer is
// the only copy of the entry it removed, and is dropped with the
// acknowledgement, so reading back finds a queue without the entry and a
// retry takes the next one: the caller is told where the entry is instead.
func (e *writeNotDurableError) forPop(target string) error {
	return &sentencedError{cause: e, message: fmt.Sprintf("the pop removed an entry from %s but the removal could not be written to disk (%v), "+
		"so the pop is not acknowledged and the entry is not in this answer. The entry is no longer in the queue while this node runs, "+
		"and may come back after a restart or a power loss if the removal never reaches the disk. "+
		"Check the node's disk (free space, I/O errors in the kernel log), then run peek to see what is left", target, e.syncErr)}
}

// answerPop is the answer to a pop whose removal committed and whose sync
// then failed: answer, the pop's own answer with the entry it removed,
// restamped with write_outcome applied and a message that says the removal
// is not yet durable (BUG-0548, owner ruling of 2026-10-14). It is a pop-only
// exception to not acknowledging a write that is not synced. The answer is
// the only copy of the entry, so an error in its place loses the entry for
// certain, while this answer at worst serves it again after a restart or a
// power loss. Applied, not unknown: the removal is committed and every
// reader sees it, as a write-concern TIMEOUT's write is applied though fewer
// copies than asked hold it.
func (e *writeNotDurableError) answerPop(answer *transport.Frame, target string) (*transport.Frame, error) {
	// At one entry the text is the one BUG-0548 settled.
	held, keep := "the entry is in this answer, and may be served again", "Keep it; do not pop again to get it back, because a pop takes the next entry. "
	if entries := poppedItemCount(answer.Payload); entries > 1 {
		held = fmt.Sprintf("the %d entries are in this answer, and may be served again", entries)
		keep = "Keep them; do not pop again to get them back, because a pop takes the next entries. "
	}
	payload, err := restampedAnswer(answer.Payload, bson.D{
		{Key: "message", Value: fmt.Sprintf("popped from %s, but the removal could not be written to disk (%v), so it is not yet durable: "+
			"%s after a restart or a power loss if the removal never reaches the disk. "+
			"%sCheck the node's disk (free space, I/O errors in the kernel log)", target, e.syncErr, held, keep)},
		{Key: "write_outcome", Value: outcomeApplied}})
	if err != nil {
		return nil, fmt.Errorf("stamping the answer to a pop from %s, which holds the popped entry: %w", target, err)
	}
	answer.Payload = payload
	return answer, nil
}

// forClaim is the failure worded for a claim whose answer holds no lease:
// its entries wait out any lease it granted, so claiming again is safe
// (claim-ack-spec 5.4, text 3).
func (e *writeNotDurableError) forClaim(target string) error {
	return &sentencedError{cause: e, message: fmt.Sprintf("the claim on %s wrote to the bucket, but the write could not be written to disk (%v), "+
		"so the claim is not acknowledged and any lease it granted is not in this answer. Entries it claimed return to the queue when the lease ends, "+
		"or at once after a restart if the lease never reached the disk; claim again. "+
		"Check the node's disk (free space, I/O errors in the kernel log)", target, e.syncErr)}
}

// answerClaim is answerPop for a claim that took entries: its answer, with the
// lease, restamped applied with a message that says the lease is not yet
// durable (claim-ack-spec 5.4, text 4).
func (e *writeNotDurableError) answerClaim(answer *transport.Frame, target string) (*transport.Frame, error) {
	payload, err := restampedAnswer(answer.Payload, bson.D{
		{Key: "message", Value: fmt.Sprintf("claimed from %s, but the lease could not be written to disk (%v), so it is not yet durable: "+
			"the lease and its entries are in this answer. After a restart or a power loss the lease may not exist: its ack then answers unknown, "+
			"and the entries are delivered again. Check the node's disk (free space, I/O errors in the kernel log)", target, e.syncErr)},
		{Key: "write_outcome", Value: outcomeApplied}})
	if err != nil {
		return nil, fmt.Errorf("stamping the answer to a claim from %s, which holds its lease: %w", target, err)
	}
	answer.Payload = payload
	return answer, nil
}

// popTargetOf names what a pop frame pops, as a message names it: the bucket
// (database.bucket), or for a store-field pop the field of its document
// (database.bucket["key"].Field).
func popTargetOf(frame *transport.Frame) string {
	bucket, database := decodeFrameMeta(frame.Payload).scope(frame.Opcode)
	if frame.Opcode != transport.OpFieldPop {
		return database + "." + bucket
	}
	request := bson.Raw(frame.Payload)
	key, _ := request.Lookup("key").StringValueOK()
	field, _ := request.Lookup("field").StringValueOK()
	return fmt.Sprintf("%s.%s[%q].%s", database, bucket, key, field)
}

// ackGatherLimit bounds how long a Sync waits for the writes still being
// applied, so it can answer them too. Badger's DB.Sync holds the store's lock
// while it syncs the active memtable's log, and every commit takes that lock
// exclusively (DB.ensureRoomForWrite), so no write commits while a Sync runs:
// started the moment its first caller arrives, a Sync answers that caller and
// the one or two whose commits slipped in before it, and each of the others
// waits out one more. Waiting for them first took 16 concurrent writers from
// 1.5-3.5 writes per Sync to 14-15, and 2.4 to 5 times the throughput, on a
// workstation and on a VM (measured); 250 µs gathered half as many on one
// bucket, 2 ms no more than 1 ms. A lone writer waits for nothing; the bound
// caps what one slow write (a large batch) can add to the others.
const ackGatherLimit = time.Millisecond

// ackSyncGroup is group commit over the store's Sync. A caller's commits are
// durable once a Sync that started after the caller arrived has finished, so
// a caller that arrives while a Sync runs waits for the next one, and every
// caller waiting then is answered by that next one. Before a Sync starts it
// waits, up to ackGatherLimit, for the writes still being applied to arrive.
// At most one Sync runs at a time, and it runs on a caller's goroutine: the
// group starts none. The zero value is ready to use.
type ackSyncGroup struct {
	mu sync.Mutex
	// started counts the Syncs begun, and finished the number of the last
	// one to end; Syncs end in the order they start, because one runs at a
	// time.
	started  uint64
	finished uint64
	// running is set from the moment a caller takes the next Sync on — its
	// gathering included — until that Sync ends.
	running bool
	// done is closed when the running Sync ends.
	done chan struct{}
	// lastErr is the error of the Sync numbered finished.
	lastErr error
	// writing counts the writes being applied that have not yet reached
	// syncAfter, and drained, while a Sync gathers, is closed when it falls
	// to zero.
	writing int
	drained chan struct{}
}

// writeStarted counts a write that may soon need a Sync.
func (g *ackSyncGroup) writeStarted() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writing++
}

// writeEnded uncounts it, before it calls syncAfter or returns.
func (g *ackSyncGroup) writeEnded() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writing--
	if g.writing == 0 && g.drained != nil {
		close(g.drained)
		g.drained = nil
	}
}

// syncAfter returns once a Sync of kv that started after this call has
// finished, with that Sync's error. A Sync that ended later and also started
// after this call may answer instead: it covers the same commits.
func (g *ackSyncGroup) syncAfter(kv storage.KV) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	needed := g.started + 1
	for g.finished < needed {
		if g.running {
			done := g.done
			g.mu.Unlock()
			<-done
			g.mu.Lock()
			continue
		}
		g.running = true
		g.done = make(chan struct{})
		g.gather()
		// Numbered only now: a caller that arrived while this Sync gathered
		// needs this one, and is covered by it.
		g.started++
		number := g.started
		g.mu.Unlock()
		err := kv.Sync()
		g.mu.Lock()
		g.finished, g.lastErr, g.running = number, err, false
		close(g.done)
	}
	return g.lastErr
}

// gather waits, with g.mu held on entry and on return, until no write is
// being applied or ackGatherLimit has passed.
func (g *ackSyncGroup) gather() {
	if g.writing == 0 {
		return
	}
	drained := make(chan struct{})
	g.drained = drained
	g.mu.Unlock()
	limit := time.NewTimer(ackGatherLimit)
	select {
	case <-drained:
	case <-limit.C:
	}
	limit.Stop()
	g.mu.Lock()
	g.drained = nil
}

// unsafeAckWarning is the line a node logs at WARN when it starts with
// storage.unsafe_ack_before_sync on, or "" when the setting is off.
func (s *Server) unsafeAckWarning() string {
	if !s.config.Storage.UnsafeAckBeforeSync {
		return ""
	}
	// One line for every node: a node with a write-ahead log is still alone
	// in its ring until a peer joins, and answers its writes unlogged then.
	return "storage.unsafe_ack_before_sync is on: a write this node does not log to a write-ahead log " +
		"(every write while it has no peers in its ring) is answered before it is synced to disk, " +
		"so a power cut or a kernel crash can lose writes it acknowledged (a process crash cannot). " +
		"It is meant for benchmarks: unset it (or HOARDDB_UNSAFE_ACK_BEFORE_SYNC) before keeping any data you need"
}
