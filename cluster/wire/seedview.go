package wire

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// This file is the seam between the seeding operator surface (the RING_ADMIN commands
// seeding_status, seeding_retry and seeding_forget) and the seeding engine (the pullers, the
// buffer and the readiness gate). The surface knows nothing of pulls: it asks a SeedingView
// for rows and forwards two actions. The engine lives in this package too, so it implements
// the interface without an import cycle, and a Node without an engine answers through NoSeeding.

// SeedState is where one slot of one database stands on this node, as the operator reads it.
// The wire carries the lowercase word; the CLI prints it upper-cased.
type SeedState string

// The states of a SeedRow (seeding section 11).
const (
	// SeedPlacing means the slot is in this node's copy set and its pull is queued.
	SeedPlacing SeedState = "placing"
	// SeedSeeding means a pull is applying chunks.
	SeedSeeding SeedState = "seeding"
	// SeedStalled means no chunk was applied for replication.seed_idle_seconds.
	SeedStalled SeedState = "stalled"
	// SeedReady means this node holds a full, attested copy and serves the slot.
	SeedReady SeedState = "ready"
	// SeedRetired means the slot left this node's copy set and its range is kept until the
	// deletion gate passes. Only this state can be forgotten.
	SeedRetired SeedState = "retired"
	// SeedRetiring means the deletion gate passed and the range delete has started. It can never
	// be cancelled, so an operator waits: neither retry nor forget applies (the machine's SlotRetiring).
	SeedRetiring SeedState = "retiring"
)

var seedStates = []SeedState{SeedPlacing, SeedSeeding, SeedStalled, SeedReady, SeedRetired, SeedRetiring}

// SeedRow is one slot of one database in the operator's view of this node's seeding.
type SeedRow struct {
	// Database is the database's name.
	Database string
	// Slot is the slot, 0 to 8191.
	Slot int
	// State is where the slot stands.
	State SeedState
	// Source is the node id the pull reads from; empty when none is chosen.
	Source string
	// BytesDone is the bytes applied so far. BytesKnown is the bytes the source said the
	// range holds, or 0 while it is not known.
	BytesDone, BytesKnown int64
	// Entries is the entries applied so far.
	Entries int64
	// Idle is how long ago the last chunk was applied; 0 when none has been.
	Idle time.Duration
	// Refusal is the text of the last refusal the source gave, or empty. It is shown to the
	// operator as it is, so it should name the fix.
	Refusal string
}

// SeedingView is what the seeding engine gives the operator surface. A node serves it from
// memory the engine already keeps, so a status is O(rows in flight and queued), never
// O(buckets). Its methods may be called concurrently with the engine and with each other.
//
// The lifecycle rules below bind the engine (owner-reviewed 2026-11-01); the surface cannot
// check them. A record over data is never removed by anything but the retire path and Forget.
type SeedingView interface {
	// Status returns every slot that is not an uneventful ready one: the pulls placed, running
	// and stalled, and the retired and retiring records that wait for or run their range delete.
	// The order is free; the surface sorts and pages. Every row's State must be one of the six
	// words; the surface refuses a status that holds another, naming the row.
	Status() []SeedRow
	// Retry restarts the pull of one slot from a fresh cut, on another legal source if one
	// exists, and does not wait for the pull to make progress. It is valid only on a record
	// that is seeding or stalled. Every other state, a slot with no record included, is
	// SeedNotHeld carrying that state's own word ("absent", "placing", "ready", "retired" or
	// "retiring"), and nothing changes.
	Retry(ctx context.Context, database string, slot int) error
	// Forget removes the retired record of one slot and keeps its range, so the slot seeds from
	// scratch the next time it is in this node's copy set (the next seed's reset deletes the
	// range first, once a source has accepted the pull; a pair that leaves the copy set again
	// before that is dropped as a left seed, range included). The engine must take the same pair lock the reconciler takes, re-read the
	// durable state under it, and delete the record only if it is still retired: a record that
	// is placing, seeding, stalled, ready or retiring (the delete has begun and cannot be
	// stopped) is SeedHeld with its state word, and a record that left retired while Forget waited
	// for the lock is judged by what it is now. A slot with no record is SeedNothingToForget. The
	// delete must be durable (synced) before Forget returns nil, because the CLI tells the
	// operator it is done.
	//
	// A retired record that no other member of the pair's copy set attests a copy of may hold
	// the only copy of the slot's data. Forget refuses that with SeedOnlyCopy, changing nothing,
	// unless anyway is set; a record another member has confirmed a copy of is forgotten without it.
	Forget(ctx context.Context, database string, slot int, anyway bool) error
}

// SeedNotHeld is the refusal of Retry for a slot that is not seeding or stalled. state is the
// slot's own word: "absent" when this node has no record, otherwise the SeedState (conventions
// Errors, SEED_NOT_HELD, exit 2).
func SeedNotHeld(database string, slot int, state string) error {
	return &cluster.Error{Code: cluster.CodeSeedNotHeld, Message: cluster.CodeSeedNotHeld.Text(cluster.CondSeedNotHeld, map[string]string{
		"slot": strconv.Itoa(slot), "db": database, "state": state,
	})}
}

// SeedNothingToForget is the refusal of Forget for a slot with no record (conventions Errors,
// SEED_NOT_HELD with the forget text, exit 2).
func SeedNothingToForget(database string, slot int) error {
	return &cluster.Error{Code: cluster.CodeSeedNotHeld, Message: cluster.CodeSeedNotHeld.Text(cluster.CondSeedNotHeldForget, map[string]string{
		"slot": strconv.Itoa(slot), "db": database,
	})}
}

// SeedHeld is the refusal of Forget for a record that is not retired (conventions Errors,
// SEED_HELD, exit 8): the node holds that copy, or is deleting it, and forgetting it would delete
// a copy in use or race the delete.
func SeedHeld(database string, slot int, state SeedState) error {
	return &cluster.Error{Code: cluster.CodeSeedHeld, Message: cluster.CodeSeedHeld.Text(cluster.CondSeedHeld, map[string]string{
		"slot": strconv.Itoa(slot), "db": database, "state": string(state),
	})}
}

// SeedOnlyCopy is the refusal of Forget for a retired record that no other member attests a
// copy of (BUG-0795): this node may hold the only copy, and the next seed of the slot would
// delete the kept range. It is SEED_HELD, exit 8, and names the anyway form.
func SeedOnlyCopy(database string, slot int) error {
	return &cluster.Error{Code: cluster.CodeSeedHeld, Message: cluster.CodeSeedHeld.Text(cluster.CondSeedHeldOnlyCopy, map[string]string{
		"slot": strconv.Itoa(slot), "db": database,
	})}
}

// noSeeding is the view of a node with no seeding engine: nothing is in flight, so there are
// no rows and nothing to retry or forget.
type noSeeding struct{}

// NoSeeding returns the view a Node answers with until SetSeedingView gives it another.
func NoSeeding() SeedingView { return noSeeding{} }

func (noSeeding) Status() []SeedRow { return []SeedRow{} }

func (noSeeding) Retry(_ context.Context, database string, slot int) error {
	return SeedNotHeld(database, slot, "absent")
}

func (noSeeding) Forget(_ context.Context, database string, slot int, _ bool) error {
	return SeedNothingToForget(database, slot)
}

// seedingHolder is the node's view slot. The zero value holds nothing and reads as NoSeeding.
type seedingHolder struct{ view atomic.Pointer[SeedingView] }

func (h *seedingHolder) load() SeedingView {
	if view := h.view.Load(); view != nil {
		return *view
	}
	return NoSeeding()
}

// SetSeedingView makes view the node's seeding view; the seeding engine calls it once when it
// starts. A nil view puts NoSeeding back.
func (n *Node) SetSeedingView(view SeedingView) {
	if view == nil {
		n.seeding.view.Store(nil)
		return
	}
	n.seeding.view.Store(&view)
}

// SeedingView returns the node's seeding view, which is NoSeeding until one is set. It is
// never nil.
func (n *Node) SeedingView() SeedingView { return n.seeding.load() }
