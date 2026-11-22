package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Bucket migration (item 1d / snapshot transfer). Design record:
// the snapshot-transfer-design record. opcodes 0x93–0x95 already exist; these
// handlers are their missing server side.
//
// Two cases share this path, and they differ in what makes the snapshot
// safe:
//
//   - The owner moves (a join re-keyed the ring and the new owner pulls from
//     an old holder). The bucket is frozen by the join fence — its ring owner
//     has no local copy and refuses writes — so the source's data is a stable
//     point-in-time state and any holder is a valid source.
//   - The owner stays and a non-owner replica is added (a join re-keyed the
//     ring so an existing bucket gained the newcomer, or the bucket's replica
//     set grew). Writes keep flowing on the owner throughout. The snapshot is
//     then taken under the bucket's apply fence, at an exact sequence of the
//     owner's stream (MigrateStartResponse.HighWater), and that sequence
//     becomes the new replica's position on both sides: the replica skips
//     the bucket's entries at or below it (its snapshot holds them)
//     and the owner ships and catches up from there. Only the
//     owner can be the source in this case — the position is in its stream.

const (
	// snapshotChunkSize caps one MIGRATE_DATA frame.
	snapshotChunkSize = 8 << 20
)

var (
	// ErrMigrateBucketNotFound is returned when the source does not hold the
	// bucket being migrated.
	ErrMigrateBucketNotFound = errors.New("this node does not hold the bucket to migrate")
	errMigrateNoInProgress   = errors.New("no migration in progress for this bucket")
	errMigrateDigestMismatch = errors.New("migrated data failed its digest on the source")
	// errSnapshotPending refuses a replicated CREATE_BUCKET while a snapshot
	// pull of that bucket is landing on this node (refuseUnservable), once the
	// stream push has waited for the landing and it has not ended. The refusal
	// fails the push's frame; the sender stalls and re-sends the frame from its
	// log, and the entry is then skipped as held (BUG-0729).
	errSnapshotPending = errors.New("a snapshot of this bucket is being pulled onto this node; " +
		"its replayed history is refused until the pull lands and is then skipped as already held")
	// errMigrateEmptyKey refuses a key record whose k is empty. The
	// v1 stream has no decoder-level check between the wire and migratedValue,
	// so this guard is the only thing standing between a damaged frame (or a
	// hostile, already-token-authenticated peer) and an index-out-of-range
	// panic on record.Key[0].
	errMigrateEmptyKey = errors.New("damaged migration stream: a key record has an empty key")
	// errPulledCopyRefused refuses a pulled copy that failed this node's
	// checks of what it holds (checkPulledCopy), before anything of it was
	// written or any copy it replaces was released.
	errPulledCopyRefused = errors.New("this node refused the copy it was sent, and wrote nothing of it")
)

// pullPending reports whether a snapshot pull of the bucket is in flight on
// this node — the marker ensureMigrated keeps for the pull's duration.
func (c *Cluster) pullPending(database, bucket string) bool {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	_, busy := c.migrateActive[migrateKey(database, bucket)]
	return busy
}

// pullRunning reports whether a pull of the bucket is landing on this node now.
func (c *Cluster) pullRunning(database, bucket string) bool {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	return len(c.pulls[migrateKey(database, bucket)]) > 0
}

// pendingSnapshot is a bucket's data captured on the source for one migration
// pull, held in memory between MIGRATE_START and MIGRATE_DONE.
type pendingSnapshot struct {
	storeType BucketType
	indices   []catalogIndex
	stores    *storeFieldSet
	// fields and fieldTypes are the bucket's typed-field declaration as its
	// catalog records it, unread fields included.
	fields     []catalogField
	fieldTypes string
	// replicationFactor is the factor the bucket declared on the source, or 0.
	replicationFactor int32
	// placement is the source's placement label, "" for a non-hash bucket.
	placement string
	// part and partOf are a part's catalog fields, 0 for any other bucket.
	part   int32
	partOf uint64
	// generation is the incarnation of its database the bucket was created in,
	// and droppedGenerations the database's incarnations this node knows to
	// be dropped, both sent with the start response.
	generation         int64
	droppedGenerations []int64
	highWater          uint64
	// incarnation is the incarnation of this node's log highWater is a
	// sequence of (MigrateStartResponse.Incarnation).
	incarnation [16]byte
	digest      [sha256.Size]byte
	chunks      [][]byte
	// target is the pulling node's identity (MigrateStartRequest.FromNode),
	// kept so the verified pull can adopt highWater as that peer's position
	// on the bucket (handleMigrateDone). Empty when the target did not say.
	target string
	// transfer is the id this node issued when it accepted the pull
	// (MigrateStartResponse.Transfer). The pull's later requests must carry it:
	// every check a pull gets is made at MIGRATE_START, so the snapshot is
	// served and verified only for the request that passed them.
	transfer string
	// idle expires the snapshot when its puller stops asking for it; every
	// request of the pull resets it. Set by setPending, under migrateMu.
	idle *time.Timer
}

// pendingSnapshotIdle is how long a snapshot is kept for a puller that has
// asked for nothing since. A puller materialises the bucket between its last
// chunk and its MIGRATE_DONE, which for a bucket of tens of GB takes minutes,
// and a snapshot freed meanwhile makes a landed copy report a failed pull; an
// hour is far past that and still frees what a puller that went away left.
// Nothing else does. A var so a test can shorten it.
var pendingSnapshotIdle = time.Hour

// errPullDiscardedByDrop is a pull whose bucket was dropped on this node
// while its stream was in flight: the stream is discarded, not attached.
var errPullDiscardedByDrop = errors.New("the bucket was dropped on this node while it was being pulled; the pulled copy is discarded")

// errPullDatabaseDropped is a pull whose database was dropped on this node
// while the pull was in flight: the pulled copy is discarded, not
// attached, so the drop is not undone.
var errPullDatabaseDropped = errors.New("the database was dropped on this node while this bucket was being pulled; " +
	"the pulled copy is discarded and its space reclaimed in the background")

// pullRecord is one pull this node is running as the target. cancelled is
// guarded by Cluster.migrateMu.
type pullRecord struct {
	cancelled bool
	// replacing marks a pull that replaces a copy this node holds
	// (pullPlan.replaces), from before it asks the source until its
	// materialise ends: the bucket's replicated entries are refused meanwhile
	// (copyBeingReplaced). releasing is set while the pull itself releases
	// that copy, so the release's own cancel by name passes it over.
	replacing, releasing bool
	// generation is the incarnation of its database the pulled bucket was
	// created in, from the source's start response; 0 until then. A drop
	// cancels only the pulls of the generations it names, and every pull
	// whose generation is not known yet (SA-DG-12).
	generation int64
}

// setPullGeneration records the generation a pull is landing.
func (c *Cluster) setPullGeneration(record *pullRecord, generation int64) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	record.generation = generation
}

// beginPull registers a pull of the bucket.
func (c *Cluster) beginPull(database, bucket string) *pullRecord {
	record := &pullRecord{}
	key := migrateKey(database, bucket)
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	if c.pulls[key] == nil {
		c.pulls[key] = make(map[*pullRecord]struct{})
	}
	c.pulls[key][record] = struct{}{}
	return record
}

// endPull retires a pull's record.
func (c *Cluster) endPull(database, bucket string, record *pullRecord) {
	key := migrateKey(database, bucket)
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	c.endReplacingLocked(record)
	delete(c.pulls[key], record)
	if len(c.pulls[key]) == 0 {
		delete(c.pulls, key)
	}
}

// beginReplacing marks a pull as replacing a copy this node holds. The
// caller holds the bucket's fence exclusively, so an entry being applied to
// the copy has finished and every later one is refused (refuseUnservable).
func (c *Cluster) beginReplacing(record *pullRecord) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	if !record.replacing {
		record.replacing = true
		c.replacingPulls.Add(1)
	}
}

// endReplacing clears a pull's replacing mark, once the copy it replaces is
// released or the pull ends.
func (c *Cluster) endReplacing(record *pullRecord) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	c.endReplacingLocked(record)
}

func (c *Cluster) endReplacingLocked(record *pullRecord) {
	if record.replacing {
		record.replacing = false
		c.replacingPulls.Add(-1)
	}
}

// copyBeingReplaced reports whether a pull running on this node replaces its
// copy of the bucket. Until it releases the copy, the copy keeps serving
// reads, and the bucket's replicated entries are refused as for a bucket this
// node does not serve: an entry acknowledged into the copy after the source
// took its snapshot would be lost when the copy is replaced, and refused, it
// is re-sent and lands on whichever copy is served then. One atomic load
// while no such pull runs.
func (c *Cluster) copyBeingReplaced(database, bucket string) bool {
	if c.replacingPulls.Load() == 0 {
		return false
	}
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	for record := range c.pulls[migrateKey(database, bucket)] {
		if record.replacing {
			return true
		}
	}
	return false
}

// beginLanding marks a pull of the bucket as landing: under the bucket's
// exclusive fence, before it records its snapshot position, and until its copy
// is in place. Until the returned end runs, the stream's create of the bucket
// is refused on this node (refuseUnservable) and a stream push waits for the
// landing before it judges an entry of the bucket (awaitLanding). Only this
// window needs the refusal: before the position is recorded a create the stream
// applies makes the pull stop as already served (pullBucketWith), and after the
// copy is in place every entry the snapshot covers is skipped as held. end is
// idempotent; the caller runs it once the copy is in place or its position
// forgotten. One channel and one map entry per landing pull, for its length;
// nothing per bucket at rest (BUG-0729).
func (c *Cluster) beginLanding(database, bucket string) (end func()) {
	key := migrateKey(database, bucket)
	landed := make(chan struct{})
	c.migrateMu.Lock()
	if c.landing[key] == nil {
		c.landing[key] = make(map[chan struct{}]struct{})
	}
	c.landing[key][landed] = struct{}{}
	c.landingPulls.Add(1)
	c.migrateMu.Unlock()
	return sync.OnceFunc(func() {
		c.migrateMu.Lock()
		defer c.migrateMu.Unlock()
		delete(c.landing[key], landed)
		if len(c.landing[key]) == 0 {
			delete(c.landing, key)
		}
		c.landingPulls.Add(-1)
		close(landed)
	})
}

// pullLanding returns the channel of a pull of the bucket that is landing on
// this node (beginLanding), closed when it has, or nil when none is. One atomic
// load while no pull lands.
func (c *Cluster) pullLanding(database, bucket string) <-chan struct{} {
	if c.landingPulls.Load() == 0 {
		return nil
	}
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	for landed := range c.landing[migrateKey(database, bucket)] {
		return landed
	}
	return nil
}

// landingWait bounds how long a stream push waits for a landing pull of an
// entry's bucket: half the sender's bound on the ship (replShipTimeout), so
// the rest of the frame is still answered within it.
func (c *Cluster) landingWait() time.Duration {
	return c.replShipTimeout() / 2
}

// awaitLanding waits for a pull of e's bucket that is landing on this node, so
// the entry is judged once the copy is in place: an entry its snapshot covers
// is then skipped as held, and the create is not refused. Answered before,
// either refusal failed the whole frame and took the stream out of the in-sync
// set for every bucket it carries (BUG-0729). The wait ends at until, or at
// landingWait from now when until is zero, and the deadline in force is
// returned: the caller passes it back when it judges the entry again, so both
// waits share one bound. A landing that outlasts it is refused as before and
// re-sent. The caller holds no fence: a landing takes its bucket's.
func (c *Cluster) awaitLanding(ctx context.Context, e ReplEntry, until time.Time) time.Time {
	if c.landingPulls.Load() == 0 {
		return until
	}
	_, database := decodeFrameMeta(e.Payload).scope(transport.Opcode(e.Op))
	landed := c.pullLanding(database, e.Bucket)
	if landed == nil {
		return until
	}
	if until.IsZero() {
		until = time.Now().Add(c.landingWait())
	}
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-landed:
	case <-timer.C:
	case <-ctx.Done():
	}
	return until
}

// dropClearsPulls is the drop's half of spec §5.3's ordering: under the
// drop's exclusive hold it cancels every pull of the name running on this
// node, and forgets any snapshot of it this node is serving as a source.
func (c *Cluster) dropClearsPulls(database, bucket string) {
	key := migrateKey(database, bucket)
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	c.cancelPullsLocked(key)
	c.forgetPendingLocked(key)
}

// cancelPullsOf cancels every pull of the name running on this node, and
// nothing a source serves: the half of dropClearsPulls an alter that
// un-places this node needs (bucket-replication-rev8-spec §5.3 row 5; G3 build
// doc OQ-G3-8). It runs under the alter's exclusive fence. O(pulls of the
// name), under migrateMu.
func (c *Cluster) cancelPullsOf(database, bucket string) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	c.cancelPullsLocked(migrateKey(database, bucket))
}

// cancelPullsLocked marks every pull of key cancelled and forgets them. The
// caller holds migrateMu.
func (c *Cluster) cancelPullsLocked(key string) {
	records := c.pulls[key]
	for record := range records {
		// The drop is the pull's own release of the copy it replaces
		// (releaseReplacedCopyLocked). Every cancel by name runs under the
		// bucket's exclusive fence, which that release holds, so no other
		// drop is passed over.
		if record.releasing {
			continue
		}
		record.cancelled = true
		delete(records, record)
	}
	if len(records) == 0 {
		delete(c.pulls, key)
	}
}

// dropDatabaseClearsPulls is dropClearsPulls for a database's drop:
// it cancels every pull of a bucket of the database running on this node whose
// generation the drop names — or is not known yet — and forgets every snapshot
// of one this node is serving as a source. A nil generations is a drop logged
// before generations existed, which cancels every pull of the database. It
// costs O(pulls and snapshots in flight), never O(buckets). A drop calls it
// under the allocator's mutex, right after it commits; a materialise holds that
// mutex from its last cancellation check to its catalog commit, so a pull
// either committed before the drop — and the drop covers it — or sees the
// cancel. The database drop runs in no bucket's fence and a materialise in
// its bucket's, so no fence gives that exclusion.
func (c *Cluster) dropDatabaseClearsPulls(database string, generations []int64) {
	prefix := migrateKey(database, "")
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	for key, records := range c.pulls {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for record := range records {
			if generations == nil || record.generation == 0 || slices.Contains(generations, record.generation) {
				record.cancelled = true
				delete(records, record)
			}
		}
		if len(records) == 0 {
			delete(c.pulls, key)
		}
	}
	for key := range c.migratePending {
		if strings.HasPrefix(key, prefix) {
			c.forgetPendingLocked(key)
		}
	}
}

func (c *Cluster) pullCancelled(record *pullRecord) bool {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	return record.cancelled
}

// beforeMaterializeHook, when set, runs on the target between receiving a
// pull's stream and materialising it. Tests use it to hold a pull there. Nil
// in production.
var beforeMaterializeHook atomic.Pointer[func(database, bucket string)]

// afterReplicaJudgedHook, when set, runs in handleReplPush after a pushed
// entry has been judged against a landing pull and the snapshot position
// (R-4) and before it is applied, with the entry's bucket. Tests hold it to
// build the interleaving where a pull begins landing in between (BUG-0729).
// Nil in production.
var afterReplicaJudgedHook atomic.Pointer[func(database, bucket string)]

// materializeCatalogHook, when set, runs once a migrated range's keys are
// written and before its catalog batch commits, with the reserved id; an error
// ends the materialise there, as a crash would. Nil in production.
var materializeCatalogHook atomic.Pointer[func(id uint64) error]

// waitForInFlightPullHook, when set, runs in ensureMigrated when a caller finds
// a pull of the bucket in flight and is about to wait for it. Tests use it to
// know every caller is waiting. Nil in production.
var waitForInFlightPullHook atomic.Pointer[func(database, bucket string)]

// beforeMigrateSnapshotHook, when set, runs on the source in
// handleMigrateStart once every check of the pull has passed and immediately
// before the snapshot is taken: a test holds a pull between the placement
// check, made outside the bucket's fence, and the snapshot, made inside it
// (T-34 case 5). Nil in production.
var beforeMigrateSnapshotHook atomic.Pointer[func(database, bucket string)]

// afterSnapshotFenceReleasedHook, when set, runs in snapshotBucket once the
// bucket's exclusive fence is released and before the snapshot is returned:
// T-172 holds the snapshot there until the writes queued on the fence have
// been routed, so a clear of the seeded member's record placed anywhere after
// the fence runs after them (BUG-0594). Nil in production.
var afterSnapshotFenceReleasedHook atomic.Pointer[func(database, bucket string)]

// replacingPullCheckedHook, when set, runs on the target of a pull that
// replaces a copy it holds, once the pull has fetched and checked the copy and
// before it releases the one it holds. Tests write through it while the held
// copy is still served. Nil in production.
var replacingPullCheckedHook atomic.Pointer[func(database, bucket string)]

// pullStartedHook, when set, runs as a pull of a bucket starts on the target,
// once it is in the pulls registry. Tests use it to see which pulls of one
// bucket run at once. Nil in production.
var pullStartedHook atomic.Pointer[func(database, bucket string)]

func migrateKey(database, bucket string) string { return database + "\x00" + bucket }

// MigrateStartRequest is the target's request to begin pulling a bucket.
type MigrateStartRequest struct {
	Token    string `bson:"token"`
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	// FromNode is the pulling node's identity, so that once the pull is
	// verified the source can adopt the snapshot's high-water as that peer's
	// position on the bucket and ship to it from there. Optional
	// and additive, like GossipResponse.Self: an older target omits it and
	// the source then adopts nothing, which is the pre-fix behaviour.
	FromNode string `bson:"from_node,omitempty"`
	// Epoch is the membership epoch the puller is on. A source on another one
	// refuses the pull as "not yet" (spec §19.3.7): the two place the bucket
	// differently.
	Epoch uint64 `bson:"epoch,omitempty"`
	// Store is the pulling node's own word on whether its store is encrypted
	// at rest (Peer.Store). A sealed source hands a bucket only to a puller
	// that says sealed; the claim rides the request so that it is the word of
	// the process pulling now, not a record of an earlier one.
	// Additive: a puller from before the field says nothing and is refused by
	// a sealed source.
	Store storeClaim `bson:"store,omitempty"`
	// DroppedGenerations are the incarnations of the database the puller
	// knows to be dropped; the source applies the ones it lacks before it
	// answers (G-11 (b)). Internode only.
	DroppedGenerations []int64 `bson:"dropped_generations,omitempty"`
	// Incarnation is the incarnation of the source's log the puller has
	// recorded for the source. The source serves the pull only when it is the
	// incarnation of its own process: the stream has then opened, and the
	// snapshot's position is in the log the puller holds a position in (A-4.2).
	// Internode only.
	Incarnation []byte `bson:"incarnation,omitempty"`
	// Handoff names the pull a new owner's take-over (step 10b.5): any holder
	// serves it, a removed node included. A pull without it is served only by
	// a settled owner, so a replica is always seeded from its owner's term.
	// Internode only.
	Handoff bool `bson:"handoff,omitempty"`
	// Operator names a pull made for an operator's `migrate` verb (not a
	// hand-off, a take-over or a sweep). A source that holds the bucket as a
	// spread template refuses it: the collection moves part by part
	// (migrateOfSpreadRefusal), and the puller cannot tell a template from
	// any other bucket before it holds one. Internode only.
	Operator bool `bson:"operator,omitempty"`
}

// MigrateStartResponse acknowledges the pull and names the snapshot layout.
type MigrateStartResponse struct {
	Ok        bool       `bson:"ok"`
	StoreType BucketType `bson:"store_type"`
	NumChunks int        `bson:"num_chunks"`
	// HighWater is the sequence of the source's replication stream the
	// snapshot was taken at, under the bucket's apply fence: every entry of
	// the bucket at or below it is in the snapshot and none above it is. The
	// target records it as the bucket's position for that source, so replay
	// at or below it is a no-op; when the source is the bucket's
	// owner it adopts the same position for the target.
	HighWater uint64 `bson:"high_water"`
	// Indices is the bucket's declared secondary indexes, as the source's
	// catalog records them. The range stream carries the index keys but not
	// the declaration, and without it the target attaches no index manager
	// and its search answers that none is defined. Additive: an
	// older source omits it and the target then records no indexes, which is
	// the pre-fix behaviour; an older target ignores it.
	Indices []catalogIndex `bson:"indices,omitempty"`
	// Stores and NextStoreID are the bucket's declared store fields, as the
	// source's catalog records them (nested buckets, GAP-22). The range stream
	// carries the 0x07 keys but not the declaration, and without it the target
	// serves a bucket whose field stores are invisible: every field verb is
	// refused as undeclared. The ids travel with the keys, which carry them,
	// so the target adopts them and never renumbers. Additive, as Indices is.
	Stores      []catalogStore `bson:"stores,omitempty"`
	NextStoreID int32          `bson:"next_store_id,omitempty"`
	// Fields and FieldTypes are the bucket's typed-field declaration, as the
	// source's catalog records it (typed-fields spec §8.4). A field the source
	// cannot read travels as the bytes it recorded. Additive, as Indices is;
	// no sequence travels with it (spec §5.1).
	Fields     []catalogField `bson:"fields,omitempty"`
	FieldTypes string         `bson:"field_types,omitempty"`
	// ReplicationFactor is the factor the bucket declared, as the source's
	// catalog records it, or absent for a bucket that inherits. The range stream
	// does not carry it, and a target that never held the bucket cannot know it:
	// it learns it here, before deciding whether it belongs. Internode only;
	// additive, and never part of a driver vector.
	ReplicationFactor int32 `bson:"replication_factor,omitempty"`
	// Placement is the source's recorded placement of a hash bucket, "spread"
	// or "whole"; absent for any other type.
	Placement string `bson:"placement,omitempty"`
	// Part and PartOf are a part's catalog fields, absent for any other
	// bucket: a moved part must stay a part, or the next start would read it
	// as a pre-spread bucket.
	Part   int32  `bson:"part,omitempty"`
	PartOf uint64 `bson:"part_of,omitempty"`
	// NotPlaced marks a refusal that is final: the puller is outside the bucket's
	// set on the view of a source at least as new as the puller's own. A refusal
	// without it (a source behind the puller's epoch, a snapshot that failed) is
	// only "not now", and the sweep asks again.
	NotPlaced bool `bson:"not_placed,omitempty"`
	// RefusedForGood marks a refusal that waiting cannot clear: the source's
	// stream to the puller is refused by the handshake until an operator acts
	// (errHandshakeRefusedForGood). Internode only, additive: an older source
	// omits it and the puller waits as it did.
	RefusedForGood bool   `bson:"refused_for_good,omitempty"`
	Error          string `bson:"error,omitempty"`
	// Transfer identifies the pull this source accepted. The puller sends it
	// back on every MIGRATE_DATA and on its MIGRATE_DONE, and the source answers
	// those only for the pull it names. Internode only, never a driver vector.
	// An older source does not send it, and the puller then sends none back.
	Transfer string `bson:"transfer,omitempty"`
	// DatabaseGeneration is the incarnation the bucket was created in, and
	// DroppedGenerations the incarnations of its database the source knows to
	// be dropped: the puller applies those first, and discards a copy of a
	// dropped incarnation (G-7, G-11 (b)). Internode only; additive, and never
	// part of a driver vector.
	DatabaseGeneration int64   `bson:"db_gen,omitempty"`
	DroppedGenerations []int64 `bson:"dropped_generations,omitempty"`
	// Incarnation is the incarnation of the source's log HighWater is a
	// sequence of. The puller records the snapshot position only under the
	// incarnation it holds for the source, so a position is never assumed to
	// belong to a log it was not taken from (A-4.2). Internode only.
	Incarnation []byte `bson:"incarnation,omitempty"`
}

// MigrateDataRequest asks for one chunk of the snapshot. The bucket is carried
// so the source keys the pull unambiguously (migrations are serialized, but a
// second pull may start before the first finishes if a target retries).
type MigrateDataRequest struct {
	Token    string `bson:"token"`
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Index    int    `bson:"index"`
	// Transfer is MigrateStartResponse.Transfer of the pull this request is
	// part of. A source refuses a request without it, and names the upgrade.
	Transfer string `bson:"transfer,omitempty"`
}

// MigrateDataResponse is one chunk of snapshot bytes.
type MigrateDataResponse struct {
	Ok    bool   `bson:"ok"`
	Data  []byte `bson:"data"`
	Error string `bson:"error,omitempty"`
}

// MigrateDoneRequest delivers the target's digest of the received bytes.
type MigrateDoneRequest struct {
	Token    string `bson:"token"`
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Digest   string `bson:"digest"`
	// Transfer is MigrateStartResponse.Transfer, as on MigrateDataRequest.
	Transfer string `bson:"transfer,omitempty"`
}

// MigrateDoneResponse confirms the move.
type MigrateDoneResponse struct {
	Ok    bool   `bson:"ok"`
	Error string `bson:"error,omitempty"`
}

// snapshotBucket captures a consistent point-in-time snapshot of one bucket,
// with the sequence of this node's stream it was taken at.
//
// The bucket's apply fence is held exclusively for the whole capture. That
// drains every in-flight append-and-apply of the bucket and blocks new ones
// (they wait, they are not refused), so the log's last sequence read here is
// one every appended entry of the bucket has been applied at, and the backup
// contains exactly those entries. That does not hold without the fence: on
// the owner the sequence is assigned before the apply, so an entry can be in
// the log and not yet in the store. The range itself is read from one
// snapshot. The cost is that writes to this one bucket wait for the capture to
// finish; reads are untouched.
//
// puller is the member the snapshot seeds (MigrateStartRequest.FromNode), ""
// when the caller names none. Its awaiting record, if this node holds one, is
// cleared under the same fence (spec §7.3): an entry logged before the
// snapshot is in it, and one logged after reads the map after the delete and
// is routed to the member. A snapshot that names no puller clears nothing.
func (c *Cluster) snapshotBucket(database, bucket, puller string) (*pendingSnapshot, error) {
	fence := c.s.acquireFence(placementOf(database, bucket), fenceExclusive)
	defer func() {
		fence.release()
		if hook := afterSnapshotFenceReleasedHook.Load(); hook != nil {
			(*hook)(database, bucket)
		}
	}()

	data := c.s.serverDataAccessor()
	if data == nil {
		return nil, ErrMigrateBucketNotFound
	}
	if data.getDB(database) == nil {
		return nil, fmt.Errorf("%w: database %q not found", ErrMigrateBucketNotFound, database)
	}
	facts, held := c.s.peekSettled(database, bucket)
	if !held {
		return nil, fmt.Errorf("%w: bucket %q not found in database %q", ErrMigrateBucketNotFound, bucket, database)
	}
	// The declaration is read from the facts after the bucket's lock is
	// released: the exclusive fence taken above keeps every declare and every
	// replicated apply of this bucket out, so the three lists are as one hold
	// would have read them.
	indices := recordIndexSpecs(facts.indexes())
	stores := facts.storeFields()
	fields, fieldTypes := facts.fieldTypes().catalogForm()
	id := facts.id
	part, partOf, err := c.s.partFieldsOf(database, bucket)
	if err != nil {
		return nil, fmt.Errorf("snapshot bucket %q: %w", bucket, err)
	}
	// An entry with no id is a bucket this node is itself still pulling: it
	// holds nothing to serve yet (build doc SA-14).
	if id == 0 {
		return nil, fmt.Errorf("%w: bucket %q in database %q is not attached on this node yet", ErrMigrateBucketNotFound, bucket, database)
	}
	if err := c.clearSeededAwaiting(placementOf(database, bucket), puller); err != nil {
		return nil, fmt.Errorf("snapshot bucket %q: %w", bucket, err)
	}

	// The log's tail, not the bucket's last entry: entries of other buckets
	// between the two are not the target's concern for this bucket, and a
	// position at the tail is never below the retained horizon, which the
	// bucket's own last entry can be once retention has pruned it.
	var highWater uint64
	if c.s.wal != nil {
		highWater = c.s.wal.LastSequence()
	}

	// Every engine is a range on the shared store — a queue's cursors are in
	// its state document — and a blob bucket's large payloads follow its keys
	// in the stream, so every type migrates (spec §5.3).
	var buf bytes.Buffer
	if err := c.s.streamBucketRange(&buf, c.s.kv, facts.slot, facts.databaseID, id); err != nil {
		return nil, fmt.Errorf("snapshot bucket %q: %w", bucket, err)
	}
	bs := buf.Bytes()
	digest := sha256.Sum256(bs)
	return &pendingSnapshot{
		storeType: facts.bucketType,
		indices:   indices,
		stores:    stores,
		highWater: highWater,
		digest:    digest,
		chunks:    chunkBytes(bs, snapshotChunkSize),

		fields:     fields,
		fieldTypes: fieldTypes,

		replicationFactor: facts.replicationFactor,
		placement:         placementLabel(facts.bucketType, facts.spread),
		part:              part,
		partOf:            partOf,
		generation:        facts.generation,
	}, nil
}

// setPending / pendingByTransfer / clearPending manage in-flight snapshots. A
// bucket has one per puller, not one: two members pulling it at once (a factor
// raised from one to three) each read their own, and one finishing does not
// drop the other's. A puller that pulls again replaces its own earlier
// snapshot, so the entries of a bucket are bounded by its pullers, which its
// placement bounds by the replication factor. Only pulls in flight are held:
// nothing here is kept per bucket at rest.
func (c *Cluster) setPending(key string, ps *pendingSnapshot) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	pullers := c.migratePending[key]
	if pullers == nil {
		pullers = make(map[string]*pendingSnapshot)
		c.migratePending[key] = pullers
	}
	if replaced := pullers[ps.target]; replaced != nil {
		replaced.idle.Stop()
	}
	pullers[ps.target] = ps
	ps.idle = time.AfterFunc(pendingSnapshotIdle, func() { c.expirePending(key, ps) })
}

// expirePending frees a snapshot nobody has asked for within
// pendingSnapshotIdle, unless the puller has replaced it since.
func (c *Cluster) expirePending(key string, ps *pendingSnapshot) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	if c.migratePending[key][ps.target] != ps {
		return
	}
	delete(c.migratePending[key], ps.target)
	if len(c.migratePending[key]) == 0 {
		delete(c.migratePending, key)
	}
}

// pendingByTransfer returns the snapshot a pull's transfer id names and
// restarts its idle timer, or nil with the pullers the bucket's other
// snapshots are held for, for the refusal to name.
func (c *Cluster) pendingByTransfer(key, transfer string) (found *pendingSnapshot, heldFor []string) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	for puller, ps := range c.migratePending[key] {
		if subtle.ConstantTimeCompare([]byte(transfer), []byte(ps.transfer)) == 1 {
			ps.idle.Reset(pendingSnapshotIdle)
			return ps, nil
		}
		heldFor = append(heldFor, puller)
	}
	slices.Sort(heldFor)
	return nil, heldFor
}

// pendingCount is the number of snapshots held for the bucket.
func (c *Cluster) pendingCount(key string) int {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	return len(c.migratePending[key])
}

// clearPending drops the snapshot held for one puller of the bucket.
func (c *Cluster) clearPending(key, puller string) {
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	if ps := c.migratePending[key][puller]; ps != nil {
		ps.idle.Stop()
	}
	delete(c.migratePending[key], puller)
	if len(c.migratePending[key]) == 0 {
		delete(c.migratePending, key)
	}
}

// forgetPendingLocked drops every snapshot held for the bucket, as its drop
// does. The caller holds migrateMu.
func (c *Cluster) forgetPendingLocked(key string) {
	for _, ps := range c.migratePending[key] {
		ps.idle.Stop()
	}
	delete(c.migratePending, key)
}

// handleMigrateStart serves the target's pull-open on the source.
func handleMigrateStart(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req MigrateStartRequest
	noteRequestDecode("handleMigrateStart", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: fmt.Sprintf("decode: %v", err)}, nil)
	}
	c := s.cluster
	if c == nil || !c.ValidateToken(req.Token) {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: "invalid cluster token"}, nil)
	}
	if err := c.refuseSenderNotOfConnection(ctx, transport.OpMigrateStart, req.FromNode); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: err.Error()}, nil)
	}
	// "Not held" is answered before any refusal: a puller asks each member in
	// turn and moves on from a node that lacks the bucket, and a node that
	// refused a bucket it does not have would end that search one member
	// early. Every answer here comes before the snapshot, which takes the
	// bucket's exclusive fence and stalls its writes. The plaintext
	// refusal comes before the placement refusal because the placement refusal
	// names the bucket's replica set, which a puller this node will not hand
	// data to has no business learning.
	// The puller's dropped generations of the database are applied first
	// (G-11 (b)): one of them may be the incarnation of the copy asked for,
	// which is then dropped here and answered not held.
	if err := s.learnTombstones(ctx, req.Database, req.DroppedGenerations); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: fmt.Sprintf(
			"applying the dropped generations of database %q that the puller sent: %v", req.Database, err)}, nil)
	}
	if !s.bucketStoreOpen(req.Database, req.Bucket) {
		notHeld := fmt.Errorf("%w: bucket %q in database %q", ErrMigrateBucketNotFound, req.Bucket, req.Database)
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: notHeld.Error()}, nil)
	}
	if req.Operator && s.spreadTemplateHeld(req.Database, req.Bucket) {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: migrateOfSpreadRefusal(req.Database, req.Bucket).Error()}, nil)
	}
	if err := c.refuseSnapshotForPlaintextTarget(req); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: err.Error()}, nil)
	}
	// A puller on another epoch places the bucket differently from this node:
	// a snapshot handed over now is positioned in a stream this node does not
	// route to it, and the writes in between never reach it (spec §19.3.7, W1).
	if err := c.refuseAPullAcrossEpochs(req); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: err.Error()}, nil)
	}
	// The node that holds the bucket knows its factor, so it decides whether the
	// puller belongs, before it builds a snapshot it would throw away. A puller
	// that never held the bucket computes the set at its own default and cannot.
	if err := c.refuseAPullerOutsideTheSet(req); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{
			Ok: false, Error: err.Error(), NotPlaced: errors.Is(err, errPullerNotPlaced),
		}, nil)
	}
	if err := c.refuseAPullerNotYetStreamedTo(req.FromNode, req.Incarnation); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{
			Ok: false, Error: err.Error(), RefusedForGood: errors.Is(err, errHandshakeRefusedForGood),
		}, nil)
	}
	if err := c.refuseAnUnsettledSource(req); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: err.Error()}, nil)
	}
	if hook := beforeMigrateSnapshotHook.Load(); hook != nil {
		(*hook)(req.Database, req.Bucket)
	}
	ps, err := c.snapshotBucket(req.Database, req.Bucket, req.FromNode)
	if err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: err.Error()}, nil)
	}
	ps.incarnation = c.s.wal.Incarnation()
	ps.target = req.FromNode
	ps.transfer = rand.Text()
	if ps.droppedGenerations, err = s.droppedGenerations(req.Database); err != nil {
		return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{Ok: false, Error: err.Error()}, nil)
	}
	c.setPending(migrateKey(req.Database, req.Bucket), ps)
	c.logger.Info("migration snapshot prepared", "database", req.Database, "bucket", req.Bucket,
		"chunks", len(ps.chunks), "type", string(ps.storeType), "high_water", ps.highWater, "target", req.FromNode)
	return nodeResponse(transport.OpMigrateStart, frame, MigrateStartResponse{
		Ok: true, StoreType: ps.storeType, NumChunks: len(ps.chunks), HighWater: ps.highWater, Indices: ps.indices,
		Stores: ps.stores.stores, NextStoreID: ps.stores.next, Fields: ps.fields, FieldTypes: ps.fieldTypes, ReplicationFactor: ps.replicationFactor,
		Placement: ps.placement, Part: ps.part, PartOf: ps.partOf, Transfer: ps.transfer, DatabaseGeneration: ps.generation, DroppedGenerations: ps.droppedGenerations,
		Incarnation: ps.incarnation[:],
	}, nil)
}

// errStreamNotOpenToPuller is a source's "not yet" to a pull: its stream to the
// puller has not completed a handshake in this process.
var errStreamNotOpenToPuller = errors.New("this node's replication stream to the puller has not opened in this process yet")

// refuseAPullerNotYetStreamedTo refuses a pull from a node this node's stream
// has not completed a handshake with since it started, and wakes that
// stream's sender (A-4.2). A snapshot position is a sequence of this node's
// log, and the puller records it under the incarnation it holds for this node;
// only after a handshake in this process is that the incarnation the snapshot
// is taken in. It is "not yet", never final: the puller asks again, except
// when the handshake itself is refused for good (errHandshakeRefusedForGood),
// which the refusal then says and the puller does not wait out.
//
// The puller names the incarnation it has recorded for this node, and only the
// incarnation of this process passes: a puller records one only from a
// handshake of this process's stream. A flag on this node's side could not say
// as much: a puller emptied and restarted while this node ran has recorded
// nothing, and this node's stream to it, idle, would not have noticed. Its
// stream is then opened again, so the next ask finds it recorded.
func (c *Cluster) refuseAPullerNotYetStreamedTo(puller string, recorded []byte) error {
	current := c.s.wal.Incarnation()
	if bytes.Equal(recorded, current[:]) {
		return nil
	}
	c.peersMu.RLock()
	p, known := c.peers[puller]
	c.peersMu.RUnlock()
	if !known {
		return fmt.Errorf("%w: %s is not a member this node knows yet, so this node has no stream to it; "+
			"the pull is tried again once the membership has reached this node", errStreamNotOpenToPuller, puller)
	}
	if refusal := p.handshakeRefusedForGood(); refusal != nil {
		// Not "not yet": opening the stream again meets the same answer, so the
		// puller is told what stops it and does not wait for it (BUG-0310).
		return fmt.Errorf("refusing the pull of %s: %w", puller, refusal)
	}
	p.reopenUnlessOpening("a pull from the peer names an incarnation of this node's log it has not recorded")
	return fmt.Errorf("%w: refusing the pull of %s until it has, so that the snapshot's position is in the log %s holds a position in "+
		"(the stream has not opened since this node started, or %s has not answered it yet); the pull is tried again",
		errStreamNotOpenToPuller, puller, puller, puller)
}

// pullStep names one request of a pull after MIGRATE_START, for a refusal.
type pullStep struct {
	database, bucket, transfer string
	// action is what the source would do, as the object of "refusing to".
	action string
}

// acceptedPull returns the snapshot of the pull this request belongs to, or the
// refusal. A pull is checked once, at MIGRATE_START — membership, the sealed
// store, placement — and its later requests are answered only for the pull
// that passed, which they name by the id the source issued. A request that
// names none comes from a node older than the check; it is refused and the
// operator is told to upgrade that node, as a pull that does not say whether
// its store is encrypted is. The refusal is logged and audited.
func (c *Cluster) acceptedPull(ctx context.Context, step pullStep) (*pendingSnapshot, error) {
	var ps *pendingSnapshot
	var heldFor []string
	if step.transfer != "" {
		ps, heldFor = c.pendingByTransfer(migrateKey(step.database, step.bucket), step.transfer)
	}
	var refusal error
	switch {
	case step.transfer == "":
		refusal = fmt.Errorf("refusing to %s of bucket %q in database %q: the requesting node did not name the pull this node accepted — "+
			"it runs a version from before this check. Upgrade that node to this version and let it pull again. This node keeps its copy and sent nothing",
			step.action, step.bucket, step.database)
	case ps != nil:
		return ps, nil
	case len(heldFor) == 0:
		return nil, errMigrateNoInProgress
	default:
		refusal = fmt.Errorf("refusing to %s of bucket %q in database %q: the request names a pull this node did not accept, "+
			"or one a later pull of the bucket by the same node replaced. Start the pull again. This node keeps its copy and sent nothing: %w",
			step.action, step.bucket, step.database, errMigrateNoInProgress)
	}
	c.recordPullRefusal(ctx, step, heldFor, refusal)
	return nil, refusal
}

// recordPullRefusal logs and audits a refused pull request, naming the address
// it came from and the nodes pulls of the bucket were accepted for, at most
// once per plaintextRefusalLogInterval per accepted node.
func (c *Cluster) recordPullRefusal(ctx context.Context, step pullStep, heldFor []string, refusal error) {
	acceptedFor := "(no pull in progress)"
	if len(heldFor) > 0 {
		acceptedFor = strings.Join(heldFor, ",")
	}
	if !c.sealedPeers.due(c.sealedPeers.refusalLoggedAt, "pull-request:"+acceptedFor, plaintextRefusalLogInterval) {
		return
	}
	var from string
	if sess, ok := transport.SessionFromContext(ctx); ok && sess != nil {
		from = sess.RemoteAddr
	}
	c.logger.Warn("refused a pull request that is not part of the pull this node accepted",
		"database", step.database, "bucket", step.bucket, "from_addr", from, "accepted_for", acceptedFor, "error", refusal)
	c.s.audit.Log(AuditEntry{Event: EventAuthzDenied, User: NodeUser, SourceIP: from,
		Target: "bucket:" + step.database + "." + step.bucket, Result: refusal.Error()})
}

// refuseAPullerOutsideTheSet is the source's answer to a pull from a node its
// placement of the bucket does not include, naming the set. It asks only for a
// bucket that declared a factor, and only when this node holds it: those are the
// buckets whose set the puller cannot compute for itself. A bucket that inherits
// has the same set on every node, every node's default being one, and pulling it
// somewhere the ring does not place it is what the primitive has always allowed.
func (c *Cluster) refuseAPullerOutsideTheSet(req MigrateStartRequest) error {
	if req.FromNode == "" || !c.s.bucketStoreOpen(req.Database, req.Bucket) {
		return nil
	}
	key := placementOf(req.Database, req.Bucket)
	c.viewMu.RLock()
	_, declared := c.bucketFactors[key]
	c.viewMu.RUnlock()
	if !declared {
		return nil
	}
	placed, err := c.Replicas(key)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(placed))
	for _, node := range placed {
		if node.ID == req.FromNode {
			return nil
		}
		ids = append(ids, node.ID)
	}
	// The puller is on this node's epoch (refuseAPullAcrossEpochs ran first),
	// so the two agree on the set and the verdict is final.
	return fmt.Errorf("%w: %s is not in the replica set of bucket %q in database %q, which is %v (owner %s): a copy pulled there would be one nothing ships to",
		errPullerNotPlaced, req.FromNode, req.Bucket, req.Database, ids, ids[0])
}

// errSourceNotSettled is a source's "not yet" to a pull that is not a
// take-over's: it is not the bucket's settled owner.
var errSourceNotSettled = errors.New("this node is not the bucket's settled owner")

// refuseAnUnsettledSource refuses a pull that is not a hand-off unless this
// node's copy is opened and not marked moved (step 10b.5): a replica is seeded
// only from a settled owner, so its copy follows the owner's term from the
// start. A take-over's pull is served by any holder, a removed node included.
// It is "not yet": the sweep asks again once the owner has taken over.
func (c *Cluster) refuseAnUnsettledSource(req MigrateStartRequest) error {
	if req.Handoff {
		return nil
	}
	st, err := c.s.bucketStateNamed(req.Database, req.Bucket)
	if err != nil {
		return fmt.Errorf("reading the lineage of bucket %q in database %q before serving %s's pull: %w", req.Bucket, req.Database, req.FromNode, err)
	}
	key := placementOf(req.Database, req.Bucket)
	marks := c.placementMarksOf(key)
	if c.copyIsOpened(key, st, marks) && marks.Moved == 0 {
		return nil
	}
	return fmt.Errorf("%w: refusing to seed %s with bucket %q in database %q until this node has taken it over, "+
		"or until its owner serves it; the pull is tried again", errSourceNotSettled, req.FromNode, req.Bucket, req.Database)
}

// errPullerNotPlaced is a source's final answer that the puller is outside the
// bucket's set, and errNotPlacedHere is how the puller passes that on: the pull
// is settled, not failed. A node that released a copy of a declared bucket has
// forgotten the factor and counts itself placed by the node default; the source
// that knows the factor is right, and asking again would only be refused again.
var (
	errPullerNotPlaced = errors.New("the puller is outside the bucket's replica set")
	errNotPlacedHere   = errors.New("this node is not in the bucket's replica set")
)

// handleMigrateData serves one snapshot chunk on the source.
func handleMigrateData(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req MigrateDataRequest
	noteRequestDecode("handleMigrateData", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpMigrateData, frame, MigrateDataResponse{Ok: false, Error: "decode"}, nil)
	}
	c := s.cluster
	if c == nil || !c.ValidateToken(req.Token) {
		return nodeResponse(transport.OpMigrateData, frame, MigrateDataResponse{Ok: false, Error: "invalid cluster token"}, nil)
	}
	ps, refusal := c.acceptedPull(ctx, pullStep{database: req.Database, bucket: req.Bucket, transfer: req.Transfer,
		action: "serve a chunk of the snapshot"})
	if refusal != nil {
		return nodeResponse(transport.OpMigrateData, frame, MigrateDataResponse{Ok: false, Error: refusal.Error()}, nil)
	}
	if req.Index < 0 || req.Index >= len(ps.chunks) {
		return nodeResponse(transport.OpMigrateData, frame, MigrateDataResponse{Ok: false, Error: errMigrateNoInProgress.Error()}, nil)
	}
	return nodeResponse(transport.OpMigrateData, frame, MigrateDataResponse{Ok: true, Data: ps.chunks[req.Index]}, nil)
}

// handleMigrateDone verifies the target's digest on the source and wakes the
// source's stream to the target. It deletes nothing.
func handleMigrateDone(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req MigrateDoneRequest
	noteRequestDecode("handleMigrateDone", frame.Opcode, frame.Payload)
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nodeResponse(transport.OpMigrateDone, frame, MigrateDoneResponse{Ok: false, Error: "decode"}, nil)
	}
	c := s.cluster
	if c == nil || !c.ValidateToken(req.Token) {
		return nodeResponse(transport.OpMigrateDone, frame, MigrateDoneResponse{Ok: false, Error: "invalid cluster token"}, nil)
	}
	ps, refusal := c.acceptedPull(ctx, pullStep{database: req.Database, bucket: req.Bucket, transfer: req.Transfer,
		action: "complete the hand-over"})
	if refusal != nil {
		return nodeResponse(transport.OpMigrateDone, frame, MigrateDoneResponse{Ok: false, Error: refusal.Error()}, nil)
	}
	c.clearPending(migrateKey(req.Database, req.Bucket), ps.target)
	if hex.EncodeToString(ps.digest[:]) != req.Digest {
		c.logger.Warn("migration digest mismatch; keeping the source copy", "error", errMigrateDigestMismatch)
		return nodeResponse(transport.OpMigrateDone, frame, MigrateDoneResponse{Ok: false, Error: errMigrateDigestMismatch.Error()}, nil)
	}
	// The target records the snapshot's position itself and reports it at
	// the handshake; this node keeps none (spec §19.3.3). A stream to it that
	// was refused for the copy it lacked can deliver now: its sender tries
	// again at once.
	c.wakeSenderOf(ps.target)
	// The source of a pull deletes nothing (step 10b.7): a copy
	// outside the bucket's set is released by the sweep once every member of
	// the set holds what it holds (handOffBucket), the source of a pull as
	// any other copy, and a removed node never deletes one (step 9.3).
	c.logger.Info("a node copied a bucket from this one; this node keeps its copy until the bucket's set holds what it holds",
		"database", req.Database, "bucket", req.Bucket, "target", ps.target)
	return nodeResponse(transport.OpMigrateDone, frame, MigrateDoneResponse{Ok: true}, nil)
}

// pullBucket moves one bucket from `source` to this node (the target). This is
// the target-side of the migration: open a pull, fetch the chunks, materialize
// into the target's store, verify the digest, and tell the source to drop its
// copy. Store type and structure come from the source's start response.
func (c *Cluster) pullBucket(ctx context.Context, database, bucket string, source ring.Node) error {
	return c.pullBucketPlaced(ctx, database, bucket, source, false)
}

// pullBucketPlaced is pullBucket, optionally refusing to land a copy on a node
// the bucket's set does not include. The refusal is the entry points' (a
// migrate, a hand-off's ask, the sweep): the primitive itself moves data
// wherever it is told to.
func (c *Cluster) pullBucketPlaced(ctx context.Context, database, bucket string, source ring.Node, mustBePlaced bool) error {
	return c.pullBucketWith(ctx, pullPlan{database: database, bucket: bucket, source: source, mustBePlaced: mustBePlaced})
}

// pullPlan is one pull: the bucket, its source, whether to refuse landing it
// on a node its set does not include, and whether it is a take-over's.
type pullPlan struct {
	database, bucket string
	source           ring.Node
	mustBePlaced     bool
	// takeOver names the pull as a hand-off (MigrateStartRequest.Handoff), so
	// any holder serves it, and opens the landed copy in the batch that lands
	// it (step 10b.5).
	takeOver bool
	// replaces is a copy of the bucket this node holds that the pull
	// replaces, or nil. It is released only once the pull has fetched and
	// checked the whole copy that replaces it, and that copy fits beside it
	// (BUG-0664): a pull refused before then leaves this node's copy serving.
	replaces *copyReplacement
}

// copyReplacement is a copy of a bucket this node holds that a pull replaces.
// The pulls of one step share it — a take-over tries holder after holder — so
// the copy is released once, by the first pull that gets that far.
type copyReplacement struct {
	// why ends "deleting this node's copy of bucket %q in database %q".
	why string
	// afterRelease, if set, runs once the copy is released, outside the fence.
	afterRelease func()
	released     bool
}

// pullFallbackBufferBytes bounds the copy one pull holds in memory on a node
// that read no memory limit at start, which only a failed read leaves: the
// host's total memory is read when no cgroup limit is set.
const pullFallbackBufferBytes = 4 << 30

// pullBufferBytes is the most of one copy a pull holds in memory: half the
// memory limit this start read. A pull holds the whole copy until it is
// checked (pullBucketWith), so the source's chunk count would otherwise set
// how much this node allocates. Half leaves the rest of the node, the
// memtable included, its share; it is a policy line, not a measured one.
func (c *Cluster) pullBufferBytes() (bound, limit int64) {
	limit = c.s.memtable.advertisedLimitBytes()
	if limit <= 0 {
		return pullFallbackBufferBytes, 0
	}
	return limit / 2, limit
}

// refuseCopyOverTheBuffer refuses a pull whose start announces more chunks
// than this node holds in memory for one copy, before any chunk is asked for.
// It reads the node's own bound and the chunk count: O(1), nothing per bucket.
func (c *Cluster) refuseCopyOverTheBuffer(database, bucket string, source ring.Node, numChunks int) error {
	bound, limit := c.pullBufferBytes()
	if int64(numChunks) <= bound/snapshotChunkSize {
		return nil
	}
	why := "half the memory limit of " + formatBytesIEC(limit) + " it read at start"
	if limit == 0 {
		why = "it read no memory limit at start"
	}
	return fmt.Errorf("%w: %s offered a copy of bucket %q in database %q of %d chunks of up to %s each, "+
		"more than this node holds in memory for one copy (%s: %s); nothing was fetched. "+
		"A pull holds the whole copy in memory until it is checked: give this node more memory, restart it, "+
		"and run 'migrate %s %s' on it",
		errPulledCopyRefused, source.ID, bucket, database, numChunks, formatBytesIEC(snapshotChunkSize),
		formatBytesIEC(bound), why, database, bucket)
}

// pending reports whether there is a copy still to release.
func (r *copyReplacement) pending() bool {
	return r != nil && !r.released
}

// pullBucketWith runs one pull. It is the one place a bucket this node does
// not hold lands here, so the disk guard is here, before anything is asked of
// the source or registered (BUG-0104).
func (c *Cluster) pullBucketWith(ctx context.Context, plan pullPlan) error {
	database, bucket, source, mustBePlaced := plan.database, plan.bucket, plan.source, plan.mustBePlaced
	refused := refusedSourcesOf(ctx)
	if err := refused.skipIfRefused(source.ID); err != nil {
		return err
	}
	if err := c.refuseLandingOnFullDisk(database, bucket); err != nil {
		return err
	}
	pull := c.beginPull(database, bucket)
	defer c.endPull(database, bucket, pull)
	replacing := plan.replaces.pending()
	if replacing {
		// Marked where the copy used to be released, so the stream to this
		// node sees what it saw then: entries refused until a copy lands.
		fence := c.s.acquireFence(placementOf(database, bucket), fenceExclusive)
		c.beginReplacing(pull)
		fence.release()
	}
	if hook := pullStartedHook.Load(); hook != nil {
		(*hook)(database, bucket)
	}
	p := c.peerFor(source)
	dialed, err := p.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial migration source %s: %w", source.ID, err)
	}
	cli := peerConn(&boundedPullConn{peerConn: dialed, source: source, logger: c.logger,
		timeout: pullRequestTimeout.get(), startTimeout: pullStartTimeout.get()})

	exchange := !seamsOff().noPullExchange
	var dropped []int64
	if exchange {
		if dropped, err = c.s.droppedGenerations(database); err != nil {
			return fmt.Errorf("reading the dropped generations of database %q to pull %s: %w", database, bucket, err)
		}
	}
	// Built for every ask: the incarnation it names is the one recorded for the
	// source when it is sent, which a "not yet" waits to change. askedAt is the
	// epoch of the ask the source served.
	var askedAt uint64
	startReq := func() ([]byte, error) {
		askedAt = c.Epoch()
		encoded, err := bson.Marshal(MigrateStartRequest{
			Token: c.localToken(), Database: database, Bucket: bucket, FromNode: c.self.ID,
			Epoch: askedAt, Store: c.s.storeClaim(), DroppedGenerations: dropped,
			Incarnation: c.recordedPosition(source.ID).Incarnation, Handoff: plan.takeOver,
			Operator: operatorMigrate(ctx),
		})
		if err != nil {
			return nil, fmt.Errorf("encoding migrate start request for %s.%s: %w", database, bucket, err)
		}
		return encoded, nil
	}
	start, err := startPull(ctx, cli, startReq, source, c.wakeGossip)
	if err != nil {
		return err
	}
	if !start.Ok {
		if start.NotPlaced {
			return fmt.Errorf("%w: migration start refused by %s: %s", errNotPlacedHere, source.ID, start.Error)
		}
		if start.RefusedForGood {
			return refused.note(source.ID, fmt.Errorf("migration start refused by %s, and waiting will not change it: %s", source.ID, start.Error))
		}
		if strings.Contains(start.Error, ErrMigrateBucketNotFound.Error()) {
			return fmt.Errorf("%w (from %s): %s", ErrMigrateBucketNotFound, source.ID, start.Error)
		}
		if strings.Contains(start.Error, errPlaintextPeerRefused.Error()) {
			return fmt.Errorf("migration start rejected by %s: %s: %w", source.ID,
				strings.TrimSuffix(start.Error, ": "+errPlaintextPeerRefused.Error()), errPlaintextPeerRefused)
		}
		return fmt.Errorf("migration start rejected by %s: %s", source.ID, start.Error)
	}
	// The snapshot's position is a sequence of the source's log, and it is
	// recorded only under the incarnation this node holds for that source
	// (A-4.2): a position taken in another life of the log would be clamped
	// by the next rebase, and the entries under it applied over the copy.
	heldCap, held := c.holdSnapshotCap(source.ID, start.Incarnation)
	if !held {
		return fmt.Errorf("%s offered a snapshot of bucket %q in database %q taken in an incarnation of its log this node has not recorded "+
			"for it; nothing was pulled, and the pull is tried again once %s's stream to this node has opened", source.ID, bucket, database, source.ID)
	}
	defer c.releaseSnapshotCap(source.ID, heldCap)
	// The pull's generation is recorded before anything is learned: a drop
	// cancels the pulls of the generations it names and of those not known
	// yet, and the drops learned next must not cancel this one for being
	// unknown.
	generation := start.DatabaseGeneration
	if generation == 0 {
		generation = legacyGeneration(database)
	}
	c.setPullGeneration(pull, generation)
	// The source's dropped generations of the database are applied first
	// (G-11 (b)): they may drop a copy held here, and say whether the copy
	// offered is of an incarnation that is gone (G-7).
	if exchange {
		if err := c.s.learnTombstones(ctx, database, start.DroppedGenerations); err != nil {
			return fmt.Errorf("applying the dropped generations of database %q that %s sent: %w", database, source.ID, err)
		}
	}
	if gone, err := c.s.generationDropped(database, generation); err != nil || gone {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: %s offered bucket %q of database %q from incarnation %d, which this node knows was dropped; nothing was pulled",
			errPullDatabaseDropped, source.ID, bucket, database, generation)
	}

	// The source knows the bucket's factor and this node may not: record it for
	// placement and ask again whether this node belongs, before any chunk is
	// fetched. The source has already judged this on its own view; this is the
	// second opinion on the view it reported. A pull that does not land forgets
	// the record.
	if mustBePlaced {
		key := placementOf(database, bucket)
		c.recordBucketFactor(key, start.ReplicationFactor)
		defer func() {
			if !c.s.bucketStoreOpen(database, bucket) {
				c.forgetBucketFactor(key)
			}
		}()
		if !c.Hosts(key) {
			placed, _ := c.Replicas(key)
			return fmt.Errorf("%w: bucket %q in database %q at replication_factor %d is placed on %v (%s owns it); "+
				"not pulling a copy nothing ships to",
				errNotPlacedHere, bucket, database, c.effectiveRF(key), nodeIDs(placed), placed[0].ID)
		}
	}

	if err := c.refuseCopyOverTheBuffer(database, bucket, source, start.NumChunks); err != nil {
		return err
	}
	// Every chunk but the last is full, so a copy that cannot fit beside the
	// one it replaces even at that size is refused before it is fetched.
	if replacing && start.NumChunks > 1 {
		if err := c.refuseReplacingPastTheMark(database, bucket, uint64(start.NumChunks-1)*snapshotChunkSize); err != nil {
			return err
		}
	}
	var full bytes.Buffer
	for i := 0; i < start.NumChunks; i++ {
		dr, err := bson.Marshal(MigrateDataRequest{Token: c.localToken(), Database: database, Bucket: bucket, Index: i, Transfer: start.Transfer})
		if err != nil {
			return fmt.Errorf("encoding migrate data request %d for %s.%s: %w", i, database, bucket, err)
		}
		df, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpMigrateData, dr)
		if err != nil {
			return fmt.Errorf("migrate data chunk %d: %w", i, err)
		}
		var chunk MigrateDataResponse
		if err := decodePeerAnswer(df.Payload, &chunk); err != nil {
			return fmt.Errorf("decode migrate chunk %d: %w", i, err)
		}
		if !chunk.Ok {
			return fmt.Errorf("migrate chunk %d rejected: %s", i, chunk.Error)
		}
		// The bound on the copy is the chunk count times the chunk size, so it
		// holds only while no chunk is larger than a chunk is.
		if len(chunk.Data) > snapshotChunkSize {
			return fmt.Errorf("%w: %s sent chunk %d of bucket %q in database %q with %d bytes, larger than a chunk is (%d bytes); "+
				"nothing of the copy was written", errPulledCopyRefused, source.ID, i, bucket, database, len(chunk.Data), snapshotChunkSize)
		}
		full.Write(chunk.Data)
	}

	digest := sha256.Sum256(full.Bytes())
	pulled := migratedBucket{database: database, name: bucket, storeType: start.StoreType, indices: start.Indices,
		stores: start.Stores, nextStoreID: start.NextStoreID, fields: start.Fields, fieldTypes: start.FieldTypes, replicationFactor: start.ReplicationFactor,
		placement: start.Placement, part: start.Part, partOf: start.PartOf, generation: generation, takeOver: plan.takeOver, askedAt: askedAt}
	// The whole copy is checked before anything of it is written and before
	// a copy it replaces is released, so a copy refused for what it holds
	// changes nothing here.
	if err := c.checkPulledCopy(pulled, full.Bytes(), source); err != nil {
		return err
	}
	if replacing {
		if err := c.refuseReplacingPastTheMark(database, bucket, uint64(full.Len())); err != nil {
			return err
		}
		if _, err := c.landingStillWanted(pulled); err != nil {
			return err
		}
		if hook := replacingPullCheckedHook.Load(); hook != nil {
			(*hook)(database, bucket)
		}
	}
	// Record what the snapshot covers before the store can open: from here
	// on every entry of the bucket from this source at or below the snapshot's
	// sequence is contents this node holds, not work (handleReplPush), so the
	// owner's catch-up cannot replay the bucket's history over the copy the
	// next line materializes. A pull that fails to land forgets it
	// again, so a later pull can still seed this node. The source may have
	// restarted since MIGRATE_START: the position is recorded no higher than
	// the rebases that ran since say the new log keeps (BUG-0311).
	//
	// Only for a bucket this node does not serve yet, checked under the
	// bucket's fence, which a replicated create holds from its "is a pull of
	// it pending" check to its apply: the stream may have created the bucket
	// before this pull registered. A position recorded then makes the stream's
	// later entries pass as held by a snapshot that can never land here — its
	// materialise is refused as already served and the position forgotten — so
	// they are acknowledged and never applied. The stream that created the
	// bucket delivers the rest; the pull stops.
	//
	// A copy this pull replaces is released first, in the same hold, so the
	// name is claimed by this pull alone from the release on.
	fence := c.s.acquireFence(placementOf(database, bucket), fenceExclusive)
	var releaseErr, seedErr error
	if replacing {
		releaseErr = c.releaseReplacedCopyLocked(pull, plan.replaces, database, bucket)
	}
	served := releaseErr == nil && c.s.bucketStoreOpen(database, bucket)
	endLanding := func() {}
	if releaseErr == nil && !served {
		// The landing is marked before the position is recorded: a push that
		// sees the position (under this fence or after it) then finds the mark,
		// and waits instead of being refused.
		endLanding = c.beginLanding(database, bucket)
		seedErr = c.recordSeededWithin(source.ID, database, bucket, start.HighWater, heldCap)
		if seedErr != nil {
			endLanding()
		}
	}
	fence.release()
	defer endLanding()
	if releaseErr != nil {
		return releaseErr
	}
	if replacing && plan.replaces.afterRelease != nil {
		plan.replaces.afterRelease()
	}
	if served {
		c.logger.Info("a pull found the bucket already served here, from the stream; it stops, and the stream delivers the rest",
			"database", database, "bucket", bucket, "source", source.ID)
		// The source built a snapshot for this pull and frees it on
		// MIGRATE_DONE alone; without the message it holds the snapshot for
		// pendingSnapshotIdle. Nothing landed here, so a refusal costs this
		// pull nothing and is only logged.
		if err := c.sendMigrateDone(ctx, cli, plan, start.Transfer, digest[:]); err != nil {
			c.logger.Warn("a pull that stopped because the bucket is already served could not tell its source to free the snapshot; "+
				"the source frees it when its idle time runs out",
				"database", database, "bucket", bucket, "source", source.ID, "error", err)
		}
		return nil
	}
	if seedErr != nil {
		return seedErr
	}
	if hook := beforeMaterializeHook.Load(); hook != nil {
		(*hook)(database, bucket)
	}
	err = c.materializeBucket(ctx, pull, pulled, full.Bytes())
	c.endReplacing(pull)
	if err != nil {
		// The position is forgotten before the landing ends, so a push that
		// waited for it does not pass the bucket's entries as held.
		forgetErr := c.forgetSeeded(source.ID, database, bucket)
		endLanding()
		if forgetErr != nil {
			return errors.Join(err, forgetErr)
		}
		return err
	}
	// The copy is in place: a push waiting for it need not wait for the
	// source's answer to MIGRATE_DONE.
	endLanding()

	if err := c.sendMigrateDone(ctx, cli, plan, start.Transfer, digest[:]); err != nil {
		return err
	}
	c.logger.Info("bucket migrated in", "database", database, "bucket", bucket, "from", source.ID)
	return nil
}

// sendMigrateDone tells the source a pull is finished, so it frees the pending
// snapshot built for it. The normal landing and the already-served stop both
// end here, and the source's handler clears a snapshot by its transfer id once,
// so a second message for the same transfer is refused rather than freeing
// another puller's snapshot.
func (c *Cluster) sendMigrateDone(ctx context.Context, cli peerConn, plan pullPlan, transfer string, digest []byte) error {
	done, err := bson.Marshal(MigrateDoneRequest{Token: c.localToken(), Database: plan.database, Bucket: plan.bucket,
		Digest: hex.EncodeToString(digest), Transfer: transfer})
	if err != nil {
		return fmt.Errorf("encoding migrate done request for %s.%s: %w", plan.database, plan.bucket, err)
	}
	df, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpMigrateDone, done)
	if err != nil {
		return fmt.Errorf("migrate done to %s: %w", plan.source.ID, err)
	}
	var resp MigrateDoneResponse
	if err := decodePeerAnswer(df.Payload, &resp); err != nil {
		return fmt.Errorf("decode migrate done: %w", err)
	}
	if !resp.Ok {
		return fmt.Errorf("migrate done rejected by %s: %s", plan.source.ID, resp.Error)
	}
	return nil
}

// pullRequestTimeout bounds each MIGRATE_DATA chunk and the MIGRATE_DONE a pull
// sends its source (MIGRATE_START has pullStartTimeout), so a source that accepts the
// connection and never answers ends the attempt instead of holding the worker
// that made it (BUG-0620). It is a bound on one request, not on the pull, so a
// slow pull that keeps receiving is never cut. An atomic, so a test shortens it.
var pullRequestTimeout = newSweepBound(2 * time.Minute)

// pullStartTimeout bounds a pull's MIGRATE_START alone. The source answers it
// only after it has built the bucket's whole snapshot under the fence and
// hashed it with SHA-256, which was measured at 3.5 to 5 s per GB on a 4-vCPU
// VM with a warm page cache (1 GB in 3.2 s, 2 GB in 7.1 s). The docs allow
// buckets of tens of GB, so a START of 100 to 200 s is plausible, and under
// pullRequestTimeout such a bucket would time out on every attempt and never
// copy. Thirty minutes is that rate over a 100 GB bucket (about 8 minutes)
// with margin for a cold cache and a loaded disk. A constant, not a setting;
// an atomic only so a test shortens it. MIGRATE_DATA and MIGRATE_DONE answer
// from the built snapshot and keep pullRequestTimeout.
var pullStartTimeout = newSweepBound(30 * time.Minute)

// boundedPullConn is a pull's view of its source: every request it sends has
// its own deadline, taken from pullRequestTimeout (and pullStartTimeout for
// MIGRATE_START) when the pull starts. It is the one place the bound is applied, so the pending-copy
// worker, the take-over worker and an operator's migrate, which all pull
// through pullBucketWith, get it together. The deadline is per request and
// not per pull because a request is the pull's unit of progress: the next
// chunk gets a fresh deadline, so a slow pull that keeps receiving is never
// cut. The transport's SendRequest ends with the context but leaves the
// abandoned frame's slot held until the source answers or its socket fails, so
// a source that stays hung costs the pool a slot per abandoned request; each
// retry then waits for a slot under the same deadline.
type boundedPullConn struct {
	peerConn
	source ring.Node
	logger *slog.Logger
	// timeout bounds MIGRATE_DATA and MIGRATE_DONE; startTimeout bounds
	// MIGRATE_START, whose answer waits for the source to build and hash the
	// whole snapshot (pullStartTimeout).
	timeout, startTimeout time.Duration
}

// SendRequest is the wrapped connection's, ended after timeout. The error
// names the source and the request, and a deadline of this wrapper's is told
// from the caller's own cancellation by the caller's context still being live.
func (b *boundedPullConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	timeout := b.timeout
	if op == transport.OpMigrateStart {
		timeout = b.startTimeout
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	frame, err := b.peerConn.SendRequest(requestCtx, lane, op, payload)
	if err != nil && ctx.Err() == nil && errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
		b.logger.Warn("a pull's source did not answer within the per-request deadline; the attempt ends and is tried again",
			"source", b.source.ID, "opcode", op.String(), "deadline", timeout, "error", err)
		return nil, fmt.Errorf("source %s did not answer %s within the per-request deadline of %s, so the attempt ends and is tried again: %w",
			b.source.ID, op, timeout, err)
	}
	return frame, err
}

// pullNotYetWait bounds how long a pull waits on a source that answered "not
// yet" because its stream to this node has not opened since it started
// (errStreamNotOpenToPuller). The refusal woke that stream's sender, and its
// handshake is one round trip, so the wait is normally one retry.
const pullNotYetWait = 5 * time.Second

// startPull sends a pull's MIGRATE_START and returns the source's answer,
// asking again while the source says "not yet" — its stream to this node has
// not opened, or the two are on different membership epochs — for at most
// pullNotYetWait. An epoch refusal calls wakeGossip first, so this node learns
// the newer membership if it is the one behind; each ask names the epoch this
// node is on when it is sent.
func startPull(ctx context.Context, cli peerConn, startReq func() ([]byte, error), source ring.Node, wakeGossip func()) (MigrateStartResponse, error) {
	deadline := time.Now().Add(pullNotYetWait)
	for {
		encoded, err := startReq()
		if err != nil {
			return MigrateStartResponse{}, err
		}
		sf, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpMigrateStart, encoded)
		if err != nil {
			return MigrateStartResponse{}, fmt.Errorf("migrate start from %s: %w", source.ID, err)
		}
		var start MigrateStartResponse
		if err := decodePeerAnswer(sf.Payload, &start); err != nil {
			return MigrateStartResponse{}, fmt.Errorf("decode migrate start: %w", err)
		}
		acrossEpochs := strings.Contains(start.Error, errPullAcrossEpochs.Error())
		if acrossEpochs {
			wakeGossip()
		}
		notYet := acrossEpochs || strings.Contains(start.Error, errStreamNotOpenToPuller.Error())
		if start.Ok || !notYet || time.Now().After(deadline) {
			return start, nil
		}
		select {
		case <-ctx.Done():
			return start, nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// materializeBucket restores snapshot bytes into this node's store for the
// bucket and re-attaches it, so the join fence lifts and the node serves it.
//
// It is a second writer to the bucket, so it holds the bucket's fence shared
// across its writes and its attach: a drop's exclusive hold then waits for it
// or precedes it. A drop that preceded it cancelled the pull, which is
// re-checked here under the hold, and the stream is discarded (spec §5.3).
func (c *Cluster) materializeBucket(ctx context.Context, pull *pullRecord, pulled migratedBucket, stream []byte) error {
	key := placementOf(pulled.database, pulled.name)
	fence := c.s.acquireFence(key, fenceShared)
	defer fence.release()
	if c.pullCancelled(pull) {
		return fmt.Errorf("pulling bucket %q in database %q: %w", pulled.name, pulled.database, errPullDiscardedByDrop)
	}
	epoch, err := c.landingStillWanted(pulled)
	if err != nil {
		return err
	}
	if pulled.takeOver {
		if err := c.refuseTermPastLimit(heldBucket{database: pulled.database, bucket: pulled.name}, key, epoch); err != nil {
			return err
		}
		pulled.openAs = epoch
	}
	// A database drop does not take this fence, so the materialise re-checks
	// the pull under the allocator's mutex before its catalog commit.
	pulled.cancelled = func() bool { return c.pullCancelled(pull) }
	// Every engine lives on the shared store, so the stream is a range. The
	// target's own floor, not the source's applied_seq: replay and
	// live applies here compare this node's sequences (build doc SA-18).
	err = c.s.materializeRange(pulled, stream, c.walLastSequence())
	if errors.Is(err, errPullDatabaseDropped) || errors.Is(err, errPullOfDroppedIncarnation) {
		// The reserved id's materialise record is the reclaimer's now that
		// materializeRange has ended it; wake it rather than wait for its idle
		// timer.
		c.s.wakeReclaimer()
	}
	return err
}

// landingStillWanted refuses a pulled copy this node's view no longer wants
// landed. A copy served on an epoch this node has since left is of the
// source's placement then, not of the bucket's now: landed, it would carry no
// mark of the change and follow no owner's term until an entry of a later term
// reached it (spec §20.8.6). It is discarded, and the pull asked again on the
// newer epoch, as a source refuses a pull across epochs. A take-over's copy is
// opened here, so it needs this node to own the bucket still. A pull that
// replaces a copy asks before the release as well (BUG-0664); the
// materialise asks again under its fence. For a take-over it returns the epoch
// of the ownership view it read, which the copy is opened at; 0 otherwise.
func (c *Cluster) landingStillWanted(pulled migratedBucket) (uint64, error) {
	if !pulled.takeOver {
		if pulled.askedAt != 0 && pulled.askedAt != c.Epoch() {
			return 0, fmt.Errorf("%w: bucket %q in database %q was served on epoch %d and this node is on epoch %d; nothing was landed",
				errPullAcrossEpochs, pulled.name, pulled.database, pulled.askedAt, c.Epoch())
		}
		return 0, nil
	}
	owner, epoch, err := c.ownerView(placementOf(pulled.database, pulled.name))
	if err != nil {
		return 0, fmt.Errorf("reading the owner of bucket %q in database %q before opening the copy a take-over pulled: %w", pulled.name, pulled.database, err)
	}
	if owner.ID != c.self.ID {
		return 0, fmt.Errorf("%w: ownership of bucket %q in database %q moved to %s while this node copied it in", errNotYetDecided, pulled.name, pulled.database, owner.ID)
	}
	return epoch, nil
}

// releaseReplacedCopyLocked releases the copy a pull replaces, once the pull
// has checked the copy that replaces it. The caller holds the bucket's fence
// exclusively, so a drop of the name since the pull began has cancelled it
// and is answered as at the materialise; the release's own cancel of the
// name's pulls passes this one over (cancelPullsLocked).
func (c *Cluster) releaseReplacedCopyLocked(pull *pullRecord, replaced *copyReplacement, database, bucket string) error {
	c.migrateMu.Lock()
	cancelled := pull.cancelled
	pull.releasing = !cancelled
	c.migrateMu.Unlock()
	if cancelled {
		return fmt.Errorf("pulling bucket %q in database %q: %w", bucket, database, errPullDiscardedByDrop)
	}
	err := c.deleteLocalCopyLocked(database, bucket, replaced.why)
	c.migrateMu.Lock()
	pull.releasing = false
	c.migrateMu.Unlock()
	if err != nil {
		return err
	}
	replaced.released = true
	return nil
}

// checkPulledCopy checks a copy a pull fetched, whole, before anything of it
// is written and before anything this node holds changes: what it holds
// (checkMigratedStream) and the incarnation it is of. A copy refused for what
// it holds is errPulledCopyRefused, which a take-over answers by trying
// another holder (holderRefusedCopy). The materialise checks both again.
func (c *Cluster) checkPulledCopy(pulled migratedBucket, stream []byte, source ring.Node) error {
	state, err := checkMigratedStream(stream, migratedLayout{storeType: pulled.storeType, fields: fieldLayoutOf(pulled.stores)})
	if err != nil {
		return fmt.Errorf("%w: the copy of bucket %q in database %q that %s sent: %w", errPulledCopyRefused, pulled.name, pulled.database, source.ID, err)
	}
	dropped, err := c.s.dropRecorded(pulled.database, pulled.name, state.Incarnation)
	if err != nil {
		return fmt.Errorf("reading whether incarnation %d of bucket %q in database %q was dropped here: %w", state.Incarnation, pulled.name, pulled.database, err)
	}
	if dropped {
		return &pullOfDroppedIncarnationError{database: pulled.database, bucket: pulled.name, incarnation: state.Incarnation}
	}
	return nil
}

// migratedBucket is what the source's start response declares about a pulled
// bucket — its name, store type and index declaration — none of which the
// range stream itself carries.
type migratedBucket struct {
	database, name string
	storeType      BucketType
	indices        []catalogIndex
	stores         []catalogStore
	nextStoreID    int32
	// fields and fieldTypes are the typed-field declaration as the source's
	// catalog records it.
	fields     []catalogField
	fieldTypes string
	// replicationFactor is the factor the bucket declared on its source, or 0.
	replicationFactor int32
	// placement is the source's recorded placement, "" for a non-hash bucket.
	placement string
	// part and partOf are a part's catalog fields, 0 for any other bucket.
	part   int32
	partOf uint64
	// generation is the incarnation the bucket was created in on its source;
	// 0 from a source that predates generations, read as the legacy one.
	generation int64
	// cancelled reports whether a drop on this node cancelled the pull landing
	// the bucket. Nil when no pull is behind the materialise (a test driving
	// materializeRange directly); the database witness still guards it then.
	cancelled func() bool
	// takeOver is a take-over's pull, and openAs the epoch the landed copy is
	// opened at, read under the bucket's fence by materializeBucket: the copy
	// is opened in the catalog batch that lands it (spec §20.8.5 step 4).
	takeOver bool
	openAs   uint64
	// askedAt is the epoch this node asked the source on; zero when no pull
	// is behind the materialise.
	askedAt uint64
}

// dropped reports whether a drop cancelled the pull landing the bucket.
func (p migratedBucket) dropped() bool {
	return p.cancelled != nil && p.cancelled()
}

// The migration stream (build doc SA-10) is a sequence of BSON documents: one
// migrateRangeRecord per key of the bucket, then one migratePayloadRecord per
// blob whose content is a file. It carries no slot and no id — not in a key and
// not in a file name — and the target writes both under its own (spec §5.3).
// It is node to node only: spec §7 refuses an in-place upgrade, so no node of
// another version reads it.

// migrateRangeRecord is one key of a bucket on the shared store, its whole
// [slot][id] prefix stripped.
type migrateRangeRecord struct {
	Key   []byte `bson:"k"`
	Value []byte `bson:"v"`
}

// migratePayloadRecord is one blob payload file's content, by blob id. The
// file's name is rebuilt on the target from the target's slot and id; the
// source's is never shipped.
type migratePayloadRecord struct {
	BlobID string `bson:"blob_id"`
	Data   []byte `bson:"data"`
}

// migrateStreamRecord decodes either record; a payload record is the one with
// a blob id.
type migrateStreamRecord struct {
	Key    []byte `bson:"k,omitempty"`
	Value  []byte `bson:"v,omitempty"`
	BlobID string `bson:"blob_id,omitempty"`
	Data   []byte `bson:"data,omitempty"`
}

// migrateBatchBytes bounds each batch a target writes a migrated range in,
// well under the store's largest atomic batch.
const migrateBatchBytes = 1 << 20

// streamBucketRange writes every key of bucket id in slot, from one snapshot,
// then the content of every payload file the bucket's blob metadata names,
// read from the blob root. The caller holds the bucket's fence exclusively, so no
// put or delete changes a file between the metadata read and the file read.
func (s *Server) streamBucketRange(w *bytes.Buffer, kv storage.KV, slot uint16, databaseID, id uint64) error {
	payloads, err := streamBucketKeys(w, kv, slot, databaseID, id)
	if err != nil {
		return err
	}
	for _, payload := range payloads {
		// Through the codec on a sealed node: the file's bytes are ciphertext
		// under this node's keys, which the target cannot open and must not be
		// sent. No key crosses the wire; the target seals under its own.
		content, err := store.ReadPayload(store.PayloadRead{
			Root: s.blobRoot(), Path: payload.relPath, Slot: slot, BucketID: id,
			BlobID: payload.blobID, Size: payload.size, Keys: s.blobKeys,
		})
		if err != nil {
			return fmt.Errorf("reading the payload of blob %s of bucket %d: %w", payload.blobID, id, err)
		}
		record, err := bson.Marshal(migratePayloadRecord{BlobID: payload.blobID, Data: content})
		if err != nil {
			return fmt.Errorf("encoding the payload of blob %s of bucket %d: %w", payload.blobID, id, err)
		}
		w.Write(record)
	}
	return nil
}

// payloadRef is a file-backed blob the key stream met.
type payloadRef struct {
	blobID, relPath string
	// size is the metadata's size, which a sealed file's length is checked
	// against.
	size int64
}

// streamBucketKeys writes every key of bucket id in slot as a range record and
// returns the payload files its blob metadata names, in blob id order.
func streamBucketKeys(w *bytes.Buffer, kv storage.KV, slot uint16, databaseID, id uint64) ([]payloadRef, error) {
	snap := kv.NewSnapshot()
	defer snap.Discard()
	prefix := storage.Scope(slot, databaseID, id)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.Scope(slot, databaseID, id+1)})
	defer it.Close()
	var payloads []payloadRef
	for it.Seek(nil); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return nil, fmt.Errorf("reading key %x of bucket %d: %w", it.Key(), id, err)
		}
		key := it.Key()[len(prefix):]
		record, err := bson.Marshal(migrateRangeRecord{Key: key, Value: value})
		if err != nil {
			return nil, fmt.Errorf("encoding key %x of bucket %d: %w", it.Key(), id, err)
		}
		w.Write(record)
		if key[0] != storage.NSBlobMeta {
			continue
		}
		relPath, err := store.PayloadPathOf(value)
		if err != nil {
			return nil, fmt.Errorf("reading the metadata at %x of bucket %d: %w", it.Key(), id, err)
		}
		if relPath == "" {
			continue
		}
		size, err := store.PayloadSizeOf(value)
		if err != nil {
			return nil, fmt.Errorf("reading the metadata at %x of bucket %d: %w", it.Key(), id, err)
		}
		payloads = append(payloads, payloadRef{blobID: string(key[1:]), relPath: relPath, size: size})
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("walking the keys of bucket %d: %w", id, err)
	}
	return payloads, nil
}

// materializeRange lands a migrated range as a new bucket on this node's
// store, under its own slot and a new id: the blob payload files first, each
// durable under a name built from that slot and id, then the keys in bounded
// batches under a reserved id — so no batch commits metadata naming a file
// that is not yet on disk (spec §3.5's order) — then the catalog entry and the state document — with the
// source's applied_seq and failed_seqs replaced by this node's floor — in one
// batch, and only then the attach. Nothing serves the id until its data is
// present (build doc SA-14). The reservation writes the id's materialise
// record and the catalog batch deletes it, so a failure or a crash between the
// two leaves a record, and the reclaimer reclaims the id's keys and payload
// files once no materialise of it is running. The source's index declaration goes through
// the same prepare and into the same catalog field as a create's, so the index
// manager attaches over the migrated index keys and a restart re-attaches it.
//
// A database dropped while the range is in flight is not brought back:
// the database's marker is read before the reservation and again,
// with the pull's cancellation, under the allocator's mutex at the catalog
// commit, and a marker that has vanished or changed — or a cancelled pull —
// discards the range, leaving its materialise record for the reclaimer.
func (s *Server) materializeRange(pulled migratedBucket, stream []byte, floor uint64) error {
	database, name := pulled.database, pulled.name
	generation := entryGeneration(catalogEntry{database: database, DatabaseGeneration: pulled.generation})
	held, live, err := s.serverDataAccessor().claimPulledBucket(database, name, pulled.storeType, generation, pulled.dropped)
	if err != nil {
		return fmt.Errorf("materializing bucket %q in database %q: %w", name, database, err)
	}
	if !live {
		return fmt.Errorf("materializing bucket %q in database %q: %w", name, database, errPullDatabaseDropped)
	}
	held.rec.mu.Lock()
	defer held.rec.mu.Unlock()
	// A database drop takes no fence, so it can take the record between the
	// claim and this lock, and its slot may then name another bucket: the
	// pull is refused as the drop's re-check below would refuse it.
	if !held.current() {
		return fmt.Errorf("materializing bucket %q in database %q: %w", name, database, errPullDatabaseDropped)
	}
	bucket := s.handleLocked(database, held)
	if bucket.id.Load() != 0 {
		return fmt.Errorf("materializing bucket %q in database %q: this node already serves it", name, database)
	}
	_, catalogued, err := s.readCatalogEntry(database, name)
	if err != nil {
		return fmt.Errorf("materializing bucket %q: %w", name, err)
	}
	if catalogued {
		return fmt.Errorf("materializing bucket %q in database %q: it is already in this node's catalog but not served; restart the node to attach it", name, database)
	}
	options := indexOptions(pulled.indices)
	for key, value := range storeOptions(pulled.stores, pulled.nextStoreID) {
		if options == nil {
			options = make(map[string]interface{})
		}
		options[key] = value
	}
	for key, value := range fieldTypeOptions(pulled.fields, pulled.fieldTypes) {
		if options == nil {
			options = make(map[string]interface{})
		}
		options[key] = value
	}
	req := CreateBucketRequest{Database: database, Name: name, Type: pulled.storeType, Options: options}
	resp, err := s.prepareBucketLocked(bucket, req)
	if err != nil {
		return fmt.Errorf("materializing bucket %q: %w", name, err)
	}
	if !resp.Success {
		return fmt.Errorf("materializing bucket %q: %s", name, resp.Message)
	}
	slot := slotForCreate(database, name)
	id, err := s.reserveBucketID(materializeRecord{
		Slot: int32(slot), Type: pulled.storeType, Database: database, Name: name, ReservedAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("materializing bucket %q: %w", name, err)
	}
	defer s.endMaterialize(id)
	state, err := s.writeMigratedKeys(stream, migratedLayout{storeType: pulled.storeType, fields: bucket.fieldLayout()}, slot, bucket.databaseID, id)
	if err != nil {
		return fmt.Errorf("materializing bucket %q (id %d): %w", name, id, err)
	}
	// Both lists hold the sending node's sequences, which mean nothing here.
	// The lineage — term, position and base — is the source's and stays: it
	// is the origin's, and this copy holds exactly what the source held. The
	// copy originated nothing, so it is not opened (spec §20.8.1).
	state.AppliedSeq, state.FailedSeqs, state.LargeCommits = floor, nil, nil
	state.Opened = false
	if pulled.openAs != 0 {
		state.BaseTerm, state.BaseThrough = copyLineage(state.Term, state.Through, state.BaseTerm, state.BaseThrough)
		state.Term, state.Through, state.Opened = pulled.openAs, 0, true
	}
	// A copy of an incarnation this node dropped is never landed (R8-C):
	// a holder that has not applied the drop yet would otherwise
	// hand the dropped bucket back. The caller holds the bucket's fence, which
	// a drop takes exclusively, so the record is read after any drop of the
	// name here has committed. The reserved id is the reclaimer's.
	if dropped, err := s.dropRecorded(database, name, state.Incarnation); err != nil {
		return fmt.Errorf("materializing bucket %q (id %d): %w", name, id, err)
	} else if dropped {
		return &pullOfDroppedIncarnationError{database: database, bucket: name, incarnation: state.Incarnation}
	}
	if hook := materializeCatalogHook.Load(); hook != nil {
		if err := (*hook)(id); err != nil {
			return fmt.Errorf("materializing bucket %q (id %d): %w", name, id, err)
		}
	}
	fields, fieldTypes := bucket.declaredFieldTypes().catalogForm()
	entry := catalogEntry{
		database: database, bucket: name, Slot: int32(slot), Type: pulled.storeType,
		Indices: catalogIndices(bucket), CreatedAt: time.Now().UTC(),
		Stores: catalogStores(bucket), NextStoreID: bucket.storeFields().next,
		Fields: fields, FieldTypes: fieldTypes,

		ReplicationFactor:  pulled.replicationFactor,
		DatabaseGeneration: pulled.generation,
		Placement:          pulled.placement,
		Parts:              spreadPartsFor(pulled.placement),
		Part:               pulled.part,
		PartOf:             pulled.partOf,
	}
	// The engines are built before the commit, the one step of the attach that
	// can fail (a damaged index record), so nothing fallible but the Sync
	// follows the commit (BUG-0456).
	bucket.rec.replicationFactor.Store(pulled.replicationFactor)
	bucket.rec.setFlag(recSpread, pulled.placement == placementSpread)
	if err := s.buildEngines(bucket, s.kv, slot, id); err != nil {
		return fmt.Errorf("materializing bucket %q (id %d): %w", name, id, err)
	}
	if err := s.commitMigratedCatalog(pulled, entry, id, state); err != nil {
		return fmt.Errorf("materializing bucket %q (id %d): %w", name, id, err)
	}
	// The joiner answers the hand-off once this returns and the holder then
	// releases its copy, so the keys and the catalog entry must survive a power
	// loss first; so must the open of a take-over's copy. One Sync covers the
	// whole store, taken outside the allocator's mutex: it is per materialise,
	// not per bucket held.
	if err := s.kv.Sync(); err != nil {
		return s.abandonCommittedMaterialize(pulled, id, err)
	}
	s.noteAttachedToSweep(database, name, id)
	s.publishAttachLocked(bucket, slot, id)
	bucket.rec.setLineageStateLocked(state)
	s.registerBucketFactor(database, name, pulled.replicationFactor)
	// The range arrived raw, its unique indexes' entries unchecked (C-46). The
	// bucket is attached either way: a walk that cannot finish is reported,
	// and a failed pull would only be refused as already served.
	if err := s.verifyUniqueIndexesLocked(bucket, database, name); err != nil {
		s.logger.Error("could not verify the unique indexes of a migrated bucket; a duplicate the source held may be served",
			"database", database, "bucket", name, "error", err)
	}
	return nil
}

// abandonCommittedMaterialize undoes a catalog commit whose Sync failed: the
// entry may not be durable, so the copy is not answered for, and a retry must
// not find the name catalogued. One batch deletes the catalog entry and writes
// the id's materialise record back, so the reclaimer takes the keys once the
// materialise has ended. If that batch fails too, the entry stays and the
// error says how to clear it: a restart attaches the entry.
func (s *Server) abandonCommittedMaterialize(pulled migratedBucket, id uint64, syncErr error) error {
	database, name := pulled.database, pulled.name
	syncFailure := fmt.Errorf("syncing materialised bucket %q (id %d): %w", name, id, syncErr)
	record, err := bson.Marshal(materializeRecord{
		ID: int64(id), Slot: int32(slotForCreate(database, name)), Type: pulled.storeType,
		Database: database, Name: name, ReservedAt: time.Now().UTC(),
	})
	if err != nil {
		return errors.Join(syncFailure, fmt.Errorf("encoding the materialise record of bucket id %d: %w", id, err))
	}
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(catalogKey(database, name)); err != nil {
		return errors.Join(syncFailure, fmt.Errorf("deleting the catalog entry of bucket %q: %w", name, err))
	}
	if err := batch.Set(materializeKey(id), record); err != nil {
		return errors.Join(syncFailure, fmt.Errorf("writing the materialise record of bucket id %d: %w", id, err))
	}
	if err := batch.Commit(false); err != nil {
		return errors.Join(syncFailure, fmt.Errorf(
			"bucket %q in database %q stays catalogued but is not served: restart the node to attach it, then check the holder's copy: %w", name, database, err))
	}
	return syncFailure
}

// commitMigratedCatalog commits a materialise's catalog batch — its catalog
// entry, its state document, its database's marker when the database has none,
// and the deletion of its materialise record and of the bucket's needs-a-copy
// record — unless its database was dropped
// since the reservation. The allocator's mutex is held from the checks to the
// commit: commitDatabaseDrop commits the drop and cancels the database's pulls
// under it, so the drop and this batch are ordered one way or the other and
// never interleave. A discard leaves the materialise record, which the
// reclaimer converts and reclaims once the materialise has ended.
func (s *Server) commitMigratedCatalog(pulled migratedBucket, entry catalogEntry, id uint64, state store.BucketState) error {
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	if pulled.dropped() {
		return errPullDatabaseDropped
	}
	// The witness of a drop since the reservation is the copy's generation
	// among this node's dropped ones (SA-DG-12), checked under the mutex a
	// drop's tombstones are written under: the marker changes at every
	// generation a create adds, which says nothing about this copy.
	if gone, err := s.generationDropped(entry.database, entryGeneration(entry)); err != nil || gone {
		if err != nil {
			return err
		}
		return errPullDatabaseDropped
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := s.writeCreateRecords(batch, entry, id, state); err != nil {
		return err
	}
	// The record goes in the catalog entry's own batch: an id is never both
	// catalogued and the reclaimer's.
	if err := batch.Delete(materializeKey(id)); err != nil {
		return fmt.Errorf("deleting the materialise record: %w", err)
	}
	// A copy that lands is a fresh one: whatever marks the name had described
	// a copy this node no longer has (spec §20.8.2), and an outside mark left
	// standing would refuse the stream that follows it. The copy a
	// needs-a-copy record waits for is this one (bucket-replication-rev8-spec
	// §3.3).
	key := placementOf(entry.database, entry.bucket)
	if err := batch.Delete(placementMarkKey(key)); err != nil {
		return fmt.Errorf("deleting the placement marks of the migrated bucket: %w", err)
	}
	if err := batch.Delete(needsCopyKey(key)); err != nil {
		return fmt.Errorf("deleting the needs-a-copy record of the migrated bucket: %w", err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the migrated bucket: %w", err)
	}
	if s.cluster != nil {
		s.cluster.forgetPlacementMarks(key)
		s.cluster.forgetPendingCopy(key)
	}
	return nil
}

// migratedLayout is what a pull knows of the bucket whose range it reads and
// the range does not carry: the bucket's engine and its declared store fields.
// Together they say which values of the range are documents.
type migratedLayout struct {
	storeType BucketType
	fields    store.FieldLayout
}

// readMigrateStream decodes a migration stream into its records. The stream
// is a member's answer, so each record is validated before it is decoded, and
// so is every value of it a read of this node decodes later (storedDocumentOf):
// the whole stream is checked before any of it is written.
func readMigrateStream(stream []byte, layout migratedLayout) ([]migrateStreamRecord, error) {
	var records []migrateStreamRecord
	for consumed := 0; len(stream) > 0; {
		raw, rest, ok := bsoncore.ReadDocument(stream)
		if !ok {
			return nil, fmt.Errorf("the migration stream is damaged after %d bytes", consumed)
		}
		var record migrateStreamRecord
		if err := decodePeerAnswer(raw, &record); err != nil {
			return nil, fmt.Errorf("decoding a migration record after %d bytes: %w", consumed, err)
		}
		if err := refuseDeepMigratedValue(record, layout); err != nil {
			return nil, fmt.Errorf("the migration record after %d bytes: %w", consumed, err)
		}
		records = append(records, record)
		consumed += len(raw)
		stream = rest
	}
	return records, nil
}

// refuseDeepMigratedValue validates the document a key record's value holds,
// if it holds one, at the limit storedDocumentOf gives its kind; the value is
// written as it came, so only the refusal is kept. A payload record's data is a blob's content, and a record
// with no key is refused when it is written (migratedValue).
func refuseDeepMigratedValue(record migrateStreamRecord, layout migratedLayout) error {
	if record.BlobID != "" || len(record.Key) == 0 {
		return nil
	}
	document, limit, err := storedDocumentOf(record.Key, record.Value, layout)
	if err != nil {
		return fmt.Errorf("its key %x was not read: %w", record.Key, err)
	}
	if document == nil {
		return nil
	}
	if _, err := validateAt(document, limit); err != nil {
		return fmt.Errorf("its value was not read: %w", err)
	}
	return nil
}

// checkMigratedStream reads and checks a whole migration stream without
// writing anything, and returns the state document it carries: every refusal
// writeMigratedKeys can make of the stream's content, made up front.
func checkMigratedStream(stream []byte, layout migratedLayout) (store.BucketState, error) {
	records, err := readMigrateStream(stream, layout)
	if err != nil {
		return store.BucketState{}, err
	}
	return checkMigratedRecords(records)
}

// checkMigratedRecords checks the records of a migration stream against one
// another: a key on every key record, a decodable state document, each payload
// sized as its blob's metadata says, and a canonical blob id and a carried
// payload for each blob whose metadata names a file. It returns the state
// document.
func checkMigratedRecords(records []migrateStreamRecord) (store.BucketState, error) {
	var state store.BucketState
	if err := checkMigratedPayloadSizes(records); err != nil {
		return state, err
	}
	carried := make(map[string]bool)
	for _, record := range records {
		if record.BlobID == "" {
			continue
		}
		if _, ok := store.ParsePayloadFileName(store.PayloadFileName(0, 0, record.BlobID)); !ok {
			return state, fmt.Errorf("the migration stream carries a payload for %q, which is not a blob id", record.BlobID)
		}
		carried[record.BlobID] = true
	}
	for _, record := range records {
		if record.BlobID != "" {
			continue
		}
		if len(record.Key) == 0 {
			return state, errMigrateEmptyKey
		}
		switch record.Key[0] {
		case storage.NSState:
			if len(record.Key) != 1 {
				continue
			}
			if err := bson.Unmarshal(record.Value, &state); err != nil {
				return state, fmt.Errorf("decoding the migrated state document: %w", err)
			}
		case storage.NSBlobMeta:
			blobID := string(record.Key[1:])
			sourcePath, err := store.PayloadPathOf(record.Value)
			if err != nil {
				return state, fmt.Errorf("reading the migrated metadata of blob %s: %w", blobID, err)
			}
			if sourcePath != "" && !carried[blobID] {
				return state, fmt.Errorf("the migration stream carries the metadata of blob %s, whose content is a file, but not the file", blobID)
			}
		}
	}
	return state, nil
}

// storedDocumentOf is the BSON document the value of a bucket key holds, and
// the depth it may reach, for every value a read of this node decodes: the
// state document, a blob's metadata (whose caller-supplied part sits where a
// blob write request carries it), a hash or btree document, a queue or heap
// entry, a store field's element, and the entry a claimed or dead delivery
// record carries. key is the key after [slot][id], its namespace first. A
// blob bucket's document key maps to its blob id, a version is eight bytes
// and an index entry's value is empty, so none holds a document; nor does a
// namespace this build does not write, which no reader of it reads.
func storedDocumentOf(key, value []byte, layout migratedLayout) (document []byte, limit int, err error) {
	switch key[0] {
	case storage.NSState:
		return value, peerAnswerDepthLimit, nil
	case storage.NSBlobMeta:
		return value, requestDepthLimit(transport.OpBlobWrite), nil
	case storage.NSDocuments:
		if layout.storeType == BucketTypeBlob {
			return nil, 0, nil
		}
		return value, recordDepthLimit(recordStored, layout.storeType), nil
	case storage.NSQueue:
		return value, recordDepthLimit(recordStored, layout.storeType), nil
	case storage.NSFieldStore:
		isDocument, err := store.FieldValueIsDocument(key[1:], layout.fields)
		if err != nil || !isDocument {
			return nil, 0, err
		}
		return value, recordDepthLimit(recordStored, layout.storeType), nil
	case storage.NSDelivery:
		entry, dead, err := store.DeliveryValueDocument(key[1:], value)
		if err != nil || entry == nil {
			return nil, 0, err
		}
		kind := recordClaimed
		if dead {
			kind = recordDead
		}
		return entry, recordDepthLimit(kind, layout.storeType), nil
	}
	return nil, 0, nil
}

// writeMigratedPayloads writes every payload record's file under this node's
// slot and id, durably, and returns each blob's path by blob id. The blob id
// comes off the wire and becomes part of a file name, so it must be a
// canonical blob id — which also keeps it from naming a path outside its leaf.
func (s *Server) writeMigratedPayloads(records []migrateStreamRecord, slot uint16, id uint64) (map[string]string, error) {
	if err := checkMigratedPayloadSizes(records); err != nil {
		return nil, err
	}
	paths := make(map[string]string)
	for _, record := range records {
		if record.BlobID == "" {
			continue
		}
		if _, ok := store.ParsePayloadFileName(store.PayloadFileName(slot, id, record.BlobID)); !ok {
			return nil, fmt.Errorf("the migration stream carries a payload for %q, which is not a blob id", record.BlobID)
		}
		relPath, err := store.WritePayload(store.PayloadWrite{
			Root: s.blobRoot(), Slot: slot, BucketID: id, BlobID: record.BlobID, Keys: s.blobKeys, Content: record.Data,
		})
		if err != nil {
			return nil, fmt.Errorf("writing the migrated payload of blob %s: %w", record.BlobID, err)
		}
		paths[record.BlobID] = relPath
	}
	return paths, nil
}

// checkMigratedPayloadSizes checks, before any file is written, that each
// payload record carries exactly the bytes its blob's metadata says it has: a
// sealed file's length is 24 + size + 28·n, so a payload of another length
// would be sealed into a file this node's reader refuses (spec §4.2.1 c). A
// payload whose metadata the stream does not carry is refused too: nothing
// would name its file.
func checkMigratedPayloadSizes(records []migrateStreamRecord) error {
	recorded := make(map[string]int64)
	for _, record := range records {
		if record.BlobID != "" || len(record.Key) < 2 || record.Key[0] != storage.NSBlobMeta {
			continue
		}
		size, err := store.PayloadSizeOf(record.Value)
		if err != nil {
			return fmt.Errorf("reading the migrated metadata of blob %s: %w", record.Key[1:], err)
		}
		recorded[string(record.Key[1:])] = size
	}
	for _, record := range records {
		if record.BlobID == "" {
			continue
		}
		size, named := recorded[record.BlobID]
		if !named {
			return fmt.Errorf("the migration stream carries a payload for blob %s, but not the metadata that names it", record.BlobID)
		}
		if int64(len(record.Data)) != size {
			return fmt.Errorf("the migration stream carries %d bytes for the payload of blob %s, whose metadata records a size of %d bytes: "+
				"a payload is sealed into a file whose length is fixed by that size, so none was written", len(record.Data), record.BlobID, size)
		}
	}
	return nil
}

// migratedValue is the value a migrated key is written with: its own, except
// that a blob's metadata naming a payload file is re-pointed at the file this
// node wrote. Metadata naming a file the stream did not carry is refused: it
// would be served as a blob whose content is missing — the half-migration
// spec §5.3 warns of. An empty record.Key is refused too: the v1
// stream carries no decoder-level guarantee that a key record has a key, and
// record.Key[0] below is an index read, not a length check.
func migratedValue(record migrateStreamRecord, payloadPaths map[string]string) ([]byte, error) {
	if len(record.Key) == 0 {
		return nil, errMigrateEmptyKey
	}
	if record.Key[0] != storage.NSBlobMeta {
		return record.Value, nil
	}
	blobID := string(record.Key[1:])
	sourcePath, err := store.PayloadPathOf(record.Value)
	if err != nil {
		return nil, fmt.Errorf("reading the migrated metadata of blob %s: %w", blobID, err)
	}
	if sourcePath == "" {
		return record.Value, nil
	}
	relPath, carried := payloadPaths[blobID]
	if !carried {
		return nil, fmt.Errorf("the migration stream carries the metadata of blob %s, whose content is a file, but not the file", blobID)
	}
	return store.RepointPayload(record.Value, relPath)
}

// writeMigratedKeys writes a migration stream of a bucket laid out as layout
// says under [slot][database id][id] — its payload files, then its keys in bounded batches —
// and returns the state document it carried. The whole stream is read and
// checked first, so a stream refused writes nothing.
func (s *Server) writeMigratedKeys(stream []byte, layout migratedLayout, slot uint16, databaseID, id uint64) (store.BucketState, error) {
	var state store.BucketState
	records, err := readMigrateStream(stream, layout)
	if err != nil {
		return state, err
	}
	if _, err := checkMigratedRecords(records); err != nil {
		return state, err
	}
	payloadPaths, err := s.writeMigratedPayloads(records, slot, id)
	if err != nil {
		return state, err
	}
	stateKey := []byte{storage.NSState}
	batch := s.kv.NewBatch()
	defer func() { batch.Discard() }()
	pending := 0
	for _, record := range records {
		if record.BlobID != "" {
			continue
		}
		if bytes.Equal(record.Key, stateKey) {
			if err := bson.Unmarshal(record.Value, &state); err != nil {
				return state, fmt.Errorf("decoding the migrated state document: %w", err)
			}
			continue
		}
		value, err := migratedValue(record, payloadPaths)
		if err != nil {
			return state, err
		}
		key := append(storage.Scope(slot, databaseID, id), record.Key...)
		if err := batch.Set(key, value); err != nil {
			return state, fmt.Errorf("writing migrated key %x: %w", key, err)
		}
		pending += len(key) + len(value)
		if pending >= migrateBatchBytes {
			if err := batch.Commit(false); err != nil {
				return state, fmt.Errorf("committing migrated keys: %w", err)
			}
			batch = s.kv.NewBatch()
			pending = 0
		}
	}
	if err := batch.Commit(false); err != nil {
		return state, fmt.Errorf("committing migrated keys: %w", err)
	}
	return state, nil
}

func chunkBytes(b []byte, size int) [][]byte {
	if len(b) == 0 {
		return nil
	}
	var out [][]byte
	for len(b) > 0 {
		n := size
		if n > len(b) {
			n = len(b)
		}
		out = append(out, b[:n])
		b = b[n:]
	}
	return out
}

// MigrateRequest is the operator-facing trigger to pull a bucket onto this node
// (the ring owner) so it can serve it.
type MigrateRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
}

// MigrateResponse reports the outcome of the operator trigger.
type MigrateResponse struct {
	Success bool   `bson:"success"`
	Message string `bson:"message,omitempty"`
	Error   string `bson:"error,omitempty"`
	// DroppedIncarnation is set on a refusal because the copy the asker would
	// hand over is of an incarnation this node dropped: the asker drops its
	// copy of exactly that incarnation (R8-C).
	DroppedIncarnation uint64 `bson:"dropped_incarnation,omitempty"`
	// HandOffTaken is set on a refusal of a peer's ask by the bucket's owner
	// when it recorded the ask as a removed node's hand-off (noteHandedOff):
	// the name stays awaited there until its copy lands. A removed node counts
	// only such a refusal as named (BUG-0698).
	HandOffTaken bool `bson:"handoff_taken,omitempty"`
}

// handOffTakenError is the owner's refusal of a peer's ask that it recorded
// as a removed node's hand-off (ensureMigrated, noteHandedOff): the bucket is
// awaited here, and the copy has not landed within the wait.
type handOffTakenError struct{ err error }

func (e *handOffTakenError) Error() string { return e.err.Error() }

func (e *handOffTakenError) Unwrap() error { return e.err }

// errAnotherIncarnationKept is a holder's refusal of a peer's request to
// migrate a bucket it holds in another incarnation than its owner's.
var errAnotherIncarnationKept = errors.New("this node holds another incarnation of the bucket and keeps it")

type peerHandOffCtxKey struct{}

// withPeerHandOff marks a migrate that another cluster member asked for — a
// hand-off's wake-up, not an operator's command.
func withPeerHandOff(ctx context.Context) context.Context {
	return context.WithValue(ctx, peerHandOffCtxKey{}, true)
}

// askedByAPeer reports whether withPeerHandOff marked ctx. Ruling 53 (ii)
// lets an operator's migrate replace a holder's copy of another incarnation
// with the owner's: choosing which of two buckets of one name survives is the
// operator's exit. A peer's hand-off makes no such choice (I-H1): asked by a
// peer, the holder keeps its copy and the asker's hand-off waits, keeping its
// own copy too.
func askedByAPeer(ctx context.Context) bool {
	marked, _ := ctx.Value(peerHandOffCtxKey{}).(bool)
	return marked
}

type operatorMigrateCtxKey struct{}

// operatorMigrate reports whether ctx is an operator's migrate verb's, which
// the pulls it makes say to their source (MigrateStartRequest.Operator).
func operatorMigrate(ctx context.Context) bool {
	marked, _ := ctx.Value(operatorMigrateCtxKey{}).(bool)
	return marked
}

// handleMigrate is the operator's `migrate <database> <bucket>` verb. It
// must run on the node the ring currently owns the bucket for; that node pulls
// the bucket from a peer that still holds it and serves it. The join fence
// (which refused writes until now) lifts automatically once the store exists.
func handleMigrate(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req MigrateRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorFrame(frame, err), nil
	}
	if s.cluster == nil {
		return errorFrame(frame, errors.New("replication is not enabled on this node; a standalone node has no bucket to migrate")), nil
	}
	// ensureMigrated returns nil both when it pulled the bucket and when this
	// node already held it, and the two replies must not read alike: asked
	// before the call, the answer is what the operator's verb is told.
	alreadyHeld := s.bucketStoreOpen(req.Database, req.Bucket)
	if _, diverged := s.cluster.divergenceNaming(req.Database, req.Bucket); diverged {
		alreadyHeld = false
	}
	termBefore, _, err := s.bucketLineage(req.Database, req.Bucket)
	if err != nil {
		return errorResponse(frame, fmt.Errorf("reading the term of bucket %q in database %q before migrating it: %w", req.Bucket, req.Database, err))
	}
	sess, sessionKnown := transport.SessionFromContext(ctx)
	peer := sessionKnown && clusterMemberSession(sess)
	if peer {
		ctx = withPeerHandOff(ctx)
	} else {
		ctx = context.WithValue(ctx, operatorMigrateCtxKey{}, true)
	}
	// An operator's migrate of a spread collection is refused (N10): its
	// parts move with the rebalance. A peer's ask is a hand-off of the
	// template, an ordinary bucket, and is served.
	if !peer && s.spreadTemplateHeld(req.Database, req.Bucket) {
		return errorFrame(frame, migrateOfSpreadRefusal(req.Database, req.Bucket)), nil
	}
	// A refusal is answered, never returned as the handler's error: the
	// transport answers a handler error INTERNAL, without its frame, and closes
	// the connection (BUG-0507). A pull of an incarnation this node dropped is
	// an answer the asking holder acts on — it drops its copy of that
	// incarnation (R8-C) — so it travels in the MigrateResponse, as does a
	// peer's ask the owner took as a hand-off, which a removed node counts as
	// named (BUG-0698); every other refusal is an error frame with codeFor's
	// code, UNAVAILABLE for a hand-off in progress, which a caller retries.
	if err := s.cluster.ensureMigrated(ctx, req.Database, req.Bucket); err != nil {
		var droppedHere *pullOfDroppedIncarnationError
		if errors.As(err, &droppedHere) {
			return nodeResponse(transport.OpMigrate, frame, MigrateResponse{
				DroppedIncarnation: droppedHere.incarnation,
				Error:              fmt.Sprintf("bucket %q in %q: incarnation %d was dropped here; drop your copy", req.Bucket, req.Database, droppedHere.incarnation),
			}, nil)
		}
		var taken *handOffTakenError
		if errors.As(err, &taken) {
			return nodeResponse(transport.OpMigrate, frame, MigrateResponse{Error: explainError(err), HandOffTaken: true}, nil)
		}
		return errorResponse(frame, err)
	}
	message := fmt.Sprintf("bucket %q in %q is now served by %s", req.Bucket, req.Database, s.cluster.self.ID)
	termAfter, _, err := s.bucketLineage(req.Database, req.Bucket)
	if err != nil {
		return errorResponse(frame, fmt.Errorf("reading the term of bucket %q in database %q after migrating it: %w", req.Bucket, req.Database, err))
	}
	switch {
	case alreadyHeld && termAfter != termBefore:
		// The copy took its owner's term (followOwnersTerm): it now follows
		// the owner, which "already holds" would not say.
		message = fmt.Sprintf("bucket %q in %q on %s now follows its owner's term %d", req.Bucket, req.Database, s.cluster.self.ID, termAfter)
	case alreadyHeld:
		message = s.cluster.alreadyHeldReply(ctx, req.Database, req.Bucket)
	}
	return nodeResponse(transport.OpMigrate, frame, MigrateResponse{Success: true, Message: message}, nil)
}

// alreadyHeldReply is an operator's migrate's reply on a node that already
// holds the bucket and whose copy did not change: whether the copy follows its
// owner's term, when that can be told, and how to find and repair a replica
// that is behind. A peer's hand-off is answered without asking the owner.
func (c *Cluster) alreadyHeldReply(ctx context.Context, database, bucket string) string {
	if askedByAPeer(ctx) {
		return migrateAlreadyHeldMessage(database, bucket, c.self.ID)
	}
	beside, err := c.ownersCopyBeside(ctx, placementOf(database, bucket), database, bucket)
	if err != nil {
		// The migrate succeeded; only the reply's detail is lost.
		c.logger.Debug("could not ask the owner whether this node's copy follows it; migrate answers without it",
			"database", database, "bucket", bucket, "error", err)
		return migrateAlreadyHeldMessage(database, bucket, c.self.ID)
	}
	if !beside.comparable || beside.ownerCopy.Term != beside.own.Term {
		return migrateAlreadyHeldMessage(database, bucket, c.self.ID)
	}
	return fmt.Sprintf("bucket %q in %q on %s already follows its owner's term %d; nothing to do. %s",
		bucket, database, c.self.ID, beside.own.Term, replicaBehindAdvice(database, bucket))
}

// migrateAlreadyHeldMessage is migrate's reply on a node that already holds the
// bucket when nothing tells whether it follows its owner: nothing was pulled,
// nothing was compared, and the repair for a replica that is behind is the
// one the replication page gives (empty the node's store and log, then migrate
// onto it).
func migrateAlreadyHeldMessage(database, bucket, node string) string {
	return fmt.Sprintf("bucket %q in %q: %s already holds this bucket, so nothing was pulled. "+
		"migrate copies a bucket onto a node that does not have it, and brings a copy in another term than its owner's in line; "+
		"it does not repair a copy that is only behind. %s",
		bucket, database, node, replicaBehindAdvice(database, bucket))
}

// replicaBehindAdvice is how to find out whether a replica in its owner's
// term is behind, and how to repair one that is.
func replicaBehindAdvice(database, bucket string) string {
	return fmt.Sprintf("To find out whether this copy is behind, compare 'count' on each node. To repair a replica that is, stop it, "+
		"delete the 'store' and 'wal' entries in its data directory (keep 'keys', 'cluster', 'cluster.token', 'membership.json' "+
		"and 'users.json'), start it, and run 'migrate %s %s' on it again", database, bucket)
}

// ensureMigratedHook, when set, runs first in ensureMigrated on node nodeID;
// an error it returns is ensureMigrated's answer, in place of the pull. A test
// makes a migrate refuse with the refusal it needs. Nil in production.
var ensureMigratedHook atomic.Pointer[func(nodeID, database, bucket string) error]

// ensureMigrated makes this node serve a bucket it owns on the ring but does
// not yet hold, pulling it from the first peer that still has a copy. It is a
// no-op when the bucket is already served. A notify storm can call it for the
// same bucket concurrently, so only one pull runs per bucket; the others treat
// the in-flight pull as success (it finishes the job).
func (c *Cluster) ensureMigrated(ctx context.Context, database, bucket string) error {
	if hook := ensureMigratedHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, database, bucket); err != nil {
			return err
		}
	}
	if c.removed() {
		return fmt.Errorf("%w: bucket %q in database %q; run 'migrate' on a member", errRemovedNodeDoesNotPull, bucket, database)
	}
	placement := placementOf(database, bucket)
	if c.s.bucketStoreOpen(database, bucket) {
		// Membership before the already-held shortcut: a node that holds a copy
		// it is no longer placed on (its registry is accurate for what it holds)
		// is refused rather than told the migration worked.
		if err := c.refuseMigrateFromOutsideTheSet(placement, bucket); err != nil {
			return err
		}
		// A copy that diverged from its owner is not "already held": migrate
		// is its remedy (R8-E, review M-6).
		if record, diverged := c.divergenceNaming(database, bucket); diverged {
			if record.Reason == refusalAnotherIncarnation && askedByAPeer(ctx) {
				return fmt.Errorf("%w: bucket %q in database %q on %s is another incarnation of the name than its owner's, and this node keeps it: "+
					"only 'migrate %s %s' run on %s by an operator replaces it with the owner's",
					errAnotherIncarnationKept, bucket, database, c.self.ID, database, bucket, c.self.ID)
			}
			return c.reseedDiverged(ctx, record)
		}
		if c.placementMarksOf(placement).Outside != 0 {
			return c.copyMarkedCopyAfresh(ctx, placement, database, bucket)
		}
		return c.followOwnersTerm(ctx, placement, database, bucket)
	}
	// Whether this node belongs is the source's to say, not this node's: a node
	// that never held the bucket has no registry entry for it and computes the
	// set at its own default, which excludes it when the bucket is declared
	// above that default and includes it when below. The owner is the same node
	// at every factor, so it is a right source in both cases, and it refuses a
	// puller its placement excludes (refuseAPullerOutsideTheSet).
	replicas, err := c.Replicas(placement)
	if err != nil {
		return fmt.Errorf("placing bucket %q in database %q to migrate it: %w", bucket, database, err)
	}
	// This node is the ring's owner and holds nothing: the take-over worker
	// copies the bucket in from the holder of its most complete copy, never
	// from the first member that holds one, and opens it. A caller
	// — a holder's hand-off, an operator's migrate — gives it leave to copy
	// with auto_rebalance off (A-10.6) and waits for it.
	if replicas[0].ID == c.self.ID {
		// A peer's ask for a name a removed node owned is that node's
		// hand-off: the bucket is awaited until its take-over ends (BUG-0688).
		recorded := askedByAPeer(ctx) && c.noteHandedOff(placement)
		c.giveCopyLeave(database, bucket)
		err := c.awaitOwnCopy(ctx, placement, database, bucket)
		switch {
		case err == nil:
			c.forgetHandedOffOf(placement)
			return nil
		case recorded:
			// Only a refusal of a hand-off recorded here says the bucket is
			// awaited. A peer's ask that reaches a node not yet on the epoch
			// that makes it the owner is refused below, and one for a name no
			// removed node owned, or whose node is confirmed gone, records
			// nothing (BUG-0698).
			return &handOffTakenError{err: err}
		}
		return err
	}
	key := migrateKey(database, bucket)
	// One pull of a bucket runs at a time on this node. A caller that finds one
	// in flight waits for its real outcome rather than reporting a success that
	// has not happened yet; if it failed, the caller registers before it pulls
	// itself. Every waiter pulling at once, unregistered, made two pulls record
	// the bucket's one snapshot position, and the one that failed forgot the
	// position the other had recorded.
	for {
		c.migrateMu.Lock()
		done, busy := c.migrateActive[key]
		if !busy {
			done = make(chan struct{})
			c.migrateActive[key] = done
			c.migrateMu.Unlock()
			defer func() {
				c.migrateMu.Lock()
				delete(c.migrateActive, key)
				close(done)
				c.migrateMu.Unlock()
			}()
			break
		}
		c.migrateMu.Unlock()
		if hook := waitForInFlightPullHook.Load(); hook != nil {
			(*hook)(database, bucket)
		}
		select {
		case <-done:
			if c.s.bucketStoreOpen(database, bucket) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// A non-owner replica being backfilled while the owner keeps serving
	// writes: only the owner's snapshot carries a position in the stream
	// this node is then caught up from. A copy pulled from another holder
	// would be positioned in that holder's sequence space, and the owner
	// would still see this node at zero.
	owner := replicas[0]
	if err := c.pullBucketPlaced(ctx, database, bucket, owner, true); err != nil {
		if errors.Is(err, ErrMigrateBucketNotFound) {
			return fmt.Errorf("seeding this replica of bucket %q: its owner %s does not hold the bucket yet (it is rebalancing too); "+
				"the sweep retries, or run 'migrate %s %s' on %s first: %w", bucket, owner.ID, database, bucket, owner.ID, err)
		}
		return fmt.Errorf("seeding this replica of bucket %q from its owner %s: %w", bucket, owner.ID, err)
	}
	return nil
}

// claimPull registers a pull of database/bucket as ensureMigrated does, and
// reports false when one is in flight: a migrate that finds this one waits for
// its outcome, as it waits for another migrate's. release ends the
// registration and wakes those waiters.
func (c *Cluster) claimPull(database, bucket string) (release func(), claimed bool) {
	key := migrateKey(database, bucket)
	c.migrateMu.Lock()
	defer c.migrateMu.Unlock()
	if _, busy := c.migrateActive[key]; busy {
		return nil, false
	}
	done := make(chan struct{})
	c.migrateActive[key] = done
	return func() {
		c.migrateMu.Lock()
		delete(c.migrateActive, key)
		close(done)
		c.migrateMu.Unlock()
	}, true
}

// followOwnersTerm is migrate on a member that holds its owner's incarnation
// in another term than the owner's opened copy. A replay of an earlier
// owner's log can deliver a create after the change that moved its bucket,
// and that copy carries no placement mark, so nothing names it to the worker
// (BUG-0545). An operator's migrate names it, with leave to copy it afresh,
// and waits until the copy is in the owner's term: the worker accepts the
// owner's term when the owner's base is the copy's content, and otherwise
// drops the copy and pulls a fresh one from the owner. Any other copy is left
// as it is.
func (c *Cluster) followOwnersTerm(ctx context.Context, key placementKey, database, bucket string) error {
	// A peer's hand-off asks every holder to migrate; only an operator's
	// migrate pays the question to the owner, so a sweep costs what it did.
	if askedByAPeer(ctx) {
		return nil
	}
	beside, err := c.ownersCopyBeside(ctx, key, database, bucket)
	if err != nil {
		return err
	}
	if !beside.comparable || beside.ownerCopy.Term == beside.own.Term {
		return nil
	}
	c.giveCopyLeave(database, bucket)
	c.wakeTakeOver(database, bucket)
	ref := ReplBucketRef{Database: database, Bucket: bucket}
	deadline := time.Now().Add(c.wcTimeout)
	for {
		own, err := c.answerCopy(ref)
		if err != nil {
			return err
		}
		if own.Holds && own.Term == beside.ownerCopy.Term {
			return nil
		}
		if time.Now().After(deadline) {
			return &sentencedError{cause: ErrHandoffInProgress, message: fmt.Sprintf(
				"bucket %q in database %q on %s is in term %d and its owner %s is in term %d; this node is taking its owner's term. "+
					"Run 'migrate %s %s' on %s again in a moment",
				bucket, database, c.self.ID, own.Term, beside.owner.ID, beside.ownerCopy.Term, database, bucket, c.self.ID)}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for this node's copy of bucket %q in database %q to take its owner %s's term: %w",
				bucket, database, beside.owner.ID, ctx.Err())
		case <-time.After(takeOverWaitStep):
		}
	}
}

// copyMarkedCopyAfresh is migrate on a member whose copy is marked outside
// although this node is in the bucket's set (refuseMigrateFromOutsideTheSet
// has passed): a copy afresh that auto_rebalance off left to an operator
// (Cluster.copyAfresh, A-10.6), as after an increase that re-placed a demoted
// copy (spec §5.3 row 3) or a membership change that placed a stale copy
// back. The mark refuses the copy's reads and entries, and the term it shares
// with its owner says nothing of what it lacks, so "already held" is not an
// answer (BUG-0590). An operator's migrate gives the worker leave to copy it
// afresh, wakes it, and waits until the copy is open without the mark. A
// peer's hand-off is answered as before, so a sweep costs what it did.
func (c *Cluster) copyMarkedCopyAfresh(ctx context.Context, key placementKey, database, bucket string) error {
	if askedByAPeer(ctx) {
		return nil
	}
	c.giveCopyLeave(database, bucket)
	c.wakeTakeOver(database, bucket)
	deadline := time.Now().Add(c.wcTimeout)
	for {
		if c.s.bucketStoreOpen(database, bucket) && c.placementMarksOf(key).Outside == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return &sentencedError{cause: ErrHandoffInProgress, message: fmt.Sprintf(
				"bucket %q in database %q on %s is marked outside its set and is being copied afresh from its owner. "+
					"Run 'migrate %s %s' on %s again in a moment",
				bucket, database, c.self.ID, database, bucket, c.self.ID)}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for this node's copy of bucket %q in database %q to be copied afresh from its owner: %w",
				bucket, database, ctx.Err())
		case <-time.After(takeOverWaitStep):
		}
	}
}

// besideOwner is this node's copy of a bucket and its owner's, as migrate
// compares them. comparable is false when this node owns the bucket, either
// copy is missing, the owner's is not opened or is moved, or the two are
// different incarnations: then the terms say nothing about following.
type besideOwner struct {
	own, ownerCopy ReplCopy
	owner          ring.Node
	comparable     bool
}

// ownersCopyBeside reads this node's copy of the bucket and asks its owner
// for the owner's: one question, asked only on an operator's migrate.
func (c *Cluster) ownersCopyBeside(ctx context.Context, key placementKey, database, bucket string) (besideOwner, error) {
	replicas, err := c.Replicas(key)
	if err != nil {
		return besideOwner{}, fmt.Errorf("placing bucket %q in database %q to migrate it: %w", bucket, database, err)
	}
	beside := besideOwner{owner: replicas[0]}
	if beside.owner.ID == c.self.ID {
		return beside, nil
	}
	ref := ReplBucketRef{Database: database, Bucket: bucket}
	if beside.own, err = c.answerCopy(ref); err != nil {
		return besideOwner{}, err
	}
	copies, err := c.askCopies(ctx, beside.owner, []ReplBucketRef{ref})
	if err != nil {
		return besideOwner{}, fmt.Errorf("asking the owner %s which term its copy of bucket %q in database %q is in: %w",
			beside.owner.ID, bucket, database, err)
	}
	beside.ownerCopy = copies[0]
	beside.comparable = beside.own.Holds && beside.ownerCopy.Holds && beside.ownerCopy.Opened && beside.ownerCopy.Moved == 0 &&
		beside.ownerCopy.Incarnation == beside.own.Incarnation
	return beside, nil
}

// refuseMigrateFromOutsideTheSet refuses a migrate on a node that holds a copy
// of the bucket but is not placed on it, naming the set. It is asked only of a
// bucket this node holds: a node that holds nothing has no factor registered,
// and its answer would be a guess.
func (c *Cluster) refuseMigrateFromOutsideTheSet(key placementKey, bucket string) error {
	placed, err := c.Replicas(key)
	if err != nil {
		return err
	}
	for _, node := range placed {
		if node.ID == c.self.ID {
			return nil
		}
	}
	return fmt.Errorf("bucket %q is not served by this node: it is not in the bucket's replica set %v (%s owns it); run 'migrate' on a member of that set",
		bucket, nodeIDs(placed), placed[0].ID)
}

// memberByID returns the current member with the given identity.
func (c *Cluster) memberByID(id string) (ring.Node, bool) {
	for _, m := range c.Members() {
		if m.ID == id {
			return m, true
		}
	}
	return ring.Node{}, false
}

// stillReplica reports whether this node is in the current replica set of the
// bucket placed by key, i.e. whether a migrated copy should be kept rather than
// deleted.
func (c *Cluster) stillReplica(key placementKey) bool {
	replicas, err := c.Replicas(key)
	if err != nil {
		return false
	}
	for _, r := range replicas {
		if r.ID == c.self.ID {
			return true
		}
	}
	return false
}

// sweep rebalances this node's known buckets after a membership change: any
// bucket this node owns but does not hold is pulled, and any bucket this node
// holds whose ring owner is a peer is reported to that owner (which pulls it
// itself — only the owner's store becomes the serving copy). Idempotent: a
// bucket already served by its owner is a no-op, so it is safe to run on every
// node and repeatedly.
func (c *Cluster) sweep(ctx context.Context) {
	c.sweepPass(ctx, sweepRound{}, nil)
}

// sweepRound is the sweep generation a pass belongs to and the epoch whose
// change it hands off. The zero round is a pass nobody reports on.
type sweepRound struct {
	generation uint64
	epoch      uint64
	// dropMark is the drop serial when the round began (beginRound): the
	// names dropped after it are not the round's to write down.
	dropMark uint64
}

// beginRound registers round as running, under sweepLeftMu, and returns it
// stamped with the drop serial of that moment. Every beginRound is paired with
// an endRound once the round has recorded what it left. A rearm takes its stamp
// inside the lock that snapshots its list (rearmSweep), so no drop falls
// between the list and the stamp.
func (c *Cluster) beginRound(round sweepRound) sweepRound {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	return c.beginRoundLocked(round)
}

func (c *Cluster) beginRoundLocked(round sweepRound) sweepRound {
	c.roundsActive++
	round.dropMark = c.dropSerial
	return round
}

// endRound is the end of a round beginRound registered; with no round left it
// forgets the dropped names, which are kept only for the rounds that are older
// than the drop.
func (c *Cluster) endRound() {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	c.roundsActive--
	if c.roundsActive == 0 {
		c.droppedInRound = nil
	}
}

// droppedNote is what a round keeps of one drop: its serial, and the catalog id
// of the bucket it dropped, the identity of that incarnation of the name. Ids
// are never reused (commitBucketCreate), so a bucket attached under the name
// later has another id.
type droppedNote struct {
	serial   uint64
	bucketID uint64
}

// noteDroppedInRound records, under sweepLeftMu, that a drop took the bucket
// with catalog id bucketID, named held, off the sweep's lists, for the rounds
// running now.
func (c *Cluster) noteDroppedInRound(held heldBucket, bucketID uint64) {
	c.dropSerial++
	if c.roundsActive == 0 {
		return
	}
	if c.droppedInRound == nil {
		c.droppedInRound = make(map[heldBucket]droppedNote)
	}
	c.droppedInRound[held] = droppedNote{serial: c.dropSerial, bucketID: bucketID}
}

// noteAttachedInRound is a bucket with catalog id bucketID attaching under
// held on this node. When a round noted a drop of that name for another id, the
// name stands for a live incarnation again: the note comes off, so the round
// keeps the name in its lists (BUG-0662). With no round running it costs a lock
// and a map lookup on nil. A drop and an attach of one name are serialised by the
// bucket's fence (a drop and a create hold it exclusively, a pull's materialise
// shared), so the attach never falls between the drop's unlink and its note.
// A catalogued entry re-attached by a retried create already had its note taken
// off by the create that committed it, which calls this before its attach.
func (c *Cluster) noteAttachedInRound(held heldBucket, bucketID uint64) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if note, dropped := c.droppedInRound[held]; dropped && note.bucketID != bucketID {
		delete(c.droppedInRound, held)
	}
}

// withoutDroppedSince is list less the names a drop took off the sweep's lists
// after round began, under sweepLeftMu. It costs O(list) only while a drop
// landed in the round, and returns list itself otherwise.
func (c *Cluster) withoutDroppedSince(round sweepRound, list []heldBucket) []heldBucket {
	if len(c.droppedInRound) == 0 {
		return list
	}
	kept := make([]heldBucket, 0, len(list))
	for _, held := range list {
		if note, dropped := c.droppedInRound[held]; !dropped || note.serial <= round.dropMark {
			kept = append(kept, held)
		}
	}
	return kept
}

// sweepPass is one pass of the sweep over the buckets this node holds, or over
// only `only` when the caller is retrying, and returns the buckets it could not
// settle: a hand-off some member did not confirm, a pull that failed. It
// reports its progress for the round (recordHandOff): a full pass counts the
// buckets the change moved off this node, every pass counts those it settled
// and names the members its hand-offs wait on.
func (c *Cluster) sweepPass(ctx context.Context, round sweepRound, only []heldBucket) []heldBucket {
	var (
		unsettled   []heldBucket
		total, done uint64
		waitingOn   []string
		// outsideWaiting counts the copies outside their sets that a member
		// does not cover yet, and outsideWaitingOn names those members.
		outsideWaiting   int
		outsideWaitingOn []string
	)
	full := only == nil
	refused := &refusedSources{bySource: map[string]*sourceRefusal{}}
	ctx = context.WithValue(ctx, refusedSourcesCtxKey{}, refused)
	defer refused.logSkips(c.logger)
	settle := func(database, bucket string) {
		// A copy a decrease demoted is released here like any copy outside
		// its set, but its wait is none of the round's: the full walk lists it
		// with every held bucket, and in the residue it would keep the node
		// from settling and hold the join freeze for as long as a survivor is
		// down (spec §8.3 point 2). Filtered here, every caller of the pass is
		// covered: the full walk, its retries and the rearm.
		if held := (heldBucket{database: database, bucket: bucket}); c.stillDemoted(held) {
			c.releaseDemoted(ctx, held)
			return
		}
		counted := c.movedOffThisNode(placementOf(database, bucket), round.epoch)
		// Listed by the catalog walk, the bucket was held when the pass
		// began; one released since (handleMigrateDone) settles as done.
		if full && counted {
			total++
		}
		if err := c.migrateIfMissing(ctx, database, bucket); err != nil {
			unsettled = append(unsettled, heldBucket{database: database, bucket: bucket})
			var unconfirmed *handOffUnconfirmedError
			if errors.As(err, &unconfirmed) {
				outside := !c.Hosts(placementOf(database, bucket))
				if outside {
					outsideWaiting++
				}
				for _, member := range unconfirmed.members {
					if !slices.Contains(waitingOn, member) {
						waitingOn = append(waitingOn, member)
					}
					if outside && !slices.Contains(outsideWaitingOn, member) {
						outsideWaitingOn = append(outsideWaitingOn, member)
					}
				}
			}
			return
		}
		c.dropOrphan(heldBucket{database: database, bucket: bucket})
		if counted {
			done++
		}
	}
	if full {
		c.noteWalkBegun(round.epoch)
		c.forEachKnownBucket(settle)
	} else {
		for _, held := range only {
			settle(held.database, held.bucket)
		}
	}
	slices.Sort(waitingOn)
	slices.Sort(outsideWaitingOn)
	c.noteReleaseWait(outsideWaiting, outsideWaitingOn, time.Now())
	c.recordHandOff(round, full, total, done, waitingOn)
	c.recomputeJoinFreezeForCounts()
	c.atSweepStage(round.epoch, passFinished)
	return unsettled
}

// movedOffThisNode reports whether this node owned the bucket placed by key at
// the epoch before epoch and the change to epoch altered its replica set: each
// bucket a change moves is counted once, by its previous owner. False when
// either ring is not in the history: such a node reports no counts.
func (c *Cluster) movedOffThisNode(key placementKey, epoch uint64) bool {
	before, recorded := c.ringAt(epoch - 1)
	if !recorded {
		return false
	}
	after, recorded := c.ringAt(epoch)
	if !recorded {
		return false
	}
	factor := c.effectiveRF(key)
	oldSet, err := before.Replicas(key.encoded, factor)
	if err != nil || len(oldSet) == 0 || oldSet[0].ID != c.self.ID {
		return false
	}
	newSet, err := after.Replicas(key.encoded, factor)
	if err != nil {
		return false
	}
	return !slices.Equal(nodeIDs(oldSet), nodeIDs(newSet))
}

// recordHandOff publishes a pass's progress as this node's hand-off of the
// round's change, if the round is still the newest generation: a full pass
// starts the count, a retry adds what it settled; both replace the members
// waited on with the ones this pass met. A node that does not remember the
// membership before the change counts nothing and publishes nothing.
func (c *Cluster) recordHandOff(round sweepRound, full bool, total, done uint64, waitingOn []string) {
	if round.generation == 0 {
		return
	}
	if _, recorded := c.ringAt(round.epoch - 1); !recorded {
		return
	}
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if round.generation != c.sweepGeneration.Load() {
		return
	}
	if full || c.handOff == nil || c.handOff.Epoch != round.epoch {
		// The moved buckets a puller took off this node before the walk began
		// are not in the catalog it walked: they are counted, and done.
		var released uint64
		if full && c.released.epoch == round.epoch {
			released = c.released.count
		}
		c.handOff = &HandOffProgress{Epoch: round.epoch, Total: total + released, Done: released}
	}
	c.handOff.Done = min(c.handOff.Done+done, c.handOff.Total)
	c.handOff.WaitingOn = waitingOn
	c.handOff.GaveUp = false
}

// releaseCount is how many buckets of one epoch's change a puller took off
// this node before its sweep of that epoch walked its catalog.
type releaseCount struct {
	epoch, walked, count uint64
}

// noteWalkBegun records that a full pass of epoch is walking the catalog: a
// bucket released from now on is in the walk's list, and counted there.
func (c *Cluster) noteWalkBegun(epoch uint64) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if c.released.epoch != epoch {
		c.released = releaseCount{epoch: epoch}
	}
	c.released.walked = epoch
}

// noteReleased counts a bucket this node released (releaseOutsideSet), when
// this node counts it — it owned the bucket before the change — and no walk of
// the change has listed it yet.
func (c *Cluster) noteReleased(key placementKey) {
	epoch := c.Epoch()
	if !c.movedOffThisNode(key, epoch) {
		return
	}
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if c.released.epoch != epoch {
		c.released = releaseCount{epoch: epoch}
	}
	if c.released.walked != epoch {
		c.released.count++
	}
}

// noteGaveUp marks this node's hand-off of the round's change as given up for
// now, if the round is still the newest.
func (c *Cluster) noteGaveUp(round sweepRound) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if round.generation == c.sweepGeneration.Load() && c.handOff != nil && c.handOff.Epoch == round.epoch {
		c.handOff.GaveUp = true
	}
}

// ownHandOff is a copy of this node's hand-off progress, nil when it has none.
func (c *Cluster) ownHandOff() *HandOffProgress {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if c.handOff == nil {
		return nil
	}
	progress := *c.handOff
	progress.WaitingOn = slices.Clone(progress.WaitingOn)
	progress.Unconfirmed = slices.Clone(progress.Unconfirmed)
	// An orphan's copies still to make, with auto-rebalance off, are listed
	// with the pass's: the operator makes them the same way.
	for _, held := range slices.SortedFunc(maps.Keys(c.orphanCopies), func(a, b heldBucket) int {
		return strings.Compare(a.database+"\x00"+a.bucket, b.database+"\x00"+b.bucket)
	}) {
		for _, copyToMake := range c.orphanCopies[held] {
			if len(progress.Unconfirmed) < unconfirmedListed {
				progress.Unconfirmed = append(progress.Unconfirmed, copyToMake)
			}
			progress.UnconfirmedCount++
		}
	}
	return &progress
}

// noteOrphanCopies records the copies an orphan's set members still lack,
// with auto-rebalance off; nil forgets them.
func (c *Cluster) noteOrphanCopies(key heldBucket, lacking []PlacementToCopy) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if len(lacking) == 0 {
		delete(c.orphanCopies, key)
		return
	}
	if c.orphanCopies == nil {
		c.orphanCopies = make(map[heldBucket][]PlacementToCopy)
	}
	c.orphanCopies[key] = lacking
}

// sweep retry: a hand-off is refused for reasons that pass -- a member that has
// not yet learned the new epoch refuses a puller its older placement excludes,
// a peer is briefly down -- and a sweep is launched once per adoption of an
// epoch, so a refusal that is only "not yet" used to be final: the displaced
// holder kept a copy that stopped receiving writes until its next restart (D-2).
// The refusal itself is right and stays as it is (a node outside the set must
// not be able to pull, and F-4's member that never held the bucket is still
// seeded by the source that knows the factor); what changes is that the sweep
// asks again, with a backoff, until the buckets it could not settle have.
//
// They are atomics because a test shortens them while the cluster's own sweep
// goroutines are running and reading them.
var (
	sweepRetryFloor   = newSweepBound(time.Second)
	sweepRetryCeiling = newSweepBound(15 * time.Second)
	sweepRetryGiveUp  = newSweepBound(10 * time.Minute)
)

// sweepBound is a duration read by background goroutines and set by tests.
type sweepBound struct{ nanoseconds atomic.Int64 }

func newSweepBound(d time.Duration) *sweepBound {
	bound := &sweepBound{}
	bound.nanoseconds.Store(int64(d))
	return bound
}

func (b *sweepBound) get() time.Duration  { return time.Duration(b.nanoseconds.Load()) }
func (b *sweepBound) set(d time.Duration) { b.nanoseconds.Store(int64(d)) }

// sweepStage names a point of a sweep a test can hold it at (sweepStageHook).
type sweepStage int

const (
	// sweepStarting is a full sweep before it takes its generation.
	sweepStarting sweepStage = iota
	// sweepPassing is a full sweep after it took its generation, before its
	// first pass.
	sweepPassing
	// sweepEnded is a full sweep or a rearm after it recorded what it left and
	// marked what it could.
	sweepEnded
	// rearmPassing is a re-armed sweep after it took its generation, before its
	// pass.
	rearmPassing
	// sweepNumbered is a full sweep (or confirm) right after it took its
	// generation and published it as the newest full sweep (claimFullSweep).
	sweepNumbered
	// passFinished is a pass of the sweep that has listed what it could not
	// settle and not yet handed the list to its caller (setRetrying or
	// recordResidue): the window a drop can land in unseen by the list.
	passFinished
	// residueRecording is a round that has finished retrying and not yet
	// recorded what it left (settleSweep): the window a drop can land in
	// after the retries' last write of the list (BUG-0659).
	residueRecording
	// rearmSnapshotted is a rearm right after it took the residue's list and
	// its round stamp under sweepLeftMu, before it starts its pass: a drop
	// landing here is newer than the stamp, so the rearm must filter it.
	rearmSnapshotted
)

// sweepStageHook, when set, runs at each sweepStage of a sweep on a node, with
// the epoch the sweep is for, and may block. Nil in production.
var sweepStageHook atomic.Pointer[func(nodeID string, epoch uint64, stage sweepStage)]

func (c *Cluster) atSweepStage(epoch uint64, stage sweepStage) {
	if hook := sweepStageHook.Load(); hook != nil {
		(*hook)(c.self.ID, epoch, stage)
	}
}

// sweepUntilSettled runs a sweep of epoch's change and retries the buckets it
// could not settle until they settle, a newer sweep supersedes this one, ctx
// ends, or the give-up bound passes (it is logged). It records what it left
// only if it is still the newest sweep generation, and marks the node settled
// at epoch only if it left nothing, is still the newest, and the node is still
// on epoch (join-freeze spec §8.1): a superseded sweep, or one that ended after
// another adoption, says nothing about the change it swept.
func (c *Cluster) sweepUntilSettled(ctx context.Context, epoch uint64) {
	c.atSweepStage(epoch, sweepStarting)
	generation := c.claimFullSweep(epoch)
	c.atSweepStage(epoch, sweepPassing)
	round := c.beginRound(sweepRound{generation: generation, epoch: epoch})
	defer c.endRound()
	left := c.retrySettling(ctx, round, c.sweepPass(ctx, round, nil))
	c.settleSweep(round, left)
}

// claimFullSweep takes a new sweep generation and publishes it as the newest
// full sweep in one step under sweepLeftMu, the lock rearmSweep checks its
// guard and takes its own generation under: as two steps, a rearm between
// them saw no full sweep running and superseded this one, whose pending
// buckets were then recorded nowhere (BUG-0196).
func (c *Cluster) claimFullSweep(epoch uint64) uint64 {
	c.sweepLeftMu.Lock()
	generation := c.sweepGeneration.Add(1)
	c.fullSweepGeneration.Store(generation)
	c.sweepLeftMu.Unlock()
	c.atSweepStage(epoch, sweepNumbered)
	return generation
}

// settleSweep records what a sweep or a rearm left of its round's change, with
// the members its last pass waited on, and marks the node settled at the
// round's epoch when that was nothing.
func (c *Cluster) settleSweep(round sweepRound, left []heldBucket) {
	var waitingOn []string
	if progress := c.ownHandOff(); progress != nil && progress.Epoch == round.epoch {
		waitingOn = progress.WaitingOn
	}
	c.atSweepStage(round.epoch, residueRecording)
	if kept, recorded := c.recordResidue(round, left, waitingOn); recorded && kept == 0 && c.Epoch() == round.epoch {
		c.markSettled(round.epoch)
	}
	c.maybeSweepTombstones()
	c.atSweepStage(round.epoch, sweepEnded)
}

// retrySettling asks again about pending buckets, with a backoff, until they
// settle or one of the ways out described on sweepUntilSettled.
func (c *Cluster) retrySettling(ctx context.Context, round sweepRound, pending []heldBucket) []heldBucket {
	began, wait := time.Now(), sweepRetryFloor.get()
	pending = c.setRetrying(round, pending)
	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			return pending
		case <-time.After(wait):
		}
		if c.sweepGeneration.Load() != round.generation {
			return pending
		}
		// A drop since the last pass took its name off the list (R8-C).
		if pending = c.takeRetrying(round.generation, pending); len(pending) == 0 {
			return nil
		}
		if time.Since(began) > sweepRetryGiveUp.get() {
			c.noteGaveUp(round)
			c.logger.Warn("rebalance gave up settling some buckets for now; it asks again while its members answer, "+
				"at the next membership change, and at the next start",
				"buckets", len(pending), "first_database", pending[0].database, "first_bucket", pending[0].bucket)
			return pending
		}
		pending = c.setRetrying(round, c.sweepPass(ctx, round, pending))
		wait = min(2*wait, sweepRetryCeiling.get())
	}
	return nil
}

// setRetrying records the list round's generation is retrying and returns it
// less the names a drop took off the lists since the round began: the pass
// that made it listed them before the drop, and written as it is the list would
// bring a dropped name back for the retries, and for the residue if they give up.
func (c *Cluster) setRetrying(round sweepRound, pending []heldBucket) []heldBucket {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	pending = c.withoutDroppedSince(round, pending)
	if round.generation >= c.retrying.generation {
		c.retrying = sweepResidue{generation: round.generation, list: slices.Clone(pending)}
	}
	return pending
}

// takeRetrying is the list generation is retrying as it stands now — pending,
// less any name a drop has taken off it since setRetrying.
func (c *Cluster) takeRetrying(generation uint64, pending []heldBucket) []heldBucket {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if c.retrying.generation != generation {
		return pending
	}
	return slices.Clone(c.retrying.list)
}

// migrateIfMissing rebalances one bucket after a membership change so every
// member of its replica set holds a copy: this node pulls what it serves
// (owner or replica) but lacks, and tells every other replica-set member to pull
// too. Backfilling replicas matters because a join re-keys the ring — an
// existing bucket can gain the empty newcomer as a replica, and a majority
// write would then stall waiting for a copy that backfill has not delivered.
// A holder outside the set then releases its copy (handOffBucket), so a node
// that re-enters the set later holds nothing and is seeded afresh.
func (c *Cluster) migrateIfMissing(ctx context.Context, database, bucket string) error {
	// A removed node neither pulls nor releases a copy (step 9.3).
	if c.removed() {
		return nil
	}
	replicas, err := c.Replicas(placementOf(database, bucket))
	if err != nil {
		return fmt.Errorf("placing bucket %q in database %q: %w", bucket, database, err)
	}
	var unsettled error
	// If we are a replica-set member and lack the bucket, pull it ourselves
	// (ensureMigrated serializes concurrent pulls).
	for _, r := range replicas {
		if r.ID != c.self.ID {
			continue
		}
		if !c.s.bucketStoreOpen(database, bucket) {
			if err := c.ensureMigrated(ctx, database, bucket); errors.Is(err, errNotPlacedHere) {
				// The source that knows the factor says this node is outside the
				// set. That settles it.
				c.logger.Debug("rebalance: the bucket's source says this node is not placed on it", "database", database, "bucket", bucket, "error", err)
			} else if err != nil {
				c.logger.Warn("rebalance could not migrate a bucket it serves", "database", database, "bucket", bucket, "error", err)
				unsettled = err
			}
		}
		break
	}

	// While we still hold a copy — even if a join has since dropped us from the
	// replica set — tell every current replica-set member to pull it too. A
	// holder that is no longer in the set is exactly the source an empty new
	// owner needs, and it releases its copy once every member holds one.
	if !c.s.bucketStoreOpen(database, bucket) {
		return unsettled
	}
	if err := c.handOffBucket(ctx, database, bucket, replicas); err != nil {
		if errors.Is(err, errHandOffUnconfirmed) {
			c.logger.Debug("rebalance could not notify every replica-set member", "database", database, "bucket", bucket, "error", err)
			return err
		}
		c.logger.Warn("rebalance could not release a bucket copy outside its replica set", "database", database, "bucket", bucket, "error", err)
		return err
	}
	return unsettled
}

// errHandOffUnconfirmed is a hand-off in which some member of the bucket's
// replica set did not answer that it serves the bucket, so this node keeps
// its copy.
var errHandOffUnconfirmed = errors.New("not every replica-set member confirmed it holds the bucket; this node keeps its copy")

// handOffUnconfirmedError is a hand-off some members of the set did not
// confirm: who they are, which a node publishes as the members its hand-off
// waits on (join-freeze spec §8.5), and why each refused. It matches
// errHandOffUnconfirmed.
type handOffUnconfirmedError struct {
	database, bucket string
	members          []string
	refusals         error
}

func (e *handOffUnconfirmedError) Error() string {
	return fmt.Sprintf("handing bucket %q in database %q to its replica set: %v: %v", e.bucket, e.database, errHandOffUnconfirmed, e.refusals)
}

func (e *handOffUnconfirmedError) Unwrap() []error {
	return []error{errHandOffUnconfirmed, e.refusals}
}

// errReplicaSetMoved is a release abandoned because the bucket's replica set
// changed after its members confirmed they hold it: the new set's members
// have not confirmed anything yet.
var errReplicaSetMoved = errors.New("the bucket's replica set changed after its members confirmed; this node keeps its copy until the new set confirms")

// handOffBucket settles this node's copy of a bucket after a membership
// change (step 10b.7). It asks every member of the set other than this node
// to migrate the bucket, owner first — the wake-up that seeds a member that
// lacks a copy from the owner, and a member that cannot take it yet is waited
// on — and then asks each what its copy is (askCopies): an answer of "it
// serves the bucket" is not enough, because a stale copy serves too.
//
// A copy in the set is settled once the owner's copy is opened and not moved
// and this node's copy follows the owner's term (R8-F). A copy outside the set
// is released (releaseOutsideSet) once the owner's copy is opened and not
// moved, every member of the set answers in the owner's term, and every
// member's copy covers this node's (R8-B, R8-G). Otherwise the copy is kept,
// and the members not yet covering it are the ones the hand-off waits on.
func (c *Cluster) handOffBucket(ctx context.Context, database, bucket string, replicas []ring.Node) error {
	// A removed node releases nothing (step 9.3).
	if c.removed() {
		return nil
	}
	// A stopping node reads and releases nothing more: Stop closes the node
	// store once the goroutines that call this have returned.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("handing off bucket %q in database %q: %w", bucket, database, err)
	}
	var (
		refusals    []error
		unconfirmed []string
	)
	for _, member := range replicas {
		if member.ID == c.self.ID {
			continue
		}
		if err := c.askNodeToMigrate(ctx, member, database, bucket); err != nil {
			// The member dropped the incarnation this node holds: the drop has
			// not reached this copy yet, and the hand-off delivers it.
			var droppedThere *droppedHereError
			if errors.As(err, &droppedThere) {
				deleted, dropErr := c.dropCopyDroppedElsewhere(ctx, database, bucket, droppedThere.incarnation)
				if dropErr == nil && deleted {
					return nil
				}
				err = errors.Join(err, dropErr)
			}
			refusals = append(refusals, fmt.Errorf("%s: %w", member.ID, err))
			unconfirmed = append(unconfirmed, member.ID)
		}
	}
	// A member that could not take the bucket has nothing to answer for yet.
	if len(refusals) > 0 {
		return &handOffUnconfirmedError{database: database, bucket: bucket, members: unconfirmed, refusals: errors.Join(refusals...)}
	}
	key := placementOf(database, bucket)
	answers, refusals, unanswered := c.askSetMembers(ctx, database, bucket, replicas)
	if len(unanswered) > 0 {
		return &handOffUnconfirmedError{database: database, bucket: bucket, members: unanswered, refusals: errors.Join(refusals...)}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("handing off bucket %q in database %q: %w", bucket, database, err)
	}
	owner := replicas[0]
	ownerCopy, err := c.copyOf(owner, database, bucket, answers)
	if err != nil {
		return fmt.Errorf("reading the owner's copy of bucket %q in database %q to hand it off: %w", bucket, database, err)
	}
	if !ownerCopy.Holds || !ownerCopy.Opened || ownerCopy.Moved != 0 {
		return &handOffUnconfirmedError{database: database, bucket: bucket, members: []string{owner.ID},
			refusals: fmt.Errorf("%s has not taken the bucket over yet", owner.ID)}
	}
	if slices.ContainsFunc(replicas, func(member ring.Node) bool { return member.ID == c.self.ID }) {
		own, err := c.answerCopy(ReplBucketRef{Database: database, Bucket: bucket})
		if err != nil {
			return fmt.Errorf("reading this node's copy of bucket %q in database %q to settle it: %w", bucket, database, err)
		}
		if own.Term != ownerCopy.Term || own.Moved != 0 || own.Outside != 0 {
			return &handOffUnconfirmedError{database: database, bucket: bucket, members: []string{c.self.ID},
				refusals: fmt.Errorf("this node's copy is in term %d and its owner %s is in term %d", own.Term, owner.ID, ownerCopy.Term)}
		}
		return nil
	}
	if marks := c.placementMarksOf(key); marks.Outside == 0 {
		// A copy outside its set with no mark was left by an earlier build:
		// it is marked before anything else, so its stream refuses it and the
		// listing says it is leaving (rev 7 step 10.7).
		if err := c.markOutside(key); err != nil {
			return err
		}
	}
	return c.releaseOutsideSet(database, bucket, replicas, answers)
}

// askSetMembers asks every member of replicas but this node what its copy of
// the bucket is, and returns the answers by member, why some did not answer,
// and who.
func (c *Cluster) askSetMembers(ctx context.Context, database, bucket string, replicas []ring.Node) (map[string]ReplCopy, []error, []string) {
	answers := make(map[string]ReplCopy, len(replicas))
	var refusals []error
	var unanswered []string
	for _, member := range replicas {
		if member.ID == c.self.ID {
			continue
		}
		answer, err := c.askOne(ctx, member, heldBucket{database: database, bucket: bucket}, c.wcTimeout)
		if err != nil {
			refusals = append(refusals, fmt.Errorf("%s: %w", member.ID, err))
			unanswered = append(unanswered, member.ID)
			continue
		}
		answers[member.ID] = answer
	}
	return answers, refusals, unanswered
}

// copyOf is member's copy of the bucket: this node's own, read under its
// fence, or member's answer.
func (c *Cluster) copyOf(member ring.Node, database, bucket string, answers map[string]ReplCopy) (ReplCopy, error) {
	if member.ID == c.self.ID {
		return c.answerCopy(ReplBucketRef{Database: database, Bucket: bucket})
	}
	return answers[member.ID], nil
}

// coversForRelease reports whether member's copy, answered in its owner's
// term, holds everything this node's copy outside the set holds that the
// bucket keeps (R8-B, R8-G): one incarnation, and in the copy's term at or
// past its position, or in a later term — which its owner opened from the
// best copy it could find, and whatever this copy holds past that term's base
// was given up then (spec §20.8.6's concession).
func coversForRelease(member, own ReplCopy) bool {
	if !member.Holds || member.Incarnation != own.Incarnation {
		return false
	}
	ownTerm, ownThrough := copyLineage(own.Term, own.Through, own.BaseTerm, own.BaseThrough)
	memberTerm, memberThrough := copyLineage(member.Term, member.Through, member.BaseTerm, member.BaseThrough)
	if memberTerm == ownTerm {
		return memberThrough >= ownThrough
	}
	return member.Term > own.Term
}

// noteConcession says, once per release, what a copy released under a later
// term's owner gives up: with its count when the owner's term was opened from
// this copy's term (spec §20.8.6; review m-3); without one when the owner's
// base is of a later term still, which says nothing of how far this copy ran;
// and with a lower bound when the owner's base is of an earlier term, whose
// lineage holds none of the entries of this copy's term (BUG-0475). It is
// called by every path that deletes a copy for a later term's: the release
// outside the replica set and a take-over that copies over its own copy.
func (c *Cluster) noteConcession(database, bucket string, own, owner ReplCopy) {
	ownTerm, ownThrough := copyLineage(own.Term, own.Through, own.BaseTerm, own.BaseThrough)
	switch {
	case owner.Term <= own.Term:
		return
	case owner.BaseTerm == ownTerm && ownThrough > owner.BaseThrough:
		c.logger.Warn(fmt.Sprintf("released a copy of bucket %q in database %q that held %d entries its owner took the bucket over without; "+
			"they were on nodes the take-over could not reach, and are given up", bucket, database, ownThrough-owner.BaseThrough),
			"database", database, "bucket", bucket, "entries", ownThrough-owner.BaseThrough)
	case owner.BaseTerm > ownTerm:
		c.logger.Warn(fmt.Sprintf("released a copy of bucket %q in database %q of an older term than its owner's; "+
			"anything it held past what the owner's term was opened from is given up", bucket, database),
			"database", database, "bucket", bucket, "copy_term", own.Term, "owner_term", owner.Term)
	case owner.BaseTerm < ownTerm && ownThrough > 0:
		c.logger.Warn(fmt.Sprintf("released a copy of bucket %q in database %q that held at least %d entries of term %d, which its owner's term "+
			"was not opened from (the owner's term was opened from term %d); they were on nodes the take-over could not reach, and are given up",
			bucket, database, ownThrough, ownTerm, owner.BaseTerm),
			"database", database, "bucket", bucket, "at_least_entries", ownThrough, "copy_term", ownTerm, "owner_base_term", owner.BaseTerm)
	}
}

// markLatePlacement records, for a copy made after the placement pass of the
// current epoch ran, the marks that pass would have recorded for it.
func (c *Cluster) markLatePlacement(key placementKey) error {
	earned, known := c.placementMarksAt(key, c.Epoch())
	if !known || earned == (placementMarks{}) {
		return nil
	}
	c.placementMu.Lock()
	existing := c.unsettled[key]
	c.placementMu.Unlock()
	marks := existing.merged(earned)
	if marks == existing {
		return nil
	}
	return c.writePlacementMarks(key, marks)
}

// writePlacementMarks records a bucket's marks, synced, and in memory, merged
// with any it has recorded since its caller read them: a mark once set is
// never overwritten (BUG-0589).
func (c *Cluster) writePlacementMarks(key placementKey, marks placementMarks) error {
	c.marksWriteMu.Lock()
	defer c.marksWriteMu.Unlock()
	marks = c.recordedMarks(key).merged(marks)
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := putPlacementMarks(batch, key, marks); err != nil {
		return err
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing placement marks: %w", err)
	}
	c.publishPlacementMarks(key, marks)
	return nil
}

// markOutside records an outside mark at the current epoch for a copy outside
// its set that has none.
func (c *Cluster) markOutside(key placementKey) error {
	c.placementMu.Lock()
	marks := c.unsettled[key]
	c.placementMu.Unlock()
	marks.Outside = c.Epoch()
	return c.writePlacementMarks(key, marks)
}

// releaseOutsideSet drops this node's copy of database/bucket, outside its
// set, once every member of the set covers it.
//
// It runs inside the bucket's exclusive fence: every write applying to the
// copy has finished, a snapshot of it (snapshotBucket, which takes the same
// fence) has either been captured or has not started, and the drop cancels
// any pull of the name on this node and forgets any snapshot of it this node
// is serving (DeleteBucketCtx → dropClearsPulls), so a puller that had not
// fetched it yet fails and retries from another holder. The set is re-read
// under the fence: a membership change that put this node back in the set, or
// moved the set to members that have not answered, keeps the copy. The copy
// compared is read under the same fence. It is a release: no drop entry, no
// drop record, and the copy's marks go with it.
func (c *Cluster) releaseOutsideSet(database, bucket string, confirmed []ring.Node, answers map[string]ReplCopy) error {
	key := placementOf(database, bucket)
	fence := c.s.acquireFence(key, fenceExclusive)
	defer fence.release()
	current, err := c.Replicas(key)
	if err != nil {
		return fmt.Errorf("placing bucket %q in database %q before releasing it: %w", bucket, database, err)
	}
	if slices.ContainsFunc(current, func(member ring.Node) bool { return member.ID == c.self.ID }) {
		return nil
	}
	if !slices.EqualFunc(current, confirmed, func(a, b ring.Node) bool { return a.ID == b.ID }) {
		return fmt.Errorf("releasing bucket %q in database %q: %w", bucket, database, errReplicaSetMoved)
	}
	if !c.s.bucketStoreOpen(database, bucket) {
		return nil
	}
	own, err := c.readCopy(ReplBucketRef{Database: database, Bucket: bucket})
	if err != nil {
		return fmt.Errorf("reading this node's copy of bucket %q in database %q before releasing it: %w", bucket, database, err)
	}
	owner := answers[current[0].ID]
	var behind, otherIncarnation []string
	for _, member := range current {
		answer := answers[member.ID]
		if answer.Term != owner.Term || !coversForRelease(answer, own) {
			behind = append(behind, member.ID)
		}
		if answer.Holds && answer.Incarnation != own.Incarnation {
			otherIncarnation = append(otherIncarnation, member.ID)
		}
	}
	if len(otherIncarnation) > 0 {
		c.warnKeptBesideAnotherIncarnation(key, heldBucket{database: database, bucket: bucket}, own.Incarnation, otherIncarnation)
	}
	if len(behind) > 0 {
		return &handOffUnconfirmedError{database: database, bucket: bucket, members: behind, refusals: fmt.Errorf(
			"not every member of the set holds everything this node's copy holds, in its owner's term, yet: %s", strings.Join(behind, ", "))}
	}
	c.noteConcession(database, bucket, own, owner)
	resp, err := c.s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket})
	if err != nil {
		return fmt.Errorf("releasing bucket %q in database %q outside its replica set: %w", bucket, database, err)
	}
	if !resp.Success {
		return fmt.Errorf("releasing bucket %q in database %q outside its replica set: %s", bucket, database, resp.Message)
	}
	c.forgetDemoted(heldBucket{database: database, bucket: bucket})
	if err := c.forgetReleasedMarks(key); err != nil {
		return err
	}
	c.endTakeOver(key)
	c.noteReleased(key)
	c.logger.Info("released a bucket copy outside its replica set; every member holds what it held",
		"database", database, "bucket", bucket, "replicas", len(current))
	return nil
}

// warnKeptBesideAnotherIncarnation says, at most once per strayWarnInterval
// per bucket (the take-over's limit, shared), that this node keeps its copy
// outside the set because members hold another incarnation of the name: no
// member will ever cover it, and an operator decides which bucket is wanted
// (I-H1). It is counted with the take-over's divergences.
func (c *Cluster) warnKeptBesideAnotherIncarnation(key placementKey, held heldBucket, incarnation uint64, holders []string) {
	c.takeOver.mu.Lock()
	last, warned := c.takeOver.strayWarnedAt[key]
	due := !warned || time.Since(last) >= strayWarnInterval
	if due {
		c.takeOver.strayWarnedAt[key] = time.Now()
	}
	c.takeOver.mu.Unlock()
	c.divergencesSeen.Add(1)
	if !due {
		return
	}
	c.logger.Warn(fmt.Sprintf("this node keeps its copy of bucket %q in database %q, outside the bucket's replica set, because %s hold "+
		"another incarnation of the name: two buckets of one name, created apart, which no hand-off merges. If this node's copy "+
		"is not wanted, empty its data directory and add it again; if it is, dump it here and restore it after dropping the name",
		held.bucket, held.database, strings.Join(holders, " and ")),
		"database", held.database, "bucket", held.bucket, "incarnation", incarnation, "holders", holders)
}

// forgetReleasedMarks deletes a released copy's placement marks.
func (c *Cluster) forgetReleasedMarks(key placementKey) error {
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(placementMarkKey(key)); err != nil {
		return fmt.Errorf("deleting the marks of a released copy: %w", err)
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("deleting the marks of a released copy: %w", err)
	}
	c.forgetPlacementMarks(key)
	return nil
}

// askNodeToMigrateHook, when set, runs before this node asks member to pull
// database/bucket, and may block; an error it returns is taken as the
// member's refusal. A test holds or fails a hand-off with it. Nil in
// production.
var askNodeToMigrateHook atomic.Pointer[func(fromID, memberID, database, bucket string) error]

// askNodeToMigrate sends a bucket's owner or replica the same OpMigrate the
// operator runs, so that node pulls the bucket from a holder.
func (c *Cluster) askNodeToMigrate(ctx context.Context, member ring.Node, database, bucket string) error {
	if hook := askNodeToMigrateHook.Load(); hook != nil {
		if err := (*hook)(c.self.ID, member.ID, database, bucket); err != nil {
			return err
		}
	}
	p := c.peerFor(member)
	cli, err := p.dial(ctx)
	if err != nil {
		return err
	}
	payload, err := bson.Marshal(MigrateRequest{Database: database, Bucket: bucket})
	if err != nil {
		return fmt.Errorf("encoding migrate request for %s.%s: %w", database, bucket, err)
	}
	frame, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpMigrate, payload)
	if err != nil {
		return err
	}
	if frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		if err := decodePeerAnswer(frame.Payload, &refusal); err != nil {
			return fmt.Errorf("node %s refused to migrate %s.%s with an unreadable error frame: %w", member.ID, database, bucket, err)
		}
		return &migrateRefusedError{message: fmt.Sprintf("node %s: %s", member.ID, refusal.Message)}
	}
	var resp MigrateResponse
	if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
		return fmt.Errorf("decoding node %s's answer to migrating %s.%s: %w", member.ID, database, bucket, err)
	}
	if !resp.Success {
		if resp.DroppedIncarnation != 0 {
			return &droppedHereError{member: member.ID, incarnation: resp.DroppedIncarnation, message: resp.Error}
		}
		return &migrateRefusedError{message: fmt.Sprintf("node %s: %s", member.ID, resp.Error), handOffTaken: resp.HandOffTaken}
	}
	return nil
}

// migrateRefusedError is a member's answer refusing a migrate ask: the member
// heard the ask. handOffTaken says the member is the bucket's owner and named
// the bucket to its take-over (MigrateResponse.HandOffTaken), which a removed
// node's hand-off counts as named (removed_handoff.go); any other refusal is
// not, since the member recorded nothing (BUG-0698).
type migrateRefusedError struct {
	message      string
	handOffTaken bool
}

func (e *migrateRefusedError) Error() string { return e.message }

// forEachKnownBucket iterates the buckets recorded in this node's catalog.
func (c *Cluster) forEachKnownBucket(fn func(database, bucket string)) {
	dd := c.s.serverDataAccessor()
	if dd == nil {
		return
	}
	dd.mu.RLock()
	dbs := make([]string, 0, len(dd.dbs))
	for n := range dd.dbs {
		dbs = append(dbs, n)
	}
	dd.mu.RUnlock()
	for _, dn := range dbs {
		db := dd.getDB(dn)
		if db == nil {
			continue
		}
		db.table.scan(func(buckets []string) {
			for _, bn := range buckets {
				fn(dn, bn)
			}
		})
	}
}

// sweepResidue is what one sweep generation could not settle of one epoch's
// membership change, and the members its hand-offs were waiting on.
type sweepResidue struct {
	generation uint64
	epoch      uint64
	list       []heldBucket
	waitingOn  []string
}

// recordResidue replaces the residue with what round's sweep generation left of
// its epoch's change, less the names a drop took off the lists since the round
// began, provided the generation is still the newest. It returns how many
// buckets it recorded and whether it recorded at all. A superseded sweep writes
// nothing: written last, it would erase what the newer one left, and nothing
// would retry it (review F-4).
func (c *Cluster) recordResidue(round sweepRound, list []heldBucket, waitingOn []string) (kept int, recorded bool) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if round.generation != c.sweepGeneration.Load() {
		return 0, false
	}
	list = c.withoutDroppedSince(round, list)
	c.residue = sweepResidue{generation: round.generation, epoch: round.epoch, list: list, waitingOn: waitingOn}
	return len(list), true
}

// sweepLeftCount is how many buckets the newest recorded sweep left unsettled.
// Zero is not settled: it reads zero for the whole first pass of a new epoch.
func (c *Cluster) sweepLeftCount() int {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	return len(c.residue.list)
}

// registerCreate records, before a create decided as owner reads its view of
// ownership, that a create of the bucket is in flight at the epoch this node
// is on, and returns what to call with the create's outcome. A create that was
// logged (appended) after the node adopted another epoch is kept as an orphan
// and handed off now (handOffOrphan) — whether the change moved the bucket
// (orphaned) or not, the sweep of that change walked a catalog without it.
// Any other create is forgotten, and the node marks itself settled if the
// record was all that kept it from it. Recorded before the view is read, so a
// sweep that ends while the create is in flight cannot mark the node settled
// past it. A name already an orphan keeps the epoch it was first recorded at.
func (c *Cluster) registerCreate(database, bucket string) (settle func(appended, orphaned bool)) {
	key := heldBucket{database: database, bucket: bucket}
	epoch := c.Epoch()
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if c.orphans == nil {
		c.orphans = make(map[heldBucket]uint64)
	}
	if _, already := c.orphans[key]; already {
		return func(bool, bool) {}
	}
	c.orphans[key] = epoch
	return func(appended, orphaned bool) {
		if appended {
			// The name exists here now: a read's "nothing" for it is out of
			// date (on-demand pull spec §4.2).
			c.forgetReadOutcomeOf(placementOf(database, bucket))
		}
		overtaken := appended && c.Epoch() != epoch
		c.sweepLeftMu.Lock()
		if orphaned || overtaken {
			c.orphans[key] = epoch
			c.sweepLeftMu.Unlock()
			// The placement pass of the change ran before this copy existed:
			// it is marked now as the pass would have marked it, so the worker
			// settles it (steps 10b.5, 10b.6).
			if err := c.markLatePlacement(placementOf(database, bucket)); err != nil {
				c.logger.Warn("could not mark a bucket created across a membership change; its copy is settled when a refusal names it",
					"database", database, "bucket", bucket, "error", err)
			}
			c.signalTakeOver()
			if orphaned {
				c.logger.Warn("a bucket this node created as its owner was moved by a membership change while it was created; "+
					"it is handed off like any moved bucket, and this node does not report its hand-off settled until it is",
					"database", database, "bucket", bucket, "epoch", epoch)
			}
			c.handOffOrphan(key)
			return
		}
		delete(c.orphans, key)
		c.sweepLeftMu.Unlock()
		c.markIfResidueClear()
	}
}

// handOffOrphan settles one orphan create on a goroutine of its own, at the
// sweep's backoff with no give-up: with auto-rebalance on it hands the bucket
// off (migrateIfMissing), with it off it asks the set members whether they
// hold it, as the confirm pass does; once settled the orphan is forgotten and
// the node marks itself settled if nothing else keeps it from it. Without this
// an orphan of a change whose sweep had already ended waited for a rearm, and
// with auto-rebalance off nothing settled it at all.
func (c *Cluster) handOffOrphan(key heldBucket) {
	if !c.startStoreGoroutine() {
		return
	}
	go func() {
		defer c.storeWG.Done()
		wait := sweepRetryFloor.get()
		for {
			var settled bool
			if c.s.config.Server.AutoRebalance {
				settled = c.migrateIfMissing(c.ctx, key.database, key.bucket) == nil
			} else {
				left, lacking, _ := c.confirmPass(c.ctx, []heldBucket{key})
				c.noteOrphanCopies(key, lacking)
				c.recomputeJoinFreezeForCounts()
				settled = len(left) == 0
			}
			if settled {
				c.dropOrphan(key)
				c.markIfResidueClear()
				return
			}
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(wait):
			}
			wait = min(2*wait, sweepRetryCeiling.get())
		}
	}()
}

// markIfResidueClear marks this node settled at its epoch if the newest sweep
// of that epoch has recorded that it left nothing: for when what kept the
// sweep's own mark from landing — a create in flight, an orphan — is gone.
func (c *Cluster) markIfResidueClear() {
	epoch := c.Epoch()
	c.sweepLeftMu.Lock()
	clear := c.residue.generation != 0 && c.residue.generation == c.sweepGeneration.Load() &&
		c.residue.epoch == epoch && len(c.residue.list) == 0
	c.sweepLeftMu.Unlock()
	if clear {
		c.markSettled(epoch)
	}
}

// dropOrphan forgets an orphan create once a sweep settled its bucket.
func (c *Cluster) dropOrphan(key heldBucket) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	delete(c.orphans, key)
}

// noteDemoted puts a copy a decrease took out of its set into the demoted set,
// which the rearm (every healthy gossip round, at most once per
// sweepRearmInterval) works until every survivor covers the copy and it is
// released (bucket-replication-rev8-spec §8.3). O(1). It wakes the gossip
// rounds, whose rearm makes the first attempt, rather than rearming here: its
// caller, the alter's apply, holds the bucket's fence and lock, and a healthy
// round comes only every 10*peerRetryInterval.
func (c *Cluster) noteDemoted(held heldBucket) {
	c.sweepLeftMu.Lock()
	if c.demoted == nil {
		c.demoted = make(map[heldBucket]struct{})
	}
	c.demoted[held] = struct{}{}
	c.sweepLeftMu.Unlock()
	c.wakeGossip()
}

// forgetDemoted takes a copy out of the demoted set: it was released, or the
// bucket placed this node again. O(1).
func (c *Cluster) forgetDemoted(held heldBucket) {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	delete(c.demoted, held)
}

// stillDemoted reports whether held is in the demoted set and this node is
// still outside the bucket's set. A copy the set holds again — a membership
// change or a later increase re-placed it — leaves the set here, when its
// bucket is touched, and is settled like any other copy: a membership change
// never reads the set (build doc SA-6).
func (c *Cluster) stillDemoted(held heldBucket) bool {
	c.sweepLeftMu.Lock()
	_, listed := c.demoted[held]
	c.sweepLeftMu.Unlock()
	if !listed {
		return false
	}
	if c.stillReplica(placementOf(held.database, held.bucket)) {
		c.forgetDemoted(held)
		return false
	}
	return true
}

// releaseDemoted is one attempt to release a demoted copy through the sweep's
// own path (migrateIfMissing, then handOffBucket and releaseOutsideSet, which
// delete it only once every member of the set covers it). A copy that is gone
// or released leaves the set; one that waits stays in it, and nothing of the
// wait is counted: not in the residue, the settle mark, the hand-off progress
// or the release-wait warning (spec §8.3 point 2). A decrease is intentional,
// so its waiting copy is no unsettled change.
func (c *Cluster) releaseDemoted(ctx context.Context, held heldBucket) {
	if err := c.migrateIfMissing(ctx, held.database, held.bucket); err != nil {
		c.logger.Debug("a copy a decrease demoted waits for its survivors to cover it", "database", held.database, "bucket", held.bucket, "error", err)
		return
	}
	c.forgetDemoted(held)
}

// demotedCopies lists the demoted set; the caller holds sweepLeftMu.
func (c *Cluster) demotedCopies() []heldBucket {
	listed := make([]heldBucket, 0, len(c.demoted))
	for held := range c.demoted {
		listed = append(listed, held)
	}
	return listed
}

// markSettled raises this node's Settled epoch to epoch, persists it, and says
// so — unless an orphan create of an earlier epoch is still in flight or not
// handed off, which keeps the node unsettled (review F-11).
func (c *Cluster) markSettled(epoch uint64) {
	if !c.raiseSettled(epoch) {
		return
	}
	c.logger.Info("this node has handed off every bucket the membership change moved; it is settled at the epoch", "epoch", epoch)
	c.recomputeJoinFreeze()
	// The others lift their pause on hearing it: within a round trip, not at
	// the next gossip interval (spec §3.4).
	c.wakeGossip()
}

// raiseSettled raises this node's Settled epoch to epoch and persists it,
// unless an orphan create of an earlier epoch, a name its take-over has not
// found a copy for yet, or a removed node whose hand-off has not reached it
// (removed_settle.go) keeps it unsettled. It reports whether the epoch rose.
func (c *Cluster) raiseSettled(epoch uint64) bool {
	if c.takeOverUnfound() {
		c.logger.Debug("a bucket this node's take-over has not found a copy of keeps it from reporting the membership change settled", "epoch", epoch)
		return false
	}
	if c.awaitingRemovedHandOff(epoch) {
		c.logger.Debug("a removed node's hand-off has not reached this node yet; it does not report the membership change settled", "epoch", epoch)
		return false
	}
	c.sweepLeftMu.Lock()
	for _, orphanedAt := range c.orphans {
		if orphanedAt < epoch {
			c.sweepLeftMu.Unlock()
			c.logger.Debug("an orphan create keeps this node from reporting the membership change settled", "epoch", epoch)
			return false
		}
	}
	raised := false
	for current := c.settled.Load(); current < epoch && !raised; current = c.settled.Load() {
		raised = c.settled.CompareAndSwap(current, epoch)
	}
	c.sweepLeftMu.Unlock()
	if !raised {
		return false
	}
	if err := c.persistSettled(); err != nil {
		c.logger.Warn("could not record that this node is settled; after a restart it sweeps again, which settles it again",
			"epoch", epoch, "error", err)
	}
	return true
}

// unconfirmedListed is how many copies still to be made a node publishes by
// name: enough for the refusal to print as commands, bounded so gossip does
// not grow with the buckets a change moved.
const unconfirmedListed = 5

// confirmUntilSettled is the hand-off of epoch's change with auto-rebalance
// off (join-freeze spec §8.6, Q2 option A): it moves nothing. It asks every
// member of the new set of each bucket this node holds whose set the change
// altered whether it holds a copy (askCopies), again at the sweep's backoff
// with no give-up, and marks the node settled once every one does: the
// operator's 'migrate' commands, which the refusal and 'nodes' list, are what
// make the copies. It takes a sweep generation like a sweep and gives way to
// a newer one the same way.
func (c *Cluster) confirmUntilSettled(ctx context.Context, epoch uint64) {
	c.atSweepStage(epoch, sweepStarting)
	generation := c.claimFullSweep(epoch)
	round := sweepRound{generation: generation, epoch: epoch}
	c.atSweepStage(epoch, sweepPassing)
	pending := c.changedHeldBuckets(epoch)
	var total uint64
	for _, held := range pending {
		if c.movedOffThisNode(placementOf(held.database, held.bucket), epoch) {
			total++
		}
	}
	wait := sweepRetryFloor.get()
	for {
		var unconfirmed []PlacementToCopy
		var waitingOn []string
		pending, unconfirmed, waitingOn = c.confirmPass(ctx, pending)
		c.recordConfirmProgress(round, total, pending, unconfirmed, waitingOn)
		c.recomputeJoinFreezeForCounts()
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if c.sweepGeneration.Load() != generation {
			return
		}
		wait = min(2*wait, sweepRetryCeiling.get())
	}
	c.settleSweep(round, nil)
}

// changedHeldBuckets is the buckets this node holds whose replica set the
// change to epoch altered — every holder confirms its own, not only the
// previous owner. A node that does not remember the membership before the
// change cannot tell what changed, and confirms every bucket it holds.
func (c *Cluster) changedHeldBuckets(epoch uint64) []heldBucket {
	before, known := c.ringAt(epoch - 1)
	var changed []heldBucket
	c.forEachKnownBucket(func(database, bucket string) {
		if !c.s.bucketStoreOpen(database, bucket) {
			return
		}
		// A demoted copy needs no member to hold it: it waits only for its
		// release, which is not the change's to confirm (spec §8.3 point 2).
		if c.stillDemoted(heldBucket{database: database, bucket: bucket}) {
			return
		}
		key := placementOf(database, bucket)
		if known {
			oldSet, err := before.Replicas(key.encoded, c.effectiveRF(key))
			newSet, newErr := c.Replicas(key)
			if err == nil && newErr == nil && slices.Equal(nodeIDs(oldSet), nodeIDs(newSet)) {
				return
			}
		}
		changed = append(changed, heldBucket{database: database, bucket: bucket})
	})
	return changed
}

// confirmPass asks each set member of pending about its copies and returns the
// buckets some member does not hold yet, each copy still to make (an alter's
// awaiting members among them), and the members that did not answer.
func (c *Cluster) confirmPass(ctx context.Context, pending []heldBucket) ([]heldBucket, []PlacementToCopy, []string) {
	asked := make(map[string][]heldBucket)
	nodes := make(map[string]ring.Node)
	lacking := make(map[heldBucket]bool)
	position := make(map[PlacementToCopy]int)
	for _, held := range pending {
		set, err := c.Replicas(placementOf(held.database, held.bucket))
		if err != nil {
			// Unplaceable now; asked again at the next pass.
			c.logger.Debug("the confirm pass could not place a bucket", "database", held.database, "bucket", held.bucket, "error", err)
			lacking[held] = true
			continue
		}
		for index, member := range set {
			position[PlacementToCopy{Database: held.database, Bucket: held.bucket, Node: member.ID}] = index
			if member.ID != c.self.ID {
				asked[member.ID] = append(asked[member.ID], held)
				nodes[member.ID] = member
			}
		}
	}
	var unconfirmed []PlacementToCopy
	var waitingOn []string
	// Each member's answer per bucket, for the predicate below: a member
	// "holds" the bucket only when it follows its owner's term (R8-F).
	answered := make(map[heldBucket]map[string]ReplCopy, len(pending))
	for _, memberID := range slices.Sorted(maps.Keys(asked)) {
		buckets := asked[memberID]
		refs := make([]ReplBucketRef, 0, len(buckets))
		for _, held := range buckets {
			refs = append(refs, ReplBucketRef{Database: held.database, Bucket: held.bucket})
		}
		copies, err := c.askCopies(ctx, nodes[memberID], refs)
		for index, held := range buckets {
			if err == nil && copies[index].Holds {
				if answered[held] == nil {
					answered[held] = make(map[string]ReplCopy)
				}
				answered[held][memberID] = copies[index]
				continue
			}
			lacking[held] = true
			unconfirmed = append(unconfirmed, PlacementToCopy{Database: held.database, Bucket: held.bucket, Node: memberID})
		}
		if err != nil {
			c.logger.Debug("a member did not answer the confirm pass", "node_id", memberID, "error", err)
			waitingOn = append(waitingOn, memberID)
		}
	}
	for _, held := range pending {
		if lacking[held] || c.followsItsOwner(held, answered[held]) {
			continue
		}
		lacking[held] = true
		// A member that holds the owner's incarnation in another term is a
		// copy to make too: a replay of an earlier owner's log can deliver a
		// create after the change, and 'migrate' on that member makes its
		// copy follow the owner (BUG-0545). Unlisted, the operator had
		// nothing to run and the pause never lifted.
		for _, member := range c.membersBehindOwnersTerm(held, answered[held]) {
			unconfirmed = append(unconfirmed, PlacementToCopy{Database: held.database, Bucket: held.bucket, Node: member})
		}
	}
	// An alter's awaiting members are copies to make too, beside the old-term
	// ones (bucket-replication-rev8-spec §7.5): the awaiting record, not a
	// member's answer, says the copy is not seeded yet, and the write path
	// sends that member nothing of the bucket until it is.
	for _, held := range pending {
		match, err := c.awaitingMatchOfCopy(held.database, held.bucket)
		if err != nil {
			c.logger.Debug("the confirm pass could not read a bucket's term to list its awaiting members", "database", held.database, "bucket", held.bucket, "error", err)
			continue
		}
		for _, member := range c.awaitingMatching(placementOf(held.database, held.bucket), match) {
			copyToMake := PlacementToCopy{Database: held.database, Bucket: held.bucket, Node: member}
			if !slices.Contains(unconfirmed, copyToMake) {
				unconfirmed = append(unconfirmed, copyToMake)
			}
			lacking[held] = true
		}
	}
	// Each bucket's copies in its set's order, owner first: a replica is
	// seeded from the owner, so the owner's 'migrate' has to run first.
	slices.SortStableFunc(unconfirmed, func(a, b PlacementToCopy) int {
		if order := strings.Compare(a.Database+"\x00"+a.Bucket, b.Database+"\x00"+b.Bucket); order != 0 {
			return order
		}
		return position[a] - position[b]
	})
	left := pending[:0]
	for _, held := range pending {
		if lacking[held] {
			left = append(left, held)
		}
	}
	return left, unconfirmed, waitingOn
}

// followsItsOwner reports whether every member of the bucket's set, by its
// answer (this node's own copy read here), follows an owner that has taken the
// bucket over: the owner's copy opened and not moved, every copy in its term.
func (c *Cluster) followsItsOwner(held heldBucket, answers map[string]ReplCopy) bool {
	set, err := c.Replicas(placementOf(held.database, held.bucket))
	if err != nil {
		return false
	}
	copies := make([]ReplCopy, 0, len(set))
	for _, member := range set {
		answer, known := answers[member.ID]
		if member.ID == c.self.ID {
			if answer, err = c.answerCopy(ReplBucketRef{Database: held.database, Bucket: held.bucket}); err != nil {
				return false
			}
			known = true
		}
		if !known {
			return false
		}
		copies = append(copies, answer)
	}
	owner := copies[0]
	if !owner.Holds || !owner.Opened || owner.Moved != 0 {
		return false
	}
	for _, member := range copies {
		if !member.Holds || member.Term != owner.Term {
			return false
		}
	}
	return true
}

// membersBehindOwnersTerm is the members of the bucket's set, after the
// owner, whose copy holds the owner's incarnation in another term than the
// owner's opened copy, by their answers (this node's own copy read here). It
// is empty while the owner has not taken the bucket over: its copy to make is
// listed first, and the others follow from it.
func (c *Cluster) membersBehindOwnersTerm(held heldBucket, answers map[string]ReplCopy) []string {
	set, err := c.Replicas(placementOf(held.database, held.bucket))
	if err != nil {
		return nil
	}
	copyOf := func(member string) (ReplCopy, bool) {
		if member == c.self.ID {
			own, err := c.answerCopy(ReplBucketRef{Database: held.database, Bucket: held.bucket})
			return own, err == nil
		}
		answer, known := answers[member]
		return answer, known
	}
	owner, known := copyOf(set[0].ID)
	if !known || !owner.Holds || !owner.Opened || owner.Moved != 0 {
		return nil
	}
	var behind []string
	for _, member := range set[1:] {
		theirs, known := copyOf(member.ID)
		if known && theirs.Holds && theirs.Incarnation == owner.Incarnation && theirs.Term != owner.Term {
			behind = append(behind, member.ID)
		}
	}
	return behind
}

// recordConfirmProgress publishes a confirm pass's progress as this node's
// hand-off of the round's change, if the round is still the newest: the
// buckets this node owned before the change and how many of them every set
// member now holds, the members that did not answer, and the first copies
// still to make with how many there are.
func (c *Cluster) recordConfirmProgress(round sweepRound, total uint64, pending []heldBucket, unconfirmed []PlacementToCopy, waitingOn []string) {
	var left uint64
	for _, held := range pending {
		if c.movedOffThisNode(placementOf(held.database, held.bucket), round.epoch) {
			left++
		}
	}
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	if round.generation != c.sweepGeneration.Load() {
		return
	}
	c.handOff = &HandOffProgress{
		Epoch:            round.epoch,
		Total:            total,
		Done:             total - min(left, total),
		WaitingOn:        waitingOn,
		Unconfirmed:      unconfirmed[:min(len(unconfirmed), unconfirmedListed)],
		UnconfirmedCount: uint64(len(unconfirmed)),
	}
}

// sweepRearmInterval is the least time between two re-armed sweeps, so a peer
// that flaps cannot start a sweep each time it comes back.
var sweepRearmInterval = newSweepBound(30 * time.Second)

// rearmSweep runs the sweep again for the buckets the newest one left and for
// any orphan create. It is called when a peer becomes reachable again and from
// every healthy gossip round (verifyPeer), at most once per sweepRearmInterval,
// one at a time, and never while a full sweep that has not recorded its
// residue runs: a rearm takes a newer generation, and the full sweep it
// superseded would stop retrying and record nothing. It asks about only what
// is left -- nothing per bucket is armed while waiting, no timer, and with
// nothing left it costs a lock and a length check. 10^6 buckets do not
// change the cost of the event itself, only the cost of the pass over what is
// left, which is at worst one attempt per unsettled bucket. It first works the
// copies a decrease demoted, on their own pace (rearmDemoted).
func (c *Cluster) rearmSweep() {
	c.rearmDemoted()
	if !c.rearmRunning.CompareAndSwap(false, true) {
		return
	}
	c.sweepLeftMu.Lock()
	residue := c.residue
	pending := append([]heldBucket(nil), residue.list...)
	for orphan := range c.orphans {
		if !slices.Contains(pending, orphan) {
			pending = append(pending, orphan)
		}
	}
	// The setting is read last, when there is something to re-arm: this runs
	// at every healthy gossip round, and with nothing left costs no more than
	// the lock and the length check.
	idle := len(pending) == 0 || residue.generation < c.fullSweepGeneration.Load() ||
		time.Since(c.lastRearmAt) < sweepRearmInterval.get() || !c.s.config.Server.AutoRebalance
	var round sweepRound
	if !idle {
		c.lastRearmAt = time.Now()
		round = c.beginRoundLocked(sweepRound{generation: c.sweepGeneration.Add(1), epoch: residue.epoch})
	}
	c.sweepLeftMu.Unlock()
	if idle {
		c.rearmRunning.Store(false)
		c.maybeSweepTombstones()
		return
	}

	c.atSweepStage(residue.epoch, rearmSnapshotted)
	c.sweepRearms.Add(1)
	if !c.startStoreGoroutine() {
		c.endRound()
		c.rearmRunning.Store(false)
		return
	}
	go func() {
		defer c.storeWG.Done()
		defer c.rearmRunning.Store(false)
		defer c.endRound()
		c.atSweepStage(residue.epoch, rearmPassing)
		left := c.retrySettling(c.ctx, round, c.sweepPass(c.ctx, round, pending))
		c.settleSweep(round, left)
	}()
}

// rearmDemoted makes one attempt to release each copy a decrease demoted
// (spec §8.3), at most once per sweepRearmInterval and one pass at a time,
// with auto-rebalance on. The set is work even while a full sweep runs (point
// 3): that sweep visits a demoted copy once and leaves it out of its retries,
// so a copy waiting on a survivor would otherwise wait for the next
// membership change. It is single-flight on its own flag and takes no sweep
// generation, so it never supersedes a sweep (BUG-0196) and never waits behind
// a rearm's retries, which can run to sweepRetryGiveUp; nothing of its wait
// reaches the residue (releaseDemoted). With the set empty it costs a lock and
// a length check; a pass is O(demoted copies), never O(held buckets).
func (c *Cluster) rearmDemoted() {
	if !c.demotedRunning.CompareAndSwap(false, true) {
		return
	}
	c.sweepLeftMu.Lock()
	idle := len(c.demoted) == 0 || time.Since(c.lastDemotedAt) < sweepRearmInterval.get() || !c.s.config.Server.AutoRebalance
	var demoted []heldBucket
	if !idle {
		c.lastDemotedAt = time.Now()
		demoted = c.demotedCopies()
	}
	c.sweepLeftMu.Unlock()
	if idle || !c.startStoreGoroutine() {
		c.demotedRunning.Store(false)
		return
	}
	go func() {
		defer c.storeWG.Done()
		defer c.demotedRunning.Store(false)
		c.atSweepStage(c.Epoch(), rearmPassing)
		for _, held := range demoted {
			if c.stillDemoted(held) {
				c.releaseDemoted(c.ctx, held)
			}
		}
	}()
}

// refusedSourcesCtxKey carries a sweep pass's refusedSources in the context of
// the pulls it makes.
type refusedSourcesCtxKey struct{}

// refusedSources is what one sweep pass learned about sources whose stream to
// this node is refused for good (errHandshakeRefusedForGood): the first pull
// from such a source asks it and meets the refusal, and the pass's other pulls
// from it are not sent, so the refusal costs one ask per source, not one per
// bucket. A source stays in it for that pass only, so the next pass asks once
// more and picks up an operator's fix. It holds one entry per member, never
// one per bucket. A nil *refusedSources, a pull outside a sweep pass, asks
// every time.
type refusedSources struct {
	mu       sync.Mutex
	bySource map[string]*sourceRefusal
}

type sourceRefusal struct {
	refusal error
	skipped int
}

func refusedSourcesOf(ctx context.Context) *refusedSources {
	refused, _ := ctx.Value(refusedSourcesCtxKey{}).(*refusedSources)
	return refused
}

// note remembers that sourceID refused for good and returns refusal.
func (r *refusedSources) note(sourceID string, refusal error) error {
	if r == nil {
		return refusal
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, known := r.bySource[sourceID]; !known {
		r.bySource[sourceID] = &sourceRefusal{refusal: refusal}
	}
	return refusal
}

// skipIfRefused returns the refusal sourceID gave earlier in this pass, and
// counts the pull it spares, or nil when the source has not refused.
func (r *refusedSources) skipIfRefused(sourceID string) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	refused, known := r.bySource[sourceID]
	if !known {
		return nil
	}
	refused.skipped++
	return fmt.Errorf("not asking %s again in this pass: %w", sourceID, refused.refusal)
}

// logSkips writes the pass's one line per refused source: who, why and the
// fix (the refusal names them), and how many pulls were not sent.
func (r *refusedSources) logSkips(logger *slog.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for sourceID, refused := range r.bySource {
		logger.Warn("a sweep pass skipped the buckets of a source whose stream to this node is refused for good; "+
			"the next pass asks it once more", "source", sourceID, "buckets_skipped", refused.skipped, "reason", refused.refusal.Error())
	}
}
