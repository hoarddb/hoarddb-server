// Package server — the restore session (the dump-restore-spec record §9).
//
// This file wires OpRestoreStart/OpRestoreChunk/OpRestoreDone to the ordinary
// write path (Server.InsertCtx, HeapStore.PushItem) the same way Task 2's
// server/dump.go wired the read side to store.Enumerator. Restore consumes
// untrusted input — a dump directory can be copied to a laptop and back, or
// be deliberately hostile — so everything here follows dump-restore-spec.md
// §9.1's rule: validate before a single byte is written, and refuse by
// naming what was found.
//
// Root gate. requireRootIdentity (server/dump.go) is reused verbatim, exactly
// as the brief requires — this file does not reimplement it.
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Sentinel errors for the restore path. Each is wrapped with fmt.Errorf so
// callers match with errors.Is rather than string comparison (AGENTS.md
// rule 4).
var (
	// ErrRestoreFormatVersion means the dump's format_version does not match
	// what this build produces (dump-restore-spec.md §4, §9.1).
	ErrRestoreFormatVersion = errors.New("restore format version mismatch")
	// ErrRestorePathEscapesRoot means a database or bucket name would
	// canonicalise outside the server's data directory, or is otherwise
	// unsafe as a path component (dump-restore-spec.md §9.1, §19 A-12).
	ErrRestorePathEscapesRoot = namepolicy.ErrUnsafeComponent
	// ErrRestoreReservedName means the dump names a database reserved for
	// internal use (server/reserved.go, dump-restore-spec.md §19 A-4).
	ErrRestoreReservedName = errors.New("restore database name is reserved")
	// ErrRestoreStoreTypeMismatch means the dump's store_type disagrees with
	// an existing target bucket's type (dump-restore-spec.md §9.1, §9.4).
	ErrRestoreStoreTypeMismatch = errors.New("restore store type mismatch")
	// ErrRestoreDropBlobDataLoss means --drop would destroy an existing
	// blob bucket's payloads — either because the target already is a blob
	// bucket, or because the dump itself is a blob dump — and dump never
	// captures blob payloads in the first place, so there is nothing to
	// restore them from (dump-restore-spec.md §9.4).
	ErrRestoreDropBlobDataLoss = errors.New("restore --drop would destroy blob payloads dump never captured")
	// ErrRestoreBucketBeingDumped means OpRestoreStart named a bucket a dump
	// currently holds the single server-wide dump pin on
	// (dump-restore-spec.md §9.7).
	ErrRestoreBucketBeingDumped = errors.New("bucket is being dumped")
	// ErrRestoreBucketConflict means OpRestoreStart's --drop collides with
	// another restore already rebuilding the same bucket, or a non-drop
	// restore collides with a --drop already in progress
	// (dump-restore-spec.md §9.7).
	ErrRestoreBucketConflict = errors.New("bucket is already being restored")
	// ErrRestoreInsufficientDiskSpace means free space on this node is below
	// the configured multiplier of the bucket's raw dump bytes
	// (dump-restore-spec.md §9.1, §11, §19 A-15).
	ErrRestoreInsufficientDiskSpace = errors.New("insufficient disk space for restore")
	// ErrRestoreNoActiveSession means OpRestoreChunk/OpRestoreDone named a
	// session id this node has no open restore for.
	ErrRestoreNoActiveSession = errors.New("no active restore session")
)

// defaultRestoreDiskHeadroomMultiplier is used when
// config.Server.RestoreDiskHeadroomMultiplier is zero or unset (an empty
// config.Config built directly by a test, rather than through
// config.DefaultConfig/Load). Mirrors dump.go's defaultDumpPinCeiling
// pattern.
const defaultRestoreDiskHeadroomMultiplier = 2.0

// restoreBucketPin is the write-side half of the bucket-pin axis
// dump-restore-spec.md §9.7 introduces alongside Task 2's dump pin: a bucket
// being rebuilt by one or more restores, and whether any of them is a
// --drop. Unlike the dump pin (one per server, server/dump.go), this is
// per bucket and — for a non-drop restore — may have more than one holder,
// because two concurrent merges into the same bucket are allowed (§9.2
// makes a merge idempotent). A --drop is exclusive: it conflicts with a
// dump, with another restore of any kind, and is refused if anything else
// already holds the bucket.
type restoreBucketPin struct {
	drop    bool
	holders int
}

// bucketPinKey builds the map key claimBucketRestorePin and
// restoreActiveLocked (called from server/dump.go's claimDump) both use to
// name one bucket in s.restorePins.
func bucketPinKey(database, bucket string) string {
	return database + "/" + bucket
}

// claimBucketRestorePin reserves database/bucket for a restore, refusing a
// conflicting dump pin or a conflicting --drop (dump-restore-spec.md §9.7).
// The returned release func is idempotent-safe to call at most once; the
// caller is responsible for calling it exactly once when the restore ends,
// successfully or not.
//
// The dump-pin check and the restore-pin insert happen under one held lock
// (s.dumpMu — the same mutex server/dump.go's claimDump uses for s.dump),
// not two separately-acquired ones (fix round 1, Important #1). The
// original version checked s.dump, released s.dumpMu, then separately
// locked a dedicated bucketPinMu to record the restore pin — a window
// between those two locks in which claimDump could observe "no restore pin
// yet" and a concurrent claimBucketRestorePin could observe "no dump
// active", letting a dump and a restore both win the same bucket
// (dump-restore-spec.md §9.7/§12.13). Not race-detector-visible: each
// individual field access was correctly synchronized on its own mutex: this
// was a logical race between two lock scopes, not a data race. One mutex,
// held across the whole check-and-set on both sides, removes the window
// entirely. See restoreActiveLocked's doc comment for claimDump's half.
func (s *Server) claimBucketRestorePin(database, bucket string, drop bool) (func(), error) {
	s.dumpMu.Lock()
	defer s.dumpMu.Unlock()

	if s.dump != nil && s.dump.database == database && s.dump.bucket == bucket {
		return nil, fmt.Errorf("%w: bucket %s/%s is being dumped; wait for it to finish or dump a different bucket",
			ErrRestoreBucketBeingDumped, database, bucket)
	}

	key := bucketPinKey(database, bucket)
	if s.restorePins == nil {
		s.restorePins = make(map[string]*restoreBucketPin)
	}
	existing := s.restorePins[key]
	if existing != nil {
		if existing.drop {
			return nil, fmt.Errorf("%w: bucket %s/%s is being restored with --drop; wait for it to finish",
				ErrRestoreBucketConflict, database, bucket)
		}
		if drop {
			return nil, fmt.Errorf("%w: bucket %s/%s is already being restored (merge in progress); a --drop restore cannot run alongside it — wait for it to finish",
				ErrRestoreBucketConflict, database, bucket)
		}
		existing.holders++
	} else {
		s.restorePins[key] = &restoreBucketPin{drop: drop, holders: 1}
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			s.dumpMu.Lock()
			defer s.dumpMu.Unlock()
			p := s.restorePins[key]
			if p == nil {
				return
			}
			p.holders--
			if p.holders <= 0 {
				delete(s.restorePins, key)
			}
		})
	}
	return release, nil
}

// restoreActiveLocked reports whether a restore currently holds a pin on
// database/bucket. Caller must already hold s.dumpMu — server/dump.go's
// claimDump holds it for its entire body and calls this directly (a second,
// nested Lock would deadlock: sync.Mutex is not reentrant), which is what
// makes claimDump's dump-claim and claimBucketRestorePin's restore-claim
// mutually exclusive with no gap between them (see claimBucketRestorePin's
// doc comment for the race this closes).
func (s *Server) restoreActiveLocked(database, bucket string) (holder string, ok bool) {
	if s.restorePins == nil {
		return "", false
	}
	if p, exists := s.restorePins[bucketPinKey(database, bucket)]; exists && p.holders > 0 {
		return "restore", true
	}
	return "", false
}

// bucketHasActiveRestore is restoreActiveLocked with its own locking, for a
// caller (tests; a future admin/status command) that does not already hold
// s.dumpMu.
func (s *Server) bucketHasActiveRestore(database, bucket string) (holder string, ok bool) {
	s.dumpMu.Lock()
	defer s.dumpMu.Unlock()
	return s.restoreActiveLocked(database, bucket)
}

// RestoreStartRequest asks the server to begin restoring one bucket from a
// validated dump. The CLI has already checked format_version, the per-file
// digest and the top-level manifest's "incomplete" flag locally
// (dump-restore-spec.md §9.1) before this ever reaches the wire — everything
// here is the server-side half of that same validation, enforced again
// because a hostile client can skip the CLI entirely.
type RestoreStartRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	// FormatVersion is the bucket's own metadata.json format_version,
	// checked again here so a raw client bypassing the CLI cannot restore an
	// unsupported layout (dump-restore-spec.md §10: "OpRestoreStart must
	// refuse before the first chunk — version, store type, name validity,
	// disk space, --drop collisions").
	FormatVersion int        `bson:"format_version"`
	StoreType     BucketType `bson:"store_type"`
	Indexes       []string   `bson:"indexes,omitempty"`
	// IndexSpecs is a format-2 dump's declaration; when present the bucket is
	// created from it rather than from Indexes.
	IndexSpecs []DumpIndexSpec `bson:"index_specs,omitempty"`
	// Stores is the dump manifest's store fields (format 2). They are
	// declared by name, with fresh ids, before any element arrives.
	Stores []DumpStore `bson:"stores,omitempty"`
	// ReplicationFactor is the factor the dump records the bucket declared;
	// zero is a bucket that inherited, or a dump written before the field. A
	// bucket the restore creates declares it when this server can grant it,
	// and inherits otherwise (RestoreStartResponse.FactorNotRestored).
	ReplicationFactor int `bson:"replication_factor,omitempty"`
	// Placement is the placement the dump records for a hash bucket; a bucket
	// the restore creates gets it, so a whole bucket keeps its unique index. A
	// bucket that already exists keeps its own: a restore into it "restores
	// documents, not placement". Empty takes the create's default.
	Placement string `bson:"placement,omitempty"`
	// Bytes is the bucket's raw dump size (the .bson file's byte count, from
	// metadata.json, plus its stores and dead files) — the free-disk-space
	// check is this times the configured multiplier, checked per bucket before
	// that bucket starts.
	Bytes int64 `bson:"bytes"`
	// Dead is the dump manifest's count of dead entries (format 3). While it
	// is above 0 and a cluster member lacks queue_claim, the restore is refused
	// here, before any document: the gate would otherwise refuse the dead
	// section only after every document had been restored.
	Dead int64 `bson:"dead,omitempty"`
	// Drop asks the server to drop and recreate the bucket before restoring
	// (dump-restore-spec.md §9.4). Never implied by anything else.
	Drop bool `bson:"drop,omitempty"`
	// DryRun asks the server to run every check up to (but not including)
	// the bucket pin, --drop and create/verify steps, and report what would
	// happen, without writing anything: no bucket is created or dropped, no
	// session is opened, and the caller must not follow up with
	// OpRestoreChunk/OpRestoreDone. This is what `hoarddb restore --dry-run`
	// (cli/dump.go) drives, so dry-run validation is the same server-side
	// code path as a real restore rather than a second implementation that
	// can drift from it.
	DryRun bool `bson:"dry_run,omitempty"`
}

// RestoreStartResponse answers OpRestoreStart.
type RestoreStartResponse struct {
	Success bool   `bson:"success"`
	Message string `bson:"message,omitempty"`
	// SessionID names this restore for the OpRestoreChunk/OpRestoreDone
	// calls that follow. It exists because the wire protocol allows more
	// than one concurrent restore of the same bucket (dump-restore-spec.md
	// §9.7 — two merges may overlap) and because a forwarded cross-node
	// restore's chunks may arrive over different pooled internode
	// connections (transport.InternodePoolSize > 1) — database+bucket alone
	// cannot disambiguate either case the way it can for dump's single
	// server-wide session.
	SessionID string `bson:"session_id,omitempty"`
	// Created reports whether the bucket (and possibly its database) was
	// freshly created for this restore.
	Created bool `bson:"created,omitempty"`
	// Dropped reports whether an existing bucket was dropped and recreated
	// (only possible when Drop was set).
	Dropped bool `bson:"dropped,omitempty"`
	// MissingIndexes lists indexes the dump declares that the existing
	// target bucket does not have. Restoring into a live bucket cannot
	// retroactively add a secondary index (no such capability exists on an
	// open bucket today), so this is reported rather than silently ignored
	// or used to refuse a restore that would otherwise succeed — a missing
	// index affects Search(), not data integrity.
	MissingIndexes []string `bson:"missing_indexes,omitempty"`
	// FactorNotRestored is the factor the dump records when this server
	// cannot grant it (standalone, or fewer members than the factor): the
	// bucket was created inheriting the node's. Data restored at the default
	// is better than no data, so the restore goes on and says so.
	FactorNotRestored int `bson:"factor_not_restored,omitempty"`
	// FactorKept is the existing bucket's factor when a merge left it and the
	// dump records another: a merge restores documents, not placement.
	FactorKept int `bson:"factor_kept,omitempty"`
	// FactorLowered is the factor a bucket declared before --drop re-created
	// it from a dump that records none, when that was above the node's.
	FactorLowered int `bson:"factor_lowered,omitempty"`
}

// restoreFactorPlan is what a restore does with the factor its dump records:
// the factor the created bucket declares, and the three things a restore
// reports when the bucket does not end up with the dump's factor.
type restoreFactorPlan struct {
	declare, notRestored, kept, lowered int
}

// inheritedFactor is the factor a bucket that declares none is placed at.
func (s *Server) inheritedFactor() int {
	return max(1, s.config.Server.ReplicationFactor)
}

// planRestoreFactor decides req's factor against existing, the bucket a
// restore would merge into or drop (nil when there is none). It reads and
// changes nothing, so a dry run asks it too.
func (s *Server) planRestoreFactor(req RestoreStartRequest, existing *inMemoryBucket) restoreFactorPlan {
	var plan restoreFactorPlan
	if existing != nil && !req.Drop {
		declared := int(existing.rec.replicationFactor.Load())
		effective := declared
		if declared == 0 {
			effective = s.inheritedFactor()
		}
		if req.ReplicationFactor != 0 && req.ReplicationFactor != effective {
			plan.kept = effective
		}
		return plan
	}
	plan.declare = req.ReplicationFactor
	// The create is asked the same question a client's create is, here rather
	// than by its refusal: a refused create would fail the restore, and a
	// bypass of the refusal would be a wire field any client could set
	// (bucket-replication spec §17.4.10).
	if s.checkBucketRF(CreateBucketRequest{Database: req.Database, Name: req.Bucket, ReplicationFactor: plan.declare}) != nil {
		plan.notRestored, plan.declare = plan.declare, 0
	}
	if existing != nil && req.ReplicationFactor == 0 {
		if declared := int(existing.rec.replicationFactor.Load()); declared > s.inheritedFactor() {
			plan.lowered = declared
		}
	}
	return plan
}

// placementFactor is the factor the created bucket is placed at.
func (plan restoreFactorPlan) placementFactor(inherited int) int {
	if plan.declare > 0 {
		return plan.declare
	}
	return inherited
}

// RestoreChunkRequest carries one chunk of BSON bytes for the open bucket.
// Data must be a concatenation of complete BSON documents — the same
// contract OpDumpChunk's response promises on the way out — never a
// document split across two chunks; a chunk that violates this is refused
// rather than guessed at, since restore is exactly the boundary where a
// hostile or broken client is expected (dump-restore-spec.md §9.1).
type RestoreChunkRequest struct {
	Database  string `bson:"database"`
	Bucket    string `bson:"bucket"`
	SessionID string `bson:"session_id"`
	Data      []byte `bson:"data,omitempty"`
	// Section is "stores" for a chunk of field-store element records, which
	// follow every documents chunk, or "dead" for a chunk of dead entries,
	// which follow both. Empty is documents; any other value is refused.
	Section string `bson:"section,omitempty"`
}

// RestoreChunkResponse acknowledges one chunk.
type RestoreChunkResponse struct {
	Success   bool   `bson:"success"`
	Message   string `bson:"message,omitempty"`
	Documents int    `bson:"documents,omitempty"`
}

// RestoreDoneRequest closes the bucket's restore session.
type RestoreDoneRequest struct {
	Database  string `bson:"database"`
	Bucket    string `bson:"bucket"`
	SessionID string `bson:"session_id"`
	// Documents is the count metadata.json recorded for this bucket, used to
	// assert the stream actually delivered what the dump promised
	// (dump-restore-spec.md §10: "assert the count"). Zero skips the
	// assertion — used when a caller has no count to compare (never sent by
	// the CLI, which always has metadata.json's own count).
	Documents int64 `bson:"documents,omitempty"`
	// Elements is the count of field-store element records the dump
	// recorded, asserted the same way; zero skips it.
	Elements int64 `bson:"elements,omitempty"`
	// Dead is the count of dead entries the dump recorded. Unlike the two
	// above it is always compared, zero included: a dump that recorded none
	// and streamed a dead section is as wrong as one that lost some.
	Dead int64 `bson:"dead,omitempty"`
}

// RestoreDoneResponse reports what was actually applied.
type RestoreDoneResponse struct {
	Success bool   `bson:"success"`
	Message string `bson:"message,omitempty"`
	Applied int64  `bson:"applied"`
	// Skipped counts documents received but not written — today this is
	// exactly blob metadata documents, whose payloads dump never carries
	// (dump-restore-spec.md §3, §12.9).
	Skipped int64 `bson:"skipped,omitempty"`
	// ElementsApplied and ElementsSkipped count field-store element records;
	// a blobStore field's are skipped, as a blob bucket's documents are,
	// because a dump carries no payload.
	ElementsApplied int64 `bson:"elements_applied,omitempty"`
	ElementsSkipped int64 `bson:"elements_skipped,omitempty"`
	// DeadApplied counts dead entries written back as dead.
	DeadApplied int64 `bson:"dead_applied,omitempty"`
}

// restoreSession is the node-local state for one bucket's live restore.
// Unlike dumpSession (server/dump.go, exactly one per server), many of
// these can exist at once: one per (database, bucket, wire session id), and
// more than one for the same bucket when two merges legitimately overlap
// (dump-restore-spec.md §9.7).
type restoreSession struct {
	id         string
	database   string
	bucket     string
	bucketType BucketType
	startedAt  time.Time
	startedBy  string

	// releasePin releases this session's hold on claimBucketRestorePin's
	// per-bucket accounting. Set once in startRestore, called exactly once
	// by releaseRestoreSession.
	releasePin func()

	// setupMu guards watchCancel, lifoSpill, lifoPeakBytes and released:
	// watchCancel is written once from startRestore before the
	// connection-watcher goroutine is started, and lifoSpill is appended to by
	// every OpRestoreChunk call for a LIFO bucket and taken once, by
	// OpRestoreDone to push it or by the release to discard it.
	setupMu     sync.Mutex
	watchCancel context.CancelFunc

	// lifoSpill holds a LIFO bucket's documents on disk, in the order
	// OpDumpChunk wrote them: pop order, newest first. Reproducing that pop
	// order means pushing oldest first, the reverse, which cannot start until
	// the last element has arrived, so LIFO is the one bucket type this
	// session does not apply per document (server/restore_lifo_spill.go).
	// Every other type applies immediately (dump-restore-spec.md §9.2's
	// ordinary per-document write path). Nil until the first LIFO document.
	lifoSpill *lifoSpill
	// lifoPeakBytes is the most memory the LIFO spill has held at once.
	lifoPeakBytes int64
	// released is set by releaseRestoreSession, after which no spill is made.
	released bool

	applied int64 // atomic
	skipped int64 // atomic

	// storeEngines is the restored bucket's store fields by path — the name,
	// or for an L3 store its L2 store's name, NUL, its name — which routes
	// each element record to its verb.
	storeEngines map[string]store.FieldEngine
	// positions maps an L2 fifo or lifo element's position in the dump to
	// the position its restored push took, so an L3 element restored after
	// it names the element it hangs on. Guarded by setupMu.
	positions       map[restoredPosition]int64
	elementsApplied int64 // atomic
	elementsSkipped int64 // atomic
	deadApplied     int64 // atomic

	releaseOnce sync.Once
}

// heapDumpDocument is the wire shape store/heap.go's IterateDump emits
// for one entry: the id and priority live outside the stored value (the
// priority only in the Badger key), so restoring a heap bucket means
// re-attaching them rather than passing the document straight through
// InsertCtx the way hash/btree/FIFO do — see store/heap.go's
// IterateDump doc comment, which anticipates exactly this.
type heapDumpDocument struct {
	ID       string   `bson:"_id_"`
	Priority int64    `bson:"priority"`
	Value    bson.Raw `bson:"value"`
}

// validRestoreNameComponent rejects a database or bucket name that is unsafe
// as a single path component (dump-restore-spec.md §9.1, §12.12): empty, a NUL
// byte, an absolute path, a path separator, or "." / "..". A database name is
// still a directory under the data directory; a bucket name is a key only
// (validateBucketName).
func validRestoreNameComponent(kind, name string) error {
	return namepolicy.PathComponent(kind, name)
}

// validateBucketName checks a database and bucket name pair, as names only.
// On the shared layout (spec §14.1) a bucket is a key range and its blob
// payloads are named by its slot and id, so no bucket name is joined into a
// filesystem path any more, and the canonicalise-and-assert-inside-root check
// dump-restore-spec.md §19 A-12 asked for has nothing left to canonicalise.
// The name rules stay: a name that could traverse as a path is refused
// wherever a bucket is created, dropped or restored.
func validateBucketName(database, bucket string) error {
	return legacyNameError(namepolicy.CheckBucket(database, bucket))
}

// legacyNameError turns the shared policy's violation into the error this door has always
// returned: a path-safety error wrapping ErrRestorePathEscapesRoot, or a *nameTooLongError.
func legacyNameError(violation *namepolicy.Violation) error {
	switch {
	case violation == nil:
		return nil
	case violation.Rule == namepolicy.RuleUnsafe:
		return violation.Err
	case violation.Rule == namepolicy.RuleTooLong:
		return &nameTooLongError{kind: violation.Kind, database: violation.Database, name: violation.Name}
	}
	return fmt.Errorf("%s name %q is not allowed", violation.Kind, violation.Name)
}

// restoreIndexOptions builds the CreateBucketRequest.Options shape
// parseIndexSpecs (server/index_declaration.go) accepts, from a dump manifest's plain
// index name list, or nil for no declared indexes. Distinct from
// server/catalog.go's indexOptions, which rebuilds the same shape from a
// catalog's typed []catalogIndex — a dump manifest only ever records plain
// field names (dump-restore-spec.md §4's "indexes": ["Email"]), never the
// range/equality kind, so it cannot reuse that helper's input type.
//
// A dump's descriptors, when present, are declared instead, in id order — so
// a unique index restores unique and every id restores as its source's. The
// names are declared only for a manifest that carries no descriptor.
func restoreIndexOptions(indexes []string, specs []DumpIndexSpec) map[string]interface{} {
	if len(specs) > 0 {
		declared := make([]interface{}, 0, len(specs))
		for _, spec := range specs {
			index := map[string]interface{}{"fields": spec.Fields, "name": spec.Name}
			if spec.Unique {
				index["unique"] = true
			}
			if spec.ExpireAfterSeconds != nil {
				index["expire_after_seconds"] = *spec.ExpireAfterSeconds
			}
			declared = append(declared, index)
		}
		return map[string]interface{}{"indices": declared}
	}
	if len(indexes) == 0 {
		return nil
	}
	return map[string]interface{}{"indices": indexes}
}

// restoreCreateOptions is the create options of a restored bucket: its
// indexes, its store fields declared afresh by name, and the placement its
// dump records.
func restoreCreateOptions(indexes []string, specs []DumpIndexSpec, stores []DumpStore, placement string) map[string]interface{} {
	options := restoreIndexOptions(indexes, specs)
	if len(stores) == 0 && placement == "" {
		return options
	}
	if options == nil {
		options = make(map[string]interface{})
	}
	if len(stores) > 0 {
		options["stores"] = restoreStoreDeclarations(stores, "")
	}
	if placement != "" {
		options["placement"] = placement
	}
	return options
}

// restoreStoreDeclarations is the declaration list, nested by parent name,
// of the stores declared on parent's elements ("" for the documents).
func restoreStoreDeclarations(stores []DumpStore, parent string) []interface{} {
	var declared []interface{}
	for _, field := range stores {
		if field.Parent != parent {
			continue
		}
		entry := map[string]interface{}{"name": field.Name, "engine": field.Engine}
		if nested := restoreStoreDeclarations(stores, field.Name); len(nested) > 0 {
			entry["fields"] = nested
		}
		declared = append(declared, entry)
	}
	return declared
}

// declareRestoredStores adds a manifest's store fields to an existing bucket
// through the declare DDL: additive, an identity for a field it already
// declares, and refused for one declared with another engine.
func (s *Server) declareRestoredStores(ctx context.Context, database, bucket string, stores []DumpStore) error {
	if len(stores) == 0 {
		return nil
	}
	req := DeclareStoreFieldsRequest{Database: database, Name: bucket}
	for _, entry := range restoreStoreDeclarations(stores, "") {
		req.Stores = append(req.Stores, entry.(map[string]interface{}))
	}
	payload, err := bson.Marshal(req)
	if err != nil {
		return fmt.Errorf("encoding the store declaration of %s/%s: %w", database, bucket, err)
	}
	resp, err := s.routeOrdinaryDDL(ctx, transport.OpDeclareStoreFields, payload)
	if err != nil {
		return fmt.Errorf("declaring the store fields of %s/%s: %w", database, bucket, err)
	}
	if err := frameError(resp); err != nil {
		return fmt.Errorf("declaring the store fields of %s/%s: %w", database, bucket, err)
	}
	return nil
}

// restoreStoreEngines maps a manifest's store fields, by storePath, to their
// engines.
func restoreStoreEngines(stores []DumpStore) (map[string]store.FieldEngine, error) {
	engines := make(map[string]store.FieldEngine, len(stores))
	for _, field := range stores {
		engine, err := store.ParseFieldEngine(field.Engine)
		if err != nil {
			return nil, fmt.Errorf("the dump declares store field %q: %w", field.Name, err)
		}
		engines[storePath(field.Parent, field.Name)] = engine
	}
	return engines, nil
}

// storePath keys a store field by its L2 parent's name and its own; a NUL
// separates them because no field name holds one.
func storePath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "\x00" + name
}

// restoredPosition is one L2 fifo or lifo element in the dump: its
// document, its field and its position there.
type restoredPosition struct {
	owner, field string
	position     int64
}

// missingFrom returns the entries of want not present in have, preserving
// want's order.
func missingFrom(want, have []string) []string {
	if len(want) == 0 {
		return nil
	}
	present := make(map[string]bool, len(have))
	for _, name := range have {
		present[name] = true
	}
	var missing []string
	for _, name := range want {
		if !present[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

// routeOrdinaryDDL is routeOrdinaryWrite's twin for the database/bucket
// lifecycle changes restore makes on a client's behalf — creating the
// database and bucket a fresh restore target needs, or --drop's delete.
//
// On a multi-node cluster these have to go through the exact same DDL
// replication path a live `create bucket`/`drop bucket` command takes
// (write_path.go's applyLocal DDL exemption, reached via
// Cluster.handleClientFrame → Cluster.write → applyAsOwner), or the bucket
// exists only on whichever node happened to run startRestore. Fix round 1:
// this is what actually produced TestRestoreReplicatesAcrossCluster's
// "write concern not satisfied" timeout on the first pass of the Critical
// fix — the *document* write's replication was correct, but the bucket it
// was replicating into had never been created on the replica, so
// applyLocal's bucketStoreOpen check (server/write_path.go) rejected every
// entry shipped to it, silently, as a non-ack rather than a visible error.
func (s *Server) routeOrdinaryDDL(ctx context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	frame := &transport.Frame{Opcode: op, Payload: payload}
	if s.cluster != nil && s.cluster.Enabled() {
		return s.cluster.handleClientFrame(ctx, frame)
	}
	return s.fencedDispatch(ctx, frame)
}

// ensureDatabase creates database if it does not already exist, routed
// through routeOrdinaryDDL so every cluster member learns about it, not
// just this node. Checking existence first (rather than calling
// OpCreateDB unconditionally and inspecting its "already exists" response)
// avoids parsing a message string to tell a benign no-op from a real
// failure. This node's view is not the cluster's: a joiner learns a database
// only with its first copied bucket, so a database it does not know is asked
// about on the other members first, and one they hold is not created again in
// a second incarnation.
func (s *Server) ensureDatabase(ctx context.Context, database string) error {
	if s.serverDataAccessor().getDB(database) != nil {
		return nil
	}
	if s.cluster.Enabled() {
		live, _, err := s.generationsOfName(ctx, database, refuseWhenSilent)
		if err != nil {
			return fmt.Errorf("asking the members about database %q for restore: %w", database, err)
		}
		if len(live) > 0 {
			return nil
		}
	}
	payload, err := bson.Marshal(CreateDatabaseRequest{Name: database})
	if err != nil {
		return fmt.Errorf("encoding create-database request for restore: %w", err)
	}
	resp, err := s.routeOrdinaryDDL(ctx, transport.OpCreateDB, payload)
	if err != nil {
		return fmt.Errorf("creating database %q for restore: %w", database, err)
	}
	if err := frameError(resp); err != nil {
		return fmt.Errorf("creating database %q for restore: %w", database, err)
	}
	var createResp CreateDatabaseResponse
	if err := bson.Unmarshal(resp.Payload, &createResp); err != nil {
		return fmt.Errorf("decoding create-database response for %q: %w", database, err)
	}
	if !createResp.Success {
		return fmt.Errorf("creating database %q for restore: %s", database, createResp.Message)
	}
	return nil
}

// createBucketForRestore creates req's bucket, routed through
// routeOrdinaryDDL for the same cluster-visibility reason ensureDatabase
// is.
func (s *Server) createBucketForRestore(ctx context.Context, req CreateBucketRequest) error {
	// The create's own shape check, asked here as the direct create's door asks
	// it: that door answers a refusal in-band with no code, which this wrap would
	// read as INTERNAL, so the typed refusal is returned instead (BUG-0665).
	if err := s.createShapeRefusal(req); err != nil {
		return fmt.Errorf("creating bucket %s/%s for restore: %w", req.Database, req.Name, err)
	}
	payload, err := bson.Marshal(req)
	if err != nil {
		return fmt.Errorf("encoding create-bucket request for restore: %w", err)
	}
	resp, err := s.routeOrdinaryDDL(ctx, transport.OpCreateBucket, payload)
	if err != nil {
		return fmt.Errorf("creating bucket %s/%s for restore: %w", req.Database, req.Name, err)
	}
	if err := frameError(resp); err != nil {
		return fmt.Errorf("creating bucket %s/%s for restore: %w", req.Database, req.Name, err)
	}
	var createResp CreateBucketResponse
	if err := bson.Unmarshal(resp.Payload, &createResp); err != nil {
		return fmt.Errorf("decoding create-bucket response for %s/%s: %w", req.Database, req.Name, err)
	}
	if !createResp.Success {
		return fmt.Errorf("creating bucket %s/%s for restore: %s", req.Database, req.Name, createResp.Message)
	}
	return nil
}

// deleteBucketForRestore drops req's bucket for --drop, routed through
// routeOrdinaryDDL for the same reason.
func (s *Server) deleteBucketForRestore(ctx context.Context, req DeleteBucketRequest) error {
	payload, err := bson.Marshal(req)
	if err != nil {
		return fmt.Errorf("encoding delete-bucket request for restore --drop: %w", err)
	}
	resp, err := s.routeOrdinaryDDL(ctx, transport.OpDeleteBucket, payload)
	if err != nil {
		return fmt.Errorf("--drop: removing existing bucket %s/%s: %w", req.Database, req.Name, err)
	}
	if err := frameError(resp); err != nil {
		return fmt.Errorf("--drop: removing existing bucket %s/%s: %w", req.Database, req.Name, err)
	}
	var deleteResp DeleteBucketResponse
	if err := bson.Unmarshal(resp.Payload, &deleteResp); err != nil {
		return fmt.Errorf("decoding delete-bucket response for %s/%s: %w", req.Database, req.Name, err)
	}
	if !deleteResp.Success {
		return fmt.Errorf("--drop: removing existing bucket %s/%s: %s", req.Database, req.Name, deleteResp.Message)
	}
	return nil
}

// dropBlobCheck is what checkDropBlobDataLoss weighs: both sides of a --drop,
// each as the bucket's own type and as the blobStore fields it declares.
type dropBlobCheck struct {
	Database, Bucket   string
	ExistingType       BucketType
	DumpType           BucketType
	ExistingBlobFields []string
	DumpBlobFields     []string
}

// checkDropBlobDataLoss refuses --drop against an existing bucket when
// blob payloads are on either side of the drop: the target already is a
// blob bucket, or the dump being restored is a blob dump. Both are
// destructive in the same way — dump never captures blob payloads
// (server/restore.go's BucketTypeBlob case in applyRestoreDocument only
// counts and discards the metadata document), so --drop recreating the
// bucket permanently loses data restore cannot put back, whether or not
// the store types otherwise match (dump-restore-spec.md §9.4). A blobStore
// field is the same loss at a smaller grain: applyRestoreElement skips its
// elements, so a bucket that declares one on either side is refused too
// . Only called once an existing bucket has been confirmed — a
// fresh target has nothing to lose.
func checkDropBlobDataLoss(check dropBlobCheck) error {
	if check.ExistingType == BucketTypeBlob || check.DumpType == BucketTypeBlob {
		return fmt.Errorf("%w: bucket %s/%s is type %s, the dump is type %s — dump never captures blob payloads, so --drop would permanently destroy them recreating this bucket; run 'drop bucket %s' yourself first if this is really what you want, or restore into a different bucket",
			ErrRestoreDropBlobDataLoss, check.Database, check.Bucket, check.ExistingType, check.DumpType, check.Bucket)
	}
	if len(check.ExistingBlobFields) == 0 && len(check.DumpBlobFields) == 0 {
		return nil
	}
	side, fields := "the bucket declares", check.ExistingBlobFields
	if len(fields) == 0 {
		side, fields = "the dump declares", check.DumpBlobFields
	}
	return fmt.Errorf("%w: %s the blobStore field(s) %s of %s/%s — dump never captures blob field elements, so --drop would permanently destroy them recreating this bucket; run 'drop bucket %s' yourself first if this is really what you want, or restore into a different bucket",
		ErrRestoreDropBlobDataLoss, side, strings.Join(fields, ", "), check.Database, check.Bucket, check.Bucket)
}

// BlobFieldNames lists the blobStore fields a set of store declarations
// holds, a nested one as "Parent.Field", in declaration order. A dump does
// not carry a blob field's elements, so the CLI names these in its warning
// and the server in its --drop refusal.
func BlobFieldNames(stores []DumpStore) []string {
	var names []string
	for _, field := range stores {
		if !strings.EqualFold(field.Engine, store.FieldBlob.String()) {
			continue
		}
		if field.Parent != "" {
			names = append(names, field.Parent+"."+field.Name)
			continue
		}
		names = append(names, field.Name)
	}
	return names
}

// blobFieldsOfBucket is BlobFieldNames of the stores a bucket declares now.
func blobFieldsOfBucket(view *inMemoryBucket) []string {
	declared := view.storeFields().stores
	names := make(map[int32]string, len(declared))
	for _, field := range declared {
		names[field.ID] = field.Name
	}
	stores := make([]DumpStore, 0, len(declared))
	for _, field := range declared {
		stores = append(stores, DumpStore{Name: field.Name, Engine: field.Engine, Parent: names[field.Parent]})
	}
	return BlobFieldNames(stores)
}

// dryRunRestoreStart answers a DryRun OpRestoreStart: every read-only check
// (disk headroom, store-type/index comparison against the target) without
// claiming the bucket pin, creating a database or bucket, dropping anything,
// or opening a session — `hoarddb restore --dry-run` calls this once per
// bucket and sends no further frames for it.
func (s *Server) dryRunRestoreStart(req RestoreStartRequest) (*RestoreStartResponse, error) {
	if err := s.checkRestoreDiskHeadroom(req.Database, req.Bucket, req.StoreType, req.Bytes); err != nil {
		return nil, err
	}
	existing, existErr := s.resolveCommittedBucket(req.Database, req.Bucket)
	plan := s.planRestoreFactor(req, existing)
	if existErr != nil {
		return &RestoreStartResponse{Success: true, Created: true, FactorNotRestored: plan.notRestored,
			Message: fmt.Sprintf("would create database %q and bucket %q (type %s)", req.Database, req.Bucket, req.StoreType)}, nil
	}
	existingType := existing.bucketType
	existingIdx := bucketIndexNames(existing)

	if req.Drop {
		if err := checkDropBlobDataLoss(dropBlobCheck{
			Database: req.Database, Bucket: req.Bucket, ExistingType: existingType, DumpType: req.StoreType,
			ExistingBlobFields: blobFieldsOfBucket(existing), DumpBlobFields: BlobFieldNames(req.Stores),
		}); err != nil {
			return nil, err
		}
	}
	if existingType != req.StoreType {
		if req.Drop {
			return nil, fmt.Errorf("%w: bucket %s/%s is type %s; the dump is type %s — --drop would refuse this, since it discards a %s bucket to recreate it as a different type",
				ErrRestoreStoreTypeMismatch, req.Database, req.Bucket, existingType, req.StoreType, existingType)
		}
		return nil, fmt.Errorf("%w: bucket %s/%s is type %s; the dump is type %s — restore with --drop to replace it, or restore into a different bucket",
			ErrRestoreStoreTypeMismatch, req.Database, req.Bucket, existingType, req.StoreType)
	}
	if req.Drop {
		return &RestoreStartResponse{Success: true, Dropped: true, Created: true, FactorNotRestored: plan.notRestored, FactorLowered: plan.lowered,
			Message: fmt.Sprintf("would drop and recreate bucket %s/%s (type %s)", req.Database, req.Bucket, req.StoreType)}, nil
	}
	missing := missingFrom(req.Indexes, existingIdx)
	return &RestoreStartResponse{Success: true, MissingIndexes: missing, FactorKept: plan.kept,
		Message: fmt.Sprintf("would merge into existing bucket %s/%s (type %s)", req.Database, req.Bucket, req.StoreType)}, nil
}

// checkRestoreDeadEntries refuses, before any document is written, a dump whose
// dead entries this restore cannot write back (claim-ack-spec §7.3). A dump
// that records dead entries for a bucket that cannot hold them is a corrupt or
// hostile one. And while a cluster member lacks queue_claim, DEAD_RESTORE is
// refused by the write gate, which would happen only after every document had
// been restored; asking here leaves the target as it was.
func (s *Server) checkRestoreDeadEntries(req RestoreStartRequest) error {
	if req.Dead <= 0 {
		return nil
	}
	switch req.StoreType {
	case BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap:
	default:
		return fmt.Errorf("%w: the dump of %s/%s records %d dead entries, but a %s bucket has none; the dump directory may be corrupt",
			ErrRestoreFormatVersion, req.Database, req.Bucket, req.Dead, req.StoreType)
	}
	if s.cluster == nil || !s.cluster.Enabled() {
		return nil
	}
	member := s.cluster.memberWithout(featureQueueClaim)
	if member == "" {
		return nil
	}
	return &sentencedError{cause: errMemberLacksFeature, message: fmt.Sprintf(
		"Bucket %q (database %q): this dump holds %d dead entries, and restoring them needs every cluster member to run a hoardDB that supports %q, "+
			"and member %q does not. Nothing was restored.\n"+
			"Upgrade the cluster: stop every node once replication has settled, start the new version on all of them, then restore again.",
		req.Bucket, req.Database, req.Dead, featureQueueClaim, member)}
}

// startRestore handles OpRestoreStart: every check in dump-restore-spec.md
// §9.1 that can run before a single document is written, in this order —
// root gate, request shape, format version, path safety, the reserved
// namespace, the bucket pin (claimed before anything mutates, so two
// concurrent --drop attempts on the same bucket cannot both pass the
// store-type check and then both proceed), free disk space, then finally
// the store-type/index check against the target and (--drop only) the
// drop-and-recreate itself.
//
// Claiming the pin before the store-type/disk/drop checks — rather than
// after, as the brief's own numbered list orders it — is deliberate: those
// later checks only read state (the target bucket's type, the disk's free
// space) and are safe to run either before or after the pin is held, but
// the pin itself is what makes the destructive --drop branch race-safe
// against a second concurrent --drop on the same bucket. Every branch below
// still refuses before any bucket is dropped or created, which is the
// property dump-restore-spec.md §9.1 actually requires.
func (s *Server) startRestore(ctx context.Context, req RestoreStartRequest) (*RestoreStartResponse, error) {
	if err := requireRootIdentity(ctx, s); err != nil {
		return nil, err
	}
	if req.Database == "" || req.Bucket == "" {
		return nil, fmt.Errorf("restore requires a database and a bucket; got database=%q bucket=%q", req.Database, req.Bucket)
	}
	if !restorableFormat(req.FormatVersion) {
		return nil, fmt.Errorf("%w: dump is format_version %d, this server restores format_versions 2 and %d. Upgrade hoarddb to a build that restores format_version %d",
			ErrRestoreFormatVersion, req.FormatVersion, dumpManifestFormatVersion, req.FormatVersion)
	}
	switch req.StoreType {
	case BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBlob:
	default:
		return nil, &sentencedError{cause: errCreateInputRefused, message: fmt.Sprintf("restore %s/%s: unsupported store_type %q", req.Database, req.Bucket, req.StoreType)}
	}
	if err := validateBucketName(req.Database, req.Bucket); err != nil {
		return nil, err
	}
	if err := restoreNameRefusal(req.Database, req.Bucket); err != nil {
		return nil, err
	}
	if IsReservedDatabaseName(req.Database) {
		return nil, fmt.Errorf("%w: database %q is reserved for internal use and cannot be restored into",
			ErrRestoreReservedName, req.Database)
	}

	if err := s.checkRestoreDeadEntries(req); err != nil {
		return nil, err
	}

	if req.DryRun {
		return s.dryRunRestoreStart(req)
	}

	release, err := s.claimBucketRestorePin(req.Database, req.Bucket, req.Drop)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			release()
		}
	}()

	if err := s.checkRestoreDiskHeadroom(req.Database, req.Bucket, req.StoreType, req.Bytes); err != nil {
		return nil, err
	}

	username := sessionUsername(ctx)
	existing, existErr := s.settledCommittedBucket(ctx, req.Database, req.Bucket)
	bucketExists := existErr == nil
	plan := s.planRestoreFactor(req, existing)
	// A bucket this owner does not hold may be one it has not copied yet. Both
	// branches below would then create it, and its holders would drop their
	// copies for the empty one. Refused with retry before anything is
	// created; once the copy lands, the restore merges into it or drops it.
	if !bucketExists && s.cluster.Enabled() {
		if _, err := s.cluster.refuseCreateBesideCopy(ctx, req.Database, req.Bucket, plan.placementFactor(s.cluster.rf)); err != nil {
			return nil, err
		}
	}

	var created, dropped bool
	var missingIndexes []string

	if req.Drop {
		if bucketExists {
			existingType := existing.bucketType
			existingCount := bucketRoughCount(existing)
			if err := checkDropBlobDataLoss(dropBlobCheck{
				Database: req.Database, Bucket: req.Bucket, ExistingType: existingType, DumpType: req.StoreType,
				ExistingBlobFields: blobFieldsOfBucket(existing), DumpBlobFields: BlobFieldNames(req.Stores),
			}); err != nil {
				return nil, err
			}
			if existingType != req.StoreType {
				return nil, fmt.Errorf("%w: bucket %s/%s is type %s with roughly %d entries; the dump is type %s — --drop refuses to discard a %s bucket to recreate it as a different type. Run 'drop bucket %s' yourself first if this is really what you want, or restore into a different bucket",
					ErrRestoreStoreTypeMismatch, req.Database, req.Bucket, existingType, existingCount, req.StoreType, existingType, req.Bucket)
			}
			// The create below can be refused (a unique field on a spread bucket);
			// that is decided before the delete, so a refused restore leaves
			// the live bucket and its data alone.
			if err := restoreCreateRefusal(req); err != nil {
				return nil, err
			}
			if err := s.deleteBucketForRestore(ctx, DeleteBucketRequest{Database: req.Database, Name: req.Bucket}); err != nil {
				return nil, err
			}
			dropped = true
		}
		if err := s.ensureDatabase(ctx, req.Database); err != nil {
			return nil, err
		}
		if err := s.createBucketForRestore(ctx, CreateBucketRequest{
			Database: req.Database, Name: req.Bucket, Type: req.StoreType, Options: restoreCreateOptions(req.Indexes, req.IndexSpecs, req.Stores, req.Placement),
			ReplicationFactor: plan.declare,
		}); err != nil {
			return nil, err
		}
		created = true
		s.audit.Log(AuditEntry{Event: EventRestoreDrop, User: username, Target: req.Database + "/" + req.Bucket, Result: "ok"})
	} else if bucketExists {
		existingType := existing.bucketType
		existingIdx := bucketIndexNames(existing)
		if existingType != req.StoreType {
			return nil, fmt.Errorf("%w: bucket %s/%s is type %s; the dump is type %s — restore with --drop to replace it, or restore into a different bucket",
				ErrRestoreStoreTypeMismatch, req.Database, req.Bucket, existingType, req.StoreType)
		}
		missingIndexes = missingFrom(req.Indexes, existingIdx)
		if err := s.declareRestoredStores(ctx, req.Database, req.Bucket, req.Stores); err != nil {
			return nil, err
		}
	} else {
		if err := s.ensureDatabase(ctx, req.Database); err != nil {
			return nil, err
		}
		if err := s.createBucketForRestore(ctx, CreateBucketRequest{
			Database: req.Database, Name: req.Bucket, Type: req.StoreType, Options: restoreCreateOptions(req.Indexes, req.IndexSpecs, req.Stores, req.Placement),
			ReplicationFactor: plan.declare,
		}); err != nil {
			return nil, err
		}
		created = true
	}
	storeEngines, err := restoreStoreEngines(req.Stores)
	if err != nil {
		return nil, err
	}

	session := &restoreSession{
		id:         uuid.UUID(ids.NewRequestID()).String(),
		database:   req.Database,
		bucket:     req.Bucket,
		bucketType: req.StoreType,
		startedAt:  time.Now(),
		startedBy:  username,
		releasePin: release,

		storeEngines: storeEngines,
	}
	s.restoreSessionMu.Lock()
	if s.restoreSessions == nil {
		s.restoreSessions = make(map[string]*restoreSession)
	}
	s.restoreSessions[session.id] = session
	s.restoreSessionMu.Unlock()
	s.watchRestoreConnection(ctx, session)
	committed = true

	s.audit.Log(AuditEntry{Event: EventRestoreStart, User: username, Target: req.Database + "/" + req.Bucket, Result: "ok"})

	return &RestoreStartResponse{
		Success:           true,
		SessionID:         session.id,
		Created:           created,
		Dropped:           dropped,
		MissingIndexes:    missingIndexes,
		FactorNotRestored: plan.notRestored,
		FactorKept:        plan.kept,
		FactorLowered:     plan.lowered,
	}, nil
}

// settledCommittedBucket resolves database/bucket for a restore after giving
// the take-over of a bucket a removed node owned the leave to copy it here and
// waiting for it, as a create of that name does
// (Cluster.awaitCopyARemovedNodeMayHold, awaitTakeOver). The
// create startRestore would send next makes it anyway and is answered "already
// exists" when the copy lands; asking first lets the restore take the
// bucket-exists branch (type check, store declarations, missing indexes, --drop)
// instead of reporting a create. Cost (rule 11): one local catalog lookup per
// restore; the wait is the create's own and is paid only for a name a removed
// node owned. No per-bucket state.
func (s *Server) settledCommittedBucket(ctx context.Context, database, bucket string) (*inMemoryBucket, error) {
	existing, err := s.resolveCommittedBucket(database, bucket)
	if err == nil || !s.cluster.Enabled() {
		return existing, err
	}
	if err := s.cluster.awaitCopyARemovedNodeMayHold(ctx, database, bucket); err != nil {
		return nil, fmt.Errorf("waiting for a copy of %s/%s a removed node may hold: %w", database, bucket, err)
	}
	// The create fence waits for the take-over next, before it asks anything;
	// the copy lands in that wait, so look the bucket up only after it.
	if err := s.cluster.awaitTakeOver(ctx, placementOf(database, bucket), database, bucket); err != nil {
		return nil, fmt.Errorf("waiting for the take-over of %s/%s: %w", database, bucket, err)
	}
	return s.resolveCommittedBucket(database, bucket)
}

// bucketRoughCount returns a best-effort entry count for an error message —
// "best-effort" because a segment-backed queue has no O(1) counter and this
// runs while the caller already holds a read lock, so it is only ever
// called against the cheap engines. Never fails the request: a count that
// cannot be read is reported as 0 rather than blocking the drop refusal
// message itself.
func bucketRoughCount(b *inMemoryBucket) int64 {
	e, err := bucketEnumerator(b)
	if err != nil {
		return 0
	}
	n, err := e.DocCount()
	if err != nil {
		return 0
	}
	return n
}

// checkRestoreDiskHeadroom refuses a bucket's restore when free space is
// below the configured multiplier of its raw dump bytes
// (dump-restore-spec.md §9.1, §11, §19 A-15). Measurement failure fails
// open — the same choice server/disk_headroom.go's diskHeadroomOK makes for
// the membership-join guard — rather than refuse every restore on a
// platform or sandbox this probe cannot read.
func (s *Server) checkRestoreDiskHeadroom(database, bucket string, storeType BucketType, rawBytes int64) error {
	free, err := diskFreeBytes(s.config.Storage.DataDir)
	if err != nil || free < 0 {
		return nil
	}
	multiplier := s.config.Server.RestoreDiskHeadroomMultiplier
	if multiplier <= 0 {
		multiplier = defaultRestoreDiskHeadroomMultiplier
	}
	required := int64(float64(rawBytes) * multiplier)
	spillNote := ""
	if storeType == BucketTypeLIFO {
		// A LIFO restore holds its documents in a spill file until the finish
		// pushes them (server/restore_lifo_spill.go): about the dump's size again.
		required += rawBytes
		spillNote = fmt.Sprintf(", plus %d for the spill file a LIFO restore writes until it finishes", rawBytes)
	}
	if free < required {
		return fmt.Errorf("%w: bucket %s/%s needs roughly %d free byte(s) (%.1fx its %d dump byte(s), to account for WAL/LSM/index write amplification%s) but this node has %d free — free disk space, or lower HOARDDB_RESTORE_DISK_HEADROOM_MULTIPLIER if the smaller margin is acceptable here",
			ErrRestoreInsufficientDiskSpace, database, bucket, required, multiplier, rawBytes, spillNote, free)
	}
	return nil
}

// watchRestoreConnection releases the session immediately when ctx is done —
// the connection closing or resetting — the same abort discipline
// server/dump.go's watchDumpConnection applies to a dump session
// (dump-restore-spec.md §7.2, §16.3's reasoning applies equally here: a
// killed client must not leave a pin held forever). As with dump, this
// watches the connection that carried OpRestoreStart; a forwarded cross-node
// restore's later chunks may travel a different pooled internode connection
// (transport.InternodePoolSize), so on a multi-node cluster this is a
// best-effort abort signal, not an exact one — the same characteristic
// Task 2's dump session already has.
func (s *Server) watchRestoreConnection(ctx context.Context, session *restoreSession) {
	watchCtx, cancel := context.WithCancel(context.Background())
	session.setupMu.Lock()
	session.watchCancel = cancel
	session.setupMu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			s.restoreSessionMu.Lock()
			if s.restoreSessions[session.id] == session {
				delete(s.restoreSessions, session.id)
			}
			s.restoreSessionMu.Unlock()
			s.logger.Warn("restore aborted: connection closed", "database", session.database, "bucket", session.bucket)
			s.releaseRestoreSession(session, fmt.Errorf("restore of %s/%s aborted: client connection closed", session.database, session.bucket))
		case <-watchCtx.Done():
		}
	}()
}

// releaseRestoreSession releases everything a claimed session holds, exactly
// once: the connection-close watcher and the bucket pin. forcedErr is
// non-nil only when the release was not requested by the caller (a closed
// connection), which is what distinguishes an EventRestoreAborted from an
// ordinary OpRestoreDone — the same convention server/dump.go's releaseDump
// uses for EventDumpAborted.
func (s *Server) releaseRestoreSession(session *restoreSession, forcedErr error) {
	session.releaseOnce.Do(func() {
		session.setupMu.Lock()
		watchCancel := session.watchCancel
		session.released = true
		session.setupMu.Unlock()
		if watchCancel != nil {
			watchCancel()
		}
		// A restore that ends before its finish pushed nothing of its LIFO
		// spill, and never will.
		if spill := session.takeLIFOSpill(); spill != nil {
			if err := spill.discard(); err != nil {
				s.logger.Warn("could not remove a closed LIFO restore's spill file; the next start removes it",
					"database", session.database, "bucket", session.bucket, "error", err)
			}
		}
		if session.releasePin != nil {
			session.releasePin()
		}
		if forcedErr != nil {
			s.audit.Log(AuditEntry{Event: EventRestoreAborted, User: session.startedBy,
				Target: session.database + "/" + session.bucket, Result: forcedErr.Error()})
		}
	})
}

// activeRestoreFor resolves the session an OpRestoreChunk/OpRestoreDone
// request applies to, or a named refusal.
func (s *Server) activeRestoreFor(sessionID, database, bucket string) (*restoreSession, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: no session_id given for %s/%s — call restore start first", ErrRestoreNoActiveSession, database, bucket)
	}
	s.restoreSessionMu.Lock()
	session := s.restoreSessions[sessionID]
	s.restoreSessionMu.Unlock()
	if session == nil {
		return nil, fmt.Errorf("%w: session %q for %s/%s — call restore start first, or it already finished",
			ErrRestoreNoActiveSession, sessionID, database, bucket)
	}
	if session.database != database || session.bucket != bucket {
		return nil, fmt.Errorf("%w: session %q was started for %s/%s, not %s/%s",
			ErrRestoreNoActiveSession, sessionID, session.database, session.bucket, database, bucket)
	}
	return session, nil
}

// parseBSONDocuments splits a concatenation of whole BSON documents back
// into individual document byte slices: each document's own leading 4-byte
// little-endian length says how much of the stream belongs to it — the
// same self-describing framing bsondump (and server/dump_test.go's
// splitBSONDocuments, which verifies the dump side of this exact contract)
// rely on. A restore chunk that does not divide evenly into whole documents
// is refused rather than guessed at: restore is a boundary where a hostile
// or merely broken client is expected, and the CLI (cli/dump.go's own copy
// of this logic) never sends a chunk that splits a document.
func parseBSONDocuments(data []byte) ([][]byte, error) {
	var docs [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("trailing %d byte(s) are not a full BSON length prefix", len(data))
		}
		n := int(binary.LittleEndian.Uint32(data[:4]))
		if n < 5 || n > len(data) {
			return nil, fmt.Errorf("invalid BSON document length %d in %d remaining byte(s)", n, len(data))
		}
		docs = append(docs, data[:n])
		data = data[n:]
	}
	return docs, nil
}

// restoreChunk handles OpRestoreChunk: decode the chunk into whole documents
// and apply each immediately, except for a LIFO bucket, which spills to disk
// until OpRestoreDone (see restoreSession.lifoSpill).
func (s *Server) restoreChunk(ctx context.Context, req RestoreChunkRequest) (*RestoreChunkResponse, error) {
	if err := requireRootIdentity(ctx, s); err != nil {
		return nil, err
	}
	session, err := s.activeRestoreFor(req.SessionID, req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	docs, err := parseBSONDocuments(req.Data)
	if err != nil {
		return nil, fmt.Errorf("restore chunk for %s/%s carried malformed BSON: %w", req.Database, req.Bucket, err)
	}
	recordLimit, knownSection := restoreRecordDepthLimit(req.Section, session.bucketType)
	for _, raw := range docs {
		// A chunk's records travel inside a binary value, which the request's
		// own validation does not descend into, so each record is validated
		// here before anything decodes it, at the depth its kind allows: a
		// record wraps the client's document in levels of its own
		// (restoreRecordDepthLimit). A section this server does not know is
		// refused below, undecoded.
		if knownSection {
			if err := refuseDumpedRecord(raw, recordLimit); err != nil {
				return nil, err
			}
		}
		switch req.Section {
		case "":
			err = s.applyRestoreDocument(ctx, session, raw)
		case dumpSectionStores:
			err = s.applyRestoreElement(ctx, session, raw)
		case dumpSectionDead:
			err = s.applyRestoreDead(ctx, session, raw)
		default:
			// A section this server does not know would otherwise be pushed as
			// documents, which is how a dead entry came back alive.
			err = fmt.Errorf("restore chunk for %s/%s names section %q, which this server does not restore; the sections are documents, %q and %q",
				req.Database, req.Bucket, req.Section, dumpSectionStores, dumpSectionDead)
		}
		if err != nil {
			return nil, err
		}
	}
	return &RestoreChunkResponse{Success: true, Documents: len(docs)}, nil
}

// restoreRecordDepthLimit is the deepest a record of a dump section may nest:
// the client's document, which may nest maxDocumentDepth levels, plus the levels
// its record wraps it in. It is per record kind and never one flat number, so a
// record deeper than a legal document and its own wrapper is still refused
// (BUG-0663). known is false for a section this server does not restore.
//
//   - documents (recordDepthLimit, recordDumped): a hash, btree, fifo or lifo record is the document itself; a
//     heap's is {_id_, priority, value}, and a blob's metadata document holds the
//     caller's metadata under one key: one level more.
//   - stores: {field, owner, key, value} holds the element in value, one level.
//     A blobStore element's value is the blob metadata document, one more, and
//     the engine is not known until the record is decoded, so this is the wider
//     of the two and applyRestoreElement refuses the narrower kind past its own.
//   - dead: the recordDead row of recordDepthLimit, which the pull reads too.
func restoreRecordDepthLimit(section string, bucketType BucketType) (limit int, known bool) {
	switch section {
	case "":
		return recordDepthLimit(recordDumped, bucketType), true
	case dumpSectionStores:
		return storeElementRecordDepthLimit(store.FieldBlob), true
	case dumpSectionDead:
		return recordDepthLimit(recordDead, bucketType), true
	}
	return 0, false
}

// refuseDumpedRecord validates one record of a dump at limit. The record's
// bytes are applied as they are, so only the refusal is kept.
func refuseDumpedRecord(raw []byte, limit int) error {
	_, err := validateAt(raw, limit)
	return refusalForValidation(err, func() string { return "The dumped record" })
}

// storeElementRecordDepthLimit is the deepest an element record of a store field
// of engine may nest: see restoreRecordDepthLimit.
func storeElementRecordDepthLimit(engine store.FieldEngine) int {
	if engine == store.FieldBlob {
		return maxDocumentDepth + 2
	}
	return maxDocumentDepth + 1
}

// routeOrdinaryWrite sends a synthetic, client-shaped frame through the
// exact entry point a live client's request takes — Cluster.handleClientFrame
// when clustering is enabled, direct local dispatch on a standalone node —
// the same branch dispatch.go's NewHandler uses for every other opcode.
//
// This is what "the ordinary write path" (dump-restore-spec.md §9.2) has to
// mean on a multi-node cluster: Server.InsertCtx and Server.Push only ever
// touch this node's own local storage. WAL append, version stamping and
// replica shipping happen one layer up, in Cluster.applyAsOwner
// (server/write_path.go), which is reached only by routing a frame through
// Cluster.handleClientFrame — calling InsertCtx/Push directly, as this file
// did before this fix, skipped that layer entirely and silently
// under-replicated every restored document at ReplicationFactor > 1 (fix
// round 1, Critical finding: invisible on the single-node path every test
// in this build ran at, since s.cluster is nil there and dispatch is the
// only path either way).
//
// op must be the opcode a live client would use for this exact operation
// (OpUpsert for hash/btree, OpFIFOPush/OpLIFOPush/OpHeapPush for a push) so
// Cluster.handleClientFrame's own opcode-keyed routing (replicationRequired,
// resolveDocumentIDs, decodeFrameMeta) treats a restored document exactly
// like a client's.
func (s *Server) routeOrdinaryWrite(ctx context.Context, op transport.Opcode, payload []byte) error {
	_, err := s.routeOrdinaryFrame(ctx, op, payload)
	return err
}

// routeOrdinaryFrame is routeOrdinaryWrite returning the answer, for a
// caller that needs what the write reports (a restored push's position).
func (s *Server) routeOrdinaryFrame(ctx context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	resp, err := s.routeWrite(ctx, op, payload)
	if err != nil {
		return nil, err
	}
	return resp, frameError(resp)
}

// routeWrite sends one write down the path a client's takes and returns the
// answer frame: on a cluster, handleClientFrame (forwarded to the owner,
// appended, shipped); standalone, fencedDispatch.
func (s *Server) routeWrite(ctx context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	frame := &transport.Frame{Opcode: op, Payload: payload}
	// A restored document is a client's write and is held to this node's limits
	// (server/document_limit.go), which a restore does not pass NewHandler to meet.
	// A deferred mark is deliberately not carried: restore is never measured by
	// the owner (BUG-0582).
	if _, err := s.refuseOversizedWrite(frame); err != nil {
		return errorFrame(frame, err), nil
	}
	if s.cluster != nil && s.cluster.Enabled() {
		return s.cluster.handleClientFrame(ctx, frame)
	}
	return s.fencedDispatch(ctx, frame)
}

// frameError turns an OpError response frame into a Go error, or nil for
// anything else. server/errors.go's own doc comment establishes the
// convention this decodes: an ordinary write refusal comes back as an error
// *frame* with a nil Go error (so the connection survives), which means
// routeOrdinaryWrite's caller cannot tell success from refusal by looking
// at the Go error alone — it has to inspect the frame the same way a real
// driver's response decoder would.
func frameError(frame *transport.Frame) error {
	if frame == nil || frame.Opcode != transport.OpError {
		return nil
	}
	var errResp ErrorResponse
	if len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &errResp); err == nil && errResp.Message != "" {
			return &frameRefusal{code: transport.ErrorCode(errResp.Code), message: errResp.Message}
		}
	}
	return errors.New("write refused")
}

// frameRefusal is the error frameError makes of a refusal frame: the frame's
// message, and the wire code it was answered with. codeFor reads the code back,
// because a caller that wraps the error (restore's "creating bucket ... for
// restore: %w") would otherwise turn a retryable UNAVAILABLE into INTERNAL, the
// class that tells a driver to report a bug (BUG-0665).
type frameRefusal struct {
	code    transport.ErrorCode
	message string
}

func (e *frameRefusal) Error() string { return e.message }

// applyRestoreDocument inserts one document through the ordinary write path
// for its bucket type (dump-restore-spec.md §9.2), routed through
// routeOrdinaryWrite so it is WAL-durable and replicated exactly like a
// live client's write on a multi-node cluster: OpUpsert for hash/btree (a re-run replaces);
// OpFIFOPush for FIFO (a queue restore is a push, not a keyed insert — the
// specified semantic, dump-restore-spec.md §19 C-2/C-4 — so re-restoring
// the same dump twice means the elements twice, in order); a small
// spill-and-reverse for LIFO (see restoreSession.lifoSpill); OpHeapPush
// with the priority re-attached for heap (see heapDumpDocument); and a
// documents-received count, with no write at all, for blob
// (dump-restore-spec.md §3, §12.9: payloads are never dumped, so there is
// nothing to restore but the fact that a metadata document existed).
func (s *Server) applyRestoreDocument(ctx context.Context, session *restoreSession, raw []byte) error {
	switch session.bucketType {
	case BucketTypeHash, BucketTypeBTree:
		doc, err := decodeRestoreDocument(raw)
		if err != nil {
			return fmt.Errorf("decoding document for %s/%s: %w", session.database, session.bucket, err)
		}
		payload, err := bson.Marshal(InsertRequest{Database: session.database, Bucket: session.bucket, Data: doc})
		if err != nil {
			return fmt.Errorf("encoding document for %s/%s: %w", session.database, session.bucket, err)
		}
		if err := s.routeOrdinaryWrite(ctx, transport.OpUpsert, payload); err != nil {
			return fmt.Errorf("restoring document into %s/%s: %w", session.database, session.bucket, err)
		}
		atomic.AddInt64(&session.applied, 1)
		return nil

	case BucketTypeFIFO:
		doc, err := decodeRestoreDocument(raw)
		if err != nil {
			return fmt.Errorf("decoding document for %s/%s: %w", session.database, session.bucket, err)
		}
		payload, err := bson.Marshal(PushRequest{Database: session.database, Bucket: session.bucket, Data: doc})
		if err != nil {
			return fmt.Errorf("encoding document for %s/%s: %w", session.database, session.bucket, err)
		}
		if err := s.routeOrdinaryWrite(ctx, transport.OpFIFOPush, payload); err != nil {
			return fmt.Errorf("restoring document into %s/%s: %w", session.database, session.bucket, err)
		}
		atomic.AddInt64(&session.applied, 1)
		return nil

	case BucketTypeLIFO:
		// Decoded now, so a document the finish would refuse to decode is
		// refused with its chunk, and kept as its bytes until then.
		if _, err := decodeRestoreDocument(raw); err != nil {
			return fmt.Errorf("decoding document for %s/%s: %w", session.database, session.bucket, err)
		}
		return s.spillLIFODocument(session, raw)

	case BucketTypeHeap:
		return s.applyRestoreHeapDocument(ctx, session, raw)

	case BucketTypeBlob:
		// Payloads are never dumped (dump-restore-spec.md §3); the metadata
		// document itself carries nothing worth writing back, so it is
		// counted and otherwise discarded.
		atomic.AddInt64(&session.skipped, 1)
		return nil

	default:
		return fmt.Errorf("restore does not support bucket type %q", session.bucketType)
	}
}

// restoredElement is a dump's element record as restore reads it: the key is
// kept raw until the field's engine says what it is.
type restoredElement struct {
	Field string        `bson:"field"`
	Owner string        `bson:"owner"`
	Key   bson.RawValue `bson:"key"`
	Value bson.Raw      `bson:"value"`
	Path  []struct {
		Field string        `bson:"field"`
		Key   bson.RawValue `bson:"key"`
	} `bson:"path"`
}

// applyRestoreElement writes one field-store element through the ordinary
// field verbs, routed like a client's write so it is logged and replicated:
// a push in file order for fifo and lifo, whose file order is their position
// order, so their pop order is the source's; a push at the recorded priority
// for heap, whose element keeps its _id_ and so its entry id; a put under the
// element key for hash and btree. The field is found by name, never by id:
// the restored bucket's ids are its own. A blobStore field's element is
// counted and skipped, as a blob bucket's document is.
func (s *Server) applyRestoreElement(ctx context.Context, session *restoreSession, raw []byte) error {
	var record restoredElement
	noteRequestDecode("applyRestoreElement", transport.OpRestoreChunk, raw)
	if err := bson.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("decoding a store-field element record for %s/%s: %w", session.database, session.bucket, err)
	}
	parent := ""
	if len(record.Path) > 0 {
		parent = record.Path[0].Field
	}
	engine, declared := session.storeEngines[storePath(parent, record.Field)]
	if !declared {
		return fmt.Errorf("the dump of %s/%s holds an element of store field %q, which its manifest does not declare", session.database, session.bucket, record.Field)
	}
	if err := refuseDumpedRecord(raw, storeElementRecordDepthLimit(engine)); err != nil {
		return err
	}
	if engine == store.FieldBlob {
		atomic.AddInt64(&session.elementsSkipped, 1)
		return nil
	}
	element, err := decodeRestoreDocument(record.Value)
	if err != nil {
		return fmt.Errorf("decoding an element of %q under %q for %s/%s: %w", record.Field, record.Owner, session.database, session.bucket, err)
	}
	req := FieldRequest{Database: session.database, Bucket: session.bucket, Key: record.Owner, Field: record.Field, Data: element}
	if parent != "" {
		ancestor, err := session.restoredAncestor(record.Owner, parent, record.Path[0].Key)
		if err != nil {
			return err
		}
		req.Path = []interface{}{parent, ancestor}
	}
	op := transport.OpFieldPush
	switch engine {
	case store.FieldHeap:
		var key dumpHeapKey
		if err := record.Key.Unmarshal(&key); err != nil {
			return fmt.Errorf("decoding the priority of an element of %q under %q: %w", record.Field, record.Owner, err)
		}
		req.Priority = key.Priority
	case store.FieldHash, store.FieldBTree:
		elementKey, ok := record.Key.StringValueOK()
		if !ok {
			return fmt.Errorf("an element of %q under %q has a %s key, want a string", record.Field, record.Owner, record.Key.Type)
		}
		op, req.Element = transport.OpFieldPut, elementKey
	}
	payload, err := bson.Marshal(req)
	if err != nil {
		return fmt.Errorf("encoding an element of %q under %q: %w", record.Field, record.Owner, err)
	}
	answer, err := s.routeOrdinaryFrame(ctx, op, payload)
	if err != nil {
		return fmt.Errorf("restoring an element of %q under %q into %s/%s: %w", record.Field, record.Owner, session.database, session.bucket, err)
	}
	if parent == "" && (engine == store.FieldFIFO || engine == store.FieldLIFO) {
		if err := session.notePosition(record, answer); err != nil {
			return err
		}
	}
	atomic.AddInt64(&session.elementsApplied, 1)
	return nil
}

// applyRestoreDead writes one dead entry back as dead, through DEAD_RESTORE,
// the one write a restore sends that no client may (claim-ack-spec §7.3). It is
// routed like every restored write, so it is logged and replicated, and the
// store validates the record on every copy.
func (s *Server) applyRestoreDead(ctx context.Context, session *restoreSession, raw []byte) error {
	var record dumpDeadRecord
	noteRequestDecode("applyRestoreDead", transport.OpRestoreChunk, raw)
	if err := bson.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("decoding a dead entry for %s/%s: %w", session.database, session.bucket, err)
	}
	payload, err := bson.Marshal(DeadRestoreRequest{
		Database: session.database, Bucket: session.bucket,
		Data: record.Data, Deliveries: record.Deliveries, DeadAt: record.DeadAt, Reason: record.Reason,
	})
	if err != nil {
		return fmt.Errorf("encoding a dead entry for %s/%s: %w", session.database, session.bucket, err)
	}
	if err := s.routeOrdinaryWrite(ctx, transport.OpDeadRestore, payload); err != nil {
		return fmt.Errorf("restoring a dead entry into %s/%s: %w", session.database, session.bucket, err)
	}
	atomic.AddInt64(&session.deadApplied, 1)
	return nil
}

// notePosition records where an L2 fifo or lifo element's restored push
// landed, for the L3 elements that name it.
func (session *restoreSession) notePosition(record restoredElement, answer *transport.Frame) error {
	source, ok := record.Key.AsInt64OK()
	if !ok {
		return fmt.Errorf("an element of %q under %q has a %s position, want an integer", record.Field, record.Owner, record.Key.Type)
	}
	var pushed FieldResponse
	if err := bson.Unmarshal(answer.Payload, &pushed); err != nil {
		return fmt.Errorf("decoding the restored push of an element of %q under %q: %w", record.Field, record.Owner, err)
	}
	if pushed.Position == nil {
		return fmt.Errorf("the restored push of an element of %q under %q answered no position", record.Field, record.Owner)
	}
	session.setupMu.Lock()
	defer session.setupMu.Unlock()
	if session.positions == nil {
		session.positions = make(map[restoredPosition]int64)
	}
	session.positions[restoredPosition{owner: record.Owner, field: record.Field, position: source}] = *pushed.Position
	return nil
}

// restoredAncestor names an L3 element's L2 element as the restored bucket
// knows it: a fifo or lifo element by the position its restored push took,
// a heap element by {priority, id} and a keyed one by its key, both kept.
func (session *restoreSession) restoredAncestor(owner, field string, key bson.RawValue) (interface{}, error) {
	switch session.storeEngines[field] {
	case store.FieldFIFO, store.FieldLIFO:
		source, ok := key.AsInt64OK()
		session.setupMu.Lock()
		restored, found := session.positions[restoredPosition{owner: owner, field: field, position: source}]
		session.setupMu.Unlock()
		if !ok || !found {
			return nil, fmt.Errorf("an L3 element names %q element %v under %q, which the restore did not write", field, key, owner)
		}
		return restored, nil
	case store.FieldHeap:
		var heapKey dumpHeapKey
		if err := key.Unmarshal(&heapKey); err != nil {
			return nil, fmt.Errorf("decoding the %q element an L3 element names under %q: %w", field, owner, err)
		}
		return map[string]interface{}{"priority": heapKey.Priority, "id": heapKey.ID}, nil
	default:
		elementKey, ok := key.StringValueOK()
		if !ok {
			return nil, fmt.Errorf("the %q element an L3 element names under %q has a %s key, want a string", field, owner, key.Type)
		}
		return elementKey, nil
	}
}

// decodeRestoreDocument unmarshals one dumped document into the map shape
// InsertCtx expects.
func decodeRestoreDocument(raw []byte) (map[string]interface{}, error) {
	// A dump's bytes reach this decode without passing NewHandler, so they
	// are validated here (server/nesting_depth.go): they are not this node's
	// own stored bytes, so never TrustStored.
	document, err := validateAt(raw, maxDocumentDepth)
	if err != nil {
		return nil, refusalForValidation(err, func() string { return "The dumped document" })
	}
	var doc map[string]interface{}
	if err := document.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decoding a dumped document: %w", err)
	}
	return doc, nil
}

// applyRestoreHeapDocument re-attaches the priority store/heap.go's
// IterateDump stripped out (it lives only in the Badger key, not the
// stored value) and pushes the element back in with it, through the same
// OpHeapPush path a live client's heap push takes (Server.Push, reached via
// routeOrdinaryWrite) — with the dump's real priority instead of the
// hardcoded 0 InsertCtx's own heap branch would use.
//
// This calls Server.Push rather than HeapStore.PushItem directly (fix
// round 1, Important #2): Push is the pre-existing, exported ordinary write
// path for a heap push — it already does the correct bucket locking and,
// crucially, the correct "_id_ missing → mint one" fallback
// (entryIDFor-via-PushItem's own contract, which the first draft of this
// function reimplemented without the fallback). Calling it through
// routeOrdinaryWrite, rather than calling s.Push directly, is what gives a
// restored heap element the same WAL/replication treatment a live client's
// OpHeapPush gets — Push itself has no cluster awareness of its own; that
// lives in Cluster.applyAsOwner, reached only via handleClientFrame.
func (s *Server) applyRestoreHeapDocument(ctx context.Context, session *restoreSession, raw []byte) error {
	var wrapped heapDumpDocument
	noteRequestDecode("applyRestoreHeapDocument", transport.OpRestoreChunk, raw)
	if err := bson.Unmarshal(raw, &wrapped); err != nil {
		return fmt.Errorf("decoding heap document for %s/%s: %w", session.database, session.bucket, err)
	}
	// value stays nil, not merely empty, when wrapped.Value carries no
	// bytes at all — unlike bson.Unmarshal into a map (which always
	// allocates, even for an empty document), there is nothing here to
	// unmarshal in that case, so the map must still be allocated before the
	// _id_ assignment below.
	var value map[string]interface{}
	if len(wrapped.Value) > 0 {
		decoded, err := decodeRestoreDocument(wrapped.Value)
		if err != nil {
			return fmt.Errorf("decoding heap element value for %s/%s: %w", session.database, session.bucket, err)
		}
		value = decoded
	}
	if value == nil {
		value = make(map[string]interface{})
	}
	if wrapped.ID != "" {
		value["_id_"] = wrapped.ID
	}
	payload, err := bson.Marshal(PushRequest{Database: session.database, Bucket: session.bucket, Priority: wrapped.Priority, Data: value})
	if err != nil {
		return fmt.Errorf("encoding heap element for %s/%s: %w", session.database, session.bucket, err)
	}
	if err := s.routeOrdinaryWrite(ctx, transport.OpHeapPush, payload); err != nil {
		return fmt.Errorf("restoring heap element into %s/%s: %w", session.database, session.bucket, err)
	}
	atomic.AddInt64(&session.applied, 1)
	return nil
}

// spillLIFODocument appends one LIFO document to the session's spill file,
// creating the file with the first one.
func (s *Server) spillLIFODocument(session *restoreSession, raw []byte) error {
	session.setupMu.Lock()
	defer session.setupMu.Unlock()
	if session.released {
		return fmt.Errorf("%w: the restore of %s/%s was closed", ErrRestoreNoActiveSession, session.database, session.bucket)
	}
	if session.lifoSpill == nil {
		spill, err := newLIFOSpill(s.config.Storage.DataDir)
		if err != nil {
			return fmt.Errorf("restoring LIFO bucket %s/%s: %w", session.database, session.bucket, err)
		}
		session.lifoSpill = spill
	}
	if err := session.lifoSpill.append(raw); err != nil {
		return fmt.Errorf("restoring LIFO bucket %s/%s: %w", session.database, session.bucket, err)
	}
	session.lifoPeakBytes = max(session.lifoPeakBytes, session.lifoSpill.peakResident)
	return nil
}

// takeLIFOSpill hands the session's spill to the caller, which then owns
// discarding it; nil when the restore received no LIFO document.
func (session *restoreSession) takeLIFOSpill() *lifoSpill {
	session.setupMu.Lock()
	defer session.setupMu.Unlock()
	spill := session.lifoSpill
	session.lifoSpill = nil
	return spill
}

// lifoPeakResidentBytes is the most memory, in bytes, this session has held
// at once for a LIFO bucket's entries.
func (session *restoreSession) lifoPeakResidentBytes() int64 {
	session.setupMu.Lock()
	defer session.setupMu.Unlock()
	return session.lifoPeakBytes
}

// applySpilledLIFO pushes a LIFO bucket's spilled documents in reverse of
// arrival order, through OpLIFOPush (routeOrdinaryWrite) so a spilled
// element is exactly as durable and replicated as one applied immediately.
// The dump file lists them newest-first (pop order,
// store/delivery_dump.go:QueueStore.IterateDump); pushing oldest-first, the
// reverse, is what reproduces that same pop order once the restored bucket is
// popped again. The spill file is removed whether or not every push lands.
func (s *Server) applySpilledLIFO(ctx context.Context, session *restoreSession) (err error) {
	spill := session.takeLIFOSpill()
	if spill == nil {
		return nil
	}
	defer func() {
		session.setupMu.Lock()
		session.lifoPeakBytes = max(session.lifoPeakBytes, spill.peakResident)
		session.setupMu.Unlock()
		if discardErr := spill.discard(); discardErr != nil {
			err = errors.Join(err, discardErr)
		}
	}()
	total := spill.records
	return spill.replay(func(index uint64, record []byte) error {
		pushed := total - index
		doc, err := decodeRestoreDocument(record)
		if err != nil {
			return fmt.Errorf("decoding spilled LIFO element %d/%d for %s/%s: %w", pushed, total, session.database, session.bucket, err)
		}
		payload, err := bson.Marshal(PushRequest{Database: session.database, Bucket: session.bucket, Data: doc})
		if err != nil {
			return fmt.Errorf("encoding spilled LIFO element %d/%d for %s/%s: %w", pushed, total, session.database, session.bucket, err)
		}
		if err := s.routeOrdinaryWrite(ctx, transport.OpLIFOPush, payload); err != nil {
			return fmt.Errorf("restoring spilled LIFO element %d/%d into %s/%s: %w", pushed, total, session.database, session.bucket, err)
		}
		atomic.AddInt64(&session.applied, 1)
		return nil
	})
}

// restoreDone handles OpRestoreDone: apply any spilled LIFO elements,
// assert the dump's own document count against what was actually applied
// plus skipped, release the session, and audit the outcome. A count
// mismatch is reported (Success: false) rather than refused outright — by
// the time OpRestoreDone runs, every document that arrived has already been
// applied through the ordinary write path (dump-restore-spec.md §9.2), so
// there is nothing left to refuse; the mismatch itself is the actionable
// fact, surfaced to the operator rather than hidden behind a bare "ok".
func (s *Server) restoreDone(ctx context.Context, req RestoreDoneRequest) (*RestoreDoneResponse, error) {
	if err := requireRootIdentity(ctx, s); err != nil {
		return nil, err
	}
	session, err := s.activeRestoreFor(req.SessionID, req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	s.restoreSessionMu.Lock()
	delete(s.restoreSessions, session.id)
	s.restoreSessionMu.Unlock()
	defer s.releaseRestoreSession(session, nil)

	if session.bucketType == BucketTypeLIFO {
		if err := s.applySpilledLIFO(ctx, session); err != nil {
			return nil, err
		}
	}

	applied := atomic.LoadInt64(&session.applied)
	skipped := atomic.LoadInt64(&session.skipped)
	elementsApplied := atomic.LoadInt64(&session.elementsApplied)
	elementsSkipped := atomic.LoadInt64(&session.elementsSkipped)
	deadApplied := atomic.LoadInt64(&session.deadApplied)
	username := sessionUsername(ctx)
	target := req.Database + "/" + req.Bucket
	if req.Dead != deadApplied {
		s.audit.Log(AuditEntry{Event: EventRestoreDone, User: username, Target: target,
			Result: fmt.Sprintf("dead count mismatch: dump recorded %d, applied %d", req.Dead, deadApplied)})
		return &RestoreDoneResponse{
			Success: false,
			Message: fmt.Sprintf("bucket %s: the dump's metadata.json recorded %d dead entries, but restore applied %d — the bucket now holds what was actually streamed, but the counts disagree, which may mean the dump directory is corrupt",
				target, req.Dead, deadApplied),
			Applied: applied, Skipped: skipped, ElementsApplied: elementsApplied, ElementsSkipped: elementsSkipped, DeadApplied: deadApplied,
		}, nil
	}
	if req.Elements > 0 && req.Elements != elementsApplied+elementsSkipped {
		s.audit.Log(AuditEntry{Event: EventRestoreDone, User: username, Target: target,
			Result: fmt.Sprintf("element count mismatch: dump recorded %d, applied %d + skipped %d", req.Elements, elementsApplied, elementsSkipped)})
		return &RestoreDoneResponse{
			Success: false,
			Message: fmt.Sprintf("bucket %s: the dump's metadata.json recorded %d store-field element(s), but restore applied %d and skipped %d — the bucket now holds what was actually streamed, but the counts disagree, which may mean the dump directory is corrupt",
				target, req.Elements, elementsApplied, elementsSkipped),
			Applied: applied, Skipped: skipped, ElementsApplied: elementsApplied, ElementsSkipped: elementsSkipped,
		}, nil
	}

	if req.Documents > 0 && req.Documents != applied+skipped {
		s.audit.Log(AuditEntry{Event: EventRestoreDone, User: username, Target: target,
			Result: fmt.Sprintf("document count mismatch: dump recorded %d, applied %d + skipped %d", req.Documents, applied, skipped)})
		return &RestoreDoneResponse{
			Success: false,
			Message: fmt.Sprintf("bucket %s: the dump's metadata.json recorded %d document(s), but restore applied %d and skipped %d (%d total) — the bucket now holds what was actually streamed, but the counts disagree, which may mean the dump directory is corrupt",
				target, req.Documents, applied, skipped, applied+skipped),
			Applied: applied,
			Skipped: skipped,
		}, nil
	}

	s.audit.Log(AuditEntry{Event: EventRestoreDone, User: username, Target: target, Result: "ok"})
	return &RestoreDoneResponse{Success: true, Applied: applied, Skipped: skipped, ElementsApplied: elementsApplied, ElementsSkipped: elementsSkipped, DeadApplied: deadApplied}, nil
}

// handleRestoreStart is the wire entry point for OpRestoreStart.
func handleRestoreStart(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req RestoreStartRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal restore start request: %w", err))
	}
	resp, err := s.startRestore(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpRestoreStart, resp)
}

// handleRestoreChunk is the wire entry point for OpRestoreChunk.
func handleRestoreChunk(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req RestoreChunkRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal restore chunk request: %w", err))
	}
	resp, err := s.restoreChunk(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpRestoreChunk, resp)
}

// handleRestoreDone is the wire entry point for OpRestoreDone.
func handleRestoreDone(ctx context.Context, frame *transport.Frame, s *Server) (*transport.Frame, error) {
	var req RestoreDoneRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return errorResponse(frame, fmt.Errorf("unmarshal restore done request: %w", err))
	}
	resp, err := s.restoreDone(ctx, req)
	if err != nil {
		return errorResponse(frame, err)
	}
	return respond(frame, transport.OpRestoreDone, resp)
}

// restoreCreateRefusal asks the create restore is about to issue the refusals
// that need no state, so restore --drop can stop before it deletes the live
// bucket: a spread bucket's name length, and a unique field on a spread bucket.
// It returns an error saying nothing was dropped, or nil.
func restoreCreateRefusal(req RestoreStartRequest) error {
	create := CreateBucketRequest{Database: req.Database, Name: req.Bucket, Type: req.StoreType,
		Options: restoreCreateOptions(req.Indexes, req.IndexSpecs, req.Stores, req.Placement)}
	placement, refusal := createPlacement(create)
	if refusal == nil && placement == placementSpread {
		if specs, err := parseIndexSpecs(create.Options, nil); err == nil {
			refusal = uniqueOnSpreadRefusal(create, specs)
		}
	}
	if refusal == nil {
		return nil
	}
	return fmt.Errorf("restore --drop of %s/%s stopped before anything was dropped (the bucket and its data are not dropped or changed): %s\n"+
		"If the dump has no placement, add \"placement\": \"whole\" to its %s.metadata.json and restore again",
		req.Database, req.Bucket, refusal.Message, req.Bucket)
}
