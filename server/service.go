// Package server provides the core server implementation for hoardDB.
//
// This file contains the Server business logic methods and in-memory
// data structures backing the TCP transport. The legacy gRPC service
// layer it once held was removed when QUIC replaced gRPC entirely; QUIC
// was later removed too in favor of TCP+TLS (see
// the quic-removal-decision record).
package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ids"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// inMemoryBucket is a handle on one bucket for one operation: the bucket's
// record, the facts copied from it, and the engine built for its type. The
// registry holds no handle (catalog-gc spec §5.2): a bucket is its record,
// and a handle is built when an operation needs an engine, as a read view
// always was. A writer's handle comes from lockBucket, under the record's
// mutex; a reader's from resolveForRead, from copies taken under db.mu.
type inMemoryBucket struct {
	// rec is the bucket's record, whose mutex is the bucket's only one, and
	// epoch the claim of the record the handle was built for. Every handle
	// built while the record's mutex is held checks the epoch first: the
	// record's slot is reused once its bucket is dropped (spec §5.3). A read
	// view's rec is nil: a reader holds copies and never touches the record.
	rec   *bucketRec
	epoch uint32

	bucketType BucketType
	// id is the bucket's catalog id and slot its recorded storage slot, as
	// the record held them when the handle was built. Every key builder
	// takes the slot from here, never from storage.SlotOf. id is 0 only in a
	// create's or materialise's handle before its attach.
	id   atomic.Uint64
	slot uint16
	// databaseID is the database id every key of the bucket carries: for now
	// storage.LegacyDatabaseID of the database name, set where the handle is
	// built and read by buildEngines.
	databaseID uint64

	// kv is the node's shared store, which every engine lives on, or a read
	// view's snapshot of it. A handle never closes it.
	kv storage.KV

	// Storage engines — exactly one is non-nil, the one bucketType names. Each
	// is a view of the bucket's key range in kv and holds no file of its own.
	hashStore  *store.HashStore
	btreeStore *store.BTreeStore
	queueStore *store.QueueStore
	heapStore  *store.HeapStore
	blobStore  *store.BlobStore

	// idxMgr maintains secondary indexes for this bucket's engine (nil if
	// none declared). buildEngines sets it from the declaration, and nothing
	// else does: a manager kept apart from the declaration is one that can go
	// stale (catalog-gc spec N-8).
	idxMgr *store.IndexManager

	// decl is the bucket's declaration — its index list and store fields —
	// nil when it declares neither (decl_table.go): the declaration table's
	// entry for an attached bucket, the one a create is preparing before its
	// attach. The lists inside it are published whole, so a reader that loads
	// it reads them with no lock.
	decl atomic.Pointer[bucketDecl]

	// fieldTypes is the bucket's typed-field declaration, nil when it declares
	// none: the holder the interned set shares among every bucket that
	// declares the same fields (fieldTypeSet).
	fieldTypes *bucketFieldTypes

	// owedCarry is the failed-apply carry this handle's engine is built with
	// when the bucket owes nothing. Once it owes, the carry table holds a
	// pointer to it (owedCarries), which is what the next handle's engine is
	// handed.
	owedCarry store.OwedApplies
}

// inMemoryDB holds all buckets for a single database.
type inMemoryDB struct {
	// table is the only door to the registry.
	table bucketTable
	// dropped is set, with buckets emptied, when the database is dropped. A
	// caller that fetched this struct before the drop and locks it after finds
	// every bucket gone (spec §5.2.2, N-10). Guarded by mu.
	dropped bool
	mu      sync.RWMutex
}

// newInMemoryDB returns an empty database registry.
func newInMemoryDB() *inMemoryDB {
	db := &inMemoryDB{}
	db.table = newBucketTable(&db.mu)
	return db
}

// serverData is the in-memory data store attached to the server.
type serverData struct {
	dbs map[string]*inMemoryDB
	mu  sync.RWMutex
}

// getOrCreateDB returns the in-memory DB, creating it if needed.
func (d *serverData) getOrCreateDB(name string) *inMemoryDB {
	d.mu.Lock()
	defer d.mu.Unlock()
	if db, ok := d.dbs[name]; ok {
		return db
	}
	db := newInMemoryDB()
	d.dbs[name] = db
	return db
}

// getDB returns the in-memory DB or nil.
func (d *serverData) getDB(name string) *inMemoryDB {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dbs[name]
}

// claimBucket returns the record of name in database, creating the
// database's struct and the bucket's record as needed, claimed for generation
// (see inMemoryDB.claimBucket). A database dropped between the two lookups is
// looked up again, which finds or creates its successor: the dropped struct
// is removed from dbs before it is marked, so the second lookup never returns
// it.
func (d *serverData) claimBucket(database, name string, btype BucketType, generation int64, creating bool) (*inMemoryDB, heldRecord, error) {
	for {
		db := d.getOrCreateDB(database)
		held, live, err := db.claimBucket(name, btype, generation, creating)
		if err != nil || live {
			return db, held, err
		}
	}
}

// claimPulledBucket is claimBucket for a migration pull, which reports false
// and claims nothing once dropped reports that a drop cancelled the pull.
// claimBucket's "finds or creates its successor" is right for a create but
// would bring a database dropped mid-pull back in memory. dropped is checked
// under data.mu, and a database drop cancels the database's pulls before it
// takes data.mu to remove the database, so a lookup that no longer finds the
// database also sees the cancel.
func (d *serverData) claimPulledBucket(database, name string, btype BucketType, generation int64, dropped func() bool) (heldRecord, bool, error) {
	for {
		d.mu.Lock()
		if dropped() {
			d.mu.Unlock()
			return heldRecord{}, false, nil
		}
		db, ok := d.dbs[database]
		if !ok {
			db = newInMemoryDB()
			d.dbs[database] = db
		}
		d.mu.Unlock()
		held, live, err := db.claimBucket(name, btype, generation, false)
		if err != nil {
			return heldRecord{}, false, err
		}
		if live {
			return held, true, nil
		}
	}
}

// claimBucket returns the bucket's record, claiming a bare one if needed, and
// reports false when the database has been dropped. Engines are built per
// operation, never here.
//
// A record no create has committed (id 0) is claimed for generation here,
// under db.mu, the lock detachDroppedGenerations reads generations under. A
// drop then removes it only if it names the generation the caller is making
// the bucket in, so a drop of another generation can never take the record
// out between this lookup and the caller's commit. The caller holds the
// bucket's fence, which excludes a create but not a second pull of the same
// name: pulls hold it shared, so two pulls in different generations can both
// claim the record.
func (db *inMemoryDB) claimBucket(name string, btype BucketType, generation int64, creating bool) (heldRecord, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.dropped {
		return heldRecord{}, false, nil
	}
	held, err := db.table.claim(name, btype, generation, creating)
	return held, err == nil, err
}

// reapUncommittedCreate takes out of the registry the record a create claimed
// when the create returned without committing it, so the name is neither
// counted by ListDatabases nor left for a later drop to find. It runs inside
// the create's fence, which keeps another create or a pull from claiming the
// same name meanwhile; a database drop or a bucket drop that got there first
// makes unlink answer false, and the record is theirs. The caller holds no
// lock.
func (s *Server) reapUncommittedCreate(db *inMemoryDB, name string, held heldRecord) {
	if held.rec.id.Load() != 0 {
		return
	}
	db.mu.Lock()
	unlinked := false
	var unlinkErr error
	if held.rec.id.Load() == 0 {
		unlinked, unlinkErr = db.table.unlink(name, held)
	}
	db.mu.Unlock()
	if unlinkErr != nil {
		// This runs from the create's defer, with no error to return: the
		// record stays linked with an id of 0, so say so rather than leave the
		// name counted by ListDatabases with nothing behind it.
		s.logger.Error("an uncommitted create's record could not be unlinked from the registry; the name keeps a record with no id until a drop or a restart",
			"bucket", name, "error", unlinkErr)
	}
	if unlinked {
		s.retireRecords(db, []*bucketRec{held.rec})
	}
}

// findBucket returns the record named name, and false when there is none —
// also for a database dropped since the caller fetched it, whose table is
// empty.
func (db *inMemoryDB) findBucket(name string) (heldRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.table.get(name)
}

// serverDataAccessor provides thread-safe access to in-memory data via the
// Server.
//
// The store is created by NewService, which is the only way a Server is
// built. It used to be created lazily on first use here, which made two
// concurrent dispatch calls — an ordinary client request and a replicated one,
// or two replicated ones — write the same field at once. That is not a
// theoretical race: replication applies entries on the same server that is
// serving clients, so two creates on one node are concurrent by design.
func (s *Server) serverDataAccessor() *serverData {
	return s.dataStore
}

// CreateDatabase creates a new database with a data directory and records it in
// the catalog, so it is still there after a restart. It carries no apply
// sequence, so the new generation is stamped with the log's tail
// (generationSeq).
//
// A name already live answers Success:false with the message, as it always has
// for a caller in this process; over the wire the same refusal is an
// ALREADY_EXISTS error frame (createDatabaseCtx).
func (s *Server) CreateDatabase(name string) (*CreateDatabaseResponse, error) {
	resp, err := acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*CreateDatabaseResponse, error) {
		return s.createDatabaseCtx(ctx, CreateDatabaseRequest{Name: name})
	})
	if errors.Is(err, ErrDatabaseAlreadyExists) {
		return &CreateDatabaseResponse{Success: false, Message: err.Error()}, nil
	}
	return resp, err
}

// createDatabaseCtx creates an incarnation of a database (G-1, G-2): first the
// dropped incarnations the create carries are applied, then the incarnation is
// added to the database's live ones — unless this node knows it to be dropped,
// when the create is a late one and nothing is created. ctx carries the apply
// sequence, which a new marker records as the log position it was created at.
//
// A client's request to a node that coordinates for itself is a create of a
// database that is not live here, in an incarnation minted here, whatever the
// request said (G-9). A create logged before generations existed is the
// legacy incarnation, and a no-op on a database already live.
func (s *Server) createDatabaseCtx(ctx context.Context, req CreateDatabaseRequest) (*CreateDatabaseResponse, error) {
	name := req.Name
	if refusal := s.databaseNameRefusal(name); refusal != nil {
		return refusal, nil
	}
	exists := &CreateDatabaseResponse{Success: false, Message: databaseExistsMessage(name)}
	generation, carried := req.Generation, req.DroppedGenerations
	switch {
	case !generationsTrusted(ctx):
		live, err := s.liveGenerations(name)
		if err != nil {
			return &CreateDatabaseResponse{Success: false, Message: failureMessage(fmt.Sprintf("database %q was not created: reading its incarnations failed", name), err)}, nil
		}
		if len(live) > 0 {
			return nil, &sentencedError{message: exists.Message, cause: ErrDatabaseAlreadyExists}
		}
		dropped, err := s.droppedGenerations(name)
		if err != nil {
			return &CreateDatabaseResponse{Success: false, Message: failureMessage(fmt.Sprintf("database %q was not created: reading its dropped incarnations failed", name), err)}, nil
		}
		if generation, err = mintGeneration(dropped); err != nil {
			return &CreateDatabaseResponse{Success: false, Message: failureMessage(fmt.Sprintf("database %q was not created", name), err)}, nil
		}
		carried = nil
	case generation == 0:
		if s.serverDataAccessor().getDB(name) != nil {
			return exists, nil
		}
		generation = legacyGeneration(name)
	}
	if seamsOff().noCarried {
		carried = nil
	}
	if err := s.learnTombstones(ctx, name, carried); err != nil {
		// A store failure, not a refusal: a replica's push branch reads it
		// and leaves the entry for the owner to re-send (BUG-0573).
		if outcome := applyOutcomeFrom(ctx); outcome != nil {
			outcome.storeFailure = err
		}
		return &CreateDatabaseResponse{Success: false, Message: failureMessage(fmt.Sprintf("database %q was not created: applying the drops it carries failed", name), err)}, nil
	}

	unlock := s.lockDatabase(name)
	defer unlock()
	late, err := s.commitGenerationAdd(name, generation, applySeqFrom(ctx))
	if err == nil && !late {
		noteCommitted(ctx)
	}
	if err != nil {
		s.logger.Error("failed to record database in the catalog", "database", name, "error", err)
		if outcome := applyOutcomeFrom(ctx); outcome != nil {
			outcome.storeFailure = err
		}
		return &CreateDatabaseResponse{
			Success: false,
			Message: failureMessage(fmt.Sprintf("database %q was not created: recording it in the catalog failed", name), err),
		}, nil
	}
	if late {
		return &CreateDatabaseResponse{Success: true, Message: fmt.Sprintf(
			"database %q: this create's incarnation was dropped before the create reached this node, so nothing was created", name)}, nil
	}
	data := s.serverDataAccessor()
	data.mu.Lock()
	defer data.mu.Unlock()
	// A database is keys in the node's store and nothing else on disk: a
	// directory named after it listed every database in plaintext,
	// and the drop's removal of it could reach a directory of the node's own.
	if _, held := data.dbs[name]; !held {
		data.dbs[name] = newInMemoryDB()
	}
	return &CreateDatabaseResponse{Success: true, Message: fmt.Sprintf("database %q created", name)}, nil
}

// databaseNameRefusal refuses a name no database may have, or returns nil.
func (s *Server) databaseNameRefusal(name string) *CreateDatabaseResponse {
	violation := namepolicy.CheckDatabase(name, s.storageDirs())
	if violation == nil {
		return nil
	}
	return &CreateDatabaseResponse{Success: false, Message: databaseViolationMessage(violation)}
}

// databaseViolationMessage is the sentence this door gives for a database name the shared
// policy refused.
func databaseViolationMessage(violation *namepolicy.Violation) string {
	switch violation.Rule {
	case namepolicy.RuleEmpty:
		return "database name required"
	case namepolicy.RuleReserved:
		return fmt.Sprintf("database name %q is reserved for internal use (the auth master key lives there); choose another name", violation.Name)
	case namepolicy.RuleStorageDir:
		return violation.Detail
	case namepolicy.RuleTooLong:
		return legacyNameError(violation).Error()
	}
	return fmt.Sprintf("invalid database name: %v", violation.Err)
}

// CreateBucket creates a new bucket within a database: it records the bucket in
// the catalog under a new id, and opens nothing. It carries no
// apply sequence, so the bucket's create floor is 0.
func (s *Server) CreateBucket(req CreateBucketRequest) (*CreateBucketResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*CreateBucketResponse, error) {
		return s.createBucketLocked(ctx, req)
	})
}

// createBucketLocked validates the request and commits its create batch — the catalog entry with a freshly allocated id and
// the slot recorded once, here, and the state document whose applied_seq is
// the create floor: the apply sequence in ctx (spec §5.2.2). A name already in
// the catalog is not created again: it is attached from its entry.
func (s *Server) createBucketLocked(ctx context.Context, req CreateBucketRequest) (*CreateBucketResponse, error) {
	if req.Database == "" {
		return &CreateBucketResponse{Success: false, Message: "database name required"}, nil
	}
	if req.Name == "" {
		return &CreateBucketResponse{Success: false, Message: "bucket name required"}, nil
	}
	if req.Type == "" {
		req.Type = BucketTypeHash
	}
	switch req.Type {
	case BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBlob:
		// valid
	default:
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf("unsupported bucket type: %s", req.Type)}, nil
	}
	if err := validateBucketName(req.Database, req.Name); err != nil {
		return &CreateBucketResponse{Success: false, Message: invalidBucketNameMessage(err)}, nil
	}
	if refusal := s.storageNameRefusal(req.Database); refusal != "" {
		return &CreateBucketResponse{Success: false, Message: refusal}, nil
	}
	// Malformed, not policy: refused here at every depth. What the cluster can
	// grant is decided on the client-facing path only (checkBucketRF), because
	// a load and a replay reach this function too.
	if req.ReplicationFactor < 0 {
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket %q: replication_factor %d is not a number of copies. Use a whole number, 1 or more, or leave it out to inherit the node's factor: create bucket %s { replication_factor: 3 }",
			req.Name, req.ReplicationFactor, req.Name)}, nil
	}

	// A name with the part separator is a part's, made by the router with
	// part_of set. The client doors refuse it earlier (createNameRefusal); this
	// is the backstop for a replica or a replay.
	if hasPartSeparator(req.Name) && req.PartOf == 0 {
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket name %s in database %s holds a control character that only the parts of a spread collection carry, so it was refused. "+
				"Use printable characters only: create bucket <name> in %s { type: hash }",
			echoName(displayBucket(req.Name)), echoName(req.Database), echoName(req.Database))}, nil
	}
	placement, refusal := createPlacement(req)
	if refusal != nil {
		return refusal, nil
	}

	generation, err := s.bucketCreateGeneration(ctx, req)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: choosing its database's incarnation failed", req.Name), err)}, nil
	}
	if refusal := s.decideGenerationOfCreate(ctx, req, generation); refusal != nil {
		return refusal, nil
	}

	// The reply says whether this call brought the database into being as well:
	// claimBucket creates it on demand, and a typo in the database name would
	// otherwise look like a plain create (BUG-0264).
	databaseExisted := s.serverDataAccessor().getDB(req.Database) != nil
	db, held, err := s.serverDataAccessor().claimBucket(req.Database, req.Name, req.Type, generation, true)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created", req.Name), err)}, nil
	}
	// Registered before the record's lock so it runs after the unlock below:
	// the registry lock cannot be taken under a record's mutex.
	defer s.reapUncommittedCreate(db, req.Name, held)
	held.rec.mu.Lock()
	defer held.rec.mu.Unlock()
	// Only a database drop of the generation this create claimed the record
	// for can take it between the claim and this lock (the create's fence
	// excludes a drop of the bucket). Its slot may already name another
	// bucket, so nothing of it is touched: the answer is the one the commit
	// would have given, refused by the drop.
	if !held.current() {
		return &CreateBucketResponse{Success: false, Message: droppedWhileCreatingMessage(req.Database, req.Name)}, nil
	}
	bucket := s.handleLocked(req.Database, held)
	// A create that returns without committing leaves a bare record. Clearing
	// the mark makes it a ghost a later drop may remove; atomic so the clear
	// needs no db.mu under the record's mutex.
	defer func() {
		if held.rec.id.Load() == 0 {
			held.rec.setCreating(false)
		}
	}()

	if bucket.id.Load() != 0 {
		if bucket.bucketType != req.Type {
			return nil, bucketTypeMismatchErr(req, bucket.bucketType)
		}
		if refusal := recreateDeclarationRefusal(bucket, req); refusal != nil {
			return refusal, nil
		}
		message := fmt.Sprintf("bucket %q already exists (type: %s)", req.Name, bucket.bucketType)
		declared := bucket.rec.replicationFactor.Load()
		effective := int(declared)
		if declared == 0 {
			effective = max(1, s.config.Server.ReplicationFactor)
		}
		if req.ReplicationFactor != 0 && req.ReplicationFactor != effective {
			// Still a success -- a replay and a redelivery take this path -- but
			// the factor the caller named is not the bucket's, nothing was
			// created or shipped anywhere, and the reply must not read as if it
			// had been applied.
			message += fmt.Sprintf(" with %s; this create asked for %d and it was not changed. "+
				"To use another factor, run: alter bucket %s { replication_factor: %d }",
				existingFactorPhrase(declared, effective), req.ReplicationFactor, req.Name, req.ReplicationFactor)
		}
		recorded := placementLabel(bucket.bucketType, bucket.rec.isSpread())
		message += existingPlacementNote(req, recorded)
		return &CreateBucketResponse{Success: true, Message: message, Placement: recorded}, nil
	}
	converted, attach, resp := s.catalogedCreate(req)
	if resp != nil {
		return resp, nil
	}
	if attach != nil {
		return s.attachBucketLocked(bucket, *attach)
	}

	// Only the hash engine maintains a secondary index (buildEngines). A new
	// bucket of another type declared with one would be created with an index
	// nothing writes, and search would answer "no results" for documents that
	// match. A catalogued bucket never reaches here — replay and
	// migration re-attach what the catalog holds — so this refuses only a
	// genuinely new declaration.
	if req.Type != BucketTypeHash {
		if specs, err := parseIndexSpecs(req.Options, nil); err == nil && len(specs) > 0 {
			return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
				"bucket %q: secondary indexes are maintained only on hash buckets today, so a %s bucket's index would never be written "+
					"and search would find nothing. Create it without indices, or as a hash bucket: create bucket %s { type: hash, indices: [\"%s\"] }",
				req.Name, req.Type, req.Name, specs[0].Fields[0].Field)}, nil
		}
	}

	// A unique field other than _id_ cannot be checked across the parts of a
	// spread bucket. A parse error is left to prepareBucketLocked, which
	// answers it.
	if placement == placementSpread {
		if specs, err := parseIndexSpecs(req.Options, nil); err == nil {
			if refusal := uniqueOnSpreadRefusal(req, specs); refusal != nil {
				return refusal, nil
			}
		}
	}

	// Encryption is decided for the node, when its store is created: a bucket
	// that asks for it on a plaintext store would be written unsealed.
	if req.Encrypted && s.config.Encryption.Mode != "on" && s.config.Encryption.Mode != "require" {
		return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
			"bucket %q asks for encryption, but this node's store is not encrypted (encryption.mode is %q). "+
				"Encryption is decided for the whole node when its store is created: start a fresh data directory "+
				"with encryption.mode set to on and an encryption.keystore configured, then create the bucket there",
			req.Name, s.config.Encryption.Mode)}, nil
	}

	state, err := s.createLineage(ctx, req)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage("bucket not created: its incarnation could not be drawn", err)}, nil
	}
	resp, err = s.prepareBucketLocked(bucket, req)
	if err != nil || !resp.Success {
		return resp, err
	}
	if !databaseExisted {
		// The reply says the database appeared too: a typo in its name would
		// otherwise read as a plain create (BUG-0264).
		resp.Message = fmt.Sprintf("created database %q and bucket %q (type: %s)", req.Database, req.Name, req.Type)
	}
	slot := slotForCreate(req.Database, req.Name)
	state.AppliedSeq = applySeqFrom(ctx)
	fields, fieldTypes := bucket.declaredFieldTypes().catalogForm()
	id, err := s.commitBucketCreate(catalogEntry{
		database:  req.Database,
		bucket:    req.Name,
		Slot:      int32(slot),
		Type:      req.Type,
		Indices:   catalogIndices(bucket),
		CreatedAt: time.Now().UTC(),

		Stores:      catalogStores(bucket),
		NextStoreID: bucket.storeFields().next,

		ReplicationFactor:  int32(req.ReplicationFactor),
		DatabaseGeneration: generation,

		Fields:     fields,
		FieldTypes: fieldTypes,

		Placement: placement,
		Parts:     spreadPartsFor(placement),
		Part:      req.Part,
		PartOf:    req.PartOf,
	}, state, converted)
	if err != nil {
		s.publishIndexSpecs(bucket, nil)
		s.setFieldTypes(bucket, nil)
		var exhausted *bucketIDsExhaustedError
		if errors.As(err, &exhausted) {
			return &CreateBucketResponse{Success: false, Message: exhausted.Error()}, nil
		}
		if errors.Is(err, errGenerationDropped) {
			return &CreateBucketResponse{Success: false, Message: droppedWhileCreatingMessage(req.Database, req.Name)}, nil
		}
		return &CreateBucketResponse{Success: false, Message: failureMessage("failed to record the bucket in the catalog", err)}, nil
	}
	noteCommitted(ctx)
	if converted != nil {
		s.wakeReclaimer()
	}
	bucket.rec.replicationFactor.Store(int32(req.ReplicationFactor))
	bucket.rec.setFlag(recSpread, placement == placementSpread)
	s.noteAttachedToSweep(req.Database, req.Name, id)
	if err := s.attachSharedEngineLocked(bucket, slot, id); err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage("bucket recorded but not attached", err)}, nil
	}
	bucket.rec.setLineageStateLocked(state)
	s.registerBucketFactor(req.Database, req.Name, bucket.rec.replicationFactor.Load())
	resp.Placement = placement
	return resp, nil
}

// bucketCreateGeneration is the incarnation of its database a create makes
// the bucket in: the one the request carries when it was stamped before it was
// logged, the legacy one for a create logged before generations existed, and
// for a client's request to a node that coordinates for itself the one most
// recently added to the database here, or a new one (G-5, G-9).
func (s *Server) bucketCreateGeneration(ctx context.Context, req CreateBucketRequest) (int64, error) {
	if generationsTrusted(ctx) {
		if req.DatabaseGeneration == 0 {
			return legacyGeneration(req.Database), nil
		}
		return req.DatabaseGeneration, nil
	}
	live, err := s.liveGenerations(req.Database)
	if err != nil {
		return 0, err
	}
	if len(live) > 0 {
		return live[len(live)-1], nil
	}
	dropped, err := s.droppedGenerations(req.Database)
	if err != nil {
		return 0, err
	}
	return mintGeneration(dropped)
}

// existingFactorPhrase names a bucket's declared factor for a message: the
// number, or that it declared none and follows the node's.
func existingFactorPhrase(declared int32, effective int) string {
	if declared == 0 {
		return fmt.Sprintf("no replication_factor of its own (it follows the node's, %d)", effective)
	}
	return fmt.Sprintf("replication_factor %d", declared)
}

// catalogedCreate decides what a create does with the name's catalog key.
// Empty: create. An entry of a dropped database the expander has not reached
// (id at or below the drop's max_id): create over it, converting the old entry
// into its reclaim record — slot, id and type taken from the old entry, never
// from this create — in the create's own batch (spec §5.2.2, Q21); the create
// is never refused for a pending expansion. Any other entry is catalogued but
// not attached — its attach failed at load (a missing encryption key, say) —
// and is attached rather than given a new id, which would orphan the
// catalogued one and everything recorded under it. A non-nil response is a
// failure to report.
func (s *Server) catalogedCreate(req CreateBucketRequest) (converted *reclaimRecord, attach *catalogEntry, failure *CreateBucketResponse) {
	entry, catalogued, err := s.readCatalogEntry(req.Database, req.Name)
	if err != nil {
		return nil, nil, &CreateBucketResponse{Success: false, Message: failureMessage("failed to read the catalog", err)}
	}
	if !catalogued {
		return nil, nil, nil
	}
	drop, pending, err := s.pendingDatabaseDrop(req.Database)
	if err != nil {
		return nil, nil, &CreateBucketResponse{Success: false, Message: failureMessage("failed to read the catalog", err)}
	}
	if !pending || !drop.covers(entry) {
		return nil, &entry, nil
	}
	return &reclaimRecord{
		ID: entry.ID, Slot: entry.Slot, Type: entry.Type, Database: req.Database, Name: req.Name,
		DroppedSeq: drop.DroppedSeq, DroppedAt: drop.DroppedAt,
	}, nil, nil
}

// attachSharedEngineLocked builds the engine of a bucket on the node's shared
// store — it opens nothing — and sets the bucket's id and slot, which is the
// moment it is served.
func (s *Server) attachSharedEngineLocked(bucket *inMemoryBucket, slot uint16, id uint64) error {
	if err := s.buildEngines(bucket, s.kv, slot, id); err != nil {
		return err
	}
	s.publishAttachLocked(bucket, slot, id)
	return nil
}

// publishAttachLocked is the infallible half of the attach: it publishes the
// slot, type, declarations and id of a bucket whose engines buildEngines has
// already built, into its record and its handle. A materialise builds first
// and publishes after its catalog commit, so that no step that can fail
// follows the commit's attach. The caller holds the record's mutex and has
// checked, under it, that the record is current: a drop closes a record only
// under that mutex, so the bucket is open and its attach is counted.
func (s *Server) publishAttachLocked(bucket *inMemoryBucket, slot uint16, id uint64) {
	rec := bucket.rec
	wasAttached := rec.id.Load() != 0
	// The slot, the type and the declarations are published before id is
	// stored: a reader that loads a non-zero id reads them with no lock (I-1),
	// so the store is what publishes them.
	bucket.slot = slot
	rec.slot = slot
	rec.bucketType = bucketTypeCode(bucket.bucketType)
	if !wasAttached {
		s.enterDeclarationsLocked(bucket, id)
	}
	bucket.id.Store(id)
	rec.id.Store(id)
	if !wasAttached && !rec.isPart() {
		s.noteBucketAttached()
	}
}

// noteAttachedToSweep tells a running sweep round that a bucket with catalog id
// id now stands under the name, before its id is published and a pass can see
// it (BUG-0662).
func (s *Server) noteAttachedToSweep(database, name string, id uint64) {
	if s.cluster != nil {
		s.cluster.noteAttachedInRound(heldBucket{database: database, bucket: name}, id)
	}
}

// noteBucketAttached and noteBucketDetached are the only writers of
// Server.bucketsAttached. The attach side is counted here, on the id's 0 → id
// move under bucket.mu, and never in buildEngines, which also builds read
// views that attach nothing.
func (s *Server) noteBucketAttached() { s.bucketsAttached.Add(1) }

// noteBucketDetached takes the count retireRecords reports for a registry
// removal.
func (s *Server) noteBucketDetached(count int64) { s.bucketsAttached.Add(-count) }

// buildEngines sets the bucket's engine for its type over kv: the node's store
// at attach, or one reader's snapshot for a read view (resolveForRead).
//
// The engine's failed-apply carry is the one the carry table holds for id when
// the bucket owes an apply, so an engine built after a failure records it; the
// struct's own otherwise. A read view may be handed the table's carry too: it
// never applies, because its store refuses every batch.
func (s *Server) buildEngines(bucket *inMemoryBucket, kv storage.KV, slot uint16, id uint64) error {
	owed := s.owed.carries.take(id)
	if owed == nil {
		owed = &bucket.owedCarry
	}
	// An index recorded on a path is published, so the catalog keeps it, but
	// never maintained, planned or enforced (spec §4.9.7). The manager is set
	// whether or not one is built, so an engine rebuilt over a declaration
	// with no usable index keeps no manager from before.
	bucket.idxMgr = nil
	if specs := usableIndexSpecs(bucket.declaredIndexes()); len(specs) > 0 {
		// Only the hash store maintains secondary indexes today; btree buckets
		// are already ordered by key and use get/range/first/last.
		im, err := store.NewIndexManager(kv, slot, bucket.databaseID, id, specs)
		if err != nil {
			return fmt.Errorf("attaching the indexes of bucket %d: %w", id, err)
		}
		bucket.idxMgr = im
	}
	bucket.kv = kv
	switch bucket.bucketType {
	case BucketTypeHash:
		bucket.hashStore = store.NewHashStore(kv, slot, bucket.databaseID, id, owed)
		if bucket.idxMgr != nil {
			bucket.hashStore.SetIndexManager(bucket.idxMgr)
		}
	case BucketTypeBTree:
		bucket.btreeStore = store.NewBTreeStore(kv, slot, bucket.databaseID, id, owed)
	case BucketTypeHeap:
		bucket.heapStore = store.NewHeapStore(kv, slot, bucket.databaseID, id, owed)
	case BucketTypeFIFO:
		bucket.queueStore = store.NewQueueStore(kv, storage.BucketRef{Slot: slot, DatabaseID: bucket.databaseID, BucketID: id}, owed, store.QueueFIFO)
	case BucketTypeLIFO:
		bucket.queueStore = store.NewQueueStore(kv, storage.BucketRef{Slot: slot, DatabaseID: bucket.databaseID, BucketID: id}, owed, store.QueueLIFO)
	case BucketTypeBlob:
		bucket.blobStore = store.NewBlobStore(kv, storage.BucketRef{Slot: slot, DatabaseID: bucket.databaseID, BucketID: id}, owed, store.BlobPayloads{Root: s.blobRoot(), Keys: s.blobKeys})
	}
	return nil
}

// attachBucketLocked serves a catalogued bucket: the attach assertion, then
// the refusals and index declaration a create checks, then its engine. It is
// the reopen path — Server.LoadCatalog calls it for every catalogued bucket —
// and, like the create, it opens nothing.
func (s *Server) attachBucketLocked(bucket *inMemoryBucket, entry catalogEntry) (*CreateBucketResponse, error) {
	if err := assertRecordedSlot(entry); err != nil {
		return nil, err
	}
	resp, err := s.prepareBucketLocked(bucket, CreateBucketRequest{
		Database: entry.database,
		Name:     entry.bucket,
		Type:     entry.Type,
		Options:  indexOptions(entry.Indices),
	})
	if err != nil || !resp.Success {
		return resp, err
	}
	if refusal := s.adoptRecordedStores(bucket, entry); refusal != nil {
		return refusal, nil
	}
	if refusal := s.adoptRecordedFieldTypes(bucket, entry); refusal != nil {
		return refusal, nil
	}
	bucket.rec.replicationFactor.Store(entry.ReplicationFactor)
	bucket.rec.setFlag(recSpread, entry.Placement == placementSpread)
	bucket.rec.generation.Store(entryGeneration(entry))
	if err := s.attachSharedEngineLocked(bucket, uint16(entry.Slot), uint64(entry.ID)); err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage("failed to attach the bucket", err)}, nil
	}
	// LoadCatalog reaches here for every bucket before Cluster.Start, which is
	// what makes the registry complete when start-time relocation asks who
	// holds what.
	s.registerBucketFactor(entry.database, entry.bucket, entry.ReplicationFactor)
	return resp, nil
}

// prepareBucketLocked decides the refusals that depend on the bucket's type
// and records its type and index declaration. It opens nothing: every engine
// lives on the node's store (attachSharedEngineLocked).
func (s *Server) prepareBucketLocked(bucket *inMemoryBucket, req CreateBucketRequest) (*CreateBucketResponse, error) {
	bucket.bucketType = req.Type
	if refusal := s.prepareStoreFields(bucket, req); refusal != nil {
		return refusal, nil
	}

	// Parse index declarations from Options["indices"] if present. The CLI
	// teaches the simple form — indices: ["Email"] — and the object form
	// ({"field": "Email", "type": "range"}) is accepted too, because a bad
	// declaration that is silently dropped leaves a bucket that cannot be
	// searched while claiming the index exists.
	specs, err := parseIndexSpecs(req.Options, s.rangeTypeWarner(req.Database, req.Name))
	var onAPath *pathIndexError
	if errors.As(err, &onAPath) {
		onAPath.bucket = req.Name
	}
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: err.Error()}, nil
	}
	// Before the indexes are published, so a refused declaration leaves none
	// behind: rule 6 reads the unique ones from the request.
	if refusal := s.prepareFieldTypes(bucket, req, specs); refusal != nil {
		return refusal, nil
	}
	// The indexes live in the node's store beside the documents, so the
	// manager is built at attach, once the bucket has its id; the declaration
	// is recorded here, in the catalog entry's order, which is each field's
	// id.
	s.publishIndexSpecs(bucket, specs)
	// Only the hash engine maintains index entries, so only a hash bucket has
	// anything for the sweeper to find. The create path refuses another type's
	// index; attach and migration reach here without that refusal.
	if req.Type == BucketTypeHash {
		s.ttl.note(req.Database, req.Name, usableIndexSpecs(specs))
	}
	return &CreateBucketResponse{Success: true, Message: fmt.Sprintf("bucket %q created (type: %s)", req.Name, req.Type)}, nil
}

// Insert inserts a document into a hash, btree, fifo, lifo, heap, or blob bucket.
func (s *Server) Insert(req InsertRequest) (*InsertResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*InsertResponse, error) {
		return s.InsertCtx(ctx, req)
	})
}

// InsertCtx is Insert with a context, so the replicated write path can carry the
// entry's WAL sequence down to the store and stamp the read-repair version. When
// no version is present (single-node, replay of an unsequenced entry) the store
// is written without one, which the version side-key tolerates.
func (s *Server) InsertCtx(ctx context.Context, req InsertRequest) (*InsertResponse, error) {
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}

	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	switch req.verb {
	case verbUpsert:
		if err := refuseUpsertOn(req.Bucket, bucket.bucketType); err != nil {
			return nil, err
		}
	case verbInsert:
		if err := refuseInsertOnBlob(req.Bucket, bucket.bucketType); err != nil {
			return nil, err
		}
	}
	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}

	// Generate or use provided key. An _id_ in the document is the key: the CLI
	// accepts db.<bucket>.insert({_id_: "k", ...}) and the developer expects that
	// key to be the one the document is stored under.
	key := req.Key
	if key == "" {
		if id, ok := req.Data["_id_"].(string); ok && id != "" {
			key = id
		}
	}
	if key == "" {
		var err error
		key, err = ids.NewDocumentID()
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("generating document id", err)}, nil
		}
	}
	req.Data["_id_"] = key
	// A top-level name no path can reach is refused (E-18), and then a value
	// the bucket's declaration does not accept, except in read repair's put of
	// a document an owner already stores (spec §5.6).
	if !req.Admitted {
		if err := refuseUnreachableNames(req.Data, req.Database, req.Bucket); err != nil {
			return nil, err
		}
		if err := s.refuseMistyped(ctx, bucket.declaredFieldTypes(), bucket, req.Database, req.Bucket, req.Data); err != nil {
			return nil, err
		}
	}

	ver := writeVersionFrom(ctx)
	spared := false
	switch bucket.bucketType {
	case BucketTypeHash:
		spared, err = bucket.hashStore.PutBelow(applyInfoFrom(ctx, s), store.PutRequest{Key: key, Value: req.Data, Version: ver, Below: req.RepairBound,
			RefuseExisting: req.verb == verbInsert})
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUT"}, err)
		if refusal := uniqueRefusal(req.Bucket, err); refusal != nil {
			return nil, refusal
		}
		if refusal := insertExistsRefusal(req.Database, req.Bucket, key, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			s.logKeyTooLarge(req.Database, req.Bucket, err)
			return &InsertResponse{Success: false, Message: failureMessage("hash put failed", err)}, nil
		}
	case BucketTypeBTree:
		spared, err = bucket.btreeStore.PutBelow(applyInfoFrom(ctx, s), store.PutRequest{Key: key, Value: req.Data, Version: ver, Below: req.RepairBound,
			RefuseExisting: req.verb == verbInsert})
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUT"}, err)
		if refusal := insertExistsRefusal(req.Database, req.Bucket, key, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("btree put failed", err)}, nil
		}
	case BucketTypeFIFO, BucketTypeLIFO:
		bsonData, err := bson.Marshal(req.Data)
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("bson marshal failed", err)}, nil
		}
		_, err = bucket.queueStore.Push(applyInfoFrom(ctx, s), bsonData)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUT"}, err)
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("queue push failed", err)}, nil
		}
	case BucketTypeHeap:
		_, err := bucket.heapStore.PushItem(applyInfoFrom(ctx, s), 0, req.Data)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUT"}, err)
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("heap push failed", err)}, nil
		}
	case BucketTypeBlob:
		bsonData, err := bson.Marshal(req.Data)
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("bson marshal failed", err)}, nil
		}
		_, err = bucket.blobStore.Put(applyInfoFrom(ctx, s), key, bsonData, nil)
		err = s.notePayloadLeft(req.Database, req.Bucket, err)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUT"}, err)
		if err != nil {
			return &InsertResponse{Success: false, Message: failureMessage("blob put failed", err)}, nil
		}
	default:
		return nil, unsupportedForTypeErr("insert", bucket.bucketType)
	}

	// A repair put that found a newer write succeeded: the key holds data at
	// least as new as the read that sent it.
	if spared {
		return &InsertResponse{ID: key, Success: true, Seq: 1, Message: "document is newer than the repair; left in place"}, nil
	}
	return &InsertResponse{ID: key, Success: true, Seq: 1}, nil
}

// BatchInsert inserts multiple documents into a bucket in a single call.
// Every engine but blob writes all the documents in one batch of the node's
// store, under the bucket lock.
func (s *Server) BatchInsert(req BatchInsertRequest) (*BatchInsertResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*BatchInsertResponse, error) {
		return s.BatchInsertCtx(ctx, req)
	})
}

// BatchInsertCtx is BatchInsert with a context, carrying the write's WAL
// sequence so the read-repair version can be stamped on every document.
func (s *Server) BatchInsertCtx(ctx context.Context, req BatchInsertRequest) (*BatchInsertResponse, error) {
	if len(req.Data) == 0 {
		return &BatchInsertResponse{Success: false, Message: "no documents provided"}, nil
	}

	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if req.verb == verbUpsert {
		if err := refuseUpsertOn(req.Bucket, bucket.bucketType); err != nil {
			return nil, err
		}
	}

	// A standalone node's door does not run for a batch without now(), so the
	// handler asks too. On a cluster the door has refused it before the log and
	// this finds nothing.
	if req.verb == verbInsert && insertRefusesRepeatedIDs(bucket.bucketType) {
		if err := refuseRepeatedIDs(req.Data, req.Database, req.Bucket); err != nil {
			return nil, err
		}
	}

	// Generate IDs for all documents before writing. A caller-supplied
	// "_id_" is honored so documents can be keyed deterministically
	// (e.g. by a natural key) instead of a random UUID.
	var returnedIDs []string
	for i, doc := range req.Data {
		if doc == nil {
			doc = make(map[string]interface{})
			req.Data[i] = doc
		}
		if err := refuseEmptyDocumentID(doc, req.Database, req.Bucket); err != nil {
			return nil, inBatch(err, i+1, "")
		}
		key, _ := doc["_id_"].(string)
		if key == "" {
			var err error
			key, err = ids.NewDocumentID()
			if err != nil {
				return &BatchInsertResponse{Success: false, Message: failureMessage("generating document id", err)}, nil
			}
		}
		doc["_id_"] = key
		if err := refuseUnreachableNames(doc, req.Database, req.Bucket); err != nil {
			return nil, inBatch(err, i+1, key)
		}
	}
	if err := s.refuseMistypedBatch(ctx, bucket.declaredFieldTypes(), bucket, req.Database, req.Bucket, req.Data); err != nil {
		return nil, err
	}

	ver := writeVersionFrom(ctx)
	switch bucket.bucketType {
	case BucketTypeHash:
		returnedIDs, err = bucket.hashStore.PutBatch(applyInfoFrom(ctx, s), store.PutBatchRequest{Items: req.Data, Version: ver,
			RefuseExisting: req.verb == verbInsert})
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "BATCH PUT"}, err)
		if refusal := uniqueRefusal(req.Bucket, err); refusal != nil {
			return nil, refusal
		}
		if refusal := batchExistsRefusal(req.Database, req.Bucket, req.Data, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			s.noteOverBoundBatch(ctx, req, err)
			s.logKeyTooLarge(req.Database, req.Bucket, err)
			return &BatchInsertResponse{Success: false, Message: failureMessage("hash batch put failed", err)}, nil
		}
		return &BatchInsertResponse{IDs: returnedIDs, Count: int32(len(returnedIDs)), Success: true, Results: appliedResults(returnedIDs)}, nil

	case BucketTypeBTree:
		returnedIDs, err = bucket.btreeStore.PutBatch(applyInfoFrom(ctx, s), store.PutBatchRequest{Items: req.Data, Version: ver,
			RefuseExisting: req.verb == verbInsert})
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "BATCH PUT"}, err)
		if refusal := batchExistsRefusal(req.Database, req.Bucket, req.Data, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			s.noteOverBoundBatch(ctx, req, err)
			return &BatchInsertResponse{Success: false, Message: failureMessage("btree batch put failed", err)}, nil
		}
		return &BatchInsertResponse{IDs: returnedIDs, Count: int32(len(returnedIDs)), Success: true, Results: appliedResults(returnedIDs)}, nil

	case BucketTypeFIFO, BucketTypeLIFO:
		bsonItems := make([][]byte, 0, len(req.Data))
		for _, doc := range req.Data {
			bsonData, err := bson.Marshal(doc)
			if err != nil {
				return &BatchInsertResponse{Success: false, Message: failureMessage("bson marshal failed", err)}, nil
			}
			bsonItems = append(bsonItems, bsonData)
		}
		// The loop above gave every document its _id_, and the queue stores that
		// document as the element, so those are the ids it holds.
		returnedIDs = make([]string, len(req.Data))
		for i, doc := range req.Data {
			returnedIDs[i], _ = doc["_id_"].(string)
		}
		err := bucket.queueStore.PushBatch(applyInfoFrom(ctx, s), bsonItems)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "BATCH PUT"}, err)
		if err != nil {
			s.noteOverBoundBatch(ctx, req, err)
			return &BatchInsertResponse{Success: false, Message: failureMessage("queue batch push failed", err)}, nil
		}
		return &BatchInsertResponse{IDs: returnedIDs, Count: int32(len(returnedIDs)), Success: true, Results: appliedResults(returnedIDs)}, nil

	case BucketTypeHeap:
		items := make([]store.HeapItem, len(req.Data))
		for i, doc := range req.Data {
			items[i] = store.HeapItem{Priority: 0, Value: doc}
		}
		heapIDs, err := bucket.heapStore.PushItemBatch(applyInfoFrom(ctx, s), items)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "BATCH PUT"}, err)
		if err != nil {
			s.noteOverBoundBatch(ctx, req, err)
			return &BatchInsertResponse{Success: false, Message: failureMessage("heap batch push failed", err)}, nil
		}
		return &BatchInsertResponse{IDs: heapIDs, Count: int32(len(heapIDs)), Success: true, Results: appliedResults(heapIDs)}, nil

	default:
		return nil, unsupportedForTypeErr("batch insert", bucket.bucketType)
	}
}

// noteOverBoundBatch records, for the client's answer, a batch the store would
// not commit whole (BUG-0467 G11). This handler still answers in band, because
// a replica's apply and a replay read that shape (the sink's tooLarge, set by
// noteApplyOutcome, and BATCH_PUT_OK with success false); only the answer to
// the client is the ERROR frame BATCH_TOO_LARGE, which overBoundAnswer builds
// from what is recorded here. The storage key the batch stopped at is an
// internal encoding the client's message leaves out, so it is logged here in
// full for an operator.
func (s *Server) noteOverBoundBatch(ctx context.Context, req BatchInsertRequest, err error) {
	var tooLarge *storage.BatchTooLargeError
	if !errors.As(err, &tooLarge) {
		return
	}
	s.logger.Warn("refused a batch over the store's atomic bound",
		"database", req.Database, "bucket", req.Bucket, "count", len(req.Data), "error", err)
	outcome := applyOutcomeFrom(ctx)
	if outcome == nil {
		return
	}
	// The size is measured only here, on the refusal, so a batch that fits pays nothing.
	encoded, marshalErr := bson.Marshal(map[string]interface{}{"data": req.Data})
	if marshalErr != nil {
		encoded = nil
	}
	outcome.overBound = &batchTooLargeRefusal{cause: tooLarge, documents: len(req.Data), encodedBytes: len(encoded)}
}

// Find retrieves a document by key from a hash, btree, or blob bucket.
func (s *Server) Find(req FindRequest) (*FindResponse, error) {
	if req.Key == "" {
		return &FindResponse{Found: false, Message: "key required"}, nil
	}

	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()

	switch bucket.bucketType {
	case BucketTypeHash:
		raw, err := bucket.hashStore.Get(req.Key)
		if err != nil {
			return &FindResponse{Found: false, Message: s.readFailureMessage(req.Database, req.Bucket, "hash get failed", err)}, nil
		}
		if raw == nil {
			return &FindResponse{Found: false, Message: "document not found"}, nil
		}
		// The stored value is already BSON; hand it through untouched rather than
		// decoding to a map and re-encoding it on the way out.
		return &FindResponse{Found: true, Data: bson.Raw(raw)}, nil

	case BucketTypeBTree:
		raw, err := bucket.btreeStore.Get(req.Key)
		if err != nil {
			return &FindResponse{Found: false, Message: s.readFailureMessage(req.Database, req.Bucket, "btree get failed", err)}, nil
		}
		if raw == nil {
			return &FindResponse{Found: false, Message: "document not found"}, nil
		}
		return &FindResponse{Found: true, Data: bson.Raw(raw)}, nil

	case BucketTypeBlob:
		stored, err := bucket.blobStore.Get(req.Key)
		if err != nil {
			return &FindResponse{Found: false, Message: s.readFailureMessage(req.Database, req.Bucket,
				fmt.Sprintf("blob get failed for key %q in %s.%s", req.Key, req.Database, req.Bucket), err) + blobReadFix(err)}, nil
		}
		if stored == nil {
			return &FindResponse{Found: false, Message: fmt.Sprintf("blob %q not found in %s.%s", req.Key, req.Database, req.Bucket)}, nil
		}
		raw, meta, blobID := stored.Content, stored.Metadata, stored.BlobID
		// Blob payloads are arbitrary bytes, not documents: when the payload is
		// BSON it is returned as the document, otherwise the key/identifier/size
		// and stored metadata are returned so the caller still gets an answer.
		var doc map[string]interface{}
		if err := bson.Unmarshal(raw, &doc); err != nil || doc == nil {
			doc = map[string]interface{}{
				"_id_":    req.Key,
				"blob_id": blobID,
				"size":    len(raw),
			}
			for k, v := range meta {
				if _, exists := doc[k]; !exists {
					doc[k] = v
				}
			}
		} else if _, ok := doc["_id_"]; !ok {
			doc["_id_"] = req.Key
		}
		encoded, err := bson.Marshal(doc)
		if err != nil {
			return &FindResponse{Found: false, Message: failureMessage("encode blob document", err)}, nil
		}
		return &FindResponse{Found: true, Data: bson.Raw(encoded)}, nil

	default:
		return nil, unsupportedForTypeErr("get", bucket.bucketType)
	}
}

// Exists reports whether a key is stored in a hash, btree or blob bucket. Unlike
// Find it never turns a refusal or a failed read into "not there": a bucket
// type with no keys and an empty key come back as ErrExistsRefused, and a read
// that failed comes back as an error, because exists:false is an answer and
// those are not.
func (s *Server) Exists(req FindRequest) (bool, error) {
	if req.Key == "" {
		return false, fmt.Errorf("%w: key required. Name the key to test in %s.%s", ErrExistsRefused, req.Database, req.Bucket)
	}

	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return false, err
	}
	defer snap.Discard()

	var found bool
	switch bucket.bucketType {
	case BucketTypeHash:
		var raw []byte
		raw, err = bucket.hashStore.Get(req.Key)
		found = raw != nil
	case BucketTypeBTree:
		var raw []byte
		raw, err = bucket.btreeStore.Get(req.Key)
		found = raw != nil
	case BucketTypeBlob:
		var stored *store.StoredBlob
		stored, err = bucket.blobStore.Get(req.Key)
		found = stored != nil
	default:
		return false, wrongTypeSentence(fmt.Sprintf("exists is not supported for bucket type %s: a %s has no keys to test. Use peek to read its next element, or count for its size.",
			bucket.bucketType, bucket.bucketType), bucket.bucketType)
	}
	if err != nil {
		return false, errors.New(s.readFailureMessage(req.Database, req.Bucket,
			fmt.Sprintf("exists check of key %q in %s.%s failed", req.Key, req.Database, req.Bucket), err))
	}
	return found, nil
}

// Push adds a document to a bucket.
func (s *Server) Push(req PushRequest) (*PushResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*PushResponse, error) {
		return s.PushCtx(ctx, req)
	})
}

// PushCtx is Push with a context, which carries the apply sequence of a
// replicated or replayed push down to an engine on the shared store.
func (s *Server) PushCtx(ctx context.Context, req PushRequest) (*PushResponse, error) {
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}

	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if err := refuseEmptyDocumentID(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}

	// An _id_ in the document is the element's id, the same rule inserts follow:
	// the coordinator resolves it before the entry is logged and shipped, so
	// every applier stores the same element under the same key. Minting one here
	// instead — as this used to — gave each replica and each replay its own id,
	// which is what made a replayed push add a second element rather than
	// overwrite the one it already wrote.
	id, _ := req.Data["_id_"].(string)
	if id == "" {
		var err error
		id, err = ids.NewDocumentID()
		if err != nil {
			return &PushResponse{Success: false, Message: failureMessage("generating element id", err)}, nil
		}
	}
	req.Data["_id_"] = id
	if err := refuseUnreachableNames(req.Data, req.Database, req.Bucket); err != nil {
		return nil, err
	}
	if err := s.refuseMistyped(ctx, bucket.declaredFieldTypes(), bucket, req.Database, req.Bucket, req.Data); err != nil {
		return nil, err
	}

	switch bucket.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO:
		bsonData, err := bson.Marshal(req.Data)
		if err != nil {
			return &PushResponse{Success: false, Message: failureMessage("bson marshal failed", err)}, nil
		}
		_, err = bucket.queueStore.Push(applyInfoFrom(ctx, s), bsonData)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUSH"}, err)
		if err != nil {
			return &PushResponse{Success: false, Message: failureMessage("queue push failed", err)}, nil
		}
	case BucketTypeHeap:
		_, err := bucket.heapStore.PushItem(applyInfoFrom(ctx, s), req.Priority, req.Data)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUSH"}, err)
		if err != nil {
			return &PushResponse{Success: false, Message: failureMessage("heap push failed", err)}, nil
		}
	// A push on a hash or btree bucket is an insert of the document under its
	// _id_ (spec §6.3), so it refuses an _id_ the bucket holds.
	case BucketTypeHash:
		_, err := bucket.hashStore.PutBelow(applyInfoFrom(ctx, s), store.PutRequest{Key: id, Value: req.Data, Version: writeVersionFrom(ctx),
			RefuseExisting: req.verb == verbInsert})
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUSH"}, err)
		if refusal := uniqueRefusal(req.Bucket, err); refusal != nil {
			return nil, refusal
		}
		if refusal := insertExistsRefusal(req.Database, req.Bucket, id, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			return &PushResponse{Success: false, Message: failureMessage("hash put failed", err)}, nil
		}
	case BucketTypeBTree:
		_, err := bucket.btreeStore.PutBelow(applyInfoFrom(ctx, s), store.PutRequest{Key: id, Value: req.Data, Version: writeVersionFrom(ctx),
			RefuseExisting: req.verb == verbInsert})
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "PUSH"}, err)
		if refusal := insertExistsRefusal(req.Database, req.Bucket, id, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			return &PushResponse{Success: false, Message: failureMessage("btree put failed", err)}, nil
		}
	default:
		return nil, unsupportedForTypeErr("push", bucket.bucketType)
	}

	return &PushResponse{ID: id, Success: true, Seq: 1}, nil
}

// Pop removes and returns the next req.Limit entries of a FIFO, LIFO, or heap
// bucket, in pop order.
func (s *Server) Pop(req PopRequest) (*PopResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*PopResponse, error) {
		return s.PopCtx(ctx, req)
	})
}

// popEngine is what PopCtx needs of a bucket's queue or heap store.
type popEngine struct {
	// noun is how a message names the store: "queue" or "heap".
	noun string
	// popN is the store's PopN.
	popN func(info store.ApplyInfo, n int, now int64, visit func(entry []byte) error) (store.PopResult, error)
	// entries is the count the store's state document holds.
	entries func() (int64, error)
}

// popEngineOf is the engine of a fifo, lifo or heap bucket, and false for any
// other type.
func popEngineOf(bucket *inMemoryBucket) (popEngine, bool) {
	switch bucket.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO:
		return popEngine{"queue", bucket.queueStore.PopN, bucket.queueStore.Len}, true
	case BucketTypeHeap:
		return popEngine{"heap", bucket.heapStore.PopN, bucket.heapStore.Len}, true
	}
	return popEngine{}, false
}

// popCeiling is the result ceiling a pop is judged by, and whether it is the
// owner's stamp. Every applier of a logged pop must reach the same decision, so
// a logged entry is judged by the ceiling its owner stamped on it
// (Cluster.applyAsOwner), clamped to the range the setting is clamped to, and
// not by this node's setting. A request that was never logged is judged by this
// node's, and a client's own result_ceiling is ignored: it could not raise it.
func (s *Server) popCeiling(ctx context.Context, requested int64) (int64, bool) {
	if applySeqFrom(ctx) == 0 || requested <= 0 {
		return s.maxResultBytes(), false
	}
	return min(max(requested, config.MinMaxResultBytes), config.MaxMaxResultBytes), true
}

// answeringApply is whether this apply is the one that answers the client,
// and so counts a refusal or a delivery in the metrics (pop, claim and the
// lease verbs): the node that took the request, or
// the owner's first apply of a logged one. A replica's apply and a replay
// charge the same entries to reach the same decision and count nothing.
func answeringApply(ctx context.Context) bool {
	return applySeqFrom(ctx) == 0 || (lineageFrom(ctx).originated && !isReplay(ctx))
}

// PopCtx is Pop with a context, which carries the apply sequence of a
// replicated or replayed pop down to an engine on the shared store: a pop is
// gated by the bucket's applied_seq like every other write.
//
// Each entry is charged to the result budget before it is removed, inside the
// store's transaction, so a pop over the ceiling is refused with nothing
// removed; the first entry is exempt, as it is for a peek.
func (s *Server) PopCtx(ctx context.Context, req PopRequest) (*PopResponse, error) {
	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	engine, ok := popEngineOf(bucket)
	if !ok {
		return nil, wrongTypeSentence(popRefusal(bucket.bucketType), bucket.bucketType)
	}
	limit, err := popLimit(req.Limit)
	if err != nil {
		return nil, err
	}

	ceiling, stamped := s.popCeiling(ctx, req.ResultCeiling)
	budget := s.newResultBudgetWithCeiling(queryMethodPop, ceiling, answeringApply(ctx), resultTooLargeError{
		query: fmt.Sprintf("pop({limit: %d}) on %s.%s", limit, req.Database, req.Bucket), shape: resultShapePop, limit: limit, onOwner: stamped,
	})
	defer budget.report()
	// The walk runs inside the apply, and every copy of a logged pop must reach
	// the same decision: a caller who has gone must not fail the owner's apply
	// alone, so the walk never sees the request's context.
	walk := newQueryWalk(context.Background(), queryMethodPop, req.Database, req.Bucket)
	var items []bson.Raw
	visit := func(entry []byte) error {
		if err := walk.visit(); err != nil {
			return err
		}
		// The entry is the iterator's copy, so it is kept as it is.
		cost := int64(len(entry)) + elementHeader(len(items))
		if len(items) == 0 {
			budget.chargeFirst(cost)
		} else if err := budget.chargeDocument(cost); err != nil {
			return err
		}
		items = append(items, bson.Raw(entry))
		return nil
	}

	// A pop first returns the leases that have ended, at the owner's stamped
	// time; a logged pop with none skips the return step (claim-ack-spec 4.10).
	popped, err := engine.popN(applyInfoFrom(ctx, s), limit, s.popLeaseNow(ctx, req.LeaseNow), visit)
	removed := popped.Removed
	// A pop that found nothing but wrote (it returned or dead-lettered a lease)
	// is not an empty pop: its answer waits for the sync (build doc B-7).
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "POP", emptyPop: err == nil && removed == 0 && !popped.Delivery.Wrote}, err)
	if err == nil {
		s.noteDelivery(ctx, req.Database, req.Bucket, popped.Delivery)
	}
	if err != nil {
		// A refusal leaves as an error so errorFrame sets RESULT_TOO_LARGE with
		// its byte fields; turned into found:false it would read as an empty
		// queue, and a consumer looping "pop until empty" would stop draining.
		if errors.Is(err, ErrResultTooLarge) {
			return nil, err
		}
		return &PopResponse{Found: false, Message: s.popFailureMessage(req, engine, limit, err)}, nil
	}
	if removed == 0 {
		return &PopResponse{Found: false, Message: engine.noun + " is empty", Pending: popped.Pending, Dead: popped.Dead}, nil
	}
	return &PopResponse{Found: true, Items: items, Pending: popped.Pending, Dead: popped.Dead}, nil
}

// popFailureMessage words a pop the store failed. A state that counts entries
// the range does not hold and a batch too large for the store each name the
// cause and the fix, and neither carries the storage key, which is an internal
// encoding as long as the entry's id; the rest is readFailureMessage's.
func (s *Server) popFailureMessage(req PopRequest, engine popEngine, limit int, err error) string {
	failure := engine.noun + " pop failed"
	switch {
	case errors.Is(err, store.ErrQueueCursorLost):
		counted, _ := engine.entries()
		return fmt.Sprintf("%s: the %s's state counts %d entries and none is at its cursor, so nothing was removed. "+
			"On a cluster, restore this bucket from a replica with 'hoarddb dump' on the replica and 'restore' here. "+
			"On a standalone node, restore it from a dump or backup", failure, engine.noun, counted)
	case errors.Is(err, storage.ErrBatchTooLarge):
		var tooLarge *storage.BatchTooLargeError
		if errors.As(err, &tooLarge) {
			s.logger.Warn("a pop did not fit in one batch; the message to the client leaves out the storage key",
				"database", req.Database, "bucket", req.Bucket, "limit", limit, "bound_bytes", tooLarge.Bytes, "key_bytes", len(tooLarge.Key))
		}
		return popTooLargeMessage(failure, limit, err)
	}
	return s.readFailureMessage(req.Database, req.Bucket, failure, err)
}

// popTooLargeMessage tells a client that the deletes of a pop did not fit in
// the largest batch the store commits, what the pop can do about it, and that
// nothing was removed. failure opens it ("fifo pop failed"). It is the one
// wording of the bucket pop and the store-field pop; a batch put's advice,
// to send fewer documents or ingest, is no use to a pop.
func popTooLargeMessage(failure string, limit int, err error) string {
	bound := ""
	var tooLarge *storage.BatchTooLargeError
	if errors.As(err, &tooLarge) {
		bound = fmt.Sprintf(" (%s bytes)", groupThousands(int(tooLarge.Bytes)))
	}
	if limit > 1 {
		return fmt.Sprintf("%s: removing %d entries at once does not fit in the largest batch the store commits%s, so nothing was removed. "+
			"Pop fewer entries at a time: pop({limit: %d})", failure, limit, bound, limit/2)
	}
	return fmt.Sprintf("%s: removing this entry does not fit in the largest batch the store commits%s, so it was not removed. "+
		"Raise HOARDDB_MEMTABLE_MB on this node, which raises that batch size, and pop again", failure, bound)
}

// maxPeekLimit is the most entries one peek returns: the cap list already
// clamps to. A peek has no continuation token, so above the cap it refuses
// where list clamps.
const maxPeekLimit = 1000

// Peek returns the next req.Limit entries a pop of a fifo, lifo or heap bucket
// would take, in pop order, removing none of them. It reads one snapshot and
// charges each entry to the result budget as the walk yields it, so a request
// over the ceiling is refused without reading further; the first entry is
// exempt, because a pop returns it however large.
func (s *Server) Peek(req PeekRequest) (*PeekResponse, error) {
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()

	switch bucket.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap:
	default:
		return nil, wrongTypeSentence(peekRefusal(bucket.bucketType), bucket.bucketType)
	}
	limit, err := peekLimit(req.Limit, peekAdvice(bucket.bucketType))
	if err != nil {
		return nil, err
	}

	budget := s.newResultBudget(queryMethodPeek, resultTooLargeError{
		query: fmt.Sprintf("peek({limit: %d}) on %s.%s", limit, req.Database, req.Bucket), shape: resultShapePeek, limit: limit,
	})
	defer budget.report()
	// A peek takes no execution slot and has no context to observe: it is
	// bounded by the cap and the ceiling, as a pop is (result_budget.go).
	walk := newQueryWalk(context.Background(), queryMethodPeek, req.Database, req.Bucket)
	var items []bson.Raw
	visit := func(entry []byte) error {
		if err := walk.visit(); err != nil {
			return err
		}
		// The entry is the iterator's copy, so it is kept as it is.
		cost := int64(len(entry)) + elementHeader(len(items))
		if len(items) == 0 {
			budget.chargeFirst(cost)
		} else if err := budget.chargeDocument(cost); err != nil {
			return err
		}
		items = append(items, bson.Raw(entry))
		return nil
	}

	emptyMessage, failure := "queue is empty", "queue peek failed"
	var counts store.DeliveryCounts
	if bucket.bucketType == BucketTypeHeap {
		counts, err = bucket.heapStore.PeekCounts(limit, visit)
		emptyMessage, failure = "heap is empty", "heap peek failed"
	} else {
		counts, err = bucket.queueStore.PeekCounts(limit, visit)
	}
	if err != nil {
		// A refusal leaves as an error so errorFrame sets RESULT_TOO_LARGE with
		// its byte fields; turned into a found:false answer it would read as an
		// empty queue.
		if errors.Is(err, ErrResultTooLarge) {
			return nil, err
		}
		return &PeekResponse{Found: false, Message: s.readFailureMessage(req.Database, req.Bucket, failure, err)}, nil
	}
	held := PeekResponse{Pending: counts.Pending, Dead: counts.Dead, Expired: s.leaseHasEnded(counts)}
	if len(items) == 0 {
		held.Message = emptyMessage
		return &held, nil
	}
	held.Found, held.Items = true, items
	return &held, nil
}

// maxPopLimit is the most entries one pop removes. It is a tenth of peek's
// cap because a pop's answer is the only copy of what it removed: an answer
// lost after the commit loses every entry in it (pop-n-spec.md section 7).
const maxPopLimit = 100

// requestLimit is the count a request asks for: zero or absent is one. advice
// finishes the refusal of a count above ceiling, whole, so each verb words the
// fix its own way.
func requestLimit(verb string, requested, ceiling int, sentinel error, advice string) (int, error) {
	switch {
	case requested < 0:
		return 0, fmt.Errorf("%w: %d is not valid; %s({limit: N}) takes a whole number of 1 or more", sentinel, requested, verb)
	case requested > ceiling:
		return 0, fmt.Errorf("%w: %d is above the %s maximum of %d. %s", sentinel, requested, verb, ceiling, advice)
	case requested == 0:
		return 1, nil
	}
	return requested, nil
}

// peekLimit is requestLimit for a peek. advice names what reads more.
func peekLimit(requested int, advice string) (int, error) {
	return requestLimit("peek", requested, maxPeekLimit, ErrPeekLimit, "To read more than that, "+advice)
}

// popLimit is requestLimit for a pop. The peek it offers is capped at peek's
// own maximum, so the advice is never itself refused.
func popLimit(requested int) (int, error) {
	advice := fmt.Sprintf("A pop removes what it returns, so it takes at most %d at a time: repeat pop({limit: %d}), or read them without removing any with peek({limit: %d})",
		maxPopLimit, maxPopLimit, min(requested, maxPeekLimit))
	return requestLimit("pop", requested, maxPopLimit, ErrPopLimit, advice)
}

// peekAdvice is what a bucket queue offers instead of a peek above the cap: a
// fifo's storage order is its pop order, so find() reads it; a lifo's and a
// heap's is not, so only pop() does (a bucket queue has no list).
func peekAdvice(bucketType BucketType) string {
	if bucketType == BucketTypeFIFO {
		return "find() the bucket (a fifo's storage order is its pop order) or pop() them"
	}
	return "pop() them"
}

// peekRefusal is the answer to a peek of a bucket type with no pop order: why,
// and the verbs that do read it.
func peekRefusal(bucketType BucketType) string {
	reading := `Read one with get("<key>") or several with find().`
	if bucketType == BucketTypeBlob {
		reading = `Read one with get("<key>") or its metadata with info("<key>").`
	}
	return fmt.Sprintf(`peek is not defined for %s buckets: a %s has no order to take from, so there is no "next" to look at. %s`, bucketType, bucketType, reading)
}

// popRefusal is the answer to a pop of a bucket type with no pop order: why,
// and the verbs that do remove and read it.
func popRefusal(bucketType BucketType) string {
	reading := `read them with get("<key>") or find()`
	if bucketType == BucketTypeBlob {
		reading = `read its metadata with info("<key>")`
	}
	return fmt.Sprintf(`pop is not defined for %s buckets: a %s has no order to take from, so there is no "next" to remove. Remove one with delete("<key>"), or %s.`, bucketType, bucketType, reading)
}

// Delete removes a document by key from a hash, btree, or blob bucket.
func (s *Server) Delete(req DeleteRequest) (*DeleteResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*DeleteResponse, error) {
		return s.DeleteCtx(ctx, req)
	})
}

// DeleteCtx is Delete with a context, which carries the apply sequence of a
// replicated or replayed delete down to an engine on the shared store.
func (s *Server) DeleteCtx(ctx context.Context, req DeleteRequest) (*DeleteResponse, error) {
	if req.Key == "" {
		return &DeleteResponse{Success: false, Message: "key required"}, nil
	}

	bucket, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()

	switch bucket.bucketType {
	case BucketTypeHash:
		if req.ExpireIf != nil {
			return s.expireLocked(ctx, bucket, req)
		}
		return s.deleteStoredDocument(ctx, bucket, req)

	case BucketTypeBTree:
		// A btree bucket does not evaluate the condition, and deleting
		// unconditionally would remove a document refreshed after the sweep's scan.
		if req.ExpireIf != nil {
			return &DeleteResponse{Success: false, Message: fmt.Sprintf(
				"bucket %q is a btree bucket and does not evaluate expire_if; the delete was not applied. Expiry conditions apply to hash buckets only; send a plain delete to remove key %q",
				req.Bucket, req.Key), WriteOutcome: outcomeNotApplied}, nil
		}
		return s.deleteStoredDocument(ctx, bucket, req)

	case BucketTypeBlob:
		deleted, err := bucket.blobStore.Delete(applyInfoFrom(ctx, s), req.Key)
		err = s.notePayloadLeft(req.Database, req.Bucket, err)
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "DELETE"}, err)
		if err != nil {
			return &DeleteResponse{Success: false, Message: failureMessage("blob delete failed", err)}, nil
		}
		if !deleted {
			return deleteNotFound("blob not found"), nil
		}
		return &DeleteResponse{Success: true, Message: "blob deleted"}, nil

	default:
		return nil, unsupportedForTypeErr("delete", bucket.bucketType)
	}
}

// deleteNotFound is the answer to a delete of a key that is not stored,
// the same on every store type that has keys. The delete itself is idempotent,
// which replication, replay and a TTL expiry rely on; only the reply to the
// client says the key was absent. Nothing was removed, so a retry is safe.
func deleteNotFound(message string) *DeleteResponse {
	return &DeleteResponse{Success: false, Message: message, WriteOutcome: outcomeNotApplied}
}

// deleteStoredDocument deletes a hash or btree document under bucket.mu and says
// whether it was there. The existence read and the delete happen under the same
// hold of the lock that every write to the bucket takes, so nothing can remove
// the key between them. A repair's delete bounded by a version that finds the
// document newer leaves it, and says so as a spared expiry does.
func (s *Server) deleteStoredDocument(ctx context.Context, bucket *inMemoryBucket, req DeleteRequest) (*DeleteResponse, error) {
	var stored []byte
	var err error
	if bucket.bucketType == BucketTypeBTree {
		stored, err = bucket.btreeStore.Get(req.Key)
	} else {
		stored, err = bucket.hashStore.Get(req.Key)
	}
	if err != nil {
		// The store failed the existence read, not the delete: mark it and
		// record it as a failed apply, so the replica path leaves the entry for
		// its owner to re-send (BUG-0583) and the owner's replay retries it
		// (BUG-0584).
		readErr := markReadApplyFailure(err)
		s.recordReadApplyFailure(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "DELETE"}, readErr)
		return &DeleteResponse{Success: false, Message: deleteFailureMessage(bucket, req, readErr)}, nil
	}
	spared, err := s.deleteDocument(ctx, bucket, req)
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: bucket, op: "DELETE"}, err)
	switch {
	case err != nil:
		return &DeleteResponse{Success: false, Message: deleteFailureMessage(bucket, req, err)}, nil
	case stored == nil:
		return deleteNotFound("document not found"), nil
	case spared:
		return &DeleteResponse{Success: true, Spared: true, Message: "document is newer than the repair; left in place"}, nil
	}
	return &DeleteResponse{Success: true, Message: "document deleted"}, nil
}

// Search searches a bucket by secondary index filter values.
func (s *Server) Search(req SearchRequest) (*SearchResponse, error) {
	return s.SearchCtx(context.Background(), req)
}

// SearchCtx is Search under a context, which its fetches observe, and a result
// budget. The planner and both ways of answering live in search_plan.go.
func (s *Server) SearchCtx(ctx context.Context, req SearchRequest) (*SearchResponse, error) {
	return s.searchPlanned(ctx, req)
}
