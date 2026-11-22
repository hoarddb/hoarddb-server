// Package server provides the core server implementation for hoardDB.
//
// The server manages TLS transport, authentication, storage, and
// request handling for the distributed hash-ring database.
//
// For the TCP transport wrapper, see service.go and dispatch.go.
// This file contains the core Server struct and its business logic methods.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// Server is the core server for hoardDB (business logic only).
//
// Deprecated: Use NewService to create a TCP-based server.
type Server struct {
	// ringLoading, when set, is the ring node's answer to "why does this node serve nothing yet":
	// empty when it serves, else the reason HEALTH_CHECK reports (replaying its log, diverged).
	ringLoading atomic.Pointer[func() string]
	// ringPasswordKey, when set, is the ring node's password_key status for health (ring user
	// store spec 5.2); nil before the ring node opens and after it closes.
	ringPasswordKey atomic.Pointer[func() string]
	// refusalsWarned says a bucket's refusal record grew past
	// failedSeqsWarnAt, once (recordRefusal).
	refusalsWarned atomic.Bool
	config         *config.Config
	logger         *slog.Logger
	keystore       *Keystore   // the key derivation engine; nil when keystore=off
	nodeMaster     *NodeMaster // the 32-byte root key for node-level encryption; nil when keystore=off
	// credentials reads and writes the credential files in the data directory
	// (cluster.token, auth.token.key, keys/server.key, auth.cluster.pepper), sealed under the node
	// master when the node seals them.
	credentials CredentialKeys
	// blobKeys seals blob payload files; nil on a node whose store is not
	// sealed, where payloads are written and read as they are. Loaded once at
	// start, above every lock.
	blobKeys sealed.BlobKeySource
	// keyUnwrapAt is when the node master was unwrapped at start, zero when
	// there is no keystore. NewService runs before an audit log exists, so
	// Start writes the key.unwrap entry with this time.
	keyUnwrapAt time.Time
	// wal durably records replicated mutations before they are applied, so a
	// node can replay them after a restart. Nil in single-node mode, where
	// mutations go straight to storage.
	wal *storage.WAL
	// walMode is how storage.wal_preallocate resolved at this start; the zero
	// value on a node with no log.
	walMode walModeResolution
	// walMetrics is the log's registry, nil on a node with no log.
	walMetrics *prometheus.Registry
	// seedMetrics is the seeding counters' registry, nil on a node with no entry-ring node.
	seedMetrics *prometheus.Registry
	// newLogMarkScans counts the walks of every bucket's applied mark that an
	// open finding the log new made (passMarksUnderANewLog); a test reads it.
	newLogMarkScans atomic.Int64
	// cluster holds ring placement and replication. Nil in single-node mode,
	// where every request is served locally.
	cluster *Cluster
	// catalogLoaded is set once LoadCatalog has attached the buckets on disk.
	// The cluster's start-time sweep walks the catalog and must not run over
	// an empty one (Cluster.resumeSettling).
	catalogLoaded atomic.Bool
	// queryGateOnce builds gate, the node's query slots, on first use
	// (admission.go:Server.queryGate).
	queryGateOnce sync.Once
	gate          *queryGate
	// batchSlotsOnce builds batchSlotsCh, the node's batch-item semaphore, on
	// first use (batch_run.go:Server.batchSlots).
	batchSlotsOnce sync.Once
	batchSlotsCh   chan struct{}
	// replayOnce guards WAL replay so it happens exactly once per process.
	replayOnce   sync.Once
	replayErr    error
	ctx          context.Context
	cancel       context.CancelFunc
	listener     net.Listener
	fingerprint  string
	auth         *AuthManager
	audit        *AuditLogger
	startTime    time.Time
	knownNodes   map[string]*NodeInfo
	knownNodesMu sync.RWMutex

	// dataStore holds in-memory bucket data for CRUD operations.
	dataStore *serverData

	// kv is the node's one store (nodestore.go): the catalog and the bucket id
	// allocator live in it. Opened by NewService, closed last by Stop.
	kv storage.KV
	// ackSyncs shares one sync of kv among the writes waiting to be answered
	// on a node with no write-ahead log (ack_sync.go). Its zero value is ready.
	ackSyncs ackSyncGroup
	// memtable is the size the store was opened at and why; status, the
	// gauge and gossip report it. Set once, before anything reads it.
	memtable memtableResolution
	// storeSeal is the structural check of the store, computed once right after
	// it opened. The operator surface (status, metrics, describe) reports it;
	// the cluster paths still ask storeSealed, which follows the mode.
	storeSeal storeSealCheck
	// ringAtRest is the ring node's keys as loadRingAtRest unwrapped them before anything
	// was written, handed to the ring node at Start; nil when the mode does not seal.
	ringAtRest *ringAtRest
	// ringSeal is the structural check of the ring store, taken once right after the ring node
	// opens; nil until then. /health reports it (healthEncryption).
	ringSeal atomic.Pointer[storeSealCheck]
	// blobLock holds a configured storage.blob_dir for this node alone
	// (blob_dir_unix.go); nil when blob_dir is unset. Released last by Stop.
	blobLock *blobDirLock
	// blobDiskSeparate is whether the blob directory is on a filesystem of
	// its own, decided once at start (blob_disk.go). When it is, the join
	// guard and the capacity sampler read that filesystem as well as the data
	// directory's.
	blobDiskSeparate bool
	// bucketIDs allocates bucket ids, seeded from the store when it opens.
	bucketIDs bucketIDAllocator
	// materializations is the ids whose migrated range is being materialised
	// now, which the reclaimer's orphaned-range source must not touch
	//. Zero value is ready to use.
	materializations materializeTracker
	// generations serialises the changes to a database's incarnations on this
	// node, and is how a waiter learns that a generation was dropped
	// (generation.go). Zero value is ready to use.
	generations generationState

	// owed is the node-wide registry of sequences whose store apply failed
	// and which replay still owes (owed.go); it bounds WAL retention.
	owed owedRegistry

	// decls holds the declaration of every bucket that declares indexes or
	// store fields, by bucket id (decl_table.go). Zero value is ready to use.
	decls declTable

	// schemas interns the buckets' typed-field declarations, so identical
	// ones share one schema (fieldtypes_schema.go). Zero value is ready to use.
	schemas schemaInterner

	// fieldTypes is the set of typed-field declarations attached buckets
	// hold, each by the small id its record carries (field_type_set.go).
	// Zero value is ready to use.
	fieldTypes fieldTypeSet

	// fieldTypeWarnings is field_types: warn's hourly aggregate, flushed by the
	// first mismatch after the hour and at Stop (fieldtypes_enforce.go). Zero
	// value is ready to use.
	fieldTypeWarnings fieldTypeWarnings

	// orphanWarned holds "database/bucket" for each bucket whose orphaned
	// index entry this start has already logged, so the Warn is once per
	// bucket per start, not once per search (GAP-14 build doc Step 12).
	orphanWarned sync.Map

	// ttl is the registry of buckets declaring a TTL index, which the one
	// sweeper goroutine walks; ttlWG is what Stop waits on for it.
	ttl   ttlRegistry
	ttlWG sync.WaitGroup
	// ttlSkips paces the sweeper's line about a document it skipped.
	ttlSkips ttlSkipLog
	// deadLetters paces the line about dead-lettered entries and hard-cap
	// refusals (claim-ack-spec 11.2); the capacity sampler's tick flushes it.
	deadLetters deadLetterLog
	// leaseClock is this node's wall clock for lease time, read only where
	// nothing is logged (a standalone node, or a pop or lease verb that is
	// not a logged entry). A per-node field and not nowClockHook, which is
	// process-global: three in-process nodes could not be skewed against each
	// other. Nil is time.Now.
	leaseClock func() time.Time
	// leaseClockSteps paces the WARN about a lease clock step to one line a
	// minute node-wide (claim-ack-spec 4.8).
	leaseClockSteps ttlSkipLog
	// reclaimWake tells the node's one reclaimer (reclaim.go) that a drop left
	// work; reclaimWG is what Stop waits on before it closes the store.
	reclaimWake chan struct{}
	reclaimWG   sync.WaitGroup
	// unlinkWindow is the drops between their commit and their unlink, whose
	// records the reclaimer must not step yet (reclaim.go, BUG-0527).
	unlinkWindow unlinkWindow
	// probes is the node's capacity readings (capacity_probes.go). The join
	// guard and the capacity sampler both read disk through it, so one
	// override reaches both. Stored once by NewService; a test replaces it
	// with a fresh copy, never by mutating the loaded value.
	probes atomic.Pointer[capacityProbes]
	// capacity is the node's capacity sampler (capacity_sampler.go), set when
	// it starts; capacityWG is what both Stop paths wait on before they close
	// the store and the WAL it reads.
	capacity   *capacitySampler
	capacityWG sync.WaitGroup
	// hostFacts serves the CPU, memory and load a health reply reports,
	// re-read at most once a second (host_facts.go). Its zero value reads
	// this machine.
	hostFacts hostFactsCache
	// bucketsAttached is the number of bucket structs with a non-zero id that
	// are not closed; moved only by noteBucketAttached and noteBucketDetached,
	// and published as hoarddb_node_buckets. A counter, not a walk: the walk
	// would be O(buckets) every sample.
	bucketsAttached atomic.Int64

	// recentDrops is the join fence's short, bounded memory of buckets this
	// node has dropped (cluster_join.go): a fixed-size ring, never
	// one entry per bucket ever dropped. Zero value is ready to use.
	recentDrops recentDropLog

	// fenceMapMu guards fences: one apply fence per bucket placement key with an
	// operation in flight (fence.go).
	fenceMapMu sync.Mutex
	fences     map[placementKey]*fenceEntry

	// routerGates is the router gate of each spread template with a routed
	// operation in flight here (spread_gate.go); an entry only while held or
	// awaited, like fences. Zero value is ready to use.
	routerGates routerGateMap
	// spreadDumps is the dump of each spread collection this node routes that
	// a client is dumping (spread_dump.go); an entry only while it runs.
	spreadDumps spreadDumpMap

	// authReqMu guards authReqSeen, the bounded request_id dedup table for
	// user mutations (spec 6.1).
	authReqMu   sync.Mutex
	authReqSeen map[string]time.Time

	// denials batches repeated authorization refusals (spec 4 rule 5).
	denials denialTracker

	// userSessionCloser, when set, closes a user's live sessions on this node.
	// The Service installs the driver listener's registry at startup (spec
	// 8.8). Nil on a server with no listener (unit tests) and in that case
	// closeUserSessions is a no-op. Atomic: a cluster member's user-table
	// follower reads it from its own goroutine while Start installs it.
	userSessionCloser atomic.Pointer[func(username string) int]

	// dumpMu guards dump, lastDump AND restorePins: dump-restore-spec.md §10
	// allows at most one live dump per server (so a session pointer, rather
	// than a map, is enough there), while §9.7's restore pin is per bucket —
	// two concurrent merge restores into different buckets, or even the same
	// bucket, are allowed (§9.2 makes a merge idempotent); only a --drop
	// restore is mutually exclusive with anything else on its bucket. Both
	// live under this one mutex, not two separate ones, because the dump
	// pin and the restore pin have to be checked and set as a single atomic
	// step against each other (server/dump.go's claimDump, server/restore.go's
	// claimBucketRestorePin) — a dump-pin check and a restore-pin insert
	// under two different mutexes had a window where both could win the
	// same bucket (fix round 1, Important #1). The pin ceiling itself is
	// operator configurable (config.Server.DumpPinCeilingSeconds,
	// HOARDDB_LISTEN-style), read directly off s.config where it's used
	// rather than copied into a field here — the same pattern
	// WALRetentionSeconds already uses.
	dumpMu      sync.Mutex
	dump        *dumpSession
	lastDump    *dumpSession
	restorePins map[string]*restoreBucketPin
	// dumpWG is what both Stop paths wait on for a dump's walk, which reads
	// the store they close. dumpWalksStopped, guarded by dumpMu,
	// refuses a walk launched once that wait has begun; launchDumpWalk
	// (dump.go) says why the two go together.
	dumpWG           sync.WaitGroup
	dumpWalksStopped bool

	// restoreSessionMu guards restoreSessions: unlike the dump session
	// above, more than one restore can be live at once (server/restore.go),
	// so this is a map keyed by the server-generated session id rather than
	// a single pointer.
	restoreSessionMu sync.Mutex
	restoreSessions  map[string]*restoreSession
}

// Config returns the server configuration.
func (s *Server) Config() *config.Config {
	return s.config
}

// SetUserSessionCloser installs the callback that closes a dropped or
// disabled user's live sessions on this node (spec 8.8). The Service installs
// the driver listener's registry once it is listening.
func (s *Server) SetUserSessionCloser(fn func(username string) int) {
	s.userSessionCloser.Store(&fn)
}

// closeUserSessions closes a user's live sessions and returns the count. It
// is called after a committed drop, disable or password change, and when a
// pushed config applies one, never for grant/revoke.
func (s *Server) closeUserSessions(username string) int {
	closer := s.userSessionCloser.Load()
	if closer == nil || *closer == nil {
		return 0
	}
	return (*closer)(username)
}

// SeedRootUser creates a root admin user if credentials are provided.
func (s *Server) SeedRootUser(username, password string) error {
	return s.auth.SeedRootUser(username, password)
}

// Start listens on the configured address and begins serving.
//
// Deprecated: Use Service.Start() instead.
func (s *Server) Start() error {
	listenAddr := s.config.Server.Listen
	if listenAddr == "" {
		listenAddr = "127.0.0.1:4433"
	}

	var err error
	s.listener, err = net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listenAddr, err)
	}

	s.logger.Info("server listening",
		"address", listenAddr,
		"fingerprint", s.fingerprint,
		"node_id", s.config.Server.NodeID,
	)

	// Start metrics HTTP server if enabled.
	if err := StartMetricsServer(s.config, s.logger, s); err != nil {
		s.logger.Error("failed to start metrics server", "error", err)
	}

	// Start audit logger if enabled.
	if s.config.Audit.Enabled {
		al, err := NewAuditLogger(s.config.Audit.Output)
		if err != nil {
			s.logger.Error("failed to start audit logger", "error", err)
		} else {
			s.audit = al
			s.logger.Info("audit logger started", "output", s.config.Audit.Output)
		}
	}

	return nil
}

// Stop gracefully stops the server and closes all storage instances.
func (s *Server) Stop() error {
	s.logger.Info("shutting down server")

	// Stop replication first so in-flight batches are flushed before the
	// storage engines close underneath them.
	if s.cluster != nil {
		s.cluster.Stop()
	}

	// Cancel context to signal all goroutines, and wait for the reclaimer,
	// the capacity sampler, the TTL sweeper and any dump walk: they use the store, which
	// closes below. The cancel comes first because a dump walk blocked on a
	// document nobody reads leaves only when it sees it.
	s.cancel()
	s.reclaimWG.Wait()
	s.capacityWG.Wait()
	s.ttlWG.Wait()
	s.stopDumpWalks()

	// Close audit logger if running.
	if s.audit != nil {
		s.audit.Close()
		s.logger.Info("audit logger stopped")
	}

	// Close the WAL next: replication is stopped and storage is closed, so
	// nothing can append to it from here on.
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			s.logger.Error("failed to close WAL", "error", err)
		}
	}
	// The node store goes last: everything above may still have used it.
	if s.kv != nil {
		if err := s.kv.Close(); err != nil {
			s.logger.Error("failed to close the node store", "error", err)
		}
	}
	if err := s.blobLock.release(); err != nil {
		s.logger.Error("failed to release the blob directory", "error", err)
	}
	// Every writer has stopped: warn's mismatches since its last hourly line
	// are logged now rather than lost.
	s.flushFieldTypeWarnings()

	s.logger.Info("server stopped")
	return nil
}

// Run starts the server and blocks until the context is cancelled.
func (s *Server) Run() error {
	if err := s.Start(); err != nil {
		return err
	}

	s.logger.Info("hoardDB server running",
		"node_id", s.config.Server.NodeID,
		"fingerprint", s.fingerprint,
	)

	// Block until context is done.
	<-s.ctx.Done()

	return s.Stop()
}

// Auth returns the AuthManager for external use (e.g., CLI token generation).
func (s *Server) Auth() *AuthManager {
	return s.auth
}

// Audit returns the AuditLogger for logging security-relevant events.
// Returns nil if audit logging is disabled.
func (s *Server) Audit() *AuditLogger {
	return s.audit
}

// Cluster returns the cluster state, or nil when this node is running
// standalone (replication factor 1 or no peers configured).
func (s *Server) Cluster() *Cluster {
	return s.cluster
}

// WAL returns the write-ahead log, or nil when replication is disabled.
func (s *Server) WAL() *storage.WAL {
	return s.wal
}
