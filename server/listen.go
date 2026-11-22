// Package server provides the TCP-based server implementation for hoardDB.
//
// Service wraps the core Server business logic with the TCP+TLS transport
// (the tcp-protocol-spec record, the internode-tcp-build record; see
// the quic-removal-decision record for why this replaced QUIC).
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// legacyRingOn is whether the earlier hash ring runs: it does when replication_factor is above
// one or seeds are listed, and never in cluster mode, where hash_ring.seed_nodes drives the
// entry-ring cluster only and the earlier ring's log, gossip and cluster token are not built.
func legacyRingOn(cfg *config.Config) bool {
	return !cfg.Cluster.Enabled && (cfg.Server.ReplicationFactor > 1 || len(cfg.HashRing.SeedNodes) > 0)
}

// Service wraps the core Server and its TCP transport layer: a driver
// listener for clients and an internode listener for node-to-node traffic.
type Service struct {
	core           *Server
	handler        transport.RequestHandler
	logger         *slog.Logger
	config         *config.Config
	tlsCfg         *tls.Config
	limiter        *transport.ConnLimiter
	tcpListener    *transport.TCPListener
	internodeTCPLn *transport.TCPListener

	// driverListener and internodeListener, when non-nil, are listeners the
	// caller already bound and handed over. A test harness uses them so the
	// port stays bound from reservation to serve instead of being reserved and
	// released as a string. Production leaves them nil and Start
	// binds the configured addresses. Set before Start; Stop closes them.
	driverListener    net.Listener
	internodeListener net.Listener

	// ringNode is the entry-ring node Start opens on <data dir>/ring, nil until then
	// and when encryption at rest is on (see openRingNode).
	ringNode *wire.Node
	// bootWaitAborted ends awaitReplay's wait (AbortBootWait).
	bootWaitAborted atomic.Bool
	// ringJoin is true when cluster mode started the ring node to be joined rather than to
	// bootstrap, so the start-up line can say which.
	ringJoin bool
	// users follows the cluster's user table on a cluster member (ring user store spec 6.3);
	// stopUsers ends it before the ring node closes. Both nil on a node without a cluster.
	users     *userFollower
	stopUsers context.CancelFunc
	// clusterRoot is the first administrator a cluster member was given at start; the root task
	// (stopRoot, rootDone) creates it through the cluster once the ring node is open, and ends
	// before the node closes (ring_users_root.go). All nil on a node without a cluster.
	clusterRoot *clusterRoot
	stopRoot    context.CancelFunc
	rootDone    chan struct{}
	// operator is cmd/server's stderr, where a root password's fate is told (SetOperatorOutput).
	operator io.Writer
	// proposalBudget, when set, is how long a cluster command and a user change wait for a
	// leader on this node: a test option (ring user store step M2b.2), so a case without a quorum
	// costs seconds. Production leaves it zero: the node's default and userProposalDeadline.
	proposalBudget time.Duration
}

// userDeadline is how long a user change waits for the cluster on this node.
func (qs *Service) userDeadline() time.Duration {
	if qs.proposalBudget > 0 {
		return qs.proposalBudget
	}
	return userProposalDeadline
}

// NewService creates a new TCP-based server. It sets up TLS, auth, and
// registers the request handler that dispatches frames to the core Server
// methods.
func NewService(cfg *config.Config, logger *slog.Logger) (*Service, error) {
	if err := refuseBeforeWriting(cfg); err != nil {
		return nil, err
	}
	keys, err := OpenNodeKeys(cfg)
	if err != nil {
		return nil, err
	}
	return newService(cfg, logger, keys)
}

// NewServiceWithKeys is NewService for a caller that opened the node's keys
// itself, before it wrote anything that must be sealed: cmd/server opens them
// first so the cluster token it saves is sealed from its first write.
func NewServiceWithKeys(cfg *config.Config, logger *slog.Logger, keys *NodeKeys) (*Service, error) {
	if err := refuseBeforeWriting(cfg); err != nil {
		return nil, err
	}
	return newService(cfg, logger, keys)
}

// refuseBeforeWriting is NewService's checks that must pass before anything
// is written to the data directory.
func refuseBeforeWriting(cfg *config.Config) error {
	// Fail closed on a configuration that would leave internode identity
	// unverified, before any listener or key is created. Load()
	// checks this too; the second check covers callers that build a Config in
	// code.
	if err := cfg.Validate(); err != nil {
		return err
	}

	// Refuse a data directory in the per-bucket layout before anything is
	// written beside it: the refusal promises the old directory is as it was,
	// and the keys and tokens created below would break that promise.
	return RefuseLegacyLayout(cfg)
}

// NodeKeys is the keystore and node master a start opens before it reads or
// writes any sealed file, and how it keeps its credential files.
type NodeKeys struct {
	keystore    *Keystore
	nodeMaster  *NodeMaster
	unwrappedAt time.Time
	credentials CredentialKeys
}

// Credentials is how this start reads and writes its credential files.
func (k *NodeKeys) Credentials() CredentialKeys {
	return k.credentials
}

// OpenNodeKeys opens the keystore cfg names and loads the node master,
// creating it on a genuine first start. With keystore off both are nil and
// the credential files are plaintext. A keystore or node master failure is a
// *KeyUnwrapFailure, which the caller audits.
func OpenNodeKeys(cfg *config.Config) (*NodeKeys, error) {
	// This placement is "above all three locks" (trap 5): the command
	// keystore runs once at startup, never under bucket.mu.
	ks, err := OpenKeystore(cfg)
	if err != nil {
		return nil, &KeyUnwrapFailure{Class: "keystore", err: fmt.Errorf("opening keystore: %w", err)}
	}
	keys := &NodeKeys{keystore: ks}
	if ks != nil {
		keys.nodeMaster, err = LoadOrCreateNodeMaster(cfg.Storage.DataDir, ks)
		if err != nil {
			return nil, &KeyUnwrapFailure{Class: "node-master", err: fmt.Errorf("loading node master: %w", adviseRingMember(err, cfg.Cluster.Enabled))}
		}
		keys.unwrappedAt = time.Now().UTC()
	}
	keys.credentials, err = credentialKeysFor(cfg, keys.nodeMaster)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// newService builds the service on keys already opened.
func newService(cfg *config.Config, logger *slog.Logger, keys *NodeKeys) (*Service, error) {
	// The node master is the root of all node-level encryption; the keystore
	// and the node master are nil when keystore=off.
	ks, nm, keyUnwrappedAt := keys.keystore, keys.nodeMaster, keys.unwrappedAt

	// The ring's keys and refusals come first, read-only: whenever the ring directory holds
	// evidence of its mode, the ring's own text answers a mismatch rather than the earlier
	// store's (ring encryption at rest spec §5.5). The keys are created inside Node.open.
	ringKeys, err := loadRingAtRest(cfg, nm)
	if err != nil {
		return nil, err
	}

	// Decide the node's encryption state before anything reads a sealed file:
	// users.json is sealed under the node master, so a sealed node started
	// with encryption off must be refused by the store's own diagnosis here,
	// not by users.json's.
	if err := refuseSealedStoreWithEncryptionOff(cfg); err != nil {
		return nil, err
	}

	// Seal any credential file still in plaintext before the first read, then
	// load or generate the TLS identity, sealed when the node seals its
	// credentials.
	credentials := keys.credentials
	if err := recoverUnfinishedFirstStart(cfg.Storage.DataDir, logger); err != nil {
		return nil, err
	}
	if err := credentials.sealCredentialFiles(cfg.Storage.DataDir, logger); err != nil {
		return nil, err
	}
	if err := removeLeftoverLIFOSpills(cfg.Storage.DataDir, logger); err != nil {
		return nil, err
	}
	tlsCfg, fingerprint, err := credentials.loadNodeTLS(cfg.Storage.DataDir)
	if err != nil {
		return nil, fmt.Errorf("make server TLS config: %w", err)
	}

	// Create auth manager. The signing keyring and epoch counters live beside
	// the cluster token. A clustered node derives the key from the cluster
	// token so every node validates every other node's tokens; a standalone
	// node uses a per-node key file (spec 3.5). The decision follows cluster
	// mode, not the mere presence of cluster.token: no start of this release
	// writes that file, but an earlier build left one in every data
	// directory, standalone included, and that token is not what a
	// standalone node's user tokens should hang off.
	clusterToken, err := clusterAuthToken(cfg, credentials)
	if err != nil {
		return nil, err
	}
	keyring, err := buildKeyring(keyringRequest{
		dataDir:      cfg.Storage.DataDir,
		clusterToken: clusterToken,
		deploymentID: cfg.Auth.AuthDeploymentID,
		window:       time.Duration(cfg.Auth.AuthKeyringWindowSeconds) * time.Second,
		credentials:  credentials,
		logger:       logger,
	})
	if err != nil {
		return nil, fmt.Errorf("build auth signing keyring: %w", err)
	}
	usersPath := UsersFilePath(cfg.Storage.DataDir)
	if cfg.Cluster.Enabled {
		// A cluster member's users are the cluster's replicated table (ring user store spec 6.3):
		// it neither reads nor writes users.json, and refuses to start beside one (7.3).
		if err := refuseUsersFileInCluster(usersPath); err != nil {
			return nil, err
		}
		usersPath = ""
	}
	if clusterToken != "" && !cfg.Cluster.Enabled {
		if err := refuseStandaloneUsersInCluster(cfg.Storage.DataDir, keyring, nm, credentials); err != nil {
			return nil, err
		}
	}
	epochs, err := LoadEpochStore(cfg.Storage.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load auth epoch store: %w", err)
	}
	authMgr := NewAuthManager(AuthTokenOptions{
		TTL:          time.Duration(cfg.Auth.TokenTTLSeconds) * time.Second,
		MaxLifetime:  time.Duration(cfg.Auth.TokenMaxLifetimeHours) * time.Hour,
		Skew:         time.Duration(cfg.Auth.AuthSkewSeconds) * time.Second,
		Keyring:      keyring,
		Epochs:       epochs,
		NodeID:       cfg.Server.NodeID,
		DeploymentID: cfg.Auth.AuthDeploymentID,
		UsersPath:    usersPath,
		Master:       cfg.Server.NodeID,
		Argon2: Argon2Params{
			Memory:      cfg.Auth.Argon2Memory,
			Iterations:  cfg.Auth.Argon2Iterations,
			Parallelism: cfg.Auth.Argon2Parallelism,
			SaltLen:     cfg.Auth.Argon2SaltLen,
			HashLen:     cfg.Auth.Argon2HashLen,
		},
		MaxDerivations: cfg.Auth.AuthMaxDerivations,
		// config clamps the setting to 1..1024, so it fits an int32.
		PasswordMinLength: int32(cfg.Auth.PasswordMinLength),
		ClusterMember:     cfg.Cluster.Enabled,
	}, logger)

	// Set the encryption config on the AuthManager so it can save with the correct sealing.
	authMgr.nodeMaster = nm
	authMgr.mode = cfg.Encryption.Mode

	// Load the persisted users before anything can serve AUTH. A missing file
	// leaves the manager empty for BootstrapUsers to seed; a corrupt or
	// logically-wrong file is a startup error, never "start fresh".
	present, folded := false, 0
	if !cfg.Cluster.Enabled {
		present, folded, err = authMgr.LoadUsers(usersPath, nm, epochs)
		if err != nil {
			if clusterToken != "" && errors.Is(err, ErrUsersKeyMissing) {
				return nil, memberTokenChanged(cfg.Storage.DataDir, usersPath, err)
			}
			return nil, fmt.Errorf("load users file %s: %w", usersPath, err)
		}
	}
	if !present && legacyRingOn(cfg) {
		// A clustered node with no config must not serve logins from an empty
		// user list; it blocks AUTH until the master's first push arrives
		// (spec 5.3.4). A node that has a file serves immediately while it
		// reconciles, which is what keeps a cold start with the master
		// arriving last from deadlocking.
		authMgr.SetConfigBlocked(true)
	}
	if _, statErr := os.Stat(filepath.Join(cfg.Storage.DataDir, epochsFile)); statErr == nil {
		// The retired side file is left in place on purpose (spec 6.4.2):
		// deleting it turns a rollback into a silent security regression.
		logger.Warn("auth.token.epochs is retired and no longer written; its counters were folded into users.json",
			"path", filepath.Join(cfg.Storage.DataDir, epochsFile),
			"counters_folded", folded)
	}

	// Create the core server (no gRPC).
	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		config:      cfg,
		logger:      logger,
		keystore:    ks,
		nodeMaster:  nm,
		credentials: credentials,
		keyUnwrapAt: keyUnwrappedAt,
		ctx:         ctx,
		cancel:      cancel,
		fingerprint: fingerprint,
		auth:        authMgr,
		startTime:   time.Now(),
		knownNodes:  make(map[string]*NodeInfo),
		// Initialised here rather than lazily on first use: the lazy path
		// wrote this field from whichever goroutine got there first, which
		// raced with every other reader — a client create and a replicated
		// create on the same node are concurrent by design.
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		authReqSeen: make(map[string]time.Time),
		reclaimWake: make(chan struct{}, 1),
		ringAtRest:  ringKeys,
	}

	// A configured blob directory is checked and locked before the store
	// opens, so a start that would share it with another node, or put it
	// inside a directory the node owns, is refused before anything is written.
	// Every failure below releases it, so a retry in the same process can take
	// it again.
	blobLock, err := prepareBlobDir(cfg.Storage)
	if err != nil {
		return nil, err
	}
	s.blobLock = blobLock
	started := false
	defer func() {
		if started {
			return
		}
		if err := blobLock.release(); err != nil {
			logger.Error("releasing the blob directory after a failed start", "error", err)
		}
	}()

	// The node store holds the catalog and the bucket id allocator, so it
	// opens before anything that could create a bucket, and before LoadCatalog.
	// A require node reads an existing store's files before Badger opens it:
	// opening rotates the logs, which would destroy the evidence of the damage
	// and is itself a write (BUG-0612).
	if err := refuseUnsealedRequireStore(cfg); err != nil {
		return nil, err
	}
	kv, memtable, err := openNodeStore(cfg, nm)
	if err != nil {
		return nil, err
	}
	s.kv = kv
	s.memtable = memtable
	// Once, here: the store is open and nothing is being served or written yet.
	s.storeSeal = checkStoreSeal(filepath.Join(cfg.Storage.DataDir, nodeStoreDirName), cfg.Encryption.Mode)
	if s.storeSeal.problem != "" && cfg.Encryption.Mode == "require" {
		// Refused before the WAL, the blob keys or the catalog are touched:
		// require means never serve unsealed (BUG-0612).
		return nil, errors.Join(errRequireStoreNotSealed(s.storeSeal.problem), closeNodeStoreOnFailure(kv))
	}
	if s.storeSeal.problem != "" {
		logger.Warn("the store is configured to be sealed but its files do not show it", "problem", s.storeSeal.problem)
	}
	logger.Info(memtable.startLine(), "component", "storage")
	for _, warning := range memtable.warnings() {
		logger.Warn(warning, "component", "storage")
	}
	s.blobDiskSeparate, err = blobDiskIsSeparate(cfg.Storage.DataDir, s.blobRoot())
	if err != nil {
		return nil, errors.Join(err, closeNodeStoreOnFailure(kv))
	}
	lastID, err := seedBucketIDAllocator(kv, cfg.Storage.DataDir)
	if err != nil {
		return nil, errors.Join(err, closeNodeStoreOnFailure(kv))
	}
	s.bucketIDs.last = lastID

	// A sealed store seals blob payload files with the keys in keys/blobs.wrapped
	// (at-rest encryption spec §4.2.1 g). The registry is unwrapped once, here,
	// above every lock; a node whose store is plaintext holds none and writes
	// its payloads as they are, even when a keystore is configured.
	if modeSealsStore(cfg.Encryption.Mode) {
		s.blobKeys, err = LoadOrCreateBlobKeySource(cfg, nm, kv, logger)
		if err != nil {
			return nil, errors.Join(err, closeNodeStoreOnFailure(kv))
		}
	}

	// Replication needs a durable log: an entry is recorded, then applied,
	// then shipped, and a restart replays the log to rebuild state. Open it
	// only when replication can actually happen, so a single node pays
	// nothing for it.
	if legacyRingOn(cfg) {
		walDir := filepath.Join(cfg.Storage.DataDir, "wal")
		// The epoch registry is unwrapped exactly once, here, above the WAL
		// open: it is sealed under the node master, and the only other place
		// that could do it is a per-write path (trap 2). A nil node master —
		// keystore=off — gives a nil key source, which is today's plaintext
		// format byte for byte. Nothing is created on a node with no WAL,
		// because this whole block only runs when replication can happen.
		walKeys, err := LoadOrCreateWALKeySource(cfg.Storage.DataDir, nm)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("load WAL epoch registry in %s: %w", walDir, err), closeNodeStoreOnFailure(kv))
		}
		walMode := walPreallocateResolver(cfg, walDir)
		w, err := storage.OpenWALWithOptions(walDir, walKeys, storage.WALOptions{Preallocate: walMode.On, SegmentBytes: walSegmentBytes})
		if err != nil {
			return nil, errors.Join(fmt.Errorf("open WAL in %s: %w", walDir, err), closeNodeStoreOnFailure(kv))
		}
		s.wal = w
		s.walMode = walMode
		walMode.logWALMode(logger)
		if s.walMetrics, err = newWALMetrics(w); err != nil {
			return nil, errors.Join(err, w.Close(), closeNodeStoreOnFailure(kv))
		}
		logger.Info("write-ahead log open", "dir", walDir, "sealed", walKeys != nil)
		// Every open starts a format 2 segment, so this is where the format
		// changes, before anything is written.
		noteSegmentFormat2(walDir, logger)
		// A repaired log means the previous run stopped mid-entry: a crash or a
		// full disk. Say so loudly — the entries that were dropped were never
		// acknowledged, but an operator still needs to know the log was cut.
		if rec := w.Recovery(); rec != nil {
			logger.Warn("WAL ended in a partial entry and was repaired by discarding the tail",
				"segment", rec.Segment,
				"recovered_to_offset", rec.RecoveredTo,
				"dropped_bytes", rec.DroppedBytes,
				"reason", rec.Reason)
		}
		// Before anything can append: a log found new under kept data starts
		// below the marks the data holds (BUG-0609).
		if err := s.passMarksUnderANewLog(); err != nil {
			return nil, errors.Join(err, w.Close(), closeNodeStoreOnFailure(kv))
		}
	} else {
		logWALPreallocateNoEffect(cfg, logger)
	}

	if warning := s.unsafeAckWarning(); warning != "" {
		logger.Warn(warning, "component", "storage")
	}

	// The capacity probes read the store and the WAL, which are open by now,
	// and nothing before NewCluster can reach the join guard that reads them.
	s.probes.Store(defaultCapacityProbes(s))
	// Built here, before NewCluster, so that its failure (it registers
	// collectors) never has a cluster to stop. It starts at the end.
	capacity, err := newCapacitySampler(s)
	if err != nil {
		failure := []error{fmt.Errorf("building the capacity sampler: %w", err)}
		if s.wal != nil {
			failure = append(failure, s.wal.Close())
		}
		return nil, errors.Join(append(failure, closeNodeStoreOnFailure(kv))...)
	}

	// The legacy replication state file's fate is a property of the data
	// directory, not of the cluster (BUG-0216): every start imports its
	// positions into the store and deletes it, standalone included, and an
	// unreadable one stops the start with the file's path and the mv remedy.
	if err := importLegacyReplicationState(cfg.Storage.DataDir, kv, logger, nil); err != nil {
		if s.wal != nil {
			_ = s.wal.Close()
		}
		return nil, errors.Join(err, closeNodeStoreOnFailure(kv))
	}

	// Build cluster state (ring placement + peer connections). A single node
	// with no seeds stays standalone and never routes.
	if legacyRingOn(cfg) {
		cl, err := NewCluster(s, cfg, logger)
		if err != nil {
			if s.wal != nil {
				_ = s.wal.Close()
			}
			return nil, errors.Join(err, closeNodeStoreOnFailure(kv))
		}
		s.cluster = cl
	}

	// A node that is not the auth master trails it for config and is subject
	// to the staleness bound (spec 5.3.5). A standalone node is its own
	// master and never stale. The replication loop keeps this current if
	// mastership moves.
	authMgr.SetSelfID(s.recordedMasterID())
	authMgr.SetFollower(!s.isAuthMaster())

	handler := InstrumentHandler(NewHandler(s))

	// Connection caps (spec 5.6): one shared limiter for both accept loops,
	// with a reserved peer budget so a client flood cannot starve
	// replication. Clients draw MaxConns; connections that AUTH as a node
	// (NodeUser) draw MaxPeerConns.
	limiter := transport.NewConnLimiter(cfg.Server.MaxConns, cfg.Server.MaxPeerConns)

	// The reclaimer starts last, once nothing below can fail: it resumes every
	// pending reclaim record from its cursor, and loads nothing to do so. The
	// capacity sampler follows it, whatever Metrics.Enabled says: its log and
	// status are the channels that need no setup.
	s.startReclaimer()
	s.startCapacitySampler(capacity)
	s.startTTLSweeper()
	started = true

	return &Service{
		core:    s,
		handler: handler,
		logger:  logger,
		config:  cfg,
		tlsCfg:  tlsCfg,
		limiter: limiter,
	}, nil
}

// closeNodeStoreOnFailure releases the node store when NewService fails after
// opening it, so a retry in the same process can take the directory's lock.
func closeNodeStoreOnFailure(kv storage.KV) error {
	if err := kv.Close(); err != nil {
		return fmt.Errorf("closing the node store after a failed start: %w", err)
	}
	return nil
}

// SeedRootUser creates a root admin user if credentials are provided. A cluster member refuses
// it: its first administrator comes from the root task (BootstrapUsersFrom).
func (qs *Service) SeedRootUser(username, password string) error {
	return qs.core.SeedRootUser(username, password)
}

// BootstrapUsers applies the spec 7.1 root-bootstrap precedence against the
// already-loaded user store. It is the only path that seeds root.
func (qs *Service) BootstrapUsers(username, password string, allowSeed bool) error {
	return qs.BootstrapUsersFrom(username, password, allowSeed, "HOARDDB_ROOT_PASSWORD")
}

// BootstrapUsersFrom is BootstrapUsers for a password read from setting, so a
// refusal names that setting (BUG-0504). On a cluster member it arms the root task, which
// creates the cluster's first administrator through the cluster (ring user store spec 6.5).
func (qs *Service) BootstrapUsersFrom(username, password string, allowSeed bool, setting string) error {
	if qs.config.Cluster.Enabled {
		return qs.armClusterRoot(username, password, allowSeed, setting)
	}
	return qs.core.auth.BootstrapUserFrom(username, password, allowSeed, setting)
}

// memberTokenChanged is the refusal of a cluster member whose users.json was
// sealed under a key derived from another cluster token: the cause first,
// then the fix (AGENTS.md rule 3). A rotation cannot be carried out on a
// running cluster in this release, so the only fix that keeps the users is to
// put the cluster's token back.
func memberTokenChanged(dataDir, usersPath string, cause error) error {
	tokenPath := clusterTokenCredential.path(dataDir)
	return fmt.Errorf("cannot open %s: this cluster member's cluster token changed. A member seals users.json with a key derived from the cluster token "+
		"(and HOARDDB_AUTH_DEPLOYMENT_ID, if set), and the token in %s is not the one it was sealed under. "+
		"Put back the token the cluster runs on (%s), then start this node again. "+
		"If the cluster's token really must change, this release can only rebuild the user store: stop every node, move each users.json aside, "+
		"start every node with a cluster.token holding the new token and HOARDDB_ROOT_PASSWORD, and create every other user again: %w",
		usersPath, tokenPath, clusterTokenSource, cause)
}

// clusterAuthToken returns the cluster token used for the auth signing key,
// or "" for a standalone node. It is shared by NewService and ResetPassword
// so the pepper and the token key derive from the same material.
func clusterAuthToken(cfg *config.Config, credentials CredentialKeys) (string, error) {
	if !legacyRingOn(cfg) {
		return "", nil
	}
	ct, err := credentials.LoadClusterToken(cfg.Storage.DataDir)
	if err == nil {
		return ct.Token, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	return "", fmt.Errorf("load cluster token for auth signing key: %w", err)
}

// CheckServerStopped refuses an offline operation (a password reset, a key
// export or import) while the driver port is bound. The bind is the
// observable form of "a server is using this data dir": a running server
// writes to the files these commands change or read.
func CheckServerStopped(cfg *config.Config) error {
	addr := cfg.TCP.Listen
	if addr == "" {
		addr = "127.0.0.1:7433"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("a server appears to be running on %s (%v); stop it first: this command works on the data directory of a stopped server, and a running server writes to the same files", addr, err)
	}
	return ln.Close()
}

// Auth returns the AuthManager for external use (e.g., CLI token generation).
func (qs *Service) Auth() *AuthManager {
	return qs.core.auth
}

// Audit returns the AuditLogger for logging security-relevant events.
func (qs *Service) Audit() *AuditLogger {
	return qs.core.audit
}

// Config returns the server configuration.
func (qs *Service) Config() *config.Config {
	return qs.core.config
}

// auditHook builds the SessionDeps.Audit callback shared by both TCP
// listeners: a login the per-IP ledger refused is recorded with its source IP
// and the transport's result text, which says why (counted failures, or
// logins still being checked).
func (qs *Service) auditHook() func(ip string, result string) {
	return func(ip string, result string) {
		qs.core.audit.Log(AuditEntry{
			Event:    EventAuthFailure,
			SourceIP: ip,
			User:     "(rate-limited)",
			Result:   result,
		})
	}
}

// Start begins listening for TCP connections and starts supporting services.
func (qs *Service) Start() (err error) {
	listenAddr := qs.config.Server.Listen
	if listenAddr == "" {
		listenAddr = "127.0.0.1:4433"
	}

	// Rebuild state from the write-ahead log before accepting connections, so
	// no client can observe a half-replayed node.
	if err := qs.core.replayWAL(context.Background()); err != nil {
		return fmt.Errorf("replay write-ahead log: %w", err)
	}
	// Before any listener: no write of this node's own is logged until a
	// majority of its peers has said where its log stands (start_hold.go).
	if qs.core.cluster != nil {
		qs.core.cluster.startHold.arm()
	}

	// The ring node opens after replay and before any listener, so the first client frame
	// already finds the ring opcodes. A start that fails below closes it again.
	driverHandler, err := qs.openRingNode()
	if err != nil {
		return err
	}
	// Until the node's log is applied the driver listener answers UNAVAILABLE (replay_gate.go).
	driverHandler = qs.gateDriverHandler(driverHandler)
	defer func() {
		if err != nil {
			err = errors.Join(err, qs.closeRingNode())
		}
	}()
	// Audit logger must exist before either TCP listener starts accepting:
	// both listeners' SessionDeps.Audit callback calls qs.core.audit.Log, and
	// the auth-rate-limiter can fire on the very first connection. Creating
	// it after Start() had already opened both listeners left a startup
	// window — exactly the window an attacker probing auth would target —
	// where a rate-limited refusal was silently dropped instead of audited.
	if qs.config.Audit.Enabled {
		al, err := NewAuditLogger(qs.config.Audit.Output)
		if err != nil {
			qs.logger.Error("failed to start audit logger", "error", err)
		} else {
			qs.core.audit = al
			qs.logger.Info("audit logger started", "output", qs.config.Audit.Output)
			qs.core.auditKeyUnwrap()
		}
	}
	// The cluster's first administrator, when this member was given a root password (spec 6.5),
	// once the audit logger it writes to exists.
	qs.startRootTask()

	// TCP driver listener (tcp-protocol-spec.md): the only client-facing
	// transport, so unconditional — there is no other transport left to
	// serve clients with if this does not start. Same certificate, same
	// ALPN, same frame codec and auth gate as the internode listener. A
	// failure to bind must fail startup — a missing listener is a visible
	// log line, not a silent gap.
	tcpCfg := transport.TCPConfig{
		Listen:         qs.config.TCP.Listen,
		IdleTimeout:    time.Duration(qs.config.TCP.IdleTimeoutSeconds) * time.Second,
		FrameTimeout:   time.Duration(qs.config.TCP.FrameTimeoutSeconds) * time.Second,
		MaxConnsPerIP:  qs.config.TCP.MaxConnsPerIP,
		AuthMaxFails:   qs.config.TCP.AuthMaxFails,
		PipelineFrames: qs.config.TCP.PipelineFrames,
		PipelineBytes:  qs.config.TCP.PipelineBytes,
	}
	driverLn := qs.driverListener
	if driverLn == nil {
		bound, err := net.Listen("tcp", tcpCfg.Listen)
		if err != nil {
			return fmt.Errorf("start TCP driver listener on %s: %w", tcpCfg.Listen, err)
		}
		driverLn = bound
	}
	tcpListener := transport.ServeTCP(tls.NewListener(driverLn, qs.tlsCfg), qs.limiter, tcpCfg, driverHandler, &transport.SessionDeps{
		Logger:  qs.logger,
		Context: qs.core.ctx,
		Audit:   qs.auditHook(),
	})
	qs.tcpListener = tcpListener
	// A drop or disable must close that user's live sessions on this node,
	// and the driver listener is where users authenticate (spec 8.8).
	qs.core.SetUserSessionCloser(tcpListener.CloseSessionsFor)
	qs.logger.Info("TCP driver listener listening",
		"address", driverLn.Addr().String(),
		"fingerprint", qs.core.fingerprint,
		"node_id", qs.config.Server.NodeID,
		"alpn", transport.ALPNFor(transport.ProtocolMajor),
	)

	// Internode TCP listener (the internode-tcp-build record): node-to-node traffic
	// over a dedicated TCP port. Unconditional — there is no other transport
	// left to reach a peer with. The listen address is the driver's
	// internode-derived address (Server.Listen, udp-port+1 historically,
	// now just a plain TCP port) or an explicit override; peers derive the
	// same address from the ring member's Addr. It shares the limiter's peer
	// budget and the driver listener's machinery, with PeerMode lifting the
	// node-to-node refusal.
	internodeAddr := qs.config.TCP.InternodeListen
	internodeBound := qs.internodeListener
	if internodeBound != nil {
		internodeAddr = internodeBound.Addr().String()
	} else {
		if internodeAddr == "" {
			derived, err := transport.InternodePeerAddr(listenAddr)
			if err != nil {
				return fmt.Errorf("derive internode TCP listen address: %w", err)
			}
			internodeAddr = derived
		}
		bound, err := net.Listen("tcp", internodeAddr)
		if err != nil {
			return fmt.Errorf("start internode TCP listener on %s: %w", internodeAddr, err)
		}
		internodeBound = bound
	}
	internodeLn := transport.ServeTCP(tls.NewListener(internodeBound, qs.tlsCfg), qs.limiter,
		transport.TCPConfig{
			Listen:         internodeAddr,
			IdleTimeout:    time.Duration(qs.config.TCP.IdleTimeoutSeconds) * time.Second,
			FrameTimeout:   time.Duration(qs.config.TCP.FrameTimeoutSeconds) * time.Second,
			MaxConnsPerIP:  qs.config.TCP.MaxConnsPerIP,
			AuthMaxFails:   qs.config.TCP.AuthMaxFails,
			PipelineFrames: qs.config.TCP.PipelineFrames,
			PipelineBytes:  qs.config.TCP.PipelineBytes,
			PeerMode:       true,
		},
		qs.handler,
		&transport.SessionDeps{
			Logger:  qs.logger,
			Context: qs.core.ctx,
			Audit:   qs.auditHook(),
		})
	qs.internodeTCPLn = internodeLn
	qs.logger.Info("internode TCP listener listening",
		"address", internodeAddr,
		"alpn", transport.ALPNFor(transport.ProtocolMajor),
	)

	// Begin replicating: connect to peers and verify that every node agrees
	// on membership. Peers that are down are retried in the background, so a
	// node can start while the rest of the cluster is still coming up.
	if qs.core.cluster != nil {
		qs.core.cluster.Start()
	}

	// Start metrics HTTP server if enabled.
	if err := StartMetricsServer(qs.config, qs.logger, qs.core); err != nil {
		qs.logger.Error("failed to start metrics server", "error", err)
	}

	// Last, so a divergence the replay records is already known to the caller that checks
	// Diverged right after Start returns.
	qs.awaitReplay()
	return nil
}

// Stop gracefully stops the server and closes all storage instances.
func (qs *Service) Stop() error {
	qs.logger.Info("shutting down server")

	// Close the TCP listeners first: stop accepting new driver and
	// internode connections, then drain the ones already admitted (and any
	// in-flight dispatches) before storage shutdown below runs.
	if qs.tcpListener != nil {
		if err := qs.tcpListener.Close(); err != nil {
			qs.logger.Error("stop TCP driver listener", "error", err)
		}
		qs.tcpListener = nil
	}
	if qs.internodeTCPLn != nil {
		if err := qs.internodeTCPLn.Close(); err != nil {
			qs.logger.Error("stop internode TCP listener", "error", err)
		}
		qs.internodeTCPLn = nil
	}
	// No ring frame is in flight once the listeners have drained.
	if err := qs.closeRingNode(); err != nil {
		qs.logger.Error("stop the entry-ring node", "error", err)
	}

	// Stop replication first: in-flight batches are flushed to peers while
	// storage is still open, and peer connections close before the transport
	// goes away.
	if qs.core.cluster != nil {
		qs.core.cluster.Stop()
	}

	// Cancel context to signal all goroutines, and wait for the reclaimer,
	// the capacity sampler, the TTL sweeper and any dump walk: they use the store, which
	// closes below. The cancel comes first because a dump walk blocked on a
	// document nobody reads leaves only when it sees it.
	qs.core.cancel()
	qs.core.reclaimWG.Wait()
	qs.core.capacityWG.Wait()
	qs.core.ttlWG.Wait()
	qs.core.stopDumpWalks()

	// Close audit logger if running.
	if qs.core.audit != nil {
		qs.core.audit.Close()
		qs.logger.Info("audit logger stopped")
	}

	// Close every bucket, so a writer still in flight finds it closed. There
	// is no engine to release: every engine is a handle's, built per
	// operation on the node's store.
	if qs.core.dataStore != nil {
		qs.core.closeAllRecords()
	}

	// Close the WAL next: replication has stopped and storage is closed, so
	// nothing can append to it from here on.
	if qs.core.wal != nil {
		if err := qs.core.wal.Close(); err != nil {
			qs.logger.Error("failed to close WAL", "error", err)
		}
	}
	// The node store goes last: every loop above may still have used it.
	if qs.core.kv != nil {
		if err := qs.core.kv.Close(); err != nil {
			qs.logger.Error("failed to close the node store", "error", err)
		}
	}
	if err := qs.core.blobLock.release(); err != nil {
		qs.logger.Error("failed to release the blob directory", "error", err)
	}
	// Every writer has stopped: warn's mismatches since its last hourly line
	// are logged now rather than lost.
	qs.core.flushFieldTypeWarnings()

	qs.logger.Info("server stopped")
	return nil
}

// Run starts the server and blocks until the context is cancelled.
func (qs *Service) Run() error {
	if err := qs.Start(); err != nil {
		return err
	}

	qs.logger.Info("hoardDB server running",
		"node_id", qs.config.Server.NodeID,
		"fingerprint", qs.core.fingerprint,
	)

	// Block until context is done.
	<-qs.core.ctx.Done()

	return qs.Stop()
}
