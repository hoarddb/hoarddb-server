package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Config holds the full hoardDB configuration.
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	TCP         TCPConfig         `yaml:"tcp"`
	Query       QueryConfig       `yaml:"query"`
	Cluster     ClusterConfig     `yaml:"cluster"`
	Replication ReplicationConfig `yaml:"replication"`
	TLS         TLSConfig         `yaml:"tls"`
	Auth        AuthConfig        `yaml:"auth"`
	Logging     LoggingConfig     `yaml:"logging"`
	Metrics     MetricsConfig     `yaml:"metrics"`
	Audit       AuditConfig       `yaml:"audit"`
	Debug       DebugConfig       `yaml:"debug"`
	Storage     StorageConfig     `yaml:"storage"`
	HashRing    HashRingConfig    `yaml:"hash_ring"`
	Encryption  EncryptionConfig  `yaml:"encryption"`
}

// EncryptionConfig holds at-rest encryption configuration.
type EncryptionConfig struct {
	// Keystore is the key storage method: "off", "keyfile", "command", "systemd-creds"
	Keystore string `yaml:"keystore" env:"HOARDDB_ENCRYPTION_KEYSTORE"`
	// KeyFile is the path to a file holding the 32-byte KEK (for keystore=keyfile)
	KeyFile string `yaml:"key_file" env:"HOARDDB_ENCRYPTION_KEY_FILE"`
	// KeyCommand is the argv to execute to get the KEK (for keystore=command)
	// This is a []string in YAML, not a shell string.
	KeyCommand []string `yaml:"key_command" env:"-"`
	// Mode is the encryption mode: "off", "on" or "require". "on" and
	// "require" both seal this node's ring store and ring log under keys in
	// <data_dir>/ring/keys, wrapped by the node master. "require" also runs a
	// structural check of the node's own ring store before it opens and
	// refuses to start when the check fails; "on" reports the finding in
	// /health and warns. Neither checks other nodes: every node of a cluster
	// must use the same setting, and the cluster does not detect one that
	// differs. A node's mode is fixed when its ring store is created.
	Mode string `yaml:"mode" env:"HOARDDB_ENCRYPTION_MODE"`
}

type ServerConfig struct {
	// Listen is the ring identity base: the address peers are told to reach
	// this node on, from which the internode port (port+1) is derived. It
	// defaults to loopback (SEC-1), so a node in a multi-machine cluster must
	// set it to an address its peers can dial (Validate refuses otherwise).
	Listen string `yaml:"listen" env:"HOARDDB_LISTEN"`
	NodeID string `yaml:"node_id" env:"HOARDDB_NODE_ID"`
	// NodeIDDefaulted is set by Load when no node id was configured and NodeID
	// holds the hostname. Cloned hosts share a hostname, so a defaulted id is
	// never matched against the seed list by id alone (BUG-0598).
	NodeIDDefaulted   bool   `yaml:"-" env:"-"`
	MaxConns          int    `yaml:"max_conns" env:"HOARDDB_MAX_CONNS"`
	RootUser          string `yaml:"-" env:"HOARDDB_ROOT_USER"`
	RootPassword      string `yaml:"-" env:"HOARDDB_ROOT_PASSWORD"`
	ReplicationFactor int    `yaml:"replication_factor" env:"HOARDDB_REPLICATION_FACTOR"`
	// WriteConcern is the durability level for replicated writes:
	// "one", "majority" or "all". Empty means the default for the
	// replication factor (majority when RF>1, one otherwise).
	WriteConcern string `yaml:"write_concern" env:"HOARDDB_WRITE_CONCERN"`
	// WALRetentionSeconds is how often the WAL retention pass runs. Each pass
	// prunes segments every replica has confirmed and every local store has
	// been synced past, so a lagging replica always stays repairable from the
	// log. Zero disables the loop entirely.
	WALRetentionSeconds int `yaml:"wal_retention_seconds" env:"HOARDDB_WAL_RETENTION_INTERVAL"`
	// TTLSweepSeconds is how often the TTL sweeper runs a pass over the
	// buckets that declare a TTL index. Expiry is background work: a document
	// can outlive its expiry by up to one interval, plus the time a backlog
	// takes to drain. Zero disables the sweeper.
	TTLSweepSeconds int `yaml:"ttl_sweep_seconds" env:"HOARDDB_TTL_SWEEP_SECONDS"`
	// TTLSweepBudgetSeconds bounds one pass. A pass that ends on the budget
	// with expired documents left starts the next pass at once rather than
	// waiting for the next interval. Zero or less uses the default.
	TTLSweepBudgetSeconds int `yaml:"ttl_sweep_budget_seconds" env:"HOARDDB_TTL_SWEEP_BUDGET_SECONDS"`
	// DumpPinCeilingSeconds bounds how long a FIFO/LIFO bucket's compaction
	// pin may be held for one dump (dump-restore-spec.md §16.3) before it is
	// force-released and the dump fails loudly. Zero or unset falls back to
	// the 5-minute default (server/dump.go's defaultDumpPinCeiling); a
	// continuously-popped queue should be dumped in a maintenance window
	// with this raised instead.
	DumpPinCeilingSeconds int `yaml:"dump_pin_ceiling_seconds" env:"HOARDDB_DUMP_PIN_CEILING_SECONDS"`
	// DiskHighWaterPct is the used-disk watermark (0-100) at or above which
	// this node copies in no bucket it does not already hold, and votes
	// against adding a cluster member: an mmap-backed write into a full disk
	// crashes the node (SIGBUS) no matter how cleanly the error path handles
	// it. Zero disables the guard.
	DiskHighWaterPct int `yaml:"disk_high_water_pct" env:"HOARDDB_DISK_HIGH_WATER"`
	// RestoreDiskHeadroomMultiplier bounds how much free disk space a restore
	// requires before it starts writing one bucket (dump-restore-spec.md
	// §9.1/§11/§19 A-15): the check is `free bytes >= multiplier * that
	// bucket's raw dump bytes`, evaluated per bucket, before that bucket
	// starts. The archived design used 1.2x (documents only); this defaults
	// to 2.0x because a restore is a full write path — WAL, LSM compaction,
	// index rebuild — not just the document bytes themselves. Zero or unset
	// falls back to the 2.0 default (server/restore.go's
	// defaultRestoreDiskHeadroomMultiplier).
	RestoreDiskHeadroomMultiplier float64 `yaml:"restore_disk_headroom_multiplier" env:"HOARDDB_RESTORE_DISK_HEADROOM_MULTIPLIER"`
	// AutoRebalance is whether a membership change immediately starts moving
	// data so every bucket's ring owner and replicas hold a copy. When false,
	// the operator drives rebalancing explicitly (`migrate` per bucket or a
	// future sweep verb). The migration is idempotent, so the flag only trades
	// promptness for controllability.
	AutoRebalance bool `yaml:"auto_rebalance" env:"HOARDDB_AUTO_REBALANCE"`
	// MaxPeerConns is the connection budget reserved for node-to-node
	// (cluster) connections. It is separate from MaxConns so a client flood
	// cannot starve replication: peer acceptance draws this budget and is
	// never refused because the client budget is full (tcp-protocol-spec.md
	// section 5 item 6).
	MaxPeerConns int `yaml:"max_peer_conns" env:"HOARDDB_MAX_PEER_CONNS"`
}

// QueryConfig bounds what one query may hold in memory while it builds its
// answer (query-pipelining spec §3.2).
type QueryConfig struct {
	// MaxResultBytes is the result ceiling: the encoded bytes of the
	// elements one query may retain — page documents, sort keys, distinct
	// values — before it is refused with RESULT_TOO_LARGE. Clamped to
	// 1..24 MiB on load, so that a page at the ceiling still fits the 32 MiB
	// frame with room for its envelope.
	MaxResultBytes int `yaml:"max_result_bytes" env:"HOARDDB_QUERY_MAX_RESULT_BYTES"`
	// Concurrency is how many queries (SCAN, SEARCH, BTREE_RANGE, COUNT,
	// DISTINCT) execute at once on this node, and separately how many
	// forwarded results it holds for its clients at once. A query past it
	// waits; it is never refused. 0 means 2 × GOMAXPROCS. Clamped to 4..256
	// on load.
	Concurrency int `yaml:"concurrency" env:"HOARDDB_QUERY_CONCURRENCY"`
	// PageLimitDefault is the page size a list or scan uses when the request
	// names none, and PageLimitMax is the largest page a request may ask for
	// (queries §3). Documents per page.
	PageLimitDefault int `yaml:"page_limit_default" env:"HOARDDB_QUERY_PAGE_LIMIT_DEFAULT"`
	PageLimitMax     int `yaml:"page_limit_max" env:"HOARDDB_QUERY_PAGE_LIMIT_MAX"`
	// NearestMaxElements is the most elements one nearest call scans in one store field; a field
	// holding more is refused before any element is read. Clamped to 1..10000000 on load.
	NearestMaxElements int `yaml:"nearest_max_elements" env:"HOARDDB_QUERY_NEAREST_MAX_ELEMENTS"`
}

// TCPConfig configures the TCP driver listener (tcp-protocol-spec.md). It
// serves language drivers over plain TLS sockets, using the shared frame
// codec, auth gate and error semantics on a dedicated port.
type TCPConfig struct {
	// Listen is the TCP+TLS listen address, deliberately outside the
	// 4433-443x range cluster nodes use so a driver address is never
	// confused with a node address. It defaults to loopback (SEC-1): set it
	// to 0.0.0.0:7433 to accept drivers from other machines.
	Listen string `yaml:"listen" env:"HOARDDB_TCP_LISTEN"`
	// Advertise is the host:port a cluster member tells drivers to connect to,
	// when that is not tcp.listen (a wildcard or loopback listen address, or an
	// address behind NAT). Empty uses tcp.listen, unless that is a wildcard or
	// loopback address, which is never published. It must be on the same host
	// as server.listen: the cluster refuses any other host.
	Advertise string `yaml:"advertise" env:"HOARDDB_TCP_ADVERTISE"`
	// IdleTimeoutSeconds closes a connection that has sent no frame for this
	// long. It is jittered ±10% per connection so a pool that goes idle
	// together does not come back together as one handshake storm.
	IdleTimeoutSeconds int `yaml:"idle_timeout_seconds" env:"HOARDDB_TCP_IDLE_TIMEOUT"`
	// FrameTimeoutSeconds bounds a single frame read (header + body). It
	// catches a client drip-feeding one byte at a time, which would
	// otherwise hold a goroutine and TLS buffers open indefinitely.
	FrameTimeoutSeconds int `yaml:"frame_timeout_seconds" env:"HOARDDB_TCP_FRAME_TIMEOUT"`
	// MaxConnsPerIP caps concurrent connections from one source address.
	// Zero disables the per-IP cap.
	MaxConnsPerIP int `yaml:"max_conns_per_ip" env:"HOARDDB_TCP_MAX_CONNS_PER_IP"`
	// AuthMaxFails is the consecutive-failure count after which an IP is
	// refused with a fixed delay, and the threshold at which it is refused
	// outright for 60s. Zero disables auth attempt limiting.
	AuthMaxFails int `yaml:"auth_max_fails" env:"HOARDDB_TCP_AUTH_MAX_FAILS"`
	// PipelineFrames bounds the read-ahead pipelining window in frames (1 =
	// strict lockstep, the CLI-compatible mode). Clamped to 1..64 on load.
	PipelineFrames int `yaml:"pipeline_frames" env:"HOARDDB_TCP_PIPELINE_FRAMES"`
	// PipelineBytes bounds the read-ahead pipelining window in bytes of
	// in-flight frame payload. Clamped to 1..16 MiB on load.
	PipelineBytes int `yaml:"pipeline_bytes" env:"HOARDDB_TCP_PIPELINE_BYTES"`
	// InternodeListen overrides the internode TCP listen address. Empty
	// derives it from Server.Listen (port+1: 4433 → 4434), which is also
	// how peers derive the address of a member from its ring Addr. The
	// internode listener is unconditional — it is the only way to reach a
	// peer (see the quic-removal-decision record).
	InternodeListen string `yaml:"internode_listen" env:"HOARDDB_TCP_INTERNODE_LISTEN"`
}

type TLSConfig struct {
	Mode string `yaml:"mode" env:"HOARDDB_TLS_MODE"`
	Cert string `yaml:"cert" env:"HOARDDB_TLS_CERT"`
	Key  string `yaml:"key" env:"HOARDDB_TLS_KEY"`
	CA   string `yaml:"ca" env:"HOARDDB_TLS_CA"`
}

type AuthConfig struct {
	Argon2Memory      uint32 `yaml:"argon2_memory"`
	Argon2Iterations  uint32 `yaml:"argon2_iterations"`
	Argon2Parallelism uint8  `yaml:"argon2_parallelism"`
	Argon2SaltLen     int    `yaml:"argon2_salt_length"`
	Argon2HashLen     int    `yaml:"argon2_hash_length"`

	// TokenExpiry is deprecated: use TokenMaxLifetimeHours. It is YAML-only
	// (there is no HOARDDB_TOKEN_EXPIRY_HOURS), and 0 means "not set" — a
	// non-zero value is honoured only when TokenMaxLifetimeHours is absent,
	// because defaults are applied before the YAML is unmarshalled and
	// otherwise "unset" and "explicitly 24" are indistinguishable (spec 7).
	TokenExpiry int `yaml:"token_expiry_hours"`
	// TokenTTLSeconds is the per-token lifetime; a token is refreshed by use.
	TokenTTLSeconds int `yaml:"token_ttl_seconds" env:"HOARDDB_TOKEN_TTL"`
	// TokenMaxLifetimeHours is the absolute deadline carried across refresh.
	TokenMaxLifetimeHours int `yaml:"token_max_lifetime_hours" env:"HOARDDB_TOKEN_MAX_LIFETIME"`
	// AuthSkewSeconds is the clock-skew leeway on exp/iat, both directions.
	AuthSkewSeconds int `yaml:"auth_skew_seconds" env:"HOARDDB_AUTH_SKEW"`
	// AuthMaxDerivations bounds concurrent Argon2 derivations cluster-wide.
	AuthMaxDerivations int `yaml:"auth_max_derivations" env:"HOARDDB_AUTH_MAX_DERIVATIONS"`
	// AuthKeyringWindowSeconds is how long the previous signing key keeps
	// validating after a rotation; 0 disables the overlap.
	AuthKeyringWindowSeconds int `yaml:"auth_keyring_window_seconds" env:"HOARDDB_AUTH_KEYRING_WINDOW"`
	// AuthDeploymentID is the domain-separation suffix in the HKDF info:
	// "auth-token-v1:" + id. Empty means the literal prefix only.
	AuthDeploymentID string `yaml:"auth_deployment_id" env:"HOARDDB_AUTH_DEPLOYMENT_ID"`
	// AuthConfigMaxStalenessSeconds bounds how far behind the master a node
	// may fall before it refuses new password AUTHs and token refreshes
	// (spec 5.3.5).
	AuthConfigMaxStalenessSeconds int `yaml:"auth_config_max_staleness_seconds" env:"HOARDDB_AUTH_CONFIG_MAX_STALENESS"`
	// AuthMasterRetrySeconds is the backoff between replication retries to a
	// lagging member (spec 5.3.3).
	AuthMasterRetrySeconds int `yaml:"auth_master_retry_seconds" env:"HOARDDB_AUTH_MASTER_RETRY"`
	// PasswordMinLength is the minimum accepted password length, named in the
	// error when refused (spec 7).
	PasswordMinLength int `yaml:"password_min_length" env:"HOARDDB_PASSWORD_MIN_LENGTH"`
	// AuthBootstrapRoot controls whether HOARDDB_ROOT_USER/_PASSWORD may seed
	// an empty store (spec 7.1).
	AuthBootstrapRoot bool `yaml:"auth_bootstrap_root" env:"HOARDDB_AUTH_BOOTSTRAP_ROOT"`
	// RequireRootPassword makes a first start with no supplied root password
	// (HOARDDB_ROOT_PASSWORD or _FILE) a refusal instead of a prompt or a
	// generated password. Off by default so a bare hoarddb-server still
	// starts; the recommended production setting.
	RequireRootPassword bool `yaml:"require_root_password" env:"HOARDDB_REQUIRE_ROOT_PASSWORD"`
}

type LoggingConfig struct {
	Level  string `yaml:"level" env:"HOARDDB_LOG_LEVEL"`
	Format string `yaml:"format" env:"HOARDDB_LOG_FORMAT"`
	Output string `yaml:"output" env:"HOARDDB_LOG_OUTPUT"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" env:"HOARDDB_METRICS_ENABLED"`
	Listen  string `yaml:"listen" env:"HOARDDB_METRICS_LISTEN"`
}

type AuditConfig struct {
	Enabled bool   `yaml:"enabled" env:"HOARDDB_AUDIT_ENABLED"`
	Output  string `yaml:"output" env:"HOARDDB_AUDIT_OUTPUT"`
}

type DebugConfig struct {
	PProfEnabled bool `yaml:"pprof_enabled" env:"HOARDDB_PPROF_ENABLED"`
	// PProfBlockRateNS and PProfMutexFraction make the block and mutex profiles of the pprof
	// endpoint non-empty: the runtime samples nothing until told to. They apply only when
	// PProfEnabled is set; 0 turns that sampling off.
	PProfBlockRateNS   int `yaml:"pprof_block_rate_ns" env:"HOARDDB_PPROF_BLOCK_RATE_NS"`
	PProfMutexFraction int `yaml:"pprof_mutex_fraction" env:"HOARDDB_PPROF_MUTEX_FRACTION"`
}

type StorageConfig struct {
	DataDir string `yaml:"data_dir" env:"HOARDDB_DATA_DIR"`

	// BlobDir is where blob buckets keep their payload files, one tree per
	// node. Empty means <data dir>/blobs, the path before this setting
	// existed. Set it to put large payloads on their own volume. It belongs
	// to one node: the server refuses to start on a directory another
	// running node holds, because each node's reclaimer deletes payloads its
	// own store does not own.
	BlobDir string `yaml:"blob_dir" env:"HOARDDB_BLOB_DIR"`

	// MaxBlobBytes is the largest blob, in bytes, this node accepts from a
	// client. It defaults to DefaultMaxBlobBytes and may be raised to at most
	// MaxBlobBytesCeiling; a value outside 1..MaxBlobBytesCeiling stops the
	// server from starting rather than being clamped, because a silently
	// lowered cap would refuse writes the operator meant to allow.
	MaxBlobBytes int `yaml:"max_blob_bytes" env:"HOARDDB_MAX_BLOB_BYTES"`

	// MaxDocumentBytes is the largest single document, in encoded bytes, this
	// node accepts from a client (GAP-36). It defaults to
	// DefaultMaxDocumentBytes and may be set between MinDocumentBytes and
	// MaxDocumentBytesCeiling; a value outside that range stops the server
	// from starting, as MaxBlobBytes does. It counts one document's own
	// bytes: not its bucket, and not the elements of its store fields, which
	// are stored separately.
	MaxDocumentBytes int `yaml:"max_document_bytes" env:"HOARDDB_MAX_DOCUMENT_BYTES"`

	// BadgerBlockCacheMB, BadgerIndexCacheMB and BadgerNumCompactors tune
	// the node's one BadgerDB instance, which every bucket shares
	// (storage.OpenBadgerKV): they are node-wide and do not multiply by
	// bucket count. Zero uses storage's own defaults (128 MB / 64 MB / 4
	// compactors). Raising the block cache keeps more hot data resident —
	// a 10M-record bucket outgrowing the 128 MB default measured a 12%
	// read-throughput drop (bench/RESULTS.md, on the per-bucket layout,
	// where that cache was the bucket's alone).
	BadgerBlockCacheMB  int `yaml:"badger_block_cache_mb" env:"HOARDDB_BADGER_BLOCK_CACHE_MB"`
	BadgerIndexCacheMB  int `yaml:"badger_index_cache_mb" env:"HOARDDB_BADGER_INDEX_CACHE_MB"`
	BadgerNumCompactors int `yaml:"badger_num_compactors" env:"HOARDDB_BADGER_NUM_COMPACTORS"`

	// BadgerTableSync is how the store makes each new table file (a memtable
	// flush or a compaction output) durable: "msync", the default, is
	// Badger's own single sync per table, which on ext4 waits for a journal
	// commit and stalls writes while it does; "stepwise" writes it back in
	// 4 MiB steps and waits once, and leaves its metadata to the manifest
	// sync that follows. Stepwise is crash-safe only on a filesystem that
	// commits its journal in order (ext4 or XFS with a journal), which the
	// server cannot detect, so it is opt-in. See TableSyncStepwise.
	BadgerTableSync string `yaml:"badger_table_sync" env:"HOARDDB_BADGER_TABLE_SYNC"`

	// WALPreallocate is whether a node with a write-ahead log fills each log
	// segment with zeros before it uses it, so a flush of the log writes data
	// only: "auto", the default, which the server resolves to on for ext2/3/4
	// and off elsewhere, "on" for every filesystem, or "off". The entry-ring
	// node's log and the legacy node's log share this one key. It is read where
	// the log is opened, so a change needs a restart. The segment format does not depend
	// on it (every segment is format 2); the key only turns the zero fill on.
	WALPreallocate string `yaml:"wal_preallocate" env:"HOARDDB_WAL_PREALLOCATE"`
	// WALPreallocateSet is whether the file or the environment set the key,
	// an explicit "off" included, so the legacy node says the key has no effect
	// on it only when the operator asked for the key. Load fills it.
	WALPreallocateSet bool `yaml:"-" env:"-"`

	// MemtableMB is the node store's memtable size in MiB. The store's largest
	// atomic write batch is 15% of it, so it also decides how many documents
	// one insert-many or one ingest batch commits all-or-nothing: 64 MiB gives
	// about 10 MB, the 32 MiB floor about 5 MB. A bigger memtable stalls writes
	// less and holds up to three of them in memory; it is fixed at start, and a
	// node that crashed at a larger size cannot be opened at a smaller one until
	// it has been started once at the old size and stopped cleanly
	// (storage.guardMemtableShrink). Zero means not set: the server then runs at
	// the size the store chose at its first start (HostMemtableMB), which an
	// explicit value overrides for as long as it is set. The range is
	// MinMemtableMB..MaxMemtableMB (Validate).
	MemtableMB int `yaml:"memtable_mb" env:"HOARDDB_MEMTABLE_MB"`

	// ReclaimKeysPerSecond is the budget of the node's one background
	// reclaimer, which deletes the space of dropped buckets: stored versions
	// visited plus payload files unlinked, per second, node-wide. Zero means
	// 10,000, at which a 100-key bucket is reclaimed in about 10 ms of budget
	// and a 2×10⁷-key one in about 33 minutes. Raise it when
	// hoarddb_reclaim_oldest_pending_seconds keeps growing.
	ReclaimKeysPerSecond int `yaml:"reclaim_keys_per_second" env:"HOARDDB_RECLAIM_KEYS_PER_SECOND"`

	// UnsafeAckBeforeSync lets a node answer a write it did not log to a
	// write-ahead log before its store is synced to disk: every write of a
	// single node, and of a node with a log while it is alone in its ring.
	// Off, the default, such a write is synced before it is answered, one
	// sync shared among concurrent writes (owner ruling 43). On, a
	// power cut or a kernel crash can lose writes the node acknowledged; a
	// process crash cannot. It is for benchmarks. A write the node logs is
	// unaffected: the log is synced before the write is applied either way.
	UnsafeAckBeforeSync bool `yaml:"unsafe_ack_before_sync" env:"HOARDDB_UNSAFE_ACK_BEFORE_SYNC"`
}

type HashRingConfig struct {
	SeedNodes []string `yaml:"seed_nodes" env:"HOARDDB_SEED_NODES"`
	// SeedFingerprints is the trust anchor for seed dials: each entry is a
	// canonical SHA256:<hex> fingerprint (trusted for any seed) or a mapped
	// host:port=SHA256:<hex> (trusted only for that address). It is required
	// whenever SeedNodes is set — a seed dial without an anchor would accept
	// any certificate.
	SeedFingerprints []string `yaml:"seed_fingerprints" env:"HOARDDB_SEED_FINGERPRINTS"`
}

// The values of storage.badger_table_sync. storage.BadgerTuning.TableSync
// takes the same strings.
const (
	// TableSyncMsync is Badger's single blocking msync per new table, the
	// default.
	TableSyncMsync = "msync"
	// TableSyncStepwise writes a new table back in steps and waits once; the
	// file's metadata is made durable by the store's manifest sync, which on
	// ext4 or XFS with a journal commits the journal in order. Opt-in.
	TableSyncStepwise = "stepwise"
)

// The values of storage.wal_preallocate.
const (
	// WALPreallocateOff leaves log segments unfilled, as before the default
	// became auto.
	WALPreallocateOff = "off"
	// WALPreallocateOn zero-fills every segment on every filesystem.
	WALPreallocateOn = "on"
	// WALPreallocateAuto lets the server pick by filesystem at start; the default.
	WALPreallocateAuto = "auto"
)

// DefaultMaxBlobBytes is the blob size cap a node starts with (GAP-35).
const DefaultMaxBlobBytes = 16 << 20 // 16 MiB

// BlobFrameReserve is the room kept under transport.MaxFrameLength for
// everything around a blob's bytes: the frame's opcode and request id, the
// BSON envelope with its database, bucket, key and metadata, and the
// replication wrapper that carries the same payload to a replica (a second
// copy of the key, the bucket name, the token and the entry fields). A blob
// larger than MaxFrameLength minus this cannot be written, replicated or read
// back through a forwarding node in one frame.
const BlobFrameReserve = 64 << 10 // 64 KiB

// MaxBlobBytesCeiling is the largest MaxBlobBytes a node accepts: the largest
// blob that fits one frame with its envelope and its replication wrapper. It
// is 32 MiB less BlobFrameReserve, not 32 MiB, because the frame is 32 MiB
// including everything around the bytes.
const MaxBlobBytesCeiling = transport.MaxFrameLength - BlobFrameReserve

// DefaultMaxDocumentBytes is the per-document size limit a node starts with
// (GAP-36).
const DefaultMaxDocumentBytes = 24 << 20 // 24 MiB

// MaxDocumentBytesCeiling is the largest MaxDocumentBytes a node accepts. A
// document at the ceiling must fit a write frame, the replication frame that
// carries it to a replica, and the dump window
// (server/dump.go:maxDumpDocumentBytes) with their envelopes; 30 MiB leaves
// 2 MiB of the 32 MiB frame for all of them, which
// TestThirtyMiBDocumentReplicatesDumpsAndRestores measures.
const MaxDocumentBytesCeiling = 30 << 20 // 30 MiB

// MinDocumentBytes is the smallest MaxDocumentBytes a node accepts. Below it
// a limit would refuse the ordinary catalog-sized documents an application
// writes, which is a misconfiguration and not a policy.
const MinDocumentBytes = 4 << 10 // 4 KiB

// The Argon2 work-factor bounds. The credential validator in server clamps a
// client-submitted work factor against the configured default by
// Argon2WorkFactorScale and against the absolute floors and ceilings below;
// Config.Validate refuses a configured default outside the same range at load,
// so a mistyped auth.argon2_* key is reported at start-up rather than at the
// first user seed (BUG-0228). None of the three keys has an environment
// variable: they are YAML-only (cluster-settings-spec.md).
const (
	// MinArgon2Memory is the smallest auth.argon2_memory accepted, in KiB,
	// and the credential validator's memory floor. The default is 64 MiB.
	MinArgon2Memory uint32 = 65536
	// MaxArgon2Memory is the largest auth.argon2_memory accepted, in KiB.
	// Each password hash allocates this much, so the ceiling is a resource
	// bound on one derivation, not a credential-format rule; 1 GiB is 16×
	// the default and leaves room for the work factors BUG-0326 supports.
	MaxArgon2Memory uint32 = 1 << 20 // 1 GiB
	// MinArgon2Iterations is the smallest auth.argon2_iterations accepted,
	// and the credential validator's iteration floor. The default is 3.
	MinArgon2Iterations uint32 = 3
	// MaxArgon2Iterations is the largest auth.argon2_iterations accepted.
	// Each pass repeats the memory cost, so the ceiling bounds work per
	// derivation.
	MaxArgon2Iterations uint32 = 16
	// MinArgon2Parallelism is the smallest auth.argon2_parallelism accepted.
	MinArgon2Parallelism uint8 = 1
	// MaxArgon2Parallelism is the largest auth.argon2_parallelism accepted.
	// It is a constant, not runtime.GOMAXPROCS(0), because a host-dependent
	// bound made one users.json valid on one node and invalid on another:
	// the server refused its own default-4 root user on a 1-CPU host, at
	// seed, at restart and on a pushed config (BUG-0226).
	//
	// Lanes are not a cost multiplier, which is why a fixed bound is safe.
	// In golang.org/x/crypto/argon2, deriveKey allocates
	// make([]block, memory) once (initBlocks) and processBlocks splits those
	// blocks into `threads` lanes, filling each block once per pass; so
	// memory is the Memory field and CPU time is about iterations × memory
	// whatever the lane count. What lanes do add is one goroutine per lane
	// per segment, so the bound keeps a client from fanning one derivation
	// out to 255 goroutines (IDKey takes threads as a uint8 and sets no
	// ceiling of its own below that). 16 is four times the default.
	MaxArgon2Parallelism uint8 = 16
	// Argon2WorkFactorScale is how far above the configured
	// auth.argon2_memory or auth.argon2_iterations the credential validator
	// accepts a client-submitted value: the upper cap on submitted work
	// factors, in one place.
	Argon2WorkFactorScale uint32 = 2
)

// defaultArgon2HashLen is the only auth.argon2_hash_length the credential
// validator accepts (server/auth.go:defaultArgon2HashLen, checked by
// server/users.go:validateCredentialClamp). The hash length is part of the
// credential format, so Validate refuses any other value at load (BUG-0739)
// rather than letting the node start and then refuse every credential it makes.
const defaultArgon2HashLen = 32

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Debug: DebugConfig{PProfBlockRateNS: 1000000, PProfMutexFraction: 100},
		Server: ServerConfig{
			Listen:                        "127.0.0.1:4433",
			MaxConns:                      1000,
			MaxPeerConns:                  64,
			ReplicationFactor:             1,
			WALRetentionSeconds:           60,
			TTLSweepSeconds:               60,
			TTLSweepBudgetSeconds:         5,
			DumpPinCeilingSeconds:         300,
			DiskHighWaterPct:              85,
			RestoreDiskHeadroomMultiplier: 2.0,
			AutoRebalance:                 true,
			// The root user defaults to admin so a no-argument start still
			// has an identity to bootstrap; the password is generated on
			// first start when none is configured (see cmd/server).
			RootUser: "admin",
		},
		TCP: TCPConfig{
			Listen:              "127.0.0.1:7433",
			IdleTimeoutSeconds:  300,
			FrameTimeoutSeconds: 30,
			MaxConnsPerIP:       64,
			AuthMaxFails:        20,
			PipelineFrames:      4,
			PipelineBytes:       1 << 20,
		},
		Query:       QueryConfig{MaxResultBytes: 16 << 20, PageLimitDefault: 100, PageLimitMax: 10000, NearestMaxElements: 100_000},
		Cluster:     DefaultClusterConfig(),
		Replication: DefaultReplicationConfig(),
		TLS:         TLSConfig{Mode: "auto"},
		Auth: AuthConfig{
			Argon2Memory:      65536,
			Argon2Iterations:  3,
			Argon2Parallelism: 4,
			Argon2SaltLen:     16,
			Argon2HashLen:     defaultArgon2HashLen,
			// TokenExpiry is deprecated; 0 means unset (spec 7).
			TokenExpiry:                   0,
			TokenTTLSeconds:               900,
			TokenMaxLifetimeHours:         12,
			AuthSkewSeconds:               60,
			AuthMaxDerivations:            runtime.GOMAXPROCS(0),
			AuthKeyringWindowSeconds:      1800,
			AuthConfigMaxStalenessSeconds: 300,
			AuthMasterRetrySeconds:        5,
			PasswordMinLength:             12,
			AuthBootstrapRoot:             true,
		},
		Logging: LoggingConfig{Level: "info", Format: "json", Output: "stdout"},
		Metrics: MetricsConfig{Listen: "127.0.0.1:9090"},
		Audit:   AuditConfig{Output: "/var/log/hoarddb/audit.jsonl"},
		Storage: StorageConfig{DataDir: "./data", MaxBlobBytes: DefaultMaxBlobBytes, MaxDocumentBytes: DefaultMaxDocumentBytes, BadgerTableSync: TableSyncMsync, WALPreallocate: WALPreallocateAuto},
		Encryption: EncryptionConfig{
			Keystore: "off",
			Mode:     "off",
		},
	}
}

// Load builds the configuration in three layers, lowest first: the built-in
// defaults, the YAML file at yamlPath (skipped when empty), then the HOARDDB_*
// environment variables. A file that cannot be read, is not valid YAML, or
// holds a key Config does not define is an error.
func Load(yamlPath string) (*Config, error) {
	return load(yamlPath, true)
}

// LoadAllowingOldReplicatedMode is Load without the refusal of the old replicated mode. The tests of
// that mode use it, and so does the CLI's connection (cli/connect.go): the refusal belongs to a
// server start, not to a client that reads the same environment. A start never uses it.
// DELETE with the old mode: the load(path, refuseOldMode) split (fold load back into Load) and
// the two tests in server/ that call this, TestJoinRequiresSeedFingerprints and
// TestANodeNeverTakesAListedIDAtAnotherAddress; the CLI then calls Load again.
func LoadAllowingOldReplicatedMode(yamlPath string) (*Config, error) {
	return load(yamlPath, false)
}

func load(yamlPath string, refuseOldMode bool) (*Config, error) {
	cfg := DefaultConfig()
	var yamlData []byte
	if yamlPath != "" {
		data, err := readConfigFile(yamlPath)
		if err != nil {
			return nil, err
		}
		if err := decodeConfigFile(yamlPath, data, cfg); err != nil {
			return nil, err
		}
		yamlData = data
	}
	if err := refuseRemovedClusterEnv(); err != nil {
		return nil, err
	}
	fromFile := *cfg
	if err := applyEnvOverrides(cfg); err != nil {
		return nil, fmt.Errorf("environment: %w", err)
	}
	cfg.Storage.WALPreallocateSet = walPreallocateInFile(yamlData) || os.Getenv("HOARDDB_WAL_PREALLOCATE") != ""
	warnEnvOverridesFile(&fromFile, cfg, yamlData)
	if err := applySecretFileOverrides(cfg); err != nil {
		return nil, err
	}
	resolveAuthConfig(cfg, yamlData)
	if cfg.Server.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.Server.NodeID = hostname
		cfg.Server.NodeIDDefaulted = true
	}
	if env := os.Getenv("HOARDDB_SEED_NODES"); env != "" {
		cfg.HashRing.SeedNodes = strings.Split(env, ",")
	}
	if env := os.Getenv("HOARDDB_SEED_FINGERPRINTS"); env != "" {
		cfg.HashRing.SeedFingerprints = strings.Split(env, ",")
	}
	// The start-up cluster check (E11) runs on every start, not only when the
	// node has peers. The defaults pass it, so a single node with no cluster
	// settings is unaffected, and the join path reuses the same function
	// instead of a copy.
	if err := cfg.ValidateTimings(); err != nil {
		return nil, err
	}
	if err := cfg.ValidateDedupeWindow(); err != nil {
		return nil, err
	}
	if err := cfg.ValidateReplWindow(); err != nil {
		return nil, err
	}
	if err := cfg.ValidateRetentionCheck(); err != nil {
		return nil, err
	}
	// Before Validate: a seed list without cluster mode would otherwise be reported as a
	// missing fingerprint, when the fix is to use the ring.
	if refuseOldMode {
		if err := cfg.RefuseOldReplicatedMode(); err != nil {
			return nil, err
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// walPreallocateInFile is whether the YAML file names storage.wal_preallocate.
// Defaults are applied before the file is decoded, so an explicit "off" is
// indistinguishable from the default without a shadow decode. A file that
// cannot be decoded here already failed in decodeConfigFile.
func walPreallocateInFile(yamlData []byte) bool {
	var raw struct {
		Storage struct {
			WALPreallocate *string `yaml:"wal_preallocate"`
		} `yaml:"storage"`
	}
	if len(yamlData) == 0 || yaml.Unmarshal(yamlData, &raw) != nil {
		return false
	}
	return raw.Storage.WALPreallocate != nil
}

// Validate fails closed on a configuration the server must not start with.
// It checks the storage ranges (blob cap, document limit, memtable size), the
// encryption settings, and the one combination whose absence silently turns a
// protection off: seed nodes configured without a trust anchor. Without a
// fingerprint there is nothing to verify a seed's certificate against, so any
// host could impersonate a member of the cluster. Single-node configurations — no seed
// nodes — are unaffected by that last check.
func (c *Config) Validate() error {
	if err := validateWriteConcern(c.Server.WriteConcern); err != nil {
		return err
	}
	if err := c.validateClusterMode(); err != nil {
		return err
	}

	if c.Storage.MaxBlobBytes < 1 || c.Storage.MaxBlobBytes > MaxBlobBytesCeiling {
		return fmt.Errorf(`storage.max_blob_bytes (HOARDDB_MAX_BLOB_BYTES) is %d, but it must be between 1 and %d bytes (%.1f MiB), the largest blob that fits in one frame
       set it inside that range, or remove it to use the default of %d MiB
       for larger files, keep the file on your own storage and store a pointer document (see "Large file stores" in the docs)`,
			c.Storage.MaxBlobBytes, MaxBlobBytesCeiling, float64(MaxBlobBytesCeiling)/(1<<20), DefaultMaxBlobBytes>>20)
	}

	if c.Storage.MaxDocumentBytes < MinDocumentBytes || c.Storage.MaxDocumentBytes > MaxDocumentBytesCeiling {
		return fmt.Errorf(`storage.max_document_bytes (HOARDDB_MAX_DOCUMENT_BYTES) is %d, but it must be between %d and %d bytes (%d KiB and %d MiB)
       set it inside that range, or remove it to use the default of %d MiB
       a value larger than that belongs in a blob bucket, or on your own storage behind a pointer document (see "Large file stores" in the docs)`,
			c.Storage.MaxDocumentBytes, MinDocumentBytes, MaxDocumentBytesCeiling, MinDocumentBytes>>10, MaxDocumentBytesCeiling>>20, DefaultMaxDocumentBytes>>20)
	}

	if err := validateAdvertise(c.TCP.Advertise); err != nil {
		return err
	}

	if err := validateMemtableMB(c.Storage.MemtableMB); err != nil {
		return err
	}

	if c.Storage.BadgerTableSync != TableSyncStepwise && c.Storage.BadgerTableSync != TableSyncMsync {
		return fmt.Errorf(`storage.badger_table_sync (HOARDDB_BADGER_TABLE_SYNC) is %q, but it must be %q or %q
       remove it to use the default, %q; set %q only on ext4 or XFS with a journal, where it lowers write tail latency`,
			c.Storage.BadgerTableSync, TableSyncMsync, TableSyncStepwise, TableSyncMsync, TableSyncStepwise)
	}

	if w := c.Storage.WALPreallocate; w != WALPreallocateOff && w != WALPreallocateOn && w != WALPreallocateAuto {
		return fmt.Errorf(`storage.wal_preallocate (HOARDDB_WAL_PREALLOCATE) is %q, but it must be %q, %q or %q
       remove it to use the default, %q; it is not a switch: %q preallocates on every filesystem, %q on ext2/3/4 only`,
			w, WALPreallocateOff, WALPreallocateOn, WALPreallocateAuto, WALPreallocateAuto, WALPreallocateOn, WALPreallocateAuto)
	}

	if err := validateArgon2Settings(c.Auth); err != nil {
		return err
	}

	if err := validateTLS(c.TLS); err != nil {
		return err
	}

	// Validate encryption config
	validKeystores := map[string]bool{"off": true, "keyfile": true, "command": true, "systemd-creds": true}
	if !validKeystores[c.Encryption.Keystore] {
		return fmt.Errorf("encryption.keystore: unknown value %q (accepted: off, keyfile, command, systemd-creds)", c.Encryption.Keystore)
	}

	validModes := map[string]bool{"off": true, "on": true, "require": true}
	if !validModes[c.Encryption.Mode] {
		return fmt.Errorf("encryption.mode: unknown value %q (accepted: off, on, require)", c.Encryption.Mode)
	}

	// mode=on or mode=require with keystore=off is meaningless
	if (c.Encryption.Mode == "on" || c.Encryption.Mode == "require") && c.Encryption.Keystore == "off" {
		return fmt.Errorf("encryption.mode=%s requires encryption.keystore to be set (got \"off\")\n       set encryption.keystore to keyfile, command, or systemd-creds", c.Encryption.Mode)
	}

	// keystore=keyfile requires key_file to be set
	if c.Encryption.Keystore == "keyfile" && c.Encryption.KeyFile == "" {
		return fmt.Errorf("encryption.keystore=keyfile requires encryption.key_file to be set")
	}

	// keystore=command requires key_command to be set
	if c.Encryption.Keystore == "command" && len(c.Encryption.KeyCommand) == 0 {
		return fmt.Errorf("encryption.keystore=command requires encryption.key_command to be set")
	}

	// Validate seed nodes and fingerprints
	if len(c.HashRing.SeedNodes) == 0 {
		return nil
	}
	if len(c.HashRing.SeedFingerprints) == 0 {
		return fmt.Errorf(`cluster seeds are configured but seed_fingerprints is empty
       set HOARDDB_SEED_FINGERPRINTS to the SHA256 fingerprint(s) of your
       seed node(s); each seed prints its fingerprint at startup:
         grep fingerprint <seed startup log>
       this is required on every join: without it a peer's certificate
       cannot be verified and any server could impersonate the cluster`)
	}
	for _, entry := range c.HashRing.SeedFingerprints {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		value := entry
		if _, after, ok := strings.Cut(entry, "="); ok {
			value = after
		}
		if _, err := transport.ParseFingerprint(value); err != nil {
			return fmt.Errorf("seed_fingerprints: %w", err)
		}
	}
	if err := c.validateClusterReachability(); err != nil {
		return err
	}
	return c.validateListenMatchesSeeds()
}

// validateArgon2Settings refuses a configured work factor the credential
// validator would not accept. It runs at load, so a mistyped auth.argon2_*
// key is refused before the store is opened, naming the key, the range and
// the default (AGENTS.md rule 3) instead of failing at the first user seed
// (BUG-0228). The default in the message comes from DefaultConfig so the
// stated number cannot drift from the value the server actually starts with.
func validateArgon2Settings(auth AuthConfig) error {
	defaults := DefaultConfig().Auth
	if auth.Argon2Memory < MinArgon2Memory || auth.Argon2Memory > MaxArgon2Memory {
		return fmt.Errorf(`auth.argon2_memory is %d KiB, but the permitted range is [%d, %d] KiB (%d MiB to %d MiB)
       set it inside that range, or remove it to use the default of %d MiB
       each password hash allocates this much, so the ceiling bounds one derivation`,
			auth.Argon2Memory, MinArgon2Memory, MaxArgon2Memory, MinArgon2Memory>>10, MaxArgon2Memory>>10, defaults.Argon2Memory>>10)
	}
	if auth.Argon2Iterations < MinArgon2Iterations || auth.Argon2Iterations > MaxArgon2Iterations {
		return fmt.Errorf(`auth.argon2_iterations is %d, but the permitted range is [%d, %d]
       set it inside that range, or remove it to use the default of %d
       each pass repeats the memory cost, so the ceiling bounds work per derivation`,
			auth.Argon2Iterations, MinArgon2Iterations, MaxArgon2Iterations, defaults.Argon2Iterations)
	}
	if auth.Argon2Parallelism < MinArgon2Parallelism || auth.Argon2Parallelism > MaxArgon2Parallelism {
		return fmt.Errorf(`auth.argon2_parallelism is %d, but the permitted range is [%d, %d]
       set it inside that range, or remove it to use the default of %d
       lanes split the memory cost rather than multiply it (BUG-0226)`,
			auth.Argon2Parallelism, MinArgon2Parallelism, MaxArgon2Parallelism, defaults.Argon2Parallelism)
	}
	if auth.Argon2HashLen != defaultArgon2HashLen {
		return fmt.Errorf(`auth.argon2_hash_length is %d, but this build accepts only %d
       remove the key to use the default of %d, or set it to %d
       the hash length is part of the credential format and cannot be changed`,
			auth.Argon2HashLen, defaultArgon2HashLen, defaultArgon2HashLen, defaultArgon2HashLen)
	}
	return nil
}

// resolveAuthConfig applies the defaults that depend on other auth values and
// the token_expiry_hours deprecation. It runs after env overrides, because
// the keyring-window default depends on the effective token TTL and the
// deprecated key's precedence depends on whether the new key was set at all.
func resolveAuthConfig(cfg *Config, yamlData []byte) {
	// Defaults are applied before the YAML is unmarshalled, so presence must
	// be recovered from a shadow decode — otherwise an omitted
	// token_max_lifetime_hours is indistinguishable from an explicit 12 and
	// the deprecated key could never be honoured.
	var raw struct {
		Auth struct {
			TokenExpiryHours         *int `yaml:"token_expiry_hours"`
			TokenMaxLifetimeHours    *int `yaml:"token_max_lifetime_hours"`
			AuthKeyringWindowSeconds *int `yaml:"auth_keyring_window_seconds"`
		} `yaml:"auth"`
	}
	if len(yamlData) > 0 {
		if err := yaml.Unmarshal(yamlData, &raw); err != nil {
			slog.Warn("could not inspect auth config for deprecated keys", "error", err)
		}
	}

	deprecatedSet := raw.Auth.TokenExpiryHours != nil && *raw.Auth.TokenExpiryHours != 0
	maxLifetimeSet := raw.Auth.TokenMaxLifetimeHours != nil || os.Getenv("HOARDDB_TOKEN_MAX_LIFETIME") != ""
	keyringWindowSet := raw.Auth.AuthKeyringWindowSeconds != nil || os.Getenv("HOARDDB_AUTH_KEYRING_WINDOW") != ""

	if deprecatedSet {
		if maxLifetimeSet {
			slog.Warn("token_expiry_hours is deprecated and ignored because token_max_lifetime_hours is set",
				"token_expiry_hours", cfg.Auth.TokenExpiry,
				"token_max_lifetime_hours", cfg.Auth.TokenMaxLifetimeHours)
		} else {
			slog.Warn("token_expiry_hours is deprecated; use token_max_lifetime_hours (the deprecated key is honoured until it is removed)",
				"token_expiry_hours", cfg.Auth.TokenExpiry,
				"replacement", "token_max_lifetime_hours")
			cfg.Auth.TokenMaxLifetimeHours = cfg.Auth.TokenExpiry
		}
	}

	if !keyringWindowSet {
		window := 2 * time.Duration(cfg.Auth.TokenTTLSeconds) * time.Second
		if window < 15*time.Minute {
			window = 15 * time.Minute
		}
		cfg.Auth.AuthKeyringWindowSeconds = int(window.Seconds())
	}

	clampAuthConfig(cfg)
}

// clampAuthConfig bounds auth values whose zero or negative form is
// dangerous, warning and naming each clamped value so a misconfiguration is
// visible rather than silently accepted.
func clampAuthConfig(cfg *Config) {
	clampConfigInt("HOARDDB_TOKEN_TTL", &cfg.Auth.TokenTTLSeconds, 1, 86400)
	clampConfigInt("token_max_lifetime_hours", &cfg.Auth.TokenMaxLifetimeHours, 1, 8760)
	clampConfigInt("HOARDDB_AUTH_SKEW", &cfg.Auth.AuthSkewSeconds, 0, 300)
	clampConfigInt("HOARDDB_AUTH_MAX_DERIVATIONS", &cfg.Auth.AuthMaxDerivations, 1, 1024)
	clampConfigInt("auth_keyring_window_seconds", &cfg.Auth.AuthKeyringWindowSeconds, 0, 86400)
	clampConfigInt("HOARDDB_AUTH_CONFIG_MAX_STALENESS", &cfg.Auth.AuthConfigMaxStalenessSeconds, 1, 86400)
	clampConfigInt("HOARDDB_AUTH_MASTER_RETRY", &cfg.Auth.AuthMasterRetrySeconds, 1, 3600)
	clampConfigInt("HOARDDB_PASSWORD_MIN_LENGTH", &cfg.Auth.PasswordMinLength, 1, 1024)
}

// applyEnvOverrides applies the HOARDDB_* variables over the file and the
// defaults. A typed variable that does not parse is an error rather than a
// silent fallback to the default: for HOARDDB_REPLICATION_FACTOR
// the fallback is the harm. Every bad variable is collected so one start shows
// them all.
func applyEnvOverrides(cfg *Config) error {
	var badVariables []error
	setString := func(ptr *string, env string) {
		if v := os.Getenv(env); v != "" {
			*ptr = v
		}
	}
	setInt := func(ptr *int, env string) {
		v := os.Getenv(env)
		if v == "" {
			return
		}
		n, err := parseEnvInt(env, v, int64(*ptr), strconv.IntSize)
		if err != nil {
			badVariables = append(badVariables, err)
			return
		}
		*ptr = int(n)
	}
	setBool := func(ptr *bool, env string) {
		v := os.Getenv(env)
		if v == "" {
			return
		}
		b, err := parseEnvBool(env, v, *ptr)
		if err != nil {
			badVariables = append(badVariables, err)
			return
		}
		*ptr = b
	}
	setFloat := func(ptr *float64, env string) {
		v := os.Getenv(env)
		if v == "" {
			return
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			badVariables = append(badVariables, fmt.Errorf(
				"%s=%q is not a number. Set it to a number such as 2.0, or unset it to keep the current value of %g",
				env, v, *ptr))
			return
		}
		*ptr = f
	}
	setString(&cfg.Server.Listen, "HOARDDB_LISTEN")
	setString(&cfg.Server.NodeID, "HOARDDB_NODE_ID")
	setString(&cfg.Storage.DataDir, "HOARDDB_DATA_DIR")
	setString(&cfg.Storage.BlobDir, "HOARDDB_BLOB_DIR")
	setInt(&cfg.Storage.MaxBlobBytes, "HOARDDB_MAX_BLOB_BYTES")
	setInt(&cfg.Storage.MaxDocumentBytes, "HOARDDB_MAX_DOCUMENT_BYTES")
	setString(&cfg.Server.RootUser, "HOARDDB_ROOT_USER")
	setString(&cfg.Server.RootPassword, "HOARDDB_ROOT_PASSWORD")
	setString(&cfg.TLS.Mode, "HOARDDB_TLS_MODE")
	setString(&cfg.TLS.Cert, "HOARDDB_TLS_CERT")
	setString(&cfg.TLS.Key, "HOARDDB_TLS_KEY")
	setString(&cfg.TLS.CA, "HOARDDB_TLS_CA")
	setString(&cfg.Logging.Level, "HOARDDB_LOG_LEVEL")
	setString(&cfg.Logging.Format, "HOARDDB_LOG_FORMAT")
	setString(&cfg.Logging.Output, "HOARDDB_LOG_OUTPUT")
	setBool(&cfg.Metrics.Enabled, "HOARDDB_METRICS_ENABLED")
	setString(&cfg.Metrics.Listen, "HOARDDB_METRICS_LISTEN")
	setBool(&cfg.Audit.Enabled, "HOARDDB_AUDIT_ENABLED")
	setString(&cfg.Audit.Output, "HOARDDB_AUDIT_OUTPUT")
	setBool(&cfg.Debug.PProfEnabled, "HOARDDB_PPROF_ENABLED")
	setInt(&cfg.Debug.PProfBlockRateNS, "HOARDDB_PPROF_BLOCK_RATE_NS")
	setInt(&cfg.Debug.PProfMutexFraction, "HOARDDB_PPROF_MUTEX_FRACTION")
	setInt(&cfg.Server.ReplicationFactor, "HOARDDB_REPLICATION_FACTOR")
	setInt(&cfg.Server.DumpPinCeilingSeconds, "HOARDDB_DUMP_PIN_CEILING_SECONDS")
	setString(&cfg.Server.WriteConcern, "HOARDDB_WRITE_CONCERN")
	setInt(&cfg.Server.MaxConns, "HOARDDB_MAX_CONNS")
	setInt(&cfg.Server.MaxPeerConns, "HOARDDB_MAX_PEER_CONNS")
	setString(&cfg.TCP.Listen, "HOARDDB_TCP_LISTEN")
	setString(&cfg.TCP.Advertise, "HOARDDB_TCP_ADVERTISE")
	setInt(&cfg.TCP.IdleTimeoutSeconds, "HOARDDB_TCP_IDLE_TIMEOUT")
	setInt(&cfg.TCP.FrameTimeoutSeconds, "HOARDDB_TCP_FRAME_TIMEOUT")
	setInt(&cfg.TCP.MaxConnsPerIP, "HOARDDB_TCP_MAX_CONNS_PER_IP")
	setInt(&cfg.TCP.AuthMaxFails, "HOARDDB_TCP_AUTH_MAX_FAILS")
	setInt(&cfg.TCP.PipelineFrames, "HOARDDB_TCP_PIPELINE_FRAMES")
	setInt(&cfg.TCP.PipelineBytes, "HOARDDB_TCP_PIPELINE_BYTES")
	setString(&cfg.TCP.InternodeListen, "HOARDDB_TCP_INTERNODE_LISTEN")
	clampTCPConfig(cfg)
	setInt(&cfg.Query.MaxResultBytes, "HOARDDB_QUERY_MAX_RESULT_BYTES")
	setInt(&cfg.Query.Concurrency, "HOARDDB_QUERY_CONCURRENCY")
	clampQueryConfig(cfg)
	setInt(&cfg.Server.WALRetentionSeconds, "HOARDDB_WAL_RETENTION_INTERVAL")
	setInt(&cfg.Server.TTLSweepSeconds, "HOARDDB_TTL_SWEEP_SECONDS")
	setInt(&cfg.Server.TTLSweepBudgetSeconds, "HOARDDB_TTL_SWEEP_BUDGET_SECONDS")
	setInt(&cfg.Server.DiskHighWaterPct, "HOARDDB_DISK_HIGH_WATER")
	setFloat(&cfg.Server.RestoreDiskHeadroomMultiplier, "HOARDDB_RESTORE_DISK_HEADROOM_MULTIPLIER")
	if cfg.Server.RestoreDiskHeadroomMultiplier < 1.0 {
		slog.Warn("clamping restore_disk_headroom_multiplier below 1.0x to the default",
			"value", cfg.Server.RestoreDiskHeadroomMultiplier, "default", 2.0)
		cfg.Server.RestoreDiskHeadroomMultiplier = 2.0
	}
	setBool(&cfg.Server.AutoRebalance, "HOARDDB_AUTO_REBALANCE")
	setInt(&cfg.Storage.BadgerBlockCacheMB, "HOARDDB_BADGER_BLOCK_CACHE_MB")
	setInt(&cfg.Storage.BadgerIndexCacheMB, "HOARDDB_BADGER_INDEX_CACHE_MB")
	setInt(&cfg.Storage.BadgerNumCompactors, "HOARDDB_BADGER_NUM_COMPACTORS")
	setString(&cfg.Storage.BadgerTableSync, "HOARDDB_BADGER_TABLE_SYNC")
	setString(&cfg.Storage.WALPreallocate, "HOARDDB_WAL_PREALLOCATE")
	setInt(&cfg.Storage.MemtableMB, "HOARDDB_MEMTABLE_MB")
	setInt(&cfg.Storage.ReclaimKeysPerSecond, "HOARDDB_RECLAIM_KEYS_PER_SECOND")
	setBool(&cfg.Storage.UnsafeAckBeforeSync, "HOARDDB_UNSAFE_ACK_BEFORE_SYNC")
	setInt(&cfg.Auth.TokenTTLSeconds, "HOARDDB_TOKEN_TTL")
	setInt(&cfg.Auth.TokenMaxLifetimeHours, "HOARDDB_TOKEN_MAX_LIFETIME")
	setInt(&cfg.Auth.AuthSkewSeconds, "HOARDDB_AUTH_SKEW")
	setInt(&cfg.Auth.AuthMaxDerivations, "HOARDDB_AUTH_MAX_DERIVATIONS")
	setInt(&cfg.Auth.AuthKeyringWindowSeconds, "HOARDDB_AUTH_KEYRING_WINDOW")
	setString(&cfg.Auth.AuthDeploymentID, "HOARDDB_AUTH_DEPLOYMENT_ID")
	setInt(&cfg.Auth.AuthConfigMaxStalenessSeconds, "HOARDDB_AUTH_CONFIG_MAX_STALENESS")
	setInt(&cfg.Auth.AuthMasterRetrySeconds, "HOARDDB_AUTH_MASTER_RETRY")
	setInt(&cfg.Auth.PasswordMinLength, "HOARDDB_PASSWORD_MIN_LENGTH")
	setBool(&cfg.Auth.AuthBootstrapRoot, "HOARDDB_AUTH_BOOTSTRAP_ROOT")
	setBool(&cfg.Auth.RequireRootPassword, "HOARDDB_REQUIRE_ROOT_PASSWORD")

	// Encryption config overrides
	setString(&cfg.Encryption.Keystore, "HOARDDB_ENCRYPTION_KEYSTORE")
	setString(&cfg.Encryption.KeyFile, "HOARDDB_ENCRYPTION_KEY_FILE")
	setString(&cfg.Encryption.Mode, "HOARDDB_ENCRYPTION_MODE")

	// Entry-ring keys: their names are derived from the key path (EnvName).
	if err := applyEntryRingEnv(cfg); err != nil {
		badVariables = append(badVariables, err)
	}
	// Clamped after the entry-ring binding: the env value is only set there, and the
	// file value needs the same bound.
	clampConfigInt("HOARDDB_QUERY_NEAREST_MAX_ELEMENTS", &cfg.Query.NearestMaxElements, 1, 10_000_000)
	return errors.Join(badVariables...)
}

// applySecretFileOverrides implements the spec 7.1 rule 4 _FILE indirection:
// an environment variable is readable through /proc/<pid>/environ, docker
// inspect and most orchestrator manifests, while a path is readable only by
// whoever can read the file. When both are set the file wins, with a warning
// naming which value was used.
func applySecretFileOverrides(cfg *Config) error {
	path := os.Getenv("HOARDDB_ROOT_PASSWORD_FILE")
	if path == "" {
		return nil
	}
	value, err := readSecretFile("HOARDDB_ROOT_PASSWORD_FILE", path)
	if err != nil {
		return err
	}
	if cfg.Server.RootPassword != "" && cfg.Server.RootPassword != value {
		slog.Warn("both HOARDDB_ROOT_PASSWORD and HOARDDB_ROOT_PASSWORD_FILE are set; using the file",
			"file", path)
	}
	cfg.Server.RootPassword = value
	return nil
}

// readSecretFile reads a 0600 secret file, warning (not failing) when the
// mode is broader, because a world-readable secret is a problem the operator
// must see but not one that should stop a recovery operation.
func readSecretFile(envName, path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("read %s %q: %w", envName, path, err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		slog.Warn("secret file is readable beyond its owner",
			"env", envName, "path", path, "mode", fmt.Sprintf("%04o", perm))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s %q: %w", envName, path, err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("%s %q is empty; write the secret or unset the variable", envName, path)
	}
	return value, nil
}

// clampConfigInt bounds a config value to [min, max], warning and naming the
// clamped value so an out-of-range number is visible instead of silently
// accepted.
func clampConfigInt(name string, ptr *int, min, max int) {
	if *ptr < min || *ptr > max {
		slog.Warn("clamping out-of-range config value",
			"key", name,
			"value", *ptr,
			"min", min,
			"max", max,
		)
		if *ptr < min {
			*ptr = min
		} else {
			*ptr = max
		}
	}
}

// clampTCPConfig bounds the pipelining window to the ranges the TCP
// listener can honour (spec section 6) and logs a warning naming the
// clamped value, so an out-of-range number is visible instead of silently
// accepted. The window is a server-enforced maximum: 1 frame is strict
// lockstep and 64 frames / 16 MiB is the largest read-ahead the listener
// will ever perform.
func clampTCPConfig(cfg *Config) {
	clampConfigInt("HOARDDB_TCP_PIPELINE_FRAMES", &cfg.TCP.PipelineFrames, 1, 64)
	clampConfigInt("HOARDDB_PPROF_BLOCK_RATE_NS", &cfg.Debug.PProfBlockRateNS, 0, math.MaxInt32)
	clampConfigInt("HOARDDB_PPROF_MUTEX_FRACTION", &cfg.Debug.PProfMutexFraction, 0, math.MaxInt32)
	clampConfigInt("HOARDDB_TCP_PIPELINE_BYTES", &cfg.TCP.PipelineBytes, 1<<20, 16<<20)
}

// The range HOARDDB_QUERY_MAX_RESULT_BYTES is clamped to. A pop's owner stamps
// its ceiling on the logged entry and every applier clamps the stamp to the
// same range, so the bounds have one source.
const (
	MinMaxResultBytes = 1 << 20
	MaxMaxResultBytes = 24 << 20
)

// clampQueryConfig bounds the result ceiling. Its upper end is what keeps a
// found query's frame under MaxFrameLength (spec §3.2 "The ceiling, by
// derivation"): 24 MiB of elements leaves 8 MiB for the envelope and the
// echoed names.
//
// Concurrency 0 resolves to 2 × GOMAXPROCS, the server/auth.go precedent for
// sizing a CPU-bound pool: a query's walk is BSON decode and filtering.
func clampQueryConfig(cfg *Config) {
	clampConfigInt("HOARDDB_QUERY_MAX_RESULT_BYTES", &cfg.Query.MaxResultBytes, MinMaxResultBytes, MaxMaxResultBytes)
	if cfg.Query.Concurrency == 0 {
		cfg.Query.Concurrency = DefaultQueryConcurrency()
	}
	clampConfigInt("HOARDDB_QUERY_CONCURRENCY", &cfg.Query.Concurrency, 4, 256)
}

// DefaultQueryConcurrency is the query slot count when none is configured:
// 2 × GOMAXPROCS, clamped to 4..256 as a configured value is.
func DefaultQueryConcurrency() int {
	return min(max(2*runtime.GOMAXPROCS(0), 4), 256)
}

// The memtable range. The floor is where Badger's atomic batch bound (15% of
// the memtable) is 5,033,164 bytes, the figure the batch-split work and the
// ingest batch budget assume. The ceiling is an operating limit, not Badger's:
// Badger's write-ahead log file is 2 x the memtable and addressed with 32 bits,
// so it breaks just under 2 GiB, and the node holds three memtables, each with
// an arena of about 1.3 x its size, so 1 GiB is already about 3.9 GB of arena.
// AutoMemtableMaxMB is the largest size a node chooses for itself: the owner
// capped the automatic choice at 64 MiB on 2026-10-02 (release burn-down
// 2026-09-30 §4d item 22) and raised the cap to 128 MiB on 2026-10-03 (item 23)
// because 64 MiB measured about 14% less write throughput than 128 on a 16 GB
// host. 256 to 1024 MiB are explicit only.
const (
	MinMemtableMB     = 32
	MaxMemtableMB     = 1024
	AutoMemtableMaxMB = 128
)

// memtableSizingDivisor is the memory limit per MiB of memtable the automatic
// choice allows: at 56, the predicted batch-load peak stays at or under about a
// fifth of the limit at every step, and the steps (3.5, 7, 14 GiB) fall between
// nominal machine sizes rather than on them.
const memtableSizingDivisor = 56 << 20

// HostMemtableMB is the memtable a new store chooses from the memory limit it
// is allowed: the limit / 56 MiB rounded down to a power of two, from
// MinMemtableMB to AutoMemtableMaxMB. That is 32 MiB below 3.5 GiB, 64 MiB from
// 3.5 GiB and 128 MiB from 7 GiB.
func HostMemtableMB(limitBytes int64) int {
	memtableMB := MinMemtableMB
	for memtableMB < AutoMemtableMaxMB && int64(memtableMB*2)*memtableSizingDivisor <= limitBytes {
		memtableMB *= 2
	}
	return memtableMB
}

// HostMemtableMinLimitBytes is the smallest memory limit at which a new store
// chooses memtableMB by itself: 3.5 GiB for 64 MiB, 7 GiB for 128 MiB.
func HostMemtableMinLimitBytes(memtableMB int) int64 {
	return int64(memtableMB) * memtableSizingDivisor
}

// PredictedPeakBytes is the batch-load peak resident memory expected at a
// memtable size: about 200 MB plus 8.2 MB per MiB of memtable, the slope
// measured on a 1 vCPU / 3 GB VM (463 MB at 32 MiB, 1,251 MB at 128 MiB, one
// client loading in batches). Many concurrent writers need more.
func PredictedPeakBytes(memtableMB int) int64 {
	return 200_000_000 + 8_200_000*int64(memtableMB)
}

// MemtableBatchBytes is the store's largest atomic batch at a memtable size:
// Badger allows 15% of the memtable in one transaction.
func MemtableBatchBytes(memtableMB int) int64 {
	return 15 * (int64(memtableMB) << 20) / 100
}

// tlsNotBuiltAdvice is what to do instead of a custom certificate in this
// version, shared by every tls refusal.
const tlsNotBuiltAdvice = `custom certificates are not available in this version: every node serves the self-signed certificate it generates in <data_dir>/keys/, whatever the tls settings say
       remove the setting (or set HOARDDB_TLS_MODE=auto) to start
       to verify a node's identity, read the fingerprint from its start-up log and pin it with HOARDDB_SEED_FINGERPRINTS (see "Seed pinning" in the configuration docs)`

// validateTLS refuses tls settings that no code reads. Custom mode
// is not built, so accepting it, or a certificate path under auto mode, would
// let an operator believe a node presents a certificate it never serves. An
// empty mode is auto: it claims nothing.
func validateTLS(tlsCfg TLSConfig) error {
	switch tlsCfg.Mode {
	case "", "auto":
	case "custom":
		return fmt.Errorf("tls.mode (HOARDDB_TLS_MODE) is \"custom\", but %s", tlsNotBuiltAdvice)
	default:
		return fmt.Errorf("tls.mode (HOARDDB_TLS_MODE) is %q, an unknown value; the only mode is \"auto\", and %s", tlsCfg.Mode, tlsNotBuiltAdvice)
	}
	ignored := []struct {
		key, variable, value string
	}{
		{"tls.cert", "HOARDDB_TLS_CERT", tlsCfg.Cert},
		{"tls.key", "HOARDDB_TLS_KEY", tlsCfg.Key},
		{"tls.ca", "HOARDDB_TLS_CA", tlsCfg.CA},
	}
	for _, setting := range ignored {
		if setting.value != "" {
			return fmt.Errorf("%s (%s) is set to %q and would be ignored: %s", setting.key, setting.variable, setting.value, tlsNotBuiltAdvice)
		}
	}
	return nil
}

// validateMemtableMB refuses a memtable size outside the supported range. Zero
// is accepted and means not set: the store's own choice applies.
func validateMemtableMB(memtableMB int) error {
	switch {
	case memtableMB == 0:
		return nil
	case memtableMB < MinMemtableMB:
		return fmt.Errorf("storage.memtable_mb is %d, below the minimum of %d. "+
			"The store's largest atomic batch is 15%% of the memtable, and under %d MiB it drops below 5 MB, "+
			"which the ingest batch budget and batch splitting assume. "+
			"Set storage.memtable_mb (or HOARDDB_MEMTABLE_MB) to a value from %d to %d, or remove it to run at the size this node's store chose at its first start",
			memtableMB, MinMemtableMB, MinMemtableMB, MinMemtableMB, MaxMemtableMB)
	case memtableMB > MaxMemtableMB:
		return fmt.Errorf("storage.memtable_mb is %d, above the maximum of %d. "+
			"The node holds three memtables, each needing about 1.3 times its size in memory, so %d MiB is already about 3.9 GB. "+
			"Set storage.memtable_mb (or HOARDDB_MEMTABLE_MB) to a value from %d to %d, or remove it to run at the size this node's store chose at its first start",
			memtableMB, MaxMemtableMB, MaxMemtableMB, MinMemtableMB, MaxMemtableMB)
	}
	return nil
}

// parseEnvInt reads a whole-number environment value at the given bit width,
// for both the closure-based overrides and the entry-ring binder. A refused
// value keeps the field's current value, which the message names.
func parseEnvInt(env, raw string, current int64, bitSize int) (int64, error) {
	n, err := strconv.ParseInt(raw, 10, bitSize)
	if err != nil {
		return 0, fmt.Errorf(
			"%s=%q is not a whole number. Set it to an integer with no unit, space or fraction (for example 64, not 64MB or 64.5), or unset it to keep the current value of %d",
			env, raw, current)
	}
	return n, nil
}

// parseEnvBool reads a true-or-false environment value, for both the
// closure-based overrides and the entry-ring binder.
func parseEnvBool(env, raw string, current bool) (bool, error) {
	switch strings.ToLower(raw) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	}
	return false, fmt.Errorf(
		"%s=%q is not true or false. Set it to true, false, 1, 0, yes or no, or unset it to keep the current value of %t",
		env, raw, current)
}
