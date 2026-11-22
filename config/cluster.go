package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The defaults the start-up check quotes in its refusal texts. They are the
// values conventions §16 gives for the keys they name.
const (
	defaultHeartbeatMS  = 100
	defaultStepDownMS   = 2000
	defaultProbeIntvMS  = 1000
	defaultProbeTimeout = 500
	defaultProbeFail    = 3
	defaultFailoverMS   = 5000
	defaultDedupeWindow = 8192
)

// The environment names the cluster mode refusals quote. The struct tags carry
// the same names, but a tag must be a literal; TestClusterModeEnvConstantsMatchTags
// keeps the two in step.
const (
	envClusterEnabled   = "HOARDDB_CLUSTER_ENABLED"
	envClusterListen    = "HOARDDB_CLUSTER_LISTEN"
	envClusterAdvertise = "HOARDDB_CLUSTER_ADVERTISE"
	envClusterWitness   = "HOARDDB_CLUSTER_WITNESS"
	// envClusterAllowMembership names cluster.allow_membership.
	envClusterAllowMembership = "HOARDDB_CLUSTER_ALLOW_MEMBERSHIP"
)

// ClusterConfig holds the Raft timers and limits of a cluster (conventions §16, the
// cluster group). Every key here is new to the entry-ring design, so each env name is
// derived from its key path by EnvName. The failure-detection and lease timings are not
// keys: the cluster runs on its committed values (RemovedClusterKeys, BUG-0838).
type ClusterConfig struct {
	// Enabled starts the node as an entry-ring cluster member, with a peer
	// listener. Off, the node is the one-voter ring node it has always been.
	Enabled bool `yaml:"enabled" env:"HOARDDB_CLUSTER_ENABLED"`
	// Listen is the peer listener address. Empty means server.listen.
	Listen string `yaml:"listen" env:"HOARDDB_CLUSTER_LISTEN"`
	// Advertise is the address peers are told to dial. Empty means Listen; a
	// wildcard Listen needs it set.
	Advertise string `yaml:"advertise" env:"HOARDDB_CLUSTER_ADVERTISE"`
	// Witness joins the node as a witness: it votes and stores no data.
	Witness bool `yaml:"witness" env:"HOARDDB_CLUSTER_WITNESS"`
	// AllowMembership is the operator lock on join, promote, leave, remove-node and
	// replace-node. Unset means true in cluster mode: a membership statement works with the
	// keys the five-minute page already sets. False refuses every membership statement on this
	// node and, when it leads, on behalf of the members that forward to it (plan section 6).
	// A pointer, so an explicit true can be told from the default.
	AllowMembership       *bool `yaml:"allow_membership" env:"HOARDDB_CLUSTER_ALLOW_MEMBERSHIP"`
	RaftHeartbeatMS       int   `yaml:"raft_heartbeat_ms" env:"HOARDDB_CLUSTER_RAFT_HEARTBEAT_MS"`
	RaftElectionMS        int   `yaml:"raft_election_ms" env:"HOARDDB_CLUSTER_RAFT_ELECTION_MS"`
	RaftLeaderLeaseMS     int   `yaml:"raft_leader_lease_ms" env:"HOARDDB_CLUSTER_RAFT_LEADER_LEASE_MS"`
	RaftSnapshotThreshold int   `yaml:"raft_snapshot_threshold" env:"HOARDDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD"`
	RaftTrailingLogs      int   `yaml:"raft_trailing_logs" env:"HOARDDB_CLUSTER_RAFT_TRAILING_LOGS"`
	// Voters is the voting member count: 3 or 5, witnesses included.
	Voters int `yaml:"voters" env:"HOARDDB_CLUSTER_VOTERS"`
	// DeclCacheEntries and DeclCacheBytes bound the declaration cache per
	// node; the byte bound wins (conventions §20).
	DeclCacheEntries int `yaml:"decl_cache_entries" env:"HOARDDB_CLUSTER_DECL_CACHE_ENTRIES"`
	DeclCacheBytes   int `yaml:"decl_cache_bytes" env:"HOARDDB_CLUSTER_DECL_CACHE_BYTES"`
	// PeerMaxPending is the most internode connections the port holds before they are
	// routed to a channel (a caller that connects and says nothing holds one for up to 5 s).
	PeerMaxPending int `yaml:"peer_max_pending" env:"HOARDDB_CLUSTER_PEER_MAX_PENDING"`
	// PeerMaxConns and PeerMaxInFlight bound each of the data and control channels:
	// connections held, and requests being handled at once.
	PeerMaxConns    int `yaml:"peer_max_conns" env:"HOARDDB_CLUSTER_PEER_MAX_CONNS"`
	PeerMaxInFlight int `yaml:"peer_max_in_flight" env:"HOARDDB_CLUSTER_PEER_MAX_IN_FLIGHT"`
	// PeerIdleSeconds closes an authenticated data or control connection that sends nothing
	// for this long; PeerFrameTimeoutMS bounds the rest of a frame once it has begun, and
	// each reply write.
	PeerIdleSeconds    int `yaml:"peer_idle_seconds" env:"HOARDDB_CLUSTER_PEER_IDLE_SECONDS"`
	PeerFrameTimeoutMS int `yaml:"peer_frame_timeout_ms" env:"HOARDDB_CLUSTER_PEER_FRAME_TIMEOUT_MS"`
	// RaftMaxConns is the most inbound Raft connections held; RaftIdleSeconds closes an
	// admitted one that goes quiet. It must stay well above the Raft timers (ValidateTimings).
	RaftMaxConns    int `yaml:"raft_max_conns" env:"HOARDDB_CLUSTER_RAFT_MAX_CONNS"`
	RaftIdleSeconds int `yaml:"raft_idle_seconds" env:"HOARDDB_CLUSTER_RAFT_IDLE_SECONDS"`
}

// ReplicationConfig holds the write-path, dedupe and seeding bounds of a node
// (conventions §16, the replication group). Every key is new.
type ReplicationConfig struct {
	RetainedLogSeconds int `yaml:"retained_log_seconds" env:"HOARDDB_REPLICATION_RETAINED_LOG_SECONDS"`
	// RetentionCheckSeconds is the period of the ring node's retention ticker, which also
	// wakes the reclaim loop for the dedupe sweep (storage-format spec §3.7); 1 to 3600.
	RetentionCheckSeconds int `yaml:"retention_check_seconds" env:"HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS"`
	// LogMaxBytes is int64 because its default, 4 GiB, does not fit the int of
	// a 32-bit build, which must still compile (the low-power targets).
	LogMaxBytes           int64 `yaml:"log_max_bytes" env:"HOARDDB_REPLICATION_LOG_MAX_BYTES"`
	ReplWindow            int   `yaml:"repl_window" env:"HOARDDB_REPLICATION_REPL_WINDOW"`
	DedupeWindow          int   `yaml:"dedupe_window" env:"HOARDDB_REPLICATION_DEDUPE_WINDOW"`
	DedupeBytes           int   `yaml:"dedupe_bytes" env:"HOARDDB_REPLICATION_DEDUPE_BYTES"`
	HandoffDrainMS        int   `yaml:"handoff_drain_ms" env:"HOARDDB_REPLICATION_HANDOFF_DRAIN_MS"`
	HandoffRetryMS        int   `yaml:"handoff_retry_ms" env:"HOARDDB_REPLICATION_HANDOFF_RETRY_MS"`
	WCTimeoutMS           int   `yaml:"wc_timeout_ms" env:"HOARDDB_REPLICATION_WC_TIMEOUT_MS"`
	SeedSlotsInFlight     int   `yaml:"seed_slots_in_flight" env:"HOARDDB_REPLICATION_SEED_SLOTS_IN_FLIGHT"`
	SeedBytesPerSecond    int   `yaml:"seed_bytes_per_second" env:"HOARDDB_REPLICATION_SEED_BYTES_PER_SECOND"`
	SeedWindowBytes       int   `yaml:"seed_window_bytes" env:"HOARDDB_REPLICATION_SEED_WINDOW_BYTES"`
	SeedChunkBytes        int   `yaml:"seed_chunk_bytes" env:"HOARDDB_REPLICATION_SEED_CHUNK_BYTES"`
	SeedChunkEntries      int   `yaml:"seed_chunk_entries" env:"HOARDDB_REPLICATION_SEED_CHUNK_ENTRIES"`
	SeedBufferBytes       int   `yaml:"seed_buffer_bytes" env:"HOARDDB_REPLICATION_SEED_BUFFER_BYTES"`
	SeedSyncBytes         int   `yaml:"seed_sync_bytes" env:"HOARDDB_REPLICATION_SEED_SYNC_BYTES"`
	SeedResumeHoldSeconds int   `yaml:"seed_resume_hold_seconds" env:"HOARDDB_REPLICATION_SEED_RESUME_HOLD_SECONDS"`
	SeedIdleSeconds       int   `yaml:"seed_idle_seconds" env:"HOARDDB_REPLICATION_SEED_IDLE_SECONDS"`
	SeedDiskHighWaterPct  int   `yaml:"seed_disk_high_water_pct" env:"HOARDDB_REPLICATION_SEED_DISK_HIGH_WATER_PCT"`
	IncomingSeedsMax      int   `yaml:"incoming_seeds_max" env:"HOARDDB_REPLICATION_INCOMING_SEEDS_MAX"`
}

// DefaultClusterConfig returns the cluster group at the values conventions §16
// gives. ValidateTimings passes at these values, so a start with no cluster
// settings is unaffected.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		RaftHeartbeatMS:       1000,
		RaftElectionMS:        1000,
		RaftLeaderLeaseMS:     500,
		RaftSnapshotThreshold: 128,
		RaftTrailingLogs:      64,
		Voters:                3,
		DeclCacheEntries:      4096,
		DeclCacheBytes:        33554432,
		PeerMaxPending:        128,
		PeerMaxConns:          256,
		PeerMaxInFlight:       256,
		PeerIdleSeconds:       120,
		PeerFrameTimeoutMS:    10000,
		RaftMaxConns:          64,
		RaftIdleSeconds:       120,
	}
}

// DefaultReplicationConfig returns the replication group at the values
// conventions §16 gives.
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		RetainedLogSeconds:    1800,
		RetentionCheckSeconds: 60,
		LogMaxBytes:           4294967296,
		ReplWindow:            64,
		DedupeWindow:          8192,
		DedupeBytes:           536870912,
		HandoffDrainMS:        2000,
		HandoffRetryMS:        200,
		WCTimeoutMS:           5000,
		SeedSlotsInFlight:     2,
		SeedBytesPerSecond:    67108864,
		SeedWindowBytes:       8388608,
		SeedChunkBytes:        1048576,
		SeedChunkEntries:      1024,
		SeedBufferBytes:       268435456,
		SeedSyncBytes:         67108864,
		SeedResumeHoldSeconds: 300,
		SeedIdleSeconds:       120,
		SeedDiskHighWaterPct:  85,
		IncomingSeedsMax:      4,
	}
}

// EnvName derives the environment variable of a configuration key: the
// prefix, then the key path upper-cased with each dot made an underscore.
// replication.log_max_bytes becomes HOARDDB_REPLICATION_LOG_MAX_BYTES
// (conventions §16).
func EnvName(key string) string {
	return "HOARDDB_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

// ValidateTimings is the start-up check of membership §Errors (E11) for the keys a node can
// set: the voter count and the peer and Raft limits. It runs in Load for every start, before
// anything joins: the defaults pass it, so a one-node start with no cluster settings is
// unaffected, and the join path inherits the same function rather than a copy of it. The
// lease and failure-detection timings are not configurable, so they are checked as
// CommittedTimings, by the test harness that changes them through SET_SETTING.
func (c *Config) ValidateTimings() error {
	cluster := c.Cluster
	if cluster.Voters != 3 && cluster.Voters != 5 {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text is the spec's verbatim E11 refusal (membership Errors), which ends in a full stop
			"cluster.voters is %d, and hoardDB runs 3 or 5 voters: with 2 neither node can commit while the other is down, "+
				"so writes stop instead of degrading, and with 4 a majority needs 3 nodes to agree. Set 'cluster.voters: 3' "+
				"(the default) or 'cluster.voters: 5' in the configuration file, or HOARDDB_CLUSTER_VOTERS=3, then restart this node.",
			cluster.Voters)
	}
	return cluster.validatePeerLimits()
}

// CommittedTimings are the lease and failure-detection values a cluster commits, in
// milliseconds except ProbeFailures (a count). The cluster seeds them at creation and only
// SET_SETTING changes them; no file or variable does (BUG-0838).
type CommittedTimings struct {
	HeartbeatMS     int
	StepDownMS      int
	FailoverMS      int
	ProbeIntervalMS int
	ProbeTimeoutMS  int
	ProbeFailures   int
}

// DefaultCommittedTimings returns the values conventions §16 gives, which pass Validate.
func DefaultCommittedTimings() CommittedTimings {
	return CommittedTimings{
		HeartbeatMS:     defaultHeartbeatMS,
		StepDownMS:      defaultStepDownMS,
		FailoverMS:      defaultFailoverMS,
		ProbeIntervalMS: defaultProbeIntvMS,
		ProbeTimeoutMS:  defaultProbeTimeout,
		ProbeFailures:   defaultProbeFail,
	}
}

// Validate checks the E11 inequalities between the committed timings (membership §Errors).
func (t CommittedTimings) Validate() error {
	detection := (t.ProbeFailures-1)*t.ProbeIntervalMS + t.ProbeTimeoutMS
	if t.StepDownMS+t.HeartbeatMS > detection {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text is the spec's verbatim E11 refusal (membership Errors), which ends in a full stop
			"step_down_ms (%d) plus heartbeat_ms (%d) must not exceed the failure-detection floor "+
				"(probe_failures - 1) x probe_interval_ms + probe_timeout_ms = %d ms: that is the earliest instant a majority "+
				"can commit a node down, so a node whose lease runs past it could still accept writes after it has been replaced. "+
				"Set 'step_down_ms' to %d (the default) or raise 'probe_interval_ms'. "+
				"The values leave %d - %d - %d = %d ms of margin.",
			t.StepDownMS, t.HeartbeatMS, detection,
			defaultStepDownMS,
			detection, t.StepDownMS, t.HeartbeatMS, detection-t.StepDownMS-t.HeartbeatMS)
	}
	floor := t.ProbeFailures*t.ProbeIntervalMS + t.ProbeTimeoutMS
	if t.FailoverMS < floor {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text is the spec's verbatim E11 refusal (membership Errors), which ends in a full stop
			"failover_ms (%d) must be at least probe_failures x probe_interval_ms + "+
				"probe_timeout_ms = %d ms, so the second observer's report reaches the leader while the window is open; "+
				"otherwise no member is ever declared down. Set 'failover_ms' to %d or more "+
				"(the defaults give %d x %d + %d = %d), or lower probe_interval_ms.",
			t.FailoverMS, floor, floor,
			defaultProbeFail, defaultProbeIntvMS, defaultProbeTimeout, floor)
	}
	return nil
}

// validatePeerLimits refuses a negative connection bound, and a Raft idle bound that a
// healthy leader's heartbeats could not keep alive. Zero means "take the default", which
// is how a struct built field by field in a test stays valid.
func (c ClusterConfig) validatePeerLimits() error {
	for _, bound := range []struct {
		key   string
		value int
	}{
		{"cluster.peer_max_pending", c.PeerMaxPending}, {"cluster.peer_max_conns", c.PeerMaxConns},
		{"cluster.peer_max_in_flight", c.PeerMaxInFlight}, {"cluster.peer_idle_seconds", c.PeerIdleSeconds},
		{"cluster.peer_frame_timeout_ms", c.PeerFrameTimeoutMS}, {"cluster.raft_max_conns", c.RaftMaxConns},
		{"cluster.raft_idle_seconds", c.RaftIdleSeconds},
	} {
		if bound.value < 0 {
			return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
				"%s is %d, and it must be positive. Remove it from the configuration file to take the default, or set a positive number, then restart this node.",
				bound.key, bound.value)
		}
	}
	timer := max(c.RaftHeartbeatMS, c.RaftElectionMS)
	if c.RaftIdleSeconds > 0 && c.RaftIdleSeconds*1000 < 10*timer {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"cluster.raft_idle_seconds (%d) is under ten times the Raft timers (%d ms), so a quiet but healthy peer would have its connection closed between messages. "+
				"Set 'cluster.raft_idle_seconds: %d' (the default) or at least %d, then restart this node.",
			c.RaftIdleSeconds, timer, DefaultClusterConfig().RaftIdleSeconds, (10*timer+999)/1000)
	}
	return nil
}

// ValidateDedupeWindow refuses a replication.dedupe_window that is not a
// positive count of applied records (conventions §16). A zero window would
// look valid and let a retried write apply twice, so the start refuses it
// rather than letting the machine quietly treat it as 1.
func (c *Config) ValidateDedupeWindow() error {
	window := c.Replication.DedupeWindow
	if window > 0 {
		return nil
	}
	return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
		"replication.dedupe_window is %d, and it must be a positive number of applied records: a window of 0 or less leaves "+
			"every dedupe entry dead, so a retried request applies twice. Set 'replication.dedupe_window: %d' (the default) "+
			"in the configuration file, or HOARDDB_REPLICATION_DEDUPE_WINDOW=%d, then restart this node.",
		window, defaultDedupeWindow, defaultDedupeWindow)
}

// MaxRetentionCheckSeconds is the longest replication.retention_check_seconds the start
// accepts: an hour between passes is already a log held an hour longer than its window.
const MaxRetentionCheckSeconds = 3600

// EnvRetentionCheckSeconds is replication.retention_check_seconds' environment name, the
// one the refusal and the user documentation quote.
const EnvRetentionCheckSeconds = "HOARDDB_REPLICATION_RETENTION_CHECK_SECONDS"

// ValidateRetentionCheck refuses a replication.retention_check_seconds outside 1 to 3600.
func (c *Config) ValidateRetentionCheck() error {
	seconds := c.Replication.RetentionCheckSeconds
	if seconds >= 1 && seconds <= MaxRetentionCheckSeconds {
		return nil
	}
	return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
		"replication.retention_check_seconds is %d, and it must be 1 to %d seconds: it is how often the ring node checks its "+
			"retained log and sweeps dead dedupe entries. Set 'replication.retention_check_seconds: 60' (the default) in the "+
			"configuration file, or %s=60, then restart this node.",
		seconds, MaxRetentionCheckSeconds, EnvRetentionCheckSeconds)
}

// MaxReplWindow is the largest replication.repl_window the start accepts. The window is the
// frames a copy may hold unacknowledged for each pair, and a copy queues up to twice that many,
// so a typo such as 1000000 would let one pair hold that much memory.
const MaxReplWindow = 4096

// ValidateReplWindow refuses a replication.repl_window outside 1 to 4096.
func (c *Config) ValidateReplWindow() error {
	window := c.Replication.ReplWindow
	if window >= 1 && window <= MaxReplWindow {
		return nil
	}
	return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
		"replication.repl_window is %d, and it must be between 1 and %d frames: it is how many unacknowledged records the "+
			"owner sends a copy before it waits, and a copy queues twice that many per pair. Set 'replication.repl_window: 64' "+
			"(the default) in the configuration file, or HOARDDB_REPLICATION_REPL_WINDOW=64, then restart this node.",
		window, MaxReplWindow)
}

// MembershipAllowed reports whether membership statements are allowed on this node: true unless
// cluster.allow_membership is set to false.
func (c ClusterConfig) MembershipAllowed() bool {
	return c.AllowMembership == nil || *c.AllowMembership
}

// validateClusterMode refuses a cluster mode start that would open a peer
// listener under an identity or on storage it cannot keep safe. Each refusal
// names the key and the fix, because the node stays down until someone reads it.
func (c *Config) validateClusterMode() error {
	if err := validateClusterAddress("cluster.listen", envClusterListen, c.Cluster.Listen); err != nil {
		return err
	}
	if err := validateClusterAddress("cluster.advertise", envClusterAdvertise, c.Cluster.Advertise); err != nil {
		return err
	}
	// The listen address is the one peers dial when cluster.advertise is unset, so a zone here is
	// refused whatever advertise says: one rule is easier to state than the narrow one.
	if zone := advertiseZone(c.Cluster.Listen); zone != "" {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"cluster.listen is %q, which names a network zone (%s), and no other machine can use an interface of this host: "+
				"peers would be told to dial this address when cluster.advertise is not set, and it goes nowhere. "+
				"Set 'cluster.listen: <this node's address>:<port>' (for example 192.0.2.1:4443) in the configuration file, "+
				"or %s=<this node's address>:<port>, then restart this node.",
			c.Cluster.Listen, zone, envClusterListen)
	}
	if zone := advertiseZone(c.Cluster.Advertise); zone != "" {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"cluster.advertise is %q, which names a network zone (%s), and no other machine can use an interface of this host: "+
				"peers would be told to dial an address that goes nowhere. Set 'cluster.advertise: <this node's address>:<port>' "+
				"(for example 192.0.2.1:4443) in the configuration file, or %s=<this node's address>:<port>, then restart this node.",
			c.Cluster.Advertise, zone, envClusterAdvertise)
	}
	if !c.Cluster.Enabled {
		if c.Cluster.AllowMembership != nil && *c.Cluster.AllowMembership {
			return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
				"cluster.allow_membership is true but cluster.enabled is false: there is no cluster to change the membership of. "+
					"Set 'cluster.enabled: true' (%s=1) as well, or remove cluster.allow_membership.", envClusterEnabled)
		}
		return nil
	}
	if c.Server.NodeIDDefaulted {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"cluster.enabled is true but server.node_id is not set, so this node would take its ring identity from the hostname %q. "+
				"Hosts cloned from one image share a hostname, and two nodes with one identity cannot join the same cluster. "+
				"Set 'server.node_id: <unique name>' in the configuration file, or HOARDDB_NODE_ID=<unique name>, then restart this node.",
			c.Server.NodeID)
	}
	if strings.Contains(c.Server.NodeID, ":") {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"server.node_id is %q, which contains a colon: in cluster mode seed_fingerprints reads a key with a colon as an address, not a node id. "+
				"Set 'server.node_id: <name without a colon>' in the configuration file, or HOARDDB_NODE_ID=<name>, then restart this node.",
			c.Server.NodeID)
	}
	listen := c.Cluster.Listen
	if listen == "" {
		listen = c.Server.Listen
	}
	// cluster.listen unset: the peer listener binds server.listen, and with cluster.advertise unset
	// that is the address peers are told to dial. cluster.listen was checked above under its own name.
	if c.Cluster.Listen == "" && c.Cluster.Advertise == "" {
		if zone := advertiseZone(listen); zone != "" {
			return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
				"server.listen is %q, which names a network zone (%s), and no other machine can use an interface of this host: "+
					"cluster.listen is not set, so the peer listener binds it and peers would be told to dial it, and it goes nowhere. "+
					"Set 'server.listen: <this node's address>:<port>' (for example 192.0.2.1:4433) in the configuration file, "+
					"or HOARDDB_LISTEN=<this node's address>:<port>, or set cluster.advertise (%s) to the address peers can dial, then restart this node.",
				listen, zone, envClusterAdvertise)
		}
	}
	if err := c.refuseSharedClusterPort(listen); err != nil {
		return err
	}
	if ListensOnWildcard(listen) && c.Cluster.Advertise == "" {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"cluster.advertise is not set, and the peer listener binds %q, a wildcard address: peers would be told to dial the wildcard, "+
				"which no other machine can reach. Set 'cluster.advertise: <this node's address>:<port>' (for example 192.0.2.1:4443) "+
				"in the configuration file, or %s=<this node's address>:<port>, then restart this node.",
			listen, envClusterAdvertise)
	}
	return nil
}

// validateClusterAddress refuses a cluster.listen or cluster.advertise value
// that is not host:port with a numeric port. An empty value is the default
// and passes. Only the form is checked here: whether the address is reachable
// is the reachability check's job.
func validateClusterAddress(key, envName, value string) error {
	if value == "" {
		return nil
	}
	_, port, err := net.SplitHostPort(value)
	if err == nil {
		if _, portErr := strconv.ParseUint(port, 10, 16); portErr != nil {
			err = fmt.Errorf("port %q is not a number from 0 to 65535", port)
		}
	}
	if err != nil {
		return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
			"%s is %q, which is not host:port (%v): set '%s: <host>:<port>' (for example 192.0.2.1:4443) in the configuration file, "+
				"or %s=<host>:<port>, then restart this node.",
			key, value, err, key, envName)
	}
	return nil
}

// advertiseZone returns the zone of a host:port value ("%eth0" in "[fe80::1%eth0]:4433"), or "" when
// the host has none. A zone names an interface of this host, which no other machine can dial.
func advertiseZone(value string) string {
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		return ""
	}
	if i := strings.IndexByte(host, '%'); i >= 0 {
		return host[i:]
	}
	return ""
}

// applyEntryRingEnv binds the environment variables of the entry-ring keys.
// Each name is the env tag on the field, which TestEnvNamesAreDerived checks
// against the key path. The query group is shared with product keys that
// applyEnvOverrides already binds, so only its page-limit and nearest-ceiling fields are bound
// here.
func applyEntryRingEnv(cfg *Config) error {
	var bad []error
	bind := func(section reflect.Value, only ...string) {
		sectionType := section.Type()
		for index := 0; index < sectionType.NumField(); index++ {
			field := sectionType.Field(index)
			envName := field.Tag.Get("env")
			yamlName, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
			if envName == "" || envName == "-" || (len(only) > 0 && !slices.Contains(only, yamlName)) {
				continue
			}
			raw := os.Getenv(envName)
			if raw == "" {
				continue
			}
			fieldValue := section.Field(index)
			switch fieldValue.Kind() {
			case reflect.Int, reflect.Int64:
				n, err := parseEnvInt(envName, raw, fieldValue.Int(), fieldValue.Type().Bits())
				if err != nil {
					bad = append(bad, err)
					continue
				}
				fieldValue.SetInt(n)
			case reflect.Bool:
				b, err := parseEnvBool(envName, raw, fieldValue.Bool())
				if err != nil {
					bad = append(bad, err)
					continue
				}
				fieldValue.SetBool(b)
			case reflect.String:
				fieldValue.SetString(raw)
			case reflect.Pointer:
				// A tri-state bool: unset keeps the default, a value sets it.
				current := !fieldValue.IsNil() && fieldValue.Elem().Bool()
				b, err := parseEnvBool(envName, raw, current)
				if err != nil {
					bad = append(bad, err)
					continue
				}
				fieldValue.Set(reflect.ValueOf(&b))
			}
		}
	}
	bind(reflect.ValueOf(&cfg.Cluster).Elem())
	bind(reflect.ValueOf(&cfg.Replication).Elem())
	bind(reflect.ValueOf(&cfg.Query).Elem(), "page_limit_default", "page_limit_max", "nearest_max_elements")
	return errors.Join(bad...)
}

// refuseSharedClusterPort refuses a peer listener on the port of the driver listener or of the
// node-to-node listener of the hash ring. Left alone, the peer listener binds first and
// the start then fails blaming the other listener for a collision the cluster keys made.
func (c *Config) refuseSharedClusterPort(listen string) error {
	internode := c.TCP.InternodeListen
	if internode == "" {
		derived, err := transport.InternodePeerAddr(c.Server.Listen)
		if err == nil {
			internode = derived
		}
	}
	for _, other := range []struct{ name, addr, fix string }{
		{"tcp.listen", c.TCP.Listen, "tcp.listen"},
		{"the node-to-node listener (server.listen plus one, or tcp.internode_listen)", internode, "tcp.internode_listen"},
	} {
		if other.addr != "" && sameListenPort(listen, other.addr) {
			return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the key and the fix, and ends in a full stop like the other start-up refusals
				"cluster.listen is %s, the port %s already binds: the peer listener and that listener cannot share a port. "+
					"Set 'cluster.listen: <host>:<another port>' (the default is server.listen) or change %s, then restart this node.",
				listen, other.name, other.fix)
		}
	}
	return nil
}

// sameListenPort reports whether two listen addresses would collide: the same port on the same
// host, or on a wildcard that covers it.
func sameListenPort(first, second string) bool {
	firstHost, firstPort, firstErr := net.SplitHostPort(first)
	secondHost, secondPort, secondErr := net.SplitHostPort(second)
	if firstErr != nil || secondErr != nil || firstPort != secondPort || firstPort == "0" {
		return false
	}
	return firstHost == secondHost || ListensOnWildcard(first) || ListensOnWildcard(second)
}

// RefuseOldReplicatedMode is the refusal of the earlier replicated mode: a replication factor
// above one, or seed nodes, without cluster.enabled. The mode is no longer supported (before the
// first public release nothing older is kept), so the start stops here, before any port is
// opened or data directory touched, and names the setting that selected it and the ring setting
// that replaces it. The mode's code is deleted in a later step.
func (c *Config) RefuseOldReplicatedMode() error {
	if c.Cluster.Enabled {
		if c.Server.ReplicationFactor > 1 {
			return fmt.Errorf( //nolint:staticcheck // ST1005: names the setting and the fix, ends in a full stop like the other start-up refusals
				"HOARDDB_REPLICATION_FACTOR=%d (server.replication_factor) is not read by the ring, which sets replication per database, so this node would have kept one copy. "+
					"To keep copies, create the database with 'create database <name> { replication_factor: %d }'. "+
					"To start this node, unset HOARDDB_REPLICATION_FACTOR (and server.replication_factor in the config file).",
				c.Server.ReplicationFactor, c.Server.ReplicationFactor)
		}
		return nil
	}
	var cause string
	switch {
	case c.Server.ReplicationFactor > 1:
		cause = fmt.Sprintf("HOARDDB_REPLICATION_FACTOR=%d (server.replication_factor)", c.Server.ReplicationFactor)
	case len(c.HashRing.SeedNodes) > 0:
		cause = "HOARDDB_SEED_NODES (hash_ring.seed_nodes)"
	default:
		return nil
	}
	return fmt.Errorf( //nolint:staticcheck // ST1005: the text names the setting and the fix, and ends in a full stop like the other start-up refusals
		"%s selects the old replicated mode, which is no longer supported. "+
			"To run a cluster, start the ring instead: set %s=true; the first node lists nobody (or only itself) in HOARDDB_SEED_NODES, "+
			"and each later node lists the nodes already in the cluster as <node-id>@<host:port>; "+
			"HOARDDB_SEED_FINGERPRINTS holds one <node-id>=SHA256:<64 hex> entry per node listed, which that node prints at its start "+
			"(see the Cluster mode section of the configuration page of the user docs). "+
			"In the ring a database gets its copies from 'create database <name> { replication_factor: N }', not from HOARDDB_REPLICATION_FACTOR. "+
			"The ring does not read the data the old mode wrote: it opens <data dir>/ring and leaves the old files alone, "+
			"and no command of this build moves that data into the ring: keep the old data directory untouched and start the ring in a fresh one. "+
			"To run one node on fresh data, remove HOARDDB_REPLICATION_FACTOR and HOARDDB_SEED_NODES.",
		cause, envClusterEnabled)
}
