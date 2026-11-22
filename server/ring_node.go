package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringDirName is the entry-ring node's directory under the data directory. It sits beside
// the legacy node store (nodeStoreDirName), the WAL and the blob tree and shares nothing
// with them: the ring node keeps its own raft, badger and wal directories under it, so the
// legacy layout checks and the legacy store never see its files.
const ringDirName = "ring"

// advertiseFor is the data address a cluster node commits, and empty for a node that is not
// in cluster mode: a one-voter node has no peers to tell.
func advertiseFor(cfg *config.Config) string {
	if !cfg.Cluster.Enabled {
		return ""
	}
	return clusterAdvertiseAddr(cfg)
}

// ringNodeConfig is how the server opens the entry-ring node. Every knob is one the server
// already has: the table-sync mode is storage.badger_table_sync, the log zero fill is
// storage.wal_preallocate, the dedupe window is replication.dedupe_window, the retention
// tick is replication.retention_check_seconds and the node's name is server.node_id. The
// cluster keys add the advertised address and the witness flag, and query.nearest_max_elements
// is the nearest ceiling.
// The peer transport itself is added by ringNodeSettings, which binds the listener.
func ringNodeConfig(cfg *config.Config) wire.NodeConfig {
	blockCacheMB, indexCacheMB := ringCacheSizesMB(cfg, hostMemoryLimit)
	return wire.NodeConfig{
		Dir:            filepath.Join(cfg.Storage.DataDir, ringDirName),
		NodeID:         cfg.Server.NodeID,
		TableSync:      cfg.Storage.BadgerTableSync,
		WALPreallocate: ringWALMode(cfg).On,
		ClientAddr:     wire.ClientAddrFor(cfg.TCP.Advertise, cfg.TCP.Listen),
		DedupeWindow:   uint64(cfg.Replication.DedupeWindow),
		ReplWindow:     cfg.Replication.ReplWindow,
		Advertise:      advertiseFor(cfg),
		Witness:        cfg.Cluster.Enabled && cfg.Cluster.Witness,
		Voters:         cfg.Cluster.Voters,
		BlockCacheMB:   blockCacheMB,
		IndexCacheMB:   indexCacheMB,

		ReclaimKeysPerSecond: cfg.Storage.ReclaimKeysPerSecond,
		RetentionCheck:       time.Duration(cfg.Replication.RetentionCheckSeconds) * time.Second,
		NearestMaxElements:   cfg.Query.NearestMaxElements,
		GuardObserved:        observeGuard,
		DrainObserved:        observeDrain,
	}
}

// ringWALMode is what storage.wal_preallocate comes to for the ring node's log, which lives
// in the ring directory and not in the legacy node's.
func ringWALMode(cfg *config.Config) walModeResolution {
	return walPreallocateResolver(cfg, filepath.Join(cfg.Storage.DataDir, ringDirName, "wal"))
}

// ringNodeSettings is ringNodeConfig plus the database names this server's storage
// directories take, so the ring door refuses the same names the legacy door does. In cluster
// mode it also plans the peer trust and binds the peer listener, and returns the listener so
// the caller can close it if the node never takes ownership of it.
func (qs *Service) ringNodeSettings() (wire.NodeConfig, net.Listener, error) {
	nodeCfg := ringNodeConfig(qs.config)
	nodeCfg.StorageNames = qs.core.storageDirs()
	// Set only when the mode seals: a nil *ringAtRest in the interface would not read as nil.
	if qs.core.ringAtRest != nil {
		nodeCfg.AtRest = qs.core.ringAtRest
	}
	if !qs.config.Cluster.Enabled {
		return nodeCfg, nil, nil
	}
	plan, err := planRingPeers(qs.config, qs.core.fingerprint, qs.tlsCfg)
	if err != nil {
		return wire.NodeConfig{}, nil, err
	}
	options, listener, err := plan.clusterOptions(qs.config, func(err error) { qs.logger.Warn("entry-ring peer", "error", err) })
	if err != nil {
		return wire.NodeConfig{}, nil, err
	}
	// The cluster's password key lives beside the other credential files and is sealed like them
	// (ring user store spec 5.4). A witness keeps none; the node does not read it there.
	options.Pepper = newClusterPepperFile(qs.core.credentials, qs.config.Storage.DataDir)
	options.ProposalBudget = qs.proposalBudget
	nodeCfg.Cluster = options
	qs.ringJoin = plan.join
	return nodeCfg, listener, nil
}

// openRingNode opens the entry-ring node and returns the driver listener's handler: the ring
// handler in front of the legacy one, asking this server's own authorizer. With encryption at
// rest on the node seals its store and its log under the keys loadRingAtRest unwrapped, or
// creates them (ring encryption at rest spec §5.3).
func (qs *Service) openRingNode() (transport.RequestHandler, error) {
	settings, listener, err := qs.ringNodeSettings()
	if err != nil {
		return nil, err
	}
	node, err := wire.OpenNode(qs.core.ctx, settings)
	if err != nil {
		err = fmt.Errorf("open the entry-ring node in %s: %w", filepath.Join(qs.config.Storage.DataDir, ringDirName), err)
		if listener != nil {
			// The node closes the listener it was handed once it has taken it; a failure
			// before that leaves it bound, and a second close then reports it closed.
			if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}
		return nil, err
	}
	// Membership changes are composed in cluster mode only (join and promote today); a node with
	// no cluster keeps the handler's default, NoMembership, which authorizes the commands and
	// then refuses them.
	// User commands are this node's own users on a node without a cluster (ring_users.go), and the
	// cluster's replicated table on a cluster member (ring_users_replicated.go), which the follower
	// installs into this node's AuthManager.
	var users wire.UserAdmin = &ringUsers{server: qs.core}
	var follower *userFollower
	if qs.config.Cluster.Enabled {
		follower = newUserFollower(qs.core.auth, node, qs.core.closeUserSessions, qs.logger)
		users = newRingUsersReplicated(qs.core, node, follower, qs.userDeadline())
	}
	ringOptions := []wire.RingOption{wire.WithNodeFacts(qs.core.ringNodeFacts()), wire.WithRingResultBudget(qs.core.maxResultBytes()),
		wire.WithUserAdmin(users)}
	if qs.config.Cluster.Enabled {
		ringOptions = append(ringOptions, wire.WithMembership(wire.NewMembership(node)))
	}
	handler, err := wire.NewRingHandler(node, qs.handler, qs.core.RingAuthorizer(), ringOptions...)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("compose the ring handler: %w", err), node.Close())
	}
	qs.ringNode = node
	if qs.config.Cluster.Enabled {
		qs.core.auth.SetClusterPasswordKey(node)
		qs.users = follower
		ctx, stop := context.WithCancel(qs.core.ctx)
		qs.stopUsers = stop
		qs.users.start(ctx)
	}
	ringSeal := checkStoreSeal(filepath.Join(settings.Dir, ringStoreDirName), qs.config.Encryption.Mode)
	qs.core.ringSeal.Store(&ringSeal)
	if ringSeal.problem != "" {
		qs.logger.Warn("the ring store is configured to be sealed but its files do not show it", "problem", ringSeal.problem)
	}
	qs.logger.Info(qs.core.ringAtRestLine())
	loading := func() string { return ringLoadingReason(node.IsDiverged(), node.StateLoaded()) }
	qs.core.ringLoading.Store(&loading)
	passwordKey := node.PasswordKeyStatus
	qs.core.ringPasswordKey.Store(&passwordKey)
	qs.core.seedMetrics, err = newSeedingMetrics(node)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("register the seeding metrics: %w", err), node.Close())
	}
	if !qs.config.Cluster.Enabled && qs.config.TCP.Advertise != "" {
		qs.logger.Warn("tcp.advertise is set but this node is not in a ring cluster, so it is not published",
			"tcp_advertise", qs.config.TCP.Advertise, "fix", "set cluster.enabled: true (HOARDDB_CLUSTER_ENABLED) to publish it, or remove tcp.advertise")
	}
	ringWALMode(qs.config).logRingWALMode(qs.logger)
	if !qs.config.Cluster.Enabled {
		qs.logger.Info("entry-ring node open", "dir", filepath.Join(qs.config.Storage.DataDir, ringDirName))
		return handler, nil
	}
	mode := "bootstrap"
	if qs.ringJoin {
		mode = "join"
	}
	state := node.State()
	if notice, fix := ringStartNotice(qs.ringJoin, len(state.Members), state.StateSeq, qs.config.Server.NodeID, node.Identity().DataAddr); notice != "" {
		qs.logger.Warn(notice, "seeds", qs.config.HashRing.SeedNodes, "fix", fix)
	}
	qs.logger.Info("entry-ring node open", "id", qs.config.Server.NodeID, "data_addr", node.Identity().DataAddr, "mode", mode,
		"fingerprint", qs.core.fingerprint, "dir", filepath.Join(qs.config.Storage.DataDir, ringDirName))
	return handler, nil
}

// ringAtRestLine is the start-up line that says whether the ring store and log are sealed,
// from the keys the ring node was handed (ring encryption at rest spec §6).
func (s *Server) ringAtRestLine() string {
	if s.ringAtRest == nil {
		return "ring store and log are not encrypted (encryption.mode off)"
	}
	return fmt.Sprintf("ring store and log encrypted at rest (node master kid %s)", s.ringAtRest.nodeMaster.Kid())
}

// ringStartNotice is the warning a cluster start gives, and the fix it names, or two empty
// strings when there is nothing to say. A node that waits to be joined says so only while it
// holds no member row: a joined member restarts with its seeds still configured. A node that
// has just minted a cluster says so once, at its first state, so an operator who meant to
// join learns it before a second cluster exists.
func ringStartNotice(joining bool, members int, stateSeq int64, node, addr string) (notice, fix string) {
	switch {
	case joining && members == 0:
		return wire.AwaitingAdmissionNotice(node, addr)
	case !joining && stateSeq <= 1:
		return "this node started a new cluster",
			"if you meant to join an existing cluster, put the existing nodes in hash_ring.seed_nodes (HOARDDB_SEED_NODES) and restart with an empty data directory"
	}
	return "", ""
}

// prefixBloomLogFields names the eleven prefix bloom counters for the log line the node's close
// writes: per rule the reads that asked the filter, the tables they would have opened, the
// tables the filter left out, the tables built without the rule, and the filter entries added.
func prefixBloomLogFields(stats storage.PrefixBloomStats) []any {
	fields := make([]any, 0, 22)
	for _, rule := range []struct {
		name  string
		stats storage.PrefixBloomRuleStats
	}{{"document", stats.Document}, {"store", stats.Store}} {
		fields = append(fields,
			rule.name+"_iterators", rule.stats.Iterators,
			rule.name+"_candidates", rule.stats.Candidates,
			rule.name+"_skipped", rule.stats.Skipped,
			rule.name+"_unmarked", rule.stats.Unmarked,
			rule.name+"_prefix_hashes", rule.stats.PrefixHashes)
	}
	return append(fields, "refused", stats.Refused)
}

// closeRingNode closes the entry-ring node, if one is open.
func (qs *Service) closeRingNode() error {
	if qs.ringNode == nil {
		return nil
	}
	node := qs.ringNode
	qs.ringNode = nil
	// The root task reads the node and the follower: it ends before both.
	qs.stopRootTask()
	if qs.stopUsers != nil {
		// The follower reads the node: it ends before the node closes.
		qs.stopUsers()
		qs.users.wait()
		qs.users, qs.stopUsers = nil, nil
	}
	qs.core.ringLoading.Store(nil)
	qs.core.ringSeal.Store(nil)
	qs.core.ringPasswordKey.Store(nil)
	// The store's prefix bloom counters have no other way out of a running server (they are
	// internal: no metric, no statement), so a measurement reads them from this line.
	qs.logger.Info("entry-ring prefix bloom counters since the node opened", prefixBloomLogFields(node.PrefixBloomStats())...)
	if err := node.Close(); err != nil {
		return fmt.Errorf("close the entry-ring node: %w", err)
	}
	return nil
}

// Bounds of the host-sized Badger caches of the ring node. Badger's own defaults (128 MB of
// blocks, 64 MB of index) are smaller than the working set of a node with 100,000 entities,
// and a read that misses the block cache pays a checksum and a decompression: on a 4 vCPU,
// 15 GB node with that data set a 1 GB block cache cut the CPU per read-heavy operation by
// about a fifth.
const (
	ringCacheMinMB        = 128
	ringCacheMaxMB        = 1024
	ringCacheMemoryDivide = 16
)

// ringCacheSizesMB are the block and index cache sizes the ring node's Badger opens with.
// A size set in storage.badger_block_cache_mb or storage.badger_index_cache_mb is used
// as given. Otherwise the block cache is a sixteenth of the memory limit, between 128 MB
// and 1 GB, and the index cache half of that. A host whose limit cannot be read keeps
// Badger's defaults (0 here), so a small device is never given a large cache.
func ringCacheSizesMB(cfg *config.Config, limit func() (memoryLimit, error)) (blockMB, indexMB int) {
	blockMB, indexMB = cfg.Storage.BadgerBlockCacheMB, cfg.Storage.BadgerIndexCacheMB
	if blockMB != 0 && indexMB != 0 {
		return blockMB, indexMB
	}
	hostLimit, err := limit()
	if err != nil {
		return blockMB, indexMB
	}
	hostBlockMB := int(min(max(hostLimit.bytes>>20/ringCacheMemoryDivide, ringCacheMinMB), ringCacheMaxMB))
	if blockMB == 0 {
		blockMB = hostBlockMB
	}
	if indexMB == 0 {
		indexMB = hostBlockMB / 2
	}
	return blockMB, indexMB
}

// Diverged returns a channel closed when the entry-ring node records a divergence from the
// cluster, and a channel that never closes when no ring node is open.
func (qs *Service) Diverged() <-chan struct{} {
	if qs.ringNode == nil {
		return nil
	}
	return qs.ringNode.Diverged()
}
