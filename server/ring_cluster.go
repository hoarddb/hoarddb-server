package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	ringpeer "github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringPeerPlan is what cluster.enabled comes to before anything is bound: the pinned trust,
// the id-to-fingerprint map and whether this node starts a cluster or waits to be joined.
// It is built first so a refusal leaves no port bound and no directory created.
type ringPeerPlan struct {
	trust        *ringpeer.Trust
	fingerprints map[string]string
	join         bool
	listen       string
	advertise    string
}

// clusterListenAddr is the address the peer listener binds: cluster.listen, else server.listen.
func clusterListenAddr(cfg *config.Config) string {
	if cfg.Cluster.Listen != "" {
		return cfg.Cluster.Listen
	}
	if cfg.Server.Listen != "" {
		return cfg.Server.Listen
	}
	return "127.0.0.1:4433"
}

// clusterAdvertiseAddr is the address peers are told to dial: cluster.advertise, else the
// address the peer listener binds. A wildcard bind with no advertise is refused at config
// load, so the bound address is never committed as 0.0.0.0 by a validated config.
func clusterAdvertiseAddr(cfg *config.Config) string {
	if cfg.Cluster.Advertise != "" {
		return cfg.Cluster.Advertise
	}
	return clusterListenAddr(cfg)
}

// ClusterRootPromptAllowed reports whether a start of this cluster member may ask for a root
// password on a terminal (ring user store spec 6.5): only a data node that starts a new cluster
// (its seed list names no other node, the rule planRingPeers applies) and has no Raft state yet.
// A joiner's and a witness's administrator comes from the cluster, and a restarted member's is in
// its Raft state, so neither prompts, nor refuses to start for want of a password.
func ClusterRootPromptAllowed(cfg *config.Config) (bool, error) {
	if cfg.Cluster.Witness {
		return false, nil
	}
	listen, advertise := clusterListenAddr(cfg), clusterAdvertiseAddr(cfg)
	for _, entry := range cfg.HashRing.SeedNodes {
		member, err := parseMember(entry)
		if err != nil {
			return false, fmt.Errorf("hash_ring.seed_nodes entry %q: %w", entry, err)
		}
		if member.Addr != listen && member.Addr != advertise {
			return false, nil
		}
	}
	state, err := cluster.RaftStateOnDisk(filepath.Join(cfg.Storage.DataDir, ringDirName))
	if err != nil {
		return false, fmt.Errorf("check this node's cluster state before asking for a root password: %w", err)
	}
	return !state, nil
}

// planRingPeers reads hash_ring.seed_nodes and hash_ring.seed_fingerprints into the plan.
// selfFingerprint is this node's own key, which is admitted without being listed: a node
// must be able to admit itself.
//
// One rule decides bootstrap or join: a node bootstraps a new cluster when its seed list is
// empty or names only itself, and joins when it names any other node. So the first node lists
// nobody else, and every later node lists nodes that already exist.
//
// A seed_fingerprints entry is <node-id>=SHA256:<hex>, a bare SHA256:<hex> (trusted at the TLS
// layer, bound to no id) or <host:port>=SHA256:<hex> (a pin for that address, which also binds
// the seed listed at that address). Each seed other than this node needs an id@host:port form
// and a pin; a missing one is refused with the exact fix.
func planRingPeers(cfg *config.Config, selfFingerprint string, serverTLS *tls.Config) (*ringPeerPlan, error) {
	selfID := cfg.Server.NodeID
	selfFP, err := transport.ParseFingerprint(selfFingerprint)
	if err != nil {
		return nil, fmt.Errorf("reading this node's own fingerprint: %w", err)
	}
	plan := &ringPeerPlan{fingerprints: map[string]string{selfID: selfFP}, listen: clusterListenAddr(cfg), advertise: clusterAdvertiseAddr(cfg)}
	owner := map[string]string{selfFP: selfID}
	hostPins := map[string]string{}
	pins := []string{selfFP}
	for _, entry := range cfg.HashRing.SeedFingerprints {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, value, mapped := strings.Cut(entry, "=")
		if !mapped {
			key, value = "", entry
		}
		fp, err := transport.ParseFingerprint(value)
		if err != nil {
			return nil, fmt.Errorf("hash_ring.seed_fingerprints entry %q: %w", entry, err)
		}
		key = strings.TrimSpace(key)
		switch {
		case mapped && key == "":
			return nil, fmt.Errorf("hash_ring.seed_fingerprints entry %q has an empty node id before the '='. Write <node-id>=SHA256:<hex>, for example node-b=%s", entry, fp)
		case strings.Contains(key, ":"):
			hostPins[key] = fp
			pins = append(pins, key+"="+fp)
		case !mapped:
			pins = append(pins, fp)
		default:
			if err := plan.pinID(key, fp, selfID, owner); err != nil {
				return nil, err
			}
			pins = append(pins, fp)
		}
	}
	others, err := plan.readSeeds(cfg, hostPins, owner)
	if err != nil {
		return nil, err
	}
	plan.join = others > 0
	if cfg.Cluster.Witness && !plan.join {
		return nil, errors.New("cluster.witness is true but hash_ring.seed_nodes names no other node: a witness joins an existing cluster, it cannot start one. " +
			"List the cluster's nodes as <node-id>@<address> in hash_ring.seed_nodes, or set cluster.witness to false")
	}
	plan.trust, err = ringpeer.NewTrust(serverTLS, pins)
	if err != nil {
		return nil, fmt.Errorf("pin the peer fingerprints: %w", err)
	}
	return plan, nil
}

// pinID records fp as the key of node id, refusing a second key for the id and a key already
// pinned under another id: two ids on one key would let its holder vote as both.
func (plan *ringPeerPlan) pinID(id, fp, selfID string, owner map[string]string) error {
	if prior, dup := plan.fingerprints[id]; dup && prior != fp {
		return ringPinConflict(id, selfID, prior, fp)
	}
	if first, taken := owner[fp]; taken && first != id {
		return fmt.Errorf("hash_ring.seed_fingerprints pins %s to both %s and %s. One key belongs to one node: "+
			"keep the entry for the node that prints that fingerprint on its start-up line, then restart", fp, first, id)
	}
	plan.fingerprints[id] = fp
	owner[fp] = id
	return nil
}

// readSeeds classifies hash_ring.seed_nodes into this node and the others, binds each other
// seed to a pin, and returns how many others there are.
func (plan *ringPeerPlan) readSeeds(cfg *config.Config, hostPins map[string]string, owner map[string]string) (int, error) {
	selfID := cfg.Server.NodeID
	others := 0
	for _, entry := range cfg.HashRing.SeedNodes {
		member, err := parseMember(entry)
		if err != nil {
			return 0, fmt.Errorf("hash_ring.seed_nodes entry %q: %w", entry, err)
		}
		hasID := strings.Contains(entry, "@")
		atSelf := member.Addr == plan.advertise || member.Addr == plan.listen
		switch {
		case hasID && member.ID == selfID && !atSelf:
			return 0, fmt.Errorf("hash_ring.seed_nodes lists this node (%s) at %s, but this node's address is %s (cluster.advertise, else cluster.listen). "+
				"Write the seed with that address so the node knows it is itself, then restart", selfID, member.Addr, plan.advertise)
		case atSelf && hasID && member.ID != selfID:
			return 0, fmt.Errorf("hash_ring.seed_nodes lists %s at %s, which is this node's own address (%s is this node's id). "+
				"Correct the id or the address, then restart", member.ID, member.Addr, selfID)
		case atSelf:
			continue
		case !hasID:
			return 0, fmt.Errorf("seed %q names no node id, and the peer listener pins a key to a node id. "+
				"Write it as <node-id>@%s in hash_ring.seed_nodes on every node, then restart this node", entry, member.Addr)
		}
		others++
		if _, pinned := plan.fingerprints[member.ID]; pinned {
			continue
		}
		fp, ok := hostPins[member.Addr]
		if !ok {
			return 0, ringMissingPin(member.ID)
		}
		if err := plan.pinID(member.ID, fp, selfID, owner); err != nil {
			return 0, err
		}
	}
	return others, nil
}

// ringMissingPin is the refusal for a configured seed that has no fingerprint under its id.
func ringMissingPin(seedID string) error {
	return fmt.Errorf("no fingerprint is pinned for seed %q, so the peer listener cannot tell its key from a stranger's. "+
		"Add %s=SHA256:<%s's fingerprint> to hash_ring.seed_fingerprints (HOARDDB_SEED_FINGERPRINTS) on this node and on %s, then restart both. "+
		"%s prints its fingerprint on its start-up line. If you pinned it by address, write the address exactly as hash_ring.seed_nodes lists it (host:port=SHA256:<hex>)", seedID, seedID, seedID, seedID, seedID)
}

// ringPinConflict is the refusal for one node id pinned to two keys.
func ringPinConflict(id, selfID, first, second string) error {
	if id == selfID {
		return fmt.Errorf("hash_ring.seed_fingerprints pins %s to %s, but this node's own key is %s. "+
			"Correct that entry (this node prints its fingerprint on its start-up line), then restart", id, second, first)
	}
	return fmt.Errorf("hash_ring.seed_fingerprints pins %s to two keys, %s and %s. Keep the one %s prints on its start-up line, then restart", id, first, second, id)
}

// clusterOptions binds the peer listener and returns the options the node opens with. The
// listener is bound last, after every refusal that can be known without it.
func (plan *ringPeerPlan) clusterOptions(cfg *config.Config, onError func(error)) (*wire.ClusterOptions, net.Listener, error) {
	listener, err := net.Listen("tcp", plan.listen)
	if err != nil {
		return nil, nil, fmt.Errorf("bind the peer listener on %s (cluster.listen, else server.listen): %w. "+
			"Free that port or set cluster.listen to another address, then restart", plan.listen, err)
	}
	fingerprints := plan.fingerprints
	plan.trust.BindIDs(func(id string) (string, bool) { fp, ok := fingerprints[id]; return fp, ok })
	return &wire.ClusterOptions{
		Listener:     plan.trust.Listener(listener),
		TLS:          plan.trust.ClientConfigFor,
		Fingerprints: func(id string) (string, bool) { fp, ok := fingerprints[id]; return fp, ok },
		Join:         plan.join,
		// cluster.allow_membership false locks every membership statement on this node.
		MembershipLocked: !cfg.Cluster.MembershipAllowed(),
		WCTimeout:        time.Duration(cfg.Replication.WCTimeoutMS) * time.Millisecond,
		OnError:          onError,
		Seed: wire.SeedOptions{
			WindowBytes: cfg.Replication.SeedWindowBytes, ChunkBytes: cfg.Replication.SeedChunkBytes,
			ChunkEntries:     cfg.Replication.SeedChunkEntries,
			ResumeHold:       time.Duration(cfg.Replication.SeedResumeHoldSeconds) * time.Second,
			Idle:             time.Duration(cfg.Replication.SeedIdleSeconds) * time.Second,
			DiskHighWaterPct: cfg.Replication.SeedDiskHighWaterPct, IncomingMax: cfg.Replication.IncomingSeedsMax,
			BytesPerSecond: int64(cfg.Replication.SeedBytesPerSecond),
		},
	}, listener, nil
}
