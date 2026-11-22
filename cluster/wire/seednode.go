package wire

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// newSeedSource builds the node's seed source over its committed topology, pair set and
// Badger. slots is the node's own record state, which decides whether it may be a source.
func (n *Node) newSeedSource(slots recordSlots) *SeedSource {
	opt := n.cl.opts.Seed
	if opt.DiskUsedPct == nil {
		opt.DiskUsedPct = func() (int, error) { return diskUsedPct(n.cfg.Dir) }
	}
	return NewSeedSource(SeedSourceConfig{
		Self: n.cfg.NodeID, Options: opt, Pairs: n.pairs, KV: n.kv,
		Committed: func() uint64 { return n.topology.Committed() },
		StateSeq:  func() uint64 { return uint64(n.raft.StateSeq()) },
		Database: func(db uint64) bool {
			_, found := n.topology.Database(db)
			return found
		},
		Owner: n.ownerOf,
		Holds: func(slot uint16, db uint64, node string) bool {
			info, found := n.topology.Database(db)
			if !found {
				return false
			}
			for _, member := range n.topology.Table().Copies(slot, info.RF) {
				if member.ID == node {
					return true
				}
			}
			return false
		},
		SlotState:     slots.State,
		RetiredIntact: slots.RetiredIntact,
		Deferred:      n.pairDeferred,
	})
}

// serveSeed answers the seed channel. The admission is the data and control channels' own
// (peer.Config.Admit): the pinned key bound to the member id, the cluster id, a committed
// member. The requester's id is the handshake's, never a field of the message.
func (n *Node) serveSeed(_ context.Context, from transport.Handshake, req transport.Message) (*transport.Message, error) {
	if n.cl == nil || !n.cl.serving.Load() || n.cl.seed == nil {
		return nil, errStarting
	}
	return n.cl.seed.Serve(from.NodeID, req)
}

// seedRegisterBound bounds the SEED_REGISTER exchange, the way linkCallTimeout bounds a
// replication call. A stream call is bounded by seed_idle_seconds instead: a source that is
// slow to read a chunk is not a source that is down.
const seedRegisterBound = linkCallTimeout

// dialSeed sends one seed-channel message to source and returns its reply, which is nil for
// an acknowledgement with no message. The peer network owns the connection, its dial and
// its backoff, so a source that stopped answering costs one dial per backoff period, not
// one per call.
func (n *Node) dialSeed(ctx context.Context, source string, msg transport.Message) (*transport.Message, error) {
	if n.cl == nil {
		return nil, fmt.Errorf("this node is not part of a cluster, so it has no seed source to dial")
	}
	bound := n.cl.opts.Seed.withDefaults().Idle
	if msg.Env.Kind == transport.KindSeedRegister {
		bound = seedRegisterBound
	}
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	reply, err := n.cl.links.callOn(ctx, transport.ChannelSeed, source, peer.LaneMain, msg.Env, msg.Body, 0)
	if err != nil {
		return nil, fmt.Errorf("calling the seed channel of %s: %w", source, err)
	}
	return reply, nil
}

// SeedCall is dialSeed for the seed pull and for tests that play a receiver: one request
// to source's seed channel and its reply.
func (n *Node) SeedCall(ctx context.Context, source string, msg transport.Message) (*transport.Message, error) {
	return n.dialSeed(ctx, source, msg)
}

// SeedPins is the number of seeds this node serves as a source now.
func (n *Node) SeedPins() int {
	if n.cl == nil || n.cl.seed == nil {
		return 0
	}
	return n.cl.seed.Pins()
}

// diskUsedPct is the percentage of the filesystem holding dir that is in use.
func diskUsedPct(dir string) (int, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, fmt.Errorf("reading the disk usage of %s: %w", dir, err)
	}
	// Bavail is int64 on freebsd, where it goes negative when the reserved
	// blocks are gone; converting it raw would report an empty disk.
	bavail := uint64(0)
	if stat.Bavail > 0 {
		bavail = uint64(stat.Bavail)
	}
	return usedPct(uint64(stat.Blocks), bavail), nil
}

// usedPct is the share of blocks in use. A filesystem reporting no blocks is
// 0%, and one reporting more free blocks than it has blocks is 100%: that
// reading is not a filesystem but a wrapped negative Bavail, and a copy must
// refuse to seed into a disk it cannot measure rather than treat it as empty.
func usedPct(blocks, bavail uint64) int {
	if blocks == 0 || bavail == blocks {
		return 0
	}
	if bavail > blocks {
		return 100
	}
	return int((blocks - bavail) * 100 / blocks)
}

// PairDump returns the pair's applied position and every key and value of its range, read
// from one snapshot taken the way a seed source takes its cut. It exists for tests that
// compare a seed stream with what the source holds; it reads the whole range into memory.
func (n *Node) PairDump(slot uint16, db uint64) (cluster.Position, []transport.SeedEntry, error) {
	pin := &seedPin{slot: slot, db: db}
	held := false
	n.pairs.WithHeld(slot, db, func(pair *cluster.Pair) {
		held = true
		pin.snapshot = n.kv.NewSnapshot()
		pin.cut = pair.AppliedPos
	})
	if !held {
		return cluster.Position{}, nil, fmt.Errorf("this node holds no pair for slot %d of database %d, so there is no range to dump", slot, db)
	}
	defer pin.snapshot.Discard()
	prefix := storage.PairPrefix(slot, db)
	iter := pin.snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	var entries []transport.SeedEntry
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		if err != nil {
			return cluster.Position{}, nil, fmt.Errorf("reading the value of key %x: %w", iter.Key(), err)
		}
		entries = append(entries, transport.SeedEntry{Key: append([]byte(nil), iter.Key()...), Value: value})
	}
	if err := iter.Err(); err != nil {
		return cluster.Position{}, nil, fmt.Errorf("dumping slot %d of database %d: %w", slot, db, err)
	}
	return pin.cut, entries, nil
}
