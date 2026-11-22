package wire

import (
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// PeerLimits are the bounds on what the internode port holds for callers: the cluster.peer_*
// and cluster.raft_max_conns / raft_idle_seconds keys. A zero field takes the default,
// which the config package repeats and a test keeps equal. Nothing here is per bucket,
// per database or per peer: the cost is one counter per channel.
type PeerLimits struct {
	// MaxPending is cluster.peer_max_pending.
	MaxPending int
	// MaxConns and MaxInFlight are cluster.peer_max_conns and peer_max_in_flight, for each
	// of the data and control channels.
	MaxConns, MaxInFlight int
	// Idle is cluster.peer_idle_seconds; FrameTimeout is cluster.peer_frame_timeout_ms.
	Idle, FrameTimeout time.Duration
	// RaftMaxConns and RaftIdle are cluster.raft_max_conns and raft_idle_seconds.
	RaftMaxConns int
	RaftIdle     time.Duration
}

const (
	peerDefaultMaxPending   = 128
	peerDefaultMaxConns     = 256
	peerDefaultMaxInFlight  = 256
	peerDefaultIdle         = 120 * time.Second
	peerDefaultFrameTimeout = 10 * time.Second
	peerDefaultRaftMaxConns = 64
	peerDefaultRaftIdle     = 120 * time.Second
)

func (l PeerLimits) withDefaults() PeerLimits {
	if l.MaxPending <= 0 {
		l.MaxPending = peerDefaultMaxPending
	}
	if l.MaxConns <= 0 {
		l.MaxConns = peerDefaultMaxConns
	}
	if l.MaxInFlight <= 0 {
		l.MaxInFlight = peerDefaultMaxInFlight
	}
	if l.Idle <= 0 {
		l.Idle = peerDefaultIdle
	}
	if l.FrameTimeout <= 0 {
		l.FrameTimeout = peerDefaultFrameTimeout
	}
	if l.RaftMaxConns <= 0 {
		l.RaftMaxConns = peerDefaultRaftMaxConns
	}
	if l.RaftIdle <= 0 {
		l.RaftIdle = peerDefaultRaftIdle
	}
	return l
}

// mux is the pre-routing bound of the shared port.
func (l PeerLimits) mux() transport.PeerMuxLimits {
	return transport.PeerMuxLimits{MaxPending: l.withDefaults().MaxPending}
}

// channel is the bound of one data or control channel server.
func (l PeerLimits) channel() transport.ChannelLimits {
	l = l.withDefaults()
	return transport.ChannelLimits{MaxConns: l.MaxConns, Idle: l.Idle, MaxInFlight: l.MaxInFlight, FrameTimeout: l.FrameTimeout}
}

// raft is the bound of the Raft channel.
func (l PeerLimits) raft() cluster.RaftLimits {
	l = l.withDefaults()
	return cluster.RaftLimits{MaxConns: l.RaftMaxConns, Idle: l.RaftIdle}
}

// LimitsFromConfig reads the cluster.peer_* and cluster.raft_max_conns / raft_idle_seconds
// keys. The server builds ClusterOptions.Limits with it, so a key an operator sets reaches the
// listener; a zero key takes the default here, as it does in withDefaults.
func LimitsFromConfig(c config.ClusterConfig) PeerLimits {
	return PeerLimits{
		MaxPending: c.PeerMaxPending, MaxConns: c.PeerMaxConns, MaxInFlight: c.PeerMaxInFlight,
		Idle:         time.Duration(c.PeerIdleSeconds) * time.Second,
		FrameTimeout: time.Duration(c.PeerFrameTimeoutMS) * time.Millisecond,
		RaftMaxConns: c.RaftMaxConns, RaftIdle: time.Duration(c.RaftIdleSeconds) * time.Second,
	}
}
