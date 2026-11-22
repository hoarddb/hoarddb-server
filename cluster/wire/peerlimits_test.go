package wire

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestPeerLimitDefaultsAreTheConfigDefaults keeps this package's constants equal to the
// config package's, which it cannot import in its own code. [GUARD: change one default]
func TestPeerLimitDefaultsAreTheConfigDefaults(t *testing.T) {
	want := config.DefaultClusterConfig()
	got := PeerLimits{}.withDefaults()
	if got.MaxPending != want.PeerMaxPending || got.MaxConns != want.PeerMaxConns || got.MaxInFlight != want.PeerMaxInFlight ||
		got.Idle != time.Duration(want.PeerIdleSeconds)*time.Second ||
		got.FrameTimeout != time.Duration(want.PeerFrameTimeoutMS)*time.Millisecond ||
		got.RaftMaxConns != want.RaftMaxConns || got.RaftIdle != time.Duration(want.RaftIdleSeconds)*time.Second {
		t.Errorf("defaults %+v differ from config %+v", got, want)
	}
}

// TestPeerLimitsReachEachBound: every field lands in the bound it is named for, so a swap of
// two fields cannot go unseen.
func TestPeerLimitsReachEachBound(t *testing.T) {
	limits := PeerLimits{MaxPending: 1, MaxConns: 2, MaxInFlight: 3, Idle: 4 * time.Second, FrameTimeout: 5 * time.Second, RaftMaxConns: 6, RaftIdle: 7 * time.Second}
	if mux := limits.mux(); mux.MaxPending != 1 {
		t.Errorf("mux %+v, want MaxPending 1", mux)
	}
	if ch := limits.channel(); ch.MaxConns != 2 || ch.MaxInFlight != 3 || ch.Idle != 4*time.Second || ch.FrameTimeout != 5*time.Second {
		t.Errorf("channel %+v, want 2 connections, 3 in flight, 4 s idle, 5 s frame", ch)
	}
	if raft := limits.raft(); raft.MaxConns != 6 || raft.Idle != 7*time.Second {
		t.Errorf("raft %+v, want 6 connections and 7 s idle", raft)
	}
}

// TestEveryPeerKeyReachesItsLimit (pi F10): each of the seven cluster keys lands in the
// bound it names, so a swapped or forgotten key cannot go unseen. The server's ClusterOptions
// builder (C2) must call LimitsFromConfig. [GUARD: map a key to the wrong field]
func TestEveryPeerKeyReachesItsLimit(t *testing.T) {
	c := config.DefaultClusterConfig()
	c.PeerMaxPending, c.PeerMaxConns, c.PeerMaxInFlight = 11, 12, 13
	c.PeerIdleSeconds, c.PeerFrameTimeoutMS = 14, 15
	c.RaftMaxConns, c.RaftIdleSeconds = 16, 17
	limits := LimitsFromConfig(c)
	if limits.mux().MaxPending != 11 {
		t.Errorf("peer_max_pending reached %d", limits.mux().MaxPending)
	}
	if ch := limits.channel(); ch.MaxConns != 12 || ch.MaxInFlight != 13 || ch.Idle != 14*time.Second || ch.FrameTimeout != 15*time.Millisecond {
		t.Errorf("peer keys reached %+v", ch)
	}
	if raft := limits.raft(); raft.MaxConns != 16 || raft.Idle != 17*time.Second {
		t.Errorf("raft keys reached %+v", raft)
	}
	if got, want := LimitsFromConfig(config.DefaultClusterConfig()).withDefaults(), (PeerLimits{}).withDefaults(); got != want {
		t.Errorf("the config defaults give %+v, the package defaults %+v", got, want)
	}
}
