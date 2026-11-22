package cluster

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// joinNonvoter opens a node that was told to join and registers it through the leader, on a
// listener the test can bind again after a restart.
func joinNonvoter(t *testing.T, leader *Raft, id string) (*Raft, RaftConfig) {
	t.Helper()
	cfg := quickRaft(t, t.TempDir(), id)
	cfg.Bootstrap = false
	cfg.DataAddr = cfg.Listener.Addr().String()
	node := openOrFail(t, cfg)
	if _, err := leader.Join(JoinRequest{
		Member:     Member{ID: id, Incarnation: 1, DataAddr: cfg.DataAddr},
		RaftAddr:   cfg.DataAddr,
		PlacingSet: make([]byte, 1024),
	}); err != nil {
		t.Fatalf("join %s: %v", id, err)
	}
	return node, cfg
}

// TestAConsistentClusterNeverFiresATrigger is the property test of the detection rule (spec test
// 7): a leader and two followers on one build, commands drawn at random (valid, refused by the
// FSM's own rules, stamped fresh, stale and ahead), proposed concurrently with a restamp after
// STATE_MOVED, snapshots taken at random points and a follower restarted from its snapshot. The
// real ProposeContext dry run sits in front of the log, so a refusal is returned to the proposer
// and never committed, and no node records a divergence.
// Break one: append one command stamped StateSeq+1 by a raw append (r.raft.Apply), bypassing the
// dry run; every node then sees an ahead stamp and records a divergence. Break two: remove the
// dry run; a restamped stale CREATE_DATABASE id then commits and every node refuses it CONFLICT.
func TestAConsistentClusterNeverFiresATrigger(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	var rngMu sync.Mutex
	pick := func(n int) int { rngMu.Lock(); defer rngMu.Unlock(); return rng.IntN(n) }

	leader := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second, _ := joinNonvoter(t, leader, "n2")
	third, thirdCfg := joinNonvoter(t, leader, "n3")
	nodes := []*Raft{leader, second, third}

	names := []string{"a", "b", "c", "d", "e"}
	build := func() Command {
		state := leader.State()
		stamp := uint64(state.StateSeq)
		switch pick(4) {
		case 0:
			stamp -= uint64(pick(3)) // stale: something else committed first
		case 1:
			stamp += uint64(1 + pick(2)) // ahead: a proposer reading a state this node has not applied
		}
		name := names[pick(len(names))]
		id := state.NextDatabaseID
		if pick(4) == 0 {
			id = int64(1 + pick(6)) // a taken or far id: CONFLICT at the dry run
		}
		if pick(3) == 0 && len(state.Databases) > 0 {
			db := state.Databases[pick(len(state.Databases))]
			return Command{Version: CommandVersion, Kind: CommandDropDatabase, BaseSeq: stamp, Body: fsmDoc(fsmI64("id", db.ID), fsmI64("schema_version", db.SchemaVersion))}
		}
		return Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp, Body: fsmCreateDatabaseBody(name, id, 1)}
	}

	var workers sync.WaitGroup
	var accepted, refused, moved int
	var tally sync.Mutex
	for range 4 {
		workers.Go(func() {
			for range 25 {
				var err error
				for range 3 { // the restamp loop of applyAsLeader and Raft.propose
					cmd := build()
					_, err = leader.ProposeContext(context.Background(), cmd)
					var refusal *Error
					if !errors.As(err, &refusal) || refusal.Code != CodeStateMoved {
						break
					}
					tally.Lock()
					moved++
					tally.Unlock()
				}
				tally.Lock()
				if err == nil {
					accepted++
				} else {
					refused++
				}
				tally.Unlock()
				if pick(8) == 0 {
					// The library refuses a snapshot it cannot take yet (a configuration entry
					// not applied, one taken at the same index); that is not what is tested.
					if err := nodes[pick(len(nodes))].TakeSnapshot(); err != nil {
						t.Logf("snapshot not taken: %v", err)
					}
				}
			}
		})
	}
	workers.Wait()
	t.Logf("accepted %d, refused to the proposer %d, STATE_MOVED restamps %d", accepted, refused, moved)
	if accepted == 0 || refused == 0 {
		t.Fatalf("the draw exercised accepted=%d refused=%d: the property is not tested", accepted, refused)
	}

	converge := func(nodes ...*Raft) {
		t.Helper()
		want := leader.State().StateSeq
		deadline := time.Now().Add(10 * time.Second)
		for _, n := range nodes {
			for n.State().StateSeq != want && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if got := n.State().StateSeq; got != want {
				t.Fatalf("a follower stopped at state_seq %d, the leader is at %d", got, want)
			}
		}
	}
	converge(second, third)

	// A follower restarts from its own snapshot and the retained log.
	if err := third.TakeSnapshot(); err != nil && !errors.Is(err, hraft.ErrNothingNewToSnapshot) {
		t.Fatal(err)
	}
	addr := thirdCfg.Listener.Addr().String()
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("the follower's port was taken before it could restart: %v", err)
	}
	thirdCfg.Listener = listener
	third = openOrFail(t, thirdCfg)
	nodes[2] = third
	converge(second, third)

	for i, n := range nodes {
		if record, flagged := n.Divergence(); flagged {
			t.Errorf("node n%d recorded a divergence on a consistent cluster: %+v", i+1, record)
		}
	}
}

// TestLeaderChangesNeverFireATrigger is the leader-change half of the property test (G1 review
// MINOR-5): three voters, concurrent proposals through whichever node leads, stamped from the
// applied state of any node (a follower can be ahead of a new leader), repeated leadership
// transfers and periodic snapshots. The dry run's barrier path and the ahead-stamp mapping are
// what keep a healthy cluster from recording a divergence. Break: remove the dry run from
// ProposeContext (a stale-stamped refusal then commits and every node records it).
func TestLeaderChangesNeverFireATrigger(t *testing.T) {
	t.Skip("BUG-0887: Raft.Close can hang in hashicorp/raft v1.8.0 when a pipelined replication stream stops decoding under leadership changes; skipped during the feature freeze (owner 2026-11-21), to be fixed after release")
	first := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	second := c5Join(t, first, "n2")
	third := c5Join(t, first, "n3")
	if _, err := first.Promote("n2"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Promote("n3"); err != nil {
		t.Fatal(err)
	}
	nodes := []*Raft{first, second, third}
	leaderOf := func() *Raft {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		return nil
	}
	var stop atomic.Bool
	var accepted, refused, notleader, moved atomic.Int64
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for !stop.Load() {
				l := leaderOf()
				if l == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				// stamp from a random node's applied state: followers can be ahead of a new leader
				src := nodes[rng.IntN(3)]
				st := src.State()
				var cmd Command
				if rng.IntN(3) == 0 && len(st.Databases) > 0 {
					db := st.Databases[rng.IntN(len(st.Databases))]
					cmd = Command{Version: CommandVersion, Kind: CommandDropDatabase, BaseSeq: uint64(st.StateSeq), Body: fsmDoc(fsmI64("id", db.ID), fsmI64("schema_version", db.SchemaVersion))}
				} else {
					id := st.NextDatabaseID
					if rng.IntN(4) == 0 {
						id = int64(1 + rng.IntN(8))
					}
					cmd = Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq), Body: fsmCreateDatabaseBody([]string{"a", "b", "c", "d"}[rng.IntN(4)], id, 1)}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := l.ProposeContext(ctx, cmd)
				cancel()
				var nl *NotLeaderError
				var ref *Error
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.As(err, &nl):
					notleader.Add(1)
				case errors.As(err, &ref) && ref.Code == CodeStateMoved:
					moved.Add(1)
				default:
					refused.Add(1)
				}
			}
		})
	}
	for i := range 12 {
		time.Sleep(60 * time.Millisecond)
		if l := leaderOf(); l != nil {
			_ = l.raft.LeadershipTransfer().Error()
		}
		if i%3 == 0 {
			_ = nodes[i%3].TakeSnapshot()
		}
	}
	stop.Store(true)
	wg.Wait()
	t.Logf("accepted %d refused %d notleader %d moved %d", accepted.Load(), refused.Load(), notleader.Load(), moved.Load())
	time.Sleep(500 * time.Millisecond)
	for i, n := range nodes {
		if rec, ok := n.Divergence(); ok {
			t.Errorf("n%d diverged on a healthy cluster: %+v", i+1, rec)
		}
	}
}
