package wire

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// catchUpPinNode is a source node with a topology in which "j" is a joining gainer (in no
// slot ranking), a seed source that has served (or not) a pin, and the smallest Node that
// reaches mayCatchUp and serveCatchUp.
type catchUpPinNode struct {
	t    *testing.T
	node *Node
	rig  *t2Rig
}

func newCatchUpPinNode(t *testing.T) *catchUpPinNode {
	t.Helper()
	state := cluster.ClusterState{Epoch: 3, StateSeq: 9, Members: []cluster.Member{
		{ID: "n1", State: cluster.MemberActive}, {ID: "n2", State: cluster.MemberActive}, {ID: "j", State: cluster.MemberJoining}, {ID: "k", State: cluster.MemberJoining},
	}, Databases: []cluster.Database{{Name: "app", ID: int64(t2DB), RF: 2}, {Name: "other", ID: int64(t2DB) + 1, RF: 2}}}
	rig := newT2Rig(t, SeedOptions{})
	source := newU32Node(t, "n1", nil)
	node := &Node{
		cfg: NodeConfig{NodeID: "n1"}, topology: NewClusterTopology("n1", c5Source{state: state}), handoff: &Handoff{},
		cl: &clusterParts{repl: source.rep, seed: rig.src},
	}
	return &catchUpPinNode{t: t, node: node, rig: rig}
}

// pin registers a seed of (slot, db) for requester and returns its seed id.
func (c *catchUpPinNode) pin(requester string, slot uint16, db uint64, id byte) [16]byte {
	c.t.Helper()
	seed := t2ID(id)
	msg := t2Message(transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: c.rig.epoch.Load(), Slot: slot, DBID: db}, SeedID: seed}))
	reply, err := c.rig.src.Serve(requester, msg)
	if err != nil || reply.Env.Kind != transport.KindSeedAccept {
		c.t.Fatalf("SEED_REGISTER of %s for slot %d database %d: kind 0x%02x, %v", requester, slot, db, reply.Env.Kind, err)
	}
	return seed
}

func (c *catchUpPinNode) may(requester string, slot uint16, db uint64) bool {
	return c.node.mayCatchUp(slot, db, requester)
}

// refusal sends CATCHUP_REQ through serveCatchUp and returns the error, which is the
// epoch-fenced refusal when the request is not allowed.
func (c *catchUpPinNode) refusal(requester string, slot uint16, db uint64) error {
	c.t.Helper()
	env := transport.Envelope{Epoch: 3, Slot: slot, DBID: db}
	_, err := c.node.serveCatchUp(c.t.Context(), transport.Handshake{NodeID: requester}, env, EncodeCatchUpReqBody(CatchUpRequest{}))
	return err
}

func (c *catchUpPinNode) wantFenced(requester string, slot uint16, db uint64) {
	c.t.Helper()
	var refusal *cluster.Error
	if err := c.refusal(requester, slot, db); !errors.As(err, &refusal) || refusal.Code != cluster.CodeEpochFenced {
		c.t.Fatalf("CATCHUP_REQ from %s for slot %d database %d: error %v, want the epoch-fenced refusal", requester, slot, db, err)
	}
}

// TestAPinHolderMayCatchUpItsPair: the closing round of a seed asks its source for
// CATCHUP_REQ, and the requester is a joining gainer that is in no copy set.
// [GUARD: delete the Holds condition]
func TestAPinHolderMayCatchUpItsPair(t *testing.T) {
	c := newCatchUpPinNode(t)
	if catchUpEligible(c.node.topology.Table(), nil, 2, 2, t2Slot, "j") {
		t.Fatalf("the fixture is wrong: j is in the copy set of slot %d", t2Slot)
	}
	c.pin("j", t2Slot, t2DB, 1)
	if !c.may("j", t2Slot, t2DB) {
		t.Fatalf("the registered receiver of a live pin was refused CATCHUP_REQ for its own pair")
	}
}

// TestANodeWithoutALivePinIsStillRefusedCatchUp: the same joining node, with no pin, gets
// the refusal. [GUARD: return true when Holds is false]
func TestANodeWithoutALivePinIsStillRefusedCatchUp(t *testing.T) {
	c := newCatchUpPinNode(t)
	if c.may("j", t2Slot, t2DB) {
		t.Fatalf("a joining node with no pin may catch up")
	}
	c.wantFenced("j", t2Slot, t2DB)
}

// TestAReleasedPinNoLongerServesCatchUp: a pin that was dropped, and a pin that is closed
// but still in the map for an instant, both stop serving. [GUARD: ignore the closed flag]
func TestAReleasedPinNoLongerServesCatchUp(t *testing.T) {
	c := newCatchUpPinNode(t)
	seed := c.pin("j", t2Slot, t2DB, 1)
	if !c.may("j", t2Slot, t2DB) {
		t.Fatalf("the fixture is wrong: the live pin does not serve")
	}
	c.rig.src.mu.Lock()
	pin := c.rig.src.pins[seed]
	c.rig.src.mu.Unlock()
	pin.release()
	if c.may("j", t2Slot, t2DB) {
		t.Fatalf("a pin that is closed but still in the map serves CATCHUP_REQ")
	}
	c.wantFenced("j", t2Slot, t2DB)

	c.rig.src.dropPin(seed)
	if c.may("j", t2Slot, t2DB) {
		t.Fatalf("a dropped pin serves CATCHUP_REQ")
	}
	c.wantFenced("j", t2Slot, t2DB)
}

// TestAPinForAnotherPairDoesNotServeCatchUp: the pin names one slot, one database and one
// receiver, and the request must match all three. [GUARD: compare only the requester]
func TestAPinForAnotherPairDoesNotServeCatchUp(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requester string
		slot      uint16
		db        uint64
	}{
		{"another slot", "j", t2Slot + 1, t2DB},
		{"another database", "j", t2Slot, t2DB + 1},
		{"another requester", "k", t2Slot, t2DB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCatchUpPinNode(t)
			c.pin(tc.requester, tc.slot, tc.db, 1)
			if c.may("j", t2Slot, t2DB) {
				t.Fatalf("a pin of %s for slot %d database %d serves j for slot %d database %d", tc.requester, tc.slot, tc.db, t2Slot, t2DB)
			}
			c.wantFenced("j", t2Slot, t2DB)
		})
	}
}

// TestASourceThatNoLongerStandsStopsServingItsPin: the source reset its own range or lost the
// copy while a receiver's pin is live. Its tail has regressed, so the pinned answer would call
// the receiver current at a position the source dropped; a source that is not a ready copy of
// the pair serves nobody its pin, the same standing it needs to register one.
// [BREAK: ignore the standing check in Holds]
func TestASourceThatNoLongerStandsStopsServingItsPin(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state machine.SlotState
	}{
		{"a source that reset its own range", machine.SlotSeeding},
		{"a source that lost the copy", machine.SlotAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCatchUpPinNode(t)
			c.pin("j", t2Slot, t2DB, 1)
			req := CatchUpRequest{Slot: t2Slot, DB: t2DB, Node: "j", From: u32Pos(u32Epoch, 1)}
			chunk, err := c.node.answerCatchUp(t.Context(), req)
			if err != nil || chunk.Divergent || !current(chunk) {
				t.Fatalf("the fixture is wrong: a standing source's closing round answered %+v, %v, want current", chunk, err)
			}
			c.rig.mu.Lock()
			c.rig.state = tc.state
			c.rig.mu.Unlock()
			if c.rig.src.Holds(t2Slot, t2DB, "j") || c.may("j", t2Slot, t2DB) {
				t.Fatalf("a source in state %d still serves the pin of j", tc.state)
			}
			c.wantFenced("j", t2Slot, t2DB)
			if chunk, err = c.node.answerCatchUp(t.Context(), req); err == nil && current(chunk) && !chunk.Divergent {
				t.Fatalf("the pinned answer called the receiver current from a source that does not stand: %+v", chunk)
			}
		})
	}
}

// TestAPinOutlivesNeitherItsSourcesRoleNorItsEpoch: after the epoch moves, a source that is no
// longer the owner it registered as stops serving the pin, as the seed ack path retires it.
// [BREAK: ignore sourceStands in Holds]
func TestAPinOutlivesNeitherItsSourcesRoleNorItsEpoch(t *testing.T) {
	c := newCatchUpPinNode(t)
	c.pin("j", t2Slot, t2DB, 1)
	c.rig.epoch.Store(c.rig.epoch.Load() + 1)
	if !c.rig.src.Holds(t2Slot, t2DB, "j") {
		t.Fatal("the fixture is wrong: a source that is still the owner after the epoch moved stopped serving its pin")
	}
	c.rig.mu.Lock()
	c.rig.owner = "n2"
	c.rig.mu.Unlock()
	if c.rig.src.Holds(t2Slot, t2DB, "j") {
		t.Fatal("a source that is no longer the owner it registered as still serves the pin")
	}
}

// TestACatchUpRequestKeepsItsPinAlive: a closing round that outlasts the resume hold must not
// lose its pin; each CATCHUP_REQ the receiver sends counts as activity, as its SEED_ACK does.
// [BREAK: serve the request without touching the pin]
func TestACatchUpRequestKeepsItsPinAlive(t *testing.T) {
	c := newCatchUpPinNode(t)
	c.pin("j", t2Slot, t2DB, 1)
	hold := c.rig.src.opt.ResumeHold
	c.rig.advance(hold * 2 / 3)
	req := CatchUpRequest{Slot: t2Slot, DB: t2DB, Node: "j", From: u32Pos(u32Epoch, 1)}
	if _, err := c.node.answerCatchUp(t.Context(), req); err != nil {
		t.Fatalf("the pin holder's CATCHUP_REQ was refused: %v", err)
	}
	c.rig.advance(hold * 2 / 3)
	c.rig.src.sweep()
	if !c.rig.src.Holds(t2Slot, t2DB, "j") {
		t.Fatalf("the pin was swept %v after its last SEED_ACK although the receiver asked for a catch-up %v ago", hold*4/3, hold*2/3)
	}
}
