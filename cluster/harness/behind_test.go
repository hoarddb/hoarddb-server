package harness_test

import (
	"github.com/hoarddb/hoarddb-server/cluster"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAFrameFromALaterEpochStopsTheReceiverServingUntilTheLeaderConfirms: conventions
// section 7 says a frame naming a later epoch marks the receiver behind and it stops serving
// at once, before the FSM applies the change. A frame from a member names an epoch the
// cluster has not reached, which is a forgery or a bug, and the receiver must not stay
// silent for it: the leader's next lease grant says the cluster is not ahead, and the mark
// ends. [GUARD: skip the mark; skip the confirmation]
func TestAFrameFromALaterEpochStopsTheReceiverServingUntilTheLeaderConfirms(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2)
	if err != nil {
		t.Fatal(err)
	}
	state := c.AwaitConverged(10 * time.Second)
	bucketID, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"})
	if err != nil {
		t.Fatal(err)
	}
	c.AwaitLeases(10 * time.Second)
	encID, err := store.EncodeComponent("probe")
	if err != nil {
		t.Fatal(err)
	}
	body, err := wire.EncodeForwardReadBody(wire.ForwardReadBody{Req: wire.ReadRequest{BucketID: bucketID, ID: "probe", EncID: encID}})
	if err != nil {
		t.Fatal(err)
	}
	ids := c.IDs()
	from, victim := ids[0], ids[1]
	ahead := transport.Message{Env: transport.Envelope{Version: transport.EnvelopeVersion, Kind: transport.KindForwardRead, DBID: dbID, Epoch: uint64(state.Epoch) + 5}, Body: body}

	sawBehind := false
	for attempt := 0; attempt < 6 && !sawBehind; attempt++ {
		_, _ = c.CallAs(from, victim, transport.ChannelData, ahead) // refused as EPOCH_FENCED; the refusal is not the point
		sawBehind = !c.Node(victim).LeaseValid()
		if !sawBehind {
			c.AwaitLeases(10 * time.Second)
		}
	}
	if !sawBehind {
		t.Fatal("a frame naming a later epoch did not stop the receiver serving")
	}
	c.Await("the leader's lease grant to clear a mark above the cluster's epoch", 10*time.Second, func() bool {
		return c.Node(victim).LeaseValid()
	})
}

// TestALeaderLostWithTheFailingMemberStillGetsTheMemberDeclaredDown: D1. Five voters. A
// member and the leader are lost together, so the reports that the observers made to the
// leader died with it. The new leader has seen none of them, and the detector returns each
// report once per run; the supervisor must say them again or the member is never marked down.
func TestALeaderLostWithTheFailingMemberStillGetsTheMemberDeclaredDown(t *testing.T) {
	c := harness.NewCluster(t, 5, harness.Options{FastDetection: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	subject := "n5"
	if leaderID == subject {
		subject = "n4"
	}
	var survivor string
	for _, id := range c.IDs() {
		if id != leaderID && id != subject {
			survivor = id
			break
		}
	}
	epoch := c.Node(survivor).State().Epoch
	c.Kill(subject)
	// Long enough for the observers' first failed probes and their reports to leave for the
	// leader, short of the leader committing the member down.
	time.Sleep(250 * time.Millisecond)
	c.Kill(leaderID)
	c.Await("a new leader to be elected", 20*time.Second, func() bool {
		id := c.Node(survivor).LeaderID()
		return id != "" && id != leaderID
	})
	c.Await("the new leader to commit the failing member down", 40*time.Second, func() bool {
		for _, member := range c.Node(survivor).State().Members {
			if member.ID == subject && member.State == cluster.MemberDown {
				return true
			}
		}
		return false
	})
	if c.Node(survivor).State().Epoch <= epoch {
		t.Fatalf("the epoch did not move: %d", c.Node(survivor).State().Epoch)
	}
}
