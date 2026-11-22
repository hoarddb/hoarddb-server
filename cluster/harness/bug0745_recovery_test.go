package harness_test

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestALostAckIsRecoveredAndTheInsertStillConfirmsOnTheCopy (BUG-0745): the WRITE reply is only a
// receipt, so the copy's acknowledgement is its own call back to the owner and can be lost
// without anyone seeing an error on the send. The copy refuses its first dial back to the owner;
// the record is durable and applied on the copy but never confirmed, and the owner's ack
// deadline ships it again, the copy acknowledges the duplicate at its applied position, and the
// insert ends applied with the copy counted. [GUARD: no ack deadline]
func TestALostAckIsRecoveredAndTheInsertStillConfirmsOnTheCopy(t *testing.T) {
	c := proofSetup(t, harness.Options{WCTimeout: 40 * time.Second})
	id, owner, copyNode, _ := proofID(t, c, "lostack")
	c.DropChannel(copyNode, owner, transport.ChannelData, 1)
	start := time.Now()
	got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("insert through the owner %s: %v (background %v)", owner, err, c.BackgroundErrors())
	}
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("outcome %v copies_confirmed %d, want applied and 2 (background %v)", got.Outcome, got.CopiesConfirmed, c.BackgroundErrors())
	}
	if elapsed := time.Since(start); elapsed < 3*time.Second {
		t.Fatalf("the insert confirmed in %s: the copy's ack was not lost, so the ack deadline was not exercised", elapsed)
	}
	if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
		t.Fatalf("the copy %s does not hold %s: found %v err %v", copyNode, id, found, err)
	}
}

// TestACopyThatRestartsAfterApplyingBeforeAckingIsConfirmedByTheDuplicate (BUG-0745): the copy
// applies the record and tries its ack, the ack fails (the harness records the attempt and fails
// it), and the copy restarts. The insert stays unconfirmed until the restarted copy, which holds
// the record in its log and Badger, answers the owner's duplicate with its applied position; the
// copy is then counted exactly once. The test fails if the copy is not restarted (the second
// incarnation's ack attempt is asserted), and the insert cannot confirm before the restart (it is
// checked while the acks still fail). [GUARD: break the duplicate path or remove the restart]
func TestACopyThatRestartsAfterApplyingBeforeAckingIsConfirmedByTheDuplicate(t *testing.T) {
	c := proofSetup(t, harness.Options{WCTimeout: 40 * time.Second})
	id, owner, copyNode, _ := proofID(t, c, "restart")
	c.FailAcks(copyNode, true)
	type answer struct {
		copies  int
		outcome string
		err     error
	}
	done := make(chan answer, 1)
	go func() {
		got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
		done <- answer{copies: int(got.CopiesConfirmed), outcome: got.Outcome.String(), err: err}
	}()
	c.Await("the copy applied the record and tried its ack", 20*time.Second, func() bool {
		_, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
		return err == nil && found && c.AckAttempts(copyNode) >= 1
	})
	select {
	case got := <-done:
		t.Fatalf("the insert answered %+v while the copy's ack was failing", got)
	default:
	}
	attemptsBefore := c.AckAttempts(copyNode)
	first := c.Node(copyNode)
	c.Kill(copyNode)
	c.FailAcks(copyNode, false)
	c.Restart(copyNode)
	if c.Node(copyNode) == first {
		t.Fatal("the copy was not restarted")
	}
	select {
	case got := <-done:
		if got.err != nil || got.outcome != machine.OutcomeApplied.String() || got.copies != 2 {
			t.Fatalf("insert: outcome %s copies_confirmed %d err %v, want applied and 2 (background %v)", got.outcome, got.copies, got.err, c.BackgroundErrors())
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("the insert never confirmed after the copy restarted (background %v)", c.BackgroundErrors())
	}
	if c.AckAttempts(copyNode) <= attemptsBefore {
		t.Fatal("the restarted copy never acknowledged the duplicate")
	}
	if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
		t.Fatalf("the restarted copy lost %s: found %v err %v", id, found, err)
	}
	for _, msg := range c.BackgroundErrors() {
		if strings.Contains(msg, "panic") {
			t.Fatalf("background: %s", msg)
		}
	}
}
