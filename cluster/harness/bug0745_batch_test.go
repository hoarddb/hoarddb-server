package harness_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAckBatchesConfirmEveryWriteOnBothCopies (BUG-0745): 32 inserts on 32 pairs of one
// owner and one slowed copy are each confirmed by both copies, and the copy sent fewer ack
// calls than it acknowledged records, so acknowledgements of several pairs travelled in one
// WRITE_ACK_BATCH, and the owner made fewer write calls than it shipped records, so the records
// of several pairs travelled in one WRITE_BATCH.
// [GUARD: hide AckBatch or WriteBatch behind the harness wrapper]
func TestAckBatchesConfirmEveryWriteOnBothCopies(t *testing.T) {
	const pairs = 32
	c := proofSetup(t, harness.Options{WCTimeout: 10 * time.Second})
	_, owner, copyNode, _ := proofID(t, c, "bug0745-batch")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "bug0745-batch-id", pairs)
	// The slow append holds the copy's acknowledgements back long enough for the ack lane to
	// find several of them waiting when it makes its next call.
	c.SlowDisk(copyNode, 50*time.Millisecond)
	t.Cleanup(func() { c.SlowDisk(copyNode, 0) })

	var wg sync.WaitGroup
	errs := make(chan error, pairs)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
			if err != nil {
				errs <- err
				return
			}
			if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
				errs <- &notConfirmed{id: id, outcome: got.Outcome.String(), copies: int(got.CopiesConfirmed)}
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("insert through the owner: %v (background %v)", err, c.BackgroundErrors())
	}
	for _, id := range ids {
		if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
			t.Errorf("the copy %s does not hold %s: found %v err %v", copyNode, id, found, err)
		}
	}
	counts := c.StreamCounts(copyNode)
	t.Logf("the copy sent %d ack calls for %d acknowledged records", counts.AckCalls, counts.AckEntries)
	if counts.AckEntries < pairs {
		t.Fatalf("the copy acknowledged %d records, want at least %d", counts.AckEntries, pairs)
	}
	if counts.AckCalls >= counts.AckEntries {
		t.Fatalf("the copy made %d ack calls for %d acknowledged records: every ack travelled alone, so the batch path is not in use", counts.AckCalls, counts.AckEntries)
	}
	sent := c.StreamCounts(owner)
	t.Logf("the owner sent %d WRITE_BATCH calls for %d shipped records", sent.WriteCalls, sent.WriteEntries)
	if sent.WriteCalls >= sent.WriteEntries {
		t.Fatalf("the owner made %d write calls for %d shipped records: every record travelled alone, so the batch path is not in use", sent.WriteCalls, sent.WriteEntries)
	}
}

// TestALostWriteBatchIsReshippedAndNothingIsLost (BUG-0745, invariants I1, I10, crash case e):
// the owner's first data-channel dial to the copy is refused, so its first WRITE_BATCH never
// leaves. The lane must rewind every pair of the frame and ship it again; the insert then ends
// applied with both copies counted and the copy holds the document. The send failure must show
// in the background errors, or the fault was never exercised and the test proves nothing.
// The write concern waits 3 s, less than the owner's 5 s ack deadline: a frame that is not
// rewound is only shipped again when that deadline passes, too late for this insert.
// [BREAK: do not rewind after a failed WriteBatch in sendShipments: the insert ends degraded]
func TestALostWriteBatchIsReshippedAndNothingIsLost(t *testing.T) {
	c := proofSetup(t, harness.Options{WCTimeout: 3 * time.Second})
	_, owner, copyNode, _ := proofID(t, c, "bug0745-lostbatch")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "bug0745-lostbatch-id", 2)
	// The first insert warms the owner's bucket cache: a cold lookup of the declaration is
	// itself a call on the data channel and would use up the refused dial.
	warm, id := ids[0], ids[1]
	if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: warm}, {Key: "name", Value: "Bea"}}); err != nil {
		t.Fatalf("the warming insert of %s through %s: %v (background %v)", warm, owner, err, c.BackgroundErrors())
	}
	c.DropChannel(owner, copyNode, transport.ChannelData, 1)
	got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
	if err != nil {
		t.Fatalf("insert through the owner %s: %v (background %v)", owner, err, c.BackgroundErrors())
	}
	if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
		t.Fatalf("outcome %v copies_confirmed %d, want applied and 2: the lost frame was not shipped again (background %v)", got.Outcome, got.CopiesConfirmed, c.BackgroundErrors())
	}
	if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
		t.Fatalf("the copy %s does not hold %s: found %v err %v", copyNode, id, found, err)
	}
	lost := false
	for _, message := range c.BackgroundErrors() {
		if strings.Contains(message, "sending") && strings.Contains(message, "WRITE") && !strings.Contains(message, "WRITE_ACK") && strings.Contains(message, " to "+copyNode) {
			lost = true
		}
	}
	if !lost {
		t.Fatalf("no send to %s failed, so the refused dial never hit the stream (background %v)", copyNode, c.BackgroundErrors())
	}
}

// TestABurstOnOnePairIsAppliedOnceAndInOrder (BUG-0745, invariants I1, I2): six writers insert
// ten documents each into one pair of RF 2, with the copy's appends slowed and the stream
// delayed so the records of a lane pass leave in frames of several entries. Every insert ends
// applied with both copies counted, the copy holds all sixty documents, its applied position is
// the owner's, and it acknowledged the records one by one, so each arrived on the stream, in
// order, and was applied once. A frame applied in the wrong order meets gaps; the copy refuses
// them and catch-up fills them in, which the insert outcomes alone do not show, so the count of
// acknowledgements is what fails.
// [BREAK: iterate a frame's entries in reverse in OnWriteBatch: about half the records arrive by catch-up]
func TestABurstOnOnePairIsAppliedOnceAndInOrder(t *testing.T) {
	const writers, perWriter = 6, 10
	c := proofSetup(t, harness.Options{WCTimeout: 4 * time.Second})
	seed, owner, copyNode, _ := proofID(t, c, "bug0745-burst")
	ids := reseedIDsInOneSlot(t, seed, writers*perWriter)
	c.SlowDisk(copyNode, 20*time.Millisecond)
	t.Cleanup(func() { c.SlowDisk(copyNode, 0) })
	// Every write on the stream takes 40 ms to arrive, so the records the six writers append
	// meanwhile wait for the lane and leave together in the next frame.
	c.DelayChannel(owner, copyNode, transport.ChannelData, 40*time.Millisecond)
	t.Cleanup(func() { c.DelayChannel(owner, copyNode, transport.ChannelData, 0) })

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	insertAll := func(mine []string) {
		defer wg.Done()
		for _, id := range mine {
			got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
			if err != nil {
				errs <- err
				continue
			}
			if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
				errs <- &notConfirmed{id: id, outcome: got.Outcome.String(), copies: int(got.CopiesConfirmed)}
			}
		}
	}
	// One insert alone first: the pair's first record meets a copy that has never seen the pair.
	wg.Add(1)
	insertAll(ids[:1])
	rest := ids[1:]
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go insertAll(rest[writer*len(rest)/writers : (writer+1)*len(rest)/writers])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("insert through the owner: %v (background %v)", err, c.BackgroundErrors())
	}
	for _, id := range ids {
		if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
			t.Errorf("the copy %s does not hold %s: found %v err %v", copyNode, id, found, err)
		}
	}
	// The copy's applies are 20 ms apart, so every record that reaches it on the stream is
	// acknowledged at its own position. A record that arrives by catch-up after the stream met a
	// gap is not, and a copy that applied the burst in the wrong order meets many gaps.
	acks := c.StreamCounts(copyNode)
	sent := c.StreamCounts(owner)
	t.Logf("the owner shipped %d records in %d WRITE_BATCH calls and the copy sent %d acknowledgements for %d inserts", sent.WriteEntries, sent.WriteCalls, acks.AckEntries, writers*perWriter)
	if acks.AckEntries < writers*perWriter*4/5 {
		t.Errorf("the copy acknowledged %d of %d records one by one: the rest reached it by catch-up, so the stream did not deliver the burst in order", acks.AckEntries, writers*perWriter)
	}
	ownerPos, err := c.Node(owner).PairPosition(proofCtx(t), proofDB, proofBucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	copyPos, err := c.Node(copyNode).PairPosition(proofCtx(t), proofDB, proofBucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	if ownerPos != copyPos || ownerPos.Seq != writers*perWriter {
		t.Fatalf("the owner is at %s and the copy at %s, want both at sequence %d", ownerPos, copyPos, writers*perWriter)
	}
}

// TestABatchedStreamSurvivesTheCopyRestartingMidBurst (BUG-0745, invariants I3, I10, crash cases c
// and d): sixteen writers insert on distinct pairs while the copy's appends are slow; once the
// first inserts are confirmed the copy is killed and restarted under the burst. Every insert that
// confirmed two copies while the copy was up must already be in the copy's store at that moment
// (an acknowledgement is queued only after the apply), and every insert that returned without an
// error must be readable on the owner and, within 40 s, on the copy: a record the copy dropped
// with its queue is shipped again, one it applied is answered as a duplicate.
// [BREAK: queue the ack of a record before applyQueued runs, in drainPair: a confirmed insert is not yet on the copy]
func TestABatchedStreamSurvivesTheCopyRestartingMidBurst(t *testing.T) {
	const writers, perWriter = 16, 16
	c := proofSetup(t, harness.Options{NoDetection: true, WCTimeout: 2 * time.Second})
	_, owner, copyNode, _ := proofID(t, c, "bug0745-midburst")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "bug0745-midburst-id", writers*perWriter)
	c.SlowDisk(copyNode, 20*time.Millisecond)
	t.Cleanup(func() { c.SlowDisk(copyNode, 0) })

	// copyUp guards every read of the copy's store: Kill closes the node's Badger, and a read
	// racing the close is not a result worth having. A writer reads only while holding the read
	// side; the test takes the write side around the kill and the restart.
	var copyUp sync.RWMutex
	copyDown := false
	var confirmed, finished atomic.Int64
	var mu sync.Mutex
	var returned []string
	var early []string

	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(mine []string) {
			defer wg.Done()
			for _, id := range mine {
				got, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
				finished.Add(1)
				if err != nil {
					continue
				}
				mu.Lock()
				returned = append(returned, id)
				mu.Unlock()
				if got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
					continue
				}
				confirmed.Add(1)
				copyUp.RLock()
				if !copyDown {
					if _, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err == nil && !found {
						mu.Lock()
						early = append(early, id)
						mu.Unlock()
					}
				}
				copyUp.RUnlock()
			}
		}(ids[writer*perWriter : (writer+1)*perWriter])
	}

	c.Await("the first inserts to be confirmed by both copies", 30*time.Second, func() bool { return confirmed.Load() >= writers })
	copyUp.Lock()
	copyDown = true
	finishedAtKill := finished.Load()
	c.Kill(copyNode)
	c.Restart(copyNode)
	copyDown = false
	copyUp.Unlock()
	if finishedAtKill >= writers*perWriter {
		t.Fatalf("the burst was over (%d of %d inserts returned) when the copy was killed: the kill did not land mid-burst", finishedAtKill, writers*perWriter)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	t.Logf("%d of %d inserts returned without an error; %d were confirmed by both copies; the copy was killed with %d returned", len(returned), writers*perWriter, confirmed.Load(), finishedAtKill)
	if len(early) > 0 {
		t.Errorf("%d inserts were confirmed by the copy before it held them (first %s): an acknowledgement left before its record was applied", len(early), early[0])
	}
	for _, id := range returned {
		if _, found, err := c.Node(owner).LocalDocument(proofCtx(t), proofDB, proofBucket, id); err != nil || !found {
			t.Errorf("the owner %s lost the acknowledged %s: found %v err %v (background %v)", owner, id, found, err, c.BackgroundErrors())
		}
	}
	for _, id := range returned {
		c.Await("the restarted copy holds "+id, 40*time.Second, func() bool {
			_, found, err := c.Node(copyNode).LocalDocument(proofCtx(t), proofDB, proofBucket, id)
			return err == nil && found
		})
	}
}

// bug0745IDsInSlotOf returns count ids of bucket whose slot is that of the first one, which is
// seed, so every write lands in one pair.
func bug0745IDsInSlotOf(t *testing.T, bucket, seed string, count int) []string {
	t.Helper()
	slot, err := wire.PlacementOf(proofDB, bucket, seed)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{seed}
	for i := 0; len(ids) < count && i < 50_000_000; i++ {
		candidate := fmt.Sprintf("e-%d", i)
		if got, err := wire.PlacementOf(proofDB, bucket, candidate); err == nil && got == slot {
			ids = append(ids, candidate)
		}
	}
	if len(ids) < count {
		t.Fatalf("found %d ids in slot %d, want %d", len(ids), slot, count)
	}
	return ids
}

// TestABatchedStreamStopsAtAnEpochMoveWithoutLosingAnAckedWrite (BUG-0745, invariants I9, I10,
// crash case g): ten inserts are confirmed by both copies, then a burst of unconfirmed inserts
// is queued on a copy whose applies are slow, and the owner is killed. When the committed epoch
// moves the copy becomes the owner, and no record queued under the old epoch may be applied
// after that: its applied position stays where it was at the move (one record already inside the
// worker may finish). The old owner is restarted and every acknowledged id is readable on both
// nodes.
// [BREAK: remove the epoch re-check in applyQueued: the promoted copy applies the old epoch's queue]
func TestABatchedStreamStopsAtAnEpochMoveWithoutLosingAnAckedWrite(t *testing.T) {
	const acked, burst = 10, 12
	const appendDelay = 500 * time.Millisecond
	c := harness.NewCluster(t, 3, harness.Options{FastDetection: true, JoinerActivation: true, WCTimeout: 30 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatalf("creating the RF=2 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, bug0749Bucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	w := &bug0749World{c: c}
	probe := c.Node(c.IDs()[0])
	for i := 0; w.id == "" && i < 2000; i++ {
		candidate := fmt.Sprintf("epoch-%d", i)
		o, copies, err := probe.Owner(proofCtx(t), proofDB, bug0749Bucket, candidate)
		if err != nil {
			t.Fatalf("placing %s: %v", candidate, err)
		}
		if len(copies) == 2 && o == copies[0] {
			w.id, w.owner, w.copyNode = candidate, copies[0], copies[1]
		}
	}
	if w.id == "" {
		t.Fatal("no id placed")
	}
	for _, n := range c.IDs() {
		if n != w.owner && n != w.copyNode {
			w.outsid = n
		}
	}
	ids := bug0745IDsInSlotOf(t, bug0749Bucket, w.id, acked+burst)
	insert := func(id string) (wire.Inserted, error) {
		return c.Node(w.owner).Insert(proofCtx(t), proofDB, bug0749Bucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
	}
	var acknowledged []string
	for _, id := range ids[:acked] {
		got, err := insert(id)
		if err != nil || got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
			t.Fatalf("insert of %s: %+v %v, want applied and 2 copies (background %v)", id, got, err, c.BackgroundErrors())
		}
		acknowledged = append(acknowledged, id)
	}
	w.copyCaughtUp(t)
	ackedPos, err := c.Node(w.copyNode).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
	if err != nil {
		t.Fatal(err)
	}

	// The burst: the owner ships every record at once and the copy applies them one per
	// appendDelay, so most of them are queued, unapplied, when the owner dies. WCTimeout is long
	// so none of them answers degraded before then; the kill ends them with an error.
	c.SlowDisk(w.copyNode, appendDelay)
	t.Cleanup(func() { c.SlowDisk(w.copyNode, 0) })
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range ids[acked:] {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			got, err := insert(id)
			if err == nil && got.Outcome == machine.OutcomeApplied && got.CopiesConfirmed == 2 {
				mu.Lock()
				acknowledged = append(acknowledged, id)
				mu.Unlock()
			}
		}(id)
	}
	c.Await("the copy to apply the first record of the burst", 20*time.Second, func() bool {
		got, err := c.Node(w.copyNode).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
		return err == nil && ackedPos.Less(got)
	})

	epochBefore := c.Node(w.copyNode).State().Epoch
	c.Kill(w.owner)
	c.Await("the copy to commit the owner down (an epoch bump)", 30*time.Second, func() bool {
		return c.Node(w.copyNode).State().Epoch > epochBefore
	})
	atMove, err := c.Node(w.copyNode).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
	if err != nil {
		t.Fatal(err)
	}
	if queued := burst - int(atMove.Seq-ackedPos.Seq); queued < 4 {
		t.Fatalf("only %d of %d burst records were still queued at the epoch move (copy at %s, was %s): too few to tell a dropped queue from an applied one", queued, burst, atMove, ackedPos)
	}
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	c.Await("the copy to own the pair", 30*time.Second, func() bool {
		o, _, err := c.Node(w.copyNode).Owner(proofCtx(t), proofDB, bug0749Bucket, w.id)
		return err == nil && o == w.copyNode
	})
	// Everything that was queued would have been applied by now if the epoch had not stopped it.
	time.Sleep(burst*appendDelay + time.Second)
	later, err := c.Node(w.copyNode).PairPosition(proofCtx(t), proofDB, bug0749Bucket, w.id)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the copy applied up to %s before the burst, was at %s when the epoch moved and is at %s a full queue later", ackedPos, atMove, later)
	if later.Epoch != atMove.Epoch || later.Seq > atMove.Seq+1 {
		t.Fatalf("the copy was at %s when the epoch moved and is at %s a full queue later: records queued under the old epoch were applied after the move", atMove, later)
	}
	c.SlowDisk(w.copyNode, 0)
	wg.Wait()

	c.Restart(w.owner)
	w.awaitActive(t, w.owner)
	mu.Lock()
	defer mu.Unlock()
	for _, node := range []string{w.copyNode, w.owner} {
		for _, id := range acknowledged {
			c.Await("node "+node+" holds the acknowledged "+id, 40*time.Second, func() bool {
				_, found, err := c.Node(node).LocalDocument(proofCtx(t), proofDB, bug0749Bucket, id)
				return err == nil && found
			})
		}
	}
}
