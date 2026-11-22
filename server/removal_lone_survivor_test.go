package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// loneSurvivor is a two-node factor-1 cluster after the removal of node 2,
// with node 1 restarted alone before it took node 2's bucket over (BUG-0502).
type loneSurvivor struct {
	tc     *testCluster
	bucket string
	// second is a second bucket node 2 owns, with one acknowledged write
	// (key s0), when the setup asked for it; "" otherwise.
	second string
	// refuse holds the removed node's asks for bucket (and second) while it
	// is true; asked counts them.
	refuse *atomic.Bool
	asked  *atomic.Int64
}

// restartedLoneSurvivor builds the BUG-0502 scenario: five acknowledged writes
// to a bucket node 2 owns, node 2 removed while it runs, its asks to the
// survivor held, and the survivor restarted as the only member of its ring.
// The asks stay held until the caller clears refuse.
func restartedLoneSurvivor(t *testing.T) *loneSurvivor {
	t.Helper()
	return restartedLoneSurvivorWith(t, loneSurvivorSetup{})
}

// loneSurvivorSetup is what restartedLoneSurvivorWith adds to the scenario.
type loneSurvivorSetup struct {
	// second makes a second bucket node 2 owns (loneSurvivor.second).
	second bool
	// beforeRestart, when set, runs on the survivor just before its restart.
	beforeRestart func(survivor *Cluster, removedID string)
}

// restartedLoneSurvivorWith is restartedLoneSurvivor with setup's additions.
func restartedLoneSurvivorWith(t *testing.T, setup loneSurvivorSetup) *loneSurvivor {
	t.Helper()
	tc := newTestCluster(t, 2, 1)
	t.Cleanup(tc.stop)
	survivor := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, tc.addrs[1])
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	for i := 0; i < 5; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	second := ""
	if setup.second {
		second = bucketNamed(t, "second", func(name string) bool {
			owner, err := survivor.Owner(placementOf(testDatabase, name))
			return err == nil && owner.ID == tc.addrs[1]
		})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: second, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: second, Key: "s0",
			Data: map[string]interface{}{"n": 0}, WriteConcern: "all"})
	}
	scenario := &loneSurvivor{tc: tc, bucket: bucket, second: second, refuse: &atomic.Bool{}, asked: &atomic.Int64{}}
	scenario.refuse.Store(true)
	hook := func(fromID, _, _, name string) error {
		if fromID != tc.addrs[1] || name != bucket && (second == "" || name != second) {
			return nil
		}
		scenario.asked.Add(1)
		if scenario.refuse.Load() {
			return errors.New("test: the ask is held until the survivor restarts")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	if !holdsWithin(10*time.Second, func() bool { return scenario.asked.Load() > 0 }) {
		t.Fatal("setup: the removed node never asked")
	}
	if setup.beforeRestart != nil {
		setup.beforeRestart(survivor, tc.addrs[1])
	}
	tc.restartNode(0)
	if clusterOf(tc, 0).Enabled() {
		t.Fatal("setup: the restarted survivor is not alone on its ring")
	}
	return scenario
}

// requireEveryWrite checks the survivor holds the five acknowledged writes.
func (s *loneSurvivor) requireEveryWrite(t *testing.T) {
	t.Helper()
	for i := 0; i < 5; i++ {
		if key := fmt.Sprintf("k%02d", i); !holdsKeyIn(s.tc.nodes[0], testDatabase, s.bucket, key) {
			t.Errorf("the survivor lacks acknowledged write %s", key)
		}
	}
}

// TestALoneSurvivorRestartedAfterARemovalTakesTheBucketOver is BUG-0502: the
// survivor of a 2→1 removal at factor 1 restarts before it took the removed
// node's bucket over. It starts as the only member, so Start takes the
// single-node return; it must still take the bucket over when the removed node
// asks. Guard: no take-over worker on a lone node, so every ask waits on
// nothing and the bucket answers "bucket not found" for good.
func TestALoneSurvivorRestartedAfterARemovalTakesTheBucketOver(t *testing.T) {
	s := restartedLoneSurvivor(t)
	before := s.asked.Load()
	s.refuse.Store(false)
	served := holdsWithin(takeOverWait, func() bool { return s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) })
	if !served {
		frame := s.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "k00"})
		t.Fatalf("the survivor, restarted alone after the removal, never took %s over within %s although the removed node asked %d times; a read answers %s: %q",
			s.bucket, takeOverWait, s.asked.Load()-before, frame.Opcode, string(frame.Payload))
	}
	s.requireEveryWrite(t)
}

// TestAnOperatorMigrateOnALoneSurvivorTakesTheBucketOver is BUG-0502's
// documented remedy: with the removed node's asks still held, an operator's
// `migrate <database> <bucket>` on the restarted survivor makes it serve the
// bucket. Guard: migrate on the owner waits on the take-over worker
// (Cluster.awaitOwnCopy), which a lone node never started, so every attempt
// answers that the hand-off is in progress once the write-concern timeout
// has passed. The refusal is an error frame (BUG-0507), which decodedMigrate
// reads as the answer's error.
func TestAnOperatorMigrateOnALoneSurvivorTakesTheBucketOver(t *testing.T) {
	s := restartedLoneSurvivor(t)
	payload, err := bson.Marshal(MigrateRequest{Database: testDatabase, Bucket: s.bucket})
	if err != nil {
		t.Fatalf("encoding the migrate request: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), takeOverWait)
	defer cancel()
	frame, err := s.tc.nodes[0].client.SendRequest(ctx, transport.OpMigrate, payload)
	if err != nil {
		t.Fatalf("an operator's migrate of %s on the survivor, restarted alone after the removal, failed: %v", s.bucket, err)
	}
	answer := decodedMigrate(t, frame)
	if !answer.Success {
		t.Fatalf("an operator's migrate of %s on the survivor, restarted alone after the removal, was refused: %q", s.bucket, answer.Error)
	}
	if !s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatalf("migrate answered %q, and the survivor does not serve %s", answer.Message, s.bucket)
	}
	s.requireEveryWrite(t)
}

// TestANodeThatWasAlwaysAloneStartsNoTakeOverWorker pins the cost side of
// BUG-0502's fix (rule 11): a node that never had another member, at its
// first start and after a restart, runs no take-over worker. The probe is the
// worker's wake channel: a running worker drains a signal at once, so a signal
// still pending a second later means no worker. Guard: start the worker on
// every node and the signal is drained. It also pins that such a node's door
// pays one atomic load for BUG-0630's question (historyNamedAnotherNode).
func TestANodeThatWasAlwaysAloneStartsNoTakeOverWorker(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	for _, phase := range []string{"first start", "restart"} {
		if phase == "restart" {
			tc.restartNode(0)
		}
		lone := clusterOf(tc, 0)
		if lone.Enabled() || len(lone.removedInHistory()) > 0 {
			t.Fatalf("setup (%s): the node is not a node that was always alone", phase)
		}
		// The one-member door asks aloneAfterARemoval on every request
		// (BUG-0630); for this node it must answer on its one atomic load.
		if lone.historyNamedAnotherNode.Load() {
			t.Errorf("after its %s, a node that was always alone has a history naming another node, so every request at its door walks the history", phase)
		}
		lone.signalTakeOver()
		time.Sleep(time.Second)
		if pending := len(lone.takeOver.wake); pending != 1 {
			t.Errorf("after its %s, a node that was always alone drained a take-over signal: it runs a take-over worker it has no use for", phase)
		}
	}
}

// TestALoneSurvivorsCreateWaitsForTheTakeOverAndKeepsTheWrites is BUG-0625: a
// client creates, on the survivor restarted alone, the bucket whose only copy
// is on the removed node, before the survivor took it over. The survivor
// takes the one-member door (it is not Enabled), which must still ask what an
// owner asks: the create waits for the take-over and is answered from the
// copy it lands, or is refused with retry, and never makes an empty bucket of
// the name. Guard: no owner's refusals at the one-member door, and the create
// makes an empty twin, the removed node's copy is refused as one, and the five
// acknowledged writes never reach the survivor.
func TestALoneSurvivorsCreateWaitsForTheTakeOverAndKeepsTheWrites(t *testing.T) {
	s := restartedLoneSurvivor(t)
	if s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the survivor holds the bucket before the create")
	}
	frame := s.tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: s.bucket, Type: BucketTypeHash})
	t.Logf("the create on the lone survivor was answered %s: %q", frame.Opcode, string(frame.Payload))
	s.refuse.Store(false)
	if !holdsWithin(takeOverWait, func() bool {
		for i := 0; i < 5; i++ {
			if !holdsKeyIn(s.tc.nodes[0], testDatabase, s.bucket, fmt.Sprintf("k%02d", i)) {
				return false
			}
		}
		return true
	}) {
		s.requireEveryWrite(t)
		t.Fatalf("after a create sent to the lone survivor before it took %s over, the removed node's acknowledged writes never reached it (the create was answered %s)",
			s.bucket, frame.Opcode)
	}
}

// TestAOneMemberClusterRefusesSchemaChangesWhileItsViewPauses is BUG-0625's
// second half: a node alone on its ring whose view pauses schema changes (the
// members settling the removal of a joiner that left before its join
// settled) refuses database and bucket DDL with ErrSchemaChangePaused, as a
// routed node does at its door (Cluster.refuseSchemaChange). The paused
// report is published by hand: reaching it for real needs a join and a
// removal inside one settle. Guard: no refuseSchemaChange at the one-member
// door, and every change is applied.
func TestAOneMemberClusterRefusesSchemaChangesWhileItsViewPauses(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	lone := clusterOf(tc, 0)
	if lone.Enabled() {
		t.Fatal("setup: the node is not alone on its ring")
	}
	const departed = "192.0.2.9:7000"
	cases := []struct {
		name string
		op   transport.Opcode
		req  any
	}{
		{"create database", transport.OpCreateDB, CreateDatabaseRequest{Name: "paused-made"}},
		{"drop database", transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase}},
		{"create bucket", transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "paused-bucket", Type: BucketTypeHash}},
		{"alter bucket", transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: testBucket, ReplicationFactor: 1}},
		{"drop bucket", transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: testBucket}},
	}
	for _, tt := range cases {
		paused := &joinFreezeReport{Epoch: lone.Epoch(), Paused: true, Departed: []string{departed}}
		lone.joinFreeze.Store(paused)
		frame := tc.frame(0, tt.op, tt.req)
		if lone.joinFreeze.Load() != paused {
			t.Fatalf("setup: the paused report was replaced while %s was in flight", tt.name)
		}
		text := string(frame.Payload)
		if frame.Opcode != transport.OpError || !strings.Contains(text, "settle the removal of "+departed) {
			t.Errorf("%s on a one-member cluster whose view pauses schema changes was answered %s: %q", tt.name, frame.Opcode, text)
		}
	}
	lone.recomputeJoinFreeze()
	core := tc.nodes[0].server.core
	if !core.bucketStoreOpen(testDatabase, testBucket) || core.bucketStoreOpen(testDatabase, "paused-bucket") {
		t.Error("a schema change was applied while the one-member cluster's view paused them")
	}
}

// TestALoneSurvivorStillCreatesANewName pins the other side of BUG-0625's
// wait: a create of a name no node holds is not held for ever on a survivor a
// removal left alone, while the removed node runs and once it is gone. The
// take-over finds the name nowhere (awaitOwnCopy's ErrMigrateBucketNotFound)
// and the create goes on. Guard: refuse a create whenever the wait does not
// land a copy, and both creates are refused.
func TestALoneSurvivorStillCreatesANewName(t *testing.T) {
	s := restartedLoneSurvivor(t)
	for _, phase := range []string{"removed node running", "removed node stopped"} {
		if phase == "removed node stopped" {
			if err := s.tc.nodes[1].server.Stop(); err != nil {
				t.Fatalf("stopping the removed node: %v", err)
			}
			s.tc.nodes[1].server = nil
		}
		name := "fresh-" + strings.ReplaceAll(phase, " ", "-")
		began := time.Now()
		frame := s.tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		if text := string(frame.Payload); frame.Opcode == transport.OpError || !strings.Contains(text, "created") {
			t.Errorf("%s: a create of a new name on the lone survivor was answered %s after %s: %q", phase, frame.Opcode, time.Since(began).Round(time.Millisecond), text)
			continue
		}
		t.Logf("%s: a new name was created in %s", phase, time.Since(began).Round(time.Millisecond))
	}
}

// TestALoneSurvivorAnswersAnAlterOfAnUnheldBucketAsItsOwner is BUG-0625's
// owner's answer at the one-member door (Cluster.fenceUnheldBucket): an alter
// of the bucket the removed node holds, sent to the survivor before it took
// the bucket over, is refused as the hand-off of a bucket it owns, which names
// the bucket to the take-over, and neither applied nor refused as a bucket that
// does not exist, whose refusal tells the client to create it. Guard: no
// fenceUnheldBucket at the one-member door, and the alter of a bucket the
// survivor does not hold is answered with a success.
func TestALoneSurvivorAnswersAnAlterOfAnUnheldBucketAsItsOwner(t *testing.T) {
	s := restartedLoneSurvivor(t)
	if s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the survivor holds the bucket before the alter")
	}
	frame := s.tc.frame(0, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: s.bucket, ReplicationFactor: 1})
	text := string(frame.Payload)
	if frame.Opcode != transport.OpError || strings.Contains(text, "to create it") {
		t.Fatalf("an alter of %s, which the removed node holds, sent to the lone survivor before it took the bucket over, was answered %s: %q",
			s.bucket, frame.Opcode, text)
	}
	if outcome := writeOutcomeOf(t, frame); outcome != outcomeNotApplied {
		t.Errorf("the alter's refusal says write_outcome %q, want %q", outcome, outcomeNotApplied)
	}
}

// TestALoneSurvivorAnswersADataOpOfAnUnheldBucketAsItsOwner is BUG-0632: a put
// and a get of a bucket only the removed node held, sent to the survivor
// before it took the bucket over, are answered with the hand-off the routed
// owner gives, which says retry, and never with the not-found advice to create
// a bucket that exists. The put says nothing was stored. When the take-over
// has finished both are served. A bucket that exists nowhere and that the
// removed node owned is read, under owner ruling R8 (2026-10-21, which changed
// this test): while the survivor still waits for the removed node's hand-off
// (its asks are held), a get of it is a retry naming the removed node; once
// the hand-off has reached the survivor and its window has closed, it is not
// found, with its create advice and a note naming the removed node. A put of
// it gets the same two answers (owner ruling R8 extended to writes,
// 2026-10-23, BUG-0693). Guards: no unheld-bucket question for a data op at
// the one-member door (Cluster.answerUnheldDataOp), so the held bucket's put
// and get answer "to create it"; and R8's two answers, for the get and the
// put.
func TestALoneSurvivorAnswersADataOpOfAnUnheldBucketAsItsOwner(t *testing.T) {
	s := restartedLoneSurvivor(t)
	if s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatal("setup: the survivor holds the bucket before the ops")
	}
	// The take-over is held before it opens the bucket, so the ops meet it
	// pending; a wake alone, which the ops now give, would otherwise land it.
	takeOver := holdTakeOverOf(t, s.tc.addrs[0], s.bucket)
	put := InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k99", Data: map[string]interface{}{"n": 99}}
	get := FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "k00"}

	putFrame := s.tc.frame(0, transport.OpPut, put)
	if text := string(putFrame.Payload); putFrame.Opcode != transport.OpError || strings.Contains(text, "to create it") || !strings.Contains(text, "Retry in a moment") {
		t.Errorf("a put of %s, which the removed node holds, sent to the lone survivor before it took the bucket over, was answered %s: %q",
			s.bucket, putFrame.Opcode, text)
	}
	if outcome := writeOutcomeOf(t, putFrame); outcome != outcomeNotApplied {
		t.Errorf("the put's refusal says write_outcome %q, want %q", outcome, outcomeNotApplied)
	}
	getFrame := s.tc.frame(0, transport.OpGet, get)
	if text := string(getFrame.Payload); getFrame.Opcode != transport.OpError || strings.Contains(text, "to create it") || !strings.Contains(text, "Retry in a moment") {
		t.Errorf("a get of %s, sent to the lone survivor before it took the bucket over, was answered %s: %q", s.bucket, getFrame.Opcode, text)
	}

	// A name the removed node owned that exists nowhere (owner ruling R8).
	survivor := clusterOf(s.tc, 0)
	removedID := s.tc.addrs[1]
	nowhere := ""
	for i := 0; nowhere == ""; i++ {
		if name := fmt.Sprintf("never-made-%d", i); survivor.ownedByARemovedNode(placementOf(testDatabase, name)) {
			nowhere = name
		}
		if i > 512 {
			t.Fatal("setup: no name in 512 tries was owned by the removed node")
		}
	}
	nowherePut := InsertRequest{Database: testDatabase, Bucket: nowhere, Key: "k", Data: map[string]interface{}{"n": 1}}
	nowhereGet := FindRequest{Database: testDatabase, Bucket: nowhere, Key: "k"}
	// The survivor still waits for the removed node's hand-off: retry, naming it.
	retryNamingRemovedNode(t, "a put of a bucket that exists nowhere, while the removed node's hand-off is held",
		s.tc.frame(0, transport.OpPut, nowherePut), removedID)
	if frame := s.tc.frame(0, transport.OpGet, nowhereGet); frame.Opcode != transport.OpError ||
		strings.Contains(string(frame.Payload), "to create it") || !strings.Contains(string(frame.Payload), removedID) {
		t.Errorf("a get of a bucket that exists nowhere, while the removed node's hand-off is held, was answered %s: %q; want a retry naming %s",
			frame.Opcode, string(frame.Payload), removedID)
	}

	s.refuse.Store(false)
	takeOver.release()
	if !holdsWithin(takeOverWait, func() bool { return s.tc.nodes[0].server.core.bucketStoreOpen(testDatabase, s.bucket) }) {
		t.Fatalf("the survivor never took %s over", s.bucket)
	}
	// The hand-off has reached the survivor and its window closes: an ordinary
	// miss that names the removed node.
	if !holdsWithin(takeOverWait, func() bool { return !survivor.readWindowOpen(survivor.Epoch()) }) {
		t.Fatalf("the survivor's settling window never closed once the removed node's hand-off reached it")
	}
	missNamingRemovedNode(t, "a get of a bucket that exists nowhere, after the window", s.tc.frame(0, transport.OpGet, nowhereGet), removedID)
	missNamingRemovedNode(t, "a put of a bucket that exists nowhere, after the window", s.tc.frame(0, transport.OpPut, nowherePut), removedID)
	s.tc.ok(0, transport.OpPut, put)
	if frame := s.tc.frame(0, transport.OpGet, get); frame.Opcode == transport.OpError {
		t.Errorf("after the take-over a get of %s was refused: %q", s.bucket, string(frame.Payload))
	}
}

// heldReplica is a two-node factor-2 cluster after the removal of node 2,
// the owner of bucket, with node 1 alone on its ring and holding a replica copy
// of bucket it has not taken over (BUG-0630). Node 2 acknowledged two writes
// at w:one that never reached node 1: k0 overwritten from "old" to
// "from-removed", and k1 made.
type heldReplica struct {
	tc     *testCluster
	bucket string
	// release lets node 2's ships to node 1 and its asks through.
	release func()
}

// replicaHeldAfterARemoval builds the BUG-0630 scenario: node 2's ships to
// node 1 and its asks for bucket are held from just after k0's first write, so
// node 1's copy stops at k0 "old" and the take-over cannot run until the
// caller calls release.
func replicaHeldAfterARemoval(t *testing.T) *heldReplica {
	t.Helper()
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	survivor := clusterOf(tc, 0)
	survivorID, removedID := tc.addrs[0], tc.addrs[1]
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketNameOwnedBy(t, survivor, testDatabase, removedID)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k0",
		Data: map[string]interface{}{"v": "old"}, WriteConcern: "all"})

	held := make(chan struct{})
	var once atomic.Bool
	release := func() {
		if once.CompareAndSwap(false, true) {
			close(held)
		}
	}
	shipHook := func(peerID string) func() {
		if peerID == survivorID {
			<-held
		}
		return func() {}
	}
	beforeShipHook.Store(&shipHook)
	askHook := func(fromID, _, _, name string) error {
		select {
		case <-held:
			return nil
		default:
		}
		if fromID == removedID && name == bucket {
			return errors.New("test: the removed node's ask is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&askHook)
	// Registered after tc.stop, so it runs first: a sender blocked in the
	// hook would hold up the stop.
	t.Cleanup(func() {
		release()
		beforeShipHook.Store(nil)
		askNodeToMigrateHook.Store(nil)
	})

	for _, put := range []InsertRequest{
		{Database: testDatabase, Bucket: bucket, Key: "k0", Data: map[string]interface{}{"v": "from-removed"}, WriteConcern: "one"},
		{Database: testDatabase, Bucket: bucket, Key: "k1", Data: map[string]interface{}{"v": "from-removed"}, WriteConcern: "one"},
	} {
		if frame := tc.frame(1, transport.OpUpsert, put); frame.Opcode == transport.OpError {
			t.Fatalf("setup: node 2's w:one put of %s was refused: %q", put.Key, string(frame.Payload))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing the running owner: %v", err)
	}
	key := placementOf(testDatabase, bucket)
	if !holdsWithin(10*time.Second, func() bool {
		if !survivor.aloneAfterARemoval() {
			return false
		}
		_, epoch, err := survivor.ownerView(key)
		if err != nil {
			return false
		}
		awaits, err := survivor.ownerAwaitsTakeOver(key, epoch, testDatabase, bucket)
		return err == nil && awaits
	}) {
		t.Fatal("setup: the survivor is not alone after the removal with its replica copy awaiting the take-over")
	}
	if !holdsKeyIn(tc.nodes[0], testDatabase, bucket, "k0") || holdsKeyIn(tc.nodes[0], testDatabase, bucket, "k1") {
		t.Fatal("setup: the survivor's copy is not the one from before node 2's w:one writes")
	}
	return &heldReplica{tc: tc, bucket: bucket, release: release}
}

// awaitTakeOverDone releases the held ships and asks and waits for the
// survivor to take the bucket over.
func (h *heldReplica) awaitTakeOverDone(t *testing.T) {
	t.Helper()
	h.release()
	survivor := clusterOf(h.tc, 0)
	key := placementOf(testDatabase, h.bucket)
	if !holdsWithin(takeOverWait, func() bool {
		_, epoch, err := survivor.ownerView(key)
		if err != nil {
			return false
		}
		awaits, err := survivor.ownerAwaitsTakeOver(key, epoch, testDatabase, h.bucket)
		return err == nil && !awaits
	}) {
		t.Fatalf("the survivor never took %s over once the removed node's asks were let through", h.bucket)
	}
}

// valueAt is the "v" of key in the survivor's own store, or "" when it holds
// no such document.
func (h *heldReplica) valueAt(t *testing.T, key string) string {
	t.Helper()
	resp, err := h.tc.nodes[0].server.core.Find(FindRequest{Database: testDatabase, Bucket: h.bucket, Key: key})
	if err != nil || resp == nil || !resp.Found {
		return ""
	}
	stored, _ := resp.Data.Lookup("v").StringValueOK()
	return stored
}

// answeredValue reads a GET's answer: found and the document's "v", or the
// refusal's text.
func answeredValue(t *testing.T, frame *transport.Frame) (found bool, stored, refusal string) {
	t.Helper()
	if frame.Opcode == transport.OpError {
		return false, "", string(frame.Payload)
	}
	var resp FindResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding a GET's answer (%s): %v", frame.Opcode, err)
	}
	if !resp.Found {
		return false, "", resp.Message
	}
	stored, _ = resp.Data.Lookup("v").StringValueOK()
	return true, stored, ""
}

// TestALoneSurvivorDoesNotWriteAReplicaCopyBeforeTheTakeOver is BUG-0630's
// write half: the survivor of a 2→1 removal at factor 2 holds a replica copy
// of the removed owner's bucket that it has not taken over. A client's w:one
// overwrite of k0 sent to it in that window is refused as the hand-off, with
// nothing stored, as a routed owner refuses it (Cluster.fenceAsSettledOwner),
// and is never acknowledged and then replaced by the removed node's copy when
// the take-over lands it. Once the take-over is done, a write at the survivor
// is served. Guard: no take-over question at the one-member door, and the
// overwrite is acknowledged, then lost.
func TestALoneSurvivorDoesNotWriteAReplicaCopyBeforeTheTakeOver(t *testing.T) {
	h := replicaHeldAfterARemoval(t)
	frame := h.tc.frame(0, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: h.bucket, Key: "k0",
		Data: map[string]interface{}{"v": "from-survivor"}, WriteConcern: "one"})
	acknowledged := frame.Opcode != transport.OpError
	t.Logf("the survivor's overwrite of k0 before the take-over was answered %s: %q", frame.Opcode, string(frame.Payload))
	if !acknowledged {
		if !strings.Contains(string(frame.Payload), "is moving to this node") {
			t.Errorf("the overwrite was refused, but not as the hand-off: %q", string(frame.Payload))
		}
		if outcome := writeOutcomeOf(t, frame); outcome != outcomeNotApplied {
			t.Errorf("the refusal says write_outcome %q, want %q", outcome, outcomeNotApplied)
		}
	}
	h.awaitTakeOverDone(t)
	if got := h.valueAt(t, "k0"); acknowledged && got != "from-survivor" {
		t.Errorf("the survivor acknowledged its overwrite of k0 before the take-over, and after it k0 is %q: the acknowledged write is lost", got)
	}
	if got := h.valueAt(t, "k1"); got != "from-removed" {
		t.Errorf("after the take-over the survivor's k1 is %q, want the removed node's acknowledged %q", got, "from-removed")
	}
	after := h.tc.frame(0, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: h.bucket, Key: "k0",
		Data: map[string]interface{}{"v": "after-take-over"}, WriteConcern: "one"})
	if after.Opcode == transport.OpError {
		t.Fatalf("after the take-over, a write at the survivor was refused: %q", string(after.Payload))
	}
	if got := h.valueAt(t, "k0"); got != "after-take-over" {
		t.Errorf("after the take-over the survivor's acknowledged write of k0 reads %q", got)
	}
}

// TestALoneSurvivorDoesNotAnswerAReadFromAReplicaCopyBeforeTheTakeOver is
// BUG-0630's read half: in the same window, a read of the bucket at the
// survivor is forwarded to the copy's previous owner, as a routed owner does
// (Cluster.serveRead), or refused as the hand-off; it never answers from the
// replica copy, which lacks the removed node's acknowledged w:one writes. Once
// the take-over is done, reads are answered from the survivor's copy. Guard:
// no read gate at the one-member door, and k0 reads "old" and k1 is not found.
func TestALoneSurvivorDoesNotAnswerAReadFromAReplicaCopyBeforeTheTakeOver(t *testing.T) {
	h := replicaHeldAfterARemoval(t)
	for _, key := range []string{"k0", "k1"} {
		found, stored, refusal := answeredValue(t, h.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: h.bucket, Key: key}))
		switch {
		case found && stored == "from-removed":
			t.Logf("a read of %s before the take-over was forwarded: %q", key, stored)
		case !found && strings.Contains(refusal, "is moving to this node"):
			t.Logf("a read of %s before the take-over was refused as the hand-off: %q", key, refusal)
		default:
			t.Errorf("a read of %s at the survivor before the take-over answered found=%v %q %q: a stale answer from its replica copy, which lacks the removed node's acknowledged write",
				key, found, stored, refusal)
		}
	}
	h.awaitTakeOverDone(t)
	for _, key := range []string{"k0", "k1"} {
		found, stored, refusal := answeredValue(t, h.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: h.bucket, Key: key}))
		if !found || stored != "from-removed" {
			t.Errorf("after the take-over a read of %s at the survivor answered found=%v %q %q, want %q", key, found, stored, refusal, "from-removed")
		}
	}
}

// TestTheLoneAnswerFollowsARemovalAndAJoin pins the invalidation of
// aloneAfterARemoval's stored answer (BUG-0630's per-request cost): it is
// asked before each membership change, so a stale answer would be what the
// next ask returns. Two members: not alone. Node 2 removed: alone, and the
// door answers as the owner. A member joined to the survivor: not alone, and
// its requests take the routed door again. Guard: never invalidate the stored
// answer, and the survivor still answers "not alone" after the removal.
func TestTheLoneAnswerFollowsARemovalAndAJoin(t *testing.T) {
	// The joined member is fabricated and never runs, so its join never
	// settles; the test is about the answer, not the freeze.
	switchJoinFreezeOff(t)
	tc := newTestCluster(t, 2, 1)
	t.Cleanup(tc.stop)
	survivor := clusterOf(tc, 0)
	if survivor.aloneAfterARemoval() {
		t.Fatal("a member of a two-node cluster answers alone after a removal")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := survivor.proposeRemoval(ctx, tc.addrs[1]); err != nil {
		t.Fatalf("removing node 2: %v", err)
	}
	if survivor.Enabled() || !survivor.aloneAfterARemoval() {
		t.Fatalf("after node 2's removal the survivor answers alone after a removal %v (enabled %v)", survivor.aloneAfterARemoval(), survivor.Enabled())
	}
	joined := fabricatedMemberAddr(t)
	if err := survivor.proposeJoin(ctx, ring.Node{ID: joined, Addr: joined}); err != nil {
		t.Fatalf("joining a member to the survivor: %v", err)
	}
	if !survivor.Enabled() {
		t.Fatal("setup: the survivor is not routed after the join")
	}
	if survivor.aloneAfterARemoval() {
		t.Error("after a member joined, the survivor still answers alone after a removal: its requests would take the one-member door")
	}
}
