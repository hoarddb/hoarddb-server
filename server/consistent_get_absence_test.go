package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.4b (dispatch 11): the settled owner's absence at its position
// outranks a copy it has covered, and a bounded delete removes that copy.

// absenceCluster is three nodes at factor 3 holding the test bucket, with its
// owner and replicas by placement.
type absenceCluster struct {
	tc       *testCluster
	owner    int
	replicas []int
}

func newAbsenceCluster(t *testing.T) absenceCluster {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	owner, replicas := ownerAndReplicas(t, tc, testDatabase, testBucket)
	return absenceCluster{tc: tc, owner: owner, replicas: replicas}
}

func (a absenceCluster) put(t *testing.T, key, value, concern string) {
	t.Helper()
	// UPSERT: the tests write a key again to give it a later version.
	frame := a.tc.frame(a.owner, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
		Data: map[string]interface{}{"v": value}, WriteConcern: concern})
	if concern == "all" {
		requireEveryCopy(t, frame, "the put of "+key)
		return
	}
	if frame.Opcode == transport.OpError {
		t.Fatalf("the put of %s at %s: %s", key, concern, frame.Payload)
	}
}

func (a absenceCluster) remove(t *testing.T, key string) {
	t.Helper()
	a.tc.ok(a.owner, transport.OpDelete, deleteAt(testDatabase, testBucket, key, "all"))
}

func (a absenceCluster) read(t *testing.T, key string) ConsistentGetResponse {
	t.Helper()
	return answeredConsistentGet(t, consistentGetFrame(t, a.tc.nodes[a.owner], testDatabase, testBucket, key))
}

func (a absenceCluster) ownerLog() uint64 {
	return clusterOf(a.tc, a.owner).walLastSequence()
}

// heldEverywhere fails unless every node holds key with value at one version,
// and returns the version.
func (a absenceCluster) heldEverywhere(t *testing.T, key, value string) uint64 {
	t.Helper()
	var version uint64
	for index := range a.tc.nodes {
		found, data, held, err := a.tc.nodes[index].server.core.versionedFind(testDatabase, testBucket, key)
		if err != nil || !found || docField(t, data, "v") != value {
			t.Errorf("node %d does not hold %s = %s (found=%v, %v)", index+1, key, value, found, err)
			continue
		}
		if version != 0 && held != version {
			t.Errorf("node %d holds %s at version %d, another node at %d", index+1, key, held, version)
		}
		version = held
	}
	return version
}

// ownerLogged reports whether the owner's log holds an entry of op for the
// bucket above after.
func (a absenceCluster) ownerLogged(t *testing.T, op transport.Opcode, after uint64) bool {
	t.Helper()
	entries, err := a.tc.nodes[a.owner].server.core.wal.ReadFrom(after + 1)
	if err != nil {
		t.Fatalf("reading the owner's log: %v", err)
	}
	for _, entry := range entries {
		if entry.Bucket == testBucket && transport.Opcode(entry.Op) == op {
			return true
		}
	}
	return false
}

// TestConsistentGetStillReturnsAWriteOnlyOneHolderHas: a put acknowledged at
// one, which only the owner applied, is the answer of a consistent get, which
// repairs it onto the replicas: the replicas' absence is not the owner's.
// Guard: let any absent holder outrank (the read answers not found).
func TestConsistentGetStillReturnsAWriteOnlyOneHolderHas(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	owner := clusterOf(a.tc, a.owner)
	var unfreezes []func()
	for _, replica := range a.replicas {
		unfreezes = append(unfreezes, freezePeer(owner.peerFor(mustNode(t, owner, a.tc.addrs[replica]))))
	}
	a.put(t, "k2", "only-owner", "one")
	resp := a.read(t, "k2")
	if !resp.Found || !resp.Repaired || docField(t, resp.Data, "v") != "only-owner" {
		t.Errorf("a put only the owner holds answered found=%v repaired=%v", resp.Found, resp.Repaired)
	}
	for _, unfreeze := range unfreezes {
		unfreeze()
	}
	pollUntil(t, "every node holds k2", func() bool {
		for index := range a.tc.nodes {
			if found, _, _, err := a.tc.nodes[index].server.core.versionedFind(testDatabase, testBucket, "k2"); err != nil || !found {
				return false
			}
		}
		return true
	})
}

// TestAReplicasAbsenceDoesNotDeleteTheOwnersWrite: a replica that refuses, by
// its own declaration, a put its owner applied is absent at a position past
// the put. A consistent get answers the owner's document, and the owner still
// holds it afterwards. Guard: let a positioned replica's absence outrank (the
// read answers not found).
func TestAReplicasAbsenceDoesNotDeleteTheOwnersWrite(t *testing.T) {
	tc := typedTestCluster(t)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	stripDeclaration(t, tc, owner)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped",
		Data: map[string]interface{}{"Age": "thirty"}})
	resp := answeredConsistentGet(t, consistentGetFrame(t, tc.nodes[owner], testDatabase, testBucket, "mistyped"))
	if !resp.Found || docField(t, resp.Data, "Age") != "thirty" {
		t.Errorf("the owner's write, which its replicas refused, answered found=%v", resp.Found)
	}
	time.Sleep(time.Second)
	if found, _, _, err := tc.nodes[owner].server.core.versionedFind(testDatabase, testBucket, "mistyped"); err != nil || !found {
		t.Errorf("after the read the owner no longer holds its write (found=%v, %v)", found, err)
	}
}

// holdWhile installs hook so that the first time it fires on node for the
// test bucket it runs during, and returns whether it fired.
func holdWhile(t *testing.T, hook *hookOnce, node string, during func()) func(nodeID, database, bucket string) {
	t.Helper()
	return func(nodeID, _, bucket string) {
		if nodeID != node || bucket != testBucket {
			return
		}
		hook.once.Do(func() {
			during()
			hook.fired = true
		})
	}
}

// hookOnce is a seam's one firing.
type hookOnce struct {
	once  sync.Once
	fired bool
}

// putAllDuringRead is a put at all through a client, for a seam that runs on a server
// goroutine and cannot fail the test; it returns why the put failed, or "".
func putAllDuringRead(tc *testCluster, index int, key, value string) string {
	return writeAllDuringRead(transport.OpPut, tc, index, key, value)
}

// upsertAllDuringRead is putAllDuringRead for a key the bucket already holds:
// insert refuses an existing id, so a write that replaces sends an upsert.
func upsertAllDuringRead(tc *testCluster, index int, key, value string) string {
	return writeAllDuringRead(transport.OpUpsert, tc, index, key, value)
}

func writeAllDuringRead(op transport.Opcode, tc *testCluster, index int, key, value string) string {
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
		Data: map[string]interface{}{"v": value}, WriteConcern: "all"})
	if err != nil {
		return err.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := tc.dial(index)
	defer func() { _ = client.Close() }()
	frame, err := client.SendRequest(ctx, op, payload)
	switch {
	case err != nil:
		return err.Error()
	case frame.Opcode == transport.OpError:
		return string(frame.Payload)
	}
	return ""
}

// TestAnAbsenceIsReadWithItsPosition: (a) a put that lands between the owner's
// read of a key and its read of the copy's position is above the position the
// owner reports, so the read answers the put and deletes nothing; (b) a put
// that lands between the owner's answer and the replicas' is above the
// owner's position on the replicas: the read answers it, found and not
// repaired, and sends no repair of either kind. Guards: read the position
// from another snapshot ((a): the put is deleted); no position comparison
// ((b): the put is deleted); a put repair above the position ((b): the
// owner's log grows).
func TestAnAbsenceIsReadWithItsPosition(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	ownerID := a.tc.addrs[a.owner]
	var putErr string

	var between hookOnce
	hold := holdWhile(t, &between, ownerID, func() { putErr = putAllDuringRead(a.tc, a.owner, "k3a", "landed") })
	betweenVersionedReadsHook.Store(&hold)
	resp := a.read(t, "k3a")
	betweenVersionedReadsHook.Store(nil)
	if !between.fired || putErr != "" {
		t.Fatalf("setup: the put between the owner's two reads did not land (fired=%v): %s", between.fired, putErr)
	}
	if !resp.Found || docField(t, resp.Data, "v") != "landed" {
		t.Errorf("(a) a put between the owner's reads answered found=%v repaired=%v", resp.Found, resp.Repaired)
	}
	time.Sleep(time.Second)
	a.heldEverywhere(t, "k3a", "landed")

	var after hookOnce
	var tailAfterPut uint64
	held := holdWhile(t, &after, a.tc.addrs[a.owner], func() {
		putErr = putAllDuringRead(a.tc, a.owner, "k3b", "landed")
		tailAfterPut = a.ownerLog()
	})
	afterOwnerVersionedReadHook.Store(&held)
	resp = a.read(t, "k3b")
	afterOwnerVersionedReadHook.Store(nil)
	if !after.fired || putErr != "" {
		t.Fatalf("setup: the put after the owner's answer did not land (fired=%v): %s", after.fired, putErr)
	}
	if !resp.Found || resp.Repaired || docField(t, resp.Data, "v") != "landed" {
		t.Errorf("(b) a put after the owner's answer answered found=%v repaired=%v", resp.Found, resp.Repaired)
	}
	time.Sleep(time.Second)
	if tail := a.ownerLog(); tail != tailAfterPut {
		t.Errorf("(b) the owner's log went from %d after the put to %d after the read: the read sent a repair", tailAfterPut, tail)
	}
	a.heldEverywhere(t, "k3b", "landed")
}

// TestARepairDeleteSparesAWriteMadeDuringTheRead: a key the owner deleted and
// one replica still holds — at the version its put gave it, and (the
// version-0 arm) with no version — is read; the coordinator is held after the
// last holder's read, a put of the key lands at all, and the read then sends
// its bounded delete. The read answers not found and repaired, and every node
// still holds the put at its version: the owner applied the delete as spared
// (it recorded no refusal of it) and no replica recorded a divergence.
// Guards: send the delete without the bound (the put is deleted, both arms);
// send an inclusive bound of the highest version read, left off the wire when
// 0 (the version-0 arm's put is deleted).
func TestARepairDeleteSparesAWriteMadeDuringTheRead(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	stale := a.tc.nodes[a.replicas[0]].server.core
	for _, arm := range []struct {
		key     string
		version bool
	}{{key: "stamped", version: true}, {key: "unstamped"}} {
		a.put(t, arm.key, "deleted", "all")
		putVersion := versionOn(t, stale, testDatabase, testBucket, arm.key)
		a.remove(t, arm.key)
		if arm.version {
			writeBackAt(t, stale, testDatabase, testBucket, arm.key, map[string]interface{}{"v": "deleted"}, putVersion)
		} else {
			forceStale(t, stale, testDatabase, testBucket, arm.key, map[string]interface{}{"v": "deleted"})
		}
		var putErr string
		var during hookOnce
		hold := holdWhile(t, &during, a.tc.addrs[a.owner], func() { putErr = putAllDuringRead(a.tc, a.owner, arm.key, "rewritten") })
		beforeRepairDeleteHook.Store(&hold)
		logBefore := a.ownerLog()
		resp := a.read(t, arm.key)
		beforeRepairDeleteHook.Store(nil)
		if !during.fired || putErr != "" {
			t.Fatalf("%s: setup: the put during the read did not land (fired=%v): %s", arm.key, during.fired, putErr)
		}
		if resp.Found || !resp.Repaired {
			t.Errorf("%s: the read answered found=%v repaired=%v; want not found, repaired", arm.key, resp.Found, resp.Repaired)
		}
		time.Sleep(time.Second)
		a.heldEverywhere(t, arm.key, "rewritten")
		requireNoRefusalAbove(t, a, logBefore)
		for _, replica := range a.replicas {
			if record, diverged := clusterOf(a.tc, replica).divergenceNaming(testDatabase, testBucket); diverged {
				t.Errorf("%s: node %d recorded a divergence: %+v", arm.key, replica+1, record)
			}
		}
	}
}

// requireNoRefusalAbove fails if the owner's state document records a
// refusal of an entry above after: a repair delete the owner answered spared
// is applied, and records none.
func requireNoRefusalAbove(t *testing.T, a absenceCluster, after uint64) {
	t.Helper()
	st := lineageOf(t, a.tc.nodes[a.owner].server.core, testDatabase, testBucket)
	for _, refusal := range st.Refusals {
		if refusal.Seq > after {
			t.Errorf("the owner recorded a refusal of sequence %d during the read: it did not apply the repair delete as spared", refusal.Seq)
		}
	}
}

// TestARepairDeleteRemovesAStrayOnAHolderThatDidNotAnswer: the bound is the
// owner's position, not the highest version a holder answered. A key deleted
// at all is written back at v1 into one replica and at v2 > v1 into the other,
// which does not answer the read but still takes the owner's stream. The read
// answers not found, repaired, saying a holder could not answer, and within
// ten seconds no node holds the key. Guard: bound by the highest version an
// answering holder had (the other replica keeps the key at v2).
func TestARepairDeleteRemovesAStrayOnAHolderThatDidNotAnswer(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	r1, r2 := a.replicas[0], a.replicas[1]
	a.put(t, "k", "first", "all")
	v1 := versionOn(t, a.tc.nodes[r1].server.core, testDatabase, testBucket, "k")
	a.put(t, "k", "second", "all")
	v2 := versionOn(t, a.tc.nodes[r2].server.core, testDatabase, testBucket, "k")
	a.remove(t, "k")
	writeBackAt(t, a.tc.nodes[r1].server.core, testDatabase, testBucket, "k", map[string]interface{}{"v": "first"}, v1)
	writeBackAt(t, a.tc.nodes[r2].server.core, testDatabase, testBucket, "k", map[string]interface{}{"v": "second"}, v2)
	defer failVersionedGets(t, clusterOf(a.tc, a.owner), a.tc.addrs[r2])()
	resp := a.read(t, "k")
	if resp.Found || !resp.Repaired || !strings.Contains(resp.Message, "could not answer") {
		t.Errorf("the read answered found=%v repaired=%v message=%q; want not found, repaired, a holder that could not answer", resp.Found, resp.Repaired, resp.Message)
	}
	requireNowhere(t, a.tc, testDatabase, testBucket, "k", 0, 1, 2)
}

// TestAnOwnerThatOwesHasNoSayOverAbsence: the owner's apply of a delete
// failed, so it owes it and still holds the key the replicas deleted. The
// read answers as the comparison does, repairs nothing — a put repair would
// be logged after the delete the owner owes — and says the owner owes an
// apply. Guard: ignore Owes (a repair put is logged).
func TestAnOwnerThatOwesHasNoSayOverAbsence(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	a.put(t, "owed", "kept", "all")
	failed := failNextApplyOn(t, a.tc.nodes[a.owner].server.core.kv, anySeq)
	a.tc.frame(a.owner, transport.OpDelete, deleteAt(testDatabase, testBucket, "owed", "all"))
	if failed.Load() == 0 {
		t.Fatal("setup: the owner's apply of the delete did not fail")
	}
	logBefore := a.ownerLog()
	resp := a.read(t, "owed")
	if !resp.Found || resp.Repaired {
		t.Errorf("an owner that owes the delete answered found=%v repaired=%v; want the comparison's answer and no repair", resp.Found, resp.Repaired)
	}
	if !strings.Contains(resp.Message, "owes an apply") || !strings.Contains(resp.Message, a.tc.addrs[a.owner]) {
		t.Errorf("the message %q does not name the owner's owed apply", resp.Message)
	}
	time.Sleep(time.Second)
	if a.ownerLogged(t, transport.OpPut, logBefore) {
		t.Errorf("the read logged a repair put through an owner that owes an apply")
	}
}

// TestAnOwnerWhoseLogIsShortHasNoSayOverAbsence: the construction of
// TestConsistentGetDoesNotResurrectADeletedKey's first arm — a key deleted at
// all and written back into a replica at its put's version —
// read from an owner whose log tail, as its answer reads it, is below its
// position. The owner's absence has no say: the read answers as the
// comparison does and sends no delete. Guard: report authority without the
// tail comparison (a bounded delete is logged).
func TestAnOwnerWhoseLogIsShortHasNoSayOverAbsence(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	stale := a.tc.nodes[a.replicas[0]].server.core
	a.put(t, "k1", "deleted", "all")
	putVersion := versionOn(t, stale, testDatabase, testBucket, "k1")
	a.remove(t, "k1")
	writeBackAt(t, stale, testDatabase, testBucket, "k1", map[string]interface{}{"v": "deleted"}, putVersion)
	short := func(nodeID string, tail uint64) uint64 {
		if nodeID == a.tc.addrs[a.owner] {
			return 0
		}
		return tail
	}
	ownerLogTailHook.Store(&short)
	defer ownerLogTailHook.Store(nil)
	logBefore := a.ownerLog()
	resp := a.read(t, "k1")
	if !resp.Found {
		t.Errorf("an owner whose log is short answered found=%v repaired=%v; want the comparison's answer", resp.Found, resp.Repaired)
	}
	time.Sleep(time.Second)
	if a.ownerLogged(t, transport.OpDelete, logBefore) {
		t.Errorf("the read logged a repair delete through an owner whose log is short")
	}
}

// TestAWriteItsOwnerRefusedAsTooLargeIsRemovedByAConsistentGet pins an
// intended consequence: a put the owner refused as too large for its batch,
// which the replicas kept, is above the owner's position until the owner's
// next write to the bucket, so a read before it answers the replicas' copy
// and repairs nothing; after it the owner's absence covers the put, and a
// read answers not found and removes it. Guard: count a too-large refusal as
// owed (the second read answers the replicas' copy).
func TestAWriteItsOwnerRefusedAsTooLargeIsRemovedByAConsistentGet(t *testing.T) {
	a := newAbsenceCluster(t)
	defer a.tc.stop()
	refuseNextCommitAsTooLarge(t, a.tc.nodes[a.owner].server.core.kv)
	frame := a.tc.frame(a.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k4",
		Data: map[string]interface{}{"v": "large"}, WriteConcern: "all"})
	if outcome := writeOutcomeOf(t, frame); outcome != outcomeUnknown {
		t.Fatalf("setup: the put the owner refused as too large answered write_outcome %q, want unknown: %s", outcome, frame.Payload)
	}
	if found, _, _, _ := a.tc.nodes[a.owner].server.core.versionedFind(testDatabase, testBucket, "k4"); found {
		t.Fatal("setup: the owner holds the put it refused as too large")
	}
	for _, replica := range a.replicas {
		pollUntil(t, "the replicas hold the put the owner refused", func() bool {
			found, _, _, err := a.tc.nodes[replica].server.core.versionedFind(testDatabase, testBucket, "k4")
			return err == nil && found
		})
	}
	logBefore := a.ownerLog()
	resp := a.read(t, "k4")
	if !resp.Found || resp.Repaired {
		t.Errorf("before the owner's next write the read answered found=%v repaired=%v; want the replicas' copy, unrepaired", resp.Found, resp.Repaired)
	}
	if tail := a.ownerLog(); tail != logBefore {
		t.Errorf("the first read grew the owner's log from %d to %d", logBefore, tail)
	}
	a.put(t, "k5", "next", "all")
	resp = a.read(t, "k4")
	if resp.Found || !resp.Repaired {
		t.Errorf("after the owner's next write the read answered found=%v repaired=%v; want not found, repaired", resp.Found, resp.Repaired)
	}
	requireNowhere(t, a.tc, testDatabase, testBucket, "k4", 0, 1, 2)
}

// writeOutcomeOf reads write_outcome from a write's answer or refusal.
func writeOutcomeOf(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	var answer struct {
		WriteOutcome string `bson:"write_outcome"`
	}
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the write's answer: %v", err)
	}
	return answer.WriteOutcome
}
