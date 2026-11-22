package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 9, step 9.1: a term and a position in the bucket's state document
// (T-114, T-115, T-116; A-9.1).

// lineageOf reads a node's own state document for a bucket.
func lineageOf(t *testing.T, s *Server, database, bucket string) store.BucketState {
	t.Helper()
	st, err := s.bucketStateNamed(database, bucket)
	if err != nil {
		t.Fatalf("reading the state of %s.%s: %v", database, bucket, err)
	}
	return st
}

// lineageBelow reports whether a's (term, through) compares below b's.
func lineageBelow(a, b store.BucketState) bool {
	if a.Term != b.Term {
		return a.Term < b.Term
	}
	return a.Through < b.Through
}

// lastOwnSeq is the sequence of the last entry of bucket this node logged as
// its owner, leaving out the opcodes in skip.
func lastOwnSeq(t *testing.T, s *Server, bucket string, skip ...transport.Opcode) uint64 {
	t.Helper()
	entries, err := s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	var last uint64
	for _, e := range entries {
		if e.Bucket != bucket || e.Origin.Kind != storage.WALOriginOwn {
			continue
		}
		skipped := false
		for _, op := range skip {
			if transport.Opcode(e.Op) == op {
				skipped = true
			}
		}
		if !skipped {
			last = e.Seq
		}
	}
	return last
}

// TestEveryCopyOfABucketRecordsTheSameLineage is T-114: a hash, a FIFO and a
// heap bucket at factor 3, twenty writes each, a pop, an insert a unique index
// refuses and a $inc every copy refuses. On every node's own store the
// bucket's (term, through) is the same pair, through is the owner's sequence
// of the last entry that applied, and only the owner's copy is opened. Guard:
// a replica records its own log's sequence as the position — the copies then
// differ.
func TestEveryCopyOfABucketRecordsTheSameLineage(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	// One node's log gains entries the others' do not, so no node's own
	// sequences are the owner's by coincidence.
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "lineage_padding", Type: BucketTypeHash, ReplicationFactor: 1})
	waitForServing(t, tc, testDatabase, "lineage_padding", 1)
	for i := 0; i < 7; i++ {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "lineage_padding", Key: fmt.Sprintf("p%d", i),
			Data: map[string]interface{}{"n": i}})
	}
	const (
		hashBucket = "lineage_hash"
		fifoBucket = "lineage_fifo"
		heapBucket = "lineage_heap"
	)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hashBucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{uniqueOn("Email")}}})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifoBucket, Type: BucketTypeFIFO})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: heapBucket, Type: BucketTypeHeap})
	for _, bucket := range []string{hashBucket, fifoBucket, heapBucket} {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}

	for i := 0; i < 20; i++ {
		tc.ok(i%3, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: hashBucket, Key: fmt.Sprintf("k%02d", i),
			Data: map[string]interface{}{"Email": fmt.Sprintf("u%02d@example.com", i)}, WriteConcern: "all"})
		tc.ok(i%3, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: fifoBucket,
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		tc.ok(i%3, transport.OpHeapPush, PushRequest{Database: testDatabase, Bucket: heapBucket,
			Data: map[string]interface{}{"n": i}, Priority: int64(i), WriteConcern: "all"})
	}
	tc.ok(1, transport.OpFIFOPop, PopRequest{Database: testDatabase, Bucket: fifoBucket})
	tc.ok(2, transport.OpHeapPop, PopRequest{Database: testDatabase, Bucket: heapBucket})
	if frame := tc.frame(1, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: hashBucket, Key: "duplicate",
		Data: map[string]interface{}{"Email": "u00@example.com"}, WriteConcern: "all"}); frame.Opcode != transport.OpError {
		t.Fatalf("test setup: an insert of a duplicate Email was answered %s, want a refusal", frame.Opcode)
	}
	if frame := tc.frame(2, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: hashBucket, Key: "k00",
		Inc: map[string]int64{"Email": 1}}); frame.Opcode != transport.OpError {
		t.Fatalf("test setup: a $inc of a string was answered %s, want a refusal", frame.Opcode)
	}

	for _, bucket := range []string{hashBucket, fifoBucket, heapBucket} {
		ownerIndex := ownerIndexOf(t, tc, bucket)
		// The refused $inc is logged and applies nowhere.
		want := lastOwnSeq(t, tc.nodes[ownerIndex].server.core, bucket, transport.OpUpdate)
		var states [3]store.BucketState
		agreed := eventually(func() bool {
			for index := range tc.nodes {
				states[index] = lineageOf(t, tc.nodes[index].server.core, testDatabase, bucket)
			}
			for index := range tc.nodes {
				if states[index].Term != states[0].Term || states[index].Through != want {
					return false
				}
			}
			return true
		})
		if !agreed {
			t.Errorf("bucket %s: the copies' (term, through) are (%d, %d), (%d, %d), (%d, %d); want all equal, through %d (the owner's sequence of the last entry that applied)",
				bucket, states[0].Term, states[0].Through, states[1].Term, states[1].Through, states[2].Term, states[2].Through, want)
		}
		if states[ownerIndex].Term != InitialEpoch {
			t.Errorf("bucket %s: the owner's copy is in term %d; a bucket created at epoch %d starts in that term", bucket, states[ownerIndex].Term, InitialEpoch)
		}
		for index := range tc.nodes {
			if opened := states[index].Opened; opened != (index == ownerIndex) {
				t.Errorf("bucket %s: node %d's copy has opened=%v; only the owner (node %d) opened it", bucket, index+1, opened, ownerIndex+1)
			}
		}
	}
}

// TestABucketsPositionSurvivesAKillAndAReplay is T-115. A replica killed after
// its log append and before its apply reached the store replays the entry at
// its next start, and records the origin's sequence, not its own log's. An
// owner whose store failed a write replays it, and its position then matches
// its replicas'. Guard: replay records the log's own sequence.
func TestABucketsPositionSurvivesAKillAndAReplay(t *testing.T) {
	t.Run("a replica killed between its log append and its apply", func(t *testing.T) {
		s, kv, c, dataDir := newReplicaOnFaultStore(t)
		const firstOriginSeq = 100
		for seq := uint64(firstOriginSeq); seq < firstOriginSeq+3; seq++ {
			entry := ReplEntry{Seq: seq, Op: int32(transport.OpPut), Bucket: "held", Key: []byte("k"),
				Payload: putPayload(t, "app", "held", fmt.Sprintf("k%d", seq))}
			if _, err := c.applyAsReplica(context.Background(), entry, true, "test-owner", true); err != nil {
				t.Fatalf("applying origin sequence %d: %v", seq, err)
			}
		}
		// The power cut takes every apply the store committed without a sync;
		// the log synced each append.
		restarted := killedAfterPowerLoss(t, dataDir, kv, s)
		if found, err := restarted.s.Find(FindRequest{Database: "app", Bucket: "held", Key: "k102"}); err == nil && found.Found {
			t.Fatal("test setup: the last apply survived the power cut, so nothing is left to replay")
		}
		if err := restarted.s.replayWAL(context.Background()); err != nil {
			t.Fatalf("replaying the log: %v", err)
		}
		if found, err := restarted.s.Find(FindRequest{Database: "app", Bucket: "held", Key: "k102"}); err != nil || !found.Found {
			t.Fatalf("after the replay the replica does not hold the last entry (%v)", err)
		}
		if got := lineageOf(t, restarted.s, "app", "held").Through; got != firstOriginSeq+2 {
			t.Errorf("after the replay the replica's copy is through %d; its owner's sequence of the last entry is %d", got, firstOriginSeq+2)
		}
	})

	t.Run("an owner restarted with a write its store failed", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, testBucket, 3)
		ownerIndex := ownerIndexOf(t, tc, testBucket)
		for i := 0; i < 5; i++ {
			putValue(t, tc, ownerIndex, fmt.Sprintf("k%d", i), "v", "all")
		}
		failed := failNextApplyOn(t, tc.nodes[ownerIndex].server.core.kv, anySeq)
		putValue(t, tc, ownerIndex, "failed", "v", "all")
		failedSeq := failed.Load()
		if failedSeq == 0 {
			t.Fatal("test setup: the owner's store failed no apply")
		}
		store.ApplyFaultHook.Store(nil)
		tc.restartNode(ownerIndex)

		var states [3]store.BucketState
		agreed := eventually(func() bool {
			for index := range tc.nodes {
				states[index] = lineageOf(t, tc.nodes[index].server.core, testDatabase, testBucket)
			}
			return states[0].Through == failedSeq && states[1].Through == failedSeq && states[2].Through == failedSeq
		})
		if !agreed {
			t.Errorf("after the owner (node %d) replayed the write its store failed, the copies are through %d, %d and %d; want %d on each",
				ownerIndex+1, states[0].Through, states[1].Through, states[2].Through, failedSeq)
		}
	})
}

// bucketReplicatedTo returns a bucket name whose owner is the same on the
// three nodes of tc and on the ring with joiner added, and whose set on the
// second ring holds joiner as a replica: the joiner then pulls it from the
// owner (Cluster.ensureMigrated).
func bucketReplicatedTo(t *testing.T, tc *testCluster, joiner string) string {
	t.Helper()
	before, after := ring.New(3), ring.New(3)
	for _, id := range append(append([]string(nil), tc.addrs...), joiner) {
		if id != joiner {
			if err := before.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
				t.Fatalf("building a ring: %v", err)
			}
		}
		if err := after.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("pulled%03d", i)
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		newSet, err := after.Replicas(key, 3)
		if err != nil || newSet[0].ID != oldSet[0].ID {
			continue
		}
		for _, member := range newSet[1:] {
			if member.ID == joiner {
				return name
			}
		}
	}
	t.Fatalf("no bucket name of 1000 keeps its owner and places a replica on %s", joiner)
	return ""
}

// TestASnapshotCarriesItsLineageAndNotItsOwnership is T-116: a fourth node
// joins and pulls a bucket from its owner. Its copy's (term, through) is the
// owner's at the snapshot, and it is not opened. Guard: materializeRange keeps
// the source's opened.
func TestASnapshotCarriesItsLineageAndNotItsOwnership(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	bucket := bucketReplicatedTo(t, tc, ports.ringAddr)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	ownerIndex := ownerIndexOf(t, tc, bucket)
	for i := 0; i < 10; i++ {
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	owner := tc.nodes[ownerIndex].server.core
	source := lineageOf(t, owner, testDatabase, bucket)
	if want := lastOwnSeq(t, owner, bucket); source.Through != want || !source.Opened {
		t.Fatalf("the owner's copy is through %d, opened %v; want through %d (its sequence of the last write), opened", source.Through, source.Opened, want)
	}

	joiner := joinNewcomer(t, tc, ownerIndex, ports)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := joiner.cluster().ensureMigrated(ctx, testDatabase, bucket); err != nil {
		t.Fatalf("the joiner pulling %s from its owner: %v", bucket, err)
	}
	pulled := lineageOf(t, joiner.cluster().s, testDatabase, bucket)
	if pulled.Term != source.Term || pulled.Through != source.Through {
		t.Errorf("the pulled copy's (term, through) is (%d, %d); the owner's at the snapshot is (%d, %d)",
			pulled.Term, pulled.Through, source.Term, source.Through)
	}
	if pulled.Opened {
		t.Error("the pulled copy is opened: a copy materialised from a snapshot follows its source and never originated anything")
	}
}

// bucketsForARecreate returns the names TestARecreatedBucketIsNeverMistakenForTheOneItReplaced
// needs on four nodes at factor 2: a bucket, a padding bucket with the same
// owner, and that owner. The padding bucket is created at factor 1, so its
// writes lengthen the owner's log and no other node's.
func bucketsForARecreate(t *testing.T, addrs []string) (bucket, padding, removed string) {
	t.Helper()
	full := ring.New(2)
	for _, id := range addrs {
		if err := full.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	ownerOf := func(name string) string {
		owner, err := full.Owner(placementOf(testDatabase, name).encoded)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		return owner.ID
	}
	bucket = "recreated"
	removed = ownerOf(bucket)
	for i := 0; i < 1000; i++ {
		padding = fmt.Sprintf("padding%03d", i)
		if ownerOf(padding) == removed {
			return bucket, padding, removed
		}
	}
	t.Fatal("no padding bucket name of 1000 has the owner of the bucket under test")
	return "", "", ""
}

// TestARecreatedBucketIsNeverMistakenForTheOneItReplaced is A-9.1's test (Q46:
// dispatch 9 committed the clauses it could observe, and step 10b.5 adds the
// new owner's ten writes). A bucket is written through an owner whose log is
// long; that owner is removed while running and keeps its copy; the bucket is
// dropped and created again under the next owner, whose log is short, and
// written ten times at `all`. On every holder the re-created bucket's term is
// at or above the dropped bucket's last term, and the copy the removed node
// kept compares below the live one; the re-created bucket's owner is then
// stopped and removed, and the next owner holds the ten and nothing of the
// dropped bucket. And a replica killed between its log
// append of a create and its apply holds, after its restart, the term the
// owner stamped in the create. Guard: term 0 at create — the kept copy, at a
// higher sequence of another origin, then compares above the live one.
func TestARecreatedBucketIsNeverMistakenForTheOneItReplaced(t *testing.T) {
	t.Run("a dropped bucket's kept copy compares below its successor", func(t *testing.T) {
		tc := newTestCluster(t, 4, 2)
		defer tc.stop()
		bucket, padding, removedID := bucketsForARecreate(t, tc.addrs)
		removedIndex := nodeIndexByAddr(t, tc.addrs, removedID)
		via := (removedIndex + 1) % len(tc.nodes)
		tc.ok(via, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(via, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: padding, Type: BucketTypeHash, ReplicationFactor: 1})
		waitForServing(t, tc, testDatabase, padding, 1)
		for i := 0; i < 120; i++ {
			tc.ok(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: padding, Key: fmt.Sprintf("p%03d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}
		tc.ok(via, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 2)
		for i := 0; i < 10; i++ {
			tc.ok(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("old%d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}

		removed := tc.nodes[removedIndex].server.core
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := clusterOf(tc, via).proposeRemoval(ctx, removedID); err != nil {
			t.Fatalf("removing the bucket's owner, node %d: %v", removedIndex+1, err)
		}
		// What the next gossip round would tell it.
		viaCluster := clusterOf(tc, via)
		if err := removed.cluster.reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
			t.Fatalf("telling the removed node of its removal: %v", err)
		}
		if removed.cluster.removedAt.Load() == 0 {
			t.Fatal("test setup: the removed node does not know it was removed")
		}
		// The removed node keeps its copy, and the two members of the bucket's
		// set under the new membership serve it. Waiting for those two, not for
		// any three: the old replica outside the set serves it too until the
		// set confirms, and a count of three met by it let the drop below run
		// during the hand-off.
		waitForSetMembersServing(t, tc, viaCluster, testDatabase, bucket)
		// The removed node's hand-off pass asks this bucket's members to take it
		// over (removed_handoff.go). If the pass is still asking when the bucket
		// is dropped and created again below, the owner answers an ask about the
		// dropped incarnation by starting a take-over, and the re-create is
		// refused with the designed retryable "owns it ... copying it" answer
		// (BUG-0503). Wait for the pass to name every bucket and end before the
		// drop, so no ask can be in flight across it.
		if !holdsWithin(takeOverWait, func() bool { return !removed.cluster.removedHandOffRunning.Load() }) {
			t.Fatal("the removed node's hand-off pass never ended, so the drop and re-create below would race its asks")
		}
		kept := lineageOf(t, removed, testDatabase, bucket)
		var droppedTerm uint64
		for _, node := range tc.nodes {
			if node.server.core.bucketStoreOpen(testDatabase, bucket) {
				droppedTerm = max(droppedTerm, lineageOf(t, node.server.core, testDatabase, bucket).Term)
			}
		}

		tc.ok(via, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
		waitForServing(t, tc, testDatabase, bucket, 1)
		tc.ok(via, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForSetMembersServing(t, tc, viaCluster, testDatabase, bucket)
		for i := 0; i < 10; i++ {
			tc.ok(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("new%d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}

		holders := 0
		for index, node := range tc.nodes {
			if index == removedIndex || !node.server.core.bucketStoreOpen(testDatabase, bucket) {
				continue
			}
			holders++
			live := lineageOf(t, node.server.core, testDatabase, bucket)
			if live.Term < droppedTerm {
				t.Errorf("node %d: the re-created bucket is in term %d, below the dropped bucket's last term %d", index+1, live.Term, droppedTerm)
			}
			if !lineageBelow(kept, live) {
				t.Errorf("node %d: the copy the removed node kept of the dropped bucket, (term %d, through %d), does not compare below the live bucket's (term %d, through %d): a fallback would take the dropped bucket over the live one",
					index+1, kept.Term, kept.Through, live.Term, live.Through)
			}
		}
		if holders != 2 {
			t.Fatalf("test setup: %d members hold the re-created bucket, want 2", holders)
		}

		// Step 10b.5's clause: the re-created bucket's owner is stopped and
		// removed; the next owner holds the ten and nothing of the dropped
		// bucket, whose copy the removed node still answers about.
		var members []string
		for index, addr := range tc.addrs {
			if index != removedIndex {
				members = append(members, addr)
			}
		}
		owner := nodeIndexByAddr(t, tc.addrs, ownerOn(ringWith(t, 2, members...), bucket))
		var rest []string
		for _, addr := range members {
			if addr != tc.addrs[owner] {
				rest = append(rest, addr)
			}
		}
		next := nodeIndexByAddr(t, tc.addrs, ownerOn(ringWith(t, 2, rest...), bucket))
		tc.stopNodeKeepingData(owner)
		nextCluster := clusterOf(tc, next)
		if err := nextCluster.proposeRemoval(ctx, tc.addrs[owner]); err != nil {
			t.Fatalf("removing the re-created bucket's owner, node %d: %v", owner+1, err)
		}
		tc.bringMembershipTo(t, nextCluster)
		waitTakenOver(t, tc.nodes[next].server.core, testDatabase, bucket)
		for i := 0; i < 10; i++ {
			if key := fmt.Sprintf("new%d", i); !holdsKeyIn(tc.nodes[next], testDatabase, bucket, key) {
				t.Errorf("the next owner, node %d, lacks %s of the re-created bucket", next+1, key)
			}
			if key := fmt.Sprintf("old%d", i); holdsKeyIn(tc.nodes[next], testDatabase, bucket, key) {
				t.Errorf("the next owner, node %d, holds %s of the dropped bucket: it took the removed node's kept copy", next+1, key)
			}
		}
	})

	t.Run("a replica killed between its log append of a create and its apply", func(t *testing.T) {
		s, kv, c, dataDir := newReplicaOnFaultStore(t)
		generations, err := s.liveGenerations("app")
		if err != nil || len(generations) == 0 {
			t.Fatalf("test setup: the generation of database app: %v %v", generations, err)
		}
		const ownersTerm = 7
		payload, err := bson.Marshal(CreateBucketRequest{Database: "app", Name: "fresh", Type: BucketTypeHash,
			Term: ownersTerm, DatabaseGeneration: generations[len(generations)-1]})
		if err != nil {
			t.Fatalf("encoding the create: %v", err)
		}
		create := ReplEntry{Seq: 200, Op: int32(transport.OpCreateBucket), Bucket: "fresh", Payload: payload}
		if _, err := c.applyAsReplica(context.Background(), create, true, "test-owner", true); err != nil {
			t.Fatalf("applying the create: %v", err)
		}
		restarted := killedAfterPowerLoss(t, dataDir, kv, s)
		if restarted.s.bucketStoreOpen("app", "fresh") {
			t.Fatal("test setup: the create's apply survived the power cut, so nothing is left to replay")
		}
		if err := restarted.s.replayWAL(context.Background()); err != nil {
			t.Fatalf("replaying the log: %v", err)
		}
		if !restarted.s.bucketStoreOpen("app", "fresh") {
			t.Fatal("after the replay the replica does not hold the bucket")
		}
		if got := lineageOf(t, restarted.s, "app", "fresh"); got.Term != ownersTerm || got.Through != 200 {
			t.Errorf("after the replay the replica's copy is (term %d, through %d); the owner logged the create in term %d at sequence 200",
				got.Term, got.Through, ownersTerm)
		}
	})
}

// TestARecreateWaitsOutARemovedNodesInFlightAsk is the deterministic form of
// BUG-0503. A removed node's hand-off pass asks the bucket's members to take
// its copy over. When an ask lands after the bucket's drop, the owner starts a
// take-over of the name, and a create sent before that take-over ends is
// refused with the designed retryable answer a coordinator renders as
// cluster_join.go:awaitingCopyAt (catalog_question.go's refuseCreateBesideCopy,
// or takeover.go's handoffInProgress).
//
// The removed node's ask is held open in askNodeToMigrateHook, so it is
// provably in flight across the drop. The other set member's apply of the drop
// is held in afterReplicaAdmittedHook, which fences it: it still holds the live
// incarnation the take-over copies from, so the take-over cannot end and the
// create below would be refused on every run without the wait. The wait
// releases that member and waits for the take-over and the removed node's pass
// to end before the create, so no ask is in flight across it.
func TestARecreateWaitsOutARemovedNodesInFlightAsk(t *testing.T) {
	tc := newTestCluster(t, 4, 2)
	defer tc.stop()
	bucket, padding, removedID := bucketsForARecreate(t, tc.addrs)
	removedIndex := nodeIndexByAddr(t, tc.addrs, removedID)
	via := (removedIndex + 1) % len(tc.nodes)
	tc.ok(via, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(via, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: padding, Type: BucketTypeHash, ReplicationFactor: 1})
	waitForServing(t, tc, testDatabase, padding, 1)
	for i := 0; i < 120; i++ {
		tc.ok(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: padding, Key: fmt.Sprintf("p%03d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	tc.ok(via, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	for i := 0; i < 10; i++ {
		tc.ok(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("old%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}

	// Hold the removed node's ask for the bucket open, so the ask is provably in
	// flight when the bucket is dropped and created again below.
	askInFlight := make(chan struct{})
	releaseAsk := make(chan struct{})
	releaseAskOnce := sync.OnceFunc(func() { close(releaseAsk) })
	defer releaseAskOnce()
	var askOnce sync.Once
	holdAsk := func(fromID, _, _, name string) error {
		if fromID != removedID || name != bucket {
			return nil
		}
		askOnce.Do(func() { close(askInFlight) })
		<-releaseAsk
		return nil
	}
	askNodeToMigrateHook.Store(&holdAsk)
	defer askNodeToMigrateHook.Store(nil)

	removed := tc.nodes[removedIndex].server.core
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clusterOf(tc, via).proposeRemoval(ctx, removedID); err != nil {
		t.Fatalf("removing the bucket's owner, node %d: %v", removedIndex+1, err)
	}
	viaCluster := clusterOf(tc, via)
	if err := removed.cluster.reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
		t.Fatalf("telling the removed node of its removal: %v", err)
	}
	waitForSetMembersServing(t, tc, viaCluster, testDatabase, bucket)
	select {
	case <-askInFlight:
	case <-time.After(20 * time.Second):
		t.Fatalf("the removed node's hand-off pass never asked about %s", bucket)
	}

	// Hold the other set member's apply of the drop: its fence keeps the
	// owner's take-over of the dropped name running until the wait below
	// releases it.
	set, err := viaCluster.Replicas(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("placing %s.%s: %v", testDatabase, bucket, err)
	}
	otherID := set[1].ID
	dropAdmitted := make(chan struct{})
	releaseDrop := make(chan struct{})
	releaseDropOnce := sync.OnceFunc(func() { close(releaseDrop) })
	defer releaseDropOnce()
	var admitOnce sync.Once
	holdDrop := func(c *Cluster, entry ReplEntry) {
		if c.self.ID != otherID || transport.Opcode(entry.Op) != transport.OpDeleteBucket || entry.Bucket != bucket {
			return
		}
		admitOnce.Do(func() {
			close(dropAdmitted)
			<-releaseDrop
		})
	}
	afterReplicaAdmittedHook.Store(&holdDrop)
	defer afterReplicaAdmittedHook.Store(nil)

	tc.ok(via, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	select {
	case <-dropAdmitted:
	case <-time.After(20 * time.Second):
		t.Fatalf("the other set member never admitted the drop of %s", bucket)
	}
	releaseAskOnce()
	owner := clusterOf(tc, nodeIndexByAddr(t, tc.addrs, set[0].ID))
	if !holdsWithin(takeOverWait, func() bool { return owner.pullPending(testDatabase, bucket) }) {
		t.Fatalf("the owner never started taking the dropped name over after the removed node's ask")
	}

	// Do not race the ask: release the held apply and wait for the owner's
	// take-over of the dropped name and the removed node's pass to end, or the
	// owner refuses the create with its designed retryable "has no local copy
	// of it" answer (BUG-0503).
	releaseDropOnce()
	if !holdsWithin(takeOverWait, func() bool {
		return !owner.pullPending(testDatabase, bucket) && !removed.cluster.removedHandOffRunning.Load()
	}) {
		t.Fatal("the owner's take-over of the dropped name or the removed node's hand-off pass never ended")
	}

	tc.ok(via, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
}

// TestACreatesTermTravelsAsTerm pins the wire name of a create's first term
// (A-9.1), and that a create without one — logged before this build — carries
// no such element.
func TestACreatesTermTravelsAsTerm(t *testing.T) {
	encoded, err := bson.Marshal(CreateBucketRequest{Database: "app", Name: "b", Type: BucketTypeHash, Term: 3})
	if err != nil {
		t.Fatalf("encoding a create: %v", err)
	}
	if term, ok := bson.Raw(encoded).Lookup("term").AsInt64OK(); !ok || term != 3 {
		t.Fatalf("a create in term 3 carries term %v (present %v): %v", term, ok, bson.Raw(encoded))
	}
	encoded, err = bson.Marshal(CreateBucketRequest{Database: "app", Name: "b", Type: BucketTypeHash})
	if err != nil {
		t.Fatalf("encoding a create: %v", err)
	}
	if _, err := bson.Raw(encoded).LookupErr("term"); err == nil {
		t.Fatalf("a create with no term encodes a term element: %v", bson.Raw(encoded))
	}
}

// waitForSetMembersServing waits until every member of database/bucket's
// replica set, as via places it, serves the bucket.
func waitForSetMembersServing(t *testing.T, tc *testCluster, via *Cluster, database, bucket string) {
	t.Helper()
	set, err := via.Replicas(placementOf(database, bucket))
	if err != nil {
		t.Fatalf("placing %s.%s: %v", database, bucket, err)
	}
	serving := func() bool {
		for _, member := range set {
			if !tc.nodes[nodeIndexByAddr(t, tc.addrs, member.ID)].server.core.bucketStoreOpen(database, bucket) {
				return false
			}
		}
		return true
	}
	if !eventually(serving) {
		t.Fatalf("the members of %s.%s's set %v do not all serve it", database, bucket, nodeIDs(set))
	}
}
