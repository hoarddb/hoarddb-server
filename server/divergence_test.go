package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.8 (R8-E; review B-1, M-6): a replica never
// acknowledges what it did not apply, live or from the log, and `migrate`
// re-seeds it.

// divergencesCounted reads hoarddb_replica_divergences_total.
func divergencesCounted(t *testing.T) float64 {
	t.Helper()
	var metric dto.Metric
	if err := replicaDivergences.Write(&metric); err != nil {
		t.Fatalf("reading the divergence counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// TestLoggedOwnerOutcomeAnswersAppliedForWhatTheOwnerApplied (review B-1, N-1):
// the owner's word rebuilt from its state document.
func TestLoggedOwnerOutcomeAnswersAppliedForWhatTheOwnerApplied(t *testing.T) {
	st := store.BucketState{AppliedSeq: 20, FailedSeqs: []uint64{7}, LargeCommits: []uint64{25}}
	st.Refuse(5, true, 0)
	st.Refuse(9, false, 0)
	for _, row := range []struct {
		seq  uint64
		want int32
		what string
	}{
		{3, ownerOutcomeApplied, "applied, at or below applied_seq"},
		{5, ownerOutcomeRefusedByConstraint, "refused by a constraint"},
		{9, ownerOutcomeUnknown, "refused for another reason"},
		{7, ownerOutcomeOwed, "a store failure replay owes"},
		{25, ownerOutcomeApplied, "a large commit above applied_seq"},
		{21, ownerOutcomeOwed, "above applied_seq, not applied or refused"},
	} {
		if got := loggedOwnerOutcome(st, row.seq); got != row.want {
			t.Errorf("sequence %d (%s): loggedOwnerOutcome = %d, want %d", row.seq, row.what, got, row.want)
		}
	}
	st.Refuse(30, false, 10)
	if refused, _ := st.RefusalAt(5); refused {
		t.Error("a refusal below the log's first sequence was kept")
	}
}

// divergedDeclaration is the second divergence instance: three nodes at factor 3 and a
// hash bucket whose catalog entry on one replica declares a field type this
// binary cannot read, so that replica refuses every write its owner applies.
type divergedDeclaration struct {
	tc             *testCluster
	bucket, other  string
	owner, replica int
}

func setUpDivergedDeclaration(t *testing.T) *divergedDeclaration {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	full := ringWith(t, 3, tc.addrs...)
	s := &divergedDeclaration{tc: tc, owner: 0, replica: 1}
	s.bucket = bucketNamed(t, "typed", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	s.other = bucketNamed(t, "other", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] && name != s.bucket })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, name := range []string{s.bucket, s.other} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, name, 3)
	}
	held := tc.nodes[s.replica].server.core
	writeRawCatalogEntry(t, held, testDatabase, s.bucket, withRawFields(t, rawCatalogEntry(t, held, testDatabase, s.bucket), unreadableFields()))
	// The replica's take-over worker is held: these tests watch the record
	// that migrate settles, and the worker would re-seed the copy itself
	// (step 10b.6; re-aimed by replication dispatch 10b).
	// resumeReseeds lets it run.
	replicaID := tc.addrs[s.replica]
	paused := func(nodeID string) bool { return nodeID == replicaID }
	takeOverPausedHook.Store(&paused)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	return s
}

// resumeReseeds lets the replica's take-over worker run. It names no bucket
// to it: a test of the automatic re-seed must see the divergence record do
// that.
func (s *divergedDeclaration) resumeReseeds() {
	takeOverPausedHook.Store(nil)
}

// divergedFrom waits for the replica to record a divergence from the owner.
func (s *divergedDeclaration) divergedFrom(t *testing.T) divergenceRecord {
	t.Helper()
	var record divergenceRecord
	recorded := eventually(func() bool {
		var ok bool
		record, ok = clusterOf(s.tc, s.replica).divergenceFrom(s.tc.addrs[s.owner])
		return ok
	})
	if !recorded || record.Reason != refusalDiverged || record.Bucket != s.bucket {
		t.Fatalf("the replica recorded no divergence on %s from the owner: %+v", s.bucket, record)
	}
	return record
}

// TestAReplicaThatCannotEnforceADeclarationDoesNotConfirmTheWrite covers the
// second divergence instance, in three arms. Live: a put at `all` is not confirmed by the
// replica, which records the divergence and counts it once. Re-send: the
// replica restarts, the owner re-sends the same sequence, and it is refused
// before it is logged; the record survived the restart. From the log: the
// replica is stopped before the put and started after, so the entry reaches it
// through the owner's log. Guards: today's loggedOwnerOutcome (the log arm);
// key the record by bucket, not sequence (the re-send arm); mark applied on a
// logical refusal (every arm).
func TestAReplicaThatCannotEnforceADeclarationDoesNotConfirmTheWrite(t *testing.T) {
	t.Run("live, then re-sent", func(t *testing.T) {
		s := setUpDivergedDeclaration(t)
		defer s.tc.stop()
		s.tc.restartNode(s.replica)
		s.tc.waitReady()
		before := divergencesCounted(t)
		frame := s.tc.frame(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k",
			Data: map[string]interface{}{"Age": 1}, WriteConcern: "all"})
		if frame.Opcode != transport.OpError {
			if confirmed, ok := bson.Raw(frame.Payload).Lookup("copies_confirmed").AsInt64OK(); !ok || confirmed >= 3 {
				t.Errorf("a put at all was answered with %d copies confirmed (%v) although one replica could not apply it", confirmed, ok)
			}
		}
		record := s.divergedFrom(t)
		if holdsKeyLocally(s.tc.nodes[s.replica], s.bucket, "k") {
			t.Error("the replica stores the write it refused")
		}
		if got := divergencesCounted(t) - before; got != 1 {
			t.Errorf("hoarddb_replica_divergences_total rose by %v, want 1", got)
		}
		if text := (&replicaDivergence{source: s.tc.addrs[s.owner], record: record}).Error(); !strings.Contains(text, "stopped for every bucket") ||
			!strings.Contains(text, fmt.Sprintf("migrate %s %s", testDatabase, s.bucket)) {
			t.Errorf("the divergence's text does not state its reach and remedy: %s", text)
		}

		logged := s.tc.nodes[s.replica].server.core.wal.LastSequence()
		s.tc.restartNode(s.replica)
		s.tc.waitReady()
		time.Sleep(2 * time.Second)
		if after := s.tc.nodes[s.replica].server.core.wal.LastSequence(); after != logged {
			t.Errorf("the replica's log grew from %d to %d after the owner re-sent the refused entry", logged, after)
		}
		if again, ok := clusterOf(s.tc, s.replica).divergenceFrom(s.tc.addrs[s.owner]); !ok || again.Seq != record.Seq {
			t.Errorf("the divergence record did not survive the restart: %+v", again)
		}
	})
	t.Run("from the log", func(t *testing.T) {
		s := setUpDivergedDeclaration(t)
		defer s.tc.stop()
		base := s.tc.stopNodeKeepingData(s.replica)
		s.tc.ok(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k",
			Data: map[string]interface{}{"Age": 1}, WriteConcern: "majority"})
		s.tc.nodes[s.replica] = s.tc.startNode(s.replica, base, s.tc.token)
		s.divergedFrom(t)
		if holdsKeyLocally(s.tc.nodes[s.replica], s.bucket, "k") {
			t.Error("the replica stores the write it refused")
		}
	})
}

// TestARefusalBeforeTheStoreIsNotADivergenceFromTheLog (review N-1): a replica
// is stopped; on the owner, a delete of a missing key, an update of a missing
// key and an insert a unique index refuses, then a put. The replica restarts
// and catches up from the owner's log: its stream does not stall, the put
// arrives, and nothing is counted diverged. Guard: record only refusals made
// in the store (ApplyRequest.consume) — the delete and the update read
// "applied" from the log, and the stream stalls.
func TestARefusalBeforeTheStoreIsNotADivergenceFromTheLog(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "refusing", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{uniqueOn("Email")}}})
	waitForServing(t, tc, testDatabase, bucket, 3)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "first",
		Data: map[string]interface{}{"Email": "a@example.com"}, WriteConcern: "all"})
	before := divergencesCounted(t)

	base := tc.stopNodeKeepingData(1)
	tc.frame(0, transport.OpDelete, DeleteRequest{Database: testDatabase, Bucket: bucket, Key: "missing"})
	tc.frame(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Key: "missing", Set: map[string]interface{}{"n": 1}})
	tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "duplicate",
		Data: map[string]interface{}{"Email": "a@example.com"}})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "last",
		Data: map[string]interface{}{"Email": "b@example.com"}, WriteConcern: "majority"})
	tc.nodes[1] = tc.startNode(1, base, tc.token)

	if !eventually(func() bool { return holdsKeyLocally(tc.nodes[1], bucket, "last") }) {
		record, diverged := clusterOf(tc, 1).divergenceFrom(tc.addrs[0])
		t.Fatalf("the restarted replica never received the put after three refusals (diverged %v: %+v)", diverged, record)
	}
	if got := divergencesCounted(t) - before; got != 0 {
		t.Errorf("hoarddb_replica_divergences_total rose by %v; a refusal both sides give is not a divergence", got)
	}
}

// TestMigrateReSeedsADivergedReplica (review M-6): instance 2, the replica's
// declaration still unreadable. A write at `all` to another bucket of the same
// owner waits behind the stall; `migrate` on the replica drops its copy and
// pulls the owner's, which carries a readable declaration; the record settles,
// the stream resumes, and the held write completes. Guard: answer "already
// holds".
func TestMigrateReSeedsADivergedReplica(t *testing.T) {
	s := setUpDivergedDeclaration(t)
	defer s.tc.stop()
	s.tc.restartNode(s.replica)
	s.tc.waitReady()
	s.tc.frame(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k",
		Data: map[string]interface{}{"Age": 1}, WriteConcern: "one"})
	s.divergedFrom(t)

	held := make(chan *transport.Frame, 1)
	writer := s.tc.dial(s.owner)
	defer func() { _ = writer.Close() }()
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: s.other, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		frame, err := writer.SendRequest(t.Context(), transport.OpPut, payload)
		if err != nil {
			frame = &transport.Frame{Opcode: transport.OpError, Payload: []byte(err.Error())}
		}
		held <- frame
	}()

	resp := decodedMigrate(t, s.tc.frame(s.replica, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: s.bucket}))
	if !resp.Success || strings.Contains(resp.Message, "already holds") {
		t.Fatalf("migrate on the diverged replica answered %+v; want a re-seed from the owner", resp)
	}
	replica := s.tc.nodes[s.replica].server.core
	if fields := rawCatalogEntry(t, replica, testDatabase, s.bucket).Lookup("fields"); fields.Value != nil &&
		strings.Contains(fields.String(), "polygon") {
		t.Error("the re-seeded copy still carries the unreadable declaration")
	}
	if !eventually(func() bool { _, ok := clusterOf(s.tc, s.replica).divergenceFrom(s.tc.addrs[s.owner]); return !ok }) {
		t.Error("the divergence record never settled after the re-seed")
	}
	select {
	case frame := <-held:
		if frame.Opcode == transport.OpError {
			t.Errorf("the write at all to %s, held behind the stall, failed: %s", s.other, frame.Payload)
		}
	case <-time.After(30 * time.Second):
		t.Error("the write at all to another bucket of the same owner never completed after the re-seed")
	}
	if !holdsKeyLocally(s.tc.nodes[s.replica], s.bucket, "k") {
		t.Error("the re-seeded replica lacks the owner's write")
	}
}

// makeStray is 10a.6's stray: a joiner owns bucket and holds no copy, and
// creates it anyway, as stray, with the join freeze and the owner's question
// off. before, when non-nil, runs between the setup and the stray's create.
func makeStray(t *testing.T, stray BucketType, before func(s joinerWithoutCopies)) (joinerWithoutCopies, string) {
	t.Helper()
	switchJoinFreezeOff(t)
	ownerQuestionPassOverHook.Store(true)
	t.Cleanup(func() { ownerQuestionPassOverHook.Store(false) })
	var bucket string
	s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
		placed := ringWith(t, 3, nodeIDs(joined)...)
		bucket = bucketNamed(t, "stray", func(name string) bool { return ownerOn(placed, name) == joined[len(joined)-1].ID })
		return []string{bucket}
	})
	if before != nil {
		before(s)
	}
	s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, bson.M{"database": testDatabase, "name": bucket, "type": string(stray), "write_concern": "one"})
	if stray == BucketTypeFIFO {
		s.tc.frameOn(s.joinerNode, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"stray": 1}, WriteConcern: "one"})
	} else {
		s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "stray", Data: map[string]interface{}{"stray": 1}, WriteConcern: "one"})
	}
	return s, bucket
}

// refusedStray waits for node index to record the stray's create as another
// incarnation, and checks it stores nothing of the stray's.
func refusedStray(t *testing.T, s joinerWithoutCopies, index int, bucket, when string) {
	t.Helper()
	if !eventually(func() bool {
		record, ok := s.tc.nodes[index].server.core.cluster.divergenceFrom(s.joiner.self.ID)
		return ok && record.Reason == refusalAnotherIncarnation
	}) {
		t.Errorf("%s: node %d did not refuse the stray's create", when, index+1)
	}
	if got := s.tc.bucketCountOn(index, testDatabase, bucket); got != 0 {
		t.Errorf("%s: node %d holds %d of the stray's documents", when, index+1, got)
	}
}

// TestAReplicaOfAnotherIncarnationDoesNotConfirmAPush covers the first
// divergence instance: a holder of one incarnation never acknowledges a push of another's. Arms:
// FIFO over hash and hash over hash, each live, from the log (a holder stopped
// across the stray's create and push), and re-sent (a holder restarted: the
// re-send is refused before it is logged, and the record survives).
func TestAReplicaOfAnotherIncarnationDoesNotConfirmAPush(t *testing.T) {
	for _, stray := range []BucketType{BucketTypeFIFO, BucketTypeHash} {
		t.Run(string(stray)+" over hash", func(t *testing.T) {
			var base string
			s, bucket := makeStray(t, stray, func(s joinerWithoutCopies) { base = s.tc.stopNodeKeepingData(1) })
			refusedStray(t, s, 0, bucket, "live")
			s.tc.nodes[1] = s.tc.startNode(1, base, s.tc.token)
			refusedStray(t, s, 1, bucket, "from the log")

			logged := s.tc.nodes[0].server.core.wal.LastSequence()
			record, _ := s.tc.nodes[0].server.core.cluster.divergenceFrom(s.joiner.self.ID)
			s.tc.restartNode(0)
			time.Sleep(2 * time.Second)
			if after := s.tc.nodes[0].server.core.wal.LastSequence(); after != logged {
				t.Errorf("re-sent: node 1's log grew from %d to %d", logged, after)
			}
			if again, ok := s.tc.nodes[0].server.core.cluster.divergenceFrom(s.joiner.self.ID); !ok || again.Seq != record.Seq {
				t.Errorf("re-sent: the record did not survive the restart: %+v", again)
			}
			refusedStray(t, s, 0, bucket, "re-sent")
		})
	}
}

// decodedMigrate decodes a migrate's answer, an error frame's message as its
// Error.
func decodedMigrate(t *testing.T, frame *transport.Frame) MigrateResponse {
	t.Helper()
	var resp MigrateResponse
	if frame.Opcode == transport.OpError {
		resp.Error = refusalOf(t, frame).Message
		return resp
	}
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the migrate answer: %v", err)
	}
	return resp
}

// TestMigrateRefusesToReSeedACopyOfAnotherIncarnation (review M-6, R8-E): a
// replica diverged on an entry of its owner's (instance 2), and its copy is of
// another incarnation than the owner's — here its state document is rewritten
// to one. `migrate` copies nothing over a different bucket of the same name: it
// refuses, names both incarnations, and the copy stays. (A holder that refused
// a create of another incarnation is the other case, and owner ruling 53 has
// migrate re-seed it: TestMigrateReSeedsAHolderOfAnotherIncarnation.) Guard:
// re-seed regardless.
func TestMigrateRefusesToReSeedACopyOfAnotherIncarnation(t *testing.T) {
	s := setUpDivergedDeclaration(t)
	defer s.tc.stop()
	s.tc.restartNode(s.replica)
	s.tc.waitReady()
	s.tc.frame(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k",
		Data: map[string]interface{}{"Age": 1}, WriteConcern: "one"})
	s.divergedFrom(t)
	replica := s.tc.nodes[s.replica].server.core
	owned := lineageOf(t, s.tc.nodes[s.owner].server.core, testDatabase, s.bucket).Incarnation
	held := owned ^ 0x5a5a
	b := lookupBucket(replica, testDatabase, s.bucket)
	st := lineageOf(t, replica, testDatabase, s.bucket)
	st.Incarnation = held
	batch := replica.kv.NewBatch()
	if err := st.Put(batch, b.slot, b.databaseID, b.id.Load()); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()

	resp := decodedMigrate(t, s.tc.frame(s.replica, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: s.bucket}))
	if resp.Success {
		t.Fatalf("migrate re-seeded a copy of incarnation %d from an owner of incarnation %d: %+v", held, owned, resp)
	}
	for _, incarnation := range []uint64{held, owned} {
		if !strings.Contains(resp.Error, fmt.Sprint(incarnation)) {
			t.Errorf("the refusal does not name incarnation %d: %s", incarnation, resp.Error)
		}
	}
	if got := lineageOf(t, replica, testDatabase, s.bucket).Incarnation; got != held {
		t.Errorf("the replica's copy is incarnation %d after the refused migrate, want %d", got, held)
	}
}

// TestMigrateReSeedsAHolderOfAnotherIncarnation is owner ruling 53's manual
// exit (ii): a holder that refused a stray's create of another incarnation
// stalls the stray owner's stream (10a.6). `migrate` of the bucket on that
// holder replaces its copy with the owner's — the re-seed of 10a.8 — and the
// record settles: the holder serves the owner's incarnation with its document,
// and a later write of the owner's reaches it. The refusal's text names
// exactly the two ways out: a drop through the owner, and this migrate (iii).
// Guard: refuse across incarnations as for a diverged replica — the holder's
// record stands.
func TestMigrateReSeedsAHolderOfAnotherIncarnation(t *testing.T) {
	s, bucket := makeStray(t, BucketTypeHash, nil)
	refusedStray(t, s, 0, bucket, "live")
	joinerID := s.joiner.self.ID
	record, _ := s.tc.nodes[0].server.core.cluster.divergenceFrom(joinerID)
	text := (&replicaDivergence{source: joinerID, record: record}).Error()
	for _, want := range []string{"'drop bucket " + bucket + "'", fmt.Sprintf("'migrate %s %s'", testDatabase, bucket)} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal's text does not name the remedy %s: %s", want, text)
		}
	}
	for _, unworkable := range []string{"restore", "dump"} {
		if strings.Contains(text, unworkable) {
			t.Errorf("the refusal's text names %q, which does not end the stall: %s", unworkable, text)
		}
	}
	stray := lineageOf(t, s.joiner.s, testDatabase, bucket).Incarnation

	resp := decodedMigrate(t, s.tc.frame(0, transport.OpMigrate, MigrateRequest{Database: testDatabase, Bucket: bucket}))
	if !resp.Success {
		t.Fatalf("migrate on a holder of another incarnation answered %+v; want a re-seed from the owner (ruling 53)", resp)
	}
	holder := s.tc.nodes[0].server.core
	if !eventually(func() bool { _, ok := holder.cluster.divergenceFrom(joinerID); return !ok }) {
		t.Error("the holder's another_incarnation record never settled after the re-seed")
	}
	if got := lineageOf(t, holder, testDatabase, bucket).Incarnation; got != stray {
		t.Errorf("the re-seeded copy is incarnation %d, want the owner's %d", got, stray)
	}
	if !holdsKeyLocally(s.tc.nodes[0], bucket, "stray") {
		t.Error("the re-seeded copy lacks the owner's document")
	}
	s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after",
		Data: map[string]interface{}{"n": 2}, WriteConcern: "one"})
	if !eventually(func() bool { return holdsKeyLocally(s.tc.nodes[0], bucket, "after") }) {
		t.Error("a write of the owner's after the re-seed never reached the holder: its stream is still stalled")
	}
}

// TestRefusalsArePrunedBelowTheLowestMemberCursor (stop-and-ask S-13, taken as
// recommended): the owner keeps a refusal only while a member may still be
// re-sent its entry — above the lowest member cursor, not merely above the
// log's first sequence. 10,000 refusals made a 219,074-byte state document
// that every refusal rewrote (the build report's measurement). Three members,
// all up; 200 deletes of missing keys at `all`, each confirmed by every member
// before the next: the owner's state document holds at most the last two.
// Guard: prune at the log's first sequence only — all 200 stay.
func TestRefusalsArePrunedBelowTheLowestMemberCursor(t *testing.T) {
	const refusals = 200
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "pruned", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	for i := range refusals {
		tc.frame(0, transport.OpDelete, bson.M{"database": testDatabase, "bucket": bucket, "key": fmt.Sprintf("missing%03d", i), "writeconcern": "all"})
	}
	st, err := tc.nodes[0].server.core.bucketStateNamed(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading the owner's state document: %v", err)
	}
	if len(st.Refusals) > 2 {
		t.Errorf("the owner keeps %d of %d refusals although every member confirmed all but the last: %v ... (S-13)",
			len(st.Refusals), refusals, st.Refusals[:3])
	}
}
