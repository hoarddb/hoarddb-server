package server

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Copies, replay and repair of a declared bucket (typed-fields spec §5.1,
// §5.6, §6.1, §9.5; build doc TF-6 step 6.3).

// typedTestCluster starts three nodes at factor 3 with testBucket declaring
// Age int in strict, served by every node.
func typedTestCluster(t *testing.T) *testCluster {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createTypedOn(tc, 0, testBucket, ageFields(), "")
	waitForServing(t, tc, testDatabase, testBucket, 3)
	return tc
}

// TestReplicasGiveTheOwnersAnswer (spec §5.1): the check runs in every
// copy's apply against the declaration at the entry's position, so a write
// the owner refuses is stored on no copy, whichever node the client reached.
func TestReplicasGiveTheOwnersAnswer(t *testing.T) {
	tc := typedTestCluster(t)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	for _, via := range []int{owner, (owner + 1) % 3} {
		refusal := refusalOf(t, tc.frame(via, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped",
			Data: map[string]interface{}{"Age": "thirty"}, WriteConcern: "all"}))
		if transport.ErrorCode(refusal.Code) != transport.ErrFieldType {
			t.Fatalf("a mistyped put through node %d was answered code %#x %q; want FIELD_TYPE", via+1, refusal.Code, refusal.Message)
		}
	}
	requireEveryCopy(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "fits",
		Data: map[string]interface{}{"Age": int32(36)}, WriteConcern: "all"}), "the conforming put")
	for index := range tc.nodes {
		if holdsKeyLocallyIn(tc.nodes[index], testBucket, "mistyped") {
			t.Errorf("node %d stores the write its owner refused", index+1)
		}
		if !holdsKeyLocallyIn(tc.nodes[index], testBucket, "fits") {
			t.Errorf("node %d does not store the conforming write", index+1)
		}
	}
}

// TestReplayRefusesAgainWhatItRefusedLive (spec §6.1, T-2): a type refusal
// is logged and shipped like any refused write; a restarted owner's replay
// meets it again and counts it rejected_by_constraint, not skipped, which
// pages an operator.
func TestReplayRefusesAgainWhatItRefusedLive(t *testing.T) {
	recorder := recordClusterLogs(t)
	tc := typedTestCluster(t)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	refusal := refusalOf(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped",
		Data: map[string]interface{}{"Age": "thirty"}}))
	if transport.ErrorCode(refusal.Code) != transport.ErrFieldType {
		t.Fatalf("the mistyped put was answered code %#x %q; want FIELD_TYPE", refusal.Code, refusal.Message)
	}
	tc.restartNode(owner)
	replayed := recorder.matching(slog.LevelInfo, "replayed write-ahead log")
	if len(replayed) == 0 {
		t.Fatal("the restarted owner logged no replay line")
	}
	if last := replayed[len(replayed)-1]; last.attrs["rejected_by_constraint"] != "1" || last.attrs["skipped"] != "0" {
		t.Errorf("replay reported %v; want rejected_by_constraint=1 and skipped=0", last.attrs)
	}
	if holdsKeyLocallyIn(tc.nodes[owner], testBucket, "mistyped") {
		t.Error("the replay stored the write it refused live")
	}
}

// stripDeclaration rewrites one node's catalog entry for testBucket without
// its typed fields and restarts the node: a copy seeded before the
// declaration existed, the case spec §9.5's disagreement report is for.
func stripDeclaration(t *testing.T, tc *testCluster, index int) {
	t.Helper()
	core := tc.nodes[index].server.core
	var entry bson.D
	if err := bson.Unmarshal(rawCatalogEntry(t, core, testDatabase, testBucket), &entry); err != nil {
		t.Fatalf("decoding node %d's catalog entry: %v", index+1, err)
	}
	stripped := make(bson.D, 0, len(entry))
	for _, element := range entry {
		if element.Key != "fields" && element.Key != "field_types" {
			stripped = append(stripped, element)
		}
	}
	encoded, err := bson.Marshal(stripped)
	if err != nil {
		t.Fatalf("encoding the stripped entry: %v", err)
	}
	writeRawCatalogEntry(t, core, testDatabase, testBucket, encoded)
	restarted := tc.restartNode(index)
	waitForServing(t, tc, testDatabase, testBucket, 3)
	if declared := servedBucket(t, restarted.server.core, testDatabase, testBucket).declaredFieldTypes(); declared != nil {
		t.Fatalf("test setup: node %d still declares typed fields after its entry was stripped", index+1)
	}
	// Confirmed by every copy, so the streams are in step and the next entry
	// ships live, carrying its owner's outcome. A restarted node's stream is
	// connecting for a while, and all is met by the copies in sync (D-2), so
	// the same put is repeated until every copy confirms it.
	owner := ownerIndexOf(t, tc, testBucket)
	deadline := time.Now().Add(30 * time.Second)
	for {
		frame := tc.frame(owner, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Key: "in-step", Data: map[string]interface{}{"Age": int32(1)}, WriteConcern: "all"})
		if frame.Opcode == transport.OpError {
			t.Fatalf("the write that brings the streams in step was refused: %s", frame.Payload)
		}
		if confirmed, factor, _, _ := copiesConfirmedOf(frame.Payload); confirmed == factor {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("test setup: the restarted node's stream never came back in step")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// disagreementsAfter waits for the disagreement counter to move past before
// by want, and returns how far it moved.
func disagreementsAfter(t *testing.T, before, want float64) float64 {
	t.Helper()
	const counter = "hoarddb_replica_constraint_disagreements_total"
	deadline := time.Now().Add(5 * time.Second)
	for gatheredCounter(t, counter)-before < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return gatheredCounter(t, counter) - before
}

// TestReplicaTypeDisagreementIsLoggedAndCounted (spec §9.5, review A N-5): a
// replica whose copy answers a shipped entry otherwise than its owner did
// under the declared types counts it in the existing disagreement counter
// and logs one ERROR, which names the constraint: from the replica's own
// refusal when it refused, and as one of the constraints an owner refuses
// under when only the owner did. The entry is shipped live, since only a live
// entry carries its owner's outcome.
func TestReplicaTypeDisagreementIsLoggedAndCounted(t *testing.T) {
	t.Run("a replica without the declaration applies what its owner refused", func(t *testing.T) {
		recorder := recordClusterLogs(t)
		tc := typedTestCluster(t)
		defer tc.stop()
		owner := ownerIndexOf(t, tc, testBucket)
		stripDeclaration(t, tc, (owner+1)%3)
		before := gatheredCounter(t, "hoarddb_replica_constraint_disagreements_total")
		refusal := refusalOf(t, tc.frame(owner, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped",
			Data: map[string]interface{}{"Age": "thirty"}}))
		if transport.ErrorCode(refusal.Code) != transport.ErrFieldType {
			t.Fatalf("the owner answered code %#x %q; want FIELD_TYPE", refusal.Code, refusal.Message)
		}
		if moved := disagreementsAfter(t, before, 1); moved != 1 {
			t.Errorf("the disagreement counter moved by %v; want 1", moved)
		}
		loud := recorder.matching(slog.LevelError, testBucket)
		if len(loud) != 1 {
			t.Fatalf("%d ERROR lines name the bucket; want 1: %+v", len(loud), loud)
		}
		for _, want := range []string{"the owner refused the write", "under a unique index, the bucket's declared field types or a path rule",
			"and this replica applied it"} {
			if !strings.Contains(loud[0].msg, want) {
				t.Errorf("the ERROR line %q does not say %q", loud[0].msg, want)
			}
		}
	})
	// The insert twin of the subtest above (build doc step 5, T-05): an
	// insert is decided at its owner, so a replica without the declaration
	// skips the insert its owner refused instead of applying it. The copies
	// agree, so nothing is counted or logged.
	t.Run("a replica without the declaration skips an insert its owner refused", func(t *testing.T) {
		recorder := recordClusterLogs(t)
		tc := typedTestCluster(t)
		defer tc.stop()
		owner := ownerIndexOf(t, tc, testBucket)
		stripped := (owner + 1) % 3
		stripDeclaration(t, tc, stripped)
		before := gatheredCounter(t, "hoarddb_replica_constraint_disagreements_total")
		divergences := divergencesCounted(t)
		base := walTails(tc)
		refusal := refusalOf(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped",
			Data: map[string]interface{}{"Age": "thirty"}}))
		if transport.ErrorCode(refusal.Code) != transport.ErrFieldType {
			t.Fatalf("the owner answered code %#x %q; want FIELD_TYPE", refusal.Code, refusal.Message)
		}
		waitForAllWALs(t, tc, base, 1)
		strippedCore := tc.nodes[stripped].server.core
		deadline := time.Now().Add(5 * time.Second)
		for bucketStateOf(t, strippedCore, testDatabase, testBucket).AppliedSeq < strippedCore.wal.LastSequence() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if found, err := strippedCore.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped"}); err != nil || found.Found {
			t.Errorf("the replica without the declaration holds the insert its owner refused (found=%v, %v)", found != nil && found.Found, err)
		}
		if moved := gatheredCounter(t, "hoarddb_replica_constraint_disagreements_total") - before; moved != 0 {
			t.Errorf("the disagreement counter moved by %v; want 0: the replica followed its owner", moved)
		}
		if moved := divergencesCounted(t) - divergences; moved != 0 {
			t.Errorf("the divergence counter moved by %v; want 0", moved)
		}
		if loud := recorder.matching(slog.LevelError, testBucket); len(loud) != 0 {
			t.Errorf("%d ERROR lines name the bucket; want none: %+v", len(loud), loud)
		}
	})
	t.Run("replicas refuse what an owner without the declaration applied", func(t *testing.T) {
		recorder := recordClusterLogs(t)
		tc := typedTestCluster(t)
		defer tc.stop()
		owner := ownerIndexOf(t, tc, testBucket)
		stripDeclaration(t, tc, owner)
		before := gatheredCounter(t, "hoarddb_replica_constraint_disagreements_total")
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "mistyped",
			Data: map[string]interface{}{"Age": "thirty"}})
		if moved := disagreementsAfter(t, before, 2); moved != 2 {
			t.Errorf("the disagreement counter moved by %v; want 2, one per replica", moved)
		}
		loud := recorder.matching(slog.LevelError, testBucket)
		if len(loud) != 2 {
			t.Fatalf("%d ERROR lines name the bucket; want 2: %+v", len(loud), loud)
		}
		for _, line := range loud {
			if !strings.Contains(line.msg, "the owner applied the write") ||
				!strings.Contains(line.msg, "this replica refused it under the bucket's declared field types") {
				t.Errorf("the ERROR line %q does not name the declared field types as the constraint that refused", line.msg)
			}
		}
	})
}

// TestReadRepairIsNotTypeChecked (spec §5.6): read repair re-puts a value an
// owner already stores, so a document the declaration does not accept,
// stored before it existed, still converges. The repair's put carries
// Admitted, and the type check skips it on every applier.
func TestReadRepairIsNotTypeChecked(t *testing.T) {
	tc := typedTestCluster(t)
	defer tc.stop()
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1",
		Data: map[string]interface{}{"Age": int32(1)}, WriteConcern: "all"}), "the write of k1")
	owner := ownerIndexOf(t, tc, testBucket)
	stale := (owner + 1) % len(tc.nodes)
	predating := map[string]interface{}{"_id_": "k1", "Age": "stored before the declaration"}
	for index := range tc.nodes {
		if index != stale {
			seedLegacyDocument(t, tc.nodes[index].server.core, "k1", predating)
		}
	}
	forceStale(t, tc.nodes[stale].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"Age": int32(0)})

	resp := consistentGet(t, tc.nodes[owner], testDatabase, testBucket, "k1")
	if !resp.Found || !resp.Repaired {
		t.Fatalf("the quorum read did not repair the stale copy (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		found, data, _, err := tc.nodes[stale].server.core.versionedFind(testDatabase, testBucket, "k1")
		if err == nil && found && docField(t, data, "Age") == "stored before the declaration" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stale copy did not converge to the stored document (found=%v err=%v data=%s)", found, err, bson.Raw(data))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
