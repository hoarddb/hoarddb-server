package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// awaitDropExpanded waits until node has applied the drop of database and
// its pending drop record is gone — the reclaimer's expander has converted
// the database's buckets and deleted the record, the normal state a few
// moments after any drop and the precondition the restart fix needs. The database
// being gone from memory and its marker gone are checked too: with no drop
// applied yet there is no record either, and that would pass vacuously.
func awaitDropExpanded(t *testing.T, node *Server, database string) {
	t.Helper()
	pollUntil(t, "the drop of database "+database+" to be applied and its record expanded away", func() bool {
		node.wakeReclaimer()
		if node.serverDataAccessor().getDB(database) != nil {
			return false
		}
		markerMissing, err := node.databaseMarkerMissing(database)
		if err != nil || !markerMissing {
			return false
		}
		_, pending, err := node.pendingDatabaseDrop(database)
		return err == nil && !pending
	})
}

// seedDroppedDatabase creates database with one bucket and one document
// through node 0, drops it through node 1 (so node 0's log holds the drop as
// a replicated entry, an operator's drop rather than a local one) and waits
// until node 0 has expanded the drop's record away.
func seedDroppedDatabase(t *testing.T, tc *testCluster, database, bucket string) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: database, Bucket: bucket, Key: "doomed", Data: map[string]interface{}{"v": "doomed"}})
	tc.ok(1, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})
	awaitDropExpanded(t, tc.nodes[0].server.core, database)
}

// TestCommittedDropThenRestartSucceeds is the acceptance test, its
// reproduction verbatim: an ordinary drop database commits, the expander
// deletes its record, and the node restarts. Before the fix, replay reached
// the logged OpDeleteDB with the database live again (its own replayed
// CreateDB had just re-created it) and no pending record to consult, and
// refused to start. The node must start, keep the database dropped, and do
// so again on a second restart.
func TestCommittedDropThenRestartSucceeds(t *testing.T) {
	const database, bucket = "droppedthenrestarted", "bucket1"
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	seedDroppedDatabase(t, tc, database, bucket)

	for restart := 1; restart <= 2; restart++ {
		restarted := tc.restartNode(0).server.core
		if restarted.serverDataAccessor().getDB(database) != nil {
			t.Fatalf("after restart %d, database %q, dropped with nothing re-created after it, is served again", restart, database)
		}
		if lookupBucket(restarted, database, bucket) != nil {
			t.Fatalf("after restart %d, bucket %q of dropped database %q is served again", restart, bucket, database)
		}
	}
}

// loggedDropSeqs is every sequence at which node's log holds an OpDeleteDB of
// database, ascending.
func loggedDropSeqs(t *testing.T, node *Server, database string) []uint64 {
	t.Helper()
	entries, err := node.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node's WAL: %v", err)
	}
	var seqs []uint64
	for _, e := range entries {
		if transport.Opcode(e.Op) != transport.OpDeleteDB {
			continue
		}
		if _, named := decodeFrameMeta(e.Data).scope(transport.OpDeleteDB); named == database {
			seqs = append(seqs, e.Seq)
		}
	}
	return seqs
}

// liveMarkerCreatedSeq reads the created_seq of database's marker on node.
func liveMarkerCreatedSeq(t *testing.T, node *Server, database string) uint64 {
	t.Helper()
	snap := node.kv.NewSnapshot()
	defer snap.Discard()
	createdSeq, err := markerCreatedSeq(snap, database)
	if err != nil {
		t.Fatalf("reading the marker of database %q: %v", database, err)
	}
	return createdSeq
}

// TestRecreateAfterExpandedDropSurvivesRestart pins the other half of
// acceptance criteria: a name dropped (its record expanded away),
// the node restarted, the name re-created through the cluster and written,
// and the node restarted again, twice. The old logged drop sits below the
// re-created generation's created_seq, so the gate skips it, and the node
// must start. (The generation here is wholly in the log, so a replay that
// wrongly took it would also rebuild it: the marker assertion below is what
// shows the drop is below the new generation, and
// TestDirectRecreateAfterExpandedDropSurvivesRestart is the one that loses
// data if the gate is wrong.)
func TestRecreateAfterExpandedDropSurvivesRestart(t *testing.T) {
	const database, oldBucket, newBucket = "recreatedafterexpand", "original", "survivor"
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	seedDroppedDatabase(t, tc, database, oldBucket)
	tc.restartNode(0)

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: newBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: database, Bucket: newBucket, Key: "kept", Data: map[string]interface{}{"v": "kept"}})

	node := tc.nodes[0].server.core
	createdSeq := liveMarkerCreatedSeq(t, node, database)
	for _, dropSeq := range loggedDropSeqs(t, node, database) {
		if dropSeq >= createdSeq {
			t.Fatalf("the re-created database's marker records created_seq %d, not above the logged drop at %d: the gate cannot tell the generations apart", createdSeq, dropSeq)
		}
	}

	for restart := 1; restart <= 2; restart++ {
		restarted := tc.restartNode(0).server.core
		found, err := restarted.Find(FindRequest{Database: database, Bucket: newBucket, Key: "kept"})
		if err != nil {
			t.Fatalf("after restart %d, reading the re-created generation's document failed: %v", restart, err)
		}
		if !found.Found {
			t.Fatalf("after restart %d, the re-created database %q lost its document: the old logged drop took the new generation (%s)", restart, database, found.Message)
		}
	}
}

// TestDirectRecreateAfterExpandedDropSurvivesRestart is
// TestReplayedDropDatabaseTakesTheNewGeneration with the drop
// record already expanded: the name is re-created and written directly on
// node 0's core, outside the log, after the drop's record is gone. The logged
// drop must not take that generation on restart, and the node must start.
func TestDirectRecreateAfterExpandedDropSurvivesRestart(t *testing.T) {
	const database, oldBucket, newBucket = "directrecreate", "original", "survivor"
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()

	seedDroppedDatabase(t, tc, database, oldBucket)

	target := tc.nodes[0].server.core
	if resp, err := target.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("re-creating database %q directly: %+v, %v", database, resp, err)
	}
	if resp, err := target.CreateBucket(CreateBucketRequest{Database: database, Name: newBucket, Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("re-creating bucket %q directly: %+v, %v", newBucket, resp, err)
	}
	if resp, err := target.Insert(InsertRequest{Database: database, Bucket: newBucket, Key: "seeded", Data: map[string]interface{}{"v": "seeded"}}); err != nil || !resp.Success {
		t.Fatalf("writing the seeded document directly: %+v, %v", resp, err)
	}

	restarted := tc.restartNode(0).server.core
	found, err := restarted.Find(FindRequest{Database: database, Bucket: newBucket, Key: "seeded"})
	if err != nil {
		t.Fatalf("after the restart, reading the seeded document failed: %v", err)
	}
	if !found.Found {
		t.Fatalf("after the restart the directly re-created database %q lost its document: the old logged drop took it (%s)", database, found.Message)
	}
}

// TestDatabaseDropGateArms pins every arm of databaseDropAtOrBelowMark,
// including the fallback for a marker written before
// created_seq existed, which the cluster tests above no longer reach.
func TestDatabaseDropGateArms(t *testing.T) {
	const database = "gated"
	s := newTestServer(t)
	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("creating database %q: %+v, %v", database, resp, err)
	}
	dropPayload, err := bson.Marshal(DeleteDatabaseRequest{Name: database})
	if err != nil {
		t.Fatalf("encoding the drop request: %v", err)
	}
	dropAt := func(seq uint64) storage.WALEntry {
		return storage.WALEntry{Seq: seq, Op: byte(transport.OpDeleteDB), Data: dropPayload}
	}

	cases := []struct {
		name       string
		seq        uint64
		generation databaseGeneration
		wantSkip   bool
		wantErr    bool
	}{
		{name: "not live at replay start: replay rebuilt it, the drop takes it", seq: 5, generation: databaseGeneration{}, wantSkip: false},
		{name: "drop below the live generation's created_seq: stale, skipped", seq: 5, generation: databaseGeneration{live: true, createdSeq: 9}, wantSkip: true},
		{name: "drop at the live generation's created_seq: stale, skipped", seq: 9, generation: databaseGeneration{live: true, createdSeq: 9}, wantSkip: true},
		{name: "drop above the live generation's created_seq: its own uncommitted drop, replayed", seq: 10, generation: databaseGeneration{live: true, createdSeq: 9}, wantSkip: false},
		{name: "legacy marker, pending record, re-created above max_id: skipped", seq: 5, generation: databaseGeneration{live: true, pendingDrop: true, recreated: true}, wantSkip: true},
		{name: "legacy marker, pending record, nothing above max_id: replayed", seq: 5, generation: databaseGeneration{live: true, pendingDrop: true}, wantSkip: false},
		{name: "legacy marker, no record: refused", seq: 5, generation: databaseGeneration{live: true}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skip, err := s.databaseDropAtOrBelowMark(dropAt(tc.seq), map[string]databaseGeneration{database: tc.generation})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("gate = skip %v, nil error; want a refusal", skip)
				}
				return
			}
			if err != nil {
				t.Fatalf("gate refused: %v", err)
			}
			if skip != tc.wantSkip {
				t.Fatalf("gate skip = %v, want %v", skip, tc.wantSkip)
			}
		})
	}

	t.Run("database not live now: dispatched, which answers not found", func(t *testing.T) {
		missing, err := bson.Marshal(DeleteDatabaseRequest{Name: "never-created"})
		if err != nil {
			t.Fatalf("encoding the drop request: %v", err)
		}
		skip, err := s.databaseDropAtOrBelowMark(storage.WALEntry{Seq: 5, Op: byte(transport.OpDeleteDB), Data: missing}, nil)
		if err != nil || skip {
			t.Fatalf("gate = skip %v, err %v; want neither", skip, err)
		}
	})
}

// assertDroppedBucketNotResurrected fails if bucket — dropped with its
// database's older generation — is served in the re-created database, or
// its document is readable there.
func assertDroppedBucketNotResurrected(t *testing.T, node *Server, database, bucket string) {
	t.Helper()
	if lookupBucket(node, database, bucket) == nil {
		return
	}
	found, err := node.Find(FindRequest{Database: database, Bucket: bucket, Key: "doomed"})
	t.Fatalf("after the restart, bucket %q of the dropped generation of %q is served in the re-created database (its dropped document readable: %v, err %v): "+
		"replay re-applied the old generation's CreateBucket beneath the new generation's created_seq", bucket, database, found != nil && found.Found, err)
}

// TestRecreatedDatabaseDoesNotResurrectDroppedBucket: once a logged drop is
// skipped as below the live generation, the dropped generation's own
// earlier CreateBucket and writes are below it too. A bucket name the new
// generation does not have does not resolve, so the bucket DDL gate did not
// catch it, and replay re-created the dropped bucket — and replayed its
// writes into it — inside the re-created database. Found while fixing
// the restart-after-drop defect; the pending-record arm reproduces on the code
// before that fix (it is the replay gate's own shape), the expanded arm became
// reachable once that fix let that node start.
func TestRecreatedDatabaseDoesNotResurrectDroppedBucket(t *testing.T) {
	const oldBucket, newBucket = "original", "survivor"

	t.Run("drop expanded, re-created through the cluster", func(t *testing.T) {
		const database = "noresurrectexpanded"
		tc := newTestCluster(t, 2, 2)
		defer tc.stop()

		seedDroppedDatabase(t, tc, database, oldBucket)
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: newBucket, Type: BucketTypeHash})

		assertDroppedBucketNotResurrected(t, tc.restartNode(0).server.core, database, oldBucket)
	})

	t.Run("drop record pending, re-created directly", func(t *testing.T) {
		const database = "noresurrectpending"
		tc := newTestCluster(t, 2, 2)
		defer tc.stop()

		blockExpansionOf(t, database)
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: oldBucket, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: database, Bucket: oldBucket, Key: "doomed", Data: map[string]interface{}{"v": "doomed"}})
		tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})

		target := tc.nodes[0].server.core
		if resp, err := target.CreateDatabase(database); err != nil || !resp.Success {
			t.Fatalf("re-creating database %q directly: %+v, %v", database, resp, err)
		}
		if resp, err := target.CreateBucket(CreateBucketRequest{Database: database, Name: newBucket, Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("re-creating bucket %q directly: %+v, %v", newBucket, resp, err)
		}

		restarted := tc.restartNode(0).server.core
		assertDroppedBucketNotResurrected(t, restarted, database, oldBucket)
		if lookupBucket(restarted, database, newBucket) == nil {
			t.Fatalf("after the restart the re-created generation's bucket %q is not served", newBucket)
		}
	})
}
