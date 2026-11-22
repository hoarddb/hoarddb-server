package server

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The generation fields are int64 in every struct that carries one; a uint64
// would fail to encode above MaxInt64 (review F-3). These fail to compile if
// a field's type changes.
var (
	_ int64   = CreateDatabaseRequest{}.Generation
	_ []int64 = CreateDatabaseRequest{}.DroppedGenerations
	_ []int64 = DeleteDatabaseRequest{}.Generations
	_ int64   = CreateBucketRequest{}.DatabaseGeneration
	_ []int64 = CreateBucketRequest{}.DroppedGenerations
	_ []int64 = MigrateStartRequest{}.DroppedGenerations
	_ int64   = MigrateStartResponse{}.DatabaseGeneration
	_ []int64 = MigrateStartResponse{}.DroppedGenerations
	_ int64   = catalogEntry{}.DatabaseGeneration
	_ []int64 = databaseMarker{}.Generations
	_ []int64 = databaseDropRecord{}.Generations
	_ []int64 = DatabaseTombstones{}.Generations
)

// TestGenerationsRoundTripAtTheirBounds draws 10,000 generations through the
// minting function. Each is in [1, MaxInt64] and survives a round trip through
// the module's BSON codec inside every real struct that carries one.
//
// Guard: rev 1's uint64 generation minted from 64 bits. Half of all such values
// exceed MaxInt64 and the encoder refuses them ("overflows int64"), so 10,000
// draws fail with probability 1 − 2⁻¹⁰⁰⁰⁰ (rev 3, A-5).
func TestGenerationsRoundTripAtTheirBounds(t *testing.T) {
	for i := 0; i < 10000; i++ {
		generation, err := mintGeneration()
		if err != nil {
			t.Fatalf("mint %d: %v", i, err)
		}
		// An int64 cannot exceed MaxInt64; a negative one is a 64-bit draw
		// that was not masked to 63 bits.
		if generation < 1 {
			t.Fatalf("mint %d: generation %d is outside [1, MaxInt64]", i, generation)
		}
		set := []int64{generation, 1, math.MaxInt64}
		bsonRoundTrip(t, CreateDatabaseRequest{Name: "app", Generation: generation, DroppedGenerations: set})
		bsonRoundTrip(t, DeleteDatabaseRequest{Name: "app", Generations: set})
		bsonRoundTrip(t, CreateBucketRequest{Database: "app", Name: "b", DatabaseGeneration: generation, DroppedGenerations: set})
		bsonRoundTrip(t, MigrateStartRequest{Database: "app", Bucket: "b", DroppedGenerations: set})
		bsonRoundTrip(t, MigrateStartResponse{Ok: true, DatabaseGeneration: generation, DroppedGenerations: set})
		bsonRoundTrip(t, ReplPushResponse{Success: true, DroppedGenerations: []DatabaseTombstones{{Database: "app", Generations: set}}})
		bsonRoundTrip(t, databaseMarker{Generations: set})
		bsonRoundTrip(t, databaseDropRecord{Database: "app", Generations: set})
		bsonRoundTrip(t, tombstoneRecord{DroppedSeq: generation})
		entry := catalogEntry{ID: 1, DatabaseGeneration: generation}
		encoded, err := encodeCatalogEntry(entry)
		if err != nil {
			t.Fatalf("encode a catalog entry with generation %d: %v", generation, err)
		}
		var decoded catalogEntry
		if err := bson.Unmarshal(encoded, &decoded); err != nil || decoded.DatabaseGeneration != generation {
			t.Fatalf("a catalog entry with generation %d decodes as %d (%v)", generation, decoded.DatabaseGeneration, err)
		}
	}
}

// bsonRoundTrip marshals value with the module's codec and requires it back equal.
func bsonRoundTrip[T any](t *testing.T, value T) {
	t.Helper()
	encoded, err := bson.Marshal(value)
	if err != nil {
		t.Fatalf("encode %T: %v", value, err)
	}
	var decoded T
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode %T: %v", value, err)
	}
	again, err := bson.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-encode %T: %v", value, err)
	}
	if string(again) != string(encoded) {
		t.Fatalf("%T does not survive a round trip: %x became %x", value, encoded, again)
	}
}

// TestALegacyGenerationIsDerivedFromTheNameAlone: every node derives the same
// non-zero generation for a database that predates generations, and two names
// derive two values.
func TestALegacyGenerationIsDerivedFromTheNameAlone(t *testing.T) {
	first, again, other := legacyGeneration("app"), legacyGeneration("app"), legacyGeneration("tenant")
	if first != again || first < 1 || first == other {
		t.Fatalf("legacy generations: app %d and %d, tenant %d; want one non-zero value per name", first, again, other)
	}
}

// loggedPayload returns the payload of the last entry with op naming database
// (a bucket create's database) in node index's own log.
func loggedPayload(t *testing.T, tc *testCluster, index int, op transport.Opcode, database string) []byte {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	var payload []byte
	for _, entry := range entries {
		if _, named := decodeFrameMeta(entry.Data).scope(op); transport.Opcode(entry.Op) == op && named == database {
			payload = entry.Data
		}
	}
	if payload == nil {
		t.Fatalf("node %d's log holds no %v of %q", index+1, op, database)
	}
	return payload
}

// TestAClientCannotChooseADatabaseGeneration: a client's create and drop of a
// database name generations of their own choosing. What is logged carries the
// generations of the node the client reached.
//
// Guard: G-9. Trusting the client's field lets it name a generation another
// node tombstoned, or a drop name generations it never saw.
func TestAClientCannotChooseADatabaseGeneration(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	const chosen = int64(42)

	// Exact names, and the case-variants the struct decoder also matches.
	tc.ok(0, transport.OpCreateDB, bson.D{{Key: "name", Value: testDatabase}, {Key: "generation", Value: chosen},
		{Key: "dropped_generations", Value: []int64{chosen + 1}}, {Key: "Dropped_Generations", Value: []int64{chosen + 2}}, {Key: "GENERATION", Value: chosen}})
	var created CreateDatabaseRequest
	if err := bson.Unmarshal(loggedPayload(t, tc, 0, transport.OpCreateDB, testDatabase), &created); err != nil {
		t.Fatalf("decode the logged create: %v", err)
	}
	if created.Generation == chosen || created.Generation == 0 || len(created.DroppedGenerations) != 0 {
		t.Fatalf("the logged create carries generation %d and dropped %v; the client chose %d and %d",
			created.Generation, created.DroppedGenerations, chosen, chosen+1)
	}
	live, err := tc.nodes[0].server.core.liveGenerations(testDatabase)
	if err != nil || len(live) != 1 || live[0] != created.Generation {
		t.Fatalf("the coordinator's live generations are %v (%v); want the logged %d", live, err, created.Generation)
	}

	tc.ok(0, transport.OpDeleteDB, bson.D{{Key: "name", Value: testDatabase}, {Key: "generations", Value: []int64{chosen}}, {Key: "GENERATIONS", Value: []int64{chosen}}})
	var dropped DeleteDatabaseRequest
	if err := bson.Unmarshal(loggedPayload(t, tc, 0, transport.OpDeleteDB, testDatabase), &dropped); err != nil {
		t.Fatalf("decode the logged drop: %v", err)
	}
	if len(dropped.Generations) != 1 || dropped.Generations[0] != created.Generation {
		t.Fatalf("the logged drop names %v; the client named %d and the database's generation is %d",
			dropped.Generations, chosen, created.Generation)
	}
}

// TestAForwardedCreateBucketCarriesTheCoordinatorsGeneration (SA-DG-14, which
// replaces rev 3's owner stamp): a bucket create sent to a node that is not its
// owner, with a db_gen of the client's choosing, is logged by the owner with
// the generation the node the client reached stamped.
//
// Guard: G-9 on the forwarded path. Keeping the client's value logs a create
// in an incarnation nobody created.
func TestAForwardedCreateBucketCarriesTheCoordinatorsGeneration(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	coordinator := (owner + 1) % 3
	const chosen = int64(42)

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	live, err := tc.nodes[coordinator].server.core.liveGenerations(testDatabase)
	if err != nil || len(live) != 1 {
		t.Fatalf("setup: the coordinator's live generations are %v (%v)", live, err)
	}
	tc.ok(coordinator, transport.OpCreateBucket, bson.M{
		"database": testDatabase, "name": testBucket, "type": string(BucketTypeHash), "db_gen": chosen,
	})
	var logged CreateBucketRequest
	if err := bson.Unmarshal(loggedPayload(t, tc, owner, transport.OpCreateBucket, testDatabase), &logged); err != nil {
		t.Fatalf("decode the owner's logged create: %v", err)
	}
	if logged.DatabaseGeneration != live[0] {
		t.Fatalf("the owner logged the create with db_gen %d; the client sent %d and the coordinator's generation is %d",
			logged.DatabaseGeneration, chosen, live[0])
	}
	waitForServing(t, tc, testDatabase, testBucket, 3)
	for index, node := range tc.nodes {
		if generation := heldBucketGeneration(node, testDatabase, testBucket); generation != live[0] {
			t.Errorf("node %d holds %s/%s in generation %d; want %d", index+1, testDatabase, testBucket, generation, live[0])
		}
	}
}

// heldBucketGeneration is the generation of the copy of database/bucket a
// node serves, or 0.
func heldBucketGeneration(node *testNode, database, bucket string) int64 {
	db := node.server.core.serverDataAccessor().getDB(database)
	if db == nil {
		return 0
	}
	held := db.getBucket(bucket)
	if held == nil || held.id.Load() == 0 {
		return 0
	}
	return held.generation.Load()
}

// TestADroppedGenerationRecordIsNeverReadAsAPosition: the replication
// namespace holds dropped generations beside the stream's positions. A start
// over a store that holds one loads its positions and is not refused, and the
// key decoder tells a dropped generation apart before it reads a source.
//
// Guard: the narrowed start scan. Scanning the whole namespace hands a 'g' key
// to the position decoder, which refuses an unknown kind and stops the start.
func TestADroppedGenerationRecordIsNeverReadAsAPosition(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	core := tc.nodes[1].server.core
	encoded, err := bson.Marshal(tombstoneRecord{DroppedSeq: 7})
	if err != nil {
		t.Fatalf("encode a dropped generation: %v", err)
	}
	batch := core.kv.NewBatch()
	if err := batch.Set(tombstoneKey("app", 99), encoded); err != nil {
		t.Fatalf("write a dropped generation: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit a dropped generation: %v", err)
	}
	batch.Discard()
	if _, err := decodeReplicationKey(tombstoneKey("app", 99)); err == nil {
		t.Fatal("the position decoder read a dropped generation's key as a position")
	}

	restarted := tc.restartNode(1)
	if err := restarted.server.core.cluster.loadStoredPositions(); err != nil {
		t.Fatalf("loading positions beside a dropped generation: %v", err)
	}
	dropped, err := restarted.server.core.droppedGenerations("app")
	if err != nil || len(dropped) != 1 || dropped[0] != 99 {
		t.Fatalf("after a restart the dropped generations of app are %v (%v); want [99]", dropped, err)
	}
	if prefix := tombstonePrefix("app"); len(prefix) != len(storage.NodeKey(storage.NSReplication))+1+1+len("app") {
		t.Fatalf("a dropped generation's prefix is %d bytes; want namespace, kind, length and name", len(prefix))
	}
}

// TestAClientCannotChooseAGenerationUnderAnyCaseOfItsName is the verifier's
// G-9 attack: the module's BSON struct decoder matches a key it does not know
// to a field by its lower-cased name, so a client that writes
// Dropped_Generations, DB_GEN or GENERATION reaches the same fields as the
// exact names. Through the bucket's owner and through another node, on a
// bucket create, a database create and a database drop, what is logged
// carries only the generations of the node the client reached, no node
// records the live generation dropped, and an acknowledged write survives.
//
// Guard: G-9's strip by exact key name only, with the dropped generations
// written only when the node reached holds some.
func TestAClientCannotChooseAGenerationUnderAnyCaseOfItsName(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	live, err := tc.nodes[0].server.core.liveGenerations(testDatabase)
	if err != nil || len(live) != 1 {
		t.Fatalf("setup: live generations %v (%v)", live, err)
	}
	generation := live[0]
	victim := bucketOwnedIn(t, tc, 1, testDatabase, "victim")
	createBucketAt(tc, 1, testDatabase, victim, 0)
	putIn(tc, 1, testDatabase, victim, "precious", "all")

	forgeries := []bson.D{
		{{Key: "Dropped_Generations", Value: []int64{generation}}},
		{{Key: "DROPPED_GENERATIONS", Value: []int64{generation}}},
		{{Key: "DB_GEN", Value: int64(42)}, {Key: "Dropped_Generations", Value: []int64{generation}}},
		{{Key: "Db_Gen", Value: int64(42)}},
		{{Key: "Generation", Value: int64(42)}, {Key: "Generations", Value: []int64{generation}}},
	}
	for round, forged := range forgeries {
		for _, through := range []string{"owner", "another node"} {
			bucket := bucketOwnedIn(t, tc, 1, testDatabase, fmt.Sprintf("forged%d", round*2+len(through)%2))
			index := 1
			if through == "another node" {
				index = 2
			}
			doc := append(bson.D{{Key: "database", Value: testDatabase}, {Key: "name", Value: bucket}, {Key: "type", Value: string(BucketTypeHash)}}, forged...)
			tc.frame(index, transport.OpCreateBucket, doc)
			var logged CreateBucketRequest
			if err := bson.Unmarshal(loggedPayload(t, tc, 1, transport.OpCreateBucket, testDatabase), &logged); err != nil {
				t.Fatalf("decode the owner's logged create: %v", err)
			}
			if logged.DatabaseGeneration != generation || len(logged.DroppedGenerations) != 0 {
				t.Errorf("a create through the %s forging %v was logged with db_gen %d and dropped %v; want %d and none",
					through, forged, logged.DatabaseGeneration, logged.DroppedGenerations, generation)
			}
		}
	}

	// A database create and drop through two nodes, forging every key.
	for _, index := range []int{0, 2} {
		database := fmt.Sprintf("forged-db-%d", index)
		tc.frame(index, transport.OpCreateDB, bson.D{{Key: "name", Value: database},
			{Key: "GENERATION", Value: int64(42)}, {Key: "Dropped_Generations", Value: []int64{99}}, {Key: "DROPPED_GENERATIONS", Value: []int64{98}}})
		var created CreateDatabaseRequest
		if err := bson.Unmarshal(loggedPayload(t, tc, index, transport.OpCreateDB, database), &created); err != nil {
			t.Fatalf("decode the logged create: %v", err)
		}
		if created.Generation == 42 || len(created.DroppedGenerations) != 0 {
			t.Errorf("a database create through node %d was logged with generation %d and dropped %v", index+1, created.Generation, created.DroppedGenerations)
		}
		tc.frame(index, transport.OpDeleteDB, bson.D{{Key: "name", Value: database}, {Key: "Generations", Value: []int64{99, generation}}})
		var dropped DeleteDatabaseRequest
		if err := bson.Unmarshal(loggedPayload(t, tc, index, transport.OpDeleteDB, database), &dropped); err != nil {
			t.Fatalf("decode the logged drop: %v", err)
		}
		if !slices.Equal(dropped.Generations, []int64{created.Generation}) {
			t.Errorf("a database drop through node %d was logged naming %v; want [%d]", index+1, dropped.Generations, created.Generation)
		}
	}

	for _, index := range []int{0, 1, 2} {
		waitAckedEverywhere(t, tc, index)
	}
	for index, node := range tc.nodes {
		dropped, err := node.server.core.droppedGenerations(testDatabase)
		if err != nil || slices.Contains(dropped, generation) {
			t.Errorf("node %d records the live generation %d of %s dropped (%v %v)", index+1, generation, testDatabase, dropped, err)
		}
		if !holdsKeyIn(node, testDatabase, victim, "precious") {
			t.Errorf("node %d lost the write acknowledged at all to %s/%s", index+1, testDatabase, victim)
		}
	}
}
