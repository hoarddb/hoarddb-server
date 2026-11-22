package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// A drop of a database's last live generation on this node, landing
// while a bucket of the database is being created or pulled, must leave that
// bucket either served (its generation is not the dropped one) or refused with
// nothing catalogued (it is) — never catalogued and not served.

// pendingDrop is a drop of generations running beside a create or a pull that
// holds a bucket lock the drop's close of that bucket would wait on.
type pendingDrop struct {
	done chan struct{}
	resp *DeleteDatabaseResponse
	err  error
}

// startDrop applies a peer's drop of generations of database in the
// background.
func startDrop(s *Server, database string, generations []int64) *pendingDrop {
	drop := &pendingDrop{done: make(chan struct{})}
	go func() {
		defer close(drop.done)
		ctx := withPeerEntry(withClusterApply(context.Background()), true)
		drop.resp, drop.err = s.applyGenerationDrop(ctx, database, generations)
	}()
	return drop
}

// awaitDetach waits until the drop has finished, or has committed and taken
// database out of the registry — the furthest it gets while a bucket lock it
// would close is held. Either way its registry change is made.
func (d *pendingDrop) awaitDetach(t *testing.T, s *Server, database string, generation int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-d.done:
			return
		default:
		}
		dropped, err := s.generationDropped(database, generation)
		if err != nil {
			t.Fatalf("reading the tombstone of generation %d: %v", generation, err)
		}
		if dropped && s.serverDataAccessor().getDB(database) == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the drop of generation %d of %q neither finished nor detached the database in 10s", generation, database)
}

// finish waits for the drop and fails the test unless it succeeded.
func (d *pendingDrop) finish(t *testing.T) {
	t.Helper()
	<-d.done
	if d.err != nil || d.resp == nil || !d.resp.Success {
		t.Fatalf("the drop: %+v, %v", d.resp, d.err)
	}
}

// otherGeneration is a valid generation that is not g.
func otherGeneration(g int64) int64 {
	if other := g ^ 0x5a5a; other > 0 {
		return other
	}
	return 777
}

// nodeView is what a node says about one bucket and its database.
type nodeView struct {
	catalogued, open, listed bool
	live                     []int64
}

func viewOf(t *testing.T, s *Server, database, bucket string) nodeView {
	t.Helper()
	entry, catalogued, err := s.readCatalogEntry(database, bucket)
	if err != nil {
		t.Fatalf("reading the catalog entry of %s/%s: %v", database, bucket, err)
	}
	// An entry a drop covers is the expander's to remove, not a bucket.
	drop, pending, err := s.pendingDatabaseDrop(database)
	if err != nil {
		t.Fatalf("reading the pending drop of %q: %v", database, err)
	}
	catalogued = catalogued && (!pending || !drop.covers(entry))
	live, err := s.liveGenerations(database)
	if err != nil {
		t.Fatalf("reading the live generations of %q: %v", database, err)
	}
	listing, err := s.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("listing databases: %v", err)
	}
	listed := slices.ContainsFunc(listing.Databases, func(info DatabaseInfo) bool { return info.Name == database })
	return nodeView{catalogued: catalogued, open: s.bucketStoreOpen(database, bucket), listed: listed, live: live}
}

// assertSameAfterRestart restarts the node and fails unless it says the same
// about the bucket and its database as it did before.
func assertSameAfterRestart(t *testing.T, server *catalogServer, dataDir, database, bucket string) *catalogServer {
	t.Helper()
	before := viewOf(t, server.core, database, bucket)
	restarted := restartCatalog(t, server, dataDir)
	after := viewOf(t, restarted.core, database, bucket)
	if before.catalogued != after.catalogued || before.open != after.open || before.listed != after.listed || !slices.Equal(before.live, after.live) {
		t.Fatalf("the node's view of %s/%s changed across a restart: before %+v, after %+v", database, bucket, before, after)
	}
	return restarted
}

// createDatabaseIn creates database and returns its one live generation.
func createDatabaseIn(t *testing.T, s *Server, database string) int64 {
	t.Helper()
	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("creating database %q: %+v, %v", database, resp, err)
	}
	live, err := s.liveGenerations(database)
	if err != nil || len(live) != 1 {
		t.Fatalf("live generations of %q: %v, %v", database, live, err)
	}
	return live[0]
}

// dropDuringMaterialise materialises database/bucket in generation while a
// drop of dropped lands between the materialise's registry lookup and its
// catalog commit, and returns the materialise's error.
func dropDuringMaterialise(t *testing.T, s *Server, database, bucket string, generation, dropped int64) error {
	t.Helper()
	var drop *pendingDrop
	hook := func(uint64) error {
		drop = startDrop(s, database, []int64{dropped})
		drop.awaitDetach(t, s, database, dropped)
		return nil
	}
	materializeCatalogHook.Store(&hook)
	err := s.materializeRange(migratedBucket{database: database, name: bucket, storeType: BucketTypeHash, placement: placementWhole, generation: generation}, nil, 0)
	materializeCatalogHook.Store(nil)
	if drop == nil {
		t.Fatal("the materialise never reached its catalog commit")
	}
	drop.finish(t)
	return err
}

// dropDuringCreate creates database/bucket as a peer's logged create in
// generation while a drop of dropped lands between the create's registry
// lookup and its catalog commit, and returns the create's answer.
func dropDuringCreate(t *testing.T, s *Server, database, bucket string, generation, dropped int64) *CreateBucketResponse {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	hook := func(db, name string) (uint16, bool) {
		if db == database && name == bucket {
			close(entered)
			<-release
		}
		return 0, false
	}
	slotOverrideHook.Store(&hook)
	defer slotOverrideHook.Store(nil)

	type answer struct {
		resp *CreateBucketResponse
		err  error
	}
	created := make(chan answer, 1)
	go func() {
		ctx := withPeerEntry(withClusterApply(context.Background()), true)
		resp, err := s.createBucketLocked(ctx, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash, DatabaseGeneration: generation})
		created <- answer{resp, err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the create never reached its slot choice")
	}
	drop := startDrop(s, database, []int64{dropped})
	drop.awaitDetach(t, s, database, dropped)
	close(release)
	got := <-created
	drop.finish(t)
	if got.err != nil {
		t.Fatalf("the create: %v", got.err)
	}
	return got.resp
}

func TestADropOfTheLastGenerationMidPullLeavesThePulledCopyServed(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	const database, bucket = "pulldb", "b"
	g1 := createDatabaseIn(t, server.core, database)
	g2 := otherGeneration(g1)

	if err := dropDuringMaterialise(t, server.core, database, bucket, g2, g1); err != nil {
		t.Fatalf("the pull of %s/%s in a live generation failed: %v", database, bucket, err)
	}
	got := viewOf(t, server.core, database, bucket)
	if !got.catalogued || !got.open || !got.listed || !slices.Equal(got.live, []int64{g2}) {
		t.Fatalf("after a drop of g1 during the pull of %s/%s in g2: %+v; want it catalogued, served, listed and live in [%d]", database, bucket, got, g2)
	}
	assertSameAfterRestart(t, server, dataDir, database, bucket)
}

func TestADropOfTheLastGenerationMidCreateLeavesTheNewBucketServed(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	const database, bucket = "racedb", "c"
	g1 := createDatabaseIn(t, server.core, database)
	g2 := otherGeneration(g1)

	resp := dropDuringCreate(t, server.core, database, bucket, g2, g1)
	if resp == nil || !resp.Success {
		t.Fatalf("the create of %s/%s in a live generation was refused: %+v", database, bucket, resp)
	}
	got := viewOf(t, server.core, database, bucket)
	if !got.catalogued || !got.open || !got.listed || !slices.Equal(got.live, []int64{g2}) {
		t.Fatalf("after a drop of g1 during the create of %s/%s in g2: %+v; want it catalogued, served, listed and live in [%d]", database, bucket, got, g2)
	}
	if insert, err := server.core.Insert(InsertRequest{Database: database, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}}); err != nil || !insert.Success {
		t.Fatalf("a write to %s/%s after the race: %+v, %v", database, bucket, insert, err)
	}
	assertSameAfterRestart(t, server, dataDir, database, bucket)
}

// Guard: the fix keeps a database whose buckets are not all of the dropped
// generations; a drop of the only generation, with nothing racing it, must
// still take everything.
func TestADropOfTheOnlyGenerationStillDetachesTheWholeDatabase(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	const database = "solodb"
	g1 := createDatabaseIn(t, server.core, database)
	for _, bucket := range []string{"a", "b"} {
		if resp, err := server.core.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("creating %s/%s: %+v, %v", database, bucket, resp, err)
		}
	}
	drop := startDrop(server.core, database, []int64{g1})
	drop.finish(t)
	if !strings.Contains(drop.resp.Message, "2 bucket(s)") {
		t.Fatalf("the drop removed other than both buckets: %q", drop.resp.Message)
	}
	if server.core.serverDataAccessor().getDB(database) != nil {
		t.Fatalf("a drop of the only generation of %q left it in the registry", database)
	}
	for _, bucket := range []string{"a", "b"} {
		got := viewOf(t, server.core, database, bucket)
		if got.catalogued || got.open || got.listed || len(got.live) != 0 {
			t.Fatalf("after a drop of the only generation, %s/%s: %+v; want nothing left", database, bucket, got)
		}
	}
	assertSameAfterRestart(t, server, dataDir, database, "a")
}

// Guard: a create in the generation being dropped must still be dropped with
// it — refused, nothing catalogued, and the database gone from the registry.
func TestADropRacingACreateInTheDroppedGenerationStillDropsIt(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	const database, bucket = "samegendb", "c"
	g1 := createDatabaseIn(t, server.core, database)

	resp := dropDuringCreate(t, server.core, database, bucket, g1, g1)
	if resp == nil || resp.Success || resp.Message != droppedWhileCreatingMessage(database, bucket) {
		t.Fatalf("a create in the generation dropped under it answered %+v; want the dropped-while-creating refusal", resp)
	}
	if server.core.serverDataAccessor().getDB(database) != nil {
		t.Fatalf("the drop of %q's only generation left it in the registry because a create in that generation was racing it", database)
	}
	got := viewOf(t, server.core, database, bucket)
	if got.catalogued || got.open || got.listed || len(got.live) != 0 {
		t.Fatalf("after a create raced the drop of its own generation, %s/%s: %+v; want nothing left", database, bucket, got)
	}
	assertSameAfterRestart(t, server, dataDir, database, bucket)
}

// Guard: the pull path of the same race.
func TestADropRacingAPullInTheDroppedGenerationStillDropsIt(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	const database, bucket = "samegenpulldb", "b"
	g1 := createDatabaseIn(t, server.core, database)

	if err := dropDuringMaterialise(t, server.core, database, bucket, g1, g1); err == nil {
		t.Fatalf("a pull of %s/%s in the generation dropped under it succeeded", database, bucket)
	}
	if server.core.serverDataAccessor().getDB(database) != nil {
		t.Fatalf("the drop of %q's only generation left it in the registry because a pull in that generation was racing it", database)
	}
	got := viewOf(t, server.core, database, bucket)
	if got.catalogued || got.open || got.listed || len(got.live) != 0 {
		t.Fatalf("after a pull raced the drop of its own generation, %s/%s: %+v; want nothing left", database, bucket, got)
	}
	assertSameAfterRestart(t, server, dataDir, database, bucket)
}
