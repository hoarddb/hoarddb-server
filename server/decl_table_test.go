package server

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/store"
)

// The declaration leaves the bucket struct (catalog-gc spec §5.2 "declaration
// table", §5.3 "post-publication writes"; build doc CG-9). A bucket that
// declares indexes or store fields keeps that declaration in a side table
// keyed by its id; a plain bucket carries no declaration at all.

// TestDeclarationIsNotHeldInTheBucketStruct: the record every bucket has
// holds neither the index list, nor the store-field set, nor the typed-field
// declaration, nor a pointer to any of them. A bucket's declarations are
// reached through the declaration table (recHasDecl) and the interned
// typed-field set (typesID). The handle an operation builds may hold them; it
// is not kept.
func TestDeclarationIsNotHeldInTheBucketStruct(t *testing.T) {
	recordType := reflect.TypeFor[bucketRec]()
	held := map[reflect.Type]string{
		reflect.TypeFor[publishedSpecs]():                    "the index list",
		reflect.TypeFor[atomic.Pointer[[]store.IndexSpec]](): "the index list",
		reflect.TypeFor[atomic.Pointer[storeFieldSet]]():     "the store-field set",
		reflect.TypeFor[*storeFieldSet]():                    "the store-field set",
		reflect.TypeFor[atomic.Pointer[bucketFieldTypes]]():  "the typed-field declaration",
		reflect.TypeFor[*bucketFieldTypes]():                 "the typed-field declaration",
		reflect.TypeFor[*bucketDecl]():                       "the declaration",
		reflect.TypeFor[atomic.Pointer[bucketDecl]]():        "the declaration",
	}
	for i := range recordType.NumField() {
		field := recordType.Field(i)
		if what, isDeclaration := held[field.Type]; isDeclaration {
			t.Errorf("bucketRec.%s (%s) holds %s: a plain bucket pays for a declaration it does not have", field.Name, field.Type, what)
		}
	}
	t.Logf("bucketRec is %d bytes", recordType.Size())
}

// TestDeclarationVisibleToReaders (spec §8): a reader racing the declare that
// gives a plain bucket its first declaration sees the bucket plain, or flagged
// with its declaration — never flagged with no declaration to find. The writer
// enters the declaration in the table and only then sets the flag; a reader
// that sees the flag first and finds nothing would serve the bucket with no
// store fields and no indexes. Run it under -race.
func TestDeclarationVisibleToReaders(t *testing.T) {
	const iterations = 2000
	s := newTestServer(t)
	for i := range iterations {
		name := fmt.Sprintf("b%04d", i)
		createTestBucket(t, s, "app", name, BucketTypeHash)
		resident := lookupBucket(s, "app", name)
		id := resident.id.Load()

		var stop atomic.Bool
		var flaggedWithout, viewsWithout atomic.Int64
		reading := make(chan struct{})
		go func() {
			defer close(reading)
			for !stop.Load() {
				// The flag, then the table: what readView does.
				for range 64 {
					if resident.rec.hasDecl() && declOf(s, id) == nil {
						flaggedWithout.Add(1)
					}
				}
				flagged := resident.rec.hasDecl()
				view, snap, err := s.resolveForRead("app", name)
				if err != nil {
					t.Errorf("reading %s during its declare: %v", name, err)
					return
				}
				if _, declared := view.storeFields().find("Notes", 0); flagged && !declared {
					viewsWithout.Add(1)
				}
				snap.Discard()
			}
		}()
		resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", name, StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
		stop.Store(true)
		<-reading
		if err != nil || !resp.Success {
			t.Fatalf("declaring Notes on %s: %+v, %v", name, resp, err)
		}
		if n := flaggedWithout.Load(); n != 0 {
			t.Fatalf("iteration %d: a reader saw %s flagged %d time(s) with no declaration in the table", i, name, n)
		}
		if n := viewsWithout.Load(); n != 0 {
			t.Fatalf("iteration %d: %d read view(s) of %s, built after the flag was seen, had no Notes", i, n, name)
		}
		view, snap, err := s.resolveForRead("app", name)
		if err != nil {
			t.Fatalf("reading %s after its declare: %v", name, err)
		}
		if _, declared := view.storeFields().find("Notes", 0); !declared || !resident.rec.hasDecl() {
			t.Fatalf("iteration %d: after the declare returned, %s is flagged=%v and a read view declares Notes=%v", i, name, resident.rec.hasDecl(), declared)
		}
		snap.Discard()
	}

	// The race above finds a flag set before its entry only when a reader
	// lands in the gap. This finds it every time: with the bucket's table
	// shard held, a writer that puts first cannot get past the put, so the
	// flag stays clear for as long as the shard is held.
	createTestBucket(t, s, "app", "held", BucketTypeHash)
	resident := lookupBucket(s, "app", "held")
	shard := s.decls.shard(resident.id.Load())
	shard.mu.Lock()
	declared := make(chan error, 1)
	go func() {
		resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "held", StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
		if err == nil && !resp.Success {
			err = fmt.Errorf("declare refused: %s", resp.Message)
		}
		declared <- err
	}()
	for deadline := time.Now().Add(200 * time.Millisecond); time.Now().Before(deadline); {
		if resident.rec.hasDecl() {
			shard.mu.Unlock()
			<-declared
			t.Fatal("the bucket was flagged while its declaration could not yet be in the table: the flag is set before the entry is put")
		}
		runtime.Gosched()
	}
	shard.mu.Unlock()
	if err := <-declared; err != nil {
		t.Fatalf("declaring Notes on held: %v", err)
	}
	if !resident.rec.hasDecl() || declOf(s, resident.id.Load()) == nil {
		t.Fatal("after its declare the bucket is not flagged with its declaration in the table")
	}
}

// TestAttachEntersTheDeclarationBeforeTheId (I-1): a create of a flagged
// bucket enters its declaration in the table before it stores the id, so a
// reader that sees the id finds the declaration. With every shard of the table
// held, a create that puts first cannot store its id; one that stores the id
// first is seen doing it.
func TestAttachEntersTheDeclarationBeforeTheId(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "first", BucketTypeHash)
	for i := range s.decls.shards {
		s.decls.shards[i].mu.Lock()
	}
	release := func() {
		for i := range s.decls.shards {
			s.decls.shards[i].mu.Unlock()
		}
	}
	created := make(chan error, 1)
	go func() {
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "indexed", Type: BucketTypeHash,
			Options: map[string]interface{}{"indices": []interface{}{"Email"}}})
		if err == nil && !resp.Success {
			err = fmt.Errorf("create refused: %s", resp.Message)
		}
		created <- err
	}()
	for deadline := time.Now().Add(200 * time.Millisecond); time.Now().Before(deadline); {
		if claimed := lookupBucket(s, "app", "indexed"); claimed != nil && claimed.id.Load() != 0 {
			release()
			<-created
			t.Fatal("the bucket's id was stored while its declaration could not yet be in the table: a reader that sees the id would serve it with no indexes")
		}
		runtime.Gosched()
	}
	release()
	if err := <-created; err != nil {
		t.Fatalf("creating indexed: %v", err)
	}
	indexed := lookupBucket(s, "app", "indexed")
	if decl := declOf(s, indexed.id.Load()); decl == nil || len(decl.indexSpecs.load()) != 1 || !indexed.rec.hasDecl() {
		t.Fatalf("after its create the bucket is flagged=%v with declaration %v in the table, want flagged with one index", indexed.rec.hasDecl(), decl)
	}
}

// TestWriteAfterTheUpgradeRebuildUsesTheRebuiltIndex (spec N-8): the attach
// builds the bucket's engine over its recorded, pre-build index, and the
// rebuild then records it as this build's. A write after that must be indexed
// under this build's rule — a document without the field gets a null entry —
// which only an engine built again from the declaration does: the manager the
// attach built still follows the old rule and writes no entry.
func TestWriteAfterTheUpgradeRebuildUsesTheRebuiltIndex(t *testing.T) {
	dataDir := t.TempDir()
	const seeded = 9
	first := seedPreBuildIndex(t, dataDir, seeded)
	s := restartCatalog(t, first, dataDir).core
	wantRebuiltEmailIndex(t, s, seeded)

	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "late", Data: map[string]interface{}{"n": seeded}}); err != nil || !resp.Success {
		t.Fatalf("inserting a document without Email after the rebuild: %+v, %v", resp, err)
	}
	users := servedBucket(t, s, "app", "users")
	if entries := indexEntries(t, s, users, 0); len(entries) != seeded+1 {
		t.Fatalf("after one more document the Email index holds %d entries for %d documents: the write was indexed by the manager built before the rebuild", len(entries), seeded+1)
	}
}

// TestDeclareOnAPlainBucketThenRestart: a bucket created plain and given a
// store field afterwards — the declare that makes a plain bucket flagged —
// comes back from a restart with the declaration, and a write to the new
// field works.
func TestDeclareOnAPlainBucketThenRestart(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "plain", BucketTypeHash)
	if declared := servedBucket(t, cs.core, "app", "plain").storeFields().stores; len(declared) != 0 {
		t.Fatalf("a bucket created with no store fields declares %v", declared)
	}
	resp, err := cs.core.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "plain", StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
	if err != nil || !resp.Success {
		t.Fatalf("declaring Notes on the plain bucket: %+v, %v", resp, err)
	}
	if _, found := servedBucket(t, cs.core, "app", "plain").storeFields().find("Notes", 0); !found {
		t.Fatal("the declare succeeded and the bucket does not declare Notes")
	}

	cs = restartCatalog(t, cs, dataDir)
	notes, found := servedBucket(t, cs.core, "app", "plain").storeFields().find("Notes", 0)
	if !found {
		t.Fatal("after a restart the bucket no longer declares Notes")
	}
	if insert, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "plain", Key: "d1", Data: map[string]interface{}{"title": "t"}}); err != nil || !insert.Success {
		t.Fatalf("inserting d1 after the restart: %+v, %v", insert, err)
	}
	fieldOK(t)(cs.core.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: "plain", Key: "d1", Field: "Notes", Element: "n1", Data: map[string]interface{}{"text": "hi"}}))
	got, err := cs.core.FieldGet(FieldRequest{Database: "app", Bucket: "plain", Key: "d1", Field: "Notes", Element: "n1"})
	if err != nil || !got.Found {
		t.Fatalf("Notes[n1] (store id %d) after a restart = %+v, %v; want the element", notes.ID, got, err)
	}
}

// TestIndexSpecsGaugeBalancesAcrossDeclareAndDrop: hoarddb_indexes counts a
// bucket's indexes while it is served and stops when it is dropped, by either
// drop: the bucket's own, and its database's. The flag that says an index list
// is counted moves with the declaration, so a drop that loses it leaves the
// gauge high for good, and one that uncounts twice drives it below zero.
func TestIndexSpecsGaugeBalancesAcrossDeclareAndDrop(t *testing.T) {
	s := newTestServer(t)
	ready := func() float64 { return gatheredGauge(t, "hoarddb_indexes", "state", "ready") }
	before := ready()

	createTestBucketWithOptions(t, s, "app", "users", BucketTypeHash, map[string]interface{}{"indices": []interface{}{"Email", "Name"}})
	createTestBucketWithOptions(t, s, "app", "orders", BucketTypeHash, map[string]interface{}{"indices": []interface{}{"Total"}})
	createTestBucket(t, s, "app", "notes", BucketTypeHash)
	if resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "notes", StoreDeclaration{Name: "Tags", Engine: "hashStore"})); err != nil || !resp.Success {
		t.Fatalf("declaring Tags on notes: %+v, %v", resp, err)
	}
	if moved := ready() - before; moved != 3 {
		t.Fatalf("three ready indexes declared moved hoarddb_indexes{state=\"ready\"} by %v, want 3", moved)
	}
	if n := s.decls.count(); n != 3 {
		t.Fatalf("the declaration table holds %d entries for the three flagged buckets, want 3", n)
	}

	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "users"}); err != nil || !resp.Success {
		t.Fatalf("dropping users: %+v, %v", resp, err)
	}
	if moved := ready() - before; moved != 1 {
		t.Errorf("after dropping users (two indexes) hoarddb_indexes{state=\"ready\"} is %v above where it started, want 1", moved)
	}
	if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: "app"}); err != nil || !resp.Success {
		t.Fatalf("dropping database app: %+v, %v", resp, err)
	}
	if moved := ready() - before; moved != 0 {
		t.Errorf("after dropping the database hoarddb_indexes{state=\"ready\"} is %v away from where it started, want 0", moved)
	}
	if n := s.decls.count(); n != 0 {
		t.Errorf("the declaration table holds %d entries after every flagged bucket was dropped, want 0", n)
	}
}
