package server

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Migration carries the declaration (typed-fields spec §8.4; build doc TF-5
// step 5.4): the migrate-start answer, the pulled bucket, and the catalog
// literal the target writes.

// nestedFieldsOption is a two-level declaration: an object whose member is an
// array of objects.
func nestedFieldsOption() bson.A {
	return bson.A{
		bson.M{"name": "Title", "type": "string", "required": true},
		bson.M{"name": "Order", "type": "object", "members": bson.A{
			bson.M{"name": "Lines", "type": "array", "elem": bson.M{"type": "object", "members": bson.A{
				bson.M{"name": "Sku", "type": "string", "required": true},
				bson.M{"name": "Qty", "type": "int"},
			}}},
		}},
	}
}

// createTypedOn creates bucket on node index of tc with fields and mode.
func createTypedOn(tc *testCluster, index int, bucket string, fields bson.A, mode string) {
	options := map[string]interface{}{"fields": fields}
	if mode != "" {
		options["field_types"] = mode
	}
	tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, Options: options})
}

// pullWhenReady pulls bucket from the pair's source, asking again while the
// source answers "not yet", as a restarted source does until its stream to
// the puller has opened in its new process.
func pullWhenReady(t *testing.T, tc *testCluster, pair migrationPair, bucket string) {
	t.Helper()
	target := tc.nodes[pair.target].server.core.cluster
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := target.pullBucket(ctx, testDatabase, bucket, mustNode(t, target, tc.addrs[pair.source]))
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("migrating %s from node %d to node %d: %v", bucket, pair.source, pair.target, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestMigratedBucketKeepsItsDeclarationAndItsOldDocuments(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "users_typed"
	pair := migrationPairFor(t, tc, bucket)
	createTypedOn(tc, pair.source, bucket, userFieldsOption(), "warn")
	// The documents are written as they are: nothing checks them yet, and
	// documents never are checked when they arrive by migration (spec §5.6).
	documents := map[string]map[string]interface{}{
		"u1": {"Name": "Ada", "Age": int32(36)},
		"u2": {"Name": "Bob", "Age": "thirty"},
	}
	for key, document := range documents {
		tc.ok(pair.source, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: document})
	}
	source := tc.nodes[pair.source].server.core
	pair.pull(t, tc, bucket)
	target := tc.nodes[pair.target].server.core

	sourceEntry, _, err := source.readCatalogEntry(testDatabase, bucket)
	if err != nil {
		t.Fatalf("reading the source's entry: %v", err)
	}
	targetEntry, catalogued, err := target.readCatalogEntry(testDatabase, bucket)
	if err != nil || !catalogued {
		t.Fatalf("reading the target's entry: catalogued %v, %v", catalogued, err)
	}
	if !reflect.DeepEqual(targetEntry.Fields, userFieldsCanonical()) || targetEntry.FieldTypes != "warn" {
		t.Errorf("the target's catalog records %+v in mode %q; want the source's %+v in warn", targetEntry.Fields, targetEntry.FieldTypes, sourceEntry.Fields)
	}
	fields, mode := lookupBucket(target, testDatabase, bucket).declaredFieldTypes().catalogForm()
	if !reflect.DeepEqual(fields, userFieldsCanonical()) || mode != "warn" {
		t.Errorf("the target serves the bucket declaring %+v in mode %q; want the source's declaration", fields, mode)
	}
	for key, want := range documents {
		got := documentOf(t, target, testDatabase, bucket, key)
		for field, value := range want {
			if !reflect.DeepEqual(got[field], value) {
				t.Errorf("document %s on the target has %s = %#v, want %#v", key, field, got[field], value)
			}
		}
	}
}

func TestNestedDeclarationSurvivesRestartAndMigration(t *testing.T) {
	// A restart: the tree is read back into the same bytes.
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTypedBucket(t, cs.core, "Order", nestedFieldsOption(), "")
	written := rawCatalogEntry(t, cs.core, "app", "Order").Lookup("fields").Value
	cs = restartCatalog(t, cs, dataDir)
	holder := servedBucket(t, cs.core, "app", "Order").declaredFieldTypes()
	fields, _ := holder.catalogForm()
	reencoded, err := encodeCatalogEntry(catalogEntry{Fields: fields})
	if err != nil {
		t.Fatalf("encoding the adopted declaration: %v", err)
	}
	if adopted := bson.Raw(reencoded).Lookup("fields").Value; !bytes.Equal(adopted, written) {
		t.Errorf("after a restart the nested declaration encodes as %x, want %x", adopted, written)
	}

	// A migration: the target records the same bytes.
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "orders_nested"
	pair := migrationPairFor(t, tc, bucket)
	createTypedOn(tc, pair.source, bucket, nestedFieldsOption(), "")
	pair.pull(t, tc, bucket)
	sourceFields := rawCatalogEntry(t, tc.nodes[pair.source].server.core, testDatabase, bucket).Lookup("fields").Value
	targetFields, err := rawCatalogEntry(t, tc.nodes[pair.target].server.core, testDatabase, bucket).LookupErr("fields")
	if err != nil {
		t.Fatalf("the migrated bucket's entry records no fields")
	}
	if !bytes.Equal(targetFields.Value, sourceFields) || !bytes.Equal(sourceFields, written) {
		t.Errorf("the nested declaration is %x on the source and %x on the target; want both %x", sourceFields, targetFields.Value, written)
	}
}

// TestUnreadableRecordedDeclarationSurvivesMigration is the migration clause of
// TestUnreadableRecordedDeclarationOpensRefusesWritesAndSurvivesAlterAndMigration
// (spec §8.4 item 4): a declaration the source cannot read travels byte for
// byte and the target opens the bucket too.
func TestUnreadableRecordedDeclarationSurvivesMigration(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "newer_decl"
	pair := migrationPairFor(t, tc, bucket)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	source := tc.nodes[pair.source].server.core
	written := withRawFields(t, rawCatalogEntry(t, source, testDatabase, bucket), unreadableFields())
	writeRawCatalogEntry(t, source, testDatabase, bucket, written)
	recordedFields := bson.Raw(written).Lookup("fields").Value
	// The source adopts the entry the way every node does: at its start.
	tc.restartNode(pair.source)

	pullWhenReady(t, tc, pair, bucket)
	target := tc.nodes[pair.target].server.core
	if lookupBucket(target, testDatabase, bucket).declaredFieldTypes() == nil {
		t.Error("the target serves the bucket with no declaration")
	}
	fields, err := rawCatalogEntry(t, target, testDatabase, bucket).LookupErr("fields")
	if err != nil {
		t.Fatalf("the target's entry dropped the recorded declaration")
	}
	if !bytes.Equal(fields.Value, recordedFields) {
		t.Errorf("the target records %x, want the source's %x byte for byte", fields.Value, recordedFields)
	}
}
