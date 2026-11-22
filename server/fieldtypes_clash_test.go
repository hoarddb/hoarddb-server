package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// A name is a typed field or a store field, never both (typed-fields spec
// §4.7 rule 3), at the alter as at the create; and a catalog that holds such a
// clash anyway still opens, because a refused attach leaves a bucket's data
// unreachable.

// TestStoreFieldNamedLikeTypedFieldIsRefused: the alter that would add the
// clash is refused, and the bucket is served across a restart.
func TestStoreFieldNamedLikeTypedFieldIsRefused(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTypedBucket(t, cs.core, "P", bson.A{bson.M{"name": "Tags", "type": "string"}}, "")
	if r, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "P", Key: "k1", Data: map[string]interface{}{"Tags": "x"}}); err != nil || !r.Success {
		t.Fatalf("insert: %+v %v", r, err)
	}
	resp, err := cs.core.DeclareStoreFieldsCtx(t.Context(), DeclareStoreFieldsRequest{Database: "app", Name: "P",
		Stores: []map[string]interface{}{{"name": "Tags", "engine": "fifoStore"}}})
	if err == nil && resp.Success {
		t.Errorf("a store field named like the typed field Tags was declared: %q", resp.Message)
	} else if err == nil || !strings.Contains(err.Error(), "both as a typed field and as a store field") {
		t.Errorf("the declare answered %+v, %v; want the rule-3 refusal", resp, err)
	}
	cs = restartCatalog(t, cs, dataDir)
	if lookupBucket(cs.core, "app", "P") == nil {
		t.Fatal("after the restart bucket P is not served")
	}
	if document := documentOf(t, cs.core, "app", "P", "k1"); document["Tags"] != "x" {
		t.Errorf("after the restart document k1 reads %v", document)
	}
}

// TestRecordedStoreFieldNamedFieldTypesStillOpens: field_types became a
// reserved store-field name in this build (SA-13). A bucket recorded with a
// store field of that name before it is still served, with a warning, and a
// new declaration of the name is still refused.
func TestRecordedStoreFieldNamedFieldTypesStillOpens(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "Old", BucketTypeHash)
	if r, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "Old", Key: "k1", Data: map[string]interface{}{"n": int32(1)}}); err != nil || !r.Success {
		t.Fatalf("insert: %+v %v", r, err)
	}
	writeCatalogStores(t, cs.core, "app", "Old", []catalogStore{{Name: "field_types", ID: 1, Engine: "fifo"}}, 2)

	cs = restartCatalog(t, cs, dataDir)
	bucket := lookupBucket(cs.core, "app", "Old")
	if bucket == nil {
		t.Fatal("a bucket with a store field named field_types, recorded before the name was reserved, is not served after a restart")
	}
	if names := storeNames(bucket); len(names) != 1 || names[0] != "field_types" {
		t.Errorf("the bucket declares store fields %v; want its field_types kept", names)
	}
	if document := documentOf(t, cs.core, "app", "Old", "k1"); document == nil {
		t.Error("document k1 is not served")
	}
	_, err := cs.core.DeclareStoreFieldsCtx(t.Context(), DeclareStoreFieldsRequest{Database: "app", Name: "Old",
		Stores: []map[string]interface{}{{"name": "field_types", "engine": "hashStore"}}})
	if err == nil || !strings.Contains(err.Error(), `"field_types" is reserved`) {
		t.Errorf("a new store field named field_types: %v; want it refused as reserved", err)
	}
}

// TestRecordedTypedAndStoreFieldClashStillOpens: a catalog written with the
// clash (by a build before the alter refused it) opens and serves its
// documents; the declaration is kept as recorded.
func TestRecordedTypedAndStoreFieldClashStillOpens(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTypedBucket(t, cs.core, "P", bson.A{bson.M{"name": "Tags", "type": "string"}}, "")
	if r, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "P", Key: "k1", Data: map[string]interface{}{"Tags": "x"}}); err != nil || !r.Success {
		t.Fatalf("insert: %+v %v", r, err)
	}
	writeCatalogStores(t, cs.core, "app", "P", []catalogStore{{Name: "Tags", ID: 1, Engine: "fifo"}}, 2)
	recorded := rawCatalogEntry(t, cs.core, "app", "P").Lookup("fields").Value

	cs = restartCatalog(t, cs, dataDir)
	if lookupBucket(cs.core, "app", "P") == nil {
		t.Fatal("a bucket whose catalog holds a typed field and a store field of one name is not served after a restart: its data is unreachable")
	}
	if document := documentOf(t, cs.core, "app", "P", "k1"); document["Tags"] != "x" {
		t.Errorf("document k1 reads %v", document)
	}
	if fields := rawCatalogEntry(t, cs.core, "app", "P").Lookup("fields").Value; string(fields) != string(recorded) {
		t.Error("the start changed the recorded declaration")
	}
}

// TestRecordedDeclarationThatFailsValidationRefusesWrites is the coordinator's decision of 2026-10-05: a bucket whose recorded
// declaration fails this build's validator is served, but its declaration is
// not enforced as recorded. Every write is refused as an unreadable
// declaration's is (UNSUPPORTED_VERSION, SA-16), with the validator's reason
// and the fix; reads are served; describe says why writes are refused.
func TestRecordedDeclarationThatFailsValidationRefusesWrites(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTypedBucket(t, cs.core, "P", bson.A{bson.M{"name": "Tags", "type": "string"}}, "")
	if r, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "P", Key: "k1", Data: map[string]interface{}{"Tags": "x"}}); err != nil || !r.Success {
		t.Fatalf("insert: %+v %v", r, err)
	}
	writeCatalogStores(t, cs.core, "app", "P", []catalogStore{{Name: "Tags", ID: 1, Engine: "fifo"}}, 2)
	cs = restartCatalog(t, cs, dataDir)

	reason := `"Tags" is declared on bucket "P" both as a typed field and as a store field`
	_, err := cs.core.Insert(InsertRequest{Database: "app", Bucket: "P", Key: "k2", Data: map[string]interface{}{"Tags": "y"}})
	if err == nil {
		t.Fatal("a write to a bucket whose recorded declaration fails validation was stored")
	}
	if codeFor(err) != transport.ErrUnsupportedVersion {
		t.Errorf("the refusal answers %s; want UNSUPPORTED_VERSION, as an unreadable declaration's", codeFor(err))
	}
	want := `A write to bucket "P" (database "app") was refused: its recorded typed-field declaration fails this version's checks (` + reason + `; a name is one or the other. Rename one of them.` +
		`), so it cannot be enforced. Reads still work. To write to it again: dump the bucket, create a new bucket whose declaration passes, restore the dump into it, then drop "P".`
	if err.Error() != want {
		t.Errorf("the refusal reads\n  %q\nwant\n  %q", err.Error(), want)
	}
	if document := documentOf(t, cs.core, "app", "P", "k1"); document["Tags"] != "x" {
		t.Errorf("document k1 reads %v; want it served", document)
	}
	stats, err := cs.core.BucketStats(BucketStatsRequest{Database: "app", Bucket: "P"})
	if err != nil || !strings.HasPrefix(stats.WritesRefused, "Writes are refused: this bucket's ") || !strings.Contains(stats.WritesRefused, reason) {
		t.Errorf("describe says writes_refused %q (%v); want a sentence starting \"Writes are refused: this bucket's \" with the validator's reason", stats.WritesRefused, err)
	}
}
