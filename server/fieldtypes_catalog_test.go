package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The catalog record of a declaration (typed-fields spec §8.1, §8.2, §8.4,
// §9.3; build doc TF-5 step 5.2).

// rawCatalogEntry is the bytes of a bucket's catalog entry as the store holds
// them.
func rawCatalogEntry(t *testing.T, s *Server, database, bucket string) bson.Raw {
	t.Helper()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(catalogKey(database, bucket))
	if err != nil {
		t.Fatalf("reading the catalog entry of %s/%s: %v", database, bucket, err)
	}
	return bson.Raw(append([]byte(nil), encoded...))
}

// writeRawCatalogEntry replaces a bucket's catalog entry with encoded.
func writeRawCatalogEntry(t *testing.T, s *Server, database, bucket string, encoded []byte) {
	t.Helper()
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(catalogKey(database, bucket), encoded); err != nil {
		t.Fatalf("writing the catalog entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the catalog entry: %v", err)
	}
}

// withRawFields returns entry with its fields key set to fields, appended
// after every other key: the way a newer binary would have written it.
func withRawFields(t *testing.T, entry bson.Raw, fields bson.A) []byte {
	t.Helper()
	var document bson.D
	if err := bson.Unmarshal(entry, &document); err != nil {
		t.Fatalf("decoding the entry: %v", err)
	}
	document = append(document, bson.E{Key: "fields", Value: fields})
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("encoding the entry: %v", err)
	}
	return encoded
}

// TestCatalogWithoutFieldsLoadsUndeclaredAndIsNotRewritten (spec §8.2): an
// entry that declares no typed fields carries no key for them, and a start
// does not rewrite it.
func TestCatalogWithoutFieldsLoadsUndeclaredAndIsNotRewritten(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "Plain", BucketTypeHash)
	before := rawCatalogEntry(t, cs.core, "app", "Plain")
	for _, key := range []string{"fields", "field_types"} {
		if _, err := before.LookupErr(key); err == nil {
			t.Errorf("an undeclared bucket's entry carries %q: every entry written before typed fields would differ from this one", key)
		}
	}
	cs = restartCatalog(t, cs, dataDir)
	if after := rawCatalogEntry(t, cs.core, "app", "Plain"); !bytes.Equal(before, after) {
		t.Errorf("the start rewrote an undeclared bucket's entry:\n before %x\n after  %x", []byte(before), []byte(after))
	}
}

// encodedSize is the size of entry as encodeCatalogEntry writes it.
func encodedSize(t *testing.T, entry catalogEntry) int {
	t.Helper()
	encoded, err := encodeCatalogEntry(entry)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	return len(encoded)
}

// TestCatalogFieldBytesMatchTheSpec pins spec §9.3's arithmetic: 13 bytes for
// the array; per field 30 + name + word at positions 0 to 9 and 31 from 10;
// 11 more when required; 22 for a non-default mode.
func TestCatalogFieldBytesMatchTheSpec(t *testing.T) {
	base := catalogEntry{database: "app", bucket: "User", ID: 7, Slot: 3, Type: BucketTypeHash}
	plain := encodedSize(t, base)

	age := base
	age.Fields = []catalogField{{Name: "Age", catalogType: catalogType{Type: "int"}}}
	if got := encodedSize(t, age) - plain; got != 13+36 {
		t.Errorf("Age int costs %d bytes with its array; want 13 + 36", got)
	}

	twelve := base
	for i := range 12 {
		twelve.Fields = append(twelve.Fields, catalogField{Name: fmt.Sprintf("F%02d", i), catalogType: catalogType{Type: "int"}})
	}
	if got, want := encodedSize(t, twelve)-plain, 13+10*(30+3+3)+2*(31+3+3); got != want {
		t.Errorf("twelve 3-letter int fields cost %d bytes; want %d", got, want)
	}

	required := age
	required.Fields = []catalogField{{Name: "Age", catalogType: catalogType{Type: "int"}, Required: true}}
	if got := encodedSize(t, required) - encodedSize(t, age); got != 11 {
		t.Errorf("required adds %d bytes; want 11", got)
	}

	warn := age
	warn.FieldTypes = "warn"
	if got := encodedSize(t, warn) - encodedSize(t, age); got != 22 {
		t.Errorf("field_types: warn adds %d bytes; want 22", got)
	}
}

// unreadableFields is a declaration a newer binary wrote: one field this
// binary reads, one with a type word it does not know, one with a key it does
// not know, and one whose unknown word is two levels down.
func unreadableFields() bson.A {
	return bson.A{
		bson.D{{Key: "name", Value: "Age"}, {Key: "type", Value: "int"}},
		bson.D{{Key: "name", Value: "Area"}, {Key: "type", Value: "polygon"}, {Key: "required", Value: true}},
		bson.D{{Key: "name", Value: "Score"}, {Key: "type", Value: "int"}, {Key: "default", Value: int32(3)}},
		bson.D{{Key: "name", Value: "Address"}, {Key: "type", Value: "object"}, {Key: "members", Value: bson.A{
			bson.D{{Key: "name", Value: "Geo"}, {Key: "type", Value: "array"}, {Key: "elem", Value: bson.D{{Key: "type", Value: "point"}}}},
		}}},
	}
}

// TestUnreadableRecordedDeclarationOpensRefusesWritesAndSurvivesAlterAndMigration
// is spec §8.4's rule for a declaration this binary cannot read. Each clause
// is added by the step that makes it true: this step's are that the bucket
// opens and that a rewrite of its entry keeps the declaration byte for byte;
// step 5.4 adds migration (fieldtypes_migrate_test.go), TF-6 the refused
// writes and TF-8 the alter.
func TestUnreadableRecordedDeclarationOpensRefusesWritesAndSurvivesAlterAndMigration(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "Newer", BucketTypeHash)
	written := withRawFields(t, rawCatalogEntry(t, cs.core, "app", "Newer"), unreadableFields())
	writeRawCatalogEntry(t, cs.core, "app", "Newer", written)
	recordedFields := bson.Raw(written).Lookup("fields").Value

	cs = restartCatalog(t, cs, dataDir)
	servedBucket(t, cs.core, "app", "Newer")

	// A store-field declare reads the entry, changes its stores and writes it
	// back: the one rewrite of an entry this build has besides a create's.
	resp, err := cs.core.DeclareStoreFieldsCtx(t.Context(), DeclareStoreFieldsRequest{Database: "app", Name: "Newer",
		Stores: []map[string]interface{}{{"name": "Comments", "engine": "fifoStore"}}})
	if err != nil || !resp.Success {
		t.Fatalf("declaring a store field on the bucket: %+v, %v", resp, err)
	}
	rewritten := rawCatalogEntry(t, cs.core, "app", "Newer")
	if _, err := rewritten.LookupErr("stores"); err != nil {
		t.Fatalf("the declare did not rewrite the entry: %v", err)
	}
	fields, err := rewritten.LookupErr("fields")
	if err != nil {
		t.Fatalf("the rewrite dropped the recorded declaration")
	}
	if !bytes.Equal(fields.Value, recordedFields) {
		t.Errorf("the rewrite changed the recorded declaration:\n written %x\n now     %x", recordedFields, fields.Value)
	}

	cs = restartCatalog(t, cs, dataDir)
	servedBucket(t, cs.core, "app", "Newer")
	if fields := rawCatalogEntry(t, cs.core, "app", "Newer").Lookup("fields"); !bytes.Equal(fields.Value, recordedFields) {
		t.Errorf("after a second start the recorded declaration reads %x, want %x", fields.Value, recordedFields)
	}

	// TF-6's clause: every write is refused, naming the newer binary, with
	// UNSUPPORTED_VERSION (SA-16), even one that the readable fields accept;
	// reads are served, and describe says why writes are refused.
	requireWritesRefusedAsNewer := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s was stored in a bucket whose declaration this binary cannot read", what)
			return
		}
		const want = `A write to bucket "Newer" (database "app") was refused: its typed-field declaration was written by a newer version of hoardDB, which this version cannot read, so it cannot check the write. Reads still work. Upgrade this node to that version or later; the declaration is kept as recorded.`
		if codeFor(err) != transport.ErrUnsupportedVersion || err.Error() != want {
			t.Errorf("%s was refused with %q (code %s); want UNSUPPORTED_VERSION and\n  %q", what, err, codeFor(err), want)
		}
	}
	_, err = cs.core.Insert(InsertRequest{Database: "app", Bucket: "Newer", Key: "k1", Data: map[string]interface{}{"Age": int32(36)}})
	requireWritesRefusedAsNewer("an insert", err)
	_, err = cs.core.Push(PushRequest{Database: "app", Bucket: "Newer", Data: map[string]interface{}{"Age": int32(36)}})
	requireWritesRefusedAsNewer("a push", err)
	_, err = cs.core.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "Newer", Data: []map[string]interface{}{{"Age": int32(36)}}})
	requireWritesRefusedAsNewer("a batch", err)
	// TF-7's clause: an update is refused the same way, before the document
	// is read, even a multi-update that matches nothing.
	_, err = cs.core.Update(UpdateRequest{Database: "app", Bucket: "Newer", Key: "k1", Set: map[string]interface{}{"Age": int32(36)}})
	requireWritesRefusedAsNewer("an update", err)
	_, err = cs.core.Update(UpdateRequest{Database: "app", Bucket: "Newer", Multi: true, Set: map[string]interface{}{"Age": int32(36)}})
	requireWritesRefusedAsNewer("a multi-update", err)
	if found, err := cs.core.Find(FindRequest{Database: "app", Bucket: "Newer", Key: "absent"}); err != nil || found.Found {
		t.Errorf("a read of the bucket answered %+v, %v; want it served (not found)", found, err)
	}
	stats, err := cs.core.BucketStats(BucketStatsRequest{Database: "app", Bucket: "Newer"})
	if err != nil || !strings.HasPrefix(stats.WritesRefused, "Writes are refused: this bucket's ") || !strings.Contains(stats.WritesRefused, "newer version of hoardDB") {
		t.Errorf("describe says writes_refused %q (%v); want a sentence starting \"Writes are refused: this bucket's \" naming the newer version", stats.WritesRefused, err)
	}
}
