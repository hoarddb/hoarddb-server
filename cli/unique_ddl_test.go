package cli

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// S-15 (index-parity-build.md): "<Field> <type> unique" in a create bucket
// body is a unique index, the same descriptor the indices option produces.

// createFromStatement runs one CLI statement against database app and returns
// its error.
func createFromStatement(t *testing.T, c *CLI, statement string) error {
	t.Helper()
	cmd, err := ParseCommand(statement)
	if err != nil {
		t.Fatalf("parsing %q: %v", statement, err)
	}
	cmd.Database = "app"
	return c.handler.Handle(cmd)
}

// dumpedIndexSpecs dumps one bucket and returns the descriptors its metadata
// carries: the catalogued declaration, id and name included.
func dumpedIndexSpecs(t *testing.T, c *CLI, bucket string) []server.DumpIndexSpec {
	t.Helper()
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", bucket, outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dumping %s = %+v, %v", bucket, summary, err)
	}
	raw, err := os.ReadFile(filepath.Join(outDir, "app", bucket+".metadata.json"))
	if err != nil {
		t.Fatalf("reading the metadata of %s: %v", bucket, err)
	}
	var meta struct {
		IndexSpecs []server.DumpIndexSpec `json:"index_specs"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("parsing the metadata of %s: %v", bucket, err)
	}
	return meta.IndexSpecs
}

func insertEmail(c *CLI, bucket, key, email string) error {
	return c.handler.send(transport.OpPut, &server.InsertRequest{Database: "app", Bucket: bucket, Key: key,
		Data: map[string]interface{}{"Email": email}}, &server.InsertResponse{})
}

func TestDDLUniqueMatchesIndicesOption(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	if err := createFromStatement(t, c, "create bucket FromDDL { type: hash, placement: whole, Email string unique }"); err != nil {
		t.Fatalf("create from the DDL: %v", err)
	}
	if err := createFromStatement(t, c, `create bucket FromOption { type: hash, placement: whole, indices: [{fields: ["Email"], unique: true}] }`); err != nil {
		t.Fatalf("create from the indices option: %v", err)
	}
	fromDDL, fromOption := dumpedIndexSpecs(t, c, "FromDDL"), dumpedIndexSpecs(t, c, "FromOption")
	if len(fromOption) != 1 || !fromOption[0].Unique {
		t.Fatalf("the indices-option bucket declares %+v, want one unique index", fromOption)
	}
	if !reflect.DeepEqual(fromDDL, fromOption) {
		t.Fatalf("the DDL form catalogued %+v, the indices option %+v: they must be identical", fromDDL, fromOption)
	}
}

func TestDDLUniqueRefusesADuplicate(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	if err := createFromStatement(t, c, "create bucket Users { type: hash, placement: whole, Email string unique }"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := insertEmail(c, "Users", "u1", "ada@example.com"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := insertEmail(c, "Users", "u2", "ada@example.com")
	if err == nil || !strings.Contains(err.Error(), "Duplicate value for unique index") {
		t.Fatalf("a second document with the same Email = %v; want index parity's duplicate-key refusal", err)
	}
}

func TestScalarNoteDropsUniqueFields(t *testing.T) {
	declaration, err := parseDeclarationBody("{ Name string, Email string unique }")
	if err != nil {
		t.Fatal(err)
	}
	note := scalarNote(declaration.scalars)
	if !strings.Contains(note, "Name string") {
		t.Errorf("the note %q no longer lists Name string", note)
	}
	if strings.Contains(note, "Email") {
		t.Errorf("the note %q lists Email, which is enforced", note)
	}
}

func TestDDLUniqueWithStoreFieldSendsBothOptions(t *testing.T) {
	cmd, err := ParseCommand("create bucket Post { type: hash, placement: whole, Title string unique, Comments fifoStore }")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := parseBucketSchema(cmd, "app")
	if err != nil {
		t.Fatal(err)
	}
	options := schema.Options
	if _, ok := options["stores"]; !ok {
		t.Errorf("options %v carry no stores", options)
	}
	if _, ok := options["indices"]; !ok {
		t.Errorf("options %v carry no indices", options)
	}

	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	if err := createFromStatement(t, c, "create bucket Post { type: hash, placement: whole, Title string unique, Comments fifoStore }"); err != nil {
		t.Fatalf("create: %v", err)
	}
	var stats server.BucketStatsResponse
	mustSend(t, c, transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "Post"}, &stats)
	if len(stats.Stores) != 1 || stats.Stores[0].Name != "Comments" {
		t.Errorf("the bucket reports stores %+v, want Comments", stats.Stores)
	}
	if len(stats.Indexes) != 1 || stats.Indexes[0] != "Title_1 (unique)" {
		t.Errorf("the bucket reports indexes %q, want [Title_1 (unique)]", stats.Indexes)
	}
}

func TestDDLUniqueOnANonIndexableTypeIsIndexParitysRefusal(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	fromOption := createFromStatement(t, c, `create bucket Events { type: fifo, indices: [{fields: ["Email"], unique: true}] }`)
	fromDDL := createFromStatement(t, c, "create bucket Events { type: fifo, Email string unique }")
	if fromOption == nil || !strings.Contains(fromOption.Error(), "maintained only on hash buckets") {
		t.Fatalf("the indices option on a fifo bucket = %v; want index parity's refusal", fromOption)
	}
	if fromDDL == nil || fromDDL.Error() != fromOption.Error() {
		t.Fatalf("the DDL form = %v; want exactly %q", fromDDL, fromOption)
	}
}

// TestDDLUniqueSurvivesDumpAndRestore: a unique index declared in the DDL is
// catalogued as one, so a dump carries its uniqueness and the restored bucket
// refuses a duplicate.
func TestDDLUniqueSurvivesDumpAndRestore(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	if err := createFromStatement(t, c, "create bucket users { type: hash, placement: whole, Email string unique }"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := insertEmail(c, "users", "u1", "held@example.com"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	outDir := filepath.Join(t.TempDir(), "dump")
	if summary, err := runDumpBuckets(c.handler, "app", "users", outDir, io.Discard, nil); err != nil || len(summary.failures) != 0 {
		t.Fatalf("dump = %+v, %v", summary, err)
	}
	dbDir := filepath.Join(outDir, "app")
	for _, suffix := range []string{".bson", ".metadata.json"} {
		if err := os.Rename(filepath.Join(dbDir, "users"+suffix), filepath.Join(dbDir, "restored"+suffix)); err != nil {
			t.Fatal(err)
		}
	}
	rewriteJSONFile(t, filepath.Join(dbDir, "restored.metadata.json"), func(meta map[string]interface{}) { meta["bucket"] = "restored" })
	if summary := restoreFrom(t, c, outDir); len(summary.failures) != 0 {
		t.Fatalf("restore reported failures: %v", summary.failures)
	}
	if got := restoredIndexes(t, c); len(got) != 1 || got[0] != "Email_1 (unique)" {
		t.Fatalf("the restored bucket declares %q, want [Email_1 (unique)]", got)
	}
	if insertRestored(t, c, "u2", "held@example.com") {
		t.Fatal("the restored bucket accepted a duplicate Email: the DDL-declared unique index restored as a plain one")
	}
}

// TestAlterRefusesAUniqueEntry: index parity has no way to add an index to an
// existing bucket, so an alter that asks for one is refused, not half-applied.
func TestAlterRefusesAUniqueEntry(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash}, nil)
	for _, body := range []string{`{ Email string unique }`, `{ Tags hashStore, Email string unique }`} {
		err := createFromStatement(t, c, "alter bucket Post "+body)
		if err == nil || !strings.Contains(err.Error(), "cannot add a unique index (Email)") || !strings.Contains(err.Error(), "create bucket Post") {
			t.Errorf("alter bucket Post %s = %v; want the refusal naming Email and the create form", body, err)
		}
	}
	var stats server.BucketStatsResponse
	mustSend(t, c, transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "Post"}, &stats)
	if len(stats.Stores) != 0 {
		t.Errorf("a refused alter still declared stores %+v", stats.Stores)
	}
}

// TestDDLUniqueBesideAnExplicitDuplicateIsRefusedByTheServer: no merge rule
// here; parseIndexSpecs refuses two indexes on the same fields.
func TestDDLUniqueBesideAnExplicitDuplicateIsRefusedByTheServer(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	err := createFromStatement(t, c, `create bucket Users { type: hash, indices: ["Email"], Email string unique }`)
	if err == nil || !strings.Contains(err.Error(), "both on the same fields") {
		t.Fatalf("a DDL unique beside indices naming the same field = %v; want the server's same-fields refusal", err)
	}
}
