package cli

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCreateBucketParsesEveryType covers the six store keywords plus the
// b+tree alias, with and without braces, and with the optional `in <db>`
// clause.
func TestCreateBucketParsesEveryType(t *testing.T) {
	cases := []struct {
		line     string
		wantType string
		wantDB   string
	}{
		{"create bucket Users { type: hash }", "hash", ""},
		{`create bucket ByEmail { type: "btree", indices: ["Email"] }`, "btree", ""},
		{"create bucket Jobs type fifo", "fifo", ""},
		{"create bucket Stack type lifo", "lifo", ""},
		{"create bucket Alerts { type: heap }", "heap", ""},
		{"create bucket Attachments { type: blob }", "blob", ""},
		// The alias is accepted by the parser but prints as the keyword.
		{"create bucket Graphs { type: b+tree }", "btree", ""},
		// `in <db>` names the database the session does not.
		{"create bucket Users in app { type: hash }", "hash", "app"},
		{"create bucket Events in metrics type fifo", "fifo", "metrics"},
	}

	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			cmd, err := ParseCommand(tc.line)
			if err != nil {
				t.Fatalf("ParseCommand(%q): %v", tc.line, err)
			}
			if cmd.Method != "create_bucket" {
				t.Fatalf("Method = %q, want create_bucket", cmd.Method)
			}
			if cmd.Database != tc.wantDB {
				t.Fatalf("Database = %q, want %q", cmd.Database, tc.wantDB)
			}
			schema, err := parseBucketSchema(cmd, "app")
			if err != nil {
				t.Fatalf("parseBucketSchema(%q): %v", tc.line, err)
			}
			if string(schema.Type) != tc.wantType {
				t.Fatalf("type = %q, want %q", schema.Type, tc.wantType)
			}
		})
	}
}

// TestDefineBucketIsARenameError keeps the old verb loud: it must name the
// replacement and a complete command, never fall through to the generic
// "unknown command: define".
func TestDefineBucketIsARenameError(t *testing.T) {
	_, err := ParseCommand("define bucket Users { type: hash }")
	if err == nil {
		t.Fatal("define bucket parsed; it must be a rename error")
	}
	if !strings.Contains(err.Error(), "create bucket") {
		t.Fatalf("error %q does not name 'create bucket'", err)
	}
	if strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("error %q is the generic unknown-command message; the rename must be loud", err)
	}
}

// TestBucketTypeListHasOneSource is the drift guard: the store-type table is
// the single source for the unknown-type error, the completion vocabulary and
// the help topics, so adding a seventh type without wiring it up fails here.
func TestBucketTypeListHasOneSource(t *testing.T) {
	keywords := storeTypeKeywords()
	if len(keywords) != 6 {
		t.Fatalf("storeTypeKeywords = %v, want the six store types", keywords)
	}

	// The unknown-type error lists exactly the table's keywords.
	err := unknownBucketTypeError("graph")
	for _, kw := range keywords {
		if !strings.Contains(err.Error(), kw) {
			t.Fatalf("unknown-type error %q does not list %q", err, kw)
		}
	}

	// Every keyword is a completion word and a help topic.
	for _, kw := range keywords {
		if !listContains(completionWords, kw) {
			t.Fatalf("keyword %q is missing from completionWords", kw)
		}
		if !listContains(completionTopics, kw) {
			t.Fatalf("keyword %q is missing from completionTopics", kw)
		}
	}

	// The completion vocabulary and help topics share the type list: the
	// subsets that are store types are exactly the table's keywords.
	completionTypes := intersect(completionWords, keywords)
	if !reflect.DeepEqual(completionTypes, keywords) {
		t.Fatalf("completion store-type words = %v, want %v", completionTypes, keywords)
	}
	topicTypes := intersect(completionTopics, keywords)
	if !reflect.DeepEqual(topicTypes, keywords) {
		t.Fatalf("help topic store-type words = %v, want %v", topicTypes, keywords)
	}
}

// TestCreateDatabaseHintOrder pins the hint verbatim: use comes before the
// bucket line because a fresh database does not become the session's database.
//
// The hint carries both vocabularies because both are real and they are not
// interchangeable: `type: lifo` names the bucket's engine, `Photos lifoStore`
// declares a per-document store field. A hint that showed only the first led
// straight to `Photos lifo`, which the declaration parser refuses.
func TestCreateDatabaseHintOrder(t *testing.T) {
	var out strings.Builder
	h := &CommandHandler{terminal: &out}
	h.printCreateDatabaseHint("test")

	want := "Next: use test\n" +
		"Then: create bucket <name> {type: hash | btree | fifo | lifo | heap | blob}   (hash is the default)\n" +
		"     create bucket Post {Photos lifoStore, Comments fifoStore}\n" +
		"     a store field is <name> <type>Store — hashStore, btreeStore, fifoStore, lifoStore, heapStore, blobStore\n"
	if got := out.String(); got != want {
		t.Fatalf("hint output:\n%q\nwant:\n%q", got, want)
	}
}

// TestCreateDatabaseHintNamesBothVocabularies is the property behind the
// pinned string: every type in the table appears in the hint twice, once as a
// `type:` value and once as a `<type>Store` field, and the two spellings never
// cross over. It is the drift guard — a seventh store type that reaches the
// table but not the hint fails here rather than in a developer's terminal.
func TestCreateDatabaseHintNamesBothVocabularies(t *testing.T) {
	var out strings.Builder
	h := &CommandHandler{terminal: &out}
	h.printCreateDatabaseHint("test")
	hint := out.String()

	for _, st := range storeTypes {
		if !strings.Contains(hint, st.Keyword) {
			t.Errorf("the hint never names the type %q, so a developer cannot write `type: %s`", st.Keyword, st.Keyword)
		}
		if !strings.Contains(hint, st.Keyword+"Store") {
			t.Errorf("the hint never names the store field %qStore, so a developer cannot declare one", st.Keyword)
		}
		// The bare keyword must not appear glued to "Store": the parser reads
		// `type: fifoStore` as an unknown bucket type and refuses it.
		if strings.Contains(hint, "type: "+st.Keyword+"Store") {
			t.Errorf("the hint shows `type: %sStore`, which the parser refuses; the Store spelling is a field, not a type", st.Keyword)
		}
	}
}

// TestEveryStoreTypeIsUsableAsAField guards the rule the hint teaches: for
// every type in the table, `Name <keyword>Store` parses as a store field
// declaration. A type in the table that no declaration can name is a type the
// hint would be lying about.
func TestEveryStoreTypeIsUsableAsAField(t *testing.T) {
	for _, st := range storeTypes {
		line := "create bucket Post { Photos " + st.Keyword + "Store }"
		cmd, err := ParseCommand(line)
		if err != nil {
			t.Errorf("ParseCommand(%q): %v", line, err)
			continue
		}
		schema, err := parseBucketSchema(cmd, "app")
		if err != nil {
			t.Errorf("parseBucketSchema(%q): %v", line, err)
			continue
		}
		stores, _ := schema.Options["stores"].([]interface{})
		if len(stores) != 1 {
			t.Errorf("%q declared %d stores, want 1", line, len(stores))
			continue
		}
		field, _ := stores[0].(map[string]interface{})
		if field["engine"] != st.Keyword+"Store" {
			t.Errorf("%q declared engine %v, want %q", line, field["engine"], st.Keyword+"Store")
		}
	}
}

// TestBareTypeKeywordIsNotAStoreField pins the confusion the hint exists to
// prevent: `Photos lifo` names no engine the parser accepts, and the refusal
// must say so in terms a developer can act on.
//
// The refusal comes from the declaration parser (parseBucketSchema), not from
// ParseCommand, which only reads the command's shape — so the assertion runs
// the path a real `create bucket` takes.
func TestBareTypeKeywordIsNotAStoreField(t *testing.T) {
	cmd, err := ParseCommand("create bucket Post { Photos lifo }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	_, err = parseBucketSchema(cmd, "app")
	if err == nil {
		t.Fatal("`Photos lifo` was accepted; it must be refused, the engine name is lifoStore")
	}
	if !strings.Contains(err.Error(), "lifoStore") {
		t.Fatalf("refusal %q does not name the spelling the developer needs (lifoStore)", err)
	}
}

// TestStoreSuffixIsNotABucketType is the mirror image, and the reason the hint
// shows two vocabularies rather than one: `type: fifoStore` is refused as an
// unknown bucket type, so the Store spelling cannot quietly replace the type
// list everywhere.
func TestStoreSuffixIsNotABucketType(t *testing.T) {
	cmd, err := ParseCommand("create bucket Post { type: fifoStore }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	if _, err := parseBucketSchema(cmd, "app"); err == nil {
		t.Fatal("`type: fifoStore` was accepted; the Store spelling names a field, not a bucket type")
	}
}

// intersect returns the elements of left that also appear in right, in left's
// order.
func intersect(left, right []string) []string {
	rightSet := make(map[string]bool, len(right))
	for _, word := range right {
		rightSet[word] = true
	}
	var out []string
	for _, word := range left {
		if rightSet[word] {
			out = append(out, word)
		}
	}
	return out
}

// listContains reports whether list holds want.
func listContains(list []string, want string) bool {
	for _, word := range list {
		if word == want {
			return true
		}
	}
	return false
}

// TestParseBucketSchemaLiftsReplicationFactor asserts the factor arrives as a
// typed value and leaves the options map, exactly as `type` already does. An
// unknown key left in Options travels to the server inside the opaque map whose
// contract is "ignore what you do not know" — which is the dropped-option
// failure this whole feature exists to remove.
//
// Ported from a4d11c5, re-aimed: parseBucketSchema takes the database name and
// returns bucketSchema with Scalars on main; the body is also exercised in the
// declaration form (a body that is not a document), which main added.
func TestParseBucketSchemaLiftsReplicationFactor(t *testing.T) {
	for _, line := range []string{
		"create bucket Sessions { type: hash, replication_factor: 3 }",
		"create bucket Sessions { replication_factor: 3, type: hash }",
		"create bucket Sessions { Notes fifoStore, replication_factor: 3 }",
	} {
		t.Run(line, func(t *testing.T) {
			cmd, err := ParseCommand(line)
			if err != nil {
				t.Fatalf("ParseCommand: %v", err)
			}
			schema, err := parseBucketSchema(cmd, "app")
			if err != nil {
				t.Fatalf("parseBucketSchema: %v", err)
			}
			if schema.ReplicationFactor != 3 {
				t.Fatalf("ReplicationFactor = %d, want 3", schema.ReplicationFactor)
			}
			if _, stillThere := schema.Options["replication_factor"]; stillThere {
				t.Fatalf("replication_factor was left in the options map: %v", schema.Options)
			}
		})
	}

	// Omitted means inherit, and inherit is zero.
	plain, err := ParseCommand("create bucket Sessions { type: hash }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	plainSchema, err := parseBucketSchema(plain, "app")
	if err != nil {
		t.Fatalf("parseBucketSchema: %v", err)
	}
	if plainSchema.ReplicationFactor != 0 {
		t.Fatalf("an omitted factor parsed as %d, want 0", plainSchema.ReplicationFactor)
	}
}

// TestParseBucketSchemaRefusesANonNumericReplicationFactor keeps the refusal at
// the CLI, where the message can name the form that works, instead of letting a
// string reach the server as an option nobody reads.
//
// Ported from a4d11c5, re-aimed at the new return shape.
func TestParseBucketSchemaRefusesANonNumericReplicationFactor(t *testing.T) {
	cases := []string{
		`create bucket Sessions { type: hash, replication_factor: "three" }`,
		"create bucket Sessions { type: hash, replication_factor: -1 }",
		"create bucket Sessions { type: hash, replication_factor: 0 }",
		"create bucket Sessions { type: hash, replication_factor: 2.5 }",
	}
	for _, line := range cases {
		t.Run(line, func(t *testing.T) {
			cmd, err := ParseCommand(line)
			if err != nil {
				t.Fatalf("ParseCommand(%q): %v", line, err)
			}
			_, err = parseBucketSchema(cmd, "app")
			if err == nil {
				t.Fatalf("parseBucketSchema(%q) accepted it", line)
			}
			if !strings.Contains(err.Error(), "replication_factor: 3") {
				t.Fatalf("the refusal does not name the working form: %v", err)
			}
		})
	}
}

// TestCreateBucketSendsItsReplicationFactorToTheServer: the factor lifted out
// of the body must reach the server, or `create bucket X { replication_factor:
// 3 }` would be silently accepted as a plain bucket. A standalone server
// refuses a factor above one and says why, which is how the arrival shows; and
// a create of a name that exists says so, with what the server kept, instead of
// printing "created".
func TestCreateBucketSendsItsReplicationFactorToTheServer(t *testing.T) {
	c := newDumpTestServer(t)
	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}

	cmd, err := ParseCommand("create bucket Sessions in app { type: hash, replication_factor: 3 }")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	err = c.handler.handleCreateBucket(cmd)
	if err == nil || !strings.Contains(err.Error(), "standalone") || !strings.Contains(err.Error(), "replication_factor 3") {
		t.Fatalf("a standalone server was asked for 3 copies and answered %v; want the refusal that names the fix", err)
	}

	plain, _ := ParseCommand("create bucket Sessions in app { type: hash }")
	if err := c.handler.handleCreateBucket(plain); err != nil {
		t.Fatalf("plain create: %v", err)
	}
	terminal.Reset()
	one, _ := ParseCommand("create bucket Sessions in app { type: hash, replication_factor: 1 }")
	if err := c.handler.handleCreateBucket(one); err != nil {
		t.Fatalf("repeated create: %v", err)
	}
	printed := terminal.String()
	// A standalone server can be asked for one copy only, which is what this
	// bucket already has, so the reply is the ordinary "already exists": the note
	// that a different factor was not applied is the server tests' (a factor
	// above one is refused here before the bucket is looked at). What matters
	// here is that the CLI does not say "created".
	if strings.Contains(printed, "created bucket") || !strings.Contains(printed, "already exists") {
		t.Fatalf("a create of an existing bucket printed %q", printed)
	}
}
