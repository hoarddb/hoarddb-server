package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0706: describe, drop and alter bucket parsed `in <db>` without reading it,
// so the form the product prints ran against the session database (or none).

func TestBucketVerbsReadTheInDatabaseClause(t *testing.T) {
	cases := []struct {
		line       string
		wantMethod string
		wantDetail string // alter only: the change text, without the in clause
	}{
		{"describe bucket User_00 in social", "describe", ""},
		{"drop bucket User_00 in social", "drop_bucket", ""},
		{"alter bucket User_00 in social add index Email", "alter_bucket", "add index Email"},
		{"alter bucket User_00 in social { Tags hashStore }", "alter_bucket", "{ Tags hashStore }"},
		{"DESCRIBE BUCKET User_00 IN social", "describe", ""},
	}
	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			cmd, err := ParseCommand(tc.line)
			if err != nil {
				t.Fatalf("%q: %v", tc.line, err)
			}
			if cmd.Method != tc.wantMethod {
				t.Errorf("%q: Method = %q, want %q", tc.line, cmd.Method, tc.wantMethod)
			}
			if cmd.Database != "social" {
				t.Errorf("%q: Database = %q, want social", tc.line, cmd.Database)
			}
			if cmd.Name != "User_00" {
				t.Errorf("%q: Name = %q, want User_00", tc.line, cmd.Name)
			}
			if tc.wantMethod == "alter_bucket" {
				if detail, _ := cmd.Options["detail"].(string); detail != tc.wantDetail {
					t.Errorf("%q: change text = %q, want %q", tc.line, detail, tc.wantDetail)
				}
			}
		})
	}
}

func TestBucketVerbsWithoutAnInClauseKeepTheSessionDatabase(t *testing.T) {
	for _, line := range []string{"describe bucket U", "drop bucket U", "alter bucket U add index x"} {
		cmd, err := ParseCommand(line)
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if cmd.Database != "" {
			t.Errorf("%q: Database = %q, want empty (the session database applies)", line, cmd.Database)
		}
	}
}

func TestBucketVerbsRefuseLeftoverWordsWithAUsageLine(t *testing.T) {
	cases := map[string]string{
		"describe bucket U in":              "usage: describe bucket <name> [in <database>]",
		"describe bucket U social":          "usage: describe bucket <name> [in <database>]",
		"describe bucket U in social extra": "usage: describe bucket <name> [in <database>]",
		"drop bucket U in":                  "usage: drop bucket <name> [in <database>]",
		"drop bucket U social":              "usage: drop bucket <name> [in <database>]",
		"drop bucket U in social now":       "usage: drop bucket <name> [in <database>]",
	}
	for line, want := range cases {
		_, err := ParseCommand(line)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: error = %v, want it to contain %q", line, err, want)
		}
	}
}

func bucketVerbStub(t *testing.T) (*stubServer, *CLI, *bytes.Buffer) {
	t.Helper()
	script := map[transport.Opcode]interface{}{
		transport.OpBucketStats:        server.BucketStatsResponse{Found: true, Database: "social", Bucket: "U", Type: server.BucketTypeHash},
		transport.OpDeleteBucket:       server.DeleteBucketResponse{Success: true, Message: "dropped"},
		transport.OpDeclareStoreFields: server.DeclareStoreFieldsResponse{Success: true, Message: "declared"},
		transport.OpAlterBucket:        server.AlterBucketResponse{Success: true, Message: "altered"},
		transport.OpBucketInfo:         server.BucketInfoResponse{Found: true, Database: "social", Bucket: server.BucketInfo{Name: "U", Type: server.BucketTypeHash}},
	}
	var out bytes.Buffer
	stub, c := newStubServer(t, script, &out)
	stub.sequence()
	out.Reset()
	return stub, c, &out
}

func TestBucketVerbsInDatabaseSendThatDatabase(t *testing.T) {
	decodeStats := func(p []byte) (string, error) {
		var req server.BucketStatsRequest
		err := bson.Unmarshal(p, &req)
		return req.Database, err
	}
	decodeDrop := func(p []byte) (string, error) {
		var req server.DeleteBucketRequest
		err := bson.Unmarshal(p, &req)
		return req.Database, err
	}
	decodeDeclare := func(p []byte) (string, error) {
		var req server.DeclareStoreFieldsRequest
		err := bson.Unmarshal(p, &req)
		return req.Database, err
	}
	decodeAlter := func(p []byte) (string, error) {
		var req server.AlterBucketRequest
		err := bson.Unmarshal(p, &req)
		return req.Database, err
	}
	cases := []struct {
		line   string
		op     transport.Opcode
		decode func(payload []byte) (string, error)
	}{
		{"describe bucket U in social", transport.OpBucketStats, decodeStats},
		{"drop bucket U in social", transport.OpDeleteBucket, decodeDrop},
		{"alter bucket U in social { Tags hashStore }", transport.OpDeclareStoreFields, decodeDeclare},
		{"alter bucket U in social { replication_factor: 2 }", transport.OpAlterBucket, decodeAlter},
	}
	// "other" is a different use in force; "" is no use at all.
	for _, session := range []string{"other", ""} {
		for _, tc := range cases {
			t.Run(tc.line+" with use "+session, func(t *testing.T) {
				stub, c, _ := bucketVerbStub(t)
				c.currentDB = session
				if err := c.handle(tc.line); err != nil {
					t.Fatalf("%q: %v", tc.line, err)
				}
				payloads := stub.payloadsOf(tc.op)
				if len(payloads) == 0 {
					t.Fatalf("%q sent no %s frame", tc.line, tc.op)
				}
				got, err := tc.decode(payloads[len(payloads)-1])
				if err != nil {
					t.Fatalf("decoding the %s request: %v", tc.op, err)
				}
				if got != "social" {
					t.Errorf("%q with use %q sent database %q, want social", tc.line, session, got)
				}
			})
		}
	}
}

func TestBucketVerbsWithNoDatabaseRefuseBeforeSendingAFrame(t *testing.T) {
	for _, line := range []string{
		"describe bucket U",
		"drop bucket U",
		"alter bucket U { Tags hashStore }",
		"alter bucket U { replication_factor: 2 }",
	} {
		t.Run(line, func(t *testing.T) {
			stub, c, _ := bucketVerbStub(t)
			c.currentDB = ""
			err := c.handle(line)
			if err == nil {
				t.Fatalf("%q with no database in force was accepted", line)
			}
			for _, fix := range []string{"use <database>", "in <database>"} {
				if !strings.Contains(err.Error(), fix) {
					t.Errorf("%q: error %q does not name the fix %q", line, err, fix)
				}
			}
			if sent := stub.sequence(); len(sent) != 0 {
				t.Errorf("%q sent %v, want no frame", line, sent)
			}
		})
	}
}

// printedBucketStatement matches a quoted bucket statement inside a message the
// product prints, e.g. 'describe bucket %s in %s'.
var printedBucketStatement = regexp.MustCompile(`'((?:describe|drop|alter) bucket [^']*)'`)

// TestPrintedDescribeHintParses: every 'describe bucket ... in ...' line the
// server or the CLI prints must be a statement the parser accepts with that
// database, so a hint the product prints is a command that works.
func TestPrintedDescribeHintParses(t *testing.T) {
	if cmd, err := ParseCommand("describe bucket Post in app"); err != nil || cmd.Database != "app" {
		t.Fatalf("the hint the server prints does not parse to database app: %+v, %v", cmd, err)
	}

	checked := 0
	for _, dir := range []string{".", "../server"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatalf("listing %s: %v", dir, err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			source, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("reading %s: %v", file, err)
			}
			for _, match := range printedBucketStatement.FindAllStringSubmatch(string(source), -1) {
				if !strings.Contains(match[1], " in ") {
					continue
				}
				// Format verbs fill in as the bucket first, then the database.
				statement := strings.Replace(match[1], "%s", "Post", 1)
				statement = strings.Replace(statement, "%s", "app", 1)
				cmd, err := ParseCommand(statement)
				if err != nil {
					t.Errorf("%s prints %q, which does not parse: %v", file, match[1], err)
					continue
				}
				if cmd.Database != "app" {
					t.Errorf("%s prints %q, which parses with database %q, want app", file, match[1], cmd.Database)
				}
				checked++
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no printed 'describe bucket ... in ...' hint to check; the scan is broken")
	}
}
