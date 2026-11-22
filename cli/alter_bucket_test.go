package cli

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// alterStub is a connected CLI on a stub that answers every opcode an alter
// can send. The server's own answers (spec 4.2) are scripted, because the
// dispatch itself needs no server (build doc G47-11).
func alterStub(t *testing.T, alterReply interface{}) (*stubServer, *CLI, func() string) {
	t.Helper()
	stub, c, out := newDeclarationStub(t, map[transport.Opcode]interface{}{
		transport.OpAlterBucket:        alterReply,
		transport.OpDeclareStoreFields: server.DeclareStoreFieldsResponse{Success: true, Message: "declared"},
	})
	return stub, c, out.String
}

// TestAlterBucketAcceptsOnlyReplicationFactor is spec 4.1's dispatch table:
// one option key, replication_factor, goes to OpAlterBucket (0x8E); every other
// body is refused or routed as before, and none but that one reaches 0x8E.
func TestAlterBucketAcceptsOnlyReplicationFactor(t *testing.T) {
	cases := []struct {
		name        string
		statement   string
		wantOpcode  transport.Opcode // the one opcode the statement may send; zero means none
		wantRefusal []string         // fragments the refusal must contain; nil means accepted
	}{
		{"the factor goes to 0x8E", `alter bucket Sessions { replication_factor: 5 }`, transport.OpAlterBucket, nil},
		{"type has its own refusal", `alter bucket Sessions { type: hash }`, 0,
			[]string{`bucket "Sessions" keeps its store type`, "is not available yet", "create a bucket of the new type"}},
		{"placement is fixed at create", `alter bucket Sessions { placement: whole }`, 0,
			[]string{`bucket "Sessions" keeps its placement`, "fixed when the bucket is created", "dump --out ./dump-Sessions", "create bucket Sessions", "{ placement: whole }", "restore --dir ./dump-Sessions"}},
		{"type in the two-word form too", `alter bucket Sessions { type hash }`, 0, []string{"keeps its store type"}},
		{"any other key is errAlterBucket", `alter bucket Sessions { indices: ["Email"] }`, 0, []string{errAlterBucket("Sessions").Error()}},
		{"a store field body is a declare", `alter bucket Sessions { Tags hashStore }`, transport.OpDeclareStoreFields, nil},
		{"a mix names both halves", `alter bucket Sessions { Notes hashStore, replication_factor: 3 }`, 0,
			[]string{"Notes", "replication_factor"}},
		{"two option keys name a statement each", `alter bucket Sessions { replication_factor: 3, type: hash }`, 0,
			[]string{"alter bucket Sessions { replication_factor: 3 }", "alter bucket Sessions { type: hash }"}},
		{"a fraction is refused before it is sent", `alter bucket Sessions { replication_factor: 2.5 }`, 0, []string{"replication_factor", "2.5"}},
		{"zero is refused before it is sent", `alter bucket Sessions { replication_factor: 0 }`, 0, []string{"replication_factor", "1 or more"}},
		{"a string is refused before it is sent", `alter bucket Sessions { replication_factor: "three" }`, 0, []string{"replication_factor", "three"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub, c, _ := alterStub(t, server.AlterBucketResponse{Success: true, Message: "ok", ReplicationFactor: 5, PreviousFactor: 3})
			err := c.handle(tc.statement)
			switch {
			case tc.wantRefusal == nil && err != nil:
				t.Fatalf("%s = %v; want it accepted", tc.statement, err)
			case tc.wantRefusal != nil && err == nil:
				t.Fatalf("%s was accepted; want a refusal", tc.statement)
			}
			for _, fragment := range tc.wantRefusal {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("%s refused with %q; want it to contain %q", tc.statement, err, fragment)
				}
			}
			sent := stub.sequence()
			wantSent := 0
			if tc.wantOpcode != 0 {
				wantSent = 1
			}
			if len(sent) != wantSent || (wantSent == 1 && sent[0] != tc.wantOpcode) {
				t.Fatalf("%s sent %v; want exactly [%v]", tc.statement, sent, tc.wantOpcode)
			}
		})
	}
}

// TestAlterBucketSendsTheFactorForTheCurrentDatabase pins the frame itself:
// the database, the bucket and the factor are the three things the owner reads.
func TestAlterBucketSendsTheFactorForTheCurrentDatabase(t *testing.T) {
	stub, c, _ := alterStub(t, server.AlterBucketResponse{Success: true, Message: "ok", ReplicationFactor: 5, PreviousFactor: 3})
	if err := c.handle(`alter bucket Sessions { replication_factor: 5 }`); err != nil {
		t.Fatalf("the alter failed: %v", err)
	}
	payloads := stub.payloadsOf(transport.OpAlterBucket)
	if len(payloads) != 1 {
		t.Fatalf("the stub saw %d alter frames; want 1", len(payloads))
	}
	var sentRequest server.AlterBucketRequest
	if err := bson.Unmarshal(payloads[0], &sentRequest); err != nil {
		t.Fatalf("decoding the frame: %v", err)
	}
	if sentRequest.Database != "app" || sentRequest.Name != "Sessions" || sentRequest.ReplicationFactor != 5 {
		t.Fatalf("the frame carried %+v; want app, Sessions, factor 5", sentRequest)
	}
	if sentRequest.PreviousFactor != 0 || sentRequest.Epoch != 0 || sentRequest.Generation != 0 {
		t.Fatalf("the frame carried the owner's own fields %+v; a client sets only database, name and factor", sentRequest)
	}
}

// TestAlterBucketRefusalNamesTheStatementsThatWork: a two-key body is refused,
// and the refusal says what to type instead, one statement per key.
func TestAlterBucketRefusalNamesTheStatementsThatWork(t *testing.T) {
	_, c, _ := alterStub(t, server.AlterBucketResponse{Success: true})
	err := c.handle(`alter bucket Sessions { replication_factor: 3, indices: ["Email"] }`)
	if err == nil {
		t.Fatal("a two-key alter was accepted")
	}
	for _, want := range []string{
		"alter bucket Sessions { replication_factor: 3 }",
		`alter bucket Sessions { indices: ["Email"] }`,
		"one change per statement",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
	if info := classify(err); info.Exit != ExitUsage {
		t.Errorf("the refusal exits %d; want %d (usage)", info.Exit, ExitUsage)
	}
}

// TestAlterBucketPrintsTheFourAnswers: spec 4.2's four answers each print the
// server's sentence after OK, and a refusal in the body prints nothing as
// success. The frames are stubs; the server texts they carry are the ones
// server/bucket_rf_alter.go builds, pinned against a real cluster in package server.
func TestAlterBucketPrintsTheFourAnswers(t *testing.T) {
	cases := []struct {
		name    string
		answer  server.AlterBucketResponse
		wantOut string
	}{
		{"raise", server.AlterBucketResponse{Success: true, ReplicationFactor: 5, PreviousFactor: 3, Pending: []string{"node-d", "node-e"},
			Message: `bucket "Sessions" now asks for 5 copies (was 3). node-d and node-e will copy it; writes keep going to the 3 copies that hold it.`},
			`OK  bucket "Sessions" now asks for 5 copies (was 3). node-d and node-e will copy it`},
		{"decrease", server.AlterBucketResponse{Success: true, ReplicationFactor: 2, PreviousFactor: 3, Releasing: []string{"node-c"},
			Message: `bucket "Sessions" now asks for 2 copies (was 3). node-c's copy will be deleted once node-a and node-b are shown to hold everything it holds.`},
			`OK  bucket "Sessions" now asks for 2 copies (was 3). node-c's copy will be deleted`},
		{"declaration", server.AlterBucketResponse{Success: true, ReplicationFactor: 3, PreviousFactor: 3,
			Message: `bucket "Sessions" now declares 3 copies (it inherited 3 from this node's default); a later change of the default no longer changes it.`},
			`OK  bucket "Sessions" now declares 3 copies`},
		{"no change", server.AlterBucketResponse{Success: true, ReplicationFactor: 3, PreviousFactor: 3,
			Message: `bucket "Sessions" already asks for 3 copies; nothing changed.`},
			`OK  bucket "Sessions" already asks for 3 copies; nothing changed.`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, printed := alterStub(t, tc.answer)
			if err := c.handle(`alter bucket Sessions { replication_factor: 3 }`); err != nil {
				t.Fatalf("the alter failed: %v", err)
			}
			if !strings.Contains(printed(), tc.wantOut) {
				t.Fatalf("printed %q; want it to contain %q", printed(), tc.wantOut)
			}
		})
	}

	t.Run("a refusal in the body is a refusal", func(t *testing.T) {
		_, c, printed := alterStub(t, server.AlterBucketResponse{Success: false, Message: `bucket "Sessions" is being changed; try again`})
		err := c.handle(`alter bucket Sessions { replication_factor: 3 }`)
		if err == nil || !strings.Contains(err.Error(), "is being changed") {
			t.Fatalf("alter = %v; want the server's refusal text", err)
		}
		if strings.Contains(printed(), "OK") {
			t.Fatalf("printed %q for a refusal", printed())
		}
	})
}

// TestAlterBucketLowerFailureNamesTheSurvivor: R11's refusal arrives as an
// UNAVAILABLE error frame. Its text, which names the survivor, the copy it must
// show, and the retry, reaches the operator unchanged, and the exit code says
// "transient" (8), not "refused" (2).
//
// STUBBED, on purpose: the frame is a stub's, carrying the exact text of
// server/bucket_rf_alter.go:decreaseNotCovered. A real refusal needs a
// cluster whose survivor has not shown coverage, and that harness
// (server.newTestCluster, fault hooks such as failNextApplyOn) is unexported
// test code of package server that a cli test cannot import; rebuilding it here
// would copy the cluster token, TLS pinning and gossip wiring. The text itself
// is pinned against a real cluster by server's
// TestAnAlterIsRefusedAtThePlaintextMemberItWouldPlaceTheBucketOn (R11 row)
// and TestADecreaseIsRefusedUntilTheSurvivorsAreShownToCoverIt.
func TestAlterBucketLowerFailureNamesTheSurvivor(t *testing.T) {
	cases := []struct {
		name    string
		refusal string
		want    []string
	}{
		{"the survivor is the owner",
			`bucket "Sessions" cannot be lowered from replication_factor 3 to 2: node-a has not shown that it holds everything node-c holds ` +
				`(its copy is behind, or it did not answer). Run 'show buckets' on node-a and retry when the row reads 3/3.`,
			[]string{"cannot be lowered from replication_factor", "node-a", "Run 'show buckets'", "3/3"}},
		{"the survivor is another member",
			`bucket "Sessions" cannot be lowered from replication_factor 3 to 2: node-b has not shown that it holds everything node-c holds ` +
				`(its copy is behind, or it did not answer). Run 'show buckets' on node-a and retry when the row reads 3/3. ` +
				`If node-b is not coming back, run 'remove-node node-b' first.`,
			[]string{"cannot be lowered from replication_factor", "node-b", "Run 'show buckets'", "remove-node node-b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, _ := newDeclarationStub(t, map[transport.Opcode]interface{}{
				transport.OpAlterBucket: errorFrame(t, transport.ErrUnavailable, tc.refusal),
			})
			err := c.handle(`alter bucket Sessions { replication_factor: 2 }`)
			if err == nil {
				t.Fatal("the lower was accepted")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not contain %q", err, want)
				}
			}
			info := classify(err)
			if info.Exit != ExitUnavailable || info.Code != "UNAVAILABLE" {
				t.Fatalf("classify = %q exit %d; want UNAVAILABLE exit %d", info.Code, info.Exit, ExitUnavailable)
			}
		})
	}
}

// TestTheAlterRaiseHelpExampleParses reads the raise example out of the help
// text itself and runs it: it must parse, and it must reach OpAlterBucket. A
// wrong example in the help fails here, which TestHelpStatementExamplesParse
// alone would not catch if the example were dropped.
func TestTheAlterRaiseHelpExampleParses(t *testing.T) {
	var example string
	for _, statement := range helpStatements(allHelpText(t)["help"]) {
		if strings.HasPrefix(statement, "alter bucket") && strings.Contains(statement, "replication_factor: 5") {
			example = statement
		}
	}
	if example == "" {
		t.Fatal("the help prints no `alter bucket … { replication_factor: N }` raise example")
	}
	stub, c, _ := alterStub(t, server.AlterBucketResponse{Success: true, Message: "ok", ReplicationFactor: 5, PreviousFactor: 3})
	if err := c.handle(example); err != nil {
		t.Fatalf("the help's example %q = %v; want it accepted", example, err)
	}
	if sent := stub.sequence(); len(sent) != 1 || sent[0] != transport.OpAlterBucket {
		t.Fatalf("the help's example %q sent %v; want exactly [OpAlterBucket]", example, sent)
	}
}

// TestTheAlterLowerHelpExampleParses reads the lowering example out of the help
// text itself and parses it. A wrong example in the help fails here, which
// TestHelpStatementExamplesParse alone would not catch if the example were
// dropped. It is parse-only: the lowering's server answers are covered by
// TestAlterBucketPrintsTheFourAnswers and the server's own decrease tests.
func TestTheAlterLowerHelpExampleParses(t *testing.T) {
	var example string
	for _, statement := range helpStatements(allHelpText(t)["help"]) {
		if strings.HasPrefix(statement, "alter bucket") && strings.Contains(statement, "replication_factor: 2") {
			example = statement
		}
	}
	if example == "" {
		t.Fatal("the help prints no lowering `alter bucket … { replication_factor: N }` example")
	}
	cmd, err := ParseCommand(example)
	if err != nil {
		t.Fatalf("the help's example %q = %v; want it to parse", example, err)
	}
	if cmd.Method != "alter_bucket" {
		t.Fatalf("the help's example %q parses to %q; want alter_bucket", example, cmd.Method)
	}
}

// TestAlterAddOfIsRefusedWithTheFix: [GUARD a reference cannot be added by alter, and the
// refusal says where it is declared].
func TestAlterAddOfIsRefusedWithTheFix(t *testing.T) {
	_, err := Parse("alter bucket Room { add Members hashStore of User }", "app")
	if err == nil || !strings.Contains(err.Error(), "alter cannot declare a reference yet") || !strings.Contains(err.Error(), "create bucket") {
		t.Fatalf("Parse = %v, want the refusal with the fix", err)
	}
	// The fix is written in the statement's own names, not a canned example.
	_, err = Parse("alter bucket Order in shop { add Buyer string of crm.Customer }", "app")
	if err == nil || !strings.Contains(err.Error(), "Buyer string of crm.Customer") || strings.Contains(err.Error(), "Members") || strings.Contains(err.Error(), "Room") {
		t.Fatalf("Parse = %v, want the fix in the user's names (Buyer string of crm.Customer)", err)
	}
}
