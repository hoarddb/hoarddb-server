package cli

import (
	"errors"
	"strings"
	"testing"
)

// The CLI's batch statement (batch-request spec §8.3, T-D3).

const batchSetup = `create database social
use social
create bucket User_7 { Feed fifoStore, Posts hashStore }
create bucket User_42 { Feed fifoStore }
db.User_7.insert({_id_: "7", posts_n: 0})
db.User_42.insert({_id_: "42"})
`

func TestParseBatchSplitsStatements(t *testing.T) {
	cmd, err := ParseCommand(`batch {
  db.User_7["7"].Posts.put("p-1001", {text: "a; b", tags: ["x", "y"]});
  db.User_42["42"].Feed.push({author: "7"});
  db.User_7.update("7", {$inc: {posts_n: 1}})
}`)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Method != "batch" || len(cmd.Args) != 3 {
		t.Fatalf("parsed %+v; want a batch of 3 statements", cmd)
	}
	if !strings.Contains(cmd.Args[0], `"a; b"`) {
		t.Errorf("a semicolon inside a string split the statement: %q", cmd.Args[0])
	}
	if _, err := ParseCommand("batch{db.User_42[\"42\"].Feed.len()}"); err != nil {
		t.Errorf("batch with no space before the brace: %v", err)
	}
	for _, refused := range []string{"batch { }", "batch", "batch db.X.len()"} {
		if _, err := ParseCommand(refused); err == nil {
			t.Errorf("%q parsed; it carries no statement", refused)
		}
	}
}

// TestBatchStatementPrintsTheLoneForms runs a batch whose third statement
// names a missing bucket: the table shows OK for the others, the lone error
// for it, and the summary counts it not applied; the run ends partial.
func TestBatchStatementPrintsTheLoneForms(t *testing.T) {
	script := batchSetup + `batch {
  db.User_7["7"].Posts.put("p-1001", {text: "hello"});
  db.User_42["42"].Feed.push({author: "7", post: "p-1001", text: "hello"});
  db.User_43["43"].Feed.push({author: "7", post: "p-1001", text: "hello"});
  db.User_7.update("7", {$inc: {posts_n: 1}});
  db.User_42["42"].Feed.len()
}
`
	transcript, runErr := runScript(t, script)
	var outcome *BatchOutcomeError
	if !errors.As(runErr, &outcome) || outcome.Exit != ExitPartial {
		t.Fatalf("Run returned %v; want a partial batch (exit %d)\n%s", runErr, ExitPartial, transcript)
	}
	for _, want := range []string{
		` #  statement`,
		`1  db.User_7["7"].Posts.put("p-1001", …)`,
		`2  db.User_42["42"].Feed.push(…)`,
		`ERROR: bucket not found: bucket "User_43" not found in database "social". Run 'show buckets in social'`,
		`4  db.User_7.update("7", …)`,
		`5  db.User_42["42"].Feed.len()`,
		"5 statements: 4 succeeded, 1 not applied (nothing stored for it; safe to resend). Statements in one batch are not applied together.",
	} {
		if !strings.Contains(transcript, want) {
			t.Errorf("the transcript lacks %q:\n%s", want, transcript)
		}
	}
	if !strings.Contains(transcript, "                OK") && !strings.Contains(transcript, "  OK") {
		t.Errorf("no OK result column:\n%s", transcript)
	}
}

// TestBatchStatementCountsAVerbFailure: an update of a missing key is the
// verb's own success false, counted as refused by the store, not succeeded.
func TestBatchStatementCountsAVerbFailure(t *testing.T) {
	transcript, runErr := runScript(t, batchSetup+`batch { db.User_7.update("ghost", {$inc: {posts_n: 1}}); db.User_42["42"].Feed.push({a: 1}) }
`)
	var outcome *BatchOutcomeError
	if !errors.As(runErr, &outcome) || outcome.Exit != ExitPartial {
		t.Fatalf("Run returned %v; want partial\n%s", runErr, transcript)
	}
	if !strings.Contains(transcript, "2 statements: 1 succeeded, 1 refused by the store") {
		t.Errorf("the summary does not count the update of a missing key as a failure:\n%s", transcript)
	}
}

// TestBatchStatementRefusesWhatABatchDoesNotCarry: a pop is refused before
// anything is sent, in the server's own words.
func TestBatchStatementRefusesWhatABatchDoesNotCarry(t *testing.T) {
	transcript, runErr := runScript(t, batchSetup+`batch { db.User_42["42"].Feed.push({a: 1}); db.User_42["42"].Feed.pop() }
db.User_42["42"].Feed.len()
`)
	var outcome *BatchOutcomeError
	if !errors.As(runErr, &outcome) || outcome.Exit != ExitRefused {
		t.Fatalf("Run returned %v; want the batch refused whole before sending (exit %d)\n%s", runErr, ExitRefused, transcript)
	}
	if !strings.Contains(runErr.Error(), `Item 2 of 2, db.User_42["42"].Feed.pop(), is a pop, which a batch does not carry`) {
		t.Errorf("the refusal is not the server's text: %v", runErr)
	}
	if strings.Contains(transcript, "statements:") {
		t.Errorf("a refused batch printed a table:\n%s", transcript)
	}
}

// TestBatchStatementRefusedWholeExitsRefused: an item the server's door
// refuses (here, the depth scan) refuses the batch whole: exit 2.
func TestBatchStatementRefusedWholeExitsRefused(t *testing.T) {
	deep := strings.Repeat("{a: ", 110) + "1" + strings.Repeat("}", 110)
	transcript, runErr := runScript(t, batchSetup+`batch { db.User_42["42"].Feed.push({a: 1}); db.User_7["7"].Feed.push(`+deep+`) }
`)
	var outcome *BatchOutcomeError
	if !errors.As(runErr, &outcome) || outcome.Exit != ExitRefused {
		t.Fatalf("Run returned %v; want the batch refused whole (exit %d)\n%s", runErr, ExitRefused, transcript)
	}
	if !strings.Contains(runErr.Error(), "Nothing in the batch was run.") {
		t.Errorf("the refusal does not say nothing ran: %v", runErr)
	}
	if info := classify(runErr); info.Exit != ExitRefused || info.Code != "INVALID_ARGUMENT" {
		t.Errorf("classified %+v; want exit 2 with the server's code", info)
	}
}

// TestBatchSummaryOfReadsOnlyDoesNotWarnAboutApplying: a batch of reads has
// nothing to apply, so the summary leaves out the sentence about writes.
func TestBatchSummaryOfReadsOnlyDoesNotWarnAboutApplying(t *testing.T) {
	transcript, runErr := runScript(t, batchSetup+`batch { db.User_42["42"].Feed.len(); db.User_7["7"].Posts.get("none") }
`)
	if runErr != nil {
		t.Fatalf("Run returned %v\n%s", runErr, transcript)
	}
	if !strings.Contains(transcript, "2 statements: 2 succeeded.") {
		t.Errorf("the summary line is not the read-only one:\n%s", transcript)
	}
	if strings.Contains(transcript, "not applied together") {
		t.Errorf("a read-only batch warned about writes:\n%s", transcript)
	}
}
