package main

import (
	"bytes"
	"strings"
	"testing"
)

// cleanLedger is two rounds of two writers: five acknowledged writes and two whose answer
// never came (in flight at the kill).
func cleanLedger() []ledgerRow {
	return []ledgerRow{
		{Round: 1, Writer: 0, Seq: 0, ID: "r1-w0-0", Outcome: outcomeAcked},
		{Round: 1, Writer: 0, Seq: 1, ID: "r1-w0-1", Outcome: outcomeAcked},
		{Round: 1, Writer: 1, Seq: 0, ID: "r1-w1-0", Outcome: outcomeAcked},
		{Round: 1, Writer: 1, Seq: 1, ID: "r1-w1-1", Outcome: outcomeUnknown, Detail: "CONN"},
		{Round: 2, Writer: 0, Seq: 0, ID: "r2-w0-0", Outcome: outcomeAcked},
		{Round: 2, Writer: 1, Seq: 0, ID: "r2-w1-0", Outcome: outcomeAcked},
		{Round: 2, Writer: 1, Seq: 1, ID: "r2-w1-1", Outcome: outcomeUnknown, Detail: "TIMEOUT"},
	}
}

// cleanStore is what a store that kept every acknowledged write answers for cleanLedger: one
// unknown write landed (r1-w1-1) and one did not (r2-w1-1).
func cleanStore() observations {
	return observations{
		Gets: map[string]string{
			"r1-w0-0": statePresent, "r1-w0-1": statePresent, "r1-w1-0": statePresent, "r1-w1-1": statePresent,
			"r2-w0-0": statePresent, "r2-w1-0": statePresent, "r2-w1-1": stateAbsent,
		},
		Listed: map[string]int{
			"r1-w0-0": 1, "r1-w0-1": 1, "r1-w1-0": 1, "r1-w1-1": 1, "r2-w0-0": 1, "r2-w1-0": 1,
		},
		ServerCount: 6,
	}
}

func mustJudge(t *testing.T, rows []ledgerRow, seen observations) report {
	t.Helper()
	found, err := judge(rows, seen)
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	return found
}

func TestJudgeCleanLedgerPassesWithTruthfulCounts(t *testing.T) {
	found := mustJudge(t, cleanLedger(), cleanStore())
	if failures := found.failures(); len(failures) != 0 {
		t.Fatalf("a store that kept every acknowledged write must pass, got failures: %v", failures)
	}
	if found.Sent != 7 || found.Acknowledged != 5 || found.Present != 5 || found.Unknown != 2 ||
		found.UnknownPresent != 1 || found.UnknownAbsent != 1 || found.ListedDistinct != 6 {
		t.Fatalf("counts: %+v", found)
	}
	want := []roundCounts{
		{Round: 1, Sent: 4, Acknowledged: 3, Present: 3, Unknown: 1, UnknownPresent: 1},
		{Round: 2, Sent: 3, Acknowledged: 2, Present: 2, Unknown: 1, UnknownAbsent: 1},
	}
	if len(found.Rounds) != len(want) {
		t.Fatalf("rounds: %+v", found.Rounds)
	}
	for i := range want {
		if found.Rounds[i] != want[i] {
			t.Errorf("round %d: got %+v, want %+v", want[i].Round, found.Rounds[i], want[i])
		}
	}
}

func TestJudgeFailsWhenAnAcknowledgedWriteIsGone(t *testing.T) {
	seen := cleanStore()
	seen.Gets["r1-w0-1"] = stateAbsent
	delete(seen.Listed, "r1-w0-1")
	seen.ServerCount = 5
	found := mustJudge(t, cleanLedger(), seen)
	if len(found.Missing) != 1 || found.Missing[0] != "r1-w0-1" {
		t.Fatalf("missing: %v", found.Missing)
	}
	if found.Present != 4 || found.Acknowledged != 5 {
		t.Fatalf("present %d of acknowledged %d, want 4 of 5", found.Present, found.Acknowledged)
	}
	failures := strings.Join(found.failures(), "\n")
	if !strings.Contains(failures, "r1-w0-1") || !strings.Contains(failures, "1 acknowledged write") {
		t.Fatalf("the failure must count and name the missing id, got: %s", failures)
	}
	if found.Rounds[0].Missing != 1 || found.Rounds[1].Missing != 0 {
		t.Fatalf("the missing write belongs to round 1: %+v", found.Rounds)
	}
}

func TestJudgeAnAbsentUnknownWriteIsNotAFailure(t *testing.T) {
	seen := cleanStore()
	seen.Gets["r1-w1-1"] = stateAbsent
	delete(seen.Listed, "r1-w1-1")
	found := mustJudge(t, cleanLedger(), seen)
	if failures := found.failures(); len(failures) != 0 {
		t.Fatalf("an unknown write promised nothing, got failures: %v", failures)
	}
	if found.UnknownAbsent != 2 || found.UnknownPresent != 0 {
		t.Fatalf("unknown present %d absent %d, want 0 and 2", found.UnknownPresent, found.UnknownAbsent)
	}
}

func TestJudgeFailsOnADuplicate(t *testing.T) {
	seen := cleanStore()
	seen.Listed["r2-w0-0"] = 2
	found := mustJudge(t, cleanLedger(), seen)
	if len(found.Duplicates) != 1 || found.Duplicates[0] != "r2-w0-0" || len(found.failures()) != 1 {
		t.Fatalf("duplicates %v, failures %v", found.Duplicates, found.failures())
	}
}

func TestJudgeFailsOnADocumentNeverSent(t *testing.T) {
	seen := cleanStore()
	seen.Listed["r9-w9-9"] = 1
	found := mustJudge(t, cleanLedger(), seen)
	if len(found.NeverSent) != 1 || found.NeverSent[0] != "r9-w9-9" || len(found.failures()) != 1 {
		t.Fatalf("never sent %v, failures %v", found.NeverSent, found.failures())
	}
}

func TestJudgeFailsOnAChangedBody(t *testing.T) {
	seen := cleanStore()
	seen.Gets["r1-w0-0"] = stateMismatch
	found := mustJudge(t, cleanLedger(), seen)
	if len(found.Mismatched) != 1 || found.Present != 4 || len(found.failures()) != 1 {
		t.Fatalf("mismatched %v, present %d, failures %v", found.Mismatched, found.Present, found.failures())
	}
}

// A read that got no conclusive answer is neither present nor missing: the drill must say it
// could not tell, and must not pass.
func TestJudgeFailsOnAnUnreadableOrUnreadID(t *testing.T) {
	seen := cleanStore()
	seen.Gets["r1-w0-0"] = stateUnreadable
	delete(seen.Gets, "r2-w0-0")
	found := mustJudge(t, cleanLedger(), seen)
	if len(found.Unreadable) != 2 || len(found.Missing) != 0 {
		t.Fatalf("unreadable %v, missing %v", found.Unreadable, found.Missing)
	}
	if len(found.failures()) != 1 || !strings.Contains(found.failures()[0], "could not be read") {
		t.Fatalf("failures: %v", found.failures())
	}
}

// Get found it and the bucket scan did not: two read paths disagree about one document.
func TestJudgeFailsWhenTheScanOmitsAnAcknowledgedWrite(t *testing.T) {
	seen := cleanStore()
	delete(seen.Listed, "r1-w0-0")
	found := mustJudge(t, cleanLedger(), seen)
	if len(found.NotListed) != 1 || len(found.failures()) != 1 {
		t.Fatalf("not listed %v, failures %v", found.NotListed, found.failures())
	}
}

func TestJudgeAnEmptyLedgerIsNotAPass(t *testing.T) {
	found := mustJudge(t, nil, observations{Gets: map[string]string{}, Listed: map[string]int{}})
	if len(found.failures()) == 0 {
		t.Fatal("a drill that acknowledged nothing proved nothing and must not pass")
	}
	onlyUnknown := []ledgerRow{{Round: 1, ID: "r1-w0-0", Outcome: outcomeUnknown, Detail: "CONN"}}
	found = mustJudge(t, onlyUnknown, observations{Gets: map[string]string{"r1-w0-0": stateAbsent}, Listed: map[string]int{}})
	if len(found.failures()) == 0 {
		t.Fatal("a ledger with no acknowledged write must not pass")
	}
}

func TestJudgeRefusesALedgerThatNamesAnIDTwice(t *testing.T) {
	rows := append(cleanLedger(), ledgerRow{Round: 2, Writer: 0, Seq: 0, ID: "r1-w0-0", Outcome: outcomeAcked})
	if _, err := judge(rows, cleanStore()); err == nil || !strings.Contains(err.Error(), "r1-w0-0") {
		t.Fatalf("want an error naming the repeated id, got %v", err)
	}
}

// Every acknowledged write is in exactly one of present, missing, mismatched, unreadable.
func TestJudgeCountsAddUp(t *testing.T) {
	seen := cleanStore()
	seen.Gets["r1-w0-0"] = stateMismatch
	seen.Gets["r1-w0-1"] = stateAbsent
	seen.Gets["r2-w0-0"] = stateUnreadable
	found := mustJudge(t, cleanLedger(), seen)
	acknowledged := found.Present + len(found.Missing) + found.AckedMismatched + found.AckedUnreadable
	if acknowledged != found.Acknowledged {
		t.Fatalf("present+missing+mismatched+unreadable = %d, acknowledged = %d", acknowledged, found.Acknowledged)
	}
}

func TestReportPrintsEveryCountAndNeverABarePass(t *testing.T) {
	var out bytes.Buffer
	found := mustJudge(t, cleanLedger(), cleanStore())
	if err := found.print(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"sent                      7", "acknowledged              5", "present                   5",
		"missing                   0", "unknown                   2", "unknown and present       1",
		"unknown and absent        1", "duplicates                0", "never sent                0",
		"changed body              0", "could not be read         0", "RESULT PASS",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the report lacks %q:\n%s", want, text)
		}
	}
	seen := cleanStore()
	seen.Gets["r1-w0-1"] = stateAbsent
	out.Reset()
	found = mustJudge(t, cleanLedger(), seen)
	if err := found.print(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "RESULT FAIL") || strings.Contains(out.String(), "RESULT PASS") ||
		!strings.Contains(out.String(), "missing                   1") {
		t.Fatalf("a lost write must print RESULT FAIL with the count:\n%s", out.String())
	}
}

func TestLedgerAndObservationsRoundTrip(t *testing.T) {
	var ledger bytes.Buffer
	if err := writeLedger(&ledger, cleanLedger(), true); err != nil {
		t.Fatal(err)
	}
	rows, err := readLedger(&ledger)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 7 || rows[3] != cleanLedger()[3] {
		t.Fatalf("ledger round trip: %+v", rows)
	}
	var file bytes.Buffer
	if err := writeObservations(&file, cleanStore()); err != nil {
		t.Fatal(err)
	}
	seen, err := readObservations(&file)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen.Gets) != 7 || len(seen.Listed) != 6 || seen.ServerCount != 6 || seen.Gets["r2-w1-1"] != stateAbsent {
		t.Fatalf("observations round trip: %+v", seen)
	}
}

func TestReadLedgerRefusesAnOutcomeItDoesNotKnow(t *testing.T) {
	text := ledgerHeader + "\n1,0,0,r1-w0-0,probably,\n"
	if _, err := readLedger(strings.NewReader(text)); err == nil || !strings.Contains(err.Error(), "probably") {
		t.Fatalf("want an error naming the outcome, got %v", err)
	}
}

func TestDocumentBodyIsCheckedAgainstItsID(t *testing.T) {
	body := drillBody(2, 3, 41, "r2-w3-41", 64)
	check, pad := body["Check"].(string), body["Pad"].(string)
	if len(pad) != 64 || !bodyMatches("r2-w3-41", check, pad) {
		t.Fatalf("the body the drill writes must match its id: check %q pad %q", check, pad)
	}
	if bodyMatches("r2-w3-42", check, pad) {
		t.Fatal("another id's body must not match")
	}
	if bodyMatches("r2-w3-41", check, pad[:63]+"!") {
		t.Fatal("a changed pad must not match")
	}
}
