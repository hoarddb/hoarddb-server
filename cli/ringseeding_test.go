package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestSeedingStatementsAreAvailableInRingMode: the three seeding statements run on a cluster
// node since S4, so the grammar does not mark them and the help pages do not say otherwise.
// [BREAK: leave Unavailable: true on one of the three forms in ringgrammar.go]
func TestSeedingStatementsAreAvailableInRingMode(t *testing.T) {
	for _, verb := range []string{"status seeding", "seeding retry", "seeding forget"} {
		found := false
		for _, form := range ringForms {
			if form.Verb == verb {
				found = true
				if form.Unavailable {
					t.Errorf("%q is still marked unavailable", verb)
				}
			}
		}
		if !found {
			t.Errorf("no form %q", verb)
		}
	}
	runnable := strings.SplitN(HelpAll(), "Not available yet", 2)[0]
	for _, syntax := range []string{"status seeding", "seeding retry <db> <slot>", "seeding forget <db> <slot>"} {
		if !strings.Contains(runnable, syntax) {
			t.Errorf("help all does not list %q among the runnable statements", syntax)
		}
	}
	page, err := HelpFor("seeding")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(page, "Not available yet") {
		t.Errorf("the seeding help page still says it is not available yet:\n%s", page)
	}
	if got := CompleteWith(u51Source{}, "seeding ", "app"); !u51Contains(got, "retry") || !u51Contains(got, "forget") {
		t.Errorf("completion of \"seeding \" offers %v, want retry and forget", got)
	}
	session, fake := u52Session()
	fake.seeding = []ringclient.SeedingRow{{Database: "app", Slot: 7, State: "retired"}}
	output, err := session.Execute(context.Background(), "status seeding")
	if err != nil {
		t.Fatalf("status seeding: %v", err)
	}
	if len(output.Lines) == 0 || output.Lines[0] != "SEEDING" {
		t.Errorf("status seeding printed %q, want a SEEDING title", output.Lines)
	}
}

// TestARetiredRowNamesWhatTheGateWaitsFor: a retired row with a refusal prints it in
// parentheses and still names the forget statement; without one the line is the plain one.
// [BREAK: ignore row.Refusal in the retired case of ringSeedingExplanation]
func TestARetiredRowNamesWhatTheGateWaitsFor(t *testing.T) {
	waiting := ringclient.SeedingRow{Database: "app", Slot: 7, State: "retired", Refusal: "waiting for node-c to confirm it holds this slot."}
	got := ringSeedingExplanation(waiting)
	want := "RETIRED  app slot 7 (0x0007): its range is kept until the deletion gate passes (waiting for node-c to confirm it holds this slot). " +
		"If no other copy will ever confirm it, run 'seeding forget app 7' to drop the record; the data goes on the next seed"
	if got != want {
		t.Errorf("with a reason:\n got %q\nwant %q", got, want)
	}
	waiting.Refusal = ""
	got = ringSeedingExplanation(waiting)
	want = "RETIRED  app slot 7 (0x0007): its range is kept until the deletion gate passes. " +
		"If no other copy will ever confirm it, run 'seeding forget app 7' to drop the record; the data goes on the next seed"
	if got != want {
		t.Errorf("without a reason:\n got %q\nwant %q", got, want)
	}
}
