package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The T6 tests cover the seeding statements: status seeding, seeding retry and seeding forget.

// SeedingStatus answers the fake's rows, filtered like the server, paged by index.
func (f *u52Fake) SeedingStatus(_ context.Context, query ringclient.SeedingQuery) (ringclient.SeedingPage, error) {
	f.seedingQueries = append(f.seedingQueries, query)
	if err := f.fail("SeedingStatus"); err != nil {
		return ringclient.SeedingPage{}, err
	}
	var rows []ringclient.SeedingRow
	for _, row := range f.seeding {
		if query.Database != "" && row.Database != query.Database || query.Slot != nil && row.Slot != *query.Slot {
			continue
		}
		rows = append(rows, row)
	}
	if f.seedingStuck {
		return ringclient.SeedingPage{Rows: []ringclient.SeedingRow{}, More: true, Cursor: []byte{0, 1}}, nil
	}
	start := 0
	if len(query.Page.Cursor) > 0 {
		start = int(query.Page.Cursor[0])<<8 | int(query.Page.Cursor[1])
	}
	rows = rows[min(start, len(rows)):]
	page := ringclient.SeedingPage{Rows: rows}
	if limit := int(query.Page.Limit); limit > 0 && len(rows) > limit {
		page.Rows, page.More, page.Cursor = rows[:limit], true, []byte{byte((start + limit) >> 8), byte(start + limit)}
	}
	return page, nil
}

func (f *u52Fake) SeedingRetry(_ context.Context, database string, slot int) error {
	f.seedingActions = append(f.seedingActions, fmt.Sprintf("retry %s %d", database, slot))
	return f.fail("SeedingRetry")
}

func (f *u52Fake) SeedingForget(_ context.Context, database string, slot int, anyway bool) error {
	action := fmt.Sprintf("forget %s %d", database, slot)
	if anyway {
		action += " anyway"
	}
	f.seedingActions = append(f.seedingActions, action)
	return f.fail("SeedingForget")
}

// t6Session is a session over the fake; the seeding statements run on every session since S4.
func t6Session(t *testing.T) (*RingSession, *u52Fake) {
	t.Helper()
	return u52Session()
}

func t6Rows() []ringclient.SeedingRow {
	return []ringclient.SeedingRow{
		{Database: "app", Slot: 7, State: "retired"},
		{Database: "app", Slot: 2638, State: "stalled", Source: "node-b", BytesDone: 10 << 20, BytesKnown: 40 << 20, Entries: 123456, Idle: 372 * time.Second,
			Refusal: "node-b has not answered since 14:02:11"},
		{Database: "app", Slot: 2639, State: "placing"},
		{Database: "shop", Slot: 3, State: "seeding", Source: "node-c", BytesDone: 5 << 20, Entries: 40, Idle: 2 * time.Second},
	}
}

// t6Cells cuts each line of a table at the start of its header words, so a test pins the
// columns and that every cell starts under its header.
func t6Cells(t *testing.T, lines []string, headers ...string) [][]string {
	t.Helper()
	starts := make([]int, len(headers))
	for i, header := range headers {
		starts[i] = strings.Index(lines[0], header)
		if starts[i] < 0 || i > 0 && starts[i] <= starts[i-1] {
			t.Fatalf("header %q is not in order in %q", header, lines[0])
		}
	}
	var table [][]string
	for _, line := range lines {
		var cells []string
		for i, start := range starts {
			end := len(line)
			if i+1 < len(starts) {
				end = starts[i+1]
			}
			if start > len(line) {
				t.Fatalf("line %q is shorter than its header", line)
			}
			if i > 0 && strings.TrimSpace(line[starts[i]-1:starts[i]]) != "" {
				t.Fatalf("cell %d of %q does not start under its header", i, line)
			}
			cells = append(cells, strings.TrimSpace(line[start:min(end, len(line))]))
		}
		table = append(table, cells)
	}
	return table
}

// TestT6StatusSeedingPrintsOneRowPerSlotAndTheFixBelow: [GUARD the table has the seven columns in
// order with every cell under its header, a stalled pull and a refusal are explained below the
// table with the statement that restarts it, and a retired slot names the statement that forgets it].
func TestT6StatusSeedingPrintsOneRowPerSlotAndTheFixBelow(t *testing.T) {
	session, fake := t6Session(t)
	fake.seeding = t6Rows()
	output, err := session.Execute(context.Background(), "status seeding")
	if err != nil {
		t.Fatal(err)
	}
	lines := output.Lines
	if lines[0] != "SEEDING" || lines[1] != "---" {
		t.Fatalf("title and rule are %q", lines[:2])
	}
	table := t6Cells(t, lines[2:7], "DATABASE", "SLOT", "STATE", "SOURCE", "COPIED", "ENTRIES", "LAST CHUNK")
	want := [][]string{
		{"DATABASE", "SLOT", "STATE", "SOURCE", "COPIED", "ENTRIES", "LAST CHUNK"},
		{"app", "7", "RETIRED", "-", "-", "0", "-"},
		{"app", "2638", "STALLED", "node-b", "10.0 MiB of 40.0 MiB (25%)", "123456", "6m12s ago"},
		{"app", "2639", "PLACING", "-", "-", "0", "-"},
		{"shop", "3", "SEEDING", "node-c", "5.0 MiB", "40", "2s ago"},
	}
	if fmt.Sprint(table) != fmt.Sprint(want) {
		t.Fatalf("table:\n%v\nwant:\n%v", table, want)
	}
	rest := strings.Join(lines[7:], "\n")
	for _, wantText := range []string{
		"STALLED  app slot 2638 (0x0a4e) from node-b: node-b has not answered since 14:02:11. Run 'seeding retry app 2638' to start it again from a fresh cut on another source",
		"RETIRED  app slot 7 (0x0007): its range is kept until the deletion gate passes. If no other copy will ever confirm it, run 'seeding forget app 7' to drop the record; the data goes on the next seed",
	} {
		if !strings.Contains(rest, wantText) {
			t.Errorf("the explanation lacks %q in:\n%s", wantText, rest)
		}
	}
	if strings.Contains(rest, "PLACING") || strings.Contains(rest, "slot 3 ") {
		t.Errorf("a placing or healthy seeding row is explained:\n%s", rest)
	}
}

// TestT6StatusSeedingNarrowsAndSaysWhenNothingIsSeeding: [GUARD the database and slot are
// sent as typed (decimal or 0x hex), an empty answer says what empty means and how to widen, and
// a slot past the last is refused before anything is sent].
func TestT6StatusSeedingNarrowsAndSaysWhenNothingIsSeeding(t *testing.T) {
	session, fake := t6Session(t)
	got, err := u52Run(session, "status seeding")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "SEEDING", "---", "Nothing is being copied to this node: no slot is placing, seeding, stalled or retired.")
	got, err = u52Run(session, "status seeding app 0x0a4e")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "SEEDING", "---", `No seeding row for slot 2638 of database "app". Run 'status seeding' to list every row.`)
	if q := fake.seedingQueries[len(fake.seedingQueries)-1]; q.Database != "app" || q.Slot == nil || *q.Slot != 2638 {
		t.Errorf("the query sent was %+v", q)
	}
	got, err = u52Run(session, "status seeding nobody")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "SEEDING", "---", `No seeding row for database "nobody". Run 'status seeding' to list every row.`)
	if _, err := session.Execute(context.Background(), "status seeding app 9000"); err == nil ||
		!strings.Contains(err.Error(), "8191") || !strings.Contains(err.Error(), "status seeding") {
		t.Errorf("a slot past the last was accepted or refused without the fix: %v", err)
	}
}

// TestT6RetryAndForgetSayWhatWentAndPassTheRefusalsOn: [GUARD each statement sends its action with the
// database and slot as typed, prints one OK line that names the next statement, and a refusal keeps
// its code and the exit of conventions Errors].
func TestT6RetryAndForgetSayWhatWentAndPassTheRefusalsOn(t *testing.T) {
	session, fake := t6Session(t)
	got, err := u52Run(session, "seeding retry app 0x0a4e", "seeding forget app 7")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"OK  retrying slot 2638 (0x0a4e) of app from a fresh cut. Run 'status seeding app 2638' to watch it",
		"OK  forgot the retired record of slot 7 (0x0007) of app. Its data stays on disk until another copy accepts this node's next pull of the slot, or until this node leaves the slot's copy set after being named a copy again; either deletes it. Run 'status seeding app 7' to confirm")
	if fmt.Sprint(fake.seedingActions) != "[retry app 2638 forget app 7]" {
		t.Errorf("the API saw %v", fake.seedingActions)
	}
	for _, tc := range []struct {
		method, statement, code string
		refusal                 cluster.Code
		exit                    int
	}{
		{"SeedingRetry", "seeding retry app 5", "SEED_NOT_HELD", cluster.CodeSeedNotHeld, 2},
		{"SeedingForget", "seeding forget app 5", "SEED_HELD", cluster.CodeSeedHeld, 8},
		{"SeedingForget", "seeding forget app 5", "PERMISSION_DENIED", cluster.CodePermissionDenied, 2},
	} {
		fake.failures[tc.method] = &ringclient.Error{Code: tc.refusal, Message: "the server's text"}
		_, err := u52Run(session, tc.statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Code != tc.code || failed.Exit != tc.exit || failed.Message != "the server's text" {
			t.Errorf("%s: %v (want %s, exit %d)", tc.statement, err, tc.code, tc.exit)
		}
		delete(fake.failures, tc.method)
	}
	if _, err := u52Run(session, "seeding forget app 8191", "seeding forget app 0", "seeding forget app 010", "seeding forget app 0X0a4e", "seeding forget app 00"); err != nil {
		t.Errorf("the first and last slots, a leading zero or an upper-case X were refused: %v", err)
	}
	// 010 is ten: a leading zero is not octal.
	if got := fake.seedingActions[len(fake.seedingActions)-3]; got != "forget app 10" {
		t.Errorf("010 was read as %q, want slot 10", got)
	}
	if got := fake.seedingActions[len(fake.seedingActions)-2]; got != "forget app 2638" {
		t.Errorf("0X0a4e was read as %q", got)
	}
	for _, statement := range []string{"seeding retry app", "seeding retry", "seeding forget app x", "seeding forget app 9000", "seeding forget app 8192", "seeding forget app 0x2000", "seeding forget app -1",
		"seeding forget app 0b101", "seeding forget app 0o17", "seeding forget app 1_0", "seeding forget app +5", "seeding forget app 0x", "seeding forget app 0x_1"} {
		if _, err := session.Execute(context.Background(), statement); err == nil {
			t.Errorf("%q was accepted", statement)
		}
	}
}

// TestSeedingForgetAnywayReachesTheAPIAndTheFixParses: the optional trailing word anyway is
// sent as the flag, a plain forget is not, the statement renders back with the word, and the
// refusal's quoted fix parses. [BREAK: drop the flag in seedingAction]
func TestSeedingForgetAnywayReachesTheAPIAndTheFixParses(t *testing.T) {
	session, fake := t6Session(t)
	if _, err := u52Run(session, "seeding forget app 7 anyway", "seeding forget app 8", "SEEDING FORGET app 9 ANYWAY"); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(fake.seedingActions); got != "[forget app 7 anyway forget app 8 forget app 9 anyway]" {
		t.Errorf("the API saw %s", got)
	}
	st, err := Parse("seeding forget app 7 anyway", "")
	if err != nil {
		t.Fatal(err)
	}
	if st.String() != "seeding forget app 7 anyway" {
		t.Errorf("the statement renders as %q", st.String())
	}
	for _, bad := range []string{"seeding forget app anyway", "seeding forget app 7 anyways", "seeding forget app 7 anyway anyway", "seeding retry app 7 anyway"} {
		if _, err := session.Execute(context.Background(), bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
	if got := CompleteWith(u51Source{}, "seeding forget app 5 an", "app"); strings.Join(got, " ") != "anyway" {
		t.Errorf("completion after the slot offers %v, want anyway", got)
	}
	text, _ := cluster.Template(cluster.CodeSeedHeld, cluster.CondSeedHeldOnlyCopy)
	if !strings.Contains(text, "'seeding forget <db> <slot> anyway'") {
		t.Errorf("the only-copy refusal does not quote the anyway fix: %s", text)
	}
}

// TestT6ARetiringSlotSaysWaitAndNeverForget: [GUARD a retiring row is explained as a delete that cannot
// be stopped, the line names no statement to run, and a state the CLI does not know is shown as it
// came and never as retired].
func TestT6ARetiringSlotSaysWaitAndNeverForget(t *testing.T) {
	session, fake := t6Session(t)
	fake.seeding = []ringclient.SeedingRow{{Database: "app", Slot: 9, State: "retiring"}, {Database: "app", Slot: 10, State: "mystery"}}
	output, err := session.Execute(context.Background(), "status seeding")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(output.Lines, "\n")
	if !strings.Contains(text, "RETIRING  app slot 9 (0x0009): its range is being deleted and that cannot be stopped. Wait for the row to leave 'status seeding'") {
		t.Errorf("no wait line for the retiring row:\n%s", text)
	}
	if strings.Contains(text, "seeding forget") || strings.Contains(text, "RETIRED") {
		t.Errorf("a retiring or unknown row mentions forget or retired:\n%s", text)
	}
	if !strings.Contains(text, "MYSTERY") {
		t.Errorf("an unknown state is not shown as it came:\n%s", text)
	}
}

// TestT6StatusSeedingStopsOnAPageThatAddsNothing: [GUARD a server that keeps answering more with no
// rows ends the statement with an error that says so, instead of asking forever].
func TestT6StatusSeedingStopsOnAPageThatAddsNothing(t *testing.T) {
	session, fake := t6Session(t)
	fake.seedingStuck = true
	_, err := session.Execute(context.Background(), "status seeding")
	if err == nil || !strings.Contains(err.Error(), "no new rows") || len(fake.seedingQueries) > 3 {
		t.Fatalf("err = %v after %d queries", err, len(fake.seedingQueries))
	}
}

// TestT6ThePercentageNeverOverflows: [GUARD a range of 2^62 bytes copied to the end prints 100%].
func TestT6ThePercentageNeverOverflows(t *testing.T) {
	session, fake := t6Session(t)
	fake.seeding = []ringclient.SeedingRow{{Database: "app", Slot: 1, State: "seeding", BytesDone: 1 << 62, BytesKnown: 1 << 62}}
	output, err := session.Execute(context.Background(), "status seeding")
	if err != nil || !strings.Contains(strings.Join(output.Lines, "\n"), "(100%)") {
		t.Fatalf("%v, %v", output.Lines, err)
	}
}
