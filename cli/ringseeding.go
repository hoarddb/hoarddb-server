package cli

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// This file prints the seeding statements: status seeding, seeding retry and seeding forget.
// The statements are the operator's view of the slots this node is copying (seeding section 11);
// every number is the server's, and nothing here is computed per bucket.

// ringSlotText writes a slot as decimal and as the hex the specs print, 2638 (0x0a4e).
func ringSlotText(slot int) string { return fmt.Sprintf("%d (0x%04x)", slot, slot) }

// ringAgo writes how long ago a chunk was applied, or "-" when none has been.
func ringAgo(idle time.Duration) string {
	if idle <= 0 {
		return "-"
	}
	return idle.Round(time.Second).String() + " ago"
}

// ringCopied writes how much of the range is copied: "10.0 MiB of 40.0 MiB (25%)" when the
// source has said how big the range is, the bytes so far when it has not, "-" before the first chunk.
func ringCopied(row ringclient.SeedingRow) string {
	switch {
	case row.BytesKnown > 0:
		// Divide first: done*100 overflows int64 for a range past 2^56 bytes.
		percent := min(100, row.BytesDone/(max(row.BytesKnown/100, 1)))
		return fmt.Sprintf("%s of %s (%d%%)", humanBytes(row.BytesDone), humanBytes(row.BytesKnown), percent)
	case row.BytesDone > 0:
		return humanBytes(row.BytesDone)
	}
	return "-"
}

// ringSeedingExplanation is the line under the table for a row that needs an operator's eye: a
// stalled pull or a refusal says why and names the statement that fixes it, and a retired slot
// names the statement that lets go of it. It returns "" for a row that is simply working.
func ringSeedingExplanation(row ringclient.SeedingRow) string {
	database, slot := terminalSafe(row.Database), ringSlotText(row.Slot)
	reason := terminalSafe(row.Refusal)
	switch row.State {
	case "stalled":
		if reason == "" {
			reason = "no chunk has been applied for " + row.Idle.Round(time.Second).String()
		}
		return fmt.Sprintf("STALLED  %s slot %s from %s: %s. Run 'seeding retry %s %d' to start it again from a fresh cut on another source",
			database, slot, terminalSafe(row.Source), strings.TrimRight(reason, ". "), database, row.Slot)
	case "retiring":
		return fmt.Sprintf("RETIRING  %s slot %s: its range is being deleted and that cannot be stopped. Wait for the row to leave 'status seeding'; there is nothing to run",
			database, slot)
	case "retired":
		gate := ""
		if reason != "" {
			gate = " (" + strings.TrimRight(reason, ". ") + ")"
		}
		return fmt.Sprintf("RETIRED  %s slot %s: its range is kept until the deletion gate passes%s. If no other copy will ever confirm it, run 'seeding forget %s %d' to drop the record; the data goes on the next seed",
			database, slot, gate, database, row.Slot)
	}
	if reason != "" {
		return fmt.Sprintf("%s  %s slot %s: %s", strings.ToUpper(row.State), database, slot, strings.TrimRight(reason, ". ")+".")
	}
	return ""
}

const (
	// ringSeedingPage is the rows asked of the server at a time: its page ceiling.
	ringSeedingPage = 1000
	// ringSeedingRows is the most rows one status prints; a longer list is cut with the way to narrow it.
	ringSeedingRows = 2000
)

// statusSeeding prints the slot pulls, narrowed by the database and slot the statement names. It
// follows the server's pages itself, up to ringSeedingRows rows, so the statement has no paging to learn.
func (s *RingSession) statusSeeding(ctx context.Context, st Statement) (RingOutput, error) {
	query := ringclient.SeedingQuery{Database: ringText(st, "database"), Page: ringclient.PageRequest{Limit: ringSeedingPage}}
	if slotArg, ok := st.Arg("slot"); ok {
		slot := int(slotArg.Int)
		query.Slot = &slot
	}
	var rows []ringclient.SeedingRow
	truncated := false
	for {
		previous := query.Page.Cursor
		page, err := s.api.SeedingStatus(ctx, query)
		if err != nil {
			return RingOutput{}, err
		}
		rows = append(rows, page.Rows...)
		if !page.More {
			break
		}
		if len(page.Rows) == 0 || bytes.Equal(page.Cursor, previous) {
			return RingOutput{}, ringRefusal("", "the node keeps answering that more rows follow and sends no new rows. Nothing is wrong with your statement; run 'status seeding' again in a few seconds, and report it if it repeats")
		}
		if len(rows) >= ringSeedingRows {
			rows, truncated = rows[:ringSeedingRows], true
			break
		}
		query.Page.Cursor = page.Cursor
	}
	lines := []string{"SEEDING", "---"}
	if len(rows) == 0 {
		switch {
		case query.Slot != nil:
			return ringLines(append(lines, fmt.Sprintf("No seeding row for slot %d of database %q. Run 'status seeding' to list every row.", *query.Slot, terminalSafe(query.Database)))...), nil
		case query.Database != "":
			return ringLines(append(lines, fmt.Sprintf("No seeding row for database %q. Run 'status seeding' to list every row.", terminalSafe(query.Database)))...), nil
		}
		return ringLines(append(lines, "Nothing is being copied to this node: no slot is placing, seeding, stalled or retired.")...), nil
	}
	table := [][]string{{"DATABASE", "SLOT", "STATE", "SOURCE", "COPIED", "ENTRIES", "LAST CHUNK"}}
	var explained []string
	for _, row := range rows {
		source := "-"
		if row.Source != "" {
			source = terminalSafe(row.Source)
		}
		table = append(table, []string{terminalSafe(row.Database), fmt.Sprint(row.Slot), strings.ToUpper(row.State), source, ringCopied(row), fmt.Sprint(row.Entries), ringAgo(row.Idle)})
		if line := ringSeedingExplanation(row); line != "" {
			explained = append(explained, line)
		}
	}
	widths := make([]int, len(table[0]))
	for _, cells := range table {
		for i, cell := range cells {
			widths[i] = max(widths[i], ringWidth(cell))
		}
	}
	for _, cells := range table {
		var line strings.Builder
		for i, cell := range cells {
			if i == len(cells)-1 {
				line.WriteString(cell)
				break
			}
			line.WriteString(cell + strings.Repeat(" ", widths[i]-ringWidth(cell)+2))
		}
		lines = append(lines, line.String())
	}
	if len(explained) > 0 {
		lines = append(lines, "")
		lines = append(lines, explained...)
	}
	if truncated {
		lines = append(lines, "", fmt.Sprintf("Showing the first %d rows. Run 'status seeding <db>' or 'status seeding <db> <slot>' to narrow it.", ringSeedingRows))
	}
	return RingOutput{Lines: lines}, nil
}

// seedingAction runs seeding retry or seeding forget and says what it did. A refusal is the
// server's, with its code and its text, which already names the statement to run instead.
func (s *RingSession) seedingAction(ctx context.Context, st Statement) (RingOutput, error) {
	database := ringText(st, "database")
	slotArg, _ := st.Arg("slot")
	slot := int(slotArg.Int)
	if st.Verb == "seeding retry" {
		if err := s.api.SeedingRetry(ctx, database, slot); err != nil {
			return RingOutput{}, err
		}
		return ringLines(fmt.Sprintf("OK  retrying slot %s of %s from a fresh cut. Run 'status seeding %s %d' to watch it", ringSlotText(slot), terminalSafe(database), terminalSafe(database), slot)), nil
	}
	_, anyway := st.Arg("anyway")
	if err := s.api.SeedingForget(ctx, database, slot, anyway); err != nil {
		return RingOutput{}, err
	}
	return ringLines(fmt.Sprintf("OK  forgot the retired record of slot %s of %s. Its data stays on disk until another copy accepts this node's next pull of the slot, or until this node leaves the slot's copy set after being named a copy again; either deletes it. Run 'status seeding %s %d' to confirm",
		ringSlotText(slot), terminalSafe(database), terminalSafe(database), slot)), nil
}
