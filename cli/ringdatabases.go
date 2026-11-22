package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringPageOf reads the optional 'limit <n> cursor <hex>' clause of a paged
// statement, and says whether a limit was written so the next-page hint repeats it.
func ringPageOf(st Statement) (page ringclient.PageRequest, limit int64, hasLimit bool, err error) {
	limitArg, hasLimit := st.Arg("limit")
	if hasLimit {
		page.Limit = int32(limitArg.Int)
	}
	if cursorArg, ok := st.Arg("cursor"); ok {
		page.Cursor, err = ringDecodeCursor(cursorArg.Text)
	}
	return page, limitArg.Int, hasLimit, err
}

// ringMoreHint is the line a page that is not the last ends with: the statement that fetches the next page.
func ringMoreHint(statement string, limit int64, hasLimit bool, cursor []byte) string {
	next := "more: " + statement
	if hasLimit {
		next += fmt.Sprintf(" limit %d", limit)
	}
	return fmt.Sprintf("%s cursor %x", next, cursor)
}

// showDatabases lists the databases the session may see in the table of v1.0
// ('DATABASES', a rule, NAME ...). v1.0's BUCKETS column is left out: the ring
// reply carries no bucket count, and a count per database is O(buckets).
func (s *RingSession) showDatabases(ctx context.Context, st Statement) (RingOutput, error) {
	page, limit, hasLimit, err := ringPageOf(st)
	if err != nil {
		return RingOutput{}, err
	}
	listed, err := s.api.ShowDatabases(ctx, page)
	if err != nil {
		return RingOutput{}, err
	}
	lines := []string{"DATABASES", "---"}
	if len(listed.Databases) == 0 && !listed.More && len(page.Cursor) == 0 {
		return ringLines(append(lines, "(no databases yet — create one with 'create database <name>')")...), nil
	}
	width := len("NAME")
	for _, database := range listed.Databases {
		width = max(width, len(terminalSafe(database.Name)))
	}
	lines = append(lines, fmt.Sprintf("%-*s  %-16s  %s", width, "NAME", "ID", "REPLICATION"))
	for _, database := range listed.Databases {
		lines = append(lines, fmt.Sprintf("%-*s  %016x  rf %d", width, terminalSafe(database.Name), database.ID, database.RF))
	}
	if listed.More {
		lines = append(lines, ringMoreHint("show databases", limit, hasLimit, listed.Cursor))
	}
	return RingOutput{Lines: lines}, nil
}

// describeDatabase prints a database's id and replication factor and one page of
// its buckets, laid out as v1.0's 'describe database' (a title, a rule, facts,
// the bucket table).
func (s *RingSession) describeDatabase(ctx context.Context, st Statement) (RingOutput, error) {
	name := ringText(st, "database")
	page, limit, hasLimit, err := ringPageOf(st)
	if err != nil {
		return RingOutput{}, err
	}
	described, err := s.api.DescribeDatabase(ctx, name, page)
	if err != nil {
		return RingOutput{}, err
	}
	count := fmt.Sprint(len(described.Buckets))
	if described.More {
		count += " on this page, more follow"
	}
	storage := described.Storage
	if storage == "" {
		storage = "disk"
	}
	rows := []fact{{"Id", fmt.Sprintf("%016x", described.ID)}, {"Replication factor", fmt.Sprint(described.RF)}, {"Storage", storage}}
	if storage == "memory" && (described.MemoryBytes != 0 || described.MemoryCapBytes != 0) {
		rows = append(rows, fact{"Memory store (this node)",
			fmt.Sprintf("%d bytes of %d, shared by every memory database on this node", described.MemoryBytes, described.MemoryCapBytes)})
	}
	facts := formatFacts(append(rows, fact{"Buckets", count}))
	lines := append([]string{"DATABASE " + terminalSafe(name), "---"}, strings.Split(strings.TrimRight(facts, "\n"), "\n")...)
	if len(described.Buckets) > 0 {
		width := len("NAME")
		for _, bucket := range described.Buckets {
			width = max(width, len(terminalSafe(bucket.Name)))
		}
		lines = append(lines, "", fmt.Sprintf("%-*s  %-16s  %s", width, "NAME", "ID", "STATE"))
		for _, bucket := range described.Buckets {
			lines = append(lines, fmt.Sprintf("%-*s  %016x  %s", width, terminalSafe(bucket.Name), bucket.ID, bucket.State))
		}
	}
	if described.More {
		lines = append(lines, ringMoreHint("describe database "+name, limit, hasLimit, described.Cursor))
	}
	return RingOutput{Lines: lines}, nil
}
