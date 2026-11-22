package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// roundCounts is one round's share of the ledger. Sent = Acknowledged + Unknown.
type roundCounts struct {
	Round          int
	Sent           int
	Acknowledged   int
	Present        int
	Missing        int
	Unknown        int
	UnknownPresent int
	UnknownAbsent  int
}

// report is the judge's finding. Every acknowledged write is in exactly one of Present,
// Missing, AckedMismatched and AckedUnreadable; the id lists name what failed.
type report struct {
	Rounds []roundCounts

	Sent         int
	Acknowledged int
	Present      int
	// Missing are acknowledged writes a Get answered NOT_FOUND for.
	Missing []string
	// Mismatched are documents, acknowledged or unknown, that came back with a body the
	// drill did not write under that id.
	Mismatched      []string
	AckedMismatched int
	// Unreadable are ledger ids no conclusive read exists for.
	Unreadable      []string
	AckedUnreadable int

	Unknown        int
	UnknownPresent int
	UnknownAbsent  int

	// Duplicates are ids the bucket scan returned more than once.
	Duplicates []string
	// NeverSent are ids the bucket scan returned that the ledger does not hold.
	NeverSent []string
	// NotListed are ids a Get found and the bucket scan did not return.
	NotListed []string

	ListedDistinct int
	ServerCount    int64

	// Database and Bucket name where the drill wrote, for the commands a failure prints.
	Database string
	Bucket   string
}

// readCommand is the CLI line that reads one document of the drill's bucket.
func (r report) readCommand(id string) string {
	database, bucket := r.Database, r.Bucket
	if database == "" {
		database = "<database>"
	}
	if bucket == "" {
		bucket = "<bucket>"
	}
	return fmt.Sprintf("use %s; db.%s[%q].get()", database, bucket, id)
}

// judge compares the ledger with what the store answered. It reads no server: the verdict
// depends only on its two arguments, which is what lets a test hand it a store that lost a
// write.
func judge(rows []ledgerRow, seen observations) (report, error) {
	found := report{ServerCount: seen.ServerCount, ListedDistinct: len(seen.Listed)}
	byRound := map[int]*roundCounts{}
	sent := make(map[string]bool, len(rows))
	for _, row := range rows {
		if sent[row.ID] {
			return report{}, fmt.Errorf("the ledger names id %s twice; the drill sends every id once, so this ledger is damaged or two runs wrote it. Start again with a new --ledger file", row.ID)
		}
		sent[row.ID] = true
		round := byRound[row.Round]
		if round == nil {
			round = &roundCounts{Round: row.Round}
			byRound[row.Round] = round
		}
		state, read := seen.Gets[row.ID]
		if !read {
			state = stateUnreadable
		}
		acked := row.Outcome == outcomeAcked
		found.Sent++
		round.Sent++
		if acked {
			found.Acknowledged++
			round.Acknowledged++
		} else {
			found.Unknown++
			round.Unknown++
		}
		switch state {
		case statePresent:
			if acked {
				found.Present++
				round.Present++
			} else {
				found.UnknownPresent++
				round.UnknownPresent++
			}
			if seen.Listed[row.ID] == 0 {
				found.NotListed = append(found.NotListed, row.ID)
			}
		case stateAbsent:
			if acked {
				found.Missing = append(found.Missing, row.ID)
				round.Missing++
			} else {
				found.UnknownAbsent++
				round.UnknownAbsent++
			}
		case stateMismatch:
			found.Mismatched = append(found.Mismatched, row.ID)
			if acked {
				found.AckedMismatched++
			}
		default:
			found.Unreadable = append(found.Unreadable, row.ID)
			if acked {
				found.AckedUnreadable++
			}
		}
	}
	for _, id := range sortedKeys(seen.Listed) {
		if seen.Listed[id] > 1 {
			found.Duplicates = append(found.Duplicates, id)
		}
		if !sent[id] {
			found.NeverSent = append(found.NeverSent, id)
		}
	}
	for _, round := range byRound {
		found.Rounds = append(found.Rounds, *round)
	}
	sort.Slice(found.Rounds, func(i, j int) bool { return found.Rounds[i].Round < found.Rounds[j].Round })
	return found, nil
}

// failures lists why the drill does not pass; empty means it passes.
func (r report) failures() []string {
	var failures []string
	if r.Acknowledged == 0 {
		failures = append(failures, "no write was acknowledged, so the drill proved nothing. Check that the server was serving before the load started (durabilitydrill ready --addr HOST:PORT --insecure) and run the round again")
	}
	if len(r.Missing) > 0 {
		failures = append(failures, fmt.Sprintf("%s missing after the restart: the server answered \"applied\" and a Get now answers NOT_FOUND. Ids: %s. This is acknowledged data lost; keep the data directory and the ledger, and read one id again in the hoarddb CLI: %s",
			plural(len(r.Missing), "acknowledged write is", "acknowledged writes are"), sample(r.Missing), r.readCommand(r.Missing[0])))
	}
	if len(r.Mismatched) > 0 {
		failures = append(failures, fmt.Sprintf("%s came back with a body the drill did not write under that id (Check or Pad differs). Ids: %s. Read one in the hoarddb CLI and compare its Round, Writer and Seq with the id: %s",
			plural(len(r.Mismatched), "document", "documents"), sample(r.Mismatched), r.readCommand(r.Mismatched[0])))
	}
	if len(r.Unreadable) > 0 {
		failures = append(failures, fmt.Sprintf("%s could not be read (no conclusive answer before the read's deadline), so the drill cannot say they are present. Ids: %s. This is data unreachable, not shown lost: wait for the node to serve (durabilitydrill ready) and run 'durabilitydrill read' and 'durabilitydrill judge' again on the same ledger",
			plural(len(r.Unreadable), "ledger id", "ledger ids"), sample(r.Unreadable)))
	}
	if len(r.Duplicates) > 0 {
		failures = append(failures, fmt.Sprintf("%s more than once in a scan of the bucket. Ids: %s. The drill sends every id once, so the store holds or lists a document twice",
			plural(len(r.Duplicates), "id was listed", "ids were listed"), sample(r.Duplicates)))
	}
	if len(r.NeverSent) > 0 {
		failures = append(failures, fmt.Sprintf("%s in the bucket that the drill never sent. Ids: %s. If another run wrote to this bucket, start the drill on an empty data directory; otherwise the store invented a document",
			plural(len(r.NeverSent), "document is", "documents are"), sample(r.NeverSent)))
	}
	if len(r.NotListed) > 0 {
		failures = append(failures, fmt.Sprintf("%s by a Get and left out by a scan of the bucket. Ids: %s. Two read paths disagree about the same documents",
			plural(len(r.NotListed), "document was found", "documents were found"), sample(r.NotListed)))
	}
	return failures
}

// text is the report: every count, then the verdict. The counts come first on purpose, and
// print writes the whole text or fails, so a verdict never appears without them.
func (r report) text() string {
	out := &strings.Builder{}
	fmt.Fprintf(out, "%-6s %12s %12s %12s %12s %12s %12s %12s\n", "round", "sent", "acknowledged", "present", "missing", "unknown", "unk-present", "unk-absent")
	for _, round := range r.Rounds {
		fmt.Fprintf(out, "%-6d %12d %12d %12d %12d %12d %12d %12d\n", round.Round, round.Sent, round.Acknowledged, round.Present, round.Missing, round.Unknown, round.UnknownPresent, round.UnknownAbsent)
	}
	line := func(name string, count int, note string) {
		fmt.Fprintf(out, "%-25s %d%s\n", name, count, note)
	}
	line("sent", r.Sent, "")
	line("acknowledged", r.Acknowledged, "   (the server answered \"applied\")")
	line("present", r.Present, "   (acknowledged and read back whole)")
	line("missing", len(r.Missing), "   (acknowledged and NOT_FOUND; must be 0)")
	line("unknown", r.Unknown, "   (no acknowledgement: in flight at the kill, refused or timed out)")
	line("unknown and present", r.UnknownPresent, "")
	line("unknown and absent", r.UnknownAbsent, "")
	line("duplicates", len(r.Duplicates), "   (listed more than once; must be 0)")
	line("never sent", len(r.NeverSent), "   (in the bucket, not in the ledger; must be 0)")
	line("changed body", len(r.Mismatched), "   (must be 0)")
	line("could not be read", len(r.Unreadable), "   (must be 0)")
	line("found but not listed", len(r.NotListed), "   (must be 0)")
	line("listed by the scan", r.ListedDistinct, "   (distinct ids)")
	if r.ServerCount >= 0 {
		note := ""
		if r.ServerCount != int64(r.ListedDistinct) {
			note = fmt.Sprintf("   WARNING: differs from the %d ids the scan listed", r.ListedDistinct)
		}
		fmt.Fprintf(out, "%-25s %d%s\n", "server's own count", r.ServerCount, note)
	}
	failures := r.failures()
	if len(failures) == 0 {
		fmt.Fprintf(out, "RESULT PASS: %d of %d acknowledged writes present, 0 missing, 0 duplicates, 0 never sent\n", r.Present, r.Acknowledged)
		return out.String()
	}
	for _, failure := range failures {
		fmt.Fprintf(out, "FAIL: %s\n", failure)
	}
	fmt.Fprintf(out, "RESULT FAIL: %d of %d acknowledged writes present, %d missing, %d duplicates, %d never sent\n", r.Present, r.Acknowledged, len(r.Missing), len(r.Duplicates), len(r.NeverSent))
	return out.String()
}

// print writes the report to out.
func (r report) print(out io.Writer) error {
	if _, err := io.WriteString(out, r.text()); err != nil {
		return fmt.Errorf("printing the report: %w", err)
	}
	return nil
}

func plural(count int, one, many string) string {
	if count == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", count, many)
}

// sample names up to ten ids and says how many it left out.
func sample(ids []string) string {
	const shown = 10
	if len(ids) <= shown {
		return strings.Join(ids, " ")
	}
	return fmt.Sprintf("%s (and %d more; every id is in the observations file)", strings.Join(ids[:shown], " "), len(ids)-shown)
}
