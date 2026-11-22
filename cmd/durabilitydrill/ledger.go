package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The two outcomes of the ledger. A write is acked only when the server answered "applied"
// with no error; every other ending is unknown, with the reason in Detail.
const (
	outcomeAcked   = "acked"
	outcomeUnknown = "unknown"
)

// What a read of one id came to. Unreadable means no conclusive answer arrived before the
// read's deadline: the document is neither counted present nor missing.
const (
	statePresent    = "present"
	stateAbsent     = "absent"
	stateMismatch   = "mismatch"
	stateUnreadable = "unreadable"
)

const (
	ledgerHeader       = "round,writer,seq,id,outcome,detail"
	observationsHeader = "kind,id,state"
)

// ledgerRow is one write the drill sent: who sent it, and how it ended.
type ledgerRow struct {
	Round   int
	Writer  int
	Seq     int64
	ID      string
	Outcome string
	Detail  string
}

// observations is what the store answered after a restart: one Get per ledger id, how many
// times each id came back from a scan of the whole bucket, and the server's own count
// (-1 when it was not read).
type observations struct {
	Gets        map[string]string
	Listed      map[string]int
	ServerCount int64
}

// drillID is the id of one write. Round, writer and seq make it unique across a whole drill.
func drillID(round, writer int, seq int64) string {
	return "r" + strconv.Itoa(round) + "-w" + strconv.Itoa(writer) + "-" + strconv.FormatInt(seq, 10)
}

// checkOf is the value a document carries in Check: a digest of its own id, so a read can
// tell a document that came back whole from one that came back under the wrong id.
func checkOf(id string) string {
	sum := sha256.Sum256([]byte("hoarddb-durability-drill:" + id))
	return hex.EncodeToString(sum[:16])
}

// padOf repeats the check to size bytes.
func padOf(check string, size int) string {
	if size <= 0 {
		return ""
	}
	return strings.Repeat(check, size/len(check)+1)[:size]
}

// drillBody is the document written under id.
func drillBody(round, writer int, seq int64, id string, padBytes int) ringclient.Doc {
	check := checkOf(id)
	return ringclient.Doc{"Round": int64(round), "Writer": int64(writer), "Seq": seq, "Check": check, "Pad": padOf(check, padBytes)}
}

// bodyMatches reports whether check and pad are what drillBody wrote under id.
func bodyMatches(id, check, pad string) bool {
	want := checkOf(id)
	return check == want && pad == padOf(want, len(pad))
}

func (row ledgerRow) record() []string {
	return []string{strconv.Itoa(row.Round), strconv.Itoa(row.Writer), strconv.FormatInt(row.Seq, 10), row.ID, row.Outcome, row.Detail}
}

// writeLedger writes rows as CSV, with the header first when header is set.
func writeLedger(out io.Writer, rows []ledgerRow, header bool) error {
	writer := csv.NewWriter(out)
	if header {
		if err := writer.Write(strings.Split(ledgerHeader, ",")); err != nil {
			return fmt.Errorf("writing the ledger header: %w", err)
		}
	}
	for _, row := range rows {
		if err := writer.Write(row.record()); err != nil {
			return fmt.Errorf("writing ledger row %s: %w", row.ID, err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flushing the ledger: %w", err)
	}
	return nil
}

// readLedger parses a ledger. It refuses an outcome it does not know, so a damaged ledger
// cannot turn a write into something the judge skips.
func readLedger(in io.Reader) ([]ledgerRow, error) {
	records, err := readCSV(in, ledgerHeader, 6)
	if err != nil {
		return nil, fmt.Errorf("reading the ledger: %w", err)
	}
	rows := make([]ledgerRow, 0, len(records))
	for line, record := range records {
		round, roundErr := strconv.Atoi(record[0])
		writer, writerErr := strconv.Atoi(record[1])
		seq, seqErr := strconv.ParseInt(record[2], 10, 64)
		if err := errors.Join(roundErr, writerErr, seqErr); err != nil {
			return nil, fmt.Errorf("ledger row %d (%s): %w", line+2, strings.Join(record, ","), err)
		}
		row := ledgerRow{Round: round, Writer: writer, Seq: seq, ID: record[3], Outcome: record[4], Detail: record[5]}
		if row.Outcome != outcomeAcked && row.Outcome != outcomeUnknown {
			return nil, fmt.Errorf("ledger row %d: outcome %q is neither %q nor %q", line+2, row.Outcome, outcomeAcked, outcomeUnknown)
		}
		if row.ID == "" {
			return nil, fmt.Errorf("ledger row %d has no id", line+2)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// writeObservations writes what the store answered, ids sorted so two reads of one store
// give the same file.
func writeObservations(out io.Writer, seen observations) error {
	writer := csv.NewWriter(out)
	records := [][]string{strings.Split(observationsHeader, ",")}
	for _, id := range sortedKeys(seen.Gets) {
		records = append(records, []string{"get", id, seen.Gets[id]})
	}
	for _, id := range sortedKeys(seen.Listed) {
		records = append(records, []string{"list", id, strconv.Itoa(seen.Listed[id])})
	}
	records = append(records, []string{"count", "", strconv.FormatInt(seen.ServerCount, 10)})
	if err := writer.WriteAll(records); err != nil {
		return fmt.Errorf("writing the observations: %w", err)
	}
	return nil
}

func readObservations(in io.Reader) (observations, error) {
	seen := observations{Gets: map[string]string{}, Listed: map[string]int{}, ServerCount: -1}
	records, err := readCSV(in, observationsHeader, 3)
	if err != nil {
		return seen, fmt.Errorf("reading the observations: %w", err)
	}
	for line, record := range records {
		kind, id, state := record[0], record[1], record[2]
		switch kind {
		case "get":
			switch state {
			case statePresent, stateAbsent, stateMismatch, stateUnreadable:
				seen.Gets[id] = state
			default:
				return seen, fmt.Errorf("observations row %d: state %q of %s is not one the read writes", line+2, state, id)
			}
		case "list":
			times, err := strconv.Atoi(state)
			if err != nil || times < 1 {
				return seen, fmt.Errorf("observations row %d: %q is not how many times %s was listed", line+2, state, id)
			}
			seen.Listed[id] = times
		case "count":
			count, err := strconv.ParseInt(state, 10, 64)
			if err != nil {
				return seen, fmt.Errorf("observations row %d: count %q: %w", line+2, state, err)
			}
			seen.ServerCount = count
		default:
			return seen, fmt.Errorf("observations row %d: kind %q is not get, list or count", line+2, kind)
		}
	}
	return seen, nil
}

// readCSV returns the records after the header, each with exactly fields fields.
func readCSV(in io.Reader, header string, fields int) ([][]string, error) {
	reader := csv.NewReader(bufio.NewReader(in))
	reader.FieldsPerRecord = fields
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parsing: %w", err)
	}
	if len(records) == 0 || strings.Join(records[0], ",") != header {
		return nil, fmt.Errorf("the first line is not the header %q", header)
	}
	return records[1:], nil
}

func loadLedger(path string) ([]ledgerRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening the ledger: %w", err)
	}
	defer func() { _ = file.Close() }() // read-only: a close error loses nothing
	return readLedger(file)
}

func loadObservations(path string) (observations, error) {
	file, err := os.Open(path)
	if err != nil {
		return observations{}, fmt.Errorf("opening the observations: %w", err)
	}
	defer func() { _ = file.Close() }() // read-only: a close error loses nothing
	return readObservations(file)
}

func sortedKeys[V any](from map[string]V) []string {
	keys := make([]string, 0, len(from))
	for key := range from {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
