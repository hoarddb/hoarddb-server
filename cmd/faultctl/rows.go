package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

// csvHeader is the one row layout of every CSV faultctl writes and reads: one row per attempt.
// A write that was acknowledged has an outcome; a refusal has an empty outcome and a code; a
// deadline has the code TIMEOUT and a lost connection the code CONN.
var csvHeader = []string{"client", "id", "seq", "pos_epoch", "pos_seq", "outcome", "copies", "served_by", "via", "at_ns", "err_code"}

// attempt is one request as the client saw it, written as one CSV row.
type attempt struct {
	Client   string
	ID       string
	Seq      int64 // the client's own counter; -1 for a read that found nothing
	PosEpoch uint64
	PosSeq   uint64
	Outcome  string // applied, applied_degraded or unknown; empty for a refusal and for every read
	Copies   int
	ServedBy string // the owner that ordered the write, when the client can learn it; else empty
	Via      string // the address the request was sent to
	AtNs     int64  // wall clock at completion, Unix nanoseconds
	ErrCode  string // empty when the node answered
}

func (a attempt) record() []string {
	return []string{
		a.Client, a.ID, strconv.FormatInt(a.Seq, 10), strconv.FormatUint(a.PosEpoch, 10), strconv.FormatUint(a.PosSeq, 10),
		a.Outcome, strconv.Itoa(a.Copies), a.ServedBy, a.Via, strconv.FormatInt(a.AtNs, 10), a.ErrCode,
	}
}

// writeAttempts writes the header and then one row per attempt.
func writeAttempts(out io.Writer, rows []attempt) error {
	writer := csv.NewWriter(out)
	if err := writer.Write(csvHeader); err != nil {
		return fmt.Errorf("writing the CSV header: %w", err)
	}
	for _, row := range rows {
		if err := writer.Write(row.record()); err != nil {
			return fmt.Errorf("writing a CSV row for id %q: %w", row.ID, err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flushing the CSV: %w", err)
	}
	return nil
}

// readAttempts parses a CSV written by writeAttempts. A malformed row is an error naming its
// line, because a verifier that skips a row it cannot read would pass a corrupted file.
func readAttempts(in io.Reader) ([]attempt, error) {
	reader := csv.NewReader(in)
	reader.FieldsPerRecord = len(csvHeader)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parsing the CSV: %w", err)
	}
	if len(records) == 0 || !equalStrings(records[0], csvHeader) {
		return nil, fmt.Errorf("the first CSV line is not the header %v: this is not a faultctl file", csvHeader)
	}
	rows := make([]attempt, 0, len(records)-1)
	for index, record := range records[1:] {
		row, err := parseRecord(record)
		if err != nil {
			return nil, fmt.Errorf("CSV line %d: %w", index+2, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseRecord(record []string) (attempt, error) {
	seq, errSeq := strconv.ParseInt(record[2], 10, 64)
	epoch, errEpoch := strconv.ParseUint(record[3], 10, 64)
	posSeq, errPosSeq := strconv.ParseUint(record[4], 10, 64)
	copies, errCopies := strconv.Atoi(record[6])
	atNs, errAt := strconv.ParseInt(record[9], 10, 64)
	for _, err := range []error{errSeq, errEpoch, errPosSeq, errCopies, errAt} {
		if err != nil {
			return attempt{}, fmt.Errorf("a numeric column is not a number: %w", err)
		}
	}
	return attempt{
		Client: record[0], ID: record[1], Seq: seq, PosEpoch: epoch, PosSeq: posSeq, Outcome: record[5], Copies: copies,
		ServedBy: record[7], Via: record[8], AtNs: atNs, ErrCode: record[10],
	}, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

// loadAttempts reads one CSV file.
func loadAttempts(path string) ([]attempt, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	rows, err := readAttempts(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return rows, nil
}
