package cli

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// dumpFIFOStoreWord and dumpLIFOStoreWord are the two store words whose claimed and
// dead-lettered messages are in no list reply, so a dump that declares one of them
// leaves those messages out (section 3.1, "A complete file is not a complete copy of
// every queue").
const (
	dumpFIFOStoreWord = "fifoStore"
	dumpLIFOStoreWord = "lifoStore"
)

// dumpNoReadableFooter fills the want place of the damaged text when the read stopped
// before a footer could be decoded. Section 3.1 fixes the text for a footer that was
// reached; a bad record length or a record with no t member stops the read with no
// readable footer, and this is the word this build uses there.
const dumpNoReadableFooter = "(no readable footer)"

// The three sentinels a caller branches on. The text a user reads is carried by the
// error types below, because section 3.1 fixes each text word for word.
var (
	errDumpIncomplete   = errors.New("a dump file is cut off before its footer")
	errDumpDamaged      = errors.New("a dump file does not match the checksum and counts its footer and summary state")
	errDumpNewerVersion = errors.New("a dump file was written by a newer hoarddb")
)

// dumpVerification is what verifyDump returns about a whole file: the header's
// consistency word, the five counts the file holds and whether any bucket declares a
// fifoStore or lifoStore field. HasQueueField is a fact the caller reports, never an
// error: a dump with a queue field is whole, it just does not hold every queue message.
type dumpVerification struct {
	Consistency   string
	Records       int64
	Databases     int64
	Buckets       int64
	Entities      int64
	Elements      int64
	HasQueueField bool
	// SHA256 is the checksum the footer carried and the file matched, so a later read of the
	// same path can prove it is the file that was verified.
	SHA256 [32]byte
}

// dumpTally counts the records a verification has read. It holds counters and nothing
// else: no record, no name and no element survives a call, so verification keeps one
// record in memory at a time and its memory does not grow with the file.
type dumpTally struct {
	records   int64
	databases int64
	buckets   int64
	entities  int64
	elements  int64
}

// dumpRecordCounters is what one decoded record of each kind adds to the tally. A kind
// the summary does not count has no row, and a test that registers a kind in
// dumpRecordKinds registers its counter here too: that is what makes bringing a record
// kind back a table edit (section 3.1, "What is frozen").
var dumpRecordCounters = map[string]func(*dumpTally, dumpRecord){
	dumpKindDB:     func(tally *dumpTally, _ dumpRecord) { tally.databases++ },
	dumpKindBucket: func(tally *dumpTally, _ dumpRecord) { tally.buckets++ },
	dumpKindDoc:    func(tally *dumpTally, _ dumpRecord) { tally.entities++ },
	dumpKindElems: func(tally *dumpTally, record dumpRecord) {
		tally.elements += int64(len(record.(*dumpElems).Els))
	},
}

// incompleteDumpError is the text of case 1 of section 3.1. records is the number of
// records read completely, so an operator can tell how far the dump got before it was
// cut off.
type incompleteDumpError struct {
	records int64
}

//nolint:staticcheck // ST1005: the user-facing text is fixed by section 3.1 of the build spec
func (e incompleteDumpError) Error() string {
	return fmt.Sprintf("This dump is incomplete: it ends after %d records with no footer, so it was cut off (disk full, or the dump was killed). Take a new dump; an incomplete file cannot be restored.", e.records)
}

// Unwrap returns errDumpIncomplete, so a caller can branch on the sentinel.
func (e incompleteDumpError) Unwrap() error { return errDumpIncomplete }

// damagedDumpError is the text of case 2 and case 3 of section 3.1: the last part of
// the file that was read and what the footer says about it.
type damagedDumpError struct {
	covered int64
	got     string
	want    string
}

//nolint:staticcheck // ST1005: the user-facing text is fixed by section 3.1 of the build spec
func (e damagedDumpError) Error() string {
	return fmt.Sprintf("This dump is damaged: the checksum of the first %d bytes is %s and the footer says %s. The file was changed after it was written. Restore from another copy, or take a new dump.", e.covered, e.got, e.want)
}

// Unwrap returns errDumpDamaged, so a caller can branch on the sentinel.
func (e damagedDumpError) Unwrap() error { return errDumpDamaged }

// countMismatchError is the text of case 4 of section 3.1 for a summary counter that
// disagrees with what the file holds.
type countMismatchError struct {
	said  int64
	what  string
	holds int64
}

//nolint:staticcheck // ST1005: the user-facing text is fixed by section 3.1 of the build spec
func (e countMismatchError) Error() string {
	return fmt.Sprintf("This dump is damaged: its summary says %d %s but the file holds %d. The file was changed after it was written. Restore from another copy, or take a new dump.", e.said, e.what, e.holds)
}

// Unwrap returns errDumpDamaged, so a caller can branch on the sentinel.
func (e countMismatchError) Unwrap() error { return errDumpDamaged }

// newerVersionError is the refusal of a record kind or a header consistency word this
// version does not know (section 3.1, rev 7.1). It is returned only after the checksum
// and every count agreed, so a flipped byte in a kind or a word can never be blamed on a
// newer version.
type newerVersionError struct {
	kind           string
	consistency    string
	version        int32
	versionUnknown bool
}

//nolint:staticcheck // ST1005: the user-facing text is fixed by section 3.1 of the build spec
func (e newerVersionError) Error() string {
	if e.versionUnknown {
		return fmt.Sprintf("This dump is format version %d and this version of hoarddb reads version %d. Use the hoarddb that wrote it, or take a new dump with this version.", e.version, dumpFormatVersion)
	}
	if e.consistency != "" {
		return fmt.Sprintf("This dump says its consistency is %q, which this version of hoarddb does not know. A newer hoarddb wrote it; use that version to read it.", e.consistency)
	}
	return fmt.Sprintf("This dump holds a record of kind %q, which this version of hoarddb does not know. A newer hoarddb wrote it; use that version to read it.", e.kind)
}

// Unwrap returns errDumpNewerVersion, so a caller can branch on the sentinel.
func (e newerVersionError) Unwrap() error { return errDumpNewerVersion }

// notADumpError is the refusal of a file whose first bytes are not the magic. Its text
// is the one errBadDumpMagic carries (section 3.1); the reader's own message adds the
// context an operator does not need, and the fix is what they do.
type notADumpError struct{}

func (notADumpError) Error() string { return errBadDumpMagic.Error() }

// Unwrap returns errBadDumpMagic, so a caller can branch on the F3 sentinel.
func (notADumpError) Unwrap() error { return errBadDumpMagic }

// verifyDump verifies the dump file at path and returns what it holds. It reads the file
// once, streaming, and never trusts the footer before it has read everything the footer
// covers (section 3.1): every record is strict-decoded, records, databases, buckets,
// entities and elements are counted, the sha256 of the magic through the summary is
// computed, and only then are the footer and the summary compared with what was read.
//
// The three sentinels errDumpIncomplete, errDumpDamaged and errDumpNewerVersion let a
// caller branch without reading the text; the text is what a user sees and is fixed by
// section 3.1. A file that does not start with the magic gives an error that matches
// errBadDumpMagic.
func verifyDump(path string) (dumpVerification, error) {
	file, err := os.Open(path)
	if err != nil {
		return dumpVerification{}, fmt.Errorf("opening the dump %s: %w", path, err)
	}
	verification, verifyErr := verifyDumpStream(file)
	closeErr := file.Close()
	if verifyErr != nil {
		// The verification's text is what the user must read, so a close error on a
		// read-only file does not bury it.
		return dumpVerification{}, verifyErr
	}
	if closeErr != nil {
		return dumpVerification{}, fmt.Errorf("closing the dump %s: %w", path, closeErr)
	}
	return verification, nil
}

// verifyDumpStream is verifyDump over an io.Reader, so a test can feed it a file's bytes
// without a path.
func verifyDumpStream(source io.Reader) (dumpVerification, error) {
	reader := newDumpReader(source)
	var (
		tally              dumpTally
		recordsRead        int64
		consistency        string
		summary            *dumpSummaryRecord
		summarySeen        bool
		footer             *dumpFooter
		footerSeen         bool
		coveredBytes       int64
		coveredSum         [32]byte
		formatDamage       bool
		strictDamage       bool
		unknownKind        string
		unknownConsistency string
		unknownVersion     int32
		versionUnknown     bool
		hasQueueField      bool
	)

	// damage is the damaged text with the range the checksum covers: the footer's
	// members once it was read, the summary's end once that was reached, and otherwise
	// the bytes read so far.
	damage := func() error {
		covered := reader.Offset()
		checksum := reader.Sum()
		if summarySeen {
			covered, checksum = coveredBytes, coveredSum
		}
		want := dumpNoReadableFooter
		if footer != nil {
			covered = footer.Bytes
			want = hex.EncodeToString(footer.SHA256)
		}
		return damagedDumpError{covered: covered, got: hex.EncodeToString(checksum[:]), want: want}
	}

	for {
		kind, raw, nextErr := reader.Next()
		if nextErr != nil {
			if footerSeen {
				// A clean file ends after its footer; anything else means bytes follow
				// the footer, even a byte too short to be a record.
				if errors.Is(nextErr, io.EOF) {
					break
				}
				return dumpVerification{}, damage()
			}
			switch {
			case errors.Is(nextErr, io.EOF), errors.Is(nextErr, io.ErrUnexpectedEOF):
				// The file ends, or a record's declared length runs past it, before any
				// footer: it was cut off.
				return dumpVerification{}, incompleteDumpError{records: recordsRead}
			case errors.Is(nextErr, errBadDumpMagic):
				return dumpVerification{}, notADumpError{}
			default:
				// errBadRecordLength or errRecordHasNoKind: the framing is lost, so no
				// footer is reachable and the bytes read so far are all that is known.
				return dumpVerification{}, damage()
			}
		}
		if footerSeen {
			return dumpVerification{}, damage()
		}
		recordsRead++
		if recordsRead == 1 && kind != dumpKindHeader {
			formatDamage = true
		}
		beforeSummary := !summarySeen
		if kind == dumpKindSummary {
			if summarySeen {
				formatDamage = true
			} else {
				summarySeen = true
				// The checksum covers the file through the end of the summary, so this
				// is the range the footer's bytes and sha256 members must match.
				coveredBytes = reader.Offset()
				coveredSum = reader.Sum()
			}
		}
		if beforeSummary && kind != dumpKindSummary && kind != dumpKindFooter {
			// Every record before the summary, the header included, is one the summary's
			// records member counts, whether or not it decodes.
			tally.records++
		}
		if !beforeSummary {
			// The record after the summary is the footer's place whatever its bytes now
			// say: a flipped byte in it is damage, and reading on to the end of the file
			// would call that file cut off.
			footerSeen = true
			if kind != dumpKindFooter {
				formatDamage = true
			}
		}

		record, decodeErr := decodeRecord(raw)
		if decodeErr != nil {
			switch {
			case errors.Is(decodeErr, errUnknownRecordKind):
				// A newer hoarddb may have written this kind. Remember the first one
				// and read on: only a file whose checksum and counts agree may blame it
				// on a newer version.
				if unknownKind == "" {
					var kindErr unknownRecordKindError
					if errors.As(decodeErr, &kindErr) {
						unknownKind = kindErr.word
					}
				}
			case errors.Is(decodeErr, errUnknownFormatVersion):
				var versionErr unknownFormatVersionError
				if !versionUnknown && errors.As(decodeErr, &versionErr) {
					unknownVersion, versionUnknown = versionErr.version, true
				}
			case errors.Is(decodeErr, errUnknownConsistency):
				if unknownConsistency == "" {
					var consistencyErr unknownConsistencyError
					if errors.As(decodeErr, &consistencyErr) {
						unknownConsistency = consistencyErr.word
					}
				}
			default:
				// The reader gave the whole record, so the framing still holds and the
				// read goes on; the damage is decided after the footer is checked.
				strictDamage = true
			}
			continue
		}

		if counter, counted := dumpRecordCounters[kind]; counted {
			counter(&tally, record)
		}
		switch record := record.(type) {
		case *dumpHeader:
			consistency = record.Consistency
		case *dumpSummaryRecord:
			summary = record
		case *dumpFooter:
			footer = record
			footerSeen = true
		case *dumpBucket:
			if dumpBucketHasQueueField(record) {
				hasQueueField = true
			}
		}
	}

	if formatDamage || strictDamage || summary == nil {
		// summary == nil is a summary record that did not decode, which is damage in
		// its own right and leaves no counters to compare.
		return dumpVerification{}, damage()
	}
	if footer.Bytes != coveredBytes || !bytes.Equal(footer.SHA256, coveredSum[:]) {
		return dumpVerification{}, damage()
	}
	if err := checkDumpCounts(summary, &tally); err != nil {
		return dumpVerification{}, err
	}
	if versionUnknown {
		return dumpVerification{}, newerVersionError{version: unknownVersion, versionUnknown: true}
	}
	if unknownKind != "" {
		return dumpVerification{}, newerVersionError{kind: unknownKind}
	}
	if unknownConsistency != "" {
		return dumpVerification{}, newerVersionError{consistency: unknownConsistency}
	}
	return dumpVerification{
		Consistency:   consistency,
		Records:       tally.records,
		Databases:     tally.databases,
		Buckets:       tally.buckets,
		Entities:      tally.entities,
		Elements:      tally.elements,
		HasQueueField: hasQueueField,
		SHA256:        coveredSum,
	}, nil
}

// checkDumpCounts compares the summary's five counters with what the read counted, in
// the order the summary lists them, and reports the first one that differs.
func checkDumpCounts(summary *dumpSummaryRecord, tally *dumpTally) error {
	counts := []struct {
		what  string
		said  int64
		holds int64
	}{
		{"records", summary.Records, tally.records},
		{"databases", summary.Databases, tally.databases},
		{"buckets", summary.Buckets, tally.buckets},
		{"entities", summary.Entities, tally.entities},
		{"elements", summary.Elements, tally.elements},
	}
	for _, count := range counts {
		if count.said != count.holds {
			return countMismatchError{said: count.said, what: count.what, holds: count.holds}
		}
	}
	return nil
}

// dumpBucketHasQueueField reports whether a bucket declares a fifoStore or lifoStore
// field. The word is the field's declared type, as a store field is spelled in a bucket
// declaration (rule 11).
func dumpBucketHasQueueField(bucket *dumpBucket) bool {
	for _, field := range bucket.Fields {
		if field.Type == dumpFIFOStoreWord || field.Type == dumpLIFOStoreWord {
			return true
		}
	}
	return false
}
