package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

const (
	// scanKeptRefusals bounds the refusals and duplicates a scan remembers by position: the
	// first few are what a user fixes, the count says how many there are.
	scanKeptRefusals = 20
	// scanDuplicateIDLimit bounds the memory of the duplicate-_id check to 8 bytes of hash
	// per document and a map entry (about 40 MB at the limit). Past it the check stops and
	// the scan says so, because a duplicate only makes the later document replace the earlier
	// one, and that is not worth holding an unbounded set for.
	scanDuplicateIDLimit = 1_000_000
	// scanExtraNameLimit bounds the names counted beyond the 999 a bucket can hold, so a file
	// of random field names cannot grow the scan without bound while the refusal still names
	// a real count.
	scanExtraNameLimit = 100_000
)

// scanRefusal is one document pass 1 cannot load: it has no usable _id, or its bytes are not a
// document. The run decides whether that stops it (--on-error stop) or is skipped.
type scanRefusal struct {
	Ordinal int64
	Err     error
}

// text is the sentence for the report, naming the document by its position.
func (r scanRefusal) text(file string) string {
	var failure *documentFailure
	if errors.As(r.Err, &failure) {
		return fmt.Sprintf("Document %s of %s cannot be read: %s.", formatCount(r.Ordinal), file, strings.TrimSuffix(failure.Reason, "."))
	}
	return ingestIDRefusalText(r.Err, r.Ordinal, file)
}

// scanDuplicate is two documents of one file whose _id renders to the same text.
type scanDuplicate struct {
	First, Again int64
}

// text says both positions and which one survives.
func (d scanDuplicate) text(file string) string {
	return fmt.Sprintf("Warning: documents %s and %s of %s have the same _id. Ingest writes each document with an upsert, so the later one replaces the earlier.",
		formatCount(d.First), formatCount(d.Again), file)
}

// scanResult is what pass 1 learned: enough to declare a bucket and to tell the user what the
// run will skip, in memory bounded by the field names and the refusals kept.
type scanResult struct {
	// Documents is how many documents pass 2 will write; Refused how many it will not.
	Documents, Refused int64
	// Refusals are the first few refused documents by position.
	Refusals []scanRefusal
	// Fields and Order are the counters of each top-level field except _id, and the order
	// the fields were first seen in.
	Fields map[string]*planFieldStats
	Order  []string
	// FirstOf is, per field and type word, the ordinal of the first document that showed
	// that type, so a type mismatch can say where to look.
	FirstOf map[string]map[string]int64
	// Duplicates are the first few repeated _ids by position, DuplicateCount how many there
	// are, and DuplicateCheckStopped says the check hit scanDuplicateIDLimit.
	Duplicates            []scanDuplicate
	DuplicateCount        int64
	DuplicateCheckStopped bool
}

// fieldNameRefusal is the refusal of a top-level field name that no declaration can carry.
type fieldNameRefusal struct {
	Name string
	// Reason is what the ring's own encoder said about the name.
	Reason string
}

// Error states the facts without the file's name.
func (e *fieldNameRefusal) Error() string {
	return fmt.Sprintf("field %q cannot be declared on the ring: %s", e.Name, e.Reason)
}

// fileText is the refusal with the file named and the fix said (build plan 4.3).
func (e *fieldNameRefusal) fileText(file string) string {
	return fmt.Sprintf("Field %q in %s cannot be declared on the ring: %s. Nothing was written. Rename that field in the file and run ingest again.", e.Name, file, e.Reason)
}

// fileRefusal is a refusal of the whole file that needs the file's name to be said well.
type fileRefusal interface {
	error
	fileText(file string) string
}

// checkFieldName asks the ring's own request encoder whether a declaration may carry name,
// so the rule is the ringclient's and is not copied here. A name that passes it has been
// measured to be accepted by the server (cli/ringprobe_test.go:TestRingFieldNameLegality).
func checkFieldName(name string) *fieldNameRefusal {
	_, err := wire.EncodeRingAdmin(wire.RingAdminRequest{
		Cmd: wire.AdminCreateBucket, Database: "d", Name: "b", Fields: []wire.RingAdminField{{Name: name, Type: "any"}},
	})
	if err == nil {
		return nil
	}
	// The encoder numbers the field of a one-field request ("create_bucket: field 0 has ..."),
	// which means nothing to the user.
	reason := err.Error()
	if _, rest, found := strings.Cut(reason, "field 0 "); found {
		reason = "it " + rest
	}
	return &fieldNameRefusal{Name: name, Reason: strings.TrimSuffix(reason, ".")}
}

// scanFile is pass 1: it reads the whole file once, folds every top-level field except _id
// into its counters, and remembers which documents cannot be loaded. It writes nothing and
// holds one document plus the counters, so a file of any size costs memory in the number of
// distinct field names (at most 999 plus the bounded extras) and not in documents.
func scanFile(open func() (documentReader, io.Closer, error)) (scanResult, error) {
	reader, closer, err := open()
	if err != nil {
		return scanResult{}, err
	}
	defer func() { _ = closer.Close() }()

	scan := scanResult{Fields: map[string]*planFieldStats{}, FirstOf: map[string]map[string]int64{}}
	extraNames := map[string]struct{}{}
	seenIDs := map[uint64]int64{}
	for {
		next, err := reader.Next()
		var failure *documentFailure
		switch {
		case errors.Is(err, io.EOF):
			if total := len(scan.Order) + len(extraNames); total > ringMaxDeclaredFields {
				return scan, &tooManyFieldsError{Count: total, AtLeast: len(extraNames) >= scanExtraNameLimit}
			}
			return scan, nil
		case errors.As(err, &failure):
			scan.refuse(failure.Ordinal, failure)
			continue
		case err != nil:
			return scan, err
		}

		id, err := ingestID(next.Doc)
		if err != nil {
			scan.refuse(next.Ordinal, err)
			continue
		}
		scan.noteID(seenIDs, id, next.Ordinal)
		if err := scan.foldFields(next, extraNames); err != nil {
			return scan, err
		}
		scan.Documents++
	}
}

// refuse counts a refused document and keeps the first few by position.
func (s *scanResult) refuse(ordinal int64, reason error) {
	s.Refused++
	if len(s.Refusals) < scanKeptRefusals {
		s.Refusals = append(s.Refusals, scanRefusal{Ordinal: ordinal, Err: reason})
	}
}

// noteID records a rendered _id and counts a repeat.
func (s *scanResult) noteID(seen map[uint64]int64, id string, ordinal int64) {
	if s.DuplicateCheckStopped {
		return
	}
	key := xxhash.Sum64String(id)
	if first, repeated := seen[key]; repeated {
		s.DuplicateCount++
		if len(s.Duplicates) < scanKeptRefusals {
			s.Duplicates = append(s.Duplicates, scanDuplicate{First: first, Again: ordinal})
		}
		return
	}
	if len(seen) >= scanDuplicateIDLimit {
		s.DuplicateCheckStopped = true
		return
	}
	seen[key] = ordinal
}

// foldFields adds one valid document's fields to the counters. A name seen for the first time
// is checked against the ring's rule, so a file that cannot be declared is refused at its
// first bad name and nothing has been written.
func (s *scanResult) foldFields(document readDocument, extraNames map[string]struct{}) error {
	elements, err := document.Doc.Elements()
	if err != nil {
		return fmt.Errorf("reading the fields of document %d: %w", document.Ordinal, err)
	}
	for _, element := range elements {
		name := element.Key()
		if name == "_id" {
			continue
		}
		stats, known := s.Fields[name]
		if _, beyondCap := extraNames[name]; beyondCap {
			continue
		}
		if !known {
			if refusal := checkFieldName(name); refusal != nil {
				return refusal
			}
			if len(s.Order) >= ringMaxDeclaredFields {
				if len(extraNames) < scanExtraNameLimit {
					extraNames[name] = struct{}{}
				}
				continue
			}
			stats = newPlanFieldStats()
			s.Fields[name] = stats
			s.Order = append(s.Order, name)
		}
		if word := planTypeWord(element.Value().Type); stats.types[word] == 0 {
			if s.FirstOf[name] == nil {
				s.FirstOf[name] = map[string]int64{}
			}
			s.FirstOf[name][word] = document.Ordinal
		}
		if err := stats.observe(element.Value()); err != nil {
			return fmt.Errorf("reading field %q of document %d: %w", name, document.Ordinal, err)
		}
	}
	return nil
}
