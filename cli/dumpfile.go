package cli

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/config"
)

// dumpMagic starts every dump file. Its last byte is the layout generation, not the
// format version of the records: a later layout gets a new magic, so a reader can
// refuse a file it does not know before reading a single record.
const dumpMagic = "HDBDUMP1"

// dumpFormatVersion is the v member of the header record: the member set of the
// records below (the ring cli parity build spec section 3.1).
const dumpFormatVersion = 1

// maxDumpRecordBytes is the most bytes one record may hold and the most the reader
// will read for one. A record wraps one stored document, so the node's own document
// ceiling plus a megabyte for the record's own members bounds it. It is an expression,
// not a typed number, so a raised ceiling moves the bound with it.
const maxDumpRecordBytes = config.MaxDocumentBytesCeiling + (1 << 20)

// dumpRecordDepth is the nesting a record is checked against before it is unpacked.
// An element is an arbitrary stored document and the record wraps it two levels deep
// (the array, then the element), so the library's own ceiling is the most a reader
// can allow; it is also what the wire uses for a stored document.
const dumpRecordDepth = bson.MaxNestingCeiling

// dumpRecordSHA256Bytes is the length of the footer's sha256 member. The strict
// decoder checks it by hand because a shorter binary re-encodes to itself.
const dumpRecordSHA256Bytes = 32

// dumpTypeMember is the first member of every record: a string naming its kind.
const dumpTypeMember = "t"

// The seven kind words of format version 1, in the order the records are written.
// A missing-messages record is not one of them in this release (deferred with slice
// Q), so a reader treats it as an unknown kind.
const (
	dumpKindHeader  = "header"
	dumpKindDB      = "db"
	dumpKindBucket  = "bucket"
	dumpKindDoc     = "doc"
	dumpKindElems   = "elems"
	dumpKindSummary = "summary"
	dumpKindFooter  = "footer"
)

// errUnknownRecordKind is under the refusal of a record kind format version 1 does not
// have. A newer hoarddb may have written it, so the reader names it and refuses the
// whole file: skipping a record it does not understand would drop data silently.
var errUnknownRecordKind = errors.New("a dump record names a kind this version of hoarddb does not know")

// errUnknownConsistency is under the refusal of a header consistency word this version
// does not know. Like an unknown kind it is decided after the checksum is read, so the
// caller holds the word rather than blaming damage on a newer version.
var errUnknownConsistency = errors.New("a dump header names a consistency this version of hoarddb does not know")

// unknownRecordKindError carries the word of a record kind format version 1 does not
// have, as data and not only in the text: the verifier reads on to the end of the file
// and names the word only if the checksum and the counts agree (section 3.1, rev 7.1).
type unknownRecordKindError struct {
	word string
}

func (e unknownRecordKindError) Error() string {
	return fmt.Sprintf("%v: the record's kind is %q", errUnknownRecordKind, e.word)
}

// Unwrap returns errUnknownRecordKind, so a caller can branch on the sentinel.
func (e unknownRecordKindError) Unwrap() error { return errUnknownRecordKind }

// unknownConsistencyError carries the header consistency word this version does not
// know, for the same reason: the verifier decides after the whole file is read.
type unknownConsistencyError struct {
	word string
}

func (e unknownConsistencyError) Error() string {
	return fmt.Sprintf("%v: the header's consistency is %q", errUnknownConsistency, e.word)
}

// Unwrap returns errUnknownConsistency, so a caller can branch on the sentinel.
func (e unknownConsistencyError) Unwrap() error { return errUnknownConsistency }

// errUnknownFormatVersion is under the refusal of a header whose v member is not the
// format version this build reads. Like an unknown kind it is decided after the checksum
// is known, so a flipped byte in v is reported as damage and not as another version.
var errUnknownFormatVersion = errors.New("a dump header names a format version this version of hoarddb does not read")

// unknownFormatVersionError carries the header's v member as data, for the same reason
// as unknownRecordKindError.
type unknownFormatVersionError struct {
	version int32
}

func (e unknownFormatVersionError) Error() string {
	return fmt.Sprintf("%v: the header's v is %d", errUnknownFormatVersion, e.version)
}

// Unwrap returns errUnknownFormatVersion, so a caller can branch on the sentinel.
func (e unknownFormatVersionError) Unwrap() error { return errUnknownFormatVersion }

// dumpHeader is the first record of a dump: what wrote it, which scope it covers and
// which consistency promise it makes.
type dumpHeader struct {
	T           string    `bson:"t"`
	V           int32     `bson:"v"`
	Created     time.Time `bson:"created"`
	Tool        string    `bson:"tool"`
	User        string    `bson:"user"`
	Scope       string    `bson:"scope"`
	Consistency string    `bson:"consistency"`
}

func (*dumpHeader) kindWord() string { return dumpKindHeader }

// dumpDB is written before the first bucket of a database, with the declaration the
// restore must recreate.
type dumpDB struct {
	T       string `bson:"t"`
	Name    string `bson:"name"`
	RF      int32  `bson:"rf"`
	Storage string `bson:"storage"`
}

func (*dumpDB) kindWord() string { return dumpKindDB }

// dumpField is one field of a bucket, in declared order, store fields included.
type dumpField struct {
	Name       string `bson:"name"`
	Type       string `bson:"type"`
	Of         string `bson:"of,omitempty"`
	ExpirePath string `bson:"expire_path,omitempty"`
}

// dumpAggregate is one maintained aggregate of a store field.
type dumpAggregate struct {
	Store string `bson:"store"`
	Kind  string `bson:"kind"`
	Field string `bson:"field,omitempty"`
	By    string `bson:"by,omitempty"`
}

// dumpBucket is written before the first entity of a bucket. Unsupported names the
// fields whose payload is not dumped at all (every blobStore field).
type dumpBucket struct {
	T           string          `bson:"t"`
	DB          string          `bson:"db"`
	Name        string          `bson:"name"`
	Fields      []dumpField     `bson:"fields"`
	ExpireField string          `bson:"expire_field,omitempty"`
	Aggregates  []dumpAggregate `bson:"aggregates"`
	Unsupported []string        `bson:"unsupported"`
}

func (*dumpBucket) kindWord() string { return dumpKindBucket }

// dumpDoc is one entity's scalar fields and the id the ring holds them under.
type dumpDoc struct {
	T  string   `bson:"t"`
	DB string   `bson:"db"`
	B  string   `bson:"b"`
	ID string   `bson:"id"`
	F  bson.Raw `bson:"f"`
}

func (*dumpDoc) kindWord() string { return dumpKindDoc }

// dumpElems is one chunk of one store field of one entity, in the store's own
// StoreList order. Keys and Prios are present only for the kinds that have them: keys
// for a hashStore, a btreeStore and a heapStore, priorities for a heapStore.
type dumpElems struct {
	T     string     `bson:"t"`
	DB    string     `bson:"db"`
	B     string     `bson:"b"`
	ID    string     `bson:"id"`
	Field string     `bson:"field"`
	Kind  string     `bson:"kind"`
	Seq   int32      `bson:"seq"`
	Els   []bson.Raw `bson:"els"`
	Keys  []string   `bson:"keys,omitempty"`
	Prios []int64    `bson:"prios,omitempty"`
}

func (*dumpElems) kindWord() string { return dumpKindElems }

// dumpSummaryRecord is the second-to-last record: the counters the reader must match
// while it reads. It is named for the summary record and not for cli/dump.go's legacy
// run accumulator, which holds that name already.
type dumpSummaryRecord struct {
	T         string `bson:"t"`
	Records   int64  `bson:"records"`
	Databases int64  `bson:"databases"`
	Buckets   int64  `bson:"buckets"`
	Entities  int64  `bson:"entities"`
	Elements  int64  `bson:"elements"`
}

func (*dumpSummaryRecord) kindWord() string { return dumpKindSummary }

// dumpFooter is the last record: how many bytes the checksum covers and the checksum
// itself, of the file from the first byte of the magic through the end of the summary.
type dumpFooter struct {
	T      string `bson:"t"`
	Bytes  int64  `bson:"bytes"`
	SHA256 []byte `bson:"sha256"`
}

func (*dumpFooter) kindWord() string { return dumpKindFooter }

// dumpRecord is one record of a dump file, whatever its kind.
type dumpRecord interface {
	// kindWord is the t member's word, which is the record's row in section 3.1.
	kindWord() string
}

// dumpRecordKind is one row of the table of known kinds.
type dumpRecordKind struct {
	word      string
	newRecord func() dumpRecord
}

// dumpRecordKinds is every kind of format version 1. The order is the order the
// records are written in, and a lookup goes by the word.
var dumpRecordKinds = []dumpRecordKind{
	{word: dumpKindHeader, newRecord: func() dumpRecord { return &dumpHeader{} }},
	{word: dumpKindDB, newRecord: func() dumpRecord { return &dumpDB{} }},
	{word: dumpKindBucket, newRecord: func() dumpRecord { return &dumpBucket{} }},
	{word: dumpKindDoc, newRecord: func() dumpRecord { return &dumpDoc{} }},
	{word: dumpKindElems, newRecord: func() dumpRecord { return &dumpElems{} }},
	{word: dumpKindSummary, newRecord: func() dumpRecord { return &dumpSummaryRecord{} }},
	{word: dumpKindFooter, newRecord: func() dumpRecord { return &dumpFooter{} }},
}

// decodeRecord reads one record strictly: its first member is t, t is a string naming
// a known kind, and the record has exactly that kind's members, in order, each of the
// stated type, a binary member with subtype 0. It is strict by re-encoding what it
// decoded and refusing any byte difference, the way
// cluster/wire/ringreply.go:DecodeRingReply is: one rule catches a missing, extra,
// repeated, reordered or mistyped member and a binary subtype other than 0, where a
// hand-written check per member would miss one of them.
func decodeRecord(raw bson.Raw) (dumpRecord, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: dumpRecordDepth, Size: maxDumpRecordBytes}); err != nil {
		return nil, fmt.Errorf("a dump record is not a BSON document of a kind this dump holds: %w", err)
	}
	word, err := dumpRecordWord(raw)
	if err != nil {
		return nil, err
	}
	record, known := dumpRecordForKind(word)
	if !known {
		return nil, unknownRecordKindError{word: word}
	}
	if err := bson.Unmarshal(raw, record); err != nil {
		return nil, fmt.Errorf("a %q dump record does not decode as that kind: %w", word, err)
	}
	keepEmptyDumpArrays(record)
	again, err := bson.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("a %q dump record holds a value that cannot be read back: %w", word, err)
	}
	if !bytes.Equal(again, raw) {
		return nil, fmt.Errorf("a %q dump record does not have the layout of dump format version %d: its members must be the ones of that kind, in that order, each of the stated type, with a binary member's subtype 0 and no member missing, repeated, extra or mistyped", word, dumpFormatVersion)
	}
	switch record := record.(type) {
	case *dumpHeader:
		if record.V != dumpFormatVersion {
			return nil, unknownFormatVersionError{version: record.V}
		}
		if record.Consistency != dumpConsistencyWord {
			return nil, unknownConsistencyError{word: record.Consistency}
		}
	case *dumpFooter:
		if len(record.SHA256) != dumpRecordSHA256Bytes {
			return nil, fmt.Errorf("a dump footer holds a sha256 of %d bytes and a sha256 is %d bytes", len(record.SHA256), dumpRecordSHA256Bytes)
		}
	}
	return record, nil
}

// dumpRecordWord returns the t member of a record. t must be the first member and a
// string; anything else is damage, not a record of an unknown kind.
func dumpRecordWord(raw bson.Raw) (string, error) {
	elements, err := raw.Elements()
	if err != nil {
		return "", fmt.Errorf("a dump record's members cannot be read: %w", err)
	}
	if len(elements) == 0 {
		return "", fmt.Errorf("a dump record has no members and its first member must be %q", dumpTypeMember)
	}
	if elements[0].Key() != dumpTypeMember {
		return "", fmt.Errorf("a dump record's first member is %q and its first member must be %q", elements[0].Key(), dumpTypeMember)
	}
	value := elements[0].Value()
	if value.Type != bsontype.String {
		return "", fmt.Errorf("a dump record's %q member is of BSON type %s and it must be a string", dumpTypeMember, value.Type)
	}
	word, ok := value.StringValueOK()
	if !ok {
		return "", fmt.Errorf("a dump record's %q member is not a readable string", dumpTypeMember)
	}
	return word, nil
}

// dumpRecordForKind returns a fresh record of word's kind, and false when format
// version 1 has no such kind.
func dumpRecordForKind(word string) (dumpRecord, bool) {
	for _, kind := range dumpRecordKinds {
		if kind.word == word {
			return kind.newRecord(), true
		}
	}
	return nil, false
}

// keepEmptyDumpArrays gives a decoded record's required arrays their empty form. It
// exists for the null case, which the re-encode check alone cannot see: a null where
// the format says an array decodes to a nil slice, encodes back to null, and would be
// accepted as the same bytes. Giving the member its empty form makes that difference
// visible in the re-encode, so a reader refuses it as the wrong type. An empty array
// decodes to an empty slice already and passes through unchanged.
func keepEmptyDumpArrays(record dumpRecord) {
	switch record := record.(type) {
	case *dumpBucket:
		if record.Fields == nil {
			record.Fields = []dumpField{}
		}
		if record.Aggregates == nil {
			record.Aggregates = []dumpAggregate{}
		}
		if record.Unsupported == nil {
			record.Unsupported = []string{}
		}
	case *dumpElems:
		if record.Els == nil {
			record.Els = []bson.Raw{}
		}
	}
}

// The consistency of a dump, in one place: dump, restore and restore --verify print
// these words, and a test pins them. The dump is not a point-in-time copy, so no text
// here promises one; the advice is what an operator can do about it.
const (
	// dumpConsistencyWord is the header's consistency member: the only word format
	// version 1 knows. A later consistent dump would add a word, not a layout change.
	dumpConsistencyWord = "online-not-point-in-time"

	// dumpStartStatement is printed before the dump's first progress line.
	dumpStartStatement = "Dumping while the database stays online. This dump is not a point-in-time copy: each entity is read once, entities are read at different moments, and an entity that is written to while it is read can be saved half-updated. For a copy of one moment, stop the writers (or dump during a quiet period) first."

	// dumpEndStatementFormat is printed after the dump's final summary line, with the
	// file, the entity count and the element count.
	dumpEndStatementFormat = "Done: %s, %d entities, %d elements. This dump is not a point-in-time copy (see the start of this output); if writers were active, it may mix moments."

	// dumpQueueLeftOutLine follows the end statement when the dump holds a fifoStore
	// or lifoStore field: claimed and dead-lettered messages are in no list reply.
	dumpQueueLeftOutLine = "Queue messages that are claimed or dead-lettered are not included in this dump. To include a claimed message, let its consumer acknowledge or release it before you dump."

	// restoreConsistencyStatement is the first line restore and restore --verify print:
	// the statement the header's word maps to.
	restoreConsistencyStatement = "This dump was taken online and is not a point-in-time copy: entities were read at different moments, and an entity written to during the dump may be half-updated."

	// restoreQueueLeftOutLine is printed by restore and restore --verify when pass 1
	// saw a bucket with a fifoStore or lifoStore field.
	restoreQueueLeftOutLine = "Queue messages that were claimed or dead-lettered when the dump ran are not in this file, so they are not restored."
)

// consistencyStatement maps a dump's consistency word to the statement the tool prints
// about it, and reports whether this version knows the word. An unknown word is refused
// by the verifier with errUnknownConsistency rather than guessed at, so the mapping is
// total over the words a reader accepts.
func consistencyStatement(word string) (string, bool) {
	if word == dumpConsistencyWord {
		return restoreConsistencyStatement, true
	}
	return "", false
}
