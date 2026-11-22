package cli

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"github.com/hoarddb/hoarddb-server/storage"
)

// marshalDumpTestRecord encodes a record the writer would write, as a reader gets it.
func marshalDumpTestRecord(t *testing.T, record any) bson.Raw {
	t.Helper()
	encoded, err := bson.Marshal(record)
	if err != nil {
		t.Fatalf("marshalling %T: %v", record, err)
	}
	return bson.Raw(encoded)
}

// dumpTestDocument is a small stored document, the payload of a doc or elems record.
func dumpTestDocument(t *testing.T, name string) bson.Raw {
	t.Helper()
	return marshalDumpTestRecord(t, bson.D{{Key: "name", Value: name}, {Key: "count", Value: int32(3)}})
}

// bsonDocumentOfSize returns the BSON document of one binary member whose encoded
// bytes are exactly size long.
func bsonDocumentOfSize(t *testing.T, size int) bson.Raw {
	t.Helper()
	empty, err := bson.Marshal(bson.D{{Key: "p", Value: []byte{}}})
	if err != nil {
		t.Fatalf("marshalling an empty payload: %v", err)
	}
	if size < len(empty) {
		t.Fatalf("a document of %d bytes is shorter than the %d bytes of its members", size, len(empty))
	}
	payload := make([]byte, size-len(empty))
	document, err := bson.Marshal(bson.D{{Key: "p", Value: payload}})
	if err != nil {
		t.Fatalf("marshalling a %d-byte payload: %v", size, err)
	}
	if len(document) != size {
		t.Fatalf("a document of %d requested bytes encoded to %d", size, len(document))
	}
	return bson.Raw(document)
}

// TestRecordsRoundTripThroughBSON pins the layout of format version 1 for every kind:
// each record type marshals, its first member is t, and decodeRecord returns a record
// equal to the one written. An empty fields, aggregates, unsupported or els array is a
// case of its own, because a nil slice would re-encode as BSON null.
func TestRecordsRoundTripThroughBSON(t *testing.T) {
	created := time.Date(2026, 11, 20, 9, 30, 15, 250_000_000, time.UTC)
	sha := bytes.Repeat([]byte{0x5a}, 32)
	records := []dumpRecord{
		&dumpHeader{
			T: dumpKindHeader, V: dumpFormatVersion, Created: created, Tool: "hoarddb 0.1.0",
			User: "alice", Scope: "all", Consistency: dumpConsistencyWord,
		},
		&dumpHeader{
			T: dumpKindHeader, V: dumpFormatVersion, Created: created, Tool: "hoarddb 0.1.0",
			User: "", Scope: "db:app/bucket:User", Consistency: dumpConsistencyWord,
		},
		&dumpDB{T: dumpKindDB, Name: "app", RF: 2, Storage: "disk"},
		&dumpBucket{
			T: dumpKindBucket, DB: "app", Name: "User",
			Fields: []dumpField{
				{Name: "Name", Type: "string"},
				{Name: "Purchases", Type: "fifoStore", ExpirePath: "at"},
				{Name: "Author", Type: "hashStore", Of: "Person"},
			},
			ExpireField: "seen",
			Aggregates: []dumpAggregate{
				{Store: "Purchases", Kind: "sum", Field: "amount"},
				{Store: "Purchases", Kind: "count by", By: "kind"},
			},
			Unsupported: []string{"Data"},
		},
		&dumpBucket{
			T: dumpKindBucket, DB: "app", Name: "Empty",
			Fields: []dumpField{}, Aggregates: []dumpAggregate{}, Unsupported: []string{},
		},
		&dumpDoc{
			T: dumpKindDoc, DB: "app", B: "User", ID: "507f1f77bcf86cd799439011",
			F: dumpTestDocument(t, "Alice"),
		},
		&dumpDoc{T: dumpKindDoc, DB: "app", B: "User", ID: "alice", F: bson.Raw([]byte{5, 0, 0, 0, 0})},
		&dumpElems{
			T: dumpKindElems, DB: "app", B: "User", ID: "alice", Field: "Purchases",
			Kind: "fifoStore", Seq: 0,
			Els: []bson.Raw{dumpTestDocument(t, "one"), dumpTestDocument(t, "two")},
		},
		&dumpElems{
			T: dumpKindElems, DB: "app", B: "User", ID: "alice", Field: "Friends",
			Kind: "hashStore", Seq: 1,
			Els:  []bson.Raw{dumpTestDocument(t, "bob")},
			Keys: []string{"bob"},
		},
		&dumpElems{
			T: dumpKindElems, DB: "app", B: "User", ID: "alice", Field: "Books",
			Kind: "heapStore", Seq: 0,
			Els:   []bson.Raw{dumpTestDocument(t, "dune")},
			Keys:  []string{"entry-1"},
			Prios: []int64{7},
		},
		&dumpElems{
			T: dumpKindElems, DB: "app", B: "User", ID: "alice", Field: "Photos",
			Kind: "lifoStore", Seq: 2, Els: []bson.Raw{},
		},
		&dumpSummaryRecord{
			T: dumpKindSummary, Records: 9, Databases: 1, Buckets: 2, Entities: 3, Elements: 4,
		},
		&dumpFooter{T: dumpKindFooter, Bytes: 4096, SHA256: sha},
	}
	for _, record := range records {
		raw := marshalDumpTestRecord(t, record)
		elements, err := raw.Elements()
		if err != nil {
			t.Fatalf("%s record: reading its members: %v", record.kindWord(), err)
		}
		if len(elements) == 0 || elements[0].Key() != dumpTypeMember {
			t.Fatalf("%s record does not start with the %q member: %s", record.kindWord(), dumpTypeMember, raw)
		}
		word, ok := elements[0].Value().StringValueOK()
		if !ok || word != record.kindWord() {
			t.Fatalf("%s record holds t = %v, want %q", record.kindWord(), elements[0].Value(), record.kindWord())
		}
		decoded, err := decodeRecord(raw)
		if err != nil {
			t.Fatalf("%s record: decoding: %v", record.kindWord(), err)
		}
		if !reflect.DeepEqual(decoded, record) {
			t.Fatalf("%s record decoded to %#v, want %#v", record.kindWord(), decoded, record)
		}
		again, err := bson.Marshal(decoded)
		if err != nil {
			t.Fatalf("%s record: re-encoding: %v", record.kindWord(), err)
		}
		if !bytes.Equal(again, raw) {
			t.Fatalf("%s record re-encoded to different bytes", record.kindWord())
		}
	}
}

// TestStrictDecodeRefusesAnExtraMissingRepeatedOrMistypedMember is the strict-layout
// rule of format version 1: a record has exactly its members, in order, each of the
// stated type. Every damaged footer below is a valid BSON document, so only the strict
// decode can refuse it.
func TestStrictDecodeRefusesAnExtraMissingRepeatedOrMistypedMember(t *testing.T) {
	sha := make([]byte, 32)
	for _, test := range []struct {
		reason string
		raw    bson.D
	}{
		{"an extra member", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "bytes", Value: int64(1)},
			{Key: "sha256", Value: sha},
			{Key: "note", Value: "extra"},
		}},
		{"a missing member", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "bytes", Value: int64(1)},
		}},
		{"a repeated member", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "bytes", Value: int64(1)},
			{Key: "sha256", Value: sha},
			{Key: "sha256", Value: sha},
		}},
		{"a member of the wrong BSON type", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "bytes", Value: "1"},
			{Key: "sha256", Value: sha},
		}},
		{"a binary subtype other than 0", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "bytes", Value: int64(1)},
			{Key: "sha256", Value: primitive.Binary{Subtype: 0x02, Data: sha}},
		}},
		{"a sha256 of the wrong length", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "bytes", Value: int64(1)},
			{Key: "sha256", Value: make([]byte, 31)},
		}},
		{"reordered members", bson.D{
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "sha256", Value: sha},
			{Key: "bytes", Value: int64(1)},
		}},
		// A null re-encodes to itself, so only the empty form given to a required
		// array makes this member's type visible to the re-encode check.
		{"a null where the format says an array", bson.D{
			{Key: dumpTypeMember, Value: dumpKindElems},
			{Key: "db", Value: "app"},
			{Key: "b", Value: "User"},
			{Key: "id", Value: "alice"},
			{Key: "field", Value: "Friends"},
			{Key: "kind", Value: "fifoStore"},
			{Key: "seq", Value: int32(0)},
			{Key: "els", Value: nil},
		}},
		{"t not as the first member", bson.D{
			{Key: "bytes", Value: int64(1)},
			{Key: dumpTypeMember, Value: dumpKindFooter},
			{Key: "sha256", Value: sha},
		}},
		{"t that is not a string", bson.D{
			{Key: dumpTypeMember, Value: int32(1)},
			{Key: "bytes", Value: int64(1)},
			{Key: "sha256", Value: sha},
		}},
		{"no members at all", bson.D{}},
	} {
		t.Run(test.reason, func(t *testing.T) {
			raw := marshalDumpTestRecord(t, test.raw)
			if _, err := decodeRecord(raw); err == nil {
				t.Fatalf("a footer with %s was accepted", test.reason)
			} else if errors.Is(err, errUnknownRecordKind) {
				t.Fatalf("a footer with %s was refused as an unknown kind: %v", test.reason, err)
			}
		})
	}
}

// TestStrictDecodeRefusesAnUnknownRecordKind pins the rule of rev 7.1: missing is not
// a kind of format version 1, and no other word is either, so both are refused and the
// refusal carries the word a newer version would have written.
func TestStrictDecodeRefusesAnUnknownRecordKind(t *testing.T) {
	for _, word := range []string{"missing", "bogus"} {
		t.Run(word, func(t *testing.T) {
			raw := marshalDumpTestRecord(t, bson.D{
				{Key: dumpTypeMember, Value: word},
				{Key: "db", Value: "app"},
			})
			_, err := decodeRecord(raw)
			if !errors.Is(err, errUnknownRecordKind) {
				t.Fatalf("a %q record gave %v, want errUnknownRecordKind", word, err)
			}
			if !strings.Contains(err.Error(), strconv.Quote(word)) {
				t.Fatalf("the refusal %q does not name the kind %q", err, word)
			}
		})
	}
}

// dumpTestRecordBytes is one encoded record with the kind word it starts with.
type dumpTestRecordBytes struct {
	kind string
	raw  bson.Raw
}

// largestDumpTestRecords builds the two records that hold the largest payload the ring
// accepts: a doc record whose f is a document of exactly
// config.MaxDocumentBytesCeiling encoded bytes, and an elems record holding one element
// of that size plus the longest element key. A bound derived from DefaultMaxDocumentBytes
// (24 MiB) instead of the ceiling (30 MiB) is too small for both, so a test built on
// them can fail where an assertion on the constant alone could not.
//
// The lengths are the ring's own maxima: namepolicy.MaxBytes (255) for a database and a
// bucket name (cluster/wire/names.go:databaseNameRefusal, server/name_limit.go:maxNameBytes),
// cli/ringid.go:ringMaxEncodedID (1024) for a document id, and storage.MaxKeyBytes (65000)
// for an element key, which bounds any hashStore or btreeStore key from above.
func largestDumpTestRecords(t *testing.T) []dumpTestRecordBytes {
	t.Helper()
	document := bsonDocumentOfSize(t, config.MaxDocumentBytesCeiling)
	longName := strings.Repeat("n", namepolicy.MaxBytes)
	longID := strings.Repeat("i", ringMaxEncodedID)
	longKey := strings.Repeat("k", storage.MaxKeyBytes)

	return []dumpTestRecordBytes{
		{dumpKindDoc, marshalDumpTestRecord(t, &dumpDoc{
			T: dumpKindDoc, DB: longName, B: longName, ID: longID, F: document,
		})},
		{dumpKindElems, marshalDumpTestRecord(t, &dumpElems{
			T: dumpKindElems, DB: longName, B: longName, ID: longID, Field: "Friends",
			Kind: "hashStore", Seq: 0, Els: []bson.Raw{document}, Keys: []string{longKey},
		})},
	}
}

// TestMaxDumpRecordBytesCoversTheLargestDocument makes the headroom falsifiable: both
// records of largestDumpTestRecords are within maxDumpRecordBytes. The reader half of
// the step's test is TestDumpReaderAcceptsTheLargestRecords, which reads the same two
// records back through the streaming reader.
func TestMaxDumpRecordBytesCoversTheLargestDocument(t *testing.T) {
	for _, test := range largestDumpTestRecords(t) {
		if len(test.raw) > maxDumpRecordBytes {
			t.Fatalf("a %s record of the largest document the ring accepts is %d bytes, over maxDumpRecordBytes (%d) by %d bytes",
				test.kind, len(test.raw), maxDumpRecordBytes, len(test.raw)-maxDumpRecordBytes)
		}
	}
}

// TestStrictDecodeRefusesAnUnknownConsistencyWord pins the one consistency word of
// format version 1: the withdrawn rev 3 to rev 5 words and any other are refused with
// errUnknownConsistency, and the refusal carries the word.
func TestStrictDecodeRefusesAnUnknownConsistencyWord(t *testing.T) {
	created := time.Date(2026, 11, 20, 9, 30, 15, 0, time.UTC)
	header := func(word string) bson.Raw {
		return marshalDumpTestRecord(t, bson.D{
			{Key: dumpTypeMember, Value: dumpKindHeader},
			{Key: "v", Value: int32(dumpFormatVersion)},
			{Key: "created", Value: created},
			{Key: "tool", Value: "hoarddb 0.1.0"},
			{Key: "user", Value: ""},
			{Key: "scope", Value: "all"},
			{Key: "consistency", Value: word},
		})
	}
	if _, err := decodeRecord(header(dumpConsistencyWord)); err != nil {
		t.Fatalf("the known consistency word was refused: %v", err)
	}
	for _, word := range []string{"entity-recheck-online", "entity-no-recheck-online", "bogus"} {
		t.Run(word, func(t *testing.T) {
			_, err := decodeRecord(header(word))
			if !errors.Is(err, errUnknownConsistency) {
				t.Fatalf("a header with consistency %q gave %v, want errUnknownConsistency", word, err)
			}
			if !strings.Contains(err.Error(), strconv.Quote(word)) {
				t.Fatalf("the refusal %q does not name the word %q", err, word)
			}
		})
	}
}

// TestDumpStatementsAreFixed pins the texts that dump, restore and restore --verify
// print, so that the words a user reads cannot drift away from section 3.1.
func TestDumpStatementsAreFixed(t *testing.T) {
	for _, test := range []struct {
		name string
		got  string
		want string
	}{
		{"dumpConsistencyWord", dumpConsistencyWord, "online-not-point-in-time"},
		{"dumpStartStatement", dumpStartStatement,
			"Dumping while the database stays online. This dump is not a point-in-time copy: each entity is read once, entities are read at different moments, and an entity that is written to while it is read can be saved half-updated. For a copy of one moment, stop the writers (or dump during a quiet period) first."},
		{"dumpEndStatementFormat", dumpEndStatementFormat,
			"Done: %s, %d entities, %d elements. This dump is not a point-in-time copy (see the start of this output); if writers were active, it may mix moments."},
		{"dumpQueueLeftOutLine", dumpQueueLeftOutLine,
			"Queue messages that are claimed or dead-lettered are not included in this dump. To include a claimed message, let its consumer acknowledge or release it before you dump."},
		{"restoreConsistencyStatement", restoreConsistencyStatement,
			"This dump was taken online and is not a point-in-time copy: entities were read at different moments, and an entity written to during the dump may be half-updated."},
		{"restoreQueueLeftOutLine", restoreQueueLeftOutLine,
			"Queue messages that were claimed or dead-lettered when the dump ran are not in this file, so they are not restored."},
	} {
		if test.got != test.want {
			t.Errorf("%s = %q, want %q", test.name, test.got, test.want)
		}
	}
	statement, known := consistencyStatement(dumpConsistencyWord)
	if !known || statement != restoreConsistencyStatement {
		t.Errorf("consistencyStatement(%q) = %q, %v; want the restore statement and true", dumpConsistencyWord, statement, known)
	}
	if statement, known := consistencyStatement("bogus"); known || statement != "" {
		t.Errorf("consistencyStatement(\"bogus\") = %q, %v; want no statement and false", statement, known)
	}
}
