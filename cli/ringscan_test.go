package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// bsonFileOf is the bytes of a .bson file: the documents back to back.
func bsonFileOf(t *testing.T, documents ...bson.D) []byte {
	t.Helper()
	var file bytes.Buffer
	for _, document := range documents {
		raw, err := bson.Marshal(document)
		if err != nil {
			t.Fatalf("marshal %v: %v", document, err)
		}
		file.Write(raw)
	}
	return file.Bytes()
}

// opensBSON is the open function scanFile and the sender take, over bytes in memory.
func opensBSON(content []byte) func() (documentReader, io.Closer, error) {
	return func() (documentReader, io.Closer, error) {
		reader := newBSONReader(bytes.NewReader(content), readerStart{name: "users.bson", totalBytes: int64(len(content))})
		return reader, io.NopCloser(nil), nil
	}
}

func opensJSONL(content string) func() (documentReader, io.Closer, error) {
	return func() (documentReader, io.Closer, error) {
		reader := newJSONLReader(strings.NewReader(content), readerStart{name: "users.jsonl", totalBytes: int64(len(content))})
		return reader, io.NopCloser(nil), nil
	}
}

// TestScanFileCountsFieldsAndTypesInOnePass: _id is not a field, every other top-level name is
// counted with the types it showed, in first-seen order.
func TestScanFileCountsFieldsAndTypesInOnePass(t *testing.T) {
	content := bsonFileOf(t,
		bson.D{{Key: "_id", Value: "a"}, {Key: "name", Value: "Ann"}, {Key: "age", Value: int32(30)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "name", Value: "Bo"}, {Key: "score", Value: 1.5}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "age", Value: int64(31)}, {Key: "name", Value: primitive.Null{}}},
	)
	scan, err := scanFile(opensBSON(content))
	if err != nil {
		t.Fatalf("scanFile: %v", err)
	}
	if scan.Documents != 3 {
		t.Errorf("documents = %d, want 3", scan.Documents)
	}
	if got := strings.Join(scan.Order, ","); got != "name,age,score" {
		t.Errorf("field order = %s, want name,age,score (first seen, _id left out)", got)
	}
	if _, present := scan.Fields["_id"]; present {
		t.Errorf("_id was counted as a field: it becomes _id_ and is never declared")
	}
	if name := scan.Fields["name"]; name.present != 3 || name.types["string"] != 2 || name.types["null"] != 1 {
		t.Errorf("name counters = %+v, want 3 present, 2 string, 1 null", name)
	}
	if age := scan.Fields["age"]; age.types["int"] != 2 {
		t.Errorf("age counters = %+v, want 2 int (int32 and int64 are one word)", age)
	}
}

// TestScanFileReportsABadDocumentWithItsPosition: a line that is not a document does not stop
// the scan, and its position is kept so the run can refuse or skip it.
func TestScanFileReportsABadDocumentWithItsPosition(t *testing.T) {
	scan, err := scanFile(opensJSONL("{\"_id\":\"a\",\"n\":1}\nthis is not json\n{\"_id\":\"c\",\"n\":3}\n"))
	if err != nil {
		t.Fatalf("scanFile: %v", err)
	}
	if scan.Documents != 2 || scan.Refused != 1 {
		t.Fatalf("documents = %d, refused = %d; want 2 and 1", scan.Documents, scan.Refused)
	}
	if len(scan.Refusals) != 1 || scan.Refusals[0].Ordinal != 2 {
		t.Fatalf("refusals = %+v, want one at document 2", scan.Refusals)
	}
	if text := scan.Refusals[0].text("users.jsonl"); !strings.Contains(text, "Document 2 of users.jsonl") {
		t.Errorf("refusal text %q does not name document 2 of the file", text)
	}
}

// TestScanFileReportsADocumentWithoutAnIDAndKeepsItsFieldsOut: a refused document cannot
// change the declaration.
func TestScanFileReportsADocumentWithoutAnIDAndKeepsItsFieldsOut(t *testing.T) {
	content := bsonFileOf(t,
		bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: int32(1)}},
		bson.D{{Key: "orphan", Value: true}},
	)
	scan, err := scanFile(opensBSON(content))
	if err != nil {
		t.Fatalf("scanFile: %v", err)
	}
	if scan.Refused != 1 || len(scan.Refusals) != 1 || !errors.Is(scan.Refusals[0].Err, errIngestNoID) {
		t.Fatalf("refusals = %+v, want one document without an _id", scan.Refusals)
	}
	if _, seen := scan.Fields["orphan"]; seen {
		t.Errorf("a field of a refused document was counted, so the declaration would hold a field no written document has")
	}
}

// TestScanFileReportsTwoDocumentsWithTheSameRenderedID: the same _id twice loads as one
// document (the later replaces the earlier), and the report must say so with both positions.
func TestScanFileReportsTwoDocumentsWithTheSameRenderedID(t *testing.T) {
	content := bsonFileOf(t,
		bson.D{{Key: "_id", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "x"}},
		bson.D{{Key: "_id", Value: 1.0}},
	)
	scan, err := scanFile(opensBSON(content))
	if err != nil {
		t.Fatalf("scanFile: %v", err)
	}
	if len(scan.Duplicates) != 1 || scan.DuplicateCount != 1 {
		t.Fatalf("duplicates = %+v (count %d), want one", scan.Duplicates, scan.DuplicateCount)
	}
	duplicate := scan.Duplicates[0]
	if duplicate.First != 1 || duplicate.Again != 3 {
		t.Errorf("duplicate positions = %d and %d, want 1 and 3 (int 1 and double 1.0 are one id by design)", duplicate.First, duplicate.Again)
	}
	text := duplicate.text("users.bson")
	if !strings.Contains(text, "documents 1 and 3 of users.bson") {
		t.Errorf("duplicate text %q does not name both positions", text)
	}
}

// TestScanFileRefusesAFieldNameTheRingCannotDeclare: the three cases of build plan 4.3, before
// anything is written. Every other name a file can carry (a dollar, a dot, __v) is accepted.
func TestScanFileRefusesAFieldNameTheRingCannotDeclare(t *testing.T) {
	tests := []struct {
		name  string
		field string
		want  string
	}{
		{"empty", "", "a name of 0 bytes and a field name is 1 to 255"},
		{"256 bytes", strings.Repeat("n", 256), "a name of 256 bytes and a field name is 1 to 255"},
		{"control character", "a\tb", "not printable UTF-8"},
		{"invalid UTF-8", "\xff\xfe", "not printable UTF-8"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			content := bsonFileOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: tc.field, Value: "x"}})
			_, err := scanFile(opensBSON(content))
			var refusal *fieldNameRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("scanFile: error %v, want *fieldNameRefusal", err)
			}
			text := refusal.fileText("users.bson")
			for _, want := range []string{"in users.bson cannot be declared on the ring", tc.want, "Nothing was written", "Rename that field in the file and run ingest again."} {
				if !strings.Contains(text, want) {
					t.Errorf("refusal %q does not say %q", text, want)
				}
			}
		})
	}
	for _, accepted := range []string{"__v", "a.b", "$x", "Ünï", "a b", strings.Repeat("n", 255)} {
		content := bsonFileOf(t, bson.D{{Key: "_id", Value: "a"}, {Key: accepted, Value: "x"}})
		if _, err := scanFile(opensBSON(content)); err != nil {
			t.Errorf("field name %q was refused: %v", accepted, err)
		}
	}
}

// TestScanFileRefusesMoreThanTheFieldCapWithTheExactCount: 1,204 names are counted, not just
// detected, so the text can say how far over the file is.
func TestScanFileRefusesMoreThanTheFieldCapWithTheExactCount(t *testing.T) {
	var document bson.D
	document = append(document, bson.E{Key: "_id", Value: "a"})
	for i := 0; i < 1204; i++ {
		document = append(document, bson.E{Key: fmt.Sprintf("f%04d", i), Value: "x"})
	}
	_, err := scanFile(opensBSON(bsonFileOf(t, document)))
	var tooMany *tooManyFieldsError
	if !errors.As(err, &tooMany) || tooMany.Count != 1204 {
		t.Fatalf("scanFile: error %v, want *tooManyFieldsError with 1204 names", err)
	}
}

// TestScanFileStopsOnAnInputThatCannotBeReadFurther: a truncated BSON file is the file's fault
// as a whole, not one document's.
func TestScanFileStopsOnAnInputThatCannotBeReadFurther(t *testing.T) {
	content := bsonFileOf(t, bson.D{{Key: "_id", Value: "a"}})
	content = append(content, 0xff, 0xff, 0xff, 0x7f, 0x01)
	_, err := scanFile(opensBSON(content))
	var corrupt *inputCorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("scanFile: error %v, want *inputCorruptError", err)
	}
}

// TestScanFileRemembersWhereEachTypeFirstAppeared: a type mismatch against an existing bucket
// names the first document that shows it, so the user can open the file there.
func TestScanFileRemembersWhereEachTypeFirstAppeared(t *testing.T) {
	content := bsonFileOf(t,
		bson.D{{Key: "_id", Value: "a"}, {Key: "age", Value: int32(1)}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "age", Value: int32(2)}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "age", Value: "three"}},
		bson.D{{Key: "_id", Value: "d"}, {Key: "age", Value: "four"}},
	)
	scan, err := scanFile(opensBSON(content))
	if err != nil {
		t.Fatalf("scanFile: %v", err)
	}
	if got := scan.FirstOf["age"]["string"]; got != 3 {
		t.Errorf("first string age at document %d, want 3", got)
	}
	if got := scan.FirstOf["age"]["int"]; got != 1 {
		t.Errorf("first int age at document %d, want 1", got)
	}
}
