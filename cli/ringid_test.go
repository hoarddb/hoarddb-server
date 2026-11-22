package cli

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// idDocument builds a one-member document {_id: value} the way a dump file holds it.
func idDocument(t *testing.T, value any) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: value}})
	if err != nil {
		t.Fatalf("marshal {_id: %T}: %v", value, err)
	}
	return raw
}

func mustIngestID(t *testing.T, value any) string {
	t.Helper()
	id, err := ingestID(idDocument(t, value))
	if err != nil {
		t.Fatalf("ingestID of %T %v: %v", value, value, err)
	}
	return id
}

// TestIngestIDRendersEveryType pins one golden string per row of the table in build plan 4.4.
func TestIngestIDRendersEveryType(t *testing.T) {
	objectID, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
	if err != nil {
		t.Fatal(err)
	}
	emptyDocument := bson.D{}
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"object id is its lowercase hex", objectID, "507f1f77bcf86cd799439011"},
		{"plain string stays bare", "alice", "alice"},
		{"string that begins with a tilde is escaped", "~x", "~s:~x"},
		{"string of 24 lowercase hex digits is escaped", "507f1f77bcf86cd799439011", "~s:507f1f77bcf86cd799439011"},
		{"int32", int32(1), "~i:1"},
		{"int64 negative", int64(-5), "~i:-5"},
		{"integral double is the integer", 1.0, "~i:1"},
		{"negative zero is the integer zero", math.Copysign(0, -1), "~i:0"},
		{"fractional double", 2.5, "~d:2.5"},
		{"double at 2^63 is not an integer", math.Ldexp(1, 63), "~d:9.223372036854776e+18"},
		{"bool true", true, "~b:true"},
		{"bool false", false, "~b:false"},
		{"datetime is epoch milliseconds", primitive.DateTime(1700000000000), "~t:1700000000000"},
		{"datetime before the epoch", primitive.DateTime(-1500), "~t:-1500"},
		{"null", primitive.Null{}, "~z:"},
		{"binary carries its subtype", primitive.Binary{Subtype: 0x04, Data: []byte{0x00, 0x11, 0x22, 0x33}}, "~y:04:00112233"},
		{"empty document keeps its raw bytes", emptyDocument, "~x:03:0500000000"},
		{"empty array keeps its raw bytes", bson.A{}, "~x:04:0500000000"},
		{"timestamp keeps its raw bytes", primitive.Timestamp{T: 1, I: 2}, "~x:11:0200000001000000"},
		{"undefined", primitive.Undefined{}, "~x:06:"},
		{"min key", primitive.MinKey{}, "~x:ff:"},
		{"max key", primitive.MaxKey{}, "~x:7f:"},
		{"NaN", math.NaN(), "~d:NaN"},
		{"positive infinity", math.Inf(1), "~d:+Inf"},
		{"negative infinity", math.Inf(-1), "~d:-Inf"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustIngestID(t, tc.value); got != tc.want {
				t.Errorf("_id %v renders %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

// TestIngestIDNeverCollidesAcrossTypes renders a corpus of ids of every type and asserts that
// two ids share a rendering only inside the two groups the plan designs: the integers with the
// integral doubles of one value, and the NaNs of different payloads.
func TestIngestIDNeverCollidesAcrossTypes(t *testing.T) {
	objectID, err := primitive.ObjectIDFromHex("507f1f77bcf86cd799439011")
	if err != nil {
		t.Fatal(err)
	}
	type corpusEntry struct {
		label string
		value any
		group string
	}
	corpus := []corpusEntry{
		{"object id", objectID, ""},
		{"the 24-hex string of that object id", "507f1f77bcf86cd799439011", ""},
		{"int32 1", int32(1), "one"},
		{"int64 1", int64(1), "one"},
		{"double 1.0", 1.0, "one"},
		{"string 1", "1", ""},
		{"string ~i:1", "~i:1", ""},
		{"string ~s:x", "~s:x", ""},
		{"string ~x", "~x", ""},
		{"string ~", "~", ""},
		{"empty string", "", ""},
		{"bool true", true, ""},
		{"bool false", false, ""},
		{"null", primitive.Null{}, ""},
		{"datetime", primitive.DateTime(1700000000000), ""},
		{"datetime zero", primitive.DateTime(0), ""},
		{"int 0", int64(0), "zero"},
		{"double 0.0", 0.0, "zero"},
		{"double -0.0", math.Copysign(0, -1), "zero"},
		{"binary", primitive.Binary{Subtype: 0, Data: []byte{0, 1}}, ""},
		{"string ~y:00:0001", "~y:00:0001", ""},
		{"double 2^63", math.Ldexp(1, 63), ""},
		{"double -2^63", math.Ldexp(-1, 63), "min"},
		{"int64 minimum", int64(math.MinInt64), "min"},
		{"int64 maximum", int64(math.MaxInt64), ""},
		{"double 1e19", 1e19, ""},
		{"double 2.5", 2.5, ""},
		{"NaN payload one", math.Float64frombits(0x7ff8000000000001), "nan"},
		{"NaN payload two", math.Float64frombits(0x7ff8000000000002), "nan"},
		{"NaN default", math.NaN(), "nan"},
		{"positive infinity", math.Inf(1), ""},
		{"negative infinity", math.Inf(-1), ""},
		{"string 23 lowercase hex", "507f1f77bcf86cd79943901", ""},
		{"string 25 lowercase hex", "507f1f77bcf86cd7994390112", ""},
		{"string 24 uppercase hex", "507F1F77BCF86CD799439011", ""},
		{"empty document", bson.D{}, ""},
		{"empty array", bson.A{}, ""},
		{"timestamp", primitive.Timestamp{T: 1, I: 2}, ""},
		{"undefined", primitive.Undefined{}, ""},
		{"min key", primitive.MinKey{}, ""},
		{"max key", primitive.MaxKey{}, ""},
		{"symbol", primitive.Symbol("s"), ""},
		{"javascript", primitive.JavaScript("1"), ""},
		{"alice", "alice", ""},
	}
	if len(corpus) < 40 {
		t.Fatalf("the corpus holds %d values and the plan asks for at least 40", len(corpus))
	}
	rendered := make([]string, len(corpus))
	for i, entry := range corpus {
		rendered[i] = mustIngestID(t, entry.value)
	}
	for i := range corpus {
		for j := i + 1; j < len(corpus); j++ {
			sameGroup := corpus[i].group != "" && corpus[i].group == corpus[j].group
			if rendered[i] == rendered[j] && !sameGroup {
				t.Errorf("%s and %s both render %q and they are not one designed group", corpus[i].label, corpus[j].label, rendered[i])
			}
			if rendered[i] != rendered[j] && sameGroup {
				t.Errorf("%s (%q) and %s (%q) are one designed group and must render alike", corpus[i].label, rendered[i], corpus[j].label, rendered[j])
			}
		}
	}
}

// TestIngestIDOfAStringThatLooksLikeAnObjectIDIsEscaped: a collection may hold an ObjectId and
// a string of the same 24 digits, and they must stay two documents.
func TestIngestIDOfAStringThatLooksLikeAnObjectIDIsEscaped(t *testing.T) {
	const digits = "507f1f77bcf86cd799439011"
	objectID, err := primitive.ObjectIDFromHex(digits)
	if err != nil {
		t.Fatal(err)
	}
	fromObjectID := mustIngestID(t, objectID)
	fromString := mustIngestID(t, digits)
	if fromObjectID != digits {
		t.Errorf("the ObjectId renders %q, want its bare hex %q", fromObjectID, digits)
	}
	if fromString != "~s:"+digits {
		t.Errorf("the string of the same digits renders %q, want %q", fromString, "~s:"+digits)
	}
}

// TestIngestIDRefusesAMissingID: ingest never invents an id, because running the file again
// would then add the document again.
func TestIngestIDRefusesAMissingID(t *testing.T) {
	raw, err := bson.Marshal(bson.D{{Key: "name", Value: "alice"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingestID(raw)
	if !errors.Is(err, errIngestNoID) {
		t.Fatalf("a document with no _id: error %v, want errIngestNoID", err)
	}
	text := ingestIDRefusalText(err, 17, "users.bson")
	for _, want := range []string{"Document 17 of users.bson has no _id", "will not invent one", "Give every document an _id"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal %q does not say %q", text, want)
		}
	}
}

// TestIngestIDRefusesAnOverlongRenderingAndNamesTheDocument: the ring takes an id whose
// encoding is at most 1024 bytes (cluster/wire/delete_reclaim.go:maxEncodedID), and a refusal
// names the document and the real limit.
func TestIngestIDRefusesAnOverlongRenderingAndNamesTheDocument(t *testing.T) {
	atLimit := strings.Repeat("a", ringMaxEncodedID-3)
	if _, err := ingestID(idDocument(t, atLimit)); err != nil {
		t.Fatalf("an id that encodes to exactly %d bytes was refused: %v", ringMaxEncodedID, err)
	}
	_, err := ingestID(idDocument(t, atLimit+"a"))
	var tooLong *ingestIDTooLongError
	if !errors.As(err, &tooLong) {
		t.Fatalf("an id one byte over the limit: error %v, want *ingestIDTooLongError", err)
	}
	text := ingestIDRefusalText(err, 17, "users.bson")
	for _, want := range []string{"Document 17 of users.bson has an _id that renders to a text of 1,022 bytes", "which encodes to 1,025 bytes", "at most 1,024 bytes", "up to 1,021 bytes", "Change the _id in the file"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal %q does not say %q", text, want)
		}
	}
	// A binary id doubles in hex, so about 500 raw bytes is already too long.
	_, err = ingestID(idDocument(t, primitive.Binary{Subtype: 0, Data: make([]byte, 600)}))
	if !errors.As(err, &tooLong) {
		t.Errorf("a 600-byte binary id: error %v, want *ingestIDTooLongError", err)
	}
}

// TestIngestIDOfADoubleAtTheInt64Boundary: 2^63 is outside int64 and renders as a double;
// -2^63 equals the int64 minimum by design and is the only double that does.
func TestIngestIDOfADoubleAtTheInt64Boundary(t *testing.T) {
	if got := mustIngestID(t, math.Ldexp(1, 63)); got != "~d:9.223372036854776e+18" {
		t.Errorf("2^63 renders %q, want a ~d: id", got)
	}
	minimum := mustIngestID(t, int64(math.MinInt64))
	if got := mustIngestID(t, math.Ldexp(-1, 63)); got != minimum || minimum != "~i:-9223372036854775808" {
		t.Errorf("-2^63 renders %q and the int64 minimum %q, want both ~i:-9223372036854775808", got, minimum)
	}
	if got := mustIngestID(t, 1e19); !strings.HasPrefix(got, "~d:") {
		t.Errorf("1e19 renders %q, want a ~d: id", got)
	}
}

// TestIngestIDOfNaNsWithDifferentPayloadsIsOneID: every NaN is one id, the second designed
// group.
func TestIngestIDOfNaNsWithDifferentPayloadsIsOneID(t *testing.T) {
	first := mustIngestID(t, math.Float64frombits(0x7ff8000000000001))
	second := mustIngestID(t, math.Float64frombits(0xfff8000000000042))
	if first != "~d:NaN" || second != "~d:NaN" {
		t.Errorf("NaNs render %q and %q, want ~d:NaN for both", first, second)
	}
}

// TestIngestIDOfTheEmptyStringRoundTrips: a document whose _id is the empty string is written
// and read back under the same id, and is not given a minted one (build plan 7.4 item 21).
func TestIngestIDOfTheEmptyStringRoundTrips(t *testing.T) {
	if got := mustIngestID(t, ""); got != "" {
		t.Fatalf("the empty string renders %q, want the empty string", got)
	}
	_, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "users", []ringclient.FieldDecl{{Name: "name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	bucket := client.Bucket("app", "users")
	if _, err := bucket.Upsert(ctx, "", ringclient.Doc{"name": "nobody"}); err != nil {
		t.Fatalf("upsert under the empty id: %v", err)
	}
	if _, err := bucket.Get(ctx, ""); err != nil {
		t.Fatalf("get under the empty id: %v", err)
	}
	page, err := bucket.List(ctx, nil, ringclient.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].ID != "" {
		t.Errorf("list after one upsert under the empty id: %d entries, first id %q; want one entry with the empty id", len(page.Entries), firstEntryID(page))
	}
}

func firstEntryID(page ringclient.EntryPage) string {
	if len(page.Entries) == 0 {
		return "<none>"
	}
	return page.Entries[0].ID
}

// TestIngestIDLimitMatchesTheRing writes the longest id ingestID accepts to a live ring and
// shows that one byte more is refused by the ring itself, so the copied limit cannot drift
// from cluster/wire/delete_reclaim.go:maxEncodedID without this test failing.
func TestIngestIDLimitMatchesTheRing(t *testing.T) {
	_, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "users", []ringclient.FieldDecl{{Name: "name", Type: "string"}}); err != nil {
		t.Fatal(err)
	}
	bucket := client.Bucket("app", "users")
	longest := strings.Repeat("a", ringMaxEncodedID-3)
	if _, err := bucket.Upsert(ctx, longest, ringclient.Doc{"name": "x"}); err != nil {
		t.Fatalf("the longest id ingestID accepts (%d bytes encoded) was refused by the ring: %v", ringMaxEncodedID, err)
	}
	if _, err := bucket.Upsert(ctx, longest+"a", ringclient.Doc{"name": "x"}); err == nil {
		t.Errorf("an id one byte over ringMaxEncodedID was accepted by the ring, so the copied limit is too low")
	}
}
