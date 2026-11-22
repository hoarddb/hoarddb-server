package server

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Enforcement of a declaration on insert, put, push and batch insert
// (typed-fields spec §5.2, §5.8, §6; build doc TF-6 step 6.1).

// createTypedBucketOfType creates bucket of kind in database "app" with the
// given fields option and mode, and fails the test on a refusal.
func createTypedBucketOfType(t *testing.T, s *Server, bucket string, kind BucketType, fields bson.A, mode string) {
	t.Helper()
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	options := map[string]interface{}{"fields": fields}
	if mode != "" {
		options["field_types"] = mode
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: bucket, Type: kind, Options: options})
	if err != nil || !resp.Success {
		t.Fatalf("creating %s %s with typed fields: %+v, %v", kind, bucket, resp, err)
	}
}

// ageFields declares Age int and nothing else.
func ageFields() bson.A {
	return bson.A{bson.M{"name": "Age", "type": "int"}}
}

// requireTypeRefusal fails unless err is a FIELD_TYPE refusal saying every
// part.
func requireTypeRefusal(t *testing.T, what string, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s was stored; want it refused for its type", what)
		return
	}
	if !errors.Is(err, ErrFieldType) || codeFor(err) != transport.ErrFieldType {
		t.Errorf("%s was refused with %v (code %s); want FIELD_TYPE", what, err, codeFor(err))
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("%s was refused with %q; want it to say %q", what, err, part)
		}
	}
}

// TestPutOverwriteIsChecked: a put over an existing key is a whole document
// and is checked like an insert; the refused put leaves the stored one.
func TestPutOverwriteIsChecked(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", ageFields(), "")
	mustInsert(t, s, "app", "User", "k1", map[string]interface{}{"Age": int32(36)})
	_, err := s.Insert(InsertRequest{Database: "app", Bucket: "User", Key: "k1", Data: map[string]interface{}{"Age": "thirty"}})
	requireTypeRefusal(t, "a put over k1 with a string Age", err, `Field "Age"`, "is declared int", "is a string")
	if document := documentOf(t, s, "app", "User", "k1"); document["Age"] != int32(36) {
		t.Errorf("after the refused put k1 reads %v; want Age 36 kept", document)
	}
}

// TestPushToTypedFIFOLIFOAndHeapIsChecked: a push stores a whole document, so
// it is checked on every engine a push reaches; so is an insert into a queue
// or a heap.
func TestPushToTypedFIFOLIFOAndHeapIsChecked(t *testing.T) {
	s := newTestServer(t)
	for _, kind := range []BucketType{BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		bucket := "typed" + string(kind)
		createTypedBucketOfType(t, s, bucket, kind, ageFields(), "")
		_, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Data: map[string]interface{}{"Age": "thirty"}})
		requireTypeRefusal(t, "a "+string(kind)+" push of a string Age", err, `Field "Age"`, `bucket "`+bucket+`"`)
		_, err = s.Insert(InsertRequest{Database: "app", Bucket: bucket, Data: map[string]interface{}{"Age": 1.5}})
		requireTypeRefusal(t, "a "+string(kind)+" insert of a double Age", err, "is a double")
		if count := entriesOf(t, s, bucket); count != 0 {
			t.Errorf("%s bucket %s holds %d entries after two refused writes; want 0", kind, bucket, count)
		}
		if resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Data: map[string]interface{}{"Age": int64(36)}}); err != nil || !resp.Success {
			t.Errorf("a %s push of an int Age was refused: %+v %v", kind, resp, err)
		}
	}
	createTypedBucket(t, s, "hashed", ageFields(), "")
	_, err := s.Push(PushRequest{Database: "app", Bucket: "hashed", Data: map[string]interface{}{"Age": "thirty"}})
	requireTypeRefusal(t, "a hash push of a string Age", err, `Field "Age"`)
}

// entriesOf is a bucket's entry count in database "app", as describe reports it.
func entriesOf(t *testing.T, s *Server, bucket string) int64 {
	t.Helper()
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: bucket})
	if err != nil {
		t.Fatalf("describing %s: %v", bucket, err)
	}
	return stats.Entries
}

// TestBatchInsertRefusesTheWholeBatchNamingPositionAndKey (spec §5.2): the
// first mismatch refuses the whole batch, naming the document's 1-based
// position and its key; nothing of the batch is stored.
func TestBatchInsertRefusesTheWholeBatchNamingPositionAndKey(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", ageFields(), "")
	_, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "User", Data: []map[string]interface{}{
		{"_id_": "first", "Age": int32(1)}, {"_id_": "second", "Age": "two"}, {"_id_": "third", "Age": "three"},
	}})
	requireTypeRefusal(t, "a batch whose second document is mistyped", err,
		`Document 2 of the batch (_id_ "second"): Field "Age" in bucket "User" (database "app") is declared int`)
	if documentOf(t, s, "app", "User", "first") != nil {
		t.Error("the refused batch stored its first document")
	}
}

// TestStrictIsTheDefaultWhenTypesAreDeclared (spec §4.8): a declaration
// without field_types is strict, refuses a mismatch, and describe says so.
func TestStrictIsTheDefaultWhenTypesAreDeclared(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", ageFields(), "")
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "User"})
	if err != nil || stats.FieldTypes != "strict" {
		t.Fatalf("describe says field_types %q (%v); want strict", stats.FieldTypes, err)
	}
	_, err = s.Insert(InsertRequest{Database: "app", Bucket: "User", Key: "k", Data: map[string]interface{}{"Age": "thirty"}})
	requireTypeRefusal(t, "a string Age in a bucket with no field_types", err, `Field "Age"`)
	if documentOf(t, s, "app", "User", "k") != nil {
		t.Error("the refused insert was stored")
	}
}

// typedUserFields is the declaration the refusal texts are pinned against.
func typedUserFields() bson.A {
	return bson.A{
		bson.M{"name": "Name", "type": "string", "required": true},
		bson.M{"name": "Age", "type": "int"},
		bson.M{"name": "Small", "type": "int32"},
		bson.M{"name": "Score", "type": "double"},
		bson.M{"name": "Price", "type": "decimal"},
		bson.M{"name": "Ref", "type": "uuid"},
		bson.M{"name": "Born", "type": "datetime"},
	}
}

// TestRefusalTextsAreExact pins E-1 to E-5 and E-8 word for word (spec §6.2),
// with the widening rule: a numeric pair widens to number, anything else to
// any, and the received type's own word is suggested only when the bucket
// holds no documents (D-5). A required field keeps required in the alter it
// is offered. E-6 and E-7 (TF-7) are pinned on updates of the accepted document.
func TestRefusalTextsAreExact(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", typedUserFields(), "")
	createTypedBucket(t, s, "Fresh", typedUserFields(), "")
	mustInsert(t, s, "app", "User", "seed", map[string]interface{}{"Name": "Ada"})
	const uuidText = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"
	cases := []struct {
		name     string
		bucket   string
		document map[string]interface{}
		want     string
	}{
		{"E-1 widens to any", "User", map[string]interface{}{"Name": "Ada", "Age": "thirty"},
			`Field "Age" in bucket "User" (database "app") is declared int, but this document's Age is a string, so the document was not written.` + "\n" +
				`Send Age as int, for example {Age: 36}; or, to accept other values here, widen the declaration: alter bucket User { Age any }`},
		{"E-1 widens a numeric pair to number", "User", map[string]interface{}{"Name": "Ada", "Price": int32(10)},
			`Field "Price" in bucket "User" (database "app") is declared decimal, but this document's Price is a 32-bit integer, so the document was not written.` + "\n" +
				`Send Price as decimal, for example {Price: {$numberDecimal: "9.99"}}; or, to accept other values here, widen the declaration: alter bucket User { Price number }`},
		{"E-1 keeps required in the widening", "User", map[string]interface{}{"Name": int32(5)},
			`Field "Name" in bucket "User" (database "app") is declared string, but this document's Name is a 32-bit integer, so the document was not written.` + "\n" +
				`Send Name as string, for example {Name: "text"}; or, to accept other values here, widen the declaration: alter bucket User { Name any required }`},
		{"E-1 on an empty bucket names the received type", "Fresh", map[string]interface{}{"Name": "Ada", "Born": "yesterday"},
			`Field "Born" in bucket "Fresh" (database "app") is declared datetime, but this document's Born is a string, so the document was not written.` + "\n" +
				`Send Born as datetime, for example {Born: {$date: "2026-10-03T12:00:00Z"}}; or, since the bucket holds no documents yet, change the declaration: alter bucket Fresh { Born string }`},
		{"E-2", "User", map[string]interface{}{"Name": "Ada", "Small": int64(1) << 40},
			`Field "Small" in bucket "User" (database "app") is declared int32, but this document's Small is a 64-bit integer outside int32's range (-2147483648 to 2147483647), so the document was not written.` + "\n" +
				`To store 64-bit values, widen the declaration: alter bucket User { Small int }`},
		{"E-3", "User", map[string]interface{}{"Name": "Ada", "Score": int64(1) << 60},
			`Field "Score" in bucket "User" (database "app") is declared double, but this document's Score is a 64-bit integer larger than 2^53 in magnitude, and a double field accepts integers only up to 2^53, where every integer is held exactly. The document was not written.` + "\n" +
				`To accept it, widen the declaration: alter bucket User { Score number }`},
		{"E-4", "User", map[string]interface{}{"Name": "Ada", "Age": 36.0},
			`Field "Age" in bucket "User" (database "app") is declared int, but this document's Age is a double (a number written with a decimal point or exponent), so the document was not written.` + "\n" +
				`Send a whole number without a decimal point, for example {Age: 36}; or, to accept fractions too, widen the declaration: alter bucket User { Age number }`},
		{"E-4 into decimal", "User", map[string]interface{}{"Name": "Ada", "Price": 9.99},
			`Field "Price" in bucket "User" (database "app") is declared decimal, but this document's Price is a double, which cannot hold most decimal fractions exactly, so the document was not written.` + "\n" +
				`Send it as a decimal, for example {Price: {$numberDecimal: "9.99"}}; or, to accept doubles too, widen the declaration: alter bucket User { Price number }`},
		{"E-5 missing", "User", map[string]interface{}{"Age": int32(1)},
			`Field "Name" in bucket "User" (database "app") is required, and this document has no Name (or it is null), so the document was not written.` + "\n" +
				`Add Name to the document, for example {Name: "text"}; or make it optional: alter bucket User { Name string }`},
		{"E-5 null", "User", map[string]interface{}{"Name": nil},
			`Field "Name" in bucket "User" (database "app") is required, and this document has no Name (or it is null), so the document was not written.` + "\n" +
				`Add Name to the document, for example {Name: "text"}; or make it optional: alter bucket User { Name string }`},
		{"E-8", "User", map[string]interface{}{"Name": "Ada", "Ref": uuidText},
			`Field "Ref" in bucket "User" (database "app") is declared uuid, but this document's Ref is a string, so the document was not written.` + "\n" +
				`A UUID field holds a 16-byte UUID value, not its text: send {Ref: {$uuid: "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"}}. Document keys (_id_) are UUID text; to accept text here too, widen the declaration: alter bucket User { Ref any }`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Insert(InsertRequest{Database: "app", Bucket: tc.bucket, Key: "refused", Data: tc.document})
			if err == nil {
				t.Fatalf("stored; want %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Errorf("the refusal reads\n  %q\nwant\n  %q", err.Error(), tc.want)
			}
		})
	}
	// Every value the declaration accepts is stored.
	mustInsert(t, s, "app", "User", "every", map[string]interface{}{
		"Name": "Ada", "Age": int64(36), "Small": int64(7), "Score": int32(2), "Price": primitive.NewDecimal128(0, 999),
		"Ref": primitive.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}, "Born": primitive.DateTime(1759492800000),
	})

	// E-6 and E-7, on updates of that document (TF-7). On a multi-update the
	// key is the first refused document's (D-11).
	const unchangedRequired = "\nChange its value with $set instead, or make it optional first: alter bucket User { Name string }"
	const mulDouble = `$mul on field "Age" in bucket "User" (database "app") was refused: the product is a double (the factor is fractional, not a number, infinite, or outside int64's range), and Age is declared int. Nothing was changed.` + "\n" +
		`Multiply by a whole number within int64's range, set the computed whole number with $set, or widen the declaration: alter bucket User { Age number }`
	mustInsert(t, s, "app", "User", "moving", map[string]interface{}{"Name": "Ada", "Nick": "ada"})
	updates := []struct {
		name   string
		update UpdateRequest
		want   string
	}{
		{"E-6 $inc on a string", UpdateRequest{Key: "every", Inc: map[string]int64{"Name": 1}},
			`$inc on field "Name" in bucket "User" (database "app") was refused: Name is declared string, and $inc only applies to int32, int, double and number fields. Nothing was changed.` + "\n" +
				`To change it, use $set, for example: db.User.update({_id_: "every"}, {$set: {Name: "text"}})`},
		{"E-6 $mul on a datetime", UpdateRequest{Key: "every", Mul: map[string]float64{"Born": 2}},
			`$mul on field "Born" in bucket "User" (database "app") was refused: Born is declared datetime, and $mul only applies to int32, int, double and number fields. Nothing was changed.` + "\n" +
				`To change it, use $set, for example: db.User.update({_id_: "every"}, {$set: {Born: {$date: "2026-10-03T12:00:00Z"}}})`},
		{"E-6 on a decimal", UpdateRequest{Key: "every", Inc: map[string]int64{"Price": 1}},
			`$inc on field "Price" in bucket "User" (database "app") was refused: Price is declared decimal, and $inc only applies to int32, int, double and number fields; decimal arithmetic is not supported in this release. Nothing was changed.` + "\n" +
				`To change it, use $set, for example: db.User.update({_id_: "every"}, {$set: {Price: {$numberDecimal: "9.99"}}})`},
		{"E-6 on a multi-update", UpdateRequest{Multi: true, Filter: map[string]interface{}{"Age": int64(36)}, Inc: map[string]int64{"Name": 1}},
			`$inc on field "Name" in bucket "User" (database "app") was refused: Name is declared string, and $inc only applies to int32, int, double and number fields. Nothing was changed.` + "\n" +
				`To change it, use $set, for example: db.User.update({_id_: "every"}, {$set: {Name: "text"}})`},
		{"E-7 $unset", UpdateRequest{Key: "every", Unset: []string{"Name"}},
			`$unset of field "Name" in bucket "User" (database "app") was refused: Name is required. Nothing was changed.` + unchangedRequired},
		{"E-7 $rename", UpdateRequest{Key: "every", Rename: map[string]string{"Name": "FullName"}},
			`$rename of field "Name" away in bucket "User" (database "app") was refused: Name is required. Nothing was changed.` + unchangedRequired},
		{"E-7 $set to null", UpdateRequest{Key: "every", Set: map[string]interface{}{"Name": nil}},
			`setting required field "Name" to null in bucket "User" (database "app") was refused: Name is required. Nothing was changed.` + "\n" +
				`Set it to a string instead, for example {$set: {Name: "text"}}; or make it optional first: alter bucket User { Name string }`},
		{"E-6 $mul by a fractional factor", UpdateRequest{Key: "every", Mul: map[string]float64{"Age": 1.5}}, mulDouble},
		{"E-6 $mul by NaN", UpdateRequest{Key: "every", Mul: map[string]float64{"Age": math.NaN()}}, mulDouble},
		{"E-6 $mul by a factor past int64", UpdateRequest{Key: "every", Mul: map[string]float64{"Age": 1e19}}, mulDouble},
		{"E-1 $max operand", UpdateRequest{Key: "every", Max: map[string]interface{}{"Age": "zzz"}},
			`$max on field "Age" in bucket "User" (database "app") was refused: Age is declared int, but its operand is a string. Nothing was changed.` + "\n" +
				`Send Age as int, for example {$max: {Age: 36}}; or, to accept other values here, widen the declaration: alter bucket User { Age any }`},
		{"$rename result", UpdateRequest{Key: "moving", Rename: map[string]string{"Nick": "Age"}},
			`$rename onto field "Age" in bucket "User" (database "app") was refused: Age is declared int, but the value moved there is a string. Nothing was changed.` + "\n" +
				`Rename onto Age only a value it accepts, or set it directly: db.User.update({_id_: "moving"}, {$set: {Age: 36}}); or, to accept other values here, widen the declaration: alter bucket User { Age any }`},
		{"path through a declaration", UpdateRequest{Key: "every", Set: map[string]interface{}{"Born.Year": int32(1815)}},
			`$set of "Born.Year" in bucket "User" (database "app") was refused: Born is declared datetime, not a document, so it has no member Year. Nothing was changed.` + "\n" +
				`To replace it, set the whole value, for example: db.User.update({_id_: "every"}, {$set: {Born: {$date: "2026-10-03T12:00:00Z"}}}); or, to store members in it, widen the declaration: alter bucket User { Born any }`},
		{"$unset of a path through a declaration", UpdateRequest{Key: "every", Unset: []string{"Born.Year"}},
			`$unset of "Born.Year" in bucket "User" (database "app") was refused: Born is declared datetime, not a document, so it has no member Year. Nothing was changed.` + "\n" +
				`To remove the whole field, unset it: {$unset: ["Born"]}`},
	}
	for _, tc := range updates {
		t.Run(tc.name, func(t *testing.T) {
			tc.update.Database, tc.update.Bucket = "app", "User"
			_, err := s.Update(tc.update)
			if err == nil {
				t.Fatalf("stored; want %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Errorf("the refusal reads\n  %q\nwant\n  %q", err.Error(), tc.want)
			}
		})
	}
}

// TestRequiredAnyExampleIsAValue: any has no value of its own to show, so E-5's
// example for a required any field is a string, not a placeholder.
func TestRequiredAnyExampleIsAValue(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Loose", bson.A{bson.M{"name": "Tag", "type": "any", "required": true}}, "")
	_, err := s.Insert(InsertRequest{Database: "app", Bucket: "Loose", Key: "k", Data: map[string]interface{}{"Other": int32(1)}})
	requireTypeRefusal(t, "a document with no Tag", err, `for example {Tag: "text"}; or make it optional`)
}

// TestBatchWideningIsOfferedOnlyWhenItAdmitsTheBatch: the widening or change
// E-1 offers for one document is offered for a batch only when it would admit
// every document in it; otherwise the refusal names the positions it would
// refuse, since offering it would send the caller into the next refusal.
func TestBatchWideningIsOfferedOnlyWhenItAdmitsTheBatch(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Prices", bson.A{bson.M{"name": "Price", "type": "decimal"}}, "")
	createTypedBucket(t, s, "Counts", bson.A{bson.M{"name": "Count", "type": "int"}}, "")
	mustInsert(t, s, "app", "Prices", "seed", map[string]interface{}{"Price": primitive.NewDecimal128(0, 1)})
	cases := []struct {
		name         string
		bucket       string
		batch        []map[string]interface{}
		wantContains []string
		wantAbsent   []string
	}{
		{"number would admit the batch", "Prices",
			[]map[string]interface{}{{"_id_": "a", "Price": int32(10)}, {"_id_": "b", "Price": primitive.NewDecimal128(0, 5)}},
			[]string{"widen the declaration: alter bucket Prices { Price number }"}, nil},
		{"number would refuse a string in the batch", "Prices",
			[]map[string]interface{}{{"_id_": "a", "Price": int32(10)}, {"_id_": "b", "Price": "free"}},
			[]string{"would refuse document 2 of the batch", "Price number"}, []string{"widen the declaration: alter bucket Prices"}},
		{"the empty-bucket change would refuse the others", "Counts",
			[]map[string]interface{}{{"_id_": "a", "Count": int64(1)}, {"_id_": "b", "Count": "two"}, {"_id_": "c", "Count": int64(3)}},
			[]string{"would refuse documents 1 and 3 of the batch", "Count string"}, []string{"since the bucket holds no documents yet, change the declaration"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: tc.bucket, Data: tc.batch})
			if err == nil {
				t.Fatal("the batch was stored")
			}
			for _, want := range tc.wantContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not say %q", err, want)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(err.Error(), absent) {
					t.Errorf("the refusal %q offers %q, which would refuse the rest of the batch", err, absent)
				}
			}
		})
	}
}

// TestNowIsCheckedAsADatetime: now() is fixed at the door before the apply
// , so the check sees the datetime it became, not the
// sentinel document.
func TestNowIsCheckedAsADatetime(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "Events", bson.A{bson.M{"name": "When", "type": "datetime", "required": true}}, "")
	createTypedBucket(t, s, "Notes", bson.A{bson.M{"name": "When", "type": "string"}}, "")
	doorWrite(t, s, transport.OpPut, InsertRequest{Database: "app", Bucket: "Events", Key: "e1", Data: map[string]interface{}{"When": nowArg()}})
	if when, isDate := documentOf(t, s, "app", "Events", "e1")["When"].(primitive.DateTime); !isDate || when == 0 {
		t.Errorf("now() into a datetime field stored %v", documentOf(t, s, "app", "Events", "e1")["When"])
	}
	answer, err := s.fencedDispatch(t.Context(), &transport.Frame{Opcode: transport.OpPut, Payload: marshalRequest(t,
		InsertRequest{Database: "app", Bucket: "Notes", Key: "n1", Data: map[string]interface{}{"When": nowArg()}})})
	if err != nil {
		t.Fatalf("the put through the door: %v", err)
	}
	if refusal := refusalOf(t, answer); transport.ErrorCode(refusal.Code) != transport.ErrFieldType || !strings.Contains(refusal.Message, "is a datetime") {
		t.Errorf("now() into a string field was answered code %#x %q; want FIELD_TYPE naming a datetime", refusal.Code, refusal.Message)
	}
}

// TestOversizeIsRefusedBeforeType (spec §5.8): the door's size limit runs
// before the apply, so a document both too large and mistyped is refused for
// its size.
func TestOversizeIsRefusedBeforeType(t *testing.T) {
	const limit = 1 << 20
	s := newTestServer(t)
	s.config.Storage.MaxDocumentBytes = limit
	createTypedBucket(t, s, "User", ageFields(), "")
	oversized := docOfEncodedSize(limit + 64)
	oversized["Age"] = "thirty"
	refusal := refusalOf(t, sendFrame(t, NewHandler(s), transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "big", Data: oversized}))
	if transport.ErrorCode(refusal.Code) != transport.ErrInvalidArgument || !strings.Contains(refusal.Message, "limit") {
		t.Errorf("an oversized, mistyped document was answered code %#x %q; want the size refusal", refusal.Code, refusal.Message)
	}
}

// TestTypeIsRefusedBeforeDuplicateStandalone (spec §5.8): on a standalone
// node the type check runs before the engine, which holds the unique index,
// so a document both mistyped and a duplicate is refused for its type.
func TestTypeIsRefusedBeforeDuplicateStandalone(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{
		"fields":    bson.A{bson.M{"name": "Email", "type": "string"}, bson.M{"name": "Age", "type": "int"}},
		"placement": "whole",
		"indices":   []interface{}{map[string]interface{}{"field": "Email", "unique": true}},
	}})
	if err != nil || !resp.Success {
		t.Fatalf("creating User: %+v %v", resp, err)
	}
	mustInsert(t, s, "app", "User", "a", map[string]interface{}{"Email": "ada@example.com", "Age": int32(36)})
	_, err = s.Insert(InsertRequest{Database: "app", Bucket: "User", Key: "b", Data: map[string]interface{}{"Email": "ada@example.com", "Age": "thirty"}})
	requireTypeRefusal(t, "a mistyped duplicate", err, `Field "Age"`)
}

// TestDuplicateIsRefusedBeforeTypeOnAClusterWhenThePrecheckHits (spec §5.8,
// SA-6): on a cluster the owner's unique pre-check runs before the entry is
// logged, and it does not read types, so a document both mistyped and a
// duplicate is refused as a duplicate when the pre-check finds the holder.
func TestDuplicateIsRefusedBeforeTypeOnAClusterWhenThePrecheckHits(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{
		"fields":    bson.A{bson.M{"name": "Email", "type": "string"}, bson.M{"name": "Age", "type": "int"}},
		"placement": "whole",
		"indices":   []interface{}{map[string]interface{}{"field": "Email", "unique": true}},
	}})
	waitForServing(t, tc, testDatabase, "User", 3)
	ownerIndex := ownerIndexOf(t, tc, "User")
	tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "User", Key: "a",
		Data: map[string]interface{}{"Email": "ada@example.com", "Age": int32(36)}, WriteConcern: "all"})
	refusal := refusalOf(t, tc.frame(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "User", Key: "b",
		Data: map[string]interface{}{"Email": "ada@example.com", "Age": "thirty"}}))
	if transport.ErrorCode(refusal.Code) != transport.ErrDuplicateKey {
		t.Errorf("a mistyped duplicate was answered code %#x %q; want DUPLICATE_KEY from the pre-check", refusal.Code, refusal.Message)
	}
}
