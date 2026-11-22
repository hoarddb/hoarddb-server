package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Enforcement of a declaration on updates (typed-fields spec §4.5.6, §5.4,
// E-6, E-7; build doc TF-7). The static checks run before any operator,
// against the declaration node at each key's path; the result checks run
// after the operators, on what only the result tells.

// updateFields is the declaration the update tests run against.
func updateFields() bson.A {
	return bson.A{
		bson.M{"name": "Name", "type": "string", "required": true},
		bson.M{"name": "Age", "type": "int"},
		bson.M{"name": "Small", "type": "int32"},
		bson.M{"name": "Score", "type": "double"},
		bson.M{"name": "Price", "type": "decimal"},
		bson.M{"name": "Tags", "type": "array", "elem": bson.M{"type": "string"}},
		bson.M{"name": "Address", "type": "object"},
		bson.M{"name": "Born", "type": "datetime"},
	}
}

// updateSeed is a document updateFields accepts.
func updateSeed() map[string]interface{} {
	price, _ := primitive.ParseDecimal128("9.99")
	return map[string]interface{}{
		"Name": "Ada", "Age": int32(36), "Small": int32(1), "Score": 2.5, "Price": price,
		"Tags": bson.A{"a"}, "Address": map[string]interface{}{"City": "Tirana"},
	}
}

// typedUpdateServer is a server with bucket User in database "app" declaring
// updateFields in mode, holding updateSeed under key "u".
func typedUpdateServer(t *testing.T, mode string) *Server {
	t.Helper()
	s := newTestServer(t)
	createTypedBucket(t, s, "User", updateFields(), mode)
	mustInsert(t, s, "app", "User", "u", updateSeed())
	return s
}

// typedUpdate applies an update to User's document u and returns its error;
// an update that answered without an error must have succeeded.
func typedUpdate(t *testing.T, s *Server, req UpdateRequest) error {
	t.Helper()
	req.Database, req.Bucket = "app", "User"
	if req.Key == "" && !req.Multi {
		req.Key = "u"
	}
	resp, err := s.Update(req)
	if err == nil && !resp.Success {
		t.Fatalf("the update answered %+v without an error", resp)
	}
	return err
}

// requireUnchanged fails unless User's document u is still updateSeed.
func requireUnchanged(t *testing.T, s *Server, what string) {
	t.Helper()
	stored := documentOf(t, s, "app", "User", "u")
	delete(stored, "_id_")
	if !equalDocuments(normalized(t, stored), normalized(t, updateSeed())) {
		t.Errorf("after %s, u reads %v; want it unchanged", what, stored)
	}
}

// normalized is a document as it reads back after a round trip through BSON.
func normalized(t *testing.T, document map[string]interface{}) map[string]interface{} {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("encoding %v: %v", document, err)
	}
	var decoded map[string]interface{}
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decoding %v: %v", document, err)
	}
	return decoded
}

// TestIncOnADeclaredStringIsRefused (spec §5.4 item 2, F-5): $inc on a field
// declared string is refused before the operator runs, as a FIELD_TYPE
// refusal, and the string is unchanged.
func TestIncOnADeclaredStringIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Name": 1}})
	requireTypeRefusal(t, "$inc on Name string", err, `$inc on field "Name"`, "Name is declared string")
	requireUnchanged(t, s, "the refused $inc")
}

// TestIncOnAnUndeclaredStringAnswersInvalidArgument is the two-codes rule
// (spec §5.4): on a field the bucket does not declare, the operand
// refusal stands, with its code.
func TestIncOnAnUndeclaredStringAnswersInvalidArgument(t *testing.T) {
	s := typedUpdateServer(t, "")
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Nick": "ada"}}); err != nil {
		t.Fatalf("setting the undeclared Nick: %v", err)
	}
	err := typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Nick": 1}})
	if !errors.Is(err, ErrUpdateOperand) || errors.Is(err, ErrFieldType) || codeFor(err) != transport.ErrInvalidArgument {
		t.Errorf("$inc on the undeclared string Nick answered %v (code %s); want INVALID_ARGUMENT", err, codeFor(err))
	}
}

// TestIncAndMulOnDecimalAreRefused (OC-5): decimal arithmetic is not
// supported in this release, and on a declared decimal the refusal says so.
func TestIncAndMulOnDecimalAreRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Price": 1}})
	requireTypeRefusal(t, "$inc on Price decimal", err, `$inc on field "Price"`, "decimal arithmetic is not supported in this release")
	err = typedUpdate(t, s, UpdateRequest{Mul: map[string]float64{"Price": 2}})
	requireTypeRefusal(t, "$mul on Price decimal", err, `$mul on field "Price"`, "decimal arithmetic is not supported in this release")
	requireUnchanged(t, s, "the refused $inc and $mul")
}

// TestMaxWithAWrongTypedOperandIsRefused (D-4): without the static check,
// $max of a string over an int is a silent no-op, because the two do not
// compare and the stored value still conforms.
func TestMaxWithAWrongTypedOperandIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Max: map[string]interface{}{"Age": "zzz"}})
	requireTypeRefusal(t, "$max of a string on Age int", err, `$max on field "Age"`, "Age is declared int", "a string")
	err = typedUpdate(t, s, UpdateRequest{Min: map[string]interface{}{"Age": 1.5}})
	requireTypeRefusal(t, "$min of a double on Age int", err, `$min on field "Age"`, "a double")
	requireUnchanged(t, s, "the refused $max and $min")
}

// TestAddToSetOfAPresentWrongTypedValueIsRefused (D-5): the pushed operand is
// checked against the element type, even when $addToSet would find it
// already present and change nothing. The present wrong element was stored
// while elements went unchecked (until TF-9 checks them on insert).
func TestAddToSetOfAPresentWrongTypedValueIsRefused(t *testing.T) {
	s := newTestServer(t)
	createTypedBucket(t, s, "User", updateFields(), "")
	seed := updateSeed()
	seed["Tags"] = bson.A{int32(5)}
	mustInsert(t, s, "app", "User", "u", seed)
	err := typedUpdate(t, s, UpdateRequest{AddToSet: map[string]interface{}{"Tags": int32(5)}})
	requireTypeRefusal(t, "$addToSet of a present int into Tags array<string>", err,
		`$addToSet on field "Tags[1]"`, "is declared string (an element of Tags: array<string>)", "a 32-bit integer")
	err = typedUpdate(t, s, UpdateRequest{Push: map[string]interface{}{"Tags": int32(6)}})
	requireTypeRefusal(t, "$push of an int onto Tags array<string>", err, `$push on field "Tags[1]"`)
	if err := typedUpdate(t, s, UpdateRequest{Push: map[string]interface{}{"Tags": "b"}}); err != nil {
		t.Errorf("$push of a string onto Tags array<string> was refused: %v", err)
	}
}

// TestPushOntoADeclaredScalarIsRefused: $push and $addToSet turn a value
// into an array (server/manage.go:arrayForAppend), so on a field declared
// anything but array or any they are refused.
func TestPushOntoADeclaredScalarIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Push: map[string]interface{}{"Age": int32(1)}})
	requireTypeRefusal(t, "$push onto Age int", err, `$push on field "Age"`, "Age is declared int", "only applies to array fields")
	err = typedUpdate(t, s, UpdateRequest{AddToSet: map[string]interface{}{"Address": "x"}})
	requireTypeRefusal(t, "$addToSet onto Address object", err, `$addToSet on field "Address"`)
	requireUnchanged(t, s, "the refused $push and $addToSet")
}

// TestUnsetOfARequiredFieldIsRefused (E-7).
func TestUnsetOfARequiredFieldIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Unset: []string{"Name"}})
	requireTypeRefusal(t, "$unset of the required Name", err, `$unset of field "Name"`, "Name is required")
	requireUnchanged(t, s, "the refused $unset")
	if err := typedUpdate(t, s, UpdateRequest{Unset: []string{"Age"}}); err != nil {
		t.Errorf("$unset of the optional Age was refused: %v", err)
	}
}

// TestRenameOfARequiredFieldIsRefused (E-7): a rename moves the field away.
func TestRenameOfARequiredFieldIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Rename: map[string]string{"Name": "FullName"}})
	requireTypeRefusal(t, "$rename of the required Name", err, `$rename of field "Name" away`, "Name is required")
	requireUnchanged(t, s, "the refused $rename")
}

// TestSetRequiredToNullIsRefused (E-7): null is a value, and a required field
// refuses it as it refuses a missing one.
func TestSetRequiredToNullIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Name": nil}})
	requireTypeRefusal(t, "$set of the required Name to null", err, `setting required field "Name" to null`)
	requireUnchanged(t, s, "the refused $set")
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Age": nil}}); err != nil {
		t.Errorf("$set of the optional Age to null was refused: %v", err)
	}
}

// TestSetOfAWrongTypedOperandIsRefused: $set's operand is checked against
// the field's node (spec §5.4 item 3), with the widening a refusal offers.
func TestSetOfAWrongTypedOperandIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Small": int64(1) << 40}})
	requireTypeRefusal(t, "$set of a large int64 on Small int32", err, `$set on field "Small"`, "outside int32's range")
	err = typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Born": "yesterday"}})
	requireTypeRefusal(t, "$set of a string on Born datetime", err, `$set on field "Born"`, "alter bucket User { Born any }")
	requireUnchanged(t, s, "the refused $set")
}

// TestPathThroughADeclaredScalarIsRefused: a path whose first name is
// declared as something that holds no members (or no positions) either finds
// a stored value it cannot enter, or creates a document where the
// declaration wants a scalar. Either way the result is not the declared type,
// so the update is refused for its type before any operator runs. A path
// into a declared object is written: its members are not declared (until
// TF-9).
func TestPathThroughADeclaredScalarIsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	if err := typedUpdate(t, s, UpdateRequest{Unset: []string{"Born"}}); err != nil {
		t.Fatalf("unsetting Born: %v", err)
	}
	for _, refused := range []struct {
		what string
		req  UpdateRequest
		says string
	}{
		{"$set of Born.Year, Born absent", UpdateRequest{Set: map[string]interface{}{"Born.Year": int32(1815)}},
			`$set of "Born.Year" in bucket "User" (database "app") was refused: Born is declared datetime, not a document, so it has no member Year.`},
		{"$inc of Age.Months, Age an int", UpdateRequest{Inc: map[string]int64{"Age.Months": 1}}, "Age is declared int, not a document"},
		{"$set of Tags.First, Tags an array", UpdateRequest{Set: map[string]interface{}{"Tags.First": "a"}}, "Tags is declared array<string>, not a document"},
		{"$set of Address[0], Address an object", UpdateRequest{Set: map[string]interface{}{"Address[0]": "a"}}, "Address is declared object, not an array, so it has no element 0"},
		{"$rename onto Born.Year", UpdateRequest{Rename: map[string]string{"Nick": "Born.Year"}}, `$rename of "Born.Year"`},
		{"$max of Born.Year", UpdateRequest{Max: map[string]interface{}{"Born.Year": int32(1815)}}, `$max of "Born.Year"`},
	} {
		err := typedUpdate(t, s, refused.req)
		requireTypeRefusal(t, refused.what, err, refused.says)
	}
	if born, present := documentOf(t, s, "app", "User", "u")["Born"]; present {
		t.Errorf("a refused path update left Born %v; want it absent", born)
	}
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Address.Zip": "1001", "Tags[0]": "b"}}); err != nil {
		t.Errorf("a path into the declared object Address and the declared array Tags was refused: %v", err)
	}
}

// TestUpdateInWarnStoresAndCountsOnce and its siblings are the modes on an
// update (spec §4.8, §9.5): warn stores a mismatch and counts the update
// once, however many of its documents mismatch; off checks nothing; strict
// counts its refusal once.
func TestUpdateInWarnStoresAndCountsOnce(t *testing.T) {
	s := typedUpdateServer(t, "warn")
	mustInsert(t, s, "app", "User", "v", updateSeed())
	refusedBefore, acceptedBefore := typeCounters(t)
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Age": "thirty"}}); err != nil {
		t.Fatalf("warn refused a mismatched $set: %v", err)
	}
	if age := documentOf(t, s, "app", "User", "u")["Age"]; age != "thirty" {
		t.Errorf("warn stored Age %v; want the operand", age)
	}
	if err := typedUpdate(t, s, UpdateRequest{Multi: true, Unset: []string{"Name"}}); err != nil {
		t.Fatalf("warn refused a multi-update unsetting the required Name: %v", err)
	}
	requireCounterDeltas(t, "two mismatched updates in warn, one of them to two documents", refusedBefore, acceptedBefore, 0, 2)
}

func TestUpdateInOffChecksNothing(t *testing.T) {
	s := typedUpdateServer(t, "off")
	refusedBefore, acceptedBefore := typeCounters(t)
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Age": "thirty"}, Push: map[string]interface{}{"Small": int32(1)}}); err != nil {
		t.Fatalf("off refused a mismatched update: %v", err)
	}
	requireCounterDeltas(t, "a mismatched update in off", refusedBefore, acceptedBefore, 0, 0)
}

func TestUpdateRefusalInStrictCountsOnce(t *testing.T) {
	s := typedUpdateServer(t, "")
	mustInsert(t, s, "app", "User", "v", updateSeed())
	refusedBefore, acceptedBefore := typeCounters(t)
	err := typedUpdate(t, s, UpdateRequest{Multi: true, Set: map[string]interface{}{"Age": "thirty"}})
	requireTypeRefusal(t, "a mismatched multi-update in strict", err, `$set on field "Age"`)
	requireCounterDeltas(t, "a mismatched multi-update over two documents in strict", refusedBefore, acceptedBefore, 1, 0)
	requireUnchanged(t, s, "the refused multi-update")
}

// TestUpdateNowIsCheckedAsADatetime: now() in an update is fixed once, at
// the door or before the log, so the check sees the datetime it
// became, on a standalone node and on every copy of a cluster.
func TestUpdateNowIsCheckedAsADatetime(t *testing.T) {
	s := typedUpdateServer(t, "")
	doorWrite(t, s, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: "User", Key: "u", Set: map[string]interface{}{"Born": nowArg()}})
	if born, isDate := documentOf(t, s, "app", "User", "u")["Born"].(primitive.DateTime); !isDate || born == 0 {
		t.Errorf("$set of now() into Born datetime stored %v", documentOf(t, s, "app", "User", "u")["Born"])
	}
	answer, err := s.fencedDispatch(t.Context(), &transport.Frame{Opcode: transport.OpUpdate, Payload: marshalRequest(t,
		UpdateRequest{Database: "app", Bucket: "User", Key: "u", Set: map[string]interface{}{"Name": nowArg()}})})
	if err != nil {
		t.Fatalf("the update through the door: %v", err)
	}
	if refusal := refusalOf(t, answer); transport.ErrorCode(refusal.Code) != transport.ErrFieldType || !strings.Contains(refusal.Message, "a datetime") {
		t.Errorf("$set of now() into Name string was answered code %#x %q; want FIELD_TYPE naming a datetime", refusal.Code, refusal.Message)
	}

	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createTypedOn(tc, 0, "Events", bson.A{bson.M{"name": "When", "type": "datetime", "required": true}}, "")
	waitForServing(t, tc, testDatabase, "Events", 3)
	owner := ownerIndexOf(t, tc, "Events")
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "Events", Key: "e",
		Data: map[string]interface{}{"When": primitive.DateTime(1)}, WriteConcern: "all"})
	tc.ok(owner, transport.OpUpdate, concerned(t, UpdateRequest{Database: testDatabase, Bucket: "Events", Key: "e",
		Set: map[string]interface{}{"When": nowArg()}}))
	for index, node := range tc.nodes {
		// Write concern all answers once the copies in sync hold the update;
		// a slower copy is not the defect under test.
		var when bson.RawValue
		deadline := time.Now().Add(10 * time.Second)
		for {
			found, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: "Events", Key: "e"})
			if err != nil || !found.Found {
				t.Fatalf("node %d: reading e: %+v %v", index+1, found, err)
			}
			when = bson.Raw(found.Data).Lookup("When")
			if (when.Type == bson.TypeDateTime && when.DateTime() > 1) || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if when.Type != bson.TypeDateTime || when.DateTime() <= 1 {
			t.Errorf("node %d holds When %v after $set of now(); want a later datetime", index+1, when)
		}
	}
}

// TestIncOverflowingInt32IsRefused (spec §5.4's table): $inc's result is an
// int64, accepted on an int32 field only while it fits int32. A double field
// takes an integer result up to 2^53 in magnitude.
func TestIncOverflowingInt32IsRefused(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Small": 1 << 40}})
	requireTypeRefusal(t, "$inc of Small int32 past int32's range", err, `$inc on field "Small"`, "Small is declared int32",
		"the result is a 64-bit integer outside int32's range", "alter bucket User { Small int }")
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Score": 4.0}}); err != nil {
		t.Fatalf("setting Score to a whole double: %v", err)
	}
	err = typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Score": 1 << 54}})
	requireTypeRefusal(t, "$inc of Score double past 2^53", err, `$inc on field "Score"`, "larger than 2^53")
	if score := documentOf(t, s, "app", "User", "u")["Score"]; score != 4.0 {
		t.Errorf("after the refused $inc Score reads %v; want 4", score)
	}
	if err := typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Small": 1}, Set: map[string]interface{}{"Score": 2.5}}); err != nil {
		t.Errorf("$inc of Small int32 within range was refused: %v", err)
	}
	if small := documentOf(t, s, "app", "User", "u")["Small"]; small != int64(2) {
		t.Errorf("Small reads %v (%T) after $inc 1; want the int64 2", small, small)
	}
}

// TestIncOnIntIsAccepted: an int field takes $inc's int64 result.
func TestIncOnIntIsAccepted(t *testing.T) {
	s := typedUpdateServer(t, "")
	if err := typedUpdate(t, s, UpdateRequest{Inc: map[string]int64{"Age": 1 << 40}}); err != nil {
		t.Fatalf("$inc on Age int was refused: %v", err)
	}
	if age := documentOf(t, s, "app", "User", "u")["Age"]; age != int64(36)+1<<40 {
		t.Errorf("Age reads %v after $inc; want %d", age, int64(36)+1<<40)
	}
}

// TestMulOfAnIntByAWholeFactorIsAccepted: an integer times a whole factor is
// an int64, which an int field takes.
func TestMulOfAnIntByAWholeFactorIsAccepted(t *testing.T) {
	s := typedUpdateServer(t, "")
	if err := typedUpdate(t, s, UpdateRequest{Mul: map[string]float64{"Age": 2}}); err != nil {
		t.Fatalf("$mul of Age int by 2 was refused: %v", err)
	}
	if age := documentOf(t, s, "app", "User", "u")["Age"]; age != int64(72) {
		t.Errorf("Age reads %v (%T) after $mul 2; want the int64 72", age, age)
	}
}

// TestMulOfAnIntByAFractionalFactorIsRefusedNamingTheFix: the product is a
// double, which an integer field refuses; the text names the fix (E-6's
// $mul text). A whole factor too large for int32 is E-2's.
func TestMulOfAnIntByAFractionalFactorIsRefusedNamingTheFix(t *testing.T) {
	s := typedUpdateServer(t, "")
	err := typedUpdate(t, s, UpdateRequest{Mul: map[string]float64{"Age": 1.5}})
	requireTypeRefusal(t, "$mul of Age int by 1.5", err, `$mul on field "Age"`,
		"the product is a double", "Multiply by a whole number within int64's range")
	err = typedUpdate(t, s, UpdateRequest{Mul: map[string]float64{"Small": 1 << 40}})
	requireTypeRefusal(t, "$mul of Small int32 past int32's range", err, `$mul on field "Small"`, "outside int32's range")
	requireUnchanged(t, s, "the refused $mul")
}

// TestRenameOntoADeclaredFieldChecksTheMovedValue: a rename moves a value
// the declaration has not seen into a declared field; the moved value is
// checked against the target's declaration (a result check).
func TestRenameOntoADeclaredFieldChecksTheMovedValue(t *testing.T) {
	s := typedUpdateServer(t, "")
	if err := typedUpdate(t, s, UpdateRequest{Set: map[string]interface{}{"Nick": "ada", "Years": int32(40)}, Unset: []string{"Age"}}); err != nil {
		t.Fatalf("setting the undeclared Nick and Years: %v", err)
	}
	err := typedUpdate(t, s, UpdateRequest{Rename: map[string]string{"Nick": "Age"}})
	requireTypeRefusal(t, "$rename of a string onto Age int", err, `$rename onto field "Age"`, "the value moved there is a string")
	if document := documentOf(t, s, "app", "User", "u"); document["Nick"] != "ada" || document["Age"] != nil {
		t.Errorf("after the refused $rename u reads %v; want Nick kept and no Age", document)
	}
	if err := typedUpdate(t, s, UpdateRequest{Rename: map[string]string{"Years": "Age"}}); err != nil {
		t.Errorf("$rename of an int onto Age int was refused: %v", err)
	}
	// $unset runs before $rename, so a target the same update unsets is
	// written by the rename, and checked.
	err = typedUpdate(t, s, UpdateRequest{Unset: []string{"Age"}, Rename: map[string]string{"Nick": "Age"}})
	requireTypeRefusal(t, "$unset of Age and $rename of a string onto it", err, `$rename onto field "Age"`)
	if age := documentOf(t, s, "app", "User", "u")["Age"]; age != int32(40) {
		t.Errorf("after the refused $unset and $rename Age reads %v; want 40 kept", age)
	}
	// A rename that moves nothing (the target is taken) writes nothing, so
	// it checks nothing.
	if err := typedUpdate(t, s, UpdateRequest{Rename: map[string]string{"Nick": "Age"}}); err != nil {
		t.Errorf("$rename onto a taken Age, which moves nothing, was refused: %v", err)
	}
}

// TestUpdateChecksOnlyTheFieldsItWrites (spec §5.4): fields the update does
// not write are not checked again. The stored mismatch is a document written
// before the declaration was recorded (the catalog is edited by hand, as no
// alter exists before TF-8).
func TestUpdateChecksOnlyTheFieldsItWrites(t *testing.T) {
	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	createTestBucket(t, cs.core, "app", "Mixed", BucketTypeHash)
	mustInsert(t, cs.core, "app", "Mixed", "u", map[string]interface{}{"Age": "thirty", "Count": int32(1)})
	written := withRawFields(t, rawCatalogEntry(t, cs.core, "app", "Mixed"), bson.A{
		bson.D{{Key: "name", Value: "Age"}, {Key: "type", Value: "int"}},
		bson.D{{Key: "name", Value: "Count"}, {Key: "type", Value: "int32"}},
	})
	writeRawCatalogEntry(t, cs.core, "app", "Mixed", written)
	cs = restartCatalog(t, cs, dataDir)
	servedBucket(t, cs.core, "app", "Mixed")

	resp, err := cs.core.Update(UpdateRequest{Database: "app", Bucket: "Mixed", Key: "u", Inc: map[string]int64{"Count": 1}})
	if err != nil || !resp.Success {
		t.Fatalf("$inc of Count beside the stored mismatch in Age was refused: %+v %v", resp, err)
	}
	if document := documentOf(t, cs.core, "app", "Mixed", "u"); document["Count"] != int64(2) || document["Age"] != "thirty" {
		t.Errorf("after $inc u reads %v; want Count 2 and Age untouched", document)
	}
}

// TestMultiUpdateRefusesAllWhenOneResultMismatches: a multi-update is one
// apply, so one document's mismatched result refuses it whole, and its text
// names that document.
func TestMultiUpdateRefusesAllWhenOneResultMismatches(t *testing.T) {
	s := typedUpdateServer(t, "")
	full := updateSeed()
	full["Small"] = int32(2147483647)
	mustInsert(t, s, "app", "User", "v", full)
	err := typedUpdate(t, s, UpdateRequest{Multi: true, Inc: map[string]int64{"Small": 1}})
	requireTypeRefusal(t, "a multi-update whose $inc overflows Small in v", err, `$inc on field "Small"`, "outside int32's range")
	requireUnchanged(t, s, "the refused multi-update")
	if small := documentOf(t, s, "app", "User", "v")["Small"]; small != int32(2147483647) {
		t.Errorf("after the refused multi-update v's Small reads %v; want it unchanged", small)
	}
}
