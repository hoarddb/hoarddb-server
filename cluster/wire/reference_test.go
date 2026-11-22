package wire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

func referenceWorld(t *testing.T) (*expandWorld, context.Context) {
	t.Helper()
	world := newExpandWorld(t, u70AllowAll{})
	node, ctx := world.server.node, context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Room", Field{Name: "Name", Type: "string"}, Field{Name: "Members", Type: "hashStore", Of: "User"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Message", Field{Name: "Author", Type: "string", Of: "User"}, Field{Name: "Text", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	return world, ctx
}

func refusalText(t *testing.T, err error, code cluster.Code) string {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != code {
		t.Fatalf("error = %v, want a %s refusal", err, code)
	}
	return coded.Message
}

// TestOfDeclarationRoundTripsAndDescribes: [GUARD the reference is stored and describe names its bucket].
func TestOfDeclarationRoundTripsAndDescribes(t *testing.T) {
	world, ctx := referenceWorld(t)
	reply, err := world.server.describe(ctx, AdminDescribeBucket, u30iDB, "Room")
	if err != nil {
		t.Fatal(err)
	}
	of := map[string]string{}
	for _, field := range reply.Fields {
		of[field.Name] = field.Of
	}
	if of["Members"] != "User" || of["Name"] != "" {
		t.Fatalf("describe of = %v", of)
	}
	room := world.bucket(t, "Room")
	if bucket, ok := room.entry.Decl.ReferenceOf(room.table.Entries[2].ID); !ok || bucket != world.near.entry.Decl.BucketID {
		t.Fatalf("the declaration holds %d, %v for Members", bucket, ok)
	}
}

// TestOfRefusesAMissingTarget: [GUARD a reference to nothing is never declared].
func TestOfRefusesAMissingTarget(t *testing.T) {
	world, ctx := referenceWorld(t)
	_, err := world.server.node.CreateBucket(ctx, u30iDB, "Order", Field{Name: "Buyer", Type: "string", Of: "Nobody"})
	if text := refusalText(t, err, cluster.CodeNotFound); !strings.Contains(text, "create bucket Nobody") {
		t.Fatalf("the refusal does not give the fix: %s", text)
	}
	if _, err := world.server.node.bucket(ctx, u30iDB, "Order"); err == nil {
		t.Fatal("a refused create left a bucket")
	}
}

// TestOfRefusesANonStringType: [GUARD non-text ids are refused].
func TestOfRefusesANonStringType(t *testing.T) {
	world, ctx := referenceWorld(t)
	_, err := world.server.node.CreateBucket(ctx, u30iDB, "Order", Field{Name: "Buyer", Type: "objectid", Of: "User"})
	if text := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(text, "string") {
		t.Fatalf("the refusal does not name string: %s", text)
	}
}

// TestOfAcrossDatabases: [GUARD the target is looked up in the database the reference names].
func TestOfAcrossDatabases(t *testing.T) {
	world, ctx := referenceWorld(t)
	node := world.server.node
	if _, err := node.CreateDatabase(ctx, "ref"); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, "ref", "Person", Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Comment", Field{Name: "By", Type: "string", Of: "ref.Person"}); err != nil {
		t.Fatal(err)
	}
	reply, err := world.server.describe(ctx, AdminDescribeBucket, u30iDB, "Comment")
	if err != nil {
		t.Fatal(err)
	}
	if got := reply.Fields[1].Of; got != "ref.Person" {
		t.Fatalf("describe of = %q, want ref.Person", got)
	}
}

// TestTheAdminWireCarriesTheOfWord: [GUARD of rides the admin document and is absent when empty].
func TestTheAdminWireCarriesTheOfWord(t *testing.T) {
	with := RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "Room", Fields: []RingAdminField{{Name: "Members", Type: "hashStore", Of: "ref.User"}}}
	raw, err := EncodeRingAdmin(with)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingAdmin(raw)
	if err != nil || got.Fields[0].Of != "ref.User" {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	without := with
	without.Fields = []RingAdminField{{Name: "Members", Type: "hashStore"}}
	raw, err = EncodeRingAdmin(without)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "\x00of\x00") {
		t.Fatal("a field with no reference wrote an of member")
	}
	bad := with
	bad.Fields = []RingAdminField{{Name: "Members", Type: "hashStore", Of: "a.b.c"}}
	if _, err := EncodeRingAdmin(bad); err == nil {
		t.Fatal("a three-part of was accepted")
	}
}

func typedString(t *testing.T, text string) []byte {
	t.Helper()
	return stringValue(text)
}

// referenceWrite is an insert of m1 with value in Message.Author, the reference field of referenceWorld.
func referenceWrite(t *testing.T, world *expandWorld, value []byte) error {
	t.Helper()
	message := world.bucket(t, "Message")
	author, _ := entryNamed(message.table, "Author")
	encID, _ := store.EncodeComponent("m1")
	op := cluster.Op{Class: cluster.OpInsert, EncID: encID, IDValue: stringValue("m1"), Fields: []cluster.FieldOp{{FieldID: author.ID, Value: value}}}
	return ringCheckDocument(message, op, world.server.referenceNamer(context.Background(), message))
}

// TestOfChecksTheIdTypeAtWrite: [GUARD a number is never stored as a reference; the refusal
// names the field and the real target bucket].
func TestOfChecksTheIdTypeAtWrite(t *testing.T) {
	world, _ := referenceWorld(t)
	err := referenceWrite(t, world, append([]byte{byte(bson.TypeInt32)}, 1, 0, 0, 0))
	text := refusalText(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(text, "Author") || !strings.Contains(text, "refers to User") || strings.Contains(text, "declared of") {
		t.Fatalf("the refusal does not name the field and the real target bucket User: %s", text)
	}
}

// TestOfDoesNotCheckExistence: [GUARD the write path gains no cross-owner read: text is
// accepted although nobody has that id, and no far read is made].
func TestOfDoesNotCheckExistence(t *testing.T) {
	world, _ := referenceWorld(t)
	world.server.expandFetch = func(context.Context, bucketRef, []byte, []uint16, bool) (Document, error) {
		t.Error("the write check read a far document")
		return Document{}, nil
	}
	reads := world.server.node.ExpandStats().Reads.Load()
	if err := referenceWrite(t, world, typedString(t, "nobody")); err != nil {
		t.Fatalf("an id with no document was refused: %v", err)
	}
	if got := world.server.node.ExpandStats().Reads.Load(); got != reads {
		t.Fatalf("the write check made %d far reads, want none", got-reads)
	}
}

// TestNullReferenceIsAllowed: [GUARD null is "no reference"].
func TestNullReferenceIsAllowed(t *testing.T) {
	world, _ := referenceWorld(t)
	if err := referenceWrite(t, world, []byte{byte(bson.TypeNull)}); err != nil {
		t.Fatalf("a null reference was refused: %v", err)
	}
}

// TestOfOnAScalarOtherThanStringIsRefused: [GUARD only a string, hashStore or btreeStore field
// can be declared of a bucket].
func TestOfOnAScalarOtherThanStringIsRefused(t *testing.T) {
	world, ctx := referenceWorld(t)
	_, err := world.server.node.CreateBucket(ctx, u30iDB, "Badge", Field{Name: "Owner", Type: "int", Of: "User"})
	text := refusalText(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(text, "Owner") || !strings.Contains(text, "string of User") {
		t.Fatalf("the refusal does not name the field and the fix 'string of User': %s", text)
	}
}

// TestExpandUsesTheDeclaredReference: [GUARD expand: {Author: true} needs no bucket name].
func TestExpandUsesTheDeclaredReference(t *testing.T) {
	world, ctx := referenceWorld(t)
	if _, err := world.server.node.Insert(ctx, u30iDB, "Message", bson.D{{Key: "_id_", Value: "m1"}, {Key: "Author", Value: "u2"}, {Key: "Text", Value: "hi"}}); err != nil {
		t.Fatal(err)
	}
	reply, err := world.server.serveList(ctx, RingListRequest{Database: u30iDB, Bucket: "Message",
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Author", Fields: []string{"Name"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := entryBodies(t, reply); got["m1"] != "Name2" {
		t.Fatalf("expansions = %v", got)
	}
}

// TestExpandedDeclaredFarBucketGoneIsRefusedAsDenied: [GUARD a declared target that is gone never
// reads as no author; it is the generic PERMISSION_DENIED, the same a target the session may
// not read gets, so the refusal tells nothing about what exists].
func TestExpandedDeclaredFarBucketGoneIsRefusedAsDenied(t *testing.T) {
	world, ctx := referenceWorld(t)
	node := world.server.node
	if _, err := node.CreateBucket(ctx, u30iDB, "Tag", Field{Name: "Label", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Note", Field{Name: "Tag", Type: "string", Of: "Tag"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Note", bson.D{{Key: "_id_", Value: "n1"}, {Key: "Tag", Value: "t1"}}); err != nil {
		t.Fatal(err)
	}
	if err := node.DropBucket(ctx, u30iDB, "Tag"); err != nil {
		t.Fatalf("the fixture could not drop the far bucket: %v", err)
	}
	_, err := world.server.serveList(ctx, RingListRequest{Database: u30iDB, Bucket: "Note",
		Options: RingReadOptions{Expand: []RingExpandSpec{{Field: "Tag"}}}})
	text := refusalText(t, err, cluster.CodePermissionDenied)
	if !strings.Contains(text, "grant role") || strings.Contains(text, "Tag") {
		t.Fatalf("the refusal is not the generic one that names no bucket: %s", text)
	}
}

// TestOfChecksTheIdTypeOnEveryUpdateOperation: [GUARD update does not bypass W-F1; one case
// per operation code, and rename into a reference field].
func TestOfChecksTheIdTypeOnEveryUpdateOperation(t *testing.T) {
	world, ctx := referenceWorld(t)
	message := world.bucket(t, "Message")
	author, _ := entryNamed(message.table, "Author")
	text, _ := entryNamed(message.table, "Text")
	encID, _ := store.EncodeComponent("m1")
	number := append([]byte{byte(bson.TypeInt32)}, 1, 0, 0, 0)
	update := func(field cluster.FieldOp) error {
		op := cluster.Op{Class: cluster.OpUpdate, EncID: encID, Fields: []cluster.FieldOp{field}}
		_, err := world.server.writeUpdate(ctx, message, op, op, writeOptions{})
		return err
	}
	for name, code := range map[string]byte{
		"set": cluster.UpdateSet, "push": cluster.UpdatePush, "add_to_set": cluster.UpdateAddToSet,
		"min": cluster.UpdateMin, "max": cluster.UpdateMax, "inc": cluster.UpdateInc, "mul": cluster.UpdateMul,
	} {
		err := update(cluster.FieldOp{FieldID: author.ID, OpCode: code, Value: number})
		if got := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(got, "Author") || !strings.Contains(got, "not text") {
			t.Errorf("%s of a number into a reference field: %q, want the id-type refusal naming Author", name, got)
		}
	}
	// A rename moves a value nobody checked into the reference field.
	target := []byte{byte(author.ID >> 8), byte(author.ID)}
	err := update(cluster.FieldOp{FieldID: text.ID, OpCode: cluster.UpdateRenameTo, Value: target})
	if got := refusalText(t, err, cluster.CodeInvalidArgument); !strings.Contains(got, "Author") || !strings.Contains(got, "Nothing was changed") {
		t.Errorf("rename into a reference field: %q", got)
	}
	// Text and null are ids, unset clears the reference, and none of it reads a document.
	for _, value := range [][]byte{typedString(t, "u1"), {byte(bson.TypeNull)}} {
		fields := []cluster.FieldOp{{FieldID: author.ID, OpCode: cluster.UpdateSet, Value: value}}
		if err := message.referenceRefusal(fields, world.server.referenceNamer(ctx, message)); err != nil {
			t.Errorf("a text or null id was refused: %v", err)
		}
	}
	if err := message.referenceRefusal([]cluster.FieldOp{{FieldID: author.ID, OpCode: cluster.UpdateUnset}}, world.server.referenceNamer(ctx, message)); err != nil {
		t.Errorf("unset of a reference was refused: %v", err)
	}
}

// denyDatabase refuses every class on one database and allows the rest.
type denyDatabase struct{ database string }

func (d denyDatabase) Authorize(_ context.Context, access RingAccess) error {
	if access.Database == d.database {
		return errors.New("no grant")
	}
	return nil
}

// TestOfIsNotAnExistenceOracle: [GUARD declaring 'of' a bucket the session may not read answers
// the same whether the bucket exists or not, and creates nothing].
func TestOfIsNotAnExistenceOracle(t *testing.T) {
	world, ctx := referenceWorld(t)
	node := world.server.node
	if _, err := node.CreateDatabase(ctx, "secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, "secret", "Payroll", Field{Name: "Pay", Type: "int"}); err != nil {
		t.Fatal(err)
	}
	world.server.authz = denyDatabase{database: "secret"}
	create := func(name, of string) string {
		t.Helper()
		_, err := world.server.serveAdmin(ctx, RingAdminRequest{Cmd: AdminCreateBucket, Database: u30iDB, Name: name,
			Fields: []RingAdminField{{Name: "P", Type: "string", Of: of}}})
		return refusalText(t, err, cluster.CodePermissionDenied)
	}
	present, absent := create("Probe1", "secret.Payroll"), create("Probe2", "secret.Ghost")
	if present != absent {
		t.Fatalf("an existing and an absent target are told apart:\n%s\n%s", present, absent)
	}
	for _, leak := range []string{"Payroll", "Ghost", "secret"} {
		if strings.Contains(present, leak) {
			t.Fatalf("the refusal names %q: %s", leak, present)
		}
	}
	for _, name := range []string{"Probe1", "Probe2"} {
		if _, err := node.bucket(ctx, u30iDB, name); err == nil {
			t.Fatalf("a refused create left %s", name)
		}
	}
	// With a grant the missing target is still named, with its fix.
	world.server.authz = u70AllowAll{}
	_, err := world.server.serveAdmin(ctx, RingAdminRequest{Cmd: AdminCreateBucket, Database: u30iDB, Name: "Probe3",
		Fields: []RingAdminField{{Name: "P", Type: "string", Of: "secret.Ghost"}}})
	refusalText(t, err, cluster.CodeNotFound)
}

// TestAlterRefusesOfInsteadOfDroppingIt: [GUARD alter cannot declare a reference yet, so a field
// added 'of' is refused with the fix, not accepted with the reference silently dropped].
func TestAlterRefusesOfInsteadOfDroppingIt(t *testing.T) {
	world, ctx := referenceWorld(t)
	_, err := world.server.serveAdmin(ctx, RingAdminRequest{Cmd: AdminAlterBucket, Database: u30iDB, Bucket: "Message",
		Add: []RingAdminField{{Name: "Editor", Type: "string", Of: "User"}}})
	text := refusalText(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(text, "Editor") || !strings.Contains(text, "create bucket") {
		t.Fatalf("the refusal does not name the field and the fix: %s", text)
	}
	reply, err := world.server.describe(ctx, AdminDescribeBucket, u30iDB, "Message")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range reply.Fields {
		if field.Name == "Editor" {
			t.Fatalf("a refused alter added the field: %+v", field)
		}
	}
}

// TestDroppingAReferencingFieldPrunesItsRowInField13: [GUARD a dropped field's reference row is
// deleted when its drop is folded, in the same write; a second alter and a drop bucket succeed].
func TestDroppingAReferencingFieldPrunesItsRowInField13(t *testing.T) {
	world, ctx := referenceWorld(t)
	node := world.server.node
	if _, err := node.AlterBucket(ctx, u30iDB, "Message", AlterChange{Drop: []string{"Author"}}); err != nil {
		t.Fatalf("dropping the referencing field: %v", err)
	}
	if _, err := node.AlterBucket(ctx, u30iDB, "Message", AlterChange{Add: []FieldEntry{{Name: "Note", Type: "string"}}}); err != nil {
		t.Fatalf("a second alter after the drop: %v", err)
	}
	message := world.bucket(t, "Message")
	if len(message.entry.Decl.References) != 0 {
		t.Fatalf("the declaration still holds %+v for a dropped field", message.entry.Decl.References)
	}
	if err := node.DropBucket(ctx, u30iDB, "Message"); err != nil {
		t.Fatalf("drop bucket after the prune: %v", err)
	}
}
