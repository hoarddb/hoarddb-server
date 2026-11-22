package wire

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

func u67UserNode(t *testing.T) *Node {
	t.Helper()
	node, _ := u30iFresh(t)
	if _, err := node.Insert(context.Background(), u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	return node
}

func u67User(t *testing.T, node *Node) bson.Raw {
	t.Helper()
	doc, err := node.Get(context.Background(), u30iDB, "User", "u1")
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(doc.Body)
}

func u67Update(node *Node, req UpdateRequest, options ...WriteOption) (Updated, error) {
	req.Database, req.Bucket, req.ID = u30iDB, "User", "u1"
	return node.Update(context.Background(), req, options...)
}

func u67UserPosition(node *Node) any {
	return node.pairs.Snapshot(node.slotOf(u30iDB, "User", "u1"), 1)
}

// u67StillWritableUser proves the pair of u1 orders a write after the refusals.
func u67StillWritableUser(t *testing.T, node *Node) {
	t.Helper()
	if _, err := u67Update(node, UpdateRequest{Set: bson.D{{Key: "City", Value: "after"}}}); err != nil {
		t.Fatalf("the pair did not take a write after the refusals: %v", err)
	}
}

// TestU67UpdateSetUnsetInc: [GUARD fillUpdate writes and deletes the field keys].
func TestU67UpdateSetUnsetInc(t *testing.T) {
	node := u67UserNode(t)
	updated, err := u67Update(node, UpdateRequest{Set: bson.D{{Key: "City", Value: "Bergen"}}, Unset: []string{"Name"}, Inc: bson.D{{Key: "Age", Value: int32(2)}}})
	if err != nil || updated.Pos.Zero() || updated.Deduped {
		t.Fatalf("Update = %+v, %v", updated, err)
	}
	doc := u67User(t, node)
	if doc.Lookup("City").StringValue() != "Bergen" || doc.Lookup("Age").Int32() != 38 || doc.Lookup("_id_").StringValue() != "u1" {
		t.Fatalf("after the update: %v", doc)
	}
	if _, err := doc.LookupErr("Name"); err == nil {
		t.Fatalf("Name was unset and is still there: %v", doc)
	}
	// An unset field is gone, and an inc on a field that is not there takes the amount.
	if _, err := u67Update(node, UpdateRequest{Inc: bson.D{{Key: "Name", Value: int32(5)}}}); err != nil {
		t.Fatal(err)
	}
	if got := u67User(t, node).Lookup("Name").Int32(); got != 5 {
		t.Fatalf("inc on an absent field gave %d, want the amount 5", got)
	}
}

// TestU67IncArithmetic: [GUARD addNumbers keeps Mongo's promotion and refuses overflow].
func TestU67IncArithmetic(t *testing.T) {
	node := u67UserNode(t)
	set := func(value any) {
		t.Helper()
		if _, err := u67Update(node, UpdateRequest{Set: bson.D{{Key: "Age", Value: value}}}); err != nil {
			t.Fatal(err)
		}
	}
	inc := func(value any) error {
		_, err := u67Update(node, UpdateRequest{Inc: bson.D{{Key: "Age", Value: value}}})
		return err
	}
	set(int32(math.MaxInt32))
	if err := inc(int32(1)); err != nil {
		t.Fatal(err)
	}
	if got := u67User(t, node).Lookup("Age"); got.Type != bson.TypeInt64 || got.Int64() != math.MaxInt32+1 {
		t.Fatalf("int32 overflow gave %v, want the int64 promotion", got)
	}
	if err := inc(int64(-5)); err != nil || u67User(t, node).Lookup("Age").Int64() != math.MaxInt32-4 {
		t.Fatalf("int64 minus 5: %v", err)
	}
	set(int32(10))
	if err := inc(1.5); err != nil {
		t.Fatal(err)
	}
	if got := u67User(t, node).Lookup("Age"); got.Type != bson.TypeDouble || got.Double() != 11.5 {
		t.Fatalf("int32 plus double gave %v", got)
	}
	set(int64(math.MaxInt64))
	before := u67UserPosition(node)
	err := inc(int64(1))
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "overflow") || !strings.Contains(err.Error(), "Nothing was changed") {
		t.Errorf("int64 overflow: %q", err)
	}
	if after := u67UserPosition(node); after != before {
		t.Error("an overflowing inc moved the pair")
	}
	set(1e308)
	err = inc(1e308)
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if !strings.Contains(err.Error(), "overflow") {
		t.Errorf("double overflow: %q", err)
	}
	set(int32(1))
	for name, operand := range map[string]any{"string": "x", "bool": true, "NaN": math.NaN(), "Inf": math.Inf(1)} {
		before = u67UserPosition(node)
		err := inc(operand)
		u30iCode(t, err, cluster.CodeInvalidArgument)
		if after := u67UserPosition(node); after != before {
			t.Errorf("%s operand moved the pair", name)
		}
	}
}

// TestU67RefusedUpdatesDoNotMoveThePair: [GUARD updateShapeRefusal, updateStateRefusal and
// the engine, size and liveness checks of updatePrecondition].
func TestU67RefusedUpdatesDoNotMoveThePair(t *testing.T) {
	node := u67UserNode(t)
	ctx := context.Background()
	for name, tc := range map[string]struct {
		req  UpdateRequest
		code cluster.Code
		said string
	}{
		"nothing to change": {UpdateRequest{}, cluster.CodeInvalidArgument, "Nothing was changed"},
		"a store field":     {UpdateRequest{Set: bson.D{{Key: "Purchases", Value: "x"}}}, cluster.CodeFieldType, "Purchases"},
		"an unknown field":  {UpdateRequest{Set: bson.D{{Key: "Shoe", Value: "x"}}}, cluster.CodeFieldType, "Shoe"},
		"set and inc":       {UpdateRequest{Set: bson.D{{Key: "Age", Value: int32(1)}}, Inc: bson.D{{Key: "Age", Value: int32(1)}}}, cluster.CodeInvalidArgument, "Age"},
		"unset twice":       {UpdateRequest{Unset: []string{"City", "City"}}, cluster.CodeInvalidArgument, "City"},
		"inc a string":      {UpdateRequest{Inc: bson.D{{Key: "City", Value: int32(1)}}}, cluster.CodeInvalidArgument, "string"},
	} {
		before := u67UserPosition(node)
		_, err := u67Update(node, tc.req)
		u30iCode(t, err, tc.code)
		if !strings.Contains(err.Error(), tc.said) {
			t.Errorf("%s: %q does not say %q", name, err, tc.said)
		}
		if after := u67UserPosition(node); after != before {
			t.Errorf("%s moved the pair from %+v to %+v", name, before, after)
		}
	}
	missing := UpdateRequest{Database: u30iDB, Bucket: "User", ID: "ghost", Set: bson.D{{Key: "City", Value: "x"}}}
	_, err := node.Update(ctx, missing)
	u30iCode(t, err, cluster.CodeNotFound)
	if err := node.Delete(ctx, u30iDB, "User", "u1"); err != nil {
		t.Fatal(err)
	}
	_, err = u67Update(node, UpdateRequest{Set: bson.D{{Key: "City", Value: "x"}}})
	if err == nil || !strings.Contains(err.Error(), "deleted") {
		t.Errorf("update of a deleted document: %v, want it to say deleted", err)
	}
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatalf("the pair is not writable after the refusals: %v", err)
	}
	u67StillWritableUser(t, node)
}

// u67RawUpdate writes a hand-built update through the owner write path, bypassing every
// check the Node API makes.
func u67RawUpdate(node *Node, fields ...cluster.FieldOp) error {
	ctx := context.Background()
	ref, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		return err
	}
	opts, err := newWriteOptions(nil)
	if err != nil {
		return err
	}
	op := ref.template()
	op.Class, op.Fields = cluster.OpUpdate, fields
	encID, err := store.EncodeComponent("u1")
	if err != nil {
		return err
	}
	_, err = node.send(ctx, ref, encID, op, opts)
	return err
}

// TestU67OwnerRefusesWhatTheApplyCouldNotApplyBeforeAPosition: [GUARD the owner's own
// checks, for a record that never went through Node.Update].
func TestU67OwnerRefusesWhatTheApplyCouldNotApplyBeforeAPosition(t *testing.T) {
	node := u67UserNode(t)
	ref, err := node.bucket(context.Background(), u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	id := func(name string) uint16 { return u67FieldID(t, ref, name) }
	for name, tc := range map[string]struct {
		fields []cluster.FieldOp
		said   string
	}{
		"no operations":      {nil, "names no field"},
		"header field id":    {[]cluster.FieldOp{{FieldID: 1, OpCode: cluster.UpdateSet, Value: []byte{0x10, 2, 0, 0, 0}}}, "field id 1"},
		"set without a type": {[]cluster.FieldOp{{FieldID: id("Age"), OpCode: cluster.UpdateSet}}, "value"},
		"a lying value":      {[]cluster.FieldOp{{FieldID: id("Name"), OpCode: cluster.UpdateSet, Value: []byte{0x02, 0xff, 0xff, 0xff, 0x7f, 'x', 0}}}, "BSON"},
		"unset with a value": {[]cluster.FieldOp{{FieldID: id("Age"), OpCode: cluster.UpdateUnset, Value: []byte{0x10, 2, 0, 0, 0}}}, "unset"},
		"inc a string":       {[]cluster.FieldOp{{FieldID: id("Age"), OpCode: cluster.UpdateInc, Value: stringValue("x")}}, "number"},
		"duplicate field":    {[]cluster.FieldOp{{FieldID: id("Age"), OpCode: cluster.UpdateUnset}, {FieldID: id("Age"), OpCode: cluster.UpdateUnset}}, "two operations"},
		"a store field":      {[]cluster.FieldOp{{FieldID: id("Purchases"), OpCode: cluster.UpdateUnset}}, ""},
	} {
		before := u67UserPosition(node)
		err := u67RawUpdate(node, tc.fields...)
		var coded *cluster.Error
		if err == nil || !errors.As(err, &coded) || (coded.Code != cluster.CodeInvalidArgument && coded.Code != cluster.CodeFieldType) {
			t.Errorf("%s: err = %v, want INVALID_ARGUMENT or FIELD_TYPE", name, err)
			continue
		}
		if !strings.Contains(err.Error(), tc.said) || (coded.Code == cluster.CodeInvalidArgument && !strings.Contains(err.Error(), "Nothing was changed")) {
			t.Errorf("%s: %q, want it to say %q and that nothing changed", name, err, tc.said)
		}
		if after := u67UserPosition(node); after != before {
			t.Errorf("%s moved the pair", name)
		}
	}
	// Inc on a stored string is a state refusal, which only the owner can see.
	before := u67UserPosition(node)
	err = u67RawUpdate(node, cluster.FieldOp{FieldID: id("City"), OpCode: cluster.UpdateInc, Value: []byte{0x10, 1, 0, 0, 0}})
	u30iCode(t, err, cluster.CodeInvalidArgument)
	if after := u67UserPosition(node); after != before {
		t.Error("an inc on a stored string moved the pair")
	}
	u67StillWritableUser(t, node)
}

// TestU67UpdateIsDedupedAndRebuildsFromTheLog: [GUARD the request id reaches the writer; the
// record replays to the same state].
func TestU67UpdateIsDedupedAndRebuildsFromTheLog(t *testing.T) {
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	u30iSchema(t, node)
	ctx := context.Background()
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser("u1", "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	request := [16]byte{0x67, 5}
	first, err := u67Update(node, UpdateRequest{Inc: bson.D{{Key: "Age", Value: int32(4)}}, Set: bson.D{{Key: "City", Value: "Bergen"}}}, WithRequestID(request))
	if err != nil {
		t.Fatal(err)
	}
	again, err := u67Update(node, UpdateRequest{Inc: bson.D{{Key: "Age", Value: int32(4)}}, Set: bson.D{{Key: "City", Value: "Bergen"}}}, WithRequestID(request))
	if err != nil || !again.Deduped || again.Pos != first.Pos {
		t.Fatalf("the retry = %+v, %v; want the first answer deduped", again, err)
	}
	if got := u67User(t, node).Lookup("Age").Int32(); got != 40 {
		t.Fatalf("a retried inc applied twice: Age %d, want 40", got)
	}
	if _, err := u67Update(node, UpdateRequest{Unset: []string{"Name"}}); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	loseStoreForTheReplay(t, dir)
	reopened := u30iOpen(t, dir)
	doc, err := reopened.Get(ctx, u30iDB, "User", "u1")
	if err != nil {
		t.Fatal(err)
	}
	body := bson.Raw(doc.Body)
	if _, err := body.LookupErr("Name"); err == nil || body.Lookup("Age").Int32() != 40 || body.Lookup("City").StringValue() != "Bergen" {
		t.Fatalf("after the rebuild: %v", body)
	}
}

func u67FieldID(t *testing.T, ref bucketRef, name string) uint16 {
	t.Helper()
	entry, ok := entryNamed(ref.table, name)
	if !ok {
		t.Fatalf("no field %s", name)
	}
	return entry.ID
}

// u67NoStores declares no store field, so every field id reads as a scalar.
type u67NoStores struct{}

func (u67NoStores) FieldEngine(uint64, uint64, uint16) (store.FieldEngine, error) {
	return 0, errUndeclaredField
}

// TestU67UpdateBatchBoundOnAFaultStore: [GUARD the batch bound in updateStateRefusal]. An
// update whose values do not fit one batch would be appended and then fail to apply.
func TestU67UpdateBatchBoundOnAFaultStore(t *testing.T) {
	world := u72BoundSeed(t, 64<<10, map[uint16][]byte{2: stringValue("x")})
	op := world.update(cluster.FieldOp{FieldID: 3, OpCode: cluster.UpdateSet, Value: stringValue(strings.Repeat("x", 40<<10))})
	if code := world.precondition(t, op); code != 0 {
		t.Fatalf("a 40 KiB set under a 64 KiB bound: code %v, want accepted", code)
	}
	op.Fields = append(op.Fields, cluster.FieldOp{FieldID: 4, OpCode: cluster.UpdateSet, Value: stringValue(strings.Repeat("x", 40<<10))})
	if code := world.precondition(t, op); code != cluster.CodeInvalidArgument {
		t.Fatalf("two 40 KiB sets under a 64 KiB bound: code %v, want INVALID_ARGUMENT", code)
	}
	words, said := world.applier.Explain(cluster.CodeInvalidArgument, u35Slot, u35DB, op)
	if !said || !strings.Contains(words, "batch") || !strings.Contains(words, "Nothing was changed") {
		t.Fatalf("Explain = %q, %v; want the batch named and that nothing changed", words, said)
	}
}
