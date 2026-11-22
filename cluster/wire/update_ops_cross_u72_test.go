package wire

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestU72OneStatementAppliesTogetherOrNotAtAll: [GUARD updateStateRefusal checks every
// operation before the one position, and fillUpdate writes one batch].
func TestU72OneStatementAppliesTogetherOrNotAtAll(t *testing.T) {
	node := u72Node(t)
	u72Put(t, node, "Qty", int32(3))
	u72Put(t, node, "Tags", bson.A{"a"})
	u72Put(t, node, "Price", 9.5)
	together := UpdateRequest{
		Mul: bson.D{{Key: "Qty", Value: int32(4)}}, Push: bson.D{{Key: "Tags", Value: "b"}},
		Min: bson.D{{Key: "Price", Value: 2.5}}, Rename: bson.D{{Key: "Label", Value: "Alias"}},
		Max: bson.D{{Key: "Seen", Value: bson.D{}}},
	}
	// The last operation cannot be applied, so none of the others may be.
	before, position := u72Item(t, node), u72Position(node)
	u72Refused(t, node, "a statement with one bad operation", together, "max")
	if after := u72Item(t, node); string(after) == string(before) && u72Position(node) != position {
		t.Error("the refusal moved the pair")
	}
	for _, key := range []string{"Qty", "Price"} {
		if got, want := u72Item(t, node).Lookup(key), before.Lookup(key); got.String() != want.String() {
			t.Errorf("%s changed to %v by a refused statement", key, got)
		}
	}
	if len(u72Tags(t, node)) != 1 {
		t.Error("the push of a refused statement was applied")
	}
	together.Max = bson.D{{Key: "Seen", Value: int64(5)}}
	updated, err := u72Update(node, together)
	if err != nil || updated.Pos.Zero() {
		t.Fatalf("the statement: %+v, %v", updated, err)
	}
	doc := u72Item(t, node)
	u72Same(t, "Qty", doc.Lookup("Qty"), int32(12))
	u72Same(t, "Price", doc.Lookup("Price"), 2.5)
	u72Same(t, "Alias", doc.Lookup("Alias"), "a")
	u72Same(t, "Seen", doc.Lookup("Seen"), int64(5))
	if tags := u72Tags(t, node); len(tags) != 2 {
		t.Errorf("Tags = %v", tags)
	}
}

// TestU72TwoOperationsOnOneFieldAreRefused: [GUARD updateOpsRefusal's conflict rule, as MongoDB].
func TestU72TwoOperationsOnOneFieldAreRefused(t *testing.T) {
	node := u72Node(t)
	one := bson.D{{Key: "Qty", Value: int32(1)}}
	for name, req := range map[string]UpdateRequest{
		"mul and min":       {Mul: one, Min: one},
		"push and addToSet": {Push: bson.D{{Key: "Tags", Value: "x"}}, AddToSet: bson.D{{Key: "Tags", Value: "x"}}},
		"max and unset":     {Max: one, Unset: []string{"Qty"}},
		"rename source":     {Rename: bson.D{{Key: "Qty", Value: "Spare"}}, Mul: one},
	} {
		u72Refused(t, node, name, req, "two operations")
	}
}

// TestU72UpdateIsDedupedAndRebuildsFromTheLog: [GUARD the request id reaches the writer: a
// retried mul must not square; the logged records replay to the same state].
func TestU72UpdateIsDedupedAndRebuildsFromTheLog(t *testing.T) {
	dir := t.TempDir()
	node := u30iOpen(t, dir)
	u30iSchema(t, node)
	ctx := context.Background()
	if _, err := node.CreateBucket(ctx, u30iDB, "Item",
		Field{Name: "Qty", Type: "int"}, Field{Name: "Tags", Type: "array"}, Field{Name: "Label", Type: "string"},
		Field{Name: "Alias", Type: "string"}, Field{Name: "Price", Type: "double"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Insert(ctx, u30iDB, "Item", bson.D{{Key: "_id_", Value: "i1"}, {Key: "Qty", Value: int32(3)}, {Key: "Label", Value: "a"}, {Key: "Price", Value: 8.0}}); err != nil {
		t.Fatal(err)
	}
	statement := UpdateRequest{
		Mul: bson.D{{Key: "Qty", Value: int32(5)}}, Push: bson.D{{Key: "Tags", Value: "t"}},
		Max: bson.D{{Key: "Price", Value: 9.0}}, Rename: bson.D{{Key: "Label", Value: "Alias"}},
	}
	request := [16]byte{0x72, 1}
	first, err := u72Update(node, statement, WithRequestID(request))
	if err != nil {
		t.Fatal(err)
	}
	again, err := u72Update(node, statement, WithRequestID(request))
	if err != nil || !again.Deduped || again.Pos != first.Pos {
		t.Fatalf("the retry = %+v, %v; want the first answer, deduped", again, err)
	}
	check := func(label string, doc bson.Raw) {
		t.Helper()
		u72Same(t, label+" Qty", doc.Lookup("Qty"), int32(15))
		u72Same(t, label+" Price", doc.Lookup("Price"), 9.0)
		u72Same(t, label+" Alias", doc.Lookup("Alias"), "a")
		if values, err := doc.Lookup("Tags").Array().Values(); err != nil || len(values) != 1 {
			t.Errorf("%s: Tags = %v, %v; a retried push applied twice", label, values, err)
		}
		if _, err := doc.LookupErr("Label"); err == nil {
			t.Errorf("%s: Label survived the rename", label)
		}
	}
	check("after the retry", u72ItemOf(t, node))
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for round := range 2 {
		if round == 0 {
			loseStoreForTheReplay(t, dir)
		}
		reopened := u30iOpen(t, dir)
		check("after reopening", u72ItemOf(t, reopened))
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func u72ItemOf(t *testing.T, node *Node) bson.Raw {
	t.Helper()
	return u72Item(t, node)
}

// TestU72OperatorsNeverPanicOrWriteInvalidBSON: [GUARD every parser checks lengths before
// it reads]. 100,000 mutated stored values and operands go through every operator; what
// an operator would write is always one valid BSON value.
func TestU72OperatorsNeverPanicOrWriteInvalidBSON(t *testing.T) {
	seeds := [][]byte{
		intValue(5), append([]byte{byte(bsontype.Int64)}, 1, 2, 3, 4, 5, 6, 7, 8), append([]byte{byte(bsontype.Double)}, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f),
		stringValue("abc"), {byte(bsontype.Boolean), 1}, {byte(bsontype.Null)}, append([]byte{byte(bsontype.DateTime)}, 1, 2, 3, 4, 5, 6, 7, 8),
		append([]byte{byte(bsontype.Timestamp)}, 1, 2, 3, 4, 5, 6, 7, 8),
		append([]byte{byte(bsontype.Array)}, mustRaw(t, bson.D{{Key: "0", Value: "a"}, {Key: "1", Value: int32(2)}})...),
		append([]byte{byte(bsontype.EmbeddedDocument)}, mustRaw(t, bson.D{{Key: "a", Value: int32(1)}})...),
		append([]byte{byte(bsontype.Binary)}, 2, 0, 0, 0, 0, 7, 8),
		append([]byte{byte(bsontype.ObjectID)}, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
	}
	rng := rand.New(rand.NewSource(72))
	mutate := func() []byte {
		out := append([]byte(nil), seeds[rng.Intn(len(seeds))]...)
		for range rng.Intn(4) {
			switch {
			case len(out) == 0 || rng.Intn(6) == 0:
				out = append(out, byte(rng.Intn(256)))
			case rng.Intn(5) == 0:
				out = out[:rng.Intn(len(out))]
			default:
				out[rng.Intn(len(out))] = byte(rng.Intn(256))
			}
		}
		return out
	}
	codes := []byte{cluster.UpdateInc, cluster.UpdateMul, cluster.UpdateMin, cluster.UpdateMax, cluster.UpdatePush, cluster.UpdateAddToSet}
	for range 100_000 {
		stored, operand := mutate(), mutate()
		if rng.Intn(8) == 0 {
			stored = nil
		}
		code := codes[rng.Intn(len(codes))]
		next, err := updateResult(code, stored, operand)
		if err != nil || next == nil {
			continue
		}
		if err := ringCheckTypedValue(next); err != nil {
			t.Fatalf("%s of %x onto %x wrote %x, which is not valid BSON: %v", updateCodeName(code), operand, stored, next, err)
		}
	}
}

func mustRaw(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// u72Serve puts node behind the ring handler on a TLS listener on a port the kernel picks,
// with the header of bucket (u66Serve is fixed on the bucket Box).
func u72Serve(t *testing.T, node *Node, bucket string) *u66Env {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, bucket)
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg, _, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRingHandler(node, u66AuthNext, u70AllowAll{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := transport.ListenTCP("127.0.0.1:0", tlsCfg, transport.NewConnLimiter(64, 64), transport.TCPConfig{},
		handler, &transport.SessionDeps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return &u66Env{node: node, addr: listener.Addr().String(), ref: ref}
}

// TestU72OperatorsOverTLS: [GUARD the ring handler takes the six codes and refuses a rename
// target of another type by name].
func TestU72OperatorsOverTLS(t *testing.T) {
	node := u72Node(t)
	env := u72Serve(t, node, "Item")
	c := env.login(t)
	id := func(name string) uint16 { return env.fieldID(t, name) }
	send := func(fields ...cluster.FieldOp) *transport.Frame { return c.keyed(env.update(t, "i1", fields...)) }
	u72Put(t, node, "Qty", int32(6))
	reply := u66Reply(t, send(
		cluster.FieldOp{FieldID: id("Qty"), OpCode: cluster.UpdateMul, Value: intValue(7)},
		cluster.FieldOp{FieldID: id("Tags"), OpCode: cluster.UpdatePush, Value: stringValue("x")},
		cluster.FieldOp{FieldID: id("Price"), OpCode: cluster.UpdateMax, Value: append([]byte{byte(bsontype.Double)}, 0, 0, 0, 0, 0, 0, 0x24, 0x40)},
		cluster.FieldOp{FieldID: id("Label"), OpCode: cluster.UpdateRenameTo, Value: []byte{byte(id("Alias") >> 8), byte(id("Alias"))}}))
	if reply.WriteOutcome != "applied" || reply.Position == nil {
		t.Fatalf("reply %+v", reply)
	}
	doc := u72Item(t, node)
	u72Same(t, "Qty", doc.Lookup("Qty"), int32(42))
	u72Same(t, "Alias", doc.Lookup("Alias"), "a")
	u72Same(t, "Price", doc.Lookup("Price"), 10.0)
	before := u72Position(node)
	refusal := u66Refusal(t, send(cluster.FieldOp{FieldID: id("Qty"), OpCode: cluster.UpdateRenameTo, Value: []byte{byte(id("Label") >> 8), byte(id("Label"))}}), cluster.CodeInvalidArgument)
	for _, word := range []string{"Qty", "Label", "same type", "Nothing was changed"} {
		if !strings.Contains(refusal.Message, word) {
			t.Errorf("the rename refusal %q does not say %q", refusal.Message, word)
		}
	}
	refusal = u66Refusal(t, send(cluster.FieldOp{FieldID: id("Alias"), OpCode: cluster.UpdateMul, Value: intValue(2)}), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "mul") || !strings.Contains(refusal.Message, "string") {
		t.Errorf("mul on a stored string: %q", refusal.Message)
	}
	u66Refusal(t, send(cluster.FieldOp{FieldID: id("Log"), OpCode: cluster.UpdatePush, Value: stringValue("x")}), cluster.CodeFieldType)
	u66Refusal(t, send(cluster.FieldOp{FieldID: id("Qty"), OpCode: cluster.UpdateRenameTo, Value: []byte{byte(id("Log") >> 8), byte(id("Log"))}}), cluster.CodeInvalidArgument)
	if after := u72Position(node); after != before {
		t.Fatalf("refused updates moved the pair from %+v to %+v", before, after)
	}
	// A retried mul is answered from the dedupe window and does not multiply twice.
	payload, err := EncodeRingKeyed(env.update(t, "i1", cluster.FieldOp{FieldID: id("Qty"), OpCode: cluster.UpdateMul, Value: intValue(2)}))
	if err != nil {
		t.Fatal(err)
	}
	request := u66ID()
	first := u66Reply(t, c.send(transport.OpRingKeyed, request, payload))
	second := u66Reply(t, c.send(transport.OpRingKeyed, request, payload))
	if first.Deduped || !second.Deduped || *first.Position != *second.Position {
		t.Fatalf("first %+v, retry %+v", first, second)
	}
	u72Same(t, "Qty after a retried mul", u72Item(t, node).Lookup("Qty"), int32(84))
}

// TestU72RawWireModifierIsRefusedByTheOwner: [GUARD the owner refuses a modifier document sent
// by a client that is not the Go one].
func TestU72RawWireModifierIsRefusedByTheOwner(t *testing.T) {
	node := u72Node(t)
	env := u72Serve(t, node, "Item")
	c := env.login(t)
	before := u72Position(node)
	body := append([]byte{byte(bsontype.EmbeddedDocument)}, mustRaw(t, bson.D{{Key: "$each", Value: bson.A{"a", "b"}}})...)
	for _, code := range []byte{cluster.UpdatePush, cluster.UpdateAddToSet} {
		refusal := u66Refusal(t, c.keyed(env.update(t, "i1", cluster.FieldOp{FieldID: env.fieldID(t, "Tags"), OpCode: code, Value: body})), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "$each") || !strings.Contains(refusal.Message, "Nothing was changed") {
			t.Errorf("%s: %q", updateCodeName(code), refusal.Message)
		}
	}
	if after := u72Position(node); after != before {
		t.Error("a refused modifier moved the pair")
	}
}
