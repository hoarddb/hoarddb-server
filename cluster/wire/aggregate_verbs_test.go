package wire

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// aggModel is the client-side view of one store: the elements it holds, leased or not.
type aggModel struct{ elements []bson.D }

func (m *aggModel) add(element bson.D) { m.elements = append(m.elements, element) }

// remove drops one element equal to raw, which is how the model learns what a pop or take removed.
func (m *aggModel) remove(t *testing.T, raw []byte) {
	t.Helper()
	for i, element := range m.elements {
		encoded, err := bson.Marshal(element)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) == string(raw) {
			m.elements = append(m.elements[:i], m.elements[i+1:]...)
			return
		}
	}
	t.Fatalf("the model holds no element %v", bson.Raw(raw))
}

// aggSetOf is the declared aggregates of one store field of Ledger.
func aggSetOf(t *testing.T, node *Node, field string) machine.AggregateSet {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField(field)
	if err != nil {
		t.Fatal(err)
	}
	set, ok := ref.entry.Decl.Aggregates.Set(fieldID)
	if !ok {
		t.Fatalf("%s keeps no aggregates", field)
	}
	return set
}

// check compares every aggregate of field with what the model computes from the elements.
func (m *aggModel) check(t *testing.T, node *Node, field, step string) {
	t.Helper()
	var total int64
	bySum, byCount := map[string]int64{}, map[string]uint64{}
	for _, element := range m.elements {
		amount, currency := element[0].Value.(int64), element[1].Value.(string)
		total += amount
		bySum[currency] += amount
		byCount[currency]++
	}
	for _, entry := range aggSetOf(t, node, field).Entries {
		if entry.Kind == machine.AggSum {
			if sum := aggPlainSum(t, node, field, entry.ID); sum.Whole != total {
				t.Fatalf("after %s: %s sum %d, the elements add up to %d", step, field, sum.Whole, total)
			}
			continue
		}
		for _, currency := range []string{"USD", "EUR", "GBP"} {
			raw, ok := aggRead(t, node, field, entry.ID, currency)
			if byCount[currency] == 0 {
				if ok {
					t.Fatalf("after %s: %s keeps a key for %s, which no element holds", step, field, currency)
				}
				continue
			}
			if !ok {
				t.Fatalf("after %s: %s has no key for %s", step, field, currency)
			}
			if entry.Kind == machine.AggSumBy {
				sum, _, n, err := store.DecodeSumByValue(nil, raw)
				if err != nil || sum.Whole != bySum[currency] || n != byCount[currency] {
					t.Fatalf("after %s: %s sum by %s = %d over %d (%v), want %d over %d", step, field, currency, sum.Whole, n, err, bySum[currency], byCount[currency])
				}
			} else if n, err := store.DecodeCountByValue(nil, raw); err != nil || n != byCount[currency] {
				t.Fatalf("after %s: %s count by %s = %d (%v), want %d", step, field, currency, n, err, byCount[currency])
			}
		}
	}
}

// TestEveryStoreVerbKeepsTheSumExact drives every store verb shape and lease class once on the store
// kinds that take it and, after each step, compares the plain sum, the sum by and the count by with what a
// client computes from the elements it knows the store holds. A verb that forgets its aggregate, or
// a store kind it was not wired for, fails here.
func TestEveryStoreVerbKeepsTheSumExact(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	models := map[string]*aggModel{}
	model := func(field string) *aggModel {
		if models[field] == nil {
			models[field] = &aggModel{}
		}
		return models[field]
	}
	step := func(name, field string, act func(m *aggModel)) {
		t.Helper()
		m := model(field)
		act(m)
		m.check(t, node, field, name)
	}
	push := func(field string, amount int64, currency string, extra ...func(*PushRequest)) func(*aggModel) {
		return func(m *aggModel) {
			element := bson.D{{Key: "Amount", Value: amount}, {Key: "Currency", Value: currency}}
			req := PushRequest{StoreTarget: aggTarget(field), Element: element}
			for _, set := range extra {
				set(&req)
			}
			if err := node.Push(ctx, req); err != nil {
				t.Fatalf("push to %s: %v", field, err)
			}
			m.add(element)
		}
	}
	// shapeElement on a fifoStore and a lifoStore
	step("fifo push", "Fifo", push("Fifo", 10, "USD"))
	step("fifo push", "Fifo", push("Fifo", 20, "EUR"))
	step("fifo push", "Fifo", push("Fifo", 30, "USD"))
	step("fifo push", "Fifo", push("Fifo", 40, "GBP"))
	step("fifo push", "Fifo", push("Fifo", 50, "USD"))
	step("lifo push", "Stack", push("Stack", 7, "USD"))
	step("lifo push", "Stack", push("Stack", 8, "USD"))
	// shapePop with and without a count, on fifo and lifo
	step("fifo pop", "Fifo", func(m *aggModel) {
		popped, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Fifo")})
		if err != nil || !popped.Found {
			t.Fatal(popped, err)
		}
		m.remove(t, popped.Element)
	})
	step("fifo pop(2)", "Fifo", func(m *aggModel) {
		popped, err := node.PopN(ctx, PopNRequest{StoreTarget: aggTarget("Fifo"), Count: 2})
		if err != nil || len(popped.Elements) != 2 {
			t.Fatal(popped, err)
		}
		for _, raw := range popped.Elements {
			m.remove(t, raw)
		}
	})
	step("lifo pop", "Stack", func(m *aggModel) {
		popped, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Stack")})
		if err != nil || !popped.Found {
			t.Fatal(popped, err)
		}
		m.remove(t, popped.Element)
	})
	// the lease classes: claim changes nothing, extend and release change nothing, ack subtracts
	step("fifo push", "Fifo", push("Fifo", 60, "EUR"))
	step("fifo push", "Fifo", push("Fifo", 70, "EUR"))
	var lease Lease
	step("fifo claim", "Fifo", func(m *aggModel) {
		var err error
		if lease, err = node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 1}); err != nil || len(lease.Elements) != 1 {
			t.Fatal(lease, err)
		}
	})
	step("fifo extend", "Fifo", func(m *aggModel) {
		if _, err := node.Extend(ctx, ExtendRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID, LeaseMs: 60000}); err != nil {
			t.Fatal(err)
		}
	})
	step("fifo release", "Fifo", func(m *aggModel) {
		if _, err := node.Release(ctx, ReleaseRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID}); err != nil {
			t.Fatal(err)
		}
	})
	step("fifo claim", "Fifo", func(m *aggModel) {
		var err error
		if lease, err = node.Claim(ctx, ClaimRequest{StoreTarget: aggTarget("Fifo"), Count: 2}); err != nil || len(lease.Elements) != 2 {
			t.Fatal(lease, err)
		}
	})
	step("fifo ack", "Fifo", func(m *aggModel) {
		if err := node.Ack(ctx, AckRequest{StoreTarget: aggTarget("Fifo"), Lease: lease.ID}); err != nil {
			t.Fatal(err)
		}
		for _, raw := range lease.Elements {
			m.remove(t, raw)
		}
	})
	// shapeHeapPush: a new entry, then the same (priority, entry id) replaced, then pop and take by entry id
	heapPush := func(id string, priority, amount int64, currency string) func(*aggModel) {
		return func(m *aggModel) {
			element := bson.D{{Key: "Amount", Value: amount}, {Key: "Currency", Value: currency}}
			if err := node.Push(ctx, PushRequest{StoreTarget: aggTarget("Ranked"), Element: element, EntryID: id, Priority: priority}); err != nil {
				t.Fatal(err)
			}
			m.add(element)
		}
	}
	step("heap push", "Ranked", heapPush("a", 1, 11, "USD"))
	step("heap push", "Ranked", heapPush("b", 2, 22, "EUR"))
	step("heap push", "Ranked", heapPush("c", 3, 33, "USD"))
	step("heap push replacing (3, c)", "Ranked", func(m *aggModel) {
		old := bson.D{{Key: "Amount", Value: int64(33)}, {Key: "Currency", Value: "USD"}}
		raw, _ := bson.Marshal(old)
		m.remove(t, raw)
		heapPush("c", 3, 44, "EUR")(m)
	})
	step("heap pop", "Ranked", func(m *aggModel) {
		popped, err := node.Pop(ctx, PopRequest{StoreTarget: aggTarget("Ranked")})
		if err != nil || !popped.Found {
			t.Fatal(popped, err)
		}
		m.remove(t, popped.Element)
	})
	step("heap take by entry id", "Ranked", func(m *aggModel) {
		taken, err := node.Take(ctx, TakeRequest{StoreTarget: aggTarget("Ranked"), Key: "a"})
		if err != nil || !taken.Found {
			t.Fatal(taken, err)
		}
		m.remove(t, taken.Element)
	})
	// shapePut and shapeTakeKey on a btreeStore and a hashStore: new, replace, take
	for _, field := range []string{"Tree", "Table"} {
		put := func(key string, amount int64, currency string) func(*aggModel) {
			return func(m *aggModel) {
				element := bson.D{{Key: "Amount", Value: amount}, {Key: "Currency", Value: currency}}
				got, err := node.GetField(ctx, StoreGetRequest{StoreTarget: aggTarget(field), Key: key})
				if err == nil && got.Found {
					m.remove(t, got.Element)
				}
				if err := node.Put(ctx, PutRequest{StoreTarget: aggTarget(field), Key: key, Element: element}); err != nil {
					t.Fatal(err)
				}
				m.add(element)
			}
		}
		step(field+" put new", field, put("a", 5, "USD"))
		step(field+" put new", field, put("b", 6, "EUR"))
		step(field+" put replace", field, put("a", 9, "EUR"))
		step(field+" inc existing", field, func(m *aggModel) {
			got, err := node.GetField(ctx, StoreGetRequest{StoreTarget: aggTarget(field), Key: "a"})
			if err != nil || !got.Found {
				t.Fatal(got, err)
			}
			if _, err := node.Inc(ctx, IncRequest{StoreTarget: aggTarget(field), Key: "a", Increments: bson.D{{Key: "Amount", Value: int32(4)}}}); err != nil {
				t.Fatal(err)
			}
			m.remove(t, got.Element)
			m.add(bson.D{{Key: "Amount", Value: int64(13)}, {Key: "Currency", Value: "EUR"}}) // the replaced "a" held 9 EUR
		})
		step(field+" take key", field, func(m *aggModel) {
			taken, err := node.Take(ctx, TakeRequest{StoreTarget: aggTarget(field), Key: "b"})
			if err != nil || !taken.Found {
				t.Fatal(taken, err)
			}
			m.remove(t, taken.Element)
		})
	}
}

// TestEveryApplyVerbArmKeepsTheAggregate is the guard that a new store verb cannot forget its aggregate:
// every case of applyVerb's switch must reach aggAdd (directly, or through fillPopN, fillTakeKey or fillInc,
// which must themselves call it), and every verbShape must have a case.
func TestEveryApplyVerbArmKeepsTheAggregate(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "writepath_apply.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	incFile, err := parser.ParseFile(fset, "inc_apply.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	storeFile, err := parser.ParseFile(fset, "storeverbarg.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	ast.Inspect(storeFile, func(n ast.Node) bool {
		if spec, ok := n.(*ast.ValueSpec); ok {
			for _, name := range spec.Names {
				if len(name.Name) > 5 && name.Name[:5] == "shape" && name.Name != "shape" {
					declared[name.Name] = true
				}
			}
		}
		return true
	})
	mentions := func(node ast.Node, name string) bool {
		found := false
		ast.Inspect(node, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok && ident.Name == name {
				found = true
			}
			return !found
		})
		return found
	}
	funcs := map[string]*ast.FuncDecl{}
	for _, parsed := range []*ast.File{file, incFile} {
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				funcs[fn.Name.Name] = fn
			}
		}
	}
	for _, name := range []string{"applyVerb", "fillPopN", "fillTakeKey", "fillInc"} {
		if funcs[name] == nil {
			t.Fatalf("%s is gone: update this guard with it", name)
		}
	}
	for _, name := range []string{"fillPopN", "fillTakeKey", "fillInc"} {
		if !mentions(funcs[name], "aggAdd") {
			t.Errorf("%s never calls aggAdd", name)
		}
	}
	covered := map[string]bool{}
	ast.Inspect(funcs["applyVerb"], func(n ast.Node) bool {
		clause, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, expr := range clause.List {
			ident, isIdent := expr.(*ast.Ident)
			if !isIdent || !declared[ident.Name] {
				continue
			}
			covered[ident.Name] = true
			ok := false
			for _, stmt := range clause.Body {
				ok = ok || mentions(stmt, "aggAdd") || mentions(stmt, "fillPopN") || mentions(stmt, "fillTakeKey") || mentions(stmt, "fillInc")
			}
			if !ok {
				t.Errorf("the applyVerb arm of %s never reaches aggAdd", ident.Name)
			}
		}
		return true
	})
	var missing []string
	for name := range declared {
		if !covered[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("verb shapes %v have no applyVerb arm: add one that maintains the aggregates, and a row in TestEveryStoreVerbKeepsTheSumExact", missing)
	}
}
