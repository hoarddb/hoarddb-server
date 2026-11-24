package model

import (
	"strings"
	"testing"
)

func TestParseVariantReadsBaseAndAggregateWords(t *testing.T) {
	cases := []struct {
		word string
		want Variant
	}{
		{"", Variant{}},
		{"common", Variant{}},
		{"native", Variant{Native: true}},
		{"common+agg", Variant{Aggregates: true}},
		{"native+agg", Variant{Native: true, Aggregates: true}},
		{"common+deep500", Variant{LedgerExpenses: 500}},
		{"common+agg+deep500", Variant{Aggregates: true, LedgerExpenses: 500}},
		{"native+deep10000+agg", Variant{Native: true, Aggregates: true, LedgerExpenses: 10000}},
	}
	for _, c := range cases {
		got, err := ParseVariant(c.word)
		if err != nil || got != c.want {
			t.Errorf("ParseVariant(%q) = %+v, %v, want %+v", c.word, got, err, c.want)
		}
	}
	for _, word := range []string{"agg", "common+", "common+native", "native+agg+agg", "Common", "common+aggs", "common+deep", "common+deep0", "common+deep-3", "common+deep1x", "common+deep5+deep6"} {
		if _, err := ParseVariant(word); err == nil {
			t.Errorf("ParseVariant(%q) was accepted", word)
		}
	}
}

func TestVariantStringIsTheCanonicalWord(t *testing.T) {
	for _, word := range []string{"common", "native", "common+agg", "native+agg", "common+deep500", "native+agg+deep20000"} {
		variant, err := ParseVariant(word)
		if err != nil || variant.String() != word {
			t.Errorf("%q parsed to %+v and printed %q (%v)", word, variant, variant.String(), err)
		}
	}
	if (Variant{}).String() != "common" {
		t.Errorf("the zero variant prints %q", (Variant{}).String())
	}
}

func TestLedgerAggregatesDeclareOnlyWhatTheModelHolds(t *testing.T) {
	decls := LedgerAggregates()
	schema := map[string]map[string]string{}
	for _, bucket := range ledgerSchema() {
		fields := map[string]string{}
		for _, field := range bucket.Fields {
			fields[field.Name] = field.Type
		}
		schema[bucket.Name] = fields
	}
	for store, list := range decls {
		bucket, field := splitStore(store)
		if schema[bucket][field] == "" || !isStoreType(schema[bucket][field]) {
			t.Errorf("%s is not a store field of the ledger schema", store)
		}
		if len(list) == 0 {
			t.Errorf("%s declares nothing", store)
		}
		for _, decl := range list {
			if decl.Kind != "sum" && decl.Kind != "sum by" && decl.Kind != "count by" {
				t.Errorf("%s: kind %q", store, decl.Kind)
			}
		}
	}
	if len(decls["Account.Expenses"]) != 3 || len(decls["Account.Invoices"]) != 1 {
		t.Errorf("ledger aggregates %v: want 3 on Expenses and 1 on Invoices", decls)
	}
}

func splitStore(name string) (bucket, field string) {
	bucket, field, _ = strings.Cut(name, ".")
	return bucket, field
}

func isStoreType(typeWord string) bool { return strings.HasSuffix(typeWord, "Store") }

func ledgerKeys(t *testing.T, gen *Generator) (map[string][]string, map[string]int64) {
	t.Helper()
	keys, amounts := map[string][]string{}, map[string]int64{}
	for element := range gen.Elements("Account.Expenses") {
		keys[element.Owner] = append(keys[element.Owner], element.Key)
		for _, field := range element.Body {
			if field.Name == "amount" {
				amounts[element.Owner+"/"+element.Key] = field.Value.(int64)
			}
		}
	}
	return keys, amounts
}

func TestLedgerExpensesOptionSetsTheDepthAndKeepsTheFirstOnes(t *testing.T) {
	plain, err := New(Ledger, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := NewWith(Ledger, 1, 1, Options{})
	if err != nil || zero.Counts()["Account.Expenses"] != plain.Counts()["Account.Expenses"] {
		t.Fatalf("the zero Options must be the default ledger: %v", err)
	}
	if plain.Counts()["Account.Expenses"] != 30*100 {
		t.Fatalf("the default ledger holds %d expenses, want 3000", plain.Counts()["Account.Expenses"])
	}
	deep, err := NewWith(Ledger, 1, 1, Options{LedgerExpenses: 250})
	if err != nil {
		t.Fatal(err)
	}
	if got := deep.Counts()["Account.Expenses"]; got != 30*250 {
		t.Fatalf("depth 250 holds %d expenses, want %d", got, 30*250)
	}
	plainKeys, plainAmounts := ledgerKeys(t, plain)
	deepKeys, deepAmounts := ledgerKeys(t, deep)
	for owner, keys := range plainKeys {
		if len(deepKeys[owner]) != 250 {
			t.Fatalf("%s holds %d expenses at depth 250", owner, len(deepKeys[owner]))
		}
		for i, key := range keys {
			if deepKeys[owner][i] != key || deepAmounts[owner+"/"+key] != plainAmounts[owner+"/"+key] {
				t.Fatalf("%s expense %d differs between the default and the deep ledger", owner, i)
			}
		}
	}
	for unit := range deep.Units() {
		for _, counter := range unit.Doc.Counters {
			if counter.Name == "expense_n" && counter.Value.(int64) != 250 {
				t.Fatalf("%s expense_n is %v at depth 250", unit.Doc.ID, counter.Value)
			}
		}
	}
	if _, err := NewWith(Ledger, 1, 1, Options{LedgerExpenses: -1}); err == nil {
		t.Error("a negative depth must be refused")
	}
	if _, err := NewWith(Social, 1, 1, Options{LedgerExpenses: 500}); err != nil {
		t.Errorf("the depth belongs to the ledger and is ignored by another layout: %v", err)
	}
}

// Keys list in time order whatever the depth, so a btree store lists a deep account oldest first.
func TestDeepLedgerKeysStayInOrder(t *testing.T) {
	deep, err := NewWith(Ledger, 1, 1, Options{LedgerExpenses: 12000})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := ledgerKeys(t, deep)
	for owner, list := range keys {
		for i := 1; i < len(list); i++ {
			if list[i-1] >= list[i] {
				t.Fatalf("%s: key %d %q is not after %q", owner, i, list[i], list[i-1])
			}
		}
		break
	}
}
