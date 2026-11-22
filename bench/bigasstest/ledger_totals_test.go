package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func firstAccount(t *testing.T, w *world) string {
	t.Helper()
	return w.sets[model.Ledger].ids("Account")[0]
}

func TestExpectedTotalsAreTheSeededSumsAndCountsByCategory(t *testing.T) {
	w := testWorld(t)
	account := firstAccount(t, w)
	want := map[string]CategoryTotal{}
	for _, e := range w.sets[model.Ledger].elements(StoreRef{"Account", account, "Expenses"}) {
		category := bodyString(e, "category")
		entry := want[category]
		entry.Category, entry.Total, entry.Count = category, entry.Total+fieldInt(e, "amount"), entry.Count+1
		want[category] = entry
	}
	got := expectedTotals(w.sets[model.Ledger], account)
	if len(got) != len(want) || len(got) < 2 {
		t.Fatalf("expected %d categories, got %v", len(want), got)
	}
	for i, entry := range got {
		if entry != want[entry.Category] {
			t.Errorf("%s: %+v, want %+v", entry.Category, entry, want[entry.Category])
		}
		if i > 0 && got[i-1].Category >= entry.Category {
			t.Errorf("categories out of order: %v", got)
		}
	}
}

func TestVerifyTotalsRefusesWhatIsNotTheSeededTotals(t *testing.T) {
	w := testWorld(t)
	account := firstAccount(t, w)
	good := expectedTotals(w.sets[model.Ledger], account)
	op := ReadOp{Layout: model.Ledger, Kind: ReadLedgerTotals, Entity: account}
	if err := verifyRead(w, newRegistry(), op, Reply{Totals: good}); err != nil {
		t.Fatalf("the seeded totals failed: %v", err)
	}
	cases := map[string]func([]CategoryTotal) ([]CategoryTotal, string){
		"a wrong total": func(in []CategoryTotal) ([]CategoryTotal, string) { in[0].Total++; return in, in[0].Category },
		"a wrong count": func(in []CategoryTotal) ([]CategoryTotal, string) { in[1].Count--; return in, in[1].Category },
		"a missing row": func(in []CategoryTotal) ([]CategoryTotal, string) { return in[1:], in[0].Category },
		"an invented row": func(in []CategoryTotal) ([]CategoryTotal, string) {
			return append(in, CategoryTotal{"zzz", 1, 1}), "zzz"
		},
		"rows out of order": func(in []CategoryTotal) ([]CategoryTotal, string) { in[0], in[1] = in[1], in[0]; return in, "order" },
	}
	for name, corrupt := range cases {
		bad, mention := corrupt(append([]CategoryTotal(nil), good...))
		err := verifyRead(w, newRegistry(), op, Reply{Totals: bad})
		if err == nil || !strings.Contains(err.Error(), mention) {
			t.Errorf("%s: got %v, want a failure naming %q", name, err, mention)
		}
	}
	if err := verifyRead(w, newRegistry(), op, Reply{Totals: good, Elements: []model.Element{{}}}); err == nil {
		t.Error("a totals reply that also holds elements must fail")
	}
}

func TestTotalsReadIsRecordOnlyNotInTheMix(t *testing.T) {
	if _, err := newMix(benchLayouts, string(ReadLedgerTotals)); err != nil {
		t.Fatalf("-op ledger.totals_by_category must be accepted: %v", err)
	}
	mix, err := newMix(benchLayouts, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range mix.kinds() {
		if kind == string(ReadLedgerTotals) {
			t.Error("the totals read must not join the mix: it would move the request stream")
		}
	}
	w := testWorld(t)
	if len(w.readOwners(ReadLedgerTotals)) != len(w.sets[model.Ledger].ids("Account")) {
		t.Error("the totals read starts from every account")
	}
	if got := w.readLimit(ReadLedgerTotals); got != 0 {
		t.Errorf("the totals read has no page limit, got %d", got)
	}
}

// The fake engine answers the read from its stored expenses; a run of only this op verifies every
// reply, and a fake that answers one short fails the verifier.
func TestRunnerVerifiesTheTotalsRead(t *testing.T) {
	run := func(corrupt bool) *Record {
		p, _ := fakeRun(t, func(f *fakeEngine) { f.aggregates, f.corruptTotals = true, corrupt })
		mix, err := newMix(benchLayouts, string(ReadLedgerTotals))
		if err != nil {
			t.Fatal(err)
		}
		p.Mix = mix
		rec, err := runRepetition(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	rec := run(false)
	if rec.Failed || rec.Ops[string(ReadLedgerTotals)].Count == 0 {
		t.Fatalf("failed=%v audit=%q verify=%d ops=%v", rec.Failed, rec.AuditError, rec.VerifyFailures, rec.Ops)
	}
	if rec.Ops[string(ReadLedgerTotals)].RoundTripsPerOp != 1 {
		t.Errorf("round trips per op %v, want 1", rec.Ops[string(ReadLedgerTotals)].RoundTripsPerOp)
	}
	if bad := run(true); bad.VerifyFailures == 0 || !bad.Failed {
		t.Errorf("a wrong total must fail the run: verify failures %d failed %v", bad.VerifyFailures, bad.Failed)
	}
}

func TestParseSumByRows(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "values", Value: bson.A{
		bson.D{{Key: "by", Value: "meals"}, {Key: "sum", Value: int64(900)}, {Key: "n", Value: int64(3)}},
		bson.D{{Key: "by", Value: "fees"}, {Key: "sum", Value: int64(40)}, {Key: "n", Value: int64(1)}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseSumByRows(doc)
	if err != nil || len(got) != 2 || got[0] != (CategoryTotal{"meals", 900, 3}) || got[1] != (CategoryTotal{"fees", 40, 1}) {
		t.Fatalf("rows %+v, %v", got, err)
	}
	for name, raw := range map[string]bson.D{
		"no values":             {{Key: "x", Value: 1}},
		"a double sum":          {{Key: "values", Value: bson.A{bson.D{{Key: "by", Value: "a"}, {Key: "sum", Value: 1.5}, {Key: "n", Value: int64(1)}}}}},
		"a by that is not text": {{Key: "values", Value: bson.A{bson.D{{Key: "by", Value: int64(4)}, {Key: "sum", Value: int64(1)}, {Key: "n", Value: int64(1)}}}}},
	} {
		doc, err := bson.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseSumByRows(doc); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}

// Every competitor computes the totals the idiomatic way for that engine: a GROUP BY for the SQL
// engines, a $group stage for MongoDB, over the same table or collection the pages read.
func TestTotalsQueriesGroupTheExpensesTheIdiomaticWay(t *testing.T) {
	pg, err := pgTotalsSQL(pgMaps[model.Ledger].stores["Account.Expenses"])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`FROM "expenses"`, `WHERE "account_id" = $1`, `GROUP BY "category"`, `SUM("amount")`, `COUNT(*)`} {
		if !strings.Contains(pg, want) {
			t.Errorf("the PostgreSQL query lacks %s: %s", want, pg)
		}
	}
	my, err := mysqlTotalsSQL("`db`", mysqlStores[model.Ledger]["Account.Expenses"])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"FROM `db`.`expenses`", "WHERE `account_id` = ?", "GROUP BY `category`", "SUM(`amount`)", "COUNT(*)"} {
		if !strings.Contains(my, want) {
			t.Errorf("the MySQL query lacks %s: %s", want, my)
		}
	}
	spec, err := mongoLayouts[model.Ledger].storeOf("Account", "Expenses")
	if err != nil {
		t.Fatal(err)
	}
	pipeline, err := mongoTotalsPipeline(spec, "a1")
	if err != nil {
		t.Fatal(err)
	}
	encoded := fmt.Sprintf("%v", pipeline)
	for _, want := range []string{"$match", "account_id a1", "$group", "$category", "$sum", "$amount"} {
		if !strings.Contains(encoded, want) {
			t.Errorf("the MongoDB pipeline lacks %s: %s", want, encoded)
		}
	}
}

func TestTotalsNeedTheRightColumns(t *testing.T) {
	if _, err := pgTotalsSQL(pgStore{table: "t", ownerCol: "o"}); err == nil || !strings.Contains(err.Error(), "category") {
		t.Errorf("a store with no category column must be refused naming it: %v", err)
	}
}
