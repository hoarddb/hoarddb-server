package main

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// -ledger-expenses sets the depth of the ledger the world holds, the way a deep seed was made.
func TestWorldDepthSetsTheLedgerExpenses(t *testing.T) {
	w, err := newWorldWith(7, 1, model.Options{LedgerExpenses: 300})
	if err != nil {
		t.Fatal(err)
	}
	if got := w.sets[model.Ledger].count("Account.Expenses"); got != 30*300 {
		t.Errorf("the ledger holds %d expenses, want %d", got, 30*300)
	}
	plain, err := newWorld(7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := plain.sets[model.Ledger].count("Account.Expenses"); got != 30*100 {
		t.Errorf("the default ledger holds %d expenses, want 3000", got)
	}
	if w.sets[model.Ledger].finger == plain.sets[model.Ledger].finger {
		t.Error("a deep ledger and the default one must have different fingerprints")
	}
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Agent} {
		if w.sets[layout].finger != plain.sets[layout].finger {
			t.Errorf("%s must not change with the ledger depth", layout)
		}
	}
}

func TestRunFlagLedgerExpenses(t *testing.T) {
	args := []string{"-engine", "ring", "-facts", "f.json", "-out", "o", "-data-dir", "/d"}
	cfg, err := parseRunFlags(append(slicesClone(args), "-ledger-expenses", "5000"))
	if err != nil || cfg.LedgerExpenses != 5000 {
		t.Fatalf("depth = %d, err = %v", cfg.LedgerExpenses, err)
	}
	cfg, err = parseRunFlags(args)
	if err != nil || cfg.LedgerExpenses != 0 {
		t.Fatalf("the default depth is 0 (the default ledger): %d, %v", cfg.LedgerExpenses, err)
	}
	if _, err = parseRunFlags(append(slicesClone(args), "-ledger-expenses", "-4")); err == nil || !strings.Contains(err.Error(), "-ledger-expenses") {
		t.Errorf("a negative depth must be refused naming the flag: %v", err)
	}
}

func slicesClone(in []string) []string { return append([]string(nil), in...) }

func TestRecordDepthKeepsLegsApart(t *testing.T) {
	plain, deep := completeRecord(), completeRecord()
	deep.LedgerExpenses = 10000
	if plain.legName() != completeRecord().legName() {
		t.Error("a default-depth record keeps the leg name it always had")
	}
	if deep.legName() != plain.legName()+"-deep10000" {
		t.Errorf("a deep record is named %q, want the plain name plus -deep10000", deep.legName())
	}
	both := completeRecord()
	both.Aggregates, both.LedgerExpenses = "on", 500
	if want := plain.legName() + "-agg-deep500"; both.legName() != want {
		t.Errorf("the leg is %q, want %q", both.legName(), want)
	}
}

// The runner records the depth the world was built with.
func TestRunnerRecordsTheLedgerDepth(t *testing.T) {
	p, _ := fakeRun(t, nil)
	w, err := newWorldWith(7, 1, model.Options{LedgerExpenses: 150})
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeEngine("bat")
	for _, layout := range benchLayouts {
		if err := fake.loadWith(layout, 7, 1, model.Options{LedgerExpenses: 150}); err != nil {
			t.Fatal(err)
		}
	}
	p.Engine, p.World, p.LedgerExpenses = fake, w, 150
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.LedgerExpenses != 150 || !rec.AuditOK || rec.Failed {
		t.Fatalf("depth %d, audit ok %v (%s), failed %v", rec.LedgerExpenses, rec.AuditOK, rec.AuditError, rec.Failed)
	}
	if !strings.Contains(rec.legName(), "-deep150") {
		t.Errorf("leg %q lacks -deep150", rec.legName())
	}
}
