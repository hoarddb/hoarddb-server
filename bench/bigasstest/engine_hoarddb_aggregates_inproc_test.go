package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// createLedger makes the ledger database and its Account bucket the way the seeder does, with or
// without the declared aggregates, on a real in-process server.
func createLedger(ctx context.Context, t *testing.T, client *ringclient.Client, base string, aggregates bool) string {
	t.Helper()
	database := layoutDatabase(base, model.Ledger)
	if _, err := client.CreateDatabase(ctx, database, 1); err != nil {
		t.Fatal(err)
	}
	gen, err := model.New(model.Ledger, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, bucket := range gen.Schema() {
		fields := make([]ringclient.FieldDecl, len(bucket.Fields))
		for i, field := range bucket.Fields {
			fields[i] = ringclient.FieldDecl{Name: field.Name, Type: field.Type}
			if aggregates {
				for _, decl := range model.LedgerAggregates()[bucket.Name+"."+field.Name] {
					fields[i].Aggregates = append(fields[i].Aggregates, ringclient.AggregateDecl{Kind: decl.Kind, Field: decl.Field, By: decl.By})
				}
			}
		}
		if _, err := client.CreateBucket(ctx, database, bucket.Name, fields); err != nil {
			t.Fatal(err)
		}
	}
	return database
}

// A session open runs the aggregates probe: -param aggregates=on on a common seed, and the reverse,
// are refused before any request, so a mislabelled record cannot be written. Deleting the probe call
// from ringSession.Layout fails this test.
func TestSessionOpenRefusesTheOtherLedgerSeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	addr := startRingServer(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "admin", Password: inprocPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	createLedger(ctx, t, client, "plain", false)
	createLedger(ctx, t, client, "agg", true)
	for _, tc := range []struct {
		base, param, want string
	}{
		{"plain", "off", ""},
		{"agg", "on", ""},
		{"plain", "on", "-layout-variant common+agg"},
		{"agg", "off", "-layout-variant common"},
	} {
		engine := openInprocRing(t, ctx, addr, map[string]string{"aggregates": tc.param})
		session, err := engine.Session(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = session.Layout(model.Ledger, layoutDatabase(tc.base, model.Ledger))
		_ = session.Close()
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("seed %s, aggregates=%s: %v", tc.base, tc.param, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("seed %s, aggregates=%s: got %v, want a refusal naming %q", tc.base, tc.param, err, tc.want)
		}
	}
}

// ReadSum against the real maintained aggregate: after puts, an overwrite and a delete-free update
// the server's sum equals the elements' sum, for Expenses and for Invoices.
func TestRingReadSumReadsTheServersMaintainedSum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	addr := startRingServer(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "admin", Password: inprocPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	database := createLedger(ctx, t, client, "agg", true)
	account := client.Bucket(database, "Account")
	if _, err := account.Insert(ctx, "a1", ringclient.Doc{"Name": "x", "Currency": "EUR"}); err != nil {
		t.Fatal(err)
	}
	put := func(store, key string, amount int64) {
		t.Helper()
		body := bson.D{{Key: "amount", Value: amount}, {Key: "category", Value: "fees"}}
		if _, err := account.Put(ctx, "a1", store, key, body); err != nil {
			t.Fatal(err)
		}
	}
	put("Expenses", "k1", 100)
	put("Expenses", "k2", 250)
	put("Expenses", "k1", 120) // an overwrite replaces, it does not add
	put("Invoices", "INV-1", 900)
	engine := openInprocRing(t, ctx, addr, map[string]string{"aggregates": "on"})
	session, err := engine.Session(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	ops, err := session.Layout(model.Ledger, database)
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := ops.(SumReader)
	if !ok {
		t.Fatal("the ring adapter must implement SumReader")
	}
	for store, want := range map[string]int64{"Expenses": 370, "Invoices": 900} {
		got, err := reader.ReadSum(ctx, StoreRef{"Account", "a1", store}, "amount")
		if err != nil || got != want {
			t.Errorf("ReadSum of %s = %d, %v, want %d", store, got, err, want)
		}
	}
}

// The shared contract, with the totals read, against a real in-process server seeded +agg and run
// with aggregates=on: every read verified, including ledger.totals_by_category answered from the
// declared aggregates, every write audited, including the maintained sums.
func TestContractRingAggregatesAndTotals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	addr := startRingServer(t)
	engine := openInprocRing(t, ctx, addr, map[string]string{"aggregates": "on"})
	runContract(t, contractTarget{Engine: engine, Name: "ring", Database: "aggin", Variant: "common+agg", Totals: true, SeedArgs: []string{"-addr", addr, "-insecure"}})
}

// Without the aggregates the totals read says how to get them instead of failing obscurely.
func TestRingTotalsNeedTheDeclaredAggregates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	addr := startRingServer(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "admin", Password: inprocPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	database := createLedger(ctx, t, client, "plain", false)
	engine := openInprocRing(t, ctx, addr, map[string]string{"aggregates": "off"})
	session, err := engine.Session(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	ops, err := session.Layout(model.Ledger, database)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ops.Read(ctx, ReadOp{Layout: model.Ledger, Kind: ReadLedgerTotals, Entity: "a1"})
	if err == nil || !strings.Contains(err.Error(), "common+agg") || !strings.Contains(err.Error(), "aggregates=on") {
		t.Errorf("got %v, want a refusal naming common+agg and aggregates=on", err)
	}
}
