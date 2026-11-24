package seed_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	serverconfig "github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/seed"
	"github.com/hoarddb/hoarddb-server/seed/model"
	"github.com/hoarddb/hoarddb-server/server"
)

const testPassword = "seed-test-password-1"

// startServer runs one in-process hoarddb-server on a free local port and returns a client on it.
func startServer(t *testing.T) *ringclient.Client {
	t.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := serverconfig.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = addr
	service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SeedRootUser("admin", testPassword); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	dialContext, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := ringclient.Dial(dialContext, ringclient.Options{Addr: addr, User: "admin", Password: testPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// smallLayout is a layout capped at two elements per store (model.Options.ElementsPerStore), so a
// live seed takes seconds instead of the minute a whole layout needs on a busy host.
func smallLayout(t *testing.T, layout model.Layout) *model.Generator {
	t.Helper()
	gen, err := model.NewWith(layout, 1, 1, model.Options{ElementsPerStore: 2})
	if err != nil {
		t.Fatal(err)
	}
	return gen
}

func ledgerLayout(t *testing.T) *model.Generator { return smallLayout(t, model.Ledger) }

// TestSeedLoadsALayoutAndTwoDatabasesHoldTheSameData seeds a small ledger into two databases of
// one server and reads a document list and a store back from both: the data is the same, so the
// how-to pages' sample output does not depend on which run produced it.
func TestSeedLoadsALayoutAndTwoDatabasesHoldTheSameData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := startServer(t)
	seeder := seed.New([]*ringclient.Client{client}, seed.Options{Workers: 16})
	gen := ledgerLayout(t)
	for _, database := range []string{"first", "second"} {
		if _, err := seeder.Seed(ctx, gen, database, seed.NewProgress(nil)); err != nil {
			t.Fatalf("seeding %s: %v", database, err)
		}
	}
	counts := gen.Counts()
	for _, database := range []string{"first", "second"} {
		got, err := client.Bucket(database, "Account").Count(ctx, nil)
		if err != nil || got != counts["Account"] {
			t.Fatalf("%s holds %d accounts (error %v), want %d", database, got, err, counts["Account"])
		}
	}
	var texts [2][]string
	for i, database := range []string{"first", "second"} {
		page, err := client.Bucket(database, "Account").StoreList(ctx, "a17", "Invoices", 10, nil)
		if err != nil || len(page.Elements) == 0 {
			t.Fatalf("%s: Invoices of a17: %d elements, error %v", database, len(page.Elements), err)
		}
		for _, element := range page.Elements {
			texts[i] = append(texts[i], element.Lookup("number").StringValue())
		}
	}
	if !slices.Equal(texts[0], texts[1]) {
		t.Errorf("the same layout seeded twice holds different invoices:\n%v\n%v", texts[0], texts[1])
	}
}

// TestSeedRefusesABucketThatExistsAndNamesIt checks both entry points: Check before anything is
// written, and Seed itself, which must not write into a database that holds the layout.
func TestSeedRefusesABucketThatExistsAndNamesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := startServer(t)
	seeder := seed.New([]*ringclient.Client{client}, seed.Options{Workers: 16})
	gen := ledgerLayout(t)
	if err := seeder.Check(ctx, gen, "demo"); err != nil {
		t.Fatalf("a database that does not exist must pass the check: %v", err)
	}
	if _, err := seeder.Seed(ctx, gen, "demo", seed.NewProgress(nil)); err != nil {
		t.Fatal(err)
	}
	var exists *seed.DatabaseExistsError
	if err := seeder.Check(ctx, gen, "demo"); !errors.As(err, &exists) || exists.Database != "demo" || !slices.Contains(exists.Buckets, "Account") {
		t.Fatalf("Check must name the buckets that exist: %v", err)
	}
	if _, err := seeder.Seed(ctx, gen, "demo", seed.NewProgress(nil)); !errors.As(err, &exists) {
		t.Fatalf("a second Seed must be refused with *DatabaseExistsError: %v", err)
	}
}

// TestSeedReusesADatabaseWhoseBucketsWereDropped covers the way out the refusal names: there is no
// drop database statement, so a database emptied bucket by bucket must be seedable again.
func TestSeedReusesADatabaseWhoseBucketsWereDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := startServer(t)
	seeder := seed.New([]*ringclient.Client{client}, seed.Options{Workers: 16})
	gen := ledgerLayout(t)
	if _, err := client.CreateDatabase(ctx, "emptied", 0); err != nil {
		t.Fatal(err)
	}
	if err := seeder.Check(ctx, gen, "emptied"); err != nil {
		t.Fatalf("an existing database with none of the layout's buckets must pass the check: %v", err)
	}
	if _, err := seeder.Seed(ctx, gen, "emptied", seed.NewProgress(nil)); err != nil {
		t.Fatalf("seeding into a database that exists and is empty: %v", err)
	}
}

// TestSeedLoadsEveryLayoutIntoOneServer seeds all five layouts, each into a database of its own, and
// checks every document count. It is the one test that touches every layout, so -short skips it.
func TestSeedLoadsEveryLayoutIntoOneServer(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds all five layouts; skipped by -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client := startServer(t)
	seeder := seed.New([]*ringclient.Client{client}, seed.Options{Workers: 16})
	for _, layout := range model.Layouts {
		gen := smallLayout(t, layout)
		database := "all_" + string(layout)
		if _, err := seeder.Seed(ctx, gen, database, seed.NewProgress(nil)); err != nil {
			t.Fatalf("seeding %s: %v", layout, err)
		}
		for bucket, want := range gen.Counts() {
			if strings.Contains(bucket, ".") {
				continue
			}
			if got, err := client.Bucket(database, bucket).Count(ctx, nil); err != nil || got != want {
				t.Errorf("%s.%s holds %d (error %v), want %d", database, bucket, got, err, want)
			}
		}
	}
}
