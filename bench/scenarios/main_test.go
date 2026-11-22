package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestFlagsRefuseWhatCannotWorkAndNameTheFix(t *testing.T) {
	good := config{Engine: "hoarddb", Layout: "all", Scale: 1, Database: "scenarios", Addr: "host:1", Password: "pw", Insecure: true, RF: 1, Workers: 1, Connections: 1,
		MySQLDSN: "bench@tcp(localhost:3306)/", MongoURI: "mongodb://localhost:27017"}
	if err := good.check(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*config){
		"addr":     func(c *config) { c.Addr = "" },
		"password": func(c *config) { c.Password = "" },
		"trust":    func(c *config) { c.Insecure = false },
		"rf":       func(c *config) { c.RF = 0 },
		"workers":  func(c *config) { c.Workers = 0 },
		"scale":    func(c *config) { c.Scale = 0 },
		"layout":   func(c *config) { c.Layout = "blog" },
		"engine":   func(c *config) { c.Engine = "" },
		"unknown":  func(c *config) { c.Engine = "sqlite" },
		"database": func(c *config) { c.Database = "" },
		"dsn":      func(c *config) { c.Engine, c.MySQLDSN = "mysql", "" },
		"uri":      func(c *config) { c.Engine, c.MongoURI = "mongo", "" },
	} {
		bad := good
		mutate(&bad)
		if err := bad.check(); err == nil {
			t.Errorf("%s: a config that cannot work was accepted", name)
		}
	}
	missing := good
	missing.Insecure = false
	if err := missing.check(); err == nil || !strings.Contains(err.Error(), "-fingerprint") || !strings.Contains(err.Error(), "-insecure") {
		t.Errorf("the trust refusal must name both flags: %v", err)
	}
	// -ddl needs no server and so no address, password or engine.
	ddl := config{Layout: "social", Scale: 1, Database: "scenarios", DDL: true}
	if err := ddl.check(); err != nil {
		t.Errorf("-ddl must not need a server: %v", err)
	}
}

func TestParseFlagsReadsTheSameEnvironmentAsRingseed(t *testing.T) {
	t.Setenv("HOARDDB_ADDR", "host:9")
	t.Setenv("HOARDDB_ROOT_USER", "root")
	t.Setenv("HOARDDB_ROOT_PASSWORD", "secret-secret")
	cfg, err := parseFlags([]string{"-engine", "hoarddb", "-insecure", "-layout", "chat"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "host:9" || cfg.User != "root" || cfg.Password != "secret-secret" || cfg.Workers != 16 || cfg.Connections != 4 || cfg.RF != 1 || cfg.Database != "scenarios" {
		t.Errorf("defaults or environment not applied: %+v", cfg)
	}
}

// countLines counts the lines of text that start with prefix, or contain it when contains is set.
func countLines(text, prefix string, contains bool) int {
	count := 0
	for _, line := range strings.Split(text, "\n") {
		if (contains && strings.Contains(line, prefix)) || (!contains && strings.HasPrefix(line, prefix)) {
			count++
		}
	}
	return count
}

// TestDDLHasTheExpectedStatementShapes: a reader reviews the three schemas side by side, so
// the number of buckets, tables and indexes per layout is pinned.
func TestDDLHasTheExpectedStatementShapes(t *testing.T) {
	want := map[model.Layout]struct{ buckets, tables, indexes int }{
		model.Social: {4, 15, 15}, model.Chat: {3, 4, 3}, model.Ledger: {1, 3, 2}, model.Agent: {3, 7, 5}, model.Video: {3, 10, 10},
	}
	for layout, expected := range want {
		var out bytes.Buffer
		cfg := config{Layout: string(layout), Scale: 1, Seed: 1, Database: "scenarios", RF: 1, DDL: true}
		if err := run(context.Background(), cfg, &out, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		text := out.String()
		if got := countLines(text, "create bucket ", false); got != expected.buckets {
			t.Errorf("%s: %d create bucket lines, want %d", layout, got, expected.buckets)
		}
		if got := countLines(text, "CREATE TABLE ", false); got != 2*expected.tables {
			t.Errorf("%s: %d CREATE TABLE lines, want %d (MySQL and PostgreSQL)", layout, got, 2*expected.tables)
		}
		if got := countLines(text, ".createIndex(", true); got != expected.indexes {
			t.Errorf("%s: %d createIndex lines, want %d", layout, got, expected.indexes)
		}
		if got := countLines(text, "db.createCollection(", false); got != expected.tables {
			t.Errorf("%s: %d createCollection lines, want %d", layout, got, expected.tables)
		}
		if !strings.Contains(text, "create database scenarios_"+string(layout)+" { replication_factor: 1 };") {
			t.Errorf("%s: no create database line", layout)
		}
	}
}

// TestEveryDeclaredStoreAndBucketHasAHomeInSQL: each bucket and each store the layout declares
// is either a table of its own or held by a paired table, so a count never goes unchecked.
func TestEveryDeclaredStoreAndBucketHasAHomeInSQL(t *testing.T) {
	for _, layout := range model.Layouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		sources := map[string]bool{}
		for _, spec := range tables(layout) {
			sources[spec.Source] = true
		}
		for name := range gen.Counts() {
			if !sources[name] && pairedStores(layout)[name] == "" {
				t.Errorf("%s: %s has no table and is not a paired store", layout, name)
			}
		}
		for store, holder := range pairedStores(layout) {
			if !sources[holder] || gen.Counts()[store] != gen.Counts()[holder] {
				t.Errorf("%s: paired store %s and its holder %s disagree", layout, store, holder)
			}
		}
		if _, err := expectedRows(layout, gen.Counts()); err != nil {
			t.Error(err)
		}
	}
}

func TestEveryColumnIsFedByTheModel(t *testing.T) {
	for _, layout := range model.Layouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range tables(layout) {
			if gen.Counts()[spec.Source] == 0 {
				continue // drafts: declared, nothing seeded
			}
			known := map[string]bool{"_id": true, "_owner": true, "_key": true, "_key:playlist": true, "_key:position": true, "_seq": true, "": true}
			for unit := range gen.Units() {
				for _, field := range append(unit.Doc.Fields, unit.Doc.Counters...) {
					if unit.Doc.Bucket == spec.Source {
						known[field.Name] = true
					}
				}
				for _, element := range unit.Elements {
					if element.StoreName() == spec.Source {
						for _, field := range element.Body {
							known[field.Name] = true
						}
					}
				}
			}
			for _, col := range spec.Cols {
				if !known[col.From] {
					t.Errorf("%s.%s: column %s takes %q, which the model does not produce for %s", layout, spec.Name, col.Name, col.From, spec.Source)
				}
			}
		}
	}
}

// TestVideoDDLDeclaresTheApprovedBuckets: the three create bucket lines of the video layout are
// pinned word for word, because a reader checks them against the layouts document.
func TestVideoDDLDeclaresTheApprovedBuckets(t *testing.T) {
	var out bytes.Buffer
	cfg := config{Layout: layoutAll, Scale: 1, Seed: 1, Database: "scenarios", RF: 1, DDL: true}
	if err := run(context.Background(), cfg, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"create bucket Video in scenarios_video { Owner string, Title string, Path string, Duration int, Views int, Comments fifoStore, Likes hashStore };",
		"create bucket Channel in scenarios_video { Name string, Uploads lifoStore, Subscribers hashStore };",
		"create bucket Viewer in scenarios_video { Name string, Subscriptions hashStore, Progress hashStore, History fifoStore, Playlists btreeStore };",
	} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("-ddl -layout all lacks %q", line)
		}
	}
}

// Every engine but hoardDB refuses the native variant and names the way out, because only the
// hoardDB seeder knows how to embed the copies.
func TestOtherSeedersRefuseVariant(t *testing.T) {
	good := config{Layout: "social", Scale: 1, Database: "scenarios", Variant: variantNative, Workers: 1, Connections: 1,
		MySQLDSN: "bench@tcp(localhost:3306)/", PostgresDSN: "postgres://x", MongoURI: "mongodb://localhost:27017",
		RedisURL: "redis://localhost:6379/0", KvrocksURL: "redis://localhost:6666/0"}
	for _, engine := range []string{"mysql", "postgres", "mongo", "redis", "kvrocks"} {
		cfg := good
		cfg.Engine = engine
		want := "engine " + engine + " has no native layout variant yet; drop -layout-variant, or seed hoarddb"
		if err := cfg.check(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: check() = %v, want a refusal containing %q", engine, err, want)
		}
	}
	hoard := good
	hoard.Engine, hoard.Addr, hoard.Password, hoard.Insecure, hoard.RF = "hoarddb", "host:1", "pw", true, 1
	if err := hoard.check(); err != nil {
		t.Errorf("hoarddb must accept the native variant: %v", err)
	}
	for _, engine := range []string{"mysql", "mongo"} {
		cfg := good
		cfg.Engine, cfg.Variant = engine, variantCommon
		if err := cfg.check(); err != nil {
			t.Errorf("%s with the common variant: %v", engine, err)
		}
	}
	bad := hoard
	bad.Variant = "fancy"
	if err := bad.check(); err == nil || !strings.Contains(err.Error(), "-layout-variant") || !strings.Contains(err.Error(), "common") || !strings.Contains(err.Error(), "native") {
		t.Errorf("an unknown variant must be refused naming both words: %v", err)
	}
}

func TestLayoutVariantDefaultsToCommon(t *testing.T) {
	t.Setenv("HOARDDB_ROOT_PASSWORD", "secret-secret")
	cfg, err := parseFlags([]string{"-engine", "hoarddb", "-addr", "h:1", "-insecure"}, &bytes.Buffer{})
	if err != nil || cfg.Variant != variantCommon {
		t.Fatalf("variant = %q, err = %v, want common", cfg.Variant, err)
	}
	cfg, err = parseFlags([]string{"-engine", "hoarddb", "-addr", "h:1", "-insecure", "-layout-variant", "native"}, &bytes.Buffer{})
	if err != nil || cfg.Variant != variantNative {
		t.Fatalf("variant = %q, err = %v, want native", cfg.Variant, err)
	}
}

func TestVariantAggregatesIsHoardDBOnlyAndRefusesUnknownWords(t *testing.T) {
	good := config{Layout: "ledger", Scale: 1, Database: "scenarios", Variant: "common+agg", Workers: 1, Connections: 1,
		MySQLDSN: "bench@tcp(localhost:3306)/", PostgresDSN: "postgres://x", MongoURI: "mongodb://localhost:27017"}
	for _, engine := range []string{"mysql", "postgres", "mongo"} {
		cfg := good
		cfg.Engine = engine
		if err := cfg.check(); err == nil || !strings.Contains(err.Error(), "+agg") || !strings.Contains(err.Error(), "seed hoarddb") {
			t.Errorf("%s: check() = %v, want a refusal that names +agg and hoarddb", engine, err)
		}
	}
	hoard := good
	hoard.Engine, hoard.Addr, hoard.Password, hoard.Insecure, hoard.RF = "hoarddb", "host:1", "pw", true, 1
	for _, word := range []string{"common+agg", "native+agg"} {
		hoard.Variant = word
		if err := hoard.check(); err != nil {
			t.Errorf("hoarddb must accept %s: %v", word, err)
		}
	}
	hoard.Variant = "common+aggs"
	if err := hoard.check(); err == nil || !strings.Contains(err.Error(), "+agg") {
		t.Errorf("an unknown modifier must be refused naming +agg: %v", err)
	}
}

// +deep<N> changes the data every engine is seeded with, so every engine accepts it; only +agg and
// native are hoarddb-only.
func TestVariantDeepIsAcceptedByEveryEngine(t *testing.T) {
	good := config{Layout: "ledger", Scale: 1, Database: "scenarios", Variant: "common+deep500", Workers: 1, Connections: 1,
		MySQLDSN: "bench@tcp(localhost:3306)/", PostgresDSN: "postgres://x", MongoURI: "mongodb://localhost:27017"}
	for _, engine := range []string{"mysql", "postgres", "mongo"} {
		cfg := good
		cfg.Engine = engine
		if err := cfg.check(); err != nil {
			t.Errorf("%s with +deep500: %v", engine, err)
		}
		cfg.Variant = "common+agg+deep500"
		if err := cfg.check(); err == nil || !strings.Contains(err.Error(), "+agg") {
			t.Errorf("%s with +agg+deep500 must still refuse +agg: %v", engine, err)
		}
	}
}
