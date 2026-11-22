package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	serverconfig "github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

const testPassword = "scenarios-test-password"

// startServer runs one in-process hoarddb-server on a free local port, the way
// bench/social/ringload's tests do, and returns its address.
func startServer(t *testing.T) string {
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
	return addr
}

// TestSeedSocialIntoARealServer seeds the social layout at scale 1 through run, which verifies
// the counts, then checks the JSON line, that a second run is refused with the way out, and
// that a vector-free spot check of a document and a store reads back.
func TestSeedSocialIntoARealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	addr := startServer(t)
	cfg := config{Engine: "hoarddb", Layout: "social", Scale: 1, Seed: 1, Database: "scenarios", Addr: addr, User: "admin", Password: testPassword,
		Insecure: true, RF: 1, Workers: 8, Connections: 2}
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	line := strings.TrimSpace(stdout.String())
	gen, err := model.New(model.Social, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	counts := gen.Counts()
	writes := counts["User"] + counts["Post"] + counts["Group"] + counts["Follow"]
	for name, count := range counts {
		if strings.Contains(name, ".") {
			writes += count
		}
	}
	for _, want := range []string{`"engine":"hoarddb"`, `"layout":"social"`, `"database":"scenarios_social"`, `"scale":1`, `"seed":1`, `"User":40`, `"Post":1600`, `"Group":10`, `"seed_s":`} {
		if !strings.Contains(line, want) {
			t.Errorf("JSON line lacks %s: %s", want, line)
		}
	}
	if !strings.Contains(line, `"writes":`+strconv.FormatInt(writes, 10)) {
		t.Errorf("JSON line must report %d writes: %s", writes, line)
	}
	if strings.Count(stdout.String(), "\n") != 1 {
		t.Errorf("stdout must be exactly one line: %q", stdout.String())
	}

	again := run(ctx, cfg, io.Discard, io.Discard)
	if again == nil || !strings.Contains(again.Error(), "-database") || !strings.Contains(again.Error(), "already exists") {
		t.Fatalf("a second seed must be refused with the way out: %v", again)
	}
}

// TestSeedAllLayoutsAndReadAVectorBack seeds every layout into one server (each in a
// database of its own, since several of them declare a User bucket) and reads a memory's vector
// back as a 256-byte BSON binary of subtype 0.
func TestSeedAllLayoutsAndReadAVectorBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	addr := startServer(t)
	cfg := config{Engine: "hoarddb", Layout: layoutAll, Scale: 1, Seed: 3, Database: "scen", Addr: addr, User: "admin", Password: testPassword,
		Insecure: true, RF: 1, Workers: 8, Connections: 2}
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != len(model.Layouts) {
		t.Fatalf("want one JSON line per layout, got %d:\n%s", lines, stdout.String())
	}
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "admin", Password: testPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	memory, err := client.Bucket("scen_agent", "User").StoreGet(ctx, "u3", "Memories", "m007")
	if err != nil {
		t.Fatal(err)
	}
	subtype, packed, ok := memory.Lookup("vec").BinaryOK()
	if !ok || subtype != 0 || len(packed) != 4*model.VectorLen || memory.Lookup("text").StringValue() == "" {
		t.Fatalf("memory is not a text and a 64-value subtype-0 vector: %v", memory)
	}
	gen, err := model.New(model.Agent, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	for element := range gen.Elements("User.Memories") {
		if element.Owner == "u3" && element.Key == "m007" {
			for _, field := range element.Body {
				if field.Name == "vec" && !bytes.Equal(field.Value.([]byte), packed) {
					t.Error("the vector read back differs from the generated one")
				}
			}
		}
	}
}

// fieldNames lists the member names of a stored element in stored order.
func fieldNames(t *testing.T, raw bson.Raw) []string {
	t.Helper()
	members, err := raw.Elements()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(members))
	for i, member := range members {
		name, err := member.KeyErr()
		if err != nil {
			t.Fatal(err)
		}
		names[i] = name
	}
	return names
}

// TestHoardSeederEmbedsFeedAndMembers seeds social and chat twice into one real server, once as
// the common layout and once as the native one, and reads the first feed and the first room's
// members back: native elements carry the copies in the model's order and values, common ones
// do not, and the report line says which variant it loaded.
func TestHoardSeederEmbedsFeedAndMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	addr := startServer(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "admin", Password: testPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	for _, variant := range []string{variantCommon, variantNative} {
		native := variant == variantNative
		for _, layout := range []model.Layout{model.Social, model.Chat} {
			cfg := config{Engine: "hoarddb", Layout: string(layout), Scale: 1, Seed: 1, Database: variant, Addr: addr, User: "admin", Password: testPassword,
				Insecure: true, RF: 1, Workers: 8, Connections: 2, Variant: variant}
			var stdout, stderr bytes.Buffer
			if err := run(ctx, cfg, &stdout, &stderr); err != nil {
				t.Fatalf("%s %s: %v\n%s", variant, layout, err, stderr.String())
			}
			var report seedReport
			if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
				t.Fatalf("report line %q: %v", stdout.String(), err)
			}
			gen, err := model.New(layout, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if report.Variant != variant {
				t.Errorf("%s %s: report variant %q", variant, layout, report.Variant)
			}
			if want := gen.Native().Fingerprint(); native && report.NativeFingerprint != want || !native && report.NativeFingerprint != "" {
				t.Errorf("%s %s: native fingerprint %q, want %q for native and none for common", variant, layout, report.NativeFingerprint, want)
			}
			database := variant + "_" + string(layout)
			store, bucket, owner := "User.Feed", "User", ""
			if layout == model.Chat {
				store, bucket = "Room.Members", "Room"
			}
			var want []model.Element
			for element := range gen.Elements(store) {
				if owner == "" {
					owner = element.Owner
				}
				if element.Owner == owner {
					want = append(want, element)
				}
			}
			storeName := want[0].Store
			page, err := client.Bucket(database, bucket).StoreList(ctx, owner, storeName, 1000, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Elements) != len(want) {
				t.Fatalf("%s %s: %s of %s holds %d elements, want %d", variant, layout, storeName, owner, len(page.Elements), len(want))
			}
			// A hash store lists in key order, not in push order: match a member by its key.
			byKey := map[string]model.Element{}
			for _, element := range want {
				byKey[element.Key] = element
			}
			for i, raw := range page.Elements {
				element := want[i]
				if layout == model.Chat {
					element = byKey[page.Keys[i]]
				}
				var names []string
				for _, field := range element.Body {
					names = append(names, field.Name)
				}
				if native {
					extra, err := gen.Native().Embed(element)
					if err != nil {
						t.Fatal(err)
					}
					for _, field := range extra {
						names = append(names, field.Name)
						if got := raw.Lookup(field.Name).StringValue(); got != field.Value {
							t.Errorf("%s of %s element %d: %s = %q, want %q", storeName, owner, i, field.Name, got, field.Value)
						}
					}
				}
				if got := fieldNames(t, raw); strings.Join(got, ",") != strings.Join(names, ",") {
					t.Errorf("%s %s element %d has fields %v, want %v", variant, storeName, i, got, names)
				}
			}
		}
	}
}

// TestHoardSeederDeclaresTheLedgerAggregates seeds the ledger twice, as the common layout and as
// common+agg, and reads back what the server holds: the +agg Account declares the four aggregates
// of model.LedgerAggregates, all ready, and the maintained sum of the first account equals the
// expense total the model computed; the common one declares none.
func TestHoardSeederDeclaresTheLedgerAggregates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	addr := startServer(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "admin", Password: testPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	for _, variant := range []string{"common", "common+agg"} {
		cfg := config{Engine: "hoarddb", Layout: "ledger", Scale: 1, Seed: 1, Database: variant, Addr: addr, User: "admin", Password: testPassword,
			Insecure: true, RF: 1, Workers: 4, Connections: 2, Variant: variant}
		var stdout, stderr bytes.Buffer
		if err := run(ctx, cfg, &stdout, &stderr); err != nil {
			t.Fatalf("%s: %v\n%s", variant, err, stderr.String())
		}
		var report seedReport
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("report line %q: %v", stdout.String(), err)
		}
		if report.Variant != variant {
			t.Errorf("%s: report variant %q", variant, report.Variant)
		}
		info, err := client.DescribeBucket(ctx, variant+"_ledger", "Account")
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if variant == "common+agg" {
			want = 4
		}
		if len(info.Aggregates) != want {
			t.Fatalf("%s: Account declares %d aggregates, want %d: %+v", variant, len(info.Aggregates), want, info.Aggregates)
		}
		for _, aggregate := range info.Aggregates {
			if aggregate.State != "ready" {
				t.Errorf("%s: aggregate %+v is not ready", variant, aggregate)
			}
		}
		if want == 0 {
			continue
		}
		gen, err := model.New(model.Ledger, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for unit := range gen.Units() {
			var total int64
			for _, counter := range unit.Doc.Counters {
				if counter.Name == "expense_total" {
					total = counter.Value.(int64)
				}
			}
			got, err := client.Bucket(variant+"_ledger", "Account").Sum(ctx, unit.Doc.ID, "Expenses", "amount")
			if err != nil || got.Int != total {
				t.Fatalf("Sum of %s Expenses = %+v, %v, want %d", unit.Doc.ID, got, err, total)
			}
			break
		}
	}
}

// A deep ledger seeds the depth asked for, with the aggregates still ready and equal to the totals.
func TestHoardSeederSeedsADeepLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	addr := startServer(t)
	cfg := config{Engine: "hoarddb", Layout: "ledger", Scale: 1, Seed: 1, Database: "deep", Addr: addr, User: "admin", Password: testPassword,
		Insecure: true, RF: 1, Workers: 4, Connections: 2, Variant: "common+agg+deep250"}
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	var report seedReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("report line %q: %v", stdout.String(), err)
	}
	if report.Variant != "common+agg+deep250" || report.Counts["Account.Expenses"] != 30*250 {
		t.Errorf("report variant %q, expenses %d, want common+agg+deep250 and %d", report.Variant, report.Counts["Account.Expenses"], 30*250)
	}
}
