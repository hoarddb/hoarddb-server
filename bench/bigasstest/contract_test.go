package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// This file is the contract suite every real adapter's live test calls:
//
//	func TestContractRing(t *testing.T) { runContract(t, contractTarget{...}) }
//
// It seeds the four layouts at scale 2, then runs every read and write operation of every
// layout through the same runner code the benchmark uses, verifies every read, and audits the
// counts and the ledger sums. An adapter passes when it does.

const (
	contractScale = 2
	contractSeed  = 1
	// contractWritesPerKind is how many writes of each kind the contract issues.
	contractWritesPerKind = 8
)

// contractTarget is an engine under the contract.
type contractTarget struct {
	// Engine is the opened adapter.
	Engine Engine
	// Name is the engine name the scenarios binary seeds ("ring", "mongo", ...).
	Name string
	// Database is the base name; a layout is <Database>_<layout>.
	Database string
	// SeedArgs are the connection flags scenarios needs for this engine.
	SeedArgs []string
	// Variant is the layout variant to seed ("" is common).
	Variant string
	// Totals adds the record-only ledger.totals_by_category read to the contract. Every competitor
	// answers it; hoardDB answers it only with the aggregates declared (+agg, aggregates=on).
	Totals bool
	// Seeder replaces the default, which builds bench/scenarios and execs it with SeedArgs.
	Seeder func(ctx context.Context, layout model.Layout, scale int, seed uint64) error
}

var (
	scenariosOnce sync.Once
	scenariosBin  string
	scenariosErr  error
)

// buildScenarios compiles bench/scenarios once per test process.
func buildScenarios() (string, error) {
	scenariosOnce.Do(func() {
		_, file, _, _ := runtime.Caller(0)
		moduleRoot := filepath.Join(filepath.Dir(file), "..", "..")
		dir, err := os.MkdirTemp("", "bigasstest-scenarios")
		if err != nil {
			scenariosErr = fmt.Errorf("making a directory for the scenarios binary: %w", err)
			return
		}
		scenariosBin = filepath.Join(dir, "scenarios")
		build := exec.Command("go", "build", "-o", scenariosBin, "./bench/scenarios")
		build.Dir = moduleRoot
		if out, err := build.CombinedOutput(); err != nil {
			scenariosErr = fmt.Errorf("building bench/scenarios: %w: %s", err, out)
		}
	})
	return scenariosBin, scenariosErr
}

func (c contractTarget) seed(ctx context.Context, layout model.Layout) error {
	if c.Seeder != nil {
		return c.Seeder(ctx, layout, contractScale, contractSeed)
	}
	bin, err := buildScenarios()
	if err != nil {
		return err
	}
	_, err = runSeed(ctx, osExec, seedRequest{Bin: bin, Engine: c.Name, Scale: contractScale, Seed: contractSeed, Database: c.Database, Variant: c.Variant, Passthrough: c.SeedArgs}, layout)
	return err
}

// runContract fails the test when the engine breaks the contract.
func runContract(t *testing.T, target contractTarget) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := contractCheck(ctx, target); err != nil {
		t.Fatal(err)
	}
}

// contractReads lists the read operations of each layout, including the record-only ones.
var contractReads = map[model.Layout][]ReadKind{
	model.Social: {ReadSocialFeed, ReadSocialInbox, ReadSocialFollowers, ReadSocialFollowScan},
	model.Chat:   {ReadChatRoomPage, ReadChatUserRooms},
	model.Ledger: {ReadLedgerPage, ReadLedgerInvoices, ReadLedgerAll},
	model.Agent:  {ReadAgentMemories, ReadAgentSession},
}

// contractWrites lists the write operations of each layout.
var contractWrites = map[model.Layout][]WriteKind{
	model.Social: {WriteSocialPost, WriteSocialComment, WriteSocialLike, WriteSocialFollow, WriteSocialGroupPost, WriteSocialSettings},
	model.Chat:   {WriteChatMessage, WriteChatJoin},
	model.Ledger: {WriteLedgerExpense, WriteLedgerInvoice},
	model.Agent:  {WriteAgentMessage, WriteAgentTask, WriteAgentScratch, WriteAgentMemory},
}

// idempotentWrites are the write kinds that put by key, so a repeat of the same write id
// leaves the store and the audit unchanged.
var idempotentWrites = map[WriteKind]bool{
	WriteSocialLike: true, WriteSocialFollow: true, WriteSocialSettings: true,
	WriteChatJoin: true, WriteAgentScratch: true,
}

// contractOffsets lists the offsets the contract reads one entity at: 0, then, when the store
// has room for a page after the first element, offset 1 and the deepest offset that keeps the
// page inside the seeded elements.
func (w *world) contractOffsets(layout model.Layout, kind ReadKind, entity string) []int {
	offsets := []int{0}
	if !offsetEligible(kind) {
		return offsets
	}
	store := readStores[kind]
	room := len(w.sets[layout].elements(StoreRef{store[0], entity, store[1]})) - w.readLimit(kind)
	if w.readLimit(kind) == 0 || room < 1 {
		return offsets
	}
	offsets = append(offsets, 1)
	if room > 1 {
		offsets = append(offsets, room)
	}
	return offsets
}

// countersDeclared names the engines whose schema keeps the denormalised counters. hoardDB
// derives them and has nothing to read back.
var countersDeclared = map[string]bool{"mongo": true, "mysql": true, "postgres": true}

// contractCheck is runContract without the testing.T, so a test can prove it fails.
func contractCheck(ctx context.Context, target contractTarget) error {
	settings, err := target.Engine.Settings(ctx)
	if err != nil {
		return fmt.Errorf("contract: reading settings: %w", err)
	}
	for _, key := range []string{"engine_version", "tls"} {
		if settings[key] == "" {
			return fmt.Errorf("contract: Settings has no %q key", key)
		}
	}
	for _, layout := range benchLayouts {
		if err := target.seed(ctx, layout); err != nil {
			return fmt.Errorf("contract: %w", err)
		}
	}
	w, err := newWorld(contractSeed, contractScale)
	if err != nil {
		return fmt.Errorf("contract: %w", err)
	}
	// Page depth above 0 lets the contract read each ordered store at offset 1 and at the deepest
	// offset an owner's seeded elements allow.
	if err := w.configureAccess(accessConfig{Mode: accessUniform, ZipfS: 1, PageDepth: 0.3}); err != nil {
		return fmt.Errorf("contract: %w", err)
	}
	session, err := target.Engine.Session(ctx, 0)
	if err != nil {
		return fmt.Errorf("contract: opening a session: %w", err)
	}
	defer func() { _ = session.Close() }() // a session close error cannot change the contract verdict
	environment := &env{w: w, reg: newRegistry(), book: newBook(w)}
	ops := map[model.Layout]LayoutOps{}
	for _, layout := range benchLayouts {
		layoutOps, err := session.Layout(layout, layoutDatabase(target.Database, layout))
		if err != nil {
			return fmt.Errorf("contract: opening layout %s: %w", layout, err)
		}
		if _, ok := layoutOps.(StoreReader); !ok {
			return fmt.Errorf("contract: the %s adapter does not implement StoreReader, which the audit reads replaced keys back with", target.Name)
		}
		if _, ok := layoutOps.(CounterReader); !ok && countersDeclared[target.Name] {
			return fmt.Errorf("contract: the %s schema declares counters, so its adapter must implement CounterReader", target.Name)
		}
		ops[layout] = layoutOps
	}
	for _, layout := range benchLayouts {
		if err := checkFresh(ctx, w, layout, ops[layout]); err != nil {
			return fmt.Errorf("contract: right after the seed: %w", err)
		}
	}
	readAll := func(phase string) error {
		for _, layout := range benchLayouts {
			kinds := contractReads[layout]
			if target.Totals && layout == model.Ledger {
				kinds = append(slices.Clone(kinds), ReadLedgerTotals)
			}
			for _, kind := range kinds {
				// The totals are verified against the seeded data alone (no write runs beside the
				// record-only read); after the writes they are checked against the store below.
				if kind == ReadLedgerTotals && phase != "seeded data" {
					continue
				}
				for _, entity := range w.readOwners(kind) {
					// Every read from the start, then, for a paged ordered store, at the first
					// and at the deepest offset that stays inside the seeded elements.
					for _, offset := range w.contractOffsets(layout, kind, entity) {
						op := w.readOp(layout, kind, entity)
						if err := w.setOffset(op, offset); err != nil {
							return fmt.Errorf("contract (%s): %w", phase, err)
						}
						res := environment.runRequest(ctx, ops[layout], request{Layout: layout, Kind: string(kind), Read: op})
						if res.OpErr != nil {
							return fmt.Errorf("contract (%s): %s of %s at offset %d: %w", phase, kind, entity, offset, res.OpErr)
						}
						if res.VerifyErr != nil {
							return fmt.Errorf("contract (%s): %s of %s at offset %d: %w", phase, kind, entity, offset, res.VerifyErr)
						}
					}
				}
			}
		}
		return nil
	}
	if err := readAll("seeded data"); err != nil {
		return err
	}
	for _, layout := range benchLayouts {
		for _, kind := range contractWrites[layout] {
			mix, err := newMix(benchLayouts, string(kind))
			if err != nil {
				return err
			}
			planner := newPlanner(w, mix, 1, 1, 42)
			var first *request
			for range contractWritesPerKind {
				req := planner.next()
				if first == nil {
					first = &req
				}
				res := environment.runRequest(ctx, ops[layout], req)
				if res.OpErr != nil {
					return fmt.Errorf("contract: write %s: %w", kind, res.OpErr)
				}
				if res.VerifyErr != nil {
					return fmt.Errorf("contract: write %s: %w", kind, res.VerifyErr)
				}
			}
			// Repeat the first write, but only for the kinds that put by key: likes,
			// follows, joins, settings and scratch are idempotent, and the audit must not see
			// a second copy. A push is not replayable: its seq (SeqBase + Ordinal) is unique
			// on every engine, so repeating it unchanged is a duplicate-key error by design.
			if !idempotentWrites[kind] {
				continue
			}
			if res := environment.runRequest(ctx, ops[layout], *first); res.OpErr != nil {
				return fmt.Errorf("contract: repeating write %s: %w", kind, res.OpErr)
			}
		}
	}
	if err := readAll("after writes"); err != nil {
		return err
	}
	if target.Totals {
		// After the writes the totals must equal the totals of what the store holds now, which the
		// audit below holds to seeded plus acknowledged writes.
		reader, ok := ops[model.Ledger].(StoreReader)
		if !ok {
			return fmt.Errorf("contract: the %s adapter does not implement StoreReader, so the totals cannot be checked after the writes", target.Name)
		}
		for _, account := range w.readOwners(ReadLedgerTotals) {
			reply, err := ops[model.Ledger].Read(ctx, ReadOp{Layout: model.Ledger, Kind: ReadLedgerTotals, Entity: account})
			if err != nil {
				return fmt.Errorf("contract (after writes): ledger.totals_by_category of %s: %w", account, err)
			}
			held, err := reader.ReadStore(ctx, StoreRef{"Account", account, "Expenses"})
			if err != nil {
				return fmt.Errorf("contract (after writes): reading the expenses of %s: %w", account, err)
			}
			if want := totalsOf(held); !slices.Equal(want, reply.Totals) {
				return fmt.Errorf("contract (after writes): ledger.totals_by_category of %s is %v, the expenses it holds add up to %v", account, reply.Totals, want)
			}
		}
	}
	for _, layout := range benchLayouts {
		if err := environment.book.audit(ctx, layout, ops[layout]); err != nil {
			return fmt.Errorf("contract: %w", err)
		}
	}
	return nil
}

func fakeTarget(t *testing.T, mutate func(*fakeEngine)) (contractTarget, *fakeEngine) {
	t.Helper()
	fake := newFakeEngine("bat")
	if mutate != nil {
		mutate(fake)
	}
	seeder := func(_ context.Context, layout model.Layout, scale int, seed uint64) error {
		return fake.load(layout, seed, scale)
	}
	return contractTarget{Engine: fake, Name: "fake", Database: "bat", Seeder: seeder}, fake
}

func TestContractPassesOnTheFake(t *testing.T) {
	target, _ := fakeTarget(t, nil)
	runContract(t, target)
}

func TestContractCatchesSilentlyEmpty(t *testing.T) {
	target, _ := fakeTarget(t, func(f *fakeEngine) { f.emptyPages = true })
	err := contractCheck(context.Background(), target)
	if err == nil || !errors.Is(err, errShortPage) {
		t.Fatalf("an engine that returns empty pages must fail the contract as a short page, got %v", err)
	}
}

func TestContractCatchesLostWrites(t *testing.T) {
	target, _ := fakeTarget(t, func(f *fakeEngine) { f.dropWrites = true })
	err := contractCheck(context.Background(), target)
	if err == nil || (!strings.Contains(err.Error(), "audit") && !strings.Contains(err.Error(), "short page")) {
		t.Fatalf("acknowledged writes that were not applied must fail the audit or a read, got %v", err)
	}
}

// An engine that serves every page from the first element, whatever the offset, must fail: the
// verifier compares the page at an offset with the seeded elements from that offset.
func TestContractCatchesAnIgnoredOffset(t *testing.T) {
	target, _ := fakeTarget(t, func(f *fakeEngine) { f.ignoreOffset = true })
	if err := contractCheck(context.Background(), target); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("an engine that ignores the offset must fail the contract and name it, got %v", err)
	}
}

func TestContractCatchesWrongOrder(t *testing.T) {
	target, _ := fakeTarget(t, func(f *fakeEngine) { f.reorder = true })
	if err := contractCheck(context.Background(), target); err == nil {
		t.Fatal("pages in the wrong order must fail the contract")
	}
}

func TestContractRequiresSettingsKeys(t *testing.T) {
	target, _ := fakeTarget(t, nil)
	target.Engine = noSettings{target.Engine}
	if err := contractCheck(context.Background(), target); err == nil || !strings.Contains(err.Error(), "engine_version") {
		t.Fatalf("missing engine_version must be named, got %v", err)
	}
}

type noSettings struct{ Engine }

func (noSettings) Settings(context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}
