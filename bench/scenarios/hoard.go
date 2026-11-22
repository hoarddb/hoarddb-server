package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// hoardSeeder loads a layout through the entry-ring client: a document is an Insert and a
// store element is a Push (fifoStore, lifoStore) or a Put (hashStore, btreeStore) on that
// document.
type hoardSeeder struct {
	clients []*ringclient.Client
	workers int
	rf      int
	// native embeds the copies of the native layout variant into the elements it writes.
	native bool
	// aggregates declares the ledger's maintained aggregates (model.LedgerAggregates) when it
	// creates the buckets (-layout-variant common+agg).
	aggregates bool
	next       atomic.Uint64
}

func newHoardSeeder(ctx context.Context, cfg config) (*hoardSeeder, error) {
	options := ringclient.Options{Addr: cfg.Addr, User: cfg.User, Password: cfg.Password, Fingerprint: cfg.Fingerprint, Insecure: cfg.Insecure}
	variant, err := model.ParseVariant(cfg.Variant)
	if err != nil {
		return nil, fmt.Errorf("-layout-variant: %w", err)
	}
	seeder := &hoardSeeder{workers: cfg.Workers, rf: cfg.RF, native: variant.Native, aggregates: variant.Aggregates}
	for range cfg.Connections {
		client, err := ringclient.Dial(ctx, options)
		if err != nil {
			seeder.close()
			return nil, fmt.Errorf("connecting to hoarddb-server at %s: %w", cfg.Addr, err)
		}
		seeder.clients = append(seeder.clients, client)
	}
	return seeder, nil
}

func (s *hoardSeeder) close() {
	for _, client := range s.clients {
		_ = client.Close()
	}
}

// pick returns a client round-robin, so the workers spread over the connections.
func (s *hoardSeeder) pick() *ringclient.Client {
	return s.clients[int(s.next.Add(1))%len(s.clients)]
}

// declared converts a layout's buckets to the field declarations the client sends.
// With aggregates, a store field named in model.LedgerAggregates carries its declarations.
func declared(bucket model.BucketDecl, aggregates bool) []ringclient.FieldDecl {
	fields := make([]ringclient.FieldDecl, len(bucket.Fields))
	for i, field := range bucket.Fields {
		fields[i] = ringclient.FieldDecl{Name: field.Name, Type: field.Type}
		if !aggregates {
			continue
		}
		for _, decl := range model.LedgerAggregates()[bucket.Name+"."+field.Name] {
			fields[i].Aggregates = append(fields[i].Aggregates, ringclient.AggregateDecl{Kind: decl.Kind, Field: decl.Field, By: decl.By})
		}
	}
	return fields
}

// createSchema creates the database and the buckets as the layout declares them. A database
// that exists is refused with the way out, because seeding into it would collide with what is there.
func (s *hoardSeeder) createSchema(ctx context.Context, gen *model.Generator, database string) error {
	client := s.clients[0]
	if _, err := client.CreateDatabase(ctx, database, int32(s.rf)); err != nil {
		var refusal *ringclient.Error
		if errors.As(err, &refusal) && refusal.Code == cluster.CodeConflict {
			return fmt.Errorf("database %q already exists on this server, so it may already hold this layout and seeding again would collide with it. "+
				"Seed a fresh database with -database <name>, or stop the server and start it on an empty data directory: %w", database, err)
		}
		return fmt.Errorf("creating database %q: %w", database, err)
	}
	for _, bucket := range gen.Schema() {
		if _, err := client.CreateBucket(ctx, database, bucket.Name, declared(bucket, s.aggregates)); err != nil {
			return fmt.Errorf("creating bucket %q in database %q: %w", bucket.Name, database, err)
		}
	}
	return nil
}

// applied turns a write that was not acknowledged as applied into an error.
func applied(doing string, written ringclient.WriteResult, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", doing, err)
	}
	if !written.Applied() {
		return fmt.Errorf("%s: the server's write outcome was %q, not applied", doing, written.Outcome)
	}
	return nil
}

// scalar turns a model value into what the client marshals. A vector is a BSON binary of
// subtype 0 (generic), which is the layout's wire form for an embedding.
func scalar(field any) any {
	if packed, ok := field.([]byte); ok {
		return primitive.Binary{Subtype: 0, Data: packed}
	}
	return field
}

func document(fields []model.Field) ringclient.Doc {
	doc := make(ringclient.Doc, len(fields))
	for _, field := range fields {
		doc[field.Name] = scalar(field.Value)
	}
	return doc
}

func elementBody(fields []model.Field) bson.D {
	body := make(bson.D, len(fields))
	for i, field := range fields {
		body[i] = bson.E{Key: field.Name, Value: scalar(field.Value)}
	}
	return body
}

// writeUnit writes one document and then its store elements in order. One worker does a unit
// start to finish, so the pushes onto a store keep their order. A non-nil native appends the
// embedded copies of the native layout variant to the elements that carry them.
func (s *hoardSeeder) writeUnit(ctx context.Context, database string, unit model.Unit, native *model.Native, progress *progress) error {
	bucket := s.pick().Bucket(database, unit.Doc.Bucket)
	outcome, err := bucket.Insert(ctx, unit.Doc.ID, document(unit.Doc.Fields))
	progress.add(1)
	if err := applied(fmt.Sprintf("storing %s %s", unit.Doc.Bucket, unit.Doc.ID), outcome, err); err != nil {
		return err
	}
	for _, element := range unit.Elements {
		fields := element.Body
		if native != nil {
			extra, err := native.Embed(element)
			if err != nil {
				return fmt.Errorf("embedding the copy into %s of %s %s: %w", element.Store, unit.Doc.Bucket, element.Owner, err)
			}
			fields = append(append([]model.Field(nil), fields...), extra...)
		}
		body := elementBody(fields)
		if element.Key == "" {
			outcome, err = bucket.Push(ctx, element.Owner, element.Store, body)
		} else {
			outcome, err = bucket.Put(ctx, element.Owner, element.Store, element.Key, body)
		}
		progress.add(1)
		if err := applied(fmt.Sprintf("writing %s of %s %s", element.Store, unit.Doc.Bucket, element.Owner), outcome, err); err != nil {
			return err
		}
	}
	return nil
}

// seed creates the database, writes every unit and verifies the counts.
func (s *hoardSeeder) seed(ctx context.Context, gen *model.Generator, database string, progress *progress) (int64, error) {
	if err := s.createSchema(ctx, gen, database); err != nil {
		return 0, err
	}
	progress.phase("documents and their stores")
	var native *model.Native
	if s.native {
		native = gen.Native()
	}
	err := pool(ctx, s.workers, gen.Units(), func(ctx context.Context, unit model.Unit) error {
		return s.writeUnit(ctx, database, unit, native, progress)
	}, progress)
	if err != nil {
		return 0, fmt.Errorf("writing %s: %w", gen.Layout(), err)
	}
	if err := s.verify(ctx, gen, database, progress); err != nil {
		return 0, err
	}
	if err := s.verifyVariant(ctx, gen, database); err != nil {
		return 0, err
	}
	if err := s.verifyAggregates(ctx, gen, database, progress); err != nil {
		return 0, err
	}
	return progress.total.Load(), nil
}

// verify reads every count back: documents per bucket with Count, and the elements of every
// store of every document with StoreCount, so a seed that lost a write does not report success.
// A heapStore is skipped: the client has no push that takes a priority, so none is seeded.
func (s *hoardSeeder) verify(ctx context.Context, gen *model.Generator, database string, progress *progress) error {
	progress.phase("count verification")
	client := s.clients[0]
	counts := gen.Counts()
	stores := map[string][]string{}
	for _, bucket := range gen.Schema() {
		got, err := client.Bucket(database, bucket.Name).Count(ctx, nil)
		if err != nil {
			return fmt.Errorf("counting bucket %s: %w", bucket.Name, err)
		}
		if got != counts[bucket.Name] {
			return fmt.Errorf("bucket %s holds %d documents after the seed, want %d", bucket.Name, got, counts[bucket.Name])
		}
		for _, field := range bucket.Fields {
			if field.IsStore() && field.Type != "heapStore" {
				stores[bucket.Name] = append(stores[bucket.Name], field.Name)
			}
		}
	}
	held := map[string]int64{}
	for unit := range gen.Units() {
		for _, element := range unit.Elements {
			held[element.Bucket+"/"+element.Owner+"/"+element.Store]++
		}
	}
	return pool(ctx, s.workers, gen.Units(), func(ctx context.Context, unit model.Unit) error {
		bucket := s.pick().Bucket(database, unit.Doc.Bucket)
		for _, store := range stores[unit.Doc.Bucket] {
			want := held[unit.Doc.Bucket+"/"+unit.Doc.ID+"/"+store]
			got, err := bucket.StoreCount(ctx, unit.Doc.ID, store)
			if err != nil {
				return fmt.Errorf("counting %s of %s %s: %w", store, unit.Doc.Bucket, unit.Doc.ID, err)
			}
			if got != want {
				return fmt.Errorf("%s of %s %s holds %d elements after the seed, want %d", store, unit.Doc.Bucket, unit.Doc.ID, got, want)
			}
		}
		return nil
	}, progress)
}

// verifyVariant reads the first element of the first store that can carry a copy and checks that
// it carries one exactly when the variant is native, so a seed made with the wrong flag is not
// reported as a success. A layout with no such store has nothing to check.
func (s *hoardSeeder) verifyVariant(ctx context.Context, gen *model.Generator, database string) error {
	for unit := range gen.Units() {
		for _, element := range unit.Elements {
			if !model.IsNativeStore(element.Bucket, element.Store) {
				continue
			}
			page, err := s.clients[0].Bucket(database, element.Bucket).StoreList(ctx, element.Owner, element.Store, 1, nil)
			if err != nil {
				return fmt.Errorf("reading the first %s of %s %s to check the layout variant: %w", element.Store, element.Bucket, element.Owner, err)
			}
			if len(page.Elements) != 1 {
				return fmt.Errorf("%s of %s %s holds %d elements after the seed, want at least one", element.Store, element.Bucket, element.Owner, len(page.Elements))
			}
			extra, err := gen.Native().Embed(element)
			if err != nil {
				return fmt.Errorf("working out the copy of %s of %s %s: %w", element.Store, element.Bucket, element.Owner, err)
			}
			for _, field := range extra {
				_, isPresent := page.Elements[0].Lookup(field.Name).StringValueOK()
				if isPresent != s.native {
					return fmt.Errorf("%s of %s %s: the %q copy is present=%v after a seed with the %s variant", element.Store, element.Bucket, element.Owner, field.Name, isPresent, s.variant())
				}
			}
			return nil
		}
	}
	return nil
}

// variant is the variant word of the seeder, for messages.
func (s *hoardSeeder) variant() string {
	return model.Variant{Native: s.native, Aggregates: s.aggregates}.String()
}

// verifyAggregates checks the declared aggregates against what the model holds: every aggregate of
// model.LedgerAggregates is ready on the server and the maintained expense sum of every account
// equals the expense_total the model computed, so a seed whose aggregates were declared late or
// lost a write is not reported as a success. Without +agg it checks that none was declared.
func (s *hoardSeeder) verifyAggregates(ctx context.Context, gen *model.Generator, database string, progress *progress) error {
	if gen.Layout() != model.Ledger {
		return nil
	}
	info, err := s.clients[0].DescribeBucket(ctx, database, "Account")
	if err != nil {
		return fmt.Errorf("describing bucket Account to check its aggregates: %w", err)
	}
	want := 0
	if s.aggregates {
		for _, list := range model.LedgerAggregates() {
			want += len(list)
		}
	}
	if len(info.Aggregates) != want {
		return fmt.Errorf("bucket Account declares %d aggregates after a seed with the %s variant, want %d", len(info.Aggregates), s.variant(), want)
	}
	for _, aggregate := range info.Aggregates {
		if aggregate.State != "ready" {
			return fmt.Errorf("aggregate %s %s of %s is %s after the seed, want ready", aggregate.Kind, aggregate.Field, aggregate.Store, aggregate.State)
		}
	}
	if want == 0 {
		return nil
	}
	return pool(ctx, s.workers, gen.Units(), func(ctx context.Context, unit model.Unit) error {
		var total int64
		for _, counter := range unit.Doc.Counters {
			if counter.Name == "expense_total" {
				total, _ = counter.Value.(int64)
			}
		}
		got, err := s.pick().Bucket(database, "Account").Sum(ctx, unit.Doc.ID, "Expenses", "amount")
		if err != nil {
			return fmt.Errorf("reading the maintained expense sum of Account %s: %w", unit.Doc.ID, err)
		}
		if got.Int != total {
			return fmt.Errorf("the maintained expense sum of Account %s is %d after the seed, the model's expense_total is %d", unit.Doc.ID, got.Int, total)
		}
		return nil
	}, progress)
}
