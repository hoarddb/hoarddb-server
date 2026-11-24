// Package seed loads the example data sets of seed/model (social, chat, ledger, agent memory,
// video) into a running hoarddb-server through the ring client. The same code backs
// 'hoarddb seed'; it knows no other database engine.
package seed

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/seed/model"
)

// Options say how a Seeder writes.
type Options struct {
	// Workers is the number of documents written at once; one worker writes a document and its
	// store elements in order.
	Workers int
	// ReplicationFactor is the factor of the databases the seed creates.
	ReplicationFactor int
	// Native embeds the copies of the native layout variant (model.Native) into the elements.
	// An engine-test feature: 'hoarddb seed' never sets it.
	Native bool
	// Aggregates declares the ledger's maintained aggregates (model.LedgerAggregates) when the
	// buckets are created. An engine-test feature: 'hoarddb seed' never sets it.
	Aggregates bool
}

// DatabaseExistsError says the database already holds buckets of the layout, so seeding it
// again would collide with them.
type DatabaseExistsError struct {
	Database string
	// Buckets are the layout's buckets that already exist in it, in the layout's order.
	Buckets []string
}

func (e *DatabaseExistsError) Error() string {
	return fmt.Sprintf("database %q already holds bucket(s) %v of this layout", e.Database, e.Buckets)
}

// Seeder loads a layout through the entry-ring client: a document is an Insert and a store
// element is a Push (fifoStore, lifoStore) or a Put (hashStore, btreeStore) on that document.
// It borrows its clients; the caller closes them.
type Seeder struct {
	clients []*ringclient.Client
	options Options
	next    atomic.Uint64
}

// New returns a Seeder that spreads its writes over clients, which must not be empty.
func New(clients []*ringclient.Client, options Options) *Seeder {
	return &Seeder{clients: clients, options: options}
}

// pick returns a client round-robin, so the workers spread over the connections.
func (s *Seeder) pick() *ringclient.Client {
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
// that already exists is reused only when none of the layout's buckets is in it, which is how
// it looks after its buckets were dropped (there is no drop database statement); otherwise
// seeding would collide with what is there, and a *DatabaseExistsError names the buckets.
func (s *Seeder) createSchema(ctx context.Context, gen *model.Generator, database string) error {
	client := s.clients[0]
	if _, err := client.CreateDatabase(ctx, database, int32(s.options.ReplicationFactor)); err != nil {
		var refusal *ringclient.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeConflict {
			return fmt.Errorf("creating database %q: %w", database, err)
		}
		if err := s.refuseExistingBuckets(ctx, gen, database); err != nil {
			return err
		}
	}
	for _, bucket := range gen.Schema() {
		if _, err := client.CreateBucket(ctx, database, bucket.Name, declared(bucket, s.options.Aggregates)); err != nil {
			return fmt.Errorf("creating bucket %q in database %q: %w", bucket.Name, database, err)
		}
	}
	return nil
}

// Check returns a *DatabaseExistsError when database already holds a bucket of the layout, before
// anything is written. A database that does not exist, or exists with none of the layout's
// buckets, passes.
func (s *Seeder) Check(ctx context.Context, gen *model.Generator, database string) error {
	_, err := s.clients[0].DescribeDatabase(ctx, database, ringclient.PageRequest{Limit: 1})
	var refusal *ringclient.Error
	switch {
	case err == nil:
		return s.refuseExistingBuckets(ctx, gen, database)
	case errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound:
		return nil
	}
	return fmt.Errorf("looking for database %q: %w", database, err)
}

// refuseExistingBuckets returns a *DatabaseExistsError when database holds any bucket of the layout.
func (s *Seeder) refuseExistingBuckets(ctx context.Context, gen *model.Generator, database string) error {
	var present []string
	for _, bucket := range gen.Schema() {
		_, err := s.clients[0].DescribeBucket(ctx, database, bucket.Name)
		var refusal *ringclient.Error
		switch {
		case err == nil:
			present = append(present, bucket.Name)
		case errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound:
		default:
			return fmt.Errorf("looking for bucket %q in database %q: %w", bucket.Name, database, err)
		}
	}
	if len(present) > 0 {
		return &DatabaseExistsError{Database: database, Buckets: present}
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

// document is the body Insert takes: a map, because the client marshals a document as a BSON
// document keyed by field name, and the model keeps its fields as an ordered list.
func document(fields []model.Field) ringclient.Doc {
	doc := make(ringclient.Doc, len(fields))
	for _, field := range fields {
		doc[field.Name] = scalar(field.Value)
	}
	return doc
}

// elementBody is the body Push and Put take: a bson.D, not a map, because an element keeps its
// fields in the model's order and the layout's checks (and the native variant) compare that order.
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
func (s *Seeder) writeUnit(ctx context.Context, database string, unit model.Unit, native *model.Native, progress *Progress) error {
	bucket := s.pick().Bucket(database, unit.Doc.Bucket)
	outcome, err := bucket.Insert(ctx, unit.Doc.ID, document(unit.Doc.Fields))
	progress.Add(1)
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
		progress.Add(1)
		if err := applied(fmt.Sprintf("writing %s of %s %s", element.Store, unit.Doc.Bucket, element.Owner), outcome, err); err != nil {
			return err
		}
	}
	return nil
}

// Seed creates the database, writes every unit and verifies the counts.
func (s *Seeder) Seed(ctx context.Context, gen *model.Generator, database string, progress *Progress) (int64, error) {
	if err := s.createSchema(ctx, gen, database); err != nil {
		return 0, err
	}
	progress.Phase("documents and their stores")
	var native *model.Native
	if s.options.Native {
		native = gen.Native()
	}
	err := Pool(ctx, s.options.Workers, gen.Units(), func(ctx context.Context, unit model.Unit) error {
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
	return progress.Total(), nil
}

// verify reads every count back: documents per bucket with Count, and the elements of every
// store of every document with StoreCount, so a seed that lost a write does not report success.
// A heapStore is skipped: the client has no push that takes a priority, so none is seeded.
func (s *Seeder) verify(ctx context.Context, gen *model.Generator, database string, progress *Progress) error {
	progress.Phase("count verification")
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
	return Pool(ctx, s.options.Workers, gen.Units(), func(ctx context.Context, unit model.Unit) error {
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
func (s *Seeder) verifyVariant(ctx context.Context, gen *model.Generator, database string) error {
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
				if isPresent != s.options.Native {
					return fmt.Errorf("%s of %s %s: the %q copy is present=%v after a seed with the %s variant", element.Store, element.Bucket, element.Owner, field.Name, isPresent, s.variant())
				}
			}
			return nil
		}
	}
	return nil
}

// variant is the variant word of the seeder, for messages.
func (s *Seeder) variant() string {
	return model.Variant{Native: s.options.Native, Aggregates: s.options.Aggregates}.String()
}

// verifyAggregates checks the declared aggregates against what the model holds: every aggregate of
// model.LedgerAggregates is ready on the server and the maintained expense sum of every account
// equals the expense_total the model computed, so a seed whose aggregates were declared late or
// lost a write is not reported as a success. Without +agg it checks that none was declared.
func (s *Seeder) verifyAggregates(ctx context.Context, gen *model.Generator, database string, progress *Progress) error {
	if gen.Layout() != model.Ledger {
		return nil
	}
	accountBucket, err := s.clients[0].DescribeBucket(ctx, database, "Account")
	if err != nil {
		return fmt.Errorf("describing bucket Account to check its aggregates: %w", err)
	}
	want := 0
	if s.options.Aggregates {
		for _, list := range model.LedgerAggregates() {
			want += len(list)
		}
	}
	if len(accountBucket.Aggregates) != want {
		return fmt.Errorf("bucket Account declares %d aggregates after a seed with the %s variant, want %d", len(accountBucket.Aggregates), s.variant(), want)
	}
	for _, aggregate := range accountBucket.Aggregates {
		if aggregate.State != "ready" {
			return fmt.Errorf("aggregate %s %s of %s is %s after the seed, want ready", aggregate.Kind, aggregate.Field, aggregate.Store, aggregate.State)
		}
	}
	if want == 0 {
		return nil
	}
	return Pool(ctx, s.options.Workers, gen.Units(), func(ctx context.Context, unit model.Unit) error {
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
