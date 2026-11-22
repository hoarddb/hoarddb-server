package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"slices"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

const (
	// mongoInsertDocuments bounds the documents of one InsertMany; mongoUnitBatch is how many
	// units one task carries.
	mongoInsertDocuments = 500
	mongoUnitBatch       = 50
)

// mongoSeeder loads a layout into one collection per entity and per store, never an array
// inside an owner's document.
type mongoSeeder struct {
	client  *mongo.Client
	workers int
}

// newMongoSeeder connects with write concern w1j ({w:1, j:true}, spelled out so the request
// does not depend on a server default), the concern the social harness uses.
func newMongoSeeder(ctx context.Context, cfg config) (*mongoSeeder, error) {
	journaled := true
	clientOptions := options.Client().ApplyURI(cfg.MongoURI).
		SetMaxPoolSize(uint64(cfg.Workers)).
		SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journaled})
	if cfg.MongoTLS {
		clientOptions.SetTLSConfig(&tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, //nolint:gosec // self-signed certificates, the same rule as the hoardDB client (spec 3.1)
		})
	}
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB (check -mongo-uri): %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("pinging MongoDB (check -mongo-uri and -mongo-tls): %w", err)
	}
	return &mongoSeeder{client: client, workers: cfg.Workers}, nil
}

func (s *mongoSeeder) close() { _ = s.client.Disconnect(context.Background()) }

// createSchema creates the collections and their indexes. A database that exists is refused
// with the way out.
func (s *mongoSeeder) createSchema(ctx context.Context, layout model.Layout, database string) error {
	names, err := s.client.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: database}})
	if err != nil {
		return fmt.Errorf("listing databases: %w", err)
	}
	if slices.Contains(names, database) {
		return fmt.Errorf("database %q already exists on this server, so it may already hold this layout and seeding again would collide with it. "+
			"Seed a fresh database with -database <name>, or drop it with: use %s; db.dropDatabase()", database, database)
	}
	db := s.client.Database(database)
	for _, spec := range tables(layout) {
		if err := db.CreateCollection(ctx, spec.Name); err != nil {
			return fmt.Errorf("creating collection %q: %w", spec.Name, err)
		}
		for _, idx := range mongoIndexes(spec) {
			keys := make(bson.D, len(idx.Keys))
			for i, name := range idx.Keys {
				keys[i] = bson.E{Key: name, Value: 1}
			}
			indexModel := mongo.IndexModel{Keys: keys, Options: options.Index().SetUnique(idx.Unique)}
			if _, err := db.Collection(spec.Name).Indexes().CreateOne(ctx, indexModel); err != nil {
				return fmt.Errorf("creating index %v on %s: %w", idx.Keys, spec.Name, err)
			}
		}
	}
	return nil
}

// documentsOf turns the rows of a table into documents, naming the id column _id where the
// table is keyed by it.
func documentsOf(spec table, rows [][]any) []any {
	documents := make([]any, len(rows))
	for i, row := range rows {
		document := make(bson.D, len(row))
		for j, col := range spec.Cols {
			name := col.Name
			if name == "id" && mongoIdentity(spec) {
				name = "_id"
			}
			cell := row[j]
			if packed, ok := cell.([]byte); ok {
				cell = primitive.Binary{Subtype: 0, Data: packed}
			}
			document[j] = bson.E{Key: name, Value: cell}
		}
		documents[i] = document
	}
	return documents
}

// seed creates the database, loads the units in batches and verifies the document counts.
func (s *mongoSeeder) seed(ctx context.Context, gen *model.Generator, database string, progress *progress) (int64, error) {
	layout := gen.Layout()
	if err := s.createSchema(ctx, layout, database); err != nil {
		return 0, err
	}
	progress.phase("documents")
	db := s.client.Database(database)
	specs := map[string]table{}
	for _, spec := range tables(layout) {
		specs[spec.Name] = spec
	}
	var written atomic.Int64
	err := pool(ctx, s.workers, chunks(gen.Units(), mongoUnitBatch), func(ctx context.Context, batch []model.Unit) error {
		rows, err := rowsOf(layout, batch)
		if err != nil {
			return err
		}
		for name, tableRows := range rows {
			documents := documentsOf(specs[name], tableRows)
			for len(documents) > 0 {
				part := documents[:min(len(documents), mongoInsertDocuments)]
				documents = documents[len(part):]
				if _, err := db.Collection(name).InsertMany(ctx, part, options.InsertMany().SetOrdered(false)); err != nil {
					return fmt.Errorf("inserting %d documents into %s: %w", len(part), name, err)
				}
			}
			written.Add(int64(len(tableRows)))
			progress.add(int64(len(tableRows)))
		}
		return nil
	}, progress)
	if err != nil {
		return 0, fmt.Errorf("writing %s: %w", layout, err)
	}
	want, err := expectedRows(layout, gen.Counts())
	if err != nil {
		return 0, err
	}
	for _, spec := range tables(layout) {
		got, err := db.Collection(spec.Name).CountDocuments(ctx, bson.D{})
		if err != nil {
			return 0, fmt.Errorf("counting collection %s: %w", spec.Name, err)
		}
		if got != want[spec.Name] {
			return 0, fmt.Errorf("collection %s holds %d documents after the seed, want %d", spec.Name, got, want[spec.Name])
		}
	}
	return written.Load(), nil
}
