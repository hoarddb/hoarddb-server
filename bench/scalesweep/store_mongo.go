package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	mongoDatabase   = "scale"
	mongoCollection = "records"
	duplicateKey    = 11000
	loadBatch       = 1000
	loadParallel    = 8
)

// mongoStore keeps the records in one collection keyed by _id with a secondary
// index on owner.
type mongoStore struct {
	client   *mongo.Client
	records  *mongo.Collection
	concern  string
	poolSize int
}

func newMongoStore(ctx context.Context, uri, concernName string, poolSize int) (*mongoStore, error) {
	var concern *writeconcern.WriteConcern
	switch concernName {
	case "w1j":
		journaled := true
		concern = &writeconcern.WriteConcern{W: 1, Journal: &journaled}
	case "w1":
		concern = writeconcern.W1()
	default:
		return nil, fmt.Errorf("unknown -mongo-wc %q: use w1j (journaled, the default) or w1", concernName)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).
		SetMaxPoolSize(uint64(poolSize)).SetMinPoolSize(uint64(poolSize)).SetWriteConcern(concern))
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("MongoDB did not answer at the -mongo-uri given; is the container up (docker ps)? %w", err)
	}
	return &mongoStore{
		client: client, records: client.Database(mongoDatabase).Collection(mongoCollection),
		concern: "w=1,j=" + fmt.Sprint(concernName == "w1j"), poolSize: poolSize,
	}, nil
}

func (m *mongoStore) name() string       { return "mongo" }
func (m *mongoStore) durability() string { return m.concern }
func (m *mongoStore) close()             { _ = m.client.Disconnect(context.Background()) }

func (m *mongoStore) prepare(ctx context.Context) error {
	index := mongo.IndexModel{Keys: bson.D{{Key: "owner", Value: 1}}}
	if _, err := m.records.Indexes().CreateOne(ctx, index); err != nil {
		return fmt.Errorf("creating the owner index: %w", err)
	}
	return nil
}

func (m *mongoStore) nextKey(ctx context.Context) (int64, error) {
	var top struct {
		ID int64 `bson:"_id"`
	}
	err := m.records.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}).SetProjection(bson.D{{Key: "_id", Value: 1}})).Decode(&top)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading the highest key: %w", err)
	}
	return top.ID + 1, nil
}

func mongoDoc(rec record) bson.D {
	return bson.D{{Key: "_id", Value: rec.ID}, {Key: "owner", Value: rec.Owner}, {Key: "seq", Value: rec.Seq}, {Key: "body", Value: rec.Body}}
}

// loadRange inserts in unordered batches over several connections; a key that
// already exists is skipped, so a repeated range is harmless.
func (m *mongoStore) loadRange(ctx context.Context, from, to int64, bodyBytes int) error {
	return parallelBatches(ctx, from, to, func(batchFrom, batchTo int64) error {
		docs := make([]any, 0, batchTo-batchFrom)
		for id := batchFrom; id < batchTo; id++ {
			docs = append(docs, mongoDoc(newRecord(id, bodyBytes)))
		}
		_, err := m.records.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		if err != nil && !onlyDuplicates(err) {
			return fmt.Errorf("loading keys %d to %d: %w", batchFrom, batchTo, err)
		}
		return nil
	})
}

func onlyDuplicates(err error) bool {
	var bulk mongo.BulkWriteException
	if !errors.As(err, &bulk) || bulk.WriteConcernError != nil {
		return false
	}
	for _, failure := range bulk.WriteErrors {
		if failure.Code != duplicateKey {
			return false
		}
	}
	return true
}

func (m *mongoStore) get(ctx context.Context, id int64) error {
	var found bson.Raw
	if err := m.records.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&found); err != nil {
		return fmt.Errorf("reading key %d: %w", id, err)
	}
	return nil
}

func (m *mongoStore) update(ctx context.Context, id, version int64, bodyBytes int) error {
	change := bson.D{{Key: "$set", Value: bson.D{{Key: "seq", Value: version}, {Key: "body", Value: bodyFor(id, version, bodyBytes)}}}}
	res, err := m.records.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, change)
	if err != nil {
		return fmt.Errorf("updating key %d: %w", id, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("updating key %d: no such record", id)
	}
	return nil
}

func (m *mongoStore) insert(ctx context.Context, id int64, bodyBytes int) error {
	if _, err := m.records.InsertOne(ctx, mongoDoc(newRecord(id, bodyBytes))); err != nil {
		return fmt.Errorf("inserting key %d: %w", id, err)
	}
	return nil
}

func (m *mongoStore) byOwner(ctx context.Context, owner int64) (int, error) {
	cursor, err := m.records.Find(ctx, bson.D{{Key: "owner", Value: owner}})
	if err != nil {
		return 0, fmt.Errorf("reading owner %d: %w", owner, err)
	}
	var rows []bson.Raw
	if err := cursor.All(ctx, &rows); err != nil {
		return 0, fmt.Errorf("reading owner %d: %w", owner, err)
	}
	return len(rows), nil
}
