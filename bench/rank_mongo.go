// bench/rank_mongo.go — the MongoDB leg of the keyed rank (rank_bench.go).
//
// Both keyed stores are collections under _id, which MongoDB always indexes
// with a B-tree: there is no hash analogue on _id, so the "hash" and "btree"
// rows are the same structure on this engine and are reported as such. Ops run
// one at a time on one goroutine, so the driver has one command outstanding.
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

const rankMongoDatabase = "hoardRank"

type rankMongo struct {
	ctx     context.Context
	client  *mongo.Client
	journal bool
	legID   rankLeg
}

// rankMongoDoc is the stored shape: the key is _id, as it would be in an
// idiomatic MongoDB schema.
type rankMongoDoc struct {
	Key     string `bson:"_id"`
	Seq     int64  `bson:"seq"`
	Payload string `bson:"payload"`
}

func newRankMongo(ctx context.Context, journal bool, leg rankLeg) (*rankMongo, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI()))
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("pinging MongoDB: %w", err)
	}
	durability, err := rankMongoDurability(ctx, client, journal)
	if err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	leg.Durability = durability
	leg.Engine = "MongoDB"
	if journal {
		leg.Engine = "MongoDB j:true"
	}
	return &rankMongo{ctx: ctx, client: client, journal: journal, legID: leg}, nil
}

// rankMongoDurability reads the settings that decide when an acknowledged
// write is on disk, rather than assuming the defaults.
func rankMongoDurability(ctx context.Context, client *mongo.Client, journal bool) (string, error) {
	admin := client.Database("admin")
	var buildInfo struct {
		Version string `bson:"version"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		return "", fmt.Errorf("reading MongoDB buildInfo: %w", err)
	}
	var commitInterval bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "getParameter", Value: 1}, {Key: "journalCommitInterval", Value: 1}}).Decode(&commitInterval); err != nil {
		return "", fmt.Errorf("reading MongoDB journalCommitInterval: %w", err)
	}
	// getDefaultRWConcern is refused on a standalone mongod, so the implicit
	// {w:1} is stated rather than read; the storage engine is read.
	var serverStatus struct {
		StorageEngine struct {
			Name string `bson:"name"`
		} `bson:"storageEngine"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus); err != nil {
		return "", fmt.Errorf("reading MongoDB serverStatus: %w", err)
	}
	requested := "implicit {w:1}, no j"
	if journal {
		requested = "{w:1, j:true} per write"
	}
	return fmt.Sprintf("mongod %s standalone, %s; write concern %s; journalCommitInterval=%vms",
		buildInfo.Version, serverStatus.StorageEngine.Name, requested, commitInterval["journalCommitInterval"]), nil
}

func (m *rankMongo) leg() rankLeg { return m.legID }

// createdKnown is true: an upsert reports UpsertedCount.
func (m *rankMongo) createdKnown() bool { return true }

func (m *rankMongo) close() error {
	if err := m.client.Disconnect(m.ctx); err != nil {
		return fmt.Errorf("disconnecting from MongoDB: %w", err)
	}
	return nil
}

func (m *rankMongo) open(store string) (rankStore, error) {
	collectionOptions := options.Collection()
	if m.journal {
		collectionOptions.SetWriteConcern(writeconcern.Journaled())
	}
	collection := m.client.Database(rankMongoDatabase).Collection("rank_"+store, collectionOptions)
	// Dropping gives every leg fresh state; _id is indexed on creation.
	if err := collection.Drop(m.ctx); err != nil {
		return nil, fmt.Errorf("dropping collection %s: %w", collection.Name(), err)
	}
	// Created explicitly so the first timed upsert does not also pay for the
	// implicit create — the other engines create their bucket or table here too.
	if err := m.client.Database(rankMongoDatabase).CreateCollection(m.ctx, collection.Name()); err != nil {
		return nil, fmt.Errorf("creating collection %s: %w", collection.Name(), err)
	}
	return &rankMongoCollection{ctx: m.ctx, collection: collection}, nil
}

type rankMongoCollection struct {
	ctx        context.Context
	collection *mongo.Collection
}

func (c *rankMongoCollection) upsert(doc rankDoc) (bool, error) {
	replaceResult, err := c.collection.ReplaceOne(c.ctx,
		bson.D{{Key: "_id", Value: doc.Key}},
		rankMongoDoc(doc),
		options.Replace().SetUpsert(true))
	if err != nil {
		return false, fmt.Errorf("upserting %s: %w", doc.Key, err)
	}
	return replaceResult.UpsertedCount == 1, nil
}

func (c *rankMongoCollection) read(key string) (rankDoc, bool, error) {
	var stored rankMongoDoc
	err := c.collection.FindOne(c.ctx, bson.D{{Key: "_id", Value: key}}).Decode(&stored)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return rankDoc{}, false, nil
	}
	if err != nil {
		return rankDoc{}, false, fmt.Errorf("finding %s: %w", key, err)
	}
	return rankDoc(stored), true, nil
}

func (c *rankMongoCollection) scan(low, high string) ([]rankDoc, error) {
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: low}, {Key: "$lte", Value: high}}}}
	cursor, err := c.collection.Find(c.ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("finding _id in [%s, %s]: %w", low, high, err)
	}
	var stored []rankMongoDoc
	if err := cursor.All(c.ctx, &stored); err != nil {
		return nil, fmt.Errorf("reading the cursor for [%s, %s]: %w", low, high, err)
	}
	band := make([]rankDoc, len(stored))
	for i, doc := range stored {
		band[i] = rankDoc(doc)
	}
	return band, nil
}
