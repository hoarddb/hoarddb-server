package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const mongoCollectionName = "doc"

// mongoEngine writes at w:1 j:true: the acknowledgement waits for the journal.
// The key is the document's _id, MongoDB's primary key, holding the same
// 36-character string the other engines key on as _id_.
type mongoEngine struct {
	cfg        *config
	client     *mongo.Client
	collection *mongo.Collection
	concern    *writeconcern.WriteConcern
	targetTxt  string
}

func newMongoEngine(ctx context.Context, cfg *config) (*mongoEngine, error) {
	uri := cfg.DSN
	if uri == "" {
		target := &url.URL{Scheme: "mongodb", Host: cfg.Addr}
		if cfg.User != "" {
			target.User = url.UserPassword(cfg.User, cfg.Password)
		}
		uri = target.String()
	}
	journaled := true
	concern := &writeconcern.WriteConcern{W: 1, Journal: &journaled}
	opts := options.Client().ApplyURI(uri).SetWriteConcern(concern).
		SetMaxPoolSize(uint64(cfg.Workers + 2)).SetMinPoolSize(uint64(cfg.Workers))
	if cfg.TLS {
		opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}) //nolint:gosec // self-signed benchmark node, same rule as the hoardDB client
	}
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB at %s: %w", cfg.Addr, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("pinging MongoDB at %s (check -addr, -user and %s): %w", cfg.Addr, envPassword, err)
	}
	collection := client.Database(cfg.Name).Collection(mongoCollectionName)
	return &mongoEngine{cfg: cfg, client: client, collection: collection, concern: concern, targetTxt: cfg.Addr}, nil
}

// readVariables reads the write concern every insert will send and the storage
// engine's persistence from the server.
func (e *mongoEngine) readVariables(ctx context.Context) (map[string]string, error) {
	admin := e.client.Database("admin")
	var build struct {
		Version string `bson:"version"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&build); err != nil {
		return nil, fmt.Errorf("reading MongoDB buildInfo: %w", err)
	}
	var status struct {
		StorageEngine struct {
			Name       string `bson:"name"`
			Persistent bool   `bson:"persistent"`
		} `bson:"storageEngine"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		return nil, fmt.Errorf("reading MongoDB serverStatus: %w", err)
	}
	// driver v1 exposes no accessor on the collection, so the concern is read back
	// from the one value the client was built with, which every insert inherits.
	concern := e.concern
	return map[string]string{
		"version":            build.Version,
		"storage_engine":     status.StorageEngine.Name,
		"storage_persistent": fmt.Sprint(status.StorageEngine.Persistent),
		"write_concern_w":    fmt.Sprint(concern.W),
		"write_concern_j":    fmt.Sprint(concern.Journal != nil && *concern.Journal),
	}, nil
}

func (e *mongoEngine) target() string { return e.targetTxt }

func (e *mongoEngine) setup(ctx context.Context, _ int) error {
	if err := e.collection.Drop(ctx); err != nil {
		return fmt.Errorf("dropping the old %s collection: %w", mongoCollectionName, err)
	}
	return nil
}

func (e *mongoEngine) newWorker(context.Context) (worker, error) {
	return &mongoWorker{collection: e.collection}, nil
}

func (e *mongoEngine) close() { _ = e.client.Disconnect(context.Background()) }

type mongoWorker struct{ collection *mongo.Collection }

func (w *mongoWorker) close() {}

func (w *mongoWorker) insert(ctx context.Context, id, payload string) error {
	if _, err := w.collection.InsertOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: payload}}); err != nil {
		return fmt.Errorf("insert %s: %w", id, err)
	}
	return nil
}

func (w *mongoWorker) get(ctx context.Context, id string) error {
	err := w.collection.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("get %s: document not found", id)
	}
	if err != nil {
		return fmt.Errorf("get %s: %w", id, err)
	}
	return nil
}
