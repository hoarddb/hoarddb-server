package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestMongoDocumentsCarryIDsAndBinaryVectors needs no server: a table keyed by id uses it as
// _id, a compound-keyed table leaves _id to the server and gets a unique index instead, and a
// vector is a BSON binary of subtype 0 of 256 bytes.
func TestMongoDocumentsCarryIDsAndBinaryVectors(t *testing.T) {
	gen, err := model.New(model.Agent, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	var units []model.Unit
	for unit := range gen.Units() {
		units = append(units, unit)
	}
	rows, err := rowsOf(model.Agent, units)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range agentTables {
		documents := documentsOf(spec, rows[spec.Name])
		if len(documents) == 0 {
			t.Fatalf("%s: no documents", spec.Name)
		}
		first := documents[0].(bson.D)
		hasID := first[0].Key == "_id"
		if hasID != mongoIdentity(spec) {
			t.Errorf("%s: _id present %v, keyed by id %v", spec.Name, hasID, mongoIdentity(spec))
		}
		if !mongoIdentity(spec) && !mongoIndexes(spec)[0].Unique {
			t.Errorf("%s: the primary key must be a unique index", spec.Name)
		}
	}
	memory := documentsOf(agentTables[6], rows["memories"])[0].(bson.D)
	vec, ok := memory[3].Value.(primitive.Binary)
	if memory[3].Key != "vec" || !ok || vec.Subtype != 0 || len(vec.Data) != 4*model.VectorLen {
		t.Errorf("memory vec is %+v, want a 256-byte binary of subtype 0", memory[3])
	}
}

// TestSeedMongo seeds every layout into a real server and skips when MONGO_URI is unset. The
// databases it creates carry a time-based name and are dropped afterwards.
func TestSeedMongo(t *testing.T) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI is not set; set it to mongodb://host:27017 to run against a MongoDB server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := fmt.Sprintf("scn_%d", time.Now().UnixNano()%1_000_000_000)
	cfg := config{Engine: "mongo", Layout: layoutAll, Scale: 1, Seed: 1, Database: base, Workers: 4, MongoURI: uri, MongoTLS: false}
	t.Cleanup(func() {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
		if err != nil {
			return
		}
		defer func() { _ = client.Disconnect(context.Background()) }()
		for _, layout := range model.Layouts {
			_ = client.Database(layoutDatabase(base, layout)).Drop(context.Background())
		}
	})
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != len(model.Layouts) {
		t.Fatalf("want one JSON line per layout, got %d", lines)
	}
	again := run(ctx, cfg, &bytes.Buffer{}, &bytes.Buffer{})
	if again == nil || !strings.Contains(again.Error(), "-database") || !strings.Contains(again.Error(), "already exists") {
		t.Fatalf("a second seed must be refused with the way out: %v", again)
	}
}
