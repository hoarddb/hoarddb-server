// bench/mongo_bench.go — Benchmark MongoDB with configurable scale.
//
// Mirrors the hoardDB/MySQL benchmarks: batch insert of N documents, then N
// random point reads by an indexed "username" field. The collection is dropped
// and the index rebuilt at the start of every run so each scale starts clean.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoURI is read from the environment so no credential is stored in the
// repository.
func mongoURI() string {
	v := os.Getenv("HOARDDB_BENCH_MONGO_URI")
	if v == "" {
		log.Fatal("HOARDDB_BENCH_MONGO_URI is not set. Example: " +
			"HOARDDB_BENCH_MONGO_URI='mongodb://admin:secret@localhost:27017' hoarddb-bench mongo")
	}
	return v
}

func benchMongo(mode string, totalRecords, readCount int) {
	batchSize := 1000
	if totalRecords < 1000 {
		batchSize = totalRecords
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7200*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI()))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("MongoDB ping failed: %v", err)
	}

	coll := client.Database("SocialBench").Collection("User")

	// Clean slate + the index that makes point reads a real index lookup.
	if err := coll.Drop(ctx); err != nil {
		log.Fatalf("Failed to drop collection: %v", err)
	}
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "username", Value: 1}},
	}); err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}

	fmt.Printf("MongoDB | %s | %d records | %d reads\n", mode, totalRecords, readCount)

	// Insert
	insertStart := time.Now()
	switch mode {
	case "single":
		for i := 0; i < totalRecords; i++ {
			doc := bson.M{"username": fmt.Sprintf("user_%06d", i), "email": fmt.Sprintf("u%d@test.com", i)}
			if _, err := coll.InsertOne(ctx, doc); err != nil {
				log.Fatalf("Insert failed at %d: %v", i, err)
			}
		}
	case "batch":
		for i := 0; i < totalRecords; i += batchSize {
			end := i + batchSize
			if end > totalRecords {
				end = totalRecords
			}
			docs := make([]interface{}, end-i)
			for j := range docs {
				docs[j] = bson.M{"username": fmt.Sprintf("user_%06d", i+j), "email": fmt.Sprintf("u%d@test.com", i+j)}
			}
			if _, err := coll.InsertMany(ctx, docs); err != nil {
				log.Fatalf("BatchInsert failed at %d: %v", i, err)
			}
		}
	}
	insertTime := time.Since(insertStart)

	// Read
	hits, misses := 0, 0
	readStart := time.Now()
	for i := 0; i < readCount; i++ {
		key := fmt.Sprintf("user_%06d", rand.Intn(totalRecords))
		var result bson.M
		err := coll.FindOne(ctx, bson.M{"username": key}).Decode(&result)
		switch {
		case err == nil:
			hits++
		case errors.Is(err, mongo.ErrNoDocuments):
			misses++
		default:
			log.Fatalf("Find failed at %d: %v", i, err)
		}
	}
	readTime := time.Since(readStart)

	if hits == 0 {
		fmt.Fprintf(os.Stderr, "WARNING: all %d reads missed — benchmark is not measuring point lookups\n", readCount)
	}

	totalTime := insertTime + readTime
	fmt.Printf("  Insert: %v (%.0f/s) | Read: %v (%.0f/s) | Total: %v (%.0f/s) | hits=%d misses=%d\n",
		insertTime.Round(time.Millisecond), float64(totalRecords)/insertTime.Seconds(),
		readTime.Round(time.Millisecond), float64(readCount)/readTime.Seconds(),
		totalTime.Round(time.Millisecond), float64(totalRecords+readCount)/totalTime.Seconds(),
		hits, misses)
}
