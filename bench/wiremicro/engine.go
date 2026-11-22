package main

import (
	"context"
	"fmt"
)

// engine is one database under test. Everything the phases need goes through
// it, so the runner is the same code for all four.
type engine interface {
	variableReader
	// setup creates the database and the empty table, collection or bucket.
	setup(ctx context.Context, payloadBytes int) error
	// target names the server without credentials, for the result file.
	target() string
	// newWorker opens one connection (hoardDB: socket and login) owned by one goroutine.
	newWorker(ctx context.Context) (worker, error)
	// close releases pools and connections.
	close()
}

// worker is one connection. Its methods block until the server's durable
// acknowledgement (insert) or the document (get) is back.
type worker interface {
	// insert stores a new document and returns once the write is acknowledged durably.
	insert(ctx context.Context, id, payload string) error
	// get reads the document by id and fails when it is not there.
	get(ctx context.Context, id string) error
	close()
}

// newEngine builds the engine cfg names. It opens connections for the checks
// but starts no worker.
func newEngine(ctx context.Context, cfg *config) (engine, error) {
	switch cfg.Engine {
	case engineHoarddb:
		return newHoardEngine(cfg)
	case engineMySQL:
		return newMySQLEngine(ctx, cfg)
	case enginePostgres:
		return newPostgresEngine(ctx, cfg)
	case engineMongo:
		return newMongoEngine(ctx, cfg)
	case engineCockroach:
		return newCockroachEngine(ctx, cfg)
	default:
		return nil, fmt.Errorf("engine %q is not known", cfg.Engine)
	}
}
