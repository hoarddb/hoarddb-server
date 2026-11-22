package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// witnessNamespace names the scratch database, collection or table of one
// witness run. The time makes a leftover from a killed run harmless.
func witnessNamespace(base string) string {
	return fmt.Sprintf("%s_witness_%d", base, time.Now().UnixNano())
}

// timeWrites runs warmup + timed calls of write one after another and returns
// the timed durations. write gets the sequence number of the call.
func timeWrites(timed int, write func(sequence int) error) ([]time.Duration, error) {
	durations := make([]time.Duration, 0, timed)
	for sequence := range witnessWarmup + timed {
		started := time.Now()
		if err := write(sequence); err != nil {
			return nil, fmt.Errorf("write %d: %w", sequence, err)
		}
		if sequence >= witnessWarmup {
			durations = append(durations, time.Since(started))
		}
	}
	return durations, nil
}

// witnessWrites puts documents one at a time into a database of its own on one
// connection, which carries the run's write concern like every other.
func (h *hoardEngine) witnessWrites(_ context.Context, timed int) (durations []time.Duration, err error) {
	caller, release, err := h.openCaller()
	if err != nil {
		return nil, err
	}
	defer release()
	scratch := witnessNamespace(h.cfg.Database)
	var created server.CreateDatabaseResponse
	if err := caller.call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: scratch}, &created); err != nil || !created.Success {
		return nil, fmt.Errorf("creating the scratch database %q: %w", scratch, errors.Join(err, messageError(created.Message)))
	}
	defer func() {
		var dropped server.DeleteDatabaseResponse
		if dropErr := caller.call(transport.OpDeleteDB, server.DeleteDatabaseRequest{Name: scratch}, &dropped); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("dropping the scratch database %q: %w", scratch, dropErr))
		}
	}()
	if err := createSocialBucket(caller, server.CreateBucketRequest{Database: scratch, Name: "w", Type: server.BucketTypeHash}); err != nil {
		return nil, err
	}
	return timeWrites(timed, func(sequence int) error {
		var stored server.InsertResponse
		request := server.InsertRequest{Database: scratch, Bucket: "w", Key: strconv.Itoa(sequence), Data: map[string]interface{}{"n": sequence}}
		if err := caller.call(transport.OpUpsert, request, &stored); err != nil {
			return err
		}
		if !stored.Success {
			return messageError(stored.Message)
		}
		return nil
	})
}

// messageError turns a server's refusal text into an error, or nil when empty.
func messageError(message string) error {
	if message == "" {
		return nil
	}
	return errors.New(message)
}

// witnessWrites inserts documents one at a time into a collection of its own,
// at the write concern the run uses.
func (m *mongoEngine) witnessWrites(ctx context.Context, timed int) (durations []time.Duration, err error) {
	collection := m.client.Database(m.cfg.Database).Collection(witnessNamespace("w"))
	defer func() {
		if dropErr := collection.Drop(ctx); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("dropping the scratch collection %q: %w", collection.Name(), dropErr))
		}
	}()
	return timeWrites(timed, func(sequence int) error {
		_, err := collection.InsertOne(ctx, bson.D{{Key: "_id", Value: sequence}})
		return err
	})
}

// witnessWrites inserts rows one at a time into a table of its own, each its
// own committed transaction on the source.
func (m *mysqlEngine) witnessWrites(ctx context.Context, timed int) (durations []time.Duration, err error) {
	table := witnessNamespace("w")
	if _, err := m.db.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB"); err != nil {
		return nil, fmt.Errorf("creating the scratch table %q: %w", table, err)
	}
	defer func() {
		if _, dropErr := m.db.ExecContext(ctx, "DROP TABLE "+table); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("dropping the scratch table %q: %w", table, dropErr))
		}
	}()
	return timeWrites(timed, func(sequence int) error {
		_, err := m.db.ExecContext(ctx, "INSERT INTO "+table+" (id, v) VALUES (?, ?)", sequence, sequence)
		return err
	})
}
