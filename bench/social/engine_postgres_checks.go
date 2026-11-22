package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// witnessWrites inserts rows one at a time into a table of its own, each its
// own committed autocommit statement; PostgreSQL with synchronous_commit on
// acknowledges a commit after the write-ahead log is flushed, which is what the
// witness times. The scratch table is logged (the default), never UNLOGGED, so
// its commits take the same WAL path as the workload's.
func (c *postgresEngine) witnessWrites(ctx context.Context, timed int) (durations []time.Duration, err error) {
	table := witnessNamespace("w")
	if _, err := c.db.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"); err != nil {
		return nil, fmt.Errorf("creating the scratch table %q: %w", table, err)
	}
	defer func() {
		if _, dropErr := c.db.ExecContext(ctx, "DROP TABLE "+table); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("dropping the scratch table %q: %w", table, dropErr))
		}
	}()
	return timeWrites(timed, func(sequence int) error {
		_, err := c.db.ExecContext(ctx, "INSERT INTO "+table+" (id, v) VALUES ($1, $2)", sequence, sequence)
		return err
	})
}

// checkEncryption holds up to -conc connections to the server and asks the
// server, per connection, what it negotiated: pg_stat_ssl has one row per
// backend, and ssl is false for a session in the clear.
func (c *postgresEngine) checkEncryption(ctx context.Context) ([]tlsFinding, error) {
	node := c.target()
	finding := tlsFinding{Node: node}
	held := make([]*sql.Conn, 0, c.cfg.Conc)
	defer func() {
		for _, conn := range held {
			_ = conn.Close() // returning a checked connection to the pool; nothing to recover
		}
	}()
	plaintext := 0
	var lastDetail string
	for range c.cfg.Conc {
		conn, err := c.db.Conn(ctx)
		if err != nil {
			finding.Detail = fmt.Sprintf("the load client cannot connect: %v", err)
			return []tlsFinding{finding}, nil
		}
		held = append(held, conn)
		var encrypted bool
		var version, cipher sql.NullString
		if err := conn.QueryRowContext(ctx, "SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()").Scan(&encrypted, &version, &cipher); err != nil {
			return nil, fmt.Errorf("asking %s for the session of a connection: %w", node, err)
		}
		if !encrypted {
			plaintext++
		}
		lastDetail = fmt.Sprintf("%s, cipher %s", version.String, cipher.String)
		if !encrypted {
			lastDetail = "plaintext"
		}
	}
	finding.Encrypted = plaintext == 0
	finding.Detail = fmt.Sprintf("%d connections held, %d in the clear; last: %s", len(held), plaintext, lastDetail)
	return []tlsFinding{finding}, nil
}
