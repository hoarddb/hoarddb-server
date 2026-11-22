package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// witnessWrites inserts rows one at a time into a table of its own, each its
// own committed autocommit statement; CockroachDB acknowledges a commit after
// the write-ahead log is synced, which is what the witness times.
func (c *cockroachEngine) witnessWrites(ctx context.Context, timed int) (durations []time.Duration, err error) {
	table := witnessNamespace("w")
	if _, err := c.db.ExecContext(ctx, "CREATE TABLE "+table+" (id INT8 NOT NULL PRIMARY KEY, v INT8 NOT NULL)"); err != nil {
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

// checkEncryption holds up to -conc connections to the node and judges each
// from the session the driver negotiated.
func (c *cockroachEngine) checkEncryption(ctx context.Context) ([]tlsFinding, error) {
	node := c.target()
	finding := tlsFinding{Node: node}
	held := make([]*sql.Conn, 0, c.cfg.Conc)
	defer func() {
		for _, conn := range held {
			_ = conn.Close() // returning a checked connection to the pool; nothing to recover
		}
	}()
	plaintext := 0
	var detail string
	for range c.cfg.Conc {
		conn, err := c.db.Conn(ctx)
		if err != nil {
			finding.Detail = fmt.Sprintf("the load client cannot connect: %v", err)
			return []tlsFinding{finding}, nil
		}
		held = append(held, conn)
		var judged tlsFinding
		if err := conn.Raw(func(driverConn any) error {
			wire := driverConn.(*stdlib.Conn).Conn().PgConn().Conn()
			judged = judgeCockroachConnection(node, wire)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reading the session of a connection to %s: %w", node, err)
		}
		if !judged.Encrypted {
			plaintext++
		}
		detail = judged.Detail
	}
	finding.Encrypted = plaintext == 0
	finding.Detail = fmt.Sprintf("%d connections held, %d in the clear; last: %s", len(held), plaintext, strings.TrimSpace(detail))
	return []tlsFinding{finding}, nil
}
