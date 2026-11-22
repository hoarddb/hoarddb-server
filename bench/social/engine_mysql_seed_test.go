package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	errMySQLDeadlock = &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "Deadlock found when trying to get lock; try restarting transaction"}
	errMySQLDupKey   = &mysql.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry"}
)

// shortDeadlockBackoff keeps the retry tests fast; the real wait is tens of
// milliseconds per attempt.
func shortDeadlockBackoff(t *testing.T, backoff time.Duration) {
	t.Helper()
	previous := mysqlDeadlockBackoff
	mysqlDeadlockBackoff = backoff
	t.Cleanup(func() { mysqlDeadlockBackoff = previous })
}

// failFirstLikeInserts makes the first count INSERT IGNORE INTO likes
// statements fail with a deadlock, as parallel seed workers do on a real server.
func failFirstLikeInserts(state *fakeSQL, count int64) {
	var failed atomic.Int64
	state.execFailure = func(query string) error {
		if strings.HasPrefix(query, "INSERT IGNORE INTO likes") && failed.Add(1) <= count {
			return errMySQLDeadlock
		}
		return nil
	}
}

// TestMySQLSeedRetriesDeadlocks: MySQL 1213 aborts one of two parallel
// INSERT IGNOREs and asks for a retry (4 of 4 runs at S-10k with 32 workers).
// Two deadlocks then success must seed, and the retries must be counted so a
// run that needed many is visible.
func TestMySQLSeedRetriesDeadlocks(t *testing.T) {
	shortDeadlockBackoff(t, time.Microsecond)
	engine, state := newFakeMySQLEngine(t)
	failFirstLikeInserts(state, 2)
	graph := newSocialGraph(tinySpec, 7)
	if err := engine.seedSocial(context.Background(), graph, 2); err != nil {
		t.Fatalf("seeding failed on a deadlock the server asked to retry: %v", err)
	}
	if got := engine.seedDeadlockRetries.Load(); got != 2 {
		t.Errorf("counted %d deadlock retries; want 2", got)
	}
}

// TestMySQLInsertRowsGivesUpOnPersistentDeadlock: a deadlock that never clears
// fails after the bound with an error that names it.
func TestMySQLInsertRowsGivesUpOnPersistentDeadlock(t *testing.T) {
	shortDeadlockBackoff(t, time.Microsecond)
	engine, state := newFakeMySQLEngine(t)
	state.execFailure = func(string) error { return errMySQLDeadlock }
	err := engine.insertRows(context.Background(), "INSERT IGNORE", "likes", []string{"author_id", "seq", "liker_id", "ts"}, [][]any{{uint64(1), int64(1), uint64(2), graphEpoch}})
	if err == nil {
		t.Fatal("insertRows succeeded against a server that always deadlocks")
	}
	var serverError *mysql.MySQLError
	if !errors.As(err, &serverError) || serverError.Number != 1213 {
		t.Errorf("the error does not wrap the MySQL 1213 error: %v", err)
	}
	if !strings.Contains(err.Error(), "deadlock") || !strings.Contains(err.Error(), "likes") {
		t.Errorf("the error %q does not name the deadlock and the table", err)
	}
	if got := len(state.recorded()); got != mysqlDeadlockAttempts {
		t.Errorf("%d attempts; want the bound of %d", got, mysqlDeadlockAttempts)
	}
	if got := engine.seedDeadlockRetries.Load(); got != mysqlDeadlockAttempts-1 {
		t.Errorf("counted %d retries; want %d", got, mysqlDeadlockAttempts-1)
	}
}

// TestMySQLInsertRowsRetriesOnlyDeadlocks: any other server error is the first
// and last attempt.
func TestMySQLInsertRowsRetriesOnlyDeadlocks(t *testing.T) {
	shortDeadlockBackoff(t, time.Microsecond)
	engine, state := newFakeMySQLEngine(t)
	state.execFailure = func(string) error { return errMySQLDupKey }
	err := engine.insertRows(context.Background(), "INSERT", "users", []string{"id"}, [][]any{{uint64(1)}})
	if !errors.Is(err, errMySQLDupKey) {
		t.Fatalf("got %v; want the duplicate-key error", err)
	}
	if got := len(state.recorded()); got != 1 {
		t.Errorf("a duplicate key was tried %d times; want 1", got)
	}
}

// TestMySQLDeadlockBackoffStopsOnCancel: a cancelled run does not sit out the
// backoff.
func TestMySQLDeadlockBackoffStopsOnCancel(t *testing.T) {
	shortDeadlockBackoff(t, time.Hour)
	engine, state := newFakeMySQLEngine(t)
	state.execFailure = func(string) error { return errMySQLDeadlock }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err := engine.insertRows(cancelled, "INSERT", "users", []string{"id"}, [][]any{{uint64(1)}})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled context gave %v; want context.Canceled", err)
	}
}

// TestMySQLSeedSortsLikesByPrimaryKey: parallel workers lock rows in the same
// order, which keeps most pairs of batches from deadlocking.
func TestMySQLSeedSortsLikesByPrimaryKey(t *testing.T) {
	engine, state := newFakeMySQLEngine(t)
	if err := engine.seedSocial(context.Background(), newSocialGraph(tinySpec, 7), 1); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	var compared int
	for _, statement := range state.recorded() {
		if !strings.HasPrefix(statement.Query, "INSERT IGNORE INTO likes") {
			continue
		}
		for row := 4; row < len(statement.Args); row += 4 {
			previous, current := statement.Args[row-4:row-1], statement.Args[row:row+3]
			if !keyNotAfter(previous, current) {
				t.Fatalf("likes batch is not in primary-key order: %v then %v", previous, current)
			}
			compared++
		}
	}
	if compared == 0 {
		t.Fatal("no like rows were compared")
	}
}

// keyNotAfter reports whether the (author_id, seq, liker_id) key before is at
// or before the key after. database/sql hands the driver every integer as an
// int64.
func keyNotAfter(before, after []driver.Value) bool {
	for column := range before {
		if left, right := before[column].(int64), after[column].(int64); left != right {
			return left < right
		}
	}
	return true
}

// retryingEngine is a social engine whose seeding "needed" a number of deadlock
// retries.
type retryingEngine struct {
	*hoardEngine
	retries int64
}

func (e retryingEngine) seedRetries() int64 { return e.retries }

// TestMixLegNotesSeedDeadlockRetries: a run that needed retries says how many
// in its notes, and a run that needed none stays quiet.
func TestMixLegNotesSeedDeadlockRetries(t *testing.T) {
	for _, test := range []struct {
		retries int64
		want    bool
	}{{15, true}, {0, false}} {
		cfg := mixTestConfig("rh", "social_mix")
		record, err := runMixLeg(context.Background(), cfg, tinySpec, retryingEngine{fakeHoardEngine(cfg), test.retries})
		if err != nil {
			t.Fatalf("running the leg: %v", err)
		}
		noted := slices.Contains(record.Notes, "seed_deadlock_retries=15")
		if noted != test.want {
			t.Errorf("with %d retries the notes are %q; want the retry count present: %v", test.retries, record.Notes, test.want)
		}
	}
}

// liveDatabaseDSN is the DSN of the live tests' scratch database: the same
// server, account and parameters, a different database.
func liveDatabaseDSN(dsn, database string) (string, error) {
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parsing the DSN: %w", err)
	}
	parsed.DBName = database
	return parsed.FormatDSN(), nil
}

// TestLiveDatabaseDSN: the scratch database replaces the DSN's database and
// the rest (user, TLS and every other parameter) survives, so ?tls=skip-verify
// still reaches a REQUIRE SSL account.
func TestLiveDatabaseDSN(t *testing.T) {
	for _, test := range []struct {
		name, dsn string
		wantTLS   string
		wantUser  string
	}{
		{"query kept", "bench:pw@tcp(192.0.2.1:3306)/social?tls=skip-verify", "skip-verify", "bench"},
		{"several parameters", "bench:pw@tcp(192.0.2.1:3306)/social?tls=skip-verify&timeout=5s", "skip-verify", "bench"},
		{"no query", "bench:pw@tcp(192.0.2.1:3306)/social", "", "bench"},
		{"slash and question mark in the password", "bench:p/w?x@tcp(192.0.2.1:3306)/social?tls=skip-verify", "skip-verify", "bench"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := liveDatabaseDSN(test.dsn, "social_live_1")
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := mysql.ParseDSN(dsn)
			if err != nil {
				t.Fatalf("the rewritten DSN %q does not parse: %v", dsn, err)
			}
			if parsed.DBName != "social_live_1" || parsed.User != test.wantUser || parsed.Addr != "192.0.2.1:3306" {
				t.Errorf("rewritten DSN has database %q, user %q, address %q", parsed.DBName, parsed.User, parsed.Addr)
			}
			if parsed.TLSConfig != test.wantTLS {
				t.Errorf("rewritten DSN has tls %q; want %q (the query string was dropped)", parsed.TLSConfig, test.wantTLS)
			}
		})
	}
}
