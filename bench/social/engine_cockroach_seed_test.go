package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	errCockroachRestart = &pgconn.PgError{Code: "40001", Message: "restart transaction: TransactionRetryWithProtoRefreshError"}
	errCockroachDupKey  = &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}
)

// shortRetryBackoff keeps the retry tests fast; the real wait is tens of
// milliseconds per attempt.
func shortRetryBackoff(t *testing.T, backoff time.Duration) {
	t.Helper()
	previous := pgRetryBackoff
	pgRetryBackoff = backoff
	t.Cleanup(func() { pgRetryBackoff = previous })
}

// cockroachFailFirstLikeInserts makes the first count like INSERTs fail with the
// serialization failure CockroachDB asks clients to retry.
func cockroachFailFirstLikeInserts(state *fakeSQL, count int64) {
	var failed atomic.Int64
	state.execFailure = func(query string) error {
		if strings.HasPrefix(query, "INSERT INTO likes") && failed.Add(1) <= count {
			return errCockroachRestart
		}
		return nil
	}
}

// TestCockroachSeedRetriesSerializationFailures: CockroachDB aborts one of two
// contending transactions with SQLSTATE 40001 and asks the client to retry. Two
// failures then success must seed, and the retries must be counted so a run
// that needed many is visible.
func TestCockroachSeedRetriesSerializationFailures(t *testing.T) {
	shortRetryBackoff(t, time.Microsecond)
	engine, state := newFakeCockroachEngine(t)
	cockroachFailFirstLikeInserts(state, 2)
	if err := engine.seedSocial(context.Background(), newSocialGraph(tinySpec, 7), 2); err != nil {
		t.Fatalf("seeding failed on a failure the server asked to retry: %v", err)
	}
	if got := engine.seedRetries(); got != 2 {
		t.Errorf("counted %d retries; want 2", got)
	}
}

// TestCockroachInsertRowsGivesUpOnPersistentRestart: a failure that never clears
// stops after the bound with an error that names the table and wraps the cause.
func TestCockroachInsertRowsGivesUpOnPersistentRestart(t *testing.T) {
	shortRetryBackoff(t, time.Microsecond)
	engine, state := newFakeCockroachEngine(t)
	state.execFailure = func(string) error { return errCockroachRestart }
	err := engine.insertRows(context.Background(), "likes", []string{"author_id", "seq", "liker_id", "ts"}, [][]any{{int64(1), int64(1), int64(2), graphEpoch}}, onConflictDoNothing)
	if err == nil {
		t.Fatal("insertRows succeeded against a server that always asks for a restart")
	}
	var serverError *pgconn.PgError
	if !errors.As(err, &serverError) || serverError.Code != "40001" {
		t.Errorf("the error does not wrap the 40001 error: %v", err)
	}
	if !strings.Contains(err.Error(), "likes") {
		t.Errorf("the error %q does not name the table", err)
	}
	if got := len(state.recorded()); got != pgRetryAttempts {
		t.Errorf("%d attempts; want the bound of %d", got, pgRetryAttempts)
	}
	if got := engine.seedRetries(); got != pgRetryAttempts-1 {
		t.Errorf("counted %d retries; want %d", got, pgRetryAttempts-1)
	}
}

// TestCockroachInsertRowsRetriesOnlyRestarts: any other server error is the
// first and last attempt.
func TestCockroachInsertRowsRetriesOnlyRestarts(t *testing.T) {
	shortRetryBackoff(t, time.Microsecond)
	engine, state := newFakeCockroachEngine(t)
	state.execFailure = func(string) error { return errCockroachDupKey }
	err := engine.insertRows(context.Background(), "users", []string{"id"}, [][]any{{int64(1)}}, "")
	if !errors.Is(err, errCockroachDupKey) {
		t.Fatalf("got %v; want the duplicate-key error", err)
	}
	if got := len(state.recorded()); got != 1 {
		t.Errorf("a duplicate key was tried %d times; want 1", got)
	}
}

// TestCockroachRetryBackoffStopsOnCancel: a cancelled run does not sit out the
// backoff.
func TestCockroachRetryBackoffStopsOnCancel(t *testing.T) {
	shortRetryBackoff(t, time.Hour)
	engine, state := newFakeCockroachEngine(t)
	state.execFailure = func(string) error { return errCockroachRestart }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err := engine.insertRows(cancelled, "users", []string{"id"}, [][]any{{int64(1)}}, "")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled context gave %v; want context.Canceled", err)
	}
}

// TestCockroachSeedSortsLikesByPrimaryKey: parallel workers write rows in the
// same order, which keeps most pairs of batches from contending.
func TestCockroachSeedSortsLikesByPrimaryKey(t *testing.T) {
	engine, state := newFakeCockroachEngine(t)
	if err := engine.seedSocial(context.Background(), newSocialGraph(tinySpec, 7), 1); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	var compared int
	for _, statement := range state.recorded() {
		if !strings.HasPrefix(statement.Query, "INSERT INTO likes") {
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

// TestCockroachInsertRowsSplitsLargeBatches: a statement stays at
// pgInsertRows rows, far below the 65,535 parameters the wire protocol allows.
func TestCockroachInsertRowsSplitsLargeBatches(t *testing.T) {
	engine, state := newFakeCockroachEngine(t)
	rows := make([][]any, 2*pgInsertRows+1)
	for i := range rows {
		rows[i] = []any{int64(i), int64(i)}
	}
	if err := engine.insertRows(context.Background(), "t", []string{"a", "b"}, rows, ""); err != nil {
		t.Fatal(err)
	}
	statements := state.recorded()
	if len(statements) != 3 {
		t.Fatalf("%d statements; want 3", len(statements))
	}
	for _, statement := range statements {
		if len(statement.Args) > 65535 {
			t.Errorf("a statement binds %d parameters; the protocol allows 65535", len(statement.Args))
		}
	}
	if !strings.Contains(statements[0].Query, "($999,$1000)") {
		t.Errorf("the first statement does not end its numbering at $1000: %q", statements[0].Query[max(0, len(statements[0].Query)-40):])
	}
}

// TestCockroachTransactionRetriesARestart: a transaction that fails with 40001
// is rolled back and run again from the top, and the retry is counted.
func TestCockroachTransactionRetriesARestart(t *testing.T) {
	shortRetryBackoff(t, time.Microsecond)
	engine, state := newFakeCockroachEngine(t)
	var failed atomic.Int64
	state.execFailure = func(query string) error {
		if strings.HasPrefix(query, "INSERT INTO likes") && failed.Add(1) == 1 {
			return errCockroachRestart
		}
		return nil
	}
	ops := &cockroachOps{engine: engine}
	if err := ops.likePost(context.Background(), 4, 3, 1); err != nil {
		t.Fatalf("a like failed on a restart the server asked for: %v", err)
	}
	if begins, commits, rollbacks := state.count("begin"), state.count("commit"), state.count("rollback"); begins != 2 || commits != 1 || rollbacks != 1 {
		t.Errorf("%d begin, %d commit, %d rollback; want the failed attempt rolled back and a second one committed", begins, commits, rollbacks)
	}
	if got := engine.transactionRetries.Load(); got != 1 {
		t.Errorf("counted %d transaction retries; want 1", got)
	}
}

// TestCockroachTransactionDoesNotRetryOtherErrors: only 40001 is the client's
// to retry; a duplicate key is the operation's answer.
func TestCockroachTransactionDoesNotRetryOtherErrors(t *testing.T) {
	shortRetryBackoff(t, time.Microsecond)
	engine, state := newFakeCockroachEngine(t)
	state.execFailure = func(string) error { return errCockroachDupKey }
	ops := &cockroachOps{engine: engine}
	err := ops.likePost(context.Background(), 4, 3, 1)
	if !errors.Is(err, errCockroachDupKey) {
		t.Fatalf("got %v; want the duplicate-key error", err)
	}
	if state.count("begin") != 1 || state.count("rollback") != 1 {
		t.Errorf("%d begin, %d rollback; want one attempt rolled back", state.count("begin"), state.count("rollback"))
	}
}

// TestIsCockroachRestart pins the one SQLSTATE the retry loops act on, also
// when the driver wraps it.
func TestIsCockroachRestart(t *testing.T) {
	if !isCockroachRestart(errCockroachRestart) {
		t.Error("40001 is not a restart")
	}
	if !isCockroachRestart(wrapForTest(errCockroachRestart)) {
		t.Error("a wrapped 40001 is not a restart")
	}
	if isCockroachRestart(errCockroachDupKey) || isCockroachRestart(nil) || isCockroachRestart(driver.ErrBadConn) {
		t.Error("an error that is not 40001 counted as a restart")
	}
}

func wrapForTest(err error) error { return errors.Join(errors.New("executing"), err) }
