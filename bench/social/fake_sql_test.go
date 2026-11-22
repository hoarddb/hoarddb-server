package main

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
)

// A database/sql driver that executes nothing and remembers every statement, by
// the name the pool was opened with, so a test can see which server each query
// of a multi-server MySQL engine reached.

var (
	recordedMu       sync.Mutex
	recordedByServer = map[string][]string{}
	registerRecorder sync.Once
)

type recordingDriver struct{}

func (recordingDriver) Open(name string) (driver.Conn, error) {
	return &recordingConn{server: name}, nil
}

type recordingConn struct{ server string }

func (c *recordingConn) Prepare(query string) (driver.Stmt, error) {
	return &recordingStmt{server: c.server, query: query}, nil
}
func (c *recordingConn) Close() error              { return nil }
func (c *recordingConn) Begin() (driver.Tx, error) { return recordingTx{}, nil }

type recordingTx struct{}

func (recordingTx) Commit() error   { return nil }
func (recordingTx) Rollback() error { return nil }

type recordingStmt struct{ server, query string }

func (s *recordingStmt) Close() error  { return nil }
func (s *recordingStmt) NumInput() int { return -1 }
func (s *recordingStmt) record(kind string) {
	recordedMu.Lock()
	defer recordedMu.Unlock()
	recordedByServer[s.server] = append(recordedByServer[s.server], kind+" "+s.query)
}

func (s *recordingStmt) Exec([]driver.Value) (driver.Result, error) {
	s.record("exec")
	return driver.RowsAffected(1), nil
}

func (s *recordingStmt) Query([]driver.Value) (driver.Rows, error) {
	s.record("query")
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Columns() []string         { return []string{"c"} }
func (emptyRows) Close() error              { return nil }
func (emptyRows) Next([]driver.Value) error { return io.EOF }

// recordingPool opens a pool whose statements are recorded under name.
func recordingPool(t *testing.T, name string) *sql.DB {
	t.Helper()
	registerRecorder.Do(func() { sql.Register("g4b-recording", recordingDriver{}) })
	recordedMu.Lock()
	recordedByServer[name] = nil
	recordedMu.Unlock()
	db, err := sql.Open("g4b-recording", name)
	if err != nil {
		t.Fatalf("opening the recording pool %s: %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// statementsOn is what the pool named server was asked to run.
func statementsOn(server string) []string {
	recordedMu.Lock()
	defer recordedMu.Unlock()
	return slices.Clone(recordedByServer[server])
}

// countKind counts the statements that start with kind: "query" or "exec".
func countKind(statements []string, kind string) int {
	count := 0
	for _, statement := range statements {
		if strings.HasPrefix(statement, kind+" ") {
			count++
		}
	}
	return count
}
