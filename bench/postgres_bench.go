// bench/postgres_bench.go — Benchmark PostgreSQL with configurable scale.
//
// Mirrors mysql_bench.go: insert of N rows (single or batch), then N random
// point reads by the unique "username" column, every read verified as a hit or
// a miss. -conc keeps that many requests in flight, as for hoardDB. The table
// is dropped and recreated each run. The tool refuses a server whose commit is
// not on disk when acknowledged (synchronous_commit, fsync, full_page_writes).
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver name
)

// postgresBenchDSN is read from the environment so no credential is stored in
// the repository.
func postgresBenchDSN() (string, error) {
	dsn := os.Getenv("HOARDDB_BENCH_POSTGRES_DSN")
	if dsn == "" {
		return "", errors.New("HOARDDB_BENCH_POSTGRES_DSN is not set. Example: " +
			"HOARDDB_BENCH_POSTGRES_DSN='postgresql://postgres:secret@localhost:5432/postgres?sslmode=disable' hoarddb-bench -db=postgres -mode=batch")
	}
	return dsn, nil
}

// postgresBatchInsert builds one multi-row INSERT for rows [start, end).
func postgresBatchInsert(start, end int) (string, []any) {
	rows := make([]string, 0, end-start)
	args := make([]any, 0, (end-start)*2)
	for offset := 0; offset < end-start; offset++ {
		rows = append(rows, fmt.Sprintf("($%d, $%d)", 2*offset+1, 2*offset+2))
		args = append(args, fmt.Sprintf("user_%06d", start+offset), fmt.Sprintf("u%d@test.com", start+offset))
	}
	return `INSERT INTO "User" (username, email) VALUES ` + strings.Join(rows, ","), args
}

// checkPostgresDurable refuses a server that would acknowledge a commit before
// it is on disk. A setting that could not be read counts as relaxed.
func checkPostgresDurable(settings map[string]string) error {
	var problems []string
	for _, name := range []string{"synchronous_commit", "fsync", "full_page_writes"} {
		if settings[name] != "on" {
			problems = append(problems, fmt.Sprintf("%s is %q, it must be on (run 'ALTER SYSTEM SET %s = on' and reload)", name, settings[name], name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("refusing to run, PostgreSQL is not at durable settings: %s", strings.Join(problems, "; "))
	}
	return nil
}

func readPostgresDurability(ctx context.Context, db *sql.DB) (map[string]string, error) {
	settings := map[string]string{}
	for _, name := range []string{"server_version", "synchronous_commit", "fsync", "full_page_writes"} {
		var setting string
		if err := db.QueryRowContext(ctx, "SELECT current_setting($1)", name).Scan(&setting); err != nil {
			return nil, fmt.Errorf("reading PostgreSQL setting %s: %w", name, err)
		}
		settings[name] = setting
	}
	return settings, nil
}

func benchPostgres(mode string, totalRecords, readCount, conc int) {
	if err := runPostgresBench(mode, totalRecords, readCount, conc); err != nil {
		log.Fatalf("postgres: %v", err)
	}
}

// runInFlight calls work(worker) on conc goroutines and returns their errors joined.
func runInFlight(conc int, work func(worker int) error) error {
	errs := make([]error, conc)
	var wg sync.WaitGroup
	for worker := 0; worker < conc; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[worker] = work(worker)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func runPostgresBench(mode string, totalRecords, readCount, conc int) error {
	if mode != "single" && mode != "batch" {
		return fmt.Errorf("unknown -mode %q; use single or batch", mode)
	}
	conc = max(conc, 1)
	dsn, err := postgresBenchDSN()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("opening PostgreSQL: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(conc + 1)
	db.SetMaxIdleConns(conc + 1)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging PostgreSQL (check HOARDDB_BENCH_POSTGRES_DSN): %w", err)
	}
	settings, err := readPostgresDurability(ctx, db)
	if err != nil {
		return err
	}
	if err := checkPostgresDurable(settings); err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "User"`); err != nil {
		return fmt.Errorf("dropping the old table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE "User" (
		id BIGSERIAL PRIMARY KEY,
		username VARCHAR(64) UNIQUE NOT NULL,
		email VARCHAR(128) NOT NULL
	)`); err != nil {
		return fmt.Errorf("creating the table: %w", err)
	}

	fmt.Printf("PostgreSQL %s | %s | %d records | %d reads | insert-conc=%d | read-conc=%d | synchronous_commit=%s fsync=%s\n",
		settings["server_version"], mode, totalRecords, readCount, conc, conc, settings["synchronous_commit"], settings["fsync"])

	step := int64(1)
	if mode == "batch" {
		step = int64(min(1000, totalRecords))
	}
	var next atomic.Int64 // next row (single) or batch start (batch) to hand out
	insertStart := time.Now()
	err = runInFlight(conc, func(int) error {
		for {
			start := int(next.Add(step) - step)
			if start >= totalRecords {
				return nil
			}
			query, args := postgresBatchInsert(start, min(start+int(step), totalRecords))
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("insert at %d: %w", start, err)
			}
		}
	})
	if err != nil {
		return err
	}
	insertTime := time.Since(insertStart)

	var hits, misses, readsLeft atomic.Int64
	readsLeft.Store(int64(readCount))
	readStart := time.Now()
	err = runInFlight(conc, func(int) error {
		for readsLeft.Add(-1) >= 0 {
			key := fmt.Sprintf("user_%06d", rand.Intn(totalRecords))
			var username, email string
			err := db.QueryRowContext(ctx, `SELECT username, email FROM "User" WHERE username = $1`, key).Scan(&username, &email)
			switch {
			case err == nil && username == key:
				hits.Add(1)
			case err == nil, errors.Is(err, sql.ErrNoRows):
				misses.Add(1)
			default:
				return fmt.Errorf("find %s: %w", key, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	readTime := time.Since(readStart)

	if hits.Load() == 0 {
		fmt.Fprintf(os.Stderr, "WARNING: all %d reads missed — benchmark is not measuring point lookups\n", readCount)
	}
	totalTime := insertTime + readTime
	fmt.Printf("  Insert: %v (%.0f/s) | Read: %v (%.0f/s) | Total: %v (%.0f/s) | hits=%d misses=%d\n",
		insertTime.Round(time.Millisecond), float64(totalRecords)/insertTime.Seconds(),
		readTime.Round(time.Millisecond), float64(readCount)/readTime.Seconds(),
		totalTime.Round(time.Millisecond), float64(totalRecords+readCount)/totalTime.Seconds(),
		hits.Load(), misses.Load())
	return nil
}
