// bench/cockroach_bench.go — Benchmark CockroachDB with configurable scale.
//
// Mirrors mysql_bench.go: batch insert of N rows, then N random point reads by
// the indexed "username" column. The table is dropped and recreated at the start
// of every run so each scale starts clean. A single insecure node is assumed;
// the commit sync is the server default.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver name
)

func benchCockroach(mode string, totalRecords, readCount int) {
	dsn, err := cockroachBenchDSN()
	if err != nil {
		log.Fatal(err)
	}
	batchSize := 1000
	if totalRecords < 1000 {
		batchSize = totalRecords
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Failed to open CockroachDB: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		log.Fatalf("CockroachDB ping failed: %v", err)
	}

	// Clean slate + the unique index that makes point reads a real index lookup.
	_, _ = db.Exec(`DROP TABLE IF EXISTS "User"`)
	if _, err := db.Exec(`CREATE TABLE "User" (
		id INT8 PRIMARY KEY DEFAULT unique_rowid(),
		username VARCHAR(64) UNIQUE NOT NULL,
		email VARCHAR(128) NOT NULL
	)`); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Printf("CockroachDB | %s | %d records | %d reads\n", mode, totalRecords, readCount)

	insertStart := time.Now()
	switch mode {
	case "single":
		for i := 0; i < totalRecords; i++ {
			if _, err := db.Exec(`INSERT INTO "User" (username, email) VALUES ($1, $2)`, fmt.Sprintf("user_%06d", i), fmt.Sprintf("u%d@test.com", i)); err != nil {
				log.Fatalf("Insert failed at %d: %v", i, err)
			}
		}
	case "batch":
		for i := 0; i < totalRecords; i += batchSize {
			end := min(i+batchSize, totalRecords)
			valueStrings := make([]string, 0, end-i)
			valueArgs := make([]interface{}, 0, (end-i)*2)
			for j := 0; j < end-i; j++ {
				valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", 2*j+1, 2*j+2))
				valueArgs = append(valueArgs, fmt.Sprintf("user_%06d", i+j), fmt.Sprintf("u%d@test.com", i+j))
			}
			query := fmt.Sprintf(`INSERT INTO "User" (username, email) VALUES %s`, strings.Join(valueStrings, ","))
			if _, err := db.Exec(query, valueArgs...); err != nil {
				log.Fatalf("BatchInsert failed at %d: %v", i, err)
			}
		}
	}
	insertTime := time.Since(insertStart)

	hits, misses := 0, 0
	readStart := time.Now()
	for i := 0; i < readCount; i++ {
		key := fmt.Sprintf("user_%06d", rand.Intn(totalRecords))
		var username, email string
		err := db.QueryRow(`SELECT username, email FROM "User" WHERE username = $1`, key).Scan(&username, &email)
		switch {
		case err == nil:
			hits++
		case errors.Is(err, sql.ErrNoRows):
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
