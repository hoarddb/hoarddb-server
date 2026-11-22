// bench/mysql_bench.go — Benchmark MySQL with configurable scale.
//
// Mirrors the hoardDB/MongoDB benchmarks: batch insert of N rows, then N random
// point reads by the indexed "username" column. The table is dropped and
// recreated at the start of every run so each scale starts clean.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlDSN is read from the environment so no credential is stored in the
// repository.
func mysqlDSN() string {
	v := os.Getenv("HOARDDB_BENCH_MYSQL_DSN")
	if v == "" {
		log.Fatal("HOARDDB_BENCH_MYSQL_DSN is not set. Example: " +
			"HOARDDB_BENCH_MYSQL_DSN='admin:secret@tcp(localhost:3306)/' hoarddb-bench mysql")
	}
	return v
}

func benchMySQL(mode string, totalRecords, readCount int) {
	batchSize := 1000
	if totalRecords < 1000 {
		batchSize = totalRecords
	}

	db, err := sql.Open("mysql", mysqlDSN())
	if err != nil {
		log.Fatalf("Failed to open MySQL: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		log.Fatalf("MySQL ping failed: %v", err)
	}

	// Clean slate + the index that makes point reads a real index lookup.
	_, _ = db.Exec("DROP TABLE IF EXISTS User")
	if _, err := db.Exec(`CREATE TABLE User (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		username VARCHAR(64) UNIQUE NOT NULL,
		email VARCHAR(128) NOT NULL,
		INDEX idx_username (username)
	) ENGINE=InnoDB`); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Printf("MySQL | %s | %d records | %d reads\n", mode, totalRecords, readCount)

	// Insert
	insertStart := time.Now()
	switch mode {
	case "single":
		for i := 0; i < totalRecords; i++ {
			username := fmt.Sprintf("user_%06d", i)
			email := fmt.Sprintf("u%d@test.com", i)
			if _, err := db.Exec("INSERT INTO User (username, email) VALUES (?, ?)", username, email); err != nil {
				log.Fatalf("Insert failed at %d: %v", i, err)
			}
		}
	case "batch":
		for i := 0; i < totalRecords; i += batchSize {
			end := i + batchSize
			if end > totalRecords {
				end = totalRecords
			}
			valueStrings := make([]string, 0, end-i)
			valueArgs := make([]interface{}, 0, (end-i)*2)
			for j := 0; j < end-i; j++ {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, fmt.Sprintf("user_%06d", i+j), fmt.Sprintf("u%d@test.com", i+j))
			}
			query := fmt.Sprintf("INSERT INTO User (username, email) VALUES %s", strings.Join(valueStrings, ","))
			if _, err := db.Exec(query, valueArgs...); err != nil {
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
		var username, email string
		err := db.QueryRow("SELECT username, email FROM User WHERE username = ?", key).Scan(&username, &email)
		switch err {
		case nil:
			hits++
		case sql.ErrNoRows:
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
