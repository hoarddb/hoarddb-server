package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestRowsMatchTheCountsOfEveryLayout needs no server: the rows the SQL mapping builds from the
// generator must be exactly the counts the verification expects, for every table of every layout.
func TestRowsMatchTheCountsOfEveryLayout(t *testing.T) {
	for _, layout := range model.Layouts {
		gen, err := model.New(layout, 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		var units []model.Unit
		for unit := range gen.Units() {
			units = append(units, unit)
		}
		rows, err := rowsOf(layout, units)
		if err != nil {
			t.Fatal(err)
		}
		want, err := expectedRows(layout, gen.Counts())
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range tables(layout) {
			if int64(len(rows[spec.Name])) != want[spec.Name] {
				t.Errorf("%s.%s: %d rows built, want %d", layout, spec.Name, len(rows[spec.Name]), want[spec.Name])
			}
			for _, row := range rows[spec.Name] {
				if len(row) != len(spec.Cols) {
					t.Fatalf("%s.%s: a row has %d values for %d columns", layout, spec.Name, len(row), len(spec.Cols))
				}
			}
		}
	}
}

func TestMySQLTLSRules(t *testing.T) {
	on := config{MySQLDSN: "bench:bench@tcp(localhost:3306)/ignored", MySQLTLS: true}
	target, err := mysqlConfig(on)
	if err != nil || target.TLSConfig != "skip-verify" || target.DBName != "" || !target.InterpolateParams {
		t.Fatalf("tls on: %+v %v", target, err)
	}
	named := on
	named.MySQLDSN = "bench:bench@tcp(localhost:3306)/?tls=preferred"
	if target, err := mysqlConfig(named); err != nil || target.TLSConfig != "preferred" {
		t.Fatalf("a tls= value of the DSN must be kept: %+v %v", target, err)
	}
	undo := on
	undo.MySQLDSN = "bench:bench@tcp(localhost:3306)/?tls=false"
	if _, err := mysqlConfig(undo); err == nil || !strings.Contains(err.Error(), "-mysql-tls") {
		t.Fatalf("tls=false against -mysql-tls must be refused with the fix: %v", err)
	}
	off := on
	off.MySQLTLS = false
	if target, err := mysqlConfig(off); err != nil || target.TLSConfig != "" {
		t.Fatalf("tls off: %+v %v", target, err)
	}
	if _, err := mysqlConfig(config{MySQLDSN: "not a dsn"}); err == nil {
		t.Fatal("a bad DSN was accepted")
	}
}

// TestSeedMySQL seeds every layout into a real server and skips when MYSQL_DSN is unset. The
// databases it creates carry a time-based name and are dropped afterwards.
func TestSeedMySQL(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN is not set; set it to user:password@tcp(host:3306)/ to run against a MySQL server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := fmt.Sprintf("scn_%d", time.Now().UnixNano()%1_000_000_000)
	cfg := config{Engine: "mysql", Layout: layoutAll, Scale: 1, Seed: 1, Database: base, Workers: 4, MySQLDSN: dsn, MySQLTLS: false}
	t.Cleanup(func() {
		target, err := mysqlConfig(cfg)
		if err != nil {
			return
		}
		admin, err := sql.Open("mysql", target.FormatDSN())
		if err != nil {
			return
		}
		defer func() { _ = admin.Close() }()
		for _, layout := range model.Layouts {
			_, _ = admin.Exec("DROP DATABASE IF EXISTS " + layoutDatabase(base, layout))
		}
	})
	var stdout, stderr bytes.Buffer
	if err := run(ctx, cfg, &stdout, &stderr); err != nil {
		t.Fatalf("%v\n%s", err, stderr.String())
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != len(model.Layouts) {
		t.Fatalf("want one JSON line per layout, got %d", lines)
	}
	again := run(ctx, cfg, &bytes.Buffer{}, &bytes.Buffer{})
	if again == nil || !strings.Contains(again.Error(), "-database") || !strings.Contains(again.Error(), "already exists") {
		t.Fatalf("a second seed must be refused with the way out: %v", again)
	}
}
