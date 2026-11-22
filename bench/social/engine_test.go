package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestResolveTotal(t *testing.T) {
	cases := []struct {
		name      string
		limit     int
		low, high int64
		pageLen   int
		want      int64
		wantErr   bool
	}{
		{"snapshot count", 20, 25, 25, 20, 25, false},
		{"short timeline", 20, 7, 7, 7, 7, false},
		{"write landed after the page", 20, 5, 6, 5, 5, false},
		{"write landed before the page", 20, 5, 6, 6, 6, false},
		{"both counts above the limit", 20, 30, 31, 20, 31, false},
		{"page too short for either count", 20, 30, 31, 19, 0, true},
		{"page longer than either count", 20, 5, 6, 8, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveTotal(tc.limit, tc.low, tc.high, tc.pageLen)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("resolveTotal = %d, %v; want %d, error=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestMongoWriteConcern(t *testing.T) {
	cases := []struct {
		name    string
		journal bool
		wantW   interface{}
	}{
		{"w1", false, 1},
		{"w1j", true, 1},
		{"majority", false, "majority"},
	}
	for _, tc := range cases {
		concern, err := mongoWriteConcern(tc.name)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if concern.W != tc.wantW || (concern.Journal != nil && *concern.Journal) != tc.journal {
			t.Errorf("%s: got w=%v j=%v, want w=%v j=%v", tc.name, concern.W, concern.Journal, tc.wantW, tc.journal)
		}
	}
	if _, err := mongoWriteConcern("w0"); err == nil {
		t.Error("w0 accepted: an unacknowledged write would make the run measure nothing")
	}
}

func TestParseFlagsEngine(t *testing.T) {
	cfg, err := parseFlags(nil)
	if err != nil || cfg.Engine != engineHoardDB || cfg.MongoWC != "" {
		t.Fatalf("defaults: engine=%q wc=%q err=%v", cfg.Engine, cfg.MongoWC, err)
	}
	for _, args := range [][]string{{"-engine", "redis"}, {"-engine", "mongo", "-mongo-wc", "w0"}} {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
	if _, err := parseFlags([]string{"-engine", "mysql", "-attach", "-depth", "4"}); err != nil {
		t.Errorf("-attach with mysql rejected: %v", err)
	}
}

func TestErrorKind(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{context.DeadlineExceeded, "timeout"},
		{&mysql.MySQLError{Number: 1213}, "server_mysql_1213"},
		{mongo.CommandError{Code: 11000}, "server_mongo_11000"},
		{errors.New("broken pipe"), "io_or_decode"},
	}
	for _, tc := range cases {
		if got := errorKind(tc.err); got != tc.want {
			t.Errorf("errorKind(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestOpClockElapsed(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	stopped := opClock{start: start, stop: start.Add(3 * time.Millisecond)}
	if got := stopped.elapsed(); got != 3*time.Millisecond {
		t.Errorf("stopped clock charged %v, want 3ms", got)
	}
	if running := (&opClock{start: start}).elapsed(); running < time.Hour {
		t.Errorf("running clock charged %v, want at least an hour", running)
	}
}

func TestRedactedHost(t *testing.T) {
	if got := redactedHost("mongodb://admin:secret@db1:27017/?w=1"); got != "db1:27017" || strings.Contains(got, "secret") {
		t.Errorf("redactedHost = %q", got)
	}
}
