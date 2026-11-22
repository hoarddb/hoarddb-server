package main

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"
)

// The competitors' social engines fan a publish out in one of two ways. In
// single mode (the default) each follower's feed entry is its own write, one
// after another, as hoardDB's is; in bulk mode it is one multi-row write.

func TestFanoutFlag(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "mongo", "-mix", "rh"})
	if err != nil || cfg.Fanout != fanoutSingle {
		t.Fatalf("default: fanout %q, err %v; want %q", cfg.Fanout, err, fanoutSingle)
	}
	for _, mode := range []string{fanoutSingle, fanoutBulk} {
		if cfg, err := parseFlags([]string{"-engine", "mysql", "-mix", "rh", "-fanout", mode}); err != nil || cfg.Fanout != mode {
			t.Errorf("-fanout %s: got %q, err %v", mode, cfg.Fanout, err)
		}
	}
	if _, err := parseFlags([]string{"-engine", "mongo", "-mix", "rh", "-fanout", "burst"}); err == nil || !strings.Contains(err.Error(), "single or bulk") {
		t.Errorf("-fanout burst: err %v; want one that says single or bulk", err)
	}
	if _, err := parseFlags([]string{"-engine", "hoarddb", "-mix", "rh", "-fanout", "bulk"}); err == nil || !strings.Contains(err.Error(), "hoardDB") {
		t.Errorf("-engine hoarddb -fanout bulk: err %v; want a refusal naming hoardDB, which has no bulk fan-out", err)
	}
}

func TestLegRecordNamesTheFanout(t *testing.T) {
	cases := []struct {
		engine, fanout, want string
	}{{engineMySQL, fanoutBulk, "bulk"}, {engineMongo, fanoutSingle, "single"}, {engineMongo, "", "single"}, {engineHoardDB, fanoutSingle, "single"}}
	for _, tc := range cases {
		record := newLegRecord()
		if err := labelRecord(record, &config{Engine: tc.engine, Fanout: tc.fanout, Placement: "h1"}, tinySpec); err != nil {
			t.Fatal(err)
		}
		if record.Fanout != tc.want {
			t.Errorf("%s -fanout %q recorded %q; want %q", tc.engine, tc.fanout, record.Fanout, tc.want)
		}
	}
}

// TestSummaryKeepsFanoutModesApart: a single leg and a bulk leg of one engine
// are not repeats of each other.
func TestSummaryKeepsFanoutModesApart(t *testing.T) {
	single, bulk := testLeg(func(r *legRecord) { r.Fanout = fanoutSingle }), testLeg(func(r *legRecord) { r.Fanout = fanoutBulk })
	if groups := groupLegs([]*legRecord{single, bulk}); len(groups) != 2 {
		t.Errorf("a single and a bulk leg made %d group(s); want 2", len(groups))
	}
}

// feedInserts counts the statements that write a feed entry.
func feedInserts(queries []string) int {
	total := 0
	for _, query := range queries {
		if strings.Contains(query, "INSERT INTO feed") {
			total++
		}
	}
	return total
}

func TestMySQLPublishFansOutByMode(t *testing.T) {
	followers := [][]driver.Value{{int64(7)}, {int64(8)}, {int64(9)}}
	cases := []struct {
		mode                           string
		statements, feedWrites, begins int
	}{
		{fanoutSingle, 1 + 1 + 3, 3, 0},
		{fanoutBulk, 1 + 1 + 1, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			engine, state := newFakeMySQLEngine(t)
			engine.cfg.Fanout = tc.mode
			state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, followers...)
			ops := &mysqlOps{engine: engine}
			var ledger recordingLedger
			post := postRecord{Author: "3", Seq: 100, Text: "t", Created: time.Now().UTC()}
			delivered, err := ops.publishPost(context.Background(), post, &ledger)
			if err != nil || delivered != 3 {
				t.Fatalf("publishPost: %d followers, %v", delivered, err)
			}
			queries := mysqlStatementsOf(state)
			if got := feedInserts(queries); got != tc.feedWrites {
				t.Errorf("%d feed writes, want %d: %v", got, tc.feedWrites, queries)
			}
			if ops.requests() != int64(tc.statements) || len(queries) != tc.statements {
				t.Errorf("req_per_op counted %d, the driver saw %d statements, want %d", ops.requests(), len(queries), tc.statements)
			}
			if begins := state.count("begin"); begins != tc.begins {
				t.Errorf("%d transactions, want %d (a single-mode publish is one autocommit write after another)", begins, tc.begins)
			}
			if len(ledger.issued) != 3 || len(ledger.acknowledged) != 3 {
				t.Errorf("ledger: %d sent, %d delivered; want 3 and 3", len(ledger.issued), len(ledger.acknowledged))
			}
			if tc.mode == fanoutSingle {
				var order []uint64
				for _, statement := range state.recorded() {
					if strings.Contains(statement.Query, "INSERT INTO feed") {
						order = append(order, uint64(statement.Args[0].(int64)))
					}
				}
				if len(order) != 3 || order[0] != 7 || order[1] != 8 || order[2] != 9 {
					t.Errorf("feed entries went to %v, want followers 7, 8, 9 in the order they were listed", order)
				}
			}
		})
	}
}

func TestMongoPublishFansOutByMode(t *testing.T) {
	cases := []struct {
		mode               string
		commands, feedRuns int
	}{
		{fanoutSingle, 1 + 1 + 3, 3},
		{fanoutBulk, 1 + 1 + 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			server := newFakeMongod(t, []int{7, 8, 9})
			engine := newFakeMongoEngine(t, server, &config{Fanout: tc.mode})
			ops := &mongoOps{engine: engine}
			var ledger recordingLedger
			post := postRecord{Author: "3", Seq: 100, Text: "t", Created: time.Now().UTC()}
			delivered, err := ops.publishPost(context.Background(), post, &ledger)
			if err != nil || delivered != 3 {
				t.Fatalf("publishPost: %d followers, %v", delivered, err)
			}
			var feedWrites, feedDocuments int
			for _, command := range server.received() {
				if command.Name == "insert" && command.Collection == mongoFeed {
					feedWrites++
					feedDocuments += command.Documents
				}
			}
			if feedWrites != tc.feedRuns || feedDocuments != 3 {
				t.Errorf("%d feed inserts carrying %d documents, want %d carrying 3: %v", feedWrites, feedDocuments, tc.feedRuns, server.received())
			}
			if ops.requests() != int64(tc.commands) || len(server.received()) != tc.commands {
				t.Errorf("req_per_op counted %d, the server saw %d commands, want %d: %v", ops.requests(), len(server.received()), tc.commands, server.received())
			}
			if len(ledger.issued) != 3 || len(ledger.acknowledged) != 3 {
				t.Errorf("ledger: %d sent, %d delivered; want 3 and 3", len(ledger.issued), len(ledger.acknowledged))
			}
		})
	}
}

// TestPublishFigureIsHoardDBOnly: ops.W1.fanout_mean and resume_requests_mean
// let a gate subtract the per-follower requests from req_per_op, which only
// means something when a publish is one request per follower. A competitor
// with -fanout bulk sends about 3 requests however many followers there are,
// so there the subtraction would go negative: the fields are hoardDB's alone.
func TestPublishFigureIsHoardDBOnly(t *testing.T) {
	publish := func(engine string, mode string) *mixResult {
		var ops socialOps
		var fanout int
		var err error
		post := postRecord{Author: "3", Seq: 100, Text: "t", Created: time.Now().UTC()}
		var ledger recordingLedger
		switch engine {
		case engineMongo:
			ops = &mongoOps{engine: newFakeMongoEngine(t, newFakeMongod(t, []int{7, 8, 9}), &config{Fanout: mode})}
		case engineMySQL:
			sqlEngine, state := newFakeMySQLEngine(t)
			sqlEngine.cfg.Fanout = mode
			state.answer("FROM follows WHERE followee_id", []string{"follower_id"}, []driver.Value{int64(7)}, []driver.Value{int64(8)}, []driver.Value{int64(9)})
			ops = &mysqlOps{engine: sqlEngine}
		default:
			t.Fatalf("no fake for %s", engine)
		}
		fanout, err = ops.publishPost(context.Background(), post, &ledger)
		if err != nil {
			t.Fatalf("%s publishPost: %v", engine, err)
		}
		result := newMixResult()
		accumulator := &opAccumulator{latencies: newHistogram(), requests: ops.requests(), fanout: int64(fanout)}
		if err := accumulator.latencies.record(time.Millisecond); err != nil {
			t.Fatal(err)
		}
		result.perOperation["W1"] = accumulator
		return result
	}
	for _, engine := range []string{engineMongo, engineMySQL} {
		for _, mode := range []string{fanoutSingle, fanoutBulk} {
			record := newLegRecord()
			cfg := &config{Engine: engine, Fanout: mode}
			if err := fillMixRecord(record, cfg, []*mixResult{publish(engine, mode)}, time.Second); err != nil {
				t.Fatal(err)
			}
			w1 := record.Ops["W1"]
			if w1.FanoutMean != nil || w1.ResumeRequestsMean != nil {
				t.Errorf("%s -fanout %s wrote fanout_mean %v / resume_requests_mean %v; only hoardDB's one-request-per-follower publish may", engine, mode, w1.FanoutMean, w1.ResumeRequestsMean)
			}
		}
	}
	hoard := newMixResult()
	hoard.perOperation["W1"] = &opAccumulator{latencies: newHistogram(), requests: 5, fanout: 3}
	if err := hoard.perOperation["W1"].latencies.record(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	record := newLegRecord()
	if err := fillMixRecord(record, &config{Engine: engineHoardDB, Fanout: fanoutSingle}, []*mixResult{hoard}, time.Second); err != nil {
		t.Fatal(err)
	}
	if w1 := record.Ops["W1"]; w1.FanoutMean == nil || w1.ResumeRequestsMean == nil {
		t.Errorf("hoardDB W1 lacks fanout_mean / resume_requests_mean: %+v", w1)
	}
}
