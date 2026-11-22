package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// repeated is n copies of d.
func repeated(d time.Duration, n int) []time.Duration {
	durations := make([]time.Duration, n)
	for i := range durations {
		durations[i] = d
	}
	return durations
}

// TestWitnessMedianIsNotTheMean: a few slow syncs among fast unsafe acks must not
// lift the median over the line.
func TestWitnessMedianIsNotTheMean(t *testing.T) {
	durations := append(repeated(200*time.Microsecond, 180), repeated(50*time.Millisecond, 20)...)
	if result := judgeWitness(engineHoardDB, "L-A", durations, syncFigures{DatasyncMs: 3.9}); result.Pass {
		t.Errorf("a median of %.3f ms passed against 3.9 ms because of 20 slow outliers", result.Depth1WriteP50Ms)
	}
}

// witnessStub is an engine that can only time writes, with the durability facts
// and write durations the test hands it.
type witnessStub struct {
	engine
	facts     map[string]string
	durations []time.Duration
}

func (s witnessStub) durability() map[string]string { return s.facts }
func (s witnessStub) witnessWrites(context.Context, int) ([]time.Duration, error) {
	return s.durations, nil
}

func TestWitnessModePrintsOneLineAndFailsOnUnsafeAck(t *testing.T) {
	cfg := &config{Engine: engineHoardDB, Level: "L-A", SyncMS: 3.9}
	var out bytes.Buffer
	err := judgeEngineWitness(context.Background(), cfg, witnessStub{durations: repeated(270*time.Microsecond, 200)}, &out)
	if err == nil || !strings.Contains(err.Error(), "witness failed") {
		t.Fatalf("error = %v; want a failed witness", err)
	}
	var printed witnessResult
	if err := json.Unmarshal(out.Bytes(), &printed); err != nil || printed.Pass || printed.Mode != "witness" {
		t.Errorf("printed %q (%v); want one JSON result with pass=false", out.String(), err)
	}
	out.Reset()
	if err := judgeEngineWitness(context.Background(), cfg, witnessStub{durations: repeated(4500*time.Microsecond, 200)}, &out); err != nil {
		t.Errorf("a durable engine failed the witness: %v", err)
	}
}

// TestWitnessModeFailsOnTheLaunchFiles: even slow writes do not save a hoardDB run
// whose launch environment contradicts the level.
func TestWitnessModeFailsOnTheLaunchFiles(t *testing.T) {
	cfg := &config{Engine: engineHoardDB, Level: "L-A", SyncMS: 3.9}
	stub := witnessStub{durations: repeated(4500*time.Microsecond, 200), facts: map[string]string{durabilityFailureKey: "the run claims L-A but the switch is on"}}
	if err := judgeEngineWitness(context.Background(), cfg, stub, &bytes.Buffer{}); err == nil {
		t.Error("the witness passed although the launch files contradict the level")
	}
}

// TestHoardWitnessWritesOneAtATimeInAScratchDatabase: the timed writes are
// sequential on one connection, carry the run's write concern, and go to a
// database that is created and dropped around them.
func TestHoardWitnessWritesOneAtATimeInAScratchDatabase(t *testing.T) {
	recorder := newConcernRecorder("majority")
	durations, err := recorder.engine.witnessWrites(context.Background(), 25)
	if err != nil || len(durations) != 25 {
		t.Fatalf("witnessWrites = %d durations, %v; want the 25 timed writes", len(durations), err)
	}
	if len(recorder.wires) != 1 {
		t.Errorf("%d connections used; depth 1 is one", len(recorder.wires))
	}
	var puts, creates, drops int
	for _, request := range recorder.sent() {
		switch request.op {
		case transport.OpUpsert:
			puts++
			if concern, _ := writeConcernOf(request.request); concern != "majority" {
				t.Errorf("a witness write carried concern %q; want the run's", concern)
			}
		case transport.OpCreateDB:
			creates++
		case transport.OpDeleteDB:
			drops++
		}
	}
	if puts != 25+witnessWarmup || creates != 1 || drops != 1 {
		t.Errorf("puts %d (want %d), creates %d, drops %d; want the warm-up and timed writes between one create and one drop", puts, 25+witnessWarmup, creates, drops)
	}
}

// TestMySQLWitnessWritesCommittedRowsInAScratchTable: each timed write is its
// own autocommit INSERT on the source, between a CREATE and a DROP.
func TestMySQLWitnessWritesCommittedRowsInAScratchTable(t *testing.T) {
	name := t.Name() + "/source"
	engine := &mysqlEngine{cfg: &config{Engine: engineMySQL}, db: recordingPool(t, name)}
	durations, err := engine.witnessWrites(context.Background(), 12)
	if err != nil || len(durations) != 12 {
		t.Fatalf("witnessWrites = %d durations, %v; want 12", len(durations), err)
	}
	statements := statementsOn(name)
	if len(statements) != 12+witnessWarmup+2 || !strings.Contains(statements[0], "CREATE TABLE") || !strings.Contains(statements[len(statements)-1], "DROP TABLE") {
		t.Errorf("%d statements, first %q; want a CREATE, %d INSERTs and a DROP", len(statements), statements[0], 12+witnessWarmup)
	}
}

func TestWitnessNeedsTheMeasuredSync(t *testing.T) {
	err := runWitness(context.Background(), &config{Engine: engineHoardDB}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "-sync-ms") {
		t.Errorf("error = %v; want one naming -sync-ms", err)
	}
}
