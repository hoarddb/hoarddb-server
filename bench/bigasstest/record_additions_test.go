package main

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// stepProbe answers a cumulative CPU time that grows by step on every read, like a server
// process that keeps working.
type stepProbe struct {
	total, step time.Duration
}

func (p *stepProbe) cpuTime() (time.Duration, error) {
	p.total += p.step
	return p.total, nil
}

// The per-operation latency sum lets a reader compute time shares without estimating from
// percentiles; the documents and elements an operation returned show how big its pages were.
func TestCollectRecordsLatencySumAndReturnedSizes(t *testing.T) {
	u := newVuserState()
	for _, n := range []int{3, 5} {
		reply := Reply{Documents: make([]model.Document, n), Elements: make([]model.Element, 2*n), RoundTrips: 1}
		u.record(result{Layout: "chat", Kind: "chat.room_page", Reply: reply}, 10*time.Millisecond, true)
	}
	// A warm-up request and a request that failed (without timing out) add nothing to the sums.
	u.record(result{Layout: "chat", Kind: "chat.room_page", Reply: Reply{Documents: make([]model.Document, 9)}}, time.Second, false)
	u.record(result{Layout: "chat", Kind: "chat.room_page", OpErr: errors.New("refused")}, 5*time.Millisecond, true)
	rec := &Record{}
	collect(rec, []*vuserState{u}, time.Second)
	stats := rec.Ops["chat.room_page"]
	if want := 20.0; math.Abs(stats.LatencySumMs-want) > 0.01 {
		t.Errorf("latency_sum_ms %v, want %v (two ok requests at 10 ms)", stats.LatencySumMs, want)
	}
	if stats.DocumentsPerOp != 4 || stats.ElementsPerOp != 8 {
		t.Errorf("documents per op %v and elements per op %v, want 4 and 8", stats.DocumentsPerOp, stats.ElementsPerOp)
	}
}

// The server CPU over the measured window reaches the record when a probe is given, and the
// record stays valid and unchanged in every other field when it is not.
func TestRunnerRecordsServerCPUOverTheWindow(t *testing.T) {
	p, _ := fakeRun(t, nil)
	p.ServerCPU = &stepProbe{step: 2 * time.Second}
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ServerCPUSeconds != 2 {
		t.Errorf("server_cpu_seconds %v, want 2 (two reads of a probe that advances 2 s)", rec.ServerCPUSeconds)
	}
	if want := 2 / rec.Seconds; math.Abs(rec.ServerCPUCores-want) > 1e-9 {
		t.Errorf("server_cpu_cores %v, want %v", rec.ServerCPUCores, want)
	}
	p, _ = fakeRun(t, nil)
	rec, err = runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ServerCPUSeconds != 0 || rec.ServerCPUCores != 0 {
		t.Errorf("no probe, yet server CPU %v s and %v cores", rec.ServerCPUSeconds, rec.ServerCPUCores)
	}
}

// -server-cpu-cmd prints the server process's cumulative CPU seconds.
func TestCmdCPUReadsSeconds(t *testing.T) {
	got, err := cmdCPU("echo 12.5").cpuTime()
	if err != nil || got != 12500*time.Millisecond {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := cmdCPU("echo many").cpuTime(); err == nil {
		t.Error("a command that prints no number must fail")
	}
}
