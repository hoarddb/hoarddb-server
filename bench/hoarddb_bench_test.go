package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// concurrencyProbeClient answers every request after a short wait and records
// how many SendRequest calls were in flight at once, and which keys arrived.
type concurrencyProbeClient struct {
	mu           sync.Mutex
	inFlight     int
	peakInFlight int
	keysSeen     map[string]int
}

func (probe *concurrencyProbeClient) SendRequest(_ context.Context, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	probe.mu.Lock()
	probe.inFlight++
	probe.peakInFlight = max(probe.peakInFlight, probe.inFlight)
	probe.mu.Unlock()

	time.Sleep(20 * time.Millisecond) // long enough for every worker to overlap

	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.inFlight--
	switch op {
	case transport.OpUpsert:
		var request server.InsertRequest
		if err := bson.Unmarshal(payload, &request); err != nil {
			return nil, fmt.Errorf("unmarshal put: %w", err)
		}
		probe.keysSeen[request.Key]++
	case transport.OpBatchUpsert:
		var request server.BatchInsertRequest
		if err := bson.Unmarshal(payload, &request); err != nil {
			return nil, fmt.Errorf("unmarshal batch put: %w", err)
		}
		for _, doc := range request.Data {
			probe.keysSeen[doc["_id_"].(string)]++
		}
	}
	return &transport.Frame{Opcode: op}, nil
}

func (probe *concurrencyProbeClient) SendGetFast(context.Context, transport.GetFastRequest) (*transport.Frame, error) {
	return nil, fmt.Errorf("the insert phase does not read")
}

func (probe *concurrencyProbeClient) Close() error { return nil }

// TestInsertPhaseKeepsConcRequestsInFlight pins that -conc used to size
// the socket pool and the read phase, while the insert phase sent one request
// at a time, so every published insert figure was single-stream.
func TestInsertPhaseKeepsConcRequestsInFlight(t *testing.T) {
	const totalRecords = 64
	tests := []struct {
		mode      string
		batchSize int
	}{
		{mode: "single", batchSize: 1},
		{mode: "batch", batchSize: 4},
	}
	for _, tc := range tests {
		t.Run(tc.mode, func(t *testing.T) {
			probe := &concurrencyProbeClient{keysSeen: map[string]int{}}
			err := runInsertPhase(context.Background(), probe, insertPhaseRequest{
				Mode:         tc.mode,
				Database:     "BenchTest",
				TotalRecords: totalRecords,
				BatchSize:    tc.batchSize,
				Conc:         8,
			})
			if err != nil {
				t.Fatalf("runInsertPhase: %v", err)
			}
			if probe.peakInFlight != 8 {
				t.Errorf("peak requests in flight = %d, want 8", probe.peakInFlight)
			}
			if len(probe.keysSeen) != totalRecords {
				t.Errorf("distinct keys inserted = %d, want %d", len(probe.keysSeen), totalRecords)
			}
			for key, times := range probe.keysSeen {
				if times != 1 {
					t.Errorf("key %s inserted %d times, want once", key, times)
				}
			}
		})
	}
}

// TestInsertPhaseReportsAFailedRequest checks that a worker's error reaches the
// caller instead of being dropped when the insert phase runs concurrently.
func TestInsertPhaseReportsAFailedRequest(t *testing.T) {
	failing := &failingClient{}
	err := runInsertPhase(context.Background(), failing, insertPhaseRequest{
		Mode: "single", Database: "BenchTest", TotalRecords: 16, BatchSize: 1, Conc: 4,
	})
	if err == nil {
		t.Fatal("runInsertPhase returned nil although every request failed")
	}
}

type failingClient struct{ concurrencyProbeClient }

func (*failingClient) SendRequest(context.Context, transport.Opcode, []byte) (*transport.Frame, error) {
	return nil, fmt.Errorf("connection refused")
}
