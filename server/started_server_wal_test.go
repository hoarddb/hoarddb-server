package server

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// attachWALToStartedServer gives a server that newTestServer has already
// started a write-ahead log. The capacity sampler's walBytes probe reads s.wal
// on its own goroutine, so a bare assignment races it; production
// sets the WAL in NewService, before the sampler starts.
//
// The swap is made under the sampler's own sampleMu, which each sample holds
// from start to finish, so every sample is ordered before or after it.
func attachWALToStartedServer(s *Server, w *storage.WAL) {
	s.capacity.sampleMu.Lock()
	defer s.capacity.sampleMu.Unlock()
	s.wal = w
}

// TestAttachingAWALToAStartedServerDoesNotRaceTheSampler drives the sampler
// from a second goroutine, as its ticker does, while the WAL is attached. The
// ticker is a fixed 15 s, so the original race fired only when a tick landed
// inside the test; the loop makes that certain. It needs -race to fail.
func TestAttachingAWALToAStartedServerDoesNotRaceTheSampler(t *testing.T) {
	s := newTestServer(t)
	w, err := storage.OpenWAL(filepath.Join(t.TempDir(), "wal"), nil)
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	defer func() { _ = w.Close() }()

	var samplesTaken atomic.Int64
	stopSampling := make(chan struct{})
	samplingStopped := make(chan struct{})
	go func() {
		defer close(samplingStopped)
		for {
			select {
			case <-stopSampling:
				return
			default:
				s.capacity.sampleNow()
				samplesTaken.Add(1)
			}
		}
	}()

	waitForSamples := func(atLeast int64) {
		for samplesTaken.Load() < atLeast {
			// busy-wait: a sample takes microseconds
		}
	}
	waitForSamples(1)
	attachWALToStartedServer(s, w)
	waitForSamples(samplesTaken.Load() + 3)
	close(stopSampling)
	<-samplingStopped
}
