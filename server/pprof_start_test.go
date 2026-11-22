package server

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// syncBuffer is a bytes.Buffer safe for the logger's goroutine and the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var pprofListeningLine = regexp.MustCompile(`msg="pprof debug server listening" address=(\S+)`)

// TestPProfStartsWithMetricsDisabled: With metrics off (the
// shipped default) and pprof on, StartMetricsServer returned before it reached
// the pprof block: no server on the pprof port, no log line, no error. The
// pprof server must start on its own switch, and answer.
func TestPProfStartsWithMetricsDisabled(t *testing.T) {
	previous := pprofListen
	pprofListen = "127.0.0.1:0"
	t.Cleanup(func() { pprofListen = previous })

	var logged syncBuffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	cfg := config.DefaultConfig()
	cfg.Metrics.Enabled = false
	cfg.Debug.PProfEnabled = true
	if err := StartMetricsServer(cfg, logger, nil); err != nil {
		t.Fatalf("StartMetricsServer: %v", err)
	}

	var address string
	deadline := time.Now().Add(5 * time.Second)
	for address == "" && time.Now().Before(deadline) {
		if match := pprofListeningLine.FindStringSubmatch(logged.String()); match != nil {
			address = match[1]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if address == "" {
		t.Fatalf("with metrics disabled and pprof enabled, no pprof server started; the log says only:\n%s", logged.String())
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", address))
	if err != nil {
		t.Fatalf("the pprof server logged %s but does not answer: %v", address, err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Errorf("closing the pprof response: %v", err)
	}
	if readErr != nil {
		t.Fatalf("reading the pprof index: %v", readErr)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "goroutine") {
		t.Fatalf("GET /debug/pprof/ answered %d, not the pprof index", resp.StatusCode)
	}
}

// TestPProfSamplingRatesAreSetOnlyWhenPProfIsEnabled: block and mutex profiles are empty unless the
// runtime is told to sample, so enabling pprof sets both from the config, and a node without pprof
// leaves the runtime alone (sampling costs a little on every blocking call).
func TestPProfSamplingRatesAreSetOnlyWhenPProfIsEnabled(t *testing.T) {
	var blockRate, mutexFraction, calls int
	previousBlock, previousMutex := setBlockProfileRate, setMutexProfileFraction
	setBlockProfileRate = func(rate int) { blockRate = rate; calls++ }
	setMutexProfileFraction = func(rate int) int { mutexFraction = rate; calls++; return 0 }
	t.Cleanup(func() { setBlockProfileRate, setMutexProfileFraction = previousBlock, previousMutex })

	cfg := config.DefaultConfig()
	cfg.Debug.PProfEnabled = false
	applyPProfSampling(cfg)
	if calls != 0 {
		t.Fatalf("pprof off: the runtime was told to sample %d times", calls)
	}
	cfg.Debug.PProfEnabled = true
	cfg.Debug.PProfBlockRateNS, cfg.Debug.PProfMutexFraction = 2000, 7
	applyPProfSampling(cfg)
	if blockRate != 2000 || mutexFraction != 7 {
		t.Fatalf("rates set to %d ns and 1 in %d, want 2000 and 7", blockRate, mutexFraction)
	}
	cfg.Debug.PProfBlockRateNS, cfg.Debug.PProfMutexFraction = 0, 0
	applyPProfSampling(cfg)
	if blockRate != 0 || mutexFraction != 0 {
		t.Fatalf("0 must switch sampling off, got %d and %d", blockRate, mutexFraction)
	}
}
