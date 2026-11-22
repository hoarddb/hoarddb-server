package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

const (
	flushFailureHelperDirEnv  = "HOARDDB_FLUSH_FAILURE_HELPER_DIR"
	flushFailureHelperModeEnv = "HOARDDB_FLUSH_FAILURE_HELPER_MODE"

	// removedDataDir removes the data directory under the running node:
	// neither a memtable log nor a table can be created.
	removedDataDir = "removed-data-dir"
	// tablesCannotBeCreated puts a directory where each of the next table
	// files would go: memtable logs can be created, every flush fails.
	tablesCannotBeCreated = "tables-cannot-be-created"
)

// TestANodeThatCannotPersistRefusesWritesAndStops defends the case of a node
// that cannot persist its memtables and went on acknowledging writes. With its
// data directory removed, the first memtable that filled could not be
// replaced and the next write ended the process; with every flush failing,
// the writes were acknowledged until every memtable was full, then blocked,
// and Stop waited forever on a flush retried every second. The node must
// refuse writes with an error naming the store directory and the cause,
// report itself unhealthy, and stop within a bound. Each case runs in a child
// process, so that the old crash or hang fails this test rather than the
// whole package.
func TestANodeThatCannotPersistRefusesWritesAndStops(t *testing.T) {
	for _, mode := range []string{removedDataDir, tablesCannotBeCreated} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFlushFailureHelperProcess$", "-test.v")
			cmd.Env = append(os.Environ(), flushFailureHelperDirEnv+"="+t.TempDir(), flushFailureHelperModeEnv+"="+mode)
			helperOutput, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("the node did not finish within 2 minutes: a write or Stop hung\n%s", helperOutput)
			}
			if err != nil {
				t.Fatalf("the node failed: %v\n%s", err, helperOutput)
			}
			if !strings.Contains(string(helperOutput), "FLUSH-FAILURE-HELPER-DONE") {
				t.Fatalf("the helper did not run its scenario\n%s", helperOutput)
			}
		})
	}
}

// TestFlushFailureHelperProcess is the child of
// TestANodeThatCannotPersistRefusesWritesAndStops; run alone it skips.
func TestFlushFailureHelperProcess(t *testing.T) {
	dataDir := os.Getenv(flushFailureHelperDirEnv)
	if dataDir == "" {
		t.Skip("run by TestANodeThatCannotPersistRefusesWritesAndStops")
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = dataDir
	cfg.Storage.MemtableMB = config.MinMemtableMB
	cfg.Server.ReplicationFactor = 1
	service, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	node := service.core
	if resp, err := node.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp, err := node.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	storeDir := filepath.Join(dataDir, nodeStoreDirName)
	mode := os.Getenv(flushFailureHelperModeEnv)
	cause := breakTheStore(t, mode, dataDir, storeDir)

	// 64 KiB documents: the 32 MiB memtable fills after about 500. Then its
	// successor's log cannot be created, or its flush fails.
	pad := strings.Repeat("x", 64<<10)
	refusal := ""
	wantedWords := []string{storeDir, cause, "refuses writes", "free space"}
	if mode == removedDataDir {
		// BUG-0390: every write into the open memtable log of a removed
		// directory was acknowledged until the memtable filled, and lost at
		// the next restart. Once the store has re-checked its directory (it
		// does so at most every 100 ms), the first write is refused.
		time.Sleep(400 * time.Millisecond)
		if refusal = insertRefusal(node, "first-after-removal", pad); refusal == "" {
			t.Fatalf("the first insert after the data directory was removed was acknowledged; it is lost at the next restart")
		}
		wantedWords = []string{storeDir, cause, "refuses writes", "removed or replaced", "restart"}
	}
	for index := 0; index < 4000 && refusal == ""; index++ {
		refusal = insertRefusal(node, fmt.Sprintf("event-%05d", index), pad)
	}
	if refusal == "" {
		t.Fatalf("4,000 inserts of 64 KiB (250 MiB) were acknowledged into a store that cannot persist them")
	}
	t.Logf("refused with: %s", refusal)
	for _, wanted := range wantedWords {
		if !strings.Contains(refusal, wanted) {
			t.Errorf("the refusal does not name %q: %s", wanted, refusal)
		}
	}
	if again := insertRefusal(node, "after-the-refusal", "x"); again == "" {
		t.Errorf("an insert after the refusal was acknowledged")
	}

	health, err := node.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Healthy {
		t.Errorf("a node that refuses every write reports itself healthy: %+v", health)
	}
	if !strings.Contains(health.StoreRefusal, storeDir) || !strings.Contains(health.StoreRefusal, cause) {
		t.Errorf("status does not say why the store refuses writes: %q", health.StoreRefusal)
	}
	probe := httptest.NewRecorder()
	newMetricsMux(node).ServeHTTP(probe, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if probe.Code != http.StatusServiceUnavailable || !strings.Contains(probe.Body.String(), "refuses writes") {
		t.Errorf("/ready answered %d %q for a node that refuses every write", probe.Code, probe.Body.String())
	}

	stopped := make(chan error, 1)
	go func() { stopped <- service.Stop() }()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		t.Fatalf("Stop did not return within 15 s")
	}
	fmt.Println("FLUSH-FAILURE-HELPER-DONE")
}

// breakTheStore makes the node's store unable to persist, the way mode says,
// and returns the cause its refusal must name.
func breakTheStore(t *testing.T, mode, dataDir, storeDir string) string {
	switch mode {
	case removedDataDir:
		if err := os.RemoveAll(dataDir); err != nil {
			t.Fatalf("removing the data directory: %v", err)
		}
		return "no such file or directory"
	case tablesCannotBeCreated:
		// A new store has no table yet, so its tables are numbered from 1;
		// each flush attempt takes the next number.
		for id := 1; id <= 1000; id++ {
			if err := os.Mkdir(filepath.Join(storeDir, fmt.Sprintf("%06d.sst", id)), 0o700); err != nil {
				t.Fatalf("blocking table %d: %v", id, err)
			}
		}
		return "file exists"
	default:
		t.Fatalf("unknown mode %q", mode)
		return ""
	}
}

// insertRefusal inserts one document and returns why it was refused, or ""
// when it was acknowledged.
func insertRefusal(node *Server, key, pad string) string {
	resp, err := node.Insert(InsertRequest{Database: "app", Bucket: "events", Key: key, Data: map[string]interface{}{"pad": pad}})
	switch {
	case err != nil:
		return err.Error()
	case !resp.Success:
		return resp.Message
	default:
		return ""
	}
}
