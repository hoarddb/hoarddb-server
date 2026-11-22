package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
)

// ringAtRestMarker is a value no other test writes, so finding it in a file means a document
// reached that file in the clear.
const ringAtRestMarker = "RING-AT-REST-MARKER-7c41e9a2d05b3f68"

const (
	ringScanHelperDirEnv  = "HOARDDB_RING_SCAN_HELPER_DIR"
	ringScanHelperModeEnv = "HOARDDB_RING_SCAN_HELPER_MODE"
	ringScanHelperKeyEnv  = "HOARDDB_RING_SCAN_HELPER_KEY"
	ringScanHelperReady   = "RING-SCAN-HELPER-READY"
)

// writeRingMarker creates a database and a bucket on the running ring node and inserts a
// document carrying the marker. The insert is acknowledged, so its log record is durable.
func writeRingMarker(t testing.TB, qs *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := qs.ringNode.CreateDatabase(ctx, "scan"); err != nil {
		t.Fatalf("creating the database: %v", err)
	}
	if _, err := qs.ringNode.CreateBucket(ctx, "scan", "User", wire.Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	if _, err := qs.ringNode.Insert(ctx, "scan", "User", bson.D{{Key: "_id_", Value: "m1"}, {Key: "Name", Value: ringAtRestMarker}}); err != nil {
		t.Fatalf("inserting the marker: %v", err)
	}
}

// markerFilesUnder lists the files under dir whose names end in suffix (any file for "") that
// hold the marker. A directory that does not exist holds none.
func markerFilesUnder(t testing.TB, dir, suffix string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, fs.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, suffix) {
			return nil
		}
		content, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if bytes.Contains(content, []byte(ringAtRestMarker)) {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanning %s: %v", dir, err)
	}
	return found
}

// copyRingWhileRunning copies every file under src into dst as it is now; a file the running
// node removes during the copy is skipped.
func copyRingWhileRunning(t testing.TB, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, fs.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		content, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return os.WriteFile(target, content, 0o600)
	})
	if err != nil {
		t.Fatalf("copying %s while the node runs: %v", src, err)
	}
}

// checkRingScan asserts what a leg's scan of ringDir must show: no marker in the store or the
// log of a sealed node, and the marker in each of the two for the plaintext control.
func checkRingScan(t *testing.T, leg, mode, ringDir, storeSuffix string) {
	t.Helper()
	storeHits := markerFilesUnder(t, filepath.Join(ringDir, "badger"), storeSuffix)
	logHits := markerFilesUnder(t, filepath.Join(ringDir, "wal"), "")
	if modeSealsStore(mode) {
		if len(storeHits) > 0 {
			t.Errorf("%s leg, mode %s: the marker is in the clear in the ring store: %v", leg, mode, storeHits)
		}
		if len(logHits) > 0 {
			t.Errorf("%s leg, mode %s: the marker is in the clear in the ring log: %v", leg, mode, logHits)
		}
		return
	}
	if len(storeHits) == 0 {
		t.Errorf("%s leg, plaintext control: no marker in the ring store (%s files), so this leg cannot see a leak there", leg, storeSuffix)
	}
	if len(logHits) == 0 {
		t.Errorf("%s leg, plaintext control: no marker in the ring log, so this leg cannot see a leak there", leg)
	}
}

// TestARingNodeWithEncryptionOnLeavesNoPlaintextMarkerInItsStoreOrLog: [GUARD a sealed ring node
// writes a document into neither ring/badger nor ring/wal in the clear, while running (copied
// live, memtable logs included), after its process is killed, and after Close; each leg's
// plaintext control finds the marker in the store and in the log separately; red when AtRest is
// not passed to the store, and separately to the log]. Ring encryption at rest spec §9.1 step 3.
func TestARingNodeWithEncryptionOnLeavesNoPlaintextMarkerInItsStoreOrLog(t *testing.T) {
	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			keyPath := ringKEKFile(t, ringFirstKEK)
			edit := func(cfg *config.Config) {
				cfg.Encryption.Keystore = "keyfile"
				cfg.Encryption.KeyFile = keyPath
				cfg.Encryption.Mode = mode
			}

			dataDir := t.TempDir()
			qs, err := ringStartAt(t, dataDir, edit)
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			writeRingMarker(t, qs)
			live := filepath.Join(t.TempDir(), "ring")
			copyRingWhileRunning(t, filepath.Join(dataDir, ringDirName), live)
			checkRingScan(t, "open", mode, live, "")
			if err := qs.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			// Close flushes the memtable, so the control finds the document in a table file.
			checkRingScan(t, "closed", mode, filepath.Join(dataDir, ringDirName), ".sst")

			killedDir := t.TempDir()
			runKilledRingScanHelper(t, killedDir, mode, keyPath)
			checkRingScan(t, "killed", mode, filepath.Join(killedDir, ringDirName), "")
		})
	}
}

// runKilledRingScanHelper starts a node in a child process over dataDir, waits until it has
// written the marker, and kills it.
func runKilledRingScanHelper(t *testing.T, dataDir, mode, keyPath string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRingAtRestScanHelperProcess$", "-test.v")
	cmd.Env = append(os.Environ(), ringScanHelperDirEnv+"="+dataDir, ringScanHelperModeEnv+"="+mode, ringScanHelperKeyEnv+"="+keyPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the helper: %v", err)
	}
	lines := bufio.NewScanner(stdout)
	ready := false
	var seen strings.Builder
	for lines.Scan() {
		seen.WriteString(lines.Text() + "\n")
		if strings.Contains(lines.Text(), ringScanHelperReady) {
			ready = true
			break
		}
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("killing the helper: %v", err)
	}
	if _, err := io.Copy(io.Discard, stdout); err != nil {
		t.Logf("draining the helper's output: %v", err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("the helper exited cleanly; it should have been killed")
	}
	if !ready {
		t.Fatalf("the helper never wrote the marker\nstdout:\n%s\nstderr:\n%s", seen.String(), stderr.String())
	}
}

// TestRingAtRestScanHelperProcess is the child of the killed leg: it starts a node, writes the
// marker, says so, and waits to be killed. It does nothing in a normal run.
func TestRingAtRestScanHelperProcess(t *testing.T) {
	dataDir := os.Getenv(ringScanHelperDirEnv)
	if dataDir == "" {
		t.Skip("the child process of TestARingNodeWithEncryptionOnLeavesNoPlaintextMarkerInItsStoreOrLog")
	}
	qs, err := ringStartAt(t, dataDir, func(cfg *config.Config) {
		cfg.Encryption.Keystore = "keyfile"
		cfg.Encryption.KeyFile = os.Getenv(ringScanHelperKeyEnv)
		cfg.Encryption.Mode = os.Getenv(ringScanHelperModeEnv)
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	writeRingMarker(t, qs)
	if _, err := os.Stdout.WriteString(ringScanHelperReady + "\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Minute)
	t.Fatal("the helper was not killed")
}
