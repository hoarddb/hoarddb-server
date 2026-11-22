package main

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// startNode runs one real ring node in process and returns its address; the same way the
// ringclient live tests start one, so no harness is linked. The service takes its driver
// address as a string, so the port is picked and released before the service binds it, and
// another process can take it in between: a start refused for exactly that is tried again
// on a fresh port and a fresh data directory, a bounded number of times.
func startNode(t *testing.T) string {
	t.Helper()
	const starts = 5
	t.Setenv("FAULTCTL_PASSWORD", "faultctl-test-password")
	for try := 1; ; try++ {
		addr, err := startNodeOnce(t)
		if err == nil {
			return addr
		}
		if !errors.Is(err, syscall.EADDRINUSE) || try == starts {
			t.Fatalf("starting a node (try %d of %d): %v", try, starts, err)
		}
	}
}

// startNodeOnce is one try of startNode: an error is the service's refusal to start.
func startNodeOnce(t *testing.T) (string, error) {
	t.Helper()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = addr
	service, err := server.NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the start, so a service that failed to start is stopped too.
	t.Cleanup(func() { _ = service.Stop() })
	if err := service.SeedRootUser("root", "faultctl-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(); err != nil {
		return "", err
	}
	return addr, nil
}

// writeCSV runs the two-client write load of seed against addr for duration and returns its CSV.
func writeCSV(t *testing.T, addr string, seed int, duration time.Duration) []byte {
	t.Helper()
	var out bytes.Buffer
	args := []string{"--addr", addr, "--insecure", "--create", "--clients", "2", "--rate", "20", "--duration", duration.String(), "--seed", strconv.Itoa(seed)}
	if err := runLoad("write", args, &out); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func runWrite(t *testing.T, addr string, seed int, duration time.Duration) []attempt {
	t.Helper()
	rows, err := readAttempts(bytes.NewReader(writeCSV(t, addr, seed, duration)))
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// idsByClient is the ids each client attempted, in the order it attempted them.
func idsByClient(rows []attempt) map[string][]string {
	ids := map[string][]string{}
	for _, row := range rows {
		ids[row.Client] = append(ids[row.Client], row.ID)
	}
	return ids
}

// writeAtLeast runs the two-client write load of seed against a fresh node until both clients
// have made floor attempts each, and returns their ids. The load's clock starts before its
// clients connect, so on a busy host a one-second window can close after a single attempt:
// a short run is run again on a new node for three times as long (1 s, 3 s, 9 s, 27 s), and
// a load still short after the longest fails the test.
func writeAtLeast(t *testing.T, seed, floor int) map[string][]string {
	t.Helper()
	const longest = 27 * time.Second
	for duration := time.Second; ; duration *= 3 {
		ids := idsByClient(runWrite(t, startNode(t), seed, duration))
		if len(ids["c0"]) >= floor && len(ids["c1"]) >= floor {
			return ids
		}
		if duration >= longest {
			t.Fatalf("clients made %d and %d attempts in %s at 20/s, want %d each", len(ids["c0"]), len(ids["c1"]), duration, floor)
		}
	}
}

// Break: seed the stream from the clock, or from the client number alone.
func TestWriteStreamsAreReproducibleBySeed(t *testing.T) {
	first, second, other := newWriteStream(7, 1), newWriteStream(7, 1), newWriteStream(8, 1)
	differs := false
	for i := 0; i < 50; i++ {
		a, b, c := first.take(), second.take(), other.take()
		if a != b {
			t.Fatalf("write %d differs between two streams of one seed: %+v vs %+v", i, a, b)
		}
		differs = differs || a.Pad != c.Pad
	}
	if !differs {
		t.Fatal("seeds 7 and 8 produced the same bodies: the seed is not used")
	}
	if newWriteStream(7, 1).take().Pad == newWriteStream(7, 2).take().Pad {
		t.Fatal("two clients of one seed share a stream")
	}

	one, two := writeAtLeast(t, 7, 5), writeAtLeast(t, 7, 5)
	if len(one) != 2 || len(two) != 2 {
		t.Fatalf("two clients wrote, the rows name %d and %d", len(one), len(two))
	}
	for client, ids := range one {
		for i := 0; i < min(len(ids), len(two[client])); i++ {
			if ids[i] != two[client][i] || ids[i] != client+"-"+strconv.Itoa(i) {
				t.Fatalf("client %s attempt %d: %q vs %q", client, i, ids[i], two[client][i])
			}
		}
	}
}

// writeApplying runs the two-client write load of seed against a fresh node until the write
// of id was applied, and returns the node's address and the load's CSV. The corrupted-CSV
// half of the round trip deletes the read-back of that one write, so it must have been
// acknowledged; a first attempt can run out its deadline on a busy host. A load without it
// is run again on a new node for three times as long (1 s, 3 s, 9 s, 27 s), and one still
// without it after the longest fails the test.
func writeApplying(t *testing.T, seed int, id string) (addr string, written []byte) {
	t.Helper()
	const longest = 27 * time.Second
	for duration := time.Second; ; duration *= 3 {
		addr = startNode(t)
		written = writeCSV(t, addr, seed, duration)
		rows, err := readAttempts(bytes.NewReader(written))
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.ID == id && (row.Outcome == "applied" || row.Outcome == "applied_degraded") {
				return addr, written
			}
		}
		if duration >= longest {
			t.Fatalf("the write of %s was not applied in %s at 20/s (%d attempts made)", id, duration, len(rows))
		}
	}
}

// Break: print the read rows with the seq column zeroed, or skip NOT_FOUND rows.
func TestTheCSVRoundTripsThroughVerify(t *testing.T) {
	addr, written := writeApplying(t, 3, "c0-0")
	dir := t.TempDir()
	acks, obs := filepath.Join(dir, "acks.csv"), filepath.Join(dir, "obs.csv")
	if err := os.WriteFile(acks, written, 0o600); err != nil {
		t.Fatal(err)
	}
	var read bytes.Buffer
	if err := runLoad("read", []string{"--addr", addr, "--insecure", "--ids", acks, "--passes", "2", "--clients", "2"}, &read); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(obs, read.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	var verdicts bytes.Buffer
	if err := runVerify([]string{"--acks", acks, "--obs", obs}, &verdicts); err != nil {
		t.Fatalf("a clean single-node run must verify: %v\n%s", err, verdicts.String())
	}
	if !strings.Contains(verdicts.String(), "all 5 invariants hold") {
		t.Fatalf("missing the verdict block: %s", verdicts.String())
	}

	// The hand-corrupted CSV: delete one acked document's read rows, as a lost write would look.
	corrupted := filepath.Join(dir, "obs-corrupt.csv")
	var kept []string
	dropped := 0
	for _, line := range strings.Split(strings.TrimSpace(read.String()), "\n") {
		if strings.HasPrefix(line, "c0,c0-0,") && dropped < 2 {
			dropped++
			continue
		}
		kept = append(kept, line)
	}
	if dropped != 2 {
		t.Fatalf("expected two read rows of c0-0 to drop, found %d", dropped)
	}
	if err := os.WriteFile(corrupted, []byte(strings.Join(kept, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	verdicts.Reset()
	if err := runVerify([]string{"--acks", acks, "--obs", corrupted}, &verdicts); err == nil || !strings.Contains(verdicts.String(), "c0-0 (never read back)") {
		t.Fatalf("a CSV without the read-back of c0-0 must fail naming it, got %v\n%s", err, verdicts.String())
	}
}
