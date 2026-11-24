package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestDumpRefusesWhenANodeHoldsEarlierStoreData (review F5, owner ruling 2026-11-23): a node
// also holds the earlier single-node store, which the earlier driver interface writes and no ring
// request reads. v1.1.0's dump wrote the ring's databases, exited 0 and said nothing of that data,
// and on a node with no ring database it said only that the ring is empty. The dump must refuse,
// name the node and the flag; with --leave-legacy-store-data it completes and says, at its start
// and its end, that the data is not in the file.
func TestDumpRefusesWhenANodeHoldsEarlierStoreData(t *testing.T) {
	session := newGoldenSession(t)
	runStatements(t, session.cli, session.out, "create database legacyapp", "use legacyapp", "create bucket Users { type: hash }")
	login := []string{"-address", session.addr, "-user", "admin", "-password=" + goldenPassword, "-insecure"}
	const said = "also holds 1 bucket in its earlier single-node store"

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: session.addr, User: "admin", Password: goldenPassword, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	dumpTestDatabase(t, client, "app")

	refused := filepath.Join(t.TempDir(), "refused.hoarddump")
	code, stdout, stderr := runRingDumpForTest(t, append([]string{"--out", refused}, login...)...)
	if code != ExitRefused || !strings.Contains(stderr, said) || !strings.Contains(stderr, "--leave-legacy-store-data") || !strings.Contains(stderr, "dump and restore do not carry it") {
		t.Errorf("a dump of a node whose earlier store holds a bucket: exit %d, want %d, the node's line and the flag\nstdout: %s\nstderr: %s", code, ExitRefused, stdout, stderr)
	}
	requireNoDumpFile(t, refused)

	file := filepath.Join(t.TempDir(), "app.hoarddump")
	code, stdout, stderr = runRingDumpForTest(t, append([]string{"--out", file, "--json", "--leave-legacy-store-data"}, login...)...)
	if code != ExitOK {
		t.Fatalf("dump --leave-legacy-store-data: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if strings.Count(stderr, said) != 2 || !strings.Contains(stderr, "NOT in this file") {
		t.Errorf("the dump does not say, at its start and its end, that the earlier store's bucket is not in the file:\n%s", stderr)
	}
	if !strings.Contains(stdout, `"earlier_store_left_out":["Node `) {
		t.Errorf("the --json object does not carry the earlier store's line:\n%s", stdout)
	}
}
