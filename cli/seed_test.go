package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/seed/model"
)

// seedLogin is the connection flags of the test host.
func seedLogin(addr string) []string {
	return []string{"--address", addr, "--user", "root", "--password=live-test-password", "--insecure"}
}

// runSeedForTest runs the verb the way main does, with its own streams.
func runSeedForTest(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	code = runSeed(ctx, args, seedEnv{stdout: &out, stderr: &errOut})
	return code, out.String(), errOut.String()
}

func TestSeedIsAVerbAndItsUsageErrorsNameTheFix(t *testing.T) {
	if _, isVerb := verbRunners["seed"]; !isVerb {
		t.Fatal("seed must be dispatched as a verb")
	}
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"no layout":      {nil, "Example: hoarddb seed agent"},
		"two layouts":    {[]string{"agent", "chat"}, "exactly one layout"},
		"unknown layout": {[]string{"blog"}, "social, chat, ledger, agent or video"},
		"bad flag":       {[]string{"--nope", "agent"}, "hoarddb seed --help"},
	} {
		code, _, stderr := runSeedForTest(t, test.args...)
		if code != ExitUsage || !strings.Contains(stderr, test.want) {
			t.Errorf("%s: exit %d, stderr %q; want exit %d naming %q", name, code, stderr, ExitUsage, test.want)
		}
	}
}

func TestSeedHelpShowsARealExample(t *testing.T) {
	code, stdout, _ := runSeedForTest(t, "--help")
	if code != ExitOK {
		t.Fatalf("--help exited %d", code)
	}
	for _, want := range []string{"hoarddb seed agent --insecure", "scenarios_<layout>", "-database", "-fingerprint"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("--help lacks %q:\n%s", want, stdout)
		}
	}
}

// seededCounts is what the model says a layout holds, as the verb reports it.
func seededCounts(t *testing.T, layout model.Layout) (entities, elements int64) {
	t.Helper()
	gen, err := model.New(layout, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for name, count := range gen.Counts() {
		if strings.Contains(name, ".") {
			elements += count
		} else {
			entities += count
		}
	}
	return entities, elements
}

func TestSeedLoadsALayoutAndRefusesASecondRunWithTheWayOut(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	code, stdout, stderr := runSeedForTest(t, append([]string{"ledger", "--json"}, seedLogin(addr)...)...)
	if code != ExitOK {
		t.Fatalf("seed ledger exited %d:\n%s", code, stderr)
	}
	entities, elements := seededCounts(t, model.Ledger)
	var report seedReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &report); err != nil || strings.Count(stdout, "\n") != 1 {
		t.Fatalf("stdout must be one JSON object: %q (%v)", stdout, err)
	}
	if report.Layout != "ledger" || report.Database != "scenarios_ledger" || report.Entities != entities || report.Elements != elements {
		t.Errorf("report %+v, want layout ledger in scenarios_ledger with %d entities and %d elements", report, entities, elements)
	}
	if !strings.Contains(stderr, "Seeded scenarios_ledger") || strings.Contains(stderr, "/s") {
		t.Errorf("the summary must say what was seeded and print no rate:\n%s", stderr)
	}
	got, err := client.Bucket("scenarios_ledger", "Account").Count(ctx, nil)
	if err != nil || got == 0 {
		t.Fatalf("scenarios_ledger.Account holds %d (error %v)", got, err)
	}

	code, _, stderr = runSeedForTest(t, append([]string{"ledger"}, seedLogin(addr)...)...)
	if code != ExitRefused {
		t.Fatalf("a second run exited %d, want %d:\n%s", code, ExitRefused, stderr)
	}
	for _, want := range []string{"Nothing was written", "drop bucket Account in scenarios_ledger", "--database scenarios2"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal lacks %q:\n%s", want, stderr)
		}
	}
}

// TestSeedAllWritesNothingWhenOneLayoutIsRefused is the pre-flight: a refusal found at the fifth
// layout must not leave the first four half there.
func TestSeedAllWritesNothingWhenOneLayoutIsRefused(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if code, _, stderr := runSeedForTest(t, append([]string{"video"}, seedLogin(addr)...)...); code != ExitOK {
		t.Fatalf("seed video exited %d:\n%s", code, stderr)
	}
	code, _, stderr := runSeedForTest(t, append([]string{"all"}, seedLogin(addr)...)...)
	if code != ExitRefused || !strings.Contains(stderr, "scenarios_video") {
		t.Fatalf("seed all exited %d, want the refusal naming scenarios_video:\n%s", code, stderr)
	}
	_, err := client.DescribeDatabase(ctx, "scenarios_social", ringclient.PageRequest{Limit: 1})
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeNotFound {
		t.Errorf("scenarios_social must not exist after a refused seed all: %v", err)
	}
}

func TestSeedDatabaseFlagNamesTheDatabaseAndRefusesWithoutCredentials(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if code, _, stderr := runSeedForTest(t, append([]string{"video", "--database", "demo"}, seedLogin(addr)...)...); code != ExitOK {
		t.Fatalf("seed video --database demo exited %d:\n%s", code, stderr)
	}
	if got, err := client.Bucket("demo_video", "Video").Count(ctx, nil); err != nil || got == 0 {
		t.Fatalf("demo_video.Video holds %d (error %v)", got, err)
	}
	t.Setenv("HOARDDB_ROOT_PASSWORD", "")
	t.Setenv("HOARDDB_PASSWORD", "")
	code, _, stderr := runSeedForTest(t, "chat", "--database", "nopass", "--address", addr, "--insecure", "--user", "root")
	if code != ExitUsage || !strings.Contains(strings.ToLower(stderr), "password") {
		t.Errorf("seed without a password exited %d, want %d naming the password:\n%s", code, ExitUsage, stderr)
	}
}
