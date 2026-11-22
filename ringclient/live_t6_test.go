package ringclient_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestLiveASingleNodeHasNoSeedingToShowOrRestart: the seeding commands against the real
// server. A node with no seeding engine at work answers an empty status, and retry and forget
// are SEED_NOT_HELD with the text that names the fix.
func TestLiveASingleNodeHasNoSeedingToShowOrRestart(t *testing.T) {
	addr := startLiveServer(t)
	ctx := liveContext(t)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: "root", Password: "live-test-password", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	page, err := client.SeedingStatus(ctx, ringclient.SeedingQuery{})
	if err != nil || len(page.Rows) != 0 || page.More {
		t.Fatalf("SeedingStatus = %+v, %v, want an empty page", page, err)
	}
	for name, call := range map[string]func() error{
		"retry":  func() error { return client.SeedingRetry(ctx, "app", 5) },
		"forget": func() error { return client.SeedingForget(ctx, "app", 5, false) },
	} {
		var refusal *ringclient.Error
		if err := call(); !errors.As(err, &refusal) || refusal.Code != cluster.CodeSeedNotHeld ||
			!strings.Contains(refusal.Message, "status seeding") || !strings.Contains(refusal.Message, `slot 5 of database "app"`) {
			t.Errorf("%s of a slot with no seed = %v", name, err)
		}
	}
}
