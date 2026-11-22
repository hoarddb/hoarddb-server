package wire

import (
	"context"
	"testing"
)

// TestU62StoreVerbsReplayWithAColdDeclarationCache: a node whose Badger is lost replays
// pushes and claims from the log with an empty declaration cache. The applier must read the
// declaration it needs from the replayed system bucket, not refuse the record.
func TestU62StoreVerbsReplayWithAColdDeclarationCache(t *testing.T) {
	dir := t.TempDir()
	node, err := OpenNode(context.Background(), NodeConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	u62Schema(t, node)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b")
	if _, err := node.Claim(ctx, ClaimRequest{StoreTarget: u62Target("Queue"), Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	loseStoreForTheReplay(t, dir)
	reopened := u30iOpen(t, dir)
	doc, err := reopened.Get(ctx, u30iDB, "Box", "b1")
	if err != nil {
		t.Fatal(err)
	}
	if got := u30iString(t, doc, "Queue"); got != "fifoStore (1 element)" {
		t.Fatalf("Queue after the rebuild = %q, want one element left after a claim of one", got)
	}
}
