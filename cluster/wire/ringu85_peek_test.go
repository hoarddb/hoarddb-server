package wire

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestU85PeekBudgetChargesKeysAndFraming: a heap peek reply carries each entry id and priority as
// well as the element, so the reader's budget must charge them, or a peek the reader passes is
// refused when the reply is encoded. 100 elements of ~334 KB with 1024-byte entry ids sit between
// the element-only budget and the real one. [GUARD the key and framing charge in peekElements]
func TestU85PeekBudgetChargesKeysAndFraming(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 33 MB")
	}
	node, _ := u63Fresh(t)
	ctx := context.Background()
	big := strings.Repeat("x", 334_000)
	for i := range 100 {
		req := PushRequest{StoreTarget: u62Target("Ranked"), Element: u63Item(big), Priority: int64(1000 - i), EntryID: fmt.Sprintf("%01024d", i)}
		if err := node.Push(ctx, req); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}
	got, err := node.Peek(ctx, PeekRequest{StoreTarget: u62Target("Ranked"), Count: 90})
	if err != nil || len(got) != 90 {
		t.Fatalf("Peek of 90 = %d elements, %v, want them all", len(got), err)
	}
	_, err = node.Peek(ctx, PeekRequest{StoreTarget: u62Target("Ranked"), Count: 100})
	if err == nil || !strings.Contains(err.Error(), "one reply carries") || !strings.Contains(err.Error(), "Peek fewer") {
		t.Fatalf("Peek of 100 = %v, want the reader's own budget refusal naming how to fix it", err)
	}
}
