package server

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestU76DenialTrackerIsBoundedUnderDistinctResources: [GUARD 10000 distinct resources, each
// up to 1 KB, leave at most 4096 entries of at most 256 key bytes each: about 1.5 MB].
func TestU76DenialTrackerIsBoundedUnderDistinctResources(t *testing.T) {
	var tracker denialTracker
	now := time.Now()
	longTail := strings.Repeat("x", 1000)
	for i := 0; i < 10000; i++ {
		key := denialKey{User: "reader", Action: ActionRead, Resource: fmt.Sprintf("%05d%s", i, longTail)}
		if emit, _ := tracker.shouldEmit(key, now); !emit {
			t.Fatalf("a first denial of resource %d was suppressed", i)
		}
	}
	if got := len(tracker.entries); got > 4096 {
		t.Fatalf("the tracker holds %d entries after 10000 distinct resources, want at most 4096", got)
	}
	keyBytes := 0
	for key := range tracker.entries {
		keyBytes += len(key.User) + len(key.Resource)
	}
	if keyBytes > 4096*256 {
		t.Fatalf("the tracker holds %d key bytes, want at most %d", keyBytes, 4096*256)
	}
}

// TestU76AHotDenialStaysDeduplicatedUnderAFlood: [GUARD a key denied again inside the
// interval is still suppressed after 10000 other resources passed through].
func TestU76AHotDenialStaysDeduplicatedUnderAFlood(t *testing.T) {
	var tracker denialTracker
	now := time.Now()
	hot := denialKey{User: "alice", Action: ActionWrite, Resource: "app"}
	tracker.shouldEmit(hot, now)
	tracker.shouldEmit(hot, now)
	for i := 0; i < 10000; i++ {
		tracker.shouldEmit(denialKey{User: "alice", Action: ActionRead, Resource: fmt.Sprintf("flood-%d", i)}, now)
	}
	if emit, _ := tracker.shouldEmit(hot, now.Add(time.Second)); emit {
		t.Fatal("the hot key was reported again inside its interval after a flood of other resources")
	}
}
