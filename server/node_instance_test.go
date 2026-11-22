package server

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// nonceOf is a distinct instance nonce per letter.
func nonceOf(letter byte) []byte {
	return bytes.Repeat([]byte{letter}, instanceNonceBytes)
}

// TestTwoInterleavingInstancesAreADuplicateUntilOneStops is the rule the
// tracker applies (BUG-0600): two nonces under one id that keep coming back
// for duplicateMinSpan are two processes, and the mark clears
// duplicateLiveWindow after the last evidence.
func TestTwoInterleavingInstancesAreADuplicateUntilOneStops(t *testing.T) {
	tracker := instanceTracker{selfID: "n1", own: string(nonceOf('s'))}
	start := time.Unix(1_000_000, 0)
	sightings := []struct {
		nonce byte
		after time.Duration
	}{
		{'a', 0}, {'b', time.Second}, {'a', 2 * time.Second}, {'b', 3 * time.Second},
	}
	for _, sighting := range sightings {
		if detected := tracker.observe("n2", nonceOf(sighting.nonce), "in gossip from 192.0.2.2", start.Add(sighting.after)); detected != nil {
			t.Fatalf("detected after %s, before the interleaving lasted %s", sighting.after, duplicateMinSpan)
		}
	}
	last := start.Add(2*time.Second + duplicateMinSpan)
	detected := tracker.observe("n2", nonceOf('a'), "in gossip from 192.0.2.2", last)
	if detected == nil {
		t.Fatalf("a and b interleaved for %s under n2 and nothing was detected", duplicateMinSpan)
	}
	if again := tracker.observe("n2", nonceOf('b'), "in gossip from 192.0.2.9", last.Add(time.Second)); again != nil {
		t.Fatal("one live duplicate was detected twice")
	}
	if !tracker.liveDuplicate("n2", last.Add(duplicateLiveWindow)) {
		t.Fatal("the duplicate cleared within the live window")
	}
	notes := tracker.notes(last.Add(time.Second))
	if len(notes) != 1 || !strings.Contains(notes[0], "n2") || !strings.Contains(notes[0], "HOARDDB_NODE_ID") {
		t.Fatalf("status notes %q, want one naming n2 and the fix", notes)
	}
	// One process stops: only a is seen from now on.
	for after := 2 * time.Second; after < 2*duplicateLiveWindow; after += 20 * time.Second {
		tracker.observe("n2", nonceOf('a'), "answering at 192.0.2.2:4433", last.Add(after))
	}
	if tracker.liveDuplicate("n2", last.Add(2*duplicateLiveWindow)) {
		t.Fatal("the duplicate stayed live after one process stopped")
	}
}

// TestARestartsLateMessagesAreNotADuplicate: a restart shows a new nonce, and a
// message from the old process can arrive just after the new one's first. That
// is a restart, however often it happens.
func TestARestartsLateMessagesAreNotADuplicate(t *testing.T) {
	tracker := instanceTracker{selfID: "n1", own: string(nonceOf('s'))}
	at := time.Unix(1_000_000, 0)
	for _, process := range []byte{'a', 'b', 'c', 'd', 'e', 'f'} {
		previous := process - 1
		for step := 0; step < 3; step++ {
			at = at.Add(300 * time.Millisecond)
			if detected := tracker.observe("n2", nonceOf(process), "in gossip from 192.0.2.2", at); detected != nil {
				t.Fatalf("restart to %c reported as a duplicate", process)
			}
			if process != 'a' && step < 2 {
				late := at.Add(5 * time.Millisecond)
				if detected := tracker.observe("n2", nonceOf(previous), "answering at 192.0.2.2:4433", late); detected != nil {
					t.Fatalf("a late message from %c after the restart to %c reported as a duplicate", previous, process)
				}
			}
		}
	}
	if tracker.liveDuplicate("n2", at) {
		t.Fatal("six quick restarts left n2 marked a duplicate")
	}
}

// TestAnotherInstanceUnderThisNodesIDIsADuplicateAtOnce: this node's own nonce
// is itself; any other under its id is a live process, since this process's
// previous incarnation stopped before it started.
func TestAnotherInstanceUnderThisNodesIDIsADuplicateAtOnce(t *testing.T) {
	tracker := instanceTracker{selfID: "n2", own: string(nonceOf('s'))}
	at := time.Unix(1_000_000, 0)
	if detected := tracker.observe("n2", nonceOf('s'), "answering at 192.0.2.2:4433", at); detected != nil {
		t.Fatal("this process's own answer was reported as a duplicate")
	}
	if tracker.liveDuplicate("n2", at) {
		t.Fatal("this process alone is marked a duplicate")
	}
	if detected := tracker.observe("n2", nonceOf('c'), "answering at 192.0.2.2:4433", at); detected == nil {
		t.Fatal("another process under this node's id was not detected")
	}
	if !tracker.liveDuplicate("n2", at.Add(duplicateLiveWindow)) {
		t.Fatal("the duplicate of this node cleared within the live window")
	}
	if tracker.liveDuplicate("n2", at.Add(duplicateLiveWindow+time.Second)) {
		t.Fatal("the duplicate of this node stayed live with no new evidence")
	}
	reported := at.Add(2 * duplicateLiveWindow)
	if !tracker.noteReport("n3", reported) || tracker.noteReport("n1", reported) {
		t.Fatal("a member's report is fresh only when no member had reported within the window")
	}
	notes := tracker.notes(reported)
	if len(notes) != 1 || !strings.Contains(notes[0], "members n1, n3 see") {
		t.Fatalf("status notes %q, want one naming the reporting members", notes)
	}
}
