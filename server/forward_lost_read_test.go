package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestALostForwardedReadSaysNothingChangedAndHowToRecover is the read half of
// the lost-forward message. A read carries no write, so the write advice ("a
// write may have been applied there, so read it back before retrying a push or
// an $inc") is false for it, and the old message named no way out. A lost
// forwarded read must say the owner did not answer, that nothing changed, and
// name the retry and the 'nodes' command, as the LIST_BUCKETS refusal does.
//
// Guard: drop the read branch from forwardLostAdvice and the message tells a
// GET's caller to read back a write.
func TestALostForwardedReadSaysNothingChangedAndHowToRecover(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	owner := ownerIndexOf(t, tc, testBucket)
	coordinator := (owner + 1) % len(tc.nodes)
	putDocument(tc, coordinator, testDatabase, testBucket, "k")

	// The owner stops: the coordinator's forward to it cannot be answered.
	stopNode(t, tc, owner)
	ownerAddr := tc.addrs[owner]

	refused := refusalOf(t, tc.frame(coordinator, transport.OpGet, FindRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k",
	}))
	if refused.Code != int(transport.ErrUnavailable) {
		t.Errorf("the lost read is code %d, want UNAVAILABLE (%d): %q", refused.Code, transport.ErrUnavailable, refused.Message)
	}
	if refused.WriteOutcome != "" {
		t.Errorf("the lost read is write_outcome %q, want none: a read stores nothing: %q", refused.WriteOutcome, refused.Message)
	}
	for _, want := range []string{
		"GET",
		ownerAddr,
		"not answered",
		"nothing changed",
		"Retry in a moment",
		"run 'nodes'",
	} {
		if !strings.Contains(refused.Message, want) {
			t.Errorf("the lost read's message does not say %q: %q", want, refused.Message)
		}
	}
	for _, unwanted := range []string{"may have been applied", "$inc", "read it back", "may be lost"} {
		if strings.Contains(refused.Message, unwanted) {
			t.Errorf("the lost read's message still carries the write advice %q: %q", unwanted, refused.Message)
		}
	}
}

// TestALostForwardedRequestTakesTheReadOrTheWriteAdviceByWhatItStores pins the
// read/write split to what an opcode stores, not to how it is routed.
// Restoration travels as a forwarded read (no ReplEntry), but each document it
// carries is applied by the owner through the ordinary write path, so a lost
// RESTORE_CHUNK or RESTORE_DONE may have changed the bucket: "nothing changed"
// is false for them and they must keep the write advice. A dump chunk is
// routed the same way and stores nothing, so it takes the read advice.
//
// Guard: split the advice on replicationRequired and the restore rows fail.
func TestALostForwardedRequestTakesTheReadOrTheWriteAdviceByWhatItStores(t *testing.T) {
	cases := []struct {
		name string
		op   transport.Opcode
		read bool
	}{
		{"get", transport.OpGet, true},
		{"scan", transport.OpScan, true},
		{"count", transport.OpCount, true},
		{"fifo peek", transport.OpFIFOPeek, true},
		{"dump start", transport.OpDumpStart, true},
		{"dump chunk", transport.OpDumpChunk, true},
		{"dump done", transport.OpDumpDone, true},
		{"put", transport.OpPut, false},
		{"update", transport.OpUpdate, false},
		{"restore start", transport.OpRestoreStart, false},
		{"restore chunk", transport.OpRestoreChunk, false},
		{"restore done", transport.OpRestoreDone, false},
	}
	payload := mustMarshalBSON(t, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			advice := forwardLostAdvice(c.op, testDatabase, testBucket, payload)
			saysNothingChanged := strings.Contains(advice, "nothing changed")
			if saysNothingChanged != c.read {
				t.Errorf("%s: advice says nothing changed = %v, want %v: %q",
					c.op, saysNothingChanged, c.read, advice)
			}
			if wrote := strings.Contains(advice, "may have been applied there"); wrote == c.read {
				t.Errorf("%s: advice says a write may have been applied there = %v, want %v: %q",
					c.op, wrote, !c.read, advice)
			}
			if c.read && !strings.Contains(advice, "Retry in a moment") {
				t.Errorf("%s: read advice names no retry: %q", c.op, advice)
			}
			if c.read && !strings.Contains(advice, "run 'nodes'") {
				t.Errorf("%s: read advice does not name the 'nodes' command: %q", c.op, advice)
			}
		})
	}
}
