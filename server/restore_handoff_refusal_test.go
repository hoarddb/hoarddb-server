package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestARestoreStartAtANodeMidHandOffAnswersUnavailableWithTheRetrySentence is
// BUG-0665: a restore start that reaches the node ownership of the bucket is
// moving to has its create refused for the hand-off, and the restore path
// flattened that refusal into a plain error, so the wire carried INTERNAL, the
// class that tells a driver to report a bug. The refusal's own class is
// UNAVAILABLE with "Retry in a moment", and the connection serves the next
// request. Guard: frameError returning errors.New(message) for an error frame.
func TestARestoreStartAtANodeMidHandOffAnswersUnavailableWithTheRetrySentence(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "restoring", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	// The pull is registered once the restore's own create is past the join
	// fence, after the restore start's check for a copy it has not made: the
	// hold a hand-off puts on a create, which then waits out the write-concern
	// timeout and is refused for the hand-off.
	owner := clusterOf(tc, 0)
	var clearPull func()
	holdCreate := func(op transport.Opcode, _, name string) {
		if op == transport.OpCreateBucket && name == bucket && clearPull == nil {
			clearPull = markPullPending(owner, bucket)
		}
	}
	afterJoinFenceHook.Store(&holdCreate)
	defer afterJoinFenceHook.Store(nil)
	defer func() {
		if clearPull != nil {
			clearPull()
		}
	}()

	frame := tc.frame(0, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	refusal := refusalOf(t, frame)
	if !strings.Contains(refusal.Message, "for restore") || !strings.Contains(refusal.Message, "is moving to this node") {
		t.Fatalf("the restore start was refused for another reason than the hand-off in its create: %q", refusal.Message)
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
		t.Errorf("a restore start refused for a hand-off in progress answers code %d, want %d (unavailable): %q",
			refusal.Code, transport.ErrUnavailable, refusal.Message)
	}
	if !strings.Contains(refusal.Message, "Retry in a moment") {
		t.Errorf("a restore start refused for a hand-off in progress reads %q, want the hand-off's retry sentence", refusal.Message)
	}

	next := tc.frame(0, transport.OpListDBs, bson.M{})
	if next.Opcode == transport.OpError {
		t.Fatalf("the request after the refused restore start was refused: %s", answerText(next))
	}
}

// TestARestoreWrapKeepsTheClassOfTheRefusalItWraps is BUG-0665 at every site
// restore wraps a refusal frame (the database create, the bucket create, the
// --drop delete, the store declaration): whatever class the routed operation
// answered with is the class the wrapped error is answered with.
func TestARestoreWrapKeepsTheClassOfTheRefusalItWraps(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want transport.ErrorCode
	}{
		{"a hand-off in progress", ErrHandoffInProgress, transport.ErrUnavailable},
		{"a member whose disk is full", ErrMemberDiskFull, transport.ErrUnavailable},
		{"a database being created twice", ErrDatabaseAlreadyExists, transport.ErrAlreadyExists},
		{"a bucket that is not there", ErrBucketNotFound, transport.ErrNotFound},
		{"a request the server cannot name a class for", errors.New("something broke"), transport.ErrInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusal := errorFrame(&transport.Frame{}, tc.err)
			wrapped := fmt.Errorf("creating bucket %s/%s for restore: %w", testDatabase, testBucket, frameError(refusal))
			if got := codeFor(wrapped); got != tc.want {
				t.Errorf("the refusal %q wrapped by restore is answered with code %d, want %d", tc.err, got, tc.want)
			}
			if !strings.Contains(wrapped.Error(), tc.err.Error()) {
				t.Errorf("the wrapped refusal reads %q, want the refusal's own message %q kept", wrapped, tc.err)
			}
		})
	}
}

// restoreStartCode sends a restore start for a new bucket and returns the
// refusal it was answered with.
func restoreStartRefusal(t *testing.T, tc *testCluster, start RestoreStartRequest) ErrorResponse {
	t.Helper()
	start.FormatVersion, start.Bytes = dumpManifestFormatVersion, 1
	if start.StoreType == "" {
		start.StoreType = BucketTypeHash
	}
	return refusalOf(t, tc.frame(0, transport.OpRestoreStart, start))
}

// TestARestoreStartRefusedForInputTheCallerCanFixAnswersInvalidArgument is
// BUG-0665 for the refusals a restore reads from an ordinary reply or raises
// itself: a bucket name over the limit, and an index name with a control
// character (the create's Success false answer), are the caller's to fix, so
// they answer INVALID_ARGUMENT, not INTERNAL.
func TestARestoreStartRefusedForInputTheCallerCanFixAnswersInvalidArgument(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	for _, tt := range []struct {
		name  string
		start RestoreStartRequest
		said  string
	}{
		{"a bucket name over the limit", RestoreStartRequest{Database: testDatabase, Bucket: strings.Repeat("b", maxNameBytes+1)}, "too long"},
		{"an index name with a control character", RestoreStartRequest{Database: testDatabase, Bucket: "ctl", Indexes: []string{"bad\x01name"}}, "control character"},
		{"a bucket name with a control character", RestoreStartRequest{Database: testDatabase, Bucket: "a\x01b"}, "control character"},
		{"a database name with a control character", RestoreStartRequest{Database: "d\x01b", Bucket: "plain"}, "control character"},
		{"a reserved database name", RestoreStartRequest{Database: authReservedDatabase, Bucket: "plain"}, "reserved"},
		{"an unsupported store type", RestoreStartRequest{Database: testDatabase, Bucket: "plain", StoreType: "graph"}, "unsupported store_type"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			refusal := restoreStartRefusal(t, tc, tt.start)
			if !strings.Contains(refusal.Message, tt.said) {
				t.Fatalf("refused for another reason than %q: %q", tt.said, refusal.Message)
			}
			if transport.ErrorCode(refusal.Code) != transport.ErrInvalidArgument {
				t.Errorf("answers code %d, want %d (invalid argument): %q", refusal.Code, transport.ErrInvalidArgument, refusal.Message)
			}
		})
	}
}

// TestRefusalsTheServerRaisesForACallerOrAMemberHaveTheirOwnClass is BUG-0665
// for the sentinels codeFor had no case for: the replication-factor refusals
// and a restore that does not fit the disk.
func TestRefusalsTheServerRaisesForACallerOrAMemberHaveTheirOwnClass(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want transport.ErrorCode
	}{
		{"a factor on a standalone server", errRFStandalone("b", 3), transport.ErrInvalidArgument},
		{"a factor above the member count", errRFAboveMemberCount("b", 5, 3), transport.ErrInvalidArgument},
		{"a member that cannot honour a factor", errRFPeerCannotHonourIt("b", 3, "n2"), transport.ErrUnavailable},
		{"a restore that does not fit the disk", fmt.Errorf("%w: bucket d/b needs roughly 9 free byte(s)", ErrRestoreInsufficientDiskSpace), transport.ErrUnavailable},
	} {
		if got := codeFor(tt.err); got != tt.want {
			t.Errorf("%s answers code %d, want %d: %v", tt.name, got, tt.want, tt.err)
		}
	}
}

// TestARestoreChunkRefusedForItsSizeAnswersInvalidArgument is BUG-0665 for the
// document writes a restore routes: frameError is also theirs, and the class
// of a refused write reaches the client where INTERNAL did. A dumped document
// over this node's limit is refused as the caller's to fix.
func TestARestoreChunkRefusedForItsSizeAnswersInvalidArgument(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	started := asWireFrame(t, tc.ok(0, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: "sized", FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	}))
	session, _ := started["session_id"].(string)
	raw, err := bson.Marshal(bson.M{"_id": "k1", "text": strings.Repeat("x", tc.nodes[0].server.core.maxDocumentBytes()+1)})
	if err != nil {
		t.Fatalf("encoding the dumped document: %v", err)
	}
	refusal := refusalOf(t, tc.frame(0, transport.OpRestoreChunk, RestoreChunkRequest{Database: testDatabase, Bucket: "sized", SessionID: session, Data: raw}))
	if !strings.Contains(refusal.Message, "limit is") {
		t.Fatalf("the chunk was refused for another reason than the document's size: %q", refusal.Message)
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrInvalidArgument {
		t.Errorf("a restore chunk refused for a document's size answers code %d, want %d (invalid argument): %.200q",
			refusal.Code, transport.ErrInvalidArgument, refusal.Message)
	}
}
