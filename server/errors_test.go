package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestExplainErrorNamesTheRecoveryStep covers AGENTS.md rule 3 for the failures
// that are hardest to diagnose from the client side.
//
// A full disk used to reach the client as an empty Success:false with a bare error
// opcode, which is indistinguishable from a malformed request. The filesystem's
// own words are kept, and the step that fixes it is appended.
func TestExplainErrorNamesTheRecoveryStep(t *testing.T) {
	diskFull := fmt.Errorf("putting key k1: %w", syscall.ENOSPC)

	explained := explainError(diskFull)
	if !strings.Contains(explained, "no space left on device") {
		t.Errorf("explained error dropped the cause: %q", explained)
	}
	if !strings.Contains(explained, "out of disk space") || !strings.Contains(explained, "free space") {
		t.Errorf("explained error does not say what to do: %q", explained)
	}

	// An error with nothing to add is passed through unchanged.
	plain := errors.New("bucket not found")
	if got := explainError(plain); got != plain.Error() {
		t.Errorf("explainError(%v) = %q, want it unchanged", plain, got)
	}
	if got := explainError(nil); got != "" {
		t.Errorf("explainError(nil) = %q, want empty", got)
	}

	// The same treatment for the Success:false path, which is how the mutating
	// handlers report an ordinary failure. Verified against a real full disk: the
	// segment store's own text reaches the message ("... no space left on device")
	// and the hint has to be part of it.
	msg := failureMessage("segment push failed", diskFull)
	if !strings.Contains(msg, "segment push failed") || !strings.Contains(msg, "no space left on device") {
		t.Errorf("failureMessage dropped the context or the cause: %q", msg)
	}
	if !strings.Contains(msg, "free space") {
		t.Errorf("failureMessage did not say what to do: %q", msg)
	}
}

// TestHandlerErrorComesBackAsAMessageNotAReset is the wire-level half.
//
// The transport resets a stream when a handler returns a Go error, so a client
// whose request failed for an ordinary reason used to see a torn connection
// instead of a reason. Handlers now answer with an error frame carrying the
// message and a nil error, and the session must survive it.
func TestHandlerErrorComesBackAsAMessageNotAReset(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	client := tc.nodes[0].client
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. A malformed payload: the handler cannot decode it, and the client must be
	//    told so rather than having its stream reset.
	badFrame, err := client.SendRequest(ctx, transport.OpGet, []byte{0xFF, 0xFE, 0xFD})
	if err != nil {
		t.Fatalf("a malformed request tore down the session instead of returning an error frame: %v", err)
	}
	if badFrame.Opcode != transport.OpError {
		t.Fatalf("malformed request answered with %s, want %s", badFrame.Opcode, transport.OpError)
	}
	// The validating scan runs before anything else reads the payload, and a
	// payload it cannot read is refused there rather than passed to the
	// handler; the message still names the decode failure.
	if msg := decodeErrorMessage(t, badFrame.Payload); !strings.Contains(msg, "not well-formed BSON") {
		t.Fatalf("malformed request error message = %q, want it to name the decode failure", msg)
	}
	if code := decodeErrorCode(t, badFrame.Payload); code == int(transport.ErrOK) {
		t.Fatalf("error frame carried code %d (ErrOK) — a driver that trusts code reads this failure as a success", code)
	}

	// 2. A well-formed request for something that does not exist: the same rule,
	//    and the message has to say which database is missing and what to run.
	missing, err := client.SendRequest(ctx, transport.OpGet, mustMarshal(t, FindRequest{
		Database: "no_such_db",
		Bucket:   "no_such_bucket",
		Key:      "k1",
	}))
	if err != nil {
		t.Fatalf("a missing database tore down the session instead of returning an error frame: %v", err)
	}
	if missing.Opcode != transport.OpError {
		t.Fatalf("missing database answered with %s, want %s", missing.Opcode, transport.OpError)
	}
	if msg := decodeErrorMessage(t, missing.Payload); !strings.Contains(msg, "no_such_db") {
		t.Fatalf("missing-database error message = %q, want it to name the database", msg)
	}
	if code := decodeErrorCode(t, missing.Payload); code == int(transport.ErrOK) {
		t.Fatalf("error frame carried code %d (ErrOK) — a driver that trusts code reads this failure as a success", code)
	}

	// 3. The session is still usable after two failed requests.
	if frame := tc.ok(0, transport.OpCount, CountRequest{Database: testDatabase, Bucket: testBucket}); frame.Opcode == transport.OpError {
		t.Fatalf("session unusable after failed requests: %s", string(frame.Payload))
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	payload, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return payload
}

func decodeErrorMessage(t *testing.T, payload []byte) string {
	t.Helper()
	if len(payload) == 0 {
		t.Fatal("error frame carried no payload, so the client cannot know what went wrong")
	}
	var resp ErrorResponse
	if err := bson.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("decoding error response: %v", err)
	}
	return resp.Message
}

func decodeErrorCode(t *testing.T, payload []byte) int {
	t.Helper()
	if len(payload) == 0 {
		t.Fatal("error frame carried no payload, so the client cannot know what went wrong")
	}
	var resp ErrorResponse
	if err := bson.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("decoding error response: %v", err)
	}
	return resp.Code
}

// TestFailureMessagesCarryNoInternalKey is BUG-0382: the storage layer's read
// errors name the internal key in hex (slot, bucket id, namespace, document
// key), and the generic failure paths forward that text to the client, so a
// developer sees an encoding they never sent and cannot act on. This drives a
// real node-store read that fails — the store is closed under the server, so
// every read through it errors, the same failure the bug's reproduction names
// — and reads the message an insert answers.
func TestFailureMessagesCarryNoInternalKey(t *testing.T) {
	dataDir := t.TempDir()
	kv, err := storage.OpenBadgerKV(filepath.Join(dataDir, nodeStoreDirName), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the node store: %v", err)
	}
	s := serverOnStore(t, kv)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	b := lookupBucket(s, "app", "users")
	stateKey := storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSState)

	if err := kv.Close(); err != nil {
		t.Fatalf("closing the node store: %v", err)
	}

	resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "user-01284",
		Data: map[string]interface{}{"Name": "a"}})
	if err != nil {
		t.Fatalf("Insert returned a Go error instead of an answer: %v", err)
	}
	if resp.Success {
		t.Fatalf("the insert against a closed store succeeded: %+v", resp)
	}
	if hexKey := hex.EncodeToString(stateKey); strings.Contains(resp.Message, hexKey) {
		t.Fatalf("the failure message carries the internal storage key in hex:\n%s", resp.Message)
	}
}

// TestErrorFrameCodeIsNeverOK pins codeFor's fallback directly: an arbitrary
// Go error with no matching sentinel must still classify as something other
// than transport.ErrOK, since ErrorResponse.Code's Go zero value (0) is
// exactly transport.ErrOK's numeric value — leaving it unset is
// indistinguishable on the wire from success.
func TestErrorFrameCodeIsNeverOK(t *testing.T) {
	if code := codeFor(errors.New("some arbitrary failure with no sentinel")); code == transport.ErrOK {
		t.Fatalf("codeFor(unrecognized error) = %s, want anything but ErrOK", code)
	}
}

// TestErrorFrameCodeClassifiesKnownSentinels pins the few cases codeFor does
// classify precisely, so a future refactor of these error vars doesn't
// silently fall them back to the generic case.
func TestErrorFrameCodeClassifiesKnownSentinels(t *testing.T) {
	cases := []struct {
		err  error
		want transport.ErrorCode
	}{
		{ErrMemberMismatch, transport.ErrConflict},
		// An owner's disagreement clears by itself: a retry (BUG-0704).
		{ErrOwnerMembershipMismatch, transport.ErrUnavailable},
		{ErrWriteConcernTimeout, transport.ErrTimeout},
		{ErrJoinSyncIncomplete, transport.ErrUnavailable},
		// User administration (BUG-0622, owner ruling 2026-10-19).
		{ErrUserExists, transport.ErrAlreadyExists},
		{ErrUserNotFound, transport.ErrNotFound},
		{ErrUserVersion, transport.ErrConflict},
		{ErrUnknownRole, transport.ErrInvalidArgument},
		{ErrCredentialInvalid, transport.ErrInvalidArgument},
		{ErrUsernameRequired, transport.ErrInvalidArgument},
		{ErrReservedUsername, transport.ErrInvalidArgument},
		{ErrSaltReused, transport.ErrInvalidArgument},
		{ErrTooManyUsers, transport.ErrInvalidArgument},
		{ErrLastAdmin, transport.ErrInvalidArgument},
		{ErrNotMaster, transport.ErrUnavailable},
		{ErrNoQuorum, transport.ErrUnavailable},
	}
	for _, c := range cases {
		if got := codeFor(c.err); got != c.want {
			t.Errorf("codeFor(%v) = %s, want %s", c.err, got, c.want)
		}
		// Wrapped the way a real handler would wrap it (fmt.Errorf("...: %w")),
		// errors.Is must still see through to the sentinel.
		wrapped := fmt.Errorf("wrapping context: %w", c.err)
		if got := codeFor(wrapped); got != c.want {
			t.Errorf("codeFor(wrapped %v) = %s, want %s", c.err, got, c.want)
		}
	}
}

// TestCodeForDiskFullRefusal is BUG-0552: a node at or above its disk
// high-water mark refuses an operator's migrate to protect itself, not because
// it is faulty, so the refusal must not fall through to INTERNAL. It is the
// same "cannot answer here, now" as the other UNAVAILABLE refusals, and
// codeFor must see the sentinel through the handler's wrap.
func TestCodeForDiskFullRefusal(t *testing.T) {
	err := fmt.Errorf("x: %w", ErrMemberDiskFull)
	if got := codeFor(err); got != transport.ErrUnavailable {
		t.Errorf("codeFor(%v) = %s, want %s: a deliberate disk refusal must not reach a driver as INTERNAL", err, got, transport.ErrUnavailable)
	}
}

// TestMissingDatabaseAnswersNotFound checks that a request naming a
// database that does not exist is NOT_FOUND on every verb that resolves the
// database first, not INTERNAL, and the message that names the way out is kept.
func TestMissingDatabaseAnswersNotFound(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	cases := []struct {
		name    string
		opcode  transport.Opcode
		request any
	}{
		{"bucket stats", transport.OpBucketStats, BucketStatsRequest{Database: "missing", Bucket: "b"}},
		{"list buckets", transport.OpListBuckets, ListBucketsRequest{Database: "missing"}},
		{"put", transport.OpPut, InsertRequest{Database: "missing", Bucket: "b", Key: "k", Data: map[string]interface{}{"a": 1}}},
		{"drop database", transport.OpDeleteDB, DeleteDatabaseRequest{Name: "missing"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			answer, err := handler(context.Background(), transport.NewFrame(tc.opcode, mustMarshal(t, tc.request)))
			if err != nil {
				t.Fatalf("handler returned a Go error, which resets the stream: %v", err)
			}
			if answer.Opcode != transport.OpError {
				t.Fatalf("answer opcode = %s, want ERROR", answer.Opcode)
			}
			if got, want := decodeErrorCode(t, answer.Payload), int(transport.ErrNotFound); got != want {
				t.Errorf("error code = %d, want %d (NOT_FOUND)", got, want)
			}
			if message := decodeErrorMessage(t, answer.Payload); !strings.Contains(message, "create database missing") {
				t.Errorf("message %q lost the way out (create database missing)", message)
			}
		})
	}
}
