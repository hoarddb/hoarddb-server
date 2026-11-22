package ringclient

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestT6SeedingStatusReachesTheCaller: [GUARD the request carries exactly the database, slot,
// limit and cursor the caller gave, every row comes back typed, and more and the cursor are the
// server's].
func TestT6SeedingStatusReachesTheCaller(t *testing.T) {
	var seen wire.RingAdminRequest
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		request, err := wire.DecodeRingAdmin(frame.Payload)
		if err != nil || request.Cmd != wire.AdminSeedingStatus {
			t.Errorf("the request was %+v, %v", request, err)
			return
		}
		seen = request
		rows := []wire.RingAdminSeedRow{{Database: "app", Slot: 2638, State: "stalled", Source: "node-b", BytesDone: 41, BytesKnown: 66, Entries: 7, IdleMS: 372000, Refusal: "no chunk applied"}}
		adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, Seeding: &wire.RingAdminSeeding{Rows: rows, More: true, Cursor: []byte{0x0a, 0x4e, 'a'}}})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	slot := 2638
	page, err := client.SeedingStatus(testContext(t), SeedingQuery{Database: "app", Slot: &slot, Page: PageRequest{Limit: 5, Cursor: []byte{1, 2, 'x'}}})
	if err != nil {
		t.Fatal(err)
	}
	if seen.Database != "app" || seen.Slot == nil || *seen.Slot != 2638 || seen.Limit == nil || *seen.Limit != 5 || string(seen.Cursor) != "\x01\x02x" {
		t.Errorf("the server saw %+v", seen)
	}
	want := SeedingRow{Database: "app", Slot: 2638, State: "stalled", Source: "node-b", BytesDone: 41, BytesKnown: 66, Entries: 7, Idle: 372 * time.Second, Refusal: "no chunk applied"}
	if len(page.Rows) != 1 || page.Rows[0] != want || !page.More || string(page.Cursor) != "\x0a\x4ea" {
		t.Fatalf("SeedingStatus gave %+v, want %+v", page, want)
	}
	if _, err := client.SeedingStatus(testContext(t), SeedingQuery{}); err != nil {
		t.Fatal(err)
	}
	if seen.Database != "" || seen.Slot != nil || seen.Limit != nil || seen.Cursor != nil {
		t.Errorf("an empty query sent %+v, want no member besides cmd", seen)
	}
}

// TestT6SeedingRetryAndForgetReachTheServer: [GUARD each sends its own command with the
// database and slot, success is nil, and the server's refusal comes back as its *Error with
// the code].
func TestT6SeedingRetryAndForgetReachTheServer(t *testing.T) {
	var seen []wire.RingAdminRequest
	refuse := ""
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		request, err := wire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Errorf("the request was %+v, %v", request, err)
			return
		}
		seen = append(seen, request)
		switch refuse {
		case "not_held":
			conn.refuse(frame, cluster.CodeSeedNotHeld, "slot 5 of database \"app\" has no seed to restart on this node. Run 'status seeding'")
		case "held":
			conn.refuse(frame, cluster.CodeSeedHeld, "slot 5 of database \"app\" is seeding on this node. Run 'status seeding app 5'")
		default:
			adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, Seeding: &wire.RingAdminSeeding{Database: request.Database, Slot: *request.Slot}})
		}
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := client.SeedingRetry(testContext(t), "app", 5); err != nil {
		t.Fatal(err)
	}
	if err := client.SeedingForget(testContext(t), "app", 6, false); err != nil {
		t.Fatal(err)
	}
	if err := client.SeedingForget(testContext(t), "app", 7, true); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 || seen[1].Anyway || !seen[2].Anyway || seen[2].Cmd != wire.AdminSeedingForget || *seen[2].Slot != 7 || seen[0].Cmd != wire.AdminSeedingRetry || *seen[0].Slot != 5 || seen[1].Cmd != wire.AdminSeedingForget || *seen[1].Slot != 6 || seen[1].Database != "app" {
		t.Fatalf("the server saw %+v", seen)
	}
	var refusal *Error
	refuse = "not_held"
	if err := client.SeedingRetry(testContext(t), "app", 5); !errors.As(err, &refusal) || refusal.Code != cluster.CodeSeedNotHeld || !strings.Contains(refusal.Message, "status seeding") {
		t.Errorf("a retry of a slot with no seed came back as %v", err)
	}
	refuse = "held"
	if err := client.SeedingForget(testContext(t), "app", 5, false); !errors.As(err, &refusal) || refusal.Code != cluster.CodeSeedHeld {
		t.Errorf("a forget of a held slot came back as %v", err)
	}
	if err := client.SeedingRetry(testContext(t), "app", 9000); err == nil || !strings.Contains(err.Error(), "8191") {
		t.Errorf("a slot past the last was sent: %v", err)
	}
}

// TestT6ASeedingReplyWithoutItsMemberIsAnError: [GUARD a server that answers a seeding command
// with another command's reply is an error naming both, never a nil dereference].
func TestT6ASeedingReplyWithoutItsMemberIsAnError(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: wire.AdminShowDatabases, Databases: []wire.RingAdminDatabase{}})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.SeedingStatus(testContext(t), SeedingQuery{}); err == nil || !strings.Contains(err.Error(), "seeding_status") || !strings.Contains(err.Error(), "show_databases") {
		t.Errorf("a wrong reply to seeding_status gave %v", err)
	}
	if err := client.SeedingRetry(testContext(t), "app", 1); err == nil || !strings.Contains(err.Error(), "seeding_retry") {
		t.Errorf("a wrong reply to seeding_retry gave %v", err)
	}
}

// TestT6SeedingStatusRefusesASlotTheWireCannotCarry: [GUARD a slot filter that does not fit an
// int32 or is outside 0..8191 is refused before anything is sent, instead of being truncated to
// another slot].
func TestT6SeedingStatusRefusesASlotTheWireCannotCarry(t *testing.T) {
	sent := false
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) { sent = true })
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	for _, slot := range []int{1<<32 + 5, -1, 8192} {
		slot := slot
		if _, err := client.SeedingStatus(testContext(t), SeedingQuery{Database: "app", Slot: &slot}); err == nil || !strings.Contains(err.Error(), "8191") {
			t.Errorf("slot %d: %v", slot, err)
		}
	}
	if sent {
		t.Error("a request with a bad slot was sent")
	}
}
