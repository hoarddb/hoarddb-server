package ringclient

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCreateDatabaseWithStorageMemoryIsRefusedByTheClient: the client refuses storage memory with the
// rule 3 sentence before it sends anything, and a disk word still goes out and comes back.
func TestCreateDatabaseWithStorageMemoryIsRefusedByTheClient(t *testing.T) {
	sent := 0
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		request, err := wire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		sent++
		adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, DatabaseID: 7, RF: 1, Storage: "disk"})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.CreateDatabaseWithStorage(testContext(t), "cache", 1, "memory")
	var coded *Error
	const want = "storage: memory is not available yet: this build keeps every database on disk, so a memory database would not be lost on restart as its name says. Create it with storage: disk (the default): create database cache"
	if !errors.As(err, &coded) || coded.Message != want {
		t.Fatalf("memory create err = %v, want %q", err, want)
	}
	if sent != 0 {
		t.Fatalf("the refused create reached the server %d times", sent)
	}
	info, err := client.CreateDatabaseWithStorage(testContext(t), "cache", 1, "disk")
	if err != nil || info != (DatabaseInfo{ID: 7, RF: 1, Storage: "disk"}) {
		t.Fatalf("disk create gave %+v, %v", info, err)
	}
}

// TestDescribeDatabaseReturnsStorageAndMemoryFigures: the storage word and the two memory
// figures of a memory database come back in DatabaseDescription.
func TestDescribeDatabaseReturnsStorageAndMemoryFigures(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		request, err := wire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, Name: "cache", DatabaseID: 7, RF: 1, Storage: "memory",
			MemoryBytes: 4096, MemoryCapBytes: 8192, Buckets: []wire.RingAdminListed{}})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	described, err := client.DescribeDatabase(testContext(t), "cache", PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if described.Storage != "memory" || described.MemoryBytes != 4096 || described.MemoryCapBytes != 8192 || described.ID != 7 {
		t.Fatalf("DescribeDatabase gave %+v", described)
	}
}
