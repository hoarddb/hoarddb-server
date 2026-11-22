package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// liveHoardConn dials the server named by SOCIAL_LIVE_HOARD_ADDR, or skips the
// test. The password comes from SOCIAL_LIVE_HOARD_PASSWORD.
func liveHoardConn(t *testing.T) *wireConn {
	t.Helper()
	addr := os.Getenv("SOCIAL_LIVE_HOARD_ADDR")
	if addr == "" {
		t.Skip("set SOCIAL_LIVE_HOARD_ADDR (and SOCIAL_LIVE_HOARD_PASSWORD) to run against a local hoarddb-server")
	}
	conn, err := dialWire(addr, os.Getenv("SOCIAL_LIVE_HOARD_PASSWORD"), 30*time.Second)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	t.Cleanup(conn.close)
	return conn
}

// liveDatabase creates a scratch database and drops it when the test ends.
func liveDatabase(t *testing.T, conn *wireConn, prefix string) string {
	t.Helper()
	database := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	var created server.CreateDatabaseResponse
	if err := conn.call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: database}, &created); err != nil || !created.Success {
		t.Fatalf("creating database %s: %v %s", database, err, created.Message)
	}
	t.Cleanup(func() {
		var dropped server.DeleteDatabaseResponse
		if err := conn.call(transport.OpDeleteDB, server.DeleteDatabaseRequest{Name: database}, &dropped); err != nil {
			t.Logf("dropping %s: %v", database, err)
		}
	})
	return database
}

// TestWireSmokeNestedL3 is the wire-level smoke of build-doc S8: an L3 store
// on a btree element, Posts btreeStore { Likes hashStore, Comments fifoStore },
// written and read back over the wire.
func TestWireSmokeNestedL3(t *testing.T) {
	conn := liveHoardConn(t)
	database := liveDatabase(t, conn, "smoke_l3")
	create := server.CreateBucketRequest{Database: database, Name: "User", Type: server.BucketTypeHash, Options: map[string]interface{}{"stores": hoardStoreDeclarations()}}
	var bucket server.CreateBucketResponse
	if err := conn.call(transport.OpCreateBucket, create, &bucket); err != nil || !bucket.Success {
		t.Fatalf("creating User: %v %s", err, bucket.Message)
	}
	var stored server.InsertResponse
	if err := conn.call(transport.OpPut, server.InsertRequest{Database: database, Bucket: "User", Key: "u1", Data: map[string]interface{}{"name": "ada"}}, &stored); err != nil || !stored.Success {
		t.Fatalf("putting the user: %v %s", err, stored.Message)
	}
	var wrote server.FieldResponse
	post := server.FieldRequest{Database: database, Bucket: "User", Key: "u1", Field: "Posts", Element: "0000000001", Data: map[string]interface{}{"text": "hello"}}
	if err := conn.call(transport.OpFieldPut, post, &wrote); err != nil || !wrote.Success {
		t.Fatalf("putting a post: %v %s", err, wrote.Message)
	}
	path := []interface{}{"Posts", "0000000001"}
	like := server.FieldRequest{Database: database, Bucket: "User", Key: "u1", Field: "Likes", Path: path, Element: "u2", Data: map[string]interface{}{"ts": 1}}
	if err := conn.call(transport.OpFieldPut, like, &wrote); err != nil || !wrote.Success {
		t.Fatalf("putting a like on the post (the L3 store): %v %s", err, wrote.Message)
	}
	comment := server.FieldRequest{Database: database, Bucket: "User", Key: "u1", Field: "Comments", Path: path, Data: map[string]interface{}{"text": "nice"}}
	if err := conn.call(transport.OpFieldPush, comment, &wrote); err != nil || !wrote.Success {
		t.Fatalf("pushing a comment on the post (the L3 store): %v %s", err, wrote.Message)
	}
	for _, field := range []string{"Likes", "Comments"} {
		var length server.FieldResponse
		request := server.FieldRequest{Database: database, Bucket: "User", Key: "u1", Field: field, Path: path}
		if err := conn.call(transport.OpFieldLen, request, &length); err != nil || length.Len != 1 {
			t.Fatalf("%s.len() = %d, %v; want 1", field, length.Len, err)
		}
	}
}
