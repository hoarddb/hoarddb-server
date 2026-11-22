package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestTCPServerDumpKillConnectionReleasesPin drives OpDumpStart over a real
// TCP+TLS connection, kills the connection mid-dump (before OpDumpDone), and
// proves the server released the compaction pin and the single-dump slot
// promptly: a fresh connection can dump the same bucket again immediately
// (task-2 brief's "Done when" #4).
func TestTCPServerDumpKillConnectionReleasesPin(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)

	c1 := dialTCPServer(t, addr)
	c1.login()
	c1.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "app"}, nil)
	c1.call(transport.OpCreateBucket, &CreateBucketRequest{Database: "app", Name: "queue", Type: BucketTypeFIFO}, nil)
	c1.call(transport.OpFIFOPush, &PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"tag": "x"}}, nil)

	var start DumpStartResponse
	c1.call(transport.OpDumpStart, &DumpStartRequest{Database: "app", Bucket: "queue"}, &start)
	if start.Manifest == nil {
		t.Fatalf("dump start returned no manifest")
	}

	// Kill the connection before OpDumpChunk/OpDumpDone — the common abort
	// case (dump-restore-spec.md §7.2, §16.3).
	if err := c1.conn.Close(); err != nil {
		t.Fatalf("closing connection mid-dump: %v", err)
	}

	// The server's connection-close watcher releases asynchronously; poll
	// briefly rather than assume it has already happened.
	deadline := time.Now().Add(5 * time.Second)
	c2 := dialTCPServer(t, addr)
	c2.login()
	for {
		payload, err := bson.Marshal(&DumpStartRequest{Database: "app", Bucket: "queue"})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		_ = c2.conn.SetDeadline(time.Now().Add(5 * time.Second))
		frame := transport.NewFrame(transport.OpDumpStart, payload)
		if _, err := c2.conn.Write(frame.Encode()); err != nil {
			t.Fatalf("write dump start: %v", err)
		}
		resp, err := transport.DecodeFrame(c2.br)
		if err != nil {
			t.Fatalf("read dump start response: %v", err)
		}
		if resp.Opcode != transport.OpError {
			var again DumpStartResponse
			if err := bson.Unmarshal(resp.Payload, &again); err != nil {
				t.Fatalf("decode second dump start: %v", err)
			}
			if again.Manifest == nil {
				t.Fatal("second dump start returned no manifest")
			}
			// Success: prove the second dump can also finish cleanly, and
			// that the one real walk actually streamed the element the
			// first (killed) attempt never got to.
			var chunk DumpChunkResponse
			c2.call(transport.OpDumpChunk, &DumpChunkRequest{Database: "app", Bucket: "queue"}, &chunk)
			if !chunk.EOF || chunk.Documents != 1 {
				t.Fatalf("second dump chunk = %+v, want eof with 1 document", chunk)
			}
			var done DumpDoneResponse
			c2.call(transport.OpDumpDone, &DumpDoneRequest{Database: "app", Bucket: "queue"}, &done)
			if done.Documents != 1 {
				t.Fatalf("second dump done reported %d documents, want 1", done.Documents)
			}
			return
		}
		var errResp ErrorResponse
		_ = bson.Unmarshal(resp.Payload, &errResp)
		if !strings.Contains(errResp.Message, "already running") {
			t.Fatalf("dump start refused for an unexpected reason: %s", errResp.Message)
		}
		if time.Now().After(deadline) {
			t.Fatalf("pin was not released within 5s of closing the dumping connection (still refused: %s)", errResp.Message)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTCPServerDumpConcurrentRefused proves the one-live-dump-per-server
// rule over real connections: a second connection's OpDumpStart is refused
// while the first connection's dump is still open, naming the first bucket
// (task-2 brief's "Done when" #3).
func TestTCPServerDumpConcurrentRefused(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)

	c1 := dialTCPServer(t, addr)
	c1.login()
	c1.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "app"}, nil)
	c1.call(transport.OpCreateBucket, &CreateBucketRequest{Database: "app", Name: "a", Type: BucketTypeHash}, nil)
	c1.call(transport.OpCreateBucket, &CreateBucketRequest{Database: "app", Name: "b", Type: BucketTypeHash}, nil)

	var start DumpStartResponse
	c1.call(transport.OpDumpStart, &DumpStartRequest{Database: "app", Bucket: "a"}, &start)
	t.Cleanup(func() { c1.call(transport.OpDumpDone, &DumpDoneRequest{Database: "app", Bucket: "a"}, nil) })

	c2 := dialTCPServer(t, addr)
	c2.login()
	payload, _ := bson.Marshal(&DumpStartRequest{Database: "app", Bucket: "b"})
	_ = c2.conn.SetDeadline(time.Now().Add(10 * time.Second))
	frame := transport.NewFrame(transport.OpDumpStart, payload)
	if _, err := c2.conn.Write(frame.Encode()); err != nil {
		t.Fatalf("write dump start: %v", err)
	}
	resp, err := transport.DecodeFrame(c2.br)
	if err != nil {
		t.Fatalf("read dump start response: %v", err)
	}
	if resp.Opcode != transport.OpError {
		t.Fatalf("second concurrent dump start opcode = %s, want OpError", resp.Opcode)
	}
	var errResp ErrorResponse
	_ = bson.Unmarshal(resp.Payload, &errResp)
	if !strings.Contains(errResp.Message, "app/a") || !strings.Contains(errResp.Message, "already running") {
		t.Fatalf("refusal %q does not name the first dump (app/a) as already running", errResp.Message)
	}
}

// The root gate itself (non-root and unauthenticated identities refused,
// naming the caller) is pinned at the dispatch level in
// TestDumpRequiresRoot (server/dump_test.go): this server has no persistent
// multi-user store yet (cli/unsupported.go's errUsersNotAvailable), so a
// real TCP connection cannot authenticate as anyone but the seeded root
// user, and a session with a fabricated non-root identity is exactly what
// that test builds directly.

// TestTCPServerRestoreOpcodesUnimplementedCleanly proves the six opcodes'
// wiring does not crash the connection when restore's handler bodies do not
// exist yet (task-2 brief: "make sure the unhandled case returns a clean
// 'not yet implemented'/'unknown opcode' style error rather than panicking
// or crashing the connection").
func TestTCPServerRestoreOpcodesUnimplementedCleanly(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	c := dialTCPServer(t, addr)
	c.login()

	for _, op := range []transport.Opcode{transport.OpRestoreStart, transport.OpRestoreChunk, transport.OpRestoreDone} {
		_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
		frame := transport.NewFrame(op, nil)
		if _, err := c.conn.Write(frame.Encode()); err != nil {
			t.Fatalf("write %s: %v", op, err)
		}
		resp, err := transport.DecodeFrame(c.br)
		if err != nil {
			t.Fatalf("connection did not survive %s (not yet implemented): %v", op, err)
		}
		if resp.Opcode != transport.OpError {
			t.Fatalf("%s (not yet implemented) answered %s, want OpError", op, resp.Opcode)
		}
	}

	// The connection must still be usable — an unhandled opcode did not tear
	// it down.
	var health HealthResponse
	c.call(transport.OpHealthCheck, &struct{}{}, &health)
}

// TestDumpForwardsAcrossNodesWithRootIdentity is the fix-round-1
// reproduction for the cross-node routing finding: a root user connected to
// one node ("the coordinator") dumps a bucket a DIFFERENT node owns, with
// zero membership churn. Before the fix, requireRootIdentity ran on the
// owner using the forwarded request's context — which carries the internode
// peer's cluster-token session, never a client Identity — so this failed
// every time, not only when ownership moved mid-dump. RF=1 so the bucket
// lives on exactly one node and the coordinator is forced to forward rather
// than serve it locally.
func TestDumpForwardsAcrossNodesWithRootIdentity(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "k1", Data: map[string]interface{}{"tag": "v1"},
	})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)
	coordinatorIdx := 0
	if ownerIdx == 0 {
		coordinatorIdx = 1
	}
	t.Logf("bucket owner is node %d (%s); dumping from coordinator node %d", ownerIdx+1, owner.ID, coordinatorIdx+1)

	startFrame := tc.ok(coordinatorIdx, transport.OpDumpStart, DumpStartRequest{Database: testDatabase, Bucket: testBucket})
	var start DumpStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &start); err != nil {
		t.Fatalf("decode dump start: %v", err)
	}
	if start.Manifest == nil {
		t.Fatal("dump start returned no manifest")
	}
	if start.Manifest.SourceNode != owner.ID {
		t.Fatalf("manifest.source_node = %q, want the owner %q (proves the request actually reached the owner, not the coordinator)",
			start.Manifest.SourceNode, owner.ID)
	}

	var all []byte
	for {
		chunkFrame := tc.ok(coordinatorIdx, transport.OpDumpChunk, DumpChunkRequest{Database: testDatabase, Bucket: testBucket})
		var chunk DumpChunkResponse
		if err := bson.Unmarshal(chunkFrame.Payload, &chunk); err != nil {
			t.Fatalf("decode dump chunk: %v", err)
		}
		all = append(all, chunk.Data...)
		if chunk.EOF {
			break
		}
	}
	doneFrame := tc.ok(coordinatorIdx, transport.OpDumpDone, DumpDoneRequest{Database: testDatabase, Bucket: testBucket})
	var done DumpDoneResponse
	if err := bson.Unmarshal(doneFrame.Payload, &done); err != nil {
		t.Fatalf("decode dump done: %v", err)
	}
	if done.Documents != 1 {
		t.Fatalf("cross-node dump reported %d documents, want 1", done.Documents)
	}
	var doc bson.M
	docs, err := splitBSONDocuments(all)
	if err != nil || len(docs) != 1 {
		t.Fatalf("splitting dumped bytes: %d docs, err=%v", len(docs), err)
	}
	if err := bson.Unmarshal(docs[0], &doc); err != nil {
		t.Fatalf("unmarshal dumped document: %v", err)
	}
	if doc["tag"] != "v1" {
		t.Fatalf("dumped document = %v, want tag v1", doc)
	}
}

// TestDumpCrossNodeNonRootRefused is the negative-case companion to
// TestDumpForwardsAcrossNodesWithRootIdentity: a non-root identity's
// dump/restore request, arriving at the coordinator for a bucket a
// DIFFERENT node owns, must be refused at the coordinator — before
// Cluster.forward ever ships it to the owner — naming both the root
// requirement and the caller. This exercises the exact function the
// coordinator-side gate lives in (Cluster.handleClientFrame,
// server/write_path.go) directly, with a fabricated non-root session: this
// server has no persistent multi-user store yet (cli/unsupported.go's
// errUsersNotAvailable), so a second real AUTH identity cannot be minted
// over the wire, the same reason server/dump_test.go's TestDumpRequiresRoot
// fabricates a session rather than authenticating a second real user.
func TestDumpCrossNodeNonRootRefused(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIdx := nodeIndexByAddr(t, tc.addrs, owner.ID)
	coordinatorIdx := 0
	if ownerIdx == 0 {
		coordinatorIdx = 1
	}
	t.Logf("bucket owner is node %d (%s); dumping from coordinator node %d as a non-root identity",
		ownerIdx+1, owner.ID, coordinatorIdx+1)

	payload, err := bson.Marshal(&DumpStartRequest{Database: testDatabase, Bucket: testBucket})
	if err != nil {
		t.Fatalf("marshal dump start request: %v", err)
	}
	frame := transport.NewFrame(transport.OpDumpStart, payload)

	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "alice"})
	ctx := transport.WithSession(context.Background(), sess)

	resp, err := tc.nodes[coordinatorIdx].server.core.cluster.handleClientFrame(ctx, frame)
	if err != nil {
		t.Fatalf("handleClientFrame returned a transport error instead of an OpError frame: %v", err)
	}
	if resp.Opcode != transport.OpError {
		t.Fatalf("non-root cross-node dump start answered %s, want OpError", resp.Opcode)
	}
	var errResp ErrorResponse
	if err := bson.Unmarshal(resp.Payload, &errResp); err != nil {
		t.Fatalf("decode refusal: %v", err)
	}
	if !strings.Contains(errResp.Message, "root") || !strings.Contains(errResp.Message, "alice") {
		t.Fatalf("refusal %q does not name both the root requirement and the caller", errResp.Message)
	}

	// Prove the refusal happened at the coordinator, before any forward: the
	// owner must have no dump session at all.
	ownerServer := tc.nodes[ownerIdx].server.core
	ownerServer.dumpMu.Lock()
	ownerHasSession := ownerServer.dump != nil
	ownerServer.dumpMu.Unlock()
	if ownerHasSession {
		t.Fatal("owner has an active dump session even though the coordinator should have refused before forwarding")
	}
}
