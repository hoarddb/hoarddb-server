package server

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// largeDocument is a put of a document of about size bytes.
func largeDocument(key string, size int, writeConcern string) InsertRequest {
	return InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: key,
		Data:         map[string]interface{}{"blob": strings.Repeat("b", size)},
		WriteConcern: writeConcern,
	}
}

func createTestHashBucket(t *testing.T, tc *testCluster) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
}

// TestAReplicationBatchIsCutByBytesAsWellAsByCount reproduces the spec's
// probe 4: a replica is stopped, 70 documents of 600 KiB are written at
// majority, the replica comes back and the owner repairs it. A batch cut at 64
// entries regardless of size is 38 MiB, over the 32 MiB frame, and the receiver
// refuses it for ever (66 of the 70 never arrived).
func TestAReplicationBatchIsCutByBytesAsWellAsByCount(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the owner: %v", err)
	}
	downIndex := -1
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			downIndex = nodeIndexByAddr(t, tc.addrs, id)
			break
		}
	}
	if downIndex < 0 {
		t.Fatalf("the bucket has no replica besides its owner %s", owner.ID)
	}
	_ = tc.nodes[downIndex].client.Close()
	tc.nodes[downIndex].client = nil
	if err := tc.nodes[downIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", downIndex+1, err)
	}
	tc.nodes[downIndex].server = nil

	const documents = 70
	coordinator := tc.coordinatorIndex(t, downIndex, owner.ID)
	keys := make([]string, 0, documents)
	for i := 0; i < documents; i++ {
		key := "large-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		keys = append(keys, key)
		if frame := tc.frame(coordinator, transport.OpPut, largeDocument(key, 600<<10, "majority")); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at majority with a replica down: %s", key, frame.Payload)
		}
	}

	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted

	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := replayToPeer(ctx, ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[downIndex]))); err != nil {
		t.Fatalf("catch-up to the restarted replica: %v", err)
	}
	missing := 0
	for _, key := range keys {
		if found, _ := tc.readFromNode(downIndex, key); !found {
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("%d of %d documents of 600 KiB are missing from the replica after catch-up returned nil", missing, documents)
	}
}

// gatedRecordingConn wraps a peer's connection: it records the size of every
// replication frame sent on it and holds each one until the gate opens, so a
// test can let the sender's queue fill behind a frozen peer.
type gatedRecordingConn struct {
	peerConn
	gate chan struct{}
	mu   sync.Mutex
	// pushSizes are the payload sizes of the replication frames, in send order.
	pushSizes []int
}

func (g *gatedRecordingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		g.mu.Lock()
		g.pushSizes = append(g.pushSizes, len(payload))
		g.mu.Unlock()
		select {
		case <-g.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.peerConn.SendRequest(ctx, lane, op, payload)
}

func (g *gatedRecordingConn) sizes() []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int(nil), g.pushSizes...)
}

// TestTheSteadyStateSenderCutsABatchByBytes is the same defect on the path a
// live write takes: entries queue behind a slow peer, and when it answers the
// sender takes up to 64 of them into one frame.
func TestTheSteadyStateSenderCutsABatchByBytes(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve the owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	replicaPeer := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex]))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	realConn, err := replicaPeer.dial(ctx)
	if err != nil {
		t.Fatalf("dial the replica: %v", err)
	}
	gated := &gatedRecordingConn{peerConn: realConn, gate: make(chan struct{})}
	var openOnce sync.Once
	openGate := func() { openOnce.Do(func() { close(gated.gate) }) }
	// Registered after tc.stop's defer, so it runs first: a node cannot stop
	// while its sender is held.
	defer openGate()
	replicaPeer.mu.Lock()
	replicaPeer.client = gated
	replicaPeer.mu.Unlock()

	const documents = 70
	keys := make([]string, 0, documents)
	for i := 0; i < documents; i++ {
		key := "queued-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		keys = append(keys, key)
		if frame := tc.frame(ownerIndex, transport.OpPut, largeDocument(key, 600<<10, "one")); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at one: %s", key, frame.Payload)
		}
	}
	openGate()

	pollUntil(t, "the replica holds every queued document", func() bool {
		for _, key := range keys {
			if found, _ := tc.readFromNode(replicaIndex, key); !found {
				return false
			}
		}
		return true
	})
	limit := maxReplBatchBytes + replFrameOverhead
	sizes := gated.sizes()
	if len(sizes) < 3 {
		t.Fatalf("the sender used %d frames for 70 documents of 600 KiB; the queue never built up, so the test proved nothing", len(sizes))
	}
	for _, size := range sizes {
		if size > limit {
			t.Errorf("a replication frame carried %d bytes; a batch is cut at %d bytes of payload (frame limit %d)", size, maxReplBatchBytes, transport.MaxFrameLength)
		}
	}
}

// TestReplBatchEnd pins the cut itself: 64 entries or 8 MiB, whichever comes
// first, and an entry that is alone over the bound is a batch of one.
func TestReplBatchEnd(t *testing.T) {
	entryOf := func(size int) ReplEntry { return ReplEntry{Payload: make([]byte, size)} }
	repeat := func(size, count int) []ReplEntry {
		entries := make([]ReplEntry, count)
		for i := range entries {
			entries[i] = entryOf(size)
		}
		return entries
	}
	cases := []struct {
		name    string
		entries []ReplEntry
		start   int
		want    int
	}{
		{"small entries are cut by count", repeat(100, 200), 0, maxReplBatch},
		{"600 KiB entries are cut by bytes", repeat(600<<10, 70), 0, 13},
		{"a cut that starts mid-slice", repeat(600<<10, 70), 13, 26},
		{"one entry over the bound ships alone", append([]ReplEntry{entryOf(20 << 20)}, repeat(1000, 5)...), 0, 1},
		{"an entry that would cross the bound starts the next batch", append(repeat(5<<20, 1), entryOf(4<<20)), 0, 1},
		{"the tail is one short batch", repeat(1000, 3), 0, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := replBatchEnd(tc.entries, tc.start); got != tc.want {
				t.Errorf("replBatchEnd(start %d) = %d, want %d", tc.start, got, tc.want)
			}
		})
	}
}

// TestAnEntryTooLargeToReplicateIsRefusedBeforeItIsLogged guards
// write_path.go:checkFitsReplicationFrame. A request whose payload fits a
// client frame but not the replication frame that carries it (token, node id,
// field names) can never reach a replica; logging it first would leave it in
// the owner's log to be re-sent for ever.
//
// Since the per-document limit (at most 30 MiB, under the 32 MiB frame) every
// put, push, insert and batch element is refused at the door before this guard
// is reached, so this test drives it through the one write the door cannot
// measure: an update, whose result is only known after the operators run. Its
// payload is whatever the client sent, up to the frame limit.
func TestAnEntryTooLargeToReplicateIsRefusedBeforeItIsLogged(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "target", Data: map[string]interface{}{"n": 1}})

	// The database and bucket creations and the put are broadcast or
	// replicated and not waited for: let every node log them before the count
	// is taken.
	for _, n := range tc.nodes {
		waitForWALSequence(t, n, 3)
	}
	logged := func() []uint64 {
		sequences := make([]uint64, len(tc.nodes))
		for i, n := range tc.nodes {
			sequences[i] = n.server.core.wal.LastSequence()
		}
		return sequences
	}
	updateOf := func(size int) UpdateRequest {
		return UpdateRequest{Database: testDatabase, Bucket: testBucket, Key: "target",
			Set: map[string]interface{}{"blob": strings.Repeat("b", size)}}
	}
	before := logged()

	// About 2 KiB under the client frame limit: the transport takes it, the
	// replication frame (4 KiB of overhead reserved) cannot hold it.
	frame := tc.frame(0, transport.OpUpdate, updateOf(transport.MaxFrameLength-2048))
	if frame.Opcode != transport.OpError {
		t.Fatalf("an update that cannot fit a replication frame was answered %s", frame.Opcode)
	}
	message := string(frame.Payload)
	for _, want := range []string{"cannot fit a 32 MiB frame", "smaller pieces", "the large-files page of the user docs"} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal %q does not say %q", message, want)
		}
	}
	after := logged()
	for i := range before {
		if after[i] != before[i] {
			t.Errorf("node %d logged %d entries for a refused request", i+1, after[i]-before[i])
		}
	}

	// 16 KiB under the frame limit is past this guard (the document limit then
	// refuses the result, with its own message), and an update well inside
	// both is taken.
	nearFrame := tc.frame(0, transport.OpUpdate, updateOf(transport.MaxFrameLength-16<<10))
	if nearFrame.Opcode != transport.OpError || strings.Contains(string(nearFrame.Payload), "cannot fit a 32 MiB frame") {
		t.Fatalf("an update of 32 MiB less 16 KiB was answered %s %q, want the document-limit refusal", nearFrame.Opcode, nearFrame.Payload)
	}
	if fits := tc.frame(0, transport.OpUpdate, updateOf(1<<20)); fits.Opcode == transport.OpError {
		t.Fatalf("an update of 1 MiB was refused: %s", fits.Payload)
	}
}

// TestAPutTooLargeToReplicateIsRefusedByTheDocumentLimitFirst records what the
// per-document limit changed for the old replication-frame probe: a put of 32
// MiB less 2 KiB is refused at the door, naming the document limit, and logs
// nothing. A put cannot reach checkFitsReplicationFrame while the limit is
// under the frame.
func TestAPutTooLargeToReplicateIsRefusedByTheDocumentLimitFirst(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)
	for _, n := range tc.nodes {
		waitForWALSequence(t, n, 2)
	}
	before := tc.nodes[0].server.core.wal.LastSequence()

	frame := tc.frame(0, transport.OpPut, largeDocument("too-big", transport.MaxFrameLength-2048, "majority"))
	if frame.Opcode != transport.OpError || !strings.Contains(string(frame.Payload), "storage.max_document_bytes") {
		t.Fatalf("a put of 32 MiB less 2 KiB was answered %s %q, want the document-limit refusal", frame.Opcode, frame.Payload)
	}
	if after := tc.nodes[0].server.core.wal.LastSequence(); after != before {
		t.Errorf("the owner logged %d entries for a refused put", after-before)
	}
}
