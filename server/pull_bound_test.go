package server

import (
	"bytes"
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// overstatingSourceConn stands in for a source whose answers overstate a
// copy: it rewrites MIGRATE_START's chunk count to numChunks when that is
// non-zero, and each chunk's bytes to chunkBytes when that is non-zero. It
// counts the MIGRATE_DATA requests the puller sends.
type overstatingSourceConn struct {
	peerConn
	numChunks  int
	chunkBytes int
	asked      atomic.Int64
}

func (c *overstatingSourceConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpMigrateData {
		c.asked.Add(1)
	}
	frame, err := c.peerConn.SendRequest(ctx, lane, op, payload)
	if err != nil {
		return frame, err
	}
	switch {
	case op == transport.OpMigrateStart && c.numChunks != 0:
		var start MigrateStartResponse
		if bson.Unmarshal(frame.Payload, &start) != nil || !start.Ok {
			return frame, nil
		}
		start.NumChunks = c.numChunks
		return reencodedFrame(frame, start)
	case op == transport.OpMigrateData && c.chunkBytes != 0:
		var chunk MigrateDataResponse
		if bson.Unmarshal(frame.Payload, &chunk) != nil || !chunk.Ok {
			return frame, nil
		}
		chunk.Data = bytes.Repeat([]byte{0x01}, c.chunkBytes)
		return reencodedFrame(frame, chunk)
	}
	return frame, nil
}

func reencodedFrame(frame *transport.Frame, answer any) (*transport.Frame, error) {
	body, err := bson.Marshal(answer)
	if err != nil {
		return nil, err
	}
	return transport.NewFrameWithID(frame.Opcode, frame.RequestID, body), nil
}

// pullThrough pulls testBucket from its owner onto another node of a fresh
// three-node cluster through conn, which wraps the puller's connection to the
// owner. It returns the pull's error.
func pullThrough(t *testing.T, conn *overstatingSourceConn) error {
	t.Helper()
	tc := newTestClusterTuned(t, 3, 1, autoRebalanceOff, nil)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	tc.ok(ownerIndex, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: testBucket,
		Data: map[string]interface{}{"e": "one"}, WriteConcern: "all"})
	targetIndex := (ownerIndex + 1) % len(tc.addrs)
	target := clusterOf(tc, targetIndex)
	ownerID := tc.addrs[ownerIndex]
	owner := mustNode(t, target, ownerID)
	openedStream(t, target, ownerID)
	wrapPeerConn(target.peerFor(owner), func(inner peerConn) peerConn {
		conn.peerConn = inner
		return conn
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return target.pullBucket(ctx, testDatabase, testBucket, owner)
}

// TestAPullRefusesACopyLargerThanTheNodeBuffersBeforeFetchingIt: a pull holds
// the copy in memory until it is checked, so a source's chunk count sets how
// much this node buffers. A start that announces more chunks than the node's
// bound for one copy is refused before any chunk is asked for. Before the
// bound, the puller looped to the announced count. A chunk count at the top
// of int is refused too, so the check never multiplies the count by the chunk
// size where it could wrap past the bound.
func TestAPullRefusesACopyLargerThanTheNodeBuffersBeforeFetchingIt(t *testing.T) {
	counts := []struct {
		name      string
		numChunks int
	}{
		{"MaxInt32", math.MaxInt32},
		{"MaxInt64", math.MaxInt64},
	}
	for _, tc := range counts {
		t.Run(tc.name, func(t *testing.T) {
			conn := &overstatingSourceConn{numChunks: tc.numChunks}
			err := pullThrough(t, conn)
			if err == nil || !strings.Contains(err.Error(), "more than this node holds in memory for one copy") {
				t.Fatalf("a pull whose start announced %d chunks returned %v; want the refusal of a copy over the bound", conn.numChunks, err)
			}
			if asked := conn.asked.Load(); asked != 0 {
				t.Fatalf("the puller asked for %d chunks of a copy it should have refused at its start", asked)
			}
		})
	}
}

// TestAPullRefusesAChunkLargerThanAChunkIs: the bound is computed from the
// chunk count and the chunk size, so a chunk larger than the size is refused
// as it arrives, and the bytes buffered never pass the bound. Before, a
// source could send any number of bytes per chunk.
func TestAPullRefusesAChunkLargerThanAChunkIs(t *testing.T) {
	conn := &overstatingSourceConn{chunkBytes: snapshotChunkSize + 1}
	err := pullThrough(t, conn)
	if err == nil || !strings.Contains(err.Error(), "larger than a chunk is") {
		t.Fatalf("a pull sent a chunk of %d bytes and returned %v; want the refusal of an oversized chunk", conn.chunkBytes, err)
	}
}
