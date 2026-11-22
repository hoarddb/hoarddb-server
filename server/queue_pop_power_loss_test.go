package server

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAcknowledgedQueuePopSurvivesAPowerLoss covers the power-loss class at the
// current tree. The bug was that fifo and lifo were exempt from WAL replay on
// the premise that a pop persisted its head before returning, and it did not
// (os.Rename with no fsync). The segment store and the exemption are gone —
// queues live on the shared store and replay like every engine — so the
// property to guard is the one the premise existed for: a pop that was
// acknowledged stays popped after a power loss, even though the store commit
// behind it was never synced.
func TestAcknowledgedQueuePopSurvivesAPowerLoss(t *testing.T) {
	for _, queue := range []struct {
		bucketType BucketType
		push, pop  transport.Opcode
		wantAfter  string
	}{
		{BucketTypeFIFO, transport.OpFIFOPush, transport.OpFIFOPop, "second"},
		{BucketTypeLIFO, transport.OpLIFOPush, transport.OpLIFOPop, "first"},
	} {
		t.Run(string(queue.bucketType), func(t *testing.T) {
			dataDir := t.TempDir()
			kv := kvfault.New()
			s := newKVFaultServer(dataDir, kv)
			if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "q", Type: queue.bucketType}); err != nil || !resp.Success {
				t.Fatalf("creating the queue: %+v, %v", resp, err)
			}
			if err := kv.Sync(); err != nil {
				t.Fatalf("syncing the catalog: %v", err)
			}
			c := retentionTestCluster(t, s, dataDir)
			defer c.cancel()

			for _, element := range []string{"first", "second"} {
				if _, err := queueThroughOwner(c, queue.push, PushRequest{Database: "app", Bucket: "q", Data: map[string]interface{}{"v": element}}); err != nil {
					t.Fatalf("pushing %q: %v", element, err)
				}
			}
			popped, err := queueThroughOwner(c, queue.pop, PopRequest{Database: "app", Bucket: "q"})
			if err != nil {
				t.Fatalf("the acknowledged pop: %v", err)
			}
			if wantPopped := map[string]string{"first": "second", "second": "first"}[queue.wantAfter]; popped != wantPopped {
				t.Fatalf("test setup: the acknowledged pop returned %q, want %q", popped, wantPopped)
			}

			kv.PowerLoss()
			if err := s.wal.Close(); err != nil {
				t.Fatalf("closing the WAL: %v", err)
			}
			restarted := newKVFaultServer(dataDir, kv)
			if err := restarted.LoadCatalog(); err != nil {
				t.Fatalf("LoadCatalog after the power loss: %v", err)
			}
			w, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
			if err != nil {
				t.Fatalf("reopening the WAL: %v", err)
			}
			defer func() { _ = w.Close() }()
			restarted.wal = w
			if err := restarted.replayWAL(context.Background()); err != nil {
				t.Fatalf("replaying the WAL: %v", err)
			}

			resp, err := restarted.Pop(PopRequest{Database: "app", Bucket: "q"})
			if err != nil || !resp.Found {
				t.Fatalf("popping after the power loss: %+v, %v", resp, err)
			}
			if got := elementValue(t, resp.Items[0]); got != queue.wantAfter {
				t.Errorf("after the power loss the queue popped %q, want %q: the acknowledged pop of %q came back",
					got, queue.wantAfter, popped)
			}
		})
	}
}

// queueThroughOwner applies a queue operation along the owner's path — logged,
// then applied — and returns the popped element's value for a pop.
func queueThroughOwner(c *Cluster, op transport.Opcode, request interface{}) (string, error) {
	payload, err := bson.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encoding %s: %w", op, err)
	}
	resp, err := asOwner(c, ReplEntry{Op: int32(op), Bucket: "q", Payload: payload})
	if err != nil {
		return "", fmt.Errorf("applying %s: %w", op, err)
	}
	if transport.Opcode(resp.Opcode) == transport.OpError {
		return "", fmt.Errorf("applying %s: answered with an error frame: %s", op, resp.Payload)
	}
	var popped PopResponse
	if err := bson.Unmarshal(resp.Payload, &popped); err != nil || !popped.Found {
		return "", nil
	}
	var doc struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(popped.Items[0], &doc); err != nil {
		return "", fmt.Errorf("decoding the popped element: %w", err)
	}
	return doc.V, nil
}

func elementValue(t *testing.T, raw bson.Raw) string {
	t.Helper()
	var doc struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decoding a queue element: %v", err)
	}
	return doc.V
}
