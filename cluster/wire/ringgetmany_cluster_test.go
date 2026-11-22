package wire_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

type allowAll struct{}

func (allowAll) Authorize(context.Context, wire.RingAccess) error { return nil }

func refuseNext(_ context.Context, frame *transport.Frame) (*transport.Frame, error) {
	return transport.NewFrameWithID(transport.OpError, frame.RequestID, []byte{5, 0, 0, 0, 0}), nil
}

// TestGetManyFrameMemoryIsBoundedAcrossThreeNodes is the real-cluster version of the unit test of
// the same bound (spec section 10, test 18): 255 documents of 80 KiB owned by three nodes, one
// frame sent to one of them under a 256 KiB reply budget. The bytes of answers the handler holds
// (the test counter of G2, read on every landing) never pass the budget, the answers kept sum to
// within it, every key is answered once and the keys that did not fit say so, naming their index.
// A handler that kept every answer and trimmed afterwards would hold about 20 MB here.
func TestGetManyFrameMemoryIsBoundedAcrossThreeNodes(t *testing.T) {
	const database, bucket, size, budget, keys = "app", "users", 80 << 10, 256 << 10, 255
	c := harness.NewCluster(t, 3, harness.Options{RaftTimeout: 400 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(ctx, database, 2); err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	for range 20 {
		c.AwaitLeases(30 * time.Second)
		_, err := leader.CreateBucket(ctx, database, bucket, wire.Field{Name: "name", Type: "string"})
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "lease") {
			t.Fatal(err)
		}
	}
	pad := strings.Repeat("x", size)
	ids := make([]string, keys)
	for i := range ids {
		ids[i] = fmt.Sprintf("big-%03d", i)
		var last error
		for range 40 {
			_, last = c.Node(c.IDs()[i%3]).Insert(ctx, database, bucket, bson.D{{Key: "_id_", Value: ids[i]}, {Key: "name", Value: pad}})
			if last == nil || strings.Contains(last.Error(), "already exists") {
				last = nil
				break
			}
			c.AwaitLeases(30 * time.Second)
			time.Sleep(100 * time.Millisecond)
		}
		if last != nil {
			t.Fatalf("insert of %s: %v", ids[i], last)
		}
	}
	owners := map[string]bool{}
	for _, id := range ids {
		owner, _, err := c.Node("n1").Owner(ctx, database, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		owners[owner] = true
	}
	if len(owners) != 3 {
		t.Fatalf("the documents have owners %v; the test needs all three nodes", owners)
	}

	var mu sync.Mutex
	var peakHeld int64
	handler, err := wire.NewRingHandler(c.Node("n1"), refuseNext, allowAll{}, wire.WithRingResultBudget(budget),
		wire.WithGetManyHeld(func(held int64) {
			mu.Lock()
			peakHeld = max(peakHeld, held)
			mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	send := func(payload []byte) (*transport.Frame, error) {
		return handler(ctx, transport.NewFrameWithID(transport.OpRingGetMany, [16]byte{0x6D}, payload))
	}
	describe, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: database, Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := handler(ctx, transport.NewFrameWithID(transport.OpRingAdmin, [16]byte{0x61}, describe))
	if err != nil || frame.Opcode != transport.OpRingReply {
		t.Fatalf("describe_bucket: %v %v", frame, err)
	}
	header, err := wire.DecodeRingAdminReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	reads := make([]wire.RingRead, keys)
	for i, id := range ids {
		encoded, err := store.EncodeComponent(id)
		if err != nil {
			t.Fatal(err)
		}
		reads[i] = wire.RingRead{Class: wire.RingClassGet, BucketID: header.BucketID, DeclVersion: header.DeclVersion, SchemaVersionSeen: header.SchemaVersionSeen, EncID: encoded}
	}
	payload, err := wire.EncodeRingGetMany(wire.RingGetManyRequest{Reads: reads})
	if err != nil {
		t.Fatal(err)
	}
	statsBefore := c.Node("n1").GetManyStats().Errors.Load()
	frame, err = send(payload)
	if err != nil || frame.Opcode != transport.OpRingReply {
		t.Fatalf("the frame was not answered: %v %v", frame, err)
	}
	reply, err := wire.DecodeRingReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Answers) != keys {
		t.Fatalf("%d answers for %d keys", len(reply.Answers), keys)
	}
	kept, keptBytes, refused := 0, int64(0), 0
	for i, answer := range reply.Answers {
		switch {
		case answer.Reply != nil:
			kept++
			for _, element := range answer.Reply.Elements {
				keptBytes += int64(len(element))
			}
			if got := bson.Raw(answer.Reply.Elements[0]).Lookup("_id_").StringValue(); got != ids[i] {
				t.Errorf("key %d answered %q, want %s", i, got, ids[i])
			}
		case answer.Error != nil && answer.Error.Code == cluster.CodeResultTooLarge:
			refused++
			if want := fmt.Sprintf("key %d of %d", i, keys); !strings.Contains(answer.Error.Message, want) {
				t.Errorf("key %d refused %q, want it to name %q", i, answer.Error.Message, want)
			}
		default:
			t.Errorf("key %d answered %+v, want a document or RESULT_TOO_LARGE", i, answer)
		}
	}
	if kept == 0 || kept > budget/size {
		t.Errorf("%d answers were kept; a %d-byte budget holds 1 to %d documents of %d bytes", kept, budget, budget/size, size)
	}
	if keptBytes > budget {
		t.Errorf("the kept answers weigh %d bytes, over the %d-byte budget", keptBytes, budget)
	}
	mu.Lock()
	defer mu.Unlock()
	if peakHeld == 0 || peakHeld > budget {
		t.Errorf("the handler held %d bytes of answers at its peak, want 1 to %d", peakHeld, budget)
	}
	if got := c.Node("n1").GetManyStats().Errors.Load() - statsBefore; got != int64(refused) {
		t.Errorf("the node counted %d per-key errors for %d refused keys", got, refused)
	}
	t.Logf("kept %d of %d documents (%d bytes), peak held %d of the %d budget", kept, keys, keptBytes, peakHeld, budget)
}
