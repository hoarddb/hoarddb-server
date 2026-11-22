package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestForwardedWritesNotCappedAtRequestLane is the acceptance test of
// BUG-0723: 64 clients write through a holder that does not own their
// buckets, the owner's log fsync is held, and the holder must have more than
// the request lane's old 24 forwards (3 sockets x 8 frames) in flight to the
// owner. Today a forwarded write keeps its lane slot for the owner's whole
// cycle, so the 25th waits in TCPPeerClient.acquire and the count stops at 24.
//
// Break: leave the request sockets at 8 frames each; the peak is 24.
func TestForwardedWritesNotCappedAtRequestLane(t *testing.T) {
	const (
		writers         = 64
		oldRequestLane  = 24 // 3 request sockets x 8 frames, spec §3.6; a literal so the bound survives a fix
		holderIndex     = 0
		ownerIndex      = 1
		observationTime = 3 * time.Second
	)
	// 64 writers dial from one address, which is the per-address default.
	moreConnections := func(_ int, cfg *config.Config) { cfg.TCP.MaxConnsPerIP = 256 }
	tc := newBatchCluster(t, 2, 1, 0, moreConnections)
	var users []int
	for u := 1; len(users) < writers; u++ {
		if ownerOfUser(t, tc, u) == ownerIndex {
			createClusterUser(t, tc, u)
			users = append(users, u)
		}
	}
	owner := tc.nodes[ownerIndex].server.core
	ownerID := tc.addrs[ownerIndex]
	clients := make([]*transport.DriverClient, writers)
	for i := range clients {
		client := tc.dial(holderIndex)
		clients[i] = client
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Logf("closing a writer's connection: %v", err)
			}
		})
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	owner.wal.SetFsyncHook(func() { <-release })
	t.Cleanup(func() {
		letGo()
		owner.wal.SetFsyncHook(nil)
	})

	var answered sync.WaitGroup
	for i, u := range users {
		payload, err := bson.Marshal(func() FieldRequest {
			req := clusterUserField(u, "Feed")
			req.Data = map[string]interface{}{"n": u}
			return req
		}())
		if err != nil {
			t.Fatal(err)
		}
		answered.Add(1)
		go func(client *transport.DriverClient) {
			defer answered.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			frame, err := client.SendRequest(ctx, transport.OpFieldPush, payload)
			if err != nil || frame.Opcode == transport.OpError {
				t.Errorf("a forwarded push was not answered: %v %v", frame, err)
			}
		}(clients[i])
	}

	inFlight := internodeInFlight.WithLabelValues(ownerID, transport.LaneRequest.String())
	peak := 0.0
	for deadline := time.Now().Add(observationTime); time.Now().Before(deadline) && peak < writers; time.Sleep(20 * time.Millisecond) {
		var sample dto.Metric
		if err := inFlight.Write(&sample); err != nil {
			t.Fatal(err)
		}
		peak = max(peak, sample.GetGauge().GetValue())
	}
	letGo()
	answered.Wait()
	if peak <= oldRequestLane {
		t.Fatalf("%d clients wrote through the holder while the owner's fsync was held and the holder had %.0f forwards in flight to it at most; "+
			"the request lane caps them at %d, so %d wait for a lane slot", writers, peak, oldRequestLane, writers-oldRequestLane)
	}
}
