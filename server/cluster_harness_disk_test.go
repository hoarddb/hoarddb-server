package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterHarnessIgnoresTheHostDisk is BUG-0553's test. A mark of 1% is
// below any real filesystem's use, so a node that read the host's disk through
// the real statfs would report full and refuse every pull (BUG-0104). The
// harness pins its nodes to a healthy reading, so neither may happen.
func TestClusterHarnessIgnoresTheHostDisk(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.DiskHighWaterPct = 1 })
	defer tc.stop()

	for index, node := range tc.nodes {
		if dir, st, full := node.server.core.fullDisk(); full {
			t.Errorf("node %d reports %s full at %.0f%% used: the harness reads the host's disk", index+1, dir, st.usedPercent())
		}
	}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner, err := clusterOf(tc, 0).Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "doc", Data: map[string]interface{}{"n": 1}})

	target := clusterOf(tc, (ownerIndex+1)%len(tc.nodes))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.pullBucket(ctx, testDatabase, testBucket, mustNode(t, target, owner.ID)); err != nil {
		t.Fatalf("a pull on a harness node failed (ErrMemberDiskFull=%v): %v", errors.Is(err, ErrMemberDiskFull), err)
	}
}
