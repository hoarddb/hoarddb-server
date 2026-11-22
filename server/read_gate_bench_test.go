package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BenchmarkOwnerReadGateLineageCache measures the owner read gate on a bucket
// whose lineage cache starts empty (BUG-0566): steady-state is the gate after
// the first read, and forced-miss empties the cache before every read.
func BenchmarkOwnerReadGateLineageCache(b *testing.B) {
	tc := newTestCluster(b, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	c := clusterOf(tc, 0)
	key := placementOf(testDatabase, testBucket)
	served, err := c.s.resolveBucket(testDatabase, testBucket)
	if err != nil {
		b.Fatalf("resolving the bucket: %v", err)
	}
	epoch := c.Epoch()
	b.Run("steady-state", func(b *testing.B) {
		served.rec.lineageFlags.Store(0)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := c.ownerReadAwaitsTakeOver(key, epoch, testDatabase, testBucket); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("forced-miss", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			served.rec.lineageFlags.Store(0)
			if _, err := c.ownerReadAwaitsTakeOver(key, epoch, testDatabase, testBucket); err != nil {
				b.Fatal(err)
			}
		}
	})
}
