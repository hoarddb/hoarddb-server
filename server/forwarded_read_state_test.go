package server

import (
	"errors"
	"os"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestForwardedReadDoesNotPersistReplicationState is T-16 (query-pipelining
// spec §8, P-2): a forwarded GET changes no replication position, so the
// owner must not write one to answer it. Rewriting the state file took a
// node-wide lock and a file write per forwarded read; the positions now live
// in the store, and the check is that the store takes no write under their
// namespace and no file appears.
func TestForwardedReadDoesNotPersistReplicationState(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	const coordinator = 0
	owner := tc.nodes[1].server.core.cluster
	bucket := bucketNameOwnedBy(t, tc.nodes[coordinator].server.core.cluster, "app", tc.addrs[1])
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	tc.ok(coordinator, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: bucket, Type: BucketTypeHash})
	tc.ok(coordinator, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": int32(1)}})

	contentsBefore, versionsBefore := replicationNamespaceDump(t, owner.s.kv)
	for i := 0; i < 3; i++ {
		answer := tc.ok(coordinator, transport.OpGet, FindRequest{Database: "app", Bucket: bucket, Key: "k"})
		if answer.Opcode != transport.OpGetResponse {
			t.Fatalf("the forwarded GET answered %s", answer.Opcode)
		}
	}
	contentsAfter, versionsAfter := replicationNamespaceDump(t, owner.s.kv)
	if contentsAfter != contentsBefore || versionsAfter != versionsBefore {
		t.Errorf("three forwarded GETs wrote to the owner's replication positions (versions %d -> %d): "+
			"a read changes no replication position, so answering it must not persist one (P-2)", versionsBefore, versionsAfter)
	}
	if _, err := os.Stat(legacyStatePathOf(owner)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the owner has a replication state file after forwarded reads (stat: %v)", err)
	}
}
