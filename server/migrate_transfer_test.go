package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A source serves a pull's chunks and verifies its digest only for
// the pull it accepted at MIGRATE_START: every check a pull gets is made there,
// so a later request of the same exchange that is not tied to that start would
// get none of them.

// migrateExchange sends one request of a pull to the source over from's own
// internode session, which holds the cluster token, and decodes the answer.
func migrateExchange(t *testing.T, tc *testCluster, from, source int, op transport.Opcode, request, response any) {
	t.Helper()
	sourceNode := tc.nodes[source].server.core.cluster.self
	conn, err := tc.nodes[from].server.core.cluster.peerFor(sourceNode).dial(context.Background())
	if err != nil {
		t.Fatalf("node %d dialling node %d: %v", from, source, err)
	}
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encoding %s: %v", op, err)
	}
	frame, err := conn.SendRequest(context.Background(), transport.LaneRequest, op, payload)
	if err != nil {
		t.Fatalf("node %d sending %s to node %d: %v", from, op, source, err)
	}
	if err := bson.Unmarshal(frame.Payload, response); err != nil {
		t.Fatalf("decoding the %s answer: %v", op, err)
	}
}

// TestSourceServesAChunkOnlyToThePullItAccepted opens a pull of a sealed
// node's bucket for a sealed member, then asks the source for the first chunk
// in requests that are not that pull's: from a plaintext member, and from the
// accepted member's own session without what ties the request to the pull.
// Each must be refused with no bytes, and the accepted pull must not be spoilt.
func TestSourceServesAChunkOnlyToThePullItAccepted(t *testing.T) {
	tc := newMixedCluster(t, 1, "on", "on", "")
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	source := tc.nodes[0].server.core
	bucket := bucketPlaced(t, source.cluster, "transfer", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "one"})
	defer clearAllPending(source.cluster, migrateKey(testDatabase, bucket))

	var start MigrateStartResponse
	migrateExchange(t, tc, 1, 0, transport.OpMigrateStart, MigrateStartRequest{
		Token: tc.token, Database: testDatabase, Bucket: bucket, FromNode: tc.addrs[1], Store: claimSealed,
		Incarnation: openedStream(t, tc.nodes[1].server.core.cluster, tc.addrs[0]).Incarnation, Epoch: tc.nodes[0].server.core.cluster.Epoch(),
	}, &start)
	if !start.Ok || start.NumChunks == 0 {
		t.Fatalf("the sealed member's pull was not accepted: %+v", start)
	}

	for _, stranger := range []struct {
		name string
		from int
	}{
		{name: "a plaintext member", from: 2},
		{name: "the accepted member, outside its pull", from: 1},
	} {
		t.Run(stranger.name, func(t *testing.T) {
			var chunk MigrateDataResponse
			migrateExchange(t, tc, stranger.from, 0, transport.OpMigrateData, MigrateDataRequest{
				Token: tc.token, Database: testDatabase, Bucket: bucket, Index: 0,
			}, &chunk)
			if chunk.Ok || len(chunk.Data) > 0 {
				t.Fatalf("the source served %d bytes of a chunk to a request that is not the pull it accepted (marker present: %v)",
					len(chunk.Data), bytes.Contains(chunk.Data, []byte(plaintextMarker)))
			}
			t.Logf("refused with: %s", chunk.Error)
		})
	}
	if source.cluster.pendingCount(migrateKey(testDatabase, bucket)) == 0 {
		t.Fatalf("a refused request discarded the snapshot the accepted pull is reading")
	}
}

// TestPullRequestsAreAnsweredOnlyForTheIssuedTransfer drives the requests of
// one accepted pull by hand: without the transfer id (a node from before the
// check), with one the source did not issue, and with the right one. The first
// two are refused, with the upgrade named for the first, the snapshot and the
// source's copy are kept, and the refusal is audited; the third is served and
// completes.
func TestPullRequestsAreAnsweredOnlyForTheIssuedTransfer(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	tc := newTestClusterWith(t, 2, 1, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		sealNode(t, cfg, "on", byte(0x40+0x10*index))
		if index == 0 {
			cfg.Audit = config.AuditConfig{Enabled: true, Output: auditPath}
		}
	})
	stopped := false
	defer func() {
		if !stopped {
			tc.stop()
		}
	}()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	source := tc.nodes[0].server.core
	bucket := bucketPlaced(t, source.cluster, "issued", ownedBy(tc.addrs[0]))
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "one"})

	var start MigrateStartResponse
	migrateExchange(t, tc, 1, 0, transport.OpMigrateStart, MigrateStartRequest{
		Token: tc.token, Database: testDatabase, Bucket: bucket, FromNode: tc.addrs[1], Store: claimSealed,
		Incarnation: openedStream(t, tc.nodes[1].server.core.cluster, tc.addrs[0]).Incarnation, Epoch: tc.nodes[0].server.core.cluster.Epoch(),
	}, &start)
	if !start.Ok || start.Transfer == "" {
		t.Fatalf("the accepted pull carries no transfer id: %+v", start)
	}

	chunkWith := func(transfer string) MigrateDataResponse {
		t.Helper()
		var chunk MigrateDataResponse
		migrateExchange(t, tc, 1, 0, transport.OpMigrateData, MigrateDataRequest{
			Token: tc.token, Database: testDatabase, Bucket: bucket, Index: 0, Transfer: transfer,
		}, &chunk)
		return chunk
	}
	doneWith := func(transfer, digest string) MigrateDoneResponse {
		t.Helper()
		var done MigrateDoneResponse
		migrateExchange(t, tc, 1, 0, transport.OpMigrateDone, MigrateDoneRequest{
			Token: tc.token, Database: testDatabase, Bucket: bucket, Digest: digest, Transfer: transfer,
		}, &done)
		return done
	}

	for _, refused := range []struct {
		name, transfer, want string
	}{
		{name: "a node from before the check", transfer: "", want: "Upgrade that node"},
		{name: "an id the source did not issue", transfer: start.Transfer + "x", want: "did not accept"},
	} {
		t.Run(refused.name, func(t *testing.T) {
			chunk := chunkWith(refused.transfer)
			if chunk.Ok || len(chunk.Data) > 0 {
				t.Fatalf("MIGRATE_DATA from %s was served %d bytes", refused.name, len(chunk.Data))
			}
			if !strings.Contains(chunk.Error, refused.want) {
				t.Errorf("the chunk refusal does not say %q: %s", refused.want, chunk.Error)
			}
			done := doneWith(refused.transfer, "")
			if done.Ok || !strings.Contains(done.Error, refused.want) {
				t.Errorf("MIGRATE_DONE from %s was not refused saying %q: %+v", refused.name, refused.want, done)
			}
			if source.cluster.pendingCount(migrateKey(testDatabase, bucket)) == 0 {
				t.Fatalf("a refused request discarded the accepted pull's snapshot")
			}
		})
	}

	chunk := chunkWith(start.Transfer)
	if !chunk.Ok || !bytes.Contains(chunk.Data, []byte(plaintextMarker)) {
		t.Fatalf("the accepted pull's own chunk request was not served: %+v", chunk.Error)
	}
	var stream []byte
	for i := 0; i < start.NumChunks; i++ {
		var next MigrateDataResponse
		migrateExchange(t, tc, 1, 0, transport.OpMigrateData, MigrateDataRequest{
			Token: tc.token, Database: testDatabase, Bucket: bucket, Index: i, Transfer: start.Transfer,
		}, &next)
		stream = append(stream, next.Data...)
	}
	digest := sha256.Sum256(stream)
	if done := doneWith(start.Transfer, hex.EncodeToString(digest[:])); !done.Ok {
		t.Fatalf("the accepted pull's MIGRATE_DONE was refused: %s", done.Error)
	}
	if !locallyFound(t, tc.nodes[0], bucket, "k") {
		t.Errorf("the source, still the bucket's owner, lost its copy")
	}

	tc.stop()
	stopped = true
	auditLines, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("reading the source's audit log: %v", err)
	}
	if !bytes.Contains(auditLines, []byte(`"event":"authz.denied"`)) ||
		!bytes.Contains(auditLines, []byte(`"target":"bucket:`+testDatabase+"."+bucket+`"`)) {
		t.Errorf("the refused pull request is not in the source's audit log:\n%s", auditLines)
	}
}

// TestAPullStillLandsSealedAndPlain is the pull primitive end to end, on an
// encrypted pair and on a plaintext pair: the target lands the bucket and the
// source, still its owner, keeps its copy.
func TestAPullStillLandsSealedAndPlain(t *testing.T) {
	for _, pair := range []struct {
		name string
		mode string
	}{
		{name: "sealed", mode: "on"},
		{name: "plain", mode: ""},
	} {
		t.Run(pair.name, func(t *testing.T) {
			tc := newMixedCluster(t, 1, pair.mode, pair.mode)
			defer tc.stop()
			tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			source := tc.nodes[0].server.core.cluster
			bucket := bucketPlaced(t, source, "lands", ownedBy(tc.addrs[0]))
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
			tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: markerDocument(), WriteConcern: "one"})

			target := tc.nodes[1].server.core.cluster
			if err := target.pullBucket(context.Background(), testDatabase, bucket, source.self); err != nil {
				t.Fatalf("pulling the bucket: %v", err)
			}
			if !locallyFound(t, tc.nodes[1], bucket, "k") {
				t.Errorf("the pulled bucket does not hold the document on the target")
			}
			if !locallyFound(t, tc.nodes[0], bucket, "k") {
				t.Errorf("the source, still the bucket's owner, lost its copy")
			}
		})
	}
}
