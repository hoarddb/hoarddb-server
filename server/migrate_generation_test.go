package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// noGenerationSourceConn blanks the database generation of every
// MIGRATE_START answer read through it, standing in for a source from before
// the generations build: a source whose catalog entry carries no db_gen.
type noGenerationSourceConn struct {
	peerConn
}

func (c *noGenerationSourceConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	frame, err := c.peerConn.SendRequest(ctx, lane, op, payload)
	if err != nil || op != transport.OpMigrateStart {
		return frame, err
	}
	var answer MigrateStartResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		return nil, fmt.Errorf("decoding the migrate start answer to drop its generation: %w", err)
	}
	answer.DatabaseGeneration = 0
	rewritten, err := bson.Marshal(answer)
	if err != nil {
		return nil, fmt.Errorf("encoding the migrate start answer without a generation: %w", err)
	}
	copied := *frame
	copied.Payload = rewritten
	return &copied, nil
}

// TestAPullFromASourceWithoutAGenerationStampsTheLegacyOne: a source from
// before the generations build answers MIGRATE_START with db_gen 0. The pull
// must stamp the bucket it lands with legacyGeneration(database) — the one
// representation every reader (entryGeneration, the drop witness, the marker)
// agrees on — not carry the source's 0 into the catalog entry.
//
// Guard: the pull writes the raw start generation into the catalog entry.
func TestAPullFromASourceWithoutAGenerationStampsTheLegacyOne(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	const database = "legacy-pull"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: testBucket, Type: BucketTypeHash})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(database, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	targetIndex := -1
	for i := range tc.addrs {
		if i != ownerIndex {
			targetIndex = i
			break
		}
	}
	if targetIndex < 0 {
		t.Fatal("no non-owner node to act as the migration target")
	}
	tc.ok(ownerIndex, transport.OpPut, InsertRequest{
		Database: database, Bucket: testBucket, Key: "k", Data: map[string]interface{}{"n": 1},
	})

	target := clusterOf(tc, targetIndex)
	wrapPeerConn(target.peerFor(owner), func(inner peerConn) peerConn {
		return &noGenerationSourceConn{peerConn: inner}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.pullBucket(ctx, database, testBucket, owner); err != nil {
		t.Fatalf("pull from a source that sent no generation: %v", err)
	}
	entry, catalogued, err := tc.nodes[targetIndex].server.core.readCatalogEntry(database, testBucket)
	if err != nil {
		t.Fatalf("reading the migrated catalog entry: %v", err)
	}
	if !catalogued {
		t.Fatal("the pull landed no catalog entry")
	}
	if want := legacyGeneration(database); entry.DatabaseGeneration != want {
		t.Errorf("a pull from a source that sent no generation stamped db_gen %d; want the legacy generation %d", entry.DatabaseGeneration, want)
	}
}
