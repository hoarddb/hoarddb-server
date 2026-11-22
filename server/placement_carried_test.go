package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCreateOnAnExistingBucketNamesTheRecordedPlacement is the step 12a gap
// closed in 12b: a create that asks for a placement the existing bucket does
// not have answers success (a replay takes this path) but says it asked for
// that placement and it was not changed, as the replication-factor path does,
// and names the recreate that does change it.
func TestCreateOnAnExistingBucketNamesTheRecordedPlacement(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "S", BucketTypeHash, "spread"); !resp.Success {
		t.Fatalf("first create: %+v", resp)
	}

	resp := createPlacementBucket(t, s, "S", BucketTypeHash, "whole")
	if !resp.Success || resp.Placement != "spread" {
		t.Fatalf("a create asking whole of a spread bucket answered %+v, want success reporting the recorded spread", resp)
	}
	for _, want := range []string{"already exists", "Placement is spread", "asked for whole", "not changed",
		"dump --out ./dump-S app S", "drop bucket S in app", "create bucket S in app { placement: whole }", "restore --dir ./dump-S"} {
		if !strings.Contains(resp.Message, want) {
			t.Errorf("the answer lacks %q:\n%s", want, resp.Message)
		}
	}

	for _, same := range []string{"spread"} {
		quiet := createPlacementBucket(t, s, "S", BucketTypeHash, same)
		if !quiet.Success || strings.Contains(quiet.Message, "asked for") {
			t.Errorf("a create asking %q of a spread bucket answered %+v, want the plain already-exists answer", same, quiet)
		}
	}
}

// TestPullKeepsThePlacementOfTheSource guards the one line of pullBucketWith
// that copies the source's placement into the pulled catalog entry (step 12a
// carried gap): a bucket moved by a pull keeps whole and keeps spread.
func TestPullKeepsThePlacementOfTheSource(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	for _, want := range []string{"whole", "spread"} {
		bucket := "pulled-" + want
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash,
			Options: map[string]interface{}{"placement": want}})
		owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
		if err != nil {
			t.Fatalf("owner of %s: %v", bucket, err)
		}
		ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
		targetIndex := (ownerIndex + 1) % len(tc.addrs)
		tc.ok(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})

		target := tc.nodes[targetIndex].server.core
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = target.cluster.pullBucket(ctx, testDatabase, bucket, mustNode(t, target.cluster, owner.ID))
		cancel()
		if err != nil {
			t.Fatalf("pullBucket(%s): %v", bucket, err)
		}
		stats, err := target.BucketStats(BucketStatsRequest{Database: testDatabase, Bucket: bucket})
		if err != nil || !stats.Found {
			t.Fatalf("BucketStats(%s) on the target = %+v, %v", bucket, stats, err)
		}
		if stats.Placement != want {
			t.Errorf("a pulled %s bucket describes placement %q on the target, want %q", want, stats.Placement, want)
		}
		entry, found, err := target.readCatalogEntry(testDatabase, bucket)
		if err != nil || !found || entry.Placement != want {
			t.Errorf("the target's catalog entry for the pulled %s bucket records placement %q (found %v, %v), want %q", want, entry.Placement, found, err, want)
		}
	}
}
