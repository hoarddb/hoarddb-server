package server

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The typed_fields feature, create half (typed-fields spec §8.3, OC-1; build
// doc D-1, TF-5 step 5.6): an older member would create a typed bucket
// undeclared and store every write the owner refuses, so a typed create is
// refused while a member that has gossiped does not advertise the feature.

// TestTypedCreateIsRefusedWhileAMemberLacksTypedFields: refused in-band, as a
// create the cluster cannot honour is (E-19), before it is logged; a create
// without a declaration is unaffected.
func TestTypedCreateIsRefusedWhileAMemberLacksTypedFields(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	stale := stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) {
		c.memberFeatures[stale] = map[string]bool{featureBucketRF: true, featureMemtableMB: true}
	})

	for _, typed := range []struct {
		prefix  string
		options map[string]interface{}
	}{
		{"typed", map[string]interface{}{"fields": bson.A{bson.M{"name": "Age", "type": "int"}}}},
		{"moded", map[string]interface{}{"field_types": "warn"}},
	} {
		bucket := bucketOwnedBy(t, tc, 0, typed.prefix)
		answer := tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash, Options: typed.options})
		var resp CreateBucketResponse
		if err := bson.Unmarshal(answer.Payload, &resp); err != nil || answer.Opcode != transport.OpCreateBucket {
			t.Fatalf("a %s create while %s lacks typed_fields was answered %s (%v); want an in-band refusal", typed.prefix, stale, answer.Opcode, err)
		}
		if resp.Success {
			t.Errorf("a %s create while %s lacks typed_fields was applied: %q", typed.prefix, stale, resp.Message)
		}
		for _, part := range []string{bucket, testDatabase, "typed fields need every cluster member", `member "` + stale + `"`, "Nothing was changed.", "Upgrade the cluster"} {
			if !strings.Contains(resp.Message, part) {
				t.Errorf("the refusal %q does not say %q", resp.Message, part)
			}
		}
		if _, catalogued, _ := tc.nodes[0].server.core.readCatalogEntry(testDatabase, bucket); catalogued {
			t.Errorf("the refused %s create was recorded on its owner", typed.prefix)
		}
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucketOwnedBy(t, tc, 0, "plain"), Type: BucketTypeHash})
}

// waitForCatalogEntry waits until node index of tc records bucket's entry.
func waitForCatalogEntry(t *testing.T, tc *testCluster, index int, bucket string) catalogEntry {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		entry, catalogued, err := tc.nodes[index].server.core.readCatalogEntry(testDatabase, bucket)
		if err != nil {
			t.Fatalf("reading node %d's entry: %v", index+1, err)
		}
		if catalogued {
			return entry
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d never recorded bucket %s", index+1, bucket)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestDeclarationReplicatesAtCreateAndAlter, the create half: every copy's
// catalog holds the declaration, because every copy applies the create's
// options as the owner does. TF-8 adds the alter half.
func TestDeclarationReplicatesAtCreateAndAlter(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const bucket = "typed_everywhere"
	createTypedOn(tc, 0, bucket, userFieldsOption(), "warn")
	for index := range tc.nodes {
		entry := waitForCatalogEntry(t, tc, index, bucket)
		if !reflect.DeepEqual(entry.Fields, userFieldsCanonical()) || entry.FieldTypes != "warn" {
			t.Errorf("node %d's catalog records %+v in mode %q; want the declaration in warn", index+1, entry.Fields, entry.FieldTypes)
		}
		if lookupBucket(tc.nodes[index].server.core, testDatabase, bucket).declaredFieldTypes() == nil {
			t.Errorf("node %d serves the bucket undeclared", index+1)
		}
	}
}
