package server

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// listingDatabase and soloDatabase are the two databases of listingCluster:
// several buckets at factor 1 and one on every node in the first, a single
// factor-1 bucket in the other, so on three nodes some node holds no copy of a
// bucket of the first, every node but one holds nothing of the second
// (BUG-0468's evidence), and a count of held copies counts the shared bucket
// three times.
const (
	listingDatabase = "app"
	soloDatabase    = "solo"
	sharedBucket    = "shared"
	listingBuckets  = 7
)

// listingCluster is a three-node cluster with listingBuckets factor-1 buckets
// in listingDatabase, one in soloDatabase, and three documents in the first
// bucket. It returns the bucket names of listingDatabase, sorted.
func listingCluster(t *testing.T) (*testCluster, []string) {
	t.Helper()
	tc := newTestCluster(t, 3, 2)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: listingDatabase})
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: soloDatabase})
	names := make([]string, 0, listingBuckets)
	for i := range listingBuckets - 1 {
		name := fmt.Sprintf("b%d", i)
		names = append(names, name)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
			Database: listingDatabase, Name: name, Type: BucketTypeHash, ReplicationFactor: 1,
		})
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: listingDatabase, Name: sharedBucket, Type: BucketTypeHash, ReplicationFactor: 3,
	})
	names = append(names, sharedBucket)
	sort.Strings(names)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: soloDatabase, Name: "only", Type: BucketTypeHash, ReplicationFactor: 1,
	})
	for _, key := range []string{"k1", "k2", "k3"} {
		tc.ok(0, transport.OpPut, InsertRequest{
			Database: listingDatabase, Bucket: "b0", Key: key, Data: map[string]interface{}{"key": key},
		})
	}
	requireSomeNodeHoldsNoCopy(t, tc, names)
	return tc, names
}

// requireSomeNodeHoldsNoCopy fails the test when every node holds every
// bucket: then a node's own listing is the cluster's, and the test proves
// nothing.
func requireSomeNodeHoldsNoCopy(t *testing.T, tc *testCluster, names []string) {
	t.Helper()
	for _, n := range tc.nodes {
		for _, name := range names {
			if !n.server.core.bucketStoreOpen(listingDatabase, name) {
				return
			}
		}
	}
	t.Fatalf("every node holds every bucket of %q at factor 1: the placement leaves the test vacuous", listingDatabase)
}

// listBucketsAt asks node index for listingDatabase's buckets.
func listBucketsAt(t *testing.T, tc *testCluster, index int, database string) ListBucketsResponse {
	t.Helper()
	frame := tc.ok(index, transport.OpListBuckets, ListBucketsRequest{Database: database})
	var resp ListBucketsResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decode list buckets from node %d: %v", index+1, err)
	}
	return resp
}

// TestListBucketsIsClusterWide is BUG-0468: every node lists the same full set
// of a database's buckets, including the ones it holds no copy of, with the
// entry count of a holder.
func TestListBucketsIsClusterWide(t *testing.T) {
	tc, names := listingCluster(t)
	defer tc.stop()

	for index := range tc.nodes {
		resp := listBucketsAt(t, tc, index, listingDatabase)
		listed := make([]string, 0, len(resp.Buckets))
		for _, b := range resp.Buckets {
			listed = append(listed, b.Name)
			want := int64(0)
			if b.Name == "b0" {
				want = 3
			}
			if b.Entries != want {
				t.Errorf("node %d lists bucket %q with %d entries, want %d", index+1, b.Name, b.Entries, want)
			}
		}
		if !slices.Equal(listed, names) {
			t.Errorf("node %d lists %v in %q, want every bucket of the cluster %v", index+1, listed, listingDatabase, names)
		}

		solo := listBucketsAt(t, tc, index, soloDatabase)
		if len(solo.Buckets) != 1 || solo.Buckets[0].Name != "only" {
			t.Errorf("node %d lists %+v in %q, want the one bucket \"only\"", index+1, solo.Buckets, soloDatabase)
		}
	}
}

// TestListDatabasesIsClusterWide is BUG-0468: every node lists the same
// databases with the same bucket count, the cluster's.
func TestListDatabasesIsClusterWide(t *testing.T) {
	tc, _ := listingCluster(t)
	defer tc.stop()

	want := []DatabaseInfo{
		{Name: listingDatabase, BucketCount: listingBuckets},
		{Name: soloDatabase, BucketCount: 1},
	}
	for index := range tc.nodes {
		frame := tc.ok(index, transport.OpListDBs, ListDatabasesRequest{})
		var resp ListDatabasesResponse
		if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
			t.Fatalf("decode list databases from node %d: %v", index+1, err)
		}
		if !slices.Equal(resp.Databases, want) {
			t.Errorf("node %d lists databases %+v, want %+v", index+1, resp.Databases, want)
		}
	}
}

// TestDatabaseDumpListsEveryBucketOfTheCluster is BUG-0468 for a database
// dump: the CLI enumerates a database's buckets with LIST_BUCKETS at the node
// it reached and starts one dump per bucket there (server/dump.go:
// DumpStartRequest), so a listing of the node's own copies dumped part of the
// database and said nothing.
func TestDatabaseDumpListsEveryBucketOfTheCluster(t *testing.T) {
	tc, names := listingCluster(t)
	defer tc.stop()

	for index := range tc.nodes {
		var dumped []string
		for _, b := range listBucketsAt(t, tc, index, listingDatabase).Buckets {
			frame := tc.ok(index, transport.OpDumpStart, DumpStartRequest{Database: listingDatabase, Bucket: b.Name})
			var started DumpStartResponse
			if err := bson.Unmarshal(frame.Payload, &started); err != nil {
				t.Fatalf("decode dump start of %q through node %d: %v", b.Name, index+1, err)
			}
			if !started.Success || started.Manifest == nil || started.Manifest.Bucket != b.Name {
				t.Fatalf("dump start of %q through node %d answered %+v", b.Name, index+1, started)
			}
			// A server runs one dump at a time: release this one before the next.
			tc.ok(index, transport.OpDumpDone, DumpDoneRequest{Database: listingDatabase, Bucket: b.Name})
			dumped = append(dumped, b.Name)
		}
		sort.Strings(dumped)
		if !slices.Equal(dumped, names) {
			t.Errorf("a dump of %q through node %d covers %v, want every bucket %v", listingDatabase, index+1, dumped, names)
		}
	}
}

// TestClusterListingRefusesWhenAMemberDoesNotAnswer is the owner's ruling on
// BUG-0468: a listing the cluster cannot complete is refused, naming the
// member that did not answer and saying retry, not answered short.
func TestClusterListingRefusesWhenAMemberDoesNotAnswer(t *testing.T) {
	tc, _ := listingCluster(t)
	defer tc.stop()

	down := 2
	stopped := tc.addrs[down]
	_ = tc.nodes[down].client.Close()
	tc.nodes[down].client = nil
	if err := tc.nodes[down].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", down+1, err)
	}
	tc.nodes[down].server = nil

	for _, request := range []struct {
		op  transport.Opcode
		req any
	}{
		{op: transport.OpListBuckets, req: ListBucketsRequest{Database: listingDatabase}},
		{op: transport.OpListDBs, req: ListDatabasesRequest{}},
	} {
		frame := tc.frame(0, request.op, request.req)
		if frame.Opcode != transport.OpError {
			t.Errorf("%s with member %s stopped answered %s, want a refusal", request.op, stopped, frame.Opcode)
			continue
		}
		var refusal ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
			t.Fatalf("decode %s refusal: %v", request.op, err)
		}
		if !strings.Contains(refusal.Message, stopped) || !strings.Contains(strings.ToLower(refusal.Message), "retry") {
			t.Errorf("%s refusal %q does not name the member %s that did not answer and say retry", request.op, refusal.Message, stopped)
		}
	}
}

// driverDefaultRequestTimeout is the Go driver's default per-request deadline
// (the driver spec §5.2, RequestTimeout).
const driverDefaultRequestTimeout = 10 * time.Second

// TestClusterListingRefusesAHungMemberWithinTheDriverDeadline is the
// verifier's rejection of BUG-0468's first fix: a member that is connected but
// does not answer held the listing for the whole fan-out bound, which equalled
// the driver's default deadline, so the client got a local timeout and never
// the refusal that names the member. The member is hung by holding its data
// lock, which its catalog walk needs.
func TestClusterListingRefusesAHungMemberWithinTheDriverDeadline(t *testing.T) {
	tc, _ := listingCluster(t)
	defer tc.stop()

	hung := 2
	hungID := tc.addrs[hung]
	// The member answers once first, so the coordinator holds a live
	// connection to it: the hang is in the member, not in a dial.
	listBucketsAt(t, tc, 0, listingDatabase)
	data := tc.nodes[hung].server.core.serverDataAccessor()
	data.mu.Lock()
	released := false
	release := func() {
		if !released {
			released = true
			data.mu.Unlock()
		}
	}
	defer release()

	for _, request := range []struct {
		op  transport.Opcode
		req any
	}{
		{op: transport.OpListBuckets, req: ListBucketsRequest{Database: listingDatabase}},
		{op: transport.OpListDBs, req: ListDatabasesRequest{}},
	} {
		payload, err := bson.Marshal(request.req)
		if err != nil {
			t.Fatalf("marshal %s: %v", request.op, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), driverDefaultRequestTimeout)
		frame, err := tc.nodes[0].client.SendRequest(ctx, request.op, payload)
		cancel()
		if err != nil {
			t.Fatalf("%s with member %s hung got no answer within the driver's default %s: %v",
				request.op, hungID, driverDefaultRequestTimeout, err)
		}
		if frame.Opcode != transport.OpError {
			t.Fatalf("%s with member %s hung answered %s, want a refusal", request.op, hungID, frame.Opcode)
		}
		refusal := refusalOf(t, frame)
		if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
			t.Errorf("%s refusal has code %d, want %d (UNAVAILABLE): %q", request.op, refusal.Code, transport.ErrUnavailable, refusal.Message)
		}
		if !strings.Contains(refusal.Message, hungID) || !strings.Contains(strings.ToLower(refusal.Message), "retry") {
			t.Errorf("%s refusal %q does not name the hung member %s and say retry", request.op, refusal.Message, hungID)
		}
	}
	release()
}
