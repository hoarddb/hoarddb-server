package main

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/server"
)

// referenceBucket is the bucket a placement form puts a user in, written out
// again here from the spec (6.2) with the xxhash package and fmt alone, so a
// test that passes proves the engine agrees with the spec and not with itself.
func referenceBucket(placement string) func(key string) string {
	if placement != "h64" {
		return func(string) string { return "User" }
	}
	return func(key string) string { return fmt.Sprintf("User_%02d", xxhash.Sum64String(key)%64) }
}

// TestShardFunctionIsStable pins xxHash64(user id) mod 64 to fixed vectors. A
// change to the shard function moves every user to another bucket, so a dataset
// seeded before the change would be unreadable after it.
func TestShardFunctionIsStable(t *testing.T) {
	vectors := []struct {
		id      string
		hash    uint64
		shard64 int
	}{
		{"0", 7148434200721666028, 44},
		{"1", 13237225503670494420, 20},
		{"2", 6927017134761466251, 11},
		{"7", 1750302349509622455, 55},
		{"42", 7919287270473417401, 57},
		{"63", 1473612911177040125, 61},
		{"64", 1657817252226316262, 38},
		{"99", 14603841130323969312, 32},
		{"1000", 7684424893633776307, 51},
		{"9999", 12785434117683075637, 53},
		{"99999", 10628936318485420206, 46},
		{"100000", 3439474042126938805, 53},
		{"999999", 1652424797190735410, 50},
	}
	for _, vector := range vectors {
		if got := (ring.XXHasher{}).Sum64([]byte(vector.id)); got != vector.hash {
			t.Errorf("the ring hashes %q to %d; the vector says %d, so the vector is not the ring's hash", vector.id, got, vector.hash)
		}
		if got := shardOf(vector.id, 64); got != vector.shard64 {
			t.Errorf("shardOf(%q, 64) = %d; want %d", vector.id, got, vector.shard64)
		}
	}
	if got := shardOf("99", 256); got != 32 {
		t.Errorf("shardOf(\"99\", 256) = %d; want 32 (the same hash, taken mod 256)", got)
	}
}

// TestPlacementBucketNames: H-1 is the one User bucket, H-64 is User_00 to User_63.
func TestPlacementBucketNames(t *testing.T) {
	if got := placementOfShards(1).buckets(); !reflect.DeepEqual(got, []string{"User"}) {
		t.Errorf("H-1 buckets = %v; want [User]", got)
	}
	sharded := placementOfShards(64).buckets()
	if len(sharded) != 64 || sharded[0] != "User_00" || sharded[9] != "User_09" || sharded[63] != "User_63" {
		t.Errorf("H-64 buckets = %v; want User_00 to User_63", sharded)
	}
	for _, id := range []string{"0", "1", "42", "99999"} {
		user := 0
		if _, err := fmt.Sscan(id, &user); err != nil {
			t.Fatal(err)
		}
		if got, want := placementOfShards(64).bucketOf(user), referenceBucket("h64")(id); got != want {
			t.Errorf("user %s is routed to %s; the spec's shard function says %s", id, got, want)
		}
		if got := placementOfShards(1).bucketOf(user); got != "User" {
			t.Errorf("user %s under H-1 is routed to %s; want User", id, got)
		}
	}
}

// TestH64CreatesSixtyFourBucketsFromTheUserDeclaration: setup creates exactly
// the 64 shard buckets, each from the same User declaration as H-1, hash typed,
// and no bucket named User.
func TestH64CreatesSixtyFourBucketsFromTheUserDeclaration(t *testing.T) {
	fake := newFakeServer()
	engine := fakeHoardEngineOn(&config{Engine: engineHoardDB, Database: "social", Placement: "h64", OpTimeout: 1}, fake)
	info, err := engine.setupSocial(context.Background(), tinySpec)
	if err != nil || info.Layout != "nested-stores-H-64" || info.Buckets != 64+outboxBuckets {
		t.Errorf("setup = %+v, %v; want layout nested-stores-H-64 and %d buckets", info, err, 64+outboxBuckets)
	}
	var users []server.CreateBucketRequest
	for _, request := range fake.created {
		if strings.HasPrefix(request.Name, "User") {
			users = append(users, request)
		}
	}
	if len(users) != 64 {
		t.Fatalf("setup created %d User buckets; want 64", len(users))
	}
	// Buckets are created concurrently, so the order they arrive in is not fixed.
	slices.SortFunc(users, func(left, right server.CreateBucketRequest) int { return strings.Compare(left.Name, right.Name) })
	for index, request := range users {
		if want := fmt.Sprintf("User_%02d", index); request.Name != want {
			t.Errorf("bucket %d is %q; want %q", index, request.Name, want)
		}
		if request.Type != server.BucketTypeHash || !reflect.DeepEqual(request.Options["stores"], hoardStoreDeclarations()) {
			t.Errorf("bucket %s was not created from the User declaration: type %v, options %v", request.Name, request.Type, request.Options)
		}
	}
}

// TestH64EveryOperationRoutesViaItsShard runs all sixteen operations against a
// fake that refuses a user addressed in any bucket but the one the spec's shard
// function names.
func TestH64EveryOperationRoutesViaItsShard(t *testing.T) {
	checkOperationContract(t, newFakeWorldPlaced(t, "h64"))
}

// TestH64CanonicalHashEqualsH1AndTheGenerator is the correctness rule of the
// placement: the same dataset hashes to the same canonical value whether it
// lives in one bucket or in 64, and to what the generator says it must.
func TestH64CanonicalHashEqualsH1AndTheGenerator(t *testing.T) {
	ranks := make([]int, tinySpec.Users)
	for rank := range ranks {
		ranks[rank] = rank
	}
	var hashes []string
	var worlds []*socialWorld
	for _, form := range []string{"h1", "h64"} {
		world := newFakeWorldPlaced(t, form)
		worlds = append(worlds, world)
		digest, err := readSample(context.Background(), world.engine, ranks, 4)
		if err != nil {
			t.Fatalf("%s: reading the sample: %v", form, err)
		}
		hashes = append(hashes, digest.hash)
	}
	expected, err := canonicalHash(graphCanonical(worlds[0].graph, ranks))
	if err != nil {
		t.Fatal(err)
	}
	if hashes[0] != expected || hashes[1] != expected {
		t.Errorf("canonical hashes: H-1 %s, H-64 %s, generator %s; want all three equal", hashes[0], hashes[1], expected)
	}
	var out strings.Builder
	if err := parityOn(context.Background(), worlds[1].engine.cfg, worlds[1].engine, worlds[1].graph, &out); err != nil || !strings.Contains(out.String(), `"matches_generator":true`) {
		t.Errorf("parity-hash over H-64: %v, %s; want matches_generator true", err, out.String())
	}
}

// shardsOfKeys is the shard of each key, in order.
func shardsOfKeys(keys []string) []int {
	shards := make([]int, len(keys))
	for index, key := range keys {
		shards[index] = int(xxhash.Sum64String(key) % 64)
	}
	return shards
}

// TestFanOutGroupsByShard: a post's feed copies go out grouped by shard, one
// request after another, and every follower still gets exactly one.
func TestFanOutGroupsByShard(t *testing.T) {
	world := newFakeWorldPlaced(t, "h64")
	author := world.busiestAuthor()
	followers := world.followersOf(author)
	var wantKeys []string
	for _, follower := range followers {
		wantKeys = append(wantKeys, userID(follower))
	}
	if len(slices.Compact(shardsOfKeysSorted(wantKeys))) < 2 {
		t.Fatalf("the followers of user %d sit in one shard; the test cannot tell grouped from ungrouped", author)
	}
	world.fake.touched = nil
	post := postRecord{Author: userID(author), Seq: 9999, Text: "grouped", Tags: []string{"x"}}
	fanout, err := world.ops(t).publishPost(context.Background(), post, noLedger{})
	if err != nil || fanout != len(followers) {
		t.Fatalf("publishPost = %d, %v; want %d copies", fanout, err, len(followers))
	}
	var fedKeys []string
	for _, touch := range world.fake.touched {
		if touch.field == "Feed" {
			fedKeys = append(fedKeys, touch.key)
		}
	}
	if !slices.IsSorted(shardsOfKeys(fedKeys)) {
		t.Errorf("feed copies went to shards %v; want them grouped in shard order", shardsOfKeys(fedKeys))
	}
	slices.Sort(fedKeys)
	slices.Sort(wantKeys)
	if !slices.Equal(fedKeys, wantKeys) {
		t.Errorf("feeds written %v; want each follower once: %v", fedKeys, wantKeys)
	}
}

func shardsOfKeysSorted(keys []string) []int {
	shards := shardsOfKeys(keys)
	slices.Sort(shards)
	return shards
}

// TestMergedTimelineGroupsByShardAndAnswersAsH1: the merge reads followees
// grouped by shard, serially, and returns the same posts as the H-1 form does.
func TestMergedTimelineGroupsByShardAndAnswersAsH1(t *testing.T) {
	followees := []int{3, 41, 7, 58, 12, 29, 50, 1, 33, 20}
	sharded := newFakeWorldPlaced(t, "h64")
	single := newFakeWorldPlaced(t, "h1")
	sharded.fake.touched = nil
	got, err := sharded.ops(t).mergedTimeline(context.Background(), followees)
	if err != nil {
		t.Fatal(err)
	}
	want, err := single.ops(t).mergedTimeline(context.Background(), followees)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(authorAndSeq(got), authorAndSeq(want)) {
		t.Errorf("H-64 merged timeline %v; H-1 %v", authorAndSeq(got), authorAndSeq(want))
	}
	var readKeys []string
	for _, touch := range sharded.fake.touched {
		if touch.field == "Posts" {
			readKeys = append(readKeys, touch.key)
		}
	}
	if len(readKeys) != len(followees) || !slices.IsSorted(shardsOfKeys(readKeys)) {
		t.Errorf("followees were read in shards %v (%d reads); want %d reads grouped in shard order", shardsOfKeys(readKeys), len(readKeys), len(followees))
	}
}

// nodeRotation puts bucket NN's replica set on two of three nodes, owner first.
func nodeRotation(bucket string) []string {
	var shard int
	if _, err := fmt.Sscanf(bucket, "User_%d", &shard); err != nil {
		return []string{"n1"}
	}
	nodes := []string{"n1", "n2", "n3"}
	return []string{nodes[shard%3], nodes[(shard+1)%3]}
}

// TestPlacementMapReadsDescribe: owners per node count the first node of each
// shard's replica set, in node id order, and the holders are those of the
// primary bucket, User_00.
func TestPlacementMapReadsDescribe(t *testing.T) {
	fake := newFakeServer()
	fake.nodesOf = nodeRotation
	engine := fakeHoardEngineOn(&config{Engine: engineHoardDB, Database: "social", Placement: "h64", OpTimeout: 1}, fake)
	got, err := engine.placementMap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.NodeIDs, []string{"n1", "n2", "n3"}) || !slices.Equal(got.Owners, []int{22, 21, 21}) || !slices.Equal(got.Holders, []string{"n1", "n2"}) {
		t.Errorf("placement map = %+v; want nodes n1 n2 n3, owners 22 21 21, holders n1 n2", got)
	}
}

// TestPlacementMapOfAStandaloneServer: a server that names no nodes owns every
// shard itself, and the map says so rather than coming back empty.
func TestPlacementMapOfAStandaloneServer(t *testing.T) {
	engine := fakeHoardEngineOn(&config{Engine: engineHoardDB, Database: "social", Placement: "h64", OpTimeout: 1}, newFakeServer())
	got, err := engine.placementMap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Owners, []int{64}) || len(got.NodeIDs) != 1 || !slices.Equal(got.Holders, got.NodeIDs) {
		t.Errorf("standalone placement map = %+v; want one node owning 64 shards and holding the primary bucket", got)
	}
}

// TestH64LegRecordCarriesTheMap: a measured H-64 leg records the placement, the
// shard count, the owner count per node and the holders of the primary bucket.
func TestH64LegRecordCarriesTheMap(t *testing.T) {
	cfg := mixTestConfig("rh", "social_h64")
	cfg.Placement = "h64"
	fake := newFakeServer()
	fake.expectBucket = referenceBucket("h64")
	fake.nodesOf = nodeRotation
	record := completedMixRecord(t, fakeHoardEngineOn(cfg, fake), cfg)
	owned := 0
	for _, count := range record.OwnersPerNode {
		owned += count
	}
	if record.Placement != "shard-64" || record.Shards != 64 || owned != 64 || len(record.OwnersPerNode) != 3 {
		t.Errorf("record placement=%q shards=%d owners=%v; want shard-64, 64 shards over 3 nodes", record.Placement, record.Shards, record.OwnersPerNode)
	}
	if !slices.Equal(record.HoldersOfPrimaryBucket, []string{"n1", "n2"}) || !slices.Equal(record.OwnerNodeIDs, []string{"n1", "n2", "n3"}) {
		t.Errorf("record holders=%v owner nodes=%v; want n1 n2 and n1 n2 n3", record.HoldersOfPrimaryBucket, record.OwnerNodeIDs)
	}
}

// TestH1LegRecordKeepsNoMap: the single-bucket form has one owner by
// construction and its records are unchanged.
func TestH1LegRecordKeepsNoMap(t *testing.T) {
	cfg := mixTestConfig("rh", "social_h1")
	fake := newFakeServer()
	fake.nodesOf = nodeRotation
	record := completedMixRecord(t, fakeHoardEngineOn(cfg, fake), cfg)
	if record.Placement != "single" || len(record.OwnersPerNode) != 0 || len(record.HoldersOfPrimaryBucket) != 0 {
		t.Errorf("H-1 record placement=%q owners=%v holders=%v; want single and no map", record.Placement, record.OwnersPerNode, record.HoldersOfPrimaryBucket)
	}
}

// TestPerUserPlacementGivesEachUserItsOwnBucket pins H-U: the bucket name
// carries the user id, so the ring spreads users across nodes instead of
// placing one User bucket, and every user in it, on a single owner.
func TestPerUserPlacementGivesEachUserItsOwnBucket(t *testing.T) {
	place := placementOfPerUser(10_000)
	if got := len(place.buckets()); got != 10_000 {
		t.Fatalf("H-U has %d buckets; want one per user", got)
	}
	for _, user := range []int{0, 1, 42, 9_999} {
		want := fmt.Sprintf("User_%d", user)
		if got := place.bucketOf(user); got != want {
			t.Errorf("bucketOf(%d) = %q; want %q", user, got, want)
		}
	}
	if place.bucketOf(0) == place.bucketOf(1) {
		t.Error("two users share a bucket under H-U")
	}
	if got := place.shardOfUser(42); got != 0 {
		t.Errorf("shardOfUser under H-U = %d; want 0 (a user's own bucket is its unit)", got)
	}
	// A duplicate name would put two users in one bucket and break the spread
	// for both, silently.
	seen := make(map[string]struct{}, len(place.buckets()))
	for _, name := range place.buckets() {
		if _, dup := seen[name]; dup {
			t.Fatalf("H-U names %q twice", name)
		}
		seen[name] = struct{}{}
	}
	if got := placementOfConfig(&config{Placement: "hu", Users: 10_000}); !got.perUser || len(got.buckets()) != 10_000 {
		t.Errorf("placementOfConfig(hu) = perUser %v with %d buckets; want 10,000 per-user buckets", got.perUser, len(got.buckets()))
	}
}
