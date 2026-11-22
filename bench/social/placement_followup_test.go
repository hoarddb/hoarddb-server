package main

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// tieFollowees are ten users in caller order, sorted by descending shard so the
// order is not the shard order the engine reads them in.
func tieFollowees() []int {
	users := make([]int, 10)
	for rank := range users {
		users[rank] = rank
	}
	place := placementOfShards(64)
	slices.SortStableFunc(users, func(a, b int) int { return place.shardOfUser(b) - place.shardOfUser(a) })
	return users
}

// withTiedPosts gives every followee one post with the same Created, newer than
// any seeded post, so the merge has ties only the join order can break.
func withTiedPosts(t *testing.T, world *socialWorld, followees []int) {
	t.Helper()
	ops, ok := world.ops(t).(*hoardOps)
	if !ok {
		t.Fatalf("the hoardDB world's operations are %T, not *hoardOps", world.ops(t))
	}
	tied := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, followee := range followees {
		post := postRecord{Author: userID(followee), Seq: 7000, Text: "tied", Created: tied, Tags: []string{"t"}}
		put := ops.field(followee, "Posts")
		put.Element, put.Data = postKey(post.Seq), post.document()
		if _, err := ops.fieldCall(transport.OpFieldPut, put, "storing a tied post"); err != nil {
			t.Fatal(err)
		}
	}
}

// TestMergedTimelineTiesFollowTheCallersOrder: posts with equal Created across
// followees of different shards come out in the order the caller gave the
// followees, as under H-1, whatever order the shards are read in.
func TestMergedTimelineTiesFollowTheCallersOrder(t *testing.T) {
	followees := tieFollowees()
	place := placementOfShards(64)
	var shards []int
	for _, followee := range followees {
		shards = append(shards, place.shardOfUser(followee))
	}
	if slices.IsSorted(shards) {
		t.Fatalf("the callers' order %v is already the shard order; the fixture cannot tell the two joins apart", followees)
	}
	var merged [][]string
	for _, form := range []string{"h1", "h64"} {
		world := newFakeWorldPlaced(t, form)
		withTiedPosts(t, world, followees)
		posts, err := world.ops(t).mergedTimeline(context.Background(), followees)
		if err != nil {
			t.Fatalf("%s: %v", form, err)
		}
		merged = append(merged, authorAndSeq(posts))
	}
	if !slices.Equal(merged[0], merged[1]) {
		t.Errorf("the tied posts merge as %v under H-64 and %v under H-1; want the same order", merged[1], merged[0])
	}
	var authors []string
	for _, followee := range followees {
		authors = append(authors, userID(followee)+"/7000")
	}
	if !slices.Equal(merged[0][:len(authors)], authors) {
		t.Errorf("the ties came out %v; want the caller's followee order %v", merged[0][:len(authors)], authors)
	}
}

// TestCopiesBelowFactorIsNullNotZero: nothing measures how many writes were
// acknowledged with fewer copies than the factor, so a leg records null; a 0
// would read as "none were".
func TestCopiesBelowFactorIsNullNotZero(t *testing.T) {
	for _, form := range []string{"h1", "h64"} {
		cfg := mixTestConfig("rh", "social_copies_"+form)
		cfg.Placement = form
		fake := newFakeServer()
		fake.expectBucket = referenceBucket(form)
		record, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngineOn(cfg, fake))
		if err != nil {
			t.Fatalf("%s: %v", form, err)
		}
		var out bytes.Buffer
		if err := writeLegRecord(&out, record); err != nil {
			t.Fatal(err)
		}
		var fields map[string]interface{}
		if err := json.Unmarshal(out.Bytes(), &fields); err != nil {
			t.Fatal(err)
		}
		if value, present := fields["copies_below_factor"]; !present || value != nil {
			t.Errorf("%s: copies_below_factor = %v (present %v); want null", form, value, present)
		}
	}
}
