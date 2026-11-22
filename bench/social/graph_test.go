package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

// graphFingerprint hashes every record of every stream, text included, so two
// graphs are byte-identical exactly when their fingerprints match.
func graphFingerprint(t *testing.T, graph *socialGraph) [sha256.Size]byte {
	t.Helper()
	var stream strings.Builder
	record := func(format string, args ...any) { fmt.Fprintf(&stream, format, args...) }
	for user := range graph.users() {
		record("U%d|%+v|%v|%v|%v\n", user.Rank, user.Profile, user.Following, user.Feed, user.Inbox)
	}
	for post := range graph.posts() {
		record("P%+v\n", post)
	}
	for like := range graph.likes() {
		record("L%+v\n", like)
	}
	for comment := range graph.comments() {
		record("C%+v\n", comment)
	}
	for media := range graph.media() {
		record("M%+v\n", media)
	}
	return sha256.Sum256([]byte(stream.String()))
}

func mustDataset(t *testing.T, name string) datasetSpec {
	t.Helper()
	spec, err := lookupDataset(name)
	if err != nil {
		t.Fatalf("lookupDataset(%q): %v", name, err)
	}
	return spec
}

func TestGraphDeterministic(t *testing.T) {
	spec := mustDataset(t, "S-10k")
	first := graphFingerprint(t, newSocialGraph(spec, 1))
	again := graphFingerprint(t, newSocialGraph(spec, 1))
	other := graphFingerprint(t, newSocialGraph(spec, 2))
	if first != again {
		t.Error("the same seed produced different bytes for S-10k")
	}
	if first == other {
		t.Error("seeds 1 and 2 produced identical bytes: the seed is ignored")
	}
}

func TestDegreeMeanAndCap(t *testing.T) {
	spec := mustDataset(t, "S-10k")
	followers := make([]int, spec.Users)
	var edges, followingTotal, followingAtMostMean int
	minFollowing, maxFollowing := 1<<30, 0
	for user := range newSocialGraph(spec, 1).users() {
		following := len(user.Following)
		followingTotal += following
		minFollowing, maxFollowing = min(minFollowing, following), max(maxFollowing, following)
		if following <= 20 {
			followingAtMostMean++
		}
		seen := map[int32]bool{}
		for _, followee := range user.Following {
			if int(followee) == user.Rank {
				t.Fatalf("user %d follows themselves", user.Rank)
			}
			if seen[followee] {
				t.Fatalf("user %d follows %d twice", user.Rank, followee)
			}
			seen[followee] = true
			followers[followee]++
			edges++
		}
	}
	meanFollowers := float64(edges) / float64(spec.Users)
	if meanFollowers < 19 || meanFollowers > 21 {
		t.Errorf("mean followers %.2f, want 20 +/- 1", meanFollowers)
	}
	if mean := float64(followingTotal) / float64(spec.Users); mean < 19 || mean > 21 {
		t.Errorf("mean following %.2f, want 20 +/- 1", mean)
	}
	for rank, count := range followers {
		if count > 500 {
			t.Fatalf("user %d has %d followers, the celebrity cap is 500", rank, count)
		}
	}
	if minFollowing < 1 || maxFollowing > 100 {
		t.Errorf("following ranges over %d..%d, want 1..100", minFollowing, maxFollowing)
	}
	// A geometric distribution with mean 20 puts 1-(19/20)^20 = 64.2% of users at 20 or fewer.
	if share := float64(followingAtMostMean) / float64(spec.Users); share < 0.60 || share > 0.69 {
		t.Errorf("%.1f%% of users follow 20 or fewer, want about 64%% for a geometric distribution", share*100)
	}
}

// datasetCountsFromSpec6_4 is the table of spec 6.4, typed a second time so
// the test compares the generator with the document, not with itself.
var datasetCountsFromSpec6_4 = map[string]datasetSpec{
	"S-10k":  {Users: 10_000, Posts: 100_000, FollowEdges: 200_000, FeedEntries: 200_000, Likes: 100_000, Comments: 50_000, Inbox: 50_000, Avatars: 1_000, Photos: 100},
	"S-100k": {Users: 100_000, Posts: 1_000_000, FollowEdges: 2_000_000, FeedEntries: 2_000_000, Likes: 1_000_000, Comments: 500_000, Inbox: 500_000, Avatars: 10_000, Photos: 1_000},
	"S-1M":   {Users: 1_000_000, Posts: 2_000_000, FollowEdges: 5_000_000, FeedEntries: 5_000_000, Likes: 1_000_000, Comments: 0, Inbox: 1_000_000, Avatars: 10_000, Photos: 0},
}

func TestDatasetCounts(t *testing.T) {
	for name, want := range datasetCountsFromSpec6_4 {
		t.Run(name, func(t *testing.T) {
			if testing.Short() && name == "S-1M" {
				t.Skip("S-1M streams 5 million edges; run without -short")
			}
			graph := newSocialGraph(mustDataset(t, name), 1)
			var got datasetSpec
			for user := range graph.users() {
				got.Users++
				got.FollowEdges += len(user.Following)
				got.FeedEntries += len(user.Feed)
				got.Inbox += len(user.Inbox)
			}
			for range graph.posts() {
				got.Posts++
			}
			for range graph.likes() {
				got.Likes++
			}
			for range graph.comments() {
				got.Comments++
			}
			for item := range graph.media() {
				switch item.Kind {
				case mediaAvatar:
					got.Avatars++
				case mediaPhoto:
					got.Photos++
				}
			}
			got.Name = want.Name
			if got != want {
				t.Errorf("%s counts\n got  %+v\n want %+v", name, got, want)
			}
		})
	}
}

func TestZipfTargetUniformActor(t *testing.T) {
	spec := mustDataset(t, "S-10k")
	actorCount := make([]int, spec.Users)
	var hotTargets, total int
	for like := range newSocialGraph(spec, 1).likes() {
		actorCount[like.Actor]++
		total++
		if like.PostAuthor < spec.Users/100 {
			hotTargets++
		}
	}
	if total != spec.Likes {
		t.Fatalf("%d likes generated, want %d", total, spec.Likes)
	}
	var firstHalf, busiest int
	for rank, count := range actorCount {
		busiest = max(busiest, count)
		if rank < spec.Users/2 {
			firstHalf += count
		}
	}
	if share := float64(firstHalf) / float64(total); share < 0.48 || share > 0.52 {
		t.Errorf("the lower half of user ranks does %.1f%% of the likes, want 50%% for a uniform actor", share*100)
	}
	if busiest > 40 {
		t.Errorf("busiest actor made %d likes against a mean of 10: actors are not uniform", busiest)
	}
	// Zipf s=1.1 over 10,000 users puts well over a third of the targets on the hottest 1%.
	if share := float64(hotTargets) / float64(total); share < 0.35 {
		t.Errorf("the hottest 1%% of users receive %.1f%% of the likes, want more than 35%% for a Zipf target", share*100)
	}
}

func TestMediaContentIsDeterministic(t *testing.T) {
	graph := newSocialGraph(mustDataset(t, "S-10k"), 1)
	for item := range graph.media() {
		first, again := graph.content(item), graph.content(item)
		if len(first) != item.Bytes || !bytes.Equal(first, again) {
			t.Fatalf("media %+v: content is %d bytes (want %d) or differs between calls", item, len(first), item.Bytes)
		}
		return
	}
	t.Fatal("S-10k has no media")
}
