package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"testing"
	"time"
)

// TestRingSeedParity pins the harness's generator to the copy in ringload/: the figures
// below are the ones ringload/seed_test.go computes (generatorFingerprint) from its own copy
// of graph.go. If either generator changes, one of the two tests fails, so the entity-centric
// seeder keeps loading the same data as the legacy seeder.
func TestRingSeedParity(t *testing.T) {
	tiny := datasetSpec{Name: "tiny", Users: 50, Posts: 250, FollowEdges: 150, FeedEntries: 150, Likes: 100, Comments: 60, Inbox: 100}
	graph := newSocialGraph(tiny, datasetSeed)
	digest := sha256.New()
	for _, user := range []int{0, 7, 31} {
		hashf(digest, "following %v\n", graph.following(user))
		hashf(digest, "feed %d %v\n", graph.feedQuota[user], graph.feedOf(user))
		post := graph.post(user, 2)
		hashf(digest, "post %s %s %v %q\n", post.Text, post.Created.Format(time.RFC3339Nano), post.Tags, post.ReplyTo)
	}
	count := 0
	for like := range graph.likes() {
		hashf(digest, "like %v\n", like)
		if count++; count == 20 {
			break
		}
	}
	const golden = "45927e27afe7f29ff1eceab0912407abbe92cc7be2650c4a81c9246da7b19ea2"
	if got := hex.EncodeToString(digest.Sum(nil)); got != golden {
		t.Fatalf("the harness generator's fingerprint is %s, want %s: ringload/graph.go and ringload/words.go must change with graph.go and gen.go", got, golden)
	}
}

func hashf(digest hash.Hash, format string, args ...any) {
	_, _ = fmt.Fprintf(digest, format, args...)
}

// TestRingSeedMediaParity pins the harness's avatar and photo bytes to the copy in ringload/:
// the same digest ringload/seed_test.go computes (goldenMediaDigest), so the entity-centric
// layout holds the same blobs as the legacy one.
func TestRingSeedMediaParity(t *testing.T) {
	tiny := datasetSpec{Name: "tiny", Users: 50, Posts: 250, FollowEdges: 150, FeedEntries: 150, Likes: 100, Comments: 60, Inbox: 100, Avatars: 5, Photos: 2}
	graph := newSocialGraph(tiny, datasetSeed)
	var blobs []byte
	for item := range graph.media() {
		if item.Owner == 0 {
			blobs = append(blobs, graph.content(item)...)
		}
	}
	digest := sha256.Sum256(blobs)
	const golden = "9dc935c6283c917dc4d0e2ed744eaaea3d8c983c2144ce79698d92459caa47f5"
	if got := hex.EncodeToString(digest[:]); got != golden {
		t.Fatalf("the harness media of user 0 has digest %s, want %s: ringload/graph.go must change with graph.go", got, golden)
	}
}
