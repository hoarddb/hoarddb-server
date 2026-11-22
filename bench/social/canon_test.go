package main

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"
)

var canonEpoch = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// rawFixture is a small user as an engine might return it.
func rawFixture() rawUser {
	return rawUser{
		Profile: userRecord{UserID: "7", Name: "n", Bio: "b", Joined: canonEpoch, FollowerCount: 2, FollowingCount: 3},
		Posts: []rawPost{
			{Post: postRecord{ID: "p1", Author: "7", Seq: 1, Text: "first", Created: canonEpoch, Tags: []string{"x"}}, Likers: []string{"3", "1", "2"},
				Comments: []rawComment{{ID: "c1", Author: "4", Text: "hi", TS: canonEpoch}, {ID: "c2", Author: "5", Text: "yo", TS: canonEpoch}}},
			{Post: postRecord{ID: "p0", Author: "7", Seq: 0, Text: "zero", Created: canonEpoch}},
		},
		Feed: []postRecord{
			{ID: "f1", Author: "1", Seq: 5, Text: "a5", Created: canonEpoch},
			{ID: "f2", Author: "2", Seq: 9, Text: "b9", Created: canonEpoch},
			{ID: "f3", Author: "1", Seq: 4, Text: "a4", Created: canonEpoch},
		},
		Inbox: []rawNote{
			{ID: "n1", Type: "like", Actor: "3", Target: "7", TS: canonEpoch},
			{ID: "n2", Type: "follow", Actor: "1", Target: "7", TS: canonEpoch},
		},
		Following: []string{"1", "2", "3"},
		Followers: []string{"8", "9"},
		Media:     map[string][]byte{"avatar": []byte("pixels")},
	}
}

func hashOf(t *testing.T, raw rawUser) string {
	t.Helper()
	hash, err := canonicalHash([]canonicalUser{canonicalize(raw)})
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	return hash
}

func TestParityHashDiffersOnOneChangedPost(t *testing.T) {
	base := hashOf(t, rawFixture())
	if base == "" || base != hashOf(t, rawFixture()) {
		t.Fatalf("the hash of the same content is %q then %q; want one stable, non-empty value", base, hashOf(t, rawFixture()))
	}
	changes := map[string]func(*rawUser){
		"one post's text":              func(u *rawUser) { u.Posts[0].Post.Text = "first!" },
		"one feed copy's text":         func(u *rawUser) { u.Feed[1].Text = "b9!" },
		"one liker":                    func(u *rawUser) { u.Posts[0].Likers[0] = "99" },
		"one comment":                  func(u *rawUser) { u.Posts[0].Comments[1].Text = "yo!" },
		"one follow edge":              func(u *rawUser) { u.Following[2] = "4" },
		"one inbox entry":              func(u *rawUser) { u.Inbox[0].Type = "comment" },
		"the avatar":                   func(u *rawUser) { u.Media["avatar"] = []byte("pixelz") },
		"one author's feed order":      func(u *rawUser) { u.Feed[0], u.Feed[2] = u.Feed[2], u.Feed[0] },
		"a counter on the profile":     func(u *rawUser) { u.Profile.FollowerCount++ },
		"one post missing from a feed": func(u *rawUser) { u.Feed = u.Feed[:2] },
	}
	for name, change := range changes {
		changed := rawFixture()
		change(&changed)
		if hashOf(t, changed) == base {
			t.Errorf("changing %s left the hash unchanged", name)
		}
	}
}

func TestParityIgnoresIDAndInterleaving(t *testing.T) {
	base := hashOf(t, rawFixture())
	other := rawFixture()
	// different minted ids everywhere
	for i := range other.Posts {
		other.Posts[i].Post.ID = "other-" + other.Posts[i].Post.ID
		for j := range other.Posts[i].Comments {
			other.Posts[i].Comments[j].ID = "other-" + other.Posts[i].Comments[j].ID
		}
	}
	for i := range other.Feed {
		other.Feed[i].ID = "other-" + other.Feed[i].ID
	}
	for i := range other.Inbox {
		other.Inbox[i].ID = "other-" + other.Inbox[i].ID
	}
	// the same per-author sequences, interleaved the other way round
	other.Feed = []postRecord{other.Feed[1], other.Feed[0], other.Feed[2]}
	// listing order of unordered sets
	slices.Reverse(other.Following)
	slices.Reverse(other.Followers)
	slices.Reverse(other.Posts[0].Likers)
	slices.Reverse(other.Inbox)
	if got := hashOf(t, other); got != base {
		t.Errorf("same logical content with other ids and another interleaving hashes to %s, want %s", got, base)
	}
}

func TestSampleUsersIsDeterministic(t *testing.T) {
	first, second := sampleUsers(100, 10, 5), sampleUsers(100, 10, 5)
	if !slices.Equal(first, second) || len(first) != 10 || !slices.IsSorted(first) {
		t.Fatalf("sample = %v then %v; want the same 10 ranks in order", first, second)
	}
	if len(slices.Compact(slices.Clone(first))) != 10 || first[0] < 0 || first[9] >= 100 {
		t.Errorf("sample %v holds duplicates or ranks outside 0..99", first)
	}
	if slices.Equal(first, sampleUsers(100, 10, 6)) {
		t.Error("a different seed gave the same sample")
	}
	if all := sampleUsers(5, 10, 1); len(all) != 5 {
		t.Errorf("asking for 10 of 5 users gave %v; want all 5", all)
	}
}

// TestCanonicalUserMatchesGraph reads every seeded user back from the server
// and compares it with what the generator says the dataset holds: the seeder,
// the reads and the canonical form agree.
func TestCanonicalUserMatchesGraph(t *testing.T) {
	eachWorld(t, checkCanonicalMatchesGraph)
}

// checkCanonicalMatchesGraph is TestCanonicalUserMatchesGraph for any engine.
func checkCanonicalMatchesGraph(t *testing.T, world *socialWorld) {
	ops := world.ops(t)
	ranks := sampleUsers(tinySpec.Users, tinySpec.Users, 1)
	want := graphCanonical(world.graph, ranks)
	read := make([]canonicalUser, 0, len(ranks))
	for _, rank := range ranks {
		user, err := ops.canonicalUser(context.Background(), rank)
		if err != nil {
			t.Fatalf("reading user %d: %v", rank, err)
		}
		read = append(read, user)
	}
	for i := range ranks {
		gotJSON, wantJSON := mustJSON(t, read[i]), mustJSON(t, want[i])
		if gotJSON != wantJSON {
			t.Fatalf("user %d differs from the generator:\n server    %s\n generator %s", ranks[i], gotJSON, wantJSON)
		}
	}
	got, err := canonicalHash(read)
	if err != nil {
		t.Fatalf("hashing the server's users: %v", err)
	}
	wantHash, err := canonicalHash(want)
	if err != nil {
		t.Fatalf("hashing the generator's users: %v", err)
	}
	if got == "" || got != wantHash {
		t.Errorf("hash of the server's users = %q, of the generator's = %q", got, wantHash)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	return string(encoded)
}
