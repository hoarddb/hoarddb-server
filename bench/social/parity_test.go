package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// TestUniqueLikesMatchTheSpecCounts: a like is keyed by (post author, post
// sequence, liker), so the generator's repeats collapse: S-10k holds 98,371 of
// its 100,000 likes and S-100k 986,770 of its 1,000,000.
func TestUniqueLikesMatchTheSpecCounts(t *testing.T) {
	cases := []struct {
		dataset string
		want    int
	}{{"S-10k", 98_371}, {"S-100k", 986_770}}
	for _, tc := range cases {
		t.Run(tc.dataset, func(t *testing.T) {
			if testing.Short() && tc.dataset != "S-10k" {
				t.Skip("S-100k builds a two-million-edge graph")
			}
			spec, err := lookupDataset(tc.dataset)
			if err != nil {
				t.Fatal(err)
			}
			if got := uniqueLikes(newSocialGraph(spec, datasetSeed)); got != tc.want {
				t.Errorf("%s: %d unique likes; want %d", tc.dataset, got, tc.want)
			}
		})
	}
}

// parityLine runs the parity mode against a seeded fake world and decodes its line.
func parityLine(t *testing.T, world *socialWorld) parityResult {
	t.Helper()
	cfg := &config{Engine: engineHoardDB, SampleUsers: tinySpec.Users, Conc: 3}
	var out bytes.Buffer
	if err := parityOn(context.Background(), cfg, world.engine, world.graph, &out); err != nil {
		t.Fatalf("parity-hash: %v", err)
	}
	var result parityResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("decoding %q: %v", out.String(), err)
	}
	return result
}

// TestParityHashStableAcrossRuns: the same data hashes the same however often it
// is read, and a freshly seeded dataset hashes to what the generator says.
func TestParityHashStableAcrossRuns(t *testing.T) {
	world := newFakeWorld(t)
	first, second := parityLine(t, world), parityLine(t, world)
	if first.SampleHash == "" || first.SampleHash != second.SampleHash {
		t.Errorf("hashes %q and %q; want one stable hash", first.SampleHash, second.SampleHash)
	}
	if !first.MatchesGenerator || first.SampleHash != first.ExpectedHash {
		t.Errorf("a fresh seed must hash to the generator's expectation: sample %q expected %q", first.SampleHash, first.ExpectedHash)
	}
	if first.Sample.Users != tinySpec.Users || first.Sample.Posts == 0 || first.Sample.Likes == 0 || first.Sample.Feed == 0 {
		t.Errorf("sample counts %+v; want every kind of content counted", first.Sample)
	}
	if first.Totals.LikesUnique == 0 || first.Totals.LikesUnique > first.Totals.LikesMade {
		t.Errorf("dataset counts %+v; unique likes must be positive and no more than the generated ones", first.Totals)
	}
}

// TestParityHashSeesAMissingLike: one like missing from one engine changes its
// hash, so a seed that loses a like cannot pass as the same dataset.
func TestParityHashSeesAMissingLike(t *testing.T) {
	whole, damaged := newFakeWorld(t), newFakeWorld(t)
	dropOneLike(t, damaged)
	good, bad := parityLine(t, whole), parityLine(t, damaged)
	if good.SampleHash == bad.SampleHash || bad.MatchesGenerator {
		t.Errorf("a dataset missing a like hashes as the whole one (%q) or matches the generator", bad.SampleHash)
	}
	if bad.Sample.Likes != good.Sample.Likes-1 {
		t.Errorf("sampled likes %d against %d; want exactly one fewer", bad.Sample.Likes, good.Sample.Likes)
	}
}
