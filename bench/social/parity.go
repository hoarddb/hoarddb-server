package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
)

// The dataset parity check (spec P4). After seeding, the three databases must
// hold the same content for a sampled 1,000 users, compared as a canonical hash
// (canon.go): `_id_` dropped, feeds and inboxes grouped by author, only
// per-author subsequences compared. -mode parity-hash prints that hash for one
// engine; the harness compares the lines of the three engines.

// sampleDigest is what one node returned for the sampled users: the hash of the
// whole sample, and one hash per user so a disagreement can name the users.
type sampleDigest struct {
	hash    string
	perUser map[int]string
	users   []canonicalUser
}

// readSample reads the canonical form of each rank, conc handles at a time.
func readSample(ctx context.Context, eng socialEngine, ranks []int, conc int) (sampleDigest, error) {
	users := make([]canonicalUser, len(ranks))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	next := make(chan int)
	var group sync.WaitGroup
	var firstFailure error
	var failureOnce sync.Once
	fail := func(err error) {
		failureOnce.Do(func() { firstFailure = err })
		cancel()
	}
	for range min(max(conc, 1), len(ranks)) {
		group.Add(1)
		go func() {
			defer group.Done()
			ops, err := eng.newSocialOps()
			if err != nil {
				fail(fmt.Errorf("opening a handle to read the sample: %w", err))
				return
			}
			defer ops.close()
			for index := range next {
				user, err := ops.canonicalUser(ctx, ranks[index])
				if err != nil {
					fail(fmt.Errorf("reading user %d: %w", ranks[index], err))
					return
				}
				users[index] = user
			}
		}()
	}
feeding:
	for index := range ranks {
		select {
		case next <- index:
		case <-ctx.Done():
			break feeding
		}
	}
	close(next)
	group.Wait()
	if firstFailure != nil {
		return sampleDigest{}, firstFailure
	}
	return digestOf(ranks, users)
}

// digestOf hashes the sample and each user in it.
func digestOf(ranks []int, users []canonicalUser) (sampleDigest, error) {
	digest := sampleDigest{perUser: make(map[int]string, len(ranks)), users: users}
	var err error
	if digest.hash, err = canonicalHash(users); err != nil {
		return sampleDigest{}, err
	}
	for index, user := range users {
		if digest.perUser[ranks[index]], err = canonicalHash([]canonicalUser{user}); err != nil {
			return sampleDigest{}, err
		}
	}
	return digest, nil
}

// uniqueLikes counts the likes the generator makes once a like is keyed by
// (post author, post sequence, liker), as every engine stores it: a repeated
// triple is one like. S-10k holds 98,371 of its 100,000 and S-100k 986,770 of
// its 1,000,000.
func uniqueLikes(graph *socialGraph) int {
	type like struct{ author, seq, liker int }
	seen := map[like]struct{}{}
	for reaction := range graph.likes() {
		seen[like{reaction.PostAuthor, reaction.PostSeq, reaction.Actor}] = struct{}{}
	}
	return len(seen)
}

// datasetCounts are the totals the generator says a freshly seeded dataset holds.
type datasetCounts struct {
	Users        int `json:"users"`
	Posts        int `json:"posts"`
	FollowEdges  int `json:"follow_edges"`
	FeedEntries  int `json:"feed_entries"`
	LikesUnique  int `json:"likes_unique"`
	LikesMade    int `json:"likes_generated"`
	Comments     int `json:"comments"`
	InboxEntries int `json:"inbox_entries"`
}

func expectedCounts(spec datasetSpec, graph *socialGraph) datasetCounts {
	feed := 0
	for _, quota := range graph.feedQuota {
		feed += int(quota)
	}
	return datasetCounts{
		Users: spec.Users, Posts: graph.postsPerUser * spec.Users, FollowEdges: len(graph.followees), FeedEntries: feed,
		LikesUnique: uniqueLikes(graph), LikesMade: spec.Likes, Comments: spec.Comments, InboxEntries: spec.Inbox,
	}
}

// sampleCounts are what the sampled users hold, summed.
type sampleCounts struct {
	Users     int `json:"users"`
	Posts     int `json:"posts"`
	Following int `json:"following"`
	Followers int `json:"followers"`
	Feed      int `json:"feed_entries"`
	Inbox     int `json:"inbox_entries"`
	Likes     int `json:"likes"`
	Comments  int `json:"comments"`
}

func countSample(users []canonicalUser) sampleCounts {
	counts := sampleCounts{Users: len(users)}
	for _, user := range users {
		counts.Posts += len(user.Posts)
		counts.Following += len(user.Following)
		counts.Followers += len(user.Followers)
		counts.Inbox += len(user.Inbox)
		for _, run := range user.Feed {
			counts.Feed += len(run.Entries)
		}
		for _, post := range user.Posts {
			counts.Likes += len(post.Likers)
			counts.Comments += len(post.Comments)
		}
	}
	return counts
}

// parityResult is the one JSON line -mode parity-hash prints.
type parityResult struct {
	Mode             string        `json:"mode"`
	Engine           string        `json:"engine"`
	Dataset          string        `json:"dataset"`
	SampleHash       string        `json:"sample_hash"`
	ExpectedHash     string        `json:"expected_hash"`
	MatchesGenerator bool          `json:"matches_generator"`
	Sample           sampleCounts  `json:"sample_counts"`
	Totals           datasetCounts `json:"dataset_counts"`
}

// runParityHash reads the sampled users from the seeded dataset and prints their
// canonical hash beside the hash the generator says they must have. The two
// agree only on a dataset no leg has changed since the seed; across engines the
// harness compares sample_hash lines taken at that point.
func runParityHash(ctx context.Context, cfg *config, out io.Writer) error {
	spec, err := lookupDataset(cfg.Dataset)
	if err != nil {
		return err
	}
	eng, err := newEngine(ctx, cfg)
	if err != nil {
		return err
	}
	defer eng.close()
	social, ok := eng.(socialEngine)
	if !ok {
		return fmt.Errorf("-engine %s does not implement the social operations", cfg.Engine)
	}
	return parityOn(ctx, cfg, social, newSocialGraph(spec, datasetSeed), out)
}

// parityOn is runParityHash for an engine that is already open, whose dataset
// was seeded from graph.
func parityOn(ctx context.Context, cfg *config, social socialEngine, graph *socialGraph, out io.Writer) error {
	spec := graph.spec
	ranks := sampleUsers(spec.Users, cfg.SampleUsers, datasetSeed)
	digest, err := readSample(ctx, social, ranks, cfg.Conc)
	if err != nil {
		return fmt.Errorf("reading the sample of %d users from %s: %w", len(ranks), cfg.Engine, err)
	}
	expected, err := canonicalHash(graphCanonical(graph, ranks))
	if err != nil {
		return fmt.Errorf("hashing the generator's sample: %w", err)
	}
	result := parityResult{
		Mode: modeParityHash, Engine: cfg.Engine, Dataset: spec.Name,
		SampleHash: digest.hash, ExpectedHash: expected, MatchesGenerator: digest.hash == expected,
		Sample: countSample(digest.users), Totals: expectedCounts(spec, graph),
	}
	line, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encoding the parity result: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the parity result: %w", err)
	}
	return nil
}

// differingUsers lists up to limit ranks whose per-user hash differs between a and b.
func differingUsers(a, b sampleDigest, limit int) []int {
	var differing []int
	for rank, hash := range a.perUser {
		if b.perUser[rank] != hash {
			differing = append(differing, rank)
		}
	}
	slices.Sort(differing)
	return differing[:min(len(differing), limit)]
}
