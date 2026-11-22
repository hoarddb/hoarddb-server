package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// timeline builds a newest-first page of count posts by author whose newest
// has sequence number top.
func timeline(author string, top int64, count int) []postView {
	posts := make([]postView, count)
	for i := range posts {
		posts[i] = postView{Author: author, Seq: top - int64(i)}
	}
	return posts
}

func TestVerifyTimeline(t *testing.T) {
	quiet := readExpectation{Author: "7", Limit: 20, BaseTotal: 20, SeqBase: 20, IssuedAfter: 5, Quiescent: true, PriorOrdered: true, Slack: 8}
	busy := readExpectation{Author: "7", Limit: 20, BaseTotal: 20, SeqBase: 20, AckedBefore: 3, IssuedAfter: 8, Overlaps: 3, PriorOrdered: true, Slack: 8}
	wrongAuthor := timeline("7", 24, 20)
	wrongAuthor[11].Author = "8"
	reversed := timeline("7", 24, 20)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}

	cases := []struct {
		name    string
		expect  readExpectation
		total   int64
		posts   []postView
		wantErr string
	}{
		{"exact page", quiet, 25, timeline("7", 24, 20), ""},
		{"wrong author", quiet, 25, wrongAuthor, "by \"8\""},
		{"oldest first", quiet, 25, reversed, "sits below an older one"},
		{"lost write", quiet, 24, timeline("7", 23, 20), "holds 24 posts"},
		{"stale top", quiet, 25, timeline("7", 23, 20), "newest post has seq 23"},
		{"short page", quiet, 25, timeline("7", 24, 19), "returned 19 posts"},
		{"gap in new posts", quiet, 25, append(timeline("7", 24, 3), timeline("7", 20, 17)...), "lost a post"},
		{"overlapped but quiet, permanent disorder", readExpectation{Author: "7", Limit: 20, BaseTotal: 20, SeqBase: 20, IssuedAfter: 5, Quiescent: true, Overlaps: 1, PriorOrdered: true, Slack: 8},
			25, append([]postView{{Author: "7", Seq: 23}, {Author: "7", Seq: 24}}, timeline("7", 22, 18)...), ""},
		{"overlapped but quiet, wrong count", readExpectation{Author: "7", Limit: 20, BaseTotal: 20, SeqBase: 20, IssuedAfter: 5, Quiescent: true, Overlaps: 1, PriorOrdered: true, Slack: 8},
			24, timeline("7", 24, 20), "with no write in flight"},
		{"attached, old disorder not judged", readExpectation{Author: "7", Limit: 20, BaseTotal: 20, SeqBase: 20, IssuedAfter: 2, Quiescent: true, Slack: 8},
			22, append(timeline("7", 21, 2), append([]postView{{Author: "7", Seq: 3}, {Author: "7", Seq: 4}}, timeline("7", 2, 16)...)...), ""},
		{"attached, new post below old one", readExpectation{Author: "7", Limit: 20, BaseTotal: 20, SeqBase: 20, IssuedAfter: 2, Quiescent: true, Slack: 8},
			22, append([]postView{{Author: "7", Seq: 21}, {Author: "7", Seq: 5}, {Author: "7", Seq: 20}}, timeline("7", 4, 17)...), "sits below an older one"},
		{"attached, read before this run wrote, old top out of order", readExpectation{Author: "7", Limit: 20, BaseTotal: 25, SeqBase: 25, IssuedAfter: 0, Quiescent: true, Slack: 8},
			25, append([]postView{{Author: "7", Seq: 23}, {Author: "7", Seq: 24}}, timeline("7", 22, 18)...), ""},
		{"busy within bounds", busy, 26, timeline("7", 25, 20), ""},
		{"busy reordered pair", busy, 26, append([]postView{{Author: "7", Seq: 24}, {Author: "7", Seq: 25}}, timeline("7", 23, 18)...), ""},
		{"busy count too low", busy, 22, timeline("7", 21, 20), "expected 23..28"},
		{"busy count too high", busy, 29, timeline("7", 28, 20), "expected 23..28"},
		{"busy wrong author", busy, 26, wrongAuthor, "by \"8\""},
		{"busy reversed", busy, 26, reversed, "sits below an older one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyTimeline(tc.expect, tc.total, tc.posts)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("correct read rejected: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("wrong read accepted, want error containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestAttachBoundaryAfterOverlappedBurst covers the seed-then-attach boundary:
// a seed run's closing burst can leave a timeline whose newest post is not its
// highest sequence number, and the timed run must resume above the highest.
func TestAttachBoundaryAfterOverlappedBurst(t *testing.T) {
	// Seq 25 landed first and seq 24 on top of it, as two overlapped pushes may.
	seeded := append([]postView{{Author: "7", Seq: 24}, {Author: "7", Seq: 25}}, timeline("7", 23, 24)...)
	if got := nextSeqAfter(seeded); got != 26 {
		t.Fatalf("nextSeqAfter = %d, want 26 (resuming from the top post would give 25 and reuse a number)", got)
	}
	if got := nextSeqAfter(nil); got != 0 {
		t.Fatalf("nextSeqAfter of an empty timeline = %d, want 0", got)
	}

	// The timed run then writes seq 26 and 27; its page sits above the old posts.
	attached := readExpectation{Author: "7", Limit: 20, BaseTotal: 26, SeqBase: 26, IssuedAfter: 2, Quiescent: true, Slack: 8}
	page := append(timeline("7", 27, 2), seeded[:18]...)
	if err := verifyTimeline(attached, 28, page); err != nil {
		t.Fatalf("the verifier rejects a timeline whose top post was not its highest at attach: %v", err)
	}

	// Resuming from the top post instead reuses seq 25, the failure this fixes.
	fromTop := attached
	fromTop.SeqBase = seeded[0].Seq + 1
	reused := append(timeline("7", 26, 1), append([]postView{{Author: "7", Seq: 25}}, seeded...)[:19]...)
	if err := verifyTimeline(fromTop, 27, reused); err == nil || !strings.Contains(err.Error(), "sits below an older one") {
		t.Fatalf("resuming from the top post: error = %v, want a \"sits below an older one\" rejection", err)
	}
}

// entries builds a feed page from (author, seq) pairs, newest first.
func entries(pairs ...[2]int) []postRecord {
	page := make([]postRecord, len(pairs))
	for i, pair := range pairs {
		page[i] = postRecord{Author: userID(pair[0]), Seq: int64(pair[1])}
	}
	return page
}

// feedFor is an expectation for actor "7", who follows authors 1 and 2, whose
// numbering has reached 12 posts each, with a seeded feed of 6 entries.
func feedFor(mutate func(*feedExpectation)) feedExpectation {
	e := feedExpectation{
		Actor: "7", Limit: 20, Base: 6, AckedBefore: 2, IssuedAfter: 3,
		Followed:   func(author string) bool { return author == "1" || author == "2" },
		PostsBound: func(string) int64 { return 12 },
	}
	if mutate != nil {
		mutate(&e)
	}
	return e
}

func TestVerifierRejectsPhantomFeedEntry(t *testing.T) {
	good := entries([2]int{1, 11}, [2]int{2, 10}, [2]int{1, 9}, [2]int{2, 8}, [2]int{1, 7}, [2]int{2, 6}, [2]int{1, 5}, [2]int{2, 4})
	cases := []struct {
		name    string
		page    []postRecord
		wantErr string
	}{
		{"a correct page", good, ""},
		{"a post by someone never followed", append(entries([2]int{9, 3}), good[1:]...), "never followed"},
		{"a post that was never written", append(entries([2]int{1, 12}), good[1:]...), "never written"},
		{"the same post twice", append(entries([2]int{1, 11}), good...)[:8], "twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expect := feedFor(func(e *feedExpectation) { e.Base, e.AckedBefore, e.IssuedAfter = 8, 0, 0 })
			err := verifyFeed(expect, tc.page)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("correct page rejected: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestVerifierRejectsMisorderedFeed(t *testing.T) {
	expect := feedFor(func(e *feedExpectation) { e.Base, e.AckedBefore, e.IssuedAfter = 3, 0, 0 })
	if err := verifyFeed(expect, entries([2]int{1, 11}, [2]int{1, 10}, [2]int{1, 9})); err != nil {
		t.Fatalf("a newest-first page rejected: %v", err)
	}
	err := verifyFeed(expect, entries([2]int{1, 9}, [2]int{1, 10}, [2]int{1, 11}))
	if err == nil || !strings.Contains(err.Error(), "not newest-first") {
		t.Fatalf("an oldest-first page: error = %v, want \"not newest-first\"", err)
	}
}

// TestOrderCheckedPerAuthor: concurrent posts by different authors may reach a
// feed in either interleaving, so only one author's own entries are ordered.
func TestOrderCheckedPerAuthor(t *testing.T) {
	expect := feedFor(func(e *feedExpectation) { e.Base, e.AckedBefore, e.IssuedAfter = 4, 0, 0 })
	interleavedOneWay := entries([2]int{1, 11}, [2]int{2, 10}, [2]int{1, 9}, [2]int{2, 8})
	interleavedOther := entries([2]int{2, 10}, [2]int{2, 8}, [2]int{1, 11}, [2]int{1, 9})
	for name, page := range map[string][]postRecord{"one interleaving": interleavedOneWay, "another": interleavedOther} {
		if err := verifyFeed(expect, page); err != nil {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
	oneAuthorOutOfSequence := entries([2]int{1, 11}, [2]int{2, 10}, [2]int{1, 9}, [2]int{2, 8})
	oneAuthorOutOfSequence[2], oneAuthorOutOfSequence[0] = oneAuthorOutOfSequence[0], oneAuthorOutOfSequence[2]
	if err := verifyFeed(expect, oneAuthorOutOfSequence); err == nil || !strings.Contains(err.Error(), "not newest-first") {
		t.Errorf("author 1 out of sequence: error = %v, want \"not newest-first\"", err)
	}
}

func TestVerifierExactFormCountsAckedWrites(t *testing.T) {
	expect := feedFor(nil) // 6 seeded + 2 acked .. 6 seeded + 3 issued
	page := func(count int) []postRecord {
		var pairs [][2]int
		for i := range count {
			pairs = append(pairs, [2]int{1 + i%2, 11 - i})
		}
		return entries(pairs...)
	}
	for count, wantErr := range map[int]bool{7: true, 8: false, 9: false, 10: true} {
		err := verifyFeed(expect, page(count))
		if (err != nil) != wantErr {
			t.Errorf("a page of %d entries: error = %v, want error=%v (8..9 are allowed)", count, err, wantErr)
		}
	}
}

// TestVerifierSafetyFormOnReplicas: a replica may be behind, so a short page is
// fine, but a phantom, a duplicate, a wrong order within one author and a count
// above what was issued are wrong on any replica.
func TestVerifierSafetyFormOnReplicas(t *testing.T) {
	behind := entries([2]int{1, 11}, [2]int{2, 10}, [2]int{1, 9}, [2]int{2, 8}, [2]int{1, 7}, [2]int{2, 6}) // the 6 seeded only
	exact, safety := feedFor(nil), feedFor(func(e *feedExpectation) { e.Safety = true })
	if err := verifyFeed(exact, behind); err == nil {
		t.Error("the exact form accepted a page that lacks acknowledged writes")
	}
	if err := verifyFeed(safety, behind); err != nil {
		t.Errorf("the safety form rejected a replica that is behind: %v", err)
	}
	if err := verifyFeed(safety, entries([2]int{1, 11}, [2]int{2, 10}, [2]int{1, 9})); err == nil || !strings.Contains(err.Error(), "below the") {
		t.Errorf("a page below the pre-run floor: error = %v, want a floor violation", err)
	}
	misordered := slices.Clone(behind)
	misordered[0], misordered[2] = misordered[2], misordered[0] // author 1 now ascends
	wrong := map[string][]postRecord{
		"phantom":   append(entries([2]int{9, 3}), behind[1:]...),
		"duplicate": append(entries([2]int{1, 11}), behind...)[:7],
		"misorder":  misordered,
		"too many":  append(behind, entries([2]int{1, 5}, [2]int{2, 4}, [2]int{1, 3}, [2]int{2, 2})...),
	}
	for name, page := range wrong {
		if err := verifyFeed(safety, page); err == nil {
			t.Errorf("the safety form accepted a %s page", name)
		}
	}
}

func TestVerifyProfileTimeline(t *testing.T) {
	e := postsExpectation{Author: "4", Limit: 20, Base: 10, AckedBefore: 1, IssuedAfter: 2}
	newestFirst := func(top int64, count int) []postRecord {
		page := make([]postRecord, count)
		for i := range page {
			page[i] = postRecord{Author: "4", Seq: top - int64(i)}
		}
		return page
	}
	cases := []struct {
		name    string
		expect  postsExpectation
		page    []postRecord
		wantErr string
	}{
		{"one new post in flight, seen", e, newestFirst(10, 11), ""},
		{"the in-flight post not yet seen", e, newestFirst(9, 10)[:10], "below the"},
		{"too many posts", e, newestFirst(11, 13), "more than"},
		{"someone else's post", e, append(newestFirst(10, 10), postRecord{Author: "5", Seq: 0}), "by \"5\""},
		{"oldest first", e, append([]postRecord{{Author: "4", Seq: 3}}, newestFirst(10, 10)...), "not newest-first"},
		{"a gap with nothing in flight", postsExpectation{Author: "4", Limit: 5, Base: 10, AckedBefore: 2, IssuedAfter: 2},
			[]postRecord{{Author: "4", Seq: 11}, {Author: "4", Seq: 10}, {Author: "4", Seq: 9}, {Author: "4", Seq: 7}, {Author: "4", Seq: 6}}, "lost a post"},
		{"safety form tolerates a replica behind", postsExpectation{Author: "4", Limit: 20, Base: 10, AckedBefore: 2, IssuedAfter: 2, Safety: true}, newestFirst(9, 10), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyProfileTimeline(tc.expect, tc.page)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("correct page rejected: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyPostPage(t *testing.T) {
	e := postPageExpectation{Author: "4", Seq: 3, BaseLikes: 2, BaseComments: 1, LikesIssued: 2, CommentsAckedBefore: 1, CommentsIssued: 2}
	post := postRecord{Author: "4", Seq: 3}
	cases := []struct {
		name    string
		expect  postPageExpectation
		page    postPage
		wantErr string
	}{
		{"within bounds", e, postPage{Found: true, Post: post, Likes: 3, Comments: 2}, ""},
		{"the post is missing", e, postPage{}, "no post 3"},
		{"another post came back", e, postPage{Found: true, Post: postRecord{Author: "4", Seq: 4}, Likes: 2, Comments: 2}, "returned post 4"},
		{"likes lost", e, postPage{Found: true, Post: post, Likes: 1, Comments: 2}, "likes"},
		{"likes invented", e, postPage{Found: true, Post: post, Likes: 5, Comments: 2}, "likes"},
		{"an acknowledged comment lost", e, postPage{Found: true, Post: post, Likes: 2, Comments: 1}, "comments"},
		{"safety form tolerates a replica behind on comments", func() postPageExpectation { s := e; s.Safety = true; return s }(), postPage{Found: true, Post: post, Likes: 2, Comments: 1}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyPostPage(tc.expect, tc.page)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("correct page rejected: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
	var miss *readMiss
	if err := verifyPostPage(e, postPage{}); !errors.As(err, &miss) {
		t.Errorf("a missing post is %T; want a *readMiss so hits can be told from misses", err)
	}
}

func TestVerifyFollowCheck(t *testing.T) {
	cases := []struct {
		name    string
		expect  followExpectation
		follows bool
		wantErr bool
	}{
		{"seeded edge, answered yes", followExpectation{Seeded: true}, true, false},
		{"seeded edge, answered no", followExpectation{Seeded: true}, false, true},
		{"no edge, answered yes", followExpectation{}, true, true},
		{"no edge, answered no", followExpectation{}, false, false},
		{"touched pair, either answer", followExpectation{Seeded: true, Touched: true}, false, false},
	}
	for _, tc := range cases {
		if err := verifyFollowCheck(tc.expect, tc.follows); (err != nil) != tc.wantErr {
			t.Errorf("%s: error = %v, want error=%v", tc.name, err, tc.wantErr)
		}
	}
}
