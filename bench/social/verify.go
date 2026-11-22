package main

import (
	"fmt"
	"strconv"
)

// postView is the part of a stored post the verifier reads back.
type postView struct {
	Author string `bson:"author"`
	Seq    int64  `bson:"seq"`
}

// nextSeqAfter is one more than the highest sequence number in posts, or 0 for
// none. It scans every post rather than reading the first: two pushes in flight
// together may land in either order, so the newest post in a timeline is not
// always its highest.
func nextSeqAfter(posts []postView) int64 {
	var next int64
	for _, post := range posts {
		next = max(next, post.Seq+1)
	}
	return next
}

// readExpectation is what the writer bookkeeping says a timeline read may
// return. Counts are for writes made in this run; BaseTotal and SeqBase
// describe the timeline when the run started (after seeding or on -attach).
type readExpectation struct {
	Author      string
	Limit       int
	BaseTotal   int64
	SeqBase     int64 // sequence number the first write of this run uses
	AckedBefore int64 // writes acknowledged before the read was sent
	IssuedAfter int64 // writes started by the time the read returned
	// Quiescent means no write to this timeline was in flight or started
	// during the read, so the answer is exact rather than bounded.
	Quiescent bool
	// PriorOrdered says the posts older than SeqBase are known to be in strict
	// order (this run seeded them one at a time). On -attach it is false: an
	// earlier run may have left overlap disorder this run cannot see, so only
	// the order of this run's own posts is judged.
	PriorOrdered bool
	// Overlaps counts writes to this timeline, over its whole life in this
	// run, that began while another write to it was in flight. Two pushes in
	// flight together may land in either order, and that disorder is stored
	// permanently, so it is the budget for out-of-order pairs on every later
	// read, not just a concurrent one.
	Overlaps int64
	// Slack is the largest seq inversion concurrent writers can cause: two
	// pushes in flight together may land in either order, and at most one is
	// in flight per worker.
	Slack int64
}

// verifyTimeline checks one newest-first page. total is the scan's whole-
// bucket count, which comes from the same snapshot as posts.
func verifyTimeline(e readExpectation, total int64, posts []postView) error {
	if low, high := e.BaseTotal+e.AckedBefore, e.BaseTotal+e.IssuedAfter; total < low || total > high {
		return fmt.Errorf("timeline %s holds %d posts, expected %d..%d", e.Author, total, low, high)
	}
	if want := min(int64(e.Limit), total); int64(len(posts)) != want {
		return fmt.Errorf("timeline %s returned %d posts, expected %d of %d", e.Author, len(posts), want, total)
	}
	for position, post := range posts {
		if post.Author != e.Author {
			return fmt.Errorf("timeline %s position %d holds a post by %q (seq %d)", e.Author, position, post.Author, post.Seq)
		}
		if position > 0 && posts[position-1].Seq < e.SeqBase && post.Seq >= e.SeqBase {
			return fmt.Errorf("timeline %s position %d: a post from this run (seq %d) sits below an older one (seq %d)",
				e.Author, position, post.Seq, posts[position-1].Seq)
		}
	}
	if e.Quiescent {
		if want := e.BaseTotal + e.IssuedAfter; total != want {
			return fmt.Errorf("timeline %s holds %d posts with no write in flight, expected exactly %d", e.Author, total, want)
		}
		if e.Overlaps == 0 {
			return verifyExactOrder(e, posts)
		}
	}
	return verifyBoundedOrder(e, posts)
}

// verifyBoundedOrder allows only the disorder overlapping writers can cause:
// a push that lands after a later-numbered one puts one adjacent pair out of
// order, so the page may hold at most one such pair per overlapped write. A
// fully reversed page, or one with duplicates, exceeds that.
func verifyBoundedOrder(e readExpectation, posts []postView) error {
	var inversions int64
	for position := 1; position < len(posts); position++ {
		newer, older := posts[position-1], posts[position]
		if !e.judges(newer, older) {
			continue
		}
		if newer.Seq == older.Seq {
			return fmt.Errorf("timeline %s holds seq %d twice, at positions %d and %d", e.Author, newer.Seq, position-1, position)
		}
		if newer.Seq < older.Seq {
			inversions++
			if older.Seq-newer.Seq > e.Slack {
				return fmt.Errorf("timeline %s not newest-first at position %d: seq %d above seq %d (concurrency slack %d)",
					e.Author, position, newer.Seq, older.Seq, e.Slack)
			}
		}
	}
	if inversions > e.Overlaps {
		return fmt.Errorf("timeline %s not newest-first: %d out-of-order pairs but only %d writes overlapped another write to this timeline",
			e.Author, inversions, e.Overlaps)
	}
	return nil
}

// verifyExactOrder is for a timeline that never had overlapping writers and
// is quiet now, where the page is fully determined.
func verifyExactOrder(e readExpectation, posts []postView) error {
	if len(posts) == 0 {
		return nil
	}
	// Before this run writes to an attached timeline, its newest post is from an
	// earlier run whose closing burst may have left it out of order, so the top
	// is only pinned once this run has written or the old posts are known ordered.
	if wantTop := e.SeqBase + e.IssuedAfter - 1; (e.IssuedAfter > 0 || e.PriorOrdered) && posts[0].Seq != wantTop {
		return fmt.Errorf("timeline %s newest post has seq %d with no write in flight, expected %d", e.Author, posts[0].Seq, wantTop)
	}
	for position := 1; position < len(posts); position++ {
		newer, older := posts[position-1], posts[position]
		if !e.judges(newer, older) {
			continue
		}
		if newer.Seq <= older.Seq {
			return fmt.Errorf("timeline %s not newest-first at position %d: seq %d above seq %d", e.Author, position, newer.Seq, older.Seq)
		}
		// Posts written in this run were numbered consecutively; older
		// ones may have gaps if a previous run lost writes.
		if older.Seq >= e.SeqBase && newer.Seq != older.Seq+1 {
			return fmt.Errorf("timeline %s lost a post between seq %d and seq %d", e.Author, older.Seq, newer.Seq)
		}
	}
	return nil
}

// judges reports whether the order of an adjacent pair is checkable: always
// for this run's posts, and for older ones only when they are known ordered.
func (e readExpectation) judges(newer, older postView) bool {
	return e.PriorOrdered || (newer.Seq >= e.SeqBase && older.Seq >= e.SeqBase)
}

// The checks below are for the social operations of spec 6.3 (R1, R3, R4, R5).
// Each takes the answer and an expectation the writers' bookkeeping built, and
// runs after the operation's clock has stopped. A check has an exact form and
// a safety form (spec 6.6): the exact form is for a single node, where an
// acknowledged write is visible to the next read; the safety form is for
// replicated routing, where a replica may legitimately be behind, so it
// rejects only what no replica may return: a phantom, a duplicate, a wrong
// order within one author, a count above what was issued.

// readMiss is a read that found nothing where the bookkeeping says something
// must be: hits must equal verified reads (P1).
type readMiss struct{ what string }

func (m *readMiss) Error() string { return m.what }

// feedExpectation is what the bookkeeping says R1 may return.
type feedExpectation struct {
	Actor string
	Limit int
	// Base is how many entries the feed held when the run started; on an
	// attached dataset an earlier run may have added more, so it is only a
	// floor (BaseIsFloor) and the count has no upper bound.
	Base        int64
	BaseIsFloor bool
	// AckedBefore is the fan-out copies into this feed acknowledged before the
	// read was sent; IssuedAfter the copies started by the time it returned.
	AckedBefore, IssuedAfter int64
	// Followed says whether the actor may hold a post by author: they followed
	// them at seeding, or this run touched the pair.
	Followed func(author string) bool
	// PostsBound is an upper bound on the number of posts the author has, so a
	// sequence number at or above it names a post that was never written.
	PostsBound func(author string) int64
	// Safety selects the safety form for replicated routing.
	Safety bool
}

// verifyFeed checks one newest-first page of a home timeline.
//
// The count is exact when the writers have nothing in flight: a page holds
// min(limit, base + acknowledged) to min(limit, base + issued) entries, and the
// range is one number when the two agree. In the safety form only the floor
// (the seeded entries) is required, because a replica may be behind. A phantom
// (an author the actor never followed, a post that was never written), a
// duplicate, and an author's own entries out of order are wrong in both forms;
// the order between different authors is not checked, because the fan-out of
// concurrent posts may interleave differently on every engine.
func verifyFeed(e feedExpectation, entries []postRecord) error {
	if err := verifyCount("feed of user "+e.Actor, e.Limit, int64(len(entries)), countBounds{
		Base: e.Base, AckedBefore: e.AckedBefore, IssuedAfter: e.IssuedAfter, BaseIsFloor: e.BaseIsFloor, Safety: e.Safety,
	}); err != nil {
		return err
	}
	seen := make(map[[2]string]bool, len(entries))
	lastSeq := make(map[string]int64, len(entries))
	for position, entry := range entries {
		switch {
		case !e.Followed(entry.Author):
			return fmt.Errorf("feed of user %s position %d holds a post by user %s, whom they never followed (seq %d)", e.Actor, position, entry.Author, entry.Seq)
		case entry.Seq < 0 || entry.Seq >= e.PostsBound(entry.Author):
			return fmt.Errorf("feed of user %s position %d holds seq %d of user %s, a post that was never written (at most %d exist)", e.Actor, position, entry.Seq, entry.Author, e.PostsBound(entry.Author))
		}
		key := [2]string{entry.Author, strconv.FormatInt(entry.Seq, 10)}
		if seen[key] {
			return fmt.Errorf("feed of user %s holds seq %d of user %s twice (second at position %d)", e.Actor, entry.Seq, entry.Author, position)
		}
		seen[key] = true
		if previous, ok := lastSeq[entry.Author]; ok && entry.Seq > previous {
			return fmt.Errorf("feed of user %s is not newest-first for user %s: seq %d at position %d follows seq %d", e.Actor, entry.Author, entry.Seq, position, previous)
		}
		lastSeq[entry.Author] = entry.Seq
	}
	return nil
}

// countBounds is what a page's length may be: the entries present at the start,
// those acknowledged since, and those started.
type countBounds struct {
	Base                     int64
	AckedBefore, IssuedAfter int64
	BaseIsFloor, Safety      bool
}

// verifyCount checks a page of got entries against the bounds, for a page of at
// most limit.
func verifyCount(what string, limit int, got int64, bounds countBounds) error {
	floor := bounds.Base + bounds.AckedBefore
	if bounds.Safety {
		floor = bounds.Base
	}
	if low := min(int64(limit), floor); got < low {
		return fmt.Errorf("%s holds %d entries, below the %d it must (%d at the start, %d acknowledged since)", what, got, low, bounds.Base, bounds.AckedBefore)
	}
	if bounds.BaseIsFloor {
		return nil
	}
	if high := min(int64(limit), bounds.Base+bounds.IssuedAfter); got > high {
		return fmt.Errorf("%s holds %d entries, more than the %d that were ever issued (%d at the start, %d started since)", what, got, high, bounds.Base, bounds.IssuedAfter)
	}
	return nil
}

// postsExpectation is what the bookkeeping says R3 may return.
type postsExpectation struct {
	Author string
	Limit  int
	// Base is the author's post count when the run started (a floor on an
	// attached dataset).
	Base        int64
	BaseIsFloor bool
	// AckedBefore and IssuedAfter count this run's posts by the author.
	AckedBefore, IssuedAfter int64
	Safety                   bool
}

// verifyProfileTimeline checks one page of an author's own posts, newest first.
//
// R3's order is exact even under concurrent writers: the keys descend with the
// sequence number, so the store lists by sequence whatever order the posts
// arrived in. With nothing in flight the page is also gapless from the top.
func verifyProfileTimeline(e postsExpectation, entries []postRecord) error {
	if err := verifyCount("timeline of user "+e.Author, e.Limit, int64(len(entries)), countBounds{
		Base: e.Base, AckedBefore: e.AckedBefore, IssuedAfter: e.IssuedAfter, BaseIsFloor: e.BaseIsFloor, Safety: e.Safety,
	}); err != nil {
		return err
	}
	for position, entry := range entries {
		if entry.Author != e.Author {
			return fmt.Errorf("timeline of user %s position %d holds a post by %q (seq %d)", e.Author, position, entry.Author, entry.Seq)
		}
		if position == 0 {
			continue
		}
		switch previous := entries[position-1].Seq; {
		case entry.Seq == previous:
			return fmt.Errorf("timeline of user %s holds seq %d twice, at positions %d and %d", e.Author, entry.Seq, position-1, position)
		case entry.Seq > previous:
			return fmt.Errorf("timeline of user %s is not newest-first at position %d: seq %d follows seq %d", e.Author, position, entry.Seq, previous)
		}
	}
	quiet := !e.Safety && !e.BaseIsFloor && e.AckedBefore == e.IssuedAfter
	if !quiet || len(entries) == 0 {
		return nil
	}
	if want := e.Base + e.IssuedAfter - 1; entries[0].Seq != want {
		return fmt.Errorf("timeline of user %s newest post has seq %d with no write in flight, expected %d", e.Author, entries[0].Seq, want)
	}
	for position := 1; position < len(entries); position++ {
		if entries[position-1].Seq != entries[position].Seq+1 {
			return fmt.Errorf("timeline of user %s lost a post between seq %d and seq %d", e.Author, entries[position].Seq, entries[position-1].Seq)
		}
	}
	return nil
}

// postPageExpectation is what the bookkeeping says R4 may return for one post.
type postPageExpectation struct {
	Author string
	Seq    int64
	// BaseLikes and BaseComments are the counts the seed gave the post.
	BaseLikes, BaseComments int64
	// BaseIsFloor says an earlier run may have added reactions, so the seeded
	// counts bound from below only.
	BaseIsFloor bool
	// LikesIssued is the likes this run started on the post (a like by someone
	// who already liked it adds nothing, so it bounds from above only);
	// CommentsAckedBefore and CommentsIssued bound the comments.
	LikesIssued, CommentsAckedBefore, CommentsIssued int64
	Safety                                           bool
}

// verifyPostPage checks R4's answer.
//
// A like by someone who already liked the post adds nothing, so the likes are
// bounded from above by the likes started and from below by the seed only.
func verifyPostPage(e postPageExpectation, page postPage) error {
	if !page.Found {
		return &readMiss{what: fmt.Sprintf("user %s has no post %d, which the seed wrote", e.Author, e.Seq)}
	}
	if page.Post.Author != e.Author || page.Post.Seq != e.Seq {
		return fmt.Errorf("asked for post %d of user %s and returned post %d of user %s", e.Seq, e.Author, page.Post.Seq, page.Post.Author)
	}
	if low, high := e.BaseLikes, e.BaseLikes+e.LikesIssued; page.Likes < low || (page.Likes > high && !e.BaseIsFloor) {
		return fmt.Errorf("post %d of user %s shows %d likes, expected %d..%d", e.Seq, e.Author, page.Likes, low, high)
	}
	commentsFloor := e.BaseComments + e.CommentsAckedBefore
	if e.Safety {
		commentsFloor = e.BaseComments
	}
	if high := e.BaseComments + e.CommentsIssued; page.Comments < commentsFloor || (page.Comments > high && !e.BaseIsFloor) {
		return fmt.Errorf("post %d of user %s shows %d comments, expected %d..%d", e.Seq, e.Author, page.Comments, commentsFloor, high)
	}
	return nil
}

// followExpectation is what the bookkeeping says R5 must answer.
type followExpectation struct {
	Actor, Target string
	// Seeded says the actor followed the target when the dataset was seeded.
	Seeded bool
	// Touched says this run followed or unfollowed the pair, after which the
	// answer depends on the order of concurrent operations and either is valid.
	Touched bool
}

// verifyFollowCheck checks R5's answer.
func verifyFollowCheck(e followExpectation, follows bool) error {
	if e.Touched || follows == e.Seeded {
		return nil
	}
	return fmt.Errorf("user %s %s user %s, but the follow graph says %s", e.Actor, answerWord(follows), e.Target, answerWord(e.Seeded))
}

func answerWord(follows bool) string {
	if follows {
		return "follows"
	}
	return "does not follow"
}
