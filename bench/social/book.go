package main

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
)

// datasetSeed fixes the social graph. It is not the workload seed: the same
// dataset must be generated for every engine and every run, whatever -rand-seed
// says, or an attached dataset would not match the bookkeeping.
const datasetSeed uint64 = 20261001

// socialBook is the writers' bookkeeping for a mix run: what the dataset held
// at the start (the graph, which is deterministic) and what this run has
// changed. Every method is safe for concurrent use.
//
// Every counter is a pair: what was issued (started) and what was acknowledged.
// A write whose outcome is unknown stays issued and never becomes
// acknowledged, so the pair still brackets the truth, and the checks that need
// the two to be equal (the exact ones) stop applying to what it touched.
type socialBook struct {
	graph *socialGraph
	users []bookUser
	// attached says the dataset came from an earlier run, whose changes the graph
	// cannot account for: the seeded figures are then floors, not exact.
	attached bool
	// avatarOwners are the users the seed gave an avatar, which operation R7
	// reads.
	avatarOwners []int
	// seededLikes and seededComments count, per post (author times posts per
	// user plus sequence), what the seed gave it. A like is keyed by its liker,
	// so a repeated (post, liker) pair in the generator is one like.
	seededLikes, seededComments []uint16
	posts                       sync.Map // postRef -> *postCounters
	// acks is the acknowledged-write ledger of -ledger-out.
	acks    *ackLedger
	touched sync.Map // pairKey -> struct{}
}

// bookUser is one user's mutable bookkeeping.
type bookUser struct {
	// authoring serialises one author's posts so that their sequence numbers
	// are also the order their copies reach every follower's feed; the
	// per-author order check of spec 6.6 depends on it.
	authoring sync.Mutex
	// nextSeq is the sequence number of the author's next post, or -1 when the
	// dataset was attached and the newest post has not been read yet.
	nextSeq int64
	// postsBound is an upper bound on the number of posts the author has: a
	// sequence number at or above it was never written. It rises before the post
	// it makes room for is sent.
	postsBound              atomic.Int64
	postsIssued, postsAcked atomic.Int64
	feedIssued, feedAcked   atomic.Int64 // copies into this user's feed
}

type postRef struct{ author, seq int32 }

// postCounters are one post's reaction counters for this run.
type postCounters struct {
	likesIssued                   atomic.Int64
	commentsIssued, commentsAcked atomic.Int64
}

// pairKey names an (actor, target) follow edge.
type pairKey struct{ actor, target int32 }

// newSocialBook starts the bookkeeping for graph. attached says the dataset
// came from an earlier run.
func newSocialBook(graph *socialGraph, attached bool) *socialBook {
	book := &socialBook{graph: graph, users: make([]bookUser, graph.spec.Users), attached: attached, acks: newAckLedger()}
	for user := range book.users {
		book.users[user].nextSeq = int64(graph.postsPerUser)
		book.users[user].postsBound.Store(int64(graph.postsPerUser))
		if attached {
			book.users[user].nextSeq = -1
			book.users[user].postsBound.Store(math.MaxInt64)
		}
	}
	for item := range graph.media() {
		if item.Kind == mediaAvatar {
			book.avatarOwners = append(book.avatarOwners, item.Owner)
		}
	}
	book.countSeededReactions()
	return book
}

func (b *socialBook) postIndex(author int, seq int) int { return author*b.graph.postsPerUser + seq }

// countSeededReactions counts the likes and comments the seed gave each post.
func (b *socialBook) countSeededReactions() {
	b.seededLikes, b.seededComments = seededReactionCounts(b.graph)
}

// beginPost takes the author's next sequence number and holds the author's
// posting lock until finish is called, so one author's posts are published in
// sequence order. finish(true) says the post was acknowledged. On an attached
// dataset the first call reads the newest post to learn where the sequence stands.
func (b *socialBook) beginPost(ctx context.Context, ops socialOps, author int) (seq int64, finish func(acknowledged bool), err error) {
	user := &b.users[author]
	user.authoring.Lock()
	if user.nextSeq < 0 {
		newest, err := ops.profileTimeline(ctx, author, 1)
		if err != nil {
			user.authoring.Unlock()
			return 0, nil, fmt.Errorf("reading the newest post of user %d to resume its numbering: %w", author, err)
		}
		user.nextSeq = 0
		if len(newest) == 1 {
			user.nextSeq = newest[0].Seq + 1
		}
	}
	seq = user.nextSeq
	user.nextSeq++
	user.postsBound.Store(seq + 1)
	user.postsIssued.Add(1)
	b.acks.sending(author, seq)
	return seq, func(acknowledged bool) {
		if acknowledged {
			user.postsAcked.Add(1)
			b.acks.acknowledged(author, seq)
		}
		user.authoring.Unlock()
	}, nil
}

// bookLedger tells the book which feeds a post's fan-out is copying into.
type bookLedger struct{ book *socialBook }

func (l bookLedger) sending(follower int)   { l.book.users[follower].feedIssued.Add(1) }
func (l bookLedger) delivered(follower int) { l.book.users[follower].feedAcked.Add(1) }

func (b *socialBook) ledger() deliveryLedger { return bookLedger{b} }

func (b *socialBook) counters(author int, seq int64) *postCounters {
	ref := postRef{int32(author), int32(seq)}
	if counters, ok := b.posts.Load(ref); ok {
		return counters.(*postCounters)
	}
	counters, _ := b.posts.LoadOrStore(ref, &postCounters{})
	return counters.(*postCounters)
}

// touch records that this run followed or unfollowed the pair.
func (b *socialBook) touch(actor, target int) {
	b.touched.Store(pairKey{int32(actor), int32(target)}, struct{}{})
}

func (b *socialBook) wasTouched(actor, target int) bool {
	_, touched := b.touched.Load(pairKey{int32(actor), int32(target)})
	return touched
}

// seededFollow says whether actor followed target when the dataset was seeded.
func (b *socialBook) seededFollow(actor, target int) bool {
	return slices.Contains(b.graph.following(actor), int32(target))
}

// feedExpectation is what R1 may return for actor, for a read that was sent
// when ackedBefore copies had been acknowledged into the feed. It is built
// after the read returns, so the issued count is the latest.
func (b *socialBook) feedExpectation(actor int, ackedBefore int64, limit int, safety bool) feedExpectation {
	user := &b.users[actor]
	return feedExpectation{
		Actor: userID(actor), Limit: limit,
		Base: int64(b.graph.feedQuota[actor]), BaseIsFloor: b.attached,
		AckedBefore: ackedBefore, IssuedAfter: user.feedIssued.Load(),
		Followed: func(author string) bool {
			other, known := b.rank(author)
			return known && (b.attached || b.seededFollow(actor, other) || b.wasTouched(actor, other))
		},
		PostsBound: func(author string) int64 {
			if other, known := b.rank(author); known {
				return b.users[other].postsBound.Load()
			}
			return 0
		},
		Safety: safety,
	}
}

// rank is the user rank an id names; false for an id that is not a user of
// the dataset, which a feed entry can only carry if the server invented it.
func (b *socialBook) rank(id string) (int, bool) {
	rank, err := strconv.Atoi(id)
	return rank, err == nil && rank >= 0 && rank < len(b.users)
}

func (b *socialBook) postsExpectation(author int, ackedBefore int64, limit int, safety bool) postsExpectation {
	user := &b.users[author]
	return postsExpectation{
		Author: userID(author), Limit: limit,
		Base: int64(b.graph.postsPerUser), BaseIsFloor: b.attached,
		AckedBefore: ackedBefore, IssuedAfter: user.postsIssued.Load(), Safety: safety,
	}
}

func (b *socialBook) postPageExpectation(author int, seq int64, commentsAckedBefore int64, safety bool) postPageExpectation {
	counters := b.counters(author, seq)
	index := b.postIndex(author, int(seq))
	return postPageExpectation{
		Author: userID(author), Seq: seq,
		BaseLikes: int64(b.seededLikes[index]), BaseComments: int64(b.seededComments[index]), BaseIsFloor: b.attached,
		LikesIssued: counters.likesIssued.Load(), CommentsAckedBefore: commentsAckedBefore, CommentsIssued: counters.commentsIssued.Load(),
		Safety: safety,
	}
}

func (b *socialBook) followExpectation(actor, target int) followExpectation {
	return followExpectation{
		Actor: userID(actor), Target: userID(target),
		Seeded: b.seededFollow(actor, target), Touched: b.attached || b.wasTouched(actor, target),
	}
}
