package main

import (
	"context"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
)

// What the MongoDB and MySQL social engines share: a client-side time-ordered
// id (the idiomatic key of a feed, a notification or a comment, spec 6.2), a
// worker pool for the seeder and the per-post reaction counts the seed leaves.

// timeOrderedIDs hands out 63-bit ids that rise with time and are unique within
// the process: 41 bits of milliseconds, 10 random node bits chosen at start so
// two client processes rarely collide, and a 12-bit counter. A feed or an inbox
// sorts by this id, so a later push always has a larger id.
type timeOrderedIDs struct {
	mu   sync.Mutex
	last int64
	node int64
}

func newTimeOrderedIDs() *timeOrderedIDs {
	return &timeOrderedIDs{node: rand.Int64N(1 << 10)}
}

// next returns an id larger than every id this generator returned before.
func (g *timeOrderedIDs) next(nowMillis int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	candidate := nowMillis<<22 | g.node<<12
	if candidate <= g.last {
		candidate = g.last + 1
	}
	g.last = candidate
	return candidate
}

// seededReactionCounts counts, per post (author times posts per user plus
// sequence), the likes and comments the seed gives it. A like is the unique
// (post author, post sequence, liker), so a repeated triple in the generator is
// one like (the P4 ruling); comments are kept whole and self-likes stay.
func seededReactionCounts(graph *socialGraph) (likes, comments []uint16) {
	posts := graph.spec.Users * graph.postsPerUser
	likes, comments = make([]uint16, posts), make([]uint16, posts)
	pairs := make([]uint64, 0, graph.spec.Likes)
	for like := range graph.likes() {
		pairs = append(pairs, uint64(like.PostAuthor*graph.postsPerUser+like.PostSeq)<<32|uint64(like.Actor))
	}
	slices.Sort(pairs)
	for _, pair := range slices.Compact(pairs) {
		likes[pair>>32]++
	}
	for comment := range graph.comments() {
		comments[comment.PostAuthor*graph.postsPerUser+comment.PostSeq]++
	}
	return likes, comments
}

// chunks groups a stream into slices of at most size items.
func chunks[T any](items iter.Seq[T], size int) iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		group := make([]T, 0, size)
		for item := range items {
			group = append(group, item)
			if len(group) == size {
				if !yield(group) {
					return
				}
				group = make([]T, 0, size)
			}
		}
		if len(group) > 0 {
			yield(group)
		}
	}
}

// seedPool feeds items to conc workers and returns the first error. The
// producer stops at the first failure and the workers drain the queue, so
// nothing blocks and no goroutine outlives the call.
func seedPool[T any](ctx context.Context, conc int, doing string, items iter.Seq[T], write func(ctx context.Context, item T) error) error {
	queue := make(chan T, seedChannelDepth)
	var group sync.WaitGroup
	var firstFailure atomic.Pointer[error]
	for range conc {
		group.Add(1)
		go func() {
			defer group.Done()
			for item := range queue {
				if firstFailure.Load() != nil {
					continue
				}
				if err := write(ctx, item); err != nil {
					failure := fmt.Errorf("%s: %w", doing, err)
					firstFailure.CompareAndSwap(nil, &failure)
				}
			}
		}()
	}
	for item := range items {
		if firstFailure.Load() != nil {
			break
		}
		queue <- item
	}
	close(queue)
	group.Wait()
	if failure := firstFailure.Load(); failure != nil {
		return *failure
	}
	return nil
}
