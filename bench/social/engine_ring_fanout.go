package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// defaultRingFanoutWidth is how many feed pushes of one publish are in flight at once with
// -ring-fanout-parallel. It bounds connections' worth of work per client: 64 clients at
// width 8 are at most 512 requests in flight, on 64 connections and no extra connections.
const defaultRingFanoutWidth = 8

// feedPushRetries is how many times a push whose outcome is unknown is sent again, under the
// request id of its first attempt.
const feedPushRetries = 2

// pushFeeds pushes element onto the Feed of every follower. With fanoutWidth below 2 the
// pushes go one after another and the first failure stops the fan-out, as the other engines
// do. Otherwise up to fanoutWidth pushes are in flight together on the connection, so the
// owner's WAL can group their fsyncs. Each push has its own request id: it is retried alone,
// under that id, when its outcome is unknown, and the owner's dedupe window answers a copy it
// already applied. A follower named twice is pushed in list order by one worker, so pushes to
// one entity keep their order. No push starts after a failure; the pushes already in flight
// finish, and the error returned is the failed push of the earliest follower in the list.
func (o *ringOps) pushFeeds(ctx context.Context, followers []int, element any, ledger deliveryLedger) error {
	if o.batchSize > 0 {
		return o.pushFeedFrames(ctx, followers, element, ledger)
	}
	if o.fanoutWidth < 2 || len(followers) < 2 {
		for _, follower := range followers {
			ledger.sending(follower)
			if err := o.api.push(ctx, userStore(follower, ringload.UserFeed), element); err != nil {
				return fmt.Errorf("into the feed of user %d: %w", follower, err)
			}
			ledger.delivered(follower)
		}
		return nil
	}
	positions := map[int][]int{} // follower -> its places in followers, in order
	order := make([]int, 0, len(followers))
	for position, follower := range followers {
		if _, seen := positions[follower]; !seen {
			order = append(order, follower)
		}
		positions[follower] = append(positions[follower], position)
	}
	failures := make([]error, len(followers))
	var (
		mutex   sync.Mutex
		next    int
		stopped bool
		group   sync.WaitGroup
	)
	claim := func() (int, bool) {
		mutex.Lock()
		defer mutex.Unlock()
		if stopped || next == len(order) {
			return 0, false
		}
		next++
		return order[next-1], true
	}
	for range min(o.fanoutWidth, len(order)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				follower, more := claim()
				if !more {
					return
				}
				for _, position := range positions[follower] {
					ledger.sending(follower)
					if err := o.pushRetrying(ctx, userStore(follower, ringload.UserFeed), element); err != nil {
						failures[position] = fmt.Errorf("into the feed of user %d: %w", follower, err)
						mutex.Lock()
						stopped = true
						mutex.Unlock()
						break
					}
					ledger.delivered(follower)
				}
			}
		}()
	}
	group.Wait()
	for _, failure := range failures {
		if failure != nil {
			return failure
		}
	}
	return nil
}

// pushRetrying sends one push under a fresh request id and sends it again under the same id
// when the outcome is unknown, never when the owner refused it.
func (o *ringOps) pushRetrying(ctx context.Context, store storeRef, element any) error {
	id := o.api.nextRequestID()
	err := o.api.pushWithID(ctx, store, element, id)
	for attempt := 0; err != nil && attempt < feedPushRetries && outcomeUnknown(err) && ctx.Err() == nil; attempt++ {
		err = o.api.pushWithID(ctx, store, element, id)
	}
	return err
}

// outcomeUnknown is whether err leaves a write possibly applied: the connection or the wait
// failed, or the node was unavailable. A refusal of any other code means nothing was written.
func outcomeUnknown(err error) bool {
	var refusal *ringclient.Error
	if errors.As(err, &refusal) {
		return refusal.Code == cluster.CodeUnavailable
	}
	return true
}
