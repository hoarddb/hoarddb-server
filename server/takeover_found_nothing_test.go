package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestEveryWaiterOnATakeOverHearsItFoundNothing pins that the take-over's
// "no live copy anywhere" answer reaches every caller waiting on the name,
// not the first one to read it (BUG-0639). A create of a name a removed node
// owned (awaitCopyARemovedNodeMayHold), an operator's migrate and a holder's
// hand-off sweep all wait on one take-over of one name; when the answer was
// consumed by the first reader, the others waited out the write-concern
// timeout and the create was refused with the hand-off's retry.
func TestEveryWaiterOnATakeOverHearsItFoundNothing(t *testing.T) {
	const (
		waiters      = 3
		bucket       = "unheard"
		writeConcern = 3 * time.Second
	)
	tc := newTestClusterTuned(t, 3, 2, nil, func(_ int, c *Cluster) { c.wcTimeout = writeConcern })
	defer tc.stop()
	owner := clusterOf(tc, 0)
	key := placementOf(testDatabase, bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	answers := make(chan error, waiters)
	var started sync.WaitGroup
	for range waiters {
		started.Add(1)
		go func() {
			started.Done()
			answers <- owner.awaitOwnCopy(ctx, key, testDatabase, bucket)
		}()
	}
	started.Wait()
	// Let every waiter take its start time before the answer is recorded: a
	// waiter that starts after it does not hear it, by design.
	time.Sleep(200 * time.Millisecond)
	// The answer a take-over records when a migrate gave it leave, the only
	// one these waiters are answered from (on-demand pull spec §4.2, R-2).
	owner.takeOver.mu.Lock()
	owner.takeOver.foundNothing[key] = foundOutcome{at: time.Now(), epoch: owner.Epoch(), leave: true}
	owner.takeOver.mu.Unlock()

	heardBy := time.After(writeConcern / 2)
	for heard := 0; heard < waiters; heard++ {
		select {
		case err := <-answers:
			if !errors.Is(err, ErrMigrateBucketNotFound) {
				t.Fatalf("waiter %d of %d answered %v; want the take-over's found-nothing answer", heard+1, waiters, err)
			}
		case <-heardBy:
			t.Fatalf("only %d of %d waiters heard the take-over found nothing within %v; the rest wait on a record the first reader took",
				heard, waiters, writeConcern/2)
		}
	}
}
