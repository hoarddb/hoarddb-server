package harness_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestGetManyAcrossAPlacementChangeAnswersPerKey: frames are sent to n1 without a pause while a
// fourth node joins, pulls its pairs and is activated (the owner table and the epoch change under
// them). No frame is refused whole; a key either answers its document or its own error, and a
// document that exists is never answered found:false. When the node is active every key answers.
// A raw frame that carries the epoch from before the change then answers STALE_EPOCH per key, with
// the detail a driver refreshes from, and the frame is still a reply.
func TestGetManyAcrossAPlacementChangeAnswersPerKey(t *testing.T) {
	c, ids := getManyClusterWith(t, 60, harness.Options{Seed: wire.SeedOptions{SlotsInFlight: 8}})
	client := newGetManyClient(t, c, "n1")
	if warm := client.sendSettled(wire.RingPrologue{}, client.reads(ids...)); warm.Outcome != "ok" {
		t.Fatalf("the warm-up frame answered %q", warm.Outcome)
	}
	epochBefore := c.Node("n1").State().Epoch

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	frames, partial := 0, 0
	problems, answeredWith := map[string]int{}, map[string]int{}
	note := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		problems[fmt.Sprintf(format, args...)]++
	}
	// A join's own steps (register, promote) are refused when the state moves under them, which
	// background frames do not cause but a leader's seed reports can: the frames start once the
	// joiner is a member, and run through its seeding, its activation and the epoch change.
	added := addNodeRetrying(t, c)
	var stopOnce sync.Once
	stopFrames := func() {
		stopOnce.Do(func() { close(stop) })
		wg.Wait()
	}
	t.Cleanup(stopFrames)
	wg.Add(1)
	go func() {
		defer wg.Done()
		reads := client.reads(ids...)
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			reply, err := client.try(ctx, wire.RingPrologue{}, reads)
			cancel()
			if err != nil {
				note("a frame failed whole: %v", err)
				continue
			}
			mu.Lock()
			frames++
			if reply.Outcome == "partial" {
				partial++
			}
			mu.Unlock()
			for i, id := range ids {
				switch got := docID(reply.Answers[i]); got {
				case id:
				case "":
					note("%s, a document that exists, answered found:false", id)
				default:
					// A per-key error while the table moves is allowed; the frame is not.
					mu.Lock()
					answeredWith[got]++
					mu.Unlock()
				}
			}
		}
	}()

	c.ActivateWhileSeedingMoves(added, 90*time.Second, 300*time.Second)
	c.AwaitConverged(20 * time.Second)
	c.AwaitLeases(20 * time.Second)
	stopFrames()
	if c.Node("n1").State().Epoch <= epochBefore {
		t.Fatalf("the epoch is still %d after %s was activated; the test proves nothing", epochBefore, added)
	}
	mu.Lock()
	t.Logf("%d frames were answered during the join, %d of them partial; per-key errors: %v", frames, partial, answeredWith)
	for problem, n := range problems {
		t.Errorf("%dx %s", n, problem)
	}
	mu.Unlock()
	if frames == 0 {
		t.Fatal("no frame was answered while the node joined")
	}

	// The cluster has settled: every key answers its document, whichever node is asked. A lease
	// or a majority that the join has not yet renewed refuses a lone read the same way (BUG-0808),
	// so a refusal is retried for a while; a wrong answer never is.
	for _, via := range c.IDs() {
		settled := newGetManyClient(t, c, via)
		c.Await("every key answered through "+via, 60*time.Second, func() bool {
			reply, err := settled.try(proofCtx(t), wire.RingPrologue{}, settled.reads(ids...))
			if err != nil {
				return false
			}
			for i, id := range ids {
				got := docID(reply.Answers[i])
				if got == "" {
					t.Fatalf("after the join, via %s: %s, a document that exists, answered found:false", via, id)
				}
				if got != id {
					return false
				}
			}
			return true
		})
	}

	// A frame stamped with the old epoch is judged per key.
	stale := client.send(wire.RingPrologue{Epoch: uint64(epochBefore)}, client.reads(ids[:5]...))
	if stale.Outcome != "partial" {
		t.Errorf("a frame with the epoch from before the change answered %q, want partial", stale.Outcome)
	}
	for i, answer := range stale.Answers {
		if ringCode(answer) != cluster.CodeStaleEpoch {
			t.Errorf("key %d of the stale frame answered %+v, want STALE_EPOCH", i, answer)
			continue
		}
		if answer.Error.Detail == nil {
			t.Errorf("key %d: STALE_EPOCH carries no detail to refresh from", i)
		}
	}
}

// addNodeRetrying is Cluster.AddNode with one more retry: a join step the leader refuses because the
// state moved under it (a seed report landed between building the command and committing it)
// changed nothing and is run again.
func addNodeRetrying(t *testing.T, c *harness.Cluster) string {
	t.Helper()
	id := c.OpenJoiner()
	for _, step := range []struct {
		name string
		run  func(ctx context.Context, leader *wire.Node) error
	}{
		{"joining", func(ctx context.Context, leader *wire.Node) error {
			return leader.JoinRegisterOnly(ctx, id, c.Addr(id))
		}},
		{"promoting", func(ctx context.Context, leader *wire.Node) error { return leader.Promote(ctx, id) }},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		for {
			err := step.run(ctx, c.Node(c.AwaitLeader(30*time.Second)))
			if err == nil {
				break
			}
			if ctx.Err() != nil || (!strings.Contains(err.Error(), "leader") && !strings.Contains(err.Error(), "state_seq")) {
				cancel()
				t.Fatalf("%s %s: %v", step.name, id, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		cancel()
	}
	return id
}
