package harness_test

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

// TestTheSameSeedBuildsTheSameSchedule: a seed is the whole schedule and every client's
// operation stream, and two seeds are not the same schedule.
func TestTheSameSeedBuildsTheSameSchedule(t *testing.T) {
	distinct := map[string]bool{}
	for seed := int64(1); seed <= 60; seed++ {
		first := harness.NewSchedule(seed, 3, 2, 14, harness.P1)
		second := harness.NewSchedule(seed, 3, 2, 14, harness.P1)
		if first.String() != second.String() || first.Duration != second.Duration {
			t.Fatalf("seed %d built two schedules:\n%s\nand\n%s", seed, first, second)
		}
		distinct[first.String()] = true
		for client := 0; client < 2; client++ {
			a, b := first.ClientStream(client), second.ClientStream(client)
			for op := 0; op < 100; op++ {
				if x, y := a.Next(), b.Next(); x != y {
					t.Fatalf("seed %d client %d operation %d drew %+v then %+v", seed, client, op, x, y)
				}
			}
		}
	}
	if len(distinct) < 55 {
		t.Fatalf("60 seeds built only %d distinct schedules: the seed is not what decides them", len(distinct))
	}
}

// walk replays a schedule against a model of which nodes are killed, stalled or cut off,
// calling check after every step.
type walkState struct {
	killed, stalled, cutOff map[string]bool
	open                    int
}

func walk(s harness.Schedule, check func(i int, st harness.Step, w *walkState)) {
	w := &walkState{killed: map[string]bool{}, stalled: map[string]bool{}, cutOff: map[string]bool{}}
	for i, st := range s.Steps {
		switch st.Kind {
		case harness.StepKill:
			w.killed[st.Nodes[0]] = true
		case harness.StepRestart:
			delete(w.killed, st.Nodes[0])
		case harness.StepStall:
			w.stalled[st.Nodes[0]] = true
		case harness.StepUnstall:
			delete(w.stalled, st.Nodes[0])
		case harness.StepPartition:
			for _, id := range st.Nodes {
				w.cutOff[id] = true
			}
			w.open++
		case harness.StepBlockOneWay:
			w.cutOff[st.Nodes[1]] = true
			w.open++
		case harness.StepHeal:
			clear(w.cutOff)
			w.open = 0
		}
		check(i, st, w)
	}
}

// TestAScheduleNeverKillsAMajority: over a thousand seeds, at three and at five voters, no
// prefix of a schedule has fewer than a majority of the nodes alive, and the nodes out of
// service for any reason (killed, stalled, cut off) are never a majority either.
func TestAScheduleNeverKillsAMajority(t *testing.T) {
	kills := 0
	for seed := int64(1); seed <= 1000; seed++ {
		for _, n := range []int{3, 5} {
			s := harness.NewSchedule(seed, n, 2, 14, harness.P1)
			walk(s, func(i int, st harness.Step, w *walkState) {
				if st.Kind == harness.StepKill {
					kills++
				}
				out := map[string]bool{}
				for _, set := range []map[string]bool{w.killed, w.stalled, w.cutOff} {
					for id := range set {
						out[id] = true
					}
				}
				if alive := n - len(w.killed); alive < n/2+1 {
					t.Fatalf("seed %d at %d nodes: after step %d (%s) only %d are alive\n%s", seed, n, i+1, st, alive, s)
				}
				if len(out) > (n-1)/2 {
					t.Fatalf("seed %d at %d nodes: after step %d (%s) %d nodes are out of service, a majority\n%s", seed, n, i+1, st, len(out), s)
				}
			})
		}
	}
	if kills < 1000 {
		t.Fatalf("2000 schedules held only %d kills: the mix is not drawing them", kills)
	}
}

// TestARestartIsDrawnOnlyForAKilledNode: a restart names a node that is killed, an unstall
// a node that is stalled, and a kill never names a node that is already dead.
func TestARestartIsDrawnOnlyForAKilledNode(t *testing.T) {
	restarts, unstalls := 0, 0
	for seed := int64(1); seed <= 1000; seed++ {
		s := harness.NewSchedule(seed, 3, 2, 14, harness.P1)
		killed, stalled := map[string]bool{}, map[string]bool{}
		for i, st := range s.Steps {
			id := ""
			if len(st.Nodes) > 0 {
				id = st.Nodes[0]
			}
			switch st.Kind {
			case harness.StepKill:
				if killed[id] {
					t.Fatalf("seed %d step %d kills %s twice\n%s", seed, i+1, id, s)
				}
				if stalled[id] {
					t.Fatalf("seed %d step %d kills %s while it is stalled: Node.Close would hang\n%s", seed, i+1, id, s)
				}
				killed[id] = true
			case harness.StepRestart:
				restarts++
				if !killed[id] {
					t.Fatalf("seed %d step %d restarts %s, which is not killed\n%s", seed, i+1, id, s)
				}
				delete(killed, id)
			case harness.StepStall:
				stalled[id] = true
			case harness.StepUnstall:
				unstalls++
				if !stalled[id] {
					t.Fatalf("seed %d step %d unstalls %s, which is not stalled\n%s", seed, i+1, id, s)
				}
				delete(stalled, id)
			}
		}
	}
	if restarts < 300 || unstalls < 100 {
		t.Fatalf("1000 schedules held %d restarts and %d unstalls: the mix is not drawing them", restarts, unstalls)
	}
}

// TestEveryPartitionIsHealedInTime: every partition and every one-way cut is followed by a
// heal no later than twice the profile's detection time, and cuts never overlap.
func TestEveryPartitionIsHealedInTime(t *testing.T) {
	limit := 2 * harness.P1.Detection()
	cuts := 0
	for seed := int64(1); seed <= 1000; seed++ {
		s := harness.NewSchedule(seed, 3, 2, 14, harness.P1)
		for i, st := range s.Steps {
			if st.Kind != harness.StepPartition && st.Kind != harness.StepBlockOneWay {
				continue
			}
			cuts++
			healed := slices.IndexFunc(s.Steps[i+1:], func(later harness.Step) bool { return later.Kind == harness.StepHeal })
			if healed < 0 {
				t.Fatalf("seed %d step %d (%s) is never healed\n%s", seed, i+1, st, s)
			}
			if after := s.Steps[i+1+healed].At - st.At; after > limit {
				t.Fatalf("seed %d step %d (%s) is healed after %s, more than 2 x detection (%s)\n%s", seed, i+1, st, after, limit, s)
			}
			for _, between := range s.Steps[i+1 : i+1+healed] {
				if between.Kind == harness.StepPartition || between.Kind == harness.StepBlockOneWay {
					t.Fatalf("seed %d step %d (%s) is cut again before its heal\n%s", seed, i+1, st, s)
				}
			}
		}
	}
	if cuts < 500 {
		t.Fatalf("1000 schedules held %d cuts: the mix is not drawing them", cuts)
	}
}

// seedsToRun is the seeds TestRandomSchedule walks: HOARDDB_HARNESS_SEED replays exactly one
// (and HOARDDB_HARNESS_SEEDS is then ignored); otherwise HOARDDB_HARNESS_SEEDS fresh seeds
// (default 1) are drawn from the clock, so the run is new each time and the test logs them.
func seedsToRun(t *testing.T) []int64 {
	t.Helper()
	if text := os.Getenv("HOARDDB_HARNESS_SEED"); text != "" {
		seed, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			t.Fatalf("HOARDDB_HARNESS_SEED=%q is not a number; set it to the seed in the failing log's 'fault schedule seed=' line", text)
		}
		return []int64{seed}
	}
	count := 1
	if text := os.Getenv("HOARDDB_HARNESS_SEEDS"); text != "" {
		parsed, err := strconv.Atoi(text)
		if err != nil || parsed < 1 {
			t.Fatalf("HOARDDB_HARNESS_SEEDS=%q is not a positive number; set it to how many fresh seeds to walk", text)
		}
		count = parsed
	}
	// The clock only names a fresh seed here; the schedule itself never reads it.
	drawn := rand.New(rand.NewSource(time.Now().UnixNano()))
	seeds := make([]int64, count)
	for i := range seeds {
		seeds[i] = drawn.Int63n(1_000_000_000)
	}
	return seeds
}

// scheduleStepsToRun is HOARDDB_HARNESS_SEED_STEPS (default 14); -short halves it, which
// halves the run's duration.
func scheduleStepsToRun(t *testing.T) int {
	t.Helper()
	steps := 14
	if text := os.Getenv("HOARDDB_HARNESS_SEED_STEPS"); text != "" {
		parsed, err := strconv.Atoi(text)
		if err != nil || parsed < 2 {
			t.Fatalf("HOARDDB_HARNESS_SEED_STEPS=%q is not a number of at least 2; set it to the number of steps per schedule", text)
		}
		steps = parsed
	}
	if testing.Short() {
		steps /= 2
	}
	return max(steps, 2)
}

// TestRandomSchedule runs seeded fault schedules against a fresh three-voter, RF 2 cluster at
// timing profile P1 with two clients, and fails on any verdict the ledger does not call OK.
// A violation is a finding: the log holds the schedule (paste it into the bug) and the
// computed loss set. Replay a seed with HOARDDB_HARNESS_SEED=<n>, or walk HOARDDB_HARNESS_SEEDS=<n>
// fresh seeds (default 1). TestAShortScheduleOfFixedSeedsLeavesTheLedgerClean runs seeds 1 and 2
// at eight steps without the env gate, so the default suite always judges a real fault schedule.
func TestRandomSchedule(t *testing.T) {
	if os.Getenv("HOARDDB_HARNESS_FAULT") != "1" {
		t.Skip("set HOARDDB_HARNESS_FAULT=1 to run the seeded fault schedules")
	}
	seeds := seedsToRun(t)
	steps := scheduleStepsToRun(t)
	t.Logf("seeds %v, %d steps each", seeds, steps)
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runSeededSchedule(t, seed, steps)
		})
	}
}

// TestAShortScheduleOfFixedSeedsLeavesTheLedgerClean: seeds 1 and 2 at eight steps run on every
// test pass, with no HOARDDB_HARNESS_FAULT gate, so a regression in the ledger's fault judgement
// fails the default suite rather than waiting for someone to set the variable.
func TestAShortScheduleOfFixedSeedsLeavesTheLedgerClean(t *testing.T) {
	for _, seed := range []int64{1, 2} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runSeededSchedule(t, seed, 8)
		})
	}
}

// runSeededSchedule builds one fault schedule for seed, runs it against a fresh cluster and
// fails on any verdict the ledger does not call OK. It is the body of each TestRandomSchedule
// subtest and of the fixed-seed test.
func runSeededSchedule(t *testing.T, seed int64, steps int) {
	began := time.Now()
	c := ledgerSetup(t, 2, harness.Options{Timings: harness.P1})
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket, ProbeID: "c1-0"})
	schedule := harness.NewSchedule(seed, 3, 2, steps, harness.P1)
	verdicts := schedule.Run(t, c, l)
	if running := c.Running(); len(running) != 3 {
		t.Errorf("seed %d: after settling, only %v are running: the runner must leave the cluster whole before the ledger judges it", seed, running)
	}
	if len(verdicts) != 6 {
		t.Fatalf("seed %d: the ledger returned %d verdicts, want 6", seed, len(verdicts))
	}
	for _, v := range verdicts {
		if !v.OK {
			t.Errorf("seed %d: %s was violated: %s (after %+v)\nreplay: HOARDDB_HARNESS_FAULT=1 HOARDDB_HARNESS_SEED=%d go test ./cluster/harness -race -count=1 -p 1 -run TestRandomSchedule", seed, v.Name, v.Detail, v.After, seed)
		}
	}
	t.Logf("seed %d took %s", seed, time.Since(began).Round(time.Second))
}

// TestASeedReplaysFromTheLog: the log a run prints holds everything needed to rebuild its
// schedule. Seed 7 is built, its log text captured the way Run prints it, the schedule
// rebuilt from that text alone and from HOARDDB_HARNESS_SEED=7, and all three are equal.
func TestASeedReplaysFromTheLog(t *testing.T) {
	original := harness.NewSchedule(7, 3, 2, 14, harness.P1)
	logged := original.Header() + "\n" + original.String()
	fromLog, err := harness.ScheduleFromLog("=== RUN   TestRandomSchedule/seed-7\n    schedule_test.go:1: " + logged)
	if err != nil {
		t.Fatalf("rebuilding the schedule from its log: %v", err)
	}
	if got := fromLog.Header() + "\n" + fromLog.String(); got != logged {
		t.Fatalf("the schedule rebuilt from the log differs:\n%s\nwant\n%s", got, logged)
	}

	t.Setenv("HOARDDB_HARNESS_SEED", "7")
	t.Setenv("HOARDDB_HARNESS_SEEDS", "5")
	seeds := seedsToRun(t)
	if !slices.Equal(seeds, []int64{7}) {
		t.Fatalf("HOARDDB_HARNESS_SEED=7 with SEEDS=5 chose seeds %v, want [7] alone", seeds)
	}
	replayed := harness.NewSchedule(seeds[0], 3, 2, 14, harness.P1)
	if replayed.Header()+"\n"+replayed.String() != logged {
		t.Fatalf("the replay of seed 7 differs from the original run's schedule")
	}

	if _, err := harness.ScheduleFromLog("nothing useful"); err == nil {
		t.Fatalf("a log without a schedule header was accepted")
	}
}
