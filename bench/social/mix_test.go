package main

import (
	"math"
	"slices"
	"testing"
)

// specMixTables are the percentages of spec 6.5, typed in here a second time
// on purpose: the test compares the scheduler's table with the document, not
// with itself.
var specMixTables = map[string]map[string]float64{
	"rh": {"R1": 40, "R2": 10, "R3": 15, "R4": 15, "R5": 5, "R6": 3, "R7": 2, "N1": 2, "W1": 2, "W2": 4, "W3": 1, "W4": 0, "W5": 1, "W6": 0},
	"wh": {"R1": 25, "R2": 5, "R3": 8, "R4": 8, "R5": 2, "R6": 1, "R7": 1, "N1": 5, "W1": 15, "W2": 15, "W3": 4, "W4": 1, "W5": 8, "W6": 2},
	"sk": {"R1": 35, "R2": 8, "R3": 12, "R4": 12, "R5": 3, "R6": 2, "R7": 1, "N1": 9, "W1": 1, "W2": 12, "W3": 2, "W4": 2, "W5": 1, "W6": 0},
}

func TestMixRatiosWithinTenthOfAPoint(t *testing.T) {
	const draws = 1_000_000
	for mixName, wantPercent := range specMixTables {
		t.Run(mixName, func(t *testing.T) {
			scheduler, err := newMixScheduler(mixName, 1)
			if err != nil {
				t.Fatalf("newMixScheduler(%q): %v", mixName, err)
			}
			drawn := map[string]int{}
			for range draws {
				drawn[scheduler.next()]++
			}
			for operation, want := range wantPercent {
				got := float64(drawn[operation]) / draws * 100
				if math.Abs(got-want) > 0.1 {
					t.Errorf("%s %s: %.3f%% of draws, want %v%% within 0.1 points", mixName, operation, got, want)
				}
			}
			for operation := range drawn {
				if _, listed := wantPercent[operation]; !listed {
					t.Errorf("%s drew %s, which is not in the spec table", mixName, operation)
				}
			}
		})
	}
}

func TestMixTablesSumToOneHundred(t *testing.T) {
	// Spec 6.5 prints SK as 70 / 30, but its own column holds 73 reads and 27
	// writes (R1..R7 = 35+8+12+12+3+2+1); the table, not the caption, is normative.
	wantReadShare := map[string]float64{"rh": 90, "wh": 50, "sk": 73}
	for mixName := range specMixTables {
		table, err := mixTable(mixName)
		if err != nil {
			t.Fatalf("mixTable(%q): %v", mixName, err)
		}
		var total, reads int
		for _, share := range table {
			total += share.percent
			if share.operation[0] == 'R' {
				reads += share.percent
			}
		}
		if total != 100 {
			t.Errorf("%s shares sum to %d, want 100", mixName, total)
		}
		if float64(reads) != wantReadShare[mixName] {
			t.Errorf("%s reads are %d%%, want %v%%", mixName, reads, wantReadShare[mixName])
		}
	}
}

func TestOpModeRunsOneOperation(t *testing.T) {
	scheduler, err := newMixScheduler("op:R3", 1)
	if err != nil {
		t.Fatalf("newMixScheduler(op:R3): %v", err)
	}
	for draw := range 1000 {
		if got := scheduler.next(); got != "R3" {
			t.Fatalf("draw %d of -mix op:R3 was %s, want only R3", draw, got)
		}
	}
	for _, bad := range []string{"op:R9", "op:", "rhx", "", "op:r3"} {
		if _, err := newMixScheduler(bad, 1); err == nil {
			t.Errorf("newMixScheduler(%q) accepted a mix that does not exist", bad)
		}
	}
}

func TestSchedulerIsDeterministic(t *testing.T) {
	drawSequence := func(seed uint64) []string {
		scheduler, err := newMixScheduler("wh", seed)
		if err != nil {
			t.Fatalf("newMixScheduler: %v", err)
		}
		sequence := make([]string, 5000)
		for i := range sequence {
			sequence[i] = scheduler.next()
		}
		return sequence
	}
	if !slices.Equal(drawSequence(42), drawSequence(42)) {
		t.Error("two schedulers with the same seed drew different sequences")
	}
	if slices.Equal(drawSequence(1), drawSequence(2)) {
		t.Error("seeds 1 and 2 drew the same sequence: workers would move in lockstep")
	}
}
