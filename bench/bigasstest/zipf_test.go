package main

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestZipfRankFrequency draws from a fixed seed and compares the rank counts with the Zipf law
// 1/(rank+1)^s: a chi-square over the first 20 ranks plus the tail, and the log-log slope.
func TestZipfRankFrequency(t *testing.T) {
	for _, s := range []float64{1.0, 1.4} {
		const n, draws = 1000, 2_000_000
		table, err := newZipfTable(n, s)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewPCG(42, 7))
		counts := make([]float64, n)
		for range draws {
			counts[table.draw(rng.Float64())]++
		}
		var norm float64
		for k := 1; k <= n; k++ {
			norm += math.Pow(float64(k), -s)
		}
		const bins = 20
		var chi2, tailExpected, tailObserved float64
		for k := range n {
			expected := draws * math.Pow(float64(k+1), -s) / norm
			if k < bins {
				chi2 += (counts[k] - expected) * (counts[k] - expected) / expected
				continue
			}
			tailExpected += expected
			tailObserved += counts[k]
		}
		chi2 += (tailObserved - tailExpected) * (tailObserved - tailExpected) / tailExpected
		// 21 cells, 20 degrees of freedom: the 0.1% critical value is 45.3. The seed is fixed, so
		// this is a check of the table and not a flaky test.
		if chi2 > 45.3 {
			t.Errorf("s=%v: chi-square %.1f over %d cells exceeds the 0.1%% critical value 45.3", s, chi2, bins+1)
		}
		// Least-squares slope of log count against log rank over ranks 1..50.
		var sx, sy, sxx, sxy float64
		const fit = 50
		for k := 1; k <= fit; k++ {
			x, y := math.Log(float64(k)), math.Log(counts[k-1])
			sx, sy, sxx, sxy = sx+x, sy+y, sxx+x*x, sxy+x*y
		}
		slope := (fit*sxy - sx*sy) / (fit*sxx - sx*sx)
		if math.Abs(slope+s) > 0.05 {
			t.Errorf("s=%v: log-log slope %.3f, want %.3f", s, slope, -s)
		}
	}
}

func TestZipfTableRefusesBadInput(t *testing.T) {
	if _, err := newZipfTable(0, 1); err == nil {
		t.Error("a table over no entities must be refused")
	}
	for _, s := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if _, err := newZipfTable(10, s); err == nil || !strings.Contains(err.Error(), "-zipf-s") {
			t.Errorf("exponent %v must be refused and name the flag, got %v", s, err)
		}
	}
	// A table is a probability distribution: the last cumulative value is exactly 1, so a draw
	// of the largest float below 1 can never fall off the end.
	table, _ := newZipfTable(7, 1)
	if got := table.draw(math.Nextafter(1, 0)); got != 6 {
		t.Errorf("the largest draw maps to rank %d, want 6", got)
	}
	if got := table.draw(0); got != 0 {
		t.Errorf("the smallest draw maps to rank %d, want 0", got)
	}
}

// TestZipfRankOrderIsSeedingOrder pins how a rank maps to an entity: rank k is the k-th
// document of the bucket in seeding order (the model's own rank index), on every engine.
func TestZipfRankOrderIsSeedingOrder(t *testing.T) {
	w := testWorld(t)
	if err := w.configureAccess(accessConfig{Mode: accessZipf, ZipfS: 1.0}); err != nil {
		t.Fatal(err)
	}
	for _, layout := range benchLayouts {
		for bucket, ids := range w.sets[layout].idList {
			table := w.zipf[layout][bucket]
			if table == nil || len(table.cum) != len(ids) {
				t.Fatalf("%s/%s: no table over its %d entities", layout, bucket, len(ids))
			}
			if got := w.entityAtRank(layout, bucket, 0); got != ids[0] {
				t.Errorf("%s/%s: rank 0 is %s, want %s", layout, bucket, got, ids[0])
			}
		}
	}
}

// TestZipfSkewsEveryReadAndWritePick draws a planner's requests and checks that the hottest
// entity of a bucket is picked far more often than uniform, by reads and by writes alike, and
// that the uniform mode is unchanged.
func TestZipfSkewsEveryReadAndWritePick(t *testing.T) {
	hot := func(mode string) (readHot, writeHot, reads, writes float64) {
		w := testWorld(t)
		if err := w.configureAccess(accessConfig{Mode: mode, ZipfS: 1.0}); err != nil {
			t.Fatal(err)
		}
		mix, _ := newMix(benchLayouts, "")
		p := newPlanner(w, mix, 1, 0, 5)
		firstRoom := w.sets[model.Chat].ids("Room")[0]
		// Under Zipf the rank order of a read is over its eligible owners.
		firstUser := w.pages[ReadSocialInbox].Eligible[0]
		for range 300000 {
			req := p.next()
			switch {
			case req.Read != nil && req.Read.Kind == ReadSocialInbox:
				reads++
				if req.Read.Entity == firstUser {
					readHot++
				}
			case req.Write != nil && req.Write.Kind == WriteChatMessage:
				writes++
				if req.Write.Elements[0].Owner == firstRoom {
					writeHot++
				}
			}
		}
		return
	}
	for _, mode := range []string{accessUniform, accessZipf} {
		readHot, writeHot, reads, writes := hot(mode)
		users := float64(len(testWorld(t).pages[ReadSocialInbox].Eligible))
		rooms := float64(len(testWorld(t).sets[model.Chat].ids("Room")))
		readShare, writeShare := readHot/reads, writeHot/writes
		if mode == accessUniform {
			if readShare > 5/users || writeShare > 5/rooms {
				t.Errorf("uniform mode skewed: reads %.4f writes %.4f", readShare, writeShare)
			}
			continue
		}
		if readShare < 4/users || writeShare < 4/rooms {
			t.Errorf("zipf mode is not skewed: hottest user %.4f of %v reads (uniform %.4f), hottest room %.4f of %v writes (uniform %.4f)", readShare, reads, 1/users, writeShare, writes, 1/rooms)
		}
	}
}

func TestZipfIsSeededAndIdenticalOnEveryEngine(t *testing.T) {
	build := func() *planner {
		w := testWorld(t)
		if err := w.configureAccess(accessConfig{Mode: accessZipf, ZipfS: 1.2}); err != nil {
			t.Fatal(err)
		}
		mix, _ := newMix(benchLayouts, "")
		return newPlanner(w, mix, 1, 2, 11)
	}
	a, b := build(), build()
	for i := range 5000 {
		ra, rb := a.next(), b.next()
		if ra.Kind != rb.Kind || (ra.Read != nil && ra.Read.Entity != rb.Read.Entity) {
			t.Fatalf("request %d differs between two planners of the same seed", i)
		}
	}
}
