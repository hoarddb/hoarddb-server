package harness_test

// TestRFBench is a measurement, not a proof, and runs only with HOARDDB_RFBENCH=1: RF 1
// against RF 2 insert latency on the entry-ring write path in the harness (real Badger,
// log, Raft and peer transport; loopback network; every node on TMPDIR's filesystem, so
// run it twice, once on the disk and once on tmpfs, to separate the copy's fsync from
// the protocol). It prints one JSON line per leg prefixed RFBENCH. It found BUG-0745.
// Knobs: RFBENCH_N inserts per leg (300), RFBENCH_CONC concurrent writers (16),
// RFBENCH_REPS repetitions (7, rfDefaultReps), RFBENCH_SAMEPAIR=1 puts every id of a leg in
// one ring slot, so one pair takes every write (the hot-entity case of
// the entry ring pair lock across fsync spec section 9). Unset or 0 spreads the ids over
// the owner's slots as before. Legs with it set carry "same_pair":true in their RFBENCH line.
// With HOARDDB_RFGATE=1 it also fails when RF 2 concurrent throughput is under the floor below, or when any RF 2 write was confirmed by fewer than 2 copies. Never run the gate with -race.

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

type rfLeg struct {
	Host         string  `json:"host"`
	TmpDir       string  `json:"tmpdir"`
	Rep          int     `json:"rep"`
	DB           string  `json:"db"`
	RF           int     `json:"rf"`
	Mode         string  `json:"mode"`
	Conc         int     `json:"conc"`
	N            int     `json:"n"`
	MeanMS       float64 `json:"mean_ms"`
	P50MS        float64 `json:"p50_ms"`
	P90MS        float64 `json:"p90_ms"`
	P99MS        float64 `json:"p99_ms"`
	OpsPerSec    float64 `json:"ops_s"`
	Degraded     int     `json:"degraded"`
	FsyncProbeMS float64 `json:"fsync_probe_ms"`
	SamePair     bool    `json:"same_pair,omitempty"`
}

// The done-suite limits (the replication done suite spec). The owner ruled on 2026-11-04 that the gate is 0.4x, a
// number measured over five runs of the 7-repetition median (0.43x to 0.60x on the test host), and that 0.5x stays as the
// goal: the median straddled 0.5x from run to run, so a 0.5x gate would fail about two runs in five for no reason.
const rfConcurrentThroughputFloor = 0.4

// rfDefaultReps is 7, not 3: RF 1 concurrent throughput swings between runs (18,000 to 47,000
// writes per second), and at 3 repetitions the median RF2/RF1 ratio straddled the floor
// (0.41x, 0.66x, 0.51x). The median of an odd count of 7 rides out those swings.
const rfDefaultReps = 7

type rfDB struct {
	name string
	rf   int
}

func rfEnvInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// rfFsyncProbe appends 4 KiB and fsyncs it, count times, in TMPDIR; returns the mean ms.
func rfFsyncProbe(t *testing.T, count int) float64 {
	t.Helper()
	f, err := os.CreateTemp(os.TempDir(), "rfbench-fsync-*")
	if err != nil {
		t.Fatalf("fsync probe file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("fsync probe close: %v", err)
		}
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("fsync probe remove: %v", err)
		}
	}()
	block := make([]byte, 4096)
	start := time.Now()
	for i := 0; i < count; i++ {
		if _, err := f.Write(block); err != nil {
			t.Fatalf("fsync probe write: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("fsync probe sync: %v", err)
		}
	}
	return float64(time.Since(start).Microseconds()) / 1000 / float64(count)
}

func rfPercentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return float64(sorted[i].Microseconds()) / 1000
}

// rfRepRatios formats the RF 2 over RF 1 concurrent throughput of each repetition, so a run
// whose median straddles the floor shows its spread in the log. It judges nothing.
func rfRepRatios(legs []rfLeg, reps int) string {
	ops := make(map[[2]int]float64)
	for _, leg := range legs {
		if leg.Mode == "conc" {
			ops[[2]int{leg.Rep, leg.RF}] = leg.OpsPerSec
		}
	}
	parts := make([]string, 0, reps)
	for rep := 0; rep < reps; rep++ {
		parts = append(parts, fmt.Sprintf("rep%d %.2fx", rep, ops[[2]int{rep, 2}]/ops[[2]int{rep, 1}]))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// rfIDPlan asks for Count document ids in DB, tagged Tag, that the owner node keeps.
type rfIDPlan struct {
	DB, Tag  string
	Count    int
	SamePair bool
	// SlotOf is wire.PlacementOf for the bucket; Owned says whether the owner node holds the id's slot.
	SlotOf func(id string) (uint16, error)
	Owned  func(id string) (bool, error)
}

// rfPlaceIDs returns the ids of one leg, taking candidate ids in order. Without SamePair it keeps
// every candidate the owner holds, so the leg spreads over the owner's slots. With SamePair the
// first candidate the owner holds pins a slot, and the leg keeps only candidates in that slot, so
// every write lands on one pair. The owner is asked only about candidates in the pinned slot: the
// slot is hashed locally, the owner lookup goes to the cluster.
func rfPlaceIDs(plan rfIDPlan) ([]string, error) {
	ids := make([]string, 0, plan.Count)
	budget := plan.Count * 20
	if plan.SamePair {
		// One candidate in SlotCount lands in the pinned slot, so the search needs about SlotCount per id.
		budget = plan.Count * storage.SlotCount * 20
	}
	var pinned uint16
	havePin := false
	for i := 0; len(ids) < plan.Count; i++ {
		if i > budget {
			return nil, fmt.Errorf("could not place %d ids on the owner after %d candidates", plan.Count, i)
		}
		id := fmt.Sprintf("%s-%s-%d", plan.DB, plan.Tag, i)
		if plan.SamePair && havePin {
			slot, err := plan.SlotOf(id)
			if err != nil {
				return nil, fmt.Errorf("placing %s: %w", id, err)
			}
			if slot != pinned {
				continue
			}
		}
		ok, err := plan.Owned(id)
		if err != nil {
			return nil, fmt.Errorf("placing %s: %w", id, err)
		}
		if !ok {
			continue
		}
		if plan.SamePair && !havePin {
			slot, err := plan.SlotOf(id)
			if err != nil {
				return nil, fmt.Errorf("placing %s: %w", id, err)
			}
			pinned, havePin = slot, true
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func TestRFBench(t *testing.T) {
	if os.Getenv("HOARDDB_RFBENCH") == "" {
		t.Skip("a measurement, not a proof; set HOARDDB_RFBENCH=1 to run it")
	}
	n := rfEnvInt("RFBENCH_N", 300)
	conc := rfEnvInt("RFBENCH_CONC", 16)
	reps := rfEnvInt("RFBENCH_REPS", rfDefaultReps)
	samePair := rfEnvInt("RFBENCH_SAMEPAIR", 0) == 1
	host, _ := os.Hostname()

	c := harness.NewCluster(t, 3, harness.Options{WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbs := []rfDB{{"rf1", 1}, {"rf2", 2}}
	for _, db := range dbs {
		if _, err := leader.CreateDatabaseRF(proofCtx(t), db.name, db.rf); err != nil {
			t.Fatalf("creating %s: %v", db.name, err)
		}
	}
	c.AwaitConverged(10 * time.Second)
	for _, db := range dbs {
		if _, err := leader.CreateBucket(proofCtx(t), db.name, proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
			t.Fatalf("creating the bucket in %s: %v", db.name, err)
		}
	}
	ownerID := c.IDs()[0]
	owner := c.Node(ownerID)

	// The ids are owned by ownerID, so every insert runs on the owner with no forward hop.
	run := func(rep int, db rfDB, mode string, workers int, probe float64) rfLeg {
		ids, err := rfPlaceIDs(rfIDPlan{
			DB: db.name, Tag: fmt.Sprintf("%s-%d", mode, rep), Count: n, SamePair: samePair,
			SlotOf: func(id string) (uint16, error) { return wire.PlacementOf(db.name, proofBucket, id) },
			Owned: func(id string) (bool, error) {
				o, _, err := owner.Owner(proofCtx(t), db.name, proofBucket, id)
				return o == ownerID, err
			},
		})
		if err != nil {
			t.Fatalf("placing the ids of %s: %v", db.name, err)
		}
		lat := make([]time.Duration, n)
		var degraded int
		var mu sync.Mutex
		var wg sync.WaitGroup
		per := n / workers
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := w * per; i < (w+1)*per; i++ {
					t0 := time.Now()
					got, err := owner.Insert(proofCtx(t), db.name, proofBucket, bson.D{{Key: "_id_", Value: ids[i]}, {Key: "name", Value: "Ada"}})
					lat[i] = time.Since(t0)
					if err != nil {
						t.Errorf("insert %s: %v (background %v)", ids[i], err, c.BackgroundErrors())
						return
					}
					if got.Outcome != machine.OutcomeApplied || int(got.CopiesConfirmed) != db.rf {
						mu.Lock()
						degraded++
						mu.Unlock()
					}
				}
			}(w)
		}
		wg.Wait()
		elapsed := time.Since(start)
		used := lat[:per*workers]
		sorted := append([]time.Duration(nil), used...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		var sum time.Duration
		for _, d := range used {
			sum += d
		}
		leg := rfLeg{
			Host: host, TmpDir: filepath.Clean(os.TempDir()), Rep: rep, DB: db.name, RF: db.rf, Mode: mode, Conc: workers, N: len(used),
			MeanMS:    float64(sum.Microseconds()) / 1000 / float64(len(used)),
			P50MS:     rfPercentile(sorted, 0.50),
			P90MS:     rfPercentile(sorted, 0.90),
			P99MS:     rfPercentile(sorted, 0.99),
			OpsPerSec: float64(len(used)) / elapsed.Seconds(),
			Degraded:  degraded, FsyncProbeMS: probe, SamePair: samePair,
		}
		line, _ := json.Marshal(leg)
		fmt.Println("RFBENCH " + string(line))
		return leg
	}

	var legs []rfLeg
	for rep := 0; rep < reps; rep++ {
		probe := rfFsyncProbe(t, 100)
		order := dbs
		if rep%2 == 1 {
			order = []rfDB{dbs[1], dbs[0]}
		}
		for _, db := range order {
			legs = append(legs, run(rep, db, "serial", 1, probe))
		}
		for _, db := range order {
			legs = append(legs, run(rep, db, "conc", conc, probe))
		}
	}
	if os.Getenv("HOARDDB_RFGATE") == "" {
		return
	}
	median := func(mode string, rf int, field func(rfLeg) float64) float64 {
		var values []float64
		for _, leg := range legs {
			if leg.Mode == mode && leg.RF == rf {
				values = append(values, field(leg))
			}
		}
		sort.Float64s(values)
		if len(values) == 0 {
			t.Fatalf("no %s legs ran at RF %d", mode, rf)
		}
		return values[len(values)/2]
	}
	p50 := func(leg rfLeg) float64 { return leg.P50MS }
	ops := func(leg rfLeg) float64 { return leg.OpsPerSec }
	serialRatio := median("serial", 2, p50) / median("serial", 1, p50)
	concRatio := median("conc", 2, ops) / median("conc", 1, ops)
	perRep := rfRepRatios(legs, reps)
	t.Logf("RF2 against RF1: serial median latency %.2fx (logged, not judged), concurrent throughput %.2fx (floor %.2fx), concurrent per repetition %s",
		serialRatio, concRatio, rfConcurrentThroughputFloor, perRep)
	if math.IsNaN(concRatio) || math.IsInf(concRatio, 0) {
		t.Fatalf("the concurrent throughput ratio is %v: a leg ran no writes (RFBENCH_CONC larger than RFBENCH_N?), so nothing was measured", concRatio)
	}
	// Throughput that does not wait for the second copy is not a pass: an RF2 write that came
	// back confirmed by fewer than 2 copies would read as RF1 speed.
	for _, leg := range legs {
		if leg.RF == 2 && leg.Degraded > 0 {
			t.Errorf("RF2 %s leg %d had %d writes confirmed by fewer than 2 copies: the throughput ratio does not count", leg.Mode, leg.Rep, leg.Degraded)
		}
	}
	if concRatio < rfConcurrentThroughputFloor {
		t.Errorf("RF2 concurrent throughput is %.2fx RF1, under the floor of %.2fx (concurrent per repetition %s): BUG-0745, the replication stream waits for one copy at a time", concRatio, rfConcurrentThroughputFloor, perRep)
	}
}

// TestRFBenchSamePairIDs runs without HOARDDB_RFBENCH. The same-pair knob must put every id of a
// leg in one slot; the default must spread them, and keep the ids it always generated.
func TestRFBenchSamePairIDs(t *testing.T) {
	const n = 40
	ownsEverything := func(string) (bool, error) { return true, nil }
	slotOf := func(id string) (uint16, error) { return wire.PlacementOf("rf1", proofBucket, id) }

	t.Run("same pair", func(t *testing.T) {
		ids, err := rfPlaceIDs(rfIDPlan{DB: "rf1", Tag: "pair", Count: n, SamePair: true, SlotOf: slotOf, Owned: ownsEverything})
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != n {
			t.Fatalf("got %d ids, want %d", len(ids), n)
		}
		distinct := make(map[string]bool)
		slots := make(map[uint16]bool)
		for _, id := range ids {
			distinct[id] = true
			slot, err := slotOf(id)
			if err != nil {
				t.Fatal(err)
			}
			slots[slot] = true
		}
		if len(distinct) != n {
			t.Errorf("same-pair ids are not distinct: %d unique of %d", len(distinct), n)
		}
		if len(slots) != 1 {
			t.Errorf("same-pair ids land in %d slots, want 1", len(slots))
		}
	})

	t.Run("default", func(t *testing.T) {
		ids, err := rfPlaceIDs(rfIDPlan{DB: "rf1", Tag: "pair", Count: n, SlotOf: slotOf, Owned: ownsEverything})
		if err != nil {
			t.Fatal(err)
		}
		slots := make(map[uint16]bool)
		for i, id := range ids {
			if want := fmt.Sprintf("rf1-pair-%d", i); id != want {
				t.Fatalf("default id %d is %q, want %q", i, id, want)
			}
			slot, err := slotOf(id)
			if err != nil {
				t.Fatal(err)
			}
			slots[slot] = true
		}
		if len(slots) < 2 {
			t.Errorf("default ids land in %d slot, want more than one", len(slots))
		}
	})
}
