package server

import (
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The refusal texts and the two metrics (on-demand pull build doc step 12;
// spec §9.3, §10): a write of a never-created bucket is NOT_FOUND since
// BUG-0686, the on-demand read's two aggregate series are registered and move
// once per read, and a read meeting a create in flight is told which member it
// waits on (deviation (f) of steps 1 to 6).

// TestAWriteToANeverCreatedBucketIsNotFound pins what BUG-0686 made of the two
// write-side texts §9.3 named (RC-1): a put of a bucket nobody created, at its
// owner and through another member, is the standard NOT_FOUND with
// write_outcome not_applied. The owner asks the members before it answers, so
// the "never created ... or this node joined after it existed" retry is left
// to a bucket some member holds. Guard: put the fence's old answer back.
func TestAWriteToANeverCreatedBucketIsNotFound(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	placed := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "nevermade", func(name string) bool { return ownerOn(placed, name) != "" })
	owner := nodeIndexByAddr(t, tc.addrs, ownerOn(placed, bucket))
	coordinator := (owner + 1) % 3
	if replicas, err := clusterOf(tc, 0).Replicas(placementOf(testDatabase, bucket)); err == nil {
		for index := range tc.addrs {
			if !nodeListed(replicas, tc.addrs[index]) {
				coordinator = index
			}
		}
	}
	put := InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}}
	for _, at := range []int{owner, coordinator} {
		requireNotFoundNotApplied(t, tc.frame(at, transport.OpPut, put), bucket)
	}
}

// histogramSamples is how many observations h holds.
func histogramSamples(t *testing.T, c *Cluster) uint64 {
	t.Helper()
	var metric dto.Metric
	if err := c.onDemand.waits.Write(&metric); err != nil {
		t.Fatalf("reading the wait histogram: %v", err)
	}
	return metric.GetHistogram().GetSampleCount()
}

// onDemandCounts is the on-demand read counter of c by outcome.
func onDemandCounts(c *Cluster) map[onDemandOutcome]float64 {
	counts := make(map[onDemandOutcome]float64)
	for _, outcome := range []onDemandOutcome{onDemandLanded, onDemandForwarded, onDemandRetry, onDemandNotFound} {
		var metric dto.Metric
		if err := c.onDemand.reads.WithLabelValues(string(outcome)).Write(&metric); err == nil {
			counts[outcome] = metric.GetCounter().GetValue()
		}
	}
	return counts
}

// movedOnce fails the test unless, between before and after, the counter
// moved by exactly one, for outcome alone.
func movedOnce(t *testing.T, what string, before, after map[onDemandOutcome]float64, outcome onDemandOutcome) {
	t.Helper()
	for each, count := range after {
		want := before[each]
		if each == outcome {
			want++
		}
		if count != want {
			t.Errorf("%s moved hoarddb_ondemand_reads_total{outcome=%q} from %v to %v, want %v", what, each, before[each], count, want)
		}
	}
}

// TestOneReadOfEachOutcomeMovesTheCounterOnce is §10: one read of each outcome
// moves hoarddb_ondemand_reads_total{outcome} by one, a read that waited is
// one observation of hoarddb_ondemand_wait_seconds, and a read of a bucket the
// node serves moves nothing. Guard: record an outcome twice, or drop one.
func TestOneReadOfEachOutcomeMovesTheCounterOnce(t *testing.T) {
	t.Run("landed, then a served read moves nothing", func(t *testing.T) {
		s := setUpJoinerBeforeAnyPull(t)
		before, waits := onDemandCounts(s.joiner), histogramSamples(t, s.joiner)
		get := FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}
		foundHeld(t, "a get at the joiner", s.tc.frameOn(s.joinerNode, transport.OpGet, get))
		movedOnce(t, "a get landed by the take-over", before, onDemandCounts(s.joiner), onDemandLanded)
		if n := histogramSamples(t, s.joiner) - waits; n != 1 {
			t.Errorf("a get that waited for its copy made %d wait observations, want 1", n)
		}
		before, waits = onDemandCounts(s.joiner), histogramSamples(t, s.joiner)
		foundHeld(t, "a second get at the joiner", s.tc.frameOn(s.joinerNode, transport.OpGet, get))
		movedOnce(t, "a get of a bucket the joiner serves", before, onDemandCounts(s.joiner), "")
		if n := histogramSamples(t, s.joiner) - waits; n != 0 {
			t.Errorf("a get of a served bucket made %d wait observations, want 0", n)
		}
	})
	t.Run("forwarded", func(t *testing.T) {
		s := setUpLeaveCase(t, "countfwd", BucketTypeHash, putHeld)
		c := clusterOf(s.tc, s.newOwner)
		before := onDemandCounts(c)
		foundHeld(t, "a get at the new owner past the bound", s.tc.frame(s.newOwner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}))
		movedOnce(t, "a get forwarded past the bound", before, onDemandCounts(c), onDemandForwarded)
	})
	t.Run("retry", func(t *testing.T) {
		s := setUpJoinerBeforeAnyPull(t)
		s.joiner.placementMu.Lock()
		finished := s.joiner.passFinished
		s.joiner.passFinished = s.joiner.Epoch() - 1
		s.joiner.placementMu.Unlock()
		t.Cleanup(func() {
			s.joiner.placementMu.Lock()
			s.joiner.passFinished = finished
			s.joiner.placementMu.Unlock()
		})
		before := onDemandCounts(s.joiner)
		refusedAwaitingCopy(t, "a get before the placement pass", s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}))
		movedOnce(t, "a get answered retry", before, onDemandCounts(s.joiner), onDemandRetry)
	})
	t.Run("not found", func(t *testing.T) {
		s := setUpJoinerBeforeAnyPull(t)
		bucket := missingOwnedBy(t, s.joiner, "countnone")
		before := onDemandCounts(s.joiner)
		refusedNotFound(t, "a get of a name no member holds", s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
		movedOnce(t, "a get answered not found", before, onDemandCounts(s.joiner), onDemandNotFound)
	})
}

// TestOnDemandMetricsHaveNoBucketLabel is §10 and rule 11: the two series are
// registered on the node's registry, the counter with the outcome label alone
// and the histogram with none; a scrape carries no bucket or database label.
// Guard: add a bucket label.
func TestOnDemandMetricsHaveNoBucketLabel(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	t.Cleanup(tc.stop)
	c := clusterOf(tc, 0)
	families, err := c.metrics.Gather()
	if err != nil {
		t.Fatalf("gathering the node's metrics: %v", err)
	}
	found := map[string]bool{}
	for _, family := range families {
		name := family.GetName()
		if name != "hoarddb_ondemand_reads_total" && name != "hoarddb_ondemand_wait_seconds" {
			continue
		}
		found[name] = true
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != "outcome" {
					t.Errorf("%s carries the label %q; the on-demand series are labelled by outcome only", name, label.GetName())
				}
			}
		}
	}
	for _, name := range []string{"hoarddb_ondemand_reads_total", "hoarddb_ondemand_wait_seconds"} {
		if !found[name] {
			t.Errorf("%s is not registered on the node's metrics", name)
		}
	}
}

// TestAReadOfANameWithACreateInFlightNamesTheMember is deviation (f), spec §6.8
// "A create in flight at a member: UNAVAILABLE retry, naming the member": a
// founder has a create of a moved name in flight; a read of it at the joiner
// is told to retry and names the founder, and the take-over keeps the name
// until the create lands, recording nothing. Guard: the status that names
// the members with a create in flight.
func TestAReadOfANameWithACreateInFlightNamesTheMember(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	bucket := missingOwnedBy(t, s.joiner, "inflightnamed")
	founder := clusterOf(s.tc, 0)
	plantOrphan(t, founder, bucket)
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"})
	refusedAwaitingCopy(t, "a read of a name whose create is in flight at a founder", frame)
	if text := errorFrameText(frame); !strings.Contains(text, founder.self.ID) || !strings.Contains(text, "create") {
		t.Errorf("a read of a name whose create is in flight at %s was answered %q; want it to name that member and the create", founder.self.ID, text)
	}
	if hasOutcome(s.joiner, bucket) {
		t.Errorf("the take-over recorded a finding for a name whose create is in flight")
	}
}
