package server

import (
	"errors"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func popRefusalsCounted() float64 {
	var metric dto.Metric
	if err := queryResultTooLarge.WithLabelValues(queryMethodPop).Write(&metric); err != nil {
		return -1
	}
	return metric.GetCounter().GetValue()
}

// TestPopLimitReaderRefusesWithTheFix: the reader zero-defaults to one, passes
// 1..maxPopLimit through, and refuses anything else with a message that names
// the number sent, the cap and a verb that works. The peek number it offers is
// never itself above peek's cap.
func TestPopLimitReaderRefusesWithTheFix(t *testing.T) {
	for _, accepted := range []struct{ sent, want int }{{0, 1}, {1, 1}, {7, 7}, {100, 100}} {
		got, err := popLimit(accepted.sent)
		if err != nil || got != accepted.want {
			t.Errorf("popLimit(%d) = %d, %v; want %d", accepted.sent, got, err, accepted.want)
		}
	}
	refusals := []struct {
		sent int
		want string
	}{
		{101, "invalid pop limit: 101 is above the pop maximum of 100. A pop removes what it returns, so it takes at most 100 at a time: repeat pop({limit: 100}), or read them without removing any with peek({limit: 101})"},
		{5000, "invalid pop limit: 5000 is above the pop maximum of 100. A pop removes what it returns, so it takes at most 100 at a time: repeat pop({limit: 100}), or read them without removing any with peek({limit: 1000})"},
		{-1, "invalid pop limit: -1 is not valid; pop({limit: N}) takes a whole number of 1 or more"},
	}
	for _, refused := range refusals {
		_, err := popLimit(refused.sent)
		if !errors.Is(err, ErrPopLimit) || errors.Is(err, ErrPeekLimit) {
			t.Fatalf("popLimit(%d) error %v; want ErrPopLimit and not ErrPeekLimit", refused.sent, err)
		}
		if err.Error() != refused.want {
			t.Errorf("popLimit(%d) message\n got %q\nwant %q", refused.sent, err.Error(), refused.want)
		}
	}
}

// TestResultShapePopMessageNamesNothingRemoved: a pop's refusal says that
// nothing was removed, which a peek's must not, and what fits.
func TestResultShapePopMessageNamesNothingRemoved(t *testing.T) {
	refusal := resultTooLargeError{
		query: "pop({limit: 100}) on app.Jobs", shape: resultShapePop, limit: 100,
		ceiling: 16 << 20, retained: 16<<20 - 100<<10, documents: 41,
	}
	single := "pop({limit: 100}) on app.Jobs would return more than 16 MiB (stopped at 15.9 MiB after 41 entries), " +
		"so nothing was removed. Ask for fewer entries — pop({limit: 41}) fits — or raise HOARDDB_QUERY_MAX_RESULT_BYTES (16 MiB)"
	if got := refusal.Error(); got != single {
		t.Errorf("single-node message\n got %q\nwant %q", got, single)
	}
	refusal.onOwner = true
	onOwner := strings.Replace(single, "(16 MiB)", "on the bucket's owner (16 MiB)", 1)
	if got := refusal.Error(); got != onOwner {
		t.Errorf("owner-stamped message\n got %q\nwant %q", got, onOwner)
	}
	refusal.documents = 0
	if got := refusal.Error(); !strings.Contains(got, "pop({limit: 1}) fits") || !strings.Contains(got, "after 0 entries") {
		t.Errorf("a refusal before any document fits one entry: %q", got)
	}
}

// TestPopBudgetThatDoesNotCountLeavesTheMetric: a replica's and a replay's
// budget refuses like the owner's but leaves the POP series alone, so one
// request is counted once however many nodes apply it.
func TestPopBudgetThatDoesNotCountLeavesTheMetric(t *testing.T) {
	s := newTestServer(t)
	for _, counts := range []bool{false, true} {
		budget := s.newResultBudgetWithCeiling(queryMethodPop, 1<<20, counts, resultTooLargeError{shape: resultShapePop})
		budget.chargeFirst(900 << 10)
		before := popRefusalsCounted()
		err := budget.chargeDocument(200 << 10)
		if !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("counts=%v: charge past the ceiling = %v, want ErrResultTooLarge", counts, err)
		}
		want := 0.0
		if counts {
			want = 1
		}
		if got := popRefusalsCounted() - before; got != want {
			t.Errorf("counts=%v: the POP refusal counter moved by %v, want %v", counts, got, want)
		}
	}
}

// TestPopBudgetCeilingIsTheOneGiven: the explicit ceiling, not the node's, is
// what the refusal reports and the budget enforces.
func TestPopBudgetCeilingIsTheOneGiven(t *testing.T) {
	s := newTestServer(t)
	budget := s.newResultBudgetWithCeiling(queryMethodPop, 3<<20, true, resultTooLargeError{shape: resultShapePop})
	if budget.ceiling != 3<<20 || budget.refusal.ceiling != 3<<20 {
		t.Fatalf("ceiling %d, refusal ceiling %d; want 3 MiB for both", budget.ceiling, budget.refusal.ceiling)
	}
	if got := s.maxResultBytes(); got != defaultMaxResultBytes {
		t.Errorf("maxResultBytes() = %d, want the default %d on a server without config", got, defaultMaxResultBytes)
	}
}

// TestPopRefusalNamesTheVerbsThatWork pins spec 8.5's three texts.
func TestPopRefusalNamesTheVerbsThatWork(t *testing.T) {
	const orderless = `pop is not defined for %s buckets: a %s has no order to take from, so there is no "next" to remove. `
	cases := map[BucketType]string{
		BucketTypeHash:  `Remove one with delete("<key>"), or read them with get("<key>") or find().`,
		BucketTypeBTree: `Remove one with delete("<key>"), or read them with get("<key>") or find().`,
		BucketTypeBlob:  `Remove one with delete("<key>"), or read its metadata with info("<key>").`,
	}
	for bucketType, ending := range cases {
		want := strings.NewReplacer("%s", string(bucketType)).Replace(orderless) + ending
		if got := popRefusal(bucketType); got != want {
			t.Errorf("popRefusal(%s)\n got %q\nwant %q", bucketType, got, want)
		}
	}
}
