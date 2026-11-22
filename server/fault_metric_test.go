package server

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/transport"
)

// faultsRecoveredMetric counts the request handler faults a node recovered
// from, one series per port and none per bucket.
const faultsRecoveredMetric = "hoarddb_request_faults_recovered_total"

// faultsRecovered reads the series of faultsRecoveredMetric for port.
func faultsRecovered(t *testing.T, port string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != faultsRecoveredMetric {
			continue
		}
		if len(family.GetMetric()) != 2 {
			t.Fatalf("%s has %d series, want two: one per port", faultsRecoveredMetric, len(family.GetMetric()))
		}
		for _, series := range family.GetMetric() {
			labels := series.GetLabel()
			if len(labels) == 1 && labels[0].GetName() == "port" && labels[0].GetValue() == port {
				return series.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("the registry exports no counter %s{port=%q}", faultsRecoveredMetric, port)
	return 0
}

// TestARecoveredFaultIsCounted: a fault in a request's handler, recovered at
// the request boundary, counts once on the port it came in on — a client's
// request on the driver port, a member's question on the internode port —
// and not on the other.
func TestARecoveredFaultIsCounted(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, testBucket, 0)
	owner, err := clusterOf(tc, 0).Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of the test bucket: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	otherIndex := 1 - ownerIndex

	t.Run("driver port", func(t *testing.T) {
		driverBefore, internodeBefore := faultsRecovered(t, "driver"), faultsRecovered(t, "internode")
		// The other node forwards the read to the owner; the fault is in the
		// handler of the client's request, before anything is sent.
		fault := func(entry ReplEntry, _ string) {
			if entry.Bucket == testBucket {
				panic("an injected fault in a client request's handler")
			}
		}
		forwardTargetHook.Store(&fault)
		t.Cleanup(func() { forwardTargetHook.Store(nil) })
		if answer := tc.frame(otherIndex, transport.OpGet, FindRequest{Database: testDatabase, Bucket: testBucket, Key: "k"}); answer.Opcode != transport.OpError {
			t.Fatalf("the faulting request was answered %s, want an error", answer.Opcode)
		}
		if got := faultsRecovered(t, "driver"); got != driverBefore+1 {
			t.Fatalf("%s{port=\"driver\"} = %v after one recovered fault, want %v", faultsRecoveredMetric, got, driverBefore+1)
		}
		if got := faultsRecovered(t, "internode"); got != internodeBefore {
			t.Fatalf("%s{port=\"internode\"} moved from %v to %v on a driver-port fault", faultsRecoveredMetric, internodeBefore, got)
		}
	})

	t.Run("internode port", func(t *testing.T) {
		driverBefore, internodeBefore := faultsRecovered(t, "driver"), faultsRecovered(t, "internode")
		ownerID := owner.ID
		fault := func(nodeID string) {
			if nodeID == ownerID {
				panic("an injected fault in a member's question's handler")
			}
		}
		catalogAnswerHook.Store(&fault)
		t.Cleanup(func() { catalogAnswerHook.Store(nil) })
		asker := clusterOf(tc, otherIndex)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := asker.askCatalog(ctx, mustNode(t, asker, ownerID), ReplCatalogAsk{Database: testDatabase}); err == nil {
			t.Fatal("the faulting question was answered")
		}
		if got := faultsRecovered(t, "internode"); got != internodeBefore+1 {
			t.Fatalf("%s{port=\"internode\"} = %v after one recovered fault, want %v", faultsRecoveredMetric, got, internodeBefore+1)
		}
		if got := faultsRecovered(t, "driver"); got != driverBefore {
			t.Fatalf("%s{port=\"driver\"} moved from %v to %v on an internode-port fault", faultsRecoveredMetric, driverBefore, got)
		}
	})
}
