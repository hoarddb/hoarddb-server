package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BenchmarkForwardedQueryThroughput is BM-2 (query-pipelining spec §8), the S-1
// measurement. Two nodes, RF = 1, every client on node 0, which owns none of
// the scanned bucket, so every scan is forwarded to node 1. 64 connections
// each run one filtered scan at a time; beside them a steady stream of
// majority writes to a bucket node 0 owns, and a stream of forwarded writes to
// a bucket node 1 owns (QF-12), each one write at a time.
//
// It reports forwarded-scan throughput and p99, write p99, forwarded-write
// p99, S (the coordinator's forwarded-result slots, which cap forwarded
// queries below the request lane's 96 per peer when GOMAXPROCS < 48, spec
// §13.5 and BUG-0723), and the highest control-lane in-flight count sampled. That gauge is
// QP-5's; before QP-5 it does not exist, and the figure reads -1.
//
// It uses only symbols that exist before QP-5, so the same file runs before
// and after. At RF = 1 a majority write ships nothing: the write stream
// measures the coordinator's own write path under forwarded load, as the spec
// configures it. The cluster's WAL is on for both runs, so before and after
// are like for like. None of these figures is a durability claim.
func BenchmarkForwardedQueryThroughput(b *testing.B) {
	const (
		scanners      = 64
		scanDocuments = 5000
		scanGroups    = 50
		scanLimit     = 20
	)
	// 64 scanners and two writers from one address: past the per-IP default.
	tc := newTestClusterWith(b, 2, 1, func(_ int, cfg *config.Config) { cfg.TCP.MaxConnsPerIP = 256 })
	defer tc.stop()
	const coordinator, owner = 0, 1
	cluster := tc.nodes[coordinator].server.core.cluster

	scanBucket := benchBucketOwnedBy(b, cluster, "scans", tc.addrs[owner])
	localBucket := benchBucketOwnedBy(b, cluster, "local", tc.addrs[coordinator])
	forwardedBucket := benchBucketOwnedBy(b, cluster, "forwarded", tc.addrs[owner])
	for database, bucket := range map[string]string{"scans": scanBucket, "local": localBucket, "forwarded": forwardedBucket} {
		tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		tc.ok(coordinator, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
	}
	seedGroupedBucket(b, tc.nodes[owner].server.core, "scans", scanBucket, scanDocuments, scanGroups)

	scanConns := make([]*queryConn, scanners)
	for i := range scanConns {
		qc, err := dialQueryConn(tc.driverAddrs[coordinator], testRootUser, testRootPassword)
		if err != nil {
			b.Fatalf("scan connection %d: %v", i, err)
		}
		defer func() { _ = qc.conn.Close() }()
		scanConns[i] = qc
	}
	writeConns := make([]*queryConn, 2)
	for i := range writeConns {
		qc, err := dialQueryConn(tc.driverAddrs[coordinator], testRootUser, testRootPassword)
		if err != nil {
			b.Fatalf("write connection %d: %v", i, err)
		}
		defer func() { _ = qc.conn.Close() }()
		writeConns[i] = qc
	}
	scanPayload, err := bson.Marshal(ScanRequest{Database: "scans", Bucket: scanBucket, Filter: map[string]interface{}{"group": "g7"}, Limit: scanLimit})
	if err != nil {
		b.Fatalf("encoding the scan: %v", err)
	}

	var (
		scansLeft     atomic.Int64
		scanLatencies latencyLog
		writeLatency  latencyLog
		fwdLatency    latencyLog
		failures      = make(chan error, scanners+2)
		stopWriters   = make(chan struct{})
	)
	scansLeft.Store(int64(b.N))

	b.ResetTimer()
	started := time.Now()
	controlPeak := sampleControlLaneInFlight(stopWriters)

	var writers sync.WaitGroup
	for i, target := range []struct {
		database, bucket string
		log              *latencyLog
	}{{"local", localBucket, &writeLatency}, {"forwarded", forwardedBucket, &fwdLatency}} {
		writers.Add(1)
		go func(qc *queryConn, database, bucket string, log *latencyLog) {
			defer writers.Done()
			for n := 0; ; n++ {
				select {
				case <-stopWriters:
					return
				default:
				}
				payload, err := bson.Marshal(InsertRequest{
					Database: database, Bucket: bucket, Key: fmt.Sprintf("w%08d", n),
					Data: map[string]interface{}{"n": int32(n)}, WriteConcern: "majority",
				})
				if err != nil {
					failures <- fmt.Errorf("encoding %s write %d: %w", database, n, err)
					return
				}
				if err := roundTrip(qc, transport.NewFrame(transport.OpPut, payload), log); err != nil {
					failures <- fmt.Errorf("%s write %d: %w", database, n, err)
					return
				}
			}
		}(writeConns[i], target.database, target.bucket, target.log)
	}

	var scanWorkers sync.WaitGroup
	for _, qc := range scanConns {
		scanWorkers.Add(1)
		go func(qc *queryConn) {
			defer scanWorkers.Done()
			for scansLeft.Add(-1) >= 0 {
				if err := roundTrip(qc, transport.NewFrame(transport.OpScan, scanPayload), &scanLatencies); err != nil {
					failures <- fmt.Errorf("forwarded scan: %w", err)
					return
				}
			}
		}(qc)
	}
	scanWorkers.Wait()
	elapsed := time.Since(started)
	close(stopWriters)
	writers.Wait()
	b.StopTimer()
	close(failures)
	for err := range failures {
		b.Fatal(err)
	}

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "scans/s")
	b.ReportMetric(scanLatencies.p99Millis(), "scan-p99-ms")
	b.ReportMetric(writeLatency.p99Millis(), "write-p99-ms")
	b.ReportMetric(fwdLatency.p99Millis(), "fwd-write-p99-ms")
	b.ReportMetric(float64(queryConcurrencyOf(tc.nodes[coordinator].server.core)), "S")
	b.ReportMetric(float64(<-controlPeak), "control-in-flight-max")
}

// roundTrip sends one request on a connection nobody else is using, reads its
// answer and records how long that took. An error answer is an error.
func roundTrip(qc *queryConn, frame *transport.Frame, log *latencyLog) error {
	sent := time.Now()
	if err := qc.send(frame); err != nil {
		return err
	}
	answer, err := qc.read()
	if err != nil {
		return err
	}
	if answer.Opcode == transport.OpError {
		return fmt.Errorf("%s answered with an error: %s", frame.Opcode, answer.Payload)
	}
	log.record(time.Since(sent))
	return nil
}

// latencyLog collects request latencies from several goroutines.
type latencyLog struct {
	mu        sync.Mutex
	latencies []time.Duration
}

func (l *latencyLog) record(latency time.Duration) {
	l.mu.Lock()
	l.latencies = append(l.latencies, latency)
	l.mu.Unlock()
}

func (l *latencyLog) p99Millis() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), l.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return float64(sorted[(len(sorted)-1)*99/100].Microseconds()) / 1000
}

// sampleControlLaneInFlight polls the internode in-flight gauge's control-lane
// series until stop closes, and then sends the highest sum it saw across
// peers: -1 when no such series exists, which is the tree before QP-5.
func sampleControlLaneInFlight(stop <-chan struct{}) <-chan float64 {
	peak := make(chan float64, 1)
	go func() {
		highest := -1.0
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			if current, found := controlLaneInFlight(); found && current > highest {
				highest = current
			}
			select {
			case <-stop:
				peak <- highest
				return
			case <-ticker.C:
			}
		}
	}()
	return peak
}

func controlLaneInFlight() (float64, bool) {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0, false
	}
	for _, family := range families {
		if family.GetName() != "hoarddb_internode_in_flight" {
			continue
		}
		var total float64
		for _, series := range family.GetMetric() {
			for _, label := range series.GetLabel() {
				if label.GetName() == "lane" && label.GetValue() == "control" {
					total += series.GetGauge().GetValue()
				}
			}
		}
		return total, true
	}
	return 0, false
}

// benchBucketOwnedBy is bucketNameOwnedBy for a benchmark.
func benchBucketOwnedBy(tb testing.TB, c *Cluster, database, nodeID string) string {
	tb.Helper()
	for i := 0; i < 512; i++ {
		name := fmt.Sprintf("bucket-%d", i)
		owner, err := c.Owner(placementOf(database, name))
		if err != nil {
			tb.Fatalf("ring owner of %q: %v", name, err)
		}
		if owner.ID == nodeID {
			return name
		}
	}
	tb.Fatalf("no bucket name in 512 tries is placed on %q", nodeID)
	return ""
}

// seedGroupedBucket fills a hash bucket with count documents of about 1 KiB,
// spread over groups, so a filter on one group walks the bucket to find its
// page.
func seedGroupedBucket(tb testing.TB, s *Server, database, bucket string, count, groups int) {
	tb.Helper()
	padding := strings.Repeat("x", 1000)
	const batchSize = 1000
	for start := 0; start < count; start += batchSize {
		batch := make([]map[string]interface{}, 0, batchSize)
		for i := start; i < start+batchSize && i < count; i++ {
			batch = append(batch, map[string]interface{}{
				"_id_":  fmt.Sprintf("k%06d", i),
				"group": fmt.Sprintf("g%d", i%groups),
				"n":     int32(i),
				"pad":   padding,
			})
		}
		resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: batch})
		if err != nil || !resp.Success {
			tb.Fatalf("seeding %s.%s at %d: %+v, %v", database, bucket, start, resp, err)
		}
	}
}
