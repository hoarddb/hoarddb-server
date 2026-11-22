// bench/probe — pipelining-window measurement probe.
//
// The stock bench never pipelines: every request waits for its response
// before the next is sent, so HOARDDB_TCP_PIPELINE_FRAMES can only be
// measured with a client that actually sends frames before reading
// (spec section 10, the fourth measurement). This probe inserts a small
// dataset, then issues pipelined GETs at a fixed pipeline depth and reports
// per-operation latency at the 50th and 99th percentile.
//
// Run against one server twice — once with the default window, once with
// HOARDDB_TCP_PIPELINE_FRAMES=1 — and compare p50/p99:
//
//	go run ./bench/probe -addr=127.0.0.1:7433 -depth=4 -ops=20000
//
// -mode mixed is query pipelining's BM-1: one connection, 90% GET and 10%
// filtered SCAN (a 100-document page over a 10,000-document bucket),
// reporting GET p50/p99 and SCAN throughput. Answers can come back out of
// order, so they are matched by request ID:
//
//	go run ./bench/probe -addr=127.0.0.1:7433 -mode=mixed -depth=4 -ops=20000
package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	// mixedBucketDocuments and mixedGroups shape BM-1's bucket: each group
	// is a 100-document page.
	mixedBucketDocuments = 10000
	mixedGroups          = 100
	// mixedScanEvery makes one operation in ten a SCAN.
	mixedScanEvery = 10
)

type probeConn struct {
	conn *tls.Conn
	br   *bufio.Reader
}

func (p *probeConn) send(op transport.Opcode, req interface{}) [transport.RequestIDLen]byte {
	payload, err := bson.Marshal(req)
	if err != nil {
		log.Fatalf("marshal %s: %v", op, err)
	}
	frame := transport.NewFrame(op, payload)
	if _, err := p.conn.Write(frame.Encode()); err != nil {
		log.Fatalf("write %s: %v", op, err)
	}
	return frame.RequestID
}

func (p *probeConn) read() *transport.Frame {
	f, err := transport.DecodeFrame(p.br)
	if err != nil {
		log.Fatalf("read a response: %v", err)
	}
	return f
}

// call sends one request and waits for its answer.
func (p *probeConn) call(op transport.Opcode, req interface{}) *transport.Frame {
	p.send(op, req)
	f := p.read()
	if f.Opcode == transport.OpError || f.Opcode == transport.OpAuthFail {
		log.Fatalf("%s answered %s: %s", op, f.Opcode, f.Payload)
	}
	return f
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7433", "TCP listener address")
	depth := flag.Int("depth", 4, "requests the probe keeps in flight on its one connection")
	ops := flag.Int("ops", 20000, "number of pipelined operations")
	mode := flag.String("mode", "get", "get: pipelined GETs; mixed: 90% GET and 10% filtered SCAN (BM-1)")
	flag.Parse()
	if *mode != "get" && *mode != "mixed" {
		log.Fatalf("-mode must be get or mixed, not %q", *mode)
	}

	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "bench/probe tool"}}, *addr)
	if err != nil {
		log.Fatalf("TLS config: %v", err)
	}
	conn, err := tls.Dial("tcp", *addr, tlsCfg)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Minute))
	probe := &probeConn{conn: conn, br: bufio.NewReader(conn)}

	probe.call(transport.OpAuth, &server.LoginRequest{Username: "admin", Password: "bench-root-password"})
	db := fmt.Sprintf("Probe%d", time.Now().UnixNano())
	probe.call(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: db})
	probe.call(transport.OpCreateBucket, &server.CreateBucketRequest{Database: db, Name: "C", Type: server.BucketTypeHash})

	if *mode == "mixed" {
		runMixed(probe, db, *depth, *ops)
		return
	}
	runGets(probe, db, *depth, *ops)
}

// runGets is the original probe: send `depth` GETs, then read `depth`
// responses, repeating. Latency is measured per operation as rounds ÷
// operations, so the window's ability to hide the per-op round trip is what
// shows up in p50/p99.
func runGets(probe *probeConn, db string, depth, ops int) {
	for i := 0; i < 100; i++ {
		probe.call(transport.OpUpsert, &server.InsertRequest{
			Database: db, Bucket: "C", Key: fmt.Sprintf("k%03d", i),
			Data: map[string]interface{}{"i": i},
		})
	}
	rounds := ops / depth
	latencies := make([]time.Duration, 0, ops)
	for r := 0; r < rounds; r++ {
		base := r % 100
		for i := 0; i < depth; i++ {
			probe.send(transport.OpGet, &server.FindRequest{Database: db, Bucket: "C", Key: fmt.Sprintf("k%03d", (base+i)%100)})
		}
		start := time.Now()
		for i := 0; i < depth; i++ {
			if f := probe.read(); f.Opcode != transport.OpGetResponse {
				log.Fatalf("response to GET = %s", f.Opcode)
			}
		}
		latencies = append(latencies, time.Since(start)/time.Duration(depth))
	}
	p50, p99 := percentiles(latencies)
	fmt.Printf("probe | window=%d | ops=%d | depth=%d | p50=%s | p99=%s\n",
		depth, ops, depth, p50.Round(time.Microsecond), p99.Round(time.Microsecond))
}

// inFlightOp is one request the mixed probe is waiting on.
type inFlightOp struct {
	scan   bool
	sentAt time.Time
}

// runMixed keeps `depth` requests in flight: each answer, matched to its
// request by ID, is replaced by the next request. A GET's latency is from its
// send to its answer, so it includes any time spent behind a SCAN.
func runMixed(probe *probeConn, db string, depth, ops int) {
	for i := 0; i < mixedBucketDocuments; i++ {
		probe.call(transport.OpUpsert, &server.InsertRequest{
			Database: db, Bucket: "C", Key: fmt.Sprintf("k%05d", i),
			Data: map[string]interface{}{"i": i, "g": i % mixedGroups},
		})
	}

	sendOp := func(n int, inFlight map[[transport.RequestIDLen]byte]inFlightOp) {
		if n%mixedScanEvery == mixedScanEvery-1 {
			id := probe.send(transport.OpScan, &server.ScanRequest{Database: db, Bucket: "C",
				Filter: map[string]interface{}{"g": (n / mixedScanEvery) % mixedGroups}, Limit: mixedBucketDocuments / mixedGroups})
			inFlight[id] = inFlightOp{scan: true, sentAt: time.Now()}
			return
		}
		id := probe.send(transport.OpGet, &server.FindRequest{Database: db, Bucket: "C", Key: fmt.Sprintf("k%05d", n%mixedBucketDocuments)})
		inFlight[id] = inFlightOp{sentAt: time.Now()}
	}

	inFlight := make(map[[transport.RequestIDLen]byte]inFlightOp, depth)
	getLatencies := make([]time.Duration, 0, ops)
	scans := 0
	sent := 0
	started := time.Now()
	for ; sent < depth && sent < ops; sent++ {
		sendOp(sent, inFlight)
	}
	for len(inFlight) > 0 {
		f := probe.read()
		op, ok := inFlight[f.RequestID]
		if !ok {
			log.Fatalf("an answer (%s) matches no request in flight", f.Opcode)
		}
		delete(inFlight, f.RequestID)
		switch {
		case op.scan && f.Opcode == transport.OpScan:
			scans++
		case !op.scan && f.Opcode == transport.OpGetResponse:
			getLatencies = append(getLatencies, time.Since(op.sentAt))
		default:
			log.Fatalf("a %s answered %s: %s", map[bool]string{true: "SCAN", false: "GET"}[op.scan], f.Opcode, f.Payload)
		}
		if sent < ops {
			sendOp(sent, inFlight)
			sent++
		}
	}
	elapsed := time.Since(started)
	p50, p99 := percentiles(getLatencies)
	fmt.Printf("probe | mode=mixed | ops=%d | depth=%d | get_p50=%s | get_p99=%s | scans=%d | scans_per_s=%.0f | elapsed=%s\n",
		ops, depth, p50.Round(time.Microsecond), p99.Round(time.Microsecond), scans, float64(scans)/elapsed.Seconds(), elapsed.Round(time.Millisecond))
}

func percentiles(latencies []time.Duration) (p50, p99 time.Duration) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies[len(latencies)/2], latencies[len(latencies)*99/100]
}
