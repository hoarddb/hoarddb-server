// bench/hoarddb_bench.go — Benchmark hoardDB over TCP+TLS with configurable
// scale.
//
// Documents are keyed by username ("_id_" = username) so point reads are a true
// 1-for-1 match with the MongoDB/MySQL/Redis benchmarks, which all read by key.
//
// The insert phase keeps `conc` requests in flight the same way: -conc 1 is
// one request at a time, -conc N is N, for -mode=single and -mode=batch alike.
// Insert figures recorded before -conc sized the insert phase were all single-stream.
//
// Reads run either strictly sequentially (conc=1, one request in flight) or
// across a pool of `conc` sockets. Both numbers matter and they answer
// different questions: conc=1 is round-trip latency and is the only figure
// comparable to a strictly sequential client, while conc>1 is what the server
// can sustain. A TCP connection is single-request-safe, so conc>1 costs one
// socket per unit of concurrency (bench/tcp_client.go).
//
// -dbname + -noinsert exist so a dataset can be inserted once and then read
// concurrently by several processes — the only way to tell a per-connection
// ceiling apart from a global one.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
)

func benchHoardDB(mode string, totalRecords, readCount, conc int, dbNameOverride string, noInsert bool, writeConcern string, addrs []string, getFast bool) {
	batchSize := 1000
	if totalRecords < 1000 {
		batchSize = totalRecords
	}
	if conc < 1 {
		conc = 1
	}
	if len(addrs) == 0 {
		addrs = []string{"localhost:7433"}
	}

	// Unique database per run so repeated benchmarks never collide — unless the
	// caller names one explicitly, which is how reads-only runs re-attach to a
	// dataset inserted by an earlier process.
	dbName := dbNameOverride
	if dbName == "" {
		dbName = fmt.Sprintf("Bench%d_%d", totalRecords, time.Now().UnixNano())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7200*time.Second)
	defer cancel()

	// A pool of one socket per unit of concurrency: a TCP connection is
	// single-request-safe, so -conc sockets carry -conc requests in flight.
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "hoarddb_bench benchmark harness"}}, addrs[0])
	if err != nil {
		log.Fatalf("Failed to create TLS config for %s: %v", addrs[0], err)
	}
	pool, err := newTCPBenchClient(ctx, addrs, tlsCfg, conc)
	if err != nil {
		log.Fatalf("Failed to open TCP pool to %v: %v", addrs, err)
	}
	clients := []benchClient{pool}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	var nextClient atomic.Int64
	pick := func() benchClient {
		if len(clients) == 1 {
			return clients[0]
		}
		return clients[int(nextClient.Add(1)-1)%len(clients)]
	}

	send := func(op transport.Opcode, req interface{}, resp interface{}) error {
		return sendRequest(ctx, pick(), op, req, resp)
	}

	// findOne performs a point read using whichever read encoding the run
	// selected. Both paths must answer the same document; the benchmark's
	// hit/miss check is what makes a fast-but-wrong read impossible to report.
	findOne := func(key string) (server.FindResponse, error) {
		if !getFast {
			var resp server.FindResponse
			if err := send(transport.OpGet, &server.FindRequest{Database: dbName, Bucket: "User", Key: key}, &resp); err != nil {
				return server.FindResponse{}, err
			}
			return resp, nil
		}
		frame, err := pick().SendGetFast(ctx, transport.GetFastRequest{Database: dbName, Bucket: "User", Key: key})
		if err != nil {
			return server.FindResponse{}, err
		}
		status, document, err := transport.UnmarshalGetFastResponse(frame.Payload)
		if err != nil {
			return server.FindResponse{}, err
		}
		return server.FindResponse{Found: status == transport.GetFastFound, Data: document}, nil
	}

	must := func(err error, what string) {
		if err != nil {
			log.Fatalf("%s: %v", what, err)
		}
	}

	// Setup. A reads-only run attaches to an existing database, so it must not
	// try to create it again. The pooled TCP client already authenticated
	// every socket when it opened the pool.
	if !noInsert {
		must(send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: dbName}, nil), "create database")
		must(send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: dbName, Name: "User", Type: server.BucketTypeHash}, nil), "create bucket")
	}

	suffix := ""
	if noInsert {
		suffix = " | reads-only"
	}
	nodes := "nodes=1"
	if len(addrs) > 1 {
		nodes = fmt.Sprintf("nodes=%d round-robin", len(addrs))
	}
	readPath := "getfast=false"
	if getFast {
		readPath = "getfast=true"
	}
	fmt.Printf("hoardDB TCP | %s | db=%s | %d records | %d reads | insert-conc=%d | read-conc=%d | %s | WC=%s | %s%s\n", mode, dbName, totalRecords, readCount, conc, conc, nodes, writeConcernLabel(writeConcern), readPath, suffix)

	// Insert — keyed by username so reads below are real point lookups.
	var insertTime time.Duration
	if !noInsert {
		insertStart := time.Now()
		must(runInsertPhase(ctx, pool, insertPhaseRequest{
			Mode:         mode,
			Database:     dbName,
			WriteConcern: writeConcern,
			TotalRecords: totalRecords,
			BatchSize:    batchSize,
			Conc:         conc,
		}), "insert")
		insertTime = time.Since(insertStart)
	}

	// Read — random point lookups by username; count hits so a silently-empty
	// read path can never masquerade as a fast one.
	hits, misses := 0, 0
	readStart := time.Now()
	if conc == 1 {
		for i := 0; i < readCount; i++ {
			key := fmt.Sprintf("user_%06d", rand.Intn(totalRecords))
			resp, err := findOne(key)
			must(err, "find")
			if resp.Found {
				hits++
			} else {
				misses++
			}
		}
	} else {
		// conc goroutines share the socket pool (bench/tcp_client.go), so
		// `conc` lookups are genuinely in flight at once.
		var issued, hitCount, missCount int64
		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var h, m int64
				for {
					n := atomic.AddInt64(&issued, 1) - 1
					if n >= int64(readCount) {
						break
					}
					key := fmt.Sprintf("user_%06d", rand.Intn(totalRecords))
					resp, err := findOne(key)
					must(err, "find")
					if resp.Found {
						h++
					} else {
						m++
					}
				}
				atomic.AddInt64(&hitCount, h)
				atomic.AddInt64(&missCount, m)
			}()
		}
		wg.Wait()
		hits, misses = int(hitCount), int(missCount)
	}
	readTime := time.Since(readStart)

	if hits == 0 {
		fmt.Fprintf(os.Stderr, "WARNING: all %d reads missed — benchmark is not measuring point lookups\n", readCount)
	}

	insertCol := "skipped (reads-only)"
	if !noInsert {
		insertCol = fmt.Sprintf("%v (%.0f/s)", insertTime.Round(time.Millisecond), float64(totalRecords)/insertTime.Seconds())
	}
	totalTime := insertTime + readTime
	fmt.Printf("  Insert: %s | Read: %v (%.0f/s) | Total: %v (%.0f/s) | hits=%d misses=%d\n",
		insertCol,
		readTime.Round(time.Millisecond), float64(readCount)/readTime.Seconds(),
		totalTime.Round(time.Millisecond), float64(totalRecords+readCount)/totalTime.Seconds(),
		hits, misses)
}

// writeConcernLabel renders the -writeconcern flag for the run header, so a
// recorded leg states the durability level it actually measured rather than
// leaving the reader to guess whether the node default applied.
func writeConcernLabel(writeConcern string) string {
	if strings.TrimSpace(writeConcern) == "" {
		return "node-default"
	}
	return writeConcern
}

// insertPhaseRequest describes the insert phase of one benchmark run.
type insertPhaseRequest struct {
	Mode         string // "single" sends one OpUpsert per record, "batch" one OpBatchUpsert per BatchSize records
	Database     string
	WriteConcern string
	TotalRecords int
	BatchSize    int
	Conc         int // requests kept in flight
}

// runInsertPhase loads TotalRecords documents keyed user_NNNNNN, which is the
// key the read phase looks up. Conc workers pull the next unsent request from
// one counter, so Conc requests are in flight and every key is sent once.
func runInsertPhase(ctx context.Context, client benchClient, plan insertPhaseRequest) error {
	requestCount := plan.TotalRecords
	sendRequestNumber := func(requestNumber int) error {
		return insertOne(ctx, client, plan, requestNumber)
	}
	if plan.Mode == "batch" {
		requestCount = (plan.TotalRecords + plan.BatchSize - 1) / plan.BatchSize
		sendRequestNumber = func(requestNumber int) error {
			return insertBatch(ctx, client, plan, requestNumber*plan.BatchSize)
		}
	}

	var (
		nextRequest atomic.Int64
		failed      atomic.Bool
		firstErr    error
		firstErrMu  sync.Mutex
		workers     sync.WaitGroup
	)
	for range max(plan.Conc, 1) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for !failed.Load() {
				requestNumber := int(nextRequest.Add(1) - 1)
				if requestNumber >= requestCount {
					return
				}
				if err := sendRequestNumber(requestNumber); err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
					failed.Store(true)
					return
				}
			}
		}()
	}
	workers.Wait()
	return firstErr
}

func insertOne(ctx context.Context, client benchClient, plan insertPhaseRequest, recordIndex int) error {
	key := fmt.Sprintf("user_%06d", recordIndex)
	doc := map[string]interface{}{
		"_id_":     key,
		"username": key,
		"email":    fmt.Sprintf("u%d@test.com", recordIndex),
	}
	request := &server.InsertRequest{Database: plan.Database, Bucket: "User", Key: key, Data: doc, WriteConcern: plan.WriteConcern}
	if err := sendRequest(ctx, client, transport.OpUpsert, request, nil); err != nil {
		return fmt.Errorf("insert %s: %w", key, err)
	}
	return nil
}

func insertBatch(ctx context.Context, client benchClient, plan insertPhaseRequest, firstRecord int) error {
	end := min(firstRecord+plan.BatchSize, plan.TotalRecords)
	docs := make([]map[string]interface{}, end-firstRecord)
	for offset := range docs {
		key := fmt.Sprintf("user_%06d", firstRecord+offset)
		docs[offset] = map[string]interface{}{
			"_id_":     key,
			"username": key,
			"email":    fmt.Sprintf("u%d@test.com", firstRecord+offset),
		}
	}
	request := &server.BatchInsertRequest{Database: plan.Database, Bucket: "User", Data: docs, WriteConcern: plan.WriteConcern}
	if err := sendRequest(ctx, client, transport.OpBatchUpsert, request, nil); err != nil {
		return fmt.Errorf("batch insert from record %d: %w", firstRecord, err)
	}
	return nil
}

// sendRequest marshals req, sends it on client and unmarshals the reply into
// resp when there is one.
func sendRequest(ctx context.Context, client benchClient, op transport.Opcode, req interface{}, resp interface{}) error {
	payload, err := bson.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	f, err := client.SendRequest(ctx, op, payload)
	if err != nil {
		return err
	}
	if f.Opcode == transport.OpError {
		var errResp server.ErrorResponse
		if err := bson.Unmarshal(f.Payload, &errResp); err != nil {
			return fmt.Errorf("server error")
		}
		return fmt.Errorf("server error: %s", errResp.Message)
	}
	if resp != nil && len(f.Payload) > 0 {
		if err := bson.Unmarshal(f.Payload, resp); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
	}
	return nil
}
