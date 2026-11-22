// bench/resp_bench.go — Benchmark Redis/Valkey over RESP.
//
// Workload shape is deliberately identical to the other engines:
//   - insert N records {username, email} in batches of 1000 per round trip
//   - read N random records as point lookups by key
//   - start from an empty keyspace (FLUSHALL == Mongo drop == MySQL DROP TABLE
//     == hoardDB fresh database)
//
// The one honest difference from the other engines is the durability model,
// which is why three separate servers are measured: Valkey and Redis with
// default persistence (RDB snapshots, effectively memory-first) and Redis with
// AOF appendfsync=always (every write actually durable). Comparing a
// disk-backed store against a memory-only one proves nothing; the durable
// configuration is the fair fight.
package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

const (
	valkeyAddr   = "localhost:6379"
	redisAddr    = "localhost:6380"
	redisDurAddr = "localhost:6381"
)

func benchRESP(label, addr, mode string, totalRecords, readCount, pipeline int) {
	if pipeline < 1 {
		pipeline = 1
	}
	batchSize := 1000
	if totalRecords < batchSize {
		batchSize = totalRecords
	}

	rc, err := dialRESP(addr)
	if err != nil {
		log.Fatalf("connect %s: %v", addr, err)
	}
	defer rc.close()

	// Fresh keyspace, so no leg inherits another leg's data.
	rc.queue("FLUSHALL")
	if err := rc.flush(); err != nil {
		log.Fatalf("flushall write: %v", err)
	}
	if _, err := rc.reply(); err != nil {
		log.Fatalf("flushall: %v", err)
	}

	fmt.Printf("%s | %s | %d records | %d reads | pipeline=%d\n", label, mode, totalRecords, readCount, pipeline)

	value := func(i int) string {
		return fmt.Sprintf(`{"username":"user_%06d","email":"u%d@test.com"}`, i, i)
	}
	key := func(i int) string { return fmt.Sprintf("user_%06d", i) }

	// ---- Insert ------------------------------------------------------------
	insertStart := time.Now()
	switch mode {
	case "single":
		for i := 0; i < totalRecords; i++ {
			rc.queue("SET", key(i), value(i))
			if err := rc.flush(); err != nil {
				log.Fatalf("write: %v", err)
			}
			if _, err := rc.reply(); err != nil {
				log.Fatalf("reply: %v", err)
			}
		}
	case "batch":
		for i := 0; i < totalRecords; i += batchSize {
			end := min(i+batchSize, totalRecords)
			for j := i; j < end; j++ {
				rc.queue("SET", key(j), value(j))
			}
			if err := rc.flush(); err != nil {
				log.Fatalf("batch write: %v", err)
			}
			for j := i; j < end; j++ {
				if _, err := rc.reply(); err != nil {
					log.Fatalf("batch reply: %v", err)
				}
			}
		}
	default:
		log.Fatalf("unknown mode %q", mode)
	}
	insertTime := time.Since(insertStart)

	// ---- Read --------------------------------------------------------------
	// `pipeline` commands are queued before any reply is read, so `pipeline`
	// operations are in flight at once. pipeline=1 is strictly one-at-a-time and
	// therefore measures round-trip latency, not throughput.
	hits, misses := 0, 0
	inFlight := 0
	readStart := time.Now()
	for i := 0; i < readCount; i++ {
		rc.queue("GET", key(rand.Intn(totalRecords)))
		inFlight++
		if inFlight == pipeline || i == readCount-1 {
			if err := rc.flush(); err != nil {
				log.Fatalf("read write: %v", err)
			}
			for j := 0; j < inFlight; j++ {
				v, err := rc.reply()
				if err != nil {
					log.Fatalf("read reply: %v", err)
				}
				if v != "" {
					hits++
				} else {
					misses++
				}
			}
			inFlight = 0
		}
	}
	readTime := time.Since(readStart)

	if hits == 0 {
		log.Fatalf("WARNING: all %d reads missed — not measuring point lookups", readCount)
	}

	totalTime := insertTime + readTime
	fmt.Printf("  Insert: %v (%.0f/s) | Read: %v (%.0f/s) | Total: %v (%.0f/s) | hits=%d misses=%d\n",
		insertTime.Round(time.Millisecond), float64(totalRecords)/insertTime.Seconds(),
		readTime.Round(time.Millisecond), float64(readCount)/readTime.Seconds(),
		totalTime.Round(time.Millisecond), float64(totalRecords+readCount)/totalTime.Seconds(),
		hits, misses)
}
