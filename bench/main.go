// bench/main.go — Unified benchmark runner for hoardDB, MongoDB, MySQL, and Redis/Valkey.
//
// Usage:
//
//	go run ./bench -db=hoarddb -mode=batch -records=1000
//	go run ./bench -db=hoarddb -mode=batch -records=100000
//	go run ./bench -db=hoarddb -mode=batch -records=1000000
//	go run ./bench -db=hoarddb -mode=batch -records=10000000
//	go run ./bench -db=valkey  -mode=batch -records=1000000 -pipeline=100
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// splitAddrs parses the -addr flag: a single node, or a comma-separated list
// for a client that round-robins its requests across nodes.
func splitAddrs(spec string) []string {
	var out []string
	for _, part := range strings.Split(spec, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	dbFlag := flag.String("db", "", "Database to benchmark: hoarddb, mongo, mysql, postgres, cockroach, valkey, redis, redis-durable")
	modeFlag := flag.String("mode", "", "Insert mode: single (1 doc/call), batch (1000 docs/call)")
	recordsFlag := flag.Int("records", 100_000, "Number of records to insert (1K, 100K, 1M, 10M)")
	readRatio := flag.Int("reads", 0, "Number of reads (default: same as records)")
	pipelineFlag := flag.Int("pipeline", 1, "Redis/Valkey: commands queued per round trip (1 = strictly sequential)")
	concFlag := flag.Int("conc", 1, "requests in flight: hoardDB pooled sockets, postgres connections (1 = strictly sequential)")
	dbnameFlag := flag.String("dbname", "", "hoardDB: override the generated per-run database name (attach to an existing dataset)")
	noInsertFlag := flag.Bool("noinsert", false, "hoardDB: skip inserts and read existing data (-records still defines the key range)")
	wcFlag := flag.String("writeconcern", "", "hoardDB: durability per request (one, majority, all); empty uses the node's configured default")
	addrFlag := flag.String("addr", "localhost:7433", "hoardDB: node address, or a comma-separated list to round-robin requests across nodes")
	getFastFlag := flag.Bool("getfast", false, "hoardDB: route point reads through the compact GET_FAST opcode")
	engineFlag := flag.String("engine", "", "rank: engine to drive (hoarddb, mongo, mysql, postgres, cockroach); -records is docs per bucket")
	repFlag := flag.Int("rep", 1, "rank: repetition number recorded in each JSON sample")
	mongoJournalFlag := flag.Bool("mongo-journal", false, "rank: write to MongoDB with {j:true} instead of the driver default")
	summaryFlag := flag.String("summary", "", "rank-summary: JSON-lines file written by -db=rank legs")
	flag.Parse()

	// The keyed rank (bench/rank_bench.go) takes no -mode: every op is a single
	// request, one in flight.
	switch *dbFlag {
	case "rank":
		if err := runRank(*engineFlag, *recordsFlag, *repFlag, *mongoJournalFlag, splitAddrs(*addrFlag)); err != nil {
			fmt.Fprintf(os.Stderr, "rank: %v\n", err)
			os.Exit(1)
		}
		return
	case "rank-summary":
		if err := summarizeRank(*summaryFlag); err != nil {
			fmt.Fprintf(os.Stderr, "rank-summary: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *dbFlag == "" || *modeFlag == "" {
		fmt.Fprintf(os.Stderr, "Usage: go run ./bench -db=<hoarddb|mongo|mysql|postgres|cockroach|valkey|redis|redis-durable> -mode=<single|batch> -records=<N> [-pipeline=N] [-conc=N] [-addr=host:port[,host:port...]]\n")
		os.Exit(1)
	}

	reads := *readRatio
	if reads == 0 {
		reads = *recordsFlag
	}

	switch *dbFlag {
	case "hoarddb":
		benchHoardDB(*modeFlag, *recordsFlag, reads, *concFlag, *dbnameFlag, *noInsertFlag, *wcFlag, splitAddrs(*addrFlag), *getFastFlag)
	case "mongo":
		benchMongo(*modeFlag, *recordsFlag, reads)
	case "mysql":
		benchMySQL(*modeFlag, *recordsFlag, reads)
	case "postgres":
		benchPostgres(*modeFlag, *recordsFlag, reads, *concFlag)
	case "cockroach":
		benchCockroach(*modeFlag, *recordsFlag, reads)
	case "valkey":
		benchRESP("Valkey 9", valkeyAddr, *modeFlag, *recordsFlag, reads, *pipelineFlag)
	case "redis":
		benchRESP("Redis 8", redisAddr, *modeFlag, *recordsFlag, reads, *pipelineFlag)
	case "redis-durable":
		benchRESP("Redis 8 (AOF appendfsync=always)", redisDurAddr, *modeFlag, *recordsFlag, reads, *pipelineFlag)
	default:
		fmt.Fprintf(os.Stderr, "Unknown database: %s\n", *dbFlag)
		os.Exit(1)
	}
}
