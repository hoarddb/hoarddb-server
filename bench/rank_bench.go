// bench/rank_bench.go — the keyed-workload rank: hoardDB, MongoDB and MySQL
// driven through one workload and one measurement, so the comparison is one
// table rather than three differently-measured programs.
//
// The workload is bench/smokeperf's keyedLike, per engine:
//
//	hash  ↔ Mongo collection under _id  ↔ MySQL table under its PRIMARY KEY
//	btree ↔ the same, plus a range scan on the key
//
//	insert     N upserts of k0000000.., each acknowledged and counted
//	point-read N reads back in key order, every document's seq and payload
//	           compared with what was written (a wrong read FAILS the leg)
//	range      (btree only) rankRangeScans bands of rankRangeWidth keys, every
//	           band's length and order verified
//
// Every op is timed alone, one request in flight: the hoardDB leg uses one
// socket, the Mongo leg issues one command at a time, the MySQL leg holds one
// connection with server-side prepared statements. Verification runs outside
// the timed window. fifo, lifo and heap are deliberately absent: MongoDB and
// MySQL have no such store type, and a hand-built queue is not timed until a
// concurrency test proves it correct (the queue-layout spike's rule).
//
// Each leg prints one JSON line per (store, op) to stdout, and -db=rank-summary
// folds a file of those lines into the markdown table the report quotes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

const (
	// rankRangeWidth is the band a range scan covers, and rankRangeScans how
	// many bands are timed — enough samples for a p99.
	rankRangeWidth = 100
	rankRangeScans = 200
)

// rankLeg identifies one engine run: what it is, how durable its writes are,
// and which repetition and scale it measured.
type rankLeg struct {
	Engine     string `json:"engine"`
	Durability string `json:"durability"`
	Docs       int    `json:"docs"`
	Rep        int    `json:"rep"`
}

// rankSample is one (store, op) measurement of a leg, as emitted on stdout.
type rankSample struct {
	rankLeg
	Store     string  `json:"store"`
	Op        string  `json:"op"`
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Micros float64 `json:"p50_us"`
	P99Micros float64 `json:"p99_us"`
	MaxMicros float64 `json:"max_us"`
	Verify    string  `json:"verify"`
	Verified  bool    `json:"verified"`
}

// rankDoc is the i-th document of the workload, identical on every engine.
type rankDoc struct {
	Key     string
	Seq     int64
	Payload string
}

func rankDocAt(i int) rankDoc {
	return rankDoc{
		Key:     fmt.Sprintf("k%07d", i),
		Seq:     int64(i),
		Payload: fmt.Sprintf("job-%d message body %d", i, i*i),
	}
}

// rankBandStart spreads the range bands over the key space deterministically,
// so every engine scans the same bands in the same order.
func rankBandStart(scan, docs int) int {
	span := docs - rankRangeWidth + 1
	return (scan * 7919) % span
}

// rankTimings accumulates the latency of every op of one kind.
type rankTimings struct {
	latencies []time.Duration
}

func (t *rankTimings) time(op func() error) error {
	start := time.Now()
	err := op()
	t.latencies = append(t.latencies, time.Since(start))
	return err
}

// sample turns the latencies into a rankSample. ops/s is ops over the summed
// op latency: with one request in flight it is 1/mean latency, and it excludes
// the verification work done between ops.
func (t *rankTimings) sample(leg rankLeg, store, op, verify string, verified bool) rankSample {
	sorted := append([]time.Duration(nil), t.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, latency := range sorted {
		total += latency
	}
	micros := func(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
	return rankSample{
		rankLeg:   leg,
		Store:     store,
		Op:        op,
		Ops:       len(sorted),
		OpsPerSec: float64(len(sorted)) / total.Seconds(),
		P50Micros: micros(sorted[len(sorted)/2]),
		P99Micros: micros(sorted[len(sorted)*99/100]),
		MaxMicros: micros(sorted[len(sorted)-1]),
		Verify:    verify,
		Verified:  verified,
	}
}

// rankEmit writes one sample as a JSON line on stdout and a readable line on
// stderr. A sample that failed verification is still emitted — so the raw log
// shows it — and then fails the leg.
func rankEmit(sample rankSample) error {
	line, err := json.Marshal(sample)
	if err != nil {
		return fmt.Errorf("encoding the %s/%s sample: %w", sample.Store, sample.Op, err)
	}
	fmt.Println(string(line))
	fmt.Fprintf(os.Stderr, "%-24s %-6s %-10s %9.0f ops/s  p50 %8.1fµs  p99 %8.1fµs  %s\n",
		sample.Engine, sample.Store, sample.Op, sample.OpsPerSec, sample.P50Micros, sample.P99Micros, sample.Verify)
	if !sample.Verified {
		return fmt.Errorf("%s %s/%s failed verification: %s", sample.Engine, sample.Store, sample.Op, sample.Verify)
	}
	return nil
}

// rankStore is one keyed store of one engine: the three operations the
// workload runs, each a single request.
type rankStore interface {
	// upsert writes doc under doc.Key and reports whether the engine says it
	// created a new record (false when it cannot tell).
	upsert(doc rankDoc) (created bool, err error)
	// read returns the document under key and whether it was found.
	read(key string) (rankDoc, bool, error)
	// scan returns every document with low <= key <= high, in key order.
	scan(low, high string) ([]rankDoc, error)
}

// rankEngine opens the two keyed stores of one engine on fresh state.
type rankEngine interface {
	leg() rankLeg
	// createdKnown says whether upsert's created flag is meaningful.
	createdKnown() bool
	open(store string) (rankStore, error)
	close() error
}

// runRank opens the named engine and drives the workload against it.
func runRank(engineName string, docs, rep int, mongoJournal bool, addrs []string) error {
	if docs < rankRangeWidth {
		return fmt.Errorf("-records=%d is below the range band of %d keys; use at least %d", docs, rankRangeWidth, rankRangeWidth)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	leg := rankLeg{Docs: docs, Rep: rep}
	var engine rankEngine
	var err error
	switch engineName {
	case "hoarddb":
		if len(addrs) != 1 {
			return fmt.Errorf("the rank drives one hoardDB node; -addr names %d", len(addrs))
		}
		leg.Engine = "hoardDB"
		leg.Durability = "single node (RF=1): no WAL; Badger WithSyncWrites(false) — acknowledged without fsync"
		engine, err = newRankHoardDB(ctx, addrs[0], leg)
	case "mongo":
		engine, err = newRankMongo(ctx, mongoJournal, leg)
	case "mysql":
		engine, err = newRankMySQL(ctx, leg)
	case "postgres":
		engine, err = newRankPostgres(ctx, leg)
	case "cockroach":
		engine, err = newRankCockroach(ctx, leg)
	default:
		return fmt.Errorf("unknown -engine %q; use hoarddb, mongo, mysql, postgres or cockroach", engineName)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "rank leg: %s, %d docs/bucket, rep %d — %s\n", engine.leg().Engine, docs, rep, engine.leg().Durability)
	return errors.Join(runRankLeg(engine), engine.close())
}

// runRankLeg drives the whole workload against one engine.
func runRankLeg(engine rankEngine) error {
	for _, store := range []string{"hash", "btree"} {
		keyed, err := engine.open(store)
		if err != nil {
			return fmt.Errorf("opening the %s store on %s: %w", store, engine.leg().Engine, err)
		}
		if err := rankInsert(engine, store, keyed); err != nil {
			return err
		}
		if err := rankPointRead(engine.leg(), store, keyed); err != nil {
			return err
		}
		if store == "btree" {
			if err := rankRange(engine.leg(), store, keyed); err != nil {
				return err
			}
		}
	}
	return nil
}

func rankInsert(engine rankEngine, store string, keyed rankStore) error {
	leg := engine.leg()
	var timings rankTimings
	created := 0
	for i := 0; i < leg.Docs; i++ {
		doc := rankDocAt(i)
		var wasCreated bool
		err := timings.time(func() error {
			var upsertErr error
			wasCreated, upsertErr = keyed.upsert(doc)
			return upsertErr
		})
		if err != nil {
			return fmt.Errorf("%s %s: upserting %s: %w", leg.Engine, store, doc.Key, err)
		}
		if wasCreated {
			created++
		}
	}
	verify := fmt.Sprintf("acked=%d created=unreported", leg.Docs)
	verified := true
	if engine.createdKnown() {
		// On fresh state every upsert must create: a smaller count means the
		// collection or table was not empty when the leg began.
		verify = fmt.Sprintf("acked=%d created=%d", leg.Docs, created)
		verified = created == leg.Docs
	}
	return rankEmit(timings.sample(leg, store, "insert", verify, verified))
}

func rankPointRead(leg rankLeg, store string, keyed rankStore) error {
	var timings rankTimings
	hits, misses, mismatches := 0, 0, 0
	for i := 0; i < leg.Docs; i++ {
		want := rankDocAt(i)
		var got rankDoc
		var found bool
		err := timings.time(func() error {
			var readErr error
			got, found, readErr = keyed.read(want.Key)
			return readErr
		})
		if err != nil {
			return fmt.Errorf("%s %s: reading %s: %w", leg.Engine, store, want.Key, err)
		}
		switch {
		case !found:
			misses++
		case got != want:
			hits++
			mismatches++
		default:
			hits++
		}
	}
	verify := fmt.Sprintf("hits=%d misses=%d mismatches=%d", hits, misses, mismatches)
	return rankEmit(timings.sample(leg, store, "point-read", verify, hits == leg.Docs && mismatches == 0))
}

func rankRange(leg rankLeg, store string, keyed rankStore) error {
	var timings rankTimings
	badBands, matchedDocs := 0, 0
	for scan := 0; scan < rankRangeScans; scan++ {
		first := rankBandStart(scan, leg.Docs)
		low, high := rankDocAt(first).Key, rankDocAt(first+rankRangeWidth-1).Key
		var band []rankDoc
		err := timings.time(func() error {
			var scanErr error
			band, scanErr = keyed.scan(low, high)
			return scanErr
		})
		if err != nil {
			return fmt.Errorf("%s %s: scanning [%s, %s]: %w", leg.Engine, store, low, high, err)
		}
		bandOK := len(band) == rankRangeWidth
		for offset := 0; bandOK && offset < len(band); offset++ {
			if band[offset] != rankDocAt(first+offset) {
				bandOK = false
			}
		}
		if bandOK {
			matchedDocs += len(band)
		} else {
			badBands++
		}
	}
	verify := fmt.Sprintf("bands=%d width=%d matched_docs=%d bad_bands=%d", rankRangeScans, rankRangeWidth, matchedDocs, badBands)
	return rankEmit(timings.sample(leg, store, "range", verify, badBands == 0))
}
