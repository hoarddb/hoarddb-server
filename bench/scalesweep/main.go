// scalesweep grows one single-node database in steps and measures the same
// fixed workload at each size, to find where throughput or tail latency breaks.
//
// It is driven by scripts/bench/competitor-scale-sweep.sh, which owns the
// containers, the free-disk guard and the break rule; this binary does the
// parts that need a database client: create the table, load a key range, run
// one measured leg. It prints no address.
//
//	scalesweep -mode init  -engine mongo -mongo-uri mongodb://host:27017
//	scalesweep -mode init  -engine hoarddb -addr host:7433 -password ...   # hu layout, one bucket per owner
//	scalesweep -mode next  ...             # prints the next free key
//	scalesweep -mode load  ... -from 0 -to 100000
//	scalesweep -mode leg   ... -out leg.json
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

type sweepFlags struct {
	mode, engine, mongoURI, mongoWC, mysqlDSN, out string
	hoardAddr, hoardPassword, hoardDatabase        string
	hoardDurability, layout                        string
	from, to, fanout                               int64
	workers, bodyBytes                             int
	warmup, measured                               time.Duration
	seed                                           uint64
}

func parseFlags(args []string) (sweepFlags, error) {
	var opt sweepFlags
	flags := flag.NewFlagSet("scalesweep", flag.ContinueOnError)
	flags.StringVar(&opt.mode, "mode", "", "init, next, load or leg")
	flags.StringVar(&opt.engine, "engine", "", "mongo, mysql or hoarddb")
	flags.StringVar(&opt.mongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB connection string")
	flags.StringVar(&opt.mongoWC, "mongo-wc", "w1j", "MongoDB write concern: w1j (journaled) or w1")
	flags.StringVar(&opt.mysqlDSN, "mysql-dsn", "", "MySQL DSN ending in /scale")
	flags.StringVar(&opt.hoardAddr, "addr", "localhost:7433", "hoarddb: the node's TCP address, host:port")
	flags.StringVar(&opt.hoardPassword, "password", "", "hoarddb: the root password")
	flags.StringVar(&opt.hoardDatabase, "database", "scale", "hoarddb: the database holding the User_<owner> buckets")
	flags.StringVar(&opt.hoardDurability, "hoard-durability", "", "hoarddb: the server's durability settings as the launching script states them, recorded verbatim with every leg")
	flags.StringVar(&opt.layout, "layout", "hu", "hoarddb: bucket layout; only hu (one User_<owner> bucket per owner) is supported")
	flags.Int64Var(&opt.fanout, "owner-fanout", ownerFanout, "records per owner: hoarddb makes one bucket per this many records (MongoDB and MySQL keep 10)")
	flags.StringVar(&opt.out, "out", "", "append the leg record to this file instead of printing it")
	flags.Int64Var(&opt.from, "from", 0, "load: first key (inclusive)")
	flags.Int64Var(&opt.to, "to", 0, "load: last key (exclusive)")
	flags.IntVar(&opt.workers, "workers", 64, "clients in flight, closed loop")
	flags.IntVar(&opt.bodyBytes, "body-bytes", 1024, "bytes in each record's body")
	flags.DurationVar(&opt.warmup, "warmup", 10*time.Second, "unmeasured warm-up")
	flags.DurationVar(&opt.measured, "seconds", 60*time.Second, "measured window")
	flags.Uint64Var(&opt.seed, "seed", 1, "workload seed")
	if err := flags.Parse(args); err != nil {
		return opt, fmt.Errorf("reading flags: %w", err)
	}
	return opt, nil
}

// openStore connects to the engine -engine names.
func openStore(ctx context.Context, opt sweepFlags) (store, error) {
	if opt.fanout < 1 {
		return nil, fmt.Errorf("-owner-fanout %d is not a record count; give 1 or more (the default is %d)", opt.fanout, ownerFanout)
	}
	if opt.engine != "hoarddb" && opt.fanout != ownerFanout {
		return nil, fmt.Errorf("-owner-fanout applies to -engine hoarddb only; %s keeps %d records per owner", opt.engine, ownerFanout)
	}
	switch opt.engine {
	case "hoarddb":
		if opt.layout != hoardLayout {
			return nil, fmt.Errorf("-layout %q is not supported; hoardDB sweeps run the hu layout (one User_<owner> bucket per owner), so use -layout hu", opt.layout)
		}
		return newHoardStore(opt.hoardAddr, opt.hoardPassword, opt.hoardDatabase, opt.hoardDurability, opt.fanout, max(opt.workers, loadParallel)), nil
	case "mongo":
		return newMongoStore(ctx, opt.mongoURI, opt.mongoWC, max(opt.workers, loadParallel))
	case "mysql":
		return newMySQLStore(ctx, opt.mysqlDSN, max(opt.workers, loadParallel))
	default:
		return nil, fmt.Errorf("unknown -engine %q: use mongo, mysql or hoarddb", opt.engine)
	}
}

func main() {
	opt, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(context.Background(), opt, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "scalesweep:", err)
		os.Exit(1)
	}
}
